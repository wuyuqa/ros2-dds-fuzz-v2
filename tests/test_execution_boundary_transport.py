"""Socket-only contracts: no ROS, namespaces, real leases or runtime execution.

All pytest inputs are self-contained. The original failed run is read only by
an optional evidence script outside this test module, never as a test fixture.
"""
from contextlib import contextmanager
from copy import deepcopy
import array
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import tempfile

import pytest

from r2f.runtime import execution_boundary as boundary


@pytest.fixture
def channel():
    left, right = boundary._pair()
    try:
        yield left, right
    finally:
        left.close()
        right.close()


def object_of_size(size):
    value = {'x': 'x' * (size - 8)}
    assert len(boundary._json(value)) == size
    return value


def open_fds():
    result = set()
    for entry in Path('/proc/self/fd').iterdir():
        try:
            os.fstat(int(entry.name))
        except OSError:
            continue  # The directory iterator itself is already closed.
        result.add(int(entry.name))
    return result


@contextmanager
def application_fds(count):
    files = []
    try:
        for i in range(count):
            stream = tempfile.TemporaryFile()
            files.append(stream)
            stream.write(('lease-substitute-' + str(i)).encode())
            stream.flush()
            stream.seek(i)
        yield [f.fileno() for f in files]
    finally:
        for stream in files:
            stream.close()


@contextmanager
def memfd(data, seals=boundary._TRANSPORT_SEALS, *, allow_sealing=True):
    flags = os.MFD_CLOEXEC | (os.MFD_ALLOW_SEALING if allow_sealing else 0)
    fd = os.memfd_create('independent-transport-test', flags)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        if seals:
            fcntl.fcntl(fd, fcntl.F_ADD_SEALS, seals)
        yield fd
    finally:
        os.close(fd)


def envelope(data, count=0, **changes):
    result = {boundary._TRANSPORT_KEY: 'sealed_memfd', 'version': 1,
              'size': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
              'fd_index': count, 'application_fd_count': count}
    result.update(changes)
    return result


def raw_send(sock, data, fds=()):
    ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))] if fds else []
    assert sock.sendmsg([data], ancillary) == len(data)


def close_fds(fds):
    for fd in fds:
        os.close(fd)


def assert_application_fds(received, originals):
    assert len(received) == len(originals)
    for i, (copy, original) in enumerate(zip(received, originals)):
        a, b = os.fstat(copy), os.fstat(original)
        assert (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)
        assert os.pread(copy, 64, 0) == ('lease-substitute-' + str(i)).encode()
        assert not os.get_inheritable(copy)
        # SCM_RIGHTS preserves the same open file description, not a reopened path.
        os.lseek(copy, i + 2, os.SEEK_SET)
        assert os.lseek(original, 0, os.SEEK_CUR) == i + 2


def rejected(channel, data, fds, match, exception=boundary.BoundaryError):
    receiver, sender = channel
    before = open_fds()
    raw_send(sender, data, fds)
    with pytest.raises(exception, match=match):
        boundary._recv(receiver, 1)
    assert open_fds() == before
    for fd in fds:
        os.fstat(fd)  # The sender's descriptors are never consumed.


class WireTap:
    def __init__(self, sock):
        self.sock = sock
        self.packets = []

    def sendmsg(self, buffers, ancillary):
        data = b''.join(buffers)
        passed = []
        for level, kind, value in ancillary:
            assert (level, kind) == (socket.SOL_SOCKET, socket.SCM_RIGHTS)
            passed.extend(value)
        info = {'data': data, 'fds': tuple(passed)}
        if boundary._TRANSPORT_KEY in json.loads(data):
            fd = passed[-1]
            info.update(seals=fcntl.fcntl(fd, fcntl.F_GET_SEALS),
                        inheritable=os.get_inheritable(fd),
                        payload=os.pread(fd, boundary._CONTROL_MAX_JSON_BYTES, 0))
        self.packets.append(info)
        return self.sock.sendmsg(buffers, ancillary)


@pytest.mark.parametrize('size', [32, 59999, 60000])
@pytest.mark.parametrize('count', [0, 3, 16])
def test_inline_legacy_bytes_credentials_and_fd_order(channel, monkeypatch, size, count):
    receiver, sender = channel
    monkeypatch.setattr(boundary.os, 'memfd_create', lambda *a: pytest.fail('small packet used memfd'))
    value = object_of_size(size)
    tap = WireTap(sender)
    with application_fds(count) as originals:
        before = open_fds()
        boundary._send(tap, value, originals)
        result, peer, received = boundary._recv(receiver, 1)
        try:
            assert result == value
            assert tap.packets[0]['data'] == boundary._json(value)
            assert tap.packets[0]['fds'] == tuple(originals)
            assert peer == (os.getpid(), os.getuid(), os.getgid())
            assert_application_fds(received, originals)
        finally:
            close_fds(received)
        assert open_fds() == before


@pytest.mark.parametrize('count', [0, 3, 16])
def test_large_sealed_envelope_and_extra_cmsg_capacity(channel, monkeypatch, count):
    receiver, sender = channel
    value = object_of_size(60001)
    tap = WireTap(sender)
    created = []
    create = os.memfd_create

    def capture(name, flags):
        created.append((name, flags))
        return create(name, flags)

    monkeypatch.setattr(boundary.os, 'memfd_create', capture)
    with application_fds(count) as originals:
        before = open_fds()
        boundary._send(tap, value, originals)
        wire = tap.packets[0]
        assert created == [('r2f-boundary-json', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)]
        assert len(wire['data']) <= 60000
        assert json.loads(wire['data']) == envelope(boundary._json(value), count)
        assert wire['fds'][:count] == tuple(originals)
        assert len(wire['fds']) == count + 1
        assert wire['seals'] & boundary._TRANSPORT_SEALS == boundary._TRANSPORT_SEALS
        assert wire['inheritable'] is False
        assert wire['payload'] == boundary._json(value)
        with pytest.raises(OSError):
            os.fstat(wire['fds'][-1])  # Local transport FD closed after sendmsg.
        result, peer, received = boundary._recv(receiver, 1)
        try:
            assert result == value
            assert peer == (os.getpid(), os.getuid(), os.getgid())
            assert_application_fds(received, originals)
        finally:
            close_fds(received)
        assert open_fds() == before


def core_topology_request():
    """Use actual registry/provider/core capsule paths; no padding or runtime."""
    from r2f.core import Registry, canonical_hash
    from r2f.core.evidence import INPUT_SCHEMA_V2
    from r2f.core.replay import ReplayCapsule
    from r2f.core.scenario import Scenario
    from r2f.campaign.exploratory_input import runtime_input
    from r2f.modules.s1 import topology_input, topology_model
    from r2f.modules.s1.module import register_exploratory
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY

    registry = Registry()
    register_exploratory(registry, runtime_capability=TOPOLOGY_CAPABILITY)
    plan = topology_input.make_topology_plan(
        topology_model.generate_program(0, writers=1, readers=1),
        [{'operator_id': 'action.insert', 'slot_id': 'S1.topology.actions'}] * 2,
        rng_seed=7)
    scenario = Scenario(registry.get_target(TOPOLOGY_TARGET), 'rmw_cyclonedds_cpp',
                        'mutated', 0, 7, {}, plan)
    prepared = registry.get_services(TOPOLOGY_TARGET).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(
        scenario, source_manifest={'source_frozen': False, 'offline_transport_test': True},
        final_input=prepared['final_input'], input_layer_receipts=prepared['input_layer_receipts'])
    effective = capsule.input['effective_input']
    config = {k: deepcopy(effective[k]) for k in (
        'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
        'mutation_kind', 'carrier', 'input_layer_receipts', 'source_manifest', 'final_input')}
    config.update(input_identity_schema=INPUT_SCHEMA_V2,
                  effective_input_sha256=capsule.input['effective_input_sha256'],
                  effective_input=deepcopy(effective), scenario=scenario.to_dict(),
                  mutation_plan=plan.to_dict(), plan_hash=plan.plan_hash(),
                  mutation_plan_sha256=canonical_hash(plan.to_dict()),
                  runtime_capability=TOPOLOGY_CAPABILITY)
    config.update(runtime_input(effective['final_input'], backend='topology_pair',
                                runtime_capability=TOPOLOGY_CAPABILITY))
    # Same boundary request container, intentionally not an execution admission.
    return {'schema': boundary.SCHEMA, 'config': config, 'probe': None,
            'host_clearance_claimed': False}


def test_real_core_large_request_three_lease_substitutes_two_hops(record_property):
    request = core_topology_request()
    original = boundary._json(request)
    assert 60000 < len(original) <= boundary._CONTROL_MAX_JSON_BYTES
    record_property('core_request_compact_bytes', len(original))
    record_property('core_request_sha256', hashlib.sha256(original).hexdigest())
    before = open_fds()
    first_rx, first_tx = boundary._pair()
    second_rx, second_tx = boundary._pair()
    parent_pid = os.getpid()
    try:
        with application_fds(3) as originals:
            pid = os.fork()  # Socket forwarder only; no namespaces or workloads.
            if pid == 0:
                received = []
                code = 1
                try:
                    first_tx.close()
                    second_rx.close()
                    child_before = open_fds()
                    value, peer, received = boundary._recv(first_rx, 3)
                    assert open_fds() == child_before | set(received)
                    assert peer == (parent_pid, os.getuid(), os.getgid())
                    assert boundary._json(value) == original
                    child_after_recv = open_fds()
                    boundary._send(second_tx, value, received)
                    assert open_fds() == child_after_recv
                    close_fds(received)
                    received = []
                    assert open_fds() == child_before
                    code = 0
                finally:
                    close_fds(received)
                    first_rx.close()
                    second_tx.close()
                    os._exit(code)
            first_rx.close()
            second_tx.close()
            try:
                boundary._send(first_tx, request, originals)
                result, peer, received = boundary._recv(second_rx, 5)
                try:
                    assert peer == (pid, os.getuid(), os.getgid())
                    assert boundary._json(result) == original
                    assert result == request
                    assert_application_fds(received, originals)
                finally:
                    close_fds(received)
            finally:
                # Bounded recv above; no surviving child if a parent assertion fails.
                # The forwarder has no work after sendmsg; allow its finally to
                # close FDs before using a bounded emergency kill.
                import time
                deadline = time.monotonic() + 2
                while True:
                    reaped, status = os.waitpid(pid, os.WNOHANG)
                    if reaped:
                        break
                    if time.monotonic() >= deadline:
                        os.kill(pid, signal.SIGKILL)
                        _, status = os.waitpid(pid, 0)
                        break
                    time.sleep(.005)
            # SIGKILL is only a fallback; successful forwarder must exit normally.
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
            record_property('forwarder_pid', pid)
    finally:
        for sock in (first_rx, first_tx, second_rx, second_tx):
            sock.close()
    # All sockets + received FDs, including both hidden memfds, were closed.
    assert open_fds() == before


@pytest.mark.parametrize('extra', [0, 1])
def test_capacity_limit_is_explicit_and_fail_closed(channel, extra):
    receiver, sender = channel
    value = object_of_size(boundary._CONTROL_MAX_JSON_BYTES + extra)
    before = open_fds()
    if extra:
        with pytest.raises(boundary.BoundaryError, match='too_large'):
            boundary._send(sender, value)
        with pytest.raises(TimeoutError):
            boundary._recv(receiver, .001)
    else:
        boundary._send(sender, value)
        result, peer, fds = boundary._recv(receiver, 1)
        assert result == value and not fds
        assert peer[0] == os.getpid()
    assert open_fds() == before


@pytest.mark.parametrize('value', ['sealed_memfd', None, {}, 'v2'])
def test_reserved_envelope_key_not_application_data(channel, value):
    receiver, sender = channel
    with pytest.raises(boundary.BoundaryError, match='reserved'):
        boundary._send(sender, {boundary._TRANSPORT_KEY: value})
    assert not select.select([receiver], [], [], 0)[0]


def test_nested_non_top_level_reserved_key_remains_ordinary_data(channel):
    receiver, sender = channel
    value = {'nested': {boundary._TRANSPORT_KEY: 'application value'}}
    boundary._send(sender, value)
    assert boundary._recv(receiver, 1)[0] == value


@pytest.mark.parametrize('key,value,match', [
    ('version', 2, 'schema'), ('version', True, 'schema'), ('version', '1', 'schema'),
    (boundary._TRANSPORT_KEY, 'path', 'schema'), ('path', '/tmp/not-trusted', 'schema'),
    ('size', True, 'size_invalid'), ('size', 60000, 'size_invalid'),
    ('size', boundary._CONTROL_MAX_JSON_BYTES + 1, 'size_invalid'),
    ('size', 60002, 'size_mismatch'), ('size', 60000.5, 'size_invalid'),
    ('sha256', '0' * 64, 'sha256_mismatch'), ('sha256', 'A' * 64, 'sha256_invalid'),
    ('sha256', 'x', 'sha256_invalid'), ('sha256', None, 'sha256_invalid'),
    ('application_fd_count', -1, 'fd_count'), ('application_fd_count', True, 'fd_count'),
    ('application_fd_count', 17, 'fd_count'), ('application_fd_count', 1, 'fd_count'),
    ('fd_index', -1, 'fd_count'), ('fd_index', 1, 'fd_count'),
    ('fd_index', True, 'fd_count'), ('fd_index', 0.0, 'fd_count'),
])
def test_envelope_schema_hash_size_index_rejections_close_every_fd(channel, key, value, match):
    data = boundary._json(object_of_size(60001))
    with memfd(data) as fd:
        rejected(channel, boundary._json(envelope(data, **{key: value})), [fd], match)


@pytest.mark.parametrize('missing', sorted(boundary._TRANSPORT_KEYS - {boundary._TRANSPORT_KEY}))
def test_missing_envelope_field_rejected(channel, missing):
    data = boundary._json(object_of_size(60001))
    invalid = envelope(data)
    del invalid[missing]
    with memfd(data) as fd:
        rejected(channel, boundary._json(invalid), [fd], 'schema')


def test_duplicate_envelope_field_rejected(channel):
    data = boundary._json(object_of_size(60001))
    invalid = boundary._json(envelope(data))[:-1] + b',"version":1}'
    with memfd(data) as fd:
        rejected(channel, invalid, [fd], 'duplicate_key')


@pytest.mark.parametrize('count', [0, 2, 3])
def test_envelope_actual_fd_count_must_be_exact(channel, count):
    data = boundary._json(object_of_size(60001))
    with application_fds(count) as fds:
        rejected(channel, boundary._json(envelope(data)), fds, 'fd_count')


@pytest.mark.parametrize('index', [0, 1, 2, 4])
def test_transport_fd_index_cannot_reorder_application_handles(channel, index):
    data = boundary._json(object_of_size(60001))
    with application_fds(3) as app, memfd(data) as fd:
        rejected(channel, boundary._json(envelope(data, 3, fd_index=index)), [*app, fd], 'fd_count')


@pytest.mark.parametrize('missing', [fcntl.F_SEAL_WRITE, fcntl.F_SEAL_GROW,
                                     fcntl.F_SEAL_SHRINK, fcntl.F_SEAL_SEAL])
def test_every_required_memfd_seal_is_enforced(channel, missing):
    data = boundary._json(object_of_size(60001))
    with application_fds(3) as app, memfd(data, boundary._TRANSPORT_SEALS & ~missing) as fd:
        rejected(channel, boundary._json(envelope(data, 3)), [*app, fd], 'seals_missing')


def test_memfd_without_allow_sealing_is_rejected(channel):
    data = boundary._json(object_of_size(60001))
    with memfd(data, 0, allow_sealing=False) as fd:
        rejected(channel, boundary._json(envelope(data)), [fd], 'seals_missing')


@pytest.mark.parametrize('kind', ['regular', 'pipe', 'socket', 'character'])
def test_non_memfd_transport_type_is_rejected(channel, kind):
    data = boundary._json(object_of_size(60001))
    if kind == 'regular':
        with tempfile.TemporaryFile() as stream:
            stream.write(data)
            stream.flush()
            rejected(channel, boundary._json(envelope(data)), [stream.fileno()], 'not_memfd')
    elif kind == 'pipe':
        reader, writer = os.pipe()
        try:
            rejected(channel, boundary._json(envelope(data)), [reader], 'not_memfd')
        finally:
            close_fds([reader, writer])
    elif kind == 'socket':
        rejected(channel, boundary._json(envelope(data)), [channel[1].fileno()], 'not_memfd')
    else:
        with open('/dev/null', 'rb') as stream:
            rejected(channel, boundary._json(envelope(data)), [stream.fileno()], 'not_memfd')


@pytest.mark.parametrize('extra', [-1, 1])
def test_exact_memfd_size_no_prefix_or_trailing_bytes(channel, extra):
    data = boundary._json(object_of_size(60002))
    with memfd(data) as fd:
        rejected(channel, boundary._json(envelope(data, size=len(data) + extra)), [fd], 'size_mismatch')


@pytest.mark.parametrize('kind', ['list', 'string', 'invalid', 'nonfinite', 'duplicate', 'nested_envelope'])
def test_memfd_payload_must_be_strict_non_envelope_json_object(channel, kind):
    padding = 'x' * 60000
    payloads = {
        'list': json.dumps([padding]).encode(), 'string': json.dumps(padding).encode(),
        'invalid': b'{' + padding.encode(),
        'nonfinite': ('{"x":"' + padding + '","bad":NaN}').encode(),
        'duplicate': ('{"x":"' + padding + '","a":1,"a":2}').encode(),
        'nested_envelope': boundary._json({boundary._TRANSPORT_KEY: 'sealed_memfd', 'x': padding}),
    }
    data = payloads[kind]
    match = {'list': 'not_object', 'string': 'not_object', 'invalid': 'json_invalid',
             'nonfinite': 'nonfinite', 'duplicate': 'duplicate_key', 'nested_envelope': 'reserved'}[kind]
    with memfd(data) as fd:
        rejected(channel, boundary._json(envelope(data)), [fd], match)


@pytest.mark.parametrize('size', [60001, 65536, 65537])
def test_oversized_raw_inline_or_msg_trunc_rejected(channel, size):
    with application_fds(3) as app:
        rejected(channel, boundary._json(object_of_size(size)), app,
                 'truncated_kernel' if size > 65536 else 'too_large')


def test_real_msg_ctrunc_closes_all_installed_descriptors(channel):
    with application_fds(1) as originals:
        rejected(channel, b'{}', originals * 64, 'truncated_kernel')


@pytest.mark.parametrize('large', [False, True])
def test_explicit_application_fd_limit_send_and_receive(channel, large):
    receiver, sender = channel
    with application_fds(17) as app:
        before = open_fds()
        with pytest.raises(boundary.BoundaryError, match='application_fd_limit'):
            boundary._send(sender, object_of_size(60001 if large else 32), app)
        assert open_fds() == before
        assert not select.select([receiver], [], [], 0)[0]
        rejected(channel, b'{}', app, 'application_fd_limit')


def test_timeout_and_eof_unchanged_and_no_fd_leak(channel):
    receiver, sender = channel
    before = open_fds()
    with pytest.raises(TimeoutError, match='boundary_control_timeout'):
        boundary._recv(receiver, .001)
    assert open_fds() == before
    sender_fd = sender.fileno()
    sender.close()
    with pytest.raises(EOFError, match='boundary_peer_closed'):
        boundary._recv(receiver, .1)
    assert open_fds() == before - {sender_fd}


@pytest.mark.parametrize('large', [False, True])
@pytest.mark.parametrize('failure', ['exception', 'short'])
def test_send_failure_never_closes_caller_fds_or_leaks_memfd(channel, large, failure):
    class FailingSocket:
        def sendmsg(self, buffers, ancillary):
            if failure == 'exception':
                raise OSError(errno.EPIPE, 'test peer closed')
            return len(buffers[0]) - 1

    with application_fds(3) as app:
        before = open_fds()
        with pytest.raises(OSError if failure == 'exception' else boundary.BoundaryError):
            boundary._send(FailingSocket(), object_of_size(60001 if large else 32), app)
        assert open_fds() == before
        for fd in app:
            os.fstat(fd)


@pytest.mark.parametrize('failure', ['create', 'write', 'zero_write', 'seal'])
def test_memfd_preparation_errors_close_internal_fd_only(channel, monkeypatch, failure):
    receiver, sender = channel
    with application_fds(3) as app:
        before = open_fds()
        def fail(*args):
            raise OSError(errno.EIO, 'injected ' + failure)
        if failure == 'create':
            monkeypatch.setattr(boundary.os, 'memfd_create', fail)
        elif failure in ('write', 'zero_write'):
            monkeypatch.setattr(boundary.os, 'write', fail if failure == 'write' else lambda *a: 0)
        else:
            monkeypatch.setattr(boundary.fcntl, 'fcntl', fail)
        with pytest.raises(boundary.BoundaryError if failure == 'zero_write' else OSError):
            boundary._send(sender, object_of_size(60001), app)
        assert open_fds() == before
        assert not select.select([receiver], [], [], 0)[0]


def test_partial_writes_and_bounded_pread_ignore_shared_transport_offset(channel, monkeypatch):
    receiver, sender = channel
    value = object_of_size(160000)
    write, pread = os.write, os.pread
    writes, reads = [], []
    def partial(fd, data):
        writes.append(len(data))
        return write(fd, data[:8192])
    def read(fd, size, offset):
        assert 0 < size <= 65536
        reads.append((size, offset))
        return pread(fd, min(size, 8192), offset)
    monkeypatch.setattr(boundary.os, 'write', partial)
    monkeypatch.setattr(boundary.os, 'pread', read)
    boundary._send(sender, value)
    result, _, fds = boundary._recv(receiver, 1)
    assert result == value and not fds
    assert len(writes) > 1 and len(reads) > 1
    assert reads[0][1] == 0


@pytest.mark.parametrize('failure', ['eof', 'exception'])
def test_read_failure_closes_transport_and_application_fds(channel, monkeypatch, failure):
    data = boundary._json(object_of_size(60001))
    with application_fds(3) as app, memfd(data) as fd:
        def fail(*args):
            if failure == 'eof':
                return b''
            raise OSError(errno.EIO, 'injected read failure')
        monkeypatch.setattr(boundary.os, 'pread', fail)
        rejected(channel, boundary._json(envelope(data, 3)), [*app, fd],
                 'read_incomplete' if failure == 'eof' else 'read failure',
                 boundary.BoundaryError if failure == 'eof' else OSError)


@pytest.mark.parametrize('failure', ['missing', 'duplicate', 'malformed', 'rights', 'json', 'object', 'eof'])
def test_all_ancillary_fds_collected_before_any_parse_failure(failure):
    # A synthetic recvmsg result exercises malformed cmsgs the kernel itself
    # normally prevents, especially credentials appearing before rights.
    with application_fds(2) as originals:
        before = open_fds()
        copies = [os.dup(fd) for fd in originals]
        cred = struct.pack('3i', os.getpid(), os.getuid(), os.getgid())
        creds = [] if failure == 'missing' else [cred, cred] if failure == 'duplicate' else [b'x'] if failure == 'malformed' else [cred]
        rights = array.array('i', copies).tobytes() + (b'x' if failure == 'rights' else b'')
        ancillary = [(socket.SOL_SOCKET, socket.SCM_CREDENTIALS, c) for c in creds]
        ancillary.append((socket.SOL_SOCKET, socket.SCM_RIGHTS, rights))
        data = {'json': b'{', 'object': b'[]', 'eof': b''}.get(failure, b'{}')
        class FakeReceiver:
            def recvmsg(self, size, capacity, flags):
                assert capacity == boundary._CONTROL_ANCILLARY_BYTES
                assert flags == socket.MSG_CMSG_CLOEXEC
                return data, ancillary, 0, None
        with pytest.raises((boundary.BoundaryError, ValueError, EOFError)):
            boundary._recv(FakeReceiver())
        assert open_fds() == before


def test_large_json_cannot_replace_kernel_credentials(channel):
    receiver, sender = channel
    value = object_of_size(60001)
    value['credentials'] = {'pid': 1, 'uid': 0, 'gid': 0}
    boundary._send(sender, value)
    result, peer, fds = boundary._recv(receiver, 1)
    assert result == value and not fds
    assert peer == (os.getpid(), os.getuid(), os.getgid())


@pytest.mark.parametrize('large', [False, True])
def test_real_closed_socket_send_does_not_consume_application_fds(channel, large):
    receiver, sender = channel
    receiver.close()
    with application_fds(3) as originals:
        before = open_fds()
        with pytest.raises(BrokenPipeError):
            boundary._send(sender, object_of_size(60001 if large else 32), originals)
        assert open_fds() == before


def test_missing_real_kernel_credentials_closes_large_message_fds():
    receiver, sender = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        with application_fds(3) as originals:
            before = open_fds()
            boundary._send(sender, object_of_size(60001), originals)
            with pytest.raises(boundary.BoundaryError, match='credentials'):
                boundary._recv(receiver, 1)
            assert open_fds() == before
    finally:
        receiver.close()
        sender.close()


def test_memfd_size_upper_bound_checked_before_read(channel, monkeypatch):
    data = boundary._json(object_of_size(60001))
    def no_read(*args):
        pytest.fail('oversized or size-mismatched carrier must not be read')
    with memfd(data) as fd:
        monkeypatch.setattr(boundary.os, 'pread', no_read)
        rejected(channel, boundary._json(envelope(data, size=boundary._CONTROL_MAX_JSON_BYTES + 1)), [fd], 'size_invalid')
        rejected(channel, boundary._json(envelope(data, size=len(data) + 1)), [fd], 'size_mismatch')


def test_success_and_failure_stress_leave_no_transport_descriptors(channel):
    receiver, sender = channel
    data = boundary._json(object_of_size(60001))
    with application_fds(3) as originals, memfd(data) as fd:
        before = open_fds()
        for _ in range(32):
            boundary._send(sender, json.loads(data), originals)
            value, peer, received = boundary._recv(receiver, 1)
            try:
                assert boundary._json(value) == data
                assert peer[0] == os.getpid()
                assert_application_fds(received, originals)
            finally:
                close_fds(received)
            rejected(channel, boundary._json(envelope(data, 3, sha256='0' * 64)),
                     [*originals, fd], 'sha256_mismatch')
            assert open_fds() == before


def test_legacy_nonserializable_input_keeps_caller_fds(channel):
    with application_fds(3) as originals:
        before = open_fds()
        with pytest.raises(ValueError):
            boundary._send(channel[1], {'nan': float('nan')}, originals)
        assert open_fds() == before


@pytest.mark.parametrize('number', ['1e999', '-1e999', '1.7976931348623159e308', '-1.7976931348623159e308'])
@pytest.mark.parametrize('nested', [False, True])
def test_memfd_payload_exponent_overflow_is_rejected_without_fd_leak(channel, number, nested):
    numeric = '[{"n":' + number + '}]' if nested else number
    data = ('{"x":"' + 'x' * 60000 + '","numeric":' + numeric + '}').encode()
    # These are valid JSON number tokens, not the parse_constant NaN/Infinity extension.
    assert not __import__('math').isfinite(float(number))
    with application_fds(3) as app, memfd(data) as fd:
        rejected(channel, boundary._json(envelope(data, 3)), [*app, fd], 'json_nonfinite')


@pytest.mark.parametrize('number', ['1e999', '-1e999'])
@pytest.mark.parametrize('key', ['version', 'size', 'application_fd_count', 'fd_index', 'unexpected'])
def test_envelope_exponent_overflow_is_rejected_without_fd_leak(channel, number, key):
    data = boundary._json(object_of_size(60001))
    env = envelope(data, 3)
    env.pop(key, None)
    wire = boundary._json(env)[:-1] + (',"' + key + '":' + number + '}').encode()
    with application_fds(3) as app, memfd(data) as fd:
        rejected(channel, wire, [*app, fd], 'json_nonfinite')


@pytest.mark.parametrize('number', ['1.7976931348623157e308', '-1.7976931348623157e308', '1e-999'])
def test_finite_extreme_json_numbers_are_still_reencodable(channel, number):
    receiver, sender = channel
    data = ('{"x":"' + 'x' * 60000 + '","numeric":' + number + '}').encode()
    before = open_fds()
    with memfd(data) as fd:
        raw_send(sender, boundary._json(envelope(data)), [fd])
        result, peer, received = boundary._recv(receiver, 1)
        assert received == [] and peer[0] == os.getpid()
        assert result == json.loads(data)
        encoded = boundary._json(result)  # allow_nan=False must continue to work.
        assert json.loads(encoded) == result
    assert open_fds() == before
