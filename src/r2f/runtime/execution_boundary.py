"""Opt-in full execution namespace sidecar; never an alternate campaign runner.

Public callable: run_isolated_runtime(config, output_dir).  The host caller is
lease owner; a detached guardian inherits the *same open file descriptions*,
without calling LOCK_UN, and keeps them until the namespace init has exited.
Thus caller SIGKILL/EOF does not release leases while its workers survive.
The guardian is deliberately outside the workload PID/user/mount/net namespaces.

The trusted launcher uses unshare(2), a gated fork and pidfds (not an env-var
namespace assertion). SCM_CREDENTIALS authenticates each sender; SCM_RIGHTS
carries the kernel pidfd and namespace FDs. No ROS imports occur in probe mode.
This is process/network isolation, NOT a filesystem or hostile-code sandbox.
"""
from __future__ import annotations

import array
import contextlib
import ctypes
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import sys
import time
import uuid

from .domains import LeaseSet
from .environment import REPO

NAMESPACES = ('user', 'mnt', 'pid', 'net')
CLONE_FLAGS = {'user': 0x10000000, 'mnt': 0x20000, 'pid': 0x20000000, 'net': 0x40000000}
SCHEMA = 'r2f.execution-boundary/v1'
MODULE = 'r2f.runtime.execution_boundary'
_RECEIPT_KEYS = ('planned', 'submitted', 'carrier_admitted', 'configured', 'applied', 'hit', 'effect_observed')
# Only retained on cleanup uncertainty: never unlock resources still in use.
_QUARANTINED_LEASES = []


class BoundaryError(RuntimeError):
    pass


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _hash(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as out:
        out.write(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n')
        out.flush()
        os.fsync(out.fileno())


def _ticks(pid):
    text = Path(f'/proc/{pid}/stat').read_text()
    if int(text.split(' ', 1)[0]) != pid:
        raise BoundaryError('process_identity_invalid')
    return int(text.rsplit(')', 1)[1].split()[19])


def _status(pid='self'):
    return dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in line)


def _identity(pid=None):
    pid = os.getpid() if pid is None else pid
    status = _status(pid)
    return {'pid': pid, 'start_ticks': _ticks(pid),
            'namespaces': {name: os.readlink(f'/proc/{pid}/ns/{name}') for name in NAMESPACES},
            'uid': int(status['Uid'].split()[0]), 'cap_eff': int(status['CapEff'].strip(), 16),
            'nspid': [int(x) for x in status.get('NSpid', '').split()],
            'uid_map': Path(f'/proc/{pid}/uid_map').read_text(),
            'gid_map': Path(f'/proc/{pid}/gid_map').read_text()}


def _libcall(name, *args):
    libc = ctypes.CDLL(None, use_errno=True)
    if getattr(libc, name)(*args) != 0:
        error = ctypes.get_errno()
        raise OSError(error, name + ':' + os.strerror(error))


def _pdeath(sig):
    _libcall('prctl', 1, int(sig), 0, 0, 0)


def _dumpable():
    _libcall('prctl', 4, 1, 0, 0, 0)


def _drop_caps():
    class Header(ctypes.Structure):
        _fields_ = [('version', ctypes.c_uint32), ('pid', ctypes.c_int)]
    class Data(ctypes.Structure):
        _fields_ = [('effective', ctypes.c_uint32), ('permitted', ctypes.c_uint32), ('inheritable', ctypes.c_uint32)]
    header, data = Header(0x20080522, 0), (Data * 2)()
    _libcall('capset', ctypes.byref(header), ctypes.byref(data))
    _dumpable()  # Permit the same-uid host guardian to independently inspect /proc.
    if int(_status()['CapEff'].strip(), 16) != 0:
        raise BoundaryError('controller_capabilities_not_zero')


def _loopback(*, enable=False):
    if socket.if_nameindex() != [(1, 'lo')]:
        raise BoundaryError('boundary_has_non_loopback_interface')
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        flags = struct.unpack('16sH', fcntl.ioctl(sock, 0x8913, struct.pack('16sH', b'lo', 0))[:18])[1]
        if enable:
            fcntl.ioctl(sock, 0x8914, struct.pack('16sH', b'lo', flags | 1))
            flags = struct.unpack('16sH', fcntl.ioctl(sock, 0x8913, struct.pack('16sH', b'lo', 0))[:18])[1]
    if not flags & 1:
        raise BoundaryError('loopback_not_up')
    return {'interfaces': [[1, 'lo']], 'flags': flags, 'up': True}


def _pair():
    pair = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    for sock in pair:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
    return pair


def _send(sock, value, fds=()):
    ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))] if fds else []
    data = _json(value)
    if len(data) > 60000:
        raise BoundaryError('control_message_too_large')
    if sock.sendmsg([data], ancillary) != len(data):
        raise BoundaryError('control_message_truncated')


def _recv(sock, timeout=None):
    if timeout is not None and not select.select([sock], [], [], timeout)[0]:
        raise TimeoutError('boundary_control_timeout')
    data, ancillary, flags, _ = sock.recvmsg(65536, socket.CMSG_SPACE(4 * 16) + socket.CMSG_SPACE(12), socket.MSG_CMSG_CLOEXEC)
    fds, credentials = [], []
    try:
        for level, kind, value in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                received = array.array('i')
                received.frombytes(value[:len(value) - len(value) % received.itemsize])
                fds.extend(received)
            elif level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS:
                credentials.append(struct.unpack('3i', value))
        if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
            raise BoundaryError('truncated_kernel_credentials_or_message')
        if not data:
            raise EOFError('boundary_peer_closed')
        if len(credentials) != 1:
            raise BoundaryError('missing_or_duplicate_kernel_credentials')
        value = json.loads(data)
        if not isinstance(value, dict):
            raise BoundaryError('control_message_not_object')
        return value, credentials[0], fds
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise


def _pidfd_pid(fd):
    # fdinfo is rendered in the guardian's PID namespace by the kernel.
    if os.readlink(f'/proc/self/fd/{fd}') != 'anon_inode:[pidfd]':
        raise BoundaryError('not_a_kernel_pidfd')
    fields = dict(line.split(':', 1) for line in Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines() if ':' in line)
    pid = int(fields['Pid'])
    if pid <= 0:
        raise BoundaryError('pidfd_process_already_gone')
    return pid


def _exited(fd):
    return bool(select.select([fd], [], [], 0)[0])


def _signal_pidfd(fd, sig):
    try:
        signal.pidfd_send_signal(fd, sig)
    except ProcessLookupError:
        pass


def _parent_of(pid):
    return int(_status(pid)['PPid'].strip())


def _proof(sock, kind):
    fds = [os.pidfd_open(os.getpid())] + [os.open(f'/proc/self/ns/{name}', os.O_RDONLY | os.O_CLOEXEC) for name in NAMESPACES]
    try:
        _send(sock, {'kind': kind, 'reported_inner': _identity(), 'loopback': _loopback()}, fds)
    finally:
        for fd in fds:
            os.close(fd)


def _authenticate(message, credentials, fds, host, *, init_pid=None, launcher_pid=None):
    """Never use reported JSON/env as proof of host PID, namespace or UID."""
    if len(fds) != 5:
        raise BoundaryError('namespace_proof_fd_count')
    pid, uid, gid = credentials
    if pid != _pidfd_pid(fds[0]) or uid != host['uid'] or gid != os.getgid():
        raise BoundaryError('kernel_credentials_pidfd_mismatch')
    before = _ticks(pid)
    actual = _identity(pid)
    if before != actual['start_ticks'] or before != _ticks(pid):
        raise BoundaryError('kernel_process_start_changed')
    if actual['uid'] != 1000 or actual['cap_eff'] != 0:
        raise BoundaryError('inner_controller_not_uid1000_cap0')
    if launcher_pid is not None and _parent_of(pid) != launcher_pid:
        raise BoundaryError('namespace_init_not_launcher_child')
    for name, fd in zip(NAMESPACES, fds[1:]):
        # NS_GET_NSTYPE verifies fd type, not just an arbitrary inode number.
        if fcntl.ioctl(fd, 0xb703) != CLONE_FLAGS[name]:
            raise BoundaryError('namespace_fd_type_mismatch:' + name)
        local = os.stat(f'/proc/{pid}/ns/{name}')
        remote = os.fstat(fd)
        if (local.st_dev, local.st_ino) != (remote.st_dev, remote.st_ino):
            raise BoundaryError('namespace_fd_process_mismatch:' + name)
        if actual['namespaces'][name] == host['namespaces'][name]:
            raise BoundaryError('namespace_not_independent:' + name)
    mapping = [[int(v) for v in row.split()] for row in actual['uid_map'].splitlines()]
    groups = [[int(v) for v in row.split()] for row in actual['gid_map'].splitlines()]
    if mapping != [[1000, 1000, 1]] or groups != [[os.getgid(), os.getgid(), 1]]:
        raise BoundaryError('unexpected_single_user_mapping')
    reported = message.get('reported_inner', {})
    if (reported.get('pid') != actual['nspid'][-1] or reported.get('start_ticks') != before
            or reported.get('namespaces') != actual['namespaces']):
        raise BoundaryError('inner_report_kernel_identity_mismatch')
    if init_pid is None:
        if actual['nspid'][-1] != 1:
            raise BoundaryError('controller_is_not_namespace_init')
    elif actual['namespaces']['pid'] != _identity(init_pid)['namespaces']['pid']:
        raise BoundaryError('fixture_outside_owned_pid_namespace')
    return {'authentication': 'SCM_CREDENTIALS+SCM_RIGHTS+pidfd+proc_start_ticks+NS_GET_NSTYPE',
            'host': actual, 'inner_pid': actual['nspid'][-1],
            'pid_scope': actual['namespaces']['pid'], 'loopback_report': message.get('loopback')}


class _HostLeases(LeaseSet):
    """Add start ticks to *our own, already locked* owner records, never others'."""
    def __init__(self, *args, identity, boundary_id):
        super().__init__(*args)
        self.identity, self.boundary_id = identity, boundary_id

    def __enter__(self):
        super().__enter__()
        try:
            for handle, receipt in zip(self.handles, self.receipts):
                receipt['owner'].update(start_ticks=self.identity['start_ticks'],
                    pid_namespace=self.identity['namespaces']['pid'], lease_scope='host_supervisor',
                    boundary_id=self.boundary_id)
                handle.seek(0)
                handle.truncate()
                json.dump(receipt['owner'], handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
                st = os.fstat(handle.fileno())
                receipt.update(inode=st.st_ino, device=st.st_dev)
            return self
        except BaseException:
            super().__exit__(None, None, None)
            raise


def _check_inherited_leases(request, fds):
    if len(fds) != 3 or len(request['host_leases']) != 3:
        raise BoundaryError('host_lease_count')
    for fd, receipt in zip(fds, request['host_leases']):
        st, current = os.fstat(fd), os.stat(receipt['path'])
        if (st.st_dev, st.st_ino) != (current.st_dev, current.st_ino) or st.st_ino != receipt['inode']:
            raise BoundaryError('host_lease_inode_changed')
        if json.loads(Path(receipt['path']).read_text()) != receipt['owner']:
            raise BoundaryError('host_lease_owner_changed')
        with open(receipt['path']) as contender:
            try:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                fcntl.flock(contender, fcntl.LOCK_UN)
                raise BoundaryError('host_lease_not_held')
        # Reasserting an exclusive flock on the same locked OFD is a no-op.
        # An independent open of the same inode conflicts instead: inode/owner
        # equality plus somebody else's lock is not inheritance proof. Never
        # LOCK_UN here, since that would unlock every SCM_RIGHTS/dup copy.
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise BoundaryError('host_lease_fd_not_locked_ofd') from None


def _source():
    from r2f.campaign.identity import source_manifest
    return source_manifest(REPO)


def _same_source(expected):
    current = _source()
    if (current.get('source_frozen') is not True or
            any(current.get(key) != expected.get(key) for key in ('head', 'source_repo', 'behavior_tree_sha256'))):
        raise BoundaryError('source_not_frozen_or_identity_changed')
    return current


def _formal_identity(config):
    from r2f.core import Registry
    from r2f.core.evidence import canonical_hash
    from r2f.core.replay import ReplayCapsule
    from r2f.core.scenario import Scenario
    from r2f.modules.s1.module import register, register_exploratory
    expected = config.get('source_manifest')
    if not isinstance(expected, dict) or expected.get('source_frozen') is not True:
        raise BoundaryError('source_frozen_required')
    current = _same_source(expected)
    if config.get('expected_head') != current['head'] or Path(config.get('source_repo', '')).resolve() != REPO:
        raise BoundaryError('source_config_identity_mismatch')
    scenario = Scenario.from_dict(config['scenario'])
    registry = Registry()
    if scenario.target_id in ('discovery.late_endpoint', 'discovery.late_join_delivery',
                               'discovery.interruption_recovery', 'builtin.discovery_projection_reconciliation'):
        register(registry)
    else:
        register_exploratory(registry, runtime_capability=config.get('runtime_capability'))
    if registry.get_target(scenario.target_id).to_dict() != scenario.target.to_dict():
        raise BoundaryError('registered_target_manifest_mismatch')
    for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed', 'mutation_kind', 'carrier', 'mutation_parameters'):
        if config.get(key) != scenario.to_dict().get(key):
            raise BoundaryError('scenario_config_mismatch:' + key)
    capability = scenario.target.observation_contract.get('runtime_capability')
    if capability is not None and config.get('runtime_capability') != capability:
        raise BoundaryError('registered_runtime_capability_mismatch')
    if config.get('expected_target_revision') != scenario.target_revision:
        raise BoundaryError('expected_target_revision_mismatch')
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=expected,
        final_input=config['final_input'], input_layer_receipts=config['input_layer_receipts'])
    if capsule.input['effective_input_sha256'] != config.get('effective_input_sha256'):
        raise BoundaryError('effective_input_hash_mismatch')
    for key, value in config['final_input'].items():
        if config.get(key) != value:
            raise BoundaryError('submitted_config_mismatch:' + key)
    return {'source_manifest': current, 'target_sha256': canonical_hash(scenario.target.to_dict()),
            'effective_input_sha256': capsule.input['effective_input_sha256'],
            'config_sha256': _hash(config), 'dependency_lock_sha256': hashlib.sha256(
                (REPO / 'config/dependencies.lock.json').read_bytes()).hexdigest()}


def _blocked(reason):
    return {'real_execution': False, 'candidate_eligible': False,
            'runtime_status': 'blocked', 'status': 'blocked', 'reason': reason,
            'events': [], 'facts': {}, 'environment': {}, 'precondition_receipts': [], 'observation_receipts': [],
            'mutation_receipt': dict.fromkeys(_RECEIPT_KEYS, False), 'execution': None,
            'execution_metadata': None, 'raw_path': None, 'raw_sha256': None}


def _options(value):
    if not isinstance(value, dict):
        raise BoundaryError('execution_boundary_options_not_object')
    unknown = set(value) - {'enabled', 'timeout_sec', 'term_grace_sec', 'kill_wait_sec', 'startup_timeout_sec'}
    if unknown:
        raise BoundaryError('unknown_execution_boundary_option:' + ','.join(sorted(unknown)))
    result = {}
    for name, default, maximum in (('timeout_sec', 180, 86400), ('term_grace_sec', 2, 30),
                                    ('kill_wait_sec', 10, 60), ('startup_timeout_sec', 15, 60)):
        value_ = value.get(name, default)
        if isinstance(value_, bool) or not isinstance(value_, (int, float)) or not math.isfinite(value_) or not 0 < value_ <= maximum:
            raise BoundaryError('invalid_boundary_timeout:' + name)
        result[name] = float(value_)
    return result


def run_isolated_runtime(config, output_dir):
    """Standard runtime callable. Inert unless explicitly enabled AND frozen.

    The unified runner must supply its normal config (scenario/source/final
    input/hash). No fixture/backend override is accepted on this formal path.
    """
    if not isinstance(config, dict):
        return _blocked('blocked:boundary_config_not_object')
    boundary = config.get('execution_boundary', {})
    if not isinstance(boundary, dict) or boundary.get('enabled') is not True:
        return _blocked('blocked:execution_boundary_not_enabled')
    try:
        options = _options(boundary)
        identity = _formal_identity(config)
        from .ros2_pair import normalize_config
        normalized = normalize_config(config)
        if _hash(config) != identity['config_sha256']:
            raise BoundaryError('config_changed_during_admission')
        return _supervise(normalized, Path(output_dir), options, identity, probe=None, submitted_config=config)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as exc:
        result = _blocked('blocked:execution_boundary:' + str(exc))
        result['execution_boundary'] = {'schema': SCHEMA, 'admission_error': type(exc).__name__ + ':' + str(exc),
            'source_frozen_verified': False, 'host_clearance_claimed': False}
        return result


def probe_execution_boundary(output_dir, *, mode='success', timeout_sec=5,
                             preflight_rmws=(), domain_id=181):
    """Explicit non-DDS fixture; temporary private lockroot only, no maturity.

    Modes success/crash/hang exercise a detached, TERM-ignoring descendant;
    preflight optionally calls the unmodified environment probe (no contexts).
    """
    if mode not in ('success', 'crash', 'hang', 'controller_error', 'nested_pid_proc'):
        raise ValueError('invalid_nonDDS_probe_mode')
    if any(rmw not in ('rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp') for rmw in preflight_rmws):
        raise ValueError('invalid_probe_rmw')
    if type(domain_id) is not int or not 0 <= domain_id <= 231:
        raise ValueError('invalid_probe_domain')
    out = Path(output_dir).resolve()
    config = {'run_id': 'nonDDS-' + uuid.uuid4().hex, 'campaign_id': 'nonDDS-boundary-fixture',
              'domain_id': domain_id, 'lock_root': str(out / 'test-only-lockroot')}
    identity = {'source_observed_not_credited': _source(), 'config_sha256': _hash(config),
                'target_sha256': None, 'effective_input_sha256': None, 'source_frozen_required': False}
    return _supervise(config, out, _options({'timeout_sec': timeout_sec, 'term_grace_sec': .3}),
                      identity, probe={'mode': mode, 'preflight_rmws': list(preflight_rmws)})


def _supervise(config, output_dir, options, identity, probe, submitted_config=None):
    out = output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'runtime_result.json').exists():
        raise BoundaryError('output_already_contains_runtime_result')
    host = _identity()
    if host['uid'] != 1000 or host['cap_eff'] != 0:
        raise BoundaryError('host_supervisor_requires_uid1000_cap0')
    boundary_id = uuid.uuid4().hex
    audit = out / ('execution-boundary-' + boundary_id)
    audit.mkdir(mode=0o700)
    _write(audit / 'submitted-config.json', submitted_config if submitted_config is not None else config)
    root = Path(config.get('lock_root', f'/tmp/r2f-runtime-locks-{os.getuid()}')).resolve()
    inner_root = root / 'execution-boundaries' / boundary_id / 'inner'
    inner_config = dict(config, lock_root=str(inner_root))
    request = {'schema': SCHEMA, 'boundary_id': boundary_id, 'host': host,
               'config': inner_config, 'submitted_config_sha256': _hash(config),
               'inner_config_sha256': _hash(inner_config), 'options': options,
               'output_dir': str(out), 'audit_dir': str(audit), 'identity': identity, 'probe': probe,
               'host_clearance_claimed': False,
               'clearance_scope': 'authenticated_independent_user_mount_pid_net_boundary_only'}
    lease = _HostLeases(root, out, config['campaign_id'], config['domain_id'], identity=host, boundary_id=boundary_id)
    parent, child = _pair()
    guardian, complete, cancel_reason = None, None, None
    watched_launcher, watched_init = None, None
    result = _blocked('blocked:boundary_not_started')
    try:
        lease.__enter__()
        request['host_leases'] = lease.receipts
        _write(audit / 'request.json', request)
        with (audit / 'guardian.stdout').open('xb') as stdout, (audit / 'guardian.stderr').open('xb') as stderr:
            guardian = subprocess.Popen([sys.executable, '-B', '-m', MODULE, '_guardian', str(child.fileno())],
                pass_fds=(child.fileno(),), stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                start_new_session=True, cwd=REPO, env=_python_env())
        child.close()
        _send(parent, request, [handle.fileno() for handle in lease.handles])
        # Independent watchdog also bounds a stopped/unresponsive guardian.
        watch_deadline = time.monotonic() + sum(options.values()) + 5
        # Kernel peer binding: reject a control response from any other process.
        while True:
            try:
                packet, credentials, fds = _recv(parent, .2)
                try:
                    if credentials[0] != guardian.pid:
                        raise BoundaryError('guardian_credentials_mismatch')
                    kind = packet.get('kind')
                    if kind in ('launcher_watch', 'pid1_watch'):
                        if len(fds) != 1:
                            raise BoundaryError('supervisor_watch_fd_count')
                        pid = _pidfd_pid(fds[0])
                        expected_parent = guardian.pid if kind == 'launcher_watch' else _pidfd_pid(watched_launcher)
                        if _parent_of(pid) != expected_parent or _ticks(pid) != packet.get('start_ticks'):
                            raise BoundaryError('supervisor_watch_kernel_identity_mismatch')
                        if kind == 'launcher_watch' and watched_launcher is None:
                            watched_launcher, fds = fds[0], []
                        elif kind == 'pid1_watch' and watched_init is None:
                            watched_init, fds = fds[0], []
                        else:
                            raise BoundaryError('duplicate_supervisor_watch')
                        _send(parent, {'kind': 'watch_ack', 'watch': kind})
                    elif kind == 'complete':
                        complete = packet
                        break
                    else:
                        raise BoundaryError('unexpected_guardian_control')
                finally:
                    for fd in fds:
                        os.close(fd)
            except TimeoutError:
                if time.monotonic() > watch_deadline:
                    raise TimeoutError('host_guardian_watchdog_timeout')
                if guardian.poll() is not None:
                    break
            except (KeyboardInterrupt, SystemExit) as exc:
                cancel_reason = type(exc).__name__
                with contextlib.suppress(OSError):
                    _send(parent, {'kind': 'cancel', 'reason': cancel_reason})
                break
        if cancel_reason:
            # Guardian has a bounded TERM/KILL path. Never SIGKILL the lease keeper.
            guardian.wait(timeout=options['term_grace_sec'] + options['kill_wait_sec'] + 8)
        else:
            guardian.wait(timeout=options['kill_wait_sec'] + 8)
        if not (audit / 'manifest.json').exists():
            _rescue_guardian(audit, watched_init, watched_launcher, options)
        manifest_path = audit / 'manifest.json'
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        if complete and complete.get('manifest_sha256') != hashlib.sha256(manifest_path.read_bytes()).hexdigest():
            raise BoundaryError('guardian_manifest_hash_mismatch')
        cleanup = manifest.get('cleanup', {})
        if cleanup.get('confirmed') is not True:
            raise BoundaryError('namespace_cleanup_unconfirmed')
        runtime_path = audit / 'inner-runtime-result.json'
        if runtime_path.is_file() and manifest.get('runtime_result_sha256') == hashlib.sha256(runtime_path.read_bytes()).hexdigest():
            result = json.loads(runtime_path.read_text())
        else:
            result = _blocked('blocked:execution_boundary:' + str(manifest.get('error', 'inner_result_missing')))
        if manifest.get('error') or cancel_reason:
            result.update(status='failed' if result.get('real_execution') else 'blocked',
                runtime_status='failed' if result.get('real_execution') else 'blocked',
                reason='execution_boundary:' + str(manifest.get('error') or cancel_reason))
        context = {'schema': SCHEMA, 'boundary_id': boundary_id, 'manifest_path': str(manifest_path),
            'manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            'host_supervisor': host, 'controller_binding': manifest.get('controller_binding'),
            'host_leases': lease.receipts, 'inner_lock_root': str(inner_root),
            'inner_leases': manifest.get('inner_leases', []), 'cleanup': cleanup,
            'host_clearance_claimed': False, 'clearance_scope': request['clearance_scope'],
            'nonDDS': probe is not None, 'maturity_credit': False if probe else 'decided_by_unified_runner',
            'raw_pid_scope': (manifest.get('controller_binding') or {}).get('pid_scope'),
            'raw_network_scope': 'controller boundary; existing nested backend may create its own user/net namespace'}
        raw_identity = {'schema': SCHEMA, 'boundary_id': boundary_id,
            'raw_path': result.get('raw_path'), 'raw_sha256': result.get('raw_sha256'),
            'pid_namespace': context['raw_pid_scope'],
            'pid_interpretation': 'raw pid/pgid are namespace identifiers, NOT host identifiers',
            'controller_binding': context['controller_binding'],
            'owned_process_observations': manifest.get('owned_process_observations', []),
            'unobserved_worker_mapping': 'unknown; never infer host PID from raw PID equality'}
        _annotate_nested_identity(result, context, raw_identity)
        _write(audit / 'raw-identity.json', raw_identity)
        context['raw_identity_path'] = str(audit / 'raw-identity.json')
        context['raw_identity_sha256'] = hashlib.sha256((audit / 'raw-identity.json').read_bytes()).hexdigest()
        result['execution_boundary'] = context
        for key in ('execution', 'execution_metadata'):
            if isinstance(result.get(key), dict):
                result[key] = dict(result[key], pid_namespace=context['raw_pid_scope'],
                                   pid_interpretation='namespace_PID_not_host_PID')
                if isinstance(result[key].get('workers'), list):
                    result[key]['workers'] = [dict(w, pid_namespace=context['raw_pid_scope'],
                        pid_interpretation='namespace_PID_not_host_PID') for w in result[key]['workers']]
        if probe is None:
            _same_source(identity['source_manifest'])
        return result
    except BaseException as exc:
        with contextlib.suppress(Exception):
            _write(audit / 'supervisor-failure.json', {'error': type(exc).__name__ + ':' + str(exc)})
        if guardian is not None and guardian.poll() is None:
            with contextlib.suppress(OSError):
                _send(parent, {'kind': 'cancel', 'reason': type(exc).__name__})
            with contextlib.suppress(subprocess.TimeoutExpired):
                guardian.wait(timeout=options['term_grace_sec'] + options['kill_wait_sec'] + 8)
        if guardian is not None and not (audit / 'manifest.json').exists() and not (audit / 'supervisor-rescue.json').exists():
            with contextlib.suppress(Exception):
                _rescue_guardian(audit, watched_init, watched_launcher, options)
        reason = 'execution_boundary:' + type(exc).__name__ + ':' + str(exc)
        if result.get('real_execution'):
            result.update(status='failed', runtime_status='failed', reason=reason)
        else:
            result = _blocked('blocked:' + reason)
        result.setdefault('execution_boundary', {}).update({'schema': SCHEMA, 'boundary_id': boundary_id,
            'audit_dir': str(audit), 'host_clearance_claimed': False, 'nonDDS': probe is not None,
            'maturity_credit': False, 'cleanup_confirmed': False,
            'rescue_path': str(audit / 'supervisor-rescue.json') if (audit / 'supervisor-rescue.json').exists() else None})
        return result
    finally:
        parent.close()
        child.close()
        # LOCK_UN acts on the shared open file description, so do NOT unlock
        # until the guardian has closed its lease copies after PIDNS teardown.
        safe = guardian is None or (guardian.poll() is not None and any(
            path.exists() and json.loads(path.read_text()).get('cleanup', {}).get('confirmed') is True
            for path in (audit / 'manifest.json', audit / 'supervisor-rescue.json')))
        if safe:
            lease.__exit__(None, None, None)
        else:
            _QUARANTINED_LEASES.append(lease)
        _write(audit / 'supervisor-final.json', {'host': host, 'guardian_pid': guardian.pid if guardian else None,
            'leases_released': safe, 'leases_quarantined': not safe, 'monotonic_ns': time.monotonic_ns()})
        for fd in (watched_launcher, watched_init):
            if fd is not None:
                os.close(fd)
        if not (out / 'runtime_result.json').exists():
            _write(out / 'runtime_result.json', result)



def _annotate_nested_identity(result, context, raw_identity):
    """Producer extension only, not qualification. Legacy absence is a no-op."""
    nested = result.get('topology_namespace')
    if not isinstance(nested, dict):
        return
    # The original controller_binding continues to identify OUTER PID1. Never
    # relabel nested workers as members of that namespace to satisfy a consumer.
    context.update(topology_namespace=nested,
                   nested_controller_binding=nested.get('nested_controller_binding'),
                   raw_pid_scope=nested.get('raw_pid_scope'),
                   raw_network_scope='authenticated nested topology controller and workers')
    raw_identity.update(topology_namespace=nested,
                        nested_controller_binding=nested.get('nested_controller_binding'),
                        pid_namespace=nested.get('raw_pid_scope'))


def _rescue_guardian(audit, init_fd, launcher_fd, options):
    """Only kernel FDs already bound while guardian was alive are signalable.

    A killed guardian leaves no final manifest. Never infer cleanup from that
    absence or an arbitrary stored PID. The launcher also holds close-only
    inherited lease copies until it reaps PID1, covering caller+guardian death.
    """
    actions = []
    if init_fd is not None and not _exited(init_fd):
        _signal_pidfd(init_fd, signal.SIGTERM)
        actions.append('bound_pid1_TERM')
        select.select([init_fd], [], [], options['term_grace_sec'])
        if not _exited(init_fd):
            _signal_pidfd(init_fd, signal.SIGKILL)
            actions.append('bound_pid1_KILL')
            select.select([init_fd], [], [], options['kill_wait_sec'])
    if launcher_fd is not None and not _exited(launcher_fd):
        _signal_pidfd(launcher_fd, signal.SIGTERM)
        actions.append('bound_launcher_TERM')
        select.select([launcher_fd], [], [], options['kill_wait_sec'])
    # If no PID1 was acknowledged, launcher-confirmed.json (written after its
    # waitpid) is supporting evidence; no backend could have been admitted.
    launcher_record = audit / 'launcher-cleanup.json'
    launcher_reaped = launcher_record.exists() and json.loads(launcher_record.read_text()).get('pid1_reaped') is True
    confirmed = ((init_fd is not None and _exited(init_fd) and
                  launcher_fd is not None and _exited(launcher_fd)) or
                 (init_fd is None and launcher_fd is not None and _exited(launcher_fd) and launcher_reaped))
    _write(audit / 'supervisor-rescue.json', {'schema': SCHEMA, 'guardian_failed': True,
        'cleanup': {'confirmed': confirmed, 'pid1_exited': init_fd is not None and _exited(init_fd),
                    'launcher_exited': launcher_fd is not None and _exited(launcher_fd),
                    'actions': actions, 'monotonic_ns': time.monotonic_ns()},
        'execution_credit': False, 'real_execution_observation': 'unknown_without_final_backend_result'})


def _guardian_watch(control, kind, fd, parent_pid):
    _send(control, {'kind': kind, 'start_ticks': _ticks(_pidfd_pid(fd))}, [fd])
    packet, peer, handles = _recv(control, 3)
    for handle in handles:
        os.close(handle)
    if peer[0] != parent_pid or packet != {'kind': 'watch_ack', 'watch': kind}:
        raise BoundaryError('supervisor_watch_not_acknowledged')


def _python_env():
    # No namespace claim comes from this environment; PYTHONPATH is just loading.
    env = dict(os.environ)
    env['PYTHONPATH'] = str(REPO / 'src')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env


def _launcher(fd):
    """Trusted small host-child. Gated PID1 fork removes PDEATHSIG setup races."""
    control = socket.socket(fileno=fd)
    parent_pid = os.getppid()
    cancelled = False
    def cancel(*_):
        nonlocal cancelled
        cancelled = True
    signal.signal(signal.SIGTERM, cancel)
    signal.signal(signal.SIGINT, cancel)
    _pdeath(signal.SIGTERM)
    if os.getppid() != parent_pid or parent_pid == 1:
        return 125
    while True:
        if cancelled:
            return 125
        try:
            request, credentials, handles = _recv(control, .1)
            break
        except TimeoutError:
            continue
    if credentials[0] != parent_pid:
        raise BoundaryError('launcher_parent_credentials_mismatch')
    _check_inherited_leases(request, handles)
    uid, gid = os.getuid(), os.getgid()
    _libcall('unshare', CLONE_FLAGS['user'])
    Path('/proc/self/setgroups').write_text('deny')
    Path('/proc/self/uid_map').write_text(f'{uid} {uid} 1\n')
    Path('/proc/self/gid_map').write_text(f'{gid} {gid} 1\n')
    _libcall('unshare', CLONE_FLAGS['mnt'] | CLONE_FLAGS['pid'] | CLONE_FLAGS['net'])
    _libcall('mount', None, b'/', None, 0x4000 | 0x40000, None)  # MS_REC|MS_PRIVATE
    _dumpable()
    launcher_pidfd = os.pidfd_open(os.getpid())
    gate_read, gate_write = os.pipe2(os.O_CLOEXEC)
    ready_read, ready_write = os.pipe2(os.O_CLOEXEC)
    pid = os.fork()
    if pid == 0:
        for lease_fd in handles:
            os.close(lease_fd)
        os.close(gate_write)
        os.close(ready_read)
        _pdeath(signal.SIGKILL)
        # Parent may have died before PR_SET_PDEATHSIG. A pre-fork pidfd
        # authenticates that race independently of getppid()==0 in a PIDNS.
        if _exited(launcher_pidfd):
            os._exit(126)
        os.write(ready_write, b'1')
        os.close(ready_write)
        if os.read(gate_read, 1) != b'G' or _exited(launcher_pidfd):
            os._exit(126)
        os.close(gate_read)
        os.close(launcher_pidfd)
        try:
            code = _inner(control, request)
        except BaseException as exc:
            with contextlib.suppress(Exception):
                _write(Path(request['audit_dir']) / 'inner-failure.json', {'error': type(exc).__name__ + ':' + str(exc)})
            code = 127
        os._exit(code)
    os.close(gate_read)
    os.close(ready_write)
    os.close(launcher_pidfd)
    init_fd = os.pidfd_open(pid)
    try:
        if not select.select([ready_read], [], [], 5)[0] or os.read(ready_read, 1) != b'1':
            raise BoundaryError('pid1_pdeath_setup_failed')
        _send(control, {'kind': 'spawned', 'launcher_pid': os.getpid()}, [init_fd])
        while True:
            if cancelled:
                raise BoundaryError('launcher_parent_died_before_gate')
            try:
                packet, peer, received = _recv(control, .1)
                break
            except TimeoutError:
                continue
        for handle in received:
            os.close(handle)
        if packet.get('kind') != 'spawn_ack' or peer[0] != parent_pid or cancelled:
            raise BoundaryError('pid1_gate_not_authorized')
        os.write(gate_write, b'G')
        os.close(gate_write)
        gate_write = -1
        # Only PID1 uses the socket now. Closing this duplicate makes EOF exact.
        control.close()
        while True:
            if cancelled:
                _signal_pidfd(init_fd, signal.SIGKILL)
            waited, status = os.waitpid(pid, os.WNOHANG)
            if waited:
                return os.waitstatus_to_exitcode(status) % 256
            time.sleep(.02)
    finally:
        if gate_write >= 0:
            os.close(gate_write)
        os.close(ready_read)
        if not _exited(init_fd):
            _signal_pidfd(init_fd, signal.SIGKILL)
        with contextlib.suppress(ChildProcessError):
            os.waitpid(pid, 0)
        _write(Path(request['audit_dir']) / 'launcher-cleanup.json', {
            'pid1_reaped': True, 'init_pidfd_exited': _exited(init_fd),
            'monotonic_ns': time.monotonic_ns(), 'launcher_pid': os.getpid()})
        for lease_fd in handles:
            os.close(lease_fd)  # backup keeper: close only after namespace exit
        os.close(init_fd)


def _inner(control, request):
    _libcall('mount', b'proc', b'/proc', b'proc', 2 | 4 | 8, None)  # NOSUID|NODEV|NOEXEC
    _loopback(enable=True)
    _drop_caps()
    def cancelled(*_):
        raise BoundaryError('inner_cancelled_by_supervisor')
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    _proof(control, 'ready')
    packet, _, fds = _recv(control, request['options']['startup_timeout_sec'])
    for fd in fds:
        os.close(fd)
    if packet.get('kind') != 'execute':
        raise BoundaryError('inner_execution_not_admitted')
    audit = Path(request['audit_dir'])
    config = request['config']
    if request['probe'] is not None:
        result, receipts = _fixture(control, request)
    else:
        _same_source(request['identity']['source_manifest'])
        from .environment import check_environment
        # Deliberately unmodified scanner/preflight, only in authenticated net+PID boundary.
        environment = check_environment(config)
        _write(audit / 'inner-preflight.json', {'pid_scope': _identity()['namespaces']['pid'],
                                               'environment': environment})
        if not environment.get('ok'):
            result, receipts = _blocked(str(environment.get('reason'))), []
            result['environment'] = environment
        else:
            from .topology_namespace import route_requested, run_isolated_topology
            if route_requested(config):
                # Only this real, guardian-authenticated PID1 path delegates.
                # Host/injected run_pair never enters a nested launcher.
                result = run_isolated_topology(config, Path(request['output_dir']) / 'inner-runtime',
                                               boundary_request=request)
            else:
                from .ros2_pair import run_pair
                result = run_pair(config, Path(request['output_dir']) / 'inner-runtime')
            receipts = [lease for event in result.get('events', [])
                        if event.get('event') == 'isolation_locks_acquired' for lease in event.get('locks', [])]
        _same_source(request['identity']['source_manifest'])
    inner_lease_record = {'pid_scope': _identity()['namespaces']['pid'],
                          'lock_root': config['lock_root'], 'receipts': receipts}
    nested = result.get('topology_namespace')
    if isinstance(nested, dict):
        # Numeric PID1 is ambiguous. The legacy record collector is outer;
        # leases on the new branch belong to the separately bound nested PID1.
        inner_lease_record.update(outer_pid_scope=inner_lease_record['pid_scope'],
            pid_scope=nested.get('raw_pid_scope'),
            nested_controller_binding=nested.get('nested_controller_binding'))
    _write(audit / 'inner-leases.json', inner_lease_record)
    path = audit / 'inner-runtime-result.json'
    _write(path, result)
    _send(control, {'kind': 'result', 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    return 0


def _fixture(control, request):
    audit, config, probe = Path(request['audit_dir']), request['config'], request['probe']
    if probe['mode'] == 'nested_pid_proc':
        return _fixture_nested_pid_proc(request)
    observations = []
    if probe['preflight_rmws']:
        from .environment import check_environment
        for rmw in probe['preflight_rmws']:
            environment = check_environment(dict(config, rmw=rmw))
            observations.append({'rmw': rmw, 'environment': environment})
        _write(audit / 'nonDDS-preflights.json', {'scope': 'isolated; no context/endpoint', 'results': observations})
    from .discovery_network import namespace_command
    command = namespace_command([sys.executable, '-B', '-c',
        'import json; from r2f.runtime.discovery_network import assert_isolated_namespace; '
        'print(json.dumps(assert_isolated_namespace(),sort_keys=True))'])
    nested = subprocess.run(command, capture_output=True, text=True, timeout=10, env=_python_env())
    _write(audit / 'nonDDS-nested-discovery.json', {'command': command, 'returncode': nested.returncode,
        'stdout': nested.stdout, 'stderr': nested.stderr, 'no_DDS': True})
    if nested.returncode:
        raise BoundaryError('nonDDS_nested_discovery_wrapper_failed:' + nested.stderr)
    with LeaseSet(Path(config['lock_root']), Path(request['output_dir']) / 'inner-runtime',
                  config['campaign_id'], config['domain_id']) as leases:
        _write(audit / 'fixture-inner-leases-held.json', {'receipts': leases.receipts,
            'pid_scope': _identity()['namespaces']['pid'], 'identity': _identity()})
        child = subprocess.Popen([sys.executable, '-B', '-m', MODULE, '_fixture_descendant',
                                  str(control.fileno())], pass_fds=(control.fileno(),), start_new_session=True)
        # Child sends its own credentials/pidfd, not an init-forwarded fake PID.
        deadline = time.monotonic() + 4
        marker = audit / 'fixture-tree-ready.json'
        while not marker.exists():
            if child.poll() is not None or time.monotonic() > deadline:
                raise BoundaryError('fixture_descendant_failed_to_start')
            time.sleep(.01)
        mode = probe['mode']
        if mode == 'crash':
            os._exit(73)
        if mode == 'controller_error':
            raise BoundaryError('intentional_nonDDS_controller_error')
        if mode == 'hang':
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                time.sleep(.1)
        result = _blocked(None)
        result.update(status='nonDDS_probe_complete', runtime_status='nonDDS_probe_complete',
                      evidence_purpose='nonDDS_execution_boundary_probe',
                      probe={'no_ROS_context': True, 'no_endpoint': True, 'no_SUT_candidate': True,
                             'no_execution_maturity_credit': True, 'preflights': observations})
        return result, leases.receipts


def _active_mount_id(path):
    """The kernel-selected mount, not the count/order of stacked mountinfo rows."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        fields = dict(line.split(':', 1) for line in Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines() if ':' in line)
        return fields['mnt_id'].strip()
    finally:
        os.close(fd)


def _nested_proc_scope(outer):
    """Kernel-only PID/proc checks shared by isolated controllers and fixed probe."""
    from .discovery_network import assert_isolated_namespace
    proof = assert_isolated_namespace()
    actual = _identity()
    if os.getpid() != 1 or actual['nspid'][-1] != 1:
        raise BoundaryError('nested_controller_not_pid1')
    for name in NAMESPACES:
        if actual['namespaces'][name] == outer['namespaces'][name]:
            raise BoundaryError('nested_namespace_not_independent:' + name)
    if os.readlink('/proc/1/ns/pid') != actual['namespaces']['pid'] or _ticks(1) != actual['start_ticks']:
        raise BoundaryError('nested_proc_not_pidns_scoped')
    rows = []
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left, sep, right = line.partition(' - ')
        if sep and left.split()[4] == '/proc':
            rows.append({'mount': left.split()[4], 'root': left.split()[3],
                         'fs_type': right.split()[0], 'mount_id': left.split()[0]})
    active_mount_id = _active_mount_id('/proc')
    active = [row for row in rows if row['mount_id'] == active_mount_id]
    if len(active) != 1 or active[0]['root'] != '/' or active[0]['fs_type'] != 'proc':
        raise BoundaryError('nested_private_proc_mount_missing:' + json.dumps(
            {'active_mount_id': active_mount_id, 'proc_mounts': rows}, sort_keys=True))
    return {'network': proof, 'identity': actual, 'proc_mounts': rows,
            'active_proc_mount_id': active_mount_id, 'pid1_proc_scoped': True}


def _fixed_probe_source(expected):
    current = _source()
    # Explicit nonDDS measurements, not a relaxation of _same_source/formal GO.
    for key in ('head', 'source_repo', 'behavior_tree_sha256'):
        if current.get(key) != expected.get(key):
            raise BoundaryError('nonDDS_nested_source_changed:' + key)
    return current


def _nested_pid_proc(request, fd):
    """Fixed nonDDS workload. No callable, worker command or extra mode accepted."""
    from .ros2_pair import verify_pair_lease
    from .topology_environment_observation import _netns_members, _KernelProbe
    control = socket.socket(fileno=fd)
    # Keep same-UID ancestor inspection possible; this is not capability removal.
    _dumpable()
    _write(Path(request['audit_dir']) / 'nested-preproof.json', {
        'identity': _identity(), 'proc_mountinfo': Path('/proc/self/mountinfo').read_text(),
        'active_proc_mount_id': _active_mount_id('/proc')})
    proof = _nested_proc_scope(request['outer'])
    _fixed_probe_source(request['source'])
    cfg, out = request['config'], Path(request['inner_output'])
    with LeaseSet(Path(cfg['lock_root']), out, cfg['campaign_id'], cfg['domain_id']) as leases:
        receipts = verify_pair_lease(leases, cfg, out)
        netns = proof['network']['observed_net_namespace']
        empty = _netns_members(netns)
        if [row['pid'] for row in empty] != [1]:
            raise BoundaryError('nested_empty_inventory_has_foreign_process')
        sleeper = subprocess.Popen([sys.executable, '-B', '-c',
            'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(15)'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, close_fds=True)
        members = _netns_members(netns)
        if {r['pid'] for r in members} != {1, sleeper.pid}:
            raise BoundaryError('nested_live_inventory_incomplete')
        for row in members:
            if row['namespaces']['pid'] != proof['identity']['namespaces']['pid']:
                raise BoundaryError('nested_inventory_pidns_mismatch')
        # Pure kernel/iproute2/nft readbacks, no packet generation or ROS context.
        readbacks = _KernelProbe().read_network()
        _fixed_probe_source(request['source'])
        _nested_proc_scope(request['outer'])
        packet = {'kind': 'nested_fixed_ready', 'proof': proof,
                  'empty_workload_inventory': empty, 'live_inventory': members,
                  'leases': receipts, 'readbacks': readbacks, 'no_DDS': True,
                  'sleeper_identity': _identity(sleeper.pid)}
        fds = [os.pidfd_open(os.getpid()), os.pidfd_open(sleeper.pid)]
        fds += [os.open('/proc/self/ns/' + name, os.O_RDONLY | os.O_CLOEXEC) for name in NAMESPACES]
        try:
            _send(control, packet, fds)
        finally:
            for handle in fds:
                os.close(handle)
        # PID1 must be killed, including the detached session child, by the
        # supervisor test. Never synthesize a successful restore or cleanup.
        time.sleep(15)
        raise BoundaryError('nested_fixed_probe_cleanup_not_requested')


def _bind_nested_probe(packet, credentials, fds, outer):
    """Independent parent binding. Never reuse the outer UID1000 authenticator."""
    if len(fds) != 6 or packet.get('kind') != 'nested_fixed_ready':
        raise BoundaryError('nested_fixed_proof_fd_count_or_kind')
    pid, uid, gid = credentials
    if pid != _pidfd_pid(fds[0]) or uid != 1000 or gid != os.getgid():
        raise BoundaryError('nested_fixed_credentials_pidfd_mismatch')
    actual = _identity(pid)
    reported = packet['proof']['identity']
    if (actual['uid'] != 1000 or actual['nspid'][-1] != 1
            or actual['start_ticks'] != reported['start_ticks']
            or actual['namespaces'] != reported['namespaces']
            or reported['uid'] != 0 or reported['pid'] != 1
            or [[int(v) for v in row.split()] for row in actual['uid_map'].splitlines()] != [[0, 1000, 1]]):
        raise BoundaryError('nested_fixed_kernel_identity_mismatch')
    for name, handle in zip(NAMESPACES, fds[2:]):
        local = os.stat(f'/proc/{pid}/ns/{name}')
        remote = os.fstat(handle)
        if (fcntl.ioctl(handle, 0xb703) != CLONE_FLAGS[name]
                or (local.st_dev, local.st_ino) != (remote.st_dev, remote.st_ino)
                or actual['namespaces'][name] == outer['namespaces'][name]):
            raise BoundaryError('nested_fixed_namespace_fd_mismatch:' + name)
    child = _identity(_pidfd_pid(fds[1]))
    if (child['namespaces'] != actual['namespaces']
            or child['start_ticks'] != packet['sleeper_identity']['start_ticks']
            or child['nspid'][-1] != packet['sleeper_identity']['pid']):
        raise BoundaryError('nested_fixed_sleeper_binding_mismatch')
    return {'authentication': 'SCM_CREDENTIALS+SCM_RIGHTS+pidfd+proc_start_ticks+NS_GET_NSTYPE',
            'controller_in_outer_pidns': actual, 'detached_child_in_outer_pidns': child}


def _fixture_nested_pid_proc(request):
    """One opt-in nonDDS child, using the unchanged outer gates and guardian."""
    from .discovery_network import namespace_command
    audit, cfg = Path(request['audit_dir']), request['config']
    outer = _identity()
    child_request = {'outer': outer, 'config': cfg, 'audit_dir': str(audit),
                     'inner_output': str(Path(request['output_dir']) / 'inner-runtime'),
                     'source': request['identity']['source_observed_not_credited']}
    parent, child = _pair()
    command = namespace_command([sys.executable, '-B', '-m', MODULE, '_nested_pid_proc',
                                 json.dumps(child_request, sort_keys=True), str(child.fileno())], pid_mount=True)
    path = audit / 'nonDDS-nested-pid-proc.json'
    record = {'command': command, 'no_DDS': True, 'outer': outer,
              'kernel_proof': 'unknown', 'inventory': 'unknown', 'empty_workload': 'unknown',
              'cleanup': {'confirmed': False}}
    proc, fds, packet, error = None, [], None, None
    try:
        with (audit / 'nested.stdout').open('xb') as stdout, (audit / 'nested.stderr').open('xb') as stderr:
            proc = subprocess.Popen(command, pass_fds=(child.fileno(),), start_new_session=True,
                stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, env=_python_env(), cwd=REPO)
        child.close()
        packet, credentials, fds = _recv(parent, min(6, request['options']['timeout_sec']))
        record['binding'] = _bind_nested_probe(packet, credentials, fds, outer)
        record['packet'] = packet
        record.update(kernel_proof='verified', inventory='verified', empty_workload='verified_nonDDS_only')
    except BaseException as exc:
        error = exc
        record['error'] = type(exc).__name__ + ':' + str(exc)
    finally:
        child.close()
        if proc is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=.3)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=3)
            # util-linux --kill-child and kernel PID1 teardown include escaped
            # sessions. Verify both prebound pidfds, not absence in an inner scan.
            deadline = time.monotonic() + 2
            while len(fds) == 6 and not all(_exited(handle) for handle in fds[:2]) and time.monotonic() < deadline:
                time.sleep(.01)
            record['cleanup'] = {'confirmed': len(fds) == 6 and all(_exited(handle) for handle in fds[:2]),
                'wrapper_returncode': proc.returncode, 'method': 'prebound_pidfds+unshare_kill_child+PIDNS_teardown',
                'controller_exited': bool(fds) and _exited(fds[0]),
                'detached_child_exited': len(fds) >= 2 and _exited(fds[1])}
            # Reap adopted descendants; no receipt or lease is rewritten.
            while True:
                try:
                    waited, _ = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break
                if not waited:
                    break
        _write(path, record)
        for handle in fds:
            os.close(handle)
        parent.close()
    if error is not None:
        raise error
    if record['cleanup']['confirmed'] is not True:
        raise BoundaryError('nested_fixed_probe_cleanup_unconfirmed')
    _fixed_probe_source(child_request['source'])
    result = _blocked(None)
    result.update(status='nonDDS_probe_complete', runtime_status='nonDDS_probe_complete',
        evidence_purpose='nonDDS_nested_pid_proc_probe',
        probe={'no_ROS_context': True, 'no_endpoint': True, 'no_SUT_candidate': True,
               'no_execution_maturity_credit': True, 'nested_pid_proc_path': str(path),
               'nested_pid_proc_sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    return result, packet['leases']


def _fixture_descendant(fd):
    control = socket.socket(fileno=fd)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _proof(control, 'fixture_descendant')
    if len(sys.argv) < 4:
        subprocess.Popen([sys.executable, '-B', '-m', MODULE, '_fixture_descendant', str(fd), 'grandchild'],
                         pass_fds=(fd,), start_new_session=True)
    # Guardian writes the ready marker only after independent authentication.
    while True:
        time.sleep(.1)



def _observe_owned_tree(root_pid, root_start, observations):
    """Supporting mapping only. Never scan unrelated host PIDs or signal them.

    Missed/short-lived descendants remain explicitly unobserved; kernel PIDNS
    teardown, not this sampling, is the cleanup proof.
    """
    pending, seen = [(root_pid, root_start, None, None)], set()
    while pending and len(seen) < 4096:
        pid, expected_start, parent_pid, parent_start = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        try:
            before = _ticks(pid)
            if expected_start is not None and before != expected_start:
                continue
            if parent_pid is not None and (_parent_of(pid) != parent_pid or _ticks(parent_pid) != parent_start):
                continue
            identity = _identity(pid)
            children = [int(x) for x in Path(f'/proc/{pid}/task/{pid}/children').read_text().split()]
            if _ticks(pid) != before:
                continue
            observations[(pid, before)] = dict(identity, inner_pid=identity['nspid'][-1],
                observed_monotonic_ns=time.monotonic_ns(), evidence='kernel_owned_child_traversal_not_host_clearance')
            pending.extend((child, None, pid, before) for child in children)
        except (OSError, ValueError, IndexError):
            continue


def _guardian(fd):
    control = socket.socket(fileno=fd)
    inherited, namespace_fds, descendants = [], [], []
    launcher, launcher_fd, init_fd, child_control = None, None, None, None
    controller, cleanup, error, request, result_hash = None, {'confirmed': False}, None, None, None
    owned_processes = {}
    parent_dead = False
    def on_parent_death(*_):
        nonlocal parent_dead
        parent_dead = True
    parent_pid = os.getppid()
    signal.signal(signal.SIGTERM, on_parent_death)
    signal.signal(signal.SIGINT, on_parent_death)
    _pdeath(signal.SIGTERM)
    if os.getppid() != parent_pid:
        parent_dead = True
    events = None
    def emit(event, **fields):
        if events:
            events.write(json.dumps({'event': event, 'monotonic_ns': time.monotonic_ns(), **fields}, sort_keys=True) + '\n')
            events.flush()
            os.fsync(events.fileno())
    try:
        request, credentials, inherited = _recv(control, 10)
        host = request['host']
        if credentials[0] != parent_pid or host['pid'] != parent_pid or _ticks(parent_pid) != host['start_ticks']:
            raise BoundaryError('supervisor_kernel_identity_mismatch')
        _check_inherited_leases(request, inherited)
        audit = Path(request['audit_dir'])
        events = (audit / 'guardian-events.jsonl').open('x')
        emit('host_lease_copies_authenticated', owner=host, guardian=_identity(), receipts=request['host_leases'])
        parent_control, child_control = _pair()
        with (audit / 'launcher.stdout').open('xb') as stdout, (audit / 'launcher.stderr').open('xb') as stderr:
            launcher = subprocess.Popen([sys.executable, '-B', '-m', MODULE, '_launcher', str(child_control.fileno())],
                pass_fds=(child_control.fileno(),), stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                cwd=REPO, env=_python_env(), start_new_session=True)
        launcher_fd = os.pidfd_open(launcher.pid)
        emit('owned_launcher_started', host_pid=launcher.pid, start_ticks=_ticks(launcher.pid))
        child_control.close()
        child_control = parent_control
        _guardian_watch(control, 'launcher_watch', launcher_fd, host['pid'])
        _send(child_control, request, inherited)
        begun, ready_at, next_source = time.monotonic(), None, 0
        options = request['options']
        while True:
            now = time.monotonic()
            if parent_dead:
                raise BoundaryError('supervisor_parent_died')
            if now - begun > options['timeout_sec']:
                raise TimeoutError('boundary_runtime_timeout')
            if controller is None and now - begun > options['startup_timeout_sec']:
                raise TimeoutError('boundary_startup_timeout')
            if request['probe'] is None and now >= next_source:
                _same_source(request['identity']['source_manifest'])
                next_source = now + 1
            if controller is not None and not _exited(init_fd):
                _observe_owned_tree(controller['host']['pid'], controller['host']['start_ticks'], owned_processes)
            readable = select.select([control, child_control], [], [], .05)[0]
            if control in readable:
                packet, peer, received = _recv(control)
                for handle in received:
                    os.close(handle)
                if peer[0] != host['pid'] or packet.get('kind') != 'cancel':
                    raise BoundaryError('invalid_supervisor_control')
                raise BoundaryError('supervisor_cancel:' + str(packet.get('reason')))
            if child_control in readable:
                try:
                    packet, peer, received = _recv(child_control)
                except EOFError:
                    break
                kind = packet.get('kind')
                if kind == 'spawned':
                    try:
                        if peer[0] != launcher.pid or len(received) != 1 or init_fd is not None:
                            raise BoundaryError('launcher_spawn_credentials_invalid')
                        child_pid = _pidfd_pid(received[0])
                        if _parent_of(child_pid) != launcher.pid:
                            raise BoundaryError('pidfd_not_launcher_child')
                        init_fd, received = received[0], []
                        emit('pid1_kernel_bound_before_gate', host_pid=child_pid, start_ticks=_ticks(child_pid))
                        _guardian_watch(control, 'pid1_watch', init_fd, host['pid'])
                        _send(child_control, {'kind': 'spawn_ack'})
                    finally:
                        for handle in received:
                            os.close(handle)
                elif kind in ('ready', 'fixture_descendant'):
                    try:
                        if kind == 'ready':
                            if controller is not None or init_fd is None or peer[0] != _pidfd_pid(init_fd):
                                raise BoundaryError('duplicate_or_unbound_controller')
                            binding = _authenticate(packet, peer, received, host, launcher_pid=launcher.pid)
                            controller, ready_at = binding, now
                            namespace_fds.extend(received)
                            received = []
                            emit('independent_boundary_authenticated', binding=binding)
                            _send(child_control, {'kind': 'execute'})
                            emit('execute_authorized', config_sha256=request['inner_config_sha256'])
                        else:
                            if request['probe'] is None or controller is None:
                                raise BoundaryError('fixture_message_in_formal_runtime')
                            binding = _authenticate(packet, peer, received, host, init_pid=controller['host']['pid'])
                            descendants.append({'binding': binding, 'pidfd': received[0]})
                            namespace_fds.extend(received[1:])
                            received = []
                            emit('fixture_descendant_authenticated', binding=binding)
                            _write(audit / f"fixture-descendant-{binding['inner_pid']}-ready.json", binding)
                            if len(descendants) == 1:
                                _write(audit / 'fixture-descendant-ready.json', binding)
                            if len(descendants) == 2:
                                _write(audit / 'fixture-tree-ready.json', {'bindings': [d['binding'] for d in descendants]})
                    finally:
                        for handle in received:
                            os.close(handle)
                elif kind == 'result':
                    for handle in received:
                        os.close(handle)
                    if controller is None or peer[0] != controller['host']['pid'] or result_hash is not None:
                        raise BoundaryError('unbound_or_duplicate_runtime_result')
                    result_hash = packet['sha256']
                    emit('runtime_result_received', sha256=result_hash)
                else:
                    for handle in received:
                        os.close(handle)
                    raise BoundaryError('unknown_child_control_message')
            if launcher.poll() is not None and not readable:
                break
        if launcher.wait(timeout=options['kill_wait_sec']) != 0:
            raise BoundaryError('namespace_init_or_launcher_failed:' + str(launcher.returncode))
        if controller is None or result_hash is None:
            raise BoundaryError('namespace_exited_without_authenticated_result')
    except BaseException as exc:
        error = type(exc).__name__ + ':' + str(exc)
        emit('boundary_failure', error=error)
    finally:
        if request:
            options = request['options']
            emit('cleanup_begin', error=error)
            if init_fd is not None:
                if not _exited(init_fd):
                    _signal_pidfd(init_fd, signal.SIGTERM)
                    emit('owned_pid1_term')
                    select.select([init_fd], [], [], options['term_grace_sec'])
                if not _exited(init_fd):
                    _signal_pidfd(init_fd, signal.SIGKILL)
                    emit('owned_pid1_kill')
                    select.select([init_fd], [], [], options['kill_wait_sec'])
                cleanup['pid1_exited'] = _exited(init_fd)
            else:
                cleanup['pid1_exited'] = None
            if launcher is not None:
                if init_fd is None and launcher.poll() is None:
                    # Trusted launcher handles TERM by killing/reaping its gated
                    # init; no backend is admitted before pidfd authentication.
                    _signal_pidfd(launcher_fd, signal.SIGTERM)
                    emit('owned_launcher_term_before_admission')
                try:
                    launcher.wait(timeout=options['kill_wait_sec'])
                except subprocess.TimeoutExpired:
                    # Do not kill a lease keeper or unlock on an unproved exit.
                    error = (error or '') + ';launcher_cleanup_unconfirmed'
                cleanup['launcher_exited'] = launcher.poll() is not None
                cleanup['launcher_returncode'] = launcher.poll()
            else:
                cleanup['launcher_exited'] = True
            cleanup['descendants'] = [{'binding': item['binding'], 'pidfd_exited': _exited(item['pidfd'])}
                                      for item in descendants]
            cleanup['confirmed'] = (cleanup['launcher_exited'] and
                (cleanup['pid1_exited'] is True or (init_fd is None and controller is None)) and
                all(item['pidfd_exited'] for item in cleanup['descendants']))
            cleanup['method'] = 'pidfd-bound PID1 exit + launcher wait; kernel PIDNS teardown, not host PID scan'
            cleanup['monotonic_ns'] = time.monotonic_ns()
            emit('cleanup_observed', receipt=cleanup)
            receipts_path = Path(request['audit_dir']) / 'inner-leases.json'
            fixture_path = Path(request['audit_dir']) / 'fixture-inner-leases-held.json'
            receipts = json.loads(receipts_path.read_text()).get('receipts', []) if receipts_path.exists() else (
                json.loads(fixture_path.read_text()).get('receipts', []) if fixture_path.exists() else [])
            manifest = {'schema': SCHEMA, 'boundary_id': request['boundary_id'], 'identity': request['identity'],
                'host_supervisor': request['host'], 'guardian': _identity(), 'controller_binding': controller,
                'host_leases': request['host_leases'], 'inner_lock_root': request['config']['lock_root'],
                'owned_process_observations': list(owned_processes.values()),
                'owned_process_observation_limitation': 'sampled supporting mapping, not complete lifecycle/host clearance',
                'inner_leases': receipts, 'cleanup': cleanup, 'error': error,
                'runtime_result_sha256': result_hash, 'nonDDS': request['probe'] is not None,
                'host_clearance_claimed': False, 'clearance_scope': request['clearance_scope'],
                'source_freeze_credit': False if request['probe'] else request['identity']['source_manifest']['source_frozen'],
                'no_new_scope_or_maturity_denominator': True,
                'source_after': _source(),
                'artifact_hashes': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in Path(request['audit_dir']).iterdir() if p.is_file()
                    and p.name != 'guardian-events.jsonl'}}
            path = Path(request['audit_dir']) / 'manifest.json'
            _write(path, manifest)
            with contextlib.suppress(OSError):
                _send(control, {'kind': 'complete', 'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            if not cleanup['confirmed']:
                # Fail closed: this detached guardian retains the inherited
                # flocks until the exact bound process exits, even if caller dies.
                emit('lease_release_deferred_until_proven_exit')
                while ((init_fd is not None and not _exited(init_fd)) or
                       (launcher is not None and launcher.poll() is None) or
                       any(not _exited(item['pidfd']) for item in descendants)):
                    time.sleep(.1)
                _write(Path(request['audit_dir']) / 'cleanup-recovered.json', {
                    'confirmed': True, 'monotonic_ns': time.monotonic_ns(),
                    'pid1_exited': init_fd is None or _exited(init_fd),
                    'launcher_exited': launcher is None or launcher.poll() is not None})
            emit('guardian_lease_copies_closing_after_cleanup')
        if events:
            events.close()
        for handle in inherited + namespace_fds + [item['pidfd'] for item in descendants]:
            with contextlib.suppress(OSError):
                os.close(handle)  # Never LOCK_UN an inherited OFD.
        for handle in (init_fd, launcher_fd):
            if handle is not None:
                os.close(handle)
        if child_control:
            child_control.close()
        control.close()
    return 0 if not error else 2


def _main():
    if sys.argv[1] == '_nested_pid_proc':
        return _nested_pid_proc(json.loads(sys.argv[2]), int(sys.argv[3]))
    action, fd = sys.argv[1], int(sys.argv[2])
    return {'_guardian': _guardian, '_launcher': _launcher,
            '_fixture_descendant': _fixture_descendant}[action](fd)


if __name__ == '__main__':
    raise SystemExit(_main())
