"""Pure offline ctypes/public-getter mocks. No dlopen, ROS, DDS or build."""
from __future__ import annotations

import copy
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.runtime import fastdds_peer_observer as b

REF = {'observation_mode': 'instrumentation', 'manifest_path': '/isolated/manifest.json',
       'manifest_sha256': 'a' * 64}
WRITER = '010f7f014c00961e0000000000000603'
READER = '010f7f014c00961e0000000000000704'
MAPS = [{'role': role, 'path': '/isolated/' + role + '.so', 'sha256': role[0] * 64,
         'device': '00:2a', 'inode': i + 1} for i, role in enumerate(('vendor', 'provider'))]


class Handle:
    __module__ = 'rclpy._rclpy_pybind11'

    def __init__(self, pointer=0x1000):
        self.value = pointer
        self.active = False
        self.entries = self.exits = 0
        self.fail_exit = False

    def __enter__(self):
        assert not self.active
        self.active = True
        self.entries += 1
        # Actual installed Destroyable.__enter__ returns None, not the handle.
        return None

    def __exit__(self, *_):
        assert self.active
        self.active = False
        self.exits += 1
        if self.fail_exit:
            raise RuntimeError('synthetic_handle_exit_failure')

    @property
    def pointer(self):
        assert self.active, 'pointer may only be borrowed inside with handle'
        return self.value


class Publisher(Handle):
    __module__ = 'rclpy._rclpy_pybind11'


class Subscription(Handle):
    __module__ = 'rclpy._rclpy_pybind11'


class Native:
    def __init__(self, endpoint, kind):
        self.endpoint, self.kind = endpoint, kind
        self.ordinal = 0
        self.peers = [READER if kind == 'writer' else WRITER]
        self.calls, self.chains = [], []
        self.reset_count = self.validations = 0
        self.hook = None
        self.validation_hook = None
        self.rcl_getters = {kind: self.rcl}
        self.native_getters = {kind: self.native}
        self.snapshots = {kind: self.snapshot}

    def revalidate(self):
        self.validations += 1
        if self.validation_hook:
            self.validation_hook(self.validations)
        return {'mapped_libraries': copy.deepcopy(MAPS),
                'library_provenance': {'fixture': True, 'execution_evidence': False}}

    def rcl(self, pointer):
        assert self.endpoint.handle.active
        self.chains.append(('rcl', pointer))
        return pointer + 0x100

    def native(self, pointer):
        assert self.endpoint.handle.active
        self.chains.append(('native', pointer))
        return pointer + 0x100

    def reset_error(self):
        self.reset_count += 1

    def snapshot(self, pointer, buffer, capacity, output, size):
        assert self.endpoint.handle.active
        assert size == 64
        self.ordinal += 1
        self.calls.append((pointer, capacity))
        out = C.cast(output, C.POINTER(b.Result)).contents
        out.schema_version, out.struct_size, out.endpoint_kind = 1, 64, 1 if self.kind == 'writer' else 2
        out.local_guid[:] = bytes.fromhex(WRITER if self.kind == 'writer' else READER)
        out.snapshot_ordinal = self.ordinal
        peers = self.peers
        out.required_count = len(peers)
        out.status = 4 if len(peers) > capacity else 0
        if not out.status:
            for i, gid in enumerate(peers):
                buffer[i * 16:(i + 1) * 16] = bytes.fromhex(gid)
            out.copied, out.complete = len(peers), 1
        if self.hook:
            return self.hook(out, buffer, capacity, len(self.calls))
        return out.status


@pytest.fixture
def make_observer(monkeypatch):
    def make(kind='writer'):
        endpoint = SimpleNamespace(handle=(Publisher() if kind == 'writer' else Subscription()))
        native = Native(endpoint, kind)
        monkeypatch.setattr(b, '_Bindings', lambda reference: native)
        obj = b.FastddsMatchedPeerObserver(endpoint, instrumentation=REF)
        return obj, native, endpoint
    return make


def test_schema_constant_and_result_layout():
    assert b.FastddsMatchedPeerObserver.SCHEMA == 'r2f.fastdds-native-matched-peer-observation/v1'
    assert C.sizeof(b.Result) == 64
    assert {key: getattr(b.Result, key).offset for key in b.EXPECTED_OFFSETS} == b.EXPECTED_OFFSETS


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_single_complete_typed_snapshot_and_receipt(make_observer, kind):
    obj, native, endpoint = make_observer(kind)
    local = WRITER if kind == 'writer' else READER
    observation = obj.sample(152, local, kind)
    assert observation['complete'], observation
    assert observation['schema'] == obj.SCHEMA
    assert observation['manifest_reference'] == REF
    assert observation['observation_mode'] == 'instrumentation'
    assert observation['snapshot_ordinal_basis'] == b.ORDINAL_BASIS
    assert observation['membership_basis'] == b.MEMBERSHIP_BASIS
    assert observation['local_gid'] == local
    assert observation['role_kind'] == kind
    assert observation['domain_id'] == 152
    assert observation['matched_peer_gids'] == native.peers
    assert observation['mapped_libraries'] == MAPS
    assert observation['native_result'] == {
        'schema_version': 1, 'struct_size': 64, 'status': 0,
        'endpoint_kind': 1 if kind == 'writer' else 2, 'local_guid': local,
        'required_count': 1, 'copied': 1, 'snapshot_ordinal': 1, 'complete': 1, 'reserved': 0}
    assert len(bytes.fromhex(observation['native_result_hex'])) == 64
    assert observation['started_monotonic_ns'] <= observation['ended_monotonic_ns']
    assert len(native.calls) == 1 and native.validations == 2
    assert endpoint.handle.entries == endpoint.handle.exits == 1
    assert len(native.chains) == 4  # resolve before/after, never a second snapshot
    json.dumps(observation)


def test_complete_empty_is_native_success_not_unavailable(make_observer):
    obj, native, _ = make_observer()
    native.peers = []
    row = obj.sample(0, WRITER, 'writer')
    assert row['complete'] and row['matched_peer_gids'] == []
    assert row['native_result']['complete'] == 1
    assert row['native_result']['required_count'] == 0
    assert len(native.calls) == 1


@pytest.mark.parametrize('input_kind,domain,gid', [
    ('writer', -1, WRITER), ('writer', 232, WRITER), ('writer', True, WRITER),
    ('writer', '152', WRITER), ('reader', 152, WRITER), ('writer', 152, READER),
    ('publisher', 152, WRITER), ('writer', 152, '0' * 32), ('writer', 152, WRITER.upper()),
    ('writer', 152, 'z' * 32), ('writer', 152, WRITER[:-1]),
    ('writer', 152, '0' * 24 + '00000603'),
    ('writer', 152, WRITER[:24] + '00000003'),
])
def test_invalid_identity_has_no_native_call(make_observer, input_kind, domain, gid):
    obj, native, _ = make_observer()
    row = obj.sample(domain, gid, input_kind)
    assert not row['complete'] and 'matched_peer_gids' not in row
    assert not native.calls


@pytest.mark.parametrize('status', [1, 2, 3, 5, 6, -1, 7])
def test_all_nonretry_statuses_are_incomplete_never_empty(make_observer, status):
    obj, native, endpoint = make_observer()

    def fail(out, *_):
        out.status, out.complete, out.copied = status, 0, 0
        return status

    native.hook = fail
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and 'matched_peer_gids' not in row
    assert row['native_result']['status'] == status
    assert len(native.calls) == 1
    assert endpoint.handle.entries == endpoint.handle.exits == 1


@pytest.mark.parametrize('field,value', [
    ('schema_version', 2), ('struct_size', 63), ('status', 3),
    ('endpoint_kind', 2), ('reserved', 1), ('complete', 2),
    ('complete', 0), ('required_count', 2), ('copied', 0),
    ('snapshot_ordinal', 0), ('required_count', 257),
])
def test_corrupt_native_receipt_not_accepted(make_observer, field, value):
    obj, native, _ = make_observer()

    def corrupt(out, *_):
        setattr(out, field, value)
        return 0

    native.hook = corrupt
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'], row
    assert 'matched_peer_gids' not in row
    assert len(native.calls) == 1


def test_native_local_gid_mismatch_not_accepted(make_observer):
    obj, native, _ = make_observer()

    def wrong(out, *_):
        out.local_guid[:] = bytes.fromhex(WRITER[:24] + '00000803')
        return 0

    native.hook = wrong
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and 'matched_peer_gids' not in row


@pytest.mark.parametrize('peers', [[READER, READER], [WRITER], ['0' * 32],
                                  [READER[:24] + '00000004'], [READER[:30] + '01']])
def test_invalid_or_duplicate_peer_gid_is_not_proof(make_observer, peers):
    obj, native, _ = make_observer()
    native.peers = peers
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and 'matched_peer_gids' not in row


def test_short_buffer_retry_uses_only_final_snapshot(make_observer):
    obj, native, _ = make_observer()

    def changing(out, buffer, capacity, call):
        if call == 1:
            out.status, out.complete, out.copied, out.required_count = 4, 0, 0, 9
            buffer[:16] = bytes.fromhex(READER)  # discarded garbage, NOT a partial list
            return 4
        fresh = READER[:24] + '00009904'
        buffer[:16] = bytes.fromhex(fresh)
        out.required_count = out.copied = out.complete = 1
        return 0

    native.hook = changing
    row = obj.sample(152, WRITER, 'writer')
    assert row['complete'], row
    assert row['matched_peer_gids'] == [READER[:24] + '00009904']
    assert [capacity for _, capacity in native.calls] == [8, 16]
    assert len([a for a in row['native_attempts'] if a['return_status'] == 0]) == 1
    assert row['native_result']['snapshot_ordinal'] == 2


@pytest.mark.parametrize('fault', ['copied', 'complete', 'count', 'bound', 'exhausted'])
def test_malformed_or_unbounded_short_buffer_never_succeeds(make_observer, fault):
    obj, native, _ = make_observer()

    def short(out, _buffer, capacity, _call):
        out.status, out.complete, out.copied, out.required_count = 4, 0, 0, capacity + 1
        if fault == 'copied':
            out.copied = 1
        elif fault == 'complete':
            out.complete = 1
        elif fault == 'count':
            out.required_count = capacity
        elif fault == 'bound':
            out.required_count = 257
        return 4

    native.hook = short
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and 'matched_peer_gids' not in row
    assert len(native.calls) == (3 if fault == 'exhausted' else 1)
    assert all(capacity <= obj.MAX_PEERS for _, capacity in native.calls)


def test_current_handle_and_native_chain_refreshed_after_recreation(make_observer):
    obj, native, endpoint = make_observer('reader')
    old = endpoint.handle
    first = obj.sample(152, READER, 'reader')
    endpoint.handle = Subscription(0x5000)
    second = obj.sample(152, READER, 'reader')
    assert first['complete'] and second['complete']
    assert [pointer for pointer, _ in native.calls] == [0x1200, 0x5200]
    assert old.entries == old.exits == endpoint.handle.entries == endpoint.handle.exits == 1
    assert all(not isinstance(value, Handle) for value in vars(obj).values())


def test_same_rcl_handle_requeries_public_native_getter(make_observer):
    obj, native, _ = make_observer()
    assert obj.sample(152, WRITER, 'writer')['complete']
    native.native_getters['writer'] = lambda rmw: rmw + 0x900
    assert obj.sample(152, WRITER, 'writer')['complete']
    assert [pointer for pointer, _ in native.calls] == [0x1200, 0x1A00]


def test_native_changes_during_capture_reject_receipt(make_observer):
    obj, native, _ = make_observer()

    def mutate(out, *_):
        native.native_getters['writer'] = lambda rmw: rmw + 0x800
        return 0

    native.hook = mutate
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and row['reason'] == 'native_endpoint_changed_during_sample'
    assert 'matched_peer_gids' not in row


@pytest.mark.parametrize('boundary', ['rcl', 'rmw'])
def test_public_getter_failure_resets_error_and_never_calls_provider(make_observer, boundary):
    obj, native, endpoint = make_observer()
    getters = native.rcl_getters if boundary == 'rcl' else native.native_getters
    getters['writer'] = lambda _: None
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and not native.calls
    assert native.reset_count == 1
    assert endpoint.handle.entries == endpoint.handle.exits == 1


def test_bad_handle_role_rejected_before_any_pointer_dereference(make_observer):
    obj, native, endpoint = make_observer()
    endpoint.handle = Subscription()
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and not native.calls and not native.chains


@pytest.mark.parametrize('fault', ['exit', 'native', 'maps_after'])
def test_exceptions_clean_up_handle_without_fallback_or_peer_proof(make_observer, fault):
    obj, native, endpoint = make_observer()
    if fault == 'exit':
        endpoint.handle.fail_exit = True
    elif fault == 'native':
        def fail(*_):
            raise RuntimeError('synthetic_native_exception')
        native.snapshots['writer'] = fail
    else:
        def fail_validation(n):
            if n == 2:
                raise ValueError('mapped_binary_identity_mismatch')
        native.validation_hook = fail_validation
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and 'matched_peer_gids' not in row
    assert len(native.calls) <= 1
    assert endpoint.handle.entries == endpoint.handle.exits == 1
    assert not endpoint.handle.active


def test_serial_reuse_is_not_fresh_observation_or_generation(make_observer):
    obj, native, _ = make_observer()
    assert obj.sample(152, WRITER, 'writer')['complete']
    native.ordinal = 0
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and 'serial_mismatch' in row['reason']
    assert row['snapshot_ordinal_basis'] == b.ORDINAL_BASIS


@pytest.mark.parametrize('reference', [None, {}, {'observation_mode': 'blackbox'},
    {**REF, 'manifest_path': 'relative'}, {**REF, 'manifest_sha256': 'A' * 64},
    {**REF, 'extra': 1}])
def test_reference_invalid_before_loader_or_endpoint_access(reference, monkeypatch):
    def forbidden(*_):
        raise AssertionError('must_not_load')
    monkeypatch.setattr(b.C, 'CDLL', forbidden)
    with pytest.raises(ValueError, match='explicit_fastdds_peer'):
        b.FastddsMatchedPeerObserver(None, instrumentation=reference)


def test_reference_is_copied_and_not_mutable_via_receipt(make_observer):
    obj, native, _ = make_observer()
    row = obj.sample(152, WRITER, 'writer')
    row['manifest_reference']['manifest_sha256'] = '0' * 64
    assert obj.sample(152, WRITER, 'writer')['manifest_reference'] == REF


def test_one_writer_two_readers_full_native_list(make_observer):
    obj, native, _ = make_observer()
    native.peers = [READER, READER[:24] + '00000804']
    row = obj.sample(231, WRITER, 'writer')
    assert row['complete'] and row['matched_peer_gids'] == native.peers
    assert row['native_result']['required_count'] == row['native_result']['copied'] == 2
    assert len(native.calls) == 1


class Function:
    def __init__(self, action=lambda *_: None):
        self.action = action
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)
        return self.action(*args)


def abi_manifest():
    return {
        'binaries': {role: {'path': '/isolated/' + role + '.so', 'sha256': 'b' * 64}
                     for role in ('vendor', 'provider')},
        'provider_source_sha256': 'c' * 64,
        'patch': {'abi': {
            'schema_version': 1, 'result_size': 64, 'guid_size': 16,
            'layout_changed': False, 'virtual_slots_added': False,
            'snapshot_ordinal_basis': b.ORDINAL_BASIS, 'membership_basis': b.MEMBERSHIP_BASIS,
            'statuses': {name: status for status, name in b.STATUS_NAMES.items()},
            'functions': ['r2f_fastdds_peer_writer_v1', 'r2f_fastdds_peer_reader_v1'],
        }},
    }


@pytest.fixture
def loader_boundary(monkeypatch):
    manifest = abi_manifest()
    events, libraries = [], {}
    for name in ('rcl', 'rmw_fastrtps_cpp', 'rcutils', 'provider'):
        path = '/isolated/provider.so' if name == 'provider' else str(b.PREFIX / ('lib/lib' + name + '.so'))
        libraries[path] = SimpleNamespace()
    rcl = libraries[str(b.PREFIX / 'lib/librcl.so')]
    rmw = libraries[str(b.PREFIX / 'lib/librmw_fastrtps_cpp.so')]
    rcutils = libraries[str(b.PREFIX / 'lib/librcutils.so')]
    provider = libraries['/isolated/provider.so']
    for name in ('rcl_publisher_get_rmw_handle', 'rcl_subscription_get_rmw_handle'):
        setattr(rcl, name, Function(lambda _: 0x2000))
    for name in b.GETTER_SYMBOLS.values():
        setattr(rmw, name, Function(lambda _: 0x3000))
    rmw.rmw_get_implementation_identifier = Function(lambda: b.RMW_IDENTIFIER)
    rcutils.rcutils_reset_error = Function()
    for kind in ('writer', 'reader'):
        setattr(provider, 'r2f_fastdds_peer_' + kind + '_v1', Function())

    def load(path, **kwargs):
        events.append(('load', path))
        assert kwargs['mode'] == os.RTLD_LOCAL
        return libraries[path]

    def vendor_check(ref):
        assert ref == REF
        events.append(('vendor_mapping', None))
        return {'identity_verified': True}

    monkeypatch.delenv('LD_PRELOAD', raising=False)
    monkeypatch.delenv('LD_AUDIT', raising=False)
    monkeypatch.setattr(b, 'manifest_for_reference', lambda ref: copy.deepcopy(manifest))
    monkeypatch.setattr(b, 'verify_abi', lambda: {'fixture_abi': True})
    monkeypatch.setattr(b, 'verify_vendor_mapping', vendor_check)
    monkeypatch.setattr(b, '_verify_api_maps', lambda **kw: [])
    monkeypatch.setattr(b, 'verify_mapped_binaries', lambda manifest: copy.deepcopy(MAPS))
    monkeypatch.setattr(b.C, 'CDLL', load)
    return SimpleNamespace(manifest=manifest, libraries=libraries, events=events,
                           rcl=rcl, rmw=rmw, provider=provider)


def test_real_binding_code_uses_public_rcl_and_vendor_direct_symbols(loader_boundary):
    f = loader_boundary
    native = b._Bindings(REF)
    assert f.events[0][0] == 'vendor_mapping'
    assert [event[1] for event in f.events if event[0] == 'load'] == [
        str(b.PREFIX / 'lib/librcl.so'), str(b.PREFIX / 'lib/librmw_fastrtps_cpp.so'),
        str(b.PREFIX / 'lib/librcutils.so'), '/isolated/provider.so']
    assert len(f.rmw.rmw_get_implementation_identifier.calls) == 1
    for kind in ('writer', 'reader'):
        assert native.rcl_getters[kind].argtypes == [C.c_void_p]
        assert native.rcl_getters[kind].restype is C.c_void_p
        assert native.native_getters[kind] is getattr(f.rmw, b.GETTER_SYMBOLS[kind])
        assert native.native_getters[kind].restype is C.c_void_p
        assert native.snapshots[kind].argtypes == [
            C.c_void_p, C.POINTER(C.c_uint8), C.c_uint64, C.POINTER(b.Result), C.c_uint32]
        assert native.snapshots[kind].restype is C.c_int32
        assert not native.snapshots[kind].calls  # init must not sample any endpoint
    assert native.revalidate()['mapped_libraries'] == MAPS


@pytest.mark.parametrize('fault', ['vendor_pre', 'api_pre', 'manifest', 'header'])
def test_preflight_failures_before_native_load(loader_boundary, monkeypatch, fault):
    def fail(*_, **__):
        raise ValueError('synthetic_' + fault)
    key = {'vendor_pre': 'verify_vendor_mapping', 'api_pre': '_verify_api_maps',
           'manifest': 'manifest_for_reference', 'header': 'verify_abi'}[fault]
    monkeypatch.setattr(b, key, fail)
    with pytest.raises(ValueError, match='synthetic_' + fault):
        b._Bindings(REF)
    assert not [event for event in loader_boundary.events if event[0] == 'load']


@pytest.mark.parametrize('fault', ['missing_symbol', 'load_failure', 'identifier', 'maps_after'])
def test_loader_symbol_identifier_or_postload_maps_failure_explicit(loader_boundary, monkeypatch, fault):
    f = loader_boundary
    if fault == 'missing_symbol':
        delattr(f.rmw, b.GETTER_SYMBOLS['reader'])
    elif fault == 'load_failure':
        def fail(*_, **__):
            raise OSError('synthetic_dlopen_failure')
        monkeypatch.setattr(b.C, 'CDLL', fail)
    elif fault == 'identifier':
        f.rmw.rmw_get_implementation_identifier = Function(lambda: b'rmw_cyclonedds_cpp')
    else:
        def fail(*_):
            raise ValueError('synthetic_maps_after_load')
        monkeypatch.setattr(b, 'verify_mapped_binaries', fail)
    with pytest.raises(ValueError):
        b._Bindings(REF)
    assert not f.provider.r2f_fastdds_peer_writer_v1.calls
    assert not f.provider.r2f_fastdds_peer_reader_v1.calls


@pytest.mark.parametrize('key,value', [
    ('schema_version', 2), ('result_size', 63), ('guid_size', 24),
    ('layout_changed', True), ('virtual_slots_added', True),
    ('snapshot_ordinal_basis', 'match_epoch'), ('membership_basis', 'delivery_inferred'),
    ('schema_version', True), ('layout_changed', 0),
    ('statuses', {'ok': 0}), ('functions', ['r2f_fastdds_peer_writer_v1']),
])
def test_manifest_abi_mismatch_rejected_before_loading(loader_boundary, key, value):
    loader_boundary.manifest['patch']['abi'][key] = value
    with pytest.raises(ValueError, match='unsupported_provider_abi_manifest'):
        b._Bindings(REF)
    assert not [event for event in loader_boundary.events if event[0] == 'load']


def test_manifest_status_bool_not_an_integer(loader_boundary):
    loader_boundary.manifest['patch']['abi']['statuses']['ok'] = False
    with pytest.raises(ValueError, match='unsupported_provider_abi_manifest'):
        b._Bindings(REF)


def test_each_revalidation_rechecks_reference_and_pins(loader_boundary, monkeypatch):
    native = b._Bindings(REF)
    checks = []

    def manifest_again(ref):
        checks.append('manifest')
        assert ref == REF
        return copy.deepcopy(loader_boundary.manifest)

    monkeypatch.setattr(b, 'manifest_for_reference', manifest_again)
    monkeypatch.setattr(b, 'verify_abi', lambda: checks.append('pins') or {})
    monkeypatch.setattr(b, 'verify_vendor_mapping', lambda ref: checks.append('vendor_maps') or {})
    monkeypatch.setattr(b, '_verify_api_maps', lambda **kw: checks.append('api_maps') or [])
    monkeypatch.setattr(b, 'verify_mapped_binaries', lambda m: checks.append('provider_maps') or MAPS)
    native.revalidate()
    assert checks == ['manifest', 'pins', 'vendor_maps', 'api_maps', 'provider_maps']
    loader_boundary.manifest['binaries']['vendor']['sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='selected_binary_identity_changed'):
        native.revalidate()


@pytest.mark.parametrize('hook', ['LD_PRELOAD', 'LD_AUDIT'])
def test_no_untracked_loader_hooks(loader_boundary, monkeypatch, hook):
    monkeypatch.setenv(hook, '/unexpected/library.so')
    with pytest.raises(ValueError, match='instrumentation_untracked_loader_hook'):
        b._Bindings(REF)
    assert not loader_boundary.events


def test_abi_pin_checks_are_file_reads_only(tmp_path, monkeypatch):
    path = tmp_path / 'header.h'
    path.write_bytes(b'pinned header\n')
    monkeypatch.setattr(b, 'PREFIX', tmp_path)
    monkeypatch.setattr(b, 'API_PINS', {'header.h': hashlib.sha256(path.read_bytes()).hexdigest()})
    assert b.verify_abi()['result_size'] == 64
    path.write_bytes(b'changed')
    with pytest.raises(ValueError, match='public_api_abi_hash_mismatch:header.h'):
        b.verify_abi()


def test_result_layout_mismatch_before_loading(monkeypatch):
    monkeypatch.setattr(b, 'EXPECTED_OFFSETS', {**b.EXPECTED_OFFSETS, 'complete': 60})
    with pytest.raises(ValueError, match='ctypes_result_layout_mismatch'):
        b.verify_abi()


@pytest.mark.parametrize('fault', [None, 'path', 'inode', 'device', 'deleted', 'missing', 'extra_foreign'])
def test_public_api_maps_require_exact_path_device_inode(tmp_path, monkeypatch, fault):
    path = tmp_path / 'lib/librcl.so'
    path.parent.mkdir()
    path.write_bytes(b'pinned ELF fixture, not executable')
    monkeypatch.setattr(b, 'PREFIX', tmp_path)
    monkeypatch.setattr(b, 'API_PINS', {'lib/librcl.so': hashlib.sha256(path.read_bytes()).hexdigest()})
    st = path.stat()
    device = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'
    mapped, inode = str(path), st.st_ino
    if fault == 'path':
        mapped = '/foreign/librcl.so'
    elif fault == 'inode':
        inode += 1
    elif fault == 'device':
        device = 'ff:ff'
    elif fault == 'deleted':
        mapped += ' (deleted)'
    text = f'1000-2000 r-xp 00000000 {device} {inode} {mapped}\n'
    if fault == 'missing':
        text = ''
    elif fault == 'extra_foreign':
        text += '2000-3000 r-xp 00000000 00:00 123 /foreign/librcl.so\n'
    if fault is not None:
        with pytest.raises(ValueError, match='mapped_public_api_identity_mismatch|required_public_api_not_mapped'):
            b._verify_api_maps(maps_text=text)
    else:
        assert b._verify_api_maps(maps_text=text) == [
            {'path': str(path), 'device': device, 'inode': inode}]
        assert b._verify_api_maps(require_all=False, maps_text='') == []


def test_handle_replaced_inside_sample_cannot_accept_old_generation(make_observer):
    obj, native, endpoint = make_observer()
    original = endpoint.handle

    def replace(out, *_):
        endpoint.handle = Publisher(0x9000)
        return 0

    native.hook = replace
    row = obj.sample(152, WRITER, 'writer')
    assert not row['complete'] and row['reason'] == 'endpoint_handle_changed_during_sample'
    assert 'matched_peer_gids' not in row
    assert original.entries == original.exits == 1
    assert endpoint.handle.entries == endpoint.handle.exits == 0
    native.hook = None
    assert obj.sample(152, WRITER, 'writer')['complete']
    assert native.calls[-1][0] == 0x9200


def test_worker_receives_native_schema_without_public_api_fallback(make_observer):
    from r2f.runtime.topology_worker import _matched_peer_fields

    obj, native, _ = make_observer()
    observation = obj.sample(152, WRITER, 'writer')
    fields = _matched_peer_fields(obj, 152, WRITER, 'writer', 1, 1, observation)
    assert fields['peer_proof_source'] == 'fastdds_native_stateful_peer_snapshot'
    assert fields['peer_gids_complete'] is True
    assert fields['native_peer_observation'] == observation
    observation['schema'] = 'bad-schema'
    fields = _matched_peer_fields(obj, 152, WRITER, 'writer', 1, 1, observation)
    assert fields['peer_gids_complete'] is False
    assert 'peer_proof_source' not in fields


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_destroyable_enter_returns_none_but_original_handle_is_borrowed(make_observer, kind):
    obj, native, endpoint = make_observer(kind)
    borrowed = endpoint.handle
    with borrowed as enter_result:
        assert enter_result is None
        assert borrowed.pointer == 0x1000
    row = obj.sample(152, WRITER if kind == 'writer' else READER, kind)
    assert row['complete'], row
    assert len(native.calls) == 1
    assert borrowed.entries == borrowed.exits == 2
    assert not borrowed.active
