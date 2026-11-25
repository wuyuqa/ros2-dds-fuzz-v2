"""Offline manifest/maps contracts; placeholder binaries are NEVER native-loaded."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock
import sys

import pytest

from r2f.runtime import rcl_take_bridge as b
from r2f.instrumentation.fastdds_peer import activation, probe
from test_rcl_take_bridge import Handle, Native, String


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def installation(monkeypatch, tmp_path):
    prefix = tmp_path / 'installed'
    pins = {}
    for relative in ('include/rmw/rmw/types.h', 'lib/librcl.so', 'lib/librmw.so',
            'lib/librmw_fastrtps_cpp.so', 'lib/librmw_fastrtps_shared_cpp.so',
            'lib/librcutils.so', 'lib/libstd_msgs__rosidl_generator_c.so', 'lib/libfastrtps.so'):
        path = prefix / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(('offline-installed:' + relative).encode())
        pins[relative] = digest(path)
    isolated = tmp_path / 'isolated'
    vendor = isolated / 'lib/libfastrtps.so.2.14'
    provider = isolated / 'lib/libr2f_fastdds_peer.so'
    vendor.parent.mkdir(parents=True)
    vendor.write_bytes(b'offline-only-instrumented-vendor-not-ELF')
    provider.write_bytes(b'offline-only-provider-not-ELF')
    manifest = dict(schema=probe.BUILD_SCHEMA, status='built', observation_mode='instrumentation',
        vendor_version='2.14.7', verification={'compilation': 'passed'}, install=str(isolated),
        provider_source_sha256=digest(Path(probe.__file__).with_name('provider.cpp')),
        patch={'abi': dict(schema_version=1, result_size=64, guid_size=16,
                           layout_changed=False, virtual_slots_added=False)},
        binaries={role: dict(path=str(path), sha256=digest(path))
                  for role, path in [('vendor', vendor), ('provider', provider)]},
        fixture=True, execution_evidence=False)
    manifest_path = tmp_path / 'offline-manifest.json'
    manifest_path.write_text(json.dumps(manifest, sort_keys=True))
    reference = dict(observation_mode='instrumentation', manifest_path=str(manifest_path),
                     manifest_sha256=digest(manifest_path))
    monkeypatch.setattr(b, 'PREFIX', prefix)
    monkeypatch.setattr(b, '_PINS', pins)
    maps = {'paths': [prefix / key for key in pins
                      if key.startswith('lib/') and key != 'lib/libfastrtps.so'] + [vendor]}
    def mapping(path):
        st = path.stat()
        return f'100-200 r--p 00000000 {os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x} {st.st_ino} {path}\n'
    maps['line'] = mapping
    read_text = Path.read_text
    def read(path, *args, **kwargs):
        if str(path) == '/proc/self/maps':
            return maps.get('text', ''.join(mapping(p) for p in maps['paths']))
        return read_text(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    return SimpleNamespace(prefix=prefix, pins=pins, vendor=vendor, provider=provider,
        manifest=manifest, manifest_path=manifest_path, reference=reference, maps=maps)


def fake_loader(monkeypatch):
    names = ('rcl_take', 'std_msgs__msg__String__init', 'std_msgs__msg__String__fini',
             'rcutils_reset_error', 'rcutils_get_error_string', 'rmw_get_implementation_identifier')
    functions = {name: Mock() for name in names}
    functions['rmw_get_implementation_identifier'].return_value = b.RMW_IDENTIFIER.encode()
    loader = Mock(return_value=SimpleNamespace(**functions))
    monkeypatch.setattr(b.C, 'CDLL', loader)
    return loader, functions


def make_bridge(install):
    handle = Handle()
    native = Native(handle)
    native.abi = b.verify_abi(instrumentation=install.reference)
    bridge = b.RclTakeBridge(instrumentation=install.reference, _bindings=native, _string_type=String)
    return bridge, SimpleNamespace(handle=handle, msg_type=String, raw=False), native


def executor_modules(monkeypatch):
    class Base:
        def __init__(self, *, context):
            self.context = context
    async def await_or_execute(callback, *args):
        return callback(*args)
    for name, fields in {
        'rclpy': {},
        'rclpy.executors': dict(SingleThreadedExecutor=Base, await_or_execute=await_or_execute),
        'rclpy.exceptions': dict(InvalidHandle=RuntimeError),
        'rclpy.subscription': dict(Subscription=SimpleNamespace(CallbackType=SimpleNamespace())),
        'std_msgs': {}, 'std_msgs.msg': dict(String=String),
    }.items():
        module = ModuleType(name)
        module.__dict__.update(fields)
        monkeypatch.setitem(sys.modules, name, module)


def test_selects_only_vendor_pin_without_mutating_original_table(installation):
    i = installation
    before = deepcopy(b._PINS)
    receipt = b.verify_abi(instrumentation=i.reference)
    expected = dict(before, **{'lib/libfastrtps.so': digest(i.vendor)})
    assert receipt['files'] == expected and b._PINS == before
    assert receipt['file_paths']['lib/libfastrtps.so'] == str(i.vendor)
    assert receipt['manifest_reference'] == i.reference
    assert receipt['observation_mode'] == 'instrumentation'
    blackbox = b.verify_abi()
    assert blackbox['files'] == before and blackbox['observation_mode'] == 'blackbox'
    assert blackbox['pin_sha256'] == hashlib.sha256(json.dumps([before, b._LAYOUT], sort_keys=True).encode()).hexdigest()
    assert blackbox['pin_sha256'] != receipt['pin_sha256']


def test_none_does_not_infer_instrumentation_from_environment(installation, monkeypatch):
    monkeypatch.setenv('LD_LIBRARY_PATH', str(installation.vendor.parent))
    monkeypatch.setenv('OBSERVATION_MODE', 'instrumentation')
    assert b.verify_abi()['manifest_reference'] is None
    with pytest.raises(b.BridgeUnavailable, match='mapped_library_identity_mismatch:libfastrtps'):
        b._verify_loaded_libraries()


@pytest.mark.parametrize('fault', ['extra', 'relative', 'wrong_mode', 'wrong_hash', 'missing', 'null', 'list'])
def test_explicit_reference_is_validated_not_guessed(installation, fault):
    ref = dict(installation.reference)
    if fault == 'extra': ref['vendor_path'] = str(installation.vendor)
    elif fault == 'relative': ref['manifest_path'] = 'relative.json'
    elif fault == 'wrong_mode': ref['observation_mode'] = 'blackbox'
    elif fault == 'wrong_hash': ref['manifest_sha256'] = 'f' * 64
    elif fault == 'missing': ref.pop('manifest_sha256')
    elif fault == 'null': ref = {}
    else: ref = []
    with pytest.raises(b.BridgeUnavailable) as error:
        b.verify_abi(instrumentation=ref)
    assert error.value.capability_receipt['status'] == 'unknown'
    assert error.value.capability_receipt['fallback_installed'] is False


@pytest.mark.parametrize('relative', ['include/rmw/rmw/types.h', 'lib/librmw_fastrtps_cpp.so'])
def test_installed_nonvendor_pins_are_not_replaced(installation, relative):
    (installation.prefix / relative).write_bytes(b'changed')
    with pytest.raises(b.BridgeUnavailable, match='abi_file_hash_mismatch:' + relative):
        b.verify_abi(instrumentation=installation.reference)


@pytest.mark.parametrize('role', ['vendor', 'provider'])
def test_manifest_binary_hash_change_is_not_a_new_pin(installation, role):
    getattr(installation, role).write_bytes(b'changed')
    with pytest.raises(b.BridgeUnavailable, match='instrumentation_manifest_unavailable:'):
        b.verify_abi(instrumentation=installation.reference)


@pytest.mark.parametrize('document', [[], None, 'bad_verification'])
def test_malformed_manifest_is_unknown_not_an_uncaught_parser_error(installation, document):
    if document == 'bad_verification':
        document = dict(installation.manifest, verification=[])
    installation.manifest_path.write_text(json.dumps(document))
    reference = dict(installation.reference, manifest_sha256=digest(installation.manifest_path))
    with pytest.raises(b.BridgeUnavailable) as error:
        b.verify_abi(instrumentation=reference)
    assert error.value.capability_receipt['status'] == 'unknown'
    assert error.value.capability_receipt['fallback_installed'] is False


def test_disk_and_mapped_vendor_are_bound_to_same_manifest(installation):
    result = b._verify_loaded_libraries(instrumentation=installation.reference)
    identity = result['vendor_identity']
    assert identity['identity_verified'] and not identity['membership_verified']
    assert identity['manifest_reference'] == installation.reference
    assert identity['path'] == str(installation.vendor)
    assert identity['sha256'] == digest(installation.vendor)
    assert identity['inode'] == installation.vendor.stat().st_ino
    assert any(row['path'] == str(installation.vendor) for row in result['mapped_libraries'])


@pytest.mark.parametrize('fault', ['original', 'second_vendor', 'absent', 'inode', 'device', 'deleted', 'other_rmw'])
def test_instrumentation_does_not_disable_maps_checks(installation, fault):
    i = installation
    if fault == 'original':
        i.maps['paths'][-1] = i.prefix / 'lib/libfastrtps.so'
    elif fault == 'second_vendor':
        i.maps['paths'].append(i.prefix / 'lib/libfastrtps.so')
    elif fault == 'absent':
        i.maps['paths'].pop()
    elif fault == 'other_rmw':
        foreign = i.vendor.parent / 'librmw_fastrtps_cpp.so'
        foreign.write_bytes(b'foreign RMW')
        i.maps['paths'].append(foreign)
    else:
        line = i.maps['line'](i.vendor)
        if fault == 'inode': line = line.replace(str(i.vendor.stat().st_ino), '999999')
        elif fault == 'device': line = line.replace(line.split()[3], '00:00')
        else: line = line.rstrip() + ' (deleted)\n'
        i.maps['text'] = line
    with pytest.raises(b.BridgeUnavailable):
        b._verify_loaded_libraries(instrumentation=i.reference)


def test_maps_unavailable_is_unknown(installation, monkeypatch):
    read = Path.read_text
    def denied(path, *args, **kwargs):
        if str(path) == '/proc/self/maps': raise OSError('offline maps denied')
        return read(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', denied)
    with pytest.raises(b.BridgeUnavailable, match='mapped_library_identity_unavailable'):
        b._verify_loaded_libraries(instrumentation=installation.reference)


def test_bindings_forward_reference_and_recheck_after_identifier_call(installation, monkeypatch):
    loader, functions = fake_loader(monkeypatch)
    original = activation.verify_vendor_mapping
    verifier = Mock(wraps=original)
    monkeypatch.setattr(activation, 'verify_vendor_mapping', verifier)
    native = b._Bindings(b.RMW_IDENTIFIER, instrumentation=installation.reference)
    assert verifier.call_count == 2
    assert all(call.args[0] == installation.reference for call in verifier.call_args_list)
    assert native.abi['vendor_identity']['identity_verified']
    assert native.abi['manifest_reference'] == installation.reference
    assert functions['rmw_get_implementation_identifier'].call_count == 1
    assert all(str(installation.prefix) in call.args[0] for call in loader.call_args_list)


def test_mapping_change_in_identifier_call_is_rejected(installation, monkeypatch):
    _, functions = fake_loader(monkeypatch)
    def identifier():
        installation.maps['paths'][-1] = installation.prefix / 'lib/libfastrtps.so'
        return b.RMW_IDENTIFIER.encode()
    functions['rmw_get_implementation_identifier'].side_effect = identifier
    with pytest.raises(b.BridgeUnavailable, match='mapped_library_identity_mismatch:libfastrtps'):
        b._Bindings(b.RMW_IDENTIFIER, instrumentation=installation.reference)
    assert functions['rmw_get_implementation_identifier'].call_count == 1


def test_bad_manifest_fails_before_any_native_load(installation, monkeypatch):
    loader, _ = fake_loader(monkeypatch)
    bad = dict(installation.reference, manifest_sha256='f' * 64)
    with pytest.raises(b.BridgeUnavailable):
        b._Bindings(b.RMW_IDENTIFIER, instrumentation=bad)
    loader.assert_not_called()


def test_mode_reference_is_copied_and_receipt_retains_mapping_provenance(installation):
    expected_reference = dict(installation.reference)
    bridge, sub, native = make_bridge(installation)
    installation.reference['manifest_sha256'] = 'f' * 64
    message, info = bridge.take(sub)
    proof = info['rcl_take_receipt']['instrumentation']
    assert message.data == 'original0\x00same String.data'
    assert bridge.instrumentation == expected_reference
    assert proof['manifest_reference'] == expected_reference
    assert proof['vendor_identity']['path'] == str(installation.vendor)
    assert proof['abi_pin_sha256'] == bridge.abi_receipt['pin_sha256']
    assert info['rcl_take_receipt']['candidate_eligible'] is False
    assert len([c for c in native.calls if isinstance(c, tuple)]) == 1
    bridge.close()


def test_changed_manifest_before_take_is_unknown_without_consuming(installation):
    bridge, sub, native = make_bridge(installation)
    installation.manifest_path.write_text('{}')
    with pytest.raises(b.BridgeUnavailable):
        bridge.take(sub)
    assert native.calls == []


def test_changed_mapping_during_take_is_unknown_without_retry_or_fallback(installation):
    bridge, sub, native = make_bridge(installation)
    native.take_hook = lambda: installation.maps['paths'].__setitem__(
        -1, installation.prefix / 'lib/libfastrtps.so')
    with pytest.raises(b.BridgeUnavailable):
        bridge.take(sub)
    assert len([c for c in native.calls if isinstance(c, tuple)]) == 1
    assert native.calls.count('fini') == 1
    assert native.calls.count('reset') == 1


def test_executor_forwards_reference_to_owned_bridge(installation, monkeypatch):
    executor_modules(monkeypatch)
    fake_loader(monkeypatch)
    context = object()
    executor = b.make_full_info_executor(context=context, instrumentation=installation.reference)
    assert executor.context is context
    assert executor.take_bridge.instrumentation == installation.reference
    assert executor.take_bridge.abi_receipt['vendor_identity']['identity_verified']
    executor.take_bridge.close()


@pytest.mark.parametrize('factory_instrumented', [False, True])
def test_borrowed_bridge_mode_must_match_factory(installation, monkeypatch, factory_instrumented):
    executor_modules(monkeypatch)
    if factory_instrumented:
        native = SimpleNamespace(abi={'pin_sha256': 'offline'})
        bridge = b.RclTakeBridge(_bindings=native, _string_type=String)
        ref = installation.reference
    else:
        bridge, _, _ = make_bridge(installation)
        ref = None
    with pytest.raises(b.BridgeUnavailable, match='bridge_executor_instrumentation_mismatch'):
        b.make_full_info_executor(bridge=bridge, instrumentation=ref)


def test_executor_accepts_matching_borrowed_bridge_and_revalidates_it(installation, monkeypatch):
    executor_modules(monkeypatch)
    bridge, _, native = make_bridge(installation)
    with monkeypatch.context() as patch:
        check = Mock(wraps=bridge._revalidate_instrumentation)
        patch.setattr(bridge, '_revalidate_instrumentation', check)
        executor = b.make_full_info_executor(bridge=bridge, instrumentation=installation.reference)
        assert executor.take_bridge is bridge
        check.assert_called_once_with()
    assert native.calls == []
    installation.maps['paths'][-1] = installation.prefix / 'lib/libfastrtps.so'
    with pytest.raises(b.BridgeUnavailable):
        b.make_full_info_executor(bridge=bridge, instrumentation=installation.reference)
    assert native.calls == []
    bridge.close()


def test_array_executor_explicitly_rejects_unqualified_instrumentation_before_native_load(monkeypatch):
    from unittest.mock import Mock
    monkeypatch.setattr(b, 'verify_abi', Mock(side_effect=AssertionError('must not downgrade array instrumentation')))
    with pytest.raises(b.BridgeUnavailable, match='instrumented_array_take_not_qualified'):
        b.make_full_info_executor(message_type='std_msgs/msg/UInt8MultiArray',
                                  instrumentation={'manifest': 'unqualified-control'})
    b.verify_abi.assert_not_called()
