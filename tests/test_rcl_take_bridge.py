"""Mock ctypes boundaries only: never initialize ROS, spin or create DDS entities."""
import asyncio
import ctypes as C
from enum import Enum
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import threading
from unittest.mock import Mock

import pytest

from r2f.runtime import rcl_take_bridge as b


class String:
    def __init__(self):
        self.data = ''


class InvalidHandle(Exception):
    pass


class Handle:
    def __init__(self, pointer=0x1000):
        self._pointer = pointer
        self.entered = False
        self.enters = self.exits = self.stock_takes = 0
        self.destroyed = False

    def __enter__(self):
        if self.destroyed:
            raise InvalidHandle()
        assert not self.entered
        self.enters += 1
        self.entered = True
        return self

    def __exit__(self, *_):
        assert self.entered
        self.exits += 1
        self.entered = False

    @property
    def pointer(self):
        assert self.entered, 'must borrow pointer only inside with handle'
        return self._pointer

    def take_message(self, *_):
        self.stock_takes += 1
        raise AssertionError('competing stock take must never run')


class Native:
    """Python implementation of the exact pointer-to-struct C call boundary."""
    abi = {'pin_sha256': 'mock-offline-only'}

    def __init__(self, handle):
        self.handle = handle
        self.calls = []
        self.payload = b'original0\x00same String.data'
        self.code = 0
        self.init_result = True
        self.identifier = b'rmw_fastrtps_cpp'
        self.gid = bytes.fromhex('010f7f014c00961e0000000000000603')
        self.pubseq, self.recseq = 3, b.UINT64_MAX
        self.raise_take = None
        self.take_hook = None

    def init(self, ptr):
        self.calls.append('init')
        assert self.handle.entered
        return self.init_result

    def take(self, pointer, message, info, allocation):
        assert self.handle.entered
        assert pointer == self.handle.pointer
        assert allocation is None
        self.calls.append(('take', pointer))
        if self.take_hook:
            self.take_hook()
        if self.raise_take:
            raise self.raise_take
        self.buffer = C.create_string_buffer(self.payload + b'\0')
        value = C.cast(message, C.POINTER(b._String)).contents
        value.data.data = C.addressof(self.buffer)
        value.data.size = len(self.payload)
        value.data.capacity = len(self.buffer)
        meta = C.cast(info, C.POINTER(b._MessageInfo)).contents
        meta.source_timestamp = 123456789
        meta.received_timestamp = 123456999
        meta.publication_sequence_number = self.pubseq
        meta.reception_sequence_number = self.recseq
        meta.publisher_gid.implementation_identifier = self.identifier
        meta.publisher_gid.data[:] = self.gid
        meta.from_intra_process = False
        return self.code

    def fini(self, ptr):
        assert self.handle.entered
        self.calls.append('fini')
        # Poison released buffers/metadata; Python output must already be copied.
        if hasattr(self, 'buffer'):
            C.memset(C.addressof(self.buffer), 0x78, len(self.buffer))

    def error(self):
        self.calls.append('error')
        return 'mock rcl failure'

    def reset_error(self):
        self.calls.append('reset')


@pytest.fixture
def fixture():
    h = Handle()
    native = Native(h)
    bridge = b.RclTakeBridge(_bindings=native, _string_type=String)
    sub = SimpleNamespace(handle=h, msg_type=String, raw=False)
    return bridge, sub, native


def take_calls(native):
    return [c for c in native.calls if isinstance(c, tuple)]


def test_exact_string_all_metadata_and_provenance_copied_before_fini(fixture):
    bridge, sub, native = fixture
    result, info = bridge.take(sub)
    assert result.data == 'original0\x00same String.data'
    assert info['publisher_gid'] == native.gid
    assert type(info['publisher_gid']) is bytes and len(info['publisher_gid']) == 16
    assert info['publisher_gid_status'] == 'available'
    assert info['identity_basis'] == 'message_info_gid'
    assert info['basis'] == 'public_rcl_take_rmw_message_info'
    assert info['publication_sequence_number'] == 3
    assert info['source_timestamp'] == 123456789
    assert info['received_timestamp'] == 123456999
    assert info['reception_sequence_number'] is None
    assert info['from_intra_process'] is False
    receipt = info['rcl_take_receipt']
    assert receipt['rcl_subscription_pointer'] == '0x1000'
    assert receipt['message_info_receipt']['publisher_gid']['value'] == native.gid.hex()
    assert receipt['message_info_receipt']['fields']['reception_sequence_number']['status'] == 'unsupported_rmw'
    assert not receipt['original_rclpy_dict_provided_gid']
    assert not receipt['identity_qualified'] and not receipt['candidate_eligible']
    json.dumps(receipt)
    assert native.calls == ['init', ('take', 0x1000), 'fini']
    assert sub.handle.enters == sub.handle.exits == 1
    assert sub.handle.stock_takes == 0


@pytest.mark.parametrize('identifier,gid,status', [
    (None, b'\x01'*16, 'invalid_implementation_identifier'),
    (b'rmw_cyclonedds_cpp', b'\x01'*16, 'invalid_implementation_identifier'),
    (b'rmw_fastrtps_cpp', bytes(16), 'invalid_gid'),
    (b'rmw_fastrtps_cpp', bytes(12)+b'\x00\x00\x06\x03', 'invalid_gid'),
    (b'rmw_fastrtps_cpp', b'\x01'*12+bytes(4), 'invalid_gid'),
])
def test_invalid_gid_never_becomes_payload_or_graph_identity(fixture, identifier, gid, status):
    bridge, sub, native = fixture
    native.identifier, native.gid = identifier, gid
    _, info = bridge.take(sub)
    assert info['publisher_gid'] is None
    assert info['publisher_gid_status'] == status
    assert info['identity_basis'] == 'unavailable'
    assert info['message_info_receipt']['publisher_gid']['value'] is None
    assert len(take_calls(native)) == 1


def test_unsupported_sequences_are_none_but_raw_sentinel_in_receipt(fixture):
    bridge, sub, native = fixture
    native.pubseq = b.UINT64_MAX
    _, info = bridge.take(sub)
    assert info['publication_sequence_number'] is None
    field = info['message_info_receipt']['fields']['publication_sequence_number']
    assert field == {'status': 'unsupported_rmw', 'value': None, 'raw_value': b.UINT64_MAX}


def test_no_sample_returns_none_resets_error_and_finalizes_once(fixture):
    bridge, sub, native = fixture
    native.code = 401  # rcl_take uses return code, not a fabricated taken boolean.
    assert bridge.take(sub) is None
    assert native.calls == ['init', ('take', 0x1000), 'reset', 'fini']


@pytest.mark.parametrize('payload', [b'\xff', b'\xed\xa0\x80', b'\xf0\x9f'])
def test_utf8_error_finalizes_and_never_retries_take(fixture, payload):
    bridge, sub, native = fixture
    native.payload = payload
    with pytest.raises(UnicodeDecodeError):
        bridge.take(sub)
    assert native.calls[-2:] == ['reset', 'fini']
    assert len(take_calls(native)) == 1
    assert not sub.handle.entered
    native.payload = b'next actual sample'
    assert bridge.take(sub)[0].data == 'next actual sample'  # lock released, not a retry.


@pytest.mark.parametrize('code', [1, 2, 11, 200, 400])
def test_rcl_error_preserves_code_and_error_resets_before_exit(fixture, code):
    bridge, sub, native = fixture
    native.code = code
    with pytest.raises(b.BridgeError, match=f'rcl_take_failed:{code}:mock rcl failure'):
        bridge.take(sub)
    assert native.calls[-3:] == ['error', 'reset', 'fini']
    assert len(take_calls(native)) == 1


def test_init_failure_finalizes_partial_buffer_no_take(fixture):
    bridge, sub, native = fixture
    native.init_result = False
    with pytest.raises(b.BridgeError, match='String_init_failed'):
        bridge.take(sub)
    assert native.calls == ['init', 'error', 'reset', 'fini']


def test_native_exception_cleanup_and_close_are_idempotent(fixture):
    bridge, sub, native = fixture
    native.raise_take = RuntimeError('ctypes boundary failure')
    with pytest.raises(RuntimeError, match='ctypes boundary failure'):
        bridge.take(sub)
    assert native.calls[-2:] == ['reset', 'fini']
    count = len(native.calls)
    bridge.close()
    bridge.close()
    with pytest.raises(b.BridgeError, match='bridge_closed'):
        bridge.take(sub)
    assert len(native.calls) == count


def test_handle_and_cft_generation_pointer_are_fresh_each_time(fixture):
    bridge, sub, native = fixture
    first = bridge.take(sub)[1]['rcl_take_receipt']
    old = sub.handle
    new = Handle(0x2000)
    sub.handle = native.handle = new
    second = bridge.take(sub)[1]['rcl_take_receipt']
    assert [first['rcl_subscription_pointer'], second['rcl_subscription_pointer']] == ['0x1000', '0x2000']
    assert second['take_index'] == first['take_index']+1
    assert second['bridge_id'] == first['bridge_id']
    assert old.enters == old.exits == new.enters == new.exits == 1
    assert take_calls(native) == [('take', 0x1000), ('take', 0x2000)]


@pytest.mark.parametrize('pointer', [0, None, True, -1, 1 << 64])
def test_invalid_pointer_rejected_without_native_call(fixture, pointer):
    bridge, sub, native = fixture
    sub.handle._pointer = pointer
    with pytest.raises(b.BridgeError, match='invalid_rcl_subscription_pointer'):
        bridge.take(sub)
    assert not native.calls
    assert sub.handle.enters == sub.handle.exits == 1


@pytest.mark.parametrize('raw,type_override', [(True, False), (None, False), (False, True)])
def test_type_and_raw_strictly_rejected_before_take(fixture, raw, type_override):
    bridge, sub, native = fixture
    sub.raw = raw
    if type_override:
        sub.msg_type = type('String', (String,), {})
    with pytest.raises(b.BridgeUnavailable, match='only_std_msgs'):
        bridge.take(sub)
    assert not native.calls and sub.handle.enters == 0


def test_reentrant_take_rejected_no_nested_take(fixture):
    bridge, sub, native = fixture
    def reenter():
        with pytest.raises(b.BridgeError, match='concurrent_or_reentrant'):
            bridge.take(sub)
    native.take_hook = reenter
    bridge.take(sub)
    assert len(take_calls(native)) == 1


def test_close_waits_for_active_take_then_prevents_more(fixture):
    bridge, sub, native = fixture
    inside, release, closed = threading.Event(), threading.Event(), threading.Event()
    native.take_hook = lambda: (inside.set(), release.wait(5))
    errors = []
    def take():
        try:
            bridge.take(sub)
        except BaseException as exc:
            errors.append(exc)
    taker = threading.Thread(target=take)
    taker.start()
    assert inside.wait(5)
    closer = threading.Thread(target=lambda: (bridge.close(), closed.set()))
    closer.start()
    assert not closed.wait(.02)
    release.set()
    taker.join(5)
    closer.join(5)
    assert closed.is_set() and not errors
    with pytest.raises(b.BridgeError, match='bridge_closed'):
        bridge.take(sub)
    assert len(take_calls(native)) == 1


class CallbackType(Enum):
    MessageOnly = 0
    WithMessageInfo = 1


class BaseExecutor:
    def __init__(self, **kwargs):
        self.context = kwargs['context']

    def untouched(self):
        return 'inherited behavior'

    def _take_subscription(self, sub):
        raise AssertionError('must not call stock/fallback take')


async def await_or_execute(callback, *args):
    value = callback(*args)
    if hasattr(value, '__await__'):
        return await value
    return value


@pytest.mark.parametrize('kind', list(CallbackType))
@pytest.mark.parametrize('is_async', [True, False])
def test_executor_single_take_original_callback_semantics(fixture, kind, is_async):
    bridge, sub, native = fixture
    cls = b._executor_class(BaseExecutor, await_or_execute, InvalidHandle, CallbackType)
    context = object()
    executor = cls(context=context, take_bridge=bridge)
    received = []
    def callback(*args):
        received.append(args)
    async def async_callback(*args):
        received.append(args)
    sub.callback = async_callback if is_async else callback
    sub._callback_type = kind
    work = executor._take_subscription(sub)
    assert received == []
    asyncio.run(work())
    assert executor.context is context and executor.untouched() == 'inherited behavior'
    assert len(received) == 1 and len(received[0]) == (1 if kind is CallbackType.MessageOnly else 2)
    assert len(take_calls(native)) == 1 and sub.handle.stock_takes == 0


def test_executor_invalid_handle_no_take_and_unknown_callback_type_rejected(fixture):
    bridge, sub, native = fixture
    cls = b._executor_class(BaseExecutor, await_or_execute, InvalidHandle, CallbackType)
    executor = cls(context=None, take_bridge=bridge)
    sub._callback_type = CallbackType.WithMessageInfo
    sub.handle.destroyed = True
    assert executor._take_subscription(sub) is None
    assert not native.calls
    sub._callback_type = 'wrong'
    with pytest.raises(b.BridgeError, match='unsupported_subscription_callback_type'):
        executor._take_subscription(sub)
    assert not native.calls


def test_callback_failure_propagates_without_second_take(fixture):
    bridge, sub, native = fixture
    cls = b._executor_class(BaseExecutor, await_or_execute, InvalidHandle, CallbackType)
    sub._callback_type = CallbackType.WithMessageInfo
    sub.callback = Mock(side_effect=RuntimeError('callback error'))
    work = cls(context=None, take_bridge=bridge)._take_subscription(sub)
    with pytest.raises(RuntimeError, match='callback error'):
        asyncio.run(work())
    assert len(take_calls(native)) == 1


def test_executor_no_sample_schedules_nothing(fixture):
    bridge, sub, native = fixture
    native.code = 401
    cls = b._executor_class(BaseExecutor, await_or_execute, InvalidHandle, CallbackType)
    sub._callback_type = CallbackType.WithMessageInfo
    assert cls(context=None, take_bridge=bridge)._take_subscription(sub) is None
    assert len(take_calls(native)) == 1


def test_abi_size_offsets_match_pinned_public_headers():
    assert b._layout() == b._LAYOUT
    assert C.sizeof(b._Gid) == 24  # pointer+16-byte GID, NOT 24-byte GID.
    assert C.sizeof(b._MessageInfo) == 64


@pytest.mark.parametrize('mutation', ['hash', 'missing', 'layout', 'platform'])
def test_abi_mismatch_is_explicit_unknown_before_loading(monkeypatch, tmp_path, mutation):
    relative = 'mock-public-header'
    file = tmp_path/relative
    file.write_bytes(b'known ABI')
    monkeypatch.setattr(b, 'PREFIX', tmp_path)
    monkeypatch.setattr(b, '_PINS', {relative: hashlib.sha256(file.read_bytes()).hexdigest()})
    monkeypatch.setattr(b.C, 'CDLL', Mock(side_effect=AssertionError('load before validation')))
    if mutation == 'hash':
        file.write_bytes(b'new incompatible ABI')
    elif mutation == 'missing':
        file.unlink()
    elif mutation == 'layout':
        monkeypatch.setattr(b, '_layout', lambda: {})
    else:
        monkeypatch.setattr(b.platform, 'machine', lambda: 'aarch64')
    with pytest.raises(b.BridgeUnavailable) as err:
        b._Bindings(b.RMW_IDENTIFIER)
    assert err.value.capability_receipt['status'] == 'unknown'
    assert not err.value.capability_receipt['fallback_installed']
    b.C.CDLL.assert_not_called()


def test_native_load_failure_is_unknown_no_take_fallback(monkeypatch):
    monkeypatch.setattr(b, 'verify_abi', lambda: {'pin_sha256': 'mock'})
    monkeypatch.setattr(b.C, 'CDLL', Mock(side_effect=OSError('missing native library')))
    with pytest.raises(b.BridgeUnavailable, match='native_load_or_symbol_failure') as err:
        b.RclTakeBridge()
    assert err.value.capability_receipt['status'] == 'unknown'


def test_unsupported_rmw_rejected_without_loading(monkeypatch):
    monkeypatch.setattr(b, 'verify_abi', Mock(side_effect=AssertionError()))
    with pytest.raises(b.BridgeUnavailable, match='unsupported_rmw_identifier'):
        b.RclTakeBridge(rmw_identifier='rmw_cyclonedds_cpp')
    b.verify_abi.assert_not_called()


def test_factory_inherits_only_take_and_passes_original_context(monkeypatch, fixture):
    bridge, sub, native = fixture
    import types
    monkeypatch.setattr(b, 'verify_abi', lambda: {'pin_sha256': 'mock'})
    for name, attrs in {
        'rclpy': {},
        'rclpy.executors': {'SingleThreadedExecutor': BaseExecutor, 'await_or_execute': await_or_execute},
        'rclpy.exceptions': {'InvalidHandle': InvalidHandle},
        'rclpy.subscription': {'Subscription': SimpleNamespace(CallbackType=CallbackType)},
    }.items():
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, module)
    context = object()
    executor = b.make_full_info_executor(context=context, bridge=bridge)
    assert executor.context is context
    assert executor.take_bridge is bridge
    assert set(type(executor).__dict__) - {'__module__', '__doc__', '__qualname__'} == {'__init__', '_take_subscription'}


def test_production_type_import_does_not_run_ros_at_module_import():
    source = Path(b.__file__).read_text()
    assert 'rclpy.init(' not in source and 'create_subscription(' not in source
    assert 'create_publisher(' not in source


def test_reader_entity_id_is_not_valid_publisher_gid(fixture):
    bridge, sub, native = fixture
    native.gid = native.gid[:-1] + b'\x04'
    _, info = bridge.take(sub)
    assert info['publisher_gid'] is None
    assert info['publisher_gid_status'] == 'invalid_gid'
    assert info['message_info_receipt']['publisher_gid']['raw_value'] == native.gid.hex()


@pytest.mark.parametrize('damage', ['capacity', 'null', 'terminator'])
def test_invalid_native_string_never_reads_out_of_bounds_or_retries(fixture, damage):
    bridge, sub, native = fixture
    original_take = native.take
    def damaged_take(pointer, message, info, allocation):
        code = original_take(pointer, message, info, allocation)
        data = C.cast(message, C.POINTER(b._String)).contents.data
        if damage == 'capacity':
            data.capacity = data.size
        elif damage == 'null':
            data.data = None
        else:
            C.memset(data.data+data.size, 1, 1)
        return code
    native.take = damaged_take
    with pytest.raises(b.BridgeError, match='native_String|native_String_size'):
        bridge.take(sub)
    assert native.calls[-2:] == ['reset', 'fini']
    assert len(take_calls(native)) == 1


def test_empty_string_is_preserved(fixture):
    bridge, sub, native = fixture
    native.payload = b''
    assert bridge.take(sub)[0].data == ''


def test_fini_exception_exits_handle_releases_lock_no_retry(fixture):
    bridge, sub, native = fixture
    native.fini = Mock(side_effect=RuntimeError('fini failed'))
    with pytest.raises(RuntimeError, match='fini failed'):
        bridge.take(sub)
    assert sub.handle.enters == sub.handle.exits == 1
    bridge.close()  # Would block if take leaked its lock.
    assert len(take_calls(native)) == 1


def test_zero_signed_timestamp_not_replaced_with_monotonic(fixture):
    bridge, sub, native = fixture
    original_take = native.take
    def timestamps(pointer, message, info, allocation):
        ret = original_take(pointer, message, info, allocation)
        meta = C.cast(info, C.POINTER(b._MessageInfo)).contents
        meta.source_timestamp, meta.received_timestamp = 0, -1
        return ret
    native.take = timestamps
    _, info = bridge.take(sub)
    assert info['source_timestamp'] == 0 and info['received_timestamp'] == -1
    fields = info['message_info_receipt']['fields']
    assert fields['source_timestamp']['status'] == 'zero_unavailable_or_unspecified'
    assert fields['received_timestamp']['status'] == 'invalid_value'


def mock_executor_modules(monkeypatch):
    import types
    for name, attrs in {
        'rclpy': {},
        'rclpy.executors': {'SingleThreadedExecutor': BaseExecutor, 'await_or_execute': await_or_execute},
        'rclpy.exceptions': {'InvalidHandle': InvalidHandle},
        'rclpy.subscription': {'Subscription': SimpleNamespace(CallbackType=CallbackType)},
    }.items():
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(b, 'verify_abi', lambda: {'pin_sha256': 'mock'})


def test_factory_unavailability_no_fallback_executor_created(monkeypatch):
    mock_executor_modules(monkeypatch)
    native = Mock(side_effect=b.BridgeUnavailable('mock loader unavailable'))
    monkeypatch.setattr(b, 'RclTakeBridge', native)
    monkeypatch.setattr(b, '_executor_class', Mock(side_effect=AssertionError('no fallback')))
    with pytest.raises(b.BridgeUnavailable, match='mock loader unavailable'):
        b.make_full_info_executor(context=object())
    b._executor_class.assert_not_called()


def test_factory_closes_owned_bridge_if_executor_constructor_fails(monkeypatch, fixture):
    mock_executor_modules(monkeypatch)
    bridge = fixture[0]
    monkeypatch.setattr(b, 'RclTakeBridge', lambda **_: bridge)
    monkeypatch.setattr(b, '_executor_class', lambda *_: Mock(side_effect=RuntimeError('bad context')))
    with pytest.raises(RuntimeError, match='bad context'):
        b.make_full_info_executor(context=object())
    with pytest.raises(b.BridgeError, match='bridge_closed'):
        bridge.take(fixture[1])


def test_factory_does_not_close_borrowed_bridge_on_constructor_failure(monkeypatch, fixture):
    mock_executor_modules(monkeypatch)
    bridge, sub, _ = fixture
    monkeypatch.setattr(b, '_executor_class', lambda *_: Mock(side_effect=RuntimeError('bad context')))
    with pytest.raises(RuntimeError, match='bad context'):
        b.make_full_info_executor(context=object(), bridge=bridge)
    assert bridge.take(sub) is not None


def test_factory_bridge_rmw_mismatch_fails_before_executor(monkeypatch, fixture):
    mock_executor_modules(monkeypatch)
    monkeypatch.setattr(b, '_executor_class', Mock(side_effect=AssertionError('bad mismatch')))
    with pytest.raises(b.BridgeUnavailable, match='bridge_executor_rmw_identifier_mismatch'):
        b.make_full_info_executor(rmw_identifier='rmw_cyclonedds_cpp', bridge=fixture[0])
    b._executor_class.assert_not_called()


def mock_native_loader(monkeypatch, identifier=b'rmw_fastrtps_cpp'):
    monkeypatch.setattr(b, 'verify_abi', lambda: {'pin_sha256': 'mock'})
    monkeypatch.setattr(b, '_verify_loaded_libraries', Mock())
    functions = {name: Mock(return_value=None) for name in [
        'rcl_take', 'std_msgs__msg__String__init', 'std_msgs__msg__String__fini',
        'rcutils_reset_error', 'rcutils_get_error_string', 'rmw_get_implementation_identifier']}
    functions['rmw_get_implementation_identifier'].return_value = identifier
    library = SimpleNamespace(rcl_take=functions['rcl_take'])
    libraries = {
        'librcl.so': library,
        'librmw.so': SimpleNamespace(),
        'librmw_fastrtps_cpp.so': SimpleNamespace(
            rmw_get_implementation_identifier=functions['rmw_get_implementation_identifier']),
        'librcutils.so': SimpleNamespace(
            rcutils_reset_error=functions['rcutils_reset_error'],
            rcutils_get_error_string=functions['rcutils_get_error_string']),
        'libstd_msgs__rosidl_generator_c.so': SimpleNamespace(
            std_msgs__msg__String__init=functions['std_msgs__msg__String__init'],
            std_msgs__msg__String__fini=functions['std_msgs__msg__String__fini']),
    }
    monkeypatch.setattr(b.C, 'CDLL', lambda path: libraries[Path(path).name])
    return functions, library


def test_ctypes_function_argtypes_restypes_are_pinned(monkeypatch):
    functions, _ = mock_native_loader(monkeypatch)
    bindings = b._Bindings(b.RMW_IDENTIFIER)
    assert bindings.take.argtypes == [C.c_void_p, C.POINTER(b._String), C.POINTER(b._MessageInfo), C.c_void_p]
    assert bindings.take.restype is C.c_int
    assert bindings.init.argtypes == [C.POINTER(b._String)] and bindings.init.restype is C.c_bool
    assert bindings.fini.argtypes == [C.POINTER(b._String)] and bindings.fini.restype is None
    assert bindings.error_string.restype is b._ErrorString
    functions['rcl_take'].assert_not_called()
    functions['std_msgs__msg__String__init'].assert_not_called()
    assert b._verify_loaded_libraries.call_count == 2


@pytest.mark.parametrize('identifier', [None, b'rmw_cyclonedds_cpp'])
def test_loaded_rmw_identifier_mismatch_is_explicit_capability_unknown(monkeypatch, identifier):
    functions, _ = mock_native_loader(monkeypatch, identifier)
    with pytest.raises(b.BridgeUnavailable, match='loaded_rmw_identifier_mismatch'):
        b._Bindings(b.RMW_IDENTIFIER)
    functions['rcl_take'].assert_not_called()


def test_missing_native_symbol_is_capability_unknown(monkeypatch):
    functions, library = mock_native_loader(monkeypatch)
    del library.rcl_take
    with pytest.raises(b.BridgeUnavailable, match='native_load_or_symbol_failure:AttributeError'):
        b._Bindings(b.RMW_IDENTIFIER)
    functions['rmw_get_implementation_identifier'].assert_not_called()


def test_mapped_soname_override_is_not_accepted_as_pinned(monkeypatch):
    monkeypatch.setattr(b.Path, 'read_text', lambda _: '1-2 r--p 0 00:00 42 /tmp/override/librcl.so\n')
    with pytest.raises(b.BridgeUnavailable, match='mapped_library_identity_mismatch:librcl'):
        b._verify_loaded_libraries()


def test_mapped_library_inventory_unavailable_is_unknown(monkeypatch):
    def denied(_):
        raise OSError('maps unavailable')
    monkeypatch.setattr(b.Path, 'read_text', denied)
    with pytest.raises(b.BridgeUnavailable, match='mapped_library_identity_unavailable'):
        b._verify_loaded_libraries()


def test_identifier_is_bound_from_pinned_vendor_not_rmw_core(monkeypatch):
    # Unlike the original fake, each CDLL exposes ONLY its real library's API.
    monkeypatch.setattr(b, 'verify_abi', lambda: {'pin_sha256': 'mock'})
    monkeypatch.setattr(b, '_verify_loaded_libraries', Mock())
    core = SimpleNamespace()  # librmw.so does NOT export the identifier API.
    vendor_identifier = Mock(return_value=b'rmw_fastrtps_cpp')
    vendor = SimpleNamespace(rmw_get_implementation_identifier=vendor_identifier)
    libraries = {
        'librcl.so': SimpleNamespace(rcl_take=Mock()),
        'librmw.so': core,
        'librmw_fastrtps_cpp.so': vendor,
        'librcutils.so': SimpleNamespace(rcutils_reset_error=Mock(), rcutils_get_error_string=Mock()),
        'libstd_msgs__rosidl_generator_c.so': SimpleNamespace(
            std_msgs__msg__String__init=Mock(), std_msgs__msg__String__fini=Mock()),
    }
    loaded = []
    def load(path):
        loaded.append(path)
        return libraries[Path(path).name]
    monkeypatch.setattr(b.C, 'CDLL', load)
    native = b._Bindings(b.RMW_IDENTIFIER)
    vendor_identifier.assert_called_once_with()
    assert vendor_identifier.argtypes == []
    assert vendor_identifier.restype is C.c_char_p
    assert str(b.PREFIX/'lib/librmw_fastrtps_cpp.so') in loaded
    assert 'lib/librmw_fastrtps_cpp.so' in b._PINS
    native.take.assert_not_called()


def test_real_native_load_no_ros_init_or_dds_entities():
    """Installation integration check, not fake coverage or a ROS/DDS execution.

    Runs separately to provide an actual dynamic-loader search path without
    polluting the pytest process. Missing ROS installation is the only skip;
    installed ABI/symbol mismatch FAILS instead of being turned into capability skip.
    """
    import os
    import subprocess
    if not b.PREFIX.is_dir():
        pytest.skip('No local pinned Jazzy installation; native-load check unavailable')
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = str(b.PREFIX/'lib') + (
        ':'+env['LD_LIBRARY_PATH'] if env.get('LD_LIBRARY_PATH') else '')
    env['PYTHONPATH'] = str(Path(b.__file__).resolve().parents[2])
    script = '''
import ctypes, json, sys
from r2f.runtime.rcl_take_bridge import _Bindings
native = _Bindings('rmw_fastrtps_cpp')
f = native.libs['rmw_fastrtps_cpp'].rmw_get_implementation_identifier
assert f.argtypes == [] and f.restype is ctypes.c_char_p
assert f() == b'rmw_fastrtps_cpp'
assert 'rclpy' not in sys.modules
print(json.dumps({'status':'native_load_verified', 'identifier':f().decode(),
 'library':native.libs['rmw_fastrtps_cpp']._name,
 'abi_pin_sha256':native.abi['pin_sha256'], 'ros_init':False, 'dds_entities_created':False}))
'''
    proc = subprocess.run(['/usr/bin/python3', '-c', script], env=env,
                          capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0, proc.stdout+'\n'+proc.stderr
    result = json.loads(proc.stdout)
    assert result['status'] == 'native_load_verified'
    assert result['identifier'] == b.RMW_IDENTIFIER
    assert result['library'] == str(b.PREFIX/'lib/librmw_fastrtps_cpp.so')
    assert not result['ros_init'] and not result['dds_entities_created']


class UInt8MultiArray:
    def __init__(self):
        self.data = []
        self.layout = SimpleNamespace(dim=[], data_offset=0)


class ArrayNative(Native):
    """Generated array allocation/conversion boundary; no native DDS runtime."""
    def array_create(self):
        assert self.handle.entered
        self.calls.append('array_create')
        return 0x2000

    def take(self, pointer, message, info, allocation):
        assert message == 0x2000
        # Reuse only the native MessageInfo output fixture, never String bytes
        # as an array buffer. The real generated converter owns that layout.
        scratch = b._String()
        return super().take(pointer, C.byref(scratch), info, allocation)

    def array_convert(self, pointer):
        assert pointer == 0x2000 and self.handle.entered
        self.calls.append('array_convert')
        result = UInt8MultiArray()
        result.data = list(b'\x00\xffarray payload')
        result.layout.data_offset = 2
        result.layout.dim = [SimpleNamespace(label='维度', size=2, stride=4)]
        return result

    def array_destroy(self, pointer):
        assert pointer == 0x2000 and self.handle.entered
        self.calls.append('array_destroy')


def test_array_take_preserves_generated_body_and_full_identity_before_destroy():
    handle = Handle()
    native = ArrayNative(handle)
    bridge = b.RclTakeBridge(message_type='std_msgs/msg/UInt8MultiArray',
                             _bindings=native, _array_type=UInt8MultiArray)
    subscription = SimpleNamespace(handle=handle, msg_type=UInt8MultiArray, raw=False)
    message, info = bridge.take(subscription)
    assert message.data == [0, 255, 97, 114, 114, 97, 121, 32, 112, 97, 121, 108, 111, 97, 100]
    assert message.layout.data_offset == 2
    assert [(d.label, d.size, d.stride) for d in message.layout.dim] == [('维度', 2, 4)]
    assert info['publisher_gid'] == bytes.fromhex('010f7f014c00961e0000000000000603')
    assert info['publication_sequence_number'] == 3
    assert info['rcl_take_receipt']['message_type'] == 'std_msgs/msg/UInt8MultiArray'
    assert native.calls == ['array_create', ('take', 0x1000), 'array_convert', 'array_destroy']
    assert handle.enters == handle.exits == 1 and handle.stock_takes == 0


@pytest.mark.parametrize('code', [401, 11])
def test_array_no_sample_or_error_destroys_once_without_second_take(code):
    handle = Handle()
    native = ArrayNative(handle)
    native.code = code
    bridge = b.RclTakeBridge(message_type='std_msgs/msg/UInt8MultiArray',
                             _bindings=native, _array_type=UInt8MultiArray)
    subscription = SimpleNamespace(handle=handle, msg_type=UInt8MultiArray, raw=False)
    if code == 401:
        assert bridge.take(subscription) is None
    else:
        with pytest.raises(b.BridgeError, match='rcl_take_failed:11'):
            bridge.take(subscription)
    assert 'array_convert' not in native.calls
    assert native.calls.count('array_destroy') == 1
    assert len(take_calls(native)) == 1 and handle.stock_takes == 0
