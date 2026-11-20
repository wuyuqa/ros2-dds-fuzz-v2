"""Pinned Jazzy/FastDDS String take observer, NOT a second subscription/take.

Use ``make_full_info_executor(context=ctx, rmw_identifier='rmw_fastrtps_cpp')``
INSTEAD OF the worker's SingleThreadedExecutor. Other executor behavior is inherited.
Do not spin the same subscription with another executor. Shutdown the executor before
closing its ``take_bridge``. Native availability failure is explicit: callers may
choose the ORIGINAL executor, never run both or retry a consumed sample through it.

Only the public rcl_take consumption point is replaced. No ROS imports/initialization
at module import; no publisher, subscription, context or message-body replacement.
These pins describe one installed x86_64 Jazzy ABI, not portable ABI discovery.
"""
import ctypes as C
import hashlib
import json
from pathlib import Path
import platform
import sys
import threading
import time
import uuid

from .message_info_receipt import capture_message_info, UINT64_MAX

PREFIX = Path('/opt/ros/jazzy')
RCL_RET_SUBSCRIPTION_TAKE_FAILED = 401
RMW_IDENTIFIER = 'rmw_fastrtps_cpp'
# Generated from the inspected installation; a different install must be reviewed,
# not accepted by silently generating a new pin at startup.
_PINS = {'include/rcl/rcl/subscription.h': '93e55501a52805a8e57c04d918fdb484d4942e4248954e99a7865a6c5d490871',
 'include/rcl/rcl/types.h': '4c51e951f716723f8d0aa7dd331feb047d60c69f66c15c4dd907d22ca4bb1ddd',
 'include/rcl/rcl/error_handling.h': 'a96376e765c5ab9c09f9385cfea7e178ed8d5bfa3da5c41bbfcdc46387df68a4',
 'include/rmw/rmw/types.h': 'c1e7be9e420d3c32cf6b24db62c3fbd6b97d0518811f6f4677db09d835cf3830',
 'include/rmw/rmw/time.h': '5a59df5de3ef1d4a97b9f7fdc7351118b825e0353d77937aaf9e3c63b8f18d32',
 'include/rcutils/rcutils/time.h': '501c759c84aacac6eeab95f40df73ebeff9866448bcac882b57ae9057b857808',
 'include/rcutils/rcutils/error_handling.h': 'e5049ca840b95857b757a4efe9a2d6d5984486229615f467ba1b07d3aeccfa03',
 'include/rosidl_runtime_c/rosidl_runtime_c/string.h': 'ba984355db95669027fd0b1a2905da35a87cec503bad2933ffab8504319a743c',
 'include/std_msgs/std_msgs/msg/detail/string__struct.h': '97bd1c97baee5bbad0ecef48a18d00f677986380e7ec9d9f37292c4b5385e8fd',
 'include/std_msgs/std_msgs/msg/detail/string__functions.h': '72fc1dbf4329cee3a413e33f40d0dd89bd9b567721137103d2937a8d348b9b72',
 'lib/librcl.so': '94570a4bea52192aa768fb6600fffb7fa05df779c324328d077d63ce062f5fb3',
 'lib/librmw.so': '9e03d0c31f03b2ce01d0d934ba76bf349faa226e6b6ea69a51f62aeddfe018ae',
 'lib/librcutils.so': '9aedab61c0b1265c476fe12bf36d2d31bfc5fe3813c85a67b70406406a0567bc',
 'lib/libstd_msgs__rosidl_generator_c.so': '467ff4c37471e199a9698dcf841e3f3f1ff2ef2c044e72358a7421a7266417d0',
 'lib/librmw_fastrtps_cpp.so': '7b59ab082ef48bd12d5e2d598fed9b4cc5a1c6668493902c6a03f617acbd021f',
 'lib/librmw_fastrtps_shared_cpp.so': '47e1965e31d15809089ce145a42850dce41522d8def367a4a899e927b1842bd1',
 'lib/libfastrtps.so': '2b80489f57d87ce4832766371c5e9f4b0c6b035dc850b284b9f6afcad4962929',
 'lib/python3.12/site-packages/rclpy/executors.py': '46f052cf43e1fa1040af235f3508a0a8c00d98a2ab59b8dcaed9aceb3921a452',
 'lib/python3.12/site-packages/rclpy/_rclpy_pybind11.cpython-312-x86_64-linux-gnu.so': '53efd7622c109ae6f9ff7ffafc3bffac4856700d39ca51d8689ecaa617a596bd',
 'lib/librosidl_runtime_c.so': '446f57672940d74c277f353132f32f61b30ba4c83621fb1be1d97ae1cadf62aa',
 'lib/python3.12/site-packages/rclpy/subscription.py': 'b21a0f4ef3bd60583b569d912ab60a20377ba7b0f3213af43d29403382630ef9'}


class BridgeError(RuntimeError):
    """A bridge failure is never grounds for a second/fallback take."""


class BridgeUnavailable(BridgeError):
    def __init__(self, reason):
        self.capability_receipt = {
            'schema': 'r2f.rcl-take-capability/v1', 'status': 'unknown',
            'reason': reason, 'api': 'public rcl_take', 'fallback_installed': False,
            'candidate_eligible': False,
        }
        super().__init__(reason)


class _RosString(C.Structure):
    _fields_ = [('data', C.c_void_p), ('size', C.c_size_t), ('capacity', C.c_size_t)]


class _String(C.Structure):
    _fields_ = [('data', _RosString)]


class _Gid(C.Structure):
    # Local public rmw/types.h has RMW_GID_STORAGE_SIZE == 16, NOT 24.
    _fields_ = [('implementation_identifier', C.c_char_p), ('data', C.c_uint8 * 16)]


class _MessageInfo(C.Structure):
    _fields_ = [('source_timestamp', C.c_int64), ('received_timestamp', C.c_int64),
                ('publication_sequence_number', C.c_uint64),
                ('reception_sequence_number', C.c_uint64), ('publisher_gid', _Gid),
                ('from_intra_process', C.c_bool)]


class _ErrorString(C.Structure):
    _fields_ = [('str', C.c_char * 1024)]


def _layout():
    return {name: {'size': C.sizeof(cls), 'alignment': C.alignment(cls),
                   'offsets': {key: getattr(cls, key).offset for key, *_ in cls._fields_}}
            for name, cls in [('String', _String), ('RosString', _RosString),
                              ('Gid', _Gid), ('MessageInfo', _MessageInfo),
                              ('ErrorString', _ErrorString)]}


_LAYOUT = {
    'String': {'size': 24, 'alignment': 8, 'offsets': {'data': 0}},
    'RosString': {'size': 24, 'alignment': 8,
                  'offsets': {'data': 0, 'size': 8, 'capacity': 16}},
    'Gid': {'size': 24, 'alignment': 8,
            'offsets': {'implementation_identifier': 0, 'data': 8}},
    'MessageInfo': {'size': 64, 'alignment': 8,
                    'offsets': {'source_timestamp': 0, 'received_timestamp': 8,
                                'publication_sequence_number': 16,
                                'reception_sequence_number': 24,
                                'publisher_gid': 32, 'from_intra_process': 56}},
    'ErrorString': {'size': 1024, 'alignment': 1, 'offsets': {'str': 0}},
}


def verify_abi():
    """Hash/size/offset check only; no native calls or ROS initialization."""
    if (sys.platform != 'linux' or platform.machine() != 'x86_64'
            or sys.byteorder != 'little' or C.sizeof(C.c_void_p) != 8):
        raise BridgeUnavailable('unsupported_platform_abi')
    if _layout() != _LAYOUT:
        raise BridgeUnavailable('ctypes_struct_layout_mismatch')
    for relative, expected in _PINS.items():
        try:
            actual = hashlib.sha256((PREFIX / relative).read_bytes()).hexdigest()
        except OSError as exc:
            raise BridgeUnavailable('abi_file_unavailable:' + relative) from exc
        if actual != expected:
            raise BridgeUnavailable('abi_file_hash_mismatch:' + relative)
    return {'schema': 'r2f.rcl-take-abi/v1', 'status': 'pinned',
            'platform': 'linux-x86_64-little-endian', 'gid_storage_size': 16,
            'layout': _layout(), 'files': dict(_PINS),
            'pin_sha256': hashlib.sha256(json.dumps(
                [_PINS, _LAYOUT], sort_keys=True).encode()).hexdigest()}


def _verify_loaded_libraries():
    """Reject same-SONAME overrides: disk pins alone do not prove mapped identity."""
    expected = {(PREFIX / name).name.split('.so')[0]: (PREFIX / name).resolve()
                for name in _PINS if name.startswith('lib/') and '.so' in name}
    try:
        lines = Path('/proc/self/maps').read_text().splitlines()
        for line in lines:
            fields = line.split(maxsplit=5)
            if len(fields) != 6 or not fields[5].startswith('/'):
                continue
            mapped = Path(fields[5])
            key = mapped.name.split('.so')[0]
            if key in expected and mapped.resolve() != expected[key]:
                raise BridgeUnavailable('mapped_library_identity_mismatch:' + key)
    except OSError as exc:
        raise BridgeUnavailable('mapped_library_identity_unavailable') from exc


def _bind(library, name, args, result):
    function = getattr(library, name)
    function.argtypes = args
    function.restype = result
    return function


class _Bindings:
    def __init__(self, rmw_identifier):
        if rmw_identifier != RMW_IDENTIFIER:
            raise BridgeUnavailable('unsupported_rmw_identifier:' + str(rmw_identifier))
        self.abi = verify_abi()  # Must precede loading/dereferencing any ABI.
        try:
            # Absolute paths; retain CDLLs throughout the bridge's lifetime.
            self.libs = {name: C.CDLL(str(PREFIX / ('lib/lib' + name + '.so')))
                         for name in ('rcl', 'rmw_fastrtps_cpp', 'rcutils', 'std_msgs__rosidl_generator_c')}
            self.take = _bind(self.libs['rcl'], 'rcl_take',
                              [C.c_void_p, C.POINTER(_String), C.POINTER(_MessageInfo),
                               C.c_void_p], C.c_int)
            generator = self.libs['std_msgs__rosidl_generator_c']
            self.init = _bind(generator, 'std_msgs__msg__String__init',
                              [C.POINTER(_String)], C.c_bool)
            self.fini = _bind(generator, 'std_msgs__msg__String__fini',
                              [C.POINTER(_String)], None)
            self.reset_error = _bind(self.libs['rcutils'], 'rcutils_reset_error', [], None)
            self.error_string = _bind(self.libs['rcutils'], 'rcutils_get_error_string',
                                      [], _ErrorString)
            # librmw.so contains core utilities, not this backend entry point.
            # Query the already-pinned FastDDS vendor DSO directly; do not resolve
            # through librmw_implementation or guess from environment variables.
            get_identifier = _bind(self.libs['rmw_fastrtps_cpp'], 'rmw_get_implementation_identifier',
                                   [], C.c_char_p)
            _verify_loaded_libraries()
            identifier = get_identifier()
            _verify_loaded_libraries()
            if identifier != rmw_identifier.encode('ascii'):
                raise BridgeUnavailable('loaded_rmw_identifier_mismatch')
        except BridgeUnavailable:
            raise
        except (OSError, AttributeError) as exc:
            raise BridgeUnavailable('native_load_or_symbol_failure:' + type(exc).__name__) from exc

    def error(self):
        return bytes(self.error_string().str).decode('utf-8', errors='replace')


def _metadata(native, rmw_identifier):
    """Copy ALL public fields while the native message/handle is still alive."""
    raw = {name: int(getattr(native, name)) for name in (
        'source_timestamp', 'received_timestamp', 'publication_sequence_number',
        'reception_sequence_number')}
    identifier = native.publisher_gid.implementation_identifier
    gid = bytes(native.publisher_gid.data)
    if identifier != rmw_identifier.encode('ascii'):
        gid, status = None, 'invalid_implementation_identifier'
    elif not any(gid[:12]) or not any(gid[12:]) or gid[-1] not in (0x02, 0x03):
        gid, status = None, 'invalid_gid'
    else:
        status = 'available'
    # Preserve unsupported sentinels in receipt, but match rclpy dict's None API.
    receipt = capture_message_info(raw)
    receipt.update(api='public rcl_take / rmw_message_info_t (ctypes ABI-pinned)',
                   publisher_gid={'status': status,
                                  'value': gid.hex() if gid is not None else None,
                                  'raw_value': bytes(native.publisher_gid.data).hex(),
                                  'storage_size': 16,
                                  'implementation_identifier': (
                                      identifier.decode('ascii', errors='replace')
                                      if identifier is not None else None)},
                   from_intra_process=bool(native.from_intra_process),
                   identity_qualified=False,
                   identity_limitation='direct_local_rmw_gid_requires_producer_and_cross_context_binding')
    info = dict(raw)
    for name in ('publication_sequence_number', 'reception_sequence_number'):
        if info[name] == UINT64_MAX:
            info[name] = None
    info.update(publisher_gid=gid, publisher_gid_status=status,
                identity_basis='message_info_gid' if gid is not None else 'unavailable',
                basis='public_rcl_take_rmw_message_info',
                from_intra_process=bool(native.from_intra_process),
                message_info_receipt=receipt)
    return info


class RclTakeBridge:
    """Single take owner. Native bindings/type injection is for offline tests only.

    ``take`` does not invoke callbacks. A successful take is consumed exactly once,
    even if decoding/callback subsequently raises. close() never takes or destroys
    any ROS entity; it prevents further reads and waits for an active take to finish.
    """
    def __init__(self, *, rmw_identifier=RMW_IDENTIFIER, _bindings=None, _string_type=None):
        if rmw_identifier != RMW_IDENTIFIER:
            raise BridgeUnavailable('unsupported_rmw_identifier:' + str(rmw_identifier))
        self._bindings = _bindings if _bindings is not None else _Bindings(rmw_identifier)
        if _string_type is None:
            try:
                from std_msgs.msg import String
            except ImportError as exc:
                raise BridgeUnavailable('std_msgs_python_type_unavailable') from exc
            _string_type = String
        self._string_type = _string_type
        self.rmw_identifier = rmw_identifier
        self.abi_receipt = self._bindings.abi
        self._lock = threading.Lock()
        self._closed = False
        self._take_index = 0
        self._bridge_id = uuid.uuid4().hex

    def close(self):
        with self._lock:
            self._closed = True

    def take(self, subscription):
        if subscription.msg_type is not self._string_type or subscription.raw is not False:
            raise BridgeUnavailable('only_std_msgs_msg_String_raw_false_supported')
        if not self._lock.acquire(blocking=False):
            raise BridgeError('concurrent_or_reentrant_take_not_supported')
        try:
            if self._closed:
                raise BridgeError('bridge_closed')
            # Fresh handle AND pointer for every take, including CFT recreation.
            handle = subscription.handle
            with handle:
                pointer = handle.pointer
                if type(pointer) is not int or pointer <= 0 or pointer >= 1 << 64:
                    raise BridgeError('invalid_rcl_subscription_pointer')
                message = _String()
                native = _MessageInfo()
                # Unknown values must not accidentally look like sequence 0.
                native.publication_sequence_number = UINT64_MAX
                native.reception_sequence_number = UINT64_MAX
                self._take_index += 1
                started = time.monotonic_ns()
                try:
                    if not self._bindings.init(C.byref(message)):
                        error = self._bindings.error()
                        raise BridgeError('String_init_failed:' + error)
                    code = self._bindings.take(pointer, C.byref(message), C.byref(native), None)
                    returned = time.monotonic_ns()
                    if code == RCL_RET_SUBSCRIPTION_TAKE_FAILED:
                        self._bindings.reset_error()
                        return None
                    if code != 0:
                        error = self._bindings.error()
                        raise BridgeError('rcl_take_failed:' + str(code) + ':' + error)
                    data = message.data
                    if not data.data or data.capacity <= data.size:
                        raise BridgeError('invalid_native_String_size_or_capacity')
                    if C.string_at(data.data + data.size, 1) != b'\0':
                        raise BridgeError('native_String_not_null_terminated')
                    text = C.string_at(data.data, data.size).decode('utf-8', errors='strict')
                    info = _metadata(native, self.rmw_identifier)
                    result = self._string_type()
                    result.data = text
                    info['rcl_take_receipt'] = {
                        'schema': 'r2f.rcl-take-receipt/v1', 'api': 'public rcl_take',
                        'observation_basis': 'single_take_owner_public_rcl_api',
                        'bridge_id': self._bridge_id, 'take_index': self._take_index,
                        'rcl_subscription_pointer': hex(pointer),
                        'pointer_scope': 'borrowed_fresh_inside_current_subscription_handle',
                        'rmw_identifier': self.rmw_identifier, 'return_code': code,
                        'message_type': 'std_msgs/msg/String', 'raw': False,
                        'take_begin_monotonic_ns': started,
                        'take_return_monotonic_ns': returned,
                        'abi_pin_sha256': self.abi_receipt['pin_sha256'],
                        'message_info_receipt': info['message_info_receipt'],
                        'identity_qualified': False, 'candidate_eligible': False,
                        'original_rclpy_dict_provided_gid': False,
                    }
                    return result, info
                except BaseException:
                    self._bindings.reset_error()
                    raise
                finally:
                    # Zero-initialized String is safe even after partial init failure.
                    # All copied Python values outlive the single native buffer.
                    self._bindings.fini(C.byref(message))
        finally:
            self._lock.release()


def _executor_class(base, await_or_execute, invalid_handle, callback_types):
    """Dependency seam used to test the actual executor override without ROS."""
    class FullInfoSingleThreadedExecutor(base):
        def __init__(self, *, take_bridge, **kwargs):
            self.take_bridge = take_bridge
            super().__init__(**kwargs)

        def _take_subscription(self, sub):
            # Validate before consuming; never call super()._take_subscription.
            kind = sub._callback_type
            if kind not in (callback_types.MessageOnly, callback_types.WithMessageInfo):
                raise BridgeError('unsupported_subscription_callback_type')
            try:
                msg_info = self.take_bridge.take(sub)
                if msg_info is None:
                    return None
                args = (msg_info[0],) if kind is callback_types.MessageOnly else msg_info

                async def _execute():
                    await await_or_execute(sub.callback, *args)

                return _execute
            except invalid_handle:
                return None
    return FullInfoSingleThreadedExecutor


def make_full_info_executor(*, context=None, rmw_identifier=RMW_IDENTIFIER, bridge=None):
    """Create an opt-in executor; raises BridgeUnavailable, NEVER installs fallback.

    On failure main may explicitly use its old executor (GID unavailable). On success
    main must use ONLY this executor for these subscriptions. Receipts distinguish
    this public-API bridge from the unchanged stock rclpy callback dictionary.
    """
    # Validate installed executor source before depending on its private take hook.
    verify_abi()
    try:
        from rclpy.executors import SingleThreadedExecutor, await_or_execute
        from rclpy.exceptions import InvalidHandle
        from rclpy.subscription import Subscription
    except ImportError as exc:
        raise BridgeUnavailable('rclpy_executor_unavailable') from exc
    if bridge is not None and bridge.rmw_identifier != rmw_identifier:
        raise BridgeUnavailable('bridge_executor_rmw_identifier_mismatch')
    owned = bridge is None
    if owned:
        bridge = RclTakeBridge(rmw_identifier=rmw_identifier)
    cls = _executor_class(SingleThreadedExecutor, await_or_execute,
                          InvalidHandle, Subscription.CallbackType)
    try:
        return cls(context=context, take_bridge=bridge)
    except BaseException:
        if owned:
            bridge.close()
        raise
