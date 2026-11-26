"""Pinned Jazzy/FastDDS String/UInt8MultiArray take observer, NOT a second subscription/take.

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
from copy import deepcopy
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


# Additional generated allocation/conversion support for the array path only.
_ARRAY_PINS = {'lib/python3.12/site-packages/std_msgs/std_msgs_s__rosidl_typesupport_c.so': 'a9674b2533ffed903a6d321928bda76d92209dab0579affa5a83b102b7fa9c68', 'lib/python3.12/site-packages/std_msgs/msg/_u_int8_multi_array.py': '1548da44f8123c15a709936dbfa2dfab9e9c0c9e08399bc923a8a6dabd4e61f5', 'include/std_msgs/std_msgs/msg/detail/u_int8_multi_array__struct.h': '3b49cf4c400bcca26b3d4fd30af895cbaccd262e1defc1330619d846a051a22c', 'include/std_msgs/std_msgs/msg/detail/multi_array_layout__struct.h': 'a37fa4799b04609cdaedf0c25dbcaa4e5f1ff6675323e3bc73c950cd259bee14', 'include/std_msgs/std_msgs/msg/detail/multi_array_dimension__struct.h': '13b16d611524b55fc2548702bd138e6d816144a9bda9e5944c24748296fa14ce'}

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


def _pin_selection(instrumentation):
    """Select only the vendor pin; never mutate the installed pin table."""
    files = dict(_PINS)
    paths = {name: PREFIX / name for name in files}
    reference, vendor = None, None
    if instrumentation is not None:
        try:
            from r2f.instrumentation.fastdds_peer.activation import manifest_for_reference
            manifest = manifest_for_reference(instrumentation)
            reference = dict(instrumentation)
            vendor = dict(manifest['binaries']['vendor'])
            path = Path(vendor['path']).resolve(strict=True)
            if 'lib/libfastrtps.so' not in files:
                raise BridgeUnavailable('instrumentation_vendor_pin_missing')
            files['lib/libfastrtps.so'] = vendor['sha256']
            paths['lib/libfastrtps.so'] = path
        except BridgeUnavailable:
            raise
        except (ImportError, OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise BridgeUnavailable('instrumentation_manifest_unavailable:' + str(exc)) from exc
    return files, paths, reference, vendor


def verify_abi(instrumentation=None):
    """Hash/size/offset check only; explicit instrumentation loads no native code."""
    if (sys.platform != 'linux' or platform.machine() != 'x86_64'
            or sys.byteorder != 'little' or C.sizeof(C.c_void_p) != 8):
        raise BridgeUnavailable('unsupported_platform_abi')
    if _layout() != _LAYOUT:
        raise BridgeUnavailable('ctypes_struct_layout_mismatch')
    files, paths, reference, vendor = _pin_selection(instrumentation)
    for relative, expected in files.items():
        try:
            actual = hashlib.sha256(paths[relative].read_bytes()).hexdigest()
        except OSError as exc:
            raise BridgeUnavailable('abi_file_unavailable:' + relative) from exc
        if actual != expected:
            raise BridgeUnavailable('abi_file_hash_mismatch:' + relative)
    identity = [files, _LAYOUT]
    if reference is not None:
        identity += [reference, {name: str(path) for name, path in paths.items()}]
    return {'schema': 'r2f.rcl-take-abi/v1', 'status': 'pinned',
            'platform': 'linux-x86_64-little-endian', 'gid_storage_size': 16,
            'layout': _layout(), 'files': files,
            'observation_mode': 'instrumentation' if reference is not None else 'blackbox',
            'manifest_reference': reference, 'vendor_binary': vendor,
            'file_paths': {name: str(path) for name, path in paths.items()},
            'pin_sha256': hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()}


def _message_abi(message_type, instrumentation=None):
    """One receipt constructor for binding assembly and per-take revalidation."""
    abi = verify_abi() if instrumentation is None else verify_abi(instrumentation=instrumentation)
    if message_type == 'std_msgs/msg/UInt8MultiArray':
        for relative, expected in _ARRAY_PINS.items():
            try:
                actual = hashlib.sha256((PREFIX / relative).read_bytes()).hexdigest()
            except OSError as exc:
                raise BridgeUnavailable('array_abi_file_unavailable:' + relative) from exc
            if actual != expected:
                raise BridgeUnavailable('array_abi_file_hash_mismatch:' + relative)
        abi['array_generated_support'] = dict(_ARRAY_PINS)
        abi['pin_sha256'] = hashlib.sha256(json.dumps(
            [abi['pin_sha256'], _ARRAY_PINS], sort_keys=True).encode()).hexdigest()
    return abi


def _verify_loaded_libraries(instrumentation=None):
    """Keep every installed path gate; only explicit vendor selection differs."""
    _, paths, reference, _ = _pin_selection(instrumentation)
    expected = {(PREFIX / name).name.split('.so')[0]: path.resolve()
                for name, path in paths.items() if name.startswith('lib/') and '.so' in name}
    # Classify by the installed SONAME, not the selected binary's basename.
    if 'lib/libfastrtps.so' in paths:
        vendor = paths['lib/libfastrtps.so'].resolve()
        expected.update(libfastrtps=vendor, libfastdds=vendor)
    records = []
    try:
        text = Path('/proc/self/maps').read_text()
        for line in text.splitlines():
            fields = line.split(maxsplit=5)
            if len(fields) != 6 or not fields[5].startswith('/'):
                continue
            mapped = Path(fields[5])
            key = mapped.name.split('.so')[0]
            if key in expected:
                if mapped.resolve() != expected[key]:
                    raise BridgeUnavailable('mapped_library_identity_mismatch:' + key)
                record = {'path': str(mapped.resolve()), 'device': fields[3], 'inode': fields[4]}
                if record not in records:
                    records.append(record)
    except OSError as exc:
        raise BridgeUnavailable('mapped_library_identity_unavailable') from exc
    vendor_identity = None
    if reference is not None:
        try:
            from r2f.instrumentation.fastdds_peer.activation import verify_vendor_mapping
            vendor_identity = verify_vendor_mapping(reference, maps_text=text)
        except (ImportError, OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise BridgeUnavailable('instrumentation_vendor_mapping_unavailable:' + str(exc)) from exc
    return {'manifest_reference': reference, 'mapped_libraries': records,
            'vendor_identity': vendor_identity}


def _bind(library, name, args, result):
    function = getattr(library, name)
    function.argtypes = args
    function.restype = result
    return function


class _Bindings:
    def __init__(self, rmw_identifier, instrumentation=None, message_type='std_msgs/msg/String'):
        _bridge_options(message_type, instrumentation)
        if rmw_identifier != RMW_IDENTIFIER:
            raise BridgeUnavailable('unsupported_rmw_identifier:' + str(rmw_identifier))
        self.instrumentation = deepcopy(instrumentation)
        # Preserve the blackbox call shape as well as its unchanged pin digest.
        self.abi = _message_abi(message_type, self.instrumentation)
        try:
            # Absolute paths; retain CDLLs throughout the bridge's lifetime.
            self.libs = {name: C.CDLL(str(PREFIX / ('lib/lib' + name + '.so')))
                         for name in ('rcl', 'rmw_fastrtps_cpp', 'rcutils', 'std_msgs__rosidl_generator_c')}
            self.take = _bind(self.libs['rcl'], 'rcl_take',
                              [C.c_void_p, (C.c_void_p if message_type == 'std_msgs/msg/UInt8MultiArray'
                                            else C.POINTER(_String)), C.POINTER(_MessageInfo),
                               C.c_void_p], C.c_int)
            generator = self.libs['std_msgs__rosidl_generator_c']
            self.init = _bind(generator, 'std_msgs__msg__String__init',
                              [C.POINTER(_String)], C.c_bool)
            self.fini = _bind(generator, 'std_msgs__msg__String__fini',
                              [C.POINTER(_String)], None)
            if message_type == 'std_msgs/msg/UInt8MultiArray':
                from std_msgs.msg import UInt8MultiArray
                UInt8MultiArray.__class__.__import_type_support__()
                # Same generated converter used by rclpy. PYFUNCTYPE retains the
                # GIL while constructing a Python message; no ctypes array ABI
                # or manually reconstructed nested layout is assumed.
                capsule_pointer = C.pythonapi.PyCapsule_GetPointer
                capsule_pointer.argtypes = [C.py_object, C.c_char_p]
                capsule_pointer.restype = C.c_void_p
                address = capsule_pointer(UInt8MultiArray.__class__._CONVERT_TO_PY, None)
                if not address:
                    raise BridgeUnavailable('array_generated_converter_unavailable')
                self.array_convert = C.PYFUNCTYPE(C.py_object, C.c_void_p)(address)
                self.array_create = _bind(generator, 'std_msgs__msg__UInt8MultiArray__create',
                                          [], C.c_void_p)
                self.array_destroy = _bind(generator, 'std_msgs__msg__UInt8MultiArray__destroy',
                                           [C.c_void_p], None)
            self.reset_error = _bind(self.libs['rcutils'], 'rcutils_reset_error', [], None)
            self.error_string = _bind(self.libs['rcutils'], 'rcutils_get_error_string',
                                      [], _ErrorString)
            # librmw.so contains core utilities, not this backend entry point.
            # Query the already-pinned FastDDS vendor DSO directly; do not resolve
            # through librmw_implementation or guess from environment variables.
            get_identifier = _bind(self.libs['rmw_fastrtps_cpp'], 'rmw_get_implementation_identifier',
                                   [], C.c_char_p)
            if self.instrumentation is None:
                _verify_loaded_libraries()
            else:
                self.abi.update(_verify_loaded_libraries(instrumentation=self.instrumentation))
            identifier = get_identifier()
            if self.instrumentation is None:
                _verify_loaded_libraries()
            else:
                self.abi.update(_verify_loaded_libraries(instrumentation=self.instrumentation))
            if identifier != rmw_identifier.encode('ascii'):
                raise BridgeUnavailable('loaded_rmw_identifier_mismatch')
        except BridgeUnavailable:
            raise
        except (ImportError, OSError, AttributeError, TypeError, ValueError) as exc:
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


def _bridge_options(message_type, instrumentation):
    if message_type not in ('std_msgs/msg/String', 'std_msgs/msg/UInt8MultiArray'):
        raise BridgeUnavailable('native_take_message_type_not_supported')
    if instrumentation is not None and message_type != 'std_msgs/msg/String':
        # Only the explicit, independently qualified local-API build is in the
        # array scope. Legacy peer/filter/PDP builds retain their String scope.
        from r2f.instrumentation.fastdds_peer.activation import manifest_for_reference
        try:
            manifest = manifest_for_reference(instrumentation)
            if ('local_api' not in manifest or manifest.get('filter_trace') is not None
                    or manifest.get('pdp_trace') is not None):
                raise ValueError('array_requires_local_api_without_trace_extensions')
        except (ValueError, OSError, TypeError, KeyError) as exc:
            raise BridgeUnavailable('instrumented_array_take_not_qualified:' + str(exc)) from exc
    options = {}
    if instrumentation is not None:
        options['instrumentation'] = instrumentation
    if message_type != 'std_msgs/msg/String':
        options['message_type'] = message_type
    return options


class RclTakeBridge:
    """Single take owner. Native bindings/type injection is for offline tests only.

    ``take`` does not invoke callbacks. A successful take is consumed exactly once,
    even if decoding/callback subsequently raises. close() never takes or destroys
    any ROS entity; it prevents further reads and waits for an active take to finish.
    """
    def __init__(self, *, rmw_identifier=RMW_IDENTIFIER, instrumentation=None,
                 _bindings=None, _string_type=None, _array_type=None,
                 message_type='std_msgs/msg/String'):
        options = _bridge_options(message_type, instrumentation)
        self.message_type = message_type
        if rmw_identifier != RMW_IDENTIFIER:
            raise BridgeUnavailable('unsupported_rmw_identifier:' + str(rmw_identifier))
        self.instrumentation = deepcopy(instrumentation)
        self._bindings = (_bindings if _bindings is not None else
                          _Bindings(rmw_identifier, **options))
        if self._bindings.abi.get('manifest_reference') != self.instrumentation:
            raise BridgeUnavailable('bridge_bindings_instrumentation_mismatch')
        selected = _array_type if message_type == 'std_msgs/msg/UInt8MultiArray' else _string_type
        if selected is None:
            try:
                if message_type == 'std_msgs/msg/UInt8MultiArray':
                    from std_msgs.msg import UInt8MultiArray as selected
                else:
                    from std_msgs.msg import String as selected
            except ImportError as exc:
                raise BridgeUnavailable('std_msgs_python_type_unavailable') from exc
        self._message_class = selected
        self.rmw_identifier = rmw_identifier
        self.abi_receipt = self._bindings.abi
        self._lock = threading.Lock()
        self._closed = False
        self._take_index = 0
        self._bridge_id = uuid.uuid4().hex
        self._revalidate_instrumentation()

    def _revalidate_instrumentation(self):
        if self.instrumentation is None:
            return
        fresh = _message_abi(self.message_type, self.instrumentation)
        if (fresh['pin_sha256'] != self.abi_receipt['pin_sha256']
                or fresh['manifest_reference'] != self.abi_receipt['manifest_reference']):
            raise BridgeUnavailable('bridge_instrumentation_identity_changed')
        self.abi_receipt.update(_verify_loaded_libraries(instrumentation=self.instrumentation))

    def close(self):
        with self._lock:
            self._closed = True

    def take(self, subscription):
        if subscription.msg_type is not self._message_class or subscription.raw is not False:
            raise BridgeUnavailable('only_' + self.message_type.replace('/', '_') + '_raw_false_supported')
        if not self._lock.acquire(blocking=False):
            raise BridgeError('concurrent_or_reentrant_take_not_supported')
        try:
            if self._closed:
                raise BridgeError('bridge_closed')
            self._revalidate_instrumentation()
            return self._take_locked(subscription, revalidate_after=True)
        finally:
            self._lock.release()


    def _take_locked(self, subscription, *, revalidate_after):
        """One consumption, with a fresh borrow and native buffer, under our lock."""
        # Fresh handle AND pointer for every take, including CFT recreation.
        handle = subscription.handle
        with handle:
            pointer = handle.pointer
            if type(pointer) is not int or pointer <= 0 or pointer >= 1 << 64:
                raise BridgeError('invalid_rcl_subscription_pointer')
            is_array = self.message_type == 'std_msgs/msg/UInt8MultiArray'
            message = None if is_array else _String()
            native = _MessageInfo()
            # Unknown values must not accidentally look like sequence 0.
            native.publication_sequence_number = UINT64_MAX
            native.reception_sequence_number = UINT64_MAX
            self._take_index += 1
            started = time.monotonic_ns()
            try:
                if is_array:
                    message = self._bindings.array_create()
                    if not message:
                        raise BridgeError('UInt8MultiArray_create_failed:' + self._bindings.error())
                elif not self._bindings.init(C.byref(message)):
                    error = self._bindings.error()
                    raise BridgeError('String_init_failed:' + error)
                code = self._bindings.take(pointer, message if is_array else C.byref(message),
                                           C.byref(native), None)
                returned = time.monotonic_ns()
                if revalidate_after:
                    self._revalidate_instrumentation()
                if code == RCL_RET_SUBSCRIPTION_TAKE_FAILED:
                    self._bindings.reset_error()
                    return None
                if code != 0:
                    error = self._bindings.error()
                    raise BridgeError('rcl_take_failed:' + str(code) + ':' + error)
                if is_array:
                    result = self._bindings.array_convert(message)
                    if type(result) is not self._message_class:
                        raise BridgeError('array_generated_converter_type_mismatch')
                else:
                    data = message.data
                    if not data.data or data.capacity <= data.size:
                        raise BridgeError('invalid_native_String_size_or_capacity')
                    if C.string_at(data.data + data.size, 1) != b'\0':
                        raise BridgeError('native_String_not_null_terminated')
                    result = self._message_class()
                    result.data = C.string_at(data.data, data.size).decode('utf-8', errors='strict')
                info = _metadata(native, self.rmw_identifier)
                info['rcl_take_receipt'] = {
                    'schema': 'r2f.rcl-take-receipt/v1', 'api': 'public rcl_take',
                    'observation_basis': 'single_take_owner_public_rcl_api',
                    'bridge_id': self._bridge_id, 'take_index': self._take_index,
                    'rcl_subscription_pointer': hex(pointer),
                    'pointer_scope': 'borrowed_fresh_inside_current_subscription_handle',
                    'rmw_identifier': self.rmw_identifier, 'return_code': code,
                    'message_type': self.message_type, 'raw': False,
                    'take_begin_monotonic_ns': started,
                    'take_return_monotonic_ns': returned,
                    'abi_pin_sha256': self.abi_receipt['pin_sha256'],
                    'message_info_receipt': info['message_info_receipt'],
                    'identity_qualified': False, 'candidate_eligible': False,
                    'original_rclpy_dict_provided_gid': False,
                }
                if self.instrumentation is not None:
                    info['rcl_take_receipt']['instrumentation'] = {
                        'manifest_reference': deepcopy(self.abi_receipt['manifest_reference']),
                        'vendor_identity': deepcopy(self.abi_receipt['vendor_identity']),
                        'mapped_libraries': deepcopy(self.abi_receipt['mapped_libraries']),
                        'abi_pin_sha256': self.abi_receipt['pin_sha256'],
                    }
                return result, info
            except BaseException:
                self._bindings.reset_error()
                raise
            finally:
                # Zero-initialized String is safe even after partial init failure.
                # All copied Python values outlive the single native buffer.
                if is_array:
                    if message:
                        self._bindings.array_destroy(message)
                else:
                    self._bindings.fini(C.byref(message))

    def take_batch(self, subscription, *, max_samples=64, max_duration_ns=10_000_000):
        """Bounded single-owner drain; no result escapes until full post-validation.

        The budget covers data draining only, excluding expensive verification.
        Each sample retains its actual rcl_take begin/return times. Full bytes,
        manifest and mapping checks bracket the batch, NOT each individual take.
        Errors discard the entire Python batch; consumed samples are never retried.
        """
        if (type(max_samples) is not int or not 1 <= max_samples <= 64
                or type(max_duration_ns) is not int or not 1 <= max_duration_ns <= 50_000_000):
            raise BridgeError('invalid_take_batch_budget')
        if subscription.msg_type is not self._message_class or subscription.raw is not False:
            raise BridgeUnavailable('only_' + self.message_type.replace('/', '_') + '_raw_false_supported')
        if not self._lock.acquire(blocking=False):
            raise BridgeError('concurrent_or_reentrant_take_not_supported')
        try:
            if self._closed:
                raise BridgeError('bridge_closed')
            before_started = time.monotonic_ns()
            self._revalidate_instrumentation()
            before_finished = time.monotonic_ns()
            before = deepcopy(self.abi_receipt)
            batch_id = uuid.uuid4().hex
            samples = []
            primary = None
            drain_started = time.monotonic_ns()
            deadline = drain_started + max_duration_ns
            try:
                for _ in range(max_samples):
                    # Always permit the first attempt; check fairness between takes.
                    if samples and time.monotonic_ns() >= deadline:
                        break
                    sample = self._take_locked(subscription, revalidate_after=False)
                    if sample is None:
                        break
                    samples.append(sample)
            except BaseException as exc:
                primary = exc
                raise
            finally:
                drain_finished = time.monotonic_ns()
                after_started = time.monotonic_ns()
                try:
                    self._revalidate_instrumentation()
                    after_finished = time.monotonic_ns()
                    after = deepcopy(self.abi_receipt)
                except BaseException as validation_error:
                    if primary is None:
                        raise
                    # InvalidHandle alone is routinely ignored by the executor.
                    # A failed mandatory final proof must never take that path.
                    failure = BridgeError('take_batch_and_post_validation_failed:' + str(validation_error))
                    failure.post_validation_error = validation_error
                    raise failure from primary
            proof = {
                'schema': 'r2f.rcl-take-batch-validation/v1', 'batch_id': batch_id,
                'bridge_id': self._bridge_id, 'validation_scope': 'batch_before_after',
                'max_samples': max_samples, 'max_duration_ns': max_duration_ns,
                'budget_scope': 'data_drain_only_checked_between_takes',
                'sample_count': len(samples),
                'before_validation_started_ns': before_started,
                'before_validation_finished_ns': before_finished,
                'drain_started_ns': drain_started, 'drain_finished_ns': drain_finished,
                'after_validation_started_ns': after_started,
                'after_validation_finished_ns': after_finished,
                'before_abi': before, 'after_abi': after,
                'manifest_reference': deepcopy(self.instrumentation),
                'candidate_eligible': False, 'filter_effect_observed': None,
            }
            for ordinal, (_, info) in enumerate(samples, 1):
                receipt = info['rcl_take_receipt']
                receipt['batch_ordinal'] = ordinal
                receipt['batch_validation'] = deepcopy(proof)
                if self.instrumentation is not None:
                    receipt['instrumentation']['validation_scope'] = 'batch_before_after'
            return samples
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
                batch_take = getattr(self.take_bridge, 'take_batch', None)
                if (getattr(self.take_bridge, 'instrumentation', None) is not None
                        and callable(batch_take)):
                    samples = batch_take(sub)
                else:
                    # Blackbox/legacy and injected bridges without batch stay single-take.
                    msg_info = self.take_bridge.take(sub)
                    samples = [] if msg_info is None else [msg_info]
                if not samples:
                    return None

                async def _execute():
                    for msg_info in samples:
                        args = ((msg_info[0],) if kind is callback_types.MessageOnly else msg_info)
                        await await_or_execute(sub.callback, *args)

                return _execute
            except invalid_handle:
                return None
    return FullInfoSingleThreadedExecutor


def make_full_info_executor(*, context=None, rmw_identifier=RMW_IDENTIFIER, bridge=None,
                            instrumentation=None, message_type='std_msgs/msg/String'):
    """Create an opt-in executor; raises BridgeUnavailable, NEVER installs fallback.

    On failure main may explicitly use its old executor (GID unavailable). On success
    main must use ONLY this executor for these subscriptions. Receipts distinguish
    this public-API bridge from the unchanged stock rclpy callback dictionary.
    """
    options = _bridge_options(message_type, instrumentation)
    # Validate installed executor source before depending on its private take hook.
    if instrumentation is None:
        verify_abi()
    else:
        verify_abi(instrumentation=instrumentation)
    try:
        from rclpy.executors import SingleThreadedExecutor, await_or_execute
        from rclpy.exceptions import InvalidHandle
        from rclpy.subscription import Subscription
    except ImportError as exc:
        raise BridgeUnavailable('rclpy_executor_unavailable') from exc
    if bridge is not None and bridge.message_type != message_type:
        raise BridgeUnavailable('bridge_executor_message_type_mismatch')
    if bridge is not None and bridge.rmw_identifier != rmw_identifier:
        raise BridgeUnavailable('bridge_executor_rmw_identifier_mismatch')
    if bridge is not None and getattr(bridge, 'instrumentation', None) != instrumentation:
        raise BridgeUnavailable('bridge_executor_instrumentation_mismatch')
    owned = bridge is None
    if owned:
        bridge = RclTakeBridge(rmw_identifier=rmw_identifier, **options)
    elif instrumentation is not None:
        bridge._revalidate_instrumentation()
    cls = _executor_class(SingleThreadedExecutor, await_or_execute,
                          InvalidHandle, Subscription.CallbackType)
    try:
        return cls(context=context, take_bridge=bridge)
    except BaseException:
        if owned:
            bridge.close()
        raise
