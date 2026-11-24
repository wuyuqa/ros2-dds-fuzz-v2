"""Observe the *existing* Fast DDS publish call; never issue another write.

The installed Jazzy rmw_fastrtps implementation passes the same source stamp
into ros2:rmw_publish and DataWriter::write_w_timestamp. A process-local UST
probe copies that stamp inside an explicit, same-thread Python publish scope.
No trace timestamp/order is substituted for a DDS sample sequence number.
The receipt is only a producer observation: a later consumer must independently
join direct receiver GID/stamp/pubseq, reject collisions, and close its window.

Native ABI support is intentionally pinned. Unsupported libraries still use
ordinary Publisher.publish, but get an unknown observation, never fake identity.
"""
from __future__ import annotations

import ctypes as C
import hashlib
from pathlib import Path
import platform
import threading
import time
import uuid

SCHEMA = 'r2f.rmw-publish-occurrence/v1'
RMW = 'rmw_fastrtps_cpp'
TRACE_API = 'ros2:rmw_publish'
IDENTITY_API = 'rcl_publisher_get_rmw_handle+rmw_get_gid_for_publisher'
SIGNATURE = b'const void *, rmw_publisher_handle_arg, const void *, message_arg, int64_t, timestamp_arg'
# Verified against the installed public headers and the no-DDS trace ABI probe.
PINNED_FILES = {
    '/opt/ros/jazzy/lib/libtracetools.so': 'e7203baf1ff36817a90dfb59364d0678b941174f4451ee488868d87b7e9f23fd',
    '/lib/x86_64-linux-gnu/liblttng-ust-tracepoint.so.1': 'c5a75aea47fbfa1380e75fad869a00e225a947bbfa99edc6c62c2241959e1eb8',
    '/opt/ros/jazzy/lib/librcl.so': '94570a4bea52192aa768fb6600fffb7fa05df779c324328d077d63ce062f5fb3',
    '/opt/ros/jazzy/lib/librmw_fastrtps_cpp.so': '7b59ab082ef48bd12d5e2d598fed9b4cc5a1c6668493902c6a03f617acbd021f',
    '/opt/ros/jazzy/lib/librmw_fastrtps_shared_cpp.so': '47e1965e31d15809089ce145a42850dce41522d8def367a4a899e927b1842bd1',
    '/opt/ros/jazzy/include/rmw/rmw/types.h': 'c1e7be9e420d3c32cf6b24db62c3fbd6b97d0518811f6f4677db09d835cf3830',
    '/opt/ros/jazzy/include/rcl/rcl/publisher.h': '93c0f458a61dcd109e5b544ea562a831a770e328c65891f60de2a7c648bf8a3e',
    '/usr/include/x86_64-linux-gnu/lttng/tracepoint-types.h': 'd723856c62c6b89cfcadcf7ed0a942c3f14ab503783ade405c327da14d4fecef',
    '/opt/ros/jazzy/include/tracetools/tracetools/tp_call.h': 'e106ce948cfaff3e45cf1b3dcb07976d1b10f9e7c7a4f04a96fab3f29824b6ba',
}


class _Tracepoint(C.Structure):
    _fields_ = [('struct_size', C.c_uint32), ('provider', C.c_char_p),
                ('event', C.c_char_p), ('state', C.c_int), ('probes', C.c_void_p),
                ('provider_ref', C.c_void_p), ('signature', C.c_char_p)]


class _Gid(C.Structure):
    # This installation's RMW_GID_STORAGE_SIZE is 16, not the older 24 ABI.
    _fields_ = [('implementation_identifier', C.c_char_p), ('data', C.c_uint8 * 16)]


# A failed native unregistration must not release a callable still known to UST.
_PINNED_FAILED_CLOSE = []
_REGISTERED_NATIVE = {}


def _verify_mapped_identity(maps_text=None):
    expected = {Path(path).name.split('.so')[0]: Path(path).resolve()
                for path in PINNED_FILES if '.so' in Path(path).name}
    text = Path('/proc/self/maps').read_text() if maps_text is None else maps_text
    for line in text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or not fields[5].startswith('/'):
            continue
        path = Path(fields[5])
        key = path.name.split('.so')[0]
        if key in expected and path.resolve() != expected[key]:
            raise RuntimeError('mapped_library_identity_mismatch:' + key)


class NativePublishTrace:
    """Narrow ABI bridge, no ROS/DDS initialization, session or new endpoint."""
    def __init__(self, callback):
        if platform.machine() != 'x86_64' or C.sizeof(C.c_void_p) != 8:
            raise RuntimeError('unsupported_native_architecture')
        for path, expected in PINNED_FILES.items():
            if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
                raise RuntimeError('native_abi_hash_mismatch:' + path)
        if C.sizeof(_Tracepoint) != 56 or C.sizeof(_Gid) != 24 or _Gid.data.offset != 8:
            raise RuntimeError('native_struct_abi_mismatch')
        self.trace = C.CDLL('/opt/ros/jazzy/lib/libtracetools.so')
        self.ust = C.CDLL('/lib/x86_64-linux-gnu/liblttng-ust-tracepoint.so.1')
        self.rcl = C.CDLL('/opt/ros/jazzy/lib/librcl.so')
        self.rmw = C.CDLL('/opt/ros/jazzy/lib/librmw_fastrtps_cpp.so')
        _verify_mapped_identity()
        self.tp = _Tracepoint.in_dll(self.trace, 'lttng_ust_tracepoint_ros2___rmw_publish')
        if (self.tp.struct_size != C.sizeof(_Tracepoint) or self.tp.provider != b'ros2'
                or self.tp.event != b'rmw_publish' or self.tp.signature != SIGNATURE):
            raise RuntimeError('native_tracepoint_signature_mismatch')
        self.rcl.rcl_publisher_get_rmw_handle.argtypes = [C.c_void_p]
        self.rcl.rcl_publisher_get_rmw_handle.restype = C.c_void_p
        self.rmw.rmw_get_gid_for_publisher.argtypes = [C.c_void_p, C.POINTER(_Gid)]
        self.rmw.rmw_get_gid_for_publisher.restype = C.c_int
        self.register = self.ust.lttng_ust_tracepoint_provider_register
        self.register.argtypes = [C.c_char_p, C.c_char_p, C.c_void_p, C.c_void_p, C.c_char_p]
        self.register.restype = C.c_int
        self.unregister = self.ust.lttng_ust_tracepoint_provider_unregister
        self.unregister.argtypes = [C.c_char_p, C.c_char_p, C.c_void_p, C.c_void_p]
        self.unregister.restype = C.c_int
        c_callback = C.CFUNCTYPE(None, C.c_void_p, C.c_void_p, C.c_void_p, C.c_int64)
        # The leading argument is UST probe data, NOT a publisher pointer.
        self.callback = c_callback(lambda data, pub, msg, stamp: callback(pub, msg, stamp))
        self.callback_pointer = C.cast(self.callback, C.c_void_p)
        self.closed = False
        self.provenance = {
            'api': TRACE_API, 'observer': 'existing_ust_tracepoint_process_local_probe',
            'library_and_header_sha256': dict(PINNED_FILES),
            'tracepoint_struct_size': C.sizeof(_Tracepoint), 'rmw_gid_struct_size': C.sizeof(_Gid),
            'signature': SIGNATURE.decode(), 'creates_dds_entities': False,
            'replaces_publish': False, 'scope_binding': 'explicit_same_thread_call_id',
        }
        rc = self.register(b'ros2', b'rmw_publish', self.callback_pointer, None, SIGNATURE)
        if rc != 0:
            raise RuntimeError('tracepoint_register_failed:' + str(rc))
        # UST stores a raw function pointer. Keep its trampoline alive even if
        # the caller drops an observer or unwinds construction unexpectedly.
        _REGISTERED_NATIVE[id(self)] = self

    def publisher_identity(self, rcl_pointer):
        if self.closed or type(rcl_pointer) is not int or rcl_pointer <= 0:
            raise ValueError('invalid_or_closed_publisher_pointer')
        rmw_pointer = self.rcl.rcl_publisher_get_rmw_handle(rcl_pointer)
        if not rmw_pointer:
            raise ValueError('publisher_rmw_handle_unavailable')
        gid = _Gid()
        rc = self.rmw.rmw_get_gid_for_publisher(rmw_pointer, C.byref(gid))
        if rc != 0:
            raise ValueError('publisher_gid_query_failed:' + str(rc))
        if gid.implementation_identifier != RMW.encode() or not any(gid.data):
            raise ValueError('publisher_gid_implementation_or_value_invalid')
        return int(rmw_pointer), bytes(gid.data).hex()

    def close(self):
        if self.closed:
            return 0
        rc = self.unregister(b'ros2', b'rmw_publish', self.callback_pointer, None)
        self.closed = rc == 0
        if rc == 0:
            _REGISTERED_NATIVE.pop(id(self), None)
        else:
            _PINNED_FAILED_CLOSE.append(self)
        # UST unregister performs its native grace period before releasing probes.
        return rc


class PublishOccurrenceObserver:
    """Bind a trace only inside one real Publisher.publish on the same thread."""
    def __init__(self, emit, rmw_identifier, *, native_factory=NativePublishTrace):
        self.emit, self.rmw_identifier = emit, rmw_identifier
        self.native = None
        self.local = threading.local()
        self.out_of_scope_traces = 0
        self.unavailable = 'unsupported_rmw' if rmw_identifier != RMW else None
        if self.unavailable is None:
            try:
                self.native = native_factory(self._on_trace)
            except Exception as exc:
                self.unavailable = 'native_observer_unavailable:' + type(exc).__name__ + ':' + str(exc)
        try:
            self.emit('publish_observer_capability', status='observed' if self.native else 'unavailable',
                      reason=self.unavailable, observation_api=TRACE_API,
                      provenance=self.native.provenance if self.native else None,
                      candidate_eligible=False, filtering_effect_proven=False)
        except BaseException:
            # No caller can close a constructor that did not return. Keep the
            # telemetry exception primary and never drop a registered callback.
            try:
                self.close()
            except BaseException:
                pass
            raise

    def _on_trace(self, publisher, message, stamp):
        scope = getattr(self.local, 'scope', None)
        if scope is None:
            self.out_of_scope_traces += 1
            return
        try:
            trace = {'publisher_rmw_handle': publisher, 'trace_message_handle': message,
                     'source_timestamp': stamp, 'trace_scope_matched': True,
                     'trace_ref': scope['call_id'] + '.trace.' + str(len(scope['traces'])),
                     'thread_id': threading.get_ident(), 'native_thread_id': threading.get_native_id(),
                     'monotonic_ns': time.monotonic_ns()}
            scope['traces'].append(trace)
            self.emit('rmw_publish_trace', **scope['common'], **trace,
                      trace_api=TRACE_API, candidate_eligible=False)
        except BaseException as exc:  # Never unwind Python through the native UST callback.
            scope['callback_error'] = type(exc).__name__

    def publish(self, endpoint, message, *, command_id, send_sequence, writer_gid):
        data = message.data
        payload_sha256 = hashlib.sha256(data.encode('utf-8') if isinstance(data, str) else bytes(data)).hexdigest()
        call_id = uuid.uuid4().hex
        common = {'call_id': call_id, 'command_id': command_id, 'send_sequence': send_sequence,
                  'writer_gid': writer_gid, 'endpoint_gid': writer_gid,
                  'payload_sha256': payload_sha256, 'rmw_identifier': self.rmw_identifier}
        scope = {'call_id': call_id, 'common': common, 'traces': [], 'callback_error': None}
        reason = self.unavailable
        rcl_handle = rmw_handle = actual_gid = None
        outer = getattr(self.local, 'scope', None)
        if outer is not None:
            raise RuntimeError('nested_publish_observation_scope')
        with endpoint.handle:
            if self.native is not None:
                try:
                    rcl_handle = endpoint.handle.pointer
                    rmw_handle, actual_gid = self.native.publisher_identity(rcl_handle)
                    if actual_gid != writer_gid:
                        reason = 'writer_gid_mismatch'
                except Exception as exc:
                    reason = 'publisher_identity_unavailable:' + type(exc).__name__ + ':' + str(exc)
            thread = threading.get_ident()
            native_thread = threading.get_native_id()
            begin = time.monotonic_ns()
            raw_common = {**common, 'rcl_publisher_handle': rcl_handle,
                          'publisher_rmw_handle': rmw_handle, 'observed_writer_gid': actual_gid,
                          'thread_id': thread, 'native_thread_id': native_thread,
                          'identity_api': IDENTITY_API}
            telemetry_error = None
            try:
                self.emit('publish_call_begin', **raw_common, monotonic_ns=begin)
            except Exception as exc:
                telemetry_error = 'telemetry_emit_failed:publish_call_begin:' + type(exc).__name__
            self.local.scope = scope
            returned = False
            error_type = None
            try:
                # This is the original public API, exactly once, with unchanged message data.
                endpoint.publish(message)
                returned = True
            except BaseException as exc:
                error_type = type(exc).__name__
                raise
            finally:
                end = time.monotonic_ns()
                self.local.scope = None
                try:
                    self.emit('publish_call_end', **raw_common, monotonic_ns=end,
                              publish_returned=returned, error_type=error_type,
                              trace_count=len(scope['traces']))
                except Exception as exc:
                    telemetry_error = telemetry_error or ('telemetry_emit_failed:publish_call_end:' + type(exc).__name__)
        reason = reason or telemetry_error
        traces = scope['traces']
        trace = traces[0] if len(traces) == 1 else {}
        if reason is None:
            if scope['callback_error']:
                reason = 'trace_callback_error:' + scope['callback_error']
            elif len(traces) != 1:
                reason = 'trace_count_not_one'
            elif trace['publisher_rmw_handle'] != rmw_handle:
                reason = 'trace_publisher_handle_mismatch'
            elif not trace['trace_message_handle']:
                reason = 'trace_message_handle_missing'
            elif type(trace['source_timestamp']) is not int or trace['source_timestamp'] <= 0:
                reason = 'trace_timestamp_unavailable'
            elif (trace['thread_id'] != thread or trace['native_thread_id'] != native_thread
                  or not begin <= trace['monotonic_ns'] <= end):
                reason = 'trace_outside_explicit_call_scope'
            elif message.data != data:
                reason = 'payload_changed_during_publish'
        return {'schema': SCHEMA, 'status': 'observed' if reason is None else 'unknown',
                'reason': reason, **common, 'publish_returned': returned,
                'rcl_publisher_handle': rcl_handle, 'publisher_rmw_handle': rmw_handle,
                'observed_writer_gid': actual_gid, 'thread_id': thread,
                'native_thread_id': native_thread, 'call_begin_monotonic_ns': begin,
                'call_return_monotonic_ns': end, 'trace_count': len(traces),
                'source_timestamp': trace.get('source_timestamp') if reason is None else None,
                'trace_ref': trace.get('trace_ref'), 'trace_message_handle': trace.get('trace_message_handle'),
                'trace_monotonic_ns': trace.get('monotonic_ns'),
                'trace_scope_matched': reason is None, 'identity_api': IDENTITY_API,
                'trace_api': TRACE_API, 'candidate_eligible': False,
                'filter_effect_observed': None}

    def close(self):
        if self.native is None:
            result = {'status': 'not_registered', 'reason': self.unavailable}
        else:
            try:
                rc = self.native.close()
            except BaseException:
                _PINNED_FAILED_CLOSE.append(self.native)
                raise
            if rc != 0:
                _PINNED_FAILED_CLOSE.append(self.native)
            result = {'status': 'closed' if rc == 0 else 'unknown', 'returncode': rc}
        return {**result, 'out_of_scope_traces': self.out_of_scope_traces,
                'candidate_eligible': False}
