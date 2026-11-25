"""Observe an EXISTING Fast DDS endpoint through the opt-in typed provider ABI.

No ROS imports, endpoint creation, take, publish, listener replacement or vendor
loading override. Activation must select/map the independent vendor BEFORE ROS
initialization. This module never loads it as a fallback. All endpoint/Impl/RTPS
pointers are borrowed anew inside endpoint.handle; none survive a sample.

The owner must serialize endpoint/CFT lifecycle changes with sampling. The RTPS
mutex protects each native snapshot, not a distributed pair barrier or a matching
epoch. A short-buffer retry is a NEW snapshot, never part of a stitched list.
"""
from __future__ import annotations

import copy
import ctypes as C
import hashlib
import os
from pathlib import Path
import platform
import re
import sys
import threading
import time

from r2f.instrumentation.fastdds_peer.activation import (
    manifest_for_reference, verify_vendor_mapping,
)
from r2f.instrumentation.fastdds_peer.probe import (
    EXPECTED_OFFSETS, Result, verify_mapped_binaries,
)

SCHEMA = 'r2f.fastdds-native-matched-peer-observation/v1'
ORDINAL_BASIS = 'provider_instance_observation_serial_not_match_epoch'
MEMBERSHIP_BASIS = 'reliable_stateful_native_predicate_guid_set_under_entity_mutex'
PREFIX = Path('/opt/ros/jazzy')
RMW_IDENTIFIER = b'rmw_fastrtps_cpp'
# Installed public signatures, the libraries implementing them and handle ABI.
# No struct cast/dereference of rcl/rmw/Impl pointers is performed in Python.
API_PINS = {
    'include/rcl/rcl/publisher.h': '93c0f458a61dcd109e5b544ea562a831a770e328c65891f60de2a7c648bf8a3e',
    'include/rcl/rcl/subscription.h': '93e55501a52805a8e57c04d918fdb484d4942e4248954e99a7865a6c5d490871',
    'include/rmw/rmw/types.h': 'c1e7be9e420d3c32cf6b24db62c3fbd6b97d0518811f6f4677db09d835cf3830',
    'include/rmw/rmw/rmw.h': '9fbdba3127f095e1b14057c07d4c4acb3df4226d9bfebacbfb9123fc7088a3d2',
    'include/rmw_fastrtps_cpp/rmw_fastrtps_cpp/get_publisher.hpp': '22a7fde7ecc181b53ad92c268b9d7283bc13d65c992c8069b72a017c96c59bb9',
    'include/rmw_fastrtps_cpp/rmw_fastrtps_cpp/get_subscriber.hpp': '54dcfc06b0d44a6cc87a3ce7f4ce57319d55ceb8c1ed39aee74f64f1a225f815',
    'include/rcutils/rcutils/error_handling.h': 'e5049ca840b95857b757a4efe9a2d6d5984486229615f467ba1b07d3aeccfa03',
    'lib/librcl.so': '94570a4bea52192aa768fb6600fffb7fa05df779c324328d077d63ce062f5fb3',
    'lib/librmw_fastrtps_cpp.so': '7b59ab082ef48bd12d5e2d598fed9b4cc5a1c6668493902c6a03f617acbd021f',
    'lib/librmw_fastrtps_shared_cpp.so': '47e1965e31d15809089ce145a42850dce41522d8def367a4a899e927b1842bd1',
    'lib/librcutils.so': '9aedab61c0b1265c476fe12bf36d2d31bfc5fe3813c85a67b70406406a0567bc',
    'lib/python3.12/site-packages/rclpy/_rclpy_pybind11.cpython-312-x86_64-linux-gnu.so': '53efd7622c109ae6f9ff7ffafc3bffac4856700d39ca51d8689ecaa617a596bd',
}
GETTER_SYMBOLS = {
    'writer': '_ZN16rmw_fastrtps_cpp14get_datawriterEP15rmw_publisher_s',
    'reader': '_ZN16rmw_fastrtps_cpp14get_datareaderEP18rmw_subscription_s',
}
STATUS_NAMES = {
    0: 'ok', 1: 'invalid_argument', 2: 'unsupported', 3: 'not_enabled',
    4: 'buffer_too_small', 5: 'internal_error', 6: 'abi_mismatch',
}


class PeerObservationUnavailable(ValueError):
    """Explicit capability failure, not a DDS mismatch or fallback permission."""


def _reference(value):
    if (not isinstance(value, dict)
            or set(value) != {'observation_mode', 'manifest_path', 'manifest_sha256'}
            or value['observation_mode'] != 'instrumentation'
            or not isinstance(value['manifest_path'], str)
            or not Path(value['manifest_path']).is_absolute()
            or not isinstance(value['manifest_sha256'], str)
            or re.fullmatch(r'[0-9a-f]{64}', value['manifest_sha256']) is None):
        raise PeerObservationUnavailable('explicit_fastdds_peer_instrumentation_reference_required')
    return dict(value)


def verify_abi():
    """File reads/layout checks only; never import rclpy or load a library."""
    if (sys.platform != 'linux' or platform.machine() != 'x86_64'
            or sys.byteorder != 'little' or C.sizeof(C.c_void_p) != 8):
        raise PeerObservationUnavailable('unsupported_native_platform')
    if (C.sizeof(Result) != 64 or C.alignment(Result) != 8
            or {key: getattr(Result, key).offset for key in EXPECTED_OFFSETS} != EXPECTED_OFFSETS):
        raise PeerObservationUnavailable('ctypes_result_layout_mismatch')
    for relative, expected in API_PINS.items():
        if hashlib.sha256((PREFIX / relative).read_bytes()).hexdigest() != expected:
            raise PeerObservationUnavailable('public_api_abi_hash_mismatch:' + relative)
    return {'schema': 'r2f.fastdds-peer-public-api-abi/v1',
            'platform': 'linux-x86_64-little-endian',
            'result_size': 64, 'result_offsets': dict(EXPECTED_OFFSETS),
            'files': dict(API_PINS)}


def _verify_api_maps(*, require_all=True, maps_text=None):
    """Every relevant mapping must be the pinned file, device AND inode."""
    expected = {Path(rel).name.split('.so')[0]: (PREFIX / rel).resolve(strict=True)
                for rel in API_PINS if '.so' in Path(rel).name}
    text = Path('/proc/self/maps').read_text() if maps_text is None else maps_text
    records = {}
    for line in text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6:
            continue
        key = Path(fields[5]).name.split('.so')[0]
        if key not in expected:
            continue
        path = expected[key]
        st = path.stat()
        device = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'
        if (fields[5].endswith(' (deleted)') or Path(fields[5]).resolve() != path
                or fields[3] != device or fields[4] != str(st.st_ino)):
            raise PeerObservationUnavailable('mapped_public_api_identity_mismatch:' + key)
        records[key] = {'path': str(path), 'device': device, 'inode': st.st_ino}
    if require_all and set(records) != set(expected):
        raise PeerObservationUnavailable('required_public_api_not_mapped:' + ','.join(sorted(set(expected) - set(records))))
    return [records[key] for key in sorted(records)]


def _bind(library, name, args, result):
    function = getattr(library, name)
    function.argtypes = args
    function.restype = result
    return function


class _Bindings:
    """Retain DSOs/functions only, NEVER an endpoint/rmw/native object pointer."""
    def __init__(self, reference):
        self.reference = _reference(reference)
        self.manifest = manifest_for_reference(self.reference)
        if self.manifest.get('pdp_trace') is not None:
            from r2f.instrumentation.fastdds_pdp_trace.activation import verify_loader_selection
        else:
            from r2f.instrumentation.fastdds_filter_trace.activation import verify_loader_selection
        verify_loader_selection(self.manifest)
        self.abi = verify_abi()
        self._check_manifest_abi(self.manifest)
        # The existing RMW must already use this vendor. Do not rescue an
        # incorrectly launched worker by loading a second vendor here.
        verify_vendor_mapping(self.reference)
        _verify_api_maps(require_all=False)
        try:
            self.libs = {name: C.CDLL(str(PREFIX / ('lib/lib' + name + '.so')), mode=os.RTLD_LOCAL)
                         for name in ('rcl', 'rmw_fastrtps_cpp', 'rcutils')}
            self.libs['provider'] = C.CDLL(self.manifest['binaries']['provider']['path'], mode=os.RTLD_LOCAL)
            self.rcl_getters = {
                'writer': _bind(self.libs['rcl'], 'rcl_publisher_get_rmw_handle', [C.c_void_p], C.c_void_p),
                'reader': _bind(self.libs['rcl'], 'rcl_subscription_get_rmw_handle', [C.c_void_p], C.c_void_p),
            }
            self.native_getters = {
                kind: _bind(self.libs['rmw_fastrtps_cpp'], name, [C.c_void_p], C.c_void_p)
                for kind, name in GETTER_SYMBOLS.items()
            }
            self.snapshots = {
                kind: _bind(self.libs['provider'], 'r2f_fastdds_peer_' + kind + '_v1',
                            [C.c_void_p, C.POINTER(C.c_uint8), C.c_uint64,
                             C.POINTER(Result), C.c_uint32], C.c_int32)
                for kind in ('writer', 'reader')
            }
            self.reset_error = _bind(self.libs['rcutils'], 'rcutils_reset_error', [], None)
            identifier = _bind(self.libs['rmw_fastrtps_cpp'], 'rmw_get_implementation_identifier', [], C.c_char_p)
            self.revalidate()
            if identifier() != RMW_IDENTIFIER:
                raise PeerObservationUnavailable('loaded_rmw_identifier_mismatch')
            self.revalidate()
        except (OSError, AttributeError) as exc:
            raise PeerObservationUnavailable('native_load_or_symbol_failure:' + str(exc)) from exc

    @staticmethod
    def _check_manifest_abi(manifest):
        abi = manifest.get('patch', {}).get('abi', {})
        expected = {
            'schema_version': 1, 'result_size': 64, 'guid_size': 16,
            'layout_changed': False, 'virtual_slots_added': False,
            'snapshot_ordinal_basis': ORDINAL_BASIS, 'membership_basis': MEMBERSHIP_BASIS,
            'statuses': {name: status for status, name in STATUS_NAMES.items()},
            'functions': ['r2f_fastdds_peer_writer_v1', 'r2f_fastdds_peer_reader_v1'],
        }
        if (any(type(abi.get(key)) is not type(value) or abi.get(key) != value
                for key, value in expected.items())
                or any(type(abi['statuses'][name]) is not int for name in STATUS_NAMES.values())):
            raise PeerObservationUnavailable('unsupported_provider_abi_manifest')

    def revalidate(self):
        manifest = manifest_for_reference(self.reference)
        self._check_manifest_abi(manifest)
        if manifest['binaries'] != self.manifest['binaries']:
            raise PeerObservationUnavailable('selected_binary_identity_changed')
        abi = verify_abi()
        vendor = verify_vendor_mapping(self.reference)
        api_maps = _verify_api_maps()
        maps = verify_mapped_binaries(manifest)
        return {'mapped_libraries': maps, 'vendor_identity': vendor,
                'library_provenance': {'provider_binary': copy.deepcopy(manifest['binaries']['provider']),
                                       'vendor_binary': copy.deepcopy(manifest['binaries']['vendor']),
                                       'provider_source_sha256': manifest['provider_source_sha256'],
                                       'provider_abi': copy.deepcopy(manifest['patch']['abi']),
                                       'public_api_abi': abi, 'public_api_mapped_libraries': api_maps}}


def _pointer(value, reason):
    # Public pybind pointer and ctypes void* returns are ordinary Python ints.
    if type(value) is not int or not 0 < value < 2 ** 64:
        raise PeerObservationUnavailable(reason)
    return value


def _guid(value, kind):
    if (not isinstance(value, str) or re.fullmatch(r'[0-9a-f]{32}', value) is None):
        raise ValueError('native_guid_invalid:' + kind)
    raw = bytes.fromhex(value)
    suffixes = (0x02, 0x03) if kind == 'writer' else (0x04, 0x07)
    if not any(raw[:12]) or not any(raw[12:15]) or raw[15] not in suffixes:
        raise ValueError('native_guid_invalid:' + kind)
    return value


def _receipt(result):
    return {name: bytes(result.local_guid).hex() if name == 'local_guid'
            else int(getattr(result, name)) for name, *_ in Result._fields_}


class FastddsMatchedPeerObserver:
    SCHEMA = SCHEMA
    INITIAL_CAPACITY = 8
    MAX_PEERS = 256
    MAX_ATTEMPTS = 3

    def __init__(self, endpoint, *, instrumentation):
        self._reference = _reference(instrumentation)
        self.endpoint = endpoint
        self._native = _Bindings(self._reference)
        self._lock = threading.Lock()
        self._last_ordinal = 0  # only serial freshness; NEVER endpoint generation

    @staticmethod
    def _check_handle(handle, kind):
        expected = 'Publisher' if kind == 'writer' else 'Subscription'
        if (type(handle).__module__ != 'rclpy._rclpy_pybind11'
                or type(handle).__name__ != expected):
            raise PeerObservationUnavailable('endpoint_handle_role_or_abi_mismatch')

    def _resolve(self, handle, kind):
        if self.endpoint.handle is not handle:
            raise PeerObservationUnavailable('endpoint_handle_changed_during_sample')
        self._check_handle(handle, kind)
        rcl = _pointer(handle.pointer, 'rcl_handle_pointer_unavailable')
        rmw = self._native.rcl_getters[kind](rcl)
        if not rmw:
            self._native.reset_error()
            raise PeerObservationUnavailable('rcl_get_rmw_handle_failed')
        rmw = _pointer(rmw, 'rmw_handle_pointer_invalid')
        native = self._native.native_getters[kind](rmw)
        if not native:
            self._native.reset_error()
            raise PeerObservationUnavailable('rmw_fastrtps_public_native_getter_failed')
        return rcl, rmw, _pointer(native, 'native_endpoint_pointer_invalid')

    def sample(self, domain, gid, kind):
        began = time.monotonic_ns()
        observation = {
            'schema': self.SCHEMA, 'observation_mode': 'instrumentation', 'complete': False,
            'domain_id': domain, 'local_gid': gid, 'role_kind': kind,
            'manifest_reference': dict(self._reference), 'snapshot_ordinal_basis': ORDINAL_BASIS,
            'membership_basis': MEMBERSHIP_BASIS, 'started_monotonic_ns': began,
            'domain_basis': 'caller_worker_domain_not_native_provider_observed',
        }
        try:
            with self._lock:
                if type(domain) is not int or not 0 <= domain <= 231 or kind not in ('writer', 'reader'):
                    raise ValueError('native_observation_input_identity_invalid')
                _guid(gid, kind)
                observation.update(self._native.revalidate())
                attempts = []
                observation['native_attempts'] = attempts
                capacity = self.INITIAL_CAPACITY
                peers = None
                # Borrow the CURRENT handle, not one retained by construction or
                # a previous CFT generation. Only pointers from public getters.
                borrowed = self.endpoint.handle
                # Destroyable.__enter__ -> None in the pinned rclpy ABI.
                # Borrow the object itself, never the context manager result.
                with borrowed:
                    for _ in range(self.MAX_ATTEMPTS):
                        chain = self._resolve(borrowed, kind)
                        buffer = (C.c_uint8 * (capacity * 16))()
                        native_result = Result()
                        status = self._native.snapshots[kind](
                            chain[2], buffer, capacity, C.byref(native_result), C.sizeof(Result))
                        receipt = _receipt(native_result)
                        # Exact native bytes/fields survive failures for audit.
                        observation['native_result'] = receipt
                        observation['native_result_hex'] = bytes(native_result).hex()
                        attempts.append({'capacity': capacity, 'return_status': status,
                                         'native_result': dict(receipt)})
                        if type(status) is not int or status not in STATUS_NAMES:
                            raise ValueError('native_status_unknown')
                        if (receipt['schema_version'] != 1 or receipt['struct_size'] != 64
                                or receipt['status'] != status or receipt['reserved'] != 0
                                or receipt['endpoint_kind'] != (1 if kind == 'writer' else 2)
                                or receipt['complete'] not in (0, 1)):
                            raise ValueError('native_result_abi_or_metadata_mismatch')
                        if status not in (0, 4):
                            raise PeerObservationUnavailable('native_status:' + STATUS_NAMES[status])
                        if (_guid(receipt['local_guid'], kind) != gid
                                or receipt['snapshot_ordinal'] <= self._last_ordinal):
                            raise ValueError('native_local_gid_or_observation_serial_mismatch')
                        self._last_ordinal = receipt['snapshot_ordinal']
                        count = receipt['required_count']
                        if count > self.MAX_PEERS:
                            raise ValueError('native_peer_count_bound_exceeded')
                        if self._resolve(borrowed, kind) != chain:
                            raise ValueError('native_endpoint_changed_during_sample')
                        if status == 4:
                            if receipt['complete'] != 0 or receipt['copied'] != 0 or count <= capacity:
                                raise ValueError('native_short_buffer_receipt_invalid')
                            # Discard buffer completely, never combine attempts.
                            capacity = min(self.MAX_PEERS, max(count, capacity * 2))
                            continue
                        if receipt['complete'] != 1 or receipt['copied'] != count or count > capacity:
                            raise ValueError('native_complete_count_mismatch')
                        opposite = 'reader' if kind == 'writer' else 'writer'
                        peers = [_guid(bytes(buffer[i * 16:(i + 1) * 16]).hex(), opposite)
                                 for i in range(count)]
                        if len(set(peers)) != len(peers) or gid in peers:
                            raise ValueError('native_peer_gid_aliased_or_duplicate')
                        # ONE successful native snapshot only. No stability-
                        # polling second success and no status/count getter.
                        break
                    if peers is None:
                        raise ValueError('native_short_buffer_retry_exhausted')
                # Finish validation before making any peer proof visible.
                observation.update(self._native.revalidate())
                observation.update(complete=True, matched_peer_gids=peers,
                                   limitations=['per_entity_snapshot_not_distributed_atomic_or_continuous_match',
                                                'owner_serializes_endpoint_and_cft_lifecycle',
                                                'snapshot_ordinal_is_not_match_epoch'])
        except Exception as exc:
            # InvalidHandle/library/provenance/ABI failures must not become a
            # successful empty list or a legacy public-DDS-API proof label.
            observation.pop('matched_peer_gids', None)
            observation.update(complete=False, reason=str(exc), failure_type=type(exc).__name__)
        finally:
            observation['ended_monotonic_ns'] = time.monotonic_ns()
        return observation
