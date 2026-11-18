"""Native CycloneDDS participant backend via ctypes (no compiler, no wrapper).

Why ctypes: the host has CycloneDDS headers and ``libddsc.so.0`` but no
``cc``/``gcc``/``clang`` on PATH, so a compiled shim is not the only route.
Every C type below is declared by hand from the installed header and the
declaration is hashed so a mismatching library version is detected instead of
silently mis-read.

This backend can really create/destroy a DDS participant and read the builtin
``DCPSParticipant`` topic.  It is still **not executed here**: live ROS/DDS
execution has no GO for this task.  ``capability()`` proves the load path,
library identity and symbol set without creating any entity; ``run()`` refuses
unless the caller passes an explicit opt-in (``enabled`` plus a
``live_authorization`` record), and refuses again if the library identity
changed since the capability receipt.

Provenance/capacity rules:
* pure-Python backend: no build root, no compiled artifact, recorded as such;
* the resolved library realpath + SHA-256 and each header SHA-256 are recorded;
* a symbol missing from the loaded library is a capability gap, never a
  fallback to another vendor;
* the receipt never claims ROS/rcl context: this is a bare DDS participant.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
from pathlib import Path
import time

SCHEMA = 'r2f.native-participant/v1'
MODULE = 'r2f.runtime.native_participant'
VENDOR = 'cyclonedds'

ROS_PREFIX = Path('/opt/ros/jazzy')
LIBRARY_DIR = ROS_PREFIX / 'lib' / 'x86_64-linux-gnu'
HEADER = ROS_PREFIX / 'include' / 'CycloneDDS' / 'dds' / 'dds.h'
QOS_HEADER = ROS_PREFIX / 'include' / 'CycloneDDS' / 'dds' / 'ddsc' / 'dds_public_qos.h'
IMPL_HEADER = ROS_PREFIX / 'include' / 'CycloneDDS' / 'dds' / 'ddsc' / 'dds_public_impl.h'

# Preloaded companion libraries; libddsc refuses to load without them and the
# generic search path is not set up in this environment.
PRELOAD = ('libiceoryx_platform.so', 'libiceoryx_hoofs.so', 'libiceoryx_posh.so',
           'libiceoryx_binding_c.so')

# CycloneDDS 0.10.5 constants, from dds_public_impl.h and dds.h.
DDS_DOMAIN_DEFAULT = 0xFFFFFFFF
DDS_MIN_PSEUDO_HANDLE = 0x7FFF0000
BUILTIN_PARTICIPANT_TOPIC = DDS_MIN_PSEUDO_HANDLE + 1
INSTANCE_STATE = {0x10: 'ALIVE', 0x20: 'NOT_ALIVE_DISPOSED', 0x40: 'NOT_ALIVE_NO_WRITERS'}
ENTITY_NIL = 0
# Bound both Python ctypes allocations and the native sample count.
MAX_BUILTIN_SAMPLES = 1024

REQUIRED_SYMBOLS = ('dds_create_participant', 'dds_delete', 'dds_get_guid',
                    'dds_create_reader', 'dds_take', 'dds_return_loan',
                    'dds_builtintopic_free_participant', 'dds_qget_liveliness',
                    'dds_qget_history')

_entity_t = ctypes.c_int32
_return_t = ctypes.c_int32
_domainid_t = ctypes.c_uint32


class _Guid(ctypes.Structure):
    _fields_ = [('v', ctypes.c_uint8 * 16)]


class _SampleInfo(ctypes.Structure):
    """dds_sample_info_t from dds.h; 64 bytes on this ABI."""
    _fields_ = [('sample_state', ctypes.c_int32),
                ('view_state', ctypes.c_int32),
                ('instance_state', ctypes.c_int32),
                ('valid_data', ctypes.c_bool),
                ('source_timestamp', ctypes.c_int64),
                ('instance_handle', ctypes.c_uint64),
                ('publication_handle', ctypes.c_uint64),
                ('disposed_generation_count', ctypes.c_uint32),
                ('no_writers_generation_count', ctypes.c_uint32),
                ('sample_rank', ctypes.c_uint32),
                ('generation_rank', ctypes.c_uint32),
                ('absolute_generation_rank', ctypes.c_uint32)]


class _BuiltinParticipant(ctypes.Structure):
    """dds_builtintopic_participant_t from dds.h."""
    _fields_ = [('key', _Guid), ('qos', ctypes.c_void_p)]


ABI_LAYOUT = {
    'dds_entity_t': ctypes.sizeof(_entity_t),
    'dds_return_t': ctypes.sizeof(_return_t),
    'dds_domainid_t': ctypes.sizeof(_domainid_t),
    'dds_guid_t': ctypes.sizeof(_Guid),
    'dds_sample_info_t': ctypes.sizeof(_SampleInfo),
    'dds_builtintopic_participant_t': ctypes.sizeof(_BuiltinParticipant),
    'dds_sample_info_t_offsets': [getattr(_SampleInfo, name).offset
                                  for name, _ in _SampleInfo._fields_],
}


def abi_sha256():
    return hashlib.sha256(json.dumps(ABI_LAYOUT, sort_keys=True).encode()).hexdigest()


class NativeParticipantError(RuntimeError):
    def __init__(self, layer, reason, **detail):
        super().__init__(reason)
        self.layer = layer
        self.reason = reason
        self.detail = detail


def _now():
    return time.time_ns(), time.monotonic_ns()


def receipt(kind, **fields):
    wall, mono = _now()
    value = {'schema': SCHEMA, 'module': MODULE, 'receipt_kind': kind,
             'vendor': VENDOR, 'time_ns': wall, 'monotonic_ns': mono,
             'counts_toward_maturity': False, 'candidate_eligible': False,
             'mutation_receipt': False, 'ros_context_created': False}
    value.update(fields)
    return value


def blocked(reason, layer='precondition', **fields):
    value = receipt('native_blocked', reason=reason, layer=layer,
                    ok=False, accepted=False, real_execution=False,
                    sut_bug=False, participant_created=False,
                    participant_destroyed=False)
    value.update(fields)
    return value


def _sha(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def _guid_text(guid):
    return bytes(bytearray(guid.v)).hex() if guid is not None else None


def load_library(library_path=None):
    """Preload companions and dlopen libddsc; no entity is created."""
    path = Path(library_path or (LIBRARY_DIR / 'libddsc.so.0'))
    preloads = []
    for name in PRELOAD:
        candidate = path.parent / name
        if not candidate.is_file():
            raise NativeParticipantError('capability_gap', 'preload_missing:' + name,
                                        path=str(candidate))
        try:
            ctypes.CDLL(str(candidate), mode=ctypes.RTLD_GLOBAL)
        except OSError as exc:
            raise NativeParticipantError('capability_gap', 'preload_failed:' + name,
                                        error=str(exc)) from None
        preloads.append({'name': name, 'realpath': os.path.realpath(candidate),
                         'sha256': _sha(candidate)})
    try:
        library = ctypes.CDLL(str(path), mode=ctypes.RTLD_GLOBAL)
    except OSError as exc:
        raise NativeParticipantError('capability_gap', 'library_load_failed',
                                    error=str(exc), path=str(path)) from None
    missing = [name for name in REQUIRED_SYMBOLS if not hasattr(library, name)]
    if missing:
        raise NativeParticipantError('capability_gap', 'symbols_missing',
                                    symbols=missing)
    library.dds_create_participant.restype = _entity_t
    library.dds_create_participant.argtypes = [_domainid_t, ctypes.c_void_p, ctypes.c_void_p]
    library.dds_delete.restype = _return_t
    library.dds_delete.argtypes = [_entity_t]
    library.dds_get_guid.restype = _return_t
    library.dds_get_guid.argtypes = [_entity_t, ctypes.POINTER(_Guid)]
    library.dds_create_reader.restype = _entity_t
    library.dds_create_reader.argtypes = [_entity_t, _entity_t, ctypes.c_void_p, ctypes.c_void_p]
    library.dds_take.restype = _return_t
    library.dds_take.argtypes = [_entity_t, ctypes.POINTER(ctypes.c_void_p),
                                 ctypes.POINTER(_SampleInfo), ctypes.c_size_t, ctypes.c_uint32]
    library.dds_return_loan.restype = _return_t
    library.dds_return_loan.argtypes = [_entity_t, ctypes.POINTER(ctypes.c_void_p), _return_t]
    library.dds_builtintopic_free_participant.restype = None
    library.dds_builtintopic_free_participant.argtypes = [ctypes.POINTER(_BuiltinParticipant)]
    return library, {'library': os.path.realpath(path), 'library_sha256': _sha(path),
                     'preloads': preloads}


def provenance(library_path=None):
    """Independent build/runtime provenance for this backend."""
    return {'backend': 'ctypes-cyclonedds',
            'module_sha256': _sha(__file__),
            'build': {'kind': 'no_compiled_artifact', 'compiler_required': False,
                      'build_root': None, 'binary': None,
                      'note': 'pure ctypes binding; no C shim compiled or shipped'},
            'abi': {'source_headers': {'dds.h': _sha(HEADER),
                                       'dds_public_qos.h': _sha(QOS_HEADER),
                                       'dds_public_impl.h': _sha(IMPL_HEADER)},
                    'declared_layout': ABI_LAYOUT, 'layout_sha256': abi_sha256()},
            'runtime': {'python': '%d.%d.%d' % tuple(__import__('sys').version_info[:3]),
                        'library': library_path or str(LIBRARY_DIR / 'libddsc.so.0')},
            'vendor': VENDOR}


def capability(library_path=None):
    """Read-only: dlopen + symbol check + identity.  Creates no participant."""
    value = receipt('native_capability', probe='cyclonedds_ctypes_v1',
                    provenance=provenance(library_path))
    try:
        _, identity = load_library(library_path)
    except NativeParticipantError as exc:
        value.update(ok=False, available=False, capability_status='capability_gap',
                     reason=exc.reason, layer=exc.layer, detail=exc.detail,
                     participant_created=False)
        return value
    value.update(ok=True, available=True, capability_status='observed',
                 **identity, symbols=list(REQUIRED_SYMBOLS),
                 builtin_participant_topic=BUILTIN_PARTICIPANT_TOPIC,
                 participant_created=False,
                 live_execution='not_run_no_live_go')
    return value


class NativeParticipantBackend:
    """Real participant lifecycle; refuses to run without explicit opt-in.

    ``run()`` performs: create participant -> read own GUID -> create a
    builtin DCPSParticipant reader -> optional windowed take -> delete
    everything in reverse order.  Every entity handle is tracked so cleanup is
    verifiable even on error.
    """

    def __init__(self, emit=None, library_path=None):
        self.emit = emit
        self.library_path = library_path
        self.library = None
        self.identity = None
        self.created = []
        self.deleted = []
        self.participant = ENTITY_NIL

    def _record(self, value):
        if self.emit is not None:
            self.emit(value)
        return value

    @staticmethod
    def _gate(config):
        if not isinstance(config, dict):
            return blocked('config_must_be_object', layer='config_type')
        if config.get('enabled') is not True:
            return blocked('native_backend_not_enabled', layer='precondition',
                           enabled=config.get('enabled'))
        authorization = config.get('live_authorization')
        if not isinstance(authorization, dict) or authorization.get('granted') is not True:
            return blocked('live_authorization_required', layer='precondition',
                           live_execution='not_run_no_live_go',
                           note='bare DDS participant creation is live execution')
        expected = authorization.get('library_sha256')
        actual = _sha(Path(config.get('library') or (LIBRARY_DIR / 'libddsc.so.0')))
        if expected is not None and expected != actual:
            return blocked('library_identity_mismatch', layer='identity',
                           expected=expected, observed=actual)
        return None

    def open(self, config):
        refusal = self._gate(config)
        if refusal is not None:
            return self._record(refusal)
        try:
            self.library, self.identity = load_library(config.get('library'))
        except NativeParticipantError as exc:
            return self._record(blocked(exc.reason, layer=exc.layer, **exc.detail))
        domain = config.get('domain_id', DDS_DOMAIN_DEFAULT)
        if isinstance(domain, bool) or not isinstance(domain, int):
            return self._record(blocked('domain_id_must_be_integer', layer='config_value'))
        started = time.monotonic_ns()
        handle = self.library.dds_create_participant(_domainid_t(domain), None, None)
        if handle <= ENTITY_NIL:
            return self._record(blocked('participant_create_failed', layer='runtime',
                                        return_code=int(handle), domain_id=domain))
        self.participant = handle
        self.created.append(('participant', handle))
        guid = _Guid()
        rc = self.library.dds_get_guid(_entity_t(handle), ctypes.byref(guid))
        text = _guid_text(guid) if rc == 0 else None
        value = receipt('native_participant_created', stage='carrier_admitted',
                        participant_handle=int(handle), guid=text,
                        guid_return_code=int(rc), domain_id=domain,
                        created_monotonic_ns=started,
                        **self.identity, real_execution=True,
                        ros_context_created=False,
                        identity_basis='ddsc_dds_get_guid' if text else 'unavailable')
        return self._record(value)

    def builtin_reader(self):
        if self.participant == ENTITY_NIL or self.library is None:
            return self._record(blocked('participant_not_created', layer='precondition'))
        handle = self.library.dds_create_reader(_entity_t(self.participant),
                                                _entity_t(BUILTIN_PARTICIPANT_TOPIC),
                                                None, None)
        if handle <= ENTITY_NIL:
            return self._record(blocked('builtin_reader_create_failed', layer='runtime',
                                        return_code=int(handle),
                                        topic_handle=int(BUILTIN_PARTICIPANT_TOPIC)))
        self.created.append(('builtin_reader', handle))
        return self._record(receipt('native_builtin_reader_created', stage='carrier_admitted',
                                    reader_handle=int(handle),
                                    topic='DCPSParticipant',
                                    topic_handle=int(BUILTIN_PARTICIPANT_TOPIC),
                                    real_execution=True))

    def take_participants(self, reader_handle, max_samples=32):
        """Copy builtin fields, then return the reader-owned loan in ``finally``.

        ``dds_take`` receives NULL sample pointers, so CycloneDDS owns any
        returned samples (including their keys/QoS). Never manually call
        ``dds_builtintopic_free_participant`` on these reader loans: only
        ``dds_return_loan`` releases them. Parse errors still return the loan;
        return errors are reported, never retried or hidden by a manual free.

        Capacity must be an integer (not bool) in [1, MAX_BUILTIN_SAMPLES].
        An impossible positive take count is not parsed and cleanup is bounded
        to the actual buffer capacity. This is a harness/ABI violation:
        even a successful bounded cleanup cannot certify all loans were
        returned, so cleanup remains uncertain. Empty reads have no loan
        and no effect.
        Failed reads/decodes/returns produce harness-blocked receipts; partial
        samples are diagnostic only, not successful observation evidence.
        """
        failure_fields = {'stage': 'aborted', 'effect_observed': False,
                          'responsibility_layer': 'harness'}
        if isinstance(max_samples, bool) or not isinstance(max_samples, int):
            return self._record(blocked('max_samples_must_be_integer',
                                        layer='config_type', **failure_fields))
        if not 1 <= max_samples <= MAX_BUILTIN_SAMPLES:
            return self._record(blocked('max_samples_out_of_range',
                                        layer='config_value', max_samples=max_samples,
                                        max_samples_limit=MAX_BUILTIN_SAMPLES,
                                        **failure_fields))
        if self.library is None:
            return self._record(blocked('library_not_loaded', layer='precondition',
                                        **failure_fields))
        buffer = (ctypes.c_void_p * max_samples)()
        info = (_SampleInfo * max_samples)()
        rc, loan_rc, loan_count = None, None, 0
        samples, errors = [], []
        reason, take_error, parse_error = None, None, None
        try:
            try:
                rc = int(self.library.dds_take(_entity_t(reader_handle), buffer, info,
                                               ctypes.c_size_t(max_samples),
                                               ctypes.c_uint32(max_samples)))
            except Exception as exc:
                reason = 'builtin_take_failed'
                take_error = type(exc).__name__ + ':' + str(exc)
            if rc is not None:
                if rc < 0:
                    reason = 'builtin_take_failed'
                elif rc > max_samples:
                    reason = 'builtin_take_count_out_of_range'
                else:
                    for index in range(rc):
                        try:
                            pointer = buffer[index]
                            if not pointer:
                                raise ValueError('builtin sample pointer is NULL')
                            sample = ctypes.cast(
                                pointer, ctypes.POINTER(_BuiltinParticipant)).contents
                            state = INSTANCE_STATE.get(int(info[index].instance_state), 'UNKNOWN')
                            samples.append({'guid': _guid_text(sample.key),
                                            'instance_state': state,
                                            'valid_data': bool(info[index].valid_data),
                                            'source_timestamp_ns': int(info[index].source_timestamp),
                                            'observation': 'builtin_participant_sample'})
                        except Exception as exc:
                            reason = 'builtin_sample_parse_failed'
                            parse_error = {'sample_index': index,
                                           'error': type(exc).__name__ + ':' + str(exc)}
                            break
        finally:
            # A contract-valid positive take grants a reader loan even
            # if decoding fails. With an impossible count, this is only
            # a bounded best-effort cleanup, not proof of full return.
            # Never pass a corrupt/oversized rc to native cleanup or traverse
            # more pointers than were allocated. Nonpositive takes grant none.
            if rc is not None and rc > 0:
                loan_count = min(rc, max_samples)
                try:
                    loan_rc = int(self.library.dds_return_loan(
                        _entity_t(reader_handle), buffer, _return_t(loan_count)))
                except Exception as exc:
                    errors.append('return_loan:' + type(exc).__name__ + ':' + str(exc))
                if loan_rc not in (None, 0):
                    errors.append('return_loan:return_code=' + str(loan_rc))
        abi_violation = rc is not None and rc > max_samples
        loan_confirmed = loan_count > 0 and loan_rc == 0 and not abi_violation
        fields = {'reader_handle': int(reader_handle), 'max_samples': max_samples,
                  'sample_count': len(samples), 'samples': samples,
                  'take_return_code': rc, 'take_error': take_error,
                  'parse_error': parse_error, 'return_loan_code': loan_rc,
                  'loan_return_attempted': loan_count > 0,
                  'loan_return_count': loan_count,
                  'loan_return_count_basis': ('capacity_bound_after_abi_violation' if abi_violation
                                              else 'take_return_code' if loan_count
                                              else 'no_positive_take'),
                  'loan_returned': loan_confirmed,
                  'loan_cleanup_complete': (loan_count == 0 or loan_rc == 0) and not abi_violation,
                  'loan_cleanup_uncertain': abi_violation or (loan_count > 0 and loan_rc != 0),
                  'abi_contract_violation': ('dds_take_count_exceeds_capacity' if abi_violation
                                             else None),
                  'free_errors': errors, 'real_execution': True,
                  'observation_basis': 'dcpsparticipant_builtin_topic'}
        if reason or errors:
            # Keep native return codes associated with the failing operation;
            # parse failures have parse_error, not a misleading success code.
            if reason in ('builtin_take_failed', 'builtin_take_count_out_of_range'):
                fields['return_code'] = rc
            elif reason is None:
                fields['return_code'] = loan_rc
            return self._record(blocked(reason or 'builtin_return_loan_failed',
                                        layer='abi_contract' if abi_violation else 'runtime',
                                        **failure_fields, **fields))
        return self._record(receipt('native_builtin_participants',
                                    stage='effect_observed' if rc else 'carrier_admitted',
                                    ok=True, accepted=True, effect_observed=rc > 0,
                                    **fields))

    def close(self):
        """Delete every created entity in reverse order; report what failed."""
        failures = []
        for kind, handle in reversed(self.created):
            try:
                rc = int(self.library.dds_delete(_entity_t(handle)))
            except Exception as exc:
                failures.append({'kind': kind, 'handle': int(handle),
                                 'error': type(exc).__name__ + ':' + str(exc)})
                continue
            if rc != 0:
                failures.append({'kind': kind, 'handle': int(handle), 'return_code': rc})
            else:
                self.deleted.append((kind, handle))
        self.created = []
        self.participant = ENTITY_NIL
        value = receipt('native_participant_closed', stage='effect_observed',
                        deleted=[{'kind': kind, 'handle': int(handle)}
                                 for kind, handle in self.deleted],
                        failures=failures, cleanup_complete=not failures,
                        real_execution=True)
        return self._record(value)

    def run(self, config):
        """Full gated lifecycle; guards are offline, runtime failures stay explicit."""
        opened = self.open(config)
        if not opened.get('real_execution'):
            return opened
        try:
            reader = self.builtin_reader()
            if not reader.get('real_execution'):
                return reader
            samples = self.take_participants(reader['reader_handle'])
            return samples
        finally:
            self.close()


def summarize_capability(capability_receipt):
    """Reduce a capability receipt to the fields the runner/report needs."""
    return {'schema': SCHEMA, 'probe': capability_receipt.get('probe'),
            'ok': capability_receipt.get('ok'),
            'capability_status': capability_receipt.get('capability_status'),
            'available': capability_receipt.get('available'),
            'vendor': VENDOR,
            'library_sha256': capability_receipt.get('library_sha256'),
            'ros_context_created': False,
            'participant_created': capability_receipt.get('participant_created'),
            'live_execution': capability_receipt.get('live_execution'),
            'counts_toward_maturity': False}
