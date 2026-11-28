"""Native CycloneDDS participant backend via ctypes (no compiler, no wrapper).

Why ctypes: the host has CycloneDDS headers and ``libddsc.so.0`` but no
``cc``/``gcc``/``clang`` on PATH, so a compiled shim is not the only route.
Every C type below is declared by hand from the installed header and the
declaration is hashed so a mismatching library version is detected instead of
silently mis-read.

This backend can really create/destroy a DDS participant and read the builtin
``DCPSParticipant`` topic, with an opt-in reader selector for ``DCPSTopic``.  It is still **not executed here**: live ROS/DDS
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
import platform
import re
import sys
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
QOS_DEFS_HEADER = QOS_HEADER.with_name('dds_public_qosdefs.h')
VERSION_HEADER = HEADER.with_name('version.h')
FEATURES_HEADER = HEADER.with_name('features.h')

# Preloaded companion libraries; libddsc refuses to load without them and the
# generic search path is not set up in this environment.
PRELOAD = ('libiceoryx_platform.so', 'libiceoryx_hoofs.so', 'libiceoryx_posh.so',
           'libiceoryx_binding_c.so')

# CycloneDDS 0.10.5 constants, from dds_public_impl.h and dds.h.
DDS_DOMAIN_DEFAULT = 0xFFFFFFFF
DDS_MIN_PSEUDO_HANDLE = 0x7FFF0000
BUILTIN_PARTICIPANT_TOPIC = DDS_MIN_PSEUDO_HANDLE + 1
BUILTIN_TOPIC_TOPIC = DDS_MIN_PSEUDO_HANDLE + 2
INSTANCE_STATE = {0x10: 'ALIVE', 0x20: 'NOT_ALIVE_DISPOSED', 0x40: 'NOT_ALIVE_NO_WRITERS'}
ENTITY_NIL = 0
# Bound both Python ctypes allocations and the native sample count.
MAX_BUILTIN_SAMPLES = 1024

REQUIRED_SYMBOLS = ('dds_create_participant', 'dds_delete', 'dds_get_guid',
                    'dds_create_reader', 'dds_take', 'dds_return_loan',
                    'dds_builtintopic_free_participant', 'dds_qget_liveliness')

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


class _TopicKey(ctypes.Structure):
    """dds_builtintopic_topic_key_t: opaque bytes, not a DDS GUID."""
    _fields_ = [('d', ctypes.c_ubyte * 16)]


class _BuiltinTopic(ctypes.Structure):
    """dds_builtintopic_topic_t from CycloneDDS 0.10.5 dds.h."""
    _fields_ = [('key', _TopicKey), ('topic_name', ctypes.c_char_p),
                ('type_name', ctypes.c_char_p), ('qos', ctypes.c_void_p)]


def _topic_abi(identity):
    """Fail closed outside the inspected Linux x86-64 CycloneDDS 0.10.5 ABI.

    Match relevant declarations (comments/whitespace don't change the ABI),
    retain full header hashes, and pin the runtime binary separately. Header
    presence alone is not evidence that any topic discovery endpoint is enabled.
    """
    if (sys.platform != 'linux' or platform.machine() != 'x86_64' or
            sys.byteorder != 'little' or ctypes.sizeof(ctypes.c_void_p) != 8):
        raise NativeParticipantError('abi_contract', 'topic_abi_unsupported_platform')
    # Exact binary inspected with these installed headers; an equal version
    # string from another build does not establish enum/layout compatibility.
    library_sha256 = '9a63f94ee673c5efddb337a7dd012e4c98960b10ce256dcc3a7b4c331fabf5b5'
    if not isinstance(identity, dict) or identity.get('library_sha256') != library_sha256:
        raise NativeParticipantError('abi_contract', 'topic_abi_library_mismatch')
    layout = {
        'topic_key_size': ctypes.sizeof(_TopicKey),
        'topic_size': ctypes.sizeof(_BuiltinTopic),
        'topic_alignment': ctypes.alignment(_BuiltinTopic),
        'topic_offsets': [getattr(_BuiltinTopic, name).offset
                          for name, _ in _BuiltinTopic._fields_],
        'sample_info_size': ctypes.sizeof(_SampleInfo),
        'sample_info_alignment': ctypes.alignment(_SampleInfo),
        'sample_info_offsets': [getattr(_SampleInfo, name).offset
                                for name, _ in _SampleInfo._fields_],
        'scalar_sizes': [ctypes.sizeof(t) for t in
                         (_entity_t, _return_t, _domainid_t, ctypes.c_int32,
                          ctypes.c_bool, ctypes.c_size_t)],
    }
    if layout != {'topic_key_size': 16, 'topic_size': 40, 'topic_alignment': 8,
                  'topic_offsets': [0, 16, 24, 32], 'sample_info_size': 64,
                  'sample_info_alignment': 8,
                  'sample_info_offsets': [0, 4, 8, 12, 16, 24, 32, 40, 44, 48, 52, 56],
                  'scalar_sizes': [4, 4, 4, 4, 1, 8]}:
        raise NativeParticipantError('abi_contract', 'topic_abi_layout_mismatch',
                                     layout=layout)
    declarations = {
        HEADER: [
            '#define DDS_BUILTIN_TOPIC_DCPSTOPIC ((dds_entity_t) (DDS_MIN_PSEUDO_HANDLE + 2))',
            'typedef struct dds_sample_info { dds_sample_state_t sample_state; '
            'dds_view_state_t view_state; dds_instance_state_t instance_state; bool valid_data; '
            'dds_time_t source_timestamp; dds_instance_handle_t instance_handle; '
            'dds_instance_handle_t publication_handle; uint32_t disposed_generation_count; '
            'uint32_t no_writers_generation_count; uint32_t sample_rank; '
            'uint32_t generation_rank; uint32_t absolute_generation_rank; } dds_sample_info_t;',
            'typedef struct dds_builtintopic_topic_key { unsigned char d[16]; } dds_builtintopic_topic_key_t;',
            'typedef struct dds_builtintopic_topic { dds_builtintopic_topic_key_t key; '
            'char *topic_name; char *type_name; dds_qos_t *qos; } dds_builtintopic_topic_t;'],
        QOS_HEADER: ['bool dds_qget_history (const dds_qos_t * __restrict qos, '
                     'dds_history_kind_t *kind, int32_t *depth);'],
        QOS_DEFS_HEADER: ['typedef enum dds_history_kind { DDS_HISTORY_KEEP_LAST, '
                          'DDS_HISTORY_KEEP_ALL } dds_history_kind_t;'],
        VERSION_HEADER: ['#define DDS_VERSION "0.10.5"'],
        FEATURES_HEADER: ['#define DDS_HAS_TOPIC_DISCOVERY 1'],
    }
    hashes = {}
    for path, fragments in declarations.items():
        try:
            data = path.read_bytes()
            text = re.sub(r'/\*.*?\*/|//[^\n]*', '', data.decode('utf-8'), flags=re.S)
        except (OSError, UnicodeError):
            raise NativeParticipantError('abi_contract', 'topic_abi_header_unavailable',
                                         header=str(path)) from None
        hashes[path.name] = hashlib.sha256(data).hexdigest()
        # Keep token boundaries and require a single declaration. In particular,
        # feature=10 isn't feature=1, and duplicate/conflicting macros fail closed.
        tokens = lambda value: ' '.join(re.findall(r'\w+|[^\s]', value))
        mismatches = []
        for part in fragments:
            if part.startswith('#define'):
                matches = sum(tokens(line) == tokens(part) for line in text.splitlines())
                macro = part.split()[1]
                definitions = len(re.findall(r'^\s*#\s*define\s+' + re.escape(macro) +
                                             r'\b', text, flags=re.M))
                matches = matches if definitions == 1 else 0
            else:
                matches = tokens(text).count(tokens(part))
            mismatches.append(matches != 1)
        if any(mismatches):
            raise NativeParticipantError('abi_contract', 'topic_abi_header_mismatch',
                                         header=str(path), header_sha256=hashes[path.name])
    return {'profile': 'cyclonedds-0.10.5-linux-x86_64-le',
            'source_headers': hashes, 'library_sha256': library_sha256,
            'declared_layout': layout,
            'layout_sha256': hashlib.sha256(json.dumps(layout, sort_keys=True).encode()).hexdigest()}


def _topic_fields(sample, info, library):
    result = {'topic_key': bytes(sample.key.d).hex(), 'key_kind': 'opaque16',
              'topic_name': None, 'type_name': None, 'observation': 'builtin_topic_sample'}
    if not info.valid_data or int(info.instance_state) != 0x10:
        status = ('not_applicable_invalid_data' if not info.valid_data
                  else 'not_applicable_instance_not_alive')
        result['history'] = {'status': status, 'kind': None, 'depth': None,
                             'kind_raw': None, 'depth_raw': None, 'depth_status': status,
                             'source': 'sample.qos:dds_qget_history'}
        return result
    history = {'status': 'unavailable_on_sample', 'kind': None, 'kind_raw': None,
               'depth': None, 'depth_raw': None, 'depth_status': 'unavailable_on_sample',
               'source': 'sample.qos:dds_qget_history', 'reason': 'qos_null'}
    if sample.qos:
        kind, depth = ctypes.c_int32(), ctypes.c_int32()
        found = library.dds_qget_history(ctypes.c_void_p(sample.qos),
                                          ctypes.byref(kind), ctypes.byref(depth))
        if type(found) is not bool:
            raise ValueError('dds_qget_history did not return bool')
        if found:
            if kind.value not in (0, 1) or (kind.value == 0 and depth.value <= 0):
                raise ValueError('dds_qget_history returned invalid kind/depth')
            history = {'status': 'observed',
                       'kind': 'KEEP_ALL' if kind.value == 1 else 'KEEP_LAST',
                       'kind_raw': kind.value,
                       'depth': None if kind.value == 1 else depth.value, 'depth_raw': depth.value,
                       'depth_status': ('not_applicable_keep_all' if kind.value == 1
                                        else 'observed'), 'source': 'sample.qos:dds_qget_history'}
        else:
            history['reason'] = 'getter_false'
    result.update(topic_name=sample.topic_name.decode('utf-8') if sample.topic_name else None,
                  type_name=sample.type_name.decode('utf-8') if sample.type_name else None,
                  history=history)
    return result


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
    if hasattr(library, 'dds_qget_history'):
        library.dds_qget_history.restype = ctypes.c_bool
        library.dds_qget_history.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int32),
                                            ctypes.POINTER(ctypes.c_int32)]
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
        self._reader_topics = {}

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

    def builtin_reader(self, topic='DCPSParticipant'):
        """Create a typed builtin reader; this does not enable topic discovery.

        DCPSTopic requires the inspected header/binary ABI and history symbol.
        All these additional gates are isolated from the legacy participant API.
        """
        if self.participant == ENTITY_NIL or self.library is None:
            return self._record(blocked('participant_not_created', layer='precondition'))
        topics = {'DCPSParticipant': BUILTIN_PARTICIPANT_TOPIC,
                  'DCPSTopic': BUILTIN_TOPIC_TOPIC}
        if not isinstance(topic, str) or topic not in topics:
            return self._record(blocked('unsupported_builtin_topic', layer='config_value'))
        if topic == 'DCPSTopic' and not hasattr(self.library, 'dds_qget_history'):
            return self._record(blocked('topic_symbols_missing', layer='capability_gap',
                                        symbols=['dds_qget_history']))
        topic_abi = None
        if topic == 'DCPSTopic':
            try:
                topic_abi = _topic_abi(self.identity)
            except NativeParticipantError as exc:
                return self._record(blocked(exc.reason, layer=exc.layer, **exc.detail))
        topic_handle = topics[topic]
        handle = self.library.dds_create_reader(_entity_t(self.participant),
                                                _entity_t(topic_handle), None, None)
        if handle <= ENTITY_NIL:
            return self._record(blocked('builtin_reader_create_failed', layer='runtime',
                                        return_code=int(handle),
                                        topic_handle=int(topic_handle)))
        self.created.append(('builtin_reader', handle))
        self._reader_topics[int(handle)] = topic
        return self._record(receipt('native_builtin_reader_created', stage='carrier_admitted',
                                    reader_handle=int(handle), topic=topic,
                                    topic_handle=int(topic_handle), real_execution=True,
                                    **({'topic_abi': topic_abi} if topic_abi else {})))

    def take_participants(self, reader_handle, max_samples=32):
        """Legacy DCPSParticipant samples; retain unregistered-handle support."""
        return self._take_builtin(reader_handle, max_samples, topic='DCPSParticipant')

    def take_topics(self, reader_handle, max_samples=32):
        """Copy a registered DCPSTopic reader's samples, then return its loan.

        Topic keys are opaque16, not GUIDs. History is read only from valid,
        ALIVE sample QoS; unavailable fields remain unknown. KEEP_ALL's raw
        depth is diagnostic only. No discovery lease is inferred from QoS.
        """
        return self._take_builtin(reader_handle, max_samples, topic='DCPSTopic')

    def _take_builtin(self, reader_handle, max_samples, *, topic):
        """Copy builtin fields, then return the reader-owned loan in ``finally``.

        ``dds_take`` receives NULL sample pointers, so CycloneDDS owns any
        returned samples (including their keys/QoS). Never manually call
        ``dds_builtintopic_free_*`` on these reader loans: only
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
        if topic == 'DCPSTopic' and (type(reader_handle) is not int or reader_handle <= 0):
            return self._record(blocked('builtin_reader_handle_invalid', layer='config_type',
                                        **failure_fields))
        registered = (self._reader_topics.get(reader_handle)
                      if isinstance(reader_handle, int) else None)
        if ((topic == 'DCPSTopic' and registered != topic) or
                (registered is not None and registered != topic)):
            return self._record(blocked('builtin_reader_topic_mismatch', layer='precondition',
                                        requested_topic=topic, reader_topic=registered,
                                        **failure_fields))
        topic_abi = None
        if topic == 'DCPSTopic':
            try:
                topic_abi = _topic_abi(self.identity)
            except NativeParticipantError as exc:
                return self._record(blocked(exc.reason, layer=exc.layer,
                                            **failure_fields, **exc.detail))
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
                            sample_type = (_BuiltinTopic if topic == 'DCPSTopic'
                                           else _BuiltinParticipant)
                            sample = ctypes.cast(pointer, ctypes.POINTER(sample_type)).contents
                            state = INSTANCE_STATE.get(int(info[index].instance_state), 'UNKNOWN')
                            fields = (_topic_fields(sample, info[index], self.library)
                                      if topic == 'DCPSTopic' else {
                                          'guid': _guid_text(sample.key),
                                          'observation': 'builtin_participant_sample'})
                            samples.append({**fields, 'instance_state': state,
                                            'valid_data': bool(info[index].valid_data),
                                            'source_timestamp_ns': int(info[index].source_timestamp)})
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
                  'observation_basis': ('dcpstopic_builtin_topic' if topic == 'DCPSTopic'
                                        else 'dcpsparticipant_builtin_topic')}
        if topic_abi:
            fields['topic_abi'] = topic_abi
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
        return self._record(receipt('native_builtin_topics' if topic == 'DCPSTopic'
                                    else 'native_builtin_participants',
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
        self._reader_topics.clear()
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
