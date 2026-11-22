"""Endpoint ParameterList fields and explicitly versioned duration interpretation."""
from __future__ import annotations
import copy
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import struct

BASELINE_FIELDS = ('endpoint_guid', 'role', 'topic', 'type', 'reliability', 'durability',
                   'deadline', 'liveliness.kind', 'liveliness.lease_duration')
PID = {'endpoint_guid': 0x5a, 'topic': 5, 'type': 7, 'reliability': 0x1a,
       'durability': 0x1d, 'deadline': 0x23, 'liveliness.kind': 0x1b,
       'liveliness.lease_duration': 0x1b}
ENUMS = {'reliability': {1: 'BEST_EFFORT', 2: 'RELIABLE'},
         'durability': {0: 'VOLATILE', 1: 'TRANSIENT_LOCAL', 2: 'TRANSIENT', 3: 'PERSISTENT'},
         'liveliness.kind': {0: 'AUTOMATIC', 1: 'MANUAL_BY_PARTICIPANT', 2: 'MANUAL_BY_TOPIC'}}
DEFAULTS = {'durability': ('VOLATILE', 'DDS1.4 2.2.3.4'),
            'deadline': ({'infinite': True, 'nanoseconds': None}, 'DDS1.4 2.2.3.7'),
            'liveliness.kind': ('AUTOMATIC', 'DDS1.4 2.2.3.11'),
            'liveliness.lease_duration': ({'infinite': True, 'nanoseconds': None}, 'DDS1.4 2.2.3.11')}
# These are local installed-binary profiles, not assumptions about all old RTPS.
LEGACY = {
    ('0110', (2, 1), '0.10.5'): {
        'library_sha256': '9a63f94ee673c5efddb337a7dd012e4c98960b10ce256dcc3a7b4c331fabf5b5',
        'rounding': 'nearest_ns', 'source': 'CycloneDDS0.10.5 ddsi_plist.c XD / ddsi_time.c',
        'source_sha256': 'ec918a523fe439f8c5338f2e54e345deb87f3bd2df40fe2699543286c7e8442c'},
    ('010f', (2, 3), '2.14.7'): {
        'library_sha256': '2b80489f57d87ce4832766371c5e9f4b0c6b035dc850b284b9f6afcad4962929',
        'rounding': 'floor_ns', 'source': 'FastDDS2.14.7 QosPoliciesSerializer.hpp / Time_t.cpp',
        'source_sha256': 'd311bd75227eb0366f2e89237a3f3a3bd55e2d673e6439117ed123096033e10b'},
}


def _manifest_has_hash(value, digest):
    if isinstance(value, dict):
        return any(value.get(key) == digest for key in ('sha256', 'library_sha256')) or any(_manifest_has_hash(v, digest) for v in value.values())
    if isinstance(value, list):
        return any(_manifest_has_hash(v, digest) for v in value)
    return False


def legacy_duration_profile(vendor_id, protocol_version, policy):
    declaration = policy.get('vendor_versions', {}).get(vendor_id)
    if not isinstance(declaration, str):
        return None
    profile = LEGACY.get((vendor_id, tuple(protocol_version), declaration))
    ref = policy.get('vendor_provenance', {}).get(vendor_id, {})
    if not profile or not isinstance(ref, dict):
        return None
    try:
        data = Path(ref['path']).read_bytes()
        if hashlib.sha256(data).hexdigest() != ref['sha256'] or not _manifest_has_hash(json.loads(data), profile['library_sha256']):
            return None
    except (KeyError, OSError, TypeError, ValueError):
        return None
    return {**profile, 'vendor_version': declaration, 'provenance_ref': copy.deepcopy(ref)}


def decode_duration(data, endian, *, vendor_id, protocol_version, field_policy=None):
    """Keep exact wire rational; old vendor decoding requires hashed binary evidence.

    DDS Duration_t uses seconds/nanoseconds in its API. These verified installed
    serializers use seconds/fraction on the wire. Do not reuse INFO_TS decoding
    blindly for unverified pre-2.4 implementations.
    """
    if len(data) != 8:
        raise ValueError('duration_length')
    seconds, second = struct.unpack(endian + 'iI', data)
    profile = legacy_duration_profile(vendor_id, protocol_version, field_policy or {})
    standard = tuple(protocol_version) >= (2, 4)
    result = {'seconds': seconds, 'second_word': second, 'raw_hex': data.hex(),
              'semantic_type': 'DDS_QoS_duration', 'vendor_id': vendor_id,
              'protocol_version': list(protocol_version), 'nanoseconds': None,
              'encoding': 'seconds_fraction_2pow32' if profile or standard else 'unknown_legacy',
              'encoding_basis': profile or ('DDSI-RTPS2.5 9.3.2.3 (specified since 2.4)' if standard else None)}
    if (seconds, second) == (0x7fffffff, 0xffffffff) and (profile or standard):
        return {**result, 'infinite': True, 'status': 'observed'}
    if seconds < 0:
        return {**result, 'infinite': None, 'status': 'unknown', 'reason': 'invalid_negative_duration'}
    if not (profile or standard):
        # Integral seconds have no fractional ambiguity; nonzero second word does.
        if second == 0:
            return {**result, 'infinite': False, 'status': 'observed', 'nanoseconds': seconds * 10**9,
                    'exact_seconds_numerator': seconds, 'exact_seconds_denominator': 1,
                    'encoding': 'integral_seconds_only_unambiguous'}
        return {**result, 'infinite': None, 'status': 'unknown', 'reason': 'unverified_legacy_duration_encoding'}
    ns = None
    if profile:
        ns = seconds * 10**9 + ((second * 10**9 + (1 << 31)) >> 32 if profile['rounding'] == 'nearest_ns' else (second * 10**9) >> 32)
    elif (second * 10**9) % (1 << 32) == 0:
        ns = seconds * 10**9 + (second * 10**9) // (1 << 32)
    return {**result, 'status': 'observed', 'infinite': False, 'nanoseconds': ns,
            'exact_seconds_numerator': (seconds << 32) + second, 'exact_seconds_denominator': 1 << 32,
            'nanoseconds_policy': profile['rounding'] if profile else 'exact_only_no_vendor_rounding_assumed'}


def parse_parameter_list(payload, start, end, endian, *, raw_ref):
    params, cursor = [], start
    while cursor + 4 <= end:
        pid, size = struct.unpack_from(endian + 'HH', payload, cursor)
        if pid == 1:
            return {'complete': True, 'parameters': params, 'end_offset': cursor + 4,
                    'sentinel_offset': cursor, 'sentinel_length_ignored': size}
        if size % 4 or cursor + 4 + size > end:
            return {'complete': False, 'parameters': params, 'end_offset': cursor,
                    'reason': 'invalid_parameter_length_or_truncation'}
        params.append({'pid': pid, 'length': size, 'value_hex': payload[cursor + 4:cursor + 4 + size].hex(),
                       'raw_ref': {**raw_ref, 'value_offset_in_payload': cursor + 4,
                                   'pcap_value_offset': raw_ref['payload_pcap_offset'] + cursor + 4,
                                   'value_length': size}})
        cursor += 4 + size
    return {'complete': False, 'parameters': params, 'end_offset': cursor, 'reason': 'missing_PID_SENTINEL'}


def endpoint_fields(pl, *, role, vendor_id, protocol_version, field_policy=None):
    policy = field_policy or {}
    defaults = {**DEFAULTS, 'reliability': (
        'RELIABLE' if role == 'writer' else 'BEST_EFFORT', 'DDS1.4 Table2.1 / 2.2.3.14')}
    by_pid = {}
    for p in pl['parameters']:
        by_pid.setdefault(p['pid'], []).append(p)
    unknown = lambda reason: {'status': 'unknown', 'value': None, 'reason': reason}
    fields = {name: unknown('absent_parameter' if pl['complete'] else 'incomplete_parameter_list') for name in BASELINE_FIELDS}
    fields['role'] = {'status': 'observed', 'value': role, 'provenance': 'SEDP_builtin_writer_entity_id'}
    for name, pid in PID.items():
        values = by_pid.get(pid, [])
        if not values:
            continue
        if len(values) != 1:
            fields[name] = unknown('duplicate_singleton_parameter')
            continue
        param, endian = values[0], pl['endian']
        data = bytes.fromhex(param['value_hex'])
        try:
            required_length = {'reliability': 12, 'durability': 4, 'deadline': 8,
                               'liveliness.kind': 12, 'liveliness.lease_duration': 12}.get(name)
            if required_length is not None and len(data) != required_length:
                raise ValueError('invalid_QoS_parameter_length')
            if name == 'endpoint_guid':
                if len(data) != 16:
                    raise ValueError('invalid_guid_length')
                value = data.hex()
            elif name in ('topic', 'type'):
                size = struct.unpack_from(endian + 'I', data)[0]
                if size < 1 or 4 + size > len(data) or data[3 + size] != 0:
                    raise ValueError('invalid_CDR_string')
                value = data[4:3 + size].decode('utf-8')
            elif name in ENUMS:
                number = struct.unpack_from(endian + 'i', data)[0]
                value = ENUMS[name].get(number)
                if value is None:
                    raise ValueError('unsupported_QoS_enum')
            else:
                value = decode_duration(data[4:12] if name.startswith('liveliness') else data[:8], endian,
                                        vendor_id=vendor_id, protocol_version=protocol_version, field_policy=policy)
            field = {'status': 'observed', 'value': value, 'provenance': 'wire_parameter',
                     'pid': pid, 'raw_refs': [param['raw_ref']]}
            if isinstance(value, dict) and value.get('status') == 'unknown':
                field.update(status='unknown', reason=value['reason'])
            fields[name] = field
        except (ValueError, UnicodeDecodeError, struct.error) as exc:
            fields[name] = {**unknown(str(exc)), 'pid': pid, 'raw_refs': [param['raw_ref']]}
    if policy.get('protocol_defaults', True) and pl['complete']:
        for name, (value, reference) in defaults.items():
            if fields[name].get('reason') == 'absent_parameter':
                fields[name] = {'status': 'observed', 'value': copy.deepcopy(value), 'provenance': 'protocol_default',
                                'parameter_present': False, 'parameter_list_complete': True,
                                'standard': reference, 'default_rule': 'DDSI-RTPS2.5 9.4.2.11 / Table9.19',
                                'raw_refs': [pl['raw_ref']], 'sentinel_offset': pl['sentinel_offset']}
    guid = fields['endpoint_guid'].get('value')
    if guid:
        kind = int(guid[-2:], 16)
        expected = 'writer' if kind in (2, 3) else 'reader' if kind in (4, 7) else None
        if expected != role:
            fields['role'] = unknown('GUID_entity_kind_vs_SEDP_role_mismatch')
    for name in ('history', 'depth'):
        fields[name] = {'status': 'not_applicable_by_endpoint_schema', 'value': None,
                        'provenance': 'D014_main_agent_revision0_field_applicability'}
    diagnostics = {}
    if 0x40 in by_pid:
        diagnostics['history_extension'] = by_pid[0x40]
    return fields, diagnostics


def semantic_duration(value):
    if not isinstance(value, dict):
        return None
    if value.get('infinite') is True:
        return 'infinite'
    if isinstance(value.get('nanoseconds'), int) and not isinstance(value['nanoseconds'], bool) and value['nanoseconds'] >= 0:
        return Fraction(value['nanoseconds'], 10**9)
    numerator, denominator = value.get('exact_seconds_numerator'), value.get('exact_seconds_denominator')
    if (isinstance(numerator, int) and not isinstance(numerator, bool) and numerator >= 0
            and isinstance(denominator, int) and not isinstance(denominator, bool) and denominator > 0):
        return Fraction(numerator, denominator)
    return None


def discovery_layer_schema():
    """Applicability of the current passive SEDP/public endpoint graph seam.

    Participant prefixes and endpoint-derived topic names are supporting facts,
    not independent participant or DCPSTopic builtin observations.
    """
    return {
        'participant': {
            'status': 'unavailable_api',
            'required_fields': ['participant_guid', 'lease_duration'],
            'fields': {'participant_guid': 'unavailable_api',
                       'lease_duration': 'unavailable_api'},
            'basis': 'public endpoint graph does not expose participant builtin data'},
        'endpoint': {
            'status': 'field_comparison', 'required_fields': list(BASELINE_FIELDS),
            'fields': {**{name: 'required' for name in BASELINE_FIELDS},
                       'history': 'not_applicable_by_endpoint_schema',
                       'depth': 'not_applicable_by_endpoint_schema'},
            'basis': 'D014 endpoint schema; absent required values remain unknown'},
        'topic': {
            'status': 'endpoint_derived_only',
            'required_fields': ['topic', 'type', 'history', 'depth'],
            'fields': {'topic': 'endpoint_derived_only', 'type': 'endpoint_derived_only',
                       'history': 'unavailable_api', 'depth': 'unavailable_api'},
            'basis': 'endpoint topic/type are not an independent DCPSTopic observation'}}
