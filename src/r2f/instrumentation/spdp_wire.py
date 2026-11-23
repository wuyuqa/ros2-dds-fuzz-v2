"""Passive participant lease advertisements, not remote expiry or ownership facts."""
from __future__ import annotations

import copy
from pathlib import Path
import re
import struct

from .rtps import rtps_submessages, udp_frame
from .sedp_fields import decode_duration, parse_parameter_list
from .sedp_wire import MAX_CAPTURE_PREFIX_BYTES, _read_capture_prefix


def parse_spdp_capture(artifact, *, field_policy=None):
    """Decode hash-bound retained SPDP DATA; no graph/config supplies wire facts.

    field_policy only supplies the existing hashed legacy duration provenance;
    it cannot supply a missing GUID, duration or lifecycle generation.
    """
    if not isinstance(artifact, dict):
        return _read_unknown('capture_artifact_mapping_required', None)
    health = artifact.get('health')
    if _invalid_health(health):
        return _metadata_unknown('invalid_capture_health', health)
    if _invalid_policy(field_policy):
        return _metadata_unknown('invalid_field_policy', health)
    try:
        supplied = artifact.get('raw', artifact)
        if not isinstance(supplied, dict):
            raise ValueError('capture_raw_mapping_required')
        if supplied.get('final') is False:
            raise ValueError('capture_prefix_requires_prefix_parser')
        _validate_final_reference(supplied)
        path = Path(supplied['path'])
        before = path.stat()
        if before.st_size > MAX_CAPTURE_PREFIX_BYTES:
            raise ValueError('capture_size_limit')
        # Reuse the bounded inode/hash reader even for finalized captures. Verify
        # the full-file metadata on both sides to reject append/rewrite races.
        prefix = {**supplied, 'size': before.st_size, 'final': False,
                  'file_identity': {'device': before.st_dev, 'inode': before.st_ino}}
        raw, records, diagnostics = _read_capture_prefix(prefix)
        after = path.stat()
        if any(getattr(before, k) != getattr(after, k) for k in
               ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')):
            raise ValueError('capture_changed_during_read')
        raw = {k: v for k, v in raw.items() if k not in ('final', 'file_identity')}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return _read_unknown(exc, health)
    return _decode(raw, records, diagnostics, health, field_policy)


def parse_spdp_capture_prefix(raw_prefix, *, capture_health=None, field_policy=None):
    """Observe a fixed flushed prefix; append is allowed, replacement is unknown."""
    if _invalid_health(capture_health):
        return _metadata_unknown('invalid_capture_health', capture_health)
    if _invalid_policy(field_policy):
        return _metadata_unknown('invalid_field_policy', capture_health)
    try:
        raw, records, diagnostics = _read_capture_prefix(raw_prefix)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return _read_unknown(exc, capture_health)
    result = _decode(raw, records, diagnostics, capture_health, field_policy)
    result['independent_source'] = 'passive_rtps_wire_prefix'
    return result


def _validate_final_reference(raw):
    name, digest = raw.get('path'), raw.get('sha256')
    if not isinstance(name, str) or not name or '\0' in name or not Path(name).is_absolute():
        raise ValueError('absolute_capture_path_required')
    if not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None:
        raise ValueError('invalid_capture_sha256')
    if 'final' in raw and type(raw['final']) is not bool:
        raise ValueError('invalid_capture_final')
    if 'size' in raw and (type(raw['size']) is not int or not 24 <= raw['size'] <= MAX_CAPTURE_PREFIX_BYTES):
        raise ValueError('invalid_capture_size')
    if 'file_identity' in raw:
        identity = raw['file_identity']
        if (not isinstance(identity, dict) or set(identity) != {'device', 'inode'} or
                any(type(value) is not int or value < 0 for value in identity.values()) or not identity['inode']):
            raise ValueError('invalid_capture_file_identity')


def _invalid_health(health):
    # None/missing health remains an observation limitation, never evidence of ok.
    if health is None:
        return False
    if not isinstance(health, dict):
        return True
    if 'status' in health and not isinstance(health['status'], str):
        return True
    if 'reasons' in health and (not isinstance(health['reasons'], list) or
            any(not isinstance(reason, str) for reason in health['reasons'])):
        return True
    if health.get('status') == 'ok' and health.get('reasons'):
        return True
    for key in ('filtered_rtps_count', 'filtered_rtps_target_related_count'):
        if key in health and (type(health[key]) is not int or health[key] < 0):
            return True
    if 'filtered_rtps_metadata_overflow' in health:
        # The capture producer emits a count; retain legacy boolean receipts.
        overflow = health['filtered_rtps_metadata_overflow']
        if type(overflow) not in (int, bool) or overflow < 0:
            return True
    for key in ('kernel_drop_status', 'filtered_rtps_completeness'):
        if key in health and not isinstance(health[key], str):
            return True
    return False


def _invalid_policy(policy):
    if policy is None:
        return False
    if not isinstance(policy, dict):
        return True
    versions = policy.get('vendor_versions', {})
    provenance = policy.get('vendor_provenance', {})
    if (not isinstance(versions, dict) or not isinstance(provenance, dict) or
            any(not isinstance(version, str) for version in versions.values())):
        return True
    for ref in provenance.values():
        if not isinstance(ref, dict):
            return True
        name, digest = ref.get('path'), ref.get('sha256')
        if (not isinstance(name, str) or not name or '\0' in name or
                not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None):
            return True
    # Shape validation supplies no profile or facts; decode_duration still checks
    # the declared version, manifest hash and pinned installed-library digest.
    return False


def _metadata_unknown(reason, health):
    result = _decode(None, [], [{'reason': reason, 'kind': 'input_metadata_unknown'}], None, None)
    result['capture_health'] = copy.deepcopy(health)
    result['reasons'].append(reason)
    return result


def _read_unknown(error, health):
    result = _decode(None, [], [{'reason': str(error), 'kind': 'capture_reference_unknown'}], health, None)
    result['reasons'].append('capture_reference_unknown')
    return result


def _singleton(pl, pid, name, reasons):
    values = [p for p in pl['parameters'] if p['pid'] == pid]
    if not values:
        reasons.append('missing_' + name)
        return None
    if len(values) != 1:
        kind = 'duplicate_' if len({p['value_hex'] for p in values}) == 1 else 'conflicting_'
        reasons.append(kind + name)
        return None
    return bytes.fromhex(values[0]['value_hex'])


def _lease_event(payload, sm, ref, unix_ns, policy):
    pos, end, se = sm['offset'], sm['end'], sm['endian']
    event = {k: copy.deepcopy(sm[k]) for k in ('source_guid_prefix', 'destination_guid_prefix',
                                              'vendor_id', 'protocol_version')}
    reasons = []
    event.update(raw_ref=ref, unix_ns=unix_ns, clock='pcap_unix_ns',
                 builtin_writer_id='000100c2', participant_guid=None, lease=None,
                 generation=None, generation_status='unknown_separate_lifecycle_evidence_required',
                 ownership_status='unknown_separate_ownership_evidence_required',
                 candidate_eligible=False, status='unknown', reasons=reasons)
    if sm['kind'] == 0x16:
        reasons.append('DATA_FRAG_participant_reassembly_unsupported')
        return event
    if end-pos < 24:
        reasons.append('truncated_SPDP_DATA')
        return event
    hi, lo = struct.unpack_from(se+'iI', payload, pos+16)
    event['sequence_number'] = (hi << 32) + lo
    if event['sequence_number'] <= 0:
        reasons.append('invalid_writer_sequence_number')
    if sm['protocol_version'][0] != 2 or not 1 <= sm['protocol_version'][1] <= 5:
        reasons.append('unsupported_protocol_version')
    if sm['source_guid_prefix'] == '00'*12:
        reasons.append('unknown_source_guid_prefix')
    if sm['flags'] & 0xf0 or payload[pos+4:pos+6] != b'\0\0':
        reasons.append('unsupported_DATA_flags')
    if not sm['flags'] & 4 or sm['flags'] & 8:
        reasons.append('key_only_or_no_serialized_data')
        return event
    offset = struct.unpack_from(se+'H', payload, pos+6)[0]
    cursor = pos + 8 + offset
    if offset < 16 or offset % 4 or cursor > end:
        reasons.append('invalid_octetsToInlineQos')
        return event
    inline_key = None
    if sm['flags'] & 2:
        inline = parse_parameter_list(payload, cursor, end, se, raw_ref=ref)
        event['inline_qos'] = inline
        if not inline['complete']:
            reasons.append('incomplete_inline_QoS')
            return event
        cursor = inline['end_offset']
        for pid, name in ((0x71, 'status_info'), (0x70, 'key_hash')):
            if not any(p['pid'] == pid for p in inline['parameters']):
                continue
            value = _singleton(inline, pid, name, reasons)
            if value is None:
                continue
            if pid == 0x71:
                if len(value) != 4:
                    reasons.append('invalid_status_info_length')
                elif int.from_bytes(value, 'big'):
                    reasons.append('dispose_or_unregister_not_lease_announcement')
            else:
                inline_key = value
        if any(p['pid'] & 0x4000 for p in inline['parameters']):
            reasons.append('unsupported_required_inline_parameter')
    if cursor+4 > end:
        reasons.append('truncated_spdp_encapsulation')
        return event
    representation = int.from_bytes(payload[cursor:cursor+2], 'big')
    event['representation_id'] = representation
    event['encapsulation_options_hex'] = payload[cursor+2:cursor+4].hex()
    if representation not in (2, 3):
        reasons.append('unsupported_spdp_representation')
        return event
    if payload[cursor+2:cursor+4] != b'\0\0':
        reasons.append('unsupported_encapsulation_options')
    endian = '<' if representation == 3 else '>'
    pl = parse_parameter_list(payload, cursor+4, end, endian, raw_ref=ref)
    pl['endian'] = endian
    event['parameter_list'] = pl
    if not pl['complete']:
        reasons.append(pl['reason'])
    if any(p['pid'] & 0x4000 for p in pl['parameters']):
        reasons.append('unsupported_required_parameter')
    guid = _singleton(pl, 0x50, 'participant_guid', reasons)
    duration = _singleton(pl, 2, 'participant_lease_duration', reasons)
    if guid is not None:
        event['advertised_participant_guid_hex'] = guid.hex()
        if len(guid) != 16:
            reasons.append('invalid_participant_guid_length')
        elif guid[12:] != bytes.fromhex('000001c1'):
            reasons.append('not_participant_entity')
        elif guid[:12].hex() != sm['source_guid_prefix']:
            reasons.append('participant_guid_source_mismatch')
        else:
            event['participant_guid'] = guid.hex()
        if inline_key is not None and inline_key != guid:
            reasons.append('inline_key_participant_guid_conflict')
    for pid, name, expected in ((0x15, 'protocol_version', bytes(sm['protocol_version'])),
                                 (0x16, 'vendor_id', bytes.fromhex(sm['vendor_id']))):
        if any(p['pid'] == pid for p in pl['parameters']):
            value = _singleton(pl, pid, name, reasons)
            if value is not None and (len(value) != 4 or value[:2] != expected):
                reasons.append(name + '_parameter_conflict')
    if duration is not None:
        if len(duration) != 8:
            reasons.append('invalid_participant_lease_duration_length')
        else:
            lease = decode_duration(duration, endian, vendor_id=sm['vendor_id'],
                                    protocol_version=sm['protocol_version'], field_policy=policy)
            lease['semantic_type'] = 'participant_discovery_lease'
            event['lease'] = lease
            if lease['status'] != 'observed':
                reasons.append(lease['reason'])
    if not reasons:
        event['status'] = 'observed'
    return event


def _decode(raw, records, diagnostics, health, policy):
    observations = []
    for record in records:
        udp, error = udp_frame(record['frame'])
        if error:
            diagnostics.append({'packet_index': record['packet_index'], 'reason': error})
        if udp is None or udp['payload'][:4] != b'RTPS':
            continue
        payload = udp['payload']
        messages, errors = rtps_submessages(payload)
        diagnostics.extend({'packet_index': record['packet_index'], 'reason': e} for e in errors)
        for sm in messages:
            pos, end = sm['offset'], sm['end']
            if sm['kind'] not in (0x15, 0x16):
                continue
            if end-pos < 16:
                diagnostics.append({'packet_index': record['packet_index'], 'reason': 'truncated_discovery_DATA_identity'})
                continue
            if payload[pos+12:pos+16] != bytes.fromhex('000100c2'):
                continue
            ref = {k: record[k] for k in ('packet_index', 'record_offset', 'frame_offset',
                    'captured_length', 'original_length', 'frame_sha256')}
            ref.update(path=raw['path'], sha256=raw['sha256'], submessage_offset=pos,
                       submessage_end=end, submessage_pcap_offset=record['frame_offset']+udp['payload_offset']+pos,
                       payload_pcap_offset=record['frame_offset']+udp['payload_offset'])
            if raw.get('final') is False:
                ref.update(size=raw['size'], final=False, file_identity=copy.deepcopy(raw['file_identity']))
            event = _lease_event(payload, sm, ref, record['unix_ns'], policy)
            if errors or record['captured_length'] != record['original_length']:
                event['status'] = 'unknown'
                event['reasons'].append('frame_framing_incomplete')
            observations.append(event)
    reasons = []
    if (not health or health.get('status') != 'ok' or
            health.get('filtered_rtps_metadata_overflow', 0) > 0):
        reasons.append('capture_health_degraded_or_unknown')
    if diagnostics:
        reasons.append('capture_parse_diagnostics')
    if not observations:
        reasons.append('no_participant_lease_observation_in_finite_capture')
    elif any(e['status'] != 'observed' for e in observations):
        reasons.append('unknown_participant_lease_observation')
    return {'schema': 'r2f.spdp-participant-lease/v1', 'raw': raw,
            'independent_source': 'passive_rtps_wire', 'observations': observations,
            'diagnostics': diagnostics, 'capture_health': copy.deepcopy(health),
            'status': 'unknown' if reasons else 'observed', 'reasons': reasons,
            'candidate_eligible': False, 'expiry_status': 'unknown', 'cleanup_status': 'unknown',
            'assurance': 'advertised participant lease only; finite silence does not prove remote expiry or cleanup'}
