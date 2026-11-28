"""Passive, finite DCPSTopic advertisements; no endpoint or execution facts."""
from __future__ import annotations

import copy
from pathlib import Path
import struct

from .rtps import rtps_submessages, udp_frame
from .sedp_fields import parse_parameter_list
from .sedp_wire import _read_capture_prefix
from .spdp_wire import _invalid_health, _invalid_policy, _validate_final_reference


def parse_topic_capture(artifact, *, field_policy=None):
    """Decode retained topic DATA. Configuration never supplies wire fields."""
    if not isinstance(artifact, dict):
        return _unknown('capture_artifact_mapping_required', None)
    health = artifact.get('health')
    for invalid, reason in ((_invalid_health(health), 'invalid_capture_health'),
                            (_invalid_policy(field_policy), 'invalid_field_policy')):
        if invalid:
            result = _unknown(reason, None)
            result['capture_health'] = copy.deepcopy(health)
            result['reasons'].append(reason)
            return result
    try:
        supplied = artifact.get('raw', artifact)
        if not isinstance(supplied, dict):
            raise ValueError('capture_raw_mapping_required')
        if supplied.get('final') is False:
            raise ValueError('capture_prefix_requires_prefix_parser')
        _validate_final_reference(supplied)
        path = Path(supplied['path'])
        before = path.stat()
        identity = {'device': before.st_dev, 'inode': before.st_ino}
        if 'size' in supplied and supplied['size'] != before.st_size:
            raise ValueError('capture_size_mismatch')
        if 'file_identity' in supplied and supplied['file_identity'] != identity:
            raise ValueError('capture_file_identity_mismatch')
        raw, records, diagnostics = _read_capture_prefix({
            **supplied, 'size': before.st_size, 'final': False, 'file_identity': identity})
        after = path.stat()
        if any(getattr(before, k) != getattr(after, k) for k in
               ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')):
            raise ValueError('capture_changed_during_read')
        raw = {k: v for k, v in raw.items() if k not in ('final', 'file_identity')}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return _unknown(str(exc), health)
    return _decode(raw, records, diagnostics, health, field_policy)


def _unknown(reason, health):
    result = _decode(None, [], [{'reason': reason}], health)
    result['status'] = 'unknown'
    result['reasons'].append('capture_reference_unknown')
    return result


def _decode(raw, records, diagnostics, health, field_policy=None):
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
            if sm['kind'] not in (0x15, 0x16) or end-pos < 16:
                continue
            if payload[pos+12:pos+16] != bytes.fromhex('000002c2'):
                continue
            ref = {k: record[k] for k in ('packet_index', 'record_offset', 'frame_offset',
                    'captured_length', 'original_length', 'frame_sha256')}
            ref.update(path=raw['path'], sha256=raw['sha256'], submessage_offset=pos,
                       submessage_end=end, submessage_pcap_offset=record['frame_offset']+udp['payload_offset']+pos,
                       payload_pcap_offset=record['frame_offset']+udp['payload_offset'])
            observations.append(_topic_event(payload, sm, ref, record['unix_ns'], field_policy))
    reasons = []
    if (not health or health.get('status') != 'ok' or
            health.get('filtered_rtps_metadata_overflow', 0) > 0 or
            health.get('filtered_rtps_target_related_count', 0) > 0 or
            ('filtered_rtps_completeness' in health and
             health['filtered_rtps_completeness'] != 'complete_for_known_target_prefixes') or
            ('kernel_drop_status' in health and health['kernel_drop_status'] != 'known_at_stop')):
        reasons.append('capture_health_degraded_or_unknown')
    if diagnostics:
        reasons.append('capture_parse_diagnostics')
    if not observations:
        reasons.append('no_topic_observation_in_finite_capture')
    if any(e['status'] != 'observed' for e in observations):
        reasons.append('unknown_topic_observation')
    return {'schema': 'r2f.topic-discovery/v1', 'raw': raw,
            'independent_source': 'passive_rtps_wire', 'observations': observations,
            'diagnostics': diagnostics, 'capture_health': copy.deepcopy(health),
            'absence_status': 'unknown',
            'assurance': 'advertised topic QoS only; finite silence does not prove topic absence',
            'status': 'unknown' if reasons else 'observed', 'reasons': reasons, 'candidate_eligible': False}


def _topic_event(payload, sm, ref, unix_ns, field_policy=None):
    pos, end = sm['offset'], sm['end']
    event = {k: copy.deepcopy(sm[k]) for k in ('source_guid_prefix', 'destination_guid_prefix',
                                              'vendor_id', 'protocol_version')}
    event.update(raw_ref=ref, unix_ns=unix_ns, clock='pcap_unix_ns',
                 builtin_writer_id='000002c2', topic_key=None, generation=None,
                 generation_status='unknown_separate_lifecycle_evidence_required',
                 candidate_eligible=False, status='unknown', reasons=[],
                 fields={name: {'status': 'unknown', 'value': None, 'reason': 'not_decoded'}
                         for name in ('topic', 'type', 'history', 'depth')})
    reasons = event['reasons']
    if sm['kind'] == 0x16:
        reasons.append('DATA_FRAG_topic_reassembly_unsupported')
        return event
    if end-pos < 24:
        reasons.append('truncated_topic_DATA')
        return event
    if sm['flags'] & 0xf0 or payload[pos+4:pos+6] != b'\0\0':
        reasons.append('unsupported_DATA_flags')
        return event
    if not sm['flags'] & 4 or sm['flags'] & 8:
        reasons.append('key_only_or_no_serialized_data')
        return event
    cursor = pos + 8 + struct.unpack_from(sm['endian']+'H', payload, pos+6)[0]
    if cursor < pos+24 or (cursor-pos-8) % 4 or cursor > end:
        reasons.append('invalid_octetsToInlineQos')
        return event
    if sm['flags'] & 2:
        inline = parse_parameter_list(payload, cursor, end, sm['endian'], raw_ref=ref)
        event['inline_qos'] = inline
        if not inline['complete']:
            reasons.append('incomplete_inline_QoS')
            return event
        cursor = inline['end_offset']
        for pid in (0x71, 0x70):
            params = [p for p in inline['parameters'] if p['pid'] == pid]
            if len(params) > 1:
                reasons.append('duplicate_inline_parameter')
                return event
            # KeyHash_t remains opaque; a malformed key is not a valid DATA
            # announcement and cannot be promoted to topic fields.
            if pid == 0x70 and params and params[0]['length'] != 16:
                reasons.append('invalid_key_hash_length')
                return event
            if pid == 0x71 and params:
                value = bytes.fromhex(params[0]['value_hex'])
                if len(value) != 4:
                    reasons.append('invalid_status_info_length')
                    return event
                if int.from_bytes(value, 'big'):
                    reasons.append('dispose_or_unregister_not_topic_announcement')
                    return event
    if cursor+4 > end:
        reasons.append('truncated_topic_encapsulation')
        return event
    representation = int.from_bytes(payload[cursor:cursor+2], 'big')
    event['representation_id'] = representation
    if representation not in (2, 3):
        reasons.append('unsupported_topic_representation')
        return event
    if payload[cursor+2:cursor+4] != b'\0\0':
        reasons.append('unsupported_encapsulation_options')
        return event
    endian = '<' if payload[cursor:cursor+2] == b'\0\3' else '>'
    pl = parse_parameter_list(payload, cursor+4, end, endian, raw_ref=ref)
    pl['endian'] = endian
    pl['raw_ref'] = copy.deepcopy(ref)
    event['parameter_list'] = pl
    event['fields'] = _topic_fields(pl, field_policy)
    if not pl['complete']:
        event['reasons'].append(pl['reason'])
    if any(f['status'] == 'unknown' for f in event['fields'].values()):
        event['reasons'].append('unknown_topic_fields')
    event['status'] = 'unknown' if reasons else 'observed'
    return event


def _topic_fields(pl, field_policy=None):
    fields = {}
    for name, pid in (('topic', 5), ('type', 7), ('history', 0x40), ('depth', 0x40)):
        params = [p for p in pl['parameters'] if p['pid'] == pid]
        reason = ('incomplete_parameter_list' if not pl['complete'] else
                  'absent_parameter' if not params else
                  'duplicate_singleton_parameter' if len(params) != 1 else None)
        if (reason == 'absent_parameter' and name in ('history', 'depth')
                and (field_policy or {}).get('protocol_defaults') is True and pl['complete']):
            fields[name] = {'status': 'observed', 'value': 'KEEP_LAST' if name == 'history' else 1,
                'provenance': 'protocol_default', 'parameter_present': False,
                'parameter_list_complete': True, 'standard': 'DDS1.4 2.2.3.18',
                'default_rule': 'DDSI-RTPS2.5 9.4.2.11 / Table9.19',
                'raw_refs': [copy.deepcopy(pl['raw_ref'])], 'sentinel_offset': pl['sentinel_offset']}
            continue
        if reason:
            fields[name] = {'status': 'unknown', 'value': None, 'pid': pid, 'reason': reason,
                            'raw_refs': [p['raw_ref'] for p in params]}
            continue
        param, = params
        data = bytes.fromhex(param['value_hex'])
        try:
            if name in ('topic', 'type'):
                if len(data) < 8:
                    raise ValueError('invalid_CDR_string')
                size = struct.unpack_from(pl['endian']+'I', data)[0]
                if (size < 2 or 4+size > len(data) or len(data) != 4+((size+3)//4)*4
                        or data[3+size] != 0 or b'\0' in data[4:3+size]):
                    raise ValueError('invalid_CDR_string')
                try:
                    value = data[4:3+size].decode('utf-8')
                except UnicodeDecodeError:
                    raise ValueError('invalid_CDR_string') from None
            else:
                if len(data) != 8:
                    raise ValueError('invalid_topic_history_length')
                kind, depth = struct.unpack(pl['endian']+'ii', data)
                if kind not in (0, 1) or (kind == 0 and depth < 1):
                    raise ValueError('invalid_topic_history')
                value = {0: 'KEEP_LAST', 1: 'KEEP_ALL'}[kind] if name == 'history' else depth
        except ValueError as exc:
            fields[name] = {'status': 'unknown', 'value': None, 'pid': pid,
                            'reason': str(exc), 'raw_refs': [param['raw_ref']]}
            continue
        ref = dict(param['raw_ref'])
        ref.update(pcap_parameter_offset=ref['pcap_value_offset']-4,
                   parameter_offset_in_frame=ref['pcap_value_offset']-4-ref['frame_offset'])
        fields[name] = {'status': 'observed', 'value': value, 'pid': pid,
                        'provenance': 'wire_parameter', 'raw_refs': [ref]}
        if name == 'depth' and kind == 1:
            fields[name].update(status='not_applicable', value=None, wire_value=depth,
                                reason='ignored_for_KEEP_ALL')
    return fields
