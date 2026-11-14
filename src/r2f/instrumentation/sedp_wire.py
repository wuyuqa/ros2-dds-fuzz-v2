"""Offline pcap decoding with raw identity; wire input cannot use graph or QoS config."""
from __future__ import annotations
from collections import Counter
import copy
import hashlib
import json
from pathlib import Path
import struct
from .rtps import discovery_activity, udp_frame, rtps_submessages
from .sedp_fields import parse_parameter_list, endpoint_fields


def read_capture(artifact):
    """Read a stable, hash-bound classic pcap with Ethernet linktype."""
    raw = artifact.get('raw', artifact)
    if not isinstance(raw, dict) or not raw.get('path') or not raw.get('sha256'):
        raise ValueError('capture requires explicit raw path and sha256')
    path = Path(raw['path'])
    before = path.stat()
    data = path.read_bytes()
    after = path.stat()
    keys = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
    if any(getattr(before, key) != getattr(after, key) for key in keys):
        raise ValueError('capture_changed_during_read')
    digest = hashlib.sha256(data).hexdigest()
    if digest != raw['sha256']:
        raise ValueError('capture_sha256_mismatch')
    magic = {b'\x4d\x3c\xb2\xa1': ('<', 1), b'\xa1\xb2\x3c\x4d': ('>', 1),
             b'\xd4\xc3\xb2\xa1': ('<', 1000), b'\xa1\xb2\xc3\xd4': ('>', 1000)}
    if len(data) < 24 or data[:4] not in magic:
        raise ValueError('unsupported_or_truncated_pcap_header')
    endian, precision = magic[data[:4]]
    major, minor, _, _, snaplen, linktype = struct.unpack_from(endian + 'HHIIII', data, 4)
    if (major, minor) != (2, 4) or linktype != 1:
        raise ValueError('unsupported_pcap_version_or_linktype')
    records, diagnostics = [], []
    pos = 24
    while pos < len(data):
        if pos + 16 > len(data):
            diagnostics.append({'offset': pos, 'reason': 'truncated_pcap_record_header'})
            break
        seconds, subseconds, captured, original = struct.unpack_from(endian + 'IIII', data, pos)
        if captured > snaplen or captured > original or pos + 16 + captured > len(data):
            diagnostics.append({'offset': pos, 'reason': 'truncated_or_invalid_pcap_record'})
            break
        if subseconds >= (10**9 if precision == 1 else 10**6):
            diagnostics.append({'offset': pos, 'reason': 'invalid_pcap_timestamp'})
            break
        frame = data[pos + 16:pos + 16 + captured]
        records.append({'frame': frame, 'unix_ns': seconds * 10**9 + subseconds * precision,
                        'packet_index': len(records), 'record_offset': pos, 'frame_offset': pos + 16,
                        'captured_length': captured, 'original_length': original,
                        'frame_sha256': hashlib.sha256(frame).hexdigest()})
        if captured < original:
            diagnostics.append({'packet_index': len(records) - 1, 'reason': 'truncated_frame'})
        pos += 16 + captured
    return {'path': str(path.resolve()), 'sha256': digest, 'size': len(data),
            'linktype': linktype, 'timestamp_precision_ns': precision}, records, diagnostics


def parse_sedp_capture(artifact, *, field_policy=None):
    """Parse independent wire frames and discovery activity. No graph/config inputs.

    Field-policy interpretation is added independently of the capture lifecycle;
    consumers of third-target activity need no endpoint QoS fields.
    """
    raw, records, diagnostics = read_capture(artifact)
    activity, copies, endpoint_events = [], Counter(), []
    policy = copy.deepcopy(field_policy or {})
    for record in records:
        events, errors = discovery_activity(record['frame'])
        diagnostics.extend({'packet_index': record['packet_index'], 'reason': error} for error in errors)
        ref = {key: record[key] for key in ('packet_index', 'record_offset', 'frame_offset',
                                          'captured_length', 'original_length', 'frame_sha256')}
        ref.update(path=raw['path'], sha256=raw['sha256'])
        for event in events:
            key = (event['frame_sha256'], event['submessage_offset'])
            copies[key] += 1
            activity.append({**event, 'unix_ns': record['unix_ns'], 'raw_ref': ref})
        udp, diagnostic = udp_frame(record['frame'])
        if udp is None or udp['payload'][:4] != b'RTPS':
            continue
        payload = udp['payload']
        messages, _ = rtps_submessages(payload)
        for sm in messages:
            if sm['kind'] == 0x16:
                diagnostics.append({'packet_index': record['packet_index'], 'reason': 'DATA_FRAG_endpoint_reassembly_unsupported'})
            if sm['kind'] != 0x15 or sm['end'] - sm['offset'] < 24:
                continue
            pos, end, endian = sm['offset'], sm['end'], sm['endian']
            writer = payload[pos + 12:pos + 16].hex()
            if writer not in ('000003c2', '000004c2'):
                continue
            role = 'writer' if writer == '000003c2' else 'reader'
            hi, lo = struct.unpack_from(endian + 'iI', payload, pos + 16)
            inline_offset = struct.unpack_from(endian + 'H', payload, pos + 6)[0]
            if inline_offset < 16 or inline_offset % 4:
                diagnostics.append({'packet_index': record['packet_index'], 'reason': 'invalid_octetsToInlineQos'})
                continue
            cursor = pos + 8 + inline_offset
            eref = {**ref, 'payload_pcap_offset': record['frame_offset'] + udp['payload_offset']}
            event = {**{k: sm[k] for k in ('source_guid_prefix', 'destination_guid_prefix', 'vendor_id', 'protocol_version')},
                     'unix_ns': record['unix_ns'], 'builtin_writer_id': writer,
                     'sequence_number': (hi << 32) + lo, 'raw_ref': eref, 'role': role,
                     'lifecycle': 'alive', 'endpoint_guid': None, 'fields': {}}
            inline = None
            if sm['flags'] & 2:
                inline = parse_parameter_list(payload, cursor, end, endian, raw_ref=eref)
                event['inline_qos'] = inline
                if not inline['complete']:
                    diagnostics.append({'packet_index': record['packet_index'], 'reason': 'incomplete_inline_QoS'})
                    endpoint_events.append(event)
                    continue
                cursor = inline['end_offset']
                for param in inline['parameters']:
                    if param['pid'] == 0x71:
                        status = bytes.fromhex(param['value_hex'])
                        if len(status) == 4 and int.from_bytes(status, 'big') & 3:
                            event.update(lifecycle='dispose_or_unregister', status_info_hex=status.hex())
                    if param['pid'] == 0x70 and param['length'] == 16:
                        event['endpoint_guid'] = param['value_hex']
            if cursor > end:
                diagnostics.append({'packet_index': record['packet_index'], 'reason': 'invalid_octetsToInlineQos'})
            elif sm['flags'] & 0x0c and cursor + 4 <= end:
                representation = int.from_bytes(payload[cursor:cursor + 2], 'big')
                event['representation_id'] = representation
                if representation in (2, 3):
                    pe = '<' if representation == 3 else '>'
                    pl = parse_parameter_list(payload, cursor + 4, end, pe, raw_ref=eref)
                    pl.update(endian=pe, raw_ref=eref)
                    event['parameter_list'] = pl
                    fields, extra = endpoint_fields(pl, role=role, vendor_id=sm['vendor_id'],
                                                    protocol_version=sm['protocol_version'], field_policy=policy if event['lifecycle']=='alive' else {**policy, 'protocol_defaults':False})
                    event['fields'], event['supporting_diagnostics'] = fields, extra
                    event['endpoint_guid'] = fields['endpoint_guid']['value'] or event['endpoint_guid']
                    if not pl['complete']:
                        diagnostics.append({'packet_index': record['packet_index'], 'reason': pl['reason']})
                else:
                    diagnostics.append({'packet_index': record['packet_index'], 'reason': 'unsupported_endpoint_representation:' + str(representation)})
            endpoint_events.append(event)
    unique = {}
    for event in activity:
        unique.setdefault((event['frame_sha256'], event['submessage_offset']), event)
    return {'schema': 'r2f.sedp-observation/v1', 'raw': raw,
            'independent_source': 'passive_rtps_wire', 'graph_or_config_field_input': False,
            'activity': list(unique.values()), 'events': endpoint_events, 'diagnostics': diagnostics,
            'capture_receipt': copy.deepcopy(artifact) if 'ready' in artifact and 'raw' in artifact else None,
            'counters': {'packet_records': len(records), 'activity_copies': len(activity),
                         'activity_unique': len(unique), 'counts_by_kind': dict(Counter(e['kind'] for e in unique.values()))},
            'capture_health': artifact.get('health', {'status': 'unknown', 'reasons': ['external_pcap_no_capture_health']}),
            'field_policy': policy,
            'field_policy_sha256': hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            'assurance': 'wire advertisement/activity, not remote vendor received state'}
