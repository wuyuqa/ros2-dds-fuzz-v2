"""Small passive RTPS framing helpers shared by live capture and offline parsing.

These helpers never create DDS entities or transmit packets. Unsupported framing
is explicit; absence of parsed activity cannot establish endpoint absence.
"""
from __future__ import annotations
import hashlib
import struct

# OMG DDSI-RTPS 2.5, section 9.3.1.3, Table 9.2; also installed CycloneDDS
# dds/ddsi/q_rtps.h NN_ENTITYID_SEDP_BUILTIN_TOPIC_{WRITER,READER} (0x2c2/0x2c7).
# DCPSTopic discovery is distinct from publication/subscription endpoint SEDP.
BUILTIN_WRITERS = {'000100c2': 'SPDP', '000002c2': 'DCPSTopic',
                   '000003c2': 'SEDP', '000004c2': 'SEDP'}


def udp_frame(frame):
    """Decode Ethernet IPv4/IPv6 UDP; return (metadata, diagnostic)."""
    if len(frame) < 14:
        return None, 'truncated_ethernet'
    kind = frame[12:14]
    if kind == b'\x08\x00':
        if len(frame) < 34:
            return None, 'truncated_ipv4'
        ihl = (frame[14] & 15) * 4
        total = struct.unpack_from('!H', frame, 16)[0]
        if ihl < 20 or total < ihl or len(frame) < 14 + total:
            return None, 'truncated_or_invalid_ipv4'
        if struct.unpack_from('!H', frame, 20)[0] & 0x3fff:
            return None, 'ipv4_fragment_unsupported'
        if frame[23] != 17:
            return None, None
        offset, end = 14 + ihl, 14 + total
        addresses = {'source_address': '.'.join(map(str, frame[26:30])),
                     'destination_address': '.'.join(map(str, frame[30:34])), 'ip_version': 4}
    elif kind == b'\x86\xdd':
        if len(frame) < 54:
            return None, 'truncated_ipv6'
        end = 54 + struct.unpack_from('!H', frame, 18)[0]
        if len(frame) < end:
            return None, 'truncated_ipv6'
        if frame[20] != 17:
            return None, 'ipv6_extension_or_non_udp_unsupported'
        offset, addresses = 54, {'ip_version': 6}
    else:
        return None, None
    if offset + 8 > end:
        return None, 'truncated_udp'
    source, destination, length = struct.unpack_from('!HHH', frame, offset)
    if length < 8 or offset + length > end:
        return None, 'truncated_or_invalid_udp'
    payload_offset = offset + 8
    return {**addresses, 'source_port': source, 'destination_port': destination,
            'payload_offset': payload_offset, 'payload': frame[payload_offset:offset + length]}, None


def rtps_submessages(payload):
    """Return framed submessages with effective INFO_SRC/INFO_DST identity."""
    result, diagnostics = [], []
    if len(payload) < 20 or payload[:4] != b'RTPS':
        return result, ['not_rtps_or_truncated_header']
    source, destination = payload[8:20].hex(), None
    vendor, version = payload[6:8].hex(), list(payload[4:6])
    cursor = 20
    while cursor < len(payload):
        if cursor + 4 > len(payload):
            diagnostics.append('truncated_submessage_header')
            break
        kind, flags = payload[cursor:cursor + 2]
        endian = '<' if flags & 1 else '>'
        length = struct.unpack_from(endian + 'H', payload, cursor + 2)[0]
        end = cursor + 4 + length if length else len(payload)
        if end > len(payload):
            diagnostics.append('truncated_submessage')
            break
        if kind == 0x0c:
            if end - cursor < 24:
                diagnostics.append('truncated_INFO_SRC')
            else:
                version = list(payload[cursor + 8:cursor + 10])
                vendor = payload[cursor + 10:cursor + 12].hex()
                source = payload[cursor + 12:cursor + 24].hex()
        elif kind == 0x0e:
            if end - cursor < 16:
                diagnostics.append('truncated_INFO_DST')
            else:
                destination = payload[cursor + 4:cursor + 16].hex()
        result.append({'kind': kind, 'flags': flags, 'endian': endian,
                       'offset': cursor, 'end': end, 'source_guid_prefix': source,
                       'destination_guid_prefix': destination,
                       'vendor_id': vendor, 'protocol_version': version})
        cursor = end
    return result, diagnostics


def discovery_activity(frame):
    """Observe discovery builtin DATA/control traffic; no QoS interpretation."""
    udp, diagnostic = udp_frame(frame)
    if udp is None:
        return [], [diagnostic] if diagnostic else []
    payload = udp['payload']
    if payload[:4] != b'RTPS':
        return [], []
    messages, diagnostics = rtps_submessages(payload)
    events = []
    for sm in messages:
        kind, cursor, end = sm['kind'], sm['offset'], sm['end']
        if kind in (0x15, 0x16):
            if end - cursor < 24:
                diagnostics.append('truncated_DATA')
                continue
            reader = payload[cursor + 8:cursor + 12].hex()
            writer = payload[cursor + 12:cursor + 16].hex()
            sn_offset, suffix = cursor + 16, ('DATA' if kind == 0x15 else 'DATA_FRAG')
        elif kind in (0x06, 0x07, 0x08, 0x12, 0x13):
            minimum = {0x06: 28, 0x07: 32, 0x08: 32, 0x12: 32, 0x13: 28}[kind]
            if end - cursor < minimum:
                diagnostics.append('truncated_discovery_control')
                continue
            reader = payload[cursor + 4:cursor + 8].hex()
            writer = payload[cursor + 8:cursor + 12].hex()
            sn_offset = cursor + 12
            suffix = {0x06: 'ACKNACK', 0x07: 'HEARTBEAT', 0x08: 'GAP',
                      0x12: 'NACK_FRAG', 0x13: 'HEARTBEAT_FRAG'}[kind]
        else:
            continue
        family = BUILTIN_WRITERS.get(writer)
        if family is None:
            continue
        hi, lo = struct.unpack_from(sm['endian'] + 'iI', payload, sn_offset)
        events.append({**{key: sm[key] for key in ('source_guid_prefix', 'destination_guid_prefix',
                                                  'vendor_id', 'protocol_version')},
                       'kind': family + '_' + suffix, 'builtin_writer_id': writer,
                       'builtin_reader_id': reader, 'sequence_number': (hi << 32) + lo,
                       'submessage_offset': udp['payload_offset'] + cursor,
                       'source_port': udp['source_port'], 'destination_port': udp['destination_port'],
                       'frame_sha256': hashlib.sha256(frame).hexdigest()})
    return events, diagnostics
