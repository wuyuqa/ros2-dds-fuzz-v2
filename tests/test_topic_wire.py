"""Offline DCPSTopic worked examples through the public capture seam only."""
import copy
import hashlib
from pathlib import Path
import struct

import pytest

from r2f.instrumentation.topic_wire import parse_topic_capture

# DDSI-RTPS 2.5: DCPSTopic writer 000002c2; PL_CDR_LE name='demo',
# type='Msg', HISTORY=(KEEP_LAST=0, depth=7). Not a captured DDS run.
HEADER = bytes.fromhex('52545053020501100102030405060708090a0b0c')
TOPIC = bytes.fromhex('05000c000500000064656d6f00000000')
TYPE = bytes.fromhex('07000800040000004d736700')
HISTORY = bytes.fromhex('400008000000000007000000')
SENTINEL = bytes.fromhex('01000000')


def packet(parameters=TOPIC+TYPE+HISTORY+SENTINEL, *, endian='<',
           representation=b'\0\3\0\0', inline=b'', flags=None, kind=0x15,
           writer='000002c2', inline_offset=16):
    if flags is None:
        flags = (endian == '<') | 4 | (2 if inline else 0)
    body = (b'\0\0' + struct.pack(endian+'H', inline_offset) + bytes(4)
            + bytes.fromhex(writer) + struct.pack(endian+'iI', 0, 1)
            + inline + representation + parameters)
    return HEADER + bytes([kind, flags]) + struct.pack(endian+'H', len(body)) + body


def frame(payload):
    udp = struct.pack('!HHHH', 7400, 7400, len(payload)+8, 0) + payload
    ip = (bytes.fromhex('4500') + struct.pack('!H', len(udp)+20) + bytes(5)
          + b'\x11' + bytes(2) + bytes.fromhex('7f0000017f000001'))
    return bytes(12) + b'\x08\x00' + ip + udp


def capture(tmp_path, payloads):
    data = bytes.fromhex('4d3cb2a1020004000000000000000000ffff000001000000')
    for payload in payloads:
        wire = frame(payload)
        data += struct.pack('<IIII', 1700000000, 123, len(wire), len(wire)) + wire
    path = tmp_path / 'topic.pcap'
    path.write_bytes(data)
    return {'raw': {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()},
            'health': {'status': 'ok', 'reasons': [], 'filtered_rtps_metadata_overflow': 0}}


def test_topic_fields_have_independent_absolute_wire_provenance(tmp_path):
    artifact = capture(tmp_path, [packet()])
    before = copy.deepcopy(artifact), Path(artifact['raw']['path']).read_bytes()
    result = parse_topic_capture(artifact)
    assert result['status'] == 'observed'
    event, = result['observations']
    assert {k: v['value'] for k, v in event['fields'].items()} == {
        'topic': 'demo', 'type': 'Msg', 'history': 'KEEP_LAST', 'depth': 7}
    assert event['builtin_writer_id'] == '000002c2'
    assert event['generation'] is None
    assert event['topic_key'] is None
    assert 'participant_guid' not in event and 'endpoint_guid' not in event
    ref = event['fields']['topic']['raw_refs'][0]
    assert ref['path'] == artifact['raw']['path']
    assert ref['sha256'] == artifact['raw']['sha256']
    assert ref['frame_sha256'] == hashlib.sha256(frame(packet())).hexdigest()
    assert ref['frame_offset'] == 40
    assert ref['submessage_pcap_offset'] == 102
    assert ref['pcap_parameter_offset'] == 130
    assert ref['pcap_value_offset'] == 134
    assert ref['parameter_offset_in_frame'] == 90
    assert event['unix_ns'] == 1700000000000000123
    assert event['candidate_eligible'] is False and result['candidate_eligible'] is False
    assert before == (artifact, Path(artifact['raw']['path']).read_bytes())


@pytest.mark.parametrize('endian', ['<', '>'])
@pytest.mark.parametrize('representation', [b'\0\3\0\0', b'\0\2\0\0'])
def test_encapsulation_endian_is_independent_of_data_and_inline_qos(tmp_path, endian, representation):
    params = (TOPIC+TYPE+HISTORY+SENTINEL if representation[1] == 3 else bytes.fromhex(
        '0005000c0000000564656d6f00000000'
        '00070008000000044d73670000400008000000000000000700010000'))
    inline = (bytes.fromhex('710004000000000001000000') if endian == '<'
              else bytes.fromhex('007100040000000000010000'))
    result = parse_topic_capture(capture(tmp_path, [packet(params, endian=endian,
        representation=representation, inline=inline)]))
    assert result['status'] == 'observed'
    event, = result['observations']
    assert event['fields']['topic']['value'] == 'demo'
    assert event['fields']['depth']['value'] == 7
    assert event['inline_qos']['complete'] is True
    assert event['fields']['topic']['raw_refs'][0]['pcap_parameter_offset'] == 142


@pytest.mark.parametrize('parameters,field,reason', [
    (TYPE+HISTORY+SENTINEL, 'topic', 'absent_parameter'),
    (TOPIC+HISTORY+SENTINEL, 'type', 'absent_parameter'),
    (TOPIC+TYPE+SENTINEL, 'history', 'absent_parameter'),
    (TOPIC+TYPE+SENTINEL, 'depth', 'absent_parameter'),
    (TOPIC+TOPIC+TYPE+HISTORY+SENTINEL, 'topic', 'duplicate_singleton_parameter'),
    (TOPIC+TYPE+HISTORY+HISTORY+SENTINEL, 'history', 'duplicate_singleton_parameter'),
    (TOPIC+TYPE+HISTORY+bytes.fromhex('400008000000000008000000')+SENTINEL,
     'depth', 'duplicate_singleton_parameter'),
    (TOPIC+TYPE+HISTORY, 'topic', 'incomplete_parameter_list'),
    (TOPIC+TYPE+HISTORY[:-1], 'depth', 'incomplete_parameter_list'),
])
def test_missing_ambiguous_or_unclosed_fields_remain_unknown(tmp_path, parameters, field, reason):
    result = parse_topic_capture(capture(tmp_path, [packet(parameters)]),
        field_policy={'protocol_defaults': True, 'topic': 'configured', 'depth': 1})
    assert result['status'] == 'unknown'
    event, = result['observations']
    assert event['status'] == 'unknown'
    assert event['fields'][field]['status'] == 'unknown'
    assert event['fields'][field]['value'] is None
    assert event['fields'][field]['reason'] == reason


@pytest.mark.parametrize('history,kind,depth_status,depth', [
    ('0000000001000000', 'KEEP_LAST', 'observed', 1),
    ('00000000ffffff7f', 'KEEP_LAST', 'observed', 2147483647),
    ('0100000007000000', 'KEEP_ALL', 'not_applicable', None),
    ('01000000ffffffff', 'KEEP_ALL', 'not_applicable', None),
    ('0000000000000000', None, 'unknown', None),
    ('00000000ffffffff', None, 'unknown', None),
    ('0200000007000000', None, 'unknown', None),
    ('ffffffff07000000', None, 'unknown', None),
])
def test_history_is_topic_semantics_and_keep_all_has_no_effective_depth(tmp_path, history, kind, depth_status, depth):
    parameters = TOPIC+TYPE+bytes.fromhex('40000800'+history)+SENTINEL
    result = parse_topic_capture(capture(tmp_path, [packet(parameters)]))
    fields = result['observations'][0]['fields']
    assert fields['history']['value'] == kind
    assert fields['depth']['status'] == depth_status
    assert fields['depth']['value'] == depth
    assert result['status'] == ('unknown' if kind is None else 'observed')
    if kind == 'KEEP_ALL':
        assert fields['depth']['reason'] == 'ignored_for_KEEP_ALL'
        assert fields['depth']['wire_value'] == (7 if history.endswith('07000000') else -1)


@pytest.mark.parametrize('pid,value,field,reason', [
    (5, b'', 'topic', 'invalid_CDR_string'),
    (5, bytes.fromhex('0500000064656d6f'), 'topic', 'invalid_CDR_string'),
    (5, bytes.fromhex('02000000ff000000'), 'topic', 'invalid_CDR_string'),
    (5, bytes.fromhex('0300000061000000'), 'topic', 'invalid_CDR_string'),
    (7, bytes.fromhex('ffffffff4d736700'), 'type', 'invalid_CDR_string'),
    (0x40, bytes(4), 'history', 'invalid_topic_history_length'),
    (0x40, bytes(12), 'depth', 'invalid_topic_history_length'),
])
def test_malformed_parameter_value_is_named_unknown(tmp_path, pid, value, field, reason):
    parameters = (b'' if pid == 5 else TOPIC) + (b'' if pid == 7 else TYPE)
    parameters += b'' if pid == 0x40 else HISTORY
    parameters += struct.pack('<HH', pid, len(value))+value+SENTINEL
    result = parse_topic_capture(capture(tmp_path, [packet(parameters)]))
    assert result['status'] == 'unknown'
    decoded = result['observations'][0]['fields'][field]
    assert decoded['status'] == 'unknown' and decoded['value'] is None
    assert decoded['reason'] == reason


@pytest.mark.parametrize('kwargs,reason', [
    ({'kind': 0x16}, 'DATA_FRAG_topic_reassembly_unsupported'),
    ({'flags': 9}, 'key_only_or_no_serialized_data'),
    ({'flags': 1}, 'key_only_or_no_serialized_data'),
    ({'flags': 13}, 'key_only_or_no_serialized_data'),
    ({'flags': 0x15}, 'unsupported_DATA_flags'),
    ({'inline_offset': 12}, 'invalid_octetsToInlineQos'),
    ({'inline_offset': 18}, 'invalid_octetsToInlineQos'),
    ({'inline_offset': 4096}, 'invalid_octetsToInlineQos'),
    ({'representation': b'\0\1\0\0'}, 'unsupported_topic_representation'),
    ({'representation': b'\0\3\0\1'}, 'unsupported_encapsulation_options'),
    ({'inline': bytes.fromhex('710004000000000101000000')}, 'dispose_or_unregister_not_topic_announcement'),
    ({'inline': bytes.fromhex('710004000000000201000000')}, 'dispose_or_unregister_not_topic_announcement'),
    ({'inline': bytes.fromhex('7100000001000000')}, 'invalid_status_info_length'),
    ({'inline': bytes.fromhex('7100040000000000710004000000000001000000')}, 'duplicate_inline_parameter'),
    ({'inline': bytes.fromhex('7100040000000000'), 'parameters': b''}, 'incomplete_inline_QoS'),
])
def test_nonannouncement_data_cannot_supply_topic_facts(tmp_path, kwargs, reason):
    result = parse_topic_capture(capture(tmp_path, [packet(**kwargs)]))
    assert result['status'] == 'unknown'
    event, = result['observations']
    assert event['status'] == 'unknown' and reason in event['reasons']
    assert all(f['status'] == 'unknown' for f in event['fields'].values())


@pytest.mark.parametrize('change,reason', [
    ({'sha256': '0'*64}, 'capture_prefix_sha256_mismatch'),
    ({'sha256': 'wrong'}, 'invalid_capture_sha256'),
    ({'path': 'relative.pcap'}, 'absolute_capture_path_required'),
    ({'size': 24}, 'capture_size_mismatch'),
    ({'size': True}, 'invalid_capture_size'),
    ({'file_identity': {'device': 0, 'inode': 1}}, 'capture_file_identity_mismatch'),
    ({'file_identity': {'device': 0, 'inode': True}}, 'invalid_capture_file_identity'),
    ({'final': False}, 'capture_prefix_requires_prefix_parser'),
    ({'final': 'true'}, 'invalid_capture_final'),
])
def test_final_reference_mismatch_is_unknown_without_repairing_input(tmp_path, change, reason):
    artifact = capture(tmp_path, [packet()])
    artifact['raw'].update(change)
    before = copy.deepcopy(artifact), (tmp_path/'topic.pcap').read_bytes()
    result = parse_topic_capture(artifact)
    assert result['status'] == 'unknown'
    assert result['observations'] == []
    assert reason in [d['reason'] for d in result['diagnostics']]
    assert before == (artifact, (tmp_path/'topic.pcap').read_bytes())


@pytest.mark.parametrize('artifact', [None, [], {}, {'raw': []}, {'raw': {'path': '/nonexistent', 'sha256': '0'*64}}])
def test_unreadable_or_malformed_artifact_is_named_unknown(tmp_path, artifact):
    result = parse_topic_capture(artifact)
    assert result['status'] == 'unknown'
    assert result['observations'] == [] and result['diagnostics']


@pytest.mark.parametrize('health', [
    None, {'status': 'unknown'}, {'status': 'degraded', 'reasons': ['kernel_drop']},
    {'status': 'ok', 'filtered_rtps_metadata_overflow': 1},
    {'status': 'ok', 'filtered_rtps_metadata_overflow': True},
    [], {'status': True}, {'status': 'ok', 'reasons': ['contradiction']},
    {'status': 'ok', 'filtered_rtps_metadata_overflow': -1},
    {'status': 'ok', 'filtered_rtps_metadata_overflow': '0'},
])
def test_health_limits_aggregate_without_promoting_finite_observation(tmp_path, health):
    artifact = capture(tmp_path, [packet()])
    artifact['health'] = health
    result = parse_topic_capture(artifact)
    assert result['status'] == 'unknown'
    assert result['capture_health'] == health
    assert result['candidate_eligible'] is False
    if health is None or (isinstance(health, dict) and (health.get('status') in ('unknown', 'degraded') or
            type(health.get('filtered_rtps_metadata_overflow')) in (int, bool) and health.get('filtered_rtps_metadata_overflow', 0) > 0)):
        assert result['observations'][0]['status'] == 'observed'
        assert 'capture_health_degraded_or_unknown' in result['reasons']
    else:
        assert result['observations'] == []
        assert 'invalid_capture_health' in result['reasons']


@pytest.mark.parametrize('payloads', [[], [packet(writer='000003c2')], [packet(writer='000100c2')]])
def test_finite_capture_without_topic_is_not_topic_absence(tmp_path, payloads):
    result = parse_topic_capture(capture(tmp_path, payloads))
    assert result['status'] == 'unknown' and result['observations'] == []
    assert 'no_topic_observation_in_finite_capture' in result['reasons']
    assert result['absence_status'] == 'unknown'


@pytest.mark.parametrize('policy', [[], {'vendor_versions': []}, {'vendor_provenance': {'0110': {'path': 'x'}}}])
def test_malformed_field_policy_cannot_supply_facts(tmp_path, policy):
    result = parse_topic_capture(capture(tmp_path, [packet()]), field_policy=policy)
    assert result['status'] == 'unknown' and result['observations'] == []
    assert 'invalid_field_policy' in result['reasons']


@pytest.mark.parametrize('limitation', [
    {'filtered_rtps_target_related_count': 1},
    {'filtered_rtps_completeness': 'incomplete'},
    {'kernel_drop_status': 'pending_stop'},
])
def test_explicit_capture_limitations_cannot_be_masked_by_ok_status(tmp_path, limitation):
    artifact = capture(tmp_path, [packet()])
    artifact['health'].update(limitation)
    result = parse_topic_capture(artifact)
    assert result['status'] == 'unknown'
    assert 'capture_health_degraded_or_unknown' in result['reasons']
    assert result['capture_health'] == artifact['health']
    assert result['observations'][0]['status'] == 'observed'
    assert result['candidate_eligible'] is False


def test_literal_pcap_worked_example_has_exact_offsets_and_hashes(tmp_path):
    # Independent literal, not packet()/frame()/capture() output. Layout in bytes:
    # pcap [0,24), record [24,40), Ethernet [40,54), IPv4 [54,74),
    # UDP [74,82), RTPS header [82,102), DATA header+body [102,126),
    # PL_CDR_LE [126,130), topic [130,146), type [146,158),
    # HISTORY(kind=int32(0), depth=int32(7)) [158,170), sentinel [170,174).
    wire = bytes.fromhex(
        '4d3cb2a1020004000000000000000000ffff000001000000'
        '00f153657b0000008600000086000000'
        '0000000000000000000000000800'
        '4500007800000000001100007f0000017f000001'
        '1ce81ce800640000'
        '52545053020501100102030405060708090a0b0c'
        '150544000000100000000000000002c2000000000100000000030000'
        '05000c000500000064656d6f00000000'
        '07000800040000004d736700'
        '40000800000000000700000001000000')
    path = tmp_path / 'literal.pcap'
    path.write_bytes(wire)
    digest = '7d205f59154be3bdb6c5f35edbb7d62e8262e75604f4d9e680321aa4d4697d39'
    artifact = {'raw': {'path': str(path), 'sha256': digest, 'size': 174, 'final': True},
                'health': {'status': 'ok', 'kernel_drop_status': 'known_at_stop',
                           'filtered_rtps_completeness': 'complete_for_known_target_prefixes'}}
    result = parse_topic_capture(artifact)
    assert result['status'] == 'observed'
    event, = result['observations']
    assert {name: field['value'] for name, field in event['fields'].items()} == {
        'topic': 'demo', 'type': 'Msg', 'history': 'KEEP_LAST', 'depth': 7}
    for name, parameter, value, length in (
        ('topic', 130, 134, 12), ('type', 146, 150, 8),
        ('history', 158, 162, 8), ('depth', 158, 162, 8),
    ):
        ref, = event['fields'][name]['raw_refs']
        assert ref['path'] == str(path) and ref['sha256'] == digest
        assert ref['frame_sha256'] == '283dea247ef63db309c5e009d7fb254407ebb840c864897761141d5af5d8fb70'
        assert (ref['record_offset'], ref['frame_offset'], ref['payload_pcap_offset']) == (24, 40, 82)
        assert (ref['submessage_offset'], ref['submessage_end'], ref['submessage_pcap_offset']) == (20, 92, 102)
        assert (ref['pcap_parameter_offset'], ref['pcap_value_offset'], ref['value_length']) == (parameter, value, length)
        assert ref['parameter_offset_in_frame'] == parameter - 40
    assert event['parameter_list']['sentinel_offset'] == 88
    assert event['source_guid_prefix'] == '0102030405060708090a0b0c'
    assert event['topic_key'] is None and event['generation'] is None
    assert result['absence_status'] == 'unknown' and result['candidate_eligible'] is False


def test_opaque_key_bytes_do_not_become_topic_guid_or_generation(tmp_path):
    key_hash = bytes.fromhex('70001000') + bytes(range(16))
    guid_parameter = bytes.fromhex('5a001000') + bytes(range(16))
    result = parse_topic_capture(capture(tmp_path, [packet(
        TOPIC+TYPE+HISTORY+guid_parameter+SENTINEL, inline=key_hash+SENTINEL)]))
    assert result['status'] == 'observed'
    event, = result['observations']
    assert event['topic_key'] is None and event['generation'] is None
    assert 'endpoint_guid' not in event and 'participant_guid' not in event
    assert event['inline_qos']['parameters'][0]['value_hex'] == '000102030405060708090a0b0c0d0e0f'


@pytest.mark.parametrize('fragment_bits', [0x2000, 0x0001])
def test_ipv4_fragments_cannot_supply_topic_fields(tmp_path, fragment_bits):
    artifact = capture(tmp_path, [packet()])
    path = Path(artifact['raw']['path'])
    data = bytearray(path.read_bytes())
    struct.pack_into('!H', data, 60, fragment_bits)
    path.write_bytes(data)
    artifact['raw']['sha256'] = hashlib.sha256(data).hexdigest()
    result = parse_topic_capture(artifact)
    assert result['status'] == 'unknown' and result['observations'] == []
    assert 'ipv4_fragment_unsupported' in [d['reason'] for d in result['diagnostics']]


def test_symlink_capture_is_not_followed(tmp_path):
    artifact = capture(tmp_path, [packet()])
    link = tmp_path / 'link.pcap'
    link.symlink_to(artifact['raw']['path'])
    artifact['raw']['path'] = str(link)
    result = parse_topic_capture(artifact)
    assert result['status'] == 'unknown' and result['observations'] == []
    assert result['diagnostics'][0]['reason'] == 'capture_prefix_path_or_inode_changed'


def test_capture_larger_than_64_mib_is_rejected_without_decoding(tmp_path):
    path = tmp_path / 'oversize.pcap'
    with path.open('wb') as stream:
        stream.truncate(64 * 1024 * 1024 + 1)
    result = parse_topic_capture({'raw': {'path': str(path), 'sha256': '0'*64},
                                  'health': {'status': 'ok'}})
    assert result['status'] == 'unknown' and result['observations'] == []
    assert result['diagnostics'][0]['reason'] == 'invalid_capture_prefix_size'


def test_repeated_advertisements_keep_distinct_absolute_record_references(tmp_path):
    result = parse_topic_capture(capture(tmp_path, [packet(), packet()]))
    assert result['status'] == 'observed'
    first, second = result['observations']
    assert first['raw_ref']['packet_index'] == 0 and second['raw_ref']['packet_index'] == 1
    assert first['fields']['topic']['raw_refs'][0]['pcap_parameter_offset'] == 130
    assert second['fields']['topic']['raw_refs'][0]['pcap_parameter_offset'] == 280
    assert first['raw_ref']['frame_sha256'] == second['raw_ref']['frame_sha256']
    assert first['generation'] is None and second['generation'] is None


@pytest.mark.parametrize('length', [0, 4, 12, 20])
def test_malformed_inline_key_hash_does_not_promote_topic_announcement(tmp_path, length):
    inline = struct.pack('<HH', 0x70, length) + bytes(length) + SENTINEL
    result = parse_topic_capture(capture(tmp_path, [packet(inline=inline)]))
    assert result['status'] == 'unknown'
    event, = result['observations']
    assert 'invalid_key_hash_length' in event['reasons']
    assert all(f['status'] == 'unknown' for f in event['fields'].values())
