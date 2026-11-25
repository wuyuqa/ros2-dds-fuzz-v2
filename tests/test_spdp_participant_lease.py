"""Literal RTPS2.5 worked examples: offline contracts, never live execution."""
import hashlib
from pathlib import Path
import struct

from r2f.instrumentation.spdp_wire import parse_spdp_capture

PREFIX = '0102030405060708090a0b0c'
GUID = PREFIX + '000001c1'
# RTPS2.5 header, DATA little endian, inline offset16, SPDP writer, SN1,
# PL_CDR_LE, participant GUID PID0050, lease PID0002 = 3 + 1/2 sec, sentinel.
LE_DATA = bytes.fromhex(
    '52545053020501100102030405060708090a0b0c'
    '15053c000000100000000000000100c20000000001000000'
    '00030000500010000102030405060708090a0b0c000001c1'
    '02000800030000000000008001000000')


def frame(payload):
    udp = struct.pack('!HHHH', 7400, 7400, len(payload)+8, 0) + payload
    ip = bytes.fromhex('4500') + struct.pack('!H',len(udp)+20) + bytes(5) + b'\x11' + bytes(2) + bytes.fromhex('7f0000017f000001')
    return bytes(12) + b'\x08\x00' + ip + udp


def capture(tmp_path, payloads):
    data = bytes.fromhex('4d3cb2a1020004000000000000000000ffff000001000000')
    for payload in payloads:
        packet = frame(payload)
        data += struct.pack('<IIII', 1700000000, 123, len(packet), len(packet)) + packet
    path = tmp_path / 'normal-rtps.pcap'
    path.write_bytes(data)
    return {'raw': {'path':str(path),'sha256':hashlib.sha256(data).hexdigest()},
            'health':{'status':'ok','reasons':[]}}


def test_literal_lease_is_independent_and_raw_bound(tmp_path):
    result = parse_spdp_capture(capture(tmp_path, [LE_DATA]))
    assert result['status'] == 'observed'
    event, = result['observations']
    assert event['participant_guid'] == GUID
    assert event['lease']['nanoseconds'] == 3500000000
    assert event['lease']['encoding'] == 'seconds_fraction_2pow32'
    assert event['unix_ns'] == 1700000000000000123
    assert event['raw_ref']['frame_offset'] == 40
    assert event['raw_ref']['submessage_offset'] == 20
    assert event['raw_ref']['frame_sha256'] == hashlib.sha256(frame(LE_DATA)).hexdigest()
    assert event['generation'] is None
    assert event['candidate_eligible'] is False
    assert result['expiry_status'] == 'unknown'


def data_packet(parameters=None, *, endian='<', representation=None, inline=b'', flags=None,
                version=b'\x02\x05', prefix=PREFIX, kind=0x15, inline_offset=16):
    # Worked parameter values are literals, independent of the production parser.
    if parameters is None:
        parameters = bytes.fromhex('50001000'+GUID+'02000800030000000000008001000000')
    if representation is None:
        representation = b'\0\3\0\0' if endian == '<' else b'\0\2\0\0'
    if flags is None:
        flags = (1 if endian == '<' else 0) | 4 | (2 if inline else 0)
    body = b'\0\0' + struct.pack(endian+'H',inline_offset) + bytes(4) + bytes.fromhex('000100c2') + struct.pack(endian+'iI',0,1) + inline + representation + parameters
    return b'RTPS'+version+b'\x01\x10'+bytes.fromhex(prefix)+bytes([kind,flags])+struct.pack(endian+'H',len(body))+body


def test_encapsulation_controls_parameter_endian_and_inline_is_skipped(tmp_path):
    # DATA header/inline QoS LE, serialized PL BE. 3.5 sec remains 3.5 sec.
    params = bytes.fromhex('00500010'+GUID+'00020008000000038000000000010000')
    inline = bytes.fromhex('710004000000000001000000')
    result = parse_spdp_capture(capture(tmp_path, [data_packet(params, representation=b'\0\2\0\0', inline=inline)]))
    assert result['status'] == 'observed'
    event, = result['observations']
    assert event['participant_guid'] == GUID
    assert event['lease']['nanoseconds'] == 3500000000
    assert event['inline_qos']['complete']
    assert event['raw_ref']['submessage_pcap_offset'] == 102
    assert event['parameter_list']['parameters'][0]['raw_ref']['pcap_value_offset'] == 146


def test_big_endian_infinite_is_duration_not_timestamp_infinite(tmp_path):
    params = bytes.fromhex('00500010'+GUID+'000200087fffffffffffffff00010000')
    result = parse_spdp_capture(capture(tmp_path,[data_packet(params,endian='>')]))
    event, = result['observations']
    assert event['lease']['infinite'] is True
    assert event['lease']['nanoseconds'] is None


import pytest

GUID_PARAM = bytes.fromhex('50001000'+GUID)
LEASE_PARAM = bytes.fromhex('020008000300000000000080')
SENTINEL = bytes.fromhex('01000000')


@pytest.mark.parametrize('parameters,reason', [
    (GUID_PARAM+LEASE_PARAM, 'missing_PID_SENTINEL'),
    (GUID_PARAM+LEASE_PARAM[:-1], 'invalid_parameter_length_or_truncation'),
    (GUID_PARAM+SENTINEL, 'missing_participant_lease_duration'),
    (LEASE_PARAM+SENTINEL, 'missing_participant_guid'),
    (GUID_PARAM+GUID_PARAM+LEASE_PARAM+SENTINEL, 'duplicate_participant_guid'),
    (GUID_PARAM+LEASE_PARAM+LEASE_PARAM+SENTINEL, 'duplicate_participant_lease_duration'),
    (GUID_PARAM+LEASE_PARAM+bytes.fromhex('020008000400000000000000')+SENTINEL, 'conflicting_participant_lease_duration'),
    (bytes.fromhex('50001000'+'11'*12+'000001c1')+LEASE_PARAM+SENTINEL, 'participant_guid_source_mismatch'),
    (bytes.fromhex('50001000'+PREFIX+'00001003')+LEASE_PARAM+SENTINEL, 'not_participant_entity'),
    (bytes.fromhex('50000c00'+PREFIX)+LEASE_PARAM+SENTINEL, 'invalid_participant_guid_length'),
    (GUID_PARAM+bytes.fromhex('0200040003000000')+SENTINEL, 'invalid_participant_lease_duration_length'),
    (GUID_PARAM+bytes.fromhex('02000800ffffffff00000000')+SENTINEL, 'invalid_negative_duration'),
    (GUID_PARAM+bytes.fromhex('1540040000000000')+LEASE_PARAM+SENTINEL, 'unsupported_required_parameter'),
    (GUID_PARAM+bytes.fromhex('1500040002030000')+LEASE_PARAM+SENTINEL, 'protocol_version_parameter_conflict'),
    (GUID_PARAM+bytes.fromhex('16000400010f0000')+LEASE_PARAM+SENTINEL, 'vendor_id_parameter_conflict'),
])
def test_invalid_singletons_never_produce_verified_lease(tmp_path, parameters, reason):
    result = parse_spdp_capture(capture(tmp_path,[data_packet(parameters)]))
    event, = result['observations']
    assert event['status'] == result['status'] == 'unknown'
    assert reason in event['reasons']
    assert event['candidate_eligible'] is False
    assert event['parameter_list']['parameters']  # retain raw evidence


@pytest.mark.parametrize('kwargs,reason', [
    ({'representation':b'\0\1\0\0'},'unsupported_spdp_representation'),
    ({'representation':b'\0\3\0\1'},'unsupported_encapsulation_options'),
    ({'inline_offset':12},'invalid_octetsToInlineQos'),
    ({'inline_offset':200},'invalid_octetsToInlineQos'),
    ({'inline':bytes.fromhex('7100fcff00000003')},'incomplete_inline_QoS'),
    ({'inline':bytes.fromhex('710004000000000301000000')},'dispose_or_unregister_not_lease_announcement'),
    ({'flags':9},'key_only_or_no_serialized_data'),
    ({'flags':13},'key_only_or_no_serialized_data'),
    ({'kind':0x16},'DATA_FRAG_participant_reassembly_unsupported'),
    ({'version':b'\x03\x00'},'unsupported_protocol_version'),
    ({'prefix':'00'*12},'unknown_source_guid_prefix'),
])
def test_unsupported_carrier_input_is_named_unknown(tmp_path, kwargs, reason):
    result = parse_spdp_capture(capture(tmp_path,[data_packet(**kwargs)]))
    event, = result['observations']
    assert result['status'] == event['status'] == 'unknown'
    assert reason in event['reasons']


def test_unverified_legacy_fraction_stays_unknown(tmp_path):
    result = parse_spdp_capture(capture(tmp_path,[data_packet(version=b'\x02\x01')]))
    event, = result['observations']
    assert result['status'] == event['status'] == 'unknown'
    assert 'unverified_legacy_duration_encoding' in event['reasons']
    assert event['lease']['second_word'] == 2147483648


def test_capture_health_and_empty_window_never_imply_expiry(tmp_path):
    artifact = capture(tmp_path,[LE_DATA])
    artifact['health'] = {'status':'degraded','reasons':['kernel_drops']}
    result = parse_spdp_capture(artifact)
    assert result['status'] == 'unknown'
    assert 'capture_health_degraded_or_unknown' in result['reasons']
    assert result['observations'][0]['status'] == 'observed'
    result = parse_spdp_capture(capture(tmp_path,[]))
    assert result['status'] == 'unknown'
    assert 'no_participant_lease_observation_in_finite_capture' in result['reasons']
    assert result['expiry_status'] == result['cleanup_status'] == 'unknown'


def test_truncated_rtps_is_diagnostic_not_exception(tmp_path):
    result = parse_spdp_capture(capture(tmp_path,[LE_DATA[:-1]]))
    assert result['status'] == 'unknown'
    assert 'truncated_submessage' in {d['reason'] for d in result['diagnostics']}


def test_prefix_is_bounded_inode_and_hash_bound_and_can_append(tmp_path):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    artifact = capture(tmp_path,[LE_DATA])
    path = Path(artifact['raw']['path'])
    original = path.read_bytes()
    st = path.stat()
    raw = {**artifact['raw'], 'size':len(original), 'final':False,
           'file_identity':{'device':st.st_dev,'inode':st.st_ino}}
    first = parse_spdp_capture_prefix(raw, capture_health={'status':'ok'})
    with path.open('ab') as stream:
        stream.write(b'ignored suffix')
    second = parse_spdp_capture_prefix(raw, capture_health={'status':'ok'})
    assert first == second
    assert first['status'] == 'observed'
    assert first['observations'][0]['raw_ref']['final'] is False
    assert first['observations'][0]['raw_ref']['size'] == len(original)
    bad = parse_spdp_capture_prefix({**raw,'sha256':'0'*64})
    assert bad['status'] == 'unknown'
    assert 'capture_prefix_sha256_mismatch' in bad['diagnostics'][0]['reason']
    replacement = tmp_path/'other.pcap'
    replacement.write_bytes(original)
    replacement.replace(path)
    raced = parse_spdp_capture_prefix(raw)
    assert raced['status'] == 'unknown'
    assert 'inode' in raced['diagnostics'][0]['reason']


def test_final_reference_mismatch_and_oversize_are_named_unknown(tmp_path):
    from r2f.instrumentation.sedp_wire import MAX_CAPTURE_PREFIX_BYTES
    artifact = capture(tmp_path,[LE_DATA])
    result = parse_spdp_capture({**artifact,'raw':{**artifact['raw'],'sha256':'0'*64}})
    assert result['status'] == 'unknown'
    assert result['raw'] is None
    assert 'sha256_mismatch' in result['diagnostics'][0]['reason']
    with Path(artifact['raw']['path']).open('ab') as stream:
        stream.truncate(MAX_CAPTURE_PREFIX_BYTES+1)
    result = parse_spdp_capture(artifact)
    assert result['status'] == 'unknown'
    assert 'capture_size_limit' in result['diagnostics'][0]['reason']


def test_cyclone_legacy_requires_existing_hash_bound_profile(tmp_path):
    import json
    from r2f.instrumentation.sedp_fields import LEGACY
    profile = LEGACY[('0110',(2,1),'0.10.5')]
    path = tmp_path/'binary-manifest.json'
    path.write_text(json.dumps({'library_sha256':profile['library_sha256']}))
    policy = {'vendor_versions':{'0110':'0.10.5'}, 'vendor_provenance':{'0110':{
        'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}}}
    artifact = capture(tmp_path,[data_packet(version=b'\x02\x01')])
    result = parse_spdp_capture(artifact,field_policy=policy)
    assert result['observations'][0]['lease']['nanoseconds'] == 3500000000
    assert result['status'] == 'observed'
    path.write_text('{}')
    result = parse_spdp_capture(artifact,field_policy=policy)
    assert result['status'] == 'unknown'
    assert 'unverified_legacy_duration_encoding' in result['observations'][0]['reasons']


def test_passive_snapshot_integrates_existing_prefix_health_window(tmp_path):
    from r2f.instrumentation.sedp_capture import PassiveSedpObserver
    artifact = capture(tmp_path,[LE_DATA])
    path = Path(artifact['raw']['path'])
    st = path.stat()
    boundary = {'raw_prefix':{**artifact['raw'],'size':st.st_size,'final':False,
        'file_identity':{'device':st.st_dev,'inode':st.st_ino}},
        'health':{'status':'ok'},'since_ns':1700000000000000000,
        'until_ns':1700000000000001000,'monotonic_ns':77,
        'window_buffer_retained':True, 'participant_prefixes':[PREFIX]}
    # Deterministic fake at necessary carrier boundary. No start/socket/library.
    class OfflineObserver(PassiveSedpObserver):
        def __init__(self):
            pass
        def snapshot_activity(self, *, since_ns, participant_prefixes):
            return {**boundary,'since_ns':since_ns,'participant_prefixes':list(participant_prefixes)}
    observer = OfflineObserver()
    result = observer.snapshot_participant_leases(since_ns=boundary['since_ns'],participant_prefixes=[PREFIX])
    assert result['status'] == 'observed'
    assert result['observations'][0]['participant_guid'] == GUID
    assert result['snapshot_monotonic_ns'] == 77
    assert result['candidate_eligible'] is False
    assert result['ownership_status'] == 'unknown_separate_ownership_evidence_required'
    # A prefix filter is not endpoint ownership or lifecycle evidence.
    result = observer.snapshot_participant_leases(since_ns=boundary['since_ns'],participant_prefixes=['11'*12])
    assert result['status'] == 'unknown'
    assert result['observations'] == []
    result = observer.snapshot_participant_leases(since_ns=1700000000000000500,participant_prefixes=[PREFIX])
    assert result['status'] == 'unknown'
    assert result['observations'] == []
    boundary['health'] = {'status':'degraded','reasons':['filtered_target_related_rtps']}
    result = observer.snapshot_participant_leases(since_ns=boundary['since_ns'],participant_prefixes=[PREFIX])
    assert result['status'] == 'unknown'
    assert result['observations'][0]['status'] == 'observed'
    boundary['health'] = {'status':'ok'}
    boundary['window_buffer_retained'] = False
    result = observer.snapshot_participant_leases(since_ns=boundary['since_ns'],participant_prefixes=[PREFIX])
    assert result['status'] == 'unknown'
    assert 'activity_window_not_retained' in result['reasons']


def test_snapshot_requires_observation_for_each_requested_prefix_and_final_read(tmp_path):
    from r2f.instrumentation.sedp_capture import PassiveSedpObserver
    artifact = capture(tmp_path,[LE_DATA])
    class OfflineFinalObserver(PassiveSedpObserver):
        def __init__(self):
            pass
        def snapshot_activity(self, *, since_ns, participant_prefixes):
            return {'raw_prefix':{**artifact['raw'],'final':True},'health':artifact['health'],
                'since_ns':since_ns,'until_ns':1700000000000001000,'monotonic_ns':88,
                'window_buffer_retained':True,'participant_prefixes':list(participant_prefixes)}
    observer = OfflineFinalObserver()
    result = observer.snapshot_participant_leases(since_ns=0,participant_prefixes=[PREFIX,'11'*12])
    assert result['status'] == 'unknown'
    assert result['missing_participant_prefixes'] == ['11'*12]
    assert 'missing_requested_participant_lease_observation' in result['reasons']
    assert len(result['observations']) == 1
    result = observer.snapshot_participant_leases(since_ns=0,participant_prefixes=[PREFIX])
    assert result['status'] == 'observed'
    assert result['missing_participant_prefixes'] == []


def test_fine_fraction_is_exact_not_nanosecond_word(tmp_path):
    params = GUID_PARAM + bytes.fromhex('020008000000000001000000') + SENTINEL
    event, = parse_spdp_capture(capture(tmp_path,[data_packet(params)]))['observations']
    assert event['lease']['exact_seconds_numerator'] == 1
    assert event['lease']['exact_seconds_denominator'] == 4294967296
    assert event['lease']['nanoseconds'] is None


def test_effective_info_src_identity_is_used_and_bound(tmp_path):
    other = '11'*12
    info_src = bytes.fromhex('0c0000140000000002050110'+other)
    params = bytes.fromhex('50001000'+other+'000001c1') + LEASE_PARAM + SENTINEL
    packet = data_packet(params)
    packet = packet[:20]+info_src+packet[20:]
    event, = parse_spdp_capture(capture(tmp_path,[packet]))['observations']
    assert event['status'] == 'observed'
    assert event['participant_guid'] == other+'000001c1'
    assert event['source_guid_prefix'] == other
    assert event['vendor_id'] == '0110'
    assert event['protocol_version'] == [2,5]
    assert event['raw_ref']['submessage_offset'] == 44


@pytest.mark.parametrize('inline,reason', [
    (bytes.fromhex('70001000'+'11'*16+'01000000'), 'inline_key_participant_guid_conflict'),
    (bytes.fromhex('7100040000000000710004000000000001000000'), 'duplicate_status_info'),
])
def test_inline_identity_conflicts_cannot_be_promoted(tmp_path,inline,reason):
    result = parse_spdp_capture(capture(tmp_path,[data_packet(inline=inline)]))
    assert result['status'] == 'unknown'
    assert reason in result['observations'][0]['reasons']


@pytest.mark.parametrize('value', [None, False, 42, 1.5, '', 'metadata', []])
@pytest.mark.parametrize('location', ['artifact', 'raw', 'prefix'])
def test_public_parser_rejects_nonmapping_capture_metadata(location, value):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    if location == 'prefix':
        result = parse_spdp_capture_prefix(value, capture_health={'status':'ok'})
        reason = 'nonfinal_capture_prefix_required'
    else:
        result = parse_spdp_capture(value if location == 'artifact' else {'raw':value})
        reason = 'capture_artifact_mapping_required' if location == 'artifact' else 'capture_raw_mapping_required'
    assert result['status'] == 'unknown'
    assert reason in {item['reason'] for item in result['diagnostics']}
    assert result['observations'] == []
    assert result['candidate_eligible'] is False


@pytest.fixture
def producer_health():
    # Literal health from the retained domain148 producer receipt (source 4be53e9),
    # api-feasibility/spdp-observation.json; no collector or DDS is started.
    return {'filtered_rtps_completeness': 'complete_for_known_target_prefixes',
            'filtered_rtps_count': 26, 'filtered_rtps_metadata_overflow': 0,
            'filtered_rtps_target_related_count': 0,
            'kernel_drop_status': 'known_at_stop', 'reasons': [], 'status': 'ok'}


@pytest.mark.parametrize('prefix', [False, True])
@pytest.mark.parametrize('overflow', [0, False])
def test_public_parser_accepts_producer_zero_overflow_and_legacy_false(
        tmp_path, producer_health, prefix, overflow):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    artifact = capture(tmp_path, [LE_DATA])
    health = {**producer_health, 'filtered_rtps_metadata_overflow': overflow}
    if prefix:
        path = Path(artifact['raw']['path'])
        st = path.stat()
        raw = {**artifact['raw'], 'size': st.st_size, 'final': False,
               'file_identity': {'device': st.st_dev, 'inode': st.st_ino}}
        result = parse_spdp_capture_prefix(raw, capture_health=health)
    else:
        result = parse_spdp_capture({**artifact, 'health': health})
    assert result['status'] == 'observed'
    event, = result['observations']
    assert event['participant_guid'] == GUID
    assert event['lease']['nanoseconds'] == 3500000000
    assert result['capture_health'] == health
    assert type(result['capture_health']['filtered_rtps_metadata_overflow']) is type(overflow)
    assert result['candidate_eligible'] is False
    assert result['expiry_status'] == result['cleanup_status'] == 'unknown'


@pytest.mark.parametrize('prefix', [False, True])
@pytest.mark.parametrize('overflow,status', [
    (1, 'ok'), (2, 'ok'), (True, 'ok'),
    (1, 'degraded'), (True, 'degraded'), (0, 'degraded'), (False, 'degraded')])
def test_public_parser_keeps_overflow_or_degraded_health_unknown(
        tmp_path, producer_health, prefix, overflow, status):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    artifact = capture(tmp_path, [LE_DATA])
    health = {**producer_health, 'filtered_rtps_metadata_overflow': overflow,
              'status': status}
    if prefix:
        path = Path(artifact['raw']['path'])
        st = path.stat()
        raw = {**artifact['raw'], 'size': st.st_size, 'final': False,
               'file_identity': {'device': st.st_dev, 'inode': st.st_ino}}
        result = parse_spdp_capture_prefix(raw, capture_health=health)
    else:
        result = parse_spdp_capture({**artifact, 'health': health})
    assert result['status'] == 'unknown'
    assert 'capture_health_degraded_or_unknown' in result['reasons']
    assert 'invalid_capture_health' not in result['reasons']
    assert result['capture_health'] == health
    assert result['raw']['sha256'] == artifact['raw']['sha256']
    assert result['observations'][0]['participant_guid'] == GUID
    assert result['candidate_eligible'] is False
    assert result['expiry_status'] == result['cleanup_status'] == 'unknown'


@pytest.mark.parametrize('prefix', [False, True])
@pytest.mark.parametrize('health', [False, 42, 'ok', [],
    {'status':42}, {'status':[]}, {'status':'ok','reasons':None},
    {'status':'ok','reasons':'degraded'}, {'status':'ok','reasons':[42]},
    {'status':'ok','reasons':['kernel_drops']},
    {'status':'ok','filtered_rtps_count':True},
    {'status':'ok','filtered_rtps_count':float('nan')},
    {'status':'ok','filtered_rtps_target_related_count':float('inf')},
    *[{'status':'ok','filtered_rtps_metadata_overflow': value}
      for value in (-1, '0', '1', 0.0, 1.0, float('nan'), float('inf'), None, [], {})]])
def test_public_parser_rejects_malformed_health(tmp_path, prefix, health):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    artifact = capture(tmp_path, [LE_DATA])
    if prefix:
        path = Path(artifact['raw']['path'])
        st = path.stat()
        raw = {**artifact['raw'], 'size':st.st_size, 'final':False,
               'file_identity':{'device':st.st_dev,'inode':st.st_ino}}
        result = parse_spdp_capture_prefix(raw, capture_health=health)
    else:
        result = parse_spdp_capture({**artifact, 'health':health})
    assert result['status'] == 'unknown'
    assert 'invalid_capture_health' in result['reasons']
    assert result['observations'] == []


def test_malformed_health_with_invalid_reference_is_named_unknown():
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    result = parse_spdp_capture_prefix(None, capture_health=42)
    assert result['status'] == 'unknown'
    assert 'invalid_capture_health' in result['reasons']


@pytest.mark.parametrize('prefix', [False, True])
@pytest.mark.parametrize('version', [b'\x02\x01', b'\x02\x05'])
@pytest.mark.parametrize('policy', [False, 42, 'policy', [],
    {'vendor_versions':None}, {'vendor_versions':[]},
    {'vendor_versions':{'0110':42}},
    {'vendor_versions':{'0110':'0.10.5'},'vendor_provenance':[]},
    {'vendor_provenance':{'0110':[]}},
    {'vendor_provenance':{'0110':{'path':[],'sha256':'0'*64}}},
    {'vendor_provenance':{'0110':{'path':'/unread','sha256':False}}}])
def test_public_parser_rejects_malformed_policy(tmp_path, prefix, version, policy):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    artifact = capture(tmp_path, [data_packet(version=version)])
    if prefix:
        path = Path(artifact['raw']['path'])
        st = path.stat()
        raw = {**artifact['raw'], 'size':st.st_size, 'final':False,
               'file_identity':{'device':st.st_dev,'inode':st.st_ino}}
        result = parse_spdp_capture_prefix(raw, capture_health=artifact['health'], field_policy=policy)
    else:
        result = parse_spdp_capture(artifact, field_policy=policy)
    assert result['status'] == 'unknown'
    assert 'invalid_field_policy' in result['reasons']
    assert result['observations'] == []


@pytest.mark.parametrize('prefix', [False, True])
@pytest.mark.parametrize('key,value', [
    ('path', None), ('path', []), ('path', False), ('path', ''),
    ('sha256', None), ('sha256', []), ('sha256', True),
    ('final', None), ('final', 0), ('final', 1),
    ('size', True), ('size', 24.0), ('size', float('nan')),
    ('size', float('inf')), ('size', -1),
    ('file_identity', None), ('file_identity', []),
    ('file_identity', {'device':False,'inode':1}),
    ('file_identity', {'device':0,'inode':float('nan')}),
    ('file_identity', {'device':float('inf'),'inode':1})])
def test_public_parser_rejects_malformed_reference_fields(tmp_path, prefix, key, value):
    from r2f.instrumentation.spdp_wire import parse_spdp_capture_prefix
    artifact = capture(tmp_path, [LE_DATA])
    raw = artifact['raw']
    if prefix:
        st = Path(raw['path']).stat()
        raw = {**raw, 'size':st.st_size, 'final':False,
               'file_identity':{'device':st.st_dev,'inode':st.st_ino}}
        result = parse_spdp_capture_prefix({**raw,key:value}, capture_health=artifact['health'])
    else:
        result = parse_spdp_capture({**artifact, 'raw':{**raw,key:value}})
    assert result['status'] == 'unknown'
    assert result['observations'] == []
    assert result['diagnostics']


def test_keyed_participant_dispose_is_preserved_without_fabricating_lease(tmp_path):
    # Captured shape worked literally: KeyFlag+InlineQos, DISPOSE|UNREGISTER,
    # PL_CDR participant key, no duration. This is not a lease announcement.
    params = bytes.fromhex('50001000'+GUID+'01000000')
    dispose = data_packet(params, flags=11,
        inline=bytes.fromhex('710004000000000301000000'))
    result = parse_spdp_capture(capture(tmp_path, [LE_DATA, dispose]))
    assert result['status'] == 'observed'
    alive, dead = result['observations']
    assert alive['lease']['nanoseconds'] == 3500000000
    assert dead['status'] == 'not_applicable'
    assert dead['event_kind'] == 'participant_dispose_or_unregister'
    assert dead['participant_guid'] == GUID
    assert dead['lease'] is None
    assert dead['lifecycle_status_info'] == 3
    assert dead['candidate_eligible'] is False
    assert dead['raw_ref']['packet_index'] == 1
    # Silence or lifecycle-only captures cannot satisfy a lease observation.
    assert parse_spdp_capture(capture(tmp_path, [dispose]))['status'] == 'unknown'
    for bad in (data_packet(params, flags=11),
                data_packet(params, flags=11, inline=bytes.fromhex('710004000000008001000000')),
                data_packet(params, flags=11, prefix='11'*12,
                            inline=bytes.fromhex('710004000000000301000000'))):
        assert parse_spdp_capture(capture(tmp_path, [LE_DATA, bad]))['status'] == 'unknown'
