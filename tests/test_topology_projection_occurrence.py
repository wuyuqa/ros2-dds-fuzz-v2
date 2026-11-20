"""Authored offline occurrence fixtures; no ROS/native load or SUT verdicts."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.topology_state_projection import _Projection, project_topology_state
from r2f.core.evidence import canonical_hash
from test_topology_publish_occurrence import prepared
from test_topology_state_projection import QOS, RawFixture


LIVE = Path('/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/'
            's1-fastdds-peer-runtime-20261120/live-v3/original')
LIVE_SHA256 = 'a17f7ecd348bf98a7b096e3e1739f305ea19909723b81e2341c16a4d926712c0'


@pytest.fixture(autouse=True)
def no_native(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('offline projection must not load native/ROS')
    monkeypatch.setattr('ctypes.CDLL', forbidden)
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare', forbidden)


def native_fixture(tmp_path):
    source, _ = prepared()
    program = RawFixture(readers=2).program
    rows = []
    def add(event, clock, **fields):
        row = dict(event=event, monotonic_ns=clock, run_id='offline-occurrence',
                   backend='offline_fake', fixture=True, **fields)
        rows.append(row)
        return row
    add('input_binding', 1, receipt={'program_sha256': canonical_hash(program)})
    add('topology_backend_prepared', 2, real_ros=False)
    add('topology_network_phase', 3, network_phase='normal', recovery_cycle=0,
                  observed=True, implementation='offline_neutral', audit_neutral=True)
    add('topology_network_observation', 4, network_phase='normal', recovery_cycle=0,
        source='carrier_audit', carrier_ref=2)
    before = add('topology_state_boundary', 5, observation_complete=True,
                 lifecycle_complete=True, window_complete=True)
    rows.extend(source[:3])
    tokens = {e['logical_id']: e for e in source[:3]}
    for token in tokens.values():
        identity = RawFixture.identity(token)
        cid = 'offline-snapshot-' + token['logical_id']
        add('worker_command_requested', 102, **identity, command='snapshot', command_id=cid)
        peers = [t['endpoint_gid'] for t in tokens.values()
                 if (t['logical_id'] == 'w0') != (token['logical_id'] == 'w0')]
        add('match_snapshot', 102, **identity, endpoint_gid=token['endpoint_gid'],
            command_id=cid, matched_count=len(peers), matched_peer_gids=peers,
            peer_gids_complete=True, peer_proof_source='dds_matched_endpoint_api')
        def graph(writer):
            return [dict(endpoint_gid=t['endpoint_gid'], topic_type=t['message_type'], qos=deepcopy(QOS))
                    for t in tokens.values() if (t['logical_id'] == 'w0') == writer]
        add('graph_snapshot', 102, **identity, command_id=cid,
            publishers=graph(True), subscriptions=graph(False))
        add('command_done', 102, **identity, command_id=cid)
    opened = add('topology_delivery_window_open', 102, writer='w0', reader='r0',
                 writer_generation=1, reader_generation=1)
    rows.extend(source[3:])
    closed = add('topology_delivery_window_closed', 130, writer='w0', reader='r0',
                 writer_generation=1, reader_generation=1, window_complete=True,
                 sentinel_sequence=4)
    after = add('topology_state_boundary', 131, observation_complete=True,
                lifecycle_complete=True, window_complete=True)
    for i, row in enumerate(rows):
        row.update(event_index=i, fixture=True, backend='offline_fake')
    closed['open_ref'] = opened['event_index']
    path = tmp_path / 'offline-native.jsonl'
    return program, rows, tokens, before, after, opened, closed, path


def projector(fixture):
    program, rows, _, _, _, _, _, path = fixture
    content = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in rows).encode()
    path.write_bytes(content)
    return _Projection(program, str(path), hashlib.sha256(content).hexdigest(), rows,
                       {'input_ref': {'event_index': 0}})


def deliver(fixture):
    _, _, tokens, _, _, _, closed, _ = fixture
    return projector(fixture).delivery(tokens['w0'], tokens['r0'], tokens, closed)


def test_repeated_complete_string_maps_three_independent_occurrences(tmp_path):
    fixture = native_fixture(tmp_path)
    original = deepcopy(fixture[1])
    status, refs, detail = deliver(fixture)
    assert status == 'closed'
    assert detail['expected'] == detail['received'] == [0, 2, 4]
    assert detail['missing'] == []
    assert fixture[1] == original
    assert detail['attribution'] == 'direct_gid_scoped_publish_occurrence'
    assert [i['sequence'] for i in detail['native_delivery_identities']] == [0, 2, 4]
    assert all(i['native_witness']['scope_refs']['trace_ref']['event_index'] in
               {r['event_index'] for r in refs} for i in detail['native_delivery_identities'])


def live_fixture():
    paths = list(LIVE.glob('cases/*/runtime/inner-runtime/raw.jsonl'))
    if not paths:
        pytest.skip('immutable live-v3 artifact not installed')
    path = paths[0]
    content = path.read_bytes()
    assert hashlib.sha256(content).hexdigest() == LIVE_SHA256
    capsule = json.loads((path.parents[2] / 'capsule.json').read_text())
    rows = [json.loads(line) for line in content.splitlines()]
    projection = _Projection(capsule['execution']['final_input']['topology_program'],
                             str(path), LIVE_SHA256, rows,
                             {'input_ref': {'event_index': next(e['event_index'] for e in rows
                                                              if e['event'] == 'input_binding')}})
    tokens = {e['logical_id']: e for e in rows if e['event'] == 'endpoint_created'}
    return projection, tokens, content


def test_immutable_live_v3_eight_window_delivery_projections():
    projection, tokens, content = live_fixture()
    original = deepcopy(projection.events)
    observed = []
    for closed in (e for e in projection.events if e['event'] == 'topology_delivery_window_closed'):
        status, refs, detail = projection.delivery(tokens[closed['writer']], tokens[closed['reader']],
                                                    tokens, closed)
        observed.append((closed['event_index'], status, detail['received'], detail['missing']))
    assert observed == [(206, 'closed', [0], []), (207, 'closed', [0], []),
                        (301, 'closed', [1], []), (302, 'closed', [1], []),
                        (500, 'closed_with_gap', [3], [2]), (501, 'closed', [2, 3], []),
                        (696, 'closed', [4, 5], []), (697, 'closed', [4, 5], [])]
    assert projection.events == original
    assert Path(projection.path).read_bytes() == content


def test_public_projection_preserves_body_and_raw_bound_witnesses(tmp_path):
    fixture = native_fixture(tmp_path)
    program, rows, _, before, after, _, _, path = fixture
    projector(fixture)
    content = path.read_bytes()
    result = project_topology_state(program, path, before_ref=before['event_index'],
                                    after_ref=after['event_index'])
    component = result['receipt']['after']['component_refs']['/pairs/w0/r0/delivery_closure']
    assert component['value'] == 'closed'
    assert component['detail']['received'] == [0, 2, 4]
    for identity in component['detail']['native_delivery_identities']:
        assert identity['send_ref']['sha256'] == hashlib.sha256(content).hexdigest()
        assert identity['receive_ref']['path'] == str(path)
        receive = rows[identity['receive_ref']['event_index']]
        assert receive['send_sequence'] == 0
        assert identity['source_timestamp'] == receive['message_info_receipt']['fields']['source_timestamp']['value']
    assert result['eligible'] is False  # Fixture, never real execution evidence.
    assert path.read_bytes() == content


@pytest.mark.parametrize('fault', ['receipt_schema', 'receipt_none', 'deleted_receipt', 'trace_handle',
                                   'take_gid', 'top_level_gid', 'control_config', 'pubseq', 'payload_hash'])
def test_declared_native_invalid_never_falls_back_to_body_or_direct_gid(tmp_path, fault):
    fixture = native_fixture(tmp_path)
    rows = fixture[1]
    send = next(e for e in rows if e['event'] == 'send')
    receive = next(e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0')
    if fault == 'receipt_schema':
        send['publish_occurrence_receipt']['schema'] = 'wrong'
    elif fault == 'receipt_none':
        send['publish_occurrence_receipt'] = None
    elif fault == 'deleted_receipt':
        del send['publish_occurrence_receipt']
    elif fault == 'trace_handle':
        next(e for e in rows if e['event'] == 'rmw_publish_trace')['publisher_rmw_handle'] += 1
    elif fault == 'take_gid':
        receive['rcl_take_receipt']['message_info_receipt']['publisher_gid']['value'] = '07' * 16
    elif fault == 'top_level_gid':
        receive['publisher_gid'] = None
    elif fault == 'control_config':
        fixture[2]['r1'].pop('initial_content_filter')
    elif fault == 'pubseq':
        receive['message_info_receipt']['fields']['publication_sequence_number']['value'] += 1
        receive['rcl_take_receipt']['message_info_receipt'] = deepcopy(receive['message_info_receipt'])
    else:
        receive['payload_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='native_(publish_occurrence_invalid|delivery_identity):'):
        deliver(fixture)


@pytest.mark.parametrize('field,value', [('generation', 2), ('pid', 999),
                                        ('endpoint_generation', 'stale-worker'),
                                        ('process_start_ticks', 999), ('reader_gid', '07' * 16)])
def test_native_keeps_receiver_incarnation_gates(tmp_path, field, value):
    fixture = native_fixture(tmp_path)
    receive = next(e for e in fixture[1] if e['event'] == 'receive' and e['logical_id'] == 'r0')
    receive[field] = value
    with pytest.raises(ValueError, match='endpoint_(identity|process|gid)'):
        deliver(fixture)


@pytest.mark.parametrize('omitted,expected_status,received,missing', [
    (2, 'closed_with_gap', [0, 4], [2]), (4, 'open', [0, 2], [4]),
])
def test_native_gap_still_needs_a_later_received_sentinel(tmp_path, omitted, expected_status, received, missing):
    fixture = native_fixture(tmp_path)
    stamp = {2: 9003, 4: 12009}[omitted]
    next(e for e in fixture[1] if e['event'] == 'receive' and e['logical_id'] == 'r0'
         and e['message_info_receipt']['fields']['source_timestamp']['value'] == stamp)['event'] = 'offline_omitted_receive'
    status, _, detail = deliver(fixture)
    assert (status, detail['received'], detail['missing']) == (expected_status, received, missing)


def test_native_receipt_does_not_replace_snapshot_matching(tmp_path):
    fixture = native_fixture(tmp_path)
    for row in fixture[1]:
        if row['event'] == 'match_snapshot':
            row.pop('matched_peer_gids')
    status, _, detail = deliver(fixture)
    assert status == 'closed' and detail['observed_match_used'] == 'unknown'


def test_reused_declared_send_sequence_is_still_rejected(tmp_path):
    fixture = native_fixture(tmp_path)
    next(e for e in fixture[1] if e['event'] == 'send' and e['send_sequence'] == 2)['send_sequence'] = 0
    with pytest.raises(ValueError):
        deliver(fixture)


def test_same_take_cannot_be_relabelled_as_a_second_occurrence(tmp_path):
    fixture = native_fixture(tmp_path)
    receives = [e for e in fixture[1] if e['event'] == 'receive' and e['logical_id'] == 'r0']
    receives[1]['rcl_take_receipt']['take_index'] = receives[0]['rcl_take_receipt']['take_index']
    with pytest.raises(ValueError, match='single_take_receipt_reused'):
        deliver(fixture)


def test_native_send_after_capture_clock_is_not_admitted(tmp_path):
    fixture = native_fixture(tmp_path)
    sent = next(e for e in fixture[1] if e['event'] == 'send' and e['send_sequence'] == 4)
    sent['monotonic_ns'] = fixture[6]['monotonic_ns'] + 1
    with pytest.raises(ValueError, match='raw_ref_after_boundary_chronology'):
        deliver(fixture)


def test_control_after_close_cannot_backfill_native_attribution(tmp_path):
    fixture = native_fixture(tmp_path)
    control = next(e for e in fixture[1] if e['event'] == 'receive' and e['logical_id'] == 'r1')
    control['monotonic_ns'] = fixture[6]['monotonic_ns'] + 1
    with pytest.raises(ValueError, match='raw_ref_after_boundary_chronology'):
        deliver(fixture)


@pytest.mark.parametrize('publisher_gid,payload,expected', [
    (True, False, 'closed'), (False, True, 'closed'), (False, False, 'unknown'),
])
def test_legacy_non_native_attribution_gates_unchanged(tmp_path, publisher_gid, payload, expected):
    fixture = RawFixture(publisher_gid=publisher_gid, payload=payload)
    rows = fixture.events
    path = tmp_path / 'legacy.jsonl'
    projection = _Projection(fixture.program, str(path), 'a' * 64, rows,
                             {'input_ref': {'event_index': 0}})
    status, _, detail = projection.delivery(fixture.tokens['w0'], fixture.tokens['r0'],
                                           fixture.tokens, rows[fixture.windows[0]['close']])
    assert status == expected
    assert not detail.get('native_delivery_identities')


def test_historical_native_send_hashes_need_not_be_unique_but_scopes_must_be(tmp_path):
    fixture = native_fixture(tmp_path)
    projection = projector(fixture)
    sends = [e for e in fixture[1] if e['event'] == 'send']
    assert len({s['payload_sha256'] for s in sends}) == 1
    for sent in sends:
        assert projection._native_send_evidence(sent, fixture[6])
    sends[1]['publish_occurrence_receipt']['source_timestamp'] = sends[0]['publish_occurrence_receipt']['source_timestamp']
    with pytest.raises(ValueError, match='native_publish_occurrence_invalid:'):
        projection._native_send_evidence(sends[1], fixture[6])
