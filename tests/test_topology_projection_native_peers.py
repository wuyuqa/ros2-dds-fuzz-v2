"""Offline native-peer raw contracts; no ROS execution or native library loads."""
from copy import deepcopy

import pytest

from r2f.campaign.topology_state_projection import _Projection
from test_fastdds_peer_receipt import authored_manifest_loader, instrumented_window
from test_topology_state_projection import RawFixture


@pytest.fixture
def raw_projection(tmp_path, authored_manifest_loader):
    rows, opened, close = instrumented_window()
    projection = _Projection(RawFixture(readers=2).program,
                             str(tmp_path / 'authored-native.jsonl'), 'a' * 64, rows,
                             {'input_ref': {'event_index': 0}})
    w, r = rows[:2]
    wc = projection.snapshot_chain(w, close)
    rc = projection.snapshot_chain(r, close)
    return projection, w, r, wc, rc, close


def match(raw_projection):
    projection, w, r, wc, rc, close = raw_projection
    return projection.match(w, r, wc, rc, close)


def test_native_peer_membership_is_validated_without_controlled_isolation(raw_projection):
    before = deepcopy(raw_projection[0].events)
    status, refs, isolation = match(raw_projection)
    assert status == 'bidirectional'
    assert isolation is None
    assert any(e['event'] == 'native_peer_provider_selected' for e in refs)
    assert raw_projection[0].events == before


@pytest.mark.parametrize('damage,reason', [
    ('missing', 'native_peer_observation_missing'),
    ('short_copy', 'native_peer_result_invalid'),
    ('other_manifest', 'native_peer_selected_source_identity_invalid'),
    ('no_selection', 'native_peer_selection_not_unique_raw_worker'),
    ('wrong_process', 'native_peer_selection_not_unique_raw_worker'),
    ('capture_before_request', 'native_peer_capture_chronology_invalid'),
    ('pretend_public', 'native_peer_proof_source_mislabeled'),
    ('strip_proof', 'native_peer_proof_source_mislabeled'),
    ('incomplete_peers', 'native_peer_membership_incomplete'),
])
def test_bad_native_evidence_does_not_fall_back_to_isolation(raw_projection, monkeypatch, damage, reason):
    projection, _, _, wc, _, _ = raw_projection
    row = wc[1]
    obs = row['native_peer_observation']
    selected = next(e for e in projection.events
                    if e['event'] == 'native_peer_provider_selected'
                    and e['logical_id'] == row['logical_id'])
    if damage == 'missing': row.pop('native_peer_observation')
    elif damage == 'short_copy': obs['native_result']['copied'] -= 1
    elif damage == 'other_manifest': obs['manifest_reference']['manifest_sha256'] = 'f' * 64
    elif damage == 'no_selection': selected['event'] = 'irrelevant'
    elif damage == 'wrong_process': selected['process_start_ticks'] += 1
    elif damage == 'capture_before_request': obs['started_monotonic_ns'] = 1
    elif damage == 'pretend_public': row['peer_proof_source'] = 'dds_matched_endpoint_api'
    elif damage == 'strip_proof':
        for key in ('peer_proof_source', 'peer_gids_complete', 'native_peer_observation', 'observation_mode'):
            row.pop(key, None)
    elif damage == 'incomplete_peers': row['peer_gids_complete'] = False
    def forbidden(*args, **kwargs):
        pytest.fail('bad declared native evidence must not fall back to isolation')
    monkeypatch.setattr(projection, 'isolation', forbidden)
    with pytest.raises(ValueError, match=reason):
        match(raw_projection)


@pytest.mark.parametrize('damage,reason', [
    ('extra_peer', 'per_peer_list_count_contradiction'),
    ('graph_scope', 'direct_peer_graph_and_raw_scope_differ'),
    ('graph_type', 'direct_peer_graph_and_raw_type_differ'),
    ('current_generation', 'direct_peer_query_identity_not_current_raw_generation'),
])
def test_native_receipt_preserves_existing_raw_scope_gates(raw_projection, damage, reason):
    _, _, r, wc, _, _ = raw_projection
    if damage == 'extra_peer': wc[1]['matched_count'] += 1
    elif damage == 'graph_scope': wc[2]['subscriptions'].pop()
    elif damage == 'graph_type': wc[2]['subscriptions'][0]['topic_type'] = 'wrong/type'
    elif damage == 'current_generation':
        raw_projection = list(raw_projection)
        raw_projection[2] = dict(r, process_start_ticks=r['process_start_ticks'] + 1)
    with pytest.raises(ValueError, match=reason):
        match(raw_projection)


def test_native_observation_serial_cannot_be_reused(raw_projection):
    projection, _, _, wc, _, _ = raw_projection
    earlier = next(e for e in projection.events
                   if e['event'] == 'native_peer_provider_selected'
                   and e['logical_id'] != wc[1]['logical_id'])
    # An authored earlier capture by the same worker, not a persisted run edit.
    index, clock = earlier['event_index'], earlier['monotonic_ns']
    earlier.clear()
    earlier.update(deepcopy(wc[1]), event_index=index, monotonic_ns=clock)
    with pytest.raises(ValueError, match='native_peer_observation_serial_reused_or_regressed'):
        match(raw_projection)


def test_legacy_public_membership_remains_supported(tmp_path):
    fixture = RawFixture(readers=2)
    projection = _Projection(fixture.program, str(tmp_path / 'legacy.jsonl'), 'a' * 64,
                             fixture.events, {'input_ref': {'event_index': 0}})
    w, r = fixture.tokens['w0'], fixture.tokens['r0']
    end = fixture.events[-1]
    status, refs, isolation = projection.match(w, r, projection.snapshot_chain(w, end),
                                              projection.snapshot_chain(r, end), end)
    assert status == 'bidirectional' and isolation is None
    assert not any(e['event'] == 'native_peer_provider_selected' for e in refs)
