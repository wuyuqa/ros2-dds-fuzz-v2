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


def grouped_native_projection(tmp_path, layout):
    """Compose existing authored protocols; no native library or DDS is loaded."""
    from test_topology_participant_control_multi import multi_fixture

    fixture = multi_fixture(tmp_path / 'group', writers=2, readers=2,
                            layout=layout, producer_framing=True)
    rows = fixture.events
    tokens = {e['logical_id']: e for e in rows if e['event'] == 'endpoint_created'}
    native_rows, _, _ = instrumented_window()
    selection = next(e for e in native_rows if e['event'] == 'native_peer_provider_selected')
    observation = next(e['native_peer_observation'] for e in native_rows
                       if e['event'] == 'match_snapshot')
    for row in rows:
        row['rmw_identifier'] = 'rmw_fastrtps_cpp'
        if row.get('logical_id') in tokens:
            row['observation_mode'] = 'instrumentation'
    providers = []
    for token in tokens.values():
        provider = {**deepcopy(selection), **deepcopy(token),
                    'event': 'native_peer_provider_selected'}
        providers.append(provider)
    first_snapshot = next(i for i, row in enumerate(rows)
                          if row['event'] == 'group_command_requested'
                          and row.get('command') == 'snapshot')
    rows[first_snapshot:first_snapshot] = providers
    for index, row in enumerate(rows):
        row.update(event_index=index, monotonic_ns=1000 + index * 1000,
                   monotonic_clock_identity='offline-group-native-clock')
        if row['event'] != 'match_snapshot':
            continue
        token = tokens[row['logical_id']]
        kind = 'writer' if token['role'].startswith('writer') else 'reader'
        count = len(row['matched_peer_gids'])
        proof = deepcopy(observation)
        proof.update(domain_id=token['domain_id'], local_gid=token['endpoint_gid'],
                     role_kind=kind, matched_peer_gids=deepcopy(row['matched_peer_gids']),
                     started_monotonic_ns=row['monotonic_ns'],
                     ended_monotonic_ns=row['monotonic_ns'])
        proof['native_result'].update(endpoint_kind=1 if kind == 'writer' else 2,
            local_guid=token['endpoint_gid'], required_count=count, copied=count,
            snapshot_ordinal=index + 1)
        row.update(peer_proof_source='fastdds_native_stateful_peer_snapshot',
                   native_peer_observation=proof)
    projection = _Projection(fixture.program, str(fixture.raw), 'a' * 64, rows,
                             {'input_ref': {'event_index': 0}})
    boundary = next(row for row in reversed(rows)
                    if row['event'] == 'topology_state_boundary')
    return projection, tokens, boundary


def grouped_match(data, writer='w0', reader='r0'):
    projection, tokens, boundary = data
    w, r = tokens[writer], tokens[reader]
    return projection.match(w, r, projection.snapshot_chain(w, boundary),
                            projection.snapshot_chain(r, boundary), boundary)


@pytest.mark.parametrize('layout', ['shared', 'roles'])
def test_grouped_native_projection_authenticates_all_four_directed_edges(
        tmp_path, authored_manifest_loader, layout):
    data = grouped_native_projection(tmp_path, layout)
    before = deepcopy(data[0].events)
    for writer in ('w0', 'w1'):
        for reader in ('r0', 'r1'):
            status, refs, isolation = grouped_match(data, writer, reader)
            assert status == 'bidirectional' and isolation is None
            assert any(e['event'] == 'native_peer_provider_selected' for e in refs)
    assert data[0].events == before


@pytest.mark.parametrize('damage,reason', [
    ('program_opt_in', 'direct_peer_scope_identity_alias_or_missing:pid'),
    ('raw_opt_in', 'group_lineage:group_lineage_requires_explicit_input'),
    ('missing_binding', 'group_lineage:endpoint_binding'),
    ('missing_context', 'group_lineage:context_created'),
    ('future_binding', 'group_lineage:endpoint_binding'),
    ('context_conflict', 'group_lineage:raw_process_context_link'),
    ('aliased_gid', 'direct_peer_scope_identity_alias_or_missing:endpoint_gid'),
    ('stale_query', 'fresh_current_generation_snapshot_missing'),
    ('native_count', 'per_peer_list_count_contradiction'),
    ('graph_scope', 'direct_peer_graph_and_raw_scope_differ'),
    ('graph_type', 'direct_peer_graph_and_raw_type_differ'),
    ('aliased_endpoint', 'direct_peer_scope_identity_alias_or_missing:endpoint_generation'),
    ('native_selection', 'native_peer_selection_not_unique_raw_worker'),
    ('native_mapping', 'native_peer_selected_vendor_mapping_invalid'),
    ('native_guid', 'native_peer_observation_contract_invalid'),
    ('pretend_public', 'native_peer_proof_source_mislabeled'),
])
def test_grouped_native_projection_preserves_lineage_and_native_gates(
        tmp_path, authored_manifest_loader, monkeypatch, damage, reason):
    data = grouped_native_projection(tmp_path, 'shared')
    projection, tokens, _ = data
    rows = projection.events
    if damage == 'program_opt_in':
        projection.program.pop('participant_control')
    elif damage == 'raw_opt_in':
        next(e for e in rows if e['event'] == 'input_binding')['receipt'].pop('participant_control')
    elif damage == 'missing_binding':
        next(e for e in rows if e['event'] == 'endpoint_group_bound'
             and e['endpoint']['logical_id'] == 'w0')['event'] = 'irrelevant'
    elif damage == 'missing_context':
        next(e for e in rows if e['event'] == 'group_context_created')['event'] = 'irrelevant'
    elif damage == 'future_binding':
        bound = next(e for e in rows if e['event'] == 'endpoint_group_bound'
                     and e['endpoint']['logical_id'] == 'w0')
        future = deepcopy(bound)
        future.update(event_index=len(rows), monotonic_ns=rows[-1]['monotonic_ns'] + 1000)
        rows.append(future)
        bound['event'] = 'irrelevant'
    elif damage == 'context_conflict':
        next(e for e in rows if e['event'] == 'graph_snapshot'
             and e['logical_id'] == 'w1')['context_instance_id'] = 'foreign-context'
    elif damage == 'aliased_gid':
        for row in rows:
            if row.get('logical_id') == 'w1':
                row['endpoint_gid'] = tokens['w0']['endpoint_gid']
    elif damage == 'stale_query':
        tokens['w0'] = dict(tokens['w0'], generation=2)
    elif damage in ('native_count', 'graph_scope', 'graph_type'):
        for row in rows:
            if row.get('logical_id') != 'w0':
                continue
            if damage == 'native_count' and row['event'] == 'match_snapshot':
                row['matched_count'] += 1
            elif row['event'] == 'graph_snapshot':
                if damage == 'graph_scope':
                    row['subscriptions'].pop()
                elif damage == 'graph_type':
                    row['publishers'][0]['topic_type'] = 'wrong/type'
    elif damage == 'aliased_endpoint':
        for row in rows:
            if row.get('logical_id') == 'w1':
                row['endpoint_generation'] = tokens['w0']['endpoint_generation']
    elif damage == 'native_selection':
        next(e for e in rows if e['event'] == 'native_peer_provider_selected'
             and e['logical_id'] == 'w0')['event'] = 'irrelevant'
    elif damage == 'native_mapping':
        next(e for e in rows if e['event'] == 'native_peer_provider_selected'
             and e['logical_id'] == 'w0')['vendor_identity']['inode'] += 1
    else:
        for row in rows:
            if row['event'] == 'match_snapshot' and row['logical_id'] == 'w0':
                if damage == 'native_guid':
                    row['native_peer_observation']['local_gid'] = 'f' * 32
                else:
                    row['peer_proof_source'] = 'dds_matched_endpoint_api'

    def forbidden(*args, **kwargs):
        pytest.fail('invalid grouped native evidence must not fall back to isolation')

    monkeypatch.setattr(projection, 'isolation', forbidden)
    with pytest.raises(ValueError, match=reason):
        grouped_match(data)
