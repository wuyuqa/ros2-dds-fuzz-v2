"""Group cleanup contracts through the public fact adapter; no live execution."""
import pytest

from test_topology_participant_control_public import facts, group_fixture, resave


def group_trace(tmp_path, shared):
    fixture = group_fixture(tmp_path, shared=shared)
    # The older reusable fixture also emits endpoint-shaped legacy exits.
    # Preserve offsets, but make those rows unavailable as health evidence.
    for event in fixture.events:
        if event['event'] == 'topology_worker_exit':
            event['event'] = 'fixture_unused_legacy_exit'
    resave(fixture)
    return fixture


@pytest.mark.parametrize('shared', [True, False], ids=['shared', 'split'])
def test_complete_group_cleanup_is_healthy_without_endpoint_process_exits(tmp_path, shared):
    result = facts(group_trace(tmp_path, shared))

    assert result['diagnostic_valid'], result
    assert result['process_healthy'] is True
    assert result['endpoint_identity_complete'] is True
    # Protocol fixtures still have no environment/source/execution authority.
    assert result['execution_evidence'] is False
    assert result['candidate_eligible'] is False
    assert result['seal_eligible'] is False


@pytest.mark.parametrize('shared', [True, False], ids=['shared', 'split'])
@pytest.mark.parametrize('kind', ['process_started', 'process_exit', 'group_context_shutdown',
                                 'endpoint_destroyed'])
@pytest.mark.parametrize('damage', ['missing', 'duplicate'])
def test_group_health_rejects_incomplete_or_ambiguous_cleanup(tmp_path, shared, kind, damage):
    fixture = group_trace(tmp_path, shared)
    event = next(e for e in fixture.events if e['event'] == kind)
    if damage == 'missing':
        event['event'] = 'fixture_omitted_event'
    else:
        spare = next(e for e in fixture.events if e['event'] == 'fixture_unused_legacy_exit')
        # Keep the raw envelope/offset; duplicate the process/cleanup identity.
        spare.update({k: v for k, v in event.items()
                      if k not in ('event_index', 'monotonic_ns', 'time_ns')})
    resave(fixture)
    result = facts(fixture)

    assert result['diagnostic_valid'] is False
    assert result['evidence_error'].startswith('group_lineage:')
    assert result['process_healthy'] is False
    assert result['candidate_eligible'] is False


@pytest.mark.parametrize('shared', [True, False], ids=['shared', 'split'])
@pytest.mark.parametrize('kind,key,value', [
    ('process_exit', 'pid', 9999),
    ('process_exit', 'process_start_ticks', 9999),
    ('process_exit', 'worker_instance_id', 'foreign-worker'),
    ('process_exit', 'group_id', 'foreign-group'),
    ('process_exit', 'returncode', 1),
    ('group_context_shutdown', 'context_instance_id', 'foreign-context'),
    ('group_context_shutdown', 'closed', False),
    ('group_context_shutdown', 'errors', ['shutdown-failed']),
    ('endpoint_destroyed', 'endpoint_gid', 'foreign-endpoint'),
    ('endpoint_destroyed', 'cleanup_status', 'failed'),
])
def test_group_health_rejects_bad_identity_exit_or_cleanup(tmp_path, shared, kind, key, value):
    fixture = group_trace(tmp_path, shared)
    next(e for e in fixture.events if e['event'] == kind)[key] = value
    resave(fixture)
    result = facts(fixture)

    assert result['diagnostic_valid'] is False
    assert result['evidence_error'].startswith('group_lineage:')
    assert result['process_healthy'] is False
    assert result['candidate_eligible'] is False


@pytest.mark.parametrize('damage', ['cleanup_incomplete', 'tool_error', 'metadata_forgery'])
def test_group_health_does_not_replace_other_health_gates(tmp_path, damage):
    fixture = group_trace(tmp_path, True)
    if damage == 'cleanup_incomplete':
        fixture.events[-1]['cleanup_complete'] = False
    elif damage == 'tool_error':
        next(e for e in fixture.events if e['event'] == 'fixture_unused_legacy_exit')['event'] = 'topology_tool_error'
    else:
        fixture.runtime['execution_metadata']['participant_groups']['processes'][0]['pid'] = 9999
    resave(fixture)
    result = facts(fixture)

    assert result['process_healthy'] is False
    assert result['candidate_eligible'] is False
    if damage == 'metadata_forgery':
        assert result['evidence_error'] == 'group_lineage_metadata_mismatch'
    else:
        assert result['diagnostic_valid'] is True


@pytest.mark.parametrize('damage', [None, 'missing', 'duplicate', 'identity', 'nonzero', 'group_exits'])
def test_legacy_health_still_requires_exact_endpoint_worker_exits(tmp_path, damage):
    from test_topology_adapter import RawFixture

    fixture = RawFixture().save(tmp_path)
    exits = [e for e in fixture.events if e['event'] == 'topology_worker_exit']
    if damage == 'missing':
        exits[0]['event'] = 'fixture_omitted_event'
    elif damage == 'duplicate':
        exits[1].update({k: v for k, v in exits[0].items()
                         if k not in ('event_index', 'monotonic_ns', 'time_ns')})
    elif damage == 'identity':
        exits[0]['pid'] = 9999
    elif damage == 'nonzero':
        exits[0]['returncode'] = 1
    elif damage == 'group_exits':
        for event in exits:
            event['event'] = 'process_exit'
        # Unauthenticated helper metadata must not select the group path.
        fixture.runtime['worker_metadata'] = {'process_healthy': True, 'returncode': 0}
        fixture.runtime['facts']['process_healthy'] = True
    resave(fixture)
    result = facts(fixture)

    assert result['diagnostic_valid'], result
    assert result['process_healthy'] is (damage is None)
    assert result['candidate_eligible'] is False


def project(fixture):
    from r2f.campaign.topology_state_projection import project_topology_state

    boundaries = [e for e in fixture.events if e['event'] == 'topology_state_boundary']
    return project_topology_state(fixture.program, fixture.raw,
        before_ref=boundaries[0]['event_index'], after_ref=boundaries[-1]['event_index'],
        expected_sha256=fixture.runtime['raw_sha256'])


@pytest.mark.parametrize('shared', [True, False], ids=['shared', 'split'])
def test_projection_accepts_current_raw_bound_group_snapshots(tmp_path, shared):
    fixture = group_trace(tmp_path, shared)
    result = project(fixture)

    assert result['state_after']['pairs'][0]['observed_match'] == 'bidirectional', result['receipt']['reasons']
    assert result['receipt']['status'] == 'observed'
    assert result['receipt']['fixture'] is True
    assert result['eligible'] is False


@pytest.mark.parametrize('kind', ['process_started', 'group_context_created', 'group_worker_ready',
                                 'endpoint_group_bound'])
@pytest.mark.parametrize('damage', ['missing', 'duplicate'])
def test_projection_rejects_missing_or_ambiguous_live_group_lineage(tmp_path, kind, damage):
    fixture = group_trace(tmp_path, True)
    event = next(e for e in fixture.events if e['event'] == kind)
    if damage == 'missing':
        event['event'] = 'fixture_omitted_event'
    else:
        # Insert a second lineage claim at an existing pre-snapshot offset.
        # Use an existing pre-snapshot row without shifting raw references.
        spare = next(e for e in fixture.events if e['event'] == 'topology_network_observation')
        spare.update({k: v for k, v in event.items()
                      if k not in ('event_index', 'monotonic_ns', 'time_ns')})
    resave(fixture)
    result = project(fixture)

    assert result['state_after']['pairs'][0]['observed_match'] == 'unknown'
    assert any('group_lineage:' in reason for reason in result['receipt']['reasons'])
    assert result['eligible'] is False


@pytest.mark.parametrize('key,value', [
    ('pgid', 9999), ('group_id', 'foreign-group'),
    ('worker_instance_id', 'foreign-worker'), ('context_instance_id', 'foreign-context'),
    ('process_start_ticks', 9999), ('run_id', 'foreign-run'), ('domain_id', 77),
])
def test_projection_binds_snapshot_process_group_and_context_identity(tmp_path, key, value):
    fixture = group_trace(tmp_path, True)
    for event in fixture.events:
        if event['event'] == 'match_snapshot' and event.get('logical_id') == 'r0':
            event[key] = value
    resave(fixture)
    result = project(fixture)

    assert result['state_after']['pairs'][0]['observed_match'] == 'unknown'
    assert result['eligible'] is False


@pytest.mark.parametrize('damage', ['program_opt_in_missing', 'raw_opt_in_missing',
                                   'future_binding', 'future_start', 'native_source',
                                   'invalid_generation', 'unsupported_program'])
def test_projection_shared_pid_requires_current_public_cp1_evidence(tmp_path, damage):
    from r2f.core.evidence import canonical_hash

    fixture = group_trace(tmp_path, True)
    if damage == 'program_opt_in_missing':
        fixture.program.pop('participant_control')
        fixture.events[0]['receipt']['program_sha256'] = canonical_hash(fixture.program)
    elif damage in ('invalid_generation', 'unsupported_program'):
        fixture.program['actions'].append(dict(action_id='invalid_future_action',
            op='destroy' if damage == 'invalid_generation' else 'fixture_unsupported_op',
            endpoint_id='r0', generation=2 if damage == 'invalid_generation' else 1))
        fixture.events[0]['receipt']['program_sha256'] = canonical_hash(fixture.program)
    elif damage == 'raw_opt_in_missing':
        fixture.events[0]['receipt'].pop('participant_control')
    elif damage in ('future_binding', 'future_start'):
        kind = 'endpoint_group_bound' if damage == 'future_binding' else 'process_started'
        event = next(e for e in fixture.events if e['event'] == kind)
        spare = next(e for e in fixture.events if e['event'] == 'fixture_unused_legacy_exit')
        spare.update({k: v for k, v in event.items()
                      if k not in ('event_index', 'monotonic_ns', 'time_ns')})
        event['event'] = 'fixture_omitted_event'
        # Complete future cleanup remains present, but cannot qualify the past.
    else:
        for event in fixture.events:
            if event['event'] == 'match_snapshot':
                event['peer_proof_source'] = 'fastdds_native_stateful_peer_snapshot'
    resave(fixture)
    result = project(fixture)

    assert result['state_after']['pairs'][0]['observed_match'] == 'unknown'
    assert result['eligible'] is False
    if damage in ('invalid_generation', 'unsupported_program'):
        reason = ('participant_control_scope:generation' if damage == 'invalid_generation'
                  else 'participant_control_scope:unsupported_action')
        assert any(reason in item for item in result['receipt']['reasons'])


def test_projection_legal_future_destroy_preserves_current_snapshot_prefix(tmp_path):
    from r2f.core.evidence import canonical_hash

    fixture = group_trace(tmp_path, True)
    # A declared future CP3 operation is supported but has not run at this
    # boundary. It neither retires the current reader nor invalidates its proof.
    fixture.program['actions'].append(dict(action_id='future_destroy', op='destroy',
        endpoint_id='r0', generation=1))
    fixture.events[0]['receipt']['program_sha256'] = canonical_hash(fixture.program)
    resave(fixture)
    result = project(fixture)

    assert result['state_after']['pairs'][0]['observed_match'] == 'bidirectional', result['receipt']['reasons']
    reader = next(e for e in result['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['generation'] == 1 and reader['liveness'] == 'alive'
    assert result['receipt']['status'] == 'observed'
    assert result['receipt']['fixture'] is True
    assert result['eligible'] is False


def test_projection_uses_live_snapshot_prefix_without_future_cleanup(tmp_path):
    fixture = group_trace(tmp_path, True)
    boundary = [e for e in fixture.events if e['event'] == 'topology_state_boundary'][-1]
    fixture.events = fixture.events[:boundary['event_index'] + 1]
    resave(fixture)
    result = project(fixture)

    assert result['state_after']['pairs'][0]['observed_match'] == 'bidirectional', result['receipt']['reasons']
    assert result['receipt']['status'] == 'observed'
    assert result['eligible'] is False
