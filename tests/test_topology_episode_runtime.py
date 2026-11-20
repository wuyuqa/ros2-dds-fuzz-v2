"""Offline contracts only: no ROS, domain owner or execution qualification."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.episode import EpisodeStore, EpisodeStoreError
from r2f.campaign.topology_episode import audit_topology_episode, reset_from_events
from r2f.modules.s1.fuzz_mutators_v2 import continuous_episode_program
from r2f.runtime.topology_pair import run_topology
from r2f.runtime.topology_worker import OfflineTopologyBackend

CFG = {'backend': 'offline_fake', 'run_id': 'offline-episode', 'domain_id': 191,
       'source_repo': '/tmp/offline-src'}


@pytest.fixture
def executed(tmp_path):
    program = continuous_episode_program()
    result = run_topology(program, CFG, tmp_path)
    assert result['status'] == 'executed', result['reason']
    return program, result


def test_actual_actions_and_immutable_progress_precede_next_side_effect(tmp_path):
    class InspectNextAction(OfflineTopologyBackend):
        def create_endpoint(self, spec, generation, role):
            if spec['logical_id'] == 'r0' and generation == 1:
                commits = list((tmp_path/'episode'/'checkpoints').glob('*/*.json'))
                assert len(commits) == 1
                progress = json.loads(commits[0].read_text())
                assert progress['receipts'][0]['action_id'] == 'create_w1'
                assert progress['receipts'][0]['after_state'] is not None
            return super().create_endpoint(spec, generation, role)
    program = continuous_episode_program()
    result = run_topology(program, CFG, tmp_path, backend=InspectNextAction())
    assert result['status'] == 'executed', result['reason']
    execution = result['episode']['execution']
    assert execution['stop_reason'] == 'episode_completed'
    assert execution['action_count'] == len(program['actions']) == 14
    assert execution['state_schema'] == 'r2f.s1-topology-state/v1'
    assert execution['state_model'] == 'no_pass_claim_model'
    assert all(r['oracle_verdict'] == 'unknown' for r in execution['receipts'])
    progress = list((tmp_path/'episode'/'checkpoints').glob('*/*.json'))
    assert len(progress) == 14
    assert sum(json.loads(p.read_text())['checkpoint'] for p in progress) == 2
    commits = [e for e in result['events'] if e['event'] == 'topology_episode_action_committed']
    starts = [e for e in result['events'] if e['event'] == 'topology_action_begin']
    assert all(c['event_index'] < s['event_index'] for c, s in zip(commits, starts[1:]))
    # Supplying a forged runtime summary cannot upgrade independently read raw.
    result['episode'] = {'completed': True, 'reset_verified': True, 'oracle_verdict': 'pass'}
    audit = audit_topology_episode(program, result, CFG)
    assert audit['structural_valid'] is True, audit
    assert audit['completed'] is True
    assert audit['execution_evidence'] is False
    assert audit['fixture'] is True
    assert audit['reset_verified'] is False
    assert audit['candidate_eligible'] is False
    assert audit['restoration_supported'] is False


def test_persistence_failure_stops_before_next_action_and_preserves_completed_receipt(tmp_path, monkeypatch):
    original = EpisodeStore.commit_progress
    def fail_checkpoint(self, episode, receipts, *, checkpoint=False):
        if checkpoint:
            raise OSError('controlled checkpoint disk failure')
        return original(self, episode, receipts, checkpoint=checkpoint)
    monkeypatch.setattr(EpisodeStore, 'commit_progress', fail_checkpoint)
    result = run_topology(continuous_episode_program(), CFG, tmp_path)
    assert result['status'] == 'aborted'
    assert 'receipt_observer_tool_failure' in result['reason']
    execution = result['episode']['execution']
    assert execution['action_count'] == 6
    assert execution['receipts'][-1]['action_id'] == 'checkpoint_1'
    assert execution['receipts'][-1]['status'] == 'executed'
    assert execution['stop_details']['responsibility_layer'] == 'harness'
    assert not any(e['event'] == 'topology_action_begin' and e['action_id'] == 'destroy_r1'
                   for e in result['events'])
    assert result['cleanup_receipt']['closed'] is True
    assert list((tmp_path/'episode'/'episodes').glob('*.json'))


def test_budget_stop_keeps_terminal_store_without_starting_an_action(tmp_path):
    program = continuous_episode_program()
    program['episode']['max_seconds'] = 1e-12
    result = run_topology(program, CFG, tmp_path)
    assert result['status'] == 'aborted'
    assert result['episode']['execution']['stop_reason'] == 'max_seconds'
    assert result['episode']['execution']['action_count'] == 0
    assert not any(e['event'] == 'endpoint_created' for e in result['events'])
    audit = audit_topology_episode(program, result, CFG)
    assert audit['structural_valid'] is True, audit
    assert audit['completed'] is False
    assert audit['checkpoint_observations_complete'] is False


def test_append_only_progress_cannot_be_overwritten(executed):
    program, result = executed
    root = Path(result['episode']['store_root'])
    store = EpisodeStore(root)
    record = store.load(result['episode']['episode_id'])
    with pytest.raises(EpisodeStoreError, match='append-only'):
        store.commit_progress(record['episode'], record['execution']['receipts'][:1])


@pytest.mark.parametrize('artifact', ['prefix', 'progress', 'terminal'])
def test_independent_consumer_rejects_artifact_tamper(executed, artifact):
    program, result = executed
    root = Path(result['episode']['store_root'])
    if artifact == 'prefix':
        path = root/'prefixes'/'action-0005.jsonl'
        path.write_bytes(path.read_bytes()+b'{}\n')
    elif artifact == 'progress':
        path = next((root/'checkpoints').glob('*/0005.json'))
        value = json.loads(path.read_text());value['restoration_supported'] = True
        path.write_text(json.dumps(value))
    else:
        path = next((root/'episodes').glob('*.json'))
        value = json.loads(path.read_text());value['execution']['stop_reason'] = 'forged_completed'
        path.write_text(json.dumps(value))
    audit = audit_topology_episode(program, result, CFG)
    assert audit['status'] == 'invalid'
    assert audit['structural_valid'] is False
    assert audit['candidate_eligible'] is False
    assert audit['reasons']


def test_real_runtime_reset_needs_exit_retirement_and_distinct_new_identity():
    def row(kind, index, generation, pid, gid):
        return dict(event=kind, event_index=index, logical_id='w0', generation=generation,
            pid=pid, process_start_ticks=pid+100, endpoint_generation='epoch-'+str(generation),
            endpoint_gid=gid, backend='ros2_public_api', fixture=False)
    previous = row('endpoint_created', 0, 1, 101, 'a'*32)
    exited = dict(row('topology_worker_exit', 1, 1, 101, 'a'*32), returncode=0)
    retired = dict(row('topology_endpoint_destroyed', 2, 1, 101, 'a'*32), cleanup_status='closed')
    current = row('endpoint_created', 3, 2, 102, 'b'*32)
    events = [previous, exited, retired, current]
    action = {'endpoint_id': 'w0', 'generation': 2}
    prefix = {'path': '/synthetic/unit-only/prefix.jsonl', 'sha256': 'c'*64}
    reset = reset_from_events(events, action, prefix)
    assert reset['status'] == 'observed'
    assert reset['receipt']['state_origin'] == 'fresh_runtime'
    assert reset['receipt']['baseline_restored'] is False
    assert reset['receipt']['restored_state_id'] is None
    for index, key, value in [(1, 'returncode', 1), (2, 'cleanup_status', 'unknown'),
            (3, 'endpoint_gid', 'a'*32), (3, 'pid', 101), (0, 'fixture', True)]:
        changed = deepcopy(events);changed[index][key] = value
        if key == 'pid':
            changed[index]['process_start_ticks'] = previous['process_start_ticks']
        assert reset_from_events(changed, action, prefix)['status'] == 'unknown'
    assert reset_from_events([previous, retired, current], action, prefix)['status'] == 'unknown'


@pytest.fixture
def routed_episode(tmp_path):
    from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
    from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts, TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
    from r2f.runtime.ros2_pair import run_pair
    from r2f.campaign.topology_adapter import facts_from_events
    plan = make_topology_plan(continuous_episode_program(), [{'operator_id': 'action.insert'}], rng_seed=5)
    identity = {'input_identity_schema': INPUT_SCHEMA_V2, 'target_id': TOPOLOGY_TARGET,
        'target_revision': 0, 'mutation_kind': 'endpoint_program', 'carrier': 'ros2_endpoint',
        'mutation_parameters': {}, 'rmw': 'rmw_cyclonedds_cpp', 'repeat': 0,
        'seed': 5, 'arm': 'mutated', 'mutation_layer_order': ['endpoint_program'],
        'input_layer_receipts': topology_input_layer_receipts(plan), 'final_input': plan.final_config,
        'source_manifest': {}, 'mutation_plan': plan.to_dict(),
        'operations': [op.to_dict() for op in plan.operations], 'runtime_windows': [],
        'contract_hash': plan.contract_hash}
    config = {**deepcopy(identity), 'effective_input': deepcopy(identity),
        'effective_input_sha256': effective_input_hash(identity, schema=INPUT_SCHEMA_V2),
        'topology_program': deepcopy(plan.final_config['topology_program']),
        'plan_hash': plan.plan_hash(), 'runtime_capability': TOPOLOGY_CAPABILITY,
        **CFG}
    runtime = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    assert runtime['status'] == 'executed', runtime.get('reason')
    return config, runtime


def test_unified_fact_consumer_requires_persisted_episode_evidence(tmp_path, routed_episode):
    from r2f.campaign.topology_adapter import facts_from_events
    config, runtime = routed_episode
    facts = facts_from_events(runtime['events'], runtime, config)
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    assert facts['episode_observation']['structural_valid'] is True
    assert facts['unresolved_historical_obligations'] == []
    assert len(facts['retired_episode_history_windows']) == 1
    assert {row['send_ref']['event_index'] for row in facts['historical_send_coverage']} == {
        e['event_index'] for e in runtime['events'] if e['event']=='send' and e['generation']==1}
    assert facts['candidate_eligible'] is False
    progress = next((tmp_path/'episode'/'checkpoints').glob('*/0000.json'))
    progress.unlink()
    facts = facts_from_events(runtime['events'], runtime, config)
    assert facts['diagnostic_valid'] is False
    assert facts['candidate_eligible'] is False
    assert facts['evidence_error'] == 'topology_episode_evidence_invalid'


def test_new_prefix_progress_and_terminal_directory_entries_are_synced(tmp_path, monkeypatch):
    import os
    import stat
    original = os.fsync
    directories = []
    def observe(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            directories.append(Path(os.readlink('/proc/self/fd/'+str(fd))))
        return original(fd)
    monkeypatch.setattr(os, 'fsync', observe)
    result = run_topology(continuous_episode_program(), CFG, tmp_path)
    assert result['status'] == 'executed', result['reason']
    root = tmp_path/'episode'
    assert directories.count(root/'prefixes') >= 15  # initial + fourteen actions
    progress_dir = root/'checkpoints'/result['episode']['episode_id']
    assert directories.count(progress_dir) >= 14
    assert root/'episodes' in directories
    assert all(directory in directories for directory in (root, tmp_path, tmp_path.parent))


def test_directory_sync_failure_stops_subsequent_actions_and_cleans_up(tmp_path, monkeypatch):
    import os
    import stat
    original = os.fsync
    def fail_progress_publish(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            directory = Path(os.readlink('/proc/self/fd/'+str(fd)))
            # Fail the post-link sync, not directory creation. The actual first
            # action remains in raw and its terminal receipt remains in store.
            if directory.parent.name == 'checkpoints' and (directory/'0000.json').exists():
                raise OSError('controlled directory sync failure')
        return original(fd)
    monkeypatch.setattr(os, 'fsync', fail_progress_publish)
    result = run_topology(continuous_episode_program(), CFG, tmp_path)
    assert result['status'] == 'aborted'
    assert 'receipt_observer_tool_failure' in result['reason']
    assert result['episode']['execution']['action_count'] == 1
    assert result['episode']['execution']['receipts'][0]['status'] == 'executed'
    assert result['cleanup_receipt']['closed'] is True
    assert [e['action_id'] for e in result['events'] if e['event'] == 'topology_action_begin'] == ['create_w1']


@pytest.mark.parametrize('fault', ['missing_exit', 'wrong_exit_gid', 'cleanup_unknown',
                                  'successor_same_gid', 'incomplete_close', 'missing_delivery'])
def test_retired_episode_history_cannot_borrow_new_generation_proof(routed_episode, fault):
    """Authored offline private-consumer counterexamples, not real executions."""
    from types import SimpleNamespace
    from r2f.campaign.topology_adapter import (_Audit, facts_from_events,
        _retired_episode_history_windows, _retired_episode_send_coverage)
    config, runtime = routed_episode
    facts = facts_from_events(runtime['events'], runtime, config)
    original = _Audit(runtime['events'], runtime, config)
    events = deepcopy(original.events)
    old_close = next(e for e in events if e['event']=='topology_delivery_window_closed'
                     and e['writer_generation']==e['reader_generation']==1)
    retired = next(e for e in events if e['event']=='endpoint_destroyed' and e['logical_id']=='r0')
    exited = next(e for e in events if e['event']=='topology_worker_exit' and e['logical_id']=='r0' and e['generation']==1)
    successor = next(e for e in events if e['event']=='endpoint_created' and e['logical_id']=='r0' and e['generation']==2)
    deliveries = deepcopy(facts['deliveries'])
    if fault == 'missing_exit': exited['event'] = 'authored_missing_exit'
    elif fault == 'wrong_exit_gid': exited['endpoint_gid'] = 'f'*32
    elif fault == 'cleanup_unknown': retired['cleanup_status'] = 'unknown'
    elif fault == 'successor_same_gid': successor['endpoint_gid'] = retired['endpoint_gid']
    elif fault == 'incomplete_close': old_close['window_complete'] = False
    else: deliveries = [d for d in deliveries if d['writer_generation'] != 1]
    audit = SimpleNamespace(program=original.program, path=original.path, sha=original.sha,
        events=events, binding=original.binding, specs=original.specs,
        tokens={(e['logical_id'],e['generation']):e for e in events if e['event']=='endpoint_created'},
        ref=original.ref, resolve=lambda ref: events[ref['event_index'] if isinstance(ref,dict) else ref])
    projection = {'receipt': facts['state_projection_receipt']}
    windows = _retired_episode_history_windows(audit, projection, deliveries, facts['episode_observation'])
    sends = [e for e in events if e['event']=='send' and e['generation']==1]
    assert all(_retired_episode_send_coverage(audit,windows,send) is None for send in sends)
