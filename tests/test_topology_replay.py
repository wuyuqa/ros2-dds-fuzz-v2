"""Saved action-program replay/audit: real services, explicitly offline runtime."""
from copy import deepcopy
from functools import lru_cache
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign import replay
from r2f.core import ReplayCapsule, canonical_hash
from r2f.reports import run_report
from test_topology_cycle import (SOURCE, capsule_from_registry, offline_runtime, profile, run_cycle)


def test_fresh_domain_replay_same_program_without_second_mutation(tmp_path):
    _, _, plan, capsule = capsule_from_registry(4)
    source = tmp_path / 'original' / 'capsule.json'
    source.parent.mkdir()
    source.write_text(json.dumps(capsule.to_dict()))
    original = source.read_bytes()
    captured = []
    runtime = offline_runtime(captured)
    with patch.object(replay, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch('r2f.modules.s1.topology_input.make_topology_plan', side_effect=AssertionError('remutation')):
        result = replay.replay_cell(source, output_dir=tmp_path / 'replay', domain_id=81, runtime=runtime)
        audit = replay.audit_replay(result['report_path'])
    runtime.assert_called_once()
    assert captured[0]['topology_program'] == plan.final_config['topology_program']
    assert captured[0]['mutation_plan'] == plan.to_dict()
    assert captured[0]['input_layer_receipts'] == capsule.input['effective_input']['input_layer_receipts']
    assert captured[0]['effective_input_sha256'] == capsule.input['effective_input_sha256']
    assert captured[0]['domain_id'] == 81 and source.read_bytes() == original
    assert result['real_ros_execution'] is False and result['real_runtime_attempt'] is False
    assert result['status'] != 'pass'
    assert audit['valid'], audit
    assert not audit['real_ros_execution']


@pytest.mark.parametrize('damage', ['capability', 'operator', 'final', 'layer_hash', 'order', 'input_mutations'])
def test_replay_route_rejects_tamper_no_runtime(tmp_path, damage):
    _, _, _, capsule = capsule_from_registry(2)
    config = replay._replay_config(capsule, 'replay-config', 81, SOURCE)
    from r2f.runtime.topology_route import validate_topology_route
    assert validate_topology_route(config)
    if damage == 'capability': config.pop('runtime_capability')
    elif damage == 'operator': config['mutation_plan']['operations'][0]['parameters']['operator_id'] = 'unknown'
    elif damage == 'final': config['final_input']['topology_program']['actions'] = []
    elif damage == 'layer_hash': config['input_layer_receipts'][0]['input_after_sha256'] = '0' * 64
    elif damage == 'order': config['mutation_plan']['operations'].reverse()
    else: config['input_mutations'] = [{'operator_id': 'action.insert'}]
    with pytest.raises(ValueError):
        validate_topology_route(config)


def test_report_reopens_plan_and_offline_result_not_saved_labels(tmp_path):
    captured = []
    result = run_cycle(tmp_path / 'original', profile(2), runtime=offline_runtime(captured))
    row = result['rows'][0]
    errors = []
    with patch.object(run_report, 'source_manifest', return_value=deepcopy(SOURCE)):
        assert run_report._verify_exploratory_row(tmp_path / 'original', row, result, errors), errors
        qualified = run_report._topology_artifact_qualification(tmp_path / 'original', row, result)
        assert qualified['real_ros_execution'] is False
        forged = dict(row, candidate_eligible=True, feedback_eligible=True, seal_eligible=True,
                      execution_qualification={'real_ros_execution': True})
        errors = []
        assert not run_report._verify_exploratory_row(tmp_path / 'original', forged, result, errors)
        assert 'claims_eligibility' in ' '.join(errors)
        config_path = Path(row['runtime_config_path'])
        config = json.loads(config_path.read_text())
        config['topology_program']['actions'] = []
        config_path.write_text(json.dumps(config))
        errors = []
        assert not run_report._verify_exploratory_row(tmp_path / 'original', row, result, errors)
        assert errors


@pytest.mark.parametrize('damage', ['program', 'plan', 'layer', 'path', 'source', 'missing_result', 'outer_failed'])
def test_independent_topology_report_rejects_changed_artifacts(tmp_path, damage):
    # Plan fixtures suffice for structural cases; outer/result tests use actual
    # OfflineTopologyBackend artifacts, never claim real SUT observation.
    runtime = offline_runtime([]) if damage in {'missing_result', 'outer_failed'} else None
    root = tmp_path / 'case'
    result = run_cycle(root, profile(2), runtime=runtime)
    row = deepcopy(result['rows'][0])
    config_path = Path(row['runtime_config_path'])
    if damage in {'program', 'layer', 'source'}:
        config = json.loads(config_path.read_text())
        if damage == 'program': config['topology_program']['actions'] = []
        elif damage == 'layer': config['input_layer_receipts'][0]['input_after_sha256'] = '0' * 64
        else: config['source_manifest']['behavior_tree_sha256'] = 'c' * 64
        config_path.write_text(json.dumps(config))
    elif damage == 'plan':
        plan_path = config_path.parent / 'mutation_plan.json'
        plan = json.loads(plan_path.read_text()); plan['operations'].reverse()
        plan_path.write_text(json.dumps(plan))
    elif damage == 'path': row['replay_path'] = str(tmp_path / 'outside.json')
    elif damage == 'missing_result':
        Path(row['runtime_result_path']).unlink(); row['actual_run'] = True
    else:
        path = Path(row['runtime_result_path']); outer = json.loads(path.read_text())
        outer.update(runtime_status='failed', real_execution=True, cleanup_ok=False)
        path.write_text(json.dumps(outer))
        # Do not rewrite the previous raw or row to disguise the failed wrapper.
    errors = []
    with patch.object(run_report, 'source_manifest', return_value=deepcopy(SOURCE)):
        assert not run_report._verify_exploratory_row(root, row, result, errors)
    assert errors


@lru_cache(maxsize=1)
def _context_input():
    # Author once through the actual Registry/provider/capsule/route; each test
    # receives a detached copy. No real-source traversal/time budget assertion.
    _, _, plan, capsule = capsule_from_registry(1)
    return plan.final_config, replay._replay_config(capsule, 'context-fixture', 81, SOURCE)


def context_artifacts(root):
    """Positive context-only fixture, NOT execution/frozen/kernel evidence."""
    final, config = deepcopy(_context_input())
    env = {'ok': True, 'environment_mode': 'ros_overlay', 'ros_distro': 'jazzy',
           'domain_id': 81, 'target_revision': config['target_revision'],
           'source': {'head': SOURCE['head'], 'source_repo': SOURCE['source_repo']},
           'rmw': {config['rmw']: {'available': True}}, 'dependency_lock_sha256': 'f' * 64,
           'process_scan': {'ok': True}, 'old_lock_scan': {'ok': True}}
    metadata = {key: deepcopy(config[key]) for key in ('run_id', 'target_id', 'target_revision', 'rmw',
                'arm', 'repeat', 'domain_id', 'seed', 'mutation_kind', 'carrier', 'effective_input_sha256',
                'source_manifest')}
    metadata.update(declared_domain_id=81, declared_rmw=config['rmw'],
                    source_identity=deepcopy(env['source']), source_after=deepcopy(SOURCE),
                    input_program_sha256=canonical_hash(final['topology_program']),
                    context_basis='sealed_input_declaration_not_sut_observation',
                    sealed_execution_context={key: deepcopy(config.get(key)) for key in
                        ('run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat',
                         'input_identity_schema', 'effective_input_sha256', 'plan_hash')})
    root.mkdir()
    env_path = root / 'environment.json'; env_path.write_text(json.dumps(env))
    from r2f.core import raw_sha256
    metadata['environment_manifest'] = {'path': str(env_path), 'sha256': raw_sha256(env_path)}
    event = {'event': 'environment_preflight', 'event_index': 0, 'ok': True,
             'run_id': config['run_id'], 'domain_id': 81, 'environment': deepcopy(env)}
    result = {'execution_metadata': metadata, 'environment': env, 'events': [event,
              {'event': 'topology_execution_metadata', 'event_index': 1, 'run_id': config['run_id'],
               'domain_id': 81, 'metadata': deepcopy(metadata)}],
              'real_execution': False, 'evidence_origin': 'fixture', 'runtime_status': 'completed'}
    (root / 'execution_metadata.json').write_text(json.dumps(metadata))
    return config, result


def test_generic_topology_context_positive_fixture_not_execution_proof(tmp_path):
    config, result = context_artifacts(tmp_path / 'context')
    assert replay._topology_context_errors(config, result, output_root=tmp_path / 'context') == []
    (tmp_path / 'context/runtime_result.json').write_text(json.dumps(result))
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': True, 'execution_stage': 'judged'}, output_root=tmp_path / 'context')
    assert qualified['real_ros_execution'] is False
    assert 'offline_fixture_cannot_establish_real_ros' in qualified['qualification_errors']


@pytest.mark.parametrize('key', ['run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat',
    'domain_id', 'source_manifest', 'source_after', 'sealed_execution_context', 'context_basis',
    'input_program_sha256', 'declared_domain_id', 'source_identity', 'environment_manifest'])
def test_topology_metadata_rehash_tamper_not_hidden_by_raw_facts(tmp_path, key):
    root = tmp_path / 'context'; config, result = context_artifacts(root)
    result['execution_metadata'][key] = 'tampered'
    # Rehash/persist outer artifacts too: the gate must compare semantic binding.
    (root / 'runtime_result.json').write_text(json.dumps(result))
    context_errors = replay._topology_context_errors(config, result, output_root=root)
    assert context_errors
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': True, 'execution_stage': 'judged'}, output_root=root)
    assert set(context_errors) <= set(qualified['qualification_errors'])
    assert qualified['real_ros_execution'] is False


@pytest.mark.parametrize('damage', ['disk', 'result', 'raw', 'missing', 'raw_duplicate', 'all_domain', 'all_source', 'all_rmw'])
def test_topology_environment_rehash_tamper_cross_surface(tmp_path, damage):
    from r2f.core import raw_sha256
    root = tmp_path / 'context'; config, result = context_artifacts(root)
    env_path = root / 'environment.json'
    if damage == 'missing': env_path.unlink()
    elif damage == 'disk': env_path.write_text(json.dumps(dict(result['environment'], domain_id=99)))
    elif damage == 'result': result['environment']['domain_id'] = 99
    elif damage == 'raw': result['events'][0]['environment']['domain_id'] = 99
    elif damage == 'raw_duplicate': result['events'].append(deepcopy(result['events'][0]))
    else:
        if damage == 'all_domain': result['environment']['domain_id'] = 99
        elif damage == 'all_source': result['environment']['source']['head'] = 'other-head'
        else: result['environment']['rmw'][config['rmw']]['available'] = False
        result['events'][0]['environment'] = deepcopy(result['environment'])
        env_path.write_text(json.dumps(result['environment']))
    raw = root / 'raw.jsonl'; raw.write_text(''.join(json.dumps(e) + '\n' for e in result['events']))
    result.update(raw_path=str(raw), raw_sha256=raw_sha256(raw))
    (root / 'runtime_result.json').write_text(json.dumps(result))
    context_errors = replay._topology_context_errors(config, result, output_root=root)
    assert context_errors
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': True, 'execution_stage': 'judged'}, output_root=root)
    assert set(context_errors) <= set(qualified['qualification_errors'])
    assert qualified['real_ros_execution'] is False


def worker_artifacts(root):
    """Authored two-generation worker protocol fixture, never a real process."""
    config, result = context_artifacts(root)
    events, workers = [result['events'][0]], []
    raw = root / 'raw.jsonl'
    for index, role in enumerate(('writer', 'reader')):
        token = {'logical_id': role + '_fixture', 'generation': 1, 'pid': 200 + index,
                 'pgid': 200 + index, 'process_start_ticks': 1000 + index,
                 'endpoint_gid': 'fixture-gid-' + str(index), 'endpoint_generation': 'fixture-epoch-' + str(index),
                 'worker_generation': 1, 'worker_role': 'topology_' + role, 'role': role,
                 'real_ros': True, 'backend': 'ros2_public_api', 'domain_id': 81,
                 'observed_domain_id': 81, 'domain_observation_source': 'worker_environment',
                 'rmw_identifier': config['rmw']}
        def emit(kind, **fields):
            event = dict(token, event=kind, event_index=len(events), run_id=config['run_id'], **fields)
            events.append(event)
            return {'raw_path': str(raw), 'event_index': event['event_index']}
        start = emit('process_started', fresh_process=True, separate_context=True)
        created = emit('endpoint_created')
        exited = emit('process_exit', returncode=0)
        cleanup = emit('endpoint_destroyed', cleanup_status='closed')
        worker = {k: deepcopy(token[k]) for k in ('logical_id', 'generation', 'pid', 'pgid',
            'process_start_ticks', 'endpoint_gid', 'endpoint_generation', 'worker_generation', 'rmw_identifier')}
        worker.update(role=token['worker_role'], endpoint_role=role, fixture=False, real_ros=True,
            domain_id=81, rmw=config['rmw'], domain_observation_source='worker_environment',
            execution_domain_id=81, fresh_process=True, separate_context=True, closed=True,
            returncode=0, cleanup_status='closed', unknowns=[], token_raw_ref=created,
            start_raw_refs=[start], exit_raw_refs=[exited], cleanup_raw_refs=[cleanup])
        workers.append(worker)
    result['execution_metadata']['workers'] = workers
    result['events'] = events
    result['raw_path'] = str(raw)
    return config, result


def test_n_worker_raw_reference_positive_is_not_execution_qualification(tmp_path):
    config, result = worker_artifacts(tmp_path / 'workers')
    assert replay._topology_worker_errors(config, result) == []
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': True, 'execution_stage': 'judged'}, output_root=tmp_path / 'workers')
    assert qualified['real_ros_execution'] is False
    assert 'offline_fixture_cannot_establish_real_ros' in qualified['qualification_errors']


@pytest.mark.parametrize('damage', ['missing_worker', 'duplicate_worker', 'foreign_ref', 'ref_kind',
    'pid', 'gid', 'generation', 'rmw', 'domain', 'exit_code', 'missing_exit', 'unreferenced_exit',
    'fresh_context', 'unknown_cleanup', 'raw_lineage', 'raw_exit_identity'])
def test_n_worker_lineage_tamper_not_pair_count_or_boolean_proof(tmp_path, damage):
    config, result = worker_artifacts(tmp_path / 'workers')
    workers = result['execution_metadata']['workers']; worker = workers[0]
    if damage == 'missing_worker': workers.pop()
    elif damage == 'duplicate_worker': workers[1] = deepcopy(worker)
    elif damage == 'foreign_ref': worker['token_raw_ref']['raw_path'] = '/outside/raw.jsonl'
    elif damage == 'ref_kind': worker['token_raw_ref']['event_index'] = worker['start_raw_refs'][0]['event_index']
    elif damage == 'pid': worker['pid'] += 1
    elif damage == 'gid': worker['endpoint_gid'] = 'another-gid'
    elif damage == 'generation': worker['generation'] += 1
    elif damage == 'rmw': worker['rmw_identifier'] = 'other-rmw'
    elif damage == 'domain': worker['domain_id'] += 1
    elif damage == 'exit_code': worker['returncode'] = 9
    elif damage == 'missing_exit': worker['exit_raw_refs'] = []
    elif damage == 'unreferenced_exit':
        extra = deepcopy(result['events'][worker['exit_raw_refs'][0]['event_index']])
        extra['event_index'] = len(result['events']); result['events'].append(extra)
    elif damage == 'fresh_context': worker['separate_context'] = False
    elif damage == 'unknown_cleanup': worker['cleanup_status'] = 'unknown'
    elif damage == 'raw_lineage': result['events'][worker['start_raw_refs'][0]['event_index']]['generation'] += 1
    else: result['events'][worker['exit_raw_refs'][0]['event_index']]['process_start_ticks'] += 1
    # Even if an outer writer rehashes everything, semantic lineage must fail.
    assert replay._topology_worker_errors(config, result)


@pytest.mark.parametrize('kind', ['worker_exit', 'topology_worker_exit'])
@pytest.mark.parametrize('damage', ['none', 'identity', 'unreferenced'])
def test_worker_exit_public_receipt_scope_has_no_invented_pgid(tmp_path, kind, damage):
    config, result = worker_artifacts(tmp_path / 'workers')
    worker = result['execution_metadata']['workers'][0]
    exited = deepcopy(result['events'][worker['exit_raw_refs'][0]['event_index']])
    exited.update(event=kind, event_index=len(result['events']))
    if kind == 'worker_exit':
        exited.pop('pgid')
    if damage == 'identity':
        exited['process_start_ticks'] += 1
    result['events'].append(exited)
    if damage != 'unreferenced':
        worker['exit_raw_refs'].append({'raw_path': result['raw_path'], 'event_index': exited['event_index']})
    errors = replay._topology_worker_errors(config, result)
    assert bool(errors) is (damage != 'none')


@pytest.mark.parametrize('damage', ['cleanup_false', 'outer_failed', 'metadata_raw', 'metadata_disk', 'sealed_context'])
def test_full_qualification_keeps_topology_wrapper_failure_ineligible(tmp_path, damage):
    root = tmp_path / 'context'; config, result = context_artifacts(root)
    result.update(status='executed', real_execution=True,
                  cleanup_receipt={'closed': True, 'survivors': [], 'errors': []})
    result['events'].append({'event': 'topology_cleanup', 'event_index': len(result['events']),
        'run_id': config['run_id'], 'domain_id': config['domain_id'], 'receipt': deepcopy(result['cleanup_receipt'])})
    if damage == 'cleanup_false': result['cleanup_receipt']['closed'] = False
    elif damage == 'outer_failed': result.update(status='failed', runtime_status='failed')
    elif damage == 'metadata_raw': result['events'][1]['metadata']['target_id'] = 'other'
    elif damage == 'metadata_disk':
        path = root / 'execution_metadata.json'; doc = json.loads(path.read_text()); doc['target_id'] = 'other'
        path.write_text(json.dumps(doc))
    else: result['execution_metadata']['sealed_execution_context']['effective_input_sha256'] = '0' * 64
    from r2f.core import raw_sha256
    raw = root / 'raw.jsonl'; raw.write_text(''.join(json.dumps(e) + '\n' for e in result['events']))
    result.update(raw_path=str(raw), raw_sha256=raw_sha256(raw))
    (root / 'runtime_result.json').write_text(json.dumps(result))
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': True, 'execution_stage': 'judged'}, output_root=root)
    assert qualified['real_ros_execution'] is False
    expected = {'cleanup_false': 'topology_cleanup_not_verified', 'outer_failed': 'topology_runtime_not_completed',
                'metadata_raw': 'raw_execution_metadata_mismatch', 'metadata_disk': 'persisted_execution_metadata_mismatch',
                'sealed_context': 'execution_metadata_sealed_context_mismatch'}[damage]
    assert expected in qualified['qualification_errors']
