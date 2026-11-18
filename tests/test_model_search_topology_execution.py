"""Topology BFS admission contracts. Injected/offline results are never real DDS.

No test starts ROS, namespaces, a campaign process, or a canonical runtime.
"""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign import runner
from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend
from test_model_search_topology import narrowed, fresh, SOURCE, CYCLE

ROOT = Path(__file__).resolve().parents[1]


def cycle(out, doc, *, runtime=None, state=None, execute=True, source=None,
          maximum=1, hook=None, boundary='host'):
    state = state or fresh(doc)
    with patch.object(runner, 'source_manifest', return_value=deepcopy(source or SOURCE)):
        return runner.run_exploratory_cycle(out, **state, profile=doc,
            runtime=runtime, execute=execute, max_inputs=maximum,
            run_id=out.name, runtime_boundary=boundary, checkpoint_hook=hook)


def injected(captured):
    def invoke(config, directory):
        captured.append(deepcopy(config))
        # Core composition already happened. The runtime must consume the exact
        # submitted plan, not mutate again or fall back to random generation.
        with patch('r2f.modules.s1.topology_input.make_topology_plan',
                   side_effect=AssertionError('runtime re-mutation')), \
             patch('r2f.modules.s1.fuzz_mutators_v2.mutate',
                   side_effect=AssertionError('runtime re-mutation')), \
             patch.object(runner, '_topology_generation',
                   side_effect=AssertionError('implicit topology fallback')):
            return run_pair(config, directory, topology_backend=OfflineTopologyBackend())
    return Mock(side_effect=invoke)


def assert_not_real(result):
    for row in result['rows']:
        assert row.get('candidate_eligible') is not True
        if 'execution_qualification' in row:
            assert row['execution_qualification']['real_ros_execution'] is False
            assert row['feedback_eligible'] is False and row['seal_eligible'] is False
    ledger = json.loads((Path(result['report_path']).parent/'candidate_ledger.json').read_text())
    assert ledger['natural_candidate_count'] == 0


def test_exact_model_payload_parent_inheritance_and_cumulative_budget(tmp_path):
    doc = narrowed()
    doc['search']['max_depth'] = 3
    state = fresh(doc)
    captured = []
    runtime = injected(captured)
    result = cycle(tmp_path/'execute', doc, state=state, runtime=runtime, maximum=3)
    assert result['abort'] is None
    assert runtime.call_count == result['budget']['attempted'] == 3
    frontier = result['selection_log']['frontier']
    assert len(result['selection_log']['selections']) == 3
    for row, config in zip(result['rows'], captured):
        plan = row['mutation_plan']
        capsule = json.loads(Path(row['replay_path']).read_text())
        assert config['topology_program'] == plan['final_config']['topology_program']
        assert capsule['input']['effective_input']['final_input'] == plan['final_config']
        assert config['effective_input_sha256'] == row['effective_input_sha256']
        assert config.get('allow_topology_execution') is not True
        assert row['topology_generation']['fallback'] is False
        assert row['topology_generation']['task_id'] == row['input_search']['task_id']
        task = next(t for t in frontier['tasks'] if t['task_id'] == row['input_search']['task_id'])
        assert row['topology_generation']['request'] == task['mutation_request']
    # A depth-three exact task must descend from the previous final config,
    # not reselect the root or a novelty-only corpus parent.
    assert [r['input_search']['derivation_depth'] for r in result['rows']] == [1, 2, 3]
    for previous, child in zip(result['rows'], result['rows'][1:]):
        assert child['parent_content_sha256'] == previous['content_sha256']
        capsule = json.loads(Path(child['replay_path']).read_text())
        receipt = capsule['input']['effective_input']['input_layer_receipts'][0]
        assert receipt['input_before'] == previous['mutation_plan']['final_config']['topology_program']
    assert_not_real(result)


@pytest.mark.parametrize('kind', ['guard', 'no_change', 'duplicate'])
def test_terminal_tasks_never_submit_or_invoke_runtime(tmp_path, kind):
    if kind == 'guard':
        doc = narrowed('filter.expr', 'S1.topology.filter', {'filter_variant': 'invalid'})
    elif kind == 'no_change':
        doc = narrowed('relation.pair', 'S1.topology.qos.reliability', {'pair': 'BB'})
    else:
        doc = narrowed('queue.depth', 'S1.topology.qos.depth', {'boundary_class': 'one'}, seed='topology.writer_only')
        doc['search'].update(samples_per_class=2, max_depth=1)
    state = fresh(doc)
    captured = []
    runtime = injected(captured)
    result = cycle(tmp_path/kind, doc, state=state, runtime=runtime,
                   maximum=2 if kind == 'duplicate' else 1)
    tasks = result['selection_log']['frontier']['tasks']
    task = next(t for t in tasks if t.get('generation_outcome') ==
                {'guard': 'guard_terminal', 'no_change': 'generation_rejected',
                 'duplicate': 'duplicate_content'}[kind])
    assert task['status'] == 'rejected'
    journal = result['selection_log']['frontier']['journal']
    assert not any(e['action'] == 'submit' and e['task_id'] == task['task_id'] for e in journal)
    assert runtime.call_count == (1 if kind == 'duplicate' else 0)
    assert result['budget']['attempted'] == runtime.call_count
    assert_not_real(result)


@pytest.mark.parametrize('damage', ['capability', 'descriptor', 'profile', 'policy'])
def test_explicit_topology_profile_gate_before_runtime_and_artifacts(tmp_path, damage):
    doc = narrowed()
    state = fresh(doc)
    if damage == 'capability': doc.pop('runtime_capability')
    elif damage == 'descriptor': doc['topology_search']['schema'] = 'unknown'
    elif damage == 'profile': doc['target_ids'] = ['P01.type_consistency']
    else: doc['scheduler_policy'] = 'fair'
    runtime = Mock(side_effect=AssertionError('invalid declaration reached runtime'))
    with pytest.raises(ValueError):
        cycle(tmp_path/'invalid', doc, state=state, runtime=runtime)
    runtime.assert_not_called()
    assert not (tmp_path/'invalid').exists()


@pytest.mark.parametrize('boundary', ['host', 'isolated_netns'])
@pytest.mark.parametrize('damage', ['not_frozen', 'dirty', 'head', 'behavior'])
def test_canonical_source_gate_still_precedes_submission(tmp_path, boundary, damage):
    source = dict(SOURCE, source_frozen=True, dirty_behavior_paths=[], head='a'*40,
                  behavior_tree_sha256='b'*64)
    if damage == 'not_frozen': source['source_frozen'] = False
    elif damage == 'dirty': source['dirty_behavior_paths'] = ['src/changed.py']
    elif damage == 'head': source['head'] = None
    else: source['behavior_tree_sha256'] = None
    canonical_tripwire = Mock(side_effect=AssertionError('source gate bypassed'))
    with patch.object(runner, 'runtime_for_boundary', return_value=canonical_tripwire):
        result = cycle(tmp_path/'source-gate', narrowed(), runtime=canonical_tripwire,
                       source=source, boundary=boundary)
    canonical_tripwire.assert_not_called()
    assert result['abort']['reason_code'] == 'source_behavior_not_tracked_and_clean_at_HEAD'
    assert result['budget']['attempted'] == result['budget']['actual'] == 0


def test_callable_and_isolated_identity_gates_before_any_artifacts(tmp_path):
    with pytest.raises(ValueError, match='runtime callable'):
        cycle(tmp_path/'missing', narrowed())
    wrapper = Mock(side_effect=AssertionError('isolated injected runtime invoked'))
    with pytest.raises(ValueError, match='canonical execution boundary runtime'):
        cycle(tmp_path/'isolated', narrowed(), runtime=wrapper, boundary='isolated_netns')
    wrapper.assert_not_called()
    assert not (tmp_path/'missing').exists() and not (tmp_path/'isolated').exists()


def test_source_drift_blocks_injected_submission_too(tmp_path):
    doc = narrowed()
    runtime = Mock(side_effect=AssertionError('source drift invoked runtime'))
    with patch.object(runner, 'source_manifest', side_effect=[deepcopy(SOURCE), dict(SOURCE, head='changed')]):
        result = runner.run_exploratory_cycle(tmp_path/'drift', **fresh(doc), profile=doc,
                    execute=True, runtime=runtime, max_inputs=1, run_id='drift')
    runtime.assert_not_called()
    assert result['rows'][0]['reason_code'] == 'source_repo_HEAD_or_behavior_tree_changed'
    assert result['budget']['attempted'] == 0


def test_planned_checkpoint_resume_submits_exact_pending_task_once(tmp_path):
    doc = narrowed(); doc['search']['max_depth'] = 3
    out = tmp_path/'interrupted'
    persist = make_checkpoint_hook(out, profile=doc, cycle_id=CYCLE, source_manifest=SOURCE)
    class Interrupted(Exception): pass
    def hook(payload):
        persist(payload)
        if payload['phase'] == 'planned' and payload['pending_selection']['derivation_depth'] == 2:
            raise Interrupted()
    with pytest.raises(Interrupted):
        cycle(out, doc, execute=False, maximum=3, hook=hook)
    resumed = tmp_path/'resume'
    state = load_checkpoint(out/'checkpoint.json', profile=doc, cycle_id=CYCLE,
                            source_manifest=SOURCE, output_dir=resumed)
    pending = deepcopy(state['continuation']['pending_selection'])
    captured = []; runtime = injected(captured)
    result = cycle(resumed, doc, state=state, runtime=runtime, maximum=3)
    assert runtime.call_count == result['budget']['attempted'] == 2
    assert result['budget']['selected'] == 3
    assert result['rows'][0]['input_search']['task_id'] == pending['task_id']
    submits = [e for e in result['selection_log']['frontier']['journal'] if e['action'] == 'submit']
    assert len(submits) == len({e['task_id'] for e in submits}) == 2
    assert_not_real(result)


@pytest.mark.parametrize('boundary', ['host', 'isolated_netns'])
def test_cli_execute_dispatch_is_intercepted_not_a_live_run(tmp_path, monkeypatch, boundary):
    spec = importlib.util.spec_from_file_location('bfs_runtime_shadow_cli', ROOT/'scripts/run_s1.py')
    cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
    monkeypatch.setattr(cli, 'source_manifest', lambda _: deepcopy(SOURCE))
    token = Mock(side_effect=AssertionError('CLI must not invoke any runtime'))
    resolver = Mock(return_value=token)
    monkeypatch.setattr(cli, 'runtime_for_boundary', resolver)
    class Intercepted(Exception): pass
    dispatch = Mock(side_effect=Intercepted())
    monkeypatch.setattr(cli, 'run_exploratory_cycle', dispatch)
    doc = narrowed(); profile_path = tmp_path/'profile.json'
    profile_path.write_text(json.dumps(doc))
    with pytest.raises(Intercepted):
        cli.main(['--output-dir', str(tmp_path/'cli'), '--exploratory-profile', str(profile_path),
                  '--cycle-id', CYCLE, '--execute', '--runtime-boundary', boundary])
    resolver.assert_called_once_with(boundary); token.assert_not_called()
    kwargs = dispatch.call_args.kwargs
    assert kwargs['execute'] is True and kwargs['runtime'] is token
    assert kwargs['runtime_boundary'] == boundary and kwargs['scheduler'].policy == 'bfs'
    assert kwargs['profile'] == doc
