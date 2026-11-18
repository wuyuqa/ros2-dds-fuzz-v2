"""Unified cycle tests using real registry/provider and explicit offline backend."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign import runner
from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler, required_catalog_hash
from r2f.core import Registry, ReplayCapsule, Scenario, canonical_hash
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend

REPO = Path(__file__).resolve().parents[1]
SOURCE = {'source_repo': str(REPO), 'head': 'a' * 40, 'behavior_tree_sha256': 'b' * 64,
          'source_frozen': True, 'dirty_behavior_paths': []}
CYCLE = 'topology-authored'


def profile(depth=2, *, seeds=('topology.multi_endpoint',), operators=('action.insert',)):
    doc = json.loads((REPO / 'config/s1_fuzz/profiles/topology-chains.json').read_text())
    specs = [s for s in doc['seed_specs'] if s['seed_id'] in seeds]
    assert len(specs) == len(seeds)
    doc.update(chain_length=depth, operator_whitelist=list(operators),
               seed_specs=deepcopy(specs), declared_seed_specs_union=deepcopy(specs))
    doc['cycles'][0]['seed_specs'] = deepcopy(specs)
    doc['budget'] = {'max_inputs': 4, 'max_seconds': 300}
    return doc


def fresh(doc, seed=17):
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    feedback = FeedbackLog()
    return {'corpus': corpus, 'feedback': feedback,
            'scheduler': ExploratoryScheduler(corpus, feedback, rng_seed=seed, warmup=0),
            'continuation': None}


def offline_runtime(captured):
    def invoke(config, directory):
        captured.append(deepcopy(config))
        with patch('r2f.modules.s1.topology_input.make_topology_plan', side_effect=AssertionError('runtime remutation')), \
             patch('r2f.modules.s1.fuzz_mutators_v2.mutate', side_effect=AssertionError('runtime remutation')):
            return run_pair(config, directory, topology_backend=OfflineTopologyBackend())
    return Mock(side_effect=invoke)


def run_cycle(out, doc, *, state=None, runtime=None, maximum=1, checkpoint_hook=None):
    state = state or fresh(doc)
    with patch.object(runner, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch('r2f.reports.run_report.source_manifest', return_value=deepcopy(SOURCE)):
        return runner.run_exploratory_cycle(
            out, **state, profile=doc, max_inputs=maximum, runtime=runtime,
            execute=runtime is not None, run_id=out.name, domain_id=80,
            checkpoint_hook=checkpoint_hook)


def capsule_from_registry(depth=2):
    doc = profile(depth)
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    seed = corpus.selectable()[0]
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability=TOPOLOGY_CAPABILITY)
    target = registry.get_target(TOPOLOGY_TARGET)
    plan = make_topology_plan(seed['final_config']['topology_program'],
                              [{'operator_id': 'action.insert'}] * depth, rng_seed=7)
    scenario = Scenario(target, seed['rmw'], 'mutated', 0, 7,
                        {'mutation_kind': 'endpoint_program', 'carrier': 'ros2_endpoint',
                         'mutation_parameters': plan.final_config}, plan)
    prepared = registry.get_services(TOPOLOGY_TARGET).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
        final_input=plan.final_config, input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
    return registry, scenario, plan, capsule


@pytest.mark.parametrize('depth', [1, 2, 4])
def test_unified_plan_real_provider_capsule_and_every_layer_changes(tmp_path, depth):
    before = required_catalog_hash()
    result = run_cycle(tmp_path / ('plan-' + str(depth)), profile(depth))
    row = result['rows'][0]
    assert row['execution_stage'] == 'planned', row
    assert result['budget']['generated'] == 1
    assert result['budget']['attempted'] == 0
    capsule = ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
    plan = capsule.input['mutation_plan']
    assert len(plan['operations']) == depth
    assert set(capsule.input['effective_input']['final_input']) == {'topology_program'}
    assert 'topic' not in capsule.input and 'qos' not in capsule.input
    assert capsule.input['payload_layout']['schema'] == 'r2f.replay-action-program-payload/v1'
    receipts = capsule.input['effective_input']['input_layer_receipts']
    for index, receipt in enumerate(receipts):
        assert receipt['input_before'] != receipt['input_after']
        assert receipt['input_before_sha256'] == canonical_hash(receipt['input_before'])
        assert receipt['input_after_sha256'] == canonical_hash(receipt['input_after'])
        if index:
            assert receipt['input_before'] == receipts[index-1]['input_after']
    config = json.loads(Path(row['runtime_config_path']).read_text())
    from r2f.runtime.topology_route import validate_topology_route
    assert validate_topology_route(config).to_dict() == plan
    assert config['topology_program'] == plan['final_config']['topology_program']
    assert required_catalog_hash() == before
    from r2f.reports.run_report import audit_report
    audited = audit_report(result['report_path'])
    assert audited['evidence_valid'], audited
    assert audited['sealed'] is False and audited['coverage'][0]['judged'] == 0


def test_execute_single_offline_backend_preserves_input_no_candidate_novelty(tmp_path):
    doc = profile(4)
    state = fresh(doc)
    original_corpus, original_feedback = deepcopy(state['corpus'].to_dict()), deepcopy(state['feedback'].to_dict())
    captured = []
    runtime = offline_runtime(captured)
    result = run_cycle(tmp_path / 'offline', doc, state=state, runtime=runtime)
    runtime.assert_called_once()
    row = result['rows'][0]
    capsule = ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
    assert captured[0]['topology_program'] == capsule.input['effective_input']['final_input']['topology_program']
    assert captured[0]['effective_input_sha256'] == capsule.input['effective_input_sha256']
    assert row['candidate_eligible'] is False and row['feedback_eligible'] is False
    assert result['budget']['attempted'] == 1
    assert state['corpus'].to_dict() == original_corpus
    assert len(state['feedback'].events) == len(original_feedback['events']) + 1
    assert state['feedback'].covered_surfaces() == set() and not state['feedback'].edge_counts()
    event = state['feedback'].events[-1]
    assert event['complete'] is False and event['coverage'] is False and event['edge_id'] is None
    assert state['scheduler'].feedback_event_count == 1
    assert json.loads(Path(row['runtime_result_path']).read_text())['real_execution'] is False
    from r2f.reports.run_report import audit_report
    with patch('r2f.reports.run_report.source_manifest', return_value=deepcopy(SOURCE)):
        audited = audit_report(result['report_path'])
    assert audited['evidence_valid'], audited
    assert audited['sealed'] is False and audited['coverage'][0]['judged'] == 0
    ledger = json.loads((tmp_path / 'offline/candidate_ledger.json').read_text())
    assert ledger['natural_candidate_count'] == 0 and ledger['execution_exclusions']


@pytest.mark.parametrize('damage', ['capability', 'operator', 'chain', 'mutator'])
def test_profile_rejects_before_selection_and_output(tmp_path, damage):
    doc = profile()
    state = fresh(doc)
    if damage == 'capability': doc.pop('runtime_capability')
    elif damage == 'operator': doc['operator_whitelist'] = ['unknown.operator']
    elif damage == 'chain': doc['chain_length'] = 5
    else: doc['mutator_whitelist'] = ['M01']
    with pytest.raises(ValueError):
        run_cycle(tmp_path / damage, doc, state=state)
    assert not (tmp_path / damage).exists()
    assert not state['scheduler'].selection_log


def test_deterministic_requests_and_named_generation_failure(tmp_path):
    doc = profile(2, operators=('action.insert', 'timing.shift'))
    left = run_cycle(tmp_path / 'left', doc)['rows'][0]
    right = run_cycle(tmp_path / 'right', doc)['rows'][0]
    assert left['mutation_plan'] == right['mutation_plan']
    assert left['topology_generation'] == right['topology_generation']
    # A known operator with no applicable slot must reject, not fallback M01.
    doc = profile(1, seeds=('topology.empty_roles',), operators=('queue.depth',))
    result = run_cycle(tmp_path / 'no-slot', doc)
    row = result['rows'][0]
    assert row['generation_status'] == 'rejected', row
    assert 'topology_input_rejected' in row['reason_code']
    assert row['topology_generation_attempts']
    assert result['budget']['attempted'] == 0


def test_guard_seed_retained_unselectable_and_not_natural(tmp_path):
    doc = profile(seeds=('topology.multi_endpoint', 'topology.structure_duplicate',
                         'topology.dependency_missing', 'topology.environment_fault'))
    state = fresh(doc)
    guards = [r for r in state['corpus'].entries.values() if r['case_kind'] == 'guard_negative']
    assert guards and all(r['executable'] is False and r['candidate_eligible'] is False for r in guards)
    result = run_cycle(tmp_path / 'guards', doc, state=state)
    assert result['rows'][0]['seed_id'] == 'topology.multi_endpoint'
    assert all(r['executable'] is False for r in guards)


def test_checkpoint_resume_same_selection_program_plan_and_input_hash(tmp_path):
    from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
    doc = profile(2, operators=('action.insert', 'timing.shift'))
    whole = run_cycle(tmp_path / 'whole', doc, maximum=2)
    first_root = tmp_path / 'first'
    hook = make_checkpoint_hook(first_root, profile=doc, cycle_id=CYCLE, source_manifest=SOURCE)
    first = run_cycle(first_root, doc, maximum=1, checkpoint_hook=hook)
    original = {str(p.relative_to(first_root)): p.read_bytes() for p in first_root.rglob('*') if p.is_file()}
    restored = load_checkpoint(first_root, profile=doc, cycle_id=CYCLE,
                               source_manifest=SOURCE, output_dir=tmp_path / 'second')
    state = {key: restored[key] for key in ('corpus', 'feedback', 'scheduler', 'continuation')}
    second = run_cycle(tmp_path / 'second', doc, state=state, maximum=2)
    def inputs(result):
        return [(r['mutation_plan'], r['final_input'], r['effective_input_sha256']) for r in result['rows']]
    assert inputs(whole) == inputs(first) + inputs(second)
    assert second['selection_log'] == whole['selection_log']
    assert second['budget']['attempted'] == 0
    assert original == {str(p.relative_to(first_root)): p.read_bytes() for p in first_root.rglob('*') if p.is_file()}


def test_exact_mock_canonical_callable_still_offline_fixture_not_real(tmp_path):
    import r2f.runtime.ros2_pair as pair_module
    runtime = offline_runtime([])
    with patch.object(pair_module, 'run_pair', runtime):
        result = run_cycle(tmp_path / 'canonical-fixture', profile(1), runtime=runtime)
    assert result['runtime_formal'] is True
    row = result['rows'][0]
    assert row['candidate_eligible'] is False and row['seal_eligible'] is False
    assert row['execution_qualification']['real_ros_execution'] is False
    assert any('fixture' in e for e in row['execution_qualification']['qualification_errors'])
    assert not result['feedback']['edge_counts']


def test_topology_formal_source_incomplete_aborts_before_runtime(tmp_path):
    import r2f.runtime.ros2_pair as pair_module
    runtime = offline_runtime([])
    state = fresh(profile(1))
    with patch.object(pair_module, 'run_pair', runtime), \
         patch.object(runner, 'source_manifest', return_value=dict(SOURCE, head=None)):
        result = runner.run_exploratory_cycle(tmp_path / 'unfrozen', **state, profile=profile(1),
            runtime=runtime, execute=True, max_inputs=1, domain_id=80)
    runtime.assert_not_called()
    assert result['budget']['attempted'] == 0 and result['rows'] == []
    assert result['abort']['reason_code'] == 'source_behavior_not_tracked_and_clean_at_HEAD'
