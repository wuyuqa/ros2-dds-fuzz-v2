"""Shadow/offline-only topology input BFS. No ROS, runtime, netns or evidence."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.campaign.search_frontier import InputSearchFrontier
from r2f.campaign.topology_search import descriptor, enumerate_tasks, validate_task
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.core import Registry
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY

ROOT = Path(__file__).resolve().parents[1]
CYCLE = 'topology-authored'
SOURCE = {'source_repo': '/shadow-offline-only', 'head': 'shadow-baseline',
          'behavior_tree_sha256': 'e'*64, 'source_frozen': False,
          'dirty_behavior_paths': ['shadow-fixture']}


def profile():
    doc = json.loads((ROOT/'config/s1_fuzz/profiles/model-bfs-topology.json').read_text())
    doc['rmws'] = doc['rmws'][:1]
    doc['budget']['max_inputs'] = 128
    doc['budget']['max_seconds'] = 300
    doc['cycles'][0]['seed_specs'] = [s for s in doc['cycles'][0]['seed_specs'] if s['seed_id'] == 'topology.multi_endpoint']
    doc['seed_specs'] = deepcopy(doc['cycles'][0]['seed_specs'])
    doc['declared_seed_specs_union'] = deepcopy(doc['seed_specs'])
    return doc


def fresh(doc):
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=41, warmup=0, policy='bfs',
                                    sampling_kinds=doc.get('sampling_kinds'), sampling_weight=doc.get('sampling_weight'))
    scheduler.configure_search(doc['search'], descriptor(doc['topology_search'], doc['operator_whitelist']))
    return dict(corpus=corpus, feedback=feedback, scheduler=scheduler, continuation=None)


def run(out, doc, state=None, maximum=6, wrapper=None):
    state = state or fresh(doc)
    hook = make_checkpoint_hook(out, profile=doc, cycle_id=CYCLE, source_manifest=SOURCE,
                               continuation=state['continuation'])
    if wrapper:
        hook = wrapper(hook)
    runtime = Mock(side_effect=AssertionError('runtime forbidden'))
    with patch('r2f.campaign.runner.source_manifest', return_value=deepcopy(SOURCE)):
        result = run_exploratory_cycle(out, **state, profile=doc, max_inputs=maximum,
            checkpoint_hook=hook, execute=False, runtime=runtime, run_id=out.name)
    runtime.assert_not_called()
    return result


def restore(path, out, doc):
    return load_checkpoint(path, profile=doc, cycle_id=CYCLE, source_manifest=SOURCE, output_dir=out)


def projection(result):
    return [(r.get('content_sha256'), r.get('mutation_plan'), r['input_search']) for r in result['rows']]


def test_explicit_catalog_applicability_is_not_coverage():
    doc = profile(); state = fresh(doc); frontier = state['scheduler'].frontier
    ledger = next(iter(frontier.nodes.values()))['sampling_applicability']
    assert len(ledger) == 31
    assert len({row['operator_id'] for row in ledger}) == 21
    assert len({row['slot_id'] for row in ledger}) == 11
    mismatches = [row for row in ledger if row['reason'] == 'operator_slot_kind_mismatch']
    assert {(r['operator_id'], r['slot_id']) for r in mismatches} == {
        ('action.delete','S1.topology.dependency'),('action.swap','S1.topology.dependency'),
        ('lease.value','S1.topology.qos.deadline')}
    assert all(not r['operator_id'].startswith('env.reorder') for r in ledger)
    assert any(r['reason'] == 'handler_not_supported_in_first_slice' for r in ledger)
    assert all(t['task_kind'] == 'topology_request' and t['mutation_request']['slot_id'] == t['slot_id'] for t in frontier.tasks)


def test_shared_runner_exact_request_capsule_and_no_runtime(tmp_path):
    doc = profile()
    doc['topology_search']['samples'] = [s for s in doc['topology_search']['samples'] if (s['operator_id'],s['slot_id'],s['class_id']) in {
        ('action.insert','S1.topology.actions','handler_rng_v1'),('endpoint.count','S1.topology.endpoints','add'),
        ('filter.expr','S1.topology.filter','invalid'),('relation.pair','S1.topology.qos.reliability','BB')}]
    result = run(tmp_path/'plan', doc, maximum=4)
    assert not result['abort']
    assert result['budget']['actual'] == result['budget']['attempted'] == 0
    assert result['feedback']['events'] == []
    assert len(result['rows']) == 4
    tasks = result['selection_log']['frontier']['tasks']
    for row in result['rows']:
        task = next(t for t in tasks if t['task_id'] == row['input_search']['task_id'])
        assert row['input_search']['mutation_request'] == task['mutation_request']
        assert row['actual_run'] is False and row['input_search']['execution'] is None
        if row.get('mutation_plan'):
            op = row['mutation_plan']['operations'][0]
            assert op['parameters']['request']['operator_id'] == task['operator_id']
            assert op['parameters']['request']['slot_id'] == task['slot_id']
            assert op['parameters']['request']['context'] == task['mutation_request']['context']
            if row.get('generation_status') != 'rejected':
                capsule = json.loads((tmp_path/'plan/cases'/row['cell_id']/'capsule.json').read_text())
                assert capsule['execution'] is None


def test_checkpoint_resume_equal_to_continuous(tmp_path):
    doc = narrowed(); doc['search']['max_depth'] = 3
    full = run(tmp_path/'full', doc, maximum=3)
    part = run(tmp_path/'part', doc, maximum=1)
    state = restore(tmp_path/'part/checkpoint.json',tmp_path/'resume',doc)
    resumed = run(tmp_path/'resume',doc,state,maximum=3)
    assert projection(part)+projection(resumed) == projection(full)
    assert resumed['selection_log'] == full['selection_log']


def test_chain_length_rejected_not_silently_truncated():
    from r2f.campaign.exploratory_input import validate_profile_fields
    doc = profile(); doc['chain_length'] = 2
    with pytest.raises(ValueError,match='chain_length=1'):
        validate_profile_fields(doc)


def test_no_legacy_schema_migration():
    state = fresh(profile())['scheduler'].frontier.to_dict()
    state['schema'] = 'r2f.input-search-frontier/v1'
    with pytest.raises(ValueError,match='migrate'):
        InputSearchFrontier.from_dict(state)


def test_unconsumed_context_and_delivery_operator_refused():
    doc = profile(); samples = doc['topology_search']['samples']
    samples[0]['context']['delay_sec'] = 10
    with pytest.raises(ValueError,match='selector_not_consumed'):
        descriptor(doc['topology_search'],doc['operator_whitelist'])
    doc = profile(); doc['topology_search']['samples'][0]['operator_id'] = 'env.drop'
    with pytest.raises(ValueError,match='undeclared'):
        descriptor(doc['topology_search'],doc['operator_whitelist'])


def test_full_descriptor_drift_rejected():
    doc = profile(); f = fresh(doc)['scheduler'].frontier.to_dict()
    f['topology_descriptor']['sampler'] = 'tampered'
    with pytest.raises(ValueError,match='descriptor'):
        InputSearchFrontier.from_dict(f)


def narrowed(op='queue.depth', slot='S1.topology.qos.depth', context=None, seed='topology.multi_endpoint'):
    doc = profile()
    doc['cycles'][0]['seed_specs'] = [s for s in json.loads((ROOT/'config/s1_fuzz/profiles/model-bfs-topology.json').read_text())['seed_specs'] if s['seed_id'] == seed]
    doc['seed_specs'] = deepcopy(doc['cycles'][0]['seed_specs'])
    doc['declared_seed_specs_union'] = deepcopy(doc['seed_specs'])
    doc['topology_search']['samples'] = [dict(operator_id=op, slot_id=slot, class_id='explicit',
                                             context=context if context is not None else {'boundary_class':'neighbor'})]
    return doc


def registry_for_topology():
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability=TOPOLOGY_CAPABILITY)
    return registry


def generate_one(state, doc):
    from r2f.campaign.runner import _build_case_plan, _TopologyInputRejection, _NoEffectiveInputRejection
    from r2f.campaign.search_frontier import derived_record
    scheduler = state['scheduler']; selection = scheduler.next(); entry = selection['entry']
    target = registry_for_topology().get_target(entry['target_id']); contract = target.exploration_contract
    try:
        # Random generation/fallback must never be consulted in the explicit path.
        with patch('r2f.campaign.runner._topology_generation', side_effect=AssertionError('fallback forbidden')):
            plan = _build_case_plan(entry,target,contract,contract.surface_by_id[entry['surface_id']],
                    entry['final_config'],entry['mutator_id'],doc,selection['rng_seed'],selection)['plan']
    except _TopologyInputRejection as exc:
        scheduler.frontier.generation_rejected(selection['task_id'],str(exc),exc.attempts)
        scheduler.frontier.finish(selection['task_id'],rejected=True,reason='topology_input_rejected:'+str(exc))
        return selection, None
    except _NoEffectiveInputRejection as exc:
        plan = exc.plan
    record = derived_record(entry,plan)
    scheduler.frontier.planned(selection['task_id'],record)
    outcome = scheduler.frontier.task(selection['task_id'])['generation_outcome']
    rejected = outcome != 'new_input'
    scheduler.frontier.finish(selection['task_id'],rejected=rejected,reason=outcome if rejected else None)
    return selection, record


def test_derived_parent_uses_previous_final_config_and_depth_is_not_chain():
    from r2f.core.evidence import canonical_hash
    doc = narrowed(); state = fresh(doc)
    first, child = generate_one(state,doc)
    second, grandchild = generate_one(state,doc)
    assert child and grandchild
    assert second['parent_content_sha256'] == child['content_sha256']
    assert second['entry']['final_config'] == child['final_config']
    assert grandchild['mutation_plan']['parent_content_sha256'] == canonical_hash(child['final_config'])
    assert second['derivation_depth'] == 2
    assert len(grandchild['operations']) == 1
    assert state['corpus'].entries.keys() == {first['parent_content_sha256']}
    frontier = InputSearchFrontier.from_dict(state['scheduler'].frontier.to_dict())
    frontier.validate_model(profile=doc,seeds=state['corpus'],registry=registry_for_topology())


def test_feedback_cannot_affect_topology_jobs_or_programs():
    doc = narrowed(); a,b = fresh(doc),fresh(doc)
    for i in range(2):
        b['feedback'].record({'target_id':'offline-fixture','content_sha256':f'{i:064x}',
                              'surface_id':'S1.topology.program','boundary_class':'offline',
                              'execution_stage':'planned','actual_run':False})
        b['scheduler'].advance_feedback(b['feedback'])
        left,lc = generate_one(a,doc); right,rc = generate_one(b,doc)
        assert {k:left[k] for k in ('task_id','rng_seed','mutation_request')} == {k:right[k] for k in ('task_id','rng_seed','mutation_request')}
        assert lc == rc


def test_duplicate_guard_and_no_change_never_submit_or_expand():
    doc = narrowed(context={'boundary_class':'one'},seed='topology.writer_only')
    doc['search'].update(samples_per_class=2,max_depth=1)
    state = fresh(doc)
    generate_one(state,doc); selection,_ = generate_one(state,doc)
    task = state['scheduler'].frontier.task(selection['task_id'])
    assert task['generation_outcome'] == 'duplicate_content'
    assert len(state['scheduler'].frontier.nodes) == 2
    with pytest.raises(ValueError):
        state['scheduler'].frontier.submitted(task['task_id'])
    doc = narrowed('filter.expr','S1.topology.filter',{'filter_variant':'invalid'})
    state = fresh(doc); selection,record = generate_one(state,doc)
    assert record['executable'] is False and record['case_kind'] == 'guard_negative'
    task = state['scheduler'].frontier.task(selection['task_id'])
    assert task['generation_outcome'] == 'guard_terminal' and task['status'] == 'rejected'
    assert len(state['scheduler'].frontier.nodes) == 1
    with pytest.raises(ValueError):
        state['scheduler'].frontier.submitted(task['task_id'])
    doc = narrowed('relation.pair','S1.topology.qos.reliability',{'pair':'BB'})
    state = fresh(doc); selection,record = generate_one(state,doc)
    task = state['scheduler'].frontier.task(selection['task_id'])
    assert record is None and task['status'] == 'rejected'
    assert task['task_applicability']['status'] == 'not_applicable'
    assert task['task_applicability']['reason'] == 'generation_no_change'
    assert len(state['scheduler'].frontier.nodes) == 1


def test_rejection_is_regenerated_not_just_hash_checked():
    doc = narrowed('relation.pair','S1.topology.qos.reliability',{'pair':'BB'})
    state = fresh(doc); generate_one(state,doc)
    original = state['scheduler'].frontier.to_dict()
    rebuilt = InputSearchFrontier.prefix(original,0)
    for event in deepcopy(original['journal']):
        if event['action'] == 'generation_rejected':
            event['reason'] = 'forged refusal'
        if event['action'] == 'finish':
            event['reason'] = 'topology_input_rejected:forged refusal'
        rebuilt.apply_event(event)
    forged = InputSearchFrontier.from_dict(rebuilt.to_dict())
    with pytest.raises(ValueError,match='rejection not model-generatable'):
        forged.validate_model(profile=doc,seeds=state['corpus'],registry=registry_for_topology())


def test_capacity_and_task_bounds_retain_work():
    doc = narrowed(); doc['search']['max_nodes'] = 2
    state = fresh(doc); generate_one(state,doc)
    f = state['scheduler'].frontier
    assert f.stop_reason == 'input_search_max_nodes_reached_work_retained'
    assert any(t['status'] == 'pending' for t in f.tasks)
    assert InputSearchFrontier.from_dict(f.to_dict()).to_dict() == f.to_dict()
    doc = profile(); doc['search']['max_tasks'] = 1
    f = fresh(doc)['scheduler'].frontier
    assert f.stop_reason == 'input_search_max_tasks_reached_work_retained'
    assert all(n['expansion_status'] == 'task_budget_blocked' for n in f.nodes.values())


def test_model_tampered_request_cannot_change_slot_or_operator():
    doc=profile(); f=fresh(doc)['scheduler'].frontier
    task=f.select(); parent=f.entry(task)
    task['mutation_request']['slot_id'] = 'S1.topology.environment'
    with pytest.raises(ValueError,match='explicit model request'):
        validate_task(task,parent,f.topology_descriptor)


def test_shipped_profile_roots_exclude_guard_seeds_on_restore():
    doc=json.loads((ROOT/'config/s1_fuzz/profiles/model-bfs-topology.json').read_text())
    state=fresh(doc); f=state['scheduler'].frontier
    assert len(f.roots) == len(state['corpus'].selectable()) < len(state['corpus'].entries)
    InputSearchFrontier.from_dict(f.to_dict()).validate_model(profile=doc,seeds=state['corpus'],registry=registry_for_topology())


def test_pending_plan_resume_has_no_second_selection_or_submission(tmp_path):
    doc = narrowed(); doc['search']['max_depth'] = 3
    class Interrupted(Exception):
        pass
    def wrap(hook):
        def callback(payload):
            hook(payload)
            if payload['phase'] == 'planned' and payload['pending_selection']['derivation_depth'] == 2:
                raise Interrupted()
        return callback
    with pytest.raises(Interrupted):
        run(tmp_path/'interrupted',doc,maximum=3,wrapper=wrap)
    state=restore(tmp_path/'interrupted/checkpoint.json',tmp_path/'resume',doc)
    assert state['continuation']['pending_selection']['derivation_depth'] == 2
    result=run(tmp_path/'resume',doc,state,maximum=3)
    assert len(result['selection_log']['selections']) == 3
    assert result['budget']['actual'] == result['budget']['attempted'] == 0
    assert not any(e['action'] == 'submit' for e in result['selection_log']['frontier']['journal'])


def test_topology_never_uses_f1_sampling_aliases():
    from r2f.campaign.exploratory_input import validate_profile_fields
    doc=profile(); doc['sampling_kinds']=['representative']
    with pytest.raises(ValueError,match='explicit classes'):
        validate_profile_fields(doc)
