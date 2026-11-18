"""Offline F2 input/capsule/continuation plumbing, never ROS qualification.

All output is private tmp_path. Runtime callables are rejecting/tool-error
fixtures. A deterministic source identity is injected only for pure scheduling
and source-drift tests; no live source/lock/domain gate is bypassed.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.campaign.corpus import Corpus, CorpusProfileError, available_cycles, corpus_from_profile
from r2f.campaign.exploratory_input import (
    F2_WINDOWS_TARGET as TARGET, F2_WINDOWS_CAPABILITY as CAPABILITY,
    apply_profile_controls, runtime_input, validate_profile_fields,
)
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import (
    _adapt_parent, _compose_recovery_inputs, _materialize_recovery_windows,
    _plan_surface_bindings, _route, run_exploratory_cycle,
)
from r2f.campaign.scheduler import ExploratoryScheduler, required_catalog_hash
from r2f.core import Registry
from r2f.core.evidence import INPUT_SCHEMA_V2, canonical_hash, effective_input_hash
from r2f.core.mutation_plan import MutationPlan, compose
from r2f.core.mutators import MUTATORS, make_rng, mutate, mutate_f2
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import register_exploratory

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / 'config/s1_fuzz/profiles/f2-windows.json'
SOURCE = {'source_repo': '/offline/f2-fixture', 'head': 'fixture-head',
          'behavior_tree_sha256': 'f' * 64, 'source_frozen': True,
          'dirty_behavior_paths': []}


def profile():
    return json.loads(PROFILE.read_text())


def narrowed(surface='E02', count=2):
    doc = profile()
    doc['rmws'] = ['rmw_cyclonedds_cpp']
    doc['budget'] = {'max_inputs': 4, 'warmup_inputs': 1}
    cycle = deepcopy(next(c for c in doc['cycles'] if c['surfaces'] == [surface]))
    cycle['seed_specs'] = [deepcopy(cycle['seed_specs'][0])]
    cycle['seed_specs'][0]['overrides']['logical_config']['environment_windows']['count'] = count
    doc.update(surfaces=[surface], cycles=[cycle], seed_specs=deepcopy(cycle['seed_specs']),
               mutator_whitelist={surface: ['M07' if surface == 'E02' else 'M06']})
    return doc


def state(doc):
    corpus = corpus_from_profile(doc, cycle_id=doc['cycles'][0]['cycle_id'])
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=17,
                                     warmup=doc['budget']['warmup_inputs'])
    return dict(corpus=corpus, feedback=feedback, scheduler=scheduler, continuation=None)


def cycle(out, doc, current=None, maximum=1, runtime=None):
    current = current or state(doc)
    hook = make_checkpoint_hook(out, profile=doc, cycle_id=doc['cycles'][0]['cycle_id'],
                                source_manifest=SOURCE, continuation=current['continuation'])
    with patch('r2f.campaign.runner.source_manifest', return_value=deepcopy(SOURCE)):
        return run_exploratory_cycle(out, **current, profile=doc, runtime=runtime,
            execute=runtime is not None, max_inputs=maximum, run_id=out.name,
            checkpoint_hook=hook)


def force_proposal(**values):
    """Choose an explicit mutator input for a fixture, NEVER supply observations."""
    def generate(parent, surface, rng, context=None, mutator_id=None):
        return mutate_f2(parent, surface, rng, dict(context or {}, **values), mutator_id)
    return patch('r2f.campaign.runner.mutate_f2', side_effect=generate)


def contract():
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability=CAPABILITY)
    return registry.get_target(TARGET).exploration_contract


def tree_bytes(folder):
    return {str(p.relative_to(folder)): p.read_bytes() for p in folder.rglob('*') if p.is_file()}


def test_profile_explicit_target_scope_and_legacy_templates_immutable():
    doc = profile()
    before = {s: (ROOT / ('config/s1_fuzz/seeds/' + s.lower() + '.json')).read_bytes()
              for s in ('E02', 'E03')}
    assert doc['enabled'] is False and doc['chain_length'] == 1
    validate_profile_fields(doc)
    assert all(c['buildable'] for c in available_cycles(doc))
    for c in doc['cycles']:
        corpus = corpus_from_profile(doc, cycle_id=c['cycle_id'])
        for entry in corpus.entries.values():
            assert entry['target_id'] == TARGET and entry['target_revision'] == 0
            assert entry['seed_template_capability']['status'] == 'capability_pending'
            assert entry['seed_template_capability']['f1_routable'] is False
            assert entry['seed_provenance_kind'] == 'template'
            assert entry['derived_from_execution'] is False
            assert entry['runtime_authorization']['observation_guards_discharged'] is False
            assert entry['runtime_authorization']['contract_hash'] == entry['contract_hash']
            assert entry['final_config']['runtime_capability'] == CAPABILITY
            assert entry['mutator_id'] == doc['mutator_whitelist'][entry['surface_id']][0]
        assert Corpus.from_dict(corpus.to_dict()).to_dict() == corpus.to_dict()
    for s, data in before.items():
        assert (ROOT / ('config/s1_fuzz/seeds/' + s.lower() + '.json')).read_bytes() == data
    legacy = Registry()
    register_exploratory(legacy, include_required=False)
    with pytest.raises(KeyError):
        legacy.get_target(TARGET)
    assert required_catalog_hash() == '947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6'


@pytest.mark.parametrize('damage', ['missing_cap', 'unknown_cap', 'target', 'surface', 'mutator',
                                    'chain', 'wide', 'enabled', 'policy', 'tolerance', 'controls'])
def test_profile_capability_cannot_widen_f1_or_other_f2_surfaces(damage):
    doc = profile()
    if damage == 'missing_cap': doc.pop('runtime_capability')
    elif damage == 'unknown_cap': doc['runtime_capability'] = 'f2_all'
    elif damage == 'target': doc['target_ids'] = ['discovery.interruption_recovery.fuzz_v1']
    elif damage == 'surface': doc['surfaces'].append('G01')
    elif damage == 'mutator': doc['mutator_whitelist']['E03'] = ['M07']
    elif damage == 'chain': doc['chain_length'] = 2
    elif damage == 'wide': doc['sampling_kinds'] = ['representative']
    elif damage == 'enabled': doc['enabled'] = True
    elif damage == 'policy': doc['anchor_timing_policy'] = 'earliest_match'
    elif damage == 'tolerance': doc['anchor_lateness_tolerance_sec'] = 3
    else: doc['reader_recreate_in_window'] = True
    with pytest.raises((ValueError, CorpusProfileError)):
        corpus_from_profile(doc, cycle_id=doc['cycles'][0]['cycle_id'])


def test_generation_only_and_default_mutator_api_remain_nonexecutable():
    assert 'M06' not in MUTATORS and 'M07' not in MUTATORS
    for sid in ('E02', 'E03', 'G01'):
        assert not fuzz_catalog.f2_generation_surface(sid).executable
    for sid in ('E02', 'E03'):
        surface = fuzz_catalog.f2_live_surface(sid)
        seed = fuzz_catalog.load_seeds()['seeds'][sid]
        with pytest.raises(ValueError):
            mutate(seed['logical_config'], surface, make_rng(0), mutator_id=surface.mutator_ids[0])
    doc = json.loads((ROOT / 'config/s1_fuzz/profiles/environment.json').read_text())
    doc['surfaces'].append('E02')
    with pytest.raises(CorpusProfileError, match='pending|F1'):
        corpus_from_profile(doc, cycle_id=doc['cycles'][0]['cycle_id'])


@pytest.mark.parametrize('sid,anchor,count',
    [('E02', 'fresh_match', n) for n in (1, 2, 3)] +
    [('E03', 'fresh_match', n) for n in (1, 2, 3)] +
    [('E03', 'previous_restore_confirmed', n) for n in (2, 3)])
def test_input_plan_capsule_hash_and_runtime_windows_are_one_input(tmp_path, count, sid, anchor):
    doc = narrowed(sid, count)
    controls = {'window_count': count, 'restore_interval_sec': 0.2} if sid == 'E02' else {
        'anchor': anchor, 'offset_sec': 0.1}
    with force_proposal(**controls):
        result = cycle(tmp_path / ('planned-%s-%s-%s' % (sid, count, anchor)), doc)
    assert not result['rejected']
    row = result['rows'][0]
    assert row['binding']['status'] == 'preserved'
    assert row['execution_stage'] == 'planned' and row['actual_run'] is False
    assert row['mutation_receipt']['hit'] is False
    plan = MutationPlan.from_dict(row['mutation_plan'])
    plan.validate(contract())
    op = plan.operations[0]
    assert op.applicability_status == 'needs_observation'
    assert op.parameters.get('anchor_observed') in (False, None)
    assert canonical_hash(plan.final_config) == op.input_after_sha256
    assert plan.final_config['anchor_timing_policy'] == 'f2_anchor_not_before_v1'
    assert plan.final_config['anchor_lateness_tolerance_sec'] == 0.5
    windows = plan.final_config['environment_windows']
    assert len(windows) == count
    assert plan.runtime_windows == [{'schema': 'r2f.runtime-window/v1', **w} for w in windows]
    assert [w['trigger_anchor'] for w in windows] == ['fresh_match'] + ['previous_restore_confirmed'] * (count-1)
    if sid == 'E03':
        affected = [i for i, w in enumerate(windows) if w['trigger_anchor'] == anchor]
        assert affected == ([0] if anchor == 'fresh_match' else list(range(1, count)))
        assert all(windows[i]['trigger_offset_sec'] == 0.1 for i in affected)
        assert row['submitted_parameters']['environment_windows.anchor'] == anchor
        assert row['submitted_parameters']['environment_windows.anchor_offset_sec'] == 0.1
    config = json.loads(Path(row['runtime_config_path']).read_text())
    capsule_path = Path(row['replay_path'])
    capsule_bytes = capsule_path.read_bytes()
    capsule = json.loads(capsule_bytes)
    submitted = capsule['input']['effective_input']['final_input']
    assert submitted == config['final_input'] == plan.final_config
    assert capsule['execution'] is None
    assert effective_input_hash(capsule['input']['effective_input'], schema=INPUT_SCHEMA_V2) == row['effective_input_sha256']
    assert runtime_input(submitted, backend='discovery_namespace') == plan.final_config
    assert capsule_path.read_bytes() == capsule_bytes
    assert result['budget']['executed'] == result['budget']['attempted'] == 0
    assert result['feedback']['events'] == []


def test_previous_restore_first_window_rejected_before_callback_without_fallback(tmp_path):
    doc = narrowed('E03', 1)
    runtime = Mock(side_effect=AssertionError('offline fixture callback must not run'))
    with force_proposal(anchor='previous_restore_confirmed', offset_sec=0.1):
        result = cycle(tmp_path / 'rejected-anchor', doc, runtime=runtime)
    runtime.assert_not_called()
    assert result['rows'][0]['reason_code'] == 'f2_input_rejected:f2_previous_restore_anchor_requires_multiple_windows'
    assert result['rows'][0]['mutation_plan'] is None
    assert result['budget']['attempted'] == result['budget']['generated'] == 0


@pytest.mark.parametrize("anchor,affected", [("fresh_match", [0]),
                                             ("previous_restore_confirmed", [1, 2])])
def test_reselecting_concrete_e03_child_keeps_windows_and_original_plan_immutable(tmp_path, anchor, affected):
    doc = narrowed('E03', 3)
    with force_proposal(anchor='fresh_match', offset_sec=0.1):
        result = cycle(tmp_path / 'parent-plan', doc)
    row = result['rows'][0]
    saved = tree_bytes(tmp_path / 'parent-plan')
    parent = deepcopy(row['final_input'])
    adapted, receipt = _adapt_parent(TARGET, _route(TARGET), parent)
    proposal = mutate_f2(adapted, contract().surface_by_id['E03'], make_rng(2),
                         {'anchor': anchor, 'offset_sec': 0.5}, 'M06')
    proposals, windows, _, _ = _compose_recovery_inputs(adapted, [proposal])
    plan = compose(adapted, proposals, contract(), rng_seed=2, runtime_windows=windows)
    final_windows = plan.final_config['environment_windows']
    for i, window in enumerate(final_windows):
        expected = 0.5 if i in affected else parent['environment_windows'][i]['trigger_offset_sec']
        assert window['trigger_offset_sec'] == expected
    _, submitted = _plan_surface_bindings(plan, contract())
    assert submitted['environment_windows.anchor'] == anchor
    assert submitted['environment_windows.anchor_offset_sec'] == 0.5
    damaged = MutationPlan.from_dict(plan.to_dict())
    damaged.final_config['environment_windows'][affected[-1]]['trigger_offset_sec'] = 0.6
    with pytest.raises(ValueError, match='affected_window_offset_binding_mismatch'):
        _plan_surface_bindings(damaged, contract())
    assert len(plan.final_config['environment_windows']) == 3
    assert receipt['f2_logical_parent_projection']['concrete_windows_preserved']
    assert canonical_hash(plan.final_config) == plan.operations[0].input_after_sha256
    assert saved == tree_bytes(tmp_path / 'parent-plan')
    assert parent == row['final_input']


def test_timing_policy_is_hashed_and_unobserved_controls_are_not_accepted():
    parent = deepcopy(fuzz_catalog.load_seeds()['seeds']['E03']['logical_config'])
    first = apply_profile_controls(parent, narrowed('E03'), backend='discovery_namespace')
    changed = dict(first, anchor_lateness_tolerance_sec=0.6)
    assert canonical_hash(first) != canonical_hash(changed)
    with pytest.raises(ValueError, match='timing_contract'):
        runtime_input(changed, backend='discovery_namespace')
    for key in ('anchor_observed', 'post_match_observed'):
        with pytest.raises(ValueError, match='observation_must_come_from_runtime'):
            runtime_input(dict(first, **{key: True}), backend='discovery_namespace')


def test_tool_error_flows_through_shared_feedback_checkpoint_without_execution_claim(tmp_path):
    doc = narrowed('E02')
    current = state(doc)
    runtime = Mock(side_effect=RuntimeError('offline_fixture_only:no_ROS_carrier'))
    with force_proposal(window_count=2, restore_interval_sec=0.2):
        result = cycle(tmp_path / 'fixture-failure', doc, current, runtime=runtime)
    assert runtime.call_count == 1
    assert result['rows'][0]['execution_stage'] == 'aborted'
    assert result['rows'][0]['actual_run'] is False
    assert result['budget']['attempted'] == 1 and result['budget']['actual'] == 0
    assert result['budget']['executed'] == 0
    assert len(current['feedback'].events) == current['scheduler'].feedback_event_count == 1
    assert current['feedback'].events[0]['coverage'] is False
    restored = load_checkpoint(tmp_path / 'fixture-failure/checkpoint.json', profile=doc,
        cycle_id=doc['cycles'][0]['cycle_id'], source_manifest=SOURCE, output_dir=tmp_path / 'new-output')
    assert restored['feedback'].to_dict() == current['feedback'].to_dict()
    assert restored['scheduler'].feedback_event_count == 1


def test_split_resume_continues_same_plans_warmup_sequence_and_preserves_old_evidence(tmp_path):
    doc = narrowed('E02')
    with force_proposal(window_count=3, restore_interval_sec=0.2):
        whole = cycle(tmp_path / 'whole', doc, maximum=4)
        first = cycle(tmp_path / 'first', doc, maximum=2)
        original = tree_bytes(tmp_path / 'first')
        restored = load_checkpoint(tmp_path / 'first/checkpoint.json', profile=doc,
            cycle_id=doc['cycles'][0]['cycle_id'], source_manifest=SOURCE, output_dir=tmp_path / 'second')
        second = cycle(tmp_path / 'second', doc, restored, maximum=4)
    fields = ('parent_content_sha256', 'content_sha256', 'final_input', 'mutation_plan')
    projection = lambda rows: [{k: r[k] for k in fields} for r in rows]
    assert projection(whole['rows']) == projection(first['rows'] + second['rows'])
    assert whole['selection_log'] == second['selection_log']
    assert original == tree_bytes(tmp_path / 'first')
    assert second['budget']['selected'] == 4 and second['budget']['actual'] == 0
    assert second['selection_log']['selections'][0]['phase'] == 'warmup'
    assert all(s['phase'] == 'exploratory' for s in second['selection_log']['selections'][1:])


@pytest.mark.parametrize('damage', ['source', 'profile', 'contract', 'payload', 'catalog'])
def test_checkpoint_rejects_old_or_rehashed_other_identity_without_migrating_evidence(tmp_path, damage):
    doc = narrowed('E02')
    with force_proposal(window_count=2, restore_interval_sec=0.2):
        cycle(tmp_path / 'old', doc)
    path = tmp_path / 'old/checkpoint.json'
    saved = path.read_bytes()
    data = json.loads(saved)
    source = deepcopy(SOURCE)
    if damage == 'source': source['behavior_tree_sha256'] = '0' * 64
    elif damage == 'profile': doc['profile_id'] += '-changed'
    elif damage == 'contract': data['identity']['target_manifest'][0]['target_revision'] += 1
    elif damage == 'catalog': data['identity']['catalog_digest'] = 'b' * 64
    else: data['state']['continuation']['budget']['generated'] += 1
    if damage in ('contract', 'catalog'):
        data['checkpoint_sha256'] = canonical_hash({k:v for k,v in data.items() if k!='checkpoint_sha256'})
    if damage not in ('source', 'profile'):
        path.write_text(json.dumps(data))
    original = path.read_bytes()
    with pytest.raises(ValueError, match='identity|hash'):
        load_checkpoint(path, profile=doc, cycle_id=doc['cycles'][0]['cycle_id'],
                        source_manifest=source, output_dir=tmp_path / 'refused')
    assert path.read_bytes() == original
    assert not (tmp_path / 'refused').exists()
    if damage in ('source', 'profile'):
        assert saved == path.read_bytes()


def test_f2_source_drift_never_submits_to_fixture_callback(tmp_path):
    doc = narrowed('E02')
    current = state(doc)
    changed = dict(SOURCE, behavior_tree_sha256='a' * 64)
    source_calls = []
    def source(_repo):
        source_calls.append(1)
        return deepcopy(SOURCE if len(source_calls) == 1 else changed)
    runtime = Mock(side_effect=AssertionError('source drift cannot call runtime'))
    with force_proposal(window_count=2, restore_interval_sec=0.2), patch(
            'r2f.campaign.runner.source_manifest', side_effect=source):
        result = run_exploratory_cycle(tmp_path / 'source-drift', **current,
            profile=doc, execute=True, runtime=runtime, max_inputs=1, run_id='source-drift')
    runtime.assert_not_called()
    assert result['rows'][0]['abort_reason'] == 'source_repo_HEAD_or_behavior_tree_changed'
    assert result['budget']['attempted'] == result['budget']['actual'] == 0


@pytest.mark.parametrize('count', [0, 4, True])
def test_profile_cannot_register_out_of_range_window_templates(count):
    doc = narrowed('E03', count)
    with pytest.raises(ValueError, match='count'):
        corpus_from_profile(doc, cycle_id=doc['cycles'][0]['cycle_id'])
