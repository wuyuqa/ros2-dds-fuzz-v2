"""Bounded ordered topology sequences; exclusively offline shared runner/CLI.

No runtime evidence is inferred from layer accounting. Representative small
frontiers use real checkpoint hooks; this is not a large-frontier stress test.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign.topology_search import (CHAINS_SCHEMA, CHAINS_FRONTIER_SCHEMA,
    descriptor, enumerate_tasks, generate, validate_task, plan_accounting)
from r2f.campaign.search_frontier import InputSearchFrontier, derived_record
from r2f.campaign.corpus import content_identity
from r2f.core.evidence import canonical_hash
from r2f.modules.s1.topology_input import make_topology_plan, validate_topology_plan
from test_model_search_topology import (fresh, run, restore, generate_one,
    registry_for_topology, profile as single_profile, narrowed)
from test_model_search_topology_cli import cli, arguments, read

ROOT = Path(__file__).resolve().parents[1]


def profile(length=2, *, classes=None, seed='topology.multi_endpoint', depth=1):
    doc = json.loads((ROOT/'config/s1_fuzz/profiles/model-bfs-topology-combinations.json').read_text())
    doc['chain_length'] = doc['topology_search']['chain_length'] = length
    doc['rmws'] = doc['rmws'][:1]
    specs = [s for s in doc['seed_specs'] if s['seed_id'] == seed]
    assert specs
    for holder, key in ((doc,'seed_specs'),(doc,'declared_seed_specs_union'),(doc['cycles'][0],'seed_specs')):
        holder[key] = deepcopy(specs)
    if classes is not None:
        doc['topology_search']['samples'] = [s for s in doc['topology_search']['samples'] if s['class_id'] in classes]
        assert len(doc['topology_search']['samples']) == len(classes)
    doc['search'].update(max_depth=depth, max_nodes=12, max_tasks=24, samples_per_class=1)
    doc['budget'].update(max_inputs=8, max_seconds=300)
    return doc


def multiface(length, depth=1):
    return profile(length, classes={['two','three','four'][length-2]+('.repeatable' if depth > 1 and length > 2 else '.multiface')}, depth=depth)


def dsc(doc):
    return descriptor(doc['topology_search'], doc['operator_whitelist'])


def project(result):
    return [(row.get('content_sha256'), row.get('effective_input_sha256'),
             row.get('mutation_plan'), row['input_search'], row['topology_chain_accounting'])
            for row in result['rows']]


@pytest.mark.parametrize('length', [2,3,4])
def test_bounded_schema_keeps_authored_order_and_all_pair_applicability(length):
    doc = profile(length); state = fresh(doc); frontier = state['scheduler'].frontier
    desc = dsc(doc)
    assert desc['schema'] == CHAINS_SCHEMA
    assert frontier.to_dict()['schema'] == CHAINS_FRONTIER_SCHEMA
    node = next(iter(frontier.nodes.values()))
    assert len(node['sampling_applicability']) == 31
    assert len({r['operator_id'] for r in node['sampling_applicability']}) == 21
    assert len({r['slot_id'] for r in node['sampling_applicability']}) == 11
    assert len(node['sequence_applicability']) == len(desc['samples']) == 13
    selected = [s for s in desc['samples'] if len(s['layers']) == length]
    assert len(frontier.tasks) == len(selected)  # no Cartesian expansion or truncation
    for task, sample in zip(frontier.tasks, selected):
        assert task['class_id'] == sample['class_id']
        assert task['layer_samples'] == sample['layers']
        assert task['declared_layer_count'] == len(task['mutation_requests']) == length
        assert task['mutation_requests'] == [{k:layer[k] for k in ('operator_id','slot_id','context')}
                                              for layer in sample['layers']]
    for item in node['sequence_applicability']:
        assert (item['status'] == 'applicable') == (item['declared_layers'] == length)
        if item['declared_layers'] != length:
            assert item['reason'] == 'profile_chain_length_not_selected'


@pytest.mark.parametrize('damage', ['length','bool','long','empty','class','selector','oldschema','class_conflict'])
def test_invalid_sequence_never_silently_downgrades(damage):
    doc = multiface(3); value = doc['topology_search']; layer = value['samples'][0]['layers'][0]
    if damage == 'length': value['chain_length'] = 1
    elif damage == 'bool': value['chain_length'] = True
    elif damage == 'long': value['samples'][0]['layers'] *= 2
    elif damage == 'empty': value['samples'] = []
    elif damage == 'class': value['samples'][0]['layers'][0].pop('class_id')
    elif damage == 'selector': layer['context']['endpoint_id'] = 'w0'
    elif damage == 'oldschema': value['schema'] = 'r2f.topology-search/operators-v1'
    elif damage == 'class_conflict':
        other = deepcopy(value['samples'][0]); other['class_id'] = 'conflict'
        other['layers'][0]['context']['boundary_class'] = 'one'; value['samples'].append(other)
    with pytest.raises(ValueError): dsc(doc)


@pytest.mark.parametrize('length', [2,3,4])
def test_exact_existing_planner_receipts_seeds_windows_and_no_fallback(length):
    doc = multiface(length); state = fresh(doc)
    with patch('r2f.campaign.runner._topology_generation', side_effect=AssertionError('shuffle forbidden')):
        selection, record = generate_one(state,doc)
    expected = make_topology_plan(selection['entry']['final_config']['topology_program'],
                                  selection['mutation_requests'], rng_seed=selection['rng_seed'])
    validate_topology_plan(expected)
    assert record['mutation_plan'] == expected.to_dict()
    assert record['final_config'] == expected.final_config
    assert record['runtime_windows'] == expected.runtime_windows
    assert record['operations'] == [o.to_dict() for o in expected.operations]
    assert len(record['operations']) == length
    current = selection['entry']['final_config']['topology_program']
    for index, op in enumerate(record['operations']):
        params = op['parameters']; receipt = params['input_layer_receipts'][0]
        assert params['request'] == selection['mutation_requests'][index]
        assert params['seed'] == receipt['seed'] == selection['rng_seed'] + index
        assert receipt['layer_index'] == index and receipt['input_before'] == current
        assert receipt['input_before_sha256'] == canonical_hash(current)
        assert receipt['execution_evidence'] is receipt['candidate_eligible'] is False
        current = receipt['input_after']
    assert current == expected.final_config['topology_program']
    task = state['scheduler'].frontier.task(selection['task_id'])
    counts = task['chain_accounting']
    assert [counts[k] for k in ('declared_layers','planned_layers','executable_layers','effective_layers')] == [length]*4
    assert counts['overridden_layers'] == 0 and counts['execution_evidence'] is False
    rebuilt = InputSearchFrontier.from_dict(state['scheduler'].frontier.to_dict())
    rebuilt.validate_model(profile=doc,seeds=state['corpus'],registry=registry_for_topology())


@pytest.mark.parametrize('length', [2,3,4])
def test_bfs_previous_final_parent_not_ancestor_concatenation(length):
    doc = multiface(length,depth=2); state = fresh(doc)
    first,child = generate_one(state,doc); second,grandchild = generate_one(state,doc)
    assert child and grandchild
    assert second['entry']['final_config'] == child['final_config']
    assert second['parent_content_sha256'] == child['content_sha256']
    assert grandchild['mutation_plan']['parent_content_sha256'] == canonical_hash(child['final_config'])
    assert second['derivation_depth'] == 2
    assert len(grandchild['operations']) == length  # never length * derivation_depth
    assert set(state['corpus'].entries) == {first['parent_content_sha256']}


@pytest.mark.parametrize('class_id,outcome', [('two.same_slot','no_effective_input'),
    ('two.repeated_value','generation_rejected')])
def test_single_endpoint_same_slot_cancellation_and_nochange_are_terminal(class_id,outcome):
    doc = profile(2,classes={class_id},seed='topology.writer_only'); state = fresh(doc)
    selection,record = generate_one(state,doc); frontier = state['scheduler'].frontier
    task = frontier.task(selection['task_id']); counts = task['chain_accounting']
    assert task['generation_outcome'] == outcome and task['status'] == 'rejected'
    assert len(frontier.nodes) == 1 and counts['declared_layers'] == 2
    if record:
        assert record['final_config'] == selection['entry']['final_config']
        assert counts['effective_layers'] == 0
        assert counts['noop_reason'] == 'unwindowed compile_time mutation plan has no net input change'
    else:
        assert task['rejection']['reason'] == 'operator produced identical content'
        assert counts['attempted_layers'] == 2 and counts['failure']['layer_index'] == 1
        assert counts['prefix_diagnostic_only'] and counts['full_sequence_rejected']
    with pytest.raises(ValueError): frontier.submitted(task['task_id'])


@pytest.mark.parametrize('length,class_id,index', [(2,'two.guard_prefix',0),
    (3,'three.guard_middle',1),(4,'four.guard_middle',1)])
def test_intermediate_guard_keeps_exact_failure_and_unattempted_suffix(length,class_id,index,tmp_path):
    doc = profile(length,classes={class_id}); result = run(tmp_path/'guard',doc,maximum=1)
    frontier = result['selection_log']['frontier']; task = frontier['tasks'][0]
    assert task['generation_outcome'] == 'generation_rejected'
    assert task['rejection']['attempts'] == task['mutation_requests']
    counts = task['chain_accounting']
    assert counts['declared_layers'] == length and counts['planned_layers'] == index+1
    assert counts['attempted_layers'] == index+1
    assert counts['failure']['layer_index'] == index and counts['failure']['guard_terminal']
    assert counts['failure']['reason'] == task['rejection']['reason'] == 'topology chain continues after a deliberate guard negative'
    assert counts['unattempted_suffix'] == task['mutation_requests'][index+1:]
    assert counts['full_sequence_rejected'] and counts['prefix_diagnostic_only']
    assert counts['effective_layers'] == 0
    assert len(frontier['nodes']) == 1 and not list((tmp_path/'guard/cases').glob('*/capsule.json'))
    assert result['budget']['actual'] == result['budget']['generated'] == 0
    restored = restore(tmp_path/'guard/checkpoint.json',tmp_path/'resume',doc)
    resumed = run(tmp_path/'resume',doc,restored,maximum=1)
    assert resumed['selection_log'] == result['selection_log']


def test_final_guard_has_full_plan_but_no_capsule_or_submission(tmp_path):
    doc = profile(2,classes={'two.guard_final'}); result = run(tmp_path/'final-guard',doc,maximum=1)
    task = result['selection_log']['frontier']['tasks'][0]
    assert task['generation_outcome'] == 'guard_terminal'
    assert len(task['generated_record']['operations']) == 2
    assert task['generated_record']['executable'] is False
    assert task['chain_accounting']['planned_layers'] == 2
    assert task['chain_accounting']['executable_layers'] == 1
    assert task['chain_accounting']['effective_layers'] == 0
    assert not list((tmp_path/'final-guard/cases').glob('*/capsule.json'))
    assert not any(e['action'] == 'submit' for e in result['selection_log']['frontier']['journal'])


@pytest.mark.parametrize('length', [2,3,4])
def test_continuous_split_and_pending_checkpoint_exact_recovery(length,tmp_path):
    doc = multiface(length,depth=2)
    full = run(tmp_path/'full',doc,maximum=2)
    part = run(tmp_path/'part',doc,maximum=1)
    resumed = run(tmp_path/'resume',doc,restore(tmp_path/'part/checkpoint.json',tmp_path/'resume',doc),maximum=2)
    assert project(part)+project(resumed) == project(full)
    assert resumed['selection_log'] == full['selection_log']
    class Interrupted(Exception): pass
    def wrap(hook):
        def callback(payload):
            hook(payload)
            if payload['phase'] == 'planned': raise Interrupted()
        return callback
    with pytest.raises(Interrupted): run(tmp_path/'pending',doc,maximum=2,wrapper=wrap)
    recovered = run(tmp_path/'pending-resume',doc,
        restore(tmp_path/'pending/checkpoint.json',tmp_path/'pending-resume',doc),maximum=1)
    assert project(recovered) == project(part)
    assert recovered['selection_log'] == part['selection_log']
    assert all(row['input_search']['execution'] is None for row in recovered['rows'])
    assert recovered['budget']['actual'] == recovered['budget']['attempted'] == 0


@pytest.mark.parametrize('damage',['request','order','slot','parameter','receipt'])
def test_rehashed_completed_checkpoint_cannot_forge_intermediate_layer(damage,tmp_path):
    doc = multiface(3); run(tmp_path/'part',doc,maximum=1)
    path = tmp_path/'part/checkpoint.json'; envelope = json.loads(path.read_text())
    f = envelope['state']['scheduler']['frontier']
    if damage == 'request': f['tasks'][0]['mutation_requests'][1]['operator_id'] = 'param.boundary'
    elif damage == 'order': f['tasks'][0]['mutation_requests'].reverse()
    elif damage == 'slot': f['tasks'][0]['layer_samples'][1]['slot_id'] = 'S1.topology.qos.lease'
    elif damage == 'parameter': f['tasks'][0]['mutation_requests'][1]['context']['delay_sec'] = 2
    else:
        # Rebuild an internally consistent journal and all outer hashes. The
        # authoritative planner (not a checksum) must reject its forged layer.
        forged = InputSearchFrontier.prefix(f,0)
        for event in deepcopy(f['journal']):
            if event['action'] == 'plan':
                record = event['record']
                record['mutation_plan']['operations'][1]['parameters']['request']['context']['fake'] = 1
                record['operations'] = deepcopy(record['mutation_plan']['operations'])
                record['content_sha256'] = content_identity(record)
            forged.apply_event(event)
        f = forged.to_dict(); envelope['state']['scheduler']['frontier'] = f
    from test_model_search_integration import rehash
    rehash(envelope)
    path.write_text(json.dumps(envelope))
    with pytest.raises(ValueError): restore(path,tmp_path/'refused',doc)


@pytest.mark.parametrize('length', [2,3,4])
def test_standard_cli_exact_plan_split_resume_and_capsule_null(cli,length,tmp_path):
    doc = multiface(length,depth=2)
    for name,maximum,extra in [('full',2,[]),('part',1,[]),
        ('resume',2,['--resume-from',str(tmp_path/'part/checkpoint.json')])]:
        assert cli.main(arguments(tmp_path,name,doc,maximum)+extra) == 0
    full,part,resumed = (read(tmp_path/name) for name in ('full','part','resume'))
    assert project(part)+project(resumed) == project(full)
    assert full['selection_log'] == resumed['selection_log']
    for result,name in ((full,'full'),(part,'part'),(resumed,'resume')):
        assert not result['abort'] and not result['execute']
        assert result['budget']['actual'] == result['budget']['attempted'] == result['budget']['executed'] == 0
        assert not result['feedback']['events']
        for row in result['rows']:
            assert len(row['mutation_plan']['operations']) == length
            assert len(row['input_search']['mutation_requests']) == length
            capsule = json.loads((tmp_path/name/'cases'/row['cell_id']/'capsule.json').read_text())
            assert capsule['execution'] is None and row['input_search']['execution'] is None


def test_cft_rule_extension_rebinds_single_golden_without_schema_migration():
    from r2f.campaign.corpus import CONTENT_IDENTITY_SCHEMA, TOPOLOGY_CONTENT_IDENTITY_SCHEMA
    assert CONTENT_IDENTITY_SCHEMA == 'r2f.corpus-content/v2'
    assert TOPOLOGY_CONTENT_IDENTITY_SCHEMA == 'r2f.corpus-content/topology-v4'
    # The old ac3143bb frontier used generic-v2, not topology-v3.
    # Issue19 extends the CFT rule contract: descriptor/frontier must rebind,
    # while the input schema and sampling applicability remain unchanged.
    doc = single_profile(); state = fresh(doc)
    assert dsc(doc)['descriptor_sha256'] == '5ce584d542f4901a9860131bb47be50a85e61427a77d8a00bfe3a8f892d45d9e'
    ledger = next(iter(state['scheduler'].frontier.nodes.values()))['sampling_applicability']
    assert canonical_hash(ledger) == 'a20db8636a3ea853e85f8a5465fca44e4961270b3b0d8afffb36616fe91064a0'
    assert canonical_hash(state['scheduler'].frontier.to_dict()) == '3fd62ef7bd04426e23b098a93e9fcf566ccf686e6602a2f15406ce698fb77594'
    data = fresh(multiface(2))['scheduler'].frontier.to_dict()
    data['schema'] = 'r2f.input-search-frontier/topology-v1'
    with pytest.raises(ValueError): InputSearchFrontier.from_dict(data)


def test_feedback_independence_and_declared_task_bound():
    doc = multiface(2,depth=2); a,b = fresh(doc),fresh(doc)
    for index in range(2):
        b['feedback'].record(dict(target_id='fixture',content_sha256=f'{index:064x}',surface_id='S1.topology.program',
                                  boundary_class='fixture',execution_stage='planned',actual_run=False))
        b['scheduler'].advance_feedback(b['feedback'])
        left,lr = generate_one(a,doc); right,rr = generate_one(b,doc)
        assert left['mutation_requests'] == right['mutation_requests'] and lr == rr
    assert len(a['scheduler'].frontier.tasks) == 2


def test_accounting_does_not_credit_core_overridden_operations():
    doc = multiface(2); state = fresh(doc); task = state['scheduler'].next()
    plan = generate(task['entry'],task,dsc(doc))
    # Accounting projection only: do not mutate/relax a valid core plan or claim
    # that the topology compiler actually emitted an overridden layer here.
    from types import SimpleNamespace
    op = SimpleNamespace(applicability_status='overridden')
    fake = SimpleNamespace(operations=[op],runtime_windows=[])
    with patch('r2f.core.mutation_plan.mutation_plan_noop_reason',return_value=None):
        counts = plan_accounting(fake,2)
    assert counts['overridden_layers'] == 1 and counts['effective_layers'] == 0
    assert counts['execution_evidence'] is False


def test_same_final_with_different_request_prefix_uses_existing_content_projection():
    doc = profile(2,classes={'two.multiface'},seed='topology.writer_only')
    state = fresh(doc); entry = next(iter(state['corpus'].entries.values()))
    layers = doc['topology_search']['samples'][0]['layers']
    requests = [{k:deepcopy(layer[k]) for k in ('operator_id','slot_id','context')} for layer in layers]
    requests[0]['context'] = {'boundary_class':'one'}  # exact value, not RNG-dependent neighbor
    a = derived_record(entry,make_topology_plan(entry['final_config']['topology_program'],requests,rng_seed=41))
    b = derived_record(entry,make_topology_plan(entry['final_config']['topology_program'],list(reversed(requests)),rng_seed=41))
    assert a['final_config'] == b['final_config']
    # Existing content v2 retains ordered semantic operations, not final-only
    # identity. Different prefixes/order must not be forced into a new deduper.
    assert a['content_sha256'] == content_identity(a)
    assert b['content_sha256'] == content_identity(b)
    assert a['content_sha256'] != b['content_sha256']


def test_self_consistent_alternate_intermediate_request_rejected_by_model():
    doc = multiface(3); state = fresh(doc); selected = state['scheduler'].next()
    changed = deepcopy(selected['mutation_requests']); changed[1]['operator_id'] = 'timing.shift'
    changed[1]['context'] = {'offset_sec':.002}
    plan = make_topology_plan(selected['entry']['final_config']['topology_program'],changed,rng_seed=selected['rng_seed'])
    state['scheduler'].frontier.planned(selected['task_id'],derived_record(selected['entry'],plan))
    f = InputSearchFrontier.from_dict(state['scheduler'].frontier.to_dict())
    with pytest.raises(ValueError,match='model-generatable'):
        f.validate_model(profile=doc,seeds=state['corpus'],registry=registry_for_topology())


def test_rejection_reason_regenerated_after_consistent_journal_rehash():
    doc = profile(3,classes={'three.guard_middle'}); state = fresh(doc); generate_one(state,doc)
    original = state['scheduler'].frontier.to_dict(); f = InputSearchFrontier.prefix(original,0)
    for event in deepcopy(original['journal']):
        if event['action'] == 'generation_rejected': event['reason'] = 'forged middle-layer refusal'
        elif event['action'] == 'finish': event['reason'] = 'topology_input_rejected:forged middle-layer refusal'
        f.apply_event(event)
    f = InputSearchFrontier.from_dict(f.to_dict())
    with pytest.raises(ValueError,match='rejection not model-generatable'):
        f.validate_model(profile=doc,seeds=state['corpus'],registry=registry_for_topology())


@pytest.mark.parametrize('length',[2,3,4])
def test_standard_cli_pending_and_rejected_checkpoint(cli,length,tmp_path,monkeypatch):
    doc = multiface(length)
    original = cli.make_checkpoint_hook
    class Interrupted(Exception): pass
    def factory(*args,**kwargs):
        hook = original(*args,**kwargs)
        def callback(payload):
            hook(payload)
            if payload['phase'] == 'planned': raise Interrupted()
        return callback
    monkeypatch.setattr(cli,'make_checkpoint_hook',factory)
    with pytest.raises(Interrupted): cli.main(arguments(tmp_path,'pending',doc,1))
    monkeypatch.setattr(cli,'make_checkpoint_hook',original)
    assert cli.main(arguments(tmp_path,'resumed-pending',doc,1)+
                    ['--resume-from',str(tmp_path/'pending/checkpoint.json')]) == 0
    assert cli.main(arguments(tmp_path,'continuous',doc,1)) == 0
    resumed,full = read(tmp_path/'resumed-pending'),read(tmp_path/'continuous')
    assert project(resumed) == project(full) and resumed['selection_log'] == full['selection_log']
    guard = profile(length,classes={['two.guard_prefix','three.guard_middle','four.guard_middle'][length-2]})
    assert cli.main(arguments(tmp_path,'guard',guard,1)) == 0
    assert cli.main(arguments(tmp_path,'guard-resume',guard,1)+
                    ['--resume-from',str(tmp_path/'guard/checkpoint.json')]) == 0
    assert read(tmp_path/'guard')['selection_log'] == read(tmp_path/'guard-resume')['selection_log']
    assert not list((tmp_path/'guard/cases').glob('*/capsule.json'))


@pytest.mark.parametrize('damage',['request','order','slot','parameter'])
def test_rehashed_pending_checkpoint_rejects_middle_request_tamper(tmp_path,damage):
    doc = multiface(3)
    class Interrupted(Exception): pass
    def wrap(hook):
        def callback(payload):
            hook(payload)
            if payload['phase'] == 'planned': raise Interrupted()
        return callback
    with pytest.raises(Interrupted): run(tmp_path/'pending',doc,maximum=1,wrapper=wrap)
    path = tmp_path/'pending/checkpoint.json'; data = json.loads(path.read_text())
    selection = data['continuation']['pending_selection'] if 'continuation' in data else data['state']['continuation']['pending_selection']
    if damage == 'request': selection['mutation_requests'][1]['operator_id'] = 'param.boundary'
    elif damage == 'order': selection['mutation_requests'].reverse()
    elif damage == 'slot': selection['layer_samples'][1]['slot_id'] = 'S1.topology.qos.lease'
    else: selection['mutation_requests'][1]['context']['delay_sec'] = 1
    from test_model_search_integration import rehash
    rehash(data); path.write_text(json.dumps(data))
    with pytest.raises(ValueError): restore(path,tmp_path/'refused',doc)
