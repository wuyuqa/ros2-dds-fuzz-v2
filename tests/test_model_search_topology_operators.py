"""Expanded declared topology operators: input generation only, never execution."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from r2f.campaign.topology_search import descriptor, enumerate_tasks, generate, OPERATORS_SCHEMA
from r2f.campaign.search_frontier import InputSearchFrontier
from r2f.campaign.exploratory_input import validate_profile_fields
from r2f.modules.s1.topology_input import make_topology_plan
from test_model_search_topology import (fresh, run, restore, projection, generate_one,
                                       registry_for_topology, profile as original_profile)
from test_model_search_topology_cli import cli, arguments, read, hashes

ROOT = Path(__file__).resolve().parents[1]
NEW = {'env.delay','env.health','env.resource','env.lease_expiry','env.lease_refresh',
       'guard.dependency','guard.parameter','guard.structure','guard.environment'}


def profile():
    doc=json.loads((ROOT/'config/s1_fuzz/profiles/model-bfs-topology-operators.json').read_text())
    doc['rmws']=doc['rmws'][:1]
    specs=[s for s in doc['seed_specs'] if s['seed_id']=='topology.multi_endpoint']
    doc['seed_specs']=deepcopy(specs);doc['declared_seed_specs_union']=deepcopy(specs)
    doc['cycles'][0]['seed_specs']=deepcopy(specs)
    doc['search'].update(max_depth=1,max_nodes=128,max_tasks=512)
    doc['budget'].update(max_inputs=128,max_seconds=300)
    return doc


def subset(op, slot=None, context=None):
    doc=profile()
    samples=[s for s in doc['topology_search']['samples'] if s['operator_id']==op
             and (slot is None or s['slot_id']=='S1.topology.'+slot)
             and (context is None or s['context']==context)]
    assert samples
    doc['topology_search']['samples']=samples
    return doc


def test_complete_declared_ledger_and_handler_constraints():
    doc=profile(); f=fresh(doc)['scheduler'].frontier
    ledger=next(iter(f.nodes.values()))['sampling_applicability']
    assert len(ledger)==31
    assert len({r['operator_id'] for r in ledger})==21
    assert len({r['slot_id'] for r in ledger})==11
    assert len([r for r in ledger if r['status']=='applicable'])==27
    assert {(r['operator_id'],r['slot_id'],r['reason']) for r in ledger if r['reason']}=={
        ('action.delete','S1.topology.dependency','operator_slot_kind_mismatch'),
        ('action.swap','S1.topology.dependency','operator_slot_kind_mismatch'),
        ('lease.value','S1.topology.qos.deadline','operator_slot_kind_mismatch'),
        ('env.lease_refresh','S1.topology.qos.liveliness','handler_requires_parameter_lease')}
    assert len(f.tasks)==64
    assert len({t['operator_id'] for t in f.tasks})==21
    assert all(r['classes'] for r in ledger if r['status']=='applicable')
    assert not {'env.reorder','env.duplicate','env.drop'} & {r['operator_id'] for r in ledger}
    assert all('sampling_scope' in r for r in ledger)
    assert all(t['class_id']=='handler_rng' for t in f.tasks
               if t['operator_id'] in NEW and not t['mutation_request']['context'])


@pytest.mark.parametrize('op',sorted(NEW))
def test_new_family_exact_request_to_plan_or_named_generation_rejection(op):
    doc=subset(op);state=fresh(doc);f=state['scheduler'].frontier
    count=len(f.tasks)
    for _ in range(count):
        selection,record=generate_one(state,doc)
        task=f.task(selection['task_id'])
        parent=f.entry(task)
        # Reuse exact authoritative planner, including its rejection, never fallback.
        try:
            direct=make_topology_plan(parent['final_config']['topology_program'],
                                     [selection['mutation_request']],rng_seed=selection['rng_seed'])
        except ValueError as exc:
            assert record is None
            assert task['status']=='rejected' and str(exc) in task['rejection']['reason']
        else:
            assert direct.to_dict()==generate(parent,task,f.topology_descriptor).to_dict()
            assert record is not None
            assert record['mutation_plan']==direct.to_dict()
        assert task['status']!='submitted'
        if record and not record['executable']:
            assert task['generation_outcome']=='guard_terminal'
            assert task['status']=='rejected'
    InputSearchFrontier.from_dict(f.to_dict()).validate_model(
        profile=doc,seeds=state['corpus'],registry=registry_for_topology())


def test_all_classes_runner_capsule_and_rejection_artifacts(tmp_path):
    doc=profile()
    # Catalogue-wide request/capsule audit. Per-step checkpoint/recovery is
    # independently exercised below with representative families and refusals.
    result=run(tmp_path/'all',doc,maximum=64,wrapper=lambda hook: None)
    assert result['abort'] is None
    assert len(result['rows'])==64
    assert result['budget']['actual']==result['budget']['attempted']==result['budget']['executed']==0
    assert result['feedback']['events']==[]
    tasks={t['task_id']:t for t in result['selection_log']['frontier']['tasks']}
    assert {t['operator_id'] for t in tasks.values()}=={s['operator_id'] for s in doc['topology_search']['samples']}
    assert any(r.get('generation_status')=='rejected' for r in result['rows'])
    assert any(t.get('generation_outcome')=='guard_terminal' for t in tasks.values())
    for row in result['rows']:
        task=tasks[row['input_search']['task_id']]
        assert row['input_search']['mutation_request']==task['mutation_request']
        assert row['input_search']['execution'] is None and row['actual_run'] is False
        if row.get('mutation_plan'):
            assert row['mutation_plan']['operations'][0]['parameters']['request']==task['mutation_request']
        capsule=tmp_path/'all/cases'/row['cell_id']/'capsule.json'
        if capsule.exists():assert json.loads(capsule.read_text())['execution'] is None
    assert not any(e['action']=='submit' for e in result['selection_log']['frontier']['journal'])


@pytest.mark.parametrize('op,context', [('env.delay',{}),('env.lease_refresh',{}),
    ('guard.structure',{}),('env.health',{'key':'transport_ready'})])
def test_split_resume_request_plan_hash_and_rejection_equivalence(tmp_path,op,context):
    doc=subset(op,context=context);doc['search']['samples_per_class']=2
    full=run(tmp_path/'full',doc,maximum=2)
    part=run(tmp_path/'part',doc,maximum=1)
    state=restore(tmp_path/'part/checkpoint.json',tmp_path/'resume',doc)
    resumed=run(tmp_path/'resume',doc,state,maximum=2)
    assert projection(part)+projection(resumed)==projection(full)
    assert resumed['selection_log']==full['selection_log']


def test_expanded_feedback_does_not_change_tasks():
    doc=profile();a=fresh(doc);b=fresh(doc)
    b['feedback'].events.append({'target_id':'arbitrary','oracle':'fail','novel':True})
    assert a['scheduler'].frontier.to_dict()==b['scheduler'].frontier.to_dict()
    assert a['scheduler'].next()==b['scheduler'].next()


@pytest.mark.parametrize('op,slot,context',[
    ('env.delay','qos.deadline',{'delay_sec':0.1}),
    ('env.lease_refresh','qos.lease',{'lease_action':'refresh'}),
    ('guard.structure','actions',{'boundary_class':'budget_max'}),
    ('env.health','environment',{'key':'type_matching'}),
    ('param.enum','qos.reliability',{'enum_value':'automatic'}),
    ('param.boundary','qos.lease',{'boundary_class':'one'}),
    ('env.lease_refresh','qos.liveliness',{}),
    ('action.delete','dependency',{}),
])
def test_unconsumed_or_inapplicable_selectors_never_scheduled(op,slot,context):
    doc=profile();doc['topology_search']['samples']=[dict(operator_id=op,slot_id='S1.topology.'+slot,
                                                      class_id='handler_rng',context=context)]
    with pytest.raises(ValueError):descriptor(doc['topology_search'],doc['operator_whitelist'])


def test_rng_sample_is_not_falsely_named_boundary():
    doc=subset('guard.structure');doc['topology_search']['samples'][0]['class_id']='budget_max'
    with pytest.raises(ValueError,match='handler_rng'):fresh(doc)


@pytest.mark.parametrize('length',[2,3,4])
def test_chain_remains_explicitly_unsupported(length):
    doc=profile();doc['chain_length']=length
    with pytest.raises(ValueError,match='chain_length=1'):validate_profile_fields(doc)


def test_old_descriptor_and_ledger_remain_identical_to_frozen_card():
    # The old schema must not silently acquire classes or change applicability.
    doc=original_profile();f=fresh(doc)['scheduler'].frontier
    ledger=next(iter(f.nodes.values()))['sampling_applicability']
    assert len([r for r in ledger if r['status']=='applicable'])==15
    assert any(r['reason']=='handler_not_supported_in_first_slice' for r in ledger)
    assert not any('sampling_scope' in r for r in ledger)
    assert f.topology_descriptor['sampler']=='explicit-consumed-context/v1'
    # Read-only calculation against the frozen old ten-file card.
    from r2f.core.evidence import canonical_hash
    assert f.topology_descriptor['descriptor_sha256']=='9689aec389dd801ba8eaff00a7b4fdc3b9a22ef7604eb9af1c790e24e6cd894b'
    assert canonical_hash(f.to_dict())=='ac3143bb98b32d9dee52722d9e4c620323d1291b2cc6c1ccea9139a17fbcb97f'


def test_task_tamper_rejected_and_rejection_reason_regenerated():
    doc=subset('env.health',context={'key':'filter_support'});state=fresh(doc);generate_one(state,doc)
    original=state['scheduler'].frontier.to_dict()
    rebuilt=InputSearchFrontier.prefix(original,0)
    for event in deepcopy(original['journal']):
        if event['action']=='generation_rejected':event['reason']='forged'
        if event['action']=='finish':event['reason']='topology_input_rejected:forged'
        rebuilt.apply_event(event)
    with pytest.raises(ValueError,match='rejection not model-generatable'):
        InputSearchFrontier.from_dict(rebuilt.to_dict()).validate_model(
            profile=doc,seeds=state['corpus'],registry=registry_for_topology())
    fresh_state=fresh(profile());task=fresh_state['scheduler'].frontier.tasks[0]
    task['mutation_request']['context']={'ignored_selector':1}
    with pytest.raises(ValueError):
        InputSearchFrontier.from_dict(fresh_state['scheduler'].frontier.to_dict()).validate_model(
            profile=profile(),seeds=fresh_state['corpus'],registry=registry_for_topology())


def test_expanded_standard_cli_split_resume_plan_only(cli,tmp_path):
    doc=subset('env.delay');doc['search']['samples_per_class']=2
    assert cli.main(arguments(tmp_path,'full',doc,2))==0
    assert cli.main(arguments(tmp_path,'part',doc,1))==0
    assert cli.main(arguments(tmp_path,'resume',doc,2)+['--resume-from',str(tmp_path/'part/checkpoint.json')])==0
    full,part,resumed=[read(tmp_path/name) for name in ['full','part','resume']]
    assert hashes(part)+hashes(resumed)==hashes(full)
    assert resumed['selection_log']==full['selection_log']
    assert full['budget']['actual']==resumed['budget']['actual']==0


@pytest.mark.parametrize('op,expected_field,expected_value',[
    ('env.delay','delay_sec',None),
    ('env.lease_expiry','lease_lifecycle','declared_only'),
    ('env.lease_refresh','lease_lifecycle','declared_only'),
])
def test_environment_receipts_are_declared_values_not_execution(op,expected_field,expected_value):
    doc=subset(op);state=fresh(doc);selection,record=generate_one(state,doc)
    receipt=record['mutation_plan']['operations'][0]['parameters']['input_layer_receipts'][0]
    assert receipt['execution_evidence'] is False and receipt['candidate_eligible'] is False
    effect=receipt['v2_case']['expected_guard_effect']
    if expected_value is not None:assert effect[expected_field]==expected_value
    else:
        assert type(effect[expected_field]) in (int,float)
        assert selection['mutation_request']['context']=={}


def test_pending_expanded_plan_restores_without_second_selection(tmp_path):
    doc=subset('env.delay')
    class StopAfterPlan(Exception):pass
    def interrupt(hook):
        def callback(payload):
            hook(payload)
            if payload['phase']=='planned':raise StopAfterPlan()
        return callback
    with pytest.raises(StopAfterPlan):run(tmp_path/'part',doc,maximum=1,wrapper=interrupt)
    restored=restore(tmp_path/'part/checkpoint.json',tmp_path/'resume',doc)
    result=run(tmp_path/'resume',doc,restored,maximum=1)
    assert len(result['selection_log']['selections'])==1
    assert result['budget']['actual']==0
