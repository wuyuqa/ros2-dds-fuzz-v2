"""Offline plumbing/denial tests, NOT actual DDS or natural admission evidence.

Positive scope tests MODEL provider/formal qualifications and mock canonical
execution/input validators. They exercise transport/lifecycle only. The public
runner integration uses the explicit OfflineTopologyBackend and must deny.
"""
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from test_topology_candidate_identity import row
from test_topology_conformance_signature import model
from r2f.campaign import topology_candidate_assessment as plumbing
from r2f.core.evidence import canonical_hash
from r2f.reports.candidate_report import exploratory_candidate_ledger


@pytest.fixture
def setup(row,monkeypatch):
    model(row)
    facts=row['typed_facts'];facts.update(target_id=plumbing.TARGET,preconditions_reached=True)
    row['oracle']['rule_role']='primary'
    row.update(execution_source='ros2_public_api',repeat=0,
               test_fixture='offline_mocked_canonical_and_formal_boundaries',
               source_manifest={'head':'a'*40,'behavior_tree_sha256':'b'*64,
                                'source_frozen':True,'dirty_behavior_paths':[]})
    row['mutation_receipt'].update(planned=True,submitted=True)
    row['target']={'target_id':plumbing.TARGET,'target_revision':0,'rule_contract':{'primary_rule':plumbing.RULE}}
    target=SimpleNamespace(to_dict=lambda:deepcopy(row['target']),expected_effect_set={
        'allowed_observations':[], 'forbidden_residuals':['topology.closed_delivery_gap','topology.pair_graph_mismatch']})
    scenario=SimpleNamespace(target_id=plumbing.TARGET,target=target,arm='mutated')
    source=deepcopy(row['source_manifest']); final=deepcopy(row['final_input'])
    capsule=SimpleNamespace(input={'source_manifest':source,'effective_input_sha256':row['effective_input_sha256'],
        'effective_input':{'final_input':final,'input_layer_receipts':[]}},execution=deepcopy(row))
    capsule.to_dict=lambda: {'schema':'r2f.conformance-capsule/v1','input':deepcopy(capsule.input),
                             'execution':deepcopy(capsule.execution),'execution_history':[], 'replay_attempts':[]}
    config={'source_manifest':source,'final_input':final,'input_layer_receipts':[],
            'effective_input_sha256':row['effective_input_sha256']}
    facts['program_sha256']=canonical_hash(final['topology_program'])
    calls=[]
    def checked(name):
        def run(*args,**kwargs): calls.append(name);return True
        return run
    monkeypatch.setattr(plumbing,'validate_replay_input',checked('sealed_input'))
    monkeypatch.setattr(plumbing,'validate_execution',checked('canonical_execution'))
    monkeypatch.setattr(plumbing,'_v2_execution_input',checked('v2_execution_binding'))
    # Existing lifecycle is exercised with its other identity/mutation/triage
    # gates intact, but NO claim of authentic canonical execution qualification.
    monkeypatch.setattr('r2f.core.candidate.validate_execution',checked('lifecycle_execution_mock'))
    return scenario,config,capsule,row,calls


def publish(setup):
    scenario,config,capsule,row,_=setup
    row['oracle']['candidate_assessment']=plumbing.provider_assessment(scenario,row['typed_facts'],row['oracle'])
    return row


def finalize(setup,boundary=None,qualification=None):
    scenario,config,capsule,row,_=setup
    plumbing.finalize_assessment(scenario,config,capsule,row,
        boundary={'status':'qualified','eligible':True} if boundary is None else boundary,
        qualification={'real_ros_execution':True,'qualification_errors':[]} if qualification is None else qualification)
    return row


def test_provider_receipt_is_formal_pending_not_qualified(setup):
    scenario,_,_,row,_=setup
    receipt=plumbing.provider_assessment(scenario,row['typed_facts'],row['oracle'])
    assert receipt['qualified'] is None and receipt['eligible_at_scope'] is True
    assert not receipt['blockers'] and set(receipt['pending'])=={'canonical_ingest','formal_boundary','formal_topology_execution'}


@pytest.mark.parametrize(('owner', 'field'), [
    ('oracle', 'filter_false_implies_unmatch'),
    ('facts', 'input_contract_changed'),
])
def test_provider_rejects_json_type_aliases_in_cft_contract(setup, owner, field):
    from r2f.modules.s1.topology_cft_contract import predicate_contract
    scenario, _, _, row, _ = setup
    contract = predicate_contract()
    row['typed_facts']['content_filter_observations'] = {
        'declared': True, 'evidence_conflicts': [],
        'predicate_contract': deepcopy(contract),
    }
    row['oracle']['cft_rule_contract'] = deepcopy(contract)
    receipt = plumbing.provider_assessment(scenario, row['typed_facts'], row['oracle'])
    assert 'cft_evidence_consistent' not in receipt['blockers']
    target = (row['oracle']['cft_rule_contract'] if owner == 'oracle'
              else row['typed_facts']['content_filter_observations']['predicate_contract'])
    target[field] = 0
    receipt = plumbing.provider_assessment(scenario, row['typed_facts'], row['oracle'])
    assert 'cft_evidence_consistent' in receipt['blockers']


def test_ingest_transports_identity_and_provenance_without_granting(setup):
    publish(setup);scenario,config,capsule,row,calls=setup
    plumbing.ingest_assessment(scenario,config,capsule,row)
    assert row['candidate_eligible'] is None
    assert row['endpoint_identity']['schema']=='r2f.topology-candidate-identity/v1'
    assert row['provenance']['capsule_input_sha256']==canonical_hash(capsule.input)
    assert row['candidate_assessment']['ingest']['qualified'] is None
    assert {'sealed_input','canonical_execution'}<=set(calls)


def test_no_oracle_handoff_remains_pending_not_promoted(setup):
    row=setup[3];row['oracle']['candidate_eligible']=False
    finalize(setup)
    assert not row['candidate_eligible']
    assert 'provider_assessment_contract_not_published' in row['candidate_assessment']['final']['pending']
    assert row['oracle']['candidate_eligible'] is False


def test_modeled_qualified_fail_retained_through_existing_deferred_lifecycle(setup):
    row=publish(setup);snapshot=deepcopy(setup[2].execution)
    finalize(setup)
    assert row['candidate_eligible'] is True
    assert (row['triage_disposition'],row['responsibility_layer'],row['reason_code'])==('deferred','observation','attribution_insufficient')
    ledger=exploratory_candidate_ledger([row])
    assert len(ledger['candidates'])==1,ledger
    candidate=ledger['candidates'][0]
    assert candidate['candidate_status']=='deferred' and candidate['admission_error'] is None
    assert [h['event'] for h in candidate['history']][:3]==['observed','admitted','triaged']
    assert ledger['natural_candidate_count']==ledger['confirmed_candidates']==0
    attachment=plumbing.capsule_attachment(setup[2],row)
    assert attachment['execution']==snapshot and attachment['execution_history']==[]
    assert attachment['candidate_assessment_attachment']['assessment']==row['candidate_assessment']
    assert attachment['candidate_assessment_attachment']['projection']['candidate_eligible'] is True
    assert attachment['candidate_assessment_attachment']['execution_snapshot_sha256']==canonical_hash(snapshot)


@pytest.mark.parametrize('damage',['fixture','negative','control','tool_error','unknown','pass','noeffect',
    'dirty_source','different_source','input_changed','program_changed','raw_changed','bad_generation',
    'provider_receipt_changed','provider_denied','facts_denied','canonical_failed','sealed_input_failed','v2_failed'])
def test_each_denial_stays_closed(setup,monkeypatch,damage):
    row=setup[3];facts=row['typed_facts']
    if damage=='fixture':facts['fixture']=True
    elif damage=='negative':row['arm']='negative';setup[0].arm='negative'
    elif damage=='control':row['control_kind']='fault'
    elif damage=='tool_error':row['tool_error']=True
    elif damage=='unknown':row['oracle_verdict']=row['oracle']['verdict']='unknown'
    elif damage=='pass':row['oracle_verdict']=row['oracle']['verdict']='pass'
    elif damage=='noeffect':row['mutation_receipt']['effect_observed']=False
    elif damage=='dirty_source':row['source_manifest']['dirty_behavior_paths']=['changed.py']
    elif damage=='different_source':row['source_manifest']=dict(row['source_manifest'],head='c'*40)
    elif damage=='input_changed':setup[1]['effective_input_sha256']='d'*64
    elif damage=='program_changed':facts['program_sha256']='e'*64
    elif damage=='raw_changed':row['raw_sha256']='f'*64
    elif damage=='bad_generation':facts['pairs'][0]['writer_generation']=99
    elif damage=='provider_denied':row['oracle']['candidate_eligible']=False
    elif damage=='facts_denied':facts['candidate_eligible']=False
    def fail(*args,**kwargs):raise ValueError('explicit offline gate failure')
    if damage=='canonical_failed':monkeypatch.setattr(plumbing,'validate_execution',fail)
    elif damage=='sealed_input_failed':monkeypatch.setattr(plumbing,'validate_replay_input',fail)
    elif damage=='v2_failed':monkeypatch.setattr(plumbing,'_v2_execution_input',fail)
    publish(setup)
    if damage=='provider_receipt_changed':row['oracle']['candidate_assessment']['binding']['facts_sha256']='0'*64
    finalize(setup)
    assert row['candidate_eligible'] is False,damage
    assert row['candidate_assessment']['final']['blockers'],damage
    ledger=exploratory_candidate_ledger([row])
    assert ledger['natural_candidate_count']==ledger['confirmed_candidates']==0
    for entry in ledger['candidates']:
        assert entry['candidate_status']=='observed' and entry['admission_error'],damage


@pytest.mark.parametrize('boundary,formal',[
    ({'status':'invalid','eligible':False},{'real_ros_execution':True,'qualification_errors':[]}),
    ({'status':'qualified','eligible':False},{'real_ros_execution':True,'qualification_errors':[]}),
    ({'status':'qualified','eligible':True},{'real_ros_execution':False,'qualification_errors':[]}),
    ({'status':'qualified','eligible':True},{'real_ros_execution':True,'qualification_errors':['unknown']}),
])
def test_final_formal_denials_are_not_overridden(setup,boundary,formal):
    publish(setup);row=finalize(setup,boundary,formal)
    assert not row['candidate_eligible'] and row['candidate_assessment']['final']['blockers']


def test_missing_formal_work_is_pending_not_false_deny(setup):
    publish(setup);scenario,config,capsule,row,_=setup
    plumbing.finalize_assessment(scenario,config,capsule,row,boundary=None,qualification=None)
    final=row['candidate_assessment']['final']
    assert final['qualified'] is None and not final['blockers']
    assert set(final['pending'])=={'formal_boundary','formal_topology_execution'}
    assert row['candidate_eligible'] is False


def test_same_target_capsule_input_and_raw_provenance_tamper_detected(setup):
    publish(setup);row=finalize(setup)
    assert row['candidate_eligible']
    row['candidate_assessment']['ingest']['checks']['sealed_input_valid']=True
    row['provenance']['raw']['sha256']='0'*64
    setup[1]['input_layer_receipts']=[{'fake':'receipt'}]
    row=finalize(setup)
    assert not row['candidate_eligible']
    assert row['provenance']['raw']['sha256']==row['raw_sha256']


def test_non_topology_helper_is_identity_noop(setup):
    scenario,config,capsule,row,_=setup
    scenario.target_id='F1.unrelated'
    before=deepcopy(row)
    assert plumbing.ingest_assessment(scenario,config,capsule,row) is row
    assert plumbing.finalize_assessment(scenario,config,capsule,row,boundary=None,qualification=None) is row
    assert row==before


def test_public_ingest_and_runner_consume_helper_offline_only(tmp_path):
    # A real registry/runner invocation, but explicit offline backend: no ROS,
    # netns, native workers, or natural candidate qualification is performed.
    from test_topology_cycle import run_cycle,profile,offline_runtime
    doc=profile(1);doc['budget']['max_inputs']=1
    with patch.object(plumbing,'ingest_assessment',wraps=plumbing.ingest_assessment) as ingest, \
         patch.object(plumbing,'finalize_assessment',wraps=plumbing.finalize_assessment) as final:
        cycle=run_cycle(tmp_path/'offline-plumbing',doc,runtime=offline_runtime([]))
    assert ingest.call_count>=1 and final.call_count==1
    row=cycle['rows'][0]
    assert row['candidate_eligible'] is False and row['candidate_assessment']['final']['blockers']
    assert row['oracle']['candidate_assessment']['schema'] == plumbing.PROVIDER_SCHEMA
    assert row['oracle']['candidate_assessment']['qualified'] is False
    report = json.loads((tmp_path/'offline-plumbing/report.json').read_text())
    assert report['rows'][0]['candidate_assessment'] == row['candidate_assessment']
    assert report['rows'][0]['candidate_eligible'] is False
    ledger = json.loads((tmp_path/'offline-plumbing/candidate_ledger.json').read_text())
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0
    capsule=json.loads(open(row['replay_path']).read())
    assert capsule['candidate_assessment_attachment']['assessment']==row['candidate_assessment']
    assert capsule['candidate_assessment_attachment']['projection']['candidate_eligible'] is False
    assert capsule['execution'] is None  # offline backend is not actual execution
    assert not cycle['candidate_ledger']['candidates'] if 'candidate_ledger' in cycle else True


def test_unverified_caller_sut_support_flags_are_not_promoted(setup):
    row=publish(setup)
    row.update(controls_support_sut=True,provenance_supports_sut=True,
               provenance={'invented':'independent-sut-support'})
    row['stage_history']=[{'execution_stage':'judged','test_fixture':True}]
    history=deepcopy(row['stage_history'])
    finalize(setup)
    assert row['triage_disposition']=='deferred' and row['reason_code']=='attribution_insufficient'
    assert row['provenance']['attribution']=='not_independent_sut_support'
    assert row['stage_history']==history
    assert exploratory_candidate_ledger([row])['natural_candidate_count']==0


def test_final_assessment_is_idempotent_and_recomputable(setup):
    publish(setup);row=finalize(setup)
    first=deepcopy(row['candidate_assessment']);provenance=deepcopy(row['provenance'])
    attachment=plumbing.capsule_attachment(setup[2],row)
    finalize(setup)
    assert row['candidate_assessment']==first and row['provenance']==provenance
    assert plumbing.capsule_attachment(setup[2],row)==attachment


def test_real_oracle_publishes_provider_pending_then_existing_lifecycle(setup):
    # Offline FAIL FIXTURE, NOT actual candidate admission. The oracle hook is
    # real; canonical and formal validators remain explicitly modeled by setup.
    from r2f.modules.s1.topology_oracles import evaluate_topology
    from r2f.modules.s1.topology_input import TOPOLOGY_FACT_PRODUCER
    scenario, _, _, row, _ = setup
    facts = row['typed_facts']
    facts.update(producer_id=TOPOLOGY_FACT_PRODUCER, fact_values={}, action_receipts=[{}])
    row['oracle'] = evaluate_topology(scenario, facts)
    oracle = row['oracle']
    assert oracle['verdict'] == 'fail'
    assert oracle['candidate_eligible'] is True  # provider-local only
    assert oracle['candidate_assessment']['qualified'] is None
    assert oracle['candidate_assessment']['scope'] == 'provider_typed_evidence'
    finalize(setup)
    assert row['candidate_eligible'] is True
    assert row['candidate_assessment']['final']['qualified'] is True
    ledger = exploratory_candidate_ledger([row])
    assert ledger['candidates'][0]['candidate_status'] == 'deferred'
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0


@pytest.mark.parametrize('damage', ['wrong_target', 'bad_schema', 'tool', 'identity', 'fixture', 'control', 'pass'])
def test_oracle_publishes_explicit_denial_for_early_and_final_returns(setup, damage):
    from r2f.modules.s1.topology_oracles import evaluate_topology
    from r2f.modules.s1.topology_input import TOPOLOGY_FACT_PRODUCER
    scenario, _, _, row, _ = setup
    facts = row['typed_facts']
    facts.update(producer_id=TOPOLOGY_FACT_PRODUCER, fact_values={}, action_receipts=[{}])
    if damage == 'wrong_target': scenario.target_id = 'unrelated'
    elif damage == 'bad_schema': facts = None
    elif damage == 'tool': facts['tool_error'] = True
    elif damage == 'identity': facts['endpoint_identity_complete'] = False
    elif damage == 'fixture': facts['fixture'] = True
    elif damage == 'control': scenario.arm = 'negative'
    elif damage == 'pass':
        for pair in facts['pairs']:
            pair.update(compatibility='compatible', observed_match='bidirectional', delivery_closure='no_obligation')
    result = evaluate_topology(scenario, facts)
    assert result['candidate_eligible'] is False
    assert result['candidate_assessment']['qualified'] is False
    assert result['candidate_assessment']['blockers']
    assert result['candidate_assessment']['pending']


def test_fact_resolver_diagnostic_namespace_still_supported(setup):
    from r2f.modules.s1.topology_oracles import resolve_fact
    from r2f.modules.s1.topology_input import TOPOLOGY_FACT_PRODUCER, TOPOLOGY_FACT_GRAPH
    facts = setup[3]['typed_facts']
    facts.update(producer_id=TOPOLOGY_FACT_PRODUCER, fact_values={})
    assert resolve_fact(TOPOLOGY_FACT_GRAPH, facts) in (True, False, None)
