"""P3 immutable protocol fixtures through existing fact/replay/boundary services.

No fixture is an actual run, candidate, seal, or sharing qualification.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import pytest
from r2f.core import Scenario
from r2f.core.evidence import canonical_hash, effective_input_hash, INPUT_SCHEMA_V2
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.modules.s1.topology_targets import topology_target_declaration, TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
from r2f.campaign.topology_adapter import TopologyFactProducer
from r2f.runtime.participant_groups import CONTROL, OBSERVATION_SCHEMA, project_lineage
from test_topology_adapter import RawFixture


def group_fixture(root, shared=True):
    """Adapt the established raw protocol fixture, preserving all action evidence.

    Only supporting captures are unavailable. These rows are NOT measurements.
    """
    f=RawFixture()
    parent=deepcopy(f.plan.operations[0].parameters['input_layer_receipts'][0]['input_before'])
    parent['participant_control']=dict(CONTROL)
    for i,e in enumerate(parent['endpoints']): e['participant_id']='a' if shared or i==0 else 'b'
    f.plan=make_topology_plan(parent,[{'operator_id':'action.insert'}],rng_seed=7)
    f.program=f.plan.final_config['topology_program']
    f.scenario=Scenario(topology_target_declaration(),'rmw_cyclonedds_cpp','mutated',0,seed=7,mutation_plan=f.plan)
    f.layers=topology_input_layer_receipts(f.plan)
    eff=f.scenario.effective_input_v2(final_input=f.plan.final_config,input_layer_receipts=f.layers,
        source_manifest={'source_repo':'/nonexistent/offline/fixture','head':None,'source_frozen':False})
    f.config.update(**deepcopy(eff),effective_input=eff,effective_input_sha256=effective_input_hash(eff,schema=INPUT_SCHEMA_V2),
                    plan_hash=f.plan.plan_hash(),topology_program=deepcopy(f.program))
    f.route.update(plan_hash=f.plan.plan_hash(),final_input_sha256=canonical_hash(f.plan.final_config),
                   input_layer_receipts_sha256=canonical_hash(f.layers),effective_input_sha256=f.config['effective_input_sha256'])
    f.events[0]['receipt'].update(program_sha256=canonical_hash(f.program),participant_control=dict(CONTROL),route_binding=deepcopy(f.route))
    for e in f.events:
        if e['event']=='mutation_begin': e['route_binding']=deepcopy(f.route)
    labels={e['logical_id']:e['participant_id'] for e in f.program['endpoints']}
    group_ids=list(dict.fromkeys(labels.values()))
    identities={g:dict(group_id=g,worker_instance_id='fixture-process-'+g,context_instance_id='fixture-context-'+g,
                pid=1200+group_ids.index(g),pgid=1200+group_ids.index(g),process_start_ticks=9100+group_ids.index(g)) for g in group_ids}
    serial={g:0 for g in group_ids}
    tokens={e['logical_id']:e for e in f.events if e['event']=='endpoint_created'}
    def update(value):
        if isinstance(value,dict):
            eid=value.get('logical_id')
            if eid in labels:
                # Endpoint declarations are not changed into observations.
                if 'pid' in value or 'endpoint_generation' in value:
                    value.update(identities[labels[eid]])
            for v in list(value.values()): update(v)
        elif isinstance(value,list):
            for v in value:update(v)
    update(f.events)
    for eid,t in tokens.items():
        t.update(worker_role=t['role'],rmw_identifier='rmw_cyclonedds_cpp',observed_domain_id=191,
                 domain_observation_source='offline_fixture',**identities[labels[eid]])
    additions=[]
    def row(kind,g,**kw):
        return dict(event=kind,fixture=True,backend='offline_fake',real_ros=False,run_id='fixture-topology',
                    domain_id=191,observed_domain_id=191,rmw_identifier='rmw_cyclonedds_cpp',**identities[g],**kw)
    started=set()
    pending={}
    def request(g,command,eid=None,**kw):
        serial[g]+=1
        address={} if eid is None else {k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation')}
        if command=='create_endpoint': address.pop('endpoint_generation')
        return row('group_command_requested',g,command=command,command_id=serial[g],**address,**kw)
    # Existing snapshot fixture uses the same controller CID for both endpoints;
    # group transport has process-serial addressed CIDs instead.
    for old in f.events:
        kind=old['event'];eid=old.get('logical_id');g=labels.get(eid)
        if kind=='endpoint_created':
            if g not in started:
                additions += [row('process_started',g,fresh_process=True,separate_context=False),
                    row('group_context_created',g,ros_context_created=True,api='rclpy.context.Context+rclpy.init'),
                    row('group_worker_ready',g)]
                started.add(g)
            req=request(g,'create_endpoint',eid);additions.append(req)
            additions += [old,row('endpoint_group_bound',g,endpoint={k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid')})]
            additions.append(row('command_done',g,command='create_endpoint',command_id=req['command_id'],
                                 **{k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid')}))
            continue
        if kind=='worker_command_requested' or kind=='topology_snapshot_requested':
            if g is not None:
                req=request(g,old.get('command','snapshot'),eid)
                pending[eid]=req['command_id'];additions.append(req)
        if eid is not None and 'command_id' in old and eid in pending:
            old['command_id']=pending[eid]
        if kind=='topology_cleanup':
            # Final cleanup is one actual process exit per group, with every endpoint linked.
            for label in group_ids:
                group_tokens=[t for eid,t in tokens.items() if labels[eid]==label]
                # Unknown capture is honest; no declared label->root inference.
                req=request(label,'group_snapshot',group_tokens[0]['logical_id']);additions.append(req)
                obs=dict(schema=OBSERVATION_SCHEMA,requested_grouping=label,supporting_diagnostic=True,candidate_eligible=False,
                    ros_context_application_observed=True,endpoint_ownership_observed=False,local_root_inventory_complete=False,
                    group_relation_observed=False,actual_local_root_count=None,roots_before=None,roots_after=None,endpoints=[],
                    unknown_reasons=['local_root_inventory_incomplete'])
                additions.append(row('participant_group_snapshot',label,command_id=req['command_id'],**obs))
                additions.append(row('command_done',label,command='group_snapshot',command_id=req['command_id'],
                    **{k:group_tokens[0][k] for k in ('logical_id','generation','endpoint_generation')}))
                stop=request(label,'stop');additions += [stop,row('command_done',label,command='stop',command_id=stop['command_id'])]
                for t in group_tokens:
                    additions.append(row('endpoint_destroyed',label,cleanup_status='closed',
                        **{k:t[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid')}))
                additions += [row('group_context_shutdown',label,closed=True,errors=[]),row('process_exit',label,returncode=0)]
        additions.append(old)
    mapping={id(e):i for i,e in enumerate(additions)}
    old_indices={e['event_index']:mapping[id(e)] for e in f.events}
    def refs(v):
        if isinstance(v,dict):
            for k,x in v.items():
                if (k.endswith('_ref') or k.endswith('_index')) and k not in ('action_index','event_index') and type(x) is int and x in old_indices:
                    v[k]=old_indices[x]
                elif isinstance(x,(dict,list)):refs(x)
        elif isinstance(v,list):
            for x in v:refs(x)
    refs(additions)
    for i,e in enumerate(additions):e.update(event_index=i,monotonic_ns=1000+i*1000,time_ns=1700000000000000000+i*1000)
    f.events=additions
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    f.save(root)
    lineage=project_lineage(f.program,f.events,str(f.raw),config=f.config)
    f.runtime['execution_metadata']={'schema':'r2f.topology-execution-metadata/v2','participant_groups':lineage,'workers':lineage['workers']}
    return f


def facts(f):
    return TopologyFactProducer().produce({'events':f.events,'runtime':f.runtime,'config':f.config})


def resave(f):
    f.raw.write_text(''.join(json.dumps(e,sort_keys=True)+'\n' for e in f.events))
    f.runtime.update(events=deepcopy(f.events),raw_sha256=hashlib.sha256(f.raw.read_bytes()).hexdigest())


def test_forged_receive_context_is_rejected_even_after_rehash(tmp_path):
    f=group_fixture(tmp_path)
    receive=next(e for e in f.events if e['event']=='receive')
    receive['context_instance_id']='forged-context'
    resave(f)
    result=facts(f)
    assert result.get('evidence_error'),result
    assert not result['candidate_eligible']

@pytest.mark.parametrize('shared',[True,False])
def test_fixture_group_lineage_preserves_unknown_ownership_and_no_authority(tmp_path,shared):
    f=group_fixture(tmp_path,shared)
    result=facts(f)
    assert result['diagnostic_valid'],result
    assert result['endpoint_identity_complete']
    assert result['participant_group_relation']['observed'] is False
    assert result['participant_group_observations'][0]['unknown_reasons']==['local_root_inventory_incomplete']
    assert not result['candidate_eligible'] and not result['seal_eligible'] and not result['execution_evidence']
    assert len(f.runtime['execution_metadata']['participant_groups']['processes'])==(1 if shared else 2)

@pytest.mark.parametrize('damage', ['missing_start','extra_exit','context','generation','gid','missing_cleanup',
    'extra_endpoint','missing_capture','forged_relation','request_ack','process_label','input_binding','nonce_reuse','raw_hash'])
def test_public_consumer_rejects_group_forgery_without_changing_original(tmp_path,damage):
    f=group_fixture(tmp_path)
    original=f.raw.read_bytes()
    if damage=='missing_start':next(e for e in f.events if e['event']=='process_started')['event']='missing_process_start'
    elif damage=='extra_exit':
        e=deepcopy(next(e for e in f.events if e['event']=='process_exit'));e['event_index']=len(f.events);f.events.append(e)
    elif damage=='context':next(e for e in f.events if e['event']=='endpoint_created')['context_instance_id']='foreign'
    elif damage=='generation':next(e for e in f.events if e['event']=='endpoint_created')['generation']=2
    elif damage=='gid':next(e for e in f.events if e['event']=='endpoint_group_bound')['endpoint']['endpoint_gid']='forged'
    elif damage=='missing_cleanup':next(e for e in f.events if e['event']=='endpoint_destroyed')['event']='missing_endpoint_cleanup'
    elif damage=='extra_endpoint':next(e for e in f.events if e['event']=='endpoint_created')['logical_id']='unlisted'
    elif damage=='missing_capture':next(e for e in f.events if e['event']=='participant_group_snapshot')['event']='missing_capture'
    elif damage=='forged_relation':next(e for e in f.events if e['event']=='participant_group_snapshot')['group_relation_observed']=True
    elif damage=='request_ack':next(e for e in f.events if e['event']=='command_done')['logical_id']='r0'
    elif damage=='process_label':next(e for e in f.events if e['event']=='process_started')['group_id']='foreign'
    elif damage=='input_binding':f.events[0]['receipt'].pop('participant_control')
    elif damage=='nonce_reuse':
        ts=[e for e in f.events if e['event']=='endpoint_created'];ts[1]['endpoint_generation']=ts[0]['endpoint_generation']
    else:f.runtime['raw_sha256']='0'*64
    # Preserve the original separately, then authenticate the intentionally altered fixture.
    (tmp_path/'original.raw.jsonl').write_bytes(original)
    if damage!='raw_hash':resave(f)
    result=facts(f)
    assert result.get('evidence_error'),(damage,result)
    assert not result['candidate_eligible'] and not result['execution_evidence']
    assert (tmp_path/'original.raw.jsonl').read_bytes()==original


def test_legacy_metadata_cannot_select_group_contract(tmp_path):
    f=RawFixture().save(tmp_path)
    f.runtime['execution_metadata']={'schema':'r2f.topology-execution-metadata/v2','participant_groups':{'declared':True}}
    result=facts(f)
    assert result.get('evidence_error')=='group_metadata_without_sealed_opt_in'


@pytest.mark.parametrize('lifecycle', [False, True], ids=['create-only', 'destroy-recreate'])
def test_existing_runner_carrier_capsule_replay_keep_exact_opt_in_and_fixture_exclusion(tmp_path, lifecycle):
    from unittest.mock import patch
    from r2f.campaign import runner,replay
    from r2f.reports import run_report
    from r2f.campaign.corpus import topology_corpus_from_profile
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler,required_catalog_hash
    from r2f.modules.s1 import fuzz_mutators_v2,input_model
    from r2f.runtime.ros2_pair import run_pair
    from r2f.runtime.topology_worker import OfflineTopologyBackend
    from r2f.core import ReplayCapsule
    from test_topology_cycle import profile,SOURCE
    from test_topology_participant_control_input import controlled
    doc=profile(1);doc['rmws']=['rmw_cyclonedds_cpp']
    source=fuzz_mutators_v2.corpus_v2()
    selected=next(e for e in source['entries'] if e['seed_id']=='topology.multi_endpoint')
    selected['document']=controlled()
    if lifecycle:
        from test_s1_topology_model import action
        selected['document']['actions'] += [
            action('destroy_r0', 'destroy', 'r0', 1),
            action('recreate_r0', 'recreate', 'r0', 2)]
    selected['content_sha256']=input_model.content_hash(selected['document'])
    corpus=topology_corpus_from_profile(doc,source,cycle_id='topology-authored')
    feedback=FeedbackLog();scheduler=ExploratoryScheduler(corpus,feedback,rng_seed=17,warmup=0)
    captured=[]
    def offline(config,directory):
        captured.append(deepcopy(config))
        return run_pair(config,directory,topology_backend=OfflineTopologyBackend())
    before=required_catalog_hash()
    with patch.object(runner,'source_manifest',return_value=deepcopy(SOURCE)),patch.object(replay,'source_manifest',return_value=deepcopy(SOURCE)),patch.object(run_report,'source_manifest',return_value=deepcopy(SOURCE)):
        result=runner.run_exploratory_cycle(tmp_path/'cycle',corpus=corpus,feedback=feedback,scheduler=scheduler,
            profile=doc,max_inputs=1,runtime=offline,execute=True,run_id='offline-group-fixture',domain_id=80)
        row=result['rows'][0]
        capsule=ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
        p=capsule.input['mutation_plan']['final_config']['topology_program']
        assert p['participant_control']==CONTROL
        if lifecycle:
            assert [(a['action_id'], a['op'], a['endpoint_id'], a['generation'])
                    for a in p['actions'] if a['op'] in ('destroy', 'recreate')] == [
                        ('destroy_r0', 'destroy', 'r0', 1),
                        ('recreate_r0', 'recreate', 'r0', 2)]
        assert captured[0]['topology_program']==p
        config=json.loads(Path(row['runtime_config_path']).read_text())
        assert config['topology_program']==row['final_input']['topology_program']==p
        assert capsule.input['effective_input']['final_input']['topology_program']==p
        assert row['input_layer_receipts'][0]['input_before']==selected['document']
        # Plain OfflineTopologyBackend lacks actual group context/lineage; keep that unknown.
        runtime=json.loads(Path(row['runtime_result_path']).read_text())
        assert runtime['execution_metadata']['schema']=='r2f.topology-execution-metadata/v2'
        assert runtime['execution_metadata']['participant_groups'].get('error')
        assert not row['actual_run'] and not row['candidate_eligible'] and not row['seal_eligible']
        report=json.loads(Path(result['report_path']).read_text())
        assert report['rows'][0]['typed_facts']==row['typed_facts']
        assert row['typed_facts']['fixture']
        assert not row['typed_facts']['execution_evidence']
        assert not row['typed_facts']['candidate_eligible'] and not row['typed_facts']['seal_eligible']
        assert row['typed_facts']['unknown_reasons']
        assert row['oracle_verdict']=='unknown'
        assert runtime['execution_metadata']['participant_groups']['group_relation_observed'] is False
        originals={path: path.read_bytes() for path in (tmp_path/'cycle').rglob('*') if path.is_file()}
        attempt=replay.replay_cell(row['replay_path'],output_dir=tmp_path/'replay',domain_id=81,runtime=offline)
        assert captured[-1]['topology_program']==p
        assert all(path.read_bytes()==saved for path,saved in originals.items())
        assert not attempt['real_ros_execution'] and not attempt['maturity_denominator_changed']
        assert attempt['original']['effective_input_sha256']==row['effective_input_sha256']
        assert attempt['original']['capsule_sha256']==hashlib.sha256(originals[Path(row['replay_path'])]).hexdigest()
        derived=ReplayCapsule.from_dict(json.loads((tmp_path/'replay'/'derived_capsule.json').read_text()))
        assert derived.input['mutation_plan']==capsule.input['mutation_plan']
        # Check the persisted action receipts, not only the submitted program.
        # This backend deliberately has no actual participant-group observations.
        replay_runtime=json.loads((tmp_path/'replay'/'runtime'/'runtime_result.json').read_text())
        for saved_runtime in (runtime, replay_runtime):
            assert saved_runtime['fixture'] and not saved_runtime['real_execution']
            assert not saved_runtime['candidate_eligible'] and not saved_runtime['sealed']
            events=[json.loads(line) for line in Path(saved_runtime['raw_path']).read_text().splitlines()]
            done=[event for event in events if event['event']=='topology_action_done']
            assert [event['action'] for event in done]==p['actions']
            assert [event['receipt']['action'] for event in done]==p['actions']
            assert saved_runtime['input_receipts']['validation_status']=='valid'
            assert saved_runtime['input_receipts']['program_sha256']==canonical_hash(p)
            expected=[('w0', 1), ('r0', 1)] + ([('r0', 2)] if lifecycle else [])
            created=[event for event in events if event['event']=='endpoint_created']
            assert [(event['logical_id'],event['generation']) for event in created]==expected
            assert len({event['endpoint_generation'] for event in created})==len(expected)
            if lifecycle:
                receipts={event['action_id']:event['receipt'] for event in done}
                assert receipts['destroy_r0']['disposition']=='applied'
                assert receipts['destroy_r0']['after']=={'alive': False, 'generation': 1}
                assert receipts['recreate_r0']['disposition']=='applied'
                assert receipts['recreate_r0']['after']['generation']==2
                assert receipts['recreate_r0']['after']['endpoint_generation']==created[-1]['endpoint_generation']
    assert required_catalog_hash()==before


def test_boundary_rejects_group_schema_without_authenticated_kernel_sidecar(tmp_path):
    from r2f.campaign.boundary_audit import audit_execution_boundary
    f=group_fixture(tmp_path)
    result=audit_execution_boundary(f.config,f.runtime,output_root=tmp_path)
    assert not result['valid'] and not result['candidate_eligible']


def test_receive_cannot_drop_group_metadata_to_select_legacy_lineage(tmp_path):
    f=group_fixture(tmp_path)
    receive=next(e for e in f.events if e['event']=='receive')
    for key in ('group_id','worker_instance_id','context_instance_id'):receive.pop(key)
    resave(f)
    result=facts(f)
    assert result.get('evidence_error'),result

@pytest.mark.parametrize('schema',['r2f.topology-execution-metadata/v1',None])
def test_legacy_input_cannot_select_group_lineage_by_metadata_field(tmp_path,schema):
    f=RawFixture().save(tmp_path)
    f.runtime['execution_metadata']={'schema':schema,'participant_groups':{'declared':True}}
    result=facts(f)
    assert result.get('evidence_error')=='group_metadata_without_sealed_opt_in'


@pytest.mark.parametrize('shared', [True, False], ids=['shared', 'split'])
@pytest.mark.parametrize('damage', [None, 'missing', 'group_id', 'worker_instance_id',
    'context_instance_id', 'pgid', 'pid', 'process_start_ticks', 'endpoint_generation'])
def test_public_snapshot_requests_remain_consumable_with_exact_worker_identity(
        tmp_path, monkeypatch, shared, damage):
    """Actual carrier request shape -> existing public facts seam, offline only.

    The OS transport is the external double. The established complete protocol
    fixture supplies action/graph/delivery evidence, not worker observations.
    Rebind observed identities and command serials into that fixture namespace;
    absent or mismatched request identities must survive and fail consumption.
    """
    from test_topology_participant_control_input import controlled
    from test_topology_participant_control_worker import transport, prepared_backend

    transport.__wrapped__(monkeypatch, tmp_path / 'transport')
    program = controlled(shared)
    backend = prepared_backend(tmp_path / 'carrier', program)
    emitted = []
    backend.set_event_sink(lambda event, **fields: emitted.append(dict(event=event, **fields)))
    try:
        writer = backend.create_endpoint(program['endpoints'][0], 1, 'writer')
        reader = backend.create_endpoint(program['endpoints'][1], 1, 'reader')
        snapshot = backend.snapshot({'w0': writer, 'r0': reader})
    finally:
        backend.close()
    assert snapshot['complete']
    requests = {e['logical_id']: deepcopy(e) for e in emitted
                if e['event'] == 'topology_snapshot_requested'}
    observed = {e['logical_id']: e for e in emitted if e['event'] == 'endpoint_created'}
    assert set(requests) == set(observed) == {'w0', 'r0'}
    (tmp_path / 'carrier-events.json').write_text(json.dumps(emitted, indent=2) + '\n')

    fixture = group_fixture(tmp_path / 'facts', shared=shared)
    # Independent complete transcript: this event type is accepted when its
    # worker/endpoint linkage is complete, regardless of the carrier under test.
    for event in fixture.events:
        if event['event'] == 'worker_command_requested' and event.get('command') == 'snapshot':
            event['event'] = 'topology_snapshot_requested'
    resave(fixture)
    control = facts(fixture)
    assert control['diagnostic_valid'], control
    control_raw = tmp_path / 'control.raw.jsonl'
    control_raw.write_bytes(fixture.raw.read_bytes())
    original = control_raw.read_bytes()
    identity = ('logical_id', 'generation', 'endpoint_gid', 'endpoint_generation',
                'pid', 'pgid', 'process_start_ticks', 'group_id',
                'worker_instance_id', 'context_instance_id')
    if damage == 'missing':
        for key in ('pgid', 'group_id', 'worker_instance_id', 'context_instance_id'):
            requests['w0'].pop(key, None)
    elif damage is not None:
        requests['w0'][damage] = -1 if damage in ('pid', 'pgid', 'process_start_ticks') else 'foreign-incarnation'
    tokens = {e['logical_id']: e for e in fixture.events if e['event'] == 'endpoint_created'}
    for event in fixture.events:
        if event['event'] != 'topology_snapshot_requested':
            continue
        eid = event['logical_id']
        request, source, token = requests[eid], observed[eid], tokens[eid]
        # Only rename values actually observed in the emitted request. Never
        # fill omissions from declarations, config or the reference transcript.
        for key in identity:
            if key not in request:
                event.pop(key, None)
            else:
                event[key] = token[key] if request[key] == source.get(key) else request[key]
    resave(fixture)
    consumed_raw = fixture.raw.read_bytes()
    result = facts(fixture)
    (tmp_path / 'consumer-result.json').write_text(json.dumps({
        'shared': shared, 'damage': damage, 'control_diagnostic_valid': control['diagnostic_valid'],
        'requests': list(requests.values()), 'facts': result}, indent=2) + '\n')
    assert not result['candidate_eligible'] and not result['execution_evidence']
    if damage is None:
        assert result['diagnostic_valid'], result
        for eid, request in requests.items():
            assert {key: request[key] for key in identity} == {key: observed[eid][key] for key in identity}
    else:
        assert result['evidence_error'] == 'group_lineage:endpoint_event_context_or_incarnation', result
    # A consumer must never rewrite its input transcript or the independent control.
    assert fixture.raw.read_bytes() == consumed_raw
    assert control_raw.read_bytes() == original
