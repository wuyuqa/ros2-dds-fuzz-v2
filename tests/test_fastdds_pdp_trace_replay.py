"""PDP contradictions must fail common replay preflight before dispatch."""
from test_fastdds_peer_replay import bound, no_native_load, _persist_raw, project
import pytest


def test_default_blackbox_cannot_hide_native_pdp_raw(bound):
    for item in (bound.execution,bound.execution['runtime_execution_metadata'],bound.events[0]['metadata']):
        item.pop('observation_mode',None);item.pop('fastdds_peer_instrumentation',None)
    _persist_raw(bound)
    assert project(bound).get('observation_mode','blackbox')=='blackbox'
    bound.events.append({'event':'native_pdp_lifecycle','fixture':True})
    _persist_raw(bound)
    with pytest.raises(ValueError,match='pdp_trace:unselected_observer_has_raw'):project(bound)


def test_replay_rejects_pdp_selection_linked_to_foreign_peer_manifest(bound,tmp_path):
    from copy import deepcopy
    from pdp_trace_fixtures import authored_pdp, complete_rows
    from r2f.core import raw_sha256
    root=tmp_path/'pdp';root.mkdir()
    path,peer,trace=authored_pdp((bound.manifest_path,bound.manifest),root)
    reference={'manifest_path':str(path),'manifest_sha256':raw_sha256(path)}
    for item in (bound.execution,bound.execution['runtime_execution_metadata'],bound.events[0]['metadata']):
        item['fastdds_peer_instrumentation']=deepcopy(reference)
    copied=bound.raw_path.parent/'native-peer-build-manifest.json';copied.write_bytes(path.read_bytes())
    rawref=dict(observation_mode='instrumentation',manifest_path=str(copied),manifest_sha256=reference['manifest_sha256'])
    rows=complete_rows(path,peer,trace)
    for row in rows:
        row.update(run_id=bound.execution['run_id'],domain_id=bound.execution['domain_id'])
        if row['event']=='native_pdp_provider_selected':row['peer_manifest_reference']=deepcopy(rawref)
    bound.events.extend(rows);_persist_raw(bound)
    assert project(bound)['observation_mode']=='instrumentation'
    chosen=next(e for e in rows if e['event']=='native_pdp_provider_selected')
    chosen['peer_manifest_reference']['manifest_path']='/foreign/native-peer-build-manifest.json'
    _persist_raw(bound)
    with pytest.raises(ValueError,match='pdp_trace:worker_selection_mismatch'):project(bound)


@pytest.mark.parametrize('selection',['instrumentation','default_blackbox'])
def test_refresh_raw_contradiction_is_rejected_before_common_replay_dispatch(bound,tmp_path,monkeypatch,selection):
    from copy import deepcopy
    import json
    from unittest.mock import Mock
    from r2f.campaign import replay
    assert project(bound)['observation_mode']=='instrumentation'
    if selection=='default_blackbox':
        for record in (bound.execution,bound.execution['runtime_execution_metadata'],bound.events[0]['metadata']):
            record.pop('observation_mode',None);record.pop('fastdds_peer_instrumentation',None)
    bound.events.append(dict(event='participant_refresh_result',command_id=7,refresh_receipt=dict(status='applied')))
    _persist_raw(bound)  # Consistent raw hash does not make an undeclared control valid.
    with pytest.raises(ValueError,match='refresh_unbound_worker_result'):
        project(bound)
    path=tmp_path/'original-input/capsule.json';path.parent.mkdir();path.write_text(json.dumps(bound.capsule.to_dict()))
    for name in ('report.json','runtime_config.json','runtime/runtime_result.json'):
        artifact=path.parent/name;artifact.parent.mkdir(exist_ok=True);artifact.write_text('{}')
    monkeypatch.setattr(replay,'source_manifest',lambda _:deepcopy(bound.capsule.input['source_manifest']))
    # Load the authored in-memory capsule at the serialization seam; it is not real ROS evidence.
    monkeypatch.setattr(replay.ReplayCapsule,'from_dict',lambda data:bound.capsule)
    runtime=Mock(side_effect=AssertionError('dispatch_forbidden_for_contradictory_raw'))
    result=replay.replay_cell(path,output_dir=tmp_path/'replay',domain_id=81,runtime=runtime)
    runtime.assert_not_called()
    assert result['status']=='fail_artifact' and result['reason_code']=='observation_execution_metadata_invalid', result.get('failure_reason')


def _refresh_original(bound,tmp_path):
    """Authored original at external ABI/artifact seams; never DDS evidence."""
    from copy import deepcopy
    from pathlib import Path
    import json
    from r2f.core import ReplayCapsule,Scenario,canonical_hash,raw_sha256
    from r2f.modules.s1.topology_input import make_topology_plan
    from test_fastdds_participant_refresh import authored_refresh_chain,refresh_program
    from pdp_trace_fixtures import complete_rows
    from test_topology_cycle import capsule_from_registry,SOURCE
    root=tmp_path/'refresh';root.mkdir()
    events,action,receipt,begin,done,reference=authored_refresh_chain((bound.manifest_path,bound.manifest),root)
    program=refresh_program('new');action=program['actions'][1]
    receipt.update(action=action,step_id='action:refresh',disposition='effect_observed',satisfied=True,
                   requires=[dict(id='native_participant_refresh_receipt',status='satisfied')])
    begin.update(action=action,action_id='refresh',action_index=1,before=receipt['before'])
    done.update(action=action,action_id='refresh',action_index=1,receipt=deepcopy(receipt))
    events.insert(-1,dict(event='state_step_after',monotonic_ns=125,monotonic_clock_identity=deepcopy(begin['monotonic_clock_identity']),**deepcopy(receipt)))
    plan=make_topology_plan(program,[dict(operator_id='action.insert')],rng_seed=7)
    begin['action_index']=done['action_index']=next(i for i,a in enumerate(plan.final_config['topology_program']['actions']) if a['action_id']=='refresh')
    registry,original,_,_=capsule_from_registry(2)
    scenario=Scenario(original.target,'rmw_fastrtps_cpp','mutated',0,7,
        dict(mutation_kind='endpoint_program',carrier='ros2_endpoint',mutation_parameters=plan.final_config),plan)
    prepared=registry.get_services(scenario.target_id).mutation_provider.prepare_plan(scenario,plan)
    capsule=ReplayCapsule.from_scenario(scenario,source_manifest=SOURCE,final_input=plan.final_config,
        input_layer_receipts=prepared['input_layer_receipts'],origin_domain=80)
    execution=deepcopy(bound.execution)
    for key in ('target_id','target_revision','target','seed','rmw','arm','repeat','mutation_kind','carrier',
                'effective_input_sha256','input_identity_schema','source_manifest','mutation_parameters','state_steps','mutation_plan'):
        execution[key]=deepcopy(capsule.input[key])
    execution.update(final_input=deepcopy(plan.final_config),input_layer_receipts=deepcopy(prepared['input_layer_receipts']),
        fastdds_peer_instrumentation={k:reference[k] for k in ('manifest_path','manifest_sha256')})
    context={k:execution[k] for k in ('run_id','target_id','target_revision','rmw','domain_id','arm','repeat')}
    context.update(input_identity_schema=capsule.input['input_identity_schema'],effective_input_sha256=capsule.input['effective_input_sha256'],plan_hash=plan.plan_hash())
    metadata=deepcopy(execution['runtime_execution_metadata'])
    metadata.update(sealed_execution_context=context,input_program_sha256=canonical_hash(plan.final_config['topology_program']),
                    fastdds_peer_instrumentation=deepcopy(execution['fastdds_peer_instrumentation']))
    execution['runtime_execution_metadata']=metadata
    peer=json.loads(Path(reference['manifest_path']).read_text())
    trace=json.loads(Path(peer['pdp_trace']['manifest_path']).read_text())
    provider_rows=complete_rows(Path(reference['manifest_path']),peer,trace)
    copied=bound.raw_path.parent/'native-peer-build-manifest.json';copied.write_bytes(Path(reference['manifest_path']).read_bytes())
    rawref=dict(reference,manifest_path=str(copied))
    receipt['after']['participant_refresh']['manifest_reference']=rawref
    next(e for e in events if e['event']=='participant_refresh_result')['refresh_receipt']['manifest_reference']=deepcopy(rawref)
    events[-1]['receipt']['after']['participant_refresh']['manifest_reference']=deepcopy(rawref)
    next(e for e in events if e['event']=='state_step_after')['after']['participant_refresh']['manifest_reference']=deepcopy(rawref)
    for row in provider_rows:
        if row['event']=='native_pdp_provider_selected':row['peer_manifest_reference']=deepcopy(rawref)
    rows=[dict(event='topology_execution_metadata',metadata=deepcopy(metadata)),*provider_rows[:2],*events,*provider_rows[3:]]
    for index,row in enumerate(rows):row.update(event_index=index,run_id=execution['run_id'],domain_id=execution['domain_id'])
    capsule.execution=execution;bound.capsule=capsule;bound.execution=execution;bound.events=rows
    _persist_raw(bound)
    return bound


@pytest.mark.parametrize('damage',['remove_both','terminal'])
@pytest.mark.parametrize('selection',['instrumentation','no_hints_blackbox'])
def test_canonical_refresh_terminal_contradictions_cannot_dispatch(bound,tmp_path,monkeypatch,damage,selection):
    from copy import deepcopy
    import json
    from unittest.mock import Mock
    from r2f.campaign import replay
    bound=_refresh_original(bound,tmp_path)
    assert project(bound)['observation_mode']=='instrumentation'
    if damage=='remove_both':
        bound.events[:]=[e for e in bound.events if e['event'] not in {'participant_refresh_observation','participant_refresh_result'}]
        for i,e in enumerate(bound.events):e['event_index']=i
    else:
        done=next(e for e in bound.events if e['event']=='topology_action_done')
        done['receipt']['after']['participant_refresh']['after_hex']='00'
    if selection=='no_hints_blackbox':
        for record in (bound.execution,bound.execution['runtime_execution_metadata'],bound.events[0]['metadata']):
            for key in ('observation_mode','fastdds_peer_instrumentation','observe_native_peers'):record.pop(key,None)
        bound.events[:]=[e for e in bound.events if not (e['event'].startswith(('native_','participant_refresh_','topology_participant_refresh_')) or e['event'] in {'worker_cleanup_step','worker_shutdown'})]
        for i,e in enumerate(bound.events):e['event_index']=i
    _persist_raw(bound)
    with pytest.raises(ValueError,match='refresh_'):project(bound)
    path=tmp_path/'original-input/capsule.json';path.parent.mkdir();path.write_text(json.dumps(bound.capsule.to_dict()))
    for name in ('report.json','runtime_config.json','runtime/runtime_result.json'):
        artifact=path.parent/name;artifact.parent.mkdir(exist_ok=True);artifact.write_text('{}')
    monkeypatch.setattr(replay,'source_manifest',lambda _:deepcopy(bound.capsule.input['source_manifest']))
    monkeypatch.setattr(replay.ReplayCapsule,'from_dict',lambda data:bound.capsule)
    runtime=Mock(side_effect=AssertionError('dispatch_forbidden_for_contradictory_raw'))
    result=replay.replay_cell(path,output_dir=tmp_path/'replay',domain_id=81,runtime=runtime)
    runtime.assert_not_called()
    assert result['status']=='fail_artifact' and result['reason_code']=='observation_execution_metadata_invalid',result.get('failure_reason')
