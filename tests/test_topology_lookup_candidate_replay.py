"""Lookup candidate/replay schema controls, not DDS or natural admission evidence.

Qualifications in model() are synthetic and raw retains a test_fixture marker.
The assertions exercise the existing candidate ledger and replay signature seams.
"""
from copy import deepcopy
import pytest
from test_topology_candidate_identity import row, validate
from test_topology_conformance_signature import model, read, rewrite
from test_topology_lookup_contract import lookup_trace
from r2f.campaign.topology_lookup_observation import lookup_observations, semantic_checks
from r2f.modules.s1.topology_lookup_contract import predicate_contract
from r2f.core.replay import conformance_signature
from r2f.reports.candidate_report import exploratory_candidate_ledger


def lookup_model(row):
    model(row)
    program,trace=lookup_trace('type')
    events=read(row)
    for event in events:
        event['monotonic_ns']=100+event['event_index']
    token=events[0]
    token.update(pgid=token['pid'],domain_id=191,message_type='fixture/Type',rmw_identifier='rmw_cyclonedds_cpp')
    # Retain current raw incarnation; the lookup result comes from that worker.
    for event in trace[1:]:
        if event['event'] in ('topic_lookup_result','command_done'):
            event.update({k:token[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid',
                'pid','pgid','process_start_ticks','role','run_id','domain_id','rmw_identifier','message_type')})
        if 'lookup_receipt' in event:
            receipt=event['lookup_receipt'];receipt['local_gid']=token['endpoint_gid']
            for name in ('ownership_before','ownership_after'):
                receipt[name]['local_gid']=token['endpoint_gid']
            if 'worker_identity' in receipt:
                receipt['worker_identity']={k:token[k] for k in receipt['worker_identity']}
        if event['event']=='topic_lookup_observation':event['endpoint_gid']=token['endpoint_gid']
        event.update(event_index=len(events),monotonic_ns=100+len(events),run_id=row['run_id'],fixture=False,
                     test_fixture='pure-offline-lookup-contract')
        events.append(event)
    row['final_input']['topology_program']['actions']=[deepcopy(program['actions'][1])]
    rewrite(row,events)
    observed=lookup_observations(row['final_input']['topology_program'],events,
        ref=lambda e:dict(path=row['raw_path'],sha256=row['raw_sha256'],event_index=e['event_index']))
    assert not observed['unknown_reasons'],observed
    row['typed_facts']['topic_lookup_observations']=observed
    row['oracle'].update(lookup_checks=semantic_checks(observed),lookup_rule_contract=predicate_contract())
    row['oracle']['forbidden_residuals'].append('topology.topic_lookup_mismatch')
    return row


def test_raw_bound_lookup_has_stable_semantic_replay_signature(row):
    lookup_model(row)
    signature=conformance_signature(row)
    assert signature is not None
    lookup,=signature['topic_lookup_signature']
    assert lookup['status']=='mismatch' and lookup['detail']==['result_type_differs']
    assert lookup['violation']=='topology.topic_lookup_mismatch'
    assert any(a.get('residual')=='topology.topic_lookup_mismatch' for a in signature['typed_anomaly_signature'])
    # Only process-local native handles change; semantic result must remain equal.
    events=read(row)
    for event in events:
        receipt=event.get('lookup_receipt')
        if receipt:
            receipt['participant_handle']+=100
            receipt['result_participant_handle']+=100
            for name in ('ownership_before','ownership_after'):
                receipt[name]['participant_handle']+=100;receipt[name]['local_handle']+=100
    rewrite(row,events)
    row['typed_facts']['topic_lookup_observations']=lookup_observations(row['final_input']['topology_program'],events,
        ref=lambda e:dict(path=row['raw_path'],sha256=row['raw_sha256'],event_index=e['event_index']))
    assert conformance_signature(row)==signature


@pytest.mark.parametrize('damage',['raw_receipt','wrong_ack','contract','fact_claim','oracle_claim','missing_raw'])
def test_lookup_replay_and_candidate_reject_divergent_claims(row,damage):
    lookup_model(row);events=read(row)
    if damage=='raw_receipt':
        for event in events:
            if 'lookup_receipt' in event:event['lookup_receipt']['ownership_after']['local_gid']='other'
    elif damage=='wrong_ack':next(e for e in events if e['event']=='command_done')['command_id']+=1
    elif damage=='contract':row['oracle']['lookup_rule_contract']['rule_id']='unregistered'
    elif damage=='fact_claim':row['typed_facts']['topic_lookup_observations']['checks'][0]['detail']=['invented']
    elif damage=='oracle_claim':row['oracle']['lookup_checks'][0]['detail']=['invented']
    else:next(e for e in events if e['event']=='topic_lookup_result')['event']='not_lookup'
    rewrite(row,events)
    assert conformance_signature(row) is None
    with pytest.raises(ValueError, match='topic lookup'):
        validate(row)
    # Candidate intake must reject the same raw/semantic divergence, even when
    # there is an independent modeled pair anomaly in this fixture.
    ledger=exploratory_candidate_ledger([row])
    assert ledger['natural_candidate_count']==0
    assert all(c.get('admission_error') for c in ledger.get('candidates',[])) or not ledger.get('candidates')


def test_lookup_only_anomaly_reaches_existing_identity_validation(row):
    lookup_model(row)
    row['typed_facts']['pairs']=[]
    row['oracle']['forbidden_residuals']=['topology.topic_lookup_mismatch']
    row['oracle']['closure']={'pairs':[]}
    evidence=validate(row)
    assert evidence['endpoint_identity']['endpoint_lineage']
    signature=conformance_signature(row)
    assert signature and len(signature['typed_anomaly_signature'])==1
    assert signature['typed_anomaly_signature'][0]['residual']=='topology.topic_lookup_mismatch'
