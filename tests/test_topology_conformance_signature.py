"""Pure offline signature fixtures. NO actual DDS, natural candidate or admission.

Eligibility booleans below model a future qualified producer contract; they are
NOT qualification evidence. Raw events carry a test_fixture marker; modeled
qualification flags do not claim that any real runtime was executed.
"""
from copy import deepcopy
import json

import pytest

from test_topology_candidate_identity import row  # existing frozen fixture only
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.replay import conformance_signature, TOPOLOGY_SIGNATURE_SCHEMA


def rewrite(row, events):
    from pathlib import Path
    path = row['raw_path']
    Path(path).write_text(''.join(json.dumps(e) + '\n' for e in events))
    digest = raw_sha256(path)
    def bind(value):
        if isinstance(value, dict):
            if 'event_index' in value and 'path' in value:
                value.update(path=path, sha256=digest)
            for v in value.values(): bind(v)
        elif isinstance(value, list):
            for v in value: bind(v)
    row['raw_sha256'] = digest
    row['typed_facts']['raw'] = dict(path=path, sha256=digest)
    bind(row)


def read(row):
    from pathlib import Path
    return [json.loads(s) for s in Path(row['raw_path']).read_text().splitlines()]


def model(row, kind='gap'):
    facts = row['typed_facts']
    # Modeled flags for schema tests only, never written to real artifacts.
    facts.update(fixture=False, candidate_eligible=True, diagnostic_valid=True, tool_error=False,
                 execution_evidence=True, source_provenance_qualified=True, environment_healthy=True,
                 process_healthy=True, observation_complete=True, endpoint_identity_complete=True)
    row.update(candidate_eligible=True, actual_run=True, input_identity_schema='r2f.input-identity/v2',
               effective_input_sha256='a' * 64, rule_id='s1.topology.program_consistency.v1', oracle_verdict='fail',
               mutation_receipt={k: True for k in ('carrier_admitted', 'configured', 'applied', 'hit', 'effect_observed')})
    for endpoint in row['final_input']['topology_program']['endpoints']:
        endpoint.update(topic='/fixture/topic',type_name='fixture/Type',qos={'reliability':'reliable','depth':16})
    pair = facts['pairs'][0]
    pair.update(reliable_history_contract=True, filter_free=True, delivery_window_identity_complete=True,
                compatibility='compatible' if kind == 'gap' else 'incompatible',
                delivery_closure='closed_with_gap' if kind == 'gap' else 'not_applicable')
    events = read(row)
    for ev in events:
        ev.update(fixture=False,test_fixture='pure-offline-unit-model')
    def emit(event, **kw):
        ev = dict(event=event, event_index=len(events), run_id=row['run_id'], **kw)
        events.append(ev)
        return dict(path=row['raw_path'], sha256=row['raw_sha256'], event_index=ev['event_index'])
    opened = emit('topology_delivery_window_open', writer='w0', writer_generation=1, reader='r0', reader_generation=1)
    refs = []; samples = []
    for seq in [0, 1, 2]:
        send = emit('send', logical_id='w0', generation=1, endpoint_generation='instance-w0',
                    send_sequence=seq, payload_sha256='payload-' + str(seq))
        refs.append(send)
        if seq == 1 and kind == 'gap': continue
        receive = emit('receive', logical_id='r0', generation=1, endpoint_generation='instance-r0',
                       send_sequence=seq, payload_sha256='payload-' + str(seq), payload_writer_id='w0',
                       payload_writer_generation=1, payload_run_id=row['run_id'], payload_endpoint_gid='gid-w0')
        refs.append(receive)
        samples.append(dict(writer='w0', writer_generation=1, reader='r0', reader_generation=1,
                            sequence=seq, send_ref=send, receive_ref=receive))
    missing = [1] if kind == 'gap' else []
    received = [s['sequence'] for s in samples]
    closed = emit('topology_delivery_window_closed', writer='w0', reader='r0', writer_generation=1,
                  reader_generation=1, window_complete=True, open_ref=opened['event_index'], sentinel_sequence=2,
                  sentinel_send_ref=samples[-1]['send_ref']['event_index'],
                  sentinel_receive_ref=samples[-1]['receive_ref']['event_index'],
                  expected_sequences=[0, 1, 2], missing_sequences=missing, received_sequences=received)
    proofrefs = pair['proofs']['observed_match']['refs'] + [opened] + refs + [closed]
    pair['proofs'].update(
        compatibility=dict(status='observed', value=pair['compatibility'], refs=deepcopy(proofrefs)),
        delivery_closure=dict(status='observed', value=pair['delivery_closure'], refs=deepcopy(proofrefs),
                              detail=dict(expected=[0, 1, 2], received=received, missing=missing,
                                          sentinel_sequence=2, open_ref=opened, close_ref=closed)))
    pair['communication_witnesses'] = samples
    row['oracle'] = dict(candidate_eligible=True, verdict='fail', rule_id=row['rule_id'], diagnostic_only=False,
                         forbidden_residuals=['topology.closed_delivery_gap' if kind == 'gap' else 'topology.pair_graph_mismatch'],
                         closure={'pairs': [dict(writer='w0', writer_generation=1, reader='r0', reader_generation=1,
                                                  missing=missing)]})
    rewrite(row, events)
    return row


def test_closed_intermediate_same_writer_gap_signature(row):
    model(row)
    sig = conformance_signature(row)
    assert sig and sig['schema'] == TOPOLOGY_SIGNATURE_SCHEMA
    anomaly, = sig['typed_anomaly_signature']
    assert anomaly['writer'] == dict(logical_id='w0', generation=1)
    assert anomaly['reader'] == dict(logical_id='r0', generation=1)
    assert anomaly['missing'] == [1] and anomaly['sentinel_sequence'] == 2


def test_incompatible_communication_is_concrete_graph_residual(row):
    model(row, 'graph')
    sig = conformance_signature(row)
    assert sig and sig['typed_anomaly_signature'][0]['residual'] == 'topology.pair_graph_mismatch'


@pytest.mark.parametrize('damage', ['tail', 'open', 'no_sentinel', 'other_writer_sentinel', 'other_reader',
    'other_writer_generation', 'other_reader_generation', 'forged_payload', 'different_instance',
    'raw_missing_send', 'wrong_close_pair', 'wrong_close_ref', 'wrong_close_missing', 'incomplete_window',
    'non_reliable', 'filtered', 'incomplete_identity', 'missing_residual', 'extra_residual', 'unknown_residual',
    'false_claim_only', 'unbound_explicit', 'bad_schema', 'other_rule', 'oracle_other_rule', 'noeffect',
    'unqualified_row', 'unqualified_oracle', 'unqualified_facts', 'control', 'fixture', 'tool_error',
    'unknown', 'diagnostic', 'aborted', 'missing_proof', 'aggregate_graph', 'crossrun', 'missing_input_schema'])
def test_fail_closed_signature_negatives(row, damage):
    model(row); facts=row['typed_facts']; pair=facts['pairs'][0]; events=read(row)
    detail=pair['proofs']['delivery_closure']['detail']; close=events[detail['close_ref']['event_index']]
    if damage == 'tail': detail['sentinel_sequence']=1
    elif damage == 'open': pair['delivery_closure']='open'
    elif damage == 'no_sentinel': pair['communication_witnesses'].pop()
    elif damage == 'other_writer_sentinel': pair['communication_witnesses'][-1]['writer']='w1'
    elif damage == 'other_reader': pair['communication_witnesses'][-1]['reader']='r9'
    elif damage == 'other_writer_generation': pair['communication_witnesses'][-1]['writer_generation']=2
    elif damage == 'other_reader_generation': pair['communication_witnesses'][-1]['reader_generation']=2
    elif damage == 'forged_payload': events[pair['communication_witnesses'][-1]['receive_ref']['event_index']]['payload_sha256']='other'
    elif damage == 'different_instance': events[pair['communication_witnesses'][-1]['receive_ref']['event_index']]['endpoint_generation']='other'
    elif damage == 'raw_missing_send': events[5]['event']='not_send'
    elif damage == 'wrong_close_pair': close['reader']='r9'
    elif damage == 'wrong_close_ref': close['open_ref']=0
    elif damage == 'wrong_close_missing': close['missing_sequences']=[]
    elif damage == 'incomplete_window': close['window_complete']=False
    elif damage == 'non_reliable': pair['reliable_history_contract']=False
    elif damage == 'filtered': pair['filter_free']=False
    elif damage == 'incomplete_identity': pair['delivery_window_identity_complete']=False
    elif damage == 'missing_residual': row['oracle']['forbidden_residuals']=[]
    elif damage == 'extra_residual': row['oracle']['forbidden_residuals'].append('topology.pair_graph_mismatch')
    elif damage == 'unknown_residual': row['oracle']['forbidden_residuals']=['topology.future']
    elif damage == 'false_claim_only': facts['pairs']=[]
    elif damage == 'unbound_explicit': facts['pairs']=[];row['typed_anomaly_signature']={'invented':'signature'}
    elif damage == 'bad_schema': facts['schema']='r2f.s1-topology-facts/v999'
    elif damage == 'other_rule': row['rule_id']='some.unvalidated.rule'
    elif damage == 'oracle_other_rule': row['oracle']['rule_id']='other'
    elif damage == 'noeffect': row['mutation_receipt']['effect_observed']=False
    elif damage == 'unqualified_row': row['candidate_eligible']=False
    elif damage == 'unqualified_oracle': row['oracle']['candidate_eligible']=False
    elif damage == 'unqualified_facts': facts['candidate_eligible']=False
    elif damage == 'control': row['control_kind']='fault'
    elif damage == 'fixture': facts['fixture']=True
    elif damage == 'tool_error': row['tool_error']=True
    elif damage == 'unknown': row['oracle_verdict']='unknown'
    elif damage == 'diagnostic': row['oracle']['diagnostic_only']=True
    elif damage == 'aborted': row['execution_stage']='aborted'
    elif damage == 'missing_proof': pair['proofs']['delivery_closure']['refs']=[]
    elif damage == 'aggregate_graph': facts['pairs']={'writer_count':2,'reader_count':1}
    elif damage == 'crossrun': row['run_id']='other-run'
    elif damage == 'missing_input_schema': row.pop('input_identity_schema')
    rewrite(row, events)
    assert conformance_signature(row) is None, damage


def test_explicit_signature_cannot_override_topology_branch(row):
    model(row)
    original = conformance_signature(row)
    row['typed_anomaly_signature'] = {'invented': 'ignore-me'}
    row['typed_facts']['anomaly_signature'] = ['also-ignore-me']
    assert conformance_signature(row) == original


def test_ephemeral_identity_run_path_domain_changes_do_not_change_signature(row, tmp_path):
    from pathlib import Path
    model(row); original=conformance_signature(row); assert original
    events=read(row)
    # Rewrite ONLY synthetic fixture data. Never a real pass row or run artifact.
    aliases={e['endpoint_gid']:'new-'+e['endpoint_gid'] for e in row['typed_facts']['endpoint_lineage']}
    instances={e['endpoint_generation']:'new-'+e['endpoint_generation'] for e in row['typed_facts']['endpoint_lineage']}
    def replace(v):
        if isinstance(v,dict):
            for k,x in list(v.items()):
                if k in ('run_id','payload_run_id'): v[k]='fresh-run'
                elif k in ('pid','process_start_ticks') and type(x) is int: v[k]=x+1000
                elif isinstance(x,str) and x in aliases: v[k]=aliases[x]
                elif isinstance(x,str) and x in instances: v[k]=instances[x]
                else: replace(x)
        elif isinstance(v,list):
            for x in v: replace(x)
    replace(row);replace(events)
    row['raw_path']=str(tmp_path/'different.jsonl');row['domain_id']=222
    rewrite(row,events)
    assert conformance_signature(row)==original


@pytest.mark.parametrize('role', ['writer','reader'])
def test_endpoint_generation_semantics_do_not_merge(row, role):
    model(row); original=conformance_signature(row); assert original
    endpoint='w0' if role=='writer' else 'r0';events=read(row)
    for e in row['typed_facts']['endpoint_lineage']:
        if e['logical_id']==endpoint:e['generation']=2
    for ev in events:
        if ev.get('logical_id')==endpoint:ev['generation']=2
        if ev.get(role)==endpoint:ev[role+'_generation']=2
        if role=='writer' and ev.get('payload_writer_id')==endpoint:ev['payload_writer_generation']=2
    for pair in row['typed_facts']['pairs']:
        if pair.get(role)==endpoint:pair[role+'_generation']=2
        for sample in pair.get('communication_witnesses',[]):
            if sample.get(role)==endpoint:sample[role+'_generation']=2
    for subject in row['oracle']['closure']['pairs']:
        if subject.get(role)==endpoint:subject[role+'_generation']=2
    rewrite(row,events)
    changed=conformance_signature(row)
    assert changed and changed != original


def test_different_obligation_not_merged(row):
    model(row); original=conformance_signature(row)
    row['final_input']['topology_program']['endpoints'][0]['qos']={'reliability':'reliable','depth':32}
    assert conformance_signature(row) != original


def test_unrelated_pair_counts_or_order_not_used_as_anomaly(row):
    model(row); original=conformance_signature(row); assert original
    # This unrelated pair is still unknown, not a forbidden residual.
    unrelated=row['typed_facts']['pairs'][1]
    unrelated.update(observed_match='unknown',matched_count=1000,delivery_closure='open')
    row['typed_facts']['pairs'].reverse()
    assert conformance_signature(row)==original


def test_graph_count_without_directed_proof_is_not_a_signature(row):
    model(row,'graph');pair=row['typed_facts']['pairs'][0]
    pair['communication_witnesses']=[]
    pair['proofs']['observed_match'].update(status='observed',value='bidirectional',
                                         detail={'proof_kind':'aggregate_count'})
    assert conformance_signature(row) is None


def test_direct_peer_api_match_requires_named_raw_peer(row):
    model(row,'graph');pair=row['typed_facts']['pairs'][0];pair['communication_witnesses']=[]
    events=read(row); refs=pair['proofs']['observed_match']['refs']
    for logical_id,peer in [('w0','gid-r0'),('r0','gid-w0')]:
        refs.append(dict(path=row['raw_path'],sha256=row['raw_sha256'],event_index=len(events)))
        events.append(dict(event='match_snapshot',event_index=len(events),run_id=row['run_id'],logical_id=logical_id,
                           generation=1,endpoint_generation='instance-'+logical_id,matched_peer_gids=[peer]))
    pair['proofs']['observed_match'].update(value='bidirectional',detail={'proof_kind':'direct_per_peer_api'})
    rewrite(row,events)
    assert conformance_signature(row)
    events[-1]['matched_peer_gids']=[]
    rewrite(row,events)
    assert conformance_signature(row) is None


def test_pass_fixture_does_not_yield_signature(row):
    assert conformance_signature(row) is None


@pytest.mark.parametrize('metadata,expected', [
    ({'oracle_verdict':'pass','rule_id':'pair.rule','typed_facts':{}},
     {'schema':'r2f.conformance-signature/v1','rule_id':'pair.rule','oracle_verdict':'pass','typed_anomaly_signature':{}}),
    ({'oracle_verdict':'fail','rule_id':'pair.rule','typed_facts':{'missing_sequences':[1]}},
     {'schema':'r2f.conformance-signature/v1','rule_id':'pair.rule','oracle_verdict':'fail','typed_anomaly_signature':{'missing_sequences':[1]}}),
    ({'oracle_verdict':'fail','rule_id':'pair.rule','typed_anomaly_signature':{'legacy':'explicit'}},
     {'schema':'r2f.conformance-signature/v1','rule_id':'pair.rule','oracle_verdict':'fail','typed_anomaly_signature':{'legacy':'explicit'}}),
    ({'oracle_verdict':'fail','rule_id':'pair.rule','typed_facts':{}},None),
])
def test_legacy_pair_signature_bytes(metadata,expected):
    assert json.dumps(conformance_signature(metadata),sort_keys=True,separators=(',',':')) == json.dumps(expected,sort_keys=True,separators=(',',':'))


def test_unrelated_writer_receive_order_is_not_globally_validated(row):
    model(row); original=conformance_signature(row); assert original
    events=read(row); unrelated=row['typed_facts']['pairs'][1]
    unrelated.update(delivery_closure='open',observed_match='unknown')
    unrelated['communication_witnesses']=[]
    for seq in [9,3]:
        send=dict(event='send',event_index=len(events),run_id=row['run_id'],logical_id='w1',generation=1,
                  endpoint_generation='instance-w1',send_sequence=seq,payload_sha256='other-'+str(seq))
        events.append(send)
        receive=dict(event='receive',event_index=len(events),run_id=row['run_id'],logical_id='r0',generation=1,
                     endpoint_generation='instance-r0',send_sequence=seq,payload_sha256='other-'+str(seq),
                     payload_writer_id='w1',payload_writer_generation=1,payload_run_id=row['run_id'],payload_endpoint_gid='gid-w1')
        events.append(receive)
        unrelated['communication_witnesses'].append(dict(writer='w1',writer_generation=1,reader='r0',reader_generation=1,
            sequence=seq,send_ref=dict(path=row['raw_path'],sha256=row['raw_sha256'],event_index=send['event_index']),
            receive_ref=dict(path=row['raw_path'],sha256=row['raw_sha256'],event_index=receive['event_index'])))
    rewrite(row,events)
    assert conformance_signature(row)==original


def test_hiding_an_actual_receive_cannot_manufacture_missing_gap(row):
    model(row); events=read(row);pair=row['typed_facts']['pairs'][0]
    closed=events.pop(); old_index=closed['event_index']
    events.append(dict(event='receive',event_index=old_index,run_id=row['run_id'],logical_id='r0',generation=1,
                       endpoint_generation='instance-r0',send_sequence=1,payload_sha256='payload-1',
                       payload_writer_id='w0',payload_writer_generation=1,payload_run_id=row['run_id'],payload_endpoint_gid='gid-w0'))
    closed['event_index']+=1;events.append(closed)
    def move_close_ref(value):
        if isinstance(value,dict):
            if 'path' in value and value.get('event_index')==old_index:value['event_index']+=1
            for v in value.values():move_close_ref(v)
        elif isinstance(value,list):
            for v in value:move_close_ref(v)
    move_close_ref(row)
    rewrite(row,events)
    assert conformance_signature(row) is None


def test_different_residual_semantics_do_not_merge(row):
    untouched=deepcopy(row)
    model(row); gap=conformance_signature(row); assert gap
    model(untouched,'graph'); graph=conformance_signature(untouched); assert graph
    assert canonical_hash(gap)!=canonical_hash(graph)


def test_raw_declared_fixture_denied_despite_modeled_eligible_flags(row):
    model(row); events=read(row);events[0]['fixture']=True
    rewrite(row,events)
    assert conformance_signature(row) is None


@pytest.mark.parametrize('field',['oracle','final_input','target'])
def test_malformed_topology_containers_fail_closed(row,field):
    model(row);row[field]=['not-an-object']
    assert conformance_signature(row) is None
