"""Synthetic identity transitions must reach runtime delivery consumers."""
from copy import deepcopy
import pytest
from test_endpoint_incarnation import trace, OLD, NEW
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.topology_pair import _delivery_snapshot_chain, payload_delivery_witness
from r2f.runtime.topology_worker import TopologyBackendError


def snapshot_trace(*, receipt=True):
    rows=trace()
    if not receipt: rows=rows[:1]
    original=rows[0]
    handle={**original, 'endpoint_gid':NEW}
    for event, extra in [
        ('topology_snapshot_requested', dict(command='snapshot', endpoint_gid=NEW)),
        ('match_snapshot', dict(endpoint_gid=NEW, matched_count=1)),
        ('graph_snapshot', dict(publishers=[],subscriptions=[dict(endpoint_gid=NEW)])),
        ('command_done', {})]:
        rows.append(dict(original, event=event, command_id=8, **extra))
    for i, row in enumerate(rows):
        row.update(event_index=i, monotonic_ns=100+i)
    return rows,handle


def test_snapshot_accepts_verified_current_incarnation():
    rows, handle=snapshot_trace()
    chain=_delivery_snapshot_chain(handle,8,rows,after_index=8)
    assert chain[1]['endpoint_gid']==NEW


def test_changed_handle_and_snapshot_without_transition_rejected():
    rows, handle=snapshot_trace(receipt=False)
    with pytest.raises(TopologyBackendError):
        _delivery_snapshot_chain(handle,8,rows,after_index=1)


def test_old_handle_not_silently_rebound():
    rows, handle=snapshot_trace()
    handle['endpoint_gid']=OLD
    with pytest.raises(TopologyBackendError):
        _delivery_snapshot_chain(handle,8,rows,after_index=8)


def payload_trace():
    rows=trace()
    writer=dict(rows[0],logical_id='w0',role='writer',pid=200,pgid=200,
                endpoint_generation='worker-w',node_name='writer_node',endpoint_gid='04'*16)
    rows.insert(1,writer)
    send=dict(writer,event='send',writer_gid=writer['endpoint_gid'],send_sequence=1,
              payload_sha256='a'*64, publish_returned=True)
    receive=dict(rows[0],event='receive',reader_gid=NEW,payload_run_id='run',
                 payload_writer_id='w0',payload_writer_generation=1,
                 payload_endpoint_gid=writer['endpoint_gid'],send_sequence=1,payload_sha256='a'*64)
    rows.extend([send,receive])
    for i, row in enumerate(rows):
        row.update(event_index=i,monotonic_ns=100+i)
    # Keep API interval evidence consistent after inserting writer creation.
    rows[6].update(update_started_ns=102,update_finished_ns=106)
    for r in rows:
        if r['event']=='content_filter_api_call': r['api_monotonic_ns']=r['monotonic_ns']
    pair=dict(writer='w0',reader='r0',writer_generation=1,reader_generation=1,
              writer_gid=writer['endpoint_gid'],reader_gid=NEW)
    return rows, pair, receive


def test_payload_witness_binds_current_reader_not_fake_second_creation():
    rows,pair,receive=payload_trace()
    witness, reason=payload_delivery_witness(pair,receive,rows)
    assert witness is not None,reason
    assert len([e for e in rows if e['event']=='endpoint_created' and e['logical_id']=='r0'])==1
    assert endpoint_identity_at(rows, rows[0])['incarnation_ordinal']==1


def test_old_receive_cannot_inherit_new_incarnation():
    rows,pair,receive=payload_trace()
    receive['reader_gid']=OLD
    pair['reader_gid']=OLD
    witness,reason=payload_delivery_witness(pair,receive,rows)
    assert witness is None


def test_transition_ack_missing_does_not_qualify_payload():
    rows,pair,receive=payload_trace()
    rows[:]=[e for e in rows if not(e['event']=='command_done' and e.get('command_id')==7)]
    assert payload_delivery_witness(pair,receive,rows)[0] is None


@pytest.mark.parametrize('direct', [True,False])
def test_supporting_fact_cannot_borrow_old_reader_incarnation(direct):
    from r2f.runtime.topology_pair import pair_delivery_fact
    rows,pair,receive=payload_trace()
    receive['reader_gid']=OLD
    receive['publisher_gid']=pair['writer_gid'] if direct else None
    pair['status']='matched'
    fact=pair_delivery_fact(pair,rows,active_writers_on_topic=1)
    assert fact['received_sequences']==[]


def test_execution_metadata_keeps_origin_and_exposes_current_incarnation():
    from r2f.runtime.topology_pair import _execution_workers
    rows=trace()
    metadata=_execution_workers(rows,'/synthetic/raw.jsonl')[0]
    assert metadata['endpoint_gid']==OLD
    assert metadata['current_endpoint_gid']==NEW
    assert metadata['incarnation_ordinal']==1
    assert metadata['identity_transition_raw_refs']==[dict(raw_path='/synthetic/raw.jsonl',event_index=6)]


def test_new_snapshot_clock_must_follow_identity_ack():
    rows,handle=snapshot_trace()
    for i,row in enumerate(rows[8:]):row['monotonic_ns']=102+i
    with pytest.raises(TopologyBackendError):_delivery_snapshot_chain(handle,8,rows,after_index=8)


def test_new_receive_clock_must_follow_identity_ack():
    rows,pair,receive=payload_trace()
    receive['monotonic_ns']=104
    assert payload_delivery_witness(pair,receive,rows)[0] is None


def test_reader2_alias_is_supported_through_raw_and_delivery():
    rows,pair,receive=payload_trace()
    for row in rows:
        if row.get('logical_id')=='r0':row['role']='reader2'
    witness,reason=payload_delivery_witness(pair,receive,rows)
    assert witness is not None,reason
    assert endpoint_identity_at(rows,rows[0])['role']=='reader2'


def window_trace():
    rows,pair,receive=payload_trace()
    send,receive=rows[-2:]
    rows=rows[:-2]
    tokens={e['logical_id']:e for e in rows if e['event']=='endpoint_created'}
    current={eid:endpoint_identity_at(rows,t) for eid,t in tokens.items()}
    graph={name:[dict(endpoint_gid=t['endpoint_gid'],node_name=t['node_name'],topic_type=t['message_type'])
                 for t in current.values() if t['role']==role]
           for name,role in [('publishers','writer'),('subscriptions','reader')]}
    chains={};start=len(rows)
    for eid in ['w0','r0']:
        t=current[eid];refs=[]
        for kind,extra in [
          ('topology_snapshot_requested',dict(command='snapshot',endpoint_gid=t['endpoint_gid'])),
          ('match_snapshot',dict(endpoint_gid=t['endpoint_gid'],matched_count=1)),
          ('graph_snapshot',deepcopy(graph)),('command_done',{})]:
            refs.append(len(rows));rows.append(dict(t,event=kind,command_id=8,**extra))
        chains[eid]=refs
    opened=dict(event='topology_delivery_window_open',run_id='run',**pair,
                snapshot_start_ref=start,snapshot_refs=chains,topic='/t')
    rows.append(opened)
    rows.append(dict(event='topology_delivery_window_publish_requested',open_ref=len(rows)-1,
                     action_id='publish',requested_sequences=[1]))
    send['phase']='publish';rows.extend([send,receive])
    for i,r in enumerate(rows):r.update(event_index=i,monotonic_ns=100+i)
    return rows,opened


def test_new_incarnation_runtime_window_closes_with_real_contract_witness():
    from r2f.runtime.topology_pair import _delivery_window_evidence
    rows,opened=window_trace()
    evidence=_delivery_window_evidence(opened,rows)
    assert evidence['window_complete'],evidence
    assert evidence['received_sequences']==[1]


def test_no_obligation_can_bind_new_identity_without_fake_creation():
    from r2f.runtime.topology_pair import _delivery_no_obligation_basis
    rows,opened=window_trace()
    writer=next(e for e in rows if e['event']=='endpoint_created' and e['logical_id']=='w0')
    writer['message_type']='other/msg/Type'
    for event in rows:
        for member in event.get('publishers',[]):member['topic_type']='other/msg/Type'
    result=_delivery_no_obligation_basis(opened,rows)
    assert result is not None
    assert result['reader_gid']==NEW
    assert result['reason']=='actual_endpoint_type_mismatch'


def test_window_does_not_accept_old_callback_after_switch():
    from r2f.runtime.topology_pair import _delivery_window_evidence
    rows,opened=window_trace()
    rows[-1]['reader_gid']=OLD
    evidence=_delivery_window_evidence(opened,rows)
    assert not evidence['window_complete']
    assert 'receive_identity_or_window_invalid' in evidence['reasons']


def test_window_cannot_span_another_acknowledged_incarnation():
    from r2f.runtime.topology_pair import _delivery_window_evidence
    from test_endpoint_incarnation import NEXT
    rows,opened=window_trace()
    second=deepcopy(trace()[1:])
    for row in second:
        row['command_id']=9
        for key in ('endpoint_gid','reader_gid','before_gid','after_gid'):
            if key in row:row[key]={OLD:NEW,NEW:NEXT}[row[key]]
        for member in row.get('subscriptions',[]):
            member['endpoint_gid']={OLD:NEW,NEW:NEXT}[member['endpoint_gid']]
        if row['event']=='endpoint_identity_transition':row.update(incarnation_before=1,incarnation_after=2)
        if row['event']=='command_done':row['incarnation_ordinal']=2
    first=len(rows)
    rows.extend(second)
    for i,row in enumerate(rows[first:],start=first):
        row.update(event_index=i,monotonic_ns=100+i)
        if row['event']=='content_filter_api_call':row['api_monotonic_ns']=row['monotonic_ns']
        if row['event']=='content_filter_dynamic_update':
            row.update(update_started_ns=100+first,update_finished_ns=100+i)
    assert endpoint_identity_at(rows,rows[0])['endpoint_gid']==NEXT
    evidence=_delivery_window_evidence(opened,rows)
    assert not evidence['window_complete']
    assert 'endpoint_incarnation_changed_inside_window' in evidence['reasons']
