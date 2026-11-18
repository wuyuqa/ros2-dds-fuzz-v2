"""Offline producer -> consumer receipts; never ROS/DDS execution evidence."""
from copy import deepcopy
import json

import pytest

from r2f.campaign.topology_state_projection import _Projection, project_topology_state
from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import (
    _delivery_no_obligation_basis, _delivery_window_evidence, run_topology,
)
from r2f.runtime.topology_worker import OfflineTopologyBackend
from test_multipeer_match_observation import capture, window_capture


def prepared(reason='topic'):
    data=capture(); events,tokens,snaps,chains,emit=data
    if reason=='topic': tokens['r0']['topic']='/other/actual/topic'
    elif reason=='type':
        tokens['r0']['message_type']='observed/msg/Other'
        for snap in snaps.values():
            for row in snap['graph_snapshot']['subscriptions']:
                if row['endpoint_gid']==tokens['r0']['endpoint_gid']:
                    row['topic_type']='observed/msg/Other'
    # Different-topic/type endpoints cannot be directly matched; the complete
    # lists may include other actual peers, but never this target pair.
    if reason in ('topic','type','direct_none','directional'):
        snaps['w0']['match_snapshot'].update(matched_peer_gids=[tokens['r1']['endpoint_gid']],matched_count=1)
        if reason!='directional':
            snaps['r0']['match_snapshot'].update(matched_peer_gids=[tokens['w1']['endpoint_gid']],matched_count=1)
    if reason=='unknown':
        for snap in snaps.values():
            for key in ('matched_peer_gids','peer_gids_complete','peer_proof_source'):
                snap['match_snapshot'].pop(key)
    opened=emit('topology_delivery_window_open', writer='w0',reader='r0',writer_generation=1,
                reader_generation=1,writer_gid=tokens['w0']['endpoint_gid'],reader_gid=tokens['r0']['endpoint_gid'],
                snapshot_start_ref=5,snapshot_refs={eid:[e['event_index'] for e in chains[eid]] for eid in ('w0','r0')})
    return data,opened


def close_window(data,opened,*,claim=True,forge=False):
    events,tokens,snaps,chains,emit=data
    basis=_delivery_no_obligation_basis(opened,events)
    if claim and (basis is not None or forge):
        basis=basis or dict(reason='both_complete_direct_peer_lists_exclude_pair',
             writer_gid=tokens['w0']['endpoint_gid'],reader_gid=tokens['r0']['endpoint_gid'],
             writer_topic=tokens['w0']['topic'],reader_topic=tokens['r0']['topic'],
             writer_type=tokens['w0']['message_type'],reader_type=tokens['r0']['message_type'],
             snapshot_refs=opened['snapshot_refs'],obligation='not_applicable',scope='open_snapshot_only_not_persistent_nonmatch')
        emit('topology_delivery_no_obligation', open_ref=opened['event_index'],
             **{key:opened[key] for key in ('writer','reader','writer_generation','reader_generation')}, **basis)
    evidence=_delivery_window_evidence(opened,events)
    close=emit('topology_delivery_window_closed', open_ref=opened['event_index'],
               **{key:opened[key] for key in ('writer','reader','writer_generation','reader_generation')}, **evidence)
    return close


def consumer(data,opened,close):
    events,tokens,snaps,chains,emit=data
    program=model.generate_program(7,writers=2,readers=2)
    # Only endpoint names/roles are used by this private delivery unit seam.
    projection=_Projection(program,'offline-fixture.jsonl','f'*64,events,{'input_ref':{'event_index':0}})
    return projection.delivery(tokens['w0'],tokens['r0'],tokens,close)


@pytest.mark.parametrize('reason,expected',[
    ('topic','actual_endpoint_topic_mismatch'),('type','actual_endpoint_type_mismatch'),
    ('direct_none','both_complete_direct_peer_lists_exclude_pair')])
def test_observed_no_obligation_producer_and_independent_consumer_agree(reason,expected):
    data,opened=prepared(reason); close=close_window(data,opened)
    assert close['window_complete'] and close['expected_sequences']==[]
    assert close['no_obligation_reason']==expected
    value,refs,details=consumer(data,opened,close)
    assert value=='no_obligation' and details['no_obligation_reason']==expected
    assert details['expected']==[] and details['attribution']=='not_applicable'
    assert all(row['fixture'] and not row['execution_evidence'] for row in refs)


@pytest.mark.parametrize('reason',['unknown','directional','matched'])
def test_unknown_directional_and_matched_cannot_skip_obligation(reason):
    data,opened=prepared(reason)
    assert _delivery_no_obligation_basis(opened,data[0]) is None
    close=close_window(data,opened,forge=True)
    assert not close['window_complete']
    # Even a forged close boolean cannot persuade the independent consumer.
    close.update(window_complete=True,closure_status='no_obligation',no_obligation_ref=opened['event_index']+1)
    with pytest.raises(ValueError): consumer(data,opened,close)


@pytest.mark.parametrize('fault',['missing_receipt','wrong_reason','wrong_gid','wrong_generation',
    'missing_ack','stale_ack','wrong_type','wrong_ref','claim_after_send','duplicate_receipt',
    'forged_empty_expected','old_close_ref','interrupted'])
def test_no_obligation_forgery_or_incomplete_raw_is_rejected(fault):
    data,opened=prepared('topic'); events,tokens,snaps,chains,emit=data
    close=close_window(data,opened,claim=fault!='missing_receipt')
    claims=[e for e in events if e['event']=='topology_delivery_no_obligation']
    if fault=='missing_receipt':
        close.update(window_complete=True,closure_status='no_obligation')
    elif fault=='wrong_reason': claims[0]['reason']='model_expected_incompatible'
    elif fault=='wrong_gid': claims[0]['writer_gid']='f'*32
    elif fault=='wrong_generation': claims[0]['writer_generation']=2
    elif fault=='missing_ack': chains['r0'][3]['event']='ack_not_captured'
    elif fault=='stale_ack': chains['r0'][3]['command_id']='other_snapshot'
    elif fault=='wrong_type': snaps['r0']['graph_snapshot']['subscriptions'][0]['topic_type']='forged/msg/Other'
    elif fault=='wrong_ref': claims[0]['snapshot_refs']=dict(w0=[1,2,3,4],r0=[5,6,7,8])
    elif fault=='claim_after_send':
        # Raw chronology contradiction without rewriting fixture index order.
        send=events[opened['event_index']+1]
        claim=deepcopy(send)
        send.clear();send.update(event='send',logical_id='w0',event_index=claim['event_index'],
                                 monotonic_ns=claim['monotonic_ns'],run_id=opened['run_id'])
        close_index=close['event_index'];events.insert(close_index,claim)
        for index,e in enumerate(events):e['event_index']=index
    elif fault=='duplicate_receipt':
        dupe=deepcopy(claims[0]);dupe['event_index']=close['event_index'];
        events.insert(close['event_index'],dupe)
        for index,e in enumerate(events):e['event_index']=index
    elif fault=='forged_empty_expected': close['expected_sequences']=[0]
    elif fault=='old_close_ref': close['open_ref']=0
    elif fault=='interrupted':
        interrupt=dict(event='topology_delivery_window_interrupted',event_index=close['event_index'],
                       open_ref=opened['event_index'],monotonic_ns=close['monotonic_ns'],reason='actual_lifecycle_change')
        events.insert(close['event_index'],interrupt)
        for index,e in enumerate(events):e['event_index']=index
    with pytest.raises((ValueError,KeyError)):consumer(data,opened,close)


def test_real_producer_raw_to_projection_topic_mismatch_receipt(tmp_path):
    program=model.generate_program(7,writers=1,readers=1)
    for endpoint in program['endpoints']:
        if endpoint['role']=='reader':endpoint['topic']+='/actually-other-topic'
    runtime=run_topology(program,{'backend':'offline_fake','run_id':'no-obligation-fixture','domain_id':191},
                         tmp_path/'offline-runtime',backend=OfflineTopologyBackend())
    raw=[json.loads(line) for line in open(runtime['raw_path'])]
    assert raw==runtime['events']
    claims=[e for e in raw if e['event']=='topology_delivery_no_obligation']
    assert len(claims)==1 and claims[0]['reason']=='actual_endpoint_topic_mismatch'
    closes=[e for e in raw if e['event']=='topology_delivery_window_closed']
    assert closes[-1]['window_complete'] and closes[-1]['expected_sequences']==[]
    boundaries=[e for e in raw if e['event']=='topology_state_boundary']
    projected=project_topology_state(program,runtime['raw_path'],before_ref=boundaries[0]['event_index'],
                                    after_ref=boundaries[-1]['event_index'],expected_sha256=runtime['raw_sha256'])
    assert projected['state_after']['pairs'][0]['delivery_closure']=='no_obligation'
    assert not runtime['real_execution'] and not runtime['candidate_eligible'] and not projected['eligible']


def test_n_positive_producer_consumer_window_keeps_its_own_raw_samples():
    data,opened=window_capture()
    close=close_window(data,opened,claim=False)
    assert close['window_complete'] and close['closure_status']=='closed'
    value,refs,details=consumer(data,opened,close)
    assert value=='closed' and details['received']==[0]
    assert details['attribution']=='endpoint_correlated_payload'


@pytest.mark.parametrize('direct_gid',[False,True])
@pytest.mark.parametrize('missing_middle,missing_tail',[(False,False),(True,False),(False,True)])
def test_two_writers_interleaved_same_sequences_preserve_middle_gap_and_tail_unknown(direct_gid,missing_middle,missing_tail):
    data,opened=window_capture();events,tokens,snaps,chains,emit=data
    request=next(e for e in events if e['event']=='topology_delivery_window_publish_requested')
    request['requested_sequences']=[0,1,2]
    for sequence in (1,2):
        for eid in ('w1','w0'):
            token=tokens[eid];reader=tokens['r0']
            identity={k:token[k] for k in ('logical_id','generation','pid','process_start_ticks','endpoint_generation')}
            import hashlib
            digest=hashlib.sha256((eid+str(sequence)).encode()).hexdigest()
            emit('send',**identity,writer_gid=token['endpoint_gid'],send_sequence=sequence,
                 payload_sha256=digest,publish_returned=True,phase='publish-'+eid)
            if eid=='w0' and ((missing_middle and sequence==1) or (missing_tail and sequence==2)):
                continue
            identity={k:reader[k] for k in ('logical_id','generation','pid','process_start_ticks','endpoint_generation')}
            emit('receive',**identity,reader_gid=reader['endpoint_gid'],
                 publisher_gid=token['endpoint_gid'] if direct_gid else None,send_sequence=sequence,
                 payload_sha256=digest,payload_run_id=opened['run_id'],payload_writer_id=eid,
                 payload_writer_generation=1,payload_endpoint_gid=token['endpoint_gid'])
    if direct_gid:
        for row in events:
            if row['event']=='receive':row['publisher_gid']=tokens[row['payload_writer_id']]['endpoint_gid']
    close=close_window(data,opened,claim=False)
    assert close['window_complete'] is (not missing_tail)
    if missing_tail:
        assert 'finite_window_without_received_final_sentinel' in close['reasons']
        with pytest.raises(ValueError):consumer(data,opened,close)
    else:
        value,refs,details=consumer(data,opened,close)
        assert value==('closed_with_gap' if missing_middle else 'closed')
        assert close['missing_sequences']==([1] if missing_middle else [])
        assert close['sentinel_sequence']==2
        assert details['received']==([0,2] if missing_middle else [0,1,2])
        assert not details['cross_writer_order_required']


class DirectPeerContractFixture(OfflineTopologyBackend):
    """Synthetic per-peer protocol driver; NEVER an actual native API source.

    Exercises the real producer/consumer code with explicit fixture markers.
    The existing offline backend's independently held edge table supplies this
    double's list; no field is added to any historical actual run.
    """
    def __init__(self,**kwargs):
        super().__init__(publisher_gid_available=False,graph_visibility='all_live_topic',**kwargs)
    def peer_fields(self,eid):
        token=self.handles[eid]
        if token['role']=='writer':
            peers=[handle['endpoint_gid'] for other,handle in self.handles.items()
                   if handle.get('alive') and self.writer_sees.get((eid,other),False)]
        else:
            peers=[handle['endpoint_gid'] for other,handle in self.handles.items()
                   if handle.get('alive') and self.reader_sees.get((other,eid),False)]
        return dict(peer_proof_source='dds_matched_endpoint_api',peer_gids_complete=True,
                    matched_peer_gids=peers,fixture_peer_source='offline_edge_table_not_native_API')
    def _emit(self,event,**fields):
        if event=='match_snapshot':fields.update(self.peer_fields(fields['logical_id']))
        return super()._emit(event,**fields)
    def snapshot(self,handles):
        result=super().snapshot(handles)
        for eid,snap in result['snapshots'].items():snap['match_snapshot'].update(self.peer_fields(eid))
        return result


def test_four_writers_four_readers_real_producer_and_consumer_offline_contract(tmp_path):
    program=model.generate_program(7,writers=4,readers=4)
    for endpoint in program['endpoints']:
        endpoint['topic']='/observed/topic/'+('a' if int(endpoint['logical_id'][1:])%2 else 'b')
    runtime=run_topology(program,{'backend':'offline_fake','run_id':'4w4r-contract-not-DDS','domain_id':191},
                         tmp_path/'offline-4w4r',backend=DirectPeerContractFixture())
    events=runtime['events'];boundaries=[e for e in events if e['event']=='topology_state_boundary']
    closes=[e for e in events if e['event']=='topology_delivery_window_closed']
    assert len(closes)==16
    assert all(e['window_complete'] for e in closes), [(e['writer'],e['reader'],e['reasons']) for e in closes]
    assert sum(e['closure_status']=='no_obligation' for e in closes)==8
    deliveries=[e for e in closes if e['closure_status']!='no_obligation']
    assert len(deliveries)==8 and all(e['sentinel_sequence']>=0 for e in deliveries)
    assert boundaries[-1]['window_complete']
    projected=project_topology_state(program,runtime['raw_path'],before_ref=boundaries[0]['event_index'],
                                    after_ref=boundaries[-1]['event_index'],expected_sha256=runtime['raw_sha256'])
    pairs=projected['state_after']['pairs']
    assert sum(e['delivery_closure']=='no_obligation' for e in pairs)==8
    assert sum(e['delivery_closure']=='closed' for e in pairs)==8
    assert sum(e['observed_match']=='bidirectional' for e in pairs)==8
    assert sum(e['observed_match']=='none' for e in pairs)==8
    assert all(e['observed_match']!='unknown' for e in pairs)
    assert all(not e['cross_writer_order_required'] for e in closes)
    assert not runtime['real_execution'] and not projected['eligible']
    assert not runtime['candidate_eligible'] and not runtime['mature'] and not runtime['sealed']


@pytest.mark.parametrize('fault',['extra_unknown_peer','duplicate_peer','missing_scope','wrong_generation','directional'])
def test_independent_consumer_refuses_forged_direct_none_for_incomplete_peer_scope(fault):
    data,opened=prepared('direct_none');events,tokens,snaps,chains,emit=data
    close=close_window(data,opened)
    if fault=='extra_unknown_peer':snaps['w0']['match_snapshot']['matched_peer_gids']=['f'*32]
    elif fault=='duplicate_peer':
        snaps['w0']['match_snapshot'].update(matched_peer_gids=[tokens['r1']['endpoint_gid']]*2,matched_count=2)
    elif fault=='missing_scope':tokens['w1']['event']='creation_not_captured'
    elif fault=='wrong_generation':chains['r0'][1]['generation']=2
    elif fault=='directional':snaps['w0']['match_snapshot']['matched_peer_gids']=[tokens['r0']['endpoint_gid']]
    with pytest.raises(ValueError):consumer(data,opened,close)


def test_direct_none_exemption_is_not_reused_after_next_batch_observes_match(tmp_path):
    class BecomesMatched(DirectPeerContractFixture):
        def __init__(self):super().__init__(auto_match=False)
        def publish_batch(self,*args,**kwargs):
            result=super().publish_batch(*args,**kwargs)
            self.set_match_edge('w0','r0',writer_sees=True,reader_sees=True)
            return result
    program=model.generate_program(7,writers=1,readers=1)
    first=next(action for action in program['actions'] if action['op']=='publish_batch')
    later=deepcopy(first);later['action_id']='later-fresh-matched-batch'
    later['params']['start_sequence']=first['params']['start_sequence']+first['params']['count']
    later['depends_on']=[first['action_id']]
    program['actions'].insert(program['actions'].index(first)+1,later)
    assert model.validate_program(program)['status']=='valid'
    runtime=run_topology(program,{'backend':'offline_fake','run_id':'nonpersistent-none-fixture','domain_id':191},
                         tmp_path/'offline-none-to-match',backend=BecomesMatched())
    closes=[e for e in runtime['events'] if e['event']=='topology_delivery_window_closed']
    assert len(closes)==2
    assert closes[0]['closure_status']=='no_obligation' and closes[0]['expected_sequences']==[]
    assert closes[1]['window_complete'] and closes[1]['closure_status']=='closed'
    assert closes[1]['expected_sequences']==list(range(later['params']['start_sequence'],
        later['params']['start_sequence']+later['params']['count']))
    assert closes[0]['open_ref']!=closes[1]['open_ref']
    assert not runtime['real_execution'] and not runtime['candidate_eligible']


def test_no_obligation_consumer_requires_ack_start_ticks_not_just_pid():
    data,opened=prepared('topic');close=close_window(data,opened)
    data[3]['r0'][3].pop('process_start_ticks')
    with pytest.raises(ValueError,match='no_obligation_snapshot_worker_identity_incomplete'):
        consumer(data,opened,close)


@pytest.mark.parametrize('fault', [None, 'extra_unknown', 'alias', 'missing_raw',
                                  'wrong_query_gid', 'wrong_query_generation',
                                  'missing_graph', 'incomplete_api', 'api_absent'])
def test_cross_topic_direct_none_requires_complete_api_and_independent_raw_identity(fault):
    from r2f.runtime.topology_pair import pair_graph_facts
    data=capture();events,tokens,snaps,chains,emit=data
    for eid in ('w1','r0'):
        tokens[eid]['topic']='/actual/other'
    for eid, token in tokens.items():
        snap=snaps[eid]
        opposite=[t['endpoint_gid'] for t in tokens.values()
                  if t['role']!=token['role'] and t['topic']==token['topic']]
        snap['match_snapshot'].update(matched_peer_gids=opposite,matched_count=len(opposite))
        for name,role in [('publishers','writer'),('subscriptions','reader')]:
            snap['graph_snapshot'][name]=[
                dict(endpoint_gid=t['endpoint_gid'],topic_type=t['message_type'])
                for t in tokens.values() if t['role']==role and t['topic']==token['topic']]
    queried=deepcopy(tokens['r0'])
    if fault=='extra_unknown':
        snaps['w0']['match_snapshot'].update(matched_peer_gids=['f'*32],matched_count=1)
    elif fault=='alias':tokens['r1']['endpoint_gid']=tokens['r0']['endpoint_gid']
    elif fault=='missing_raw':tokens['r0']['event']='creation_not_captured'
    elif fault=='wrong_query_gid':queried['endpoint_gid']='e'*32
    elif fault=='wrong_query_generation':queried['generation']=2
    elif fault=='missing_graph':snaps['w0']['graph_snapshot']['subscriptions']=[]
    elif fault=='incomplete_api':snaps['w0']['match_snapshot']['peer_gids_complete']=False
    elif fault=='api_absent':
        for snap in snaps.values():
            for key in ('peer_proof_source','peer_gids_complete','matched_peer_gids'):
                snap['match_snapshot'].pop(key)
    boundary=emit('offline_boundary')
    projection=_Projection(model.generate_program(7,writers=2,readers=2),
                           'offline-fixture.jsonl','f'*64,events,{'input_ref':{'event_index':0}})
    value=pair_graph_facts(tokens['w0'],queried,snaps['w0'],snaps['r0'],raw_events=events)
    if fault is None:
        assert value['status']=='unmatched'
        assert value['identity_basis']=='direct_bidirectional_matched_peer_api'
        assert projection.match(tokens['w0'],queried,chains['w0'],chains['r0'],boundary)[0]=='none'
    else:
        # Legacy absent-API compatibility is not a match/nonmatch proof.
        assert value['status'] not in ('matched','unmatched')
        with pytest.raises(ValueError):
            projection.match(tokens['w0'],queried,chains['w0'],chains['r0'],boundary)
