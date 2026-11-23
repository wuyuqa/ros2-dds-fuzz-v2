"""Offline contract fixtures only: no ROS init, DDS call or execution qualification."""
from copy import deepcopy
import ctypes as c
import hashlib
import time

import pytest

from r2f.runtime.topology_pair import pair_graph_facts, _delivery_window_evidence
from r2f.runtime.topology_worker import CycloneMatchedPeerObserver, _matched_peer_fields


def capture(*, direct=True):
    events, tokens, snaps, chains = [], {}, {}, {}
    began = time.monotonic_ns() - 1_000_000_000
    def emit(kind, **fields):
        row = dict(event=kind, event_index=len(events), monotonic_ns=began+len(events),
                   run_id='offline-multipeer-contract', real_ros=False,
                   execution_evidence=False, fixture=True, candidate_eligible=False)
        row.update(fields)
        events.append(row)
        return row
    emit('input_binding')
    for i, eid in enumerate(('w0', 'w1', 'r0', 'r1')):
        tokens[eid] = emit('endpoint_created', logical_id=eid, generation=1,
                           endpoint_gid=f'{i+1:032x}', endpoint_generation='generation-'+eid,
                           role='writer' if eid[0]=='w' else 'reader', pid=100+i,
                           process_start_ticks=200+i, topic='/actual/topic',
                           message_type='std_msgs/msg/String')
    for eid, token in tokens.items():
        identity = {k: token[k] for k in ('logical_id', 'generation', 'endpoint_generation',
                                        'pid', 'process_start_ticks', 'endpoint_gid')}
        cid = 'snapshot-'+eid
        request = emit('topology_snapshot_requested', command='snapshot', command_id=cid, **identity)
        opposite = [t['endpoint_gid'] for key, t in tokens.items() if key[0]!=eid[0]]
        match = emit('match_snapshot', command_id=cid, matched_count=2, **identity)
        if direct:
            match.update(peer_proof_source='dds_matched_endpoint_api', peer_gids_complete=True,
                         matched_peer_gids=opposite)
        graph = emit('graph_snapshot', command_id=cid, **identity,
                     publishers=[dict(endpoint_gid=t['endpoint_gid'],topic_type='std_msgs/msg/String') for key,t in tokens.items() if key[0]=='w'],
                     subscriptions=[dict(endpoint_gid=t['endpoint_gid'],topic_type='std_msgs/msg/String') for key,t in tokens.items() if key[0]=='r'])
        ack = emit('command_done', command_id=cid, **identity)
        chains[eid] = [request,match,graph,ack]
        snaps[eid] = dict(complete=True, command_id=cid, match_snapshot=match, graph_snapshot=graph)
    return events,tokens,snaps,chains,emit


def pair(data, *, events=True):
    raw,tokens,snaps,_,_=data
    return pair_graph_facts(tokens['w0'],tokens['r0'],snaps['w0'],snaps['r0'],
                            raw_events=raw if events else None, real_ros=False)


def test_direct_actual_protocol_supports_n_peer_positive_edge_but_not_real_qualification():
    data=capture()
    value=pair(data)
    assert value['status']=='matched'
    assert value['identity_basis']=='direct_bidirectional_matched_peer_api'
    assert not value['real_ros'] and not value['execution_evidence'] and not value['candidate_eligible']
    assert all(not row['execution_evidence'] for row in data[0])


def test_aggregate_two_and_graph_full_are_not_membership_proof():
    value=pair(capture(direct=False))
    assert value['status']=='unknown'
    assert 'aggregate_counts_do_not_identify_directed_pair' in value['reasons']


@pytest.mark.parametrize('fault', ['absent_end','incomplete','wrong_gid','alias','extra_peer',
    'count_mismatch','string_count','missing_ack','wrong_ack_command','wrong_ack_generation',
    'wrong_ack_pid','wrong_ack_start','wrong_ack_endpoint_generation','late_request',
    'graph_extra','graph_alias','graph_missing','creation_alias','missing_scope', 'retired_peer'])
def test_bad_or_incomplete_scope_cannot_make_n_peer_match(fault):
    data=capture(); raw,tokens,snaps,chains,emit=data
    row=snaps['r0']['match_snapshot']
    if fault=='absent_end':
        for key in ('peer_proof_source','peer_gids_complete','matched_peer_gids'): row.pop(key)
    elif fault=='incomplete': row['peer_gids_complete']=False
    elif fault=='wrong_gid': row['endpoint_gid']='f'*32
    elif fault=='alias': row['matched_peer_gids']=['1'*32,'1'*32]
    elif fault=='extra_peer': row['matched_peer_gids'][0]='f'*32
    elif fault=='count_mismatch': row['matched_count']=1
    elif fault=='string_count': row['matched_count']='2'
    elif fault=='missing_ack': chains['r0'][3]['event']='not_a_command_ack'
    elif fault=='wrong_ack_command': chains['r0'][3]['command_id']='other-command'
    elif fault=='wrong_ack_generation': chains['r0'][3]['generation']=2
    elif fault=='wrong_ack_pid': chains['r0'][3]['pid']=999
    elif fault=='wrong_ack_start': chains['r0'][3]['process_start_ticks']=999
    elif fault=='wrong_ack_endpoint_generation': chains['r0'][3]['endpoint_generation']='other-incarnation'
    elif fault=='late_request': chains['r0'][0]['monotonic_ns']=chains['r0'][3]['monotonic_ns']+1
    elif fault=='graph_extra': snaps['r0']['graph_snapshot']['publishers'].append(dict(endpoint_gid='f'*32))
    elif fault=='graph_alias': snaps['r0']['graph_snapshot']['publishers'].append(dict(endpoint_gid=tokens['w0']['endpoint_gid']))
    elif fault=='graph_missing': snaps['r0']['graph_snapshot']['publishers'].pop()
    elif fault=='creation_alias': tokens['w1']['endpoint_gid']=tokens['w0']['endpoint_gid']
    elif fault=='missing_scope': tokens['w1']['event']='creation_not_captured'
    elif fault=='retired_peer': emit('worker_exit',logical_id='w1',generation=1)
    assert pair(data)['status']=='unknown'


def test_native_list_without_raw_chain_is_not_proof():
    assert pair(capture(),events=False)['status']=='unknown'


def test_directional_disagreement_is_unknown_not_bidirectional():
    data=capture(); row=data[2]['r0']['match_snapshot']
    row.update(matched_peer_gids=[data[1]['w1']['endpoint_gid']],matched_count=1)
    assert pair(data)['status']=='unknown'


def test_only_complete_both_end_lists_prove_snapshot_absence_not_persistent_loss():
    data=capture()
    for eid,other in [('w0','r1'),('r0','w1')]:
        data[2][eid]['match_snapshot'].update(matched_peer_gids=[data[1][other]['endpoint_gid']],matched_count=1)
    value=pair(data)
    assert value['status']=='unmatched' and not value['candidate_eligible']


def window_capture(*, direct=True):
    data=capture(direct=direct);raw,tokens,snaps,chains,emit=data
    w,r=tokens['w0'],tokens['r0']
    opened=emit('topology_delivery_window_open', writer='w0',reader='r0',writer_generation=1,
                reader_generation=1, writer_gid=w['endpoint_gid'],reader_gid=r['endpoint_gid'],
                snapshot_start_ref=5,snapshot_refs={eid:[e['event_index'] for e in chains[eid]] for eid in ('w0','r0')})
    emit('topology_delivery_window_publish_requested',open_ref=opened['event_index'],
         action_id='publish-w0',requested_sequences=[0])
    for eid in ('w0','w1'):
        token=tokens[eid]
        identity={k:token[k] for k in ('logical_id','generation','pid','process_start_ticks','endpoint_generation')}
        digest=hashlib.sha256(eid.encode()).hexdigest()
        emit('send',**identity, writer_gid=token['endpoint_gid'],send_sequence=0,
             payload_sha256=digest,publish_returned=True,phase='publish-'+eid)
        identity={k:r[k] for k in ('logical_id','generation','pid','process_start_ticks','endpoint_generation')}
        emit('receive',**identity, reader_gid=r['endpoint_gid'],publisher_gid=None,send_sequence=0,
             payload_sha256=digest,payload_run_id=opened['run_id'],payload_writer_id=eid,
             payload_writer_generation=1,payload_endpoint_gid=token['endpoint_gid'])
    return data,opened


def test_n_writer_window_payloads_are_separate_despite_same_sequence():
    data,opened=window_capture()
    value=_delivery_window_evidence(opened,data[0])
    assert value['window_complete'], value
    assert value['received_sequences']==[0] and len(value['receive_refs'])==1
    assert value['attribution']=='endpoint_correlated_payload'
    assert all(not e['execution_evidence'] for e in data[0])


def test_aggregate_only_n_writer_delivery_cannot_backfill_early_match():
    data,opened=window_capture(direct=False)
    assert not _delivery_window_evidence(opened,data[0])['window_complete']


@pytest.mark.parametrize('fault',['wrong_writer_gid','wrong_reader_gid','wrong_payload_hash','wrong_generation',
                                 'fake_other_writer','no_sentinel','late_ack','alias_send','native_incomplete'])
def test_n_writer_window_incomplete_evidence_stays_unknown(fault):
    data,opened=window_capture();raw,tokens,snaps,chains,emit=data
    own=[e for e in raw if e['event']=='receive' and e['payload_writer_id']=='w0'][0]
    if fault=='wrong_writer_gid': own['payload_endpoint_gid']='f'*32
    elif fault=='wrong_reader_gid': own['reader_gid']='f'*32
    elif fault=='wrong_payload_hash': own['payload_sha256']='f'*64
    elif fault=='wrong_generation': own['payload_writer_generation']=2
    elif fault=='fake_other_writer': own['payload_writer_id']='w1'
    elif fault=='no_sentinel': own['event']='receive_not_observed'
    elif fault=='late_ack': chains['r0'][3]['monotonic_ns']=opened['monotonic_ns']+1
    elif fault=='alias_send': emit('send',**{k:v for k,v in next(e for e in raw if e['event']=='send').items()
                                          if k not in ('event','event_index','monotonic_ns')})
    elif fault=='native_incomplete': snaps['w0']['match_snapshot']['peer_gids_complete']=False
    assert not _delivery_window_evidence(opened,raw)['window_complete']


class FakeNative:
    """Explicit FFI unit double. Its receipts are never SUT observations."""
    def __init__(self):
        class Guid(c.Structure): _fields_=[('v',c.c_uint8*16)]
        class Endpoint(c.Structure):
            _fields_=[('key',Guid),('participant_key',Guid),('participant_instance_handle',c.c_uint64),
                      ('topic_name',c.c_char_p),('type_name',c.c_char_p),('qos',c.c_void_p)]
        self.Guid,self.Endpoint=Guid,Endpoint
        self.local='1'*32
        self.peer='2'*32
        self.retained=[]
        self.freed=0
        self.enum_calls=0
        self.race=False
        self.overflow=False
        self.missing_data=False
        self.domain=186
    def dds_lookup_participant(self,domain,array,size): array[0]=1;return 1
    def dds_get_children(self,entity,array,size):
        children={1:[2],2:[3],3:[]}[entity]
        for i,v in enumerate(children): array[i]=v
        return len(children)
    def dds_get_guid(self,entity,pointer):
        if entity!=3: return -12
        guid=c.cast(pointer,c.POINTER(self.Guid)).contents
        guid.v[:]=bytes.fromhex(self.local)
        return 0
    def dds_get_domainid(self,entity,pointer): c.cast(pointer,c.POINTER(c.c_uint32))[0]=self.domain;return 0
    def dds_get_matched_subscriptions(self,entity,array,size):
        self.enum_calls+=1
        if self.overflow:return size+1
        array[0]=99 if self.race and self.enum_calls>1 else 88
        return 1
    dds_get_matched_publications=dds_get_matched_subscriptions
    def dds_get_matched_subscription_data(self,entity,handle):
        if self.missing_data:return None
        row=self.Endpoint()
        row.key.v[:]=bytes.fromhex(self.peer)
        row.participant_key.v[:]=bytes.fromhex('3'*32)
        row.topic_name=b'rt/actual/topic';row.type_name=b'std_msgs::msg::dds_::String_'
        self.retained.append(row)
        return c.pointer(row)
    dds_get_matched_publication_data=dds_get_matched_subscription_data
    def dds_builtintopic_free_endpoint(self,pointer): self.freed+=1


def observer_fixture():
    fake=FakeNative()
    obj=CycloneMatchedPeerObserver.__new__(CycloneMatchedPeerObserver)
    obj.c=c;obj.Guid=fake.Guid;obj.Endpoint=fake.Endpoint;obj.lib=fake
    obj.provenance={'fixture':True,'execution_evidence':False}
    return obj,fake


@pytest.mark.parametrize('kind',['writer','reader'])
def test_native_api_contract_copies_guid_and_returns_allocated_endpoint(kind):
    obj,fake=observer_fixture()
    result=obj.sample(186,fake.local,kind)
    assert result['complete'] and result['matched_peer_gids']==[fake.peer]
    assert fake.freed==1
    assert result['provenance']['fixture']


@pytest.mark.parametrize('fault',['overflow','race','missing_data','wrong_local_gid','wrong_domain','peer_alias'])
def test_native_incomplete_or_aliased_api_sample_has_no_complete_peer_proof(fault):
    obj,fake=observer_fixture()
    if fault in ('overflow','race','missing_data'): setattr(fake,fault,True)
    elif fault=='wrong_local_gid': fake.local='4'*32
    elif fault=='wrong_domain': fake.domain=187
    elif fault=='peer_alias': fake.peer=fake.local
    result=obj.sample(186,'1'*32,'writer')
    assert not result['complete']
    assert 'matched_peer_gids' not in result
    assert fake.freed==len(fake.retained)


@pytest.mark.parametrize('before,after,complete',[(1,1,True),(2,1,False),(1,2,False),(True,1,False)])
def test_projection_peer_fields_require_public_counts_agree_with_native_inventory(before,after,complete):
    obj,fake=observer_fixture(); result=obj.sample(186,fake.local,'writer')
    fields=_matched_peer_fields(obj,186,fake.local,'writer',before,after,result)
    assert fields['peer_gids_complete'] is complete
    assert ('peer_proof_source' in fields) is complete


def test_worker_snapshot_payload_without_controller_envelope_binds_exact_raw_payload():
    data=capture()
    # Real RosTopologyBackend snapshots use process.events (no controller
    # event_index). Test this actual producer/consumer envelope distinction.
    for eid in ('w0','r0'):
        snap=data[2][eid]
        for field in ('match_snapshot','graph_snapshot'):
            snap[field]={k:v for k,v in snap[field].items() if k not in
                         ('event_index','fixture','execution_evidence','candidate_eligible','backend')}
    assert pair(data)['status']=='matched'
    data[2]['r0']['match_snapshot']['matched_peer_gids']=['f'*32, data[1]['w1']['endpoint_gid']]
    assert pair(data)['status']=='unknown'


def test_both_missing_raw_scope_cannot_be_rescued_by_expected_or_synthetic_flags():
    data=capture(direct=False)
    for snap in data[2].values():
        snap['match_snapshot'].update(expected_peers=['w0','w1'],inventory_complete=True,
                                      matched_count=2,synthetic_witness=True)
    assert pair(data)['status']=='unknown'


def test_graph_type_must_bind_observed_generation_not_configuration_defaults():
    data=capture()
    data[2]['r0']['graph_snapshot']['publishers'][0]['topic_type']='unrelated/msg/Type'
    assert pair(data)['status']=='unknown'


def test_negative_lists_are_snapshot_absence_not_disappearance_or_candidate():
    data=capture()
    for eid,other in [('w0','r1'),('r0','w1')]:
        data[2][eid]['match_snapshot'].update(matched_peer_gids=[data[1][other]['endpoint_gid']],matched_count=1)
    value=pair(data)
    assert value['status']=='unmatched'
    assert 'snapshot_absence_only_not_persistent_nonmatch_or_disappearance' in value['limitations']
    assert not value['candidate_eligible']


def _offline_abi_inputs(monkeypatch, tmp_path):
    """Temporary identity bytes, never a native library or installed C ABI proof."""
    from pathlib import Path
    library = Path('/opt/ros/jazzy/lib/x86_64-linux-gnu/libddsc.so.0.10.5').resolve()
    header = Path('/opt/ros/jazzy/include/CycloneDDS/dds/dds.h')
    inputs = {}
    for index, name in enumerate([str(library), *CycloneMatchedPeerObserver.VERIFIED_C_HEADERS]):
        target = tmp_path / str(index)
        target.write_bytes(('offline ABI identity fixture:' + name).encode())
        inputs[name] = target
    read_bytes, stat = Path.read_bytes, Path.stat
    header_hashes = {name: hashlib.sha256(inputs[name].read_bytes()).hexdigest()
                     for name in CycloneMatchedPeerObserver.VERIFIED_C_HEADERS}
    library_hash = hashlib.sha256(inputs[str(library)].read_bytes()).hexdigest()
    def fixture_bytes(path):
        return read_bytes(inputs.get(str(path), path))
    def fixture_stat(path, *args, **kwargs):
        return stat(inputs.get(str(path), path), *args, **kwargs)
    monkeypatch.setattr(Path, 'read_bytes', fixture_bytes)
    monkeypatch.setattr(Path, 'stat', fixture_stat)
    monkeypatch.setattr(CycloneMatchedPeerObserver, 'VERIFIED_C_HEADERS', header_hashes)
    monkeypatch.setattr(CycloneMatchedPeerObserver, 'VERIFIED_C_ABIS', ((
        header_hashes[str(header)], library_hash, (64, 0, 16, 32, 40, 48, 56)),))
    return library


def test_unverified_c_header_abi_cannot_reach_dlopen_or_native_calls(monkeypatch, tmp_path):
    from pathlib import Path
    library=_offline_abi_inputs(monkeypatch, tmp_path)
    original=Path.read_text
    import os
    stat=library.stat()
    mapping=f'0-1 r-xp 00000000 {os.major(stat.st_dev):x}:{os.minor(stat.st_dev):x} {stat.st_ino} {library}\n'
    def read_text(path,*args,**kwargs):
        if str(path)=='/proc/self/maps':
            return mapping
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'read_text',read_text)
    def no_dlopen(*args,**kwargs):pytest.fail('unverified ABI must not dlopen')
    monkeypatch.setattr(c,'CDLL',no_dlopen)
    monkeypatch.setattr(CycloneMatchedPeerObserver,'VERIFIED_C_ABIS',())
    with pytest.raises(ValueError,match='native_endpoint_c_abi_probe_unverified'):
        CycloneMatchedPeerObserver()


def test_no_loaded_library_never_falls_back_to_load_second_copy(monkeypatch):
    from pathlib import Path
    original=Path.read_text
    def read_text(path,*args,**kwargs):
        return '' if str(path)=='/proc/self/maps' else original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'read_text',read_text)
    monkeypatch.setattr(c,'CDLL',lambda *a,**k:pytest.fail('must not load library'))
    with pytest.raises(ValueError,match='loaded_cyclone_library_or_abi_not_unique'):
        CycloneMatchedPeerObserver()


def test_fastdds_has_named_unknown_not_a_cyclone_peer_proof():
    fields=_matched_peer_fields(None,186,'1'*32,'writer',2,2,
                                {'complete':False,'reason':'rmw_has_no_public_peer_observer'})
    assert not fields['peer_gids_complete']
    assert 'peer_proof_source' not in fields
    assert fields['peer_observation_limitation']=='rmw_has_no_public_peer_observer'


def _mapped_constructor(monkeypatch, tmp_path, *, extra_map='', changed_header=False, changed_library=False):
    from pathlib import Path
    import os
    from types import SimpleNamespace
    library=_offline_abi_inputs(monkeypatch, tmp_path)
    stat=library.stat()
    mapping=f'0-1 r-xp 00000000 {os.major(stat.st_dev):x}:{os.minor(stat.st_dev):x} {stat.st_ino} {library}\n'
    read_text,read_bytes=Path.read_text,Path.read_bytes
    def text(path,*args,**kwargs):
        return mapping+extra_map.replace('SAME_MAPPING',mapping) if str(path)=='/proc/self/maps' else read_text(path,*args,**kwargs)
    def binary(path):
        if changed_header and str(path).endswith('/dds/version.h'):return b'changed header'
        if changed_library and path==library:return b'changed library'
        return read_bytes(path)
    monkeypatch.setattr(Path,'read_text',text)
    monkeypatch.setattr(Path,'read_bytes',binary)
    calls=[]
    class NotNativeFunction:
        def __call__(self,*args):pytest.fail('ABI constructor must not call DDS')
    fake=SimpleNamespace(**{name:NotNativeFunction() for name in (
        'dds_lookup_participant','dds_get_children','dds_get_guid','dds_get_domainid',
        'dds_get_matched_subscriptions','dds_get_matched_publications','dds_get_matched_subscription_data',
        'dds_get_matched_publication_data','dds_builtintopic_free_endpoint')})
    def noload(path,*,mode):
        calls.append((path,mode))
        assert mode==os.RTLD_NOLOAD|os.RTLD_LOCAL
        return fake
    monkeypatch.setattr(c,'CDLL',noload)
    return calls


def test_ctypes_fixture_layout_matches_pinned_C_probe_metadata(monkeypatch, tmp_path):
    calls=_mapped_constructor(monkeypatch, tmp_path)
    obj=CycloneMatchedPeerObserver()
    assert c.sizeof(obj.Endpoint)==64
    assert {name:getattr(obj.Endpoint,name).offset for name in (
        'key','participant_key','participant_instance_handle','topic_name','type_name','qos')}=={
        'key':0,'participant_key':16,'participant_instance_handle':32,'topic_name':40,'type_name':48,'qos':56}
    assert len(calls)==1
    assert len(obj.provenance['header_fingerprints'])==22
    assert obj.provenance['c_probe_sha256']=='979a4a0a59e49f248eb9eb954e4137aea943595b01cbafb89050ac1f6ac0fab0'


@pytest.mark.parametrize('fault',['second_library','second_instance','changed_header','changed_library'])
def test_supported_layout_does_not_waive_loaded_instance_and_fingerprint_gate(monkeypatch, tmp_path, fault):
    extra_map=''
    if fault=='second_library':extra_map='2-3 r-xp 00000000 00:00 1 /elsewhere/libddsc.so.0.10.5\n'
    if fault=='second_instance':extra_map='SAME_MAPPING'
    calls=_mapped_constructor(monkeypatch, tmp_path, extra_map=extra_map,changed_header=fault=='changed_header',
                              changed_library=fault=='changed_library')
    with pytest.raises(ValueError):CycloneMatchedPeerObserver()
    assert calls==[]
