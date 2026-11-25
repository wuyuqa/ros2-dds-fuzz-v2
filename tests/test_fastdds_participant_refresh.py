"""Normal-control input -> persisted receipt, never synthetic DDS qualification."""
import copy
import json
import pytest
from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_input import make_topology_plan
from r2f.runtime.topology_pair import run_topology
from test_topology_pair import CFG, action, endpoint, program


def refresh_program(data='issue14-refresh-g1'):
    return program([endpoint('w0','writer')], [action('create','create','w0',1),
        action('refresh','update_participant_userdata','w0',1,data=data)])


def test_refresh_action_is_deterministic_input_and_fixture_cannot_claim_native_update(tmp_path):
    p=refresh_program()
    assert model.validate_program(p)['status']=='valid'
    plan=make_topology_plan(p,[{'operator_id':'action.insert'}],rng_seed=0)
    changed=make_topology_plan(refresh_program('another-refresh'),[{'operator_id':'action.insert'}],rng_seed=0)
    assert plan.final_config!=changed.final_config and plan.plan_hash()!=changed.plan_hash()
    result=run_topology(p,CFG,tmp_path)
    rows=[json.loads(line) for line in (tmp_path/'raw.jsonl').read_text().splitlines()]
    row=next(row for row in rows if row['event']=='participant_refresh_observation')
    assert row['refresh_receipt']['status']=='unsupported'
    assert row['refresh_receipt']['reason']=='offline_fixture_no_native_participant_refresh'
    assert row['refresh_receipt']['candidate_eligible'] is False
    assert row['refresh_receipt']['execution_evidence'] is False
    assert result['real_execution'] is False
    assert any(r.get('action',{}).get('op')=='update_participant_userdata' for r in result['precondition_receipts'])


@pytest.mark.parametrize('data',['','x'*257,'bad\x00data','\ud800',None,7])
def test_refresh_input_rejects_ambiguous_or_unbounded_userdata(data):
    assert model.validate_program(refresh_program(data))['status']!='valid'


def test_refresh_keeps_writer_generation_guard_and_never_applies_after_destroy(tmp_path):
    p=refresh_program();p['actions'].insert(1,action('destroy','destroy','w0',1))
    result=run_topology(p,CFG,tmp_path)
    rows=[json.loads(line) for line in (tmp_path/'raw.jsonl').read_text().splitlines()]
    assert not any(e['event']=='participant_refresh_observation' for e in rows)
    assert any(r.get('action',{}).get('op')=='update_participant_userdata' and not r['satisfied']
               for r in result['precondition_receipts'])


def test_untrusted_refresh_control_remains_excluded_even_if_raw_audit_fails():
    from r2f.campaign.topology_adapter import facts_from_events
    facts=facts_from_events([{'event':'participant_refresh_observation','refresh_receipt':{'complete':False}}],{}, {})
    assert facts['control_excluded'] is True
    assert facts['candidate_eligible'] is False


def test_native_refresh_decodes_observed_bytes_and_rejects_noop_or_wrong_entity():
    import ctypes as c
    from r2f.instrumentation.fastdds_pdp_trace.refresh import RefreshResult, decode_result
    value=RefreshResult();value.schema_version=1;value.struct_size=c.sizeof(value)
    value.status=value.dds_return=0;value.complete=1
    gid='01020304050607080910111200000103'
    value.endpoint_guid[:]=bytes.fromhex(gid)
    value.participant_guid[:]=bytes.fromhex(gid[:24]+'000001c1')
    value.before_size=3;value.before[:3]=b'old';value.after_size=3;value.after[:3]=b'new'
    assert c.sizeof(value)==576
    receipt=decode_result(value,b'new',gid)
    assert receipt['before_hex']=='6f6c64' and receipt['after_hex']=='6e6577'
    value.before[:3]=b'new'
    with pytest.raises(ValueError):decode_result(value,b'new',gid)
    value.before[:3]=b'old';value.participant_guid[0]=0
    with pytest.raises(ValueError):decode_result(value,b'new',gid)


def test_public_worker_keeps_unsupported_refresh_request_and_command_identity(monkeypatch):
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture
    cfg={**CFG,**endpoint('w0','writer'),'generation':1}
    cfg['qos']=worker.endpoint_expressibility(endpoint('w0','writer'),{})['public_qos']
    commands=[{'command':'refresh_participant','command_id':7,'data':'normal-marker'}, {'command':'stop','command_id':8}]
    with ros_fixture(monkeypatch,commands) as (_,_,stream):
        monkeypatch.setattr(sys,'argv',['worker',json.dumps(cfg),'writer'])
        assert worker.worker_main()==0,stream.getvalue()
    rows=[json.loads(line) for line in stream.getvalue().splitlines()]
    result=next(e for e in rows if e['event']=='participant_refresh_result')
    assert result['command_id']==7 and result['refresh_receipt']['requested_data']=='normal-marker'
    assert result['refresh_receipt']['complete'] is False
    assert result['refresh_receipt']['execution_evidence'] is False
    assert rows.index(next(e for e in rows if e.get('command_id')==7 and e['event']=='command_done'))>rows.index(result)


def test_refresh_seed_is_opt_in_and_keeps_the_existing_default_corpus_identity():
    from r2f.modules.s1 import fuzz_mutators_v2 as v2, input_model
    assert input_model.content_hash(v2.corpus_v2())=='7c420c8cc5425828bb3e15b3a7655904d2ba069fb6aa0059ff99db787f883ecd'
    source=v2.corpus_v2(include_participant_refresh=True)
    seed=next(e for e in source['entries'] if e['seed_id']==v2.PARTICIPANT_REFRESH_EPISODE_SEED)
    actions=seed['document']['actions']
    refresh=[a for a in actions if a['op']=='update_participant_userdata']
    assert [(a['endpoint_id'],a['generation'],a['params']['data']) for a in refresh]==[
        ('w0',1,'issue14-normal-participant-userdata-g1'),('w0',2,'issue14-normal-participant-userdata-g2')]
    for item in refresh:
        pos=actions.index(item)
        assert actions[pos-1]['op']=='barrier' and actions[pos-1]['params']['condition']=='matched'
        assert actions[pos+1]['op']=='publish_batch' and actions[pos+1]['depends_on']==[item['action_id']]
    assert seed['execution_evidence'] is seed['candidate_eligible'] is False


from test_fastdds_peer_probe import build_manifest


def authored_refresh_chain(build_manifest,tmp_path):
    """External artifact/ABI double; no library load or real DDS qualification."""
    import ctypes as c
    import os
    from pathlib import Path
    from pdp_trace_fixtures import authored_pdp
    from r2f.instrumentation.fastdds_pdp_trace import activation as a
    from r2f.instrumentation.fastdds_pdp_trace.refresh import RefreshResult,decode_result,SCHEMA
    from test_fastdds_peer_activation import reference
    path,peer,trace=authored_pdp(build_manifest,tmp_path)
    binary=Path(trace['install'])/'libr2f_fastdds_participant_refresh.so';binary.write_bytes(b'authored-not-loadable-control')
    control=dict(schema='r2f.fastdds-participant-refresh-build/v1',source_sha256=a.digest(Path(a.__file__).with_name('refresh.cpp')),
                 binary=dict(path=str(binary),sha256=a.digest(binary)),abi_size=576,compilation='passed')
    trace['refresh_control']=control
    trace_path=Path(peer['pdp_trace']['manifest_path']);trace_path.write_text(json.dumps(trace))
    peer['pdp_trace']['manifest_sha256']=a.digest(trace_path);path.write_text(json.dumps(peer));ref=reference(path)
    gid='01020304050607080910111200000103';userdata='new'
    native=RefreshResult();native.schema_version=1;native.struct_size=c.sizeof(native);native.complete=1
    native.endpoint_guid[:]=bytes.fromhex(gid);native.participant_guid[:]=bytes.fromhex(gid[:24]+'000001c1')
    native.before_size=native.after_size=3;native.before[:3]=b'old';native.after[:3]=b'new'
    owner=dict(logical_id='w0',generation=1,endpoint_generation='g1',endpoint_gid=gid,pid=100,pgid=100,process_start_ticks=10)
    provenance=dict(library_provenance=dict(vendor_binary=peer['binaries']['vendor'],provider_binary=peer['binaries']['provider']))
    st=binary.stat()
    value=dict(schema=SCHEMA,**decode_result(native,b'new',gid),requested_data=userdata,candidate_eligible=False,
               execution_evidence=True,started_monotonic_ns=35,call_started_monotonic_ns=45,call_returned_monotonic_ns=75,
               ended_monotonic_ns=95,manifest_reference=ref,control_manifest=control,native_result_hex=bytes(native).hex(),
               return_status=0,provenance_before=provenance,provenance_after=provenance,
               control_mapping=dict(path=str(binary.resolve()),sha256=a.digest(binary),inode=st.st_ino,
                                    device=f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'))
    command=7;observed=dict(value,command_id=command,worker_identity=owner)
    clock=dict(boot_id='fixture-boot',device=5,inode=44)
    def row(name,index,mono,**kw):return dict(event=name,event_index=index,monotonic_ns=mono,monotonic_clock_identity=clock,**kw)
    begin=row('topology_action_begin',1,20);done=row('topology_action_done',7,130)
    events=[row('endpoint_created',0,10,role='writer',**owner),begin,
            row('topology_participant_refresh_requested',2,30,**owner,command_id=command,command='refresh_participant',requested_data=userdata),
            row('topology_participant_refresh_write_result',3,40,**owner,command_id=command,command='refresh_participant',status='flushed',bytes_expected=20,bytes_written=20,error_type=None),
            row('participant_refresh_result',4,100,**owner,command_id=command,refresh_receipt=value),
            row('command_done',5,110,**owner,command_id=command),
            row('participant_refresh_observation',6,120,logical_id='w0',generation=1,action_id='refresh',refresh_receipt=observed),done]
    act=action('refresh','update_participant_userdata','w0',1,data=userdata)
    before=dict(generation=1,alive=True,next_sequence=0)
    receipt=dict(before=before,after=dict(before,participant_refresh=observed))
    return events,act,receipt,begin,done,ref


def test_control_audit_binds_native_bytes_to_program_command_and_mapping(build_manifest,tmp_path):
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_action,validate_declarations
    args=authored_refresh_chain(build_manifest,tmp_path)
    assert validate_action(*args)['after_hex']=='6e6577'
    validate_declarations(args[0],dict(actions=[args[1]]),args[-1])
    with pytest.raises(ValueError):validate_declarations(args[0],dict(actions=[]),args[-1])
    with pytest.raises(ValueError):validate_declarations(args[0],dict(actions=[args[1]]),None)


@pytest.mark.parametrize('corruption',['missing_ack','data','worker','mapping','native_bytes','clock','write','unselected'])
def test_control_audit_rejects_broken_provenance_and_never_counts_a_setter_return(build_manifest,tmp_path,corruption):
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_action
    args=list(authored_refresh_chain(build_manifest,tmp_path));events,act,receipt,begin,done,ref=args
    value=receipt['after']['participant_refresh']
    if corruption=='missing_ack':events[5]['event']='unrelated'
    elif corruption=='data':act['params']['data']='different'
    elif corruption=='worker':value['worker_identity']['generation']=2
    elif corruption=='mapping':value['control_mapping']['inode']+=1
    elif corruption=='native_bytes':value['native_result_hex']='00'*576
    elif corruption=='clock':events[5]['monotonic_clock_identity']={}
    elif corruption=='write':events[3]['status']='failed'
    elif corruption=='unselected':args[-1]=None
    with pytest.raises(ValueError):validate_action(*args)


def test_refresh_audit_binds_the_canonical_copied_peer_manifest(build_manifest,tmp_path):
    from pathlib import Path
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_action
    args=list(authored_refresh_chain(build_manifest,tmp_path));events,act,receipt,begin,done,ref=args
    copied=tmp_path/'native-peer-build-manifest.json';copied.write_bytes(Path(ref['manifest_path']).read_bytes())
    expected=dict(ref,manifest_path=str(copied))
    receipt['after']['participant_refresh']['manifest_reference']=expected
    events[4]['refresh_receipt']['manifest_reference']=expected
    assert validate_action(*args,raw_path=tmp_path/'raw.jsonl')['complete'] is True


def test_worker_output_is_accepted_by_real_backend_refresh_command(monkeypatch,tmp_path):
    import sys
    from types import SimpleNamespace
    from r2f.runtime import topology_worker as worker
    from r2f.instrumentation.fastdds_pdp_trace import refresh
    from test_topology_participant_control_worker import ros_fixture
    cfg={**CFG,**endpoint('w0','writer'),'generation':1}
    cfg['qos']=worker.endpoint_expressibility(endpoint('w0','writer'),{})['public_qos']
    commands=[dict(command='refresh_participant',command_id=7,data='marker'),dict(command='stop',command_id=8)]
    with ros_fixture(monkeypatch,commands) as (_,_,stream):
        monkeypatch.setattr(refresh,'refresh_participant',lambda observer,domain,gid,data:dict(status='applied',complete=True,requested_data=data))
        monkeypatch.setattr(sys,'argv',['worker',json.dumps(cfg),'writer'])
        assert worker.worker_main()==0,stream.getvalue()
    rows=[json.loads(line) for line in stream.getvalue().splitlines()]
    created=next(e for e in rows if e['event']=='endpoint_created')
    class CapturedPipe:
        events=rows
        def command(self,name,**fields):
            assert name=='refresh_participant' and fields==dict(data='marker');return 7
        def has(self,name,**fields):return any(e['event']==name and all(e.get(k)==v for k,v in fields.items()) for e in rows)
        def wait(self,predicate,*args):assert predicate()
    handle={k:created[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid','pid','pgid','process_start_ticks')}
    handle['process']=CapturedPipe()
    result=worker.RosTopologyBackend(cfg,tmp_path).refresh_participant(handle,'marker')
    assert result['status']=='applied' and result['command_id']==7


@pytest.mark.parametrize('damage',['ack_time','observation_time','done_time','reader','retired'])
def test_refresh_audit_rejects_impossible_chronology_or_worker(build_manifest,tmp_path,damage):
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_action
    args=list(authored_refresh_chain(build_manifest,tmp_path));events,act,receipt,begin,done,ref=args
    if damage=='ack_time':events[5]['monotonic_ns']=41
    elif damage=='observation_time':events[6]['monotonic_ns']=99
    elif damage=='done_time':done['monotonic_ns']=110
    elif damage=='reader':events[0]['role']='reader'
    elif damage=='retired':receipt['before']['alive']=False
    with pytest.raises(ValueError):validate_action(*args)


def test_replay_control_admission_reuses_complete_raw_command_validation(build_manifest,tmp_path):
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_replay
    args=list(authored_refresh_chain(build_manifest,tmp_path));events,act,receipt,begin,done,ref=args
    receipt.update(action=act,step_id='action:refresh',disposition='effect_observed',satisfied=True,
                   requires=[dict(id='native_participant_refresh_receipt',status='satisfied')])
    begin.update(action=act,action_id=act['action_id'],action_index=0,before=receipt['before'])
    done.update(action=act,action_id=act['action_id'],action_index=0,receipt=copy.deepcopy(receipt))
    events.insert(-1,dict(event='state_step_after',event_index=7,monotonic_ns=125,monotonic_clock_identity=begin['monotonic_clock_identity'],**receipt));done['event_index']=8
    p=dict(endpoints=[dict(logical_id='w0',role='writer')],actions=[act])
    assert validate_replay(events,p,ref) is None
    events[5]['monotonic_ns']=42
    with pytest.raises(ValueError):validate_replay(events,p,ref)


def test_replay_keeps_a_real_raw_guard_rejection_without_requiring_a_successful_control(tmp_path):
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_replay
    p=refresh_program();p['endpoints'][0]['role']='reader'
    result=run_topology(p,CFG,tmp_path)
    events=[json.loads(line) for line in (tmp_path/'raw.jsonl').read_text().splitlines()]
    assert not any(e['event']=='participant_refresh_observation' for e in events)
    validate_replay(events,p,None)


@pytest.mark.parametrize('damage',['before_missing','guard_clock','guard_controller'])
def test_replay_rejects_corrupt_guard_transcript(tmp_path,damage):
    from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_replay
    p=refresh_program();p['endpoints'][0]['role']='reader'
    run_topology(p,CFG,tmp_path)
    events=[json.loads(line) for line in (tmp_path/'raw.jsonl').read_text().splitlines()]
    validate_replay(events,p,None)
    if damage=='before_missing':
        events[:]=[e for e in events if not(e['event']=='state_step_before' and e.get('step_id')=='action:refresh')]
        for i,e in enumerate(events):e['event_index']=i
    else:
        guard=next(e for e in events if e['event']=='topology_guard_reject' and e.get('step_id')=='action:refresh')
        if damage=='guard_clock':guard['monotonic_ns']=1
        else:guard['pid']+=1
    with pytest.raises(ValueError):validate_replay(events,p,None)
