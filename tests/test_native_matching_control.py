"""Public action -> raw -> facts tests; synthetic control never proves DDS."""
from copy import deepcopy
import time
import pytest
from r2f.modules.s1 import topology_model as model
from r2f.runtime.native_matching import validate_control, audit_control, SCHEMA
from r2f.runtime.topology_route import run_topology_route
from test_topology_no_obligation_facts import ObservedPeers, config_for, consume, rewrite


def control():
    q=model.generate_program(7,writers=1,readers=1)['endpoints'][0]['qos']
    return {'support':{'path':'/offline/native-support.json','sha256':'a'*64},
            'topic':'r2f_native_matching_test','same_participant':False,'timeout_sec':1,
            'writer_type':'std_msgs/msg/String','reader_type':'std_msgs/msg/String',
            'writer_qos':deepcopy(q),'reader_qos':deepcopy(q)}


def native_receipt(request, pid):
    q={'history':'keep_last','depth':16,'reliability':'reliable','durability':'volatile',
       'deadline_sec':'infinite','lease_duration_sec':'infinite','liveliness':'automatic',
       'partitions':[],'ownership':'shared'}
    value={'schema':SCHEMA,'request':deepcopy(request),'pid':pid,'status':'observed',
            'workload_path':'native_dds_public_api','ros_qos_adoption':False,'vendor':'cyclonedds','version':'0.10.5',
            'started_monotonic_ns':time.monotonic_ns(),'finished_monotonic_ns':time.monotonic_ns(),
            'support':deepcopy(request['support']), 'cleanup_confirmed':True,
            'participant_inventory_before':['f'*32], 'participant_inventory_after':['f'*32],
            'cleanup':[{'kind':'participant','return_code':0}],
            'entities':[{'role':role,'topic':'r2f_native_matching_test','type_name':'r2f_lookup::String',
                         'qos':deepcopy(q),'endpoint_gid':gid*32,'participant_gid':pp*32,'generation':gid}
                        for role,gid,pp in [('writer','1','3'),('reader','2','4')]],
            'samples':[{'peer_gids':[['2'*32],['1'*32]],'monotonic_ns':time.monotonic_ns()}],
            'delivery':{'write_return_code':0,'sent':'sample','received':'sample',
                        'sample_identity_writer_guid':'1'*32}}
    for e in value['entities']:
        e.update(domain_id=191,observed_monotonic_ns=time.monotonic_ns())
    value['samples'][0]['monotonic_ns']=time.monotonic_ns()
    value['fresh_entities']=deepcopy(value['entities'])
    for e in value['fresh_entities']:
        e['observed_monotonic_ns']=time.monotonic_ns()
    value['finished_monotonic_ns']=time.monotonic_ns()
    return value


class NativeFixture(ObservedPeers):
    make_receipt=staticmethod(native_receipt)

    def native_matching_control(self, handle, value):
        self.command_serial+=1;cid=self.command_serial
        identity={k:handle[k] for k in ('logical_id','generation','endpoint_gid','endpoint_generation','pid','pgid','process_start_ticks')}
        self._emit('native_matching_requested',command_id=cid,control=value,**identity)
        receipt=self.make_receipt(value,handle['pid'])
        self._emit('native_matching_result',command_id=cid,control_receipt=receipt,rmw_identifier='rmw_cyclonedds_cpp',**identity)
        self._emit('command_done',command_id=cid,**identity)
        return receipt


def produce(tmp_path, *, request=None, backend=None, empty_snapshot=False):
    p=model.generate_program(7,writers=1,readers=1)
    p['actions'].insert(-1, {'action_id':'native','op':'native_matching_control','endpoint_id':'w0','generation':1,
                            'params':{'control':request or control()},'depends_on':[]})
    if empty_snapshot:
        p['actions'].insert(0, {'action_id':'empty','op':'snapshot','endpoint_id':None,'generation':None,
                                'params':{'label':'before_create'},'depends_on':[]})
    assert model.validate_program(p)['status']=='valid'
    cfg=config_for(p);cfg['rmw']='rmw_cyclonedds_cpp'
    runtime=run_topology_route(cfg,tmp_path/'runtime',backend=backend or NativeFixture())
    assert runtime['status']=='executed',runtime
    return runtime,cfg


def test_native_control_passes_through_existing_runner_raw_facts_and_oracle(tmp_path):
    runtime,cfg=produce(tmp_path)
    facts,oracle=consume(runtime,cfg)
    assert facts['native_matching_controls'][0]['status']=='pass',facts
    assert facts['native_matching_controls'][0]['ros_qos_adoption'] is False
    assert oracle['verdict']=='pass'
    assert facts['candidate_eligible'] is False


def test_empty_snapshot_before_native_control_needs_no_nonexistent_worker_ack(tmp_path):
    runtime,cfg=produce(tmp_path,empty_snapshot=True)
    facts,oracle=consume(runtime,cfg)
    assert facts['diagnostic_valid'],facts
    assert facts['native_matching_controls'][0]['status']=='pass',facts
    assert oracle['verdict']=='pass',oracle
    assert facts['fixture'] is True and facts['execution_evidence'] is False
    assert facts['candidate_eligible'] is False


@pytest.mark.parametrize('damage',['missing_observation','pairs','delivery','command','controller','complete_int',
                                   'before_alive','before_action','before_requires','before_after',
                                   'before_satisfied','after_complete_only','after_requires'])
def test_empty_snapshot_requires_exact_controller_capture_and_terminal(tmp_path,damage):
    runtime,cfg=produce(tmp_path,empty_snapshot=True)
    def corrupt(events):
        captured=next(e for e in events if e['event']=='topology_pair_observation'
                      and e.get('step_id')=='snapshot:empty')
        if damage=='missing_observation':events.remove(captured)
        elif damage in ('pairs','delivery'):captured[damage]=[{'unattributed':'endpoint'}]
        elif damage=='command':captured.pop('snapshot_command_id')
        elif damage=='controller':captured['pid']+=1
        elif damage.startswith('before_'):
            before=next(e for e in events if e['event']=='state_step_before' and e.get('step_id')=='action:empty')
            if damage=='before_alive':before['before']['alive']=['w0']
            elif damage=='before_action':before['action']['op']='barrier'
            elif damage=='before_requires':before['requires']=[{'id':'unmet','status':'unsatisfied'}]
            elif damage=='before_after':before['after']={}
            elif damage=='before_satisfied':before['satisfied']=False
        elif damage in ('after_complete_only','after_requires'):
            after=next(e for e in events if e['event']=='state_step_after' and e.get('step_id')=='action:empty')
            if damage=='after_complete_only':after['after']['complete']=1
            else:after['requires']=[{'id':'unmet','status':'unsatisfied'}]
        elif damage=='complete_int':
            for e in events:
                if e['event']=='topology_action_done' and e.get('action_id')=='empty':
                    e['receipt']['after']['complete']=1
                elif e['event']=='state_step_after' and e.get('step_id')=='action:empty':
                    e['after']['complete']=1
    rewrite(runtime,corrupt)
    facts,oracle=consume(runtime,cfg)
    assert facts['diagnostic_valid'] is False
    assert oracle['verdict']!='pass'
    assert facts['candidate_eligible'] is False


def test_live_snapshot_cannot_use_the_empty_controller_ack_exception(tmp_path):
    runtime,cfg=produce(tmp_path,empty_snapshot=True)
    def corrupt(events):
        begin=next(e for e in events if e['event']=='topology_action_begin'
                   and e['action']['op']=='snapshot' and e['before'].get('alive'))
        done=next(e for e in events if e['event']=='topology_action_done'
                  and e['action_id']==begin['action_id'])
        events[:]=[e for e in events if not (begin['event_index']<e['event_index']<done['event_index']
                                            and e['event']=='command_done')]
    rewrite(runtime,corrupt)
    facts,oracle=consume(runtime,cfg)
    assert facts['diagnostic_valid'] is False
    assert oracle['verdict']!='pass'
    assert facts['candidate_eligible'] is False


@pytest.mark.parametrize('field,bad',[('support',None),('timeout_sec',True),('same_participant',1),('topic','bad\x00topic')])
def test_native_control_rejects_ambiguous_input_before_execution(field,bad):
    value=control();value[field]=bad
    with pytest.raises(ValueError):validate_control(value)


@pytest.mark.parametrize('damage',['request','pid','guid','qos','cleanup','delivery','peer','ack','fresh','sample_identity'])
def test_native_control_raw_damage_cannot_pass_or_qualify_a_candidate(tmp_path,damage):
    runtime,cfg=produce(tmp_path)
    def corrupt(events):
        if damage=='ack':
            row=next(e for e in events if e['event']=='native_matching_result')
            for e in events:
                if e['event']=='command_done' and e.get('pid')==row['pid'] and e.get('command_id')==row['command_id']:
                    e['pid']=-1
            return
        for e in events:
            if e['event'] not in ('native_matching_result','native_matching_observation'):continue
            r=e['control_receipt']
            if damage=='request':r['request']['writer_qos']['partitions']=['forged']
            elif damage=='pid':r['pid']=-1
            elif damage=='guid':r['entities'][0]['endpoint_gid']='invalid'
            elif damage=='qos':r['entities'][0]['qos']['reliability']='best_effort'
            elif damage=='cleanup':r['cleanup'][0]['return_code']=-1
            elif damage=='delivery':r['delivery']['received']='other-source'
            elif damage=='fresh':r.pop('fresh_entities',None)
            elif damage=='sample_identity':r['delivery']['sample_identity_writer_guid']='f'*32
            elif damage=='peer':r['samples'][-1]['peer_gids'][0].append('f'*32)
    rewrite(runtime,corrupt)
    facts,oracle=consume(runtime,cfg)
    assert not facts.get('native_matching_controls') or facts['native_matching_controls'][0]['status']=='unknown'
    assert oracle['verdict']=='unknown'
    assert facts['candidate_eligible'] is False


@pytest.mark.parametrize('vendor,writer,reader,expected',[
    ('cyclonedds','*','*',False), ('fastdds','*','*',True),
    ('cyclonedds','zone/*','zone/a',True), ('cyclonedds','zone/[ab]','zone/a',False),
    ('cyclonedds','zone/[ab]','zone/[ab]',True), ('cyclonedds','?','é',False),
])
def test_native_partition_contract_uses_the_pinned_vendor_pattern_rules(vendor,writer,reader,expected):
    from r2f.runtime.native_matching import compatibility
    receipt=native_receipt(control(), 1)
    w,r=receipt['entities']
    w['qos']['partitions']=[writer];r['qos']['partitions']=[reader]
    assert compatibility(w,r,vendor=vendor)[0] is expected


def test_native_control_never_erases_an_earlier_forbidden_match():
    request=control();request['writer_qos']['reliability']='best_effort'
    value=native_receipt(request,1)
    value['entities'][0]['qos']['reliability']='best_effort'
    value['fresh_entities'][0]['qos']['reliability']='best_effort'
    first=value['samples'][0]
    first['peer_gids']=[['2'*32],[]]
    value['samples'].append({'monotonic_ns':first['monotonic_ns']+1_000_000_000,'peer_gids':[[],[]]})
    for e in value['fresh_entities']:e['observed_monotonic_ns']=first['monotonic_ns']+1_100_000_000
    value['finished_monotonic_ns']=first['monotonic_ns']+1_200_000_000
    result=audit_control(request,value,pid=1,fixture=True)
    assert result['status']=='fail',result
    assert result['reason']=='native_incompatible_pair_matched'


def test_malformed_native_match_sample_is_named_unknown():
    request=control();value=native_receipt(request,1);value['samples']=[None]
    assert audit_control(request,value,pid=1,fixture=True)['status']=='unknown'


def test_fastdds_native_string_probe_on_array_harness_is_rejected_before_launch():
    from r2f.runtime.topology_pair import build_input_receipts
    p=model.generate_program(7,writers=1,readers=1)
    next(e for e in p['endpoints'] if e['logical_id']=='w0')['type_name']='std_msgs/msg/UInt8MultiArray'
    p['actions'].insert(-1,{'action_id':'native','op':'native_matching_control','endpoint_id':'w0','generation':1,
                            'params':{'control':control()},'depends_on':[]})
    receipt=build_input_receipts(p,{'rmw':'rmw_fastrtps_cpp'})
    row=next(e for e in receipt['endpoints'] if e['logical_id']=='w0')
    assert row['binding_status']=='not_expressible'
    assert row['reason']=='native_matching_control_requires_String_harness'


class ForbiddenNativeFixture(NativeFixture):
    @staticmethod
    def make_receipt(value,pid):
        receipt=native_receipt(value,pid)
        receipt['entities'][0]['qos']['reliability']='best_effort'
        receipt['fresh_entities'][0]['qos']['reliability']='best_effort'
        return receipt


def test_native_forbidden_match_enters_the_existing_primary_rule(tmp_path):
    request=control();request['writer_qos']['reliability']='best_effort'
    runtime,cfg=produce(tmp_path,request=request,backend=ForbiddenNativeFixture())
    facts,oracle=consume(runtime,cfg)
    assert facts['native_matching_controls'][0]['status']=='fail'
    assert oracle['verdict']=='fail'
    assert 'topology.pair_graph_mismatch' in oracle['forbidden_residuals']
    assert oracle['candidate_eligible'] is False


def test_pinned_fastdds_persistent_api_rejection_is_retained_as_unsupported():
    request=control();request['writer_qos']['durability']='persistent'
    value=native_receipt(request,1)
    value.update(vendor='fastdds',version='2.14.7',status='unsupported',
                 reason='native_control_create_datawriter_returned_null',entities=[],fresh_entities=[],samples=[])
    result=audit_control(request,value,pid=1,fixture=True)
    assert result['status']=='unsupported'
    assert result['sut_failure'] is False


@pytest.mark.parametrize('field,bad',[('entities',[None]),('entities',None),('fresh_entities',[None,None]),
                                     ('cleanup',[None]),('samples',[None]),('samples','invalid')])
def test_nested_malformed_native_receipts_do_not_abort_public_evidence_consumption(tmp_path,field,bad):
    runtime,cfg=produce(tmp_path)
    def corrupt(events):
        for e in events:
            if e['event'] in ('native_matching_result','native_matching_observation'):
                e['control_receipt'][field]=deepcopy(bad)
    rewrite(runtime,corrupt)
    facts,oracle=consume(runtime,cfg)
    assert not facts.get('native_matching_controls') or facts['native_matching_controls'][0]['status']=='unknown'
    assert oracle['verdict']=='unknown'


def test_native_control_topic_cannot_alias_a_ros_wire_topic():
    request=control();request['topic']='rt/r2f/topology/a'
    with pytest.raises(ValueError,match='native_matching_control_value_invalid'):
        validate_control(request)


def test_fastdds_aggregate_match_without_counterpart_guid_is_unknown():
    request=control();request['writer_qos']['partitions']=['alpha']
    value=native_receipt(request,1);value.update(vendor='fastdds',version='2.14.7')
    for collection in ('entities','fresh_entities'):
        for e in value[collection]:e['type_name']='std_msgs::msg::dds_::String_'
        value[collection][0]['qos']['partitions']=['alpha']
    sample=value['samples'][0];sample.pop('peer_gids');sample.update(writer_count=0,reader_count=1)
    value['samples'].append(dict(sample,monotonic_ns=sample['monotonic_ns']+1_000_000_000))
    for e in value['fresh_entities']:e['observed_monotonic_ns']=sample['monotonic_ns']+1_100_000_000
    value['finished_monotonic_ns']=sample['monotonic_ns']+1_200_000_000
    assert audit_control(request,value,pid=1,fixture=True)['status']=='unknown'


@pytest.mark.parametrize('field,bad',[('delivery',['invalid']),('delivery',True),
    ('delivery',{'write_return_code':False,'sent':'sample','received':'sample','sample_identity_writer_guid':'1'*32}),
    ('delivery',{'write_return_code':0,'sent':123,'received':123,'sample_identity_writer_guid':'1'*32}),
    ('participant_inventory_before',[123]),('participant_inventory_before',[None]),
    ('participant_inventory_before',['0'*32]),('participant_inventory_before',['not-a-guid'])])
def test_malformed_delivery_and_cleanup_identity_are_named_unknown(tmp_path,field,bad):
    runtime,cfg=produce(tmp_path)
    def corrupt(events):
        for e in events:
            if e['event'] in ('native_matching_result','native_matching_observation'):
                value=e['control_receipt'];value[field]=deepcopy(bad)
                if field=='participant_inventory_before':value['participant_inventory_after']=deepcopy(bad)
    rewrite(runtime,corrupt)
    facts,oracle=consume(runtime,cfg)
    assert facts['native_matching_controls'][0]['status']=='unknown'
    assert oracle['verdict']=='unknown'


@pytest.mark.parametrize('vendor',['cyclonedds','fastdds'])
@pytest.mark.parametrize('later_gap',['other_peer','malformed_sample','chronology'])
def test_proven_native_forbidden_match_survives_later_observation_gaps(tmp_path,vendor,later_gap):
    class LaterGapFixture(ForbiddenNativeFixture):
        @staticmethod
        def make_receipt(request,pid):
            value=ForbiddenNativeFixture.make_receipt(request,pid)
            value['vendor']=vendor;value['version']='0.10.5' if vendor=='cyclonedds' else '2.14.7'
            first=value['samples'][0]
            if vendor=='fastdds':
                for collection in ('entities','fresh_entities'):
                    for e in value[collection]:e['type_name']='std_msgs::msg::dds_::String_'
                first.pop('peer_gids');first.update(writer_count=1,reader_count=1,
                    writer_total_count=1,reader_total_count=1,
                    writer_last_peer_gid='2'*32,reader_last_peer_gid='1'*32)
            second=deepcopy(first);second['monotonic_ns']+=1
            if later_gap=='other_peer':
                if vendor=='cyclonedds':second['peer_gids'][0].append('a'*32)
                else:second.update(writer_count=2,writer_total_count=2,writer_last_peer_gid='a'*32)
            elif later_gap=='malformed_sample':second=None
            else:second['monotonic_ns']=first['monotonic_ns']
            value['samples'].append(second)
            return value
    request=control();request['writer_qos']['reliability']='best_effort'
    runtime,cfg=produce(tmp_path,request=request,backend=LaterGapFixture())
    facts,oracle=consume(runtime,cfg)
    observed=facts['native_matching_controls'][0]
    assert observed['status']=='fail',observed
    assert observed['observation_gaps']
    assert facts['matching_observation_incomplete'] is True
    assert oracle['verdict']=='fail'
    assert oracle['diagnostic_only'] is True
    assert oracle['candidate_eligible'] is False
    assert 'topology.pair_graph_mismatch' in oracle['forbidden_residuals']


def fast_transient_receipt(tmp_path):
    from r2f.runtime.matching_qos import digest
    request=control()
    for role in ('writer','reader'): request[role+'_qos']['durability']='transient'
    value=native_receipt(request,1);value.update(vendor='fastdds',version='2.14.7')
    tmp_path.mkdir(parents=True,exist_ok=True)
    db=tmp_path/'native.db';db.write_bytes(b'offline persistence artifact')
    value['persistence_artifacts']={'directory':str(tmp_path),'creator_pid':1,
        'recorded_monotonic_ns':value['finished_monotonic_ns'],
        'files':[{'path':str(db),'sha256':digest(db),'size':db.stat().st_size}]}
    for key in ('entities','fresh_entities'):
        for e in value[key]:
            e['type_name']='std_msgs::msg::dds_::String_';e['qos']['durability']='transient'
            prefix=e['participant_gid'][:24];suffix='00000102' if e['role']=='writer' else '00000107'
            gid=prefix+suffix
            text='.'.join(gid[i:i+2] for i in range(0,24,2))+'|'+'.'.join(gid[i:i+2] for i in range(24,32,2))
            e['persistence']={'dds.persistence.plugin':'builtin.SQLITE3',
                'dds.persistence.sqlite3.filename':str(db),'dds.persistence.guid':text}
    sample=value['samples'][0];sample.pop('peer_gids');sample.update(writer_count=1,reader_count=1,
        writer_total_count=1,reader_total_count=1,writer_last_peer_gid='2'*32,reader_last_peer_gid='1'*32)
    return request,value


@pytest.mark.parametrize('damage',['missing_properties','changed_fresh','missing_artifact','changed_artifact','other_guid'])
def test_transient_control_requires_fresh_persistence_readback_and_preserved_artifact(tmp_path,damage):
    request,value=fast_transient_receipt(tmp_path)
    if damage=='missing_properties':
        for key in ('entities','fresh_entities'):
            for e in value[key]:e.pop('persistence')
    elif damage=='changed_fresh':value['fresh_entities'][0]['persistence']['dds.persistence.plugin']='other'
    elif damage=='missing_artifact':value.pop('persistence_artifacts')
    elif damage=='changed_artifact':(tmp_path/'native.db').write_bytes(b'changed')
    else:
        for key in ('entities','fresh_entities'):
            value[key][0]['persistence']['dds.persistence.guid']='01.'*11+'01|00.00.01.02'
    result=audit_control(request,value,pid=1,fixture=True)
    assert result['status']=='unknown',result
    assert 'persistence' in result['reason']


def test_transient_control_preserves_verified_database_and_fresh_properties(tmp_path):
    request,value=fast_transient_receipt(tmp_path)
    result=audit_control(request,value,pid=1,fixture=True)
    assert result['status']=='pass',result
    assert result['limitation']=='bounded_native_control_not_ROS_QoS_or_persistence_service_proof'


@pytest.mark.parametrize('damage',['missing_artifact','changed_artifact','missing_properties','malformed_artifact'])
def test_native_forbidden_match_survives_persistence_evidence_gaps(tmp_path,damage):
    request,value=fast_transient_receipt(tmp_path)
    request['writer_qos']['partitions']=['alpha'];value['request']=deepcopy(request)
    for key in ('entities','fresh_entities'):value[key][0]['qos']['partitions']=['alpha']
    if damage=='missing_artifact':value.pop('persistence_artifacts')
    elif damage=='changed_artifact':(tmp_path/'native.db').write_bytes(b'changed')
    elif damage=='malformed_artifact':value['persistence_artifacts']['directory']=None
    else:
        for key in ('entities','fresh_entities'):value[key][0].pop('persistence')
    result=audit_control(request,value,pid=1,fixture=True)
    assert result['status']=='fail',result
    assert result['reason']=='native_incompatible_pair_matched'
    assert result['observation_gaps']
    assert result['candidate_eligible'] is False


def test_transient_control_rejects_database_outside_trusted_worker_directory(tmp_path):
    request,value=fast_transient_receipt(tmp_path/'outside')
    result=audit_control(request,value,pid=1,fixture=True,artifact_root=tmp_path/'worker'/'native-controls')
    assert result['status']=='unknown',result
    assert 'persistence' in result['reason']


def test_transient_control_accepts_canonical_worker_owned_database(tmp_path):
    root=tmp_path/'native-controls'
    request,value=fast_transient_receipt(root/('a'*32))
    result=audit_control(request,value,pid=1,fixture=True,artifact_root=root)
    assert result['status']=='pass',result


def test_transient_control_rejects_ancestor_symlink_for_database(tmp_path):
    real=tmp_path/'real';real.mkdir()
    link=tmp_path/'native-controls';link.symlink_to(real,target_is_directory=True)
    request,value=fast_transient_receipt(link/('a'*32))
    result=audit_control(request,value,pid=1,fixture=True,artifact_root=link)
    assert result['status']=='unknown',result
    assert 'persistence' in result['reason']


@pytest.mark.parametrize('forbidden',[False,True])
def test_persistence_symlink_loop_is_an_observation_gap(tmp_path,forbidden):
    request,value=fast_transient_receipt(tmp_path/'original')
    if forbidden:
        request['writer_qos']['partitions']=['alpha'];value['request']=deepcopy(request)
        for key in ('entities','fresh_entities'):value[key][0]['qos']['partitions']=['alpha']
    root=tmp_path/'native-controls';root.symlink_to(root,target_is_directory=True)
    value['persistence_artifacts']['directory']=str(root/('a'*32))
    result=audit_control(request,value,pid=1,fixture=True,artifact_root=root)
    assert result['status']==('fail' if forbidden else 'unknown'),result
    if forbidden:assert result['observation_gaps']
