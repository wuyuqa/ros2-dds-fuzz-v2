"""Public carrier receipts from a labelled native C ABI double; no ROS/DDS."""
import copy
import ctypes as C
import json
from types import SimpleNamespace
import pytest
from test_fastdds_peer_observer import WRITER, make_observer


def local_response(observer, native, **changes):
    root={'participant_handle':33,'participant_gid':'a'*32,'domain_id':215}
    value={'schema_version':1,'domain_id':215,'roots':[root],
           'local_handle':0x1200,'local_gid':WRITER,'parent_handle':22,
           'participant_handle':33,'participant_gid':'a'*32,'role_kind':'writer'}
    value.update(changes)
    def call(pointer,reader,domain,name,output,capacity):
        assert observer.endpoint.handle.active
        assert pointer==0x1200 and reader==0 and domain==215
        payload=json.dumps(value).encode()+b'\0';C.memmove(output,payload,len(payload));return 0
    native.libs={'provider':SimpleNamespace(r2f_fastdds_local_v1=call)}
    native.local_api=lambda: call  # explicit labelled C-ABI boundary double
    return value


def test_current_borrowed_endpoint_qualifies_native_parent_and_domain(make_observer):
    observer,native,_=make_observer('writer')
    local_response(observer,native)
    result=observer.sample_ownership(215,WRITER,'writer')
    assert result['complete'] is True and result['status']=='observed'
    assert result['participant_gid']=='a'*32 and result['local_gid']==WRITER
    assert result['schema']=='r2f.fastdds-participant-ownership/v1'
    assert result['candidate_eligible'] is False
    assert observer.endpoint.handle.entries==observer.endpoint.handle.exits==1


@pytest.mark.parametrize('changes',[{'domain_id':216},{'local_gid':'b'*32},
    {'participant_gid':'b'*32},{'roots':[]},{'parent_handle':0}])
def test_wrong_endpoint_domain_or_root_never_grants_ownership(make_observer,changes):
    observer,native,_=make_observer('writer');local_response(observer,native,**changes)
    result=observer.sample_ownership(215,WRITER,'writer')
    assert result['complete'] is False and result['status']=='unknown'
    assert result['reason'] and result['candidate_eligible'] is False


def test_explicit_fast_group_uses_instrumented_mode_and_keeps_other_modes_blocked():
    from r2f.runtime.participant_groups import validate_control
    from test_topology_participant_control_input import controlled
    config={'rmw':'rmw_fastrtps_cpp','observation_mode':'instrumentation',
            'fastdds_peer_instrumentation':{'manifest_path':'/qualified/build.json','manifest_sha256':'a'*64}}
    assert validate_control(controlled(),config)
    for damage in ({'observation_mode':'blackbox'}, {'fastdds_peer_instrumentation':None},
                   {'environment_mutation':{'kind':'other'}}):
        with pytest.raises(ValueError,match='participant_control_scope'):
            validate_control(controlled(),{**config,**damage})


def fast_group_capture():
    roots={'schema':'r2f.fastdds-local-participant-roots/v1','domain_id':215,'complete':True,
           'count':1,'roots':[{'participant_handle':33,'participant_gid':'a'*32,'domain_id':215}],
           'provenance':{'mapped_libraries':[{'path':'/labelled/carrier-double','sha256':'f'*64}]}}
    token={'logical_id':'w0','generation':1,'endpoint_generation':'incarnation-1',
           'endpoint_gid':WRITER,'role':'writer','kind':'writer'}
    sample={'schema':'r2f.fastdds-participant-ownership/v1','schema_version':1,'complete':True,
            'status':'observed','candidate_eligible':False,'local_handle':0x1200,'parent_handle':22,
            'local_gid':WRITER,'role_kind':'writer','participant_handle':33,'participant_gid':'a'*32,
            'domain_id':215,'roots':copy.deepcopy(roots['roots']),'provenance':copy.deepcopy(roots['provenance'])}
    return {'roots_before':roots,'roots_after':copy.deepcopy(roots),
            'endpoints':[{**token,'samples':[sample,copy.deepcopy(sample)]}]},[token]


def test_fast_group_relation_requires_native_parent_root_and_stable_double_samples():
    from r2f.runtime.participant_groups import qualify_observation
    observation,tokens=fast_group_capture()
    result=qualify_observation(observation,tokens)
    assert result['group_relation_observed'] is True and result['local_root_inventory_complete'] is True
    for change in ('root','sample','role','identity','missing'):
        bad=copy.deepcopy(observation)
        if change=='root':bad['roots_after']['roots'][0]['participant_gid']='b'*32
        elif change=='sample':bad['endpoints'][0]['samples'][1]['parent_handle']=23
        elif change=='role':bad['endpoints'][0]['samples'][0]['role_kind']='reader'
        elif change=='identity':bad['endpoints'][0]['samples'][0]['local_gid']='b'*32
        else:bad['endpoints'][0]['samples'].pop()
        assert qualify_observation(bad,tokens)['group_relation_observed'] is False


def test_fast_local_find_returns_public_name_type_and_releases_added_reference(make_observer):
    observer,native,_=make_observer('writer')
    local_response(observer,native,source_name='rt/local',source_type_name='String',
                   result_participant_handle=33,lookup_status='found',actual_name='rt/local',actual_type_name='String',delete_topic_return_code=0)
    result=observer.lookup_topic(215,WRITER,'writer','participant','rt/local')
    assert result['schema']=='r2f.scoped-topic-lookup/v1'
    assert result['status']=='found' and result['complete'] is True and result['cleanup_confirmed'] is True
    assert result['binding']=='FastDDS_C++' and result['actual_type_name']=='String'
    assert result['candidate_eligible'] is False
    assert result['api']=='DomainParticipant::find_topic(name,timeout)'


@pytest.mark.parametrize('damage,status',[({'actual_name':'wrong'},'contract_mismatch'),
    ({'actual_type_name':'Wrong'},'contract_mismatch'),
    ({'delete_topic_return_code':1},'tool_error')])
def test_fast_find_distinguishes_authenticated_mismatch_and_cleanup_error(make_observer,damage,status):
    observer,native,_=make_observer('writer')
    value=dict(source_name='rt/local',source_type_name='String',result_participant_handle=33,lookup_status='found',
               actual_name='rt/local',actual_type_name='String',delete_topic_return_code=0)
    local_response(observer,native,**{**value,**damage})
    result=observer.lookup_topic(215,WRITER,'writer','participant','rt/local')
    assert result['status']==status and result['candidate_eligible'] is False
    assert result['complete'] is (status=='contract_mismatch')


def test_cyclone_only_lookup_scopes_are_explicit_fast_capability_limits(make_observer):
    observer,native,_=make_observer('writer')
    local_response(observer,native)
    for scope in ('global','local_domain'):
        result=observer.lookup_topic(215,WRITER,'writer',scope,'rt/local')
        assert result['status']=='unsupported' and result['complete'] is False
        assert result['reason']=='fastdds_find_topic_has_participant_local_scope_only'
    assert not native.chains


@pytest.mark.parametrize('damage,expected',[(None,'consistent'),('type','mismatch'),('native_domain','unknown'),('missing_type','unknown')])
def test_fast_lookup_receipt_crosses_authenticated_raw_rule_boundary(make_observer,damage,expected):
    from copy import deepcopy
    from test_topology_lookup_contract import lookup_trace
    from r2f.campaign.topology_lookup_observation import lookup_observations
    observer,native,_=make_observer('writer')
    value=dict(source_name='rt/local',source_type_name='String',result_participant_handle=33,
        lookup_status='found',actual_name='rt/local',actual_type_name='Wrong' if damage=='type' else 'String',delete_topic_return_code=0)
    local_response(observer,native,**value)
    receipt=observer.lookup_topic(215,WRITER,'writer','participant','rt/local')
    program,events=lookup_trace();program['actions'][1]['params']['name']='rt/local'
    for event in events:
        if 'domain_id' in event:event['domain_id']=215
        if 'rmw_identifier' in event:event['rmw_identifier']='rmw_fastrtps_cpp'
        if 'endpoint_gid' in event:event['endpoint_gid']=WRITER
        if 'action' in event:event['action']=deepcopy(program['actions'][1])
        if 'lookup_receipt' in event:event['lookup_receipt']=deepcopy(receipt)
    events[4]['lookup_receipt'].update(command_id=3,worker_identity={k:events[0][k] for k in
        ('logical_id','generation','endpoint_generation','endpoint_gid','pid','pgid','process_start_ticks')})
    for i in (2,4):
        if damage=='native_domain':events[i]['lookup_receipt']['native_readback']['domain_id']=216
        elif damage=='missing_type':events[i]['lookup_receipt']['native_readback'].pop('actual_type_name')
    observed=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    if expected=='unknown':assert observed['unknown_reasons'] and not observed['checks']
    else:
        assert not observed['unknown_reasons'],observed
        assert len(observed['checks'])==1 and observed['checks'][0]['status']==expected
