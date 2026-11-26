"""Authenticated lookup carrier/observation contract fixtures, never DDS evidence."""
from copy import deepcopy
import pytest
from test_topology_scoped_lookup import native_fixture,lookup_program
from r2f.runtime.scoped_topic_lookup import observe_topic_lookup


def lookup_trace(damage=None):
    observer,lib,_=native_fixture()
    if damage=='type':
        original=lib.dds_get_type_name
        def different(entity,buf,size):
            if entity==40:buf.value=b'WrongType';return 9
            return original(entity,buf,size)
        lib.dds_get_type_name=different
    value=observe_topic_lookup(observer,191,'b'*32,'writer','participant','rt/r2f/topology/a')
    program=lookup_program();action=program['actions'][1]
    token=dict(logical_id='w0',generation=1,endpoint_generation='incarnation',endpoint_gid='b'*32,
               pid=41,pgid=41,process_start_ticks=123,role='writer',worker_role='writer',
               run_id='labelled-fixture',domain_id=191,message_type='std_msgs/msg/String',rmw_identifier='rmw_cyclonedds_cpp')
    identity={k:token[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid','pid','pgid','process_start_ticks')}
    receipt={**deepcopy(value),'command_id':3,'worker_identity':identity}
    rows=[dict(event='endpoint_created',**token),dict(event='topology_action_begin',action_id=action['action_id'],action=action),
          dict(event='topic_lookup_result',command_id=3,lookup_receipt=value,**token),
          dict(event='command_done',command_id=3,**token),
          dict(event='topic_lookup_observation',action_id=action['action_id'],logical_id='w0',generation=1,
               endpoint_gid='b'*32,lookup_receipt=receipt),
          dict(event='topology_action_done',action_id=action['action_id'],action=action)]
    for i,row in enumerate(rows):row.update(event_index=i,monotonic_ns=100+i)
    return program,rows


def test_independently_observed_type_mismatch_reaches_supporting_rule():
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=lookup_trace('type')
    result=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert result['declared'] is True and not result['unknown_reasons']
    check,=result['checks'];assert check['violation']=='topology.topic_lookup_mismatch'
    assert check['status']=='mismatch' and check['detail']==['result_type_differs']
    assert check['subject']=={'logical_id':'w0','generation':1,'scope':'participant',
        'requested_name':'rt/r2f/topology/a','binding':'CycloneDDS_C'}


@pytest.mark.parametrize('damage',['ack','worker','request','ownership','cleanup','echo'])
def test_incomplete_or_wrong_lookup_evidence_never_becomes_sut_mismatch(damage):
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=lookup_trace('type')
    if damage=='ack':events[3]['command_id']=4
    elif damage=='worker':events[2]['process_start_ticks']+=1
    elif damage=='request':program['actions'][1]['params']['name']='different'
    else:
        for index in (2,4):
            receipt=events[index]['lookup_receipt']
            if damage=='ownership':receipt['ownership_after']['participant_gid']='c'*32
            elif damage=='cleanup':receipt['cleanup_confirmed']=False
            else:receipt['status']='found'
    result=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert result['unknown_reasons'] and not any(c.get('violation') for c in result['checks'])


def test_lookup_residual_outranks_unrelated_unknown_and_unknown_lookup_blocks_pass(tmp_path):
    from test_topology_adapter import RawFixture
    from r2f.modules.s1.topology_oracles import evaluate_topology
    f=RawFixture();facts=f.facts(tmp_path)
    facts['topic_lookup_observations']={'declared':True,'checks':[{'action_id':'lookup','status':'mismatch',
        'violation':'topology.topic_lookup_mismatch','subject':{'logical_id':'w0','generation':1},
        'detail':['result_type_differs']}],'unknown_reasons':[]}
    result=evaluate_topology(f.scenario,facts)
    assert result['verdict']=='fail' and result['forbidden_residuals']==['topology.topic_lookup_mismatch']
    facts['topic_lookup_observations']={'declared':True,'checks':[],'unknown_reasons':['missing lookup']}
    assert evaluate_topology(f.scenario,facts)['verdict']=='unknown'


@pytest.mark.parametrize('damage',['boolean_return','boolean_handle','domain','role','provenance','action','binding'])
def test_lookup_contract_rejects_unbound_native_context_and_boolean_c_abi_values(damage):
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=lookup_trace('type')
    if damage=='action':events[1]['action']={**deepcopy(program['actions'][1]),'action_id':'other'}
    elif damage=='binding':events[2]['rmw_identifier']='rmw_fastrtps_cpp'
    else:
        for index in (2,4):
            receipt=events[index]['lookup_receipt']
            if damage=='boolean_return':receipt['type_info_return_code']=False
            elif damage=='boolean_handle':
                receipt['ownership_before']['local_handle']=True
                receipt['ownership_after']['local_handle']=True
            elif damage=='domain':receipt['domain_id']=192
            elif damage=='role':receipt['role_kind']='reader'
            else:receipt['provenance']={}
    result=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert result['unknown_reasons'] and not any(c.get('violation') for c in result['checks'])


@pytest.mark.parametrize('missing',['actual_name','actual_type_name','result_participant_handle'])
def test_missing_found_readback_is_unknown_rather_than_a_semantic_mismatch(missing):
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=lookup_trace('type')
    for index in (2,4):events[index]['lookup_receipt'].pop(missing)
    observed=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert observed['unknown_reasons'] and not observed['checks']


def test_native_placement_echo_without_created_entities_is_not_lookup_failure():
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=lookup_trace()
    reference={'manifest_path':'/not/a/real/qualified/control.json','manifest_sha256':'a'*64,'placement':'same_participant'}
    program['actions'][1]['params']['native_type_support']=reference
    for index in (2,4):
        receipt=events[index]['lookup_receipt'];receipt.update(status='contract_mismatch',find_return_code=0,
            native_type_support={'reference':deepcopy(reference),'entities':[],'cleanup':[],'cleanup_confirmed':True})
    events[1]['action']=deepcopy(program['actions'][1]);events[5]['action']=deepcopy(program['actions'][1])
    observed=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert observed['unknown_reasons'] and not observed['checks']


def native_control_trace(tmp_path,placement='same_participant',scope='participant'):
    """Authored readback + immutable descriptor-artifact fixture, never dlopen."""
    import hashlib,json
    from r2f.runtime import native_lookup_support as support
    from r2f.runtime.topology_worker import CycloneMatchedPeerObserver
    from test_topology_cycle import SOURCE
    program,events=lookup_trace()
    binary=tmp_path/'descriptor-fixture.bin';binary.write_bytes(b'not a DSO: offline immutable control input')
    manifest={'schema':support.SCHEMA,'source':deepcopy(SOURCE),
        'source_sha256':{k:hashlib.sha256(v.encode()).hexdigest() for k,v in support.SOURCES.items()},
        'sdk_sha256':dict(CycloneMatchedPeerObserver.VERIFIED_C_HEADERS),
        'library_path':str(binary),'library_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()}
    path=tmp_path/'manifest.json';path.write_text(json.dumps(manifest))
    reference={'manifest_path':str(path),'manifest_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'placement':placement}
    action=program['actions'][1];action['params'].update(scope=scope,native_type_support=reference)
    source_name='r2f_lookup_source_'+hashlib.sha256(action['params']['name'].encode()).hexdigest()[:24]
    entities=[{'handle':90,'name':source_name,'type_name':'r2f_lookup::String','participant_handle':10,'schema_source':'LookupTypes.idl'}]
    owned=[('topic',90)]
    control={'reference':reference,'entities':entities,'cleanup_confirmed':True,
        'qualification':'native_control_not_rmw_or_historical_binding','bound_ros_type_name':'std_msgs/msg/String',
        'candidate_eligible':False,'library_path':str(binary),'library_sha256':manifest['library_sha256'],
        'manifest_source':deepcopy(SOURCE)}
    if placement!='absent':
        owner=20 if placement=='other_participant' else 10
        if owner==20:
            control['sibling_participant']={'handle':20,'gid':'c'*32};owned.append(('participant',20))
        entities.append({'handle':91,'name':action['params']['name'],
            'type_name':'r2f_lookup::UInt8MultiArray' if placement=='different_type' else 'r2f_lookup::String',
            'participant_handle':owner,'schema_source':'LookupTypes.idl'});owned.append(('topic',91))
    control['cleanup']=[{'kind':k,'handle':h,'return_code':0} for k,h in reversed(owned)]
    visible=placement=='same_participant' or placement=='other_participant' and scope!='participant'
    for index in (2,4):
        receipt=events[index]['lookup_receipt'];receipt.update(scope=scope,scope_value={'participant':2,'local_domain':1,'global':0}[scope],
            native_type_support=deepcopy(control),native_source_topic=90,bound_rmw_type_name=receipt['bound_type_name'],
            bound_type_name='r2f_lookup::String',type_selection='native_type_info_from_explicit_generated_control',
            find_return_code=0,status='contract_mismatch' if visible else 'not_found')
        for field in ('actual_name','actual_type_name','result_participant_handle','dds_delete_return_code'):receipt.pop(field,None)
    events[1]['action']=deepcopy(action);events[5]['action']=deepcopy(action)
    events.insert(1,{'event':'topology_source_checked','source':deepcopy(SOURCE)})
    for i,e in enumerate(events):e.update(event_index=i,monotonic_ns=100+i)
    return program,events


@pytest.mark.parametrize('placement,scope,mismatch',[
    ('same_participant','participant',True),('other_participant','participant',False),
    ('other_participant','local_domain',True),('different_type','participant',False),('absent','global',False)])
def test_native_scope_expectations_require_independent_entity_and_cleanup_readbacks(tmp_path,placement,scope,mismatch):
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=native_control_trace(tmp_path,placement,scope)
    observed=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert not observed['unknown_reasons'],observed
    assert len(observed['checks'])==1 and (observed['checks'][0]['status']=='mismatch') is mismatch


@pytest.mark.parametrize('damage',['source','target_owner','target_name','target_type','entity_count','cleanup','sibling','manifest_source','library'])
def test_native_control_claim_cannot_replace_missing_or_conflicting_readbacks(tmp_path,damage):
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=native_control_trace(tmp_path,'other_participant','global')
    for event in events:
        if 'lookup_receipt' not in event:continue
        control=event['lookup_receipt']['native_type_support']
        if damage=='source':control['entities'][0]['handle']=0
        elif damage=='target_owner':control['entities'][1]['participant_handle']=10
        elif damage=='target_name':control['entities'][1]['name']='different'
        elif damage=='target_type':control['entities'][1]['type_name']='different'
        elif damage=='entity_count':control['entities'].pop()
        elif damage=='cleanup':control['cleanup'].pop()
        elif damage=='sibling':control['sibling_participant']['gid']='a'*32
        elif damage=='manifest_source':control['manifest_source']['head']='f'*40
        else:control['library_sha256']='f'*64
    observed=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert observed['unknown_reasons'] and not observed['checks']


@pytest.mark.parametrize('field,before_value,after_value',[('domain_id',0,False),('local_handle',1,True),('participant_handle',1,True)])
def test_each_ownership_sample_rejects_boolean_integer_aliases(field,before_value,after_value):
    from r2f.campaign.topology_lookup_observation import lookup_observations
    program,events=lookup_trace('type')
    if field=='domain_id':
        for event in events:
            if 'domain_id' in event:event['domain_id']=0
    for index in (2,4):
        receipt=events[index]['lookup_receipt']
        if field=='domain_id':receipt['domain_id']=0
        receipt['ownership_before'][field]=before_value;receipt['ownership_after'][field]=after_value
        if field=='participant_handle':receipt['participant_handle']=before_value;receipt['result_participant_handle']=before_value
    observed=lookup_observations(program,events,ref=lambda e:{'event_index':e['event_index']})
    assert observed['unknown_reasons'] and not observed['checks']
