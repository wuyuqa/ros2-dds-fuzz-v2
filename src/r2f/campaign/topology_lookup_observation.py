"""Raw-authenticated topic lookup facts; never accepts a status echo as a rule."""
from copy import deepcopy
import hashlib
import re
from r2f.modules.s1.topology_lookup_contract import RESIDUAL, predicate_contract
from r2f.runtime.endpoint_incarnation import endpoint_identity_at

KEYS=('logical_id','generation','endpoint_generation','endpoint_gid','pid','pgid','process_start_ticks')


def _require(ok,reason):
    if not ok:raise ValueError('topic_lookup:'+reason)


def _integer(value, *, minimum=0):
    return type(value) is int and minimum <= value < 2**64


def _zero(value):
    return type(value) is int and value == 0


def _one(rows,event,**fields):
    found=[e for e in rows if e.get('event')==event and all(e.get(k)==v for k,v in fields.items())]
    _require(len(found)==1,'missing_or_duplicate:'+event)
    return found[0]


def _text(value):
    return isinstance(value,str) and 0 < len(value) <= 1024 and '\0' not in value


def _native_control(receipt,action,source,ros_type):
    """Reconstruct placement from created entities, never the placement echo."""
    from r2f.runtime.native_lookup_support import validate_build_reference
    from r2f.runtime.topology_worker import CycloneMatchedPeerObserver
    control=receipt['native_type_support'];reference=action['params']['native_type_support']
    _require(isinstance(source,dict) and source.get('source_frozen') is True,'native_control_source_missing')
    manifest=validate_build_reference(reference,expected_source=source,
        sdk_sha256=CycloneMatchedPeerObserver.VERIFIED_C_HEADERS)
    _require(control.get('manifest_source')==source
             and control.get('library_path')==manifest['library_path']
             and control.get('library_sha256')==manifest['library_sha256']
             and control.get('candidate_eligible') is False
             and control.get('qualification')=='native_control_not_rmw_or_historical_binding','native_control_provenance')
    ros=control.get('bound_ros_type_name')
    _require(ros==ros_type and ros in ('std_msgs/msg/String','std_msgs/msg/UInt8MultiArray'),'native_control_bound_type')
    source_type='r2f_lookup::'+('String' if ros=='std_msgs/msg/String' else 'UInt8MultiArray')
    placement=reference['placement'];entities=control.get('entities');owner=receipt['participant_handle']
    _require(isinstance(entities,list) and len(entities)==(1 if placement=='absent' else 2),'native_control_entity_coverage')
    _require(all(isinstance(e,dict) and _integer(e.get('handle'),minimum=1)
                 and _text(e.get('name')) and _text(e.get('type_name'))
                 and _integer(e.get('participant_handle'),minimum=1)
                 and e.get('schema_source')=='LookupTypes.idl' for e in entities),'native_control_entity_readback')
    _require(len({e['handle'] for e in entities})==len(entities),'native_control_entity_reuse')
    first=entities[0];name=action['params']['name']
    _require(first['handle']==receipt.get('native_source_topic')
             and first['name']=='r2f_lookup_source_'+hashlib.sha256(name.encode()).hexdigest()[:24]
             and first['type_name']==source_type==receipt['bound_type_name']
             and first['participant_handle']==owner,'native_control_source_entity')
    owned=[('topic',first['handle'])]
    target_owner=owner
    if placement=='other_participant':
        sibling=control.get('sibling_participant',{})
        _require(_integer(sibling.get('handle'),minimum=1) and sibling['handle']!=owner
                 and isinstance(sibling.get('gid'),str) and re.fullmatch('[0-9a-f]{32}',sibling['gid']) is not None
                 and sibling['gid'] not in ('0'*32,receipt['participant_gid']),'native_control_sibling')
        target_owner=sibling['handle'];owned.append(('participant',target_owner))
    else:_require('sibling_participant' not in control,'native_control_unexpected_sibling')
    if placement!='absent':
        target=entities[1]
        target_type=('r2f_lookup::UInt8MultiArray' if source_type=='r2f_lookup::String' else 'r2f_lookup::String') if placement=='different_type' else source_type
        _require(target['name']==name and target['type_name']==target_type
                 and target['participant_handle']==target_owner,'native_control_target_entity')
        owned.append(('topic',target['handle']))
    cleanup=control.get('cleanup')
    _require(control.get('cleanup_confirmed') is True and isinstance(cleanup,list) and len(cleanup)==len(owned)
             and all(isinstance(c,dict) and c.get('kind')==kind and type(c.get('handle')) is int
                     and c['handle']==handle and _zero(c.get('return_code')) and 'error' not in c
                     for c,(kind,handle) in zip(cleanup,reversed(owned))),'native_control_cleanup_coverage')
    return (len(entities)==2 and entities[1]['type_name']==source_type
            and (target_owner==owner or action['params']['scope'] in ('local_domain','global')))


def _predicate(receipt,action,source,ros_type):
    p=action['params'];binding=receipt.get('binding')
    _require(receipt.get('schema')=='r2f.scoped-topic-lookup/v1' and binding in ('CycloneDDS_C','FastDDS_C++'), 'binding_unqualified')
    _require(receipt.get('complete') is True and receipt.get('execution_evidence') is True
             and receipt.get('cleanup_confirmed') is True and receipt.get('candidate_eligible') is False,'observation_or_cleanup_incomplete')
    _require(receipt.get('scope')==p['scope'] and receipt.get('requested_name')==p['name'],'request_mismatch')
    before,after=receipt.get('ownership_before',{}),receipt.get('ownership_after',{})
    stable=('local_handle','participant_handle','participant_gid','local_gid','domain_id')
    _require(before.get('complete') is True and after.get('complete') is True
             and all(before.get(k) is not None and type(before[k]) is type(after.get(k)) and before[k]==after.get(k) for k in stable)
             and before['local_gid']==receipt.get('local_gid')
             and before['participant_handle']==receipt.get('participant_handle')
             and before['participant_gid']==receipt.get('participant_gid'),'ownership_unverified')
    _require(all(_integer(before.get(k), minimum=1) for k in ('local_handle','participant_handle'))
             and _integer(before.get('domain_id')) and before['domain_id']<=231 and before['domain_id']==receipt.get('domain_id')
             and isinstance(receipt.get('provenance'),dict) and bool(receipt['provenance']),'native_context_unqualified')
    detail=[]
    if binding=='CycloneDDS_C':
        _require(receipt['provenance'].get('lookup_api_version')=='0.10.5'
                 and receipt['provenance'].get('observer',{}).get('abi')=='CycloneDDS-0.10.5-lp64-endpoint64'
                 and receipt.get('api')=='dds_find_topic(scope,participant,name,type_info,timeout)'
                 and type(receipt.get('find_return_code')) is int and receipt['find_return_code']>=0
                 and _zero(receipt.get('type_info_return_code')) and _zero(receipt.get('dds_free_typeinfo_return_code'))
                 and _text(receipt.get('bound_type_name'))
                 and type(receipt.get('scope_value')) is int and receipt['scope_value']=={'participant':2,'local_domain':1,'global':0}[p['scope']],'typed_api_incomplete')
        found=receipt['find_return_code']>0
        if found:
            _require(_text(receipt.get('actual_name')) and _text(receipt.get('actual_type_name'))
                     and _integer(receipt.get('result_participant_handle'),minimum=1),'found_readback_incomplete')
            _require(_zero(receipt.get('dds_delete_return_code')),'found_reference_not_released')
            if receipt.get('actual_name')!=p['name']:detail.append('result_name_differs')
            if receipt.get('actual_type_name')!=receipt['bound_type_name']:detail.append('result_type_differs')
            if receipt.get('result_participant_handle')!=before['participant_handle']:detail.append('result_owner_differs')
        control=receipt.get('native_type_support')
        _require((control or {}).get('reference')==p.get('native_type_support'),'native_control_input_mismatch')
        if control:
            expected=_native_control(receipt,action,source,ros_type)
            if found is not expected:detail.append('type_or_scope_visibility_differs')
    else:
        value=receipt.get('native_readback',{})
        _require('native_type_support' not in p and receipt.get('native_type_support') is None
                 and p['scope']=='participant' and receipt.get('api')=='DomainParticipant::find_topic(name,timeout)'
                 and value.get('lookup_status') in ('found','not_found'),'local_api_incomplete')
        stable=('local_handle','local_gid','parent_handle','participant_handle','participant_gid','role_kind','domain_id','roots','provenance')
        _require(before.get('schema')=='r2f.fastdds-participant-ownership/v1'
                 and all(before.get(k) is not None and type(before[k]) is type(after.get(k)) and before[k]==after.get(k) for k in stable)
                 and before['provenance']==receipt['provenance']
                 and all(type(value.get(k)) is type(before[k]) and value[k]==before[k] for k in stable if k!='provenance')
                 and _text(value.get('source_name')) and _text(value.get('source_type_name')),'local_native_context_incomplete')
        found=value['lookup_status']=='found'
        if found:
            _require(_text(value.get('actual_name')) and _text(value.get('actual_type_name'))
                     and _integer(value.get('result_participant_handle'),minimum=1),'found_readback_incomplete')
            _require(type(value.get('delete_topic_return_code')) is int and value['delete_topic_return_code']==0,'found_reference_not_released')
            if value.get('result_participant_handle')!=before['participant_handle']:detail.append('result_owner_differs')
            if value.get('actual_name')!=p['name']:detail.append('result_name_differs')
            if p['name']==value.get('source_name') and value.get('actual_type_name')!=value.get('source_type_name'):
                detail.append('result_type_differs')
        elif p['name']==value.get('source_name'):detail.append('existing_local_topic_not_found')
    expected_status='contract_mismatch' if detail else ('found' if found else 'not_found')
    _require(receipt.get('status')==expected_status,'status_echo_conflicts_with_readbacks')
    return detail


def lookup_observations(program,events,*,ref):
    actions=[a for a in program['actions'] if a['op']=='lookup_topic']
    result={'declared':bool(actions),'predicate_contract':predicate_contract(),'checks':[],'unknown_reasons':[]}
    for action in actions:
        aid=action['action_id']
        try:
            begin=_one(events,'topology_action_begin',action_id=aid)
            done=_one(events,'topology_action_done',action_id=aid)
            inside=[e for e in events if begin['event_index']<e['event_index']<done['event_index']]
            _require(begin.get('action')==action and done.get('action')==action,'raw_action_input_mismatch')
            observation=_one(inside,'topic_lookup_observation',action_id=aid)
            receipt=observation['lookup_receipt'];identity=receipt['worker_identity'];cid=receipt['command_id']
            _require(type(cid) is int and cid>0,'command_id_invalid')
            worker=_one(inside,'topic_lookup_result',command_id=cid,logical_id=action['endpoint_id'],generation=action['generation'])
            ack=_one(inside,'command_done',command_id=cid,logical_id=action['endpoint_id'],generation=action['generation'])
            token=endpoint_identity_at(events,worker,before_index=worker['event_index'])
            _require(all(identity.get(k) is not None and worker.get(k)==identity[k]==ack.get(k)==token.get(k) for k in KEYS),'worker_identity_mismatch')
            _require(observation.get('endpoint_gid')==token['endpoint_gid'] and receipt.get('local_gid')==token['endpoint_gid'],'endpoint_gid_mismatch')
            _require(all(worker.get(k) is not None and worker[k]==ack.get(k)==token.get(k) for k in ('role','run_id','domain_id')),'worker_context_mismatch')
            _require(worker['lookup_receipt']=={k:v for k,v in receipt.items() if k not in ('command_id','worker_identity')},'controller_worker_receipt_mismatch')
            rmw={'CycloneDDS_C':'rmw_cyclonedds_cpp','FastDDS_C++':'rmw_fastrtps_cpp'}.get(receipt.get('binding'))
            from r2f.runtime.topology_worker import worker_role_kind
            _require(worker.get('rmw_identifier')==rmw and rmw is not None
                     and receipt.get('role_kind')==worker_role_kind(worker['role'])
                     and type(receipt.get('domain_id')) is int and receipt['domain_id']==worker['domain_id'],'worker_binding_context_mismatch')
            chain=[begin,worker,ack,observation,done]
            _require(all(_integer(e.get('event_index')) and _integer(e.get('monotonic_ns'),minimum=1) for e in chain),'chronology_identity_invalid')
            _require(all(a['event_index']<b['event_index'] and a['monotonic_ns']<b['monotonic_ns'] for a,b in zip(chain,chain[1:])),'chronology_mismatch')
            sources=[e.get('source') for e in events if e.get('event')=='topology_source_checked' and e['event_index']<worker['event_index']]
            source=sources[-1] if sources and all(s==sources[0] for s in sources) else None
            declarations=[e for e in program['endpoints'] if e['logical_id']==action['endpoint_id']]
            _require(len(declarations)==1 and worker.get('message_type')==declarations[0]['type_name'],'worker_message_type_mismatch')
            detail=_predicate(receipt,action,source,declarations[0]['type_name'])
            result['checks'].append({'action_id':aid,'subject':{'logical_id':action['endpoint_id'],'generation':action['generation'],
                'scope':action['params']['scope'],'requested_name':action['params']['name'],'binding':receipt['binding']},
                'status':'mismatch' if detail else 'consistent','detail':detail,'violation':RESIDUAL if detail else None,
                'raw_refs':[ref(e) for e in (token,worker,ack,observation)]})
        except (ValueError,TypeError,KeyError,IndexError,AttributeError,OSError) as exc:
            result['unknown_reasons'].append({'action_id':aid,'reason':str(exc)})
    return result


def semantic_checks(observations):
    return [{k:deepcopy(check[k]) for k in ('action_id','subject','status','detail','violation')}
            for check in observations.get('checks',[])]
