"""Explicit normal public participant control; never part of passive tracing."""
from __future__ import annotations
import ctypes as C
import hashlib
import os
from pathlib import Path
import time
from .activation import digest, need, selected_manifest

SCHEMA='r2f.fastdds-participant-refresh/v1'

class RefreshResult(C.Structure):
    _fields_=[('schema_version',C.c_uint32),('struct_size',C.c_uint32),
        ('status',C.c_int32),('dds_return',C.c_int32),
        ('endpoint_guid',C.c_uint8*16),('participant_guid',C.c_uint8*16),
        ('before_size',C.c_uint32),('after_size',C.c_uint32),
        ('before',C.c_uint8*256),('after',C.c_uint8*256),
        ('complete',C.c_uint32),('reserved',C.c_uint32)]


def decode_result(value, requested, gid):
    need(C.sizeof(value)==576 and value.schema_version==1 and value.struct_size==576
         and value.status==value.dds_return==0 and value.complete==1 and value.reserved==0,
         'refresh_native_result_invalid')
    need(0<=value.before_size<=256 and 1<=value.after_size<=256,'refresh_native_size_invalid')
    before=bytes(value.before)[:value.before_size];after=bytes(value.after)[:value.after_size]
    participant=bytes(value.participant_guid).hex()
    need(bytes(value.endpoint_guid).hex()==gid and participant==gid[:24]+'000001c1', 'refresh_native_guid_mismatch')
    need(after==requested and before!=after,'refresh_noop_or_readback_mismatch')
    return dict(before_hex=before.hex(),after_hex=after.hex(),before_sha256=hashlib.sha256(before).hexdigest(),
                after_sha256=hashlib.sha256(after).hexdigest(),participant_guid=participant,
                local_gid=gid,status='applied',complete=True,dds_return=0)


def control_mapping(binary):
    path=Path(binary['path']).resolve(strict=True);before=path.stat()
    need(digest(path)==binary['sha256'],'refresh_control_binary_changed')
    device=f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}'
    found=False
    for line in Path('/proc/self/maps').read_text().splitlines():
        fields=line.split(maxsplit=5)
        if len(fields)!=6 or not Path(fields[5]).name.startswith('libr2f_fastdds_participant_refresh.so'):continue
        need(not fields[5].endswith(' (deleted)') and Path(fields[5]).resolve()==path
             and fields[3]==device and fields[4]==str(before.st_ino),'refresh_control_mapping_conflict')
        found=True
    after=path.stat()
    need(found and (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)==
         (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns),'refresh_control_mapping_missing_or_changed')
    return dict(path=str(path),sha256=binary['sha256'],device=device,inode=before.st_ino)


def refresh_participant(observer, domain, gid, data):
    began=time.monotonic_ns()
    receipt=dict(schema=SCHEMA,status='unsupported',complete=False,requested_data=data,domain_id=domain,
                 local_gid=gid,candidate_eligible=False,execution_evidence=False,started_monotonic_ns=began,
                 scope='normal_public_participant_userdata_control_not_natural_sut_candidate')
    try:
        need(isinstance(data,str) and '\0' not in data and 1<=len(data.encode('utf-8'))<=256,'refresh_request_invalid')
        need(observer is not None,'refresh_requires_explicit_fastdds_instrumentation')
        with observer._lock:
            receipt['provenance_before']=observer._native.revalidate()
            trace=selected_manifest(observer._native.manifest)
            need(trace is not None and isinstance(trace.get('refresh_control'),dict),'refresh_control_not_selected')
            control=trace['refresh_control'];binary=control['binary']
            receipt.update(manifest_reference=dict(observer._reference),control_manifest=control)
            library=C.CDLL(binary['path'],mode=os.RTLD_LOCAL|os.RTLD_NOW)
            function=library.r2f_fastdds_participant_refresh_writer_v1
            function.argtypes=[C.c_void_p,C.POINTER(C.c_uint8),C.c_uint32,C.POINTER(RefreshResult),C.c_uint32]
            function.restype=C.c_int32
            receipt['control_mapping']=control_mapping(binary)
            borrowed=observer.endpoint.handle
            with borrowed:
                chain=observer._resolve(borrowed,'writer')
                payload=data.encode('utf-8');buffer=(C.c_uint8*len(payload)).from_buffer_copy(payload)
                value=RefreshResult()
                receipt.update(call_started_monotonic_ns=time.monotonic_ns(),execution_evidence=True)
                status=function(chain[2],buffer,len(payload),C.byref(value),C.sizeof(value))
                receipt.update(call_returned_monotonic_ns=time.monotonic_ns(),return_status=status,
                               native_result_hex=bytes(value).hex())
                need(observer._resolve(borrowed,'writer')==chain,'refresh_endpoint_changed_during_call')
                need(status==value.status,'refresh_status_mismatch')
                receipt.update(decode_result(value,payload,gid))
            need(control_mapping(binary)==receipt['control_mapping'],'refresh_control_mapping_changed')
            receipt['provenance_after']=observer._native.revalidate()
    except (ValueError,TypeError,OSError,AttributeError,RuntimeError) as exc:
        receipt.update(status='unsupported' if not receipt['execution_evidence'] else 'failed',complete=False,
                       reason=str(exc),failure_type=type(exc).__name__)
    receipt['ended_monotonic_ns']=time.monotonic_ns()
    return receipt


def validate_action(events, action, receipt, begin, done, reference, *, raw_path=None):
    """Audit a normal-control API effect, not a remote discovery verdict."""
    from r2f.instrumentation.fastdds_peer.activation import manifest_for_reference
    inside=[e for e in events if begin['event_index']<e['event_index']<done['event_index']]
    observed=[e for e in inside if e.get('event')=='participant_refresh_observation' and e.get('action_id')==action['action_id']]
    need(len(observed)==1,'refresh_action_observation_missing_or_duplicate')
    value=observed[0].get('refresh_receipt',{})
    need(receipt.get('after')==dict(receipt['before'],participant_refresh=value)
         and observed[0].get('logical_id')==action['endpoint_id']
         and observed[0].get('generation')==action['generation'], 'refresh_action_receipt_not_raw_backed')
    need(reference is not None and value.get('requested_data')==action['params']['data']
         and value.get('schema')==SCHEMA and value.get('status')=='applied' and value.get('complete') is True
         and value.get('execution_evidence') is True and value.get('candidate_eligible') is False,
         'refresh_action_request_or_status_invalid')
    worker=value.get('worker_identity',{});keys=('logical_id','generation','endpoint_generation','endpoint_gid','pid','pgid','process_start_ticks')
    created=[e for e in events if e.get('event')=='endpoint_created' and e.get('logical_id')==action['endpoint_id']
             and e.get('generation')==action['generation'] and e['event_index']<begin['event_index']]
    need(len(created)==1,'refresh_action_endpoint_missing_or_duplicate')
    from r2f.runtime.endpoint_incarnation import endpoint_identity_at
    token=endpoint_identity_at(events,created[0],before_index=begin['event_index'])
    from r2f.runtime.topology_worker import worker_role_kind
    need(worker_role_kind(token.get('role'))=='writer' and receipt['before'].get('alive') is True
         and receipt['before'].get('generation')==action['generation']
         and not any(e.get('event') in {'endpoint_destroyed','topology_endpoint_destroyed'}
                     and e.get('logical_id')==action['endpoint_id'] and e.get('generation')==action['generation']
                     and e['event_index']<begin['event_index'] for e in events),'refresh_requires_current_live_writer')
    need(worker.get('logical_id')==action['endpoint_id'] and worker.get('generation')==action['generation']
         and all(worker.get(k) is not None and worker[k]==token.get(k) for k in keys), 'refresh_action_worker_mismatch')
    matching=lambda e: all(e.get(k)==worker[k] for k in keys) and e.get('command_id')==value.get('command_id')
    results=[e for e in inside if e.get('event')=='participant_refresh_result' and matching(e)]
    acks=[e for e in inside if e.get('event')=='command_done' and matching(e)]
    requests=[e for e in inside if e.get('event')=='topology_participant_refresh_requested' and e.get('command_id')==value.get('command_id')]
    writes=[e for e in inside if e.get('event')=='topology_participant_refresh_write_result' and e.get('command_id')==value.get('command_id')]
    need(len(results)==len(acks)==len(requests)==len(writes)==1,'refresh_action_command_chain_missing_or_duplicate')
    result,ack,request,write=results[0],acks[0],requests[0],writes[0]
    need(result['refresh_receipt']=={k:v for k,v in value.items() if k not in ('worker_identity','command_id')}
         and request.get('command')==write.get('command')=='refresh_participant'
         and request.get('requested_data')==action['params']['data']
         and all(e.get(k)==worker[k] for e in (request,write) for k in keys if k!='pgid'), 'refresh_action_command_payload_mismatch')
    need(write.get('status')=='flushed' and write.get('error_type') is None and type(write.get('bytes_written')) is int
         and write['bytes_written']>0 and write['bytes_written']==write.get('bytes_expected'), 'refresh_action_command_write_failed')
    clock=result.get('monotonic_clock_identity')
    need(isinstance(clock,dict) and all(e.get('monotonic_clock_identity')==clock for e in (begin,result,ack,request,write,observed[0],done)), 'refresh_action_clock_unbound')
    need(request['event_index']<result['event_index']<ack['event_index']<observed[0]['event_index']
         and request['monotonic_ns']<=value.get('started_monotonic_ns',0)<=value.get('call_started_monotonic_ns',0)
         <=value.get('call_returned_monotonic_ns',0)<=value.get('ended_monotonic_ns',0)<=result['monotonic_ns']
         and write['event_index']<ack['event_index']
         and begin['monotonic_ns']<=request['monotonic_ns']<=write['monotonic_ns']<=ack['monotonic_ns']
         and result['monotonic_ns']<=ack['monotonic_ns']<=observed[0]['monotonic_ns']<=done['monotonic_ns'],
         'refresh_action_chronology_invalid')
    manifest=manifest_for_reference(reference);trace=selected_manifest(manifest)
    expected_reference=dict(reference)
    if raw_path is not None:
        expected_reference['manifest_path']=str(Path(raw_path).resolve().parent/'native-peer-build-manifest.json')
        need(manifest_for_reference(expected_reference)==manifest,'refresh_copied_peer_manifest_mismatch')
    need(trace is not None and value.get('control_manifest')==trace.get('refresh_control')
         and value.get('manifest_reference')==expected_reference,'refresh_action_native_selection_mismatch')
    encoded=value.get('native_result_hex','')
    need(isinstance(encoded,str) and len(encoded)==1152,'refresh_action_native_bytes_missing')
    native=RefreshResult.from_buffer_copy(bytes.fromhex(encoded))
    decoded=decode_result(native,action['params']['data'].encode('utf-8'),worker['endpoint_gid'])
    need(value.get('return_status')==0 and all(value.get(k)==v for k,v in decoded.items()), 'refresh_action_native_bytes_mismatch')
    need(value.get('provenance_before')==value.get('provenance_after') and isinstance(value.get('provenance_before'),dict)
         and value['provenance_before'].get('library_provenance',{}).get('vendor_binary')==manifest['binaries']['vendor']
         and value['provenance_before'].get('library_provenance',{}).get('provider_binary')==manifest['binaries']['provider'],
         'refresh_action_provenance_mismatch')
    binary=trace['refresh_control']['binary'];path=Path(binary['path']).resolve(strict=True);st=path.stat()
    need(value.get('control_mapping')==dict(path=str(path),sha256=binary['sha256'],inode=st.st_ino,
         device=f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'), 'refresh_action_mapping_mismatch')
    return value


def validate_declarations(events, program, reference):
    actions={a['action_id']:a for a in program['actions'] if a.get('op')=='update_participant_userdata'}
    observations=[e for e in events if e.get('event')=='participant_refresh_observation']
    for event in observations:
        action=actions.get(event.get('action_id'))
        need(action is not None and event.get('logical_id')==action['endpoint_id']
             and event.get('generation')==action['generation']
             and event.get('refresh_receipt',{}).get('requested_data')==action['params']['data'],
             'refresh_undeclared_or_wrong_input_control')
        if event['refresh_receipt'].get('complete') is True:
            need(reference is not None,'refresh_unselected_native_control')
    for event in events:
        if event.get('event')!='participant_refresh_result':continue
        matching=[o for o in observations if o['refresh_receipt'].get('command_id')==event.get('command_id')
                  and all(o['refresh_receipt'].get('worker_identity',{}).get(k)==event.get(k)
                          for k in ('logical_id','generation','endpoint_generation','pid','process_start_ticks'))]
        need(len(matching)==1,'refresh_unbound_worker_result')


def validate_replay(events, program, reference, *, raw_path=None):
    """Validate canonical control terminals before any replay dispatch."""
    from r2f.core import canonical_hash
    validate_declarations(events,program,reference)
    specs={e['logical_id']:e for e in program['endpoints']}
    for index,action in enumerate(program['actions']):
        if action.get('op')!='update_participant_userdata':continue
        aid=action['action_id'];eid=action['endpoint_id']
        begins=[e for e in events if e.get('event')=='topology_action_begin' and e.get('action_id')==aid]
        dones=[e for e in events if e.get('event')=='topology_action_done' and e.get('action_id')==aid]
        need(len(begins)==len(dones)==1 and begins[0].get('action')==dones[0].get('action')==action
             and begins[0].get('action_index')==dones[0].get('action_index')==index,
             'refresh_replay_action_binding_missing_or_duplicate')
        begin,done=begins[0],dones[0]
        inside=[e for e in events if begin['event_index']<e['event_index']<done['event_index']]
        states=[e for e in inside if e.get('event')=='state_step_after' and e.get('step_id')=='action:'+aid]
        need(len(states)==1,'refresh_replay_state_missing_or_duplicate')
        state=states[0];terminal=done.get('receipt')
        need(begin['monotonic_ns']<=state.get('monotonic_ns',0)<=done['monotonic_ns']
             and begin.get('monotonic_clock_identity')==state.get('monotonic_clock_identity')==done.get('monotonic_clock_identity')
             and isinstance(begin.get('monotonic_clock_identity'),dict),'refresh_replay_terminal_chronology_invalid')
        need(isinstance(terminal,dict) and terminal.get('action')==action and terminal.get('step_id')=='action:'+aid
             and all(k in state for k in terminal)
             and canonical_hash({k:state[k] for k in terminal})==canonical_hash(terminal)
             and state.get('before')==begin.get('before')
             and isinstance(terminal.get('requires'),list) and bool(terminal['requires']),
             'refresh_replay_terminal_not_raw_backed')
        physical=dict(generation=0,alive=False,next_sequence=0)
        for event in events[:begin['event_index']]:
            if event.get('logical_id')!=eid:continue
            if event.get('event')=='endpoint_created':physical.update(generation=event['generation'],alive=True,next_sequence=0)
            elif event.get('event') in {'endpoint_destroyed','topology_endpoint_destroyed'}:physical['alive']=False
            elif event.get('event')=='send':physical['next_sequence']=event['send_sequence']+1
        need(begin.get('before')==physical,'refresh_replay_before_not_observed_lifecycle')
        if terminal.get('disposition')=='guard_reject':
            need(not (physical['alive'] and physical['generation']==action['generation'] and specs[eid]['role']=='writer'),
                 'refresh_replay_guard_not_reproduced')
            guards=[e for e in inside if e.get('event')=='topology_guard_reject' and e.get('step_id')=='action:'+aid]
            need(len(guards)==1 and terminal.get('satisfied') is False and terminal.get('after')==physical
                 and terminal.get('no_side_effect') is True and terminal.get('execution_evidence') is False
                 and terminal.get('code')=='writer_generation_alive_guard' and terminal.get('kind')=='guard'
                 and terminal['requires']==[dict(id='writer_generation_alive_guard',status='unsatisfied',kind='guard')]
                 and terminal.get('message')=='participant refresh requires a live writer generation'
                 and terminal.get('details')=={} and terminal.get('method')=='runtime_guard_before_side_effect'
                 and all(guards[0].get(k)==terminal.get(k) for k in ('step_id','code','kind','message','no_side_effect','details','before'))
                 and all(e.get('event') in {'state_step_before','topology_guard_reject','state_step_after'} for e in inside),
                 'refresh_replay_guard_not_raw_backed')
            befores=[e for e in inside if e.get('event')=='state_step_before' and e.get('step_id')=='action:'+aid]
            need(len(befores)==1 and inside==[befores[0],guards[0],state], 'refresh_replay_guard_chain_invalid')
            before=befores[0];chain=[begin,before,guards[0],state,done]
            need(canonical_hash({k:before.get(k) for k in ('step_id','action','before','after','requires','satisfied')})==
                 canonical_hash(dict(step_id='action:'+aid,action=action,before=physical,after=None,requires=[],satisfied=None)),
                 'refresh_replay_guard_before_invalid')
            controller=('pid','pgid','role','backend','monotonic_clock_identity')
            need(all(begin.get(k) is not None and all(e.get(k)==begin[k] for e in chain) for k in controller)
                 and all(a['event_index']<b['event_index'] and a['monotonic_ns']<=b['monotonic_ns']
                         for a,b in zip(chain,chain[1:])), 'refresh_replay_guard_controller_or_chronology_invalid')
            continue
        need(terminal.get('disposition')=='effect_observed' and terminal.get('satisfied') is True
             and terminal['requires']==[dict(id='native_participant_refresh_receipt',status='satisfied')],
             'refresh_original_control_unverified')
        need(specs[eid]['role']=='writer','refresh_replay_requires_writer')
        validate_action(events,action,terminal,begin,done,reference,raw_path=raw_path)
