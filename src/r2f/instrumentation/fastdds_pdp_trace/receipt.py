"""Check the PDP carrier chain through the common execution/replay audits."""
from __future__ import annotations
import os
from pathlib import Path
import re
from .activation import need, selected_manifest
from .observer import KINDS

OWNER_KEYS=('logical_id','generation','endpoint_generation','pid','pgid','process_start_ticks','run_id','domain_id')


def validate_raw(events, peer, *, peer_reference=None):
    observed=[e for e in events if str(e.get('event','')).startswith('native_pdp_')]
    trace=selected_manifest(peer) if peer is not None else None
    if trace is None:
        need(not observed,'unselected_observer_has_raw')
        return
    created=[e for e in events if e.get('event')=='endpoint_created']
    need(bool(created),'workers_missing')
    assigned=set()
    for worker in created:
        owner=tuple(worker.get(k) for k in OWNER_KEYS)
        need(all(v is not None for v in owner),'worker_identity_missing')
        own=[e for e in observed if tuple(e.get(k) for k in OWNER_KEYS)==owner]
        assigned.update(id(e) for e in own)
        need(all(e.get('observation_mode')=='instrumentation' and e.get('candidate_eligible') is False for e in own),
             'raw_mode_or_candidate_mismatch')
        selected=[e for e in own if e['event']=='native_pdp_provider_selected']
        need(len(selected)==1,'worker_selection_missing_or_duplicated')
        selection=selected[0]
        need(selection.get('manifest_reference')==peer['pdp_trace']
             and selection.get('capture_enabled') is trace['capture_enabled']
             and selection.get('binary')==trace['binary']
             and selection.get('vendor_binary')==peer['binaries']['vendor']
             and selection.get('abi')==trace['abi']
             and (peer_reference is None or selection.get('peer_manifest_reference')==peer_reference)
             and selection.get('event_index',-1)<worker.get('event_index',-1),'worker_selection_mismatch')
        mapping=selection.get('provider_mapping',{})
        path=Path(trace['binary']['path']).resolve(strict=True);st=path.stat()
        need(mapping.get('path')==str(path) and mapping.get('sha256')==trace['binary']['sha256']
             and mapping.get('inode')==st.st_ino
             and mapping.get('device')==f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}', 'provider_mapping_mismatch')
        health=[e for e in own if e['event']=='native_pdp_trace_health' and e.get('final') is True]
        need(len(health)==1 and health[0].get('capture_complete') is True
             and health[0].get('manifest_reference')==peer['pdp_trace'],'final_health_missing_or_incomplete')
        facts=[e for e in own if e['event']=='native_pdp_lifecycle']
        need(trace['capture_enabled'] or not facts,'disabled_observer_has_capture')
        shutdown=[e for e in events if e.get('event')=='worker_shutdown'
                  and tuple(e.get(k) for k in OWNER_KEYS)==owner]
        context=[e for e in events if e.get('event')=='worker_cleanup_step' and e.get('component')=='context'
                 and tuple(e.get(k) for k in OWNER_KEYS)==owner]
        need(len(shutdown)==len(context)==1 and context[0].get('status')=='returned'
             and shutdown[0].get('cleanup_outcomes',{}).get('context',{}).get('status')=='returned'
             and context[0].get('event_index',-1)<shutdown[0].get('event_index',-1)<health[0].get('event_index',-1)
             and 0<context[0].get('monotonic_ns',0)<=shutdown[0].get('monotonic_ns',0)<=health[0].get('monotonic_ns',0),
             'final_health_precedes_context_shutdown_or_cleanup_failed')
        stats=health[0].get('stats',{})
        need(all(type(stats.get(k)) is int for k in ('schema_version','struct_size','produced','consumed','dropped',
                                                        'pending','resolve_errors','enabled')), 'health_field_invalid')
        need(stats.get('schema_version')==1 and stats.get('struct_size')==48
             and stats.get('dropped')==stats.get('pending')==stats.get('resolve_errors')==0
             and stats.get('produced')==stats.get('consumed')==len(facts)
             and stats.get('enabled')==int(trace['capture_enabled']), 'final_health_counts_invalid')
        calls={};stacks={};times={}
        for ordinal,event in enumerate(facts,1):
            row=event.get('receipt',{})
            need(row.get('schema')=='r2f.fastdds-pdp-lifecycle-event/v1'
                 and row.get('manifest_reference')==peer['pdp_trace'] and row.get('provider_mapping')==mapping
                 and row.get('process_pid')==worker['pid'] and row.get('ordinal')==ordinal
                 and row.get('candidate_eligible') is False and row.get('observation_mode')=='instrumentation'
                 and row.get('kind') in KINDS.values() and row.get('phase') in ('entry','return')
                 and row.get('completion')==('entry' if row.get('phase')=='entry' else
                                              ('exception' if row.get('result')==-2 else 'returned'))
                 and isinstance(row.get('participant_guid'),str)
                 and re.fullmatch('[0-9a-f]{32}',row['participant_guid']), 'raw_record_identity_invalid')
            for name in ('ordinal','capture_monotonic_ns','thread_id','call_id','object_address'):
                need(type(row.get(name)) is int and row[name]>0,'raw_record_field_invalid:'+name)
            need(type(row.get('parent_call_id')) is int and row['parent_call_id']>=0
                 and type(row.get('proxy_address')) is int and row['proxy_address']>=0
                 and type(row.get('result')) is int and row['result'] in (-2,0,1)
                 and (row.get('pdp_lock_owned') is None or type(row['pdp_lock_owned']) is bool)
                 and row['capture_monotonic_ns']<=event.get('monotonic_ns',0), 'raw_record_contract_invalid')
            tid=row['thread_id'];stack=stacks.setdefault(tid,[])
            need(row['capture_monotonic_ns']>=times.get(tid,0),'thread_clock_regressed')
            times[tid]=row['capture_monotonic_ns']
            call=row['call_id']
            if row['phase']=='entry':
                need(call not in calls and row['result']==-2
                     and row['parent_call_id']==(stack[-1] if stack else 0),'call_entry_order_invalid')
                calls[call]=row;stack.append(call)
            else:
                need(bool(stack) and stack.pop()==call and call in calls,'call_return_order_invalid')
                entry=calls[call]
                need(all(row.get(k)==entry.get(k) for k in ('parent_call_id','thread_id','object_address','kind','participant_guid'))
                     and (row['kind']=='proxy_create' or row['proxy_address']==entry['proxy_address']), 'call_return_identity_invalid')
        need(not any(stacks.values()),'incomplete_native_calls')
        need(health[0].get('event_index',-1)>max((e.get('event_index',-1) for e in facts),default=selection['event_index']),
             'final_health_precedes_capture')
        need(all(e['event'] in ('native_pdp_provider_selected','native_pdp_lifecycle','native_pdp_trace_health') for e in own),
             'unexpected_observer_error_or_event')
    need(len(assigned)==len(observed),'raw_has_unbound_worker')
