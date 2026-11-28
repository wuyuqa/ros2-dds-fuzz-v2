"""Drain actual PDP calls from an explicitly preloaded, pinned interposer."""
from __future__ import annotations

import ctypes as C
import os
import time

from .activation import ABI, need, selected_manifest, verify_loader_selection

KINDS={1:'discovery_new',2:'discovery_update',3:'proxy_create',4:'assign_remote_endpoints',
       5:'remove_remote_participant',6:'proxy_clear',7:'proxy_update',8:'proxy_copy'}


class Record(C.Structure):
    _fields_=[('schema_version',C.c_uint32),('struct_size',C.c_uint32),
              *[(k,C.c_uint64) for k in ('ordinal','time_ns','call_id','parent_id','object','proxy')],
              *[(k,C.c_uint32) for k in ('pid','tid','kind','phase')],
              ('lock_owned',C.c_int32),('result',C.c_int32),('guid',C.c_uint8*16)]


class Stats(C.Structure):
    _fields_=[('schema_version',C.c_uint32),('struct_size',C.c_uint32),
              *[(k,C.c_uint64) for k in ('produced','consumed','dropped','pending')],
              ('resolve_errors',C.c_uint32),('enabled',C.c_uint32)]


def decode_record(value, *, pid, now_ns):
    need(value.schema_version==1 and value.struct_size==ABI['record_size']
         and C.sizeof(value)==96 and value.pid==pid and value.tid>0
         and 0<value.time_ns<=now_ns and value.ordinal>0 and value.call_id>0
         and value.parent_id!=value.call_id and value.object>0 and value.kind in KINDS
         and value.phase in (1,2) and value.lock_owned in (-1,0,1)
         and value.result in (-2,0,1) and (value.phase==2 or value.result==-2), 'record_contract_invalid')
    return dict(schema='r2f.fastdds-pdp-lifecycle-event/v1',observation_mode='instrumentation',
        ordinal=value.ordinal,capture_monotonic_ns=value.time_ns,call_id=value.call_id,
        parent_call_id=value.parent_id,process_pid=value.pid,thread_id=value.tid,
        kind=KINDS[value.kind],phase='entry' if value.phase==1 else 'return',
        completion='entry' if value.phase==1 else ('exception' if value.result==-2 else 'returned'),
        object_address=value.object,proxy_address=value.proxy,participant_guid=bytes(value.guid).hex(),
        pdp_lock_owned=None if value.lock_owned==-1 else bool(value.lock_owned),
        result=value.result,candidate_eligible=False,
        scope='actual_version_pinned_function_boundary_not_statement_level_or_global_atomic_snapshot')


class PdpLifecycleObserver:
    def __init__(self, peer):
        self.manifest=selected_manifest(peer)
        need(self.manifest is not None,'explicit_selection_required')
        self.reference=dict(peer['pdp_trace'])
        self.mapping=verify_loader_selection(peer)
        need(C.sizeof(Record)==ABI['record_size'] and C.sizeof(Stats)==ABI['stats_size'],'python_abi_mismatch')
        self.library=C.CDLL(self.manifest['binary']['path'],mode=os.RTLD_NOLOAD|os.RTLD_NOW)
        self.pop=self.library.r2f_fastdds_pdp_trace_pop_v1
        self.pop.argtypes=[C.POINTER(Record),C.c_uint32];self.pop.restype=C.c_int32
        self.stats=self.library.r2f_fastdds_pdp_trace_stats_v1
        self.stats.argtypes=[C.POINTER(Stats),C.c_uint32];self.stats.restype=C.c_int32
        self.last_ordinal=0;self.last_consumed=0

    def drain(self):
        records=[]
        for _ in range(ABI['capacity']):
            value=Record();status=self.pop(C.byref(value),C.sizeof(value))
            need(status in (0,1),'pop_abi_failure')
            if status==0:break
            row=decode_record(value,pid=os.getpid(),now_ns=time.monotonic_ns())
            need(row['ordinal']>self.last_ordinal,'ordinal_reused_or_regressed')
            self.last_ordinal=row['ordinal']
            row.update(manifest_reference=dict(self.reference),provider_mapping=dict(self.mapping))
            records.append(row)
        value=Stats();need(self.stats(C.byref(value),C.sizeof(value))==0,'stats_abi_failure')
        stats={k:int(getattr(value,k)) for k,_ in Stats._fields_}
        need(stats['schema_version']==1 and stats['struct_size']==48
             and stats['pending']<=ABI['capacity'] and stats['enabled']==int(self.manifest['capture_enabled'])
             and stats['produced']==stats['consumed']+stats['pending']+stats['dropped']
             and stats['consumed']==self.last_consumed+len(records),'stats_contract_invalid')
        self.last_consumed=stats['consumed']
        need(not stats['dropped'] and not stats['resolve_errors'],'capture_loss_or_resolver_failure')
        need(self.manifest['capture_enabled'] or stats['produced']==0,'disabled_observer_produced_records')
        return records,stats
