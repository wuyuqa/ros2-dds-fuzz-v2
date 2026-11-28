"""Native carrier records are checked before they become persisted facts."""
import ctypes as C

import pytest


def test_native_record_rejects_wrong_process_and_future_clock():
    from r2f.instrumentation.fastdds_pdp_trace.observer import Record, decode_record
    value=Record(schema_version=1,struct_size=96,ordinal=1,time_ns=10,call_id=1,
                 object=10,proxy=20,pid=55,tid=56,kind=3,phase=2,lock_owned=-1,result=1)
    assert C.sizeof(value)==96
    valid=decode_record(value,pid=55,now_ns=11)
    assert valid['kind']=='proxy_create' and valid['proxy_address']==20
    with pytest.raises(ValueError):decode_record(value,pid=99,now_ns=11)
    with pytest.raises(ValueError):decode_record(value,pid=55,now_ns=9)


def test_native_unwinding_is_not_reported_as_a_successful_return():
    from r2f.instrumentation.fastdds_pdp_trace.observer import Record, decode_record
    value=Record(schema_version=1,struct_size=96,ordinal=1,time_ns=10,call_id=1,
                 object=10,proxy=20,pid=55,tid=56,kind=3,phase=2,lock_owned=-1,result=-2)
    assert decode_record(value,pid=55,now_ns=11)['completion']=='exception'


def test_copy_boundary_preserves_distinct_source_and_destination_addresses():
    from r2f.instrumentation.fastdds_pdp_trace.observer import Record, decode_record
    value=Record(schema_version=1,struct_size=96,ordinal=1,time_ns=10,call_id=1,
                 object=10,proxy=20,pid=55,tid=56,kind=8,phase=2,lock_owned=-1,result=1)
    row=decode_record(value,pid=55,now_ns=11)
    assert row['kind']=='proxy_copy' and row['object_address']==10 and row['proxy_address']==20


from test_topology_peer_unavailable import snapshot_worker


def test_pdp_constructor_failure_still_closes_worker_context(snapshot_worker):
    rows=snapshot_worker('writer',observer_case='pdp_init_error',expected_code=1)
    clean={e['component']:e for e in rows if e['event']=='worker_cleanup_step'}
    assert clean['executor']['status']==clean['node']['status']==clean['context']['status']=='returned'
    assert rows[-1]['event']=='worker_error' and 'offline_pdp_initialization_failed' in rows[-1]['error']
    assert not any(e['event']=='endpoint_created' for e in rows)
    assert not any(e['event']=='native_pdp_trace_health' and e.get('capture_complete') for e in rows)
