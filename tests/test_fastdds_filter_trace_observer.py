"""Offline C ABI/worker intake checks. No hook/ROS/vendor loading."""
import ctypes as C
from types import SimpleNamespace

import pytest

from r2f.instrumentation.fastdds_filter_trace.observer import Record, Stats, decode_record, decode_stats
from r2f.runtime.topology_worker import _drain_filter_decisions, TopologyBackendError


def record():
    value = Record(schema_version=1, struct_size=4192, ordinal=1, started_ns=10,
                   completed_ns=11, pid=99, decision=0, status=0,
                   sequence_high=0, sequence_low=88, payload_length=4, payload_copied=4)
    value.reader_guid[:] = bytes.fromhex('02' * 16)
    value.writer_guid[:] = bytes.fromhex('01' * 16)
    value.payload[:4] = b'CDR!'
    return value


def test_fixed_c_abi_offsets():
    assert C.sizeof(Record) == 4192 and C.sizeof(Stats) == 48
    expected = dict(schema_version=0, struct_size=4, ordinal=8, started_ns=16, completed_ns=24,
                    pid=32, decision=36, status=40, payload_length=44, payload_copied=48,
                    sequence_high=52, sequence_low=56, encapsulation=60,
                    reader_guid=64, writer_guid=80, payload=96)
    assert {key: getattr(Record, key).offset for key in expected} == expected


def test_actual_false_is_preserved_not_inferred_from_missing_delivery():
    receipt = decode_record(record(), now_ns=20, pid=99)
    assert receipt['decision'] is False and receipt['evaluate_called'] is True
    assert receipt['rtps_sequence'] == dict(high=0, low=88, value=88)
    assert receipt['payload_cdr_hex'] == b'CDR!'.hex() and receipt['payload_complete']
    assert receipt['filter_effect_observed'] is None and receipt['candidate_eligible'] is False


@pytest.mark.parametrize('field,value', [('pid',100), ('started_ns',0), ('completed_ns',9),
    ('ordinal',0), ('decision',2), ('payload_copied',3), ('schema_version',2), ('struct_size',0)])
def test_bad_record_is_rejected(field, value):
    rec = record(); setattr(rec, field, value)
    with pytest.raises(ValueError, match='record_contract_invalid'):
        decode_record(rec, now_ns=20, pid=99)


def test_truncated_payload_cannot_claim_full_cdr_identity():
    rec = record(); rec.payload_length = 5000; rec.payload_copied = 4096
    receipt = decode_record(rec, now_ns=20, pid=99)
    assert not receipt['payload_complete'] and receipt['payload_cdr_sha256'] is None


def test_exception_cannot_be_coerced_to_filter_false():
    rec = record(); rec.status = 1; rec.decision = -1
    receipt = decode_record(rec, now_ns=20, pid=99)
    assert receipt['status'] == 'threw' and receipt['decision'] is None


def test_stats_account_for_overflow_separately():
    value = Stats(schema_version=1, struct_size=48, produced=4, consumed=2, pending=1, dropped=1)
    assert decode_stats(value)['dropped'] == 1
    value.consumed = 3
    with pytest.raises(ValueError, match='stats_contract_invalid'): decode_stats(value)


@pytest.mark.parametrize('dropped,resolver', [(0,1),(1,1),(0,2)])
def test_worker_intake_keeps_actual_decision_and_reports_tool_health(dropped, resolver):
    receipt = decode_record(record(), now_ns=20, pid=99)
    stats = dict(produced=1+dropped,consumed=1,pending=0,dropped=dropped,resolve_status=resolver)
    observer = SimpleNamespace(reference={'manifest_path':'/offline/pinned.json'},
                               drain=lambda: ([receipt],stats))
    events=[]
    emit=lambda event,**fields:events.append(dict(event=event,**fields))
    if dropped or resolver == 2:
        with pytest.raises(TopologyBackendError,match='overflow_or_resolver_failure'):
            _drain_filter_decisions(observer,emit)
    else: assert _drain_filter_decisions(observer,emit) == stats
    assert [e['event'] for e in events] == ['native_content_filter_decision','native_filter_trace_health']
    assert events[0]['receipt'] == receipt and not events[0]['candidate_eligible']
