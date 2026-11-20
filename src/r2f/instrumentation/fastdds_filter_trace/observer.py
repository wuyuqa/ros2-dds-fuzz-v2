"""Drain opt-in actual evaluate calls; never infer filtering from absent delivery."""
from __future__ import annotations

import ctypes as C
import hashlib
import os
import time

from .activation import ABI, _need, mapped_identity, selected_manifest


class Record(C.Structure):
    _fields_ = [('schema_version', C.c_uint32), ('struct_size', C.c_uint32),
                ('ordinal', C.c_uint64), ('started_ns', C.c_uint64), ('completed_ns', C.c_uint64),
                ('pid', C.c_uint32), ('decision', C.c_int32), ('status', C.c_uint32),
                ('payload_length', C.c_uint32), ('payload_copied', C.c_uint32),
                ('sequence_high', C.c_int32), ('sequence_low', C.c_uint32), ('encapsulation', C.c_uint32),
                ('reader_guid', C.c_uint8 * 16), ('writer_guid', C.c_uint8 * 16),
                ('payload', C.c_uint8 * 4096)]


class Stats(C.Structure):
    _fields_ = [('schema_version', C.c_uint32), ('struct_size', C.c_uint32),
                ('produced', C.c_uint64), ('consumed', C.c_uint64), ('dropped', C.c_uint64),
                ('pending', C.c_uint64), ('resolve_status', C.c_uint32), ('reserved', C.c_uint32)]


def decode_record(record, *, now_ns, pid):
    _need(C.sizeof(record) == ABI['record_size'] and record.schema_version == 1
          and record.struct_size == ABI['record_size'] and record.pid == pid
          and record.ordinal > 0 and 0 < record.started_ns <= record.completed_ns <= now_ns
          and record.status in (0, 1)
          and record.decision in ((0, 1) if record.status == 0 else (-1,))
          and record.payload_copied == min(record.payload_length, ABI['payload_capacity']),
          'record_contract_invalid')
    payload = bytes(record.payload[:record.payload_copied])
    complete = record.payload_length == record.payload_copied
    return dict(schema='r2f.fastdds-native-filter-decision/v1', observation_mode='instrumentation',
        capture_basis='DDSSQLFilter.DDSFilterExpression.evaluate.actual_return', evaluate_called=True,
        native_result={**{key: int(getattr(record, key)) for key, _ in Record._fields_[:13]},
                       'reader_guid': bytes(record.reader_guid).hex(),
                       'writer_guid': bytes(record.writer_guid).hex(), 'payload_hex': payload.hex()},
        ordinal=record.ordinal, process_pid=record.pid,
        capture_started_monotonic_ns=record.started_ns, capture_completed_monotonic_ns=record.completed_ns,
        status='returned' if record.status == 0 else 'threw',
        decision=bool(record.decision) if record.status == 0 else None,
        reader_gid=bytes(record.reader_guid).hex(), writer_gid=bytes(record.writer_guid).hex(),
        rtps_sequence=dict(high=record.sequence_high, low=record.sequence_low,
                           value=(record.sequence_high << 32) + record.sequence_low),
        payload_encapsulation=record.encapsulation, payload_cdr_length=record.payload_length,
        payload_captured_length=record.payload_copied, payload_complete=complete,
        payload_cdr_hex=payload.hex(), payload_cdr_sha256=hashlib.sha256(payload).hexdigest() if complete else None,
        candidate_eligible=False, filter_effect_observed=None,
        scope='actual_evaluate_call_only_not_cached_or_inline_qos_decision')


def decode_stats(value):
    _need(value.schema_version == 1 and value.struct_size == ABI['stats_size']
          and value.reserved == 0 and value.resolve_status in (0, 1, 2)
          and value.pending <= 128
          and value.produced == value.consumed + value.pending + value.dropped, 'stats_contract_invalid')
    return {key: int(getattr(value, key)) for key, _ in Stats._fields_}


class FilterDecisionObserver:
    def __init__(self, peer_manifest):
        self.manifest = selected_manifest(peer_manifest)
        _need(self.manifest is not None, 'explicit_selection_required')
        self.reference = dict(peer_manifest['filter_trace'])
        binary = self.manifest['binary']
        _need(os.environ.get('LD_PRELOAD') == binary['path'], 'preload_differs_from_selected_manifest')
        self.mapping = mapped_identity(self.manifest)
        _need(C.sizeof(Record) == ABI['record_size'] and C.sizeof(Stats) == ABI['stats_size'], 'python_abi_mismatch')
        self.library = C.CDLL(binary['path'], mode=os.RTLD_NOLOAD | os.RTLD_NOW)
        self.pop = self.library.r2f_fastdds_filter_trace_pop_v1
        self.pop.argtypes = [C.POINTER(Record), C.c_uint32]
        self.pop.restype = C.c_int32
        self.stats = self.library.r2f_fastdds_filter_trace_stats_v1
        self.stats.argtypes = [C.POINTER(Stats), C.c_uint32]
        self.stats.restype = C.c_int32
        self.last_ordinal, self.last_consumed = 0, 0

    def drain(self):
        records = []
        for _ in range(128):
            value = Record()
            status = self.pop(C.byref(value), C.sizeof(value))
            _need(status in (0, 1), 'pop_abi_failure')
            if status == 0:
                break
            receipt = decode_record(value, now_ns=time.monotonic_ns(), pid=os.getpid())
            _need(receipt['ordinal'] > self.last_ordinal, 'observation_ordinal_reused_or_regressed')
            self.last_ordinal = receipt['ordinal']
            receipt.update(manifest_reference=dict(self.reference), provider_mapping=dict(self.mapping))
            records.append(receipt)
        value = Stats()
        _need(self.stats(C.byref(value), C.sizeof(value)) == 0, 'stats_abi_failure')
        stats = decode_stats(value)
        _need(stats['consumed'] == self.last_consumed + len(records), 'drain_consumer_not_unique')
        self.last_consumed = stats['consumed']
        return records, stats
