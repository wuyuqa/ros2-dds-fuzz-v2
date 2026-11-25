"""Array carrier raw -> strict scoped delivery facts; no live DDS evidence."""
from copy import deepcopy

import pytest

from test_publish_occurrence import fixture
from r2f.runtime.topology_pair import pair_delivery_fact, payload_delivery_witness


ARRAY = 'std_msgs/msg/UInt8MultiArray'


def array_capture():
    rows, pair = fixture()
    for row in rows:
        row['message_type'] = ARRAY
        if row['event'] in {'send', 'receive'}:
            row['payload_text'] = row.pop('string_data')
            row['string_data'] = None
            row['payload_encoding'] = 'utf8_json_uint8_sequence'
        if row['event'] == 'receive':
            row['rcl_take_receipt']['message_type'] = ARRAY
    return rows, pair


def test_array_deliveries_bind_full_payload_to_native_publish_and_single_take():
    rows, pair = array_capture()
    original = deepcopy(rows)
    samples = [r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r0']
    for sample, sequence in zip(samples, [0, 2, 4]):
        witness, reason = payload_delivery_witness(pair, sample, rows)
        assert witness is not None, reason
        assert witness['send_sequence'] == sequence
        assert witness['identity_basis'] == 'direct_gid_scoped_publish_occurrence'
        assert rows[witness['send_ref']]['payload_text'] == sample['payload_text']
        assert sample['string_data'] is None
        assert sample['send_sequence'] == 0  # Repeated data cannot pick the occurrence.
    fact = pair_delivery_fact(pair, rows, active_writers_on_topic=1)
    assert fact['received_sequences'] == [0, 2, 4]
    assert fact['missing_sequences'] == []
    assert fact['duplicate_sequences'] == []
    assert fact['in_order'] is True
    assert rows == original


@pytest.mark.parametrize('fault', [
    'take_type', 'encoding', 'text_missing', 'string_impostor', 'payload_hash',
    'receipt_hash', 'receipt_status', 'trace_handle', 'unsupported_type',
])
def test_bad_array_occurrence_remains_unknown_without_direct_gid_fallback(fault):
    rows, pair = array_capture()
    sample = next(r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r0')
    send = next(r for r in rows if r['event'] == 'send')
    if fault == 'take_type':
        sample['rcl_take_receipt']['message_type'] = 'std_msgs/msg/String'
    elif fault == 'encoding':
        sample['payload_encoding'] = 'utf8_json_string'
    elif fault == 'text_missing':
        sample.pop('payload_text')
    elif fault == 'string_impostor':
        sample['string_data'] = sample['payload_text']
    elif fault == 'payload_hash':
        sample['payload_sha256'] = 'f' * 64
    elif fault == 'receipt_hash':
        send['publish_occurrence_receipt']['payload_sha256'] = 'f' * 64
    elif fault == 'receipt_status':
        send['publish_occurrence_receipt']['status'] = 'unavailable'
    elif fault == 'trace_handle':
        next(r for r in rows if r['event'] == 'rmw_publish_trace')['publisher_rmw_handle'] += 1
    else:
        sample['message_type'] = sample['rcl_take_receipt']['message_type'] = 'fake/msg/Array'
    witness, reason = payload_delivery_witness(pair, sample, rows)
    assert witness is None
    assert reason
    fact = pair_delivery_fact(pair, rows, active_writers_on_topic=1)
    assert 0 not in fact['received_sequences']
    assert fact['in_order'] is None
