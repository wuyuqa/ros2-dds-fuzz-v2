"""Authored window contracts, not DDS evidence or rewritten run artifacts."""
from copy import deepcopy
import hashlib
import json

import pytest

from test_topology_cft_occurrence_decoder import contrast_rows, effect, _reclock, _reclose


def earlier_control_rows():
    rows = contrast_rows()
    original = {e['event_index']: e for e in rows}
    first_close = next(e for e in rows if e['event'] == 'topology_delivery_window_closed')
    middle = deepcopy(rows[3:first_close['event_index'] + 1])
    copies = {e['event_index']: e for e in middle}
    for e in middle:
        if 'command_id' in e:
            e['command_id'] = 101 if isinstance(e['command_id'], int) else 'middle-' + e['command_id']
        if 'call_id' in e:
            e['call_id'] = 'middle-' + e['call_id']
        if e['event'] in ('publish_call_begin', 'rmw_publish_trace', 'publish_call_end', 'send'):
            e['send_sequence'] = 1
        if e['event'] == 'rmw_publish_trace':
            e.update(source_timestamp=8002, trace_ref='middle-trace')
        if 'string_data' in e:
            body = json.loads(e['string_data'])
            body.update(send_sequence=1, declared_payload='unrelated',
                        declared_payload_sha256=hashlib.sha256(b'unrelated').hexdigest())
            e['string_data'] = json.dumps(body, sort_keys=True)
            e['payload_sha256'] = hashlib.sha256(e['string_data'].encode()).hexdigest()
        if e['event'] == 'send':
            e['phase'] = 'unrelated-health'
            r = e['publish_occurrence_receipt']
            r.update(command_id=101, send_sequence=1, call_id='middle-' + r['call_id'],
                     source_timestamp=8002, trace_ref='middle-trace', payload_sha256=e['payload_sha256'])
            for scope in middle:
                if scope['event'] in ('publish_call_begin', 'rmw_publish_trace', 'publish_call_end'):
                    scope['payload_sha256'] = e['payload_sha256']
        if e['event'] == 'receive':
            e['send_sequence'] = 1
            fields = e['message_info_receipt']['fields']
            fields['source_timestamp']['value'] = 8002
            fields['publication_sequence_number']['value'] = 50
            e['rcl_take_receipt'].update(take_index=800,
                                        message_info_receipt=deepcopy(e['message_info_receipt']))
        if e['event'] == 'topology_delivery_window_publish_requested':
            e.update(action_id='unrelated-health', requested_sequences=[1])
    insert = first_close['event_index'] + 1
    rows[insert:insert] = middle
    _reclock(rows)
    for collection, lookup in ((original.values(), original), (middle, copies)):
        for e in collection:
            for key in ('open_ref', 'snapshot_start_ref'):
                if key in e:
                    e[key] = lookup[e[key]]['event_index']
            if 'snapshot_refs' in e:
                e['snapshot_refs'] = {k: [lookup[i]['event_index'] for i in refs]
                                      for k, refs in e['snapshot_refs'].items()}
    _reclose(rows)
    closes = [e for e in rows if e['event'] == 'topology_delivery_window_closed']
    assert all(e['window_complete'] for e in closes), closes
    return rows


def test_exact_earlier_control_survives_unrelated_immediate_health_window():
    rows = earlier_control_rows()
    before = deepcopy(rows)
    value = effect(rows)
    assert value['status'] == 'delivery_contrast', value
    closes = [e for e in rows if e['event'] == 'topology_delivery_window_closed']
    assert value['pre_window_ref'] == closes[1]['event_index']
    assert value['positive_control_window_refs'] == [e['event_index'] for e in closes[:2]]
    assert value['control_equivalence'] == 'complete_String.data'
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False
    assert rows == before


@pytest.mark.parametrize('damage', ['raw_close', 'identity', 'send_hash', 'receive_hash'])
def test_earlier_control_does_not_bypass_raw_identity_or_sample_contracts(damage):
    rows = earlier_control_rows()
    close = next(e for e in rows if e['event'] == 'topology_delivery_window_closed')
    if damage == 'raw_close':
        close['sentinel_send_ref'] = -1
    elif damage == 'identity':
        rows[close['open_ref']]['reader_gid'] = '99' * 16
    elif damage == 'send_hash':
        rows[close['send_refs'][0]]['payload_sha256'] = '0' * 64
    else:
        rows[close['receive_refs'][0]]['payload_sha256'] = '0' * 64
    value = effect(rows)
    assert value['status'] == 'unknown', value
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False
