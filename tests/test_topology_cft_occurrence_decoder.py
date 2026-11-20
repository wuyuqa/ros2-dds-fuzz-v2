"""Authored offline raw contracts, not DDS executions or retro-fixed run evidence."""
from copy import deepcopy
import hashlib
import json

import pytest

from test_topology_cft_incarnation_effect import api_trace
from test_topology_publish_occurrence import native_window
from r2f.runtime.topology_pair import _delivery_window_evidence, _dynamic_cft_effect


def _reclock(rows):
    for i, row in enumerate(rows):
        row.update(event_index=i, monotonic_ns=10000 + 100 * i,
                   raw_event_ref=f'offline://cft-occurrence/{i}')
        if row['event'] == 'content_filter_api_call':
            row['api_monotonic_ns'] = row['monotonic_ns']
        if row.get('rcl_take_receipt'):
            row['rcl_take_receipt'].update(take_begin_monotonic_ns=row['monotonic_ns'] - 1,
                                          take_return_monotonic_ns=row['monotonic_ns'])
    for send in (r for r in rows if r['event'] == 'send'):
        receipt = send['publish_occurrence_receipt']
        scope = [r for r in rows if r.get('call_id') == receipt['call_id']]
        receipt.update(call_begin_monotonic_ns=scope[0]['monotonic_ns'],
                       trace_monotonic_ns=scope[1]['monotonic_ns'],
                       call_return_monotonic_ns=scope[2]['monotonic_ns'])
    worker = next(r for r in rows if r['event'] == 'content_filter_dynamic_update')
    worker.update(update_started_ns=next(r['monotonic_ns'] for r in rows
        if r['event'] == 'endpoint_identity_snapshot' and r['phase'] == 'before'),
        update_finished_ns=worker['monotonic_ns'])


def _reclose(rows):
    for close in rows:
        if close['event'] == 'topology_delivery_window_closed':
            close.update(_delivery_window_evidence(rows[close['open_ref']], rows,
                                                   before_index=close['event_index']))


def contrast_rows():
    template, opened, _ = native_window()
    origins = template[:3]
    writer, reader, _ = origins
    for origin in origins:
        origin['qos'] = {'reliability': 'reliable'}
    old, new = reader['endpoint_gid'], '02' * 16
    snapshots = template[3:11]
    burst = [r for r in template if r['event'] in
             ('publish_call_begin', 'rmw_publish_trace', 'publish_call_end', 'send', 'receive')]
    # Author bodies before consumption: original and actual repeat retain body
    # seq=0, while the distinct accepted sentinel has its own complete body.
    for row in burst:
        sequence = (row['send_sequence'] if row['event'] != 'receive' else
                    {7001: 0, 9003: 2, 12009: 4}[
                        row['message_info_receipt']['fields']['source_timestamp']['value']])
        payload = 'sentinel' if sequence == 4 else 'same'
        body = dict(run_id=writer['run_id'], writer_id=writer['logical_id'],
            writer_generation=writer['generation'], endpoint_gid=writer['endpoint_gid'],
            send_sequence=4 if sequence == 4 else 0, declared_payload=payload,
            declared_payload_sha256=hashlib.sha256(payload.encode()).hexdigest())
        text = json.dumps(body, sort_keys=True)
        row['payload_sha256'] = hashlib.sha256(text.encode()).hexdigest()
        if row['event'] in ('send', 'receive'):
            row['string_data'] = text
        if row['event'] == 'send':
            row['publish_occurrence_receipt']['payload_sha256'] = row['payload_sha256']
        if row['event'] == 'receive':
            row['send_sequence'] = body['send_sequence']
    first = [r for r in burst if r.get('command_id') == 10 or
             (r['event'] == 'receive' and
              r['message_info_receipt']['fields']['source_timestamp']['value'] == 7001)]
    rest = [r for r in burst if r not in first and not (
        r['event'] == 'receive' and r['logical_id'] == 'r0' and
        r['message_info_receipt']['fields']['source_timestamp']['value'] == 9003)]
    for row in rest:
        if row['event'] == 'receive' and row['logical_id'] == 'r0':
            row.update(reader_gid=new, endpoint_gid=new)

    protocol = api_trace()[1:]
    observed = dict(expression='data LIKE %0', parameters=["'%sentinel%'"])
    requested = dict(observed, enabled=True)
    receipt = dict(api_status='succeeded', readback_status='matched', applied=True,
                   requested=requested, observed=observed)
    for row in protocol:
        for key, value in reader.items():
            if key not in ('event', 'event_index', 'monotonic_ns', 'endpoint_gid', 'raw_event_ref'):
                row[key] = deepcopy(value)
        row.update(fixture=True, backend='offline_fake')
        for member in row.get('subscriptions', []):
            member.update(node_name=reader['node_name'], topic_type=reader['message_type'])
        if row['event'] == 'content_filter_api_call':
            if row['api_stage'] == 'set_begin':
                row['requested'] = deepcopy(requested)
            if row['api_stage'] == 'get_success':
                row['observed'] = deepcopy(observed)
                row['raw_readback']['fields'].update(filter_expression=observed['expression'],
                    expression_parameters=observed['parameters'])
        if row['event'] == 'content_filter_dynamic_update':
            row['filter_receipt'] = deepcopy(receipt)
        if row['event'] == 'topology_filter_update_result':
            row['receipt'] = deepcopy(receipt)

    pre_open = deepcopy(opened)
    pre_request = dict(event='topology_delivery_window_publish_requested', action_id='before',
                       requested_sequences=[0])
    pre_close = dict(event='topology_delivery_window_closed',
                    **{k: opened[k] for k in ('writer', 'reader', 'writer_generation', 'reader_generation')})
    post_snapshots = deepcopy(snapshots)
    for row in post_snapshots:
        row['command_id'] = 'post-' + row['command_id']
        if row.get('logical_id') == 'r0':
            row['endpoint_gid'] = new
        for member in row.get('subscriptions', []):
            if member['endpoint_gid'] == old:
                member['endpoint_gid'] = new
        if row.get('matched_peer_gids'):
            row['matched_peer_gids'] = [new if gid == old else gid for gid in row['matched_peer_gids']]
    post_open = dict(deepcopy(opened), reader_gid=new)
    post_request = dict(event='topology_delivery_window_publish_requested', action_id='after',
                        requested_sequences=[2, 4])
    post_close = deepcopy(pre_close)
    for row in first + rest:
        if row['event'] == 'send':
            row['phase'] = 'before' if row in first else 'after'
    rows = [*origins, *snapshots, pre_open, pre_request, *first, pre_close,
            *protocol, *post_snapshots, post_open, post_request, *rest, post_close]
    _reclock(rows)
    for opening, request, close, block in (
            (pre_open, pre_request, pre_close, snapshots),
            (post_open, post_request, post_close, post_snapshots)):
        opening['snapshot_start_ref'] = block[0]['event_index']
        opening['snapshot_refs'] = {eid: [r['event_index'] for r in block if r['logical_id'] == eid]
                                    for eid in ('w0', 'r0')}
        request['open_ref'] = close['open_ref'] = opening['event_index']
    _reclose(rows)
    return rows


def effect(rows):
    update = next(r for r in rows if r['event'] == 'topology_filter_update_result')
    return _dynamic_cft_effect(update, rows, real_ros=False)


def test_complete_repeat_body_uses_native_occurrence_not_embedded_sequence():
    rows = contrast_rows()
    original = deepcopy(rows)
    closes = [r for r in rows if r['event'] == 'topology_delivery_window_closed']
    assert all(r['window_complete'] for r in closes), closes
    assert closes[0]['received_sequences'] == [0]
    assert closes[1]['received_sequences'] == [4] and closes[1]['missing_sequences'] == [2]
    sends = [r for r in rows if r['event'] == 'send']
    assert sends[0]['string_data'] == sends[1]['string_data']
    assert json.loads(sends[1]['string_data'])['send_sequence'] == 0
    assert sends[1]['send_sequence'] == 2
    value = effect(rows)
    assert value['status'] == 'delivery_contrast', value
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False
    assert value['missing_send_refs'] == [sends[1]['event_index']]
    assert rows == original


def test_positive_control_uses_witness_send_ref_when_pre_body_seq_also_differs():
    rows = contrast_rows()
    mapping = {0: 10, 2: 20, 4: 30}
    for row in rows:
        if row['event'] in ('publish_call_begin', 'rmw_publish_trace', 'publish_call_end', 'send'):
            row['send_sequence'] = mapping[row['send_sequence']]
            if row.get('publish_occurrence_receipt'):
                row['publish_occurrence_receipt']['send_sequence'] = row['send_sequence']
        if row['event'] == 'topology_delivery_window_publish_requested':
            row['requested_sequences'] = [mapping[s] for s in row['requested_sequences']]
    _reclose(rows)
    value = effect(rows)
    assert value['status'] == 'delivery_contrast', value
    control = rows[value['positive_control_send_refs'][0]]
    assert control['send_sequence'] == 10 and json.loads(control['string_data'])['send_sequence'] == 0
    assert value['control_equivalence'] == 'complete_String.data'
    assert [w['send_sequence'] for w in value['publish_occurrence_witnesses']] == [10, 20, 30]
    assert [rows[w['send_ref']]['send_sequence'] for w in value['sample_delivery_witnesses']] == [10, 30]
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False


@pytest.mark.parametrize('fault', ['missing_receipt', 'unknown_receipt', 'bad_schema',
    'trace_missing', 'trace_reused', 'trace_wrong_command', 'trace_wrong_writer',
    'trace_wrong_generation', 'trace_wrong_clock', 'trace_wrong_handle',
    'duplicate_timestamp', 'wrong_body_writer', 'wrong_body_generation',
    'bad_hash', 'failed_write', 'wrong_control_pubseq', 'no_direct_gid',
    'unsupported_timestamp', 'unmatched', 'unreliable', 'missing_sentinel'])
def test_native_decoder_never_downgrades_bad_scope_or_identity(fault):
    rows = contrast_rows()
    sends = [r for r in rows if r['event'] == 'send']
    missing = sends[1]
    receipt = missing['publish_occurrence_receipt']
    trace = next(r for r in rows if r['event'] == 'rmw_publish_trace' and r['command_id'] == 12)
    if fault == 'missing_receipt':
        missing.pop('publish_occurrence_receipt')
    elif fault == 'unknown_receipt':
        receipt['status'] = 'unknown'
    elif fault == 'bad_schema':
        receipt['schema'] = 'other'
    elif fault == 'trace_missing':
        trace['event'] = 'offline_removed_trace'
    elif fault == 'trace_reused':
        receipt['trace_ref'] = sends[0]['publish_occurrence_receipt']['trace_ref']
    elif fault == 'trace_wrong_command':
        trace['command_id'] = 999
    elif fault == 'trace_wrong_writer':
        trace['logical_id'] = 'w2'
    elif fault == 'trace_wrong_generation':
        trace['generation'] = 2
    elif fault == 'trace_wrong_clock':
        trace['monotonic_clock_identity'] = 'foreign'
    elif fault == 'trace_wrong_handle':
        trace['publisher_rmw_handle'] = 999
    elif fault == 'duplicate_timestamp':
        receipt['source_timestamp'] = sends[0]['publish_occurrence_receipt']['source_timestamp']
    elif fault in ('wrong_body_writer', 'wrong_body_generation'):
        body = json.loads(missing['string_data'])
        body['writer_id' if fault == 'wrong_body_writer' else 'writer_generation'] = (
            'w2' if fault == 'wrong_body_writer' else 2)
        missing['string_data'] = json.dumps(body, sort_keys=True)
        digest = hashlib.sha256(missing['string_data'].encode()).hexdigest()
        missing['payload_sha256'] = receipt['payload_sha256'] = digest
        for row in rows:
            if row.get('command_id') == 12 and row['event'] != 'send':
                row['payload_sha256'] = digest
    elif fault == 'bad_hash':
        missing['payload_sha256'] = 'f' * 64
    elif fault == 'failed_write':
        missing['publish_returned'] = False
    elif fault == 'wrong_control_pubseq':
        row = next(r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r1'
                   and r['message_info_receipt']['fields']['source_timestamp']['value'] == 12009)
        row['message_info_receipt']['fields']['publication_sequence_number']['value'] = 999
        row['rcl_take_receipt']['message_info_receipt'] = deepcopy(row['message_info_receipt'])
    elif fault in ('no_direct_gid', 'unsupported_timestamp', 'missing_sentinel'):
        row = next(r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r0'
                   and r['reader_gid'] == '02' * 16)
        if fault == 'no_direct_gid':
            row['publisher_gid'] = None
        elif fault == 'unsupported_timestamp':
            row['message_info_receipt']['fields']['source_timestamp'].update(status='unsupported_rmw', value=None)
            row['rcl_take_receipt']['message_info_receipt'] = deepcopy(row['message_info_receipt'])
        else:
            row['event'] = 'offline_removed_receive'
    elif fault == 'unmatched':
        for row in rows:
            if row['event'] == 'match_snapshot':
                row.pop('matched_peer_gids', None)
                row.pop('peer_proof_source', None)
                row.pop('peer_gids_complete', None)
    else:
        rows[0]['qos']['reliability'] = 'best_effort'
    _reclose(rows)
    value = effect(rows)
    assert value['status'] == 'unknown', value
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False
    if fault == 'trace_missing':
        # The missing middle delivery has no callback to validate its trace;
        # decoding MUST independently validate this send, even with closed gaps.
        assert all(r['window_complete'] for r in rows if r['event'] == 'topology_delivery_window_closed')
        assert value['reason'].startswith('sample_publish_occurrence_invalid:'), value


def test_duplicate_current_reader_occurrence_is_not_a_second_control():
    rows = contrast_rows()
    duplicate = deepcopy(next(r for r in rows if r['event'] == 'receive'
                              and r['logical_id'] == 'r0' and r['reader_gid'] == '02' * 16))
    duplicate['rcl_take_receipt']['take_index'] = 999
    rows.insert(len(rows) - 1, duplicate)
    _reclock(rows)
    _reclose(rows)
    assert all(r['window_complete'] for r in rows if r['event'] == 'topology_delivery_window_closed')
    value = effect(rows)
    assert value['status'] == 'unknown' and value['reason'] == 'receive_occurrence_duplicated', value


def _later_repeat(rows):
    original_stamp = 7001
    copied = deepcopy([r for r in rows if (r['event'] in (
        'publish_call_begin', 'rmw_publish_trace', 'publish_call_end', 'send') and r.get('command_id') == 10)
        or (r['event'] == 'receive'
            and r['message_info_receipt']['fields']['source_timestamp']['value'] == original_stamp)])
    for row in copied:
        if row['event'] != 'receive':
            row.update(command_id=100, send_sequence=6)
            if 'call_id' in row:
                row['call_id'] = 'publish-later'
            if row['event'] == 'rmw_publish_trace':
                row.update(source_timestamp=77777, trace_ref='publish-later.trace.0')
            if row['event'] == 'send':
                row['publish_occurrence_receipt'].update(command_id=100, send_sequence=6,
                    call_id='publish-later', source_timestamp=77777, trace_ref='publish-later.trace.0')
        else:
            row['message_info_receipt']['fields']['source_timestamp']['value'] = 77777
            row['message_info_receipt']['fields']['publication_sequence_number']['value'] = 109
            row['rcl_take_receipt'].update(take_index=999,
                message_info_receipt=deepcopy(row['message_info_receipt']))
            if row['logical_id'] == 'r0':
                row.update(reader_gid='02' * 16, endpoint_gid='02' * 16)
    rows.extend(copied)
    _reclock(rows)
    _reclose(rows)


def test_later_identical_body_receive_is_not_retroactively_the_missing_occurrence():
    rows = contrast_rows()
    _later_repeat(rows)
    value = effect(rows)
    assert value['status'] == 'delivery_contrast', value
    late = next(r for r in reversed(rows) if r['event'] == 'send')
    assert late['send_sequence'] == 6 and json.loads(late['string_data'])['send_sequence'] == 0
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False


def test_true_late_missing_occurrence_still_invalidates_window_contrast():
    rows = contrast_rows()
    control = next(r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r1'
                   and r['message_info_receipt']['fields']['source_timestamp']['value'] == 9003)
    reader = rows[1]
    late = {**deepcopy(control), **{key: deepcopy(reader[key]) for key in (
        'logical_id', 'generation', 'pid', 'pgid', 'endpoint_generation', 'process_start_ticks',
        'role', 'node_name')}, 'reader_gid': '02' * 16, 'endpoint_gid': '02' * 16}
    late['rcl_take_receipt'].update(bridge_id='offline-bridge-r0', take_index=999)
    rows.append(late)
    _reclock(rows)
    _reclose(rows)
    value = effect(rows)
    assert value['status'] == 'unknown' and value['reason'] == 'late_receive_after_window', value


def test_bad_later_scope_is_unknown_not_assumed_distinct_by_body_seq():
    rows = contrast_rows()
    _later_repeat(rows)
    late = next(r for r in reversed(rows) if r['event'] == 'send')
    late['publish_occurrence_receipt']['status'] = 'unknown'
    value = effect(rows)
    assert value['status'] == 'unknown', value
    assert value['reason'].startswith('late_receive_occurrence_unknown:'), value
