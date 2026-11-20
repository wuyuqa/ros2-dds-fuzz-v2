"""Integration consumers use native-scope fixture, never raw body sequence guesses."""
from copy import deepcopy
import pytest

from test_publish_occurrence import fixture
from r2f.runtime.topology_pair import (
    payload_delivery_witness, pair_delivery_fact, _repeat_data_observation,
)


def prepared():
    rows, pair = fixture()
    for token in rows[:3]:
        if token['logical_id'] == 'r1':
            token['initial_content_filter'] = None
        elif token['logical_id'] == 'r0':
            token['initial_content_filter'] = {'scope': 'initial_only', 'configured': True}
    return rows, pair


def test_payload_witness_dispatches_to_real_scope_not_same_body_uniqueness():
    rows, pair = prepared()
    samples = [e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0']
    for sample, sequence in zip(samples, [0, 2, 4]):
        value, reason = payload_delivery_witness(pair, sample, rows)
        assert value is not None, reason
        assert value['send_sequence'] == sequence
        assert value['identity_basis'] == 'direct_gid_scoped_publish_occurrence'
        assert sample['send_sequence'] == 0


def test_supporting_delivery_fact_preserves_raw_body_and_maps_native_occurrence():
    rows, pair = prepared()
    original = deepcopy(rows)
    fact = pair_delivery_fact(pair, rows, active_writers_on_topic=1)
    assert fact['expected_sequences'] == [0, 2, 4]
    assert fact['received_sequences'] == [0, 2, 4]
    assert fact['duplicate_sequences'] == []
    assert fact['missing_sequences'] == []
    assert fact['in_order'] is True
    assert len(fact['payload_witnesses']) == 3
    assert rows == original


def test_unfiltered_control_is_bound_from_explicit_raw_configuration_not_name():
    rows, pair = prepared()
    pair = {k: v for k, v in pair.items() if not k.startswith('control')}
    sample = next(e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0')
    value, reason = payload_delivery_witness(pair, sample, rows)
    assert value is not None, reason
    assert value['control_identity']['logical_id'] == 'r1'


@pytest.mark.parametrize('change', ['missing_config', 'filtered', 'updated', 'ambiguous'])
def test_no_unfiltered_control_is_inferred_from_reader_order(change):
    rows, pair = prepared()
    pair = {k: v for k, v in pair.items() if not k.startswith('control')}
    if change == 'missing_config': rows[2].pop('initial_content_filter')
    elif change == 'filtered': rows[2]['initial_content_filter'] = {'configured': True}
    elif change == 'updated':
        row = dict(rows[2], event='content_filter_dynamic_update', event_index=len(rows))
        rows.append(row)
    else: rows[1]['initial_content_filter'] = None
    sample = next(e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0')
    value, reason = payload_delivery_witness(pair, sample, rows)
    assert value is None
    assert reason is not None


def test_native_receipt_failure_is_not_downgraded_to_direct_gid_body_seq():
    rows, pair = prepared()
    trace = next(e for e in rows if e['event'] == 'rmw_publish_trace')
    trace['publisher_rmw_handle'] += 1
    sample = next(e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0')
    assert payload_delivery_witness(pair, sample, rows)[0] is None
    fact = pair_delivery_fact(pair, rows, active_writers_on_topic=1)
    assert fact['received_sequences'] == [2, 4]
    assert fact['in_order'] is None


def bound_repeat():
    rows, pair = prepared()
    sends = [e for e in rows if e['event'] == 'send']
    for send, phase in zip(sends, ['original', 'repeat_rejected', 'repeat_restored']):
        send.update(phase=phase, publish_begin_ns=send['publish_occurrence_receipt']['call_begin_monotonic_ns'],
                    publish_return_ns=send['publish_occurrence_receipt']['call_return_monotonic_ns'])
    first, sent = sends[:2]
    binding = dict(event='topology_repeat_data_binding', event_index=len(rows),
                   writer=pair['writer'], writer_generation=pair['writer_generation'],
                   writer_gid=pair['writer_gid'], source_send_ref=first['event_index'],
                   source_action='original', action_id=sent['phase'], command_id=sent['command_id'],
                   occurrence_sequence=sent['send_sequence'])
    rows.append(binding)
    return rows, binding


def test_repeat_observation_maps_control_pubseq_without_claiming_filter_effect():
    rows, binding = bound_repeat()
    result = _repeat_data_observation(binding, rows)
    assert result['status'] == 'identical_input_occurrences_observed', result
    mapping = result['occurrence_mapping']
    assert mapping['source_control_witness']['publication_sequence_number'] == 41
    assert mapping['repeat_control_witness']['publication_sequence_number'] == 88
    assert result['input_equivalence'] == 'complete_String.data'
    assert result['filter_effect_observed'] is None
    assert result['candidate_eligible'] is False


def test_repeat_observation_missing_control_is_not_mapped_by_producer_alone():
    rows, binding = bound_repeat()
    rows[2].pop('initial_content_filter')
    result = _repeat_data_observation(binding, rows)
    assert result['status'] == 'identical_input_sent'
    assert result['occurrence_mapping'] is None
    assert result['reason'] == 'unfiltered_control_identity_missing_or_ambiguous'
    assert result['candidate_eligible'] is False


def native_window(*, direct_peer_proof=True, omit_middle=False):
    """Author offline raw before consumption; no persisted run/raw is modified."""
    rows, pair = prepared()
    for row in rows:
        row['monotonic_ns'] *= 100
        receipt = row.get('publish_occurrence_receipt')
        if receipt:
            for key in ('call_begin_monotonic_ns', 'trace_monotonic_ns', 'call_return_monotonic_ns'):
                receipt[key] *= 100
        take = row.get('rcl_take_receipt')
        if take:
            take['take_begin_monotonic_ns'] *= 100
            take['take_return_monotonic_ns'] *= 100
        if row['event'] == 'send': row['phase'] = 'burst'
    writer, reader, control = rows[:3]
    block = []
    snapshots = {}
    graph = dict(publishers=[dict(endpoint_gid=writer['endpoint_gid'], topic_type=writer['message_type'])],
                 subscriptions=[dict(endpoint_gid=t['endpoint_gid'], topic_type=t['message_type'])
                                for t in (reader, control)])
    for token, peers in ((writer, [reader['endpoint_gid'], control['endpoint_gid']]),
                          (reader, [writer['endpoint_gid']])):
        snapshots[token['logical_id']] = []
        for kind in ('topology_snapshot_requested', 'match_snapshot', 'graph_snapshot', 'command_done'):
            row = dict(token, event=kind, command_id='fresh-' + token['logical_id'],
                       monotonic_ns=10201 + len(block))
            row.pop('event_index', None)
            if kind == 'topology_snapshot_requested': row['command'] = 'snapshot'
            if kind == 'match_snapshot':
                row.update(matched_count=len(peers))
                if direct_peer_proof:
                    row.update(peer_proof_source='dds_matched_endpoint_api', peer_gids_complete=True,
                               matched_peer_gids=peers)
            if kind == 'graph_snapshot': row.update(deepcopy(graph))
            snapshots[token['logical_id']].append(row)
            block.append(row)
    opened = dict(pair, event='topology_delivery_window_open', run_id=writer['run_id'],
                  monotonic_ns=10209, snapshot_start_ref=3)
    request = dict(event='topology_delivery_window_publish_requested',
                   action_id='burst', requested_sequences=[0, 2, 4], monotonic_ns=10210)
    rest = rows[3:]
    if omit_middle:
        rest = [row for row in rest if not (row['event'] == 'receive'
                and row['logical_id'] == 'r0'
                and row['message_info_receipt']['fields']['publication_sequence_number']['value'] == 88)]
    rows = [*rows[:3], *block, opened, request, *rest]
    close = dict(event='topology_delivery_window_closed', monotonic_ns=rows[-1]['monotonic_ns'] + 100)
    rows.append(close)
    for index, row in enumerate(rows): row['event_index'] = index
    opened['snapshot_refs'] = {key: [r['event_index'] for r in chain] for key, chain in snapshots.items()}
    request['open_ref'] = close['open_ref'] = opened['event_index']
    return rows, opened, close


def test_delivery_window_uses_scoped_sequence_and_final_sentinel_not_body_seq():
    from r2f.runtime.topology_pair import _delivery_window_evidence
    rows, opened, close = native_window()
    proof = _delivery_window_evidence(opened, rows, before_index=close['event_index'])
    assert proof['window_complete'], proof
    assert proof['received_sequences'] == [0, 2, 4]
    assert proof['missing_sequences'] == []
    assert proof['attribution'] == 'direct_publish_occurrence'


def test_native_window_occurrence_does_not_bypass_direct_match_proof():
    from r2f.runtime.topology_pair import _delivery_window_evidence
    rows, opened, close = native_window(direct_peer_proof=False)
    proof = _delivery_window_evidence(opened, rows, before_index=close['event_index'])
    assert not proof['window_complete']
    assert proof['received_sequences'] == [0, 2, 4]
    assert 'snapshot_scope_pair_not_matched' in proof['reasons']


def test_native_missing_middle_closes_only_with_later_same_writer_sentinel():
    from r2f.runtime.topology_pair import _delivery_window_evidence
    rows, opened, close = native_window(omit_middle=True)
    proof = _delivery_window_evidence(opened, rows, before_index=close['event_index'])
    assert proof['window_complete'], proof
    assert proof['missing_sequences'] == [2]
    assert proof['sentinel_sequence'] == 4
    assert proof['closure_status'] == 'closed_with_gap'
    assert proof['gap_evidence'][0]['sequence'] == 2


def one_native_window():
    rows, opened, close = native_window()
    rows = [row for row in rows if row.get('call_id') not in ('publish-2', 'publish-4')
            and not (row.get('event') == 'send' and row.get('send_sequence') in (2, 4))
            and not (row.get('event') == 'receive'
                     and row['message_info_receipt']['fields']['source_timestamp']['value'] != 7001)]
    for row in rows:
        if row['event'] == 'topology_delivery_window_publish_requested': row['requested_sequences'] = [0]
    for index, row in enumerate(rows): row['event_index'] = index
    return rows, opened, close


def test_wrong_send_hash_cannot_bypass_stronger_contract_in_supporting_fact():
    rows, opened, close = one_native_window()
    send = next(e for e in rows if e['event'] == 'send')
    send['payload_sha256'] = 'f' * 64
    fact = pair_delivery_fact(opened, rows, active_writers_on_topic=1)
    assert fact['received_sequences'] == []
    assert fact['unknown_attribution_receipts'] == 1


@pytest.mark.parametrize('change', ['hash', 'unknown_status', 'bad_schema', 'missing_receipt'])
def test_native_contract_admission_does_not_depend_on_fields_it_must_validate(change):
    from r2f.runtime.topology_pair import _delivery_window_evidence
    rows, opened, close = one_native_window()
    send = next(e for e in rows if e['event'] == 'send')
    if change == 'hash': send['payload_sha256'] = 'f' * 64
    elif change == 'unknown_status': send['publish_occurrence_receipt']['status'] = 'unknown'
    elif change == 'bad_schema': send['publish_occurrence_receipt']['schema'] = 'not-the-schema'
    else: send.pop('publish_occurrence_receipt')
    proof = _delivery_window_evidence(opened, rows, before_index=close['event_index'])
    assert not proof['window_complete'], proof
    assert proof['received_sequences'] == []
    assert any('publish_occurrence' in reason for reason in proof['reasons'])


def test_other_direct_writer_is_not_mislabeled_as_current_scope_failure():
    rows, pair = prepared()
    other = deepcopy(next(e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0'))
    other.update(event_index=len(rows), publisher_gid='09' * 15 + '03')
    other['rcl_take_receipt']['take_index'] = 999
    rows.append(other)
    fact = pair_delivery_fact(pair, rows, active_writers_on_topic=1)
    assert fact['received_sequences'] == [0, 2, 4]
    assert fact['unknown_attribution_receipts'] == 0


def test_worker_fallback_capability_merges_duplicate_keys_without_typeerror():
    import ast
    from pathlib import Path
    from r2f.runtime.rcl_take_bridge import BridgeUnavailable
    from r2f.runtime import topology_worker
    tree = ast.parse(Path(topology_worker.__file__).read_text())
    call = next(node for node in ast.walk(tree) if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name) and node.func.id == 'emit'
                and node.args and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == 'topology_message_info_capability')
    results = []
    expr = ast.Expression(body=call)
    eval(compile(ast.fix_missing_locations(expr), '<worker-capability-expression>', 'eval'),
         dict(emit=lambda event, **fields: results.append((event, fields)),
              take_capability=BridgeUnavailable('ABI unavailable').capability_receipt))
    assert results[0][1]['status'] == 'unknown'
    assert results[0][1]['candidate_eligible'] is False


@pytest.mark.parametrize('primary', [True, False])
@pytest.mark.parametrize('telemetry_fails', [True, False])
def test_worker_bridge_cleanup_keeps_primary_and_attempts_all_resources(primary, telemetry_fails):
    from types import SimpleNamespace
    from r2f.runtime.topology_worker import _audit_worker_cleanup
    order = []
    def step(name):
        def action(): order.append(name)
        return action
    def close():
        order.append('bridge'); raise RuntimeError('bridge close error')
    def emit(event, **fields):
        if telemetry_fails: raise OSError('telemetry unavailable')
    executor = SimpleNamespace(shutdown=step('executor'), take_bridge=SimpleNamespace(close=close))
    kwargs = dict(endpoint_creation_returned=True,
                  primary_error=ValueError('original failure') if primary else None)
    if primary:
        result = _audit_worker_cleanup(executor, SimpleNamespace(destroy_node=step('node')),
                                      SimpleNamespace(shutdown=step('context')), emit, **kwargs)
        assert result['message_info_bridge']['error_type'] == 'RuntimeError'
    else:
        with pytest.raises(RuntimeError, match='bridge close error'):
            _audit_worker_cleanup(executor, SimpleNamespace(destroy_node=step('node')),
                                  SimpleNamespace(shutdown=step('context')), emit, **kwargs)
    assert order == ['executor', 'bridge', 'node', 'context']
