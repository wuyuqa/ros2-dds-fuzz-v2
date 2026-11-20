"""Offline cross-GID raw contracts, never DDS evidence or SUT candidates.

Only freshly generated offline fixture rows are transformed in memory. The raw
file produced by run_contrast is retained byte-for-byte, not retroactively fixed.
"""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from test_endpoint_incarnation import trace, OLD, NEW
from test_topology_dynamic_cft import run_contrast
from test_topology_incarnation_delivery import payload_delivery_witness
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.topology_pair import _delivery_window_evidence, _dynamic_cft_effect


def _reindex(rows):
    """Remap raw references after splicing, then give this fixture one clock."""
    mapping = {r['event_index']: i for i, r in enumerate(rows)
               if r.get('event_index') is not None}

    def refs(value, key=''):
        if isinstance(value, dict):
            return {k: refs(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [refs(v, key) for v in value]
        if type(value) is int and (key.endswith('_ref') or key.endswith('_refs')
                                  or key in ('r0', 'w0')):
            return mapping[value]
        return value

    for i, row in enumerate(rows):
        updated = refs(row)
        row.clear()
        row.update(updated, event_index=i, monotonic_ns=1000 + i, time_ns=2000 + i)
        if row['event'] == 'content_filter_api_call':
            row['api_monotonic_ns'] = row['monotonic_ns']
    worker = next(r for r in rows if r['event'] == 'content_filter_dynamic_update')
    before = next(r for r in rows if r['event'] == 'endpoint_identity_snapshot'
                  and r['phase'] == 'before')
    worker.update(update_started_ns=before['monotonic_ns'],
                  update_finished_ns=worker['monotonic_ns'])


def _close_windows(rows):
    for close in rows:
        if close['event'] == 'topology_delivery_window_closed':
            close.update(_delivery_window_evidence(rows[close['open_ref']], rows,
                                                    before_index=close['event_index']))


@pytest.fixture
def contrast(tmp_path):
    result = run_contrast(tmp_path)
    assert not result['real_execution'] and not result['candidate_eligible']
    path = Path(result['raw_path'])
    persisted = path.read_bytes()
    rows = [json.loads(line) for line in persisted.splitlines()]
    origin = next(r for r in rows if r['event'] == 'endpoint_created'
                  and r['logical_id'] == 'r0')
    old = origin['endpoint_gid']
    origin.update(node_name='offline_reader', rmw_identifier='offline_fixture')
    worker = next(r for r in rows if r['event'] == 'content_filter_dynamic_update')
    update = next(r for r in rows if r['event'] == 'topology_filter_update_result')
    command = worker['command_id']
    first = next(i for i, r in enumerate(rows) if r['event'] == 'content_filter_api_call')
    last = rows.index(worker)
    calls = deepcopy(rows[first:last])
    # Reuse the independently tested incarnation protocol, not a second creation.
    protocol = trace()[1:]
    for row in protocol:
        row.pop('event_index')
        row['command_id'] = command
        for key in ('endpoint_gid', 'reader_gid', 'before_gid', 'after_gid'):
            if key in row:
                row[key] = {OLD: old, NEW: NEW}[row[key]]
        row['topic'] = origin['topic']
        for member in row.get('subscriptions', []):
            member.update(node_name=origin['node_name'], topic_type=origin['message_type'],
                          endpoint_gid=old if member['endpoint_gid'] == OLD else NEW)
    before, _, _, after, _, transition, ack = protocol
    worker.update(reader_gid=NEW, before_gid=old, after_gid=NEW)
    update['reader_gid'] = NEW
    # Snapshot and delivery observations after the authenticated update see NEW.
    def replace_gid(value):
        if isinstance(value, dict):
            return {k: replace_gid(v) for k, v in value.items()}
        if isinstance(value, list):
            return [replace_gid(v) for v in value]
        return NEW if value == old else value
    rows[last + 1:] = [replace_gid(r) for r in rows[last + 1:]]
    rows[first:last + 1] = [before, *calls, after, worker, transition, ack]
    binding = ('logical_id', 'generation', 'pid', 'pgid', 'process_start_ticks',
               'endpoint_generation', 'run_id', 'domain_id', 'node_name',
               'rmw_identifier', 'monotonic_clock_identity', 'role', 'worker_role')
    ack_position = rows.index(ack)
    for position, row in enumerate(rows):
        if row.get('logical_id') == 'r0':
            row.update({k: deepcopy(origin[k]) for k in binding})
            if row['event'] == 'topology_snapshot_requested':
                row['endpoint_gid'] = (NEW if position > ack_position else old)
    _reindex(rows)
    _close_windows(rows)
    yield rows, old
    assert path.read_bytes() == persisted, 'persisted offline raw must not be rewritten'


def _effect(rows):
    update = next(r for r in rows if r['event'] == 'topology_filter_update_result')
    return _dynamic_cft_effect(update, rows, real_ros=False)


def test_cross_gid_contrast_is_not_filter_effect_or_candidate(contrast):
    rows, old = contrast
    value = _effect(rows)
    assert value['status'] == 'delivery_contrast', value
    assert value['filter_effect_observed'] is None
    assert value['candidate_eligible'] is False
    assert value['fixture_contrast_observed'] is True
    identity = value['reader_identity']
    assert (identity['before_gid'], identity['after_gid']) == (old, NEW)
    assert (identity['incarnation_before'], identity['incarnation_after']) == (0, 1)
    assert rows[identity['transition_ref']]['event'] == 'endpoint_identity_transition'
    assert rows[identity['ack_ref']]['command'] == 'update_filter'
    created = [r for r in rows if r['event'] == 'endpoint_created' and r['logical_id'] == 'r0']
    assert len(created) == 1 and created[0]['endpoint_gid'] == old
    assert endpoint_identity_at(rows, created[0])['endpoint_gid'] == NEW
    for key, gid in [('pre_window_ref', old), ('post_window_ref', NEW)]:
        close = rows[value[key]]
        opened = rows[close['open_ref']]
        assert opened['reader_gid'] == gid
        proof = _delivery_window_evidence(opened, rows, before_index=close['event_index'])
        assert proof['window_complete'], proof
        assert proof['receive_refs'] == close['receive_refs']
        for ref in proof['receive_refs']:
            receive = rows[ref]
            assert receive['reader_gid'] == gid
            witness, reason = payload_delivery_witness(opened, receive, rows)
            assert witness is not None, reason


@pytest.mark.parametrize('fault', ['old_callback', 'no_ack', 'pre_wrong_gid',
                                  'post_wrong_gid', 'pre_new_callback', 'intervening_update'])
def test_cross_gid_contrast_fails_closed(contrast, fault):
    rows, old = contrast
    if fault in ('old_callback', 'pre_new_callback'):
        receives = [r for r in rows if r['event'] == 'receive']
        receive = receives[-1] if fault == 'old_callback' else receives[0]
        receive['reader_gid'] = old if fault == 'old_callback' else NEW
    elif fault == 'no_ack':
        next(r for r in rows if r['event'] == 'command_done'
             and r.get('command') == 'update_filter')['event'] = 'offline_removed_ack'
    elif fault in ('pre_wrong_gid', 'post_wrong_gid'):
        openings = [r for r in rows if r['event'] == 'topology_delivery_window_open']
        openings[0 if fault == 'pre_wrong_gid' else 1]['reader_gid'] = (
            NEW if fault == 'pre_wrong_gid' else old)
    else:
        # A distinct update inside the contrast interval, not the target command.
        row = next(r for r in rows if r['event'] == 'topology_action_done'
                   and r['event_index'] > next(x['event_index'] for x in rows
                       if x['event'] == 'topology_filter_update_result'))
        worker = next(r for r in rows if r['event'] == 'content_filter_dynamic_update')
        index, clock = row['event_index'], row['monotonic_ns']
        row.clear()
        row.update(deepcopy(worker), event_index=index, monotonic_ns=clock,
                   command_id=999)
    _close_windows(rows)
    value = _effect(rows)
    assert value['status'] == 'unknown', value
    assert value['filter_effect_observed'] is None and not value['candidate_eligible']
    if fault in ('old_callback', 'pre_new_callback'):
        assert value['reason'] == 'window_not_raw_closed', value
    if fault == 'no_ack':
        assert value['reason'].startswith('update_incarnation_evidence_invalid:'), value
    if fault.endswith('wrong_gid'):
        assert value['reason'] == 'window_reader_incarnation_mismatch', value
    if fault == 'intervening_update':
        assert value['reason'] == 'intervening_update', value


def test_offline_cross_gid_fixture_cannot_be_promoted_to_dds(contrast):
    rows, _ = contrast
    update = next(r for r in rows if r['event'] == 'topology_filter_update_result')
    value = _dynamic_cft_effect(update, rows, real_ros=True)
    assert value['reason'] == 'fixture_cannot_qualify_real_effect'
    assert value['filter_effect_observed'] is None and not value['candidate_eligible']
