"""Offline raw action protocols; no ROS, provider or vendor library loaded."""
from copy import deepcopy
from types import SimpleNamespace
import time

import pytest

from test_filter_disabled_readback import NativeShapeSubscription, graph, A
from r2f.runtime.topology_worker import _update_filter_identity, _observe_filter_state
from r2f.runtime.filter_action_receipt import filter_action_evidence
from r2f.campaign.topology_adapter import _Audit


def action_rows(*, padded=False):
    sub = NativeShapeSubscription()
    rows, actions = [], []
    lineage = dict(logical_id='r0', generation=1, endpoint_generation='offline-process',
        run_id='offline-action', rmw_identifier='offline-fixture', role='reader',
        domain_id=191, node_name='reader', message_type='std_msgs/msg/String',
        pid=100, pgid=100, process_start_ticks=1,
        monotonic_clock_identity={'boot_id': 'fixture'}, fixture=True, backend='offline_fake')

    def emit(event, **fields):
        row = dict(lineage, event=event, event_index=len(rows), monotonic_ns=time.monotonic_ns())
        row.update(fields)
        rows.append(row)
        return row

    start = emit('runtime_begin')
    emit('endpoint_created', endpoint_gid=A, topic=sub.topic_name)
    _observe_filter_state(sub, emit, command='create_endpoint', command_id=None, reader_gid=A, phase='initial')
    gid, incarnation = A, 0
    before = dict(alive=True, generation=1, next_sequence=0)
    for i, (aid, expression, parameters) in enumerate([
        ('empty_initial', '', []), ('reject', 'data LIKE %0', ["'%sentinel%'"]), ('restore', '', [])]):
        if padded and expression:
            expression = '  ' + expression + '  '
        action = dict(action_id=aid, op='update_filter', endpoint_id='r0', generation=1,
                      params=dict(expression=expression, parameters=parameters), depends_on=[])
        actions.append(action)
        emit('topology_action_begin', action_id=aid, action_index=i, action=action, before=deepcopy(before))
        gid, incarnation = _update_filter_identity(sub, lambda: graph(sub), 'reader', gid, incarnation,
            dict(command_id=i + 1, expression=expression, parameters=parameters), emit,
            state_observer=_observe_filter_state)
        emit('command_done', command='update_filter', command_id=i + 1,
             endpoint_gid=gid, reader_gid=gid, incarnation_ordinal=incarnation)
        dynamic = next(e for e in reversed(rows) if e['event'] == 'content_filter_dynamic_update')
        result = emit('topology_filter_update_result', action_id=aid, reader_gid=gid,
                      command_id=i + 1, receipt=deepcopy(dynamic['filter_receipt']))
        evidence = filter_action_evidence(action, rows)
        receipt = dict(step_id='action:' + aid, action=action, before=deepcopy(before),
            after={**before, 'filter_update_raw_ref': result['event_index']},
            requires=evidence['requires'], satisfied=True, disposition=evidence['disposition'],
            filter_action_evidence=evidence, execution_evidence=False, filter_effect_observed=None,
            filter_update_receipt=deepcopy(result['receipt']))
        emit('state_step_after', **receipt)
        emit('topology_action_done', action_id=aid, action_index=i, action=action, receipt=receipt)
    end = emit('runtime_end')
    return rows, actions, start, end


def audit_actions(rows, actions, start, end):
    audit = _Audit.__new__(_Audit)
    audit.events, audit.program = rows, {'actions': actions}
    audit.specs = {'r0': {'role': 'reader'}}
    audit.begin, audit.end = start, end
    audit.tokens, audit.action_receipts = {}, []
    audit.path, audit.sha = '/offline/raw', 'a' * 64
    return audit.actions()


def test_all_three_filter_actions_are_raw_proven_without_promoting_api_errors():
    rows, actions, start, end = action_rows()
    before = deepcopy(rows)
    evidence = [filter_action_evidence(a, rows) for a in actions]
    assert [e['outcome_kind'] for e in evidence] == [
        'disabled_state_observed', 'configuration_readback', 'disabled_state_observed']
    assert [e['disposition'] for e in evidence] == ['observed', 'configured', 'observed']
    assert [e['setter_succeeded'] for e in evidence] == [False, True, True]
    assert [e['readback_status'] for e in evidence] == ['not_attempted', 'matched', 'failed']
    assert all(e['filter_effect_observed'] is None and not e['mutation_applied']
               and not e['candidate_eligible'] for e in evidence)
    assert len(audit_actions(rows, actions, start, end)) == 3
    assert rows == before


@pytest.mark.parametrize('damage', ['missing_state', 'state_status', 'nonbool', 'state_gid', 'worker',
    'clock', 'command', 'state_role', 'api_name', 'state_time', 'state_result', 'state_receipt', 'raw_envelope', 'ack',
    'request', 'requested', 'readback', 'controller_context', 'result_receipt', 'interrupted'])
def test_state_action_rejects_unbound_or_incomplete_observations(damage):
    rows, actions, _, _ = action_rows()
    action = actions[1]
    cid = 2
    scoped = [e for e in rows if e.get('command_id') == cid]
    state = next(e for e in scoped if e['event'] == 'content_filter_state_observation')
    end = next(e for e in scoped if e['event'] == 'content_filter_state_api_call' and e['api_stage'] == 'state_success')
    result = next(e for e in scoped if e['event'] == 'topology_filter_update_result')
    if damage == 'missing_state': state['event'] = 'offline_removed_state'
    elif damage == 'state_status': state['filter_receipt']['status'] = 'unavailable_api'
    elif damage == 'nonbool': state['filter_receipt']['raw_result'] = 1
    elif damage == 'state_gid': state['reader_gid'] = '99' * 16
    elif damage == 'worker': state['pid'] += 1
    elif damage == 'clock': state['monotonic_clock_identity'] = {'boot_id': 'other'}
    elif damage == 'command': end['command_id'] = 99
    elif damage == 'state_role': state['role'] = 'writer'
    elif damage == 'api_name': end.pop('api')
    elif damage == 'state_time': end['monotonic_ns'] = 0
    elif damage == 'state_result': end['raw_result'] = False
    elif damage == 'state_receipt': state['filter_receipt'] = {}
    elif damage == 'raw_envelope': state['event_index'] = 0
    elif damage == 'ack': next(e for e in scoped if e['event'] == 'command_done')['event'] = 'offline_missing_ack'
    elif damage == 'request': next(e for e in scoped if e.get('api_stage') == 'set_begin')['requested'] = {}
    elif damage == 'requested': action['params']['expression'] = ''
    elif damage == 'readback': next(e for e in scoped if e.get('api_stage') == 'get_success')['raw_readback'] = {}
    elif damage == 'controller_context': result['domain_id'] += 1
    elif damage == 'result_receipt': result['receipt'] = {}
    else:
        # An actual retirement before the command cannot be disguised as state.
        next(e for e in rows if e['event'] == 'content_filter_state_observation'
             and e.get('phase') == 'initial')['event'] = 'endpoint_destroyed'
    with pytest.raises((ValueError, KeyError, TypeError)):
        filter_action_evidence(action, rows)


@pytest.mark.parametrize('damage', ['empty_requires', 'forged_proof', 'false_execution', 'fake_after', 'terminal'])
def test_canonical_action_audit_recomputes_not_trusts_state_receipt(damage):
    rows, actions, start, end = action_rows()
    done = next(e for e in rows if e['event'] == 'topology_action_done' and e['action_id'] == 'reject')
    receipt = done['receipt']
    if damage == 'empty_requires': receipt['requires'] = []
    elif damage == 'forged_proof': receipt['filter_action_evidence'] = {}
    elif damage == 'false_execution': receipt['execution_evidence'] = True
    elif damage == 'fake_after': receipt['after']['filter_update_raw_ref'] = 0
    else: receipt['disposition'] = 'applied'
    with pytest.raises(ValueError): audit_actions(rows, actions, start, end)


def test_action_binding_uses_the_same_public_filter_normalization_as_submission():
    rows, actions, start, end = action_rows(padded=True)
    assert actions[1]['params']['expression'].startswith('  ')
    evidence = filter_action_evidence(actions[1], rows)
    assert evidence['outcome_kind'] == 'configuration_readback'
    assert len(audit_actions(rows, actions, start, end)) == 3


@pytest.mark.parametrize('damage', ['zero_interval', 'before_snapshot', 'complete_before_begin',
    'failed_status', 'wrong_result_type', 'wrong_state', 'failure_reason', 'failure_error'])
def test_review_state_conflicts_rejected_by_helper_and_canonical(damage):
    rows, actions, start, finish = action_rows()
    scoped = [e for e in rows if e.get('command_id') == 2]
    observed = next(e for e in scoped if e['event'] == 'content_filter_state_observation')
    begin = next(e for e in scoped if e.get('api_stage') == 'state_begin')
    end = next(e for e in scoped if e.get('api_stage') == 'state_success')
    snapshot = next(e for e in scoped if e['event'] == 'endpoint_identity_snapshot' and e['phase'] == 'after')
    state = observed['filter_receipt']
    if damage in {'zero_interval', 'before_snapshot', 'complete_before_begin'}:
        if damage == 'zero_interval': state['started_monotonic_ns'] = state['completed_monotonic_ns'] = 0
        elif damage == 'before_snapshot': state['started_monotonic_ns'] = snapshot['monotonic_ns'] - 1
        else: state['completed_monotonic_ns'] = begin['monotonic_ns'] - 1
        begin['started_monotonic_ns'] = state['started_monotonic_ns']
        end['completed_monotonic_ns'] = state['completed_monotonic_ns']
    elif damage == 'failed_status': end['status'] = 'failed'
    elif damage == 'wrong_result_type': end['result_type'] = 'int'
    elif damage == 'wrong_state': end['state'] = 'disabled'
    elif damage == 'failure_reason': end['reason'] = 'is_cft_enabled_failed'
    else: end['error'] = 'simulated descriptor failure'
    # Synchronize the copied summaries, so rejection is not a stale-summary or
    # stale action-proof comparison; only the reviewed raw conflict differs.
    dynamic = next(e for e in scoped if e['event'] == 'content_filter_dynamic_update')
    dynamic['filter_receipt']['state_receipt'] = deepcopy(state)
    result = next(e for e in scoped if e['event'] == 'topology_filter_update_result')
    result['receipt'] = deepcopy(dynamic['filter_receipt'])
    for row in rows:
        if row.get('action_id') == 'reject' and row['event'] == 'topology_action_done':
            row['receipt']['filter_update_receipt'] = deepcopy(result['receipt'])
    with pytest.raises(ValueError, match='filter_action:state_'):
        filter_action_evidence(actions[1], rows)
    with pytest.raises(ValueError, match='filter_action:state_'):
        audit_actions(rows, actions, start, finish)
