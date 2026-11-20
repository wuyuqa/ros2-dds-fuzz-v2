"""Offline disabled-CFT observation fixtures, NOT ROS/DDS execution evidence."""
from types import SimpleNamespace
import time

import pytest

from r2f.runtime.filter_backend import ContentFilterAdapter
from r2f.runtime.topology_worker import _update_filter_identity


A, B, C = ('01' * 15 + suffix for suffix in ('04', '14', '24'))


class RCLError(RuntimeError):
    pass


class NativeShapeSubscription:
    """Double only the public property/set/get boundaries; never instantiate ROS."""
    topic_name = '/offline-cft'

    def __init__(self):
        self.expression = ''
        self.parameters = []
        self.enabled = False
        self.calls = []
        self.gid = A

    @property
    def is_cft_enabled(self):
        self.calls.append(('is_cft_enabled',))
        return self.enabled

    def set_content_filter(self, expression, parameters):
        self.calls.append(('set_content_filter', expression, list(parameters)))
        if not expression and not self.enabled:
            raise RCLError('current subscriber has no content filter topic')
        self.expression, self.parameters = expression, list(parameters)
        self.enabled = bool(expression)
        self.gid = B if self.enabled else C

    def get_content_filter(self):
        self.calls.append(('get_content_filter',))
        if not self.enabled:
            raise RCLError('this subscriber has not created a ContentFilteredTopic')
        return {'filter_expression': self.expression,
                'expression_parameters': list(self.parameters)}


def graph(sub):
    return {'publishers': [], 'subscriptions': [dict(
        node_name='reader', endpoint_gid=sub.gid, topic_type='std_msgs/msg/String')]}


def test_public_disabled_state_is_not_a_configuration_readback():
    sub = NativeShapeSubscription()
    adapter = ContentFilterAdapter()
    adapter.attach(sub)
    events = []
    state = adapter.observe_state(api_event=lambda stage, **kw: events.append((stage, kw)))
    assert state['status'] == 'observed'
    assert state['is_cft_enabled'] is False and state['state'] == 'disabled'
    assert state['raw_result'] is False
    assert state['api'] == 'rclpy.subscription.Subscription.is_cft_enabled'
    assert state['started_monotonic_ns'] <= state['completed_monotonic_ns']
    assert [stage for stage, _ in events] == ['state_begin', 'state_success']
    assert sub.calls == [('is_cft_enabled',)]
    assert 'observed' not in state and 'readback_status' not in state
    assert state['filter_effect_observed'] is None
    assert not state['candidate_eligible'] and not state['mutation_receipt']


@pytest.mark.parametrize('raw', [0, 1, None, 'false', [], lambda: False])
def test_non_bool_state_cannot_be_coerced_to_disabled(raw):
    adapter = ContentFilterAdapter()
    adapter.attach(SimpleNamespace(is_cft_enabled=raw))
    state = adapter.observe_state()
    assert state['status'] == 'invalid_result'
    assert state['is_cft_enabled'] is None and state['state'] is None
    assert state['candidate_eligible'] is False


def test_state_descriptor_exception_is_preserved_not_missing_api():
    class Bad:
        @property
        def is_cft_enabled(self):
            raise AttributeError('live descriptor failed')
    adapter = ContentFilterAdapter()
    adapter.attach(Bad())
    stages = []
    state = adapter.observe_state(api_event=lambda stage, **kw: stages.append(stage))
    assert state['status'] == 'failed' and state['error_type'] == 'AttributeError'
    assert state['error'] == 'live descriptor failed'
    assert state['is_cft_enabled'] is None
    assert stages == ['state_begin', 'state_failure']


@pytest.mark.parametrize('attached', [False, True])
def test_missing_state_api_is_unknown(attached):
    adapter = ContentFilterAdapter()
    if attached:
        adapter.attach(SimpleNamespace())
    state = adapter.observe_state()
    assert state['status'] == 'unavailable_api'
    assert state['is_cft_enabled'] is None
    assert state['reason'] == ('is_cft_enabled_unavailable' if attached else 'no_attached_subscription')


def test_worker_preserves_empty_reject_empty_set_get_failures_and_actual_state():
    from r2f.runtime.topology_worker import _observe_filter_state
    sub = NativeShapeSubscription()
    rows = []
    lineage = dict(logical_id='r0', generation=1, endpoint_generation='offline-process',
                   run_id='offline-fixture', rmw_identifier='offline-fixture',
                   role='reader', domain_id=191, node_name='reader',
                   message_type='std_msgs/msg/String', pid=100, pgid=100,
                   process_start_ticks=1, monotonic_clock_identity={'boot_id': 'fixture'})
    def emit(event, **fields):
        row = {'event': event, 'event_index': len(rows), 'monotonic_ns': time.monotonic_ns(),
               **lineage, **fields}
        rows.append(row)
        return row
    token = emit('endpoint_created', endpoint_gid=A, topic=sub.topic_name,
                 created_monotonic_ns=time.monotonic_ns())
    initial = _observe_filter_state(sub, emit, command='create_endpoint', command_id=None,
                                    reader_gid=A, phase='initial')
    assert initial['is_cft_enabled'] is False
    gid, incarnation = A, 0
    for command_id, expression, parameters in [(1, '', []), (2, 'data = %0', ['reject']), (3, '', [])]:
        gid, incarnation = _update_filter_identity(
            sub, lambda: graph(sub), 'reader', gid, incarnation,
            dict(command_id=command_id, expression=expression, parameters=parameters), emit,
            state_observer=_observe_filter_state)
        emit('command_done', command='update_filter', command_id=command_id,
             endpoint_gid=gid, reader_gid=gid, incarnation_ordinal=incarnation)
        from r2f.runtime.endpoint_incarnation import validate_filter_identity_update
        token = validate_filter_identity_update(rows, token, command_id)
        assert token['endpoint_gid'] == gid and token['incarnation_ordinal'] == incarnation
    updates = [r for r in rows if r['event'] == 'content_filter_dynamic_update']
    first, reject, restore = (r['filter_receipt'] for r in updates)
    assert first['api_status'] == 'failed' and first['readback_status'] == 'not_attempted'
    assert 'current subscriber has no content filter topic' in first['error']
    assert first['state_receipt']['is_cft_enabled'] is False
    assert reject['api_status'] == 'succeeded' and reject['readback_status'] == 'matched'
    assert reject['state_receipt']['is_cft_enabled'] is True
    assert restore['api_status'] == 'succeeded' and restore['readback_status'] == 'failed'
    assert 'this subscriber has not created a ContentFilteredTopic' in restore['error']
    assert restore['observed'] is None and restore['applied'] is False
    assert restore['state_receipt']['is_cft_enabled'] is False
    assert (gid, incarnation) == (C, 2)
    for update in updates:
        cid = update['command_id']
        scoped = [r for r in rows if r.get('command_id') == cid]
        after = next(r for r in scoped if r['event'] == 'endpoint_identity_snapshot' and r['phase'] == 'after')
        state_events = [r for r in scoped if r['event'] == 'content_filter_state_api_call']
        observation = next(r for r in scoped if r['event'] == 'content_filter_state_observation')
        assert [r['api_stage'] for r in state_events] == ['state_begin', 'state_success']
        assert all(r['reader_gid'] == r['endpoint_gid'] == after['endpoint_gid'] for r in state_events + [observation])
        assert all(r['generation'] == 1 and r['endpoint_generation'] == 'offline-process' for r in scoped)
        assert rows.index(after) < rows.index(state_events[0]) < rows.index(state_events[1]) < rows.index(observation) < rows.index(update)
        assert observation['filter_receipt'] == update['filter_receipt']['state_receipt']
        assert update['filter_receipt']['filter_effect_observed'] is None
        assert not update['filter_receipt']['candidate_eligible']
    assert [call for call in sub.calls if call[0] == 'set_content_filter'] == [
        ('set_content_filter', '', []), ('set_content_filter', 'data = %0', ['reject']),
        ('set_content_filter', '', [])]


@pytest.mark.parametrize('observed_enabled', [False, True])
def test_get_failure_does_not_determine_state_or_change_its_own_failure(observed_enabled):
    from r2f.runtime.topology_worker import _observe_filter_state
    class Sub(NativeShapeSubscription):
        def get_content_filter(self):
            raise RCLError('get failed independently')

        @property
        def is_cft_enabled(self):
            return observed_enabled
    sub = Sub()
    sub.enabled, sub.gid = True, B
    rows = []
    _update_filter_identity(sub, lambda: graph(sub), 'reader', B, 1,
        dict(command_id=7, expression='', parameters=[]),
        lambda event, **fields: rows.append(dict(event=event, **fields)),
        state_observer=_observe_filter_state)
    value = next(r['filter_receipt'] for r in rows if r['event'] == 'content_filter_dynamic_update')
    assert value['api_status'] == 'succeeded' and value['readback_status'] == 'failed'
    assert value['observed'] is None and value['applied'] is False
    assert value['state_receipt']['is_cft_enabled'] is observed_enabled
    assert value['error'] == 'RCLError:get failed independently'
    assert value['filter_effect_observed'] is None and value['candidate_eligible'] is False


def test_worker_state_failure_remains_unknown_and_set_error_is_preserved():
    from r2f.runtime.topology_worker import _observe_filter_state
    class Sub(NativeShapeSubscription):
        @property
        def is_cft_enabled(self):
            raise RCLError('state API failed')
    sub = Sub()
    rows = []
    _update_filter_identity(sub, lambda: graph(sub), 'reader', A, 0,
        dict(command_id=7, expression='', parameters=[]),
        lambda event, **fields: rows.append(dict(event=event, **fields)),
        state_observer=_observe_filter_state)
    value = next(r['filter_receipt'] for r in rows if r['event'] == 'content_filter_dynamic_update')
    assert value['api_status'] == 'failed' and value['readback_status'] == 'not_attempted'
    assert value['error'] == 'RCLError:current subscriber has no content filter topic'
    assert value['state_receipt']['status'] == 'failed'
    assert value['state_receipt']['is_cft_enabled'] is None
    assert value['state_receipt']['error'] == 'state API failed'
    assert value['state_receipt']['layer'] == 'submission_rcl'
    assert [r['api_stage'] for r in rows if r['event'] == 'content_filter_state_api_call'] == [
        'state_begin', 'state_failure']
    assert [r['api_stage'] for r in rows if r['event'] == 'content_filter_api_call'] == [
        'set_begin', 'set_failure']


def test_raw_evidence_failure_is_not_swallowed_as_state_success():
    adapter = ContentFilterAdapter()
    adapter.attach(NativeShapeSubscription())
    def emit(stage, **fields):
        if stage == 'state_success':
            raise OSError('raw sink failed')
    with pytest.raises(OSError, match='raw sink failed'):
        adapter.observe_state(api_event=emit)
    assert adapter.history == []
