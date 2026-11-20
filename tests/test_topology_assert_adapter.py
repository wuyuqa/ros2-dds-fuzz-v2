"""Offline API-action binding only; no DDS lifecycle inference."""
from copy import deepcopy
import pytest
from r2f.campaign.topology_adapter import _assert_api_event


def sample():
    identity = dict(logical_id='writer', generation=1, pid=101,
                    process_start_ticks=9, endpoint_generation='worker-1')
    clock = {'boot_id': 'boot-1', 'device': 4, 'inode': 42}
    token = {**identity, 'endpoint_gid': 'aabb', 'event_index': 1,
             'monotonic_ns': 1, 'monotonic_clock_identity': clock}
    action = {'endpoint_id': 'writer', 'generation': 1}
    api = 'rclpy.publisher.Publisher.assert_liveliness'
    receipt = {'after': {'assert': {'command_id': 7, 'api': api}}}
    request = dict(event='topology_assert_liveliness_requested', **identity, endpoint_gid='aabb',
                   command='assert_liveliness', command_id=7, event_index=6, monotonic_ns=61,
                   request_monotonic_ns=60, request_clock_identity=deepcopy(clock),
                   monotonic_clock_identity=deepcopy(clock))
    write = dict(event='topology_assert_liveliness_write_result', **identity, endpoint_gid='aabb',
                 command='assert_liveliness', command_id=7, event_index=7, monotonic_ns=111,
                 result_monotonic_ns=110, result_clock_identity=deepcopy(clock),
                 monotonic_clock_identity=deepcopy(clock), status='flushed',
                 bytes_written=45, bytes_expected=45, error_type=None)
    raw = [dict(event='liveliness_asserted', **identity, endpoint_gid='aabb',
                command_id=7, api=api, event_index=10, monotonic_ns=100,
                monotonic_clock_identity=deepcopy(clock)),
           dict(event='command_done', **identity, command_id=7,
                event_index=11, monotonic_ns=101, monotonic_clock_identity=deepcopy(clock))]
    begin = dict(event_index=5, monotonic_ns=50, monotonic_clock_identity=deepcopy(clock))
    after = dict(event_index=12, monotonic_ns=120, monotonic_clock_identity=deepcopy(clock))
    done = dict(event_index=13, monotonic_ns=130, monotonic_clock_identity=deepcopy(clock))
    raw.extend([request, write])
    return raw, action, receipt, token, begin, after, done


def test_matching_command_and_writer_is_api_only():
    raw, action, receipt, token, begin, after, done = sample()
    assert _assert_api_event(raw, action, receipt, token, begin, after, done) is raw[0]


@pytest.mark.parametrize('fault', ['command', 'writer', 'generation', 'gid', 'missing',
                                    'no_id', 'no_ack', 'wrong_ack', 'order', 'duplicate', 'fake'])
def test_incomplete_or_misattributed_assert_is_rejected(fault):
    raw, action, receipt, token, begin, after, done = sample()
    if fault == 'command': raw[0]['command_id'] = 8
    if fault == 'writer': raw[0]['logical_id'] = 'other'
    if fault == 'generation': raw[0]['generation'] = 2
    if fault == 'gid': raw[0]['endpoint_gid'] = 'other'
    if fault == 'missing': raw.pop(0)
    if fault == 'no_id': receipt['after']['assert'].pop('command_id')
    if fault == 'no_ack': raw.pop()
    if fault == 'wrong_ack': raw[1]['command_id'] = 8
    if fault == 'order': raw[1]['monotonic_ns'] = 99
    if fault == 'duplicate': raw.append(deepcopy(raw[0]))
    if fault == 'fake': receipt['after']['assert'] = {'api': 'offline_fake_assert_liveliness', 'real_ros': False}
    with pytest.raises(ValueError):
        _assert_api_event(raw, action, receipt, token, begin, after, done)


@pytest.mark.parametrize('location,change', [
    ('row', 'missing'), ('ack', 'missing'), ('begin', 'missing'),
    ('after', 'missing'), ('done', 'missing'), ('token', 'missing'),
    ('row', 'boot'), ('ack', 'device'), ('begin', 'inode'),
    ('row', 'invalid_inode'), ('row', 'invalid_device'), ('row', 'empty_boot'),
])
def test_clock_identity_required_for_all_cross_process_events(location, change):
    raw, action, receipt, token, begin, after, done = sample()
    event = {'row': raw[0], 'ack': raw[1], 'token': token,
             'begin': begin, 'after': after, 'done': done}[location]
    if change == 'missing':
        event.pop('monotonic_clock_identity')
    else:
        event['monotonic_clock_identity'] = deepcopy(event['monotonic_clock_identity'])
        key, value = {'boot': ('boot_id', 'different'), 'device': ('device', 5),
                      'inode': ('inode', 43), 'invalid_inode': ('inode', True),
                      'invalid_device': ('device', -1), 'empty_boot': ('boot_id', '')}[change]
        event['monotonic_clock_identity'][key] = value
    with pytest.raises(ValueError, match='action_assert_monotonic_clock_identity_missing_or_mismatch'):
        _assert_api_event(raw, action, receipt, token, begin, after, done)


@pytest.mark.parametrize('fault', ['request_missing', 'write_missing', 'request_command',
                                    'request_writer', 'request_generation', 'request_gid',
                                    'write_command', 'write_failed', 'short_write',
                                    'request_clock', 'write_clock', 'request_time',
                                    'request_after_api', 'duplicate_request'])
def test_request_chain_fail_closed(fault):
    raw, action, receipt, token, begin, after, done = sample()
    request, write = raw[2], raw[3]
    if fault == 'request_missing': raw.remove(request)
    if fault == 'write_missing': raw.remove(write)
    if fault == 'request_command': request['command_id'] = 8
    if fault == 'request_writer': request['logical_id'] = 'other'
    if fault == 'request_generation': request['generation'] = 2
    if fault == 'request_gid': request['endpoint_gid'] = 'other'
    if fault == 'write_command': write['command_id'] = 8
    if fault == 'write_failed': write['status'] = 'failed'
    if fault == 'short_write': write['bytes_written'] = 44
    if fault == 'request_clock': request['request_clock_identity'] = None
    if fault == 'write_clock': write['result_clock_identity'] = None
    if fault == 'request_time': request.pop('request_monotonic_ns')
    if fault == 'request_after_api': request['request_monotonic_ns'] = 101
    if fault == 'duplicate_request': raw.append(deepcopy(request))
    with pytest.raises(ValueError):
        _assert_api_event(raw, action, receipt, token, begin, after, done)


def test_pair_sink_endpoint_command_offline_integration(tmp_path):
    """Exercise the real pair sink/pipe controller, not a real ROS worker."""
    import json
    from types import SimpleNamespace
    from r2f.modules.s1 import topology_model as model
    from r2f.runtime.topology_pair import run_topology
    from r2f.runtime.topology_worker import OfflineTopologyBackend, _EndpointProcess

    class Pipe:
        def __init__(self, fail): self.fail = fail
        def write(self, data):
            self.payload = json.loads(data)
            if self.fail: raise BrokenPipeError('offline test')
            return len(data)
        def flush(self): pass

    class Backend(OfflineTopologyBackend):
        def __init__(self, fail):
            super().__init__()
            self.fail = fail
        def assert_liveliness(self, handle):
            pipe = Pipe(self.fail)
            proc = _EndpointProcess({'logical_id': handle['logical_id'],
                                     'generation': handle['generation']}, {}, tmp_path,
                                    'writer', self.event_sink)
            proc.process = SimpleNamespace(pid=handle['pid'], stdin=pipe, poll=lambda: None)
            proc.events.append({'event': 'endpoint_created', **handle,
                                'run_id': 'offline-topology'})
            command_id = proc.command('assert_liveliness')
            identity = {k: handle[k] for k in ('logical_id', 'generation', 'endpoint_gid',
                        'pid', 'process_start_ticks', 'endpoint_generation')}
            from r2f.runtime.topology_worker import _monotonic_clock_identity
            import time
            for event, extra in [('liveliness_asserted', {
                'api': 'rclpy.publisher.Publisher.assert_liveliness'}), ('command_done', {})]:
                row = {'event': event, **identity, **extra, 'command_id': command_id,
                       'monotonic_ns': time.monotonic_ns(),
                       'monotonic_clock_identity': _monotonic_clock_identity()}
                proc._dispatch(json.dumps(row).encode())
            return {'api': 'rclpy.publisher.Publisher.assert_liveliness',
                    'command_id': command_id}

    for fail in (False, True):
        program = model.generate_program(seed=1, writers=1, readers=0)
        writer = program['endpoints'][0]
        writer['qos']['liveliness'] = 'manual_by_topic'
        wid = writer['logical_id']
        program['actions'] = [model._action('create', 'create', wid, 1),
                              model._action('assert', 'assert_liveliness', wid, 1)]
        out = tmp_path / ('failure' if fail else 'success')
        result = run_topology(program, {'backend': 'offline_fake', 'run_id': 'offline-topology',
                              'domain_id': 191, 'source_repo': '/tmp/offline-src'}, out,
                              backend=Backend(fail))
        rows = [json.loads(line) for line in (out / 'raw.jsonl').read_text().splitlines()]
        begin = next(e for e in rows if e['event'] == 'topology_action_begin' and e['action_id'] == 'assert')
        done = next(e for e in rows if e['event'] == 'topology_action_done' and e['action_id'] == 'assert')
        after = next(e for e in rows if e['event'] == 'state_step_after' and e['step_id'] == 'action:assert')
        token = next(e for e in rows if e['event'] == 'endpoint_created')
        inside = [e for e in rows if begin['event_index'] < e['event_index'] < after['event_index']]
        write = next(e for e in inside if e['event'] == 'topology_assert_liveliness_write_result')
        if fail:
            assert write['status'] == 'failed'
            assert done['receipt']['disposition'] == 'tool_error'
            with pytest.raises(ValueError):
                _assert_api_event(inside, begin['action'], done['receipt'], token, begin, after, done)
        else:
            assert write['status'] == 'flushed'
            assert _assert_api_event(inside, begin['action'], done['receipt'], token, begin, after, done)['event'] == 'liveliness_asserted'
            assert done['receipt']['lifecycle_evidence']['status'] == 'unknown'
