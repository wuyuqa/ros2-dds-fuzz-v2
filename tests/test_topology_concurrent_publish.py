"""All tests are offline contract fixtures, never live DDS concurrency evidence."""
from copy import deepcopy
import json
import time

import pytest

from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_concurrency import audit_concurrency
from r2f.runtime.topology_worker import RosTopologyBackend, TopologyBackendTimeout


def program():
    p = model.generate_program(7, writers=2, readers=1)
    for a in p['actions']:
        if a['op'] == 'publish_batch':
            a['params']['concurrency_group'] = 'writers-1'
            a['params']['count'] = 3
    return p


def raw_fixture(serial=False, fixture=True):
    actions = [a for a in program()['actions'] if a['op'] == 'publish_batch']
    events = []
    def add(kind, t, who, **fields):
        e = dict(event=kind, monotonic_ns=t, event_index=len(events),
                 logical_id=actions[who]['endpoint_id'], generation=1,
                 pid=100 + who, pgid=100 + who, process_start_ticks=123,
                 endpoint_generation='epoch-' + str(who), run_id='offline-fixture', domain_id=191,
                 endpoint_gid='aabb' + str(who), monotonic_clock_identity={'boot_id': 'fixture', 'inode': 1},
                 fixture=fixture, real_ros=not fixture)
        e.update(fields)
        events.append(e)
    for who in range(2):
        add('endpoint_created', 1, who)
    for kind, ts in [('concurrent_publish_requested', 10), ('concurrent_publish_ready', 20),
                     ('concurrent_publish_release', 30), ('concurrent_publish_started', 40)]:
        for who in range(2):
            add(kind, ts, who, command_id=7 + who, concurrency_group='writers-1')
    for who in range(2):
        for i in range(3):
            lo = 50 + i * 20 + (100 if serial and who else who)
            add('send', lo + 10, who, command_id=7 + who,
                publish_begin_ns=lo, publish_return_ns=lo+9, publish_returned=True,
                writer_gid='aabb' + str(who), send_sequence=i, phase=actions[who]['action_id'])
        add('concurrent_publish_finished', 220, who, command_id=7+who, concurrency_group='writers-1')
        add('command_done', 230, who, command_id=7+who)
    return events, actions


def test_positive_individual_api_overlap_fixture_never_qualifies_dds():
    events, actions = raw_fixture()
    got = audit_concurrency(events, actions, 'writers-1')
    assert got['fixture_overlap_observed'] and not got['concurrency_observed']
    assert got['publish_api_overlaps'][0]['overlap_ns'] == 8
    assert not got['candidate_eligible']


def test_local_real_flags_without_global_provenance_never_qualify_dds():
    # Local synthetic real flags alone do not establish global execution origin.
    # The complete positive shape is exercised in the provenance unit tests.
    events, actions = raw_fixture(fixture=False)
    observation = audit_concurrency(events, actions, 'writers-1')
    assert not observation['concurrency_observed']
    assert observation['fixture'] and observation['fixture_overlap_observed']


def test_serial_api_intervals_despite_overlapping_batch_envelopes():
    events, actions = raw_fixture(serial=True)
    got = audit_concurrency(events, actions, 'writers-1')
    assert not got['concurrency_observed'] and not got['fixture_overlap_observed']
    assert got['status'] == 'serial_or_no_api_overlap'


@pytest.mark.parametrize('kind,key,value', [
    ('concurrent_publish_ready', 'pid', 999),
    ('concurrent_publish_ready', 'pgid', 999),
    ('concurrent_publish_ready', 'command_id', 999),
    ('command_done', 'endpoint_gid', 'foreign'),
    ('command_done', 'process_start_ticks', 999),
    ('concurrent_publish_started', 'monotonic_clock_identity', {'boot_id': 'foreign'}),
    ('concurrent_publish_ready', 'monotonic_ns', 31),
    ('send', 'publish_begin_ns', 0),
    ('send', 'writer_gid', 'foreign'),
    ('send', 'phase', 'foreign'),
])
def test_forged_identity_clock_hb_and_publish_intervals(kind, key, value):
    events, actions = raw_fixture()
    next(e for e in events if e['event'] == kind)[key] = value
    with pytest.raises(ValueError, match='concurrency:'):
        audit_concurrency(events, actions, 'writers-1')


@pytest.mark.parametrize('kind', ['concurrent_publish_ready', 'command_done', 'concurrent_publish_finished'])
def test_missing_or_duplicate_ready_completion(kind):
    events, actions = raw_fixture()
    index = next(i for i,e in enumerate(events) if e['event'] == kind)
    duplicate = deepcopy(events)
    duplicate.append(deepcopy(events[index]))
    with pytest.raises(ValueError):
        audit_concurrency(duplicate, actions, 'writers-1')
    del events[index]
    with pytest.raises(ValueError):
        audit_concurrency(events, actions, 'writers-1')


@pytest.mark.parametrize('fault', ['same_writer', 'dependency', 'noncontiguous', 'single', 'repeat', 'lifecycle'])
def test_group_structure_rejects(fault):
    p = program()
    pubs = [a for a in p['actions'] if a['op'] == 'publish_batch']
    if fault == 'same_writer':
        pubs[1]['endpoint_id'] = pubs[0]['endpoint_id']
    elif fault == 'dependency':
        pubs[1]['depends_on'] = [pubs[0]['action_id']]
    elif fault == 'noncontiguous':
        p['actions'].insert(p['actions'].index(pubs[1]), model._action('middle', 'snapshot', params={}))
    elif fault == 'single':
        del pubs[1]['params']['concurrency_group']
    elif fault == 'repeat':
        pubs[1]['params']['repeat_data_from'] = pubs[0]['action_id']
    elif fault == 'lifecycle':
        p['actions'][0]['params']['concurrency_group'] = 'writers-1'
    assert model.validate_program(p)['status'] == 'invalid'


def test_group_identity_and_legacy_no_injected_defaults():
    p = model.generate_program(7, writers=2, readers=1)
    before = deepcopy(p)
    old_hash = model.validate_program(p)['input_sha256']
    assert p == before and all('concurrency_group' not in a['params'] for a in p['actions'])
    grouped = program()
    assert model.validate_program(grouped)['status'] == 'valid'
    assert old_hash != model.validate_program(grouped)['input_sha256']
    assert model.validate_program(before)['input_sha256'] == old_hash


class MockProcess:
    """No subprocesses; exercises production backend submit/join/timeout logic."""
    def __init__(self, identity, trace, missing=None):
        self.identity, self.trace, self.missing = identity, trace, missing
        self.token = dict(logical_id='w'+str(identity), generation=1, pid=100+identity, pgid=100+identity,
                          endpoint_gid='aa'+str(identity), process_start_ticks=1, endpoint_generation='e'+str(identity),
                          run_id='mock-only', domain_id=191, monotonic_clock_identity={'boot_id': 'mock'}, monotonic_ns=1)
        self.events = [dict(event='endpoint_created', **self.token)]
        self.process = self
        self.serial = 0
    def poll(self):
        return None
    def pump(self, timeout):
        pass
    def has(self, kind, **fields):
        return any(e['event'] == kind and all(e.get(k) == v for k,v in fields.items()) for e in self.events)
    def command(self, command, **fields):
        self.serial += 1
        self.trace.append((self.identity, command))
        if command == 'prepare_concurrent_publish':
            self.pending = fields
            if self.missing != 'ready':
                self.events.append(dict(event='concurrent_publish_ready', command_id=self.serial, concurrency_group=fields['concurrency_group'], **self.token))
        else:
            assert all((i, 'prepare_concurrent_publish') in self.trace for i in (0, 1))
            cid = fields['prepared_command_id']
            for seq in self.pending['sequences']:
                self.events.append(dict(event='send', command_id=cid, send_sequence=seq,
                                        publish_begin_ns=100, publish_return_ns=110))
            if self.missing != 'completion':
                self.events.append(dict(event='command_done', command_id=cid))
        return self.serial


def backend_fixture(missing=None):
    backend = RosTopologyBackend.__new__(RosTopologyBackend)
    backend.config = {'publish_timeout_sec': 0.005}
    backend.backend_name = 'mock-offline-only'
    backend.closed = False
    trace, cleaned = [], []
    requests = []
    for i in range(2):
        proc = MockProcess(i, trace, missing if i else None)
        requests.append(dict(handle={'logical_id': 'w'+str(i), 'process': proc},
                             sequences=[0, 1, 2], interval_sec=0, phase='publish_w'+str(i)))
    backend.destroy_endpoint = lambda h: cleaned.append(h['logical_id'])
    return backend, requests, trace, cleaned


def test_production_backend_async_submit_all_ready_release_all():
    backend, requests, trace, cleaned = backend_fixture()
    result = backend.publish_concurrent('writers-1', requests)
    assert list(result) == ['publish_w0', 'publish_w1']
    assert trace == [(0, 'prepare_concurrent_publish'), (1, 'prepare_concurrent_publish'),
                     (0, 'release_concurrent_publish'), (1, 'release_concurrent_publish')]
    assert cleaned == []


@pytest.mark.parametrize('missing', ['ready', 'completion'])
def test_production_backend_bounded_timeout_cleans_entire_group(missing):
    backend, requests, trace, cleaned = backend_fixture(missing)
    with pytest.raises(TopologyBackendTimeout):
        backend.publish_concurrent('writers-1', requests)
    assert cleaned == ['w0', 'w1']
    if missing == 'ready':
        assert not any(c == 'release_concurrent_publish' for _,c in trace)

from r2f.runtime.topology_worker import OfflineTopologyBackend
from r2f.runtime.topology_pair import run_topology


class SerialGroupFixture(OfflineTopologyBackend):
    """Offline producer plumbing only: deliberately serial publish intervals."""
    def __init__(self):
        super().__init__(match_policy='all')
        self.defer_done = False
        self.held_done = []
    def _emit(self, kind, **fields):
        if kind == 'command_done' and self.defer_done:
            self.held_done.append(fields)
            return fields
        return super()._emit(kind, **{'fixture': True, **fields})
    def publish_concurrent(self, group, requests):
        def fields(request, cid):
            return dict(concurrency_group=group, command_id=cid,
                        **{k: request['handle'][k] for k in
                           ('logical_id', 'generation', 'endpoint_gid', 'pid', 'pgid', 'process_start_ticks', 'endpoint_generation')})
        ids = [self.command_serial + i + 1 for i in range(2)]
        for kind in ('concurrent_publish_requested', 'concurrent_publish_ready', 'concurrent_publish_release'):
            for request, cid in zip(requests, ids):
                self._emit(kind, **fields(request, cid))
        results = {}
        for request, cid in zip(requests, ids):
            self._emit('concurrent_publish_started', **fields(request, cid))
            self.defer_done = True
            result = self.publish_batch(request['handle'], request['sequences'], request['interval_sec'],
                                        request['phase'], payload=request.get('payload'))
            self.defer_done = False
            self._emit('concurrent_publish_finished', **fields(request, cid))
            self._emit('command_done', **self.held_done.pop())
            results[request['phase']] = result
        return results


def test_program_backend_raw_consumer_plumbing_serial_fixture(tmp_path):
    p = program()
    runtime = run_topology(p, {'backend': 'offline_fake', 'run_id': 'offline-l02', 'domain_id': 191},
                           tmp_path, backend=SerialGroupFixture())
    assert runtime['status'] == 'executed', runtime
    events = runtime['events']
    assert json.loads(open(runtime['raw_path']).readline()) == events[0]
    got = audit_concurrency(events, p['actions'], 'writers-1')
    assert got['status'] == 'serial_or_no_api_overlap'
    assert not got['concurrency_observed']
    assert any(e['event'] == 'topology_concurrency_observation' for e in events)
    assert not runtime['candidate_eligible']
    assert len([e for e in events if e['event'] == 'topology_action_done']) == len(p['actions'])
    # Existing per-action raw consumer must consume the second writer's earlier
    # command-bound sends, not incorrectly require its later controller window.
    from r2f.runtime.topology_route import _action_proof
    for action in p['actions']:
        refs, reason = _action_proof(action, events)
        assert reason is None, (action, reason)
        assert refs


def test_existing_canonical_route_and_fact_consumer_offline(tmp_path):
    from unittest.mock import patch
    from test_topology_runtime_route import config_for
    from r2f.runtime.ros2_pair import run_pair
    from r2f.campaign.topology_adapter import facts_from_events
    p = program()
    # Fixture helper only: deterministic pre-hash input, no posthash edits.
    with patch.object(model, 'generate_program', return_value=p):
        plan, config = config_for()
    assert any(a['params'].get('concurrency_group') for a in plan.final_config['topology_program']['actions'])
    runtime = run_pair(config, tmp_path, topology_backend=SerialGroupFixture())
    assert runtime['status'] == 'executed', runtime
    facts = facts_from_events(runtime['events'], runtime, config)
    assert facts['diagnostic_valid'], facts
    assert not facts['concurrency_observations'][0]['concurrency_observed']
    assert not facts['candidate_eligible'] and not facts['seal_eligible']


@pytest.mark.parametrize('key,value', [('pid', 999), ('pgid', 999), ('endpoint_gid', 'foreign'),
                                      ('command_id', 999), ('monotonic_clock_identity', {'boot_id':'foreign'})])
def test_backend_rejects_forged_ready_before_any_release(key, value):
    from r2f.runtime.topology_worker import TopologyBackendError
    backend, requests, trace, cleaned = backend_fixture()
    process = requests[1]['handle']['process']
    command = process.command
    def forged(cmd, **fields):
        cid = command(cmd, **fields)
        if cmd == 'prepare_concurrent_publish':
            process.events[-1][key] = value
        return cid
    process.command = forged
    with pytest.raises((TopologyBackendError, TopologyBackendTimeout)):
        backend.publish_concurrent('writers-1', requests)
    assert not any(c == 'release_concurrent_publish' for _,c in trace)
    assert cleaned == ['w0', 'w1']


def test_submit_failure_cleans_both_workers_even_unsubmitted_peer():
    from r2f.runtime.topology_worker import TopologyBackendError
    backend, requests, trace, cleaned = backend_fixture()
    def fail(*args, **kwargs):
        raise TopologyBackendError('offline_injected_pipe_failure')
    requests[0]['handle']['process'].command = fail
    with pytest.raises(TopologyBackendError, match='offline_injected_pipe_failure'):
        backend.publish_concurrent('writers-1', requests)
    assert cleaned == ['w0', 'w1']


def test_worker_prepare_release_real_publish_segment_ast_offline():
    """Execute production command branches without importing ROS or starting workers."""
    import ast
    from pathlib import Path
    import hashlib
    import math
    from contextlib import nullcontext
    from r2f.runtime.rmw_publish_observer import PublishOccurrenceObserver
    import r2f.runtime.topology_worker as module
    tree = ast.parse(Path(module.__file__).read_text())
    condition = lambda n: isinstance(n, ast.If) and ast.unparse(n.test)
    prepare = next(n for n in ast.walk(tree) if condition(n) == "command['command'] == 'prepare_concurrent_publish'")
    release = next(n for n in ast.walk(tree) if condition(n) == "command['command'] == 'release_concurrent_publish'")
    dispatch = next(n for n in ast.walk(tree) if condition(n) == "command['command'] == 'snapshot'")
    wrapper = ast.parse('for command in commands:\n    command_id = command.get("command_id")\n')
    wrapper.body[0].body.extend([prepare, release, dispatch])
    code = compile(ast.fix_missing_locations(wrapper), '<offline-worker-branches>', 'exec')
    emitted, calls = [], []
    class Endpoint:
        handle = nullcontext()  # Public publisher handle protocol; no ROS entity.
        def publish(self, message):
            calls.append((time.monotonic_ns(), message.data))
    from topology_ros_message_fixture import bound_message_codec
    codec = bound_message_codec()
    def emit(kind, **fields):
        emitted.append(dict(event=kind, monotonic_ns=time.monotonic_ns(), **fields))
    # Cyclone uses the public publish path without loading a native observer.
    def unexpected_native_factory(callback):
        pytest.fail('offline public publish must not load native instrumentation')
    observer = PublishOccurrenceObserver(emit, 'rmw_cyclonedds_cpp',
                                         native_factory=unexpected_native_factory)
    assert observer.native is None and observer.unavailable == 'unsupported_rmw'
    emitted.clear()
    env = dict(publish_observer=observer, rmw_identifier='rmw_cyclonedds_cpp',
               commands=[], pending_publish=None, kind='writer', emit=emit, endpoint=Endpoint(), codec=codec,
               SUPPORTED_MESSAGE_TYPE='std_msgs/msg/String',
               math=math, json=json, time=time, MAX_INTERVAL_SEC=30, local_gid='aabb',
               config={'run_id':'offline', 'logical_id':'w0', 'generation':1,
                       'type_name':'std_msgs/msg/String'},
               _sha256_text=lambda s: hashlib.sha256(s.encode()).hexdigest())
    prepare_command = dict(command='prepare_concurrent_publish', command_id=1, concurrency_group='g',
                           sequences=[0,1,2], interval_sec=0, phase='publish_w0', payload=None)
    env['commands'] = [prepare_command]
    exec(code, env)
    assert not calls and [e['event'] for e in emitted] == ['concurrent_publish_ready']
    time.sleep(0.001)  # Waiting at barrier must not inflate any publish interval.
    env['commands'] = [dict(command='release_concurrent_publish', command_id=2,
                            prepared_command_id=1, concurrency_group='g')]
    exec(code, env)
    assert len(calls) == 3
    sends = [e for e in emitted if e['event'] == 'send']
    assert all(e['publish_begin_ns'] <= t <= e['publish_return_ns'] for e,(t,_) in zip(sends,calls))
    assert all(e['publish_begin_ns'] > emitted[0]['monotonic_ns'] for e in sends)
    assert emitted[-1]['event'] == 'command_done' and emitted[-1]['command_id'] == 1


def test_failed_group_retains_side_effects_not_false_no_side_effect_claim(tmp_path):
    from r2f.runtime.topology_worker import TopologyBackendError
    class FailingGroup(SerialGroupFixture):
        def publish_concurrent(self, group, requests):
            super().publish_concurrent(group, requests)
            raise TopologyBackendError('offline_failure_after_publish')
    p = program()
    result = run_topology(p, {'backend':'offline_fake', 'run_id':'failed-fixture', 'domain_id':191},
                          tmp_path, backend=FailingGroup())
    assert any(e['event'] == 'send' for e in result['events'])
    dones = [e for e in result['events'] if e['event'] == 'topology_action_done'
             and e['action']['op'] == 'publish_batch']
    assert len(dones) == 2
    assert all(e['receipt']['satisfied'] is False and e['receipt'].get('no_side_effect') is not True for e in dones)
    assert any(e['event'] == 'topology_tool_error' for e in result['events'])
    assert not result['candidate_eligible']


def test_reordered_raw_ack_cannot_pass_by_preserving_timestamps():
    events, actions = raw_fixture(fixture=False)
    ready = next(e for e in events if e['event'] == 'concurrent_publish_ready')
    done = next(e for e in events if e['event'] == 'command_done')
    ready['event_index'], done['event_index'] = done['event_index'], ready['event_index']
    with pytest.raises(ValueError, match='raw_happens_before'):
        audit_concurrency(events, actions, 'writers-1')
