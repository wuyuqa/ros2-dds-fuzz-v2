"""Public controller regression; all OS/worker/carrier I/O is virtual.

The real host dispatch, namespace controller, recovery gates, delivery loop and
raw serialization run. No ROS, subprocess, netns, nft, signals or replay runs.
"""
import contextlib
import io
import hashlib
import json
from unittest.mock import patch

import pytest

from r2f.instrumentation import sedp
from r2f.runtime import namespace_pair as ns, ros2_pair as pair


class Clock:
    def __init__(self):
        self.now = 10_000_000_000

    def advance(self, seconds):
        self.now += max(1, round(seconds * 1e9))

    def monotonic_ns(self):
        return self.now

    def monotonic(self):
        return self.now / 1e9

    def time_ns(self):
        return 1_700_000_000_000_000_000 + self.now


class OfflineController:
    def __init__(self, *, hit=True, restore=True, activity=True, stale_final=False):
        self.clock = Clock()
        self.hit, self.restore_ok, self.activity_ok = hit, restore, activity
        self.stale_final = stale_final
        self.restores, self.installs = [], []
        self.source = {'head': '33079b5db172a2886e4a96a0a036aa00a50b657d',
                       'behavior_tree_sha256': 'offline-controller-fixture'}

    def carrier(self, domain):
        owner = self

        class Carrier:
            def setup(self):
                return {'health_ok': True}

            def pair_bindings(self, pids):
                return {'verified': True, 'worker_pids': pids}

            def start(self, spec=None, **kwargs):
                owner.clock.advance(.007)
                owner.installs.append(owner.clock.now)
                return {'configured': True, 'applied': not bool(spec and spec['neutral']),
                        'neutral': bool(spec and spec['neutral']),
                        'table': 'offline-table',
                        'counter_snapshot': {'packets': 0, 'bytes': 0}}

            def counters(self):
                return {'hit': owner.hit, 'mutation_hit': owner.hit,
                        'packets': int(owner.hit), 'bytes': 80 * int(owner.hit),
                        'group_counters_available': True,
                        'group_counters': {'data': {'packets': int(owner.hit),
                                                    'bytes': 80 * int(owner.hit)}}}

            def restore(self):
                owner.clock.advance(.011)
                owner.restores.append(owner.clock.now)
                if not owner.restore_ok:
                    raise ns.DiscoveryNetworkError('offline_restore_failed',
                                                   receipt={'restored': False})
                return {'restored': owner.restore_ok, 'hit': owner.hit,
                        'table': 'offline-table',
                        'counter_snapshot': self.counters(), 'only_noqueue_root': owner.restore_ok}

            def confirm_recovery(self, **kwargs):
                return {'recovery_verified': owner.restore_ok, 'restored': owner.restore_ok}

        return Carrier()

    def observer(self, spec):
        owner = self

        class Observer:
            def start(self):
                return {'status': 'ready'}

            def snapshot_activity(self, *, since_ns, participant_prefixes):
                owner.clock.advance(.08)
                return {'status': 'observed' if owner.activity_ok else 'unknown',
                        'health': {'status': 'ok'}, 'window_buffer_retained': True,
                        'participant_prefixes': participant_prefixes,
                        'since_ns': since_ns, 'until_ns': owner.clock.time_ns(),
                        'events': [{'source_guid_prefix': prefix,
                                    'unix_ns': owner.clock.time_ns(),
                                    'received_monotonic_ns': owner.clock.now,
                                    'clock': 'kernel_timestampns', 'kind': 'SEDP_DATA',
                                    'builtin_writer_id': '000003c2',
                                    'raw_ref': {'frame_sha256': 'offline-frame'}}
                                   for prefix in participant_prefixes]}

            def stop(self):
                return {'health': {'status': 'ok'}, 'thread_stopped': True, 'socket_closed': True}

        return Observer()

    def pool(self, config, environment, output_dir, emit, **kwargs):
        owner = self

        class Pool:
            def __init__(self):
                self.pending, self.metadata, self.endpoints = [], [], {}
                self.serial = 0
                self.check_owner = True

            def start(self, role):
                i = ('writer', 'reader').index(role)
                self.endpoints[role] = dict(role=role, pid=101 + i, process_start_ticks=501 + i,
                    endpoint_generation=role + '-generation', endpoint_gid=('%02x' % (i + 1)) * 16,
                    rmw_identifier=config['rmw'], run_id=config['run_id'], domain_id=config['domain_id'],
                    created_monotonic_ns=owner.clock.now, topic=config['topic'],
                    message_type='std_msgs/msg/String', qos=config['qos'])
                emit('endpoint_created', **self.endpoints[role])
                self.metadata.append({'role': role, 'pid': 101 + i, 'returncode': 0})

            def command(self, role, name, **fields):
                self.serial += 1
                self.pending.append((role, name, fields, self.serial))
                return self.serial

            def pump(self, timeout=.05):
                owner.clock.advance(timeout)
                pending, self.pending = self.pending, []
                for role, name, fields, cid in pending:
                    identity = {k: v for k, v in self.endpoints[role].items()
                                if k not in ('created_monotonic_ns', 'topic', 'message_type', 'qos')}
                    common = dict(identity, command_id=cid)
                    if name == 'snapshot':
                        sampled = owner.clock.now
                        # An adversarial cached snapshot after delivery must still
                        # fail the real final-boundary gate, even with fresh ACKs.
                        if owner.stale_final and getattr(owner, 'delivery_started', False):
                            sampled = owner.clock.now - 10_000_000_000
                        emit('match_snapshot', **common, sampled_monotonic_ns=sampled, matched_count=1)
                        emit('graph_snapshot', **common,
                             publishers=[{'endpoint_gid': self.endpoints['writer']['endpoint_gid']}],
                             subscriptions=[{'endpoint_gid': self.endpoints['reader']['endpoint_gid']}])
                    elif name == 'publish':
                        sequence = fields['send_sequence']
                        if fields['phase'] == 'post_restore':
                            owner.delivery_started = True
                        if fields.get('omit'):
                            emit('harness_omission', **common, send_sequence=sequence, phase=fields['phase'])
                        else:
                            emit('send', **common, send_sequence=sequence, phase=fields['phase'], publish_returned=True)
                            if fields['phase'] == 'post_restore':
                                emit('receive', role='reader', send_sequence=sequence, phase=fields['phase'])
                    else:
                        raise AssertionError(name)
                    emit('command_done', **common)

            def wait(self, predicate, timeout, label):
                deadline = owner.clock.now + round(timeout * 1e9)
                while not predicate():
                    if owner.clock.now >= deadline:
                        raise TimeoutError(label)
                    self.pump(min(.01, (deadline - owner.clock.now) / 1e9))

            def close(self):
                self.pump(0)

        return Pool()

    def supervise(self, config, output_dir, environment, lease, source, emit):
        # Replace only the OS child transport, executing its complete production
        # controller and forwarding its actual JSON tokens to the public host.
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            inner = ns._body({'config': config, 'output_dir': str(output_dir / 'inner'),
                              'source': source, 'leases': []}, None)
        self.inner_events = [json.loads(line) for line in captured.getvalue().splitlines()]
        for token in self.inner_events:
            fields = dict(token)
            emit(fields.pop('event'), **fields)
        emit('namespace_controller_exited', returncode=0)
        return inner

    @contextlib.contextmanager
    def installed(self):
        self.inner_events = []
        class Lease:
            receipts = []

            def __init__(self, *args):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        with contextlib.ExitStack() as stack:
            replacements = {'time': self.clock, 'LeaseSet': Lease,
                'execution_chain_identity': lambda: self.source,
                'check_environment': lambda c: {'ok': True},
                'activated_environment': lambda c: (None, {}),
                '_supervise': self.supervise,
                'validate_host_leases': lambda request: {
                    'net_namespace': 'net:[101]', 'host_net_namespace': 'net:[1]',
                    'user_namespace': 'user:[102]', 'host_user_namespace': 'user:[2]',
                    'host_uid': 1000, 'host_cap_eff': 0},
                'probe_rtps_ports': lambda domain: {'ok': True},
                'write_profiles': lambda *args: ({}, {}),
                'assert_isolated_namespace': lambda: {'offline': True},
                'DiscoveryDropController': self.carrier, 'DiscoveryNetemController': self.carrier,
                'OwnerCheckedPool': self.pool}
            for name, value in replacements.items():
                stack.enter_context(patch.object(ns, name, value))
            stack.enter_context(patch.object(pair, 'time', self.clock))
            stack.enter_context(patch.object(sedp, 'PassiveSedpObserver', self.observer))
            yield


def config(arm='baseline', **extra):
    return dict(target_id='discovery.interruption_recovery', target_revision=0,
                mutation_kind='discovery_drop', carrier='wire_proxy', arm=arm,
                run_id='offline-final-boundary', domain_id=114,
                evidence_purpose='offline_controller_regression',
                mutation_parameters={'drop_duration_sec': .12, 'recovery_timeout_sec': .3,
                                     'counter_interval_sec': .03}, **extra)


def test_required_baseline_finishes_with_restore_bound_final_graph(tmp_path):
    controller = OfflineController()
    with controller.installed():
        result = pair.run_pair(config(), tmp_path)
    assert result['runtime_status'] == 'completed', result['reason']
    final = next(r for r in result['observation_receipts'] if r['step_id'] == 'observe_final_graph')
    assert final['satisfied'] is True
    events = result['events']
    boundary = next(e for e in events if e['event'] == 'discovery_restore_boundary')
    by_ref = {e['raw_event_ref']: e for e in events}
    for role in ('writer', 'reader'):
        snapshot = by_ref[final['evidence']['matches'][role]]
        assert snapshot['sampled_monotonic_ns'] >= boundary['post_restore_monotonic_ns']
        assert by_ref[final['evidence']['graphs'][role]]['command_id'] == snapshot['command_id']


def branch_config(branch, arm):
    extra = {}
    if branch in ('single', 'chained'):
        extra['environment_windows'] = [
            {'window_id': 'w%d' % i, 'drop_duration_sec': .12,
             'trigger_anchor': 'fresh_match' if i == 0 else 'previous_restore_confirmed',
             'restore_interval_sec': .02}
            for i in range(1 if branch == 'single' else 3)]
    if branch.startswith('netem'):
        extra['environment_mutation'] = {'kind': 'discovery_netem', 'spec': {
            'delay_ms': 0 if branch == 'netem_neutral' else 10,
            'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': .12,
            'scope': 'data' if branch == 'netem_covered' else 'discovery'}}
        extra['netem_covers_delivery'] = branch == 'netem_covered'
    return config(arm, **extra)


def run_public(entry, value, output_dir):
    if entry == 'run_pair':
        return pair.run_pair(value, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {'real_execution': False, 'runtime_status': 'blocked', 'status': 'blocked',
              'reason': None, 'events': [], 'facts': {}, 'environment': {},
              'precondition_receipts': [], 'observation_receipts': [],
              'mutation_receipt': dict.fromkeys(pair.RECEIPT_KEYS, False),
              'execution': None, 'execution_metadata': None, 'raw_path': None,
              'raw_sha256': None, 'capabilities': pair.runtime_capabilities()}
    return ns.run_discovery_pair(pair.normalize_config(value), output_dir, result)


BRANCHES = ('legacy', 'single', 'chained', 'netem', 'netem_neutral', 'netem_covered')


@pytest.mark.parametrize('entry', ('run_pair', 'run_discovery_pair'))
@pytest.mark.parametrize('arm', ('baseline', 'transparent', 'mutated', 'negative'))
@pytest.mark.parametrize('branch', BRANCHES)
def test_public_final_graph_is_fresh_after_selected_branch_restore(tmp_path, entry, arm, branch):
    controller = OfflineController()
    with controller.installed():
        result = run_public(entry, branch_config(branch, arm), tmp_path)
    assert result['runtime_status'] == 'completed', result['reason']
    events = result['events']
    raw = (tmp_path / 'raw.jsonl').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == result['raw_sha256']
    assert [json.loads(line) for line in raw.splitlines()] == events
    by_ref = {e['raw_event_ref']: e for e in events}
    final = next(r for r in result['observation_receipts'] if r['step_id'] == 'observe_final_graph')
    assert final['satisfied'] is True
    before, after = (by_ref[final[k]['raw_event_ref']] for k in ('before', 'after'))
    boundaries = [e for e in events if e['event'] in ('discovery_restore_boundary', 'delivery_restore_boundary')]
    assert len([e for e in boundaries if e['event'] == 'discovery_restore_boundary']) == (3 if branch == 'chained' else 1)
    delivery_open = next(e for e in events if e['event'] == 'window_open' and e.get('window') == 'post_restore_delivery')
    delivery_close = next(e for e in events if e['event'] == 'window_close' and e.get('window') == 'post_restore_delivery')
    assert boundaries[0]['event_index'] < delivery_open['event_index'] < delivery_close['event_index'] < before['event_index']
    for boundary in boundaries:
        assert boundary['event_index'] < before['event_index']
        if arm == 'mutated':
            preceding_restores = [e for e in events if e['event'] in (
                'discovery_drop_end', 'discovery_netem_end', 'delivery_netem_end')
                and e['event_index'] < boundary['event_index']]
            assert preceding_restores
            restored_event = preceding_restores[-1]
            assert restored_event['receipt']['restored'] is True
            assert restored_event['monotonic_ns'] in controller.restores
            assert restored_event['monotonic_ns'] <= boundary['post_restore_monotonic_ns']
    for role in ('writer', 'reader'):
        snapshot = by_ref[final['evidence']['matches'][role]]
        graph = by_ref[final['evidence']['graphs'][role]]
        request = next(e for e in events if e['event'] == 'match_snapshot_requested'
                       and e.get('role') == role and e.get('command_id') == snapshot['command_id'])
        ack = next(e for e in events if e['event'] == 'command_done'
                   and e.get('role') == role and e.get('command_id') == snapshot['command_id'])
        assert before['event_index'] < request['event_index'] < snapshot['event_index'] < graph['event_index'] < ack['event_index'] < after['event_index']
        assert graph['command_id'] == snapshot['command_id']
        assert snapshot['sampled_monotonic_ns'] >= max(b['post_restore_monotonic_ns'] for b in boundaries)
        assert ack['pid'] == graph['pid'] == snapshot['pid']
    receipt = result['mutation_receipt']
    if arm != 'mutated':
        assert controller.installs == []
        assert receipt['applied'] is receipt['hit'] is False
        # Netem's existing neutral observation may record an effect, never a hit.
        assert receipt['effect_observed'] is branch.startswith('netem')
    else:
        assert len(controller.installs) == (3 if branch == 'chained' else 2 if branch == 'netem_covered' else 1)
        assert receipt['hit'] is (branch != 'netem_neutral')
    if branch in ('single', 'chained'):
        recovery_steps = [r for r in result['observation_receipts']
                          if r['step_id'].startswith('observe_recovery_graph_')]
        assert len(recovery_steps) == len(boundaries)
        for record, boundary in zip(recovery_steps, boundaries):
            for ref in record['evidence']['matches'].values():
                assert by_ref[ref]['sampled_monotonic_ns'] >= boundary['post_restore_monotonic_ns']
        effect = next(e for e in events if e['event'] == ('mutation_effect' if arm == 'mutated' else 'neutral_control_observation'))
        assert effect['window_hits'] == [arm == 'mutated'] * len(boundaries)
    if branch == 'netem_covered' and arm == 'mutated':
        overlap = next(e for e in events if e['event'] == 'netem_delivery_overlap')
        assert overlap['delivery_open_ref'] == delivery_open['raw_event_ref']
        assert overlap['delivery_close_ref'] == delivery_close['raw_event_ref']
        assert by_ref[overlap['restore_event_ref']]['event_index'] > delivery_close['event_index']
        assert overlap['boundary_event_ref'] == boundaries[-1]['raw_event_ref']
    omissions = [e for e in events if e['event'] == 'harness_omission']
    if arm == 'negative':
        assert len(omissions) == 1
        fault = next(e for e in events if e['event'] == 'fault_control_receipt')
        assert fault['applied'] is True and fault['candidate_eligible'] is False
        assert fault['omission_event_ref'] == omissions[0]['raw_event_ref']
        assert result['facts']['candidate_eligible'] is False
    else:
        assert not omissions


@pytest.mark.parametrize('branch', ('legacy', 'single', 'chained', 'netem', 'netem_covered'))
def test_no_hit_cannot_become_mutation_effect(tmp_path, branch):
    controller = OfflineController(hit=False)
    with controller.installed():
        result = pair.run_pair(branch_config(branch, 'mutated'), tmp_path)
    assert result['runtime_status'] == 'completed', result['reason']
    assert result['mutation_receipt']['hit'] is False
    assert result['mutation_receipt']['effect_observed'] is False


@pytest.mark.parametrize('branch', ('legacy', 'single', 'chained', 'netem', 'netem_covered'))
def test_failed_restore_never_reports_recovery_observed(tmp_path, branch):
    controller = OfflineController(restore=False)
    with controller.installed():
        result = pair.run_pair(branch_config(branch, 'mutated'), tmp_path)
    assert result['facts']['discovery_restore_verified'] is not True
    assert result['facts']['discovery_recovery_status'] == 'unknown'
    assert result['mutation_receipt']['effect_observed'] is False
    assert result['runtime_status'] == 'failed'
    assert 'offline_restore_failed' in result['reason']
    assert len(controller.installs) == 1
    assert not any(e['event'] == 'discovery_restore_boundary' for e in result['events'])


@pytest.mark.parametrize('arm', ('baseline', 'mutated'))
def test_stale_final_snapshot_is_unknown_not_recovery_success(tmp_path, arm):
    controller = OfflineController(stale_final=True)
    with controller.installed():
        result = pair.run_pair(config(arm), tmp_path)
    assert result['runtime_status'] == 'completed', result['reason']
    final = next(r for r in result['observation_receipts'] if r['step_id'] == 'observe_final_graph')
    assert final['satisfied'] is False
    assert result['facts']['recovery_fresh_count_graph'] is False
    assert result['facts']['discovery_recovery_status'] == 'unknown'
    assert any(e['event'] == 'discovery_observation_unknown' for e in result['events'])


def test_missing_pair_activity_cannot_be_replaced_by_fresh_graph_and_delivery(tmp_path):
    controller = OfflineController(activity=False)
    with controller.installed():
        result = pair.run_pair(config('mutated'), tmp_path)
    assert result['runtime_status'] == 'completed', result['reason']
    assert result['facts']['recovery_fresh_count_graph'] is True
    assert result['facts']['discovery_recovery_activity_observed'] is False
    assert result['facts']['discovery_recovery_status'] == 'unknown'
    assert result['mutation_receipt']['effect_observed'] is False
