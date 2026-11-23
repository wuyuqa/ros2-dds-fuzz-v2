"""Offline lifecycle evidence and real scheduling regressions; every runner is mocked.

No ROS import, subprocess, namespace, nft/tc operation or campaign is executed.
"""
import hashlib
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from r2f.runtime import namespace_pair as runtime
from r2f.runtime.discovery_network import normalize_netem_spec
from r2f.runtime.process import WorkerPool


class Clock:
    def __init__(self):
        self.now = 1_000_000_000

    def monotonic_ns(self):
        return self.now

    def monotonic(self):
        return self.now / 1e9

    def time_ns(self):
        return 1_800_000_000_000_000_000 + self.now

    def advance(self, seconds):
        self.now += max(1, round(seconds * 1e9))


class Process:
    def __init__(self, pid):
        self.pid, self.returncode = pid, None
        self.stdin, self.stdout = Mock(), Mock()
        self.stdout.fileno.return_value = pid

    def poll(self):
        return self.returncode


class LifecyclePool:
    """Deterministic command pipe with controllable delayed/wrong evidence."""
    def __init__(self, clock):
        self.clock, self.events = clock, []
        self.workers, self.endpoints, self.generations = {}, {}, {}
        self.commands, self.starts, self.pending, self.wait_initial = [], [], [], []
        self.serial = 0
        self.on_start, self.on_stop, self.on_burst = None, None, None
        self.start('writer')
        self.start('reader')
        self.starts.clear()

    def emit(self, event, **fields):
        token = dict(event=event, monotonic_ns=self.clock.now,
                     namespace_event_index=len(self.events), raw_event_ref=f'mock#{len(self.events)}')
        token.update(fields)
        self.events.append(token)
        return token

    def start(self, role):
        old = self.workers.get(role)
        if old and old.poll() is None:
            raise RuntimeError('worker_role_still_live:' + role)
        generation = self.generations.get(role, 0) + 1
        self.generations[role] = generation
        proc = Process(100 + sum(self.generations.values()))
        self.workers[role] = proc
        identity = dict(pid=proc.pid, endpoint_generation=f'{role}.g{generation}',
                        worker_generation=generation)
        self.starts.append((role, dict(identity)))
        self.emit('process_started', role=role, **identity)
        fields = dict(role=role, endpoint_gid=f'{role}.gid{generation}',
                      rmw_identifier='rmw_cyclonedds_cpp', **identity)
        if self.on_start:
            self.on_start(role, fields)
        else:
            self.endpoints[role] = self.emit('endpoint_created', **fields)

    def command(self, role, command, **fields):
        self.serial += 1
        cid = self.serial
        self.commands.append((role, command, cid, fields))
        identity = runtime._churn_identity(self.endpoints[role])
        self.emit('worker_command_requested', role=role, command=command, command_id=cid, **identity)
        if command == 'stop':
            if self.on_stop:
                self.on_stop(role, cid, identity)
            else:
                self.emit('worker_shutdown', role=role, stop_command_id=cid, **identity)
                self.workers[role].returncode = 0
        elif command == 'publish_burst':
            if self.on_burst:
                self.on_burst(role, cid, identity, fields)
            else:
                for seq in fields['sequences']:
                    self.emit('send', role=role, send_sequence=seq, phase=fields['phase'], **identity)
                self.emit('command_done', role=role, command_id=cid,
                          burst_count=len(fields['sequences']), **identity)
        return cid

    def pump(self, timeout=0.05):
        self.clock.advance(max(0.000001, timeout))
        ready = [item for item in self.pending if item[0] <= self.clock.now]
        self.pending = [item for item in self.pending if item not in ready]
        for _, callback in ready:
            callback()

    def wait(self, predicate, timeout, label):
        self.wait_initial.append((label, predicate()))
        end = self.clock.now + int(timeout * 1e9)
        while not predicate():
            if self.clock.now >= end:
                raise TimeoutError(label)
            self.pump(min(0.01, (end - self.clock.now) / 1e9))

    def later(self, callback, seconds=0.03):
        self.pending.append((self.clock.now + int(seconds * 1e9), callback))


def action(kind, role='reader2', fraction=0.0):
    return dict(kind=kind, role=role, fraction=fraction)


def config(**extra):
    c = dict(mutation_parameters={}, rmw='rmw_cyclonedds_cpp', arm='mutated',
             drop_duration_sec=1.0, startup_timeout_sec=0.5, pre_match_count=2,
             counter_interval_sec=0.05, recovery_timeout_sec=0.2)
    c.update(extra)
    return runtime.normalize_discovery_config(c)


def run_churn(pool, schedule, **kwargs):
    values = dict(window_start_ns=pool.clock.now, window_duration_sec=1.0,
                  startup_timeout_sec=0.2, pre_match_count=2, publish_burst_count=3,
                  publish_burst_interval_sec=0.0)
    values.update(kwargs)
    return runtime.run_window_churn(pool, schedule, pool.events, emit=pool.emit, **values)


class ChurnRoleProfileTests(unittest.TestCase):
    def test_every_scheduled_role_has_hashed_vendor_profile_and_unique_fast_index(self):
        cyclone, fast = 'rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp'
        cases = ((cyclone, None), (fast, None),
                 (cyclone, {'writer': fast, 'reader': cyclone,
                            'reader2': fast, 'reader3': fast}),
                 (fast, {'writer': cyclone, 'reader': fast,
                         'reader2': cyclone, 'reader3': cyclone}))
        # Existing indices remain stable; writer2 must not reuse either the
        # main writer or a reader's FastDDS participant ID.
        roles = ['writer', 'reader'] + [f'reader{i}' for i in range(2, 10)] + ['writer2']
        for base_rmw, rmws in cases:
            with self.subTest(base_rmw=base_rmw, rmws=rmws), tempfile.TemporaryDirectory() as temp:
                extra = {} if rmws is None else {'rmws': rmws}
                c = config(rmw=base_rmw, reader_recreate_in_window=True,
                           reader_recreate_count=runtime.MAX_READER_RECREATE_COUNT,
                           writer_churn_in_window=True, publish_burst_in_window=True, **extra)
                scheduled = {item['role'] for item in runtime.window_churn_schedule(c, 1.0)}
                self.assertEqual(scheduled | {'reader'}, set(roles))
                records, environments = runtime._write_role_profiles(Path(temp), c)
                self.assertEqual(set(records), set(roles))
                self.assertEqual(set(environments), set(roles))
                resolved_env = runtime.role_rmw_environments(c, environments)
                expected = {role: base_rmw for role in roles}
                if rmws is not None:
                    expected.update(rmws)
                    expected['writer2'] = rmws['writer']
                    for role in roles[4:-1]:
                        expected[role] = rmws['reader']
                fast_indices, paths = [], []
                for index, role in enumerate(roles):
                    with self.subTest(role=role):
                        record = records[role]
                        path = Path(record['path'])
                        paths.append(path)
                        payload = path.read_bytes()
                        self.assertEqual(record['sha256'], hashlib.sha256(payload).hexdigest())
                        self.assertEqual(record['kind'], 'explicit_loopback_udp_profile')
                        self.assertEqual(record['rmw'], expected[role])
                        self.assertEqual(path, Path(temp) / 'profiles' / (role + '.xml'))
                        xml = ET.fromstring(payload)
                        if expected[role] == cyclone:
                            self.assertEqual(environments[role], {'CYCLONEDDS_URI': str(path)})
                            self.assertEqual(record['participant_index'], 'auto')
                            self.assertEqual(xml.findtext('.//{*}Transport'), 'udp')
                            self.assertEqual(xml.find('.//{*}NetworkInterface').get('name'), 'lo')
                        else:
                            self.assertEqual(environments[role],
                                             {'FASTRTPS_DEFAULT_PROFILES_FILE': str(path)})
                            self.assertEqual(record['participant_index'], index)
                            self.assertEqual(int(xml.findtext('.//{*}participantID')), index)
                            self.assertEqual(xml.find('.//{*}participant').get('profile_name'),
                                             'r2f_' + role)
                            self.assertEqual(xml.findtext('.//{*}type'), 'UDPv4')
                            self.assertEqual(xml.findtext('.//{*}address'), '127.0.0.1')
                            self.assertEqual(xml.findtext('.//{*}useBuiltinTransports'), 'false')
                            fast_indices.append(record['participant_index'])
                        if rmws is not None:
                            self.assertEqual(resolved_env[role]['RMW_IMPLEMENTATION'], expected[role])
                self.assertEqual(len(fast_indices), len(set(fast_indices)))
                self.assertEqual(len(paths), len(set(paths)))


class FreshnessTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.pool = LifecyclePool(self.clock)
        self.patch = patch.object(runtime, 'time', self.clock)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def old_generation(self):
        self.pool.start('reader2')
        token = self.pool.endpoints['reader2']
        runtime._stop_churn_role(self.pool, self.pool.events, 'reader2', emit=self.pool.emit,
                                 startup_timeout_sec=0.2)
        return token

    def test_old_token_does_not_return_early_even_without_host_event_index(self):
        old = self.old_generation()
        def delayed(role, fields):
            self.pool.later(lambda: self.pool.endpoints.update(
                {role: self.pool.emit('endpoint_created', **fields)}))
        self.pool.on_start = delayed
        start = self.clock.now
        run_churn(self.pool, [action('reader_create')])
        self.assertGreaterEqual(self.clock.now - start, 30_000_000)
        self.assertIn(('reader2_created', False), self.pool.wait_initial)
        receipt = self.pool.events[-1]
        self.assertNotEqual(receipt['generation'], old['endpoint_generation'])
        self.assertEqual(receipt['reader_pid'], self.pool.workers['reader2'].pid)
        self.assertGreater(receipt['endpoint_ref']['local_event_index'], old['namespace_event_index'])

    def test_no_new_generation_times_out_without_recreate_receipt(self):
        self.old_generation()
        self.pool.on_start = lambda role, fields: None
        with self.assertRaisesRegex(TimeoutError, 'reader2_created'):
            run_churn(self.pool, [action('reader_create')])
        self.assertFalse(any(e['event'] == 'discovery_reader_recreated' for e in self.pool.events))

    def test_new_index_with_old_generation_or_wrong_pid_is_not_creation(self):
        old = self.old_generation()
        def wrong_then_right(role, fields):
            self.pool.emit('endpoint_created', **{**fields, 'endpoint_generation': old['endpoint_generation']})
            self.pool.emit('endpoint_created', **{**fields, 'pid': old['pid']})
            self.pool.later(lambda: self.pool.endpoints.update(
                {role: self.pool.emit('endpoint_created', **fields)}))
        self.pool.on_start = wrong_then_right
        run_churn(self.pool, [action('reader_create')])
        self.assertIn(('reader2_created', False), self.pool.wait_initial)
        self.assertEqual(self.pool.events[-1]['generation'], 'reader2.g2')

    def test_wrong_worker_transport_generation_cannot_satisfy_create(self):
        def wrong_transport(role, fields):
            self.pool.emit('endpoint_created', **{**fields, 'worker_generation': 0})
            self.pool.later(lambda: self.pool.endpoints.update(
                {role: self.pool.emit('endpoint_created', **fields)}))
        self.pool.on_start = wrong_transport
        run_churn(self.pool, [action('reader_create')])
        self.assertIn(('reader2_created', False), self.pool.wait_initial)
        self.assertEqual(self.pool.events[-1]['worker_identity']['worker_generation'], 1)

    def test_reinserted_old_index_is_not_fresh_evidence(self):
        old = self.old_generation()
        def only_bad_index(role, fields):
            self.pool.emit('endpoint_created', **fields,
                           namespace_event_index=old['namespace_event_index'])
        self.pool.on_start = only_bad_index
        with self.assertRaisesRegex(TimeoutError, 'reader2_created'):
            run_churn(self.pool, [action('reader_create')])

    def test_destroy_rejects_old_shutdown_wrong_pid_generation_and_command(self):
        old = self.old_generation()
        run_churn(self.pool, [action('reader_create')])
        def delayed_stop(role, cid, identity):
            self.pool.emit('worker_shutdown', role=role, stop_command_id=cid,
                           **{**identity, 'pid': old['pid']})
            self.pool.emit('worker_shutdown', role=role, stop_command_id=cid,
                           **{**identity, 'endpoint_generation': old['endpoint_generation']})
            self.pool.emit('worker_shutdown', role=role, stop_command_id=cid - 1, **identity)
            def finish():
                self.pool.emit('worker_shutdown', role=role, stop_command_id=cid, **identity)
                self.pool.workers[role].returncode = 0
            self.pool.later(finish)
        self.pool.on_stop = delayed_stop
        run_churn(self.pool, [action('reader_destroy')], prior_created=['reader2'])
        self.assertIn(('reader2_stop', False), self.pool.wait_initial)
        receipt = self.pool.events[-1]
        self.assertEqual(receipt['generation'], 'reader2.g2')
        shutdown = self.pool.events[receipt['shutdown_ref']['local_event_index']]
        self.assertEqual(shutdown['stop_command_id'], receipt['command_id'])
        self.assertEqual(shutdown['pid'], receipt['reader_pid'])

    def test_buffered_precommand_shutdown_is_not_fresh_even_if_appended_later(self):
        run_churn(self.pool, [action('reader_create')])
        queued_at = self.clock.now
        self.clock.advance(0.01)
        self.pool.on_stop = lambda role, cid, identity: self.pool.emit(
            'worker_shutdown', role=role, stop_command_id=cid, monotonic_ns=queued_at, **identity)
        with self.assertRaisesRegex(TimeoutError, 'reader2_stop'):
            run_churn(self.pool, [action('reader_destroy')], prior_created=['reader2'])

    def test_missing_shutdown_identity_fails_closed(self):
        run_churn(self.pool, [action('reader_create')])
        self.pool.on_stop = lambda role, cid, identity: self.pool.emit('worker_shutdown', role=role)
        with self.assertRaisesRegex(TimeoutError, 'reader2_stop'):
            run_churn(self.pool, [action('reader_destroy')], prior_created=['reader2'])

    def test_stop_requires_actual_exit_not_just_shutdown_token(self):
        run_churn(self.pool, [action('reader_create')])
        self.pool.on_stop = lambda role, cid, identity: self.pool.emit(
            'worker_shutdown', role=role, stop_command_id=cid, **identity)
        with self.assertRaisesRegex(TimeoutError, 'reader2_exit'):
            run_churn(self.pool, [action('reader_destroy')], prior_created=['reader2'])
        self.assertFalse(any(e['event'] == 'discovery_reader_destroyed' for e in self.pool.events))

    def test_missing_launched_process_identity_cannot_fall_back_to_endpoint_token(self):
        original = self.pool.start
        def missing_process(role):
            original(role)
            self.pool.workers[role] = object()
        self.pool.start = missing_process
        with self.assertRaisesRegex(RuntimeError, 'churn_process_identity_missing'):
            run_churn(self.pool, [action('reader_create')])
        self.assertFalse(any(e['event'] == 'discovery_reader_recreated' for e in self.pool.events))

    def test_rmw_validation_still_uses_this_generations_endpoint(self):
        c = dict(rmw='rmw_cyclonedds_cpp', rmws={'writer': 'rmw_cyclonedds_cpp',
                  'reader': 'rmw_cyclonedds_cpp', 'reader2': 'rmw_fastrtps_cpp'})
        def mixed(role, fields):
            self.pool.endpoints[role] = self.pool.emit('endpoint_created',
                **{**fields, 'rmw_identifier': 'rmw_fastrtps_cpp'})
        self.pool.on_start = mixed
        run_churn(self.pool, [action('reader_create')], config=c)
        runtime._stop_churn_role(self.pool, self.pool.events, 'reader2', emit=self.pool.emit,
                                 startup_timeout_sec=0.2)
        self.pool.on_start = None
        with self.assertRaisesRegex(RuntimeError, 'rmw_identity_mismatch:reader2'):
            run_churn(self.pool, [action('reader_create')], config=c)

    def test_same_role_second_window_resets_destroyed_generation(self):
        first = run_churn(self.pool, [action('reader_create'), action('reader_destroy', fraction=0.1)])
        second = run_churn(self.pool, [action('reader_create')],
                           prior_created=first['created_roles'], prior_destroyed=first['destroyed_roles'])
        self.assertEqual(second['destroyed_roles'], [])
        third = run_churn(self.pool, [action('reader_destroy')],
                          prior_created=second['created_roles'], prior_destroyed=second['destroyed_roles'])
        self.assertEqual(third['destroyed_roles'], ['reader2'])
        receipts = [e for e in self.pool.events if e['event'] == 'discovery_reader_destroyed']
        self.assertEqual([e['generation'] for e in receipts], ['reader2.g1', 'reader2.g2'])
        self.assertNotEqual(receipts[0]['reader_pid'], receipts[1]['reader_pid'])

    def test_live_role_create_is_explicit_error_not_silent_skip(self):
        first = run_churn(self.pool, [action('reader_create')])
        with self.assertRaisesRegex(RuntimeError, 'churn_role_still_live'):
            run_churn(self.pool, [action('reader_create')], prior_created=first['created_roles'])
        self.assertEqual(len(self.pool.starts), 1)

    def test_burst_requires_new_ack_with_current_identity_and_send_receipts(self):
        def wrong_ack(role, cid, identity, fields):
            self.pool.emit('command_done', role=role, command_id=cid - 1, burst_count=3, **identity)
            self.pool.emit('command_done', role=role, command_id=cid, burst_count=3,
                           **{**identity, 'endpoint_generation': 'stale'})
        self.pool.on_burst = wrong_ack
        with self.assertRaisesRegex(TimeoutError, 'writer2_burst'):
            run_churn(self.pool, [action('writer_create', 'writer2'), action('writer_publish', 'writer2')])
        self.assertFalse(any(e['event'] == 'publish_burst_completed' for e in self.pool.events))

    def test_ack_without_real_sends_is_not_a_completed_burst(self):
        self.pool.on_burst = lambda role, cid, identity, fields: self.pool.emit(
            'command_done', role=role, command_id=cid, burst_count=3, **identity)
        with self.assertRaisesRegex(RuntimeError, 'churn_burst_send_evidence_incomplete'):
            run_churn(self.pool, [action('pair_publish', 'writer')])

    def test_lifecycle_wait_is_bounded_by_remaining_window(self):
        self.pool.on_start = lambda role, fields: None
        start = self.clock.now
        with self.assertRaisesRegex(TimeoutError, 'reader2_created'):
            run_churn(self.pool, [action('reader_create', fraction=0.9)],
                       window_duration_sec=0.1, startup_timeout_sec=100)
        self.assertEqual(self.clock.now - start, 100_000_000)

    def test_pair_roles_cannot_be_created_or_destroyed_by_churn(self):
        for item in [action('reader_create', 'reader'), action('reader_destroy', 'reader'),
                     action('writer_destroy', 'writer')]:
            with self.subTest(item=item), self.assertRaisesRegex(RuntimeError, 'invalid_window_churn_role'):
                run_churn(self.pool, [item])


class Trajectory:
    def __init__(self, events, endpoints, emit, start, deadline, matched):
        self.start, self.deadline, self.next_due_ns = start, deadline, deadline
        self.after = None

    def poll(self, pool):
        pass


class Carrier:
    def __init__(self, clock):
        self.clock, self.installs, self.restores = clock, [], []

    def start(self, spec=None, *, match_receipt=None):
        self.installs.append(self.clock.now)
        return dict(configured=True, applied=True, neutral=False, table='mock',
                    counter_snapshot=dict(packets=0, bytes=0))

    def counters(self):
        return dict(hit=True, mutation_hit=True, packets=1, bytes=8)

    def restore(self):
        self.restores.append(self.clock.now)
        return dict(restored=True, table='mock', counter_snapshot=dict(packets=1, bytes=8))

    def confirm_recovery(self, *, activity_receipt):
        return dict(recovery_verified=True)


class RoutedWindowTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.pool = LifecyclePool(self.clock)
        self.carrier = Carrier(self.clock)
        self.base_pair = {role: dict(self.pool.endpoints[role]) for role in ('writer', 'reader')}
        for name, value in [('time', self.clock), ('_RecoveryMatchTrajectory', Trajectory)]:
            p = patch.object(runtime, name, value)
            p.start()
            self.addCleanup(p.stop)
        def delayed(role, fields):
            self.pool.later(lambda: self.pool.endpoints.update(
                {role: self.pool.emit('endpoint_created', **fields)}), 0.05)
        self.pool.on_start = delayed

    def activity(self, since, label, **kwargs):
        return self.pool.emit('discovery_activity_observation', activity={'status': 'mock'},
                              activity_observed=True, window=label)

    def invoke(self, c, netem=True, active=True):
        args = [c, active, dict(planned=active), self.pool.emit,
                lambda name, requires, fn, **kwargs: fn(), self.activity,
                lambda *args, **kwargs: True, lambda anchor: {'matches': {}, 'graphs': {}},
                self.clock.advance, self.carrier, self.pool, self.pool.events, self.base_pair, {},
                {'raw_event_ref': 'mock-precondition'}, {'domain_id': 173}]
        if netem:
            return runtime._run_netem_window(*args)
        collections = [[] for _ in range(7)]
        return runtime._run_window_chain(*args, *collections)

    def netem_config(self, **extra):
        return config(environment_mutation={'kind': 'discovery_netem', 'spec': normalize_netem_spec(
            dict(delay_ms=10, reorder_pct=0, duplicate_pct=0, restore_sec=0, scope='discovery'))}, **extra)

    def assert_window_not_extended(self, seconds=1.0):
        opens = [e for e in self.pool.events if e['event'] == 'window_open']
        self.assertEqual(len(opens), len(self.carrier.restores))
        for opened, restored in zip(opens, self.carrier.restores):
            self.assertEqual(restored - opened['monotonic_ns'], round(seconds * 1e9))

    def test_netem_two_readers_and_128_burst_are_real_calls_and_raw(self):
        c = self.netem_config(reader_recreate_in_window=True, reader_recreate_count=2,
                              reader_recreate_fractions=[0, 0.3],
                              publish_burst_in_window=True, publish_burst_count=128)
        # Exact carrier class from netem-multi-1; no live artifact dependency.
        c['environment_mutation']['spec'] = normalize_netem_spec(dict(
            delay_ms=400, duplicate_pct=100, reorder_pct=100, restore_sec=0, scope='data'))
        result = self.invoke(c)
        self.assertEqual([role for role, _ in self.pool.starts], ['reader2', 'reader3'])
        self.assertEqual([(r, cmd) for r, cmd, _, _ in self.pool.commands],
                         [('writer', 'publish_burst'), ('reader2', 'stop'), ('reader3', 'stop')])
        raw = self.pool.events
        self.assertEqual(len([e for e in raw if e['event'] == 'process_started']), 4)
        self.assertEqual(len([e for e in raw if e['event'] == 'discovery_reader_recreated']), 2)
        burst = next(e for e in raw if e['event'] == 'publish_burst_completed')
        self.assertEqual(len(burst['send_refs']), 128)
        self.assertEqual(len([e for e in raw if e['event'] == 'send' and e['phase'] == 'window_burst']), 128)
        opened = next(e for e in raw if e['event'] == 'window_open')
        restored = next(e for e in raw if e['event'] == 'discovery_netem_end')
        for event in raw:
            if event['event'] in ('discovery_reader_recreated', 'publish_burst_completed'):
                self.assertLess(opened['namespace_event_index'], event['namespace_event_index'])
                self.assertLess(event['namespace_event_index'], restored['namespace_event_index'])
        self.assertEqual(self.base_pair, {r: self.pool.endpoints[r] for r in ('writer', 'reader')})
        self.assertTrue(result['restored'])
        self.assert_window_not_extended()

    def test_netem_restore_cap_is_the_actual_churn_deadline(self):
        c = self.netem_config(reader_recreate_in_window=True, reader_recreate_count=2,
                              reader_recreate_fractions=[0, 0.3])
        c['environment_mutation']['spec']['restore_sec'] = 0.5
        self.invoke(c)
        self.assert_window_not_extended(0.5)
        created = [e for e in self.pool.events if e['event'] == 'discovery_reader_recreated']
        self.assertEqual(len(created), 2)

    def test_netem_writer2_and_reader_destroy_use_same_real_scheduler(self):
        c = self.netem_config(reader_recreate_in_window=True, reader_destroy_fractions=[0.4],
                              writer_churn_in_window=True, publish_burst_count=4)
        self.invoke(c)
        self.assertEqual([r for r, _ in self.pool.starts], ['reader2', 'writer2'])
        names = [e['event'] for e in self.pool.events]
        for required in ('discovery_reader_destroyed', 'discovery_writer_recreated',
                         'publish_burst_completed', 'discovery_writer_destroyed'):
            self.assertIn(required, names)
        self.assert_window_not_extended()

    def test_drop_chain_recreates_same_roles_after_each_cleanup_without_extra_window(self):
        c = config(reader_recreate_in_window=True, reader_recreate_count=2,
                   reader_recreate_fractions=[0, 0.3], publish_burst_in_window=True,
                   publish_burst_count=4, environment_windows=[
                       dict(window_id='first', drop_duration_sec=1),
                       dict(window_id='second', drop_duration_sec=1)])
        self.invoke(c, netem=False)
        created = [e for e in self.pool.events if e['event'] == 'discovery_reader_recreated']
        destroyed = [e for e in self.pool.events if e['event'] == 'discovery_reader_destroyed']
        self.assertEqual([e['generation'] for e in created],
                         ['reader2.g1', 'reader3.g1', 'reader2.g2', 'reader3.g2'])
        self.assertEqual([e['generation'] for e in destroyed], [e['generation'] for e in created])
        self.assertEqual(len([e for e in self.pool.events if e['event'] == 'publish_burst_completed']), 2)
        self.assert_window_not_extended()

    def test_third_window_really_creates_and_destroys_a_third_generation(self):
        c = config(reader_recreate_in_window=True, writer_churn_in_window=True,
                   publish_burst_count=2, environment_windows=[
                       dict(window_id=f'w{i}', drop_duration_sec=1) for i in range(3)])
        self.invoke(c, netem=False)
        created = [e for e in self.pool.events if e['event'] == 'discovery_reader_recreated']
        writers = [e for e in self.pool.events if e['event'] == 'discovery_writer_recreated']
        self.assertEqual([e['generation'] for e in created],
                         ['reader2.g1', 'reader2.g2', 'reader2.g3'])
        self.assertEqual([e['generation'] for e in writers],
                         ['writer2.g1', 'writer2.g2', 'writer2.g3'])
        self.assert_window_not_extended()

    def test_netem_timeout_restores_instead_of_reporting_skipped_actions_as_pass(self):
        self.pool.on_start = lambda role, fields: None
        c = self.netem_config(reader_recreate_in_window=True, startup_timeout_sec=10)
        with self.assertRaisesRegex(TimeoutError, 'reader2_created'):
            self.invoke(c)
        self.assertEqual(len(self.carrier.restores), 1)
        self.assert_window_not_extended()
        names = [e['event'] for e in self.pool.events]
        self.assertNotIn('mutation_effect', names)
        self.assertNotIn('discovery_reader_recreated', names)

    def test_neutral_arm_does_not_claim_requested_churn_was_executed(self):
        self.invoke(self.netem_config(reader_recreate_in_window=True,
                                     publish_burst_in_window=True), active=False)
        self.assertEqual(self.pool.starts, [])
        self.assertEqual(self.pool.commands, [])
        self.assertFalse(any(e['event'] == 'window_churn_action_requested' for e in self.pool.events))

    def test_nonboolean_requests_are_rejected_instead_of_silently_skipped(self):
        for field in ('reader_recreate_in_window', 'publish_burst_in_window'):
            for value in ('true', 1, None):
                with self.subTest(field=field, value=value), self.assertRaisesRegex(RuntimeError, field):
                    self.netem_config(**{field: value})
        self.assertEqual(self.carrier.installs, [])

    def test_netem_multiwindow_is_explicitly_rejected_before_dispatch(self):
        with self.assertRaisesRegex(ValueError, 'environment_windows_not_supported_for_discovery_netem'):
            self.netem_config(environment_windows=[dict(window_id='a', drop_duration_sec=1),
                                                    dict(window_id='b', drop_duration_sec=1)])
        self.assertEqual(self.pool.starts, [])
        self.assertEqual(self.carrier.installs, [])


class WorkerGenerationTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        clock_patch = patch('r2f.runtime.process.time', self.clock)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.events = []
        self.pool = WorkerPool(dict(domain_id=173, rmw='mock', source_repo='/mock'), {},
                               Path(self.temp.name), lambda name, **f: self.events.append(dict(event=name, **f)))
        self.pool.selector.close()
        self.pool.selector = Mock()
        self.pool.selector.select.return_value = []
        self.first, self.second = Process(501), Process(502)
        # Popen PIDs are synthetic: model the OS boundary too, never bind or
        # signal a host process/group (even if PID 501/502 happens to exist).
        # The self-PID entry serves the capability probe; worker pidfds and
        # /proc start identities remain distinct across the two generations.
        pidfds = {os.getpid(): 10000, 501: 10501, 502: 10502}
        identities = {pid: dict(pid=pid, state='S', pgid=pid, sid=pid,
                                start_ticks=pid * 10) for pid in (501, 502)}
        for target, options in (
            ('os.pidfd_open', dict(side_effect=pidfds.__getitem__, create=True)),
            ('signal.pidfd_send_signal', dict(side_effect=ProcessLookupError,
                                             create=True)),
            ('os.close', {}),
            ('_identity', dict(side_effect=identities.__getitem__)),
        ):
            p = patch('r2f.runtime.process.' + target, **options)
            p.start()
            self.addCleanup(p.stop)
        # ESRCH above models no surviving group at cleanup (and is accepted
        # by the capability probe); wait only reaps an already-exited fake.
        for process in (self.first, self.second):
            process.wait = Mock(side_effect=lambda timeout, proc=process: proc.returncode)
        p = patch('r2f.runtime.process.subprocess.Popen', side_effect=[self.first, self.second])
        p.start()
        self.addCleanup(p.stop)
        p = patch('r2f.runtime.process.os.set_blocking')
        p.start()
        self.addCleanup(p.stop)
        # Never leave real file handles open, including on a failing assertion.
        self.addCleanup(lambda: [h.close() for h in self.pool.stderr_handles])

    def feed(self, instance, *events):
        key = SimpleNamespace(data=instance, fileobj=instance['process'].stdout)
        self.pool.selector.select.return_value = [(key, 1)]
        for event in events:
            event.setdefault('monotonic_ns', self.clock.now)
        data = b''.join((json.dumps(e) + '\n').encode() for e in events)
        with patch('r2f.runtime.process.os.read', return_value=data):
            self.pool.pump(0)
        self.pool.selector.select.return_value = []

    def test_metadata_stdout_and_stop_state_are_per_process_not_role(self):
        self.pool.start('reader2')
        old = self.pool.instances[0]
        self.feed(old, dict(event='endpoint_created', role='reader2', pid=501,
                            endpoint_generation='old', endpoint_gid='old-gid'))
        stop_id = self.pool.command('reader2', 'stop')
        self.first.returncode = 0
        self.pool.start('reader2')
        current = self.pool.instances[1]
        self.feed(current, dict(event='endpoint_created', role='reader2', pid=502,
                                endpoint_generation='new', endpoint_gid='new-gid'))
        # A late old pipe is still old, even after the same role was restarted.
        self.feed(old, dict(event='worker_shutdown', role='reader2', pid=501,
                            endpoint_generation='old'))
        late = self.events[-1]
        self.assertEqual((late['pid'], late['endpoint_generation'], late['worker_generation'],
                          late['stop_command_id']), (501, 'old', 1, stop_id))
        self.assertFalse(self.pool.is_stopped('reader2'))
        self.assertEqual(self.pool.metadata[0]['returncode'], 0)
        self.assertNotIn('returncode', self.pool.metadata[1])
        self.assertNotEqual(self.pool.metadata[0]['stderr_path'], self.pool.metadata[1]['stderr_path'])
        self.pool.command('reader2', 'stop')
        self.second.returncode = 0
        self.pool.close()
        self.assertEqual([m['pid'] for m in self.pool.metadata], [501, 502])
        self.assertEqual([m['returncode'] for m in self.pool.metadata], [0, 0])
        self.assertEqual([e['pid'] for e in self.events if e['event'] == 'process_exit'], [501, 502])
        self.first.stdout.close.assert_called_once()
        self.second.stdout.close.assert_called_once()

    def test_live_same_role_is_rejected_before_spawning(self):
        self.pool.start('reader2')
        with self.assertRaisesRegex(RuntimeError, 'worker_role_still_live'):
            self.pool.start('reader2')
        self.assertEqual(len(self.pool.instances), 1)

    def test_old_pipe_cannot_claim_new_process_identity_or_generation(self):
        self.pool.start('reader2')
        old = self.pool.instances[0]
        self.feed(old, dict(event='endpoint_created', pid=501, endpoint_generation='old', endpoint_gid='x'))
        self.feed(old, dict(event='worker_shutdown', pid=502, endpoint_generation='new'),
                  dict(event='command_done', pid=501, endpoint_generation='new', command_id=1))
        self.assertEqual([e['event'] for e in self.events[-2:]],
                         ['worker_stdout_diagnostic', 'worker_stdout_diagnostic'])

    def test_buffered_shutdown_from_before_stop_is_not_its_ack(self):
        self.pool.start('reader2')
        instance = self.pool.instances[0]
        self.feed(instance, dict(event='endpoint_created', pid=501,
                                  endpoint_generation='old', endpoint_gid='x'))
        queued_at = self.clock.now
        self.clock.advance(0.01)
        self.pool.command('reader2', 'stop')
        self.feed(instance, dict(event='worker_shutdown', pid=501,
                                endpoint_generation='old', monotonic_ns=queued_at))
        self.assertIsNone(self.events[-1]['stop_command_id'])
        self.assertEqual(self.events[-1]['stop_command_binding'],
                         'controller_pipe_request_not_worker_ack')

    def test_old_generation_nonzero_exit_is_not_hidden_by_restart(self):
        self.pool.start('reader2')
        self.pool.command('reader2', 'stop')
        self.first.returncode = 7
        self.pool.start('reader2')
        with self.assertRaisesRegex(RuntimeError, 'worker_exited:.*7'):
            self.pool.wait(lambda: False, 0.01, 'mock')


if __name__ == '__main__':
    unittest.main()
