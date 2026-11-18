"""Offline F3 endpoint-lifecycle contracts; no ROS, namespace, nft or campaign."""
import unittest
import tempfile
from unittest.mock import patch

from r2f.runtime.namespace_pair import (
    MAX_PUBLISH_BURST_COUNT,
    MAX_READER_RECREATE_COUNT,
    normalize_discovery_config,
    reader_churn_plan,
    reader_destroy_schedule,
    run_discovery_pair,
    run_window_churn,
    window_churn_schedule,
)
from r2f.runtime.process import WorkerPool


def base_config(**extra):
    config = {
        'mutation_parameters': {},
        'reader_recreate_in_window': True,
        'reader_recreate_count': 1,
    }
    config.update(extra)
    return config


class ChurnPlanValidation(unittest.TestCase):
    def test_reader_count_one_to_eight_accepted(self):
        for count in range(1, MAX_READER_RECREATE_COUNT + 1):
            with self.subTest(count=count):
                c = normalize_discovery_config(base_config(reader_recreate_count=count))
                plan = reader_churn_plan(c, 4.0)
                self.assertEqual([p['role'] for p in plan],
                                 ['reader%d' % (i + 2) for i in range(count)])
                self.assertEqual([p['generation'] for p in plan], list(range(1, count + 1)))

    def test_reader_count_out_of_range_or_wrong_type_rejected(self):
        for count in (0, 9, -1, True, 2.0, '2', None):
            with self.subTest(count=count), self.assertRaisesRegex(RuntimeError, 'reader_recreate_count'):
                normalize_discovery_config(base_config(reader_recreate_count=count))

    def test_fractions_take_priority_and_are_strictly_increasing(self):
        c = normalize_discovery_config(base_config(
            reader_recreate_count=3,
            reader_recreate_fractions=[0.0, 0.25, 0.75],
            reader_recreate_at_fraction=0.5,
            reader_recreate_interval_sec=9.0,
        ))
        plan = reader_churn_plan(c, 2.0)
        self.assertEqual([p['delay_sec'] for p in plan], [0.0, 0.5, 1.5])
        self.assertEqual([round(p['fraction'], 6) for p in plan], [0.0, 0.25, 0.75])

    def test_fraction_shape_type_and_order_errors_name_the_field(self):
        cases = [
            ([0.1], 'reader_recreate_fractions length'),
            ([0.1, 0.2, 0.3], 'reader_recreate_fractions length'),
            ([0.2, 0.1], 'reader_recreate_fractions must be strictly increasing'),
            ([0.1, 0.1], 'reader_recreate_fractions must be strictly increasing'),
            ([0.1, 1.0], r'reader_recreate_fractions\[1\] must be in \[0,1\)'),
            ([0.1, -0.1], r'reader_recreate_fractions\[1\] must be in \[0,1\)'),
            ([0.1, True], r'reader_recreate_fractions\[1\] must be a number'),
            ([0.1, '0.2'], r'reader_recreate_fractions\[1\] must be a number'),
            ('0.1,0.2', 'reader_recreate_fractions must be a list'),
        ]
        for fractions, message in cases:
            with self.subTest(fractions=fractions), self.assertRaisesRegex(RuntimeError, message):
                normalize_discovery_config(base_config(
                    reader_recreate_count=2, reader_recreate_fractions=fractions))

    def test_legacy_formula_is_preserved(self):
        c = normalize_discovery_config(base_config(
            reader_recreate_count=2, reader_recreate_at_fraction=0.1,
            reader_recreate_interval_sec=0.5))
        self.assertEqual([p['delay_sec'] for p in reader_churn_plan(c, 4.0)], [0.4, 0.9])
        c = normalize_discovery_config(base_config(reader_recreate_count=3))
        self.assertEqual([round(p['fraction'], 6) for p in reader_churn_plan(c, 10.0)],
                         [0.0, 0.2, 0.4])

    def test_destroy_schedule_bounds_order_and_role_mapping(self):
        c = normalize_discovery_config(base_config(
            reader_recreate_count=3, reader_destroy_fractions=[0.3, 0.6]))
        self.assertEqual(reader_destroy_schedule(c, 3), [
            {'role': 'reader2', 'index': 0, 'fraction': 0.3},
            {'role': 'reader3', 'index': 1, 'fraction': 0.6}])
        with self.assertRaisesRegex(RuntimeError, 'reader_destroy_fractions length'):
            normalize_discovery_config(base_config(
                reader_recreate_count=2, reader_destroy_fractions=[0.1, 0.2, 0.3]))
        with self.assertRaisesRegex(RuntimeError, 'strictly increasing'):
            normalize_discovery_config(base_config(
                reader_recreate_count=2, reader_destroy_fractions=[0.5, 0.2]))
        with self.assertRaisesRegex(RuntimeError, 'later than reader2 creation'):
            normalize_discovery_config(base_config(
                reader_recreate_count=2, reader_recreate_fractions=[0.4, 0.6],
                reader_destroy_fractions=[0.3]))

    def test_writer_churn_and_burst_bounds_are_strict(self):
        c = normalize_discovery_config(base_config(publish_burst_count=4096))
        self.assertEqual(c['publish_burst_count'], 4096)
        for value in (0, 4097, True, 32.0):
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, 'publish_burst_count'):
                normalize_discovery_config(base_config(publish_burst_count=value))
        for value in (1, 'yes', None, 0):
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, 'writer_churn_in_window'):
                normalize_discovery_config(base_config(writer_churn_in_window=value))

    def test_schedule_orders_create_before_destroy_and_places_writer2(self):
        c = normalize_discovery_config(base_config(
            reader_recreate_count=2, reader_recreate_fractions=[0.0, 0.4],
            reader_destroy_fractions=[0.2, 0.6], writer_churn_in_window=True))
        schedule = window_churn_schedule(c, 2.0)
        kinds = [(item['kind'], item['role']) for item in schedule]
        self.assertLess(kinds.index(('reader_create', 'reader2')),
                        kinds.index(('reader_destroy', 'reader2')))
        self.assertLess(kinds.index(('reader_create', 'reader3')),
                        kinds.index(('reader_destroy', 'reader3')))
        self.assertIn(('writer_create', 'writer2'), kinds)
        self.assertIn(('writer_publish', 'writer2'), kinds)
        self.assertIn(('writer_destroy', 'writer2'), kinds)
        self.assertEqual([item['fraction'] for item in schedule],
                         sorted(item['fraction'] for item in schedule))


class FakeLifecycleProcess:
    def __init__(self, pid):
        self.pid, self.returncode = pid, None

    def poll(self):
        return self.returncode


class FakeLifecyclePool:
    def __init__(self, clock, events, emit):
        self.clock, self.events, self.emit = clock, events, emit
        self.workers = {'writer': FakeLifecycleProcess(100),
                        'reader': FakeLifecycleProcess(101)}
        self.started, self.commands, self.stopped = [], [], set()
        self.generations, self.identities = {}, {}

    def start(self, role):
        previous = self.workers.get(role)
        if previous is not None and previous.poll() is None:
            raise AssertionError('offline role still live: ' + role)
        self.started.append(role)
        generation = self.generations.get(role, 0) + 1
        self.generations[role] = generation
        process = FakeLifecycleProcess(1000 + len(self.started))
        self.workers[role] = process
        self.stopped.discard(role)
        identity = dict(pid=process.pid, worker_generation=generation,
                        endpoint_generation=f'{role}-generation-{generation}')
        self.identities[role] = identity
        self.emit('process_started', role=role, **identity)
        self.emit('endpoint_created', role=role, endpoint_gid=f'{role}-gid-{generation}',
                  **identity)
        return role

    def command(self, role, command, **fields):
        self.commands.append((role, command, fields))
        command_id = len(self.commands)
        identity = self.identities[role]
        self.emit('worker_command_requested', role=role, command=command,
                  command_id=command_id, **identity)
        if command == 'stop':
            self.emit('worker_shutdown', role=role, stop_command_id=command_id,
                      **identity)
            self.workers[role].returncode = 0
            self.stopped.add(role)
        elif command == 'publish_burst':
            for sequence in fields['sequences']:
                self.emit('send', role=role, send_sequence=sequence,
                          phase=fields['phase'], **identity)
            self.emit('command_done', role=role, command_id=command_id,
                      burst_count=len(fields['sequences']), **identity)
        else:
            raise AssertionError('unexpected offline command: ' + command)
        return command_id

    def wait(self, predicate, timeout, label, **kwargs):
        if not predicate():
            raise AssertionError('offline predicate not satisfied: ' + label)

    def pump(self, timeout=0.05):
        # Advance the virtual clock so due-time scheduling terminates offline.
        self.clock.now += max(1_000_000, int(float(timeout) * 1_000_000_000))

    def is_stopped(self, role):
        return role in self.stopped


class FakeClock:
    def __init__(self):
        self.now = 1_000_000_000

    def monotonic(self):
        return self.now / 10**9

    def monotonic_ns(self):
        return self.now


class InWindowLifecycleOrder(unittest.TestCase):
    def test_reader_destroy_event_follows_create_and_shutdown(self):
        clock = FakeClock()
        events = []
        def emit(event, **fields):
            fields.setdefault('monotonic_ns', clock.now)
            fields.setdefault('namespace_event_index', len(events))
            fields.setdefault('raw_event_ref', f'mock#{len(events)}')
            token = dict(event=event, **fields)
            events.append(token)
            return token
        pool = FakeLifecyclePool(clock, events, emit)
        schedule = [
            {'kind': 'reader_create', 'fraction': 0.1, 'role': 'reader2'},
            {'kind': 'reader_destroy', 'fraction': 0.4, 'role': 'reader2'},
        ]
        with patch('r2f.runtime.namespace_pair.time', clock):
            run_window_churn(pool, schedule, events, window_start_ns=1_000_000_000,
                             window_duration_sec=3.0,
                             startup_timeout_sec=1, emit=emit,
                             pre_match_count=2, publish_burst_count=8,
                             publish_burst_interval_sec=0.0)
        names = [event['event'] for event in events]
        create = names.index('endpoint_created')
        recreate = names.index('discovery_reader_recreated')
        shutdown = names.index('worker_shutdown')
        destroyed = names.index('discovery_reader_destroyed')
        self.assertLess(create, recreate)
        self.assertLess(recreate, shutdown)
        self.assertLess(shutdown, destroyed)
        self.assertEqual(events[destroyed]['reason'], 'planned_in_window_churn')
        self.assertIs(events[destroyed]['pair_member'], False)

    def test_writer2_uses_own_lifecycle_and_is_never_pair_member(self):
        clock = FakeClock()
        events = []
        def emit(event, **fields):
            fields.setdefault('monotonic_ns', clock.now)
            fields.setdefault('namespace_event_index', len(events))
            fields.setdefault('raw_event_ref', f'mock#{len(events)}')
            token = dict(event=event, **fields)
            events.append(token)
            return token
        pool = FakeLifecyclePool(clock, events, emit)
        schedule = [
            {'kind': 'writer_create', 'fraction': 0.0, 'role': 'writer2'},
            {'kind': 'writer_publish', 'fraction': 0.0, 'role': 'writer2'},
            {'kind': 'writer_destroy', 'fraction': 0.5, 'role': 'writer2'},
        ]
        with patch('r2f.runtime.namespace_pair.time', clock):
            run_window_churn(pool, schedule, events, window_start_ns=1_000_000_000,
                             window_duration_sec=1.0,
                             startup_timeout_sec=1, emit=emit,
                             pre_match_count=1, publish_burst_count=8,
                             publish_burst_interval_sec=0.0)
        writer_events = [e for e in events if e.get('role') == 'writer2']
        self.assertIn('discovery_writer_recreated', [e['event'] for e in writer_events])
        self.assertIn('discovery_writer_destroyed', [e['event'] for e in writer_events])
        burst = next(e for e in events if e['event'] == 'publish_burst_submitted')
        self.assertEqual(burst['phase'], 'writer2_churn')
        self.assertEqual(len(burst['sequences']), 8)
        self.assertIs(burst['pair_member'], False)
        self.assertNotIn('writer', [role for role, _, _ in pool.commands if role == 'writer'])

    def test_schedule_that_cannot_finish_is_an_error_not_a_noop(self):
        clock = FakeClock()
        pool = FakeLifecyclePool(clock, [], lambda *a, **k: None)
        with self.assertRaisesRegex(RuntimeError, 'window_churn_schedule_incomplete'):
            with patch('r2f.runtime.namespace_pair.time', clock):
                run_window_churn(pool, [{'kind': 'reader_create', 'fraction': 0.9, 'role': 'reader2'}],
                                 [],
                                 window_start_ns=clock.now, window_duration_sec=0.00001,
                                 startup_timeout_sec=1, emit=lambda *a, **k: None,
                                 pre_match_count=1, publish_burst_count=1,
                                 publish_burst_interval_sec=0.0)


class AdmissionFailClosed(unittest.TestCase):
    def test_invalid_lifecycle_field_blocks_instead_of_raising(self):
        config = {'target_id': 'discovery.interruption_recovery', 'run_id': 'offline',
                  'domain_id': 1, 'rmw': 'rmw_cyclonedds_cpp', 'arm': 'mutated',
                  'carrier': 'wire_proxy', 'mutation_kind': 'discovery_drop',
                  'mutation_parameters': {}, 'reader_recreate_in_window': True,
                  'reader_recreate_count': 9}
        result = {}
        with tempfile.TemporaryDirectory() as directory:
            run_discovery_pair(config, directory, result)
        self.assertEqual(result['reason'],
                         'blocked:reader_recreate_count must be an integer in 1..8')


class WorkerPoolHealthContract(unittest.TestCase):
    def test_clean_stopped_role_is_healthy_but_crash_is_not(self):
        class Proc:
            def __init__(self, returncode):
                self.returncode = returncode
            def poll(self):
                return self.returncode

        fake = WorkerPool.__new__(WorkerPool)
        fake.workers = {'reader2': Proc(0), 'reader3': Proc(1)}
        fake.stopped_roles = {'reader2', 'reader3'}
        fake.pump = lambda timeout=0.05: None
        # rc0 on a controlled stop is expected; rc1 is still a crash.
        with patch('r2f.runtime.process.time.monotonic', return_value=0.0):
            with self.assertRaisesRegex(RuntimeError, 'reader3'):
                fake.wait(lambda: False, 1, 'offline', check_health=True)


if __name__ == '__main__':
    unittest.main()
