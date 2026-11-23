"""Offline contracts for mixed-vendor (writer/reader different RMW) pairs.

No ROS context, worker process, namespace, or DDS traffic is created here.
Every environment below is constructed by pure Python and compared as data.
"""
import contextlib
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from r2f.runtime.namespace_pair import _write_role_profiles
from r2f.runtime.process import WorkerPool
from r2f.runtime.ros2_pair import (
    RoleRmwWorkerPool, effective_rmws, effective_rmws_receipt, is_mixed_rmws,
    mixed_base_environment, mixed_worker_start_failure, normalize_config,
    pair_is_mixed, require_role_rmw, require_rmws_available, role_rmw,
    role_rmw_differs_from_config, role_rmw_environments,
)


def base(**overrides):
    config = {'run_id': 'offline-mixed', 'domain_id': 173, 'target_revision': 0,
              'arm': 'mutated', 'source_repo': '/tmp/offline-src'}
    config.update(overrides)
    return config


class FakeProcess:
    pid = 4242

    def __init__(self):
        self.stdout = Mock()
        self.stdout.fileno.return_value = 11
        self.stdin = Mock()


@contextlib.contextmanager
def fake_process_pidfds():
    """The fake transport has no kernel PID; own and release its fixture FDs."""
    actual_open = os.pidfd_open
    handles = []
    def open_pidfd(pid):
        if pid == FakeProcess.pid:
            fd = actual_open(os.getpid())
            handles.append(fd)
            return fd
        return actual_open(pid)
    try:
        with patch('r2f.runtime.process.os.pidfd_open', side_effect=open_pidfd):
            yield
    finally:
        for fd in handles:
            os.close(fd)


class FakePool:
    """Capture the exact environment WorkerPool.start builds."""

    def __init__(self, config, environment, output_dir, role_environments):
        self.config = config
        self.environment = environment
        self.output_dir = Path(output_dir)
        self.role_environments = role_environments
        self.emitted = []

    def emit(self, event, **fields):
        self.emitted.append((event, fields))


class EnvironmentContract(unittest.TestCase):
    def start_env(self, config, base_env, role_env, *, mixed_pool, output_dir=None):
        pool = FakePool(config, base_env, output_dir or tempfile.mkdtemp(), role_env)
        process = FakeProcess()
        with fake_process_pidfds(), patch('r2f.runtime.process.subprocess.Popen', return_value=process) as popen, \
             patch('r2f.runtime.process.selectors.DefaultSelector') as selector, \
             patch('r2f.runtime.process.os.set_blocking'):
            selector.return_value = Mock()
            if mixed_pool:
                role_pool = RoleRmwWorkerPool(pool.config, pool.environment, pool.output_dir,
                                              pool.emit, role_environments=pool.role_environments)
                role_pool.selector = Mock()
                role_pool.start('writer')
            else:
                plain = WorkerPool(pool.config, pool.environment, pool.output_dir, pool.emit,
                                   role_environments=pool.role_environments)
                plain.selector = Mock()
                plain.start('writer')
        return popen.call_args.kwargs['env']

    def test_absent_rmws_matches_head_golden_environment(self):
        config = normalize_config(base())
        base_env = {'PATH': '/usr/bin', 'HOME': '/tmp/home'}
        role_env = {'writer': {'CYCLONEDDS_URI': '/tmp/writer.xml', 'ROLE_VALUE': 'writer'}}
        captured = self.start_env(config, base_env, role_env, mixed_pool=True)
        golden = {
            'PATH': '/usr/bin',
            'HOME': '/tmp/home',
            'CYCLONEDDS_URI': '/tmp/writer.xml',
            'ROLE_VALUE': 'writer',
            'ROS_DOMAIN_ID': '173',
            'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp',
            'ROS_LOCALHOST_ONLY': '1',
            'ROS_LOG_DIR': captured['ROS_LOG_DIR'],
            'PYTHONDONTWRITEBYTECODE': '1',
            'PYTHONUNBUFFERED': '1',
        }
        # Byte-for-byte dict equality, including insertion order.
        self.assertEqual(captured, golden)
        # The default path must not synthesize per-role overlays at all.
        self.assertEqual(role_rmw_environments(config, role_env), role_env)
        # A plain WorkerPool (the historical path) must produce the identical
        # environment when rmws is absent.
        shared = tempfile.mkdtemp()
        captured = self.start_env(config, base_env, role_env, mixed_pool=True,
                                  output_dir=shared)
        plain = self.start_env(config, base_env, role_env, mixed_pool=False,
                               output_dir=shared)
        self.assertEqual(captured, plain)
        self.assertEqual(list(captured), list(plain))

    def test_mixed_environment_is_role_correct(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp',
                                              'reader': 'rmw_cyclonedds_cpp'}))
        role_env = role_rmw_environments(config, {})
        self.assertEqual(role_env['writer']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')
        self.assertEqual(role_env['reader']['RMW_IMPLEMENTATION'], 'rmw_cyclonedds_cpp')
        self.assertEqual(role_env['reader2']['RMW_IMPLEMENTATION'], 'rmw_cyclonedds_cpp')
        self.assertEqual(role_env['writer2']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')
        self.assertEqual(role_env['probe']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')

        for role, expected in (('writer', 'rmw_fastrtps_cpp'),
                               ('reader', 'rmw_cyclonedds_cpp'),
                               ('reader2', 'rmw_cyclonedds_cpp'),
                               ('writer2', 'rmw_fastrtps_cpp')):
            with self.subTest(role=role):
                pool = FakePool(config, {'BASE': '1'}, tempfile.mkdtemp(), role_env)
                process = FakeProcess()
                with fake_process_pidfds(), patch('r2f.runtime.process.subprocess.Popen', return_value=process) as popen, \
                     patch('r2f.runtime.process.selectors.DefaultSelector') as selector, \
                     patch('r2f.runtime.process.os.set_blocking'):
                    selector.return_value = Mock()
                    role_pool = RoleRmwWorkerPool(pool.config, pool.environment, pool.output_dir,
                                                  pool.emit, role_environments=pool.role_environments)
                    role_pool.selector = Mock()
                    role_pool.start(role)
                self.assertEqual(popen.call_args.kwargs['env']['RMW_IMPLEMENTATION'], expected)
                # The worker argv sees the same role vendor as its environment.
                import json
                worker_config = json.loads(popen.call_args.args[0][4])
                self.assertEqual(worker_config['rmw'], expected)
                self.assertEqual(worker_config['rmws'], config['rmws'])
        self.assertEqual(config['rmw'], 'rmw_cyclonedds_cpp')

    def test_reader2_reader3_override_and_fallback(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp',
                                              'reader': 'rmw_cyclonedds_cpp',
                                              'reader2': 'rmw_fastrtps_cpp'}))
        self.assertEqual(effective_rmws(config), {
            'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_cyclonedds_cpp',
            'reader2': 'rmw_fastrtps_cpp', 'reader3': 'rmw_cyclonedds_cpp'})
        role_env = role_rmw_environments(config, {})
        self.assertEqual(role_env['reader2']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')
        self.assertEqual(role_env['reader3']['RMW_IMPLEMENTATION'], 'rmw_cyclonedds_cpp')


class IdentityContract(unittest.TestCase):
    def test_role_specific_identity_passes_and_fails_closed(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp',
                                              'reader': 'rmw_cyclonedds_cpp'}))
        require_role_rmw(config, 'writer', {'rmw_identifier': 'rmw_fastrtps_cpp'})
        require_role_rmw(config, 'reader', {'rmw_identifier': 'rmw_cyclonedds_cpp'})
        require_role_rmw(config, 'reader2', {'rmw_identifier': 'rmw_cyclonedds_cpp'})
        require_role_rmw(config, 'reader3', {'rmw_identifier': 'rmw_cyclonedds_cpp'})
        with self.assertRaisesRegex(RuntimeError,
                                    'rmw_identity_mismatch:writer:expected=rmw_fastrtps_cpp'):
            require_role_rmw(config, 'writer', {'rmw_identifier': 'rmw_cyclonedds_cpp'})
        with self.assertRaisesRegex(RuntimeError,
                                    'rmw_identity_mismatch:reader:expected=rmw_cyclonedds_cpp'):
            require_role_rmw(config, 'reader', {'rmw_identifier': 'rmw_fastrtps_cpp'})
        with self.assertRaisesRegex(RuntimeError, 'rmw_identity_mismatch:reader2'):
            require_role_rmw(config, 'reader2', {'rmw_identifier': 'rmw_fastrtps_cpp'})

    def test_reader2_explicit_vendor_changes_identity(self):
        config = normalize_config(base(rmws={'writer': 'rmw_cyclonedds_cpp',
                                              'reader': 'rmw_cyclonedds_cpp',
                                              'reader2': 'rmw_fastrtps_cpp'}))
        require_role_rmw(config, 'reader2', {'rmw_identifier': 'rmw_fastrtps_cpp'})
        with self.assertRaisesRegex(RuntimeError, 'rmw_identity_mismatch:reader2'):
            require_role_rmw(config, 'reader2', {'rmw_identifier': 'rmw_cyclonedds_cpp'})
        require_role_rmw(config, 'reader3', {'rmw_identifier': 'rmw_cyclonedds_cpp'})

    def test_unavailable_vendor_is_explicitly_blocked(self):
        config = normalize_config(base(rmws={'writer': 'rmw_cyclonedds_cpp',
                                              'reader': 'rmw_fastrtps_cpp'}))
        environment = {'rmw': {'rmw_cyclonedds_cpp': {'available': True},
                               'rmw_fastrtps_cpp': {'available': False}}}
        with self.assertRaisesRegex(RuntimeError,
                                    'blocked:rmw_unavailable:reader:rmw_fastrtps_cpp'):
            require_rmws_available(environment, config)


class ReceiptContract(unittest.TestCase):
    def test_receipt_records_configuration_before_observation(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp'}))
        receipt = effective_rmws_receipt(config)
        self.assertEqual(receipt['schema'], 'r2f.effective-rmws/v1')
        self.assertTrue(receipt['configured'])
        self.assertTrue(receipt['mixed'])
        self.assertEqual(set(receipt['roles']), {'writer', 'reader', 'reader2', 'reader3'})
        for row in receipt['roles'].values():
            self.assertEqual(set(row), {'rmw', 'observed_rmw_identifier', 'match'})
            self.assertIsNone(row['observed_rmw_identifier'])
            self.assertIsNone(row['match'])
        self.assertEqual(receipt['roles']['writer']['rmw'], 'rmw_fastrtps_cpp')
        self.assertEqual(receipt['roles']['reader']['rmw'], 'rmw_cyclonedds_cpp')

    def test_receipt_records_observed_identifiers_and_mismatch(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp',
                                              'reader': 'rmw_cyclonedds_cpp'}))
        observed = {'writer': {'rmw_identifier': 'rmw_fastrtps_cpp'},
                    'reader': {'rmw_identifier': 'rmw_cyclonedds_cpp'},
                    'reader2': {'rmw_identifier': 'rmw_fastrtps_cpp'}}
        receipt = effective_rmws_receipt(config, observed)
        self.assertTrue(receipt['roles']['writer']['match'])
        self.assertTrue(receipt['roles']['reader']['match'])
        self.assertFalse(receipt['roles']['reader2']['match'])
        self.assertIsNone(receipt['roles']['reader3']['match'])

    def test_absent_rmws_receipt_is_single_vendor(self):
        receipt = effective_rmws_receipt(normalize_config(base()))
        self.assertFalse(receipt['configured'])
        self.assertFalse(receipt['mixed'])
        self.assertEqual({row['rmw'] for row in receipt['roles'].values()},
                         {'rmw_cyclonedds_cpp'})

    def test_mixed_flag_uses_pair_not_observation_aliases(self):
        same = normalize_config(base(rmws={'writer': 'rmw_cyclonedds_cpp',
                                            'reader': 'rmw_cyclonedds_cpp',
                                            'reader2': 'rmw_fastrtps_cpp'}))
        self.assertFalse(pair_is_mixed(same))
        self.assertTrue(is_mixed_rmws(same))
        mixed = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp'}))
        self.assertTrue(pair_is_mixed(mixed))


class RunReceiptWiring(unittest.TestCase):
    def test_blocked_run_still_records_receipt_in_result_and_raw(self):
        from r2f.runtime.ros2_pair import run_pair
        config = {'run_id': 'offline-mixed-receipt', 'domain_id': 173,
                  'source_repo': '/tmp/offline-src',
                  'rmws': {'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_cyclonedds_cpp'}}
        pool = Mock()
        pool.metadata = []
        pool.start.side_effect = RuntimeError('offline_worker_not_started')
        with tempfile.TemporaryDirectory() as temp, \
             patch('r2f.runtime.ros2_pair.check_environment', return_value={'ok': True}), \
             patch('r2f.runtime.ros2_pair.activated_environment', return_value=({}, {'RAW': 'original'})), \
             patch('r2f.runtime.ros2_pair.RoleRmwWorkerPool', return_value=pool):
            result = run_pair(config, Path(temp) / 'state')
            self.assertFalse(result['real_execution'])
            self.assertIsNone(result['execution'])
            self.assertIn('effective_rmws', result)
            self.assertTrue(result['effective_rmws']['mixed'])
            self.assertEqual(result['effective_rmws']['roles']['writer']['rmw'], 'rmw_fastrtps_cpp')
            raw = Path(result['raw_path']).read_text().splitlines()
        receipts = [json.loads(line) for line in raw if '"effective_rmws"' in line]
        self.assertTrue(receipts)
        self.assertEqual(receipts[0]['receipt']['schema'], 'r2f.effective-rmws/v1')

    def test_mixed_instrumentation_is_blocked_explicitly(self):
        from r2f.runtime.ros2_pair import run_pair
        config = {'run_id': 'offline-mixed-instrumentation', 'domain_id': 173,
                  'source_repo': '/tmp/offline-src', 'observation_mode': 'instrumentation',
                  'rmws': {'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_cyclonedds_cpp'}}
        with tempfile.TemporaryDirectory() as temp:
            result = run_pair(config, Path(temp) / 'state')
        self.assertEqual(result['reason'], 'blocked:mixed_vendor_instrumentation_not_supported')
        self.assertFalse(result['real_execution'])


class NamespaceWiring(unittest.TestCase):
    def test_discovery_blocked_run_still_records_receipt(self):
        from r2f.runtime.ros2_pair import run_pair
        config = {'run_id': 'offline-mixed-discovery', 'domain_id': 173,
                  'source_repo': '/home/wuyuqa/ros2-dds-fuzz-v2',
                  'target_id': 'discovery.interruption_recovery',
                  'carrier': 'wire_proxy', 'mutation_kind': 'discovery_drop',
                  'rmws': {'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_cyclonedds_cpp'}}
        with tempfile.TemporaryDirectory() as temp, \
             patch('r2f.runtime.namespace_pair.check_environment',
                   return_value={'ok': False, 'reason': 'blocked:offline_environment'}), \
             patch('r2f.runtime.namespace_pair._supervise') as supervise:
            result = run_pair(config, Path(temp) / 'state')
            self.assertEqual(result['reason'], 'blocked:offline_environment')
            self.assertFalse(result['real_execution'])
            self.assertTrue(result['effective_rmws']['mixed'])
            self.assertEqual(result['effective_rmws']['roles']['writer']['rmw'],
                             'rmw_fastrtps_cpp')
            raw = Path(result['raw_path']).read_text().splitlines()
        supervise.assert_not_called()
        receipts = [json.loads(line) for line in raw if '"effective_rmws"' in line]
        self.assertTrue(receipts)
        self.assertEqual(receipts[0]['receipt']['schema'], 'r2f.effective-rmws/v1')

    def test_mixed_projection_is_blocked_before_raw_or_observer(self):
        from r2f.runtime.ros2_pair import run_pair
        config = {'run_id': 'offline-mixed-projection', 'domain_id': 173,
                  'source_repo': '/home/wuyuqa/ros2-dds-fuzz-v2',
                  'target_id': 'builtin.discovery_projection_reconciliation',
                  'carrier': 'ros2_endpoint', 'mutation_kind': 'endpoint_qos_variation',
                  'baseline_qos': {'reliability': 'best_effort', 'durability': 'volatile',
                                   'depth': 64},
                  'rmws': {'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_cyclonedds_cpp'}}
        with tempfile.TemporaryDirectory() as temp:
            result = run_pair(config, Path(temp) / 'state')
            self.assertEqual(result['reason'], 'blocked:mixed_vendor_projection_not_supported')
            self.assertFalse(result['real_execution'])
            self.assertFalse((Path(temp) / 'state' / 'raw.jsonl').exists())

    def test_unavailable_vendor_blocks_discovery_pair_without_retry(self):
        from r2f.runtime.ros2_pair import run_pair
        config = {'run_id': 'offline-mixed-unavailable', 'domain_id': 173,
                  'source_repo': '/home/wuyuqa/ros2-dds-fuzz-v2',
                  'target_id': 'discovery.interruption_recovery',
                  'carrier': 'wire_proxy', 'mutation_kind': 'discovery_drop',
                  'rmws': {'writer': 'rmw_cyclonedds_cpp', 'reader': 'rmw_fastrtps_cpp'}}
        environment = {'ok': True, 'rmw': {'rmw_cyclonedds_cpp': {'available': True},
                                           'rmw_fastrtps_cpp': {'available': False}}}
        with tempfile.TemporaryDirectory() as temp, \
             patch('r2f.runtime.namespace_pair.check_environment', return_value=environment), \
             patch('r2f.runtime.namespace_pair.activated_environment',
                   return_value=({}, {})), \
             patch('r2f.runtime.namespace_pair._supervise') as supervise:
            result = run_pair(config, Path(temp) / 'state')
        self.assertEqual(result['reason'],
                         'blocked:rmw_unavailable:reader:rmw_fastrtps_cpp')
        self.assertFalse(result['real_execution'])
        supervise.assert_not_called()


class ChurnIdentityWiring(unittest.TestCase):
    class Pool:
        class Process:
            def __init__(self, pid):
                self.pid, self.returncode = pid, None

            def poll(self):
                return self.returncode

        def __init__(self, events, rmw):
            self.events, self.rmw = events, rmw
            self.workers, self.generations, self.identities = {}, {}, {}
            self.command_id = 0

        def emit(self, event, **fields):
            token = dict(event=event, monotonic_ns=time.monotonic_ns(),
                         namespace_event_index=len(self.events),
                         raw_event_ref=f'mock#{len(self.events)}', **fields)
            self.events.append(token)
            return token

        def pump(self, timeout=0.05):
            pass

        def start(self, role):
            previous = self.workers.get(role)
            if previous is not None and previous.poll() is None:
                raise AssertionError('offline role still live: ' + role)
            generation = self.generations.get(role, 0) + 1
            self.generations[role] = generation
            process = self.Process(1000 + sum(self.generations.values()))
            self.workers[role] = process
            identity = dict(pid=process.pid, worker_generation=generation,
                            endpoint_generation=f'{role}-generation-{generation}')
            self.identities[role] = identity
            self.emit('process_started', role=role, **identity)
            self.emit('endpoint_created', role=role, rmw_identifier=self.rmw,
                      endpoint_gid=f'{role}-gid-{generation}', **identity)

        def wait(self, predicate, timeout, label):
            if not predicate():
                raise AssertionError('predicate_not_met: ' + label)

        def command(self, role, command, **fields):
            if command != 'stop':
                raise AssertionError('unexpected offline command: ' + command)
            self.command_id += 1
            identity = self.identities[role]
            self.emit('worker_command_requested', role=role, command=command,
                      command_id=self.command_id, **identity)
            self.emit('worker_shutdown', role=role, stop_command_id=self.command_id,
                      **identity)
            self.workers[role].returncode = 0
            return self.command_id

    def test_reader2_recreation_uses_its_own_vendor(self):
        from r2f.runtime.namespace_pair import run_window_churn
        config = normalize_config(base(rmws={'writer': 'rmw_cyclonedds_cpp',
                                              'reader': 'rmw_cyclonedds_cpp',
                                              'reader2': 'rmw_fastrtps_cpp'}))
        events = []
        schedule = [{'kind': 'reader_create', 'fraction': 0.0, 'role': 'reader2'}]
        run_window_churn(self.Pool(events, 'rmw_fastrtps_cpp'), schedule, events,
                         window_start_ns=time.monotonic_ns(), window_duration_sec=1.0,
                         startup_timeout_sec=1, emit=lambda *a, **k: None,
                         pre_match_count=1, publish_burst_count=1,
                         publish_burst_interval_sec=0.0, config=config)
        self.assertEqual(events[0]['role'], 'reader2')

    def test_reader2_recreation_mismatch_fails_closed(self):
        from r2f.runtime.namespace_pair import run_window_churn
        config = normalize_config(base(rmws={'writer': 'rmw_cyclonedds_cpp',
                                              'reader': 'rmw_cyclonedds_cpp',
                                              'reader2': 'rmw_fastrtps_cpp'}))
        events = []
        schedule = [{'kind': 'reader_create', 'fraction': 0.0, 'role': 'reader2'}]
        with self.assertRaisesRegex(RuntimeError, 'rmw_identity_mismatch:reader2'):
            run_window_churn(self.Pool(events, 'rmw_cyclonedds_cpp'), schedule, events,
                             window_start_ns=time.monotonic_ns(), window_duration_sec=1.0,
                             startup_timeout_sec=1, emit=lambda *a, **k: None,
                             pre_match_count=1, publish_burst_count=1,
                             publish_burst_interval_sec=0.0, config=config)


class BaseEnvironmentContract(unittest.TestCase):
    def test_single_vendor_selectors_are_removed_from_mixed_base(self):
        base_env = {'PATH': '/usr/bin', 'CYCLONEDDS_URI': '/tmp/cyclone.xml',
                    'FASTRTPS_DEFAULT_PROFILES_FILE': '/tmp/fast.xml',
                    'FASTDDS_DEFAULT_PROFILES_FILE': '/tmp/fastdds.xml',
                    'RMW_FASTRTPS_USE_QOS_FROM_XML': '1',
                    'ROS_DISCOVERY_SERVER': '127.0.0.1:11811',
                    'ROS_DOMAIN_ID': '173'}
        cleaned = mixed_base_environment(base_env)
        self.assertEqual(cleaned, {'PATH': '/usr/bin', 'ROS_DOMAIN_ID': '173'})
        # Input is not mutated.
        self.assertIn('CYCLONEDDS_URI', base_env)


class SingleVendorAliasContract(unittest.TestCase):
    def test_reader2_only_override_still_uses_role_environment(self):
        config = normalize_config(base(rmws={'writer': 'rmw_cyclonedds_cpp',
                                              'reader': 'rmw_cyclonedds_cpp',
                                              'reader2': 'rmw_fastrtps_cpp'}))
        self.assertTrue(role_rmw_differs_from_config(config))
        self.assertFalse(pair_is_mixed(config))
        role_env = role_rmw_environments(config, {})
        self.assertEqual(role_env['reader2']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')
        self.assertEqual(role_env['reader']['RMW_IMPLEMENTATION'], 'rmw_cyclonedds_cpp')


class MixedExecutionPath(unittest.TestCase):
    def test_mixed_pair_uses_role_pool_and_no_single_vendor_sidecar(self):
        from r2f.runtime.ros2_pair import run_pair
        config = {'run_id': 'offline-mixed-path', 'domain_id': 173,
                  'source_repo': '/tmp/offline-src',
                  'target_id': 'discovery.late_endpoint',
                  'carrier': 'ros2_endpoint', 'mutation_kind': 'late_joiner',
                  'rmws': {'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_cyclonedds_cpp'}}
        pool = Mock()
        pool.metadata = []
        pool.start.side_effect = RuntimeError('offline_worker_not_started')
        events = []
        with tempfile.TemporaryDirectory() as temp, \
             patch('r2f.runtime.ros2_pair.check_environment', return_value={'ok': True}), \
             patch('r2f.runtime.ros2_pair.activated_environment', return_value=({}, {})), \
             patch('r2f.runtime.instrumentation_bridge.prepare') as prepare, \
             patch('r2f.runtime.ros2_pair.RoleRmwWorkerPool', return_value=pool) as pool_cls:
            result = run_pair(config, Path(temp) / 'state')
        self.assertEqual(result['reason'], 'offline_worker_not_started')
        prepare.assert_not_called()
        self.assertEqual(pool_cls.call_args.args[0]['rmw'], 'rmw_cyclonedds_cpp')
        names = [e['event'] for e in result['events']]
        self.assertIn('mixed_vendor_observation_unsupported', names)
        self.assertIn('effective_rmws', names)
        role_env = pool_cls.call_args.kwargs['role_environments']
        self.assertEqual(role_env['writer']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')
        self.assertEqual(role_env['reader']['RMW_IMPLEMENTATION'], 'rmw_cyclonedds_cpp')


class WorkerStartFailureContract(unittest.TestCase):
    def test_mixed_worker_exit_is_named_blocked(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp'}))
        self.assertEqual(mixed_worker_start_failure(config, "worker_exited:[('reader', 1)]"),
                         'blocked:mixed_vendor_worker_start_failed:'
                         "worker_exited:[('reader', 1)]")
        self.assertEqual(mixed_worker_start_failure(config, 'delivery_deadline'),
                         'delivery_deadline')
        single = normalize_config(base())
        self.assertEqual(mixed_worker_start_failure(single, 'worker_exited'), 'worker_exited')


class ValidationContract(unittest.TestCase):
    def test_third_vendor_and_empty_values_name_the_field(self):
        cases = (
            ({'writer': 'rmw_other_cpp'}, 'rmws.writer'),
            ({'reader': 'rmw_other_cpp'}, 'rmws.reader'),
            ({'reader2': 'rmw_other_cpp'}, 'rmws.reader2'),
            ({'reader': ''}, 'rmws.reader'),
            ({'writer': None}, 'rmws.writer'),
            ({'writer': 7}, 'rmws.writer'),
        )
        for rmws, field in cases:
            with self.subTest(rmws=rmws), \
                 self.assertRaisesRegex(ValueError, field.replace('.', r'\.')):
                normalize_config(base(rmws=rmws))

    def test_unknown_role_and_non_dict_are_rejected(self):
        with self.assertRaisesRegex(ValueError, r'rmws\.bogus'):
            normalize_config(base(rmws={'bogus': 'rmw_cyclonedds_cpp'}))
        with self.assertRaisesRegex(ValueError, 'invalid_rmws:rmws'):
            normalize_config(base(rmws=['rmw_cyclonedds_cpp']))

    def test_missing_roles_fall_back_without_inventing_a_vendor(self):
        config = normalize_config(base(rmw='rmw_fastrtps_cpp', rmws={}))
        self.assertEqual(effective_rmws(config), {
            'writer': 'rmw_fastrtps_cpp', 'reader': 'rmw_fastrtps_cpp',
            'reader2': 'rmw_fastrtps_cpp', 'reader3': 'rmw_fastrtps_cpp'})
        config = normalize_config(base(rmw='rmw_cyclonedds_cpp',
                                       rmws={'writer': 'rmw_fastrtps_cpp'}))
        self.assertEqual(effective_rmws(config)['reader'], 'rmw_cyclonedds_cpp')

    def test_normalized_config_preserves_explicit_map(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp',
                                              'reader': 'rmw_cyclonedds_cpp'}))
        self.assertEqual(config['rmws'], {'writer': 'rmw_fastrtps_cpp',
                                          'reader': 'rmw_cyclonedds_cpp'})
        self.assertEqual(config['rmw'], 'rmw_cyclonedds_cpp')


class CarrierWiringContract(unittest.TestCase):
    def test_mixed_profiles_follow_each_role_vendor(self):
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp',
                                              'reader': 'rmw_cyclonedds_cpp'}))
        with tempfile.TemporaryDirectory() as temp:
            records, environments = _write_role_profiles(Path(temp), config)
        self.assertEqual(records['writer']['rmw'], 'rmw_fastrtps_cpp')
        self.assertEqual(records['reader']['rmw'], 'rmw_cyclonedds_cpp')
        self.assertEqual(records['reader2']['rmw'], 'rmw_cyclonedds_cpp')
        self.assertEqual(records['reader3']['rmw'], 'rmw_cyclonedds_cpp')
        self.assertEqual(records['reader9']['rmw'], 'rmw_cyclonedds_cpp')
        self.assertIn('FASTRTPS_DEFAULT_PROFILES_FILE', environments['writer'])
        self.assertIn('CYCLONEDDS_URI', environments['reader'])
        self.assertNotIn('CYCLONEDDS_URI', environments['writer'])
        self.assertNotIn('FASTRTPS_DEFAULT_PROFILES_FILE', environments['reader'])
        self.assertIn('CYCLONEDDS_URI', environments['reader2'])
        self.assertIn('CYCLONEDDS_URI', environments['reader3'])
        self.assertIn('CYCLONEDDS_URI', environments['reader9'])

    def test_mixed_observation_sidecar_is_unavailable_not_faked(self):
        from r2f.runtime.ros2_pair import _MixedObservationContext
        config = normalize_config(base(rmws={'writer': 'rmw_fastrtps_cpp'}))
        context = _MixedObservationContext(config, {'BASE': '1'})
        self.assertEqual(context.role_env['writer']['RMW_IMPLEMENTATION'], 'rmw_fastrtps_cpp')
        self.assertEqual(context.role_env['reader']['RMW_IMPLEMENTATION'], 'rmw_cyclonedds_cpp')
        self.assertFalse(context.capture_worker('writer', 1, {})['accepted'])
        self.assertEqual(context.collect({})['status'], 'incomplete')


if __name__ == '__main__':
    unittest.main()
