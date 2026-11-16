"""Offline admission contracts for exploratory runtime routing (T04/T06).

No ROS, DDS, namespace, nft rule or worker process is created here.  These
tests prove only dispatch shape and fail-closed preflight handling.  They are
not execution evidence and cannot set ``execution_admitted`` or a live
capability flag.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from r2f.runtime.domains import IsolationError
from r2f.runtime.ros2_pair import (
    EXPLORATORY_TARGETS, exploratory_runtime_capabilities, normalize_config,
    recovery_fuzz_admission_error, run_pair, runtime_capabilities,
)

PROJECTION_FUZZ = 'builtin.discovery_projection_reconciliation.fuzz_v1'
RECOVERY_FUZZ = 'discovery.interruption_recovery.fuzz_v1'
LEGACY_PROJECTION = 'builtin.discovery_projection_reconciliation'
LEGACY_RECOVERY = 'discovery.interruption_recovery'

PROJECTION_QOS = {'reliability': 'best_effort', 'durability': 'volatile', 'depth': 64,
                  'deadline_ns': 1250000000, 'liveliness': 'manual_by_topic',
                  'liveliness_lease_duration_ns': 5000000000}


def state_dir():
    return Path(tempfile.mkdtemp()) / 'state'


def window(index, drop=0.05, offset=0.0, restore=0.0):
    return {'window_id': 'w%d' % index, 'window_index': index,
            'trigger_anchor': 'fresh_match' if index == 0 else 'previous_restore_confirmed',
            'trigger_offset_sec': offset, 'drop_duration_sec': drop,
            'restore_interval_sec': restore}


def projection_config(**overrides):
    config = {
        'run_id': 'admission-projection', 'domain_id': 173, 'target_revision': 0,
        'target_id': PROJECTION_FUZZ, 'carrier': 'ros2_endpoint',
        'mutation_kind': 'endpoint_qos_variation', 'arm': 'mutated',
        'fuzz_surfaces': ['P02'], 'qos': dict(PROJECTION_QOS),
        'submitted_parameters': {'qos.reliability': 'best_effort'},
        'mutation_parameters': {'qos': dict(PROJECTION_QOS)},
    }
    config.update(overrides)
    return config


def recovery_config(surfaces=('E01',), windows=None, **overrides):
    windows = [window(0)] if windows is None else windows
    config = {
        'run_id': 'admission-recovery', 'domain_id': 173, 'target_revision': 0,
        'target_id': RECOVERY_FUZZ, 'carrier': 'wire_proxy',
        'mutation_kind': 'discovery_drop', 'arm': 'mutated',
        'fuzz_surfaces': list(surfaces), 'environment_windows': windows,
        'drop_duration_sec': windows[0]['drop_duration_sec'],
        'submitted_parameters': {
            'drop_duration_sec': windows[0]['drop_duration_sec'],
            'environment_windows.count': len(windows),
            'environment_windows.restore_interval_sec': windows[0]['restore_interval_sec'],
        },
        'mutation_parameters': {'drop_duration_sec': windows[0]['drop_duration_sec']},
    }
    if 'E03' in surfaces:
        config['submitted_parameters']['environment_windows.anchor_offset_sec'] = \
            windows[0]['trigger_offset_sec']
    config.update(overrides)
    return config


def sentinel(name):
    return {'real_execution': False, 'runtime_status': 'blocked', 'status': 'blocked',
            'reason': 'sentinel:' + name, 'events': [], 'facts': {}, 'environment': {},
            'precondition_receipts': [], 'observation_receipts': [],
            'mutation_receipt': {}, 'execution': None, 'execution_metadata': None,
            'raw_path': None, 'raw_sha256': None, 'capabilities': {}, 'sentinel': name}


class DispatchTests(unittest.TestCase):
    def test_projection_fuzz_dispatches_to_projection_backend_without_rewriting_id(self):
        config = projection_config()
        original = dict(config)
        with patch('r2f.runtime.namespace_pair.run_discovery_pair',
                   return_value=sentinel('projection')) as backend:
            result = run_pair(config, state_dir())
        self.assertEqual(result['sentinel'], 'projection')
        self.assertEqual(backend.call_count, 1)
        passed = backend.call_args[0][0]
        self.assertEqual(passed['target_id'], PROJECTION_FUZZ)
        # Caller config is not mutated and the exploratory id is never swapped.
        self.assertEqual(config, original)
        self.assertEqual(passed['carrier'], 'ros2_endpoint')
        self.assertEqual(passed['mutation_kind'], 'endpoint_qos_variation')
        # Plan-derived public QoS survives normalization; roles derive from it.
        self.assertEqual(passed['qos']['deadline_ns'], 1250000000)
        self.assertEqual(passed['writer_qos'], passed['qos'])
        self.assertEqual(passed['reader_qos'], passed['qos'])
        # The backend normalizer accepts the dispatched config offline and
        # preserves the exploratory identity and the submitted profile.
        from r2f.runtime.projection_pair import normalize_projection_config
        derived = normalize_projection_config(dict(passed))
        self.assertEqual(derived['target_id'], PROJECTION_FUZZ)
        self.assertEqual(derived['qos'], passed['qos'])
        self.assertEqual(derived['baseline_qos']['deadline_ns'], 9223372036854775807)
        self.assertNotEqual(derived['baseline_qos'], derived['qos'])
        self.assertEqual(result['real_execution'], False)
        self.assertIsNone(result['execution'])

    def test_recovery_fuzz_dispatches_single_window_without_rewriting_id(self):
        config = recovery_config()
        original = dict(config)
        with patch('r2f.runtime.namespace_pair.run_discovery_pair',
                   return_value=sentinel('recovery')) as backend:
            result = run_pair(config, state_dir())
        self.assertEqual(result['sentinel'], 'recovery')
        self.assertEqual(backend.call_count, 1)
        passed = backend.call_args[0][0]
        self.assertEqual(passed['target_id'], RECOVERY_FUZZ)
        self.assertEqual(config, original)
        self.assertEqual(passed['carrier'], 'wire_proxy')
        self.assertEqual(passed['mutation_kind'], 'discovery_drop')
        self.assertEqual(len(passed['environment_windows']), 1)
        self.assertEqual(passed['environment_windows'][0]['drop_duration_sec'], 0.05)
        self.assertEqual(passed['drop_duration_sec'], 0.05)
        # Backend normalizer accepts the dispatched config offline: one window,
        # same target id, and the mutated duration is not silently changed.
        from r2f.runtime.namespace_pair import normalize_discovery_config
        normalized = normalize_discovery_config(dict(passed))
        self.assertEqual(normalized['target_id'], RECOVERY_FUZZ)
        self.assertEqual(len(normalized['runtime_windows']), 1)
        self.assertEqual(normalized['runtime_windows'][0]['drop_duration_sec'], 0.05)
        # A one-window chain is a chain, not a second window: the count is what
        # matters, and E01 can never reach the multi-window branch.
        self.assertTrue(normalized['window_chain_requested'])
        self.assertEqual(len(normalized['runtime_windows']), 1)
        self.assertFalse(result['real_execution'])

    def test_unknown_target_is_still_rejected_before_any_backend(self):
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair({'run_id': 'admission-unknown', 'domain_id': 173,
                               'target_id': 'discovery.not_a_target'},
                              state_dir())
        self.assertEqual(result['reason'], 'blocked:target_not_supported')
        self.assertFalse(result['real_execution'])
        self.assertIsNone(result['execution'])
        backend.assert_not_called()

    def test_unknown_fuzz_target_is_rejected_even_with_plan_fields(self):
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair({'run_id': 'admission-unknown-fuzz', 'domain_id': 173,
                               'target_id': 'discovery.not_a_target.fuzz_v1',
                               'carrier': 'ros2_endpoint', 'mutation_kind': 'late_joiner',
                               'fuzz_surfaces': ['P01'],
                               'submitted_parameters': {'reader_delay_sec': 0.1},
                               'reader_delay_sec': 0.1}, state_dir())
        self.assertEqual(result['reason'], 'blocked:target_not_supported')
        backend.assert_not_called()

    def test_projection_carrier_contract_mismatch_never_dispatches(self):
        config = projection_config(mutation_kind='late_joiner')
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair(config, state_dir())
        self.assertIn('exploratory_carrier_contract_mismatch', result['reason'])
        backend.assert_not_called()

    def test_plan_supplied_baseline_qos_is_accepted_and_kept_distinct(self):
        baseline = {'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
                    'deadline_ns': 9223372036854775807, 'liveliness': 'automatic',
                    'liveliness_lease_duration_ns': 9223372036854775807}
        config = projection_config(baseline_qos=dict(baseline))
        with patch('r2f.runtime.namespace_pair.run_discovery_pair',
                   return_value=sentinel('projection-baseline')) as backend:
            run_pair(config, state_dir())
        passed = backend.call_args[0][0]
        from r2f.runtime.projection_pair import normalize_projection_config
        derived = normalize_projection_config(dict(passed))
        self.assertEqual(derived['baseline_qos'], baseline)
        self.assertNotEqual(derived['baseline_qos'], derived['qos'])

    def test_plan_field_outside_projection_schema_is_rejected_by_backend(self):
        config = projection_config(qos=dict(PROJECTION_QOS, invented=1))
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair(config, state_dir())
        self.assertIn('unsupported_qos_field', result['reason'])
        backend.assert_not_called()

    def test_missing_submitted_parameter_blocks_before_dispatch(self):
        config = projection_config(submitted_parameters={})
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair(config, state_dir())
        self.assertIn('exploratory_binding_error', result['reason'])
        backend.assert_not_called()


class PendingSurfaceTests(unittest.TestCase):
    def test_e02_multi_window_is_not_admitted(self):
        config = recovery_config(surfaces=('E02',), windows=[window(0), window(1, 0.1, 0.0, 0.02)])
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair(config, state_dir())
        self.assertEqual(result['reason'], 'unsupported_capability:surface_not_admitted:E02')
        self.assertEqual(result['capability_status'], 'unsupported_capability')
        backend.assert_not_called()

    def test_e03_anchor_offset_is_not_admitted(self):
        config = recovery_config(surfaces=('E03',), windows=[window(0, offset=0.1)])
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair(config, state_dir())
        self.assertEqual(result['reason'], 'unsupported_capability:surface_not_admitted:E03')
        backend.assert_not_called()

    def test_e01_with_two_windows_is_still_refused(self):
        config = recovery_config(surfaces=('E01',), windows=[window(0), window(1, 0.1, 0.0, 0.02)])
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend:
            result = run_pair(config, state_dir())
        self.assertEqual(result['reason'], 'unsupported_capability:multi_window_not_admitted')
        backend.assert_not_called()

    def test_admission_predicate_is_scoped_to_the_recovery_fuzz_target(self):
        self.assertIsNone(recovery_fuzz_admission_error({'target_id': LEGACY_RECOVERY}))
        self.assertIsNone(recovery_fuzz_admission_error(
            {'target_id': RECOVERY_FUZZ, 'fuzz_surfaces': ['E01'],
             'environment_windows': [window(0)]}))

    def test_capability_flags_stay_unadmitted(self):
        caps = exploratory_runtime_capabilities()
        for target in (PROJECTION_FUZZ, RECOVERY_FUZZ):
            self.assertFalse(caps[target]['admitted'])
            self.assertFalse(caps[target]['execution_evidence'])
            self.assertTrue(caps[target]['admission_requires_live_preflight'])
        live = runtime_capabilities()
        self.assertFalse(live['projection_observation']['admitted'])
        self.assertFalse(live['discovery_network']['admitted'])


class PreflightFailClosedTests(unittest.TestCase):
    class GoodLease:
        def __init__(self, *args, **kwargs):
            self.receipts = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def test_environment_preflight_failure_blocks_both_fuzz_targets(self):
        for config in (projection_config(), recovery_config()):
            with self.subTest(target=config['target_id']), \
                 patch('r2f.runtime.namespace_pair.LeaseSet', self.GoodLease), \
                 patch('r2f.runtime.namespace_pair.check_environment',
                       return_value={'ok': False, 'reason': 'blocked:offline_environment'}), \
                 patch('r2f.runtime.namespace_pair._supervise') as supervise:
                output = state_dir()
                result = run_pair(config, output)
            self.assertEqual(result['reason'], 'blocked:offline_environment')
            self.assertFalse(result['real_execution'])
            self.assertIsNone(result['execution'])
            supervise.assert_not_called()
            self.assertTrue((output / 'raw.jsonl').is_file())

    def test_lease_acquisition_failure_blocks_without_supervisor(self):
        class BadLease:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                raise IsolationError('lease_conflict:offline')

            def __exit__(self, *exc):
                return False

        with patch('r2f.runtime.namespace_pair.LeaseSet', BadLease), \
             patch('r2f.runtime.namespace_pair.check_environment', return_value={'ok': True}), \
             patch('r2f.runtime.namespace_pair._supervise') as supervise:
            result = run_pair(recovery_config(), state_dir())
        self.assertEqual(result['reason'], 'lease_conflict:offline')
        self.assertFalse(result['real_execution'])
        supervise.assert_not_called()

    def test_namespace_host_admission_failure_blocks(self):
        with patch('r2f.runtime.namespace_pair.LeaseSet', self.GoodLease), \
             patch('r2f.runtime.namespace_pair.check_environment',
                   return_value={'ok': True, 'reason': None}), \
             patch('r2f.runtime.namespace_pair._supervise',
                   side_effect=IsolationError('namespace_lease_not_held')):
            result = run_pair(recovery_config(), state_dir())
        self.assertEqual(result['reason'], 'namespace_lease_not_held')
        self.assertFalse(result['real_execution'])
        self.assertIsNone(result['execution'])

    def test_namespace_child_failure_keeps_original_reason(self):
        with patch('r2f.runtime.namespace_pair.LeaseSet', self.GoodLease), \
             patch('r2f.runtime.namespace_pair.check_environment',
                   return_value={'ok': True, 'reason': None}), \
             patch('r2f.runtime.namespace_pair._supervise',
                   side_effect=RuntimeError('projection_observer_not_ready')):
            result = run_pair(projection_config(), state_dir())
        self.assertEqual(result['reason'], 'projection_observer_not_ready')
        self.assertFalse(result['real_execution'])

    def test_source_identity_change_during_execution_blocks(self):
        before = {'head': 'A', 'runtime_source_sha256': 'a', 'observer_source_sha256': 'a'}
        after = {'head': 'B', 'runtime_source_sha256': 'b', 'observer_source_sha256': 'b'}
        inner = {'runtime_status': 'completed', 'reason': None, 'facts': {},
                 'precondition_receipts': [], 'observation_receipts': [],
                 'mutation_receipt': {}, 'environment': {}, 'workers': []}
        with patch('r2f.runtime.namespace_pair.LeaseSet', self.GoodLease), \
             patch('r2f.runtime.namespace_pair.check_environment',
                   return_value={'ok': True, 'reason': None}), \
             patch('r2f.runtime.namespace_pair._supervise', return_value=inner), \
             patch('r2f.runtime.namespace_pair.execution_chain_identity',
                   side_effect=[before, after]):
            result = run_pair(recovery_config(), state_dir())
        self.assertEqual(result['reason'], 'source_identity_changed_during_execution')
        self.assertEqual(result['runtime_status'], 'blocked')
        self.assertFalse(result['real_execution'])
        self.assertIsNone(result['execution'])


class RequiredTargetBehaviourTests(unittest.TestCase):
    def test_legacy_recovery_and_projection_still_dispatch_to_namespace_backend(self):
        legacy_recovery = {'run_id': 'legacy-recovery', 'domain_id': 173,
                           'target_id': LEGACY_RECOVERY, 'carrier': 'wire_proxy',
                           'mutation_kind': 'discovery_drop', 'arm': 'mutated',
                           'drop_duration_sec': 0.2,
                           'mutation_parameters': {'drop_duration_sec': 0.2}}
        legacy_projection = {'run_id': 'legacy-projection', 'domain_id': 173,
                             'target_id': LEGACY_PROJECTION, 'carrier': 'ros2_endpoint',
                             'mutation_kind': 'endpoint_qos_variation', 'arm': 'mutated',
                             'qos': {'reliability': 'reliable', 'durability': 'volatile',
                                     'depth': 64, 'deadline_ns': 1250000000},
                             'mutation_parameters': {}}
        for config in (legacy_recovery, legacy_projection):
            with self.subTest(target=config['target_id']), \
                 patch('r2f.runtime.namespace_pair.run_discovery_pair',
                       return_value=sentinel(config['target_id'])) as backend:
                result = run_pair(config, state_dir())
            self.assertEqual(result['sentinel'], config['target_id'])
            self.assertEqual(backend.call_count, 1)
            self.assertEqual(backend.call_args[0][0]['target_id'], config['target_id'])

    def test_legacy_late_endpoint_does_not_use_discovery_backend(self):
        config = {'run_id': 'legacy-late', 'domain_id': 173,
                  'target_id': 'discovery.late_endpoint', 'carrier': 'ros2_endpoint',
                  'mutation_kind': 'late_joiner', 'arm': 'mutated',
                  'mutation_parameters': {'reader_delay_sec': 0.15}}
        with patch('r2f.runtime.namespace_pair.run_discovery_pair') as backend, \
             patch('r2f.runtime.ros2_pair.LeaseSet', PreflightFailClosedTests.GoodLease), \
             patch('r2f.runtime.ros2_pair.check_environment',
                   return_value={'ok': False, 'reason': 'blocked:offline_environment'}):
            result = run_pair(config, state_dir())
        backend.assert_not_called()
        self.assertEqual(result['reason'], 'blocked:offline_environment')
        self.assertFalse(result['real_execution'])

    def test_exploratory_target_ids_are_unchanged_in_declarations(self):
        self.assertEqual(set(EXPLORATORY_TARGETS), {
            'discovery.late_endpoint.fuzz_v1', 'discovery.late_join_delivery.fuzz_v1',
            PROJECTION_FUZZ, RECOVERY_FUZZ, 'discovery.match_compatibility.fuzz_v1'})
        self.assertNotIn(PROJECTION_FUZZ, (LEGACY_PROJECTION, LEGACY_RECOVERY))

    def test_required_normalization_defaults_are_unchanged(self):
        c = normalize_config({'run_id': 'legacy-defaults', 'domain_id': 173,
                              'target_id': 'discovery.late_endpoint'})
        self.assertEqual(c['target_id'], 'discovery.late_endpoint')
        self.assertEqual(sorted(c['qos']), ['depth', 'durability', 'reliability'])
        self.assertEqual(c['qos']['depth'], 32)


if __name__ == '__main__':
    unittest.main()
