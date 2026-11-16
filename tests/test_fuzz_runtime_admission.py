"""Offline admission contracts for exploratory runtime routing (T04/T06).

No ROS, DDS, namespace, nft rule or worker process is created here.  These
tests prove only dispatch shape and fail-closed preflight handling.  They are
not execution evidence and cannot set ``execution_admitted`` or a live
capability flag.
"""
import contextlib
import io
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


class BackendSelectionTests(unittest.TestCase):
    """`_body` is the in-namespace switch; prove the concrete backend choice."""

    def test_projection_fuzz_id_selects_projection_namespace_body(self):
        from r2f.runtime import namespace_pair
        request = {'config': {'target_id': PROJECTION_FUZZ, 'arm': 'mutated'},
                   'output_dir': str(state_dir()), 'source': {}, 'leases': [],
                   'host_pid': 1, 'host_preflight_ok': True}
        with patch('r2f.runtime.projection_pair.namespace_body',
                   return_value={'sentinel': 'projection-body'}) as body:
            result = namespace_pair._body(request, object())
        self.assertEqual(result, {'sentinel': 'projection-body'})
        self.assertEqual(body.call_count, 1)
        self.assertIs(body.call_args[0][0], request)

    def test_recovery_fuzz_id_never_enters_projection_body(self):
        from r2f.runtime import namespace_pair
        request = {'config': {'target_id': RECOVERY_FUZZ, 'arm': 'mutated',
                              'mutation_parameters': {}, 'drop_duration_sec': 0.05,
                              'recovery_timeout_sec': 1.0, 'counter_interval_sec': 0.1,
                              'domain_id': 173, 'run_id': 'admission-body',
                              'match_timeout_sec': 1, 'delivery_timeout_sec': 1,
                              'startup_timeout_sec': 1, 'publish_interval_sec': 0.01,
                              'pre_match_count': 1, 'post_match_count': 3,
                              'qos': {'reliability': 'reliable',
                                      'durability': 'volatile', 'depth': 32}},
                   'output_dir': str(state_dir()), 'source': {}, 'leases': [],
                   'host_pid': 1, 'host_preflight_ok': True}
        with patch('r2f.runtime.namespace_pair.validate_host_leases',
                   side_effect=IsolationError('namespace_host_preflight_missing')), \
             patch('r2f.runtime.namespace_pair.execution_chain_identity', return_value={}), \
             patch('r2f.runtime.projection_pair.namespace_body') as body, \
             contextlib.redirect_stdout(io.StringIO()):
            result = namespace_pair._body(request, object())
        body.assert_not_called()
        # It enters the recovery body and fails closed at host admission.
        self.assertEqual(result['runtime_status'], 'blocked')
        self.assertEqual(result['reason'],
                         'IsolationError:namespace_host_preflight_missing')
        self.assertFalse(result['mutation_receipt']['applied'])
        self.assertFalse(result['mutation_receipt']['hit'])


class QosBindingRaceTests(unittest.TestCase):
    """Reproduce the observed live race: the binding row is still in the pipe.

    The real failure (results/fuzz-cli/late-endpoint-d167ed3-20261116T190550Z)
    showed event_index=29 carrying the correct reader GID after a0abort that
    fired before the controller pumped it.  These tests model the pipe with a
    pool that only appends the row when pumped.
    """

    def make_pool(self, rows, *, emit_row=True):
        from r2f.runtime import ros2_pair

        class PipePool:
            def __init__(self):
                self.pumped = 0
                self.waits = []

            def wait(self, predicate, timeout, label, **kwargs):
                self.waits.append((label, timeout))
                deadline = self.pumped + 25
                while self.pumped < deadline:
                    self.pumped += 1
                    if emit_row and self.pumped == 3 and predicate is not None:
                        rows.append({'event': 'qos_binding_observed', 'role': 'reader',
                                     'event_index': 29,
                                     'observed_self_gid': '0110286bf078e2f2f7063cc100000904',
                                     'observed_self': {'reliability': 'reliable',
                                                       'durability': 'volatile', 'depth': 64}})
                    if predicate():
                        return
                import time as _time
                _time.sleep(0)
                raise TimeoutError(label)

            def pump(self, timeout=0.05):
                self.pumped += 1

        return ros2_pair, PipePool()

    def test_late_binding_row_is_waited_for_not_reported_missing(self):
        rows = []
        ros2_pair, pool = self.make_pool(rows)
        captured = []
        row = ros2_pair.wait_for_qos_binding(
            rows, pool, 'reader', '0110286bf078e2f2f7063cc100000904', 8.0,
            emit=lambda event, **fields: captured.append((event, fields)), exactly_one=False)
        self.assertEqual(row['observed_self_gid'], '0110286bf078e2f2f7063cc100000904')
        self.assertEqual(len(rows), 1)
        self.assertEqual(pool.waits, [('qos_binding_observation:reader', 8.0)])
        self.assertEqual(captured, [])

    def test_timeout_is_recorded_as_observation_gap_not_sut_violation(self):
        rows = []
        ros2_pair, pool = self.make_pool(rows, emit_row=False)
        captured = []

        def emit(event, **fields):
            captured.append((event, fields))

        with self.assertRaisesRegex(RuntimeError, 'qos_binding_observation_missing:reader'):
            ros2_pair.wait_for_qos_binding(rows, pool, 'reader', 'gid', 1.0,
                                           emit=emit, exactly_one=True)
        self.assertEqual([item[0] for item in captured], ['discovery_observation_unknown'])
        fields = captured[0][1]
        self.assertEqual(fields['classification'], 'observation_timeout_not_sut_violation')
        self.assertEqual(fields['role'], 'reader')

    def test_wrong_gid_is_still_rejected(self):
        rows = [{'event': 'qos_binding_observed', 'role': 'reader', 'event_index': 29,
                 'observed_self_gid': 'deadbeef', 'observed_self': {'depth': 64}}]
        ros2_pair, pool = self.make_pool(rows)
        with self.assertRaisesRegex(RuntimeError,
                                    'qos_binding_observation_not_bound_to_endpoint:reader'):
            ros2_pair.wait_for_qos_binding(rows, pool, 'reader', '0110286b', 1.0,
                                           emit=lambda *a, **k: None, exactly_one=True)

    def test_empty_observed_profile_is_still_rejected(self):
        rows = [{'event': 'qos_binding_observed', 'role': 'writer', 'event_index': 11,
                 'observed_self_gid': 'abc', 'observed_self': None}]
        ros2_pair, pool = self.make_pool(rows)
        with self.assertRaisesRegex(RuntimeError, 'qos_binding_observation_empty:writer'):
            ros2_pair.wait_for_qos_binding(rows, pool, 'writer', 'abc', 1.0,
                                           emit=lambda *a, **k: None, exactly_one=True)

    def test_projection_requires_exactly_one_binding_row(self):
        rows = [{'event': 'qos_binding_observed', 'role': 'writer', 'event_index': 11,
                 'observed_self_gid': 'abc', 'observed_self': {'depth': 64}},
                {'event': 'qos_binding_observed', 'role': 'writer', 'event_index': 12,
                 'observed_self_gid': 'abc', 'observed_self': {'depth': 64}}]
        ros2_pair, pool = self.make_pool(rows)
        with self.assertRaisesRegex(RuntimeError, 'qos_binding_observation_ambiguous:writer'):
            ros2_pair.wait_for_qos_binding(rows, pool, 'writer', 'abc', 1.0,
                                           emit=lambda *a, **k: None, exactly_one=True)

    def test_endpoint_pair_source_consults_the_waiting_helper(self):
        from pathlib import Path as _Path
        import r2f.runtime.ros2_pair as _module
        source = _Path(_module.__file__).read_text()
        self.assertIn("wait_for_qos_binding(events, pool, role, token['endpoint_gid']", source)
        # The old stale-list check must be gone from the exploratory branch.
        self.assertNotIn("if not observed_rows or observed_rows[-1].get('observed_self_gid')", source)

    def test_projection_source_consults_the_waiting_helper(self):
        from pathlib import Path as _Path
        import r2f.runtime.projection_pair as _module
        source = _Path(_module.__file__).read_text()
        self.assertIn('wait_for_qos_binding(', source)
        self.assertNotIn("if len(rows) != 1:\n                    raise RuntimeError('qos_binding_observation_missing:'", source)


class PlannedOnlyBackendTests(unittest.TestCase):
    """`execute=True` must not reach a backend whose route is unadmitted."""

    @staticmethod
    def _cycle(target_id, surface_id):
        import json
        from r2f.campaign.corpus import Corpus
        from r2f.campaign.feedback import FeedbackLog
        from r2f.campaign.scheduler import ExploratoryScheduler
        from r2f.core import Registry
        from r2f.modules.s1 import fuzz_catalog
        from r2f.modules.s1.module import register_exploratory

        registry = Registry()
        register_exploratory(registry, include_required=False)
        target = registry.get_target(target_id)
        contract = target.exploration_contract
        surface = contract.surface_by_id[surface_id]
        seed = fuzz_catalog.load_seeds()["seeds"][surface_id]
        corpus = Corpus()
        corpus.add_seed({
            "target_id": target_id, "target_revision": 0,
            "rmw": "rmw_cyclonedds_cpp", "contract_hash": contract.contract_hash(),
            "surface_id": surface_id, "boundary_class": surface.boundary_classes[0],
            "seed_id": surface_id, "executable": True,
            "mutator_id": surface.mutator_ids[0],
            "final_config": json.loads(json.dumps(seed["logical_config"])),
        })
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0)
        profile = {
            "schema_version": "r2f.s1-fuzz-profile/v1",
            "profile_id": "runtime-admission-planned-only",
            "campaign_kind": "exploratory", "enabled": False,
            "required_catalog_included": False, "target_ids": [target_id],
            "surfaces": [surface_id], "rmws": ["rmw_cyclonedds_cpp"],
            "seed_specs": [], "execution_boundary": "offline planning only",
        }
        return corpus, feedback, scheduler, profile

    def test_planned_only_backends_never_call_the_injected_runtime(self):
        from r2f.campaign.runner import run_exploratory_cycle
        cases = ((PROJECTION_FUZZ, "P02",
                  "unsupported_capability:execution_backend_not_admitted:projection_pair"),
                 (RECOVERY_FUZZ, "E01",
                  "unsupported_capability:execution_backend_not_admitted:discovery_namespace"))
        for target_id, surface_id, expected in cases:
            with self.subTest(target=target_id):
                corpus, feedback, scheduler, profile = self._cycle(target_id, surface_id)
                calls = []
                with tempfile.TemporaryDirectory() as temp:
                    result = run_exploratory_cycle(
                        Path(temp) / "cycle", corpus=corpus, feedback=feedback,
                        scheduler=scheduler, profile=profile, execute=True,
                        runtime=lambda config, output: calls.append(config),
                        max_inputs=1, run_id="admission-planned-only")
                self.assertEqual(calls, [])
                self.assertEqual(len(result["rows"]), 1)
                row = result["rows"][0]
                self.assertEqual(row["execution_stage"], "planned")
                self.assertIs(row["actual_run"], False)
                self.assertEqual(row["reason_code"], expected)
                self.assertFalse(any(row["mutation_receipt"].values()))


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
