"""Offline contracts for exploratory runtime parameter binding (T04/V35).

No ROS context, namespace, nft rule or worker process is created here. Every
event below is a pure-Python fixture; nothing in this file is execution evidence.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from r2f.runtime import ros2_pair
from r2f.runtime.ros2_pair import (
    EXPLORATORY_TARGETS, exploratory_bindings, exploratory_runtime_capabilities,
    normalize_config, require_binding_preserved, run_pair,
)
from r2f.runtime.worker import PUBLIC_QOS_FIELDS, create_public_qos, self_qos_entry


def base(**overrides):
    config = {'run_id': 'offline-bind', 'domain_id': 173, 'target_revision': 0,
              'arm': 'mutated', 'source_repo': '/tmp/offline-src'}
    config.update(overrides)
    return config


def p01_input(**overrides):
    fields = dict(target_id='discovery.late_endpoint.fuzz_v1', carrier='ros2_endpoint',
                  mutation_kind='late_joiner', fuzz_surfaces=['P01', 'W02'],
                  submitted_parameters={'reader_delay_sec': 0.15, 'publish_interval_sec': 0.03},
                  reader_delay_sec=0.15, publish_interval_sec=0.03)
    fields.update(overrides)
    return base(**fields)


def projection_input(**overrides):
    qos = {'reliability': 'best_effort', 'durability': 'volatile', 'depth': 64,
           'deadline_ns': 1250000000, 'liveliness': 'manual_by_topic',
           'liveliness_lease_duration_ns': 5000000000}
    fields = dict(target_id='builtin.discovery_projection_reconciliation.fuzz_v1',
                  carrier='ros2_endpoint', mutation_kind='endpoint_qos_variation',
                  fuzz_surfaces=['P02', 'P03', 'P04', 'P05', 'P06'],
                  submitted_parameters={'qos.reliability': 'best_effort',
                                        'qos.durability': 'volatile',
                                        'qos.deadline_ns': 1250000000,
                                        'qos.liveliness': 'manual_by_topic',
                                        'qos.liveliness_lease_duration_ns': 5000000000},
                  qos=qos)
    fields.update(overrides)
    return base(**fields)


class BindingDeclarationTests(unittest.TestCase):
    def test_exploratory_targets_declare_backend_and_surfaces(self):
        from r2f.core import Registry
        from r2f.modules.s1.module import register_exploratory
        from r2f.modules.s1.targets import F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET

        legacy_targets = {
            'discovery.late_endpoint.fuzz_v1', 'discovery.late_join_delivery.fuzz_v1',
            'builtin.discovery_projection_reconciliation.fuzz_v1',
            'discovery.interruption_recovery.fuzz_v1',
            'discovery.match_compatibility.fuzz_v1'}
        default = Registry()
        register_exploratory(default, include_required=False)
        self.assertEqual(set(default.targets), legacy_targets)
        with self.assertRaises(KeyError):
            default.get_target(F2_WINDOWS_TARGET)
        with self.assertRaises(KeyError):
            default.get_services(F2_WINDOWS_TARGET)
        # The runtime directory describes routes; it does not grant registry access.
        self.assertEqual(set(EXPLORATORY_TARGETS), legacy_targets | {F2_WINDOWS_TARGET})
        opted = Registry()
        register_exploratory(opted, include_required=False,
                             runtime_capability=F2_WINDOWS_CAPABILITY)
        self.assertEqual(set(opted.targets), legacy_targets | {F2_WINDOWS_TARGET})
        self.assertEqual({tid: opted.get_target(tid).to_dict() for tid in legacy_targets},
                         {tid: default.get_target(tid).to_dict() for tid in legacy_targets})
        self.assertEqual(EXPLORATORY_TARGETS[F2_WINDOWS_TARGET]['runtime_capability'],
                         F2_WINDOWS_CAPABILITY)
        caps = exploratory_runtime_capabilities()
        self.assertFalse(caps['discovery.match_compatibility.fuzz_v1']['implemented'])
        self.assertFalse(caps['discovery.match_compatibility.fuzz_v1']['admitted'])
        self.assertEqual(caps['discovery.match_compatibility.fuzz_v1']['backend'],
                         'unsupported_relation_observation')
        for target in ('discovery.late_endpoint.fuzz_v1',
                       'builtin.discovery_projection_reconciliation.fuzz_v1',
                       'discovery.interruption_recovery.fuzz_v1'):
            self.assertTrue(caps[target]['implemented'])
            self.assertIsNone(caps[target]['available'])
            self.assertFalse(caps[target]['execution_evidence'])

    def test_known_f2_route_does_not_supply_missing_input_authorization(self):
        from r2f.modules.s1.targets import F2_WINDOWS_TARGET

        for capability in (None, "", "f2_all"):
            with self.subTest(capability=capability):
                config = normalize_config(base(
                    target_id=F2_WINDOWS_TARGET, carrier="wire_proxy",
                    mutation_kind="discovery_drop", arm="mutated",
                    runtime_capability=capability, fuzz_surfaces=["E03"],
                    submitted_parameters={}))
                with self.assertRaisesRegex(ValueError,
                        "f2_windows_explicit_capability_and_timing_policy_required"):
                    exploratory_bindings(config)
        caps = exploratory_runtime_capabilities()[F2_WINDOWS_TARGET]
        self.assertTrue(caps["implemented"])
        self.assertFalse(caps["admitted"])
        self.assertIsNone(caps["available"])
        self.assertFalse(caps["execution_evidence"])
        self.assertTrue(caps["admission_requires_live_preflight"])

    def test_unknown_target_remains_blocked(self):
        result = run_pair(base(target_id='discovery.not_a_target'), Path(tempfile.mkdtemp()) / 'state')
        self.assertEqual(result['reason'], 'blocked:target_not_supported')
        self.assertFalse(result['real_execution'])
        self.assertIsNone(result['execution'])

    def test_surface_must_belong_to_target(self):
        with self.assertRaisesRegex(ValueError, 'surface_not_supported_by_target'):
            exploratory_bindings(normalize_config(p01_input(fuzz_surfaces=['P02'])))
        with self.assertRaisesRegex(ValueError, 'surface_not_supported_by_target'):
            exploratory_bindings(normalize_config(projection_input(fuzz_surfaces=['P01'])))

    def test_missing_submitted_parameter_is_never_a_noop(self):
        config = p01_input()
        config['submitted_parameters'] = {'reader_delay_sec': 0.15}
        c = normalize_config(config)
        with self.assertRaisesRegex(ValueError, 'submitted_parameter_missing'):
            exploratory_bindings(c)

    def test_missing_field_falls_back_to_default_and_fails_binding(self):
        config = projection_input()
        del config['qos']['deadline_ns']
        c = normalize_config(config)
        # The adapter applied its default instead of the submitted value: that
        # is a visible binding failure, never a silent pass.
        self.assertEqual(c['qos']['deadline_ns'], ros2_pair.INFINITE_QOS_NS)
        expected = exploratory_bindings(c)
        with self.assertRaisesRegex(ValueError, 'exploratory_binding_normalized_away:qos.deadline_ns'):
            require_binding_preserved(expected, c)

    def test_silent_normalization_is_a_binding_failure(self):
        c = normalize_config(p01_input())
        expected = exploratory_bindings(c)
        normalized = dict(c)
        normalized['reader_delay_sec'] = 0.02  # adapter quietly clamped the delay
        with self.assertRaisesRegex(ValueError, 'exploratory_binding_normalized_away:reader_delay_sec'):
            require_binding_preserved(expected, normalized)

    def test_qos_schema_is_per_target_not_globally_widened(self):
        # Legacy projection target keeps its extended QoS contract.
        legacy = normalize_config(base(target_id='builtin.discovery_projection_reconciliation',
                                       carrier='ros2_endpoint',
                                       mutation_kind='endpoint_qos_variation',
                                       mutation_parameters={},
                                       qos={'reliability': 'reliable', 'durability': 'volatile',
                                            'depth': 64, 'deadline_ns': 1250000000,
                                            'liveliness': 'automatic',
                                            'liveliness_lease_duration_ns': 5000000000}))
        self.assertEqual(legacy['qos']['deadline_ns'], 1250000000)

        # Late-join targets still reject unknown QoS fields.
        with self.assertRaisesRegex(ValueError, 'unsupported_qos_field'):
            normalize_config(base(target_id='discovery.late_endpoint',
                                  qos={'reliability': 'reliable', 'durability': 'volatile',
                                       'depth': 32, 'deadline_ns': 1250000000}))
        with self.assertRaisesRegex(ValueError, 'unsupported_qos_field'):
            normalize_config(base(target_id='discovery.interruption_recovery',
                                  carrier='wire_proxy', mutation_kind='discovery_drop',
                                  qos={'reliability': 'reliable', 'durability': 'volatile',
                                       'depth': 32, 'liveliness': 'manual_by_topic'}))

    def test_invalid_exploratory_qos_values_are_rejected_before_execution(self):
        for overrides in ({'deadline_ns': -1}, {'deadline_ns': True},
                          {'deadline_ns': 9223372036854775808},
                          {'liveliness': 'manual'}, {'history': 'keep_all'}):
            config = projection_input()
            config['qos'] = dict(config['qos'], **overrides)
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                normalize_config(config)

    def test_g01_is_unsupported_without_relation_observer(self):
        with tempfile.TemporaryDirectory() as temp:
            result = run_pair(base(target_id='discovery.match_compatibility.fuzz_v1',
                                   carrier='ros2_endpoint',
                                   mutation_kind='endpoint_qos_variation',
                                   fuzz_surfaces=['G01'],
                                   submitted_parameters={'writer_qos.reliability': 'best_effort',
                                                         'reader_qos.reliability': 'reliable'},
                                   writer_qos={'reliability': 'best_effort', 'durability': 'volatile',
                                               'depth': 32},
                                   reader_qos={'reliability': 'reliable', 'durability': 'volatile',
                                               'depth': 32}),
                              Path(temp) / 'state')
        self.assertEqual(result['capability_status'], 'unsupported_capability')
        self.assertIn('pair_relation_observation_not_implemented', result['reason'])
        self.assertFalse(result['real_execution'])
        self.assertFalse(result['mutation_receipt']['hit'])
        self.assertFalse(result['mutation_receipt']['applied'])

    def test_control_arm_does_not_require_fuzz_surfaces(self):
        class OfflineLease:
            def __init__(self, *args, **kwargs):
                self.receipts = []

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        config = base(target_id='discovery.late_endpoint.fuzz_v1', arm='baseline',
                      carrier='ros2_endpoint', mutation_kind='late_joiner')
        with patch('r2f.runtime.ros2_pair.LeaseSet', OfflineLease), \
             patch('r2f.runtime.ros2_pair.check_environment',
                   return_value={'ok': False, 'reason': 'offline_missing_environment'}), \
             patch('r2f.runtime.ros2_pair.WorkerPool') as pool:
            result = run_pair(config, Path(tempfile.mkdtemp()) / 'state')
        self.assertEqual(result['reason'], 'offline_missing_environment')
        self.assertEqual(result['exploratory_bindings'], [])
        pool.assert_not_called()


class WorkerQosAdapterTests(unittest.TestCase):
    def test_unknown_public_qos_field_fails_closed(self):
        with self.assertRaisesRegex(ValueError, 'unsupported_public_qos_field'):
            create_public_qos({'reliability': 'reliable', 'durability': 'volatile',
                               'depth': 10, 'deadline_ns': 1, 'invented': 1})

    def test_supported_field_set_is_explicit(self):
        self.assertEqual(PUBLIC_QOS_FIELDS, {'depth', 'history', 'reliability', 'durability',
                                             'deadline_ns', 'liveliness',
                                             'liveliness_lease_duration_ns'})

    def test_self_qos_entry_requires_matching_gid(self):
        graph = {'publishers': [{'endpoint_gid': 'aa', 'qos': {'reliability': 'reliable'}}],
                 'subscriptions': [{'endpoint_gid': 'bb', 'qos': {'durability': 'volatile'}}]}
        self.assertEqual(self_qos_entry(graph, 'writer', 'aa')['qos']['reliability'], 'reliable')
        self.assertEqual(self_qos_entry(graph, 'reader', 'bb')['qos']['durability'], 'volatile')
        self.assertIsNone(self_qos_entry(graph, 'writer', 'bb'))
        self.assertIsNone(self_qos_entry(graph, 'reader', 'aa'))

    def test_observed_self_qos_is_not_derived_from_request(self):
        """A graph entry whose QoS differs must be visible, not overwritten."""
        graph = {'publishers': [{'endpoint_gid': 'aa',
                                 'qos': {'reliability': 'best_effort', 'durability': 'volatile',
                                         'depth': 32}}], 'subscriptions': []}
        entry = self_qos_entry(graph, 'writer', 'aa')
        self.assertEqual(entry['qos']['reliability'], 'best_effort')


class FakeEvents:
    """Minimal event recorder that mirrors the controller 'emit' shape."""

    def __init__(self):
        self.events = []

    def emit(self, event, **fields):
        token = dict(fields, event=event, event_index=len(self.events))
        self.events.append(token)
        return token


class EndpointBindingObservationTests(unittest.TestCase):
    """V35 on the endpoint-pair route, exercised through pure helpers."""

    def test_endpoint_qos_round_trip_is_exact(self):
        config = normalize_config(p01_input())
        for role in ('writer', 'reader'):
            self.assertEqual(config[role + '_qos'], config['qos'])
            self.assertEqual(config[role + '_qos']['depth'], 32)
        self.assertEqual(config['reader_delay_sec'], 0.15)
        self.assertEqual(config['publish_interval_sec'], 0.03)

    def test_projection_role_qos_can_differ_but_stays_declared(self):
        config = normalize_config(projection_input(
            writer_qos={'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
                        'deadline_ns': 1, 'liveliness': 'automatic',
                        'liveliness_lease_duration_ns': 1}))
        self.assertEqual(config['writer_qos']['reliability'], 'reliable')
        self.assertEqual(config['reader_qos']['reliability'], 'best_effort')
        self.assertNotEqual(config['writer_qos'], config['reader_qos'])

    def test_every_named_surface_binds_to_a_distinct_field(self):
        cases = {
            'P02': (projection_input(fuzz_surfaces=['P02'],
                    submitted_parameters={'qos.reliability': 'best_effort'},
                    qos={'reliability': 'best_effort', 'durability': 'volatile', 'depth': 64}),
                    ['qos.reliability']),
            'P03': (projection_input(fuzz_surfaces=['P03'],
                    submitted_parameters={'qos.durability': 'transient_local'},
                    qos={'reliability': 'reliable', 'durability': 'transient_local', 'depth': 64}),
                    ['qos.durability']),
            'P04': (projection_input(fuzz_surfaces=['P04'],
                    submitted_parameters={'qos.deadline_ns': 0},
                    qos={'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
                         'deadline_ns': 0}), ['qos.deadline_ns']),
            'P05': (projection_input(fuzz_surfaces=['P05'],
                    submitted_parameters={'qos.liveliness': 'manual_by_topic'},
                    qos={'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
                         'liveliness': 'manual_by_topic'}), ['qos.liveliness']),
            'P06': (projection_input(fuzz_surfaces=['P06'],
                    submitted_parameters={'qos.liveliness_lease_duration_ns': 9223372036854775807},
                    qos={'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
                         'liveliness_lease_duration_ns': 9223372036854775807}),
                    ['qos.liveliness_lease_duration_ns']),
            'W01': (p01_input(fuzz_surfaces=['W01'],
                    submitted_parameters={'qos.depth': 1, 'pre_match_count': 1,
                                          'post_match_count': 3},
                    qos={'reliability': 'reliable', 'durability': 'volatile', 'depth': 1},
                    pre_match_count=1, post_match_count=3),
                    ['qos.depth', 'pre_match_count', 'post_match_count']),
        }
        for surface_id, (config, fields) in cases.items():
            with self.subTest(surface=surface_id):
                c = normalize_config(config)
                expected = exploratory_bindings(c)
                require_binding_preserved(expected, c)
                self.assertEqual([item['field'] for item in expected], fields)

    def test_e03_anchor_offset_is_bound_not_just_declared(self):
        from test_fuzz_discovery_windows import window
        config = base(target_id='discovery.interruption_recovery.fuzz_v1',
                      carrier='wire_proxy', mutation_kind='discovery_drop',
                      fuzz_surfaces=['E03'],
                      submitted_parameters={'environment_windows.anchor_offset_sec': 0.1},
                      environment_windows=[window(0, trigger_offset_sec=0.1)])
        c = normalize_config(config)
        expected = exploratory_bindings(c)
        require_binding_preserved(expected, c)
        shifted = dict(c, environment_windows=[dict(c['environment_windows'][0],
                                                    trigger_offset_sec=0.5)])
        with self.assertRaisesRegex(ValueError,
                                    'exploratory_binding_normalized_away:'
                                    'environment_windows.anchor_offset_sec'):
            require_binding_preserved(expected, shifted)

    def test_g01_requires_both_role_policies(self):
        config = base(target_id='discovery.match_compatibility.fuzz_v1',
                      carrier='ros2_endpoint', mutation_kind='endpoint_qos_variation',
                      fuzz_surfaces=['G01'],
                      submitted_parameters={'writer_qos.reliability': 'best_effort'},
                      writer_qos={'reliability': 'best_effort', 'durability': 'volatile', 'depth': 32},
                      reader_qos={'reliability': 'reliable', 'durability': 'volatile', 'depth': 32})
        with self.assertRaisesRegex(ValueError, 'submitted_parameter_missing:reader_qos.reliability'):
            exploratory_bindings(normalize_config(config))

    def test_binding_receipt_shape_is_json_serializable(self):
        c = normalize_config(p01_input())
        expected = exploratory_bindings(c)
        require_binding_preserved(expected, c)
        encoded = json.dumps(expected, sort_keys=True)
        self.assertIn('reader_delay_sec', encoded)
        for record in expected:
            self.assertEqual(set(record), {'surface_id', 'field', 'path', 'submitted',
                                           'configured', 'evidence_class'})


if __name__ == '__main__':
    unittest.main()
