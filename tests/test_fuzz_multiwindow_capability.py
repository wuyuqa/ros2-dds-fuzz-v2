"""Offline execution-capability gates, not ROS/mutation-hit evidence.

The route already executes discovery_drop window chains. These tests only
prove that a complete identity-bound plan can reach that route, while missing
capabilities, other routes and generation-only surfaces remain closed.
"""
from copy import deepcopy
from dataclasses import replace
import unittest
from unittest.mock import patch

from r2f.core import compile_scenario
from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import RUNTIME_WINDOW_SCHEMA, compose, validate_plan
from r2f.core.mutators import make_rng, mutate
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.providers import (
    EXPLORATORY_ACTIVATION_CAPABILITIES, ExploratoryInputError,
    S1ExploratoryMutationProvider, validate_exploratory_plan,
)
from r2f.modules.s1.targets import (
    F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET, exploratory_target_declarations,
)
from r2f.runtime.discovery_network import validate_window_plan
from r2f.runtime.ros2_pair import exploratory_runtime_capabilities


RECOVERY = 'discovery.interruption_recovery.fuzz_v1'
ROUTE = dict(target_id=RECOVERY, carrier='wire_proxy', mutation_kind='discovery_drop')
BASE = {'drop_duration_sec': 0.2, 'recovery_timeout_sec': 8.0,
        'counter_interval_sec': 0.25}
REQUIRED_HASHES = (
    'c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940',
    '1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d',
    'fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce',
    'eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0',
)


def windows(count, *, schema=False):
    return [dict(
        **({'schema': RUNTIME_WINDOW_SCHEMA} if schema else {}),
        window_id=f'w{index}', window_index=index,
        trigger_anchor='fresh_match' if index == 0 else 'previous_restore_confirmed',
        trigger_offset_sec=index * 0.01, drop_duration_sec=0.2,
        restore_interval_sec=0.02,
    ) for index in range(count)]


def core_windows(environment):
    return [dict(schema=RUNTIME_WINDOW_SCHEMA, **item) for item in environment]


class MultiwindowCore(unittest.TestCase):
    def setUp(self):
        self.contract = fuzz_catalog.exploration_contract(('E01',))

    def plan(self, count=2, *, environment=None, planned=None, legacy=False):
        parent = dict(BASE)
        if not legacy:
            parent['environment_windows'] = deepcopy(
                windows(count) if environment is None else environment)
        return compose(parent, [], self.contract, runtime_windows=(
            windows(count, schema=True) if planned is None else planned))

    def assert_invalid(self, plan, reason):
        result = validate_plan(plan, {'multi_window': True}, self.contract)
        self.assertEqual(result.status, 'invalid', result.reasons)
        self.assertIn(reason, ' '.join(result.reasons))
        self.assertFalse(result.executable)

    def test_one_window_keeps_legacy_without_explicit_capability(self):
        for legacy in (False, True):
            plan = self.plan(1, legacy=legacy)
            before = plan.to_dict(), plan.plan_hash()
            for caps in (None, {}, {'multi_window': False}, {'multi_window': True}):
                with self.subTest(legacy=legacy, caps=caps):
                    self.assertEqual(validate_plan(plan, caps, self.contract).status, 'valid')
                    self.assertEqual((plan.to_dict(), plan.plan_hash()), before)

    def test_two_and_three_require_literal_true_capability(self):
        for count in (2, 3):
            plan = self.plan(count)
            for caps in (None, {}, {'multi_window': False}, {'multi_window': 1},
                         {'multi_window': 'true'}, {'multi_window': None}):
                with self.subTest(count=count, caps=caps):
                    result = validate_plan(plan, caps, self.contract)
                    self.assertEqual(result.status, 'unsupported_capability', result.reasons)
                    self.assertFalse(result.executable)
            result = validate_plan(plan, {'multi_window': True}, self.contract)
            self.assertEqual(result.status, 'valid', result.reasons)
            self.assertTrue(result.executable)
            self.assertEqual(validate_window_plan(plan.final_config['environment_windows']),
                             plan.final_config['environment_windows'])

    def test_four_windows_and_invalid_capability_limits_are_refused(self):
        for count, maximum in ((4, 3), (3, 2), (2, 1), (2, 4), (2, True), (2, None)):
            with self.subTest(count=count, maximum=maximum):
                result = validate_plan(self.plan(count), {
                    'multi_window': True, 'max_runtime_windows': maximum}, self.contract)
                self.assertEqual(result.status, 'unsupported_capability', result.reasons)
                self.assertFalse(result.executable)

    def test_either_window_representation_can_reveal_a_multiwindow_request(self):
        for count in (0, 1, 3):
            plan = self.plan(2, planned=windows(count, schema=True))
            self.assertEqual(validate_plan(plan).status, 'unsupported_capability')
            self.assert_invalid(plan, 'count mismatch')

    def test_multiwindow_requires_final_config_materialized_list(self):
        self.assert_invalid(self.plan(2, legacy=True), 'count mismatch')
        self.assert_invalid(self.plan(2, environment={'count': 2}), 'count mismatch')

    def test_planconfig_order_and_every_window_field_are_bound(self):
        for key, value in (
                ('window_id', 'another'), ('window_index', 2),
                ('trigger_anchor', 'fresh_match'), ('trigger_offset_sec', 0.1),
                ('drop_duration_sec', 0.3), ('restore_interval_sec', 0.4)):
            environment = windows(2)
            environment[1][key] = value
            with self.subTest(key=key):
                self.assert_invalid(self.plan(environment=environment), 'differ at window 1')
        self.assert_invalid(self.plan(environment=list(reversed(windows(2)))), 'differ at window 0')

    def test_multiwindow_contract_does_not_fill_defaults_or_allow_extra_fields(self):
        for key in windows(2)[0]:
            environment = windows(2)
            del environment[0][key]
            with self.subTest(key=key):
                self.assert_invalid(self.plan(environment=environment), 'missing field')
        environment = windows(2)
        environment[0]['schema'] = RUNTIME_WINDOW_SCHEMA
        self.assert_invalid(self.plan(environment=environment), 'unknown field')

    def test_contiguous_indices_and_restore_anchors_are_required(self):
        for index, key, value, reason in (
                (0, 'window_index', 1, 'sequential'),
                (1, 'window_index', 3, 'sequential'),
                (0, 'trigger_anchor', 'pre_match', 'fresh_match'),
                (1, 'trigger_anchor', 'fresh_match', 'previous_restore_confirmed'),
                (1, 'trigger_anchor', 'pre_match', 'previous_restore_confirmed')):
            environment = windows(2)
            environment[index][key] = value
            with self.subTest(index=index, key=key, value=value):
                self.assert_invalid(self.plan(environment=environment,
                                              planned=core_windows(environment)), reason)

    def test_multiwindow_timing_cannot_exceed_namespace_bounds(self):
        for key, value in (('drop_duration_sec', 0), ('drop_duration_sec', 30.01),
                           ('trigger_offset_sec', 30.01), ('restore_interval_sec', 30.01)):
            environment = windows(2)
            environment[0][key] = value
            with self.subTest(key=key, value=value):
                self.assert_invalid(self.plan(environment=environment,
                                              planned=core_windows(environment)), key)

    def test_config_booleans_cannot_compare_equal_to_numeric_plan_values(self):
        for key, value in (('window_index', False), ('trigger_offset_sec', False),
                           ('drop_duration_sec', True), ('restore_interval_sec', False)):
            environment = windows(2)
            environment[0][key] = value
            with self.subTest(key=key):
                self.assert_invalid(self.plan(environment=environment), key)

    def test_duplicate_ids_and_nonfinite_or_negative_timing_are_invalid(self):
        for key, value in (('window_id', 'w0'), ('trigger_offset_sec', -1),
                           ('drop_duration_sec', float('nan')),
                           ('restore_interval_sec', float('inf'))):
            planned = windows(2, schema=True)
            planned[1][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.plan(planned=planned)

    def test_no_capability_change_can_bypass_input_operation_hash_chain(self):
        plan = self.plan()
        data = plan.to_dict()
        data['final_config']['environment_windows'][0]['drop_duration_sec'] = 0.3
        result = validate_plan(data, {'multi_window': True}, self.contract)
        self.assertEqual(result.status, 'invalid')
        self.assertIn('final operation', ' '.join(result.reasons))


class MultiwindowProvider(unittest.TestCase):
    def setUp(self):
        self.targets = exploratory_target_declarations()
        self.provider = S1ExploratoryMutationProvider(self.targets)
        self.contract = self.provider.contract_for(RECOVERY)

    def plan(self, count=2, *, legacy=False, environment_mutation=None):
        parent = dict(BASE)
        if not legacy:
            parent['environment_windows'] = windows(count)
        if environment_mutation is not None:
            parent['environment_mutation'] = environment_mutation
        proposal = mutate(parent, self.contract.surface_by_id['E01'], make_rng(7),
                          {'boundary_class': 'log_range'}, 'M03')
        materialized = windows(count)
        for window in materialized:
            window['drop_duration_sec'] = proposal.input_after['drop_duration_sec']
        if not legacy:
            proposal = replace(proposal, changes={**proposal.changes,
                                                  'environment_windows': materialized},
                               input_after={**proposal.input_after,
                                            'environment_windows': materialized})
        return compose(parent, [proposal], self.contract, rng_seed=7,
                       runtime_windows=core_windows(materialized))

    def test_provider_prepares_one_two_three_without_execution_evidence(self):
        for count in (1, 2, 3):
            plan = self.plan(count)
            scenario = compile_scenario(self.targets[RECOVERY], 'rmw_cyclonedds_cpp',
                                        'mutated', 0, 7,
                                        {'mutation_plan': plan.to_dict()})
            before = plan.to_dict(), plan.plan_hash()
            prepared = self.provider.prepare_plan(scenario, plan)
            self.assertEqual(prepared['plan_status'], 'valid')
            self.assertEqual(prepared['final_input'], plan.final_config)
            self.assertEqual(prepared['input_layer_receipts'][0]['runtime_windows'],
                             plan.runtime_windows)
            self.assertNotIn('mutation_receipt', prepared)
            self.assertEqual((plan.to_dict(), plan.plan_hash()), before)

    def test_admitted_chains_use_existing_namespace_normalizer_unchanged(self):
        # This invokes only the pure normalizer, never its execution route.
        from r2f.runtime.namespace_pair import normalize_discovery_config

        for count in (1, 2, 3):
            plan = self.plan(count)
            self.assertTrue(validate_exploratory_plan(plan, self.contract, **ROUTE).executable)
            normalized = normalize_discovery_config({**plan.final_config, "mutation_parameters": {}})
            self.assertEqual(normalized["runtime_windows"], plan.final_config["environment_windows"])
            self.assertTrue(normalized["window_chain_requested"])
            self.assertNotIn("mutation_receipt", normalized)

    def test_legacy_singlewindow_omits_environment_list_and_keeps_same_hash(self):
        plan = self.plan(1, legacy=True)
        before = plan.to_dict(), plan.plan_hash()
        result = validate_exploratory_plan(plan.to_dict(), self.contract, **ROUTE)
        self.assertTrue(result.executable)
        self.assertNotIn('environment_windows', plan.final_config)
        self.assertEqual((plan.to_dict(), plan.plan_hash()), before)

    def test_route_must_explicitly_be_recovery_discovery_namespace_wire_proxy(self):
        for overrides in ({'target_id': None}, {'target_id': 'discovery.interruption_recovery'},
                          {'target_id': 'discovery.late_endpoint.fuzz_v1'},
                          {'target_id': 'builtin.discovery_projection_reconciliation.fuzz_v1'},
                          {'carrier': None}, {'carrier': 'ros2_endpoint'},
                          {'mutation_kind': 'endpoint_qos_variation'}):
            route = {**ROUTE, **overrides}
            with self.subTest(route=route), self.assertRaisesRegex(
                    ExploratoryInputError, 'multi-window execution is not enabled'):
                validate_exploratory_plan(self.plan(), self.contract, **route)
        with self.assertRaises(ExploratoryInputError):
            validate_exploratory_plan(self.plan(), self.contract)

    def test_runtime_missing_false_or_nonliteral_capability_is_not_enabled(self):
        for value in (None, False, 1, 'true'):
            capabilities = exploratory_runtime_capabilities()
            if value is None:
                del capabilities[RECOVERY]['multi_window']
            else:
                capabilities[RECOVERY]['multi_window'] = value
            with self.subTest(value=value), patch(
                    'r2f.runtime.ros2_pair.exploratory_runtime_capabilities',
                    return_value=capabilities), self.assertRaisesRegex(
                        ExploratoryInputError, 'multi-window execution is not enabled'):
                validate_exploratory_plan(self.plan(), self.contract, **ROUTE)

    def test_incorrect_runtime_route_or_undeclared_limit_is_not_enabled(self):
        for key, value in (('implemented', False), ('backend', 'endpoint_pair'),
                           ('carrier', 'ros2_endpoint'), ('mutation_kind', 'late_joiner'),
                           ('max_runtime_windows', None), ('max_runtime_windows', 4)):
            capabilities = exploratory_runtime_capabilities()
            capabilities[RECOVERY][key] = value
            with self.subTest(key=key), patch(
                    'r2f.runtime.ros2_pair.exploratory_runtime_capabilities',
                    return_value=capabilities), self.assertRaises(ExploratoryInputError):
                validate_exploratory_plan(self.plan(), self.contract, **ROUTE)

    def test_multiwindow_does_not_activate_netem(self):
        plan = self.plan(environment_mutation={'kind': 'discovery_netem', 'spec': {}})
        with self.assertRaisesRegex(ExploratoryInputError, 'multi-window execution is not enabled'):
            validate_exploratory_plan(plan, self.contract, **ROUTE)
        drop = self.plan(environment_mutation={'kind': 'discovery_drop'})
        self.assertTrue(validate_exploratory_plan(drop, self.contract, **ROUTE).executable)

    def test_existing_mutator_surface_binding_activation_gates_are_preserved(self):
        plan = self.plan()
        for limits in ({'mutators': ()}, {'surface_kinds': ('relation',)},
                       {'runtime_bindings': ()}, {'activation': ('F2',)}):
            result = validate_plan(plan, {'multi_window': True, **limits}, self.contract)
            with self.subTest(limits=limits):
                self.assertEqual(result.status, 'unsupported_capability', result.reasons)
                self.assertFalse(result.executable)
        self.assertEqual(EXPLORATORY_ACTIVATION_CAPABILITIES['activation'], ('F1',))
        self.assertEqual(EXPLORATORY_ACTIVATION_CAPABILITIES['mutators'], ('M01', 'M02', 'M03'))
        self.assertFalse(EXPLORATORY_ACTIVATION_CAPABILITIES['multi_window'])

    def test_global_capability_cannot_expand_other_routes(self):
        with patch.dict(EXPLORATORY_ACTIVATION_CAPABILITIES, multi_window=True):
            for target_id in ("discovery.late_endpoint.fuzz_v1",
                              "discovery.interruption_recovery"):
                with self.subTest(target_id=target_id), self.assertRaises(ExploratoryInputError):
                    validate_exploratory_plan(self.plan(), self.contract,
                                              **{**ROUTE, "target_id": target_id})

    def test_generation_only_surfaces_remain_outside_live_contracts(self):
        # The legacy/default registry and F1 projection still refuse F2. The
        # separate explicit target must not leak into these unchanged paths.
        self.assertNotIn(F2_WINDOWS_TARGET, self.targets)
        opted_in = exploratory_target_declarations(runtime_capability=F2_WINDOWS_CAPABILITY)
        self.assertEqual(set(opted_in), set(self.targets) | {F2_WINDOWS_TARGET})
        for target_id, target in self.targets.items():
            self.assertEqual(opted_in[target_id].to_dict(), target.to_dict())
        live = opted_in[F2_WINDOWS_TARGET]
        self.assertEqual(live.observation_contract['runtime_capability'], F2_WINDOWS_CAPABILITY)
        self.assertEqual({sid: surface.mutator_ids for sid, surface
                          in live.exploration_contract.surface_by_id.items()},
                         {'E02': ['M07'], 'E03': ['M06']})
        for sid in ('E02', 'E03', 'G01'):
            self.assertNotIn(sid, self.contract.surface_by_id)
            with self.subTest(sid=sid), self.assertRaises(fuzz_catalog.UnsupportedCapabilityError):
                fuzz_catalog.surface_to_core(fuzz_catalog.SURFACES[sid])
        compatibility = self.provider.contract_for('discovery.match_compatibility.fuzz_v1')
        # Only the separately selected G01 revision1 target gains M08.
        self.assertEqual(set(compatibility.surface_by_id), {'G01'})
        self.assertEqual(compatibility.surface_by_id['G01'].mutator_ids, ['M08'])
        self.assertTrue(compatibility.surface_by_id['G01'].executable)
        self.assertEqual(self.targets['discovery.match_compatibility.fuzz_v1'].target_revision, 1)

    def test_runtime_advertises_only_recovery_route_and_never_live_admission(self):
        capabilities = exploratory_runtime_capabilities()
        # Legacy E01 window chains remain distinct from explicit E02/E03 F2.
        recovery_routes = {RECOVERY, F2_WINDOWS_TARGET}
        self.assertEqual({target_id for target_id, cap in capabilities.items()
                          if cap['multi_window'] is True}, recovery_routes)
        for target_id, capability in capabilities.items():
            with self.subTest(target_id=target_id):
                self.assertIs(capability['multi_window'], target_id in recovery_routes)
                self.assertEqual(capability['max_runtime_windows'],
                                 3 if target_id in recovery_routes else 1)
                if target_id in recovery_routes:
                    self.assertIs(capability['implemented'], True)
                    self.assertEqual(capability['backend'], 'discovery_namespace')
                    self.assertEqual(capability['carrier'], 'wire_proxy')
                    self.assertEqual(capability['mutation_kind'], 'discovery_drop')
                if target_id == F2_WINDOWS_TARGET:
                    self.assertEqual(capability['runtime_capability'], F2_WINDOWS_CAPABILITY)
                    self.assertEqual(capability['surfaces'], ['E02', 'E03'])
                else:
                    self.assertNotIn('runtime_capability', capability)
                self.assertFalse(capability['admitted'])
                self.assertTrue(capability['admission_requires_live_preflight'])
                self.assertFalse(capability['execution_evidence'])

    def test_required_catalog_targets_revision_and_denominator_are_unchanged(self):
        module = S1Module()
        self.assertEqual(canonical_hash(module.to_dict()),
                         '947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6')
        self.assertEqual(len(module.targets) * 2 * 3 * 3, 72)
        for target, digest in zip(module.targets.values(), REQUIRED_HASHES):
            self.assertEqual(target.target_revision, 0)
            self.assertIsNone(target.exploration_contract)
            self.assertEqual(canonical_hash(target.to_dict()), digest)


if __name__ == '__main__':
    unittest.main()
