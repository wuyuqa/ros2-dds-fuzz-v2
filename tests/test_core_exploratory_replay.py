"""Core plan-aware replay contracts using explicitly offline, raw-backed fixtures.

Nothing here starts ROS/a campaign or establishes runtime qualification. The
full campaign attempt/audit boundary is exercised in the companion test file.
"""
import copy
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from r2f.campaign.identity import semantic_input
from r2f.campaign.replay import _bind_execution_input, _registered_target, _replay_config
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.core import ReplayCapsule, canonical_hash, compile_scenario
from r2f.core.evidence import INPUT_SCHEMA_V2
from r2f.core.replay import conformance_signature, rerun_conformance
from r2f.modules.s1.module import S1Module
from test_campaign_exploratory_replay import (
    RMW, RECOVERY, _ReplayFixtures, healthy, missing_middle, reseal_window_input,
)


class CoreExploratoryReplayContracts(_ReplayFixtures):
    def callback(self, capsule, *, fixture=healthy, change=None):
        def run(scenario, domain):
            config = _replay_config(capsule, 'offline-core-' + str(domain), domain, self.source)
            result = fixture(config, self.root / ('core-runtime-' + str(domain)))
            row = ingest_execution(scenario, config, result, capsule,
                                   services=_registered_target(capsule)[1])
            _bind_execution_input(row, config, capsule)
            row.update(domain=domain, domain_id=domain)
            if change:
                change(scenario, row)
            return row
        return run

    def test_unaware_callback_still_refuses_v2_without_running(self):
        capsule, _ = self.case()
        before = copy.deepcopy(capsule.input)
        callback = Mock(side_effect=AssertionError('unsupported callback must not run'))
        attempt = rerun_conformance(capsule, callback, domain=81)
        self.assertEqual(attempt['status'], 'fail_artifact')
        self.assertEqual(attempt['reason_code'], 'v2_plan_aware_runner_required')
        callback.assert_not_called()
        self.assertEqual(capsule.input, before)
        self.assertEqual(capsule.replay_attempts, [attempt])
        self.assertIsNone(capsule.execution)

    def test_supported_callback_receives_exact_saved_plan_and_submits_bound_input(self):
        capsule, path = self.case(chain=True)
        before = copy.deepcopy(capsule.input)
        invoke = self.callback(capsule)
        seen = []

        def callback(scenario, domain):
            seen.append((scenario.input_identity_schema, scenario.mutation_plan.to_dict(),
                         copy.deepcopy(scenario.final_input), copy.deepcopy(scenario.input_layer_receipts),
                         copy.deepcopy(scenario.source_manifest), scenario.effective_input_sha256))
            return invoke(scenario, domain)

        with patch('r2f.core.mutators.mutate', side_effect=AssertionError('no regeneration')):
            attempt = rerun_conformance(capsule, callback, domain=81, plan_aware=True)
        self.assertEqual(attempt['status'], 'pass', attempt)
        self.assertEqual(attempt['comparison_basis'], 'execution_success_only')
        self.assertFalse(attempt['comparison_matches_original'])
        self.assertEqual(seen, [(INPUT_SCHEMA_V2, before['mutation_plan'],
                                 before['effective_input']['final_input'],
                                 before['effective_input']['input_layer_receipts'],
                                 before['source_manifest'], before['effective_input_sha256'])])
        self.assertEqual(capsule.input, before)
        self.assertEqual(capsule.execution, attempt['execution_metadata'])
        self.assertEqual(capsule.execution['input_identity_schema'], INPUT_SCHEMA_V2)
        self.assertEqual(capsule.execution['actual_submitted_payload_sha256'],
                         canonical_hash(before['effective_input']['final_input']))
        self.assertEqual(ReplayCapsule.from_dict(capsule.to_dict()).to_dict(), capsule.to_dict())
        self.assertNotIn('real_ros_execution', attempt)
        self.assert_immutable()

    def test_planless_v2_factory_rejects_instead_of_downgrading(self):
        target = S1Module().targets['discovery.late_endpoint']
        scenario = compile_scenario(target, RMW, 'mutated', 0, seed=7)
        with self.assertRaisesRegex(ValueError, 'explicit mutation_plan'):
            ReplayCapsule.from_scenario(
                scenario, source_manifest=self.source, final_input=semantic_input('mutated'),
                origin_domain=80, input_schema=INPUT_SCHEMA_V2)

    def test_capsule_plan_or_final_input_tampering_is_a_retained_artifact_failure(self):
        for section in ('mutation_plan', 'effective_input'):
            capsule, _ = self.case()
            if section == 'mutation_plan':
                capsule.input[section]['operations'][0]['surface_id'] = 'unknown'
            else:
                capsule.input[section]['final_input']['reader_delay_sec'] = 99
            callback = Mock()
            attempt = rerun_conformance(capsule, callback, domain=81, plan_aware=True)
            self.assertEqual(attempt['status'], 'fail_artifact')
            self.assertIn(attempt, capsule.replay_attempts)
            callback.assert_not_called()
            self.assertIsNone(capsule.execution)

    def test_self_consistent_multiwindow_structure_is_rejected_before_callback(self):
        from r2f.core.evidence import effective_input_hash
        from r2f.core.mutation_plan import validate_plan
        from r2f.core import Scenario
        for change in (lambda windows: windows.pop(),
                       lambda windows: windows[1].update(restore_interval_sec=0.75)):
            capsule, _ = self.case(RECOVERY, windows=2)
            data = reseal_window_input(capsule, change)
            self.assertEqual(data['effective_input_sha256'],
                             effective_input_hash(data['effective_input'], schema=INPUT_SCHEMA_V2))
            plan = Scenario.from_dict(data['scenario']).mutation_plan
            # This is the exact early unsupported result that used to hide
            # the structural mismatch when no live capabilities were supplied.
            self.assertEqual(validate_plan(plan, contract=Scenario.from_dict(data['scenario']).target.exploration_contract).status,
                             'unsupported_capability')
            with self.assertRaisesRegex(ValueError, 'runtime_windows/environment_windows'):
                ReplayCapsule.from_dict(dict(capsule.to_dict(), input=data))
            capsule.input = data
            before = copy.deepcopy(data)
            callback = Mock(side_effect=AssertionError('bad plan must not execute'))
            attempt = rerun_conformance(capsule, callback, domain=81, plan_aware=True)
            self.assertEqual(attempt['status'], 'fail_artifact', attempt)
            self.assertIn('runtime_windows/environment_windows', attempt['failure_reason'])
            self.assertEqual(capsule.input, before)
            self.assertEqual(capsule.replay_attempts, [attempt])
            self.assertIsNone(capsule.execution)
            callback.assert_not_called()

    def test_actual_input_plan_identity_source_hash_and_domain_cannot_be_substituted(self):
        changes = {
            'actual_run': False, 'execution_source': 'fixture',
            'input_identity_schema': 'r2f.input-identity/v1',
            'effective_input_sha256': '0' * 64, 'source_manifest': {'head': 'other'},
            'final_input': {'rmws': {'reader': 'rmw_fastrtps_cpp'}},
            'actual_submitted_payload_sha256': 'f' * 64,
            'input_layer_receipts': [], 'mutation_plan': {}, 'mutation_parameters': {},
            'state_steps': [], 'repeat': False, 'domain': 82, 'domain_id': 82,
        }
        for index, (field, value) in enumerate(changes.items()):
            capsule, _ = self.case()
            before = copy.deepcopy(capsule.input)
            with self.subTest(field=field):
                attempt = rerun_conformance(
                    capsule, self.callback(capsule, change=lambda scenario, row: row.update({field: value})),
                    domain=100 + index, plan_aware=True)
                self.assertEqual(attempt['status'], 'fail_artifact', attempt)
                self.assertEqual(attempt['execution_metadata'][field], value)
                self.assertIsNone(capsule.execution)
                self.assertEqual(capsule.input, before)
                self.assert_immutable()

    def test_plan_aware_callback_must_report_actual_submission_not_just_a_copied_hash(self):
        for field in ('final_input', 'actual_submitted_payload_sha256', 'input_layer_receipts'):
            capsule, _ = self.case()
            with self.subTest(field=field):
                attempt = rerun_conformance(
                    capsule, self.callback(capsule, change=lambda scenario, row: row.pop(field)),
                    domain=80 + self.serial, plan_aware=True)
                self.assertEqual(attempt['status'], 'fail_artifact', attempt)
                self.assertIn('actual input mismatch', attempt['failure_reason'])
                self.assertIsNone(capsule.execution)

    def test_record_execution_validates_the_actual_plan_and_every_supplied_binding(self):
        capsule, _ = self.case()
        from r2f.core import Scenario
        scenario = Scenario.from_dict(capsule.input['scenario'])
        row = self.callback(capsule)(scenario, 81)
        for field, value in (
                ('mutation_plan', dict(row['mutation_plan'], final_config={'topic': '/wrong'})),
                ('mutation_parameters', {'reader_delay_sec': 999}),
                ('effective_input', {}), ('operations', []), ('runtime_windows', [{'wrong': True}]),
                ('contract_hash', 'e' * 64), ('mutation_plan_sha256', 'f' * 64),
                ('final_input', dict(row['final_input'], reader_delay_sec=999)),
                ('actual_submitted_payload_sha256', 'd' * 64)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                capsule.record_execution(dict(row, **{field: value}))
            self.assertIsNone(capsule.execution)
        capsule.record_execution(row)
        self.assertEqual(capsule.execution, row)

    def test_new_domain_validation_and_history_are_retained_without_callback(self):
        for domain in (80, -1, 232, True, '81'):
            capsule, _ = self.case()
            callback = Mock()
            with self.subTest(domain=domain):
                attempt = rerun_conformance(capsule, callback, domain=domain, plan_aware=True)
                self.assertEqual(attempt['status'], 'fail_environment')
                callback.assert_not_called()
                self.assertEqual(len(capsule.replay_attempts), 1)
        capsule, _ = self.case()
        first = rerun_conformance(capsule, self.callback(capsule), domain=81, plan_aware=True)
        second = rerun_conformance(capsule, self.callback(capsule), domain=82, plan_aware=True)
        self.assertEqual((first['status'], second['status']), ('pass', 'pass'))
        callback = Mock()
        attempt = rerun_conformance(capsule, callback, domain=82, plan_aware=True)
        self.assertEqual(attempt['reason_code'], 'fresh_domain_required')
        callback.assert_not_called()
        self.assertEqual(len(capsule.execution_history), 1)

    def test_runtime_failures_are_retained_without_deleting_original_execution(self):
        capsule, _ = self.executed_case()
        capsule.mark_non_replayable('offline-original-disposition', 'fixture only, not ROS evidence')
        before = copy.deepcopy(capsule.to_dict())
        for domain, error, expected in ((81, TimeoutError('offline deadline'), 'fail_timing'),
                                        (82, RuntimeError('offline tool failure'), 'fail_environment')):
            attempt = rerun_conformance(capsule, Mock(side_effect=error), domain=domain, plan_aware=True)
            self.assertEqual(attempt['status'], expected)
            self.assertIn(str(error), attempt['failure_reason'])
            self.assertEqual(capsule.execution, before['execution'])
            self.assertEqual(capsule.non_replayable, before['non_replayable'])
            self.assertEqual(capsule.input, before['input'])
            self.assertEqual(capsule.execution_history, [])
        self.assertEqual(len(capsule.replay_attempts), 2)
        self.assert_immutable()

    def test_raw_hash_mismatch_is_not_execution_success(self):
        capsule, _ = self.case()

        def change(scenario, row):
            raw = Path(row['raw_path'])
            raw.write_text(raw.read_text() + '{}\n')

        attempt = rerun_conformance(capsule, self.callback(capsule, change=change), domain=81, plan_aware=True)
        self.assertEqual(attempt['status'], 'fail_artifact')
        self.assertIn('SHA256', attempt['failure_reason'])
        self.assertIsNone(capsule.execution)
        self.assertIn('raw_sha256', attempt['execution_metadata'])
        self.assert_immutable()

    def test_same_typed_failure_matches_but_a_new_pass_does_not_confirm_it(self):
        capsule, _ = self.executed_case(missing_middle)
        baseline = copy.deepcopy(capsule.execution)
        attempt = rerun_conformance(capsule, self.callback(capsule, fixture=missing_middle),
                                    domain=81, plan_aware=True)
        self.assertEqual(attempt['status'], 'pass', attempt)
        self.assertTrue(attempt['comparison_matches_original'])
        self.assertEqual(attempt['execution_metadata']['oracle_verdict'], 'fail')
        self.assertEqual(attempt['original_signature']['schema'], 'r2f.conformance-signature/v2')
        attempt = rerun_conformance(capsule, self.callback(capsule), domain=82, plan_aware=True)
        self.assertEqual(attempt['status'], 'inconclusive')
        self.assertFalse(attempt['comparison_matches_original'])
        self.assertEqual(capsule.execution, baseline)
        self.assertEqual(len(capsule.execution_history), 2)
        self.assert_immutable()

    def test_runner_cannot_change_supplied_scenario_or_capsule_input(self):
        capsule, _ = self.case()
        before = copy.deepcopy(capsule.input)

        def change_scenario(scenario, row):
            scenario.final_input['reader_delay_sec'] = 999

        attempt = rerun_conformance(capsule, self.callback(capsule, change=change_scenario),
                                    domain=81, plan_aware=True)
        self.assertEqual(attempt['status'], 'fail_artifact')
        self.assertIn('sealed scenario', attempt['failure_reason'])
        self.assertEqual(capsule.input, before)
        self.assertIsNone(capsule.execution)

        def change_capsule(scenario, row):
            capsule.input['source_manifest']['head'] = 'changed'

        attempt = rerun_conformance(capsule, self.callback(capsule, change=change_capsule),
                                    domain=82, plan_aware=True)
        self.assertEqual(attempt['status'], 'fail_artifact')
        self.assertEqual(capsule.input, before)
        self.assertIsNone(capsule.execution)

    def test_capsule_input_is_restored_even_when_mutating_callback_raises(self):
        capsule, _ = self.case()
        before = copy.deepcopy(capsule.input)

        def callback(scenario, domain):
            capsule.input['source_manifest']['head'] = 'changed'
            raise RuntimeError('also failed')

        attempt = rerun_conformance(capsule, callback, domain=81, plan_aware=True)
        self.assertEqual(attempt['status'], 'fail_artifact')
        self.assertEqual(capsule.input, before)
        self.assertIn('also failed', attempt['runner_error'])
        self.assertIsNone(capsule.execution)


class StableV2SignatureContracts(unittest.TestCase):
    def projection(self, *, guid='live-guid-A', generation='generation-A', field='deadline', value=2):
        return {'input_identity_schema': INPUT_SCHEMA_V2, 'target_id': 'projection.fixture',
                'target_revision': 0, 'effective_input_sha256': 'a' * 64,
                'oracle_verdict': 'fail', 'rule_id': 'projection.primary',
                'typed_facts': {'writer': {'id': guid}, 'projection': {
                    'differences': [{'endpoint_guid': guid, 'generation': generation,
                                     'field': field, 'equal': False,
                                     'wire': {'status': 'observed', 'value': 1, 'raw': '/old-path'},
                                     'graph_value': value, 'graph_raw': '/new-path'}]}}}

    def test_projection_signature_uses_role_field_values_not_live_guid_generation_or_paths(self):
        first = self.projection()
        second = self.projection(guid='new-guid', generation='new-generation')
        self.assertEqual(conformance_signature(first), conformance_signature(second))
        self.assertNotEqual(conformance_signature(first), conformance_signature(self.projection(field='durability')))
        self.assertNotEqual(conformance_signature(first), conformance_signature(self.projection(value=3)))
        signature = str(conformance_signature(first))
        for transient in ('live-guid-A', 'generation-A', '/old-path', '/new-path'):
            self.assertNotIn(transient, signature)

    def test_projection_generic_fail_or_unbound_role_is_insufficient_for_confirmation(self):
        row = self.projection()
        row['typed_facts']['projection']['differences'] = []
        self.assertIsNone(conformance_signature(row))
        row = self.projection()
        row['typed_facts'].pop('writer')
        self.assertIsNone(conformance_signature(row))

    def test_v2_execution_identity_participates_in_signature_not_v1_serialization(self):
        row = self.projection()
        changed = dict(row, effective_input_sha256='b' * 64)
        self.assertNotEqual(conformance_signature(row), conformance_signature(changed))
        for data in (row, changed):
            data.pop('input_identity_schema')
            data['typed_anomaly_signature'] = {'stable_failure': True}
        self.assertEqual(conformance_signature(row), conformance_signature(changed))
        self.assertEqual(conformance_signature(row)['schema'], 'r2f.conformance-signature/v1')


if __name__ == '__main__':
    unittest.main()
