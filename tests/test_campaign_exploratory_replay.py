"""Offline replay contracts only: fixture tokens never establish real ROS evidence.

No campaign, ROS process, namespace, socket, mutating carrier or external profile
is launched. Saved plans are composed in memory; every runtime is injected or
intercepted before the existing namespace/backend execution boundary.
"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from r2f.campaign.identity import semantic_input
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.campaign.replay import (
    DEFAULT_RUNTIME, _bind_execution_input, _check_source, _digest, _qualification, _registered_target,
    _replay_config, _worker_counts, _worker_qualification, audit_replay, replay_cell,
)
from r2f.core import Registry, ReplayCapsule, Scenario, canonical_hash, compile_scenario, raw_sha256
from r2f.core.mutation_plan import compose
from r2f.core.mutators import make_rng, mutate
from r2f.modules.s1.module import S1Module, register_exploratory
from r2f.modules.s1.providers import S1ExploratoryMutationProvider
from r2f.modules.s1.targets import EXPLORATORY_RUNTIME_TARGET
from r2f.runtime.ros2_pair import role_rmw
from test_campaign_evidence import fixture_pair

REPO = Path(__file__).resolve().parents[1]
LATE = 'discovery.late_endpoint.fuzz_v1'
JOIN = 'discovery.late_join_delivery.fuzz_v1'
RECOVERY = 'discovery.interruption_recovery.fuzz_v1'
PROJECTION = 'builtin.discovery_projection_reconciliation.fuzz_v1'
RMW = 'rmw_cyclonedds_cpp'
OTHER_RMW = 'rmw_fastrtps_cpp'


def healthy(config, directory):
    return fixture_pair(config, directory, fresh_matches=True)


def blocked(config, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    event = {'event': 'runtime_end', 'event_index': 0, 'run_id': config['run_id'],
             'domain_id': config['domain_id'], 'real_execution': False,
             'reason': 'domain_conflict:offline_fixture'}
    raw = directory / 'raw.jsonl'
    raw.write_text(json.dumps(event) + '\n')
    return {'real_execution': False, 'runtime_status': 'blocked',
            'reason': event['reason'], 'evidence_origin': 'fixture',
            'events': [event], 'raw_path': str(raw), 'raw_sha256': raw_sha256(raw),
            'environment': {'ok': False, 'reason': event['reason']}}


def missing_middle(config, directory):
    result = healthy(config, directory)
    received = [event for event in result['events'] if event['event'] == 'receive']
    result['events'].remove(received[len(received) // 2])
    for index, event in enumerate(result['events']):
        event['event_index'] = index
    Path(result['raw_path']).write_text(''.join(json.dumps(event) + '\n' for event in result['events']))
    result['raw_sha256'] = raw_sha256(result['raw_path'])
    return result


def reseal_plan_input(capsule, change):
    """Author a self-consistent capsule, not merely corrupt its outer digest."""
    from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
    from r2f.core.mutation_plan import MutationPlan
    scenario = Scenario.from_dict(capsule.input['scenario'])
    saved = scenario.mutation_plan.to_dict()
    change(saved)
    scenario.mutation_plan = MutationPlan.from_dict(saved)
    scenario.parameters['mutation_parameters'] = copy.deepcopy(saved['final_config'])
    plan = scenario.mutation_plan
    receipts = copy.deepcopy(capsule.input['effective_input']['input_layer_receipts'])
    receipts[0].update(plan_hash=plan.plan_hash(), content_sha256=plan.content_hash(),
                       runtime_windows=copy.deepcopy(plan.runtime_windows))
    effective = scenario.effective_input_v2(input_layer_receipts=receipts,
                                           source_manifest=capsule.input['source_manifest'])
    data = copy.deepcopy(capsule.input)
    data.update(scenario=scenario.to_dict(), mutation_parameters=scenario.mutation_parameters,
                effective_input=effective,
                effective_input_sha256=effective_input_hash(effective, schema=INPUT_SCHEMA_V2))
    for key in ('mutation_plan', 'operations', 'runtime_windows', 'contract_hash'):
        data[key] = copy.deepcopy(effective[key])
    return data


def reseal_window_input(capsule, change):
    return reseal_plan_input(capsule, lambda plan: change(plan['runtime_windows']))


class _ReplayFixtures(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = {'source_repo': str(REPO), 'head': 'offline-frozen-source',
                       'behavior_tree_sha256': 'a' * 64, 'source_frozen': True,
                       'dirty_behavior_paths': []}
        source = patch('r2f.campaign.replay.source_manifest', return_value=copy.deepcopy(self.source))
        self.current_source = source.start()
        self.addCleanup(source.stop)
        self.registry = Registry()
        register_exploratory(self.registry, include_required=True)
        self.serial = 0
        self.originals = {}

    def case(self, target_id=LATE, controls=None, *, windows=0, chain=False):
        """Generate a valid capsule in memory, not by starting a campaign."""
        self.serial += 1
        target = self.registry.get_target(target_id)
        base = semantic_input('mutated', target_id=EXPLORATORY_RUNTIME_TARGET[target_id])
        # Role QoS is a pure runtime projection; do not freeze a stale copy
        # before mutating the shared submitted QoS.
        base.pop('reader_qos', None)
        base.update(copy.deepcopy(controls or {}))
        contract = target.exploration_contract
        surface = contract.surface_by_id['P04' if target_id == PROJECTION else
                                         'E01' if target_id == RECOVERY else 'P01']
        proposal = mutate(base, surface, make_rng(7),
                          mutator_id='M03' if target_id == RECOVERY else 'M01')
        runtime_windows = [
            {'schema': 'r2f.runtime-window/v1', 'window_id': 'saved-' + str(index),
             'window_index': index,
             'trigger_anchor': 'fresh_match' if index == 0 else 'previous_restore_confirmed',
             'trigger_offset_sec': 0.0,
             'drop_duration_sec': float(proposal.input_after['drop_duration_sec']),
             'restore_interval_sec': 0.05}
            for index in range(windows)]
        if windows > 1:
            base['environment_windows'] = [
                {key: value for key, value in window.items() if key != 'schema'}
                for window in runtime_windows]
            proposal = mutate(base, surface, make_rng(7), mutator_id='M03')
        proposals = [proposal]
        if chain:
            proposals.append(mutate(proposal.input_after, contract.surface_by_id['W02'],
                                    make_rng(8), mutator_id='M03'))
        plan = compose(base, proposals, contract, rng_seed=7, runtime_windows=runtime_windows,
                       **({'chain_length': 2} if chain else {}))
        slot = target.mutation_slots[0]
        scenario = Scenario(target, RMW, 'mutated', 0, 7,
                            {'mutation_kind': slot.mutation_kind, 'carrier': slot.carrier,
                             'mutation_parameters': plan.final_config}, plan)
        prepared = S1ExploratoryMutationProvider({target_id: target}).prepare_plan(scenario, plan)
        capsule = ReplayCapsule.from_scenario(
            scenario, source_manifest=self.source, final_input=plan.final_config,
            input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
        directory = self.root / ('original-' + str(self.serial))
        directory.mkdir()
        path = directory / 'capsule.json'
        path.write_text(json.dumps(capsule.to_dict()))
        # Deliberately contrary, hash-external controls. Replay must never use
        # this historical runtime config as a source of semantics.
        (directory / 'runtime_config.json').write_text(json.dumps({
            'profile_path': '/tmp/no-such-profile.json', 'rmws': {'writer': OTHER_RMW},
            'projection_roles': ['writer9', 'reader9'], 'publish_burst_count': 4096}))
        self.snapshot(directory)
        return capsule, path

    def executed_case(self, runtime=healthy):
        capsule, path = self.case()
        config = _replay_config(capsule, 'offline-original', 80, self.source)
        result = runtime(config, path.parent / 'original-runtime')
        row = ingest_execution(Scenario.from_dict(capsule.input['scenario']), config, result, capsule,
                               services=_registered_target(capsule)[1])
        self.assertEqual(row['execution_stage'], 'judged')
        _bind_execution_input(row, config, capsule)
        row.update(domain=80, domain_id=80)
        capsule.record_execution(row)
        path.write_text(json.dumps(capsule.to_dict()))
        self.snapshot(path.parent)
        return capsule, path

    def snapshot(self, directory):
        self.originals.update({path: path.read_bytes() for path in directory.rglob('*') if path.is_file()})

    def assert_immutable(self):
        for path, before in self.originals.items():
            self.assertEqual(path.read_bytes(), before, str(path))

    def run_replay(self, path, runtime=healthy, *, output=None, domain=81):
        report = replay_cell(path, output_dir=output or self.root / ('replay-' + str(self.serial)),
                             domain_id=domain, runtime=runtime)
        self.assert_immutable()
        self.assertFalse(report['real_ros_execution'], report)
        self.assertFalse(report['real_runtime_attempt'], report)
        audit = audit_replay(report['report_path'])
        self.assertTrue(audit['valid'], audit)
        self.assertFalse(audit['real_ros_execution'])
        return report

    def config(self, capsule):
        return _replay_config(capsule, 'offline-replay', 81, self.source)

class ExploratoryReplayContracts(_ReplayFixtures):
    def test_registered_exploratory_identity_and_healthy_unified_replay(self):
        for target_id in (LATE, JOIN):
            with self.subTest(target=target_id):
                capsule, path = self.case(target_id)
                seen = []

                def runtime(config, directory):
                    seen.append(copy.deepcopy(config))
                    return healthy(config, directory)

                report = self.run_replay(path, runtime)
                self.assertEqual(report['status'], 'pass', report)
                self.assertEqual(report['oracle_verdict'], 'pass')
                self.assertEqual(report['comparison_basis'], 'execution_success_only')
                self.assertFalse(report['comparison_matches_original'])
                self.assertEqual(seen[0]['target_id'], target_id)
                self.assertEqual(seen[0]['final_input'], capsule.input['effective_input']['final_input'])
                self.assertEqual(seen[0]['effective_input'], capsule.input['effective_input'])
                self.assertEqual(seen[0]['mutation_plan'], capsule.input['mutation_plan'])
                self.assertEqual(seen[0]['effective_input_sha256'], capsule.input['effective_input_sha256'])
                self.assertNotIn('profile_path', seen[0])
                self.assertNotIn('rmws', seen[0])
                derived = ReplayCapsule.from_dict(json.loads(
                    (Path(report['report_path']).parent / 'derived_capsule.json').read_text()))
                self.assertEqual(derived.input, capsule.input)
                self.assertEqual(len(derived.replay_attempts), 1)

    def test_executed_original_raw_and_history_are_immutable_after_healthy_replay(self):
        capsule, path = self.executed_case()
        report = self.run_replay(path)
        self.assertEqual(report['status'], 'pass', report)
        self.assertTrue(report['comparison_matches_original'])
        derived = ReplayCapsule.from_dict(json.loads(
            (Path(report['report_path']).parent / 'derived_capsule.json').read_text()))
        self.assertEqual(derived.execution, capsule.execution)
        self.assertEqual(derived.input, capsule.input)
        self.assertEqual(len(derived.execution_history), 1)
        self.assert_immutable()

    def test_original_typed_failure_is_retained_not_confirmed_by_new_pass(self):
        capsule, path = self.executed_case(missing_middle)
        self.assertEqual(capsule.execution['oracle_verdict'], 'fail')
        report = self.run_replay(path)
        self.assertEqual(report['status'], 'inconclusive', report)
        self.assertEqual(report['oracle_verdict'], 'pass')
        self.assertFalse(report['comparison_matches_original'])
        derived = ReplayCapsule.from_dict(json.loads(
            (Path(report['report_path']).parent / 'derived_capsule.json').read_text()))
        self.assertEqual(derived.execution, capsule.execution)
        self.assertEqual(derived.execution['oracle_verdict'], 'fail')
        self.assertEqual(derived.execution_history[0]['oracle_verdict'], 'pass')

    def test_original_raw_tampering_fails_before_runtime(self):
        capsule, path = self.executed_case()
        raw = Path(capsule.execution['raw_path'])
        raw.write_text(raw.read_text() + '{}\n')
        self.originals[raw] = raw.read_bytes()
        runtime = Mock(side_effect=AssertionError('must not launch'))
        report = self.run_replay(path, runtime)
        self.assertEqual(report['status'], 'fail_artifact')
        runtime.assert_not_called()

    def test_runtime_tool_exception_is_retained_and_not_a_sut_candidate(self):
        _, path = self.case()
        report = self.run_replay(path, Mock(side_effect=RuntimeError('offline tool failure')))
        self.assertEqual(report['status'], 'fail_environment', report)
        self.assertEqual(report['reason_code'], 'runtime_tool_failure')
        self.assertIn('offline tool failure', report['failure_reason'])
        self.assertTrue(report['runtime_invoked'])
        self.assertIsNone(report.get('candidate_status'))

    def test_failed_runtime_keeps_new_raw_and_the_original_capsule(self):
        _, path = self.case()
        report = self.run_replay(path, blocked)
        self.assertEqual(report['status'], 'fail_environment', report)
        self.assertEqual(report['runtime_status'], 'blocked')
        self.assertEqual(report['runtime_reason'], 'domain_conflict:offline_fixture')
        root = Path(report['report_path']).parent
        self.assertEqual(raw_sha256(root / 'runtime/raw.jsonl'),
                         json.loads((root / 'runtime_returned.json').read_text())['raw_sha256'])

    def test_invalid_target_is_not_downgraded_by_fuzz_suffix(self):
        capsule, _ = self.case()
        capsule.input['target_id'] = 'discovery.late_endpoint.fuzz_v100'
        with self.assertRaisesRegex(ValueError, 'registered target'):
            _registered_target(capsule)

    def test_registered_manifest_or_revision_change_fails_closed(self):
        capsule, _ = self.case()
        for field, value in (('target_revision', 1), ('intent_and_scope', 'changed')):
            altered = copy.deepcopy(capsule)
            altered.input['target'][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'registered target'):
                _check_source(altered, self.source)

    def test_unknown_registered_backend_is_not_sent_to_endpoint_runtime(self):
        capsule, _ = self.case()
        with patch.dict('r2f.runtime.ros2_pair.EXPLORATORY_TARGETS',
                        {LATE: {'backend': 'unsupported_relation_observation'}}):
            with self.assertRaisesRegex(ValueError, 'no admitted replay runtime backend'):
                self.config(capsule)

    def test_capsule_plan_or_final_input_tampering_never_invokes_runtime(self):
        for section in ('effective_input', 'mutation_plan'):
            capsule, path = self.case()
            document = capsule.to_dict()
            if section == 'effective_input':
                document['input'][section]['final_input']['rmws'] = {'writer': OTHER_RMW}
            else:
                document['input'][section]['operations'][0]['surface_id'] = 'P99'
            path.write_text(json.dumps(document))
            self.originals[path] = path.read_bytes()
            runtime = Mock(side_effect=AssertionError('must not execute'))
            report = self.run_replay(path, runtime)
            self.assertEqual(report['status'], 'fail_artifact')
            runtime.assert_not_called()

    def test_self_consistent_window_mismatch_never_invokes_runtime_even_with_same_source(self):
        for change in (lambda windows: windows.pop(),
                       lambda windows: windows[1].update(trigger_offset_sec=0.5)):
            capsule, path = self.case(RECOVERY, windows=2)
            data = reseal_window_input(capsule, change)
            path.write_text(json.dumps(dict(capsule.to_dict(), input=data)))
            self.snapshot(path.parent)
            runtime = Mock(side_effect=AssertionError('mismatched saved windows must not execute'))
            report = self.run_replay(path, runtime)
            self.assertEqual(report['status'], 'fail_artifact', report)
            self.assertIn('runtime_windows/environment_windows', report['failure_reason'])
            runtime.assert_not_called()

    def test_missing_or_null_operation_input_path_never_reaches_runtime(self):
        for missing in (True, False):
            capsule, path = self.case()

            def change(plan):
                if missing:
                    plan['final_config'].pop('reader_delay_sec')
                else:
                    plan['final_config']['reader_delay_sec'] = None
                plan['operations'][-1]['input_after_sha256'] = canonical_hash(plan['final_config'])

            data = reseal_plan_input(capsule, change)
            # Input/plan/source/hash are mutually consistent. The declared
            # operation still needs an actual non-null submitted path.
            malformed = ReplayCapsule.from_dict(dict(capsule.to_dict(), input=data))
            self.assertEqual(malformed.input, data)
            path.write_text(json.dumps(malformed.to_dict()))
            self.snapshot(path.parent)
            runtime = Mock(side_effect=AssertionError('missing binding must not execute'))
            with patch('r2f.campaign.exploratory_input.runtime_input',
                       side_effect=AssertionError('do not default a missing submitted scalar')):
                report = self.run_replay(path, runtime)
            self.assertEqual(report['status'], 'fail_artifact', report)
            code = 'submitted_parameter_missing:' if missing else 'submitted_parameter_null:'
            self.assertIn(code + 'reader_delay_sec', report['failure_reason'])
            runtime.assert_not_called()

    def test_exact_route_capability_is_checked_without_repreparing_saved_input(self):
        capsule, path = self.case(RECOVERY, windows=2)
        from r2f.runtime.ros2_pair import exploratory_runtime_capabilities
        routes = exploratory_runtime_capabilities()
        routes[RECOVERY] = dict(routes[RECOVERY], multi_window=False)
        runtime = Mock(side_effect=AssertionError('unsupported route must not execute'))
        with patch('r2f.runtime.ros2_pair.exploratory_runtime_capabilities', return_value=routes), \
             patch('r2f.core.mutators.mutate', side_effect=AssertionError('no regeneration')), \
             patch.object(S1ExploratoryMutationProvider, 'prepare', side_effect=AssertionError('no prepare')), \
             patch.object(S1ExploratoryMutationProvider, 'prepare_plan', side_effect=AssertionError('no prepare_plan')):
            report = self.run_replay(path, runtime)
        self.assertEqual(report['status'], 'fail_artifact', report)
        self.assertEqual(report['reason_code'], 'exploratory_plan_not_routable')
        self.assertEqual(report['phase'], 'preflight')
        self.assertIn('multi-window execution is not enabled', report['failure_reason'])
        runtime.assert_not_called()

    def test_source_identity_and_source_drift_remain_strict(self):
        capsule, path = self.case()
        self.current_source.return_value = dict(self.source, head='other-source')
        runtime = Mock()
        report = self.run_replay(path, runtime)
        self.assertEqual(report['reason_code'], 'source_identity_mismatch')
        runtime.assert_not_called()
        self.current_source.side_effect = [self.source, dict(self.source, head='changed-during-runtime')]
        report = self.run_replay(path, healthy, output=self.root / 'changed-during-replay')
        self.assertEqual(report['reason_code'], 'source_identity_changed', report)
        self.assertEqual(report['status'], 'fail_environment')

    def test_fresh_domain_and_independent_original_cases_directory(self):
        _, path = self.case()
        runtime = Mock()
        report = self.run_replay(path, runtime, domain=80)
        self.assertEqual(report['reason_code'], 'fresh_domain_required')
        runtime.assert_not_called()
        run = self.root / 'saved-exploration'
        original = run / 'cases' / 'cell'
        original.mkdir(parents=True)
        nested = original / 'capsule.json'
        nested.write_bytes(path.read_bytes())
        with self.assertRaisesRegex(ValueError, 'independent'):
            replay_cell(nested, output_dir=run / 'other-replay', domain_id=81, runtime=runtime)
        runtime.assert_not_called()

    def test_chain_uses_every_saved_operation_surface_without_regeneration(self):
        capsule, path = self.case(chain=True)
        config = self.config(capsule)
        self.assertEqual(config['fuzz_surfaces'], ['P01', 'W02'])
        self.assertEqual(set(config['submitted_parameters']), {'reader_delay_sec', 'publish_interval_sec'})
        with patch('r2f.core.mutators.mutate', side_effect=AssertionError('do not regenerate')):
            report = self.run_replay(path)
        self.assertEqual(report['status'], 'pass', report)

    def test_mixed_role_rmws_are_rebuilt_only_from_sealed_input(self):
        controls = {'rmws': {'writer': RMW, 'reader': OTHER_RMW}}
        capsule, path = self.case(controls=controls)
        seen = []

        def runtime(config, directory):
            seen.append(copy.deepcopy(config))
            return blocked(config, directory)

        self.run_replay(path, runtime)
        self.assertEqual(seen[0]['rmws'], controls['rmws'])
        self.assertEqual(seen[0]['final_input']['rmws'], controls['rmws'])
        self.assertEqual(self.config(capsule)['rmws'], controls['rmws'])

    def test_projection_four_role_qos_is_derived_from_sealed_controls(self):
        roles = ['writer', 'reader', 'writer2', 'reader2']
        controls = {'projection_roles': roles, 'projection_role_qos': {'writer2': {'depth': 16}},
                    'rmws': {'writer': RMW, 'reader': OTHER_RMW, 'reader2': RMW}}
        capsule, path = self.case(PROJECTION, controls)
        config = self.config(capsule)
        self.assertEqual(config['projection_roles'], roles)
        self.assertEqual(config['writer_qos'], config['qos'])
        self.assertEqual(config['reader_qos'], config['qos'])
        self.assertEqual(config['writer2_qos'], dict(config['qos'], depth=16))
        self.assertEqual(config['reader2_qos'], config['qos'])
        self.assertEqual(_worker_counts(config), dict.fromkeys(roles, 1))
        report = self.run_replay(path, blocked)
        self.assertEqual(report['status'], 'fail_environment', report)

    def test_recovery_churn_burst_netem_controls_and_saved_windows_reach_runtime(self):
        controls = {'reader_recreate_in_window': True, 'reader_recreate_count': 2,
                    'reader_recreate_fractions': [0.1, 0.3], 'reader_destroy_fractions': [0.6, 0.8],
                    'writer_churn_in_window': True, 'publish_burst_in_window': True,
                    'publish_burst_count': 24, 'publish_burst_interval_sec': 0.0,
                    'environment_mutation': {'kind': 'discovery_drop'}, 'netem_covers_delivery': False,
                    'rmws': {'reader2': OTHER_RMW}}
        capsule, path = self.case(RECOVERY, controls, windows=1)
        config = self.config(capsule)
        for key, value in controls.items():
            self.assertEqual(config[key], value, key)
        self.assertEqual(config['runtime_windows'], [
            {key: value for key, value in window.items() if key != 'schema'}
            for window in capsule.input['runtime_windows']])
        self.assertEqual(_worker_counts(config),
                         {'writer': 1, 'reader': 1, 'reader2': 1, 'reader3': 1, 'writer2': 1})
        report = self.run_replay(path, blocked)
        self.assertEqual(report['status'], 'fail_environment', report)

    def test_canonical_netem_and_delivery_overlap_are_sealed_controls(self):
        from r2f.runtime.namespace_pair import normalize_environment_mutation
        environment = normalize_environment_mutation({
            'kind': 'discovery_netem',
            'spec': {'delay_ms': 10, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 3}})
        capsule, path = self.case(RECOVERY, {'environment_mutation': environment,
                                           'netem_covers_delivery': True}, windows=1)
        config = self.config(capsule)
        self.assertEqual(config['environment_mutation'], environment)
        self.assertTrue(config['netem_covers_delivery'])
        self.assertEqual(config['final_input']['environment_mutation'], environment)
        seen = []

        def runtime(config, directory):
            seen.append(copy.deepcopy(config))
            return blocked(config, directory)

        report = self.run_replay(path, runtime)
        self.assertEqual(report['status'], 'fail_environment', report)
        self.assertEqual(seen[0]['environment_mutation'], environment)
        self.assertTrue(seen[0]['netem_covers_delivery'])

    def test_default_endpoint_dispatch_still_uses_offline_qualified_fixture(self):
        _, path = self.case()
        with patch('r2f.runtime.ros2_pair.run_pair', side_effect=healthy) as runtime:
            report = self.run_replay(path, runtime=None)
        self.assertEqual(report['status'], 'pass', report)
        runtime.assert_called_once()
        self.assertEqual(runtime.call_args.args[0]['target_id'], LATE)
        self.assertTrue(report['formal_runtime'])
        self.assertFalse(report['real_ros_execution'])
        self.assertFalse(report['real_runtime_attempt'])

    def test_default_dispatch_reaches_namespace_for_recovery_and_projection(self):
        # run_pair is exercised as the production dispatcher, but the actual
        # namespace call is intercepted. No lease/process/socket is opened.
        for target_id in (RECOVERY, PROJECTION):
            capsule, path = self.case(target_id, windows=1 if target_id == RECOVERY else 0)
            seen = []

            def namespace(config, directory, result):
                seen.append(copy.deepcopy(config))
                return blocked(config, directory)

            with patch('r2f.runtime.namespace_pair.run_discovery_pair', side_effect=namespace):
                report = self.run_replay(path, runtime=None)
            self.assertEqual(report['execution_origin'], DEFAULT_RUNTIME)
            self.assertTrue(report['formal_runtime'])
            self.assertEqual(report['status'], 'fail_environment', report)
            self.assertEqual(len(seen), 1, report)
            self.assertEqual(seen[0]['target_id'], target_id)
            self.assertEqual(seen[0]['effective_input_sha256'], capsule.input['effective_input_sha256'])

    def test_replay_raw_hash_tampering_is_detected_without_changing_original(self):
        _, path = self.case()
        report = self.run_replay(path)
        raw = Path(report['report_path']).parent / 'runtime/raw.jsonl'
        raw.write_text(raw.read_text() + '{}\n')
        self.assertFalse(audit_replay(report['report_path'])['valid'])
        self.assert_immutable()

    def test_rehashed_audit_still_rejects_hash_external_runtime_override(self):
        _, path = self.case()
        report = self.run_replay(path, blocked)
        root = Path(report['report_path']).parent
        config_path = root / 'runtime_config.json'
        config = json.loads(config_path.read_text())
        config['reader_recreate_in_window'] = True
        config_path.write_text(json.dumps(config))
        report['artifacts']['runtime_config.json'] = raw_sha256(config_path)
        report['report_sha256'] = _digest(report)
        Path(report['report_path']).write_text(json.dumps(report))
        audit = audit_replay(report['report_path'])
        self.assertFalse(audit['valid'])
        self.assertTrue(any('deterministic capsule reconstruction' in error for error in audit['errors']), audit)
        self.assert_immutable()

    def test_legacy_required_capsule_still_uses_unified_replay(self):
        target = S1Module().targets['discovery.late_endpoint']
        scenario = compile_scenario(target, RMW, 'mutated', 0, seed=7)
        capsule = ReplayCapsule.from_scenario(scenario, source_manifest=self.source,
                                             final_input=semantic_input('mutated'), origin_domain=80)
        self.serial += 1
        original = self.root / 'required'
        original.mkdir()
        path = original / 'capsule.json'
        path.write_text(json.dumps(capsule.to_dict()))
        self.snapshot(original)
        report = self.run_replay(path)
        self.assertEqual(report['status'], 'pass', report)
        config = json.loads((Path(report['report_path']).parent / 'runtime_config.json').read_text())
        self.assertEqual(config['target_id'], target.target_id)
        self.assertNotIn('mutation_plan', config)
        self.assertNotIn('fuzz_surfaces', config)


class WorkerIdentityContracts(_ReplayFixtures):
    # Use only pure in-memory receipt construction for qualification tests.
    # evidence_origin is always fixture, including the nominally healthy rows.
    def worker_fixture(self, config):
        workers, events = [], []
        pid = 12000
        for role, count in _worker_counts(config).items():
            for generation in range(1, count + 1):
                pid += 1
                worker = {'role': role, 'pid': pid, 'worker_generation': generation,
                          'returncode': 0, 'fresh_process': True, 'separate_context': True}
                workers.append(worker)
                for kind in ('process_started', 'endpoint_created', 'process_exit'):
                    events.append({'event': kind, 'event_index': len(events),
                                   'run_id': config['run_id'], 'domain_id': config['domain_id'],
                                   'role': role, 'pid': pid, 'worker_generation': generation,
                                   'returncode': 0, 'endpoint_gid': 'gid-' + str(pid),
                                   'endpoint_generation': 'generation-' + str(pid),
                                   'rmw_identifier': role_rmw(config, role)})
        return {'workers': workers}, events

    def test_four_projection_workers_are_accepted_but_fixture_never_claims_ros(self):
        capsule, _ = self.case(PROJECTION, {'projection_roles': ['writer', 'writer2', 'reader', 'reader2']})
        config = self.config(capsule)
        metadata, events = self.worker_fixture(config)
        self.assertEqual(_worker_qualification(config, metadata, events), [])
        result = {'evidence_origin': 'fixture', 'execution_metadata': metadata, 'events': events}
        qualification = _qualification(DEFAULT_RUNTIME, config, result, {})
        self.assertFalse(qualification['real_ros_execution'])
        self.assertFalse(qualification['real_runtime_attempt'])
        self.assertIn('offline_fixture_cannot_establish_real_ros', qualification['qualification_errors'])
        self.assertNotIn('independent_worker_metadata_missing', qualification['qualification_errors'])

    def test_projection_tools_helper_uses_its_real_ready_receipt_not_endpoint_identity(self):
        capsule, _ = self.case(PROJECTION)
        config = self.config(capsule)
        metadata, events = self.worker_fixture(config)
        ref = {'path': '/offline/manifest.json', 'sha256': 'a'*64}
        config.update(projection_discovery_layers='participant_topic_v1', projection_discovery_tools=ref)
        identity = {'role': 'discovery_tools', 'pid': 13000, 'worker_generation': 1}
        metadata['workers'].append(dict(identity, returncode=0, fresh_process=True,
            separate_context=True, start_ticks=42, cleanup_confirmed=True))
        for kind in ('process_started', 'discovery_tools_ready', 'process_exit'):
            events.append(dict(identity, event=kind, event_index=len(events),
                run_id=config['run_id'], domain_id=config['domain_id'], returncode=0,
                manifest=ref, process_start_ticks=42, start_ticks=42,
                observer_generation='observer-generation', participant_guid='01'*12+'000001c1',
                cleanup_confirmed=True))
        self.assertEqual(_worker_qualification(config, metadata, events), [])
        ready = next(event for event in events if event['event'] == 'discovery_tools_ready')
        ready['manifest'] = dict(ref, sha256='b'*64)
        self.assertIn('discovery_tools_identity_mismatch', _worker_qualification(config, metadata, events))

    def test_multiwindow_same_role_generations_are_not_mistaken_for_duplicates(self):
        capsule, _ = self.case(RECOVERY, {'reader_recreate_in_window': True,
                                        'reader_recreate_count': 2, 'writer_churn_in_window': True}, windows=2)
        config = self.config(capsule)
        metadata, events = self.worker_fixture(config)
        self.assertEqual(_worker_counts(config),
                         {'writer': 1, 'reader': 1, 'reader2': 2, 'reader3': 2, 'writer2': 2})
        self.assertEqual(_worker_qualification(config, metadata, events), [])
        # Cross-wire just one generation; a same-role/PID-looking receipt
        # cannot borrow another generation's endpoint return.
        next(event for event in events if event['event'] == 'endpoint_created'
             and event['role'] == 'reader2')['worker_generation'] = 2
        errors = _worker_qualification(config, metadata, events)
        self.assertIn('worker_raw_link_missing:reader2:endpoint_created', errors)
        self.assertIn('worker_raw_identity_unaccounted', errors)

    def test_role_pid_generation_vendor_and_endpoint_identity_tampering(self):
        capsule, _ = self.case(PROJECTION, {'projection_roles': ['writer', 'reader', 'writer2', 'reader2'],
                                          'rmws': {'reader': OTHER_RMW}})
        config = self.config(capsule)
        for field, value in (('role', 'writer'), ('pid', 1), ('worker_generation', 9),
                             ('endpoint_generation', ''), ('rmw_identifier', RMW)):
            metadata, events = self.worker_fixture(config)
            endpoint = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'reader')
            endpoint[field] = value
            with self.subTest(field=field):
                self.assertTrue(_worker_qualification(config, metadata, events))

    def test_missing_worker_extra_raw_and_reused_pid_do_not_qualify(self):
        capsule, _ = self.case(PROJECTION, {'projection_roles': ['writer', 'reader', 'writer2', 'reader2']})
        config = self.config(capsule)
        metadata, events = self.worker_fixture(config)
        metadata['workers'].pop()
        errors = _worker_qualification(config, metadata, events)
        self.assertIn('independent_worker_metadata_missing', errors)
        self.assertIn('worker_raw_identity_unaccounted', errors)
        metadata, events = self.worker_fixture(config)
        metadata['workers'][1]['pid'] = metadata['workers'][0]['pid']
        self.assertIn('independent_worker_metadata_missing', _worker_qualification(config, metadata, events))

    def test_reused_same_role_generation_is_rejected_even_with_unique_pids(self):
        capsule, _ = self.case(RECOVERY, {'writer_churn_in_window': True}, windows=2)
        config = self.config(capsule)
        metadata, events = self.worker_fixture(config)
        restarted = next(worker for worker in metadata['workers']
                         if worker['role'] == 'writer2' and worker['worker_generation'] == 2)
        restarted['worker_generation'] = 1
        for event in events:
            if event['pid'] == restarted['pid']:
                event['worker_generation'] = 1
        self.assertIn('worker_generation_sequence_invalid:writer2',
                      _worker_qualification(config, metadata, events))

    def test_worker_start_endpoint_exit_chronology_and_exit_code_are_bound(self):
        capsule, _ = self.case()
        config = self.config(capsule)
        metadata, events = self.worker_fixture(config)
        events[0], events[1] = events[1], events[0]
        self.assertIn('worker_raw_chronology_mismatch:writer', _worker_qualification(config, metadata, events))
        metadata, events = self.worker_fixture(config)
        events[2]['returncode'] = 3
        self.assertIn('worker_exit_receipt_mismatch:writer', _worker_qualification(config, metadata, events))


if __name__ == '__main__':
    unittest.main()
