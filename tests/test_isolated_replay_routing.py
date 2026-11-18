"""Pure routing/audit fixtures; never run ROS, DDS, netns or a campaign.

Canonical runtime attributes are replaced by mocks before replay entry. A
formal-runtime flag records callable selection only, never ROS qualification.
"""
import copy
import functools
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from r2f.campaign import replay
from r2f.campaign import runner
from r2f.core import raw_sha256
from test_campaign_exploratory_replay import _ReplayFixtures, blocked, healthy


class IsolatedReplayRouting(_ReplayFixtures):
    def isolated(self, path, *, explicit=False):
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime',
                   side_effect=blocked) as runtime:
            report = replay.replay_cell(
                path, output_dir=self.root / ('isolated-' + str(self.serial)),
                domain_id=81, runtime_boundary='isolated_netns',
                **({'runtime': runtime} if explicit else {}))
        runtime.assert_called_once()
        config = runtime.call_args.args[0]
        self.assert_immutable()
        return report, config

    def test_default_isolated_is_canonical_and_enabled_only_on_execution_copy(self):
        capsule, path = self.case()
        original_input = copy.deepcopy(capsule.input)
        report, config = self.isolated(path)
        self.assertIs(report['formal_runtime'], True)
        self.assertEqual(report['runtime_boundary'], 'isolated_netns')
        self.assertEqual(report['execution_origin'], replay.ISOLATED_RUNTIME)
        self.assertIs(config['execution_boundary']['enabled'], True)
        expected = replay._replay_config(capsule, report['replay_id'], 81, self.source)
        expected = runner._boundary_execution_config(expected, 'isolated_netns')
        self.assertEqual(config, expected)
        saved = json.loads((Path(report['report_path']).parent / 'runtime_config.json').read_text())
        self.assertEqual(saved, config)
        derived = json.loads((Path(report['report_path']).parent / 'derived_capsule.json').read_text())
        self.assertEqual(capsule.input, original_input)
        self.assertEqual(derived['input'], original_input)
        self.assertEqual(config['effective_input_sha256'], capsule.input['effective_input_sha256'])
        self.assertFalse(report['real_ros_execution'])
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])

    def test_explicit_exact_isolated_callable_is_not_mislabelled_injected(self):
        _, path = self.case()
        report, _ = self.isolated(path, explicit=True)
        self.assertTrue(report['formal_runtime'])
        self.assertEqual(report['execution_origin'], replay.ISOLATED_RUNTIME)
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])

    def test_injected_isolated_rejected_before_output_and_runtime(self):
        _, path = self.case()
        runtime = Mock(side_effect=blocked)
        output = self.root / 'injected-isolated'
        with self.assertRaisesRegex(ValueError, 'requires the canonical execution boundary runtime'):
            replay.replay_cell(path, output_dir=output, domain_id=81,
                               runtime=runtime, runtime_boundary='isolated_netns')
        runtime.assert_not_called()
        self.assertFalse(output.exists())
        self.assert_immutable()

    def test_module_name_spoof_and_canonical_wrapper_are_not_canonical(self):
        _, path = self.case()
        from r2f.runtime.execution_boundary import run_isolated_runtime
        def spoof(config, directory):
            self.fail('spoof was invoked')
        spoof.__module__ = run_isolated_runtime.__module__
        spoof.__name__ = run_isolated_runtime.__name__
        for index, runtime in enumerate((spoof, functools.partial(run_isolated_runtime))):
            with self.subTest(index=index):
                output = self.root / ('spoof-' + str(index))
                with self.assertRaisesRegex(ValueError, 'canonical execution boundary runtime'):
                    replay.replay_cell(path, output_dir=output, domain_id=81,
                        runtime=runtime, runtime_boundary='isolated_netns')
                self.assertFalse(output.exists())
        self.assert_immutable()

    def test_default_host_dispatch_is_unchanged_and_not_boundary_enabled(self):
        _, path = self.case()
        with patch('r2f.runtime.ros2_pair.run_pair', side_effect=blocked) as runtime:
            report = replay.replay_cell(path, output_dir=self.root / 'host', domain_id=81)
        runtime.assert_called_once()
        self.assertNotIn('execution_boundary', runtime.call_args.args[0])
        self.assertEqual(report['execution_origin'], replay.DEFAULT_RUNTIME)
        self.assertTrue(report['formal_runtime'])
        self.assertEqual(report['runtime_boundary'], 'host')
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])
        self.assert_immutable()

    def test_exact_explicit_host_callable_matches_default_host_origin(self):
        _, path = self.case()
        with patch('r2f.runtime.ros2_pair.run_pair', side_effect=blocked) as runtime:
            report = replay.replay_cell(path, output_dir=self.root / 'explicit-host',
                                        domain_id=81, runtime=runtime)
        self.assertTrue(report['formal_runtime'])
        self.assertEqual(report['execution_origin'], replay.DEFAULT_RUNTIME)
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])

    def test_injected_host_remains_offline_even_with_spoofed_module_and_flags(self):
        _, path = self.case()
        def injected(config, directory):
            result = healthy(config, directory)
            result.pop('evidence_origin', None)
            return result
        injected.__module__ = 'r2f.runtime.ros2_pair'
        injected.__name__ = 'run_pair'
        report = replay.replay_cell(path, output_dir=self.root / 'injected-host',
                                   domain_id=81, runtime=injected)
        self.assertTrue(report['runtime_invoked'])
        self.assertFalse(report['formal_runtime'])
        self.assertEqual(report['execution_origin'], 'injected_runtime')
        self.assertFalse(report['real_runtime_attempt'])
        self.assertFalse(report['real_ros_execution'])
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])

    def test_audit_reconstruction_applies_same_boundary_and_rejects_switch_tampering(self):
        capsule, path = self.case()
        report, _ = self.isolated(path)
        root = Path(report['report_path']).parent
        errors = []
        replay._check_audit_config(root, capsule, report, errors)
        self.assertEqual(errors, [])
        config_path = root / 'runtime_config.json'
        config = json.loads(config_path.read_text())
        config['execution_boundary'] = {'enabled': False}
        config_path.write_text(json.dumps(config))
        errors = []
        replay._check_audit_config(root, capsule, report, errors)
        self.assertIn('replay config differs from deterministic capsule reconstruction', errors)

    def test_canonical_selection_is_reported_even_when_source_preflight_blocks(self):
        _, path = self.case()
        self.current_source.return_value = dict(self.source, source_frozen=False)
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime') as runtime:
            report = replay.replay_cell(path, output_dir=self.root / 'source-blocked',
                                       domain_id=81, runtime_boundary='isolated_netns')
        runtime.assert_not_called()
        self.assertTrue(report['formal_runtime'])
        self.assertFalse(report['runtime_invoked'])
        self.assertEqual(report['reason_code'], 'source_not_frozen')
        self.assertFalse(report['real_runtime_attempt'])
        self.assertFalse(report['real_ros_execution'])
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])

    def test_historical_host_audit_without_new_boundary_field_remains_valid(self):
        _, path = self.case()
        with patch('r2f.runtime.ros2_pair.run_pair', side_effect=blocked):
            report = replay.replay_cell(path, output_dir=self.root / 'legacy-host', domain_id=81)
        root = Path(report['report_path']).parent
        request_path = root / 'request.json'
        request = json.loads(request_path.read_text())
        del request['runtime_boundary']
        request_path.write_text(json.dumps(request))
        del report['runtime_boundary']
        report['artifacts']['request.json'] = raw_sha256(request_path)
        report['report_sha256'] = replay._digest(report)
        Path(report['report_path']).write_text(json.dumps(report))
        self.assertTrue(replay.audit_replay(report['report_path'])['valid'])

    def complete_isolated(self):
        from test_isolated_report_gate import complete_offline_boundary
        self.source['head'] = 'a' * 40
        self.current_source.return_value = copy.deepcopy(self.source)
        _, path = self.case()
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime',
                   side_effect=lambda c, d: complete_offline_boundary(c, d).result) as runtime:
            report = replay.replay_cell(path, output_dir=self.root / 'complete-isolated',
                                        domain_id=81, runtime_boundary='isolated_netns')
        runtime.assert_called_once()
        return report

    def test_complete_canonical_isolated_artifacts_are_still_offline_not_real(self):
        report = self.complete_isolated()
        self.assertTrue(report['formal_runtime'])
        self.assertTrue(report['runtime_invoked'])
        self.assertTrue(report['boundary_audit']['valid'], report['boundary_audit']['errors'])
        self.assertFalse(report['boundary_audit']['real_execution_attested'])
        self.assertEqual(report['qualification_errors'], ['offline_fixture_cannot_establish_real_ros'])
        self.assertFalse(report['real_ros_execution'])
        self.assertFalse(report['real_runtime_attempt'])
        audit = replay.audit_replay(report['report_path'])
        self.assertTrue(audit['valid'], audit['errors'])
        self.assertFalse(audit['real_ros_execution'])
        root = Path(report['report_path']).parent
        metadata = json.loads((root / 'runtime_returned.json').read_text())['execution_metadata']
        self.assertEqual(metadata['pid_interpretation'], 'namespace_PID_not_host_PID')
        self.assertTrue(all(worker['pid_namespace'] == metadata['pid_namespace'] for worker in metadata['workers']))
        self.assert_immutable()

    def test_replay_auditor_recomputes_helper_with_explicit_runtime_directory(self):
        from r2f.campaign.boundary_audit import audit_execution_boundary
        report = self.complete_isolated()
        root = Path(report['report_path']).parent
        with patch('r2f.campaign.boundary_audit.audit_execution_boundary',
                   wraps=audit_execution_boundary) as helper:
            audit = replay.audit_replay(report['report_path'])
        self.assertTrue(audit['valid'], audit['errors'])
        helper.assert_called_once()
        self.assertEqual(helper.call_args.kwargs, {'output_root': root / 'runtime'})
        self.assert_immutable()

    def test_replay_manifest_tamper_not_hidden_by_rehashed_report_or_valid_label(self):
        report = self.complete_isolated()
        root = Path(report['report_path']).parent
        result = json.loads((root / 'runtime_returned.json').read_text())
        manifest = Path(result['execution_boundary']['manifest_path'])
        with manifest.open('a') as stream:
            stream.write('\n')
        # Hash the outer inventory again so this must reach boundary re-audit,
        # not merely fail the outer file checksum. Stored qualification stays
        # the previously valid artifact audit and cannot grant eligibility.
        report['artifacts'] = {str(p.relative_to(root)): raw_sha256(p)
                               for p in sorted(root.rglob('*')) if p.is_file() and p != Path(report['report_path'])}
        report['report_sha256'] = replay._digest(report)
        Path(report['report_path']).write_text(json.dumps(report))
        audit = replay.audit_replay(report['report_path'])
        self.assertFalse(audit['valid'])
        self.assertIn('execution qualification differs: boundary_audit', audit['errors'])
        self.assertFalse(audit['real_ros_execution'])
        self.assertTrue(report['boundary_audit']['valid'])
        self.assert_immutable()

    def test_replay_boundary_failure_retains_actual_inner_judged_and_raw_success(self):
        from test_isolated_report_gate import inner_success_cleanup_failure
        _, path = self.case()
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime',
                   side_effect=inner_success_cleanup_failure):
            report = replay.replay_cell(path, output_dir=self.root / 'cleanup-failure',
                                        domain_id=81, runtime_boundary='isolated_netns')
        root = Path(report['report_path']).parent
        execution = json.loads((root / 'execution_metadata.json').read_text())
        result = json.loads((root / 'runtime_returned.json').read_text())
        self.assertTrue(execution['actual_run'])
        self.assertEqual(execution['execution_stage'], 'judged')
        self.assertTrue(result['real_execution'])
        self.assertEqual(result['runtime_status'], 'failed')
        self.assertEqual(result['events'][-1]['runtime_status'], 'completed')
        self.assertIn('boundary_runtime_not_completed', report['qualification_errors'])
        self.assertFalse(report['real_ros_execution'])
        audit = replay.audit_replay(report['report_path'])
        self.assertTrue(audit['valid'], audit['errors'])  # intact failed execution record
        self.assertFalse(audit['real_ros_execution'])
        self.assert_immutable()

    def test_isolated_origin_never_substitutes_for_independent_boundary_qualification(self):
        capsule, _ = self.case()
        config = replay._replay_config(capsule, 'synthetic-route-fixture', 81, self.source)
        config = runner._boundary_execution_config(config, 'isolated_netns')
        directory = self.root / 'qualification-fixture'
        directory.mkdir()
        events, workers = [], []
        for index, role in enumerate(('writer', 'reader'), start=11):
            workers.append({'role': role, 'pid': index, 'returncode': 0,
                            'fresh_process': True, 'separate_context': True})
            for event in ('process_started', 'endpoint_created', 'process_exit'):
                events.append({'event': event, 'event_index': len(events),
                    'run_id': config['run_id'], 'domain_id': config['domain_id'],
                    'role': role, 'pid': index, 'returncode': 0,
                    'endpoint_gid': role + '-synthetic-gid', 'endpoint_generation': role + '-synthetic-generation',
                    'rmw_identifier': config['rmw']})
        raw = directory / 'raw.jsonl'
        raw.write_text(''.join(json.dumps(event) + '\n' for event in events))
        environment = {'ok': True, 'environment_mode': 'ros_overlay', 'ros_distro': 'jazzy',
            'source': self.source, 'rmw': {config['rmw']: {'available': True}},
            'dependency_lock_sha256': 'd' * 64, 'process_scan': {'ok': True},
            'old_lock_scan': {'ok': True}}
        (directory / 'environment.json').write_text(json.dumps(environment))
        metadata = {key: config[key] for key in
                    ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'domain_id')}
        metadata['workers'] = workers
        result = {'real_execution': True, 'runtime_status': 'completed',
            'raw_path': str(raw), 'raw_sha256': raw_sha256(raw), 'events': events,
            'environment': environment, 'execution_metadata': metadata,
            'execution_boundary': {'cleanup': {'confirmed': True}, 'host_clearance_claimed': False}}
        row = {'actual_run': True, 'execution_stage': 'judged'}
        self.assertEqual(replay._worker_qualification(config, metadata, events), [])
        qualified = replay._qualification(replay.ISOLATED_RUNTIME, config, result, row)
        self.assertIn('boundary_output_root_missing', qualified['qualification_errors'])
        self.assertIn('worker_pid_namespace_scope_missing_or_mismatched', qualified['qualification_errors'])
        self.assertTrue(qualified['real_runtime_attempt'])  # actual canonical attempt, not eligible execution
        self.assertFalse(qualified['real_ros_execution'])
        injected = replay._qualification('injected_runtime', config, result, row)
        self.assertFalse(injected['real_runtime_attempt'])
        self.assertFalse(injected['real_ros_execution'])


class BoundaryCliRouting(unittest.TestCase):
    def load_script(self, name):
        path = Path(__file__).resolve().parents[1] / 'scripts' / name
        spec = importlib.util.spec_from_file_location('offline_' + name.replace('.', '_'), path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_replay_cli_forwards_isolated_choice_without_invoking_runtime(self):
        module = self.load_script('replay_s1.py')
        with patch.object(module, 'replay_cell', return_value={'status': 'fail_environment'}) as replay_call:
            with patch('builtins.print'):
                result = module.main(['capsule.json', '--output', 'out', '--domain', '81',
                                      '--runtime-boundary', 'isolated_netns'])
        self.assertEqual(result, 2)
        self.assertEqual(replay_call.call_args.kwargs['runtime_boundary'], 'isolated_netns')

    def test_replay_verify_does_not_silently_ignore_execution_boundary_choice(self):
        module = self.load_script('replay_s1.py')
        with patch.object(module, 'audit_replay') as audit, patch.object(module, 'replay_cell') as execute:
            with self.assertRaises(SystemExit) as error:
                module.main(['--verify', 'report.json', '--runtime-boundary', 'isolated_netns'])
        self.assertEqual(error.exception.code, 2)
        audit.assert_not_called()
        execute.assert_not_called()

    def test_exploratory_cli_forwards_exact_callable_without_starting_cycle(self):
        import tempfile
        from r2f.campaign.corpus import Corpus
        module = self.load_script('run_s1.py')
        with tempfile.TemporaryDirectory() as directory:
            profile_path = Path(directory) / 'profile.json'
            profile_path.write_text(json.dumps({'budget': {'warmup_inputs': 0}}))
            for boundary in ('host', 'isolated_netns'):
                with self.subTest(boundary=boundary):
                    with patch.object(module, 'corpus_from_profile', return_value=Corpus()), \
                            patch.object(module, 'source_manifest', return_value={}), \
                            patch.object(module, 'make_checkpoint_hook', return_value=None), \
                            patch.object(module, 'run_exploratory_cycle', return_value={
                                'run_id': 'offline', 'rows': []}) as run, patch('builtins.print'):
                        code = module.main(['--output-dir', str(Path(directory) / 'never-created'),
                            '--exploratory-profile', str(profile_path), '--cycle-id', 'offline',
                            '--execute', '--runtime-boundary', boundary])
                    self.assertEqual(code, 1)  # No actual run claimed by the mock.
                    self.assertIs(run.call_args.kwargs['runtime'], runner.runtime_for_boundary(boundary))
                    self.assertEqual(run.call_args.kwargs['runtime_boundary'], boundary)
                    self.assertTrue(run.call_args.kwargs['execute'])
            self.assertFalse((Path(directory) / 'never-created').exists())

    def test_required_cli_preserves_host_default_and_forwards_isolated(self):
        module = self.load_script('run_s1.py')
        for boundary in ('host', 'isolated_netns'):
            with self.subTest(boundary=boundary):
                args = ['--output-dir', 'offline-never-created']
                if boundary != 'host':
                    args += ['--runtime-boundary', boundary]
                with patch.object(module, 'run_campaign', return_value={
                    'run_id': 'offline', 'seal': {'sealed': False}}) as run:
                    with patch('builtins.print'):
                        result = module.main(args)
                self.assertEqual(result, 0)
                self.assertFalse(run.call_args.kwargs['execute'])
                self.assertEqual(run.call_args.kwargs['runtime_boundary'], boundary)


if __name__ == '__main__':
    unittest.main()
