"""Offline replay boundary tests; injected tokens never establish ROS evidence."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from r2f.campaign.identity import runtime_config, semantic_input
from r2f.campaign.replay import audit_replay, replay_cell
from r2f.campaign.runner import plan_campaign
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.core import ReplayCapsule, canonical_hash, compile_scenario, raw_sha256
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.targets import EXECUTION_ORDER
from r2f.runtime.instrumentation_bridge import prepare
from test_campaign_evidence import fixture_pair

REPO = Path(__file__).resolve().parents[1]


def missing_middle(config, directory):
    result = fixture_pair(config, directory, fresh_matches=True)
    received = [e for e in result['events'] if e['event'] == 'receive']
    result['events'].remove(received[len(received) // 2])
    for index, event in enumerate(result['events']):
        event['event_index'] = index
    Path(result['raw_path']).write_text(''.join(json.dumps(e) + '\n' for e in result['events']))
    result['raw_sha256'] = raw_sha256(result['raw_path'])
    return result


def healthy(config, directory):
    return fixture_pair(config, directory, fresh_matches=True)


def blocked(config, directory):
    directory.mkdir(parents=True, exist_ok=True)
    reason = 'domain_conflict:occupied_domain_lease'
    event = {'event': 'runtime_end', 'event_index': 0, 'run_id': config['run_id'],
             'domain_id': config['domain_id'], 'reason': reason, 'real_execution': False}
    raw = directory / 'raw.jsonl'
    raw.write_text(json.dumps(event) + '\n')
    return {'runtime_status': 'blocked', 'real_execution': False, 'reason': reason,
            'events': [event], 'raw_path': str(raw), 'raw_sha256': raw_sha256(raw),
            'environment': {'ok': False, 'reason': reason}}


class CampaignReplayContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = {'source_repo': str(REPO), 'head': 'offline-test-head',
                       'behavior_tree_sha256': 'a' * 64, 'source_frozen': True,
                       'dirty_behavior_paths': []}
        self.source_patch = patch('r2f.campaign.replay.source_manifest', return_value=copy.deepcopy(self.source))
        self.current_source = self.source_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.original = self.root / 'original'
        self.original.mkdir()
        self.path = self.original / 'capsule.json'
        target = S1Module().targets['discovery.late_endpoint']
        self.scenario = compile_scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0, seed=7)
        self.submitted = semantic_input('mutated')
        self.capsule = ReplayCapsule.from_scenario(self.scenario, source_manifest=self.source,
                                                 final_input=self.submitted, origin_domain=80)
        self.original_config = runtime_config(
            self.submitted, run_id='original-run', campaign_id='original-run',
            target_id=target.target_id, target_revision=0, rmw=self.scenario.rmw,
            arm='mutated', repeat=0, seed=7, domain_id=80, source_repo=str(REPO),
            source_manifest=self.source)
        self.save_original()

    def save_original(self, runtime=healthy, *, executed=True):
        # Each synthetic original gets a new raw directory; nothing is a real ROS run.
        cap = ReplayCapsule.from_dict(dict(self.capsule.to_dict(), execution=None, execution_history=[]))
        if executed:
            directory = self.original / ('raw-' + str(len(list(self.original.glob('raw-*')))))
            result = runtime(self.original_config, directory)
            execution = ingest_execution(self.scenario, self.original_config, result, cap)
            self.assertEqual(execution['execution_stage'], 'judged')
            cap.record_execution(dict(execution, domain=80))
        self.capsule = cap
        self.path.write_text(json.dumps(cap.to_dict()))
        (self.original / 'runtime_config.json').write_text(json.dumps(self.original_config))
        (self.original / 'report.json').write_text('{"original":"immutable"}\n')
        self.original_bytes = {p: p.read_bytes() for p in self.original.rglob('*') if p.is_file()}

    def run_replay(self, runtime=healthy, **options):
        return replay_cell(self.path, output_dir=self.root / 'replay', domain_id=81, runtime=runtime, **options)

    def assert_original_unchanged(self):
        for path, content in self.original_bytes.items():
            self.assertEqual(path.read_bytes(), content, str(path))

    def assert_auditable(self, report):
        checked = audit_replay(report['report_path'])
        self.assertTrue(checked['valid'], checked)
        self.assertEqual(checked['status'], report['status'])

    def test_healthy_matches_original_and_persists_independent_artifacts(self):
        seen = {}
        def runtime(config, directory):
            seen.update(config)
            return healthy(config, directory)
        report = self.run_replay(runtime)
        self.assertEqual(report['status'], 'pass')
        self.assertEqual(report['oracle_verdict'], 'pass')
        self.assertTrue(report['comparison_matches_original'])
        self.assertEqual(report['comparison_basis'], 'original_execution_signature')
        self.assertEqual(seen['final_input'], self.submitted)
        self.assertEqual(seen['effective_input_sha256'], self.capsule.input['effective_input_sha256'])
        self.assertEqual(seen['domain_id'], 81)
        self.assertNotEqual(seen['run_id'], 'original-run')
        self.assertFalse(report['formal_runtime'])
        self.assertEqual(report['execution_origin'], 'injected_runtime')
        self.assertFalse(report['real_ros_execution'])
        self.assertFalse(audit_replay(report['report_path'])['real_ros_execution'])
        output = self.root / 'replay'
        derived = ReplayCapsule.from_dict(json.loads((output / 'derived_capsule.json').read_text()))
        self.assertEqual(derived.execution, self.capsule.execution)
        self.assertEqual(len(derived.execution_history), 1)
        self.assertEqual(derived.input, self.capsule.input)
        for artifact in ('attempt.json', 'input_metadata.json', 'source_before.json',
                         'source_after.json', 'runtime_config.json', 'runtime/runtime_result.json',
                         'runtime/raw.jsonl', 'execution_metadata.json', 'original_capsule.json'):
            self.assertIn(artifact, report['artifacts'])
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_all_planned_capsules_replay_exact_nonempty_receipts_into_observation_input(self):
        fields = {'run_id', 'domain_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat',
                  'seed', 'topic', 'message_type', 'effective_input_sha256', 'final_input',
                  'input_layer_receipts'}
        checked = 0
        for index, target_id in enumerate(EXECUTION_ORDER):
            with patch('r2f.campaign.runner.source_manifest', return_value=copy.deepcopy(self.source)):
                planned = plan_campaign(self.root / ('planned-' + str(index)), target_id=target_id,
                                        run_id='planned-receipts-' + str(index), domain_id=80, seed=31)
            self.assertEqual(len(planned['rows']), 24)
            for row in planned['rows']:
                with self.subTest(target_id=target_id, cell=row['cell_id']):
                    capsule_path = Path(row['replay_path'])
                    original = capsule_path.read_bytes()
                    capsule = ReplayCapsule.from_dict(json.loads(original))
                    effective = capsule.input['effective_input']
                    self.assertTrue(effective['input_layer_receipts'])
                    seen = []

                    def runtime(config, directory):
                        # Exercise the actual observer boundary; do not insert
                        # receipt fields or start a process in this injected runtime.
                        ctx = prepare(config, directory, {})
                        seen.append({'config': copy.deepcopy(config),
                                     'execution_input': copy.deepcopy(ctx.execution_input),
                                     'prepared_ref': copy.deepcopy(ctx.prepared),
                                     'prepared': json.loads(Path(ctx.prepared['raw_path']).read_text())})
                        return blocked(config, directory)

                    output = self.root / ('replay-receipts-' + str(index)) / row['cell_id']
                    report = replay_cell(capsule_path, output_dir=output, domain_id=81, runtime=runtime)
                    self.assertEqual(len(seen), 1, report)
                    config = seen[0]['config']
                    self.assertTrue(fields.issubset(config), fields - config.keys())
                    expected = {'schema': 'r2f.observation-execution-input/v1',
                                **{key: config[key] for key in fields}}
                    self.assertEqual(seen[0]['execution_input'], expected)
                    self.assertEqual(seen[0]['prepared']['execution_input'], expected)
                    ref = seen[0]['prepared_ref']
                    self.assertEqual(raw_sha256(ref['raw_path']), ref['raw_sha256'])
                    self.assertEqual(config['input_layer_receipts'], effective['input_layer_receipts'])
                    self.assertEqual(config['final_input'], effective['final_input'])
                    self.assertEqual(config['effective_input_sha256'], capsule.input['effective_input_sha256'])
                    for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed', 'topic', 'message_type'):
                        self.assertEqual(config[key], capsule.input[key], key)
                    self.assertEqual(config['domain_id'], 81)
                    self.assertNotEqual(config['run_id'], planned['run_id'])
                    self.assertEqual(json.loads((output / 'runtime_config.json').read_text()), config)
                    derived = ReplayCapsule.from_dict(json.loads((output / 'derived_capsule.json').read_text()))
                    self.assertEqual(derived.input, capsule.input)
                    self.assertEqual(capsule_path.read_bytes(), original)
                    self.assertEqual(report['status'], 'fail_environment', report)
                    self.assertEqual(report['reason_code'], 'runtime_preflight_blocked', report)
                    self.assertFalse(report['real_ros_execution'])
                    self.assert_auditable(report)
                    checked += 1
        self.assertEqual(checked, 96)

    def test_original_failure_new_health_is_not_confirmation(self):
        self.save_original(missing_middle)
        report = self.run_replay()
        self.assertEqual(report['status'], 'inconclusive')
        self.assertEqual(report['oracle_verdict'], 'pass')
        self.assertFalse(report['comparison_matches_original'])
        derived = json.loads((self.root / 'replay/derived_capsule.json').read_text())
        self.assertEqual(derived['execution']['oracle_verdict'], 'fail')
        self.assertEqual(derived['execution_history'][-1]['oracle_verdict'], 'pass')
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_same_failure_signature_matches_while_original_failure_is_retained(self):
        self.save_original(missing_middle)
        report = self.run_replay(missing_middle)
        self.assertEqual(report['status'], 'pass')
        self.assertEqual(report['oracle_verdict'], 'fail')
        self.assertTrue(report['comparison_matches_original'])
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_without_original_execution_records_execution_success_only(self):
        self.save_original(executed=False)
        report = self.run_replay()
        self.assertEqual(report['status'], 'pass')
        self.assertEqual(report['comparison_basis'], 'execution_success_only')
        self.assertFalse(report['comparison_matches_original'])
        self.assert_auditable(report)

    def test_preflight_failure_retains_reason_raw_and_original_execution(self):
        report = self.run_replay(blocked)
        self.assertEqual(report['status'], 'fail_environment')
        self.assertEqual(report['runtime_reason'], 'domain_conflict:occupied_domain_lease')
        self.assertIn(report['runtime_reason'], report['failure_reason'])
        derived = json.loads((self.root / 'replay/derived_capsule.json').read_text())
        self.assertEqual(derived['execution'], self.capsule.execution)
        self.assertEqual(derived['execution_history'], [])
        row = json.loads((self.root / 'replay/execution_metadata.json').read_text())
        self.assertFalse(row['actual_run'])
        self.assertTrue(row['environment_raw_evidence']['verified'])
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_runtime_exceptions_are_persisted_and_classified(self):
        for exception, status in ((RuntimeError('worker launch failed'), 'fail_environment'),
                                  (TimeoutError('worker timed out'), 'fail_timing')):
            with self.subTest(exception=exception):
                report = replay_cell(self.path, output_dir=self.root / status, domain_id=81,
                                     runtime=Mock(side_effect=exception))
                self.assertEqual(report['status'], status)
                self.assertIn(str(exception), report['runtime_reason'])
                self.assertIn('runtime/runtime_result.json', report['artifacts'])
                self.assert_auditable(report)
        self.assert_original_unchanged()

    def test_source_dirty_or_changed_blocks_before_runtime(self):
        for defect in ('dirty', 'changed'):
            self.current_source.return_value = dict(self.source, **(
                {'source_frozen': False, 'dirty_behavior_paths': ['src/changed.py']}
                if defect == 'dirty' else {'behavior_tree_sha256': 'b' * 64}))
            runtime = Mock()
            report = replay_cell(self.path, output_dir=self.root / defect, domain_id=81, runtime=runtime)
            self.assertEqual(report['status'], 'fail_environment')
            runtime.assert_not_called()
            self.assert_auditable(report)

    def test_source_change_during_runtime_aborts_and_keeps_raw(self):
        self.current_source.side_effect = [self.source, dict(self.source, head='changed')]
        report = self.run_replay()
        self.assertEqual(report['status'], 'fail_environment')
        self.assertEqual(report['reason_code'], 'source_identity_changed')
        self.assertIn('runtime/raw.jsonl', report['artifacts'])
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_missing_malformed_and_corrupt_original_artifacts_are_retained_as_failure(self):
        broken = self.root / 'broken-originals'
        broken.mkdir()
        for defect in ('missing', 'json', 'input_hash', 'raw_hash'):
            data = self.capsule.to_dict()
            path = broken / (defect + '.json')
            if defect == 'json':
                path.write_text('{')
            elif defect != 'missing':
                if defect == 'input_hash': data['input']['effective_input_sha256'] = 'f' * 64
                if defect == 'raw_hash': data['execution']['raw_sha256'] = 'f' * 64
                path.write_text(json.dumps(data))
            runtime = Mock()
            report = replay_cell(path, output_dir=self.root / ('out-' + defect), domain_id=81, runtime=runtime)
            self.assertEqual(report['status'], 'fail_artifact')
            runtime.assert_not_called()
            self.assert_auditable(report)

    def test_fresh_valid_domain_is_required(self):
        for domain in (80, True, -1, 232, '81'):
            runtime = Mock()
            report = replay_cell(self.path, output_dir=self.root / ('domain-' + str(domain)),
                                 domain_id=domain, runtime=runtime)
            self.assertEqual(report['status'], 'fail_environment')
            runtime.assert_not_called()
            self.assert_auditable(report)

    def test_bad_runtime_raw_cannot_be_a_success(self):
        def bad(config, directory):
            result = healthy(config, directory)
            result['raw_sha256'] = 'f' * 64
            return result
        report = self.run_replay(bad)
        self.assertEqual(report['status'], 'fail_artifact')
        self.assertIn('runtime/raw.jsonl', report['artifacts'])
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_output_reuse_and_original_directory_are_rejected_without_writes(self):
        with self.assertRaises(ValueError):
            replay_cell(self.path, output_dir=self.original / 'nested', domain_id=81, runtime=healthy)
        report = self.run_replay()
        before = Path(report['report_path']).read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_replay()
        self.assertEqual(Path(report['report_path']).read_bytes(), before)
        self.assert_original_unchanged()

    def test_audit_detects_raw_tamper_and_rehashed_false_confirmation(self):
        self.save_original(missing_middle)
        report = self.run_replay()
        output = Path(report['report_path']).parent
        attempt_path = output / 'attempt.json'
        attempt = json.loads(attempt_path.read_text())
        attempt.update(status='pass', failure_reason='', comparison_matches_original=True)
        attempt_path.write_text(json.dumps(attempt))
        report['status'] = 'pass'
        report['comparison_matches_original'] = True
        report['failure_reason'] = ''
        report['artifacts']['attempt.json'] = raw_sha256(attempt_path)
        report['report_sha256'] = canonical_hash({k: v for k, v in report.items() if k != 'report_sha256'})
        Path(report['report_path']).write_text(json.dumps(report))
        self.assertFalse(audit_replay(report['report_path'])['valid'])
        raw = output / 'runtime/raw.jsonl'
        raw.write_text(raw.read_text() + '{}\n')
        checked = audit_replay(report['report_path'])
        self.assertFalse(checked['valid'])
        self.assertTrue(any('raw.jsonl' in error for error in checked['errors']))

    def test_injected_actual_run_flags_and_default_fixture_never_qualify_as_ros(self):
        def unlabelled(config, directory):
            result = healthy(config, directory)
            result.pop('evidence_origin')
            return result
        report = self.run_replay(unlabelled)
        self.assertEqual(report['status'], 'pass')
        self.assertFalse(report['real_ros_execution'])
        self.assertFalse(report['real_runtime_attempt'])
        self.assert_auditable(report)
        with patch('r2f.runtime.ros2_pair.run_pair', side_effect=healthy):
            report = replay_cell(self.path, output_dir=self.root / 'default-fixture', domain_id=82)
        self.assertEqual(report['execution_origin'], 'r2f.runtime.ros2_pair.run_pair')
        self.assertFalse(report['real_ros_execution'])
        self.assertFalse(audit_replay(report['report_path'])['real_ros_execution'])
        self.assertIn('offline_fixture_cannot_establish_real_ros', report['qualification_errors'])
        self.assert_auditable(report)
        with patch('r2f.runtime.ros2_pair.run_pair', side_effect=unlabelled):
            report = replay_cell(self.path, output_dir=self.root / 'default-no-worker-chain', domain_id=83)
        self.assertFalse(report['real_ros_execution'])
        self.assertIn('worker_raw_link_missing:writer:process_started', report['qualification_errors'])
        self.assert_auditable(report)

    def test_invalid_runtime_result_is_auditable_artifact_failure(self):
        report = self.run_replay(lambda config, directory: None)
        self.assertEqual(report['status'], 'fail_artifact')
        self.assertFalse(report['real_ros_execution'])
        self.assert_auditable(report)

    def test_audit_recomputes_oracle_from_raw_after_metadata_is_rehashed(self):
        report = self.run_replay()
        output = Path(report['report_path']).parent
        row_path = output / 'execution_metadata.json'
        row = json.loads(row_path.read_text())
        row['typed_facts']['received'] = row['typed_facts']['received'][:-2]
        row_path.write_text(json.dumps(row))
        report['artifacts']['execution_metadata.json'] = raw_sha256(row_path)
        report['report_sha256'] = canonical_hash({k: v for k, v in report.items() if k != 'report_sha256'})
        Path(report['report_path']).write_text(json.dumps(report))
        checked = audit_replay(report['report_path'])
        self.assertFalse(checked['valid'])
        self.assertTrue(any('ingest' in error or 'execution metadata' in error for error in checked['errors']), checked)

    def test_corrupt_preflight_raw_keeps_environment_cause_but_cannot_qualify(self):
        def damaged(config, directory):
            result = blocked(config, directory)
            result['raw_sha256'] = 'f' * 64
            return result
        report = self.run_replay(damaged)
        self.assertEqual(report['status'], 'fail_environment')
        self.assertEqual(report['runtime_reason'], 'domain_conflict:occupied_domain_lease')
        self.assertFalse(report['real_runtime_attempt'])
        row = json.loads((self.root / 'replay/execution_metadata.json').read_text())
        self.assertFalse(row['environment_raw_evidence']['verified'])
        self.assert_auditable(report)

    def test_returned_persisted_result_conflict_keeps_both_and_fails_artifact(self):
        def conflicting(config, directory):
            result = healthy(config, directory)
            (directory / 'runtime_result.json').write_text(json.dumps(dict(result, reason='conflicting result')))
            return result
        report = self.run_replay(conflicting)
        self.assertEqual(report['status'], 'fail_artifact')
        self.assertEqual(report['reason_code'], 'runtime_result_mismatch')
        output = self.root / 'replay'
        self.assertNotEqual(json.loads((output / 'runtime_returned.json').read_text()),
                            json.loads((output / 'runtime/runtime_result.json').read_text()))
        self.assert_auditable(report)

    def test_registered_target_contract_is_required_without_replacing_capsule(self):
        with patch('r2f.campaign.replay.S1Module') as module:
            module.return_value.targets = {}
            runtime = Mock()
            report = self.run_replay(runtime)
        self.assertEqual(report['status'], 'fail_artifact')
        self.assertEqual(report['reason_code'], 'target_contract_mismatch')
        runtime.assert_not_called()
        self.assert_original_unchanged()
        self.assert_auditable(report)

    def test_cli_artifact_failure_and_read_only_verification(self):
        output = self.root / 'cli'
        script = REPO / 'scripts/replay_s1.py'
        result = subprocess.run([sys.executable, str(script), str(self.root / 'missing.json'),
                                 '--output', str(output), '--domain', '81'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'fail_artifact')
        verified = subprocess.run([sys.executable, str(script), '--verify', str(output / 'replay_report.json')],
                                  capture_output=True, text=True)
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.assertTrue(json.loads(verified.stdout)['valid'])


if __name__ == '__main__':
    unittest.main()
