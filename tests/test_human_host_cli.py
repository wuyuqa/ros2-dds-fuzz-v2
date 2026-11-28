"""Real subprocess CLI ingress; synthetic keys/rows, no ROS or replay execution."""
import json
import os
from pathlib import Path
import subprocess
import sys

from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports import candidate_report as reports
from r2f.reports import run_report
from test_human_host import SignedHostFixture
from test_fuzz_audit import base_row, build_report


REPO = Path(__file__).resolve().parents[1]


class HumanHostCLI(SignedHostFixture):
    def cli(self, script, *args):
        result = subprocess.run([sys.executable, str(REPO / 'scripts' / script),
                                 *map(str, args)], capture_output=True, text=True,
                                env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
        return result, json.loads(result.stdout) if result.stdout else None

    def flags(self):
        return ['--human-policy', self.policy, '--human-policy-sha256', self.policy_hash,
                '--human-signatures', self.signatures]

    def record_file(self, record):
        path = self.root / (record['record_id'] + '.json')
        path.write_text(json.dumps(record), encoding='utf-8')
        return path

    def indexed_fixture(self, path, report):
        """Offline metadata only: no dispatch request, runtime, replay or campaign."""
        identity = self.root / 'FIXTURE-ONLY-intake.json'
        identity.write_text(json.dumps({'fixture': True, 'run_id': report['run_id']}))
        index = {'run_id': report['run_id'], 'source_manifest': report['source_manifest'],
                 'identities': [{'cell_id': row['cell_id'], 'path': identity.name,
                                 'sha256': raw_sha256(identity)} for row in report['rows']],
                 'reports': [{'path': path.name, 'sha256': raw_sha256(path)}]}
        index_path = self.root / 'evidence-index.json'
        run_report.write_json(index_path, index)
        summary = {'schema_version': 'r2f.runner-auto-replay/v1', 'run_id': report['run_id'],
                   'original_index': {'path': index_path.name, 'sha256': raw_sha256(index_path)},
                   'original_artifacts': [{'path': path.name, 'sha256': raw_sha256(path)}],
                   'original_artifacts_unchanged': True,
                   'entries': [{'cell_id': row['cell_id'], 'status': 'not_run',
                                'reason_code': 'offline_fixture_replay_not_requested',
                                'candidate_eligible': False, 'original_confirmed': False,
                                'internal_state_recovery': False} for row in report['rows']]}
        summary['summary_sha256'] = canonical_hash(summary)
        run_report.write_json(self.root / 'replay-summary.json', summary)

    def test_cli_signed_import_reopen_query_confirm_revoke_preserves_indexed_original(self):
        path, original = build_report(self.root, [self.row], reviews=self.reviews)
        self.indexed_fixture(path, original)
        checked = run_report.audit_report(path)
        self.assertTrue(checked['evidence_valid'], checked['errors'])
        originals = {p: p.read_bytes() for p in self.root.iterdir() if p.is_file()}
        record = self.record()
        self.sign(record)
        record_path = self.record_file(record)
        ledger_path = self.root / 'confirmed-projection.json'
        process, view = self.cli('import_human_decision.py', path, '--record', record_path,
                                 '--human-log', self.log, '--output-ledger', ledger_path, *self.flags())
        self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
        self.assertEqual(view['projection_kind'], 'canonical_human_ledger')
        self.assertTrue(view['original_report']['audit']['evidence_valid'])
        self.assertEqual(view['ledger']['human_confirmed_candidates'], 1)
        self.assertEqual(json.loads(ledger_path.read_text()), view['ledger'])
        before = self.log.read_bytes()
        duplicate, _ = self.cli('import_human_decision.py', path, '--record', record_path,
                                 '--human-log', self.log, *self.flags())
        self.assertEqual(duplicate.returncode, 0, duplicate.stdout)
        self.assertEqual(self.log.read_bytes(), before)
        reopened, queried = self.cli('import_human_decision.py', path, '--human-log', self.log, *self.flags())
        self.assertEqual(reopened.returncode, 0, reopened.stdout)
        self.assertTrue(queried['human_log']['reopened_and_validated'])
        self.assertEqual(queried['ledger']['human_confirmed_candidates'], 1)
        self.assertEqual(queried['ledger']['human_records'], [record])
        # A renamed derived report cannot acquire an immutable index membership.
        derived = self.project(original, [record], 'unsupported-derived')
        result = run_report.audit_report(derived, human_authority=self.host())
        self.assertFalse(result['evidence_valid'])
        self.assertTrue(any('report_not_indexed' in error for error in result['errors']), result)
        revoke = self.record('fixture-revoke', 'revoke', supersedes_record_id=record['record_id'],
                             revision_reason='Fixture withdrawal.')
        self.sign(revoke)
        withdrawn, view = self.cli('import_human_decision.py', path, '--record', self.record_file(revoke),
                                   '--human-log', self.log, *self.flags())
        self.assertEqual(withdrawn.returncode, 0, withdrawn.stdout)
        self.assertEqual(view['ledger']['human_confirmed_candidates'], 0)
        self.assertEqual(view['ledger']['human_records'], [record, revoke])
        for artifact, data in originals.items():
            self.assertEqual(artifact.read_bytes(), data, str(artifact))

    def test_verifier_policy_flags_and_unchanged_exit_semantics(self):
        path, original = build_report(self.root, [self.row], reviews=self.reviews)
        record = self.record()
        self.sign(record)
        derived = self.project(original, [record], 'supported-unindexed-derived')
        default, denied = self.cli('verify_s1.py', derived)
        self.assertEqual(default.returncode, 1)
        self.assertFalse(denied['evidence_valid'])
        trusted, accepted = self.cli('verify_s1.py', derived, *self.flags())
        self.assertEqual(trusted.returncode, 1)  # Valid exploratory reports never seal.
        self.assertTrue(accepted['evidence_valid'], accepted['errors'])
        self.assertEqual(accepted['human_confirmed_candidates'], 1)
        self.assertFalse(accepted['sealed'])
        unchanged, normal = self.cli('verify_s1.py', path)
        self.assertEqual(unchanged.returncode, 1)
        self.assertTrue(normal['evidence_valid'], normal['errors'])
        self.assertEqual(normal['human_confirmed_candidates'], 0)

    def test_indexed_zero_decision_cli_needs_no_production_policy_or_new_trust(self):
        path, report = build_report(self.root, [base_row(self.root, 'normal', verdict='pass')])
        self.indexed_fixture(path, report)
        originals = {p: p.read_bytes() for p in self.root.iterdir() if p.is_file()}
        process, view = self.cli('import_human_decision.py', path)
        self.assertEqual(process.returncode, 0, process.stdout)
        self.assertTrue(view['original_report']['audit']['evidence_valid'])
        self.assertEqual(view['ledger']['human_records'], [])
        self.assertEqual(view['ledger']['human_confirmed_candidates'], 0)
        self.assertEqual(view['ledger']['root_cause_accounting']['independent_new_root_causes'], 0)
        self.assertFalse(self.log.exists())
        for artifact, data in originals.items():
            self.assertEqual(artifact.read_bytes(), data)

    def test_cli_rejections_do_not_write_decisions_or_overwrite_outputs(self):
        path, original = build_report(self.root, [self.row], reviews=self.reviews)
        record = self.record()
        record_path = self.record_file(record)
        process, result = self.cli('import_human_decision.py', path, '--record', record_path,
                                   '--human-log', self.log)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result['outcome'], 'human_host_policy_required')
        self.assertFalse(self.log.exists())
        process, result = self.cli('import_human_decision.py', path, '--record', record_path,
                                   '--human-log', self.log, *self.flags())
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result['outcome'], 'human_host_signature_unavailable')
        self.assertFalse(self.log.exists())
        self.sign(record)
        process, result = self.cli('import_human_decision.py', path, '--record', record_path,
                                   '--human-log', self.log, '--output-ledger', path, *self.flags())
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result['outcome'], 'human_host_output_exists')
        self.assertFalse(self.log.exists())
        # Original audit failure is retained; authentication cannot promote it.
        original['candidate_ledger_sha256'] = '0' * 64
        original['report_sha256'] = run_report.exploratory_report_digest(original)
        run_report.write_json(path, original)
        process, result = self.cli('import_human_decision.py', path, '--record', record_path,
                                   '--human-log', self.log, *self.flags())
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result['outcome'], 'human_host_original_report_audit_failed')
        self.assertFalse(self.log.exists())

    def test_policy_never_loaded_from_json_and_partial_flags_are_rejected(self):
        path, original = build_report(self.root, [self.row], reviews=self.reviews)
        record = self.record()
        self.sign(record)
        derived = self.project(original, [record], 'signed')
        data = json.loads(derived.read_text())
        data['human_policy'] = {'path': str(self.policy), 'sha256': self.policy_hash,
                                'signatures': str(self.signatures)}
        data['report_sha256'] = run_report.exploratory_report_digest(data)
        run_report.write_json(derived, data)
        process, result = self.cli('verify_s1.py', derived)
        self.assertEqual(process.returncode, 1)
        self.assertFalse(result['evidence_valid'])
        self.assertEqual(result['human_confirmed_candidates'], 0)
        process, _ = self.cli('verify_s1.py', path, '--human-policy', self.policy)
        self.assertEqual(process.returncode, 2)
        self.assertIn('human_host_configuration_incomplete', process.stderr)
        process, result = self.cli('verify_s1.py', path, '--human-policy', self.root / 'missing-policy',
                                   '--human-policy-sha256', self.policy_hash,
                                   '--human-signatures', self.signatures)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result['errors'], ['human_host_policy_unavailable'])
