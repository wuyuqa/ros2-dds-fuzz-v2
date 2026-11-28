"""Canonical saved-case reconnect: fixtures prove process, never activation."""
import copy
import json
from pathlib import Path
import unittest

from r2f.campaign import runner
from r2f.core import Registry
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.modules.s1.module import register
from r2f.reports import candidate_report as reports
import test_capability_reconnect_evaluation as cp3


class ReconnectActivation(unittest.TestCase):
    def setUp(self):
        self.fixture = cp3.ReconnectEvaluation('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture
        self.root = f.root
        registry = Registry()
        register(registry)
        descriptor = registry.bind_services(f.target_id).descriptor
        model = descriptor['components']['oracle']['callables']['evaluate']
        f.cp.models['after'].update(path=model['source_file'], sha256=model['source_sha256'])
        saved_inputs = list(f.saved.values())
        proposal = f.cp.proposal()
        f.saved = {ref['sha256']: inputs for ref, inputs in zip(f.cp.bundle['cases'], saved_inputs)}
        self.record = reports.append_review_record(f.cp.case.log, proposal, original_row=f.cp.row)
        self.output = self.root / 'canonical-reconnect'

    def run_saved(self):
        f = self.fixture
        return runner.reevaluate_capability(self.output, review_log=f.cp.case.log,
            validation_record_id=self.record['record_id'], original_row=f.cp.row,
            saved_cases=f.saved)

    def test_canonical_runner_publishes_actual_pending_fixture_and_auditable_report(self):
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        report = self.run_saved()
        self.assertEqual(before, {p: p.read_bytes() for p in before})
        view = report['capability_reconnect']
        self.assertEqual(view['status'], 'pending')
        self.assertEqual(view['evaluation_status'], 'evaluated')
        self.assertEqual(view['qualification'], 'fixture')
        self.assertFalse(view['implemented'])
        self.assertFalse(view['human_confirmed'])
        self.assertFalse(view['new_execution'])
        self.assertFalse(view['blackbox_supported'])
        self.assertEqual(view['required_denominator'], 72)
        self.assertEqual(view['natural_candidates'], 0)
        self.assertIn('actual_feedback_evidence', view['pending_obligations'])
        self.assertIn('sidechannel_to_blackbox_followup', view['pending_obligations'])
        self.assertNotEqual(view['capture_source'], view['evaluation_source'])
        self.assertEqual(view['validation_review']['record_id'], self.record['record_id'])
        evaluation = json.loads((self.output / view['evaluation_ref']['path']).read_text())
        self.assertEqual([r['after']['verdict'] for r in evaluation['call_receipts']], ['fail', 'pass', 'pass'])
        self.assertTrue(all(r['oracle_called'] for r in evaluation['call_receipts']))
        self.assertEqual(view['selected_model_version'], evaluation['binding_sha256'])
        self.assertNotEqual(view['selected_model_version'], evaluation['models']['after']['version'])
        audit = reports.query_runner_evidence(self.output)
        self.assertTrue(audit['evidence_valid'], audit)

    def test_audit_rejects_rehashed_oracle_summary(self):
        self.run_saved()
        evaluation = json.loads((self.output / 'capability-reevaluation.json').read_text())
        evaluation['call_receipts'][0]['after']['verdict'] = 'pass'
        evaluation['call_receipts'][0]['result_sha256'] = canonical_hash(evaluation['call_receipts'][0]['after'])
        self.rehash_output(evaluation=evaluation)
        audit = reports.query_runner_evidence(self.output)
        self.assertFalse(audit['evidence_valid'], audit)
        self.assertIn('reconnect_evaluation_mismatch', str(audit['errors']))

    def test_review_append_and_import_reject_authored_reconnect_summary(self):
        f = self.fixture
        forged = copy.deepcopy(self.record)
        forged['capability_reconnect'] = dict(status='evaluated', implemented=True,
                                              selected_model_version='caller-supplied', verdict='pass')
        forged['record_sha256'] = canonical_hash({k: v for k, v in forged.items() if k != 'record_sha256'})
        append_log = self.root / 'forged-append.jsonl'
        append_log.write_text(f.cp.case.log.read_text().splitlines()[0] + '\n')
        with self.assertRaisesRegex(ValueError, 'reconnect_summary_requires_runner'):
            reports.append_review_record(append_log, forged, original_row=f.cp.row)
        imported = self.root / 'forged-import.jsonl'
        prefix = f.cp.case.log.read_text().splitlines()[0]
        imported.write_text(prefix + '\n' + json.dumps(forged) + '\n')
        with self.assertRaisesRegex(ValueError, 'reconnect_summary_requires_runner'):
            reports.read_review_records(imported, original_rows=[f.cp.row])
        ledger = reports.exploratory_candidate_ledger([f.cp.row], reviews=[f.cp.parent, forged])
        self.assertIn('reconnect_summary_requires_runner', str(ledger['review_errors']))

    def rehash_output(self, *, evaluation=None, report=None):
        path = self.output / 'capability-reevaluation.json'
        report_path = self.output / 'candidate-report.json'
        if report is None:
            report = json.loads(report_path.read_text())
        if evaluation is not None:
            evaluation.pop('evaluation_sha256', None)
            evaluation['evaluation_sha256'] = canonical_hash(evaluation)
            path.write_text(json.dumps(evaluation))
            report['capability_reconnect']['evaluation_ref']['sha256'] = raw_sha256(path)
        report_path.write_text(json.dumps(report))
        index_path = self.output / 'evidence-index.json'
        index = json.loads(index_path.read_text())
        index['reports'][0]['sha256'] = raw_sha256(report_path)
        index_path.write_text(json.dumps(index))

    def test_audit_rejects_rehashed_stale_binding_task_case_model_and_source(self):
        self.run_saved()
        original = json.loads((self.output / 'capability-reevaluation.json').read_text())
        for field in ('binding', 'task', 'case', 'model', 'source', 'review'):
            with self.subTest(field=field):
                evaluation = copy.deepcopy(original)
                if field == 'binding':
                    evaluation['binding_sha256'] = '0' * 64
                    evaluation['selected_model_version'] = '0' * 64
                elif field == 'task':
                    evaluation['task_ref']['task_id'] = 'unrelated-task'
                elif field == 'case':
                    evaluation['call_receipts'][0]['case_ref']['sha256'] = '0' * 64
                elif field == 'model':
                    evaluation['models']['after']['version'] = 'caller-model-v999'
                elif field == 'source':
                    evaluation['evaluation_source']['head'] = '0' * 40
                else:
                    evaluation['validation_review']['record_sha256'] = '0' * 64
                self.rehash_output(evaluation=evaluation)
                audit = reports.query_runner_evidence(self.output)
                self.assertFalse(audit['evidence_valid'], audit)
                self.assertIn('reconnect_evaluation_mismatch', str(audit['errors']))

    def test_audit_rejects_rehashed_activation_and_blackbox_promotion(self):
        report = self.run_saved()
        report['capability_reconnect'].update(implemented=True, human_confirmed=True,
            blackbox_supported=True, pending_obligations=[], status='active')
        self.rehash_output(report=report)
        audit = reports.query_runner_evidence(self.output)
        self.assertFalse(audit['evidence_valid'], audit)
        self.assertIn('reconnect_report_projection_mismatch', str(audit['errors']))

    def test_runner_rejects_unbound_saved_case_before_publication(self):
        f = self.fixture
        key = next(iter(f.saved))
        f.saved[key]['runtime']['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            self.run_saved()
        self.assertFalse(self.output.exists())

    def test_audit_rejects_changed_authoritative_log(self):
        self.run_saved()
        with self.fixture.cp.case.log.open('a') as stream:
            stream.write('\n')
        audit = reports.query_runner_evidence(self.output)
        self.assertFalse(audit['evidence_valid'], audit)

    def test_published_artifacts_are_immutable(self):
        self.run_saved()
        before = {p: p.read_bytes() for p in self.output.iterdir()}
        with self.assertRaises(FileExistsError):
            self.run_saved()
        self.assertEqual(before, {p: p.read_bytes() for p in before})

    def test_report_retains_authoritative_sidechannel_tasks_and_blackbox_reason(self):
        report = self.run_saved()
        assessment = report['capability_reconnect']['task_assessment']
        self.assertEqual(assessment['blackbox_followup'], {
            'status': 'pending', 'reason': 'Keep instrumented confirmation separate.'})
        self.assertEqual([t['task_id'] for t in assessment['proposed_tasks'] if t['kind'] == 'sidechannel'],
                         ['sidechannel-gap'])
        self.assertEqual(assessment['observation_limits'], ['Fixture has no blackbox sidechannel.'])

    def test_audit_rejects_rehashed_manifest_execution_claims(self):
        self.run_saved()
        manifest_path = self.output / 'manifest.json'
        original = json.loads(manifest_path.read_text())
        claims = [
            {'interpretation': 'runtime_execution', 'new_execution': True},
            {'interpretation': 'runtime_execution', 'new_execution': False},
            {'new_execution': False},
            {'interpretation': 'offline_reinterpretation', 'new_execution': True},
            {'interpretation': 'offline_reinterpretation'},
        ]
        for claim in claims:
            with self.subTest(claim=claim):
                manifest = {k: v for k, v in original.items()
                            if k not in ('interpretation', 'new_execution')}
                manifest.update(claim)
                manifest_path.write_text(json.dumps(manifest))
                index_path = self.output / 'evidence-index.json'
                index = json.loads(index_path.read_text())
                index['manifest']['sha256'] = raw_sha256(manifest_path)
                index_path.write_text(json.dumps(index))
                audit = reports.query_runner_evidence(self.output)
                self.assertFalse(audit['evidence_valid'], audit)
                self.assertIn('reconnect_execution_claim_forbidden', str(audit['errors']))
