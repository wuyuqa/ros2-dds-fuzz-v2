"""Issue21 CP1 offline workflow; no real SUT findings or human decisions."""
import copy
from pathlib import Path
import tempfile
import unittest

from r2f.core.candidate import candidate_execution_identity
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports import candidate_report as reports
import test_fuzz_review as fixtures


def assessment():
    claim = lambda text: {'description': text, 'evidence_keys': ['raw']}
    return {
        'schema_version': 'r2f.s1-capability-assessment/v1',
        'original_capabilities': {
            'triggering': [claim('Fixture late reader creation triggered the observation.')],
            'observation': [claim('Fixture instrumented endpoint observation.')],
            'facts': [claim('Fixture endpoint generation and match facts.')]},
        'agent_analysis_steps': [dict(claim('Agent correlated endpoint generations.'), step_id='correlate')],
        'proposed_tasks': [
            {'task_id': kind + '-gap', 'kind': kind, 'status': 'proposed',
             'description': 'Fixture proposed ' + kind + ' capability.',
             'evidence_keys': ['raw', 'source'], 'agent_step_ids': ['correlate'],
             'verification_requirements': ['Later defect case and stable normal control.']}
            for kind in ('seed', 'mutation', 'state', 'fact', 'rule', 'sidechannel')],
        'observation_limits': ['Fixture has no blackbox sidechannel.'],
        'blackbox_followup': {'status': 'pending', 'reason': 'Keep instrumented confirmation separate.'}}


class CapabilityAssessment(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='capability-offline-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        fixture = fixtures.ReviewPacketContracts('runTest')
        fixture.root = self.root
        self.row = fixture.row(observation_mode='instrumentation')
        self.refs = fixture.refs()
        self.log = self.root / 'reviews.jsonl'
        self.reviews, self.humans, self.trusted = [], [], set()

    def review(self, record_id='analysis-1', **changes):
        value = {'record_id': record_id, 'candidate_id': 'audit-run:candidate-1',
                 'reviewer_id': 'fixture-analyst', 'attribution': 'sut_candidate',
                 'conclusion': 'supported', 'independent_of': ['trigger-author'],
                 'reason': 'OFFLINE fixture analysis only.', 'evidence_refs': self.refs,
                 'capability_assessment': assessment()}
        value.update(changes)
        record = reports.append_review_record(self.log, value, original_row=self.row)
        self.reviews.append(record)
        return record

    def human(self, record_id='fixture-human-1', **changes):
        instruction = self.root / (record_id + '.txt')
        instruction.write_text('OFFLINE simulated human instruction; no actual decision.')
        value = dict(schema_version='r2f.s1-human-adjudication/v1', record_id=record_id,
                     candidate_id='audit-run:candidate-1', human_id='fixture-human',
                     origin='user_instruction', action='confirm', reason='OFFLINE fixture only.',
                     recorded_at='2026-11-22T00:00:00+08:00', instruction_ref=str(instruction),
                     instruction_sha256=raw_sha256(instruction),
                     execution_identity=candidate_execution_identity(self.row))
        value.update(changes)
        value['record_sha256'] = canonical_hash(value)
        self.trusted.add(value['record_sha256'])
        record = reports.append_human_record(self.root / 'humans.jsonl', value,
                                            original_rows=[self.row], reviews=self.reviews,
                                            human_authority=self.authority)
        self.humans.append(record)
        return record

    def authority(self, record):
        # A separately supplied host verifier fixture; never read trust from JSON.
        return record['record_sha256'] in self.trusted

    def ledger(self, **changes):
        options = dict(reviews=self.reviews, human_records=self.humans, human_authority=self.authority)
        options.update(changes)
        return reports.exploratory_candidate_ledger([self.row], **options)

    def feedback(self, **changes):
        return self.ledger(**changes)['root_cause_accounting']['cases'][0]['capability_assessment']

    def test_confirmed_fixture_projects_evidence_bound_pending_tasks_both_directions(self):
        raw_before = Path(self.row['raw_path']).read_bytes()
        review = self.review()
        human = self.human()
        ledger = self.ledger()
        feedback = ledger['root_cause_accounting']['cases'][0]['capability_assessment']
        self.assertEqual(feedback['status'], 'recorded')
        self.assertTrue(feedback['human_confirmed'])
        self.assertEqual(feedback['human_record_id'], human['record_id'])
        self.assertEqual(feedback['original_ref'], candidate_execution_identity(self.row))
        self.assertEqual(feedback['reviews'][0]['assessment'], assessment())
        self.assertEqual({task['kind'] for task in feedback['tasks']},
                         {'seed', 'mutation', 'state', 'fact', 'rule', 'sidechannel'})
        for task in feedback['tasks']:
            self.assertEqual(task['original_ref'], feedback['original_ref'])
            self.assertEqual(task['review_record_id'], review['record_id'])
            self.assertEqual(task['review_record_sha256'], review['record_sha256'])
            self.assertEqual(task['candidate_id'], 'audit-run:candidate-1')
            self.assertIn(task['task_ref'], feedback['reviews'][0]['task_refs'])
            self.assertEqual(task['evidence_sha256']['raw'], self.row['raw_sha256'])
            self.assertEqual(task['status'], 'proposed')
            self.assertFalse(task['implemented'])
        self.assertEqual(ledger['root_cause_accounting']['human_confirmed_cases'], 1)
        self.assertEqual(Path(self.row['raw_path']).read_bytes(), raw_before)
        self.assertEqual(reports.read_review_records(self.log, original_rows=[self.row]), [review])

    def test_missing_sidechannel_keeps_instrumented_confirmation_separate(self):
        value = assessment()
        value['proposed_tasks'] = [task for task in value['proposed_tasks'] if task['kind'] != 'sidechannel']
        self.review(capability_assessment=value)
        self.human()
        feedback = self.feedback()
        self.assertEqual(feedback['observation_mode'], 'instrumentation')
        self.assertTrue(feedback['human_confirmed'])
        self.assertEqual(feedback['reviews'][0]['assessment']['blackbox_followup']['status'], 'pending')
        self.assertEqual(self.ledger()['root_cause_accounting']['human_confirmed_cases'], 1)

    def test_agent_analysis_and_unverified_human_never_qualify_feedback(self):
        self.review()
        self.assertEqual(self.feedback()['status'], 'awaiting_human_confirmation')
        self.human()
        result = self.ledger(human_authority=None)
        feedback = result['root_cause_accounting']['cases'][0]['capability_assessment']
        self.assertFalse(feedback['human_confirmed'])
        self.assertIsNone(feedback['human_record_id'])
        self.assertEqual(result['root_cause_accounting']['human_confirmed_cases'], 0)
        self.assertTrue(result['human_errors'])

    def test_assessment_conflict_preserves_confirmed_case_and_all_proposals(self):
        first = self.review()
        value = assessment()
        value['agent_analysis_steps'][0]['description'] = 'Different evidence interpretation.'
        second = self.review('analysis-2', reviewer_id='second-fixture-analyst', capability_assessment=value)
        self.human()
        feedback = self.feedback()
        self.assertEqual(feedback['status'], 'conflicting')
        self.assertTrue(feedback['human_confirmed'])
        self.assertEqual([r['record_id'] for r in feedback['reviews']], [first['record_id'], second['record_id']])
        self.assertEqual(len(feedback['tasks']), 12)
        self.assertEqual(len({canonical_hash(task['task_ref']) for task in feedback['tasks']}), 12)
        self.assertEqual(self.ledger()['root_cause_accounting']['human_confirmed_cases'], 1)

    def test_review_without_assessment_does_not_silently_agree(self):
        self.review()
        self.review('analysis-2', reviewer_id='second-fixture-analyst', capability_assessment=None)
        self.human()
        feedback = self.feedback()
        self.assertEqual(feedback['status'], 'needs_assessment')
        self.assertEqual(feedback['missing_review_record_ids'], ['analysis-2'])
        self.assertTrue(feedback['human_confirmed'])

    def test_import_rejects_unbound_or_implemented_claims_without_appending(self):
        self.review()
        before = self.log.read_bytes()
        bad_values = []
        for status in ('implemented', 'completed', 'validated'):
            value = assessment()
            value['proposed_tasks'][0]['status'] = status
            bad_values.append(value)
        value = assessment()
        value['original_capabilities']['facts'][0]['evidence_keys'] = ['fabricated']
        bad_values.append(value)
        value = assessment()
        value['agent_analysis_steps'][0]['evidence_keys'] = []
        bad_values.append(value)
        value = assessment()
        value['proposed_tasks'][0]['agent_step_ids'] = ['nonexistent-step']
        bad_values.append(value)
        value = assessment()
        value['proposed_tasks'].append(copy.deepcopy(value['proposed_tasks'][0]))
        bad_values.append(value)
        value = assessment()
        value['agent_analysis_steps'].append(copy.deepcopy(value['agent_analysis_steps'][0]))
        bad_values.append(value)
        value = assessment()
        value['blackbox_followup']['status'] = 'implemented'
        bad_values.append(value)
        value = assessment()
        value['original_capabilities']['triggering'] = []
        bad_values.append(value)
        value = assessment()
        value['human_confirmed'] = True
        bad_values.append(value)
        for index, value in enumerate(bad_values):
            with self.subTest(index=index), self.assertRaises(ValueError):
                self.review('invalid-' + str(index), supersedes_record_id='analysis-1',
                            revision_reason='Invalid fixture revision.', capability_assessment=value)
            self.assertEqual(self.log.read_bytes(), before)

    def test_duplicate_same_id_conflict_and_superseding_history(self):
        original = self.review()
        before = self.log.read_bytes()
        duplicate = reports.append_review_record(self.log, original, original_row=self.row)
        self.assertEqual(duplicate, original)
        self.assertEqual(self.log.read_bytes(), before)
        conflict = copy.deepcopy(original)
        conflict['capability_assessment']['observation_limits'] = ['Conflicting content.']
        conflict['record_sha256'] = canonical_hash({k: v for k, v in conflict.items() if k != 'record_sha256'})
        with self.assertRaisesRegex(ValueError, 'conflict'):
            reports.append_review_record(self.log, conflict, original_row=self.row)
        self.assertEqual(self.log.read_bytes(), before)
        value = assessment()
        value['proposed_tasks'] = [dict(value['proposed_tasks'][0], status='pending')]
        revised = self.review('analysis-2', supersedes_record_id='analysis-1',
                              revision_reason='Narrow fixture proposal.', capability_assessment=value)
        self.human()
        ledger = self.ledger()
        self.assertEqual(ledger['candidates'][0]['analysis']['history'], [original, revised])
        self.assertEqual(len(self.feedback()['tasks']), 1)
        self.assertEqual(self.feedback()['tasks'][0]['review_record_id'], 'analysis-2')
        self.assertTrue(self.log.read_bytes().startswith(before))
        self.assertEqual(reports.read_review_records(self.log, original_rows=[self.row]), [original, revised])

    def test_removing_assessment_and_human_revocation_preserve_history(self):
        first = self.review()
        self.human()
        self.review('analysis-2', supersedes_record_id='analysis-1',
                    revision_reason='Withdraw assessment pending facts.', capability_assessment=None)
        self.assertEqual(self.feedback()['status'], 'needs_assessment')
        self.assertEqual(self.feedback()['tasks'], [])
        self.assertTrue(self.feedback()['human_confirmed'])
        self.human('fixture-human-2', action='revoke', supersedes_record_id='fixture-human-1',
                   revision_reason='Fixture revocation.')
        self.assertFalse(self.feedback()['human_confirmed'])
        self.assertIn(first, self.ledger()['candidates'][0]['analysis']['history'])

    def test_evidence_tamper_retains_case_and_rejected_analysis_without_current_tasks(self):
        record = self.review()
        self.human()
        before = self.log.read_bytes()
        Path(self.refs['source']).write_text('tampered offline source reference')
        ledger = self.ledger()
        self.assertTrue(ledger['review_errors'])
        self.assertEqual(ledger['root_cause_accounting']['human_confirmed_cases'], 0)
        feedback = ledger['root_cause_accounting']['cases'][0]['capability_assessment']
        self.assertEqual(feedback['status'], 'needs_evidence')
        self.assertEqual(feedback['tasks'], [])
        self.assertIn(record, ledger['candidates'][0]['analysis']['history'])
        self.assertEqual(self.log.read_bytes(), before)

    def test_unknown_conflicting_attribution_and_control_cannot_gain_confirmation(self):
        self.review()
        self.review('unknown-opinion', reviewer_id='unknown-analyst', conclusion='needs_evidence', attribution='unknown')
        self.human()
        self.assertFalse(self.feedback()['human_confirmed'])
        self.assertEqual(self.ledger()['root_cause_accounting']['human_confirmed_cases'], 0)
        self.row.update(arm='negative')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['human_confirmed_cases'], 0)
        self.assertEqual(ledger['natural_candidate_count'], 0)
        self.assertEqual(ledger['control_count'], 1)

    def test_zero_branch_and_legacy_reviews_keep_existing_identity_and_shape(self):
        zero = reports.exploratory_candidate_ledger([])
        self.assertEqual(zero['natural_candidate_count'], 0)
        self.assertEqual(zero['human_confirmed_candidates'], 0)
        self.assertEqual(zero['root_cause_accounting']['cases'], [])
        self.assertIn('No independently supported SUT candidate', zero['zero_candidate_explanation'])
        legacy = self.review(capability_assessment=None)
        legacy.pop('capability_assessment')
        legacy['record_sha256'] = canonical_hash({k: v for k, v in legacy.items() if k != 'record_sha256'})
        legacy_log = self.root / 'legacy.jsonl'
        stored = reports.append_review_record(legacy_log, legacy, original_row=self.row)
        before = legacy_log.read_bytes()
        self.assertEqual(reports.append_review_record(legacy_log, stored, original_row=self.row), legacy)
        self.assertEqual(legacy_log.read_bytes(), before)
        ledger = self.ledger(reviews=[legacy])
        self.assertNotIn('capability_assessment', ledger['root_cause_accounting']['cases'][0])
        self.assertEqual(ledger['review_records'], [legacy])

    def test_core_review_uses_same_validation_and_hash_binding(self):
        from r2f.core.candidate import CandidateLifecycle
        core = CandidateLifecycle('audit-run:candidate-1', self.row)
        core.record_review(reviewer_id='fixture-agent', conclusion='supported', evidence_refs=self.refs,
                           capability_assessment=assessment())
        review = core.to_dict()['reviews'][0]
        self.assertEqual(review['capability_assessment'], assessment())
        self.assertEqual(review['evidence_sha256']['raw'], self.row['raw_sha256'])
        self.assertNotEqual(core.candidate_status, 'confirmed')
        value = assessment()
        value['proposed_tasks'][0]['status'] = 'implemented'
        with self.assertRaises(ValueError):
            core.record_review(reviewer_id='fixture-agent', conclusion='supported', evidence_refs=self.refs,
                               capability_assessment=value)
        self.assertEqual(len(core.to_dict()['reviews']), 1)

    def test_derived_report_auditors_reconstruct_tasks_and_reject_rehashed_forgery(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        self.review()
        self.human()
        original_path, base = build_report(self.root, [self.row], reviews=[])
        original_bytes = original_path.read_bytes()
        raw_bytes = Path(self.row['raw_path']).read_bytes()
        ledger = self.ledger()

        def derived(value, label):
            ledger_path = self.root / (label + '-ledger.json')
            path = self.root / (label + '-report.json')
            run_report.write_json(ledger_path, value)
            report = dict(base, review_records=self.reviews, human_records=self.humans,
                          candidate_ledger_path=ledger_path.name, candidate_ledger_sha256=canonical_hash(value))
            report['report_sha256'] = run_report.exploratory_report_digest(report)
            run_report.write_json(path, report)
            return path

        valid = derived(ledger, 'faithful')
        forged = copy.deepcopy(ledger)
        forged['root_cause_accounting']['cases'][0]['capability_assessment']['tasks'][0]['implemented'] = True
        invalid = derived(forged, 'forged')
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            result = audit(valid, human_authority=self.authority)
            self.assertTrue(result['evidence_valid'], result['errors'])
            self.assertFalse(result['sealed'])
            result = audit(invalid, human_authority=self.authority)
            self.assertFalse(result['evidence_valid'])
            self.assertIn('exploratory_ledger_mismatch:root_cause_accounting', result['errors'])
        self.assertEqual(original_path.read_bytes(), original_bytes)
        self.assertEqual(Path(self.row['raw_path']).read_bytes(), raw_bytes)
