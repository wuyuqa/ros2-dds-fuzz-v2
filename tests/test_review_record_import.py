"""Offline CP1 import contracts through the public review/report interfaces."""
import copy
import json
import pathlib
import tempfile
import unittest

from r2f.reports.candidate_report import (
    append_review_record, exploratory_candidate_ledger, read_review_records,
)
from test_fuzz_audit import base_row


class ReviewRecordImport(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.row = base_row(self.root, 'original', verdict='unknown')
        self.source = self.root / 'source.json'
        self.source.write_text('{"source_commit":"fixture"}\n')
        self.capsule = self.root / 'capsule.json'
        self.capsule.write_text('{"fixture":true}\n')
        self.row['replay_path'] = str(self.capsule)
        self.log = self.root / 'reviews.jsonl'
        self.record = {
            'candidate_id': 'audit-run:original', 'reviewer_id': 'agent-reviewer',
            'conclusion': 'needs_evidence', 'attribution': 'unknown',
            'reason': 'Offline fixture has no independently observed SUT violation.',
            'evidence_refs': {'source': str(self.source), 'raw': self.row['raw_path'],
                              'capsule': str(self.capsule)},
        }

    def test_repeated_import_has_stable_id_and_keeps_original_bytes(self):
        original = copy.deepcopy(self.row)
        first = append_review_record(self.log, self.record, original_row=self.row)
        before = self.log.read_bytes()
        again = append_review_record(self.log, self.record, original_row=self.row)
        self.assertEqual(again, first)
        self.assertTrue(first['record_id'])
        self.assertEqual(self.log.read_bytes(), before)
        self.assertEqual(read_review_records(self.log, original_rows=[self.row]), [first])
        self.assertEqual(self.row, original)

    def test_import_rejects_schema_hash_and_execution_substitution(self):
        signed = append_review_record(self.log, self.record, original_row=self.row)
        before = self.log.read_bytes()
        for changes in (
                {'schema_version': 'unknown'}, {'record_sha256': '0' * 64},
                {'reason': 'Changed after signing'}, {'record_id': 'substituted-id'},
                {'execution_identity': dict(signed['execution_identity'], run_id='other-run')},
                {'candidate_id': 'audit-run:other-cell'}):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    append_review_record(self.log, dict(signed, **changes), original_row=self.row)
                self.assertEqual(self.log.read_bytes(), before)
        for field, value in (
                ('run_id', 'other-run'), ('cell_id', 'other-cell'),
                ('provenance', {'source_commit': 'other-source'}),
                ('target_id', 'other-target'), ('target_revision', 9),
                ('rule_id', 'other-rule'), ('effective_input_sha256', '0' * 64),
                ('rmw', 'other-rmw'), ('raw_sha256', '0' * 64)):
            wrong_row = dict(self.row, **{field: value})
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, 'identity|candidate|raw'):
                    append_review_record(self.root / (field + '.jsonl'), signed,
                                         original_row=wrong_row)
                with self.assertRaisesRegex(ValueError, 'identity|candidate|raw'):
                    read_review_records(self.log, original_rows=[wrong_row])
        with self.assertRaises(ValueError):
            append_review_record(self.root / 'unbound.jsonl', self.record)

    def test_original_bound_read_rejects_substituted_capsule_with_valid_hashes(self):
        from r2f.core.evidence import canonical_hash, raw_sha256
        signed = append_review_record(self.log, self.record, original_row=self.row)
        self.assertEqual(read_review_records(self.log, original_rows=[self.row]), [signed])
        wrong_capsule = self.root / 'other-execution-capsule.json'
        wrong_capsule.write_text('{"fixture":true,"run_id":"other-execution"}\n')
        wrong = copy.deepcopy(signed)
        wrong['evidence_refs']['capsule'] = str(wrong_capsule)
        wrong['evidence_sha256']['capsule'] = raw_sha256(wrong_capsule)
        wrong['record_sha256'] = canonical_hash({
            key: value for key, value in wrong.items() if key != 'record_sha256'})
        imported = self.root / 'substituted.jsonl'
        imported.write_text(json.dumps(wrong) + '\n')
        # The record and reference digests are valid; only the original-row
        # binding can distinguish this capsule from the executed input.
        self.assertEqual(read_review_records(imported), [wrong])
        with self.assertRaisesRegex(ValueError, 'review_capsule_identity_mismatch'):
            append_review_record(self.root / 'append-attempt.jsonl', wrong, original_row=self.row)
        ledger = exploratory_candidate_ledger([self.row], reviews=[wrong])
        self.assertEqual(ledger['review_records'], [])
        self.assertIn('review_capsule_identity_mismatch', ledger['review_errors'][0]['detail'])
        before = {path: path.read_bytes() for path in (
            self.log, imported, self.capsule, pathlib.Path(self.row['raw_path']))}
        with self.assertRaisesRegex(ValueError, 'review_capsule_identity_mismatch'):
            read_review_records(imported, original_rows=[self.row])
        self.assertEqual({path: path.read_bytes() for path in before}, before)

    def test_reference_hashes_are_checked_at_import_and_read(self):
        control = self.root / 'control.json'
        control.write_text('{"fixture_control":true}\n')
        self.record['evidence_refs']['control'] = str(control)
        signed = append_review_record(self.log, self.record, original_row=self.row)
        before = self.log.read_bytes()
        for key in ('source', 'raw', 'capsule', 'control'):
            path = pathlib.Path(self.record['evidence_refs'][key])
            saved = path.read_bytes()
            path.write_bytes(saved + b'changed\n')
            with self.subTest(reference=key):
                with self.assertRaisesRegex(ValueError, 'hash|digest'):
                    append_review_record(self.root / (key + '.jsonl'), signed,
                                         original_row=self.row)
                with self.assertRaisesRegex(ValueError, 'hash|digest'):
                    read_review_records(self.log)
            path.write_bytes(saved)
        self.assertEqual(self.log.read_bytes(), before)
        self.assertEqual(read_review_records(self.log), [signed])
        wrong_raw = self.root / 'different-raw.jsonl'
        wrong_raw.write_text('different execution\n')
        wrong = copy.deepcopy(self.record)
        wrong['evidence_refs']['raw'] = str(wrong_raw)
        with self.assertRaisesRegex(ValueError, 'raw|identity'):
            append_review_record(self.root / 'wrong-raw.jsonl', wrong, original_row=self.row)
        wrong = copy.deepcopy(self.record)
        wrong['evidence_refs']['capsule'] = str(self.source)
        with self.assertRaisesRegex(ValueError, 'capsule|identity'):
            append_review_record(self.root / 'wrong-capsule.jsonl', wrong, original_row=self.row)

    def test_revision_appends_reasoned_supersession_and_preserves_history(self):
        first = append_review_record(self.log, dict(self.record, record_id='review-initial'),
                                     original_row=self.row)
        before = self.log.read_bytes()
        changed = dict(self.record, record_id='review-revised', conclusion='rejected',
                       attribution='allowed', reason='The documented behavior permits this result.')
        for extra in ({}, {'supersedes_record_id': first['record_id']},
                      {'supersedes_record_id': 'missing', 'revision_reason': 'More evidence'},
                      {'supersedes_record_id': 'review-revised', 'revision_reason': 'Self reference'}):
            with self.subTest(extra=extra):
                with self.assertRaisesRegex(ValueError, 'revision|supersed'):
                    append_review_record(self.log, dict(changed, **extra), original_row=self.row)
                self.assertEqual(self.log.read_bytes(), before)
        second = append_review_record(self.log, dict(changed, supersedes_record_id=first['record_id'],
                                                     revision_reason='Read the referenced specification'),
                                      original_row=self.row)
        self.assertTrue(self.log.read_bytes().startswith(before))
        self.assertEqual(read_review_records(self.log), [first, second])
        self.assertEqual(append_review_record(self.log, first, original_row=self.row), first)
        for invalid in (
                dict(second, record_id='fork', record_sha256=None),
                dict(self.record, record_id='review-initial', conclusion='supported')):
            invalid.pop('record_sha256', None)
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, 'conflict|supersed'):
                    append_review_record(self.log, invalid, original_row=self.row)
        other = dict(self.record, candidate_id='audit-run:other', record_id='cross-case',
                     supersedes_record_id=first['record_id'], revision_reason='Cross case')
        with self.assertRaisesRegex(ValueError, 'identity|candidate|supersed'):
            append_review_record(self.log, other, original_row=dict(self.row, cell_id='other'))

    def test_report_import_rejects_unbound_or_tampered_reviews_without_confirmation(self):
        import test_fuzz_review
        fixture = test_fuzz_review.ReviewPacketContracts()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        row = fixture.row()
        from test_fuzz_audit import build_report
        original_report, _ = build_report(fixture.root, [row])
        original_bytes = original_report.read_bytes()
        raw_before = pathlib.Path(row['raw_path']).read_bytes()
        review = dict(self.record, candidate_id='audit-run:candidate-1',
                      conclusion='supported', attribution='sut_candidate',
                      evidence_refs=fixture.refs())
        signed = append_review_record(fixture.root / 'reviews.jsonl', review, original_row=row)
        accepted = exploratory_candidate_ledger([row], reviews=[signed, signed])
        self.assertEqual(accepted['review_errors'], [])
        self.assertEqual(accepted['review_records'], [signed])
        self.assertEqual(accepted['candidates'][0]['review_status'], 'supported_awaiting_adjudication')
        self.assertEqual(accepted['confirmed_candidates'], 0)
        for bad in (review, dict(signed, reason='Changed after signing'),
                    dict(signed, schema_version='unknown')):
            with self.subTest(bad=bad):
                rejected = exploratory_candidate_ledger([row], reviews=[bad])
                self.assertTrue(rejected['review_errors'])
                self.assertEqual(rejected['candidates'][0]['review_status'], 'not_reviewed')
                self.assertEqual(rejected['confirmed_candidates'], 0)
                self.assertEqual(rejected['natural_candidate_count'], accepted['natural_candidate_count'])
        revised = append_review_record(fixture.root / 'reviews.jsonl',
            dict(review, conclusion='rejected', attribution='allowed',
                 supersedes_record_id=signed['record_id'], revision_reason='More evidence'),
            original_row=row)
        ledger = exploratory_candidate_ledger([row], reviews=[signed, revised])
        self.assertEqual(ledger['review_records'], [signed, revised])
        self.assertEqual(ledger['candidates'][0]['review_status'], 'review_rejected_by_independent_agent')
        self.assertEqual(len(ledger['candidates'][0]['reviews']), 1)
        self.assertEqual(original_report.read_bytes(), original_bytes)
        self.assertEqual(pathlib.Path(row['raw_path']).read_bytes(), raw_before)

    def test_record_schema_rejects_missing_analysis_and_malformed_identity(self):
        for changes in (
                {'attribution': 'confirmed_defect'}, {'attribution': None},
                {'reason': ''}, {'recorded_at': 'not-a-time'},
                {'recorded_at': '2026-11-21T10:00:00'}, {'record_id': []},
                {'independent_of': [{'unhashable': True}]}, {'conclusion': []},
                {'candidate_id': ['audit-run:original']}):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    append_review_record(self.log, dict(self.record, **changes), original_row=self.row)
        for field in ('run_id', 'target_id', 'rule_id', 'effective_input_sha256', 'provenance'):
            missing = dict(self.row)
            missing.pop(field)
            with self.subTest(missing=field):
                with self.assertRaises(ValueError):
                    append_review_record(self.log, self.record, original_row=missing)

    def test_review_attribution_does_not_promote_unknown_tool_environment_allowed_or_controls(self):
        before = copy.deepcopy(self.row)
        for index, (attribution, changes) in enumerate((
                ('unknown', {}), ('sut_candidate', {}), ('tool', {'tool_error': True}),
                ('environment', {'execution_stage': 'aborted', 'abort_reason': 'domain_conflict'}),
                ('allowed', {'oracle_verdict': 'pass'}),
                ('sut_candidate', {'arm': 'negative'}))):
            with self.subTest(attribution=attribution):
                row = dict(self.row, **changes)
                review = dict(self.record, attribution=attribution, conclusion='supported',
                              confirmation={'adjudicator_id': 'agent-reviewer'})
                signed = append_review_record(self.root / (str(index) + '-' + attribution + '.jsonl'), review,
                                              original_row=row)
                ledger = exploratory_candidate_ledger([row], reviews=[signed])
                self.assertEqual(ledger['review_errors'], [])
                self.assertEqual(ledger['review_records'][0]['attribution'], attribution)
                self.assertEqual(ledger['natural_candidate_count'], 0)
                self.assertEqual(ledger['confirmed_candidates'], 0)
        self.assertEqual(self.row, before)

    def test_concurrent_duplicate_imports_append_once(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        gate = Barrier(8)

        def import_once(_):
            gate.wait(timeout=10)
            return append_review_record(self.log, self.record, original_row=self.row)

        with ThreadPoolExecutor(max_workers=8) as workers:
            imported = list(workers.map(import_once, range(8)))
        self.assertTrue(all(record == imported[0] for record in imported))
        self.assertEqual(len(self.log.read_text().splitlines()), 1)
        self.assertEqual(read_review_records(self.log), [imported[0]])

    def test_incomplete_log_tail_is_rejected_without_appending(self):
        append_review_record(self.log, self.record, original_row=self.row)
        self.log.write_bytes(self.log.read_bytes().rstrip(b'\n'))
        before = self.log.read_bytes()
        with self.assertRaisesRegex(ValueError, 'incomplete|line'):
            append_review_record(self.log, dict(self.record, reviewer_id='second-reviewer'),
                                 original_row=self.row)
        self.assertEqual(self.log.read_bytes(), before)
        with self.assertRaisesRegex(ValueError, 'incomplete|line'):
            read_review_records(self.log)


    def test_public_audit_uses_shared_review_import_validation(self):
        import test_fuzz_review
        from r2f.core.evidence import canonical_hash
        from r2f.reports.run_report import audit_report
        from test_fuzz_audit import build_report
        fixture = test_fuzz_review.ReviewPacketContracts()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        row = fixture.row()
        record = append_review_record(fixture.root / 'reviews.jsonl',
            dict(self.record, candidate_id='audit-run:candidate-1', conclusion='supported',
                 evidence_refs=fixture.refs()), original_row=row)
        wrong_identity = copy.deepcopy(record)
        wrong_identity['execution_identity']['source'] = {'source_commit': 'other-execution'}
        wrong_identity['record_sha256'] = canonical_hash({
            key: value for key, value in wrong_identity.items() if key != 'record_sha256'})
        conflicting = dict(record, reason='Conflicting content for the same record ID')
        conflicting['record_sha256'] = canonical_hash({
            key: value for key, value in conflicting.items() if key != 'record_sha256'})
        for reviews, accepted, rejection in (
                ([record], 1, None),
                ([dict(record, record_sha256='0' * 64)], 0, 'review_record_digest_mismatch'),
                ([wrong_identity], 0, 'review_execution_identity_mismatch'),
                ([record, record], 1, None),
                ([record, conflicting], 1, 'review_record_id_conflict')):
            with self.subTest(rejection=rejection, submitted=len(reviews)):
                report, _ = build_report(fixture.root, [row], reviews=reviews)
                before = report.read_bytes()
                raw_before = pathlib.Path(row['raw_path']).read_bytes()
                audit = audit_report(report)
                ledger = json.loads((fixture.root / 'candidate_ledger.json').read_text())
                self.assertTrue(audit['evidence_valid'], audit['errors'])
                self.assertEqual(audit['accepted_reviews'], accepted)
                self.assertEqual(audit['accepted_reviews'], len(ledger['review_records']))
                self.assertEqual([r['reason'] for r in audit['rejected_reviews']],
                                 [r['detail'] for r in ledger['review_errors']])
                if rejection is not None:
                    self.assertIn(rejection, audit['rejected_reviews'][0]['reason'])
                    self.assertEqual(audit['rejected_reviews'][0]['index'], len(reviews) - 1)
                self.assertEqual(ledger['confirmed_candidates'], 0)
                self.assertEqual(report.read_bytes(), before)
                self.assertEqual(pathlib.Path(row['raw_path']).read_bytes(), raw_before)
