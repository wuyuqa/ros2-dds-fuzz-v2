"""CP2 offline fixtures. Trusted host identity is simulated, never a real human."""
import copy
import json
import pathlib
import tempfile
import unittest

from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports import candidate_report as reports
import test_fuzz_review as review_fixtures


class HumanAdjudication(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        fixture = review_fixtures.ReviewPacketContracts(methodName='runTest')
        fixture.root = self.root
        self.row = fixture.row()
        self.reviews = fixture.signed_reviews(self.row, [{
            'candidate_id': 'audit-run:candidate-1', 'reviewer_id': 'independent-agent',
            'conclusion': 'supported', 'evidence_refs': fixture.refs(),
            'independent_of': ['trigger-author']}])
        self.log = self.root / 'human.jsonl'
        self.trusted = set()

    def authority(self, record):
        # Fixture of an OUT-OF-BAND host verifier. No verifier/authority is read
        # from the record or report; production must supply its authenticated UI.
        return record['record_sha256'] in self.trusted

    def record(self, *, row=None, record_id='human-1', action='confirm', **changes):
        row = self.row if row is None else row
        instruction = self.root / (record_id + '.txt')
        instruction.write_text('OFFLINE FIXTURE: explicit ' + action + ' of this candidate.\n')
        record = {
            'schema_version': 'r2f.s1-human-adjudication/v1', 'record_id': record_id,
            'candidate_id': f"{row['run_id']}:{row['cell_id']}",
            'human_id': 'fixture-human', 'origin': 'user_instruction', 'action': action,
            'reason': 'Offline contract fixture; no natural SUT finding or actual user confirmation.',
            'recorded_at': '2026-11-21T12:00:00+08:00',
            'instruction_ref': str(instruction), 'instruction_sha256': raw_sha256(instruction),
            'execution_identity': {
                **{key: row.get(key) for key in ('run_id', 'cell_id', 'target_id',
                    'target_revision', 'rule_id', 'rmw', 'effective_input_sha256',
                    'raw_path', 'raw_sha256')},
                'source': row.get('source_identity') or row.get('source_manifest') or row.get('provenance'),
                'execution_sha256': canonical_hash(row)},
        }
        record.update(changes)
        record['record_sha256'] = canonical_hash(record)
        self.trusted.add(record['record_sha256'])
        return record

    def ledger(self, records, **changes):
        args = dict(reviews=self.reviews, human_records=records, human_authority=self.authority)
        args.update(changes)
        return reports.exploratory_candidate_ledger([self.row], **args)

    def test_named_human_and_digest_cannot_authenticate_themselves(self):
        record = self.record()
        denied = self.ledger([record], human_authority=None)
        self.assertEqual(denied['human_confirmed_candidates'], 0)
        self.assertIn('human_authority_unverified', denied['human_errors'][0]['detail'])
        accepted = self.ledger([record])
        self.assertEqual(accepted['human_confirmed_candidates'], 1)
        candidate = accepted['candidates'][0]
        self.assertEqual(candidate['human_adjudication']['current']['action'], 'confirm')
        self.assertEqual(candidate['human_adjudication']['history'], [record])
        self.assertEqual(candidate['candidate_status'], 'confirmed')

    def test_revocation_and_revision_keep_reasoned_history_without_double_count(self):
        first = self.record()
        revoke = self.record(record_id='human-2', action='revoke',
                             supersedes_record_id='human-1', revision_reason='Withdraw fixture decision.')
        revised = self.record(record_id='human-3', supersedes_record_id='human-2',
                              revision_reason='Reconfirm with independent fixture evidence.')
        original = copy.deepcopy(self.row)
        withdrawn = self.ledger([first, first, revoke])
        self.assertEqual(withdrawn['human_confirmed_candidates'], 0)
        candidate = withdrawn['candidates'][0]
        self.assertEqual(candidate['candidate_status'], 'triaged')
        self.assertEqual(candidate['human_adjudication']['history'], [first, revoke])
        restored = self.ledger([first, revoke, revised])
        self.assertEqual(restored['human_confirmed_candidates'], 1)
        self.assertEqual(restored['candidates'][0]['human_adjudication']['history'], [first, revoke, revised])
        self.assertEqual(self.row, original)
        for invalid in (
                self.record(record_id='human-1', reason='Conflicting identity.'),
                self.record(record_id='human-4'),
                self.record(record_id='human-5', supersedes_record_id='human-1', revision_reason='Fork.'),
                self.record(record_id='human-6', supersedes_record_id='human-3'),
                self.record(record_id='human-7', supersedes_record_id='human-3', revision_reason=' ',
                            recorded_at='2026-11-20T12:00:00+08:00')):
            with self.subTest(record=invalid['record_id']):
                result = self.ledger([first, revoke, revised, invalid])
                self.assertTrue(result['human_errors'])
                self.assertEqual(result['human_records'], [])
                self.assertEqual(result['human_confirmed_candidates'], 0)
                self.assertEqual(result['candidates'][0]['human_adjudication'], {
                    'current': None, 'history': [first, revoke, revised, invalid]})

    def test_append_only_import_and_read_share_lifecycle_eligibility(self):
        first = self.record()
        options = dict(original_rows=[self.row], reviews=self.reviews, human_authority=self.authority)
        saved = reports.append_human_record(self.log, first, **options)
        before = self.log.read_bytes()
        self.assertEqual(reports.append_human_record(self.log, first, **options), saved)
        self.assertEqual(self.log.read_bytes(), before)
        revoke = self.record(record_id='human-2', action='revoke',
                             supersedes_record_id='human-1', revision_reason='Fixture withdrawal.')
        reports.append_human_record(self.log, revoke, **options)
        self.assertTrue(self.log.read_bytes().startswith(before))
        self.assertEqual(reports.read_human_records(self.log, **options), [first, revoke])
        for changes in ({'human_authority': None}, {'reviews': []},
                        {'original_rows': [dict(self.row, run_id='other-execution')]}):
            with self.subTest(changes=list(changes)):
                with self.assertRaises(ValueError):
                    reports.read_human_records(self.log, **dict(options, **changes))
                with self.assertRaises(ValueError):
                    reports.append_human_record(self.log, first, **dict(options, **changes))
        after = self.log.read_bytes()
        conflict = self.record(reason='Same ID but changed reason.')
        with self.assertRaises(ValueError):
            reports.append_human_record(self.log, conflict, **options)
        self.assertEqual(self.log.read_bytes(), after)

    def test_public_audit_rebuilds_original_analysis_and_human_layers(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        original_path, original_report = build_report(self.root, [self.row], reviews=self.reviews)
        original_bytes = original_path.read_bytes()
        raw_before = pathlib.Path(self.row['raw_path']).read_bytes()
        first = self.record()
        ledger = self.ledger([first])
        candidate = ledger['candidates'][0]
        self.assertEqual(candidate['original_result']['oracle_verdict'], 'fail')
        self.assertEqual(candidate['analysis']['current'], self.reviews[0])
        ledger_path = self.root / 'human-ledger.json'
        report_path = self.root / 'human-report.json'
        run_report.write_json(ledger_path, ledger)
        derived = dict(original_report, human_records=[first],
                       candidate_ledger_path=ledger_path.name,
                       candidate_ledger_sha256=canonical_hash(ledger))
        derived['report_sha256'] = run_report.exploratory_report_digest(derived)
        run_report.write_json(report_path, derived)
        for audit_api in (run_report.audit_report, run_report.audit_exploratory_report):
            with self.subTest(api=audit_api.__name__):
                audit = audit_api(report_path, human_authority=self.authority)
                self.assertTrue(audit['evidence_valid'], audit['errors'])
                self.assertEqual(audit['human_confirmed_candidates'], 1)
                self.assertEqual(audit['accepted_human_records'], 1)
                self.assertFalse(audit['sealed'])
                denied = audit_api(report_path)
                self.assertFalse(denied['evidence_valid'])
                self.assertEqual(denied['human_confirmed_candidates'], 0)
        # Rehashing a forged current/history/count cannot replace a human record.
        for change in ({'human_records': []}, {'human_confirmed_candidates': 3},
                       {'human_errors': [{'detail': 'invented rejection'}]}):
            forged = dict(ledger, **change)
            run_report.write_json(ledger_path, forged)
            derived['candidate_ledger_sha256'] = canonical_hash(forged)
            derived['report_sha256'] = run_report.exploratory_report_digest(derived)
            run_report.write_json(report_path, derived)
            result = run_report.audit_report(report_path, human_authority=self.authority)
            self.assertFalse(result['evidence_valid'])
        self.assertEqual(original_path.read_bytes(), original_bytes)
        self.assertEqual(pathlib.Path(self.row['raw_path']).read_bytes(), raw_before)

    def test_explicit_qualification_denial_cannot_be_overridden_at_any_seam(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.core.oracle import TriageResult
        denied_row = dict(self.row, candidate_eligible=False)
        review = reports.append_review_record(self.root / 'denied-review.jsonl', {
            key: self.reviews[0][key] for key in ('candidate_id', 'reviewer_id', 'conclusion',
                                                'attribution', 'reason', 'evidence_refs', 'independent_of')},
            original_row=denied_row)
        record = self.record(row=denied_row)
        core = CandidateLifecycle(record['candidate_id'], denied_row).admit().triage(
            TriageResult('candidate', 'sut', 'sut_contract_violation'))
        core.record_review(**{key: review[key] for key in (
            'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of')})
        with self.assertRaises(ValueError):
            core.record_human_adjudication(record, original_row=denied_row,
                                            human_authority=self.authority)
        result = reports.exploratory_candidate_ledger([denied_row], reviews=[review],
            human_records=[record], human_authority=self.authority)
        self.assertEqual(result['human_confirmed_candidates'], 0)
        self.assertTrue(result['human_errors'])
        with self.assertRaises(ValueError):
            reports.append_human_record(self.log, record, original_rows=[denied_row],
                                        reviews=[review], human_authority=self.authority)

    def test_agent_tool_or_unknown_attribution_is_not_supporting_sut_review(self):
        record = self.record()
        for attribution in ('tool', 'environment', 'allowed', 'unknown'):
            review = reports.append_review_record(self.root / (attribution + '-review.jsonl'), {
                **{key: self.reviews[0][key] for key in (
                    'candidate_id', 'reviewer_id', 'conclusion', 'reason', 'evidence_refs', 'independent_of')},
                'attribution': attribution}, original_row=self.row)
            with self.subTest(attribution=attribution):
                result = self.ledger([record], reviews=[review])
                self.assertEqual(result['human_confirmed_candidates'], 0)
                self.assertTrue(result['human_errors'])
                candidate = result['candidates'][0]
                self.assertEqual(candidate['original_result']['responsibility_layer'], 'sut')
                self.assertEqual(candidate['analysis']['current']['attribution'], attribution)
                with self.assertRaises(ValueError):
                    reports.append_human_record(self.log, record, original_rows=[self.row],
                                                reviews=[review], human_authority=self.authority)

    def test_public_lifecycle_validates_human_confirmation_and_preserves_replay_obligation(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.core.oracle import TriageResult
        record = self.record(origin='human_operation')
        candidate = CandidateLifecycle(record['candidate_id'], self.row).admit().triage(
            TriageResult('candidate', 'sut', 'sut_contract_violation'))
        candidate.record_review(**{key: self.reviews[0][key] for key in (
            'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of', 'attribution')})
        candidate.record_human_adjudication(record, human_authority=self.authority)
        self.assertTrue(candidate.validate(require_review=True))
        with self.assertRaisesRegex(ValueError, 'replay attempt|non_replayable'):
            candidate.validate(require_disposition=True, require_review=True)
        candidate.mark_non_replayable('fixture_only', 'Offline fixture cannot replay a real SUT.')
        self.assertTrue(candidate.validate(require_disposition=True, require_review=True))
        before = candidate.to_dict()
        candidate.record_human_adjudication(record, human_authority=self.authority)
        self.assertEqual(candidate.to_dict(), before)
        revoke = self.record(record_id='human-2', action='revoke', origin='human_operation',
                             supersedes_record_id='human-1', revision_reason='Fixture withdrawal.')
        candidate.record_human_adjudication(revoke, human_authority=self.authority)
        self.assertTrue(candidate.validate(require_disposition=True, require_review=True))
        self.assertEqual(candidate.to_dict()['candidate_status'], 'triaged')

    def test_bad_records_are_rejected_consistently_by_import_ledger_and_audit(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        first = self.record()
        _, base_report = build_report(self.root, [self.row], reviews=self.reviews)
        invalids = [None, [], 'fixture-human']
        for changes in (
                {'schema_version': 'unknown'}, {'record_sha256': '0' * 64},
                {'human_id': ' '}, {'reason': ''}, {'record_id': ''},
                {'recorded_at': '2026-11-21T00:00:00'}, {'action': 'approve'},
                {'origin': 'agent_review'}, {'candidate_id': 'audit-run:other'},
                {'execution_identity': dict(first['execution_identity'], target_revision=4)},
                {'instruction_ref': str(self.root / 'missing-instruction')},
                {'instruction_sha256': '0' * 64}):
            bad = dict(first, **changes)
            if 'record_sha256' not in changes:
                bad['record_sha256'] = canonical_hash({k: v for k, v in bad.items() if k != 'record_sha256'})
            self.trusted.add(bad['record_sha256'])
            invalids.append(bad)
        for index, bad in enumerate(invalids):
            with self.subTest(index=index):
                ledger = self.ledger([bad])
                self.assertEqual(ledger['human_confirmed_candidates'], 0)
                self.assertEqual(ledger['human_records'], [])
                self.assertTrue(ledger['human_errors'])
                with self.assertRaises(ValueError):
                    reports.append_human_record(self.log, bad, original_rows=[self.row],
                                                reviews=self.reviews, human_authority=self.authority)
                raw_log = self.root / 'malformed-human.jsonl'
                raw_log.write_text(json.dumps(bad) + '\n')
                with self.assertRaises(ValueError):
                    reports.read_human_records(raw_log, original_rows=[self.row],
                                              reviews=self.reviews, human_authority=self.authority)
                ledger_path = self.root / 'rejected-ledger.json'
                run_report.write_json(ledger_path, ledger)
                derived = dict(base_report, human_records=[bad], candidate_ledger_path=ledger_path.name,
                               candidate_ledger_sha256=canonical_hash(ledger))
                derived['report_sha256'] = run_report.exploratory_report_digest(derived)
                path = self.root / 'rejected-report.json'
                run_report.write_json(path, derived)
                before = path.read_bytes()
                result = run_report.audit_report(path, human_authority=self.authority)
                self.assertTrue(result['evidence_valid'], result['errors'])
                self.assertEqual(result['human_confirmed_candidates'], 0)
                self.assertEqual(result['rejected_human_records'], ledger['human_errors'])
                self.assertEqual(path.read_bytes(), before)

    def test_legacy_methods_remain_separate_from_human_confirmation_and_revocation(self):
        for method in ('single_agent', 'peer_review', 'rule_engine'):
            row = dict(self.row, confirmation={'adjudicator_id': 'legacy-agent',
                'adjudication_method': method, 'confirmation_basis': 'fixture packet',
                'reason': 'Legacy confirmation is not an authenticated human operation.'})
            review = reports.append_review_record(self.root / (method + '.jsonl'), {
                key: self.reviews[0][key] for key in ('candidate_id', 'reviewer_id', 'conclusion',
                    'attribution', 'reason', 'evidence_refs', 'independent_of')}, original_row=row)
            with self.subTest(method=method):
                legacy = reports.exploratory_candidate_ledger([row], reviews=[review])
                self.assertEqual(legacy['confirmed_candidates'], 1)
                self.assertEqual(legacy['human_confirmed_candidates'], 0)
                first = self.record(row=row, record_id=method + '-1')
                revoke = self.record(row=row, record_id=method + '-2', action='revoke',
                    supersedes_record_id=first['record_id'], revision_reason='Withdraw human fixture only.')
                ledger = reports.exploratory_candidate_ledger([row], reviews=[review],
                    human_records=[first, revoke], human_authority=self.authority)
                self.assertFalse(ledger['human_errors'])
                self.assertEqual(ledger['human_confirmed_candidates'], 0)
                self.assertEqual(ledger['confirmed_candidates'], 1)
                self.assertEqual(ledger['candidates'][0]['adjudication_method'], method)

    def test_unknown_and_control_observations_keep_separate_analysis_and_human_layers(self):
        for changes, section in (({'oracle_verdict': 'unknown'}, 'failures'),
                                 ({'control_kind': 'health'}, 'controls')):
            row = dict(self.row, **changes)
            review = reports.append_review_record(self.root / (section + '-review.jsonl'), {
                **{key: self.reviews[0][key] for key in (
                    'candidate_id', 'reviewer_id', 'reason', 'evidence_refs', 'independent_of')},
                'attribution': 'unknown', 'conclusion': 'needs_evidence'}, original_row=row)
            record = self.record(row=row, record_id=section + '-human')
            with self.subTest(section=section):
                ledger = reports.exploratory_candidate_ledger([row], reviews=[review],
                    human_records=[record], human_authority=self.authority)
                entry = ledger[section][0]
                self.assertEqual(entry['original_result']['oracle_verdict'], row['oracle_verdict'])
                self.assertEqual(entry['analysis']['current'], review)
                self.assertIsNone(entry['human_adjudication']['current'])
                self.assertEqual(ledger['human_confirmed_candidates'], 0)
                self.assertEqual(ledger['natural_candidate_count'], 0)
                self.assertTrue(ledger['human_errors'])

    def test_schema_downgrade_cannot_hide_a_forged_human_current(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        path, report = build_report(self.root, [self.row], reviews=self.reviews)
        ledger_path = self.root / 'candidate_ledger.json'
        ledger = json.loads(ledger_path.read_text())
        del ledger['human_adjudication_schema']
        ledger['candidates'][0]['human_adjudication'] = {'current': self.record(), 'history': []}
        run_report.write_json(ledger_path, ledger)
        report['candidate_ledger_sha256'] = canonical_hash(ledger)
        report['report_sha256'] = run_report.exploratory_report_digest(report)
        run_report.write_json(path, report)
        audit = run_report.audit_report(path, human_authority=self.authority)
        self.assertFalse(audit['evidence_valid'], audit['errors'])
        self.assertEqual(audit['human_confirmed_candidates'], 0)

    def test_public_lifecycle_rechecks_supporting_review_artifact_hashes(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.core.oracle import TriageResult
        record = self.record()
        core = CandidateLifecycle(record['candidate_id'], self.row).admit().triage(
            TriageResult('candidate', 'sut', 'sut_contract_violation'))
        core.record_review(**{key: self.reviews[0][key] for key in (
            'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of', 'attribution')})
        source = pathlib.Path(self.reviews[0]['evidence_refs']['source'])
        source.write_text('Changed after review intake.\n')
        with self.assertRaises(ValueError):
            core.record_human_adjudication(record, human_authority=self.authority)
        self.assertIsNone(core.to_dict()['human_adjudication']['current'])

    def test_declared_denials_controls_and_unhealthy_inputs_cannot_gain_human_status(self):
        cases = [dict(candidate_eligible=value) for value in (False, None, 0, 'false')]
        cases += [dict(candidate_source='control'), dict(actual_run=False), dict(observer_complete=False),
                  dict(tool_error=True), dict(observer_dropped=True),
                  dict(provenance={}, source_identity={'commit': 'fixture-binding-only'}),
                  dict(triage_disposition='deferred', responsibility_layer='observation',
                       reason_code='attribution_insufficient')]
        for index, changes in enumerate(cases):
            row = dict(self.row, **changes)
            review = reports.append_review_record(self.root / f'denial-{index}-review.jsonl', {
                key: self.reviews[0][key] for key in ('candidate_id', 'reviewer_id', 'conclusion',
                    'attribution', 'reason', 'evidence_refs', 'independent_of')}, original_row=row)
            record = self.record(row=row, record_id=f'denial-{index}-human')
            with self.subTest(changes=changes):
                ledger = reports.exploratory_candidate_ledger([row], reviews=[review],
                    human_records=[record], human_authority=self.authority)
                self.assertEqual(ledger['human_confirmed_candidates'], 0)
                self.assertTrue(ledger['human_errors'])
                with self.assertRaises(ValueError):
                    reports.append_human_record(self.log, record, original_rows=[row],
                                                reviews=[review], human_authority=self.authority)

    def test_cp1_report_shape_still_audits_without_granting_human_status(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        path, report = build_report(self.root, [self.row], reviews=self.reviews)
        ledger_path = self.root / 'candidate_ledger.json'
        legacy = json.loads(ledger_path.read_text())
        # CP1's documented serialized fields predate the CP2 projection marker.
        for key in ('human_adjudication_schema', 'human_records', 'human_errors',
                    'human_confirmed_candidates'):
            del legacy[key]
        for entry in legacy['candidates']:
            for key in ('original_result', 'analysis', 'human_adjudication'):
                del entry[key]
            for review in entry['reviews']:
                del review['attribution']
                del review['evidence_sha256']
        run_report.write_json(ledger_path, legacy)
        report['candidate_ledger_sha256'] = canonical_hash(legacy)
        report['report_sha256'] = run_report.exploratory_report_digest(report)
        run_report.write_json(path, report)
        original = path.read_bytes()
        audit = run_report.audit_report(path)
        self.assertTrue(audit['evidence_valid'], audit['errors'])
        self.assertEqual(audit['human_confirmed_candidates'], 0)
        self.assertEqual(path.read_bytes(), original)

    def test_concurrent_idempotent_import_keeps_one_complete_record(self):
        from concurrent.futures import ThreadPoolExecutor
        record = self.record()
        options = dict(original_rows=[self.row], reviews=self.reviews, human_authority=self.authority)
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: reports.append_human_record(self.log, record, **options), range(6)))
        self.assertEqual(results, [record] * 6)
        self.assertEqual(reports.read_human_records(self.log, **options), [record])
        complete = self.log.read_bytes()
        self.log.write_bytes(complete + b'{"incomplete":')
        damaged = self.log.read_bytes()
        with self.assertRaisesRegex(ValueError, 'incomplete final line'):
            reports.append_human_record(self.log, record, **options)
        with self.assertRaisesRegex(ValueError, 'incomplete final line'):
            reports.read_human_records(self.log, **options)
        self.assertEqual(self.log.read_bytes(), damaged)

    def test_core_revalidates_all_human_history_even_after_revocation(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.core.oracle import TriageResult
        first = self.record()
        revoke = self.record(record_id='human-2', action='revoke',
                             supersedes_record_id='human-1', revision_reason='Withdraw.')
        latest = self.record(record_id='human-3', supersedes_record_id='human-2',
                             revision_reason='Reconfirm.')
        for records in ([first, revoke], [first, revoke, latest]):
            for invalidation in ('authority', 'instruction', 'execution'):
                with self.subTest(current=records[-1]['action'], invalidation=invalidation):
                    core = CandidateLifecycle(first['candidate_id'], self.row).admit().triage(
                        TriageResult('candidate', 'sut', 'sut_contract_violation'))
                    core.record_review(**{key: self.reviews[0][key] for key in (
                        'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of', 'attribution')})
                    for record in records:
                        core.record_human_adjudication(record, human_authority=self.authority)
                    self.assertTrue(core.validate(require_review=True))
                    instruction = pathlib.Path(first['instruction_ref'])
                    before = instruction.read_bytes()
                    original_input = core.evidence['effective_input_sha256']
                    if invalidation == 'authority':
                        self.trusted.remove(first['record_sha256'])
                    elif invalidation == 'instruction':
                        instruction.write_text('Changed historical instruction.\n')
                    else:
                        core.evidence['effective_input_sha256'] = 'f' * 64
                    try:
                        with self.assertRaises(ValueError):
                            core.validate(require_review=True)
                        with self.assertRaises(ValueError):
                            core.record_human_adjudication(records[-1], human_authority=self.authority)
                    finally:
                        self.trusted.add(first['record_sha256'])
                        instruction.write_bytes(before)
                        core.evidence['effective_input_sha256'] = original_input
                    self.assertTrue(core.validate(require_review=True))

    def test_revoke_after_supporting_review_retraction_preserves_readable_audited_history(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.core.oracle import TriageResult
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        first = self.record()
        options = dict(original_rows=[self.row], reviews=self.reviews, human_authority=self.authority)
        reports.append_human_record(self.log, first, **options)
        before_log = self.log.read_bytes()
        core = CandidateLifecycle(first['candidate_id'], self.row).admit().triage(
            TriageResult('candidate', 'sut', 'sut_contract_violation'))
        core.record_review(**{key: self.reviews[0][key] for key in (
            'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of', 'attribution')})
        core.record_human_adjudication(first, human_authority=self.authority)
        revised = reports.append_review_record(self.root / 'reviews.jsonl', {
            **{key: self.reviews[0][key] for key in (
                'candidate_id', 'reviewer_id', 'reason', 'evidence_refs', 'independent_of')},
            'record_id': 'review-retraction', 'conclusion': 'rejected', 'attribution': 'tool',
            'supersedes_record_id': self.reviews[0]['record_id'],
            'revision_reason': 'Independent analysis now attributes the failure to the harness.'},
            original_row=self.row)
        reviews = [*self.reviews, revised]
        options['reviews'] = reviews
        core.record_review(**{key: revised[key] for key in (
            'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of', 'attribution')})
        with self.assertRaises(ValueError):
            core.validate(require_review=True)
        stale = self.ledger([first], reviews=reviews)
        self.assertEqual(stale['human_confirmed_candidates'], 0)
        self.assertEqual(stale['candidates'][0]['candidate_status'], 'triaged')
        self.assertEqual(stale['candidates'][0]['human_qualification_error'],
                         'human_confirmation_requires_independent_supporting_review')
        self.assertEqual(stale['candidates'][0]['human_adjudication']['history'], [first])
        self.assertEqual(reports.read_human_records(self.log, **options), [first])
        revoke = self.record(record_id='human-2', action='revoke',
                             supersedes_record_id='human-1', revision_reason='Withdraw after tool attribution.')
        core.record_human_adjudication(revoke, human_authority=self.authority)
        self.assertTrue(core.validate(require_review=True))
        self.assertEqual(core.to_dict()['human_adjudication']['history'], [first, revoke])
        reports.append_human_record(self.log, revoke, **options)
        self.assertTrue(self.log.read_bytes().startswith(before_log))
        self.assertEqual(reports.read_human_records(self.log, **options), [first, revoke])
        withdrawn = self.ledger([first, revoke], reviews=reviews)
        self.assertEqual(withdrawn['human_confirmed_candidates'], 0)
        self.assertEqual(withdrawn['candidates'][0]['candidate_status'], 'triaged')
        self.assertNotIn('human_qualification_error', withdrawn['candidates'][0])
        self.assertEqual(withdrawn['candidates'][0]['human_adjudication']['history'], [first, revoke])
        new_confirm = self.record(record_id='human-3', supersedes_record_id='human-2',
                                  revision_reason='This request still lacks supporting SUT analysis.')
        with self.assertRaises(ValueError):
            reports.append_human_record(self.log, new_confirm, **options)
        with self.assertRaises(ValueError):
            core.record_human_adjudication(new_confirm, human_authority=self.authority)
        self.assertEqual(reports.read_human_records(self.log, **options), [first, revoke])
        original_path, report = build_report(self.root, [self.row], reviews=reviews)
        original_bytes = original_path.read_bytes()
        for label, records, ledger in [('stale', [first], stale), ('revoked', [first, revoke], withdrawn)]:
            ledger_path = self.root / (label + '-ledger.json')
            path = self.root / (label + '-report.json')
            run_report.write_json(ledger_path, ledger)
            derived = dict(report, human_records=records, candidate_ledger_path=ledger_path.name,
                           candidate_ledger_sha256=canonical_hash(ledger))
            derived['report_sha256'] = run_report.exploratory_report_digest(derived)
            run_report.write_json(path, derived)
            for audit in (run_report.audit_report, run_report.audit_exploratory_report):
                result = audit(path, human_authority=self.authority)
                self.assertTrue(result['evidence_valid'], result['errors'])
                self.assertEqual(result['human_confirmed_candidates'], 0)
        self.assertEqual(original_path.read_bytes(), original_bytes)

    def test_invalid_middle_history_cannot_resurrect_a_confirmed_prefix(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.core.oracle import TriageResult

        for length in (3, 4):
            for failure in ('authority', 'instruction', 'binding'):
                with self.subTest(length=length, failure=failure):
                    records = []
                    core = CandidateLifecycle('audit-run:candidate-1', self.row).admit().triage(
                        TriageResult('candidate', 'sut', 'sut_contract_violation'))
                    core.record_review(**{key: self.reviews[0][key] for key in (
                        'reviewer_id', 'conclusion', 'evidence_refs', 'independent_of', 'attribution')})
                    options = dict(original_rows=[self.row], reviews=self.reviews,
                                   human_authority=self.authority)
                    log = self.root / f'{length}-{failure}.jsonl'
                    for index in range(length):
                        revision = ({'supersedes_record_id': records[-1]['record_id'],
                                     'revision_reason': 'Explicit fixture decision revision.'} if records else {})
                        record = self.record(record_id=f'{length}-{failure}-{index}',
                                             action='confirm' if index % 2 == 0 else 'revoke', **revision)
                        records.append(record)
                        reports.append_human_record(log, record, **options)
                        core.record_human_adjudication(record, human_authority=self.authority)
                    before_log = log.read_bytes()
                    broken = copy.deepcopy(records)
                    instruction = pathlib.Path(records[1]['instruction_ref'])
                    original_instruction = instruction.read_bytes()
                    if failure == 'authority':
                        self.trusted.remove(records[1]['record_sha256'])
                    elif failure == 'instruction':
                        instruction.write_bytes(original_instruction + b'Altered intermediate instruction.\n')
                    else:
                        broken[1]['execution_identity']['rule_id'] = 'different.rule'
                        broken[1]['record_sha256'] = canonical_hash({
                            k: v for k, v in broken[1].items() if k != 'record_sha256'})
                        self.trusted.add(broken[1]['record_sha256'])
                    # Import malformed records separately; never rewrite the append-only log.
                    imported = self.root / f'{length}-{failure}-import.jsonl'
                    imported.write_text(''.join(json.dumps(r) + '\n' for r in broken))
                    imported_bytes = imported.read_bytes()
                    with self.assertRaises(ValueError):
                        reports.read_human_records(imported, **options)
                    ledger = self.ledger(broken)
                    self.assertEqual(ledger['human_confirmed_candidates'], 0)
                    self.assertEqual(ledger['confirmed_candidates'], 0)
                    self.assertEqual(ledger['human_records'], [])
                    entry = ledger['candidates'][0]
                    self.assertEqual(entry['candidate_status'], 'triaged')
                    self.assertEqual(entry['human_adjudication'], {'current': None, 'history': broken})
                    self.assertEqual(entry['human_qualification_error'], 'human_history_invalid')
                    self.assertTrue(ledger['human_errors'])
                    self.assertEqual(imported.read_bytes(), imported_bytes)
                    self.assertEqual(log.read_bytes(), before_log)
                    if failure != 'binding':
                        with self.assertRaises(ValueError):
                            core.validate(require_review=True)
                    self.trusted.add(records[1]['record_sha256'])
                    instruction.write_bytes(original_instruction)
                    self.assertTrue(core.validate(require_review=True))
                    self.assertEqual(reports.read_human_records(log, **options), records)
                    restored = self.ledger(records)
                    self.assertFalse(restored['human_errors'])
                    self.assertEqual(restored['human_confirmed_candidates'], 1 if length == 3 else 0)
                    self.assertEqual(restored['candidates'][0]['human_adjudication'], {
                        'current': records[-1], 'history': records})

    def test_invalid_chain_is_scoped_and_both_audits_reject_a_rehashed_confirmed_prefix(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report

        records = []
        for index in range(4):
            revision = ({'supersedes_record_id': records[-1]['record_id'],
                         'revision_reason': 'Explicit fixture decision revision.'} if records else {})
            records.append(self.record(record_id=f'affected-{index}',
                                       action='confirm' if index % 2 == 0 else 'revoke', **revision))
        # A separate cell has its own original execution, raw and supporting review.
        raw = self.root / 'independent.jsonl'
        raw.write_text('{"event":"independent-fixture"}\n')
        other = dict(self.row, cell_id='independent-cell', raw_path=str(raw), raw_sha256=raw_sha256(raw))
        review = reports.append_review_record(self.root / 'independent-reviews.jsonl', {
            **{k: self.reviews[0][k] for k in (
                'reviewer_id', 'reason', 'conclusion', 'attribution', 'independent_of')},
            'record_id': 'independent-review', 'candidate_id': 'audit-run:independent-cell',
            'evidence_refs': dict(self.reviews[0]['evidence_refs'], raw=str(raw))}, original_row=other)
        independent = self.record(row=other, record_id='independent-human')
        reviews = [*self.reviews, review]
        original_path, original = build_report(self.root, [self.row, other], reviews=reviews)
        original_bytes = original_path.read_bytes()
        self.trusted.remove(records[1]['record_sha256'])
        for order, rows in enumerate(([self.row, other], [other, self.row])):
            supplied = [*records, independent]
            ledger = reports.exploratory_candidate_ledger(
                rows, reviews=reviews, human_records=supplied, human_authority=self.authority)
            self.assertEqual(ledger['human_confirmed_candidates'], 1)
            self.assertEqual(ledger['confirmed_candidates'], 1)
            self.assertEqual(ledger['human_records'], [independent])
            entries = {entry['candidate_id']: entry for entry in ledger['candidates']}
            self.assertIsNone(entries['audit-run:candidate-1']['human_adjudication']['current'])
            self.assertEqual(entries['audit-run:candidate-1']['human_adjudication']['history'], records)
            self.assertEqual(entries['audit-run:independent-cell']['human_adjudication'], {
                'current': independent, 'history': [independent]})
            forged = copy.deepcopy(ledger)
            affected = next(e for e in forged['candidates'] if e['candidate_id'] == 'audit-run:candidate-1')
            affected['human_adjudication'] = {'current': records[0], 'history': records[:1]}
            affected['candidate_status'] = 'confirmed'
            affected.pop('human_qualification_error')
            forged['human_records'] = [records[0], independent]
            forged['human_confirmed_candidates'] = forged['confirmed_candidates'] = 2
            for label, value, valid in [('faithful', ledger, True), ('prefix', forged, False)]:
                ledger_path = self.root / f'{order}-{label}-ledger.json'
                path = self.root / f'{order}-{label}-report.json'
                run_report.write_json(ledger_path, value)
                derived = dict(original, rows=rows, human_records=supplied,
                               candidate_ledger_path=ledger_path.name, candidate_ledger_sha256=canonical_hash(value))
                derived['report_sha256'] = run_report.exploratory_report_digest(derived)
                run_report.write_json(path, derived)
                for audit in (run_report.audit_report, run_report.audit_exploratory_report):
                    result = audit(path, human_authority=self.authority)
                    self.assertEqual(result['evidence_valid'], valid, result['errors'])
                    self.assertEqual(result['human_confirmed_candidates'], 1)
                    self.assertFalse(result['sealed'])
        self.assertEqual(original_path.read_bytes(), original_bytes)
