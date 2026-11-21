"""CP3 public-boundary contracts. All evidence and human authority are fixtures."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from r2f.core.candidate import CandidateLifecycle, candidate_execution_identity
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports import candidate_report as reports
import test_fuzz_review as fixtures


class RootCauseAccounting(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='r2f-cp3-', dir='/dev/shm')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        fixture = fixtures.ReviewPacketContracts('runTest')
        fixture.root = self.root
        self.first = fixture.row()
        self.refs = fixture.refs()
        raw = self.root / 'second.jsonl'
        raw.write_text('{"event":"second-offline-fixture"}\n')
        self.second = dict(self.first, cell_id='second', rmw='rmw_fastrtps_cpp',
                           raw_path=str(raw), raw_sha256=raw_sha256(raw),
                           provenance={'source_commit': 'other-fixture-version'})
        self.rows = [self.first, self.second]
        self.reviews = []
        self.humans = []
        self.trusted = set()

    def review(self, row, record_id, *, cause='cause-A', novelty='new', **changes):
        value = dict(record_id=record_id, candidate_id=f"{row['run_id']}:{row['cell_id']}",
                     reviewer_id='analysis-agent', attribution='sut_candidate', conclusion='supported',
                     independent_of=['trigger-author'], reason='Offline evidence analysis.',
                     evidence_refs=dict(self.refs, raw=row['raw_path']))
        value['root_cause'] = {'id': cause, 'novelty': novelty, 'operation': 'assign',
                               'reason': 'Fixture root-cause evidence.', 'evidence_keys': ['raw', 'source'],
                               'related_record_ids': []}
        value.update(changes)
        record = reports.append_review_record(self.root / 'reviews.jsonl', value, original_row=row)
        self.reviews.append(record)
        return record

    def human(self, row, record_id, *, action='confirm', **changes):
        instruction = self.root / (record_id + '.txt')
        instruction.write_text('OFFLINE simulated human instruction: ' + action)
        value = dict(schema_version='r2f.s1-human-adjudication/v1', record_id=record_id,
                     candidate_id=f"{row['run_id']}:{row['cell_id']}", human_id='fixture-human',
                     origin='user_instruction', action=action, reason='Fixture decision only.',
                     recorded_at='2026-11-22T00:00:00+08:00', instruction_ref=str(instruction),
                     instruction_sha256=raw_sha256(instruction), execution_identity=candidate_execution_identity(row))
        value.update(changes)
        value['record_sha256'] = canonical_hash(value)
        self.trusted.add(value['record_sha256'])
        self.humans.append(value)
        return value

    def ledger(self, **changes):
        options = dict(reviews=self.reviews, human_records=self.humans,
                       human_authority=lambda record: record['record_sha256'] in self.trusted)
        options.update(changes)
        return reports.exploratory_candidate_ledger(self.rows, **options)

    def assert_audited_accounting(self, ledger, *, reviews, label):
        """Exercise both public auditors, including a rehashed credit forgery."""
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        original_path, base = build_report(self.root, self.rows, reviews=[])
        original_bytes = original_path.read_bytes()
        raw_bytes = {row['raw_path']: Path(row['raw_path']).read_bytes() for row in self.rows}
        faithful = self.derived_report(base, ledger, label, reviews=reviews)
        forged = copy.deepcopy(ledger)
        forged['root_cause_accounting']['independent_new_root_causes'] += 1
        tampered = self.derived_report(base, forged, label + '-forged', reviews=reviews)
        authority = lambda record: record['record_sha256'] in self.trusted
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            result = audit(faithful, human_authority=authority)
            self.assertTrue(result['evidence_valid'], result['errors'])
            self.assertEqual(result['root_cause_accounting'], ledger['root_cause_accounting'])
            self.assertFalse(result['sealed'])
            rejected = audit(tampered, human_authority=authority)
            self.assertFalse(rejected['evidence_valid'], rejected['errors'])
        self.assertEqual(original_path.read_bytes(), original_bytes)
        self.assertEqual({path: Path(path).read_bytes() for path in raw_bytes}, raw_bytes)

    def test_parallel_supported_root_and_novelty_disagreements_never_gain_credit(self):
        a = self.review(self.first, 'a', reviewer_id='analyst-a')
        self.review(self.second, 'b')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        for label, changes in [('root', {'cause': 'cause-C'}),
                                ('novelty', {'novelty': 'historical'})]:
            # Separate append logs let each scenario enter via the public importer.
            (self.root / 'reviews.jsonl').write_text(json.dumps(a) + '\n')
            other = self.review(self.first, label, reviewer_id='analyst-b', **changes)
            b = next(r for r in self.reviews if r['record_id'] == 'b')
            for order, supplied in enumerate(([a, other, b], [other, a, b])):
                with self.subTest(disagreement=label, order=order):
                    ledger = self.ledger(reviews=supplied)
                    accounting = ledger['root_cause_accounting']
                    self.assertEqual(ledger['human_confirmed_candidates'], 2)
                    self.assertEqual(accounting['human_confirmed_cases'], 2)
                    self.assertEqual(accounting['independent_new_root_causes'], 1 if label == 'root' else 0)
                    case = next(c for c in accounting['cases'] if c['candidate_id'].endswith('candidate-1'))
                    self.assertEqual(case['root_cause_id'], None if label == 'root' else 'cause-A')
                    self.assertEqual(case['novelty'], 'unknown')
                    entry = next(c for c in ledger['candidates'] if c['cell_id'] == 'candidate-1')
                    self.assertIsNone(entry['analysis']['current'])
                    self.assertEqual({r['record_id'] for r in entry['analysis']['current_opinions']},
                                     {'a', label})
                    self.assertEqual(entry['analysis']['history'], supplied[:2])
                    self.assert_audited_accounting(ledger, reviews=supplied, label=f'{label}-{order}')

    def test_unconfirmed_current_same_root_evidence_constrains_novelty(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', novelty='historical')
        self.human(self.first, 'human-a')
        for order, supplied in enumerate(([a, b], [b, a])):
            ledger = self.ledger(reviews=supplied)
            accounting = ledger['root_cause_accounting']
            self.assertEqual(ledger['human_confirmed_candidates'], 1)
            self.assertEqual(accounting['human_confirmed_cases'], 1)
            self.assertEqual(accounting['confirmed_root_causes'], 1)
            self.assertEqual(accounting['independent_new_root_causes'], 0)
            self.assertEqual(accounting['historical_root_causes'], 1)
            self.assertEqual(accounting['groups'][0]['candidate_ids'], ['audit-run:candidate-1'])
            self.assertEqual(accounting['groups'][0]['evidence_candidate_ids'],
                             ['audit-run:candidate-1', 'audit-run:second'])
            self.assertEqual(accounting['groups'][0]['evidence_review_record_ids'], ['a', 'b'])
            self.assert_audited_accounting(ledger, reviews=supplied, label=f'unconfirmed-{order}')

    def test_rejected_id_and_supersedes_claims_are_order_independent_and_scoped(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='cause-B')
        third = dict(self.second, cell_id='third')
        self.rows.append(third)
        c = self.review(third, 'c', cause='cause-C')
        child = self.revise(third, c, 'c-child', cause='cause-C')
        fourth = dict(self.second, cell_id='dependent')
        self.rows.append(fourth)
        d = self.review(fourth, 'd', cause='cause-D')
        merged = self.revise(fourth, d, 'd-merge', cause='cause-B', operation='merge', related=['b'])
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        for claim in ('record_id', 'supersedes_record_id'):
            bad = copy.deepcopy(a)
            bad['record_id'] = 'bad-claim'
            bad[claim] = 'b'
            bad['root_cause']['novelty'] = 'malformed'
            bad['record_sha256'] = canonical_hash({k: v for k, v in bad.items() if k != 'record_sha256'})
            valid = [b, c, child, d, merged]
            for order, supplied in enumerate(([bad, *valid], [*valid, bad])):
                with self.subTest(claim=claim, order=order):
                    ledger = self.ledger(reviews=supplied)
                    self.assertEqual(ledger['human_confirmed_candidates'], 1)
                    accounting = ledger['root_cause_accounting']
                    self.assertEqual(accounting['independent_new_root_causes'], 1)
                    self.assertEqual(accounting['groups'][0]['candidate_ids'], ['audit-run:third'])
                    entries = {entry['cell_id']: entry for entry in ledger['candidates']}
                    self.assertIsNone(entries['candidate-1']['analysis']['current'])
                    self.assertIsNone(entries['second']['analysis']['current'])
                    self.assertIsNone(entries['dependent']['analysis']['current'])
                    self.assertEqual(entries['third']['analysis']['current'], child)
                    self.assertEqual(entries['third']['analysis']['history'], [c, child])
                    self.assertIn(bad, entries['second']['analysis']['history'])
                    self.assert_audited_accounting(ledger, reviews=supplied, label=f'claims-{claim}-{order}')

    def test_parallel_agreement_explicit_resolution_and_revoke_preserve_all_opinions(self):
        a = self.review(self.first, 'a', reviewer_id='analyst-a')
        other = self.review(self.first, 'other', cause='cause-C', reviewer_id='analyst-b')
        unrelated = self.review(self.second, 'unrelated', cause='unrelated-root')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        # Resolve the conflicting analyst's own opinion through a bound merge.
        resolved = self.review(self.first, 'resolved', reviewer_id='analyst-b', root_cause={
            'id': 'cause-A', 'novelty': 'new', 'operation': 'merge',
            'reason': 'Saved evidence resolves the parallel root disagreement.',
            'evidence_keys': ['raw', 'source'], 'related_record_ids': ['a']},
            supersedes_record_id='other', revision_reason='Explicit evidence-bound resolution.')
        for order, supplied in enumerate(([a, other, unrelated, resolved],
                                           [other, a, unrelated, resolved])):
            ledger = self.ledger(reviews=supplied)
            self.assertFalse(ledger['review_errors'])
            self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 2)
            entry = next(c for c in ledger['candidates'] if c['cell_id'] == 'candidate-1')
            self.assertEqual(entry['analysis']['current'], a)
            self.assertEqual([r['record_id'] for r in entry['analysis']['current_opinions']], ['a', 'resolved'])
            self.assertEqual(entry['analysis']['history'], [r for r in supplied if r != unrelated])
            self.assert_audited_accounting(ledger, reviews=supplied, label=f'resolved-{order}')
        revoked_b = self.review(self.first, 'revoke-b', reviewer_id='analyst-b', root_cause={
            'id': None, 'novelty': 'unknown', 'operation': 'revoke',
            'reason': 'Withdraw this analyst root assignment.',
            'evidence_keys': ['raw'], 'related_record_ids': []},
            supersedes_record_id=resolved['record_id'], revision_reason='Explicit withdrawal.')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['root_cause_accounting']['unresolved_root_cause_cases'], 1)
        self.assert_audited_accounting(ledger, reviews=self.reviews, label='one-revoked')
        self.review(self.first, 'revoke-a', reviewer_id='analyst-a', root_cause=revoked_b['root_cause'],
                    supersedes_record_id=a['record_id'], revision_reason='Both analysts withdraw assignment.')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['human_confirmed_candidates'], 2)
        self.assertEqual(ledger['root_cause_accounting']['unresolved_root_cause_cases'], 1)
        self.assertEqual(len(ledger['candidates'][0]['analysis']['history']), 5)
        self.assert_audited_accounting(ledger, reviews=self.reviews, label='both-revoked')
        # Same-reviewer changes still require an explicit predecessor.
        before = (self.root / 'reviews.jsonl').read_bytes()
        with self.assertRaises(ValueError):
            self.review(self.first, 'silent-revision', reviewer_id='analyst-a')
        self.assertEqual((self.root / 'reviews.jsonl').read_bytes(), before)

    def test_only_current_supported_novelty_evidence_constrains_a_confirmed_root(self):
        self.review(self.first, 'a')
        b = self.review(self.second, 'b', novelty='unknown')
        self.human(self.first, 'human-a')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 0)
        self.assertEqual(ledger['root_cause_accounting']['unknown_novelty_root_causes'], 1)
        self.assert_audited_accounting(ledger, reviews=self.reviews, label='unknown-evidence')
        historical = self.revise(self.second, b, 'historical', cause='cause-A', novelty='historical')
        resolved = self.revise(self.second, historical, 'resolved', cause='cause-A')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['root_cause_accounting']['groups'][0]['evidence_review_record_ids'], ['a', 'resolved'])
        self.assertEqual(ledger['human_confirmed_candidates'], 1)
        self.assert_audited_accounting(ledger, reviews=self.reviews, label='novelty-resolved')
        rejected = self.review(self.second, 'unsupported', novelty='historical', conclusion='rejected',
                               supersedes_record_id=resolved['record_id'], revision_reason='Support withdrawn.')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['root_cause_accounting']['groups'][0]['evidence_review_record_ids'], ['a'])
        self.assert_audited_accounting(ledger, reviews=self.reviews, label='unsupported-history')
        invalid = copy.deepcopy(rejected)
        invalid['record_id'] = 'malformed-historical'
        invalid['root_cause']['novelty'] = 'malformed'
        invalid['record_sha256'] = canonical_hash({k: v for k, v in invalid.items() if k != 'record_sha256'})
        supplied = [*self.reviews, invalid]
        ledger = self.ledger(reviews=supplied)
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['human_confirmed_candidates'], 1)
        self.assertEqual(ledger['root_cause_accounting']['groups'][0]['evidence_review_record_ids'], ['a'])
        self.assert_audited_accounting(ledger, reviews=supplied, label='malformed-history')

    def test_same_cause_across_cases_versions_and_vendors_counts_once(self):
        for index, row in enumerate(self.rows):
            self.review(row, f'review-{index}')
            self.human(row, f'human-{index}')
        original = copy.deepcopy(self.rows)
        ledger = self.ledger()
        self.assertEqual(ledger['human_confirmed_candidates'], 2)
        accounting = ledger['root_cause_accounting']
        self.assertEqual(accounting['human_confirmed_cases'], 2)
        self.assertEqual(accounting['confirmed_root_causes'], 1)
        self.assertEqual(accounting['independent_new_root_causes'], 1)
        self.assertEqual(accounting['groups'][0]['candidate_ids'], ['audit-run:candidate-1', 'audit-run:second'])
        self.assertEqual(self.rows, original)

    def test_root_analysis_is_retained_by_lifecycle_and_validated_on_import(self):
        review = self.review(self.first, 'root-review')
        core = CandidateLifecycle('audit-run:candidate-1', self.first).admit()
        core.record_review(reviewer_id=review['reviewer_id'], conclusion='supported',
                           evidence_refs=review['evidence_refs'], attribution='sut_candidate',
                           root_cause=review['root_cause'])
        self.assertEqual(core.to_dict()['reviews'][0]['root_cause'], review['root_cause'])
        self.assertNotEqual(core.candidate_status, 'confirmed')
        original = (self.root / 'reviews.jsonl').read_bytes()
        self.assertEqual(reports.append_review_record(self.root / 'reviews.jsonl', review,
                                                      original_row=self.first), review)
        self.assertEqual((self.root / 'reviews.jsonl').read_bytes(), original)
        conflict = copy.deepcopy(review)
        conflict['reason'] = 'Same ID, changed explanation.'
        conflict.pop('record_sha256')
        with self.assertRaisesRegex(ValueError, 'conflict'):
            reports.append_review_record(self.root / 'reviews.jsonl', conflict, original_row=self.first)
        for field, value in [('novelty', 'obviously-new'), ('evidence_keys', ['absent']),
                             ('reason', ''), ('id', []), ('operation', 'auto-confirm')]:
            changed = copy.deepcopy(review)
            changed.update(record_id='invalid-' + field, supersedes_record_id=review['record_id'],
                           revision_reason='Invalid analysis fixture.')
            changed['root_cause'][field] = value
            changed.pop('record_sha256')
            with self.subTest(field=field), self.assertRaises(ValueError):
                reports.append_review_record(self.root / 'reviews.jsonl', changed, original_row=self.first)
        self.assertEqual((self.root / 'reviews.jsonl').read_bytes(), original)

    def revise(self, row, previous, record_id, *, cause, operation='assign', related=(), novelty='new'):
        return self.review(row, record_id, root_cause={
            'id': cause, 'novelty': novelty, 'operation': operation,
            'reason': 'New fixture evidence supports this relationship.',
            'evidence_keys': ['raw', 'source'], 'related_record_ids': list(related)},
            supersedes_record_id=previous['record_id'], revision_reason='Reasoned fixture revision.')

    def test_merge_split_and_revoke_recompute_without_erasing_history(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='cause-B')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 2)
        # A merge reassigns the affected case to the evidenced destination root.
        merged = self.revise(self.second, b, 'b-merge', cause='cause-A', operation='merge', related=['a'])
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 1)
        split = self.revise(self.second, merged, 'b-split', cause='cause-C', operation='split', related=['a'])
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 2)
        revoked = self.revise(self.second, split, 'b-revoke', cause=None, operation='revoke', novelty='unknown')
        accounting = self.ledger()['root_cause_accounting']
        self.assertEqual(accounting['independent_new_root_causes'], 1)
        self.assertEqual(accounting['unresolved_root_cause_cases'], 1)
        self.assertEqual(reports.read_review_records(self.root / 'reviews.jsonl', original_rows=self.rows),
                         [a, b, merged, split, revoked])
        self.human(self.first, 'human-withdraw', action='revoke',
                   supersedes_record_id='human-0', revision_reason='Withdraw fixture confirmation.')
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 0)
        self.assertEqual(self.ledger()['human_confirmed_candidates'], 1)

    def test_merge_split_need_predecessor_and_evidenced_relationships(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='cause-B')
        before = (self.root / 'reviews.jsonl').read_bytes()
        for operation, related, cause in [('merge', [], 'cause-A'), ('merge', ['missing'], 'cause-A'),
                                          ('merge', ['a'], 'cause-C'), ('split', [], 'cause-C'),
                                          ('split', ['a'], 'cause-A')]:
            with self.subTest(operation=operation, related=related, cause=cause), self.assertRaises(ValueError):
                self.revise(self.second, b, 'bad', cause=cause, operation=operation, related=related)
        # Existing roots cannot quietly be relabeled by a generic assignment.
        with self.assertRaises(ValueError):
            self.revise(self.second, b, 'silent-relabel', cause='cause-A')
        self.assertEqual((self.root / 'reviews.jsonl').read_bytes(), before)

    def test_historical_unknown_and_unresolved_remain_visible_without_new_credit(self):
        self.rows = []
        for index, (cause, novelty) in enumerate([('new', 'new'), ('old', 'historical'),
                                                  ('pending', 'unknown'), (None, 'unknown')]):
            row = dict(self.first, cell_id=f'case-{index}')
            self.rows.append(row)
            self.review(row, f'review-{index}', cause=cause, novelty=novelty)
            self.human(row, f'human-{index}')
        ledger = self.ledger()
        accounting = ledger['root_cause_accounting']
        self.assertEqual(ledger['human_confirmed_candidates'], 4)
        self.assertEqual(accounting['confirmed_root_causes'], 3)
        self.assertEqual(accounting['independent_new_root_causes'], 1)
        self.assertEqual(accounting['historical_root_causes'], 1)
        self.assertEqual(accounting['unknown_novelty_root_causes'], 1)
        self.assertEqual(accounting['unresolved_root_cause_cases'], 1)
        self.assertEqual(len(accounting['cases']), 4)

    def test_mixed_novelty_on_same_root_is_conservative(self):
        self.review(self.first, 'a')
        b = self.review(self.second, 'b', novelty='unknown')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 0)
        self.revise(self.second, b, 'b-history', cause='cause-A', novelty='historical')
        accounting = self.ledger()['root_cause_accounting']
        self.assertEqual(accounting['independent_new_root_causes'], 0)
        self.assertEqual(accounting['historical_root_causes'], 1)

    def test_invalid_middle_blocks_whole_chain_without_poisoning_independent_case(self):
        first = self.review(self.first, 'a')
        middle = self.revise(self.first, first, 'a-middle', cause='cause-A', novelty='unknown')
        last = self.revise(self.first, middle, 'a-last', cause='cause-A')
        independent = self.review(self.second, 'b', cause='independent')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        invalid = copy.deepcopy(middle)
        invalid['root_cause']['reason'] = 'Changed without updating record digest.'
        supplied = [first, invalid, last, independent]
        for rows in (self.rows, list(reversed(self.rows))):
            self.rows = rows
            ledger = self.ledger(reviews=supplied)
            self.assertEqual(ledger['human_confirmed_candidates'], 1)
            self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
            cases = {c['candidate_id']: c for c in ledger['candidates']}
            self.assertIsNone(cases['audit-run:candidate-1']['analysis']['current'])
            self.assertEqual(cases['audit-run:candidate-1']['analysis']['history'], supplied[:3])
            self.assertEqual(cases['audit-run:second']['analysis']['current'], independent)
        # Rehashing does not turn a schema-invalid middle into a valid prefix.
        invalid['root_cause']['novelty'] = 'invalid'
        invalid['record_sha256'] = canonical_hash({k: v for k, v in invalid.items() if k != 'record_sha256'})
        ledger = self.ledger(reviews=[first, invalid, last, independent])
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)

    def test_tool_handoff_and_other_attributions_remain_separate(self):
        tool_row = dict(self.first, tool_error=True, runtime_status='tool_error')
        self.rows = [tool_row, self.second]
        handoff = {'location_basis': 'Fixture parser frame in saved source.',
                   'trigger': 'Parse the original retained fixture raw.',
                   'verification_requirements': ['Recheck original binding.', 'Exercise success and parser failure.'],
                   'evidence_keys': ['raw', 'source']}
        tool = self.review(tool_row, 'tool', cause=None, novelty='unknown', attribution='tool',
                           tool_repair_handoff=handoff)
        other = self.review(self.second, 'environment', cause=None, novelty='unknown', attribution='environment')
        ledger = self.ledger()
        accounting = ledger['root_cause_accounting']
        self.assertEqual(accounting['independent_new_root_causes'], 0)
        self.assertEqual(accounting['attributions']['tool'], ['audit-run:candidate-1'])
        self.assertEqual(accounting['attributions']['environment'], ['audit-run:second'])
        result = accounting['tool_repair_handoffs'][0]
        self.assertEqual(result['original_ref'], candidate_execution_identity(tool_row))
        self.assertEqual(result['raw_sha256'], tool_row['raw_sha256'])
        self.assertEqual(result['review_record_id'], tool['record_id'])
        for key, value in handoff.items():
            self.assertEqual(result[key], value)
        core = CandidateLifecycle('audit-run:second', self.second)
        core.record_review(reviewer_id='analysis-agent', conclusion='supported',
                           evidence_refs=other['evidence_refs'], attribution='tool', tool_repair_handoff=handoff)
        self.assertEqual(core.to_dict()['reviews'][0]['tool_repair_handoff'], handoff)
        self.assertNotEqual(core.candidate_status, 'confirmed')
        for attribute in ('allowed', 'unknown'):
            revised = self.review(self.second, attribute, cause=None, novelty='unknown', attribution=attribute,
                                  supersedes_record_id=other['record_id'], revision_reason='Fixture classification.')
            ledger = self.ledger()
            self.assertEqual(ledger['root_cause_accounting']['attributions'][attribute], ['audit-run:second'])
            self.assertEqual(ledger['human_confirmed_candidates'], 0)
            other = revised
        before = (self.root / 'reviews.jsonl').read_bytes()
        for key, value in [('location_basis', ''), ('trigger', ''), ('verification_requirements', []),
                           ('evidence_keys', ['invented'])]:
            invalid = dict(handoff, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.review(tool_row, 'bad-' + key, cause=None, novelty='unknown', attribution='tool',
                            tool_repair_handoff=invalid, supersedes_record_id='tool', revision_reason='Fixture.')
        self.assertEqual((self.root / 'reviews.jsonl').read_bytes(), before)

    def derived_report(self, base, ledger, label, *, reviews=None):
        from r2f.reports import run_report
        ledger_path = self.root / (label + '-ledger.json')
        report_path = self.root / (label + '-report.json')
        run_report.write_json(ledger_path, ledger)
        value = dict(base, review_records=self.reviews if reviews is None else reviews,
                     human_records=self.humans, candidate_ledger_path=ledger_path.name,
                     candidate_ledger_sha256=canonical_hash(ledger))
        value['report_sha256'] = run_report.exploratory_report_digest(value)
        run_report.write_json(report_path, value)
        return report_path

    def test_both_audits_recompute_root_accounting_even_after_outer_rehash(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        self.review(self.first, 'a')
        self.review(self.second, 'b')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        original_path, base = build_report(self.root, self.rows, reviews=self.reviews)
        original_bytes = original_path.read_bytes()
        raw_before = {r['raw_path']: Path(r['raw_path']).read_bytes() for r in self.rows}
        ledger = self.ledger()
        path = self.derived_report(base, ledger, 'valid')
        authority = lambda record: record['record_sha256'] in self.trusted
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            clean = audit(path, human_authority=authority)
            self.assertTrue(clean['evidence_valid'], clean['errors'])
            self.assertEqual(clean['independent_new_root_causes'], 1)
            self.assertFalse(clean['sealed'])
        for index, field in enumerate(('independent_new_root_causes', 'confirmed_root_causes',
                                       'groups', 'cases', 'attributions', 'tool_repair_handoffs')):
            forged = copy.deepcopy(ledger)
            forged['root_cause_accounting'][field] = 42 if field.endswith('causes') else {'forged': True}
            changed = self.derived_report(base, forged, f'forged-{index}')
            for audit in (run_report.audit_report, run_report.audit_exploratory_report):
                result = audit(changed, human_authority=authority)
                self.assertFalse(result['evidence_valid'], result['errors'])
                self.assertEqual(result['independent_new_root_causes'], 1)
        missing = copy.deepcopy(ledger)
        missing.pop('root_cause_accounting')
        missing_path = self.derived_report(base, missing, 'missing-accounting')
        self.assertFalse(run_report.audit_report(missing_path, human_authority=authority)['evidence_valid'])
        self.assertEqual(original_path.read_bytes(), original_bytes)
        self.assertEqual({r['raw_path']: Path(r['raw_path']).read_bytes() for r in self.rows}, raw_before)

    def test_agent_only_controls_and_unqualified_rows_never_receive_root_credit(self):
        self.review(self.first, 'agent-only')
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 0)
        for index, changes in enumerate(({'arm': 'negative'}, {'control_kind': 'health'},
                                        {'actual_run': False}, {'observer_complete': False},
                                        {'tool_error': True}, {'candidate_eligible': False},
                                        {'provenance': {}}, {'oracle_verdict': 'unknown'})):
            row = dict(self.second, cell_id=f'unqualified-{index}', source_identity='fixture-source', **changes)
            self.rows = [row]
            review = self.review(row, f'unqualified-review-{index}')
            human = self.human(row, f'unqualified-human-{index}')
            ledger = self.ledger(reviews=[review], human_records=[human])
            with self.subTest(changes=changes):
                self.assertEqual(ledger['human_confirmed_candidates'], 0)
                self.assertEqual(ledger['root_cause_accounting']['confirmed_root_causes'], 0)
                self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 0)
        self.rows = [self.first]
        human = self.human(self.first, 'unverified')
        ledger = self.ledger(reviews=[self.reviews[0]], human_records=[human], human_authority=None)
        self.assertEqual(ledger['root_cause_accounting']['human_confirmed_cases'], 0)

    def test_conflicting_id_fails_closed_for_both_claimed_chains_not_third_case(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='cause-B')
        third = dict(self.second, cell_id='third')
        self.rows.append(third)
        independent = self.review(third, 'c', cause='cause-C')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        conflict = copy.deepcopy(b)
        conflict['record_id'] = 'a'
        conflict['record_sha256'] = canonical_hash({k: v for k, v in conflict.items() if k != 'record_sha256'})
        ledger = self.ledger(reviews=[a, b, conflict, independent])
        self.assertEqual(ledger['human_confirmed_candidates'], 1)
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['root_cause_accounting']['groups'][0]['candidate_ids'], ['audit-run:third'])

    def test_audit_detects_rehashed_boolean_count_tamper(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        self.review(self.first, 'a')
        self.human(self.first, 'human')
        _, base = build_report(self.root, self.rows, reviews=self.reviews)
        ledger = self.ledger()
        ledger['root_cause_accounting']['independent_new_root_causes'] = True
        path = self.derived_report(base, ledger, 'boolean-count')
        authority = lambda record: record['record_sha256'] in self.trusted
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            result = audit(path, human_authority=authority)
            self.assertFalse(result['evidence_valid'], result['errors'])

    def test_core_rejected_middle_and_dependent_merge_cannot_restore_prefix(self):
        a = self.review(self.first, 'a')
        middle = self.revise(self.first, a, 'a-middle', cause='cause-A', novelty='unknown')
        last = self.revise(self.first, middle, 'a-last', cause='cause-A')
        b = self.review(self.second, 'b', cause='cause-B')
        merged = self.revise(self.second, b, 'b-merge', cause='cause-A', operation='merge', related=['a-last'])
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        bad_path = self.root / 'invalid source reference.txt'
        bad_path.write_text('Fixture invalid at the lifecycle boundary.')
        invalid = copy.deepcopy(middle)
        invalid['evidence_refs']['source'] = str(bad_path)
        invalid['evidence_sha256']['source'] = raw_sha256(bad_path)
        invalid['record_sha256'] = canonical_hash({k: v for k, v in invalid.items() if k != 'record_sha256'})
        supplied = [a, invalid, last, b, merged]
        ledger = self.ledger(reviews=supplied)
        self.assertEqual(ledger['human_confirmed_candidates'], 0)
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 0)
        self.assertTrue(all(c['analysis']['current'] is None for c in ledger['candidates']))
        with self.assertRaises(ValueError):
            reports.append_review_record(self.root / 'bad-log.jsonl', invalid, original_row=self.first)

    def test_failed_replay_stays_linked_and_does_not_delete_confirmed_analysis(self):
        self.first = dict(self.first, replay_attempts=[{
            'status': 'fail_environment', 'failure_reason': 'Offline fixture tool failure, no replay executed.',
            'attempt_id': 'fixture-attempt', 'timestamp': '2026-11-22T00:00:00+08:00',
            'original_ref': 'fixture-replay-ref'}])
        self.rows = [self.first]
        self.review(self.first, 'a')
        self.human(self.first, 'human')
        ledger = self.ledger()
        candidate = ledger['candidates'][0]
        self.assertEqual(candidate['replay_status'], 'fail_environment')
        self.assertEqual(candidate['replay_attempts'][0]['original_ref'], 'fixture-replay-ref')
        self.assertEqual(candidate['original_result']['oracle_verdict'], 'fail')
        self.assertEqual(candidate['evidence']['raw_sha256'], self.first['raw_sha256'])
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)

    def test_audits_retain_rejected_history_and_reject_rehashed_prefix_resurrection(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        a = self.review(self.first, 'a')
        middle = self.revise(self.first, a, 'a-middle', cause='cause-A', novelty='unknown')
        last = self.revise(self.first, middle, 'a-last', cause='cause-A')
        b = self.review(self.second, 'b', cause='independent')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        original_path, base = build_report(self.root, self.rows, reviews=self.reviews)
        original_bytes = original_path.read_bytes()
        invalid = copy.deepcopy(middle)
        invalid['root_cause']['reason'] = 'Tampered original review.'
        supplied = [a, invalid, last, b]
        ledger = self.ledger(reviews=supplied)
        faithful = self.derived_report(base, ledger, 'rejected-history', reviews=supplied)
        prefix = self.ledger(reviews=[a, b])
        forged = self.derived_report(base, prefix, 'resurrected-prefix', reviews=supplied)
        authority = lambda record: record['record_sha256'] in self.trusted
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            valid = audit(faithful, human_authority=authority)
            self.assertTrue(valid['evidence_valid'], valid['errors'])
            self.assertEqual(valid['independent_new_root_causes'], 1)
            rejected = audit(forged, human_authority=authority)
            self.assertFalse(rejected['evidence_valid'], rejected['errors'])
            self.assertEqual(rejected['independent_new_root_causes'], 1)
        self.assertEqual(original_path.read_bytes(), original_bytes)

    def test_reference_tamper_is_rechecked_and_duplicate_imports_never_inflate(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='independent')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        normal = self.ledger()
        duplicate = self.ledger(reviews=[a, b, a, b], human_records=self.humans * 2)
        self.assertEqual(normal['root_cause_accounting'], duplicate['root_cause_accounting'])
        raw = Path(self.first['raw_path'])
        original = raw.read_bytes()
        log_before = (self.root / 'reviews.jsonl').read_bytes()
        raw.write_text('{"event":"changed-fixture"}\n')
        ledger = self.ledger()
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        self.assertEqual(ledger['root_cause_accounting']['groups'][0]['candidate_ids'], ['audit-run:second'])
        with self.assertRaises(ValueError):
            reports.read_review_records(self.root / 'reviews.jsonl', original_rows=self.rows)
        with self.assertRaises(ValueError):
            reports.append_review_record(self.root / 'reviews.jsonl', a, original_row=self.first)
        self.assertEqual((self.root / 'reviews.jsonl').read_bytes(), log_before)
        raw.write_bytes(original)
        self.assertEqual(self.ledger()['root_cause_accounting'], normal['root_cause_accounting'])

    def test_group_merge_and_subset_split_use_explicit_bound_revisions_for_each_case(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='cause-B')
        third = dict(self.second, cell_id='third')
        self.rows.append(third)
        c = self.review(third, 'c', cause='cause-B')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 2)
        merged_b = self.revise(self.second, b, 'b-merge', cause='cause-A', operation='merge', related=['a'])
        self.revise(third, c, 'c-merge', cause='cause-A', operation='merge', related=['a'])
        merged = self.ledger()['root_cause_accounting']
        self.assertEqual(merged['independent_new_root_causes'], 1)
        self.assertEqual(merged['human_confirmed_cases'], 3)
        self.revise(self.second, merged_b, 'b-split', cause='cause-C', operation='split', related=['a'])
        split = self.ledger()['root_cause_accounting']
        self.assertEqual(split['independent_new_root_causes'], 2)
        self.assertEqual(split['groups'][0]['candidate_ids'], ['audit-run:candidate-1', 'audit-run:third'])

    def test_unreviewed_tool_handoff_keeps_original_ref_and_requires_more_evidence(self):
        tool = dict(self.first, runtime_status='tool_error', tool_error=True)
        self.rows = [tool]
        result = self.ledger()['root_cause_accounting']['tool_repair_handoffs'][0]
        self.assertEqual(result['status'], 'needs_evidence')
        self.assertEqual(result['original_ref'], candidate_execution_identity(tool))
        self.assertEqual(result['raw_sha256'], tool['raw_sha256'])
        self.assertEqual(self.ledger()['root_cause_accounting']['independent_new_root_causes'], 0)

    def test_lifecycle_root_analysis_without_attribution_still_hashes_its_evidence(self):
        review = self.review(self.first, 'a')
        core = CandidateLifecycle('audit-run:candidate-1', self.first)
        core.record_review(reviewer_id='analysis-agent', conclusion='supported',
                           evidence_refs=review['evidence_refs'], root_cause=review['root_cause'])
        stored = core.to_dict()['reviews'][0]
        self.assertEqual(stored['evidence_sha256'], review['evidence_sha256'])
        self.assertNotEqual(core.candidate_status, 'confirmed')

    def test_rejected_last_record_is_scoped_by_bound_identity_even_without_candidate_id(self):
        a = self.review(self.first, 'a')
        b = self.review(self.second, 'b', cause='independent')
        for index, row in enumerate(self.rows):
            self.human(row, f'human-{index}')
        invalid = copy.deepcopy(a)
        invalid.update(record_id='invalid-last', supersedes_record_id='a', revision_reason='Fixture revision.')
        invalid.pop('candidate_id')
        invalid['record_sha256'] = canonical_hash({k: v for k, v in invalid.items() if k != 'record_sha256'})
        ledger = self.ledger(reviews=[a, b, invalid])
        self.assertEqual(ledger['root_cause_accounting']['independent_new_root_causes'], 1)
        cases = {case['candidate_id']: case for case in ledger['candidates']}
        self.assertIsNone(cases['audit-run:candidate-1']['analysis']['current'])
        self.assertIn(invalid, cases['audit-run:candidate-1']['analysis']['history'])
        self.assertEqual(cases['audit-run:second']['analysis']['current'], b)
