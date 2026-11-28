"""Issue21 CP2 offline mechanisms; every row/model/decision here is a fixture."""
import copy
import json
from pathlib import Path
import unittest

from r2f.core.candidate import candidate_execution_identity
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.replay import ReplayCapsule
from test_core_contracts import scenario
from r2f.reports import candidate_report as reports
import test_capability_assessment as cp1
import test_fuzz_review as fixtures


class CapabilityValidation(unittest.TestCase):
    def setUp(self):
        self.case = cp1.CapabilityAssessment('runTest')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.root, self.row = self.case.root, self.case.row
        self.parent = self.case.review()
        self.original = candidate_execution_identity(self.row)
        self.task = dict(candidate_id=self.parent['candidate_id'],
                         review_record_id=self.parent['record_id'],
                         review_record_sha256=self.parent['record_sha256'], task_id='rule-gap')
        self.models = {}
        for name, version in [('before', 'fixture-v1'), ('after', 'fixture-v2')]:
            path = self.root / (name + '.model')
            path.write_text('OFFLINE toy model ' + version)
            self.models[name] = dict(version=version, path=str(path), sha256=raw_sha256(path),
                                     source=self.original['source'])
        self.receipts = []
        for index in range(3):
            if index == 0:
                row, kind, expected = self.row, 'defect', {'violation': True}
            else:
                directory = self.root / str(index)
                directory.mkdir()
                row = fixtures.base_row(directory, 'normal', arm='baseline', verdict='pass',
                                        run_id='normal-' + str(index))
                Path(row['raw_path']).write_text(json.dumps({'event': 'fixture', 'repeat': index}) + '\n')
                row['raw_sha256'] = raw_sha256(row['raw_path'])
                # Bind these offline controls to real canonical input capsules;
                # the generic execution fixture's asserted digest is insufficient.
                normal = scenario()
                normal.arm = row['arm']
                capsule = ReplayCapsule.from_scenario(
                    normal, source_manifest=self.original['source'],
                    final_input={'reader_delay_sec': 0.1})
                capsule_path = directory / 'input-capsule.json'
                capsule_path.write_text(json.dumps(capsule.to_dict()))
                effective = capsule.input['effective_input']
                row.update(replay_path=str(capsule_path), final_input=effective['final_input'],
                           input_layer_receipts=effective['input_layer_receipts'],
                           effective_input_sha256=capsule.input['effective_input_sha256'])
                kind, expected = 'normal', {'violation': False}
            self.receipts.append(dict(schema_version='r2f.s1-capability-evaluation/v1',
                                      task_ref=self.task, kind=kind, row=row, models=self.models,
                                      input_sha256=row['effective_input_sha256'], expected=expected,
                                      before={'violation': False}, after=expected))
        self.bundle = dict(schema_version='r2f.s1-capability-validation-evidence/v1',
                           provenance='fixture', task_ref=self.task, original_ref=self.original,
                           models=self.models, cases=[])

    def proposal(self):
        self.bundle['cases'] = []
        for index, receipt in enumerate(self.receipts):
            path = self.root / ('evaluation-' + str(index) + '.json')
            path.write_text(json.dumps(receipt))
            self.bundle['cases'].append(dict(path=str(path), sha256=raw_sha256(path)))
        control = self.root / 'validation.json'
        control.write_text(json.dumps(self.bundle))
        return dict(record_id='validation-1', candidate_id=self.parent['candidate_id'],
                    reviewer_id=self.parent['reviewer_id'], attribution='sut_candidate',
                    conclusion='supported', independent_of=['trigger-author'], reason='OFFLINE evaluation.',
                    supersedes_record_id=self.parent['record_id'], revision_reason='Validate proposed model.',
                    evidence_refs=dict(self.case.refs, control=str(control)),
                    capability_assessment=cp1.assessment(),
                    capability_validation=dict(schema_version='r2f.s1-capability-validation/v1',
                                               task_ref=self.task, original_ref=self.original,
                                               evidence_key='control', evidence_sha256=raw_sha256(control)))

    def append(self, value=None):
        return reports.append_review_record(self.case.log, value or self.proposal(), original_row=self.row)

    def test_fixture_defect_and_stable_normals_validate_mechanism_without_implementation(self):
        prefix = self.case.log.read_bytes()
        raw = Path(self.row['raw_path']).read_bytes()
        record = self.append()
        readback = reports.read_review_records(self.case.log, original_rows=[self.row])
        self.assertEqual(readback, [self.parent, record])
        ledger = self.case.ledger(reviews=readback)
        feedback = ledger['root_cause_accounting']['cases'][0]['capability_assessment']
        task = next(t for t in feedback['tasks'] if t['task_id'] == 'rule-gap')
        validation = task['validation']
        self.assertEqual(validation['status'], 'fixture_validated')
        self.assertEqual(validation['task_ref'], self.task)
        self.assertEqual(validation['normal_evaluations'], 2)
        self.assertEqual(validation['review_record_id'], record['record_id'])
        self.assertEqual(validation['models'], self.models)
        self.assertFalse(task['implemented'])
        self.assertFalse(feedback['human_confirmed'])
        self.assertEqual(ledger['human_confirmed_candidates'], 0)
        self.assertEqual(Path(self.row['raw_path']).read_bytes(), raw)
        self.assertTrue(self.case.log.read_bytes().startswith(prefix))

    def test_frozen_model_source_keeps_known_empty_dirty_paths_at_public_review(self):
        # Explicit schema fixture, not evidence of an actual frozen execution.
        source = dict(source_repo='/OFFLINE-FIXTURE/source', head='1' * 40,
                      behavior_tree_sha256='2' * 64, source_frozen=True,
                      dirty_behavior_paths=[])
        self.models['after']['source'] = source
        record = self.append()
        readback = reports.read_review_records(self.case.log, original_rows=[self.row])
        self.assertEqual(readback[-1], record)
        self.assertEqual(self.models['after']['source'], source)
        ledger = self.case.ledger(reviews=readback)
        assessment = ledger['root_cause_accounting']['cases'][0]['capability_assessment']
        task = next(t for t in assessment['tasks'] if t['task_id'] == 'rule-gap')
        self.assertFalse(task['implemented'])
        self.assertFalse(assessment['human_confirmed'])

    def test_frozen_model_source_exception_rejects_incomplete_and_empty_identities(self):
        source = dict(source_repo='/OFFLINE-FIXTURE/source', head='1' * 40,
                      behavior_tree_sha256='2' * 64, source_frozen=True,
                      dirty_behavior_paths=[])
        malformed = [None, {}, '', dict(source, head=None), dict(source, head='short'),
                     dict(source, behavior_tree_sha256='not-a-digest'),
                     dict(source, source_repo=''), dict(source, source_frozen=1),
                     dict(source, source_frozen=False), dict(source, dirty_behavior_paths=None),
                     dict(source, arbitrary_identity={})]
        for missing in ('source_repo', 'head', 'behavior_tree_sha256', 'source_frozen'):
            malformed.append({key: value for key, value in source.items() if key != missing})
        aliased = dict(source)
        aliased['dirty_paths'] = aliased.pop('dirty_behavior_paths')
        malformed.append(aliased)
        before = self.case.log.read_bytes()
        legacy_source = self.models['after']['source']
        for replacement in malformed:
            with self.subTest(source=replacement):
                self.models['after']['source'] = replacement
                with self.assertRaises(ValueError):
                    self.append()
                self.assertEqual(self.case.log.read_bytes(), before)
        # Legacy noncanonical provenance remains readable at the same public seam.
        self.models['after']['source'] = legacy_source
        record = self.append()
        self.assertEqual(reports.read_review_records(
            self.case.log, original_rows=[self.row])[-1], record)

    def test_frozen_source_allowance_does_not_apply_to_observation_identities(self):
        from r2f.core.candidate import CandidateLifecycle
        source = dict(source_repo='/OFFLINE-FIXTURE/source', head='1' * 40,
                      behavior_tree_sha256='2' * 64, source_frozen=True,
                      dirty_behavior_paths=[])
        for field in ('endpoint_identity', 'instrumentation_identity'):
            with self.subTest(field=field):
                row = copy.deepcopy(self.row)
                row[field] = source
                with self.assertRaisesRegex(ValueError, 'empty identity container'):
                    CandidateLifecycle(self.parent['candidate_id'], row)

    def test_rejects_suggestions_only_implementation_claim_at_canonical_and_core_boundaries(self):
        from r2f.core.candidate import CandidateLifecycle
        value = self.proposal()
        value['implemented'] = True
        before = self.case.log.read_bytes()
        with self.assertRaisesRegex(ValueError, 'implementation'):
            self.append(value)
        self.assertEqual(self.case.log.read_bytes(), before)
        core = CandidateLifecycle(self.parent['candidate_id'], self.row)
        with self.assertRaises(ValueError):
            core.record_review(reviewer_id='fixture-reviewer', conclusion='supported',
                               evidence_refs=value['evidence_refs'], capability_assessment=cp1.assessment(),
                               capability_validation=dict(value['capability_validation'], implemented=True),
                               capability_task_review=self.parent)
        self.assertEqual(core.to_dict()['reviews'], [])

    def test_missing_conflicting_stale_or_unrelated_controls_cannot_enter_review_log(self):
        baseline = copy.deepcopy((self.receipts, self.bundle))
        mutations = {
            'missing normal': lambda: self.receipts.pop(),
            'no defect': lambda: self.receipts.pop(0),
            'duplicate normal': lambda: self.receipts.__setitem__(2, copy.deepcopy(self.receipts[1])),
            'unrelated target': lambda: self.receipts[1]['row'].update(target_id='other-target'),
            'stale source': lambda: self.receipts[1]['row'].update(source_identity={'head': 'stale'}),
            'wrong revision': lambda: self.receipts[1]['row'].update(target_revision=99),
            'wrong RMW': lambda: self.receipts[1]['row'].update(rmw='other-rmw'),
            'wrong rule': lambda: self.receipts[1]['row'].update(rule_id='unrelated-rule'),
            'tool failure': lambda: self.receipts[1]['row'].update(tool_error=True),
            'incomplete observation': lambda: self.receipts[1]['row'].update(observer_complete=False),
            'normal input conflict': lambda: self.receipts[2].update(input_sha256='a' * 64),
            'stale evaluated model': lambda: self.receipts[1].update(models={'before': 'stale', 'after': 'stale'}),
            'normal regression': lambda: self.receipts[1].update(after={'violation': True}),
            'unstable baseline': lambda: self.receipts[1].update(before={'violation': True}),
            'improvement missed defect': lambda: self.receipts[0].update(after={'violation': False}),
            'no demonstrated improvement': lambda: self.receipts[0].update(before={'violation': True}),
            'cross-task control': lambda: self.receipts[1].update(task_ref=dict(self.task, task_id='fact-gap')),
            'injected implementation': lambda: self.bundle.update(implemented=True),
        }
        before = self.case.log.read_bytes()
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                self.receipts, self.bundle = copy.deepcopy(baseline)
                mutate()
                with self.assertRaises(ValueError):
                    self.append()
                self.assertEqual(self.case.log.read_bytes(), before)

    def test_task_original_review_and_model_artifact_identities_are_exact(self):
        value = self.proposal()
        before = self.case.log.read_bytes()
        for field, replacement in [('task_id', 'absent'), ('review_record_id', 'absent'),
                                   ('review_record_sha256', 'b' * 64), ('candidate_id', 'other:case')]:
            forged = copy.deepcopy(value)
            forged['capability_validation']['task_ref'][field] = replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.append(forged)
        for field, replacement in [('original_ref', dict(self.original, execution_sha256='a' * 64)),
                                   ('evidence_key', 'summary'), ('evidence_sha256', 'c' * 64)]:
            forged = copy.deepcopy(value)
            forged['capability_validation'][field] = replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.append(forged)
        forged = copy.deepcopy(value)
        forged['capability_assessment']['proposed_tasks'][0]['description'] = 'Changed proposal'
        with self.assertRaises(ValueError):
            self.append(forged)
        Path(self.models['after']['path']).write_text('new unvalidated model')
        with self.assertRaises(ValueError):
            self.append(value)
        self.assertEqual(self.case.log.read_bytes(), before)

    def test_read_and_ledger_recheck_stale_evidence_and_preserve_history(self):
        record = self.append()
        log_bytes = self.case.log.read_bytes()
        Path(self.models['after']['path']).write_text('source changed after review append')
        with self.assertRaises(ValueError):
            reports.read_review_records(self.case.log, original_rows=[self.row])
        ledger = self.case.ledger(reviews=[self.parent, record])
        self.assertTrue(ledger['review_errors'])
        feedback = ledger['root_cause_accounting']['cases'][0]['capability_assessment']
        self.assertEqual(feedback['status'], 'needs_evidence')
        self.assertEqual(feedback['tasks'], [])
        self.assertEqual(ledger['human_confirmed_candidates'], 0)
        self.assertEqual(self.case.log.read_bytes(), log_bytes)

    def test_core_public_review_validates_same_receipts_without_granting_qualification(self):
        from r2f.core.candidate import CandidateLifecycle
        value = self.proposal()
        core = CandidateLifecycle(self.parent['candidate_id'], self.row)
        core.record_review(reviewer_id='fixture-reviewer', conclusion='supported',
                           evidence_refs=value['evidence_refs'], capability_assessment=cp1.assessment(),
                           capability_validation=value['capability_validation'],
                           capability_task_review=self.parent)
        self.assertEqual(core.to_dict()['reviews'][0]['capability_validation'], value['capability_validation'])
        self.assertEqual(core.candidate_status, 'observed')
        for parent in (None, dict(self.parent, record_sha256='a' * 64)):
            with self.assertRaises(ValueError):
                core.record_review(reviewer_id='fixture-reviewer', conclusion='supported',
                                   evidence_refs=value['evidence_refs'], capability_assessment=cp1.assessment(),
                                   capability_validation=value['capability_validation'], capability_task_review=parent)
        self.assertEqual(len(core.to_dict()['reviews']), 1)

    def test_absent_validation_preserves_cp1_projection_and_legacy_bytes(self):
        before = self.case.log.read_bytes()
        cp1_feedback = self.case.feedback()
        self.assertTrue(all('validation' not in task and not task['implemented']
                            for task in cp1_feedback['tasks']))
        self.assertEqual(reports.append_review_record(self.case.log, self.parent, original_row=self.row), self.parent)
        self.assertEqual(self.case.log.read_bytes(), before)
        self.assertEqual(self.case.feedback(), cp1_feedback)
        self.assertEqual(reports.exploratory_candidate_ledger([])['natural_candidate_count'], 0)

    def test_normal_evidence_requires_complete_execution_and_input_identities(self):
        baseline = copy.deepcopy((self.receipts, self.bundle))
        for field in ('cell_id', 'effective_input_sha256'):
            with self.subTest(field=field):
                self.receipts, self.bundle = copy.deepcopy(baseline)
                for receipt in self.receipts[1:]:
                    receipt['row'].pop(field)
                    if field == 'effective_input_sha256':
                        receipt['input_sha256'] = None
                with self.assertRaises(ValueError):
                    self.append()

    def test_validation_revision_cannot_borrow_missing_or_superseded_task_review(self):
        value = self.proposal()
        with self.assertRaises(ValueError):
            reports.append_review_record(self.root / 'orphan.jsonl', value, original_row=self.row)
        self.assertEqual((self.root / 'orphan.jsonl').read_bytes(), b'')
        record = self.append(value)
        before = self.case.log.read_bytes()
        self.assertEqual(self.append(record), record)
        self.assertEqual(self.case.log.read_bytes(), before)
        stale = dict(value, record_id='stale-validation', reviewer_id='other-fixture-reviewer')
        with self.assertRaisesRegex(ValueError, 'already_replaced'):
            self.append(stale)
        self.assertEqual(self.case.log.read_bytes(), before)
        ledger = self.case.ledger(reviews=[record, self.parent])
        self.assertTrue(ledger['review_errors'])
        self.assertEqual(ledger['root_cause_accounting']['cases'][0]['capability_assessment']['tasks'], [])

    def test_report_auditors_reconstruct_validation_instead_of_trusting_rehashed_projection(self):
        from r2f.reports import run_report
        from test_fuzz_audit import build_report
        record = self.append()
        reviews = [self.parent, record]
        original_path, base = build_report(self.root, [self.row], reviews=[])
        original_bytes = original_path.read_bytes()
        raw_bytes = Path(self.row['raw_path']).read_bytes()
        ledger = self.case.ledger(reviews=reviews)

        def derived(value, label):
            ledger_path = self.root / (label + '-ledger.json')
            path = self.root / (label + '-report.json')
            run_report.write_json(ledger_path, value)
            report = dict(base, review_records=reviews, candidate_ledger_path=ledger_path.name,
                          candidate_ledger_sha256=canonical_hash(value))
            report['report_sha256'] = run_report.exploratory_report_digest(report)
            run_report.write_json(path, report)
            return path

        valid = derived(ledger, 'faithful')
        forged = copy.deepcopy(ledger)
        tasks = forged['root_cause_accounting']['cases'][0]['capability_assessment']['tasks']
        next(t for t in tasks if t['task_id'] == 'rule-gap')['implemented'] = True
        invalid = derived(forged, 'forged')
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            result = audit(valid)
            self.assertTrue(result['evidence_valid'], result['errors'])
            self.assertFalse(result['sealed'])
            result = audit(invalid)
            self.assertFalse(result['evidence_valid'])
            self.assertIn('exploratory_ledger_mismatch:root_cause_accounting', result['errors'])
        self.assertEqual(original_path.read_bytes(), original_bytes)
        self.assertEqual(Path(self.row['raw_path']).read_bytes(), raw_bytes)

    def test_core_cannot_accept_rehashed_task_review_with_unrelated_raw(self):
        from r2f.core.candidate import CandidateLifecycle
        other_raw = self.receipts[1]['row']['raw_path']
        self.parent['evidence_refs']['raw'] = other_raw
        self.parent['evidence_sha256']['raw'] = raw_sha256(other_raw)
        self.parent['record_sha256'] = canonical_hash({k: v for k, v in self.parent.items()
                                                       if k != 'record_sha256'})
        self.task['review_record_sha256'] = self.parent['record_sha256']
        value = self.proposal()
        core = CandidateLifecycle(self.parent['candidate_id'], self.row)
        with self.assertRaisesRegex(ValueError, 'raw_identity'):
            core.record_review(reviewer_id='fixture-reviewer', conclusion='supported',
                               evidence_refs=value['evidence_refs'], capability_assessment=cp1.assessment(),
                               capability_validation=value['capability_validation'], capability_task_review=self.parent)
        self.assertEqual(core.to_dict()['reviews'], [])

    def assert_rejected_at_public_boundaries(self, mutate, reason):
        """Rehash an invalid import so each boundary must inspect its evidence."""
        from r2f.core.candidate import CandidateLifecycle
        from r2f.reports import run_report
        from test_fuzz_audit import build_report

        prefix = self.case.log.read_bytes()
        record = self.append()
        path, report = build_report(self.root, [self.row], reviews=[self.parent, record])
        ledger_path = self.root / report['candidate_ledger_path']
        supported_ledger = ledger_path.read_text()
        old_record_hash = record['record_sha256']
        old_control_hash = record['evidence_sha256']['control']
        self.case.log.write_bytes(prefix)
        raw = Path(self.row['raw_path']).read_bytes()
        mutate()
        value = self.proposal()
        record['capability_validation'] = value['capability_validation']
        record['evidence_sha256']['control'] = value['capability_validation']['evidence_sha256']
        record['record_sha256'] = canonical_hash({k: v for k, v in record.items()
                                                  if k != 'record_sha256'})
        with self.subTest(boundary='append'):
            with self.assertRaisesRegex(ValueError, reason):
                self.append(value)
        self.case.log.write_bytes(prefix)
        self.case.log.write_text(prefix.decode() + json.dumps(record) + '\n')
        forged_bytes = self.case.log.read_bytes()
        with self.subTest(boundary='read'):
            with self.assertRaisesRegex(ValueError, reason):
                reports.read_review_records(self.case.log, original_rows=[self.row])
        ledger = self.case.ledger(reviews=[self.parent, record])
        with self.subTest(boundary='ledger'):
            self.assertTrue(any(reason in str(e) for e in ledger['review_errors']), ledger['review_errors'])
            feedback = ledger['root_cause_accounting']['cases'][0]['capability_assessment']
            self.assertEqual(feedback['status'], 'needs_evidence')
            self.assertEqual(feedback['tasks'], [])
            self.assertEqual(ledger['human_confirmed_candidates'], 0)
        core = CandidateLifecycle(self.parent['candidate_id'], self.row)
        with self.subTest(boundary='core'):
            with self.assertRaisesRegex(ValueError, reason):
                core.record_review(reviewer_id='fixture-reviewer', conclusion='supported',
                                   evidence_refs=value['evidence_refs'], capability_assessment=cp1.assessment(),
                                   capability_validation=value['capability_validation'],
                                   capability_task_review=self.parent)
            self.assertEqual(core.to_dict()['reviews'], [])
        # Rehash the previously supported projection, still claiming successful
        # validation. Auditors must reconstruct evidence, not trust that claim.
        supported_ledger = supported_ledger.replace(old_record_hash, record['record_sha256'])
        supported_ledger = supported_ledger.replace(old_control_hash, record['evidence_sha256']['control'])
        forged_ledger = json.loads(supported_ledger)
        run_report.write_json(ledger_path, forged_ledger)
        report.update(review_records=[self.parent, record],
                      candidate_ledger_sha256=canonical_hash(forged_ledger))
        report['report_sha256'] = run_report.exploratory_report_digest(report)
        run_report.write_json(path, report)
        for audit in (run_report.audit_report, run_report.audit_exploratory_report):
            with self.subTest(boundary=audit.__name__):
                result = audit(path)
                self.assertFalse(result['evidence_valid'], result)
                self.assertFalse(result['sealed'])
        self.assertEqual(self.case.log.read_bytes(), forged_bytes)
        self.assertEqual(Path(self.row['raw_path']).read_bytes(), raw)
        self.case.log.write_bytes(prefix)

    def test_contradictory_normal_configs_cannot_share_an_asserted_input_hash(self):
        self.bundle['provenance'] = 'recorded_evaluation'
        def contradict():
            self.receipts[2]['row']['final_input']['reader_delay_sec'] = 99.0
        self.assert_rejected_at_public_boundaries(contradict, 'capability_validation_normal_input')

    def test_normal_revision_requires_canonical_integer_identity(self):
        baseline = copy.deepcopy(self.receipts)
        for revision in (False, 0.0):
            with self.subTest(revision=repr(revision)):
                self.receipts = copy.deepcopy(baseline)
                self.assert_rejected_at_public_boundaries(
                    lambda: self.receipts[1]['row'].update(target_revision=revision),
                    'capability_validation_revision_invalid')

    def test_normal_control_labels_require_canonical_type_and_allowed_value(self):
        self.bundle['provenance'] = 'recorded_evaluation'
        baseline = copy.deepcopy(self.receipts)
        for kind in ([], ['fault'], 'NOT_A_CANONICAL_CONTROL', False, 0, '', {}, 'mutation'):
            with self.subTest(control_kind=repr(kind)):
                self.receipts = copy.deepcopy(baseline)
                self.assert_rejected_at_public_boundaries(
                    lambda: self.receipts[1]['row'].update(control_kind=kind),
                    'capability_validation_normal_not_stable')

    def test_normal_input_requires_available_capsule_and_submitted_config_binding(self):
        baseline = copy.deepcopy(self.receipts)
        mutations = {
            'missing capsule': lambda: self.receipts[1]['row'].pop('replay_path'),
            'unavailable capsule': lambda: self.receipts[1]['row'].update(replay_path=str(self.root / 'absent')),
            'missing submitted config': lambda: self.receipts[1]['row'].pop('final_input'),
            'missing deterministic receipts': lambda: self.receipts[1]['row'].pop('input_layer_receipts'),
            'contradictory deterministic receipts': lambda: self.receipts[1]['row'].update(
                input_layer_receipts=[{'layer_id': 'parameter', 'config': {'reader_delay_sec': 99.0}}]),
            'unverified asserted hashes': lambda: [
                (r.update(input_sha256='f' * 64), r['row'].update(effective_input_sha256='f' * 64))
                for r in self.receipts[1:]],
        }
        for name, mutate in mutations.items():
            with self.subTest(evidence=name):
                self.receipts = copy.deepcopy(baseline)
                self.assert_rejected_at_public_boundaries(mutate, 'capability_validation_normal_input')

    def test_valid_but_different_canonical_normal_inputs_are_not_stable_repeats(self):
        def different_input():
            receipt = self.receipts[2]
            plan = scenario()
            plan.arm = 'baseline'
            capsule = ReplayCapsule.from_scenario(
                plan, source_manifest=self.original['source'], final_input={'reader_delay_sec': 99.0})
            path = self.root / 'different-input-capsule.json'
            path.write_text(json.dumps(capsule.to_dict()))
            receipt['row'].update(replay_path=str(path), final_input=capsule.input['effective_input']['final_input'],
                                  effective_input_sha256=capsule.input['effective_input_sha256'])
            receipt['input_sha256'] = capsule.input['effective_input_sha256']
        self.assert_rejected_at_public_boundaries(different_input, 'capability_validation_normal_input_conflict')

    def test_valid_normal_controls_keep_mechanism_only_support_at_all_public_boundaries(self):
        from r2f.core.candidate import CandidateLifecycle
        from r2f.reports import run_report
        from test_fuzz_audit import build_report

        baseline = copy.deepcopy((self.receipts, self.bundle))
        prefix = self.case.log.read_bytes()
        for provenance in ('fixture', 'recorded_evaluation'):
            for arm, kind in (('baseline', None), ('baseline', 'health'),
                              ('transparent', None), ('transparent', 'transparency')):
                with self.subTest(provenance=provenance, arm=arm, control_kind=kind):
                    self.receipts, self.bundle = copy.deepcopy(baseline)
                    self.case.log.write_bytes(prefix)
                    self.bundle['provenance'] = provenance
                    for receipt in self.receipts[1:]:
                        row = receipt['row']
                        plan = scenario()
                        plan.arm = arm
                        capsule = ReplayCapsule.from_scenario(
                            plan, source_manifest=self.original['source'], final_input={'reader_delay_sec': 0.1})
                        Path(row['replay_path']).write_text(json.dumps(capsule.to_dict()))
                        row.update(arm=arm, control_kind=kind,
                                   final_input=capsule.input['effective_input']['final_input'],
                                   effective_input_sha256=capsule.input['effective_input_sha256'])
                        receipt['input_sha256'] = capsule.input['effective_input_sha256']
                    value = self.proposal()
                    record = self.append(value)
                    records = reports.read_review_records(self.case.log, original_rows=[self.row])
                    self.assertEqual(records, [self.parent, record])
                    ledger = self.case.ledger(reviews=records)
                    self.assertEqual(ledger['review_errors'], [])
                    self.assertEqual(ledger['human_confirmed_candidates'], 0)
                    tasks = ledger['root_cause_accounting']['cases'][0]['capability_assessment']['tasks']
                    validation = next(t['validation'] for t in tasks if t['task_id'] == 'rule-gap')
                    self.assertEqual(validation['status'], 'fixture_validated' if provenance == 'fixture'
                                     else 'evidence_supported')
                    self.assertEqual(validation['qualification'], 'mechanism_only')
                    self.assertTrue(all(t['implemented'] is False for t in tasks))
                    core = CandidateLifecycle(self.parent['candidate_id'], self.row)
                    core.record_review(reviewer_id='fixture-reviewer', conclusion='supported',
                                       evidence_refs=value['evidence_refs'], capability_assessment=cp1.assessment(),
                                       capability_validation=value['capability_validation'],
                                       capability_task_review=self.parent)
                    self.assertEqual(len(core.to_dict()['reviews']), 1)
                    path, _ = build_report(self.root, [self.row], reviews=records)
                    for audit in (run_report.audit_report, run_report.audit_exploratory_report):
                        result = audit(path)
                        self.assertTrue(result['evidence_valid'], result['errors'])
                        self.assertFalse(result['sealed'])
        self.case.log.write_bytes(prefix)
