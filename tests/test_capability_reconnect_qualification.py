"""CP5 offline qualification contracts: fixtures never count as real findings."""
import copy
import json
import unittest

from r2f.campaign import runner
from r2f.core.evidence import canonical_hash
from r2f.reports import candidate_report as reports
import test_capability_reconnect_activation as cp4
import test_human_host as signed_host


class ReconnectQualification(unittest.TestCase):
    def setUp(self):
        self.cp4 = cp4.ReconnectActivation('runTest')
        self.cp4.setUp()
        self.addCleanup(self.cp4.doCleanups)
        self.fixture = self.cp4.fixture
        self.output = self.cp4.output

    def run_saved(self, **options):
        f = self.fixture
        return runner.reevaluate_capability(self.output, review_log=f.cp.case.log,
            validation_record_id=self.cp4.record['record_id'], original_row=f.cp.row,
            saved_cases=f.saved, **options)

    def test_fixture_has_explicit_validated_mechanism_without_real_activation(self):
        report = self.run_saved()
        view = report['capability_reconnect']
        self.assertEqual(view['activation_status'], 'fixture_validated')
        self.assertFalse(view['implemented'])
        self.assertFalse(view['human_confirmed'])
        self.assertFalse(view['new_execution'])
        self.assertEqual(report['candidates'], [])
        self.assertEqual(report['human_confirmed_candidates'], 0)
        self.assertEqual(report['natural_candidate_count'], 0)
        self.assertTrue(reports.query_runner_evidence(self.output)['evidence_valid'])

    def test_publication_preserves_measured_source_without_fixture_qualification(self):
        report = self.run_saved()
        evaluation = json.loads((self.output / 'capability-reevaluation.json').read_text())
        self.assertEqual(evaluation['evaluation_source'], self.fixture.source)
        self.assertEqual(evaluation['models']['after']['source'], self.fixture.source)
        self.assertEqual([r['after']['verdict'] for r in evaluation['call_receipts']],
                         ['fail', 'pass', 'pass'])
        self.assertTrue(all(r['oracle_called'] for r in evaluation['call_receipts']))
        view = report['capability_reconnect']
        self.assertFalse(view['implemented'])
        self.assertFalse(view['human_confirmed'])
        self.assertFalse(view['new_execution'])
        self.assertEqual(view['qualification'], 'fixture')
        if not self.fixture.source['source_frozen']:
            self.assertIn('evaluation_source_not_frozen', view['implementation_pending_obligations'])
        self.assertEqual(report['candidates'], [])
        self.assertTrue(reports.query_runner_evidence(self.output)['evidence_valid'])

    def test_public_runner_still_rejects_rebound_model_source(self):
        f = self.fixture
        # A different representation is not the exact currently measured identity.
        f.cp.models['after']['source'] = dict(f.source, head='0' * 40)
        proposal = f.cp.proposal()
        saved_inputs = list(f.saved.values())
        f.saved = {ref['sha256']: inputs for ref, inputs in zip(f.cp.bundle['cases'], saved_inputs)}
        rebound_log = f.root / 'rebound-source-reviews.jsonl'
        rebound_log.write_text(json.dumps(f.cp.parent) + '\n')
        record = reports.append_review_record(rebound_log, proposal, original_row=f.cp.row)
        with self.assertRaisesRegex(ValueError, 'unbound_model_artifact'):
            runner.reevaluate_capability(self.output, review_log=rebound_log,
                validation_record_id=record['record_id'], original_row=f.cp.row, saved_cases=f.saved)
        self.assertFalse(self.output.exists())

    def signed_confirmation(self):
        # Ephemeral offline signing fixture; never a real user or runtime decision.
        host = signed_host.SignedHostFixture('runTest')
        host.setUp()
        self.addCleanup(host.doCleanups)
        host.row = self.fixture.cp.row
        host.reviews = reports.read_review_records(self.fixture.cp.case.log,
                                                   original_rows=[host.row])
        record = host.record()
        host.sign(record)
        host.append(record)
        return host, record

    def test_signed_fixture_log_is_bound_reauthenticated_and_never_promotes(self):
        host, record = self.signed_confirmation()
        report = self.run_saved(human_log=host.log, human_authority=host.host())
        view = report['capability_reconnect']
        self.assertEqual(view['activation_status'], 'fixture_validated')
        self.assertFalse(view['implemented'])
        self.assertFalse(view['human_confirmed'])
        evaluation = json.loads((self.output / 'capability-reevaluation.json').read_text())
        self.assertEqual(evaluation['human_record_refs'], [{
            'record_id': record['record_id'], 'record_sha256': record['record_sha256']}])
        self.assertTrue(reports.query_runner_evidence(
            self.output, human_authority=host.host())['evidence_valid'])
        self.assertFalse(reports.query_runner_evidence(self.output)['evidence_valid'])
        self.assertFalse(reports.query_runner_evidence(
            self.output, human_authority=lambda record: False)['evidence_valid'])

    def test_human_json_cannot_authenticate_its_own_claims(self):
        host, record = self.signed_confirmation()
        with self.assertRaisesRegex(ValueError, 'human_authority_unverified'):
            self.run_saved(human_log=host.log)
        self.assertFalse(self.output.exists())
        record['implemented'] = True
        record['human_authority'] = True
        host.log.write_text(json.dumps(record) + '\n')
        with self.assertRaises(ValueError):
            self.run_saved(human_log=host.log, human_authority=host.host())
        self.assertFalse(self.output.exists())

    def recorded_evaluation_fixture(self):
        # Adversarial relabel of offline data: "recorded" alone is no provenance.
        f = self.fixture
        saved_inputs = list(f.saved.values())
        f.cp.bundle['provenance'] = 'recorded_evaluation'
        proposal = f.cp.proposal()
        f.saved = {ref['sha256']: inputs for ref, inputs in zip(f.cp.bundle['cases'], saved_inputs)}
        f.cp.case.log = f.root / 'CP5-OFFLINE-recorded-label-reviews.jsonl'
        f.cp.case.log.write_text(json.dumps(f.cp.parent) + '\n')
        self.cp4.record = reports.append_review_record(f.cp.case.log, proposal, original_row=f.cp.row)

    def test_recorded_label_and_signed_confirmation_cannot_supply_real_source(self):
        self.recorded_evaluation_fixture()
        host, _ = self.signed_confirmation()
        report = self.run_saved(human_log=host.log, human_authority=host.host())
        view = report['capability_reconnect']
        self.assertEqual(view['evaluation_status'], 'evaluated')
        self.assertEqual(view['activation_status'], 'pending')
        self.assertIn('evaluation_source_not_frozen', view['implementation_pending_obligations'])
        self.assertIn('capture_source_not_qualified', view['implementation_pending_obligations'])
        self.assertFalse(view['implemented'])
        self.assertFalse(view['human_confirmed'])
        self.assertFalse(view['blackbox_supported'])
        self.assertEqual(report['candidates'], [])
        self.assertTrue(reports.query_runner_evidence(
            self.output, human_authority=host.host())['evidence_valid'])

    def test_existing_validation_review_is_reused_without_new_model_approval(self):
        report = self.run_saved()
        view = report['capability_reconnect']
        self.assertNotIn('current_independent_validation_review',
                         view['implementation_pending_obligations'])
        self.assertNotIn('model_activation_review', view['pending_obligations'])
        self.assertIn('human_confirmation', view['implementation_pending_obligations'])
        self.assertEqual(view['validation_review']['record_id'], self.cp4.record['record_id'])

    def test_revocation_invalidates_old_artifact_and_recomputes_new_fixture(self):
        host, confirm = self.signed_confirmation()
        self.run_saved(human_log=host.log, human_authority=host.host())
        old = {p: p.read_bytes() for p in self.output.iterdir()}
        revoke = host.record('fixture-revoke', 'revoke',
            supersedes_record_id=confirm['record_id'], revision_reason='Fixture withdrawal.')
        host.sign(revoke)
        host.append(revoke)
        self.assertFalse(reports.query_runner_evidence(
            self.output, human_authority=host.host())['evidence_valid'])
        self.output = self.fixture.root / 'after-fixture-revocation'
        report = self.run_saved(human_log=host.log, human_authority=host.host())
        view = report['capability_reconnect']
        self.assertFalse(view['human_confirmed'])
        self.assertFalse(view['implemented'])
        self.assertEqual([r['record_id'] for r in view['human_record_refs']],
                         [confirm['record_id'], revoke['record_id']])
        self.assertEqual(old, {p: p.read_bytes() for p in old})

    def test_changed_signature_and_broken_history_cannot_retain_confirmation(self):
        host, confirm = self.signed_confirmation()
        self.run_saved(human_log=host.log, human_authority=host.host())
        signature = host.signatures / (canonical_hash(confirm) + '.sig')
        signed_bytes = signature.read_bytes()
        signature.write_bytes(b'invalid offline fixture signature')
        self.assertFalse(reports.query_runner_evidence(
            self.output, human_authority=host.host())['evidence_valid'])
        signature.write_bytes(signed_bytes)
        broken = host.record('fixture-broken-revoke', 'revoke',
            supersedes_record_id='missing-confirmation', revision_reason='Broken fixture history.')
        host.sign(broken)
        with host.log.open('a') as stream:
            stream.write(json.dumps(broken) + '\n')
        self.output = self.fixture.root / 'invalid-history'
        with self.assertRaisesRegex(ValueError, 'human_'):
            self.run_saved(human_log=host.log, human_authority=host.host())
        self.assertFalse(self.output.exists())

    def test_trusted_signature_does_not_make_review_author_independent(self):
        host, _ = self.signed_confirmation()
        author = self.cp4.record['reviewer_id']
        host.policy_data['principals'][author] = copy.deepcopy(
            host.policy_data['principals']['fixture-human'])
        host.pin_policy()
        authored = host.record('authored-confirmation', human_id=author)
        host.sign(authored)
        self.assertTrue(host.host().verify(authored)['verified'])
        host.log.write_text(json.dumps(authored) + '\n')
        with self.assertRaisesRegex(ValueError, 'independent_supporting_review'):
            self.run_saved(human_log=host.log, human_authority=host.host())
        self.assertFalse(self.output.exists())

    def test_review_supersession_invalidates_old_and_rejects_stale_request(self):
        self.run_saved()
        f = self.fixture
        review = self.cp4.record
        reports.append_review_record(f.cp.case.log, dict(
            record_id='validation-withdrawn', candidate_id=review['candidate_id'],
            reviewer_id=review['reviewer_id'], conclusion='needs_evidence', attribution='unknown',
            evidence_refs=f.cp.case.refs, independent_of=review['independent_of'],
            reason='Offline fixture withdrawal.', supersedes_record_id=review['record_id'],
            revision_reason='Revisit the saved-case validation.'), original_row=f.cp.row)
        self.assertFalse(reports.query_runner_evidence(self.output)['evidence_valid'])
        self.output = f.root / 'stale-validation'
        with self.assertRaisesRegex(ValueError, 'reconnect_validation_review_superseded'):
            self.run_saved()
        self.assertFalse(self.output.exists())

    def test_rehashed_activation_authority_and_blackbox_claims_are_not_trusted(self):
        host, _ = self.signed_confirmation()
        self.run_saved(human_log=host.log, human_authority=host.host())
        original = json.loads((self.output / 'capability-reevaluation.json').read_text())
        for claim in ('activation', 'authority', 'record', 'source', 'sidechannel'):
            with self.subTest(claim=claim):
                evaluation = copy.deepcopy(original)
                if claim == 'activation':
                    evaluation['activation'].update(status='implemented', implemented=True,
                        human_confirmed=True, pending_obligations=[])
                elif claim == 'authority':
                    evaluation['request']['human_authority'] = {'verified': True}
                elif claim == 'record':
                    evaluation['human_record_refs'][0]['record_id'] = 'forged-authority'
                elif claim == 'source':
                    evaluation['evaluation_source']['source_frozen'] = True
                    evaluation['evaluation_source']['dirty_behavior_paths'] = []
                else:
                    evaluation['task_assessment']['blackbox_followup'] = {
                        'status': 'not_required', 'reason': 'Caller says supported.'}
                self.cp4.rehash_output(evaluation=evaluation)
                self.assertFalse(reports.query_runner_evidence(
                    self.output, human_authority=host.host())['evidence_valid'])

    def test_consistent_additional_review_reuses_validation_but_conflict_stays_pending(self):
        f = self.fixture
        proposal = dict(record_id='additional-independent-opinion',
            candidate_id=self.cp4.record['candidate_id'], reviewer_id='another-offline-reviewer',
            attribution='sut_candidate', conclusion='supported',
            independent_of=['trigger-author'], reason='Offline supporting opinion.',
            evidence_refs=f.cp.case.refs)
        additional = reports.append_review_record(f.cp.case.log, proposal, original_row=f.cp.row)
        view = self.run_saved()['capability_reconnect']
        self.assertNotIn('current_independent_validation_review',
                         view['implementation_pending_obligations'])
        reports.append_review_record(f.cp.case.log, dict(proposal,
            record_id='conflicting-independent-opinion', conclusion='needs_evidence',
            supersedes_record_id=additional['record_id'], revision_reason='Offline disagreement.'),
            original_row=f.cp.row)
        self.output = f.root / 'after-review-conflict'
        view = self.run_saved()['capability_reconnect']
        self.assertIn('current_independent_validation_review',
                      view['implementation_pending_obligations'])
        self.assertFalse(view['implemented'])

    def test_oracle_registration_does_not_implement_a_sidechannel_task(self):
        f = self.fixture
        saved_inputs = list(f.saved.values())
        f.cp.task['task_id'] = 'sidechannel-gap'
        proposal = f.cp.proposal()
        f.saved = {ref['sha256']: inputs for ref, inputs in zip(f.cp.bundle['cases'], saved_inputs)}
        f.cp.case.log = f.root / 'sidechannel-task-reviews.jsonl'
        f.cp.case.log.write_text(json.dumps(f.cp.parent) + '\n')
        self.cp4.record = reports.append_review_record(f.cp.case.log, proposal, original_row=f.cp.row)
        view = self.run_saved()['capability_reconnect']
        self.assertIn('unsupported_model_task_kind', view['implementation_pending_obligations'])
        self.assertFalse(view['implemented'])
        self.assertFalse(view['blackbox_supported'])
        self.assertIn('sidechannel_to_blackbox_followup', view['pending_obligations'])
