"""Signed ingress contracts. Every key, instruction and SUT row here is a fixture."""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from r2f.core.candidate import candidate_execution_identity
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports import candidate_report as reports
from r2f.reports import run_report
from r2f.reports.human_host import HumanHost, HumanHostError, canonical_record_bytes
import test_fuzz_review as review_fixtures
from test_fuzz_audit import base_row, build_report


class SignedHostFixture(unittest.TestCase):
    """Data builders only: these are ephemeral fixture keys, never production trust."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='human-host-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        fixture = review_fixtures.ReviewPacketContracts(methodName='runTest')
        fixture.root = self.root
        self.row = fixture.row()
        self.reviews = fixture.signed_reviews(self.row, [{
            'candidate_id': 'audit-run:candidate-1', 'reviewer_id': 'independent-agent',
            'conclusion': 'supported', 'evidence_refs': fixture.refs(),
            'independent_of': ['trigger-author']}])
        self.log = self.root / 'human.jsonl'
        self.private = self.root / 'FIXTURE-ONLY-private.pem'
        self.public = self.root / 'FIXTURE-ONLY-public.pem'
        self.openssl('genpkey', '-algorithm', 'Ed25519', '-out', str(self.private))
        self.openssl('pkey', '-in', str(self.private), '-pubout', '-out', str(self.public))
        self.policy = self.root / 'FIXTURE-ONLY-policy.json'
        self.policy_data = {'schema_version': 'r2f.human-host-policy/v1', 'principals': {
            'fixture-human': {'public_key_path': str(self.public),
                              'public_key_sha256': raw_sha256(self.public),
                              'permitted_origins': ['user_instruction']}}}
        self.pin_policy()
        self.signatures = self.root / 'signatures'
        self.signatures.mkdir()

    def openssl(self, *args):
        return subprocess.run(['/usr/bin/openssl', *args], check=True, capture_output=True)

    def pin_policy(self):
        self.policy.write_text(json.dumps(self.policy_data), encoding='utf-8')
        self.policy_hash = raw_sha256(self.policy)

    def host(self, **changes):
        options = dict(policy_path=self.policy, expected_policy_sha256=self.policy_hash,
                       signature_store=self.signatures)
        options.update(changes)
        return HumanHost(**options)

    def record(self, record_id='fixture-confirm', action='confirm', **changes):
        instruction = self.root / (record_id + '.txt')
        instruction.write_text('FIXTURE ONLY: explicit ' + action + '; no real user decision.\n')
        record = {'schema_version': 'r2f.s1-human-adjudication/v1', 'record_id': record_id,
                  'candidate_id': 'audit-run:candidate-1', 'human_id': 'fixture-human',
                  'origin': 'user_instruction', 'action': action,
                  'reason': 'Offline signed fixture only; no actual SUT finding.',
                  'recorded_at': '2026-11-22T00:00:00+08:00',
                  'instruction_ref': str(instruction), 'instruction_sha256': raw_sha256(instruction),
                  'execution_identity': candidate_execution_identity(self.row)}
        record.update(changes)
        record['record_sha256'] = canonical_hash(record)
        return record

    def sign(self, record, *, key=None):
        # Independent serializer in the signing fixture, including record_sha256.
        data = json.dumps(record, sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False).encode('utf-8')
        payload = self.root / 'FIXTURE-ONLY-signing-input'
        payload.write_bytes(data)
        signature = self.signatures / (hashlib.sha256(data).hexdigest() + '.sig')
        self.openssl('pkeyutl', '-sign', '-rawin', '-inkey', str(key or self.private),
                     '-in', str(payload), '-out', str(signature))
        return signature

    def append(self, record, host=None):
        return reports.append_human_record(self.log, record, original_rows=[self.row],
                                          reviews=self.reviews, human_authority=host or self.host())

    def read(self, host=None):
        return reports.read_human_records(self.log, original_rows=[self.row],
                                         reviews=self.reviews, human_authority=host or self.host())

    def project(self, original, records, name):
        # Existing report/ledger format, not an alternative human ledger.
        ledger = reports.exploratory_candidate_ledger(
            original['rows'], reviews=original['review_records'], human_records=records,
            human_authority=self.host())
        ledger_path = self.root / (name + '-ledger.json')
        run_report.write_json(ledger_path, ledger)
        derived = dict(original, human_records=records, candidate_ledger_path=ledger_path.name,
                       candidate_ledger_sha256=canonical_hash(ledger))
        derived['report_sha256'] = run_report.exploratory_report_digest(derived)
        path = self.root / (name + '-report.json')
        run_report.write_json(path, derived)
        return path


class HumanHostContracts(SignedHostFixture):
    def test_signed_confirm_append_duplicate_fresh_audit_and_revoke(self):
        original_path, original = build_report(self.root, [self.row], reviews=self.reviews)
        original_bytes = original_path.read_bytes()
        confirm = self.record()
        self.sign(confirm)
        self.assertEqual(canonical_record_bytes(confirm), json.dumps(
            confirm, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())
        verified = self.host().verify(confirm)
        self.assertTrue(verified['verified'])
        self.assertEqual(verified['key_sha256'], raw_sha256(self.public))
        self.append(confirm)
        before = self.log.read_bytes()
        self.append(copy.deepcopy(confirm), self.host())
        self.assertEqual(self.log.read_bytes(), before)
        self.assertEqual(self.read(self.host()), [confirm])
        confirmed_path = self.project(original, self.read(), 'confirmed')
        audited = run_report.audit_report(confirmed_path, human_authority=self.host())
        self.assertTrue(audited['evidence_valid'], audited['errors'])
        self.assertEqual(audited['human_confirmed_candidates'], 1)
        self.assertFalse(audited['sealed'])
        revoke = self.record('fixture-revoke', 'revoke', supersedes_record_id=confirm['record_id'],
                             revision_reason='Fixture withdrawal.')
        self.sign(revoke)
        self.append(revoke)
        self.assertEqual(self.read(), [confirm, revoke])
        revoked_path = self.project(original, self.read(), 'revoked')
        revoked = run_report.audit_report(revoked_path, human_authority=self.host())
        self.assertTrue(revoked['evidence_valid'], revoked['errors'])
        self.assertEqual(revoked['human_confirmed_candidates'], 0)
        self.assertEqual(original_path.read_bytes(), original_bytes)

    def test_public_import_query_projects_existing_ledger_with_original_audit_proof(self):
        from r2f.reports.human_host import human_decision_view
        original_path, original = build_report(self.root, [self.row], reviews=self.reviews)
        originals = {p: p.read_bytes() for p in (original_path, self.root / 'candidate_ledger.json')}
        record = self.record()
        self.sign(record)
        imported = human_decision_view(original_path, human_log=self.log,
                                       human_authority=self.host(), record=record)
        self.assertEqual(imported['projection_kind'], 'canonical_human_ledger')
        self.assertTrue(imported['original_report']['audit']['evidence_valid'])
        self.assertEqual(imported['original_report']['raw_sha256'], raw_sha256(original_path))
        self.assertEqual(imported['ledger']['human_confirmed_candidates'], 1)
        self.assertEqual(imported['ledger']['root_cause_accounting']['independent_new_root_causes'], 0)
        reopened = human_decision_view(original_path, human_log=self.log, human_authority=self.host())
        self.assertEqual(reopened['ledger']['human_records'], imported['ledger']['human_records'])
        self.assertEqual([reports.audit_projection(c) for c in reopened['ledger']['candidates']],
                         [reports.audit_projection(c) for c in imported['ledger']['candidates']])
        revoke = self.record('fixture-revoke', 'revoke', supersedes_record_id=record['record_id'],
                             revision_reason='Fixture withdrawal.')
        self.sign(revoke)
        withdrawn = human_decision_view(original_path, human_log=self.log,
                                        human_authority=self.host(), record=revoke)
        self.assertEqual(withdrawn['ledger']['human_confirmed_candidates'], 0)
        self.assertEqual(withdrawn['ledger']['human_records'], [record, revoke])
        for path, data in originals.items():
            self.assertEqual(path.read_bytes(), data)

    def test_signature_covers_every_field_and_recomputed_digest(self):
        original = self.record(reason='Fixture Unicode decision: 撤销/确认')
        signature = self.sign(original).read_bytes()
        changes = {'action': 'revoke', 'candidate_id': 'other:cell', 'human_id': 'untrusted',
                   'origin': 'agent', 'reason': 'changed', 'record_id': 'other-id',
                   'recorded_at': '2026-11-23T00:00:00+08:00',
                   'instruction_ref': '/different/instruction', 'instruction_sha256': 'a' * 64,
                   'execution_identity': dict(original['execution_identity'], cell_id='other-cell'),
                   'schema_version': 'other-schema', 'extra': {'hidden': 'changed'}}
        for field, value in changes.items():
            with self.subTest(field=field):
                record = dict(original, **{field: value})
                record.pop('record_sha256')
                record['record_sha256'] = canonical_hash(record)
                (self.signatures / (canonical_hash(record) + '.sig')).write_bytes(signature)
                code = {'human_id': 'human_host_principal_untrusted',
                        'origin': 'human_host_origin_forbidden'}.get(field, 'human_host_signature_invalid')
                with self.assertRaisesRegex(HumanHostError, code):
                    self.host().verify(record)
        record = dict(original, record_sha256='0' * 64)
        (self.signatures / (canonical_hash(record) + '.sig')).write_bytes(signature)
        with self.assertRaisesRegex(HumanHostError, 'human_host_signature_invalid'):
            self.host().verify(record)

    def test_missing_bad_signature_and_wrong_signer_fail_closed(self):
        record = self.record()
        with self.assertRaisesRegex(HumanHostError, 'human_host_signature_unavailable'):
            self.host().verify(record)
        signature = self.sign(record)
        signature.write_bytes(b'invalid')
        with self.assertRaisesRegex(HumanHostError, 'human_host_signature_invalid'):
            self.host().verify(record)
        other = self.root / 'FIXTURE-ONLY-other-key.pem'
        self.openssl('genpkey', '-algorithm', 'Ed25519', '-out', str(other))
        self.sign(record, key=other)
        with self.assertRaisesRegex(HumanHostError, 'human_host_signature_invalid'):
            self.host().verify(record)
        with self.assertRaisesRegex(ValueError, 'human_authority_unverified'):
            self.append(record)
        self.assertEqual(self.log.read_bytes(), b'')

    def test_policy_and_key_are_reopened_and_pins_cannot_come_from_record(self):
        record = self.record()
        signature = self.sign(record)
        host = self.host()
        result = host.verify(record)
        self.assertEqual(result['signature_sha256'], raw_sha256(signature))
        original_policy = self.policy.read_bytes()
        self.policy.write_bytes(original_policy + b' ')
        with self.assertRaisesRegex(HumanHostError, 'human_host_policy_hash_mismatch'):
            host.verify(record)
        self.policy.write_bytes(original_policy)
        original_key = self.public.read_bytes()
        self.public.write_bytes(original_key + b'\n')
        with self.assertRaisesRegex(HumanHostError, 'human_host_key_hash_mismatch'):
            host.verify(record)
        self.public.unlink()
        with self.assertRaisesRegex(HumanHostError, 'human_host_key_unavailable'):
            host.verify(record)
        self.public.write_bytes(original_key)
        self.policy.unlink()
        with self.assertRaisesRegex(HumanHostError, 'human_host_policy_unavailable'):
            host.verify(record)
        self.policy.write_bytes(original_policy)
        with self.assertRaisesRegex(HumanHostError, 'human_host_policy_hash_mismatch'):
            self.host(expected_policy_sha256='0' * 64).verify(dict(record, trust_policy=original_policy.decode()))

    def test_explicit_principal_origin_and_ed25519_policy(self):
        for principal, origin, expected in (
                ('agent', 'user_instruction', 'human_host_principal_untrusted'),
                ('fixture-human', 'agent', 'human_host_origin_forbidden'),
                ('fixture-human', 'human_operation', 'human_host_origin_forbidden')):
            record = self.record(human_id=principal, origin=origin)
            self.sign(record)
            with self.subTest(principal=principal, origin=origin):
                with self.assertRaisesRegex(HumanHostError, expected):
                    self.host().verify(record)
        self.policy_data['principals']['fixture-human']['permitted_origins'] = ['agent']
        self.pin_policy()
        with self.assertRaisesRegex(HumanHostError, 'human_host_policy_invalid'):
            self.host().verify(self.record())
        self.policy_data['principals']['fixture-human']['permitted_origins'] = ['user_instruction']
        self.public.write_bytes(b'not a public key')
        self.policy_data['principals']['fixture-human']['public_key_sha256'] = raw_sha256(self.public)
        self.pin_policy()
        record = self.record()
        self.sign(record)
        with self.assertRaisesRegex(HumanHostError, 'human_host_key_not_ed25519'):
            self.host().verify(record)
        ec_private = self.root / 'FIXTURE-ONLY-ec.pem'
        self.openssl('genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256',
                     '-out', str(ec_private))
        self.openssl('pkey', '-in', str(ec_private), '-pubout', '-out', str(self.public))
        self.policy_data['principals']['fixture-human']['public_key_sha256'] = raw_sha256(self.public)
        self.pin_policy()
        with self.assertRaisesRegex(HumanHostError, 'human_host_key_not_ed25519'):
            self.host().verify(record)

    def test_explicit_paths_pins_and_openssl_dependency(self):
        record = self.record()
        self.sign(record)
        for options, expected in (
                ({'expected_policy_sha256': None}, 'human_host_policy_pin_invalid'),
                ({'policy_path': 'relative-policy'}, 'human_host_policy_path_invalid'),
                ({'signature_store': 'relative-store'}, 'human_host_signature_store_invalid'),
                ({'openssl_executable': 'openssl'}, 'human_host_openssl_path_invalid'),
                ({'openssl_executable': self.root / 'missing'}, 'human_host_openssl_unavailable')):
            with self.subTest(options=options):
                with self.assertRaisesRegex(HumanHostError, expected):
                    self.host(**options).verify(record)
        self.policy_data['principals']['fixture-human']['public_key_path'] = 'relative-key.pem'
        self.pin_policy()
        with self.assertRaisesRegex(HumanHostError, 'human_host_key_path_invalid'):
            self.host().verify(record)

    def test_path_shadow_cannot_supply_authenticity(self):
        marker = self.root / 'shadow-was-executed'
        fake = self.root / 'openssl'
        fake.write_text('#!/bin/sh\n/usr/bin/touch "' + str(marker) + '"\nexit 0\n')
        fake.chmod(0o700)
        record = self.record()
        signature = self.sign(record)
        original_path = os.environ.get('PATH')
        try:
            os.environ['PATH'] = str(self.root)
            self.assertTrue(self.host().verify(record)['verified'])
            signature.write_bytes(bytes(64))
            with self.assertRaisesRegex(HumanHostError, 'human_host_signature_invalid'):
                self.host().verify(record)
            self.assertFalse(marker.exists())
        finally:
            if original_path is None:
                os.environ.pop('PATH', None)
            else:
                os.environ['PATH'] = original_path

    def test_authenticated_record_still_needs_binding_instruction_and_eligibility(self):
        bad = self.record(execution_identity=dict(candidate_execution_identity(self.row), cell_id='other'))
        self.sign(bad)
        self.assertTrue(self.host().verify(bad)['verified'])
        with self.assertRaisesRegex(ValueError, 'human_execution_identity_mismatch'):
            self.append(bad)
        record = self.record()
        self.sign(record)
        Path(record['instruction_ref']).write_text('changed instruction')
        with self.assertRaises(ValueError):
            self.append(record)
        record = self.record()
        self.sign(record)
        ineligible = dict(self.row, arm='negative')
        record['execution_identity'] = candidate_execution_identity(ineligible)
        record.pop('record_sha256')
        record['record_sha256'] = canonical_hash(record)
        self.sign(record)
        with self.assertRaisesRegex(ValueError, 'human_candidate_not_eligible'):
            reports.append_human_record(self.log, record, original_rows=[ineligible],
                                        reviews=self.reviews, human_authority=self.host())
        self.assertEqual(self.log.read_bytes(), b'')

    def test_reopened_history_does_not_keep_confirmed_prefix_after_signature_loss(self):
        confirm = self.record()
        confirm_sig = self.sign(confirm)
        self.append(confirm)
        revoke = self.record('fixture-revoke', 'revoke', supersedes_record_id=confirm['record_id'],
                             revision_reason='Fixture withdrawal.')
        self.sign(revoke)
        self.append(revoke)
        original_path, original = build_report(self.root, [self.row], reviews=self.reviews)
        derived = self.project(original, self.read(), 'with-history')
        frozen_log = self.log.read_bytes()
        confirm_sig.unlink()
        with self.assertRaisesRegex(ValueError, 'human_authority_unverified'):
            self.read()
        result = run_report.audit_report(derived, human_authority=self.host())
        self.assertFalse(result['evidence_valid'])
        self.assertEqual(result['human_confirmed_candidates'], 0)
        self.assertEqual(self.log.read_bytes(), frozen_log)

    def test_no_policy_zero_candidate_query_and_missing_log(self):
        from r2f.reports.human_host import human_decision_view
        original_path, _ = build_report(self.root, [base_row(self.root, 'normal', verdict='pass')])
        view = human_decision_view(original_path)
        self.assertTrue(view['original_report']['audit']['evidence_valid'])
        self.assertEqual(view['ledger']['human_confirmed_candidates'], 0)
        self.assertEqual(view['ledger']['root_cause_accounting']['independent_new_root_causes'], 0)
        self.assertEqual(view['ledger']['human_records'], [])
        self.assertFalse(self.log.exists())
        with self.assertRaisesRegex(HumanHostError, 'human_host_log_unavailable'):
            human_decision_view(original_path, human_log=self.log)

    def test_verification_uses_copied_bytes_despite_original_path_substitution(self):
        record = self.record()
        signature = self.sign(record)
        old_key_hash, old_signature_hash = raw_sha256(self.public), raw_sha256(signature)
        wrapper = self.root / 'FIXTURE-ONLY-trusted-openssl-wrapper'
        wrapper.write_text(
            '#!/usr/bin/python3\nimport os,sys\nfrom pathlib import Path\n'
            'if sys.argv[1] == "pkey":\n'
            '    Path(' + repr(str(self.public)) + ').write_bytes(b"substituted key")\n'
            '    Path(' + repr(str(signature)) + ').write_bytes(bytes(64))\n'
            'os.execv("/usr/bin/openssl", ["/usr/bin/openssl", *sys.argv[1:]])\n')
        wrapper.chmod(0o700)
        host = self.host(openssl_executable=wrapper)
        result = host.verify(record)  # Actual /usr/bin/openssl verifies the copied bytes.
        self.assertTrue(result['verified'])
        self.assertEqual(result['key_sha256'], old_key_hash)
        self.assertEqual(result['signature_sha256'], old_signature_hash)
        with self.assertRaisesRegex(HumanHostError, 'human_host_key_hash_mismatch'):
            host.verify(record)

    def test_policy_malformed_duplicate_keys_and_nonregular_signature_rejected(self):
        record = self.record()
        self.sign(record)
        for policy in (b'{broken', b'[]', b'null',
                       b'{"schema_version":"r2f.human-host-policy/v1",'
                       b'"principals":{},"principals":{}}'):
            with self.subTest(policy=policy):
                self.policy.write_bytes(policy)
                with self.assertRaisesRegex(HumanHostError, 'human_host_policy_invalid'):
                    self.host(expected_policy_sha256=raw_sha256(self.policy)).verify(record)
        self.pin_policy()
        signature = self.signatures / (canonical_hash(record) + '.sig')
        signature.unlink()
        signature.mkdir()
        with self.assertRaisesRegex(HumanHostError, 'human_host_signature_unavailable'):
            self.host().verify(record)

    def test_valid_signature_cannot_override_same_id_conflict_or_invent_confirmation(self):
        record = self.record()
        self.sign(record)
        self.append(record)
        original = self.log.read_bytes()
        changed = dict(record, reason='Signed but conflicting fixture ID.')
        changed.pop('record_sha256')
        changed['record_sha256'] = canonical_hash(changed)
        self.sign(changed)
        self.assertTrue(self.host().verify(changed)['verified'])
        with self.assertRaisesRegex(ValueError, 'conflict'):
            self.append(changed)
        self.assertEqual(self.log.read_bytes(), original)
        denied = reports.exploratory_candidate_ledger([self.row], reviews=self.reviews,
                                                     human_records=[record])
        self.assertEqual(denied['human_confirmed_candidates'], 0)
        self.assertIn('human_authority_unverified', denied['human_errors'][0]['detail'])
