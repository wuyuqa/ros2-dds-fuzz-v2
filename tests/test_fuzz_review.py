"""V36: review packets and append-only reviewer records.

A packet is evidence for a reviewer.  An independent reviewer's ``supported``
opinion is not a confirmation.  Without a named reviewer and concrete
source/raw/capsule references, a candidate can neither be confirmed nor have its
review status elevated.  Fixtures are synthetic; none is a real SUT defect.
"""
import copy
import json
import pathlib
import tempfile
import unittest

from r2f.core import CandidateLifecycle, Registry
from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import MutationProposal, compose, mutation_plan_noop_reason
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import register_exploratory

from r2f.reports.candidate_report import (
    REVIEW_PACKET_SCHEMA, REVIEW_RECORD_SCHEMA,
    append_review_record, exploratory_candidate_ledger, read_review_records,
    review_packet, write_review_packet,
)
from r2f.reports.run_report import audit_report, exploratory_report_digest, write_json

from test_fuzz_audit import base_row, build_report


class ReviewPacketContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def row(self, **changes):
        row = base_row(self.root, "candidate-1", verdict="fail",
                       supporting_rule_ids=["discovery.late_endpoint.match_required"],
                       mutator_id="M01", window_complete=True,
                       negative_explanations=["negative control explains delivery loss"])
        # The shared audit fixture's abbreviated plan is intentionally not an
        # admissible MutationPlan.  Review tests need to reach triage before
        # testing the independent-review gate, so supply a structurally valid,
        # explicitly effective plan.  All receipts/raw remain synthetic; this
        # exercises lifecycle contracts, not a naturally observed SUT defect.
        registry = Registry()
        register_exploratory(registry, include_required=False)
        contract = registry.get_target("discovery.late_endpoint.fuzz_v1").exploration_contract
        parent = copy.deepcopy(fuzz_catalog.load_seeds()["seeds"]["P01"]["logical_config"])
        parent["reader_delay_sec"] = 0.15
        after = dict(parent, reader_delay_sec=0.2)
        proposal = MutationProposal(
            "P01", "M01", {"boundary_class": "finite"},
            {"reader_delay_sec": 0.2}, parent, after, "compile_time",
            {"status": "applicable"})
        plan = compose(parent, [proposal], contract, rng_seed=0)
        self.assertIsNone(mutation_plan_noop_reason(plan))
        row.update(mutation_plan=plan.to_dict(), mutation_plan_sha256=canonical_hash(plan.to_dict()))
        row.update(changes)
        return row

    def test_original_abbreviated_plan_is_rejected_before_sut_triage(self):
        row = base_row(self.root, "invalid-original", verdict="fail")
        candidate = exploratory_candidate_ledger([row])["candidates"][0]
        self.assertEqual(candidate["candidate_status"], "observed")
        self.assertIn("mutation_plan missing field(s)", candidate["admission_error"])
        self.assertNotIn("triaged", [event["event"] for event in candidate["history"]])

    def test_equal_parent_plan_cannot_borrow_fixture_hit_or_supporting_review(self):
        row = self.row()
        data = copy.deepcopy(row["mutation_plan"])
        # Retain the effective fixture's final config as an explicit parent:
        # this is a valid descriptive plan, but not an effective mutation.
        registry = Registry()
        register_exploratory(registry, include_required=False)
        contract = registry.get_target("discovery.late_endpoint.fuzz_v1").exploration_contract
        parent = copy.deepcopy(data["final_config"])
        proposal = MutationProposal("P01", "M01", {"boundary_class": "finite"},
                                    {"reader_delay_sec": 0.2}, parent, parent,
                                    "compile_time", {"status": "applicable"})
        plan = compose(parent, [proposal], contract)
        self.assertIsNotNone(mutation_plan_noop_reason(plan))
        row.update(mutation_plan=plan.to_dict(), mutation_plan_sha256=canonical_hash(plan.to_dict()))
        row["confirmation"] = {"adjudicator_id": "reviewer-1",
                               "adjudication_method": "peer_review",
                               "confirmation_basis": "evidence_packet",
                               "reason": "synthetic review gate regression"}
        reviews = [{"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                    "conclusion": "supported", "evidence_refs": self.refs(),
                    "independent_of": ["trigger-author"]}]
        reviews = self.signed_reviews(row, reviews)
        ledger = exploratory_candidate_ledger([row], reviews=reviews)
        candidate = ledger["candidates"][0]
        self.assertEqual(candidate["candidate_status"], "observed")
        self.assertEqual(ledger["natural_candidate_count"], 0)
        self.assertEqual(ledger["confirmed_candidates"], 0)
        self.assertIn("effective input generation", candidate["admission_error"])
        self.assertIn("sut triage", candidate["confirmation_error"].lower())
        self.assertNotIn("triaged", [event["event"] for event in candidate["history"]])

    def test_effective_fixture_reaches_triage_but_raw_and_provenance_stay_required(self):
        row = self.row()
        candidate = exploratory_candidate_ledger([row])["candidates"][0]
        self.assertIsNone(candidate["admission_error"])
        self.assertEqual(candidate["candidate_status"], "triaged")
        self.assertEqual(candidate["responsibility_layer"], "sut")
        invalid = copy.deepcopy(row)
        invalid["provenance"] = {}
        with self.assertRaisesRegex(ValueError, "provenance"):
            CandidateLifecycle("missing-provenance", invalid).admit()
        pathlib.Path(row["raw_path"]).write_text('{"event":"tampered"}\n')
        with self.assertRaisesRegex(ValueError, "SHA256|raw"):
            CandidateLifecycle("tampered-raw", row).admit()

    def refs(self):
        capsule = self.root / "capsule.json"
        if not capsule.exists():
            write_json(capsule, {"schema_version": "fixture"})
        source = self.root / "source_commit.txt"
        if not source.exists():
            source.write_text("e448feb\n")
        return {"source": str(source), "raw": self.row()["raw_path"], "capsule": str(capsule)}

    def signed_reviews(self, row, reviews):
        return [append_review_record(
            self.root / "reviews.jsonl",
            dict(review, attribution="sut_candidate", reason="Offline review gate fixture"),
            original_row=row) for review in reviews]

    def test_packet_contains_required_identity_and_is_not_a_confirmation(self):
        packet = review_packet(self.row(), identity={"source_sha256": "1" * 64,
                                                    "target_sha256": "2" * 64,
                                                    "rule_sha256": "3" * 64,
                                                    "profile_sha256": "4" * 64,
                                                    "model_sha256": "5" * 64})
        for key in ("schema_version", "packet_id", "identity", "parent", "input", "chain",
                    "layer_receipts", "raw", "rules", "facts", "controls",
                    "observation_health", "negative_explanations", "replay", "dedupe"):
            self.assertIn(key, packet)
        self.assertEqual(packet["schema_version"], REVIEW_PACKET_SCHEMA)
        self.assertEqual(packet["identity"]["mutator"], "M01")
        self.assertEqual(packet["rules"]["supporting"], ["discovery.late_endpoint.match_required"])
        self.assertEqual(packet["raw"]["raw_sha256"], self.row()["raw_sha256"])
        self.assertEqual(packet["review_status"], "awaiting_independent_review")
        # A packet is not a lifecycle object and carries no confirmation field.
        self.assertNotIn("candidate_status", packet)
        self.assertNotIn("confirmed", json.dumps(packet))

    def test_packet_records_missing_evidence_explicitly(self):
        row = self.row()
        row.pop("raw_path")
        packet = review_packet(row)
        self.assertIn("raw", packet["incomplete_fields"])
        self.assertIn("no replay artifact", packet["replay"]["not_executed_reason"])

    def test_packet_write_is_atomic_and_never_overwrites(self):
        path = self.root / "packets" / "p1.json"
        written = write_review_packet(path, review_packet(self.row()))
        self.assertEqual(written, path)
        first = path.read_bytes()
        with self.assertRaises(FileExistsError):
            write_review_packet(path, review_packet(self.row()))
        self.assertEqual(path.read_bytes(), first)
        self.assertEqual(json.loads(path.read_text())["schema_version"], REVIEW_PACKET_SCHEMA)
        with self.assertRaises(ValueError):
            write_review_packet(self.root / "bad.json", {"schema_version": "other"})

    def test_v36_missing_reviewer_or_evidence_cannot_raise_status(self):
        row = self.row()
        for review in (
                {"conclusion": "supported", "evidence_refs": self.refs()},
                {"reviewer_id": "reviewer-1", "conclusion": "supported"},
                {"reviewer_id": "reviewer-1", "conclusion": "supported",
                 "evidence_refs": {"source": str(self.root / "source_commit.txt")}},
                {"reviewer_id": "reviewer-1", "conclusion": "confirmed", "evidence_refs": self.refs()},
                {"reviewer_id": "author", "conclusion": "supported", "evidence_refs": self.refs(),
                 "independent_of": ["author"]}):
            with self.subTest(review=review):
                with self.assertRaises(ValueError):
                    append_review_record(self.root / "reviews.jsonl", dict(review, candidate_id=row["run_id"] + ":" + row["cell_id"]), original_row=row)
        ledger = exploratory_candidate_ledger([row], reviews=[
            {"candidate_id": "audit-run:candidate-1", "reviewer_id": "",
             "conclusion": "supported", "evidence_refs": {}}])
        self.assertIn(ledger["candidates"][0]["candidate_status"], {"observed", "triaged"})
        self.assertNotEqual(ledger["candidates"][0]["candidate_status"], "confirmed")
        self.assertEqual(ledger["candidates"][0]["review_status"], "not_reviewed")
        self.assertTrue(ledger["review_errors"])

    def test_v36_supported_review_does_not_confirm_by_itself(self):
        row = self.row()
        reviews = [{"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                    "conclusion": "supported", "evidence_refs": self.refs(),
                    "independent_of": ["trigger-author"]}]
        reviews = self.signed_reviews(row, reviews)
        ledger = exploratory_candidate_ledger([row], reviews=reviews)
        candidate = ledger["candidates"][0]
        self.assertNotEqual(candidate["candidate_status"], "confirmed")
        self.assertEqual(candidate["review_status"], "supported_awaiting_adjudication")
        self.assertEqual(ledger["confirmed_candidates"], 0)

    def test_v36_confirmation_requires_a_review_even_with_replay_basis(self):
        row = self.row(confirmation={"adjudicator_id": "adjudicator",
                                     "adjudication_method": "peer_review",
                                     "confirmation_basis": "evidence_packet",
                                     "reason": "operator adjudication"})
        ledger = exploratory_candidate_ledger([row])
        candidate = ledger["candidates"][0]
        self.assertNotEqual(candidate["candidate_status"], "confirmed")
        self.assertIn("review", (candidate.get("confirmation_error") or "").lower())

    def test_v36_confirmation_with_independent_supporting_review_is_allowed(self):
        row = self.row(confirmation={"adjudicator_id": "reviewer-1",
                                     "adjudication_method": "peer_review",
                                     "confirmation_basis": "evidence_packet",
                                     "reason": "operator adjudication"})
        reviews = [{"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                    "conclusion": "supported", "evidence_refs": self.refs(),
                    "independent_of": ["trigger-author"]}]
        reviews = self.signed_reviews(row, reviews)
        ledger = exploratory_candidate_ledger([row], reviews=reviews)
        candidate = ledger["candidates"][0]
        self.assertEqual(candidate["candidate_status"], "confirmed", candidate.get("confirmation_error"))
        self.assertEqual(candidate["review_status"], "confirmed_after_review")
        self.assertEqual(ledger["confirmed_candidates"], 1)
        self.assertEqual(ledger["human_confirmed_candidates"], 0)
        self.assertIsNone(candidate["human_adjudication"]["current"])

    def test_review_log_is_append_only_and_digest_checked(self):
        log = self.root / "reviews.jsonl"
        base = {"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                "conclusion": "needs_evidence", "evidence_refs": self.refs(),
                "independent_of": ["trigger-author"], "notes": "need window evidence"}
        row = self.row()
        base.update(attribution="unknown", reason="Offline review gate fixture")
        first = append_review_record(log, dict(base), original_row=row)
        second = append_review_record(log, dict(base, conclusion="supported",
                                      supersedes_record_id=first["record_id"],
                                      revision_reason="Additional window evidence"), original_row=row)
        self.assertEqual(first["schema_version"], REVIEW_RECORD_SCHEMA)
        self.assertNotEqual(first["record_sha256"], second["record_sha256"])
        records = read_review_records(log)
        self.assertEqual([r["conclusion"] for r in records], ["needs_evidence", "supported"])
        before = log.read_text()
        a = json.loads(before.splitlines()[0])
        a["notes"] = "tampered"
        log.write_text(json.dumps(a) + "\n" + before.splitlines()[1] + "\n")
        with self.assertRaises(ValueError):
            read_review_records(log)

    def test_audit_rejects_persisted_review_history_and_rejection_tampering(self):
        row = self.row()
        log = self.root / "reviews.jsonl"
        review = {"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                  "conclusion": "supported", "attribution": "sut_candidate",
                  "reason": "Synthetic offline review", "evidence_refs": self.refs()}
        first = append_review_record(log, review, original_row=row)
        revised = append_review_record(log, dict(
            review, conclusion="needs_evidence", supersedes_record_id=first["record_id"],
            revision_reason="Additional evidence needed"), original_row=row)
        rejected = dict(revised, record_sha256="0" * 64)
        path, report = build_report(self.root, [row], reviews=[first, revised, rejected])
        ledger_path = self.root / "candidate_ledger.json"
        ledger = json.loads(ledger_path.read_text())
        self.assertEqual(ledger["review_records"], [first, revised])
        self.assertEqual(ledger["confirmed_candidates"], 0)
        audit = audit_report(path)
        self.assertTrue(audit["evidence_valid"], audit["errors"])
        self.assertEqual(audit["accepted_reviews"], 2)
        self.assertEqual(audit["rejected_reviews"], [{
            "index": 2, "candidate_id": "audit-run:candidate-1",
            "reason": "review_record_digest_mismatch"}])
        original = {p: p.read_bytes() for p in (
            path, ledger_path, log, pathlib.Path(row["raw_path"]))}
        for name, key, value in (
                ("dropped-history", "review_records", []),
                ("dropped-superseded", "review_records", [revised]),
                ("forged-history", "review_records", [dict(first, reason="Forged opinion"), revised]),
                ("invented-history", "review_records", [first, revised, first]),
                ("dropped-rejections", "review_errors", []),
                ("forged-rejection", "review_errors", [dict(
                    ledger["review_errors"][0], detail="Forged rejection")]),
                ("invented-rejection", "review_errors", ledger["review_errors"] + [dict(
                    ledger["review_errors"][0], index=3)])):
            with self.subTest(case=name):
                derived_ledger = dict(ledger, **{key: value})
                derived_ledger_path = self.root / (name + "-ledger.json")
                write_json(derived_ledger_path, derived_ledger)
                derived = dict(report, candidate_ledger_path=derived_ledger_path.name,
                               candidate_ledger_sha256=canonical_hash(derived_ledger))
                derived["report_sha256"] = exploratory_report_digest(derived)
                derived_path = self.root / (name + "-report.json")
                write_json(derived_path, derived)
                before = {p: p.read_bytes() for p in (derived_path, derived_ledger_path)}
                result = audit_report(derived_path)
                self.assertFalse(result["evidence_valid"], result["errors"])
                self.assertIn("exploratory_ledger_mismatch:" + key, result["errors"])
                self.assertEqual(result["accepted_reviews"], 2)
                self.assertEqual(result["rejected_reviews"], audit["rejected_reviews"])
                self.assertEqual({p: p.read_bytes() for p in before}, before)
                self.assertEqual({p: p.read_bytes() for p in original}, original)

    def test_audit_rejects_unreferenced_review_and_keeps_evidence_valid_separation(self):
        rows = [self.row()]
        reviews = [{"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                    "conclusion": "supported", "evidence_refs": self.refs(),
                    "independent_of": ["trigger-author"]}]
        reviews = self.signed_reviews(rows[0], reviews)
        path, _ = build_report(self.root, rows, reviews=reviews)
        audit = audit_report(path)
        self.assertTrue(audit["evidence_valid"], audit["errors"])
        # Review support alone must not create a confirmation.
        ledger = json.loads((self.root / "candidate_ledger.json").read_text())
        self.assertEqual(ledger["confirmed_candidates"], 0)
        self.assertEqual(ledger["natural_candidate_count"], 1)

    def test_audit_rejects_review_without_evidence_but_report_stays_valid(self):
        rows = [self.row()]
        reviews = [{"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                    "conclusion": "supported", "evidence_refs": {}}]
        path, _ = build_report(self.root, rows, reviews=reviews)
        audit = audit_report(path)
        # evidence_valid describes the report/evidence chain, not whether every
        # oracle passed or whether a review opinion was acceptable.
        self.assertTrue(audit["evidence_valid"], audit["errors"])
        self.assertEqual(audit["accepted_reviews"], 0)
        self.assertIn("review_without_evidence_references", audit["rejected_reviews"][0]["reason"])
        ledger = json.loads((self.root / "candidate_ledger.json").read_text())
        self.assertEqual(audit["rejected_reviews"][0]["reason"], ledger["review_errors"][0]["detail"])
        self.assertEqual(ledger["confirmed_candidates"], 0)
        self.assertNotEqual(ledger["candidates"][0]["review_status"], "confirmed_after_review")
        self.assertTrue(ledger["review_errors"])


    def test_projection_uses_run_and_cell_for_candidates_failures_and_controls(self):
        rows, reviews, expected = [], [], {}
        base = self.row()
        for run_id in ('audit-run', 'second-run'):
            for kind in ('candidate', 'failure', 'control'):
                cell_id = kind + '-1'
                raw = self.root / (run_id + '-' + kind + '.jsonl')
                raw.write_text(json.dumps({'offline': run_id, 'kind': kind}) + '\n')
                from r2f.core.evidence import raw_sha256
                row = dict(base, run_id=run_id, cell_id=cell_id, raw_path=str(raw),
                           raw_sha256=raw_sha256(raw), reason_code=run_id + '-' + kind)
                if kind == 'failure':
                    row.update(tool_error=True, runtime_status='tool_error')
                elif kind == 'control':
                    row.update(control_kind='health', arm='baseline')
                identity = run_id + ':' + cell_id
                review = append_review_record(self.root / (run_id + '-' + kind + '-review.jsonl'), {
                    'candidate_id': identity, 'record_id': identity, 'reviewer_id': 'independent-agent',
                    'conclusion': 'supported', 'attribution': 'sut_candidate', 'reason': 'Offline fixture.',
                    'evidence_refs': dict(self.refs(), raw=str(raw)), 'independent_of': ['trigger-author']},
                    original_row=row)
                rows.append(row)
                reviews.append(review)
                expected[identity] = (row, review)
        for ordered_rows in (rows, list(reversed(rows))):
            ledger = exploratory_candidate_ledger(ordered_rows, reviews=reviews)
            self.assertFalse(ledger['review_errors'])
            for bucket in ('candidates', 'failures', 'controls'):
                self.assertEqual(len(ledger[bucket]), 2)
                for entry in ledger[bucket]:
                    identity = entry['run_id'] + ':' + entry['cell_id']
                    row, review = expected[identity]
                    self.assertEqual(entry['original_result']['run_id'], row['run_id'])
                    self.assertEqual(entry['original_result']['cell_id'], row['cell_id'])
                    self.assertEqual(entry['original_result']['raw_path'], row['raw_path'])
                    self.assertEqual(entry['original_result']['raw_sha256'], row['raw_sha256'])
                    self.assertEqual(entry['original_result']['reason_code'], row['reason_code'])
                    self.assertEqual(entry['analysis'], {'current': review, 'history': [review]})
                    if bucket == 'candidates':
                        self.assertEqual(entry['candidate_id'], identity)
                        self.assertEqual(entry['evidence']['raw_sha256'], row['raw_sha256'])


if __name__ == "__main__":
    unittest.main()
