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

from r2f.reports.candidate_report import (
    REVIEW_PACKET_SCHEMA, REVIEW_RECORD_SCHEMA,
    append_review_record, exploratory_candidate_ledger, read_review_records,
    review_packet, write_review_packet,
)
from r2f.reports.run_report import audit_report, write_json

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
        row.update(changes)
        return row

    def refs(self):
        capsule = self.root / "capsule.json"
        if not capsule.exists():
            write_json(capsule, {"schema_version": "fixture"})
        source = self.root / "source_commit.txt"
        if not source.exists():
            source.write_text("e448feb\n")
        return {"source": str(source), "raw": self.row()["raw_path"], "capsule": str(capsule)}

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
                    append_review_record(self.root / "reviews.jsonl", dict(review, candidate_id=row["run_id"] + ":" + row["cell_id"]))
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
        ledger = exploratory_candidate_ledger([row], reviews=reviews)
        candidate = ledger["candidates"][0]
        self.assertEqual(candidate["candidate_status"], "confirmed", candidate.get("confirmation_error"))
        self.assertEqual(candidate["review_status"], "confirmed_after_review")
        self.assertEqual(ledger["confirmed_candidates"], 1)

    def test_review_log_is_append_only_and_digest_checked(self):
        log = self.root / "reviews.jsonl"
        base = {"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                "conclusion": "needs_evidence", "evidence_refs": self.refs(),
                "independent_of": ["trigger-author"], "notes": "need window evidence"}
        first = append_review_record(log, dict(base))
        second = append_review_record(log, dict(base, conclusion="supported"))
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

    def test_audit_rejects_unreferenced_review_and_keeps_evidence_valid_separation(self):
        rows = [self.row()]
        reviews = [{"candidate_id": "audit-run:candidate-1", "reviewer_id": "reviewer-1",
                    "conclusion": "supported", "evidence_refs": self.refs(),
                    "independent_of": ["trigger-author"]}]
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
        self.assertEqual(audit["rejected_reviews"][0]["reason"], "review_without_evidence_references")
        ledger = json.loads((self.root / "candidate_ledger.json").read_text())
        self.assertEqual(ledger["confirmed_candidates"], 0)
        self.assertNotEqual(ledger["candidates"][0]["review_status"], "confirmed_after_review")
        self.assertTrue(ledger["review_errors"])


if __name__ == "__main__":
    unittest.main()
