"""R001: stable candidate identity must bind to the endpoint facts."""

import copy
import pathlib
import tempfile
import unittest

from r2f.core import CandidateLifecycle
from test_contract_audit import observation


class CandidateIdentityContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = pathlib.Path(self.tmp.name) / "raw.json"
        self.raw.write_text('{"matched":true}\n')
        self.endpoints = {
            "writer": {"id": "writer-gid", "generation": 0, "created_ns": 10},
            "reader": {"id": "reader-gid", "generation": 0, "created_ns": 20},
        }

    def evidence(self):
        return dict(observation(self.raw), endpoint_identity=copy.deepcopy(self.endpoints),
                    typed_facts={"matched": True, **copy.deepcopy(self.endpoints)})

    def test_empty_or_malformed_endpoint_members_are_rejected(self):
        malformed = [None, {}, [], "", " ", False, 1, {"writer": None, "reader": None},
                     {"writer": {}, "reader": {}}, {"writer": "w", "reader": "r"},
                     {"writer": self.endpoints["writer"]}]
        for field, value in (("id", None), ("id", ""), ("id", " "), ("id", []),
                             ("generation", None), ("generation", True), ("generation", "0"),
                             ("generation", -1), ("extra", {"identity": None}), ("extra", [])):
            identity = copy.deepcopy(self.endpoints)
            identity["writer"][field] = value
            malformed.append(identity)
        for identity in malformed:
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                CandidateLifecycle("invalid", dict(self.evidence(), endpoint_identity=identity)).admit()

    def test_structured_identity_requires_matching_typed_endpoint_facts(self):
        CandidateLifecycle("valid", self.evidence()).admit()
        for role, field, value in (("writer", "id", "wrong-gid"), ("reader", "id", "wrong-gid"),
                                   ("writer", "generation", 1), ("reader", "generation", True)):
            evidence = self.evidence()
            evidence["endpoint_identity"][role][field] = value
            with self.subTest(role=role, field=field), self.assertRaises(ValueError):
                CandidateLifecycle("mismatch", evidence).admit()
        for facts in ({"matched": True}, {"writer": None, "reader": None}):
            with self.subTest(facts=facts), self.assertRaises(ValueError):
                CandidateLifecycle("unbound", dict(self.evidence(), typed_facts=facts)).admit()

    def test_runtime_string_generation_survives_report_ledger(self):
        from r2f.reports.candidate_report import candidate_ledger
        evidence = self.evidence()
        for role, generation in (("writer", "b1977d27dd074d0a838b8e66f9c8be46"),
                                 ("reader", "9a472cdf0a06494f9a95f5167a06cdf4")):
            evidence["endpoint_identity"][role]["generation"] = generation
            evidence["typed_facts"][role]["generation"] = generation
        evidence.update(cell_id="generation-regression", oracle_verdict="fail",
                        triage_disposition="deferred", responsibility_layer="observation",
                        reason_code="attribution_insufficient")
        ledger = candidate_ledger([evidence])
        self.assertEqual(len(ledger["candidates"]), 1, ledger["failures"])
        self.assertIsNone(ledger["candidates"][0]["admission_error"])
        self.assertEqual(ledger["candidates"][0]["candidate_status"], "deferred")
        for generation in ("", " ", True, -1, "another-generation", 0):
            mismatched = copy.deepcopy(evidence)
            mismatched["endpoint_identity"]["writer"]["generation"] = generation
            with self.subTest(generation=generation), self.assertRaises(ValueError):
                CandidateLifecycle("mismatch", mismatched).admit()
        numeric_string = self.evidence()
        numeric_string["endpoint_identity"]["writer"]["generation"] = "0"
        numeric_string["typed_facts"]["writer"]["generation"] = "0"
        CandidateLifecycle("string-zero", numeric_string).admit()
        numeric_string["typed_facts"]["writer"]["generation"] = 0
        with self.assertRaises(ValueError):
            CandidateLifecycle("different-types", numeric_string).admit()

    def test_legacy_string_is_supported_and_binds_when_endpoint_facts_exist(self):
        CandidateLifecycle("legacy", observation(self.raw)).admit()
        evidence = dict(self.evidence(), endpoint_identity="writer-gid/reader-gid")
        CandidateLifecycle("bound-string", evidence).admit()
        evidence["endpoint_identity"] = "wrong-gid/reader-gid"
        with self.assertRaises(ValueError):
            CandidateLifecycle("wrong-string", evidence).admit()

    def test_invalid_identity_cannot_hide_behind_another_valid_identity(self):
        evidence = dict(self.evidence(), endpoint_identity={"writer": None, "reader": None},
                        instrumentation_identity="instrumentation-1")
        with self.assertRaises(ValueError):
            CandidateLifecycle("bad-endpoints", evidence)
        for identity in ({"id": None}, {"id": ""}, {"other": "value"}, ["instrumentation-1"]):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                CandidateLifecycle("bad-instrumentation", dict(self.evidence(), instrumentation_identity=identity))

    def test_observed_sample_identity_mismatch_is_not_discarded(self):
        evidence = self.evidence()
        evidence["typed_facts"]["received"] = [{"writer_id": "unexpected-writer-gid", "sequence": 1}]
        candidate = CandidateLifecycle("observed-mismatch", evidence)
        candidate.admit()
        self.assertEqual(candidate.candidate_status, "admitted")


if __name__ == "__main__":
    unittest.main()
