"""Negative harness controls use evidence gates without inventing mutation hits."""

import pathlib
import tempfile
import unittest

from r2f.core import CandidateLifecycle, ExecutionRecord, MutationReceipt, validate_control, validate_execution
from test_core_contracts import scenario
from test_contract_audit import executed, hit, observation


def fault_receipt():
    return {"fault_id": "late_endpoint_delivery_omission", "applied": True,
            "omitted_sequence": 1, "raw_event_ref": 17,
            "responsibility_layer": "harness", "candidate_eligible": False}


class FaultControlContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = pathlib.Path(self.tmp.name) / "raw.json"
        self.raw.write_text('{"matched":true}\n')

    def row(self):
        return dict(executed(self.raw, "negative"), control_kind="fault", oracle_verdict="fail",
                    fault_control_receipt=fault_receipt())

    def reached(self):
        s = scenario()
        s.arm = "negative"
        record = ExecutionRecord("fault-run", s)
        record.advance("expressible", adapter_accepted=True)
        record.advance("planned")
        record.advance("precondition_executed", precondition_receipts=self.row()["precondition_receipts"])
        record.advance("reached")
        return record

    def test_negative_fault_uses_audited_skip_without_mutation_hit(self):
        validate_execution(self.row())
        record = self.reached()
        row = self.row()
        record.advance("effect_observed", control_kind="fault", fault_control_receipt=fault_receipt(),
                       raw_path=row["raw_path"], raw_sha256=row["raw_sha256"], typed_facts=row["typed_facts"])
        record.advance("judged", oracle_verdict="fail")
        data = record.to_dict()
        self.assertTrue(validate_control(data))
        self.assertEqual(data["mutation_receipt"], MutationReceipt().to_dict())
        self.assertNotIn("mutation_applied", [step["execution_stage"] for step in data["stage_history"]])

    def test_fault_requires_complete_strict_receipt(self):
        valid = fault_receipt()
        for key in valid:
            broken = dict(valid)
            del broken[key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                validate_execution(dict(self.row(), fault_control_receipt=broken))
        for key, value in (("fault_id", "other"), ("applied", 1), ("applied", False),
                           ("omitted_sequence", True), ("omitted_sequence", -1), ("omitted_sequence", "1"),
                           ("raw_event_ref", None), ("raw_event_ref", " "), ("raw_event_ref", True),
                           ("responsibility_layer", "sut"), ("candidate_eligible", 0), ("candidate_eligible", True)):
            row = dict(self.row(), fault_control_receipt=dict(valid, **{key: value}))
            with self.subTest(field=key, value=value):
                with self.assertRaises(ValueError):
                    validate_execution(row)
                self.assertFalse(validate_control(row))

    def test_fault_never_enters_mutation_applied_or_forges_hit(self):
        for stage in ("mutation_applied", "effect_observed", "judged"):
            with self.subTest(stage=stage), self.assertRaises(ValueError):
                validate_execution(dict(self.row(), execution_stage=stage, mutation_receipt=hit()))
        with self.assertRaises(ValueError):
            self.reached().advance("mutation_applied", control_kind="fault", fault_control_receipt=fault_receipt())

    def test_control_tag_required_and_mutated_cannot_borrow_fault_path(self):
        for change in ({"control_kind": None}, {"control_kind": "health"}, {"arm": "mutated"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_execution(dict(self.row(), **change))
        with self.assertRaises(ValueError):
            self.reached().advance("effect_observed")

    def test_fault_control_pass_requires_judged_primary_failure(self):
        self.assertTrue(validate_control(self.row()))
        for change in ({"oracle_verdict": "pass"}, {"oracle_verdict": "unknown"},
                       {"execution_stage": "aborted"}, {"fault_control_receipt": None}):
            self.assertFalse(validate_control(dict(self.row(), **change)))

    def test_campaign_adapter_attributes_negative_control_to_harness(self):
        from r2f.campaign.triage import attribute
        facts = {"environment_healthy": True, "process_healthy": True, "preconditions_reached": True}
        oracle = {"verdict": "fail", "forbidden_residuals": ["post_match_delivery_loss"]}
        self.assertEqual(attribute(self.row(), facts, oracle), {
            "triage_disposition": "rejected", "responsibility_layer": "harness", "reason_code": "control_excluded"})
        broken = dict(self.row(), fault_control_receipt=dict(fault_receipt(), applied=False))
        self.assertEqual(attribute(broken, facts, oracle), {
            "triage_disposition": "rejected", "responsibility_layer": "harness", "reason_code": "tool_error"})
        for changes, reason in (({"environment_healthy": False}, "environment_or_process"),
                                ({"preconditions_reached": False}, "reach_or_planning")):
            self.assertEqual(attribute(self.row(), dict(facts, **changes), oracle)["reason_code"], reason)

    def test_negative_cannot_be_admitted_even_without_control_tag(self):
        for tag in (None, "fault"):
            evidence = dict(observation(self.raw), arm="negative", control_kind=tag,
                            fault_control_receipt=fault_receipt())
            candidate = CandidateLifecycle("negative", evidence)
            with self.subTest(tag=tag), self.assertRaisesRegex(ValueError, "control"):
                candidate.admit()


if __name__ == "__main__":
    unittest.main()
