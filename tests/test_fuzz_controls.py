"""V24/V31: control scope and negative-control provenance in exploratory runs.

Controls are calibration evidence.  They are never natural fuzz inputs, never a
new coverage edge, and never a candidate.  A control may be referenced by many
rows inside one compatible scope, but it cannot be shared across source, RMW,
contract or recipe classes and it is counted once.
"""
import copy
import json
import pathlib
import tempfile
import unittest

from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports.candidate_report import exploratory_candidate_ledger
from r2f.reports.run_report import audit_report, write_json

from test_fuzz_audit import SOURCE, base_row, build_report


def control_ref(control_id, *, source=None, rmw="rmw_cyclonedds_cpp",
                contract="discovery.late_endpoint", recipe="health", cells=None,
                counts=False):
    return {"control_id": control_id,
            "source_sha256": canonical_hash(SOURCE) if source is None else source,
            "rmw": rmw, "target_contract": contract, "recipe": recipe,
            "applies": True, "raw_sha256": "d" * 64,
            "cell_ids": cells or [], "counts_as_exploratory_input": counts}


class ExploratoryControlContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def row_pair(self):
        rows = [base_row(self.root, "input-1", verdict="fail"),
                base_row(self.root, "control-1", arm="baseline", verdict="pass",
                         control_kind="health")]
        return rows

    def test_v24_control_shared_across_incompatible_rmw_is_rejected(self):
        rows = self.row_pair()
        rows.append(base_row(self.root, "control-2", arm="baseline", verdict="pass",
                             control_kind="health", rmw="rmw_fastrtps_cpp"))
        refs = [control_ref("shared-health", cells=["control-1"]),
                control_ref("shared-health", rmw="rmw_fastrtps_cpp", cells=["control-2"])]
        path, _ = build_report(self.root, rows, controls=refs)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("control_reference_shared_across_incompatible_contract" in e
                            for e in audit["errors"]), audit["errors"])

    def test_v24_control_shared_across_source_or_recipe_is_rejected(self):
        for field in ("source", "recipe", "contract"):
            with self.subTest(field=field):
                with tempfile.TemporaryDirectory() as temp:
                    root = pathlib.Path(temp)
                    rows = [base_row(root, "input-1", verdict="fail"),
                            base_row(root, "control-1", arm="baseline", verdict="pass",
                                     control_kind="health")]
                    refs = [control_ref("shared", cells=["control-1"])]
                    other = {"source": control_ref("shared", source="f" * 64, cells=["control-1"]),
                             "recipe": control_ref("shared", recipe="transparent", cells=["control-1"]),
                             "contract": control_ref("shared", contract="discovery.late_join_delivery",
                                                     cells=["control-1"])}[field]
                    refs.append(other)
                    path, _ = build_report(root, rows, controls=refs)
                    audit = audit_report(path)
                    self.assertFalse(audit["evidence_valid"])
                    self.assertTrue(any("shared_across_incompatible_contract" in e
                                        for e in audit["errors"]), audit["errors"])

    def test_control_reused_inside_one_scope_does_not_double_count_execution(self):
        rows = self.row_pair()
        rows.append(base_row(self.root, "input-2", verdict="pass"))
        refs = [control_ref("health-cyclone", cells=["control-1"])]
        path, report = build_report(self.root, rows, controls=refs)
        report["budget"]["control_executions"] = 1
        from r2f.reports.run_report import exploratory_report_digest
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertTrue(audit["evidence_valid"], audit["errors"])

    def test_v24_boolean_true_cannot_stand_in_for_a_control_reference(self):
        rows = self.row_pair()
        rows[0]["controls_support_sut"] = True
        rows[0]["provenance_supports_sut"] = True
        path, _ = build_report(self.root, rows, controls=[])
        report = json.loads(path.read_text())
        # The executed health control is not referenced; a bare boolean is not a reference.
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("unreferenced_control_execution" in e for e in audit["errors"]),
                        audit["errors"])
        ledger = exploratory_candidate_ledger(rows, reviews=report.get("review_records"))
        # Bare booleans never enter the ledger as a control reference.
        self.assertEqual(ledger["control_count"], 1)
        self.assertEqual(len(ledger["candidates"]), 1)
        self.assertEqual(ledger["candidates"][0]["review_status"], "not_reviewed")

    def test_v24_control_cannot_be_counted_as_exploratory_input(self):
        rows = self.row_pair()
        refs = [control_ref("health-cyclone", cells=["control-1"], counts=True)]
        path, _ = build_report(self.root, rows, controls=refs)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("control_counted_as_exploratory_input" in e for e in audit["errors"]),
                        audit["errors"])

    def test_v24_control_reference_requires_raw_and_applicability(self):
        rows = self.row_pair()
        ref = control_ref("health-cyclone", cells=["control-1"])
        ref["applies"] = False
        ref["raw_sha256"] = None
        path, _ = build_report(self.root, rows, controls=[ref])
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("control_reference_not_applicable" in e for e in audit["errors"]))
        self.assertTrue(any("control_reference_raw_identity_missing" in e for e in audit["errors"]))

    def test_v31_negative_oracle_fail_is_not_a_natural_candidate(self):
        rows = [base_row(self.root, "input-1", verdict="pass"),
                base_row(self.root, "negative-1", arm="negative", verdict="fail",
                         control_kind="fault", fault_control_receipt={
                             "fault_id": "post_restore_delivery_omission", "applied": True,
                             "omitted_sequence": 2, "raw_event_ref": 17,
                             "responsibility_layer": "harness", "candidate_eligible": False})]
        ledger = exploratory_candidate_ledger(rows)
        self.assertEqual(ledger["candidates"], [])
        self.assertEqual(ledger["natural_candidate_count"], 0)
        self.assertTrue(ledger["zero_candidate_explanation"])
        self.assertEqual(ledger["control_count"], 1)
        control = ledger["controls"][0]
        self.assertEqual(control["candidate_eligible"], False)
        self.assertEqual(control["source"], "harness")
        self.assertEqual(control["harness_fault"], "post_restore_delivery_omission")
        self.assertEqual(control["oracle_verdict"], "fail")

    def test_v31_negative_control_oracle_fail_survives_report_audit(self):
        rows = [base_row(self.root, "input-1", verdict="pass"),
                base_row(self.root, "negative-1", arm="negative", verdict="fail",
                         control_kind="fault", fault_control_receipt={
                             "fault_id": "post_restore_delivery_omission", "applied": True,
                             "omitted_sequence": 2, "raw_event_ref": 17,
                             "responsibility_layer": "harness", "candidate_eligible": False})]
        refs = [control_ref("fault-cyclone", recipe="fault", cells=["negative-1"])]
        path, _ = build_report(self.root, rows, controls=refs)
        audit = audit_report(path)
        self.assertTrue(audit["evidence_valid"], audit["errors"])
        self.assertEqual(audit["oracle_fail_count"], 0, "a negative control is not an SUT anomaly")
        ledger = json.loads((self.root / "candidate_ledger.json").read_text())
        self.assertEqual(ledger["candidates"], [])
        self.assertEqual(ledger["controls"][0]["source"], "harness")

    def test_negative_control_without_harness_provenance_fails_audit(self):
        # Fabricating a negative control without harness attribution must be caught.
        rows = [base_row(self.root, "input-1", verdict="pass"),
                base_row(self.root, "negative-1", arm="negative", verdict="fail",
                         control_kind="fault")]
        refs = [control_ref("fault-cyclone", recipe="fault", cells=["negative-1"])]
        path, report = build_report(self.root, rows, controls=refs)
        ledger = json.loads((self.root / "candidate_ledger.json").read_text())
        self.assertNotEqual(ledger["controls"][0]["source"], "harness")


if __name__ == "__main__":
    unittest.main()
