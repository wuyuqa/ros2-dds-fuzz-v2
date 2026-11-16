"""V25/V32/V33/V34/V35: exploratory report audit is independent and artifact-bound.

Fixtures in this file are synthetic report artefacts. They never represent a
real SUT defect and can never produce a sealed or mature report.
"""
import copy
import json
import pathlib
import tempfile
import unittest

from r2f.campaign.scheduler import required_catalog_hash
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.modules.s1.fuzz_catalog import catalog_digest
from r2f.reports.candidate_report import exploratory_candidate_ledger
from r2f.reports.run_report import (
    EXPLORATORY_SCHEMA, audit_exploratory_report, audit_report,
    exploratory_report_digest, write_json,
)
from test_contract_audit import executed
from test_core_contracts import scenario, target as core_target

PROFILE = {
    "schema_version": "r2f.s1-fuzz-profile/v1",
    "profile_id": "s1-fuzz-parameter",
    "campaign_kind": "exploratory",
    "enabled": False,
    "required_catalog_included": False,
    "seed_catalog_status": "pending_t02",
    "target_ids": ["discovery.late_endpoint", "discovery.late_join_delivery"],
    "surfaces": ["P01", "W02"],
    "rmws": ["rmw_cyclonedds_cpp", "rmw_fastrtps_cpp"],
    "seed_specs": [],
    "execution_boundary": "synthetic contract fixture only",
}

SOURCE = {"source_repo": "/fixture/repo", "head": "e448feb", "source_frozen": True,
          "dirty_behavior_paths": [], "behavior_tree_sha256": "c" * 64}

BINDING = {"expected_config_sha256": "a" * 64, "observed_config_sha256": "a" * 64,
           "silent_normalization": False, "adapter_id": "fixture_adapter"}


TARGET_MANIFEST = core_target().to_dict()


def base_row(root, cell_id, *, arm="mutated", verdict="pass", **changes):
    """Synthetic row derived from the canonical execution fixture.

    ``fixture_pair``-style rows never qualify as real ROS evidence; they exist
    only to exercise the exploratory audit contract.
    """
    raw = pathlib.Path(root) / (cell_id + ".jsonl")
    raw.write_text('{"event":"fixture"}\n')
    base = executed(raw, arm)
    endpoints = {"writer": {"id": "writer-gid", "generation": 0},
                 "reader": {"id": "reader-gid", "generation": 0}}
    base = dict(base, candidate_source="blackbox", evidence_tier="blackbox",
                typed_facts={"matched": True, **endpoints},
                endpoint_identity=copy.deepcopy(endpoints),
                timestamp="2026-11-16T00:00:00+00:00",
                rule_id="discovery.late_endpoint.match_and_receive_after_join",
                effective_input_sha256=scenario().input_hash(),
                provenance={"source_commit": "fixture"})
    plan = {"schema_version": "r2f.mutation-plan/v1", "operations": [{"mutator_id": "M01"}]}
    row = {
        "cell_id": cell_id, "run_id": "audit-run", "target_id": "discovery.late_endpoint",
        "target_revision": 0, "profile_id": "s1-fuzz-parameter", "rmw": "rmw_cyclonedds_cpp",
        "campaign_kind": "exploratory", "arm": arm, "execution_stage": "judged",
        "actual_run": True, "oracle_verdict": verdict, "control_kind": None,
        "raw_path": str(raw), "raw_sha256": raw_sha256(raw),
        "mutation_plan": plan, "mutation_plan_sha256": canonical_hash(plan),
        "input_layer_receipts": [{"layer_id": "parameter", "config": {"reader_delay_sec": 0.2}}],
        "binding": dict(BINDING),
        "observer_complete": True, "observer_dropped": False, "tool_error": False,
        "surface_ids": ["P01"], "boundary_class": "short_delay",
        "state_edge": "matched:delivered", "seed_id": "L01",
        "triage_disposition": "candidate", "responsibility_layer": "sut",
        "reason_code": "sut_contract_violation",
    }
    row.update(base)
    row["run_id"] = "audit-run"
    row["cell_id"] = cell_id
    row["oracle_verdict"] = verdict
    row.update(changes)
    row["run_id"] = changes.get("run_id", "audit-run")
    return row


def build_report(root, rows, *, controls=None, budget=None, abort=None, reviews=None):
    root = pathlib.Path(root)
    controls = controls if controls is not None else []
    reviews = reviews or []
    ledger = exploratory_candidate_ledger(rows, reviews=reviews)
    ledger_path = root / "candidate_ledger.json"
    write_json(ledger_path, ledger)
    executed = sum(1 for r in rows if r.get("execution_stage") == "judged" and r.get("actual_run") is True)
    budget = budget or {"planned": len(rows), "generated": len(rows), "attempted": len(rows),
                        "executed": executed, "aborted": 0, "interrupted": 0,
                        "control_executions": len({c["control_id"] for c in controls})}
    report = {
        "schema_version": EXPLORATORY_SCHEMA,
        "campaign_kind": "exploratory",
        "run_id": "audit-run",
        "profile": copy.deepcopy(PROFILE),
        "identity": {
            "source_sha256": canonical_hash(SOURCE),
            "profile_sha256": canonical_hash(PROFILE),
            "model_sha256": catalog_digest(),
            "rule_sha256": canonical_hash({"fixture": "rule"}),
            "target_sha256": canonical_hash({"fixture": "target"}),
        },
        "source_manifest": SOURCE,
        "model": {"catalog_digest": catalog_digest(),
                  "mutator_catalog": {"M01": {"name": "boundary_pick", "version": 1}},
                  "mutator_catalog_sha256": canonical_hash({"M01": {"name": "boundary_pick", "version": 1}})},
        "required_catalog_sha256": required_catalog_hash(),
        "rows": rows,
        "control_references": controls,
        "budget": budget,
        "last_state": {"planned": budget["planned"], "executed": budget["executed"],
                       "cell_id": rows[-1]["cell_id"] if rows else None},
        "abort": abort,
        "review_records": reviews,
        "candidate_ledger_path": "candidate_ledger.json",
        "candidate_ledger_sha256": canonical_hash(ledger),
    }
    report["report_sha256"] = exploratory_report_digest(report)
    path = root / "report.json"
    write_json(path, report)
    return path, report


class ExploratoryAuditContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def control_ref(self, cell_id):
        return {"control_id": "health-cyclone-late", "source_sha256": canonical_hash(SOURCE),
                "rmw": "rmw_cyclonedds_cpp", "target_contract": "discovery.late_endpoint",
                "recipe": "health", "applies": True, "raw_sha256": "d" * 64,
                "cell_ids": [cell_id], "counts_as_exploratory_input": False}

    def healthy_report(self):
        rows = [base_row(self.root, "input-1", verdict="fail"),
                base_row(self.root, "control-1", arm="baseline", verdict="pass", control_kind="health")]
        return build_report(self.root, rows, controls=[self.control_ref("control-1")])

    def test_clean_exploratory_report_is_evidence_valid_but_never_sealed(self):
        path, _ = self.healthy_report()
        audit = audit_report(path)
        self.assertTrue(audit["evidence_valid"], audit["errors"])
        self.assertEqual(audit["errors"], [])
        self.assertFalse(audit["sealed"])
        self.assertEqual(audit["seal"], "not_applicable_exploratory")
        self.assertEqual(audit["audit_kind"], "exploratory")
        # A real SUT anomaly leaves the report valid while oracles do not all pass.
        self.assertEqual(audit["oracle_fail_count"], 1)
        self.assertFalse(audit["all_oracles_pass"])
        self.assertEqual(audit["required_expected"], 18)
        self.assertNotIn("72", json.dumps(audit["errors"]))

    def test_required_audit_default_is_unchanged_and_not_borrowed(self):
        path, _ = self.healthy_report()
        # Downgrading kind must not make an exploratory report look required.
        report = json.loads(path.read_text())
        report["campaign_kind"] = "required"
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["sealed"])
        self.assertTrue(audit["errors"])
        self.assertFalse(audit.get("evidence_valid", False))

    def test_v25_raw_tamper_is_detected_and_old_raw_is_not_rewritten(self):
        path, _ = self.healthy_report()
        raw = self.root / "input-1.jsonl"
        before = raw.read_bytes()
        raw.write_text('{"event":"tampered"}\n')
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("SHA256" in e or "raw" in e for e in audit["errors"]), audit["errors"])
        self.assertNotEqual(raw.read_bytes(), before)

    def test_v25_plan_field_change_is_detected(self):
        path, report = self.healthy_report()
        report["rows"][0]["mutation_plan"]["operations"][0]["mutator_id"] = "M02"
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("mutation_plan_sha256" in e for e in audit["errors"]), audit["errors"])

    def test_v32_rows_after_abort_cannot_read_as_executed(self):
        path, report = self.healthy_report()
        report["abort"] = {"reason_code": "source_repo_HEAD_or_behavior_tree_changed",
                           "after_row_index": 0}
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("after_abort" in e for e in audit["errors"]), audit["errors"])
        report2 = json.loads(path.read_text())
        report2["abort"] = {"reason_code": "source_repo_HEAD_or_behavior_tree_changed",
                            "after_row_index": 1}
        report2["report_sha256"] = exploratory_report_digest(report2)
        write_json(path, report2)
        self.assertTrue(audit_report(path)["evidence_valid"])

    def test_v33_budget_counts_cannot_claim_unexecuted_work(self):
        path, report = self.healthy_report()
        report["budget"]["executed"] = 3
        report["last_state"]["executed"] = 3
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("executed_mismatch" in e for e in audit["errors"]), audit["errors"])

    def test_v34_tool_error_and_observer_drop_are_not_coverage(self):
        rows = [base_row(self.root, "tool-1", verdict="unknown", tool_error=True,
                         tool_error_detail="adapter crash"),
                base_row(self.root, "drop-1", verdict="unknown", observer_dropped=True)]
        path, _ = build_report(self.root, rows)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("tool_error" in e for e in audit["errors"]), audit["errors"])
        ledger = json.loads((self.root / "candidate_ledger.json").read_text())
        self.assertEqual(ledger["candidates"], [])
        self.assertEqual(ledger["tool_error_count"], 1)
        self.assertEqual(ledger["observer_incomplete_count"], 1)
        self.assertEqual(audit["coverage_entries"], 0)

    def test_v35_silent_adapter_normalization_breaks_binding(self):
        path, report = self.healthy_report()
        report["rows"][0]["binding"]["observed_config_sha256"] = "e" * 64
        report["rows"][0]["binding"]["silent_normalization"] = True
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("binding" in e for e in audit["errors"]), audit["errors"])

    def test_planned_rows_cannot_claim_actual_run(self):
        path, report = self.healthy_report()
        report["rows"][0].update(execution_stage="planned", actual_run=True)
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["evidence_valid"])
        self.assertTrue(any("planned row" in e for e in audit["errors"]), audit["errors"])

    def test_unknown_campaign_kind_is_rejected(self):
        path, report = self.healthy_report()
        report["campaign_kind"] = "research_preview"
        report.pop("schema_version")
        report["report_sha256"] = exploratory_report_digest(report)
        write_json(path, report)
        audit = audit_report(path)
        self.assertFalse(audit["sealed"])
        self.assertTrue(any("unknown_campaign_kind" in e for e in audit["errors"]), audit["errors"])


if __name__ == "__main__":
    unittest.main()
