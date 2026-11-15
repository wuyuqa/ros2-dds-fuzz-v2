import copy
import json
from pathlib import Path
import tempfile
import subprocess
import unittest

from r2f.campaign.identity import semantic_input, runtime_config, reject_execution_identity, source_manifest
from r2f.campaign.runner import run_campaign, plan_campaign
from r2f.campaign.scheduler import first_slice_cells, control_references
from r2f.campaign.triage import unknown_gate
from r2f.core.evidence import canonical_hash, effective_input_hash, verify_raw
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.targets import EXECUTION_ORDER
from r2f.reports.run_report import audit_report, write_json, report_digest
from r2f.runtime.instrumentation_bridge import prepare


class CampaignContracts(unittest.TestCase):
    def test_required_matrix_and_control_references_no_duplicate_rows(self):
        rows = first_slice_cells()
        self.assertEqual(len(rows), 24)
        self.assertEqual(len([r for r in rows if r["control_kind"] is None]), 18)
        refs = control_references(rows)
        self.assertEqual({k: len(v) for k, v in refs.items()}, dict.fromkeys(["health", "fault", "mutation", "transparency"], 6))
        self.assertTrue(set(refs["health"]).issubset({r["cell_id"] for r in rows}))
        self.assertEqual(len(set(r["cell_id"] for r in rows)), 24)

    def test_late_join_preconditions_precede_reader_mutation(self):
        for target_id in EXECUTION_ORDER[:2]:
            target = S1Module().targets[target_id].to_dict()
            self.assertEqual([s["step_id"] for s in target["state_steps"]], ["writer_created", "writer_prejoin_published"])
            self.assertEqual([s["fact"] for s in target["state_preconditions"]], ["writer_created", "writer_prejoin_published"])

    def test_capsule_values_reach_runtime_without_default_substitution(self):
        for arm in ("baseline", "transparent", "mutated", "negative"):
            submitted = semantic_input(arm, {"reader_delay_sec": 0.731, "baseline_reader_delay_sec": 0.123,
                "post_match_count": 9, "delivery_timeout_sec": 2.31, "publish_interval_sec": 0.041})
            config = runtime_config(submitted, arm=arm, rmw="rmw_cyclonedds_cpp", run_id="input-test", domain_id=171)
            for key, value in submitted.items():
                self.assertEqual(config[key], value, key)
            effective_delay = config["reader_delay_sec"] if arm == "mutated" else config["baseline_reader_delay_sec"]
            self.assertEqual(effective_delay, 0.731 if arm == "mutated" else 0.123)
            self.assertEqual(config["fault_id"], "late_endpoint_delivery_omission" if arm == "negative" else None)
        with self.assertRaises(ValueError):
            semantic_input("mutated", {"late_join_delay": 3})

    def test_semantic_input_separates_execution_environment(self):
        submitted = semantic_input("baseline")
        self.assertEqual(submitted, semantic_input("transparent"))
        c1 = runtime_config(submitted, arm="baseline", run_id="old", domain_id=170)
        c2 = runtime_config(submitted, arm="baseline", run_id="new", domain_id=171)
        self.assertNotEqual(c1["domain_id"], c2["domain_id"])
        self.assertEqual(c1["final_input"] if "final_input" in c1 else submitted, submitted)
        with self.assertRaises(ValueError):
            reject_execution_identity({"nested": {"run_id": "wrong"}})

    def test_plan_has_all_input_capsules_and_no_execution_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            report = plan_campaign(temp, run_id="offline-plan")
            self.assertEqual(len(report["rows"]), 24)
            hashes = {}
            for row in report["rows"]:
                capsule = ReplayCapsule.from_dict(json.loads(Path(row["replay_path"]).read_text()))
                self.assertIsNone(capsule.execution)
                self.assertFalse(row["actual_run"])
                self.assertEqual(row["execution_stage"], "planned")
                self.assertEqual(row["effective_input_sha256"], effective_input_hash(capsule.input["effective_input"]))
                hashes[row["cell_id"]] = row["effective_input_sha256"]
                changed = copy.deepcopy(capsule.input["effective_input"])
                changed.update(campaign_kind="exploratory", origin_domain=222)
                self.assertEqual(effective_input_hash(changed), row["effective_input_sha256"])
            self.assertEqual(len(set(hashes.values())), 24)
            self.assertEqual(report["full_scope_required_cells"], 72)

    def test_all_target_plans_bind_nonempty_capsule_receipts_into_observation_input(self):
        fields = {'run_id', 'domain_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat',
                  'seed', 'topic', 'message_type', 'effective_input_sha256', 'final_input',
                  'input_layer_receipts'}
        checked = 0
        with tempfile.TemporaryDirectory() as temp:
            for index, target_id in enumerate(EXECUTION_ORDER):
                report = plan_campaign(Path(temp) / str(index), target_id=target_id,
                                       run_id='receipt-plan-' + str(index), domain_id=171, seed=31)
                self.assertEqual(len(report['rows']), 24)
                for row in report['rows']:
                    with self.subTest(target_id=target_id, cell=row['cell_id']):
                        path = Path(row['replay_path'])
                        original = path.read_bytes()
                        capsule = ReplayCapsule.from_dict(json.loads(original))
                        config = json.loads(Path(row['runtime_config_path']).read_text())
                        before = copy.deepcopy(config)
                        effective = capsule.input['effective_input']
                        self.assertTrue(effective['input_layer_receipts'])
                        self.assertTrue(fields.issubset(config), fields - config.keys())
                        self.assertEqual(config['input_layer_receipts'], effective['input_layer_receipts'])
                        self.assertEqual(config['final_input'], effective['final_input'])
                        self.assertEqual(config['effective_input_sha256'], capsule.input['effective_input_sha256'])
                        self.assertEqual(effective_input_hash(effective), config['effective_input_sha256'])
                        for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed', 'topic', 'message_type'):
                            self.assertEqual(config[key], capsule.input[key], key)
                        ctx = prepare(config, Path(row['runtime_config_path']).parent / 'observer', {})
                        expected = {'schema': 'r2f.observation-execution-input/v1',
                                    **{key: config[key] for key in fields}}
                        self.assertEqual(ctx.execution_input, expected)
                        verify_raw(ctx.prepared['raw_path'], ctx.prepared['raw_sha256'])
                        prepared = json.loads(Path(ctx.prepared['raw_path']).read_text())
                        self.assertEqual(prepared['execution_input'], expected)
                        self.assertEqual(config, before)
                        self.assertEqual(path.read_bytes(), original)
                        self.assertIsNone(capsule.execution)
                        self.assertFalse(row['actual_run'])
                        self.assertEqual(row['execution_stage'], 'planned')
                        checked += 1
            self.assertEqual(checked, 96)

    def test_plan_receipt_and_input_hash_are_stable_across_execution_rebinding(self):
        with tempfile.TemporaryDirectory() as temp:
            for index, target_id in enumerate(EXECUTION_ORDER):
                with self.subTest(target_id=target_id):
                    plans = [plan_campaign(Path(temp) / str(index) / str(domain), target_id=target_id,
                                           run_id='receipt-' + str(domain), domain_id=domain, seed=31)
                             for domain in (171, 172)]
                    self.assertEqual(len(plans[0]['rows']), 24)
                    for left, right in zip(plans[0]['rows'], plans[1]['rows']):
                        configs = [json.loads(Path(row['runtime_config_path']).read_text()) for row in (left, right)]
                        self.assertNotEqual(configs[0]['run_id'], configs[1]['run_id'])
                        self.assertNotEqual(configs[0]['domain_id'], configs[1]['domain_id'])
                        self.assertTrue(configs[0]['input_layer_receipts'])
                        self.assertEqual(configs[0]['input_layer_receipts'], configs[1]['input_layer_receipts'])
                        self.assertEqual(configs[0]['effective_input_sha256'], configs[1]['effective_input_sha256'])
                        self.assertEqual(left['effective_input_sha256'], right['effective_input_sha256'])
                        capsule = ReplayCapsule.from_dict(json.loads(Path(left['replay_path']).read_text()))
                        changed = copy.deepcopy(capsule.input['effective_input'])
                        changed['input_layer_receipts'][0]['parameters_changed'] = True
                        self.assertNotEqual(effective_input_hash(changed), left['effective_input_sha256'])

    def test_planned_status_cannot_pass_seal_even_if_forged_green(self):
        with tempfile.TemporaryDirectory() as temp:
            report = run_campaign(temp, run_id="offline-plan")
            self.assertFalse(report["seal"]["sealed"])
            report["seal"] = {"sealed": True}
            for row in report["rows"]:
                row["execution_stage"] = "judged"
                row["oracle_verdict"] = "fail" if row["arm"] == "negative" else "pass"
                row["actual_run"] = True
            report["report_sha256"] = report_digest(report)
            write_json(Path(temp) / "report.json", report)
            audit = audit_report(Path(temp) / "report.json")
            self.assertFalse(audit["sealed"])
            self.assertTrue(any("evidence" in error or "file missing" in error for error in audit["errors"]))

    def test_fault_required_controls_do_not_enter_maturity_denominator(self):
        with tempfile.TemporaryDirectory() as temp:
            report = run_campaign(temp, run_id="denominator")
            required = [row for row in report["rows"] if row["campaign_kind"] == "required" and not row.get("control_kind") and row["arm"] != "negative"]
            fault = [row for row in report["rows"] if row["control_kind"] == "fault"]
            self.assertEqual(len(required), 18)
            self.assertEqual(len(fault), 6)
            self.assertTrue(all(row["campaign_kind"] == "required" for row in fault))
            self.assertNotIn("required_matrix_not_18", report["seal"]["errors"])
            self.assertNotIn("matrix_missing_duplicate_or_extra_cell", report["seal"]["errors"])
            self.assertEqual(report["seal"]["required_expected"], 18)
            self.assertEqual(report["seal"]["fault_expected"], 6)

    def test_source_freeze_checks_tracked_behavior_bytes_not_results(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, value in {"src/unit.py": "VALUE = 1\n", "config/fixed.json": "{}\n", "scripts/setup_overlay.bash": "true\n"}.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(value)
            def git(*args):
                return subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=True)
            git("init", "-q")
            git("add", "src", "config", "scripts")
            git("-c", "user.name=Contract Test", "-c", "user.email=contract@example.invalid", "commit", "-qm", "fixture")
            frozen = source_manifest(root)
            self.assertTrue(frozen["source_frozen"])
            self.assertEqual(frozen["dirty_behavior_paths"], [])
            (root / "results").mkdir()
            (root / "results/output.json").write_text('{"changed": true}')
            self.assertEqual(source_manifest(root), frozen)
            (root / "src/unit.py").write_text("VALUE = 2\n")
            dirty = source_manifest(root)
            self.assertFalse(dirty["source_frozen"])
            self.assertEqual(dirty["head"], frozen["head"])
            self.assertIn("src/unit.py", dirty["dirty_behavior_paths"])
            (root / "src/unit.py").write_text("VALUE = 1\n")
            (root / "config/untracked.json").write_text("{}")
            self.assertFalse(source_manifest(root)["source_frozen"])
            (root / "config/untracked.json").unlink()
            (root / "scripts/setup_overlay.bash").write_text("false\n")
            self.assertIn("scripts/setup_overlay.bash", source_manifest(root)["dirty_behavior_paths"])

    def test_unknown_gate_required_and_exploratory_use_core_policy(self):
        self.assertFalse(unknown_gate([{"cell_id": "x", "oracle_verdict": "unknown", "campaign_kind": "required"}])["passed"])
        self.assertTrue(unknown_gate([{"cell_id": str(i), "target_id": "t", "oracle_verdict": "unknown" if i == 0 else "pass", "campaign_kind": "exploratory"} for i in range(10)])["passed"])
        self.assertFalse(unknown_gate([{"oracle_verdict": "unknown", "campaign_kind": "required", "control_kind": "fault"}])["passed"])

    def test_runner_retains_tool_failures_and_zero_candidate_reason(self):
        def broken(config, directory):
            raise OSError("deliberate contract failure")
        with tempfile.TemporaryDirectory() as temp:
            report = run_campaign(temp, run_id="failed-runtime", execute=True, runtime=broken)
            self.assertEqual(len(report["rows"]), 24)
            self.assertTrue(all(row["execution_stage"] == "aborted" for row in report["rows"]))
            self.assertTrue(all(row["runtime_status"] == "tool_error" for row in report["rows"]))
            ledger = json.loads(Path(report["candidate_ledger_path"]).read_text())
            self.assertEqual(len(ledger["candidates"]), 0)
            self.assertEqual(len(ledger["failures"]), 18)
            self.assertEqual(len(ledger["controls"]), 6)
            self.assertTrue(ledger["zero_candidate_explanation"])
            self.assertFalse(report["seal"]["sealed"])

    def test_report_tamper_and_missing_required_cell_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            report = run_campaign(temp, run_id="tamper")
            report["rows"].pop()
            write_json(Path(temp) / "report.json", report)
            audit = audit_report(Path(temp) / "report.json")
            self.assertIn("report_hash_mismatch", audit["errors"])
            self.assertIn("matrix_missing_duplicate_or_extra_cell", audit["errors"])


if __name__ == "__main__":
    unittest.main()
