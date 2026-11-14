"""Synthetic contract tokens exercise gates; these fixtures can never seal ROS evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.runner import plan_campaign, run_campaign
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.core.evidence import raw_sha256
from r2f.core.replay import ReplayCapsule
from r2f.core.scenario import Scenario
from r2f.reports.run_report import audit_report, report_digest, write_json


def fixture_pair(config, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    events, pre, post = [], [], []
    tick = 1_000_000
    generations = {"writer": "writer-generation", "reader": "reader-generation"}
    gids = {"writer": "writer-gid", "reader": "reader-gid"}
    pids = {"writer": 8001, "reader": 8002, "controller": 8000}
    receipt = dict.fromkeys(("planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed"), config["arm"] == "mutated")

    def emit(event, role="controller", **fields):
        nonlocal tick
        tick += 10_000
        result = {"event": event, "event_index": len(events), "time_ns": 1_700_000_000_000_000_000 + tick,
                  "monotonic_ns": tick, "run_id": config["run_id"], "domain_id": config["domain_id"],
                  "role": role, "rmw_identifier": config["rmw"] if role != "controller" else None,
                  "pid": pids[role], "endpoint_generation": generations.get(role)}
        result.update(fields)
        events.append(result)
        return result

    def step(name, deps, action, observation=False):
        record = {"step_id": name, "requires": deps, "before": {"monotonic_ns": tick, "satisfied": False}, "after": None, "satisfied": False}
        before = emit("state_step_before", **copy.deepcopy(record))
        action()
        record["after"] = {"monotonic_ns": tick, "satisfied": True}
        record["satisfied"] = True
        after = emit("state_step_after", **copy.deepcopy(record))
        (post if observation else pre).append(record)
        return before, after

    def endpoint(role):
        emit("endpoint_created", role, endpoint_gid=gids[role], created_monotonic_ns=tick + 10_000,
             topic=config["topic"], qos=config["qos"], message_type=config["message_type"])

    emit("isolation_locks_acquired", locks=[{"kind": "campaign", "owner": config["run_id"]}])
    emit("environment_preflight", ok=True, reason=None)
    before, after = step("writer_created", ["environment_ok", "locks_held"], lambda: endpoint("writer"))
    refs = [{"step_id": "writer_created", "raw_before_index": before["event_index"], "raw_after_index": after["event_index"]}]
    def prejoin():
        for seq in range(config["pre_match_count"]):
            emit("send", "writer", send_sequence=seq, phase="prejoin", writer_gid=gids["writer"], publish_returned=True)
    before, after = step("writer_prejoin_published", ["writer_created"], prejoin)
    refs.append({"step_id": "writer_prejoin_published", "raw_before_index": before["event_index"], "raw_after_index": after["event_index"]})
    emit("mutation_begin" if config["arm"] == "mutated" else "control_begin",
         preconditions_snapshot={"writer_created": True, "writer_prejoin_published": True, "reader_created": False}, precondition_receipt_refs=refs)
    step("reader_created", ["writer_created", "writer_prejoin_published"], lambda: endpoint("reader"), True)
    emit("reader_created_after_writer", writer_gid=gids["writer"], reader_gid=gids["reader"], configured_delay_sec=config["reader_delay_sec"])
    if config["arm"] == "mutated":
        emit("mutation_effect", effect_id="reader_created_after_writer", mutation_receipt=receipt)
    if config["arm"] == "transparent":
        emit("transparency_control", applied_input_equal_to_baseline=True, effective_reader_delay_sec=config["reader_delay_sec"], mutation_hit=False)
    def match():
        for role in ("writer", "reader"):
            emit("match", role, matched_count=1, endpoint_gid=gids[role])
    step("endpoints_matched", ["reader_created", "writer_created"], match, True)
    def graph():
        for role in ("writer", "reader"):
            emit("graph_snapshot", role, publishers=[{"endpoint_gid": gids["writer"]}], subscriptions=[{"endpoint_gid": gids["reader"]}])
    step("graph_identity_verified", ["endpoints_matched"], graph, True)
    expected = list(range(config["pre_match_count"], config["pre_match_count"] + config["post_match_count"]))
    omit = expected[len(expected) // 2] if config["arm"] == "negative" else None
    emit("delivery_contract", expected_sequences=expected, sentinel_sequence=expected[-1], prejoin_in_delivery_contract=False, qos=config["qos"])
    emit("window_open", window="post_match_delivery")
    for seq in expected:
        if seq == omit:
            emit("harness_omission", "writer", send_sequence=seq, phase="post_match", responsibility_layer="harness", candidate_eligible=False)
        else:
            emit("send", "writer", send_sequence=seq, phase="post_match", writer_gid=gids["writer"], publish_returned=True)
            emit("receive", "reader", send_sequence=seq, phase="post_match", publisher_gid=gids["writer"], reader_gid=gids["reader"], payload_run_id=config["run_id"])
    emit("window_close", window="post_match_delivery")
    if omit is not None:
        emit("fault_control_receipt", fault_id=config["fault_id"], omitted_sequence=omit, applied=True, responsibility_layer="harness", candidate_eligible=False)
    emit("runtime_end", runtime_status="completed", real_execution=True, mutation_receipt=receipt)
    path = directory / "raw.jsonl"
    path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
    metadata = {key: config[key] for key in ("run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "domain_id")}
    metadata["workers"] = [{"pid": pids[role], "role": role, "returncode": 0, "fresh_process": True, "separate_context": True} for role in ("writer", "reader")]
    environment = {"ok": True, "environment_mode": "ros_overlay", "ros_distro": "jazzy",
                   "source": {"head": config.get("source_manifest", {}).get("head"), "source_repo": config["source_repo"]},
                   "rmw": {config["rmw"]: {"available": True}}, "dependency_lock_sha256": "a" * 64,
                   "process_scan": {"ok": True}, "old_lock_scan": {"ok": True}}
    return {"real_execution": True, "runtime_status": "completed", "reason": None,
            "events": events, "precondition_receipts": pre, "observation_receipts": post,
            "mutation_receipt": receipt, "raw_path": str(path), "raw_sha256": raw_sha256(path),
            "environment": environment, "execution_metadata": metadata, "evidence_origin": "offline"}


class EvidenceGates(unittest.TestCase):
    def setup_cell(self, directory, arm="mutated"):
        report = plan_campaign(directory, run_id="evidence-test")
        row = next(r for r in report["rows"] if r["arm"] == arm)
        config = json.loads(Path(row["runtime_config_path"]).read_text())
        capsule = ReplayCapsule.from_dict(json.loads(Path(row["replay_path"]).read_text()))
        runtime = fixture_pair(config, Path(directory) / "runtime-fixture")
        return row, config, capsule, runtime

    def test_raw_receipts_reach_core_judged_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            planned, config, capsule, runtime = self.setup_cell(temp)
            row = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
            self.assertEqual(row["execution_stage"], "judged", row.get("abort_reason"))
            self.assertEqual(row["oracle_verdict"], "pass")
            self.assertEqual(len(row["precondition_receipts"]), 2)
            self.assertEqual(len(row["observation_receipts"]), 3)
            self.assertEqual(row["precondition_receipts"][1]["after"]["writer_prejoin_published"], True)

    def test_neutral_core_path_keeps_hit_false_and_skips_mutation_applied(self):
        with tempfile.TemporaryDirectory() as temp:
            planned, config, capsule, runtime = self.setup_cell(temp, "baseline")
            row = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
            self.assertEqual(row["execution_stage"], "judged", row.get("abort_reason"))
            self.assertFalse(row["mutation_receipt"]["hit"])
            self.assertNotIn("mutation_applied", [s["execution_stage"] for s in row["stage_history"]])

    def test_negative_uses_raw_bound_fault_path_without_late_joiner_hit(self):
        with tempfile.TemporaryDirectory() as temp:
            planned, config, capsule, runtime = self.setup_cell(temp, "negative")
            row = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
            self.assertEqual(row["execution_stage"], "judged", row.get("abort_reason"))
            self.assertEqual(row["oracle_verdict"], "fail")
            self.assertEqual(row["control_kind"], "fault")
            self.assertFalse(row["mutation_receipt"]["hit"])
            self.assertNotIn("mutation_applied", [s["execution_stage"] for s in row["stage_history"]])
            fault = row["fault_control_receipt"]
            self.assertEqual(fault["omitted_sequence"], config["pre_match_count"] + config["post_match_count"] // 2)
            self.assertEqual(runtime["events"][fault["raw_event_ref"]]["event"], "fault_control_receipt")
            from r2f.campaign.triage import attribute
            triage = attribute(row, row["typed_facts"], row["oracle"])
            self.assertEqual(triage["responsibility_layer"], "harness")
            self.assertEqual(triage["triage_disposition"], "rejected")
            from r2f.reports.candidate_report import candidate_ledger
            ledger = candidate_ledger([dict(row, cell_id=planned["cell_id"])])
            self.assertEqual(ledger["candidates"], [])
            self.assertEqual(len(ledger["controls"]), 1)

    def test_negative_receipt_with_missing_applied_evidence_aborts(self):
        with tempfile.TemporaryDirectory() as temp:
            planned, config, capsule, runtime = self.setup_cell(temp, "negative")
            for event in runtime["events"]:
                if event["event"] == "fault_control_receipt":
                    event["applied"] = False
            Path(runtime["raw_path"]).write_text("".join(json.dumps(e) + "\n" for e in runtime["events"]))
            runtime["raw_sha256"] = raw_sha256(runtime["raw_path"])
            row = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
            self.assertEqual(row["execution_stage"], "aborted")
            self.assertIn("negative omission lacks independent applied receipt", row["abort_reason"])

    def test_raw_tamper_or_returned_event_mismatch_aborts(self):
        for mode in ("bytes", "events", "identity", "preconditions"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                planned, config, capsule, runtime = self.setup_cell(temp)
                if mode == "bytes":
                    with Path(runtime["raw_path"]).open("a") as stream:
                        stream.write("{}\n")
                elif mode == "events":
                    runtime["events"] = runtime["events"][:-1]
                elif mode == "identity":
                    for event in runtime["events"]:
                        event["run_id"] = "another-run"
                    Path(runtime["raw_path"]).write_text("".join(json.dumps(e) + "\n" for e in runtime["events"]))
                    runtime["raw_sha256"] = raw_sha256(runtime["raw_path"])
                else:
                    runtime["precondition_receipts"][0]["satisfied"] = False
                row = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
                self.assertEqual(row["execution_stage"], "aborted")
                self.assertTrue(row["abort_reason"])

    def test_candidate_ledger_tamper_is_detected_even_with_rehashed_report(self):
        from r2f.campaign.runner import _persist
        def missing_middle(config, directory):
            result = fixture_pair(config, directory)
            if config["arm"] == "baseline":
                victim = config["pre_match_count"] + 1
                result["events"] = [e for e in result["events"] if not (e["event"] == "receive" and e.get("send_sequence") == victim)]
                for index, event in enumerate(result["events"]):
                    event["event_index"] = index
                Path(result["raw_path"]).write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in result["events"]))
                result["raw_sha256"] = raw_sha256(result["raw_path"])
            return result
        with tempfile.TemporaryDirectory() as temp:
            report = run_campaign(temp, run_id="ledger-tamper", execute=True, runtime=missing_middle)
            ledger_path = Path(report["candidate_ledger_path"])
            original = json.loads(ledger_path.read_text())
            self.assertGreater(len(original["candidates"]), 0)
            self.assertNotIn("candidate_evidence_or_lifecycle_not_bound_to_verified_cell", report["seal"]["errors"])
            for mutation in ("raw", "facts", "responsibility", "history"):
                with self.subTest(mutation=mutation):
                    ledger = copy.deepcopy(original)
                    candidate = ledger["candidates"][0]
                    if mutation == "raw":
                        candidate["evidence"]["raw_sha256"] = "0" * 64
                    elif mutation == "facts":
                        candidate["evidence"]["typed_facts"]["writer"]["id"] = "another-writer"
                    elif mutation == "responsibility":
                        candidate["responsibility_layer"] = "sut"
                    else:
                        candidate["history"] = []
                    from r2f.core.evidence import canonical_hash
                    write_json(ledger_path, ledger)
                    report["candidate_ledger_sha256"] = canonical_hash(ledger)
                    report["report_sha256"] = report_digest(report)
                    write_json(Path(temp) / "report.json", report)
                    self.assertIn("candidate_evidence_or_lifecycle_not_bound_to_verified_cell", audit_report(Path(temp) / "report.json")["errors"])

    def test_same_status_missing_step_proof_cannot_reach(self):
        with tempfile.TemporaryDirectory() as temp:
            planned, config, capsule, runtime = self.setup_cell(temp)
            runtime["precondition_receipts"] = []
            row = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
            self.assertEqual(row["execution_stage"], "aborted")
            self.assertNotIn("reached", [s["execution_stage"] for s in row["stage_history"]])


if __name__ == "__main__":
    unittest.main()
