"""Read-only regression against pre-existing ROS smoke evidence; starts no ROS processes.

These development runs have no frozen campaign identity and cannot seal. Only
values present in raw events / runtime metadata are reconstructed here.
"""
import json
from pathlib import Path
import unittest

from r2f.campaign.runtime_adapter import ingest_execution
from r2f.campaign.triage import attribute
from r2f.core import compile_scenario
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import S1Module

ROOT = Path(__file__).resolve().parents[1]
SMOKES = (
    "cyclone-baseline-contract-v1", "cyclone-transparent-contract-v1",
    "cyclone-mutated-contract-v1", "cyclone-negative-contract-v1",
    "fastdds-mutated-contract-v1", "fastdds-negative-contract-v1",
    "cyclone-first-runner-interface-v2",
)


def ingest_smoke(path):
    runtime = json.loads(path.read_text())
    metadata, events = runtime["execution_metadata"], runtime["events"]
    writer = next(e for e in events if e["event"] == "endpoint_created" and e["role"] == "writer")
    contract = next(e for e in events if e["event"] == "delivery_contract")
    delay = next(e["configured_delay_sec"] for e in events if e["event"] == "reader_created_after_writer")
    submitted = {
        "topic": writer["topic"], "message_type": writer["message_type"], "qos": writer["qos"],
        "reader_delay_sec": delay,
        "pre_match_count": len([e for e in events if e["event"] == "send" and e.get("phase") == "prejoin"]),
        "post_match_count": len(contract["expected_sequences"]), "fault_id": contract.get("fault_id"),
    }
    config = dict(metadata, **submitted)
    if config["arm"] == "negative":
        config["control_kind"] = "fault"
    scenario = compile_scenario(S1Module().targets[metadata["target_id"]], metadata["rmw"], metadata["arm"], metadata["repeat"],
                                parameters={"mutation_parameters": {"reader_delay_sec": delay}})
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=metadata["source"],
                                         final_input=submitted, origin_domain=metadata["domain_id"])
    return ingest_execution(scenario, config, runtime, capsule), capsule


class ExistingSmokeIngest(unittest.TestCase):
    def test_existing_development_smoke_reaches_expected_primary_verdict(self):
        observed = 0
        for name in SMOKES:
            path = ROOT / "results/runtime-smoke" / name / "runtime_result.json"
            if not path.is_file():
                continue
            observed += 1
            with self.subTest(smoke=name):
                row, capsule = ingest_smoke(path)
                self.assertEqual(row["execution_stage"], "judged", row.get("abort_reason"))
                self.assertNotIn("evidence_error", row["typed_facts"])
                self.assertEqual(row["typed_facts"]["identity_proof"]["basis"], "endpoint_correlated")
                self.assertIsNone(row["typed_facts"]["identity_proof"]["direct_gid_match"])
                self.assertEqual(len(row["precondition_receipts"]), 2)
                self.assertEqual(len(row["observation_receipts"]), 3)
                self.assertIsNone(capsule.execution)  # Reconstruction is not a sealed historical capsule.
                if row["arm"] == "negative":
                    self.assertEqual(row["oracle_verdict"], "fail")
                    self.assertEqual(row["control_kind"], "fault")
                    self.assertFalse(row["mutation_receipt"]["hit"])
                    self.assertNotIn("mutation_applied", [step["execution_stage"] for step in row["stage_history"]])
                    triage = attribute(row, row["typed_facts"], row["oracle"])
                    self.assertEqual(triage["responsibility_layer"], "harness")
                    self.assertEqual(triage["triage_disposition"], "rejected")
                else:
                    self.assertEqual(row["oracle_verdict"], "pass")
        if not observed:
            self.skipTest("development smoke artifacts are not present in this checkout")


if __name__ == "__main__":
    unittest.main()
