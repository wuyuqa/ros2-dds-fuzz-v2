import copy
import unittest

from r2f.modules.s1.module import S1Module
from r2f.modules.s1.oracles import evaluate_primary
from r2f.campaign.triage import attribute


def valid_facts():
    return {
        "schema_version": 1,
        "writer": {"id": "writer-1", "generation": 0, "created_ns": 10},
        "reader": {"id": "reader-1", "generation": 0, "created_ns": 20},
        "match": {"writer_id": "writer-1", "reader_id": "reader-1", "at_ns": 30},
        "window": {"start_ns": 30, "end_ns": 100, "complete": True},
        "sent": [{"sequence": i, "at_ns": 40 + i, "writer_id": "writer-1", "generation": 0} for i in range(3)],
        "received": [{"sequence": i, "at_ns": 60 + i, "writer_id": "writer-1", "reader_id": "reader-1", "writer_generation": 0, "reader_generation": 0} for i in range(3)],
        "environment_healthy": True, "process_healthy": True,
        "preconditions_reached": True, "endpoint_identity_complete": True,
        "observed_effects": ["reader_created_after_writer", "match_established"],
    }


class S1Contracts(unittest.TestCase):
    def test_exact_scope_and_honest_carrier_dispositions(self):
        module = S1Module()
        self.assertEqual(module.id, "s1")
        self.assertEqual(len(module.targets), 4)
        self.assertEqual(module.execution_order[0], "discovery.late_endpoint")
        self.assertIn("blocked", module.dispositions["discovery.interruption_recovery"]["status"])
        self.assertIn("builtin", module.dispositions["builtin.discovery_projection_reconciliation"]["reason"])
        late = module.targets["discovery.late_join_delivery"].to_dict()
        self.assertIn("volatile", str(late))
        self.assertNotIn("transient_local", str(late))

    def test_primary_rule_checks_delivery_not_progress(self):
        result = evaluate_primary(valid_facts())
        self.assertEqual(result["verdict"], "pass")
        self.assertEqual(result["rule_id"], "discovery.late_endpoint.match_and_receive_after_join")
        facts = valid_facts()
        facts["received"].pop(1)
        result = evaluate_primary(facts)
        self.assertEqual(result["verdict"], "fail")
        self.assertIn("post_match_delivery_loss", result["forbidden_residuals"])

    def test_tail_inflight_and_unmatched_window_are_unknown(self):
        facts = valid_facts()
        facts["received"].pop()
        result = evaluate_primary(facts)
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("tail_inflight", result["reason_codes"])
        facts = valid_facts()
        facts["match"] = None
        self.assertEqual(evaluate_primary(facts)["verdict"], "unknown")

    def test_missing_or_wrongly_typed_evidence_is_unknown(self):
        for field in ("writer", "match", "sent", "window"):
            facts = valid_facts()
            facts.pop(field)
            self.assertEqual(evaluate_primary(facts)["verdict"], "unknown", field)
        facts = valid_facts()
        facts["window"]["complete"] = "true"
        self.assertEqual(evaluate_primary(facts)["verdict"], "unknown")
        facts = valid_facts()
        facts["sent"][0]["sequence"] = True
        self.assertEqual(evaluate_primary(facts)["verdict"], "unknown")

    def test_identity_and_order_violations_are_primary(self):
        facts = valid_facts()
        facts["received"][0]["writer_generation"] = 1
        self.assertIn("identity_generation_mixup", evaluate_primary(facts)["forbidden_residuals"])
        facts = valid_facts()
        facts["received"].reverse()
        self.assertEqual(evaluate_primary(facts)["verdict"], "fail")

    def test_volatile_prejoin_samples_are_not_delivery_obligations(self):
        facts = valid_facts()
        facts["sent"].insert(0, {"sequence": 99, "at_ns": 15, "writer_id": "writer-1", "generation": 0})
        self.assertEqual(evaluate_primary(facts)["verdict"], "pass")

    def test_allowed_effect_cannot_swallow_residual(self):
        facts = valid_facts()
        facts["received"].pop(1)
        oracle = evaluate_primary(facts)
        cell = {"arm": "mutated", "control_kind": None,
                "mutation_receipt": dict.fromkeys(["planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed"], True),
                "controls_support_sut": True, "provenance_supports_sut": True, "execution_stage": "judged"}
        result = attribute(cell, facts, oracle)
        self.assertEqual(result["reason_code"], "sut_contract_violation")
        cell["control_kind"] = "fault"
        self.assertNotEqual(attribute(cell, facts, oracle)["responsibility_layer"], "sut")

    def test_unknown_and_ineffective_never_become_sut_candidate(self):
        facts = valid_facts()
        oracle = evaluate_primary(facts)
        cell = {"arm": "mutated", "mutation_receipt": dict.fromkeys(["planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed"], False), "execution_stage": "judged"}
        self.assertEqual(attribute(cell, facts, oracle)["reason_code"], "mutation_ineffective")
        cell = {"arm": "baseline", "neutral_execution": {"audited": True}}
        facts["environment_healthy"] = False
        self.assertEqual(attribute(cell, facts, oracle)["responsibility_layer"], "harness")


if __name__ == "__main__":
    unittest.main()
