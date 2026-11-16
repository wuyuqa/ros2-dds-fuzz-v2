"""V12/V13/V18/V19/V20 plus V36: receipts, state abstraction, review gate.

Everything here is offline.  Nothing in this file proves a ROS execution, a
DDS hit or a confirmed real SUT candidate.
"""
import pathlib
import tempfile
import unittest

from r2f.core.candidate import CandidateLifecycle
from r2f.core.evidence import (
    aggregate_layer_receipts, effective_input_hash, validate_chain_receipt,
    INPUT_SCHEMA_V2,
)
from r2f.core.mutation import MutationReceipt
from r2f.core.state_signature import (
    STATE_COMPONENTS, abstract_state, coverage_eligible, edge_id, observation_edge,
    state_id, validate_state,
)
from test_contract_audit import observation


def full_receipt():
    return MutationReceipt(True, True, True, True, True, True, True).to_dict()


def partial_receipt():
    return MutationReceipt(True, True, True, True, False, False, False).to_dict()


def base_facts(**overrides):
    facts = {
        "live_roles": "both",
        "creation_order_class": "writer_first",
        "compatibility_class": "compatible",
        "observed_match_class": "bidirectional",
        "network_phase": "normal",
        "recovery_cycle_bucket": "0",
        "delivery_closure_class": "closed_within_contract",
        "observation_completeness": "complete",
        "observation_complete": True,
        "window_complete": True,
    }
    facts.update(overrides)
    return facts


class ReceiptsV12V13(unittest.TestCase):
    def test_v12_partial_hit_never_passes_the_chain(self):
        layers = [{"layer_id": "parameter", "hit": True},
                  {"layer_id": "environment", "hit": False}]
        clean = dict(full_receipt(), applied=False, hit=False, effect_observed=False)
        self.assertFalse(validate_chain_receipt(layers, clean)["chain_hit"])
        with self.assertRaises(ValueError):
            validate_chain_receipt(layers, full_receipt())
        aggregate = aggregate_layer_receipts([
            {"layer_id": "parameter", "receipt": full_receipt()},
            {"layer_id": "environment", "receipt": partial_receipt()},
        ])
        self.assertFalse(aggregate["hit"])
        self.assertTrue(aggregate["configured"])

    def test_v13_input_layer_receipts_reject_execution_fields(self):
        data = {
            "target_id": "discovery.late_endpoint", "target_revision": 0,
            "mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
            "mutation_parameters": {}, "rmw": "rmw_cyclonedds_cpp", "repeat": 0,
            "seed": 0, "arm": "mutated", "mutation_layer_order": ["late_joiner"],
            "input_layer_receipts": [], "final_input": {"reader_delay_sec": 0.15},
            "input_identity_schema": INPUT_SCHEMA_V2,
        }
        for field in ("hit", "applied", "effect_observed", "execution_stage", "executed_at"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                effective_input_hash(dict(data, input_layer_receipts=[{"layer": "x", field: True}]))


class StateSignatureV18V19V20(unittest.TestCase):
    def test_v18_plan_or_seed_change_without_execution_is_not_coverage(self):
        for record in ({"execution_stage": "planned", "typed_facts": base_facts()},
                       {"execution_stage": "reached", "typed_facts": base_facts()},
                       {"execution_stage": "judged", "typed_facts": base_facts(),
                        "seed": 9, "rng_seed": 10, "tool_error": True}):
            with self.subTest(record=record.get("execution_stage")):
                self.assertFalse(coverage_eligible(record))
                self.assertIsNone(observation_edge(record))

    def test_v18_same_abstract_state_under_different_seed_is_identical(self):
        left = abstract_state(base_facts(), identity_consistent=True)
        right = abstract_state(base_facts(), identity_consistent=True)
        self.assertEqual(state_id(left), state_id(right))
        self.assertEqual(edge_id(state_id(left), "create_reader", "min", state_id(right)),
                         edge_id(state_id(left), "create_reader", "min", state_id(right)))

    def test_v19_guid_pid_time_do_not_change_state_id(self):
        clean = abstract_state(base_facts(), identity_consistent=True)
        noisy = abstract_state(base_facts(), identity_consistent=True)
        # Identity/plumbing keys must be ignored even when present.
        noisy_facts = base_facts(guid="deadbeef", pid=4242, timestamp="2026-11-16T00:00:00Z",
                                 run_id="run-1", domain=173)
        carried = abstract_state(noisy_facts, identity_consistent=True)
        self.assertEqual(state_id(clean), state_id(carried))
        # Identity inconsistency is an evidence error, never a new state.
        with self.assertRaises(ValueError):
            abstract_state(base_facts(), identity_consistent=False)
        with self.assertRaises(ValueError):
            abstract_state(base_facts(endpoint_identity_consistent=False))

    def test_v19_edge_identity_uses_only_abstract_components(self):
        before = abstract_state(base_facts(), identity_consistent=True)
        after = abstract_state(base_facts(observed_match_class="single_side"), identity_consistent=True)
        first = edge_id(state_id(before), "match", "boundary_max", state_id(after))
        second = edge_id(state_id(before), "match", "boundary_max", state_id(after))
        self.assertEqual(first, second)
        self.assertNotEqual(first, edge_id(state_id(before), "match", "boundary_min", state_id(after)))

    def test_v20_incomplete_observation_has_its_own_completeness_and_no_coverage(self):
        complete = abstract_state(base_facts(), identity_consistent=True)
        incomplete = abstract_state(base_facts(observation_complete=False), identity_consistent=True)
        self.assertEqual(complete["observation_completeness"], "complete")
        self.assertEqual(incomplete["observation_completeness"], "incomplete")
        self.assertNotEqual(state_id(complete), state_id(incomplete))

        record = {"execution_stage": "judged", "typed_facts": base_facts(observation_complete=False)}
        self.assertFalse(coverage_eligible(record))
        record_complete = {"execution_stage": "judged", "typed_facts": base_facts()}
        self.assertTrue(coverage_eligible(record_complete))

    def test_v20_state_schema_is_exact(self):
        state = abstract_state(base_facts(), identity_consistent=True)
        self.assertEqual(tuple(state), STATE_COMPONENTS)
        self.assertEqual(validate_state(state), state)
        with self.assertRaises(ValueError):
            validate_state(dict(state, extra="x"))
        with self.assertRaises(ValueError):
            validate_state(dict(state, live_roles="both_alive_forever"))


class CandidateReviewV36(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.raw = pathlib.Path(cls.tmp.name) / "raw.json"
        cls.raw.write_text('{"matched":true}\n')
        cls.capsule = pathlib.Path(cls.tmp.name) / "capsule.json"
        cls.capsule.write_text('{"capsule":true}\n')
        cls.source = pathlib.Path(cls.tmp.name) / "source.txt"
        cls.source.write_text("commit\n")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def candidate(self):
        c = CandidateLifecycle("c1", observation(self.raw))
        c.admit()
        c.triage({"triage_disposition": "candidate", "responsibility_layer": "sut",
                  "reason_code": "sut_contract_violation"})
        return c

    def refs(self):
        return {"source": str(self.source), "raw": str(self.raw), "capsule": str(self.capsule)}

    def test_v36_confirmation_without_review_is_refused_when_required(self):
        c = self.candidate()
        with self.assertRaises(ValueError):
            c.confirm(adjudicator_id="reviewer", adjudication_method="peer_review",
                      confirmation_basis="non_replayable_evidence", reason="evidence retained",
                      require_review=True)
        self.assertEqual(c.candidate_status, "triaged")

    def test_v36_self_review_and_missing_artifacts_are_rejected(self):
        c = self.candidate()
        with self.assertRaises(ValueError):
            c.record_review(reviewer_id="author", conclusion="supported",
                            evidence_refs=self.refs(), independent_of=["author"])
        with self.assertRaises(ValueError):
            c.record_review(reviewer_id="reviewer", conclusion="supported",
                            evidence_refs={"source": str(self.source), "raw": str(self.raw),
                                           "capsule": str(self.capsule), "replay": "/nope.json"})
        with self.assertRaises(ValueError):
            c.record_review(reviewer_id="reviewer", conclusion="supported",
                            evidence_refs={"source": str(self.source), "raw": str(self.raw)})
        self.assertEqual(c.to_dict()["reviews"], [])

    def test_v36_needs_evidence_does_not_confirm_and_supported_review_does(self):
        c = self.candidate()
        c.record_review(reviewer_id="reviewer", conclusion="needs_evidence",
                        evidence_refs=self.refs(), independent_of=["author"])
        with self.assertRaises(ValueError):
            c.confirm(adjudicator_id="reviewer", adjudication_method="peer_review",
                      confirmation_basis="non_replayable_evidence", reason="evidence retained",
                      require_review=True)
        self.assertEqual(c.candidate_status, "triaged")
        c.record_review(reviewer_id="reviewer", conclusion="supported",
                        evidence_refs=self.refs(), independent_of=["author"])
        c.confirm(adjudicator_id="reviewer", adjudication_method="peer_review",
                  confirmation_basis="non_replayable_evidence", reason="independent review",
                  require_review=True)
        self.assertEqual(c.candidate_status, "confirmed")
        self.assertEqual(c.to_dict()["reviews"][-1]["conclusion"], "supported")
        self.assertEqual(c.to_dict()["review_id"], "review-2")


if __name__ == "__main__":
    unittest.main()
