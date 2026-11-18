"""Common completeness and independently gated state provenance; offline only."""
import copy
import unittest
from r2f.core.observation_status import assess_facts, assess_observation, observation_complete
from r2f.core.state_signature import STATE_COMPONENTS, observation_edge, coverage_eligible
from r2f.campaign.feedback import FeedbackLog


def healthy_record():
    return {"execution_stage": "judged", "actual_run": True, "oracle_verdict": "pass",
            "content_sha256": "c" * 64, "surface_id": "P01", "boundary_class": "finite",
            "raw_path": "/offline/raw.jsonl", "raw_sha256": "a" * 64,
            "typed_facts": {"environment_healthy": True, "process_healthy": True,
                            "preconditions_reached": True, "endpoint_identity_complete": True,
                            "window": {"complete": True}}}


def projected_record():
    record = healthy_record()
    state = dict(zip(STATE_COMPONENTS, ("both", "writer_first", "compatible", "bidirectional",
                                       "normal", "0", "closed_within_contract", "complete")))
    facts = record["typed_facts"]
    facts.update(state_before=copy.deepcopy(state), state_after=copy.deepcopy(state))
    def ref(index):
        return {"path": record["raw_path"], "sha256": record["raw_sha256"], "event_index": index}
    def binding(index):
        return {"boundary_ref": ref(index),
                "identity": {role: {"endpoint_gid": role + "-gid", "pid": n,
                                    "endpoint_generation": role + "-generation", "endpoint_ref": ref(0)}
                             for n, role in enumerate(("writer", "reader"), 1)},
                "component_refs": {k: {"status": "observed", "refs": [ref(0)], "reason": None}
                                   for k in STATE_COMPONENTS}}
    for side in ("before", "after"):
        facts["state_" + side].update(observation_complete=True, window_complete=True,
                                     endpoint_identity_consistent=True)
    facts["state_projection_receipt"] = {
        "schema": "r2f.s1-state-projection/v1", "status": "observed", "scope": "canonical_pair",
        "raw_path": record["raw_path"], "raw_sha256": record["raw_sha256"],
        "before": binding(1), "after": binding(2), "reasons": []}
    return record


class ObservationStatus(unittest.TestCase):
    def test_legacy_nested_complete_without_flat_flags(self):
        r = healthy_record()
        self.assertTrue(assess_facts(r["typed_facts"])["complete"])
        self.assertTrue(observation_complete(r))
        self.assertTrue(coverage_eligible(r))
        self.assertIsNone(observation_edge(r))

    def test_every_required_fact_and_conflicting_optional_flag(self):
        for key in ("environment_healthy", "process_healthy", "preconditions_reached",
                    "endpoint_identity_complete", "observation_complete", "identity_consistent",
                    "endpoint_identity_consistent", "window_complete"):
            for value in (False, None, 1, "true", [], {}):
                with self.subTest(key=key, value=value):
                    r = healthy_record(); r["typed_facts"][key] = value
                    self.assertFalse(observation_complete(r))
                    self.assertFalse(coverage_eligible(r))
                    self.assertFalse(FeedbackLog().record(r)["coverage"])

    def test_window_representations_must_agree(self):
        for name in ("window", "window_completion"):
            for value in ({"complete": False}, {"complete": None}, {}, None, True):
                r = healthy_record()
                r["typed_facts"].update(window_complete=True, **{name: value})
                self.assertFalse(observation_complete(r), (name, value))
        r = healthy_record(); r["typed_facts"].pop("window")
        r["typed_facts"]["window_completion"] = {"complete": True}
        self.assertTrue(observation_complete(r))

    def test_execution_flags_tool_and_evidence_errors(self):
        for fields in ({"actual_run": False}, {"actual_run": False, "workspace_executed": True},
                       {"actual_run": True, "workspace_executed": False}, {"actual_run": 1},
                       {"workspace_executed": "yes"}, {"execution_stage": "planned"},
                       {"tool_error": True}, {"oracle_tool_failure": "broken"},
                       {"runtime_status": "tool_error"}, {"evidence_error": "raw mismatch"}):
            r = healthy_record(); r.update(fields)
            self.assertFalse(observation_complete(r), fields)
            self.assertTrue(assess_observation(r)["reasons"])
        r = healthy_record(); r.pop("actual_run"); r["workspace_executed"] = True
        self.assertTrue(observation_complete(r))
        r["typed_facts"]["evidence_error"] = "contradiction"
        self.assertFalse(observation_complete(r))
        for value in (None, [], "bad", False):
            self.assertFalse(assess_observation(value)["complete"])

    def test_state_edge_requires_both_states_and_receipt(self):
        r = projected_record()
        self.assertIsNotNone(observation_edge(r))
        for mutate in (lambda r: r["typed_facts"].pop("state_before"),
                       lambda r: r["typed_facts"]["state_after"].pop("network_phase"),
                       lambda r: r["typed_facts"].pop("state_projection_receipt"),
                       lambda r: r["typed_facts"]["state_projection_receipt"].update(status="incomplete"),
                       lambda r: r["typed_facts"]["state_projection_receipt"].update(raw_sha256="b"*64),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["after"].update(component_refs={})):
            changed = copy.deepcopy(r); mutate(changed)
            self.assertIsNone(observation_edge(changed))
        r["typed_facts"]["state_after"]["network_phase"] = "unknown"
        self.assertIsNotNone(observation_edge(r))  # explicit uncertainty, not a default
        r["typed_facts"]["state_after"]["network_phase"] = "not_applicable"
        self.assertIsNone(observation_edge(r))  # unsupported domain is not silently coerced

    def test_projection_absence_identity_and_reference_binding(self):
        r = projected_record()
        before = r["typed_facts"]["state_before"]
        before.update(live_roles="none", creation_order_class="unresolved",
                      compatibility_class="unknown", observed_match_class="none")
        r["typed_facts"]["state_projection_receipt"]["before"]["identity"] = {}
        self.assertIsNotNone(observation_edge(r))
        for mutate in (lambda r: r["typed_facts"]["state_after"].update(window_complete=False),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["before"]["boundary_ref"].update(event_index=True),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["component_refs"]["network_phase"]["refs"][0].update(path="other"),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["component_refs"]["network_phase"]["refs"][0].update(sha256="b"*64),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["component_refs"]["network_phase"]["refs"][0].update(event_index=999),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["component_refs"].update(network_phase=True),
                       lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["component_refs"]["network_phase"].update(status="not_applicable", reason=None)):
            changed = copy.deepcopy(r); mutate(changed)
            self.assertIsNone(observation_edge(changed))
        r["typed_facts"]["state_before"]["live_roles"] = "both"
        self.assertIsNone(observation_edge(r))

    def test_feedback_does_not_trust_injected_edge(self):
        r = healthy_record(); r["observed_edge"] = "forged"
        self.assertIsNone(FeedbackLog().record(r)["edge_id"])
        r = projected_record(); event = FeedbackLog().record(r)
        self.assertEqual(event["edge_id"], observation_edge(event["edge_provenance"]["observation"])[1])


if __name__ == "__main__":
    unittest.main()
