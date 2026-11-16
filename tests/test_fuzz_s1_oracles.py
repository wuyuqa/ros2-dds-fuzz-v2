"""G01 compatibility guard accepts/rejects/unknowns (V28-V30)."""
import copy
import unittest

from r2f.modules.s1.oracles import COMPATIBILITY_RULE_ID, evaluate_match_compatibility_guard


def base_facts(offered="best_effort", requested="reliable"):
    return {
        "writer": {"id": "writer-1", "generation": 0, "created_ns": 10},
        "reader": {"id": "reader-1", "generation": 0, "created_ns": 20},
        "submitted": {"writer_reliability": offered, "reader_reliability": requested},
        "window": {"start_ns": 30, "end_ns": 1000, "complete": True},
        "match": None,
        "delivery": None,
    }


def matched(facts):
    facts["match"] = {"identity_correlated": True, "writer_id": "writer-1",
                      "writer_generation": 0, "reader_id": "reader-1",
                      "reader_generation": 0, "at_ns": 50}
    return facts


def delivered(facts, sequences=(0, 1)):
    matched(facts)
    facts["delivery"] = {
        "identity_correlated": True,
        "sent": [{"sequence": s, "writer_id": "writer-1", "writer_generation": 0} for s in sequences],
        "received": [{"sequence": s, "writer_id": "writer-1", "reader_id": "reader-1",
                      "writer_generation": 0, "reader_generation": 0} for s in sequences],
    }
    return facts


class CompatibilityGuard(unittest.TestCase):
    def test_explicit_rule_and_unknown_reasons(self):
        result = evaluate_match_compatibility_guard(base_facts())
        self.assertEqual(result["rule_id"], COMPATIBILITY_RULE_ID)
        self.assertEqual(result["rule_id"], "s1.discovery.compatibility_guard.v1")
        self.assertNotEqual(result["rule_id"], "discovery.late_endpoint.match_and_receive_after_join")
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("prohibited_event_absence_not_established", result["unknown_reasons"])
        self.assertEqual(result["applicability"]["compatible"], False)

    def test_v28_br_create_success_is_not_itself_a_bug(self):
        facts = base_facts()
        facts["create"] = {"writer": "succeeded", "reader": "succeeded"}
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "unknown", result)
        self.assertEqual(result["forbidden_residuals"], [])
        self.assertIn("prohibited_event_absence_not_established", result["reason_codes"])

    def test_v28b_br_absence_pass_requires_explicit_proven_observation(self):
        facts = base_facts()
        facts["observed"] = {"match": "absent", "delivery": "absent"}
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("prohibited_event_absence_not_established", result["unknown_reasons"])
        facts["observed"].update(observer_complete=True, provenance=True)
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "pass")
        self.assertEqual(result["reason_codes"], ["incompatible_pair_not_matched"])

    def test_v29_identity_backed_prohibited_match_fails(self):
        facts = matched(base_facts())
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "fail", result)
        self.assertEqual(result["forbidden_residuals"], ["prohibited_match_observed"])

    def test_v29b_identity_backed_prohibited_delivery_fails_and_outranks_match(self):
        facts = delivered(base_facts())
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "fail")
        self.assertEqual(result["forbidden_residuals"], ["prohibited_delivery_observed"])
        self.assertEqual(result["reason_codes"], ["prohibited_delivery_observed"])

    def test_v29c_uncorrelated_evidence_is_unknown_not_fail(self):
        facts = matched(base_facts())
        facts["match"]["identity_correlated"] = False
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("match_without_identity_correlation", result["unknown_reasons"])

        facts = base_facts()
        facts["match"] = {"identity_correlated": True, "writer_id": "writer-1",
                          "writer_generation": 7, "reader_id": "reader-1",
                          "reader_generation": 0, "at_ns": 50}
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("match_identity_mismatch", result["unknown_reasons"])

    def test_v30_compatible_finite_timeout_without_match_is_unknown(self):
        for pair in (("reliable", "reliable"), ("reliable", "best_effort"), ("best_effort", "best_effort")):
            facts = base_facts(*pair)
            result = evaluate_match_compatibility_guard(facts)
            self.assertEqual(result["verdict"], "unknown", pair)
            self.assertIn("compatible_pair_unmatched_within_finite_window", result["reason_codes"])
            self.assertEqual(result["forbidden_residuals"], [])

    def test_v30b_incomplete_window_is_unknown_even_with_compatible_match(self):
        facts = matched(base_facts("reliable", "reliable"))
        facts["window"]["complete"] = False
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("observation_window_incomplete", result["unknown_reasons"])

    def test_compatible_match_passes_and_reports_rule(self):
        facts = matched(base_facts("reliable", "reliable"))
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "pass")
        self.assertEqual(result["reason_codes"], ["compatible_pair_matched"])
        self.assertTrue(result["closure"]["match_correlated"])

    def test_missing_or_malformed_evidence_is_unknown(self):
        for remove in ("writer", "reader", "submitted", "window"):
            facts = base_facts()
            facts.pop(remove)
            self.assertEqual(evaluate_match_compatibility_guard(facts)["verdict"], "unknown", remove)
        facts = base_facts()
        facts["window"]["complete"] = "true"
        self.assertEqual(evaluate_match_compatibility_guard(facts)["verdict"], "unknown")
        facts = base_facts()
        facts["submitted"]["writer_reliability"] = "sometimes"
        self.assertEqual(evaluate_match_compatibility_guard(facts)["verdict"], "unknown")
        facts = delivered(base_facts())
        facts["delivery"]["received"].append(copy.deepcopy(facts["delivery"]["received"][0]))
        self.assertEqual(evaluate_match_compatibility_guard(facts)["verdict"], "unknown")

    def test_delivery_without_match_is_unknown_not_pass(self):
        facts = base_facts()
        facts["delivery"] = {"identity_correlated": True,
                             "sent": [{"sequence": 0, "writer_id": "writer-1", "writer_generation": 0}],
                             "received": [{"sequence": 0, "writer_id": "writer-1", "reader_id": "reader-1",
                                           "writer_generation": 0, "reader_generation": 0}]}
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "unknown")
        self.assertIn("delivery_observed_without_correlated_match", result["unknown_reasons"])

    def test_old_primary_oracle_is_unchanged_by_new_evaluator(self):
        from r2f.modules.s1.oracles import evaluate_primary
        result = evaluate_primary({})
        self.assertEqual(result["rule_id"], "discovery.late_endpoint.match_and_receive_after_join")
        self.assertEqual(result["verdict"], "unknown")


if __name__ == "__main__":
    unittest.main()
