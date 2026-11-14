"""Fixed S1 scope. A declaration is not evidence of a runnable carrier."""
from copy import deepcopy

RMWS = ("rmw_cyclonedds_cpp", "rmw_fastrtps_cpp")
EXECUTION_ORDER = (
    "discovery.late_endpoint", "discovery.late_join_delivery",
    "discovery.interruption_recovery", "builtin.discovery_projection_reconciliation",
)
EFFECT = {
    "mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
    "effect_id": "reader_created_after_writer", "window": "before_reader_match",
    "allowed_observations": ["reader_created_after_writer", "match_established"],
    "forbidden_residuals": ["post_match_delivery_loss", "wrong_endpoint_match", "identity_generation_mixup", "delivery_order_violation"],
    "fact_sources": ["endpoint_tokens", "graph_snapshot", "delivery_tokens"],
}
RULES = {
    EXECUTION_ORDER[0]: "discovery.late_endpoint.match_and_receive_after_join",
    EXECUTION_ORDER[1]: "discovery.late_join.delivery_completeness",
    EXECUTION_ORDER[2]: "discovery.recovery.resume_delivery",
    EXECUTION_ORDER[3]: "builtin.discovery.builtin_matches_projection",
}
DISPOSITIONS = {
    EXECUTION_ORDER[0]: {"status": "expressible", "reason": "public ROS2 writer/reader creation; evidence pending"},
    EXECUTION_ORDER[1]: {"status": "registered", "reason": "later slice; reliable volatile post-match delivery, no prejoin delivery obligation"},
    EXECUTION_ORDER[2]: {"status": "blocked:awaiting_endpoint_attached_discovery_capability", "reason": "requires real discovery_drop and restore on endpoint-attached discovery path; stopping publication is not discovery interruption"},
    EXECUTION_ORDER[3]: {"status": "blocked:no_independent_builtin_observation", "reason": "requires independent builtin + graph field-by-field reconciliation; graph alone insufficient; no native DDS participant permitted"},
}


def target_declarations():
    """Return core SemanticTarget instances through its JSON boundary."""
    from r2f.core.semantic_target import SemanticTarget
    targets = {}
    for target_id in EXECUTION_ORDER:
        interruption = target_id == EXECUTION_ORDER[2]
        builtin = target_id == EXECUTION_ORDER[3]
        mutations = ([{"mutation_kind": "discovery_drop", "carrier": "wire_proxy"},
                      {"mutation_kind": "restore_after_discovery_interruption", "carrier": "wire_proxy"}]
                     if interruption else [{"mutation_kind": "endpoint_qos_variation" if builtin else "late_joiner", "carrier": "ros2_endpoint"}])
        data = {
            "target_id": target_id, "target_revision": 0,
            "intent_and_scope": (
                "Independent builtin and graph discovery projections must agree field by field for identity, generation, topic, type and QoS. Graph-only observation cannot establish this contract."
                if builtin else "Actual endpoint-attached discovery drop followed by restore; observe rediscovery and reliable volatile delivery after matching; no publication-stop surrogate."
                if interruption else "Public ROS2 late reader with reliable volatile QoS: correlate endpoint identity/generation and post-match sequence delivery. Prejoin samples are outside the delivery obligation; a delivered later sentinel closes intermediate omissions; finite unmatched or in-flight tail windows are unknown."
            ),
            "state_preconditions": [
                {"condition_id": name, "fact": name, "expected": True}
                for name in ("writer_created", "writer_prejoin_published")
            ],
            "state_steps": [
                {"step_id": name, "action": name,
                 "requires": [{"condition_id": dep, "fact": dep, "expected": True} for dep in deps], "parameters": {}}
                for name, deps in (
                    ("writer_created", ["environment_ok", "locks_held"]),
                    ("writer_prejoin_published", ["writer_created"]),
                )
            ],
            "mutation_slots": mutations,
            "observation_contract": {"schema": "s1.public_pair.v1", "sources": ["endpoint_tokens", "graph_snapshot", "delivery_tokens"] if not builtin else ["independent_builtin", "graph_snapshot"], "rmw_applicability": list(RMWS), "closure": "later delivered sequence proves only intermediate gaps; unreceived tail remains unknown"},
            "expected_effect_set": deepcopy(EFFECT),
            "rule_contract": {"primary_rule": RULES[target_id], "reliable": True, "durability": "volatile", "applicability": {"rmws": list(RMWS)}, "finite_no_match": "unknown", "tail_inflight": "unknown"},
            "rmw_applicability": list(RMWS),
            "control_contract": {
                "health": {"arm": "baseline", "expected": "primary_pass"},
                "fault": {"arm": "negative", "fault_id": "late_endpoint_delivery_omission", "expected": "primary_fail", "omission": "one intermediate postmatch sequence followed by delivered sentinel", "candidate_eligible": False},
                "mutation": {"arm": "mutated", "expected": "hit"},
                "transparency": {"arm": "transparent", "expected": "no_effect_or_expected_pass", "compare": "actual_submitted_payload_sha256_and_primary_observation"},
            },
            "evidence_policy": {"raw_required": True, "raw_identity_required": True, "execution_metadata_required": True, "independent_builtin_required": builtin},
        }
        if interruption:
            data["state_steps"] = [{"step_id": action, "action": action, "requires": [], "parameters": {}} for action in ("create_endpoints", "establish_match", "apply_discovery_drop", "observe_interruption", "restore_discovery", "observe_rediscovery", "publish_postrestore", "close_observation")]
            data["state_preconditions"] = [{"condition_id": "matched_before_drop", "fact": "matched_before_drop", "expected": True}]
            data["control_contract"]["fault"]["fault_id"] = "post_restore_delivery_omission"
            data["expected_effect_set"].update(mutation_kind="discovery_drop", carrier="wire_proxy", effect_id="discovery_interruption_then_restore", window="discovery_interruption", allowed_observations=["discovery_temporarily_unavailable", "discovery_restored"], forbidden_residuals=["post_restore_discovery_inconsistency", "post_match_delivery_loss", "wrong_endpoint_match", "identity_generation_mixup"])
        if builtin:
            data["state_steps"] = [{"step_id": action, "action": action, "requires": [], "parameters": {}} for action in ("configure_endpoint_qos", "create_endpoints", "observe_graph", "observe_independent_builtin", "reconcile_identity_topic_type_qos")]
            data["state_preconditions"] = [{"condition_id": "independent_builtin_available", "fact": "independent_builtin_available", "expected": True}]
            data["control_contract"]["fault"]["fault_id"] = "controlled_projection_field_mismatch"
            data["expected_effect_set"].update(mutation_kind="endpoint_qos_variation", effect_id="public_qos_configuration", window="endpoint_creation", allowed_observations=["expected_qos_projection"], forbidden_residuals=["builtin_graph_field_mismatch"])
        targets[target_id] = SemanticTarget.from_dict(data)
    return targets
