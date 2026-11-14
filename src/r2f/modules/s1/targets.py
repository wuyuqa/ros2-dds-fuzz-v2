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
    EXECUTION_ORDER[2]: {"status": "expressible", "reason": "reviewed namespace discovery-drop pipeline; live namespace, packet observer and pair-binding admission required; execution and per-target seal remain unestablished"},
    EXECUTION_ORDER[3]: {"status": "expressible", "reason": "passive independent builtin SEDP + public graph projection pipeline; namespace/observer/raw field admission and execution remain required; no per-target acceptance established"},
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
            before_drop = (("create_endpoints", ["environment_ok", "locks_held", "namespace_ready"]),
                           ("establish_match", ["create_endpoints"]),
                           ("verify_namespace_bindings", ["establish_match"]))
            data["state_steps"] = [
                {"step_id": name, "action": name,
                 "requires": [{"condition_id": dep, "fact": dep, "expected": True} for dep in deps], "parameters": {}}
                for name, deps in before_drop]
            data["state_preconditions"] = [{"condition_id": name, "fact": name, "expected": True} for name, _ in before_drop]
            data["observation_contract"].update(
                schema="s1.discovery_recovery.v1",
                sources=["endpoint_tokens", "namespace_bindings", "discovery_drop_counter", "independent_discovery_activity", "graph_snapshot", "delivery_tokens"],
                recovery="verified rule deletion plus post-boundary identity-correlated discovery activity, fresh dual count/graph and delivery; retained match continuity is allowed",
                post_mutation_receipts=["discovery_drop_begin", "discovery_drop_end", "discovery_recovery_confirmed"],
                finite_recovery_timeout="unknown")
            data["control_contract"]["fault"].update(
                fault_id="post_restore_delivery_omission", receipt_kind="delivery_omission", window="post_restore_delivery",
                omission="one intermediate postrestore sequence followed by delivered sentinel; neutral discovery carrier")
            data["expected_effect_set"].update(
                mutation_kind="discovery_drop", carrier="wire_proxy", effect_id="discovery_interruption_then_restore",
                window="discovery_interruption", allowed_observations=["discovery_temporarily_unavailable", "discovery_restored"],
                forbidden_residuals=["post_restore_discovery_inconsistency", "post_restore_delivery_loss", "wrong_endpoint_match", "identity_generation_mixup", "delivery_order_violation"],
                fact_sources=["endpoint_tokens", "namespace_bindings", "discovery_drop_counter", "independent_discovery_activity", "graph_snapshot", "delivery_tokens"])
        if builtin:
            # Initial revision0 contract, before first seal: only real pre-create
            # admission belongs in core state steps (D010/D014).
            before_create = (("namespace_ready", ["environment_ok", "locks_held"]),
                             ("observer_ready", ["namespace_ready"]))
            data["state_steps"] = [{"step_id": name, "action": name,
                "requires": [{"condition_id": dep, "fact": dep, "expected": True} for dep in deps], "parameters": {}}
                for name, deps in before_create]
            data["state_preconditions"] = [{"condition_id": name, "fact": name, "expected": True} for name, _ in before_create]
            data["observation_contract"] = {
                "schema": "s1.independent_projection.v1", "sources": ["passive_rtps_wire", "ros_graph", "endpoint_create_lifecycle"],
                "required_fields": ["endpoint_guid", "role", "topic", "type", "reliability", "durability", "deadline", "liveliness.kind", "liveliness.lease_duration"],
                "history_depth": "not_applicable_by_endpoint_schema", "rmw_applicability": list(RMWS),
                "generation": "independent create-call begin/return bound to process identity; never a wire field",
                "post_mutation_receipts": ["endpoint_create_begin", "endpoint_created", "projection_observation"],
                "matching": "offered_requested and actual_match are diagnostics only; finite no-match does not prove violation"}
            data["rule_contract"] = {"primary_rule": RULES[target_id], "applicability": {"rmws": list(RMWS)},
                "primary": "independent wire_graph_field_equality", "missing_or_unqualified_observation": "unknown"}
            data["control_contract"]["fault"].update(fault_id="controlled_projection_field_mismatch", receipt_kind="projection_field_mismatch",
                expected="primary_fail", original_sources="both immutable wire and graph preserved; alter one derived comparison only")
            data["control_contract"]["fault"].pop("omission")
            data["expected_effect_set"].update(mutation_kind="endpoint_qos_variation", effect_id="public_qos_configuration", window="endpoint_creation",
                allowed_observations=["expected_qos_projection"], forbidden_residuals=["builtin_graph_field_mismatch"],
                fact_sources=["passive_rtps_wire", "ros_graph", "endpoint_create_lifecycle"])
        targets[target_id] = SemanticTarget.from_dict(data)
    return targets
