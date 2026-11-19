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


# ---------------------------------------------------------------------------
# Exploratory targets (revision 0).
#
# These are *not* part of the required scope.  ``EXECUTION_ORDER`` and
# ``target_declarations()`` above must stay byte-identical to the frozen
# required catalog, so exploratory targets are built by a separate function
# and are only ever registered when a caller explicitly asks for them.
#
# Each contract is resolved through ``fuzz_catalog``; an unknown surface, fact,
# rule or effect raises at construction instead of silently producing a target
# that cannot be explained.
# ---------------------------------------------------------------------------

F2_WINDOWS_TARGET = "discovery.interruption_recovery.fuzz_windows_v1"
F2_WINDOWS_CAPABILITY = "f2_windows_v1"
F2_WINDOWS_RUNTIME_TARGET = "discovery.interruption_recovery"
F2_ANCHOR_TIMING_POLICY = "f2_anchor_not_before_v1"
F2_ANCHOR_LATENESS_TOLERANCE_SEC = 0.5


EXPLORATORY_TARGET_IDS = (
    "discovery.late_endpoint.fuzz_v1",
    "discovery.late_join_delivery.fuzz_v1",
    "builtin.discovery_projection_reconciliation.fuzz_v1",
    "discovery.interruption_recovery.fuzz_v1",
    "discovery.match_compatibility.fuzz_v1",
)

EXPLORATORY_TARGET_ORDER = (
    "discovery.late_endpoint.fuzz_v1",
    "discovery.late_join_delivery.fuzz_v1",
    "discovery.interruption_recovery.fuzz_v1",
    "builtin.discovery_projection_reconciliation.fuzz_v1",
    "discovery.match_compatibility.fuzz_v1",
)

#: exploratory target id -> the required target whose runtime adapter and
#: observation schema it reuses.  This is an explicit capability binding; it
#: never rewrites execution identity, which stays the new target id.
EXPLORATORY_RUNTIME_TARGET = {
    "discovery.late_endpoint.fuzz_v1": "discovery.late_endpoint",
    "discovery.late_join_delivery.fuzz_v1": "discovery.late_join_delivery",
    "discovery.interruption_recovery.fuzz_v1": "discovery.interruption_recovery",
    "builtin.discovery_projection_reconciliation.fuzz_v1":
        "builtin.discovery_projection_reconciliation",
    "discovery.match_compatibility.fuzz_v1": None,
}

#: per exploratory target: (surface ids, extra rule ids, mutation kind, carrier,
#: activation, scope statement)
_EXPLORATORY_SCOPE = {
    "discovery.late_endpoint.fuzz_v1": (
        ("P01", "W02"), (), "late_joiner", "ros2_endpoint", "F1",
        "Fuzz variant of the required late-reader target: reader delay plus "
        "bounded workload surfaces, generated through the shared mutation plan."),
    "discovery.late_join_delivery.fuzz_v1": (
        ("P01", "W02"), (), "late_joiner", "ros2_endpoint", "F1",
        "Fuzz variant of reliable volatile post-match delivery: the delivery "
        "rule is shared; workload parameters are the new input surfaces."),
    "discovery.interruption_recovery.fuzz_v1": (
        ("E01",), (), "discovery_drop", "wire_proxy", "F1",
        "Fuzz variant of discovery drop/restore with a generated drop duration. "
        "Single-window only; multi-window surfaces remain capability pending."),
    "builtin.discovery_projection_reconciliation.fuzz_v1": (
        ("P02", "P03", "P04", "P05", "P06"), (), "endpoint_qos_variation",
        "ros2_endpoint", "F1",
        "Fuzz variant of independent wire/graph QoS reconciliation. Only the "
        "six declared projection QoS fields are generatable."),
    "discovery.match_compatibility.fuzz_v1": (
        (), ("s1.discovery.compatibility_guard.v1",), "endpoint_qos_variation",
        "ros2_endpoint", "F2",
        "Revision1 offered/requested reliability pair route over public per-role QoS. "
        "M08 input activation is not live qualification; finite no-match is unknown, "
        "and best-effort carries no reliable-delivery obligation."),
}


def exploratory_target_declarations(*, runtime_capability=None):
    """Build legacy targets plus an explicitly opted-in runtime capability.

    A surface whose runtime binding is missing is still declared here (so the
    capability gap is visible and reviewable) but the returned target records
    the exact pending surface ids; the provider refuses to execute those.
    """
    from r2f.core.semantic_target import SemanticTarget
    from r2f.modules.s1 import fuzz_catalog

    from .topology_targets import TOPOLOGY_CAPABILITY

    if runtime_capability not in (None, F2_WINDOWS_CAPABILITY, TOPOLOGY_CAPABILITY):
        raise ValueError("unsupported exploratory runtime_capability: " + str(runtime_capability))
    base = target_declarations()
    targets = {}
    for target_id in EXPLORATORY_TARGET_ORDER:
        surface_ids, extra_rules, kind, carrier, activation, scope = _EXPLORATORY_SCOPE[target_id]
        if target_id == "discovery.match_compatibility.fuzz_v1":
            contract = fuzz_catalog.g01_live_contract()
            pending_surfaces = []
            declared_surfaces = ("G01",)
        elif activation == "F1":
            contract = fuzz_catalog.exploration_contract(surface_ids, extra_rule_ids=extra_rules)
            pending_surfaces = []
            declared_surfaces = tuple(surface_ids)
        else:
            # F2: the surface is declared but not generatable yet.  No surface
            # enters the core contract (that would claim a value domain and
            # mutator that do not exist), but the rule, its facts and its
            # effect must still resolve so the pending target is auditable
            # rather than an empty shell.
            contract = fuzz_catalog.exploration_contract((), extra_rule_ids=extra_rules)
            declared_surfaces = tuple(surface_ids) or ("G01",)
            pending_surfaces = list(declared_surfaces)
        runtime_target = EXPLORATORY_RUNTIME_TARGET[target_id]
        data = {
            "target_id": target_id,
            "target_revision": 1 if target_id == "discovery.match_compatibility.fuzz_v1" else 0,
            "intent_and_scope": scope,
            "state_preconditions": [
                {"condition_id": name, "fact": name, "expected": True}
                for name in ("environment_ok", "locks_held")
            ] + ([{"condition_id": "writer_created", "fact": "writer_created", "expected": True}]
                 if runtime_target in ("discovery.late_endpoint", "discovery.late_join_delivery") else []),
            "state_steps": [
                {"step_id": "admit_environment", "action": "admit_environment",
                 "requires": [{"condition_id": "environment_ok", "fact": "environment_ok", "expected": True},
                              {"condition_id": "locks_held", "fact": "locks_held", "expected": True}],
                 "parameters": {}},
            ],
            "mutation_slots": [{"mutation_kind": kind, "carrier": carrier,
                                "parameters": {}}],
            "observation_contract": {
                "schema": "s1.exploratory.v1",
                "reuses": runtime_target,
                "reuse_kind": "runtime_adapter_and_observation_schema",
                "surfaces": list(declared_surfaces),
                "pending_surface_ids": list(pending_surfaces),
                "activation": activation,
                "evidence": "no exploratory run is implied by this declaration",
            },
            "expected_effect_set": {},
            "rule_contract": {"shared_with_required": runtime_target,
                              "compatibility_rule": "s1.discovery.compatibility_guard.v1"
                              if activation == "F2" else None},
            "rmw_applicability": list(RMWS),
            "control_contract": {},
            "exploration_contract": contract,
        }
        if target_id == "discovery.match_compatibility.fuzz_v1":
            data["observation_contract"].update(
                receipt_kind="pair_compatibility_window",
                raw_recomputation="g01_runtime_observation",
                sources=["per_role_actual_qos", "fresh_bidirectional_match_and_graph",
                         "window_bound_identity_send_receive"],
                input_binding="pair.offered_requested_reliability")
        # Reuse the reviewed required contract fields where the semantics are
        # the same instead of restating (and drifting from) them.
        if runtime_target is not None:
            source = base[runtime_target]
            data["expected_effect_set"] = deepcopy(source.expected_effect_set)
            data["rule_contract"] = deepcopy(source.rule_contract)
            data["control_contract"] = deepcopy(source.control_contract)
            data["observation_contract"].update(
                required_schema=source.observation_contract.get("schema"),
                required_sources=list(source.observation_contract.get("sources", [])),
            )
            data["state_preconditions"] = [dict(p.to_dict()) for p in source.state_preconditions]
            data["state_steps"] = [dict(s.to_dict()) for s in source.state_steps]
        targets[target_id] = SemanticTarget.from_dict(data)
    if runtime_capability == F2_WINDOWS_CAPABILITY:
        target = f2_windows_target_declaration()
        targets[target.target_id] = target
    if runtime_capability == TOPOLOGY_CAPABILITY:
        from .topology_targets import topology_target_declaration
        target = topology_target_declaration()
        targets[target.target_id] = target
    return targets


def f2_windows_target_declaration():
    """New rev0 window target; never mutate a legacy declaration in place."""
    from r2f.core.semantic_target import SemanticTarget
    from .fuzz_catalog import f2_live_contract

    source = target_declarations()[F2_WINDOWS_RUNTIME_TARGET]
    data = source.to_dict()
    data.update(
        target_id=F2_WINDOWS_TARGET, target_revision=0,
        intent_and_scope=(
            "Explicit F2 E02/M07 window count/restore interval and E03/M06 "
            "anchor offset over 1..3 discovery-drop windows. Unresolved runtime "
            "guards require fresh pair-bound raw evidence before each action; "
            "planning is not reached, mutation application, hit or execution."),
        mutation_slots=[{"mutation_kind": "discovery_drop", "carrier": "wire_proxy",
                         "parameters": {}}],
        exploration_contract=f2_live_contract(),
        observation_contract={
            "schema": "s1.exploratory.v1",
            "reuses": source.target_id,
            "reuse_kind": "runtime_adapter_and_observation_schema",
            "required_schema": source.observation_contract["schema"],
            "required_sources": list(source.observation_contract["sources"]),
            "surfaces": ["E02", "E03"], "pending_surface_ids": [],
            "activation": "F2", "runtime_capability": F2_WINDOWS_CAPABILITY,
            "max_runtime_windows": 3, "max_chain_length": 1,
            "surface_dependencies": {
                "E03": {"requires_surface_ids": ["E02"],
                        "dependency_kind": "target_capability_and_complete_window_input",
                        "operation_order_if_composed": ["E02", "E03"]}},
            "runtime_guards": {
                "E02": "fresh_match_and_previous_window_restored_before_drop",
                "E03": "named_anchor_observed_before_eligible_drop"},
            "anchor_timing_policy": F2_ANCHOR_TIMING_POLICY,
            "anchor_lateness_tolerance_sec": F2_ANCHOR_LATENESS_TOLERANCE_SEC,
            "anchor_timing_contract": {
                "fresh_match": {
                    "raw_event": "discovery_precondition_snapshot",
                    "identity": "fresh_bidirectional_match_bound_to_current_pair",
                    "not": "earliest_unobserved_network_match"},
                "previous_restore_confirmed": {
                    "raw_event": "discovery_recovery_confirmed",
                    "window": "immediately_previous_window"},
                "clock": "monotonic",
                "eligible_drop": "max(anchor_monotonic + trigger_offset_sec, "
                                 "previous_recovery_monotonic + previous_restore_interval_sec)",
                "first_window_previous_recovery_bound": "not_applicable",
                "actual_install": "eligible_drop <= install <= eligible_drop + tolerance",
                "timing_miss": "harness_incomplete_not_sut_candidate",
                "verification": "runtime_and_adapter_independent_raw_recomputation"},
            "operation_hit_contract": {
                "E02": "exact_window_count_and_restore_intervals",
                "E03": {
                    "anchor": "parameters.anchor selects an anchor class, not the first window",
                    "affected_window_indices": "[i for i,w in enumerate(environment_windows) "
                                               "if w.trigger_anchor == parameters.anchor]",
                    "offset": "every affected window trigger_offset_sec == parameters.offset_sec",
                    "empty_affected_window_set": "input_rejected",
                    "fresh_match_indices": [0],
                    "previous_restore_confirmed_indices": "1..count-1 (requires count>=2)"},
                "each_window": ["pair_bound_nonzero_discovery_drop_counter",
                                "restore_discovery_activity", "fresh_graph_and_bidirectional_match",
                                "postrestore_delivery"],
                "aggregate_or_window_count_only_hit": False},
            "evidence": "no exploratory run is implied by this declaration",
        },
    )
    return SemanticTarget.from_dict(data)
