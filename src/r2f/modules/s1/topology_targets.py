"""Explicit topology target declaration; input support is not runtime evidence."""
from r2f.core import SemanticTarget

TOPOLOGY_TARGET = "discovery.endpoint_topology.fuzz_v1"
TOPOLOGY_CAPABILITY = "topology_v1"
TOPOLOGY_MUTATION_KIND = "endpoint_program"
TOPOLOGY_CARRIER = "ros2_endpoint"
TOPOLOGY_SURFACE = "S1.topology.program"
TOPOLOGY_MUTATOR = "I2.topology_program.v1"
TOPOLOGY_BINDING = "topology.program"


def topology_target_declaration():
    from .topology_input import (topology_contract, topology_effect_spec,
                                 topology_rule_spec, TOPOLOGY_FACT_IDS,
                                 TOPOLOGY_FACT_PRODUCER, TOPOLOGY_EVALUATOR)

    def requires(*names):
        return [{"condition_id": name, "fact": name, "expected": True} for name in names]

    ready = requires("environment_ok", "locks_held", "source_identity_ok",
                     "topology_capability_admitted")
    return SemanticTarget.from_dict({
        "target_id": TOPOLOGY_TARGET, "target_revision": 0,
        "intent_and_scope": "Opt-in bounded endpoint topology/action programs over public APIs. "
                            "Plans and policy predictions are inputs, never observations or SUT candidates.",
        "state_preconditions": ready + requires("topology_admitted", "topology_input_bound"),
        "state_steps": [
            {"step_id": "admit_topology", "action": "admit_environment", "requires": ready,
             "parameters": {"runtime_capability": TOPOLOGY_CAPABILITY,
                            "after_updates": {"topology_admitted": True}}},
            {"step_id": "bind_topology_input", "action": "bind_input",
             "requires": ready + requires("topology_admitted"),
             "parameters": {"binding": TOPOLOGY_BINDING,
                            "required_sources": ["input_binding.receipt.program_sha256",
                                                 "input_binding.receipt.route_binding"],
                            "after_updates": {"topology_input_bound": True}}},
        ],
        "mutation_slots": [{"mutation_kind": TOPOLOGY_MUTATION_KIND, "carrier": TOPOLOGY_CARRIER,
                            "parameters": {}}],
        "observation_contract": {
            "schema": "r2f.topology-observation/v1", "runtime_capability": TOPOLOGY_CAPABILITY,
            "backend": "topology_pair", "surface_id": TOPOLOGY_SURFACE,
            "mutator_id": TOPOLOGY_MUTATOR, "runtime_binding": TOPOLOGY_BINDING,
            "max_chain_length": 4, "final_input_keys": ["topology_program"],
            "input_mutation_phase": "compile_time",
            "precondition_receipt_scope": "environment_and_sealed_input_before_mutation_begin",
            "post_mutation_obligations": [
                {"obligation_id": "apply_endpoint_program", "action": "apply_mutation",
                 "requires": ["topology_input_bound"], "runtime_binding": TOPOLOGY_BINDING,
                 "evidence": ["per_action_receipts", "program_content_hash", "topology_route.application_audit"]},
                {"obligation_id": "observe_topology", "action": "collect_observations",
                 "evidence": ["raw_event_identity", "endpoint_generation_binding",
                              "observed_graph_not_policy_prediction", "delivery_closure"]},
                {"obligation_id": "judge_topology", "action": "evaluate_rules",
                 "rule_id": topology_rule_spec().id, "fact_ids": list(TOPOLOGY_FACT_IDS),
                 "evidence": ["raw_backed_observations", "resolved_primary_oracle"]},
            ],
            "required_sources": ["topology_pair.raw_events", "topology_route.application_audit"],
            "requires": ["raw_event_identity", "per_action_receipts", "endpoint_generation_binding",
                         "program_content_hash", "observed_graph_not_policy_prediction"],
            "fact_producer": "r2f.campaign.topology_adapter.TopologyFactProducer",
            "missing_policy": "unknown_not_pass", "declaration_is_execution_evidence": False,
        },
        "expected_effect_set": {
            **topology_effect_spec().to_dict(), "mutation_kind": TOPOLOGY_MUTATION_KIND,
            "requires": ["runtime_program_application", "raw_backed_observations"],
            "prediction_only_is_effect": False,
        },
        "rule_contract": {
            "oracle": "r2f.modules.s1.topology_oracles.TopologyOracle",
            "primary_rule": topology_rule_spec().id,
            "rule_ids": [topology_rule_spec().id],
            "rules": [topology_rule_spec().to_dict()],
            "fact_ids": list(TOPOLOGY_FACT_IDS), "fact_producer_id": TOPOLOGY_FACT_PRODUCER,
            "evaluator_id": TOPOLOGY_EVALUATOR,
            "requires": ["independent_raw_observations", "endpoint_generation_binding"],
            "missing_policy": "unknown_not_pass", "residual_priority": True,
            "guard_negative_is_natural_candidate": False,
        },
        "evidence_policy": {
            "required": ["source_manifest", "input_layer_receipts", "effective_input_sha256",
                         "raw_path", "raw_sha256", "execution_metadata", "per_action_receipts"],
            "input_receipt_is_execution_receipt": False,
            "policy_prediction_is_observation": False, "candidate_requires_real_execution": True,
            "retain_failures": True,
        },
        "rmw_applicability": ["rmw_cyclonedds_cpp", "rmw_fastrtps_cpp"],
        "control_contract": {"guard_negative_candidate_eligible": False,
                             "required_catalog_included": False,
                             "required_maturity_denominator_changed": False},
        "exploration_contract": topology_contract().to_dict(),
    })
