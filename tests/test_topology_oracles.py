"""Independent rule/evaluator tests over NON-real raw and offline-backend fixtures."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from test_topology_adapter import RawFixture
from r2f.modules.s1.topology_input import (
    TOPOLOGY_TARGET, TOPOLOGY_EVALUATOR, TOPOLOGY_RULE, TOPOLOGY_EFFECT,
    TOPOLOGY_FACT_PRODUCER, TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY,
    topology_contract,
)
from r2f.modules.s1.topology_oracles import (
    TopologyOracle, evaluate_topology, resolve_evaluator, resolve_fact, resolve_fact_producer,
)


def evaluate(f, tmp_path):
    facts = f.facts(tmp_path)
    return facts, TopologyOracle().evaluate(f.scenario, facts)


def test_real_fact_and_evaluator_resolvers_match_core_declarations(tmp_path):
    f = RawFixture()
    facts, result = evaluate(f, tmp_path)
    c = topology_contract()
    assert result["rule_id"] == c.rules[0].id == TOPOLOGY_RULE
    assert result["evaluator_id"] == c.rules[0].evaluator_id == TOPOLOGY_EVALUATOR
    assert result["effect_id"] == c.effects[0].effect_id == TOPOLOGY_EFFECT
    assert resolve_evaluator(TOPOLOGY_EVALUATOR) is evaluate_topology
    assert type(resolve_fact_producer(TOPOLOGY_FACT_PRODUCER)).__name__ == "TopologyFactProducer"
    assert all(resolve_fact(spec.id, facts) in (True, False, None) for spec in c.facts)
    assert resolve_fact(TOPOLOGY_FACT_GRAPH, facts) is True
    assert resolve_fact(TOPOLOGY_FACT_DELIVERY, facts) is True
    assert facts["fact_values"][TOPOLOGY_FACT_GRAPH] is True
    with pytest.raises(ValueError, match="unknown_topology_evaluator"):
        resolve_evaluator(TOPOLOGY_EVALUATOR + ".alias")
    with pytest.raises(ValueError, match="unknown_topology_fact"):
        resolve_fact("unregistered", facts)
    with pytest.raises(ValueError, match="unknown_topology_fact_producer"):
        resolve_fact_producer("unregistered")


@pytest.mark.parametrize("writers,readers,publisher_gid", [(1, 1, True), (2, 2, True), (2, 2, False)])
def test_positive_complete_raw_has_diagnostic_pass_not_all_unknown(tmp_path, writers, readers, publisher_gid):
    facts, result = evaluate(RawFixture(writers=writers, readers=readers, publisher_gid=publisher_gid), tmp_path)
    assert result["verdict"] == "pass", (facts, result)
    assert result["diagnostic_only"] and not result["candidate_eligible"] and not result["seal_eligible"]
    assert not facts["execution_evidence"] and not facts["candidate_eligible"]


@pytest.mark.parametrize("publisher_gid", [True, False])
def test_closed_intermediate_gap_is_fail_only_with_same_writer_sentinel_and_contract(tmp_path, publisher_gid):
    facts, result = evaluate(RawFixture(missing=(2,), publisher_gid=publisher_gid), tmp_path)
    assert facts["pairs"][0]["delivery_closure"] == "closed_with_gap", facts
    assert result["verdict"] == "fail", result
    assert result["forbidden_residuals"] == ["topology.closed_delivery_gap"]
    assert result["closure"]["pairs"][0]["missing"] == [2]
    assert not result["candidate_eligible"]


def test_finite_tail_missing_is_unknown_never_a_failure(tmp_path):
    _, result = evaluate(RawFixture(missing=(3,)), tmp_path)
    assert result["verdict"] == "unknown"
    assert not result["forbidden_residuals"]


def test_insufficient_observed_history_capacity_cannot_prove_delivery_violation(tmp_path):
    facts, result = evaluate(RawFixture(missing=(2,), depth=2), tmp_path)
    assert not facts["pairs"][0]["reliable_history_contract"]
    assert result["verdict"] == "unknown"


def test_fresh_aggregate_count_and_graph_never_mean_n_directed_match(tmp_path):
    facts, result = evaluate(RawFixture(writers=2, readers=2, direct=False), tmp_path)
    assert result["verdict"] == "unknown"
    assert resolve_fact(TOPOLOGY_FACT_GRAPH, facts) is None
    assert not result["forbidden_residuals"]


def test_incompatible_observed_policies_plus_independent_peer_or_delivery_evidence_fail(tmp_path):
    _, result = evaluate(RawFixture(incompatible=True), tmp_path)
    assert result["verdict"] == "fail", result
    assert "topology.pair_graph_mismatch" in result["forbidden_residuals"]


def test_attributed_payload_delivery_proves_only_that_pair_not_other_edges(tmp_path):
    f = RawFixture(writers=2, readers=2, publisher_gid=False, direct=False, incompatible=True)
    facts, result = evaluate(f, tmp_path)
    assert all(p["observed_match"] == "unknown" for p in facts["pairs"])
    assert result["verdict"] == "fail"
    assert "topology.pair_graph_mismatch" in result["forbidden_residuals"]
    assert all(d["basis"] == "endpoint_correlated_payload" for d in facts["deliveries"])


def test_source_unfrozen_preserves_diagnostic_oracle_but_not_eligibility(tmp_path):
    facts, result = evaluate(RawFixture(), tmp_path)
    assert result["verdict"] == "pass"
    assert not facts["source_provenance_qualified"]
    assert not facts["candidate_eligible"] and result["diagnostic_only"]


def test_tool_failure_is_unknown_harness_not_sut(tmp_path):
    f = RawFixture()
    end = f.events.pop()
    f.add("worker_error", error="controlled_fixture_tool_error")
    f.add("runtime_end", **{k: end[k] for k in ("mutation_receipt", "cleanup_complete", "real_execution")})
    facts, result = evaluate(f, tmp_path)
    assert facts["tool_error"]
    assert result["verdict"] == "unknown" and result["responsibility_layer"] == "harness"
    assert result["reason_codes"] == ["topology_tool_failure"]
    assert not result["forbidden_residuals"]


def test_harness_guard_is_not_claimed_as_sut_rejection(tmp_path):
    from test_topology_cycle import SOURCE, capsule_from_registry
    from r2f.core import ReplayCapsule, Scenario
    from r2f.campaign.replay import _replay_config
    from r2f.campaign.topology_adapter import TopologyFactProducer
    from r2f.modules.s1 import topology_model as model
    from r2f.modules.s1.topology_input import make_topology_plan
    from r2f.runtime.ros2_pair import run_pair
    from r2f.runtime.topology_worker import OfflineTopologyBackend

    registry, original, _, _ = capsule_from_registry(1)
    parent = model.generate_program(3, writers=1, readers=1)
    # Valid model input requiring the optional native ownership carrier, which
    # this public-only route has not selected. KEEP_ALL is now supported.
    # The offline route emits bound before/guard/after/action receipts; a loose
    # guard event appended to a successful trace is correctly evidence-invalid.
    for endpoint in parent["endpoints"]:
        if endpoint["role"] == "writer":
            endpoint["qos"]["ownership"] = "exclusive"
    plan = make_topology_plan(parent, [{"operator_id": "action.insert"}], rng_seed=7)
    scenario = Scenario(original.target, original.rmw, "mutated", 0, 7,
        {"mutation_kind": "endpoint_program", "carrier": "ros2_endpoint",
         "mutation_parameters": plan.final_config}, plan)
    prepared = registry.get_services(scenario.target_id).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
        final_input=plan.final_config, input_layer_receipts=prepared["input_layer_receipts"], origin_domain=80)
    config = _replay_config(capsule, "offline-guard", 80, SOURCE)
    runtime = run_pair(config, tmp_path / "offline-runtime", topology_backend=OfflineTopologyBackend())
    facts = TopologyFactProducer().produce({"events": runtime["events"], "runtime": runtime, "config": config})
    result = TopologyOracle().evaluate(scenario, facts)
    assert facts["diagnostic_valid"], facts.get("evidence_error")
    assert facts["fixture"] and not facts["execution_evidence"]
    assert not facts["tool_error"] and facts["guard_rejected_by"] == "harness"
    assert facts["control_excluded"]
    assert result["verdict"] == "not_applicable"
    assert result["responsibility_layer"] == "harness"
    assert "not_sut_rejection" in result["reason_codes"][0]
    assert not result["candidate_eligible"] and not result["forbidden_residuals"]


def test_harness_fault_always_excluded_even_when_rule_fails(tmp_path):
    f = RawFixture(missing=(2,))
    f.config["control_kind"] = "fault"
    facts, result = evaluate(f, tmp_path)
    assert result["verdict"] == "fail"
    assert facts["control_excluded"] and not facts["candidate_eligible"]
    assert result["responsibility_layer"] == "harness" and not result["candidate_eligible"]
    negative = SimpleNamespace(target_id=TOPOLOGY_TARGET, arm="negative")
    assert not TopologyOracle().evaluate(negative, facts)["candidate_eligible"]


def test_malformed_or_alias_facts_never_fall_back_to_runtime_summary(tmp_path):
    f = RawFixture()
    _, result = evaluate(f, tmp_path)
    assert result["verdict"] == "pass"
    assert TopologyOracle().evaluate(f.scenario, {"verdict": "pass", "facts": {}})["verdict"] == "unknown"
    alias = SimpleNamespace(target_id=TOPOLOGY_TARGET + ".alias", arm="mutated")
    assert TopologyOracle().evaluate(alias, f.facts(tmp_path))["verdict"] == "not_applicable"


def test_residual_outranks_simultaneous_allowed_effect_and_unrelated_unknown(tmp_path):
    facts, result = evaluate(RawFixture(missing=(2,)), tmp_path)
    facts["observed_effects"] = ["observed_consistency"]
    unknown = deepcopy(facts["pairs"][0])
    unknown.update(writer="unrelated", observed_match="unknown", delivery_closure="unknown")
    facts["pairs"].append(unknown)
    result = TopologyOracle().evaluate(RawFixture().scenario, facts)
    assert result["verdict"] == "fail" and "topology.closed_delivery_gap" in result["forbidden_residuals"]


@pytest.mark.parametrize("contract", [{}, {"primary_rule": None}, {"primary_rule": ""},
                                       {"primary_rule": []}, {"primary_rule": "   "}])
def test_common_oracle_missing_rule_is_contained_without_guessing(contract):
    from types import SimpleNamespace
    from r2f.campaign.runtime_adapter import _evaluate_oracle
    from r2f.modules.s1.module import resolve_services
    scenario = SimpleNamespace(target_id=TOPOLOGY_TARGET,
                               target=SimpleNamespace(rule_contract=contract))
    result, failure = _evaluate_oracle(resolve_services(TOPOLOGY_TARGET, exploratory=True,
        runtime_capability="topology_v1"), scenario, {})
    assert result["verdict"] == "unknown"
    assert result["rule_id"] == "unresolved_primary_rule"
    assert result["reason_codes"] == ["oracle_tool_failure"]
    assert failure["reason"] == "target_primary_rule_missing_or_invalid"
    assert failure["exception_type"] == "ValueError"
