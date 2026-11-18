"""Versioned topology rule resolver over independently audited typed facts.

All conclusions are bounded to the observed writer/reader generation/window.
No helper result, predicted edge, finite timeout or cross-writer total is a
verdict. A diagnostic fixture verdict never grants candidate/seal eligibility.
"""
from r2f.modules.s1.topology_input import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, TOPOLOGY_FACT_PRODUCER,
    TOPOLOGY_FACT_IDS, TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY,
    TOPOLOGY_FACT_GUARDS, TOPOLOGY_RULE, TOPOLOGY_EFFECT, TOPOLOGY_EVALUATOR,
    topology_fact_specs, topology_rule_spec, topology_effect_spec,
)

FACTS_SCHEMA = "r2f.s1-topology-facts/v1"


def _aggregate(values):
    if False in values:
        return False
    if not values or any(v is None for v in values):
        return None
    return True


def resolve_fact_producer(producer_id):
    """Exact, executable resolver, NOT an ID-only declaration."""
    if producer_id != TOPOLOGY_FACT_PRODUCER:
        raise ValueError("unknown_topology_fact_producer:" + str(producer_id))
    from r2f.campaign.topology_adapter import TopologyFactProducer
    return TopologyFactProducer()


def resolve_fact(fact_id, facts):
    if fact_id not in TOPOLOGY_FACT_IDS:
        raise ValueError("unknown_topology_fact:" + str(fact_id))
    _typed(facts)
    if fact_id in {TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY, TOPOLOGY_FACT_GUARDS}:
        from types import SimpleNamespace
        result = evaluate_topology(SimpleNamespace(target_id=TOPOLOGY_TARGET, arm=facts.get("arm")), facts)
        value = result.get("fact_values", {}).get(fact_id)
    else:
        value = facts["fact_values"].get(fact_id)
    if value is not None and type(value) is not bool:
        raise ValueError("topology_fact_not_bool_or_unknown:" + fact_id)
    return value


def _typed(facts):
    if not isinstance(facts, dict) or facts.get("schema") != FACTS_SCHEMA \
            or facts.get("producer_id") != TOPOLOGY_FACT_PRODUCER \
            or facts.get("target_id") != TOPOLOGY_TARGET \
            or not isinstance(facts.get("fact_values"), dict):
        raise ValueError("topology_typed_facts_required")


def _result():
    return {"rule_id": TOPOLOGY_RULE, "rule_role": "primary", "evaluator_id": TOPOLOGY_EVALUATOR,
            "verdict": "unknown", "reason_codes": [], "forbidden_residuals": [],
            "closure": {"kind": "per_writer_generation_delivered_later_sentinel", "pairs": []},
            "applicability": {"target_id": TOPOLOGY_TARGET, "runtime_capability": TOPOLOGY_CAPABILITY},
            "responsibility_layer": "observation", "candidate_eligible": False,
            "seal_eligible": False, "diagnostic_only": True, "fact_values": {}}


def evaluate_topology(scenario, facts):
    result = _result()
    if scenario.target_id != TOPOLOGY_TARGET:
        result.update(verdict="not_applicable", reason_codes=["topology_exact_target_required"])
        return result
    try:
        _typed(facts)
    except ValueError as exc:
        result["reason_codes"] = [str(exc)]
        return result
    if facts.get("tool_error"):
        result.update(reason_codes=["topology_tool_failure"], responsibility_layer="harness")
        return result
    if facts.get("evidence_error") or facts.get("diagnostic_valid") is not True:
        result["reason_codes"] = ["topology_evidence_incomplete:" + str(facts.get("evidence_error", "raw_audit_not_complete"))]
        return result
    if facts.get("guard_rejected_by") == "harness":
        result.update(verdict="not_applicable", reason_codes=["harness_guard_rejection_not_sut_rejection"],
                      responsibility_layer="harness")
        return result
    if facts.get("endpoint_identity_complete") is not True:
        result["reason_codes"] = ["topology_endpoint_identity_incomplete"]
        return result
    graph_values, delivery_values, reasons, residuals = [], [], [], []
    for pair in facts.get("pairs", []):
        detail = {"writer": pair["writer"], "reader": pair["reader"],
                  "writer_generation": pair.get("writer_generation"), "reader_generation": pair.get("reader_generation"),
                  "missing": pair.get("proofs", {}).get("delivery_closure", {}).get("detail", {}).get("missing", [])}
        result["closure"]["pairs"].append(detail)
        compatibility, match, closure = (pair.get(k) for k in ("compatibility", "observed_match", "delivery_closure"))
        if compatibility == "not_applicable":
            continue
        # A real, attributed delivery is independent evidence for THIS pair,
        # even if a match snapshot cannot identify all N directed edges.
        incompatible_effect = compatibility == "incompatible" and (
            match in {"bidirectional", "writer_only", "reader_only"}
            or bool(pair.get("communication_witnesses")))
        if incompatible_effect:
            graph_values.append(False)
            residuals.append("topology.pair_graph_mismatch")
        elif compatibility in {"compatible", "incompatible"} and (
            compatibility == "compatible" and match == "bidirectional"
            or compatibility == "incompatible" and match == "none"):
            graph_values.append(True)
        else:
            graph_values.append(None)
            reasons.append("directed_peer_or_observed_contract_unknown")
        applicability = pair.get("proofs", {}).get("delivery_closure", {}).get("detail", {}).get("applicability_kind")
        if applicability == "not_applicable_no_sends":
            if (closure == "no_obligation" and pair.get("delivery_window_identity_complete") is True
                    and pair.get("delivery_applicability") == "not_applicable_no_sends"):
                continue
            delivery_values.append(None)
            reasons.append("no_sends_applicability_not_independently_verified")
            continue
        if closure == "not_applicable" or closure == "no_obligation":
            continue
        expected = pair.get("proofs", {}).get("delivery_closure", {}).get("detail", {}).get("expected", [])
        received = {d["sequence"] for d in pair.get("communication_witnesses", [])}
        if closure == "closed" and expected and set(expected) <= received:
            delivery_values.append(True)
        elif closure == "closed_with_gap" and pair.get("reliable_history_contract") is True \
                and pair.get("filter_free") is True and pair.get("delivery_window_identity_complete") is True:
            delivery_values.append(False)
            residuals.append("topology.closed_delivery_gap")
        else:
            delivery_values.append(None)
            reasons.append("finite_tail_or_delivery_contract_unknown" if closure in {"open", "closed_with_gap"}
                           else "per_writer_delivery_identity_unknown")
    values = dict(facts["fact_values"])
    values[TOPOLOGY_FACT_GRAPH] = _aggregate(graph_values)
    values[TOPOLOGY_FACT_DELIVERY] = _aggregate(delivery_values)
    values[TOPOLOGY_FACT_GUARDS] = True if facts.get("action_receipts") else None
    result["fact_values"] = values
    excluded = facts.get("fixture") is True or facts.get("control_excluded") is True or scenario.arm == "negative"
    result["diagnostic_only"] = excluded or facts.get("source_provenance_qualified") is not True
    # Forbidden residuals outrank allowed effects AND unrelated unknown pairs.
    # A fail is a diagnostic rule violation, never by itself a natural candidate.
    if residuals:
        result.update(verdict="fail", reason_codes=sorted(set(residuals)),
                      forbidden_residuals=sorted(set(residuals)),
                      responsibility_layer="harness" if facts.get("control_excluded") else "observation")
    elif graph_values and values[TOPOLOGY_FACT_GRAPH] is True \
            and (not delivery_values or values[TOPOLOGY_FACT_DELIVERY] is True) \
            and not facts.get("unknown_reasons_sample_identity") \
            and not facts.get("unresolved_historical_obligations"):
        result.update(verdict="pass", reason_codes=["bounded_observed_topology_consistency"],
                      observed_effects=["observed_consistency"], effect_id=TOPOLOGY_EFFECT)
    elif not graph_values and not delivery_values and not facts.get("unresolved_historical_obligations"):
        result.update(verdict="not_applicable", reason_codes=["no_applicable_pair_obligation"])
    else:
        result["reason_codes"] = sorted(set(reasons or ["topology_evidence_incomplete"]))
    return result


def resolve_evaluator(evaluator_id):
    if evaluator_id != TOPOLOGY_EVALUATOR:
        raise ValueError("unknown_topology_evaluator:" + str(evaluator_id))
    return evaluate_topology


class TopologyOracle:
    id = TOPOLOGY_EVALUATOR

    def evaluate(self, scenario, facts):
        return resolve_evaluator(TOPOLOGY_EVALUATOR)(scenario, facts)
