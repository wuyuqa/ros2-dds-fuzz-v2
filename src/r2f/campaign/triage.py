"""Thin S1 fact-to-core adapters; core owns attribution and unknown policy."""
from r2f.core.oracle import attribute_responsibility, unknown_gate as core_unknown_gate
from r2f.modules.s1.targets import EFFECT


def attribute(cell, facts, oracle):
    record = dict(cell)
    record.update(
        environment_healthy=facts.get("environment_healthy") is True and facts.get("process_healthy") is True,
        preconditions_reached=facts.get("preconditions_reached"),
        identity_complete=facts.get("endpoint_identity_complete"),
        endpoint_identity={key: facts[key] for key in ("writer", "reader") if key in facts},
        window_complete=facts.get("window", {}).get("complete"),
        expected_effect_evaluable=isinstance(cell.get("expected_effect_set", EFFECT), dict),
        expected_effect_set=cell.get("expected_effect_set", EFFECT),
        observed_effect={"effects": facts.get("observed_effects", []), "forbidden_residuals": oracle.get("forbidden_residuals", [])},
        oracle_verdict=oracle["verdict"], primary_rule_violated=oracle["verdict"] == "fail",
    )
    return attribute_responsibility(record).to_dict()


def unknown_gate(rows, campaign_kind="required"):
    rows = list(rows)
    unknowns = [r.get("cell_id") for r in rows if r.get("oracle_verdict") == "unknown"]
    try:
        core_unknown_gate(rows)
        return {"passed": True, "unknown_count": len(unknowns), "unknown_cells": unknowns}
    except ValueError as exc:
        return {"passed": False, "unknown_count": len(unknowns), "unknown_cells": unknowns, "reason": str(exc)}
