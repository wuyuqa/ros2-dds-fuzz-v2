"""Canonical fixture route: retired complete history is not final-generation data."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.runtime.topology_route import run_topology_route, TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
from test_topology_lifecycle_window_retirement import RetirementBackend, scenario
from test_topology_no_obligation_facts import ObservedGraph, consume, rewrite


class HistoryBackend(ObservedGraph, RetirementBackend):
    pass


def canonical_config(plan):
    """Use the supplied exact plan, never call action.insert to build metadata."""
    identity = {"input_identity_schema": INPUT_SCHEMA_V2, "target_id": TOPOLOGY_TARGET,
        "target_revision": 0, "mutation_kind": "endpoint_program", "carrier": "ros2_endpoint",
        "mutation_parameters": {}, "rmw": "rmw_cyclonedds_cpp", "repeat": 0, "seed": 5,
        "arm": "mutated", "mutation_layer_order": ["endpoint_program"],
        "input_layer_receipts": topology_input_layer_receipts(plan), "final_input": plan.final_config,
        "source_manifest": {}, "mutation_plan": plan.to_dict(),
        "operations": [op.to_dict() for op in plan.operations], "runtime_windows": [],
        "contract_hash": plan.contract_hash}
    return {**deepcopy(identity), "effective_input": deepcopy(identity),
        "effective_input_sha256": effective_input_hash(identity, schema=INPUT_SCHEMA_V2),
        "topology_program": deepcopy(plan.final_config["topology_program"]),
        "plan_hash": plan.plan_hash(), "runtime_capability": TOPOLOGY_CAPABILITY,
        "backend": "offline_fake", "run_id": "recreated-history-offline", "domain_id": 192}


def produce_history(tmp_path, *, fault=None):
    parent = scenario()
    parent["actions"].pop()  # authored predecessor, followed by an actual generation.action plan
    plan = make_topology_plan(parent, [{"operator_id": "generation.action", "context": {
        "parameters": {"endpoint_id": "w0", "op": "recreate"}}}], rng_seed=5)
    cfg = canonical_config(plan)
    assert len(cfg["topology_program"]["actions"]) == len(parent["actions"]) + 1
    assert cfg["topology_program"]["actions"][-1]["op"] == "recreate"
    runtime = run_topology_route(cfg, tmp_path / "runtime", backend=HistoryBackend(fault=fault))
    assert not runtime["real_execution"] and not runtime["candidate_eligible"]
    return runtime, cfg


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("offline history test must never prepare ROS")
    monkeypatch.setattr("r2f.runtime.topology_worker.RosTopologyBackend.prepare", blocked)


def test_completed_retired_history_is_accounted_without_inventing_g2_delivery(tmp_path):
    runtime, cfg = produce_history(tmp_path)
    facts, oracle = consume(runtime, cfg)
    assert facts["diagnostic_valid"]
    assert runtime["runtime_status"] == "completed"
    assert facts["unresolved_historical_obligations"] == []
    send = next(e for e in runtime["events"] if e["event"] == "send" and e.get("logical_id") == "w0")
    coverage = next(c for c in facts["historical_send_coverage"] if c["send_ref"]["event_index"] == send["event_index"])
    assert coverage["writer_generation"] == 1
    assert {r["reader"] for r in coverage["reader_windows"]} == {"r0", "r1"}
    assert all(r["writer_generation"] == 1 for r in coverage["reader_windows"])
    assert all(p["writer_generation"] == 2 and p["delivery_closure"] == "no_obligation"
               and p["delivery_applicability"] == "not_applicable_no_sends"
               for p in facts["pairs"] if p["writer"] == "w0")
    assert not any(e["event"] == "send" and e.get("logical_id") == "w0" and e.get("generation") == 2
                   for e in runtime["events"])
    assert oracle["verdict"] == "pass" and not facts["candidate_eligible"]


@pytest.mark.parametrize("fault", ["missing_sentinel", "late_receive"])
def test_unclosed_history_cannot_be_discharged_by_recreated_none(tmp_path, fault):
    runtime, cfg = produce_history(tmp_path, fault=fault)
    facts, oracle = consume(runtime, cfg)
    assert facts["diagnostic_valid"]
    assert facts["unresolved_historical_obligations"]
    assert not facts.get("historical_send_coverage")
    assert not any(e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0"
                   for e in runtime["events"])
    assert oracle["verdict"] == "unknown" and not facts["candidate_eligible"]


@pytest.mark.parametrize("fault", ["receipt_missing", "receipt_history", "old_ack", "retire_missing", "close_incomplete"])
def test_history_accounting_revalidates_raw_not_receipt_labels(tmp_path, fault):
    runtime, cfg = produce_history(tmp_path)
    assert consume(runtime, cfg)[0]["unresolved_historical_obligations"] == []
    def mutate(events):
        claim = next(e for e in events if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0")
        close = next(e for e in events if e["event"] == "topology_delivery_window_closed" and e.get("writer") == "w0")
        if fault == "receipt_missing":
            for e in events:
                if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0":
                    e["event"] = "unproven_label"
        elif fault == "receipt_history":
            for e in events:
                if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0":
                    e["prior_window_close_refs"] = []
        elif fault == "old_ack":
            opened = events[close["open_ref"]]
            events[opened["snapshot_refs"]["w0"][-1]]["event"] = "uncaptured_ack"
        elif fault == "retire_missing":
            next(e for e in events if e["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"})["cleanup_status"] = "unknown"
        elif fault == "close_incomplete":
            close["window_complete"] = False
        # Reseal the altered prefix: the independent semantic checks must reject it.
        seal = next(e for e in events if e["event"] == "topology_send_inventory_end")
        digest = hashlib.sha256(json.dumps(events[:seal["event_index"]], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        seal["inventory_sha256"] = digest
        for e in events:
            if e["event"] == "topology_delivery_no_sends": e["inventory_sha256"] = digest
    rewrite(runtime, mutate)
    facts, oracle = consume(runtime, cfg)
    assert facts.get("unresolved_historical_obligations") or not facts["diagnostic_valid"]
    assert not facts.get("historical_send_coverage")
    assert oracle["verdict"] == "unknown" and not facts["candidate_eligible"]
