"""Exact deadline env.delay auditor contracts; synthetic data, NO ROS/DDS.

Mocked formal branches exercise consumer gating, never real qualification.
Existing offline helpers are reused without changing their files or contracts.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend
from r2f.runtime.topology_route import audit_topology_application
from r2f.runtime.topology_operator_application import (
    audit_operator_application, is_deadline_configuration_delay,
)
from test_topology_operator_application import _config, _rewrite_raw
from test_topology_operator_route_integration import (
    _synthetic_formal_gate_input, _mocked_formal_audit,
)

DEADLINE = "S1.topology.qos.deadline"
REQUEST = {"operator_id": "env.delay", "slot_id": DEADLINE,
           "context": {"endpoint_index": 1}}


def _run(tmp_path, *, requests=None):
    # Non-filter wildcard slots intentionally ignore endpoint_index context.
    # Seeds 11 and 12 both resolve index 1; bind via sealed receipts, not intent.
    plan, config = _config(requests or [REQUEST], seed=11, writers=2, readers=1)
    assert all(r["slot_id"] == DEADLINE + "#1" for r in config["input_layer_receipts"])
    assert config["input_layer_receipts"][0]["input_before"]["endpoints"][1]["logical_id"] == "w1"
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare",
               side_effect=AssertionError("ROS FORBIDDEN")):
        runtime = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    return plan, config, runtime


def _events(runtime):
    return [json.loads(line) for line in Path(runtime["raw_path"]).read_text().splitlines()]


def _synthetic_api_chain(plan, runtime):
    """Build public-schema fake raw from offline fixture, never a live report."""
    _synthetic_formal_gate_input(plan, runtime)
    def transform(events):
        output = []
        for index, event in enumerate(events):
            event["monotonic_ns"] = (index + 1) * 10000
            if event["event"] == "endpoint_created":
                event["identity_source"] = ("get_publishers_info_by_topic"
                    if event["role"].startswith("writer") else "get_subscriptions_info_by_topic")
                event["requested_qos"] = deepcopy(event["qos"])
                event["create_begin_monotonic_ns"] = event["monotonic_ns"] - 200
                event["created_monotonic_ns"] = event["monotonic_ns"] - 100
                begin = deepcopy(event)
                begin.update(event="endpoint_create_begin",
                             monotonic_ns=event["create_begin_monotonic_ns"])
                output.append(begin)
            output.append(event)
        return output
    _rewrite_raw(runtime, transform)


def _formal(tmp_path, *, requests=None):
    plan, config, runtime = _run(tmp_path, requests=requests)
    _synthetic_api_chain(plan, runtime)
    return plan, config, runtime


def _independent(plan, config, runtime):
    return audit_operator_application(config["input_layer_receipts"], _events(runtime),
                                      plan.final_config["topology_program"], fixture=False)[0]


def _token(events):
    return next(e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == "w1")


def _assert_no_hit(audit):
    assert audit["status"] in {"incomplete", "invalid"}, audit
    assert not audit["mutation_receipt"]["configured"]
    assert not audit["mutation_receipt"]["applied"]
    assert not audit["mutation_receipt"]["hit"]
    assert not audit["mutation_receipt"]["effect_observed"]
    assert all(not layer["hit"] for layer in audit["layers"])
    assert not audit["candidate_eligible"]


def test_exact_endpoint_fixture_routes_to_configuration_not_netem(tmp_path):
    plan, config, runtime = _run(tmp_path)
    audit = runtime["application_audit"]
    layer = audit["layers"][0]
    assert config["input_layer_receipts"][0]["slot_id"] == DEADLINE + "#1"
    assert audit["status"] == layer["status"] == "fixture_application_observed", audit
    assert layer["qos_delta"][0]["logical_id"] == "w1"
    assert layer["qos_delta"][0]["field"] == "deadline_sec"
    assert layer["timing_delta"][0]["observed"] == 1000000
    assert layer["configuration_intervention_proven"]
    assert "netem_application_ref" not in layer
    assert not audit["execution_evidence"]
    assert not any(audit["mutation_receipt"].values())
    assert not layer["hit"] and not layer["effect_observed"]


def test_mocked_formal_hit_is_configuration_intervention_not_semantic_effect(tmp_path):
    plan, config, runtime = _formal(tmp_path)
    # Independent parser cannot grant execution/hit, even with API-shaped raw.
    record = _independent(plan, config, runtime)
    assert record["configuration_intervention_proven"], record
    assert not record["hit"] and not record["execution_evidence"] and not record["effect_observed"]
    audit = _mocked_formal_audit(plan, config, runtime)
    assert audit["status"] == "application_observed", audit
    assert audit["mutation_receipt"] == {
        "planned": True, "submitted": True, "carrier_admitted": True,
        "configured": True, "applied": True, "hit": True, "effect_observed": False}
    layer = audit["layers"][0]
    assert layer["effect_observation"] == "unknown_deadline_semantic_effect_not_measured"
    assert not audit["candidate_eligible"]
    refs = {r["event_index"] for r in layer["raw_refs"]}
    assert {e["event_index"] for e in _events(runtime)
            if e["event"] == "endpoint_create_begin" and e["logical_id"] == "w1"} <= refs


@pytest.mark.parametrize("corruption", [
    "missing_qos", "missing_deadline", "wrong_deadline", "string_deadline",
    "requested_echo_only", "wrong_source", "wrong_identity_source", "missing_api_begin",
    "duplicate_api_begin", "missing_created_time", "missing_observed_time", "bool_time",
    "stale_api_time", "future_created_time", "reversed_action_time", "wrong_api_begin_time",
    "wrong_gid", "wrong_pid", "wrong_worker", "wrong_generation", "wrong_run", "wrong_domain",
    "wrong_observed_domain", "wrong_rmw", "wrong_api_worker_identity",
])
def test_missing_or_wrong_readback_identity_source_and_chronology_fail_closed(tmp_path, corruption):
    plan, config, runtime = _formal(tmp_path)
    def corrupt(events):
        token = _token(events)
        api = next(e for e in events if e["event"] == "endpoint_create_begin" and e["logical_id"] == "w1")
        if corruption == "missing_qos": token.pop("qos")
        elif corruption == "missing_deadline": token["qos"].pop("deadline_ns")
        elif corruption == "wrong_deadline": token["qos"]["deadline_ns"] += 1
        elif corruption == "string_deadline": token["qos"]["deadline_ns"] = "1000000"
        elif corruption == "requested_echo_only": token["qos"] = None
        elif corruption == "wrong_source": token["configuration_source"] = "requested_config"
        elif corruption == "wrong_identity_source": token["identity_source"] = "configuration_echo"
        elif corruption == "missing_api_begin": events.remove(api)
        elif corruption == "duplicate_api_begin": events.insert(events.index(api), deepcopy(api))
        elif corruption == "missing_created_time": token.pop("created_monotonic_ns")
        elif corruption == "missing_observed_time": token.pop("monotonic_ns")
        elif corruption == "bool_time": token["created_monotonic_ns"] = True
        elif corruption == "stale_api_time": api["monotonic_ns"] = token["create_begin_monotonic_ns"] = 1
        elif corruption == "future_created_time": token["created_monotonic_ns"] = token["monotonic_ns"] + 1
        elif corruption == "reversed_action_time":
            end = next(e for e in events if e["event"] == "topology_action_done" and e["action_id"] == "create_w1")
            end["monotonic_ns"] = token["monotonic_ns"]
        elif corruption == "wrong_api_begin_time": token["create_begin_monotonic_ns"] += 1
        elif corruption == "wrong_gid": token["endpoint_gid"] = "other-gid"
        elif corruption == "wrong_pid": token["pid"] += 1
        elif corruption == "wrong_worker": token["worker_role"] = "writer4"
        elif corruption == "wrong_generation": token["generation"] += 1
        elif corruption == "wrong_run": token["run_id"] = "other-run"
        elif corruption == "wrong_domain": token["domain_id"] += 1
        elif corruption == "wrong_observed_domain": token["observed_domain_id"] += 1
        elif corruption == "wrong_rmw": token["rmw_identifier"] = "other-rmw"
        elif corruption == "wrong_api_worker_identity": api["process_start_ticks"] += 1
        return events
    _rewrite_raw(runtime, corrupt)
    record = _independent(plan, config, runtime)
    assert record["status"] == "unknown" and record["reasons"], record
    assert not record["configuration_intervention_proven"]
    _assert_no_hit(_mocked_formal_audit(plan, config, runtime))


@pytest.mark.parametrize("slot", ["S1.topology.qos.lease#1", "S1.network.delay", "S1.delivery.delay", "", None])
def test_other_slots_do_not_acquire_deadline_or_network_hit(slot):
    receipt = {"operator_id": "env.delay", "slot_id": slot}
    assert not is_deadline_configuration_delay(receipt)
    record = audit_operator_application([receipt], [], {}, fixture=False)[0]
    assert record["status"] == "unsupported" and not record["hit"]


@pytest.mark.parametrize("corruption", ["wrong_index", "missing_index", "extra_field", "no_op", "alias", "overridden"])
def test_exact_slot_noop_public_alias_and_later_override_fail_closed(tmp_path, corruption):
    plan, config, runtime = _formal(tmp_path)
    receipt = deepcopy(config["input_layer_receipts"][0])
    program = deepcopy(plan.final_config["topology_program"])
    if corruption == "wrong_index": receipt["slot_id"] = DEADLINE + "#0"
    elif corruption == "missing_index": receipt["slot_id"] = DEADLINE
    elif corruption == "extra_field": receipt["input_after"]["endpoints"][0]["qos"]["depth"] += 1
    elif corruption == "no_op": receipt["input_before"] = deepcopy(receipt["input_after"])
    elif corruption == "alias":
        # Public mapper truncates seconds to ns: both are 1000000ns.
        receipt["input_before"]["endpoints"][1]["qos"]["deadline_sec"] = 0.0010000000001
    elif corruption == "overridden": program["endpoints"][1]["qos"]["deadline_sec"] = "infinite"
    record = audit_operator_application([receipt], _events(runtime), program, fixture=False)[0]
    assert record["status"] == "unknown" and record["reasons"], record
    assert not record["configuration_intervention_proven"] and not record["hit"]


def test_real_two_delay_layers_cannot_claim_overwritten_first_layer(tmp_path):
    plan, config, runtime = _formal(tmp_path, requests=[REQUEST, REQUEST])
    audit = _mocked_formal_audit(plan, config, runtime)
    assert "configuration_effect_overridden_by_later_layer:w1:deadline_sec" in audit["layers"][0]["reasons"]
    assert not audit["layers"][0]["hit"]
    assert audit["layers"][1]["hit"]
    assert not audit["mutation_receipt"]["hit"]


def test_netem_audit_counters_never_substitute_for_deadline_readback(tmp_path):
    plan, config, runtime = _formal(tmp_path)
    def corrupt(events):
        _token(events)["qos"].pop("deadline_ns")
        return events
    _rewrite_raw(runtime, corrupt)
    with patch("r2f.runtime.topology_route.audit_topology_netem", return_value={
            "status": "application_observed", "hit": True, "packet_count": 500,
            "mutation_receipt": {"configured": True, "applied": True, "hit": True}}):
        _assert_no_hit(_mocked_formal_audit(plan, config, runtime))


@pytest.mark.parametrize("missing", ["topology_source_checked", "topology_cleanup", "isolation_locks_released", "environment_preflight"])
def test_formal_qualification_not_replaced_by_successful_configuration(missing, tmp_path):
    plan, config, runtime = _formal(tmp_path)
    _rewrite_raw(runtime, lambda events: [e for e in events if e["event"] != missing])
    audit = _mocked_formal_audit(plan, config, runtime)
    assert not audit["execution_evidence"]
    assert not any(audit["mutation_receipt"].values())
    assert not audit["layers"][0]["hit"]


def test_raw_hash_mismatch_blocks_deadline_auditor(tmp_path):
    plan, config, runtime = _formal(tmp_path)
    runtime["raw_sha256"] = "0" * 64
    with patch("r2f.runtime.topology_route.audit_operator_application", side_effect=AssertionError("hash first")):
        audit = audit_topology_application(plan, config["input_layer_receipts"], runtime)
    assert audit["status"] == "invalid" and "raw_hash_mismatch" in audit["reasons"][0]
