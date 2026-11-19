"""Offline route/operator integration; never runs ROS, DDS, netns or tc.

All raw comes from OfflineTopologyBackend. Deliberately corrupted copies and
synthetic public-schema parser inputs are labelled fixtures, not SUT evidence.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.runtime.topology_operator_application import audit_operator_application
from r2f.runtime.topology_route import audit_topology_application
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend
from test_topology_operator_application import (
    _audit, _config, _run, _rewrite_raw, _public_schema_fixture_events,
)


REQUESTS = [
    {"operator_id": "relation.pair", "context": {"pair": "BB"}},
    {"operator_id": "timing.shift", "slot_id": "S1.topology.qos.deadline", "context": {"offset_sec": 0.002}},
    {"operator_id": "lease.value", "slot_id": "S1.topology.qos.lease", "context": {"special_class": "finite"}},
]


def _assert_no_execution_or_hit(audit):
    assert audit["execution_evidence"] is False
    assert audit["candidate_eligible"] is False
    assert not any(audit["mutation_receipt"].values())
    for layer in audit["layers"]:
        assert not layer["hit"] and not layer["execution_evidence"] and not layer["candidate_eligible"]
        if "operator_application" in layer:
            assert not layer["operator_application"]["hit"]
            assert not layer["operator_application"]["effect_observed"]


@pytest.mark.parametrize("op_request", REQUESTS, ids=lambda r: r["operator_id"])
def test_route_delegates_hashed_raw_and_merges_the_independent_record(tmp_path, op_request):
    with patch("r2f.runtime.topology_route.audit_operator_application", wraps=audit_operator_application) as auditor:
        plan, config, result = _run(tmp_path, [op_request])
    # topology_pair audits its prefix before the final route audits the sealed raw.
    assert auditor.call_count == 2
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    for call in auditor.call_args_list:
        receipts, raw_prefix, submitted = call.args
        assert receipts == config["input_layer_receipts"]
        assert submitted == plan.final_config["topology_program"]
        assert raw_prefix == events[:len(raw_prefix)]
        assert call.kwargs == {"fixture": True}
    audit = result["application_audit"]
    layer = audit["layers"][0]
    assert audit["status"] == layer["status"] == "fixture_application_observed", audit
    assert layer["operator_application"]["schema"] == "r2f.topology-operator-application/v1"
    assert layer["configuration_application_observed"] is True
    assert layer["application_kind"] == "configuration_only"
    assert layer["qos_delta"] == _audit(plan, config, result)[0]["qos_delta"]
    refs = layer["operator_application"]["raw_refs"]
    assert refs == layer["raw_refs"] and refs
    assert all(ref["path"] == str(Path(result["raw_path"]).resolve())
               and ref["sha256"] == result["raw_sha256"] for ref in refs)
    created = {e["event_index"] for e in events if e["event"] == "endpoint_created"}
    assert created <= {ref["event_index"] for ref in refs}
    assert audit["netem_application"]["schema"] == "r2f.topology-netem-application/v1"
    assert audit["netem_application"]["status"] == "not_applicable_offline_fixture"
    _assert_no_execution_or_hit(audit)


IDENTITY_CORRUPTIONS = [
    ("endpoint_gid", None, "raw_endpoint_identity_not_bound"),
    ("pid", 999999, "raw_endpoint_identity_not_bound"),
    ("process_start_ticks", None, "raw_endpoint_identity_not_bound"),
    ("endpoint_generation", "stale-generation", "raw_endpoint_identity_not_bound"),
    ("generation", 2, "raw_endpoint_generation_missing_or_ambiguous"),
    ("role", "unrelated-role", "raw_endpoint_configuration_mismatch"),
    ("run_id", "other-run", "raw_endpoint_run_or_domain_mismatch"),
    ("domain_id", 192, "raw_endpoint_run_or_domain_mismatch"),
    ("backend", "ros2_public_api", "raw_endpoint_provenance_mismatch"),
    ("configuration_source", "requested_config", "raw_endpoint_provenance_mismatch"),
]


@pytest.mark.parametrize("op_request", REQUESTS, ids=lambda r: r["operator_id"])
@pytest.mark.parametrize("field,value,reason", IDENTITY_CORRUPTIONS, ids=[row[0] for row in IDENTITY_CORRUPTIONS])
def test_raw_identity_and_generation_refusals_reach_unified_audit(tmp_path, op_request, field, value, reason):
    plan, config, result = _run(tmp_path, [op_request])
    endpoint_id = result["application_audit"]["layers"][0]["qos_delta"][0]["logical_id"]
    def corrupt(events):
        token = next(e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == endpoint_id)
        token[field] = value
        return events
    _rewrite_raw(result, corrupt)
    standalone = _audit(plan, config, result)[0]
    assert standalone["status"] == "unknown", standalone
    assert any(reason in r for r in standalone["reasons"]), standalone
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "incomplete", audit
    layer = audit["layers"][0]
    assert layer["status"] == "unknown"
    assert any(reason in r for r in layer["reasons"]), layer
    _assert_no_execution_or_hit(audit)


@pytest.mark.parametrize("op_request", REQUESTS, ids=lambda r: r["operator_id"])
@pytest.mark.parametrize("corruption", ["missing_field", "wrong_field_value", "missing_create", "duplicate_generation"])
def test_unreflected_configuration_and_ambiguous_raw_fail_closed(tmp_path, op_request, corruption):
    plan, config, result = _run(tmp_path, [op_request])
    delta = result["application_audit"]["layers"][0]["qos_delta"][0]
    def corrupt(events):
        token = next(e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == delta["logical_id"])
        if corruption == "missing_field":
            token["qos"].pop(delta["field"])
        elif corruption == "wrong_field_value":
            token["qos"][delta["field"]] = delta["before"]
        elif corruption == "missing_create":
            events.remove(token)
        else:
            events.insert(events.index(token) + 1, deepcopy(token))
        return events
    _rewrite_raw(result, corrupt)
    independent = _audit(plan, config, result)[0]
    assert independent["status"] == "unknown" and independent["reasons"]
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "incomplete", audit
    assert audit["layers"][0]["status"] == "unknown"
    assert audit["layers"][0]["reasons"]
    _assert_no_execution_or_hit(audit)


def test_unchanged_reader_in_relation_pair_is_still_independently_required(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair", "context": {"pair": "BR"}}])
    layer = result["application_audit"]["layers"][0]
    assert layer["dds_contract"]["status"] == "incompatible"
    assert layer["effect_observation"] == "unknown_expected_no_match"
    reader = layer["relation_pair"]["reader"]
    def corrupt(events):
        token = next(e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == reader)
        token["qos"].pop("reliability")
        return events
    _rewrite_raw(result, corrupt)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "incomplete"
    assert any(reader + ":reflected_qos_field_missing" in r for r in audit["layers"][0]["reasons"])
    _assert_no_execution_or_hit(audit)


def _run_same_endpoint_chain(tmp_path, requests):
    # Wildcard slot resolution is seed-based today; assert the actual sealed
    # addresses rather than pretending endpoint_index overrides that resolver.
    plan, config = _config(requests, seed=1)
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare", side_effect=AssertionError("NO ROS")):
        result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    return plan, config, result


def test_timing_layer_overwrite_is_not_backfilled_from_the_final_program(tmp_path):
    first = deepcopy(REQUESTS[1])
    second = deepcopy(first)
    second["context"]["offset_sec"] = 0.003
    plan, config, result = _run_same_endpoint_chain(tmp_path, [first, second])
    independent = _audit(plan, config, result)
    assert independent[0]["status"] == "unknown", independent
    assert any("layer_value_not_reflected" in r for r in independent[0]["reasons"])
    assert independent[1]["status"] == "fixture_application_observed"
    audit = result["application_audit"]
    assert audit["status"] == "incomplete"
    assert [layer["status"] for layer in audit["layers"]] == ["unknown", "fixture_application_observed"]
    _assert_no_execution_or_hit(audit)


def test_distinct_timing_and_lease_fields_can_compose_on_the_same_endpoint(tmp_path):
    requests = deepcopy(REQUESTS[1:])
    plan, config, result = _run_same_endpoint_chain(tmp_path, requests)
    audit = result["application_audit"]
    assert audit["status"] == "fixture_application_observed", audit
    assert len(audit["layers"]) == 2
    assert len({layer["qos_delta"][0]["logical_id"] for layer in audit["layers"]}) == 1
    assert {layer["timing_delta"][0]["field"] for layer in audit["layers"]} == {"deadline_sec", "lease_duration_sec"}
    _assert_no_execution_or_hit(audit)


def test_reflection_checked_on_every_recreated_generation(tmp_path):
    op_request = deepcopy(REQUESTS[2])
    plan, config, result = _run_same_endpoint_chain(tmp_path, [op_request, {"operator_id": "generation.action"}])
    assert result["application_audit"]["status"] == "fixture_application_observed", result["application_audit"]
    endpoint_id = result["application_audit"]["layers"][0]["qos_delta"][0]["logical_id"]
    def corrupt(events):
        tokens = [e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == endpoint_id]
        assert len(tokens) == 2, tokens
        tokens[-1]["qos"]["lease_duration_sec"] = "infinite"
        return events
    _rewrite_raw(result, corrupt)
    assert _audit(plan, config, result)[0]["status"] == "unknown"
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "incomplete"
    assert any("layer_value_not_reflected" in r for r in audit["layers"][0]["reasons"])
    _assert_no_execution_or_hit(audit)


@pytest.mark.parametrize("operator", ["env.lease_expiry", "env.lease_refresh", "guard.dependency", "env.delay"])
def test_unsupported_layer_stays_unsupported_beside_a_supported_configuration(tmp_path, operator):
    op_request = {"operator_id": operator}
    if "lease" in operator:
        op_request["slot_id"] = "S1.topology.qos.lease"
    plan, config, result = _run(tmp_path, [REQUESTS[1], op_request])
    audit = result["application_audit"]
    layer = audit["layers"][1]
    if operator == "env.delay":
        # This sealed wildcard resolved to an endpoint deadline, not netem.
        receipt = config["input_layer_receipts"][1]
        slot, separator, index_text = receipt["slot_id"].partition("#")
        assert slot == "S1.topology.qos.deadline" and separator == "#"
        assert index_text.isdecimal() and str(int(index_text)) == index_text
        endpoint_index = int(index_text)
        endpoint = receipt["input_after"]["endpoints"][endpoint_index]
        assert audit["status"] == layer["status"] == "fixture_application_observed", audit
        assert layer["application_kind"] == "configuration_only"
        assert layer["configuration_application_observed"] is True
        assert layer["qos_delta"] == [{
            "index": endpoint_index, "logical_id": endpoint["logical_id"],
            "role": endpoint["role"], "field": "deadline_sec",
            "before": receipt["input_before"]["endpoints"][endpoint_index]["qos"]["deadline_sec"],
            "value": endpoint["qos"]["deadline_sec"],
        }]
        assert not layer["reasons"]
        assert layer["effect_observation"] == "unknown_deadline_semantic_effect_not_measured"
        assert layer["effect_observed"] is False
        assert layer["operator_application"]["effect_observed"] is False
        assert "netem_application_ref" not in layer
        assert audit["netem_application"]["status"] == "not_applicable_offline_fixture"
    else:
        assert audit["status"] == "incomplete", audit
        assert layer["status"] == "unsupported"
        assert "operator_application_auditor_not_implemented:" + operator in layer["reasons"]
    _assert_no_execution_or_hit(audit)


def test_returned_facts_and_claimed_execution_do_not_qualify_fixture(tmp_path):
    plan, config, result = _run(tmp_path, [REQUESTS[1]])
    result.update(real_execution=True, fixture=False, runtime_status="executed", facts={"effect_observed": True})
    result["mutation_receipt"].update({key: True for key in result["mutation_receipt"]})
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "fixture_application_observed" and audit["fixture"]
    _assert_no_execution_or_hit(audit)


def test_raw_hash_mismatch_stops_before_operator_audit(tmp_path):
    plan, config, result = _run(tmp_path, [REQUESTS[2]])
    result["raw_sha256"] = "0" * 64
    with patch("r2f.runtime.topology_route.audit_operator_application", side_effect=AssertionError("HASH MUST BE CHECKED FIRST")):
        audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "invalid"
    assert "raw_hash_mismatch" in audit["reasons"][0]
    assert not audit["layers"]
    _assert_no_execution_or_hit(audit)


LEGACY_REQUESTS = [
    {"operator_id": "param.enum", "slot_id": "S1.topology.qos.reliability", "context": {"enum_value": "best_effort"}},
    {"operator_id": "queue.depth", "slot_id": "S1.topology.qos.depth"},
    {"operator_id": "param.boundary", "slot_id": "S1.topology.qos.depth", "context": {"boundary_class": "neighbor"}},
]


@pytest.mark.parametrize("op_request", LEGACY_REQUESTS, ids=lambda r: r["operator_id"])
def test_legacy_configuration_path_does_not_call_the_new_auditor(tmp_path, op_request):
    with patch("r2f.runtime.topology_route.audit_operator_application", side_effect=AssertionError("LEGACY PATH MUST STAY LEGACY")):
        plan, config, result = _run(tmp_path, [op_request])
    audit = result["application_audit"]
    assert audit["status"] == "fixture_application_observed", audit
    assert audit["layers"][0]["effect_scope"] == "endpoint_public_configuration_only_not_timed_lifecycle"
    assert "operator_application" not in audit["layers"][0]
    _assert_no_execution_or_hit(audit)


def _synthetic_formal_gate_input(plan, result):
    """Raw-shaped data for branch coverage ONLY, not formal qualification.

    The test stubs canonical presteps and worker measurements below. It cannot
    demonstrate that a fixture qualifies in the real runtime. No process,
    environment probe, lease, network setup or DDS worker is created here.
    """
    events = _public_schema_fixture_events(plan, result)
    events = [e for e in events if e["event"] not in {
        "isolation_locks_acquired", "environment_preflight", "isolation_locks_released", "topology_source_checked"}]
    for event in events:
        event["backend"] = "ros2_public_api"
        event["test_kind"] = "synthetic_mocked_gate_branch_not_execution_evidence"
        if event["event"] == "topology_provenance":
            event.update(fixture=False, runtime_kind="ros2_public_api")
        if event["event"] == "topology_backend_prepared":
            event["ros_execution"] = True
        if event["event"] == "endpoint_created":
            event["observed_domain_id"] = event["domain_id"]
    source = {"source_frozen": True, "head": "MOCK-NOT-A-SOURCE-IDENTITY"}
    phases = ["before_preflight", "after_preflight", "before_presteps"] + [
        "before_action:" + a["action_id"] for a in plan.final_config["topology_program"]["actions"]] + [
        "after_actions", "after_cleanup"]
    formal = [
        {"event": "isolation_locks_acquired", "locks": ["MOCK-REGISTRY", "MOCK-DOMAIN", "MOCK-STATE"]},
        {"event": "environment_preflight", "ok": True,
         "environment": {"test_kind": "synthetic_mocked_gate_branch_not_execution_evidence",
                         "process_scan": {"ok": True}, "old_lock_scan": {"ok": True}, "rtps_probe": {"ok": True}}},
    ] + events + [{"event": "topology_source_checked", "source": source, "phase": phase} for phase in phases] + [
        {"event": "isolation_locks_released"}]
    _rewrite_raw(result, lambda unused: formal)


MOCK_WORKERS = [{"unknowns": [], "fresh_process": True, "separate_context": True, "returncode": 0, "closed": True}]


@pytest.mark.parametrize("op_request", LEGACY_REQUESTS, ids=lambda r: r["operator_id"])
def test_legacy_hit_and_seven_field_receipt_unchanged_in_mocked_formal_branch(tmp_path, op_request):
    plan, config, result = _run(tmp_path, [op_request])
    _synthetic_formal_gate_input(plan, result)
    with patch("r2f.runtime.topology_route._presteps_proven", return_value=True), patch(
            "r2f.runtime.topology_pair._execution_workers", return_value=MOCK_WORKERS):
        audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "application_observed", audit
    assert audit["layers"][0]["hit"] is True
    assert audit["mutation_receipt"] == dict.fromkeys(
        ("planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed"), True)
    assert "operator_application" not in audit["layers"][0]
    # This is a mocked branch result, not execution evidence or eligibility.
    assert audit["candidate_eligible"] is False


@pytest.mark.parametrize("op_request", REQUESTS, ids=lambda r: r["operator_id"])
def test_new_configuration_only_branch_does_not_claim_match_timing_or_lease_effect(tmp_path, op_request):
    plan, config, result = _run(tmp_path, [op_request])
    _synthetic_formal_gate_input(plan, result)
    with patch("r2f.runtime.topology_route._presteps_proven", return_value=True), patch(
            "r2f.runtime.topology_pair._execution_workers", return_value=MOCK_WORKERS):
        audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "application_observed", audit
    layer = audit["layers"][0]
    assert layer["configuration_application_observed"] is True
    assert layer["hit"] and layer["operator_application"]["hit"]
    assert layer["configuration_intervention_proven"] is True
    assert not layer["operator_application"]["effect_observed"]
    assert audit["mutation_receipt"]["configured"] and audit["mutation_receipt"]["applied"]
    assert audit["mutation_receipt"]["hit"] and not audit["mutation_receipt"]["effect_observed"]
    assert layer["effect_observation"].startswith("unknown")
    assert audit["candidate_eligible"] is False


@pytest.mark.parametrize("pair", ["BR", "BB"])
def test_configuration_intervention_is_independent_of_predicted_match(tmp_path, pair):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair", "context": {"pair": pair}}])
    _synthetic_formal_gate_input(plan, result)
    audit = _mocked_formal_audit(plan, config, result)
    assert audit["layers"][0]["hit"], audit
    assert not audit["mutation_receipt"]["effect_observed"]
    assert audit["layers"][0]["dds_contract"]["prediction_only"]
    assert not audit["candidate_eligible"]


def _mocked_formal_audit(plan, config, result):
    # Branch coverage only: never a claim of measured execution qualification.
    with patch("r2f.runtime.topology_route._presteps_proven", return_value=True), patch(
            "r2f.runtime.topology_pair._execution_workers", return_value=MOCK_WORKERS):
        return audit_topology_application(plan, config["input_layer_receipts"], result)


@pytest.mark.parametrize("corruption", ["worker_alias", "role", "process", "generation", "cross_endpoint_identity"])
def test_mocked_formal_configuration_rejects_identity_forgery(tmp_path, corruption):
    plan, config, result = _run(tmp_path, [REQUESTS[0]])
    _synthetic_formal_gate_input(plan, result)
    def corrupt(events):
        tokens = [e for e in events if e["event"] == "endpoint_created"]
        token = tokens[0]
        if corruption == "worker_alias":
            token["worker_role"] = "reader4"
        elif corruption == "role":
            token["role"] = "reader" if token["role"] == "writer" else "writer"
        elif corruption == "process":
            token["process_start_ticks"] += 1
        elif corruption == "generation":
            token["generation"] += 1
        else:
            for key in ("pid", "process_start_ticks", "endpoint_generation", "endpoint_gid"):
                token[key] = tokens[1][key]
            action_end = next(e for e in events if e["event"] == "topology_action_done"
                              and e["action"]["endpoint_id"] == token["logical_id"])
            action_end["receipt"]["after"].update({k: token[k] for k in (
                "pid", "process_start_ticks", "endpoint_generation", "endpoint_gid")})
        return events
    _rewrite_raw(result, corrupt)
    audit = _mocked_formal_audit(plan, config, result)
    assert not audit["mutation_receipt"]["hit"], audit
    assert audit["layers"][0]["reasons"]
    assert not audit["candidate_eligible"]


def test_public_duration_alias_is_not_a_configuration_intervention(tmp_path):
    requests = [{"operator_id": "timing.shift", "slot_id": "S1.topology.qos.deadline",
                 "context": {"offset_sec": 0.0000000002}}] * 2
    plan, config, result = _run_same_endpoint_chain(tmp_path, requests)
    _synthetic_formal_gate_input(plan, result)
    audit = _mocked_formal_audit(plan, config, result)
    assert not audit["mutation_receipt"]["hit"], audit
    assert "public_configuration_alias" in " ".join(audit["layers"][1]["operator_application"]["reasons"])
    assert audit["layers"][1]["configuration_intervention_proven"] is False


def test_overwritten_value_cannot_hit_even_when_public_duration_mapping_aliases(tmp_path):
    # Both final values truncate to the same ns; an earlier declared input was
    # nevertheless replaced, so the later layer cannot authenticate it.
    requests = [{"operator_id": "timing.shift", "slot_id": "S1.topology.qos.deadline",
                 "context": {"offset_sec": v}} for v in (0.002, 0.0000000002)]
    plan, config, result = _run_same_endpoint_chain(tmp_path, requests)
    _synthetic_formal_gate_input(plan, result)
    audit = _mocked_formal_audit(plan, config, result)
    assert not audit["layers"][0]["hit"]
    assert "configuration_effect_overridden_by_later_layer" in " ".join(
        audit["layers"][0]["operator_application"]["reasons"])


def test_independent_configuration_fields_compose_with_hit_but_no_semantic_effect(tmp_path):
    plan, config, result = _run_same_endpoint_chain(tmp_path, REQUESTS[1:])
    _synthetic_formal_gate_input(plan, result)
    audit = _mocked_formal_audit(plan, config, result)
    assert all(layer["hit"] for layer in audit["layers"]), audit
    assert audit["mutation_receipt"]["hit"]
    assert not audit["mutation_receipt"]["effect_observed"]


def test_mocked_formal_recreated_generations_each_require_readback(tmp_path):
    plan, config, result = _run_same_endpoint_chain(tmp_path, [REQUESTS[2], {"operator_id": "generation.action"}])
    _synthetic_formal_gate_input(plan, result)
    audit = _mocked_formal_audit(plan, config, result)
    assert audit["layers"][0]["hit"], audit
    assert len(audit["layers"][0]["configuration_generations"]) == 2
    eid = audit["layers"][0]["qos_delta"][0]["logical_id"]
    def corrupt(events):
        last = [e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == eid][-1]
        last["qos"].pop("liveliness_lease_duration_ns")
        return events
    _rewrite_raw(result, corrupt)
    audit = _mocked_formal_audit(plan, config, result)
    assert not audit["layers"][0]["hit"]
    assert not audit["mutation_receipt"]["hit"]
