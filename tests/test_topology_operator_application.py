"""Independent offline auditor for topology parameter/relation operators.

No ROS, no DDS, no campaign, no netns.  Every assertion reads either the
sealed plan snapshots or hashed raw ``endpoint_created`` bytes.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_input import (
    TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET, make_topology_plan, topology_input_layer_receipts,
)
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_operator_application import (
    AUDITED_OPERATORS, audit_operator_application, layer_qos_delta, read_raw_qos,
)
from r2f.runtime.topology_worker import OfflineTopologyBackend


def _config(requests, seed=5, writers=1, readers=1):
    program = model.generate_program(7, writers=writers, readers=readers)
    for endpoint in program["endpoints"]:
        endpoint["qos"]["reliability"] = "reliable"
    plan = make_topology_plan(program, requests, rng_seed=seed)
    identity = {"input_identity_schema": INPUT_SCHEMA_V2, "target_id": TOPOLOGY_TARGET,
                "target_revision": 0, "mutation_kind": "endpoint_program",
                "carrier": "ros2_endpoint", "mutation_parameters": {},
                "rmw": "rmw_cyclonedds_cpp", "repeat": 0, "seed": seed, "arm": "mutated",
                "mutation_layer_order": ["endpoint_program"],
                "input_layer_receipts": topology_input_layer_receipts(plan),
                "final_input": plan.final_config, "source_manifest": {},
                "mutation_plan": plan.to_dict(),
                "operations": [op.to_dict() for op in plan.operations],
                "runtime_windows": [], "contract_hash": plan.contract_hash}
    return plan, {**deepcopy(identity), "effective_input": deepcopy(identity),
                  "effective_input_sha256": effective_input_hash(identity, schema=INPUT_SCHEMA_V2),
                  "topology_program": deepcopy(plan.final_config["topology_program"]),
                  "plan_hash": plan.plan_hash(), "runtime_capability": TOPOLOGY_CAPABILITY,
                  "backend": "offline_fake", "run_id": "operator-audit-fixture", "domain_id": 191}


def _run(tmp_path, requests):
    plan, config = _config(requests)
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare",
               side_effect=AssertionError("NO ROS")):
        result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    return plan, config, result


def _audit(plan, config, result):
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    return audit_operator_application(config["input_layer_receipts"], events,
                                      plan.final_config["topology_program"],
                                      fixture=True)


def _rewrite_raw(result, transform):
    path = Path(result["raw_path"])
    events = [json.loads(line) for line in path.read_text().splitlines()]
    events = transform(events)
    for index, event in enumerate(events):
        event["event_index"] = index
    path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
    result["raw_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()


def test_audited_operators_cover_requested_parameter_family():
    assert set(AUDITED_OPERATORS) == {"param.enum", "queue.depth", "param.boundary",
                                     "relation.pair", "timing.shift", "lease.value", "filter.expr"}


def test_relation_pair_uses_raw_per_role_qos_not_config(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair",
                                            "context": {"pair": "BB"}}])
    record = _audit(plan, config, result)[0]
    assert record["status"] == "fixture_application_observed", record
    pair = record["relation_pair"]
    assert pair["mutated_fields"] == ["reliability"]
    assert pair["source"] == "declared_layer_pair_identity_qos_from_raw_only"
    assert set(record["observed_qos"]) == {pair["writer"], pair["reader"]}
    for values in record["observed_qos"].values():
        assert values == {"reliability": "best_effort"}
    assert record["dds_contract"]["status"] == "compatible"
    assert record["dds_contract"]["not_an_observed_match"] is True
    # Applied configuration is not a mutation hit and not a pass.
    assert record["hit"] is False and record["candidate_eligible"] is False
    assert record["effect_observation"] == "unknown_effect_owner_is_oracle"


def test_relation_pair_incompatible_is_observed_not_failed(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair",
                                            "context": {"pair": "BR"}}])
    record = _audit(plan, config, result)[0]
    assert record["reasons"] == []
    assert record["status"] == "fixture_application_observed"
    assert record["dds_contract"]["status"] == "incompatible"
    contract = record["dds_contract"]["fields"]["reliability"]
    assert contract["offered"] == "best_effort" and contract["requested"] == "reliable"
    assert record["effect_observation"] == "unknown_expected_no_match"
    assert record["hit"] is False


def test_relation_pair_unchanged_role_value_is_still_raw_checked(tmp_path):
    # BR changes only the writer; the reader keeps its parent value, and that
    # parent value must still appear in the reader's own raw reflection.
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair",
                                            "context": {"pair": "BR"}}])
    record = _audit(plan, config, result)[0]
    assert record["relation_pair"]["role_delta_fields"] == {record["relation_pair"]["writer"]: ["reliability"],
                                                            record["relation_pair"]["reader"]: []}
    assert record["observed_qos"][record["relation_pair"]["reader"]] == {"reliability": "reliable"}

    def change(events):
        created = next(e for e in events if e["event"] == "endpoint_created"
                       and e["logical_id"] == record["relation_pair"]["reader"])
        created["qos"]["reliability"] = "best_effort"
        return events
    _rewrite_raw(result, change)
    tampered = _audit(plan, config, result)[0]
    assert tampered["status"] == "unknown"
    assert any("layer_value_not_reflected" in reason for reason in tampered["reasons"])


def test_relation_pair_missing_role_field_is_incomplete_not_backfilled(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair",
                                            "context": {"pair": "BB"}}])
    pair = _audit(plan, config, result)[0]["relation_pair"]

    def change(events):
        created = next(e for e in events if e["event"] == "endpoint_created"
                       and e["logical_id"] == pair["reader"])
        created["qos"].pop("reliability")
        return events
    _rewrite_raw(result, change)
    record = _audit(plan, config, result)[0]
    assert record["status"] == "unknown"
    assert any("reflected_qos_field_missing" in reason for reason in record["reasons"])
    assert "dds_contract" not in record and record["hit"] is False


def test_relation_pair_wrong_role_value_fails_the_reflection_check(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair",
                                            "context": {"pair": "BB"}}])
    pair = _audit(plan, config, result)[0]["relation_pair"]

    def change(events):
        created = next(e for e in events if e["event"] == "endpoint_created"
                       and e["logical_id"] == pair["writer"])
        created["qos"]["reliability"] = "reliable"
        return events
    _rewrite_raw(result, change)
    record = _audit(plan, config, result)[0]
    assert record["status"] == "unknown"
    assert any("layer_value_not_reflected" in reason for reason in record["reasons"])


def test_relation_pair_rejects_declared_pair_with_wrong_roles(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair",
                                            "context": {"pair": "BB"}}])
    tampered = deepcopy(config["input_layer_receipts"])
    effect = tampered[0]["v2_case"]["expected_guard_effect"]
    effect["writer_index"], effect["reader_index"] = effect["reader_index"], effect["writer_index"]
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    record = audit_operator_application(tampered, events,
                                        plan.final_config["topology_program"], fixture=True)[0]
    assert record["status"] == "unknown"
    assert "declared_relation_pair_roles_not_writer_reader" in record["reasons"]


def test_timing_shift_bounded_target_must_be_raw_reflected(tmp_path):
    plan, config, result = _run(tmp_path, [{
        "operator_id": "timing.shift", "slot_id": "S1.topology.qos.deadline",
        "context": {"offset_sec": 0.002}}])
    record = _audit(plan, config, result)[0]
    assert record["status"] == "fixture_application_observed", record
    delta = record["timing_delta"][0]
    assert delta["field"] == "deadline_sec" and delta["bounded"] is True
    assert delta["domain"] == {"min": 0, "max": 86400, "special": ["infinite"]}
    assert delta["observed"] == 2_000_000        # ns, as the endpoint reported
    assert delta["observed_source"] == "offline_fixture"
    assert record["effect_observation"] == "unknown_timing_lifecycle_not_measured"
    assert "timing_field_is_not_an_event_time_shift" in record["limitations"]
    assert record["hit"] is False


def test_timing_shift_wrong_raw_target_is_incomplete(tmp_path):
    plan, config, result = _run(tmp_path, [{
        "operator_id": "timing.shift", "slot_id": "S1.topology.qos.deadline",
        "context": {"offset_sec": 0.002}}])

    def change(events):
        created = next(e for e in events if e["event"] == "endpoint_created"
                       and e["qos"].get("deadline_sec") == 0.002)
        created["qos"]["deadline_sec"] = 0.003
        return events
    _rewrite_raw(result, change)
    record = _audit(plan, config, result)[0]
    assert record["status"] == "unknown"
    assert any("timing_target_not_reflected" in reason or "layer_value_not_reflected" in reason
               for reason in record["reasons"])


def test_lease_value_is_configuration_only_never_lifecycle(tmp_path):
    plan, config, result = _run(tmp_path, [{
        "operator_id": "lease.value", "slot_id": "S1.topology.qos.lease",
        "context": {"special_class": "finite"}}])
    record = _audit(plan, config, result)[0]
    assert record["status"] == "fixture_application_observed", record
    delta = record["timing_delta"][0]
    assert delta["field"] == "lease_duration_sec"
    assert delta["bounded"] is True and delta["observed"] == 1_000_000
    assert record["effect_scope"] == "endpoint_public_timing_configuration_only_not_lifecycle"
    assert record["effect_observation"] == "unknown_timing_lifecycle_not_measured"
    assert "lease_expiry_refresh_lifecycle_not_implemented" in record["limitations"]
    assert record["hit"] is False


@pytest.mark.parametrize("operator", ["env.lease_expiry", "env.lease_refresh"])
def test_lease_lifecycle_operators_have_named_unsupported_gap(tmp_path, operator):
    plan, config, result = _run(tmp_path, [{"operator_id": operator,
                                            "slot_id": "S1.topology.qos.lease"}])
    record = _audit(plan, config, result)[0]
    assert record["status"] == "unsupported"
    assert record["reasons"] == ["operator_application_auditor_not_implemented:" + operator]
    assert "timing_delta" not in record and record["hit"] is False


@pytest.mark.parametrize("operator,slot,context", [
    ("param.enum", "S1.topology.qos.reliability", {"enum_value": "best_effort"}),
    ("param.enum", "S1.topology.qos.liveliness", {"enum_value": "manual_by_topic"}),
    ("queue.depth", "S1.topology.qos.depth", {}),
    ("param.boundary", "S1.topology.qos.deadline", {"boundary_class": "neighbor"}),
    ("param.boundary", "S1.topology.qos.depth", {"boundary_class": "neighbor"}),
])
def test_existing_parameter_operators_keep_reflected_configuration(tmp_path, operator, slot, context):
    plan, config, result = _run(tmp_path, [{"operator_id": operator, "slot_id": slot,
                                            "context": context}])
    record = _audit(plan, config, result)[0]
    assert record["status"] == "fixture_application_observed", record
    assert record["reasons"] == []
    assert record["qos_delta"], "a parameter layer must change a QoS field"
    assert record["effect_scope"] == "endpoint_public_configuration_only_not_timed_lifecycle"
    assert record["hit"] is False


def test_parameter_layer_without_qos_delta_is_named_not_hit(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "action.insert"}])
    record = _audit(plan, config, result)[0]
    # action.insert is outside this module's audited family: named gap, no hit.
    assert record["status"] == "unsupported"
    assert record["reasons"] == ["operator_application_auditor_not_implemented:action.insert"]
    assert record["hit"] is False


def test_layer_qos_delta_ignores_added_endpoints():
    before = {"endpoints": [{"logical_id": "w0", "role": "writer", "qos": {"depth": 16}}]}
    after = {"endpoints": [{"logical_id": "w0", "role": "writer", "qos": {"depth": 16}},
                           {"logical_id": "w1", "role": "writer", "qos": {"depth": 8}}]}
    assert layer_qos_delta(before, after) == []


def test_read_raw_qos_reports_missing_and_unexpressible_fields():
    created = {"backend": "ros2_public_api", "qos": {"reliability": "best_effort"}}
    assert read_raw_qos(created, "reliability") == ("best_effort", True, None)
    value, ok, why = read_raw_qos(created, "deadline_sec")
    assert value is None and ok is False and why == "reflected_qos_field_missing:deadline_ns"
    value, ok, why = read_raw_qos(created, "ownership")
    assert ok is False and why.startswith("qos_field_not_publicly_expressible")


# Additional independent-auditor regressions; the originals above are retained.
def _public_schema_fixture_events(plan, result):
    """Synthetic public-schema input for a parser test, NOT live evidence."""
    from r2f.runtime.topology_worker import endpoint_expressibility
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    specs = {e["logical_id"]: e for e in plan.final_config["topology_program"]["endpoints"]}
    for event in events:
        if event.get("event") == "endpoint_created":
            event.update(backend="ros2_public_api", real_ros=True,
                         configuration_source="public_graph_endpoint_reflection",
                         rmw_identifier="rmw_cyclonedds_cpp")
            event["qos"] = endpoint_expressibility(specs[event["logical_id"]], {})["public_qos"]
    return events


def test_caller_cannot_relabel_offline_rows_as_real_reflection(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "relation.pair", "context": {"pair": "BB"}}])
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    record = audit_operator_application(config["input_layer_receipts"], events,
                                        plan.final_config["topology_program"], fixture=False)[0]
    assert record["status"] == "unknown"
    assert any("raw_endpoint_provenance_mismatch" in r for r in record["reasons"])
    assert not record["hit"] and not record["execution_evidence"]


@pytest.mark.parametrize("operator,slot,context", [
    ("relation.pair", "S1.topology.qos.reliability", {"pair": "BR"}),
    ("timing.shift", "S1.topology.qos.deadline", {"offset_sec": 0.002}),
    ("lease.value", "S1.topology.qos.lease", {"special_class": "finite"}),
])
def test_public_schema_reflection_is_artifact_only_without_formal_qualification(tmp_path, operator, slot, context):
    plan, config, result = _run(tmp_path, [{"operator_id": operator, "slot_id": slot, "context": context}])
    events = _public_schema_fixture_events(plan, result)
    record = audit_operator_application(config["input_layer_receipts"], events,
                                        plan.final_config["topology_program"], fixture=False)[0]
    assert record["status"] == "artifact_application_observed", record
    assert not record["execution_evidence"] and not record["hit"] and not record["effect_observed"]
    if operator != "relation.pair":
        assert isinstance(record["timing_delta"][0]["observed"], int)


def test_n_endpoint_aggregate_counts_cannot_contradict_a_declared_pair(tmp_path):
    plan, config = _config([{"operator_id": "relation.pair", "context": {"pair": "BR"}}], writers=2, readers=2)
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare", side_effect=AssertionError("NO ROS")):
        result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    events = _public_schema_fixture_events(plan, result)
    for event in events:
        if event.get("event") == "match_snapshot":
            event["matched_count"] = 1
    record = audit_operator_application(config["input_layer_receipts"], events,
                                        plan.final_config["topology_program"], fixture=False)[0]
    assert record["status"] == "artifact_application_observed", record
    assert record["dds_contract"]["status"] == "incompatible"
    assert record["dds_contract"]["prediction_only"] is True
    assert record["observed_effect"]["status"] == "unknown"
    assert record["observed_effect"]["reason"] == "aggregate_counts_do_not_prove_pair_match"
    assert record["effect_observation"] == "unknown_expected_no_match"
    assert not record["hit"] and not record["effect_observed"]


def test_unknown_timing_domain_fails_closed(tmp_path):
    plan, config, result = _run(tmp_path, [{"operator_id": "timing.shift", "slot_id": "S1.topology.qos.deadline",
                                           "context": {"offset_sec": 0.002}}])
    with patch("r2f.runtime.topology_operator_application._slot_domain", return_value={}):
        record = _audit(plan, config, result)[0]
    assert record["status"] == "unknown"
    assert "timing_slot_or_domain_not_supported" in record["reasons"]
