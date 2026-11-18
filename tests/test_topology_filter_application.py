"""Initial CFT consumer contracts: fake API/raw and mocked gates, NOT DDS.

Nothing here qualifies execution, candidate admission, sample filtering, match,
dynamic updates or lease behavior. The actual creation/readback producer is
called with fake Node/Subscription objects and no ROS context is started.
"""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_route import audit_topology_application
from r2f.runtime.topology_operator_application import audit_operator_application
from r2f.runtime.topology_worker import OfflineTopologyBackend, _create_reader_with_initial_filter
from test_topology_operator_application import _config, _rewrite_raw
from test_topology_operator_route_integration import _synthetic_formal_gate_input, _mocked_formal_audit


class FakeOptions:
    def __init__(self, filter_expression, expression_parameters):
        self.filter_expression = filter_expression
        self.expression_parameters = list(expression_parameters)


class FakeNode:
    def create_subscription(self, message_type, topic, callback, qos, *, content_filter_options=None):
        content_filter_options = content_filter_options or FakeOptions("", [])
        return SimpleNamespace(get_content_filter=lambda: content_filter_options,
                               is_cft_enabled=bool(content_filter_options.filter_expression.strip()))


def _filter_run(tmp_path, *, before=True, variant="empty", generation=False):
    parent, cfg = _config([{"operator_id": "action.insert"}])
    program = deepcopy(topology_input_layer_receipts(parent)[0]["input_before"])
    reader = next(e for e in program["endpoints"] if e["role"] == "reader")
    reader_index = program["endpoints"].index(reader)
    if before:
        reader["filter"] = {"expression": "data = %0", "parameters": ["keep"], "evaluation": False}
    requests = [{"operator_id": "filter.expr",
                 "context": {"filter_variant": variant, "endpoint_index": reader_index}}]
    if generation:
        requests.append({"operator_id": "generation.action",
                         "context": {"endpoint_id": reader["logical_id"]}})
    plan = make_topology_plan(program, requests, rng_seed=1)
    cfg.update(input_layer_receipts=topology_input_layer_receipts(plan), final_input=plan.final_config,
               mutation_plan=plan.to_dict(), operations=[op.to_dict() for op in plan.operations],
               contract_hash=plan.contract_hash, topology_program=plan.final_config["topology_program"],
               plan_hash=plan.plan_hash())
    identity = deepcopy(cfg["effective_input"])
    for key in ("input_layer_receipts", "final_input", "mutation_plan", "operations", "contract_hash"):
        identity[key] = deepcopy(cfg[key])
    cfg["effective_input"] = identity
    cfg["effective_input_sha256"] = effective_input_hash(identity, schema=INPUT_SCHEMA_V2)
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare", side_effect=AssertionError("NO ROS")):
        result = run_pair(cfg, tmp_path, topology_backend=OfflineTopologyBackend())
    return plan, cfg, result


def _mock_initial_chain(plan, result, *, fixture=False):
    """Call the existing producer; bind its FAKE readback to synthetic raw.

    Times are explicitly synthetic, rebased inside each create action. This
    tests the consumer's chronology checks, not a real observation window.
    """
    specs = {e["logical_id"]: e for e in plan.final_config["topology_program"]["endpoints"]}
    def transform(events):
        events = [e for e in events if not e["event"].startswith("content_filter_initial_")]
        for index, event in enumerate(events):
            event["monotonic_ns"] = (index + 1) * 10000
        output = []
        for token in events:
            spec = specs.get(token.get("logical_id"))
            if token["event"] == "endpoint_created" and spec and "filter" in spec:
                captured = []
                _create_reader_with_initial_filter(
                    FakeNode(), object, spec["topic"], lambda _: None, object, spec["filter"],
                    options_factory=FakeOptions, fixture=fixture,
                    emit=lambda event, **fields: captured.append({"event": event, **deepcopy(fields)}))
                base = token["monotonic_ns"] - 1000
                worker = token.get("worker_role", token.get("topology_worker_role", "reader"))
                expected_backend = "offline_fake" if fixture else "ros2_public_api"
                expected_real_ros = not fixture
                identity = {"logical_id": token["logical_id"], "generation": token["generation"],
                            "role": token["role"], "worker_role": worker,
                            "topology_worker_role": worker, "topology_logical_id": token["logical_id"],
                            "topology_generation": token["generation"], "pid": token["pid"],
                            "process_start_ticks": token["process_start_ticks"],
                            "endpoint_generation": token["endpoint_generation"],
                            "run_id": token["run_id"], "domain_id": token["domain_id"],
                            "observed_domain_id": token["domain_id"],
                            "rmw_identifier": (token.get("rmw_identifier") if fixture
                                               else "rmw_cyclonedds_cpp"),
                            "backend": expected_backend, "real_ros": expected_real_ros}
                worker_begin = {**identity, "event": "endpoint_create_begin", "monotonic_ns": base + 100}
                output.append(worker_begin)
                for offset, row in zip((200, 400, 700), captured):
                    row.update(identity, monotonic_ns=base + offset,
                               test_kind="synthetic_fake_CFT_producer_not_execution_evidence")
                    receipt = row["filter_receipt"]
                    receipt["monotonic_ns"] = base + offset - 1
                    if row["event"] != "content_filter_initial_begin":
                        receipt["creation_returned_monotonic_ns"] = base + 300
                    if row["event"] == "content_filter_initial_configuration":
                        results = receipt["api_results"]
                        for api, start, end in (("get_content_filter", 450, 500), ("is_cft_enabled", 550, 600)):
                            results[api].update(started_monotonic_ns=base+start, completed_monotonic_ns=base+end)
                        token["initial_content_filter"] = deepcopy(receipt)
                    output.append(row)
                token.update(created_monotonic_ns=base+300, create_begin_monotonic_ns=base+100)
            if not fixture:
                token.update(backend="ros2_public_api", real_ros=True,
                             configuration_source="public_graph_endpoint_reflection")
            output.append(token)
        return output
    _rewrite_raw(result, transform)


def _formal_filter(tmp_path, **kwargs):
    plan, cfg, result = _filter_run(tmp_path, **kwargs)
    _synthetic_formal_gate_input(plan, result)
    _mock_initial_chain(plan, result)
    return plan, cfg, result


def _filter_rows(events):
    token = next(e for e in events if e["event"] == "endpoint_created" and "initial_content_filter" in e)
    config = next(e for e in events if e["event"] == "content_filter_initial_configuration")
    return token, config, config["filter_receipt"]


def test_plain_offline_filter_is_consumed_but_unverified_never_hit(tmp_path):
    plan, cfg, result = _filter_run(tmp_path)
    audit = result["application_audit"]
    layer = audit["layers"][0]
    assert layer["status"] == "unknown", audit
    assert "operator_application" in layer
    assert any("filter" in r for r in layer["reasons"])
    assert not audit["mutation_receipt"]["hit"] and not audit["execution_evidence"]
    assert not audit["candidate_eligible"]


def test_fake_CFT_producer_chain_without_formal_gates_is_fixture_only(tmp_path):
    plan, cfg, result = _filter_run(tmp_path)
    _mock_initial_chain(plan, result, fixture=True)
    audit = audit_topology_application(plan, cfg["input_layer_receipts"], result)
    assert audit["status"] == "fixture_application_observed", audit["layers"][0]
    assert audit["layers"][0]["configuration_intervention_proven"]
    assert not audit["layers"][0]["hit"] and not any(audit["mutation_receipt"].values())


def test_qualified_mocked_formal_filter_intervention_hits_without_semantic_effect(tmp_path):
    plan, cfg, result = _formal_filter(tmp_path)
    audit = _mocked_formal_audit(plan, cfg, result)
    assert audit["status"] == "application_observed", audit
    layer = audit["layers"][0]
    assert layer["hit"] and layer["configuration_intervention_proven"]
    assert layer["operator_application"]["hit"]
    assert layer["filter_scope"] == "initial_only"
    assert layer["filter_false_implies_unmatch"] is False
    assert not layer["effect_observed"]
    assert not layer["operator_application"]["effect_observed"]
    assert not layer["dynamic_update_covered"] and not layer["lease_covered"]
    assert audit["mutation_receipt"]["hit"] and not audit["mutation_receipt"]["effect_observed"]
    assert not audit["candidate_eligible"]
    # Standalone artifact parser still cannot grant any execution/hit rights.
    events = [json.loads(s) for s in Path(result["raw_path"]).read_text().splitlines()]
    record = audit_operator_application(cfg["input_layer_receipts"], events,
                                       plan.final_config["topology_program"], fixture=False)[0]
    assert record["configuration_intervention_proven"]
    assert not record["hit"] and not record["execution_evidence"]


@pytest.mark.parametrize("corruption", [
    "configured_echo", "configured_input_echo_only", "getter_missing", "enabled_missing", "getter_unavailable", "enabled_failed",
    "raw_expression", "raw_parameters", "raw_enabled", "observed_echo", "nested_echo", "configured_null",
    "configured_unverified", "not_accepted", "guard", "missing_chain", "duplicate_configuration",
    "wrong_worker", "wrong_process", "wrong_generation", "wrong_endpoint", "wrong_run", "wrong_domain",
    "getter_before_creation", "getter_after_create_action", "enabled_before_getter", "missing_api_time",
    "row_outside_action", "nested_time_outside_action", "created_timestamp_forged",
])
def test_filter_readback_input_echo_identity_and_freshness_fail_closed(tmp_path, corruption):
    plan, cfg, result = _formal_filter(tmp_path)
    def corrupt(events):
        token, row, receipt = _filter_rows(events)
        api = receipt["api_results"]
        if corruption == "configured_echo":
            receipt.pop("api_results")
        elif corruption == "configured_input_echo_only":
            api["get_content_filter"].pop("raw_result")
        elif corruption == "getter_missing":
            api.pop("get_content_filter")
        elif corruption == "enabled_missing":
            api.pop("is_cft_enabled")
        elif corruption == "getter_unavailable":
            api["get_content_filter"]["status"] = "unavailable_api"
        elif corruption == "enabled_failed":
            api["is_cft_enabled"]["status"] = "failed"
        elif corruption == "raw_expression":
            api["get_content_filter"]["raw_result"]["filter_expression"] = "data = %0"
        elif corruption == "raw_parameters":
            api["get_content_filter"]["raw_result"]["expression_parameters"] = ["forged"]
        elif corruption == "raw_enabled":
            api["is_cft_enabled"]["raw_result"] = True
        elif corruption == "observed_echo":
            receipt["observed"]["parameters"] = ["forged"]
        elif corruption == "nested_echo":
            token["initial_content_filter"]["observed"] = None
            return events
        elif corruption == "configured_null":
            receipt["configured"] = None
        elif corruption == "configured_unverified":
            receipt["configuration_status"] = "unverified"
        elif corruption == "not_accepted":
            receipt["accepted"] = False
        elif corruption == "guard":
            events.insert(events.index(row), {**deepcopy(row), "event": "content_filter_initial_guard"})
        elif corruption == "missing_chain":
            events[:] = [e for e in events if e["event"] != "content_filter_initial_begin"]
        elif corruption == "duplicate_configuration":
            events.insert(events.index(row), deepcopy(row))
        elif corruption.startswith("wrong_"):
            key, value = {"wrong_worker": ("worker_role", "reader4"), "wrong_process": ("pid", 123),
                          "wrong_generation": ("generation", 99), "wrong_endpoint": ("logical_id", "OTHER"),
                          "wrong_run": ("run_id", "OTHER"), "wrong_domain": ("domain_id", 0)}[corruption]
            row[key] = value
        elif corruption == "getter_before_creation":
            api["get_content_filter"]["started_monotonic_ns"] = receipt["creation_returned_monotonic_ns"] - 1
        elif corruption == "getter_after_create_action":
            api["get_content_filter"]["completed_monotonic_ns"] = 10**18
        elif corruption == "enabled_before_getter":
            api["is_cft_enabled"]["started_monotonic_ns"] = api["get_content_filter"]["started_monotonic_ns"] - 1
        elif corruption == "missing_api_time":
            api["get_content_filter"].pop("started_monotonic_ns")
        elif corruption == "row_outside_action":
            events.remove(row)
            events.append(row)
        elif corruption == "nested_time_outside_action":
            receipt["monotonic_ns"] = 1
        elif corruption == "created_timestamp_forged":
            token["created_monotonic_ns"] += 1
        token["initial_content_filter"] = deepcopy(receipt)
        return events
    _rewrite_raw(result, corrupt)
    audit = _mocked_formal_audit(plan, cfg, result)
    assert not audit["mutation_receipt"]["hit"], (corruption, audit)
    assert audit["layers"][0]["reasons"], (corruption, audit)
    assert not audit["mutation_receipt"]["effect_observed"] and not audit["candidate_eligible"]


def test_adding_empty_filter_to_unfiltered_input_is_a_public_alias_not_hit(tmp_path):
    plan, cfg, result = _formal_filter(tmp_path, before=False)
    audit = _mocked_formal_audit(plan, cfg, result)
    assert not audit["mutation_receipt"]["hit"], audit
    assert "public_configuration_alias" in " ".join(audit["layers"][0]["reasons"])


def test_current_valid_String_filter_can_hit_only_after_independent_mock_readback(tmp_path):
    plan, cfg, result = _formal_filter(tmp_path, variant="valid")
    audit = _mocked_formal_audit(plan, cfg, result)
    assert audit["mutation_receipt"]["hit"], audit
    assert not audit["mutation_receipt"]["effect_observed"]
    assert not audit["candidate_eligible"]


def test_invalid_filter_expression_is_harness_rejection_not_sut_candidate(tmp_path):
    plan, cfg, result = _filter_run(tmp_path, variant="invalid")
    audit = result["application_audit"]
    assert not audit["mutation_receipt"]["hit"] and not audit["candidate_eligible"]
    assert audit["status"] != "application_observed"


def test_filter_every_executed_generation_needs_its_own_readback(tmp_path):
    plan, cfg, result = _formal_filter(tmp_path, generation=True)
    # Bind the recreate explicitly to the filter-affected reader.
    reader = next(e["logical_id"] for e in plan.final_config["topology_program"]["endpoints"] if e["role"] == "reader")
    events = [json.loads(s) for s in Path(result["raw_path"]).read_text().splitlines()]
    created = [e for e in events if e["event"] == "endpoint_created" and e["logical_id"] == reader]
    assert len(created) == 2
    audit = _mocked_formal_audit(plan, cfg, result)
    assert audit["layers"][0]["hit"], audit
    def corrupt(rows):
        configs = [e for e in rows if e["event"] == "content_filter_initial_configuration"]
        configs[-1]["filter_receipt"]["api_results"].pop("get_content_filter")
        return rows
    _rewrite_raw(result, corrupt)
    assert not _mocked_formal_audit(plan, cfg, result)["mutation_receipt"]["hit"]
