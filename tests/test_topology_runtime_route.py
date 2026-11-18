"""I4 offline route/application contracts. Never start ROS/DDS/campaigns."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, Mock

import pytest

from r2f.core.evidence import INPUT_SCHEMA_V2, canonical_hash, effective_input_hash
from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.runtime.ros2_pair import (
    EXPLORATORY_TARGETS, acquire_pair_environment, run_pair, verify_pair_lease,
)
from r2f.runtime.topology_pair import run_topology
from r2f.runtime.topology_route import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, audit_topology_application, validate_topology_route,
)
from r2f.runtime.topology_worker import OfflineTopologyBackend, RosTopologyBackend, TopologyBackendError
from r2f.runtime.domains import IsolationError, LeaseSet


NETEM_SPEC = {"delay_ms": 20, "reorder_pct": 0, "duplicate_pct": 0,
              "restore_sec": 5, "scope": "data"}


def netem_config(config, spec=None, kind="discovery_netem"):
    config["environment_mutation"] = {"kind": kind, "spec": dict(spec or NETEM_SPEC)}
    return config


def config_for(operator="action.insert", *, requests=None, seed=5):
    program = model.generate_program(7, writers=1, readers=1)
    plan = make_topology_plan(program, requests or [{"operator_id": operator}], rng_seed=seed)
    identity = {"input_identity_schema": INPUT_SCHEMA_V2, "target_id": TOPOLOGY_TARGET,
                "target_revision": 0, "mutation_kind": "endpoint_program", "carrier": "ros2_endpoint",
                "mutation_parameters": {}, "rmw": "rmw_cyclonedds_cpp", "repeat": 0,
                "seed": seed, "arm": "mutated", "mutation_layer_order": ["endpoint_program"],
                "input_layer_receipts": topology_input_layer_receipts(plan), "final_input": plan.final_config,
                "source_manifest": {}, "mutation_plan": plan.to_dict(),
                "operations": [op.to_dict() for op in plan.operations], "runtime_windows": [],
                "contract_hash": plan.contract_hash}
    return plan, {**deepcopy(identity), "effective_input": deepcopy(identity),
                  "effective_input_sha256": effective_input_hash(identity, schema=INPUT_SCHEMA_V2),
                  "topology_program": deepcopy(plan.final_config["topology_program"]),
                  "plan_hash": plan.plan_hash(), "runtime_capability": TOPOLOGY_CAPABILITY,
                  "backend": "offline_fake", "run_id": "i4-contract-fixture", "domain_id": 191}


def run_fixture(tmp_path, operator="action.insert", **kwargs):
    plan, config = config_for(operator, **kwargs)
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare", side_effect=AssertionError("NO ROS")):
        result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    return plan, config, result


def rewrite_raw(runtime, transform):
    path = Path(runtime["raw_path"])
    events = [json.loads(line) for line in path.read_text().splitlines()]
    events = transform(events)
    for index, event in enumerate(events):
        event["event_index"] = index
    path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
    runtime["raw_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()


def test_exact_opt_in_outside_default_catalog(tmp_path):
    assert TOPOLOGY_TARGET not in EXPLORATORY_TARGETS
    plan, config, result = run_fixture(tmp_path)
    assert validate_topology_route(config).to_dict() == plan.to_dict()
    assert result["status"] == "executed"
    assert result["application_audit"]["status"] == "fixture_application_observed"
    assert result["fixture"] and not result["real_execution"]
    assert not any(result[k] for k in ("candidate_eligible", "sealed", "mature"))
    assert not any(result["mutation_receipt"][key] for key in ("applied", "hit", "effect_observed"))
    assert result["scenario_mutation_receipt"]["reason"] == "topology_backend_is_a_scenario_carrier_not_a_mutation"


@pytest.mark.parametrize("key,value,reason", [
    ("target_id", "discovery.endpoint_topology.unknown", "unknown_topology_target"),
    ("runtime_capability", None, "topology_capability_required"),
    ("runtime_capability", "topology_v0", "topology_capability_required"),
    ("mutation_kind", "late_joiner", "topology_carrier_contract_mismatch"),
    ("carrier", "wire_proxy", "topology_carrier_contract_mismatch"),
    ("input_identity_schema", "r2f.input-identity/v1", "topology_requires_v2_input_identity"),
    ("plan_hash", "0" * 64, "topology_plan_hash_mismatch"),
    ("mutation_plan_sha256", "0" * 64, "topology_mutation_plan_sha256_mismatch"),
    ("effective_input_sha256", "0" * 64, "topology_effective_input_hash_mismatch"),
    ("input_mutations", [], "topology_runtime_remutation_forbidden"),
    ("input_mutations", [{"mutator": "add_endpoint"}], "topology_runtime_remutation_forbidden"),
    ("final_input", {}, "topology_final_config_mismatch"),
    ("topology_program", {}, "topology_submitted_program_mismatch"),
    ("input_layer_receipts", [], "topology_input_layer_receipts_mismatch"),
    ("observation_mode", "instrumentation", "topology_only_public_api_blackbox_supported"),
    ("environment_mutation", {"kind": "discovery_netem"}, "topology_netem_spec_required"),
    ("environment_mutation", {"kind": "discovery_netem", "spec": NETEM_SPEC}, "offline_fixture_cannot_claim_environment_mutation"),
    ("environment_mutation", {"kind": "discovery_drop"}, "topology_netem_drop_not_supported"),
    ("live_execution_go", True, "offline_fixture_cannot_claim_real_qualification"),
    ("require_real_execution", True, "offline_fixture_cannot_claim_real_qualification"),
    ("evidence_purpose", "runtime_execution", "offline_fixture_cannot_claim_real_qualification"),
])
def test_rejects_before_execution(tmp_path, key, value, reason):
    _, config = config_for()
    config[key] = value
    result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    assert result["status"] == "blocked"
    assert reason in result["reason"]
    assert result["raw_path"] is None
    assert not list(tmp_path.iterdir())


def test_plan_validated_without_rerunning_mutator(tmp_path):
    _, config = config_for()
    with patch.object(model, "mutate_chain", side_effect=AssertionError("NO MUTATION")), patch.object(
            model, "mutate_program", side_effect=AssertionError("NO MUTATION")), patch(
            "r2f.modules.s1.fuzz_mutators_v2.mutate", side_effect=AssertionError("NO MUTATION")):
        assert run_pair(config, tmp_path)["status"] == "executed"


@pytest.mark.parametrize("operator", ["endpoint.count", "action.insert", "action.delete", "action.swap", "generation.action"])
def test_named_application_operators_have_raw_fixture_proof(tmp_path, operator):
    plan, config, result = run_fixture(tmp_path, operator)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "fixture_application_observed", audit
    layer = audit["layers"][0]
    assert layer["input_before"] != layer["input_after"] and layer["raw_refs"]
    assert not layer["hit"] and not layer["execution_evidence"]
    assert all(ref["sha256"] == result["raw_sha256"] for ref in layer["raw_refs"])


def test_audit_ignores_returned_facts_and_runtime_status(tmp_path):
    plan, config, result = run_fixture(tmp_path)
    result.update(facts={"everything": True}, status="completed", real_execution=True, fixture=False)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["fixture"] and not audit["execution_evidence"]
    assert not audit["mutation_receipt"]["hit"]


@pytest.mark.parametrize("event", ["input_binding", "topology_action_begin", "topology_action_done", "endpoint_created", "topology_pair_observation"])
def test_missing_raw_application_never_blanket_hits(tmp_path, event):
    plan, config, result = run_fixture(tmp_path)
    rewrite_raw(result, lambda events: [e for e in events if e["event"] != event])
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] in {"invalid", "incomplete"}, audit
    assert not audit["mutation_receipt"]["hit"]


def test_raw_hash_mismatch_preserves_failure(tmp_path):
    plan, config, result = run_fixture(tmp_path)
    Path(result["raw_path"]).write_text("tampered\n")
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "invalid" and "raw_hash_mismatch" in audit["reasons"][0]


def test_raw_endpoint_configuration_cannot_be_backfilled(tmp_path):
    plan, config, result = run_fixture(tmp_path)
    def change(events):
        next(e for e in events if e["event"] == "endpoint_created")["topic"] = "/wrong"
        return events
    rewrite_raw(result, change)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert "raw_endpoint_configuration_mismatch" in audit["layers"][0]["reasons"]


def test_generation_cannot_reuse_predecessor_gid(tmp_path):
    plan, config, result = run_fixture(tmp_path, "generation.action")
    def change(events):
        creates = [e for e in events if e["event"] == "endpoint_created"]
        new = next(e for e in creates if e["generation"] == 2)
        old = next(e for e in creates if e["logical_id"] == new["logical_id"] and e["generation"] == 1)
        new["endpoint_gid"] = old["endpoint_gid"]
        return events
    rewrite_raw(result, change)
    assert audit_topology_application(plan, config["input_layer_receipts"], result)["status"] == "incomplete"


def test_unimplemented_operator_named_unsupported(tmp_path):
    plan, config, result = run_fixture(tmp_path, "guard.dependency")
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["layers"][0]["status"] == "unsupported"
    assert "operator_application_auditor_not_implemented" in audit["layers"][0]["reasons"][0]


def test_multilayer_audits_each_snapshot_not_just_final(tmp_path):
    plan, config, result = run_fixture(tmp_path, requests=[{"operator_id": "endpoint.count"}, {"operator_id": "action.insert"}])
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert len(audit["layers"]) == 2
    assert audit["status"] == "fixture_application_observed", audit


def test_failed_backend_keeps_begin_done_guard_and_raw(tmp_path):
    class Broken(OfflineTopologyBackend):
        def create_endpoint(self, *args):
            raise TopologyBackendError("injected fixture creation failure")
    plan, config = config_for("endpoint.count")
    result = run_pair(config, tmp_path, topology_backend=Broken())
    assert result["guard_receipts"] and Path(result["raw_path"]).is_file()
    assert result["application_audit"]["status"] == "incomplete"
    assert not result["mutation_receipt"]["hit"]


def test_reusing_state_directory_does_not_overwrite_raw(tmp_path):
    _, config, first = run_fixture(tmp_path)
    data = Path(first["raw_path"]).read_bytes()
    second = run_pair(config, tmp_path)
    assert second["reason"] == "blocked:state_directory_already_executed"
    assert Path(first["raw_path"]).read_bytes() == data


def test_no_live_go_never_calls_preflight(tmp_path):
    _, config = config_for()
    config.pop("backend")
    with patch("r2f.runtime.ros2_pair.acquire_pair_environment", side_effect=AssertionError("NO ROS")):
        result = run_pair(config, tmp_path)
    assert result["reason"] == "blocked:no_live_ros_go"


def test_live_go_does_not_bypass_actual_source_freeze(tmp_path):
    _, config = config_for()
    config.pop("backend")
    config["live_execution_go"] = True
    with patch("r2f.campaign.identity.source_manifest", return_value={"source_frozen": False}), patch(
            "r2f.runtime.ros2_pair.acquire_pair_environment", side_effect=AssertionError("NO ROS")):
        result = run_pair(config, tmp_path)
    assert result["status"] == "blocked"
    assert "source_identity_changed_or_not_frozen" in result["reason"]
    assert Path(result["raw_path"]).is_file()


def test_backend_rejects_config_lease_receipt_boolean(tmp_path):
    backend = RosTopologyBackend({"live_execution_go": True, "lease_receipts": [True],
                                  "campaign_id": "x", "domain_id": 191}, tmp_path)
    with pytest.raises(TopologyBackendError, match="canonical_three_leases"):
        backend.prepare({"environment": {}}, {"live_execution_go": True})


def test_canonical_environment_helper_retains_three_leases_until_caller_cleanup(tmp_path):
    cfg = {"campaign_id": "i4-lease-test", "domain_id": 191, "rmw": "rmw_cyclonedds_cpp", "lock_root": str(tmp_path/"locks")}
    env = {"ok": True, "rmw": {"rmw_cyclonedds_cpp": {"available": True}}}
    with patch("r2f.runtime.ros2_pair.check_environment", return_value=env), patch(
            "r2f.runtime.ros2_pair.activated_environment", return_value=({}, {"PATH": "/offline"})):
        lease, observed, _ = acquire_pair_environment(cfg, tmp_path, lambda *a, **k: None)
    try:
        assert observed == env and len(verify_pair_lease(lease, cfg, tmp_path)) == 3
        with pytest.raises(IsolationError):
            LeaseSet(tmp_path/"locks", tmp_path, cfg["campaign_id"], 191).__enter__()
    finally:
        lease.__exit__(None, None, None)
    with pytest.raises(IsolationError):
        verify_pair_lease(lease, cfg, tmp_path)


def test_canonical_preflight_failure_releases_leases_without_workers(tmp_path):
    cfg = {"campaign_id": "i4-failed-env", "domain_id": 191, "lock_root": str(tmp_path/"locks")}
    with patch("r2f.runtime.ros2_pair.check_environment", return_value={"ok": False, "reason": "domain_conflict:fixture"}):
        with pytest.raises(IsolationError, match="domain_conflict"):
            acquire_pair_environment(cfg, tmp_path, lambda *a, **k: None)
    with LeaseSet(tmp_path/"locks", tmp_path, cfg["campaign_id"], 191):
        pass


def test_raw_action_order_and_terminal_no_success_backfill(tmp_path):
    _, _, result = run_fixture(tmp_path)
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    assert events[-1]["event"] == "runtime_end"
    begins = [e for e in events if e["event"] == "topology_action_begin"]
    ends = [e for e in events if e["event"] == "topology_action_done"]
    assert len(begins) == len(ends)
    assert all(b["event_index"] < e["event_index"] for b, e in zip(begins, ends))
    assert all(not e["mutation_receipt"]["hit"] for e in events if e["event"] == "runtime_end")


@pytest.mark.parametrize("operator,slot,context", [
    ("param.enum", "S1.topology.qos.reliability", {"enum_value": "best_effort"}),
    ("param.enum", "S1.topology.qos.liveliness", {"enum_value": "manual_by_topic"}),
    ("queue.depth", "S1.topology.qos.depth", {}),
    ("param.boundary", "S1.topology.qos.depth", {"boundary_class": "neighbor"}),
    ("param.boundary", "S1.topology.qos.deadline", {"boundary_class": "min"}),
])
def test_qos_operator_requires_raw_created_configuration(tmp_path, operator, slot, context):
    requests = [{"operator_id": operator, "slot_id": slot, "context": context}]
    plan, config, result = run_fixture(tmp_path, requests=requests)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "fixture_application_observed", audit
    assert audit["layers"][0]["effect_scope"] == "endpoint_public_configuration_only_not_timed_lifecycle"
    def change(events):
        for event in events:
            if event["event"] == "endpoint_created":
                event["qos"] = None
        return events
    rewrite_raw(result, change)
    assert audit_topology_application(plan, config["input_layer_receipts"], result)["status"] == "incomplete"


def test_canonical_receipts_exact_seven_fields_raw_and_result(tmp_path):
    from r2f.core.mutation import MutationReceipt
    _, _, result = run_fixture(tmp_path)
    keys = {"planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed"}
    assert set(result["mutation_receipt"]) == keys
    MutationReceipt.from_dict(result["mutation_receipt"])
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    for event in events:
        if "mutation_receipt" in event:
            assert set(event["mutation_receipt"]) == keys
            MutationReceipt.from_dict(event["mutation_receipt"])


def test_real_orchestration_mock_releases_only_after_cleanup(tmp_path):
    # Orchestration-only mock. No endpoint workers or ROS library imports.
    plan, config = config_for()
    program = model.generate_program(7, writers=0, readers=0)
    plan = make_topology_plan(program, [{"operator_id": "action.insert"}], rng_seed=5)
    identity = config["effective_input"]
    identity.update(mutation_plan=plan.to_dict(), operations=[o.to_dict() for o in plan.operations],
                    final_input=plan.final_config, contract_hash=plan.contract_hash,
                    input_layer_receipts=topology_input_layer_receipts(plan))
    source = {"source_repo": str(tmp_path), "head": "mock-only", "source_frozen": True, "behavior_tree_sha256": "a"*64}
    identity["source_manifest"] = source
    config.update(deepcopy(identity))
    config.update(plan_hash=plan.plan_hash(), topology_program=plan.final_config["topology_program"],
                  effective_input_sha256=effective_input_hash(identity, schema=INPUT_SCHEMA_V2),
                  live_execution_go=True, lock_root=str(tmp_path/"locks"))
    config.pop("backend")
    env = {"ok": True, "rmw": {"rmw_cyclonedds_cpp": {"available": True}},
           "environment_mode": "ros_overlay", "ros_distro": "jazzy",
           "source": {"head": "ACTUAL-MOCK-ENV-HEAD", "source_repo": str(tmp_path),
                      "runtime_source_sha256": "b" * 64},
           "fixture": True, "test_kind": "orchestration_only_no_ros_workers",
           "process_scan": {"ok": True}, "old_lock_scan": {"ok": True},
           "rtps_probe": {"ok": True}, "dependency_lock_sha256": "c" * 64}
    original_close = RosTopologyBackend.close
    def checked_close(backend):
        assert len(verify_pair_lease(backend.execution_lease, backend.config, backend.output_dir)) == 3
        return original_close(backend)
    with patch("r2f.campaign.identity.source_manifest", return_value=source), patch(
            "r2f.runtime.ros2_pair.check_environment", return_value=env), patch(
            "r2f.runtime.ros2_pair.activated_environment", return_value=({}, {"PATH": "/offline"})), patch.object(
            RosTopologyBackend, "close", checked_close), patch.object(
            RosTopologyBackend, "create_endpoint", side_effect=AssertionError("NO ROS WORKERS")):
        result = run_pair(config, tmp_path)
    assert result["status"] == "executed", result["reason"]
    assert not result["real_execution"] and not result["mutation_receipt"]["hit"]
    persisted = json.loads((tmp_path / "environment.json").read_text())
    assert result["environment"] == persisted == env
    assert result["environment_kind"] == "canonical_preflight"
    assert result["backend_preflight"] == result["preparation_receipt"]
    assert result["backend_preflight"] != result["environment"]
    assert result["execution_metadata"]["source_identity"] == env["source"]
    assert result["execution_metadata"]["source_manifest"] == source
    # Source is from observed preflight/capture, never fabricated from config.
    assert result["execution_metadata"]["source_identity"] != config["source_manifest"]
    saved = deepcopy(result["environment"])
    result["preparation_receipt"]["observed_environment"]["source"] = {"fake": True}
    assert result["environment"] == saved == json.loads((tmp_path / "environment.json").read_text())
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    closed = next(e for e in events if e["event"] == "topology_cleanup")
    source_after = next(e for e in events if e["event"] == "topology_source_checked" and e["phase"] == "after_cleanup")
    released = next(e for e in events if e["event"] == "isolation_locks_released")
    assert closed["event_index"] < source_after["event_index"] < released["event_index"]
    from r2f.modules.s1.topology_targets import topology_target_declaration
    state = {"environment_ok": True, "locks_held": True, "source_identity_ok": True,
             "topology_capability_admitted": True}
    mutation = next(e for e in events if e["event"] == "mutation_begin")
    for step in topology_target_declaration().state_steps:
        before = next(e for e in events if e["event"] == "state_step_before" and e["step_id"] == step.step_id)
        after = next(e for e in events if e["event"] == "state_step_after" and e["step_id"] == step.step_id)
        assert before["before"] == state and all(before["requires"].values())
        state.update(step.parameters["after_updates"])
        assert after["after"] == state and after["satisfied"]
        assert before["event_index"] < after["event_index"] < mutation["event_index"]
    # Mock measurements validate orchestration only, not actual live qualification.
    assert result["canonical_preconditions_reached"] is True
    assert result["application_audit"]["canonical_preconditions_observed"] is True
    # Raw chronology/state is rechecked by the application auditor, not trusted
    # from the returned readiness bit or helper receipts.
    def move_bound_after_mutation(events):
        after = next(e for e in events if e["event"] == "state_step_after" and e["step_id"] == "bind_topology_input")
        events.remove(after)
        begin = next(e for e in events if e["event"] == "mutation_begin")
        events.insert(events.index(begin) + 1, after)
        return events
    rewrite_raw(result, move_bound_after_mutation)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["canonical_preconditions_observed"] is False
    assert not audit["mutation_receipt"]["hit"]
    with LeaseSet(tmp_path/"locks", tmp_path, config["run_id"], config["domain_id"]):
        pass


def test_open_lease_handles_without_flocks_do_not_qualify(tmp_path):
    cfg = {"campaign_id": "i4-unlocked-handles", "domain_id": 191}
    lease = LeaseSet(tmp_path/"locks", tmp_path, cfg["campaign_id"], 191).__enter__()
    import fcntl
    try:
        for handle in lease.handles:
            fcntl.flock(handle, fcntl.LOCK_UN)
        with pytest.raises(IsolationError, match="flock_not_held"):
            verify_pair_lease(lease, cfg, tmp_path)
    finally:
        lease.__exit__(None, None, None)


def test_endpoint_payload_config_needs_no_fictitious_pair_fields(tmp_path):
    _, config = config_for()
    assert not {"topic", "message_type", "writer_qos", "reader_qos", "qos"} & set(config)
    result = run_pair(config, tmp_path)
    assert result["status"] == "executed"


def test_worker_metadata_not_filled_from_declared_fixture_rmw(tmp_path):
    _, config, result = run_fixture(tmp_path)
    assert result["execution_metadata"]["rmw"] is None
    assert result["execution_metadata"]["domain_id"] is None
    assert result["execution_metadata"]["declared_rmw"] == config["rmw"]


def test_unsupported_chain_cannot_promote_supported_partial_layer(tmp_path):
    requests = [{"operator_id": "action.insert"}, {"operator_id": "guard.dependency"}]
    plan, config, result = run_fixture(tmp_path, requests=requests)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "incomplete"
    assert audit["layers"][1]["status"] == "unsupported"
    assert not audit["mutation_receipt"]["applied"]


def test_later_swap_cannot_claim_earlier_reverted_order_hit(tmp_path):
    # Find two explicit legal layers that cancel their swap; no test mutator rerun
    # occurs at runtime. Input generation is allowed here before sealing the plan.
    for seed in range(20):
        plan, config = config_for(requests=[{"operator_id": "action.swap"}, {"operator_id": "action.swap"}], seed=seed)
        receipts = topology_input_layer_receipts(plan)
        if receipts[0]["input_before"]["actions"] == receipts[-1]["input_after"]["actions"]:
            break
    else:
        pytest.fail("fixture failed to find cancelling swaps")
    result = run_pair(config, tmp_path)
    audit = result["application_audit"]
    assert "swapped_order_overridden_by_later_layer" in audit["layers"][0]["reasons"]
    assert audit["status"] == "incomplete"


@pytest.mark.parametrize("operator", ["endpoint.count", "action.insert", "action.delete", "action.swap", "generation.action", "param.enum", "queue.depth", "param.boundary"])
def test_all_returned_events_equal_hashed_raw_after_receipt_updates(tmp_path, operator):
    _, _, result = run_fixture(tmp_path, operator)
    raw = Path(result["raw_path"]).read_bytes()
    rows = [json.loads(line) for line in raw.splitlines()]
    assert hashlib.sha256(raw).hexdigest() == result["raw_sha256"]
    assert result["events"] == rows
    # All post-emission receipt mutations are independent of the raw/event log.
    result["input_receipts"]["raw_event_ref"] = "changed-after-emit"
    result["input_receipts"]["endpoints"].append({"fixture_mutation": True})
    for key in ("precondition_receipts", "observation_receipts", "pair_facts", "delivery_facts", "guard_receipts"):
        for row in result[key]:
            row["after_emit_mutation"] = True
    result["mutation_receipt"]["hit"] = True  # deliberately fake returned value
    assert result["events"] == rows
    assert Path(result["raw_path"]).read_bytes() == raw


def test_failed_action_receipts_cannot_backmutate_events(tmp_path):
    class Broken(OfflineTopologyBackend):
        def create_endpoint(self, *args):
            raise TopologyBackendError("fixture creation failure")
    _, config = config_for("endpoint.count")
    result = run_pair(config, tmp_path, topology_backend=Broken())
    rows = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    assert result["events"] == rows
    for row in result["guard_receipts"]:
        row["details"]["fixture_mutation_after_emit"] = True
    assert result["events"] == rows


def test_imported_compile_time_presteps_are_before_mutation_without_fixture_reach(tmp_path):
    from r2f.modules.s1.topology_targets import topology_target_declaration
    target = topology_target_declaration()
    _, _, result = run_fixture(tmp_path)
    events = result["events"]
    binding = next(e for e in events if e["event"] == "input_binding")
    mutation = next(e for e in events if e["event"] == "mutation_begin")
    previous = binding["event_index"]
    state = {"environment_ok": False, "locks_held": False, "source_identity_ok": False,
             "topology_capability_admitted": True}
    for step in target.state_steps:
        before = next(e for e in events if e["event"] == "state_step_before" and e["step_id"] == step.step_id)
        after = next(e for e in events if e["event"] == "state_step_after" and e["step_id"] == step.step_id)
        assert previous < before["event_index"] < after["event_index"] < mutation["event_index"]
        assert before["action"] == after["action"] == step.action
        assert before["before"] == after["after"] == state
        assert before["requires"] == {c.condition_id: c.evaluate(state) for c in step.requires}
        assert not after["satisfied"] and after["fixture"] and not after["execution_evidence"]
        assert after["target_contract_sha256"] == canonical_hash(target.to_dict())
        previous = after["event_index"]
    assert after["evidence"]["input_binding_verified"] is True
    assert after["evidence"]["input_binding_ref"] == binding["event_index"]
    assert after["evidence"]["input_binding_raw_ref"] == {"raw_path": result["raw_path"], "event_index": binding["event_index"]}
    assert not result["canonical_preconditions_reached"]
    assert "topology_admitted" not in after["after"] and "topology_input_bound" not in after["after"]
    assert not any(e["event"] in {"environment_preflight", "isolation_locks_acquired", "topology_source_checked"} for e in events)
    assert result["events"] == [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]


@pytest.mark.parametrize("corruption", ["duplicate_raw_binding", "mutated_route_hash"])
def test_binding_failure_retains_before_after_and_aborts_before_mutation(tmp_path, corruption):
    class Corrupt(OfflineTopologyBackend):
        def prepare(self, program, config):
            receipt = super().prepare(program, config)
            if corruption == "duplicate_raw_binding":
                self._emit("input_binding", receipt={"fixture_corruption": True})
            else:
                config["topology_route_binding"]["plan_hash"] = "bad"
            return receipt
    _, config = config_for()
    result = run_pair(config, tmp_path, topology_backend=Corrupt())
    assert result["status"] == "blocked"
    assert "topology_prestep_raw_input_binding_mismatch" in result["reason"]
    assert not any(e["event"] == "mutation_begin" for e in result["events"])
    after = next(e for e in result["events"] if e["event"] == "state_step_after" and e["step_id"] == "bind_topology_input")
    assert after["status"] == "failed" and not after["satisfied"]
    assert not after["evidence"]["input_binding_verified"]
    assert "topology_input_bound" not in after["after"]
    assert result["events"] == [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]


def test_fixture_publish_command_ack_carries_its_actual_simulated_worker_identity(tmp_path):
    _, _, result = run_fixture(tmp_path)
    for ack in [e for e in result["events"] if e["event"] == "command_done" and "burst_count" in e]:
        created = next(e for e in result["events"] if e["event"] == "endpoint_created"
                       and e["logical_id"] == ack["logical_id"] and e["generation"] == ack["generation"])
        for key in ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"):
            assert ack[key] == created[key]
        assert ack["real_ros"] is False and ack["backend"] == "offline_fake"


def test_event_sink_owns_json_snapshot_not_backend_return_or_original_fields(tmp_path):
    class NestedReceipt(OfflineTopologyBackend):
        def prepare(self, program, config):
            prepared = super().prepare(program, config)
            fields = {"nested": ({"value": [1]},), "numeric_keys": {1: "one"}}
            returned = self._emit("fixture_snapshot_ownership_probe", receipt=fields)
            fields["nested"][0]["value"].append(2)
            returned["receipt"]["numeric_keys"][2] = "two"
            return prepared
    _, config = config_for()
    result = run_pair(config, tmp_path, topology_backend=NestedReceipt())
    rows = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    assert rows == result["events"]
    receipt = next(e["receipt"] for e in rows if e["event"] == "fixture_snapshot_ownership_probe")
    assert receipt == {"nested": [{"value": [1]}], "numeric_keys": {"1": "one"}}


def test_config_readiness_booleans_cannot_authenticate_fixture(tmp_path):
    _, config = config_for()
    config.update(environment_ok=True, locks_held=True, source_identity_ok=True,
                  topology_admitted=True, topology_input_bound=True)
    result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    assert not result["canonical_preconditions_reached"]
    assert not result["application_audit"]["canonical_preconditions_observed"]
    assert not any(result["mutation_receipt"].values())
    for receipt in result["canonical_precondition_receipts"]:
        assert not receipt["satisfied"] and not receipt["before"]["locks_held"]


def test_fixture_preparation_is_separate_from_canonical_environment(tmp_path):
    _, _, result = run_fixture(tmp_path)
    assert result["environment"] == {}
    assert not (tmp_path / "environment.json").exists()
    assert result["fixture_environment"] == result["backend_preflight"] == result["preparation_receipt"]
    assert result["environment_kind"] == "offline_fixture_no_canonical_manifest"
    assert result["fixture_environment"]["ros_execution"] is False
    assert result["execution_metadata"]["source_identity"] is None
    assert result["execution_metadata"]["source_manifest"] is None
    assert result["execution_metadata"]["rmw"] is None


def test_metadata_preserves_all_fixture_generations_and_matches_persisted_raw(tmp_path):
    _, config, result = run_fixture(tmp_path, "generation.action")
    metadata = result["execution_metadata"]
    creates = [e for e in result["events"] if e["event"] == "endpoint_created"]
    assert len(metadata["workers"]) == len(creates)
    assert len({(w["logical_id"], w["generation"]) for w in metadata["workers"]}) == len(creates)
    assert any(w["generation"] == 2 for w in metadata["workers"])
    for key in ("run_id", "target_id", "target_revision", "arm", "repeat"):
        assert metadata[key] == config[key]
    assert metadata["context_basis"] == "sealed_input_declaration_not_sut_observation"
    for worker in metadata["workers"]:
        token = result["events"][worker["token_raw_ref"]["event_index"]]
        for key in ("logical_id", "generation", "pid", "pgid", "process_start_ticks", "endpoint_generation", "endpoint_gid"):
            assert worker[key] == token[key]
        assert worker["fixture"] and not worker["real_ros"]
        assert worker["rmw_identifier"] is worker["domain_id"] is None
    assert metadata == json.loads((tmp_path / "execution_metadata.json").read_text())
    assert metadata == next(e["metadata"] for e in result["events"] if e["event"] == "topology_execution_metadata")
    assert not result["real_execution"] and not metadata["real_execution"]
    assert result["events"] == [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]


def test_worker_projection_keeps_actual_mixed_rmws_missing_domain_and_cleanup_unknown(tmp_path):
    # Pure supplied-artifact projection, NOT an executed ROS provenance claim.
    from r2f.runtime.topology_pair import _execution_workers
    events = []
    for ordinal, (logical, generation, rmw, domain) in enumerate([
        ("w0", 1, "rmw_cyclonedds_cpp", 191),
        ("w0", 2, "rmw_fastrtps_cpp", 191),
        ("r0", 1, None, None),
    ]):
        lineage = {"logical_id": logical, "generation": generation, "worker_generation": generation,
                   "pid": 1000 + ordinal, "pgid": 1000 + ordinal, "process_start_ticks": 11 + ordinal,
                   "endpoint_generation": "epoch-" + str(ordinal), "domain_id": 191,
                   "observed_domain_id": domain, "rmw_identifier": rmw, "backend": "ros2_public_api",
                   "real_ros": True, "worker_role": "writer" if logical == "w0" else "reader"}
        events.extend([
            {"event": "process_started", "fresh_process": True, "separate_context": True, **lineage},
            {"event": "endpoint_created", "endpoint_gid": "gid-" + str(ordinal), **lineage},
        ])
        if ordinal < 2:
            events.extend([
                {"event": "process_exit", "returncode": 0, **lineage},
                {"event": "worker_exit", "returncode": 0, **lineage},
                {"event": "topology_endpoint_destroyed", "cleanup_status": "closed", **lineage},
            ])
    # Exact backend identifier is the runtime's constant, not a test guess.
    from r2f.runtime.topology_worker import ROS_BACKEND
    for i, e in enumerate(events):
        e.update(event_index=i, backend=ROS_BACKEND)
    workers = _execution_workers(events, tmp_path / "raw.jsonl")
    assert [(w["logical_id"], w["generation"]) for w in workers] == [("w0", 1), ("w0", 2), ("r0", 1)]
    assert [w["rmw_identifier"] for w in workers] == ["rmw_cyclonedds_cpp", "rmw_fastrtps_cpp", None]
    assert workers[-1]["domain_id"] is None  # envelope/config domain not an observation
    assert workers[-1]["closed"] is False and workers[-1]["cleanup_status"] == "unknown"
    assert "actual_domain_unknown" in workers[-1]["unknowns"]
    assert "actual_rmw_unknown" in workers[-1]["unknowns"]
    assert all(workers[i]["exit_raw_refs"] and workers[i]["cleanup_raw_refs"] for i in (0, 1))
    assert len({w["token_raw_ref"]["event_index"] for w in workers}) == 3


def test_real_process_close_is_idempotent_and_emits_only_one_exit_without_spawning():
    from r2f.runtime.topology_worker import _EndpointProcess
    emitted = []
    process = _EndpointProcess({}, {}, "/unused", "writer", lambda event, **fields: emitted.append((event, fields)))
    process.process = Mock(pid=999)
    process.process.poll.return_value = 0
    process.process.returncode = 0
    process.metadata = {"role": "writer", "pid": 999, "pgid": 999, "logical_id": "w0", "generation": 1}
    process.pump = Mock()
    process.stderr = Mock()
    assert process.close()["closed"]
    assert process.close()["already"]
    assert [event for event, fields in emitted] == ["process_exit"]


def test_every_action_has_one_actual_terminal_before_done_including_snapshots(tmp_path):
    plan, _, result = run_fixture(tmp_path)
    events = result["events"]
    for action in plan.final_config["topology_program"]["actions"]:
        aid = action["action_id"]
        begin = next(e for e in events if e["event"] == "topology_action_begin" and e["action_id"] == aid)
        ends = [e for e in events if e["event"] == "state_step_after" and e["step_id"] == "action:" + aid]
        done = next(e for e in events if e["event"] == "topology_action_done" and e["action_id"] == aid)
        assert len(ends) == 1
        after = ends[0]
        assert begin["event_index"] < after["event_index"] < done["event_index"]
        assert after["before"] == begin["before"] == done["receipt"]["before"]
        assert after["action"] == done["receipt"]["action"] == action
        assert after["after"] == done["receipt"]["after"]
        if action["op"] == "snapshot":
            obs = next(e for e in events if e["event"] == "topology_pair_observation" and e["step_id"] == "snapshot:" + aid)
            assert begin["event_index"] < obs["event_index"] < after["event_index"]
            assert after["after"]["complete"] is True
    assert result["events"] == [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]


def test_guard_failure_emits_terminal_without_fabricating_success(tmp_path):
    class Broken(OfflineTopologyBackend):
        def create_endpoint(self, *args):
            raise TopologyBackendError("injected fixture failure")
    _, config = config_for()
    result = run_pair(config, tmp_path, topology_backend=Broken())
    for guard in result["guard_receipts"]:
        after = next(e for e in result["events"] if e["event"] == "state_step_after" and e["step_id"] == guard["step_id"])
        done = next(e for e in result["events"] if e["event"] == "topology_action_done" and "action:" + e["action_id"] == guard["step_id"])
        assert after["satisfied"] is False and after["execution_evidence"] is False
        assert after["disposition"] == guard["disposition"]
        assert after["event_index"] < done["event_index"]
    assert not any(result["mutation_receipt"].values())


def test_raw_boundaries_independently_reconcile_pair_sentinel_closure_not_network(tmp_path):
    plan, config, delivered = run_fixture(tmp_path / "delivered")
    with patch("r2f.runtime.topology_worker.RosTopologyBackend.prepare", side_effect=AssertionError("NO ROS")):
        inflight = run_pair(config, tmp_path / "inflight",
                            topology_backend=OfflineTopologyBackend(auto_delivery=False))
    roles = {e["logical_id"]: e["role"] for e in plan.final_config["topology_program"]["endpoints"]}
    for result, expected_complete in ((delivered, True), (inflight, False)):
        # Read persisted bytes and reconstruct evidence without the producer's
        # closure helper or its diagnostic pair/count/delivery summaries.
        events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
        assert result["events"] == events
        boundaries = [e for e in events if e["event"] == "topology_state_boundary"]
        assert [e["phase"] for e in boundaries] == ["before_program", "after_program"]
        before, after = boundaries
        assert before["lifecycle_complete"] is True and before["observation_complete"] is False
        assert before["window_complete"] is False and before["delivery_window_refs"] == []
        assert after["lifecycle_complete"] is True and after["observation_complete"] is True
        actions = [e for e in events if e["event"] == "topology_action_begin"]
        done = [e for e in events if e["event"] == "topology_action_done"]
        cleanup = next(e for e in events if e["event"] == "topology_cleanup")
        assert before["event_index"] < actions[0]["event_index"] < done[-1]["event_index"] < after["event_index"] < cleanup["event_index"]
        assert all("network_phase_not_measured" in e["limitations"] for e in boundaries)
        assert not any(e["event"] in {"topology_network_phase", "topology_network_observation"} for e in events)
        assert after["snapshot_refs"] and all(i < after["event_index"] for i in after["snapshot_refs"])
        assert result["fixture"] and not result["real_execution"]
        assert not any(result[k] for k in ("candidate_eligible", "sealed", "mature"))
        live = {}
        for row in events[:after["event_index"]]:
            if row["event"] == "endpoint_created":
                live[row["logical_id"]] = row
            elif row["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
                assert live[row["logical_id"]]["generation"] == row["generation"]
                live.pop(row["logical_id"])
        pair_keys = ("writer", "reader", "writer_generation", "reader_generation")
        current = {(w["logical_id"], r["logical_id"], w["generation"], r["generation"])
                   for w in live.values() if roles[w["logical_id"]] == "writer"
                   for r in live.values() if roles[r["logical_id"]] == "reader"}
        closes = [e for e in events[:after["event_index"]] if e["event"] == "topology_delivery_window_closed"]
        assert after["delivery_window_refs"] == [e["event_index"] for e in closes]
        reconciled = {}
        for close in closes:
            opened = events[close["open_ref"]]
            key = tuple(close[k] for k in pair_keys)
            assert opened["event"] == "topology_delivery_window_open"
            assert tuple(opened[k] for k in pair_keys) == key
            assert opened["event_index"] < close["event_index"] < after["event_index"]
            assert opened["fixture"] and close["fixture"]
            assert not opened["candidate_eligible"] and not close["candidate_eligible"]
            for role in ("writer", "reader"):
                token = live[close[role]]
                assert token["generation"] == close[role + "_generation"]
                chain = [events[i] for i in opened["snapshot_refs"][token["logical_id"]]]
                assert [e["event"] for e in chain] == ["topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"]
                assert len({e["command_id"] for e in chain}) == 1
                for e in chain:
                    assert all(e[k] == token[k] for k in ("run_id", "logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"))
                    assert token["event_index"] < e["event_index"] < opened["event_index"]
                    assert token["monotonic_ns"] <= e["monotonic_ns"] <= opened["monotonic_ns"]
                assert all(a["event_index"] < b["event_index"] and a["monotonic_ns"] <= b["monotonic_ns"] for a, b in zip(chain, chain[1:]))
            window = events[opened["event_index"] + 1:close["event_index"]]
            sends = [e for e in window if e["event"] == "send" and e["logical_id"] == close["writer"]]
            receives = [e for e in window if e["event"] == "receive" and e["logical_id"] == close["reader"]]
            expected = {e["send_sequence"]: e for e in sends}
            assert len(expected) == len(sends) and sends
            assert close["send_refs"] == [e["event_index"] for e in sends]
            assert close["receive_refs"] == [e["event_index"] for e in receives]
            for row, role, gid_field in [(e, "writer", "writer_gid") for e in sends] + [(e, "reader", "reader_gid") for e in receives]:
                token = live[close[role]]
                assert all(row[k] == token[k] for k in ("run_id", "logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"))
                assert row[gid_field] == token["endpoint_gid"]
                assert opened["monotonic_ns"] <= row["monotonic_ns"] <= close["monotonic_ns"]
            for send in sends:
                assert send["publish_returned"] is True
                assert len([e for e in events[:close["event_index"]] if e["event"] == "send" and e.get("logical_id") == close["writer"] and e.get("generation") == close["writer_generation"] and e.get("send_sequence") == send["send_sequence"]]) == 1
            for sample in receives:
                send = expected[sample["send_sequence"]]
                assert sample["publisher_gid"] == send["writer_gid"]
                assert sample["payload_sha256"] == send["payload_sha256"]
                assert sample["payload_writer_id"] == send["logical_id"]
                assert sample["payload_writer_generation"] == send["generation"]
                assert len([e for e in events[:close["event_index"]] if e["event"] == "send" and e.get("payload_sha256") == sample["payload_sha256"] and e["run_id"] == sample["payload_run_id"]]) == 1
                # No assertion compares sample raw index to its send raw index.
            received = {e["send_sequence"] for e in receives}
            missing = set(expected) - received
            sentinel = max(expected)
            closed = sentinel in received and all(seq < sentinel and expected[seq]["event_index"] < expected[sentinel]["event_index"] for seq in missing)
            assert close["window_complete"] is closed
            assert close["expected_sequences"] == sorted(expected)
            assert close["received_sequences"] == sorted(received)
            assert close["missing_sequences"] == sorted(missing)
            if closed:
                assert close["sentinel_sequence"] == sentinel
                assert close["sentinel_send_ref"] == expected[sentinel]["event_index"]
                sentinel_receive = events[close["sentinel_receive_ref"]]
                assert sentinel_receive in receives and sentinel_receive["send_sequence"] == sentinel
                assert close["gap_evidence"] == [{"sequence": seq, "send_ref": expected[seq]["event_index"], "sentinel_send_ref": close["sentinel_send_ref"], "sentinel_receive_ref": close["sentinel_receive_ref"]} for seq in sorted(missing)]
            else:
                assert "sentinel_sequence" not in close and close["gap_evidence"] == []
            reconciled[key] = closed
        computed = bool(current) and current <= reconciled.keys() and all(reconciled.values())
        assert after["window_complete"] is computed is expected_complete


def test_destroy_carries_same_generation_identity_and_closed_cleanup(tmp_path):
    # Explicit destroy action: same generation identity in the retirement row,
    # closed cleanup, and explicit offline-fixture provenance.
    prog = model.generate_program(7, writers=1, readers=1)
    index = next(i for i, a in enumerate(prog["actions"]) if a["action_id"].startswith("create_w0"))
    prog["actions"].insert(index + 1, {"action_id": "destroy_w0_g1", "op": "destroy",
                                       "endpoint_id": "w0", "generation": 1,
                                       "params": {}, "depends_on": []})
    assert model.validate_program(prog)["status"] == "valid"
    result = run_topology(prog, {"backend": "offline_fake", "run_id": "destroy-identity",
                                 "domain_id": 191}, tmp_path,
                          backend=OfflineTopologyBackend())
    assert result["status"] == "executed", result["reason"]
    events = result["events"]
    token = next(e for e in events if e["event"] == "endpoint_created"
                 and e["logical_id"] == "w0" and e["generation"] == 1)
    destroyed = [e for e in events
                 if e["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                 and e["logical_id"] == "w0" and e["generation"] == 1]
    assert len(destroyed) == 1
    retired = destroyed[0]
    assert retired["cleanup_status"] == "closed"
    assert retired["fixture"] is True and retired["real_ros"] is False
    assert retired["cleanup_scope"] == "offline_simulated_endpoint"
    for key in ("endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation"):
        assert retired[key] == token[key]
    assert token["event_index"] < retired["event_index"]
    receipt = next(r for r in result["precondition_receipts"]
                   if r["step_id"] == "action:destroy_w0_g1")
    assert receipt["disposition"] == "applied" and receipt["satisfied"] is True
    assert receipt["after"]["alive"] is False and receipt["after"]["generation"] == 1
    assert receipt["cleanup"]["cleanup_status"] == "closed"
    assert receipt["cleanup"]["fixture"] is True
    for key in ("endpoint_gid", "pid", "process_start_ticks", "endpoint_generation"):
        assert receipt["cleanup"][key] == token[key]


def _destroy_program():
    program = model.generate_program(7, writers=1, readers=1)
    index = next(i for i, a in enumerate(program["actions"])
                 if a["action_id"].startswith("create_w0"))
    program["actions"].insert(index + 1, {"action_id": "destroy_w0_g1", "op": "destroy",
                                          "endpoint_id": "w0", "generation": 1,
                                          "params": {}, "depends_on": []})
    assert model.validate_program(program)["status"] == "valid"
    return program


def test_destroy_audit_requires_same_identity_and_closed_cleanup(tmp_path):
    """Direct contract check of the destroy branch: identity drift fails closed.

    Uses the real runtime raw and the auditor's own per-action proof, without a
    mutation plan (a destroy plan would legitimately leave later guards failed).
    """
    from r2f.runtime.topology_route import _action_proof
    program = _destroy_program()
    result = run_topology(program, {"backend": "offline_fake",
                                    "run_id": "destroy-audit", "domain_id": 191},
                          tmp_path, backend=OfflineTopologyBackend())
    assert result["status"] == "executed", result["reason"]
    events = result["events"]
    action = next(a for a in program["actions"] if a["op"] == "destroy")
    refs, reason = _action_proof(action, events)
    assert reason is None and refs

    drifted = deepcopy(events)
    next(e for e in drifted if e["event"] in {"endpoint_destroyed",
                                              "topology_endpoint_destroyed"})["pid"] += 1
    refs, reason = _action_proof(action, drifted)
    assert refs == [] and reason == "retirement_identity_or_cleanup_mismatch"

    not_closed = deepcopy(events)
    next(e for e in not_closed if e["event"] in {"endpoint_destroyed",
                                                 "topology_endpoint_destroyed"})["cleanup_status"] = "unknown"
    refs, reason = _action_proof(action, not_closed)
    assert refs == [] and reason == "endpoint_destroy_cleanup_missing_or_ambiguous"


def test_recreate_predecessor_retirement_drift_is_rejected(tmp_path):
    plan, config, result = run_fixture(tmp_path, "generation.action")
    assert audit_topology_application(
        plan, config["input_layer_receipts"], result)["status"] == "fixture_application_observed"

    def drift(rows):
        next(e for e in rows if e["event"] in {"endpoint_destroyed",
                                               "topology_endpoint_destroyed"})["endpoint_generation"] = "rewritten"
        return rows
    rewrite_raw(result, drift)
    audit = audit_topology_application(plan, config["input_layer_receipts"], result)
    assert audit["status"] == "incomplete"
    assert any("fresh_generation_or_predecessor_retirement_not_proven" in r
               for layer in audit["layers"] for r in layer["reasons"])


def test_topology_state_boundaries_and_snapshot_terminal_are_raw_backed(tmp_path):
    plan, _, result = run_fixture(tmp_path)
    events = result["events"]
    boundaries = [e for e in events if e["event"] == "topology_state_boundary"]
    assert [e["phase"] for e in boundaries] == ["before_program", "after_program"]
    snapshot_actions = [a for a in plan.final_config["topology_program"]["actions"]
                        if a["op"] == "snapshot"]
    assert snapshot_actions
    for action in snapshot_actions:
        step = "action:" + action["action_id"]
        before = [e for e in events if e["event"] == "state_step_before" and e["step_id"] == step]
        after = [e for e in events if e["event"] == "state_step_after" and e["step_id"] == step]
        assert len(before) == 1 and len(after) == 1
        assert before[0]["event_index"] < after[0]["event_index"]
        assert after[0]["satisfied"] is True
        assert after[0]["before"] == before[0]["before"]


def test_snapshot_complete_boolean_without_raw_ack_does_not_complete_boundary(tmp_path):
    class MissingSnapshotAck(OfflineTopologyBackend):
        def _emit(self, event, **fields):
            if event == "command_done" and "burst_count" not in fields:
                return {"event": event, **fields}  # omit simulated worker ACK
            return super()._emit(event, **fields)
    _, config = config_for()
    result = run_pair(config, tmp_path, topology_backend=MissingSnapshotAck())
    after = next(e for e in result["events"] if e["event"] == "topology_state_boundary" and e["phase"] == "after_program")
    assert after["observation_complete"] is False
    assert any(reason.endswith(":command_done") for reason in after["limitations"])
    assert not after["window_complete"]
    assert not result["real_execution"] and not any(result["mutation_receipt"].values())

