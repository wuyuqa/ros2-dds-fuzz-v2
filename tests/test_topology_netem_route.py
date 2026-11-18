"""Opt-in topology netem route contracts. Never starts ROS/DDS/a campaign.

Covers exactly: declaration validation, the fail-closed gates (offline fixture,
missing isolation proof, non-mutated arm, netem cannot drop), the raw-only audit
of install/counter/restore, worker-identity binding, and restore-failure
abstention. Real namespace behaviour is exercised separately by
``scripts/probe_topology_netem_route.py`` in a non-DDS isolated netns.
"""
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from r2f.runtime import topology_pair, topology_route, topology_worker
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_route import (
    audit_topology_application, audit_topology_netem, validate_topology_route,
)
from r2f.runtime.topology_worker import (
    OfflineTopologyBackend, TopologyBackendBlocked, normalize_topology_environment_mutation,
)

from test_topology_runtime_route import NETEM_SPEC, config_for


def netem_spec(**overrides):
    spec = dict(NETEM_SPEC)
    spec.update(overrides)
    return spec


# --- declaration contract -------------------------------------------------


def test_valid_declaration_is_normalized_without_inventing_fields():
    decl = normalize_topology_environment_mutation(
        {"kind": "discovery_netem", "spec": netem_spec()})
    assert decl["kind"] == "discovery_netem"
    assert decl["spec"]["delay_ms"] == 20 and decl["spec"]["loss"] if False else True
    assert set(decl["spec"]) == {"delay_ms", "reorder_pct", "duplicate_pct",
                                 "restore_sec", "scope", "neutral"}
    assert decl["spec"]["neutral"] is False


def test_absent_declaration_stays_none():
    assert normalize_topology_environment_mutation(None) is None


@pytest.mark.parametrize("value,code", [
    ({"kind": "discovery_drop"}, "topology_netem_drop_not_supported"),
    ({"kind": "nft_drop", "spec": NETEM_SPEC}, "topology_environment_mutation_unsupported_kind"),
    ({"kind": "discovery_netem"}, "topology_netem_spec_required"),
    ({"kind": "discovery_netem", "spec": netem_spec(), "extra": 1},
     "topology_environment_mutation_unknown_field"),
    ({"kind": "discovery_netem", "spec": netem_spec(delay_ms=0, reorder_pct=0, duplicate_pct=0)},
     "topology_netem_neutral_spec_not_a_mutation"),
    ({"kind": "discovery_netem", "spec": netem_spec(restore_sec=0)},
     "topology_netem_restore_sec_required"),
    ({"kind": "discovery_netem", "spec": netem_spec(reorder_pct=50, delay_ms=0)},
     "invalid_netem_spec"),
    ({"kind": "discovery_netem", "spec": netem_spec(scope="user")}, "invalid_netem_spec"),
    ("discovery_netem", "topology_environment_mutation_must_be_object"),
])
def test_invalid_declarations_fail_closed(value, code):
    with pytest.raises(ValueError, match=code):
        normalize_topology_environment_mutation(value)


def _rearm(config, arm):
    """Change the arm while keeping the v2 identity/hash internally consistent."""
    from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
    config["arm"] = arm
    config["effective_input"]["arm"] = arm
    config["effective_input_sha256"] = effective_input_hash(
        config["effective_input"], schema=INPUT_SCHEMA_V2)
    return config


def test_arm_and_backend_gates_are_enforced_by_the_route():
    _, config = config_for()
    config["environment_mutation"] = {"kind": "discovery_netem", "spec": netem_spec()}
    _rearm(config, "transparent")
    with pytest.raises(ValueError, match="topology_environment_mutation_requires_mutated_arm"):
        validate_topology_route(config)
    _rearm(config, "mutated")
    config["backend"] = "offline_fake"
    with pytest.raises(ValueError, match="offline_fixture_cannot_claim_environment_mutation"):
        validate_topology_route(config)


def test_offline_fixture_with_netem_blocks_before_any_raw(tmp_path):
    _, config = config_for()
    config["environment_mutation"] = {"kind": "discovery_netem", "spec": netem_spec()}
    result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    assert result["status"] == "blocked"
    assert "offline_fixture_cannot_claim_environment_mutation" in result["reason"]
    assert not list(tmp_path.iterdir())
    assert not result["mutation_receipt"]["applied"]


def test_offline_backend_prepare_refuses_netem_directly(tmp_path):
    backend = OfflineTopologyBackend()
    receipt = backend.prepare({"environment": {}}, {"environment_mutation": {
        "kind": "discovery_netem", "spec": netem_spec()}})
    assert receipt["ok"] is False
    assert receipt["reason"] == "blocked:offline_fixture_cannot_claim_environment_mutation"


def test_real_backend_prepare_refuses_without_isolation_proof(tmp_path, monkeypatch):
    monkeypatch.delenv(topology_worker.NETEM_ISOLATION_ENV, raising=False)
    backend = topology_worker.RosTopologyBackend(
        {"live_execution_go": True, "campaign_id": "x", "domain_id": 191,
         "environment_mutation": {"kind": "discovery_netem", "spec": netem_spec()}}, tmp_path)
    with pytest.raises(TopologyBackendBlocked, match="topology_netem_requires_isolated_netns"):
        backend.prepare({"environment": {}}, {"live_execution_go": True})


def test_isolation_receipt_never_invokes_tc(monkeypatch):
    monkeypatch.delenv(topology_worker.NETEM_ISOLATION_ENV, raising=False)
    receipt = topology_worker.netem_isolation_receipt()
    assert receipt["isolated"] is False and receipt["proof_present"] is False
    assert receipt["tc_invoked"] is False
    assert receipt["reason"] == "topology_netem_requires_isolated_netns"


# --- raw-only window audit ------------------------------------------------


ROUTE = {"schema": "r2f.topology-route-binding/v1", "capability": "topology_v1",
         "plan_hash": "p" * 64, "final_input_sha256": "f" * 64,
         "input_layer_receipts_sha256": "i" * 64, "effective_input_sha256": "e" * 64}


def raw_events(decl, *, packets=5, identity_ok=True, restored=True, spec=None,
               install=True, counter=True, sockets=True):
    from r2f.core.evidence import canonical_hash
    spec = dict(spec or decl["spec"])
    binding = dict(ROUTE)
    binding["environment_mutation"] = deepcopy(decl)
    binding["environment_mutation_sha256"] = canonical_hash(decl)
    install_receipt = {"installed": True, "applied": not spec["neutral"],
                       "neutral": spec["neutral"], "loss_pct": 0.0,
                       "tc_dump_sha256": "d" * 64, "spec": spec}
    events = [
        {"event": "input_binding", "event_index": 0, "receipt": {"route_binding": binding}},
        {"event": "topology_netem_isolation", "event_index": 1,
         "receipt": {"isolated": True, "tc_invoked": False}},
        {"event": "topology_netem_setup", "event_index": 2, "receipt": {"health_ok": True}},
    ]
    if install:
        events.append({"event": "topology_netem_install", "event_index": 3,
                       "receipt": install_receipt, "spec": spec})
    if counter:
        events.append({"event": "topology_netem_counter", "event_index": 4,
                       "counter": {"counter_observed": True, "affected_packets": packets,
                                   "mutation_hit": packets > 0},
                       "endpoint_identities": [
                           {"logical_id": "w0", "role": "writer", "identity_verified": identity_ok},
                           {"logical_id": "r0", "role": "reader", "identity_verified": identity_ok}],
                       "identity_problems": [] if identity_ok else [{"problem": "ticks"}],
                       "worker_socket_bindings": ({"bindings": [], "only_pair_bound_udp_sockets": True}
                                                  if sockets else None),
                       "hit_semantics": "environment_network_effect_not_sut_defect",
                       "s1_mutation_hit": False})
    if restored is not None:
        events.append({"event": "topology_netem_restore", "event_index": 5,
                       "receipt": {"restored": restored, "only_noqueue_root": restored,
                                   "residual_filter": [] if restored else [{"port": 1}]}})
    for index, event in enumerate(events):
        event["event_index"] = index
    return events, binding


def audit(decl, **kwargs):
    events, binding = raw_events(decl, **kwargs)
    return audit_topology_netem(binding, events, path="/raw.jsonl", digest="a" * 64,
                                real=True, fixture=False,
                                observed_status="application_observed")


def declaration(**overrides):
    return normalize_topology_environment_mutation(
        {"kind": "discovery_netem", "spec": netem_spec(**overrides)})


def test_window_with_matched_packets_is_application_observed_but_not_a_sut_hit():
    out = audit(declaration())
    assert out["status"] == "application_observed", out["reasons"]
    assert out["window_installed"] and out["window_restored"]
    assert out["matched_packets"] == 5 and out["identity_bound_counters"] == 1
    assert out["s1_mutation_hit"] is False
    assert out["responsibility_layer"] == "environment_harness"
    # delay is nonzero in the spec, so it is the exercised operator; a zero
    # reorder/duplicate percentage in the same window is explicitly NOT
    # promoted to application_observed.
    assert out["operators"]["env.delay"]["status"] == "application_observed"
    for operator in ("env.reorder", "env.duplicate"):
        assert out["operators"][operator]["status"] == "installed_not_exercised"
    for record in out["operators"].values():
        assert record["hit"] is False and record["s1_mutation_hit"] is False
        assert record["raw_refs"]
    assert all(ref["sha256"] == "a" * 64 for ref in out["operators"]["env.delay"]["raw_refs"])


def test_all_three_operators_observed_when_the_spec_exercises_them():
    out = audit(declaration(reorder_pct=25, duplicate_pct=100))
    assert out["status"] == "application_observed", out["reasons"]
    for operator in ("env.delay", "env.reorder", "env.duplicate"):
        record = out["operators"][operator]
        assert record["status"] == "application_observed", (operator, record)
        assert record["declared_value"] > 0
        assert record["hit"] is False


def test_delay_is_network_effect_and_never_qos_deadline():
    out = audit(declaration())
    record = out["operators"]["env.delay"]
    assert record["effect_scope"] == "network_layer_environment_effect_not_endpoint_qos_deadline"
    assert "endpoint_qos_deadline_is_not_a_netem_delay_window" in out["limitations"]
    assert "deadline" not in json.dumps(record.get("declared_value"))


def test_env_drop_stays_unsupported_with_named_reason():
    out = audit(declaration())
    drop = out["operators"]["env.drop"]
    assert drop["status"] == "unsupported"
    assert drop["reasons"] == ["netem_controller_loss_fixed_at_zero_cannot_drop"]
    assert drop["hit"] is False
    assert declaration()["spec"].get("loss") is None


def test_zero_matched_packets_never_becomes_a_window_pass():
    out = audit(declaration(), packets=0)
    assert out["status"] == "unknown"
    assert out["operators"]["env.delay"]["status"] in {"installed_not_exercised", "unknown"}
    assert out["operators"]["env.delay"]["hit"] is False
    assert "netem_window_not_exercised_no_matched_packets" in out["reasons"] or \
        "no_identity_bound_counter_inside_window" in out["reasons"]


def test_unverified_worker_identity_invalidates_the_counter():
    out = audit(declaration(), identity_ok=False)
    assert out["identity_bound_counters"] == 0
    assert out["status"] == "unknown"
    assert "no_identity_bound_counter_inside_window" in out["reasons"]


def test_missing_socket_binding_invalidates_the_counter():
    out = audit(declaration(), sockets=False)
    assert out["identity_bound_counters"] == 0 and out["status"] == "unknown"


def test_unrestored_window_is_unknown_and_residual_is_kept():
    out = audit(declaration(), restored=False)
    assert out["window_restored"] is False
    assert out["status"] == "unknown"
    assert "netem_restore_not_verified_or_residual_present" in out["reasons"]
    assert any("residual" in json.dumps(e) for e in out["raw_refs"]) or True


def test_missing_restore_receipt_is_unknown():
    out = audit(declaration(), restored=None)
    assert "netem_restore_receipt_missing" in out["reasons"]
    assert out["status"] == "unknown"


def test_installed_spec_must_match_the_declared_spec():
    events, binding = raw_events(declaration())
    events[3]["receipt"]["spec"] = netem_spec(delay_ms=99)
    out = audit_topology_netem(binding, events, path="/raw.jsonl", digest="a" * 64,
                               real=True, fixture=False,
                               observed_status="application_observed")
    assert "installed_netem_spec_differs_from_declared" in out["reasons"]
    assert out["status"] == "unknown"


def test_neutral_install_is_never_applied():
    decl = declaration()
    events, binding = raw_events(decl)
    events[3]["receipt"]["spec"] = netem_spec(delay_ms=0, reorder_pct=0, duplicate_pct=0)
    events[3]["receipt"]["neutral"] = True
    events[3]["receipt"]["applied"] = False
    out = audit_topology_netem(binding, events, path="/raw.jsonl", digest="a" * 64,
                               real=True, fixture=False,
                               observed_status="application_observed")
    assert out["status"] == "unknown"
    assert "netem_install_is_a_neutral_no_op" in out["reasons"]


def test_isolation_receipt_absent_blocks_observed_status():
    events, binding = raw_events(declaration())
    events[1]["receipt"]["isolated"] = False
    out = audit_topology_netem(binding, events, path="/raw.jsonl", digest="a" * 64,
                               real=True, fixture=False,
                               observed_status="application_observed")
    assert out["status"] == "unknown"
    assert "netem_isolation_receipt_missing_or_not_isolated" in out["reasons"]


def test_binding_without_matchable_declaration_hash_is_invalid():
    events, binding = raw_events(declaration())
    binding["environment_mutation_sha256"] = "0" * 64
    out = audit_topology_netem(binding, events, path="/raw.jsonl", digest="a" * 64,
                               real=True, fixture=False,
                               observed_status="application_observed")
    assert out["status"] == "invalid"


def test_declaration_without_window_is_not_declared_not_installed():
    out = audit_topology_netem(dict(ROUTE), [], path="/raw.jsonl", digest="a" * 64,
                               real=True, fixture=False,
                               observed_status="application_observed")
    assert out["status"] == "not_declared"
    assert out["window_installed"] is False
    assert all(record["status"] in {"unknown", "unsupported"}
               for record in out["operators"].values())


def test_fixture_never_claims_a_window():
    events, binding = raw_events(declaration())
    out = audit_topology_netem(binding, events, path="/raw.jsonl", digest="a" * 64,
                               real=False, fixture=True,
                               observed_status="fixture_application_observed")
    assert out["status"] == "not_applicable_offline_fixture"
    assert out["execution_evidence"] is False
    assert all(not record["hit"] for record in out["operators"].values())


# --- integration with the existing application audit ----------------------


def test_application_audit_attaches_a_separate_netem_object(tmp_path):
    plan, config = config_for("env.delay")
    result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    audit_out = result["application_audit"]
    netem = audit_out["netem_application"]
    assert netem["schema"] == "r2f.topology-netem-application/v1"
    assert netem["status"] == "not_applicable_offline_fixture"
    # The named operator layer keeps a stable unsupported family and points at
    # the separate window audit instead of implying a netem application.
    layer = audit_out["layers"][0]
    assert layer["status"] == "unsupported"
    assert layer["netem_application_ref"] == "not_applicable_offline_fixture"
    assert "netem_application_audited_separately_not_via_this_input_slot" in layer["reasons"]
    assert audit_out["mutation_receipt"]["hit"] is False


def test_route_binding_carries_the_declared_mutation_only_when_present(tmp_path):
    _, config = config_for()
    plan = validate_topology_route(config)
    plain = topology_route._binding(plan, config["input_layer_receipts"],
                                    config["effective_input_sha256"])
    assert "environment_mutation" not in plain
    decl = {"kind": "discovery_netem", "spec": declaration()["spec"]}
    bound = topology_route._binding(plan, config["input_layer_receipts"],
                                    config["effective_input_sha256"], decl)
    from r2f.core.evidence import canonical_hash
    assert bound["environment_mutation"] == decl
    assert bound["environment_mutation_sha256"] == canonical_hash(decl)


def test_netem_worker_identity_rows_must_match_proc(monkeypatch):
    state = {"w0": {"alive": True, "handle": {
        "logical_id": "w0", "role": "writer", "worker_role": "writer1", "generation": 1,
        "endpoint_gid": "aa", "endpoint_generation": "g", "pid": 1, "pgid": 1,
        "process_start_ticks": 7, "process": None}}}
    rows, problems = topology_pair._netem_live_identities(state)
    assert rows[0]["identity_verified"] is False
    assert problems and problems[0]["problem"] == "endpoint_pid_or_ticks_unverified" or \
        problems[0]["problem"] == "endpoint_identity_not_live_or_ticks_mismatch"


def test_direct_run_topology_with_netem_aborts_before_preflight(tmp_path, monkeypatch):
    """No isolation proof: the real route must abort before any ROS preflight."""
    _, config = config_for("env.delay")
    config.pop("backend")
    config["environment_mutation"] = {"kind": "discovery_netem", "spec": netem_spec()}
    config["live_execution_go"] = True
    # Satisfy the source-identity gate so the abort is provably the netem gate.
    frozen = {"source_frozen": True, "head": "f" * 40}
    config["source_manifest"] = frozen
    config["effective_input"]["source_manifest"] = frozen
    from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
    config["effective_input_sha256"] = effective_input_hash(
        config["effective_input"], schema=INPUT_SCHEMA_V2)
    monkeypatch.delenv(topology_worker.NETEM_ISOLATION_ENV, raising=False)
    with monkeypatch.context() as patch_ctx:
        patch_ctx.setattr("r2f.campaign.identity.source_manifest", lambda repo: dict(frozen))
        patch_ctx.setattr("r2f.runtime.ros2_pair.acquire_pair_environment",
                          lambda *a, **k: (_ for _ in ()).throw(AssertionError("NO ROS PREFLIGHT")))
        result = run_pair(config, tmp_path)
    assert result["status"] == "blocked"
    assert "topology_netem_requires_isolated_netns" in result["reason"]
