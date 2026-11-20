"""Explicit topology opt-in into the canonical public-API runtime.

No registry, runner, mutation generator, candidate lifecycle or seal lives here.
Application audit reads hashed raw bytes, never returned facts/status. A fixture
can prove this contract's plumbing but cannot acquire real-execution eligibility.
"""
from copy import deepcopy
import hashlib
import json
import os
import tempfile
from pathlib import Path

from r2f.core.evidence import INPUT_SCHEMA_V2, IDENTITY_V2_KEYS, canonical_hash, effective_input_hash
from r2f.core.mutation_plan import MutationPlan
from r2f.modules.s1.topology_input import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, TOPOLOGY_MUTATION_KIND, TOPOLOGY_CARRIER,
    validate_topology_plan, topology_input_layer_receipts,
)
from .topology_worker import OFFLINE_BACKEND, ROS_BACKEND, OfflineTopologyBackend, RosTopologyBackend
from .topology_operator_application import (
    audit_operator_application, is_deadline_configuration_delay,
)

AUDIT_SCHEMA = "r2f.topology-application-audit/v1"
ROUTE_SCHEMA = "r2f.topology-route-binding/v1"
RECEIPT_KEYS = ("planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed")
STRUCTURE_OPERATORS = {"endpoint.count", "action.insert", "action.delete", "action.swap", "generation.action"}
CONFIG_OPERATORS = {"param.enum", "queue.depth", "param.boundary"}
# These three newly integrated operators attest configuration only in this card.
# The legacy three retain their existing intervention hit/receipt contract.
CONFIGURATION_AUDIT_OPERATORS = {"relation.pair", "timing.shift", "lease.value", "filter.expr"}
SUPPORTED_OPERATORS = STRUCTURE_OPERATORS | CONFIG_OPERATORS | CONFIGURATION_AUDIT_OPERATORS
NETEM_AUDIT_SCHEMA = "r2f.topology-netem-application/v1"
OBSERVED_STATUSES = {"application_observed", "fixture_application_observed",
                     "artifact_application_observed"}
#: Operators whose name suggests a network window but whose topology input slot
#: is a QoS/parameter value.  They are audited by their own slot contract and
#: never promoted from a netem window; the window has a separate audit object.
NETEM_NAMED_OPERATORS = ("env.delay", "env.reorder", "env.duplicate", "env.drop")
NETEM_SUPPORTED_OPERATORS = ("env.delay", "env.reorder", "env.duplicate")
NETEM_PARAMETER = {"env.delay": "delay_ms", "env.reorder": "reorder_pct",
                   "env.duplicate": "duplicate_pct"}


def topology_route_requested(config):
    return (isinstance(config, dict) and (
        config.get("target_id") == TOPOLOGY_TARGET
        or config.get("runtime_capability") == TOPOLOGY_CAPABILITY
        or config.get("mutation_kind") == TOPOLOGY_MUTATION_KIND))


def _effective(config):
    if "effective_input" in config:
        return deepcopy(config["effective_input"])
    # The ordinary carrier already transports a serialized Scenario. Rebuild
    # its identity without re-running any mutation algorithm.
    if "scenario" in config:
        from r2f.core.scenario import Scenario
        scenario = Scenario.from_dict(config["scenario"])
        return scenario.effective_input_v2(
            final_input=config["final_input"],
            input_layer_receipts=config["input_layer_receipts"],
            source_manifest=config.get("source_manifest", {}))
    return {key: deepcopy(config[key]) for key in IDENTITY_V2_KEYS if key in config}


def validate_topology_route(config):
    """Return the supplied core Plan after exact opt-in and v2 binding checks."""
    if not isinstance(config, dict):
        raise ValueError("topology_config_must_be_object")
    if config.get("target_id") != TOPOLOGY_TARGET:
        raise ValueError("unknown_topology_target")
    if config.get("runtime_capability") != TOPOLOGY_CAPABILITY:
        raise ValueError("topology_capability_required")
    if config.get("mutation_kind") != TOPOLOGY_MUTATION_KIND or config.get("carrier") != TOPOLOGY_CARRIER:
        raise ValueError("topology_carrier_contract_mismatch")
    if "input_mutations" in config:
        raise ValueError("topology_runtime_remutation_forbidden")
    if config.get("input_identity_schema") != INPUT_SCHEMA_V2:
        raise ValueError("topology_requires_v2_input_identity")
    if not isinstance(config.get("mutation_plan"), dict):
        raise ValueError("topology_core_plan_required")
    plan = MutationPlan.from_dict(config["mutation_plan"])
    plan.validate()
    validate_topology_plan(plan)  # validates serialized snapshots; never mutates
    if config.get("plan_hash") != plan.plan_hash():
        raise ValueError("topology_plan_hash_mismatch")
    if config.get("final_input") != plan.final_config or set(plan.final_config) != {"topology_program"}:
        raise ValueError("topology_final_config_mismatch")
    if config.get("topology_program") != plan.final_config["topology_program"]:
        raise ValueError("topology_submitted_program_mismatch")
    receipts = topology_input_layer_receipts(plan)
    if config.get("input_layer_receipts") != receipts:
        raise ValueError("topology_input_layer_receipts_mismatch")
    if config.get("mutation_plan_sha256", canonical_hash(plan.to_dict())) != canonical_hash(plan.to_dict()):
        raise ValueError("topology_mutation_plan_sha256_mismatch")
    identity = _effective(config)
    if effective_input_hash(identity, schema=INPUT_SCHEMA_V2) != config.get("effective_input_sha256"):
        raise ValueError("topology_effective_input_hash_mismatch")
    bound = {"input_identity_schema": INPUT_SCHEMA_V2, "mutation_plan": plan.to_dict(),
             "final_input": plan.final_config, "input_layer_receipts": receipts,
             "operations": [op.to_dict() for op in plan.operations],
             "runtime_windows": plan.runtime_windows, "contract_hash": plan.contract_hash}
    for key in ("target_id", "target_revision", "rmw", "arm", "repeat", "seed", "mutation_kind", "carrier", "source_manifest"):
        if key not in config:
            raise ValueError("topology_missing_execution_identity:" + key)
        bound[key] = config[key]
    for key, value in bound.items():
        if key not in identity or canonical_hash(identity[key]) != canonical_hash(value):
            raise ValueError("topology_v2_binding_mismatch:" + key)
        if key in config and canonical_hash(config[key]) != canonical_hash(value):
            raise ValueError("topology_submitted_binding_mismatch:" + key)
    if config.get("observation_mode", "blackbox") != "blackbox":
        raise ValueError("topology_only_public_api_blackbox_supported")
    for key in ("rmws", "runtime_windows"):
        if config.get(key):
            raise ValueError("topology_unimplemented_runtime_control:" + key)
    from .topology_worker import normalize_topology_environment_mutation
    mutation = normalize_topology_environment_mutation(config.get("environment_mutation"))
    if mutation is not None:
        # An environment mutation is a physically applied window; it is only
        # admissible on the mutated arm.  Neutral/control arms must not install
        # anything, so they cannot carry this declaration at all.
        if config.get("arm") != "mutated":
            raise ValueError("topology_environment_mutation_requires_mutated_arm")
        if config.get("backend") == "offline_fake":
            raise ValueError("offline_fixture_cannot_claim_environment_mutation")
    return plan


def _binding(plan, receipts, digest, environment_mutation=None):
    """Route binding; the declared environment mutation is identity, not proof.

    It is included so a sealed run cannot silently change which netem spec was
    declared, and so the raw audit can compare declaration to installed spec.
    A null/absent value keeps the legacy binding byte-identical.
    """
    binding = {"schema": ROUTE_SCHEMA, "target_id": TOPOLOGY_TARGET,
               "capability": TOPOLOGY_CAPABILITY, "plan_hash": plan.plan_hash(),
               "final_input_sha256": canonical_hash(plan.final_config),
               "input_layer_receipts_sha256": canonical_hash(receipts),
               "effective_input_sha256": digest}
    if environment_mutation is not None:
        # Store the validated/normalized declaration so a raw untyped spec
        # cannot differ from the installed receipt only by int-vs-float JSON.
        from .topology_worker import normalize_topology_environment_mutation
        normalized = normalize_topology_environment_mutation(environment_mutation)
        binding["environment_mutation"] = deepcopy(normalized)
        binding["environment_mutation_sha256"] = canonical_hash(normalized)
    return binding


def _blocked(reason):
    return {"schema": "r2f.topology-run-facts/v1", "real_execution": False,
            "runtime_status": "blocked", "status": "blocked", "reason": "blocked:" + reason,
            "events": [], "facts": {}, "environment": {}, "execution": None,
            "execution_metadata": None, "raw_path": None, "raw_sha256": None,
            "precondition_receipts": [], "observation_receipts": [],
            "mutation_receipt": dict.fromkeys(RECEIPT_KEYS, False),
            "runtime_capability": TOPOLOGY_CAPABILITY, "candidate_eligible": False,
            "sealed": False, "mature": False, "maturity_denominator": None}


def _persist_runtime_result(result, output_dir):
    """Publish only the final route result; serialization/I/O failure propagates."""
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    data = json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + "\n"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=root,
                                         prefix=".runtime_result-", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, root / "runtime_result.json")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return result


def run_topology_route(config, output_dir, *, backend=None):
    """Dispatch exactly the sealed program; normal real backend uses canonical gates."""
    try:
        plan = validate_topology_route(config)
        if backend is not None and not isinstance(backend, (OfflineTopologyBackend, RosTopologyBackend)):
            raise ValueError("topology_backend_not_supported")
        fixture = (isinstance(backend, OfflineTopologyBackend) or config.get("backend") == OFFLINE_BACKEND)
        if isinstance(backend, OfflineTopologyBackend) and (backend.real_ros or backend.backend_name != OFFLINE_BACKEND):
            raise ValueError("offline_fixture_cannot_claim_real_backend")
        if fixture and (config.get("require_real_execution") is True
                        or config.get("evidence_purpose") in {"runtime_execution", "real_execution", "required", "seal"}
                        or config.get("live_execution_go") is True):
            raise ValueError("offline_fixture_cannot_claim_real_qualification")
        if isinstance(backend, RosTopologyBackend) and (fixture or not backend.real_ros or backend.backend_name != ROS_BACKEND):
            raise ValueError("topology_backend_provenance_mismatch")
        cfg = deepcopy(config)
        cfg["topology_route_binding"] = _binding(
            plan, cfg["input_layer_receipts"], cfg["effective_input_sha256"],
            cfg.get("environment_mutation"))
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        return _blocked("topology_route:" + str(exc))
    from .topology_pair import run_topology
    result = run_topology(deepcopy(plan.final_config["topology_program"]), cfg, output_dir, backend=backend)
    result["runtime_capability"] = TOPOLOGY_CAPABILITY
    result["route_binding"] = cfg["topology_route_binding"]
    result["application_audit"] = audit_topology_application(plan, cfg["input_layer_receipts"], result)
    # These are separate artifact/application observations, not a candidate or
    # oracle verdict. Preserve the original scenario receipt and all failures.
    result.setdefault("scenario_mutation_receipt", deepcopy(result["mutation_receipt"]))
    result["mutation_receipt"] = result["application_audit"]["mutation_receipt"]
    result["fixture"] = fixture
    result["candidate_eligible"] = result["sealed"] = result["mature"] = False
    # Pre-execution rejection (including missing live GO) has no raw execution
    # artifact. Preserve that contract instead of manufacturing an output run.
    if result.get("raw_path") is None:
        return result
    return _persist_runtime_result(result, output_dir)


def _raw(runtime):
    path = Path(runtime["raw_path"]).resolve()
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != runtime.get("raw_sha256"):
        raise ValueError("raw_hash_mismatch")
    rows = [json.loads(line) for line in data.splitlines()]
    if any(not isinstance(row, dict) or type(row.get("event_index")) is not int
           or row["event_index"] != index for index, row in enumerate(rows)):
        raise ValueError("raw_event_index_mismatch")
    return rows, str(path), digest


def _action_proof(action, events):
    aid = action["action_id"]
    starts = [e for e in events if e.get("event") == "topology_action_begin" and e.get("action_id") == aid]
    ends = [e for e in events if e.get("event") == "topology_action_done" and e.get("action_id") == aid]
    if len(starts) != 1 or len(ends) != 1:
        return [], "action_begin_done_missing_or_ambiguous"
    start, end = starts[0], ends[0]
    receipt = end.get("receipt") or {}
    if (start.get("action") != action or end.get("action") != action
            or receipt.get("action") != action or receipt.get("satisfied") is not True
            or start["event_index"] >= end["event_index"]
            or receipt.get("before") != start.get("before")):
        return [], "action_input_guard_or_chronology_not_proven"
    segment = events[start["event_index"] + 1:end["event_index"]]
    op = action["op"]
    eid, gen = action["endpoint_id"], action["generation"]
    if op in {"create", "recreate"}:
        creates = [e for e in segment if e.get("event") == "endpoint_created"
                   and e.get("logical_id") == eid and e.get("generation") == gen]
        if len(creates) != 1:
            return [], "endpoint_configuration_or_generation_event_missing"
        created = creates[0]
        after = receipt.get("after") or {}
        if (any(created.get(k) is None for k in ("endpoint_gid", "pid", "process_start_ticks", "endpoint_generation", "topic", "message_type", "qos"))
                or any(after.get(k) != created.get(k) for k in ("endpoint_gid", "pid", "process_start_ticks", "endpoint_generation"))
                or after.get("generation") != gen or after.get("alive") is not True):
            return [], "endpoint_identity_not_bound_to_after_state"
        if op == "recreate":
            predecessors = [e for e in events[:start["event_index"]] if e.get("event") == "endpoint_created" and e.get("logical_id") == eid]
            if not predecessors or predecessors[-1].get("generation") != gen - 1:
                return [], "fresh_generation_or_predecessor_retirement_not_proven"
            predecessor = predecessors[-1]
            retired = [e for e in events[:created["event_index"]]
                       if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                       and e.get("logical_id") == eid and e.get("generation") == gen - 1
                       and e.get("cleanup_status") == "closed"
                       and all(e.get(k) == predecessor.get(k) for k in
                               ("endpoint_gid", "pid", "process_start_ticks", "endpoint_generation"))]
            fixture_ok = (predecessor.get("real_ros") is True
                          or all(e.get("fixture") is True and e.get("real_ros") is False
                                 for e in retired))
            if (len(retired) != 1
                    or any(predecessor.get(k) == created.get(k) for k in
                           ("endpoint_gid", "pid", "endpoint_generation"))
                    or not fixture_ok):
                return [], "fresh_generation_or_predecessor_retirement_not_proven"
    elif op == "destroy":
        live_tokens = [e for e in events[:start["event_index"]]
                       if e.get("event") == "endpoint_created"
                       and e.get("logical_id") == eid and e.get("generation") == gen]
        if len(live_tokens) != 1:
            return [], "destroy_target_generation_not_bound"
        token = live_tokens[0]
        destroyed = [e for e in segment
                     if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                     and e.get("logical_id") == eid and e.get("generation") == gen
                     and e.get("cleanup_status") == "closed"]
        if len(destroyed) != 1:
            return [], "endpoint_destroy_cleanup_missing_or_ambiguous"
        retired = destroyed[0]
        if any(retired.get(k) != token.get(k) for k in
               ("endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")):
            return [], "retirement_identity_or_cleanup_mismatch"
        if token.get("real_ros") is not True and not (
                retired.get("fixture") is True and retired.get("real_ros") is False):
            return [], "fixture_provenance_missing"
    elif op == "snapshot":
        if not any(e.get("event") == "topology_pair_observation" for e in segment):
            return [], "snapshot_raw_observation_missing"
    elif op == "publish_batch":
        sends = [e for e in segment if e.get("event") == "send" and e.get("logical_id") == eid and e.get("generation") == gen]
        p = action["params"]
        expected = list(range(p["start_sequence"], p["start_sequence"] + p["count"]))
        if [e.get("send_sequence") for e in sends] != expected or any(e.get("publish_returned") is not True for e in sends):
            return [], "publish_batch_raw_application_missing"
    else:
        return [], "action_effect_not_audited:" + op
    return [start["event_index"], end["event_index"]] + [e["event_index"] for e in segment], None


def _presteps_proven(events, ready):
    """Reconstruct the imported pre-mutation contract from immutable raw rows."""
    from r2f.modules.s1.topology_targets import topology_target_declaration
    target = topology_target_declaration()
    if target.observation_contract.get("input_mutation_phase") != "compile_time":
        return False
    bindings = [e for e in events if e.get("event") == "input_binding"]
    mutations = [e for e in events if e.get("event") == "mutation_begin"]
    if len(bindings) != 1 or len(mutations) != 1:
        return False
    previous = bindings[0]["event_index"]
    state = dict(ready)
    for step in target.state_steps:
        starts = [e for e in events if e.get("event") == "state_step_before" and e.get("step_id") == step.step_id]
        ends = [e for e in events if e.get("event") == "state_step_after" and e.get("step_id") == step.step_id]
        if len(starts) != 1 or len(ends) != 1:
            return False
        start, end = starts[0], ends[0]
        checks = {c.condition_id: c.evaluate(state) for c in step.requires}
        if (not previous < start["event_index"] < end["event_index"] < mutations[0]["event_index"]
                or start.get("before") != state or start.get("requires") != checks or not all(checks.values())
                or any(e.get("action") != step.action or e.get("target_contract_sha256") != canonical_hash(target.to_dict())
                       for e in (start, end))):
            return False
        if step.action == "bind_input":
            evidence = end.get("evidence", {})
            if (evidence.get("input_binding_verified") is not True
                    or evidence.get("input_binding_ref") != bindings[0]["event_index"]
                    or evidence.get("route_binding") != bindings[0].get("receipt", {}).get("route_binding")):
                return False
        elif step.action != "admit_environment":
            return False
        state.update(deepcopy(step.parameters.get("after_updates", {})))
        if (end.get("after") != state or end.get("satisfied") is not True
                or end.get("requires") != checks):
            return False
        previous = end["event_index"]
    return all(c.evaluate(state) for c in target.state_preconditions)


def _netem_raw_refs(events, path, digest):
    return [{"path": path, "sha256": digest, "event_index": e["event_index"]}
            for e in sorted(events, key=lambda row: row["event_index"])]


def audit_topology_netem(binding, events, *, path, digest, real, fixture, observed_status):
    """Audit an opt-in tc-netem window strictly from hashed raw rows.

    The declaration is identity only.  Application requires a real isolated
    namespace, a spec-matched install, counters taken inside the installed
    window that carry live per-worker identity, matched packets > 0, and a
    verified restore.  Matched packets are an environment network effect and
    are never a SUT defect or a topology-pair mutation hit.  ``env.drop`` stays
    unsupported because the reused controller fixes loss at 0% and installs no
    drop rule; a declaration asking for drop is refused before execution.
    """
    declared = binding.get("environment_mutation")
    audit = {"schema": NETEM_AUDIT_SCHEMA, "status": "unknown",
             "declared": deepcopy(declared),
             "declared_sha256": binding.get("environment_mutation_sha256"),
             "fixture": bool(fixture), "execution_evidence": False,
             "window_installed": False, "window_restored": False,
             "identity_bound_counters": 0, "matched_packets": 0,
             "s1_mutation_hit": False,
             "hit_semantics": "environment_network_effect_not_sut_defect",
             "responsibility_layer": "environment_harness",
             "operators": {}, "reasons": [], "limitations": [
                 "netem_matched_packets_are_not_sut_defects",
                 "netem_window_is_not_a_topology_pair_semantic_observation",
                 "netem_never_drops_packets_loss_fixed_at_zero",
                 "endpoint_qos_deadline_is_not_a_netem_delay_window"]}
    isolation = [e for e in events if e.get("event") == "topology_netem_isolation"]
    setups = [e for e in events if e.get("event") == "topology_netem_setup"]
    installs = [e for e in events if e.get("event") == "topology_netem_install"]
    counters = [e for e in events if e.get("event") == "topology_netem_counter"]
    restores = [e for e in events if e.get("event") == "topology_netem_restore"]
    failures = [e for e in events if e.get("event") in {
        "topology_netem_install_failed", "topology_netem_counter_failed",
        "topology_netem_restore_failed"}]
    audit["raw_refs"] = _netem_raw_refs(
        isolation + setups + installs + counters + restores + failures, path, digest)

    def record_for(operator):
        record = {"operator_id": operator, "status": "unknown", "reasons": [],
                  "hit": False, "execution_evidence": False, "candidate_eligible": False,
                  "s1_mutation_hit": False,
                  "hit_semantics": "environment_network_effect_not_sut_defect"}
        if operator == "env.drop":
            record.update(status="unsupported",
                          reasons=["netem_controller_loss_fixed_at_zero_cannot_drop"],
                          effect_scope="no_drop_window_available",
                          raw_refs=list(audit["raw_refs"]))
            return record
        field = NETEM_PARAMETER[operator]
        value = None
        if isinstance(declared, dict) and isinstance(declared.get("spec"), dict):
            value = declared["spec"].get(field)
        record["declared_value"] = value
        record["effect_scope"] = ("network_layer_environment_effect_not_endpoint_qos_deadline"
                                  if operator == "env.delay"
                                  else "network_layer_environment_effect")
        if audit["status"] in OBSERVED_STATUSES:
            if not value:
                record.update(status="installed_not_exercised",
                              reasons=["declared_zero_for_this_window_no_applied_effect"])
            else:
                record.update(status=audit["status"],
                              execution_evidence=audit["execution_evidence"])
        else:
            record["status"] = "unsupported" if audit["status"] == "not_declared" else "unknown"
            record["reasons"] = list(dict.fromkeys(audit["reasons"]))[:6]
        record["raw_refs"] = list(audit["raw_refs"])
        return record

    if fixture:
        audit["status"] = "not_applicable_offline_fixture"
        audit["reasons"].append("offline_fixture_cannot_own_a_real_netem_window")
        audit["operators"] = {op: record_for(op) for op in NETEM_NAMED_OPERATORS}
        return audit
    if declared is None:
        audit["status"] = "not_declared"
        audit["operators"] = {op: record_for(op) for op in NETEM_NAMED_OPERATORS}
        return audit
    if (not isinstance(declared, dict) or declared.get("kind") != "discovery_netem"
            or binding.get("environment_mutation_sha256") != canonical_hash(declared)):
        audit["status"] = "invalid"
        audit["reasons"].append("raw_route_environment_mutation_binding_invalid")
        audit["operators"] = {op: record_for(op) for op in NETEM_NAMED_OPERATORS}
        return audit
    if len(isolation) != 1 or isolation[0].get("receipt", {}).get("isolated") is not True:
        audit["reasons"].append("netem_isolation_receipt_missing_or_not_isolated")
    if len(setups) != 1 or setups[0].get("receipt", {}).get("health_ok") is not True:
        audit["reasons"].append("netem_setup_receipt_missing_or_unhealthy")
    if failures:
        audit["reasons"].append("netem_window_failure_evidence_retained")
    if len(installs) == 1:
        install_receipt = installs[0].get("receipt") or {}
        audit["window_installed"] = install_receipt.get("installed") is True
        audit["install_ref"] = installs[0]["event_index"]
        from .discovery_network import (
            NETEM_SCOPE_FIELD, NETEM_SPEC_FIELDS, DiscoveryNetworkError, normalize_netem_spec)
        try:
            normalized = normalize_netem_spec(declared.get("spec"))
        except DiscoveryNetworkError:
            normalized = None
        if normalized is None:
            audit["reasons"].append("declared_netem_spec_invalid")
        else:
            # `neutral` is a derived flag, not an applied field.  Compare only
            # the fields the controller actually installs, on both sides.
            applied_fields = (*NETEM_SPEC_FIELDS, NETEM_SCOPE_FIELD)
            declared_spec = {k: normalized[k] for k in applied_fields}
            observed_spec = install_receipt.get("spec")
            observed_spec = ({k: observed_spec[k] for k in applied_fields if k in observed_spec}
                             if isinstance(observed_spec, dict) else None)
            if observed_spec is None or canonical_hash(observed_spec) != canonical_hash(declared_spec):
                audit["reasons"].append("installed_netem_spec_differs_from_declared")
        if install_receipt.get("applied") is not True:
            audit["reasons"].append("netem_install_not_applied")
        if install_receipt.get("neutral") is True:
            audit["reasons"].append("netem_install_is_a_neutral_no_op")
        if install_receipt.get("loss_pct") not in (0, 0.0):
            audit["reasons"].append("netem_install_loss_not_declared_zero")
        if not isinstance(install_receipt.get("tc_dump_sha256"), str):
            audit["reasons"].append("netem_install_tc_dump_missing")
    else:
        audit["reasons"].append("netem_install_receipt_missing_or_ambiguous")
    bound = []
    for counter in counters:
        row = counter.get("counter") or {}
        identities = counter.get("endpoint_identities") or []
        problems = counter.get("identity_problems") or []
        sockets = counter.get("worker_socket_bindings")
        if (row.get("counter_observed") is True and not problems and bool(identities)
                and all(i.get("identity_verified") is True for i in identities)
                and isinstance(sockets, dict)
                and counter.get("hit_semantics") == "environment_network_effect_not_sut_defect"
                and counter.get("s1_mutation_hit") is False):
            bound.append(counter)
    if restores:
        end = restores[-1].get("receipt") or {}
        audit["window_restored"] = bool(end.get("restored") is True
                                        and end.get("only_noqueue_root") is True
                                        and not end.get("residual_filter"))
        audit["restore_ref"] = restores[-1]["event_index"]
        if not audit["window_restored"]:
            audit["reasons"].append("netem_restore_not_verified_or_residual_present")
    else:
        audit["reasons"].append("netem_restore_receipt_missing")
    if len(installs) == 1 and restores:
        install_index, restore_index = installs[0]["event_index"], restores[-1]["event_index"]
        if not all(install_index < c["event_index"] < restore_index for c in bound):
            audit["reasons"].append("netem_counter_outside_install_restore_window")
    if not bound:
        audit["reasons"].append("no_identity_bound_counter_inside_window")
    audit["identity_bound_counters"] = len(bound)
    audit["matched_packets"] = max(
        (int((c.get("counter") or {}).get("affected_packets") or 0) for c in bound), default=0)
    audit["counter_refs"] = [c["event_index"] for c in counters]
    window_ok = (not audit["reasons"] and bool(bound)
                 and audit["matched_packets"] > 0 and audit["window_restored"])
    audit["execution_evidence"] = bool(real and window_ok)
    audit["status"] = (observed_status if (real and window_ok) else "unknown")
    if real and not window_ok and not audit["reasons"]:
        audit["reasons"].append("netem_window_not_exercised_no_matched_packets")
    audit["operators"] = {op: record_for(op) for op in NETEM_NAMED_OPERATORS}
    return audit


def _worker_configuration_proven(created, program):
    """Recognize exact legacy reflection or validate the new actual-QoS raw.

    The schema tag alone is never an observation. New receipts must bind the
    public readback to this worker and the sealed program's create request.
    """
    source = created.get("configuration_source")
    if source == "public_graph_endpoint_reflection":
        return "actual_endpoint_qos" not in created
    if source != "public_endpoint_actual_qos":
        return False
    from .qos_binding import validate_created_qos
    from .topology_worker import endpoint_expressibility
    try:
        specs = [e for e in program["endpoints"]
                 if e.get("logical_id") == created.get("logical_id")]
        if len(specs) != 1:
            return False
        spec = specs[0]
        api = ("get_publishers_info_by_topic" if spec["role"] == "writer"
               else "get_subscriptions_info_by_topic")
        if (created.get("identity_source") != api or created.get("topic") != spec["topic"]
                or created.get("message_type") != spec["type_name"]):
            return False
        requested = endpoint_expressibility(spec, {"rmw": created.get("rmw_identifier")})["public_qos"]
        validate_created_qos(created, requested)
        return True
    except (ValueError, TypeError, KeyError, RuntimeError):
        return False


def audit_topology_application(plan, input_layer_receipts, runtime):
    """Audit per-layer input→application, retaining unsupported/unknown gaps.

    Endpoint count: exact created configuration/generation identities for all
    final declarations that are created, removed IDs absent from the FULL trace.
    Insert/delete/swap: complete final action trace + raw begin/done/effects,
    omitted actions absent, swapped order observed. Deletion proves omission,
    not an independent DDS defect. Generation: new identity + predecessor close.
    New relation/timing layers delegate raw reflection to the independent
    operator auditor without claiming match/lifecycle effects or a hit.
    The three legacy configuration operators keep their existing hit contract.
    An overwritten layer is unknown, never a blanket hit. Other operators remain
    named unsupported; downstream fact/oracle adapters own their own raw audits.
    """
    audit = {"schema": AUDIT_SCHEMA, "status": "unknown", "layers": [], "reasons": [],
             "fixture": True, "execution_evidence": False, "candidate_eligible": False,
             "mutation_receipt": dict.fromkeys(RECEIPT_KEYS, False)}
    try:
        if isinstance(plan, dict):
            plan = MutationPlan.from_dict(plan)
        plan.validate()
        validate_topology_plan(plan)
        if input_layer_receipts != topology_input_layer_receipts(plan):
            raise ValueError("audit_input_layer_receipts_mismatch")
        events, path, digest = _raw(runtime)
        program = plan.final_config["topology_program"]
        bindings = [e for e in events if e.get("event") == "input_binding"]
        if len(bindings) != 1 or bindings[0].get("receipt", {}).get("program_sha256") != canonical_hash(program):
            raise ValueError("raw_input_program_binding_mismatch")
        binding = bindings[0]["receipt"].get("route_binding") or {}
        if (binding.get("plan_hash") != plan.plan_hash()
                or binding.get("final_input_sha256") != canonical_hash(plan.final_config)
                or binding.get("input_layer_receipts_sha256") != canonical_hash(input_layer_receipts)):
            raise ValueError("raw_plan_or_layer_binding_mismatch")
        audit.update(raw_path=path, raw_sha256=digest, plan_hash=plan.plan_hash())
        provenance = [e for e in events if e.get("event") == "topology_provenance"]
        fixture = not (len(provenance) == 1 and provenance[0].get("fixture") is False
                       and provenance[0].get("runtime_kind") == "ros2_public_api")

        # Cross-check raw formal gates, never runtime['real_execution']/facts.
        sources = [e for e in events if e.get("event") == "topology_source_checked"]
        clean = [e for e in events if e.get("event") == "topology_cleanup"]
        locks = [e for e in events if e.get("event") == "isolation_locks_acquired"]
        released = [e for e in events if e.get("event") == "isolation_locks_released"]
        environments = [e for e in events if e.get("event") == "environment_preflight"]
        created_workers = [e for e in events if e.get("event") == "endpoint_created"]
        prepared = [e for e in events if e.get("event") == "topology_backend_prepared"]
        measured_environment = environments[0].get("environment", {}) if len(environments) == 1 else {}
        observed_safety = all(measured_environment.get(k, {}).get("ok") is True
                              for k in ("process_scan", "old_lock_scan", "rtps_probe"))
        expected_source_phases = ["before_preflight", "after_preflight", "before_presteps"] + ["before_action:" + a["action_id"] for a in program["actions"]] + ["after_actions", "after_cleanup"]
        source_phases_complete = [e.get("phase") for e in sources] == expected_source_phases
        from .topology_pair import _execution_workers
        workers = _execution_workers(events, path)
        worker_provenance = bool(created_workers) and all(
            e.get("backend") == ROS_BACKEND and e.get("real_ros") is True
            and _worker_configuration_proven(e, program)
            and type(e.get("observed_domain_id")) is int and e.get("observed_domain_id") == e.get("domain_id")
            and e.get("rmw_identifier") in {"rmw_cyclonedds_cpp", "rmw_fastrtps_cpp"}
            and all(e.get(k) is not None for k in ("pid", "process_start_ticks", "endpoint_generation", "endpoint_gid"))
            for e in created_workers) and all(
                not w["unknowns"] and w["fresh_process"] and w["separate_context"]
                and w["returncode"] == 0 and w["closed"] for w in workers)
        ready = {"environment_ok": len(environments) == 1 and environments[0].get("ok") is True,
                 "locks_held": len(locks) == 1 and len(locks[0].get("locks", [])) == 3,
                 "source_identity_ok": source_phases_complete and all(
                     e.get("source", {}).get("source_frozen") is True
                     and e.get("source") == sources[0].get("source") for e in sources),
                 "topology_capability_admitted": binding.get("capability") == TOPOLOGY_CAPABILITY}
        presteps_proven = _presteps_proven(events, ready)
        audit["canonical_preconditions_observed"] = presteps_proven
        real = (not fixture and presteps_proven and source_phases_complete and observed_safety and worker_provenance
                and len(prepared) == 1 and prepared[0].get("ros_execution") is True
                and bool(sources) and all(e.get("source", {}).get("source_frozen") is True
                and e.get("source") == sources[0].get("source") for e in sources)
                and sources[-1].get("phase") == "after_cleanup"
                and len(clean) == len(locks) == len(released) == len(environments) == 1
                and clean[0].get("receipt", {}).get("closed") is True
                and len(locks[0].get("locks", [])) == 3 and environments[0].get("ok") is True
                and locks[0]["event_index"] < environments[0]["event_index"] < provenance[0]["event_index"]
                < clean[0]["event_index"] < released[0]["event_index"]
                and not any(e.get("event") in {"runtime_abort", "isolation_locks_retained"} for e in events)
                and not any(e.get("backend") == OFFLINE_BACKEND for e in events))
        audit.update(fixture=fixture, execution_evidence=bool(real))
        observed_status = "application_observed" if real else ("fixture_application_observed" if fixture else "artifact_application_observed")
        audit["netem_application"] = audit_topology_netem(
            binding, events, path=path, digest=digest, real=bool(real), fixture=fixture,
            observed_status=observed_status)
        if not real and not fixture:
            audit["reasons"].append("raw_formal_execution_qualification_incomplete")
        begins = [e for e in events if e.get("event") == "topology_action_begin"]
        trace_complete = [e.get("action") for e in begins] == program["actions"]
        ends = [e for e in events if e.get("event") == "topology_action_done"]
        trace_complete = trace_complete and len(ends) == len(begins) and all(
            a["event_index"] < b["event_index"] and (i + 1 == len(begins) or b["event_index"] < begins[i+1]["event_index"])
            for i, (a, b) in enumerate(zip(begins, ends)))
        final_actions = {a["action_id"]: a for a in program["actions"]}
        all_proofs = {aid: _action_proof(action, events) for aid, action in final_actions.items()}
        netem_audit_status = (audit.get("netem_application") or {}).get("status")
        operator_records = {}
        for index, receipt in enumerate(input_layer_receipts):
            if (receipt["operator_id"] in CONFIGURATION_AUDIT_OPERATORS
                    or is_deadline_configuration_delay(receipt)):
                record = audit_operator_application([receipt], events, program, fixture=fixture)[0]
                record["layer_index"] = index
                operator_records[index] = record
        for index, receipt in enumerate(input_layer_receipts):
            operator = receipt["operator_id"]
            configuration_audit = (operator in CONFIGURATION_AUDIT_OPERATORS
                                   or is_deadline_configuration_delay(receipt))
            before, after = receipt["input_before"], receipt["input_after"]
            layer = {"layer_index": index, "op_id": plan.operations[index].op_id,
                     "operator_id": operator, "input_before": deepcopy(before), "input_after": deepcopy(after),
                     "status": "unknown", "reasons": [], "raw_refs": [], "hit": False,
                     "execution_evidence": False, "candidate_eligible": False}
            audit["layers"].append(layer)
            if operator not in SUPPORTED_OPERATORS and not configuration_audit:
                reasons = ["operator_application_auditor_not_implemented:" + operator]
                if operator in NETEM_NAMED_OPERATORS:
                    # The netem window is a config-level environment mutation
                    # and is audited in audit["netem_application"] from its own
                    # raw receipts.  This input layer is a QoS/parameter slot
                    # (or absent), so it is never evidence of a network effect.
                    reasons.append("netem_application_audited_separately_not_via_this_input_slot")
                    layer["netem_application_ref"] = netem_audit_status
                layer.update(status="unsupported", runtime_status="runtime_unsupported",
                             reasons=reasons)
                continue
            before_actions = {a["action_id"]: a for a in before["actions"]}
            after_actions = {a["action_id"]: a for a in after["actions"]}
            affected = {aid for aid, action in after_actions.items() if before_actions.get(aid) != action}
            removed = set(before_actions) - set(after_actions)
            if operator == "action.swap":
                old_order, new_order = list(before_actions), list(after_actions)
                affected = {aid for index, aid in enumerate(new_order)
                            if aid not in old_order or old_order.index(aid) != index}
            if not trace_complete:
                layer["reasons"].append("final_program_action_trace_incomplete_or_out_of_order")
            if any(final_actions.get(aid) != after_actions[aid] for aid in affected) or any(aid in final_actions for aid in removed):
                layer["reasons"].append("layer_effect_overridden_by_later_layer")
            if operator == "action.swap" and [aid for aid in final_actions if aid in affected] != [aid for aid in after_actions if aid in affected]:
                layer["reasons"].append("swapped_order_overridden_by_later_layer")
            if before == after:
                layer["reasons"].append("layer_has_no_input_change")
            # Verify every executed creation against final endpoint declarations,
            # not just the mutation receipt's declared intent or backend handle.
            final_eps = {e["logical_id"]: e for e in program["endpoints"]}
            for created in (e for e in events if e.get("event") == "endpoint_created"):
                spec = final_eps.get(created.get("logical_id"))
                if spec is None or created.get("topic") != spec["topic"] or created.get("message_type") != spec["type_name"]:
                    layer["reasons"].append("raw_endpoint_configuration_mismatch")
                    break
                # Offline exact QoS and real public-QoS normalization have
                # distinct observed schemas; neither is filled from config.
                expected_qos = spec["qos"]
                if created.get("backend") != OFFLINE_BACKEND:
                    from .topology_worker import endpoint_expressibility
                    expected_qos = endpoint_expressibility(spec, {"rmw": created.get("rmw_identifier")})["public_qos"]
                observed_qos = created.get("qos")
                if created.get("backend") != OFFLINE_BACKEND:
                    mandatory = {"reliability", "durability", "deadline_ns", "liveliness", "liveliness_lease_duration_ns"}
                    changed_fields = {"S1.topology.qos.depth": "depth", "S1.topology.qos.reliability": "reliability",
                                      "S1.topology.qos.deadline": "deadline_ns", "S1.topology.qos.lease": "liveliness_lease_duration_ns",
                                      "S1.topology.qos.liveliness": "liveliness"}
                    if operator in CONFIG_OPERATORS:
                        mandatory.add(changed_fields.get(receipt["slot_id"].split("#", 1)[0], "unimplemented_configuration_slot"))
                    if "actual_endpoint_qos" in created:
                        from .qos_binding import validate_created_qos
                        try:
                            comparison = validate_created_qos(created, expected_qos)
                            qos_matches = True
                            if comparison["default_resolutions"]:
                                layer.setdefault("qos_default_resolutions", []).append({
                                    "logical_id": created.get("logical_id"),
                                    "generation": created.get("generation"),
                                    "raw_event_index": created.get("event_index"),
                                    "resolutions": comparison["default_resolutions"]})
                        except (ValueError, TypeError, KeyError):
                            qos_matches = False
                    else:
                        qos_matches = isinstance(observed_qos, dict) and all(observed_qos.get(k) == expected_qos.get(k) and k in observed_qos for k in mandatory)
                else:
                    qos_matches = observed_qos == expected_qos
                if not qos_matches:
                    layer["reasons"].append("raw_endpoint_qos_mismatch_or_field_not_reflected")
                    break
            if operator == "endpoint.count":
                old_ids = {e["logical_id"] for e in before["endpoints"]}
                new_ids = {e["logical_id"] for e in after["endpoints"]}
                if old_ids == new_ids:
                    layer["reasons"].append("endpoint_count_delta_missing")
                added = new_ids - old_ids
                for eid in added:
                    creates = [a for a in after["actions"] if a["endpoint_id"] == eid and a["op"] in {"create", "recreate"}]
                    if not creates:
                        layer["reasons"].append("added_endpoint_never_created:" + eid)
                    affected.update(a["action_id"] for a in creates)
                if any(e.get("logical_id") in old_ids-new_ids for e in events if e.get("event") == "endpoint_created"):
                    layer["reasons"].append("removed_endpoint_was_created")
            if operator in CONFIG_OPERATORS:
                layer["effect_scope"] = "endpoint_public_configuration_only_not_timed_lifecycle"
                old_eps = {e["logical_id"]: e for e in before["endpoints"]}
                changed = [e for e in after["endpoints"] if old_eps.get(e["logical_id"]) != e]
                if not changed:
                    layer["reasons"].append("configuration_delta_missing")
                for spec in changed:
                    eid = spec["logical_id"]
                    if final_eps.get(eid) != spec:
                        layer["reasons"].append("configuration_effect_overridden_by_later_layer:" + eid)
                    creation_actions = [a for a in after["actions"] if a["endpoint_id"] == eid and a["op"] in {"create", "recreate"}]
                    if not creation_actions:
                        layer["reasons"].append("configured_endpoint_never_created:" + eid)
                    affected.update(a["action_id"] for a in creation_actions)
            if configuration_audit:
                operator_record = deepcopy(operator_records[index])
                layer["operator_application"] = operator_record
                for key in ("configuration_application_observed", "application_kind", "qos_delta", "relation_pair", "observed_qos", "dds_contract",
                            "observed_effect", "timing_delta", "effect_scope",
                            "effect_observation", "limitations", "public_input_interventions",
                            "configuration_intervention_proven", "configuration_generations",
                            "filter_generations", "filter_scope", "filter_false_implies_unmatch",
                            "dynamic_update_covered", "lease_covered"):
                    if key in operator_record:
                        layer[key] = deepcopy(operator_record[key])
                layer["reasons"].extend(operator_record["reasons"])
                layer["raw_refs"].extend(operator_record["raw_refs"])
                if operator_record["status"] not in OBSERVED_STATUSES and not operator_record["reasons"]:
                    layer["reasons"].append("operator_configuration_not_observed")
                changed_ids = {row["logical_id"] for row in operator_record.get("qos_delta", [])}
                changed_ids.update(row["logical_id"] for row in
                                   operator_record.get("public_input_interventions", []))
                changed_ids.update(row["logical_id"] for row in
                                   operator_record.get("filter_generations", []))
                affected.update(aid for aid, a in final_actions.items()
                                if a["endpoint_id"] in changed_ids and a["op"] in {"create", "recreate"})
            if not affected and not removed and operator != "endpoint.count":
                layer["reasons"].append("operator_effect_not_identified")
            proof_actions = set(final_actions) if operator in {"endpoint.count", "action.delete", "action.swap"} else affected
            proof_actions |= {aid for aid, a in final_actions.items() if a["op"] in {"create", "recreate"}}
            for aid in sorted(proof_actions):
                refs, reason = all_proofs.get(aid, ([], "action_not_in_final_program"))
                layer["raw_refs"].extend(refs)
                if reason:
                    layer["reasons"].append(aid + ":" + reason)
            layer["raw_refs"] = [{"path": path, "sha256": digest, "event_index": ref}
                                 for ref in sorted(set(layer["raw_refs"] + [bindings[0]["event_index"]]))]
            if not layer["reasons"]:
                layer.update(status=observed_status, execution_evidence=bool(real),
                             hit=bool(real and (not configuration_audit
                                                or operator_record.get("configuration_intervention_proven") is True)))
                if configuration_audit:
                    layer.update(
                        configuration_intervention_proven=(
                            operator_record.get("configuration_intervention_proven") is True),
                        effect_observed=False)
            if configuration_audit:
                layer["operator_application"].update(
                    status=layer["status"], raw_refs=deepcopy(layer["raw_refs"]),
                    execution_evidence=bool(real and not layer["reasons"]),
                    hit=bool(layer["hit"]),
                    effect_observed=bool(layer.get("effect_observed", False)))
        all_observed = bool(audit["layers"]) and all(l["status"] in {"application_observed", "fixture_application_observed", "artifact_application_observed"} for l in audit["layers"])
        audit["status"] = observed_status if all_observed else "incomplete"
        if real:
            audit["mutation_receipt"].update(planned=True, submitted=True, carrier_admitted=True,
                                            configured=all_observed, applied=all_observed,
                                            hit=all_observed and all(l["hit"] for l in audit["layers"]),
                                            effect_observed=all_observed and all(
                                                l.get("effect_observed", l["hit"]) for l in audit["layers"]))
        audit["receipt_reason"] = audit["status"]
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        audit.update(status="invalid", reasons=[type(exc).__name__ + ":" + str(exc)])
    return audit
