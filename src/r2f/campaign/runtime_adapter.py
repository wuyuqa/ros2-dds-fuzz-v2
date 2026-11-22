"""Audit runtime tokens and map actual steps to core receipts.

The runtime executes the public API actions. This module does not run a second
state machine: each core receipt names its before/after raw token indexes and
is admitted only after the corresponding runtime predicate and event proof.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
from r2f.core._json import sha256_digest, snapshot
from r2f.core.evidence import canonical_hash, verify_raw
from r2f.core.execution import ExecutionRecord, ExecutionStage
from r2f.core.mutation import MutationReceipt
from r2f.core.state_machine import StepReceipt


def read_events(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def _iso(ns):
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).isoformat()


def _one(events, kind, **fields):
    found = [e for e in events if e.get("event") == kind and all(e.get(k) == v for k, v in fields.items())]
    if len(found) != 1:
        raise ValueError(f"expected one {kind}:{fields}; observed {len(found)}")
    return found[0]


def endpoint_identity_proof(events, runtime, config, tokens, opening, closing):
    """Verify an explicit GID or bounded graph correlation; never equate them."""
    writer, reader = tokens["writer"], tokens["reader"]
    received = [e for e in events if e.get("event") == "receive"]
    if not received:
        raise ValueError("no delivery identity witness")
    direct = all(e.get("publisher_gid") is not None for e in received)
    if direct:
        return {"basis": "message_info_gid", "publisher_gid_status": "verified" if all(e["publisher_gid"] == writer["endpoint_gid"] for e in received) else "mismatch", "direct_gid_match": all(e["publisher_gid"] == writer["endpoint_gid"] for e in received), "limitations": []}
    # A present mismatching GID never falls back to unavailable API correlation.
    if any(e.get("publisher_gid") not in (None, writer["endpoint_gid"]) for e in received):
        return {"basis": "message_info_gid", "publisher_gid_status": "mismatch", "direct_gid_match": False, "limitations": []}
    if any(e.get("publisher_gid") is None and (e.get("publisher_gid_status") != "unavailable_api" or e.get("identity_basis") != "endpoint_correlated") for e in received):
        raise ValueError("missing publisher GID without explicit unavailable_api receipt")
    declaration = _one(events, "identity_observation")
    summary = runtime.get("facts", {})
    if declaration.get("identity_basis") != "endpoint_correlated" or (declaration.get("graph_stable_across_window") is not True or declaration.get("endpoint_identity_correlated") is not True) or summary.get("identity_basis") != "endpoint_correlated" or summary.get("endpoint_identity_correlated") is not True or summary.get("graph_stable_across_window") is not True:
        raise ValueError("runtime did not supply explicit bounded endpoint correlation")
    if writer.get("pid") == reader.get("pid") or not writer.get("pid") or not reader.get("pid"):
        raise ValueError("endpoint correlation lacks independent processes")
    if any(e.get("payload_run_id") != config["run_id"] for e in received):
        raise ValueError("endpoint correlation run payload mismatch")
    proof = {}
    for role in ("writer", "reader"):
        graphs = sorted((e for e in events if e.get("event") == "graph_snapshot" and e.get("role") == role), key=lambda e: e["monotonic_ns"])
        before = [g for g in graphs if g["monotonic_ns"] <= opening["monotonic_ns"]]
        after = [g for g in graphs if g["monotonic_ns"] >= closing["monotonic_ns"]]
        if not before or not after:
            raise ValueError("endpoint correlation graph boundary missing")
        bounded = [before[-1]] + [g for g in graphs if opening["monotonic_ns"] < g["monotonic_ns"] < closing["monotonic_ns"]] + [after[0]]
        if any({p["endpoint_gid"] for p in g["publishers"]} != {writer["endpoint_gid"]} or {p["endpoint_gid"] for p in g["subscriptions"]} != {reader["endpoint_gid"]} for g in bounded):
            raise ValueError("endpoint correlation graph changed across observation window")
        proof[role] = {"before_index": before[-1]["event_index"], "after_index": after[0]["event_index"], "stable": True}
    if summary.get("graph_boundary_receipts") != proof:
        raise ValueError("runtime graph boundary proof differs from raw")
    return {"basis": "endpoint_correlated", "publisher_gid_status": "unavailable_api", "direct_gid_match": None,
            "graph_boundary_receipts": proof,
            "limitations": ["Public callback does not expose publisher_gid; identity is correlated through unique bounded endpoint graph, independent process, run payload and sequence receipts."]}


def fresh_match_proof(events, tokens, opening, *, anchor_ns=None):
    """Bind the latest paired public count queries to raw request/snapshot/ack."""
    requests = [e for e in events if e.get("event") == "match_snapshot_requested"]
    if anchor_ns is not None:
        requests = [e for e in requests if e.get("requested_monotonic_ns", -1) >= anchor_ns
                    and e["event_index"] < opening["event_index"]]
    if len(requests) < 2 or len(requests) % 2:
        raise ValueError("match_snapshot request pair incomplete")
    # Controller issues writer then reader on every polling round. Do not combine rounds.
    for writer_request, reader_request in zip(requests[::2], requests[1::2]):
        if (writer_request.get("role"), reader_request.get("role")) != ("writer", "reader"):
            raise ValueError("match_snapshot request roles differ from paired query")
    selected = requests[-2:]
    if selected[0].get("command_id") == selected[1].get("command_id"):
        raise ValueError("match_snapshot command ids are not independent")
    proof, completed = {}, []
    reader_created = tokens["reader"]["created_monotonic_ns"]
    anchor = reader_created if anchor_ns is None else max(reader_created, anchor_ns)
    for request in selected:
        role, command_id = request["role"], request.get("command_id")
        requested_ns = request.get("requested_monotonic_ns")
        if (type(command_id) is not int or type(requested_ns) is not int
                or request.get("reader_created_monotonic_ns") != reader_created
                or not anchor <= requested_ns <= request["monotonic_ns"]
                or request["event_index"] <= tokens["reader"]["event_index"]):
            raise ValueError("match_snapshot request is not after reader creation")
        try:
            snapshot = _one(events, "match_snapshot", role=role, command_id=command_id)
            ack = _one(events, "command_done", role=role, command_id=command_id)
        except ValueError as exc:
            raise ValueError(f"match_snapshot command binding: {exc}") from exc
        sampled_ns = snapshot.get("sampled_monotonic_ns")
        if (type(sampled_ns) is not int or not requested_ns <= sampled_ns <= snapshot["monotonic_ns"]
                or not snapshot["monotonic_ns"] <= ack["monotonic_ns"] <= opening["monotonic_ns"]
                or not request["event_index"] < snapshot["event_index"] < ack["event_index"] < opening["event_index"]
                or type(snapshot.get("matched_count")) is not int or snapshot["matched_count"] != 1
                or snapshot.get("count_api") != ("get_subscription_count" if role == "writer" else "get_publisher_count")
                or snapshot.get("endpoint_gid") != tokens[role]["endpoint_gid"]):
            raise ValueError("match_snapshot count/API/time/command proof invalid")
        for event in (snapshot, ack):
            if any(event.get(key) != tokens[role].get(key) for key in ("pid", "endpoint_generation", "rmw_identifier")):
                raise ValueError("match_snapshot worker identity differs from endpoint")
        proof[role] = {"command_id": command_id, "request_index": request["event_index"],
                       "snapshot_index": snapshot["event_index"], "ack_index": ack["event_index"],
                       "sampled_monotonic_ns": sampled_ns, "count_api": snapshot["count_api"]}
        completed.append(ack["monotonic_ns"])
    return max(completed), proof


G01_TARGET = "discovery.match_compatibility.fuzz_v1"


def _g01_admission(events, runtime, config):
    """Reconstruct the existing environment step, never the late-reader steps."""
    import hashlib

    env = _one(events, "environment_preflight")
    lock_event = _one(events, "isolation_locks_acquired")
    locks = lock_event.get("locks")
    if env.get("ok") is not True or not isinstance(locks, list) or len(locks) != 3:
        raise ValueError("g01_environment_or_three_leases_missing")
    state_dir = str(Path(runtime["raw_path"]).resolve().parent)
    expected_names = {"domain-" + str(config["domain_id"]) + ".lock",
                      "campaign-" + hashlib.sha256(config["campaign_id"].encode()).hexdigest() + ".lock",
                      "state-" + hashlib.sha256(state_dir.encode()).hexdigest() + ".lock"}
    if {Path(lock.get("path", "")).name for lock in locks} != expected_names:
        raise ValueError("g01_lease_scope_mismatch")
    for lock in locks:
        owner = lock.get("owner", {})
        if (lock.get("acquired") is not True or owner.get("pid") != lock_event.get("pid")
                or owner.get("campaign_id") != config["campaign_id"]
                or owner.get("domain_id") != config["domain_id"] or owner.get("state_dir") != state_dir):
            raise ValueError("g01_lease_owner_mismatch")
    before = _one(events, "state_step_before", step_id="admit_environment")
    after = _one(events, "state_step_after", step_id="admit_environment")
    if (not max(env["monotonic_ns"], lock_event["monotonic_ns"]) < before["monotonic_ns"] < after["monotonic_ns"]
            or not max(env["event_index"], lock_event["event_index"]) < before["event_index"] < after["event_index"]
            or before.get("before", {}).get("satisfied") is not True
            or before.get("receipt_kind") != "precondition" or after.get("receipt_kind") != "precondition"
            or before.get("requires") != ["environment_ok", "locks_held"]
            or after.get("requires") != before["requires"]
            or after.get("satisfied") is not True or after.get("after", {}).get("satisfied") is not True):
        raise ValueError("g01_environment_step_chronology_invalid")
    begin = _one(events, "mutation_begin" if config["arm"] == "mutated" else "control_begin")
    refs = [{"step_id": "admit_environment", "raw_before_index": before["event_index"],
             "raw_after_index": after["event_index"]}]
    snapshot = begin.get("preconditions_snapshot", {})
    if (not after["monotonic_ns"] < begin["monotonic_ns"]
            or not after["event_index"] < begin["event_index"]
            or begin.get("precondition_receipt_refs") != refs
            or snapshot.get("environment_ok") is not True or snapshot.get("locks_held") is not True
            or snapshot.get("endpoints_created") is not False
            or begin.get("mutation_kind") != "endpoint_qos_variation"):
        raise ValueError("g01_mutation_begin_admission_invalid")
    for role in ("writer", "reader"):
        token = _one(events, "endpoint_created", role=role)
        if (not begin["monotonic_ns"] < token["created_monotonic_ns"] <= token["monotonic_ns"]
                or not begin["event_index"] < token["event_index"]):
            raise ValueError("g01_endpoint_created_before_configuration_begin:" + role)
    return before, after, begin


def _g01_facts(events, runtime, config):
    """Only raw event fields feed G01; returned runtime facts are ignored."""
    from r2f.runtime.relation_observer import g01_configuration_readback, g01_runtime_observation

    observation = g01_runtime_observation(events, config)
    facts = dict(observation["facts"] or {})
    facts.update(schema_version=1, environment_healthy=False, process_healthy=False,
                 preconditions_reached=False, endpoint_identity_complete=False,
                 observed_effects=[], configuration_intervention_verified=False,
                 relation_effect_observed=False, g01_observation=observation,
                 candidate_eligible=False)
    try:
        tokens = g01_configuration_readback(events, config)
        facts["configuration_intervention_verified"] = True
        _g01_admission(events, runtime, config)
        facts.update(environment_healthy=True, preconditions_reached=True)
        # Exit return codes are process-controller raw, not a summary counter.
        for role, token in tokens.items():
            started = _one(events, "process_started", role=role)
            stopped = _one(events, "worker_shutdown", role=role)
            exited = _one(events, "process_exit", role=role)
            if (started.get("pid") != token["pid"] or stopped.get("pid") != token["pid"]
                    or exited.get("pid") != token["pid"] or exited.get("returncode") != 0
                    or stopped.get("endpoint_generation") != token["endpoint_generation"]
                    or stopped.get("rmw_identifier") != token["rmw_identifier"]
                    or not started["event_index"] < token["event_index"] < stopped["event_index"] < exited["event_index"]
                    or not started["monotonic_ns"] <= token["created_monotonic_ns"] <= stopped["monotonic_ns"] <= exited["monotonic_ns"]):
                raise ValueError("g01_worker_lifecycle_identity_invalid:" + role)
        if any(e.get("event") in {"worker_error", "worker_unexpected_exit", "runtime_abort"} for e in events):
            raise ValueError("g01_worker_or_runtime_error")
        ending = _one(events, "runtime_end")
        if (ending.get("runtime_status") != "completed" or ending.get("real_execution") is not True
                or ending.get("reason") is not None):
            raise ValueError("g01_runtime_not_completed")
        facts["process_healthy"] = True
        facts["endpoint_identity_complete"] = observation["oracle_input"] is True
        facts["relation_effect_observed"] = observation["relation_effect_observed"] is True
        if facts["relation_effect_observed"]:
            facts["observed_effects"] = ["public_api_pair_match"]
        if not observation["oracle_input"]:
            facts["observation_unknown_reasons"] = observation["reasons"]
    except (KeyError, ValueError, TypeError) as exc:
        facts["evidence_error"] = str(exc)
        # A relation-looking fragment without its health/admission proof is a
        # diagnostic, never a SUT fail admitted through the primary guard.
        facts.update(match=None, delivery=None, observed=None)
        if isinstance(facts.get("window"), dict):
            facts["window"] = dict(facts["window"], complete=False)
    return facts


def _g01_receipts(scenario, runtime, events, config):
    before, after, _ = _g01_admission(events, runtime, config)
    if (len(scenario.state_steps) != 1 or scenario.state_steps[0].step_id != "admit_environment"
            or scenario.state_steps[0].action != "admit_environment"):
        raise ValueError("g01_target_environment_step_contract_mismatch")
    step = scenario.state_steps[0]
    expected = [c.condition_id for c in step.requires]
    actuals = runtime.get("precondition_receipts", [])
    if len(actuals) != 1:
        raise ValueError("g01_runtime_precondition_count_invalid")
    actual = actuals[0]
    if (actual.get("step_id") != step.step_id or actual.get("requires") != expected
            or actual.get("satisfied") is not True or before.get("requires") != expected
            or actual.get("before") != before.get("before") or actual.get("after") != after.get("after")):
        raise ValueError("g01_runtime_environment_receipt_differs_from_raw")
    state = {"environment_ok": True, "locks_held": True}
    checks = {c.condition_id: c.evaluate(state) for c in step.requires}
    if not all(checks.values()):
        raise ValueError("g01_environment_requires_not_reached")
    proof = {"runtime_step_id": step.step_id, "raw_before_index": before["event_index"],
             "raw_after_index": after["event_index"], "runtime_receipt": actual}
    return [StepReceipt(step.step_id, step.action, dict(state), dict(state, admit_environment=True),
                        checks, "executed", _iso(before["time_ns"]), _iso(after["time_ns"]), proof).to_dict()]


def facts_from_events(events, runtime, config, *, capture_source=None):
    """Only raw tokens produce primary facts; summaries cannot substitute.

    capture_source is an explicit archival Python argument, never a config flag.
    Only audited saved topology reconstruction supplies it; execution uses None.
    """
    if capture_source is not None and config.get("target_id") != "discovery.endpoint_topology.fuzz_v1":
        raise ValueError("saved_capture_source_requires_topology")
    if config.get("target_id") == G01_TARGET:
        return _g01_facts(events, runtime, config)
    if config.get("target_id") == "discovery.endpoint_topology.fuzz_v1":
        from .topology_adapter import facts_from_events as topology_facts
        if capture_source is None:
            return topology_facts(events, runtime, config)
        return topology_facts(events, runtime, config, capture_source=capture_source)
    if str(config.get("target_id", "")).startswith("builtin.discovery_projection_reconciliation"):
        from .projection_adapter import projection_facts
        return projection_facts(events, runtime, config)
    if str(config.get("target_id", "")).startswith("discovery.interruption_recovery"):
        from .recovery_adapter import recovery_facts
        return recovery_facts(events, runtime, config)
    facts = {"schema_version": 1, "environment_healthy": runtime.get("environment", {}).get("ok") is True,
             "process_healthy": False, "preconditions_reached": False,
             "endpoint_identity_complete": False, "observed_effects": [], "sent": [], "received": []}
    metadata = runtime.get("execution_metadata") or runtime.get("execution") or {}
    workers = metadata.get("workers", [])
    facts["process_healthy"] = (len(workers) == 2 and all(w.get("returncode") == 0 for w in workers)
                                  and len({w.get("pid") for w in workers}) == 2
                                  and not any(e.get("event") in {"worker_error", "worker_unexpected_exit"} for e in events))
    try:
        tokens = {role: _one(events, "endpoint_created", role=role) for role in ("writer", "reader")}
        # A mixed-vendor pair legitimately reports two different RMW identifiers;
        # each role must match its own submitted vendor, never the global one.
        from r2f.runtime.ros2_pair import role_rmw
        for role, token in tokens.items():
            if not token.get("endpoint_gid") or token.get("rmw_identifier") != role_rmw(config, role):
                raise ValueError("endpoint identity or RMW missing")
            facts[role] = {"id": token["endpoint_gid"], "generation": token["endpoint_generation"], "created_ns": token["created_monotonic_ns"]}
        writer, reader = facts["writer"], facts["reader"]
        opening = _one(events, "window_open", window="post_match_delivery")
        if any(e.get("event") in {"match_snapshot", "match_snapshot_requested"} for e in events):
            match_ns, facts["match_snapshot_proof"] = fresh_match_proof(events, tokens, opening)
        else:
            # Historical raw without the snapshot schema retains its original chronology.
            matches = {role: [e for e in events if e.get("event") == "match" and e.get("role") == role and e.get("matched_count") == 1 and e["monotonic_ns"] >= reader["created_ns"]] for role in tokens}
            if not all(matches.values()):
                raise ValueError("match_not_observed")
            match_ns = max(matches[role][0]["monotonic_ns"] for role in matches)
        facts["match"] = {"writer_id": writer["id"], "reader_id": reader["id"], "at_ns": match_ns}
        closing = _one(events, "window_close", window="post_match_delivery")
        facts["window"] = {"start_ns": opening["monotonic_ns"], "end_ns": closing["monotonic_ns"], "complete": True}
        contract = _one(events, "delivery_contract")
        expected = list(range(config["pre_match_count"], config["pre_match_count"] + config["post_match_count"]))
        if contract.get("expected_sequences") != expected or contract.get("sentinel_sequence") != expected[-1] or contract.get("prejoin_in_delivery_contract") is not False:
            raise ValueError("delivery contract differs from submitted counts")
        if contract.get("qos") != config["qos"]:
            raise ValueError("delivery QoS differs from submitted config")
        omissions = [e for e in events if e.get("event") == "harness_omission"]
        if omissions and (config["arm"] != "negative" or not config.get("fault_id")):
            raise ValueError("unplanned harness omission")
        if config["arm"] == "negative":
            omit = expected[len(expected) // 2]
            control = _one(events, "fault_control_receipt")
            if len(omissions) != 1 or omissions[0].get("send_sequence") != omit or control.get("omitted_sequence") != omit or control.get("applied") is not True or control.get("candidate_eligible") is not False or control.get("responsibility_layer") != "harness":
                raise ValueError("negative omission lacks independent applied receipt")
            facts["controlled_omission"] = {"sequence": omit, "responsibility_layer": "harness", "candidate_eligible": False}
        identity = endpoint_identity_proof(events, runtime, config, tokens, opening, closing)
        facts["identity_proof"] = identity
        for event in events:
            if event.get("event") == "send" or (event.get("event") == "harness_omission" and config["arm"] == "negative"):
                if event.get("event") == "send" and event.get("publish_returned") is not True:
                    raise ValueError("unacknowledged publish")
                facts["sent"].append({"sequence": event["send_sequence"], "at_ns": event["monotonic_ns"], "writer_id": event.get("writer_gid") or writer["id"], "generation": writer["generation"], "source": "controlled_expected_obligation" if event["event"] == "harness_omission" else "public_publish"})
            if event.get("event") == "receive":
                if event.get("payload_run_id") != config["run_id"] or event.get("endpoint_generation") != reader["generation"]:
                    raise ValueError("receive payload/generation identity mismatch")
                facts["received"].append({"sequence": event["send_sequence"], "at_ns": event["monotonic_ns"], "writer_id": event.get("publisher_gid") or (writer["id"] if identity["basis"] == "endpoint_correlated" else None), "writer_generation": writer["generation"], "reader_id": event.get("reader_gid"), "reader_generation": event.get("endpoint_generation")})
        delay = _one(events, "reader_created_after_writer")
        if delay.get("configured_delay_sec") != config["reader_delay_sec"]:
            raise ValueError("executed reader delay differs from submitted config")
        # Both independent graph APIs must contain exactly these endpoints.
        for role in ("writer", "reader"):
            if tokens[role].get("qos") != config["qos"] or tokens[role].get("topic") != config["topic"]:
                raise ValueError("actual endpoint QoS/topic differs from submitted config")
            snapshots = [e for e in events if e.get("event") == "graph_snapshot" and e.get("role") == role and e["monotonic_ns"] <= opening["monotonic_ns"]]
            if not snapshots or {p["endpoint_gid"] for p in snapshots[-1]["publishers"]} != {writer["id"]} or {p["endpoint_gid"] for p in snapshots[-1]["subscriptions"]} != {reader["id"]}:
                raise ValueError("graph endpoint identity proof missing")
        observations = runtime.get("observation_receipts", [])
        if [r.get("step_id") for r in observations] != ["reader_created", "endpoints_matched", "graph_identity_verified"]:
            raise ValueError("post-mutation observation receipt set incomplete")
        for observed in observations:
            before = _one(events, "state_step_before", step_id=observed["step_id"])
            after = _one(events, "state_step_after", step_id=observed["step_id"])
            if observed.get("satisfied") is not True or after.get("satisfied") is not True or before.get("before") != observed.get("before") or after.get("after") != observed.get("after"):
                raise ValueError("observation receipt not raw-backed")
        facts["endpoint_identity_complete"] = bool(identity) and all(e.get("reader_gid") for e in events if e.get("event") == "receive")
        facts["preconditions_reached"] = all(r.get("satisfied") is True for r in runtime.get("precondition_receipts", [])) and len(runtime.get("precondition_receipts", [])) == 2
        facts["observed_effects"] = ["reader_created_after_writer", "match_established"]
    except (KeyError, ValueError, TypeError, IndexError) as exc:
        facts["evidence_error"] = str(exc)
        facts["endpoint_identity_complete"] = False
        facts["window"] = {"complete": False}
    from .state_projection import attach_state_observations
    return attach_state_observations(facts, events, runtime, config)


def canonical_receipts(scenario, runtime, events, *, config=None, capture_source=None):
    """Exact 1:1 runtime step mapping; raw before/after events are mandatory."""
    if capture_source is not None and scenario.target_id != "discovery.endpoint_topology.fuzz_v1":
        raise ValueError("saved_capture_source_requires_topology")
    if scenario.target_id == G01_TARGET:
        if not isinstance(config, dict) or config.get("target_id") != G01_TARGET:
            raise ValueError("g01_receipts_require_exact_runtime_config")
        return _g01_receipts(scenario, runtime, events, config)
    if scenario.target_id == "discovery.endpoint_topology.fuzz_v1":
        from .topology_adapter import canonical_receipts as topology_receipts
        if capture_source is None:
            return topology_receipts(scenario, runtime, events, config=config)
        return topology_receipts(scenario, runtime, events, config=config, capture_source=capture_source)
    if scenario.target_id.startswith("builtin.discovery_projection_reconciliation"):
        from .projection_adapter import projection_receipts
        return projection_receipts(scenario, runtime, events)
    if scenario.target_id.startswith("discovery.interruption_recovery"):
        from .recovery_adapter import recovery_receipts
        return recovery_receipts(scenario, runtime, events)
    receipts, state = [], {}
    env = _one(events, "environment_preflight")
    locks = _one(events, "isolation_locks_acquired")
    state.update(environment_ok=env.get("ok") is True, locks_held=bool(locks.get("locks")))
    runtime_steps = runtime.get("precondition_receipts", [])
    if len(runtime_steps) != len(scenario.state_steps):
        raise ValueError("runtime state step count differs from target plan")
    for step, actual in zip(scenario.state_steps, runtime_steps):
        if step.step_id != actual.get("step_id") or actual.get("satisfied") is not True:
            raise ValueError("runtime step not reached or identity differs")
        before = _one(events, "state_step_before", step_id=step.step_id)
        after = _one(events, "state_step_after", step_id=step.step_id)
        if before["monotonic_ns"] >= after["monotonic_ns"] or after.get("satisfied") is not True or after.get("after") != actual.get("after") or before.get("before") != actual.get("before"):
            raise ValueError("state receipt does not match raw before/after evidence")
        if actual.get("requires") != [c.condition_id for c in step.requires]:
            raise ValueError("runtime requires differs from compiled target")
        snapshot = dict(state)
        checks = {c.condition_id: c.evaluate(snapshot) for c in step.requires}
        if not all(checks.values()):
            raise ValueError("state requires not reached")
        state[step.step_id] = True
        if any(e.get("event") == "reader_created_after_writer" and e["monotonic_ns"] <= after["monotonic_ns"] for e in events):
            state["reader_after_writer"] = True
        proof = {"runtime_step_id": step.step_id, "raw_before_index": before["event_index"], "raw_after_index": after["event_index"], "runtime_receipt": actual}
        receipts.append(StepReceipt(step.step_id, step.action, snapshot, dict(state), checks, "executed", _iso(before["time_ns"]), _iso(after["time_ns"]), proof).to_dict())
    begin = _one(events, "mutation_begin" if scenario.arm == "mutated" else "control_begin")
    if begin["monotonic_ns"] <= after["monotonic_ns"]:
        raise ValueError("mutation began before actual preconditions were reached")
    snapshot = begin.get("preconditions_snapshot", {})
    if snapshot.get("writer_created") is not True or snapshot.get("writer_prejoin_published") is not True or snapshot.get("reader_created") is not False:
        raise ValueError("mutation begin lacks pre-reader precondition snapshot")
    references = [{"step_id": r["step_id"], "raw_before_index": r["result"]["raw_before_index"], "raw_after_index": r["result"]["raw_after_index"]} for r in receipts]
    if begin.get("precondition_receipt_refs") != references:
        raise ValueError("mutation begin precondition proof references differ")
    reader_created = _one(events, "endpoint_created", role="reader")
    if reader_created["created_monotonic_ns"] <= begin["monotonic_ns"]:
        raise ValueError("reader creation preceded mutation begin")
    return receipts


def _produce_facts(services, evidence, *, capture_source=None):
    """Contain provider faults before facts can enter oracle or persistence."""
    returned = False
    result = None
    try:
        result = (services.fact_producer.produce(evidence) if capture_source is None else
                  services.fact_producer.produce(evidence, capture_source=capture_source))
        returned = True
        if not isinstance(result, Mapping):
            raise TypeError("fact producer result must be a mapping")
        # Use the core JSON contract, including string keys and finite numbers,
        # then detach the result so later persistence cannot invoke provider code.
        return json.loads(json.dumps(snapshot(result), allow_nan=False)), None
    except Exception as exc:
        return {}, {"kind": "invalid_result" if returned else "exception",
                    "exception_type": type(exc).__name__, "reason": str(exc),
                    "returned_result_type": type(result).__name__ if returned else None}


def _saved_topology_capture(row, runtime, submitted, scenario, source):
    """Admit the existing topology v1 capture, including its namespace sidecar.

    This is an artifact audit only. Source qualification and action chronology
    still belong to the canonical topology receipt/fact producers below.
    """
    from r2f.runtime.topology_namespace import AUTHENTICATION, _load_ref, _receipt_refs
    from r2f.runtime.topology_pair import _execution_workers
    from .boundary_audit import NAMESPACES, _Audit

    def require(ok, reason):
        if not ok:
            raise ValueError("saved_case_topology_" + reason)

    def same(a, b):
        return canonical_hash(a) == canonical_hash(b)

    metadata = runtime.get("execution_metadata", {})
    require(runtime.get("schema") == "r2f.topology-run-facts/v1"
            and metadata.get("schema") == "r2f.topology-execution-metadata/v1",
            "unsupported_capture_schema")
    require(runtime.get("real_execution") is True and runtime.get("fixture") is False
            and runtime.get("runtime_status") == "completed"
            and metadata.get("real_ros") is True and metadata.get("real_execution") is True
            and metadata.get("execution_evidence") is True
            and metadata.get("runtime_status") == "completed"
            and metadata.get("backend") == "ros2_public_api", "execution_identity_mismatch")
    require(isinstance(source, dict) and source.get("source_frozen") is True
            and same(metadata.get("source_manifest"), source)
            and same(metadata.get("source_after"), source), "source_identity_mismatch")
    if (row.get("arm") == "negative" or row.get("control_kind") not in (None, "health", "transparency")
            or row.get("candidate_source") == "control"
            or metadata.get("fault_id") or metadata.get("fault_control")):
        raise ValueError("saved_case_control_contamination")
    identity = metadata.get("source_identity", {})
    sha256_digest(identity.get("runtime_source_sha256"))
    require(all(same(identity.get(k), source.get(k)) for k in ("head", "source_repo"))
            and same(runtime.get("environment", {}).get("source"), identity), "runtime_source_mismatch")
    require(same(row.get("runtime_execution_metadata"), metadata), "metadata_mismatch")
    for key in ("run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "domain_id"):
        require(same(metadata.get(key), row.get(key)), "context_mismatch:" + key)
    require(all(type(metadata.get(k)) is int for k in ("target_revision", "repeat", "domain_id"))
            and metadata["repeat"] == scenario.repeat, "context_integer_invalid")
    context = metadata.get("sealed_execution_context", {})
    expected = {k: metadata[k] for k in
                ("run_id", "target_id", "target_revision", "rmw", "domain_id", "arm", "repeat")}
    expected.update(input_identity_schema=submitted["input_identity_schema"],
                    effective_input_sha256=submitted["effective_input_sha256"],
                    plan_hash=scenario.mutation_plan.plan_hash())
    require(same(context, expected), "sealed_context_mismatch")
    require(metadata.get("actual_rmws") == [metadata["rmw"]]
            and same(metadata.get("actual_domains"), [metadata["domain_id"]])
            and same(metadata.get("declared_domain_id"), metadata["domain_id"])
            and metadata.get("declared_rmw") == metadata["rmw"], "actual_context_mismatch")
    require(runtime.get("raw_path") == row["raw_path"] == metadata.get("raw_path")
            and runtime.get("raw_sha256") == row["raw_sha256"], "raw_identity_mismatch")
    verify_raw(row["raw_path"], row["raw_sha256"])
    events = read_events(row["raw_path"])
    require(same(runtime.get("events"), events), "raw_events_mismatch")
    # Namespace annotations are parent observations, never fields of raw worker
    # tokens. Verify the immutable sidecar before permitting those exact extras.
    ns = runtime.get("topology_namespace", {})
    require(ns.get("schema") == "r2f.topology-namespace/v1", "namespace_schema_unsupported")
    verify_raw(ns["manifest_path"], ns["manifest_sha256"])
    manifest = json.loads(Path(ns["manifest_path"]).read_text())
    require(manifest.get("schema") == "r2f.topology-namespace-manifest/v1"
            and manifest.get("error") is None, "namespace_manifest_invalid")
    require(all(same(ns.get(k), manifest.get(k)) for k in
                ("bridge_id", "cleanup", "inner_lease_scope", "nested_controller_binding",
                 "outer_controller", "raw_pid_scope", "worker_bindings")), "namespace_manifest_mismatch")
    root, scope = manifest["output_root"], ns["raw_pid_scope"]
    require(Path(root) == Path(row["raw_path"]).parent, "artifact_root_mismatch")
    environment = metadata.get("environment_manifest", {})
    verify_raw(environment["path"], environment["sha256"])
    require(same(json.loads(Path(environment["path"]).read_text()), runtime.get("environment")),
            "environment_artifact_mismatch")
    child = _load_ref(manifest["inner_result_ref"], root)
    wrapper_fields = {"topology_namespace", "execution", "execution_boundary", "execution_metadata"}
    require(same({k: v for k, v in child.items() if k not in wrapper_fields},
                 {k: v for k, v in runtime.items() if k not in wrapper_fields}), "inner_result_mismatch")
    require(same(manifest.get("source_manifest"), source), "namespace_source_mismatch")
    for key in ("source_before_ref", "source_after_ref"):
        require(same(_load_ref(manifest[key], root), source), "namespace_source_mismatch")
    envelope = _load_ref(manifest["envelope_ref"], root)
    require(all(same(envelope.get(k), manifest.get(k)) for k in
                ("raw_ref", "inner_result_ref", "source_before_ref", "source_after_ref")),
            "envelope_artifact_mismatch")
    lease, bindings = _receipt_refs(envelope, runtime, root, scope)
    require(same(lease, ns["inner_lease_scope"]) and same(bindings, ns["worker_bindings"]),
            "namespace_raw_binding_mismatch")
    request = _load_ref(manifest["request_ref"], root)
    require(same(ns.get("outer_controller"), request.get("outer"))
            and isinstance(request.get("outer"), dict), "namespace_request_outer_mismatch")
    binding = ns["nested_controller_binding"]
    require(isinstance(binding, dict) and set(binding) == {
                "authentication", "parent_pid_scope", "outer", "reported_inner",
                "inner_pid", "pid_scope", "pid_namespace_chain"}
            and binding.get("authentication") == AUTHENTICATION,
            "controller_binding_schema_invalid")
    ancestor, reported = binding.get("outer"), binding.get("reported_inner")
    require(isinstance(ancestor, dict) and isinstance(reported, dict),
            "controller_identity_mismatch")
    require(type(ancestor.get("pid")) is int and ancestor["pid"] > 1
            and same(ancestor.get("nspid"), [ancestor["pid"], 1])
            and same(binding.get("inner_pid"), 1)
            and same(reported.get("pid"), 1) and same(reported.get("nspid"), [1])
            and same(ancestor.get("uid"), 1000) and same(reported.get("uid"), 0)
            and type(ancestor.get("start_ticks")) is int and ancestor["start_ticks"] > 0,
            "controller_identity_mismatch")
    require(binding.get("pid_scope") == scope
            and ancestor.get("namespaces", {}).get("pid") == scope
            and same(ancestor.get("start_ticks"), lease.get("controller_start_ticks"))
            and all(same(ancestor.get(k), reported.get(k)) for k in
                    ("namespaces", "start_ticks", "cap_eff", "uid_map", "gid_map")),
            "controller_identity_mismatch")
    # Reuse the canonical pure identity/map checks, without the live boundary
    # audit's enclosing runtime artifacts. The saved request is independent of
    # the parent descriptor; the child proof must describe a distinct PID1.
    proof = _Audit()
    outer = proof.identity(request["outer"], "saved_request.outer")
    require(same(outer.get("pid"), 1) and same(outer.get("nspid"), [1]),
            "controller_identity_mismatch")
    outer_gids = proof.mapping(outer.get("gid_map"), "saved_request.outer.gid_map")
    expected_gid = outer_gids[0][0] if len(outer_gids) == 1 else None
    require(type(reported.get("cap_eff")) is int and reported["cap_eff"] > 0
            and bool(reported["cap_eff"] & (1 << 12))
            and isinstance(reported.get("namespaces"), dict)
            and set(reported["namespaces"]) == set(NAMESPACES)
            and all(isinstance(reported["namespaces"][name], str)
                    and reported["namespaces"][name] != outer["namespaces"].get(name)
                    for name in NAMESPACES), "controller_identity_mismatch")
    require(proof.mapping(reported.get("uid_map"), "saved_controller.uid_map") == [[0, 1000, 1]]
            and proof.mapping(reported.get("gid_map"), "saved_controller.gid_map") == [[0, expected_gid, 1]]
            and not proof.errors, "controller_identity_mismatch")
    outer_scope = outer["namespaces"]["pid"]
    require(binding.get("parent_pid_scope") == outer_scope
            and same(binding.get("pid_namespace_chain"), [
                {"pid_scope": outer_scope, "pid": ancestor.get("pid")},
                {"pid_scope": scope, "pid": 1}]), "controller_pid_chain_mismatch")
    cleanup = ns["cleanup"]
    require(cleanup.get("confirmed") is True and cleanup.get("controller_exited") is True
            and type(cleanup.get("wrapper_returncode")) is int and cleanup["wrapper_returncode"] == 0,
            "namespace_cleanup_incomplete")
    workers = metadata.get("workers")
    require(isinstance(workers, list) and bool(workers), "workers_missing")
    projected = _execution_workers(events, row["raw_path"])
    bare = []
    for worker in workers:
        require(worker.get("pid_namespace") == scope
                and worker.get("pid_interpretation") == "namespace_PID_not_host_PID",
                "worker_namespace_mismatch")
        require(all(type(worker.get(k)) is int and worker[k] > 0 for k in
                    ("pid", "pgid", "process_start_ticks", "generation", "worker_generation"))
                and type(worker.get("returncode")) is int and worker["returncode"] == 0
                and worker.get("closed") is True and worker.get("real_ros") is True
                and worker.get("fixture") is False and worker.get("fresh_process") is True
                and worker.get("separate_context") is True and worker.get("unknowns") == []
                and same(worker.get("domain_id"), metadata["domain_id"])
                and worker.get("rmw_identifier") == metadata["rmw"], "worker_identity_mismatch")
        matches = [b for b in bindings if all(same(b.get(k), worker.get(k)) for k in
                   ("logical_id", "generation", "endpoint_gid", "endpoint_generation", "pid", "process_start_ticks"))]
        require(len(matches) == 1 and same(matches[0].get("namespaces"), binding["outer"]["namespaces"]),
                "worker_process_binding_mismatch")
        bare.append({k: v for k, v in worker.items() if k not in ("pid_namespace", "pid_interpretation")})
    require(same(bare, projected), "worker_raw_projection_mismatch")
    raw_metadata = _one(events, "topology_execution_metadata")["metadata"]
    require(same(child.get("execution_metadata"), raw_metadata), "inner_metadata_mismatch")
    expected_metadata = {k: v for k, v in metadata.items() if k not in ("pid_namespace", "pid_interpretation")}
    expected_metadata["workers"] = bare
    require(metadata.get("pid_namespace") == scope
            and metadata.get("pid_interpretation") == "namespace_PID_not_host_PID"
            and same(raw_metadata, expected_metadata), "raw_metadata_mismatch")
    if (row.get("arm") == "negative" or row.get("control_kind") not in (None, "health", "transparency")
            or metadata.get("fault_id") or metadata.get("fault_control")
            or any(e.get("event") in ("harness_omission", "fault_control_receipt") for e in events)):
        raise ValueError("saved_case_control_contamination")
    # Use the saved, hash-bound request config. Rebuilding it from capsule
    # fields can change defaults (notably the episode contract identity).
    config = request["config"]
    require(request.get("bridge_id") == ns["bridge_id"]
            and request.get("output_root") == root
            and same(request.get("source_manifest"), source)
            and request.get("config_sha256") == manifest.get("config_sha256") == canonical_hash(config),
            "request_identity_mismatch")
    effective = submitted["effective_input"]
    for key in ("target_id", "target_revision", "rmw", "arm", "repeat", "seed", "mutation_kind",
                "mutation_parameters", "carrier", "input_identity_schema", "effective_input_sha256",
                "scenario", "source_manifest", "mutation_plan"):
        require(same(config.get(key), submitted.get(key)), "config_input_mismatch:" + key)
    require(all(same(config.get(k), v) for k, v in expected.items())
            and same(config.get("effective_input"), effective)
            and same(config.get("final_input"), effective["final_input"])
            and same(config.get("input_layer_receipts"), effective["input_layer_receipts"]),
            "config_context_mismatch")
    if (config.get("fault_id") or config.get("fault_control") or config.get("control_kind")
            or config.get("candidate_source") == "control"):
        raise ValueError("saved_case_control_contamination")
    require(metadata.get("input_program_sha256") == canonical_hash(config["topology_program"]),
            "input_program_mismatch")
    return config, events


def rebuild_saved_case_facts(case, *, runtime, capsule, services):
    """Read-only canonical raw reconstruction; never ingest a new execution.

    The caller verifies the saved JSON artifact references. This seam binds their
    contents to the original row/capsule, uses the normal raw/receipt validators,
    and independently compares the registered producer with canonical facts.
    Supplied row typed_facts and recorded oracle outputs are never inputs.
    """
    from r2f.core.scenario import Scenario

    case, runtime = snapshot(case), snapshot(runtime)
    row, submitted = case["row"], capsule.input
    effective = submitted["effective_input"]
    scenario = Scenario.from_dict(submitted["scenario"])
    source = row.get("source_identity") or row.get("source_manifest") or row.get("provenance")
    if (canonical_hash(submitted["source_manifest"]) != canonical_hash(source)
            or submitted["effective_input_sha256"] != row["effective_input_sha256"]
            or case["input_sha256"] != row["effective_input_sha256"]
            or any(canonical_hash(submitted[key]) != canonical_hash(row.get(key)) for key in
                   ("target_id", "target_revision", "rmw", "arm"))
            or any(canonical_hash(row.get(key)) != canonical_hash(effective[key])
                   for key in ("final_input", "input_layer_receipts") if key in row)
            or ("target" in row and row["target"] != submitted["target"])):
        raise ValueError("saved_case_capture_input_mismatch")
    if scenario.target_id == "discovery.endpoint_topology.fuzz_v1":
        from r2f.runtime.execution_boundary import BoundaryError
        try:
            config, events = _saved_topology_capture(row, runtime, submitted, scenario, source)
        except BoundaryError as exc:
            raise ValueError("saved_case_topology_" + str(exc)) from exc
    else:
        metadata = runtime.get("execution_metadata") or runtime.get("execution") or {}
        capture_source = metadata.get("source")
        source_bound = canonical_hash(capture_source) == canonical_hash(source)
        # Public-pair captures measure the runtime/observer tree separately from
        # the campaign behavior manifest. Bind that existing schema through the
        # row's complete captured metadata, then join its repository and HEAD.
        if not source_bound and isinstance(capture_source, dict) and isinstance(source, dict):
            sha256_digest(capture_source.get("runtime_source_sha256"))
            if "observer_source_sha256" in capture_source:
                sha256_digest(capture_source["observer_source_sha256"])
            source_bound = (bool(source.get("head")) and bool(source.get("source_repo"))
                            and all(canonical_hash(capture_source.get(key)) == canonical_hash(source[key])
                                    for key in ("head", "source_repo"))
                            and canonical_hash(row.get("runtime_execution_metadata")) == canonical_hash(metadata))
        if (runtime.get("real_execution") is not True or runtime.get("runtime_status") != "completed"
                or not source_bound
                or canonical_hash(metadata.get("source_after")) != canonical_hash(capture_source)
                or ("runtime_execution_metadata" in row
                    and canonical_hash(row["runtime_execution_metadata"]) != canonical_hash(metadata))
                or type(metadata.get("target_revision")) is not int
                or any(canonical_hash(metadata.get(key)) != canonical_hash(row.get(key)) for key in
                       ("run_id", "target_id", "target_revision", "rmw", "arm"))
                or any(canonical_hash(metadata.get(key)) != canonical_hash(row[key])
                       for key in ("domain_id", "repeat") if key in row)
                or type(metadata.get("repeat")) is not int or metadata["repeat"] != scenario.repeat
                or type(metadata.get("domain_id")) is not int
                or runtime.get("raw_path") != row["raw_path"]
                or runtime.get("raw_sha256") != row["raw_sha256"]):
            raise ValueError("saved_case_runtime_identity_mismatch")
        # Capture-side markers must survive configuration merging. Null in the
        # capsule cannot erase a fault actually recorded by the runtime.
        if metadata.get("fault_id") or metadata.get("fault_control"):
            raise ValueError("saved_case_control_contamination")
        if any(canonical_hash(metadata[key]) != canonical_hash(value)
               for key, value in effective["final_input"].items() if key in metadata):
            raise ValueError("saved_case_capture_config_mismatch")
        config = dict(metadata, **effective["final_input"])
        verify_raw(row["raw_path"], row["raw_sha256"])
        events = read_events(row["raw_path"])
        if (not events or canonical_hash(runtime.get("events")) != canonical_hash(events)
                or any(type(e.get("event_index")) is not int or e["event_index"] != i
                       or e.get("run_id") != config["run_id"]
                       or canonical_hash(e.get("domain_id")) != canonical_hash(config["domain_id"])
                       for i, e in enumerate(events))):
            raise ValueError("saved_case_raw_identity_mismatch")
        if (row.get("arm") == "negative" or row.get("control_kind") not in
                (None, "health", "transparency") or config.get("fault_id")
                or any(e.get("event") in ("harness_omission", "fault_control_receipt") for e in events)):
            raise ValueError("saved_case_control_contamination")
        # Only the processes that created these endpoints can supply their health.
        workers = metadata.get("workers", [])
        tokens = {role: _one(events, "endpoint_created", role=role)
                  for role in ("writer", "reader")}
        if (not isinstance(workers, list) or len(workers) != 2
                or {w.get("role") for w in workers} != set(tokens)
                or any(type(w.get("pid")) is not int or w["pid"] <= 0
                       or type(w.get("returncode")) is not int or w["returncode"] != 0
                       or canonical_hash(w["pid"]) != canonical_hash(tokens[w["role"]].get("pid"))
                       or any(canonical_hash(w[key]) != canonical_hash(tokens[w["role"]][key])
                              for key in ("pid_namespace", "worker_generation")
                              if key in w and key in tokens[w["role"]])
                       for w in workers)
                or len({w["pid"] for w in workers}) != 2):
            raise ValueError("saved_case_worker_identity_mismatch")
    receipt = MutationReceipt.from_dict(runtime["mutation_receipt"]).to_dict()
    if (_one(events, "runtime_end").get("mutation_receipt") != receipt
            or row.get("mutation_receipt") != receipt):
        raise ValueError("saved_case_raw_mutation_receipt_mismatch")
    # Capture admission above authenticates this recorded source and all raw
    # source-check phases. Current evaluation source is measured by the caller.
    # Keep this explicit argument out of JSON config and every execution call.
    archival = ({"capture_source": source}
                if scenario.target_id == "discovery.endpoint_topology.fuzz_v1" else {})
    canonical_receipts(scenario, runtime, events, config=config, **archival)
    canonical = facts_from_events(events, runtime, config, **archival)
    if (canonical.get("evidence_error") or not all(canonical.get(key) is True for key in
            ("environment_healthy", "process_healthy", "preconditions_reached", "endpoint_identity_complete"))):
        raise ValueError("saved_case_raw_facts_incomplete:" + str(canonical.get("evidence_error")))
    evidence = snapshot({"events": events, "runtime": runtime, "config": config})
    digest = canonical_hash(evidence)
    facts, failure = _produce_facts(services, evidence, **archival)
    if failure is not None:
        raise ValueError("saved_case_fact_producer_failure:" + str(failure))
    if canonical_hash(evidence) != digest or canonical_hash(facts) != canonical_hash(canonical):
        raise ValueError("saved_case_fact_producer_changed_raw_or_facts")
    if "input_identity_schema" in submitted:
        scenario.input_identity_schema = submitted["input_identity_schema"]
    scenario.final_input = snapshot(effective["final_input"])
    scenario.input_layer_receipts = snapshot(effective["input_layer_receipts"])
    scenario.source_manifest = snapshot(source)
    scenario.effective_input_sha256 = submitted["effective_input_sha256"]
    return scenario, snapshot(canonical)


def _primary_rule_id(scenario):
    """Return only the declared rule identity; never infer one from a verdict."""
    contract = scenario.target.rule_contract
    if scenario.target_id == G01_TARGET:
        from r2f.modules.s1.oracles import COMPATIBILITY_RULE_ID
        rule_id = contract.get("compatibility_rule") if isinstance(contract, Mapping) else None
        return rule_id if rule_id == COMPATIBILITY_RULE_ID else None
    rule_id = contract.get("primary_rule") if isinstance(contract, Mapping) else None
    return rule_id if isinstance(rule_id, str) and rule_id.strip() else None


def _evaluate_oracle(services, scenario, facts):
    """Contain an oracle tool failure without turning it into a SUT verdict."""
    returned = False
    result = None
    primary_rule = _primary_rule_id(scenario)
    try:
        if primary_rule is None:
            raise ValueError("target_primary_rule_missing_or_invalid")
        result = services.oracle.evaluate(scenario, facts)
        returned = True
        if (not isinstance(result, dict)
                or result.get("rule_id") != primary_rule
                or result.get("rule_role") != "primary"
                or not isinstance(result.get("verdict"), str)
                or result["verdict"] not in {"pass", "fail", "unknown", "not_applicable"}
                or not isinstance(result.get("closure"), dict)):
            raise ValueError("invalid primary oracle result identity/verdict/closure")
        for key in ("reason_codes", "forbidden_residuals"):
            if not isinstance(result.get(key), list) or any(not isinstance(v, str) for v in result[key]):
                raise ValueError("invalid primary oracle result:" + key)
        # Freeze only a JSON-serializable result; malformed extras cannot break
        # the later row/report persistence boundary either.
        result = json.loads(json.dumps(result, allow_nan=False))
        return result, None
    except Exception as exc:
        diagnostic = {"kind": "invalid_result" if returned else "exception",
                      "exception_type": type(exc).__name__, "reason": str(exc)}
        if returned:
            try:
                diagnostic["returned_result"] = json.loads(json.dumps(result, allow_nan=False))
            except (TypeError, ValueError, OverflowError, RecursionError):
                diagnostic["unserializable_result_type"] = type(result).__name__
        # This is an unavailable tool result, not another primary evaluation.
        return {"rule_id": primary_rule or "unresolved_primary_rule", "rule_role": "primary",
                "verdict": "unknown", "reason_codes": ["oracle_tool_failure"],
                "forbidden_residuals": [], "closure": {}}, diagnostic


NOHIT_TERMINAL_SCHEMA = "r2f.topology-nohit-terminal/v1"


def topology_nohit_terminal(config, runtime, row):
    """Exact single-input terminal, not a success/coverage qualification.

    Callers must additionally run strict runtime qualification and independently
    audit the boundary. This shape alone never grants continuation.
    """
    if (config.get("target_id") != "discovery.endpoint_topology.fuzz_v1"
            or config.get("arm") != "mutated" or row.get("execution_stage") != "reached"
            or row.get("actual_run") is not True or runtime.get("real_execution") is not True
            or runtime.get("runtime_status") != "completed" or runtime.get("status") != "executed"
            or row.get("abort_reason") or row.get("ingestion_error") or row.get("tool_error")):
        raise ValueError("topology_nohit_terminal_context_invalid")
    from r2f.core.execution import validate_execution
    validate_execution(row)
    if [entry.get("execution_stage") for entry in row.get("stage_history", [])] != [
            "modelled", "expressible", "planned", "precondition_executed", "reached"]:
        raise ValueError("topology_nohit_terminal_stage_history_invalid")
    receipt = MutationReceipt.from_dict(runtime.get("mutation_receipt")).to_dict()
    if (receipt["hit"] is not False or receipt["effect_observed"] is not False
            or not all(receipt[k] for k in ("planned", "submitted", "carrier_admitted"))
            or row.get("mutation_receipt") != receipt):
        raise ValueError("topology_nohit_terminal_receipt_invalid")
    facts = row.get("typed_facts") or {}
    if (facts.get("evidence_error") or facts.get("tool_error") not in (None, False)
            or facts.get("fixture") is not False
            or not all(facts.get(k) is True for k in (
                "diagnostic_valid", "execution_evidence", "source_provenance_qualified",
                "environment_healthy", "process_healthy", "preconditions_reached", "endpoint_identity_complete"))):
        raise ValueError("topology_nohit_terminal_evidence_incomplete")
    application = facts.get("application_audit") or {}
    if (application.get("canonical_receipt_matches") is not True
            or application.get("mutation_receipt") != receipt
            or application.get("status") not in {"incomplete", "application_observed"}
            or application.get("execution_evidence") is not True
            or application.get("fixture") is not False
            or application.get("raw_path") != runtime.get("raw_path")
            or application.get("raw_sha256") != runtime.get("raw_sha256")):
        raise ValueError("topology_nohit_terminal_application_receipt_invalid")
    verify_raw(runtime["raw_path"], runtime["raw_sha256"])
    events = read_events(runtime["raw_path"])
    if (events != runtime.get("events") or row.get("raw_path") != runtime["raw_path"]
            or row.get("raw_sha256") != runtime["raw_sha256"]
            or any(type(e.get("event_index")) is not int or e["event_index"] != i
                   or e.get("run_id") != config["run_id"] or e.get("domain_id") != config["domain_id"]
                   for i, e in enumerate(events))
            or _one(events, "runtime_end").get("mutation_receipt") != receipt):
        raise ValueError("topology_nohit_terminal_raw_identity_invalid")
    cleanup = runtime.get("cleanup_receipt") or {}
    cleanup_event = _one(events, "topology_cleanup")
    if (cleanup.get("closed") is not True or cleanup.get("survivors") not in (None, [])
            or cleanup.get("errors") not in (None, []) or cleanup_event.get("receipt") != cleanup):
        raise ValueError("topology_nohit_terminal_cleanup_invalid")
    source = config.get("source_manifest") or {}
    if (source.get("source_frozen") is not True or source.get("dirty_behavior_paths") != []
            or not source.get("head") or not source.get("behavior_tree_sha256")
            or row.get("source_manifest") != source
            or row.get("effective_input_sha256") != config.get("effective_input_sha256")):
        raise ValueError("topology_nohit_terminal_source_input_invalid")
    return {"schema": NOHIT_TERMINAL_SCHEMA, "kind": "completed_without_mutation_hit",
            "execution_stage": "reached", "qualified": False,
            "reason_code": "topology_mutation_nohit", "mutation_receipt": receipt,
            "source_sha256": canonical_hash(source), "effective_input_sha256": config["effective_input_sha256"],
            "raw_path": runtime["raw_path"], "raw_sha256": runtime["raw_sha256"],
            "facts_sha256": canonical_hash(facts), "cleanup_sha256": canonical_hash(cleanup),
            "preconditions_sha256": canonical_hash(row.get("precondition_receipts")),
            "diagnostic_oracle_sha256": canonical_hash(row.get("diagnostic_oracle"))}


def ingest_execution(scenario, config, runtime, capsule, *, services=None):
    """Advance only through core gates; failures end as aborted with raw retained."""
    if services is None:
        from r2f.modules.s1.module import resolve_services
        if scenario.target_id == "discovery.endpoint_topology.fuzz_v1":
            services = resolve_services(scenario.target_id, exploratory=True,
                                        runtime_capability=config.get("runtime_capability"))
        else:
            services = resolve_services(scenario.target_id)
    def attach_candidate_assessment(row):
        if scenario.target_id == G01_TARGET:
            # No fixture/old/unfrozen source can authorize candidate admission.
            source = capsule.input.get("source_manifest", {})
            metadata = runtime.get("execution_metadata") or runtime.get("execution") or {}
            source_ok = False
            if (source.get("source_frozen") is True and metadata.get("source") == source
                    and metadata.get("source_after") == source):
                try:
                    from .identity import source_manifest
                    source_ok = source_manifest(Path(source["source_repo"])) == source
                except (ValueError, KeyError, OSError):
                    pass
            qualified = (source_ok and scenario.arm == "mutated"
                         and row.get("execution_stage") == "judged"
                         and row.get("oracle_verdict") == "fail"
                         and not row.get("ingestion_error")
                         and row.get("typed_facts", {}).get("relation_effect_observed") is True)
            row.update(candidate_eligible=qualified, g01_source_qualified=source_ok,
                       feedback_eligible=False, seal_eligible=False)
        if scenario.target_id == "discovery.endpoint_topology.fuzz_v1":
            from .topology_candidate_assessment import ingest_assessment
            return ingest_assessment(scenario, config, capsule, row)
        return row

    record = ExecutionRecord(config["run_id"], scenario)
    audit = {"runtime_status": runtime.get("runtime_status"), "runtime_reason": runtime.get("reason"),
             "actual_run": runtime.get("real_execution") is True,
             "execution_source": "ros2_public_api" if runtime.get("real_execution") is True else "not_executed",
             "source_manifest": capsule.input["source_manifest"], "effective_input_sha256": capsule.input["effective_input_sha256"]}
    events = []
    facts = {}
    oracle = None

    def evaluate_once():
        nonlocal oracle
        if oracle is None:
            oracle, failure = _evaluate_oracle(services, scenario, facts)
            if failure is not None:
                audit.update(tool_error=True, oracle_tool_failure=failure)
        return oracle

    if runtime.get("real_execution") is not True and runtime.get("runtime_status") == "blocked":
        # Preflight never owes endpoint state receipts. Retain its cause even when
        # environment evidence is absent or damaged; report artifact health separately.
        raw_audit = {"available": bool(runtime.get("raw_path") and runtime.get("raw_sha256")), "verified": False}
        try:
            if raw_audit["available"]:
                verify_raw(runtime["raw_path"], runtime["raw_sha256"])
                events = read_events(runtime["raw_path"])
                if runtime.get("events") != events or any(
                        type(e.get("event_index")) is not int or e["event_index"] != i
                        or e.get("run_id") != config["run_id"] or e.get("domain_id") != config["domain_id"]
                        for i, e in enumerate(events)):
                    raise ValueError("blocked environment raw identity/events mismatch")
                ending = _one(events, "runtime_end")
                if ending.get("reason") != runtime.get("reason") or ending.get("real_execution") is not False:
                    raise ValueError("blocked runtime reason not bound to raw")
                raw_audit["verified"] = True
        except (ValueError, KeyError, TypeError, OSError) as exc:
            raw_audit["error"] = str(exc)
        record.abort(runtime.get("reason") or "runtime_preflight_blocked")
        facts = {"runtime_blocked": True, "environment_healthy": False, "process_healthy": False}
        oracle = evaluate_once()
        return attach_candidate_assessment(dict(record.to_dict(), **audit, typed_facts=facts, oracle=oracle,
                    oracle_verdict=oracle["verdict"], rule_id=oracle["rule_id"],
                    raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
                    runtime_execution_metadata=None, environment=runtime.get("environment", {}),
                    observation_receipts=runtime.get("observation_receipts", []), environment_raw_evidence=raw_audit))
    try:
        verify_raw(runtime["raw_path"], runtime["raw_sha256"])
        events = read_events(runtime["raw_path"])
        if runtime.get("events") != events:
            raise ValueError("returned events differ from raw")
        if any(type(event.get("event_index")) is not int or event["event_index"] != index for index, event in enumerate(events)):
            raise ValueError("raw event indexes are not contiguous artifact references")
        if any(e.get("run_id") != config["run_id"] or e.get("domain_id") != config["domain_id"] for e in events):
            raise ValueError("raw run/domain identity mismatch")
        end = _one(events, "runtime_end")
        receipt = MutationReceipt.from_dict(runtime["mutation_receipt"])
        if scenario.target_id == "discovery.endpoint_topology.fuzz_v1":
            audit["mutation_receipt"] = receipt.to_dict()
        if end.get("mutation_receipt") != receipt.to_dict():
            raise ValueError("raw mutation receipt differs from runtime metadata")
        facts, failure = _produce_facts(services, {"events": events, "runtime": runtime, "config": config})
        if failure is not None:
            audit.update(tool_error=True, fact_producer_tool_failure=failure)
            # No primary evaluation is possible without valid producer output.
            # Cache a tool unknown so the existing abort path cannot evaluate it.
            oracle = {"rule_id": (_primary_rule_id(scenario) or "unresolved_primary_rule"), "rule_role": "primary",
                      "verdict": "unknown", "reason_codes": ["fact_producer_tool_failure"],
                      "forbidden_residuals": [], "closure": {}}
            raise ValueError("fact_producer_tool_failure:" + failure["reason"])
        oracle = evaluate_once()
        if audit.get("oracle_tool_failure"):
            raise ValueError("oracle_tool_failure:" + audit["oracle_tool_failure"]["reason"])
        record.advance(ExecutionStage.EXPRESSIBLE, adapter_accepted=True)
        record.advance(ExecutionStage.PLANNED)
        receipts = canonical_receipts(scenario, runtime, events, config=config)
        if scenario.target_id == G01_TARGET:
            configuration = facts.get("configuration_intervention_verified") is True
            effect = facts.get("relation_effect_observed") is True and scenario.arm == "mutated"
            if ((receipt.applied or receipt.hit) and not configuration
                    or receipt.effect_observed is not effect):
                raise ValueError("g01_mutation_receipt_not_backed_by_raw_intervention_or_relation")
        record.advance(ExecutionStage.PRECONDITION_EXECUTED, precondition_receipts=receipts)
        record.advance(ExecutionStage.REACHED)
        neutral = None
        fault_control = None
        if scenario.arm in ("baseline", "transparent"):
            digest = canonical_hash(capsule.input["effective_input"]["final_input"])
            neutral = {"kind": "no_op", "reason": ("actual per-role QoS and raw identity-backed relation observations verified against submitted input"
                       if scenario.target_id == G01_TARGET else "actual reader delay, endpoint QoS, delivery counts and raw primary observations verified against submitted input"),
                       "before_input_sha256": digest, "after_input_sha256": digest}
        elif scenario.arm == "negative":
            actual_fault = _one(events, "fault_control_receipt")
            fault_control = {key: actual_fault.get(key) for key in (
                "fault_id", "applied", "omitted_sequence", "responsibility_layer", "candidate_eligible")}
            fault_control["raw_event_ref"] = actual_fault["event_index"]
            if scenario.target_id == "discovery.interruption_recovery":
                from .recovery_adapter import recovery_fault_receipt
                fault_control = recovery_fault_receipt(events, config)
            elif scenario.target_id == "builtin.discovery_projection_reconciliation":
                from .projection_adapter import projection_fault_receipt
                fault_control = projection_fault_receipt(events, config, runtime["discovery_projection"])
        else:
            if scenario.target_id == "discovery.endpoint_topology.fuzz_v1" and receipt.hit is False:
                # A completed input may terminate at reached without qualifying
                # mutation, oracle, coverage or candidate admission.
                row = dict(record.to_dict(), **audit)
                row.update(domain_id=config["domain_id"], typed_facts=facts,
                           diagnostic_oracle=oracle,
                           oracle={"rule_id": oracle["rule_id"], "rule_role": "primary",
                                   "verdict": "not_applicable", "reason_codes": ["topology_mutation_nohit"],
                                   "forbidden_residuals": [], "closure": {}},
                           oracle_verdict="not_applicable", rule_id=oracle["rule_id"],
                           raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
                           runtime_execution_metadata=runtime.get("execution_metadata") or runtime.get("execution"),
                           observation_receipts=runtime.get("observation_receipts", []),
                           environment=runtime.get("environment", {}),
                           candidate_eligible=False, feedback_eligible=False, seal_eligible=False)
                row["input_terminal"] = topology_nohit_terminal(config, runtime, row)
                return attach_candidate_assessment(row)
            if receipt.applied is True and receipt.hit is not True:
                # Submitted/applied but with no observable effect: keep the
                # terminal state at reached and report mutation_ineffective.
                # This is not a SUT failure and never advances to mutation_applied.
                # A data-plane carrier whose window *did* cover the delivery but
                # observed zero packets is an observation limit, not a mutation
                # that failed to take: report it under its own reason so the two
                # cannot be conflated in the coverage ledger.
                if (facts.get('delivery_inside_mutation_window') is True
                        and facts.get('data_plane_observed') is False):
                    audit["mutation_ineffective"] = True
                    audit["mutation_ineffective_reason"] = (
                        'observation_limit:'
                        + str(facts.get('data_plane_observation_reason')
                              or 'data_plane_not_observed'))
                    audit["observation_limit"] = {
                        'kind': 'data_plane_not_observed_inside_covered_window',
                        'reason': facts.get('data_plane_observation_reason'),
                        'carrier': config.get('environment_mutation', {}).get('kind')
                                   if isinstance(config.get('environment_mutation'), dict) else None,
                    }
                else:
                    audit["mutation_ineffective"] = True
                    audit["mutation_ineffective_reason"] = config.get(
                        "mutation_ineffective_reason", "no_observable_effect")
                row = record.to_dict()
                row.update(audit, domain_id=config["domain_id"], typed_facts=facts,
                           oracle={"rule_id": (_primary_rule_id(scenario) or "unresolved_primary_rule"),
                                   "rule_role": "primary", "verdict": "not_applicable",
                                   "reason_codes": ["mutation_ineffective"],
                                   "forbidden_residuals": [], "closure": {}},
                           oracle_verdict="not_applicable",
                           rule_id=(_primary_rule_id(scenario) or "unresolved_primary_rule"),
                           raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
                           runtime_execution_metadata=runtime.get("execution_metadata") or runtime.get("execution"),
                           observation_receipts=runtime.get("observation_receipts", []),
                           environment=runtime.get("environment", {}))
                return attach_candidate_assessment(row)
            record.advance(ExecutionStage.MUTATION_APPLIED, mutation_receipt=receipt.to_dict())
        if scenario.target_id == G01_TARGET and facts.get("relation_effect_observed") is not True:
            row = dict(record.to_dict(), **audit)
            row.update(domain_id=config["domain_id"], typed_facts=facts, oracle=oracle,
                       oracle_verdict=oracle["verdict"], rule_id=oracle["rule_id"],
                       raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
                       runtime_execution_metadata=runtime.get("execution_metadata") or runtime.get("execution"),
                       observation_receipts=runtime.get("observation_receipts", []),
                       environment=runtime.get("environment", {}),
                       observation_status="unknown_no_relation_effect_proof",
                       observation_unknown_reasons=facts.get("observation_unknown_reasons", oracle["reason_codes"]))
            return attach_candidate_assessment(row)
        if facts.get("evidence_error"):
            raise ValueError(facts["evidence_error"])
        if not all(facts.get(key) is True for key in ("environment_healthy", "process_healthy", "preconditions_reached", "endpoint_identity_complete")):
            raise ValueError("runtime health, preconditions, or identity evidence incomplete")
        effects = {"effects": facts.get("observed_effects", []), "forbidden_residuals": oracle["forbidden_residuals"]}
        evidence = {"mutation_receipt": receipt.to_dict(), "typed_facts": facts, "observed_effect": effects,
                    "raw_path": runtime["raw_path"], "raw_sha256": runtime["raw_sha256"]}
        if neutral:
            evidence["neutral_receipt"] = neutral
        if fault_control is not None:
            evidence.update(control_kind="fault", fault_control_receipt=fault_control)
        record.advance(ExecutionStage.EFFECT_OBSERVED, **evidence)
        record.advance(ExecutionStage.JUDGED, oracle_verdict=oracle["verdict"], rule_id=oracle["rule_id"])
    except (ValueError, KeyError, TypeError, OSError) as exc:
        oracle = evaluate_once()
        if record.execution_stage not in (ExecutionStage.ABORTED, ExecutionStage.JUDGED):
            record.abort(str(exc))
        audit["ingestion_error"] = str(exc)
    row = record.to_dict()
    row.update(audit, domain_id=config["domain_id"], typed_facts=facts, oracle=oracle,
               oracle_verdict=oracle["verdict"], rule_id=oracle["rule_id"],
               raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
               runtime_execution_metadata=runtime.get("execution_metadata") or runtime.get("execution"),
               observation_receipts=runtime.get("observation_receipts", []), environment=runtime.get("environment", {}))
    if "observation" in runtime:
        from .observations import qualify_observation
        row["instrumentation_observation"] = qualify_observation(row, runtime, config, capsule=capsule)
    return attach_candidate_assessment(row)
