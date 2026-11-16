"""Audit runtime tokens and map actual steps to core receipts.

The runtime executes the public API actions. This module does not run a second
state machine: each core receipt names its before/after raw token indexes and
is admitted only after the corresponding runtime predicate and event proof.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
from r2f.core._json import snapshot
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


def facts_from_events(events, runtime, config):
    """Only raw tokens produce primary facts; summaries cannot substitute."""
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
        for role, token in tokens.items():
            if not token.get("endpoint_gid") or token.get("rmw_identifier") != config["rmw"]:
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
    return facts


def canonical_receipts(scenario, runtime, events):
    """Exact 1:1 runtime step mapping; raw before/after events are mandatory."""
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


def _produce_facts(services, evidence):
    """Contain provider faults before facts can enter oracle or persistence."""
    returned = False
    result = None
    try:
        result = services.fact_producer.produce(evidence)
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


def _evaluate_oracle(services, scenario, facts):
    """Contain an oracle tool failure without turning it into a SUT verdict."""
    returned = False
    result = None
    try:
        result = services.oracle.evaluate(scenario, facts)
        returned = True
        if (not isinstance(result, dict)
                or result.get("rule_id") != scenario.target.rule_contract["primary_rule"]
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
        return {"rule_id": scenario.target.rule_contract["primary_rule"], "rule_role": "primary",
                "verdict": "unknown", "reason_codes": ["oracle_tool_failure"],
                "forbidden_residuals": [], "closure": {}}, diagnostic


def ingest_execution(scenario, config, runtime, capsule, *, services=None):
    """Advance only through core gates; failures end as aborted with raw retained."""
    if services is None:
        from r2f.modules.s1.module import resolve_services
        services = resolve_services(scenario.target_id)
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
        return dict(record.to_dict(), **audit, typed_facts=facts, oracle=oracle,
                    oracle_verdict=oracle["verdict"], rule_id=oracle["rule_id"],
                    raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
                    runtime_execution_metadata=None, environment=runtime.get("environment", {}),
                    observation_receipts=runtime.get("observation_receipts", []), environment_raw_evidence=raw_audit)
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
        if end.get("mutation_receipt") != receipt.to_dict():
            raise ValueError("raw mutation receipt differs from runtime metadata")
        facts, failure = _produce_facts(services, {"events": events, "runtime": runtime, "config": config})
        if failure is not None:
            audit.update(tool_error=True, fact_producer_tool_failure=failure)
            # No primary evaluation is possible without valid producer output.
            # Cache a tool unknown so the existing abort path cannot evaluate it.
            oracle = {"rule_id": scenario.target.rule_contract["primary_rule"], "rule_role": "primary",
                      "verdict": "unknown", "reason_codes": ["fact_producer_tool_failure"],
                      "forbidden_residuals": [], "closure": {}}
            raise ValueError("fact_producer_tool_failure:" + failure["reason"])
        oracle = evaluate_once()
        if audit.get("oracle_tool_failure"):
            raise ValueError("oracle_tool_failure:" + audit["oracle_tool_failure"]["reason"])
        record.advance(ExecutionStage.EXPRESSIBLE, adapter_accepted=True)
        record.advance(ExecutionStage.PLANNED)
        receipts = canonical_receipts(scenario, runtime, events)
        record.advance(ExecutionStage.PRECONDITION_EXECUTED, precondition_receipts=receipts)
        record.advance(ExecutionStage.REACHED)
        neutral = None
        fault_control = None
        if scenario.arm in ("baseline", "transparent"):
            digest = canonical_hash(capsule.input["effective_input"]["final_input"])
            neutral = {"kind": "no_op", "reason": "actual reader delay, endpoint QoS, delivery counts and raw primary observations verified against submitted input", "before_input_sha256": digest, "after_input_sha256": digest}
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
            if receipt.applied is True and receipt.hit is not True:
                # Submitted/applied but with no observable effect: keep the
                # terminal state at reached and report mutation_ineffective.
                # This is not a SUT failure and never advances to mutation_applied.
                audit["mutation_ineffective"] = True
                audit["mutation_ineffective_reason"] = config.get(
                    "mutation_ineffective_reason", "no_observable_effect")
                row = record.to_dict()
                row.update(audit, domain_id=config["domain_id"], typed_facts=facts,
                           oracle={"rule_id": scenario.target.rule_contract["primary_rule"],
                                   "rule_role": "primary", "verdict": "not_applicable",
                                   "reason_codes": ["mutation_ineffective"],
                                   "forbidden_residuals": [], "closure": {}},
                           oracle_verdict="not_applicable",
                           rule_id=scenario.target.rule_contract["primary_rule"],
                           raw_path=runtime.get("raw_path"), raw_sha256=runtime.get("raw_sha256"),
                           runtime_execution_metadata=runtime.get("execution_metadata") or runtime.get("execution"),
                           observation_receipts=runtime.get("observation_receipts", []),
                           environment=runtime.get("environment", {}))
                return row
            record.advance(ExecutionStage.MUTATION_APPLIED, mutation_receipt=receipt.to_dict())
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
    return row
