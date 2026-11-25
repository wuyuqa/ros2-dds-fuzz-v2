"""Opt-in topology evidence adapter. Artifact processing only; never starts ROS.

Persisted bytes, not ``runtime.facts``/pair.status, are authoritative. Missing
application/state evidence remains unknown. State abstraction belongs to the
shared topology projector; this adapter only binds typed facts/step receipts to
its refs and independently audits program, action and worker lineage.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.core._json import snapshot
from r2f.core.evidence import canonical_hash
from r2f.core.mutation import MutationReceipt
from r2f.core.state_machine import StepReceipt
from r2f.modules.s1.topology_input import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, TOPOLOGY_FACT_PRODUCER,
    TOPOLOGY_FACT_EXECUTION, TOPOLOGY_FACT_HEALTH, TOPOLOGY_FACT_IDENTITY,
    TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY, TOPOLOGY_FACT_GUARDS,
)
from .topology_state_projection import project_topology_state

SCHEMA = "r2f.s1-topology-facts/v1"
_WORKER_EVENTS = {"endpoint_created", "endpoint_create_begin", "worker_ready", "send", "receive",
                  "match_snapshot", "graph_snapshot", "command_done", "worker_shutdown",
                  "concurrent_publish_ready", "concurrent_publish_started", "concurrent_publish_finished",
                  "worker_command_requested", "topology_snapshot_requested", "liveliness_asserted"}
_TOOL_EVENTS = {"worker_error", "worker_command_error", "worker_unexpected_exit", "runtime_abort",
                "owned_worker_cleanup_signal", "topology_tool_error"}
_LINEAGE = ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _text(value):
    return isinstance(value, str) and bool(value)


def _object(pairs):
    out = {}
    for key, value in pairs:
        _require(key not in out, "raw_duplicate_json_key:" + key)
        out[key] = value
    return out


def _constant(value):
    raise ValueError("raw_nonfinite_json:" + value)


def _one(events, kind, **fields):
    rows = [e for e in events if e.get("event") == kind
            and all(e.get(k) == v for k, v in fields.items())]
    _require(len(rows) == 1, "raw_missing_or_duplicate:" + kind + ":" + str(fields))
    return rows[0]


def _chain(*events):
    _require(all(a["event_index"] < b["event_index"] and a["monotonic_ns"] < b["monotonic_ns"]
                 for a, b in zip(events, events[1:])), "raw_chronology_invalid")


def _assert_api_event(inside, action, receipt, token, begin, after, done):
    """Bind the legacy API action to its writer and command, not DDS recovery.

    Current offline fake lacks command IDs/ACKs and therefore cannot qualify.
    """
    _require(isinstance(token, dict), "action_assert_writer_token_missing")
    result = receipt.get("after", {}).get("assert")
    _require(isinstance(result, dict), "action_assert_result_missing")
    command_id = result.get("command_id")
    _require(_integer(command_id, 1), "action_assert_command_id_missing")
    _require(result.get("api") == "rclpy.publisher.Publisher.assert_liveliness",
             "action_assert_api_missing_or_unverified")
    _require(token.get("logical_id") == action.get("endpoint_id")
             and token.get("generation") == action.get("generation"),
             "action_assert_writer_identity_mismatch")
    rows = [e for e in inside if e.get("event") == "liveliness_asserted"]
    _require(len(rows) == 1, "action_assert_raw_missing_or_ambiguous")
    row = rows[0]
    _require(row.get("command_id") == command_id and row.get("api") == result["api"],
             "action_assert_command_or_api_mismatch")
    _require(all(row.get(k) is not None and row.get(k) == token.get(k) for k in _LINEAGE)
             and _text(token.get("endpoint_gid"))
             and row.get("endpoint_gid") == token["endpoint_gid"],
             "action_assert_writer_identity_mismatch")
    acknowledgements = [e for e in inside if e.get("event") == "command_done"
                        and e.get("command_id") == command_id
                        and all(e.get(k) is not None and e.get(k) == token.get(k) for k in _LINEAGE)]
    _require(len(acknowledgements) == 1, "action_assert_command_done_missing_or_ambiguous")
    ack = acknowledgements[0]
    # Controller and worker timestamps can only be ordered across processes
    # when every relevant event identifies the same boot/time namespace.
    requests = [e for e in inside if e.get("event") == "topology_assert_liveliness_requested"]
    writes = [e for e in inside if e.get("event") == "topology_assert_liveliness_write_result"]
    _require(len(requests) == len(writes) == 1, "action_assert_request_or_write_missing_or_ambiguous")
    request, write = requests[0], writes[0]
    for event in (request, write):
        _require(event.get("command") == "assert_liveliness"
                 and event.get("command_id") == command_id
                 and all(event.get(k) is not None and event.get(k) == token.get(k) for k in _LINEAGE)
                 and event.get("endpoint_gid") == token.get("endpoint_gid"),
                 "action_assert_request_or_write_binding_mismatch")
    _require(write.get("status") == "flushed" and write.get("error_type") is None
             and _integer(write.get("bytes_expected"), 1)
             and type(write.get("bytes_written")) is int
             and write["bytes_written"] == write["bytes_expected"],
             "action_assert_command_write_not_successful")
    chain_events = (token, begin, request, write, row, ack, after, done)
    clock = row.get("monotonic_clock_identity")
    _require(isinstance(clock, dict) and _text(clock.get("boot_id"))
             and _integer(clock.get("device")) and _integer(clock.get("inode"), 1)
             and all(event.get("monotonic_clock_identity") == clock for event in chain_events),
             "action_assert_monotonic_clock_identity_missing_or_mismatch")
    _require(request.get("request_clock_identity") == clock
             and write.get("result_clock_identity") == clock
             and _integer(request.get("request_monotonic_ns"), 1)
             and _integer(write.get("result_monotonic_ns"), 1)
             and request["request_monotonic_ns"] <= request["monotonic_ns"]
             and write["result_monotonic_ns"] <= write["monotonic_ns"]
             and request["request_monotonic_ns"] < row["monotonic_ns"]
             and request["request_monotonic_ns"] < write["result_monotonic_ns"],
             "action_assert_request_or_write_clock_invalid")
    _chain(token, begin, request, row, ack, after, done)
    _chain(request, write, after)
    return row


def _pointer(value):
    return value.replace("~", "~0").replace("/", "~1")


class _Audit:
    def __init__(self, events, runtime, config):
        self.runtime, self.config = runtime, config
        _require(config.get("target_id") == TOPOLOGY_TARGET, "topology_exact_target_required")
        _require(config.get("runtime_capability") == TOPOLOGY_CAPABILITY, "topology_opt_in_required")
        self.path = str(runtime["raw_path"])
        data = Path(self.path).read_bytes()
        self.sha = hashlib.sha256(data).hexdigest()
        _require(self.sha == runtime.get("raw_sha256"), "raw_sha256_mismatch")
        self.events = [json.loads(line, object_pairs_hook=_object, parse_constant=_constant)
                       for line in data.decode().splitlines()]
        _require(bool(self.events), "raw_events_empty")
        _require(self.events == events and runtime.get("events", self.events) == self.events,
                 "raw_events_differ_from_returned_events")
        for i, e in enumerate(self.events):
            _require(isinstance(e, dict) and type(e.get("event_index")) is int
                     and e["event_index"] == i and _text(e.get("event"))
                     and _integer(e.get("monotonic_ns"), 1) and _integer(e.get("time_ns"), 1),
                     "raw_event_identity_or_clock_invalid")
            _require(e.get("run_id") == config.get("run_id") and _text(e.get("run_id"))
                     and _integer(e.get("domain_id")) and e["domain_id"] == config.get("domain_id"),
                     "raw_run_domain_mismatch")
        from r2f.runtime.topology_route import validate_topology_route
        self.plan = validate_topology_route(config)
        from r2f.instrumentation.fastdds_peer.receipt import validate_execution_mode
        self.native_peer_reference = validate_execution_mode(self.events, config, raw_path=self.path)
        # validate_topology_route explicitly invokes validate_topology_plan,
        # including deterministic input-layer lineage; do not regenerate input.
        self.layers = snapshot([op.parameters["input_layer_receipts"][0] for op in self.plan.operations])
        _require(config.get("input_layer_receipts") == self.layers, "input_layer_lineage_mismatch")
        _require(config.get("plan_hash") == self.plan.plan_hash(), "plan_hash_mismatch")
        _require(config.get("final_input") == self.plan.final_config
                 and set(self.plan.final_config) == {"topology_program"}, "final_program_mismatch")
        self.program = self.plan.final_config["topology_program"]
        from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_declarations
        validate_declarations(self.events, self.program, self.native_peer_reference)
        self.program_sha = canonical_hash(self.program)
        binding = _one(self.events, "input_binding")
        self.binding = binding
        route = binding.get("receipt", {}).get("route_binding", {})
        expected_binding = {"schema": "r2f.topology-route-binding/v1", "target_id": TOPOLOGY_TARGET,
            "capability": TOPOLOGY_CAPABILITY, "plan_hash": self.plan.plan_hash(),
            "final_input_sha256": canonical_hash(self.plan.final_config),
            "input_layer_receipts_sha256": canonical_hash(self.layers),
            "effective_input_sha256": config.get("effective_input_sha256")}
        _require(route == expected_binding, "raw_route_input_lineage_mismatch")
        _require(binding.get("receipt", {}).get("program_sha256") == self.program_sha,
                 "raw_program_binding_mismatch")
        self.end = _one(self.events, "runtime_end")
        _chain(binding, self.end)
        self.mutation = MutationReceipt.from_dict(runtime["mutation_receipt"])
        _require(self.end.get("mutation_receipt") == self.mutation.to_dict(), "raw_mutation_receipt_mismatch")
        self.begin = _one(self.events, "mutation_begin")
        _chain(binding, self.begin, self.end)
        _require(self.begin.get("mutation_kind") == "endpoint_program"
                 and self.begin.get("route_binding") == route
                 and self.begin.get("input_binding_ref") == binding["event_index"],
                 "raw_mutation_begin_input_binding_mismatch")
        provenance = [e for e in self.events if e["event"] == "topology_provenance"]
        prepared = [e for e in self.events if e["event"] == "topology_backend_prepared"]
        self.fixture = (len(provenance) != 1 or provenance[0].get("runtime_kind") != "ros2_public_api"
                        or provenance[0].get("fixture") is not False
                        or len(prepared) != 1 or prepared[0].get("real_ros") is not True
                        or runtime.get("real_execution") is not True
                        or any(e.get("fixture") is True or e.get("offline_fake") is True
                               or e.get("backend") not in (None, "ros2_public_api") for e in self.events))
        self.specs = {e["logical_id"]: e for e in self.program["endpoints"]}
        from r2f.runtime.participant_groups import enabled, destroy_receipts
        self.group_lineage = None
        self.group_destroys = []
        if enabled(self.program):
            self.group_lineage, self.group_destroys = destroy_receipts(
                self.program, self.events, self.path, config=config)
            _require(runtime.get('execution_metadata', {}).get('participant_groups') == self.group_lineage,
                     'group_lineage_metadata_mismatch')
        elif (runtime.get('execution_metadata', {}).get('schema') == 'r2f.topology-execution-metadata/v2'
              or 'participant_groups' in runtime.get('execution_metadata', {})):
            _require(False, 'group_metadata_without_sealed_opt_in')
        self.tokens = {}
        self.action_receipts = []

    def ref(self, event):
        return {"path": self.path, "sha256": self.sha, "event_index": event["event_index"]}

    def resolve(self, ref):
        if isinstance(ref, dict):
            _require(set(ref) == {"path", "sha256", "event_index"} and ref["path"] == self.path
                     and ref["sha256"] == self.sha, "raw_reference_identity_mismatch")
            ref = ref["event_index"]
        _require(_integer(ref) and ref < len(self.events), "raw_reference_out_of_range")
        return self.events[ref]

    def identity(self):
        retired, seen_gids, seen_workers = set(), set(), set()
        destroys = getattr(self, "group_destroys", ())
        destroy_acks = {r["ack_ref"] for r in destroys}
        destroy_controllers = {r["controller_ref"] for r in destroys}
        for event in self.events:
            kind, eid = event["event"], event.get("logical_id")
            if kind == "endpoint_created":
                gen = event.get("generation")
                _require(eid in self.specs and _integer(gen, 1), "endpoint_logical_generation_missing")
                key = (eid, gen)
                _require(key not in self.tokens, "endpoint_generation_duplicate")
                previous = [(k, v) for k, v in self.tokens.items() if k[0] == eid]
                _require(gen == len(previous) + 1 and all(k in retired for k, _ in previous),
                         "endpoint_generation_missing_retirement")
                _require(any(a.get("endpoint_id") == eid and a.get("generation") == gen
                             and a.get("op") in {"create", "recreate"} for a in self.program["actions"]),
                         "endpoint_generation_not_declared")
                _require(_integer(event.get("pid"), 1) and _integer(event.get("process_start_ticks"), 1)
                         and _text(event.get("endpoint_generation")) and _text(event.get("endpoint_gid")),
                         "worker_identity_incomplete")
                worker = (event["pid"], event["process_start_ticks"], event["endpoint_generation"])
                _require(event["endpoint_gid"] not in seen_gids and worker not in seen_workers,
                         "worker_or_gid_reused_across_generations")
                seen_gids.add(event["endpoint_gid"])
                seen_workers.add(worker)
                spec = self.specs[eid]
                _require(event.get("topic") == spec["topic"] and event.get("message_type") == spec.get("type_name"),
                         "endpoint_actual_topic_type_mismatch")
                # This is the public create-call receipt, not graph/default substitution.
                from r2f.runtime.topology_worker import endpoint_expressibility
                public_input = endpoint_expressibility(spec, self.config)["public_qos"]
                actual_qos = event.get("qos")
                if "actual_endpoint_qos" in event:
                    from r2f.runtime.qos_binding import validate_created_qos
                    _require(not self.fixture, "fixture_cannot_claim_public_actual_qos")
                    validate_created_qos(event, public_input)
                else:
                    # Existing raw keeps its original exact reflection contract.
                    _require(actual_qos == public_input or self.fixture and actual_qos == spec.get("qos"),
                             "endpoint_actual_qos_mismatch")
                self.tokens[key] = event
            elif kind in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
                key = (eid, event.get("generation"))
                if event["event_index"] in destroy_controllers and key in retired:
                    continue  # Same authenticated cleanup, not a second retirement.
                _require(key in self.tokens and key not in retired, "retirement_unknown_generation")
                token = endpoint_identity_at(self.events, self.tokens[key], before_index=event["event_index"] + 1)
                _require(event["monotonic_ns"] >= token["identity_valid_after_ns"],
                         "retirement_before_identity_ack")
                _require(event.get("pid") == token["pid"] and event.get("endpoint_gid") == token["endpoint_gid"]
                         and event.get("cleanup_status") == "closed", "retirement_identity_or_cleanup_mismatch")
                _chain(token, event)
                retired.add(key)
            elif kind == "endpoint_identity_transition":
                key = (eid, event.get("generation"))
                _require(key in self.tokens and key not in retired, "transition_unknown_or_retired_generation")
                _require(event.get("after_gid") not in seen_gids, "worker_or_gid_reused_across_generations")
                seen_gids.add(event.get("after_gid"))
            elif kind in _WORKER_EVENTS and eid is not None and kind not in {"endpoint_create_begin", "worker_ready"}:
                key = (eid, event.get("generation"))
                _require(key in self.tokens and (key not in retired or event["event_index"] in destroy_acks),
                         "worker_event_unknown_or_retired_generation")
                self.bound(event, self.tokens[key])
        for event in self.events:
            if event["event"] in {"endpoint_create_begin", "worker_ready"}:
                token = self.tokens.get((event.get("logical_id"), event.get("generation")))
                _require(token is not None and all(event.get(k) == token[k] for k in _LINEAGE),
                         "worker_start_lineage_mismatch")
                _chain(event, token)
        _require(bool(self.tokens) or not self.specs, "endpoint_creation_raw_missing")
        # Summaries describe the authenticated final incarnation; tokens remain
        # immutable creations so earlier windows can resolve their own prefix.
        identities = []
        for token in self.tokens.values():
            current = endpoint_identity_at(self.events, token)
            identities.append({**{k: current[k] for k in _LINEAGE},
                **{k: current[k] for k in ("endpoint_gid", "incarnation_ordinal", "identity_transition_ref")},
                "creation_ref": self.ref(token)})
        return identities

    def bound(self, event, token):
        token = endpoint_identity_at(self.events, token, before_index=event["event_index"] + 1)
        valid_ref = token["identity_valid_after_ref"]
        _require(event["monotonic_ns"] >= token["identity_valid_after_ns"]
                 and (event["event_index"] >= valid_ref if event.get("event") == "command_done"
                      else event["event_index"] > valid_ref), "worker_event_before_identity_ack")
        _require(all(event.get(k) == token[k] for k in _LINEAGE), "worker_event_lineage_mismatch")
        _require(event["event_index"] > token["event_index"] and event["monotonic_ns"] >= token["monotonic_ns"],
                 "worker_event_before_creation")
        if "endpoint_gid" in event:
            _require(event["endpoint_gid"] == token["endpoint_gid"], "worker_endpoint_gid_mismatch")
        if event.get("event") == "send":
            _require(event.get("writer_gid") == token["endpoint_gid"] and event.get("publish_returned") is True,
                     "send_gid_or_publish_return_missing")
        if event.get("event") == "receive":
            _require(event.get("reader_gid") == token["endpoint_gid"], "receive_reader_gid_mismatch")

    def action_before(self, action, begin, *, require_success=True):
        """Replay only physical lifecycle/publish receipts, not predicted graph."""
        current = {eid: {"generation": 0, "alive": False, "next_sequence": 0} for eid in self.specs}
        for event in self.events[:begin["event_index"]]:
            eid = event.get("logical_id")
            if eid not in current:
                continue
            state = current[eid]
            if event["event"] == "endpoint_created":
                state.update(generation=event["generation"], alive=True, next_sequence=0)
            elif event["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
                state["alive"] = False
            elif event["event"] == "send":
                state["next_sequence"] = event["send_sequence"] + 1
        eid = action.get("endpoint_id")
        expected = current[eid] if eid is not None else {"alive": sorted(k for k, v in current.items() if v["alive"])}
        _require(begin.get("before") == expected, "action_before_not_observed_lifecycle")
        op, gen = action["op"], action.get("generation")
        if eid is not None and require_success:
            state = current[eid]
            if op == "create":
                _require(not state["alive"] and state["generation"] == 0 and gen == 1, "action_create_requires_failed")
            elif op == "recreate":
                _require(gen == state["generation"] + 1 and state["generation"] > 0, "action_recreate_requires_failed")
            else:
                _require(state["alive"] and state["generation"] == gen, "action_live_generation_requires_failed")
            if op == "publish_batch":
                _require(self.specs[eid]["role"] == "writer" and action["params"]["start_sequence"] >= state["next_sequence"],
                         "action_writer_sequence_requires_failed")
        return expected

    def rejected_action(self, action, begin, done, receipt, physical_before):
        """Authenticate only two pre-side-effect rejections, never API success.

        Other capability/backend errors can follow partial effects and remain
        unknown. In particular, recreate may already have destroyed its predecessor.
        """
        aid, eid, gen = action["action_id"], action["endpoint_id"], action["generation"]
        _require(canonical_hash(begin.get("before")) == canonical_hash(physical_before),
                 "action_guard_before_not_raw_backed")
        _require(eid in self.specs and action.get("params", {}).get("concurrency_group") is None,
                 "action_guard_scope_not_supported")
        op = action["op"]
        if op == "create" and receipt.get("disposition") == "capability_reject":
            _require(gen == 1 and physical_before == {"generation": 0, "alive": False, "next_sequence": 0},
                     "action_guard_create_state_invalid")
            from r2f.runtime.topology_worker import endpoint_expressibility, TopologyUnsupported
            try:
                endpoint_expressibility(self.specs[eid], self.config)
            except TopologyUnsupported as exc:
                _require(exc.code == "qos_not_expressible", "action_guard_capability_not_supported")
                code, kind, message, details = exc.code, "capability", str(exc), exc.details
            else:
                raise ValueError("action_guard_capability_not_reproduced")
        elif op == "publish_batch" and receipt.get("disposition") == "guard_reject":
            _require(not (physical_before["alive"] and physical_before["generation"] == gen
                          and self.specs[eid]["role"] == "writer"),
                     "action_guard_writer_predicate_not_reproduced")
            code, kind = "writer_generation_alive_guard", "guard"
            message, details = "publish_batch requires a live writer generation", {}
        else:
            raise ValueError("action_guard_not_independently_supported")
        expected = {"step_id": "action:" + aid, "receipt_kind": "precondition", "action": action,
            "requires": [{"id": code, "status": "unsatisfied", "kind": kind}],
            "code": code, "kind": kind, "before": physical_before, "after": physical_before,
            "satisfied": False, "disposition": "capability_reject" if kind == "capability" else "guard_reject",
            "method": "runtime_guard_before_side_effect", "no_side_effect": True,
            "message": message, "details": details, "execution_evidence": False}
        _require(canonical_hash(receipt) == canonical_hash(expected), "action_guard_terminal_not_recomputed")
        before = _one(self.events, "state_step_before", step_id=expected["step_id"])
        guard = _one(self.events, "topology_guard_reject", step_id=expected["step_id"])
        after = _one(self.events, "state_step_after", step_id=expected["step_id"])
        _chain(begin, before, guard, after, done)
        # Compare JSON values rather than Python equality: bool is not an integer receipt.
        _require(canonical_hash({k: before.get(k) for k in
                    ("step_id", "action", "before", "after", "requires", "satisfied")}) == canonical_hash({
                    "step_id": expected["step_id"], "action": action, "before": physical_before,
                    "after": None, "requires": [], "satisfied": None}), "action_guard_before_not_raw_backed")
        guard_fields = ("step_id", "code", "kind", "message", "no_side_effect", "details", "before")
        _require(all(k in guard for k in guard_fields)
                 and canonical_hash({k: guard[k] for k in guard_fields}) == canonical_hash({k: expected[k] for k in guard_fields})
                 and all(k in after for k in expected)
                 and canonical_hash({k: after[k] for k in expected}) == canonical_hash(expected),
                 "action_guard_receipt_not_raw_backed")
        envelope = {"event", "event_index", "run_id", "domain_id", "monotonic_ns", "time_ns",
                    "pid", "pgid", "role", "backend", "monotonic_clock_identity"}
        _require(set(before) == envelope | {"step_id", "action", "before", "after", "requires", "satisfied", "fixture"}
                 and set(guard) == envelope | set(guard_fields)
                 and set(after) == envelope | set(expected) | {"fixture"}
                 and before.get("fixture") is self.fixture and after.get("fixture") is self.fixture,
                 "action_guard_receipt_not_raw_backed")
        controller = ("pid", "pgid", "role", "backend", "monotonic_clock_identity")
        _require(_integer(begin.get("pid"), 1) and _text(begin.get("role"))
                 and all(begin.get(k) is not None for k in controller)
                 and all(canonical_hash({k: e.get(k) for k in controller}) ==
                         canonical_hash({k: begin[k] for k in controller})
                         for e in (self.begin, before, guard, after, done)), "action_guard_controller_mismatch")
        # This narrow branch accepts no intervening events at all. Asynchronous
        # observations/concurrent commands require their own attribution, not a waiver.
        _require(self.events[begin["event_index"] + 1:done["event_index"]] == [before, guard, after],
                 "action_guard_side_effect_or_unattributed_event")
        next_begin = min((e["event_index"] for e in self.events[done["event_index"] + 1:]
                          if e["event"] == "topology_action_begin" and e.get("action", {}).get("endpoint_id") == eid),
                         default=self.end["event_index"])
        _require(not any(any(e.get(k) == eid for k in ("logical_id", "endpoint_id", "writer", "reader"))
                         for e in self.events[done["event_index"] + 1:next_begin]),
                 "action_guard_effect_outside_terminal")
        accepted_indices = {e["event_index"] for e in (begin, before, guard, after, done)}
        _require(not any(e["event_index"] not in accepted_indices and
                         (e.get("action_id") == aid or e.get("phase") == aid or e.get("step_id") == expected["step_id"])
                         for e in self.events), "action_guard_trace_outside_terminal")
        return before, guard, after

    def actions(self):
        begins = [e for e in self.events if e["event"] == "topology_action_begin"]
        dones = [e for e in self.events if e["event"] == "topology_action_done"]
        _require([e.get("action") for e in begins] == self.program["actions"]
                 and [e.get("action") for e in dones] == self.program["actions"],
                 "action_trace_extra_missing_or_reordered")
        previous = self.begin
        for i, action in enumerate(self.program["actions"]):
            aid = action["action_id"]
            begin = _one(self.events, "topology_action_begin", action_id=aid)
            done = _one(self.events, "topology_action_done", action_id=aid)
            receipt = done.get("receipt")
            rejected = isinstance(receipt, dict) and receipt.get("disposition") in {"guard_reject", "capability_reject"}
            physical_before = self.action_before(action, begin, require_success=not rejected)
            group = action.get("params", {}).get("concurrency_group")
            prev_group = (self.program["actions"][i - 1].get("params", {}).get("concurrency_group") if i else None)
            if group is not None and group == prev_group:
                from r2f.runtime.topology_concurrency import audit_concurrency
                audit_concurrency(self.events, self.program["actions"], group)
                _chain(begin, done)
                _chain(previous, done)
            else:
                _chain(previous, begin, done)
            _require(begin.get("action") == action and done.get("action") == action
                     and begin.get("action_index") == i and done.get("action_index") == i,
                     "action_program_binding_mismatch")
            receipt = done.get("receipt")
            _require(begin.get("before") == (receipt.get("before") if isinstance(receipt, dict) else None),
                     "action_begin_before_state_mismatch")
            _require(isinstance(receipt, dict) and receipt.get("action") == action
                     and receipt.get("step_id") == "action:" + aid, "action_terminal_receipt_missing")
            _require(isinstance(receipt.get("before"), dict) and isinstance(receipt.get("after"), dict)
                     and isinstance(receipt.get("requires"), list) and bool(receipt["requires"])
                     and all(isinstance(r, dict) and _text(r.get("id"))
                             and r.get("status") in {"satisfied", "unsatisfied", "unknown"}
                             for r in receipt["requires"]), "action_state_or_requires_missing")
            if rejected:
                before, guard, after = self.rejected_action(action, begin, done, receipt, physical_before)
                self.action_receipts.append({"action_id": aid, "before": snapshot(receipt["before"]),
                    "after": snapshot(receipt["after"]), "requires": snapshot(receipt["requires"]),
                    "terminal": receipt["disposition"], "raw_begin": self.ref(begin), "raw_after": self.ref(after),
                    "raw_done": self.ref(done), "raw_state_before": self.ref(before), "raw_guard": self.ref(guard)})
                previous = done
                continue
            allowed_terminals = {"applied", "effect_observed", "observed", "satisfied"}
            if action["op"] == "update_filter":
                # Only this action may claim configured, and only after its
                # independent raw command/configuration/state proof is checked.
                from r2f.runtime.filter_action_receipt import filter_action_evidence
                evidence = filter_action_evidence(action, self.events, before_index=done["event_index"])
                _require(receipt.get("filter_action_evidence") == evidence
                         and receipt["requires"] == evidence["requires"]
                         and receipt.get("disposition") == evidence["disposition"]
                         and receipt.get("execution_evidence") is False
                         and receipt.get("filter_effect_observed") is None
                         and receipt.get("filter_update_receipt") ==
                             self.events[evidence["update_ref"]].get("receipt"),
                         "action_filter_state_not_raw_backed")
                _require(begin["event_index"] < evidence["api_call_refs"][0]
                         and evidence["ack_ref"] < evidence["update_ref"] < done["event_index"],
                         "action_filter_command_outside_action")
                allowed_terminals.add("configured")
            _require(receipt.get("satisfied") is True and all(r["status"] == "satisfied" for r in receipt["requires"])
                     and receipt.get("disposition") in allowed_terminals,
                     "action_requires_or_terminal_not_reached")
            after = _one([e for e in self.events if begin["event_index"] < e["event_index"] < done["event_index"]],
                         "state_step_after", step_id="action:" + aid)
            _require(after.get("before") == receipt["before"] and after.get("after") == receipt["after"]
                     and after.get("satisfied") is True, "action_state_receipt_not_raw_backed")
            # A raw receipt is not itself proof of an API call. Check lifecycle / command effect.
            inside = [e for e in self.events if begin["event_index"] < e["event_index"] < done["event_index"]]
            op = action["op"]
            if op in {"create", "recreate"}:
                created = _one(inside, "endpoint_created", logical_id=action["endpoint_id"], generation=action["generation"])
                _require(receipt["after"].get("alive") is True and all(receipt["after"].get(k) == created[k]
                         for k in ("generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")),
                         "action_created_after_identity_mismatch")
            elif op == "destroy":
                _require(any(e["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                             and e.get("logical_id") == action["endpoint_id"]
                             and e.get("generation") == action["generation"] for e in inside), "action_destroy_raw_missing")
            elif op == "publish_batch":
                params = action["params"]
                if params.get("concurrency_group") is not None:
                    from r2f.runtime.topology_concurrency import audit_concurrency
                    audit_concurrency(self.events, self.program["actions"], params["concurrency_group"])
                    inside = [e for e in self.events if e.get("phase") == aid]
                sends = [e for e in inside if e["event"] == "send" and e.get("logical_id") == action["endpoint_id"]
                         and e.get("generation") == action["generation"]]
                _require([e.get("send_sequence") for e in sends] == list(range(params["start_sequence"], params["start_sequence"] + params["count"])),
                         "action_publish_sequence_binding_mismatch")
                _require(receipt["after"].get("generation") == action["generation"]
                         and receipt["after"].get("alive") is True
                         and receipt["after"].get("next_sequence") == params["start_sequence"] + params["count"],
                         "action_publish_after_state_mismatch")
            elif op in {"snapshot", "barrier"}:
                _require(any(e["event"] == "command_done" for e in inside), "action_snapshot_ack_missing")
            elif op == "update_filter":
                _require(self.specs[action["endpoint_id"]].get("role") == "reader"
                         and receipt["after"] == {**receipt["before"], "filter_update_raw_ref": evidence["update_ref"]}
                         and evidence["update_ref"] < after["event_index"],
                         "action_filter_after_state_mismatch")
            elif op == "update_participant_userdata":
                _require(self.specs[action["endpoint_id"]].get("role") == "writer"
                         and receipt["before"].get("alive") is True
                         and receipt["before"].get("generation") == action["generation"],
                         "action_refresh_requires_current_live_writer")
                from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_action
                validate_action(self.events, action, receipt, begin, done, self.native_peer_reference, raw_path=self.path)
            elif op == "assert_liveliness":
                _require(self.specs[action["endpoint_id"]].get("role") == "writer",
                         "action_assert_requires_writer")
                token = self.tokens.get((action["endpoint_id"], action["generation"]))
                if token is not None:
                    token = endpoint_identity_at(self.events, token, before_index=begin["event_index"])
                _assert_api_event(inside, action, receipt, token, begin, after, done)
            self.action_receipts.append({"action_id": aid, "before": snapshot(receipt["before"]),
                "after": snapshot(receipt["after"]), "requires": snapshot(receipt["requires"]),
                "terminal": receipt["disposition"], "raw_begin": self.ref(begin), "raw_after": self.ref(after),
                "raw_done": self.ref(done)})
            previous = done
        _require({e["event_index"] for e in self.events if e["event"] == "topology_guard_reject"}
                 == {r["raw_guard"]["event_index"] for r in self.action_receipts if "raw_guard" in r},
                 "action_guard_unbound_or_duplicate")
        _chain(previous, self.end)
        return self.action_receipts

    def delivery_identity(self, event, sends):
        """Per-sample attribution, including the explicit NON-direct-GID basis."""
        token = self.tokens.get((event.get("logical_id"), event.get("generation")))
        _require(token is not None, "receive_generation_unbound")
        self.bound(event, token)
        _require(event.get("payload_run_id") == self.config["run_id"], "receive_payload_old_run")
        from .topology_sample_attribution import native_delivery_identity
        native = native_delivery_identity(event, sends, self.events, ref=self.ref)
        if native is not None:
            sent = self.resolve(native["send_ref"])
            writer = self.tokens[(sent["logical_id"], sent["generation"])]
            self.bound(sent, writer)
            _require(self.specs[sent["logical_id"]]["role"] == "writer"
                     and self.specs[event["logical_id"]]["role"] == "reader", "delivery_role_mismatch")
            return native
        seq = event.get("send_sequence")
        _require(_integer(seq), "receive_sequence_missing")
        publisher = event.get("publisher_gid")
        candidates = [s for s in sends if s.get("send_sequence") == seq]
        if publisher is not None:
            _require(_text(publisher), "publisher_gid_invalid")
            candidates = [s for s in candidates if s.get("writer_gid") == publisher]
            basis = "message_info_gid"
        else:
            _require(event.get("publisher_gid_status") == "unavailable_api", "publisher_gid_missing_without_unavailable_api")
            _require(_text(event.get("payload_sha256")) and _text(event.get("payload_writer_id"))
                     and _integer(event.get("payload_writer_generation"), 1), "payload_writer_identity_missing")
            candidates = [s for s in candidates if s.get("payload_sha256") == event["payload_sha256"]
                          and s.get("logical_id") == event["payload_writer_id"]
                          and s.get("generation") == event["payload_writer_generation"]]
            # A hash shared by multiple sends is not an independent unique binding.
            _require(len([s for s in sends if s.get("payload_sha256") == event["payload_sha256"]]) == 1,
                     "payload_hash_send_set_not_unique")
            basis = "endpoint_correlated_payload"
        _require(len(candidates) == 1, "receive_send_attribution_ambiguous")
        sent = candidates[0]
        writer = self.tokens[(sent["logical_id"], sent["generation"])]
        self.bound(sent, writer)
        _require(self.specs[sent["logical_id"]]["role"] == "writer"
                 and self.specs[event["logical_id"]]["role"] == "reader", "delivery_role_mismatch")
        # Publish-return can race callback, so require same payload, NOT receive > send-return.
        if event.get("payload_sha256") is not None:
            _require(event["payload_sha256"] == sent.get("payload_sha256"), "receive_send_payload_hash_mismatch")
        return {"writer": sent["logical_id"], "writer_generation": sent["generation"],
                "reader": event["logical_id"], "reader_generation": event["generation"],
                "sequence": seq, "basis": basis, "direct_gid_match": basis == "message_info_gid",
                "publisher_gid": publisher, "publisher_gid_status": event.get("publisher_gid_status"),
                "send_ref": self.ref(sent), "receive_ref": self.ref(event)}


def _source_qualification(audit, *, before=None, capture_source=None):
    """Reopen source and independently bind every producer phase to raw order.

    Admission uses only its own prefix. Final qualification additionally needs
    each action's pre-API check (including both concurrently begun writers),
    action completion, and cleanup; source-equal bookends alone are not proof.
    """
    source = audit.config.get("source_manifest")
    if audit.fixture or not isinstance(source, dict) or source.get("source_frozen") is not True:
        return False
    try:
        from .identity import source_manifest
        if capture_source is None:
            _require(source == source_manifest(Path(source["source_repo"])), "source_changed")
        else:
            # Explicit archival reconstruction validates the immutable capture,
            # not the checkout that happened to occupy this path at capture time.
            # Runtime callers use the default current-source measurement.
            _require(isinstance(capture_source, dict) and source == capture_source,
                     "saved_capture_source_mismatch")
        checks = [e for e in audit.events if e["event"] == "topology_source_checked"]
        if before is not None:
            checks = [e for e in checks if e["event_index"] < before["event_index"]
                      and e["monotonic_ns"] < before["monotonic_ns"]]
        _require(bool(checks) and all(e.get("source") == source for e in checks), "source_checks_mismatch")

        def phase(name):
            return _one(checks, "topology_source_checked", phase=name)

        preflight, ready, presteps = (phase(name) for name in
            ("before_preflight", "after_preflight", "before_presteps"))
        _chain(preflight, ready, presteps)
        if before is not None:
            _chain(presteps, before)
            return True

        actions = [e for e in audit.events if e["event"] == "topology_action_begin"]
        action_ids = [e.get("action_id") for e in actions]
        _require(all(_text(aid) for aid in action_ids)
                 and len(set(action_ids)) == len(action_ids), "source_action_identity")
        after_actions, after_cleanup = phase("after_actions"), phase("after_cleanup")
        expected_phases = {"before_preflight", "after_preflight", "before_presteps",
                           "after_actions", "after_cleanup"} | {"before_action:" + aid for aid in action_ids}
        _require(len(checks) == len(expected_phases)
                 and {e.get("phase") for e in checks} == expected_phases, "source_phase_set")
        for begin in actions:
            gate = phase("before_action:" + begin["action_id"])
            done = _one(audit.events, "topology_action_done", action_id=begin["action_id"])
            _chain(presteps, gate, begin, done, after_actions)
        cleanup = _one(audit.events, "topology_cleanup")
        _chain(presteps, after_actions, cleanup, after_cleanup)
        return True
    except (KeyError, OSError, ValueError, TypeError):
        return False


def _application(audit):
    try:
        from r2f.runtime.topology_route import audit_topology_application
    except ImportError:
        return {"status": "unknown", "reason": "topology_application_auditor_unavailable"}
    application = audit_topology_application(audit.plan, audit.layers, audit.runtime)
    computed = application.get("mutation_receipt")
    application["canonical_receipt_matches"] = (isinstance(computed, dict)
        and {k: computed.get(k) for k in MutationReceipt.field_names()} == audit.mutation.to_dict())
    return application


def _pair_facts(audit, projection, deliveries):
    side = projection["receipt"]["after"]
    rows = []
    for pair in projection["state_after"]["pairs"]:
        w, r = pair["writer"], pair["reader"]
        path = "/pairs/" + _pointer(w) + "/" + _pointer(r)
        components = {name: side["component_refs"].get(path + "/" + name, {})
                      for name in ("compatibility", "observed_match", "delivery_closure")}
        row = {**pair, "proofs": components, "communication_witnesses": [d for d in deliveries
                if d["writer"] == w and d["reader"] == r
                and side["identity"].get(w, {}).get("generation") == d["writer_generation"]
                and side["identity"].get(r, {}).get("generation") == d["reader_generation"]
                and d["receive_ref"]["event_index"] <= side["boundary_ref"]["event_index"]]}
        # DDS reliable/no eviction contract is independently observed in graph,
        # never substituted from the program/config or a predicted edge.
        contracts = {}
        for ref in components["compatibility"].get("refs", []):
            event = audit.resolve(ref)
            if event["event"] == "graph_snapshot":
                for eid, key in ((w, "publishers"), (r, "subscriptions")):
                    gid = side["identity"].get(eid, {}).get("endpoint_gid")
                    found = [e for e in event.get(key, []) if e.get("endpoint_gid") == gid]
                    if len(found) == 1:
                        contracts.setdefault(eid, []).append(found[0].get("qos", {}))
        expected = components["delivery_closure"].get("detail", {}).get("expected", [])
        policies = {}
        for eid in (w, r):
            values = contracts.get(eid, [])
            if len(values) >= 2 and all(v == values[0] for v in values):
                policies[eid] = values[0]
        row["reliable_history_contract"] = (len(policies) == 2 and all(
            isinstance(p, dict) and p.get("reliability") == "reliable" and (p.get("history") == "keep_all"
                or p.get("history") == "keep_last" and _integer(p.get("depth"), max(1, len(expected))))
            for p in policies.values()))
        # A dynamic update may alter filtering even when the initial spec was empty.
        # Without a raw-proven temporal partition, conservatively disable delivery obligations.
        dynamic_reader = any(a.get("op") == "update_filter" and a.get("endpoint_id") == r
                             for a in getattr(audit, "program", {}).get("actions", []))
        row["filter_free"] = (not dynamic_reader and
                              all(audit.specs[eid].get("filter") is None for eid in (w, r)))
        row["writer_generation"] = side["identity"].get(w, {}).get("generation")
        row["reader_generation"] = side["identity"].get(r, {}).get("generation")
        row["ordering_observation"] = {
            "schema": "r2f.topology-per-writer-order-observation/v1", "status": "unknown",
            "reason_codes": ["validated_pair_window_unavailable"],
            "writer": w, "reader": r, "writer_generation": row["writer_generation"],
            "reader_generation": row["reader_generation"],
            "writer_gid": side["identity"].get(w, {}).get("endpoint_gid"),
            "reader_gid": side["identity"].get(r, {}).get("endpoint_gid"),
            "oracle_coverage": "not_declared_by_target_rule", "candidate_eligible": False,
            "cross_writer_order_required": False, "rule_verdict": None}
        row.update(_delivery_window(audit, row, deliveries))
        rows.append(row)
    return rows


def _ordering_observation(audit, pair, opened, close):
    """Supporting observation, independently rebuilt from the audited raw file.

    This is reader callback/capture order, not wire order or a cross-writer
    total order. The target's existing rule declares no ordering residual:
    even an observed inversion cannot confer rule/candidate eligibility.
    """
    w, r = pair["writer"], pair["reader"]
    wg, rg = pair["writer_generation"], pair["reader_generation"]
    result = {
        "schema": "r2f.topology-per-writer-order-observation/v1",
        "scope": "per_writer_reader_generation_window",
        "writer": w, "reader": r, "writer_generation": wg, "reader_generation": rg,
        "writer_gid": endpoint_identity_at(audit.events, audit.tokens[(w, wg)],
            before_index=opened["event_index"] + 1)["endpoint_gid"],
        "reader_gid": endpoint_identity_at(audit.events, audit.tokens[(r, rg)],
            before_index=opened["event_index"] + 1)["endpoint_gid"],
        "open_ref": audit.ref(opened), "close_ref": audit.ref(close),
        "status": "unknown", "reason_codes": [], "send_order": [], "receive_order": [],
        "send_refs": [], "receive_refs": [], "witnesses": [], "inversions": [],
        "identity_bases": [], "cross_writer_order_required": False,
        "oracle_coverage": "not_declared_by_target_rule", "candidate_eligible": False,
        "rule_verdict": None,
    }
    def unknown(reason):
        result["reason_codes"].append(reason)
        return result

    lo, hi = opened["event_index"], close["event_index"]
    inside = [e for e in audit.events if lo < e["event_index"] < hi]
    result["raw_reader_receive_refs"] = [audit.ref(e) for e in inside
        if e["event"] == "receive" and e.get("logical_id") == r and e.get("generation") == rg]
    sends = [e for e in inside if e["event"] == "send"
             and e.get("logical_id") == w and e.get("generation") == wg]
    result["send_order"] = [e.get("send_sequence") for e in sends]
    result["send_refs"] = [audit.ref(e) for e in sends]
    # Do not use producer/helper summary or even the supplied deliveries list.
    all_sends = [e for e in audit.events if e["event"] == "send"]
    try:
        for event in inside:
            if (event["event"] != "receive" or event.get("logical_id") != r
                    or event.get("generation") != rg):
                continue
            witness = audit.delivery_identity(event, all_sends)
            if witness["writer"] != w or witness["writer_generation"] != wg:
                continue  # Valid other-writer/generation delivery, not this stream.
            result["witnesses"].append(witness)
    except (ValueError, KeyError, TypeError) as exc:
        return unknown("raw_sample_identity_unknown:" + str(exc))
    result["receive_order"] = [d["sequence"] for d in result["witnesses"]]
    result["receive_refs"] = [d["receive_ref"] for d in result["witnesses"]]
    result["identity_bases"] = sorted({d["basis"] for d in result["witnesses"]})
    expected, received = result["send_order"], result["receive_order"]
    if not expected or any(type(seq) is not int for seq in expected):
        return unknown("no_orderable_send_inventory")
    if len(expected) != len(set(expected)) or len(received) != len(set(received)):
        return unknown("duplicate_or_retransmission_ambiguity")
    if any(d["send_ref"] not in result["send_refs"] for d in result["witnesses"]):
        return unknown("receive_send_outside_generation_window")
    if any(e.get("publish_returned") is not True for e in sends):
        return unknown("publish_completion_unproven")
    if pair.get("reliable_history_contract") is not True or pair.get("filter_free") is not True:
        return unknown("reliable_unfiltered_contract_unproven")
    sentinel = close.get("sentinel_sequence")
    # A finite timer/close marker alone is not a delivery closure. Require the
    # later same-writer send to be observed last; never judge a missing tail.
    if (close.get("window_complete") is not True or sentinel != expected[-1]
            or not received or received[-1] != sentinel):
        return unknown("later_same_generation_sentinel_not_closed")
    result["sentinel_send_ref"] = result["send_refs"][-1]
    result["sentinel_receive_ref"] = result["receive_refs"][-1]
    ranks = {seq: i for i, seq in enumerate(expected)}
    for left, right in zip(result["witnesses"], result["witnesses"][1:]):
        if ranks[left["sequence"]] > ranks[right["sequence"]]:
            result["inversions"].append({"earlier_receive_ref": left["receive_ref"],
                                         "later_receive_ref": right["receive_ref"],
                                         "sequences": [left["sequence"], right["sequence"]]})
    result["status"] = "inversion_observed" if result["inversions"] else "in_order_observed"
    result["reason_codes"] = ["ordering_observation_only_no_declared_rule"]
    return result


def _delivery_window(audit, pair, deliveries):
    """Refine payload attribution only after the shared projector proved the
    fresh pair match at window opening. Never turn delivery into other edges.
    """
    component = pair["proofs"]["delivery_closure"]
    refs = [audit.resolve(r) for r in component.get("refs", [])]
    detail = component.get("detail", {})
    if detail.get("applicability_kind") == "not_applicable_no_sends":
        claims = [e for e in refs if e["event"] == "topology_delivery_no_sends"]
        seals = [e for e in refs if e["event"] == "topology_send_inventory_end"]
        valid = (component.get("status") == "observed" and component.get("value") == "no_obligation"
                 and pair.get("delivery_closure") == "no_obligation" and len(claims) == len(seals) == 1
                 and detail.get("no_sends_ref") == audit.ref(claims[0])
                 and detail.get("inventory_end_ref") == audit.ref(seals[0])
                 and all(detail.get(k) == [] for k in ("expected", "received", "missing")))
        return {"delivery_window_identity_complete": valid,
                "delivery_closure": "no_obligation" if valid else "unknown",
                "delivery_applicability": "not_applicable_no_sends" if valid else "unknown"}
    openings = [e for e in refs if e["event"] == "topology_delivery_window_open"]
    closings = [e for e in refs if e["event"] == "topology_delivery_window_closed"]
    if len(openings) != 1 or len(closings) != 1:
        return {"delivery_window_identity_complete": False}
    opened, close = openings[0], closings[0]
    # delivery() validates fresh command-bound snapshots before returning
    # these refs: match proof OR the independently checked no-obligation branch,
    # never helper.status or a predicted edge.
    matches = [e for e in refs if e["event"] == "match_snapshot"]
    if len(matches) != 2 or any(m["event_index"] >= opened["event_index"] for m in matches):
        return {"delivery_window_identity_complete": False}
    # This component is produced by project_topology_state from the audited
    # persisted bytes, NOT runtime.facts or a caller's no-obligation flag. Its
    # receipt branch independently checks actual endpoint identities/types,
    # fresh snapshot/ACK chronology, the claim and its complete empty close.
    # Writer sends after that claim belong to other pairs (or have no pair
    # obligation); rebuilding expected from them would invent missing samples.
    detail = component.get("detail", {})
    claims = [e for e in refs if e["event"] == "topology_delivery_no_obligation"]
    if (component.get("status") == "observed" and component.get("value") == "no_obligation"
            and pair.get("delivery_closure") == "no_obligation" and len(claims) == 1
            and detail.get("no_obligation_ref") == audit.ref(claims[0])
            and all(detail.get(key) == [] for key in ("expected", "received", "missing"))):
        detail.update(open_ref=audit.ref(opened), close_ref=audit.ref(close))
        return {"delivery_window_identity_complete": True, "delivery_closure": "no_obligation"}
    w, r = pair["writer"], pair["reader"]
    wg, rg = pair["writer_generation"], pair["reader_generation"]
    inside = [e for e in audit.events if opened["event_index"] < e["event_index"] < close["event_index"]]
    sends = [e for e in inside if e["event"] == "send" and e.get("logical_id") == w and e.get("generation") == wg]
    samples = [e for e in inside if e["event"] == "receive" and e.get("logical_id") == r and e.get("generation") == rg]
    witnesses = [d for d in deliveries if d["writer"] == w and d["reader"] == r
                 and d["writer_generation"] == wg and d["reader_generation"] == rg
                 and opened["event_index"] < d["receive_ref"]["event_index"] < close["event_index"]]
    # The projector already proved this window's identities and fresh match
    # chronology. Preserve ordering separately before the legacy set/gap gate.
    pair["ordering_observation"] = _ordering_observation(audit, pair, opened, close)
    all_attributed = {d["receive_ref"]["event_index"] for d in deliveries}
    if any(e["event_index"] not in all_attributed for e in samples):
        return {"delivery_window_identity_complete": False, "delivery_closure": "unknown"}
    expected = [e["send_sequence"] for e in sends]
    if not expected or len(expected) != len(set(expected)) or expected != sorted(expected):
        return {"delivery_window_identity_complete": False}
    sequences = [d["sequence"] for d in witnesses]
    received = set(sequences)
    if sequences != sorted(set(sequences)):
        return {"delivery_window_identity_complete": False, "delivery_closure": "unknown",
                "delivery_unknown_reason": "per_writer_duplicate_or_order_observation_needs_rule"}
    if not received <= set(expected):
        return {"delivery_window_identity_complete": False, "delivery_closure": "unknown"}
    sentinel = close.get("sentinel_sequence")
    missing = sorted(set(expected) - received)
    closed = (_integer(sentinel) and sentinel in received and sentinel == max(expected)
              and close.get("window_complete") is True)
    detail = {"expected": expected, "received": sorted(received), "missing": missing,
              "sentinel_sequence": sentinel, "open_ref": audit.ref(opened), "close_ref": audit.ref(close),
              "attribution_bases": sorted({d["basis"] for d in witnesses}), "cross_writer_order_required": False}
    component["detail"].update(detail)
    return {"delivery_window_identity_complete": True,
            "delivery_closure": ("closed_with_gap" if missing else "closed") if closed else "open"}


def _historical_no_obligation_windows(audit, pairs, projection):
    """Revalidate earlier empty windows with the SAME canonical projector.

    Retain the final pair's generation fence: older/replaced readers cannot be
    discharged using a different generation. Only window-scoped raw proofs,
    not a final match label or caller receipt, can cover a historical send.
    """
    from .topology_state_projection import _Projection
    projector = _Projection(audit.program, audit.path, audit.sha, audit.events,
                            {"input_ref": audit.ref(audit.binding)})
    windows = []
    final_endpoints = {e["logical_id"]: e for e in projection["state_after"]["endpoints"]}
    for pair in pairs:
        w, r = pair["writer"], pair["reader"]
        wg, rg = pair.get("writer_generation"), pair.get("reader_generation")
        # A token's historical existence is not a final live-generation proof.
        # Preserve the conservative lifecycle fence: this repair only spans
        # windows of the same still-live pair, not retired/replaced readers.
        if (any(final_endpoints.get(eid, {}).get("liveness") != "alive"
                or final_endpoints[eid].get("generation") != gen for eid, gen in ((w, wg), (r, rg)))
                or pair.get("delivery_closure") not in {"no_obligation", "closed", "closed_with_gap", "open"}
                or pair.get("delivery_window_identity_complete") is not True
                or (w, wg) not in audit.tokens or (r, rg) not in audit.tokens):
            continue
        for close in audit.events:
            if (close["event"] != "topology_delivery_window_closed"
                    or close.get("writer") != w or close.get("reader") != r
                    or close.get("writer_generation") != wg or close.get("reader_generation") != rg
                    or close.get("closure_status") != "no_obligation"):
                continue
            # delivery() independently checks fresh request/snapshot/ACK chains,
            # observed identities, the claim, empty close and interruption gates.
            try:
                value, refs, detail = projector.delivery(audit.tokens[(w, wg)],
                    audit.tokens[(r, rg)], {}, close)
                if value != "no_obligation" or refs.count(close) != 1:
                    continue
                row = {"writer": w, "reader": r, "writer_generation": wg, "reader_generation": rg,
                       "delivery_closure": value, "proofs": {"delivery_closure": {
                           "status": "observed", "value": value, "detail": detail,
                           "refs": [audit.ref(event) for event in refs]}}}
                row.update(_delivery_window(audit, row, []))
                if row.get("delivery_window_identity_complete") is True:
                    windows.append(row)
            except (ValueError, KeyError, TypeError):
                # A bad historical claim stays unresolved, never a waiver.
                continue
    return windows


def _no_obligation_send_covered(audit, pairs, send):
    """Discharge only this raw send, not a writer's historical inventory.

    All readers alive at this send must have the same generation in the
    independently projected empty-obligation windows covering it. Older sends,
    replaced readers, missing closes and overlapping unproved windows remain
    unresolved; final nonmatch is never persistent historical nonmatch proof.
    """
    live_readers = {}
    for event in audit.events[:send["event_index"]]:
        eid = event.get("logical_id")
        if event["event"] == "endpoint_created" and audit.specs[eid]["role"] == "reader":
            live_readers[eid] = event["generation"]
        elif event["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
            live_readers.pop(eid, None)
    # The projector retains all declared pairs, including never-created and
    # retired readers. Applicability is this send's raw lifecycle inventory,
    # not that final pair universe. A reader alive then remains mandatory even
    # if absent/replaced now; its projected generation must still match.
    rows = []
    for pair in pairs:
        if (pair.get("writer") != send.get("logical_id")
                or pair.get("writer_generation") != send.get("generation")
                or pair.get("reader") not in live_readers):
            continue
        if (pair.get("delivery_closure") != "no_obligation"
                or pair.get("delivery_window_identity_complete") is not True):
            continue
        # Unknown/not-applicable/malformed proofs have no window refs. Reject
        # missing evidence instead of indexing it or treating a label as proof.
        try:
            component = pair["proofs"]["delivery_closure"]
            if component.get("status") != "observed" or component.get("value") != "no_obligation":
                return False
            detail = component["detail"]
            opened, close = (audit.resolve(detail[key]) for key in ("open_ref", "close_ref"))
            if (opened["event"] != "topology_delivery_window_open"
                    or close["event"] != "topology_delivery_window_closed"):
                return False
            if (opened["event_index"] < send["event_index"] < close["event_index"]
                    and opened["monotonic_ns"] < send["monotonic_ns"] < close["monotonic_ns"]):
                rows.append((pair, opened))
        except (ValueError, KeyError, TypeError, AttributeError):
            return False
    if (not live_readers or len(rows) != len(live_readers)
            or {p["reader"]: p.get("reader_generation") for p, _ in rows} != live_readers):
        return False
    covered = {opened["event_index"] for _, opened in rows}
    for opened in audit.events[:send["event_index"]]:
        if (opened["event"] != "topology_delivery_window_open"
                or opened.get("writer") != send.get("logical_id")
                or opened.get("writer_generation") != send.get("generation")):
            continue
        closes = [e for e in audit.events if e["event"] == "topology_delivery_window_closed"
                  and e.get("open_ref") == opened["event_index"]]
        if len(closes) != 1 or (closes[0]["event_index"] > send["event_index"]
                                and opened["event_index"] not in covered):
            return False
    return True


def _recreated_history_windows(audit, pairs, projection, deliveries):
    """Keep verified old windows separate from the current unsent generation.

    This deliberately does NOT loosen the legacy live-generation NONE fence.
    Only a freshly reconstructed v2 retirement inventory certifies this path;
    every historical close is then re-read at its own old-generation boundary.
    """
    from .topology_state_projection import _Projection
    projector = _Projection(audit.program, audit.path, audit.sha, audit.events,
                            {"input_ref": audit.ref(audit.binding)})
    boundary = audit.resolve(projection["receipt"]["after"]["boundary_ref"])
    windows, certified = {}, set()
    for pair in pairs:
        w, r = pair["writer"], pair["reader"]
        proof = pair["proofs"]["delivery_closure"]
        # A declared endpoint may never have been created (generation=None).
        # Only an observed integer generation >=2 can certify recreated history.
        if (w in certified or not _integer(pair.get("writer_generation"), 2)
                or pair.get("delivery_applicability") != "not_applicable_no_sends"
                or pair.get("delivery_window_identity_complete") is not True
                or proof.get("status") != "observed"
                or proof.get("detail", {}).get("no_sends_schema") != "r2f.topology-delivery-no-sends/v2"):
            continue
        try:
            value, _, detail = projector.recreated_no_sends(
                audit.tokens[(w, pair["writer_generation"])],
                audit.tokens[(r, pair["reader_generation"])], boundary)
            _require(value == "no_obligation", "recreated_history_inventory_unknown")
            pending = {}
            for ref in detail["prior_window_close_refs"]:
                close = audit.resolve(ref)
                if close.get("writer") != w:
                    continue
                wg, rg = close["writer_generation"], close["reader_generation"]
                _require(wg < pair["writer_generation"], "recreated_history_not_prior_generation")
                value, rows, old_detail = projector.delivery(
                    audit.tokens[(w, wg)], audit.tokens[(close["reader"], rg)], {}, close)
                _require(value in {"closed", "no_obligation"} and not old_detail.get("missing"),
                         "recreated_history_unclosed_obligation")
                row = {"writer": w, "reader": close["reader"], "writer_generation": wg,
                       "reader_generation": rg, "delivery_closure": value,
                       "proofs": {"delivery_closure": {"status": "observed", "value": value,
                           "detail": old_detail, "refs": [audit.ref(e) for e in rows]}},
                       "generation_inventory_ref": detail["no_sends_ref"]}
                row.update(_delivery_window(audit, row, deliveries))
                _require(row.get("delivery_window_identity_complete") is True
                         and row["delivery_closure"] in {"closed", "no_obligation"},
                         "recreated_history_window_identity_incomplete")
                pending[close["open_ref"]] = row
            windows.update(pending)
            certified.add(w)
        except (ValueError, KeyError, TypeError, IndexError):
            # A missing receipt/old ACK/retirement or unresolved old window
            # leaves the historical send unresolved, never borrows g2 NONE.
            continue
    return list(windows.values())


def _recreated_history_send_coverage(audit, windows, send):
    """One old send requires its OWN complete window for every then-live reader."""
    live = {}
    for event in audit.events[:send["event_index"]]:
        eid = event.get("logical_id")
        if event["event"] == "endpoint_created" and audit.specs[eid]["role"] == "reader":
            live[eid] = event["generation"]
        elif event["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
            live.pop(eid, None)
    if not live:
        return None
    rows = []
    for row in windows:
        if (row["writer"] != send.get("logical_id") or row["writer_generation"] != send.get("generation")
                or live.get(row["reader"]) != row["reader_generation"]):
            continue
        proof = row["proofs"]["delivery_closure"]
        detail = proof["detail"]
        opened, close = (audit.resolve(detail[k]) for k in ("open_ref", "close_ref"))
        if not (opened["event_index"] < send["event_index"] < close["event_index"]
                and opened["monotonic_ns"] <= send["monotonic_ns"] <= close["monotonic_ns"]):
            continue
        if row["delivery_closure"] == "closed" and audit.ref(send) not in proof["refs"]:
            continue
        rows.append({"reader": row["reader"], "reader_generation": row["reader_generation"],
                     "writer_generation": row["writer_generation"], "open_ref": audit.ref(opened),
                     "close_ref": audit.ref(close), "closure": row["delivery_closure"],
                     "generation_inventory_ref": row["generation_inventory_ref"]})
    if len(rows) != len(live) or {r["reader"]: r["reader_generation"] for r in rows} != live:
        return None
    covered = {r["open_ref"]["event_index"] for r in rows}
    for opened in audit.events[:send["event_index"]]:
        if (opened["event"] != "topology_delivery_window_open"
                or opened.get("writer") != send.get("logical_id")
                or opened.get("writer_generation") != send.get("generation")):
            continue
        closes = [e for e in audit.events if e["event"] == "topology_delivery_window_closed"
                  and e.get("open_ref") == opened["event_index"]]
        if len(closes) != 1 or (closes[0]["event_index"] > send["event_index"]
                                and opened["event_index"] not in covered):
            return None
    return {"send_ref": audit.ref(send), "writer": send["logical_id"],
            "writer_generation": send["generation"], "reader_windows": rows,
            "scope": "complete_historical_windows_not_current_generation_delivery"}



def _live_history_windows(audit, pairs, projection, deliveries):
    """Re-project each still-live pair window at ITS close, not the final GID.

    Invalid chains remain explicit records. A closed-with-gap window proves only
    its received samples; this function grants no CFT/filtering exemption.
    """
    final = {e["logical_id"]: e for e in projection["state_after"]["endpoints"]}
    live_pairs = {(p["writer"], p.get("writer_generation"), p["reader"], p.get("reader_generation"))
                  for p in pairs if all(final.get(eid, {}).get("liveness") == "alive"
                                        and final[eid].get("generation") == generation
                                        and (eid, generation) in audit.tokens
                                        for eid, generation in ((p["writer"], p.get("writer_generation")),
                                                                (p["reader"], p.get("reader_generation"))))}
    return _history_windows_for_pairs(audit, projection, deliveries, live_pairs,
                                      "still_live_logical_generation_historical_window")


def _history_windows_for_pairs(audit, projection, deliveries, live_pairs, scope):
    from .topology_state_projection import _Projection
    projector = _Projection(audit.program, audit.path, audit.sha, audit.events,
                            {"input_ref": audit.ref(audit.binding)})
    boundary = audit.resolve(projection["receipt"]["after"]["boundary_ref"])
    windows = []
    for close in audit.events[:boundary["event_index"] + 1]:
        key = (close.get("writer"), close.get("writer_generation"),
               close.get("reader"), close.get("reader_generation"))
        if close["event"] != "topology_delivery_window_closed" or key not in live_pairs:
            continue
        w, wg, r, rg = key
        row = {"writer": w, "writer_generation": wg, "reader": r, "reader_generation": rg,
               "close_ref": audit.ref(close), "delivery_closure": "unknown",
               "delivery_window_identity_complete": False, "window_projection_valid": False,
               "delivery_witnesses": [], "reason": None,
               "scope": scope}
        try:
            opened = projector.resolve(close["open_ref"], boundary=close)
            row["open_ref"] = audit.ref(opened)
            closes = [e for e in audit.events if e["event"] == "topology_delivery_window_closed"
                      and e.get("open_ref") == opened["event_index"]]
            _require(len(closes) == 1, "live_history_window_close_missing_or_duplicate")
            value, refs, detail = projector.delivery(audit.tokens[(w, wg)], audit.tokens[(r, rg)], {}, close)
            row["proofs"] = {"delivery_closure": {"status": "observed" if value != "unknown" else "unknown",
                            "value": value, "detail": detail, "refs": [audit.ref(e) for e in refs]}}
            _require(value in {"closed", "closed_with_gap", "open", "no_obligation"},
                     "live_history_window_projection_unknown")
            row["delivery_closure"] = value
            for role, token_key in (("writer", (w, wg)), ("reader", (r, rg))):
                identity = endpoint_identity_at(audit.events, audit.tokens[token_key],
                                                before_index=close["event_index"] + 1)
                row[role + "_identity"] = {k: identity[k] for k in
                    (*_LINEAGE, "endpoint_gid", "incarnation_ordinal", "identity_valid_after_ns")}
                row[role + "_identity"].update(
                    creation_ref=audit.ref(audit.tokens[token_key]),
                    transition_ref=None if identity["identity_transition_ref"] is None else
                        audit.ref(audit.events[identity["identity_transition_ref"]]),
                    valid_after_ref=audit.ref(audit.events[identity["identity_valid_after_ref"]]))
            row.update(_delivery_window(audit, row, deliveries))
            _require(row.get("delivery_window_identity_complete") is True
                     and row["delivery_closure"] == value,
                     row.get("delivery_unknown_reason", "live_history_window_identity_incomplete"))
            row["delivery_witnesses"] = [d for d in deliveries
                if (d["writer"], d["writer_generation"], d["reader"], d["reader_generation"]) == key
                and opened["event_index"] < d["send_ref"]["event_index"] < close["event_index"]
                and opened["event_index"] < d["receive_ref"]["event_index"] < close["event_index"]]
            row["window_projection_valid"] = True
        except (ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
            row.update(reason=str(exc), delivery_closure="unknown", delivery_window_identity_complete=False)
        windows.append(row)
    for opened in audit.events[:boundary["event_index"] + 1]:
        key = (opened.get("writer"), opened.get("writer_generation"),
               opened.get("reader"), opened.get("reader_generation"))
        if (opened["event"] == "topology_delivery_window_open" and key in live_pairs
                and not any(row.get("open_ref") == audit.ref(opened) for row in windows)):
            windows.append({"writer": key[0], "writer_generation": key[1], "reader": key[2],
                            "reader_generation": key[3], "open_ref": audit.ref(opened), "close_ref": None,
                            "delivery_closure": "unknown", "delivery_window_identity_complete": False,
                            "window_projection_valid": False, "delivery_witnesses": [],
                            "reason": "live_history_window_close_missing_or_unbound",
                            "scope": scope})
    return windows



def _participant_group_history_windows(audit, projection, deliveries):
    """Reconcile group history only through authenticated, pre-boundary cleanup.

    A surviving peer keeps its exact final incarnation; every retired peer needs
    its own closed destroy operation after this window. Whole-run group lineage
    was audited independently, and no incomplete boundary is upgraded here.
    """
    if audit.group_lineage is None:
        return []
    boundary = audit.resolve(projection["receipt"]["after"]["boundary_ref"])
    if boundary.get("lifecycle_complete") is not True:
        return []
    final = {e["logical_id"]: e for e in projection["state_after"]["endpoints"]}
    retired = {(audit.events[r["cleanup_ref"]]["logical_id"],
                audit.events[r["cleanup_ref"]]["generation"]): r
               for r in audit.group_destroys if r["action_done_ref"] < boundary["event_index"]
               and audit.events[r["action_done_ref"]]["monotonic_ns"] <= boundary["monotonic_ns"]}
    pending_pairs, refs = set(), {}
    # Opens identify obligations to inspect, but only an authenticated close
    # before retirement can qualify a window. Missing closes stay unknown.
    for event in audit.events[:boundary["event_index"] + 1]:
        if event.get("event") not in {"topology_delivery_window_open", "topology_delivery_window_closed"}:
            continue
        key = (event.get("writer"), event.get("writer_generation"),
               event.get("reader"), event.get("reader_generation"))
        proofs = []
        for eid, generation in ((key[0], key[1]), (key[2], key[3])):
            receipt = retired.get((eid, generation))
            if receipt is not None:
                request = audit.events[receipt["request_ref"]]
                if (event["event_index"] >= request["event_index"]
                        or event["monotonic_ns"] > request["monotonic_ns"]):
                    break
                proofs.append(receipt)
            elif (final.get(eid, {}).get("liveness") != "alive"
                  or final[eid].get("generation") != generation):
                break
        else:
            if proofs:
                pending_pairs.add(key)
                if event["event"] == "topology_delivery_window_closed":
                    refs[event["event_index"]] = proofs
    windows = _history_windows_for_pairs(audit, projection, deliveries, pending_pairs,
        "participant_control_verified_retirement_historical_window")
    for row in windows:
        row["destroy_operation_refs"] = [
            {name: audit.ref(audit.events[index]) for name, index in receipt.items()}
            for receipt in refs.get((row.get("close_ref") or {}).get("event_index"), [])]
        if row.get("close_ref") is not None and not row["destroy_operation_refs"]:
            row.update(window_projection_valid=False, delivery_closure="unknown",
                       delivery_window_identity_complete=False, delivery_witnesses=[],
                       reason="group_history_close_without_qualified_destroy")
            if "proofs" in row:
                row["proofs"]["delivery_closure"].update(status="unknown", value="unknown")
    return windows


def _participant_group_send_coverage(audit, group_windows, live_windows, send):
    if not group_windows:
        return None
    coverage = _live_history_send_coverage(audit, group_windows + live_windows, send)
    if coverage is not None:
        coverage["scope"] = "received_sample_in_verified_participant_control_historical_windows"
    return coverage


def _retired_episode_history_windows(audit, projection, deliveries, episode_observation):
    """Only opt-in completed episodes may reconcile a retired BOTH-peer window.

    Its own close, both worker exits/retirements and fresh successors are needed.
    No current-generation NONE/readback or CFT rejection waives old delivery.
    The legacy still-live-generation fence remains unchanged.
    """
    if ("episode" not in audit.program or not episode_observation.get("structural_valid")
            or not episode_observation.get("completed")):
        return []
    boundary = audit.resolve(projection["receipt"]["after"]["boundary_ref"])
    qualified = set()
    for close in audit.events[:boundary["event_index"] + 1]:
        if close.get("event") != "topology_delivery_window_closed":
            continue
        key = (close.get("writer"), close.get("writer_generation"),
               close.get("reader"), close.get("reader_generation"))
        try:
            for eid, generation in ((key[0], key[1]), (key[2], key[3])):
                token = audit.tokens[(eid, generation)]
                successor = audit.tokens[(eid, generation + 1)]
                retired = [e for e in audit.events[:boundary["event_index"] + 1]
                    if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                    and e.get("logical_id") == eid and e.get("generation") == generation]
                exited = [e for e in audit.events[:boundary["event_index"] + 1]
                    if e.get("event") == "topology_worker_exit"
                    and e.get("logical_id") == eid and e.get("generation") == generation]
                _require(len(retired) == len(exited) == 1, "episode_history_retirement_or_exit_missing")
                retired, exited = retired[0], exited[0]
                _chain(token, close, retired, successor)
                _chain(token, close, exited, successor)
                identity = endpoint_identity_at(audit.events, token, before_index=retired["event_index"] + 1)
                _require(retired.get("cleanup_status") == "closed" and exited.get("returncode") == 0
                    and all(all(e.get(k) == identity.get(k) for k in (*_LINEAGE, "endpoint_gid"))
                            for e in (retired, exited)), "episode_history_retirement_identity_incomplete")
                _require((token["pid"], token["process_start_ticks"]) !=
                    (successor["pid"], successor["process_start_ticks"])
                    and successor["endpoint_gid"] != identity["endpoint_gid"],
                    "episode_history_successor_not_fresh")
            qualified.add(key)
        except (ValueError, KeyError, TypeError, IndexError):
            continue
    return _history_windows_for_pairs(audit, projection, deliveries, qualified,
                                     "verified_retired_episode_generation_historical_window")


def _retired_episode_send_coverage(audit, windows, send):
    coverage = _live_history_send_coverage(audit, windows, send)
    if coverage is not None:
        coverage["scope"] = "received_sample_in_verified_retired_episode_generation_windows"
    return coverage


def _native_filter_rejection_disposition(audit, row, send, observations):
    """Account ONE actual rejected sample, never rewrite a missing delivery.

    The caller supplies independently revalidated actual evaluate observations.
    A same-action accepted AND delivered later sentinel is required in this
    exact incarnation/window. Configuration/model predictions never suffice.
    """
    detail = row["proofs"]["delivery_closure"]["detail"]
    sequence = send.get("send_sequence")
    if (row["delivery_closure"] != "closed_with_gap" or sequence not in detail.get("missing", [])
            or sequence not in detail.get("expected", []) or sequence in detail.get("received", [])):
        return None
    opened, close = audit.resolve(row["open_ref"]), audit.resolve(row["close_ref"])
    if close.get("window_complete") is not True:
        return None
    def bound(observation, expected_decision):
        return (observation.get("decision") is expected_decision
            and observation.get("writer") == row["writer"]
            and observation.get("writer_generation") == row["writer_generation"]
            and observation.get("writer_gid") == row["writer_identity"]["endpoint_gid"]
            and observation.get("reader") == row["reader"]
            and observation.get("reader_generation") == row["reader_generation"]
            and observation.get("reader_gid") == row["reader_identity"]["endpoint_gid"]
            and observation.get("reader_incarnation") == row["reader_identity"]["incarnation_ordinal"]
            and opened["monotonic_ns"] <= observation.get("capture_started_ns", -1)
                <= observation.get("capture_finished_ns", -1) <= close["monotonic_ns"]
            and opened["event_index"] < observation.get("decision_ref", {}).get("event_index", -1)
                < close["event_index"])
    rejected = [o for o in observations if bound(o, False)
                and o.get("sequence") == sequence and o.get("send_ref") == audit.ref(send)]
    if len(rejected) != 1:
        return None
    rejected = rejected[0]
    sentinel = close.get("sentinel_sequence")
    accepted = [o for o in observations if bound(o, True) and o.get("sequence") == sentinel
                and all(o.get(k) is not None and o[k] == rejected.get(k)
                        for k in ("action_id", "action_result_ref", "state_ref"))
                and rejected["capture_finished_ns"] <= o["capture_started_ns"]]
    if len(accepted) != 1 or not _integer(sentinel) or sentinel <= sequence:
        return None
    accepted = accepted[0]
    witnesses = [d for d in row["delivery_witnesses"] if d["sequence"] == sentinel
                 and d["send_ref"] == accepted["send_ref"]
                 and d.get("reader_gid") == accepted["reader_gid"]]
    if len(witnesses) != 1 or accepted["send_ref"]["event_index"] <= send["event_index"]:
        return None
    received = audit.resolve(witnesses[0]["receive_ref"])
    if accepted["capture_finished_ns"] > received["monotonic_ns"]:
        return None
    return {"schema": "r2f.cft-native-rejection-window-evidence/v1",
            "filter_effect_observed": True, "candidate_eligible": False,
            "basis": "actual_native_false_and_control_with_same_incarnation_accepted_delivered_sentinel",
            "open_ref": row["open_ref"], "close_ref": row["close_ref"],
            "rejected": rejected, "accepted_sentinel": accepted,
            "sentinel_delivery_witness": witnesses[0],
            "scope": "exact_sample_filter_disposition_not_delivery_or_candidate"}


def _live_history_send_coverage(audit, windows, send, *, filter_decisions=()):
    """Every then-live reader and EVERY overlapping window needs this sample.

    Missing samples require an exact independently validated native rejection,
    never just configuration or absence. Open tails, invalid/duplicate/missing
    closes and old reader generations remain unresolved.
    """
    live = {}
    for event in audit.events[:send["event_index"]]:
        eid = event.get("logical_id")
        if event["event"] == "endpoint_created" and audit.specs[eid]["role"] == "reader":
            live[eid] = event["generation"]
        elif event["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
            live.pop(eid, None)
    if not live:
        return None
    covered, seen = [], set()
    for opened in audit.events[:send["event_index"]]:
        if (opened["event"] != "topology_delivery_window_open"
                or opened.get("writer") != send.get("logical_id")
                or opened.get("writer_generation") != send.get("generation")):
            continue
        closes = [e for e in audit.events if e["event"] == "topology_delivery_window_closed"
                  and e.get("open_ref") == opened["event_index"]]
        if len(closes) != 1:
            return None
        close = closes[0]
        if close["event_index"] <= send["event_index"]:
            continue
        candidates = [row for row in windows if row.get("open_ref") == audit.ref(opened)
                      and row["close_ref"] == audit.ref(close)]
        if (len(candidates) != 1 or not opened["monotonic_ns"] <= send["monotonic_ns"] <= close["monotonic_ns"]):
            return None
        row = candidates[0]
        if (not row["window_projection_valid"] or row["delivery_window_identity_complete"] is not True
                or row["delivery_closure"] not in {"closed", "closed_with_gap"}
                or live.get(row["reader"]) != row["reader_generation"]):
            return None
        detail = row["proofs"]["delivery_closure"]["detail"]
        sequence = send.get("send_sequence")
        witnesses = [d for d in row["delivery_witnesses"] if d["send_ref"] == audit.ref(send)
                     and d["sequence"] == sequence]
        rejection = None
        if (sequence not in detail.get("expected", []) or sequence not in detail.get("received", [])
                or sequence in detail.get("missing", []) or len(witnesses) != 1):
            # A received sample never simultaneously becomes a filtered-away
            # sample. Retain both contradictions and unproved gaps unresolved.
            if witnesses:
                return None
            rejection = _native_filter_rejection_disposition(audit, row, send, filter_decisions)
            if rejection is None:
                return None
        item = {"reader": row["reader"], "reader_generation": row["reader_generation"],
                "writer_generation": row["writer_generation"], "open_ref": row["open_ref"],
                "close_ref": row["close_ref"], "closure": row["delivery_closure"],
                "writer_identity": row["writer_identity"], "reader_identity": row["reader_identity"],
                "delivery_witness": witnesses[0] if witnesses else None,
                "delivery_disposition": "received" if rejection is None else "filtered_by_actual_native_decision"}
        if rejection is not None:
            item["filter_decision_evidence"] = rejection
        covered.append(item)
        seen.add(row["reader"])
    if seen != set(live):
        return None
    return {"send_ref": audit.ref(send), "writer": send["logical_id"],
            "writer_generation": send["generation"], "reader_windows": covered,
            "scope": ("received_or_actual_filtered_sample_in_still_live_generation_historical_windows"
                      if any("filter_decision_evidence" in row for row in covered) else
                      "received_sample_in_still_live_generation_historical_windows")}


def facts_from_events(events, runtime, config, *, capture_source=None):
    """Produce typed facts from persisted raw. All missing evidence is explicit."""
    pdp_control = isinstance(events, list) and any(
        isinstance(event, dict) and event.get("event") == "native_pdp_provider_selected"
        and event.get("capture_enabled") is False for event in events)
    refresh_control = (isinstance(events, list) and any(
        isinstance(event, dict) and event.get("event") in {"participant_refresh_observation", "participant_refresh_result"} for event in events))
    refresh_control = refresh_control or (isinstance(config.get("topology_program"), dict) and any(
        isinstance(action, dict) and action.get("op") == "update_participant_userdata"
        for action in config["topology_program"].get("actions", [])))
    facts = {"schema": SCHEMA, "producer_id": TOPOLOGY_FACT_PRODUCER, "target_id": TOPOLOGY_TARGET,
             "control_excluded": pdp_control or refresh_control,
             "environment_healthy": False, "process_healthy": False, "preconditions_reached": False,
             "endpoint_identity_complete": False, "observed_effects": [], "candidate_eligible": False,
             "seal_eligible": False, "execution_evidence": False, "diagnostic_valid": False,
             "fixture": True, "pairs": [], "deliveries": [], "unknown_reasons": [], "fact_values": {}}
    try:
        a = _Audit(events, runtime, config)
        if a.native_peer_reference is not None:
            facts.update(observation_mode="instrumentation",
                         fastdds_peer_instrumentation=a.native_peer_reference)
            if "observe_native_peers" in config:
                facts["observe_native_peers"] = config["observe_native_peers"]
        facts.update(raw={"path": a.path, "sha256": a.sha}, program_sha256=a.program_sha,
                     plan_hash=a.plan.plan_hash(), fixture=a.fixture, arm=config.get("arm"),
                     control_excluded=(pdp_control or refresh_control or config.get("observe_native_peers") is False
                        or config.get("arm") == "negative" or bool(config.get("control_kind"))
                        or config.get("candidate_source") == "control"
                        or any(e["event"] in {"fault_control_receipt", "harness_omission", "topology_guard_reject"} for e in a.events)),
                     input_binding_ref=a.ref(a.binding))
        failures = [a.ref(e) for e in a.events if e["event"] in _TOOL_EVENTS]
        facts["tool_error"] = bool(failures) or runtime.get("runtime_status") not in {"completed", "completed_with_unknown"}
        facts["tool_error_refs"] = failures
        guard = [e for e in a.events if e["event"] == "topology_guard_reject"]
        if guard:
            facts["guard_rejected_by"] = "harness"
            facts["guard_refs"] = [a.ref(e) for e in guard]
        facts["endpoint_lineage"] = a.identity()
        if a.group_lineage is not None:
            facts['participant_group_observations'] = snapshot(a.group_lineage['observations'])
            facts['participant_group_relation'] = {
                'observed': a.group_lineage['group_relation_observed'],
                'supporting_diagnostic': True, 'candidate_eligible': False}
        from .participant_identity import audit_participant_identity
        identity_audit = audit_participant_identity(a)
        if identity_audit is not None:
            facts["participant_identity"] = identity_audit
        facts["endpoint_identity_complete"] = True
        facts["action_receipts"] = a.actions()
        groups = sorted({x["params"]["concurrency_group"] for x in a.program["actions"]
                         if x.get("params", {}).get("concurrency_group") is not None})
        if groups:
            from r2f.runtime.topology_concurrency import audit_concurrency
            facts["concurrency_observations"] = [audit_concurrency(a.events, a.program["actions"], g) for g in groups]
            for observation in facts["concurrency_observations"]:
                observation["raw_evidence"] = [a.ref(a.events[i]) for i in observation["raw_refs"]]
        facts["diagnostic_valid"] = True
        boundaries = [e for e in a.events if e["event"] == "topology_state_boundary"]
        _require(len(boundaries) >= 2, "topology_state_boundaries_missing")
        projection = project_topology_state(a.program, a.path, before_ref=boundaries[0]["event_index"],
            after_ref=boundaries[-1]["event_index"], expected_sha256=a.sha)
        _require(projection["receipt"]["status"] != "invalid", "topology_state_projection_invalid")
        facts.update(state_before=projection["state_before"], state_after=projection["state_after"],
                     state_projection_receipt=projection["receipt"])
        sends = [e for e in a.events if e["event"] == "send"]
        for event in (e for e in a.events if e["event"] == "receive"):
            try:
                facts["deliveries"].append(a.delivery_identity(event, sends))
            except ValueError as exc:
                facts["unknown_reasons"].append(str(exc))
        facts["unknown_reasons_sample_identity"] = list(facts["unknown_reasons"])
        facts["pairs"] = _pair_facts(a, projection, facts["deliveries"])
        if config.get("native_matching_qos") is not None:
            from r2f.runtime.matching_qos import audit_run
            facts["matching_boundary"] = audit_run(a.program, a.events, config, fixture=a.fixture)
            facts["matching_boundary"]["pairs"] = [{
                "writer": pair["writer"], "reader": pair["reader"],
                "compatibility": pair["compatibility"], "observed_match": pair["observed_match"],
                "delivery_closure": pair["delivery_closure"],
                "policy_scope": pair.get("proofs", {}).get("compatibility", {}).get("detail", {}).get("policy_scope"),
                "raw_refs": pair.get("proofs", {}).get("compatibility", {}).get("refs", [])}
                for pair in facts["pairs"]]
            gaps = [gap for pair in facts["matching_boundary"]["pairs"]
                    for gap in (pair.get("policy_scope") or {}).get("observation_gaps", [])]
            facts["matching_boundary"]["observation_gaps"] = gaps
            facts["matching_observation_incomplete"] = bool(gaps)
            facts["unknown_reasons"].extend(gaps)
        from r2f.runtime.native_matching import audit_run as audit_native_controls
        controls = audit_native_controls(a.program, a.events, fixture=a.fixture, rmw=config.get("rmw"), raw_path=a.path)
        if controls:
            facts["native_matching_controls"] = controls
            gaps = [c.get("reason", "native_matching_control_incomplete") for c in controls if c["status"] == "unknown"]
            gaps.extend(gap for c in controls for gap in c.get("observation_gaps", []))
            facts["matching_observation_incomplete"] = bool(facts.get("matching_observation_incomplete") or gaps)
            facts["unknown_reasons"].extend(gaps)
        empty_windows = _historical_no_obligation_windows(a, facts["pairs"], projection)
        historical = _recreated_history_windows(a, facts["pairs"], projection, facts["deliveries"])
        facts["historical_live_windows"] = _live_history_windows(a, facts["pairs"], projection, facts["deliveries"])
        facts["historical_participant_group_windows"] = _participant_group_history_windows(
            a, projection, facts["deliveries"])
        if "episode" in a.program:
            from .topology_episode import audit_topology_episode
            facts["episode_observation"] = audit_topology_episode(a.program, runtime, config)
            _require(facts["episode_observation"]["structural_valid"], "topology_episode_evidence_invalid")
            facts["retired_episode_history_windows"] = _retired_episode_history_windows(
                a, projection, facts["deliveries"], facts["episode_observation"])
        from .topology_filter_decision import native_filter_decision_evidence
        facts["native_filter_decision_evidence"] = native_filter_decision_evidence(
            a.program, a.events, peer_reference=a.native_peer_reference, ref=a.ref)
        decision_evidence = facts["native_filter_decision_evidence"]
        # A declared capture with partial/broken worker provenance cannot waive
        # a historical delivery obligation using its surviving subset.
        filter_decisions = decision_evidence["observations"] if not decision_evidence["unknowns"] else []
        # Preserve the existing final-window gate exactly; do not relabel its
        # already-accounted sends as additional historical coverage.
        current_window_sends = {s["event_index"] for s in sends if any(
            p["writer"] == s.get("logical_id") and p.get("writer_generation") == s.get("generation")
            and p.get("delivery_closure") in {"closed", "closed_with_gap", "open"}
            and a.ref(s) in p.get("proofs", {}).get("delivery_closure", {}).get("refs", [])
            for p in facts["pairs"])}
        facts["historical_send_coverage"] = [coverage for send in sends
            if (coverage := (_recreated_history_send_coverage(a, historical, send)
                             or (_participant_group_send_coverage(a, facts["historical_participant_group_windows"],
                                                                 facts["historical_live_windows"], send)
                                 if send["event_index"] not in current_window_sends else None)
                             or (_retired_episode_send_coverage(a, facts["retired_episode_history_windows"], send)
                                 if "episode_observation" in facts else None)
                             or (_live_history_send_coverage(a, facts["historical_live_windows"], send,
                                                                filter_decisions=filter_decisions)
                                 if send["event_index"] not in current_window_sends else None))) is not None]
        facts["dynamic_filter_decision_effects"] = [
            row["filter_decision_evidence"] for coverage in facts["historical_send_coverage"]
            for row in coverage["reader_windows"] if "filter_decision_evidence" in row]
        covered_history = {c["send_ref"]["event_index"] for c in facts["historical_send_coverage"]}
        # A current label avoids double-counting, not the obligations to every
        # reader alive at send time (including retired peers and overlaps).
        qualified_current_window_sends = current_window_sends if a.group_lineage is None else {
            s["event_index"] for s in sends if s["event_index"] in current_window_sends
            and _live_history_send_coverage(a, facts["historical_participant_group_windows"]
                                           + facts["historical_live_windows"], s) is not None}
        facts["unresolved_historical_obligations"] = [a.ref(s) for s in sends
            if s["event_index"] not in qualified_current_window_sends and not _no_obligation_send_covered(a, empty_windows, s)
            and s["event_index"] not in covered_history]
        facts["application_audit"] = _application(a)
        facts["source_provenance_qualified"] = _source_qualification(a, capture_source=capture_source)
        facts["execution_evidence"] = not a.fixture
        env = [e for e in a.events if e["event"] == "environment_preflight"]
        locks = [e for e in a.events if e["event"] == "isolation_locks_acquired"]
        facts["environment_healthy"] = (not a.fixture and len(env) == 1 and env[0].get("ok") is True
                                         and len(locks) == 1 and bool(locks[0].get("locks")))
        if a.group_lineage is not None:
            # _Audit independently authenticates the sealed opt-in raw lineage:
            # one clean exit per process, context shutdown, and every endpoint's
            # cleanup. Shared endpoints must not invent per-endpoint exits.
            facts["process_healthy"] = (not facts["tool_error"]
                and a.end.get("cleanup_complete") is True)
        else:
            exits = [e for e in a.events if e["event"] == "topology_worker_exit"]
            facts["process_healthy"] = (not facts["tool_error"] and len(exits) == len(a.tokens)
                and len({tuple(e.get(k) for k in _LINEAGE) for e in exits}) == len(exits)
                and all(e.get("returncode") == 0 and (e.get("logical_id"), e.get("generation")) in a.tokens
                        and all(e.get(k) == a.tokens[(e["logical_id"], e["generation"])][k] for k in _LINEAGE)
                        for e in exits) and a.end.get("cleanup_complete") is True)
        # Precondition execution is separately checked against the actual target
        # in canonical_receipts; no helper boolean qualifies a step here.
        try:
            from r2f.core.scenario import Scenario
            from r2f.modules.s1.topology_targets import topology_target_declaration
            scenario = Scenario(topology_target_declaration(), config["rmw"], config["arm"],
                                config.get("repeat", 0), mutation_plan=a.plan)
            facts["canonical_step_receipts"] = canonical_receipts(
                scenario, runtime, events, config=config, capture_source=capture_source)
            facts["preconditions_reached"] = len(facts["canonical_step_receipts"]) == len(scenario.state_steps)
        except (ValueError, KeyError, TypeError, OSError) as exc:
            facts["canonical_receipt_error"] = str(exc)
        facts["unknown_reasons"].extend(projection["receipt"].get("reasons", []))
        facts["candidate_eligible"] = (facts["environment_healthy"] and facts["process_healthy"]
            and facts["source_provenance_qualified"] and facts["preconditions_reached"] and not facts["control_excluded"]
            and facts["application_audit"].get("status") == "application_observed"
            and facts["application_audit"].get("execution_evidence") is True
            and facts["application_audit"].get("canonical_receipt_matches") is True
            and not facts.get("matching_observation_incomplete"))
        if "episode_observation" in facts:
            observation = facts["episode_observation"]
            facts["candidate_eligible"] = (facts["candidate_eligible"] and observation["completed"]
                and observation["execution_evidence"] and observation["checkpoint_observations_complete"]
                and observation["reset_verified"])
    except (KeyError, ValueError, TypeError, OSError, UnicodeError, IndexError) as exc:
        facts["evidence_error"] = str(exc)
        facts["diagnostic_valid"] = False
        facts["endpoint_identity_complete"] = False
        facts["unknown_reasons"].append(str(exc))
    facts["observation_complete"] = facts.get("state_after", {}).get("observation_completeness") == "complete"
    facts["window"] = {"complete": facts["observation_complete"]}
    facts["fact_values"] = {
        TOPOLOGY_FACT_EXECUTION: facts["execution_evidence"] if facts["diagnostic_valid"] else None,
        TOPOLOGY_FACT_HEALTH: (False if facts.get("tool_error") else True
            if facts["diagnostic_valid"] and facts["environment_healthy"] and facts["process_healthy"] else None),
        TOPOLOGY_FACT_IDENTITY: True if facts["diagnostic_valid"] and facts["endpoint_identity_complete"] else None,
        TOPOLOGY_FACT_GRAPH: None, TOPOLOGY_FACT_DELIVERY: None,
        TOPOLOGY_FACT_GUARDS: None,
    }
    if "participant_identity" in facts:
        status = facts["participant_identity"]["status"]
        facts["fact_values"][TOPOLOGY_FACT_IDENTITY] = (True if status == "consistent" else
            False if status == "conflict" else None)
    from r2f.modules.s1.topology_oracles import resolve_fact
    for fact_id in (TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY, TOPOLOGY_FACT_GUARDS):
        facts["fact_values"][fact_id] = resolve_fact(fact_id, facts)
    if facts["diagnostic_valid"] and facts["action_receipts"]:
        facts["observed_effects"].append("program_correlated_action_receipts")
    if facts["fact_values"][TOPOLOGY_FACT_GRAPH] is True and facts["fact_values"][TOPOLOGY_FACT_DELIVERY] is True:
        facts["observed_effects"].append("observed_consistency")
    return facts


def canonical_receipts(scenario, runtime, events, *, config=None, capture_source=None):
    """Raw-backed core StepReceipts, with independently established requires.

    This does not advance execution stages. Fixtures can be audited, but cannot
    pass ingest's unchanged environment/source/process gates.
    """
    _require(scenario.target_id == TOPOLOGY_TARGET, "topology_exact_target_required")
    # Config is not reconstructed from result facts. The producer/runner supplies
    # this input envelope, bound by hash in the raw route receipt.
    config = config if config is not None else runtime.get("topology_config")
    _require(isinstance(config, dict), "topology_config_input_envelope_missing")
    a = _Audit(events, runtime, config)
    _require([s.step_id for s in scenario.state_steps] == ["admit_topology", "bind_topology_input"],
             "topology_premutation_step_contract_mismatch")
    env, locks = _one(a.events, "environment_preflight"), _one(a.events, "isolation_locks_acquired")
    # Only pre-mutation source observations may establish admission. The later
    # process/action/application/observation obligations remain in typed facts.
    first = _one(a.events, "state_step_before", step_id="admit_topology")
    _chain(env, first)
    _chain(locks, first)
    source_ok = _source_qualification(a, before=first, capture_source=capture_source)
    state = {"environment_ok": env.get("ok") is True, "locks_held": bool(locks.get("locks")),
             "source_identity_ok": source_ok, "topology_capability_admitted": True}
    receipts = []
    previous = None
    for step in scenario.state_steps:
        before = _one(a.events, "state_step_before", step_id=step.step_id)
        after = _one(a.events, "state_step_after", step_id=step.step_id)
        _chain(before, after, a.begin)
        if previous is not None:
            _chain(previous, before)
        _require(before.get("action") == step.action and after.get("action") == step.action
                 and before.get("target_contract_sha256") == canonical_hash(scenario.target.to_dict())
                 and after.get("target_contract_sha256") == canonical_hash(scenario.target.to_dict()),
                 "canonical_step_contract_mismatch")
        _require(before.get("before") == state, "canonical_state_before_not_observed")
        checks = {c.condition_id: c.evaluate(state) for c in step.requires}
        _require(before.get("requires") == checks and all(checks.values()), "canonical_requires_not_observed")
        new = dict(state)
        if step.step_id == "admit_topology":
            new["topology_admitted"] = True
        else:
            # _Audit has already cross-checked the persistent sealed binding
            # against the exact core plan, every input layer and final program.
            _chain(a.binding, before)
            proof = after.get("evidence", {})
            _require(proof.get("input_binding_ref") == a.binding["event_index"]
                     and proof.get("route_binding") == a.binding["receipt"]["route_binding"],
                     "canonical_input_binding_proof_mismatch")
            new["topology_input_bound"] = True
        _require(after.get("before") == state and after.get("requires") == checks
                 and after.get("status") == "executed"
                 and after.get("after") == new and after.get("satisfied") is True,
                 "canonical_state_after_not_observed")
        receipts.append(StepReceipt(step.step_id, step.action, dict(state), new, checks, "executed",
            datetime.fromtimestamp(before["time_ns"] / 1e9, timezone.utc).isoformat(),
            datetime.fromtimestamp(after["time_ns"] / 1e9, timezone.utc).isoformat(),
            {"raw_before_index": before["event_index"], "raw_after_index": after["event_index"],
             "raw_before": a.ref(before), "raw_after": a.ref(after)}).to_dict())
        state, previous = new, after
    return receipts


class TopologyFactProducer:
    id = TOPOLOGY_FACT_PRODUCER

    def produce(self, raw, *, capture_source=None):
        if capture_source is None:
            return facts_from_events(raw["events"], raw["runtime"], raw["config"])
        return facts_from_events(raw["events"], raw["runtime"], raw["config"],
                                 capture_source=capture_source)


# Spelled-out aliases retained for the registry handoff, not target aliases.
facts_from_topology_events = facts_from_events
