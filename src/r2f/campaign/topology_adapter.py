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
        # validate_topology_route explicitly invokes validate_topology_plan,
        # including deterministic input-layer lineage; do not regenerate input.
        self.layers = snapshot([op.parameters["input_layer_receipts"][0] for op in self.plan.operations])
        _require(config.get("input_layer_receipts") == self.layers, "input_layer_lineage_mismatch")
        _require(config.get("plan_hash") == self.plan.plan_hash(), "plan_hash_mismatch")
        _require(config.get("final_input") == self.plan.final_config
                 and set(self.plan.final_config) == {"topology_program"}, "final_program_mismatch")
        self.program = self.plan.final_config["topology_program"]
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
                _require(actual_qos == public_input or self.fixture and actual_qos == spec.get("qos"),
                         "endpoint_actual_qos_mismatch")
                self.tokens[key] = event
            elif kind in {"endpoint_destroyed", "topology_endpoint_destroyed"}:
                key = (eid, event.get("generation"))
                _require(key in self.tokens and key not in retired, "retirement_unknown_generation")
                token = self.tokens[key]
                _require(event.get("pid") == token["pid"] and event.get("endpoint_gid") == token["endpoint_gid"]
                         and event.get("cleanup_status") == "closed", "retirement_identity_or_cleanup_mismatch")
                _chain(token, event)
                retired.add(key)
            elif kind in _WORKER_EVENTS and eid is not None and kind not in {"endpoint_create_begin", "worker_ready"}:
                key = (eid, event.get("generation"))
                _require(key in self.tokens and key not in retired, "worker_event_unknown_or_retired_generation")
                self.bound(event, self.tokens[key])
        for event in self.events:
            if event["event"] in {"endpoint_create_begin", "worker_ready"}:
                token = self.tokens.get((event.get("logical_id"), event.get("generation")))
                _require(token is not None and all(event.get(k) == token[k] for k in _LINEAGE),
                         "worker_start_lineage_mismatch")
                _chain(event, token)
        _require(bool(self.tokens) or not self.specs, "endpoint_creation_raw_missing")
        return [{**{k: t[k] for k in _LINEAGE}, "endpoint_gid": t["endpoint_gid"],
                 "creation_ref": self.ref(t)} for t in self.tokens.values()]

    def bound(self, event, token):
        _require(all(event.get(k) == token[k] for k in _LINEAGE), "worker_event_lineage_mismatch")
        _require(event["event_index"] > token["event_index"] and event["monotonic_ns"] >= token["monotonic_ns"],
                 "worker_event_before_creation")
        if event.get("event") == "send":
            _require(event.get("writer_gid") == token["endpoint_gid"] and event.get("publish_returned") is True,
                     "send_gid_or_publish_return_missing")
        if event.get("event") == "receive":
            _require(event.get("reader_gid") == token["endpoint_gid"], "receive_reader_gid_mismatch")

    def action_before(self, action, begin):
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
        if eid is not None:
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
            self.action_before(action, begin)
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
            _require(receipt.get("satisfied") is True and all(r["status"] == "satisfied" for r in receipt["requires"])
                     and receipt.get("disposition") in {"applied", "effect_observed", "observed", "satisfied"},
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
            elif op == "assert_liveliness":
                _require(any(e["event"] == "liveliness_asserted" for e in inside), "action_assert_raw_missing")
            self.action_receipts.append({"action_id": aid, "before": snapshot(receipt["before"]),
                "after": snapshot(receipt["after"]), "requires": snapshot(receipt["requires"]),
                "terminal": receipt["disposition"], "raw_begin": self.ref(begin), "raw_after": self.ref(after),
                "raw_done": self.ref(done)})
            previous = done
        _chain(previous, self.end)
        return self.action_receipts

    def delivery_identity(self, event, sends):
        """Per-sample attribution, including the explicit NON-direct-GID basis."""
        token = self.tokens.get((event.get("logical_id"), event.get("generation")))
        _require(token is not None, "receive_generation_unbound")
        self.bound(event, token)
        _require(event.get("payload_run_id") == self.config["run_id"], "receive_payload_old_run")
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


def _source_qualification(audit, *, before=None):
    """Artifact claims alone never confer source-frozen provenance."""
    source = audit.config.get("source_manifest")
    if audit.fixture or not isinstance(source, dict) or source.get("source_frozen") is not True:
        return False
    try:
        from .identity import source_manifest
        checks = [e for e in audit.events if e["event"] == "topology_source_checked"]
        if before is not None:
            prefix = [e for e in checks if e["event_index"] < before["event_index"]
                      and e["monotonic_ns"] < before["monotonic_ns"]]
            return (source == source_manifest(Path(source["source_repo"])) and bool(prefix)
                    and any(e.get("phase") == "after_preflight" for e in prefix)
                    and all(e.get("source") == source for e in prefix))
        actions = [e for e in audit.events if e["event"] == "topology_action_begin"]
        return (source == source_manifest(Path(source["source_repo"])) and len(checks) >= 2
                and all(e.get("source") == source for e in checks)
                and checks[-1].get("phase") == "after_cleanup"
                and (not actions or checks[0]["event_index"] < actions[0]["event_index"]
                     and checks[-1]["event_index"] > actions[-1]["event_index"]))
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
            p.get("reliability") == "reliable" and (p.get("history") == "keep_all"
                or p.get("history") == "keep_last" and _integer(p.get("depth"), max(1, len(expected))))
            for p in policies.values()))
        row["filter_free"] = all(audit.specs[eid].get("filter") is None for eid in (w, r))
        row["writer_generation"] = side["identity"].get(w, {}).get("generation")
        row["reader_generation"] = side["identity"].get(r, {}).get("generation")
        row.update(_delivery_window(audit, row, deliveries))
        rows.append(row)
    return rows


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


def facts_from_events(events, runtime, config):
    """Produce typed facts from persisted raw. All missing evidence is explicit."""
    facts = {"schema": SCHEMA, "producer_id": TOPOLOGY_FACT_PRODUCER, "target_id": TOPOLOGY_TARGET,
             "environment_healthy": False, "process_healthy": False, "preconditions_reached": False,
             "endpoint_identity_complete": False, "observed_effects": [], "candidate_eligible": False,
             "seal_eligible": False, "execution_evidence": False, "diagnostic_valid": False,
             "fixture": True, "pairs": [], "deliveries": [], "unknown_reasons": [], "fact_values": {}}
    try:
        a = _Audit(events, runtime, config)
        facts.update(raw={"path": a.path, "sha256": a.sha}, program_sha256=a.program_sha,
                     plan_hash=a.plan.plan_hash(), fixture=a.fixture, arm=config.get("arm"),
                     control_excluded=(config.get("arm") == "negative" or bool(config.get("control_kind"))
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
        facts["endpoint_identity_complete"] = True
        facts["action_receipts"] = a.actions()
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
        empty_windows = _historical_no_obligation_windows(a, facts["pairs"], projection)
        facts["unresolved_historical_obligations"] = [a.ref(s) for s in sends if not any(
            p["writer"] == s.get("logical_id") and p.get("writer_generation") == s.get("generation")
            and p.get("delivery_closure") in {"closed", "closed_with_gap", "open"}
            and a.ref(s) in p.get("proofs", {}).get("delivery_closure", {}).get("refs", [])
            for p in facts["pairs"]) and not _no_obligation_send_covered(a, empty_windows, s)]
        facts["application_audit"] = _application(a)
        facts["source_provenance_qualified"] = _source_qualification(a)
        facts["execution_evidence"] = not a.fixture
        env = [e for e in a.events if e["event"] == "environment_preflight"]
        locks = [e for e in a.events if e["event"] == "isolation_locks_acquired"]
        facts["environment_healthy"] = (not a.fixture and len(env) == 1 and env[0].get("ok") is True
                                         and len(locks) == 1 and bool(locks[0].get("locks")))
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
            facts["canonical_step_receipts"] = canonical_receipts(scenario, runtime, events, config=config)
            facts["preconditions_reached"] = len(facts["canonical_step_receipts"]) == len(scenario.state_steps)
        except (ValueError, KeyError, TypeError, OSError) as exc:
            facts["canonical_receipt_error"] = str(exc)
        facts["unknown_reasons"].extend(projection["receipt"].get("reasons", []))
        facts["candidate_eligible"] = (facts["environment_healthy"] and facts["process_healthy"]
            and facts["source_provenance_qualified"] and facts["preconditions_reached"] and not facts["control_excluded"]
            and facts["application_audit"].get("status") == "application_observed"
            and facts["application_audit"].get("execution_evidence") is True
            and facts["application_audit"].get("canonical_receipt_matches") is True)
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
    from r2f.modules.s1.topology_oracles import resolve_fact
    for fact_id in (TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY, TOPOLOGY_FACT_GUARDS):
        facts["fact_values"][fact_id] = resolve_fact(fact_id, facts)
    if facts["diagnostic_valid"] and facts["action_receipts"]:
        facts["observed_effects"].append("program_correlated_action_receipts")
    if facts["fact_values"][TOPOLOGY_FACT_GRAPH] is True and facts["fact_values"][TOPOLOGY_FACT_DELIVERY] is True:
        facts["observed_effects"].append("observed_consistency")
    return facts


def canonical_receipts(scenario, runtime, events, *, config=None):
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
    source_ok = _source_qualification(a, before=first)
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

    def produce(self, raw):
        return facts_from_events(raw["events"], raw["runtime"], raw["config"])


# Spelled-out aliases retained for the registry handoff, not target aliases.
facts_from_topology_events = facts_from_events
