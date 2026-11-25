"""Raw-backed N-endpoint projection. Pure artifact processing; never runs DDS.

Public API: project_topology_state(program, raw_path, *, before_ref, after_ref,
                                  expected_sha256=None).
Returns {state_before, state_after, receipt, eligible}. ``eligible`` is only a
state-coverage gate, NOT an oracle, candidate, execution-stage or seal decision.
Input program/fixture declarations and helper pair.status are never proof.

Raw protocol (additive to topology_pair JSONL; missing additions fail closed):
* input_binding.receipt.program_sha256 binds the program. Endpoint lifecycle is
  endpoint_created / endpoint_destroyed or topology_endpoint_destroyed, carrying
  logical_id, generation, endpoint_gid, pid (creation also process_start_ticks,
  endpoint_generation, topic, message_type). Logical generations must be declared
  by create/recreate actions; history stays in receipt, not state identity.
* Each endpoint snapshot chain is worker_command_requested(command='snapshot')
  or topology_snapshot_requested, then match_snapshot, graph_snapshot, command_done.
  Same logical_id/generation/pid/endpoint_generation/command_id, current GID,
  strictly request < match < graph < ACK. These are command ACKs, not DDS ACKs.
* Per-peer match proof is match_snapshot.matched_peer_gids (complete list),
  peer_gids_complete=True and peer_proof_source='dds_matched_endpoint_api',
  or an explicit fastdds_native_stateful_peer_snapshot with validated native
  provenance, current command capture and strictly fresh observation ordinal.
  Declared native evidence cannot downgrade to public/count/isolation proof.
  Alternatively topology_controlled_isolation with nonempty isolation_id,
  writer/reader generations, inventory_complete=True, graph_refs to the two
  current graphs. Both inventories must contain ONLY that exact writer/reader.
  Positive topic counts + graph membership NEVER suffice in multi-peer graphs.
* Delivery uses topology_delivery_window_open / topology_delivery_window_closed
  with writer/reader generations; close.open_ref binds open; close may name
  sentinel_sequence. Actual sends/receives inside the window establish samples.
  Missing publisher_gid requires unique actual send/body/hash/run/identity
  correlation (endpoint_correlated_payload), or named controlled isolation,
  never multiwriter totals. Delivery does not backfill per-peer match proof.
  Closure requires a received later same-writer-generation sentinel;
  finite trailing loss stays open. No cross-writer ordering is required.
* topology_network_phase is measured carrier evidence, not program intent:
  network_phase, recovery_cycle, observed=True, implementation. normal requires
  audit_neutral=True; drop_active requires applied=True, discovery_packets>0;
  restore_pending requires rules_removed=True. topology_network_observation links
  carrier_ref to it with same phase/cycle and source='carrier_audit'. Restored
  verification additionally requires discovery_refs to identity-bound
  topology_discovery_activity plus fresh pair proof and closed postrestore data.
* A boundary (topology_state_boundary, state_step_before/after or
  topology_pair_observation) explicitly records lifecycle_complete,
  observation_complete and window_complete. Unknowns cannot be upgraded by flags.

Embedded raw refs are integer line indexes (avoiding self-referential file
hashes), or externally supplied {path,sha256,event_index} into the SAME verified
raw file. All returned receipt refs are fully path/hash bound. Raw provenance
must be ros2_public_api with prepared real_ros=True and no fixture markers to be
coverage eligible. This audits artifact consistency, not cryptographic runtime
attestation/source freeze. Synthetic fixtures always remain ineligible.
"""
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.publish_occurrence import producer_occurrence_witness
from r2f.campaign.topology_sample_attribution import declares_native_publish, native_delivery_identity
from copy import deepcopy
import hashlib
import json
import math
import re
from pathlib import Path

from r2f.core.evidence import canonical_hash
from r2f.core.topology_state import (
    PAIR_DOMAINS, abstract_topology_state, logical_endpoints, topology_state_id,
)
from r2f.instrumentation.sedp import check_offered_requested
from r2f.instrumentation.fastdds_peer.receipt import validate_observation

SCHEMA = "r2f.s1-topology-state-projection/v1"
_BOUNDARIES = {"topology_state_boundary", "state_step_before", "state_step_after", "topology_pair_observation"}


def declares_fastdds_peer(row):
    """Select the strong FastDDS contract without authenticating its receipt.

    Cyclone public API observations share the historical carrier field. Only
    their known schema/source pair is exempt; malformed or unknown declarations
    must never gain the legacy public/count/isolation fallback.
    """
    observation = row.get("native_peer_observation")
    cyclone_public = (isinstance(observation, dict)
                      and observation.get("schema") == "r2f.cyclone-matched-peer-observation/v1"
                      and row.get("peer_proof_source") == "dds_matched_endpoint_api")
    return (row.get("event") == "native_peer_provider_selected"
            or row.get("peer_proof_source") == "fastdds_native_stateful_peer_snapshot"
            or row.get("observation_mode") == "instrumentation"
            or "native_peer_observation" in row and not cyclone_public)


def _text(value):
    return isinstance(value, str) and bool(value)


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _gid(value):
    if not _text(value):
        return None
    return value[:32].lower() if len(value) == 48 and value[32:] == "0" * 16 else value.lower()


def _pointer(value):
    return value.replace("~", "~0").replace("/", "~1")


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_raw_json_key:" + key)
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError("nonfinite_raw_json:" + value)


def _finite_float(value):
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("nonfinite_raw_json:" + value)
    return parsed


class _Projection:
    def __init__(self, program, path, digest, events, receipt):
        self.program, self.path, self.digest = program, path, digest
        self.events, self.receipt = events, receipt
        self.roles = logical_endpoints(program)
        self.group_destroy_prefixes = {}
        self.declared = {eid: set() for eid in self.roles}
        for a in program["actions"]:
            if isinstance(a, dict) and a.get("op") in ("create", "recreate"):
                eid, gen = a.get("endpoint_id"), a.get("generation")
                if _text(eid) and eid in self.declared and _integer(gen, 1):
                    self.declared[eid].add(gen)

    def ref(self, event):
        return {"path": self.path, "sha256": self.digest, "event_index": event["event_index"]}

    def resolve(self, ref, *, boundary=None, integer=True):
        if integer and _integer(ref):
            index = ref
        else:
            if (not isinstance(ref, dict) or set(ref) != {"path", "sha256", "event_index"}
                    or ref["path"] != self.path or ref["sha256"] != self.digest
                    or not _integer(ref["event_index"])):
                raise ValueError("malformed_or_unbound_raw_ref")
            index = ref["event_index"]
        if index >= len(self.events):
            raise ValueError("raw_ref_out_of_range")
        event = self.events[index]
        if boundary is not None:
            if (index > boundary["event_index"]
                    or event["monotonic_ns"] > boundary["monotonic_ns"]):
                raise ValueError("raw_ref_after_boundary_chronology")
        return event

    def chain_order(self, rows):
        indexes = [e["event_index"] for e in rows]
        times = [e["monotonic_ns"] for e in rows]
        if (indexes != sorted(set(indexes)) or times != sorted(times)):
            raise ValueError("evidence_chain_chronology_invalid")

    def component(self, side, path, value, rows=(), *, status="observed", reason=None, **detail):
        boundary = self.receipt[side]["boundary_ref"]["event_index"]
        end = self.events[boundary]
        refs = [self.ref(e) for e in sorted({e["event_index"]: e for e in rows}.values(),
                                          key=lambda e: e["event_index"])]
        for ref in refs:
            self.resolve(ref, boundary=end)
        if status in ("observed", "not_applicable") and not refs:
            raise ValueError("observed_component_without_raw_refs")
        self.receipt[side]["component_refs"][path] = {
            "status": status, "value": deepcopy(value), "refs": refs, "reason": reason, "detail": detail,
        }
        if reason:
            self.receipt["reasons"].append(side + ":" + path + ":" + reason)
        return value

    def unknown(self, side, path, reason, rows=(), *, contradiction=False, **detail):
        return self.component(side, path, "unknown", rows,
                              status="contradiction" if contradiction else "unknown",
                              reason=reason, **detail)

    def prefix(self, boundary):
        return self.events[:boundary["event_index"] + 1]

    def bound(self, event, token, *, gid_field=None):
        token = endpoint_identity_at(self.events, token, before_index=event["event_index"] + 1)
        valid_ref = token["identity_valid_after_ref"]
        if (event["monotonic_ns"] < token["identity_valid_after_ns"]
                or (event["event_index"] < valid_ref if event.get("event") == "command_done"
                    else event["event_index"] <= valid_ref)):
            raise ValueError("observation_before_identity_ack")
        run_id = self.events[self.receipt["input_ref"]["event_index"]].get("run_id")
        if not _text(run_id) or event.get("run_id") != run_id or token.get("run_id") != run_id:
            raise ValueError("endpoint_worker_run_binding_mismatch")
        if (not _integer(event.get("generation"), 1) or not _integer(event.get("pid"), 1)
                or not _text(event.get("logical_id")) or not _text(event.get("endpoint_generation"))):
            raise ValueError("endpoint_worker_identity_fields_malformed")
        for k in ("logical_id", "generation", "pid", "endpoint_generation"):
            if event.get(k) != token.get(k):
                raise ValueError("endpoint_identity_or_old_generation_mismatch:" + k)
        if ("process_start_ticks" in event and
                (not _integer(event["process_start_ticks"], 1)
                 or event["process_start_ticks"] != token["process_start_ticks"])):
            raise ValueError("endpoint_process_start_ticks_mismatch")
        if gid_field and _gid(event.get(gid_field)) != _gid(token["endpoint_gid"]):
            raise ValueError("endpoint_gid_mismatch:" + gid_field)
        if event["event_index"] < token["event_index"] or event["monotonic_ns"] < token["monotonic_ns"]:
            raise ValueError("observation_before_current_generation")

    def endpoints(self, side, boundary, lifecycle):
        facts, tokens = {}, {}
        prefix = self.prefix(boundary)
        from r2f.runtime.participant_groups import enabled, closed_destroy_receipts
        def same_destroy(event):
            if not enabled(self.program):
                return False
            index = boundary["event_index"]
            if index not in self.group_destroy_prefixes:
                self.group_destroy_prefixes[index] = closed_destroy_receipts(
                    self.program, self.events, before_index=index + 1)
            return any(r["controller_ref"] == event["event_index"]
                       for r in self.group_destroy_prefixes[index])
        for eid in self.roles:
            path = "/endpoints/" + _pointer(eid)
            history = [e for e in prefix if e.get("logical_id") == eid
                       and e["event"] in ("endpoint_created", "endpoint_destroyed", "topology_endpoint_destroyed",
                                          "endpoint_identity_transition")]
            self.receipt[side]["history"][eid] = [self.ref(e) for e in history]
            try:
                if history:
                    self.chain_order(history)
                current, generation, alive = None, 0, False
                old_gids, old_workers = set(), set()
                for event in history:
                    if event["event"] == "endpoint_created":
                        gen = event.get("generation")
                        if (not _integer(gen, 1) or gen not in self.declared[eid]
                                or gen != generation + 1 or alive):
                            raise ValueError("undeclared_duplicate_or_old_logical_generation")
                        input_run = self.events[self.receipt["input_ref"]["event_index"]].get("run_id")
                        if not _text(input_run) or event.get("run_id") != input_run:
                            raise ValueError("endpoint_creation_run_binding_missing_or_mismatch")
                        if (not _gid(event.get("endpoint_gid")) or not _integer(event.get("pid"), 1)
                                or not _integer(event.get("process_start_ticks"), 1)
                                or not _text(event.get("endpoint_generation"))
                                or not _text(event.get("topic")) or not _text(event.get("message_type"))):
                            raise ValueError("endpoint_creation_identity_incomplete")
                        gid, worker = _gid(event["endpoint_gid"]), (event["pid"], event["process_start_ticks"], event["endpoint_generation"])
                        if gid in old_gids or worker in old_workers:
                            raise ValueError("generation_reused_raw_identity")
                        old_gids.add(gid)
                        old_workers.add(worker)
                        current, generation, alive = event, gen, True
                    elif event["event"] == "endpoint_identity_transition":
                        if (current is None or not alive or event.get("generation") != generation):
                            raise ValueError("transition_without_current_generation")
                        # Full command authentication is performed by the resolver
                        # at the boundary; this pass independently fences lifecycle.
                        old_gids.add(_gid(event.get("after_gid")))
                    else:
                        if (not alive and current is not None and event.get("generation") == generation
                                and same_destroy(event)):
                            continue  # Controller receipt for the already verified worker cleanup.
                        if current is None or not alive:
                            raise ValueError("retirement_without_current_generation")
                        current = endpoint_identity_at(self.events, current,
                            before_index=event["event_index"] + 1)
                        if event["monotonic_ns"] < current["identity_valid_after_ns"]:
                            raise ValueError("retirement_before_identity_ack")
                        # Controller retirement events omit endpoint_generation;
                        # PID/GID/logical ordinal must still agree with creation.
                        if (not _integer(event.get("generation"), 1) or not _integer(event.get("pid"), 1)
                                or event.get("run_id") != current.get("run_id")
                                or any(event.get(k) != current[k] for k in ("generation", "pid"))
                                or _gid(event.get("endpoint_gid")) != _gid(current["endpoint_gid"])
                                or event.get("cleanup_status") != "closed"):
                            raise ValueError("retirement_identity_or_cleanup_mismatch")
                        self.chain_order([current, event])
                        alive = False
                if not lifecycle:
                    raise ValueError("lifecycle_capture_incomplete")
                if current is not None and current["monotonic_ns"] > boundary["monotonic_ns"]:
                    raise ValueError("endpoint_creation_after_boundary_chronology")
                if current is not None:
                    current = endpoint_identity_at(self.events, current,
                        before_index=boundary["event_index"] + 1)
                    if boundary["monotonic_ns"] < current["identity_valid_after_ns"]:
                        raise ValueError("boundary_before_identity_ack")
                if alive:
                    tokens[eid] = current
                life = "alive" if alive else "retired" if generation else "absent"
                facts[eid] = {"generation": generation, "liveness": life}
                rows = history + [boundary]
                self.component(side, path + "/generation", generation, rows)
                self.component(side, path + "/liveness", life, rows)
                if current is not None:
                    self.receipt[side]["identity"][eid] = {
                        k: current[k] for k in ("generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation",
                                               "incarnation_ordinal", "identity_transition_ref")
                    }
                    self.receipt[side]["identity"][eid]["creation_ref"] = self.ref(current)
            except ValueError as exc:
                facts[eid] = {"generation": None, "liveness": "unknown"}
                self.unknown(side, path + "/generation", str(exc), history, contradiction="incomplete" not in str(exc))
                self.unknown(side, path + "/liveness", str(exc), history, contradiction="incomplete" not in str(exc))
        # Raw IDs must be one-to-one across live logical endpoints.
        gids = [_gid(t["endpoint_gid"]) for t in tokens.values()]
        if len(gids) != len(set(gids)):
            for eid in list(tokens):
                facts[eid] = {"generation": None, "liveness": "unknown"}
                path = "/endpoints/" + _pointer(eid)
                for name in ("generation", "liveness"):
                    self.unknown(side, path + "/" + name, "live_logical_ids_share_raw_gid", [tokens[eid]], contradiction=True)
            tokens.clear()
        return facts, tokens

    def snapshot_chain(self, token, boundary):
        candidates = [e for e in self.prefix(boundary) if e["event"] == "match_snapshot"
                      and e.get("logical_id") == token["logical_id"]
                      and e.get("generation") == token["generation"]]
        if not candidates:
            raise ValueError("fresh_current_generation_snapshot_missing")
        match = candidates[-1]
        at_match = endpoint_identity_at(self.events, token, before_index=match["event_index"] + 1)
        at_boundary = endpoint_identity_at(self.events, token, before_index=boundary["event_index"] + 1)
        if at_match["endpoint_gid"] != at_boundary["endpoint_gid"]:
            raise ValueError("snapshot_before_current_incarnation")
        self.bound(match, token, gid_field="endpoint_gid")
        cid = match.get("command_id")
        if not (_text(cid) or _integer(cid)):
            raise ValueError("snapshot_command_id_missing")
        related = [e for e in self.prefix(boundary)
                   if e.get("logical_id") == token["logical_id"]
                   and e.get("generation") == token["generation"] and e.get("command_id") == cid]
        def one(kinds):
            rows = [e for e in related if e["event"] in kinds]
            if len(rows) != 1:
                raise ValueError("snapshot_request_graph_ack_missing_or_duplicate")
            self.bound(rows[0], token)
            return rows[0]
        if len([e for e in related if e["event"] == "match_snapshot"]) != 1:
            raise ValueError("snapshot_match_duplicate")
        request = one({"worker_command_requested", "topology_snapshot_requested"})
        if request.get("command", "snapshot" if request["event"] == "topology_snapshot_requested" else None) != "snapshot":
            raise ValueError("snapshot_request_wrong_command")
        graph, ack = one({"graph_snapshot"}), one({"command_done"})
        self.chain_order([request, match, graph, ack])
        if (not _integer(match.get("matched_count"))
                or not isinstance(graph.get("publishers"), list)
                or not isinstance(graph.get("subscriptions"), list)):
            raise ValueError("snapshot_graph_or_count_malformed")
        for key in ("publishers", "subscriptions"):
            gids = [_gid(row.get("endpoint_gid")) if isinstance(row, dict) else None for row in graph[key]]
            if None in gids or len(gids) != len(set(gids)):
                raise ValueError("snapshot_graph_identity_malformed_or_duplicate")
        return request, match, graph, ack

    def isolation(self, w, r, wc, rc, boundary):
        proofs = [e for e in self.prefix(boundary) if e["event"] == "topology_controlled_isolation"
                  and e.get("writer") == w["logical_id"] and e.get("reader") == r["logical_id"]
                  and e.get("writer_generation") == w["generation"]
                  and e.get("reader_generation") == r["generation"]]
        if not proofs:
            raise ValueError("no_per_peer_match_proof_or_named_controlled_isolation")
        proof = proofs[-1]
        if (not _integer(proof.get("writer_generation"), 1) or not _integer(proof.get("reader_generation"), 1)
                or not _text(proof.get("isolation_id")) or not _text(proof.get("mechanism"))
                or proof.get("inventory_complete") is not True
                or not isinstance(proof.get("graph_refs"), list) or len(proof["graph_refs"]) != 2):
            raise ValueError("controlled_isolation_proof_incomplete")
        graphs = [self.resolve(ref, boundary=proof) for ref in proof["graph_refs"]]
        if {g["event_index"] for g in graphs} != {wc[2]["event_index"], rc[2]["event_index"]}:
            raise ValueError("controlled_isolation_graph_refs_stale")
        for graph in graphs:
            if ([_gid(row["endpoint_gid"]) for row in graph["publishers"]] != [_gid(w["endpoint_gid"])]
                    or [_gid(row["endpoint_gid"]) for row in graph["subscriptions"]] != [_gid(r["endpoint_gid"])]):
                raise ValueError("controlled_isolation_inventory_has_other_peers")
        return proof

    def match(self, w, r, wc, rc, boundary):
        direct, native_sources = [], []
        for token, peer, chain in ((w, r, wc), (r, w, rc)):
            row = chain[1]
            source = row.get("peer_proof_source")
            selections = [e for e in self.events[:chain[0]["event_index"]]
                          if e.get("event") == "native_peer_provider_selected"
                          and all(token.get(k) is not None and e.get(k) == token[k]
                                  for k in ("pid", "process_start_ticks", "endpoint_generation"))]
            native_declared = (declares_fastdds_peer(row)
                               or token.get("observation_mode") == "instrumentation"
                               or bool(selections))
            if native_declared and source != "fastdds_native_stateful_peer_snapshot":
                raise ValueError("native_peer_proof_source_mislabeled")
            complete = (row.get("peer_gids_complete") is True
                        and isinstance(row.get("matched_peer_gids"), list))
            if native_declared and not complete:
                raise ValueError("native_peer_membership_incomplete")
            if not native_declared and "native_peer_observation" in row:
                # The known Cyclone schema only dispatches the public contract;
                # it cannot authenticate a complete capture or permit isolation
                # to rescue a contradictory/incomplete public API receipt.
                observation = row["native_peer_observation"]
                if (observation.get("complete") is not True or not complete
                        or observation.get("local_gid") != row.get("endpoint_gid")
                        or observation.get("role_kind") != self.roles[token["logical_id"]]
                        or not _integer(observation.get("domain_id"))
                        or observation["domain_id"] != row.get("domain_id")
                        or observation["domain_id"] != token.get("domain_id")
                        or row.get("rmw_identifier") != "rmw_cyclonedds_cpp"
                        or token.get("rmw_identifier") != "rmw_cyclonedds_cpp"
                        or observation.get("matched_peer_gids") != row["matched_peer_gids"]):
                    raise ValueError("cyclone_peer_observation_incomplete_or_inconsistent")
                started, ended = (observation.get("started_monotonic_ns"),
                                  observation.get("ended_monotonic_ns"))
                if (not _integer(started, 1) or not _integer(ended, 1)
                        or not chain[0]["monotonic_ns"] <= started <= ended <= row["monotonic_ns"]):
                    raise ValueError("cyclone_peer_observation_chronology_invalid")
            if source in {"dds_matched_endpoint_api", "fastdds_native_stateful_peer_snapshot"} and complete:
                gids = [_gid(gid) for gid in row["matched_peer_gids"]]
                if None in gids or len(gids) != len(set(gids)) or row["matched_count"] != len(gids):
                    raise ValueError("per_peer_list_count_contradiction")
                if native_declared:
                    reason = validate_observation(row.get("native_peer_observation"), row,
                                                  token, self.events, chain[0])
                    if reason is not None:
                        raise ValueError(reason)
                    native_sources.extend(selections)
                # Same strict local raw universe as the producer: an extra
                # unbound peer or missing/aliased generation is not a complete
                # observation of this captured topology. No expected QoS graph.
                active = {}
                for event in self.prefix(boundary):
                    eid = event.get("logical_id")
                    if event["event"] == "endpoint_created" and event["event_index"] < chain[0]["event_index"]:
                        active[eid] = event
                    elif event["event"] in ("endpoint_destroyed", "topology_endpoint_destroyed", "worker_exit"):
                        if eid in active and event.get("generation") == active[eid].get("generation"):
                            active.pop(eid)
                active = {eid: endpoint_identity_at(self.events, origin,
                              before_index=chain[0]["event_index"])
                          for eid, origin in active.items()}
                for key in ("endpoint_gid", "endpoint_generation"):
                    values = [event.get(key) for event in active.values()]
                    if None in values or len(set(values)) != len(values):
                        raise ValueError("direct_peer_scope_identity_alias_or_missing:" + key)
                pids = [event.get("pid") for event in active.values()]
                if None in pids:
                    raise ValueError("direct_peer_scope_identity_alias_or_missing:pid")
                shared_process = len(set(pids)) != len(pids)
                if shared_process:
                    from r2f.runtime.participant_groups import (
                        LINK_KEYS, _validate_shared_process, validate_control,
                    )
                    if (native_declared or source != "dds_matched_endpoint_api"
                            or validate_control(self.program) is None):
                        raise ValueError("direct_peer_scope_identity_alias_or_missing:pid")
                    # Authenticate only the raw prefix through this snapshot ACK.
                    # A later group binding or clean shutdown cannot repair it.
                    for pid in set(pids):
                        members = [e for e in active.values() if e['pid'] == pid]
                        if len(members) > 1:
                            _validate_shared_process(members, self.events,
                                request_index=chain[0]["event_index"],
                                before_index=chain[-1]["event_index"] + 1)
                for name, role in (("publishers", "writer"), ("subscriptions", "reader")):
                    raw = [event for eid, event in active.items()
                           if self.roles.get(eid) == role and event.get("topic") == token["topic"]]
                    graph_rows = chain[2][name]
                    graph_gids = [_gid(event.get("endpoint_gid")) for event in graph_rows]
                    raw_gids = {_gid(event.get("endpoint_gid")) for event in raw}
                    if (None in graph_gids or len(set(graph_gids)) != len(graph_gids)
                            or set(graph_gids) != raw_gids):
                        raise ValueError("direct_peer_graph_and_raw_scope_differ")
                    for graph_row in graph_rows:
                        known = [event for event in raw if _gid(event["endpoint_gid"]) == _gid(graph_row["endpoint_gid"])]
                        if (len(known) != 1 or not _text(known[0].get("message_type"))
                                or graph_row.get("topic_type") != known[0]["message_type"]):
                            raise ValueError("direct_peer_graph_and_raw_type_differ")
                opposite = "reader" if self.roles.get(token["logical_id"]) == "writer" else "writer"
                known = {_gid(event["endpoint_gid"]) for eid, event in active.items()
                         if self.roles.get(eid) == opposite and event.get("topic") == token["topic"]}
                identity_keys = ("generation", "endpoint_gid", "endpoint_generation", "pid",
                                 "process_start_ticks", "topic", "message_type")
                if shared_process:
                    identity_keys += ("pgid",) + LINK_KEYS
                for queried, role in ((token, self.roles.get(token["logical_id"])), (peer, opposite)):
                    bound = active.get(queried["logical_id"])
                    if (bound is None or re.fullmatch(role + r"[1-9]?", str(bound.get("role"))) is None
                            or any(queried.get(key) is None or bound.get(key) != queried[key]
                                   for key in identity_keys)):
                        raise ValueError("direct_peer_query_identity_not_current_raw_generation")
                if (not set(gids) <= known or (peer["topic"] == token["topic"]
                        and _gid(peer["endpoint_gid"]) not in known)):
                    raise ValueError("direct_peer_gid_not_bound_to_current_raw_scope")
                direct.append(_gid(peer["endpoint_gid"]) in gids)
            else:
                direct.append(None)
        proof = None
        if None in direct:
            proof = self.isolation(w, r, wc, rc, boundary)
            counts = [c[1]["matched_count"] for c in (wc, rc)]
            if any(count not in (0, 1) for count in counts):
                raise ValueError("isolated_pair_count_out_of_range")
            measured = [count == 1 for count in counts]
            if any(a is not None and a != b for a, b in zip(direct, measured)):
                raise ValueError("isolation_and_direct_peer_proof_contradict")
            direct = measured
        value = ("bidirectional" if all(direct) else "writer_only" if direct[0]
                 else "reader_only" if direct[1] else "none")
        return value, [w, r, *wc, *rc, *native_sources] + ([proof] if proof else []), proof

    def compatibility(self, w, r, wc, rc):
        from r2f.runtime.matching_qos import pair_contract
        native = pair_contract(w, r, wc[2], rc[2], {e["logical_id"]: e for e in self.program["endpoints"]})

        def conclude(check):
            if native is not None:
                values = (check["compatible"], native["compatible"])
                check.update({key: value for key, value in native.items()
                              if key not in ("compatible", "responsibility_layer")})
                check.setdefault("responsibility_layer", native["responsibility_layer"])
                check["compatible"] = False if False in values else None if None in values else True
            check.setdefault("responsibility_layer", "qos_requested_offered")
            check.setdefault("s1_coverage_rows", ["C10", "C13", "C15"])
            return ("compatible" if check["compatible"] is True else "incompatible"
                    if check["compatible"] is False else "unknown"), check

        if w["topic"] != r["topic"]:
            return conclude({"compatible": False, "reason": "observed_topic_mismatch",
                             "responsibility_layer": "topic_matching", "actual_match": "not_evaluated"})
        contracts = {}
        for token, list_key in ((w, "publishers"), (r, "subscriptions")):
            rows = []
            for chain in (wc, rc):
                members = [row for row in chain[2][list_key]
                           if _gid(row["endpoint_gid"]) == _gid(token["endpoint_gid"])]
                if len(members) != 1:
                    raise ValueError("observed_endpoint_contract_missing_from_graph")
                rows.append(members[0])
            if rows[0] != rows[1]:
                raise ValueError("observed_contract_sources_contradict")
            row = rows[0]
            if not _text(row.get("topic_type")):
                raise ValueError("observed_contract_fields_missing")
            if row["topic_type"] != token.get("message_type"):
                raise ValueError("observed_endpoint_type_binding_mismatch")
            contracts[token["logical_id"]] = row
        writer, reader = contracts[w["logical_id"]], contracts[r["logical_id"]]
        if writer["topic_type"] != reader["topic_type"]:
            # A known type difference is independent of missing QoS. Retain
            # the observed types; this contract does not infer actual match.
            return conclude({
                "compatible": False, "reason": "observed_type_mismatch", "actual_match": "not_evaluated",
                "responsibility_layer": "type_matching", "s1_coverage_rows": ["C10", "C13", "C15"],
                "writer_type": writer["topic_type"], "reader_type": reader["topic_type"],
                "type_scope": "observed_ros_type_names_not_xtypes_assignability",
                "qos": "not_evaluated_type_mismatch",
            })
        fields_by_endpoint = {}
        for eid, row in contracts.items():
            fields = {}
            raw_qos = row.get("qos")
            if raw_qos is not None and not isinstance(raw_qos, dict):
                raise ValueError("observed_contract_qos_not_mapping")
            for policy, key in (("reliability", "reliability"), ("durability", "durability"),
                                ("liveliness.kind", "liveliness"), ("deadline", "deadline_ns"),
                                ("liveliness.lease_duration", "liveliness_lease_duration_ns")):
                value = (raw_qos or {}).get(key)
                if key.endswith("_ns"):
                    value = ({"infinite": True} if value == 9223372036854775807 else
                             {"nanoseconds": value} if _integer(value) else None)
                if value is not None:
                    fields[policy] = {"status": "observed", "value": value}
            fields_by_endpoint[eid] = fields
        check = check_offered_requested(fields_by_endpoint[w["logical_id"]], fields_by_endpoint[r["logical_id"]])
        return conclude(check)

    def pair_bound(self, event, w, r):
        if any(event.get(role) != t["logical_id"] or not _integer(event.get(role + "_generation"), 1)
               or event.get(role + "_generation") != t["generation"]
               for role, t in (("writer", w), ("reader", r))):
            raise ValueError("delivery_window_old_generation_or_pair_mismatch")
        for role, token in (("writer", w), ("reader", r)):
            field = role + "_gid"
            current = endpoint_identity_at(self.events, token, before_index=event["event_index"] + 1)
            if (event["event_index"] <= current["identity_valid_after_ref"]
                    or event["monotonic_ns"] < current["identity_valid_after_ns"]):
                raise ValueError("delivery_window_before_identity_ack")
            if field in event:
                if _gid(event[field]) != _gid(current["endpoint_gid"]):
                    raise ValueError("delivery_window_incarnation_mismatch:" + field)

    def _group_termination(self, token, boundary):
        """Consume the same closed group lineage as runtime and saved captures."""
        from r2f.runtime.topology_pair import group_termination_rows
        try:
            return group_termination_rows(self.program, self.events, self.path, (token,), boundary)
        except (ValueError, TypeError, KeyError, IndexError):
            return None

    def _termination_rows(self, token, boundary):
        from r2f.runtime.participant_groups import enabled
        if enabled(self.program):
            proof = self._group_termination(token, boundary)
            if proof is None:
                raise ValueError("group_process_exit_or_endpoint_cleanup_unbound")
            return list(proof)
        exits = [e for e in self.events if e.get("event") == "topology_worker_exit"
                 and e.get("logical_id") == token.get("logical_id")
                 and e.get("generation") == token.get("generation")]
        fields = ("generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")
        if (len(exits) != 1 or exits[0].get("returncode") != 0
                or any(exits[0].get(k) != token.get(k) for k in fields)
                or exits[0]["event_index"] <= boundary["event_index"]):
            raise ValueError("no_sends_generation_end_not_identity_bound")
        return [exits[0]]

    def no_sends(self, w, r, boundary):
        """Independently reconstruct the committed inventory, never trust a label.

        No import of runtime validator, no open/close fabrication, no replacement
        of per-peer match or endpoint health. Reuse is deliberately fail-closed.
        """
        w = endpoint_identity_at(self.events, w, before_index=boundary["event_index"] + 1)
        r = endpoint_identity_at(self.events, r, before_index=boundary["event_index"] + 1)
        prefix = self.prefix(boundary)
        claims = [e for e in prefix if e["event"] == "topology_delivery_no_sends"
                  and e.get("writer") == w["logical_id"] and e.get("reader") == r["logical_id"]]
        if len(claims) != 1 or boundary.get("phase") != "after_program":
            raise ValueError("no_sends_receipt_missing_duplicate_or_wrong_boundary")
        claim = claims[0]
        self.pair_bound(claim, w, r)
        seals = [e for e in prefix if e["event"] == "topology_send_inventory_end"]
        if len(seals) != 1:
            raise ValueError("no_sends_inventory_end_missing_or_duplicate")
        seal = seals[0]
        end = seal["event_index"]
        inventory = self.events[:end]
        digest = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if (seal.get("schema") != "r2f.topology-send-inventory/v1" or seal.get("inventory_count") != end
                or seal.get("inventory_sha256") != digest
                or boundary.get("lifecycle_complete") is not True or boundary.get("observation_complete") is not True
                or claim["event_index"] not in boundary.get("delivery_window_refs", [])
                or any(e.get("event_index") != i or e.get("run_id") != w["run_id"]
                       or type(e.get("monotonic_ns")) is not int for i, e in enumerate(self.events))):
            raise ValueError("no_sends_inventory_or_lifecycle_incomplete")
        actions = self.program["actions"]
        begins = [e for e in inventory if e["event"] == "topology_action_begin"]
        done = [e for e in inventory if e["event"] == "topology_action_done"]
        if len(begins) != len(actions) or len(done) != len(actions):
            raise ValueError("no_sends_action_chronology_incomplete")
        for i, (action, begin, finish) in enumerate(zip(actions, begins, done)):
            if (begin.get("action") != action or finish.get("action") != action
                    or begin.get("action_index") != i or finish.get("action_index") != i
                    or not begin["event_index"] < finish["event_index"] < end
                    or (i and done[i-1]["event_index"] >= begin["event_index"])
                    or finish.get("receipt", {}).get("satisfied") is not True):
                raise ValueError("no_sends_action_terminal_not_raw_bound")
            self.chain_order([begin, finish, seal])
        if seal.get("action_done_refs") != [e["event_index"] for e in done]:
            raise ValueError("no_sends_action_inventory_mismatch")
        fields = ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")
        chains = {}
        terminations = []
        for token in (w, r):
            history = [e for e in prefix if e.get("logical_id") == token["logical_id"]
                       and e["event"] in {"endpoint_created", "endpoint_destroyed", "topology_endpoint_destroyed", "topology_generation_retired"}]
            if (len(history) != 1 or token["generation"] != 1
                    or endpoint_identity_at(self.events, history[0], before_index=boundary["event_index"] + 1) != token):
                raise ValueError("no_sends_generation_history_not_unique")
            chain = chains[token["logical_id"]] = self.snapshot_chain(token, boundary)
            if any(any(row.get(k) != token[k] for k in fields if k != "endpoint_gid") for row in chain):
                raise ValueError("no_sends_snapshot_identity_incomplete")
            self.chain_order([token, seal, *chain, claim, boundary])
            terminations.extend(self._termination_rows(token, boundary))
        ends = [e for e in self.events if e["event"] == "runtime_end"]
        if (len(ends) != 1 or ends[0].get("cleanup_complete") is not True
                or ends[0].get("status") != "executed" or ends[0]["event_index"] <= boundary["event_index"]):
            raise ValueError("no_sends_complete_runtime_end_missing")
        if ends[0] != self.events[-1]:
            raise ValueError("no_sends_runtime_end_not_final")
        for termination in terminations:
            self.chain_order([boundary, termination, ends[0]])
        for e in self.events:
            if e["event"] in {"runtime_abort", "worker_error", "worker_command_error", "topology_backend_blocked"}:
                raise ValueError("no_sends_raw_worker_or_runtime_error")
            if e["event"] == "send":
                known = [t for t in self.events if t["event"] == "endpoint_created"
                         and t.get("logical_id") == e.get("logical_id") and t.get("generation") == e.get("generation")]
                if (len(known) != 1 or any(e.get(k) != known[0].get(k) for k in fields if k != "endpoint_gid")
                        or e.get("logical_id") == w["logical_id"] or e.get("pid") == w["pid"]):
                    raise ValueError("no_sends_writer_or_ambiguous_send_present")
            if ((e.get("writer") == w["logical_id"] and e["event"].startswith("topology_delivery_window"))
                    or ((e.get("action") if isinstance(e.get("action"), dict) else {}).get("endpoint_id") == w["logical_id"]
                        and (e.get("action") if isinstance(e.get("action"), dict) else {}).get("op") == "publish_batch")
                    or (e.get("logical_id") == w["logical_id"]
                        and (e["event"] in {"publish", "publish_batch", "publish_batch_done"}
                             or e.get("command") in {"publish", "publish_batch"}))):
                raise ValueError("no_sends_publication_attempt_or_window_present")
        expected = {"schema": "r2f.topology-delivery-no-sends/v1", "closure_status": "not_applicable_no_sends",
                    "scope": "entire_writer_generation_through_send_phase_end", "inventory_end_ref": end,
                    "inventory_count": end, "inventory_sha256": digest,
                    "writer_identity": {k: w[k] for k in fields}, "reader_identity": {k: r[k] for k in fields},
                    "writer_creation_ref": w["event_index"], "reader_creation_ref": r["event_index"],
                    "snapshot_refs": {t["logical_id"]: [e["event_index"] for e in chains[t["logical_id"]]] for t in (w, r)},
                    "expected_sequences": [], "received_sequences": [], "missing_sequences": [],
                    "candidate_eligible": False, "mutation_hit": False, "semantic_effect": False}
        if any(claim.get(k) != v for k, v in expected.items()):
            raise ValueError("no_sends_receipt_contradicts_raw_inventory")
        return "no_obligation", [w, r, seal, claim, *chains[w["logical_id"]], *chains[r["logical_id"]]], {
            "expected": [], "received": [], "missing": [], "applicability_kind": "not_applicable_no_sends",
            "no_sends_ref": self.ref(claim), "inventory_end_ref": self.ref(seal),
            "inventory_sha256": digest, "generation_scope": expected["scope"],
            "termination_refs": [self.ref(e) for _, e in sorted({e["event_index"]: e for e in terminations}.items())],
            "termination_runtime_end_ref": self.ref(ends[0])}

    def _native_send_evidence(self, send, boundary):
        """Declared scopes must verify even when no reader received this send."""
        events = self.prefix(boundary)
        declared = any(e.get("logical_id") == send.get("logical_id")
                       and e.get("generation") == send.get("generation")
                       and declares_native_publish(e)
                       for e in events)
        if not declared:
            return []
        witness, reason = producer_occurrence_witness(send, events)
        if witness is None:
            raise ValueError("native_publish_occurrence_invalid:" + str(reason))
        indices = [witness["send_ref"], *witness["scope_refs"].values()]
        indices.extend(witness["writer_identity"][key] for key in
                       ("creation_ref", "transition_ref", "ack_ref", "valid_after_ref")
                       if witness["writer_identity"][key] is not None)
        return [self.resolve(index, boundary=boundary) for index in indices]

    def _native_evidence_rows(self, identity, boundary):
        """Keep all helper proof refs bound to this raw capture and boundary."""
        if isinstance(identity, dict):
            if set(identity) == {"path", "sha256", "event_index"}:
                return [self.resolve(identity, boundary=boundary)]
            return [row for value in identity.values()
                    for row in self._native_evidence_rows(value, boundary)]
        return []

    def recreated_no_sends(self, w, r, boundary):
        """Independently consume v2; do not reuse the producer's validator."""
        w = endpoint_identity_at(self.events, w, before_index=boundary["event_index"] + 1)
        r = endpoint_identity_at(self.events, r, before_index=boundary["event_index"] + 1)
        prefix = self.prefix(boundary)
        claims = [e for e in prefix if e["event"] == "topology_delivery_no_sends"
                  and e.get("writer") == w["logical_id"] and e.get("reader") == r["logical_id"]]
        if (w["generation"] < 2 or len(claims) != 1 or boundary.get("phase") != "after_program"
                or any(boundary.get(k) is not True for k in ("lifecycle_complete", "observation_complete", "window_complete"))):
            raise ValueError("recreated_no_sends_current_boundary_incomplete")
        claim = claims[0]
        self.pair_bound(claim, w, r)
        seals = [e for e in prefix if e["event"] == "topology_send_inventory_end"]
        if len(seals) != 1:
            raise ValueError("recreated_no_sends_inventory_end_missing")
        seal = seals[0]; end = seal["event_index"]; inventory = self.events[:end]
        digest = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if (seal.get("schema") != "r2f.topology-send-inventory/v1" or seal.get("inventory_count") != end
                or seal.get("inventory_sha256") != digest or claim["event_index"] not in boundary.get("delivery_window_refs", [])
                or any(e.get("event_index") != i or e.get("run_id") != w["run_id"]
                       or not _integer(e.get("monotonic_ns")) for i,e in enumerate(self.events))):
            raise ValueError("recreated_no_sends_inventory_unbound")
        begins = [e for e in inventory if e["event"] == "topology_action_begin"]
        finishes = [e for e in inventory if e["event"] == "topology_action_done"]
        if len(begins) != len(self.program["actions"]) or len(finishes) != len(begins):
            raise ValueError("recreated_no_sends_action_count")
        spans = []
        for i,(action,begin,finish) in enumerate(zip(self.program["actions"],begins,finishes)):
            if (begin.get("action") != action or finish.get("action") != action
                    or begin.get("action_index") != i or finish.get("action_index") != i
                    or finish.get("receipt",{}).get("satisfied") is not True
                    or (i and finishes[i-1]["event_index"] >= begin["event_index"])):
                raise ValueError("recreated_no_sends_action_inventory")
            self.chain_order([begin, finish, seal]); spans.append((action,begin,finish))
        if seal.get("action_done_refs") != [e["event_index"] for e in finishes]:
            raise ValueError("recreated_no_sends_action_refs")
        fields = ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")
        if any(e["event"] in {"endpoint_created", "endpoint_destroyed", "topology_endpoint_destroyed"}
               and e.get("logical_id") in {w["logical_id"], r["logical_id"]}
               for e in self.events[end+1:]):
            raise ValueError("recreated_no_sends_lifecycle_after_phase_end")
        histories, chains, current_exits = {}, {}, []
        for token in (w,r):
            history = [e for e in inventory if e.get("logical_id") == token["logical_id"]
                       and e["event"] in {"endpoint_created","endpoint_destroyed","topology_endpoint_destroyed"}]
            self.chain_order(history)
            active = None; gen = 0; gids = set(); workers = set(); nonces = set()
            for event in history:
                if event["event"] == "endpoint_created":
                    if (active is not None or not _integer(event.get("generation"),1) or event["generation"] != gen+1
                            or not _text(event.get("role"))
                            or re.fullmatch(self.roles[token["logical_id"]] + r"[1-9]?", event["role"]) is None
                            or not _gid(event.get("endpoint_gid")) or not _integer(event.get("pid"),1)
                            or not _integer(event.get("process_start_ticks"),1) or not _text(event.get("endpoint_generation"))
                            or event["endpoint_gid"] in gids or (event["pid"],event["process_start_ticks"]) in workers
                            or event["endpoint_generation"] in nonces):
                        raise ValueError("recreated_no_sends_lifecycle_create")
                    operation = "create" if gen == 0 else "recreate"
                    actions = [a for a,b,f in spans if a.get("endpoint_id") == token["logical_id"]
                               and a.get("op") == operation and a.get("generation") == gen+1
                               and b["event_index"] < event["event_index"] < f["event_index"]
                               and b["monotonic_ns"] <= event["monotonic_ns"] <= f["monotonic_ns"]]
                    active = event; gen += 1; gids.add(event["endpoint_gid"])
                    workers.add((event["pid"],event["process_start_ticks"])); nonces.add(event["endpoint_generation"])
                else:
                    if active is not None:
                        active = endpoint_identity_at(self.events, active, before_index=event["event_index"] + 1)
                    if active is None or event.get("cleanup_status") != "closed" or any(event.get(k) != active[k] for k in fields):
                        raise ValueError("recreated_no_sends_retirement_unbound")
                    exits = [e for e in inventory if e["event"] == "topology_worker_exit"
                             and e.get("logical_id") == token["logical_id"] and e.get("generation") == gen]
                    if (len(exits) != 1 or exits[0].get("returncode") != 0
                            or any(exits[0].get(k) != active[k] for k in fields)):
                        raise ValueError("recreated_no_sends_retirement_exit_unbound")
                    actions = [a for a,b,f in spans if a.get("endpoint_id") == token["logical_id"]
                               and (a.get("op"),a.get("generation")) in {("destroy",gen),("recreate",gen+1)}
                               and b["event_index"] < event["event_index"] < f["event_index"]
                               and b["monotonic_ns"] <= event["monotonic_ns"] <= f["monotonic_ns"]
                               and b["event_index"] < exits[0]["event_index"] < f["event_index"]
                               and b["monotonic_ns"] <= exits[0]["monotonic_ns"] <= f["monotonic_ns"]]
                    active = None
                if len(actions) != 1:
                    raise ValueError("recreated_no_sends_lifecycle_action_unbound")
            if (active is None or endpoint_identity_at(self.events, active,
                    before_index=boundary["event_index"] + 1) != token):
                raise ValueError("recreated_no_sends_not_current_generation")
            histories[token["logical_id"]] = [e["event_index"] for e in history]
            chain = chains[token["logical_id"]] = self.snapshot_chain(token,boundary)
            if any(any(e.get(k) != token[k] for k in fields if k != "endpoint_gid") for e in chain):
                raise ValueError("recreated_no_sends_snapshot_identity")
            self.chain_order([token,seal,*chain,claim,boundary])
            exits = self._termination_rows(token, boundary)
            current_exits.extend(exits)
        ends = [e for e in self.events if e["event"] == "runtime_end"]
        if len(ends) != 1 or ends[0].get("cleanup_complete") is not True or ends[0].get("status") != "executed":
            raise ValueError("recreated_no_sends_runtime_end")
        if ends[0] != self.events[-1]:
            raise ValueError("recreated_no_sends_runtime_end_not_final")
        for exit_event in current_exits:
            self.chain_order([boundary,exit_event,ends[0]])
        creates = [e for e in self.events if e["event"] == "endpoint_created"]
        sends = [e for e in self.events if e["event"] == "send"]
        native_rows, native_identities = [], []
        for send in sends:
            tokens = [e for e in creates if e.get("logical_id") == send.get("logical_id") and e.get("generation") == send.get("generation")]
            if (len(tokens) != 1 or any(send.get(k) != tokens[0].get(k) for k in fields if k != "endpoint_gid")
                    or send.get("writer_gid") != tokens[0]["endpoint_gid"] or send.get("publish_returned") is not True
                    or send["event_index"] >= end or not _text(send.get("payload_sha256"))):
                raise ValueError("recreated_no_sends_ambiguous_send")
            native = self._native_send_evidence(send, seal)
            if not native and len([s for s in sends if s.get("payload_sha256") == send["payload_sha256"]]) != 1:
                raise ValueError("recreated_no_sends_ambiguous_send")
            native_rows.extend(native)
            if send.get("logical_id") == w["logical_id"] and (send["generation"] >= w["generation"] or send["event_index"] >= w["event_index"]):
                raise ValueError("recreated_no_sends_current_generation_sent")
        for receive in (e for e in self.events if e["event"] == "receive"):
            native = native_delivery_identity(receive, sends, inventory, ref=self.ref)
            if native is not None:
                native_rows.extend(self._native_evidence_rows(native, seal))
                native_identities.append(native)
                continue
            matching = [s for s in sends if s.get("payload_sha256") == receive.get("payload_sha256")
                        and s.get("logical_id") == receive.get("payload_writer_id")
                        and s.get("generation") == receive.get("payload_writer_generation")
                        and s.get("send_sequence") == receive.get("send_sequence")
                        and s.get("run_id") == receive.get("payload_run_id")]
            if len(matching) != 1:
                raise ValueError("recreated_no_sends_ambiguous_receive")
        for e in self.events:
            if e["event"] in {"runtime_abort","worker_error","worker_command_error","topology_backend_blocked"}:
                raise ValueError("recreated_no_sends_runtime_error")
            a = e.get("action") if isinstance(e.get("action"),dict) else {}
            if (a.get("endpoint_id") == w["logical_id"] and a.get("op") == "publish_batch"
                    and (a.get("generation") == w["generation"] or e["event_index"] > w["event_index"])):
                raise ValueError("recreated_no_sends_current_publish_action")
            if e.get("logical_id") == w["logical_id"] and (e.get("command") in {"publish","publish_batch"} or e["event"] in {"publish","publish_batch","publish_batch_done"}):
                if not _integer(e.get("generation"),1) or e["generation"] >= w["generation"] or e["event_index"] > w["event_index"]:
                    raise ValueError("recreated_no_sends_current_publish_command")
            if e.get("writer") == w["logical_id"] and e["event"].startswith("topology_delivery_window"):
                if not _integer(e.get("writer_generation"),1) or e["writer_generation"] >= w["generation"]:
                    raise ValueError("recreated_no_sends_current_window_present")
        prior, evidence = [], []
        for opened in (e for e in prefix if e["event"] == "topology_delivery_window_open"):
            closes = [e for e in prefix if e["event"] == "topology_delivery_window_closed" and e.get("open_ref") == opened["event_index"]]
            if len(closes) != 1 or closes[0].get("window_complete") is not True:
                raise ValueError("recreated_no_sends_old_window_incomplete")
            close = closes[0]
            ts = []
            for role in ("writer","reader"):
                cs = [e for e in creates if e.get("logical_id") == close.get(role) and e.get("generation") == close.get(role+"_generation")]
                if len(cs) != 1: raise ValueError("recreated_no_sends_old_window_identity")
                ts.append(cs[0])
            value, rows, detail = self.delivery(ts[0],ts[1],{},close)
            if value not in {"closed","no_obligation"} or detail.get("missing"):
                raise ValueError("recreated_no_sends_old_obligation_unresolved")
            prior.append(close); evidence.append((opened,close,value,rows))
        for send in (e for e in sends if e.get("logical_id") == w["logical_id"]):
            active = {}
            for e in self.events[:send["event_index"]]:
                if e["event"] == "endpoint_created" and self.roles.get(e.get("logical_id")) == "reader":active[e["logical_id"]]=e["generation"]
                elif e["event"] in {"endpoint_destroyed","topology_endpoint_destroyed"}:active.pop(e.get("logical_id"),None)
            for eid,gen in active.items():
                if not any(o["writer"] == w["logical_id"] and o["writer_generation"] == send["generation"]
                           and o["reader"] == eid and o["reader_generation"] == gen
                           and o["event_index"] < send["event_index"] < c["event_index"]
                           and (send in rows or value == "no_obligation") for o,c,value,rows in evidence):
                    raise ValueError("recreated_no_sends_historical_send_uncovered")
        expected = {"schema":"r2f.topology-delivery-no-sends/v2","closure_status":"not_applicable_no_sends",
                    "scope":"current_writer_generation_after_verified_retirement","inventory_end_ref":end,
                    "inventory_count":end,"inventory_sha256":digest,"writer_identity":{k:w[k] for k in fields},
                    "reader_identity":{k:r[k] for k in fields},"writer_creation_ref":w["event_index"],"reader_creation_ref":r["event_index"],
                    "generation_history_refs":histories,"prior_window_close_refs":[e["event_index"] for e in prior],
                    "snapshot_refs":{t["logical_id"]:[e["event_index"] for e in chains[t["logical_id"]]] for t in (w,r)},
                    "expected_sequences":[],"received_sequences":[],"missing_sequences":[],
                    "candidate_eligible":False,"mutation_hit":False,"semantic_effect":False}
        if any(claim.get(k) != v for k,v in expected.items()):
            raise ValueError("recreated_no_sends_proof_contradicts_raw")
        refs = [w,r,seal,claim,*chains[w["logical_id"]],*chains[r["logical_id"]],*prior,*native_rows]
        return "no_obligation",refs,{"expected":[],"received":[],"missing":[],"applicability_kind":"not_applicable_no_sends",
            "no_sends_ref":self.ref(claim),"inventory_end_ref":self.ref(seal),"inventory_sha256":digest,
            "generation_scope":expected["scope"],"no_sends_schema":expected["schema"],
            "generation_history_refs":histories,"prior_window_close_refs":[self.ref(e) for e in prior],
            "native_delivery_identities":native_identities}

    def delivery(self, w, r, tokens, boundary):
        w = endpoint_identity_at(self.events, w, before_index=boundary["event_index"] + 1)
        r = endpoint_identity_at(self.events, r, before_index=boundary["event_index"] + 1)
        prefix = self.prefix(boundary)
        closes = [e for e in prefix if e["event"] == "topology_delivery_window_closed"
                  and e.get("writer") == w["logical_id"] and e.get("reader") == r["logical_id"]]
        if w["generation"] >= 2 and not any(
                e.get("writer_generation") == w["generation"] and e.get("reader_generation") == r["generation"]
                for e in closes):
            return self.recreated_no_sends(w, r, boundary)
        if not closes:
            return self.no_sends(w, r, boundary)
        close = closes[-1]
        self.pair_bound(close, w, r)
        opened = self.resolve(close.get("open_ref"), boundary=close)
        if opened["event"] != "topology_delivery_window_open":
            raise ValueError("delivery_window_open_ref_wrong_kind")
        self.pair_bound(opened, w, r)
        self.chain_order([opened, close])
        wc, rc = self.snapshot_chain(w, opened), self.snapshot_chain(r, opened)
        try:
            matched, match_rows, isolation = self.match(w, r, wc, rc, opened)
        except ValueError:
            # Identity-bound communication may be observed independently of the
            # SDK's unavailable per-peer matching API. Never upgrade match from it.
            matched, match_rows, isolation = "unknown", [*wc, *rc], None
        if close.get("window_complete") is not True:
            raise ValueError("delivery_window_capture_incomplete")
        rows = [e for e in prefix if opened["event_index"] < e["event_index"] < close["event_index"]]
        claims = [e for e in rows if e["event"] == "topology_delivery_no_obligation"
                  and e.get("open_ref") == opened["event_index"]]
        if claims or close.get("closure_status") == "no_obligation":
            if len(claims) != 1:
                raise ValueError("no_obligation_receipt_missing_or_duplicate")
            claim = claims[0]
            self.pair_bound(claim, w, r)
            self.chain_order([opened, claim, close])
            reason = None
            types = []
            for token, chain, name in ((w, wc, "publishers"), (r, rc, "subscriptions")):
                if any(token.get(key) is None or row.get(key) != token[key]
                       for row in chain for key in ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")):
                    raise ValueError("no_obligation_snapshot_worker_identity_incomplete")
                own = [row for row in chain[2][name] if _gid(row.get("endpoint_gid")) == _gid(token["endpoint_gid"])]
                if (len(own) != 1 or not _text(token.get("message_type"))
                        or own[0].get("topic_type") != token["message_type"]):
                    raise ValueError("no_obligation_actual_type_not_raw_bound")
                types.append(token["message_type"])
            if w["topic"] != r["topic"]:
                reason = "actual_endpoint_topic_mismatch"
            elif types[0] != types[1]:
                reason = "actual_endpoint_type_mismatch"
            elif matched == "none" and isolation is None:
                reason = "both_complete_direct_peer_lists_exclude_pair"
            # UNKNOWN or directional match is never NONE. QoS incompatibility
            # predicted by the model is deliberately not an exemption source.
            if reason is None:
                raise ValueError("no_obligation_not_proved_by_actual_endpoints_or_direct_none")
            fields = {"reason": reason, "writer_gid": w["endpoint_gid"], "reader_gid": r["endpoint_gid"],
                      "writer_topic": w["topic"], "reader_topic": r["topic"],
                      "writer_type": types[0], "reader_type": types[1],
                      "scope": "open_snapshot_only_not_persistent_nonmatch", "obligation": "not_applicable",
                      "snapshot_refs": {t["logical_id"]: [e["event_index"] for e in chain]
                                        for t, chain in ((w, wc), (r, rc))}}
            if (any(claim.get(key) != value for key, value in fields.items())
                    or opened.get("snapshot_refs") != fields["snapshot_refs"]
                    or any(close.get(key) != [] for key in ("expected_sequences", "received_sequences", "missing_sequences", "gap_evidence", "send_refs", "receive_refs"))
                    or close.get("no_obligation_ref") != claim["event_index"]
                    or close.get("no_obligation_reason") != reason
                    or close.get("closure_status") != "no_obligation"
                    or any(e["event"] == "send" and e.get("logical_id") == w["logical_id"]
                           and e["event_index"] < claim["event_index"] for e in rows)
                    or any(e["event"] == "topology_delivery_window_interrupted"
                           and e.get("open_ref") == opened["event_index"] for e in rows)):
                raise ValueError("no_obligation_receipt_or_close_contradicts_raw")
            return "no_obligation", [w, r, opened, claim, close, *wc, *rc], {
                "expected": [], "received": [], "missing": [], "attribution": "not_applicable",
                "no_obligation_reason": reason, "no_obligation_ref": self.ref(claim),
                "observed_match_used": matched, "cross_writer_order_required": False,
                "limitation": "snapshot_scope_only_not_persistent_nonmatch"}
        sends = [e for e in rows if e["event"] == "send" and e.get("logical_id") == w["logical_id"]]
        expected, native_rows = {}, []
        for event in sends:
            self.bound(event, w, gid_field="writer_gid")
            native_rows.extend(self._native_send_evidence(event, close))
            seq = event.get("send_sequence")
            if not _integer(seq) or seq in expected:
                raise ValueError("send_sequence_invalid_or_duplicate_in_writer_generation")
            prior = [e for e in self.prefix(close) if e["event"] == "send"
                     and e.get("logical_id") == w["logical_id"] and e.get("generation") == w["generation"]
                     and e.get("send_sequence") == seq]
            if len(prior) != 1:
                raise ValueError("ambiguous_reused_sequence_in_writer_generation")
            expected[seq] = event
        if sends:
            self.chain_order(sends)
        received, ambiguous = {}, []
        def possible_writers_at(sample):
            active = {}
            for event in self.prefix(sample):
                eid = event.get("logical_id")
                if not _text(eid) or self.roles.get(eid) != "writer":
                    continue
                if event["event"] == "endpoint_created":
                    active[eid] = event
                elif event["event"] in ("endpoint_destroyed", "topology_endpoint_destroyed"):
                    active.pop(eid, None)
            return [t for t in active.values() if t.get("topic") == w["topic"]]
        samples, attributions, native_identities = [], [], []
        capture_sends = [e for e in self.prefix(close) if e["event"] == "send"]
        def payload_send(sample):
            digest = sample.get("payload_sha256")
            run_id = sample.get("payload_run_id")
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)
                    or not _text(run_id) or sample.get("run_id") != run_id
                    or not _text(sample.get("payload_writer_id"))
                    or not _integer(sample.get("payload_writer_generation"), 1)):
                return None
            candidates = [e for e in self.prefix(close) if e["event"] == "send"
                          and e.get("run_id") == run_id and e.get("payload_sha256") == digest]
            if len(candidates) != 1:
                return None
            send = candidates[0]
            if (send.get("logical_id") != sample["payload_writer_id"]
                    or send.get("generation") != sample["payload_writer_generation"]
                    or send.get("send_sequence") != sample.get("send_sequence")
                    or send.get("publish_returned") is not True
                    or send.get("run_id") != self.events[self.receipt["input_ref"]["event_index"]].get("run_id")):
                return None
            return send
        for event in rows:
            if event["event"] != "receive" or event.get("logical_id") != r["logical_id"]:
                continue
            self.bound(event, r, gid_field="reader_gid")
            # Select the strict native contract BEFORE any GID/body fallback.
            # Its sequence is a witnessed producer occurrence, not String.data's
            # sequence nor a guessed DDS publication-sequence offset.
            native = native_delivery_identity(event, capture_sends, self.prefix(close), ref=self.ref)
            if native is not None:
                if (native["writer"], native["writer_generation"]) != (w["logical_id"], w["generation"]):
                    continue
                linked = self.resolve(native["send_ref"], boundary=close)
                self.bound(linked, w, gid_field="writer_gid")
                seq, basis = native["sequence"], native["basis"]
            else:
                publisher = _gid(event.get("publisher_gid"))
                writer_at_sample = endpoint_identity_at(self.events, w, before_index=event["event_index"] + 1)
                if publisher is not None and publisher != _gid(writer_at_sample["endpoint_gid"]):
                    continue
                basis, linked = "direct_publisher_gid", None
                if publisher is None:
                    linked = payload_send(event)
                    if linked is not None:
                        if linked.get("logical_id") != w["logical_id"]:
                            continue
                        self.bound(linked, w, gid_field="writer_gid")
                        basis = "endpoint_correlated_payload"
                    elif len(possible_writers_at(event)) == 1 and isolation is not None:
                        basis = "controlled_isolation_endpoint_correlated"
                    else:
                        ambiguous.append(event)
                        continue
                seq = event.get("send_sequence")
            if not _integer(seq):
                raise ValueError("received_sequence_missing")
            if seq not in expected:
                raise ValueError("received_sequence_has_no_current_writer_send")
            if linked is not None and linked["event_index"] != expected[seq]["event_index"]:
                raise ValueError("payload_send_outside_current_delivery_window")
            # Worker stdout drain order is not a cross-process causal clock:
            # publish() may deliver before the sending worker logs its return.
            # Both raw events must be inside this actual capture window, but no
            # global send-log-before-receive-log order is invented.
            received.setdefault(seq, []).append(event)
            samples.append(event)
            attributions.append(basis)
            if native is not None:
                native_identities.append(native)
                native_rows.extend(self._native_evidence_rows(native, close))
        refs = [w, r, opened, close, *match_rows, *sends, *samples, *ambiguous, *native_rows]
        for event in [*sends, *samples, *ambiguous]:
            if not opened["monotonic_ns"] <= event["monotonic_ns"] <= close["monotonic_ns"]:
                raise ValueError("delivery_sample_outside_window_clock")
        if ambiguous:
            return "unknown", refs, {"reason": "multiwriter_or_unproven_publisher_attribution", "missing": sorted(set(expected) - set(received))}
        missing = sorted(set(expected) - set(received))
        detail = {"expected": sorted(expected), "received": sorted(received), "missing": missing,
                  "attribution": attributions[0] if attributions and len(set(attributions)) == 1 else "mixed" if attributions else "none",
                  "directed_pair_communication": bool(samples), "observed_match_used": matched,
                  "cross_writer_order_required": False, "cross_worker_log_order": "not_assumed",
                  "payload_uniqueness_scope": "raw_prefix_through_window_close",
                  "open_ref": self.ref(opened), "close_ref": self.ref(close),
                  "native_delivery_identities": native_identities}
        if not expected:
            return "no_obligation", refs, detail
        sentinel = close.get("sentinel_sequence")
        closed = (_integer(sentinel) and sentinel in received and sentinel in expected
                  and sentinel == max(expected)
                  and all(seq < sentinel and expected[seq]["event_index"] < expected[sentinel]["event_index"] for seq in missing))
        if not closed:
            return "open", refs, {**detail, "reason": "finite_window_without_later_sentinel_closure"}
        return "closed_with_gap" if missing else "closed", refs, detail

    def globals(self, side, boundary, tokens, pairs):
        prefix = self.prefix(boundary)
        refs = [e for e in prefix if e["event"] == "topology_network_observation"]
        values = {"network_phase": "unknown", "recovery_cycle_bucket": "unknown"}
        try:
            if not refs:
                raise ValueError("observed_network_recovery_evidence_missing")
            row = refs[-1]
            carrier = self.resolve(row.get("carrier_ref"), boundary=row)
            carriers = [e for e in prefix if e["event"] == "topology_network_phase"]
            if not carriers or carriers[-1]["event_index"] != carrier["event_index"]:
                raise ValueError("stale_network_phase_observation")
            phase, cycle = row.get("network_phase"), row.get("recovery_cycle")
            if (row.get("source") != "carrier_audit" or not _integer(cycle)
                    or carrier["event"] != "topology_network_phase"
                    or carrier.get("observed") is not True or not _text(carrier.get("implementation"))
                    or carrier.get("recovery_cycle") != cycle):
                raise ValueError("network_carrier_evidence_incomplete_or_contradictory")
            expected_phase = "restore_pending" if phase == "restored_verified" else phase
            if carrier.get("network_phase") != expected_phase:
                raise ValueError("network_phase_evidence_contradiction")
            evidence = [carrier, row]
            self.chain_order(evidence)
            if phase == "normal":
                valid = carrier.get("audit_neutral") is True
            elif phase == "drop_active":
                valid = carrier.get("applied") is True and _integer(carrier.get("discovery_packets"), 1)
            elif phase in ("restore_pending", "restored_verified"):
                valid = carrier.get("rules_removed") is True
            else:
                valid = False
            if not valid:
                raise ValueError("network_phase_effect_not_observed")
            if phase == "restored_verified":
                drefs = row.get("discovery_refs")
                if not isinstance(drefs, list) or not drefs or not tokens:
                    raise ValueError("restoration_identity_discovery_missing")
                seen = set()
                for ref in drefs:
                    discovery = self.resolve(ref, boundary=row)
                    eid = discovery.get("logical_id")
                    if (eid not in tokens or discovery["event"] != "topology_discovery_activity"
                            or discovery.get("source") != "endpoint_discovery_capture"
                            or not _integer(discovery.get("packet_count"), 1)):
                        raise ValueError("restoration_discovery_not_identity_bound")
                    self.bound(discovery, tokens[eid], gid_field="endpoint_gid")
                    self.chain_order([carrier, discovery, row])
                    evidence.append(discovery)
                    seen.add(eid)
                if seen != set(tokens):
                    raise ValueError("restoration_discovery_identity_inventory_incomplete")
                for pair in pairs:
                    if pair["compatibility"] == "not_applicable":
                        continue
                    if pair["compatibility"] == "unknown":
                        raise ValueError("restoration_pair_contract_unknown")
                    if pair["compatibility"] == "incompatible":
                        continue
                    path = "/pairs/" + _pointer(pair["writer"]) + "/" + _pointer(pair["reader"])
                    component = self.receipt[side]["component_refs"][path + "/delivery_closure"]
                    match_component = self.receipt[side]["component_refs"][path + "/observed_match"]
                    snapshots = [self.resolve(ref, boundary=boundary) for ref in match_component["refs"]]
                    if any(event["event_index"] <= carrier["event_index"] for event in snapshots
                           if event["event"] in ("worker_command_requested", "topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done")):
                        raise ValueError("restoration_pair_snapshot_not_fresh_after_restore")
                    if (pair["observed_match"] != "bidirectional" or pair["delivery_closure"] != "closed"
                            or component["detail"].get("open_ref", {}).get("event_index", -1) <= carrier["event_index"]):
                        raise ValueError("restoration_fresh_pair_and_postrestore_delivery_missing")
                    evidence.extend(self.resolve(ref, boundary=boundary) for ref in component["refs"])
            values = {"network_phase": phase, "recovery_cycle_bucket": "0" if cycle == 0 else "1" if cycle == 1 else "2plus"}
            for name, value in values.items():
                self.component(side, "/" + name, value, evidence, actual_cycle=cycle)
        except ValueError as exc:
            for name in values:
                self.unknown(side, "/" + name, str(exc), refs)
        return values

    def boundary(self, side, boundary):
        binding = self.receipt[side]
        binding["boundary_ref"] = self.ref(boundary)
        if boundary["event"] not in _BOUNDARIES:
            raise ValueError("unsupported_boundary_event")
        flags = all(boundary.get(k) is True for k in ("lifecycle_complete", "observation_complete", "window_complete"))
        endpoints, tokens = self.endpoints(side, boundary, boundary.get("lifecycle_complete") is True)
        pairs = []
        for w in (eid for eid in self.roles if self.roles[eid] == "writer"):
            for r in (eid for eid in self.roles if self.roles[eid] == "reader"):
                path = "/pairs/" + _pointer(w) + "/" + _pointer(r)
                pair = {"writer": w, "reader": r}
                nonlive = any(endpoints[eid]["liveness"] in ("absent", "retired") for eid in (w, r))
                if nonlive:
                    for name in PAIR_DOMAINS:
                        pair[name] = self.component(side, path + "/" + name, "not_applicable", [boundary],
                                                    status="not_applicable", reason=None,
                                                    basis="raw_backed_endpoint_not_alive")
                elif w not in tokens or r not in tokens:
                    for name in PAIR_DOMAINS:
                        pair[name] = self.unknown(side, path + "/" + name, "endpoint_lifecycle_unknown")
                else:
                    wc = rc = None
                    try:
                        wc, rc = self.snapshot_chain(tokens[w], boundary), self.snapshot_chain(tokens[r], boundary)
                        value, policy = self.compatibility(tokens[w], tokens[r], wc, rc)
                        pair["compatibility"] = self.component(side, path + "/compatibility", value, [tokens[w], tokens[r], *wc, *rc],
                            status="unknown" if value == "unknown" else "observed",
                            reason="observed_matching_policy_fields_missing" if value == "unknown" else None,
                            policy_scope=policy, limitation="Observed topic/type and listed offered/requested policies, not proof of match.")
                    except (ValueError, KeyError, TypeError) as exc:
                        pair["compatibility"] = self.unknown(side, path + "/compatibility", str(exc),
                            [*(wc or ()), *(rc or ())])
                    try:
                        if wc is None or rc is None:
                            raise ValueError("current_snapshot_chain_missing")
                        value, rows, proof = self.match(tokens[w], tokens[r], wc, rc, boundary)
                        sources = sorted({c[1]["peer_proof_source"] for c in (wc, rc)
                                          if isinstance(c[1].get("peer_proof_source"), str)})
                        kind = ("controlled_isolation" if proof else
                                "direct_native_per_peer" if sources == ["fastdds_native_stateful_peer_snapshot"] else
                                "direct_per_peer_api" if sources == ["dds_matched_endpoint_api"] else
                                "direct_mixed_per_peer")
                        pair["observed_match"] = self.component(side, path + "/observed_match", value, rows,
                            proof_kind=kind, peer_proof_sources=sources,
                            isolation_id=proof.get("isolation_id") if proof else None)
                    except ValueError as exc:
                        diagnostics = [e for e in self.prefix(boundary) if e["event"] == "topology_pair_observation"]
                        pair["observed_match"] = self.unknown(side, path + "/observed_match", str(exc),
                            [*(wc or ()), *(rc or ()), *diagnostics], helper_status_accepted=False)
                    try:
                        value, rows, detail = self.delivery(tokens[w], tokens[r], tokens, boundary)
                        pair["delivery_closure"] = self.component(side, path + "/delivery_closure", value, rows,
                            status="unknown" if value == "unknown" else "observed", **detail)
                    except (ValueError, KeyError, TypeError) as exc:
                        pair["delivery_closure"] = self.unknown(side, path + "/delivery_closure", str(exc))
                pairs.append(pair)
        globals_ = self.globals(side, boundary, tokens, pairs)
        errors = [e for e in self.prefix(boundary) if e["event"] in
                  ("worker_error", "worker_command_error", "topology_backend_blocked")]
        complete = (flags and not errors and all(e["liveness"] != "unknown" for e in endpoints.values())
                    and all(p[c] != "unknown" for p in pairs for c in PAIR_DOMAINS)
                    and all(v != "unknown" for v in globals_.values()))
        self.component(side, "/observation_completeness", "complete" if complete else "incomplete", [boundary, *errors],
                       status="observed" if complete else "unknown",
                       reason="raw_worker_or_backend_error" if errors else None if complete else "boundary_flags_or_component_evidence_incomplete")
        state = abstract_topology_state(self.program, {"endpoints": endpoints, "pairs": pairs,
            **globals_, "observation_complete": complete})
        binding["state_id"] = topology_state_id(state)
        return state


def project_topology_state(program, raw_path, *, before_ref, after_ref, expected_sha256=None):
    """Project two explicit boundaries from immutable JSONL bytes; fail closed.

    Invalid program identity raises ValueError. Missing/malformed raw evidence is
    returned as an invalid/incomplete receipt with ineligible unknown states.
    File I/O is read-only. The actual hash is calculated once from the same bytes
    parsed; no receipt accepts a caller-supplied helper or predicted graph.
    """
    logical_endpoints(program)
    empty = abstract_topology_state(program, {})
    path = str(raw_path)
    receipt = {"schema": SCHEMA, "status": "incomplete", "raw_path": path, "raw_sha256": None,
               "program_sha256": canonical_hash(program), "reasons": [], "execution_evidence": False,
               "fixture": True, "chronology": {"status": "unverified"},
               "before": {"boundary_ref": None, "component_refs": {}, "identity": {}, "history": {}},
               "after": {"boundary_ref": None, "component_refs": {}, "identity": {}, "history": {}}}
    result = {"state_before": deepcopy(empty), "state_after": deepcopy(empty), "receipt": receipt, "eligible": False}
    try:
        content = Path(path).read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        receipt["raw_sha256"] = digest
        if expected_sha256 is not None and expected_sha256 != digest:
            raise ValueError("raw_sha256_mismatch")
        lines = content.decode("utf-8").splitlines()
        events = [json.loads(line, object_pairs_hook=_json_object, parse_constant=_reject_constant, parse_float=_finite_float) for line in lines]
        if not events:
            raise ValueError("raw_events_empty")
        for index, event in enumerate(events):
            if (not isinstance(event, dict) or type(event.get("event_index")) is not int
                    or event["event_index"] != index or not _text(event.get("event"))
                    or not _integer(event.get("monotonic_ns"), 1)):
                raise ValueError("raw_event_index_or_clock_malformed")
        projection = _Projection(program, path, digest, events, receipt)
        before = projection.resolve(before_ref, integer=True)
        after = projection.resolve(after_ref, integer=True)
        projection.chain_order([before, after])
        bindings = [e for e in projection.prefix(before) if e["event"] == "input_binding"]
        if (len(bindings) != 1 or not isinstance(bindings[0].get("receipt"), dict)
                or bindings[0]["receipt"].get("program_sha256") != receipt["program_sha256"]):
            raise ValueError("raw_program_input_binding_missing_or_mismatch")
        receipt["input_ref"] = projection.ref(bindings[0])
        prepared = [e for e in projection.prefix(before) if e["event"] == "topology_backend_prepared"]
        real = (bindings[0].get("backend") == "ros2_public_api"
                and len(prepared) == 1 and prepared[0].get("real_ros") is True
                and all(e.get("backend") == "ros2_public_api" and e.get("fixture") is not True
                        and e.get("offline_fake") is not True and e.get("real_ros") is not False for e in events))
        receipt["execution_evidence"], receipt["fixture"] = real, not real
        for side, boundary in (("before", before), ("after", after)):
            result["state_" + side] = projection.boundary(side, boundary)
        receipt["chronology"] = {"status": "verified", "before_ref": projection.ref(before),
                                 "after_ref": projection.ref(after), "component_refs_bounded": True,
                                 "snapshot_ack_kind": "command_completion_not_dds_delivery_ack"}
        complete = all(result["state_" + side]["observation_completeness"] == "complete" for side in ("before", "after"))
        receipt["status"] = "observed" if complete else "incomplete"
        result["eligible"] = complete and real
        if not real:
            receipt["eligibility_reason"] = "fixture_or_unverified_execution_provenance"
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, IndexError, RecursionError) as exc:
        receipt["status"] = "invalid"
        receipt["reasons"].append(str(exc))
        receipt["chronology"]["status"] = "invalid"
        result["eligible"] = False
    return result
