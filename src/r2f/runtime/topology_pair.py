"""F3 topology pair/scenario carrier for the unified runner.

The module executes a validated ``r2f.s1-topology-program/v1`` program through
an explicit backend (public rclpy or an offline contract double).  It owns no
candidate lifecycle, oracle verdict, maturity counter, or seal decision.  Every
fact is derived from backend events; missing observations stay ``unknown``.

Design constraints carried from docs/decisions.md D010/D013/D014:
* logical generation is bound to a fresh real GUID, PID and process start ticks;
* a same-name recreate can never inherit the predecessor's identity;
* matching and delivery are recorded per directed (writer, reader) pair, not as
  an aggregate endpoint count;
* writer/generation publish sequences have no cross-writer total order;
* preflight/guard/tool failures are not SUT defects.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import os
import subprocess
from pathlib import Path
import time

from r2f.modules.s1 import topology_model as model

from .domains import IsolationError
from .topology_environment_observation import TopologyEnvironmentObservation

from .topology_worker import (
    MAX_INTERVAL_SEC,
    NETEM_ISOLATION_ENV,
    OFFLINE_BACKEND,
    ROS_BACKEND,
    OfflineTopologyBackend,
    RosTopologyBackend,
    TopologyBackendBlocked,
    TopologyBackendError,
    TopologyBackendTimeout,
    TopologyUnsupported,
    endpoint_expressibility,
    initial_content_filter_capability,
    netem_isolation_receipt,
    normalize_topology_config,
    normalize_topology_environment_mutation,
    sha256_json,
    worker_role_for,
    worker_role_kind,
)

INPUT_SCHEMA = "r2f.topology-input-binding/v1"
RESULT_SCHEMA = "r2f.topology-run-facts/v1"
PAIR_FACT_SCHEMA = "r2f.topology-pair-fact/v1"
# Exceptional failed cleanup retains the actual open lease objects, never just receipts.
_RETAINED_EXECUTIONS = []


def _now():
    return time.monotonic_ns()


def _clean_id(value):
    return "".join(ch if ch.isalnum() or ch in "_.-" else "_" for ch in str(value))[:96]


def build_input_receipts(program, config, *, mutation_receipts=None):
    """Bind the exact executed program/config to per-endpoint public API inputs.

    This is an input identity receipt, not execution evidence.  It never binds
    declared ``vendor``/``participant_id`` values to a measured GID/PID.
    """
    validation = model.validate_program(program)
    endpoints = []
    for spec in program.get("endpoints", []) if isinstance(program, dict) else []:
        row = {
            "logical_id": spec.get("logical_id"), "role": spec.get("role"),
            "generation_declarations": [],
            "binding_status": "not_evaluated", "binding": None,
            "errors": [],
        }
        try:
            prepared = endpoint_expressibility(spec, config)
            row["binding_status"] = "expressible"
            row["binding"] = prepared["binding"]
        except TopologyUnsupported as exc:
            row["binding_status"] = "not_expressible"
            row["reason"] = exc.code
            row["errors"].append(exc.code)
        endpoints.append(row)
    config_keys = ("domain_id", "rmw", "target_revision", "startup_timeout_sec",
                   "publish_timeout_sec", "snapshot_timeout_sec")
    return {
        "schema": INPUT_SCHEMA,
        "program_sha256": sha256_json(program),
        "validation_status": validation.get("status"),
        "guard_disposition": validation.get("guard_disposition"),
        "validation_errors": deepcopy(validation.get("errors", [])),
        "endpoints": endpoints,
        "environment_declared": deepcopy(program.get("environment", {})),
        "config": {key: config.get(key) for key in config_keys if key in config},
        "config_sha256": sha256_json({key: config.get(key) for key in config_keys if key in config}),
        "mutation_receipts": deepcopy(mutation_receipts or []),
        "fingerprints": model.fingerprints(program) if validation.get("status") == "valid" else None,
        "execution_evidence": False,
        "candidate_eligible": False,
    }


def _latest(rows, **fields):
    found = [row for row in rows
             if all(row.get(key) == value for key, value in fields.items())]
    return found[-1] if found else None


def _snapshot_complete(snapshot):
    if not isinstance(snapshot, dict):
        return False
    match = snapshot.get("match_snapshot")
    graph = snapshot.get("graph_snapshot")
    return (isinstance(match, dict) and isinstance(graph, dict)
            and type(match.get("matched_count")) is int
            and match["matched_count"] >= 0
            and isinstance(match.get("endpoint_gid"), str)
            and isinstance(graph.get("publishers"), list)
            and isinstance(graph.get("subscriptions"), list)
            and snapshot.get("complete") is True)


def _gid_set(rows):
    return {row.get("endpoint_gid") for row in rows if row.get("endpoint_gid")}


def pair_graph_facts(writer_handle, reader_handle, writer_snapshot, reader_snapshot,
                     *, snapshot_command_id=None, real_ros=False):
    """One directed pair fact from the two endpoint-specific fresh snapshots."""
    fact = {
        "schema": PAIR_FACT_SCHEMA,
        "writer": writer_handle["logical_id"],
        "reader": reader_handle["logical_id"],
        "writer_generation": writer_handle["generation"],
        "reader_generation": reader_handle["generation"],
        "writer_gid": writer_handle.get("endpoint_gid"),
        "reader_gid": reader_handle.get("endpoint_gid"),
        "writer_pid": writer_handle.get("pid"),
        "reader_pid": reader_handle.get("pid"),
        "topic": writer_handle.get("topic"),
        "writer_created_monotonic_ns": writer_handle.get("created_monotonic_ns"),
        "reader_created_monotonic_ns": reader_handle.get("created_monotonic_ns"),
        "snapshot_command_id": snapshot_command_id,
        "writer_match_command_id": (writer_snapshot or {}).get("command_id"),
        "reader_match_command_id": (reader_snapshot or {}).get("command_id"),
        "status": "unknown",
        "reasons": [],
        "writer_count": None,
        "reader_count": None,
        "writer_graph_contains_reader": None,
        "reader_graph_contains_writer": None,
        "source": ("public_rclpy_match_snapshot_and_graph_snapshot" if real_ros
                   else "offline_fake_backend_not_ros"),
        "real_ros": bool(real_ros),
        "execution_evidence": bool(real_ros),
        "candidate_eligible": False,
    }
    if writer_handle.get("topic") != reader_handle.get("topic"):
        fact["status"] = "incompatible"
        fact["reasons"] = ["topic_mismatch"]
        return fact
    if not _snapshot_complete(writer_snapshot) or not _snapshot_complete(reader_snapshot):
        fact["reasons"] = ["incomplete_snapshot"]
        return fact
    wmatch, rmatch = writer_snapshot["match_snapshot"], reader_snapshot["match_snapshot"]
    wgraph, rgraph = writer_snapshot["graph_snapshot"], reader_snapshot["graph_snapshot"]
    fact["writer_count"] = wmatch.get("matched_count")
    fact["reader_count"] = rmatch.get("matched_count")
    writer_in_reader = writer_handle.get("endpoint_gid") in _gid_set(rgraph.get("publishers", []))
    reader_in_writer = reader_handle.get("endpoint_gid") in _gid_set(wgraph.get("subscriptions", []))
    fact["writer_graph_contains_reader"] = reader_in_writer
    fact["reader_graph_contains_writer"] = writer_in_reader
    if wmatch.get("endpoint_gid") != writer_handle.get("endpoint_gid"):
        fact["reasons"].append("writer_snapshot_gid_mismatch")
        return fact
    if rmatch.get("endpoint_gid") != reader_handle.get("endpoint_gid"):
        fact["reasons"].append("reader_snapshot_gid_mismatch")
        return fact
    if wmatch["matched_count"] > 0 or rmatch["matched_count"] > 0:
        if not (writer_in_reader and reader_in_writer):
            fact["reasons"].append("count_positive_without_bidirectional_gid_evidence")
            return fact
        # Graph membership is discovery visibility, NOT a per-peer match ACK.
        # In an N graph two different edge sets can share these exact counts.
        unique_pair = all(
            _gid_set(graph.get("publishers", [])) == {writer_handle.get("endpoint_gid")}
            and _gid_set(graph.get("subscriptions", [])) == {reader_handle.get("endpoint_gid")}
            for graph in (wgraph, rgraph))
        if not unique_pair:
            fact["reasons"].append("aggregate_counts_do_not_identify_directed_pair")
        elif wmatch["matched_count"] != 1 or rmatch["matched_count"] != 1:
            fact["reasons"].append("single_pair_requires_both_counts_one")
        elif any(snap.get("command_id") is None
                 or snap["match_snapshot"].get("command_id") != snap.get("command_id")
                 for snap in (writer_snapshot, reader_snapshot)):
            fact["reasons"].append("single_pair_requires_command_bound_snapshots")
        else:
            fact["status"] = "matched"
            fact["identity_basis"] = "unique_bidirectional_graph_and_counts"
            fact["limitations"] = ["not_independent_per_peer_match_ack",
                                   "snapshot_scope_only_not_delivery_window"]
        return fact
    # Discovery visibility does not imply QoS compatibility or matching. A
    # zero aggregate count is a local non-match observation only when neither
    # graph exposes the peer. A visible peer with zero counts is an
    # observational ambiguity (stale discovery entry or a count that is not a
    # peer set), never proof of non-match; it must not become a SUT finding.
    if writer_in_reader or reader_in_writer:
        fact["limitations"] = ["graph_visibility_is_not_per_peer_match_proof",
                               "local_snapshot_counts_only_not_persistent_nonmatch"]
        fact["status"] = "unknown"
        fact["reasons"] = ["zero_count_contradicts_graph_entry"]
        return fact
    fact["limitations"] = ["local_snapshot_counts_only_not_persistent_nonmatch"]
    fact["status"] = "unmatched"
    fact["reasons"] = ["both_public_counts_zero"]
    return fact


def payload_delivery_witness(pair, receive, events):
    """Unique raw payload correlation, NOT MessageInfo/direct publisher GID.

    The payload's self-declared writer name is never trusted alone. Both worker
    identities must bind to created endpoint generations, and the same run/hash
    must select exactly one publish-returned raw send in the entire capture.
    """
    digest, run = receive.get("payload_sha256"), receive.get("payload_run_id")
    if not isinstance(digest, str) or len(digest) != 64 or not isinstance(run, str) or not run:
        return None, "payload_hash_or_run_missing"
    matches = [e for e in events if e.get("event") == "send" and e.get("payload_sha256") == digest
               and e.get("run_id") == run]
    if len(matches) != 1:
        return None, "payload_send_mapping_not_unique"
    send = matches[0]
    if (send.get("publish_returned") is not True
            or receive.get("run_id") != run
            or receive.get("payload_writer_id") != pair.get("writer")
            or type(receive.get("payload_writer_generation")) is not int
            or receive.get("payload_writer_generation") != pair.get("writer_generation")
            or send.get("logical_id") != pair.get("writer")
            or send.get("generation") != pair.get("writer_generation")
            or send.get("send_sequence") != receive.get("send_sequence")
            or send.get("writer_gid") != pair.get("writer_gid")
            or receive.get("payload_endpoint_gid") != pair.get("writer_gid")
            or receive.get("reader_gid") != pair.get("reader_gid")
            or receive.get("logical_id") != pair.get("reader")
            or receive.get("generation") != pair.get("reader_generation")):
        return None, "payload_run_writer_generation_sequence_or_gid_mismatch"
    for event, eid, gen, gid in ((send, pair.get("writer"), pair.get("writer_generation"), pair.get("writer_gid")),
                                 (receive, pair.get("reader"), pair.get("reader_generation"), pair.get("reader_gid"))):
        created = [e for e in events if e.get("event") == "endpoint_created"
                   and e.get("logical_id") == eid and e.get("generation") == gen
                   and e.get("endpoint_gid") == gid]
        if len(created) != 1 or any(event.get(k) is None or event.get(k) != created[0].get(k)
                                    for k in ("pid", "process_start_ticks", "endpoint_generation")):
            return None, "payload_worker_identity_not_bound"
        if not isinstance(event.get("monotonic_ns"), int) or created[0].get("monotonic_ns", event["monotonic_ns"]+1) > event["monotonic_ns"]:
            return None, "payload_worker_chronology_not_bound"
        if any(e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed", "worker_exit"}
               and e.get("logical_id") == eid and e.get("generation") == gen
               and e.get("monotonic_ns", event["monotonic_ns"]+1) < event["monotonic_ns"] for e in events):
            return None, "payload_retired_generation"
    return {"identity_basis": "endpoint_correlated_payload", "send_ref": send.get("event_index"),
            "receive_ref": receive.get("event_index"), "payload_sha256": digest,
            "writer": pair.get("writer"), "reader": pair.get("reader"),
            "writer_generation": pair.get("writer_generation"),
            "reader_generation": pair.get("reader_generation"),
            "direct_gid_match": None}, None


def _raw_active_writer_inventory(events, writer_id, send_ns):
    """Count writer generations alive on the writer's topic at ``send_ns``.

    Derived only from captured endpoint_created/endpoint_destroyed rows. A
    missing creation or retirement row makes the inventory incomplete, never 1.
    """
    tokens = [e for e in events if e.get("event") == "endpoint_created"
              and e.get("logical_id") == writer_id]
    if not tokens:
        return None, "raw_writer_endpoint_creation_missing"
    topics = {e.get("topic") for e in tokens}
    if len(topics) != 1 or None in topics:
        return None, "raw_writer_topic_incomplete"
    topic = next(iter(topics))
    alive = {}
    for token in [e for e in events if e.get("event") == "endpoint_created"
                  and worker_role_kind(e.get("role")) == "writer" and e.get("topic") == topic]:
        started = token.get("monotonic_ns")
        if type(started) is not int or started > send_ns:
            continue
        retired = [e for e in events if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                   and e.get("logical_id") == token.get("logical_id")
                   and e.get("generation") == token.get("generation")
                   and type(e.get("monotonic_ns")) is int]
        if len(retired) > 1:
            return None, "raw_writer_retirement_ambiguous"
        if retired and retired[0]["monotonic_ns"] <= send_ns:
            continue
        alive[(token.get("logical_id"), token.get("generation"))] = started
    return len(alive), None


def _single_writer_correlation(pair, events, active_writers_on_topic, sequence):
    """Explicit one-writer correlation, never a direct-GID or N-writer proof.

    The branch is admitted only when all of the following hold:
    * the caller supplies an actual integer inventory of exactly one live
      writer on the topic (a missing/None count is never coerced to one);
    * the pair carries bound writer/reader generations and GIDs;
    * the reader has a real creation timestamp and the matching send has a
      later real monotonic timestamp;
    * the entire captured send inventory for this sequence belongs to exactly
      one send of this writer generation.
    """
    if type(active_writers_on_topic) is not int or active_writers_on_topic != 1:
        # A missing value must never be coerced to "only one writer".
        return False, "active_writer_inventory_not_exactly_one"
    writer_id = pair.get("writer")
    writer_generation = pair.get("writer_generation")
    if (not isinstance(writer_id, str) or not writer_id
            or type(writer_generation) is not int
            or type(pair.get("reader_generation")) is not int):
        return False, "pair_identity_incomplete"
    if not (isinstance(pair.get("writer_gid"), str) and pair.get("writer_gid")
            and isinstance(pair.get("reader_gid"), str) and pair.get("reader_gid")):
        return False, "pair_gid_identity_incomplete"
    created = pair.get("reader_created_monotonic_ns")
    if type(created) is not int:
        return False, "reader_creation_chronology_missing"
    candidates = {(e.get("logical_id"), e.get("generation")) for e in events
                  if e.get("event") == "send" and e.get("send_sequence") == sequence}
    if candidates != {(writer_id, writer_generation)}:
        return False, "sequence_send_inventory_not_unique_to_pair_writer"
    sends = [e for e in events if e.get("event") == "send"
             and e.get("logical_id") == writer_id
             and e.get("generation") == writer_generation
             and e.get("send_sequence") == sequence]
    if len(sends) != 1 or type(sends[0].get("monotonic_ns")) is not int:
        return False, "send_chronology_missing"
    send_ns = sends[0]["monotonic_ns"]
    if send_ns < created:
        return False, "send_precedes_reader_creation"
    raw_count, raw_reason = _raw_active_writer_inventory(events, writer_id, send_ns)
    if raw_count is not None and raw_count != 1:
        return False, "raw_writer_inventory_contradicts_single_writer"
    return True, (raw_reason if raw_count is None else None)


def pair_delivery_fact(pair, events, *, active_writers_on_topic=None, real_ros=False):
    """Attribute deliveries per writer generation; never fill from a total count."""
    writer_gid = pair.get("writer_gid")
    reader_gid = pair.get("reader_gid")
    writer_id = pair.get("writer")
    reader_id = pair.get("reader")
    # A publish only counts for a reader that already existed when it happened.
    # Volatile history is not replayed to a later generation, so pre-creation
    # sequences must not be reported as "missing" for that pair.
    reader_created = pair.get("reader_created_monotonic_ns")
    sends = [e for e in events if e.get("event") == "send"
             and e.get("logical_id") == writer_id
             and e.get("generation") == pair.get("writer_generation")
             and (reader_created is None
                  or (isinstance(e.get("monotonic_ns"), int)
                      and e["monotonic_ns"] >= reader_created))]
    expected = [e.get("send_sequence") for e in sends]
    direct, correlated, single, unknown = [], [], [], []
    payload_witnesses, attribution_reasons, single_limitations = [], [], []
    for event in events:
        if event.get("event") != "receive":
            continue
        if event.get("logical_id") != reader_id:
            continue
        if event.get("generation") != pair.get("reader_generation"):
            continue
        sequence = event.get("send_sequence")
        if sequence not in expected:
            continue
        publisher_gid = event.get("publisher_gid")
        if publisher_gid is not None:
            if publisher_gid == writer_gid:
                direct.append(event)
            continue
        witness, reason = payload_delivery_witness(pair, event, events)
        if witness is not None:
            correlated.append(event)
            payload_witnesses.append(witness)
            continue
        # A present payload declaring another writer is not evidence for this
        # pair. No aggregate count fallback, even if only one writer happens to
        # survive at the final snapshot.
        if event.get("payload_writer_id") not in (None, writer_id):
            continue
        single_writer, single_reason = _single_writer_correlation(
            pair, events, active_writers_on_topic, sequence)
        if single_writer:
            single.append(event)
            if single_reason:
                single_limitations.append(single_reason)
            continue
        unknown.append(event)
        for item in (reason, single_reason):
            if item is not None and item not in attribution_reasons:
                attribution_reasons.append(item)
    rows = direct + correlated + single
    received = [event.get("send_sequence") for event in rows]
    missing = [sequence for sequence in expected if sequence not in received]
    duplicates = sorted({sequence for sequence in received if received.count(sequence) > 1})
    fact = {
        "schema": "r2f.topology-delivery-fact/v1",
        "writer": writer_id, "reader": reader_id,
        "writer_generation": pair.get("writer_generation"),
        "reader_generation": pair.get("reader_generation"),
        "writer_gid": writer_gid, "reader_gid": reader_gid,
        "expected_sequences": expected,
        "received_sequences": received,
        "missing_sequences": missing,
        "duplicate_sequences": duplicates,
        "receive_order": received,
        "in_order": received == sorted(received),
        "cross_writer_order_required": False,
        "identity_basis": ("message_info_gid" if direct and not unknown
                           else "endpoint_correlated_payload" if correlated and not unknown
                           else "endpoint_correlated_single_writer" if single and not unknown
                           else "unknown"),
        "direct_gid_receipts": len(direct),
        "correlated_receipts": len(correlated),
        "single_writer_receipts": len(single),
        "active_writers_on_topic": active_writers_on_topic,
        "single_writer_correlation_scope": "point_in_time_one_alive_writer_and_unique_sequence_send",
        "payload_witnesses": payload_witnesses,
        "communication_observed": bool(rows),
        "attribution_reasons": sorted(set(attribution_reasons)),
        "single_writer_limitations": sorted(set(single_limitations)),
        "limitations": ["payload_correlation_is_not_direct_publisher_gid",
                        "single_writer_correlation_is_not_direct_publisher_gid",
                        "active_writer_count_is_point_in_time_program_state",
                        "only_received_pair_generation_witnessed",
                        "finite_trailing_missing_is_not_a_violation"],
        "unknown_attribution_receipts": len(unknown),
        "status": "unknown",
        "reasons": [],
        "real_ros": bool(real_ros),
        "execution_evidence": bool(real_ros) and bool(rows or sends),
        "candidate_eligible": False,
    }
    if pair.get("status") not in ("matched", "unknown"):
        fact["status"] = "not_expected"
        fact["reasons"] = ["pair_not_matched"]
    elif not sends:
        fact["status"] = "unknown"
        fact["reasons"] = ["no_publish_batch_in_program"]
    elif unknown:
        fact["status"] = "unknown"
        fact["reasons"] = ["ambiguous_writer_attribution_without_message_info_gid"]
    elif not received:
        fact["status"] = "missing"
        fact["reasons"] = ["no_delivery_observed_for_expected_sequences"]
    elif missing:
        fact["status"] = "partial"
        fact["reasons"] = ["some_expected_sequences_not_observed"]
    else:
        fact["status"] = "complete"
    return fact


def _barrier_evaluate(backend, condition, endpoint_ids, handles, events,
                      previous=None):
    if condition == "all_created":
        targets = list(endpoint_ids) if endpoint_ids else list(handles)
        missing = [eid for eid in targets if eid not in handles]
        rows = [handles[eid] for eid in targets if eid in handles]
        alive = {h["logical_id"]: bool(h.get("alive")) for h in rows}
        ok = (not missing and bool(rows)
              and all(h.get("alive") and h.get("endpoint_gid") and h.get("pid") for h in rows))
        return ok, {"alive": alive, "missing_endpoints": missing}
    if condition in ("matched", "delivery", "quiescent") and not handles:
        # A barrier over zero live endpoints cannot be satisfied by an empty
        # snapshot alone; it stays an observation gap.
        return False, {"reason": "no_live_endpoints_for_barrier"}
    if condition in ("matched", "delivery"):
        snapshot = backend.snapshot(handles)
        real_ros = bool(getattr(backend, "real_ros", False))
        pairs = _pair_rows(handles, snapshot, events, real_ros=real_ros)
        if condition == "matched":
            ok = bool(pairs) and all(p["status"] == "matched" for p in pairs)
        else:
            delivery = _delivery_rows(pairs, events, handles, real_ros=real_ros)
            ok = bool(delivery) and all(row["status"] == "complete" for row in delivery)
        return ok, {"pairs": pairs, "snapshot_command_id": snapshot.get("command_id")}
    if condition == "quiescent":
        snapshot = backend.snapshot(handles)
        fingerprint = _snapshot_fingerprint(snapshot)
        ok = previous is not None and previous == fingerprint
        return ok, {"snapshot_command_id": snapshot.get("command_id"),
                    "snapshot_fingerprint": fingerprint, "previous_fingerprint": previous}
    raise TopologyUnsupported("unknown_barrier_condition", condition)


def _snapshot_fingerprint(snapshot):
    rows = []
    for logical_id, item in sorted((snapshot.get("snapshots") or {}).items()):
        match = item.get("match_snapshot") or {}
        graph = item.get("graph_snapshot") or {}
        rows.append({
            "logical_id": logical_id,
            "count": match.get("matched_count"),
            "publishers": sorted((row.get("endpoint_gid") for row in graph.get("publishers", [])))
            if isinstance(graph.get("publishers"), list) else None,
            "subscriptions": sorted((row.get("endpoint_gid") for row in graph.get("subscriptions", [])))
            if isinstance(graph.get("subscriptions"), list) else None,
            "complete": item.get("complete"),
        })
    return sha256_json(rows)


def _upsert(rows, new_rows, key_fields):
    """Keep the latest observation per identity key; never accumulate duplicates."""
    index = {}
    for position, row in enumerate(rows):
        index[tuple(row.get(f) for f in key_fields)] = position
    for row in new_rows:
        key = tuple(row.get(f) for f in key_fields)
        if key in index:
            rows[index[key]] = row
        else:
            index[key] = len(rows)
            rows.append(row)
    return rows


_PAIR_KEY = ("writer", "reader", "writer_generation", "reader_generation")



def _delivery_snapshot_chain(token, command_id, events, *, after_index, before_index=None):
    """Validate actual worker replies; a command ACK is never a DDS ACK."""
    limit = len(events) if before_index is None else before_index
    related = [e for e in events[after_index:limit]
               if e.get("logical_id") == token.get("logical_id")
               and e.get("generation") == token.get("generation")
               and e.get("command_id") == command_id]
    chain = []
    for kind in ("topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"):
        rows = [e for e in related if e.get("event") == kind]
        if command_id is None or len(rows) != 1:
            raise TopologyBackendError("delivery_snapshot_reply_missing_or_duplicate:" + kind)
        row = rows[0]
        if any(token.get(k) is None or row.get(k) != token.get(k) for k in
               ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")):
            raise TopologyBackendError("delivery_snapshot_worker_identity_mismatch:" + kind)
        if row.get("run_id") != events[0].get("run_id"):
            raise TopologyBackendError("delivery_snapshot_run_identity_mismatch")
        chain.append(row)
    request, match, graph, ack = chain
    if any(type(row.get("monotonic_ns")) is not int for row in chain):
        raise TopologyBackendError("delivery_snapshot_clock_invalid")
    if (request.get("command") != "snapshot"
            or match.get("endpoint_gid") != token.get("endpoint_gid")
            or type(match.get("matched_count")) is not int or match["matched_count"] < 0
            or not isinstance(graph.get("publishers"), list)
            or not isinstance(graph.get("subscriptions"), list)):
        raise TopologyBackendError("delivery_snapshot_fields_incomplete")
    for name in ("publishers", "subscriptions"):
        members = graph[name]
        gids = [row.get("endpoint_gid") if isinstance(row, dict) else None for row in members]
        if (any(not isinstance(gid, str) or not gid for gid in gids)
                or len(set(gids)) != len(gids)):
            raise TopologyBackendError("delivery_snapshot_graph_identity_malformed")
    created = [e for e in events[:limit] if e.get("event") == "endpoint_created"
               and all(e.get(k) == token.get(k) for k in
                       ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"))]
    if (len(created) != 1 or created[0]["event_index"] >= request["event_index"]
            or type(created[0].get("monotonic_ns")) is not int
            or type(request.get("monotonic_ns")) is not int
            or not created[0]["monotonic_ns"] <= request["monotonic_ns"] <= ack["monotonic_ns"] <= _now()):
        raise TopologyBackendError("delivery_snapshot_not_fresh_after_creation")
    if not all(a["event_index"] < b["event_index"]
               and type(a.get("monotonic_ns")) is int and type(b.get("monotonic_ns")) is int
               and a["monotonic_ns"] <= b["monotonic_ns"]
               for a, b in zip(chain, chain[1:])):
        raise TopologyBackendError("delivery_snapshot_chronology_invalid")
    return chain


def _delivery_window_evidence(opened, events, *, before_index=None):
    """Conservative 1W1R closure using the consumer's existing raw protocol.

    This does not prove per-peer matching or add a delivery obligation. Neither
    a publish-return nor a snapshot/publish command_done is a DDS ACK. Cross-
    worker stdout drain order is deliberately NOT a send-before-receive clock.
    """
    limit = len(events) if before_index is None else before_index
    prefix = events[:limit]
    end_ns = events[limit]["monotonic_ns"] if before_index is not None else _now()
    start = opened["event_index"]
    rows = prefix[start + 1:]
    reasons, tokens, chains = [], {}, {}
    for role in ("writer", "reader"):
        candidates = [e for e in prefix[:start] if e.get("event") == "endpoint_created"
                      and e.get("logical_id") == opened[role]
                      and e.get("generation") == opened[role + "_generation"]]
        if len(candidates) != 1:
            reasons.append("endpoint_creation_missing_or_ambiguous:" + role)
            continue
        token = tokens[role] = candidates[0]
        if (worker_role_kind(token.get("role")) != role or token.get("run_id") != opened.get("run_id")
                or token.get("endpoint_gid") != opened.get(role + "_gid")):
            reasons.append("window_endpoint_identity_mismatch:" + role)
        refs = opened.get("snapshot_refs", {}).get(token["logical_id"], [])
        try:
            if not isinstance(refs, list) or len(refs) != 4 or any(type(i) is not int or not 0 <= i < start for i in refs):
                raise TopologyBackendError("delivery_snapshot_refs_incomplete")
            chain = _delivery_snapshot_chain(
                token, events[refs[0]].get("command_id"), events,
                after_index=opened["snapshot_start_ref"], before_index=start)
            if refs != [e["event_index"] for e in chain]:
                raise TopologyBackendError("delivery_snapshot_refs_differ")
            if not token["monotonic_ns"] <= chain[0]["monotonic_ns"] <= chain[-1]["monotonic_ns"] <= opened["monotonic_ns"]:
                raise TopologyBackendError("delivery_snapshot_outside_open_chronology")
            chains[role] = chain
        except (TopologyBackendError, KeyError, TypeError, IndexError) as exc:
            reasons.append(str(exc))
        if any(e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed", "worker_exit"}
               and e.get("logical_id") == token["logical_id"]
               and e.get("generation") == token["generation"] for e in prefix[token["event_index"] + 1:]):
            reasons.append("endpoint_generation_retired:" + role)
    if len(tokens) != 2:
        return {"window_complete": False, "closure_status": "incomplete", "reasons": sorted(set(reasons)),
                "expected_sequences": [], "received_sequences": [], "missing_sequences": [],
                "gap_evidence": [], "send_refs": [], "receive_refs": [], "attribution": "unknown"}
    w, r = tokens["writer"], tokens["reader"]
    if len(chains) == 2:
        snaps = {role: {"complete": True, "command_id": chain[1]["command_id"],
                        "match_snapshot": chain[1], "graph_snapshot": chain[2]}
                 for role, chain in chains.items()}
        pair = pair_graph_facts(w, r, snaps["writer"], snaps["reader"])
        # Only a snapshot-scope unique pair is supported here. Do not emit a
        # controlled-isolation/per-peer match claim from these aggregate APIs.
        if pair["status"] != "matched":
            reasons.append("snapshot_scope_pair_not_matched")
    else:
        reasons.append("fresh_bidirectional_snapshot_chain_incomplete")
    pair = {key: opened.get(key) for key in
            ("writer", "reader", "writer_generation", "reader_generation", "writer_gid", "reader_gid")}

    def bound(event, token, gid_field):
        return (event.get("run_id") == opened.get("run_id")
                and all(token.get(k) is not None and event.get(k) == token.get(k) for k in
                        ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"))
                and event.get(gid_field) == token.get("endpoint_gid"))

    def in_clock(event):
        # The close is emitted after this evaluation, so the actual capture
        # prefix and a timestamp no earlier than open suffice at this point.
        return type(event.get("monotonic_ns")) is int and opened["monotonic_ns"] <= event["monotonic_ns"] <= end_ns

    sends = [e for e in rows if e.get("event") == "send" and e.get("logical_id") == w["logical_id"]]
    expected = {}
    for send in sends:
        seq = send.get("send_sequence")
        if (not bound(send, w, "writer_gid") or not in_clock(send)
                or send.get("publish_returned") is not True or type(seq) is not int or seq < 0 or seq in expected):
            reasons.append("send_identity_sequence_or_window_invalid")
            continue
        inventory = [e for e in prefix if e.get("event") == "send"
                     and e.get("logical_id") == w["logical_id"] and e.get("generation") == w["generation"]
                     and e.get("send_sequence") == seq]
        if len(inventory) != 1:
            reasons.append("send_sequence_not_raw_unique")
        active, _ = _raw_active_writer_inventory(prefix, w["logical_id"], send["monotonic_ns"])
        if active != 1:
            reasons.append("active_writer_generation_not_raw_unique")
        expected[seq] = send
    requests = [e for e in rows if e.get("event") == "topology_delivery_window_publish_requested"
                and e.get("open_ref") == start]
    if not requests or not expected:
        reasons.append("no_actual_postopen_send_obligation")
    for request in requests:
        actual = [e.get("send_sequence") for e in sends if e.get("phase") == request.get("action_id")]
        if actual != request.get("requested_sequences") or not actual:
            reasons.append("requested_publish_inventory_incomplete")
    if any(e.get("phase") not in {req.get("action_id") for req in requests} for e in sends):
        reasons.append("send_without_window_publish_request")
    if any(a["event_index"] >= b["event_index"]
           or type(a.get("monotonic_ns")) is not int or type(b.get("monotonic_ns")) is not int
           or a["monotonic_ns"] > b["monotonic_ns"]
           for a, b in zip(sends, sends[1:])):
        reasons.append("writer_generation_send_chronology_invalid")
    received, samples, attributions = {}, [], []
    for sample in rows:
        if sample.get("event") != "receive" or sample.get("logical_id") != r["logical_id"]:
            continue
        publisher = sample.get("publisher_gid")
        if publisher is not None and publisher != w["endpoint_gid"]:
            continue
        if not bound(sample, r, "reader_gid") or not in_clock(sample):
            reasons.append("receive_identity_or_window_invalid")
            continue
        seq = sample.get("send_sequence")
        if type(seq) is not int or seq not in expected:
            reasons.append("receive_without_current_window_send")
            continue
        active, _ = _raw_active_writer_inventory(prefix, w["logical_id"], sample["monotonic_ns"])
        if active != 1:
            reasons.append("receive_active_writer_generation_not_raw_unique")
            continue
        basis = "direct_publisher_gid"
        if publisher is None:
            witness, reason = payload_delivery_witness(pair, sample, prefix)
            if witness is None or witness["send_ref"] != expected[seq]["event_index"]:
                reasons.append("receive_payload_attribution_unknown:" + str(reason))
                continue
            basis = "endpoint_correlated_payload"
        received.setdefault(seq, []).append(sample)
        samples.append(sample)
        attributions.append(basis)
    reasons.extend(e.get("reason", "window_interrupted") for e in rows
                   if e.get("event") == "topology_delivery_window_interrupted" and e.get("open_ref") == start)
    missing = sorted(set(expected) - set(received))
    sentinel = max(expected) if expected else None
    if sentinel not in received:
        reasons.append("finite_window_without_received_final_sentinel")
    elif any(seq >= sentinel or expected[seq]["event_index"] >= expected[sentinel]["event_index"] for seq in missing):
        reasons.append("missing_sequence_without_later_same_writer_sentinel")
    complete = bool(expected) and not reasons
    evidence = {"window_complete": complete,
                "closure_status": "closed_with_gap" if complete and missing else "closed" if complete else "incomplete",
                "reasons": sorted(set(reasons)), "expected_sequences": sorted(expected),
                "received_sequences": sorted(received), "missing_sequences": missing,
                "send_refs": [e["event_index"] for e in sends],
                "receive_refs": [e["event_index"] for e in samples], "gap_evidence": [],
                "attribution": attributions[0] if attributions and len(set(attributions)) == 1 else "unknown"}
    if complete:
        evidence.update(sentinel_sequence=sentinel, sentinel_send_ref=expected[sentinel]["event_index"],
                        sentinel_receive_ref=received[sentinel][0]["event_index"])
        evidence["gap_evidence"] = [{"sequence": seq, "send_ref": expected[seq]["event_index"],
                                     "sentinel_send_ref": evidence["sentinel_send_ref"],
                                     "sentinel_receive_ref": evidence["sentinel_receive_ref"]} for seq in missing]
    return evidence


def _netem_live_identities(state):
    """Live endpoint identities verified against /proc, never from config.

    Every row is the exact generation token recorded at create time plus a
    freshly re-read process start tick.  Any mismatch is a problem that makes
    the counter snapshot unattributable instead of silently dropping the row.
    """
    rows, problems = [], []
    for eid, st in state.items():
        handle = st.get("handle")
        if not st.get("alive") or handle is None:
            continue
        pid = handle.get("pid")
        ticks = None
        try:
            ticks = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])
        except (OSError, ValueError, IndexError, TypeError) as exc:
            problems.append({"logical_id": eid, "problem": "proc_start_ticks_unavailable",
                             "error": type(exc).__name__})
        process = handle.get("process")
        running = bool(process is not None and process.process is not None
                       and process.process.poll() is None)
        row = {"logical_id": eid, "role": handle.get("role"),
               "worker_role": handle.get("worker_role"),
               "generation": handle.get("generation"),
               "endpoint_gid": handle.get("endpoint_gid"),
               "endpoint_generation": handle.get("endpoint_generation"),
               "pid": pid, "pgid": handle.get("pgid"),
               "process_start_ticks": handle.get("process_start_ticks"),
               "fresh_process_start_ticks": ticks, "process_running": running,
               "identity_verified": bool(running and ticks is not None
                                         and ticks == handle.get("process_start_ticks"))}
        rows.append(row)
        if not row["identity_verified"]:
            problems.append({"logical_id": eid, "problem": "endpoint_identity_not_live_or_ticks_mismatch"})
    return rows, problems


def _pair_rows(handles, snapshot, events, *, real_ros=False):
    rows = []
    snaps = snapshot.get("snapshots") or {}
    writers = [h for h in handles.values() if h.get("alive") and h.get("role") == "writer"]
    readers = [h for h in handles.values() if h.get("alive") and h.get("role") == "reader"]
    for writer in writers:
        for reader in readers:
            rows.append(pair_graph_facts(
                writer, reader, snaps.get(writer["logical_id"]), snaps.get(reader["logical_id"]),
                snapshot_command_id=snapshot.get("command_id"), real_ros=real_ros))
    return rows


def _delivery_rows(pairs, events, handles, *, real_ros=False):
    rows = []
    active_by_topic = {}
    for handle in handles.values():
        if handle.get("alive") and handle.get("role") == "writer":
            active_by_topic[handle.get("topic")] = active_by_topic.get(handle.get("topic"), 0) + 1
    for pair in pairs:
        rows.append(pair_delivery_fact(
            pair, events,
            active_writers_on_topic=active_by_topic.get(pair.get("topic"), 0),
            real_ros=real_ros))
    return rows


def topology_capabilities(backend=None):
    real = bool(getattr(backend, "real_ros", False))
    return {
        "schema": "r2f.topology-capabilities/v1",
        "backend": getattr(backend, "backend_name", None),
        "ros_execution": real,
        "max_writers": 4, "max_readers": 4, "max_endpoints": 8,
        "variable_cardinality": True, "empty_roles": True,
        "create_destroy_recreate": True,
        "fresh_endpoint_identity_per_generation": True,
        "per_pair_graph_facts": True,
        "per_pair_delivery_facts": True,
        "publish_batch_without_per_sample_ack": True,
        "bounded_barrier": True,
        "assert_liveliness_public_api": bool(getattr(backend, "supports_assert_liveliness", False)),
        "supported_message_types": ["std_msgs/msg/String"],
        "supported_bypass_fields": {"content_filter": initial_content_filter_capability(),
                                    "ownership_non_shared": False,
                                    "partitions_non_default": False,
                                    "durability_transient_or_later": False,
                                    "history_keep_all": False,
                                    "assert_liveliness_public_writer_api": bool(
                                        getattr(backend, "supports_assert_liveliness", False))},
        "max_batch_interval_sec": MAX_INTERVAL_SEC,
        "max_publish_batch": 1024,
        "execution_evidence": real,
        "candidate_eligible": False,
    }


def _execution_workers(events, raw_path):
    """Project all captured endpoint generations; never fill actuals from cfg.

    Raw refs are offsets into the enclosing result's SHA-bound raw file. No
    self-referential raw hash is embedded in that file's metadata event.
    """
    def ref(event):
        return {"raw_path": str(raw_path), "event_index": event["event_index"]}

    workers = []
    for token in [e for e in events if e.get("event") == "endpoint_created"]:
        eid, generation, pid = (token.get(k) for k in ("logical_id", "generation", "pid"))
        lineage = [e for e in events if e.get("pid") == pid
                   and e.get("logical_id", e.get("topology_logical_id")) == eid
                   and e.get("generation", e.get("topology_generation")) == generation]
        starts = [e for e in lineage if e.get("event") == "process_started"]
        exits = [e for e in lineage if e.get("event") in {"process_exit", "worker_exit", "topology_worker_exit"}]
        process_exits = [e for e in exits if e["event"] == "process_exit"]
        cleanup = [e for e in lineage if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}]
        actual = token.get("real_ros") is True and token.get("backend") == ROS_BACKEND
        returncodes = {e.get("returncode") for e in exits if type(e.get("returncode")) is int}
        returncode = next(iter(returncodes)) if len(returncodes) == 1 else None
        closed = bool(exits) and returncode is not None
        problems = []
        if not actual:
            problems.append("offline_fixture_not_actual_worker")
        for key in ("endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation"):
            if token.get(key) is None:
                problems.append("missing_token_field:" + key)
        if len(starts) != 1:
            problems.append("process_start_missing_or_ambiguous")
        if actual and len(process_exits) != 1:
            problems.append("process_exit_missing_or_ambiguous")
        if not closed:
            problems.append("exit_cleanup_unknown")
        if actual and token.get("rmw_identifier") is None:
            problems.append("actual_rmw_unknown")
        if actual and type(token.get("observed_domain_id")) is not int:
            problems.append("actual_domain_unknown")
        workers.append({
            "role": token.get("worker_role", token.get("topology_worker_role")),
            "endpoint_role": worker_role_kind(token.get("role")), "logical_id": eid,
            "generation": generation, "worker_generation": token.get("worker_generation"),
            "endpoint_gid": token.get("endpoint_gid"), "endpoint_generation": token.get("endpoint_generation"),
            "pid": pid, "pgid": token.get("pgid"), "process_start_ticks": token.get("process_start_ticks"),
            "rmw_identifier": token.get("rmw_identifier") if actual else None,
            "rmw": token.get("rmw_identifier") if actual else None,
            "domain_id": token.get("observed_domain_id") if actual else None,
            "domain_observation_source": token.get("domain_observation_source") if actual else None,
            "execution_domain_id": token.get("domain_id"),
            "fixture": not actual, "real_ros": actual,
            "fresh_process": len(starts) == 1 and starts[0].get("fresh_process") is True if actual else False,
            "separate_context": len(starts) == 1 and starts[0].get("separate_context") is True if actual else False,
            "returncode": returncode, "closed": closed,
            "cleanup_status": cleanup[-1].get("cleanup_status") if cleanup else "closed" if closed and actual else "unknown",
            "token_raw_ref": ref(token), "start_raw_refs": [ref(e) for e in starts],
            "exit_raw_refs": [ref(e) for e in exits], "cleanup_raw_refs": [ref(e) for e in cleanup],
            "unknowns": problems,
        })
    return workers


def run_topology(program, config, output_dir, *, backend=None):
    """Execute one bounded topology program; return runtime facts, never a verdict."""
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    events = []
    result = {
        "schema": RESULT_SCHEMA,
        "real_execution": False,
        "runtime_status": "blocked",
        "status": "blocked",
        "reason": None,
        "backend": None,
        "events": events,
        "facts": {},
        "environment": {},
        "environment_kind": "unknown",
        "backend_preflight": None,
        "preparation_receipt": None,
        "precondition_receipts": [],
        "observation_receipts": [],
        "mutation_receipt": {
            "planned": False, "submitted": False, "carrier_admitted": False,
            "configured": False, "applied": False, "hit": False,
            "effect_observed": False,
            "applicable": False,
            "reason": "topology_backend_is_a_scenario_carrier_not_a_mutation",
        },
        "execution": None,
        "execution_metadata": None,
        "raw_path": None,
        "raw_sha256": None,
        "capabilities": topology_capabilities(backend),
        "input_receipts": None,
        "pair_facts": [],
        "delivery_facts": [],
        "guard_receipts": [],
        "unknowns": [],
        "candidate_eligible": False,
        "mature": False,
        "sealed": False,
        "maturity_denominator": None,
    }
    try:
        cfg = normalize_topology_config(config)
    except (ValueError, TypeError) as exc:
        result["reason"] = "blocked:invalid_config:" + type(exc).__name__
        result["config_error"] = str(exc)
        return result
    backend_name = cfg.get("backend")
    if isinstance(backend, str):
        backend_name = backend
        backend = None
    if backend is None:
        if backend_name == OFFLINE_BACKEND:
            backend = OfflineTopologyBackend()
        elif backend_name in (None, ROS_BACKEND):
            backend = RosTopologyBackend(cfg, output_dir)
        else:
            result["reason"] = "blocked:unknown_backend:" + str(backend_name)
            return result
    result["backend"] = backend.backend_name
    result["capabilities"] = topology_capabilities(backend)
    if backend.real_ros and cfg.get("live_execution_go") is not True:
        # Real ROS/DDS execution requires an explicit, separately granted GO.
        # Nothing in this component may start a domain by default; the offline
        # backend stays available for contract tests.
        result["reason"] = "blocked:no_live_ros_go"
        result["live_go_receipt"] = {
            "required": True, "provided": False,
            "reason": "real ROS/DDS execution needs an explicit owner GO",
            "execution_evidence": False, "candidate_eligible": False,
        }
        return result
    result["live_go_receipt"] = {
        "required": bool(backend.real_ros), "provided": bool(backend.real_ros),
        "execution_evidence": False, "candidate_eligible": False,
    }
    if backend.real_ros and (cfg.get("offline") or cfg.get("fake") or backend_name == OFFLINE_BACKEND):
        result["reason"] = "blocked:offline_requested_for_live_backend"
        return result

    if backend.real_ros:
        from .topology_route import validate_topology_route
        try:
            live_plan = validate_topology_route(cfg)
            if live_plan.final_config["topology_program"] != program:
                raise ValueError("direct program differs from sealed final input")
        except (ValueError, TypeError, KeyError) as exc:
            result["reason"] = "blocked:topology_route_identity:" + str(exc)
            return result

    mutation_receipts = []
    effective_program = program
    try:
        mutations = cfg.get("input_mutations")
        if mutations:
            chain = model.mutate_chain(program, mutations, seed=cfg.get("seed", 0))
            effective_program = chain["program"]
            mutation_receipts = [{
                "layer_index": index,
                "mutator": layer.get("mutator"),
                "before_sha256": layer.get("before_sha256"),
                "after_sha256": layer.get("after_sha256"),
                "receipt": layer,
                "source": "r2f.modules.s1.topology_model.mutate_chain",
                "execution_evidence": False,
            } for index, layer in enumerate(chain.get("layers", []))]
            result["mutation_chain"] = {
                "initial_sha256": chain.get("initial_sha256"),
                "final_sha256": chain.get("input_sha256"),
                "layers": len(mutation_receipts),
                "receipt": chain.get("receipt"),
            }
    except (ValueError, TypeError) as exc:
        result["reason"] = "blocked:input_mutation_failed:" + type(exc).__name__ + ":" + str(exc)
        return result
    validation = model.validate_program(effective_program)
    try:
        result["input_receipts"] = build_input_receipts(
            effective_program, cfg, mutation_receipts=mutation_receipts)
    except (ValueError, TypeError) as exc:
        result["reason"] = "blocked:input_binding_failed:" + type(exc).__name__
        return result
    if validation.get("status") != "valid":
        result["reason"] = "blocked:invalid_program"
        result["validation"] = validation
        return result
    if not all(row.get("binding_status") in ("expressible", "not_evaluated")
               for row in result["input_receipts"].get("endpoints", [])):
        # A program containing an endpoint the public API cannot bind is
        # admitted only as an expected guard/capability rejection scenario.
        result["input_receipts"]["guard_disposition"] = "capability_reject_expected"
    if cfg.get("input_mutations") and sha256_json(effective_program) != result["input_receipts"]["program_sha256"]:
        result["reason"] = "blocked:mutated_program_not_bound"
        return result

    raw_path = output_dir / "raw.jsonl"
    try:
        raw = raw_path.open("x", buffering=1)
    except FileExistsError:
        result["reason"] = "blocked:state_directory_already_executed"
        return result
    result["raw_path"] = str(raw_path)

    def emit(event, **fields):
        value = {
            "event": event, "time_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
            "role": "topology_controller", "pid": os.getpid(), "pgid": os.getpgrp(),
            "run_id": cfg["run_id"], "domain_id": cfg["domain_id"],
            "backend": backend.backend_name,
        }
        # The raw row owns a deep JSON-like snapshot. Input/action receipts
        # continue to acquire diagnostic refs later; those must not mutate an
        # already serialized event through a shared nested dictionary.
        value.update(deepcopy(fields))
        value["event_index"] = len(events)
        serialized = json.dumps(value, sort_keys=True, allow_nan=False)
        # Own the *JSON* snapshot, including tuple/key normalization, so equality
        # with parsed raw is exact rather than merely equal for common inputs.
        value = json.loads(serialized)
        events.append(value)
        raw.write(serialized + "\n")
        if backend.real_ros and event == "endpoint_created":
            result["real_execution"] = True
        return deepcopy(value)

    backend.set_event_sink(lambda event, **fields: emit(event, **fields))
    if cfg.get("topology_route_binding") is not None:
        result["input_receipts"]["route_binding"] = deepcopy(cfg["topology_route_binding"])
    result["input_receipts"]["raw_event_ref"] = emit(
        "input_binding", receipt=result["input_receipts"])["event_index"]
    result["precondition_receipts"].append({
        "step_id": "input_binding", "receipt_kind": "input",
        "requires": [{"id": "structurally_valid_program",
                      "status": "satisfied" if validation["status"] == "valid" else "unsatisfied"}],
        "satisfied": validation["status"] == "valid",
        "before": None, "after": {"program_sha256": result["input_receipts"]["program_sha256"]},
        "disposition": "satisfied", "execution_evidence": False,
    })

    started_ns = _now()
    state = {
        spec["logical_id"]: {"generation": 0, "alive": False, "next_sequence": 0,
                             "handle": None, "history": []}
        for spec in effective_program["endpoints"]
    }
    endpoint_specs = {spec["logical_id"]: spec for spec in effective_program["endpoints"]}
    role_ordinals = {"writer": 0, "reader": 0}
    for spec in effective_program["endpoints"]:
        role_ordinals[spec["role"]] += 1
        if role_ordinals[spec["role"]] > 4:
            result["reason"] = "blocked:role_cardinality_exceeds_worker_contract"
            raw.close()
            return result
    # Assign stable worker aliases in declaration order.
    role_ordinals = {"writer": 0, "reader": 0}
    worker_roles = {}
    for spec in effective_program["endpoints"]:
        worker_roles[spec["logical_id"]] = worker_role_for(spec["role"], role_ordinals[spec["role"]])
        role_ordinals[spec["role"]] += 1

    def record_guard(action, code, message, *, kind="guard", details=None, before=None):
        receipt = {
            "step_id": "action:" + str(action.get("action_id")),
            "receipt_kind": "precondition",
            "action": deepcopy(action),
            "requires": [{"id": code, "status": "unsatisfied", "kind": kind}],
            "code": code,
            "kind": kind,
            "before": before,
            "after": deepcopy(before),
            "satisfied": False,
            "disposition": {"guard": "guard_reject", "capability": "capability_reject",
                            "tool": "tool_error", "environment": "environment_unverified"}.get(
                                kind, "guard_reject"),
            "method": "runtime_guard_before_side_effect",
            "no_side_effect": True,
            "message": message,
            "details": dict(details or {}),
            "execution_evidence": False,
        }
        result["precondition_receipts"].append(receipt)
        result["guard_receipts"].append(receipt)
        emit("topology_guard_reject", step_id=receipt["step_id"], code=code,
             kind=kind, message=message, no_side_effect=True,
             details=dict(details or {}), before=before)
        return receipt

    def record_action(action, before, after, *, disposition, satisfied, **extra):
        receipt = {
            "step_id": "action:" + str(action.get("action_id")),
            "receipt_kind": "precondition",
            "action": deepcopy(action),
            "requires": extra.pop("requires", []),
            "before": before,
            "after": after,
            "satisfied": satisfied,
            "disposition": disposition,
            "real_ros": bool(backend.real_ros),
            "execution_evidence": (bool(backend.real_ros)
                                   and disposition in ("applied", "effect_observed")),
            **extra,
        }
        result["precondition_receipts"].append(receipt)
        return receipt

    def snapshot_handles(endpoint_ids=None):
        handles = {eid: st["handle"] for eid, st in state.items()
                   if st["alive"] and st["handle"] is not None
                   and (endpoint_ids is None or eid in endpoint_ids)}
        if not handles:
            # Zero-endpoint programs are a supported boundary case; an empty
            # fresh snapshot is complete, not an observation failure.
            return {"snapshots": {}, "command_id": None, "complete": True}
        return backend.snapshot(handles)

    netem_ctx = None

    def netem_counter_snapshot(step_id, window):
        """Counter snapshot bound to the live endpoint identities and raw bytes."""
        if netem_ctx is None or not netem_ctx.get("active"):
            return None
        carrier = netem_ctx["carrier"]
        try:
            counter = carrier.counters()
        except Exception as exc:  # DiscoveryNetworkError/NetworkIsolationError
            receipt = getattr(exc, "receipt", {}) or {}
            emit("topology_netem_counter_failed", step_id=step_id,
                 error=type(exc).__name__, receipt=receipt, evidence_retained=True)
            netem_ctx.setdefault("counter_failures", []).append({"step_id": step_id,
                                                                "error": type(exc).__name__,
                                                                "receipt": receipt})
            return None
        identities, problems = _netem_live_identities(state)
        install_index = netem_ctx["install_index"]
        send_refs = [e["event_index"] for e in events
                     if e.get("event") == "send" and e["event_index"] > install_index]
        receive_refs = [e["event_index"] for e in events
                        if e.get("event") == "receive" and e["event_index"] > install_index]
        pair_bindings = None
        pair_binding_reason = None
        writers = [h for h in identities if h["role"] == "writer" and h["identity_verified"]]
        readers = [h for h in identities if h["role"] == "reader" and h["identity_verified"]]
        if len(writers) == 1 and len(readers) == 1:
            try:
                pair_bindings = carrier.pair_bindings(
                    {"writer": writers[0]["pid"], "reader": readers[0]["pid"]})
                pair_binding_reason = None
            except Exception as exc:  # socket-ownership proof unavailable
                pair_binding_reason = type(exc).__name__
        else:
            pair_binding_reason = "pair_bindings_require_exactly_one_writer_and_reader"
        receipt = emit("topology_netem_counter", step_id=step_id, window=window,
                       install_ref={"path": str(raw_path), "event_index": install_index},
                       counter=counter, counter_observed=True,
                       installed_spec=deepcopy(netem_ctx["spec"]),
                       endpoint_identities=identities, identity_problems=problems,
                       worker_socket_bindings=pair_bindings,
                       worker_socket_binding_reason=pair_binding_reason,
                       send_refs=send_refs, receive_refs=receive_refs,
                       hit_semantics="environment_network_effect_not_sut_defect",
                       responsibility_layer="environment_harness",
                       s1_mutation_hit=False)
        netem_ctx.setdefault("counters", []).append(receipt)
        return receipt

    delivery_windows = {}

    def close_delivery_windows(reason=None, *, endpoint_id=None, topic=None):
        """End the capture, not the DDS obligation; interruptions stay raw."""
        for key, opened in list(delivery_windows.items()):
            if endpoint_id is not None and endpoint_id not in (opened["writer"], opened["reader"]) and topic != opened.get("topic"):
                continue
            if reason is not None:
                emit("topology_delivery_window_interrupted", open_ref=opened["event_index"],
                     reason=reason, fixture=not backend.real_ros, candidate_eligible=False)
            evidence = _delivery_window_evidence(opened, events)
            close = emit("topology_delivery_window_closed",
                         **{k: opened[k] for k in _PAIR_KEY},
                         open_ref=opened["event_index"], **evidence,
                         cross_writer_order_required=False,
                         match_scope="snapshot_scope_only_not_per_peer_match_proof",
                         closure_basis="actual_same_writer_generation_final_sentinel_receive_only",
                         fixture=not backend.real_ros, candidate_eligible=False,
                         execution_evidence=bool(backend.real_ros))
            result.setdefault("delivery_windows", []).append(close)
            del delivery_windows[key]

    def prepare_delivery_windows(writer, action_id, sequences):
        readers = {eid: st["handle"] for eid, st in state.items()
                   if st["alive"] and st["handle"] is not None
                   and endpoint_specs[eid]["role"] == "reader"}
        if not readers:
            # Writer-only and pre-reader publishes remain valid program actions.
            # A later reader must not inherit those sends as a post-match window.
            return
        handles = {writer["logical_id"]: writer, **readers}
        snapshot_start = len(events)
        snapshot = backend.snapshot(handles)
        if snapshot.get("complete") is not True:
            raise TopologyBackendError("delivery_snapshot_capture_incomplete")
        chains = {}
        for eid, handle in handles.items():
            row = snapshot.get("snapshots", {}).get(eid, {})
            chains[eid] = _delivery_snapshot_chain(
                handle, row.get("command_id"), events, after_index=snapshot_start)
        # Named isolation must precede this window's open, not merely the
        # final state projection.  Count/graph snapshots alone are not peer
        # proof; the observer independently checks all backend-owned workers
        # and kernel namespace membership. N-peer inventories stay unknown.
        measured = environment_observer.observe(
            "before_delivery:" + str(action_id),
            {eid: st["handle"] for eid, st in state.items()
             if st["alive"] and st["handle"] is not None}, chains,
            network_mutated=netem_decl is not None or netem_ctx is not None)
        result.setdefault("environment_observations", []).append(measured)
        for reader in readers.values():
            key = (writer["logical_id"], reader["logical_id"], writer["generation"], reader["generation"])
            if key not in delivery_windows:
                delivery_windows[key] = emit(
                    "topology_delivery_window_open", **dict(zip(_PAIR_KEY, key)),
                    writer_gid=writer["endpoint_gid"], reader_gid=reader["endpoint_gid"],
                    topic=writer["topic"], snapshot_start_ref=snapshot_start,
                    snapshot_refs={eid: [e["event_index"] for e in chains[eid]]
                                   for eid in (writer["logical_id"], reader["logical_id"])},
                    observation_scope="current_pair_generation_sends_and_receives",
                    match_scope="snapshot_scope_only_not_per_peer_match_proof",
                    fixture=not backend.real_ros, candidate_eligible=False,
                    execution_evidence=bool(backend.real_ros))
            # This is a controller request, not proof of any actual send. The
            # close independently checks its exact raw publish-return inventory.
            emit("topology_delivery_window_publish_requested", **dict(zip(_PAIR_KEY, key)),
                 open_ref=delivery_windows[key]["event_index"], action_id=action_id,
                 requested_sequences=list(sequences), execution_evidence=False,
                 fixture=not backend.real_ros, candidate_eligible=False)

    def interrupt_writer_windows(writer_id, reason):
        for key, opened in delivery_windows.items():
            if key[0] == writer_id:
                emit("topology_delivery_window_interrupted", open_ref=opened["event_index"],
                     reason=reason, fixture=not backend.real_ros, candidate_eligible=False)


    def note_snapshot(action_id, snapshot, label=None):
        receipt = {
            "step_id": "snapshot:" + str(action_id),
            "receipt_kind": "observation",
            "snapshot_command_id": snapshot.get("command_id"),
            "label": label,
            "snapshots": deepcopy(snapshot.get("snapshots", {})),
            "complete": snapshot.get("complete") is True,
            "real_ros": bool(backend.real_ros),
            "execution_evidence": bool(backend.real_ros),
            "candidate_eligible": False,
        }
        result["observation_receipts"].append(receipt)
        live = {eid: st["handle"] for eid, st in state.items() if st["alive"]}
        pairs = _pair_rows(live, snapshot, events, real_ros=bool(backend.real_ros))
        delivery = _delivery_rows(pairs, events, live, real_ros=bool(backend.real_ros))
        _upsert(result["pair_facts"], pairs, _PAIR_KEY)
        _upsert(result["delivery_facts"], delivery, _PAIR_KEY)
        netem_counter_snapshot(receipt["step_id"], "action_window")
        emit("topology_pair_observation", step_id=receipt["step_id"],
             snapshot_command_id=snapshot.get("command_id"), pairs=pairs, delivery=delivery)
        return receipt

    def state_boundary(phase, snapshot=None, *, snapshot_start_ref=None):
        """Capture a real prefix boundary, not an assertion of full proof.

        Lifecycle covers only this program's managed endpoint identities.
        Snapshot completeness is command-bound per worker; peer matching,
        network phase and closed delivery windows remain separate obligations.
        """
        lifecycle_ok = True
        lifecycle_refs, snapshot_refs, missing = [], [], []
        for eid, st in state.items():
            tokens = [e for e in events if e.get("event") == "endpoint_created" and e.get("logical_id") == eid]
            if [e.get("generation") for e in tokens] != list(range(1, st["generation"] + 1)):
                lifecycle_ok = False
                missing.append("generation_capture_incomplete:" + eid)
            for token in tokens:
                lifecycle_refs.append(token["event_index"])
                if token.get("generation") < st["generation"] or not st["alive"]:
                    retired = [e for e in events if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                               and all(e.get(k) == token.get(k) for k in
                                       ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation"))
                               and e.get("cleanup_status") == "closed" and e["event_index"] > token["event_index"]]
                    if len(retired) != 1:
                        lifecycle_ok = False
                        missing.append("retirement_capture_incomplete:" + eid)
                    lifecycle_refs.extend(e["event_index"] for e in retired)
        live = {eid: st for eid, st in state.items() if st["alive"] and st["handle"] is not None}
        observed = bool(live) and isinstance(snapshot, dict) and snapshot.get("complete") is True
        chains = {}
        for eid, st in live.items():
            row = (snapshot or {}).get("snapshots", {}).get(eid, {})
            try:
                if snapshot_start_ref is None:
                    raise TopologyBackendError("boundary_snapshot_capture_start_missing")
                chains[eid] = _delivery_snapshot_chain(
                    st["handle"], row.get("command_id"), events, after_index=snapshot_start_ref)
                snapshot_refs.extend(e["event_index"] for e in chains[eid])
            except TopologyBackendError as exc:
                observed = False
                missing.append("snapshot_command_identity_or_chronology_invalid:" + eid + ":" + str(exc))
        measured = environment_observer.observe(
            phase, {eid: st["handle"] for eid, st in live.items()}, chains,
            network_mutated=netem_decl is not None or netem_ctx is not None
            or any(e.get("event") == "runtime_abort" for e in events))
        result.setdefault("environment_observations", []).append(measured)
        # Empty is an observed backend/kernel inventory, never bool(live)==False
        # masquerading as a completed observation. No DDS window is invented.
        if not live and measured["empty_inventory_verified"]:
            observed = True
        if not observed:
            missing.append("fresh_command_bound_snapshot_not_complete")
        # Re-evaluate actual close prefixes, not a permanent false/true flag.
        # Historical incomplete windows remain visible across lifecycle changes;
        # a surviving/new generation cannot silently repair the previous one.
        closes = [e for e in events if e.get("event") == "topology_delivery_window_closed"]
        current = [(w, r) for w in live if endpoint_specs[w]["role"] == "writer"
                   for r in live if endpoint_specs[r]["role"] == "reader"]
        windows_ok = (bool(current) or measured["empty_inventory_verified"]) and not delivery_windows
        window_refs = [e["event_index"] for e in closes]
        if not current and not measured["empty_inventory_verified"]:
            missing.append("no_current_pair_delivery_window")
        for w, r in current:
            matching = [e for e in closes if e.get("writer") == w and e.get("reader") == r
                        and e.get("writer_generation") == live[w]["generation"]
                        and e.get("reader_generation") == live[r]["generation"]]
            if not matching:
                windows_ok = False
                missing.append("current_generation_delivery_window_missing:" + w + ":" + r)
        for close in closes:
            open_ref = close.get("open_ref")
            valid = (type(open_ref) is int and 0 <= open_ref < close["event_index"]
                     and events[open_ref].get("event") == "topology_delivery_window_open"
                     and all(close.get(k) == events[open_ref].get(k) for k in _PAIR_KEY))
            evidence = (_delivery_window_evidence(events[open_ref], events, before_index=close["event_index"])
                        if valid else {})
            if not (valid and close.get("window_complete") is True and evidence.get("window_complete") is True
                    and all(close.get(k) == evidence.get(k) for k in
                            ("expected_sequences", "received_sequences", "missing_sequences",
                             "send_refs", "receive_refs", "sentinel_sequence", "sentinel_send_ref",
                             "sentinel_receive_ref", "gap_evidence"))):
                windows_ok = False
                missing.append("raw_delivery_window_incomplete:" + str(close["event_index"]))
        if any(e.get("event") == "runtime_abort" for e in events):
            windows_ok = False
            missing.append("abnormal_program_termination")
        return emit("topology_state_boundary", phase=phase,
             program_sha256=result["input_receipts"]["program_sha256"],
             lifecycle_complete=lifecycle_ok, observation_complete=bool(observed),
             window_complete=bool(windows_ok), delivery_window_refs=window_refs,
             lifecycle_scope="managed_program_endpoint_generations_only",
             observation_scope="current_worker_snapshot_commands_only_not_full_topology_proof",
             lifecycle_refs=sorted(set(lifecycle_refs)), snapshot_refs=sorted(set(snapshot_refs)),
             environment_measurement_ref=measured["measurement_ref"],
             controlled_isolation_ref=measured["isolation_ref"],
             limitations=missing + measured["limitations"]
             + ["delivery_closure_does_not_prove_per_peer_match", "graph_membership_not_per_peer_match_proof"]
             + ([] if measured["network_neutral_verified"] else ["network_phase_not_measured"]),
             fixture=not backend.real_ros, execution_evidence=bool(backend.real_ros))

    lease = None
    source_before = None

    def check_topology_source(phase):
        if not backend.real_ros:
            return
        from r2f.campaign.identity import source_manifest
        measured = source_manifest(cfg["source_repo"])
        emit("topology_source_checked", phase=phase, source=measured)
        if (not measured.get("source_frozen") or measured != cfg.get("source_manifest")
                or (source_before is not None and measured != source_before)):
            raise TopologyBackendBlocked("source_identity_changed_or_not_frozen")
        return measured

    def route_presteps():
        """Execute the imported compile-time target contract before any action.

        Fixture preparation is not environment/source/lease authentication. Its
        blocked receipts are retained, but only the explicit offline scenario
        may continue past them; it never reaches the canonical target.
        """
        from r2f.modules.s1.topology_targets import topology_target_declaration
        from .topology_route import validate_topology_route, _binding

        target = topology_target_declaration()
        plan = validate_topology_route(cfg)
        if (target.target_id != cfg["target_id"]
                or target.target_revision != cfg["target_revision"]
                or target.observation_contract.get("input_mutation_phase") != "compile_time"):
            raise TopologyBackendBlocked("topology_premutation_target_contract_mismatch")
        contract_hash = sha256_json(target.to_dict())
        if backend.real_ros:
            # These are authenticated resources/measurements from the canonical
            # preflight, NOT cfg booleans, prepared.ok, or lease-receipt strings.
            from .ros2_pair import verify_pair_lease
            held = verify_pair_lease(lease, cfg, output_dir)
            measured = check_topology_source("before_presteps")
            ready = {"environment_ok": preflight.get("ok") is True,
                     "locks_held": len(held) == 3,
                     "source_identity_ok": measured == source_before and measured.get("source_frozen") is True,
                     "topology_capability_admitted": True}
        else:
            ready = {"environment_ok": False, "locks_held": False,
                     "source_identity_ok": False, "topology_capability_admitted": True}
        receipts = []
        for step in target.state_steps:
            if step.action not in {"admit_environment", "bind_input"}:
                raise TopologyBackendBlocked("topology_unsupported_premutation_action:" + step.action)
            before = deepcopy(ready)
            checks = {c.condition_id: c.evaluate(before) for c in step.requires}
            record = {"step_id": step.step_id, "action": step.action,
                      "receipt_kind": "precondition", "before": before, "after": None,
                      "requires": checks, "satisfied": False,
                      "fixture": not backend.real_ros, "execution_evidence": False,
                      "target_contract_sha256": contract_hash}
            start = emit("state_step_before", **record)
            evidence = {"target_contract_sha256": contract_hash}
            error = None
            if step.action == "admit_environment":
                if step.parameters.get("runtime_capability") != cfg.get("runtime_capability"):
                    error = "topology_prestep_capability_mismatch"
            else:
                # Inspect the already persisted prefix. Neither a returned
                # receipt nor a supplied binding token authenticates the input.
                raw.flush()
                prefix = [json.loads(line) for line in raw_path.read_text().splitlines()]
                bindings = [e for e in prefix if e.get("event") == "input_binding"]
                expected = _binding(plan, cfg["input_layer_receipts"], cfg["effective_input_sha256"])
                if (step.parameters.get("binding") != "topology.program"
                        or prefix != events or len(bindings) != 1
                        or bindings[0]["event_index"] >= start["event_index"]
                        or bindings[0].get("receipt", {}).get("program_sha256") != sha256_json(effective_program)
                        or plan.final_config != {"topology_program": effective_program}
                        or bindings[0].get("receipt", {}).get("route_binding") != expected
                        or cfg.get("topology_route_binding") != expected):
                    error = "topology_prestep_raw_input_binding_mismatch"
                evidence.update(input_binding_verified=error is None,
                                input_binding_ref=bindings[0]["event_index"] if len(bindings) == 1 else None,
                                input_binding_raw_ref={"raw_path": str(raw_path),
                                                       "event_index": bindings[0]["event_index"] if len(bindings) == 1 else None},
                                route_binding=expected)
            satisfied = all(checks.values()) and error is None
            if satisfied:
                ready.update(deepcopy(step.parameters.get("after_updates", {})))
            record.update(after=deepcopy(ready), satisfied=satisfied,
                          status="failed" if error else "executed" if satisfied else "blocked",
                          disposition="satisfied" if satisfied else "environment_unverified",
                          execution_evidence=bool(backend.real_ros and satisfied), evidence=evidence,
                          reason=error or (None if satisfied else "fixture_environment_source_lease_unverified"))
            finish = emit("state_step_after", **record)
            record.update(raw_before_index=start["event_index"], raw_after_index=finish["event_index"])
            receipts.append(record)
            result["precondition_receipts"].append(record)
            if error:
                raise TopologyBackendBlocked(error)
        reached = all(c.evaluate(ready) for c in target.state_preconditions)
        result["canonical_preconditions_reached"] = reached
        result["canonical_precondition_receipts"] = receipts
        if backend.real_ros and not reached:
            raise TopologyBackendBlocked("topology_premutation_preconditions_not_reached")

    netem_decl = None
    netem_isolation = None
    environment_observer = TopologyEnvironmentObservation(backend, events, emit)
    try:
        netem_decl = normalize_topology_environment_mutation(cfg.get("environment_mutation"))
        if backend.real_ros and netem_decl is not None:
            if cfg.get("arm") != "mutated":
                raise TopologyBackendBlocked("topology_netem_requires_mutated_arm")
            # Hard gate before any host-side preflight: a declared tc-netem
            # window may only exist inside the verified isolated namespace.
            # The abort receipt stays in raw; no tc/ip/nft command ran here.
            netem_isolation = netem_isolation_receipt()
            emit("topology_netem_isolation", receipt=netem_isolation,
                 proof_env=NETEM_ISOLATION_ENV, phase="before_preflight",
                 fixture=not backend.real_ros, s1_mutation_hit=False)
            if not netem_isolation["isolated"]:
                raise TopologyBackendBlocked("topology_netem_requires_isolated_netns")
        if backend.real_ros:
            from .ros2_pair import acquire_pair_environment, verify_pair_lease
            source_before = check_topology_source("before_preflight")
            lease, preflight, child_env = acquire_pair_environment(cfg, output_dir, emit, result=result)
            result["environment"] = deepcopy(preflight)
            result["environment_kind"] = "canonical_preflight"
            verify_pair_lease(lease, cfg, output_dir)
            backend.execution_lease = lease
            backend.execution_preflight = preflight
            backend.execution_environment = child_env
            check_topology_source("after_preflight")
            from .topology_namespace import current_child_context
            namespace_context = current_child_context()
            if namespace_context is not None:
                namespace_context.leases_ready(lease, cfg, output_dir, emit)
        prepared = backend.prepare(effective_program, cfg)
        result["backend_preflight"] = deepcopy(prepared)
        result["preparation_receipt"] = deepcopy(prepared)
        if not backend.real_ros:
            result["fixture_environment"] = deepcopy(prepared)
            result["environment_kind"] = "offline_fixture_no_canonical_manifest"
        emit("topology_prepare", prepared=prepared)
        if not prepared.get("ok"):
            result["reason"] = prepared.get("reason") or "blocked:backend_prepare_failed"
            result["runtime_status"] = "blocked"
            result["status"] = "blocked"
            return result

        if cfg.get("topology_route_binding") is not None:
            route_presteps()

        emit("mutation_begin", mutation_kind=cfg.get("mutation_kind"),
             route_binding=deepcopy(cfg.get("topology_route_binding")),
             input_binding_ref=result["input_receipts"]["raw_event_ref"],
             fixture=not backend.real_ros, execution_evidence=bool(backend.real_ros))
        emit("topology_provenance", runtime_kind="ros2_public_api" if backend.real_ros else "offline_fixture",
             fixture=not backend.real_ros, backend=backend.backend_name,
             source=source_before, input_program_sha256=sha256_json(effective_program))
        state_boundary("before_program")

        # Opt-in environment mutation: only a real, isolated namespace may host
        # the existing tc-netem carrier.  Nothing here is derived from config.
        if netem_decl is not None:
            if not backend.real_ros:
                raise TopologyBackendBlocked("offline_fixture_cannot_claim_environment_mutation")
            if netem_isolation is None or netem_isolation.get("isolated") is not True:
                raise TopologyBackendBlocked("topology_netem_requires_isolated_netns")
            from .discovery_network import DiscoveryNetemController
            carrier = DiscoveryNetemController(cfg["domain_id"])
            setup = carrier.setup()
            emit("topology_netem_setup", receipt=setup)
            try:
                begin = carrier.start(netem_decl["spec"])
            except Exception as exc:  # DiscoveryNetworkError / NetworkIsolationError
                failure = getattr(exc, "receipt", {}) or {}
                emit("topology_netem_install_failed", error=type(exc).__name__,
                     receipt=failure, evidence_retained=True,
                     residual_qdisc=failure.get("restore_after_failure", {}).get("residual_qdisc"),
                     residual_filter=failure.get("restore_after_failure", {}).get("residual_filter"))
                raise TopologyBackendError("topology_netem_install_failed") from exc
            install_event = emit("topology_netem_install", receipt=begin,
                                 spec=deepcopy(netem_decl["spec"]),
                                 implementation=begin.get("implementation"),
                                 tc_dump_sha256=begin.get("tc_dump_sha256"),
                                 install_command=begin.get("install_command"),
                                 launch_stack="direct_process_in_isolated_netns",
                                 s1_mutation_hit=False,
                                 hit_semantics="environment_network_effect_not_sut_defect")
            netem_ctx = {"active": True, "carrier": carrier, "spec": deepcopy(netem_decl["spec"]),
                         "install_index": install_event["event_index"],
                         "counters": [], "counter_failures": []}
        for action_index, action in enumerate(effective_program["actions"]):
            check_topology_source("before_action:" + action["action_id"])
            action_id = action["action_id"]
            action_state = state.get(action.get("endpoint_id"))
            action_before = ({key: action_state[key] for key in
                              ("generation", "alive", "next_sequence")}
                             if action_state is not None else
                             {"alive": sorted(e for e, st in state.items() if st["alive"])})
            emit("topology_action_begin", action_id=action_id, action_index=action_index,
                 action=deepcopy(action), before=deepcopy(action_before))
            emit("state_step_before", step_id="action:" + action_id, action=deepcopy(action),
                 before=deepcopy(action_before), after=None, requires=[], satisfied=None,
                 fixture=not backend.real_ros)
            try:
                aid, op = action["action_id"], action["op"]
                eid, gen = action["endpoint_id"], action["generation"]
                params = action.get("params", {})
                if op in ("snapshot", "barrier") and eid is None:
                    before = {"alive": sorted(e for e, st in state.items() if st["alive"])}
                else:
                    st = state.get(eid)
                    if st is None:
                        record_guard(action, "unknown_endpoint", "endpoint is not declared")
                        continue
                    before = {"generation": st["generation"], "alive": st["alive"],
                              "next_sequence": st["next_sequence"]}
                if op in ("create", "recreate"):
                    st = state[eid]
                    if op == "create":
                        if not (gen == 1 and st["generation"] == 0 and not st["alive"]):
                            record_guard(action, "first_generation_guard",
                                         "create requires generation 1 from generation 0",
                                         before=before)
                            continue
                    else:
                        if not (st["generation"] > 0 and gen == st["generation"] + 1):
                            record_guard(action, "next_generation_guard",
                                         "recreate requires previous generation + 1",
                                         before=before)
                            continue
                    if st["alive"] and st["handle"] is not None:
                        # recreate replaces a live endpoint; destroy is a real step.
                        try:
                            cleanup = backend.destroy_endpoint(st["handle"])
                            close_delivery_windows("endpoint_lifecycle_changed", endpoint_id=eid,
                                                   topic=endpoint_specs[eid]["topic"])
                        except TopologyBackendError as exc:
                            record_guard(action, "predecessor_destroy_failed",
                                         type(exc).__name__, kind="tool", before=before)
                            continue
                        old = st["handle"]
                        st["history"].append({"event": "retired", "logical_id": eid,
                                              "generation": old["generation"],
                                              "endpoint_gid": old.get("endpoint_gid"),
                                              "pid": old.get("pid"), "cleanup": cleanup})
                        emit("topology_generation_retired", logical_id=eid,
                             generation=old["generation"], endpoint_gid=old.get("endpoint_gid"),
                             pid=old.get("pid"), cleanup=cleanup)
                        st["alive"] = False
                    # Backend-independent capability gate: the same decision must
                    # hold for the live and offline backends so a guard rejection is
                    # never an artifact of which backend happened to be selected.
                    try:
                        endpoint_expressibility(endpoint_specs[eid], cfg)
                    except TopologyUnsupported as exc:
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        continue
                    try:
                        handle = backend.create_endpoint(endpoint_specs[eid], gen, worker_roles[eid])
                        close_delivery_windows("endpoint_lifecycle_changed", endpoint_id=eid,
                                               topic=endpoint_specs[eid]["topic"])
                    except TopologyUnsupported as exc:
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        continue
                    except TopologyBackendTimeout as exc:
                        record_guard(action, "endpoint_create_timeout", str(exc), kind="tool",
                                     before=before)
                        continue
                    except TopologyBackendError as exc:
                        record_guard(action, "endpoint_create_error", type(exc).__name__,
                                     kind="tool", before=before)
                        continue
                    if backend.real_ros:
                        from .topology_namespace import current_child_context
                        namespace_context = current_child_context()
                        if namespace_context is not None:
                            namespace_context.worker_created(handle, emit)
                    st.update({"generation": gen, "alive": True, "next_sequence": 0,
                               "handle": handle})
                    st["history"].append({"event": "created", "logical_id": eid,
                                          "generation": gen,
                                          "endpoint_gid": handle.get("endpoint_gid"),
                                          "pid": handle.get("pid"),
                                          "endpoint_generation": handle.get("endpoint_generation")})
                    after = {"generation": gen, "alive": True,
                             "endpoint_gid": handle.get("endpoint_gid"),
                             "pid": handle.get("pid"),
                             "process_start_ticks": handle.get("process_start_ticks"),
                             "endpoint_generation": handle.get("endpoint_generation")}
                    record_action(action, before, after, disposition="applied", satisfied=True,
                                  requires=[{"id": "fresh_endpoint_identity", "status": "satisfied",
                                             "kind": "identity"}],
                                  identity_bound=True)
                    continue
                if op == "destroy":
                    st = state[eid]
                    if not (st["alive"] and st["generation"] == gen):
                        record_guard(action, "current_generation_alive_guard",
                                     "destroy requires current live generation", before=before)
                        continue
                    try:
                        cleanup = backend.destroy_endpoint(st["handle"])
                        close_delivery_windows("endpoint_lifecycle_changed", endpoint_id=eid,
                                               topic=endpoint_specs[eid]["topic"])
                    except TopologyBackendError as exc:
                        record_guard(action, "endpoint_destroy_error", type(exc).__name__,
                                     kind="tool", before=before)
                        continue
                    old = st["handle"]
                    st["history"].append({"event": "retired", "logical_id": eid,
                                          "generation": gen,
                                          "endpoint_gid": old.get("endpoint_gid"),
                                          "pid": old.get("pid"), "cleanup": cleanup})
                    st.update({"alive": False, "handle": None})
                    record_action(action, before, {"generation": gen, "alive": False},
                                  disposition="applied", satisfied=True,
                                  requires=[{"id": "current_generation_alive", "status": "satisfied"}],
                                  cleanup=cleanup)
                    continue
                if op == "publish_batch":
                    st = state[eid]
                    if not (st["alive"] and st["generation"] == gen and
                            endpoint_specs[eid]["role"] == "writer"):
                        record_guard(action, "writer_generation_alive_guard",
                                     "publish_batch requires a live writer generation", before=before)
                        continue
                    count, start = params.get("count"), params.get("start_sequence")
                    if type(count) is not int or type(start) is not int:
                        record_guard(action, "invalid_batch_params", "count/start_sequence must be integers",
                                     before=before)
                        continue
                    sequences = list(range(start, start + count))
                    if start < st["next_sequence"]:
                        record_guard(action, "nonoverlapping_writer_generation_sequence",
                                     "batch overlaps a previous sequence on this writer generation",
                                     details={"start_sequence": start,
                                              "next_sequence": st["next_sequence"]}, before=before)
                        continue
                    interval = params.get("interval_sec", 0.0)
                    payload = params.get("payload")
                    if (isinstance(interval, bool) or not isinstance(interval, (int, float))
                            or not math.isfinite(interval) or interval < 0 or interval > MAX_INTERVAL_SEC):
                        record_guard(action, "publish_interval_out_of_bounds",
                                     "interval_sec must be finite in 0..%s" % MAX_INTERVAL_SEC,
                                     kind="capability",
                                     details={"interval_sec": interval}, before=before)
                        continue
                    try:
                        # Matching unknown/incompatible is not a tool rejection:
                        # snapshots gate only command/identity integrity, while
                        # the close keeps an unestablished obligation incomplete.
                        prepare_delivery_windows(st["handle"], aid, sequences)
                        publish = backend.publish_batch(
                            st["handle"], sequences, float(interval), aid, payload=payload)
                    except TopologyUnsupported as exc:
                        interrupt_writer_windows(eid, "publish_batch_unsupported")
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        continue
                    except TopologyBackendTimeout as exc:
                        interrupt_writer_windows(eid, "publish_batch_timeout")
                        record_guard(action, "publish_batch_timeout", str(exc), kind="tool",
                                     details={"command_id": exc.command_id}, before=before)
                        continue
                    except TopologyBackendError as exc:
                        interrupt_writer_windows(eid, "publish_batch_tool_error")
                        record_guard(action, "publish_batch_error", type(exc).__name__,
                                     kind="tool", before=before)
                        continue
                    st["next_sequence"] = max(st["next_sequence"], start + count)
                    after = {**before, "next_sequence": st["next_sequence"],
                             "sent_sequences": publish.get("sent_sequences"),
                             "command_id": publish.get("command_id"),
                             "start_ns": publish.get("start_ns"), "end_ns": publish.get("end_ns")}
                    record_action(action, before, after, disposition="applied", satisfied=True,
                                  requires=[{"id": "nonoverlapping_writer_generation_sequence",
                                             "status": "satisfied"}],
                                  publish_receipt={
                                      "writer_id": eid, "generation": gen,
                                      "sequences": publish.get("sent_sequences"),
                                      "command_id": publish.get("command_id"),
                                      "controller_ack_count": publish.get("controller_ack_count"),
                                      "per_sample_ack": publish.get("per_sample_ack"),
                                      "payload_binding": publish.get("payload_binding"),
                                      "order_scope": "writer_generation",
                                      "cross_writer_order_required": False,
                              })
                    continue
                if op == "snapshot":
                    if eid is not None and not (state[eid]["alive"] and state[eid]["generation"] == gen):
                        record_guard(action, "snapshot_target_alive_guard",
                                     "snapshot requires a live current generation",
                                     before=before)
                        continue
                    targets = [eid] if eid is not None else None
                    snapshot = snapshot_handles(targets)
                    receipt = note_snapshot(aid, snapshot, params.get("label"))
                    result["precondition_receipts"].append({
                        "step_id": "action:" + aid, "receipt_kind": "precondition",
                        "action": deepcopy(action), "before": before,
                        "after": {"snapshot_command_id": snapshot.get("command_id"),
                                  "complete": snapshot.get("complete")},
                        "satisfied": snapshot.get("complete") is True,
                        "disposition": "effect_observed" if snapshot.get("complete") else "unknown",
                        "requires": [{"id": "runtime_snapshot_receipt", "kind": "observation",
                                      "status": "satisfied" if snapshot.get("complete") else "unknown"}],
                        "observation_receipt_ref": len(result["observation_receipts"]) - 1,
                        "real_ros": bool(backend.real_ros),
                        "execution_evidence": bool(backend.real_ros),
                    })
                    if not snapshot.get("complete"):
                        result["unknowns"].append({"action_id": aid,
                                                   "reason": "snapshot_incomplete"})
                    continue
                if op == "barrier":
                    condition = params.get("condition")
                    timeout = float(params.get("timeout_sec", 1.0))
                    explicit = params.get("endpoint_ids")
                    targets = explicit or list(state)
                    if condition in ("matched", "delivery") and explicit:
                        # Only model-declared barrier targets are preconditions; an
                        # implicit "all endpoints" scope stays an observation gap.
                        stale = [t for t in targets
                                 if not (state[t]["alive"] and state[t]["handle"] is not None)]
                        if stale:
                            record_guard(action, "barrier_target_alive_guard",
                                         "barrier requires live target endpoints",
                                         details={"stale_endpoints": stale}, before=before)
                            continue
                    deadline = time.monotonic() + timeout
                    previous = None
                    satisfied = False
                    detail = {}
                    attempts = 0
                    if condition == "quiescent" and not any(state[eid]["alive"] for eid in targets):
                        record_guard(action, "barrier_target_alive_guard",
                                     "quiescent barrier requires at least one live endpoint",
                                     before=before)
                        continue
                    if condition == "quiescent":
                        first = backend.snapshot({eid: state[eid]["handle"] for eid in targets
                                                  if state[eid]["alive"]})
                        previous = _snapshot_fingerprint(first)
                    while True:
                        attempts += 1
                        try:
                            satisfied, detail = _barrier_evaluate(
                                backend, condition, targets,
                                {eid: state[eid]["handle"] for eid in targets if state[eid]["alive"]},
                                events, previous=previous)
                        except TopologyUnsupported as exc:
                            record_guard(action, exc.code, str(exc), kind="capability",
                                         before=before)
                            satisfied = None
                            break
                        except TopologyBackendTimeout as exc:
                            record_guard(action, "barrier_snapshot_timeout", str(exc), kind="tool",
                                         before=before)
                            satisfied = None
                            break
                        except TopologyBackendError as exc:
                            record_guard(action, "barrier_backend_error", type(exc).__name__,
                                         kind="tool", before=before)
                            satisfied = None
                            break
                        if condition == "quiescent":
                            previous = detail.get("snapshot_fingerprint")
                        if satisfied or time.monotonic() >= deadline:
                            break
                        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
                    if satisfied is None:
                        continue
                    cancelled = not satisfied
                    receipt = {
                        "step_id": "action:" + aid, "receipt_kind": "observation",
                        "action": deepcopy(action), "before": before,
                        "after": {"satisfied": satisfied, "attempts": attempts,
                                  "condition": condition, "timeout_sec": timeout,
                                  "cancelled": cancelled, **detail},
                        "satisfied": satisfied,
                        "disposition": "effect_observed" if satisfied else "unknown",
                        "classification": None if satisfied else "observation_timeout_not_sut_violation",
                        "requires": [{"id": "runtime_barrier:" + str(condition),
                                      "kind": "observation",
                                      "status": "satisfied" if satisfied else "unknown"}],
                        "cancellation_cleanup": "no_blocking_worker_threads" if cancelled else "not_needed",
                        "real_ros": bool(backend.real_ros),
                        "execution_evidence": bool(backend.real_ros),
                        "candidate_eligible": False,
                    }
                    result["observation_receipts"].append(receipt)
                    emit("topology_barrier", **receipt)
                    if not satisfied:
                        result["unknowns"].append({"action_id": aid,
                                                   "reason": "barrier_timeout:" + str(condition)})
                    if detail.get("pairs"):
                        _upsert(result["pair_facts"], detail["pairs"], _PAIR_KEY)
                        _upsert(result["delivery_facts"],
                                _delivery_rows(detail["pairs"], events,
                                               {eid: st["handle"] for eid, st in state.items()
                                                if st["alive"]},
                                               real_ros=bool(backend.real_ros)),
                                _PAIR_KEY)
                    continue
                if op == "assert_liveliness":
                    st = state[eid]
                    spec = endpoint_specs[eid]
                    if not (st["alive"] and st["generation"] == gen and spec["role"] == "writer"):
                        record_guard(action, "writer_generation_alive_guard",
                                     "assert_liveliness requires a live writer generation", before=before)
                        continue
                    policy = (spec.get("qos") or {}).get("liveliness")
                    if policy != "manual_by_topic":
                        record_guard(action, "public_writer_manual_by_topic",
                                     "assert_liveliness requires manual_by_topic policy",
                                     details={"liveliness": policy}, before=before)
                        continue
                    try:
                        value = backend.assert_liveliness(st["handle"])
                    except TopologyUnsupported as exc:
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        continue
                    except TopologyBackendError as exc:
                        record_guard(action, "assert_liveliness_error", type(exc).__name__,
                                     kind="tool", before=before)
                        continue
                    record_action(action, before, {**before, "assert": value},
                                  disposition="effect_observed", satisfied=True,
                                  requires=[{"id": "public_writer_manual_by_topic",
                                             "status": "satisfied"}])
                    continue
                record_guard(action, "unknown_operation", "unsupported op " + str(op))
            finally:
                action_receipts = [r for r in result["precondition_receipts"]
                                   + result["observation_receipts"]
                                   if r.get("step_id") == "action:" + action_id]
                terminal = deepcopy(action_receipts[-1]) if action_receipts else {
                    "step_id": "action:" + action_id, "action": deepcopy(action),
                    "before": deepcopy(action_before), "after": None,
                    "requires": [], "satisfied": False, "disposition": "tool_error",
                    "reason": "action_terminated_without_receipt", "execution_evidence": False}
                # One actual terminal per command, including snapshot/barrier,
                # rejected guards and exceptions. Emit here, never at run end.
                emit("state_step_after", **terminal, fixture=not backend.real_ros)
                emit("topology_action_done", action_id=action_id, action_index=action_index,
                     action=deepcopy(action), receipt=terminal)

        check_topology_source("after_actions")
        # Final observation of the surviving generations before cleanup.
        final_snapshot_start_ref = len(events)
        final_snapshot = snapshot_handles()
        # Drain available worker stdout after the final snapshot commands. This
        # is observation only; an empty pump/ACK does not prove DDS completion.
        backend.pump(0.0)
        close_delivery_windows()
        if final_snapshot.get("complete"):
            note_snapshot("finalize", final_snapshot, "final_before_cleanup")
        else:
            result["unknowns"].append({"action_id": "finalize",
                                       "reason": "final_snapshot_incomplete"})
        boundary = state_boundary("after_program", final_snapshot,
                                  snapshot_start_ref=final_snapshot_start_ref)
        if not boundary["window_complete"]:
            # Public API actions can finish with no reader/no matched window.
            # That is not a completed delivery capture or an SUT failure.
            result["unknowns"].append({
                "action_id": "finalize", "reason": "delivery_window_incomplete",
                "delivery_status": ("no_obligation" if "no_current_pair_delivery_window" in boundary["limitations"]
                                    else "incomplete"),
                "raw_event_ref": boundary["event_index"],
                "delivery_window_refs": boundary["delivery_window_refs"],
            })
        result["status"] = "executed"
        result["runtime_status"] = "completed_with_unknown" if result["unknowns"] else "completed"
    except (TopologyBackendBlocked, IsolationError) as exc:
        result["reason"] = str(exc)
        result["runtime_status"] = "blocked"
        result["status"] = "blocked"
        emit("runtime_abort", reason=str(exc), error_type=type(exc).__name__)
    except (TopologyBackendError, OSError, RuntimeError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        result["reason"] = "aborted:" + type(exc).__name__ + ":" + str(exc)
        result["runtime_status"] = "aborted"
        result["status"] = "aborted"
        emit("runtime_abort", reason=result["reason"], error_type=type(exc).__name__)
    finally:
        if delivery_windows:
            close_delivery_windows("abnormal_program_termination")
        if result["status"] in ("aborted", "blocked"):
            state_boundary("aborted_program")
        cleanup_receipt = None
        try:
            cleanup_receipt = backend.close()
        except (OSError, RuntimeError, ValueError) as exc:
            cleanup_receipt = {"closed": False, "error": type(exc).__name__}
            result["status"] = "aborted"
            result["runtime_status"] = "aborted"
            result["reason"] = result["reason"] or "cleanup_failed:" + type(exc).__name__
        result["cleanup_receipt"] = cleanup_receipt
        emit("topology_cleanup", receipt=cleanup_receipt)
        if cleanup_receipt.get("closed") is not True:
            result.update(status="aborted", runtime_status="aborted",
                          reason=result["reason"] or "cleanup_incomplete")
            emit("runtime_abort", reason=result["reason"], phase="cleanup")

        if netem_ctx is not None and netem_ctx.get("active"):
            carrier = netem_ctx["carrier"]
            # Final counter snapshot is taken while the window is still
            # installed, then the window is genuinely removed.  A failed
            # restore keeps its residual evidence and blocks any hit claim.
            final_counter = netem_counter_snapshot("cleanup", "final_before_restore")
            try:
                end = carrier.restore()
            except Exception as exc:  # restore or residual verification failed
                failure = getattr(exc, "receipt", {}) or {}
                emit("topology_netem_restore_failed", error=type(exc).__name__,
                     receipt=failure, residual_qdisc=failure.get("residual_qdisc"),
                     residual_filter=failure.get("residual_filter"),
                     evidence_retained=True, restored=False)
                netem_ctx["active"] = False
                netem_ctx["restore"] = None
                netem_ctx["restore_failure"] = {"error": type(exc).__name__, "receipt": failure}
                result.update(status="aborted", runtime_status="aborted",
                              reason=result["reason"] or "topology_netem_restore_not_verified")
            else:
                netem_ctx["active"] = False
                netem_ctx["restore"] = end
                netem_ctx["final_counter"] = final_counter
                emit("topology_netem_restore", receipt=end,
                     only_noqueue_root=end.get("only_noqueue_root"),
                     residual_qdisc=end.get("residual_qdisc"),
                     residual_filter=end.get("residual_filter"),
                     counter_snapshot=end.get("counter_snapshot"),
                     final_counter_ref=(final_counter or {}).get("event_index"),
                     restored=end.get("restored") is True,
                     recovery_verified=False,
                     recovery_note="restore is not recovery; post-restore pair activity is separate")
                if end.get("restored") is not True:
                    result.update(status="aborted", runtime_status="aborted",
                                  reason=result["reason"] or "topology_netem_restore_not_verified")
        result["netem"] = ({
            "schema": "r2f.topology-netem-window/v1",
            "implementation": "isolated_netns_tc_netem_discovery_udp",
            "spec": deepcopy(netem_ctx["spec"]),
            "install_index": netem_ctx["install_index"],
            "counter_refs": [r["event_index"] for r in netem_ctx.get("counters", [])],
            "counter_failures": deepcopy(netem_ctx.get("counter_failures", [])),
            "restored": bool((netem_ctx.get("restore") or {}).get("restored") is True),
            "restore_failure": deepcopy(netem_ctx.get("restore_failure")),
            "s1_mutation_hit": False,
            "hit_semantics": "environment_network_effect_not_sut_defect",
            "responsibility_layer": "environment_harness",
            "drop_supported": False,
        } if netem_ctx is not None else None)
        try:
            check_topology_source("after_cleanup")
        except (TopologyBackendError, OSError, ValueError) as exc:
            failure_status = "aborted" if result["real_execution"] else "blocked"
            result.update(status=failure_status, runtime_status=failure_status, reason=str(exc))
            emit("runtime_abort", reason=str(exc))
        if lease is not None:
            survivors = [p.process.pid for p in getattr(backend, "processes", [])
                         if p.process is not None and p.process.poll() is None]
            if survivors or not cleanup_receipt.get("closed"):
                _RETAINED_EXECUTIONS.append((lease, backend))
                result.update(status="aborted", runtime_status="aborted",
                              reason="cleanup_incomplete_leases_retained")
                emit("isolation_locks_retained", survivors=survivors, locks=lease.receipts)
            else:
                lease.__exit__(None, None, None)
                emit("isolation_locks_released", after_full_cleanup=True)
        if result["status"] == "aborted":
            state_boundary("aborted_cleanup")
        ended_ns = _now()
        result["precondition_receipts"].append({
            "step_id": "cleanup", "receipt_kind": "precondition",
            "requires": [{"id": "full_process_tree_closed", "kind": "cleanup",
                          "status": "satisfied" if cleanup_receipt.get("closed") else "unknown"}],
            "before": None, "after": cleanup_receipt,
            "satisfied": bool(cleanup_receipt.get("closed")),
            "disposition": "observed" if cleanup_receipt.get("closed") else "unknown",
            "execution_evidence": bool(result["real_execution"]),
        })
        result["execution"] = {
            "started_monotonic_ns": started_ns,
            "ended_monotonic_ns": ended_ns,
            "duration_ns": ended_ns - started_ns,
            "backend": backend.backend_name,
            "ros_execution": bool(result["real_execution"]),
            "program_sha256": result["input_receipts"]["program_sha256"] if result["input_receipts"] else None,
            "processes_started": len(getattr(backend, "processes", []) or []),
        }
        workers = _execution_workers(events, raw_path)
        actual_workers = [w for w in workers if w["real_ros"]]
        actual_rmws = {w["rmw_identifier"] for w in actual_workers if w["rmw_identifier"] is not None}
        actual_domains = {w["domain_id"] for w in actual_workers if w["domain_id"] is not None}
        result["execution_metadata"] = {
            "schema": "r2f.topology-execution-metadata/v1",
            "backend": backend.backend_name,
            "real_ros": bool(result["real_execution"]),
            "source_identity": deepcopy(result["environment"].get("source")) if backend.real_ros else None,
            "source_manifest": deepcopy(source_before),
            "source_after": deepcopy(next((e.get("source") for e in reversed(events)
                                            if e.get("event") == "topology_source_checked"), None)),
            "domain_id": next(iter(actual_domains)) if len(actual_domains) == 1 and all(w["domain_id"] is not None for w in actual_workers) else None,
            "rmw": next(iter(actual_rmws)) if len(actual_rmws) == 1 and all(w["rmw_identifier"] is not None for w in actual_workers) else None,
            "actual_rmws": sorted(actual_rmws), "actual_domains": sorted(actual_domains),
            "workers": workers,
            "target_id": cfg.get("target_id"), "target_revision": cfg.get("target_revision"),
            "arm": cfg.get("arm"), "repeat": cfg.get("repeat"),
            "sealed_execution_context": {k: deepcopy(cfg.get(k)) for k in
                ("run_id", "target_id", "target_revision", "rmw", "domain_id", "arm", "repeat",
                 "input_identity_schema", "effective_input_sha256", "plan_hash")},
            "context_basis": "sealed_input_declaration_not_sut_observation",
            "environment_kind": result["environment_kind"],
            "environment_manifest": {"path": str(output_dir / "environment.json"),
                "sha256": hashlib.sha256((output_dir / "environment.json").read_bytes()).hexdigest()}
                if backend.real_ros and (output_dir / "environment.json").is_file() else None,
            "raw_path": str(raw_path), "raw_refs_hash_binding": "enclosing_result.raw_sha256",
            "started_monotonic_ns": started_ns, "finished_monotonic_ns": ended_ns,
            "real_execution": bool(result["real_execution"]), "runtime_status": result["runtime_status"],
            "declared_domain_id": cfg["domain_id"], "declared_rmw": cfg["rmw"],
            "run_id": cfg["run_id"],
            "input_program_sha256": result["input_receipts"]["program_sha256"] if result["input_receipts"] else None,
            "execution_evidence": bool(result["real_execution"]),
            "candidate_eligible": False,
        }
        if cfg.get("topology_route_binding"):
            from .topology_route import audit_topology_application
            result["scenario_mutation_receipt"] = deepcopy(result["mutation_receipt"])
            raw.flush()
            provisional = {"raw_path": str(raw_path),
                           "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest()}
            application = audit_topology_application(cfg["mutation_plan"], cfg["input_layer_receipts"], provisional)
            result["mutation_receipt"] = application["mutation_receipt"]
            emit("topology_application_audited", status=application["status"],
                 layers=[{"op_id": r["op_id"], "status": r["status"], "reasons": r["reasons"]}
                         for r in application["layers"]],
                 mutation_receipt=deepcopy(result["mutation_receipt"]),
                 receipt_reason=application.get("receipt_reason"), fixture=not backend.real_ros)
        emit("topology_execution_metadata", metadata=result["execution_metadata"], fixture=not backend.real_ros)
        (output_dir / "execution_metadata.json").write_text(
            json.dumps(result["execution_metadata"], sort_keys=True, indent=2) + "\n")
        emit("runtime_end", runtime_status=result["runtime_status"], status=result["status"],
             reason=result["reason"], real_execution=result["real_execution"],
             mutation_receipt={key: bool(result["mutation_receipt"].get(key)) for key in
                               ("planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed")},
             receipt_reason=result.get("scenario_mutation_receipt", {}).get("reason"),
             application_audit_status="requires_independent_raw_audit",
             fixture=not backend.real_ros)
        raw.flush()
        os.fsync(raw.fileno())
        raw.close()
        digest = hashlib.sha256(Path(raw_path).read_bytes()).hexdigest()
        result["raw_sha256"] = digest
        facts = {
            "schema": RESULT_SCHEMA,
            "backend": backend.backend_name,
            "real_execution": bool(result["real_execution"]),
            "backend_is_ros": bool(backend.real_ros),
            "offline_fake": not backend.real_ros,
            "endpoints": {
                eid: {
                    "declared_role": endpoint_specs[eid]["role"],
                    "worker_role": worker_roles.get(eid),
                    "current_generation": st["generation"],
                    "alive": st["alive"],
                    "history": deepcopy(st["history"]),
                } for eid, st in state.items()
            },
            "pair_facts": result["pair_facts"],
            "delivery_facts": result["delivery_facts"],
            "guard_rejections": [{"step_id": r["step_id"], "code": r["code"],
                                   "kind": r["kind"],
                                   "no_side_effect": True} for r in result["guard_receipts"]],
            "unknowns": result["unknowns"],
            "environment_declared": deepcopy(effective_program.get("environment", {})),
            "environment_observed": deepcopy(result["environment"]),
            "cross_writer_order_required": False,
            "per_sample_controller_ack": False,
            "execution_evidence": bool(result["real_execution"]),
            "candidate_eligible": False,
            "mature": False,
            "sealed": False,
        }
        result["facts"] = facts
    return result
