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
from .topology_message_types import SUPPORTED_MESSAGE_TYPES
from .endpoint_incarnation import endpoint_identity_at, validate_filter_identity_update
from .topology_environment_observation import TopologyEnvironmentObservation

from .liveliness_evidence import evaluate_liveliness, evaluate_peer_samples
from .topology_worker import (
    MAX_INTERVAL_SEC,
    _monotonic_clock_identity,
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
        **({"participant_control": deepcopy(program["participant_control"])}
           if "participant_control" in program else {}),
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



def _direct_peer_membership(token, peer, snapshot, events, *, before_index=None):
    """Validate the command-bound complete peer list against raw identities.

    No expected graph/QoS model participates. Unknown or aliased peer GIDs,
    incomplete capture and changed generations are not membership evidence.
    """
    match = snapshot["match_snapshot"]
    native_declared = (match.get("observation_mode") == "instrumentation"
        or token.get("observation_mode") == "instrumentation"
        or match.get("peer_proof_source") == "fastdds_native_stateful_peer_snapshot"
        or (isinstance(match.get("native_peer_observation"), dict)
            and match["native_peer_observation"].get("schema")
                == "r2f.fastdds-native-matched-peer-observation/v1")
        or any(row.get("event") == "native_peer_provider_selected"
               and all(token.get(k) is not None and row.get(k) == token[k]
                       for k in ("pid", "process_start_ticks", "endpoint_generation"))
               for row in (events or [])[:before_index]))
    if "peer_gids_complete" not in match and "peer_proof_source" not in match:
        return None, "native_peer_proof_missing" if native_declared else "per_peer_api_absent"
    if (match.get("peer_proof_source") not in {
                "dds_matched_endpoint_api", "fastdds_native_stateful_peer_snapshot"}
            or match.get("peer_gids_complete") is not True
            or not isinstance(match.get("matched_peer_gids"), list)):
        return None, "per_peer_api_incomplete"
    gids = match["matched_peer_gids"]
    def valid_gid(gid):
        return (isinstance(gid, str) and len(gid) == 32 and gid != "0" * 32
                and all(ch in "0123456789abcdef" for ch in gid))
    if (not all(valid_gid(gid) for gid in [token.get("endpoint_gid"), peer.get("endpoint_gid"), *gids])
            or len(set(gids)) != len(gids) or match["matched_count"] != len(gids)
            or token.get("endpoint_gid") in gids):
        return None, "per_peer_gid_alias_or_count_invalid"
    if events is None:
        return None, "per_peer_raw_chain_required"
    try:
        chain = _delivery_snapshot_chain(token, snapshot.get("command_id"), events,
                                         after_index=0, before_index=before_index)
    except TopologyBackendError as exc:
        return None, "per_peer_chain_invalid:" + str(exc)
    if (any(key not in chain[1] or chain[1][key] != value for key, value in match.items())
            or any(key not in chain[2] or chain[2][key] != value
                   for key, value in snapshot["graph_snapshot"].items())):
        return None, "per_peer_snapshot_not_raw_backed"
    request = chain[0]
    if match.get("peer_proof_source") == "fastdds_native_stateful_peer_snapshot":
        from r2f.instrumentation.fastdds_peer.receipt import validate_observation
        reason = validate_observation(chain[1].get("native_peer_observation"), chain[1], token, events, request)
        if reason is not None:
            return None, reason
    elif native_declared:
        return None, "native_peer_proof_source_mislabeled"
    limit = len(events) if before_index is None else before_index
    live = []
    for created in events[:request["event_index"]]:
        if created.get("event") != "endpoint_created":
            continue
        retired = [e for e in events[created["event_index"] + 1:limit]
                   if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed", "worker_exit"}
                   and e.get("logical_id") == created.get("logical_id")
                   and e.get("generation") == created.get("generation")]
        if not retired:
            try:
                live.append(endpoint_identity_at(events, created,
                            before_index=request["event_index"]))
            except ValueError as exc:
                return None, "per_peer_incarnation_invalid:" + str(exc)
    for field in ("logical_id", "endpoint_gid", "endpoint_generation"):
        values = [row.get(field) for row in live]
        if None in values or len(set(values)) != len(values):
            return None, "per_peer_live_identity_alias:" + field
    pids = [row.get("pid") for row in live]
    shared_process = len(set(pids)) != len(pids)
    if None in pids:
        return None, "per_peer_live_identity_alias:pid"
    if shared_process:
        from .participant_groups import _validate_shared_process
        for pid in set(pids):
            shared = [row for row in live if row["pid"] == pid]
            if len(shared) < 2:
                continue
            try:
                _validate_shared_process(shared, events, request_index=request["event_index"],
                                        before_index=chain[-1]["event_index"] + 1)
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                return None, "per_peer_live_identity_alias:pid:" + str(exc)
    opposite = "reader" if worker_role_kind(token.get("role")) == "writer" else "writer"
    inventory = [e for e in live if worker_role_kind(e.get("role")) == opposite
                 and e.get("topic") == token.get("topic")]
    known = {e.get("endpoint_gid") for e in inventory}
    for queried, role in ((token, worker_role_kind(token.get("role"))), (peer, opposite)):
        bound = [e for e in live if e.get("logical_id") == queried.get("logical_id")]
        identity_keys = ("generation", "endpoint_gid", "endpoint_generation", "pid",
                         "process_start_ticks", "topic", "message_type")
        if shared_process:
            from .participant_groups import LINK_KEYS
            identity_keys += ("pgid",) + LINK_KEYS
        if (len(bound) != 1 or worker_role_kind(bound[0].get("role")) != role
                or any(queried.get(key) is None or bound[0].get(key) != queried[key]
                       for key in identity_keys)):
            return None, "per_peer_query_identity_not_current_raw_generation"
    if (not set(gids) <= known
            or (peer.get("topic") == token.get("topic") and peer.get("endpoint_gid") not in known)):
        return None, "per_peer_scope_missing_raw_generation"
    for name, kind in (("publishers", "writer"), ("subscriptions", "reader")):
        rows = snapshot["graph_snapshot"][name]
        graph_gids = [row.get("endpoint_gid") for row in rows]
        raw_members = [e for e in live if worker_role_kind(e.get("role")) == kind
                       and e.get("topic") == token.get("topic")]
        raw_gids = {e.get("endpoint_gid") for e in raw_members}
        for row in rows:
            bound = [e for e in raw_members if e.get("endpoint_gid") == row.get("endpoint_gid")]
            if (len(bound) != 1 or not row.get("topic_type")
                    or row["topic_type"] != bound[0].get("message_type", bound[0].get("type_name"))):
                return None, "per_peer_graph_and_raw_type_differ"
        if (not all(valid_gid(gid) for gid in graph_gids)
                or len(graph_gids) != len(set(graph_gids)) or set(graph_gids) != raw_gids):
            return None, "per_peer_graph_and_raw_scope_differ"
    return peer["endpoint_gid"] in gids, None


def pair_graph_facts(writer_handle, reader_handle, writer_snapshot, reader_snapshot,
                     *, snapshot_command_id=None, real_ros=False, raw_events=None, before_index=None):
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
    if not _snapshot_complete(writer_snapshot) or not _snapshot_complete(reader_snapshot):
        fact["reasons"] = ["incomplete_snapshot"]
        return fact
    wmatch, rmatch = writer_snapshot["match_snapshot"], reader_snapshot["match_snapshot"]
    wgraph, rgraph = writer_snapshot["graph_snapshot"], reader_snapshot["graph_snapshot"]
    fact["peer_proof_sources"] = {"writer": wmatch.get("peer_proof_source"),
                                  "reader": rmatch.get("peer_proof_source")}
    if "fastdds_native_stateful_peer_snapshot" in fact["peer_proof_sources"].values():
        fact["observation_mode"] = "instrumentation"
        fact["source"] = "native_peer_snapshot_and_public_rclpy_count_graph"
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
    direct = [_direct_peer_membership(token, peer, snap, raw_events, before_index=before_index)
              for token, peer, snap in ((writer_handle, reader_handle, writer_snapshot),
                                        (reader_handle, writer_handle, reader_snapshot))]
    # Missing on BOTH ends preserves the qualified legacy 1W1R path. A present
    # but incomplete/contradictory native sample must never fall back to it.
    if any(reason != "per_peer_api_absent" for _, reason in direct):
        if any(value is None for value, _ in direct):
            fact["reasons"] = [reason or "peer_api_other_end_incomplete" for _, reason in direct
                               if reason is not None]
        elif all(value for value, _ in direct):
            if writer_in_reader and reader_in_writer:
                fact.update(status="matched", identity_basis="direct_bidirectional_matched_peer_api",
                            limitations=["snapshot_scope_only_not_continuous_match_or_delivery_ack"])
            else:
                fact["reasons"] = ["per_peer_api_and_graph_contradict"]
        elif not any(value for value, _ in direct):
            fact.update(status="unmatched", identity_basis="direct_bidirectional_matched_peer_api",
                        reasons=["peer_absent_from_both_complete_matched_lists"],
                        limitations=["snapshot_absence_only_not_persistent_nonmatch_or_disappearance"])
        else:
            fact["reasons"] = ["per_peer_api_directional_disagreement"]
        return fact
    if writer_handle.get("topic") != reader_handle.get("topic"):
        # Compatibility alone never supplies a direct match/nonmatch proof.
        # Keep legacy classification only when both direct API samples absent.
        fact["status"] = "incompatible"
        fact["reasons"] = ["topic_mismatch"]
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


def _has_publish_occurrence(pair, receive, events):
    """Admit the producer contract by declared raw writer scope, not its data.

    Hash/status/schema are inputs to verification, never selection predicates:
    a broken receipt must not downgrade into direct-GID/body-sequence guesses.
    Legacy captures and unsupported RMWs have no such FastDDS scope declaration.
    """
    return any(e.get("logical_id") == pair.get("writer")
               and e.get("generation") == pair.get("writer_generation")
               and e.get("rmw_identifier") == "rmw_fastrtps_cpp"
               and ((e.get("event") == "send" and "publish_occurrence_receipt" in e)
                    or e.get("event") in {"publish_call_begin", "rmw_publish_trace", "publish_call_end"})
               for e in events)


def _occurrence_pair_with_control(pair, receive, events):
    if all(pair.get(k) is not None for k in ("control", "control_generation", "control_gid")):
        return pair
    writers = [e for e in events if e.get("event") == "endpoint_created"
               and e.get("logical_id") == pair.get("writer")
               and e.get("generation") == pair.get("writer_generation")]
    if len(writers) != 1:
        return pair
    writer = writers[0]
    # Repeat-data callers have no receive boundary and retain their original
    # whole-capture control contract.
    receive_index = receive.get("event_index") if receive is not None else None
    receive_ns = receive.get("monotonic_ns") if receive is not None else None
    if receive is not None and (type(receive_index) is not int or type(receive_ns) is not int):
        return pair

    def strictly_after_sample(row, boundary):
        return (boundary is not None
                and type(row.get("event_index")) is int and row["event_index"] > boundary["event_index"]
                and type(row.get("monotonic_ns")) is int and row["monotonic_ns"] > boundary["monotonic_ns"])

    controls = []
    for created in events:
        if (created.get("event") != "endpoint_created"
                or worker_role_kind(created.get("role")) != "reader"
                or receive is not None and (
                    type(created.get("event_index")) is not int or created["event_index"] >= receive_index
                    or type(created.get("monotonic_ns")) is not int or created["monotonic_ns"] > receive_ns)
                or "initial_content_filter" not in created
                or created["initial_content_filter"] is not None
                or any(writer.get(k) is None or created.get(k) != writer[k]
                       for k in ("topic", "message_type", "run_id", "domain_id", "rmw_identifier"))):
            continue
        same = lambda row: (row.get("logical_id") == created.get("logical_id")
                            and row.get("generation") == created.get("generation"))
        # A later retirement/update cannot erase a historical unfiltered
        # reader. Both raw order and clock must prove it happened later; an
        # inconsistent or missing timestamp still prevents control selection.
        if any(same(row) and row.get("event") in {
                "content_filter_dynamic_update", "endpoint_identity_transition",
                "endpoint_destroyed", "topology_endpoint_destroyed"}
               and not strictly_after_sample(row, receive) for row in events):
            continue
        controls.append(created)
    if len(controls) != 1:
        return pair
    control = controls[0]
    selected = {**pair, "control": control["logical_id"],
                "control_generation": control["generation"], "control_gid": control["endpoint_gid"]}
    changes = [row for row in events
               if row.get("logical_id") == control.get("logical_id")
               and row.get("generation") == control.get("generation")
               and row.get("event") in {"content_filter_dynamic_update", "endpoint_identity_transition",
                                        "endpoint_destroyed", "topology_endpoint_destroyed"}]
    if receive is not None and changes:
        # Only the full native verifier can identify the actual control sample.
        # A change after the target but before that sample disqualifies control.
        from .publish_occurrence import publish_occurrence_witness
        witness, _ = publish_occurrence_witness(selected, receive, events)
        if witness is not None and any(
                not strictly_after_sample(row, events[witness["control_ref"]]) for row in changes):
            return pair
        # If authentication failed, let the caller retain the strict failure;
        # never substitute a different sample or a weaker attribution path.
    return selected


def payload_delivery_witness(pair, receive, events):
    """Unique raw payload correlation, NOT MessageInfo/direct publisher GID.

    The payload's self-declared writer name is never trusted alone. Both worker
    identities must bind to created endpoint generations, and the same run/hash
    must select exactly one publish-returned raw send in the entire capture.
    """
    if _has_publish_occurrence(pair, receive, events):
        from .publish_occurrence import publish_occurrence_witness
        return publish_occurrence_witness(_occurrence_pair_with_control(pair, receive, events), receive, events)
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
                   and e.get("logical_id") == eid and e.get("generation") == gen]
        if len(created) != 1 or any(event.get(k) is None or event.get(k) != created[0].get(k)
                                    for k in ("pid", "process_start_ticks", "endpoint_generation")):
            return None, "payload_worker_identity_not_bound"
        try:
            current = endpoint_identity_at(events, created[0], before_index=events.index(event) + 1)
        except ValueError as exc:
            return None, "payload_incarnation_invalid:" + str(exc)
        if (current.get("endpoint_gid") != gid
                or type(event.get("monotonic_ns")) is not int
                or event["monotonic_ns"] < current["identity_valid_after_ns"]):
            return None, "payload_worker_identity_not_current_incarnation"
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
        # The caller's explicit inventory remains necessary, never sufficient.
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
    # Reconstruct the send's prefix, never the final snapshot's writer count.
    # Excluding the send itself also preserves source order for equal clocks.
    prefix = events[:events.index(sends[0])]
    raw_count, raw_reason = _raw_active_writer_inventory(prefix, writer_id, send_ns)
    if any(e.get("event") == "endpoint_created" for e in prefix):
        if raw_count is None:
            return False, raw_reason
        if raw_count != 1:
            return False, "raw_writer_inventory_contradicts_single_writer"
        return True, None
    # Retain the diagnostic legacy format without raw lifecycle observations.
    if type(active_writers_on_topic) is not int or active_writers_on_topic != 1:
        return False, "active_writer_inventory_not_exactly_one"
    return True, raw_reason


def _delivery_lifetimes(events):
    """Index physical lifetimes once for a captured source prefix."""
    lifetimes = {}
    for index, event in enumerate(events):
        key = (event.get("logical_id"), event.get("generation"))
        if event.get("event") == "endpoint_created":
            lifetimes[key] = {"start": index, "created": event, "end": None, "retired": None}
        elif event.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"} and key in lifetimes:
            life = lifetimes[key]
            if life["end"] is None:
                life.update(end=index, retired=event)
    return lifetimes


def _in_delivery_lifetime(event, index, life):
    if life is None:
        return True  # Legacy diagnostic rows may lack a raw lifecycle inventory.
    if index <= life["start"] or life["end"] is not None and index >= life["end"]:
        return False
    clock = event.get("monotonic_ns")
    if type(clock) is int:
        start = life["created"].get("monotonic_ns")
        end = (life["retired"] or {}).get("monotonic_ns")
        if type(start) is int and clock < start or type(end) is int and clock > end:
            return False
    return True


def pair_delivery_fact(pair, events, *, active_writers_on_topic=None, real_ros=False,
                       _lifetimes=None):
    """Attribute deliveries per writer generation; never fill from a total count."""
    writer_gid = pair.get("writer_gid")
    reader_gid = pair.get("reader_gid")
    writer_id = pair.get("writer")
    reader_id = pair.get("reader")
    # A publish only counts for a reader that already existed when it happened.
    # Volatile history is not replayed to a later generation, so pre-creation
    # sequences must not be reported as "missing" for that pair.
    reader_created = pair.get("reader_created_monotonic_ns")
    lifetimes = _delivery_lifetimes(events) if _lifetimes is None else _lifetimes
    writer_life = lifetimes.get((writer_id, pair.get("writer_generation")))
    reader_life = lifetimes.get((reader_id, pair.get("reader_generation")))
    sends = [e for index, e in enumerate(events) if e.get("event") == "send"
             and e.get("logical_id") == writer_id
             and e.get("generation") == pair.get("writer_generation")
             and e.get("writer_gid") in (None, writer_gid)
             and _in_delivery_lifetime(e, index, writer_life)
             and _in_delivery_lifetime(e, index, reader_life)
             and (reader_life is not None or reader_created is None
                  or (isinstance(e.get("monotonic_ns"), int)
                      and e["monotonic_ns"] >= reader_created))]
    expected = [e.get("send_sequence") for e in sends]
    direct, correlated, single, unknown = [], [], [], []
    payload_witnesses, attribution_reasons, single_limitations = [], [], []
    effective_sequences = {}
    for event_index, event in enumerate(events):
        if event.get("event") != "receive":
            continue
        # A valid receipt can arrive after writer retirement. Only the reader
        # lifetime bounds receiving; the send obligation was fenced separately.
        if not _in_delivery_lifetime(event, event_index, reader_life):
            continue
        if event.get("logical_id") != reader_id:
            continue
        if event.get("generation") != pair.get("reader_generation"):
            continue
        if event.get("publisher_gid") is not None and event["publisher_gid"] != writer_gid:
            continue  # A direct sample of another writer is outside this pair.
        sequence = event.get("send_sequence")
        occurrence_witness = None
        if _has_publish_occurrence(pair, event, events):
            occurrence_witness, occurrence_reason = payload_delivery_witness(pair, event, events)
            if occurrence_witness is None:
                unknown.append(event)
                attribution_reasons.append("publish_occurrence_unresolved:" + str(occurrence_reason))
                continue
            sequence = events[occurrence_witness["send_ref"]].get("send_sequence")
            effective_sequences[id(event)] = sequence
        if sequence not in expected:
            continue
        if event.get("reader_gid") not in (None, reader_gid):
            continue
        if any(e.get("event") == "endpoint_identity_transition"
               and e.get("logical_id") == reader_id
               and e.get("generation") == pair.get("reader_generation") for e in events):
            origins = [e for e in events if e.get("event") == "endpoint_created"
                       and e.get("logical_id") == reader_id
                       and e.get("generation") == pair.get("reader_generation")]
            try:
                if len(origins) != 1:
                    raise ValueError("reader_creation_missing_or_duplicate")
                current = endpoint_identity_at(events, origins[0], before_index=events.index(event) + 1)
                if (current.get("endpoint_gid") != reader_gid or event.get("reader_gid") != reader_gid
                        or type(event.get("monotonic_ns")) is not int
                        or event["monotonic_ns"] < current["identity_valid_after_ns"]):
                    raise ValueError("reader_not_current_incarnation")
            except ValueError as exc:
                unknown.append(event)
                attribution_reasons.append("reader_incarnation_invalid:" + str(exc))
                continue
        publisher_gid = event.get("publisher_gid")
        if publisher_gid is not None:
            if publisher_gid == writer_gid:
                direct.append(event)
                if occurrence_witness is not None:
                    payload_witnesses.append(occurrence_witness)
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
    # Attribution categories are not arrival order. Retain the capture order
    # (including duplicates), never sort samples by their sequence number.
    attributed = {id(e) for e in direct + correlated + single}
    rows = [e for e in events if id(e) in attributed]
    received = [effective_sequences.get(id(event), event.get("send_sequence")) for event in rows]
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
        "send_order_refs": [e.get("event_index") for e in sends],
        "receive_order_refs": [e.get("event_index") for e in rows],
        "ordering_scope": "raw_capture_only_not_closed_window_verdict",
        "in_order": (None if duplicates or unknown or len(expected) != len(set(expected))
                     else all(expected.index(a) < expected.index(b)
                              for a, b in zip(received, received[1:]))),
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


def _sentinel_delivery(pairs, handles, events, publications):
    """Exact raw send/receive witnesses only; no all-samples obligation."""
    witnesses, gaps = [], []
    for pub in publications:
        aid, writer, gen = pub["action_id"], pub["endpoint_id"], pub["generation"]
        if (writer not in handles or handles[writer].get("generation") != gen
                or pub["op"] != "publish_batch" or pub["params"].get("count") != 1
                or "repeat_data_from" in pub["params"]):
            raise TopologyUnsupported("sentinel_current_generation_or_publish_invalid", aid)
        sends = [e for e in events if e.get("event") == "send" and e.get("phase") == aid]
        if (len(sends) != 1 or sends[0].get("logical_id") != writer
                or sends[0].get("generation") != gen
                or sends[0].get("send_sequence") != pub["params"]["start_sequence"]):
            gaps.append({"publish_action_id": aid, "reason": "sentinel_exact_send_missing_or_ambiguous"})
            continue
        readers = [h for h in handles.values() if h.get("role") == "reader"]
        for reader in readers:
            pair = next((p for p in pairs if p.get("writer") == writer
                         and p.get("reader") == reader["logical_id"]), None)
            found, reason = None, "sentinel_receive_identity_missing"
            if pair is not None and pair.get("writer_gid") and pair.get("reader_gid"):
                for receive in events:
                    if receive.get("event") != "receive":
                        continue
                    witness, witness_reason = payload_delivery_witness(pair, receive, events)
                    if witness is None:
                        reason = witness_reason or "sentinel_receive_identity_missing"
                        continue
                    if witness.get("send_ref") != sends[0].get("event_index"):
                        reason = "sentinel_receive_for_other_send"
                        continue
                    reason = "sentinel_receive_gid_or_chronology_mismatch"
                    sent = sends[0]
                    clock = sent.get("monotonic_clock_identity")
                    begin, returned, emitted, received = (sent.get("publish_begin_ns"),
                        sent.get("publish_return_ns"), sent.get("monotonic_ns"), receive.get("monotonic_ns"))
                    # Independent worker stdout is merged by capture order, not
                    # DDS causality. A callback may precede publish return; bind
                    # it to the API begin in the same proven monotonic clock.
                    if (receive.get("publisher_gid") in (None, pair.get("writer_gid"))
                            and isinstance(clock, dict) and bool(clock)
                            and receive.get("monotonic_clock_identity") == clock
                            and all(type(v) is int for v in (begin, returned, emitted, received))
                            and 0 < begin <= returned <= emitted and begin <= received):
                        found = {"publish_action_id": aid, **witness,
                                 "chronology_basis": "same_clock_publish_begin_before_receive"}
                        break
            if found:
                witnesses.append(found)
            else:
                gaps.append({"publish_action_id": aid, "reader": reader["logical_id"], "reason": reason})
        if not readers:
            gaps.append({"publish_action_id": aid, "reason": "sentinel_reader_scope_missing"})
    return bool(publications) and not gaps, {"sentinel_witnesses": witnesses, "sentinel_gaps": gaps}


def _barrier_evaluate(backend, condition, endpoint_ids, handles, events,
                      previous=None, publications=None):
    if condition == "all_created":
        targets = list(endpoint_ids) if endpoint_ids else list(handles)
        missing = [eid for eid in targets if eid not in handles]
        rows = [handles[eid] for eid in targets if eid in handles]
        alive = {h["logical_id"]: bool(h.get("alive")) for h in rows}
        ok = (not missing and bool(rows)
              and all(h.get("alive") and h.get("endpoint_gid") and h.get("pid") for h in rows))
        return ok, {"alive": alive, "missing_endpoints": missing}
    if condition in ("matched", "delivery", "delivery_sentinel", "quiescent") and not handles:
        # A barrier over zero live endpoints cannot be satisfied by an empty
        # snapshot alone; it stays an observation gap.
        return False, {"reason": "no_live_endpoints_for_barrier"}
    if condition in ("matched", "delivery", "delivery_sentinel"):
        snapshot = backend.snapshot(handles)
        real_ros = bool(getattr(backend, "real_ros", False))
        pairs = _pair_rows(handles, snapshot, events, real_ros=real_ros)
        if condition == "delivery_sentinel":
            return _sentinel_delivery(pairs, handles, events, publications or [])
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
    try:
        current = endpoint_identity_at(events, created[0], before_index=request["event_index"])
    except ValueError as exc:
        raise TopologyBackendError("delivery_snapshot_incarnation_invalid:" + str(exc)) from exc
    if (current.get("endpoint_gid") != token.get("endpoint_gid")
            or request["monotonic_ns"] < current["identity_valid_after_ns"]
            or request["event_index"] <= current["identity_valid_after_ref"]
            or (("endpoint_gid" in request or current["incarnation_ordinal"] > 0)
                and request.get("endpoint_gid") != current.get("endpoint_gid"))):
        raise TopologyBackendError("delivery_snapshot_current_identity_unproven")
    return chain



def _delivery_no_sends_basis(writer, reader, program, events, chains):
    """Conservative generation-wide applicability, never an empty DDS window.

    The send phase seal is a raw inventory commitment after all action terminals.
    Reject all reuse/history or publication attempts, not merely final config.
    Final command-bound observations remain independent of delivery applicability.
    """
    bound = []
    for handle in (writer, reader):
        creations = [e for e in events if e.get("event") == "endpoint_created"
                     and all(e.get(k) == handle.get(k) for k in
                             ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation", "endpoint_gid"))]
        if len(creations) != 1:
            return None
        bound.append(creations[0])
    writer, reader = bound
    seals = [e for e in events if e.get("event") == "topology_send_inventory_end"]
    if len(seals) != 1:
        return None
    seal = seals[0]
    end = seal["event_index"]
    prefix = events[:end]
    digest = hashlib.sha256(json.dumps(prefix, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if (seal.get("schema") != "r2f.topology-send-inventory/v1"
            or seal.get("inventory_count") != end or seal.get("inventory_sha256") != digest
            or any(e.get("event_index") != i or e.get("run_id") != seal.get("run_id")
                   or type(e.get("monotonic_ns")) is not int for i, e in enumerate(events))):
        return None
    actions = program["actions"]
    begins = [e for e in prefix if e.get("event") == "topology_action_begin"]
    done = [e for e in prefix if e.get("event") == "topology_action_done"]
    if len(begins) != len(actions) or len(done) != len(actions):
        return None
    for i, (action, begin, finish) in enumerate(zip(actions, begins, done)):
        if (begin.get("action") != action or finish.get("action") != action
                or begin.get("action_index") != i or finish.get("action_index") != i
                or not begin["event_index"] < finish["event_index"] < end
                or (i and done[i-1]["event_index"] >= begin["event_index"])
                or finish.get("receipt", {}).get("satisfied") is not True):
            return None
    if seal.get("action_done_refs") != [e["event_index"] for e in done]:
        return None
    fields = ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")
    for token, role in ((writer, "writer"), (reader, "reader")):
        history = [e for e in events if e.get("logical_id") == token.get("logical_id")
                   and e.get("event") in {"endpoint_created", "endpoint_destroyed", "topology_endpoint_destroyed", "topology_generation_retired"}]
        if (len(history) != 1 or history[0].get("event") != "endpoint_created"
                or any(token.get(k) is None or history[0].get(k) != token[k] for k in fields)
                or token.get("generation") != 1 or worker_role_kind(token.get("role")) != role
                or token.get("run_id") != seal.get("run_id") or token["event_index"] >= end):
            return None
        try:
            chain = chains[token["logical_id"]]
            actual = _delivery_snapshot_chain(token, chain[0].get("command_id"), events, after_index=end+1)
            if actual != chain or chain[0]["event_index"] <= end:
                return None
        except (TopologyBackendError, KeyError, IndexError):
            return None
    # Any writer attempt or opened window, including before this reader, blocks.
    for event in events:
        kind = event.get("event")
        if kind in {"runtime_abort", "worker_error", "worker_command_error", "topology_backend_blocked"}:
            return None
        if kind == "send":
            known = [e for e in events if e.get("event") == "endpoint_created"
                     and e.get("logical_id") == event.get("logical_id")
                     and e.get("generation") == event.get("generation")]
            if len(known) != 1 or any(event.get(k) != known[0].get(k) for k in fields if k != "endpoint_gid"):
                return None
            if event.get("logical_id") == writer["logical_id"] or event.get("pid") == writer["pid"]:
                return None
        if ((event.get("writer") == writer["logical_id"] and kind.startswith("topology_delivery_window"))
                or ((event.get("action") if isinstance(event.get("action"), dict) else {}).get("endpoint_id") == writer["logical_id"]
                    and (event.get("action") if isinstance(event.get("action"), dict) else {}).get("op") == "publish_batch")
                or (event.get("logical_id") == writer["logical_id"]
                    and (kind in {"publish", "publish_batch", "publish_batch_done"}
                         or event.get("command") in {"publish", "publish_batch"}))):
            return None
    return {"schema": "r2f.topology-delivery-no-sends/v1", "closure_status": "not_applicable_no_sends",
            "scope": "entire_writer_generation_through_send_phase_end", "inventory_end_ref": end,
            "inventory_count": end, "inventory_sha256": digest,
            "writer_identity": {k: writer[k] for k in fields}, "reader_identity": {k: reader[k] for k in fields},
            "writer_creation_ref": writer["event_index"], "reader_creation_ref": reader["event_index"],
            "snapshot_refs": {t["logical_id"]: [e["event_index"] for e in chains[t["logical_id"]]]
                              for t in (writer, reader)},
            "expected_sequences": [], "received_sequences": [], "missing_sequences": [],
            "candidate_eligible": False, "mutation_hit": False, "semantic_effect": False}


def _delivery_recreated_no_sends_basis(writer, reader, program, events, chains):
    """v2 current-generation absence, with independently retained old obligations.

    Engineering design under main dispatch: v1's generation-one fence remains.
    This is an inventory claim before cleanup; the consumer must also verify
    actual same-generation exits and runtime-end after the final boundary.
    """
    fields = ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")
    try:
        if type(writer.get("generation")) is not int or writer["generation"] < 2:
            return None
        seals = [e for e in events if e.get("event") == "topology_send_inventory_end"]
        if len(seals) != 1:
            return None
        seal = seals[0]; end = seal["event_index"]; prefix = events[:end]
        digest = hashlib.sha256(json.dumps(prefix, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if (seal.get("schema") != "r2f.topology-send-inventory/v1" or seal.get("inventory_count") != end
                or seal.get("inventory_sha256") != digest
                or any(e.get("event_index") != i or e.get("run_id") != seal["run_id"]
                       or type(e.get("monotonic_ns")) is not int for i, e in enumerate(events))):
            return None
        begins = [e for e in prefix if e.get("event") == "topology_action_begin"]
        done = [e for e in prefix if e.get("event") == "topology_action_done"]
        if len(begins) != len(program["actions"]) or len(done) != len(begins):
            return None
        spans = []
        for i, (action, begin, finish) in enumerate(zip(program["actions"], begins, done)):
            if (begin.get("action") != action or finish.get("action") != action
                    or begin.get("action_index") != i or finish.get("action_index") != i
                    or not begin["event_index"] < finish["event_index"] < end
                    or begin["monotonic_ns"] > finish["monotonic_ns"]
                    or (i and done[i-1]["event_index"] >= begin["event_index"])
                    or finish.get("receipt", {}).get("satisfied") is not True):
                return None
            spans.append((action, begin, finish))
        if seal.get("action_done_refs") != [e["event_index"] for e in done]:
            return None
        if any(e.get("event") in {"endpoint_created", "endpoint_destroyed", "topology_endpoint_destroyed"}
               and e.get("logical_id") in {writer["logical_id"], reader["logical_id"]}
               for e in events[end+1:]):
            return None
        tokens, histories = [], {}
        for handle, role in ((writer, "writer"), (reader, "reader")):
            history = [e for e in prefix if e.get("logical_id") == handle["logical_id"]
                       and e.get("event") in {"endpoint_created", "endpoint_destroyed", "topology_endpoint_destroyed"}]
            live = None; generation = 0; gids = set(); workers = set(); nonces = set()
            previous_time = -1
            for e in history:
                if e["monotonic_ns"] < previous_time:
                    return None
                previous_time = e["monotonic_ns"]
                if e["event"] == "endpoint_created":
                    if (live is not None or type(e.get("generation")) is not int or e["generation"] != generation+1
                            or any(e.get(k) is None for k in fields)
                            or worker_role_kind(e.get("role")) != role or not e.get("topic") or not e.get("message_type")
                            or e["endpoint_gid"] in gids or (e["pid"], e["process_start_ticks"]) in workers
                            or e["endpoint_generation"] in nonces):
                        return None
                    valid_action = lambda a, op=("create" if generation == 0 else "recreate"), gen=generation+1: a.get("op") == op and a.get("generation") == gen
                    gids.add(e["endpoint_gid"]); workers.add((e["pid"], e["process_start_ticks"])); nonces.add(e["endpoint_generation"])
                    live = e; generation += 1
                else:
                    if live is None or e.get("cleanup_status") != "closed" or any(e.get(k) != live[k] for k in fields):
                        return None
                    exits = [x for x in prefix if x.get("event") == "topology_worker_exit"
                             and x.get("logical_id") == live["logical_id"] and x.get("generation") == generation]
                    if (len(exits) != 1 or exits[0].get("returncode") != 0
                            or any(exits[0].get(k) != live[k] for k in fields)
                            or len([a for a,b,f in spans if a.get("endpoint_id") == live["logical_id"]
                                    and (a.get("op"), a.get("generation")) in {("destroy", generation), ("recreate", generation+1)}
                                    and b["event_index"] < exits[0]["event_index"] < f["event_index"]
                                    and b["monotonic_ns"] <= exits[0]["monotonic_ns"] <= f["monotonic_ns"]]) != 1):
                        return None
                    valid_action = lambda a: (a.get("op"), a.get("generation")) in {("destroy", generation), ("recreate", generation+1)}
                    live = None
                if len([a for a,b,f in spans if a.get("endpoint_id") == handle["logical_id"] and valid_action(a)
                        and b["event_index"] < e["event_index"] < f["event_index"]
                        and b["monotonic_ns"] <= e["monotonic_ns"] <= f["monotonic_ns"]]) != 1:
                    return None
            if live is None or any(live[k] != handle.get(k) for k in fields):
                return None
            chain = chains[handle["logical_id"]]
            if _delivery_snapshot_chain(live, chain[0].get("command_id"), events, after_index=end+1) != chain:
                return None
            histories[handle["logical_id"]] = [e["event_index"] for e in history]
            tokens.append(live)
        w, r = tokens
        creations = [e for e in events if e.get("event") == "endpoint_created"]
        sends = [e for e in events if e.get("event") == "send"]
        for send in sends:
            known = [e for e in creations if e.get("logical_id") == send.get("logical_id") and e.get("generation") == send.get("generation")]
            if (len(known) != 1 or any(send.get(k) != known[0].get(k) for k in fields if k != "endpoint_gid")
                    or send.get("writer_gid") != known[0]["endpoint_gid"] or send.get("publish_returned") is not True
                    or send["event_index"] >= end or not isinstance(send.get("payload_sha256"), str)
                    or len([s for s in sends if s.get("payload_sha256") == send["payload_sha256"]]) != 1):
                return None
            if send.get("logical_id") == w["logical_id"] and (send.get("generation") >= w["generation"] or send["event_index"] >= w["event_index"]):
                return None
        for receive in (e for e in events if e.get("event") == "receive"):
            matches = [x for x in sends if x.get("payload_sha256") == receive.get("payload_sha256")
                       and x.get("logical_id") == receive.get("payload_writer_id")
                       and x.get("generation") == receive.get("payload_writer_generation")
                       and x.get("send_sequence") == receive.get("send_sequence")
                       and x.get("run_id") == receive.get("payload_run_id")]
            if len(matches) != 1:
                return None
        for e in events:
            if e.get("event") in {"runtime_abort", "worker_error", "worker_command_error", "topology_backend_blocked"}:
                return None
            a = e.get("action") if isinstance(e.get("action"), dict) else {}
            if (a.get("endpoint_id") == w["logical_id"] and a.get("op") == "publish_batch"
                    and (a.get("generation") == w["generation"] or e["event_index"] > w["event_index"])):
                return None
            if e.get("logical_id") == w["logical_id"] and (e.get("command") in {"publish", "publish_batch"} or e.get("event") in {"publish", "publish_batch", "publish_batch_done"}):
                if type(e.get("generation")) is not int or e["generation"] >= w["generation"] or e["event_index"] > w["event_index"]:
                    return None
            if e.get("writer") == w["logical_id"] and e.get("event", "").startswith("topology_delivery_window"):
                if type(e.get("writer_generation")) is not int or e["writer_generation"] >= w["generation"]:
                    return None
        closed = []
        opens = [e for e in events if e.get("event") == "topology_delivery_window_open"]
        for opened in opens:
            closes = [e for e in events if e.get("event") == "topology_delivery_window_closed" and e.get("open_ref") == opened["event_index"]]
            if len(closes) != 1:
                return None
            close = closes[0]
            evidence = _delivery_window_evidence(opened, events, before_index=close["event_index"])
            if (not close.get("window_complete") or not evidence["window_complete"] or evidence["closure_status"] not in {"closed", "no_obligation"}
                    or any(close.get(k) != v for k,v in evidence.items())):
                return None
            closed.append((opened, close, evidence))
        # Every older writer send still needs a closed, same-reader-generation
        # obligation (or raw NONE window) for each reader alive at that send.
        for send in (e for e in sends if e.get("logical_id") == w["logical_id"]):
            active = {}
            for e in events[:send["event_index"]]:
                if e.get("event") == "endpoint_created" and worker_role_kind(e.get("role")) == "reader": active[e["logical_id"]] = e["generation"]
                elif e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}: active.pop(e.get("logical_id"), None)
            for eid, gen in active.items():
                if not any(o["writer"] == w["logical_id"] and o["writer_generation"] == send["generation"]
                           and o["reader"] == eid and o["reader_generation"] == gen
                           and o["event_index"] < send["event_index"] < c["event_index"]
                           and (send["event_index"] in proof["send_refs"] or proof["closure_status"] == "no_obligation") for o,c,proof in closed):
                    return None
        return {"schema": "r2f.topology-delivery-no-sends/v2", "closure_status": "not_applicable_no_sends",
                "scope": "current_writer_generation_after_verified_retirement", "inventory_end_ref": end,
                "inventory_count": end, "inventory_sha256": digest,
                "writer_identity": {k:w[k] for k in fields}, "reader_identity": {k:r[k] for k in fields},
                "writer_creation_ref": w["event_index"], "reader_creation_ref": r["event_index"],
                "generation_history_refs": histories, "prior_window_close_refs": [c["event_index"] for _,c,_ in closed],
                "snapshot_refs": {t["logical_id"]:[e["event_index"] for e in chains[t["logical_id"]]] for t in tokens},
                "expected_sequences": [], "received_sequences": [], "missing_sequences": [],
                "candidate_eligible": False, "mutation_hit": False, "semantic_effect": False}
    except (KeyError, TypeError, ValueError, IndexError, TopologyBackendError):
        return None


def _delivery_no_obligation_basis(opened, events):
    """Fresh observed identity/type or complete direct NONE, never QoS intent.

    This applies only to the capture beginning at this open. It does not claim
    a persistent absence/disappearance, and cannot forgive an older obligation.
    """
    start = opened.get("event_index")
    if type(start) is not int or not 0 <= start < len(events):
        return None
    tokens, snapshots = {}, {}
    try:
        for role in ("writer", "reader"):
            candidates = [e for e in events[:start] if e.get("event") == "endpoint_created"
                          and e.get("logical_id") == opened.get(role)
                          and e.get("generation") == opened.get(role + "_generation")]
            if len(candidates) != 1:
                return None
            token = tokens[role] = endpoint_identity_at(events, candidates[0], before_index=start)
            if (worker_role_kind(token.get("role")) != role or token.get("run_id") != opened.get("run_id")
                    or token.get("endpoint_gid") != opened.get(role + "_gid")):
                return None
            if any(e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed", "worker_exit"}
                   and e.get("logical_id") == token["logical_id"] and e.get("generation") == token["generation"]
                   for e in events[token["event_index"]+1:start]):
                return None
            refs = opened.get("snapshot_refs", {}).get(token["logical_id"], [])
            if not isinstance(refs, list) or len(refs) != 4 or any(type(i) is not int or not 0 <= i < start for i in refs):
                return None
            chain = _delivery_snapshot_chain(token, events[refs[0]].get("command_id"), events,
                after_index=opened["snapshot_start_ref"], before_index=start)
            if (refs != [e["event_index"] for e in chain]
                    or not token["monotonic_ns"] <= chain[0]["monotonic_ns"] <= chain[-1]["monotonic_ns"] <= opened["monotonic_ns"]):
                return None
            snapshots[role] = {"complete": True, "command_id": chain[1]["command_id"],
                               "match_snapshot": chain[1], "graph_snapshot": chain[2]}
            own = [row for row in chain[2]["publishers" if role == "writer" else "subscriptions"]
                   if row.get("endpoint_gid") == token["endpoint_gid"]]
            if (len(own) != 1 or not isinstance(token.get("message_type"), str) or not token.get("topic")
                    or own[0].get("topic_type") != token["message_type"]):
                return None
        w, r = tokens["writer"], tokens["reader"]
        if w["endpoint_gid"] == r["endpoint_gid"]:
            return None
        reason = None
        if w["topic"] != r["topic"]:
            reason = "actual_endpoint_topic_mismatch"
        elif w["message_type"] != r["message_type"]:
            reason = "actual_endpoint_type_mismatch"
        else:
            pair = pair_graph_facts(w, r, snapshots["writer"], snapshots["reader"],
                                    raw_events=events, before_index=start)
            if (pair["status"] == "unmatched"
                    and pair.get("identity_basis") == "direct_bidirectional_matched_peer_api"):
                reason = "both_complete_direct_peer_lists_exclude_pair"
        if reason is None:
            return None
        return {"reason": reason, "snapshot_refs": opened["snapshot_refs"],
                "writer_gid": w["endpoint_gid"], "reader_gid": r["endpoint_gid"],
                "writer_topic": w["topic"], "reader_topic": r["topic"],
                "writer_type": w["message_type"], "reader_type": r["message_type"],
                "scope": "open_snapshot_only_not_persistent_nonmatch", "obligation": "not_applicable"}
    except (TopologyBackendError, ValueError, KeyError, TypeError, IndexError):
        return None


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
        try:
            token = tokens[role] = endpoint_identity_at(events, candidates[0], before_index=start)
        except ValueError as exc:
            reasons.append("window_incarnation_invalid:" + role + ":" + str(exc))
            continue
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
    if any(e.get("event") == "endpoint_identity_transition"
           and any(e.get("logical_id") == t["logical_id"] and e.get("generation") == t["generation"]
                   for t in (w, r)) for e in rows):
        reasons.append("endpoint_incarnation_changed_inside_window")
    no_obligation = _delivery_no_obligation_basis(opened, prefix)
    claims = [e for e in rows if e.get("event") == "topology_delivery_no_obligation"
              and e.get("open_ref") == start]
    if claims:
        claim = claims[0]
        valid = (len(claims) == 1 and no_obligation is not None and not reasons
                 and all(claim.get(key) == value for key, value in no_obligation.items())
                 and all(claim.get(key) == opened.get(key) for key in _PAIR_KEY)
                 and type(claim.get("monotonic_ns")) is int
                 and opened["monotonic_ns"] <= claim["monotonic_ns"] <= end_ns
                 and not any(e.get("event") == "send" and e.get("logical_id") == w["logical_id"]
                             and e["event_index"] < claim["event_index"] for e in rows)
                 and not any(e.get("event") == "topology_delivery_window_interrupted"
                             and e.get("open_ref") == start for e in rows))
        if valid:
            return {"window_complete": True, "closure_status": "no_obligation",
                    "no_obligation_ref": claim["event_index"], "no_obligation_reason": no_obligation["reason"],
                    "reasons": [], "expected_sequences": [], "received_sequences": [],
                    "missing_sequences": [], "gap_evidence": [], "send_refs": [], "receive_refs": [],
                    "attribution": "not_applicable"}
        reasons.append("no_obligation_receipt_not_raw_supported")
    elif no_obligation is not None:
        reasons.append("no_obligation_receipt_missing")
    direct_peer_match = False
    if len(chains) == 2:
        snaps = {role: {"complete": True, "command_id": chain[1]["command_id"],
                        "match_snapshot": chain[1], "graph_snapshot": chain[2]}
                 for role, chain in chains.items()}
        pair = pair_graph_facts(w, r, snaps["writer"], snaps["reader"],
                                raw_events=events, before_index=start)
        direct_peer_match = (pair["status"] == "matched" and
                             pair.get("identity_basis") == "direct_bidirectional_matched_peer_api")
        # N-peer windows require the actual complete per-peer API proof before
        # open. Aggregate-only observations retain the legacy unique-pair gate.
        if pair["status"] != "matched":
            reasons.append("snapshot_scope_pair_not_matched")
    else:
        reasons.append("fresh_bidirectional_snapshot_chain_incomplete")
    pair = {key: opened.get(key) for key in
            ("writer", "reader", "writer_generation", "reader_generation", "writer_gid", "reader_gid")}

    def bound(event, token, gid_field):
        try:
            current = endpoint_identity_at(events, token, before_index=event["event_index"] + 1)
        except (ValueError, KeyError):
            return False
        return (event.get("run_id") == opened.get("run_id")
                and all(token.get(k) is not None and event.get(k) == token.get(k) for k in
                        ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"))
                and current.get("endpoint_gid") == token.get("endpoint_gid")
                and type(event.get("monotonic_ns")) is int
                and event["monotonic_ns"] >= current["identity_valid_after_ns"]
                and event.get(gid_field) == current.get("endpoint_gid"))

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
        if active != 1 and not direct_peer_match:
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
        if publisher is None and sample.get("payload_writer_id") not in (None, w["logical_id"]):
            other_sends = [e for e in prefix if e.get("event") == "send"
                           and e.get("payload_sha256") == sample.get("payload_sha256")
                           and e.get("run_id") == sample.get("payload_run_id")]
            if len(other_sends) == 1:
                other = other_sends[0]
                other_pair = {**pair, "writer": other.get("logical_id"),
                              "writer_generation": other.get("generation"),
                              "writer_gid": other.get("writer_gid")}
                witness, _ = payload_delivery_witness(other_pair, sample, prefix)
                if witness is not None:
                    continue
            reasons.append("other_writer_payload_not_raw_bound")
            continue
        seq = sample.get("send_sequence")
        occurrence_witness = None
        if _has_publish_occurrence(pair, sample, prefix):
            occurrence_witness, occurrence_reason = payload_delivery_witness(pair, sample, prefix)
            if occurrence_witness is None:
                reasons.append("receive_publish_occurrence_unknown:" + str(occurrence_reason))
                continue
            seq = prefix[occurrence_witness["send_ref"]].get("send_sequence")
        if type(seq) is not int or seq not in expected:
            reasons.append("receive_without_current_window_send")
            continue
        active, _ = _raw_active_writer_inventory(prefix, w["logical_id"], sample["monotonic_ns"])
        if active != 1 and not direct_peer_match:
            reasons.append("receive_active_writer_generation_not_raw_unique")
            continue
        basis = "direct_publish_occurrence" if occurrence_witness is not None else "direct_publisher_gid"
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


def _repeat_occurrence_observation(source, sent, events):
    """Resolve identical-body sends through a real unfiltered control sample.

    Positive occurrence identity is not a filtering result. In particular a
    missing filtered-reader receive is deliberately not promoted here.
    """
    from .publish_occurrence import producer_occurrence_witness, publish_occurrence_witness
    base = {"writer": source["logical_id"], "writer_generation": source["generation"],
            "writer_gid": source["writer_gid"]}
    pair = _occurrence_pair_with_control(base, None, events)
    if pair.get("control") is None:
        return None, "unfiltered_control_identity_missing_or_ambiguous"
    pair.update(reader=pair["control"], reader_generation=pair["control_generation"],
                reader_gid=pair["control_gid"])
    producers = []
    for send in (source, sent):
        producer, reason = producer_occurrence_witness(send, events)
        if producer is None:
            return None, reason
        producers.append(producer)
    samples = {source["event_index"]: [], sent["event_index"]: []}
    for receive in events:
        if (receive.get("event") != "receive" or receive.get("logical_id") != pair["control"]
                or receive.get("generation") != pair["control_generation"]
                or receive.get("payload_sha256") != source["payload_sha256"]):
            continue
        witness, reason = publish_occurrence_witness(pair, receive, events)
        if witness is None:
            return None, reason
        if witness["send_ref"] in samples:
            samples[witness["send_ref"]].append(witness)
    if any(len(value) != 1 for value in samples.values()):
        return None, "source_or_repeat_control_occurrence_missing_or_ambiguous"
    original, repeat = (samples[send["event_index"]][0] for send in (source, sent))
    if (original["publication_sequence_number"] == repeat["publication_sequence_number"]
            or original["source_timestamp"] == repeat["source_timestamp"]):
        return None, "source_and_repeat_sample_identity_not_distinct"
    return {"source_producer": producers[0], "repeat_producer": producers[1],
            "source_control_witness": original, "repeat_control_witness": repeat,
            "control": pair["control"], "control_generation": pair["control_generation"],
            "control_gid": pair["control_gid"],
            "identity_basis": "direct_gid_scoped_publish_occurrence"}, None


def _repeat_data_observation(binding, events):
    """Compare sent bytes, without claiming which repeated occurrence delivered."""
    unknown = {"status": "unknown", "binding_ref": binding.get("event_index"),
               "filter_effect_observed": None, "candidate_eligible": False,
               "delivery_attribution": "unknown"}
    ref = binding.get("source_send_ref")
    if type(ref) is not int or not 0 <= ref < binding.get("event_index", 0):
        return {**unknown, "reason": "source_ref_invalid"}
    source = events[ref]
    sends = [e for e in events if e.get("event") == "send"
             and e.get("logical_id") == binding.get("writer")
             and e.get("generation") == binding.get("writer_generation")
             and e.get("phase") == binding.get("action_id")
             and e.get("command_id") == binding.get("command_id")
             and e.get("send_sequence") == binding.get("occurrence_sequence")]
    if len(sends) != 1:
        return {**unknown, "reason": "repeat_send_missing_or_ambiguous"}
    sent = sends[0]
    if (source.get("event") != "send" or source.get("publish_returned") is not True
            or sent.get("publish_returned") is not True
            or any(source.get(k) != sent.get(k) or source.get(k) is None for k in
                   ("logical_id", "generation", "writer_gid", "pid", "process_start_ticks", "endpoint_generation", "run_id"))
            or source.get("writer_gid") != binding.get("writer_gid")
            or source.get("phase") != binding.get("source_action")
            or source.get("send_sequence") == sent.get("send_sequence")
            or source.get("command_id") == sent.get("command_id")
            or not (source.get("event_index", -1) < sent.get("event_index", -1) < binding["event_index"])
            or type(source.get("monotonic_ns")) is not int or type(sent.get("monotonic_ns")) is not int
            or source["monotonic_ns"] > sent["monotonic_ns"]
            or not isinstance(source.get("string_data"), str)
            or source.get("string_data") != sent.get("string_data")
            or source.get("payload_sha256") != sent.get("payload_sha256")
            or hashlib.sha256(source["string_data"].encode()).hexdigest() != source.get("payload_sha256")):
        return {**unknown, "reason": "repeat_identity_chronology_or_bytes_invalid"}
    if any(isinstance(row.get("publish_occurrence_receipt"), dict)
           and row["publish_occurrence_receipt"].get("status") == "observed"
           for row in (source, sent)):
        occurrence, reason = _repeat_occurrence_observation(source, sent, events)
        return {**unknown,
                "status": "identical_input_occurrences_observed" if occurrence is not None else "identical_input_sent",
                "reason": reason, "source_send_ref": ref, "repeat_send_ref": sent["event_index"],
                "input_equivalence": "complete_String.data", "payload_sha256": source["payload_sha256"],
                "delivery_attribution": "direct_gid_scoped_publish_occurrence" if occurrence is not None else "unknown",
                "occurrence_mapping": occurrence}
    # Legacy first occurrence can only be witnessed from the capture prefix
    # before the repeat was sent; later equal-payload receives remain ambiguous.
    updates = [e for e in events if e.get("event") == "topology_filter_update_result"
               and source["event_index"] < e.get("event_index", -1) < sent["event_index"]]
    control_ref = None
    update_ref = None
    if len(updates) == 1:
        update = updates[0]
        prior = events[:sent["event_index"]]
        def same_update(e):
            return all(e.get(k) is not None and e.get(k) == update.get(k)
                       for k in ("command_id", "logical_id", "generation", "reader_gid"))
        calls = [e for e in prior if e.get("event") == "content_filter_api_call"
                 and e.get("api_stage") == "set_begin" and same_update(e)]
        raw_updates = [e for e in prior if e.get("event") == "content_filter_dynamic_update" and same_update(e)]
        if len(calls) == len(raw_updates) == 1:
            call, raw_update = calls[0], raw_updates[0]
            clock = source.get("monotonic_clock_identity")
            begin, returned, update_begin = source.get("publish_begin_ns"), source.get("publish_return_ns"), call.get("api_monotonic_ns")
            clock_ok = (isinstance(clock, dict) and bool(clock.get("boot_id"))
                        and type(clock.get("device")) is int and type(clock.get("inode")) is int
                        and call.get("monotonic_clock_identity") == clock
                        and raw_update.get("monotonic_clock_identity") == clock)
            creations = [e for e in events if e.get("event") == "endpoint_created"
                         and e.get("logical_id") == update.get("logical_id")
                         and e.get("generation") == update.get("generation")]
            identity_ok = (len(creations) == 1 and creations[0].get("endpoint_gid") == update.get("reader_gid")
                           and all(call.get(k) is not None and call.get(k) == raw_update.get(k) == creations[0].get(k)
                                   for k in ("pid", "process_start_ticks", "endpoint_generation", "run_id"))
                           and creations[0].get("monotonic_clock_identity") == clock)
            retirements = [e for e in events if e.get("event") in
                           {"endpoint_destroyed", "topology_endpoint_destroyed", "worker_exit", "topology_worker_exit", "topology_generation_retired"}
                           and e.get("logical_id") == update.get("logical_id")
                           and e.get("generation") == update.get("generation")]
            lifecycle_ok = (identity_ok and type(creations[0].get("monotonic_ns")) is int
                            and type(raw_update.get("update_started_ns")) is int
                            and type(raw_update.get("update_finished_ns")) is int
                            and creations[0]["monotonic_ns"] <= raw_update["update_started_ns"]
                            and all(e.get("monotonic_clock_identity") == clock
                                    and type(e.get("monotonic_ns")) is int
                                    and e["monotonic_ns"] > raw_update["update_finished_ns"]
                                    for e in retirements))
            timestamps = (begin, returned, source.get("monotonic_ns"), update_begin,
                          raw_update.get("update_started_ns"), raw_update.get("update_finished_ns"))
            if (clock_ok and lifecycle_ok and all(type(x) is int for x in timestamps)
                    and begin <= returned <= source["monotonic_ns"]
                    and returned <= raw_update["update_started_ns"] <= update_begin <= raw_update["update_finished_ns"]):
                candidates = []
                for receive in prior:
                    if (receive.get("event") != "receive" or receive.get("payload_sha256") != source["payload_sha256"]
                            or receive.get("monotonic_clock_identity") != clock
                            or type(receive.get("monotonic_ns")) is not int
                            or not begin <= receive["monotonic_ns"] < raw_update["update_started_ns"]):
                        continue
                    pair = {"writer": source["logical_id"], "writer_generation": source["generation"],
                            "writer_gid": source["writer_gid"], "reader": update.get("logical_id"),
                            "reader_generation": update.get("generation"), "reader_gid": update.get("reader_gid")}
                    witness, _ = payload_delivery_witness(pair, receive, prior)
                    if witness is not None and witness.get("send_ref") == ref:
                        candidates.append(receive["event_index"])
                if len(candidates) == 1:
                    control_ref, update_ref = candidates[0], update["event_index"]
    return {**unknown, "status": "identical_input_sent", "reason": "delivery_occurrence_unresolved",
            "source_send_ref": ref, "repeat_send_ref": sent["event_index"],
            "pre_control_receive_ref": control_ref, "between_update_ref": update_ref,
            "input_equivalence": "complete_String.data", "payload_sha256": source["payload_sha256"]}



def _dynamic_cft_effect(update, events, *, real_ros):
    """Compare raw windows; no absent final sample or configuration implies effect.

    Only SQL LIKE with one quoted ASCII parameter and '%' wildcards is
    interpreted. The result is observational, never a candidate or verdict.
    """
    import re
    unknown = {"status": "unknown", "filter_effect_observed": None,
               "fixture_contrast_observed": None, "candidate_eligible": False,
               "update_ref": update.get("event_index")}
    def fail(reason):
        return {**unknown, "reason": reason}
    receipt = update.get("receipt") or {}
    observed = receipt.get("observed") or {}
    requested = receipt.get("requested") or {}
    if (receipt.get("api_status") != "succeeded" or receipt.get("readback_status") != "matched"
            or receipt.get("applied") is not True or requested.get("enabled") is not True
            or requested.get("expression") != observed.get("expression")
            or requested.get("parameters") != observed.get("parameters")):
        return fail("update_api_or_actual_readback_unavailable")
    expr, params = observed.get("expression"), observed.get("parameters")
    if not isinstance(expr, str) or not re.fullmatch(r"\s*data\s+LIKE\s+%0\s*", expr, re.I):
        return fail("predicate_unsupported")
    if not isinstance(params, list) or len(params) != 1 or not isinstance(params[0], str):
        return fail("predicate_parameter_unsupported")
    literal = params[0]
    if (len(literal) < 2 or literal[0] != "'" or literal[-1] != "'"
            or not literal.isascii() or any(c in literal[1:-1] for c in "_'\\")):
        return fail("predicate_parameter_unsupported")
    pattern = re.compile("\\A" + ".*".join(re.escape(x) for x in literal[1:-1].split("%")) + "\\Z", re.S)
    uid = update.get("event_index")
    workers = [e for e in events if e.get("event") == "content_filter_dynamic_update"
               and e.get("logical_id") == update.get("logical_id")
               and e.get("generation") == update.get("generation")
               and e.get("command_id") == update.get("command_id")]
    if len(workers) != 1:
        return fail("worker_update_missing_or_ambiguous")
    worker = workers[0]
    if real_ros and (worker.get("fixture") is True or worker.get("backend") == OFFLINE_BACKEND):
        return fail("fixture_cannot_qualify_real_effect")
    start, end = worker.get("update_started_ns"), worker.get("update_finished_ns")
    if (type(start) is not int or type(end) is not int or start > end
            or worker.get("event_index", uid) >= uid or worker.get("filter_receipt") != receipt
            or worker.get("reader_gid") != update.get("reader_gid")):
        return fail("worker_update_chronology_or_receipt_invalid")
    created = [e for e in events if e.get("event") == "endpoint_created"
               and e.get("logical_id") == update.get("logical_id")
               and e.get("generation") == update.get("generation")]
    if len(created) != 1 or any(worker.get(k) is None or worker.get(k) != created[0].get(k)
            for k in ("pid", "process_start_ticks", "endpoint_generation")):
        return fail("reader_identity_not_bound")
    related = [e for e in events if e.get("logical_id") == update.get("logical_id")
               and e.get("generation") == update.get("generation")
               and e.get("command_id") == update.get("command_id")]
    incarnation_contract = ("before_gid" in worker or "after_gid" in worker
        or any(e.get("event") in {"endpoint_identity_snapshot", "endpoint_identity_transition"}
               for e in related))
    # New captures explicitly distinguish API's old reader identity from the
    # independently observed post-update identity. Legacy stable-GID captures
    # retain their exact old gate, never an arbitrary changed-GID fallback.
    try:
        before_identity = endpoint_identity_at(events, created[0], before_index=worker["event_index"]
            if not incarnation_contract else min(e["event_index"] for e in related))
        if incarnation_contract:
            after_identity = validate_filter_identity_update(events, before_identity, worker.get("command_id"))
            if (after_identity["endpoint_gid"] != update.get("reader_gid")
                    or after_identity["identity_valid_after_ref"] >= uid
                    or after_identity["identity_valid_after_ns"] > update.get("monotonic_ns", -1)):
                return fail("update_incarnation_not_committed_before_result")
        else:
            after_identity = before_identity
            if after_identity["endpoint_gid"] != update.get("reader_gid"):
                return fail("reader_identity_not_bound")
    except (ValueError, KeyError, TypeError) as exc:
        return fail("update_incarnation_evidence_invalid:" + str(exc))
    calls = [e for e in events if e.get("event") == "content_filter_api_call"
             and e.get("logical_id") == update.get("logical_id")
             and e.get("command_id") == update.get("command_id")]
    if (len(calls) != 4 or [e.get("api_stage") for e in calls] !=
            ["set_begin", "set_success", "get_begin", "get_success"]
            or any(e.get("generation") != worker.get("generation")
                   or e.get("reader_gid") != before_identity.get("endpoint_gid")
                   or any(e.get(k) != worker.get(k) for k in
                          ("pid", "process_start_ticks", "endpoint_generation"))
                   or type(e.get("api_monotonic_ns")) is not int for e in calls)
            or not all(start <= e["api_monotonic_ns"] <= end for e in calls)
            or any(a["api_monotonic_ns"] > b["api_monotonic_ns"]
                   or a.get("event_index", -1) >= b.get("event_index", -1)
                   for a, b in zip(calls, calls[1:]))
            or calls[-1].get("event_index", uid) >= worker["event_index"]
            or calls[0].get("requested") != requested
            or calls[-1].get("observed") != observed
            or (real_ros and any(e.get("fixture") is True for e in calls))):
        return fail("independent_api_call_evidence_invalid")
    from .filter_backend import normalize_captured_update_readback
    captured = normalize_captured_update_readback(calls[-1].get("raw_readback"),
                                                 fixture=not real_ros)
    if captured is None or captured != calls[-1].get("observed") or captured != observed:
        return fail("raw_api_readback_invalid_or_inconsistent")
    # These receipts prove API identity/config observation only. Keep them when
    # a later delivery/occurrence gate is unknown; never label them filter effect.
    unknown.update(api_call_refs=[e["event_index"] for e in calls],
        reader_identity={"before_gid": before_identity["endpoint_gid"],
            "after_gid": after_identity["endpoint_gid"],
            "incarnation_before": before_identity["incarnation_ordinal"],
            "incarnation_after": after_identity["incarnation_ordinal"],
            "transition_ref": after_identity["identity_transition_ref"],
            "ack_ref": after_identity["identity_valid_after_ref"]},
        api_identity_observed=True, configuration_readback_observed=True)
    closes = [e for e in events if e.get("event") == "topology_delivery_window_closed"
              and e.get("reader") == update.get("logical_id")
              and e.get("reader_generation") == update.get("generation")]
    pre = [e for e in closes if e.get("event_index", uid) < worker["event_index"]
           and e.get("monotonic_ns", start + 1) <= start]
    post = [e for e in closes if e.get("event_index", 0) > uid
            and type(e.get("open_ref")) is int and 0 <= e["open_ref"] < len(events)
            and events[e["open_ref"]].get("monotonic_ns", 0) >= end]
    if not pre or not post:
        return fail("pre_or_post_raw_window_missing")
    before, after = pre[-1], post[0]
    keys = ("writer", "reader", "writer_generation", "reader_generation")
    if any(before.get(k) != after.get(k) for k in keys):
        return fail("window_generation_changed")
    if any(type(x.get("open_ref")) is not int or not 0 <= x["open_ref"] < x["event_index"]
           for x in (before, after)):
        return fail("window_open_ref_invalid")
    openings = [events[x["open_ref"]] for x in (before, after)]
    if openings[0].get("writer_gid") != openings[1].get("writer_gid"):
        return fail("window_gid_changed")
    if (openings[0].get("reader_gid") != before_identity["endpoint_gid"]
            or openings[1].get("reader_gid") != after_identity["endpoint_gid"]):
        return fail("window_reader_incarnation_mismatch")
    if (openings[0].get("reader_gid") != openings[1].get("reader_gid")
            and not incarnation_contract):
        return fail("window_gid_changed")
    if openings[1].get("reader_gid") != update.get("reader_gid"):
        return fail("window_reader_gid_mismatch")
    if any(e.get("event") == "content_filter_dynamic_update" and e is not worker
           and e.get("logical_id") == update.get("logical_id")
           and before["event_index"] < e.get("event_index", -1) < after["event_index"] for e in events):
        return fail("intervening_update")
    for close, opened in zip((before, after), openings):
        proof = _delivery_window_evidence(opened, events, before_index=close["event_index"])
        if (not proof.get("window_complete") or not close.get("window_complete")
                or any(proof.get(k) != close.get(k) for k in
                       ("closure_status", "send_refs", "receive_refs", "missing_sequences", "sentinel_send_ref", "sentinel_receive_ref"))):
            return fail("window_not_raw_closed")
    if before.get("missing_sequences") or not after.get("missing_sequences"):
        return fail("positive_control_or_post_contrast_missing")
    writer = [e for e in events if e.get("event") == "endpoint_created"
              and e.get("logical_id") == after.get("writer")
              and e.get("generation") == after.get("writer_generation")
              and e.get("endpoint_gid") == openings[1].get("writer_gid")]
    if len(writer) != 1 or any((e.get("qos") or {}).get("reliability") != "reliable" for e in (writer[0], created[0])):
        return fail("observed_reliable_qos_unavailable")
    pair = {**{k: after.get(k) for k in keys},
            "writer_gid": openings[1].get("writer_gid"), "reader_gid": openings[1].get("reader_gid")}
    # A decoded body retains its original sequence on publish_same_data. Only
    # authenticated native scope can supply a different actual occurrence; the
    # legacy decoder must keep its original sequence equality gate.
    native_occurrences = _has_publish_occurrence(pair, None, events)
    def sample(row):
        text = row.get("string_data")
        if not isinstance(text, str) or hashlib.sha256(text.encode()).hexdigest() != row.get("payload_sha256"):
            return None
        try:
            body = json.loads(text)
        except (ValueError, TypeError):
            return None
        if (not isinstance(body, dict) or type(body.get("send_sequence")) is not int
                or body["send_sequence"] < 0
                or (not native_occurrences and body["send_sequence"] != row.get("send_sequence"))
                or body.get("run_id") != row.get("run_id") or body.get("writer_id") != pair["writer"]
                or body.get("writer_generation") != pair["writer_generation"]
                or body.get("endpoint_gid") != pair["writer_gid"]
                or not isinstance(body.get("declared_payload"), str)):
            return None
        if body.get("declared_payload_sha256") != hashlib.sha256(body["declared_payload"].encode()).hexdigest():
            return None
        return body, bool(pattern.fullmatch(text))
    def rows(close, key):
        refs = close.get(key)
        if not isinstance(refs, list) or any(type(i) is not int or i < 0 or i >= close["event_index"] for i in refs):
            return []
        return [events[i] for i in refs]
    pre_sends, post_sends = rows(before, "send_refs"), rows(after, "send_refs")
    pre_recvs, post_recvs = rows(before, "receive_refs"), rows(after, "receive_refs")
    control_window_refs = [before["event_index"]]
    if native_occurrences:
        # The immediate health window can contain unrelated traffic. An exact
        # repeated body may have its positive delivery control in an earlier
        # closed window. Keep the immediate window/transition gates above and
        # additionally authenticate that earlier window; never rewrite either.
        rejected_bodies = {e.get("string_data") for e in post_sends
                           if isinstance(e.get("string_data"), str)
                           and not pattern.fullmatch(e["string_data"])}
        if not rejected_bodies.intersection(e.get("string_data") for e in pre_sends):
            for earlier in reversed(pre[:-1]):
                old_sends = rows(earlier, "send_refs")
                if not rejected_bodies.intersection(e.get("string_data") for e in old_sends):
                    continue
                old_ref = earlier.get("open_ref")
                if (type(old_ref) is not int or not 0 <= old_ref < earlier["event_index"]
                        or any(earlier.get(k) != before.get(k) for k in keys)):
                    return fail("prior_control_window_identity_invalid")
                old_open = events[old_ref]
                if any(old_open.get(k) != openings[0].get(k)
                       for k in (*keys, "writer_gid", "reader_gid")):
                    return fail("prior_control_window_identity_invalid")
                proof = _delivery_window_evidence(old_open, events,
                                                 before_index=earlier["event_index"])
                if (not proof.get("window_complete") or not earlier.get("window_complete")
                        or proof.get("missing_sequences")
                        or any(proof.get(k) != earlier.get(k) for k in
                               ("closure_status", "send_refs", "receive_refs", "missing_sequences",
                                "sentinel_send_ref", "sentinel_receive_ref"))):
                    return fail("prior_control_window_not_raw_closed")
                old_recvs = rows(earlier, "receive_refs")
                pre_sends = list({e["event_index"]: e for e in old_sends + pre_sends}.values())
                pre_recvs = list({e["event_index"]: e for e in old_recvs + pre_recvs}.values())
                control_window_refs.insert(0, earlier["event_index"])
                break
    if not pre_sends or not post_sends or not pre_recvs or not post_recvs:
        return fail("raw_sample_rows_missing")
    if (any(type(e.get("monotonic_ns")) is not int or e["monotonic_ns"] > start
            for e in pre_sends + pre_recvs)
            or any(type(e.get("monotonic_ns")) is not int or e["monotonic_ns"] < end
                   for e in post_sends + post_recvs)):
        return fail("sample_outside_update_time_window")
    all_sends = pre_sends + post_sends
    producer_witnesses = []
    if native_occurrences:
        from .publish_occurrence import producer_occurrence_witness
        for send in all_sends:
            producer, reason = producer_occurrence_witness(send, events)
            if producer is None:
                return fail("sample_publish_occurrence_invalid:" + str(reason))
            producer_witnesses.append(producer)
    decoded = {e["event_index"]: sample(e) for e in all_sends}
    if any(v is None for v in decoded.values()):
        return fail("sample_encoding_or_hash_invalid")
    receive_send_refs, receive_witnesses, decoded_receives = {}, [], {}
    for receives, sends, identity in ((pre_recvs, pre_sends, before_identity),
                                      (post_recvs, post_sends, after_identity)):
        allowed_refs = {send["event_index"] for send in sends}
        seen = set()
        for receive in receives:
            sample_pair = {**pair, "reader_gid": identity["endpoint_gid"]}
            witness, _ = payload_delivery_witness(sample_pair, receive, events)
            value = sample(receive)
            if witness is None or value is None:
                return fail("receive_sample_identity_or_encoding_invalid")
            ref = witness.get("send_ref")
            if ref not in allowed_refs:
                return fail("receive_occurrence_outside_its_window")
            if ref in seen:
                return fail("receive_occurrence_duplicated")
            seen.add(ref)
            send = events[ref]
            if receive.get("string_data") != send.get("string_data"):
                return fail("receive_string_data_mismatch")
            receive_send_refs[receive["event_index"]] = ref
            decoded_receives[receive["event_index"]] = value
            receive_witnesses.append(witness)
    missing = [e for e in post_sends if e.get("send_sequence") in after["missing_sequences"]]
    controls = [e for e in pre_sends if not decoded[e["event_index"]][1]
                and any(receive_send_refs[r["event_index"]] == e["event_index"] for r in pre_recvs)]
    sentinel = events[after["sentinel_send_ref"]]
    sentinel_receive = events[after["sentinel_receive_ref"]]
    if (receive_send_refs.get(sentinel_receive["event_index"]) != sentinel["event_index"]
            or type(sentinel_receive.get("monotonic_ns")) is not int
            or any(type(e.get("monotonic_ns")) is not int
                   or e["monotonic_ns"] > sentinel_receive["monotonic_ns"] for e in missing)):
        return fail("sentinel_does_not_close_missing_send")
    if any(not decoded_receives[receive["event_index"]][1] for receive in post_recvs):
        return fail("post_update_received_rejected_sample")
    if (not missing or any(decoded[e["event_index"]][1] for e in missing)
            or not decoded.get(sentinel["event_index"], (None, False))[1]
            or not any(decoded[c["event_index"]][0]["declared_payload"] == decoded[m["event_index"]][0]["declared_payload"]
                       for c in controls for m in missing)):
        return fail("positive_control_predicate_or_sentinel_missing")
    missing_refs = {send["event_index"] for send in missing}
    missing_hashes = {send.get("payload_sha256") for send in missing}
    for receive in events:
        if not (receive.get("event") == "receive" and receive.get("logical_id") == pair["reader"]
                and receive.get("generation") == pair["reader_generation"]
                and receive.get("payload_sha256") in missing_hashes
                and receive.get("event_index", 0) > after["event_index"]):
            continue
        if not native_occurrences:
            return fail("late_receive_after_window")
        # Later restoration may legitimately publish identical bytes again.
        # Resolve that receive's own reader incarnation and actual source scope;
        # equality of payload hashes is not equality of DDS occurrences.
        try:
            identity = endpoint_identity_at(events, created[0], before_index=receive["event_index"] + 1)
        except (ValueError, KeyError, TypeError):
            return fail("late_receive_reader_identity_invalid")
        witness, reason = payload_delivery_witness(
            {**pair, "reader_gid": identity["endpoint_gid"]}, receive, events)
        if witness is None:
            return fail("late_receive_occurrence_unknown:" + str(reason))
        if witness["send_ref"] in missing_refs:
            return fail("late_receive_after_window")
    full_equivalence = any(c.get("string_data") == m.get("string_data") for c in controls for m in missing)
    return {**unknown, "status": "delivery_contrast",
            "filter_effect_observed": None,
            "control_equivalence": ("complete_String.data" if full_equivalence else
                                    "declared_payload_only; complete String.data differs or is unverified"),
            "publish_occurrence_witnesses": producer_witnesses,
            "sample_delivery_witnesses": receive_witnesses,
            "attribution": "unknown; missing delivery is not proof of filtering",
            "fixture_contrast_observed": True if not real_ros else None,
            "reason": None, "pre_window_ref": before["event_index"],
            "post_window_ref": after["event_index"], "worker_update_ref": worker["event_index"],
            "positive_control_send_refs": [e["event_index"] for e in controls],
            "positive_control_window_refs": control_window_refs,
            "missing_send_refs": [e["event_index"] for e in missing],
            "sentinel_receive_ref": after["sentinel_receive_ref"],
            "interpretation": "sample_delivery_contrast_not_a_sut_verdict"}



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
                snapshot_command_id=snapshot.get("command_id"), real_ros=real_ros, raw_events=events))
    return rows


def _delivery_rows(pairs, events, handles, *, real_ros=False):
    rows = []
    active_by_topic = {}
    for handle in handles.values():
        if handle.get("alive") and handle.get("role") == "writer":
            active_by_topic[handle.get("topic")] = active_by_topic.get(handle.get("topic"), 0) + 1
    lifetimes = _delivery_lifetimes(events)
    for pair in pairs:
        rows.append(pair_delivery_fact(
            pair, events,
            active_writers_on_topic=active_by_topic.get(pair.get("topic"), 0),
            real_ros=real_ros, _lifetimes=lifetimes))
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
        "supported_message_types": list(SUPPORTED_MESSAGE_TYPES),
        "message_type_scopes": {
            "endpoint_process_blackbox": list(SUPPORTED_MESSAGE_TYPES),
            "participant_control": ["std_msgs/msg/String"],
            "instrumentation": ["std_msgs/msg/String"],
            "content_filter": ["std_msgs/msg/String"],
            "native_full_message_info_take": ["std_msgs/msg/String"],
        },
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


def group_termination_rows(program, events, raw_path, tokens, boundary):
    """Audit group shutdown once, preserving actual process and endpoint rows.

    This consumes a closed capture, not a lifecycle transition. Group processes
    have no endpoint identity; each endpoint keeps its own cleanup receipt.
    """
    from .participant_groups import project_lineage
    lineage = project_lineage(program, events, raw_path)
    selected = {}
    fields = ("logical_id", "generation", "endpoint_generation", "endpoint_gid",
              "group_id", "worker_instance_id", "context_instance_id", "pid", "pgid", "process_start_ticks")
    group_ids = set()
    for token in tokens:
        workers = [w for w in lineage["workers"] if all(w.get(k) == token.get(k) for k in fields)]
        if len(workers) != 1:
            raise ValueError("group_termination_endpoint_identity")
        group_ids.add(workers[0]["group_id"])
        if any(ref['event_index'] <= boundary['event_index'] for ref in workers[0]['cleanup_raw_refs']):
            raise ValueError('group_termination_selected_endpoint_retired')
    # Audit the entire lineage above; final closure excludes already retired siblings.
    for worker in lineage["workers"]:
        if worker["group_id"] not in group_ids:
            continue
        for ref in worker["cleanup_raw_refs"] + worker["exit_raw_refs"]:
            if ref['event_index'] <= boundary['event_index']:
                continue  # Historical endpoint closure was authenticated by project_lineage.
            selected[ref["event_index"]] = events[ref["event_index"]]
    for context in lineage["contexts"]:
        if context["group_id"] in group_ids:
            ref = context["shutdown_raw_ref"]
            selected[ref["event_index"]] = events[ref["event_index"]]
    rows = [selected[i] for i in sorted(selected)]
    previous = boundary
    for row in rows:
        if (row["event_index"] <= previous["event_index"]
                or row["monotonic_ns"] < previous["monotonic_ns"]):
            raise ValueError("group_termination_after_boundary_chronology")
        previous = row
    return rows


def _execution_workers(events, raw_path, program=None, *, config=None):
    """Project all captured endpoint generations; never fill actuals from cfg.

    Raw refs are offsets into the enclosing result's SHA-bound raw file. No
    self-referential raw hash is embedded in that file's metadata event.
    """
    from .participant_groups import enabled, lineage_metadata
    if enabled(program):
        return lineage_metadata(program, events, raw_path, config=config)['workers']
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
        incarnations = [e for e in lineage if e.get("event") == "endpoint_identity_transition"]
        try:
            current = endpoint_identity_at(events, token)
        except ValueError as exc:
            current = {}
            problems.append("endpoint_incarnation_invalid:" + str(exc))
        workers.append({
            "role": token.get("worker_role", token.get("topology_worker_role")),
            "endpoint_role": worker_role_kind(token.get("role")), "logical_id": eid,
            "generation": generation, "worker_generation": token.get("worker_generation"),
            "endpoint_gid": token.get("endpoint_gid"), "endpoint_generation": token.get("endpoint_generation"),
            "current_endpoint_gid": current.get("endpoint_gid"),
            "incarnation_ordinal": current.get("incarnation_ordinal"),
            "identity_transition_raw_refs": [ref(e) for e in incarnations],
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


def _cleanup_is_complete(receipt, backend, netem_ctx, *, netem_required=False):
    """Conservative final cleanup observation, independent of execution eligibility."""
    if (not isinstance(receipt, dict) or receipt.get("closed") is not True
            or receipt.get("errors", []) != [] or receipt.get("survivors", []) != []
            or receipt.get("error") is not None):
        return False
    returncodes = receipt.get("returncodes", [])
    workers = receipt.get("workers", [])
    if (not isinstance(returncodes, list) or not isinstance(workers, list)
            or any(type(code) is not int or code != 0 for code in returncodes)
            or any(not isinstance(worker, dict) or worker.get("closed") is not True
                   or ("returncode" in worker and
                       (type(worker["returncode"]) is not int or worker["returncode"] != 0))
                   for worker in workers)):
        return False
    for worker in getattr(backend, "processes", []) or []:
        process = worker.process
        if process is not None:
            code = process.poll()
            if type(code) is not int or code != 0:
                return False
    if netem_required and netem_ctx is None:
        # Setup/start failed before a restorable window receipt was returned.
        return False
    if netem_ctx is not None:
        restore = netem_ctx.get("restore") or {}
        if (netem_ctx.get("active") is not False or netem_ctx.get("restore_failure") is not None
                or restore.get("restored") is not True
                or restore.get("only_noqueue_root") is not True
                or restore.get("residual_qdisc") != [] or restore.get("residual_filter") != []):
            return False
    return True



def _liveliness_observation_facts(events, *, run_id, real_ros):
    """Consume the final raw liveliness chain without promoting it to a hit.

    The worker currently does not emit a trusted continuous peer-coverage row,
    so normal executions remain ``unknown``.  Keeping this consumer in the
    final facts path is intentional: a future producer must satisfy the same
    fail-closed contract before any lifecycle effect can be observed.
    """
    def unknown(reason, **extra):
        return {
            "status": "unknown", "reason": reason, "loss_observed": None,
            "recovery_observed": None, "effect_observed": None,
            "execution_evidence": False, "candidate_eligible": False, **extra,
        }

    asserted = [e for e in events if e.get("event") == "liveliness_asserted"]
    if not asserted:
        return []
    created = [e for e in events if e.get("event") == "endpoint_created"]
    observations = []
    for assertion in asserted:
        writer_rows = [e for e in created
                       if all(e.get(key) is not None and e.get(key) == assertion.get(key)
                              for key in ("logical_id", "generation", "endpoint_gid", "pid",
                                          "process_start_ticks", "endpoint_generation"))
                       and worker_role_kind(e.get("role")) == "writer"]
        if len(writer_rows) != 1:
            observations.append(unknown("writer_endpoint_token_missing_or_ambiguous",
                                        command_id=assertion.get("command_id"),
                                        writer_logical_id=assertion.get("logical_id")))
            continue
        writer_row = writer_rows[0]
        if not isinstance(writer_row.get("topic"), str) or not writer_row["topic"]:
            observations.append(unknown("writer_topic_missing",
                                        command_id=assertion.get("command_id"),
                                        writer_logical_id=assertion.get("logical_id")))
            continue
        readers = [e for e in created if worker_role_kind(e.get("role")) == "reader"
                   and e.get("topic") == writer_row["topic"]
                   and type(e.get("monotonic_ns")) is int
                   and type(assertion.get("monotonic_ns")) is int
                   and e["monotonic_ns"] < assertion["monotonic_ns"]]
        writer = {"role": "writer", **{key: writer_row.get(key) for key in
                  ("logical_id", "generation", "pid", "process_start_ticks",
                   "endpoint_generation", "endpoint_gid")}}
        if not readers:
            observations.append(unknown("reader_endpoint_token_missing",
                                        command_id=assertion.get("command_id"),
                                        writer_logical_id=writer["logical_id"]))
            continue
        for reader_row in readers:
            reader = {"role": "reader", **{key: reader_row.get(key) for key in
                      ("logical_id", "generation", "pid", "process_start_ticks",
                       "endpoint_generation", "endpoint_gid")}}
            try:
                observation = evaluate_liveliness(
                    events, writer=writer, reader=reader, run_id=run_id,
                    fixture=not real_ros,
                    assertion_command_id=assertion.get("command_id"))
            except (KeyError, TypeError, ValueError) as exc:
                observation = unknown("consumer_error:" + type(exc).__name__)
            observation.update({"command_id": assertion.get("command_id"),
                                "writer_logical_id": writer["logical_id"],
                                "reader_logical_id": reader["logical_id"],
                                "writer_generation": writer["generation"],
                                "reader_generation": reader["generation"]})
            observations.append(observation)
    return observations


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
    from .participant_groups import validate_control
    try:
        validate_control(effective_program, cfg)
    except (ValueError, TypeError, KeyError) as exc:
        result['reason'] = 'blocked:participant_control_scope:' + str(exc)
        return result
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

    episode_rebuild = None
    if "episode_rebuild" in cfg:
        try:
            from r2f.campaign.topology_episode_rebuild import load_checkpoint
            if "episode" not in effective_program:
                raise ValueError("episode_rebuild:episode_required")
            episode_rebuild = load_checkpoint(cfg["episode_rebuild"], effective_program, cfg)
            if backend.real_ros and episode_rebuild["execution_evidence"] is not True:
                raise ValueError("episode_rebuild:real_runtime_requires_real_checkpoint_origin")
        except (OSError, ValueError, TypeError, KeyError, IndexError) as exc:
            result.update(reason="blocked:invalid_episode_checkpoint:" + str(exc),
                          runtime_status="blocked", status="blocked")
            return result

    raw_path = output_dir / "raw.jsonl"
    try:
        raw = raw_path.open("x", buffering=1)
    except FileExistsError:
        result["reason"] = "blocked:state_directory_already_executed"
        return result
    result["raw_path"] = str(raw_path)

    def emit(event, **fields):
        # Keep the emitter self-contained: contract tests intentionally execute
        # this nested function from its AST without the module globals.
        clock_identity_fn = globals().get("_monotonic_clock_identity")
        if callable(clock_identity_fn):
            clock_identity = clock_identity_fn()
        else:
            try:
                with open("/proc/sys/kernel/random/boot_id", encoding="utf-8") as stream:
                    boot_id = stream.read().strip()
                time_ns = os.stat("/proc/self/ns/time")
                clock_identity = ({"boot_id": boot_id, "device": time_ns.st_dev,
                                   "inode": time_ns.st_ino} if boot_id else None)
            except OSError:
                clock_identity = None
        value = {
            "event": event, "time_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
            "role": "topology_controller", "pid": os.getpid(), "pgid": os.getpgrp(),
            "monotonic_clock_identity": clock_identity,
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
        if not handles and "participant_control" not in effective_program:
            # Zero-endpoint programs are a supported boundary case; an empty
            # fresh snapshot is complete, not an observation failure.
            return {"snapshots": {}, "command_id": None, "complete": True}
        return backend.snapshot(handles)

    netem_ctx = None
    netem_cleanup_required = False

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

    def close_delivery_windows(reason=None, *, endpoint_id=None, topic=None, no_obligation_only=False):
        """End the capture, not the DDS obligation; interruptions stay raw."""
        for key, opened in list(delivery_windows.items()):
            if no_obligation_only and not any(e.get("event") == "topology_delivery_no_obligation"
                    and e.get("open_ref") == opened["event_index"] for e in events[opened["event_index"]+1:]):
                continue
            if endpoint_id is not None and endpoint_id not in (opened["writer"], opened["reader"]) and topic != opened.get("topic"):
                continue
            # Freeze already observed delivery closure before lifecycle side
            # effects retire a token or change the same-topic inventory. NONE /
            # no-obligation is snapshot-scoped, not a delivered sentinel: keep
            # its conservative inventory interruption even if valid right now.
            evidence = _delivery_window_evidence(opened, events)
            delivered_before_change = (reason == "endpoint_lifecycle_changed"
                and evidence["window_complete"]
                and evidence["closure_status"] in {"closed", "closed_with_gap"})
            if reason is not None and not delivered_before_change:
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
            # Keep a window skeleton for every observed pair. Only an actual
            # pre-open no-obligation proof may exempt its delivery inventory.
            opened = delivery_windows[key]
            no_obligation = _delivery_no_obligation_basis(opened, events)
            if no_obligation is not None:
                claims = [e for e in events[opened["event_index"]+1:]
                          if e.get("event") == "topology_delivery_no_obligation"
                          and e.get("open_ref") == opened["event_index"]]
                if not claims:
                    emit("topology_delivery_no_obligation", **dict(zip(_PAIR_KEY, key)),
                         open_ref=opened["event_index"], **no_obligation,
                         fixture=not backend.real_ros, candidate_eligible=False,
                         execution_evidence=bool(backend.real_ros))
                continue
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
            **({"group_observations": deepcopy(snapshot["group_observations"])}
               if "group_observations" in snapshot else {}),
            "complete": snapshot.get("complete") is True,
            "real_ros": bool(backend.real_ros),
            "execution_evidence": bool(backend.real_ros),
            "candidate_eligible": False,
        }
        result["observation_receipts"].append(receipt)
        live = {eid: st["handle"] for eid, st in state.items() if st["alive"]}
        pairs = _pair_rows(live, snapshot, events, real_ros=bool(backend.real_ros))
        _upsert(result["pair_facts"], pairs, _PAIR_KEY)
        # Refresh every retained generation, including previously complete rows.
        # Each fact consumes this prefix and its own physical lifetime bounds.
        delivery = _delivery_rows(result["pair_facts"], events, live, real_ros=bool(backend.real_ros))
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
                    # Group controller receipts acknowledge the worker's same
                    # cleanup; inventory the physical destruction only. Full
                    # request/cleanup/ACK/controller binding is audited from raw.
                    from .participant_groups import enabled as groups_enabled
                    kinds = ({"endpoint_destroyed"} if groups_enabled(effective_program)
                             else {"endpoint_destroyed", "topology_endpoint_destroyed"})
                    retired = [e for e in events if e.get("event") in kinds
                               and all(e.get(k) == token.get(k) for k in
                                       ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation"))
                               and e.get("cleanup_status") == "closed" and e["event_index"] > token["event_index"]]
                    if len(retired) == 1:
                        try:
                            current = endpoint_identity_at(events, token, before_index=retired[0]["event_index"])
                            if retired[0].get("endpoint_gid") != current.get("endpoint_gid"):
                                retired = []
                        except ValueError:
                            retired = []
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
                basis = (_delivery_no_sends_basis(live[w]["handle"], live[r]["handle"], effective_program, events, chains)
                         if phase == "after_program" and lifecycle_ok and observed else None)
                if basis is None and phase == "after_program" and lifecycle_ok and observed:
                    basis = _delivery_recreated_no_sends_basis(
                        live[w]["handle"], live[r]["handle"], effective_program, events, chains)
                if basis is not None:
                    receipt = emit("topology_delivery_no_sends", writer=w, reader=r,
                                   writer_generation=live[w]["generation"], reader_generation=live[r]["generation"],
                                   **basis)
                    window_refs.append(receipt["event_index"])
                else:
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
                             "sentinel_receive_ref", "gap_evidence", "no_obligation_ref", "no_obligation_reason", "closure_status"))):
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
        initial_boundary = state_boundary("before_program")
        episode_snapshots = {}

        # Opt-in environment mutation: only a real, isolated namespace may host
        # the existing tc-netem carrier.  Nothing here is derived from config.
        if netem_decl is not None:
            if not backend.real_ros:
                raise TopologyBackendBlocked("offline_fixture_cannot_claim_environment_mutation")
            if netem_isolation is None or netem_isolation.get("isolated") is not True:
                raise TopologyBackendBlocked("topology_netem_requires_isolated_netns")
            from .discovery_network import DiscoveryNetemController
            carrier = DiscoveryNetemController(cfg["domain_id"])
            netem_cleanup_required = True
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
        concurrent_begun = {}
        concurrent_results = {}
        failed_concurrent_groups = set()
        def execute_action(action_index, action):
            if action["action_id"] not in concurrent_begun:
                check_topology_source("before_action:" + action["action_id"])
            action_id = action["action_id"]
            action_state = state.get(action.get("endpoint_id"))
            action_before = ({key: action_state[key] for key in
                              ("generation", "alive", "next_sequence")}
                             if action_state is not None else
                             {"alive": sorted(e for e, st in state.items() if st["alive"])})
            if action_id in concurrent_begun:
                action_before = deepcopy(concurrent_begun[action_id])
            if action_id not in concurrent_begun:
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
                        return
                    before = {"generation": st["generation"], "alive": st["alive"],
                              "next_sequence": st["next_sequence"]}
                if op in ("create", "recreate"):
                    st = state[eid]
                    if op == "create":
                        if not (gen == 1 and st["generation"] == 0 and not st["alive"]):
                            record_guard(action, "first_generation_guard",
                                         "create requires generation 1 from generation 0",
                                         before=before)
                            return
                    else:
                        if not (st["generation"] > 0 and gen == st["generation"] + 1):
                            record_guard(action, "next_generation_guard",
                                         "recreate requires previous generation + 1",
                                         before=before)
                            return
                    if st["alive"] and st["handle"] is not None:
                        # recreate replaces a live endpoint; destroy is a real step.
                        try:
                            close_delivery_windows("endpoint_lifecycle_changed", endpoint_id=eid,
                                                   topic=endpoint_specs[eid]["topic"])
                            cleanup = backend.destroy_endpoint(st["handle"])
                        except TopologyBackendError as exc:
                            record_guard(action, "predecessor_destroy_failed",
                                         type(exc).__name__, kind="tool", before=before)
                            return
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
                        return
                    try:
                        close_delivery_windows("endpoint_lifecycle_changed", endpoint_id=eid,
                                               topic=endpoint_specs[eid]["topic"])
                        handle = backend.create_endpoint(endpoint_specs[eid], gen, worker_roles[eid])
                    except TopologyUnsupported as exc:
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        return
                    except TopologyBackendTimeout as exc:
                        record_guard(action, "endpoint_create_timeout", str(exc), kind="tool",
                                     before=before)
                        return
                    except TopologyBackendError as exc:
                        record_guard(action, "endpoint_create_error", type(exc).__name__,
                                     kind="tool", before=before)
                        return
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
                    return
                if op == "destroy":
                    st = state[eid]
                    if not (st["alive"] and st["generation"] == gen):
                        record_guard(action, "current_generation_alive_guard",
                                     "destroy requires current live generation", before=before)
                        return
                    try:
                        close_delivery_windows("endpoint_lifecycle_changed", endpoint_id=eid,
                                               topic=endpoint_specs[eid]["topic"])
                        cleanup = backend.destroy_endpoint(st["handle"])
                    except TopologyBackendError as exc:
                        record_guard(action, "endpoint_destroy_error", type(exc).__name__,
                                     kind="tool", before=before)
                        return
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
                    return
                if op == "update_filter":
                    st = state[eid]
                    if not (st["alive"] and st["generation"] == gen and endpoint_specs[eid]["role"] == "reader"):
                        record_guard(action, "reader_generation_alive_guard",
                                     "update_filter requires a live reader generation", before=before)
                        return
                    try:
                        # Commit a real pre-update observation before issuing the API call.
                        backend.pump(0.0)
                        close_delivery_windows()
                        update = backend.update_filter(st["handle"], params["expression"], params["parameters"])
                    except TopologyBackendError as exc:
                        record_guard(action, "filter_update_transport_error", str(exc), kind="tool", before=before)
                        return
                    receipt = update.get("filter_receipt") or {}
                    update_event = emit("topology_filter_update_result", action_id=aid, logical_id=eid,
                               generation=gen, reader_gid=st["handle"].get("endpoint_gid"),
                               command_id=update.get("command_id"), receipt=receipt,
                               filter_effect_observed=None, fixture=not backend.real_ros)
                    action_evidence = None
                    evidence_error = None
                    if backend.real_ros or receipt.get("state_receipt") is not None:
                        from .filter_action_receipt import filter_action_evidence
                        try:
                            action_evidence = filter_action_evidence(
                                action, events, before_index=update_event["event_index"] + 1)
                        except (ValueError, KeyError, TypeError) as exc:
                            evidence_error = str(exc)
                    # An observed disabled state is not a successful empty setter
                    # or synthetic get readback. The command's original errors
                    # stay in filter_update_receipt, and no filtering hit is added.
                    legacy_fixture = not backend.real_ros and action_evidence is None and evidence_error is None
                    record_action(action, before, {**before, "filter_update_raw_ref": update_event["event_index"]},
                                  disposition=(action_evidence["disposition"] if action_evidence else
                                      "configured" if legacy_fixture and receipt.get("applied") is True else "unknown"),
                                  satisfied=(action_evidence is not None or
                                      legacy_fixture and receipt.get("applied") is True),
                                  requires=(action_evidence["requires"] if action_evidence else [] if legacy_fixture else
                                      [{"id": "raw_bound_filter_action_state", "status": "unknown"}]),
                                  filter_action_evidence=action_evidence, filter_action_error=evidence_error,
                                  filter_update_receipt=receipt, filter_effect_observed=None,
                                  execution_evidence=False)
                    return
                if op == "publish_batch":
                    if params.get("concurrency_group") in failed_concurrent_groups:
                        record_action(action, action_before, {k: state[eid][k] for k in ("generation", "alive", "next_sequence")},
                                      disposition="tool_error", satisfied=False,
                                      requires=[{"id": "concurrent_group_completed", "status": "unsatisfied"}],
                                      reason="concurrent_group_failed", side_effects="unknown_consult_raw_group",
                                      execution_evidence=False)
                        return
                    st = state[eid]
                    if not (st["alive"] and st["generation"] == gen and
                            endpoint_specs[eid]["role"] == "writer"):
                        record_guard(action, "writer_generation_alive_guard",
                                     "publish_batch requires a live writer generation", before=before)
                        return
                    count, start = params.get("count"), params.get("start_sequence")
                    if type(count) is not int or type(start) is not int:
                        record_guard(action, "invalid_batch_params", "count/start_sequence must be integers",
                                     before=before)
                        return
                    sequences = list(range(start, start + count))
                    if start < st["next_sequence"]:
                        record_guard(action, "nonoverlapping_writer_generation_sequence",
                                     "batch overlaps a previous sequence on this writer generation",
                                     details={"start_sequence": start,
                                              "next_sequence": st["next_sequence"]}, before=before)
                        return
                    interval = params.get("interval_sec", 0.0)
                    payload = params.get("payload")
                    if (isinstance(interval, bool) or not isinstance(interval, (int, float))
                            or not math.isfinite(interval) or interval < 0 or interval > MAX_INTERVAL_SEC):
                        record_guard(action, "publish_interval_out_of_bounds",
                                     "interval_sec must be finite in 0..%s" % MAX_INTERVAL_SEC,
                                     kind="capability",
                                     details={"interval_sec": interval}, before=before)
                        return
                    repeat_data = None
                    repeat_source = None
                    if params.get("repeat_data_from") is not None:
                        candidates = [e for e in events if e.get("event") == "send"
                                      and e.get("phase") == params["repeat_data_from"]
                                      and e.get("logical_id") == eid and e.get("generation") == gen
                                      and e.get("writer_gid") == st["handle"].get("endpoint_gid")
                                      and e.get("publish_returned") is True]
                        if len(candidates) != 1 or not isinstance(candidates[0].get("string_data"), str):
                            record_guard(action, "repeat_source_missing_or_ambiguous", "repeat requires one prior raw send from same writer generation", before=before)
                            return
                        repeat_source = candidates[0]
                        repeat_data = repeat_source["string_data"]
                    try:
                        # Matching unknown/incompatible is not a tool rejection:
                        # snapshots gate only command/identity integrity, while
                        # the close keeps an unestablished obligation incomplete.
                        group = params.get("concurrency_group")
                        if group in failed_concurrent_groups:
                            raise TopologyBackendError("concurrent_group_previously_failed")
                        if group is not None and aid not in concurrent_results:
                            members = [a for a in effective_program["actions"] if a.get("params", {}).get("concurrency_group") == group]
                            requests = []
                            for member in members:
                                ms = state[member["endpoint_id"]]
                                mp = member["params"]
                                completed = {e.get("action_id") for e in events if e.get("event") == "topology_action_done"
                                             and e.get("receipt", {}).get("satisfied") is True}
                                if any(dep not in completed for dep in member.get("depends_on", [])):
                                    raise TopologyBackendError("concurrent_dependency_not_satisfied")
                                if (not ms["alive"] or ms["generation"] != member["generation"]
                                        or endpoint_specs[member["endpoint_id"]]["role"] != "writer"
                                        or mp["start_sequence"] < ms["next_sequence"]):
                                    raise TopologyBackendError("concurrent_writer_guard")
                                seqs = list(range(mp["start_sequence"], mp["start_sequence"] + mp["count"]))
                                requests.append({"handle": ms["handle"], "sequences": seqs,
                                    "interval_sec": mp.get("interval_sec", 0.0), "phase": member["action_id"],
                                    "payload": mp.get("payload")})
                            if not hasattr(backend, "publish_concurrent"):
                                raise TopologyUnsupported("concurrent_backend_unavailable", "fixture/legacy backend has no concurrent API")
                            # Every member begins before any worker release, never backfill
                            # a second action's start after its API calls have run.
                            for member in members[1:]:
                                # Gate this member before its actual begin/API calls,
                                # not when the serial bookkeeping loop resumes later.
                                check_topology_source("before_action:" + member["action_id"])
                                ms = state[member["endpoint_id"]]
                                mb = {k: ms[k] for k in ("generation", "alive", "next_sequence")}
                                mi = next(i for i, a in enumerate(effective_program["actions"]) if a["action_id"] == member["action_id"])
                                emit("topology_action_begin", action_id=member["action_id"], action_index=mi,
                                     action=deepcopy(member), before=deepcopy(mb))
                                emit("state_step_before", step_id="action:" + member["action_id"], action=deepcopy(member),
                                     before=deepcopy(mb), after=None, requires=[], satisfied=None, fixture=not backend.real_ros)
                                concurrent_begun[member["action_id"]] = deepcopy(mb)
                            for request in requests:
                                prepare_delivery_windows(request["handle"], request["phase"], request["sequences"])
                            try:
                                concurrent_results.update(backend.publish_concurrent(group, requests))
                                from .topology_concurrency import audit_concurrency
                                observation = audit_concurrency(events, effective_program["actions"], group)
                                emit("topology_concurrency_observation", **observation)
                            except Exception as exc:
                                failed_concurrent_groups.add(group)
                                for request in requests:
                                    handle = request["handle"]
                                    interrupt_writer_windows(handle["logical_id"], "concurrent_group_failed")
                                    if handle.get("alive"):
                                        try:
                                            backend.destroy_endpoint(handle)
                                        except Exception as cleanup_error:
                                            emit("topology_tool_error", reason="concurrent_group_cleanup_failed",
                                                 concurrency_group=group, logical_id=handle["logical_id"],
                                                 error_type=type(cleanup_error).__name__)
                                    member_state = state[handle["logical_id"]]
                                    member_state["alive"] = handle.get("alive", True)
                                    # Preserve actual returned publishes even when completion
                                    # is missing; these remain raw observations, not a success ACK.
                                    sent = [e["send_sequence"] for e in events if e.get("event") == "send"
                                            and e.get("phase") == request["phase"]
                                            and e.get("logical_id") == handle["logical_id"]
                                            and e.get("generation") == handle["generation"]
                                            and e.get("writer_gid") == handle["endpoint_gid"]
                                            and e.get("publish_returned") is True]
                                    if sent:
                                        member_state["next_sequence"] = max(member_state["next_sequence"], max(sent) + 1)
                                emit("topology_tool_error", reason="concurrent_group_failed", concurrency_group=group,
                                     error_type=type(exc).__name__, error=str(exc))
                                raise TopologyBackendError("concurrent_group_failed:" + str(exc)) from exc
                        if group is None:
                            prepare_delivery_windows(st["handle"], aid, sequences)
                        kwargs = {"payload": payload}
                        if repeat_source is not None:
                            kwargs["repeat_string_data"] = repeat_data
                        publish = (concurrent_results.pop(aid) if group is not None else backend.publish_batch(
                            st["handle"], sequences, float(interval), aid, **kwargs))
                        if repeat_source is not None:
                            emit("topology_repeat_data_binding", source_send_ref=repeat_source["event_index"],
                                 writer=eid, writer_generation=gen, writer_gid=st["handle"].get("endpoint_gid"),
                                 source_action=params["repeat_data_from"], action_id=aid,
                                 occurrence_sequence=start, command_id=publish.get("command_id"))
                        close_delivery_windows(endpoint_id=eid, no_obligation_only=True)
                    except TopologyUnsupported as exc:
                        interrupt_writer_windows(eid, "publish_batch_unsupported")
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        return
                    except TopologyBackendTimeout as exc:
                        interrupt_writer_windows(eid, "publish_batch_timeout")
                        record_guard(action, "publish_batch_timeout", str(exc), kind="tool",
                                     details={"command_id": exc.command_id}, before=before)
                        return
                    except TopologyBackendError as exc:
                        interrupt_writer_windows(eid, "publish_batch_tool_error")
                        if params.get("concurrency_group") is not None:
                            record_action(action, before, {k: state[eid][k] for k in ("generation", "alive", "next_sequence")},
                                          disposition="tool_error", satisfied=False,
                                          requires=[{"id": "concurrent_group_completed", "status": "unsatisfied"}],
                                          reason=str(exc), side_effects="unknown_consult_raw_group",
                                          execution_evidence=False)
                        else:
                            record_guard(action, "publish_batch_error", type(exc).__name__,
                                         kind="tool", before=before)
                        return
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
                    return
                if op == "lookup_topic":
                    st = state[eid]
                    if not (st["alive"] and st["generation"] == gen):
                        record_guard(action, "lookup_target_alive_guard",
                                     "lookup requires a live current generation", before=before)
                        return
                    value = backend.lookup_topic(st["handle"], params["scope"], params["name"],
                        **({"native_type_support": params["native_type_support"]}
                           if "native_type_support" in params else {}))
                    emit("topic_lookup_observation", action_id=aid, logical_id=eid,
                         generation=gen, endpoint_gid=st["handle"]["endpoint_gid"],
                         lookup_receipt=deepcopy(value), fixture=not backend.real_ros,
                         candidate_eligible=False)
                    result["observation_receipts"].append({
                        "action_id": aid, "kind": "typed_topic_lookup", "receipt": deepcopy(value)})
                    observed = (value.get("complete") is True
                                and value.get("status") in ("found", "not_found"))
                    record_action(action, before, {**before, "lookup": value},
                                  disposition="effect_observed" if observed else "unknown",
                                  satisfied=observed,
                                  effect_scope="native_typed_topic_lookup_not_endpoint_matching",
                                  requires=[{"id": "runtime_typed_topic_lookup",
                                             "status": "satisfied" if observed else "unknown"}])
                    if not observed:
                        result["unknowns"].append({"action_id": aid,
                                                   "reason": value.get("reason", "topic_lookup_incomplete")})
                    return
                if op == "snapshot":
                    if eid is not None and not (state[eid]["alive"] and state[eid]["generation"] == gen):
                        record_guard(action, "snapshot_target_alive_guard",
                                     "snapshot requires a live current generation",
                                     before=before)
                        return
                    targets = [eid] if eid is not None else None
                    snapshot_start = len(events)
                    snapshot = snapshot_handles(targets)
                    if "episode" in effective_program:
                        episode_snapshots[aid] = (snapshot_start, snapshot)
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
                    return
                if op == "barrier":
                    condition = params.get("condition")
                    timeout = float(params.get("timeout_sec", 1.0))
                    explicit = params.get("endpoint_ids")
                    targets = explicit or list(state)
                    if condition in ("matched", "delivery", "delivery_sentinel") and explicit:
                        # Only model-declared barrier targets are preconditions; an
                        # implicit "all endpoints" scope stays an observation gap.
                        stale = [t for t in targets
                                 if not (state[t]["alive"] and state[t]["handle"] is not None)]
                        if stale:
                            record_guard(action, "barrier_target_alive_guard",
                                         "barrier requires live target endpoints",
                                         details={"stale_endpoints": stale}, before=before)
                            return
                    deadline = time.monotonic() + timeout
                    previous = None
                    satisfied = False
                    detail = {}
                    attempts = 0
                    if condition == "quiescent" and not any(state[eid]["alive"] for eid in targets):
                        record_guard(action, "barrier_target_alive_guard",
                                     "quiescent barrier requires at least one live endpoint",
                                     before=before)
                        return
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
                                events, previous=previous,
                                publications=[p for p in effective_program["actions"][:action_index]
                                              if p["action_id"] in params.get("publish_action_ids", [])])
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
                        return
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
                    return
                if op == "assert_liveliness":
                    st = state[eid]
                    spec = endpoint_specs[eid]
                    if not (st["alive"] and st["generation"] == gen and spec["role"] == "writer"):
                        record_guard(action, "writer_generation_alive_guard",
                                     "assert_liveliness requires a live writer generation", before=before)
                        return
                    policy = (spec.get("qos") or {}).get("liveliness")
                    if policy != "manual_by_topic":
                        record_guard(action, "public_writer_manual_by_topic",
                                     "assert_liveliness requires manual_by_topic policy",
                                     details={"liveliness": policy}, before=before)
                        return
                    try:
                        value = backend.assert_liveliness(st["handle"])
                    except TopologyUnsupported as exc:
                        record_guard(action, exc.code, str(exc), kind="capability",
                                     details=exc.details, before=before)
                        return
                    except TopologyBackendError as exc:
                        record_guard(action, "assert_liveliness_error", type(exc).__name__,
                                     kind="tool", before=before)
                        return
                    # The legacy action disposition describes the API action,
                    # not a DDS liveliness state transition.  No independently
                    # verified, writer-attributed lifecycle producer is wired.
                    # In particular backend-returned counts or raw-looking refs
                    # cannot be promoted into a recovery observation here.
                    lifecycle = {
                        "status": "unknown",
                        "loss_observed": None,
                        "recovery_observed": None,
                        "effect_observed": None,
                        "reason": "independent_writer_attributed_lifecycle_evidence_unavailable",
                        "candidate_eligible": False,
                    }
                    record_action(action, before, {**before, "assert": value},
                                  disposition="effect_observed", satisfied=True,
                                  effect_scope="assert_liveliness_api_action_only",
                                  lifecycle_evidence=lifecycle,
                                  requires=[{"id": "public_writer_manual_by_topic",
                                             "status": "satisfied"}])
                    return
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

        if "episode" not in effective_program:
            for action_index, action in enumerate(effective_program["actions"]):
                execute_action(action_index, action)
        else:
            from r2f.campaign.episode import EpisodeStore, run_episode
            from r2f.campaign.topology_episode import (
                capture_prefix, reset_from_events, topology_episode,
            )
            episode_root = output_dir / "episode"
            initial_after = state_boundary("episode_initial")
            initial_projection, initial_prefix = capture_prefix(
                raw, raw_path, episode_root, "initial",
                before_ref=initial_boundary["event_index"],
                after_ref=initial_after["event_index"], program=effective_program)
            episode = topology_episode(effective_program, cfg, initial_projection["state_after"])
            episode_store = EpisodeStore(episode_root)
            emit("topology_episode_begin", episode_id=episode.episode_id,
                 input_sha256=episode.input_identity(), initial_prefix=initial_prefix,
                 restoration_supported=False, candidate_eligible=False)
            if episode_rebuild is not None:
                emit("topology_episode_rebuild_begin", request=cfg["episode_rebuild"],
                     origin_raw=episode_rebuild["origin_raw"],
                     checkpoint_action_id=cfg["episode_rebuild"]["checkpoint_action_id"],
                     mode="public_api_prefix_rebuild_then_suffix", DDS_memory_restored=False,
                     hidden_state_equivalence_claimed=False, candidate_eligible=False)
            previous_boundary = initial_after["event_index"]
            checkpoints = set(effective_program["episode"]["checkpoint_action_ids"])
            resets = set(effective_program["episode"].get("reset_action_ids", []))

            def perform_episode_action(episode_action, observed_state):
                nonlocal previous_boundary
                action = effective_program["actions"][episode_action.sequence]
                execute_action(episode_action.sequence, action)
                snapshot_start, snapshot = episode_snapshots.get(action["action_id"], (None, None))
                if action["action_id"] in checkpoints:
                    close_delivery_windows()
                after_boundary = state_boundary("episode_after:" + action["action_id"],
                    snapshot, snapshot_start_ref=snapshot_start)
                projected, prefix = capture_prefix(raw, raw_path, episode_root,
                    f"action-{episode_action.sequence:04d}", before_ref=previous_boundary,
                    after_ref=after_boundary["event_index"], program=effective_program)
                # A previous immutable cut must remain the same observed state
                # when projected from the longer prefix; never predict it.
                if projected["state_before"] != observed_state:
                    raise TopologyBackendError("episode_observed_state_chain_changed")
                previous_boundary = after_boundary["event_index"]
                terminal = [e for e in events if e.get("event") == "topology_action_done"
                            and e.get("action_id") == action["action_id"]][-1]
                receipt = terminal["receipt"]
                observation = {"prefix": prefix, "action_done_ref": terminal["event_index"],
                    "runtime_action_receipt": deepcopy(receipt),
                    "observation_complete": projected["state_after"]["observation_completeness"] == "complete",
                    "execution_evidence": projected["receipt"]["execution_evidence"],
                    "candidate_eligible": False}
                if action["action_id"] in resets:
                    observation["reset"] = reset_from_events(events, action, prefix)
                rejected = receipt.get("disposition") in {
                    "guard_reject", "capability_reject", "tool_error", "environment_unverified"}
                return {"status": "failed" if rejected else "executed",
                    "execution_stage": "aborted" if rejected else "effect_observed",
                    "after_state": projected["state_after"], "oracle_verdict": "unknown",
                    "responsibility_layer": "harness" if receipt.get("disposition") == "tool_error" else "observation",
                    "reason_code": receipt.get("code", ""), "observation": observation}

            def commit_episode_action(receipt, receipts):
                committed = episode_store.commit_progress(episode, receipts,
                    checkpoint=receipt["action_id"] in checkpoints)
                emit("topology_episode_action_committed", episode_id=episode.episode_id,
                     action_id=receipt["action_id"], sequence=receipt["sequence"],
                     receipt_sha256=sha256_json(receipt), progress=committed,
                     checkpoint=receipt["action_id"] in checkpoints,
                     restoration_supported=False, candidate_eligible=False)
                raw.flush()
                os.fsync(raw.fileno())
                if (episode_rebuild is not None
                        and receipt["sequence"] == episode_rebuild["request"]["sequence"]):
                    from r2f.campaign.topology_episode_rebuild import validate_reconstructed_checkpoint
                    validation = validate_reconstructed_checkpoint(episode_rebuild, receipt, events)
                    emit("topology_episode_rebuild_validated", validation=validation,
                         checkpoint_commit_ref=len(events)-1, candidate_eligible=False)
                    # The semantic match and fresh-incarnation proof must be
                    # durable BEFORE run_episode can start any suffix action.
                    raw.flush()
                    os.fsync(raw.fileno())

            execution = run_episode(episode, perform=perform_episode_action,
                clock=time.monotonic, on_receipt=commit_episode_action)
            saved = episode_store.add(episode, execution)
            result["episode"] = {"episode_id": episode.episode_id,
                "input_sha256": episode.input_identity(), "execution": execution,
                "store_root": str(episode_root), "record_sha256": saved["record_sha256"],
                "restoration_supported": False, "candidate_eligible": False}
            emit("topology_episode_end", episode_id=episode.episode_id,
                 input_sha256=episode.input_identity(), record_sha256=saved["record_sha256"],
                 store_root=str(episode_root), stop_reason=execution["stop_reason"],
                 action_count=execution["action_count"], restoration_supported=False,
                 candidate_eligible=False)
            if execution["stop_reason"] != "episode_completed":
                raise TopologyBackendError("episode_stopped:" + str(execution["stop_reason"]))

        check_topology_source("after_actions")
        # Final observation of the surviving generations before cleanup.
        # No further publish action is legal after this committed raw prefix.
        inventory = deepcopy(events)
        emit("topology_send_inventory_end", schema="r2f.topology-send-inventory/v1",
             inventory_count=len(inventory),
             inventory_sha256=hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
             action_done_refs=[e["event_index"] for e in inventory if e.get("event") == "topology_action_done"])
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
            if not isinstance(cleanup_receipt, dict):
                raise TypeError("invalid_cleanup_receipt")
        except Exception as exc:  # backend cleanup must leave a failed raw receipt
            cleanup_receipt = {"closed": False, "error": type(exc).__name__}
            result["status"] = "aborted"
            result["runtime_status"] = "aborted"
            result["reason"] = result["reason"] or "cleanup_failed:" + type(exc).__name__
        result["cleanup_receipt"] = cleanup_receipt
        emit("topology_cleanup", receipt=cleanup_receipt)
        cleanup_complete = _cleanup_is_complete(cleanup_receipt, backend, netem_ctx=None)
        if not cleanup_complete:
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
        # Consume only committed raw window/worker rows after worker cleanup, so
        # late callbacks cannot be silently omitted from the comparison.
        result["repeat_data_observations"] = []
        for binding in [e for e in events if e.get("event") == "topology_repeat_data_binding"]:
            observation = _repeat_data_observation(binding, events)
            result["repeat_data_observations"].append(emit("topology_repeat_data_observation", **observation,
                                                           fixture=not backend.real_ros))
        result["dynamic_filter_effects"] = []
        for update in [e for e in events if e.get("event") == "topology_filter_update_result"]:
            observation = _dynamic_cft_effect(update, events, real_ros=bool(backend.real_ros))
            raw_observation = emit("topology_dynamic_filter_effect", **observation,
                                   fixture=not backend.real_ros, execution_evidence=bool(backend.real_ros))
            result["dynamic_filter_effects"].append(raw_observation)
        result["liveliness_peer_samples"] = []
        for observation in evaluate_peer_samples(
                events, run_id=cfg["run_id"], fixture=not backend.real_ros):
            result["liveliness_peer_samples"].append(
                emit("topology_liveliness_peer_evidence", **observation,
                     fixture=not backend.real_ros))
        # This is a final raw-backed consumer, not a lifecycle producer.  It
        # deliberately remains unknown until a trusted continuous peer
        # inventory is present; the API call alone never becomes a DDS effect.
        result["liveliness_evidence"] = []
        for observation in _liveliness_observation_facts(
                events, run_id=cfg["run_id"], real_ros=bool(backend.real_ros)):
            result["liveliness_evidence"].append(
                emit("topology_liveliness_evidence", **observation,
                     fixture=not backend.real_ros))
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
        cleanup_complete = _cleanup_is_complete(
            cleanup_receipt, backend, netem_ctx, netem_required=netem_cleanup_required)
        if not cleanup_complete and result["status"] != "aborted":
            result.update(status="aborted", runtime_status="aborted",
                          reason=result["reason"] or "cleanup_incomplete")
            emit("runtime_abort", reason=result["reason"], phase="cleanup")
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
        # Final raw assertion is made only after restore and lease disposition.
        # A release exception cannot fall through to a successful runtime_end.
        cleanup_complete = _cleanup_is_complete(
            cleanup_receipt, backend, netem_ctx, netem_required=netem_cleanup_required)
        if result["status"] == "aborted":
            state_boundary("aborted_cleanup")
        # A provisional end-of-program exemption must survive the entire worker
        # shutdown. Late sends, missing exits or identity drift keep UNKNOWN;
        # cleanup cannot retroactively invent a different delivery boundary.
        for claim in [e for e in events if e.get("event") == "topology_delivery_no_sends"]:
            try:
                writer = events[claim["writer_creation_ref"]]
                reader = events[claim["reader_creation_ref"]]
                chains = {eid: [events[i] for i in refs] for eid, refs in claim["snapshot_refs"].items()}
                regenerated = claim.get("schema") == "r2f.topology-delivery-no-sends/v2"
                validator = _delivery_recreated_no_sends_basis if regenerated else _delivery_no_sends_basis
                basis = validator(writer, reader, effective_program, events, chains)
                valid = cleanup_complete and basis is not None and all(claim.get(k) == v for k, v in basis.items())
                from .participant_groups import enabled as group_control_enabled
                if group_control_enabled(effective_program):
                    group_termination_rows(effective_program, events, raw_path, (writer, reader), claim)
                else:
                    for token in (writer, reader):
                        exits = [e for e in events if e.get("event") == "topology_worker_exit"
                                 and e.get("logical_id") == token["logical_id"]
                                 and (not regenerated or e.get("generation") == token["generation"])]
                        valid = valid and len(exits) == 1 and exits[0].get("returncode") == 0 and all(
                            exits[0].get(k) == token[k] for k in
                            ("generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation"))
                        valid = valid and exits[0]["event_index"] > claim["event_index"]
            except (KeyError, IndexError, TypeError, ValueError):
                valid = False
            if not valid:
                result["unknowns"].append({"action_id": "finalize", "reason": "no_sends_generation_end_incomplete",
                                           "raw_event_ref": claim["event_index"]})
                if result["status"] == "executed":
                    result["runtime_status"] = "completed_with_unknown"
        ended_ns = _now()
        result["precondition_receipts"].append({
            "step_id": "cleanup", "receipt_kind": "precondition",
            "requires": [{"id": "full_process_tree_closed", "kind": "cleanup",
                          "status": "satisfied" if cleanup_complete else "unknown"}],
            "before": None, "after": cleanup_receipt,
            "satisfied": cleanup_complete,
            "disposition": "observed" if cleanup_complete else "unknown",
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
        workers = _execution_workers(events, raw_path, effective_program, config=cfg)
        from .participant_groups import enabled, lineage_metadata
        group_lineage = (lineage_metadata(effective_program, events, raw_path, config=cfg)
                         if enabled(effective_program) else None)
        if group_lineage is not None:
            result['observation_receipts'].extend(deepcopy(group_lineage['observations']))
        actual_workers = [w for w in workers if w["real_ros"]]
        actual_rmws = {w["rmw_identifier"] for w in actual_workers if w["rmw_identifier"] is not None}
        actual_domains = {w["domain_id"] for w in actual_workers if w["domain_id"] is not None}
        result["execution_metadata"] = {
            "schema": ("r2f.topology-execution-metadata/v2" if group_lineage is not None
                       else "r2f.topology-execution-metadata/v1"),
            **({'participant_groups': group_lineage} if group_lineage is not None else {}),
            "backend": backend.backend_name,
            "real_ros": bool(result["real_execution"]),
            "source_identity": deepcopy(result["environment"].get("source")) if backend.real_ros else None,
            "source_manifest": deepcopy(source_before),
            "source_after": deepcopy(next((e.get("source") for e in reversed(events)
                                            if e.get("event") == "topology_source_checked"), None)),
            "domain_id": next(iter(actual_domains)) if len(actual_domains) == 1 and all(w["domain_id"] is not None for w in actual_workers) else None,
            "rmw": next(iter(actual_rmws)) if len(actual_rmws) == 1 and all(w["rmw_identifier"] is not None for w in actual_workers) else None,
            "actual_rmws": sorted(actual_rmws), "actual_domains": sorted(actual_domains),
            "observation_mode": cfg.get("observation_mode", "blackbox"),
            "fastdds_peer_instrumentation": deepcopy(cfg.get("fastdds_peer_instrumentation")),
            **({"observe_native_peers": cfg["observe_native_peers"]}
               if "observe_native_peers" in cfg else {}),
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
             cleanup_complete=cleanup_complete,
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
            "dynamic_filter_effects": deepcopy(result.get("dynamic_filter_effects", [])),
            "repeat_data_observations": deepcopy(result.get("repeat_data_observations", [])),
            "liveliness_evidence": deepcopy(result.get("liveliness_evidence", [])),
            "liveliness_peer_samples": deepcopy(result.get("liveliness_peer_samples", [])),
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
