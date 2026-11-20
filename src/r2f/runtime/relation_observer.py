"""Per-edge relation observation from public-API raw events (no ROS imports).

The existing ``ros2_pair`` runtime records endpoint creation, QoS binding,
graph snapshots, role-scoped match counts and per-sample delivery rows.  This
module turns that raw stream into *per writer/reader edge* relation facts for
G01 and W01.  It never starts ROS, never opens a context/endpoint/participant,
never creates a campaign, candidate or report, and never emits an oracle
verdict.  It returns facts/events/receipts for the unified provider.

Hard rules enforced here:

* No aggregate match count is used as evidence.  A ``match_snapshot`` row only
  counts for an edge when it is bound to that role's exact endpoint GID, was
  sampled after reader creation, and - when it carries a ``command_id`` - has a
  matching ``command_done`` with ordered timestamps.  The resulting count is
  recorded as ``role_scoped_match_count``.
* A pair edge is isolated only when command-bound graph snapshots show exactly
  this writer GID in ``publishers`` and exactly this reader GID in
  ``subscriptions``.  Extra endpoints make the edge ``pair_not_isolated`` and
  its match evidence unknown; delivery rows can still be attributed per GID.
* Missing evidence is ``unknown``/``incomplete``; it is never promoted to
  compatible, matched, delivered or a pass.
* ``history``/``depth`` never imply DDS compatibility.  W01 depth-vs-post-count
  closure is only applicability-gated, and the old delivery obligation is not
  re-approved for ``depth_lt_post``.
* Endpoint recreation is not guessed: a role with more than one creation row is
  ``role_generation_ambiguous`` unless the caller names an explicit generation.
"""
from __future__ import annotations

import hashlib
import json

from r2f.modules.s1.relation_surfaces import (
    G01_INCOMPATIBLE_PAIRS,
    RelationSurfaceError,
    canonical_sha256,
    relation_compatibility,
)

SCHEMA = "r2f.s1.relation-observer/v1"
OBSERVER_ID = "runtime.relation_observer"
DEFAULT_WRITER_ROLES = ("writer",)
DEFAULT_READER_ROLES = ("reader",)

_KNOWN_QOS_FIELDS = ("reliability", "durability", "deadline_ns", "liveliness",
                     "liveliness_lease_duration_ns", "history", "depth")
_UNAVAILABLE_QOS_FIELDS = ("ownership", "ownership_strength", "partitions")


def _plain(value):
    if hasattr(value, "to_dict"):
        return _plain(value.to_dict())
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or type(value) in (str, int, bool, float):
        return value
    raise RelationSurfaceError("not_json_serializable", type(value).__name__)


def _row_ref(event):
    if not isinstance(event, dict):
        raise RelationSurfaceError("raw_event_not_an_object")
    return {
        "event_index": event.get("event_index"),
        "event": event.get("event"),
        "role": event.get("role"),
        "event_sha256": canonical_sha256(event),
    }


def _digest_rows(rows):
    return hashlib.sha256(json.dumps([_row_ref(row) for row in rows],
                                     sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def _require_event_indexes(events):
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise RelationSurfaceError("raw_event_not_an_object")
        if type(event.get("event_index")) is not int or event["event_index"] != index:
            raise RelationSurfaceError("raw_event_index_not_contiguous", str(index))


def _validate_run(events, run_id=None, domain_id=None):
    for event in events:
        if run_id is not None and event.get("run_id") not in (None, run_id):
            raise RelationSurfaceError("raw_run_id_mismatch", str(event.get("run_id")))
        if domain_id is not None and event.get("domain_id") not in (None, domain_id):
            raise RelationSurfaceError("raw_domain_id_mismatch", str(event.get("domain_id")))


def _endpoint_creations(events):
    by_role = {}
    for event in events:
        if event.get("event") == "endpoint_created":
            by_role.setdefault(event.get("role"), []).append(event)
    return by_role


def _select_generation(rows, role, generation):
    if generation is None:
        if len(rows) == 1:
            return rows[0], None
        return None, "role_generation_ambiguous:%s:%d" % (role, len(rows))
    selected = [row for row in rows if row.get("endpoint_generation") == generation]
    if len(selected) != 1:
        return None, "explicit_generation_not_found:%s:%r" % (role, generation)
    return selected[0], None


def _policy_from_binding(events, role, gid):
    """Latest GID-bound public QoS observation for one role."""
    rows = [event for event in events
            if event.get("event") == "qos_binding_observed"
            and event.get("role") == role
            and event.get("observed_self_gid") == gid
            and isinstance(event.get("observed_self"), dict)]
    if not rows:
        return None, "qos_binding_observation_missing", None
    latest = rows[-1]
    observed = latest["observed_self"]
    return ({field: observed[field] for field in _KNOWN_QOS_FIELDS if field in observed},
            None, latest)


def _policy_from_graph(events, role, gid):
    """Last-resort graph-only policy for fields every graph entry reports."""
    rows = []
    for event in events:
        if event.get("event") != "graph_snapshot":
            continue
        for table in ("publishers", "subscriptions"):
            for row in event.get(table, []):
                if row.get("endpoint_gid") == gid:
                    rows.append((event, row))
    if not rows:
        return None, "graph_endpoint_entry_missing", None
    event, row = rows[-1]
    qos = row.get("qos")
    if not isinstance(qos, dict):
        return None, "graph_qos_entry_missing", None
    return ({field: qos[field] for field in _KNOWN_QOS_FIELDS if field in qos},
            None, event)


def _policy_view(policy, source, event, role):
    if policy is None:
        return {"role": role, "source": None, "fields": {}, "values": {}, "complete": False,
                "missing_fields": list(_KNOWN_QOS_FIELDS),
                "unavailable_fields": list(_UNAVAILABLE_QOS_FIELDS)}
    fields = {}
    for field in _KNOWN_QOS_FIELDS:
        if field in policy:
            fields[field] = {"value": policy[field], "source": source,
                             "event_ref": _row_ref(event)}
    return {
        "role": role,
        "source": source,
        "fields": fields,
        "values": {name: item["value"] for name, item in fields.items()},
        "complete": all(name in fields for name in
                        ("reliability", "durability", "deadline_ns", "liveliness",
                         "liveliness_lease_duration_ns")),
        "missing_fields": [name for name in _KNOWN_QOS_FIELDS if name not in fields],
        "unavailable_fields": list(_UNAVAILABLE_QOS_FIELDS),
    }


def _command_bound_snapshot(events, role, gid, not_before_ns):
    """Return the latest snapshot whose command has an ordered ACK.

    ``snapshot_kind`` is ``match_snapshot`` or ``graph_snapshot``.  Both are
    produced by the same worker command, so one command_id can prove both.
    """
    commands = {}
    for event in events:
        command_id = event.get("command_id")
        if event.get("role") != role or type(command_id) is not int:
            continue
        entry = commands.setdefault(command_id, {})
        entry.setdefault(event.get("event"), []).append(event)
    proofs = []
    for command_id, entry in commands.items():
        match_rows = entry.get("match_snapshot", [])
        graph_rows = entry.get("graph_snapshot", [])
        ack_rows = entry.get("command_done", [])
        if len(match_rows) != 1 or len(graph_rows) != 1 or len(ack_rows) != 1:
            continue
        match, graph, ack = match_rows[0], graph_rows[0], ack_rows[0]
        stamps = [match.get("sampled_monotonic_ns"), match.get("monotonic_ns"),
                  graph.get("monotonic_ns"), ack.get("monotonic_ns")]
        indexes = [match.get("event_index"), graph.get("event_index"),
                   ack.get("event_index")]
        if not all(type(stamp) is int for stamp in stamps):
            continue
        if not all(type(index) is int for index in indexes):
            continue
        if not (stamps[0] <= stamps[1] <= stamps[2] <= stamps[3]):
            continue
        if not (indexes[0] < indexes[1] < indexes[2]):
            continue
        if match.get("endpoint_gid") != gid:
            continue
        if type(not_before_ns) is int and stamps[0] < not_before_ns:
            continue
        proofs.append({"command_id": command_id, "match": match, "graph": graph,
                       "ack": ack, "sampled_ns": stamps[0]})
    return proofs


def _graph_isolation(graph, writer_gid, reader_gid):
    publishers = {row.get("endpoint_gid") for row in graph.get("publishers", [])}
    subscriptions = {row.get("endpoint_gid") for row in graph.get("subscriptions", [])}
    if publishers == {writer_gid} and subscriptions == {reader_gid}:
        return "exact_pair", publishers, subscriptions
    if writer_gid not in publishers or reader_gid not in subscriptions:
        return "missing_endpoint", publishers, subscriptions
    return "extra_endpoints", publishers, subscriptions


def _pair_graph_evidence(events, writer, reader, writer_role, reader_role):
    """Both roles must independently report the exact pair in fresh graphs."""
    proofs = {}
    reasons = []
    for role, gid, prefix in ((writer_role, writer.get("endpoint_gid"), "writer"),
                              (reader_role, reader.get("endpoint_gid"), "reader")):
        candidates = _command_bound_snapshot(events, role, gid, reader.get("created_monotonic_ns"))
        if not candidates:
            reasons.append("command_bound_graph_snapshot_missing:" + role)
            continue
        chosen = candidates[-1]
        isolation, publishers, subscriptions = _graph_isolation(
            chosen["graph"], writer.get("endpoint_gid"), reader.get("endpoint_gid"))
        proofs[prefix] = {
            "role": role,
            "command_id": chosen["command_id"],
            "isolation": isolation,
            "publishers": sorted(gid for gid in publishers if isinstance(gid, str)),
            "subscriptions": sorted(gid for gid in subscriptions if isinstance(gid, str)),
            "graph_event_ref": _row_ref(chosen["graph"]),
            "match_event_ref": _row_ref(chosen["match"]),
            "ack_event_ref": _row_ref(chosen["ack"]),
            "sampled_ns": chosen["sampled_ns"],
        }
        if isolation != "exact_pair":
            reasons.append("pair_not_isolated:" + role + ":" + isolation)
    status = "exact_pair" if len(proofs) == 2 and not reasons else (
        "incomplete" if reasons else "unknown")
    return {"status": status, "proofs": proofs, "reasons": reasons}


def _match_evidence(events, writer, reader, writer_role, reader_role, graph_status):
    if graph_status != "exact_pair":
        return {"status": "unknown", "basis": None,
                "reason": "graph_pair_isolation_required_before_match_evidence"}
    counts = {}
    refs = {}
    times = []
    for role, gid in ((writer_role, writer.get("endpoint_gid")),
                      (reader_role, reader.get("endpoint_gid"))):
        candidates = _command_bound_snapshot(events, role, gid, reader.get("created_monotonic_ns"))
        if not candidates:
            return {"status": "unknown", "basis": None,
                    "reason": "command_bound_match_snapshot_missing:" + role}
        chosen = candidates[-1]
        count = chosen["match"].get("matched_count")
        if type(count) is not int or count < 0:
            return {"status": "unknown", "basis": None,
                    "reason": "invalid_role_scoped_match_count:" + role}
        counts[role] = count
        refs[role] = {"match_event_ref": _row_ref(chosen["match"]),
                      "ack_event_ref": _row_ref(chosen["ack"]),
                      "sampled_ns": chosen["sampled_ns"]}
        times.append(chosen["sampled_ns"])
    if any(count != 1 for count in counts.values()):
        return {"status": "not_matched" if all(count == 0 for count in counts.values())
                else "ambiguous", "basis": "paired_role_scoped_command_bound_snapshots",
                "counts": counts, "refs": refs,
                "aggregate_used": False,
                "reason": "role_scoped_counts_not_one"}
    return {"status": "observed",
            "basis": "paired_role_scoped_command_bound_snapshots",
            "counts": counts, "refs": refs,
            "at_ns": max(times),
            "aggregate_used": False,
            "note": "role-scoped counts from exactly the two endpoint GIDs; never a global total"}


def _delivery_evidence(events, writer, reader, writer_role, reader_role, run_id):
    writer_gid, reader_gid = writer.get("endpoint_gid"), reader.get("endpoint_gid")
    sends, receives, reasons, identity_reasons = [], [], [], []
    for event in events:
        if event.get("event") == "send" and event.get("role") == writer_role:
            if event.get("writer_gid") not in (None, writer_gid):
                reasons.append("send_writer_gid_mismatch")
                identity_reasons.append("send_writer_gid_mismatch")
                continue
            if event.get("publish_returned") is not True:
                reasons.append("send_without_publish_return")
                continue
            sends.append(event)
        elif event.get("event") == "receive" and event.get("role") == reader_role:
            if event.get("reader_gid") not in (None, reader_gid):
                reasons.append("receive_reader_gid_mismatch")
                identity_reasons.append("receive_reader_gid_mismatch")
                continue
            if run_id is not None and event.get("payload_run_id") != run_id:
                reasons.append("receive_payload_run_mismatch")
                identity_reasons.append("receive_payload_run_mismatch")
                continue
            publisher_gid = event.get("publisher_gid")
            if publisher_gid is not None and publisher_gid != writer_gid:
                reasons.append("receive_publisher_gid_mismatch")
                identity_reasons.append("receive_publisher_gid_mismatch")
                continue
            if publisher_gid is None and event.get("identity_basis") != "endpoint_correlated":
                reasons.append("receive_identity_unavailable_without_explicit_correlation")
                identity_reasons.append(
                    "receive_identity_unavailable_without_explicit_correlation")
                continue
            receives.append(event)
    sent_sequences = [event.get("send_sequence") for event in sends]
    received_sequences = [event.get("send_sequence") for event in receives]
    if any(type(value) is not int or value < 0 for value in sent_sequences + received_sequences):
        reasons.append("invalid_delivery_sequence")
    if len(set(sent_sequences)) != len(sent_sequences):
        reasons.append("duplicate_send_sequence")
    if len(set(received_sequences)) != len(received_sequences):
        reasons.append("duplicate_receive_sequence")
    sent_set, received_set = set(sent_sequences), set(received_sequences)
    missing = sorted(sent_set - received_set)
    max_received = max(received_set, default=None)
    proven_gaps = ([value for value in missing if max_received is not None and value < max_received]
                   if max_received is not None else [])
    status = "unknown" if reasons else ("observed" if receives else "none")
    return {
        "status": status,
        "sent": [{"sequence": event.get("send_sequence"),
                  "at_ns": event.get("monotonic_ns"),
                  "phase": event.get("phase"),
                  "event_ref": _row_ref(event)} for event in sends],
        "received": [{"sequence": event.get("send_sequence"),
                      "at_ns": event.get("monotonic_ns"),
                      "phase": event.get("phase"),
                      "event_ref": _row_ref(event)} for event in receives],
        "sent_sequences": sent_sequences,
        "received_sequences": received_sequences,
        "missing_sequences": missing,
        "proven_gaps": proven_gaps,
        "max_received_sequence": max_received,
        "identity_correlated": bool(receives) and not identity_reasons,
        "identity_reasons": sorted(set(identity_reasons)),
        "reasons": reasons,
        "note": "per-edge rows filtered by writer/reader GID; no aggregate counter",
    }


def _submitted_policies(config, writer_role, reader_role):
    if not isinstance(config, dict):
        return None
    writer = config.get(writer_role + "_qos")
    reader = config.get(reader_role + "_qos")
    if not isinstance(writer, dict) or not isinstance(reader, dict):
        return None
    return {"writer": {field: writer[field] for field in _KNOWN_QOS_FIELDS if field in writer},
            "reader": {field: reader[field] for field in _KNOWN_QOS_FIELDS if field in reader}}


def _declared_and_observed_relation(observed_writer, observed_reader, submitted):
    declared = None
    if submitted is not None:
        declared = relation_compatibility(submitted["writer"], submitted["reader"])
    observed = relation_compatibility(observed_writer, observed_reader)
    combined = "unknown"
    if observed["status"] == "incompatible":
        combined = "incompatible"
    elif observed["status"] == "compatible":
        combined = "compatible"
    return {"declared": declared, "observed": observed, "status": combined,
            "declared_is_not_observation": True}


def g01_edge_observation(events, config=None, *, writer_role="writer",
                         reader_role="reader", writer_generation=None,
                         reader_generation=None):
    """Build one or more per-pair G01 observations from a raw event list."""
    if not isinstance(events, list):
        raise RelationSurfaceError("events_must_be_a_list")
    _require_event_indexes(events)
    run_id = config.get("run_id") if isinstance(config, dict) else None
    domain_id = config.get("domain_id") if isinstance(config, dict) else None
    _validate_run(events, run_id, domain_id)
    creations = _endpoint_creations(events)
    writer, reason = _select_generation(creations.get(writer_role, []), writer_role,
                                       writer_generation)
    if writer is None:
        return {"schema": SCHEMA, "observer": OBSERVER_ID, "kind": "g01_edges",
                "edges": [], "status": "incomplete", "reasons": [reason],
                "aggregate_used": False, "execution_evidence": False,
                "oracle_verdict": None, "candidate_eligible": False}
    reader, reason = _select_generation(creations.get(reader_role, []), reader_role,
                                       reader_generation)
    if reader is None:
        return {"schema": SCHEMA, "observer": OBSERVER_ID, "kind": "g01_edges",
                "edges": [], "status": "incomplete", "reasons": [reason],
                "aggregate_used": False, "execution_evidence": False,
                "oracle_verdict": None, "candidate_eligible": False}

    reasons = []
    if writer.get("pid") == reader.get("pid") or not writer.get("pid") or not reader.get("pid"):
        reasons.append("endpoint_processes_not_independent")
    for name, row in (("writer", writer), ("reader", reader)):
        gid = row.get("endpoint_gid")
        if not isinstance(gid, str) or not gid:
            reasons.append("endpoint_gid_missing:" + name)
        if row.get("endpoint_generation") in (None, ""):
            reasons.append("endpoint_generation_missing:" + name)

    writer_policy, writer_reason, writer_event = _policy_from_binding(events, writer_role, writer.get("endpoint_gid"))
    if writer_policy is None:
        writer_policy, writer_reason, writer_event = _policy_from_graph(events, writer_role, writer.get("endpoint_gid"))
    reader_policy, reader_reason, reader_event = _policy_from_binding(events, reader_role, reader.get("endpoint_gid"))
    if reader_policy is None:
        reader_policy, reader_reason, reader_event = _policy_from_graph(events, reader_role, reader.get("endpoint_gid"))
    writer_view = _policy_view(writer_policy, "qos_binding_observed" if writer_event and writer_event.get("event") == "qos_binding_observed" else "graph_snapshot", writer_event, writer_role)
    reader_view = _policy_view(reader_policy, "qos_binding_observed" if reader_event and reader_event.get("event") == "qos_binding_observed" else "graph_snapshot", reader_event, reader_role)
    if writer_policy is None:
        reasons.append("writer_policy_observation_missing:" + str(writer_reason))
    if reader_policy is None:
        reasons.append("reader_policy_observation_missing:" + str(reader_reason))

    graph = _pair_graph_evidence(events, writer, reader, writer_role, reader_role)
    reasons.extend(graph["reasons"])
    match = _match_evidence(events, writer, reader, writer_role, reader_role, graph["status"])
    if match["status"] in ("unknown", "ambiguous"):
        reasons.append("match_" + match["status"] + ":" + str(match.get("reason")))
    delivery = _delivery_evidence(events, writer, reader, writer_role, reader_role, run_id)
    reasons.extend(delivery["reasons"])
    submitted = _submitted_policies(config, writer_role, reader_role)
    relation = _declared_and_observed_relation(writer_view["values"], reader_view["values"],
                                              submitted)
    identity_correlated = (not reasons and graph["status"] == "exact_pair"
                           and match["status"] == "observed"
                           and delivery["status"] == "observed"
                           and delivery["identity_correlated"])
    edge_id = canonical_sha256({
        "writer": {"role": writer_role, "gid": writer.get("endpoint_gid"),
                   "generation": writer.get("endpoint_generation")},
        "reader": {"role": reader_role, "gid": reader.get("endpoint_gid"),
                   "generation": reader.get("endpoint_generation")},
    })
    edge = {
        "schema": SCHEMA,
        "observer": OBSERVER_ID,
        "kind": "g01_edge_observation",
        "edge_id": edge_id,
        "writer": {"role": writer_role, "gid": writer.get("endpoint_gid"),
                   "generation": writer.get("endpoint_generation"),
                   "pid": writer.get("pid"),
                   "created_ns": writer.get("created_monotonic_ns"),
                   "endpoint_event_ref": _row_ref(writer)},
        "reader": {"role": reader_role, "gid": reader.get("endpoint_gid"),
                   "generation": reader.get("endpoint_generation"),
                   "pid": reader.get("pid"),
                   "created_ns": reader.get("created_monotonic_ns"),
                   "endpoint_event_ref": _row_ref(reader)},
        "submitted_policies": submitted,
        "observed_policies": {"writer": writer_view, "reader": reader_view},
        "binding": {
            "writer": {"source": writer_view["source"],
                       "complete": writer_view["complete"],
                       "missing_fields": writer_view["missing_fields"]},
            "reader": {"source": reader_view["source"],
                       "complete": reader_view["complete"],
                       "missing_fields": reader_view["missing_fields"]},
        },
        "graph": graph,
        "match": match,
        "delivery": delivery,
        "relation": relation,
        "incomplete_reasons": sorted(set(reasons)),
        "identity_correlated": identity_correlated,
        "pair_identity_correlated": not reasons and graph["status"] == "exact_pair"
                                    and match["status"] == "observed",
        "aggregate_used": False,
        "execution_evidence": False,
        "oracle_verdict": None,
        "candidate_eligible": False,
    }
    status = ("observed" if identity_correlated else
              "incomplete" if reasons else "partially_observed")
    return {"schema": SCHEMA, "observer": OBSERVER_ID, "kind": "g01_edges",
            "edges": [edge], "status": status,
            "reasons": sorted(set(reasons)), "aggregate_used": False,
            "execution_evidence": False, "oracle_verdict": None,
            "candidate_eligible": False}


def window_from_events(events, name):
    """Controller-declared observation window; absent means unknown, not zero."""
    opens = [event for event in events if event.get("event") == "window_open"
             and event.get("window") == name]
    closes = [event for event in events if event.get("event") == "window_close"
              and event.get("window") == name]
    if not opens or not closes:
        return None
    opening, closing = opens[-1], closes[-1]
    start, end = opening.get("monotonic_ns"), closing.get("monotonic_ns")
    if type(start) is not int or type(end) is not int or end <= start:
        return None
    return {"start_ns": start, "end_ns": end, "complete": True,
            "open_event_ref": _row_ref(opening), "close_event_ref": _row_ref(closing)}


def guard_facts_from_edge(edge, window, *, submitted=None):
    """Convert one edge into facts for ``evaluate_match_compatibility_guard``.

    ``window`` is mandatory.  Without a complete controller window the helper
    returns ``facts=None`` plus the reason, so the oracle can only be unknown.
    """
    if edge.get("kind") != "g01_edge_observation":
        raise RelationSurfaceError("edge_kind_mismatch")
    if not isinstance(window, dict):
        return {"facts": None, "reason": "observation_window_missing",
                "oracle_input": False}
    if window.get("complete") is not True:
        return {"facts": None, "reason": "observation_window_incomplete",
                "oracle_input": False}
    submitted = submitted if submitted is not None else edge.get("submitted_policies")
    if not isinstance(submitted, dict):
        return {"facts": None, "reason": "submitted_policies_missing",
                "oracle_input": False}
    writer, reader = edge.get("writer", {}), edge.get("reader", {})
    writer_qos = submitted.get("writer", {})
    reader_qos = submitted.get("reader", {})
    if "reliability" not in writer_qos or "reliability" not in reader_qos:
        return {"facts": None, "reason": "submitted_reliability_missing",
                "oracle_input": False}
    facts = {
        "writer": {"id": writer.get("gid"), "generation": writer.get("generation")},
        "reader": {"id": reader.get("gid"), "generation": reader.get("generation")},
        "submitted": {"writer_reliability": writer_qos["reliability"],
                      "reader_reliability": reader_qos["reliability"]},
        "window": {"start_ns": window["start_ns"], "end_ns": window["end_ns"],
                   "complete": True},
        "match": None,
        "delivery": None,
        "observed": None,
    }
    match = edge.get("match", {})
    if match.get("status") == "observed" and edge.get("pair_identity_correlated", edge.get("identity_correlated")):
        at_ns = match.get("at_ns")
        if type(at_ns) is int:
            facts["match"] = {
                "identity_correlated": True,
                "writer_id": writer.get("gid"),
                "writer_generation": writer.get("generation"),
                "reader_id": reader.get("gid"),
                "reader_generation": reader.get("generation"),
                "at_ns": at_ns,
            }
    delivery = edge.get("delivery", {})
    if delivery.get("status") == "observed" and delivery.get("identity_correlated"):
        facts["delivery"] = {
            "identity_correlated": True,
            "sent": [{"sequence": row["sequence"],
                      "writer_id": writer.get("gid"),
                      "writer_generation": writer.get("generation")}
                     for row in delivery.get("sent", [])],
            "received": [{"sequence": row["sequence"],
                          "reader_id": reader.get("gid"),
                          "reader_generation": reader.get("generation")}
                         for row in delivery.get("received", [])],
        }
    if match.get("status") in ("observed", "not_matched") and delivery.get("status") in ("observed", "none"):
        facts["observed"] = {
            "match": "present" if match.get("status") == "observed" else "absent",
            "delivery": "present" if delivery.get("status") == "observed" else "absent",
            "observer_complete": bool(edge.get("identity_correlated")),
            "provenance": True,
        }
    return {"facts": facts, "reason": None, "oracle_input": True,
            "edge_id": edge.get("edge_id")}


def _w01_policy_field(events, role, gid, field):
    policy, reason, event = _policy_from_binding(events, role, gid)
    source = "qos_binding_observed"
    if policy is None or field not in policy:
        graph_policy, graph_reason, graph_event = _policy_from_graph(events, role, gid)
        if graph_policy is not None and field in graph_policy:
            policy, event, source = graph_policy, graph_event, "graph_snapshot"
        else:
            return None, reason or graph_reason, None, source
    if field not in policy:
        return None, "field_absent_from_public_observation:" + field, event, source
    return policy[field], None, event, source


def observe_w01_delivery(events, config, *, writer_role="writer", reader_role="reader",
                         writer_generation=None, reader_generation=None):
    """Per-pair W01 depth/history binding plus send/receive closure facts."""
    if not isinstance(config, dict):
        raise RelationSurfaceError("w01_config_required")
    if not isinstance(events, list):
        raise RelationSurfaceError("events_must_be_a_list")
    _require_event_indexes(events)
    _validate_run(events, config.get("run_id"), config.get("domain_id"))
    creations = _endpoint_creations(events)
    writer, reason = _select_generation(creations.get(writer_role, []), writer_role,
                                       writer_generation)
    if writer is None:
        return {"schema": SCHEMA, "observer": OBSERVER_ID, "kind": "w01_edge_observation",
                "edges": [], "status": "incomplete", "reasons": [reason],
                "aggregate_used": False, "execution_evidence": False,
                "oracle_verdict": None, "candidate_eligible": False}
    reader, reason = _select_generation(creations.get(reader_role, []), reader_role,
                                       reader_generation)
    if reader is None:
        return {"schema": SCHEMA, "observer": OBSERVER_ID, "kind": "w01_edge_observation",
                "edges": [], "status": "incomplete", "reasons": [reason],
                "aggregate_used": False, "execution_evidence": False,
                "oracle_verdict": None, "candidate_eligible": False}

    reasons = []
    if writer.get("pid") == reader.get("pid") or not writer.get("pid") or not reader.get("pid"):
        reasons.append("endpoint_processes_not_independent")
    qos = config.get("qos") if isinstance(config.get("qos"), dict) else {}
    submitted_depth = qos.get("depth")
    submitted_history = qos.get("history", "keep_last")
    pre_count = config.get("pre_match_count")
    post_count = config.get("post_match_count")
    binding = {}
    for label, role, gid in (("writer", writer_role, writer.get("endpoint_gid")),
                             ("reader", reader_role, reader.get("endpoint_gid"))):
        depth, depth_reason, depth_event, depth_source = _w01_policy_field(events, role, gid, "depth")
        history, history_reason, history_event, history_source = _w01_policy_field(events, role, gid, "history")
        binding[label] = {
            "role": role,
            "depth": {"submitted": submitted_depth, "observed": depth,
                      "source": depth_source, "event_ref": _row_ref(depth_event) if depth_event else None,
                      "match": type(depth) is int and depth == submitted_depth},
            "history": {"submitted": submitted_history, "observed": history,
                        "source": history_source, "event_ref": _row_ref(history_event) if history_event else None,
                        "match": history is not None and history == submitted_history,
                        "applicability": "unknown" if history is None else "observed"},
        }
        if not binding[label]["depth"]["match"]:
            reasons.append("depth_binding_not_observed:" + label + ":" + str(depth_reason))
        if history is None:
            if submitted_history != "keep_last":
                reasons.append("history_binding_not_observed:" + label + ":"
                               + str(history_reason))
        elif history != submitted_history:
            # Never accept a silently normalised history (e.g. submitted
            # KEEP_ALL observed as KEEP_LAST, or the reverse).
            reasons.append("history_binding_mismatch:" + label
                           + ":" + str(submitted_history) + "!=" + str(history))

    sends = [event for event in events if event.get("event") == "send"
             and event.get("role") == writer_role
             and event.get("writer_gid") in (None, writer.get("endpoint_gid"))]
    receives = [event for event in events if event.get("event") == "receive"
                and event.get("role") == reader_role
                and event.get("reader_gid") in (None, reader.get("endpoint_gid"))]
    pre_sends = sorted(event.get("send_sequence") for event in sends
                       if event.get("phase") == "prejoin")
    post_sends = sorted(event.get("send_sequence") for event in sends
                        if event.get("phase") == "post_match")
    post_receives = sorted(event.get("send_sequence") for event in receives
                           if event.get("phase") == "post_match")
    if type(pre_count) is int and pre_count >= 0 and len(pre_sends) != pre_count:
        reasons.append("pre_match_count_binding_mismatch")
    if type(post_count) is int and post_count >= 0 and len(post_sends) != post_count:
        reasons.append("post_match_count_binding_mismatch")
    missing = sorted(set(post_sends) - set(post_receives))
    max_received = max(post_receives, default=None)
    proven_gaps = ([value for value in missing if max_received is not None and value < max_received]
                   if max_received is not None else [])
    relation_class = None
    if type(submitted_depth) is int and type(post_count) is int:
        if submitted_depth < post_count:
            relation_class = "depth_lt_post"
        elif submitted_depth == post_count:
            relation_class = "depth_eq_post"
        else:
            relation_class = "depth_gt_post"
    reviewed = config.get("w01_delivery_rule_reviewed") is True
    applicability = "unknown"
    if not reasons and relation_class is not None:
        applicability = "review_required" if relation_class == "depth_lt_post" and not reviewed else "applicable"
    edge_id = canonical_sha256({
        "writer": {"role": writer_role, "gid": writer.get("endpoint_gid"),
                   "generation": writer.get("endpoint_generation")},
        "reader": {"role": reader_role, "gid": reader.get("endpoint_gid"),
                   "generation": reader.get("endpoint_generation")},
        "surface": "W01",
    })
    edge = {
        "schema": SCHEMA,
        "observer": OBSERVER_ID,
        "kind": "w01_edge_observation",
        "edge_id": edge_id,
        "writer": {"role": writer_role, "gid": writer.get("endpoint_gid"),
                   "generation": writer.get("endpoint_generation"),
                   "pid": writer.get("pid"), "endpoint_event_ref": _row_ref(writer)},
        "reader": {"role": reader_role, "gid": reader.get("endpoint_gid"),
                   "generation": reader.get("endpoint_generation"),
                   "pid": reader.get("pid"), "endpoint_event_ref": _row_ref(reader)},
        "submitted": {"depth": submitted_depth, "history": submitted_history,
                      "pre_match_count": pre_count, "post_match_count": post_count},
        "observed_binding": binding,
        "relation_class": relation_class,
        "delivery": {
            "prejoin_sent_sequences": pre_sends,
            "post_match_sent_sequences": post_sends,
            "post_match_received_sequences": post_receives,
            "missing_sequences": missing,
            "proven_gaps": proven_gaps,
            "max_received_sequence": max_received,
            "send_event_refs": [_row_ref(event) for event in sends],
            "receive_event_refs": [_row_ref(event) for event in receives],
            "aggregate_used": False,
        },
        "oracle_applicability": applicability,
        "oracle_reason": (None if applicability == "applicable"
                          else "delivery_obligation_not_reviewed_for_depth_lt_post"
                          if applicability == "review_required"
                          else "binding_or_observation_incomplete"),
        "w01_delivery_rule_reviewed": reviewed,
        "incomplete_reasons": sorted(set(reasons)),
        "identity_correlated": not reasons,
        "aggregate_used": False,
        "execution_evidence": False,
        "oracle_verdict": None,
        "candidate_eligible": False,
    }
    return {"schema": SCHEMA, "observer": OBSERVER_ID, "kind": "w01_edge_observation",
            "edges": [edge], "status": "observed" if not reasons else "incomplete",
            "reasons": sorted(set(reasons)), "aggregate_used": False,
            "execution_evidence": False, "oracle_verdict": None,
            "candidate_eligible": False}


def relation_observer_capability():
    """Static capability/limitation view for provider wiring."""
    return {
        "schema": SCHEMA,
        "observer_id": OBSERVER_ID,
        "implemented": True,
        "starts_ros": False,
        "requires": [
            "endpoint_created rows with role, endpoint_gid and endpoint_generation",
            "qos_binding_observed or graph_snapshot per role, bound to the same GID",
            "command-bound match_snapshot + graph_snapshot + command_done sequence",
            "per-sample send/receive rows with writer/reader GID or explicit correlation",
        ],
        "g01": {
            "per_edge": True,
            "aggregate_match_count_used": False,
            "incompatible_pairs": sorted(G01_INCOMPATIBLE_PAIRS),
            "unavailable_policy_fields": list(_UNAVAILABLE_QOS_FIELDS),
            "oracle": "modules.s1.oracles.evaluate_match_compatibility_guard",
        },
        "w01": {
            "per_edge": True,
            "aggregate_match_count_used": False,
            "depth_and_history_are_not_match_policies": True,
            "depth_lt_post_requires_delivery_rule_review": True,
            "history_supported_by_current_worker": ["keep_last"],
        },
        "missing_evidence_policy": "unknown_not_pass",
        "execution_evidence": False,
        "candidate_eligible": False,
    }


def edge_summary(observation):
    """Compact provider-facing summary; never an oracle verdict."""
    rows = []
    for edge in observation.get("edges", []):
        rows.append({
            "kind": edge.get("kind"),
            "edge_id": edge.get("edge_id"),
            "writer": edge.get("writer"),
            "reader": edge.get("reader"),
            "relation_class": edge.get("relation_class"),
            "relation_status": (edge.get("relation") or {}).get("status"),
            "match_status": (edge.get("match") or {}).get("status"),
            "delivery_status": (edge.get("delivery") or {}).get("status"),
            "oracle_applicability": edge.get("oracle_applicability"),
            "identity_correlated": edge.get("identity_correlated"),
            "incomplete_reasons": edge.get("incomplete_reasons"),
        })
    return {"schema": SCHEMA, "observer": OBSERVER_ID,
            "status": observation.get("status"),
            "reasons": observation.get("reasons"),
            "edges": rows,
            "aggregate_used": False,
            "execution_evidence": False,
            "oracle_verdict": None,
            "candidate_eligible": False}


__all__ = [
    "SCHEMA", "OBSERVER_ID", "g01_edge_observation", "observe_w01_delivery",
    "guard_facts_from_edge", "window_from_events", "relation_observer_capability",
    "edge_summary", "g01_configuration_readback", "g01_runtime_observation",
]


def _g01_graph_qos_matches(graph_qos, actual):
    """Reconcile available RxO fields; graph history/depth stay diagnostic."""
    if not isinstance(graph_qos, dict):
        return False
    for key in ('reliability', 'durability', 'deadline_ns', 'liveliness',
                'liveliness_lease_duration_ns'):
        if key not in graph_qos:
            continue
        value = graph_qos[key]
        if value is None or value in ('unknown', 'system_default'):
            continue
        if key.endswith('_ns') and value == 0:
            continue
        if value != actual.get(key):
            return False
    return True


def g01_binding_readback(binding, token, requested):
    """Validate actual endpoint evidence, retaining strict legacy readback."""
    observed = binding.get('observed_self')
    if 'actual_endpoint_qos' not in binding:
        if not isinstance(observed, dict) or any(observed.get(k) != v for k, v in requested.items()):
            raise RelationSurfaceError('g01_actual_qos_readback_mismatch:' + token['role'])
        return observed
    from .qos_binding import compare_public_qos, validate_actual_qos_receipt
    try:
        actual = validate_actual_qos_receipt(binding['actual_endpoint_qos'], token,
                                             binding.get('monotonic_ns'))
        comparison = compare_public_qos(requested, actual)
    except (RuntimeError, ValueError, TypeError, KeyError) as exc:
        raise RelationSurfaceError('g01_actual_qos_receipt_invalid:' + token['role']) from exc
    if comparison.get('status') != 'matched':
        raise RelationSurfaceError('g01_actual_qos_readback_mismatch:' + token['role'])
    if not _g01_graph_qos_matches(observed, actual):
        raise RelationSurfaceError('g01_graph_qos_mismatch:' + token['role'])
    return actual


def g01_configuration_readback(events, config):
    """Verify only API configuration intervention; never a match/effect claim."""
    _require_event_indexes(events)
    _validate_run(events, config.get("run_id"), config.get("domain_id"))
    if any(row.get("run_id") != config.get("run_id")
           or row.get("domain_id") != config.get("domain_id") for row in events):
        raise RelationSurfaceError("g01_run_or_domain_identity_missing")
    creations = _endpoint_creations(events)
    tokens = {}
    for role in ("writer", "reader"):
        rows = creations.get(role, [])
        if len(rows) != 1:
            raise RelationSurfaceError("g01_unique_creation_required:" + role)
        token = tokens[role] = rows[0]
        if (not isinstance(token.get("endpoint_gid"), str) or not token["endpoint_gid"]
                or not isinstance(token.get("endpoint_generation"), str)
                or not token["endpoint_generation"] or type(token.get("pid")) is not int
                or token["pid"] <= 0
                or token.get("run_id") != config.get("run_id")
                or token.get("domain_id") != config.get("domain_id")
                or token.get("rmw_identifier") != (config.get("rmws") or {}).get(
                    role, config.get("rmw", "rmw_cyclonedds_cpp"))
                or token.get("topic") != config.get("topic")
                or token.get("message_type") != "std_msgs/msg/String"
                or type(token.get("created_monotonic_ns")) is not int
                or not token["created_monotonic_ns"] <= token["monotonic_ns"]):
            raise RelationSurfaceError("g01_creation_identity_invalid:" + role)
        bindings = [e for e in events if e.get("event") == "qos_binding_observed"
                    and e.get("role") == role]
        if len(bindings) != 1:
            raise RelationSurfaceError("g01_unique_qos_readback_required:" + role)
        binding = bindings[0]
        observed = binding.get("observed_self")
        api = "get_" + ("publishers" if role == "writer" else "subscriptions") + "_info_by_topic"
        if (binding.get("observed_self_gid") != token["endpoint_gid"]
                or binding.get("observation_api") != api
                or not isinstance(observed, dict)
                or not token["monotonic_ns"] <= binding.get("monotonic_ns", -1)):
            raise RelationSurfaceError("g01_qos_readback_mismatch:" + role)
        g01_binding_readback(binding, token, config[role + "_qos"])
        # WorkerPool authenticates these on the pipe. Replays must retain
        # the same identity fields; a same-role foreign process cannot pass.
        for row in events:
            if row.get("role") != role or row.get("event") not in {
                    "qos_binding_observed", "match_snapshot", "graph_snapshot",
                    "command_done", "send", "receive"}:
                continue
            if any(row.get(k) != token.get(k) for k in
                   ("pid", "endpoint_generation", "rmw_identifier", "run_id", "domain_id")):
                raise RelationSurfaceError("g01_worker_identity_mismatch:" + role)
    if (tokens["writer"]["pid"] == tokens["reader"]["pid"]
            or tokens["writer"]["endpoint_gid"] == tokens["reader"]["endpoint_gid"]):
        raise RelationSurfaceError("g01_pair_identity_not_independent")
    return tokens


def g01_runtime_observation(events, config):
    """Strict raw-to-guard seam for the public 1W1R runtime.

    The legacy edge helper also serves partial/offline diagnostics. This seam
    requires transport identities, real API readback, command requests and a
    bounded graph before *and* after delivery. It returns no verdict and never
    treats a finite zero count as proof of prohibited-event absence.
    """
    result = {"schema": SCHEMA, "kind": "g01_runtime_observation",
              "facts": None, "oracle_input": False, "reasons": [],
              "relation_effect_observed": False, "execution_evidence": False,
              "candidate_eligible": False}
    reasons = result["reasons"]
    labels = {"reliable": "R", "best_effort": "B"}
    pair = "".join(labels.get(config.get(role + "_qos", {}).get("reliability"), "?")
                   for role in ("writer", "reader"))
    result["reliability_prediction"] = {
        "pair": pair, "compatible": pair not in G01_INCOMPATIBLE_PAIRS if "?" not in pair else None,
        "basis": "submitted_reliability_only_not_match_prediction",
        "does_not_establish_topic_type_or_other_rxo_compatibility": True}
    result["observed_relation"] = None
    result["identity_limitations"] = []
    try:
        _require_event_indexes(events)
        _validate_run(events, config.get("run_id"), config.get("domain_id"))
        window_rows = {kind: [e for e in events if e.get("event") == kind
                             and e.get("window") == "pair_compatibility_window"]
                       for kind in ("window_open", "window_close")}
        if any(len(rows) != 1 for rows in window_rows.values()):
            raise RelationSurfaceError("g01_unique_complete_window_required")
        window = window_from_events(events, "pair_compatibility_window")
        if window is None:
            raise RelationSurfaceError("g01_invalid_window")
        start, end = window["start_ns"], window["end_ns"]
        tokens = g01_configuration_readback(events, config)
        if any(not start <= t["created_monotonic_ns"] <= t["monotonic_ns"] < end
               for t in tokens.values()):
            raise RelationSurfaceError("g01_creation_outside_window")
        sends = [e for e in events if e.get("event") == "send"]
        receives = [e for e in events if e.get("event") == "receive"]
        if any(not start < e.get("monotonic_ns", -1) < end for e in sends + receives):
            raise RelationSurfaceError("g01_delivery_outside_window")
        for row in sends:
            if (row.get("role") != "writer" or row.get("writer_gid") != tokens["writer"]["endpoint_gid"]
                    or row.get("phase") != "pair_probe"):
                raise RelationSurfaceError("g01_send_identity_invalid")
        for row in receives:
            if (row.get("role") != "reader" or row.get("reader_gid") != tokens["reader"]["endpoint_gid"]
                    or row.get("phase") != "pair_probe"
                    or row.get("publisher_gid") is None and row.get("publisher_gid_status") != "unavailable_api"):
                raise RelationSurfaceError("g01_receive_identity_invalid")
        # Only request -> fresh sample -> graph -> ACK qualifies. At least one
        # complete pair snapshot precedes sends and one follows all delivery.
        before, after = {}, {}
        for role, token in tokens.items():
            proofs = _command_bound_snapshot(events, role, token["endpoint_gid"],
                                              max(t["created_monotonic_ns"] for t in tokens.values()))
            valid = []
            for proof in proofs:
                requests = [e for e in events if e.get("event") == "worker_command_requested"
                            and e.get("role") == role and e.get("command_id") == proof["command_id"]]
                if len(requests) != 1:
                    raise RelationSurfaceError("g01_snapshot_request_missing_or_ambiguous:" + role)
                request, match, graph, ack = requests[0], proof["match"], proof["graph"], proof["ack"]
                if (request.get("command") != "snapshot"
                        or request.get("origin") != "controller_pipe"
                        or any(request.get(k) != token.get(k) for k in ("pid", "endpoint_generation"))
                        or not start < request.get("requested_monotonic_ns", -1) <= request["monotonic_ns"] <= proof["sampled_ns"]
                        or not request["event_index"] < match["event_index"]
                        or not ack["monotonic_ns"] < end
                        or match.get("count_api") != ("get_subscription_count" if role == "writer"
                                                       else "get_publisher_count")
                        or type(match.get("matched_count")) is not int
                        or match["matched_count"] not in (0, 1)
                        or _graph_isolation(graph, tokens["writer"]["endpoint_gid"],
                                            tokens["reader"]["endpoint_gid"])[0] != "exact_pair"):
                    raise RelationSurfaceError("g01_snapshot_request_or_pair_invalid:" + role)
                for table, endpoint_role in (("publishers", "writer"), ("subscriptions", "reader")):
                    entries = graph.get(table, [])
                    if len(entries) != 1 or entries[0].get("topic_type") != "std_msgs/msg/String":
                        raise RelationSurfaceError("g01_graph_type_or_cardinality_invalid:" + role)
                    qos = entries[0].get("qos")
                    binding = next(e for e in events if e.get("event") == "qos_binding_observed"
                                   and e.get("role") == endpoint_role)
                    if "actual_endpoint_qos" in binding:
                        actual = g01_binding_readback(binding, tokens[endpoint_role],
                                                     config[endpoint_role + "_qos"])
                        graph_matches = _g01_graph_qos_matches(qos, actual)
                    else:
                        graph_matches = isinstance(qos, dict) and all(
                            qos.get(k) == v for k, v in config[endpoint_role + "_qos"].items())
                    if not graph_matches:
                        raise RelationSurfaceError("g01_graph_qos_mismatch:" + endpoint_role)
                valid.append(proof)
            requested = [e for e in events if e.get("event") == "worker_command_requested"
                         and e.get("role") == role and e.get("command") == "snapshot"]
            if len(requested) != len(valid):
                raise RelationSurfaceError("g01_snapshot_command_incomplete:" + role)
            first_send = min((e["monotonic_ns"] for e in sends), default=end)
            last_delivery = max((e["monotonic_ns"] for e in sends + receives), default=start)
            prior = [p for p in valid if p["ack"]["monotonic_ns"] < first_send]
            posterior = [p for p in valid if p["sampled_ns"] > last_delivery]
            if not prior or not posterior:
                raise RelationSurfaceError("g01_fresh_graph_boundaries_missing:" + role)
            before[role], after[role] = prior[-1], posterior[-1]
        if any(before[r]["match"]["matched_count"] != after[r]["match"]["matched_count"]
               for r in tokens):
            raise RelationSurfaceError("g01_match_changed_across_delivery_window")
        observation = g01_edge_observation(events, config)
        result["observation"] = observation
        if not observation.get("edges"):
            raise RelationSurfaceError("g01_edge_missing")
        edge = observation["edges"][0]
        result["observed_relation"] = {"match": edge["match"], "delivery": edge["delivery"],
                                       "policies": edge["observed_policies"]}
        if any(e.get("publisher_gid") is None for e in receives):
            result["identity_limitations"].append("publisher_gid_unavailable_api_endpoint_correlated_not_direct_sample_gid")
        if edge["incomplete_reasons"]:
            reasons.extend(edge["incomplete_reasons"])
            return result
        delivery = edge["delivery"]
        if any(seq not in delivery["sent_sequences"] for seq in delivery["received_sequences"]):
            raise RelationSurfaceError("g01_uncorrelated_received_sequence")
        converted = guard_facts_from_edge(edge, window)
        result.update(facts=converted["facts"], oracle_input=converted["oracle_input"])
        if converted["reason"]:
            reasons.append(converted["reason"])
        result["relation_effect_observed"] = (converted["oracle_input"] and edge["match"]["status"] == "observed")
        result["graph_boundaries"] = {
            role: {"before_ack": _row_ref(before[role]["ack"]),
                   "after_ack": _row_ref(after[role]["ack"])} for role in tokens}
        result["delivery_obligation"] = "not_evaluated_by_compatibility_guard"
        result["finite_absence_is_not_proof"] = True
    except (RelationSurfaceError, KeyError, TypeError, ValueError) as exc:
        reasons.append(str(exc))
        result.update(facts=None, oracle_input=False, relation_effect_observed=False)
    return result
