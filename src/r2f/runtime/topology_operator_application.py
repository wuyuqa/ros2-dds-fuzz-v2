"""Independent application auditor for topology parameter/relation operators.

Scope: exact ``env.delay`` deadline slots, ``relation.pair``, ``timing.shift``, ``lease.value`` and initial
``filter.expr`` (plus the
already-audited ``param.enum`` / ``queue.depth`` / ``param.boundary`` field
checks this module can re-verify).  Every verdict here is read from hashed raw
``endpoint_created`` bytes; a declared program value is only ever compared
against that raw reflection and is never used to fill a missing field.

This module deliberately does not import or mutate
``r2f.runtime.topology_route``; the route keeps its own layer loop.  A caller
that wants these records merges them by ``layer_index``.

What this proves and what it does not:

* ``relation.pair`` — the two endpoints the layer addressed each really
  reported the mutated QoS value, and the offered/requested DDS relation over
  exactly those changed match-policy fields.  A compatible or incompatible
  contract is a *statement about the declared policies*, not an observed
  match.  An incompatible pair therefore stays ``unknown_expected_no_match``:
  application is observed, the effect is unknown, and nothing is a pass.
* ``timing.shift`` / ``lease.value`` — the bounded target value actually
  reached the endpoint, checked against the declared slot domain and the raw
  reflection.  Lease expiry/refresh *lifecycle* is never claimed; that needs a
  runtime lease action this project does not implement.
* A configuration intervention additionally needs distinct public-API inputs
  before/after, no later override, and every addressed generation's independent
  readback. Only the enclosing, formally qualified route can grant a hit from
  this proof. This module never grants hit/execution/candidate eligibility.
* Initial CFT readback is not sample filtering, an observed match, a dynamic
  update or a lease effect. An offline fixture cannot qualify execution.
"""
from copy import deepcopy
import math

from r2f.core.evidence import canonical_hash
from .topology_worker import (
    OFFLINE_BACKEND, ROS_BACKEND, TopologyUnsupported, endpoint_expressibility,
    worker_role_for, worker_role_kind,
)

SCHEMA = "r2f.topology-operator-application/v1"
PARAMETER_OPERATORS = ("param.enum", "queue.depth", "param.boundary")
RELATION_OPERATORS = ("relation.pair",)
TIMING_OPERATORS = ("timing.shift", "lease.value")
FILTER_OPERATORS = ("filter.expr",)
INTERVENTION_OPERATORS = RELATION_OPERATORS + TIMING_OPERATORS + FILTER_OPERATORS
# Unconditionally audited operator names. Slot-qualified support is separate:
# env.delay on an endpoint deadline is NOT a network netem delay.
AUDITED_OPERATORS = PARAMETER_OPERATORS + INTERVENTION_OPERATORS
DEADLINE_DELAY_SLOT = "S1.topology.qos.deadline"


def is_deadline_configuration_delay(receipt):
    """Select only the existing endpoint deadline slot, never a netem window."""
    slot = receipt.get("slot_id")
    return (receipt.get("operator_id") == "env.delay" and isinstance(slot, str)
            and slot.split("#", 1)[0] == DEADLINE_DELAY_SLOT)


#: Program-side QoS field -> public ``rclpy.qos.QoSProfile`` field name.
PUBLIC_QOS_FIELD = {
    "depth": "depth", "reliability": "reliability", "durability": "durability",
    "liveliness": "liveliness", "history": "history",
    "deadline_sec": "deadline_ns", "lease_duration_sec": "liveliness_lease_duration_ns",
}
#: Fields any real public reflection must expose before this audit can decide.
REFLECTED_QOS_FIELDS = ("reliability", "durability", "deadline_ns", "liveliness",
                        "liveliness_lease_duration_ns")
DURATION_FIELDS = ("deadline_sec", "lease_duration_sec")
TIMING_SLOT_FIELD = {"S1.topology.qos.deadline": "deadline_sec",
                     "S1.topology.qos.lease": "lease_duration_sec"}
INFINITE_QOS_NS = 9223372036854775807


def layer_qos_delta(before, after):
    """QoS fields one sealed layer actually changed, from its two snapshots.

    Endpoint additions/removals are not QoS deltas; they belong to the
    structure operators.  Values are the declared program values, not
    observations.
    """
    rows = []
    previous = {e.get("logical_id"): e for e in before.get("endpoints", [])}
    for index, endpoint in enumerate(after.get("endpoints", [])):
        old = previous.get(endpoint.get("logical_id"))
        if old is None:
            continue
        old_qos, new_qos = old.get("qos") or {}, endpoint.get("qos") or {}
        for field, value in new_qos.items():
            if old_qos.get(field) != value:
                rows.append({"index": index, "logical_id": endpoint.get("logical_id"),
                             "role": endpoint.get("role"), "field": field,
                             "value": value, "before": old_qos.get(field)})
    return rows


def _to_ns(field, value):
    if field not in DURATION_FIELDS:
        return value
    if value == "infinite":
        return INFINITE_QOS_NS
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return int(round(value * 1_000_000_000))


def read_raw_qos(created, program_field):
    """Read one QoS field from ``endpoint_created`` only.

    Returns ``(value, ok, reason)``.  An offline fixture echoes the declared
    program names; a real public-API reflection uses the public schema with
    nanosecond durations.  Each surface is read on its own terms: nothing is
    rescaled across them and a missing key is reported, never defaulted.
    """
    qos = created.get("qos")
    if not isinstance(qos, dict):
        return None, False, "raw_endpoint_qos_missing"
    offline = created.get("backend") == OFFLINE_BACKEND
    key = program_field if offline else PUBLIC_QOS_FIELD.get(program_field)
    if key is None:
        return None, False, "qos_field_not_publicly_expressible:" + str(program_field)
    if key not in qos:
        return None, False, "reflected_qos_field_missing:" + key
    value = qos[key]
    observed = _to_ns(program_field, value) if offline else value
    if observed is None:
        return None, False, "reflected_qos_value_invalid:" + key
    return observed, True, None


def expected_qos(spec, program_field, created):
    """Expected value on the same surface the backend actually reflects."""
    offline = created.get("backend") == OFFLINE_BACKEND
    declared = (spec.get("qos") or {})
    if program_field not in declared:
        return None, False, "sealed_layer_qos_field_missing:" + program_field
    if offline:
        return _to_ns(program_field, declared[program_field]), True, None
    public = PUBLIC_QOS_FIELD.get(program_field)
    if public is None:
        return None, False, "qos_field_not_publicly_expressible:" + str(program_field)
    try:
        prepared = endpoint_expressibility(spec, {"rmw": created.get("rmw_identifier")})
    except TopologyUnsupported as exc:
        return None, False, "endpoint_not_expressible:" + exc.code
    values = prepared.get("public_qos") or {}
    if public not in values:
        return None, False, "public_qos_field_not_reflected:" + public
    return values[public], True, None


def relation_contract(writer_values, reader_values, fields, *, offline):
    """Offered/requested DDS relation over exactly the mutated match fields."""
    from r2f.modules.s1.relation_surfaces import field_relation
    views, unknown = {}, []
    for field in fields:
        offered = writer_values.get(field)
        requested = reader_values.get(field)
        view = field_relation(field, offered, requested)
        views[field] = {"status": view.get("status"), "offered": offered,
                        "requested": requested, "rule": view.get("rule"),
                        "reason": view.get("reason"),
                        "source": "offline_fixture_declared_qos" if offline
                                  else "raw_endpoint_created_qos"}
        if view.get("status") in {"unknown", "unavailable_api"}:
            unknown.append(field)
    if not views:
        status = "unknown"
    elif any(view["status"] == "incompatible" for view in views.values()):
        status = "incompatible"
    elif unknown:
        status = "unknown"
    else:
        status = "compatible"
    return {"status": status, "fields": views, "unknown_fields": unknown,
            "scope": "declared_mutated_match_policy_fields_only",
            "prediction_only": True,
            "not_an_observed_match": True}


def match_reading(events, writer_id, reader_id, *, offline):
    """Aggregate-count diagnostics only; never an observed directed edge."""
    rows = [e for e in events if e.get("event") == "match_snapshot"
            and e.get("logical_id") in {writer_id, reader_id}]
    if not rows:
        return {"status": "unknown", "reason": "match_snapshot_missing"}
    writer = next((e for e in reversed(rows) if e.get("logical_id") == writer_id), None)
    reader = next((e for e in reversed(rows) if e.get("logical_id") == reader_id), None)
    if writer is None or reader is None:
        return {"status": "unknown", "reason": "one_sided_match_snapshot"}
    counts = {"writer_count": writer.get("matched_count"),
              "reader_count": reader.get("matched_count")}
    if offline:
        return {"status": "fixture_simulated_not_dds_contract", **counts,
                "source": "offline_fake_backend"}
    if counts["writer_count"] == 0 and counts["reader_count"] == 0:
        # D009: a finite window without match is not by itself a violation.
        return {"status": "unmatched_in_finite_window", **counts,
                "source": "raw_match_snapshot",
                "reason": "finite_window_non_match_effect_unknown"}
    return {"status": "unknown", **counts, "source": "raw_match_snapshot",
            "reason": "aggregate_counts_do_not_prove_pair_match"}


def _slot_domain(slot_key):
    from r2f.modules.s1 import input_model
    try:
        surface = input_model.default_domain().get("S1.topology")
        slot = surface.slot_by_id[slot_key]
        return dict(slot.domain)
    except (AttributeError, KeyError, TypeError, ValueError):
        return {}


def _record(layer_index, operator, slot_key):
    return {"schema": SCHEMA, "layer_index": layer_index, "operator_id": operator,
            "slot_id": slot_key, "status": "unknown", "reasons": [], "raw_refs": [],
            "configuration_application_observed": False, "application_kind": "configuration_only",
            "configuration_intervention_proven": False,
            "hit": False, "effect_observed": False, "execution_evidence": False,
            "candidate_eligible": False}


def _public_configuration_intervention(record, receipt, program):
    """Compare actual API inputs, never program spelling alone or raw echoes.

    Use the same mapper as endpoint creation (not the fixture's rounded-ns
    diagnostic). A later overwrite is disqualifying even when both values
    truncate to the same public Duration. Unrelated fields may still compose.
    """
    snapshots = [{e["logical_id"]: e for e in p.get("endpoints", [])}
                 for p in (receipt["input_before"], receipt["input_after"], program)]
    proofs = []
    if record["operator_id"] in FILTER_OPERATORS:
        delta = [{"logical_id": eid, "field": "filter"}
                 for eid, spec in snapshots[1].items() if eid in snapshots[0]
                 and spec.get("filter") != snapshots[0][eid].get("filter")]
    else:
        delta = record.get("qos_delta", [])
    for row in delta:
        eid, field = row["logical_id"], row["field"]
        specs = [s.get(eid) for s in snapshots]
        if any(s is None for s in specs):
            record["reasons"].append("layer_endpoint_overridden_or_missing:" + eid)
            continue
        try:
            mapped = [endpoint_expressibility(s, {}) for s in specs]
        except TopologyUnsupported as exc:
            record["reasons"].append("public_configuration_not_expressible:" + eid + ":" + exc.code)
            continue
        if field == "filter":
            # A missing/blank filter both create without content_filter_options.
            # This is an INPUT comparison only, never a default for raw data.
            values = []
            for m in mapped:
                value = m["initial_content_filter"]
                values.append(value if value and value["expression"].strip()
                              else {"expression": "", "parameters": []})
            declared_after = specs[1].get("filter")
            declared_final = specs[2].get("filter")
            not_overridden = (mapped[1]["initial_content_filter"] == mapped[2]["initial_content_filter"]
                              and declared_after is not None and declared_final is not None)
        else:
            public_field = PUBLIC_QOS_FIELD.get(field)
            if public_field is None or any(public_field not in m["public_qos"] for m in mapped):
                record["reasons"].append("qos_field_not_publicly_expressible:" + str(field))
                continue
            values = [m["public_qos"][public_field] for m in mapped]
            not_overridden = all(field in s.get("qos", {}) for s in specs) and (
                canonical_hash(specs[1]["qos"][field]) == canonical_hash(specs[2]["qos"][field]))
        if not not_overridden:
            record["reasons"].append("configuration_effect_overridden_by_later_layer:" + eid + ":" + field)
        distinct = canonical_hash(values[0]) != canonical_hash(values[1])
        if not distinct:
            record["reasons"].append("public_configuration_alias:" + eid + ":" + field)
        if canonical_hash(values[1]) != canonical_hash(values[2]):
            record["reasons"].append("final_public_configuration_differs_from_layer:" + eid + ":" + field)
        proofs.append({"logical_id": eid, "field": field, "before_public": deepcopy(values[0]),
                       "after_public": deepcopy(values[1]), "final_public": deepcopy(values[2]),
                       "distinct": distinct, "not_overridden": not_overridden,
                       "source": "sealed_snapshots_via_endpoint_expressibility_not_observation"})
    record["public_input_interventions"] = proofs
    if not proofs:
        record["reasons"].append("public_configuration_intervention_missing")


def _bound_creations(record, endpoint_id, spec, events, program, *, fixture):
    """Bind every reflected generation to its actual create begin/done raw.

    The enclosing route authenticates the raw hash and formal execution gates.
    This check still runs independently: a first matching logical ID, stale
    generation or a fixture row relabelled by the caller is never sufficient.
    """
    actions = [a for a in program.get("actions", []) if a.get("endpoint_id") == endpoint_id
               and a.get("op") in {"create", "recreate"}]
    created = [e for e in events if e.get("event") == "endpoint_created"
               and e.get("logical_id") == endpoint_id]
    if not actions or not created:
        record["reasons"].append("endpoint_never_created:" + endpoint_id)
        return []
    if ([e.get("generation") for e in created] != [a.get("generation") for a in actions]
            or any(type(e.get("generation")) is not int for e in created)):
        record["reasons"].append("raw_endpoint_generation_missing_or_ambiguous:" + endpoint_id)
        return []
    identity_keys = ("endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")
    role = spec.get("role")
    same_role = [e["logical_id"] for e in program["endpoints"] if e.get("role") == role]
    expected_worker = worker_role_for(role, same_role.index(endpoint_id))
    for action, token in zip(actions, created):
        aid = action["action_id"]
        begins = [e for e in events if e.get("event") == "topology_action_begin" and e.get("action_id") == aid]
        ends = [e for e in events if e.get("event") == "topology_action_done" and e.get("action_id") == aid]
        if len(begins) != 1 or len(ends) != 1:
            record["reasons"].append("raw_creation_action_missing_or_ambiguous:" + endpoint_id)
            continue
        begin, end = begins[0], ends[0]
        receipt = end.get("receipt") or {}
        after = receipt.get("after") or {}
        indices = [e.get("event_index") for e in (begin, token, end)]
        if (any(type(i) is not int for i in indices) or not indices[0] < indices[1] < indices[2]
                or any(e.get("action") != action for e in (begin, end, receipt))
                or receipt.get("satisfied") is not True or receipt.get("before") != begin.get("before")
                or after.get("alive") is not True or after.get("generation") != action["generation"]):
            record["reasons"].append("raw_creation_action_or_chronology_mismatch:" + endpoint_id)
        if (any(not isinstance(token.get(k), str) or not token[k] for k in ("endpoint_gid", "endpoint_generation"))
                or any(type(token.get(k)) is not int or token[k] <= 0 for k in ("pid", "process_start_ticks"))
                or any(after.get(k) != token.get(k) for k in identity_keys)):
            record["reasons"].append("raw_endpoint_identity_not_bound:" + endpoint_id)
        if (worker_role_kind(token.get("role")) != role or token.get("topic") != spec.get("topic")
                or token.get("message_type") != spec.get("type_name")):
            record["reasons"].append("raw_endpoint_configuration_mismatch:" + endpoint_id)
        if (token.get("topology_worker_role", token.get("worker_role")) != expected_worker
                or any(k in token and token[k] != value for k, value in (
                    ("worker_role", expected_worker), ("topology_logical_id", endpoint_id),
                    ("topology_generation", action["generation"])))
                or (not fixture and token.get("role") not in {role, expected_worker})):
            record["reasons"].append("raw_endpoint_worker_identity_mismatch:" + endpoint_id)
        others = [e for e in events if e.get("event") == "endpoint_created"
                  and e.get("logical_id") != endpoint_id]
        if any(e.get("endpoint_gid") == token.get("endpoint_gid")
               or e.get("endpoint_generation") == token.get("endpoint_generation")
               or (e.get("pid"), e.get("process_start_ticks")) == (
                   token.get("pid"), token.get("process_start_ticks")) for e in others):
            record["reasons"].append("raw_endpoint_identity_reused_across_roles:" + endpoint_id)
        if (not isinstance(token.get("run_id"), str) or not token["run_id"]
                or type(token.get("domain_id")) is not int
                or any(token.get(k) != e.get(k) for e in (begin, end) for k in ("run_id", "domain_id"))):
            record["reasons"].append("raw_endpoint_run_or_domain_mismatch:" + endpoint_id)
        backend = OFFLINE_BACKEND if fixture else ROS_BACKEND
        source = ("offline_fixture" if fixture else "public_endpoint_actual_qos"
                  if "actual_endpoint_qos" in token else "public_graph_endpoint_reflection")
        if (token.get("backend") != backend or token.get("real_ros") is not (not fixture)
                or token.get("configuration_source") != source):
            record["reasons"].append("raw_endpoint_provenance_mismatch:" + endpoint_id)
        if "actual_endpoint_qos" in token:
            from .qos_binding import validate_created_qos
            try:
                if fixture:
                    raise ValueError("fixture_cannot_claim_public_actual_qos")
                public = endpoint_expressibility(spec, {"rmw": token.get("rmw_identifier")})["public_qos"]
                comparison = validate_created_qos(token, public)
                if comparison["default_resolutions"]:
                    record.setdefault("qos_default_resolutions", []).append({
                        "logical_id": endpoint_id, "generation": token.get("generation"),
                        "raw_event_index": token.get("event_index"),
                        "resolutions": comparison["default_resolutions"]})
            except (ValueError, TypeError, KeyError, TopologyUnsupported) as exc:
                record["reasons"].append("raw_actual_qos_receipt_invalid:" + endpoint_id + ":" + str(exc))
        if not fixture and (not isinstance(token.get("qos"), dict)
                            or any(k not in token["qos"] for k in REFLECTED_QOS_FIELDS)):
            record["reasons"].append("raw_public_qos_reflection_incomplete:" + endpoint_id)
        if action["op"] == "recreate":
            previous = next((e for e in created if e.get("generation") == action["generation"] - 1), None)
            retired = [e for e in events if e.get("event") in {"endpoint_destroyed", "topology_endpoint_destroyed"}
                       and previous is not None and all(e.get(k) == previous.get(k)
                           for k in ("logical_id", "generation") + identity_keys)
                       and e.get("cleanup_status") == "closed"
                       and previous["event_index"] < e.get("event_index", -1) < token["event_index"]]
            if (previous is None or len(retired) != 1 or any(token.get(k) == previous.get(k)
                    for k in ("endpoint_gid", "pid", "endpoint_generation"))):
                record["reasons"].append("raw_endpoint_fresh_generation_not_proven:" + endpoint_id)
        record["raw_refs"].extend(indices)
    return created


def _check_configuration(record, delta, events, program, layer_program, *, fixture):
    """Read the layer's own field values on every raw-bound generation.

    Compare to input_after, not the final value: a later overwrite cannot
    retroactively prove an earlier layer. Unrelated later fields may compose.
    """
    final_eps = {e["logical_id"]: e for e in program.get("endpoints", [])}
    layer_eps = {e["logical_id"]: e for e in layer_program.get("endpoints", [])}
    verified = {}
    for endpoint_id in sorted({row["logical_id"] for row in delta}):
        spec, final_spec = layer_eps.get(endpoint_id), final_eps.get(endpoint_id)
        if (spec is None or final_spec is None or any(spec.get(k) != final_spec.get(k)
                for k in ("role", "topic", "type_name"))):
            record["reasons"].append("layer_endpoint_overridden_or_missing:" + endpoint_id)
            continue
        creations = _bound_creations(record, endpoint_id, spec, events, program, fixture=fixture)
        for created in creations:
            values = {}
            for row in (r for r in delta if r["logical_id"] == endpoint_id):
                field = row["field"]
                observed, ok, why = read_raw_qos(created, field)
                want, have, why_want = expected_qos(spec, field, created)
                if not ok:
                    record["reasons"].append(endpoint_id + ":" + why)
                elif not have:
                    record["reasons"].append(endpoint_id + ":" + why_want)
                elif canonical_hash(observed) != canonical_hash(want):
                    # Only the explicitly unspecified duration can resolve to
                    # a different effective value, with the complete actual
                    # endpoint receipt independently validated above. Preserve
                    # that value; do not report a literal zero-time effect.
                    public = PUBLIC_QOS_FIELD.get(field)
                    resolutions = created.get("qos_request_comparison", {}).get("default_resolutions", [])
                    resolved = (not fixture and "actual_endpoint_qos" in created
                                and field in DURATION_FIELDS and want == 0
                                and {"field": public, "requested": 0, "observed": observed,
                                     "basis": "RMW_DURATION_UNSPECIFIED"} in resolutions)
                    if resolved:
                        values[field] = observed
                    else:
                        record["reasons"].append("layer_value_not_reflected:" + endpoint_id + ":" + field)
                else:
                    values[field] = observed
            verified[endpoint_id] = {"role": created.get("role"), "fields": values,
                                     "source": created.get("configuration_source"),
                                     "raw_event_index": created.get("event_index")}
            record.setdefault("configuration_generations", []).append({
                "logical_id": endpoint_id, "generation": created.get("generation"),
                "endpoint_gid": created.get("endpoint_gid"), "pid": created.get("pid"),
                "process_start_ticks": created.get("process_start_ticks"),
                "endpoint_generation": created.get("endpoint_generation"),
                "fields": deepcopy(values), "raw_event_index": created.get("event_index")})
    return verified


def audit_relation_pair(layer_index, receipt, events, program, *, fixture):
    """Audit ``relation.pair`` from raw per-role QoS reflection."""
    slot_key = receipt.get("slot_id", "")
    record = _record(layer_index, "relation.pair", slot_key.split("#", 1)[0])
    delta = layer_qos_delta(receipt["input_before"], receipt["input_after"])
    record["qos_delta"] = deepcopy(delta)
    if not delta:
        record["reasons"].append("relation_pair_layer_changed_no_qos_field")
        return record
    fields = sorted({row["field"] for row in delta})
    effect = ((receipt.get("v2_case") or {}).get("expected_guard_effect")) or {}
    after_eps = receipt["input_after"].get("endpoints") or []
    writer_index, reader_index = effect.get("writer_index"), effect.get("reader_index")
    if (type(writer_index) is not int or type(reader_index) is not int
            or not (0 <= writer_index < len(after_eps)) or not (0 <= reader_index < len(after_eps))):
        record["reasons"].append("declared_relation_pair_identity_missing")
        return record
    writer_id = after_eps[writer_index].get("logical_id")
    reader_id = after_eps[reader_index].get("logical_id")
    if after_eps[writer_index].get("role") != "writer" \
            or after_eps[reader_index].get("role") != "reader":
        record["reasons"].append("declared_relation_pair_roles_not_writer_reader")
        return record
    record["relation_pair"] = {
        "writer": writer_id, "reader": reader_id,
        "writer_index": writer_index, "reader_index": reader_index,
        "mutated_fields": fields,
        # One role may legitimately keep its parent value (that is the point of
        # a pair), so the relation is evaluated over the union of changed
        # fields read from BOTH roles' raw reflection.
        "role_delta_fields": {writer_id: sorted({r["field"] for r in delta
                                                 if r["logical_id"] == writer_id}),
                              reader_id: sorted({r["field"] for r in delta
                                                 if r["logical_id"] == reader_id})},
        "source": "declared_layer_pair_identity_qos_from_raw_only"}
    if any(row["logical_id"] not in {writer_id, reader_id} for row in delta):
        record["reasons"].append("relation_pair_delta_outside_declared_pair")
        return record
    # The unchanged role/field is also read from raw; checking only each
    # endpoint's delta could silently skip half of an offered/requested pair.
    pair_fields = [{"logical_id": eid, "field": field}
                   for eid in (writer_id, reader_id) for field in fields]
    verified = _check_configuration(record, pair_fields, events, program,
                                    receipt["input_after"], fixture=fixture)
    if record["reasons"]:
        return record
    writer_values = verified.get(writer_id, {}).get("fields") or {}
    reader_values = verified.get(reader_id, {}).get("fields") or {}
    if set(writer_values) != set(fields) or set(reader_values) != set(fields):
        record["reasons"].append("relation_pair_role_field_sets_incomplete")
        return record
    record["observed_qos"] = {writer_id: writer_values, reader_id: reader_values}
    contract = relation_contract(writer_values, reader_values, fields, offline=fixture)
    record["dds_contract"] = contract
    record["observed_effect"] = match_reading(events, writer_id, reader_id, offline=fixture)
    record["effect_scope"] = "endpoint_public_configuration_plus_declared_pair_relation"
    if contract["status"] == "unknown":
        record["effect_observation"] = "unknown"
        record["reasons"].append("relation_pair_contract_unknown_for_mutated_fields")
        return record
    if contract["status"] == "incompatible":
        # Incompatible is a legal DDS pair.  Application is observed; the
        # expected effect is no match. Aggregate counts cannot establish the
        # directed edge in an N-endpoint graph. Never a pass or a hit.
        record["effect_observation"] = "unknown_expected_no_match"
        return record
    record["effect_observation"] = "unknown_effect_owner_is_oracle"
    return record


def audit_timing_operator(layer_index, receipt, events, program, *, fixture, operator):
    """Audit ``timing.shift`` / ``lease.value`` configuration reflection only."""
    slot_key = receipt.get("slot_id", "").split("#", 1)[0]
    record = _record(layer_index, operator, slot_key)
    delta = layer_qos_delta(receipt["input_before"], receipt["input_after"])
    record["qos_delta"] = deepcopy(delta)
    if not delta:
        record["reasons"].append("timing_layer_changed_no_qos_field")
        return record
    domain = _slot_domain(slot_key)
    if (slot_key not in TIMING_SLOT_FIELD or not domain
            or any(row["field"] != TIMING_SLOT_FIELD.get(slot_key) for row in delta)):
        record["reasons"].append("timing_slot_or_domain_not_supported")
    minimum, maximum = domain.get("min"), domain.get("max")
    specials = set(domain.get("special") or ())
    rows = []
    for row in delta:
        value = row["value"]
        if value == "infinite":
            bounded = "infinite" in specials
        elif isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            bounded = False
        else:
            bounded = ((minimum is None or value >= minimum)
                       and (maximum is None or value <= maximum))
        rows.append({**row, "bounded": bounded,
                     "domain": {"min": minimum, "max": maximum, "special": sorted(specials)}})
        if not bounded:
            record["reasons"].append("timing_target_out_of_declared_bounds:" + row["logical_id"])
        if row["before"] == value:
            record["reasons"].append("timing_target_unchanged:" + row["logical_id"])
    record["timing_delta"] = rows
    verified = _check_configuration(record, delta, events, program, receipt["input_after"],
                                    fixture=fixture)
    for row in rows:
        observed = (verified.get(row["logical_id"], {}).get("fields") or {}).get(row["field"])
        row["observed"] = observed
        row["observed_source"] = (verified.get(row["logical_id"]) or {}).get("source")
    record["effect_scope"] = "endpoint_public_timing_configuration_only_not_lifecycle"
    record["effect_observation"] = "unknown_timing_lifecycle_not_measured"
    record["limitations"] = ["lease_expiry_refresh_lifecycle_not_implemented"
                             if operator == "lease.value" else "timing_field_is_not_an_event_time_shift",
                             "reflected_configuration_is_not_a_timed_lifecycle_claim"]
    return record


def audit_deadline_delay(layer_index, receipt, events, program, *, fixture):
    """Exact endpoint configuration intervention, NOT deadline/network effect.

    Snapshot mapping, raw reflection and every created generation use the same
    D022 gates as the timing operators. The slot index must address the sole
    changed endpoint field. Additional raw API chronology is checked here only;
    existing operators' contracts are not redefined by this bounded repair.
    """
    record = audit_timing_operator(layer_index, receipt, events, program,
                                   fixture=fixture, operator="env.delay")
    before, after = receipt["input_before"], receipt["input_after"]
    slot = receipt.get("slot_id", "")
    base, sep, text = slot.partition("#")
    endpoints = before.get("endpoints", [])
    index = int(text) if text.isdecimal() else -1
    exact = (base == DEADLINE_DELAY_SLOT and sep == "#" and str(index) == text
             and 0 <= index < len(endpoints))
    if exact:
        eid = endpoints[index]["logical_id"]
        delta = record["qos_delta"]
        exact = (len(delta) == 1 and delta[0]["logical_id"] == eid
                 and delta[0]["index"] == index and delta[0]["field"] == "deadline_sec")
        if exact:
            expected_after = deepcopy(before)
            expected_after["endpoints"][index]["qos"]["deadline_sec"] = delta[0]["value"]
            exact = canonical_hash(expected_after) == canonical_hash(after)
    if not exact:
        record["reasons"].append("deadline_delay_exact_endpoint_slot_mismatch")
    elif not fixture:
        # No requested_qos or controller config echo can substitute for qos.
        # _bound_creations already binds raw endpoint identity and create ACK.
        for created in (e for e in events if e.get("event") == "endpoint_created"
                        and e.get("logical_id") == eid):
            identity_keys = ("logical_id", "generation", "endpoint_generation", "pid",
                             "process_start_ticks", "run_id", "domain_id", "worker_role",
                             "topology_worker_role", "topology_logical_id", "topology_generation",
                             "backend", "real_ros", "rmw_identifier", "observed_domain_id")
            starts = [e for e in events if e.get("event") == "endpoint_create_begin"
                      and all(e.get(k) == created.get(k) for k in identity_keys)]
            action = next((a for a in program["actions"]
                           if a.get("op") in {"create", "recreate"}
                           and a.get("endpoint_id") == eid
                           and a.get("generation") == created.get("generation")), None)
            begins = [e for e in events if e.get("event") == "topology_action_begin"
                      and action is not None and e.get("action_id") == action["action_id"]]
            ends = [e for e in events if e.get("event") == "topology_action_done"
                    and action is not None and e.get("action_id") == action["action_id"]]
            source = ("get_publishers_info_by_topic" if endpoints[index]["role"] == "writer"
                      else "get_subscriptions_info_by_topic")
            if (len(starts) != 1 or len(begins) != 1 or len(ends) != 1
                    or created.get("identity_source") != source
                    or type(created.get("observed_domain_id")) is not int
                    or created.get("observed_domain_id") != created.get("domain_id")
                    or created.get("rmw_identifier") not in {"rmw_cyclonedds_cpp", "rmw_fastrtps_cpp"}):
                record["reasons"].append("deadline_delay_api_readback_binding_missing:" + eid)
                continue
            start, begin, end = starts[0], begins[0], ends[0]
            times = [begin.get("monotonic_ns"), start.get("monotonic_ns"),
                     created.get("created_monotonic_ns"), created.get("monotonic_ns"),
                     end.get("monotonic_ns")]
            indices = [e.get("event_index") for e in (begin, start, created, end)]
            if (any(type(t) is not int or t <= 0 for t in times)
                    or not times[0] < times[1] <= times[2] <= times[3] < times[4]
                    or created.get("create_begin_monotonic_ns") != times[1]
                    or any(type(i) is not int for i in indices)
                    or not indices[0] < indices[1] < indices[2] < indices[3]):
                record["reasons"].append("deadline_delay_api_readback_chronology_mismatch:" + eid)
            record["raw_refs"].append(start.get("event_index"))
    record["effect_scope"] = "endpoint_deadline_configuration_only_not_deadline_semantics_or_netem"
    record["effect_observation"] = "unknown_deadline_semantic_effect_not_measured"
    record["limitations"] = ["deadline_event_or_violation_not_measured",
                             "network_delay_not_measured_by_this_slot",
                             "configuration_intervention_is_not_semantic_effect"]
    return record


def _audit_filter_operator(layer_index, receipt, events, program, *, fixture):
    """Audit initial reader CFT creation/readback, without interpreting samples."""
    record = _record(layer_index, "filter.expr", "S1.topology.filter")
    record.update(filter_scope="initial_only", filter_false_implies_unmatch=False,
                  dynamic_update_covered=False, lease_covered=False,
                  effect_observed=False,
                  effect_observation="unknown_filter_sample_effect_not_measured")
    _public_configuration_intervention(record, receipt, program)
    before = {e.get("logical_id"): e for e in receipt["input_before"].get("endpoints", [])}
    after = {e.get("logical_id"): e for e in receipt["input_after"].get("endpoints", [])}
    changed = [eid for eid, spec in after.items() if eid in before
               and spec.get("filter") != before[eid].get("filter")]
    if not changed:
        record["reasons"].append("filter_input_delta_missing")
    final_specs = {e.get("logical_id"): e for e in program.get("endpoints", [])}
    proof_rows = []
    for eid in changed:
        spec, final_spec = after[eid], final_specs.get(eid)
        if (final_spec is None or spec.get("filter") != final_spec.get("filter")
                or spec.get("role") != "reader"):
            record["reasons"].append("filter_layer_overridden_or_not_reader:" + eid)
            continue
        expected = spec.get("filter")
        try:
            prepared = endpoint_expressibility(spec, {"rmw": "rmw_cyclonedds_cpp"})
            expected = prepared["initial_content_filter"]
        except TopologyUnsupported as exc:
            record["reasons"].append("filter_harness_rejected_input:" + exc.code)
            continue
        creations = _bound_creations(record, eid, spec, events, program, fixture=fixture)
        actions = [a for a in program.get("actions", [])
                   if a.get("endpoint_id") == eid and a.get("op") in {"create", "recreate"}]
        if len(creations) != len(actions) or not actions:
            record["reasons"].append("filter_created_generation_set_incomplete:" + eid)
            continue
        for action, token in zip(actions, creations):
            aid, gen = action["action_id"], action["generation"]
            begin = next((e for e in events if e.get("event") == "topology_action_begin"
                          and e.get("action_id") == aid), None)
            end = next((e for e in events if e.get("event") == "topology_action_done"
                        and e.get("action_id") == aid), None)
            if begin is None or end is None:
                record["reasons"].append("filter_create_action_missing:" + eid)
                continue
            expected_backend = OFFLINE_BACKEND if fixture else ROS_BACKEND
            expected_real_ros = not fixture
            identity = {"topology_logical_id": eid, "topology_generation": gen,
                        "logical_id": eid, "generation": gen,
                        "topology_worker_role": worker_role_for(
                            "reader", [e["logical_id"] for e in program["endpoints"]
                                       if e.get("role") == "reader"].index(eid)),
                        "role": worker_role_for(
                            "reader", [e["logical_id"] for e in program["endpoints"]
                                       if e.get("role") == "reader"].index(eid)),
                        "pid": token.get("pid"), "process_start_ticks": token.get("process_start_ticks"),
                        "endpoint_generation": token.get("endpoint_generation"),
                        "run_id": token.get("run_id"), "domain_id": token.get("domain_id"),
                        "backend": expected_backend, "real_ros": expected_real_ros}
            create_begins = [e for e in events if e.get("event") == "endpoint_create_begin"
                             and all(e.get(k) == v for k, v in identity.items())]
            rows = [e for e in events if e.get("event") in {
                "content_filter_initial_begin", "content_filter_initial_submitted",
                "content_filter_initial_configuration", "content_filter_initial_guard"}
                    and all(e.get(k) == v for k, v in identity.items())]
            by_event = {name: [e for e in rows if e.get("event") == name] for name in (
                "content_filter_initial_begin", "content_filter_initial_submitted",
                "content_filter_initial_configuration", "content_filter_initial_guard")}
            if (len(create_begins) != 1 or any(len(by_event[name]) != 1 for name in (
                    "content_filter_initial_begin", "content_filter_initial_submitted",
                    "content_filter_initial_configuration")) or by_event["content_filter_initial_guard"]):
                record["reasons"].append("filter_readback_chain_missing_ambiguous_or_guarded:" + eid)
                continue
            started, submitted, observed = (by_event[name][0] for name in (
                "content_filter_initial_begin", "content_filter_initial_submitted",
                "content_filter_initial_configuration"))
            nested = observed.get("filter_receipt")
            token_nested = token.get("initial_content_filter")
            api = nested.get("api_results") if isinstance(nested, dict) else None
            getter = (api or {}).get("get_content_filter")
            enabled = (api or {}).get("is_cft_enabled")
            expected_enabled = bool(expected and expected.get("expression", "").strip())
            expected = expected or {"expression": "", "parameters": []}
            create_returned = (nested or {}).get("creation_returned_monotonic_ns")
            getter_start = (getter or {}).get("started_monotonic_ns")
            getter_end = (getter or {}).get("completed_monotonic_ns")
            enabled_start = (enabled or {}).get("started_monotonic_ns")
            enabled_end = (enabled or {}).get("completed_monotonic_ns")
            action_start, action_end = begin.get("monotonic_ns"), end.get("monotonic_ns")
            worker_create_begin = create_begins[0]
            event_indices = [e.get("event_index") for e in
                             (begin, worker_create_begin, started, submitted, observed, token, end)]
            api_times = [create_returned, getter_start, getter_end, enabled_start, enabled_end]
            raw_getter = (getter or {}).get("raw_result")
            raw_enabled = (enabled or {}).get("raw_result")
            begin_receipt = started.get("filter_receipt") or {}
            submitted_receipt = submitted.get("filter_receipt") or {}
            valid = (
                isinstance(nested, dict) and nested == token_nested
                and nested.get("configuration_status") == "verified" and nested.get("configured") is True
                and nested.get("accepted") is True and nested.get("submitted") is True
                and nested.get("requested") == {**expected, "enabled": expected_enabled}
                and begin_receipt.get("requested") == {**expected, "enabled": expected_enabled}
                and submitted_receipt.get("requested") == {**expected, "enabled": expected_enabled}
                and submitted_receipt.get("accepted") is True
                and submitted_receipt.get("submitted") is True
                and nested.get("scope") == "initial_only" and nested.get("fixture") is fixture
                and nested.get("readback_status") == "matched"
                and (getter or {}).get("status") == "observed"
                and (enabled or {}).get("status") == "observed"
                and isinstance(raw_getter, dict)
                and raw_getter.get("filter_expression") == expected["expression"]
                and raw_getter.get("expression_parameters") == expected["parameters"]
                and type(raw_enabled) is bool and raw_enabled is expected_enabled
                and nested.get("observed") == expected and nested.get("is_cft_enabled") is expected_enabled
                and token.get("created_monotonic_ns") == create_returned
                and token.get("create_begin_monotonic_ns") == worker_create_begin.get("monotonic_ns")
                and all(type(i) is int for i in event_indices)
                and all(a < b for a, b in zip(event_indices, event_indices[1:]))
                and all(type(t) is int for t in [action_start, action_end, *api_times])
                and action_start <= worker_create_begin["monotonic_ns"] <= started["monotonic_ns"]
                <= submitted["monotonic_ns"]
                <= observed["monotonic_ns"] <= token["monotonic_ns"] <= action_end
                and action_start <= create_returned <= getter_start <= getter_end
                <= enabled_start <= enabled_end <= action_end
                and type(nested.get("monotonic_ns")) is int
                and enabled_end <= nested["monotonic_ns"] <= observed["monotonic_ns"]
                and observed.get("role") == identity["topology_worker_role"]
                and token.get("role") == identity["topology_worker_role"]
                and all(e.get("worker_role") == identity["topology_worker_role"]
                        for e in (started, submitted, observed) if "worker_role" in e)
                and nested.get("candidate_eligible") is False
                and nested.get("mutation_receipt") is False
                and nested.get("filter_effect_observed") is None
                and nested.get("filter_false_implies_unmatch") is False
                and nested.get("dynamic_update_covered") is False
                and nested.get("lease_covered") is False)
            proof_rows.append({"logical_id": eid, "generation": gen,
                               "worker_role": identity["topology_worker_role"],
                               "expected": deepcopy(expected), "expected_enabled": expected_enabled,
                               "raw_getter": deepcopy(raw_getter), "raw_enabled": raw_enabled,
                               "verified": bool(valid), "event_indices": event_indices,
                               "times_monotonic_ns": api_times})
            record["raw_refs"].extend(event_indices)
            if not valid:
                record["reasons"].append("filter_raw_readback_identity_or_chronology_mismatch:"
                                          + eid + ":g" + str(gen))
    record["filter_generations"] = proof_rows
    record["configuration_application_observed"] = bool(proof_rows) and not record["reasons"]
    record["configuration_intervention_proven"] = bool(proof_rows) and not record["reasons"]
    record["status"] = ("fixture_application_observed" if fixture else "artifact_application_observed") \
        if record["configuration_application_observed"] else "unknown"
    record["effect_scope"] = "initial_reader_filter_configuration_only"
    record["limitations"] = ["sample_filter_effect_not_measured",
                             "filter_false_does_not_imply_unmatch",
                             "dynamic_filter_update_not_measured",
                             "lease_lifecycle_not_measured"]
    return record


def audit_operator_application(plan_receipts, events, program, *, fixture):
    """Per-layer records for every operator this module can attest.

    Operators outside :data:`AUDITED_OPERATORS` and the exact deadline-delay slot
    are returned with an explicit
    named gap so a caller can merge this mapping without treating them as
    either supported or as an SUT effect.
    """
    records = {}
    for index, receipt in enumerate(plan_receipts):
        operator = receipt.get("operator_id")
        deadline_delay = is_deadline_configuration_delay(receipt)
        intervention = operator in INTERVENTION_OPERATORS or deadline_delay
        if operator not in AUDITED_OPERATORS and not deadline_delay:
            record = _record(index, operator, receipt.get("slot_id", ""))
            record.update(status="unsupported",
                          reasons=["operator_application_auditor_not_implemented:" + str(operator)])
            records[index] = record
        elif deadline_delay:
            records[index] = audit_deadline_delay(index, receipt, events, program, fixture=fixture)
        elif operator == "relation.pair":
            records[index] = audit_relation_pair(index, receipt, events, program, fixture=fixture)
        elif operator in TIMING_OPERATORS:
            records[index] = audit_timing_operator(index, receipt, events, program,
                                                   fixture=fixture, operator=operator)
        elif operator in FILTER_OPERATORS:
            records[index] = _audit_filter_operator(index, receipt, events, program, fixture=fixture)
        else:
            record = _record(index, operator, receipt.get("slot_id", "").split("#", 1)[0])
            delta = layer_qos_delta(receipt["input_before"], receipt["input_after"])
            record["qos_delta"] = deepcopy(delta)
            if not delta:
                record["reasons"].append("configuration_delta_is_not_a_qos_field")
            else:
                _check_configuration(record, delta, events, program, receipt["input_after"], fixture=fixture)
                record["effect_scope"] = "endpoint_public_configuration_only_not_timed_lifecycle"
            records[index] = record
        if intervention and operator != "filter.expr":
            _public_configuration_intervention(records[index], receipt, program)
        if (not records[index]["reasons"] and records[index]["status"] == "unknown"
                and operator not in FILTER_OPERATORS):
            records[index]["configuration_application_observed"] = True
            records[index]["status"] = ("fixture_application_observed" if fixture
                                        else "artifact_application_observed")
            if intervention and operator not in FILTER_OPERATORS:
                records[index]["configuration_intervention_proven"] = True
                records[index]["hit"] = False  # route grants hit only after formal qualification
            # Only the enclosing route can qualify formal execution from raw
            # environment/source/lock/worker/cleanup gates, never this helper.
            # hit stays false: an applied configuration is not a mutation hit.
    return records
