"""Versioned, pure N-endpoint state abstraction, not an execution state machine.

Only program-fixed logical IDs, current *logical* generation ordinals and the
components below enter state identity. Raw GID/PID/time/domain and generation
history belong in projection receipts, never in a state. No ROS/runtime imports,
I/O, prediction, oracle, candidate admission or maturity accounting occur here.
"""
from r2f.core.evidence import canonical_hash

SCHEMA = "r2f.s1-topology-state/v1"
EDGE_SCHEMA = "r2f.s1-topology-state-edge/v1"
PROGRAM_SCHEMA = "r2f.s1-topology-program/v1"
GLOBAL_DOMAINS = {
    "network_phase": {"normal", "drop_active", "restore_pending", "restored_verified", "unknown"},
    "recovery_cycle_bucket": {"0", "1", "2plus", "unknown"},
    "observation_completeness": {"complete", "incomplete"},
}
PAIR_DOMAINS = {
    "compatibility": {"compatible", "incompatible", "not_applicable", "unknown"},
    "observed_match": {"bidirectional", "writer_only", "reader_only", "none", "not_applicable", "unknown"},
    "delivery_closure": {"no_obligation", "open", "closed", "closed_with_gap", "not_applicable", "unknown"},
}
LIVENESS = {"alive", "retired", "absent", "unknown"}
STATE_KEYS = {"schema", "endpoints", "pairs", *GLOBAL_DOMAINS}


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _enum(value, choices, name):
    if not isinstance(value, str) or value not in choices:
        raise ValueError("invalid topology component " + name)
    return value


def logical_endpoints(program):
    """Return sorted program IDs/roles; never invent IDs from runtime ordering."""
    if (not isinstance(program, dict) or program.get("schema") != PROGRAM_SCHEMA
            or not isinstance(program.get("endpoints"), list)
            or not isinstance(program.get("actions"), list)):
        raise ValueError("topology program schema/endpoints/actions required")
    result = {}
    for ep in program["endpoints"]:
        if (not isinstance(ep, dict) or not _text(ep.get("logical_id"))
                or ep.get("role") not in ("writer", "reader")):
            raise ValueError("invalid topology logical endpoint")
        eid = ep["logical_id"]
        if eid in result:
            raise ValueError("duplicate topology logical endpoint: " + eid)
        result[eid] = ep["role"]
    return dict(sorted(result.items()))


def _generation(value):
    if value is not None and (type(value) is not int or value < 0):
        raise ValueError("generation must be a nonnegative logical integer or null")
    return value


def _complete(state):
    return (all(e["generation"] is not None and e["liveness"] != "unknown" for e in state["endpoints"])
            and all(p[c] != "unknown" for p in state["pairs"] for c in PAIR_DOMAINS)
            and all(state[c] != "unknown" for c in ("network_phase", "recovery_cycle_bucket")))


def abstract_topology_state(program, facts, *, identity_consistent=True):
    """Project typed facts only. Missing fields stay unknown, never program truth.

    ``facts.endpoints`` maps logical ID to {generation, liveness}; ``facts.pairs``
    is a list of {writer, reader, compatibility, observed_match, delivery_closure}.
    Extra diagnostic fields are ignored. The entire W x R graph is represented.
    ``observation_complete=True`` is necessary, not sufficient for completeness.
    """
    roles = logical_endpoints(program)
    if not isinstance(facts, dict):
        raise ValueError("topology facts must be a dict")
    if (identity_consistent is not True or facts.get("identity_consistent") is False
            or facts.get("endpoint_identity_consistent") is False):
        raise ValueError("topology state requires consistent endpoint identity")
    eps, pairs = facts.get("endpoints", {}), facts.get("pairs", [])
    if not isinstance(eps, dict) or set(eps) - set(roles) or not isinstance(pairs, list):
        raise ValueError("invalid topology endpoint/pair facts")
    state = {"schema": SCHEMA, "endpoints": [], "pairs": []}
    for eid, role in roles.items():
        row = eps.get(eid, {})
        if not isinstance(row, dict):
            raise ValueError("endpoint facts must be a dict: " + eid)
        state["endpoints"].append({"logical_id": eid, "role": role,
                                   "generation": row.get("generation"),
                                   "liveness": row.get("liveness", "unknown")})
    found = {}
    for row in pairs:
        if not isinstance(row, dict):
            raise ValueError("pair facts must be a dict")
        w, r = row.get("writer"), row.get("reader")
        if not _text(w) or not _text(r) or roles.get(w) != "writer" or roles.get(r) != "reader":
            raise ValueError("pair must reference program writer and reader")
        if (w, r) in found:
            raise ValueError("duplicate topology directed pair")
        found[w, r] = row
    for w in (eid for eid, role in roles.items() if role == "writer"):
        for r in (eid for eid, role in roles.items() if role == "reader"):
            row = found.get((w, r), {})
            state["pairs"].append({"writer": w, "reader": r,
                                   **{c: row.get(c, "unknown") for c in PAIR_DOMAINS}})
    for component in ("network_phase", "recovery_cycle_bucket"):
        state[component] = facts.get(component, "unknown")
    state["observation_completeness"] = (
        "complete" if facts.get("observation_complete") is True
        and facts.get("window_complete") is not False and _complete(state) else "incomplete")
    return validate_topology_state(state)


def validate_topology_state(state):
    """Strict schema validation and ordering canonicalization, with no raw noise."""
    if not isinstance(state, dict) or set(state) != STATE_KEYS or state.get("schema") != SCHEMA:
        raise ValueError("topology state requires exactly the declared schema/components")
    if not isinstance(state["endpoints"], list) or not isinstance(state["pairs"], list):
        raise ValueError("topology endpoints and pairs must be lists")
    eps, roles = [], {}
    for row in state["endpoints"]:
        if (not isinstance(row, dict) or set(row) != {"logical_id", "role", "generation", "liveness"}
                or not _text(row["logical_id"]) or row["role"] not in ("writer", "reader")):
            raise ValueError("invalid topology endpoint state")
        eid, gen = row["logical_id"], _generation(row["generation"])
        life = _enum(row["liveness"], LIVENESS, "liveness")
        if eid in roles:
            raise ValueError("duplicate topology logical endpoint")
        if ((life == "absent" and gen != 0)
                or (life in ("alive", "retired") and (gen is None or gen == 0))):
            raise ValueError("liveness and logical generation contradict")
        roles[eid] = row["role"]
        eps.append(dict(row))
    eps.sort(key=lambda row: row["logical_id"])
    pairs, seen = [], set()
    for row in state["pairs"]:
        if not isinstance(row, dict) or set(row) != {"writer", "reader", *PAIR_DOMAINS}:
            raise ValueError("invalid topology pair state")
        w, r = row["writer"], row["reader"]
        if not _text(w) or not _text(r) or roles.get(w) != "writer" or roles.get(r) != "reader":
            raise ValueError("topology pair role/identity mismatch")
        if (w, r) in seen:
            raise ValueError("duplicate topology directed pair")
        seen.add((w, r))
        for c, domain in PAIR_DOMAINS.items():
            _enum(row[c], domain, c)
        live = {ep["logical_id"]: ep["liveness"] for ep in eps}
        if all(live[eid] == "alive" for eid in (w, r)) and any(row[c] == "not_applicable" for c in PAIR_DOMAINS):
            raise ValueError("live topology pair cannot be not_applicable")
        pairs.append(dict(row))
    if seen != {(w, r) for w in roles for r in roles if roles[w] == "writer" and roles[r] == "reader"}:
        raise ValueError("topology state must contain every directed writer-reader pair")
    pairs.sort(key=lambda row: (row["writer"], row["reader"]))
    result = {"schema": SCHEMA, "endpoints": eps, "pairs": pairs}
    for c, domain in GLOBAL_DOMAINS.items():
        result[c] = _enum(state[c], domain, c)
    if result["observation_completeness"] == "complete" and not _complete(result):
        raise ValueError("complete topology state has unknown components")
    return result


def topology_state_id(state):
    return canonical_hash(validate_topology_state(state))


def topology_edge_id(before_state_id, operation_family, parameter_class, after_state_id):
    for value in (before_state_id, operation_family, parameter_class, after_state_id):
        if not _text(value):
            raise ValueError("topology edge requires nonempty state IDs and operation labels")
    return canonical_hash({"schema": EDGE_SCHEMA, "before_state_id": before_state_id,
                           "operation_family": operation_family, "parameter_class": parameter_class,
                           "after_state_id": after_state_id})


def topology_observation_edge(record):
    """Pure structural coverage gate for the explicit N-projection receipt.

    The projection layer verifies raw bytes/chronology. This function checks that
    the exact states and every component still bind to that receipt; it cannot
    turn a hand-authored fixture, prediction or unknown projection into coverage.
    The shared observation-completeness gate is retained, even for direct calls.
    """
    from .observation_status import observation_complete
    if not observation_complete(record):
        return None
    if not isinstance(record, dict) or not isinstance(record.get("typed_facts"), dict):
        return None
    facts = record["typed_facts"]
    receipt = facts.get("state_projection_receipt")
    if (not isinstance(receipt, dict) or receipt.get("schema") != "r2f.s1-topology-state-projection/v1"
            or receipt.get("status") != "observed" or receipt.get("execution_evidence") is not True
            or receipt.get("fixture") is not False):
        return None
    path, digest = receipt.get("raw_path"), receipt.get("raw_sha256")
    if (not _text(path) or path != record.get("raw_path") or digest != record.get("raw_sha256")
            or not isinstance(digest, str) or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)):
        return None

    def valid_ref(ref, boundary=None):
        return (isinstance(ref, dict) and set(ref) == {"path", "sha256", "event_index"}
                and ref["path"] == path and ref["sha256"] == digest
                and type(ref["event_index"]) is int and ref["event_index"] >= 0
                and (boundary is None or ref["event_index"] <= boundary))

    def pointer(value):
        return value.replace("~", "~0").replace("/", "~1")

    chronology = receipt.get("chronology")
    if (not isinstance(chronology, dict) or chronology.get("status") != "verified"
            or chronology.get("component_refs_bounded") is not True
            or chronology.get("snapshot_ack_kind") != "command_completion_not_dds_delivery_ack"):
        return None
    states, boundaries = [], []
    for side in ("before", "after"):
        binding = receipt.get(side)
        if not isinstance(binding, dict) or not valid_ref(binding.get("boundary_ref")):
            return None
        boundary = binding["boundary_ref"]["event_index"]
        if chronology.get(side + "_ref") != binding["boundary_ref"]:
            return None
        boundaries.append(boundary)
        try:
            state = validate_topology_state(facts.get("state_" + side))
        except (ValueError, TypeError):
            return None
        if (state["observation_completeness"] != "complete"
                or binding.get("state_id") != topology_state_id(state)):
            return None
        expected = {"/network_phase", "/recovery_cycle_bucket", "/observation_completeness"}
        for ep in state["endpoints"]:
            expected.update("/endpoints/" + pointer(ep["logical_id"]) + "/" + name
                            for name in ("generation", "liveness"))
        for pair in state["pairs"]:
            expected.update("/pairs/" + pointer(pair["writer"]) + "/" + pointer(pair["reader"]) + "/" + name
                            for name in PAIR_DOMAINS)
        refs = binding.get("component_refs")
        if not isinstance(refs, dict) or set(refs) != expected:
            return None
        values = {"/network_phase": state["network_phase"],
                  "/recovery_cycle_bucket": state["recovery_cycle_bucket"],
                  "/observation_completeness": state["observation_completeness"]}
        for ep in state["endpoints"]:
            for name in ("generation", "liveness"):
                values["/endpoints/" + pointer(ep["logical_id"]) + "/" + name] = ep[name]
        for pair in state["pairs"]:
            for name in PAIR_DOMAINS:
                values["/pairs/" + pointer(pair["writer"]) + "/" + pointer(pair["reader"]) + "/" + name] = pair[name]
        for name, component in refs.items():
            if (not isinstance(component, dict) or component.get("value") != values[name]
                    or component.get("status") not in ("observed", "not_applicable")
                    or not isinstance(component.get("refs"), list) or not component["refs"]
                    or any(not valid_ref(ref, boundary) for ref in component["refs"])):
                return None
        identities, history = binding.get("identity"), binding.get("history")
        if not isinstance(identities, dict) or not isinstance(history, dict):
            return None
        created = {ep["logical_id"]: ep for ep in state["endpoints"] if ep["generation"] > 0}
        if set(identities) != set(created) or set(history) != {ep["logical_id"] for ep in state["endpoints"]}:
            return None
        for eid, events in history.items():
            if not isinstance(events, list) or any(not valid_ref(ref, boundary) for ref in events):
                return None
            if eid in created:
                token = identities[eid]
                if (not isinstance(token, dict) or type(token.get("generation")) is not int
                        or token["generation"] != created[eid]["generation"]
                        or not _text(token.get("endpoint_gid")) or not _text(token.get("endpoint_generation"))
                        or type(token.get("pid")) is not int or token["pid"] <= 0
                        or type(token.get("process_start_ticks")) is not int or token["process_start_ticks"] <= 0
                        or not valid_ref(token.get("creation_ref"), boundary)
                        or token["creation_ref"] not in events):
                    return None
        states.append(state)
    if boundaries[0] >= boundaries[1] or not valid_ref(receipt.get("input_ref"), boundaries[0]):
        return None
    # The logical ID/role universe must be fixed by one input program throughout.
    if ([(e["logical_id"], e["role"]) for e in states[0]["endpoints"]]
            != [(e["logical_id"], e["role"]) for e in states[1]["endpoints"]]):
        return None
    program_hash = receipt.get("program_sha256")
    if (not isinstance(program_hash, str) or len(program_hash) != 64
            or any(c not in "0123456789abcdef" for c in program_hash)):
        return None
    before, after = map(topology_state_id, states)
    family = record.get("operation_family") or record.get("surface_id") or "unknown"
    parameter = record.get("parameter_class") or record.get("boundary_class") or "unknown"
    try:
        return before, topology_edge_id(before, family, parameter, after)
    except ValueError:
        return None
