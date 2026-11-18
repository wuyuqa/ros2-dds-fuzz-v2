"""Minimal, pure state abstraction for exploration feedback (plan section 7.1).

This module computes *abstract* state and transition identities for one fixed
writer/reader pair by default. Explicit schema dispatch delegates the versioned
N-endpoint abstraction to topology_state; no shape-based detection occurs.
It never executes ROS, never reads raw files, and never
claims that a state was reached: reaching a state requires a separate,
raw-backed execution observation.

Design constraints:

* Only the declared abstract components enter ``state_id``.  GUIDs, PIDs,
  wall-clock times, domains, run ids and paths are not components, so they
  cannot fragment the same abstract state.
* Identity consistency is checked *before* abstraction.  An inconsistent
  identity is an evidence error, not a new state.
* Incomplete observations are labelled with a distinct completeness value and
  are never eligible to refresh coverage.
"""

from ._json import nonempty, snapshot
from .evidence import canonical_hash
from .observation_status import observation_complete

STATE_ABSTRACTION_VERSION = "r2f.s1-state-abstraction/v1"
EDGE_SCHEMA = "r2f.s1-state-edge/v1"

STATE_COMPONENTS = (
    "live_roles", "creation_order_class", "compatibility_class",
    "observed_match_class", "network_phase", "recovery_cycle_bucket",
    "delivery_closure_class", "observation_completeness",
)

COMPONENT_DOMAINS = {
    "live_roles": frozenset({"none", "writer", "reader", "both", "unknown"}),
    "creation_order_class": frozenset({"writer_first", "reader_first", "unresolved"}),
    "compatibility_class": frozenset({"compatible", "incompatible", "unknown"}),
    "observed_match_class": frozenset({"bidirectional", "single_side", "none", "unknown"}),
    "network_phase": frozenset({"normal", "drop_active", "restore_pending", "restored_verified", "unknown"}),
    "recovery_cycle_bucket": frozenset({"0", "1", "2plus", "unknown"}),
    "delivery_closure_class": frozenset({"no_obligation", "open", "closed_within_contract", "unknown"}),
    "observation_completeness": frozenset({"complete", "incomplete"}),
}

# Keys that are runtime identity/observation plumbing.  They are read for the
# consistency check but can never become state components.
_IDENTITY_ONLY_KEYS = frozenset({
    "guid", "writer_guid", "reader_guid", "endpoint_guid", "participant_guid",
    "pid", "writer_pid", "reader_pid", "process_id",
    "time", "timestamp", "event_time", "started_at", "finished_at", "observed_at",
    "run_id", "campaign_id", "domain", "domain_id", "origin_domain",
    "raw_path", "raw_sha256", "output_dir", "repeat", "seed", "rng_seed",
    "edge_id", "state_id", "state_before", "state_after", "transition_id",
    "observed_edge", "surface_id", "boundary_class", "parameter_class",
    "operation_family", "oracle_verdict", "candidate_source", "evidence_tier",
})


def abstract_state(facts, *, identity_consistent=True, schema=STATE_ABSTRACTION_VERSION, program=None):
    """Return the eight-component abstraction for one observed pair state.

    ``facts`` may be a richer typed-facts mapping; only declared components are
    read.  Missing components become ``unknown`` rather than being inferred, and
    ``observation_completeness`` becomes ``incomplete`` if the caller did not
    affirm completeness.  An identity mismatch raises instead of silently
    producing a different state id.
    """
    if schema != STATE_ABSTRACTION_VERSION:
        from .topology_state import SCHEMA, abstract_topology_state
        if schema != SCHEMA:
            raise ValueError("unsupported state schema: " + str(schema))
        return abstract_topology_state(program, facts, identity_consistent=identity_consistent)
    if not isinstance(facts, dict):
        raise ValueError("state facts must be a dict")
    if identity_consistent is not True:
        raise ValueError("state abstraction requires consistent endpoint identity")
    if facts.get("identity_consistent") is False or facts.get("endpoint_identity_consistent") is False:
        raise ValueError("state abstraction requires consistent endpoint identity")

    # Identity/plumbing keys may be present in a genuine observation bundle, but
    # the abstraction is derived from the component keys only, so GUID/PID/time
    # values cannot change the state id.  _IDENTITY_ONLY_KEYS documents that
    # boundary and is used by tests that add those keys deliberately.

    state = {}
    for component in STATE_COMPONENTS:
        value = facts.get(component, "unknown")
        if component == "observation_completeness":
            # Completeness is derived from the observation flags; it can never
            # be upgraded by a caller-supplied label.
            complete_flag = facts.get("observation_complete")
            window_ok = facts.get("window_complete") is not False
            value = "complete" if complete_flag is True and window_ok else "incomplete"
        if value not in COMPONENT_DOMAINS[component]:
            raise ValueError(f"invalid state component {component}: {value!r}")
        state[component] = value
    return state


def validate_state(state, *, schema=STATE_ABSTRACTION_VERSION):
    if schema != STATE_ABSTRACTION_VERSION:
        from .topology_state import SCHEMA, validate_topology_state
        if schema != SCHEMA:
            raise ValueError("unsupported state schema: " + str(schema))
        return validate_topology_state(state)
    if not isinstance(state, dict):
        raise ValueError("state must be a dict")
    if set(state) != set(STATE_COMPONENTS):
        raise ValueError("state must contain exactly the declared components")
    for component in STATE_COMPONENTS:
        if state[component] not in COMPONENT_DOMAINS[component]:
            raise ValueError(f"invalid state component {component}: {state[component]!r}")
    return {component: state[component] for component in STATE_COMPONENTS}


def state_id(state, *, schema=STATE_ABSTRACTION_VERSION):
    """Stable abstract state id; excludes GUID/PID/time by construction."""
    if schema != STATE_ABSTRACTION_VERSION:
        from .topology_state import SCHEMA, topology_state_id
        if schema != SCHEMA:
            raise ValueError("unsupported state schema: " + str(schema))
        return topology_state_id(state)
    state = snapshot(state)
    if not isinstance(state, dict) or set(state) != set(STATE_COMPONENTS):
        raise ValueError("state_id requires exactly the declared components")
    for component in STATE_COMPONENTS:
        if state[component] not in COMPONENT_DOMAINS[component]:
            raise ValueError(f"invalid state component {component}: {state[component]!r}")
    return canonical_hash({"abstraction_version": STATE_ABSTRACTION_VERSION, **state})


def edge_id(before_state_id, operation_family, parameter_class, after_state_id, *, schema=STATE_ABSTRACTION_VERSION):
    """Transition identity: before state, operation family, parameter class, after."""
    if schema != STATE_ABSTRACTION_VERSION:
        from .topology_state import SCHEMA, topology_edge_id
        if schema != SCHEMA:
            raise ValueError("unsupported state schema: " + str(schema))
        return topology_edge_id(before_state_id, operation_family, parameter_class, after_state_id)
    nonempty(before_state_id, "edge.before_state_id")
    nonempty(operation_family, "edge.operation_family")
    nonempty(parameter_class, "edge.parameter_class")
    nonempty(after_state_id, "edge.after_state_id")
    return canonical_hash({
        "schema": EDGE_SCHEMA,
        "before_state_id": before_state_id,
        "operation_family": operation_family,
        "parameter_class": parameter_class,
        "after_state_id": after_state_id,
    })


def coverage_eligible(record):
    """The shared observation gate; state edges have an additional receipt gate."""
    return observation_complete(record)


def _projection_states(record):
    facts = record["typed_facts"]
    receipt = facts.get("state_projection_receipt")
    if not isinstance(receipt, dict):
        return None
    if (receipt.get("schema") != "r2f.s1-state-projection/v1"
            or receipt.get("status") != "observed"
            or receipt.get("scope") != "canonical_pair"
            or receipt.get("reasons") != []):
        return None
    path, digest = receipt.get("raw_path"), receipt.get("raw_sha256")
    if not isinstance(path, str) or not path:
        return None
    if (not isinstance(digest, str) or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)):
        return None
    if path != record.get("raw_path") or digest != record.get("raw_sha256"):
        return None
    def valid_ref(ref, limit=None):
        return (isinstance(ref, dict) and ref.get("path") == path
                and ref.get("sha256") == digest
                and type(ref.get("event_index")) is int and ref["event_index"] >= 0
                and (limit is None or ref["event_index"] <= limit))

    states = []
    boundaries = []
    for side in ("before", "after"):
        binding = receipt.get(side)
        if not isinstance(binding, dict) or not valid_ref(binding.get("boundary_ref")):
            return None
        boundary = binding["boundary_ref"]["event_index"]
        boundaries.append(boundary)
        refs = binding.get("component_refs")
        if not isinstance(refs, dict) or set(refs) != set(STATE_COMPONENTS):
            return None
        for component in STATE_COMPONENTS:
            evidence = refs[component]
            if (not isinstance(evidence, dict)
                    or evidence.get("status") not in ("observed", "not_applicable")
                    or not isinstance(evidence.get("refs"), list) or not evidence["refs"]
                    or any(not valid_ref(ref, boundary) for ref in evidence["refs"])):
                return None
            if evidence["status"] == "not_applicable" and not (
                    isinstance(evidence.get("reason"), str) and evidence["reason"]):
                return None
        snapshot = facts.get("state_" + side)
        if not isinstance(snapshot, dict) or not set(STATE_COMPONENTS) <= set(snapshot):
            return None
        for flag in ("observation_complete", "window_complete", "endpoint_identity_consistent",
                     "identity_consistent"):
            if flag in snapshot and snapshot[flag] is not True:
                return None
        try:
            state = validate_state({key: snapshot[key] for key in STATE_COMPONENTS})
        except (ValueError, TypeError):
            return None
        if state["observation_completeness"] != "complete":
            return None
        identity = binding.get("identity")
        if not isinstance(identity, dict):
            return None
        live = state["live_roles"]
        expected_roles = {"none": set(), "writer": {"writer"}, "reader": {"reader"},
                          "both": {"writer", "reader"}}.get(live)
        if expected_roles is not None and set(identity) != expected_roles:
            return None
        if set(identity) - {"writer", "reader"}:
            return None
        for token in identity.values():
            if (not isinstance(token, dict)
                    or not isinstance(token.get("endpoint_gid"), str) or not token["endpoint_gid"]
                    or type(token.get("pid")) is not int or token["pid"] <= 0
                    or not isinstance(token.get("endpoint_generation"), str) or not token["endpoint_generation"]
                    or not valid_ref(token.get("endpoint_ref"), boundary)):
                return None
        states.append(state)
    if boundaries[0] >= boundaries[1]:
        return None
    return states


def observation_edge(record):
    """Derive an edge only from an explicitly versioned raw-bound projection.

    The legacy pair receipt keeps its old contract; only the explicit topology
    receipt schema delegates to the N-endpoint gate. No fallback to top-level facts or user supplied edge ids.  Explicit unknown
    domain values are allowed, but missing components are never filled in.
    The adapter verifies raw chronology; this pure function checks its binding.
    """
    if not coverage_eligible(record):
        return None
    receipt = record["typed_facts"].get("state_projection_receipt")
    if isinstance(receipt, dict) and receipt.get("schema") == "r2f.s1-topology-state-projection/v1":
        from .topology_state import topology_observation_edge
        return topology_observation_edge(record)
    states = _projection_states(record)
    if states is None:
        return None
    before, after = (state_id(state) for state in states)
    family = record.get("operation_family") or record.get("surface_id") or "unknown"
    parameter = record.get("parameter_class") or record.get("boundary_class") or "unknown"
    return before, edge_id(before, family, parameter, after)
