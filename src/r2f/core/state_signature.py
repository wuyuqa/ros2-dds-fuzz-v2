"""Minimal, pure state abstraction for exploration feedback (plan section 7.1).

This module computes *abstract* state and transition identities for one fixed
writer/reader pair.  It never executes ROS, never reads raw files, and never
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


def abstract_state(facts, *, identity_consistent=True):
    """Return the eight-component abstraction for one observed pair state.

    ``facts`` may be a richer typed-facts mapping; only declared components are
    read.  Missing components become ``unknown`` rather than being inferred, and
    ``observation_completeness`` becomes ``incomplete`` if the caller did not
    affirm completeness.  An identity mismatch raises instead of silently
    producing a different state id.
    """
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


def validate_state(state):
    if not isinstance(state, dict):
        raise ValueError("state must be a dict")
    if set(state) != set(STATE_COMPONENTS):
        raise ValueError("state must contain exactly the declared components")
    for component in STATE_COMPONENTS:
        if state[component] not in COMPONENT_DOMAINS[component]:
            raise ValueError(f"invalid state component {component}: {state[component]!r}")
    return {component: state[component] for component in STATE_COMPONENTS}


def state_id(state):
    """Stable abstract state id; excludes GUID/PID/time by construction."""
    state = snapshot(state)
    if not isinstance(state, dict) or set(state) != set(STATE_COMPONENTS):
        raise ValueError("state_id requires exactly the declared components")
    for component in STATE_COMPONENTS:
        if state[component] not in COMPONENT_DOMAINS[component]:
            raise ValueError(f"invalid state component {component}: {state[component]!r}")
    return canonical_hash({"abstraction_version": STATE_ABSTRACTION_VERSION, **state})


def edge_id(before_state_id, operation_family, parameter_class, after_state_id):
    """Transition identity: before state, operation family, parameter class, after."""
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
    """Whether an execution observation may refresh coverage.

    Plans, seeds, tool errors, non-judged stages and incomplete observations are
    never coverage.  This function performs no additional execution claims: it
    only filters records that already carry real observation fields.
    """
    if not isinstance(record, dict):
        raise ValueError("coverage record must be a dict")
    if record.get("execution_stage") != "judged":
        return False
    if record.get("tool_error") is True or record.get("reason_code") == "tool_error":
        return False
    facts = record.get("typed_facts")
    if not isinstance(facts, dict) or not facts:
        return False
    if facts.get("observation_complete") is not True:
        return False
    if facts.get("window_complete") is False:
        return False
    if facts.get("endpoint_identity_consistent") is False:
        return False
    return True


def observation_edge(record):
    """Return ``(state_id, edge_id)`` for an eligible, complete observation.

    Returns ``None`` when the record is not eligible (no coverage refresh).
    ``facts.state_before``/``state_after`` may be either already abstracted
    component dicts or typed facts with components; they are abstracted here.
    """
    if not coverage_eligible(record):
        return None
    facts = record["typed_facts"]
    before = facts.get("state_before") if isinstance(facts.get("state_before"), dict) else facts
    after = facts.get("state_after") if isinstance(facts.get("state_after"), dict) else None
    if after is None:
        return None
    before_state = abstract_state(before, identity_consistent=True)
    after_state = abstract_state(after, identity_consistent=True)
    after_id = state_id(after_state)
    family = record.get("operation_family") or record.get("surface_id") or "unknown"
    parameter = record.get("parameter_class") or record.get("boundary_class") or "unknown"
    return state_id(before_state), edge_id(state_id(before_state), family, parameter, after_id)
