"""Relation-surface input model for the G01 and W01 gaps.

This module is deliberately advisory and offline.  It does not start ROS, create
an endpoint/participant/context, run a campaign, write a receipt into a runner,
or decide a candidate.  It supplies three things the unified provider can wire:

* a strict, auditable policy model for the public rclpy QoS surface (which
  fields exist, which do not, and exactly why a submitted input is accepted,
  rejected as an input guard, or rejected because the current runtime binding
  cannot carry it);
* per-pair G01 relation plans and W01 depth/count relation plans whose
  before/after content hashes and API binding receipts prove that the input
  actually changed;
* an explicit replacement-point declaration naming the legacy refusals in
  ``fuzz_catalog.surface_to_core`` (W01) and ``runtime.ros2_pair`` (G01).  The
  legacy refusals stay in place until main wires this module; importing or
  calling this module never deletes them.

Non-negotiable semantics:

* A legal value is an accepted *input* with an observation expectation, never
  an oracle pass.
* An illegal value or unmet precondition yields ``guard_reject`` with a named
  reason; it is not a SUT failure.
* A value the public API exposes but the current worker binding cannot carry
  (``KEEP_ALL`` today) yields ``binding_pending``; it is not a SUT failure and
  it is not silently normalised to ``KEEP_LAST``.
* ``ownership``, ``ownership_strength`` and ``partitions`` are not exposed by
  the Jazzy ``rclpy.qos.QoSProfile`` slots.  They are modelled as
  ``unavailable_api`` and may never be inferred from an absent observation.
* ``history``/``depth`` are local resource policies, not DDS match policies.
  They are never used to infer writer/reader compatibility or a match.
"""
from __future__ import annotations

import copy
import hashlib
import json

SCHEMA = "r2f.s1.relation-surfaces/v1"
W01_SURFACE_ID = "W01"
G01_SURFACE_ID = "G01"
INFINITE_QOS_NS = 9223372036854775807
MAX_HISTORY_DEPTH = 10000

#: Replacement points that main must wire.  These are paths plus exact legacy
#: behaviour; they are *not* changed by this module.
REPLACEMENT_POINTS = {
    "G01": {
        "legacy_refusal": "src/r2f/runtime/ros2_pair.py:990 "
                          "blocked:unsupported_capability:pair_relation_observation_not_implemented",
        "legacy_capability": "src/r2f/runtime/ros2_pair.py:289 "
                             "backend=unsupported_relation_observation",
        "wire_into": "ros2_pair.run_pair dispatch -> runtime.relation_observer.edges_from_events "
                     "+ modules.s1.oracles.evaluate_match_compatibility_guard per edge",
        "required_runtime_evidence": [
            "endpoint_created for every writer/reader with role, gid and generation",
            "qos_binding_observed or graph entry for the submitted per-role policies",
            "bidirectional graph snapshots that contain the exact writer gid in publishers "
            "and reader gid in subscriptions",
            "per-edge delivery rows whose publisher_gid (or explicit endpoint correlation) "
            "binds them to that writer/reader pair",
            "a window_open/window_close pair named pair_compatibility_window that encloses "
            "the fresh match snapshot and the judged delivery rows",
        ],
        "must_not": "never reconstruct match/delivery from config or from aggregate match counts",
    },
    "W01": {
        "legacy_refusal": "src/r2f/modules/s1/fuzz_catalog.py:884 "
                          "surface_to_core raises UnsupportedCapabilityError for W01/M04",
        "legacy_capability": "src/r2f/modules/s1/fuzz_catalog.py:1078 f2_generation_domain "
                             "is generation-only (runtime_binding is empty)",
        "wire_into": "S1MutationProvider.prepare -> relation_surfaces.build_w01_mutation "
                     "+ runtime.relation_observer.observe_w01_delivery",
        "required_runtime_evidence": [
            "submitted qos.depth/history and pre_match_count/post_match_count",
            "qos_binding_observed or graph entry proving the depth/history reached the API",
            "per-edge send/receive sequences with writer/reader identity and generation",
            "a reviewed delivery obligation before any depth_lt_post closure may fail the SUT",
        ],
        "must_not": "never substitute uniform range sampling for the M04 relation classes",
    },
}

#: Public Jazzy rclpy QoSProfile fields.  Source: rclpy.qos.QoSProfile.__slots__
#: in the installed Jazzy tree.  ``ownership``/``ownership_strength``/
#: ``partitions`` are listed separately below because they intentionally do not
#: appear here.
_PUBLIC_FIELDS = {
    "reliability": {
        "api": "rclpy.qos.QoSProfile.reliability",
        "kwarg": "reliability",
        "values": ("reliable", "best_effort"),
        "kind": "enum",
        "match_policy": True,
    },
    "durability": {
        "api": "rclpy.qos.QoSProfile.durability",
        "kwarg": "durability",
        "values": ("volatile", "transient_local"),
        "kind": "enum",
        "match_policy": True,
    },
    "deadline_ns": {
        "api": "rclpy.qos.QoSProfile.deadline",
        "kwarg": "deadline",
        "values": None,
        "kind": "duration_ns",
        "match_policy": True,
    },
    "liveliness": {
        "api": "rclpy.qos.QoSProfile.liveliness",
        "kwarg": "liveliness",
        "values": ("automatic", "manual_by_topic"),
        "kind": "enum",
        "match_policy": True,
    },
    "liveliness_lease_duration_ns": {
        "api": "rclpy.qos.QoSProfile.liveliness_lease_duration",
        "kwarg": "liveliness_lease_duration",
        "values": None,
        "kind": "duration_ns",
        "match_policy": True,
    },
    "history": {
        "api": "rclpy.qos.QoSProfile.history",
        "kwarg": "history",
        "values": ("keep_last", "keep_all"),
        "kind": "enum",
        "match_policy": False,
    },
    "depth": {
        "api": "rclpy.qos.QoSProfile.depth",
        "kwarg": "depth",
        "values": None,
        "kind": "int",
        "match_policy": False,
    },
}

#: Fields a DDS QoS profile may conceptually carry but Jazzy rclpy does not
#: expose through QoSProfile.  They are not rejected as unknown; they are
#: reported as unavailable API so no caller can pass a fabricated value.
_UNAVAILABLE_FIELDS = {
    "ownership": {
        "api": None,
        "reason": "rclpy.qos.QoSProfile.__slots__ has no ownership field",
        "disposition": "unavailable_api",
    },
    "ownership_strength": {
        "api": None,
        "reason": "rclpy.qos.QoSProfile.__slots__ has no ownership_strength field",
        "disposition": "unavailable_api",
    },
    "partitions": {
        "api": None,
        "reason": "rclpy.qos.QoSProfile has no partition field; partitions are a node/context "
                  "policy not exposed by this public API",
        "disposition": "unavailable_api",
    },
}

POLICY_FIELDS = tuple(_PUBLIC_FIELDS) + tuple(_UNAVAILABLE_FIELDS)

MATCH_POLICY_FIELDS = tuple(name for name, spec in _PUBLIC_FIELDS.items()
                            if spec["match_policy"])
NON_MATCH_POLICY_FIELDS = tuple(name for name, spec in _PUBLIC_FIELDS.items()
                                if not spec["match_policy"])

#: G01 declared reliability classes.  BR is a legal API value and an
#: incompatible relation, not an invalid input.
G01_PAIR_CLASSES = ("RR", "RB", "BB", "BR")
G01_INCOMPATIBLE_PAIRS = frozenset({"BR"})
_RELIABILITY = {"R": "reliable", "B": "best_effort"}

#: W01 relation classes and the mutators this module actually implements.
W01_RELATION_CLASSES = ("depth_lt_post", "depth_eq_post", "depth_gt_post")
W01_IMPLEMENTED_MUTATORS = frozenset({"M04"})
G01_IMPLEMENTED_MUTATORS = frozenset({"M08"})
#: Catalog-declared operators this module deliberately does not use for the
#: relation surface, with the reason.  They stay visible in chain results
#: instead of disappearing, and they are never silently substituted:
#: M03 is a range sampler and may not stand in for the M04 relation witness;
#: M02 is an enum replacement and has no offered/requested pair semantics.
DECLINED_MUTATORS = {
    "W01": {"M03": "range_sampler_must_not_substitute_for_m04_relation_witness"},
    "G01": {"M02": "enum_replace_has_no_offered_requested_pair_semantics"},
}


class RelationSurfaceError(ValueError):
    """Strict input error with a stable machine-readable reason code."""

    def __init__(self, reason_code, detail=""):
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(reason_code + (":" + detail if detail else ""))


def canonical_sha256(value):
    """Deterministic content hash used for before/after mutation proof."""
    encoded = json.dumps(_plain(value), sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _plain(value):
    if hasattr(value, "to_dict"):
        return _plain(value.to_dict())
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise RelationSurfaceError("non_string_key")
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or type(value) in (str, int, bool):
        return value
    if type(value) is float:
        if value != value or value in (float("inf"), float("-inf")):
            raise RelationSurfaceError("non_finite_number")
        return value
    raise RelationSurfaceError("not_json_serializable", type(value).__name__)


def _reason(code, detail=""):
    return {"code": code, "detail": detail} if detail else {"code": code}


def _int_in(value, low, high):
    return type(value) is int and low <= value <= high


def field_capabilities():
    """Machine-readable view of supported and unavailable relation fields."""
    fields = {}
    for name, spec in _PUBLIC_FIELDS.items():
        fields[name] = {
            "field": name,
            "public_api": True,
            "api": spec["api"],
            "kwarg": spec["kwarg"],
            "kind": spec["kind"],
            "values": list(spec["values"]) if spec["values"] else None,
            "match_policy": spec["match_policy"],
            "disposition": "public_api_supported",
        }
    for name, spec in _UNAVAILABLE_FIELDS.items():
        fields[name] = {
            "field": name,
            "public_api": False,
            "api": None,
            "kwarg": None,
            "kind": None,
            "values": None,
            "match_policy": False,
            "disposition": spec["disposition"],
            "reason": spec["reason"],
        }
    return {
        "schema": SCHEMA,
        "source": "rclpy Jazzy QoSProfile.__slots__ "
                  "(history, depth, reliability, durability, lifespan, deadline, "
                  "liveliness, liveliness_lease_duration, "
                  "avoid_ros_namespace_conventions)",
        "fields": fields,
        "match_policy_fields": list(MATCH_POLICY_FIELDS),
        "non_match_policy_fields": list(NON_MATCH_POLICY_FIELDS),
        "unavailable_fields": sorted(_UNAVAILABLE_FIELDS),
    }


def _validate_public_field(name, value):
    """Return (accepted, reason_code, detail).  Never coerces a value."""
    spec = _PUBLIC_FIELDS[name]
    kind = spec["kind"]
    if kind == "enum":
        if not isinstance(value, str) or value not in spec["values"]:
            return False, "invalid_enum_value", "expected one of " + ",".join(spec["values"])
        return True, None, None
    if kind == "int":
        if type(value) is bool or type(value) is not int:
            return False, "invalid_integer_type", type(value).__name__
        if value == 0:
            # Jazzy QoSProfile warns and treats KEEP_LAST depth=0 as
            # SYSTEM_DEFAULT, so it carries no concrete queue semantics.  It is
            # an input guard rejection, never a submitted DDS mutation.
            return False, "depth_zero_keep_last_system_default", "0 has no concrete depth"
        if not 1 <= value <= MAX_HISTORY_DEPTH:
            return False, "integer_out_of_range", "1..%d" % MAX_HISTORY_DEPTH
        return True, None, None
    if kind == "duration_ns":
        if type(value) is bool or type(value) is not int:
            return False, "invalid_duration_type", type(value).__name__
        if not 0 <= value <= INFINITE_QOS_NS:
            return False, "duration_out_of_range", "0..%d" % INFINITE_QOS_NS
        return True, None, None
    raise RelationSurfaceError("unhandled_field_kind", kind)


def evaluate_policy_request(role, policy, *, supported_history=("keep_last",)):
    """Classify one role's submitted policy without running anything.

    ``disposition`` is one of ``accepted``, ``guard_reject``,
    ``binding_pending`` or ``unavailable_api``.  ``sut_failure`` is always
    False: input validation can never manufacture a SUT verdict.
    """
    if not isinstance(role, str) or not role:
        raise RelationSurfaceError("invalid_role")
    if not isinstance(policy, dict):
        raise RelationSurfaceError("policy_not_an_object", type(policy).__name__)
    receipts = []
    reasons = []
    normalized = {}
    unavailable = []
    binding = []
    for name in sorted(policy):
        value = policy[name]
        if name in _UNAVAILABLE_FIELDS:
            unavailable.append(name)
            reason = _UNAVAILABLE_FIELDS[name]["reason"]
            reasons.append(_reason("field_not_exposed_by_public_api", name))
            receipts.append({"field": name, "submitted": value, "accepted": False,
                             "disposition": "unavailable_api", "api": None,
                             "reason": reason})
            continue
        if name not in _PUBLIC_FIELDS:
            reasons.append(_reason("unknown_policy_field", name))
            receipts.append({"field": name, "submitted": value, "accepted": False,
                             "disposition": "guard_reject", "api": None,
                             "reason": "field is not in the public rclpy QoSProfile surface"})
            continue
        ok, code, detail = _validate_public_field(name, value)
        if not ok:
            reasons.append(_reason(code or "invalid_value", name + (":" + detail if detail else "")))
            receipts.append({"field": name, "submitted": value, "accepted": False,
                             "disposition": "guard_reject", "api": _PUBLIC_FIELDS[name]["api"],
                             "reason": code, "detail": detail})
            continue
        normalized[name] = value
        receipts.append({"field": name, "submitted": value, "accepted": True,
                         "disposition": "accepted", "api": _PUBLIC_FIELDS[name]["api"],
                         "kwarg": _PUBLIC_FIELDS[name]["kwarg"]})

    # Cross-field rules: history/depth are a local queue contract.
    history = normalized.get("history", policy.get("history", "keep_last"))
    if history == "keep_all":
        if "depth" in policy:
            reasons.append(_reason("depth_not_applicable_for_keep_all"))
            receipts.append({"field": "depth", "submitted": policy.get("depth"), "accepted": False,
                             "disposition": "guard_reject", "api": _PUBLIC_FIELDS["depth"]["api"],
                             "reason": "depth is ignored by KEEP_ALL and must be omitted"})
        if history not in tuple(supported_history):
            code = _reason("history_keep_all_requires_worker_binding",
                           "current worker.create_public_qos rejects non-KEEP_LAST")
            binding.append(code)
            reasons.append(code)
    elif history == "keep_last" and "depth" not in policy and "history" in policy:
        # A bare history=keep_last without depth is incomplete at the API.
        reasons.append(_reason("depth_missing_for_keep_last"))
        receipts.append({"field": "depth", "submitted": None, "accepted": False,
                         "disposition": "guard_reject", "api": _PUBLIC_FIELDS["depth"]["api"],
                         "reason": "QoSProfile requires a depth with KEEP_LAST"})
    guard_failed = any(r["disposition"] == "guard_reject" for r in receipts)
    if guard_failed:
        disposition = "guard_reject"
    elif unavailable:
        disposition = "unavailable_api"
    elif binding:
        disposition = "binding_pending"
    else:
        disposition = "accepted"
    return {
        "schema": SCHEMA,
        "kind": "policy_request",
        "role": role,
        "submitted": copy.deepcopy(policy),
        "normalized": normalized,
        "disposition": disposition,
        "reasons": reasons,
        "unavailable_fields": unavailable,
        "binding_requirements": binding,
        "field_receipts": receipts,
        "sut_failure": False,
        "candidate_eligible": disposition == "accepted",
        "next_action": {
            "accepted": "create endpoint with this policy, then observe graph/match/delivery",
            "guard_reject": "refuse this input at the harness; do not run it as a DDS mutation",
            "binding_pending": "extend the worker public-QoS binding before submitting this input",
            "unavailable_api": "record an observation limitation; never invent the missing field",
        }[disposition],
    }


def policy_binding_receipts(role, policy, *, config_prefix=None):
    """One receipt per submitted field, with the exact public API destination."""
    prefix = config_prefix or (role + "_qos")
    out = []
    for name in sorted(policy):
        spec = _PUBLIC_FIELDS.get(name)
        if spec is None:
            continue
        out.append({
            "role": role,
            "field": name,
            "config_path": prefix + "." + name,
            "value": copy.deepcopy(policy[name]),
            "api": spec["api"],
            "kwarg": spec["kwarg"],
            "value_sha256": canonical_sha256(policy[name]),
            "binding": "public_rclpy_qosprofile",
        })
    return out


def _snapshot_hash(value):
    return canonical_sha256(value)


def _dotted_set(state, path, value):
    parts = path.split(".")
    node = state
    for key in parts[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    before = node.get(parts[-1], _MISSING)
    node[parts[-1]] = value
    return before


_MISSING = object()


def _dotted_get(state, path):
    node = state
    for key in path.split("."):
        if not isinstance(node, dict) or key not in node:
            return _MISSING
        node = node[key]
    return node


def _changes_for(before, after, paths):
    changes = {}
    for path in paths:
        old = _dotted_get(before, path)
        new = _dotted_get(after, path)
        if old is _MISSING and new is _MISSING:
            continue
        if old is _MISSING:
            changes[path] = {"before": None, "after": new, "added": True}
        elif new is _MISSING:
            changes[path] = {"before": old, "after": None, "removed": True}
        elif old != new:
            changes[path] = {"before": old, "after": new}
    return changes


# ---------------------------------------------------------------------------
# G01: per-pair relation plans
# ---------------------------------------------------------------------------

def _reliability_pair(pair):
    if not isinstance(pair, str) or len(pair) != 2 or any(ch not in "RB" for ch in pair):
        raise RelationSurfaceError("invalid_reliability_pair", repr(pair))
    return {"writer": {"reliability": _RELIABILITY[pair[0]]},
            "reader": {"reliability": _RELIABILITY[pair[1]]}}


def field_relation(field, offered, requested):
    """Evaluate one offered/requested DDS match policy.

    Returns ``compatible``, ``incompatible``, ``unknown`` or
    ``not_a_match_policy``.  A missing/ambiguous value is unknown, never
    compatible by default.
    """
    if field in _UNAVAILABLE_FIELDS:
        return {"status": "unavailable_api", "reason": "field_not_exposed_by_public_api"}
    if field in NON_MATCH_POLICY_FIELDS:
        return {"status": "not_a_match_policy",
                "reason": "local resource policy; it cannot prove or disprove endpoint compatibility"}
    if field not in _PUBLIC_FIELDS:
        return {"status": "unknown", "reason": "unknown_policy_field"}
    if offered is None or requested is None:
        return {"status": "unknown", "reason": "field_missing_on_one_side"}

    if field == "reliability":
        order = {"best_effort": 1, "reliable": 2}
        if offered not in order or requested not in order:
            return {"status": "unknown", "reason": "unrecognised_reliability"}
        return {"status": "compatible" if order[offered] >= order[requested] else "incompatible",
                "rule": "writer offered reliability must be at least as strong as reader requested"}
    if field == "durability":
        order = {"volatile": 1, "transient_local": 2}
        if offered not in order or requested not in order:
            return {"status": "unknown", "reason": "unrecognised_durability"}
        return {"status": "compatible" if order[offered] >= order[requested] else "incompatible",
                "rule": "writer offered durability must be at least as strong as reader requested"}
    if field == "liveliness":
        order = {"automatic": 1, "manual_by_topic": 2}
        if offered not in order or requested not in order:
            return {"status": "unknown", "reason": "unrecognised_liveliness"}
        return {"status": "compatible" if order[offered] >= order[requested] else "incompatible",
                "rule": "writer offered liveliness kind must be at least as strict as requested"}
    if field in ("deadline_ns", "liveliness_lease_duration_ns"):
        if any(type(value) is not int for value in (offered, requested)):
            return {"status": "unknown", "reason": "non_integer_duration"}
        if offered in (0, INFINITE_QOS_NS) or requested in (0, INFINITE_QOS_NS):
            # rclpy uses 0 in the default profile; the project also uses the
            # int64 maximum.  Neither is sufficient evidence for a concrete
            # offered<=requested comparison without a semantic ruling.
            return {"status": "unknown", "reason": "zero_or_infinite_duration_semantics_unverified"}
        return {"status": "compatible" if offered <= requested else "incompatible",
                "rule": "writer offered duration must be no greater than reader requested"}
    return {"status": "unknown", "reason": "unhandled_match_policy"}


def relation_compatibility(writer_policy, reader_policy, *, fields=None):
    """Per-field compatibility view for one writer/reader policy pair."""
    if not isinstance(writer_policy, dict) or not isinstance(reader_policy, dict):
        raise RelationSurfaceError("policies_must_be_objects")
    if fields is None:
        fields = [name for name in POLICY_FIELDS
                  if name in writer_policy or name in reader_policy]
    result = {}
    overall = "compatible"
    for field in fields:
        view = field_relation(field, writer_policy.get(field), reader_policy.get(field))
        result[field] = view
        status = view["status"]
        if status == "incompatible":
            overall = "incompatible"
        elif status in ("unknown", "unavailable_api") and overall != "incompatible":
            overall = "unknown"
    return {
        "schema": SCHEMA,
        "kind": "declared_relation",
        "writer": copy.deepcopy(writer_policy),
        "reader": copy.deepcopy(reader_policy),
        "fields": result,
        "status": overall,
        "match_policy_fields": [name for name in fields if name in MATCH_POLICY_FIELDS],
        "non_match_policy_fields": [name for name in fields if name in NON_MATCH_POLICY_FIELDS],
        "unavailable_fields": [name for name in fields if name in _UNAVAILABLE_FIELDS],
        "inference": "declared only; any observed verdict requires graph/delivery evidence",
    }


def enumerate_g01_matrix(*, pairs=G01_PAIR_CLASSES, base_writer=None, base_reader=None,
                         include_unavailable=False):
    """Enumerate the G01 reliability matrix with per-pair identity receipts.

    Every pair carries both role policies, both role binding receipts and a
    unique pair identity hash.  It is a plan, not execution evidence.
    """
    base_writer = {} if base_writer is None else base_writer
    base_reader = {} if base_reader is None else base_reader
    if include_unavailable:
        base_writer = {**base_writer, "ownership": "exclusive"}
        base_reader = {**base_reader, "ownership": "exclusive"}
    plans = []
    seen = set()
    for pair in pairs:
        if pair not in G01_PAIR_CLASSES:
            raise RelationSurfaceError("undeclared_pair_class", repr(pair))
        if pair in seen:
            raise RelationSurfaceError("duplicate_pair_class", pair)
        seen.add(pair)
        policy = _reliability_pair(pair)
        writer = {**base_writer, **policy["writer"]}
        reader = {**base_reader, **policy["reader"]}
        writer_request = evaluate_policy_request("writer", writer)
        reader_request = evaluate_policy_request("reader", reader)
        relation = relation_compatibility(writer, reader)
        plans.append({
            "schema": SCHEMA,
            "kind": "g01_pair_plan",
            "surface_id": G01_SURFACE_ID,
            "mutator_id": "M08",
            "pair_class": pair,
            "pair_key_sha256": canonical_sha256({"writer": writer, "reader": reader}),
            "writer_policy": writer,
            "reader_policy": reader,
            "writer_request": writer_request,
            "reader_request": reader_request,
            "binding_receipts": policy_binding_receipts("writer", writer)
                                + policy_binding_receipts("reader", reader),
            "declared_relation": relation,
            "guard_expectation": ("no_match_or_delivery" if pair in G01_INCOMPATIBLE_PAIRS
                                  else "match_then_delivery"),
            "oracle_expectation": ("fail_only_if_identity_correlated_prohibited_event"
                                   if pair in G01_INCOMPATIBLE_PAIRS
                                   else "pass_only_if_identity_correlated_match"),
            "creation_success_is_not_a_bug": True,
            "input_accepted": (writer_request["disposition"] == "accepted"
                               and reader_request["disposition"] == "accepted"),
            "candidate_eligible": False,
            "execution_evidence": False,
        })
    return plans


# ---------------------------------------------------------------------------
# W01: depth/count relation boundary plans
# ---------------------------------------------------------------------------

def _w01_relation(depth, post_count):
    if depth < post_count:
        return "depth_lt_post"
    if depth == post_count:
        return "depth_eq_post"
    return "depth_gt_post"


def evaluate_w01_request(request, *, supported_history=("keep_last",),
                         delivery_rule_reviewed=False):
    """Classify a W01 depth/count input and its oracle applicability.

    Accepted inputs still have to survive the runtime binding and observation.
    ``oracle_applicability=review_required`` means the old delivery obligation
    was not re-approved for ``depth_lt_post`` and may not fail the SUT.
    """
    if not isinstance(request, dict):
        raise RelationSurfaceError("w01_request_not_an_object")
    allowed = {"depth", "history", "pre_match_count", "post_match_count",
               "relation_class"}
    unknown = sorted(set(request) - allowed)
    reasons = []
    receipts = []
    if unknown:
        for name in unknown:
            reasons.append(_reason("unknown_w01_field", name))
            receipts.append({"field": name, "accepted": False,
                             "disposition": "guard_reject",
                             "reason": "not a W01 input path"})

    history = request.get("history", "keep_last")
    depth = request.get("depth")
    post = request.get("post_match_count")
    relation_class = request.get("relation_class")

    if not isinstance(history, str) or history not in ("keep_last", "keep_all"):
        reasons.append(_reason("invalid_w01_history", repr(history)))
        receipts.append({"field": "history", "submitted": history, "accepted": False,
                         "disposition": "guard_reject", "reason": "expected keep_last|keep_all"})
    elif history == "keep_all" and "depth" in request:
        reasons.append(_reason("depth_not_applicable_for_keep_all"))
        receipts.append({"field": "depth", "submitted": depth, "accepted": False,
                         "disposition": "guard_reject",
                         "reason": "KEEP_ALL ignores depth; submit it without depth"})
    elif history == "keep_all" and history not in tuple(supported_history):
        reasons.append(_reason("history_keep_all_requires_worker_binding",
                               "worker.create_public_qos accepts KEEP_LAST only"))
        receipts.append({"field": "history", "submitted": history, "accepted": True,
                         "disposition": "binding_pending",
                         "api": _PUBLIC_FIELDS["history"]["api"]})

    if history == "keep_last":
        if type(depth) is bool or type(depth) is not int:
            reasons.append(_reason("invalid_w01_depth_type", type(depth).__name__))
            receipts.append({"field": "depth", "submitted": depth, "accepted": False,
                             "disposition": "guard_reject", "reason": "expected integer"})
        elif depth == 0:
            reasons.append(_reason("depth_zero_keep_last_system_default",
                                   "rclpy warns and treats depth=0 as SYSTEM_DEFAULT"))
            receipts.append({"field": "depth", "submitted": depth, "accepted": False,
                             "disposition": "guard_reject",
                             "reason": "depth=0 has no concrete queue semantics under KEEP_LAST"})
        elif not 1 <= depth <= MAX_HISTORY_DEPTH:
            reasons.append(_reason("w01_depth_out_of_range", "1..%d" % MAX_HISTORY_DEPTH))
            receipts.append({"field": "depth", "submitted": depth, "accepted": False,
                             "disposition": "guard_reject", "reason": "out of range"})
        else:
            receipts.append({"field": "depth", "submitted": depth, "accepted": True,
                             "disposition": "accepted", "api": _PUBLIC_FIELDS["depth"]["api"]})

    for name, low in (("pre_match_count", 1), ("post_match_count", 3)):
        value = request.get(name)
        if type(value) is bool or type(value) is not int:
            reasons.append(_reason("invalid_w01_count_type", name))
            receipts.append({"field": name, "submitted": value, "accepted": False,
                             "disposition": "guard_reject", "reason": "expected integer"})
        elif value < low:
            reasons.append(_reason("w01_count_below_minimum", "%s>=%d" % (name, low)))
            receipts.append({"field": name, "submitted": value, "accepted": False,
                             "disposition": "guard_reject", "reason": "below runtime minimum"})
        elif value > MAX_HISTORY_DEPTH:
            reasons.append(_reason("w01_count_out_of_range", name))
            receipts.append({"field": name, "submitted": value, "accepted": False,
                             "disposition": "guard_reject", "reason": "above reviewed maximum"})
        else:
            receipts.append({"field": name, "submitted": value, "accepted": True,
                             "disposition": "accepted", "api": "config." + name})

    relation = None
    if type(depth) is int and type(post) is int and not isinstance(depth, bool) and not isinstance(post, bool):
        relation = _w01_relation(depth, post)
        if relation_class is not None and relation_class != relation:
            reasons.append(_reason("relation_class_mismatch",
                                   "declared=%r actual=%r" % (relation_class, relation)))
            receipts.append({"field": "relation_class", "submitted": relation_class,
                             "accepted": False, "disposition": "guard_reject",
                             "reason": "does not match depth vs post_match_count"})

    guard_failed = any(r["disposition"] == "guard_reject" for r in receipts)
    binding_pending = any(r["disposition"] == "binding_pending" for r in receipts)
    disposition = ("guard_reject" if guard_failed
                   else "binding_pending" if binding_pending else "accepted")
    applicability = "unknown"
    if disposition == "accepted" and relation is not None:
        if relation == "depth_lt_post" and not delivery_rule_reviewed:
            applicability = "review_required"
        else:
            applicability = "applicable"
    return {
        "schema": SCHEMA,
        "kind": "w01_request",
        "submitted": copy.deepcopy(request),
        "relation_class": relation,
        "disposition": disposition,
        "reasons": reasons,
        "field_receipts": receipts,
        "oracle_applicability": applicability,
        "oracle_reason": (None if applicability == "applicable"
                          else "delivery_obligation_not_reviewed_for_depth_lt_post"
                          if applicability == "review_required"
                          else "input_not_accepted"),
        "delivery_rule_reviewed": bool(delivery_rule_reviewed),
        "sut_failure": False,
        "candidate_eligible": disposition == "accepted" and applicability == "applicable",
        "next_action": {
            "accepted": "create both endpoints and observe per-edge send/receive closure",
            "guard_reject": "refuse this input at the harness; it is not a DDS mutation",
            "binding_pending": "extend worker.create_public_qos before submitting history",
        }[disposition],
    }


def build_w01_mutation(before_state, request, *, supported_history=("keep_last",),
                       delivery_rule_reviewed=False, mutator_id="M04"):
    """Build a W01 relation-boundary mutation with before/after proof.

    ``before_state`` is a plain config-shaped dict.  No ROS call is made; the
    returned receipts name the exact public API destination the unified
    provider must submit.
    """
    if not isinstance(before_state, dict):
        raise RelationSurfaceError("w01_state_not_an_object")
    if mutator_id not in W01_IMPLEMENTED_MUTATORS:
        raise RelationSurfaceError("unimplemented_w01_mutator", str(mutator_id))
    guard = evaluate_w01_request(request, supported_history=supported_history,
                                 delivery_rule_reviewed=delivery_rule_reviewed)
    before = copy.deepcopy(before_state)
    after = copy.deepcopy(before_state)
    changes = {}
    if guard["disposition"] == "accepted":
        fields = [("qos.depth", request["depth"])]
        if "history" in request:
            fields.append(("qos.history", request["history"]))
        fields.append(("pre_match_count", request["pre_match_count"]))
        fields.append(("post_match_count", request["post_match_count"]))
        for path, value in fields:
            _dotted_set(after, path, value)
        changes = _changes_for(before, after, [path for path, _ in fields])
    before_hash, after_hash = _snapshot_hash(before), _snapshot_hash(after)
    return {
        "schema": SCHEMA,
        "kind": "w01_mutation_plan",
        "surface_id": W01_SURFACE_ID,
        "mutator_id": mutator_id,
        "request": copy.deepcopy(request),
        "before": before,
        "after": after,
        "input_before_sha256": before_hash,
        "input_after_sha256": after_hash,
        "content_changed": before_hash != after_hash,
        "changes": changes,
        "change_paths": sorted(changes),
        "guard": guard,
        "binding_receipts": [
            {"role": "writer+reader", "field": "qos.depth",
             "config_path": "qos.depth", "value": request.get("depth"),
             "api": "rclpy.qos.QoSProfile.depth", "kwarg": "depth",
             "value_sha256": canonical_sha256(request.get("depth"))},
            {"role": "writer+reader", "field": "qos.history",
             "config_path": "qos.history", "value": request.get("history", "keep_last"),
             "api": "rclpy.qos.QoSProfile.history", "kwarg": "history",
             "value_sha256": canonical_sha256(request.get("history", "keep_last"))},
            {"role": "writer", "field": "pre_match_count",
             "config_path": "pre_match_count", "value": request.get("pre_match_count"),
             "api": "runtime publish schedule prejoin", "kwarg": None,
             "value_sha256": canonical_sha256(request.get("pre_match_count"))},
            {"role": "writer", "field": "post_match_count",
             "config_path": "post_match_count", "value": request.get("post_match_count"),
             "api": "runtime publish schedule post_match", "kwarg": None,
             "value_sha256": canonical_sha256(request.get("post_match_count"))},
        ],
        "notes": {
            "depth_is_not_a_sedp_wire_field": True,
            "history_depth_are_not_match_policies": True,
            "delivery_obligation": ("reviewed" if delivery_rule_reviewed
                                    else "requires_explicit_review_when_depth_lt_post"),
        },
        "candidate_eligible": False,
        "execution_evidence": False,
    }


# ---------------------------------------------------------------------------
# Composition chains
# ---------------------------------------------------------------------------

def _apply_g01(state, parameters):
    pair = parameters.get("pair_relation")
    if pair is None:
        raise RelationSurfaceError("g01_requires_pair_relation")
    policy = _reliability_pair(pair)
    before = copy.deepcopy(state)
    after = copy.deepcopy(state)
    _dotted_set(after, "writer_qos.reliability", policy["writer"]["reliability"])
    _dotted_set(after, "reader_qos.reliability", policy["reader"]["reliability"])
    changes = _changes_for(before, after, ["writer_qos.reliability", "reader_qos.reliability"])
    writer = (after.get("writer_qos") or {})
    reader = (after.get("reader_qos") or {})
    relation = relation_compatibility(writer, reader)
    guard = {
        "schema": SCHEMA,
        "kind": "g01_request",
        "pair_class": pair,
        "disposition": "accepted",
        "declared_relation": relation,
        "sut_failure": False,
        "candidate_eligible": False,
    }
    return before, after, changes, guard, policy_binding_receipts("writer", writer) + policy_binding_receipts("reader", reader)


def plan_relation_chain(base_state, operations, *, supported_history=("keep_last",),
                        delivery_rule_reviewed=False):
    """Apply an ordered W01/G01 chain with per-step content and binding proof.

    Unsupported names are preserved in ``unsupported_operations`` and the chain
    stops at the first one.  It never silently drops an unknown name and never
    turns a compile-time chain into execution evidence.
    """
    if not isinstance(base_state, dict):
        raise RelationSurfaceError("chain_state_not_an_object")
    if not isinstance(operations, list) or not operations:
        raise RelationSurfaceError("chain_operations_required")
    state = copy.deepcopy(base_state)
    steps = []
    unsupported = []
    disposition = "accepted"
    for index, operation in enumerate(operations):
        if not isinstance(operation, dict):
            unsupported.append({"index": index, "operation": operation,
                                "status": "unsupported_shape",
                                "reason": "operation must be an object"})
            disposition = "rejected_unsupported_operation"
            break
        surface = operation.get("surface_id")
        mutator = operation.get("mutator_id")
        parameters = operation.get("parameters") or {}
        allowed = {"W01": W01_IMPLEMENTED_MUTATORS, "G01": G01_IMPLEMENTED_MUTATORS}
        if surface not in allowed:
            unsupported.append({"index": index, "surface_id": surface,
                                "mutator_id": mutator, "status": "unknown_surface"})
            disposition = "rejected_unsupported_operation"
            break
        if mutator not in allowed[surface]:
            declined = DECLINED_MUTATORS.get(surface, {}).get(mutator)
            unsupported.append({"index": index, "surface_id": surface,
                                "mutator_id": mutator,
                                "status": "declined_by_design" if declined else "unknown_mutator",
                                "reason": declined,
                                "implemented": sorted(allowed[surface]),
                                "declined_mutators": dict(DECLINED_MUTATORS.get(surface, {}))})
            disposition = "rejected_unsupported_operation"
            break
        before_hash = _snapshot_hash(state)
        if surface == "W01":
            plan = build_w01_mutation(state, parameters,
                                      supported_history=supported_history,
                                      delivery_rule_reviewed=delivery_rule_reviewed,
                                      mutator_id=mutator)
            state = plan["after"]
            step = {
                "index": index, "surface_id": surface, "mutator_id": mutator,
                "input_before_sha256": before_hash,
                "input_after_sha256": plan["input_after_sha256"],
                "content_changed": plan["content_changed"],
                "changes": plan["changes"],
                "guard": plan["guard"],
                "binding_receipts": plan["binding_receipts"],
                "override": not plan["content_changed"],
            }
        else:
            before, after, changes, guard, receipts = _apply_g01(state, parameters)
            state = after
            after_hash = _snapshot_hash(after)
            step = {
                "index": index, "surface_id": surface, "mutator_id": mutator,
                "input_before_sha256": before_hash,
                "input_after_sha256": after_hash,
                "content_changed": before_hash != after_hash,
                "changes": changes,
                "guard": guard,
                "binding_receipts": receipts,
                "override": before_hash == after_hash,
            }
        if step["guard"]["disposition"] != "accepted":
            step["status"] = "guard_blocked"
            steps.append(step)
            disposition = "guard_blocked"
            break
        step["status"] = "planned"
        steps.append(step)
    return {
        "schema": SCHEMA,
        "kind": "relation_chain",
        "base_state_sha256": _snapshot_hash(base_state),
        "final_state": state,
        "final_state_sha256": _snapshot_hash(state),
        "steps": steps,
        "unsupported_operations": unsupported,
        "disposition": disposition,
        "all_steps_changed_content": bool(steps) and all(s["content_changed"] for s in steps),
        "execution_evidence": False,
        "candidate_eligible": False,
    }


# ---------------------------------------------------------------------------
# Provider/core projection seam
# ---------------------------------------------------------------------------

def relation_surface_core_projection():
    """Declare the replacement implementation main can wire into core.

    This function is intentionally inert: it does not import or mutate the
    catalog, and it does not delete the legacy refusals.
    """
    return {
        "schema": SCHEMA,
        "status": "adapter_ready_pending_main_wiring",
        "replacement_points": copy.deepcopy(REPLACEMENT_POINTS),
        "surfaces": {
            "G01": {
                "surface_id": G01_SURFACE_ID,
                "surface_kind": "compatibility",
                "value_type": "pair_relation",
                "input_paths": ["writer_qos.reliability", "reader_qos.reliability"],
                "mutator_ids": ["M08"],
                "fields": list(MATCH_POLICY_FIELDS),
                "pair_classes": list(G01_PAIR_CLASSES),
                "incompatible_pairs": sorted(G01_INCOMPATIBLE_PAIRS),
                "per_edge_required": True,
                "aggregate_match_count_is_not_evidence": True,
                "runtime_binding": "relation_observer.edges_from_events",
                "activation": "F2-adapter",
            },
            "W01": {
                "surface_id": W01_SURFACE_ID,
                "surface_kind": "relation",
                "value_type": "int",
                "input_paths": ["qos.depth", "qos.history",
                                "pre_match_count", "post_match_count"],
                "mutator_ids": ["M04"],
                "relation_classes": list(W01_RELATION_CLASSES),
                "runtime_binding": "relation_observer.observe_w01_delivery",
                "activation": "F1-auxiliary-adapter",
                "declined_mutators": dict(DECLINED_MUTATORS["W01"]),
            },
        },
        "unchanged_by_this_module": [
            "src/r2f/modules/s1/fuzz_catalog.py",
            "src/r2f/runtime/ros2_pair.py",
            "src/r2f/modules/s1/providers.py",
        ],
        "execution_evidence": False,
    }


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def audit_plan(plan):
    """Re-derive content-change and binding invariants from a returned plan."""
    kind = plan.get("kind")
    if kind == "w01_mutation_plan":
        before, after = plan.get("before"), plan.get("after")
        changed = canonical_sha256(before) != canonical_sha256(after)
        problems = []
        if changed != plan.get("content_changed"):
            problems.append("content_changed disagrees with before/after hash")
        if plan.get("content_changed") and not plan.get("change_paths"):
            problems.append("content changed without a named path")
        for receipt in plan.get("binding_receipts", []):
            if receipt.get("value_sha256") != canonical_sha256(receipt.get("value")):
                problems.append("binding receipt hash mismatch: " + str(receipt.get("field")))
        return {"ok": not problems, "problems": problems}
    if kind == "relation_chain":
        problems = []
        for step in plan.get("steps", []):
            if step["input_before_sha256"] == step["input_after_sha256"] and step["content_changed"]:
                problems.append("step %d claims change with equal hashes" % step["index"])
        return {"ok": not problems, "problems": problems}
    raise RelationSurfaceError("unknown_plan_kind", str(kind))


__all__ = [
    "SCHEMA", "POLICY_FIELDS", "MATCH_POLICY_FIELDS", "NON_MATCH_POLICY_FIELDS",
    "G01_PAIR_CLASSES", "G01_INCOMPATIBLE_PAIRS", "W01_RELATION_CLASSES",
    "RelationSurfaceError", "field_capabilities", "evaluate_policy_request",
    "policy_binding_receipts", "field_relation", "relation_compatibility",
    "enumerate_g01_matrix", "evaluate_w01_request", "build_w01_mutation",
    "plan_relation_chain", "relation_surface_core_projection", "audit_plan",
    "canonical_sha256", "REPLACEMENT_POINTS", "DECLINED_MUTATORS",
]
