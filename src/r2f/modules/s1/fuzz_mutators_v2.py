"""S1 v2 input mutators: pure, offline, content-addressed generation.

This module never starts ROS, opens sockets, reads clocks, or writes a run
receipt.  A :class:`MutationCase` is an input proposal with provenance.  Any
``expected_disposition`` is a *case expectation* for the guard/oracle, not an
oracle verdict and not evidence that the SUT did anything.

Design constraints implemented here:

* every operator must change the canonical content hash; a seed-only change is
  refused;
* the seed and RNG algorithm are recorded in provenance but excluded from the
  content identity;
* unknown surfaces, unknown slots, undeclared operators and out-of-domain
  values are rejected by name, never silently dropped or coerced;
* existing S1 models are adapted, not replaced: catalog surfaces come from
  :mod:`r2f.modules.s1.fuzz_catalog`, topology structure/action/history
  mutations delegate to :mod:`r2f.modules.s1.topology_model`, and the declared
  input domain comes from :mod:`r2f.modules.s1.input_model`.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import copy
import random

from . import input_model
from . import topology_model

CASE_SCHEMA = "r2f.s1-mutation-case/v1"
INPUT_PAYLOAD_SCHEMA = "r2f.s1-mutation-input/v1"
GENERATOR_VERSION = 2
RNG_ALGORITHM = "mt19937-v1"
MAX_CHAIN_LENGTH = 4


class MutationError(ValueError):
    """Base error; always names the offending surface/slot/operator."""


class UnknownDomainError(MutationError):
    pass


class UndeclaredOperatorError(MutationError):
    pass


class MutationRefused(MutationError):
    """A declared operator produced no distinct content; never emitted as a case."""

    def __init__(self, reason, details=None):
        super().__init__(reason)
        self.reason = reason
        self.details = dict(details or {})


# ---------------------------------------------------------------------------
# Operator registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OperatorSpec:
    operator_id: str
    category: str
    description: str
    status: str = "implemented"
    accepted_kinds: tuple = ()
    runtime_effect: str = "input_only"
    guard_expectation: str = "varies"

    def to_dict(self):
        return {"operator_id": self.operator_id, "category": self.category,
                "description": self.description, "status": self.status,
                "accepted_kinds": list(self.accepted_kinds),
                "runtime_effect": self.runtime_effect,
                "guard_expectation": self.guard_expectation}


_OPERATOR_SPECS = (
    OperatorSpec("param.boundary", "parameter",
                 "min/max/0/1/neighbour/out-of-range/special boundary value",
                 accepted_kinds=("parameter_numeric", "parameter_timing",
                                 "parameter_queue", "parameter_lease")),
    OperatorSpec("param.enum", "parameter", "different declared enum value",
                 accepted_kinds=("parameter_enum",)),
    OperatorSpec("relation.pair", "relation",
                 "offered/requested relation pair including incompatible pairs",
                 accepted_kinds=("parameter_enum", "compatibility", "parameter_queue"),
                 guard_expectation="expect_pass_or_no_match"),
    OperatorSpec("timing.shift", "timing", "bounded shift from the parent timing value",
                 accepted_kinds=("parameter_timing", "parameter_lease")),
    OperatorSpec("queue.depth", "parameter", "history depth boundary/neighbour",
                 accepted_kinds=("parameter_queue",)),
    OperatorSpec("lease.value", "parameter", "lease zero/finite/infinite value class",
                 accepted_kinds=("parameter_lease",)),
    OperatorSpec("filter.expr", "filter", "empty/valid/invalid content-filter expression",
                 accepted_kinds=("filter",), guard_expectation="expect_pass_or_reject"),
    OperatorSpec("endpoint.count", "structure", "add or remove an endpoint with action closure",
                 accepted_kinds=("structure", "endpoint_count")),
    OperatorSpec("generation.action", "history", "add or stale a logical generation action",
                 accepted_kinds=("generation", "action_history")),
    OperatorSpec("action.insert", "action_history", "insert an observation action",
                 accepted_kinds=("action_history", "structure")),
    OperatorSpec("action.delete", "action_history", "delete an action with dependency closure",
                 accepted_kinds=("action_history", "structure")),
    OperatorSpec("action.swap", "action_history", "swap adjacent independent actions",
                 accepted_kinds=("action_history", "structure")),
    OperatorSpec("guard.environment", "guard", "declared environment precondition negative",
                 accepted_kinds=("environment",), guard_expectation="expect_guard_reject"),
    OperatorSpec("guard.dependency", "guard", "missing action dependency negative",
                 accepted_kinds=("dependency", "action_history"), guard_expectation="expect_guard_reject"),
    OperatorSpec("guard.structure", "guard", "duplicate id/invalid structure negative",
                 accepted_kinds=("structure", "action_history"), guard_expectation="expect_guard_reject"),
    OperatorSpec("guard.parameter", "guard", "out-of-budget parameter negative",
                 accepted_kinds=("parameter_numeric", "parameter_queue", "action_history"),
                 guard_expectation="expect_guard_reject"),
    OperatorSpec("env.delay", "environment", "declared delay window (no runtime claim)",
                 accepted_kinds=("parameter_timing", "environment"),
                 guard_expectation="unknown_not_pass"),
    OperatorSpec("env.reorder", "environment", "declared delivery reorder plan",
                 accepted_kinds=("environment", "action_history"), guard_expectation="unknown_not_pass"),
    OperatorSpec("env.duplicate", "environment", "declared duplicate delivery plan",
                 accepted_kinds=("environment",), guard_expectation="unknown_not_pass"),
    OperatorSpec("env.drop", "environment", "declared drop window/sequence",
                 accepted_kinds=("environment", "parameter_timing"),
                 guard_expectation="unknown_not_pass"),
    OperatorSpec("env.resource", "environment", "declared resource unavailability",
                 accepted_kinds=("environment",), guard_expectation="expect_guard_reject_or_unknown"),
    OperatorSpec("env.health", "environment", "declared health unknown/faulted",
                 accepted_kinds=("environment",), guard_expectation="unknown_not_pass"),
    OperatorSpec("env.lease_expiry", "environment", "declared lease expiry action",
                 accepted_kinds=("parameter_lease", "parameter_enum", "environment"),
                 guard_expectation="unknown_not_pass"),
    OperatorSpec("env.lease_refresh", "environment", "declared lease refresh action",
                 accepted_kinds=("parameter_lease", "parameter_enum", "environment"),
                 guard_expectation="unknown_not_pass"),
)

OPERATOR_SPECS = {spec.operator_id: spec for spec in _OPERATOR_SPECS}


def operator_manifest():
    return {
        "schema": "r2f.s1-mutator-manifest/v1",
        "generator_version": GENERATOR_VERSION,
        "operators": [OPERATOR_SPECS[key].to_dict() for key in sorted(OPERATOR_SPECS)],
        "implemented": sorted(key for key, spec in OPERATOR_SPECS.items()
                              if spec.status == "implemented"),
        "pending": sorted(key for key, spec in OPERATOR_SPECS.items()
                          if spec.status != "implemented"),
        "max_chain_length": MAX_CHAIN_LENGTH,
        "execution_evidence": False,
        "candidate_eligible": False,
    }


def _check_operator(operator_id):
    spec = OPERATOR_SPECS.get(operator_id)
    if spec is None:
        raise UndeclaredOperatorError(f"unknown operator: {operator_id}")
    if spec.status != "implemented":
        raise MutationRefused(f"operator not implemented: {operator_id}",
                              {"operator_id": operator_id, "status": spec.status})
    return spec


# ---------------------------------------------------------------------------
# Compatibility adapters
# ---------------------------------------------------------------------------

def surface_by_id(surface_id, catalog=None):
    domain = catalog or input_model.default_domain()
    try:
        return domain.get(surface_id)
    except KeyError as exc:
        raise UnknownDomainError(f"unknown input-domain surface: {surface_id}") from exc


def _as_surface(value):
    if isinstance(value, input_model.SurfaceDomain):
        return value
    if isinstance(value, str):
        return surface_by_id(value)
    if isinstance(value, dict) and value.get("schema") == input_model.DOMAIN_SCHEMA:
        return input_model.SurfaceDomain.from_dict(value)
    raise ValueError("surface must be a SurfaceDomain, surface id, or domain dict")


def _surface_document(surface):
    """The canonical document kind expected by a surface."""
    if surface.surface_id == "S1.topology":
        return "topology"
    if surface.surface_id == "S1.delivery":
        return "delivery"
    return "logical_config"


def _content_hash(surface_id, document):
    """Input identity: surface + final payload only.

    The operator id, slot id, seed, rng algorithm, chain order and provenance
    are *provenance*, not input identity.  Two operators that reach the same
    final document therefore hash the same and are deduplicated; changing only
    the seed never creates a new input.
    """
    return input_model.content_hash({
        "schema": INPUT_PAYLOAD_SCHEMA,
        "surface_id": surface_id,
        "input": document,
    })


def _check_document(document, surface):
    kind = _surface_document(surface)
    if kind == "topology":
        if not isinstance(document, dict) or document.get("schema") != topology_model.SCHEMA:
            raise MutationError("S1.topology requires a r2f.s1-topology-program/v1 document")
        validation = topology_model.validate_program(document)
        if validation["status"] != "valid":
            raise MutationError("S1.topology input is structurally invalid; use a valid parent "
                                "or an explicit guard seed")
        return validation
    if kind == "delivery":
        if not isinstance(document, dict):
            raise MutationError("S1.delivery input must be a JSON object")
        unknown = set(document) - {"schema", "delivery_plan", "topic", "message_type", "qos"}
        if unknown:
            raise UnknownDomainError(
                f"S1.delivery unknown field(s): {','.join(sorted(unknown))}")
        return None
    if not isinstance(document, dict):
        raise MutationError("logical-config input must be a JSON object")
    return None


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MutationCase:
    surface_id: str
    operator_id: str
    slot_id: str
    seed: int
    input_before_sha256: str
    content_sha256: str
    input: dict
    changes: dict
    binding: dict
    expected_disposition: str
    expected_guard_effect: dict
    applicability: dict
    provenance: dict
    schema: str = CASE_SCHEMA

    def __post_init__(self):
        if self.schema != CASE_SCHEMA:
            raise ValueError("unsupported mutation case schema")
        if self.input_before_sha256 == self.content_sha256:
            raise ValueError("a mutation case must change the content hash")
        if self.expected_disposition not in input_model.GUARD_DISPOSITIONS:
            raise ValueError(f"unsupported expected_disposition: {self.expected_disposition}")

    def to_dict(self):
        return {
            "schema": self.schema,
            "surface_id": self.surface_id,
            "operator_id": self.operator_id,
            "slot_id": self.slot_id,
            "seed": self.seed,
            "rng_algorithm": RNG_ALGORITHM,
            "input_before_sha256": self.input_before_sha256,
            "content_sha256": self.content_sha256,
            "input": copy.deepcopy(self.input),
            "changes": copy.deepcopy(self.changes),
            "binding": copy.deepcopy(self.binding),
            "expected_disposition": self.expected_disposition,
            "expected_guard_effect": copy.deepcopy(self.expected_guard_effect),
            "applicability": copy.deepcopy(self.applicability),
            "provenance": copy.deepcopy(self.provenance),
            "execution_evidence": False,
            "candidate_eligible": False,
        }

    @property
    def parent_sha256(self):
        return self.input_before_sha256

    @property
    def chain(self):
        return tuple(self.provenance.get("chain", ()))

    def classify(self):
        return input_model.classify_input(self.input, surface_by_id(self.surface_id),
                                          document_kind=_surface_document(
                                              surface_by_id(self.surface_id)))


def _make_case(surface, operator_id, slot_id, document, before, changes, seed,
               expected, guard_effect, applicability, chain, binding=None):
    after_hash = _content_hash(surface.surface_id, document)
    if after_hash == before:
        raise MutationRefused("operator produced identical content",
                              {"operator_id": operator_id, "slot_id": slot_id})
    slot = surface.slot_by_id.get(slot_id) if slot_id in surface.slot_by_id else None
    resolved_binding = dict(binding or (slot.binding if slot else {}))
    provenance = {
        "generator": "r2f.modules.s1.fuzz_mutators_v2",
        "generator_version": GENERATOR_VERSION,
        "authored": False,
        "derived_from": "parent_input",
        "parent_sha256": before,
        "operator_id": operator_id,
        "chain": list(chain),
        "seed": seed,
        "rng_algorithm": RNG_ALGORITHM,
        "surface_version": surface.version,
        "domain_sha256": input_model.content_hash(surface.to_dict()),
    }
    return MutationCase(
        surface_id=surface.surface_id, operator_id=operator_id, slot_id=slot_id,
        seed=seed, input_before_sha256=before, content_sha256=after_hash,
        input=document, changes=changes, binding=resolved_binding,
        expected_disposition=expected, expected_guard_effect=guard_effect,
        applicability=applicability, provenance=provenance,
    )


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

def _special(slot, name):
    if name in slot.special_values:
        return slot.special_values[name]
    raise MutationRefused(f"special class not declared: {name}",
                          {"slot_id": slot.slot_id, "special_class": name})


def _numeric_value(slot, boundary_class, current, rng):
    domain = slot.domain
    minimum = domain.get("min")
    maximum = domain.get("max")
    step = domain.get("step", 1)
    reps = [value for value in domain.get("representatives", [])
            if type(value) in (int, float)]
    if boundary_class == "min":
        return minimum if minimum is not None else min(reps)
    if boundary_class == "max":
        return maximum if maximum is not None else max(reps)
    if boundary_class == "zero":
        return 0
    if boundary_class == "one":
        return 1
    if boundary_class in {"neighbor", "quantized_neighbor"}:
        base = current if type(current) in (int, float) else (minimum or 0)
        candidates = [base + step, base - step]
        candidates = [value for value in candidates
                      if (minimum is None or value >= minimum)
                      and (maximum is None or value <= maximum) and value != base]
        if not candidates:
            candidates = [minimum if minimum is not None else 0 + step]
        return candidates[rng.randrange(len(candidates))]
    if boundary_class == "out_of_range_low":
        if minimum is None:
            raise MutationRefused("slot has no declared lower bound", {"slot_id": slot.slot_id})
        return minimum - step
    if boundary_class == "out_of_range_high":
        if maximum is None:
            raise MutationRefused("slot has no declared upper bound", {"slot_id": slot.slot_id})
        return maximum + step
    if boundary_class in ("infinite", "special"):
        for name in ("infinite", "zero"):
            if name in slot.special_values:
                return slot.special_values[name]
        raise MutationRefused("slot declares no special value class", {"slot_id": slot.slot_id})
    if boundary_class == "finite":
        values = [value for value in reps
                  if value not in (0, slot.special_values.get("infinite"))]
        if values:
            return values[rng.randrange(len(values))]
        if current is not None:
            return current
        raise MutationRefused("finite class has no representative distinct from the parent",
                              {"slot_id": slot.slot_id})
    raise MutationRefused(f"unsupported boundary class: {boundary_class}",
                          {"slot_id": slot.slot_id, "boundary_class": boundary_class})


def _boundary_classes(slot, requested):
    declared = list(slot.boundary_classes) or ["min", "max", "neighbor"]
    if requested is None:
        return declared
    if requested not in declared:
        raise MutationRefused(f"boundary class is not declared: {requested}",
                              {"slot_id": slot.slot_id, "boundary_class": requested,
                               "declared": declared})
    return [requested]


def _slot_value(document, slot):
    try:
        return input_model.get_path(document, slot.input_path)
    except KeyError:
        return None


def _require_slot_present(document, slot, surface):
    try:
        return input_model.get_path(document, slot.input_path)
    except KeyError as exc:
        raise MutationRefused(
            f"parent input has no value at declared path {slot.input_path}",
            {"surface_id": surface.surface_id, "slot_id": slot.slot_id}) from exc


def _set_slot_value(document, slot, value):
    input_model.set_path(document, slot.input_path, value)
    return document


# ---------------------------------------------------------------------------
# Topology helpers
# ---------------------------------------------------------------------------

def _resolve_topology_filter_slot(slot, document, rng, context):
    """Resolve the filter wildcard only against declared reader endpoints."""
    endpoints = document.get("endpoints", [])
    readers = [index for index, endpoint in enumerate(endpoints)
               if endpoint.get("role") == "reader"]
    if not readers:
        raise MutationRefused("filter mutation requires a reader",
                              {"surface_id": "S1.topology"})
    if "endpoint_index" in context:
        index = context["endpoint_index"]
        if type(index) is not int or index not in readers:
            raise MutationRefused("endpoint_index is not a reader",
                                  {"surface_id": "S1.topology", "endpoint_index": index})
    else:
        index = readers[rng.randrange(len(readers))]
    return replace(slot, input_path=f"endpoints[{index}].filter.expression",
                   slot_id=f"{slot.slot_id}#{index}")


def _resolve_wildcard_slot(slot, document, rng, context):
    """Bind a ``endpoints[*].x`` slot to one concrete endpoint index.

    The declared slot stays wildcarded in the domain; a generated case needs a
    single addressable path, so the resolved copy carries the index.  Which
    endpoint was chosen is visible in ``changes``.
    """
    if "[*]" not in slot.input_path:
        return slot
    prefix, suffix = slot.input_path.split("[*]", 1)
    try:
        collection = input_model.get_path(document, prefix)
    except KeyError as exc:
        raise MutationRefused(f"parent input has no {prefix}",
                              {"slot_id": slot.slot_id, "path": prefix}) from exc
    if not isinstance(collection, list) or not collection:
        raise MutationRefused("no concrete endpoint for wildcard slot",
                              {"slot_id": slot.slot_id})
    index = context.get("endpoint_index")
    if index is None:
        index = rng.randrange(len(collection))
    elif type(index) is not int or not 0 <= index < len(collection):
        raise MutationRefused("endpoint_index is out of range",
                              {"slot_id": slot.slot_id, "endpoint_index": index})
    return replace(slot, input_path=f"{prefix}[{index}]{suffix}",
                   slot_id=f"{slot.slot_id}#{index}")


def _endpoint_targets(slot, document, rng, context):
    path = slot.input_path
    if "[*]" not in path:
        return [path]
    prefix, suffix = path.split("[*]", 1)
    collection = input_model.get_path(document, prefix)
    if not isinstance(collection, list) or not collection:
        raise MutationRefused("no concrete endpoint for wildcard slot", {"slot_id": slot.slot_id})
    index = context.get("endpoint_index")
    if index is None:
        chosen = rng.randrange(len(collection))
    else:
        if type(index) is not int or not 0 <= index < len(collection):
            raise MutationRefused("endpoint_index is out of range",
                                  {"slot_id": slot.slot_id, "endpoint_index": index})
        chosen = index
    return [f"{prefix}[{chosen}]{suffix}"]


def _topology_document_for_slot(document, slot, value, *, all_endpoints=False, rng=None,
                                context=None):
    """Write a wildcard slot for one concrete endpoint.

    A QoS attribute that the parent omits is still addressable: writing a
    declared value there is an explicit input declaration, recorded in the
    case changes, not an observation of the SUT.
    """
    paths = _endpoint_targets(slot, document, rng, context or {})
    for path in paths:
        input_model.set_path(document, path, value)
    return paths


def _topology_validation_disposition(document):
    validation = topology_model.validate_program(document)
    if validation["status"] != "valid":
        return "expect_guard_reject", validation
    if validation["guard_disposition"] == "guard_violation":
        return "expect_guard_reject", validation
    return "expect_pass", validation


# ---------------------------------------------------------------------------
# Delivery-plan helpers
# ---------------------------------------------------------------------------

def delivery_plan_template():
    return {
        "schema": "r2f.s1-delivery-plan/v1",
        "topic": "/r2f/s1/delivery",
        "message_type": "std_msgs/msg/String",
        "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64},
        "delivery_plan": {
            "delay_sec": 0.0,
            "reorder": False,
            "duplicate": False,
            "drop_sequence": 0,
            "lease_action": "none",
        },
    }


# ---------------------------------------------------------------------------
# Operator implementations: parameters
# ---------------------------------------------------------------------------

def _op_param_boundary(document, surface, slot, rng, context):
    requested = context.get("boundary_class")
    classes = _boundary_classes(slot, requested)
    current = _require_slot_present(document, slot, surface)
    start = rng.randrange(len(classes))
    value = chosen_class = None
    for offset in range(len(classes)):
        name = classes[(start + offset) % len(classes)]
        candidate = _numeric_value(slot, name, current, rng)
        if candidate != current:
            value, chosen_class = candidate, name
            break
    if value is None:
        raise MutationRefused("no boundary class changes the parent value",
                              {"slot_id": slot.slot_id, "parent": current})
    minimum, maximum = slot.domain.get("min"), slot.domain.get("max")
    out_of_range = (type(value) in (int, float) and not isinstance(value, bool)
                    and ((minimum is not None and value < minimum)
                         or (maximum is not None and value > maximum)))
    expected = "expect_guard_reject" if out_of_range else "expect_pass"
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
    else:
        input_model.set_path(document, slot.input_path, value)
        changes = {slot.input_path: value}
    return changes, expected, {"guard_disposition": expected,
                               "boundary_class": chosen_class,
                               "selected_value": value,
                               "out_of_declared_range": out_of_range}, {
        "status": "applicable", "boundary_class": chosen_class, "value": value}


def _op_param_enum(document, surface, slot, rng, context):
    values = list(slot.domain.get("values", []))
    if not values:
        raise MutationRefused("enum slot has no declared values", {"slot_id": slot.slot_id})
    current = _require_slot_present(document, slot, surface)
    candidates = [value for value in values if value != current]
    if not candidates:
        raise MutationRefused("enum domain has no distinct value", {"slot_id": slot.slot_id})
    value = context.get("enum_value") or candidates[rng.randrange(len(candidates))]
    if value not in values:
        raise MutationRefused(f"enum value is not declared: {value}",
                              {"slot_id": slot.slot_id, "value": value})
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
    else:
        _set_slot_value(document, slot, value)
        changes = {slot.input_path: value}
        expected = "expect_pass"
    return changes, expected, {"guard_disposition": expected, "enum_value": value}, {
        "status": "applicable", "enum_value": value}


def _pair_of_parent(pairs, writer_value, reader_value):
    """The declared pair label matching the parent document, or None."""
    if not isinstance(writer_value, str) or not isinstance(reader_value, str):
        return None
    label = f"{writer_value[0].upper()}{reader_value[0].upper()}"
    return label if label in pairs else None


def _relation_pair_map(slot):
    values = list(slot.domain.get("values", [])) or ["reliable", "best_effort"]
    return values


def _op_relation_pair(document, surface, slot, rng, context):
    if surface.surface_id == "W01":
        return _op_workload_relation(document, surface, slot, rng, context)
    if surface.surface_id == "S1.topology":
        return _op_topology_relation_pair(document, surface, slot, rng, context)
    # G01 has two enum slots; use both declared paths to express a pair.
    pair_slots = [item for item in surface.slots
                  if item.kind == "parameter_enum" and "reliability" in item.input_path]
    if len(pair_slots) < 2:
        pair_slots = [item for item in surface.slots if item.kind == "parameter_enum"]
    if len(pair_slots) < 2:
        raise MutationRefused("relation pair requires two declared enum slots",
                              {"surface_id": surface.surface_id})
    domain = surface.slots[0].domain
    pairs = list(domain.get("pairs", []))
    incompatible = set(domain.get("incompatible_pairs", []))
    if not pairs:
        values = _relation_pair_map(pair_slots[0])
        pairs = [f"{w[0].upper()}{r[0].upper()}" for w in values for r in values]
    writer_slot = next((item for item in pair_slots if "writer" in item.input_path), pair_slots[0])
    reader_slot = next((item for item in pair_slots if "reader" in item.input_path), pair_slots[1])
    before_writer = _slot_value(document, writer_slot)
    before_reader = _slot_value(document, reader_slot)
    requested = context.get("pair") or context.get("relation_class")
    if requested is not None:
        if requested not in pairs:
            raise MutationRefused(f"pair is not declared: {requested}",
                                  {"surface_id": surface.surface_id, "pair": requested,
                                   "declared": list(pairs)})
        candidates = [requested]
    else:
        # A generated case must differ from its parent: exclude the pair the
        # parent already declares rather than emitting a no-op.
        candidates = [candidate for candidate in pairs
                      if candidate != _pair_of_parent(pairs, before_writer, before_reader)]
        if not candidates:
            raise MutationRefused(
                "declared pair domain has no pair distinct from the parent",
                {"surface_id": surface.surface_id, "declared": list(pairs),
                 "parent_pair": _pair_of_parent(pairs, before_writer, before_reader)})
    values = _relation_pair_map(pair_slots[0])
    by_letter = {value[0].upper(): value for value in values}
    pair = candidates[rng.randrange(len(candidates))]
    if len(pair) != 2 or pair[0] not in by_letter or pair[1] not in by_letter:
        raise MutationRefused(f"pair {pair} does not map onto {values}",
                              {"surface_id": surface.surface_id, "pair": pair})
    writer_value, reader_value = by_letter[pair[0]], by_letter[pair[1]]
    if writer_value == before_writer and reader_value == before_reader:
        raise MutationRefused(
            "requested pair equals the parent pair; the request is honoured "
            "verbatim rather than silently replaced",
            {"surface_id": surface.surface_id, "pair": pair,
             "parent_pair": _pair_of_parent(pairs, before_writer, before_reader)})
    input_model.set_path(document, writer_slot.input_path, writer_value)
    input_model.set_path(document, reader_slot.input_path, reader_value)
    expected = "expect_no_match" if pair in incompatible else "expect_pass"
    return {writer_slot.input_path: writer_value,
            reader_slot.input_path: reader_value}, expected, {
        "guard_disposition": expected, "pair": pair,
        "incompatible": pair in incompatible}, {
        "status": "applicable", "pair": pair, "incompatible": pair in incompatible}


def _op_workload_relation(document, surface, slot, rng, context):
    """W01 depth/post relation using that surface's three declared slots."""
    depth_slot = next((item for item in surface.slots if item.input_path == "qos.depth"), None)
    post_slot = next((item for item in surface.slots if item.input_path == "post_match_count"), None)
    pre_slot = next((item for item in surface.slots if item.input_path == "pre_match_count"), None)
    if not (depth_slot and post_slot and pre_slot):
        raise MutationRefused("W01 relation requires depth/post/pre slots",
                              {"surface_id": surface.surface_id})
    depths = list(depth_slot.domain.get("representatives", []))
    posts = list(post_slot.domain.get("representatives", []))
    pres = list(pre_slot.domain.get("representatives", []))
    requested = context.get("relation_class") or context.get("relation")
    labels = [name for name in ("depth_lt_post", "depth_eq_post", "depth_gt_post")]
    if requested is not None and requested not in labels:
        raise MutationRefused(f"relation class is not declared: {requested}",
                              {"surface_id": surface.surface_id,
                               "relation_class": requested})
    witnesses = {}
    for label in labels:
        pairs = []
        for depth in depths:
            for post in posts:
                if (label == "depth_lt_post" and depth < post) or (
                        label == "depth_eq_post" and depth == post) or (
                        label == "depth_gt_post" and depth > post):
                    pairs.append((depth, post))
        if pairs:
            witnesses[label] = pairs
    pool = [requested] if requested else [label for label in labels if label in witnesses]
    if not pool:
        raise MutationRefused("relation slot declares no reachable witness class",
                              {"surface_id": surface.surface_id})
    label = pool[rng.randrange(len(pool))]
    depth, post = witnesses[label][rng.randrange(len(witnesses[label]))]
    pre = pres[rng.randrange(len(pres))]
    input_model.set_path(document, "qos.depth", depth)
    input_model.set_path(document, "pre_match_count", pre)
    input_model.set_path(document, "post_match_count", post)
    document.setdefault("workload_relation", {}).update(
        {"depth": depth, "pre_count": pre, "post_count": post, "class": label})
    changes = {"qos.depth": depth, "pre_match_count": pre, "post_match_count": post,
               "workload_relation.class": label,
               "workload_relation.depth": depth,
               "workload_relation.pre_count": pre,
               "workload_relation.post_count": post}
    return changes, "expect_pass", {"guard_disposition": "expect_pass",
                                    "relation_class": label,
                                    "witness": {"depth": depth, "post_count": post}}, {
        "status": "applicable", "relation_class": label}


def _op_topology_relation_pair(document, surface, slot, rng, context):
    """Offered/requested reliability pair over concrete endpoints."""
    writers = [index for index, ep in enumerate(document.get("endpoints", []))
               if ep.get("role") == "writer"]
    readers = [index for index, ep in enumerate(document.get("endpoints", []))
               if ep.get("role") == "reader"]
    if not writers or not readers:
        raise MutationRefused("relation pair requires at least one writer and reader",
                              {"surface_id": surface.surface_id})
    pairs = [("reliable", "reliable", "RR"),
             ("reliable", "best_effort", "RB"),
             ("best_effort", "best_effort", "BB"),
             ("best_effort", "reliable", "BR")]
    writer_index = writers[0]
    reader_index = readers[0]
    parent_writer = document["endpoints"][writer_index]["qos"].get("reliability")
    parent_reader = document["endpoints"][reader_index]["qos"].get("reliability")
    requested = context.get("pair")
    if requested is not None:
        matches = [item for item in pairs if item[2] == requested]
        if not matches:
            raise MutationRefused(f"pair is not declared: {requested}",
                                  {"surface_id": surface.surface_id, "pair": requested,
                                   "declared": [item[2] for item in pairs]})
        writer_value, reader_value, label = matches[0]
        if writer_value == parent_writer and reader_value == parent_reader:
            raise MutationRefused(
                "requested pair equals the parent pair; the request is honoured "
                "verbatim rather than silently replaced",
                {"surface_id": surface.surface_id, "pair": requested})
    else:
        candidates = [item for item in pairs
                      if not (item[0] == parent_writer and item[1] == parent_reader)]
        if not candidates:
            raise MutationRefused(
                "declared pair domain has no pair distinct from the parent",
                {"surface_id": surface.surface_id})
        writer_value, reader_value, label = candidates[rng.randrange(len(candidates))]
    document["endpoints"][writer_index]["qos"]["reliability"] = writer_value
    document["endpoints"][reader_index]["qos"]["reliability"] = reader_value
    expected, _ = _topology_validation_disposition(document)
    # The offline model's guard disposition tracks structure, not QoS matching;
    # an incompatible reliability pair is a no-match expectation instead.
    order = ["best_effort", "reliable"]
    if order.index(writer_value) < order.index(reader_value):
        expected = "expect_no_match"
    changes = {f"endpoints[{writer_index}].qos.reliability": writer_value,
               f"endpoints[{reader_index}].qos.reliability": reader_value,
               "pair": label}
    return changes, expected, {"guard_disposition": expected, "pair": label,
                               "writer_index": writer_index,
                               "reader_index": reader_index}, {
        "status": "applicable", "pair": label}


def _op_timing_shift(document, surface, slot, rng, context):
    domain = slot.domain
    current = _require_slot_present(document, slot, surface)
    step = domain.get("step", 0.001)
    reps = [value for value in domain.get("representatives", [])
            if type(value) in (int, float)]
    if context.get("offset_sec") is not None:
        offset = context["offset_sec"]
        if type(offset) not in (int, float) or offset < 0:
            raise MutationRefused("offset_sec must be a non-negative number",
                                  {"slot_id": slot.slot_id})
    elif reps:
        base = current if type(current) in (int, float) else reps[0]
        offset = reps[rng.randrange(len(reps))] - base
        if offset == 0:
            offset = step
    else:
        base = current if type(current) in (int, float) else (domain.get("min", 0))
        offset = step
    if type(current) not in (int, float):
        value = (domain.get("min", 0) or 0) + offset
    else:
        value = current + offset
    if value < 0:
        value = 0
    if domain.get("max") is not None and value > domain["max"]:
        value = domain["max"]
    if value == current:
        value = current + step if current is not None else step
    expected = "expect_pass"
    if value is not None and domain.get("max") is not None and value > domain["max"]:
        expected = "expect_guard_reject"
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
    else:
        _set_slot_value(document, slot, value)
        changes = {slot.input_path: value}
    return changes, expected, {"guard_disposition": expected, "offset_sec": offset}, {
        "status": "applicable", "offset_sec": offset}


def _op_queue_depth(document, surface, slot, rng, context):
    domain = slot.domain
    reps = list(domain.get("representatives", []))
    requested = context.get("boundary_class")
    if requested is not None:
        classes = _boundary_classes(slot, requested)
        current = _require_slot_present(document, slot, surface)
        value = _numeric_value(slot, classes[0], current, rng)
    elif reps:
        candidates = [value for value in reps if value != _slot_value(document, slot)] or reps
        value = candidates[rng.randrange(len(candidates))]
    else:
        current = _slot_value(document, slot)
        value = _numeric_value(slot, "neighbor", current, rng)
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
    else:
        _set_slot_value(document, slot, value)
        changes = {slot.input_path: value}
        expected = "expect_pass"
    return changes, expected, {"guard_disposition": expected, "depth": value}, {
        "status": "applicable", "depth": value}


def _op_lease_value(document, surface, slot, rng, context):
    requested = context.get("special_class") or context.get("boundary_class")
    classes = [requested] if requested else ["zero", "finite", "infinite"]
    classes = [name for name in classes if name in slot.special_values or name == "finite"]
    if not classes:
        raise MutationRefused("lease slot has no declared special class",
                              {"slot_id": slot.slot_id})
    name = classes[rng.randrange(len(classes))]
    current = _require_slot_present(document, slot, surface)
    if name == "finite":
        reps = [value for value in slot.domain.get("representatives", [])
                if type(value) in (int, float) and value != 0]
        value = reps[rng.randrange(len(reps))] if reps else (
            slot.domain.get("min", 1) + slot.domain.get("step", 1))
    else:
        value = _special(slot, name)
    if value == current and name == "finite":
        value = value + slot.domain.get("step", 1)
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
    else:
        _set_slot_value(document, slot, value)
        changes = {slot.input_path: value}
        expected = "expect_pass"
    return changes, expected, {"guard_disposition": expected, "lease_class": name}, {
        "status": "applicable", "lease_class": name}


_TOPOLOGY_FILTER_SCHEMAS = {
    # This is the message schema declared by the current topology worker, not
    # an inferred field set. Unknown type names fail closed for valid filters.
    "std_msgs/msg/String": {"data": "string"},
}
_TOPOLOGY_STRING_FILTERS = (
    ("data <> ''", []),
    ("data = 'r2f'", []),
    ("data = %0", ["'r2f'"]),
)


def _topology_filter_endpoint_index(slot, document):
    """Return the reader index selected by the resolved input-domain slot."""
    prefix = "S1.topology.filter#"
    if not slot.slot_id.startswith(prefix):
        raise MutationRefused("filter_endpoint_slot_unresolved", {"slot_id": slot.slot_id})
    index_text = slot.slot_id[len(prefix):]
    if not index_text.isdecimal() or str(int(index_text)) != index_text:
        raise MutationRefused("filter_endpoint_slot_unresolved", {"slot_id": slot.slot_id})
    index = int(index_text)
    expected_path = f"endpoints[{index}].filter.expression"
    if slot.input_path != expected_path:
        raise MutationRefused("filter_endpoint_slot_path_mismatch",
                              {"slot_id": slot.slot_id, "input_path": slot.input_path,
                               "expected_path": expected_path})
    endpoints = document.get("endpoints", [])
    if index >= len(endpoints) or endpoints[index].get("role") != "reader":
        raise MutationRefused("endpoint_index is not a reader",
                              {"surface_id": "S1.topology", "endpoint_index": index})
    return index


def _topology_valid_filter(endpoint, slot, rng):
    type_name = endpoint.get("type_name")
    field_types = _TOPOLOGY_FILTER_SCHEMAS.get(type_name)
    if field_types is None:
        raise MutationRefused(
            "filter_schema_unknown",
            {"type_name": type_name, "logical_id": endpoint.get("logical_id"),
             "slot_id": slot.slot_id, "valid_filter_emitted": False})
    if field_types.get("data") != "string":
        raise MutationRefused(
            "filter_schema_unsupported",
            {"type_name": type_name, "field_types": field_types,
             "slot_id": slot.slot_id, "valid_filter_emitted": False})
    existing = endpoint.get("filter") or {}
    prior = (existing.get("expression"), existing.get("parameters", []))
    alternatives = [(expression, parameters) for expression, parameters in
                    _TOPOLOGY_STRING_FILTERS if (expression, parameters) != prior]
    if not alternatives:
        raise MutationRefused(
            "filter_schema_has_no_distinct_valid_expression",
            {"type_name": type_name, "logical_id": endpoint.get("logical_id"),
             "slot_id": slot.slot_id, "valid_filter_emitted": False})
    expression, parameters = alternatives[rng.randrange(len(alternatives))]
    return expression, copy.deepcopy(parameters), field_types


def _op_filter_expr(document, surface, slot, rng, context):
    topology = _surface_document(surface) == "topology"
    variant = context.get("filter_variant")
    # The profile's I2 operator selection means "generate a valid filter";
    # empty/invalid remain explicit, reproducible negative/neutral requests.
    variants = ([variant] if variant else ["valid"] if topology
                else ["empty", "valid", "invalid"])
    name = variants[rng.randrange(len(variants))]
    max_length = slot.domain.get("max_length", 1024)
    if name == "empty":
        value, parameters = "", []
    elif name == "invalid":
        value, parameters = "x" * (max_length + 1), []
    elif name == "valid" and topology:
        index = _topology_filter_endpoint_index(slot, document)
        endpoint = document["endpoints"][index]
        value, parameters, _ = _topology_valid_filter(endpoint, slot, rng)
    elif name == "valid":
        # Preserve the pre-existing non-topology input domain. Its schemas are
        # not endpoint declarations, so it is not used for topology claims.
        value, parameters = "seq >= %0", ["1"]
    else:
        raise MutationRefused(f"unknown filter variant: {name}", {"slot_id": slot.slot_id})
    if topology:
        index = _topology_filter_endpoint_index(slot, document)
        endpoint = document["endpoints"][index]
        endpoint["filter"] = {"expression": value, "parameters": parameters,
                              "evaluation": None}
        expected = "expect_guard_reject" if name == "invalid" else "expect_pass"
        changes = {f"endpoints[{index}].filter": copy.deepcopy(endpoint["filter"])}
    else:
        _set_slot_value(document, slot, value)
        changes = {slot.input_path: value}
        expected = "expect_guard_reject" if name == "invalid" else "expect_pass"
    return changes, expected, {"guard_disposition": expected, "filter_variant": name}, {
        "status": "rejected" if name == "invalid" and topology else "applicable",
        "filter_variant": name,
        "execution_evidence": False,
        "candidate_eligible": False,
    }


# ---------------------------------------------------------------------------
# Operator implementations: structure / actions / generation
# ---------------------------------------------------------------------------

_TOPO_DELEGATE = {
    "action.insert": "insert_action",
    "action.delete": "delete_action",
    "action.swap": "swap_independent",
    "guard.environment": "environment_guard",
    "guard.dependency": "dependency_guard",
    "guard.structure": "structure_guard",
    "guard.parameter": "parameter_guard",
    "endpoint.count": "add_endpoint",
}


def _op_guard_parameter(document, surface, slot, rng, context):
    if _surface_document(surface) == "topology":
        return _op_topology_delegate(document, surface, slot, rng, context)
    domain = slot.domain
    maximum = domain.get("max")
    if maximum is None:
        raise MutationRefused("guard.parameter needs a declared maximum",
                              {"surface_id": surface.surface_id, "slot_id": slot.slot_id})
    step = domain.get("step", 1)
    value = maximum + step
    current = _slot_value(document, slot)
    if value == current:
        value = maximum + step * 2
    input_model.set_path(document, slot.input_path, value)
    changes = {slot.input_path: value}
    return changes, "expect_guard_reject", {
        "guard_disposition": "expect_guard_reject", "declared_max": maximum,
        "value": value}, {"status": "applicable", "value": value}


def _op_topology_delegate(document, surface, slot, rng, context):
    operator_id = context["_operator_id"]
    name = _TOPO_DELEGATE[operator_id]
    direction = context.get("direction", "add")
    if operator_id == "endpoint.count" and direction == "remove":
        endpoint_ids = [endpoint["logical_id"] for endpoint in document["endpoints"]]
        if not endpoint_ids:
            raise MutationRefused("no endpoint to remove", {"surface_id": surface.surface_id})
        target = context.get("endpoint_id") or endpoint_ids[rng.randrange(len(endpoint_ids))]
        if target not in endpoint_ids:
            raise MutationRefused(f"unknown endpoint_id: {target}",
                                  {"surface_id": surface.surface_id, "endpoint_id": target})
        removed = {target}
        changed = True
        while changed:
            changed = False
            for action in document["actions"]:
                if action["action_id"] in removed:
                    continue
                if action.get("endpoint_id") == target or set(action.get("depends_on", [])) & removed:
                    removed.add(action["action_id"])
                    changed = True
        before_endpoints = len(document["endpoints"])
        document["endpoints"] = [endpoint for endpoint in document["endpoints"]
                                 if endpoint["logical_id"] != target]
        document["actions"] = [action for action in document["actions"]
                               if action["action_id"] not in removed]
        # Barrier targets referencing the removed endpoint would leave a
        # structurally invalid document; strip them instead of claiming a
        # legal removal that never validates.
        for action in document["actions"]:
            if action.get("op") == "barrier" and "endpoint_ids" in action["params"]:
                action["params"]["endpoint_ids"] = [
                    eid for eid in action["params"]["endpoint_ids"] if eid != target]
        expected, validation = _topology_validation_disposition(document)
        changes = {
            "kind": "endpoint_remove",
            "endpoint_id": target,
            "removed_action_ids": sorted(removed),
            "remaining_endpoint_ids": [endpoint["logical_id"]
                                       for endpoint in document["endpoints"]],
        }
        return changes, expected, {"guard_disposition": expected,
                                   "endpoint_id": target,
                                   "removed_count": before_endpoints - len(document["endpoints"])}, {
            "status": "applicable", "direction": "remove", "endpoint_id": target}
    try:
        result = topology_model.mutate_program(document, name, seed=rng.randrange(2**31))
    except ValueError as exc:
        raise MutationRefused(str(exc), {"surface_id": surface.surface_id,
                                         "operator_id": operator_id,
                                         "delegate": name}) from exc
    mutated = result["program"]
    document.clear()
    document.update(mutated)
    disposition = {
        "legal_requires_runtime": "expect_pass",
        "invalid_structure": "expect_guard_reject",
        "invalid_parameters": "expect_guard_reject",
        "guard_negative": "expect_guard_reject",
    }.get(result["intent"], "unknown_not_pass")
    changes = {
        "kind": "topology_mutation",
        "delegate": name,
        "intent": result["intent"],
        "detail": copy.deepcopy(result["receipt"]["details"]),
        "before_sha256": result["receipt"]["before_sha256"],
        "after_sha256": result["receipt"]["after_sha256"],
    }
    return changes, disposition, {"guard_disposition": disposition,
                                  "intent": result["intent"]}, {
        "status": "applicable", "delegate": "topology_model.mutate_program",
        "intent": result["intent"]}


def _op_generation_action(document, surface, slot, rng, context):
    variant = context.get("variant", "next")
    endpoints = document.get("endpoints", [])
    if not endpoints:
        raise MutationRefused("generation mutation requires an endpoint", {"surface_id": surface.surface_id})
    writers = [endpoint for endpoint in endpoints if endpoint.get("role") == "writer"]
    target = context.get("endpoint_id") or (writers[0]["logical_id"] if writers
                                             else endpoints[0]["logical_id"])
    actions = document.setdefault("actions", [])
    existing = [action for action in actions if action.get("endpoint_id") == target
                and action.get("op") in ("create", "recreate")]
    if variant == "stale":
        if not existing:
            raise MutationRefused("stale generation requires an existing create action",
                                  {"surface_id": surface.surface_id, "endpoint_id": target})
        action = existing[0]
        old = action["generation"]
        action["generation"] = 0 if old != 0 else 1
        expected, validation = _topology_validation_disposition(document)
        changes = {"kind": "topology_generation",
                   "action_id": action["action_id"], "generation": action["generation"]}
        return changes, expected, {"guard_disposition": expected,
                                   "stale_generation": True}, {
            "status": "applicable", "variant": "stale", "action_id": action["action_id"]}
    # Insert a bounded recreate after the last action for the target.
    new_id = "recreate_" + target
    index = 0
    ids = {action["action_id"] for action in actions}
    while new_id in ids:
        index += 1
        new_id = f"recreate_{target}_{index}"
    generation = 2
    actions.append({"action_id": new_id, "op": "recreate", "endpoint_id": target,
                    "generation": generation, "params": {}, "depends_on": []})
    expected, validation = _topology_validation_disposition(document)
    changes = {"kind": "topology_action_insert", "action_id": new_id,
               "op": "recreate", "generation": generation}
    return changes, expected, {"guard_disposition": expected,
                               "variant": "next"}, {
        "status": "applicable", "variant": "next", "action_id": new_id}


# ---------------------------------------------------------------------------
# Operator implementations: environment
# ---------------------------------------------------------------------------

def _op_guard_environment(document, surface, slot, rng, context):
    if _surface_document(surface) != "topology":
        raise MutationRefused("guard.environment is implemented for S1.topology",
                              {"surface_id": surface.surface_id})
    key = context.get("key") or "backend_available"
    if key not in {"backend_available", "transport_ready", "discovery_enabled",
                   "type_matching", "filter_support", "filter_validity"}:
        raise MutationRefused(f"unknown environment key: {key}",
                              {"surface_id": surface.surface_id, "key": key})
    environment = document.setdefault("environment", {})
    old = environment.get(key)
    if key == "type_matching":
        value = "assignable" if old != "assignable" else "exact_name"
    else:
        value = False if old is not False else True
    environment[key] = value
    expected, validation = _topology_validation_disposition(document)
    if key == "backend_available" and value is False:
        # A declared fault only has a guard witness if an action actually
        # requires the backend.  Otherwise report unknown, not a fake reject.
        expected = ("expect_guard_reject"
                    if validation.get("guard_disposition") == "guard_violation"
                    else "unknown_not_pass")
    elif value is None:
        expected = "unknown_not_pass"
    return ({f"environment.{key}": value}, expected,
            {"guard_disposition": expected, "key": key},
            {"status": "applicable", "environment_key": key})


def _op_env_delay(document, surface, slot, rng, context):
    if _surface_document(surface) == "delivery":
        plan = document.setdefault("delivery_plan", {})
        current = plan.get("delay_sec", 0.0)
        value = context.get("delay_sec")
        if value is None:
            value = 0.0 if current else min(0.05, slot.domain.get("max", 30.0))
        plan["delay_sec"] = value
        return ({"delivery_plan.delay_sec": value}, "unknown_not_pass",
                {"guard_disposition": "unknown_not_pass", "runtime_consumer": "unbound"},
                {"status": "applicable", "value": value})
    if slot.kind not in {"parameter_timing", "parameter_lease"}:
        raise MutationRefused("env.delay requires a timing slot or a delivery plan",
                              {"surface_id": surface.surface_id, "slot_id": slot.slot_id})
    current = _slot_value(document, slot)
    step = slot.domain.get("step", 0.001)
    value = (current + step if type(current) in (int, float) else
             slot.domain.get("min", 0) + step)
    if slot.domain.get("max") is not None:
        value = min(value, slot.domain["max"])
    if value == current:
        raise MutationRefused("delay would not change the timing value",
                              {"surface_id": surface.surface_id, "slot_id": slot.slot_id})
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
        return (changes, expected,
                {"guard_disposition": expected, "delay_sec": value},
                {"status": "applicable", "value": value})
    _set_slot_value(document, slot, value)
    return ({slot.input_path: value}, "expect_pass",
            {"guard_disposition": "expect_pass", "delay_sec": value},
            {"status": "applicable", "value": value})


def _op_env_toggle(document, surface, slot, rng, context):
    if _surface_document(surface) != "delivery":
        raise MutationRefused("delivery-plan mutation requires S1.delivery",
                              {"surface_id": surface.surface_id,
                               "operator_id": context.get("_operator_id")})
    operator_id = context["_operator_id"]
    plan = document.setdefault("delivery_plan", {})
    field = {"env.reorder": "reorder", "env.duplicate": "duplicate"}[operator_id]
    value = not bool(plan.get(field, False))
    plan[field] = value
    return ({f"delivery_plan.{field}": value}, "unknown_not_pass",
            {"guard_disposition": "unknown_not_pass", "runtime_consumer": "unbound"},
            {"status": "applicable", "value": value})


def _op_env_drop(document, surface, slot, rng, context):
    if _surface_document(surface) == "delivery":
        plan = document.setdefault("delivery_plan", {})
        current = plan.get("drop_sequence", 0)
        candidates = context.get("drop_sequence")
        if candidates is None:
            declared = [1, 3, 6, 0]
            candidates = [value for value in declared if value != current] or declared
        elif isinstance(candidates, int):
            candidates = [candidates]
        candidates = [value for value in candidates if value != current] or list(candidates)
        value = candidates[rng.randrange(len(candidates))]
        if type(value) is not int or value < 0:
            raise MutationRefused("drop_sequence must be a non-negative integer",
                                  {"surface_id": surface.surface_id})
        plan["drop_sequence"] = value
        expected = "unknown_not_pass"
        return ({"delivery_plan.drop_sequence": value}, expected,
                {"guard_disposition": expected, "runtime_consumer": "unbound"},
                {"status": "applicable", "value": value})
    # Catalog discovery-drop surface: mutate its duration.
    if slot.kind not in {"environment", "parameter_timing"}:
        raise MutationRefused("env.drop requires a drop duration or delivery plan",
                              {"surface_id": surface.surface_id})
    current = _slot_value(document, slot)
    values = [value for value in slot.domain.get("representatives", [])
              if type(value) in (int, float) and value != current]
    if not values:
        # E01 declares a sampled range but no explicit representatives; use a
        # bounded step inside that declared range.
        domain = slot.domain
        step = domain.get("step", 1)
        candidate = (current if type(current) in (int, float)
                     else domain.get("min", 0)) + step
        if domain.get("max") is not None and candidate > domain["max"]:
            candidate = domain.get("min", 0)
        if candidate == current:
            raise MutationRefused("no distinct drop duration declared",
                                  {"slot_id": slot.slot_id})
        values = [candidate]
    value = context.get("drop_duration_sec") or values[rng.randrange(len(values))]
    _set_slot_value(document, slot, value)
    return ({slot.input_path: value}, "expect_pass",
            {"guard_disposition": "expect_pass", "drop_duration_sec": value},
            {"status": "applicable", "value": value})


def _op_env_resource_health(document, surface, slot, rng, context):
    if _surface_document(surface) != "topology":
        raise MutationRefused("resource/health mutation requires S1.topology",
                              {"surface_id": surface.surface_id})
    operator_id = context["_operator_id"]
    key = context.get("key") or ("backend_available" if operator_id == "env.resource"
                                 else "transport_ready")
    if key not in {"backend_available", "transport_ready", "discovery_enabled",
                   "filter_support", "filter_validity"}:
        raise MutationRefused(f"unknown environment key: {key}",
                              {"surface_id": surface.surface_id, "key": key})
    environment = document.setdefault("environment", {})
    if operator_id == "env.resource":
        value = False
        expected = "expect_guard_reject"
    else:
        value = None
        expected = "unknown_not_pass"
    environment[key] = value
    if operator_id == "env.resource":
        _, validation = _topology_validation_disposition(document)
        if not any(row["disposition"] == "expected_guard_reject"
                   for row in validation.get("actions", [])):
            # Declaring an unused resource flag changes identity but does not
            # create a guard negative; report it honestly.
            expected = "unknown_not_pass"
    return ({f"environment.{key}": value}, expected,
            {"guard_disposition": expected, "key": key},
            {"status": "applicable", "environment_key": key})


def _op_env_lease(document, surface, slot, rng, context):
    operator_id = context["_operator_id"]
    action = "expire" if operator_id == "env.lease_expiry" else "refresh"
    if _surface_document(surface) == "delivery":
        plan = document.setdefault("delivery_plan", {})
        current_action = plan.get("lease_action", "none")
        if current_action == action:
            # Rotate through the declared actions so a legal domain always yields
            # a case distinct from its parent.
            declared = list(slot.domain.get("values", ["none", "expire", "refresh"]))
            alternatives = [value for value in declared if value != current_action]
            if not alternatives:
                raise MutationRefused("lease action domain has no alternative",
                                      {"surface_id": surface.surface_id})
            action = alternatives[rng.randrange(len(alternatives))]
        plan["lease_action"] = action
        return ({"delivery_plan.lease_action": action}, "unknown_not_pass",
                {"guard_disposition": "unknown_not_pass",
                 "lease_lifecycle": "declared_only"},
                {"status": "applicable", "lease_action": action})
    if slot.kind != "parameter_lease":
        raise MutationRefused("lease lifecycle requires a lease slot",
                              {"surface_id": surface.surface_id, "slot_id": slot.slot_id})
    current = _slot_value(document, slot)
    if action == "expire":
        value = 0
    else:
        finites = [value for value in slot.domain.get("representatives", [])
                   if type(value) in (int, float) and value > 0]
        value = finites[rng.randrange(len(finites))] if finites else slot.domain.get("min", 1)
    if value == current:
        raise MutationRefused("lease action would not change the declared value",
                              {"slot_id": slot.slot_id})
    if _surface_document(surface) == "topology":
        paths = _topology_document_for_slot(document, slot, value, rng=rng, context=context)
        changes = {path: value for path in paths}
        expected, _ = _topology_validation_disposition(document)
        return changes, expected, {"guard_disposition": expected,
                                   "lease_lifecycle": "declared_only",
                                   "lease_action": action}, {
            "status": "applicable", "lease_action": action, "value": value}
    _set_slot_value(document, slot, value)
    return ({slot.input_path: value}, "unknown_not_pass",
            {"guard_disposition": "unknown_not_pass",
             "lease_lifecycle": "declared_only"},
            {"status": "applicable", "lease_action": action, "value": value})


_OPERATORS = {
    "param.boundary": _op_param_boundary,
    "param.enum": _op_param_enum,
    "relation.pair": _op_relation_pair,
    "timing.shift": _op_timing_shift,
    "queue.depth": _op_queue_depth,
    "lease.value": _op_lease_value,
    "filter.expr": _op_filter_expr,
    "endpoint.count": _op_topology_delegate,
    "generation.action": _op_generation_action,
    "action.insert": _op_topology_delegate,
    "action.delete": _op_topology_delegate,
    "action.swap": _op_topology_delegate,
    "guard.environment": _op_guard_environment,
    "guard.dependency": _op_topology_delegate,
    "guard.structure": _op_topology_delegate,
    "guard.parameter": _op_guard_parameter,
    "env.delay": _op_env_delay,
    "env.reorder": _op_env_toggle,
    "env.duplicate": _op_env_toggle,
    "env.drop": _op_env_drop,
    "env.resource": _op_env_resource_health,
    "env.health": _op_env_resource_health,
    "env.lease_expiry": _op_env_lease,
    "env.lease_refresh": _op_env_lease,
}

assert set(_OPERATORS) == set(OPERATOR_SPECS) == set(input_model.OPERATOR_IDS)


# ---------------------------------------------------------------------------
# Public single-operator API
# ---------------------------------------------------------------------------

def make_rng(seed):
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a non-negative integer")
    return random.Random(seed)


def mutate(surface, operator_id, document, *, seed=0, slot_id=None, input_path=None,
           context=None):
    """Apply one declared operator and return a content-addressed case.

    Raises :class:`UnknownDomainError`, :class:`UndeclaredOperatorError`,
    :class:`MutationRefused` or a schema ``ValueError`` instead of emitting a
    silent no-op.  Nothing is executed.
    """
    surface = _as_surface(surface)
    spec = _check_operator(operator_id)
    document = copy.deepcopy(document)
    before_doc = copy.deepcopy(document)
    _check_document(document, surface)
    context = dict(context or {})
    if "_operator_id" not in context:
        context["_operator_id"] = operator_id
    candidates = [slot for slot in surface.slots if operator_id in slot.operators]
    if spec.accepted_kinds:
        accepted = [slot for slot in candidates if slot.kind in spec.accepted_kinds]
        if accepted:
            candidates = accepted
    if slot_id is not None:
        candidates = [slot for slot in candidates if slot.slot_id == slot_id]
    if input_path is not None:
        candidates = [slot for slot in candidates
                      if slot.input_path == input_path
                      or input_model.covered(input_path, [slot.input_path])]
    if not candidates:
        if surface.slots:
            raise UndeclaredOperatorError(
                f"operator {operator_id} is not declared by surface {surface.surface_id}")
        raise UndeclaredOperatorError("surface declares no slots")
    if len(candidates) > 1:
        # Prefer a slot whose declared kind is accepted by the operator and
        # whose path actually exists in the document; remaining ambiguity is
        # an explicit error rather than an arbitrary pick.
        existing = []
        for slot in candidates:
            try:
                input_model.get_path(document, slot.input_path)
                existing.append(slot)
            except KeyError:
                pass
        if len(existing) == 1:
            candidates = existing
    if len(candidates) > 1:
        if slot_id is None and input_path is None:
            # Deterministic rotation over declared slots keeps seeds reproducible
            # without inventing an undeclared target.
            rng0 = make_rng(seed)
            candidates = [candidates[rng0.randrange(len(candidates))]]
        else:
            raise MutationRefused("slot selector is ambiguous",
                                  {"surface_id": surface.surface_id,
                                   "operator_id": operator_id,
                                   "candidates": [slot.slot_id for slot in candidates]})
    slot = candidates[0]
    if operator_id == "relation.pair" and surface.surface_id == "G01":
        slot = next((item for item in surface.slots if "reliability" in item.input_path), slot)
    if spec.accepted_kinds and slot.kind not in spec.accepted_kinds:
        raise MutationRefused(
            f"operator {operator_id} does not accept slot kind {slot.kind}",
            {"surface_id": surface.surface_id, "slot_id": slot.slot_id,
             "accepted_kinds": list(spec.accepted_kinds)})
    rng = make_rng(seed)
    if operator_id == "filter.expr" and surface.surface_id == "S1.topology":
        slot = _resolve_topology_filter_slot(slot, document, make_rng(seed), context)
    else:
        slot = _resolve_wildcard_slot(slot, document, make_rng(seed), {})
    handler = _OPERATORS[operator_id]
    changes, expected, guard_effect, applicability = handler(
        document, surface, slot, rng, context)
    if document == before_doc:
        raise MutationRefused("operator produced identical content",
                              {"surface_id": surface.surface_id,
                               "operator_id": operator_id, "slot_id": slot.slot_id})
    return _make_case(
        surface, operator_id, slot.slot_id, document,
        _content_hash(surface.surface_id, before_doc),
        changes, seed, expected, guard_effect, applicability,
        chain=[{"operator_id": operator_id, "slot_id": slot.slot_id,
                "parameters": {"seed": seed, **(context.get("parameters") or {})}}],
    )


def try_mutate(*args, **kwargs):
    """Same as :func:`mutate` but returns a refusal record instead of raising."""
    try:
        return {"status": "emitted", "case": mutate(*args, **kwargs).to_dict()}
    except MutationRefused as exc:
        return {"status": "refused", "reason": exc.reason, "details": exc.details,
                "execution_evidence": False, "candidate_eligible": False}


# ---------------------------------------------------------------------------
# Bounded composition
# ---------------------------------------------------------------------------

_DISPOSITION_PRIORITY = {
    "expect_guard_reject": 3,
    "expect_no_match": 2,
    "unknown_not_pass": 1,
    "expect_pass": 0,
}


def combine_dispositions(dispositions):
    """Chain expectation: the strongest guard expectation wins.

    A chain that contains a deliberate guard negative cannot be reported as a
    plain pass even if later layers are legal.
    """
    best = "expect_pass"
    for item in dispositions:
        if _DISPOSITION_PRIORITY[item] > _DISPOSITION_PRIORITY[best]:
            best = item
    return best


def mutate_chain(surface, operators, document, *, seed=0, context=None):
    """Apply 2..4 declared operators in order, preserving each layer's hash.

    Undeclared operator combinations are rejected unless the surface declares
    them in ``combinations``.  The last layer is terminal: a layer that would
    produce an invalid document for a non-guard operator refuses.
    """
    surface = _as_surface(surface)
    if not isinstance(operators, (list, tuple)):
        raise ValueError("operators must be a list/tuple")
    if not 2 <= len(operators) <= min(MAX_CHAIN_LENGTH, surface.chain_max):
        raise ValueError(f"chain requires 2..{min(MAX_CHAIN_LENGTH, surface.chain_max)} operators")
    for operator_id in operators:
        _check_operator(operator_id)
    declared = {tuple(combo.operators) for combo in surface.combinations}
    if tuple(operators) not in declared and tuple(reversed(operators)) not in declared:
        raise MutationRefused("operator combination is not declared by the surface",
                              {"surface_id": surface.surface_id,
                               "operators": list(operators),
                               "declared": [list(item) for item in sorted(declared)]})
    current = copy.deepcopy(document)
    _check_document(current, surface)
    layers, cases = [], []
    for index, operator_id in enumerate(operators):
        layer_context = dict(context or {})
        layer_context["_operator_id"] = operator_id
        layer_context["parameters"] = {"seed": seed + index,
                                       "phase": "chain_layer", "index": index}
        case = mutate(surface, operator_id, current, seed=seed + index,
                      context=layer_context)
        layers.append({"index": index, "operator_id": operator_id,
                       "slot_id": case.slot_id,
                       "input_before_sha256": case.input_before_sha256,
                       "content_sha256": case.content_sha256,
                       "expected_disposition": case.expected_disposition})
        cases.append(case)
        current = case.input
        if case.expected_disposition == "expect_guard_reject" and index + 1 < len(operators):
            raise MutationRefused(
                "chain continues after a deliberate guard negative",
                {"surface_id": surface.surface_id, "layer": index,
                 "operator_id": operator_id})
    final = cases[-1]
    provenance = dict(final.provenance)
    provenance["chain"] = [
        {"index": index, "operator_id": layer["operator_id"],
         "slot_id": layer["slot_id"],
         "input_before_sha256": layer["input_before_sha256"],
         "content_sha256": layer["content_sha256"]}
        for index, layer in enumerate(layers)
    ]
    provenance["chain_length"] = len(operators)
    provenance["parent_of_chain_sha256"] = cases[0].input_before_sha256
    return MutationCase(
        surface_id=surface.surface_id, operator_id=operators[-1],
        slot_id=final.slot_id, seed=seed,
        input_before_sha256=_content_hash(surface.surface_id, document),
        content_sha256=final.content_sha256, input=final.input,
        changes=final.changes, binding=final.binding,
        expected_disposition=combine_dispositions(
            [case.expected_disposition for case in cases]),
        expected_guard_effect={"guard_disposition": combine_dispositions(
            [case.expected_disposition for case in cases]),
            "layers": [case.expected_guard_effect for case in cases]},
        applicability={"status": "applicable", "layers": layers},
        provenance=provenance,
    )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def dedupe_cases(cases):
    """Keep the first case per content hash; report duplicates explicitly."""
    seen, kept, duplicates = set(), [], []
    for case in cases:
        if case.content_sha256 in seen:
            duplicates.append({"content_sha256": case.content_sha256,
                               "operator_id": case.operator_id,
                               "seed": case.seed})
            continue
        seen.add(case.content_sha256)
        kept.append(case)
    return {"cases": kept, "duplicates": duplicates,
            "unique": len(kept), "duplicate_count": len(duplicates)}


# ---------------------------------------------------------------------------
# Seed corpus (offline authored/pending/unsupported manifest)
# ---------------------------------------------------------------------------

#: Runtime capabilities declared by other modules but not implemented by this
#: input-only module.  They stay pending; they are never promoted to authored
#: seeds or runtime evidence.
PENDING_CAPABILITIES = (
    {"capability": "E02.multi_window_runner", "surface_id": "E02",
     "status": "pending", "reason": "multi-window runner is not wired"},
    {"capability": "E03.event_anchor_runner", "surface_id": "E03",
     "status": "pending", "reason": "event-anchor runner is not wired"},
    {"capability": "G01.per_role_qos_runtime", "surface_id": "G01",
     "status": "pending", "reason": "per-role offered/requested runtime is not wired"},
    {"capability": "S1.delivery.runtime_consumer", "surface_id": "S1.delivery",
     "status": "pending", "reason": "delivery-plan fields have no runtime consumer"},
    {"capability": "lease.lifecycle_runtime", "surface_id": "P06",
     "status": "pending", "reason": "lease expiry/refresh lifecycle is not wired"},
)

UNSUPPORTED_CAPABILITIES = (
    {"capability": "xtypes_type_coercion", "surface_id": "S1.topology",
     "status": "unsupported", "reason": "XTypes coercion is not modelled; no fabricated match"},
    {"capability": "wildcard_partition_semantics", "surface_id": "S1.topology",
     "status": "unsupported", "reason": "pattern semantics use no Python fnmatch substitute"},
    {"capability": "dds_wire_malformed_payload", "surface_id": "S1.topology",
     "status": "out_of_scope", "reason": "current execution boundary is public-API interoperability"},
)


def _seed_entry(surface_id, seed_id, document, intent, expected, provenance):
    return {
        "schema": "r2f.s1-seed-entry/v1",
        "seed_id": seed_id,
        "surface_id": surface_id,
        "status": "authored",
        "intent": intent,
        "expected_disposition": expected,
        "content_sha256": input_model.content_hash(document),
        "document": copy.deepcopy(document),
        "provenance": {
            "authored": True,
            "derived_from_execution": False,
            "source": provenance,
        },
        "execution_evidence": False,
        "candidate_eligible": False,
    }


def _topology_edge_program(kind):
    if kind == "empty_roles":
        return topology_model.generate_program(1, writers=0, readers=0)
    if kind == "writer_only":
        return topology_model.generate_program(2, writers=1, readers=0)
    if kind == "reader_only":
        return topology_model.generate_program(3, writers=0, readers=1)
    if kind == "multi_endpoint":
        return topology_model.generate_program(4, writers=4, readers=4)
    raise ValueError(f"unknown topology seed kind: {kind}")


def _complex_action_program():
    program = topology_model.generate_program(5, writers=2, readers=2)
    program["actions"].append({"action_id": "barrier_all", "op": "barrier",
                               "endpoint_id": None, "generation": None,
                               "params": {"condition": "all_created",
                                          "timeout_sec": 1.0,
                                          "endpoint_ids": [
                                              endpoint["logical_id"]
                                              for endpoint in program["endpoints"]]},
                               "depends_on": [action["action_id"]
                                              for action in program["actions"]
                                              if action["op"] == "create"]})
    program["actions"].append({"action_id": "barrier_matched", "op": "barrier",
                               "endpoint_id": None, "generation": None,
                               "params": {"condition": "matched", "timeout_sec": 1.0},
                               "depends_on": ["barrier_all"]})
    validation = topology_model.validate_program(program)
    if validation["status"] != "valid":
        raise ValueError("complex action seed violated the topology contract")
    return program


CONCURRENT_WRITERS_SEED = "topology.concurrent_writers"


def concurrent_writers_program():
    """Authored bounded 2W1R input, not evidence of overlapping DDS calls.

    Explicit matched barrier, sufficient reliable history, and a later sentinel
    from each writer make identity-bound ordering observations possible. The
    runtime, not this input, must prove individual publish API overlap.
    """
    program = topology_model.generate_program(7, writers=2, readers=1)
    for endpoint in program["endpoints"]:
        endpoint["qos"].update(reliability="reliable", history="keep_last", depth=128)
    creates = [a for a in program["actions"] if a["op"] == "create"]
    publishes = [a for a in program["actions"] if a["op"] == "publish_batch"]
    barrier = topology_model._action("matched_before_group", "barrier", params={
        "condition": "matched", "timeout_sec": 5.0,
        "endpoint_ids": [e["logical_id"] for e in program["endpoints"]]})
    for action in publishes:
        action["params"].update(count=64, start_sequence=0, interval_sec=0.0,
            payload="L02-public-api-" + "x" * 512,
            concurrency_group="L02-writers-1")
        action["depends_on"] = [barrier["action_id"]]
    sentinels = [topology_model._action("later_sentinel_" + a["endpoint_id"],
        "publish_batch", a["endpoint_id"], 1,
        {"count": 1, "start_sequence": 64, "interval_sec": 0.0,
         "payload": "later-same-writer-sentinel"}) for a in publishes]
    # Publish return and snapshot ACK are not DDS delivery completion. Await
    # the existing identity-bound delivery contract before closing the capture.
    delivered = topology_model._action("delivered_after_group", "barrier", params={
        "condition": "delivery", "timeout_sec": 5.0,
        "endpoint_ids": [e["logical_id"] for e in program["endpoints"]]})
    delivered["depends_on"] = [a["action_id"] for a in sentinels]
    program["actions"] = creates + [barrier] + publishes + sentinels + [delivered,
        topology_model._action("final", "snapshot", params={"label": "after-L02"})]
    if topology_model.validate_program(program)["status"] != "valid":
        raise ValueError("concurrent writer seed violated the topology contract")
    return program


CONTINUOUS_EPISODE_SEED = "topology.continuous_episode"


def continuous_episode_program():
    """Bounded 1W1R two-generation episode input; resets remain unproven."""
    program = topology_model.generate_program(7, writers=1, readers=1, budget={
        "max_writers": 1, "max_readers": 1, "max_endpoints": 2, "max_actions": 50,
        "max_batch_size": 3, "max_total_samples": 6, "max_timeout_sec": 5.0,
        "max_generations": 2})
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/r2f/topology/episode"
        endpoint["qos"].update(reliability="reliable", history="keep_last", depth=16)
    action = topology_model._action
    actions = []
    for generation in (1, 2):
        create = "create" if generation == 1 else "recreate"
        actions.extend([
            action(f"{create}_w{generation}", create, "w0", generation),
            action(f"{create}_r{generation}", create, "r0", generation),
            action(f"matched_{generation}", "barrier", params={
                "condition": "matched", "timeout_sec": 5.0, "endpoint_ids": ["w0", "r0"]}),
            action(f"publish_{generation}", "publish_batch", "w0", generation,
                   {"count": 3, "start_sequence": 0, "interval_sec": 0.01,
                    "payload": "continuous-episode"}),
            action(f"delivered_{generation}", "barrier", params={
                "condition": "delivery", "timeout_sec": 5.0, "endpoint_ids": ["w0", "r0"]}),
            action(f"checkpoint_{generation}", "snapshot", params={"label": f"episode-{generation}"}),
        ])
        if generation == 1:
            actions.extend([action("destroy_r1", "destroy", "r0", 1),
                            action("destroy_w1", "destroy", "w0", 1)])
    for previous, current in zip(actions, actions[1:]):
        current["depends_on"] = [previous["action_id"]]
    program["actions"] = actions
    program["episode"] = {
        "schema": topology_model.EPISODE_CONFIG_SCHEMA, "max_seconds": 120.0,
        "checkpoint_action_ids": ["checkpoint_1", "checkpoint_2"],
        "reset_action_ids": ["recreate_w2", "recreate_r2"],
    }
    if topology_model.validate_program(program)["status"] != "valid":
        raise ValueError("continuous episode seed violated the topology contract")
    return program


PARTICIPANT_REFRESH_EPISODE_SEED = "topology.participant_refresh_episode"


def participant_refresh_episode_program():
    """Normal opt-in input control, not an observer side effect or discovery."""
    program = continuous_episode_program()
    actions = []
    for item in program["actions"]:
        actions.append(item)
        if item["op"] == "barrier" and item["params"].get("condition") == "matched":
            generation = 1 if item["action_id"] == "matched_1" else 2
            actions.append(topology_model._action(
                f"participant_refresh_{generation}", "update_participant_userdata", "w0", generation,
                {"data": f"issue14-normal-participant-userdata-g{generation}"}))
    for previous, current in zip(actions, actions[1:]):
        current["depends_on"] = [previous["action_id"]]
    program["actions"] = actions
    if topology_model.validate_program(program)["status"] != "valid":
        raise ValueError("participant refresh seed violated topology contract")
    return program


def corpus_v2(*, include_catalog=True, seed=0, include_concurrent=False, include_cft_matrix=False,
              include_episode=False, include_participant_refresh=False, include_cft_lifecycle=False):
    """Offline seed corpus: legal, boundary, environment and guard seeds.

    The manifest separates ``authored`` entries from ``pending`` runtime
    capabilities and ``unsupported`` modelling gaps.  Nothing here is a real
    run, a mutation hit, or a candidate. ``include_concurrent`` is explicit
    opt-in, as is ``include_episode``; omitted preserves every legacy corpus
    and ingestion identity.
    """
    if type(include_episode) is not bool:
        raise ValueError("include_episode must be bool")
    if type(include_participant_refresh) is not bool:
        raise ValueError("include_participant_refresh must be bool")
    entries, manifest = [], []
    for kind in ("empty_roles", "writer_only", "reader_only", "multi_endpoint"):
        document = _topology_edge_program(kind)
        entry = _seed_entry("S1.topology", f"topology.{kind}", document, kind,
                            "expect_pass", f"topology_model.generate_program:{kind}")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
                         "status": "authored", "kind": kind,
                         "expected_disposition": "expect_pass",
                         "content_sha256": entry["content_sha256"]})
    complex_program = _complex_action_program()
    entry = _seed_entry("S1.topology", "topology.complex_actions", complex_program,
                        "complex_action_history", "expect_pass",
                        "topology_model.generate_program + barrier actions")
    entries.append(entry)
    manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
                     "status": "authored", "kind": "complex_actions",
                     "expected_disposition": "expect_pass",
                     "content_sha256": entry["content_sha256"]})

    if include_concurrent:
        entry = _seed_entry("S1.topology", CONCURRENT_WRITERS_SEED,
            concurrent_writers_program(), "concurrent_writer_calls_with_sentinels",
            "expect_pass", "authored deterministic 2W1R public API concurrency program")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
            "status": "authored", "kind": "concurrent_writers",
            "expected_disposition": "expect_pass", "content_sha256": entry["content_sha256"]})

    if include_episode:
        entry = _seed_entry("S1.topology", CONTINUOUS_EPISODE_SEED,
            continuous_episode_program(), "bounded_continuous_episode", "expect_pass",
            "authored deterministic 1W1R two-generation episode input; checkpoints/reset unverified")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
            "status": "authored", "kind": "continuous_episode",
            "expected_disposition": "expect_pass", "content_sha256": entry["content_sha256"]})

    if include_participant_refresh:
        entry = _seed_entry("S1.topology", PARTICIPANT_REFRESH_EPISODE_SEED,
            participant_refresh_episode_program(), "normal_participant_userdata_control", "expect_pass",
            "authored deterministic normal public Fast DDS participant user_data update; no natural candidate credit")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
            "status": "authored", "kind": "normal_participant_userdata_control",
            "expected_disposition": "expect_pass", "content_sha256": entry["content_sha256"]})

    if include_cft_matrix:
        from .topology_cft_matrix import CFT_MATRIX_SEED, cft_matrix_program
        entry = _seed_entry("S1.topology", CFT_MATRIX_SEED,
            cft_matrix_program(), "dynamic_cft_matrix", "expect_pass",
            "authored deterministic 1W2R complete String.data CFT matrix input")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
            "status": "authored", "kind": "dynamic_cft_matrix",
            "expected_disposition": "expect_pass", "content_sha256": entry["content_sha256"]})

    if include_cft_lifecycle:
        from .topology_cft_matrix import CFT_LIFECYCLE_SEED, cft_lifecycle_program
        entry = _seed_entry("S1.topology", CFT_LIFECYCLE_SEED,
            cft_lifecycle_program(), "cft_lifecycle", "expect_pass",
            "authored initial/update/disable/enable CFT input; actual effects require raw evidence")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
            "status": "authored", "kind": "cft_lifecycle",
            "expected_disposition": "expect_pass", "content_sha256": entry["content_sha256"]})

    # Environment / guard seeds are built through topology_model's own
    # deliberate guard mutations; they remain inputs, not execution attempts.
    guard_parent = _topology_edge_program("writer_only")
    for name, mutator, expected in (
            ("structure_duplicate", "structure_guard", "expect_guard_reject"),
            ("dependency_missing", "dependency_guard", "expect_guard_reject"),
            ("environment_fault", "environment_guard", "expect_guard_reject")):
        if mutator == "parameter_guard":
            continue
        result = topology_model.mutate_program(guard_parent, mutator, seed=7)
        document = result["program"]
        entry = _seed_entry("S1.topology", f"topology.{name}", document, name,
                            expected, f"topology_model.mutate_program:{mutator}")
        entries.append(entry)
        manifest.append({"seed_id": entry["seed_id"], "surface_id": "S1.topology",
                         "status": "authored", "kind": name,
                         "expected_disposition": expected,
                         "content_sha256": entry["content_sha256"]})

    # Delivery-plan seeds cover reorder/duplicate/drop/delay declarations.
    for name, edits in (
            ("delivery.baseline", {}),
            ("delivery.reorder", {"reorder": True}),
            ("delivery.duplicate", {"duplicate": True}),
            ("delivery.drop_middle", {"drop_sequence": 3}),
            ("delivery.delay", {"delay_sec": 0.05}),
            ("delivery.lease_expire", {"lease_action": "expire"})):
        document = delivery_plan_template()
        document["delivery_plan"].update(edits)
        entry = _seed_entry("S1.delivery", name, document, name, "unknown_not_pass",
                            "authored delivery plan template")
        entries.append(entry)
        manifest.append({"seed_id": name, "surface_id": "S1.delivery",
                         "status": "authored", "kind": name,
                         "expected_disposition": "unknown_not_pass",
                         "content_sha256": entry["content_sha256"]})

    if include_catalog:
        from . import fuzz_catalog
        catalogue = fuzz_catalog.load_seeds()
        for surface_id in catalogue["authored"]:
            document = catalogue["seeds"][surface_id]["logical_config"]
            entry = _seed_entry(surface_id, f"catalog.{surface_id}", document,
                                "catalog_authored", "expect_pass",
                                "config/s1_fuzz/seeds (read-only import)")
            entries.append(entry)
            manifest.append({"seed_id": entry["seed_id"], "surface_id": surface_id,
                             "status": "authored", "kind": "catalog_authored",
                             "expected_disposition": "expect_pass",
                             "content_sha256": entry["content_sha256"]})

    for item in PENDING_CAPABILITIES:
        manifest.append({"seed_id": None, "surface_id": item["surface_id"],
                         "status": "pending", "capability": item["capability"],
                         "reason": item["reason"], "content_sha256": None})
    for item in UNSUPPORTED_CAPABILITIES:
        manifest.append({"seed_id": None, "surface_id": item["surface_id"],
                         "status": item["status"], "capability": item["capability"],
                         "reason": item["reason"], "content_sha256": None})
    counts = {}
    for row in manifest:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    return {
        "schema": "r2f.s1-seed-corpus/v2",
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "entries": entries,
        "manifest": manifest,
        "counts": counts,
        "execution_evidence": False,
        "candidate_eligible": False,
    }


def corpus_manifest():
    """Manifest without documents, suitable for offline review."""
    corpus = corpus_v2()
    return {"schema": corpus["schema"], "counts": corpus["counts"],
            "manifest": corpus["manifest"],
            "execution_evidence": False, "candidate_eligible": False}
