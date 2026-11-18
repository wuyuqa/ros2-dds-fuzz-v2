"""S1 input-domain model (``r2f.s1-input-domain/v1``).

This module is deliberately declarative and offline.  A surface declares *what
an input may contain*, which operators may touch it, which runtime binding (if
any) consumes it, and what a generated case is expected to prove.  It never
opens a socket, imports rclpy, starts ROS, or claims an execution/hit.

The model is an adapter over the existing S1 vocabulary:

* ``r2f.modules.s1.fuzz_catalog.SurfaceSpec`` (F1/F2 parameter/environment/
  compatibility surfaces) is projected through :func:`domain_from_catalog`;
* ``r2f.modules.s1.topology_model`` programs are described by
  :func:`topology_surface`; structural/action/history mutations delegate to
  that model rather than re-implementing it;
* seed ``logical_config`` documents are accepted as plain dicts and checked
  against the declared slots.

Unknown fields are never silently discarded.  :func:`classify_input` reports
every leaf that is not covered by a slot or an explicitly declared supporting
field as ``unbound_fields`` and returns ``unknown_not_pass``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json

DOMAIN_SCHEMA = "r2f.s1-input-domain/v1"
DOMAIN_VERSION = 1

# Operator ids live here so a domain can be validated without importing the
# implementation module (which imports this module).
OPERATOR_IDS = frozenset({
    "param.boundary", "param.enum", "relation.pair", "timing.shift",
    "queue.depth", "lease.value", "filter.expr", "endpoint.count",
    "generation.action", "action.insert", "action.delete", "action.swap",
    "guard.environment", "guard.dependency", "guard.structure",
    "guard.parameter", "env.delay", "env.reorder", "env.duplicate",
    "env.drop", "env.resource", "env.health", "env.lease_expiry",
    "env.lease_refresh",
})

SLOT_KINDS = frozenset({
    "parameter_numeric", "parameter_enum", "parameter_timing",
    "parameter_queue", "parameter_lease", "filter", "relation",
    "compatibility", "environment", "structure", "action_history",
    "dependency", "endpoint_count", "generation",
})
VALUE_TYPES = frozenset({
    "int", "float", "bool", "str", "enum", "duration_sec", "duration_ns",
    "count", "policy", "pair_relation", "endpoint_list", "action_list",
    "environment", "dependency_list", "filter_expr", "generation",
})
GUARD_DISPOSITIONS = frozenset({
    "expect_pass", "expect_guard_reject", "expect_no_match", "unknown_not_pass",
})
BINDING_STATUSES = frozenset({
    "bound", "offline_model", "generation_only", "unbound", "unsupported",
})
MISSING_POLICIES = frozenset({"unknown_not_pass", "reject_not_pass", "not_applicable"})
UNSUPPORTED_POLICIES = frozenset({
    "reject_not_executable", "unsupported_not_pass", "generation_only_not_pass",
})


def canonical(value):
    """Same canonical JSON as ``r2f.core.evidence.canonical_hash``."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def content_hash(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _text_tuple(value, field, *, allow_empty=True):
    if not isinstance(value, (tuple, list)):
        raise ValueError(f"{field} must be a tuple/list of strings")
    result = tuple(value)
    if any(not isinstance(item, str) or not item for item in result):
        raise ValueError(f"{field} must contain non-empty strings")
    if not allow_empty and not result:
        raise ValueError(f"{field} must not be empty")
    return result


def _dict(value, field):
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a dict")
    return json.loads(json.dumps(value))


def _exact(data, fields, label):
    if not isinstance(data, dict):
        raise ValueError(f"{label} must be a dict")
    missing = sorted(set(fields) - set(data))
    extra = sorted(set(data) - set(fields))
    if missing:
        raise ValueError(f"{label} missing field(s): {','.join(missing)}")
    if extra:
        raise ValueError(f"{label} unknown field(s): {','.join(extra)}")
    return data


SLOT_FIELDS = (
    "slot_id", "kind", "input_path", "value_type", "unit", "domain",
    "special_values", "boundary_classes", "equivalence_classes",
    "operators", "guards", "guard_disposition", "binding", "audit_refs",
    "missing_policy", "unsupported_policy", "required",
)


@dataclass(frozen=True)
class SlotSpec:
    slot_id: str
    kind: str
    input_path: str
    value_type: str
    unit: str = ""
    domain: dict = field(default_factory=dict)
    special_values: dict = field(default_factory=dict)
    boundary_classes: tuple = ()
    equivalence_classes: tuple = ()
    operators: tuple = ()
    guards: tuple = ()
    guard_disposition: str = "unknown_not_pass"
    binding: dict = field(default_factory=dict)
    audit_refs: dict = field(default_factory=dict)
    missing_policy: str = "unknown_not_pass"
    unsupported_policy: str = "reject_not_executable"
    required: bool = False

    def __post_init__(self):
        object.__setattr__(self, "domain", _dict(self.domain, "slot.domain"))
        object.__setattr__(self, "special_values", _dict(self.special_values, "slot.special_values"))
        for name in ("boundary_classes", "equivalence_classes", "operators", "guards"):
            object.__setattr__(self, name, _text_tuple(getattr(self, name), f"slot.{name}"))
        object.__setattr__(self, "binding", _dict(self.binding, "slot.binding"))
        object.__setattr__(self, "audit_refs", _dict(self.audit_refs, "slot.audit_refs"))
        _text(self.slot_id, "slot.slot_id")
        _text(self.input_path, "slot.input_path")
        if self.kind not in SLOT_KINDS:
            raise ValueError(f"slot.kind unsupported: {self.kind}")
        if self.value_type not in VALUE_TYPES:
            raise ValueError(f"slot.value_type unsupported: {self.value_type}")
        if self.guard_disposition not in GUARD_DISPOSITIONS:
            raise ValueError(f"slot.guard_disposition unsupported: {self.guard_disposition}")
        if self.missing_policy not in MISSING_POLICIES:
            raise ValueError(f"slot.missing_policy unsupported: {self.missing_policy}")
        if self.unsupported_policy not in UNSUPPORTED_POLICIES:
            raise ValueError(f"slot.unsupported_policy unsupported: {self.unsupported_policy}")
        if type(self.required) is not bool:
            raise ValueError("slot.required must be boolean")
        if not self.domain:
            raise ValueError(f"{self.slot_id}: slot.domain must not be empty")
        unknown_ops = set(self.operators) - OPERATOR_IDS
        if unknown_ops:
            raise ValueError(f"{self.slot_id}: unknown operator(s): {','.join(sorted(unknown_ops))}")
        binding = self.binding
        if binding:
            status = binding.get("status")
            if status not in BINDING_STATUSES:
                raise ValueError(f"{self.slot_id}.binding.status unsupported: {status}")
            if status == "bound":
                _text(binding.get("runtime_binding"), f"{self.slot_id}.binding.runtime_binding")
            if status in {"unbound", "unsupported", "generation_only"}:
                _text(binding.get("reason"), f"{self.slot_id}.binding.reason")
        audit = self.audit_refs
        unknown_audit = set(audit) - {"fact_ids", "rule_ids", "effect_id", "oracle_ids"}
        if unknown_audit:
            raise ValueError(f"{self.slot_id}: unknown audit refs: {','.join(sorted(unknown_audit))}")
        for key in ("fact_ids", "rule_ids", "oracle_ids"):
            if key in audit:
                _text_tuple(audit[key], f"{self.slot_id}.audit_refs.{key}")
        if "effect_id" in audit:
            _text(audit["effect_id"], f"{self.slot_id}.audit_refs.effect_id")

    def to_dict(self):
        # Deep copies: to_dict must never expose the live declaration for
        # callers to mutate.
        data = {name: getattr(self, name) for name in SLOT_FIELDS}
        return json.loads(json.dumps(data, sort_keys=True, ensure_ascii=False))

    @classmethod
    def from_dict(cls, data):
        data = _exact(data, SLOT_FIELDS, "slot")
        return cls(**data)


GUARD_FIELDS = ("guard_id", "kind", "input_path", "expectation", "description", "binding")


@dataclass(frozen=True)
class GuardSpec:
    guard_id: str
    kind: str
    input_path: str
    expectation: str
    description: str = ""
    binding: dict = field(default_factory=dict)

    def __post_init__(self):
        _text(self.guard_id, "guard.guard_id")
        _text(self.input_path, "guard.input_path")
        if self.kind not in {"environment", "structure", "dependency", "history", "timing", "identity"}:
            raise ValueError(f"guard.kind unsupported: {self.kind}")
        if self.expectation not in GUARD_DISPOSITIONS:
            raise ValueError(f"guard.expectation unsupported: {self.expectation}")
        object.__setattr__(self, "binding", _dict(self.binding, "guard.binding"))
        if self.binding and self.binding.get("status") not in BINDING_STATUSES:
            raise ValueError("guard.binding.status unsupported")

    def to_dict(self):
        data = {name: getattr(self, name) for name in GUARD_FIELDS}
        return json.loads(json.dumps(data, sort_keys=True, ensure_ascii=False))

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(data, GUARD_FIELDS, "guard"))


COMBINATION_FIELDS = ("combination_id", "operators", "rationale", "status",
                      "depends_on", "conflicts_with", "reference_document",
                      "pending_reason")
COMBINATION_STATUSES = frozenset({"declared", "pending", "unsupported"})


@dataclass(frozen=True)
class CombinationSpec:
    """A declared multi-operator recipe inside one surface.

    ``status`` is an *input-space* declaration: ``declared`` means the recipe
    composes on this module's reference document, ``pending`` means it is
    declared but not yet demonstrably composable.  Neither is execution
    evidence.

    Omitted ids use the canonical SHA-256 of all validated declaration fields
    except ``combination_id``.  Field order in the JSON object is irrelevant;
    operator/dependency/conflict sequence order remains part of the declaration.
    Explicit ids (including ids in older serialized documents) are preserved.
    """
    operators: tuple
    rationale: str
    status: str = "declared"
    combination_id: str = ""
    depends_on: tuple = ()
    conflicts_with: tuple = ()
    reference_document: str = "authored_seed"
    pending_reason: str = ""

    def __post_init__(self):
        object.__setattr__(self, "operators", _text_tuple(self.operators, "combination.operators"))
        if not 2 <= len(self.operators) <= 4:
            raise ValueError("combination must contain 2..4 operators")
        if len(set(self.operators)) != len(self.operators):
            raise ValueError("combination operators must be distinct")
        unknown = set(self.operators) - OPERATOR_IDS
        if unknown:
            raise ValueError(f"combination unknown operators: {','.join(sorted(unknown))}")
        _text(self.rationale, "combination.rationale")
        if self.status not in COMBINATION_STATUSES:
            raise ValueError(f"combination.status unsupported: {self.status}")
        object.__setattr__(self, "depends_on", _text_tuple(self.depends_on, "combination.depends_on"))
        object.__setattr__(self, "conflicts_with", _text_tuple(
            self.conflicts_with, "combination.conflicts_with"))
        _text(self.reference_document, "combination.reference_document")
        if not isinstance(self.pending_reason, str):
            raise ValueError("combination.pending_reason must be text")
        if not self.combination_id:
            object.__setattr__(self, "combination_id",
                               "+".join(self.operators) + "@" + content_hash(self._declaration()))
        _text(self.combination_id, "combination.combination_id")
        if self.status == "pending" and not self.pending_reason:
            raise ValueError(
                f"{self.combination_id}: a pending combination must state a reason")

    def _declaration(self):
        """Content identity independent of an explicit or generated name."""
        return {name: getattr(self, name) for name in COMBINATION_FIELDS
                if name != "combination_id"}

    def to_dict(self):
        return {name: list(getattr(self, name)) if isinstance(getattr(self, name), tuple)
                else getattr(self, name) for name in COMBINATION_FIELDS}

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(data, COMBINATION_FIELDS, "combination"))


SURFACE_FIELDS = (
    "schema", "surface_id", "version", "layer", "kind", "target_refs",
    "slots", "guards", "combinations", "supporting_fields", "chain_max",
    "source", "notes",
)
SURFACE_KINDS = frozenset({
    "parameter", "environment", "structure", "action_history", "dependency",
    "relation", "compatibility", "mixed",
})


@dataclass(frozen=True)
class SurfaceDomain:
    surface_id: str
    version: int
    layer: str
    kind: str
    target_refs: tuple = ()
    slots: tuple = ()
    guards: tuple = ()
    combinations: tuple = ()
    supporting_fields: tuple = ()
    chain_max: int = 4
    source: str = "v2"
    notes: str = ""
    schema: str = DOMAIN_SCHEMA

    def __post_init__(self):
        if self.schema != DOMAIN_SCHEMA:
            raise ValueError(f"unsupported input-domain schema: {self.schema}")
        _text(self.surface_id, "surface.surface_id")
        if type(self.version) is not int or self.version < 1:
            raise ValueError("surface.version must be a positive integer")
        if not isinstance(self.layer, str) or not self.layer:
            raise ValueError("surface.layer must be non-empty")
        if self.kind not in SURFACE_KINDS:
            raise ValueError(f"surface.kind unsupported: {self.kind}")
        object.__setattr__(self, "target_refs", _text_tuple(self.target_refs, "surface.target_refs"))
        object.__setattr__(self, "supporting_fields", _text_tuple(
            self.supporting_fields, "surface.supporting_fields"))
        slots, guards, combinations = [], [], []
        for item in self.slots:
            slots.append(item if isinstance(item, SlotSpec) else SlotSpec.from_dict(item))
        for item in self.guards:
            guards.append(item if isinstance(item, GuardSpec) else GuardSpec.from_dict(item))
        for item in self.combinations:
            item = (item if isinstance(item, CombinationSpec)
                    else CombinationSpec.from_dict(item)
                    if set(item) == set(COMBINATION_FIELDS)
                    else CombinationSpec(**item))
            combinations.append(item)
        object.__setattr__(self, "slots", tuple(slots))
        object.__setattr__(self, "guards", tuple(guards))
        object.__setattr__(self, "combinations", tuple(combinations))
        if type(self.chain_max) is not int or not 1 <= self.chain_max <= 4:
            raise ValueError("surface.chain_max must be an integer in 1..4")
        _text(self.source, "surface.source")
        if not isinstance(self.notes, str):
            raise ValueError("surface.notes must be text")
        slot_ids = [slot.slot_id for slot in self.slots]
        if len(slot_ids) != len(set(slot_ids)):
            raise ValueError(f"{self.surface_id}: duplicate slot_id")
        guard_ids = [guard.guard_id for guard in self.guards]
        if len(guard_ids) != len(set(guard_ids)):
            raise ValueError(f"{self.surface_id}: duplicate guard_id")
        known_guards = set(guard_ids)
        for slot in self.slots:
            unknown = set(slot.guards) - known_guards
            if unknown:
                raise ValueError(f"{self.surface_id}.{slot.slot_id}: unknown guards {sorted(unknown)}")
        combination_ids = [combo.combination_id for combo in self.combinations]
        if len(combination_ids) != len(set(combination_ids)):
            raise ValueError(f"{self.surface_id}: duplicate combination_id")
        # Explicit names must not disguise repeated declarations either.
        declarations = [canonical(combo._declaration()) for combo in self.combinations]
        if len(declarations) != len(set(declarations)):
            raise ValueError(f"{self.surface_id}: duplicate combination declaration")
        declared_ops = {op for slot in self.slots for op in slot.operators}
        for combo in self.combinations:
            unknown = set(combo.operators) - declared_ops
            if unknown:
                raise ValueError(
                    f"{self.surface_id}: combination uses undeclared operators {sorted(unknown)}")

    @property
    def slot_by_id(self):
        return {slot.slot_id: slot for slot in self.slots}

    @property
    def operators(self):
        return frozenset(op for slot in self.slots for op in slot.operators)

    def to_dict(self):
        return {
            "schema": self.schema, "surface_id": self.surface_id,
            "version": self.version, "layer": self.layer, "kind": self.kind,
            "target_refs": list(self.target_refs),
            "slots": [slot.to_dict() for slot in self.slots],
            "guards": [guard.to_dict() for guard in self.guards],
            "combinations": [combo.to_dict() for combo in self.combinations],
            "supporting_fields": list(self.supporting_fields),
            "chain_max": self.chain_max, "source": self.source, "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data):
        data = _exact(data, SURFACE_FIELDS, "surface")
        return cls(**data)


@dataclass(frozen=True)
class DomainCatalog:
    surfaces: tuple
    schema: str = DOMAIN_SCHEMA
    version: int = DOMAIN_VERSION

    def __post_init__(self):
        if self.schema != DOMAIN_SCHEMA:
            raise ValueError(f"unsupported domain catalog schema: {self.schema}")
        if type(self.version) is not int or self.version < 1:
            raise ValueError("domain catalog version must be positive")
        surfaces = []
        for item in self.surfaces:
            surfaces.append(item if isinstance(item, SurfaceDomain) else SurfaceDomain.from_dict(item))
        object.__setattr__(self, "surfaces", tuple(surfaces))
        ids = [surface.surface_id for surface in self.surfaces]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate surface_id in domain catalog")

    @property
    def by_id(self):
        return {surface.surface_id: surface for surface in self.surfaces}

    def get(self, surface_id):
        surface = self.by_id.get(surface_id)
        if surface is None:
            raise KeyError(f"unknown input-domain surface: {surface_id}")
        return surface

    def digest(self):
        return content_hash({
            "schema": self.schema, "version": self.version,
            "surfaces": [surface.to_dict() for surface in sorted(
                self.surfaces, key=lambda item: item.surface_id)],
        })

    def manifest(self):
        """Describe declarations; oracle counts are references, not resolved code."""
        return {
            "schema": self.schema,
            "version": self.version,
            "surface_ids": sorted(self.by_id),
            "surface_count": len(self.surfaces),
            "slot_count": sum(len(surface.slots) for surface in self.surfaces),
            "operator_ids": sorted({op for surface in self.surfaces for op in surface.operators}),
            "domain_sha256": self.digest(),
            "oracle_count": len({oid for surface in self.surfaces
                                 for slot in surface.slots
                                 for oid in slot.audit_refs.get("oracle_ids", [])}),
            "oracle_audit": {"kind": "bindings_only",
                             "binding_resolution": "unresolved_no_resolver",
                             "independent_verdict": False},
            "execution_evidence": False,
            "candidate_eligible": False,
        }

    def validate(self):
        for surface in self.surfaces:
            SurfaceDomain.from_dict(surface.to_dict())
        return True

    @classmethod
    def from_dict(cls, data):
        data = _exact(data, {"schema", "version", "surfaces"}, "domain_catalog")
        return cls(surfaces=tuple(data["surfaces"]), schema=data["schema"],
                   version=data["version"])

    def to_dict(self):
        return {"schema": self.schema, "version": self.version,
                "surfaces": [surface.to_dict() for surface in self.surfaces]}


# ---------------------------------------------------------------------------
# Catalog projection (F1/F2)
# ---------------------------------------------------------------------------

_CATALOG_KIND = {
    "late_reader_delay": "parameter_timing",
    "projection_reliability": "parameter_enum",
    "projection_durability": "parameter_enum",
    "projection_deadline": "parameter_timing",
    "projection_liveliness": "parameter_enum",
    "projection_liveliness_lease_duration": "parameter_lease",
    "discovery_drop_duration": "environment",
    "discovery_multiwindow_restore_interval": "parameter_timing",
    "discovery_anchor_offset": "parameter_timing",
    "history_depth_and_sample_count": "parameter_queue",
    "publish_interval": "parameter_timing",
    "offered_requested_reliability_compatibility": "compatibility",
}

_CATALOG_VALUE_TYPE = {
    "parameter_timing": "duration_sec",
    "parameter_lease": "duration_ns",
    "parameter_queue": "count",
    "parameter_enum": "enum",
    "compatibility": "pair_relation",
    "environment": "duration_sec",
}

_CATALOG_OPERATORS = {
    "parameter_timing": ("param.boundary", "timing.shift", "env.delay"),
    "parameter_lease": ("param.boundary", "lease.value", "env.lease_expiry", "env.lease_refresh"),
    "parameter_queue": ("param.boundary", "queue.depth", "relation.pair"),
    "parameter_enum": ("param.enum",),
    "compatibility": ("relation.pair",),
    "environment": ("env.drop", "timing.shift", "param.boundary"),
}

#: Logical-config fields that are declared supporting metadata.  They are
#: accepted but intentionally not mutated by this module.  A field outside this
#: set and outside the declared slots is reported, never dropped.
_SUPPORTING_FIELDS = (
    "topic", "message_type", "qos", "pre_match_count", "post_match_count",
    "publish_interval_sec", "match_timeout_sec", "delivery_timeout_sec",
    "startup_timeout_sec", "reader_delay_sec", "baseline_reader_delay_sec",
    "drop_duration_sec", "recovery_timeout_sec", "counter_interval_sec",
    "environment_windows", "window_count_domain", "restore_interval_domain_s",
    "anchor_domain", "anchor_offset_domain_s", "special_value_classes",
    "writer_qos", "reader_qos", "pair_relation", "workload_relation",
)


def _numeric_domain(source):
    if "sampled_range_s" in source:
        low, high = source["sampled_range_s"]
        return {"min": low, "max": high, "step": source.get("quantum", 0.001)}
    if "sampled_range_ns" in source:
        low, high = source["sampled_range_ns"]
        return {"min": low, "max": high, "step": 1}
    if "finite_representatives" in source:
        values = [value for value in source["finite_representatives"]
                  if type(value) in (int, float)]
        if values:
            return {"min": min(values), "max": max(values), "step": 1,
                    "representatives": list(values)}
    if "representatives" in source:
        values = [value for value in source["representatives"] if type(value) in (int, float)]
        if values:
            return {"min": min(values), "max": max(values),
                    "step": source.get("quantum", 1),
                    "representatives": list(values)}
    if "depth" in source and isinstance(source["depth"], list) and source["depth"]:
        return {"min": min(source["depth"]), "max": max(source["depth"]), "step": 1,
                "representatives": list(source["depth"])}
    for key in ("window_counts", "restore_interval_representatives_s", "offsets_s"):
        if key in source and isinstance(source[key], list) and source[key]:
            values = [value for value in source[key] if type(value) in (int, float)]
            if values:
                return {"min": min(values), "max": max(values),
                        "step": 1 if key == "window_counts" else 0.001,
                        "representatives": values}
    return dict(source)


def _catalog_binding(surface):
    runtime_binding = surface.runtime_binding
    if runtime_binding:
        from .fuzz_catalog import RUNTIME_BINDINGS
        binding = RUNTIME_BINDINGS.get(runtime_binding)
        status = "bound" if binding else "generation_only"
        result = {
            "runtime_binding": runtime_binding,
            "runtime_field": binding.get("runtime_field") if binding else None,
            "phase": binding.get("phase") if binding else surface.runtime_phase,
            "status": status,
            "adapter": "r2f.modules.s1.fuzz_catalog",
        }
        if status != "bound":
            result["reason"] = "catalog binding is declared but not present in RUNTIME_BINDINGS"
        return result
    return {"runtime_binding": None, "status": "generation_only",
            "reason": "catalog surface has no runtime_binding"}


def _slot_kind_for_path(base_kind, path):
    tail = path.split(".")[-1]
    if tail in {"deadline_ns", "liveliness_lease_duration_ns"}:
        return "parameter_lease"
    if "reliability" in path or "durability" in path or tail == "liveliness" or tail == "anchor":
        return "parameter_enum"
    if tail in {"count", "depth"} or "count" in tail:
        return "parameter_queue"
    if any(token in tail for token in ("duration", "delay", "interval", "offset", "deadline")):
        return "parameter_timing"
    return base_kind


def _slot_domain_for_path(surface, path):
    domain = _numeric_domain(surface.domain)
    tail = path.split(".")[-1]
    if "window_counts" in surface.domain and "count" in tail:
        values = list(surface.domain["window_counts"])
        return {"min": min(values), "max": max(values), "step": 1,
                "representatives": values}
    if "restore_interval_representatives_s" in surface.domain and "restore_interval" in tail:
        values = list(surface.domain["restore_interval_representatives_s"])
        return {"min": min(values), "max": max(values), "step": 0.001,
                "representatives": values}
    if "offsets_s" in surface.domain and "offset" in tail:
        values = list(surface.domain["offsets_s"])
        return {"min": min(values), "max": max(values), "step": 0.001,
                "representatives": values}
    if tail == "anchor" and "anchors" in surface.domain:
        return {"values": list(surface.domain["anchors"])}
    if "liveliness_lease_duration" in path:
        return domain
    if "reliability" in path and "values" in surface.domain:
        return {"values": list(surface.domain["values"])}
    if "reliability" in path and "pairs" in surface.domain:
        # G01 declares offered/requested pairs rather than a flat enum list.
        # The policy values themselves are the DDS reliability enum.
        return {"values": ["best_effort", "reliable"],
                "pairs": list(surface.domain.get("pairs", [])),
                "incompatible_pairs": list(surface.domain.get("incompatible_pairs", []))}
    if "durability" in path and "values" in surface.domain:
        return {"values": list(surface.domain["values"])}
    if "liveliness" in path and "values" in surface.domain:
        return {"values": list(surface.domain["values"])}
    return domain


def _catalog_slot(surface, path, base_kind):
    kind = _slot_kind_for_path(base_kind, path)
    domain = _slot_domain_for_path(surface, path)
    allowed_numeric = {"min", "max", "zero", "one", "finite", "infinite",
                       "neighbor", "quantized_neighbor", "out_of_range_low",
                       "out_of_range_high", "min_plus_one", "max_minus_one"}
    boundary = [name for name in surface.boundary_classes if name in allowed_numeric]
    if kind in {"parameter_timing", "parameter_lease", "parameter_queue", "parameter_numeric"}:
        for extra in ("zero", "one", "neighbor", "out_of_range_low", "out_of_range_high"):
            if extra not in boundary:
                boundary.append(extra)
    if not boundary:
        boundary = ["min", "max"]
    operators = _CATALOG_OPERATORS.get(base_kind, ())
    if kind == "parameter_enum":
        operators = ("param.enum",)
        if base_kind == "compatibility":
            operators = ("param.enum", "relation.pair")
    elif kind == "parameter_queue":
        operators = ("queue.depth", "param.boundary", "guard.parameter")
        # The depth/pre/post relation classes are declared by this surface
        # only; no other catalog surface declares relation witnesses.
        if surface.kind == "history_depth_and_sample_count":
            operators = ("queue.depth", "param.boundary", "relation.pair",
                         "guard.parameter")
    elif kind == "parameter_lease":
        operators = ("param.boundary", "lease.value", "env.lease_expiry", "env.lease_refresh")
    elif kind == "parameter_timing":
        operators = ("param.boundary", "timing.shift", "env.delay")
        if "drop" in surface.kind:
            operators = operators + ("env.drop",)
    value_type = _CATALOG_VALUE_TYPE.get(kind, "str")
    if kind == "parameter_queue":
        value_type = "count"
    elif kind == "parameter_enum":
        value_type = "enum"
    elif kind == "parameter_lease":
        value_type = "duration_ns"
    return SlotSpec(
        slot_id=f"{surface.surface_id}.{path}",
        kind=kind,
        input_path=path,
        value_type=value_type,
        unit=surface.unit,
        domain=domain,
        special_values=dict(surface.special_values),
        boundary_classes=tuple(boundary),
        equivalence_classes=tuple(surface.boundary_classes),
        operators=tuple(operators),
        guards=tuple(surface.sut_guards),
        guard_disposition="unknown_not_pass",
        binding=_catalog_binding(surface),
        audit_refs={"fact_ids": list(surface.fact_ids),
                    "rule_ids": list(surface.rule_ids),
                    "effect_id": surface.effect_id},
        missing_policy="unknown_not_pass",
        unsupported_policy="reject_not_executable",
    )


def domain_from_catalog(surfaces=None):
    """Project existing catalog surfaces; does not modify the catalog."""
    if surfaces is None:
        from . import fuzz_catalog
        surfaces = fuzz_catalog.SURFACES
    results = []
    for surface_id in sorted(surfaces):
        surface = surfaces[surface_id]
        base_kind = _CATALOG_KIND.get(surface.kind)
        if base_kind is None:
            raise ValueError(f"catalog surface {surface_id}: unknown kind {surface.kind}")
        paths = list(surface.input_paths) or ["input"]
        slots = tuple(_catalog_slot(surface, path, base_kind) for path in paths)
        guards = tuple(GuardSpec(
            guard_id=name, kind="environment" if "environment" in name or "ready" in name else "structure",
            input_path="environment." + name if name in {
                "backend_available", "transport_ready", "discovery_enabled"} else "input",
            expectation="unknown_not_pass",
            description="declared SUT guard; not measured by this offline module",
            binding={"status": "unbound", "reason": "guard observation requires runtime evidence"},
        ) for name in surface.sut_guards)
        results.append(SurfaceDomain(
            surface_id=surface_id, version=surface.version, layer=surface.layer,
            kind={"parameter_enum": "parameter", "parameter_timing": "parameter",
                  "parameter_lease": "parameter", "parameter_queue": "parameter",
                  "compatibility": "compatibility", "environment": "environment"}[base_kind],
            target_refs=(f"surface:{surface_id}",),
            slots=slots, guards=guards,
            combinations=(),
            supporting_fields=_SUPPORTING_FIELDS,
            chain_max=4, source="catalog",
            notes="projected from fuzz_catalog; generation is not execution",
        ))
    return DomainCatalog(tuple(results))


# ---------------------------------------------------------------------------
# Topology and v2 extension surfaces
# ---------------------------------------------------------------------------

_TOPOLOGY_SLOTS = (
    SlotSpec(
        slot_id="S1.topology.endpoints", kind="structure", input_path="endpoints",
        value_type="endpoint_list", unit="count",
        domain={"min": 0, "max": 8},
        boundary_classes=("empty", "single", "multi", "budget_max"),
        equivalence_classes=("empty_roles", "writer_only", "reader_only", "mixed"),
        operators=("endpoint.count", "action.insert"),
        guards=("topology_budget",), guard_disposition="expect_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints", "phase": "compile_time",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
        audit_refs={"rule_ids": ["r2f.s1-topology-program/v1"],
                    "oracle_ids": ["r2f.oracle.topology.structural_validity.v1",
                                   "r2f.oracle.topology.pair_graph.v1"]},
    ),
    SlotSpec(
        slot_id="S1.topology.actions", kind="action_history", input_path="actions",
        value_type="action_list", unit="count",
        domain={"min": 0, "max": 512},
        boundary_classes=("empty", "single", "complex", "budget_max"),
        equivalence_classes=("create", "recreate", "publish", "snapshot", "barrier"),
        operators=("action.insert", "action.delete", "action.swap", "generation.action",
                   "guard.dependency", "guard.structure", "guard.parameter"),
        guards=("topology_structure",), guard_disposition="expect_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "actions", "phase": "compile_time",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
        audit_refs={"rule_ids": ["r2f.s1-topology-program/v1"],
                    "oracle_ids": ["r2f.oracle.topology.action_semantics.v1",
                                   "r2f.oracle.topology.delivery_closure.v1"]},
    ),
    SlotSpec(
        slot_id="S1.topology.environment", kind="environment", input_path="environment",
        value_type="environment", unit="flags",
        domain={"keys": ["backend_available", "transport_ready", "discovery_enabled",
                         "type_matching", "filter_support", "filter_validity"]},
        boundary_classes=("all_true", "fault_precondition", "resource_unavailable", "health_unknown"),
        equivalence_classes=("healthy", "faulted", "unknown"),
        operators=("guard.environment", "env.resource", "env.health"),
        guards=("backend_available", "transport_ready", "discovery_enabled"),
        guard_disposition="expect_guard_reject",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "environment", "phase": "compile_time",
                 "status": "offline_model", "reason": "declared assumptions only"},
        audit_refs={"rule_ids": ["r2f.s1-topology-program/v1"],
                    "oracle_ids": ["r2f.oracle.topology.environment_guard.v1"]},
    ),
    SlotSpec(
        slot_id="S1.topology.dependency", kind="dependency", input_path="actions[*].depends_on",
        value_type="dependency_list", unit="action_id",
        domain={"min": 0, "max": 16},
        boundary_classes=("empty", "valid", "missing", "cycle"),
        equivalence_classes=("satisfied", "guard_violation"),
        operators=("guard.dependency", "action.delete", "action.swap"),
        guards=("topology_structure",), guard_disposition="expect_guard_reject",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "actions.depends_on", "phase": "compile_time",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
    ),
    SlotSpec(
        slot_id="S1.topology.qos.depth", kind="parameter_queue",
        input_path="endpoints[*].qos.depth", value_type="count", unit="samples",
        domain={"min": 1, "max": 65536, "step": 1},
        boundary_classes=("min", "one", "max", "zero", "neighbor", "out_of_range_high"),
        equivalence_classes=("one", "small", "large", "invalid_zero"),
        operators=("queue.depth", "param.boundary"),
        guards=("keep_last_requires_depth",), guard_disposition="expect_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints.qos.depth", "phase": "endpoint_create",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
        audit_refs={"rule_ids": ["https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Quality-of-Service-Settings.html"]},
    ),
    SlotSpec(
        slot_id="S1.topology.qos.reliability", kind="parameter_enum",
        input_path="endpoints[*].qos.reliability", value_type="enum", unit="policy",
        domain={"values": ["best_effort", "reliable"]},
        boundary_classes=("enum_exhaustive",),
        equivalence_classes=("best_effort", "reliable"),
        operators=("param.enum", "relation.pair"),
        guards=("requested_offered",), guard_disposition="expect_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints.qos.reliability", "phase": "endpoint_create",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
    ),
    SlotSpec(
        slot_id="S1.topology.qos.deadline", kind="parameter_timing",
        input_path="endpoints[*].qos.deadline_sec", value_type="duration_sec", unit="s",
        domain={"min": 0, "max": 86400, "step": 0.001, "special": ["infinite"]},
        special_values={"infinite": "infinite", "zero": 0},
        boundary_classes=("zero", "min", "max", "infinite", "neighbor"),
        equivalence_classes=("zero", "finite", "infinite"),
        operators=("timing.shift", "env.delay", "param.boundary", "lease.value"),
        guards=("deadline_requested_offered",), guard_disposition="expect_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints.qos.deadline_sec", "phase": "endpoint_create",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
    ),
    SlotSpec(
        slot_id="S1.topology.qos.lease", kind="parameter_lease",
        input_path="endpoints[*].qos.lease_duration_sec", value_type="duration_sec", unit="s",
        domain={"min": 0, "max": 86400, "step": 0.001, "special": ["infinite"]},
        special_values={"infinite": "infinite", "zero": 0},
        boundary_classes=("zero", "min", "max", "infinite", "neighbor"),
        equivalence_classes=("zero", "finite", "infinite"),
        operators=("lease.value", "env.lease_expiry", "env.lease_refresh", "param.boundary"),
        guards=("liveliness_lease_config_only",), guard_disposition="unknown_not_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints.qos.lease_duration_sec", "phase": "endpoint_create",
                 "status": "generation_only",
                 "reason": "configuration is modelled; lease expiry/refresh lifecycle is not wired"},
    ),
    SlotSpec(
        slot_id="S1.topology.qos.liveliness", kind="parameter_enum",
        input_path="endpoints[*].qos.liveliness", value_type="enum", unit="policy",
        domain={"values": ["automatic", "manual_by_participant", "manual_by_topic"]},
        boundary_classes=("enum_exhaustive",),
        equivalence_classes=("automatic", "manual_by_participant", "manual_by_topic"),
        operators=("param.enum", "env.lease_refresh"),
        guards=("public_writer_manual_by_topic",), guard_disposition="expect_guard_reject",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints.qos.liveliness", "phase": "endpoint_create",
                 "status": "offline_model", "reason": "offline model only; no ROS execution"},
    ),
    SlotSpec(
        slot_id="S1.topology.filter", kind="filter",
        input_path="endpoints[*].filter.expression", value_type="filter_expr", unit="text",
        domain={"max_length": 1024, "parameters_max": 16},
        boundary_classes=("empty", "valid", "invalid_oversize", "missing"),
        equivalence_classes=("supported", "unsupported"),
        operators=("filter.expr",), guards=("filter_support", "filter_validity"),
        guard_disposition="expect_guard_reject",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "endpoints.filter.expression", "phase": "endpoint_create",
                 "status": "generation_only",
                 "reason": "content-filter evaluation is not modelled as match evidence"},
        audit_refs={"rule_ids": ["https://fast-dds.docs.eprosima.com/en/2.14.x/fastdds/dds_layer/topic/contentFilteredTopic/createContentFilteredTopic.html"]},
    ),
    SlotSpec(
        slot_id="S1.topology.generation", kind="generation",
        input_path="actions[*].generation", value_type="generation", unit="count",
        domain={"min": 1, "max": 16, "step": 1},
        boundary_classes=("first", "next", "budget_max", "stale", "skipped"),
        equivalence_classes=("valid_increment", "guard_violation"),
        operators=("generation.action",), guards=("next_generation", "previous_generation_exists"),
        guard_disposition="expect_pass",
        binding={"runtime_binding": "r2f.modules.s1.topology_model.validate_program",
                 "runtime_field": "actions.generation", "phase": "runtime_window",
                 "status": "offline_model", "reason": "logical generation only; runtime identity is separate"},
    ),
)


def topology_surface():
    return SurfaceDomain(
        surface_id="S1.topology", version=1, layer="W", kind="mixed",
        target_refs=("r2f.s1-topology-program/v1",),
        slots=_TOPOLOGY_SLOTS,
        guards=(
            GuardSpec("topology_budget", "structure", "budget", "expect_pass",
                      "bounded endpoint/action/sample budget", {"status": "offline_model"}),
            GuardSpec("topology_structure", "structure", "actions", "expect_guard_reject",
                      "duplicate ids / unknown references are deliberate guard negatives",
                      {"status": "offline_model"}),
            GuardSpec("backend_available", "environment", "environment.backend_available",
                      "expect_guard_reject", "create/recreate requires backend available",
                      {"status": "offline_model"}),
            GuardSpec("transport_ready", "environment", "environment.transport_ready",
                      "unknown_not_pass", "transport health is a declared assumption, not measured",
                      {"status": "unbound", "reason": "runtime observation required"}),
            GuardSpec("discovery_enabled", "environment", "environment.discovery_enabled",
                      "unknown_not_pass", "discovery health is a declared assumption, not measured",
                      {"status": "unbound", "reason": "runtime observation required"}),
            GuardSpec("delivery_obligation_reviewed", "history", "workload_relation",
                      "unknown_not_pass", "delivery obligation needs a runtime oracle",
                      {"status": "unbound", "reason": "runtime observation required"}),
            GuardSpec("keep_last_requires_depth", "structure", "endpoints[*].qos.depth",
                      "expect_guard_reject", "keep_last requires explicit positive depth",
                      {"status": "offline_model"}),
            GuardSpec("requested_offered", "structure", "endpoints[*].qos.reliability",
                      "expect_no_match", "incompatible requested/offered QoS is a no-match expectation",
                      {"status": "offline_model"}),
            GuardSpec("deadline_requested_offered", "structure", "endpoints[*].qos.deadline_sec",
                      "expect_no_match", "requested deadline stricter than offered is a no-match expectation",
                      {"status": "offline_model"}),
            GuardSpec("liveliness_lease_config_only", "timing", "endpoints[*].qos.lease_duration_sec",
                      "unknown_not_pass", "lease configuration does not prove expiry/refresh lifecycle",
                      {"status": "generation_only", "reason": "lease lifecycle runner not wired"}),
            GuardSpec("public_writer_manual_by_topic", "structure", "endpoints[*].qos.liveliness",
                      "expect_guard_reject", "assert_liveliness requires manual_by_topic writer",
                      {"status": "offline_model"}),
            GuardSpec("filter_support", "environment", "environment.filter_support",
                      "unknown_not_pass", "filter support is a declared assumption",
                      {"status": "unbound", "reason": "runtime observation required"}),
            GuardSpec("filter_validity", "environment", "environment.filter_validity",
                      "unknown_not_pass", "filter validity is a declared assumption",
                      {"status": "unbound", "reason": "runtime observation required"}),
            GuardSpec("next_generation", "history", "actions[*].generation",
                      "expect_guard_reject", "recreate must increment generation",
                      {"status": "offline_model"}),
            GuardSpec("previous_generation_exists", "history", "actions[*].generation",
                      "expect_guard_reject", "recreate requires a live previous generation",
                      {"status": "offline_model"}),
        ),
        combinations=(
            CombinationSpec(("queue.depth", "param.enum"),
                            "queue boundary followed by a QoS policy change"),
            CombinationSpec(("timing.shift", "env.delay"),
                            "timing offset then a delay window"),
            CombinationSpec(("action.insert", "action.swap"),
                            "insert an observation action then reorder independent actions"),
            CombinationSpec(("guard.environment", "action.insert"),
                            "fault precondition then an action that must be safely rejected"),
        ),
        supporting_fields=("budget", "schema", "logical_config"),
        chain_max=4, source="topology_model",
        notes="offline policy model; positive graph edge is not discovery/delivery evidence",
    )


def delivery_surface():
    return SurfaceDomain(
        surface_id="S1.delivery", version=1, layer="E", kind="environment",
        target_refs=("discovery.late_join_delivery",),
        slots=(
            SlotSpec(
                slot_id="S1.delivery.reorder", kind="environment",
                input_path="delivery_plan.reorder", value_type="bool", unit="flag",
                domain={"values": [False, True]},
                boundary_classes=("false", "true"), equivalence_classes=("ordered", "reordered"),
                operators=("env.reorder",), guards=("delivery_order_reviewed",),
                guard_disposition="unknown_not_pass",
                binding={"runtime_binding": "delivery.reorder_plan", "status": "unbound",
                         "reason": "no runtime consumer for reorder; input is declared only"},
                audit_refs={"effect_id": "s1.late_join.delivery",
                    "oracle_ids": ["r2f.oracle.delivery.order.v1"]},
            ),
            SlotSpec(
                slot_id="S1.delivery.duplicate", kind="environment",
                input_path="delivery_plan.duplicate", value_type="bool", unit="flag",
                domain={"values": [False, True]},
                boundary_classes=("false", "true"), equivalence_classes=("single", "duplicate"),
                operators=("env.duplicate",), guards=("delivery_duplicate_reviewed",),
                guard_disposition="unknown_not_pass",
                binding={"runtime_binding": "delivery.duplicate_plan", "status": "unbound",
                         "reason": "no runtime consumer for duplication; input is declared only"},
                audit_refs={"effect_id": "s1.late_join.delivery",
                    "oracle_ids": ["r2f.oracle.delivery.duplicate.v1"]},
            ),
            SlotSpec(
                slot_id="S1.delivery.delay", kind="environment",
                input_path="delivery_plan.delay_sec", value_type="duration_sec", unit="s",
                domain={"min": 0, "max": 30, "step": 0.001},
                boundary_classes=("zero", "min", "max", "neighbor", "out_of_range_high"),
                equivalence_classes=("zero", "finite", "over_max"),
                operators=("env.delay",), guards=("delivery_timing_reviewed",),
                guard_disposition="unknown_not_pass",
                binding={"runtime_binding": "delivery.delay_plan", "status": "unbound",
                         "reason": "no runtime consumer for delayed delivery; input is declared only"},
                audit_refs={"effect_id": "s1.late_join.delivery",
                    "oracle_ids": ["r2f.oracle.delivery.timing.v1"]},
            ),
            SlotSpec(
                slot_id="S1.delivery.lease", kind="parameter_enum",
                input_path="delivery_plan.lease_action", value_type="enum", unit="policy",
                domain={"values": ["none", "expire", "refresh"]},
                boundary_classes=("enum_exhaustive",),
                equivalence_classes=("none", "expire", "refresh"),
                operators=("env.lease_expiry", "env.lease_refresh"),
                guards=("lease_lifecycle_reviewed",), guard_disposition="unknown_not_pass",
                binding={"runtime_binding": "delivery.lease_lifecycle", "status": "generation_only",
                         "reason": "lease lifecycle is modelled; no runtime consumer is wired"},
                audit_refs={"effect_id": "s1.late_join.delivery",
                    "oracle_ids": ["r2f.oracle.delivery.lease.v1"]},
            ),
            SlotSpec(
                slot_id="S1.delivery.drop", kind="environment",
                input_path="delivery_plan.drop_sequence", value_type="count", unit="sequence",
                domain={"min": 0, "max": 1024, "step": 1},
                boundary_classes=("none", "first", "middle", "last", "out_of_range"),
                equivalence_classes=("none", "middle_drop", "edge_drop"),
                operators=("env.drop", "env.delay"), guards=("delivery_closure",),
                guard_disposition="unknown_not_pass",
                binding={"runtime_binding": "delivery.drop_plan", "status": "unbound",
                         "reason": "no runtime consumer for post-match drop; input is declared only"},
                audit_refs={"effect_id": "s1.late_join.delivery",
                    "oracle_ids": ["r2f.oracle.delivery.closure.v1"]},
            ),
        ),
        guards=(
            GuardSpec("delivery_order_reviewed", "history", "delivery_plan.reorder",
                      "unknown_not_pass", "delivery order must be reviewed against the oracle",
                      {"status": "unbound", "reason": "no runtime consumer"}),
            GuardSpec("delivery_duplicate_reviewed", "history", "delivery_plan.duplicate",
                      "unknown_not_pass", "duplicate delivery must be reviewed against the oracle",
                      {"status": "unbound", "reason": "no runtime consumer"}),
            GuardSpec("delivery_closure", "history", "delivery_plan.drop_sequence",
                      "unknown_not_pass", "finite-window closure must be observed before judging loss",
                      {"status": "unbound", "reason": "no runtime consumer"}),
            GuardSpec("delivery_timing_reviewed", "timing", "delivery_plan.delay_sec",
                      "unknown_not_pass", "declared delay needs a runtime timing oracle",
                      {"status": "unbound", "reason": "no runtime consumer"}),
            GuardSpec("lease_lifecycle_reviewed", "timing", "delivery_plan.lease_action",
                      "unknown_not_pass", "lease expiry/refresh is not proven by configuration",
                      {"status": "generation_only", "reason": "lease lifecycle runner not wired"}),
        ),
        combinations=(
            CombinationSpec(("env.reorder", "env.duplicate"),
                            "declared reorder plus duplicate plan", status="pending",
                            pending_reason="no runtime consumer for post-match reorder/duplicate; "
                                           "input composition only, not execution evidence"),
            CombinationSpec(("env.drop", "env.delay"),
                            "declared post-match drop plus delay plan", status="pending",
                            pending_reason="no runtime consumer for post-match drop/delay; "
                                           "input composition only, not execution evidence"),
        ),
        supporting_fields=("topic", "message_type", "qos"),
        chain_max=4, source="v2",
        notes="delivery-plan fields are declared inputs; no runtime binding is claimed",
    )


def default_domain():
    """Catalog projection plus topology and delivery-plan extension surfaces."""
    catalog = domain_from_catalog()
    surfaces = list(catalog.surfaces) + [topology_surface(), delivery_surface()]
    return DomainCatalog(tuple(surfaces))


# ---------------------------------------------------------------------------
# Input classification
# ---------------------------------------------------------------------------

def _path_tokens(path):
    import re
    tokens = []
    for raw in re.findall(r"[^.\[\]]+|\[\d+\]|\[\*\]", path):
        if raw.startswith("["):
            tokens.append(("index", raw[1:-1]))
        else:
            tokens.append(("key", raw))
    return tokens


def _get_path(document, path):
    current = document
    for kind, token in _path_tokens(path):
        if kind == "key":
            if not isinstance(current, dict) or token not in current:
                raise KeyError(path)
            current = current[token]
        else:
            if token == "*" or not isinstance(current, list) or not token.isdigit():
                raise KeyError(path)
            index = int(token)
            if index >= len(current):
                raise KeyError(path)
            current = current[index]
    return current


def _set_path(document, path, value):
    tokens = _path_tokens(path)
    if not tokens:
        raise ValueError("empty input path")
    current = document
    for index, (kind, token) in enumerate(tokens[:-1]):
        next_kind = tokens[index + 1][0]
        if kind == "key":
            if token not in current or not isinstance(current[token], (dict, list)):
                current[token] = {} if next_kind == "key" else []
            current = current[token]
        else:
            if not token.isdigit() or int(token) >= len(current):
                raise KeyError(path)
            current = current[int(token)]
    kind, token = tokens[-1]
    if kind == "key":
        current[token] = value
    else:
        if not token.isdigit() or int(token) >= len(current):
            raise KeyError(path)
        current[int(token)] = value


def _leaf_paths(document, prefix=""):
    if isinstance(document, dict):
        for key in sorted(document):
            path = f"{prefix}.{key}" if prefix else key
            yield from _leaf_paths(document[key], path)
    elif isinstance(document, list):
        if not document:
            yield prefix
        for index, item in enumerate(document):
            yield from _leaf_paths(item, f"{prefix}[{index}]")
    else:
        yield prefix


def _covered(path, declared):
    for item in declared:
        if path == item:
            return True
        # A slot for a container covers all of its descendants.
        if path.startswith(item + ".") or path.startswith(item + "["):
            return True
        # A concrete leaf is covered by a wildcard path.
        if "[*]" in item:
            prefix, suffix = item.split("[*]", 1)
            if path.startswith(prefix + "[") and (not suffix or suffix in path):
                return True
    return False


def _value_ok(value, slot):
    kind = slot.kind
    domain = slot.domain
    if kind in {"parameter_timing", "parameter_lease"}:
        if value == "infinite" and "infinite" in slot.special_values:
            return True
        if type(value) not in (int, float):
            return False
        if "min" in domain and value < domain["min"]:
            return False
        if "max" in domain and value > domain["max"]:
            return False
        return True
    if kind in {"parameter_queue", "parameter_numeric", "generation"}:
        if type(value) is not int:
            return False
        if "min" in domain and value < domain["min"]:
            return False
        if "max" in domain and value > domain["max"]:
            return False
        return True
    if kind in {"parameter_enum", "compatibility"}:
        if "values" in domain:
            return value in domain["values"]
        return True
    if kind == "filter":
        return isinstance(value, str) and len(value) <= domain.get("max_length", 1024)
    if kind == "environment":
        if "values" in domain:
            return value in domain["values"]
        if "min" in domain or "max" in domain:
            if type(value) not in (int, float) or isinstance(value, bool):
                return False
            if "min" in domain and value < domain["min"]:
                return False
            if "max" in domain and value > domain["max"]:
                return False
        return True
    return True


def classify_input(document, surface, *, document_kind=None):
    """Classify an input document without executing it.

    ``expected_disposition`` is the *case expectation* (pass, guard reject,
    no-match, or unknown), never an oracle verdict.  ``evidence_status`` is
    always ``not_executed`` here; callers must not promote this function's
    result into an execution receipt.
    """
    if not isinstance(surface, SurfaceDomain):
        surface = SurfaceDomain.from_dict(surface)
    if not isinstance(document, dict):
        return {"schema": "r2f.s1-input-classification/v1", "surface_id": surface.surface_id,
                "expected_disposition": "expect_guard_reject",
                "reason": "input document must be a JSON object",
                "unbound_fields": [], "validation": None,
                "runtime_observation_required": False,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}

    if document_kind == "delivery_plan" or surface.surface_id == "S1.delivery":
        unbound = []
        for slot in surface.slots:
            try:
                value = _get_path(document, slot.input_path)
            except KeyError:
                continue
            if not _value_ok(value, slot):
                return {"schema": "r2f.s1-input-classification/v1",
                        "surface_id": surface.surface_id,
                        "expected_disposition": "expect_guard_reject",
                        "reason": f"declared value outside domain: {slot.input_path}",
                        "unbound_fields": unbound,
                        "validation": {"slot_id": slot.slot_id, "value": value},
                        "runtime_observation_required": True,
                        "evidence_status": "not_executed", "execution_evidence": False,
                        "candidate_eligible": False}
        return {"schema": "r2f.s1-input-classification/v1",
                "surface_id": surface.surface_id,
                "expected_disposition": "unknown_not_pass",
                "reason": "delivery-plan fields have no runtime consumer; input is declared only",
                "runtime_binding": {"runtime_binding": "delivery.*", "status": "unbound"},
                "unbound_fields": [],
                "validation": None, "runtime_observation_required": True,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}

    from . import topology_model
    if document.get("schema") == topology_model.SCHEMA:
        validation = topology_model.validate_program(document)
        if validation["status"] == "valid":
            disposition = ("expect_guard_reject"
                           if validation["guard_disposition"] == "guard_violation"
                           else "expect_pass")
            return {"schema": "r2f.s1-input-classification/v1",
                    "surface_id": surface.surface_id,
                    "expected_disposition": disposition,
                    "reason": validation["guard_disposition"],
                    "runtime_observation_required": validation["guard_disposition"] == "requires_runtime",
                    "validation": validation, "unbound_fields": [],
                    "evidence_status": "not_executed", "execution_evidence": False,
                    "candidate_eligible": False}
        unknown = [error for error in validation["errors"]
                   if error.get("code") == "unknown"]
        if unknown:
            return {"schema": "r2f.s1-input-classification/v1",
                    "surface_id": surface.surface_id,
                    "expected_disposition": "unknown_not_pass",
                    "reason": "input contains fields not supported by the topology schema",
                    "validation": validation,
                    "unbound_fields": [error["path"] for error in unknown],
                    "runtime_observation_required": False,
                    "evidence_status": "not_executed", "execution_evidence": False,
                    "candidate_eligible": False}
        return {"schema": "r2f.s1-input-classification/v1",
                "surface_id": surface.surface_id,
                "expected_disposition": "expect_guard_reject",
                "reason": "structurally invalid input is a deliberate guard negative",
                "validation": validation, "unbound_fields": [],
                "runtime_observation_required": False,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}

    # Plain logical-config documents: compare every leaf with declared slots.
    declared = [slot.input_path for slot in surface.slots] + list(surface.supporting_fields)
    unbound = [path for path in _leaf_paths(document) if not _covered(path, declared)]
    if unbound:
        return {"schema": "r2f.s1-input-classification/v1",
                "surface_id": surface.surface_id,
                "expected_disposition": "unknown_not_pass",
                "reason": "leaf field(s) are not declared by this input domain; no silent drop",
                "unbound_fields": sorted(unbound), "validation": None,
                "runtime_observation_required": False,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}
    required_missing, invalid = [], []
    for slot in surface.slots:
        try:
            value = _get_path(document, slot.input_path)
        except KeyError:
            if slot.required:
                required_missing.append(slot.input_path)
            continue
        if not _value_ok(value, slot):
            invalid.append({"slot_id": slot.slot_id, "path": slot.input_path, "value": value})
    if required_missing:
        return {"schema": "r2f.s1-input-classification/v1",
                "surface_id": surface.surface_id,
                "expected_disposition": "expect_guard_reject",
                "reason": "required input slot missing", "missing": required_missing,
                "unbound_fields": [], "validation": None,
                "runtime_observation_required": False,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}
    if invalid:
        return {"schema": "r2f.s1-input-classification/v1",
                "surface_id": surface.surface_id,
                "expected_disposition": "expect_guard_reject",
                "reason": "declared value outside its domain", "invalid": invalid,
                "unbound_fields": [], "validation": None,
                "runtime_observation_required": False,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}
    unbound_binding = [slot.slot_id for slot in surface.slots
                       if slot.binding.get("status") in {"unbound", "unsupported",
                                                          "generation_only"}]
    if unbound_binding:
        return {"schema": "r2f.s1-input-classification/v1",
                "surface_id": surface.surface_id,
                "expected_disposition": "unknown_not_pass",
                "reason": "declared input has no executable runtime binding",
                "unbound_binding_slots": unbound_binding, "unbound_fields": [],
                "validation": None, "runtime_observation_required": True,
                "evidence_status": "not_executed", "execution_evidence": False,
                "candidate_eligible": False}
    return {"schema": "r2f.s1-input-classification/v1",
            "surface_id": surface.surface_id,
            "expected_disposition": "expect_pass",
            "reason": "declared input satisfies the offline domain",
            "unbound_fields": [], "validation": None,
            "runtime_observation_required": True,
            "evidence_status": "not_executed", "execution_evidence": False,
            "candidate_eligible": False}


def get_path(document, path):
    return _get_path(document, path)


def set_path(document, path, value):
    _set_path(document, path, value)
    return document


def leaf_paths(document):
    return list(_leaf_paths(document))


def covered(path, declared):
    return _covered(path, declared)


def slot_for(surface, *, slot_id=None, input_path=None, operator_id=None):
    """Resolve exactly one declared slot, or raise with the offending selector."""
    if not isinstance(surface, SurfaceDomain):
        surface = SurfaceDomain.from_dict(surface)
    candidates = list(surface.slots)
    if slot_id is not None:
        candidates = [slot for slot in candidates if slot.slot_id == slot_id]
    if input_path is not None:
        candidates = [slot for slot in candidates
                      if slot.input_path == input_path or _covered(input_path, [slot.input_path])]
    if operator_id is not None:
        candidates = [slot for slot in candidates if operator_id in slot.operators]
    if not candidates:
        raise KeyError("no declared slot matches the selector")
    if len(candidates) > 1:
        raise ValueError("selector is ambiguous; pass slot_id or input_path")
    return candidates[0]
