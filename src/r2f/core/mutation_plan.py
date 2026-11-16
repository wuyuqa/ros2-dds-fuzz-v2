"""Versioned, offline mutation-plan and exploration-contract contracts.

These structures only describe inputs and applicability.  They never claim a
DDS/ROS execution, a hit, or an oracle result.  Execution evidence remains in
the existing seven-field MutationReceipt and is recorded separately.
"""
from dataclasses import dataclass, field
from typing import Any

from ._json import integer, nonempty, sha256_digest, snapshot
from .evidence import canonical_hash


EXPLORATION_CONTRACT_SCHEMA = "r2f.exploration-contract/v1"
MUTATION_SURFACE_SCHEMA = "r2f.mutation-surface/v1"
FACT_SPEC_SCHEMA = "r2f.fact-spec/v1"
RULE_SPEC_SCHEMA = "r2f.rule-spec/v1"
EFFECT_SPEC_SCHEMA = "r2f.effect-spec/v1"
MUTATION_OPERATION_SCHEMA = "r2f.mutation-operation/v1"
MUTATION_PLAN_SCHEMA = "r2f.mutation-plan/v1"
RUNTIME_WINDOW_SCHEMA = "r2f.runtime-window/v1"

RNG_ALGORITHM = "mt19937-v1"
APPLICATION_PHASES = frozenset({
    "compile_time", "pre_create", "post_create", "pre_match", "fresh_match",
    "post_match", "runtime_window", "post_restore",
})
APPLICABILITY_STATUSES = frozenset({
    "applicable", "no_op", "overridden", "rejected", "unsupported",
    "needs_observation", "blocked",
})
MISSING_POLICIES = frozenset({"unknown", "reject", "not_applicable", "needs_observation"})
UNSUPPORTED_POLICIES = frozenset({"reject", "unsupported", "not_applicable", "unknown"})
VALUE_TYPES = frozenset({
    "int", "float", "bool", "str", "enum", "duration_ns", "duration_sec",
    "relation", "window_plan", "pair_relation",
})
FACT_SCOPES = frozenset({"run", "endpoint", "pair", "window", "sample"})
RESPONSIBILITY_LAYERS = frozenset({"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "cross_module"})
WINDOW_ANCHORS = frozenset({"pre_match", "fresh_match", "previous_restore_confirmed"})


def _exact(data, fields, label):
    if not isinstance(data, dict):
        raise ValueError(f"{label} must be a dict")
    missing = set(fields) - set(data)
    extra = set(data) - set(fields)
    if missing:
        raise ValueError(f"{label} missing field(s): {','.join(sorted(missing))}")
    if extra:
        raise ValueError(f"{label} unknown field(s): {','.join(sorted(extra))}")
    return data


def _string_list(value, field, *, allow_empty=True):
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{field} must be a list of non-empty strings")
    if not allow_empty and not value:
        raise ValueError(f"{field} must not be empty")
    return list(value)


def _mapping(value, field):
    value = snapshot(value)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a dict")
    return value


def _status(value, field="applicability_result"):
    value = _mapping(value, field)
    if "status" not in value:
        raise ValueError(f"{field} must contain status")
    if value["status"] not in APPLICABILITY_STATUSES:
        raise ValueError(f"{field} has unsupported status: {value['status']}")
    return value


def _validate_runtime_window(window):
    window = _exact(snapshot(window), {
        "schema", "window_id", "window_index", "trigger_anchor",
        "trigger_offset_sec", "drop_duration_sec", "restore_interval_sec",
    }, "runtime_window")
    if window["schema"] != RUNTIME_WINDOW_SCHEMA:
        raise ValueError("unsupported runtime window schema")
    nonempty(window["window_id"], "runtime_window.window_id")
    integer(window["window_index"], "runtime_window.window_index")
    if window["trigger_anchor"] not in WINDOW_ANCHORS:
        raise ValueError("unsupported runtime window trigger_anchor")
    for key in ("trigger_offset_sec", "drop_duration_sec", "restore_interval_sec"):
        if type(window[key]) not in (int, float) or window[key] < 0:
            raise ValueError(f"runtime_window.{key} must be a non-negative number")
    return window


def _validate_runtime_windows(windows):
    if not isinstance(windows, list):
        raise ValueError("runtime_windows must be a list")
    result = [_validate_runtime_window(item) for item in windows]
    ids = [item["window_id"] for item in result]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate runtime window_id")
    return result


@dataclass
class FactSpec:
    id: str
    version: int
    value_type: str
    unit: str
    scope: str
    sources: list
    producer_id: str
    freshness: str
    identity_binding: str
    missing_policy: str
    schema: str = FACT_SPEC_SCHEMA

    def __post_init__(self):
        if self.schema != FACT_SPEC_SCHEMA:
            raise ValueError("unsupported fact schema")
        nonempty(self.id, "fact.id")
        integer(self.version, "fact.version", minimum=1)
        if self.value_type not in VALUE_TYPES:
            raise ValueError(f"unsupported fact value_type: {self.value_type}")
        if self.scope not in FACT_SCOPES:
            raise ValueError(f"unsupported fact scope: {self.scope}")
        self.sources = _string_list(self.sources, "fact.sources", allow_empty=False)
        for name in ("unit", "producer_id", "freshness", "identity_binding"):
            nonempty(getattr(self, name), f"fact.{name}")
        if self.missing_policy not in MISSING_POLICIES:
            raise ValueError(f"unsupported fact missing_policy: {self.missing_policy}")

    def to_dict(self):
        return snapshot(vars(self))

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "schema", "id", "version", "value_type", "unit", "scope", "sources",
            "producer_id", "freshness", "identity_binding", "missing_policy",
        }, "fact_spec"))


@dataclass
class RuleSpec:
    id: str
    version: int
    spec_refs: list
    applies_when: Any
    required_fact_ids: list
    evaluator_id: str
    allowed_effects: list
    forbidden_residuals: list
    limitations: str
    responsibility_layer: str
    primary_or_supporting: str
    schema: str = RULE_SPEC_SCHEMA

    def __post_init__(self):
        if self.schema != RULE_SPEC_SCHEMA:
            raise ValueError("unsupported rule schema")
        nonempty(self.id, "rule.id")
        integer(self.version, "rule.version", minimum=1)
        self.spec_refs = _string_list(self.spec_refs, "rule.spec_refs", allow_empty=False)
        self.required_fact_ids = _string_list(self.required_fact_ids, "rule.required_fact_ids", allow_empty=False)
        self.allowed_effects = _string_list(self.allowed_effects, "rule.allowed_effects")
        self.forbidden_residuals = _string_list(self.forbidden_residuals, "rule.forbidden_residuals")
        nonempty(self.evaluator_id, "rule.evaluator_id")
        nonempty(self.limitations, "rule.limitations")
        if self.responsibility_layer not in RESPONSIBILITY_LAYERS:
            raise ValueError(f"unsupported responsibility_layer: {self.responsibility_layer}")
        if self.primary_or_supporting not in {"primary", "supporting"}:
            raise ValueError("rule.primary_or_supporting must be primary or supporting")
        self.applies_when = snapshot(self.applies_when)
        if not isinstance(self.applies_when, (dict, list, str)):
            raise ValueError("rule.applies_when must be a JSON object, list, or string")

    def to_dict(self):
        return snapshot(vars(self))

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "schema", "id", "version", "spec_refs", "applies_when", "required_fact_ids",
            "evaluator_id", "allowed_effects", "forbidden_residuals", "limitations",
            "responsibility_layer", "primary_or_supporting",
        }, "rule_spec"))


@dataclass
class EffectSpec:
    effect_id: str
    version: int
    window: str
    allowed_observations: list
    forbidden_residuals: list
    schema: str = EFFECT_SPEC_SCHEMA

    def __post_init__(self):
        if self.schema != EFFECT_SPEC_SCHEMA:
            raise ValueError("unsupported effect schema")
        nonempty(self.effect_id, "effect.effect_id")
        integer(self.version, "effect.version", minimum=1)
        nonempty(self.window, "effect.window")
        self.allowed_observations = _string_list(self.allowed_observations, "effect.allowed_observations")
        self.forbidden_residuals = _string_list(self.forbidden_residuals, "effect.forbidden_residuals")

    def to_dict(self):
        return snapshot(vars(self))

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "schema", "effect_id", "version", "window", "allowed_observations", "forbidden_residuals",
        }, "effect_spec"))


@dataclass
class MutationSurface:
    surface_id: str
    version: int
    source_refs: list
    surface_kind: str
    input_paths: list
    value_type: str
    unit: str
    domain: dict
    special_values: list
    boundary_classes: list
    mutator_ids: list
    value_relations: list
    tool_requires: dict
    sut_guards: dict
    carrier_id: str
    runtime_phase: str
    runtime_binding: str
    allowed_with: list
    conflicts_with: list
    order_after: list
    fact_ids: list
    rule_ids: list
    effect_id: str
    missing_policy: str
    unsupported_policy: str
    activation: str = "F1"
    schema: str = MUTATION_SURFACE_SCHEMA

    def __post_init__(self):
        if self.schema != MUTATION_SURFACE_SCHEMA:
            raise ValueError("unsupported mutation surface schema")
        nonempty(self.surface_id, "surface.surface_id")
        integer(self.version, "surface.version", minimum=1)
        nonempty(self.surface_kind, "surface.surface_kind")
        if self.surface_kind not in {
                "parameter_numeric", "parameter_enum", "parameter_timing",
                "relation", "environment", "compatibility"}:
            raise ValueError(f"unsupported surface_kind: {self.surface_kind}")
        self.source_refs = _string_list(self.source_refs, "surface.source_refs", allow_empty=False)
        self.input_paths = _string_list(
            self.input_paths, "surface.input_paths",
            allow_empty=self.surface_kind == "environment")
        if self.surface_kind != "environment" and not self.input_paths:
            raise ValueError("surface.input_paths must not be empty")
        if self.value_type not in VALUE_TYPES:
            raise ValueError(f"unsupported surface value_type: {self.value_type}")
        nonempty(self.unit, "surface.unit")
        self.domain = _mapping(self.domain, "surface.domain")
        if not self.domain:
            raise ValueError("surface.domain must not be empty")
        self.special_values = snapshot(self.special_values)
        if not isinstance(self.special_values, list):
            raise ValueError("surface.special_values must be a list")
        self.boundary_classes = _string_list(self.boundary_classes, "surface.boundary_classes")
        self.mutator_ids = _string_list(self.mutator_ids, "surface.mutator_ids", allow_empty=False)
        self.value_relations = snapshot(self.value_relations)
        if not isinstance(self.value_relations, list):
            raise ValueError("surface.value_relations must be a list")
        self.tool_requires = _mapping(self.tool_requires, "surface.tool_requires")
        self.sut_guards = _mapping(self.sut_guards, "surface.sut_guards")
        for key, value in self.sut_guards.items():
            if value not in (True, False, "unknown"):
                raise ValueError(f"surface.sut_guards[{key}] must be true, false, or unknown")
        for name in ("carrier_id", "runtime_phase"):
            nonempty(getattr(self, name), f"surface.{name}")
        if not isinstance(self.runtime_binding, str):
            raise ValueError("surface.runtime_binding must be a string")
        self.allowed_with = _string_list(self.allowed_with, "surface.allowed_with")
        self.conflicts_with = _string_list(self.conflicts_with, "surface.conflicts_with")
        self.order_after = _string_list(self.order_after, "surface.order_after")
        self.fact_ids = _string_list(self.fact_ids, "surface.fact_ids")
        self.rule_ids = _string_list(self.rule_ids, "surface.rule_ids")
        nonempty(self.effect_id, "surface.effect_id")
        if self.missing_policy not in MISSING_POLICIES:
            raise ValueError(f"unsupported surface missing_policy: {self.missing_policy}")
        if self.unsupported_policy not in UNSUPPORTED_POLICIES:
            raise ValueError(f"unsupported surface unsupported_policy: {self.unsupported_policy}")
        if self.activation not in {"F1", "F2"}:
            raise ValueError("surface.activation must be F1 or F2")

    @property
    def executable(self):
        return bool(self.runtime_binding) and self.runtime_binding not in {"unbound", "none"}

    def to_dict(self):
        return snapshot(vars(self))

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "schema", "surface_id", "version", "source_refs", "surface_kind", "input_paths",
            "value_type", "unit", "domain", "special_values", "boundary_classes", "mutator_ids",
            "value_relations", "tool_requires", "sut_guards", "carrier_id", "runtime_phase",
            "runtime_binding", "allowed_with", "conflicts_with", "order_after", "fact_ids",
            "rule_ids", "effect_id", "missing_policy", "unsupported_policy", "activation",
        }, "mutation_surface"))


@dataclass
class MutationProposal:
    """A pure mutator result.  Changes use dotted input paths."""
    surface_id: str
    mutator_id: str
    parameters: dict
    changes: dict
    input_before: dict
    input_after: dict
    application_phase: str
    applicability_result: dict
    runtime_windows: list = field(default_factory=list)
    override: bool = False
    notes: dict = field(default_factory=dict)
    proposal_id: str = ""

    def __post_init__(self):
        nonempty(self.surface_id, "proposal.surface_id")
        nonempty(self.mutator_id, "proposal.mutator_id")
        self.parameters = _mapping(self.parameters, "proposal.parameters")
        self.changes = _mapping(self.changes, "proposal.changes")
        for path in self.changes:
            nonempty(path, "proposal.changes path")
            if path.startswith(".") or path.endswith(".") or ".." in path:
                raise ValueError(f"invalid dotted input path: {path}")
        self.input_before = _mapping(self.input_before, "proposal.input_before")
        self.input_after = _mapping(self.input_after, "proposal.input_after")
        if self.application_phase not in APPLICATION_PHASES:
            raise ValueError(f"unsupported application_phase: {self.application_phase}")
        self.applicability_result = _status(self.applicability_result)
        self.runtime_windows = _validate_runtime_windows(self.runtime_windows)
        if type(self.override) is not bool:
            raise ValueError("proposal.override must be a boolean")
        self.notes = _mapping(self.notes, "proposal.notes")
        if self.proposal_id:
            nonempty(self.proposal_id, "proposal.proposal_id")

    @property
    def applicability_status(self):
        return self.applicability_result["status"]

    def to_dict(self):
        return snapshot(vars(self))


@dataclass
class MutationOperation:
    op_id: str
    surface_id: str
    mutator_id: str
    parameters: dict
    input_before_sha256: str
    input_after_sha256: str
    application_phase: str
    applicability_result: dict
    schema: str = MUTATION_OPERATION_SCHEMA

    def __post_init__(self):
        if self.schema != MUTATION_OPERATION_SCHEMA:
            raise ValueError("unsupported mutation operation schema")
        nonempty(self.op_id, "operation.op_id")
        nonempty(self.surface_id, "operation.surface_id")
        nonempty(self.mutator_id, "operation.mutator_id")
        self.parameters = _mapping(self.parameters, "operation.parameters")
        sha256_digest(self.input_before_sha256, "operation.input_before_sha256")
        sha256_digest(self.input_after_sha256, "operation.input_after_sha256")
        if self.application_phase not in APPLICATION_PHASES:
            raise ValueError(f"unsupported application_phase: {self.application_phase}")
        self.applicability_result = _status(self.applicability_result)

    @property
    def applicability_status(self):
        return self.applicability_result["status"]

    def to_dict(self):
        return snapshot(vars(self))

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "schema", "op_id", "surface_id", "mutator_id", "parameters",
            "input_before_sha256", "input_after_sha256", "application_phase", "applicability_result",
        }, "mutation_operation"))


@dataclass
class ExplorationContract:
    schema: str = EXPLORATION_CONTRACT_SCHEMA
    contract_version: int = 1
    surfaces: list = field(default_factory=list)
    facts: list = field(default_factory=list)
    rules: list = field(default_factory=list)
    effects: list = field(default_factory=list)

    def __post_init__(self):
        if self.schema != EXPLORATION_CONTRACT_SCHEMA:
            raise ValueError("unsupported exploration contract schema")
        integer(self.contract_version, "contract_version", minimum=1)
        self.surfaces = [MutationSurface.from_dict(s) if isinstance(s, dict) else s for s in self.surfaces]
        self.facts = [FactSpec.from_dict(f) if isinstance(f, dict) else f for f in self.facts]
        self.rules = [RuleSpec.from_dict(r) if isinstance(r, dict) else r for r in self.rules]
        self.effects = [EffectSpec.from_dict(e) if isinstance(e, dict) else e for e in self.effects]
        for items, cls, label in (
            (self.surfaces, MutationSurface, "surfaces"), (self.facts, FactSpec, "facts"),
            (self.rules, RuleSpec, "rules"), (self.effects, EffectSpec, "effects"),
        ):
            if any(not isinstance(item, cls) for item in items):
                raise ValueError(f"invalid {label}")
        validate_contract(self)

    @property
    def surface_by_id(self):
        return {surface.surface_id: surface for surface in self.surfaces}

    @property
    def fact_by_id(self):
        return {fact.id: fact for fact in self.facts}

    @property
    def rule_by_id(self):
        return {rule.id: rule for rule in self.rules}

    @property
    def effect_by_id(self):
        return {effect.effect_id: effect for effect in self.effects}

    def validate(self):
        return validate_contract(self)

    def contract_hash(self):
        from .mutators import mutator_registry_signature
        return canonical_hash({
            "hash_schema": "r2f.exploration-contract-hash/v1",
            "contract": self.to_dict(),
            "mutator_registry_sha256": mutator_registry_signature(),
        })

    def to_dict(self):
        return snapshot({
            "schema": self.schema, "contract_version": self.contract_version,
            "surfaces": [surface.to_dict() for surface in self.surfaces],
            "facts": [fact.to_dict() for fact in self.facts],
            "rules": [rule.to_dict() for rule in self.rules],
            "effects": [effect.to_dict() for effect in self.effects],
        })

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "schema", "contract_version", "surfaces", "facts", "rules", "effects",
        }, "exploration_contract"))


def _cycle_check(surfaces):
    graph = {surface.surface_id: list(surface.order_after) for surface in surfaces}
    visiting, done = set(), set()
    stack = []

    def visit(node):
        if node in done:
            return
        if node in visiting:
            start = stack.index(node)
            cycle = stack[start:] + [node]
            raise ValueError("cyclic order dependency: " + " -> ".join(cycle))
        visiting.add(node)
        stack.append(node)
        for dep in graph[node]:
            visit(dep)
        stack.pop()
        visiting.remove(node)
        done.add(node)

    for node in graph:
        visit(node)


def validate_contract(contract, mutators=None):
    if not isinstance(contract, ExplorationContract):
        contract = ExplorationContract.from_dict(contract)
    if mutators is None:
        from .mutators import MUTATOR_SPECS
        mutators = MUTATOR_SPECS
    if not isinstance(mutators, dict):
        mutators = {spec.mutator_id: spec for spec in mutators}

    for items, label, key in (
        (contract.surfaces, "surface", lambda x: x.surface_id),
        (contract.facts, "fact", lambda x: x.id),
        (contract.rules, "rule", lambda x: x.id),
        (contract.effects, "effect", lambda x: x.effect_id),
    ):
        ids = [key(item) for item in items]
        if len(ids) != len(set(ids)):
            raise ValueError(f"duplicate {label} id")

    surfaces = contract.surface_by_id
    facts = contract.fact_by_id
    rules = contract.rule_by_id
    effects = contract.effect_by_id

    for surface in contract.surfaces:
        for mutator_id in surface.mutator_ids:
            if mutator_id not in mutators:
                raise ValueError(f"unknown mutator reference: {mutator_id}")
            spec = mutators[mutator_id]
            if surface.surface_kind not in spec.accepted_surface_kinds:
                raise ValueError(
                    f"mutator {mutator_id} does not accept surface kind {surface.surface_kind}")
        for fact_id in surface.fact_ids:
            if fact_id not in facts:
                raise ValueError(f"unknown fact reference: {fact_id}")
        for rule_id in surface.rule_ids:
            if rule_id not in rules:
                raise ValueError(f"unknown rule reference: {rule_id}")
        if surface.effect_id not in effects:
            raise ValueError(f"unknown effect reference: {surface.effect_id}")
        for attr in ("allowed_with", "conflicts_with", "order_after"):
            for other in getattr(surface, attr):
                if other == surface.surface_id:
                    raise ValueError(f"surface {surface.surface_id} cannot {attr} itself")
                if other not in surfaces:
                    raise ValueError(f"unknown {attr} reference: {other}")
        for other in surface.conflicts_with:
            if surface.surface_id not in surfaces[other].conflicts_with:
                raise ValueError(
                    f"conflict relation must be symmetric: {surface.surface_id} -> {other}")
        overlap = set(surface.allowed_with) & set(surface.conflicts_with)
        if overlap:
            raise ValueError(
                f"surface {surface.surface_id} both allows and conflicts with {sorted(overlap)[0]}")

    for effect in contract.effects:
        for residual in effect.forbidden_residuals:
            nonempty(residual, "effect.forbidden_residuals item")

    for rule in contract.rules:
        for fact_id in rule.required_fact_ids:
            if fact_id not in facts:
                raise ValueError(f"unknown rule fact reference: {fact_id}")
        for effect_id in rule.allowed_effects:
            if effect_id not in effects:
                raise ValueError(f"unknown rule effect reference: {effect_id}")
        for residual in rule.forbidden_residuals:
            nonempty(residual, "rule.forbidden_residuals item")

    _cycle_check(contract.surfaces)
    return True


@dataclass
class MutationPlan:
    plan_version: str
    rng_algorithm: str
    rng_seed: int
    parent_content_sha256: str
    operations: list
    final_config: dict
    runtime_windows: list
    contract_hash: str

    def __post_init__(self):
        if self.plan_version != MUTATION_PLAN_SCHEMA:
            raise ValueError("unsupported mutation plan version")
        nonempty(self.rng_algorithm, "rng_algorithm")
        integer(self.rng_seed, "rng_seed")
        sha256_digest(self.parent_content_sha256, "parent_content_sha256")
        sha256_digest(self.contract_hash, "contract_hash")
        self.operations = [MutationOperation.from_dict(op) if isinstance(op, dict) else op
                           for op in self.operations]
        if any(not isinstance(op, MutationOperation) for op in self.operations):
            raise ValueError("operations must contain MutationOperation objects")
        self.final_config = _mapping(self.final_config, "final_config")
        self.runtime_windows = _validate_runtime_windows(self.runtime_windows)
        self.validate()

    def validate(self, contract=None):
        op_ids = [op.op_id for op in self.operations]
        if len(op_ids) != len(set(op_ids)):
            raise ValueError("duplicate operation op_id")
        previous = self.parent_content_sha256
        for operation in self.operations:
            if operation.input_before_sha256 != previous:
                raise ValueError("mutation operation chain is broken at " + operation.op_id)
            previous = operation.input_after_sha256
        if canonical_hash(self.final_config) != previous:
            raise ValueError("final_config does not match the final operation input_after_sha256")
        if contract is not None:
            if not isinstance(contract, ExplorationContract):
                contract = ExplorationContract.from_dict(contract)
            if self.contract_hash != contract.contract_hash():
                raise ValueError("mutation plan contract_hash mismatch")
            surfaces = contract.surface_by_id
            for operation in self.operations:
                if operation.surface_id not in surfaces:
                    raise ValueError(f"plan references unknown surface: {operation.surface_id}")
                surface = surfaces[operation.surface_id]
                if operation.mutator_id not in surface.mutator_ids:
                    raise ValueError(
                        f"plan mutator {operation.mutator_id} is not declared by surface {surface.surface_id}")
        return True

    def content_hash(self):
        """Semantic input content; execution metadata and RNG seed are excluded."""
        return canonical_hash({
            "plan_version": self.plan_version,
            "rng_algorithm": self.rng_algorithm,
            "parent_content_sha256": self.parent_content_sha256,
            "operations": [operation.to_dict() for operation in self.operations],
            "final_config": snapshot(self.final_config),
            "runtime_windows": snapshot(self.runtime_windows),
            "contract_hash": self.contract_hash,
        })

    def plan_hash(self):
        return canonical_hash({
            "plan": self.to_dict(),
            "content_hash": self.content_hash(),
        })

    def to_dict(self):
        return snapshot({
            "plan_version": self.plan_version, "rng_algorithm": self.rng_algorithm,
            "rng_seed": self.rng_seed, "parent_content_sha256": self.parent_content_sha256,
            "operations": [operation.to_dict() for operation in self.operations],
            "final_config": self.final_config, "runtime_windows": self.runtime_windows,
            "contract_hash": self.contract_hash,
        })

    @classmethod
    def from_dict(cls, data):
        return cls(**_exact(snapshot(data), {
            "plan_version", "rng_algorithm", "rng_seed", "parent_content_sha256",
            "operations", "final_config", "runtime_windows", "contract_hash",
        }, "mutation_plan"))


@dataclass
class ValidationResult:
    status: str
    reasons: list
    plan_hash: str = ""
    content_hash: str = ""

    def __post_init__(self):
        if self.status not in {"valid", "invalid", "unsupported_capability", "needs_observation"}:
            raise ValueError("unsupported validation status")
        self.reasons = _string_list(self.reasons, "validation.reasons")

    @property
    def executable(self):
        return self.status == "valid"

    @property
    def ok(self):
        return self.status in {"valid", "needs_observation"}

    def to_dict(self):
        return snapshot(vars(self))


def validate_plan(plan, capabilities=None, contract=None):
    """Validate syntax, contract references, and declared runtime capabilities.

    A needs_observation result is not a pass: it says a plan is structurally
    valid but its effect still requires real execution evidence.
    """
    if not isinstance(plan, MutationPlan):
        try:
            plan = MutationPlan.from_dict(plan)
        except ValueError as exc:
            return ValidationResult("invalid", [str(exc)])
    reasons = []
    try:
        plan.validate(contract=contract)
    except ValueError as exc:
        return ValidationResult("invalid", [str(exc)], plan.plan_hash(), plan.content_hash())

    surfaces = {}
    if contract is not None:
        if not isinstance(contract, ExplorationContract):
            contract = ExplorationContract.from_dict(contract)
        surfaces = contract.surface_by_id

    if capabilities is not None:
        caps = _mapping(capabilities, "capabilities")
        if "mutators" in caps:
            allowed = set(caps["mutators"])
            for operation in plan.operations:
                if operation.mutator_id not in allowed:
                    reasons.append(f"mutator capability not enabled: {operation.mutator_id}")
        if "surface_kinds" in caps:
            allowed = set(caps["surface_kinds"])
            for operation in plan.operations:
                surface = surfaces.get(operation.surface_id)
                if surface is not None and surface.surface_kind not in allowed:
                    reasons.append(f"surface kind not enabled: {surface.surface_kind}")
        if "runtime_bindings" in caps:
            allowed = set(caps["runtime_bindings"])
            for operation in plan.operations:
                surface = surfaces.get(operation.surface_id)
                if surface is not None and not surface.executable:
                    reasons.append(f"surface has no runtime binding: {surface.surface_id}")
                elif surface is not None and surface.runtime_binding not in allowed:
                    reasons.append(f"runtime binding not enabled: {surface.runtime_binding}")
        if "activation" in caps:
            allowed = set(caps["activation"]) if not isinstance(caps["activation"], str) else {caps["activation"]}
            for operation in plan.operations:
                surface = surfaces.get(operation.surface_id)
                if surface is not None and surface.activation not in allowed:
                    reasons.append(f"surface activation not enabled: {surface.activation}")
        if len(plan.runtime_windows) > 1 and not caps.get("multi_window", False):
            reasons.append("multi-window execution is not enabled")

    for operation in plan.operations:
        if operation.applicability_status == "unsupported":
            reasons.append(f"operation reports unsupported: {operation.op_id}")
        elif operation.applicability_status in {"rejected", "blocked"}:
            return ValidationResult("invalid", reasons + [f"operation is not applicable: {operation.op_id}"],
                                    plan.plan_hash(), plan.content_hash())

    if reasons:
        return ValidationResult("unsupported_capability", reasons, plan.plan_hash(), plan.content_hash())

    if len(plan.runtime_windows) > 1:
        return ValidationResult("unsupported_capability", ["multi-window execution is not implemented"],
                                plan.plan_hash(), plan.content_hash())

    if any(operation.applicability_status == "needs_observation" for operation in plan.operations):
        return ValidationResult("needs_observation", ["plan requires runtime observation"],
                                plan.plan_hash(), plan.content_hash())
    return ValidationResult("valid", [], plan.plan_hash(), plan.content_hash())


class CompositionRejection(ValueError):
    def __init__(self, reason, details=None):
        super().__init__(reason)
        self.reason = reason
        self.details = snapshot(details or {})


def _get_path(data, path):
    current = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise ValueError(f"missing input path: {path}")
        current = current[part]
    return current


def _set_path(data, path, value):
    current = data
    parts = path.split(".")
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        if not isinstance(current[part], dict):
            raise ValueError(f"input path is not a mapping: {path}")
        current = current[part]
    current[parts[-1]] = snapshot(value)


def _apply_changes(data, changes):
    result = snapshot(data)
    for path, value in changes.items():
        _set_path(result, path, value)
    return result


def compose(parent, proposals, contract, *, rng_seed=0, rng_algorithm=RNG_ALGORITHM,
            runtime_windows=None, allow_override=False):
    """Compose pure proposals into one versioned plan.

    Field conflicts are rejected unless the later proposal explicitly overrides
    (or the caller passes allow_override).  An overridden operation remains in
    the plan for provenance but is excluded from required hit aggregation.
    """
    if not isinstance(contract, ExplorationContract):
        contract = ExplorationContract.from_dict(contract)
    contract.validate()
    parent = _mapping(parent, "parent")
    if not isinstance(proposals, list) or any(not isinstance(p, MutationProposal) for p in proposals):
        raise CompositionRejection("proposals must contain MutationProposal objects")
    surfaces = contract.surface_by_id
    for proposal in proposals:
        if proposal.surface_id not in surfaces:
            raise CompositionRejection(f"unknown proposal surface: {proposal.surface_id}")
        surface = surfaces[proposal.surface_id]
        if proposal.mutator_id not in surface.mutator_ids:
            raise CompositionRejection(
                f"mutator {proposal.mutator_id} is not declared by surface {surface.surface_id}")

    op_ids = [p.proposal_id or f"op-{index:02d}-{p.surface_id}-{p.mutator_id}"
              for index, p in enumerate(proposals)]
    if len(op_ids) != len(set(op_ids)):
        raise CompositionRejection("duplicate proposal/op id")
    positions = {p.surface_id: index for index, p in enumerate(proposals)}

    for index, proposal in enumerate(proposals):
        surface = surfaces[proposal.surface_id]
        for dependency in surface.order_after:
            if dependency in positions and positions[dependency] > index:
                raise CompositionRejection(
                    f"order dependency violated: {surface.surface_id} must follow {dependency}")

    for i, left in enumerate(proposals):
        left_surface = surfaces[left.surface_id]
        for right in proposals[i + 1:]:
            right_surface = surfaces[right.surface_id]
            both = {left.surface_id, right.surface_id}
            if right.surface_id in left_surface.conflicts_with or left.surface_id in right_surface.conflicts_with:
                raise CompositionRejection(
                    f"surface conflict: {left.surface_id} conflicts with {right.surface_id}")
            if left_surface.allowed_with and right.surface_id not in left_surface.allowed_with:
                raise CompositionRejection(
                    f"surface {left.surface_id} does not allow {right.surface_id}")
            if right_surface.allowed_with and left.surface_id not in right_surface.allowed_with:
                raise CompositionRejection(
                    f"surface {right.surface_id} does not allow {left.surface_id}")

    override_map = {}
    for i, left in enumerate(proposals):
        for j in range(i + 1, len(proposals)):
            right = proposals[j]
            common = set(left.changes) & set(right.changes)
            if not common:
                continue
            if allow_override or right.override:
                override_map[i] = op_ids[j]
            else:
                raise CompositionRejection(
                    "field conflict without explicit override: " + ",".join(sorted(common)),
                    {"fields": sorted(common), "left": op_ids[i], "right": op_ids[j]})

    windows = []
    if runtime_windows is not None:
        windows.extend(_validate_runtime_windows(runtime_windows))
    for proposal in proposals:
        windows.extend(proposal.runtime_windows)

    current = snapshot(parent)
    operations = []
    for index, proposal in enumerate(proposals):
        before = snapshot(current)
        after = _apply_changes(before, proposal.changes)
        status = snapshot(proposal.applicability_result)
        if index in override_map:
            status["status"] = "overridden"
            status["overridden_by"] = override_map[index]
        operation = MutationOperation(
            op_id=op_ids[index], surface_id=proposal.surface_id, mutator_id=proposal.mutator_id,
            parameters=proposal.parameters, input_before_sha256=canonical_hash(before),
            input_after_sha256=canonical_hash(after), application_phase=proposal.application_phase,
            applicability_result=status,
        )
        operations.append(operation)
        current = after

    plan = MutationPlan(
        plan_version=MUTATION_PLAN_SCHEMA, rng_algorithm=rng_algorithm, rng_seed=rng_seed,
        parent_content_sha256=canonical_hash(parent), operations=operations,
        final_config=current, runtime_windows=windows, contract_hash=contract.contract_hash(),
    )
    plan.validate(contract=contract)
    return plan
