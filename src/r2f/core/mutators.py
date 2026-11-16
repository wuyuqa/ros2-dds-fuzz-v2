"""Minimal pure mutator slice: M01-M03 only.

M04-M08 are not implemented here and must not be reported as available.
No mutator opens sockets, reads files, consults clocks, or touches ROS.  The
caller supplies a seeded ``random.Random`` and an explicit input document.
A MutationProposal is an input proposal, never an execution receipt or a hit.
"""
from dataclasses import dataclass
import math

from ._json import nonempty, snapshot
from .mutation_plan import (
    APPLICATION_PHASES, MutationProposal, MutationSurface, RNG_ALGORITHM,
    _apply_changes, _get_path,
)


MUTATOR_SCHEMA = "r2f.mutator-spec/v1"
SURFACE_KINDS = frozenset({
    "parameter_numeric", "parameter_enum", "parameter_timing", "relation", "environment", "compatibility",
})


@dataclass(frozen=True)
class MutatorSpec:
    mutator_id: str
    version: int
    accepted_surface_kinds: tuple
    name: str
    description: str
    schema: str = MUTATOR_SCHEMA

    def __post_init__(self):
        if self.schema != MUTATOR_SCHEMA:
            raise ValueError("unsupported mutator spec schema")
        nonempty(self.mutator_id, "mutator_id")
        if type(self.version) is not int or self.version < 1:
            raise ValueError("mutator version must be a positive integer")
        if not isinstance(self.accepted_surface_kinds, tuple) or not self.accepted_surface_kinds:
            raise ValueError("accepted_surface_kinds must be a non-empty tuple")
        for kind in self.accepted_surface_kinds:
            if kind not in SURFACE_KINDS:
                raise ValueError(f"unsupported accepted surface kind: {kind}")
        nonempty(self.name, "mutator name")
        nonempty(self.description, "mutator description")

    def to_dict(self):
        return {"schema": self.schema, "mutator_id": self.mutator_id, "version": self.version,
                "accepted_surface_kinds": list(self.accepted_surface_kinds),
                "name": self.name, "description": self.description}


MUTATOR_SPECS = {
    "M01": MutatorSpec("M01", 1, ("parameter_numeric",), "boundary_pick",
                       "pick a declared boundary neighbour"),
    "M02": MutatorSpec("M02", 1, ("parameter_enum",), "enum_replace",
                       "pick a different declared enum value"),
    "M03": MutatorSpec("M03", 1, ("parameter_numeric", "parameter_timing"), "range_sample",
                       "sample a bounded, quantized numeric range"),
}


def mutator_registry_signature():
    """Stable description of the registered mutator semantics and versions."""
    from .evidence import canonical_hash
    return canonical_hash({"schema": "r2f.mutator-registry/v1",
                           "mutators": [MUTATOR_SPECS[key].to_dict() for key in sorted(MUTATOR_SPECS)]})


def _require_rng(rng):
    for name in ("randrange", "choice", "random"):
        if not callable(getattr(rng, name, None)):
            raise ValueError("rng must provide randrange/choice/random")
    return rng


def _surface(value):
    return value if isinstance(value, MutationSurface) else MutationSurface.from_dict(value)


def _spec(surface, mutator_id):
    spec = MUTATOR_SPECS.get(mutator_id)
    if spec is None:
        raise ValueError(f"unknown or not implemented mutator: {mutator_id}")
    if mutator_id not in surface.mutator_ids:
        raise ValueError(f"mutator {mutator_id} is not declared by surface {surface.surface_id}")
    if surface.surface_kind not in spec.accepted_surface_kinds:
        raise ValueError(f"mutator {mutator_id} does not accept surface kind {surface.surface_kind}")
    return spec


def _context(context):
    context = snapshot(context or {})
    if not isinstance(context, dict):
        raise ValueError("mutation context must be a dict")
    return context


def _proposal(surface, mutator_id, parameters, before, changes, context, notes=None, status="applicable"):
    after = _apply_changes(before, changes)
    phase = context.get("application_phase") or "compile_time"
    if phase not in APPLICATION_PHASES:
        raise ValueError(f"unsupported application_phase: {phase}")
    applicability = {"status": status}
    if surface.sut_guards:
        applicability["sut_guards"] = snapshot(surface.sut_guards)
    return MutationProposal(
        surface_id=surface.surface_id, mutator_id=mutator_id,
        parameters=snapshot(parameters), changes=snapshot(changes),
        input_before=before, input_after=after, application_phase=phase,
        applicability_result=applicability, notes=snapshot(notes or {}),
    )


def _numeric_domain(surface):
    domain = surface.domain
    if not isinstance(domain.get("min"), (int, float)) or not isinstance(domain.get("max"), (int, float)):
        raise ValueError("numeric surface domain requires numeric min and max")
    if type(domain["min"]) is bool or type(domain["max"]) is bool:
        raise ValueError("numeric bounds must not be booleans")
    if domain["min"] > domain["max"]:
        raise ValueError("numeric domain min exceeds max")
    return domain


def _step(surface):
    step = surface.domain.get("step", surface.domain.get("quantum", 1))
    if type(step) not in (int, float) or step <= 0:
        raise ValueError("surface step/quantum must be a positive number")
    return step


def _quantize(value, surface):
    step = _step(surface)
    if type(step) is int and type(surface.domain.get("min")) is int and type(surface.domain.get("max")) is int:
        return int(round(value))
    # Decimal-place rounding of a binary float introduces representation drift;
    # derive the surface precision from the declared step instead.
    digits = max(0, -math.floor(math.log10(step))) if step < 1 else 0
    return round(round(value / step) * step, digits)


def _boundary_values(surface):
    domain = _numeric_domain(surface)
    step = _step(surface)
    minimum, maximum = domain["min"], domain["max"]
    values = {
        "min": minimum,
        "min_plus_one": _quantize(minimum + step, surface),
        "max_minus_one": _quantize(maximum - step, surface),
        "max": maximum,
    }
    # Declared semantic classes are real input classes, not aliases for min.
    # "finite" selects a non-zero representative inside the declared range;
    # "zero"/"infinite" come from special_values and are checked by the caller
    # when it chooses the class.  Never silently fold a class into min.
    special = {item.get("name"): item.get("value")
               for item in surface.special_values if isinstance(item, dict)}
    if "finite" in surface.boundary_classes:
        finite = None
        for item in surface.special_values:
            if isinstance(item, dict) and item.get("name") == "finite":
                candidate = item.get("value")
                if type(candidate) in (int, float) and minimum <= candidate <= maximum:
                    finite = candidate
                    break
        if finite is None:
            finite = next((v for v in (0.01, 0.02, 0.15, 1.0)
                           if minimum <= v <= maximum), None)
        if finite is None:
            raise ValueError("finite boundary class has no representative in the declared domain")
        values["finite"] = _quantize(finite, surface)
    if "zero" in surface.boundary_classes and "zero" in special:
        values["zero"] = special["zero"]
    if "infinite" in surface.boundary_classes and "infinite" in special:
        values["infinite"] = special["infinite"]
    if "quantized_neighbor" in surface.boundary_classes:
        values["quantized_neighbor"] = _quantize(minimum + step, surface)
    declared = surface.boundary_classes or ["min", "max"]
    result = []
    for cls in declared:
        if cls not in values:
            raise ValueError(f"unsupported boundary class: {cls}")
        result.append((cls, values[cls]))
    return result


def M01_boundary_pick(input, surface, rng, context=None):
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M01")
    context = _context(context)
    before = snapshot(input)
    path = surface.input_paths[0]
    values = _boundary_values(surface)
    requested = context.get("boundary_class")
    if requested is not None:
        selected = [(cls, value) for cls, value in values if cls == requested]
        if not selected:
            raise ValueError(f"requested boundary class is not declared: {requested}")
    else:
        selected = values
    cls, value = selected[rng.randrange(len(selected))]
    return _proposal(surface, "M01", {"boundary_class": cls}, before, {path: value}, context,
                     notes={"selected_class": cls})


def M02_enum_replace(input, surface, rng, context=None):
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M02")
    context = _context(context)
    before = snapshot(input)
    values = surface.domain.get("values")
    if not isinstance(values, list) or not values:
        raise ValueError("enum surface requires a non-empty domain.values list")
    path = surface.input_paths[0]
    try:
        current = _get_path(before, path)
    except ValueError:
        current = surface.domain.get("default")
    candidates = [value for value in values if value != current]
    if not candidates:
        return _proposal(surface, "M02", {"enum_size": len(values)}, before, {}, context,
                         notes={"reason": "single_value_domain"}, status="no_op")
    value = candidates[rng.randrange(len(candidates))]
    return _proposal(surface, "M02", {"enum_size": len(values)}, before, {path: value}, context)


def M03_range_sample(input, surface, rng, context=None):
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M03")
    context = _context(context)
    before = snapshot(input)
    domain = _numeric_domain(surface)
    step = _step(surface)
    offset = rng.randrange(int(math.floor((domain["max"] - domain["min"]) / step)) + 1)
    raw = domain["min"] + offset * step
    value = min(max(_quantize(raw, surface), domain["min"]), domain["max"])
    path = surface.input_paths[0]
    return _proposal(surface, "M03", {"sampling": "uniform", "step": step}, before,
                     {path: value}, context)


MUTATORS = {"M01": M01_boundary_pick, "M02": M02_enum_replace, "M03": M03_range_sample}
# Declared future slice.  These identifiers are intentionally NOT registered.
TODO_MUTATORS = ("M04", "M05", "M06", "M07", "M08")


def mutate(input, surface, rng, context=None, mutator_id=None):
    """Dispatch to a pure mutator.

    If mutator_id is omitted, the surface must declare exactly one mutator.
    """
    surface = _surface(surface)
    if mutator_id is None:
        if len(surface.mutator_ids) != 1:
            raise ValueError("surface declares multiple mutators; select one explicitly")
        mutator_id = surface.mutator_ids[0]
    function = MUTATORS.get(mutator_id)
    if function is None:
        raise ValueError(f"unknown or not implemented mutator: {mutator_id}")
    _spec(surface, mutator_id)
    return function(input, surface, _require_rng(rng), context)


def make_rng(rng_seed, rng_algorithm=RNG_ALGORITHM):
    import random
    if rng_algorithm != RNG_ALGORITHM:
        raise ValueError("unsupported rng algorithm")
    if type(rng_seed) is not int or rng_seed < 0:
        raise ValueError("rng_seed must be a non-negative integer")
    return random.Random(rng_seed)
