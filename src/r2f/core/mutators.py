"""Pure mutator library: M01-M08.

The *default registry* remains M01-M03 so the already-reviewed F1 contracts and
their hashes do not change.  M04-M08 are implemented here but deliberately
unregistered: the default ``mutate()`` still rejects them, and only the
explicit ``mutate_f2()`` path can generate with them.  That keeps "generatable"
strictly separate from "registered for live execution".

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

#: Implemented but intentionally unregistered.  They are reachable only through
#: ``mutate_f2`` and never participate in the default contract/runner path.
F2_MUTATOR_SPECS = {
    "M04": MutatorSpec("M04", 1, ("relation",), "relation_boundary",
                       "pick a declared depth/count relation class and values",
                       ),
    "M05": MutatorSpec("M05", 1, ("parameter_numeric",), "special_value",
                       "pick a declared zero/finite/infinite class, never aliasing infinity",
                       ),
    "M06": MutatorSpec("M06", 1, ("parameter_timing", "parameter_numeric", "environment"),
                       "timing_offset", "offset from a declared event anchor without negative time",
                       ),
    "M07": MutatorSpec("M07", 1, ("environment",), "environment_windows",
                       "build 1-3 independent begin/end/restore runtime windows",
                       ),
    "M08": MutatorSpec("M08", 1, ("compatibility",), "compatibility_pair",
                       "enumerate offered/requested reliability pairs and the guard expectation",
                       ),
}
ALL_MUTATOR_SPECS = dict(MUTATOR_SPECS)
ALL_MUTATOR_SPECS.update(F2_MUTATOR_SPECS)


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


def _spec(surface, mutator_id, specs=None):
    specs = MUTATOR_SPECS if specs is None else specs
    spec = specs.get(mutator_id)
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


def _proposal(surface, mutator_id, parameters, before, changes, context, notes=None,
              status="applicable", applicability=None, runtime_windows=None):
    after = _apply_changes(before, changes)
    phase = context.get("application_phase") or "compile_time"
    if phase not in APPLICATION_PHASES:
        raise ValueError(f"unsupported application_phase: {phase}")
    merged = {"status": status}
    if applicability:
        merged.update(snapshot(applicability))
    if surface.sut_guards:
        merged["sut_guards"] = snapshot(surface.sut_guards)
    return MutationProposal(
        surface_id=surface.surface_id, mutator_id=mutator_id,
        parameters=snapshot(parameters), changes=snapshot(changes),
        input_before=before, input_after=after, application_phase=phase,
        applicability_result=merged, runtime_windows=list(runtime_windows or []),
        notes=snapshot(notes or {}),
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
        # For a finite seed, a finite parent value is itself a valid boundary
        # witness.  Keep it so authored variants are not collapsed to one
        # representative.
        try:
            current = _get_path(before, path)
        except ValueError:
            current = None
        if requested == "finite" and type(current) in (int, float) and current > 0:
            selected = [(requested, current)]
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
    path = surface.input_paths[0]
    # Range sampling stays local to the parent: pick a bounded neighbourhood
    # around the seed value instead of ignoring the authored parent entirely.
    current = _get_path(before, path) if isinstance(path, str) else None
    span_steps = max(1, int(round((domain["max"] - domain["min"]) / step)))
    # Local neighbourhood only for a genuinely narrow field; a wide domain
    # (e.g. nanoseconds) must still explore the declared range.
    if type(current) in (int, float) and not (current == 0 and span_steps > 1_000_000):
        # Neighbourhood relative to the parent, capped by a small absolute
        # fraction of the current value (and a few steps for tiny values) so a
        # 0.5 s window is not randomly rewritten to 0.07 s.
        relative = max(1, int(abs(current) / step * 0.05)) if step else 1
        domain_relative = max(1, int(span_steps * 0.01))
        max_delta = max(1, min(max(relative, domain_relative), span_steps, 4096))
        delta_steps = rng.randrange(-max_delta, max_delta + 1)
        raw = current + delta_steps * step
    else:
        # Deterministic coverage over the declared grid for a missing parent.
        # The RNG chooses a bucket; the seed sequence walks distinct buckets so
        # a small seed budget still exercises distinct values.
        span = int(math.floor((domain["max"] - domain["min"]) / step))
        buckets = max(1, min(64, span + 1))
        base = rng.randrange(buckets)
        sequence = int(context.get("seed_index", 0))
        offset = (base + sequence) % (span + 1)
        raw = domain["min"] + offset * step
    value = min(max(_quantize(raw, surface), domain["min"]), domain["max"])
    return _proposal(surface, "M03", {"sampling": "local_bounded", "step": step}, before,
                     {path: value}, context)


def _relation_short(surface, requested=None):
    declared, by_label = [], {}
    for cls in surface.boundary_classes:
        for short in ("lt", "eq", "gt"):
            if f"_{short}_" in cls or cls.endswith("_" + short):
                if short not in declared:
                    declared.append(short)
                by_label[cls] = short
    if requested is not None:
        if requested in ("lt", "eq", "gt"):
            short = requested
        elif requested in by_label:
            short = by_label[requested]
        elif any(token in requested for token in ("_lt_", "_eq_", "_gt_")) or requested.endswith(
                ("_lt", "_eq", "_gt")):
            raise ValueError(
                f"relation class is not declared by surface {surface.surface_id}: {requested}")
        else:
            raise ValueError(f"unsupported relation class: {requested}")
        if declared and short not in declared:
            raise ValueError(
                f"relation class is not declared by surface {surface.surface_id}: {requested}")
        return short
    if not declared:
        declared = ["lt", "eq", "gt"]
    return declared


def _relation_label(surface, short):
    for cls in surface.boundary_classes:
        if f"_{short}_" in cls or cls.endswith("_" + short):
            return cls
    return short


def _relation_candidates(surface, short):
    domain = surface.domain
    depths = [v for v in domain.get("depth", []) if type(v) is int and v > 0]
    posts = [v for v in domain.get("post_count", []) if type(v) is int and v > 0]
    pres = [v for v in domain.get("pre_count", []) if type(v) is int and v >= 0]
    pairs = []
    for depth in depths:
        for post in posts:
            if short == "lt" and depth < post:
                pairs.append((depth, post))
            elif short == "eq" and depth == post:
                pairs.append((depth, post))
            elif short == "gt" and depth > post:
                pairs.append((depth, post))
    return pairs, pres


def M04_relation_boundary(input, surface, rng, context=None):
    """W01 depth/count relation boundary.  Never substitutes uniform sampling."""
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M04", F2_MUTATOR_SPECS)
    context = _context(context)
    before = snapshot(input)
    requested = context.get("relation_class") or context.get("relation")
    shorts = _relation_short(surface, requested)
    if isinstance(shorts, str):
        shorts = [shorts]
    else:
        # Rotate instead of shuffle so any rng exposing randrange works.
        start = rng.randrange(len(shorts))
        shorts = shorts[start:] + shorts[:start]
    chosen = None
    for short in shorts:
        pairs, pres = _relation_candidates(surface, short)
        if pairs:
            chosen = (short, pairs, pres)
            break
    if chosen is None:
        return _proposal(surface, "M04", {"relation_class": None}, before, {}, context,
                         notes={"reason": "no_declared_relation_witness"},
                         status="unsupported",
                         applicability={"relation_class": None, "declared_only": True})
    short, pairs, pres = chosen
    depth, post = pairs[rng.randrange(len(pairs))]
    eligible_pre = [p for p in pres if p <= depth] or pres or [0]
    pre = eligible_pre[rng.randrange(len(eligible_pre))]
    label = _relation_label(surface, short)
    changes = {"qos.depth": depth, "pre_match_count": pre, "post_match_count": post}
    return _proposal(surface, "M04",
                     {"relation_class": label, "relation": short,
                      "depth": depth, "pre_count": pre, "post_count": post},
                     before, changes, context,
                     notes={"relation": short, "relation_class": label,
                            "depth_is_not_a_sedp_wire_field": True,
                            "delivery_obligation_not_inferred": True},
                     applicability={"relation_class": label, "relation": short,
                                    "witness": {"depth": depth, "post_count": post}})


def _special_map(surface):
    values = surface.special_values
    if isinstance(values, dict):
        return dict(values)
    result = {}
    if isinstance(values, list):
        for item in values:
            if not isinstance(item, dict) or "name" not in item or "value" not in item:
                raise ValueError("special_values entries require name and value")
            result[item["name"]] = item["value"]
    return result


def M05_special_value(input, surface, rng, context=None):
    """Pick a declared zero/finite/infinite class; infinity is never a big finite."""
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M05", F2_MUTATOR_SPECS)
    context = _context(context)
    before = snapshot(input)
    specials = _special_map(surface)
    classes = set(surface.boundary_classes or ["zero", "finite", "infinite"])
    # ``finite`` may be declared as explicit representatives instead of a single
    # special value.  Derive from the declaration only; never synthesise an
    # arbitrary finite witness and never fold it into zero/infinite.
    finite_reps = [v for v in surface.domain.get("finite_representatives", [])
                   if type(v) in (int, float) and type(v) is not bool and v > 0]
    if finite_reps and "finite" in classes:
        specials["finite"] = finite_reps
    declared = [name for name in ("zero", "finite", "infinite")
                if name in specials and name in classes]
    requested = context.get("special_class")
    if requested is not None:
        if requested not in ("zero", "finite", "infinite"):
            raise ValueError(f"unsupported special class: {requested}")
        if requested not in declared:
            raise ValueError(
                f"special class {requested} is not declared by surface {surface.surface_id}")
        chosen = requested
    else:
        if not declared:
            raise ValueError(f"surface {surface.surface_id} declares no special value classes")
        chosen = declared[rng.randrange(len(declared))]
    derived_finite = chosen == "finite" and isinstance(specials.get("finite"), list)
    if derived_finite:
        reps = specials["finite"]
        value = reps[rng.randrange(len(reps))]
    else:
        value = specials[chosen]
    if type(value) not in (int, float) or type(value) is bool:
        raise ValueError(f"special class {chosen} has a non-numeric value")
    if value < 0:
        raise ValueError(f"special class {chosen} must not be negative")
    path = surface.input_paths[0]
    return _proposal(surface, "M05",
                     {"special_class": chosen, "declared_value": value,
                      "declared_classes": declared},
                     before, {path: value}, context,
                     notes={"infinite_is_not_a_large_finite_number": chosen == "infinite",
                            "class_from_declaration": True,
                            "finite_witness_derived_from_declared_representatives": derived_finite},
                     applicability={"special_class": chosen, "declared_value": value})


def _nonnegative(value, field):
    if type(value) not in (int, float) or type(value) is bool or value != value:
        raise ValueError(f"{field} must be a finite number")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    if value == float("inf"):
        raise ValueError(f"{field} must be finite")
    return value


def M06_timing_offset(input, surface, rng, context=None):
    """Offset from a declared event anchor; never produces negative time."""
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M06", F2_MUTATOR_SPECS)
    context = _context(context)
    before = snapshot(input)
    domain = surface.domain
    anchors = list(domain.get("anchors") or ["fresh_match"])
    anchor = context.get("anchor") or anchors[rng.randrange(len(anchors))]
    if anchor not in anchors:
        raise ValueError(f"anchor {anchor} is not declared by surface {surface.surface_id}")
    observed = context.get("anchor_observed") is True
    if surface.surface_id == "E03" or len(surface.input_paths) > 1:
        offsets = list(domain.get("offsets_s") or [0.0])
        offset = context.get("offset_sec")
        offset = offsets[rng.randrange(len(offsets))] if offset is None else offset
        offset = _nonnegative(offset, "offset_sec")
        changes = {"environment_windows.anchor": anchor,
                   "environment_windows.anchor_offset_sec": offset}
    else:
        path = surface.input_paths[0]
        representatives = list(domain.get("representatives") or [0.0])
        offset = context.get("offset_sec")
        offset = representatives[rng.randrange(len(representatives))] if offset is None else offset
        offset = _nonnegative(offset, "offset_sec")
        try:
            current = _get_path(before, path)
        except ValueError:
            current = 0.0
        if type(current) not in (int, float) or type(current) is bool:
            current = 0.0
        value = current + offset
        minimum = domain.get("min")
        maximum = domain.get("max")
        if type(minimum) in (int, float):
            value = max(value, minimum)
        if type(maximum) in (int, float):
            value = min(value, maximum)
        value = _nonnegative(value, path)
        changes = {path: value}
    status = "applicable" if observed else "needs_observation"
    return _proposal(surface, "M06",
                     {"anchor": anchor, "offset_sec": offset, "unit": "s",
                      "anchor_observed": observed},
                     before, changes, context,
                     notes={"anchor_must_be_observed_before_offset": True,
                            "anchor_observation_supplied": observed,
                            "negative_time_not_generated": True},
                     status=status,
                     applicability={"anchor": anchor, "offset_sec": offset, "unit": "s",
                                    "anchor_observed": observed})


def _declared_ints(surface, key):
    values = surface.domain.get(key)
    if not isinstance(values, list) or not values:
        raise ValueError(f"surface {surface.surface_id} requires domain.{key}")
    result = []
    for value in values:
        if type(value) is not int or value < 0:
            raise ValueError(f"domain.{key} must contain non-negative integers")
        result.append(value)
    return result


def M07_environment_windows(input, surface, rng, context=None):
    """Build 1-3 independent begin/end/restore windows (planned, not executed)."""
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M07", F2_MUTATOR_SPECS)
    context = _context(context)
    before = snapshot(input)
    counts = _declared_ints(surface, "window_counts") if "window_counts" in surface.domain else [1, 2, 3]
    counts = [c for c in counts if 1 <= c <= 3]
    if not counts:
        raise ValueError("window_counts must contain a value in 1..3")
    requested = context.get("window_count")
    if requested is not None:
        if type(requested) is not int or requested not in counts:
            raise ValueError(f"window_count {requested} is not declared by surface {surface.surface_id}")
        count = requested
    else:
        count = counts[rng.randrange(len(counts))]
    max_windows = context.get("max_windows", 3)
    if type(max_windows) is not int or not 1 <= max_windows <= 3:
        raise ValueError("max_windows must be an integer in 1..3")
    count = min(count, max_windows)

    if "restore_interval_representatives_s" in surface.domain:
        intervals = list(surface.domain["restore_interval_representatives_s"])
    else:
        try:
            intervals = [_get_path(before, "environment_windows.restore_interval_sec")]
        except ValueError:
            intervals = [0.0]
    for interval in intervals:
        _nonnegative(interval, "restore_interval_sec")
    restore_interval = context.get("restore_interval_sec")
    restore_interval = intervals[rng.randrange(len(intervals))] if restore_interval is None else restore_interval
    restore_interval = _nonnegative(restore_interval, "restore_interval_sec")

    anchors = list(surface.domain.get("anchors") or ["fresh_match"])
    base_anchor = context.get("anchor") or ("fresh_match" if "fresh_match" in anchors else anchors[0])
    if base_anchor not in anchors:
        raise ValueError(f"anchor {base_anchor} is not declared by surface {surface.surface_id}")
    try:
        drop_duration = _get_path(before, "drop_duration_sec")
    except ValueError:
        drop_duration = 0.2
    _nonnegative(drop_duration, "drop_duration_sec")
    if "offsets_s" in surface.domain:
        offsets = list(surface.domain["offsets_s"])
        anchor_offset = context.get("offset_sec")
        anchor_offset = offsets[rng.randrange(len(offsets))] if anchor_offset is None else anchor_offset
    else:
        anchor_offset = context.get("offset_sec", 0.0)
    anchor_offset = _nonnegative(anchor_offset, "anchor_offset_sec")

    windows, planned = [], []
    for index in range(1, count + 1):
        trigger_anchor = base_anchor if index == 1 else "previous_restore_confirmed"
        trigger_offset = anchor_offset if index == 1 else 0.0
        windows.append({
            "schema": "r2f.runtime-window/v1",
            "window_id": f"post_match_window_{index}",
            "window_index": index,
            "trigger_anchor": trigger_anchor,
            "trigger_offset_sec": trigger_offset,
            "drop_duration_sec": drop_duration,
            "restore_interval_sec": restore_interval,
        })
        planned.append({
            "window_id": f"post_match_window_{index}",
            "window_index": index,
            "begin": True,
            "end": True,
            "restore": True,
            "requires_previous_restore": index > 1,
            "trigger_anchor": trigger_anchor,
            "trigger_offset_sec": trigger_offset,
        })

    changes = {"environment_windows.count": count,
               "environment_windows.restore_interval_sec": restore_interval,
               "environment_windows.windows": planned}
    if surface.surface_id == "E03":
        changes["environment_windows.anchor"] = base_anchor
        changes["environment_windows.anchor_offset_sec"] = anchor_offset
    observed = context.get("post_match_observed") is True
    status = "applicable" if observed else "needs_observation"
    return _proposal(surface, "M07",
                     {"window_count": count, "restore_interval_sec": restore_interval,
                      "anchor": base_anchor, "anchor_offset_sec": anchor_offset,
                      "unit": "s"},
                     before, changes, context,
                     notes={"planned_not_executed": True,
                            "each_window_has_independent_begin_end_restore": True,
                            "post_match_observation_supplied": observed},
                     status=status,
                     applicability={"window_count": count, "planned": True},
                     runtime_windows=windows)


def M08_compatibility_pair(input, surface, rng, context=None):
    """Enumerate offered/requested reliability; creation success is never a bug."""
    _require_rng(rng)
    surface = _surface(surface)
    _spec(surface, "M08", F2_MUTATOR_SPECS)
    context = _context(context)
    before = snapshot(input)
    pairs = list(surface.domain.get("pairs") or [])
    if not pairs:
        raise ValueError(f"surface {surface.surface_id} requires domain.pairs")
    incompatible = set(surface.domain.get("incompatible_pairs") or [])
    requested = context.get("pair_relation")
    if requested is not None:
        if requested not in pairs:
            raise ValueError(f"pair_relation {requested} is not declared by surface {surface.surface_id}")
        pair = requested
    else:
        pair = pairs[rng.randrange(len(pairs))]
    if len(pair) != 2 or any(ch not in "RB" for ch in pair):
        raise ValueError(f"invalid reliability pair {pair}")
    mapping = {"R": "reliable", "B": "best_effort"}
    offered, requested_qos = mapping[pair[0]], mapping[pair[1]]
    is_incompatible = pair in incompatible
    changes = {"writer_qos.reliability": offered,
               "reader_qos.reliability": requested_qos,
               "pair_relation": pair}
    return _proposal(surface, "M08",
                     {"pair_relation": pair, "offered": offered, "requested": requested_qos,
                      "incompatible": is_incompatible},
                     before, changes, context,
                     notes={"creation_success_is_not_a_violation": True,
                            "creation_and_match_are_separate_observations": True,
                            "guard_expectation": "no_match" if is_incompatible else "match"},
                     applicability={"pair_relation": pair,
                                    "compatibility": "incompatible" if is_incompatible else "compatible",
                                    "guard_expectation": "no_match" if is_incompatible else "match",
                                    "creation_success_is_not_a_bug": True})


MUTATORS = {"M01": M01_boundary_pick, "M02": M02_enum_replace, "M03": M03_range_sample}
# Implemented but intentionally NOT registered in the default dispatcher.
F2_MUTATORS = {"M04": M04_relation_boundary, "M05": M05_special_value,
               "M06": M06_timing_offset, "M07": M07_environment_windows,
               "M08": M08_compatibility_pair}
# Kept for callers that introspect the legacy "future slice" constant.
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


def mutate_f2(input, surface, rng, context=None, mutator_id=None):
    """Explicit unregistered-generation path for M04-M08.

    This function never changes the default registry: ``mutate()`` still
    rejects M04-M08.  It also opens no socket and records no execution.
    """
    surface = _surface(surface)
    if mutator_id is None:
        implemented = [mid for mid in surface.mutator_ids if mid in F2_MUTATORS]
        if len(implemented) != 1:
            raise ValueError("surface requires an explicit F2 mutator id")
        mutator_id = implemented[0]
    function = F2_MUTATORS.get(mutator_id)
    if function is None:
        raise ValueError(f"unknown or not implemented F2 mutator: {mutator_id}")
    _spec(surface, mutator_id, F2_MUTATOR_SPECS)
    return function(input, surface, _require_rng(rng), context)


def make_rng(rng_seed, rng_algorithm=RNG_ALGORITHM):
    import random
    if rng_algorithm != RNG_ALGORITHM:
        raise ValueError("unsupported rng algorithm")
    if type(rng_seed) is not int or rng_seed < 0:
        raise ValueError("rng_seed must be a non-negative integer")
    return random.Random(rng_seed)


# ---------------------------------------------------------------------------
# Wide sampling surface (T-B)
#
# ``mutate_wide`` is an independent sampling namespace layered on top of the
# registered mutators.  It never widens ``MUTATOR_SPECS``, never changes
# ``mutate()`` dispatch, and never promises execution: a proposal is still an
# input document only.  Every numeric class is bounded by the surface's own
# declared ``domain``; an undeclared domain is rejected instead of guessed.
# ---------------------------------------------------------------------------

WIDE_SAMPLING_KINDS = ("boundary", "representative", "log_uniform",
                       "linear_uniform", "neighbor", "jump")
WIDE_MAX_CHAIN_LENGTH = 4


def _wide_error(surface, detail):
    return ValueError(f"surface {surface.surface_id}: {detail}")


def _tag_sampling(proposal, sampling_kind):
    """Return a copy of ``proposal`` whose parameters/notes name the sampler."""
    parameters = dict(proposal.parameters)
    parameters["sampling_kind"] = sampling_kind
    parameters["wide_sampling"] = True
    notes = dict(proposal.notes)
    notes["sampling_kind"] = sampling_kind
    notes["wide_sampling"] = True
    return MutationProposal(
        surface_id=proposal.surface_id, mutator_id=proposal.mutator_id,
        parameters=parameters, changes=proposal.changes,
        input_before=proposal.input_before, input_after=proposal.input_after,
        application_phase=proposal.application_phase,
        applicability_result=proposal.applicability_result,
        runtime_windows=proposal.runtime_windows, override=proposal.override,
        notes=notes, proposal_id=proposal.proposal_id,
    )


def _wide_numeric_domain(surface):
    domain = surface.domain
    minimum, maximum = domain.get("min"), domain.get("max")
    if type(minimum) not in (int, float) or type(maximum) not in (int, float):
        raise _wide_error(surface, "numeric wide sampling requires declared domain.min/max")
    if type(minimum) is bool or type(maximum) is bool:
        raise _wide_error(surface, "numeric domain bounds must not be booleans")
    if minimum > maximum:
        raise _wide_error(surface, "numeric domain min exceeds max")
    try:
        step = _step(surface)
    except ValueError as exc:
        raise _wide_error(surface, str(exc)) from exc
    return minimum, maximum, step


def _wide_representatives(surface, minimum, maximum):
    declared = []
    raw = surface.domain.get("representatives")
    if isinstance(raw, list):
        declared.extend(raw)
    for item in surface.special_values:
        if isinstance(item, dict):
            declared.append(item.get("value"))
    result = []
    for value in declared:
        if type(value) in (int, float) and type(value) is not bool \
                and value == value and minimum <= value <= maximum:
            if value not in result:
                result.append(value)
    return result


def _parent_numeric(before, surface):
    path = surface.input_paths[0]
    try:
        current = _get_path(before, path)
    except ValueError:
        current = None
    if type(current) not in (int, float) or type(current) is bool:
        return None
    return current


#: Boundary classes M01 can actually honour today.
M01_BOUNDARY_CLASSES = frozenset({
    "min", "min_plus_one", "max_minus_one", "max",
    "finite", "zero", "infinite", "quantized_neighbor",
})


def _m01_boundary_classes(surface):
    if "M01" not in surface.mutator_ids:
        return []
    classes = [cls for cls in (surface.boundary_classes or ["min", "max"])
               if cls in M01_BOUNDARY_CLASSES]
    return classes


def _wide_kinds(surface, before):
    """Sampling classes actually declared for this surface and parent input."""
    kinds = []
    if _m01_boundary_classes(surface):
        kinds.append("boundary")
    try:
        minimum, maximum, step = _wide_numeric_domain(surface)
    except ValueError:
        minimum = maximum = step = None
    if minimum is not None:
        if _wide_representatives(surface, minimum, maximum):
            kinds.append("representative")
        if minimum > 0 and maximum > minimum:
            kinds.append("log_uniform")
        if maximum > minimum:
            kinds.append("linear_uniform")
        current = _parent_numeric(before, surface)
        if current is not None and minimum <= current <= maximum:
            kinds.append("neighbor")
            if _jump_decades(minimum, maximum, step, current):
                kinds.append("jump")
    elif surface.surface_kind == "parameter_enum" and "M02" in surface.mutator_ids:
        kinds.append("representative")
    return kinds


def _jump_decades(minimum, maximum, step, current):
    """Decades inside the declared domain that differ from the parent decade."""
    positive = minimum if minimum > 0 else step
    if positive <= 0 or maximum < positive or current <= 0:
        return []
    low = int(math.ceil(math.log10(positive)))
    high = int(math.floor(math.log10(maximum)))
    parent_decade = int(math.floor(math.log10(current)))
    return [k for k in range(low, high + 1) if k != parent_decade]


def _clamped_quantized(value, minimum, maximum, surface):
    return min(max(_quantize(value, surface), minimum), maximum)


def _wide_linear_uniform(surface, rng, context):
    minimum, maximum, step = _wide_numeric_domain(surface)
    fraction = context.get("fraction")
    if fraction is None:
        fraction = rng.random()
    if type(fraction) not in (int, float) or not 0 <= fraction <= 1:
        raise _wide_error(surface, "linear_uniform fraction must be within [0, 1]")
    value = _clamped_quantized(minimum + float(fraction) * (maximum - minimum), minimum, maximum, surface)
    return {"sampling_kind": "linear_uniform", "wide_sampling": True,
            "min": minimum, "max": maximum,
            "step": step, "fraction": round(float(fraction), 12)}, value


def _wide_log_uniform(surface, rng, context):
    minimum, maximum, step = _wide_numeric_domain(surface)
    if minimum <= 0 or maximum <= minimum:
        raise _wide_error(surface, "log_uniform requires declared min > 0 and max > min")
    fraction = context.get("fraction")
    if fraction is None:
        fraction = rng.random()
    if type(fraction) not in (int, float) or not 0 <= fraction <= 1:
        raise _wide_error(surface, "log_uniform fraction must be within [0, 1]")
    low, high = math.log10(minimum), math.log10(maximum)
    value = _clamped_quantized(10 ** (low + float(fraction) * (high - low)), minimum, maximum, surface)
    return {"sampling_kind": "log_uniform", "wide_sampling": True,
            "min": minimum, "max": maximum,
            "step": step, "fraction": round(float(fraction), 12)}, value


def _wide_neighbor(surface, before, rng, context):
    minimum, maximum, step = _wide_numeric_domain(surface)
    current = _parent_numeric(before, surface)
    if current is None:
        raise _wide_error(surface, "neighbor sampling requires a numeric parent value")
    multiplier = context.get("neighbor_multiplier")
    if multiplier is None:
        multiplier = (1, 2, 4)[rng.randrange(3)]
    if multiplier not in (1, 2, 4):
        raise _wide_error(surface, "neighbor_multiplier must be one of 1, 2, 4")
    direction = context.get("neighbor_direction")
    if direction is None:
        direction = 1 if rng.randrange(2) else -1
    if direction not in (-1, 1):
        raise _wide_error(surface, "neighbor_direction must be -1 or 1")
    value = _clamped_quantized(current + direction * multiplier * step, minimum, maximum, surface)
    parameters = {"sampling_kind": "neighbor", "wide_sampling": True,
                  "min": minimum, "max": maximum,
                  "step": step, "parent": current, "multiplier": multiplier,
                  "direction": direction,
                  "clamped": value == minimum or value == maximum}
    return parameters, value, value == current


def _wide_jump(surface, before, rng, context):
    minimum, maximum, step = _wide_numeric_domain(surface)
    current = _parent_numeric(before, surface)
    if current is None or current <= 0:
        raise _wide_error(surface, "jump sampling requires a positive numeric parent value")
    decades = _jump_decades(minimum, maximum, step, current)
    if not decades:
        raise _wide_error(surface, "jump sampling requires a second declared decade in domain")
    decade = context.get("decade")
    if decade is None:
        decade = decades[rng.randrange(len(decades))]
    if decade not in decades:
        raise _wide_error(surface, f"decade {decade} is not declared inside domain")
    fraction = context.get("fraction")
    if fraction is None:
        fraction = rng.random()
    if type(fraction) not in (int, float) or not 0 <= fraction <= 1:
        raise _wide_error(surface, "jump fraction must be within [0, 1]")
    value = _clamped_quantized(10 ** (decade + float(fraction)), minimum, maximum, surface)
    parameters = {"sampling_kind": "jump", "wide_sampling": True,
                  "min": minimum, "max": maximum,
                  "step": step, "parent": current, "parent_decade":
                      int(math.floor(math.log10(current))),
                  "decade": decade, "fraction": round(float(fraction), 12)}
    return parameters, value, False


def mutate_wide(input, surface, rng, context=None, mutator_id=None):
    """Wide-domain pure sampling on top of the registered mutators.

    Sampling namespaces are explicit (never inferred from ``mutate()``):

    * ``boundary`` reuses the registered M01 boundary classes unchanged.
    * ``representative`` picks a declared representative (or enum value).
    * ``log_uniform`` / ``linear_uniform`` sample the declared numeric domain.
    * ``neighbor`` walks +/- quantum * 1/2/4 around the parent value.
    * ``jump`` moves to a different declared decade.

    M04 (relation boundary) and M05 (special value) are exposed here with their
    F2 semantics; M04 still does not enter the default ``MUTATORS`` table.
    A surface without the requested declaration raises ``ValueError`` naming
    the surface id.  No clock, file, socket, ROS, receipt or hit is involved.
    """
    _require_rng(rng)
    surface = _surface(surface)
    context = _context(context)
    before = snapshot(input)

    if mutator_id in ("M04", "M05"):
        _spec(surface, mutator_id, F2_MUTATOR_SPECS)
        proposal = F2_MUTATORS[mutator_id](before, surface, rng, context)
        return _tag_sampling(proposal, "boundary" if mutator_id == "M04" else "representative")
    if mutator_id is not None and mutator_id not in ("M01", "M02", "M03"):
        raise ValueError(f"unknown wide mutator: {mutator_id}")

    if surface.surface_kind == "relation":
        if "M04" not in surface.mutator_ids:
            raise _wide_error(surface, "relation surface does not declare M04")
        proposal = F2_MUTATORS["M04"](before, surface, rng, context)
        return _tag_sampling(proposal, "boundary")

    if surface.surface_kind == "parameter_enum":
        if mutator_id not in (None, "M02"):
            raise _wide_error(
                surface, f"enum surface only accepts M02, not {mutator_id}")
        if "M02" not in surface.mutator_ids:
            raise _wide_error(surface, "enum surface does not declare M02")
        requested = context.get("sampling_kind")
        if requested not in (None, "representative"):
            raise _wide_error(
                surface, f"enum surface declares only representative, not {requested}")
        proposal = M02_enum_replace(before, surface, rng, context)
        return _tag_sampling(proposal, "representative")

    if surface.surface_kind not in ("parameter_numeric", "parameter_timing"):
        raise _wide_error(surface, f"wide sampling does not accept surface kind {surface.surface_kind}")

    available = _wide_kinds(surface, before)
    requested = context.get("sampling_kind")
    if mutator_id == "M01":
        if requested not in (None, "boundary"):
            raise _wide_error(
                surface, f"M01 only produces boundary samples, not {requested}")
        requested = "boundary"
    if mutator_id == "M02":
        raise ValueError(
            f"mutate_wide: mutator M02 does not accept numeric surface {surface.surface_id}")
    if mutator_id == "M03":
        # The boundary class is M01's declaration; M03 must never emit a
        # proposal labelled M01 through the wide namespace.
        available = [kind for kind in available if kind != "boundary"]
        if requested == "boundary":
            raise _wide_error(surface, "M03 does not produce boundary samples")
    if requested is None:
        if not available:
            raise _wide_error(surface, "no wide sampling class is declared by this surface")
        kind = available[rng.randrange(len(available))]
    else:
        if requested not in WIDE_SAMPLING_KINDS:
            raise ValueError(f"unsupported wide sampling kind: {requested}")
        if requested not in available:
            raise _wide_error(surface, f"wide sampling kind {requested} is not declared")
        kind = requested

    if kind == "boundary":
        classes = _m01_boundary_classes(surface)
        if not classes:
            raise _wide_error(surface, "no M01-honourable boundary class is declared")
        proposal = M01_boundary_pick(before, surface, rng, context)
        return _tag_sampling(proposal, "boundary")

    path = surface.input_paths[0]
    if kind == "representative":
        minimum, maximum, _ = _wide_numeric_domain(surface)
        reps = _wide_representatives(surface, minimum, maximum)
        index = context.get("representative_index")
        if index is None:
            value = reps[rng.randrange(len(reps))]
        else:
            if type(index) is not int or not 0 <= index < len(reps):
                raise _wide_error(surface, "representative_index is outside the declared set")
            value = reps[index]
        parameters = {"sampling_kind": "representative", "wide_sampling": True,
                      "declared_representatives": list(reps), "selected": value}
        return _proposal(surface, "M03", parameters, before, {path: value}, context,
                         notes={"sampling_kind": "representative", "wide_sampling": True})

    if kind == "linear_uniform":
        parameters, value = _wide_linear_uniform(surface, rng, context)
        return _proposal(surface, "M03", parameters, before, {path: value}, context,
                         notes={"sampling_kind": kind, "wide_sampling": True})
    if kind == "log_uniform":
        parameters, value = _wide_log_uniform(surface, rng, context)
        return _proposal(surface, "M03", parameters, before, {path: value}, context,
                         notes={"sampling_kind": kind, "wide_sampling": True})
    if kind == "neighbor":
        parameters, value, unchanged = _wide_neighbor(surface, before, rng, context)
        return _proposal(surface, "M03", parameters, before, {path: value}, context,
                         notes={"sampling_kind": kind, "wide_sampling": True},
                         status="no_op" if unchanged else "applicable")
    if kind == "jump":
        parameters, value, unchanged = _wide_jump(surface, before, rng, context)
        return _proposal(surface, "M03", parameters, before, {path: value}, context,
                         notes={"sampling_kind": kind, "wide_sampling": True},
                         status="no_op" if unchanged else "applicable")
    raise AssertionError("unreachable wide sampling kind: " + kind)
