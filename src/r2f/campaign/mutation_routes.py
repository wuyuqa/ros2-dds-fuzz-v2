"""Opt-in, target-bound input routing metadata; never execution evidence.

Routes do not participate in content identity. A canonical corpus contains one
payload, while the scheduler selects an ephemeral route view of that payload.
All validation here is static: runtime guards, effect, source/lock qualification
and candidate admission remain the existing runner's responsibility.
"""
from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import NamedTuple
import re

ROUTES_SCHEMA = "r2f.mutation-routes/v1"
ROUTE_KEYS = ("surface_id", "boundary_class", "mutator_id")


class _RouteSurface(NamedTuple):
    executable: bool
    boundary_classes: frozenset
    mutator_ids: frozenset


class _RouteTarget(NamedTuple):
    target_id: str
    revision: int
    contract_hash: str | None
    surfaces: object


# Only immutable projections live here, never caller-supplied models or hashes.
_ROUTE_SCOPE_EMPTY = object()
_ROUTE_VALIDATION_TARGETS = ContextVar("r2f_route_validation_targets", default=None)


def _authoritative_route_targets():
    from r2f.modules.s1.targets import exploratory_target_declarations
    result = {}
    for key, target in exploratory_target_declarations().items():
        contract = target.exploration_contract
        surfaces = {} if contract is None else {
            key: _RouteSurface(surface.executable, frozenset(surface.boundary_classes),
                               frozenset(surface.mutator_ids))
            for key, surface in contract.surface_by_id.items()
        }
        result[key] = _RouteTarget(target.target_id, target.target_revision,
                                  None if contract is None else contract.contract_hash(),
                                  MappingProxyType(surfaces))
    return MappingProxyType(result)


@contextmanager
def _route_validation_scope():
    """At most one fresh lazy snapshot per validation, including nested calls.

    Yield no writable model or directory. The token restores a parent scope on
    success or failure; separate thread/task contexts cannot overwrite it.
    """
    token = _ROUTE_VALIDATION_TARGETS.set(_ROUTE_SCOPE_EMPTY)
    try:
        yield
    finally:
        _ROUTE_VALIDATION_TARGETS.reset(token)


def routes_enabled(profile):
    composition = profile.get("composition", {})
    if not isinstance(composition, dict):
        # Leave the legacy composition validator/shape alone if not opted in.
        return False
    if "mutation_routes_schema" not in composition:
        return False
    if composition["mutation_routes_schema"] != ROUTES_SCHEMA:
        raise ValueError("unsupported mutation_routes_schema")
    if profile.get("runtime_capability") is not None:
        raise ValueError("mutation_routes require the existing F1 runtime capability")
    return True


def route_key(route):
    return tuple(route[key] for key in ROUTE_KEYS)


def normalize_routes(routes):
    if not isinstance(routes, list) or not routes:
        raise ValueError("mutation_routes must be a nonempty list")
    unique = {}
    for route in routes:
        if not isinstance(route, dict) or set(route) != set(ROUTE_KEYS):
            raise ValueError("mutation route requires exactly surface_id/boundary_class/mutator_id")
        if any(not isinstance(route[key], str) or not route[key].strip()
               or route[key] != route[key].strip() for key in ROUTE_KEYS):
            raise ValueError("mutation route fields must be nonempty canonical strings")
        unique[route_key(route)] = {key: route[key] for key in ROUTE_KEYS}
    return [unique[key] for key in sorted(unique)]


def has_routes(entry):
    return any(key in entry for key in ("mutation_routes", "mutation_routes_schema", "mutation_route_origins"))


def route_origins(entry, routes):
    """Canonical aliases, each anchored to its own *baseline* content hash.

    Descendants retain these anchors. They never borrow declarations by just
    sharing a seed name, target, surface label, or a profile membership.
    """
    if "final_config" not in entry:
        return []  # declaration-only validation, before template materialization
    from .corpus import content_identity
    origins = entry.get("mutation_route_origins")
    if origins is None:
        origins = [{"seed_id": entry.get("seed_id"),
                    "variant_id": entry.get("variant_id") or entry.get("seed_id"),
                    "baseline_content_sha256": content_identity(entry),
                    "mutation_routes": routes}]
    if not isinstance(origins, list) or not origins:
        raise ValueError("mutation route origins must be a nonempty list")
    merged = {}
    keys = {"seed_id", "variant_id", "baseline_content_sha256", "mutation_routes"}
    for origin in origins:
        if not isinstance(origin, dict) or set(origin) != keys:
            raise ValueError("mutation route origin has missing/unknown fields")
        if any(not isinstance(origin[key], str) or not origin[key].strip()
               or origin[key] != origin[key].strip() for key in ("seed_id", "variant_id")):
            raise ValueError("mutation route origin requires canonical seed/variant ids")
        if not isinstance(origin["baseline_content_sha256"], str) or not re.fullmatch(
                r"[0-9a-f]{64}", origin["baseline_content_sha256"]):
            raise ValueError("mutation route origin requires a baseline content hash")
        key = (origin["seed_id"], origin["variant_id"], origin["baseline_content_sha256"])
        values = merged.get(key, {}).get("mutation_routes", []) + normalize_routes(origin["mutation_routes"])
        merged[key] = {**deepcopy(origin), "mutation_routes": normalize_routes(values)}
    result = [merged[key] for key in sorted(merged)]
    union = normalize_routes([route for origin in result for route in origin["mutation_routes"]])
    if union != routes:
        raise ValueError("mutation route union differs from origin declarations")
    if not entry.get("parent_content_sha256"):
        if any(origin["baseline_content_sha256"] != content_identity(entry) for origin in result):
            raise ValueError("mutation route origin belongs to a different baseline content")
    return result


def _validate_profile_origins(entry, origins, cycle, profile):
    from .corpus import content_identity
    from r2f.modules.s1 import fuzz_catalog
    seed_map = fuzz_catalog.load_seeds()["seeds"]
    for origin in origins:
        specs = [spec for spec in cycle["seed_specs"]
                 if spec["seed_id"] == origin["seed_id"] and spec["variant_id"] == origin["variant_id"]]
        if len(specs) != 1:
            raise ValueError("mutation route origin not in selected profile cycle")
        spec = specs[0]
        template = seed_map.get(spec["seed_id"])
        if template is None:
            raise ValueError("mutation route origin template is unavailable")
        baseline = {key: entry[key] for key in ("target_id", "target_revision", "rmw", "contract_hash")}
        baseline.update(profile_id=profile["profile_id"], final_config={
            **deepcopy(template["logical_config"]), **deepcopy(spec["overrides"]["logical_config"])})
        if content_identity(baseline) != origin["baseline_content_sha256"]:
            raise ValueError("mutation route origin baseline differs from profile/template payload")
        if any(route not in spec["mutation_routes"] for route in origin["mutation_routes"]):
            raise ValueError("mutation route origin exceeds its exact baseline declaration")


def validate_route_entry(entry, *, contract=None, profile=None):
    """Validate current projected contract, runtime binding and profile scopes.

    This deliberately does not return an execution authorization/receipt. New
    routes are F1-only; pending and opt-in F2/topology carriers stay untouched.
    """
    if not has_routes(entry):
        if profile is not None and routes_enabled(profile):
            raise ValueError("mutation_routes profile requires routed corpus records")
        return None
    if entry.get("mutation_routes_schema") != ROUTES_SCHEMA:
        raise ValueError("unsupported or missing mutation_routes_schema")
    routes = normalize_routes(entry.get("mutation_routes"))
    origins = route_origins(entry, routes)
    from r2f.core.mutators import MUTATORS
    from .runner import _F1_CYCLE_TARGETS
    targets = _ROUTE_VALIDATION_TARGETS.get()
    if targets is None or targets is _ROUTE_SCOPE_EMPTY:
        scoped = targets is _ROUTE_SCOPE_EMPTY
        targets = _authoritative_route_targets()
        if scoped:
            _ROUTE_VALIDATION_TARGETS.set(targets)
    target = targets.get(entry.get("target_id"))
    if target is None or target.target_id not in _F1_CYCLE_TARGETS:
        raise ValueError("mutation route target has no F1 runtime capability")
    if (target.contract_hash is None or type(entry.get("target_revision")) is not int
            or entry["target_revision"] != target.revision
            or entry.get("contract_hash") != target.contract_hash
            or contract is not None and contract.contract_hash() != target.contract_hash):
        raise ValueError("mutation route target revision/contract mismatch")
    if entry.get("runtime_capability") is not None:
        raise ValueError("mutation route runtime capability mismatch")
    for route in routes:
        surface = target.surfaces.get(route["surface_id"])
        if surface is None:
            raise ValueError("mutation route surface not in target contract: " + route["surface_id"])
        if not surface.executable:
            raise ValueError("mutation route surface has no runtime binding")
        if route["boundary_class"] not in surface.boundary_classes:
            raise ValueError("mutation route boundary not in projected contract")
        local_g01_m08 = (target.target_id == "discovery.match_compatibility.fuzz_v1"
                         and target.revision == 1 and route["surface_id"] == "G01"
                         and route["mutator_id"] == "M08")
        if route["mutator_id"] not in surface.mutator_ids or (route["mutator_id"] not in MUTATORS and not local_g01_m08):
            raise ValueError("mutation route mutator not in implemented projected contract")
    if profile is not None:
        if not routes_enabled(profile):
            raise ValueError("routed corpus requires explicit profile opt-in")
        if entry.get("target_id") not in profile.get("target_ids", []):
            raise ValueError("mutation route target not in profile")
        if entry.get("rmw") not in profile.get("rmws", []):
            raise ValueError("mutation route RMW not in profile")
        from .corpus import _profile_cycles
        cycles = [cycle for cycle in _profile_cycles(profile)
                  if cycle["target_id"] == entry["target_id"] and
                  (entry.get("cycle_id") is None or cycle["cycle_id"] == entry["cycle_id"])]
        if len(cycles) != 1:
            raise ValueError("mutation route requires one target-bound profile cycle")
        cycle = cycles[0]
        if origins:
            _validate_profile_origins(entry, origins, cycle, profile)
        declared = normalize_routes([route for spec in cycle["seed_specs"]
                                     if origins or spec["seed_id"] == entry.get("seed_id")
                                     for route in spec["mutation_routes"]])
        for route in routes:
            if (route["surface_id"] not in profile.get("surfaces", [])
                    or route["surface_id"] not in cycle["surfaces"]):
                raise ValueError("mutation route surface not in profile/cycle")
            if route not in declared:
                raise ValueError("mutation route not declared for baseline template")
    return routes


def canonical_route_record(entry):
    """Normalize metadata without changing payload, hash or execution flags."""
    record = deepcopy(entry)
    routes = validate_route_entry(record)
    if routes is not None:
        record["mutation_routes"] = routes
        origins = route_origins(record, routes)
        record["mutation_route_origins"] = origins
        if origins:
            record.update(seed_id=origins[0]["seed_id"], variant_id=origins[0]["variant_id"])
        record.update(routes[0])
        record.pop("mutation_route", None)  # ephemeral scheduler view only
    return record


def merge_routes(existing, incoming):
    """Union metadata on one content node; do not promote execution evidence."""
    if not has_routes(incoming):
        return
    # Both records were validated by the corpus. Legacy labels are not an
    # implicit route declaration; only explicitly supplied routes are unioned.
    existing_routed = has_routes(existing)
    origins = (route_origins(existing, existing["mutation_routes"]) if existing_routed else [])
    origins += route_origins(incoming, incoming["mutation_routes"])
    routes = normalize_routes(existing.get("mutation_routes", []) + incoming["mutation_routes"])
    existing.update(mutation_routes_schema=ROUTES_SCHEMA, mutation_routes=routes,
                    mutation_route_origins=origins)
    existing["mutation_route_origins"] = route_origins(existing, routes)
    existing.update(routes[0])
    # Equal-content seed aliases have a stable representative, not first-wins.
    if has_routes(incoming):
        representative = min((existing, incoming) if existing_routed else (incoming,),
                             key=lambda row: (row.get("seed_id") or "", row.get("variant_id") or ""))
        for key in ("seed_id", "variant_id", "seed_content_sha256"):
            if key in representative:
                existing[key] = representative[key]


def route_view(entry, route):
    routes = validate_route_entry(entry)
    if routes is None or route not in routes:
        raise ValueError("selected mutation route is not declared")
    view = deepcopy(entry)
    view.update(route)
    view["mutation_route"] = deepcopy(route)
    return view


def validate_route_selection(entry, selection, *, profile=None):
    routes = validate_route_entry(entry, profile=profile)
    if routes is None:
        return
    selected = selection.get("mutation_route")
    if (selected not in routes or entry.get("mutation_route") != selected
            or {key: entry.get(key) for key in ROUTE_KEYS} != selected
            or selection.get("route_selection", {}).get("selected") != selected):
        raise ValueError("selected mutation route/view differs from scheduler event")


def inherit_routes(parent, child):
    """Keep the baseline's route domain on admitted descendants, not its view."""
    if has_routes(parent):
        child.update(mutation_routes_schema=ROUTES_SCHEMA,
                     mutation_routes=deepcopy(parent["mutation_routes"]),
                     mutation_route_origins=deepcopy(parent["mutation_route_origins"]),
                     variant_id=parent.get("variant_id"), cycle_id=parent.get("cycle_id"))
    return child
