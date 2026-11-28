"""One S1 identity/family directory, not another semantic model or registry.

Entries describe relationships and opt-in requirements only. The existing
factories remain the sole source of SemanticTarget manifests; listing a gated
entry neither registers it nor claims that its runtime is qualified.
"""
from .targets import (
    EXECUTION_ORDER, EXPLORATORY_TARGET_ORDER, EXPLORATORY_RUNTIME_TARGET,
    F2_WINDOWS_TARGET, F2_WINDOWS_CAPABILITY, F2_WINDOWS_RUNTIME_TARGET,
    target_declarations, exploratory_target_declarations,
)
from .topology_targets import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY


def target_catalog():
    """Return fresh JSON metadata for every declared S1 identity, including opt-ins.

    A family relates versioned contracts; it is not an alias for execution or
    replay. ``runtime_capability`` is the exact opt-in token, not evidence of
    implementation, successful execution, or maturity.
    """
    def entry(target_id, family_id, usage, baseline_id, capability=None):
        return {"target_id": target_id, "family_id": family_id, "usage": usage,
                "conformance_baseline_id": baseline_id,
                "runtime_capability": capability, "requires_opt_in": capability is not None}

    entries = [entry(identifier, identifier, "conformance_baseline", identifier)
               for identifier in EXECUTION_ORDER]
    for identifier in EXPLORATORY_TARGET_ORDER:
        baseline_id = EXPLORATORY_RUNTIME_TARGET[identifier]
        entries.append(entry(identifier, baseline_id or identifier.rsplit(".", 1)[0],
                             "fuzz", baseline_id))
    entries.extend([
        entry(F2_WINDOWS_TARGET, F2_WINDOWS_RUNTIME_TARGET, "fuzz",
              F2_WINDOWS_RUNTIME_TARGET, F2_WINDOWS_CAPABILITY),
        entry(TOPOLOGY_TARGET, TOPOLOGY_TARGET.rsplit(".", 1)[0], "fuzz",
              None, TOPOLOGY_CAPABILITY),
    ])
    for item in entries:
        item["related_target_ids"] = [other["target_id"] for other in entries
                                      if other["family_id"] == item["family_id"]
                                      and other["target_id"] != item["target_id"]]
    return tuple(entries)


def resolve_catalog_entry(target_id):
    """Resolve exact identity metadata without constructing services or opting in."""
    for entry in target_catalog():
        if entry["target_id"] == target_id:
            return entry
    raise KeyError(target_id)


def catalog_declarations(*, exploratory=False, runtime_capability=None):
    """Compatibility scope view over the original declarations, without copying.

    Required resolution still ignores exploratory capability arguments. The
    exploratory factory retains its exact capability validation and error
    precedence; only its explicitly selected extension enters this view.
    """
    targets = (exploratory_target_declarations(runtime_capability=runtime_capability)
               if exploratory else target_declarations())
    usage = "fuzz" if exploratory else "conformance_baseline"
    expected = {entry["target_id"] for entry in target_catalog()
                if entry["usage"] == usage
                and entry["runtime_capability"] in (None, runtime_capability)}
    if set(targets) != expected:
        raise ValueError("catalog/declaration target ids differ")
    return targets


def resolve_target(target_id, *, exploratory=False, runtime_capability=None):
    """Resolve the original SemanticTarget by exact id within an explicit scope.

    Family membership never redirects an id to another contract. A known but
    out-of-scope/non-opted-in id is rejected just like the legacy service view.
    """
    return catalog_declarations(exploratory=exploratory,
                                runtime_capability=runtime_capability)[target_id]


def resolve_rule_lineage(target_id, rule_id):
    """Read-only sidecar lookup; membership comes from the exact existing manifest.

    The catalog can describe an opt-in identity. Registry callers must first
    have registered that identity with the owning module/capability.
    """
    entry = resolve_catalog_entry(target_id)
    target = resolve_target(target_id, exploratory=entry["usage"] == "fuzz",
                            runtime_capability=entry["runtime_capability"])
    from .rule_lineage import resolve
    return resolve(target, rule_id, scope={key: entry[key] for key in
        ("usage", "family_id", "conformance_baseline_id", "runtime_capability")})


def coverage_lineage():
    """Read the complete source/model/evidence sidecar, never a target manifest."""
    from .coverage import catalog_lineage
    return catalog_lineage()
