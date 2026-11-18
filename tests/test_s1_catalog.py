"""Unified S1 metadata over unchanged SemanticTarget declarations; offline only."""
from copy import deepcopy
from unittest.mock import Mock

import pytest

from r2f.core import Registry, SemanticTarget, canonical_hash
from r2f.modules.s1 import catalog
from r2f.modules.s1.module import (
    S1Module, S1ExploratoryModule, register, register_exploratory, resolve_services,
)
from r2f.modules.s1.providers import service_bindings, exploratory_service_bindings
from r2f.modules.s1.targets import (
    EXECUTION_ORDER, EXPLORATORY_TARGET_ORDER, EXPLORATORY_RUNTIME_TARGET, RMWS,
    F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET, F2_WINDOWS_RUNTIME_TARGET,
    target_declarations, exploratory_target_declarations,
)
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET

# Independent canonical byte digests captured by main before consolidation
# (catalog-baseline.json, 2026-11-18). Do not regenerate from the catalog under test.
FROZEN_MANIFESTS = {
    "discovery.late_endpoint": "c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940",
    "discovery.late_join_delivery": "1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d",
    "discovery.interruption_recovery": "fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce",
    "builtin.discovery_projection_reconciliation": "eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0",
    "discovery.late_endpoint.fuzz_v1": "e173db6ca90927065f2840e72c40aa0d30aed43592f8c62d2d39858c5a2d6525",
    "discovery.late_join_delivery.fuzz_v1": "098fb797d0480d17a672e2ecce8edf41bb95ef9f4d5ff0f7f8a959ea6402dd1d",
    "discovery.interruption_recovery.fuzz_v1": "d0d008bb84f33012a6a562a7ab7acc2066076b1b42b40d896287dd767e7fb494",
    "builtin.discovery_projection_reconciliation.fuzz_v1": "0288795a666bb50d30cdf07a789ec60f7a9a3fde9f1643f15ea0c3607221a183",
    "discovery.match_compatibility.fuzz_v1": "1a884ef74dbfc8e15ca4ddb60ac121ba6dd4b81c43f7581b988426d942e68b2e",
}
REQUIRED_MODULE_SHA256 = "947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6"
OPT_INS = {F2_WINDOWS_TARGET: F2_WINDOWS_CAPABILITY, TOPOLOGY_TARGET: TOPOLOGY_CAPABILITY}


def test_one_catalog_lists_all_identities_and_family_relationships():
    entries = catalog.target_catalog()
    by_id = {entry["target_id"]: entry for entry in entries}
    assert len(entries) == len(by_id) == 11
    assert tuple(by_id) == EXECUTION_ORDER + EXPLORATORY_TARGET_ORDER + tuple(OPT_INS)
    for target_id in EXECUTION_ORDER:
        entry = by_id[target_id]
        assert entry["family_id"] == target_id
        assert entry["usage"] == "conformance_baseline"
        assert entry["conformance_baseline_id"] == target_id
    for target_id, baseline_id in EXPLORATORY_RUNTIME_TARGET.items():
        entry = by_id[target_id]
        assert entry["usage"] == "fuzz"
        assert entry["conformance_baseline_id"] == baseline_id
        assert entry["family_id"] == (baseline_id or "discovery.match_compatibility")
    assert by_id[F2_WINDOWS_TARGET]["family_id"] == F2_WINDOWS_RUNTIME_TARGET
    assert by_id[F2_WINDOWS_TARGET]["conformance_baseline_id"] == F2_WINDOWS_RUNTIME_TARGET
    assert by_id[TOPOLOGY_TARGET]["family_id"] == "discovery.endpoint_topology"
    assert by_id[TOPOLOGY_TARGET]["conformance_baseline_id"] is None
    assert len({entry["family_id"] for entry in entries}) == 6
    for entry in entries:
        identifier = entry["target_id"]
        assert entry["runtime_capability"] == OPT_INS.get(identifier)
        assert entry["requires_opt_in"] is (identifier in OPT_INS)
        assert entry["related_target_ids"] == [
            other["target_id"] for other in entries
            if other["family_id"] == entry["family_id"] and other["target_id"] != identifier
        ]
        assert catalog.resolve_catalog_entry(identifier) == entry


def test_catalog_metadata_is_read_only_and_does_not_build_semantic_targets(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("metadata listing must not construct targets/services")
    monkeypatch.setattr(catalog, "target_declarations", forbidden)
    monkeypatch.setattr(catalog, "exploratory_target_declarations", forbidden)
    before = deepcopy(catalog.target_catalog())
    changed = catalog.resolve_catalog_entry(EXECUTION_ORDER[0])
    changed["family_id"] = "not-a-family"
    changed["related_target_ids"].clear()
    assert catalog.target_catalog() == before
    assert catalog.resolve_catalog_entry(TOPOLOGY_TARGET)["requires_opt_in"] is True


@pytest.mark.parametrize("capability", [None, F2_WINDOWS_CAPABILITY, TOPOLOGY_CAPABILITY])
def test_all_nine_old_manifests_and_required_module_bytes_are_frozen(capability):
    required = S1Module()
    fuzz = S1ExploratoryModule(runtime_capability=capability)
    assert canonical_hash(required.to_dict()) == REQUIRED_MODULE_SHA256
    assert tuple(required.targets) == EXECUTION_ORDER
    assert tuple(fuzz.targets)[:5] == EXPLORATORY_TARGET_ORDER
    for identifier, expected_digest in FROZEN_MANIFESTS.items():
        target = (required.targets if identifier in EXECUTION_ORDER else fuzz.targets)[identifier]
        assert type(target) is SemanticTarget
        assert target.target_revision == 0
        assert canonical_hash(target.to_dict()) == expected_digest
        assert catalog.resolve_target(identifier, exploratory=identifier not in EXECUTION_ORDER,
                                      runtime_capability=capability).to_dict() == target.to_dict()
    assert required.to_dict().keys() == {"id", "targets", "execution_order", "dispositions"}
    assert required.id == "s1" and fuzz.id == "s1.exploratory"
    assert len(required.targets) * len(RMWS) * 3 * 3 == 72
    assert fuzz.to_dict()["required_catalog_included"] is False


@pytest.mark.parametrize("identifier,capability,expected_digest", [
    (F2_WINDOWS_TARGET, F2_WINDOWS_CAPABILITY,
     "fda04262bcd89fe974e4a8febff6f98c30c5fd5f2cfd25ab0e51a5a34e8ae661"),
    (TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY,
     "0c4815b9b1b9b70ad639089ac3cdbf6a6ae9c4ba3997bcb4eba4ec8b09ab8234"),
])
def test_opt_in_contract_manifests_are_not_merged_with_family_baselines(identifier, capability, expected_digest):
    target = catalog.resolve_target(identifier, exploratory=True, runtime_capability=capability)
    assert type(target) is SemanticTarget and target.target_revision == 0
    assert canonical_hash(target.to_dict()) == expected_digest
    assert target.target_id == identifier


@pytest.mark.parametrize("capability,extra", [
    (None, None), (F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET), (TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET),
])
def test_compatibility_registration_views_and_id_resolution_keep_exact_gates(capability, extra):
    expected = EXPLORATORY_TARGET_ORDER + ((extra,) if extra else ())
    registry = Registry()
    modules = register_exploratory(registry, include_required=True, runtime_capability=capability)
    assert [module.id for module in modules] == ["s1", "s1.exploratory"]
    assert tuple(registry.targets) == EXECUTION_ORDER + expected
    for identifier in expected:
        assert catalog.resolve_target(identifier, exploratory=True, runtime_capability=capability).target_id == identifier
        assert resolve_services(identifier, exploratory=True, runtime_capability=capability)
        with pytest.raises(KeyError):
            resolve_services(identifier, runtime_capability=capability)
        with pytest.raises(KeyError):
            catalog.resolve_target(identifier, runtime_capability=capability)
    for denied in set(OPT_INS) - {extra}:
        with pytest.raises(KeyError):
            catalog.resolve_target(denied, exploratory=True, runtime_capability=capability)
        with pytest.raises(KeyError):
            resolve_services(denied, exploratory=True, runtime_capability=capability)
    for identifier in EXECUTION_ORDER:
        with pytest.raises(KeyError):
            resolve_services(identifier, exploratory=True, runtime_capability=capability)
        with pytest.raises(KeyError):
            catalog.resolve_target(identifier, exploratory=True, runtime_capability=capability)


@pytest.mark.parametrize("capability", ["topology", "f2_windows", "", True, [TOPOLOGY_CAPABILITY]])
def test_invalid_exploratory_capability_still_rejected_and_required_ignores_it(capability):
    for resolve in (catalog.resolve_target, resolve_services):
        with pytest.raises(ValueError, match="unsupported exploratory runtime_capability"):
            resolve(EXPLORATORY_TARGET_ORDER[0], exploratory=True, runtime_capability=capability)
        # Preserve the legacy error precedence, including unknown ids.
        with pytest.raises(ValueError, match="unsupported exploratory runtime_capability"):
            resolve("unknown", exploratory=True, runtime_capability=capability)
        assert resolve(EXECUTION_ORDER[0], runtime_capability=capability)


@pytest.mark.parametrize("identifier", ["unknown", "discovery.late_endpoint.fuzz", "", "s1"])
def test_unknown_ids_are_never_guessed_or_retargeted(identifier):
    with pytest.raises(KeyError) as error:
        catalog.resolve_catalog_entry(identifier)
    assert error.value.args == (identifier,)
    for exploratory in (False, True):
        for resolve in (catalog.resolve_target, resolve_services):
            with pytest.raises(KeyError) as error:
                resolve(identifier, exploratory=exploratory)
            assert error.value.args == (identifier,)


@pytest.mark.parametrize("exploratory,capability", [
    (False, None), (True, None), (True, F2_WINDOWS_CAPABILITY), (True, TOPOLOGY_CAPABILITY),
])
def test_old_service_resolution_keeps_provider_types_and_full_binding_context(exploratory, capability):
    declarations = (exploratory_target_declarations(runtime_capability=capability)
                    if exploratory else target_declarations())
    bindings = (exploratory_service_bindings if exploratory else service_bindings)(declarations)
    for identifier, expected in bindings.items():
        actual = resolve_services(identifier, exploratory=exploratory, runtime_capability=capability)
        for service in ("mutation_provider", "carrier", "fact_producer", "oracle"):
            actual_type = type(getattr(actual, service))
            expected_type = type(getattr(expected, service))
            if "<locals>" in expected_type.__qualname__:
                # The existing topology factory defines a fresh carrier class
                # per call; compare its origin and bases, not class identity.
                assert (actual_type.__module__, actual_type.__qualname__, actual_type.__bases__) == (
                    expected_type.__module__, expected_type.__qualname__, expected_type.__bases__)
            else:
                assert actual_type is expected_type
        assert {key: target.to_dict() for key, target in actual.mutation_provider._targets.items()} == {
            key: target.to_dict() for key, target in expected.mutation_provider._targets.items()
        }


def test_catalog_is_consumed_by_registration_and_service_resolution(monkeypatch):
    spy = Mock(wraps=catalog.target_catalog)
    monkeypatch.setattr(catalog, "target_catalog", spy)
    register(Registry())
    assert spy.call_count > 0
    before = spy.call_count
    register_exploratory(Registry())
    assert spy.call_count > before
    before = spy.call_count
    resolve_services(EXECUTION_ORDER[0])
    assert spy.call_count > before
    before = spy.call_count
    resolve_services(EXPLORATORY_TARGET_ORDER[0], exploratory=True)
    assert spy.call_count > before
    # Metadata is an enforced view, not an unused parallel directory.
    rows = catalog.target_catalog()
    monkeypatch.setattr(catalog, "target_catalog", lambda: rows[1:])
    with pytest.raises(ValueError, match="catalog/declaration target ids differ"):
        S1Module()


@pytest.mark.parametrize("exploratory", [False, True])
def test_catalog_reuses_original_declaration_instances_without_new_semantic_objects(monkeypatch, exploratory):
    factory = "exploratory_target_declarations" if exploratory else "target_declarations"
    declarations = exploratory_target_declarations() if exploratory else target_declarations()
    monkeypatch.setattr(catalog, factory, lambda **kwargs: declarations)
    assert catalog.catalog_declarations(exploratory=exploratory) is declarations
    for identifier, target in declarations.items():
        assert catalog.resolve_target(identifier, exploratory=exploratory) is target
