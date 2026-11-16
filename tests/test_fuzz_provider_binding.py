"""T02/T03/T04 seam: catalog -> core contracts -> exploratory provider.

These are offline contracts.  A prepared input is not a DDS run, a mutation
hit, or an oracle result, and no test here starts ROS.
"""
import copy
import unittest
from unittest.mock import patch

from r2f.core import Registry, compile_scenario
from r2f.core.evidence import canonical_hash, effective_input_hash
from r2f.core.mutation_plan import ExplorationContract, MutationPlan, compose, validate_plan
from r2f.core.mutators import MUTATOR_SPECS, make_rng, mutate
from r2f.modules.s1 import fuzz_catalog as catalog
from r2f.modules.s1.module import (S1ExploratoryModule, S1Module, register,
                                   register_exploratory)
from r2f.modules.s1.providers import (ExploratoryInputError,
                                      S1ExploratoryMutationProvider,
                                      exploratory_service_bindings)
from r2f.modules.s1.targets import (EXECUTION_ORDER, EXPLORATORY_RUNTIME_TARGET,
                                    EXPLORATORY_TARGET_IDS, exploratory_target_declarations,
                                    target_declarations)

REQUIRED_CATALOG_HASH = "947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6"
REQUIRED_TARGET_HASHES = (
    "c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940",
    "1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d",
    "fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce",
    "eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0",
)

LATE_BASE = {
    "topic": "/r2f/s1/late_endpoint",
    "message_type": "std_msgs/msg/String",
    "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64},
    "reader_delay_sec": 0.15,
    "baseline_reader_delay_sec": 0.02,
    "pre_match_count": 2,
    "post_match_count": 6,
    "publish_interval_sec": 0.03,
    "match_timeout_sec": 8.0,
    "delivery_timeout_sec": 5.0,
    "startup_timeout_sec": 10.0,
}
PROJECTION_BASE = {
    "topic": "/r2f/s1/discovery_projection",
    "message_type": "std_msgs/msg/String",
    "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64,
            "deadline_ns": 9223372036854775807, "liveliness": "automatic",
            "liveliness_lease_duration_ns": 9223372036854775807},
}
DISCOVERY_BASE = {
    "topic": "/r2f/s1/discovery_recovery",
    "message_type": "std_msgs/msg/String",
    "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64},
    "drop_duration_sec": 3.0,
    "recovery_timeout_sec": 8.0,
    "counter_interval_sec": 0.25,
}


def _scenario(target, plan=None, *, seed=7, arm="mutated", rmw="rmw_cyclonedds_cpp"):
    parameters = {
        "mutation_kind": target.mutation_slots[0].mutation_kind,
        "carrier": target.mutation_slots[0].carrier,
    }
    if plan is not None:
        parameters["mutation_plan"] = plan.to_dict()
    return compile_scenario(target, rmw, arm, 0, seed, parameters)


def _plan(provider, target_id, surface_id, mutator_id, base, *, seed=7):
    contract = provider.contract_for(target_id)
    surface = contract.surface_by_id[surface_id]
    proposal = mutate(base, surface, make_rng(seed), mutator_id=mutator_id)
    return compose(base, [proposal], contract, rng_seed=seed)


class RequiredCatalogFrozen(unittest.TestCase):
    def test_required_module_hash_and_four_targets_are_unchanged(self):
        module = S1Module()
        self.assertEqual(canonical_hash(module.to_dict()), REQUIRED_CATALOG_HASH)
        self.assertEqual(tuple(module.targets), EXECUTION_ORDER)
        for target_id, digest in zip(EXECUTION_ORDER, REQUIRED_TARGET_HASHES):
            self.assertEqual(canonical_hash(module.targets[target_id].to_dict()), digest)
            self.assertIsNone(module.targets[target_id].exploration_contract)
            self.assertNotIn("exploration_contract", module.targets[target_id].to_dict())

    def test_required_registration_never_contains_exploratory_ids(self):
        registry = Registry()
        register(registry)
        self.assertEqual(set(registry.targets), set(EXECUTION_ORDER))
        self.assertFalse(set(registry.targets) & set(EXPLORATORY_TARGET_IDS))

    def test_default_registration_catalog_hash_is_stable_across_exploratory_import(self):
        from r2f.campaign.scheduler import required_catalog_hash
        before = required_catalog_hash()
        S1ExploratoryModule()
        self.assertEqual(required_catalog_hash(), before)
        self.assertEqual(before, REQUIRED_CATALOG_HASH)


class CatalogProjection(unittest.TestCase):
    def test_nine_f1_surfaces_project_with_implemented_mutators_only(self):
        for sid in catalog.F1_SURFACE_IDS:
            with self.subTest(sid=sid):
                surface = catalog.surface_to_core(catalog.SURFACES[sid])
                self.assertTrue(surface.executable)
                self.assertTrue(surface.mutator_ids, sid)
                for mutator_id in surface.mutator_ids:
                    self.assertIn(mutator_id, catalog.IMPLEMENTED_MUTATOR_IDS)
                    spec = MUTATOR_SPECS[mutator_id]
                    self.assertIn(surface.surface_kind, spec.accepted_surface_kinds)

    def test_enum_surfaces_expose_declared_values_and_numeric_surfaces_a_range(self):
        self.assertEqual(catalog.surface_to_core(catalog.SURFACES["P02"]).domain,
                         {"values": ["reliable", "best_effort"]})
        p04 = catalog.surface_to_core(catalog.SURFACES["P04"]).domain
        self.assertEqual((p04["min"], p04["max"], p04["step"]), (1, 9000000000, 1))
        self.assertIs(type(p04["min"]), int)
        e01 = catalog.surface_to_core(catalog.SURFACES["E01"]).domain
        self.assertEqual((e01["min"], e01["max"]), (0.05, 8.0))

    def test_pruned_boundary_classes_are_recorded_not_silently_dropped(self):
        p04 = catalog.surface_to_core(catalog.SURFACES["P04"])
        self.assertEqual(sorted(p04.boundary_classes), ["max", "min"])
        note = [r for r in p04.value_relations if r.startswith("core_projection_pruned_boundary_classes=")]
        self.assertEqual(len(note), 1)
        for cls in ("zero", "infinite"):
            self.assertIn(cls, note[0])
        names = {item["name"] for item in p04.special_values}
        self.assertEqual(names, {"zero", "infinite"})

    def test_f2_surfaces_are_refused_by_name_not_coerced(self):
        for sid in catalog.F2_SURFACE_IDS:
            with self.subTest(sid=sid):
                with self.assertRaises(catalog.UnsupportedCapabilityError) as caught:
                    catalog.surface_to_core(catalog.SURFACES[sid])
                self.assertIn(sid, str(caught.exception))
        report = catalog.validate_catalog()
        for sid in catalog.F2_SURFACE_IDS:
            self.assertIn(sid, report["unsupported_surface_ids"])
        for sid in catalog.F1_SURFACE_IDS:
            self.assertIn(sid, report["executable_surface_ids"])

    def test_unknown_surface_id_is_named(self):
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.exploration_contract(["P99"])
        self.assertIn("P99", str(caught.exception))

    def test_contract_is_deterministic_and_subset_safe(self):
        first = catalog.exploration_contract(["P01", "W02"])
        second = catalog.exploration_contract(["W02", "P01", "P01"])
        self.assertEqual(first.contract_hash(), second.contract_hash())
        self.assertEqual(set(first.surface_by_id), {"P01", "W02"})
        # Edges to surfaces outside the subset are pruned, never dangling.
        self.assertEqual(first.surface_by_id["P01"].allowed_with, ["W02"])
        self.assertEqual(first.surface_by_id["W02"].allowed_with, ["P01"])
        full = catalog.exploration_contract(catalog.F1_SURFACE_IDS)
        self.assertEqual(len(full.surfaces), 9)
        self.assertNotIn("G01", full.surface_by_id)

    def test_f2_rule_stays_resolvable_without_claiming_a_surface(self):
        contract = catalog.exploration_contract((), extra_rule_ids=("s1.discovery.compatibility_guard.v1",))
        self.assertEqual(contract.surfaces, [])
        self.assertEqual([r.id for r in contract.rules], ["s1.discovery.compatibility_guard.v1"])
        self.assertEqual([e.effect_id for e in contract.effects],
                         ["s1.discovery.compatibility_guard"])

    def test_capability_matrix_marks_f2_pending_and_mutators_unimplemented(self):
        matrix = catalog.capability_matrix()
        self.assertEqual(tuple(matrix["f1_routable_surface_ids"]), catalog.F1_SURFACE_IDS)
        self.assertEqual(tuple(matrix["f2_pending_surface_ids"]), catalog.F2_SURFACE_IDS)
        for sid in catalog.F2_SURFACE_IDS:
            self.assertEqual(matrix["surface_bindings"][sid]["status"], "capability_pending_f2")
            self.assertFalse(matrix["surface_bindings"][sid]["binding_known"])
        self.assertEqual(matrix["declared_only_mutator_ids"], ["M04", "M05", "M06", "M07", "M08"])


class ExploratoryTargets(unittest.TestCase):
    def setUp(self):
        self.targets = exploratory_target_declarations()

    def test_five_targets_revision_zero_and_required_untouched(self):
        self.assertEqual(tuple(self.targets), catalog.F2_SURFACE_IDS and
                         ("discovery.late_endpoint.fuzz_v1", "discovery.late_join_delivery.fuzz_v1",
                          "discovery.interruption_recovery.fuzz_v1",
                          "builtin.discovery_projection_reconciliation.fuzz_v1",
                          "discovery.match_compatibility.fuzz_v1"))
        for target_id, target in self.targets.items():
            self.assertEqual(target.target_id, target_id)
            self.assertEqual(target.target_revision, 0)
            self.assertIsInstance(target.exploration_contract, ExplorationContract)
        # Required declarations are produced by a separate function and are
        # unaffected by building exploratory ones.
        for target_id, digest in zip(EXECUTION_ORDER, REQUIRED_TARGET_HASHES):
            self.assertEqual(canonical_hash(target_declarations()[target_id].to_dict()), digest)

    def test_every_contract_reference_resolves_within_this_catalog(self):
        for target_id, target in self.targets.items():
            with self.subTest(target_id=target_id):
                contract = target.exploration_contract
                self.assertTrue(contract.contract_hash())
                for surface in contract.surfaces:
                    self.assertIn(surface.surface_id, catalog.SURFACES)
                    for fact_id in surface.fact_ids:
                        self.assertIn(fact_id, catalog.FACT_SPECS)
                    for rule_id in surface.rule_ids:
                        self.assertIn(rule_id, catalog.RULES)
                    self.assertIn(surface.effect_id, catalog.EFFECTS)
                    self.assertIn(surface.runtime_binding, catalog.RUNTIME_BINDINGS)

    def test_f1_targets_declare_routable_surfaces_and_f2_is_pending(self):
        for target_id, target in self.targets.items():
            activation = target.observation_contract["activation"]
            pending = target.observation_contract["pending_surface_ids"]
            if activation == "F1":
                self.assertEqual(pending, [], target_id)
                self.assertTrue(target.exploration_contract.surfaces, target_id)
                self.assertTrue(target.observation_contract["reuses"] in EXECUTION_ORDER)
            else:
                self.assertEqual(target_id, "discovery.match_compatibility.fuzz_v1")
                self.assertEqual(pending, ["G01"])
                self.assertEqual(target.exploration_contract.surfaces, [])

    def test_runtime_backend_reuse_is_explicit(self):
        self.assertIsNone(EXPLORATORY_RUNTIME_TARGET["discovery.match_compatibility.fuzz_v1"])
        for target_id, backend in EXPLORATORY_RUNTIME_TARGET.items():
            if backend is not None:
                self.assertIn(backend, EXECUTION_ORDER)
        self.assertEqual(len(EXPLORATORY_RUNTIME_TARGET), 5)

    def test_exploration_contract_preserves_required_effect_and_rule_identity(self):
        required = target_declarations()
        pair = (("discovery.late_endpoint.fuzz_v1", "discovery.late_endpoint"),
                ("discovery.interruption_recovery.fuzz_v1", "discovery.interruption_recovery"),
                ("builtin.discovery_projection_reconciliation.fuzz_v1",
                 "builtin.discovery_projection_reconciliation"))
        for exploratory_id, required_id in pair:
            self.assertEqual(canonical_hash(self.targets[exploratory_id].expected_effect_set),
                             canonical_hash(required[required_id].expected_effect_set))
            self.assertEqual(canonical_hash(self.targets[exploratory_id].rule_contract),
                             canonical_hash(required[required_id].rule_contract))


class ExploratoryRegistration(unittest.TestCase):
    def test_explicit_registration_adds_exactly_five_and_keeps_required_separate(self):
        exploratory = Registry()
        register_exploratory(exploratory)
        self.assertEqual(set(exploratory.targets), set(EXPLORATORY_TARGET_IDS))
        self.assertEqual(set(exploratory.modules), {"s1.exploratory"})

        combined = Registry()
        register_exploratory(combined, include_required=True)
        self.assertEqual(set(combined.modules), {"s1", "s1.exploratory"})
        self.assertEqual(set(combined.targets),
                         set(EXECUTION_ORDER) | set(EXPLORATORY_TARGET_IDS))
        self.assertEqual(S1Module().execution_order, EXECUTION_ORDER)

    def test_each_exploratory_service_binding_is_complete(self):
        bindings = exploratory_service_bindings(exploratory_target_declarations())
        self.assertEqual(set(bindings), set(EXPLORATORY_TARGET_IDS))
        for binding in bindings.values():
            self.assertTrue(callable(binding.mutation_provider.prepare))
            self.assertTrue(callable(binding.carrier.accepts))
            self.assertTrue(callable(binding.carrier.apply))
            self.assertTrue(callable(binding.fact_producer.produce))
            self.assertTrue(callable(binding.oracle.evaluate))


class DeterministicPlanConsumption(unittest.TestCase):
    CASES = (
        ("discovery.late_endpoint.fuzz_v1", "P01", "M03", LATE_BASE),
        ("discovery.late_endpoint.fuzz_v1", "P01", "M01", LATE_BASE),
        ("builtin.discovery_projection_reconciliation.fuzz_v1", "P02", "M02", PROJECTION_BASE),
        ("builtin.discovery_projection_reconciliation.fuzz_v1", "P04", "M01", PROJECTION_BASE),
        ("discovery.interruption_recovery.fuzz_v1", "E01", "M03", DISCOVERY_BASE),
    )

    def setUp(self):
        self.targets = exploratory_target_declarations()
        self.provider = S1ExploratoryMutationProvider(self.targets)

    def test_p01_p02_p04_have_real_generation_paths(self):
        seen = set()
        for target_id, surface_id, mutator_id, base in self.CASES:
            with self.subTest(surface=surface_id, mutator=mutator_id):
                plan = _plan(self.provider, target_id, surface_id, mutator_id, base)
                self.assertTrue(plan.operations)
                self.assertNotEqual(canonical_hash(plan.final_config), canonical_hash(base))
                prepared = self.provider.prepare(_scenario(self.targets[target_id], plan))
                self.assertEqual(prepared["plan_status"], "valid")
                self.assertEqual(canonical_hash(prepared["final_input"]),
                                 canonical_hash(plan.final_config))
                seen.add(surface_id)
        self.assertTrue({"P01", "P02", "P04"} <= seen)

    def test_same_plan_and_seed_never_regenerates_a_different_input(self):
        for target_id, surface_id, mutator_id, base in self.CASES:
            with self.subTest(surface=surface_id):
                plan = _plan(self.provider, target_id, surface_id, mutator_id, base)
                scenario = _scenario(self.targets[target_id], plan)
                first = self.provider.prepare(scenario)
                second = copy.deepcopy(self.provider).prepare(copy.deepcopy(scenario))
                self.assertEqual(first, second)
                self.assertEqual(first["input_layer_receipts"], second["input_layer_receipts"])
                self.assertEqual(first["input_layer_receipts"][0]["plan_hash"], plan.plan_hash())

    def test_different_seed_changes_the_proposed_input_but_not_the_plan_hash_semantics(self):
        plan_a = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03",
                       LATE_BASE, seed=7)
        plan_b = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03",
                       LATE_BASE, seed=8)
        self.assertNotEqual(plan_a.plan_hash(), plan_b.plan_hash())
        self.assertEqual(plan_a.rng_algorithm, plan_b.rng_algorithm)

    def test_prepare_does_not_touch_runtime_or_mutators(self):
        plan = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03", LATE_BASE)
        scenario = _scenario(self.targets["discovery.late_endpoint.fuzz_v1"], plan)
        with patch("r2f.core.mutators.mutate", side_effect=AssertionError("mutator")), \
             patch("socket.socket", side_effect=AssertionError("socket")):
            prepared = self.provider.prepare(scenario)
        self.assertEqual(prepared["plan_status"], "valid")
        self.assertFalse({"hit", "execution_stage", "effect_observed"} & set(prepared))

    def test_prepared_input_is_a_valid_effective_input(self):
        plan = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03", LATE_BASE)
        scenario = _scenario(self.targets["discovery.late_endpoint.fuzz_v1"], plan)
        prepared = self.provider.prepare(scenario)
        prepared["source_manifest"] = {"head": "x" * 40}
        effective = scenario.effective_input(
            final_input=prepared["final_input"],
            input_layer_receipts=prepared["input_layer_receipts"],
            source_manifest=prepared["source_manifest"])
        self.assertRegex(effective_input_hash(effective), r"^[0-9a-f]{64}$")
        self.assertEqual(effective["target_id"], "discovery.late_endpoint.fuzz_v1")
        self.assertEqual(scenario.target_id, "discovery.late_endpoint.fuzz_v1")


class PlanRefusals(unittest.TestCase):
    def setUp(self):
        self.targets = exploratory_target_declarations()
        self.provider = S1ExploratoryMutationProvider(self.targets)

    def test_missing_plan_is_refused_instead_of_falling_back_to_required_payload(self):
        scenario = _scenario(self.targets["discovery.late_endpoint.fuzz_v1"])
        with self.assertRaises(ExploratoryInputError) as caught:
            self.provider.prepare(scenario)
        self.assertIn("explicit mutation_plan", str(caught.exception))

    def test_seed_mismatch_is_refused(self):
        plan = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03",
                     LATE_BASE, seed=7)
        scenario = _scenario(self.targets["discovery.late_endpoint.fuzz_v1"], plan, seed=8)
        with self.assertRaises(ExploratoryInputError) as caught:
            self.provider.prepare(scenario)
        self.assertIn("rng_seed", str(caught.exception))

    def test_plan_from_a_different_contract_is_refused(self):
        other = catalog.exploration_contract(["W02"])
        surface = other.surface_by_id["W02"]
        base = dict(LATE_BASE)
        proposal = mutate(base, surface, make_rng(7), mutator_id="M03")
        foreign_plan = compose(base, [proposal], other, rng_seed=7)
        scenario = _scenario(self.targets["discovery.late_endpoint.fuzz_v1"], foreign_plan)
        with self.assertRaises(ExploratoryInputError) as caught:
            self.provider.prepare(scenario)
        self.assertIn("contract_hash", str(caught.exception))

    def test_f2_target_has_no_live_backend_and_says_so(self):
        scenario = _scenario(self.targets["discovery.match_compatibility.fuzz_v1"])
        with self.assertRaises(ExploratoryInputError) as caught:
            self.provider.prepare(scenario)
        message = str(caught.exception)
        self.assertTrue("mutation_plan" in message or "runtime backend" in message, message)
        plan = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03", LATE_BASE)
        scenario = _scenario(self.targets["discovery.match_compatibility.fuzz_v1"], plan)
        with self.assertRaises(ExploratoryInputError) as caught:
            self.provider.prepare(scenario)
        self.assertIn("no live routable surface", str(caught.exception))
        self.assertIn("G01", str(caught.exception))

    def test_unknown_surface_and_rule_references_are_named(self):
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.exploration_contract(["P01"], extra_rule_ids=("s1.nope.v9",))
        self.assertIn("s1.nope.v9", str(caught.exception))

    def test_capability_pending_surface_is_not_live_executable(self):
        # Even though E02 is declared, the provider capability set is F1-only
        # and the runtime binding is absent, so both layers must refuse it.
        self.assertIsNone(catalog.SURFACES["E02"].runtime_binding)
        from r2f.modules.s1.providers import EXPLORATORY_ACTIVATION_CAPABILITIES
        self.assertEqual(EXPLORATORY_ACTIVATION_CAPABILITIES["activation"], ("F1",))
        with self.assertRaises(catalog.UnsupportedCapabilityError):
            catalog.surface_to_core(catalog.SURFACES["E02"])

    def test_plan_with_rejected_operation_is_not_prepared(self):
        plan = _plan(self.provider, "discovery.late_endpoint.fuzz_v1", "P01", "M03", LATE_BASE)
        data = plan.to_dict()
        data["operations"][0]["applicability_result"] = {"status": "rejected"}
        broken = MutationPlan.from_dict(data)
        scenario = _scenario(self.targets["discovery.late_endpoint.fuzz_v1"], broken)
        with self.assertRaises(ExploratoryInputError):
            self.provider.prepare(scenario)

    def test_multi_window_plan_is_not_live_routable(self):
        from r2f.core.mutation_plan import RUNTIME_WINDOW_SCHEMA
        plan = _plan(self.provider, "discovery.interruption_recovery.fuzz_v1", "E01", "M03",
                     DISCOVERY_BASE)
        data = plan.to_dict()
        window = {"schema": RUNTIME_WINDOW_SCHEMA, "window_id": "w1", "window_index": 0,
                  "trigger_anchor": "fresh_match", "trigger_offset_sec": 0.0,
                  "drop_duration_sec": 0.2, "restore_interval_sec": 0.2}
        data["runtime_windows"] = [window, dict(window, window_id="w2", window_index=1)]
        multi = MutationPlan.from_dict(data)
        contract = self.provider.contract_for("discovery.interruption_recovery.fuzz_v1")
        result = validate_plan(multi, capabilities=dict(
            __import__("r2f.modules.s1.providers", fromlist=["x"]).EXPLORATORY_ACTIVATION_CAPABILITIES),
            contract=contract)
        self.assertEqual(result.status, "unsupported_capability")
        self.assertIn("multi-window", " ".join(result.reasons))


class OracleBinding(unittest.TestCase):
    def test_g01_create_success_is_not_a_compatibility_bug(self):
        from r2f.modules.s1.oracles import evaluate_match_compatibility_guard
        facts = {
            "writer": {"id": "w1", "generation": 1},
            "reader": {"id": "r1", "generation": 1},
            "submitted": {"writer_reliability": "best_effort", "reader_reliability": "reliable"},
            "window": {"start_ns": 10, "end_ns": 20, "complete": True},
            "observed": {"match": "absent", "delivery": "absent",
                         "observer_complete": True, "provenance": True},
        }
        result = evaluate_match_compatibility_guard(facts)
        self.assertEqual(result["verdict"], "pass")
        self.assertEqual(result["forbidden_residuals"], [])

    def test_exploratory_oracle_dispatch_routes_g01_to_guard(self):
        from r2f.modules.s1.providers import S1ExploratoryOracle
        target = exploratory_target_declarations()["discovery.match_compatibility.fuzz_v1"]
        scenario = _scenario(target)
        result = S1ExploratoryOracle().evaluate(scenario, {})
        self.assertEqual(result["rule_id"], "s1.discovery.compatibility_guard.v1")
        self.assertEqual(result["verdict"], "unknown")


if __name__ == "__main__":
    unittest.main()
