"""Pure F2 contract/prepare checks: no DDS, namespace or execution evidence."""
from copy import deepcopy
from dataclasses import replace
import json
import unittest
from unittest.mock import patch

from r2f.core import Registry, compile_scenario
from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import (
    ExplorationContract, MutationPlan, compose, validate_contract,
)
from r2f.core import mutators
from r2f.modules.s1 import fuzz_catalog as catalog
from r2f.modules.s1.module import (
    S1Module, S1ExploratoryModule, register_exploratory, resolve_services,
)
from r2f.modules.s1.providers import (
    ExploratoryInputError, S1ExploratoryMutationProvider, validate_exploratory_plan,
)
from r2f.modules.s1.targets import (
    EXPLORATORY_TARGET_ORDER, F2_WINDOWS_TARGET, F2_WINDOWS_CAPABILITY,
    F2_ANCHOR_TIMING_POLICY, F2_ANCHOR_LATENESS_TOLERANCE_SEC,
    exploratory_target_declarations,
)
from r2f.runtime.ros2_pair import exploratory_runtime_capabilities


RMW = "rmw_cyclonedds_cpp"
ROUTE = dict(target_id=F2_WINDOWS_TARGET, carrier="wire_proxy", mutation_kind="discovery_drop")


def planned(surface_id="E02", count=2, *, context=None, controls=None, edit_windows=None):
    """Real F2 proposal, then explicit pre-compose window materialization.

    This is an input fixture, not an observed precondition. The actual campaign
    normalizer is separately exercised by its owner; no observation=True is set.
    """
    parent = {
        "drop_duration_sec": 0.2, "recovery_timeout_sec": 8.0,
        "counter_interval_sec": 0.25,
        "runtime_capability": F2_WINDOWS_CAPABILITY,
        "anchor_timing_policy": F2_ANCHOR_TIMING_POLICY,
        "anchor_lateness_tolerance_sec": F2_ANCHOR_LATENESS_TOLERANCE_SEC,
        "environment_windows": {"count": count, "restore_interval_sec": 0.02,
                                "anchor": "fresh_match", "anchor_offset_sec": 0.0},
    }
    parent.update(controls or {})
    context = {"anchor": "fresh_match", "offset_sec": 0.1,
               "window_count": count, "restore_interval_sec": 0.02, **(context or {})}
    surface = catalog.f2_live_surface(surface_id)
    proposal = mutators.mutate_f2(parent, surface, mutators.make_rng(7), context)
    if proposal.runtime_windows:
        windows = [{key: value for key, value in window.items() if key != "schema"}
                   for window in proposal.runtime_windows]
        for index, window in enumerate(windows):
            window["window_index"] = index
    else:
        windows = [dict(window_id=f"post_match_window_{index + 1}", window_index=index,
                        trigger_anchor="fresh_match" if index == 0 else "previous_restore_confirmed",
                        trigger_offset_sec=(proposal.parameters["offset_sec"]
                                            if ("fresh_match" if index == 0 else "previous_restore_confirmed")
                                            == proposal.parameters["anchor"] else 0.0),
                        drop_duration_sec=0.2, restore_interval_sec=0.02)
                   for index in range(count)]
    if edit_windows:
        edit_windows(windows)
    final = {**parent, "environment_windows": windows}
    proposal = replace(proposal, changes={"environment_windows": windows}, input_after=final,
                       runtime_windows=[{"schema": "r2f.runtime-window/v1", **window}
                                        for window in windows])
    return compose(parent, [proposal], catalog.f2_live_contract(), rng_seed=7, chain_length=1)


class F2DeclarationContract(unittest.TestCase):
    def test_opt_in_preserves_complete_required_and_legacy_declarations(self):
        required = S1Module().to_dict()
        legacy = S1ExploratoryModule().to_dict()
        seeds = catalog.load_seeds()
        generated = {sid: catalog.f2_generation_surface(sid).to_dict() for sid in ("E02", "E03")}
        opted = S1ExploratoryModule(runtime_capability=F2_WINDOWS_CAPABILITY).to_dict()
        self.assertEqual(S1Module().to_dict(), required)
        self.assertEqual(S1ExploratoryModule().to_dict(), legacy)
        self.assertEqual({key: opted["targets"][key] for key in legacy["targets"]}, legacy["targets"])
        self.assertEqual(opted["execution_order"], list(EXPLORATORY_TARGET_ORDER) + [F2_WINDOWS_TARGET])
        self.assertEqual(catalog.load_seeds(), seeds)
        self.assertEqual({sid: catalog.f2_generation_surface(sid).to_dict() for sid in generated}, generated)
        self.assertNotIn(F2_WINDOWS_TARGET, legacy["targets"])

    def test_registry_requires_explicit_known_runtime_capability(self):
        registry = Registry()
        register_exploratory(registry)
        with self.assertRaises(KeyError):
            registry.get_target(F2_WINDOWS_TARGET)
        registry = Registry()
        register_exploratory(registry, runtime_capability=F2_WINDOWS_CAPABILITY)
        self.assertEqual(registry.get_target(F2_WINDOWS_TARGET).target_revision, 0)
        self.assertIsNotNone(resolve_services(F2_WINDOWS_TARGET, exploratory=True,
                                             runtime_capability=F2_WINDOWS_CAPABILITY))
        for bad in ("F2", "", False, "f2_windows_v2"):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "runtime_capability"):
                exploratory_target_declarations(runtime_capability=bad)

    def test_new_target_reuses_recovery_rules_and_names_precise_raw_timing(self):
        target = exploratory_target_declarations(runtime_capability=F2_WINDOWS_CAPABILITY)[F2_WINDOWS_TARGET]
        required = S1Module().targets["discovery.interruption_recovery"]
        for field in ("expected_effect_set", "rule_contract", "control_contract"):
            self.assertEqual(getattr(target, field), getattr(required, field))
        observation = target.observation_contract
        self.assertEqual(observation["activation"], "F2")
        self.assertEqual(observation["runtime_capability"], F2_WINDOWS_CAPABILITY)
        self.assertEqual(observation["max_chain_length"], 1)
        self.assertEqual(observation["surface_dependencies"]["E03"]["requires_surface_ids"], ["E02"])
        timing = observation["anchor_timing_contract"]
        self.assertEqual(timing["fresh_match"]["raw_event"], "discovery_precondition_snapshot")
        self.assertEqual(timing["previous_restore_confirmed"]["raw_event"], "discovery_recovery_confirmed")
        self.assertTrue(timing["eligible_drop"].startswith("max("))
        self.assertEqual(timing["timing_miss"], "harness_incomplete_not_sut_candidate")
        self.assertFalse(observation["operation_hit_contract"]["aggregate_or_window_count_only_hit"])

    def test_live_api_is_closed_and_preserves_e03_order_dependency(self):
        for sid, mid, binding in (("E02", "M07", "environment_windows.count"),
                                  ("E03", "M06", "environment_windows.anchor_offset_sec")):
            surface = catalog.f2_live_surface(sid)
            self.assertEqual(surface.activation, "F2")
            self.assertEqual(surface.mutator_ids, [mid])
            self.assertEqual(surface.runtime_binding, binding)
            self.assertTrue(all(flag == "unknown" for flag in surface.sut_guards.values()))
            self.assertFalse(catalog.f2_generation_surface(sid).executable)
            with self.assertRaises(catalog.UnsupportedCapabilityError):
                catalog.surface_to_core(catalog.SURFACES[sid])
        self.assertEqual(catalog.f2_live_surface("E03").order_after, ["E02"])
        for sid in ("G01", "W01", "E01", "P01", "", "E99"):
            with self.subTest(sid=sid), self.assertRaises(catalog.UnsupportedCapabilityError):
                catalog.f2_live_surface(sid)


class F2StructuralVersusRuntime(unittest.TestCase):
    def test_f2_specs_do_not_change_default_registry_or_generation_entrypoint(self):
        contract = catalog.f2_live_contract()
        self.assertEqual(ExplorationContract.from_dict(contract.to_dict()).to_dict(), contract.to_dict())
        self.assertEqual(set(mutators.MUTATOR_SPECS), {"M01", "M02", "M03"})
        for sid in ("E02", "E03"):
            surface = catalog.f2_live_surface(sid)
            with self.assertRaisesRegex(ValueError, "unknown or not implemented mutator"):
                mutators.mutate({}, surface, mutators.make_rng(7), {}, surface.mutator_ids[0])
            plan = planned(sid)
            with self.assertRaisesRegex(ExploratoryInputError, "not live-routable"):
                validate_exploratory_plan(plan, contract,
                                          target_id="discovery.interruption_recovery.fuzz_v1",
                                          carrier="wire_proxy", mutation_kind="discovery_drop")

    def test_malicious_f1_surface_cannot_reference_f2_mutators(self):
        for mid in ("M06", "M07"):
            data = catalog.f2_live_contract().to_dict()
            data["surfaces"][0].update(activation="F1", mutator_ids=[mid])
            with self.subTest(mutator=mid), self.assertRaisesRegex(ValueError, "unknown mutator reference"):
                ExplorationContract.from_dict(data)

    def test_explicit_structural_registry_is_not_expanded(self):
        contract = catalog.f2_live_contract()
        for specs in ({}, mutators.MUTATOR_SPECS, list(mutators.MUTATOR_SPECS.values())):
            with self.subTest(specs=type(specs)), self.assertRaisesRegex(ValueError, "unknown mutator reference"):
                validate_contract(contract, specs)
        self.assertTrue(validate_contract(contract, mutators.ALL_MUTATOR_SPECS))

    def test_unknown_mutators_and_wrong_surface_kinds_remain_invalid(self):
        for change, reason in (({"mutator_ids": ["M99"]}, "unknown mutator reference"),
                               ({"surface_kind": "parameter_numeric"}, "does not accept surface kind")):
            data = catalog.f2_live_contract().to_dict()
            data["surfaces"][0].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, reason):
                ExplorationContract.from_dict(data)


class F2PrepareContract(unittest.TestCase):
    def setUp(self):
        self.contract = catalog.f2_live_contract()
        self.targets = exploratory_target_declarations(runtime_capability=F2_WINDOWS_CAPABILITY)
        self.provider = S1ExploratoryMutationProvider(self.targets)

    def test_real_route_prepares_one_two_three_windows_without_discharging_guards(self):
        for sid in ("E02", "E03"):
            for count in (1, 2, 3):
                with self.subTest(surface=sid, count=count):
                    plan = planned(sid, count)
                    before = plan.to_dict()
                    result = validate_exploratory_plan(plan, self.contract, **ROUTE)
                    self.assertEqual(result.status, "needs_observation")
                    self.assertFalse(result.executable)
                    scenario = compile_scenario(self.targets[F2_WINDOWS_TARGET], RMW, "mutated", 0, 7,
                                                {"mutation_plan": plan.to_dict()}, mutation_plan=plan)
                    prepared = self.provider.prepare_plan(scenario, plan)
                    self.assertEqual(prepared["plan_status"], "needs_observation")
                    self.assertEqual(prepared["final_input"], plan.final_config)
                    self.assertEqual(plan.to_dict(), before)
                    self.assertEqual(prepared["input_layer_receipts"][0]["operation_status"],
                                     {plan.operations[0].op_id: "needs_observation"})
                    for field in ("mutation_receipt", "execution_stage", "actual_run", "reached", "hit"):
                        self.assertNotIn(field, prepared)

    def test_route_capability_missing_or_mismatch_cannot_prepare_f2(self):
        routes = exploratory_runtime_capabilities()
        for key, bad in (("runtime_capability", None), ("runtime_capability", "wrong"),
                         ("implemented", False), ("backend", "endpoint_pair"),
                         ("carrier", "ros2_endpoint"), ("mutation_kind", "netem"),
                         ("surfaces", ["E02"]), ("surfaces", None), ("multi_window", False),
                         ("max_runtime_windows", True), ("max_runtime_windows", 4)):
            changed = deepcopy(routes)
            changed[F2_WINDOWS_TARGET][key] = bad
            with self.subTest(key=key, bad=bad), patch(
                    "r2f.runtime.ros2_pair.exploratory_runtime_capabilities", return_value=changed), \
                    self.assertRaises(ExploratoryInputError):
                validate_exploratory_plan(planned(), self.contract, **ROUTE)
        for overrides in ({"target_id": None}, {"carrier": "ros2_endpoint"},
                          {"mutation_kind": "endpoint_qos_variation"}):
            with self.subTest(overrides=overrides), self.assertRaises(ExploratoryInputError):
                validate_exploratory_plan(planned(), self.contract, **{**ROUTE, **overrides})

    def test_missing_f2_route_never_falls_back_to_accepting_an_f1_plan(self):
        contract = catalog.exploration_contract(("E01",))
        surface = contract.surface_by_id["E01"]
        parent = {"drop_duration_sec": 0.2}
        proposal = mutators.mutate(parent, surface, mutators.make_rng(7), {}, "M03")
        plan = compose(parent, [proposal], contract, rng_seed=7)
        with patch("r2f.runtime.ros2_pair.exploratory_runtime_capabilities", return_value={}), \
                self.assertRaisesRegex(ExploratoryInputError, "runtime_route_capability"):
            validate_exploratory_plan(plan, contract, **ROUTE)

    def test_generation_only_projection_cannot_borrow_new_runtime_capability(self):
        data = self.contract.to_dict()
        data["surfaces"][0] = catalog.f2_generation_surface("E02").to_dict()
        contract = ExplorationContract.from_dict(data)
        plan = planned()
        plan.contract_hash = contract.contract_hash()
        with self.assertRaisesRegex(ExploratoryInputError, "no runtime binding"):
            validate_exploratory_plan(plan, contract, **ROUTE)

    def test_complete_e02_dependency_contract_cannot_be_omitted_for_e03(self):
        contract = deepcopy(self.contract)
        contract.surfaces = [contract.surface_by_id["E03"]]
        with self.assertRaisesRegex(ValueError, "unknown .*reference"):
            contract.validate()
        # A caller pruning away the dependency is still not the live target contract.
        contract.surfaces[0].allowed_with = []
        contract.surfaces[0].order_after = []
        contract.validate()
        plan = planned("E03")
        plan.contract_hash = contract.contract_hash()
        with self.assertRaisesRegex(ExploratoryInputError, "contract_mismatch"):
            validate_exploratory_plan(plan, contract, **ROUTE)

    def test_observed_true_or_applicable_status_cannot_be_injected_by_planning(self):
        for sid, guard in (("E02", "post_match_observed"), ("E03", "anchor_observed")):
            with self.subTest(sid=sid), self.assertRaisesRegex(ExploratoryInputError, "needs_observation"):
                validate_exploratory_plan(planned(sid, context={guard: True}), self.contract, **ROUTE)
        plan = planned("E03")
        plan.operations[0].parameters["anchor_observed"] = 0
        with self.assertRaisesRegex(ExploratoryInputError, "observation_cannot_be_supplied"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)

    def test_missing_invalid_timing_controls_and_foreign_environment_fail_closed(self):
        for key, bad in (("runtime_capability", None), ("anchor_timing_policy", "other"),
                         ("anchor_lateness_tolerance_sec", True),
                         ("anchor_lateness_tolerance_sec", 0.6),
                         ("anchor_lateness_tolerance_sec", None),
                         ("environment_mutation", None),
                         ("environment_mutation", {"kind": "netem"}),
                         ("writer_churn_in_window", True), ("reader_recreate_in_window", False),
                         ("anchor_observed", True), ("post_match_observed", False)):
            with self.subTest(key=key, bad=bad), self.assertRaises(ExploratoryInputError):
                validate_exploratory_plan(planned(controls={key: bad}), self.contract, **ROUTE)
        for key in ("runtime_capability", "anchor_timing_policy", "anchor_lateness_tolerance_sec"):
            plan = planned()
            del plan.final_config[key]
            plan.operations[-1].input_after_sha256 = canonical_hash(plan.final_config)
            with self.subTest(missing=key), self.assertRaisesRegex(ExploratoryInputError, "timing_contract"):
                validate_exploratory_plan(plan, self.contract, **ROUTE)

    def test_single_window_is_not_an_exception_to_window_plan_binding(self):
        plan = planned(count=1)
        plan.runtime_windows[0]["drop_duration_sec"] = 0.3
        with self.assertRaisesRegex(ExploratoryInputError, "plan_and_final_input_differ"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)
        plan = planned(count=1)
        del plan.final_config["environment_windows"][0]["restore_interval_sec"]
        plan.operations[-1].input_after_sha256 = canonical_hash(plan.final_config)
        with self.assertRaisesRegex(ExploratoryInputError, "window_fields"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)

    def test_operation_specific_parameters_must_match_materialized_windows(self):
        for sid, key, bad in (("E02", "window_count", 1), ("E02", "restore_interval_sec", 0.2),
                              ("E03", "offset_sec", 0.5), ("E03", "anchor", "previous_restore_confirmed")):
            plan = planned(sid)
            plan.operations[0].parameters[key] = bad
            with self.subTest(sid=sid, key=key), self.assertRaisesRegex(ExploratoryInputError, "binding_mismatch"):
                validate_exploratory_plan(plan, self.contract, **ROUTE)

    def test_out_of_order_anchors_empty_windows_and_changed_final_input_are_rejected(self):
        plan = planned(count=1)
        plan.final_config["environment_windows"] = []
        plan.runtime_windows = []
        plan.operations[-1].input_after_sha256 = canonical_hash(plan.final_config)
        with self.assertRaisesRegex(ExploratoryInputError, "complete_1_to_3"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)
        plan = planned(count=1, edit_windows=lambda windows: windows[0].update(trigger_anchor="previous_restore_confirmed"))
        with self.assertRaisesRegex(ExploratoryInputError, "anchor_order"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)
        plan = planned()
        plan.final_config["environment_windows"][0]["drop_duration_sec"] = 0.3
        with self.assertRaisesRegex(ExploratoryInputError, "final operation"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)

    def test_e03_anchor_class_binds_every_matching_window_not_always_first(self):
        for anchor in ("fresh_match", "previous_restore_confirmed"):
            for count in (2, 3):
                with self.subTest(anchor=anchor, count=count):
                    plan = planned("E03", count, context={"anchor": anchor, "offset_sec": 0.5})
                    self.assertEqual(validate_exploratory_plan(plan, self.contract, **ROUTE).status,
                                     "needs_observation")
                    windows = plan.final_config["environment_windows"]
                    affected = [index for index, window in enumerate(windows)
                                if window["trigger_anchor"] == anchor]
                    self.assertEqual(affected, [0] if anchor == "fresh_match" else list(range(1, count)))
                    for index in affected:
                        self.assertEqual(windows[index]["trigger_offset_sec"], 0.5)
                    self.assertEqual(windows[0]["trigger_anchor"], "fresh_match")
        plan = planned("E03", 1, context={"anchor": "previous_restore_confirmed"})
        with self.assertRaisesRegex(ExploratoryInputError, "binding_mismatch"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)
        plan = planned("E03", 3, context={"anchor": "previous_restore_confirmed"})
        plan.final_config["environment_windows"][2]["trigger_offset_sec"] = 0.2
        plan.runtime_windows[2]["trigger_offset_sec"] = 0.2
        plan.operations[-1].input_after_sha256 = canonical_hash(plan.final_config)
        with self.assertRaisesRegex(ExploratoryInputError, "binding_mismatch"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)

    def test_new_target_rejects_implicit_second_operation_and_nonmutated_scenarios(self):
        plan = planned()
        second = deepcopy(plan.operations[-1])
        second.op_id += "-extra"
        second.input_before_sha256 = second.input_after_sha256
        plan.operations.append(second)
        plan.chain = None
        with self.assertRaisesRegex(ExploratoryInputError, "exactly one operation"):
            validate_exploratory_plan(plan, self.contract, **ROUTE)
        for arm in ("baseline", "transparent", "negative"):
            plan = planned()
            scenario = compile_scenario(self.targets[F2_WINDOWS_TARGET], RMW, arm, 0, 7,
                                        {"mutation_plan": plan.to_dict()}, mutation_plan=plan)
            with self.subTest(arm=arm), self.assertRaisesRegex(ExploratoryInputError, "registered_mutated"):
                self.provider.prepare_plan(scenario, scenario.mutation_plan)

    def test_round_trip_and_different_parameters_bind_distinct_plan_inputs(self):
        for sid in ("E02", "E03"):
            plan = planned(sid)
            rebuilt = MutationPlan.from_dict(json.loads(json.dumps(plan.to_dict())))
            self.assertEqual(rebuilt.plan_hash(), plan.plan_hash())
            self.assertEqual(rebuilt.final_config, plan.final_config)
            self.assertEqual(validate_exploratory_plan(rebuilt, self.contract, **ROUTE).status, "needs_observation")
            changed = planned(sid, context={"restore_interval_sec": 0.2} if sid == "E02" else {"offset_sec": 0.5})
            self.assertNotEqual(plan.plan_hash(), changed.plan_hash())
            self.assertNotEqual(canonical_hash(plan.final_config), canonical_hash(changed.final_config))


if __name__ == "__main__":
    unittest.main()
