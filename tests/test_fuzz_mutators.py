"""Pure M01-M03 mutator and compose/validate tests.

No DDS, ROS, files, clocks, execution receipts or hit claims are involved.
M04-M08 are TODO and deliberately unregistered.
"""
import random
import unittest

from r2f.core.mutators import MUTATORS, MUTATOR_SPECS, TODO_MUTATORS, make_rng, mutate
from r2f.core.mutation_plan import (
    MUTATION_PLAN_SCHEMA, MutationPlan, MutationSurface, compose, validate_plan,
)
from test_fuzz_model_contracts import PARENT, fixture_contract


def surface(contract, sid):
    return contract.surface_by_id[sid]


def run_many(contract, sid, mutator_id, seeds=range(128), context=None):
    return [mutate(PARENT, surface(contract, sid), random.Random(seed), context, mutator_id)
            for seed in seeds]


class FuzzMutators(unittest.TestCase):
    def setUp(self):
        self.contract = fixture_contract()

    def test_registry_versions_and_todo_is_not_registered(self):
        self.assertEqual(set(MUTATORS), {"M01", "M02", "M03"})
        self.assertEqual(set(MUTATOR_SPECS), {"M01", "M02", "M03"})
        for mutator_id, spec in MUTATOR_SPECS.items():
            data = spec.to_dict()
            self.assertEqual(data["mutator_id"], mutator_id)
            self.assertIsInstance(data["version"], int)
            self.assertTrue(data["accepted_surface_kinds"])
        for todo in TODO_MUTATORS:
            self.assertNotIn(todo, MUTATORS)
            self.assertNotIn(todo, MUTATOR_SPECS)
            with self.assertRaisesRegex(ValueError, "unknown or not implemented mutator"):
                mutate(PARENT, surface(self.contract, "P04"), random.Random(0), {}, todo)

    def test_v04_same_seed_is_fully_deterministic(self):
        for sid, mutator_id in (("P01", "M03"), ("P02", "M02"), ("P04", "M01"), ("P04", "M03")):
            first = run_many(self.contract, sid, mutator_id, seeds=[7])[0]
            second = run_many(self.contract, sid, mutator_id, seeds=[7])[0]
            self.assertEqual(first.to_dict(), second.to_dict(), (sid, mutator_id))
        self.assertEqual(make_rng(3).randrange(1000), make_rng(3).randrange(1000))
        with self.assertRaises(ValueError):
            make_rng(3, "other-rng")

    def test_v05_same_surface_seeds_produce_at_least_sixteen_contents(self):
        results = run_many(self.contract, "P04", "M03", seeds=range(32))
        contents = {repr(result.input_after) for result in results}
        self.assertGreaterEqual(len(contents), 16)
        self.assertEqual({tuple(result.changes) for result in results}, {("qos.deadline_ns",)})

    def test_v07_enum_is_fully_reachable(self):
        from_reliable = {result.changes["qos.reliability"]
                         for result in run_many(self.contract, "P02", "M02", seeds=range(16))}
        self.assertEqual(from_reliable, {"best_effort"})
        parent = {"qos": {"reliability": "best_effort"}}
        from_best_effort = {mutate(parent, surface(self.contract, "P02"), random.Random(seed),
                                   {}, "M02").changes["qos.reliability"] for seed in range(16)}
        self.assertEqual(from_best_effort, {"reliable"})
        data = fixture_contract().to_dict()["surfaces"][1]
        data["domain"] = {"values": ["reliable"]}
        no_op = mutate({"qos": {"reliability": "reliable"}}, MutationSurface.from_dict(data),
                       random.Random(0), {}, "M02")
        self.assertEqual(no_op.applicability_status, "no_op")
        self.assertEqual(no_op.changes, {})

    def test_v07_boundary_neighbours_are_reachable(self):
        results = run_many(self.contract, "P04", "M01", seeds=range(32))
        classes = {result.parameters["boundary_class"] for result in results}
        self.assertEqual(classes, {"min", "min_plus_one", "max_minus_one", "max"})
        values = {result.changes["qos.deadline_ns"] for result in results}
        self.assertEqual(values, {0, 1, 1000000000, 1000000001})

    def test_v07_timing_range_is_quantized_and_nonnegative(self):
        results = run_many(self.contract, "P01", "M03", seeds=range(32))
        values = {result.changes["reader_delay_sec"] for result in results}
        self.assertTrue(values)
        for value in values:
            self.assertTrue(0.0 <= value <= 3.0)
            self.assertEqual(round(value, 3), value)

    def test_wrong_surface_kind_and_undeclared_mutator_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "not declared"):
            data = fixture_contract().to_dict()["surfaces"][0]
            data["mutator_ids"] = ["M03"]
            mutate({}, MutationSurface.from_dict(data), random.Random(0), {}, "M01")
        data = fixture_contract().to_dict()["surfaces"][2]
        data["mutator_ids"] = ["M01", "M02"]
        with self.assertRaisesRegex(ValueError, "does not accept surface kind"):
            mutate({}, MutationSurface.from_dict(data), random.Random(0), {}, "M02")

    def test_mutator_is_pure(self):
        parent = {"qos": {"deadline_ns": 5}}
        frozen = repr(parent)
        result = mutate(parent, surface(self.contract, "P04"), random.Random(2), {}, "M01")
        self.assertEqual(repr(parent), frozen)
        self.assertEqual(result.input_before, parent)
        self.assertNotEqual(result.input_after, parent)

    def test_compose_chains_hashes_and_validate_reports_valid(self):
        first = mutate(PARENT, surface(self.contract, "P02"), random.Random(1), {}, "M02")
        second = mutate(first.input_after, surface(self.contract, "P04"), random.Random(2), {}, "M03")
        second = type(second)(surface_id=second.surface_id, mutator_id=second.mutator_id,
                              parameters=second.parameters, changes=second.changes,
                              input_before=first.input_after, input_after=second.input_after,
                              application_phase=second.application_phase,
                              applicability_result=second.applicability_result,
                              notes=second.notes)
        plan = compose(PARENT, [first, second], self.contract, rng_seed=5)
        self.assertEqual(plan.final_config, second.input_after)
        self.assertEqual(len(plan.operations), 2)
        self.assertEqual(plan.to_dict()["plan_version"], MUTATION_PLAN_SCHEMA)
        self.assertEqual(MutationPlan.from_dict(plan.to_dict()).to_dict(), plan.to_dict())
        result = validate_plan(plan, {"mutators": ["M01", "M02", "M03"], "multi_window": False},
                               self.contract)
        self.assertEqual(result.status, "valid", result.reasons)
        self.assertTrue(result.executable)

    def test_compose_rejects_field_conflict_by_default(self):
        # X01 and X02 both write qos.deadline_ns.
        first = mutate(PARENT, surface(self.contract, "X01"), random.Random(1), {}, "M01")
        second = mutate(PARENT, surface(self.contract, "X02"), random.Random(2), {}, "M01")
        with self.assertRaisesRegex(ValueError, "field conflict without explicit override"):
            compose(PARENT, [first, second], self.contract)

    def test_validate_marks_unsupported_multiwindow(self):
        # Minimal multi-window plan is built by direct operation-free composition;
        # M07 is TODO, so no environment mutator is claimed here.
        plan = compose(PARENT, [], self.contract, rng_seed=0,
                       runtime_windows=[{"schema": "r2f.runtime-window/v1", "window_id": "w0",
                                         "window_index": 0, "trigger_anchor": "fresh_match",
                                         "trigger_offset_sec": 0.0, "drop_duration_sec": 0.2,
                                         "restore_interval_sec": 0.2},
                                        {"schema": "r2f.runtime-window/v1", "window_id": "w1",
                                         "window_index": 1, "trigger_anchor": "previous_restore_confirmed",
                                         "trigger_offset_sec": 0.0, "drop_duration_sec": 0.2,
                                         "restore_interval_sec": 0.2}])
        result = validate_plan(plan, {"multi_window": False}, self.contract)
        self.assertEqual(result.status, "unsupported_capability")
        self.assertFalse(result.executable)


if __name__ == "__main__":
    unittest.main()
