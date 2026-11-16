"""F2 mutators M04-M08: pure generation only.

Nothing here starts ROS, opens a socket, or records an execution hit.  A
MutationProposal is an input proposal; ``needs_observation`` means the plan is
structurally usable but its effect still requires real evidence.
"""
import random
import unittest

from r2f.core import mutators as m
from r2f.core.mutation_plan import MutationSurface, compose, validate_plan
from r2f.modules.s1 import fuzz_catalog as catalog

INFINITE_NS = 9223372036854775807


def surface(sid):
    return catalog.f2_generation_surface(sid)


def gen(sid, base, mutator_id, context=None, seed=0):
    return m.mutate_f2(base, surface(sid), m.make_rng(seed), context or {}, mutator_id)


class RegistrySeparation(unittest.TestCase):
    def test_m04_to_m08_are_implemented_but_not_in_default_registry(self):
        self.assertEqual(set(m.MUTATORS), {"M01", "M02", "M03"})
        self.assertEqual(set(m.MUTATOR_SPECS), {"M01", "M02", "M03"})
        self.assertEqual(set(m.F2_MUTATORS), {"M04", "M05", "M06", "M07", "M08"})
        self.assertEqual(set(m.ALL_MUTATOR_SPECS), {"M01", "M02", "M03", "M04", "M05", "M06", "M07", "M08"})
        for mutator_id in ("M04", "M05", "M06", "M07", "M08"):
            self.assertNotIn(mutator_id, m.MUTATOR_SPECS)
            with self.assertRaisesRegex(ValueError, "unknown or not implemented mutator"):
                m.mutate({"qos": {"depth": 1}}, surface("W01"), random.Random(0), {}, mutator_id)
        # The default registry signature is what the sealed F1 contract binds.
        self.assertTrue(m.mutator_registry_signature())

    def test_f2_mutate_is_reachable_but_unknown_ids_still_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown or not implemented F2 mutator"):
            m.mutate_f2({}, surface("W01"), random.Random(0), {}, "M99")
        with self.assertRaisesRegex(ValueError, "not declared by surface"):
            m.mutate_f2({}, surface("G01"), random.Random(0), {}, "M04")

    def test_todo_constant_still_lists_the_unregistered_ids(self):
        self.assertEqual(m.TODO_MUTATORS, ("M04", "M05", "M06", "M07", "M08"))
        for todo in m.TODO_MUTATORS:
            self.assertNotIn(todo, m.MUTATORS)


class M04RelationBoundary(unittest.TestCase):
    BASE = {"qos": {"depth": 8}, "pre_match_count": 2, "post_match_count": 6}

    def test_three_relation_classes_are_reachable_and_true(self):
        for cls in ("depth_lt_post", "depth_eq_post", "depth_gt_post"):
            prop = gen("W01", self.BASE, "M04", {"relation_class": cls})
            self.assertEqual(prop.parameters["relation_class"], cls)
            depth = prop.changes["qos.depth"]
            post = prop.changes["post_match_count"]
            if cls == "depth_lt_post":
                self.assertLess(depth, post)
            elif cls == "depth_eq_post":
                self.assertEqual(depth, post)
            else:
                self.assertGreater(depth, post)
            # relation class is reported honestly in the applicability result
            self.assertEqual(prop.applicability_result["relation_class"], cls)
            self.assertTrue(prop.applicability_result["witness"])

    def test_declared_domain_is_the_only_source_of_witnesses(self):
        prop = gen("W01", self.BASE, "M04", {"relation_class": "depth_lt_post"})
        domain = catalog.SURFACES["W01"].domain
        self.assertIn(prop.changes["qos.depth"], domain["depth"])
        self.assertIn(prop.changes["post_match_count"], domain["post_count"])
        self.assertIn(prop.changes["pre_match_count"], domain["pre_count"])

    def test_relation_boundary_does_not_masquerade_as_range_sampling(self):
        prop = gen("W01", self.BASE, "M04")
        self.assertEqual(prop.mutator_id, "M04")
        self.assertIn("relation", prop.parameters)
        self.assertNotIn("sampling", prop.parameters)

    def test_undeclared_relation_class_is_named_and_rejected(self):
        with self.assertRaisesRegex(ValueError, "relation class is not declared"):
            gen("W01", self.BASE, "M04", {"relation_class": "depth_lt_pre"})
        with self.assertRaisesRegex(ValueError, "unsupported relation class"):
            gen("W01", self.BASE, "M04", {"relation_class": "bogus"})

    def test_w01_is_auxiliary_and_not_promoted_to_core(self):
        self.assertEqual(catalog.SURFACES["W01"].layer, "W")
        self.assertNotIn("W01", catalog.F2_SURFACE_IDS)
        self.assertNotIn("W01", catalog.F2_GENERATION_MUTATORS["E02"])


class M05SpecialValue(unittest.TestCase):
    CASES = (("P04", "qos.deadline_ns"), ("P06", "qos.liveliness_lease_duration_ns"))

    def test_zero_finite_infinite_come_from_declared_classes(self):
        for sid, path in self.CASES:
            for cls in ("zero", "finite", "infinite"):
                values = {gen(sid, {"qos": {}}, "M05", {"special_class": cls}, seed).changes[path]
                          for seed in range(16)}
                if cls == "zero":
                    self.assertEqual(values, {0}, sid)
                elif cls == "infinite":
                    self.assertEqual(values, {INFINITE_NS}, sid)
                else:
                    self.assertNotIn(INFINITE_NS, values, sid)
                    self.assertNotIn(0, values, sid)
                    self.assertTrue(values, sid)

    def test_finite_witnesses_are_declared_and_never_folded_to_infinity(self):
        for sid, path in self.CASES:
            declared = set(catalog.SURFACES[sid].domain["finite_representatives"])
            values = {gen(sid, {"qos": {}}, "M05", {"special_class": "finite"}, seed).changes[path]
                      for seed in range(16)}
            self.assertTrue(values <= declared, (sid, values, declared))

    def test_infinity_is_reported_as_its_own_class(self):
        prop = gen("P04", {"qos": {}}, "M05", {"special_class": "infinite"})
        self.assertEqual(prop.parameters["special_class"], "infinite")
        self.assertTrue(prop.notes["infinite_is_not_a_large_finite_number"])
        self.assertGreater(prop.changes["qos.deadline_ns"], 10 ** 12)

    def test_undeclared_special_class_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unsupported special class"):
            gen("P04", {"qos": {}}, "M05", {"special_class": "nan"})
        # A class the surface does not declare must not be invented.
        with self.assertRaisesRegex(ValueError, "unsupported special class"):
            gen("P04", {"qos": {}}, "M05", {"special_class": "negative"})
        self.assertNotIn("negative", catalog.SURFACES["P04"].special_values)


class M06TimingOffset(unittest.TestCase):
    def test_e03_anchor_and_offset_are_both_recorded(self):
        base = {"environment_windows": {"anchor": "fresh_match", "anchor_offset_sec": 0.0}}
        for anchor in ("fresh_match", "previous_restore_confirmed"):
            prop = gen("E03", base, "M06", {"anchor": anchor, "offset_sec": 0.1,
                                            "anchor_observed": True})
            self.assertEqual(prop.changes["environment_windows.anchor"], anchor)
            self.assertEqual(prop.changes["environment_windows.anchor_offset_sec"], 0.1)
            self.assertEqual(prop.parameters["unit"], "s")
            self.assertEqual(prop.applicability_status, "applicable")

    def test_unobserved_anchor_is_needs_observation_not_applicable(self):
        base = {"environment_windows": {"anchor": "fresh_match", "anchor_offset_sec": 0.0}}
        prop = gen("E03", base, "M06", {"offset_sec": 0.01})
        self.assertEqual(prop.applicability_status, "needs_observation")
        self.assertFalse(prop.parameters["anchor_observed"])

    def test_p01_offsets_after_the_anchor_and_stays_in_domain(self):
        for seed in range(16):
            prop = gen("P01", {"reader_delay_sec": 0.15}, "M06",
                       {"anchor": "fresh_match", "anchor_observed": True, "offset_sec": 0.01}, seed)
            self.assertEqual(prop.changes["reader_delay_sec"], 0.16)
            self.assertTrue(0 <= prop.changes["reader_delay_sec"] <= 3.0)

    def test_negative_time_is_never_generated(self):
        for sid, base, ctx in (
                ("P01", {"reader_delay_sec": 0.15}, {"offset_sec": -0.01}),
                ("E03", {"environment_windows": {}}, {"offset_sec": -0.5})):
            with self.assertRaisesRegex(ValueError, "non-negative"):
                gen(sid, base, "M06", ctx)
        prop = gen("P01", {"reader_delay_sec": 0.0}, "M06",
                   {"offset_sec": 0.0, "anchor_observed": True})
        self.assertGreaterEqual(prop.changes["reader_delay_sec"], 0)

    def test_undeclared_anchor_is_named(self):
        base = {"environment_windows": {"anchor": "fresh_match"}}
        with self.assertRaisesRegex(ValueError, "is not declared"):
            gen("E03", base, "M06", {"anchor": "pre_match_drop"})


class M07EnvironmentWindows(unittest.TestCase):
    BASE = {"drop_duration_sec": 0.2}

    def windows(self, count):
        return gen("E02", self.BASE, "M07",
                   {"window_count": count, "post_match_observed": True}, seed=3).runtime_windows

    def test_one_to_three_windows_are_planned(self):
        for count in (1, 2, 3):
            windows = self.windows(count)
            self.assertEqual(len(windows), count)
            self.assertEqual([w["window_index"] for w in windows], list(range(1, count + 1)))
            self.assertEqual(len({w["window_id"] for w in windows}), count)

    def test_each_window_has_independent_begin_end_restore(self):
        for window in self.windows(3):
            self.assertEqual(window["schema"], "r2f.runtime-window/v1")
            self.assertGreater(window["drop_duration_sec"], 0)
            self.assertGreaterEqual(window["restore_interval_sec"], 0)
            self.assertIn(window["trigger_anchor"], {"pre_match", "fresh_match",
                                                     "previous_restore_confirmed"})

    def test_only_the_first_window_uses_the_fresh_anchor(self):
        windows = self.windows(3)
        self.assertEqual(windows[0]["trigger_anchor"], "fresh_match")
        for window in windows[1:]:
            self.assertEqual(window["trigger_anchor"], "previous_restore_confirmed")
            self.assertEqual(window["trigger_offset_sec"], 0.0)

    def test_plan_declares_all_windows_not_executed(self):
        prop = gen("E02", self.BASE, "M07", {"window_count": 2, "post_match_observed": True})
        self.assertEqual(prop.applicability_status, "applicable")
        self.assertTrue(prop.notes["planned_not_executed"])
        self.assertTrue(prop.notes["each_window_has_independent_begin_end_restore"])
        planned = prop.changes["environment_windows.windows"]
        self.assertEqual([w["window_index"] for w in planned], [1, 2])
        for item in planned:
            self.assertTrue(item["begin"] and item["end"] and item["restore"])

    def test_without_post_match_observation_it_is_not_applicable(self):
        prop = gen("E02", self.BASE, "M07", {"window_count": 2})
        self.assertEqual(prop.applicability_status, "needs_observation")

    def test_window_count_outside_declared_or_one_to_three_is_rejected(self):
        for count in (0, 4, 5):
            with self.assertRaisesRegex(ValueError, "not declared"):
                gen("E02", self.BASE, "M07", {"window_count": count})
        with self.assertRaisesRegex(ValueError, "max_windows"):
            gen("E02", self.BASE, "M07", {"window_count": 2, "max_windows": 4})

    def test_restore_interval_comes_from_the_declared_set(self):
        declared = set(catalog.SURFACES["E02"].domain["restore_interval_representatives_s"])
        observed = {gen("E02", self.BASE, "M07", {"window_count": 2}, seed)
                    .changes["environment_windows.restore_interval_sec"] for seed in range(16)}
        self.assertTrue(observed <= declared, observed)
        self.assertEqual(observed, declared)

    def test_multi_window_proposal_composes_into_a_plan_but_stays_unsupported_for_execution(self):
        prop = gen("E02", self.BASE, "M07", {"window_count": 2, "post_match_observed": True})
        # The F2 projection is never part of a sealed contract; compose it with a
        # synthetic single-surface contract to exercise plan plumbing only.
        contract = catalog.exploration_contract(())
        with self.assertRaisesRegex(Exception, "unknown proposal surface"):
            compose(self.BASE, [prop], contract, rng_seed=1)


class M08CompatibilityPair(unittest.TestCase):
    BASE = {"writer_qos": {"reliability": "reliable"},
            "reader_qos": {"reliability": "reliable"}}

    def test_four_offered_requested_relations_are_reachable(self):
        seen = {}
        for pair in ("RR", "RB", "BB", "BR"):
            prop = gen("G01", self.BASE, "M08", {"pair_relation": pair})
            seen[pair] = prop
            offered = {"R": "reliable", "B": "best_effort"}[pair[0]]
            requested = {"R": "reliable", "B": "best_effort"}[pair[1]]
            self.assertEqual(prop.changes["writer_qos.reliability"], offered)
            self.assertEqual(prop.changes["reader_qos.reliability"], requested)
            self.assertEqual(prop.changes["pair_relation"], pair)
        self.assertEqual(set(seen), {"RR", "RB", "BB", "BR"})

    def test_only_br_is_incompatible_and_creation_success_is_not_a_bug(self):
        for pair in ("RR", "RB", "BB", "BR"):
            prop = gen("G01", self.BASE, "M08", {"pair_relation": pair})
            expected = "incompatible" if pair == "BR" else "compatible"
            self.assertEqual(prop.applicability_result["compatibility"], expected)
            self.assertTrue(prop.applicability_result["creation_success_is_not_a_bug"])
            self.assertEqual(prop.applicability_result["guard_expectation"],
                             "no_match" if pair == "BR" else "match")
            self.assertTrue(prop.notes["creation_success_is_not_a_violation"])

    def test_incompatible_classification_comes_from_the_catalog(self):
        declared = set(catalog.SURFACES["G01"].domain["incompatible_pairs"])
        self.assertEqual(declared, {"BR"})
        self.assertTrue(gen("G01", self.BASE, "M08", {"pair_relation": "BR"})
                        .parameters["incompatible"])
        self.assertFalse(gen("G01", self.BASE, "M08", {"pair_relation": "RB"})
                         .parameters["incompatible"])

    def test_undeclared_pair_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "is not declared"):
            gen("G01", self.BASE, "M08", {"pair_relation": "XX"})

    def test_no_execution_or_hit_fields_are_produced(self):
        prop = gen("G01", self.BASE, "M08", {"pair_relation": "BR"})
        data = prop.to_dict()
        self.assertEqual(set(data), {"surface_id", "mutator_id", "parameters", "changes",
                                     "input_before", "input_after", "application_phase",
                                     "applicability_result", "runtime_windows", "override",
                                     "notes", "proposal_id"})
        for forbidden in ("executed", "hit", "raw_path", "oracle_verdict", "candidate"):
            self.assertNotIn(forbidden, data)


if __name__ == "__main__":
    unittest.main()
