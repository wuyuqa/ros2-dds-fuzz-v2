"""F2 catalog/capability/corpus separation tests (offline only).

Generation is not execution: E02/E03/G01 remain capability-pending, no runner
admission is opened, and the F2 generation corpus is never selectable by the
F1 live path.
"""
import unittest

from r2f.campaign.scheduler import required_catalog_hash
from r2f.core import canonical_hash
from r2f.core.mutation_plan import validate_plan
from r2f.modules.s1 import fuzz_catalog as catalog
from r2f.modules.s1 import targets as s1_targets

REQUIRED_CATALOG_HASH = "947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6"
REQUIRED_TARGET_HASHES = (
    "c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940",
    "1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d",
    "fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce",
    "eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0",
)


def f2_profile(target_id, surface_id, seed_id, mutator_id, boundary_class):
    return {
        "schema_version": "r2f.s1-fuzz-profile/v1",
        "profile_id": f"f2-{surface_id.lower()}",
        "campaign_kind": "exploratory",
        "activation": "F2",
        "enabled": False,
        "required_catalog_included": False,
        "seed_catalog_status": "authored_t02",
        "target_ids": [target_id],
        "surfaces": [surface_id],
        "rmws": ["rmw_cyclonedds_cpp"],
        "cycles": [{
            "cycle_id": f"cycle-{surface_id.lower()}",
            "target_id": target_id,
            "surfaces": [surface_id],
            "seed_specs": [{"seed_id": seed_id, "target_id": target_id,
                            "boundary_class": boundary_class,
                            "mutator_id": mutator_id, "executable": False}],
        }],
    }


class FrozenIdentity(unittest.TestCase):
    def test_required_catalog_hash_and_four_targets_are_unchanged(self):
        self.assertEqual(required_catalog_hash(), REQUIRED_CATALOG_HASH)
        declared = s1_targets.target_declarations()
        hashes = tuple(canonical_hash(declared[tid].to_dict()) for tid in s1_targets.EXECUTION_ORDER)
        self.assertEqual(hashes, REQUIRED_TARGET_HASHES)
        self.assertEqual(catalog.catalog_digest(),
                         "28dff57156517b8582105ad4f12fc998f4544e16d613718a8aec1116435a47c6")

    def test_f1_surface_ids_are_not_widened_by_f2_generation(self):
        self.assertEqual(catalog.F1_SURFACE_IDS,
                         ("P01", "P02", "P03", "P04", "P05", "P06", "E01", "W01", "W02"))
        self.assertEqual(catalog.F2_SURFACE_IDS, ("E02", "E03", "G01"))
        self.assertNotIn("E02", catalog.F1_SURFACE_IDS)
        self.assertNotIn("E03", catalog.F1_SURFACE_IDS)
        self.assertNotIn("G01", catalog.F1_SURFACE_IDS)
        # W01's extra M04 witness does not promote it into the core families.
        self.assertEqual(catalog.SURFACES["W01"].layer, "W")
        self.assertEqual(catalog.SURFACES["W01"].runtime_binding,
                         "pair.history_and_sample_counts")


class F2GeneratableButNotExecutable(unittest.TestCase):
    def test_e02_e03_g01_are_generatable_and_still_pending_execution(self):
        for sid in ("E02", "E03", "G01"):
            with self.subTest(sid=sid):
                surface = catalog.SURFACES[sid]
                self.assertTrue(surface.generatable, sid)
                self.assertFalse(surface.executable, sid)
                self.assertEqual(surface.capability_status, "unsupported_capability")
                self.assertIn(sid, catalog.F2_GENERATION_MUTATORS)
                projected = catalog.f2_generation_surface(sid)
                self.assertFalse(projected.executable, sid)
                self.assertEqual(projected.runtime_binding, "", sid)
                self.assertEqual(projected.activation, "F2", sid)
        report = catalog.validate_catalog()
        for sid in ("E02", "E03", "G01"):
            self.assertIn(sid, report["unsupported_surface_ids"])

    def test_f2_projection_is_never_part_of_the_sealed_contract(self):
        # The projection helper is generation-only; exploration_contract must
        # still refuse an F2 surface rather than silently widening the contract.
        for sid in ("E02", "E03", "G01"):
            with self.assertRaises(catalog.UnsupportedCapabilityError):
                catalog.exploration_contract((sid,))
        # And the projected object cannot be smuggled in by a caller: it is not
        # a contract surface, so plan validation reports it as not executable.
        projected = catalog.f2_generation_surface("E02")
        self.assertFalse(projected.executable)
        self.assertEqual(projected.activation, "F2")
        # G01's rule remains visible without claiming a generatable surface.
        contract = catalog.exploration_contract(
            (), extra_rule_ids=("s1.discovery.compatibility_guard.v1",))
        self.assertEqual(contract.surfaces, [])

    def test_capability_matrix_states_generation_and_pending_execution_separately(self):
        matrix = catalog.capability_matrix()
        generation = matrix["f2_generation"]
        self.assertEqual(generation["generatable_surface_ids"], ["E02", "E03", "G01", "P01", "P04", "P06", "W01"])
        self.assertEqual(generation["pending_execution_surface_ids"], ["E02", "E03", "G01"])
        self.assertTrue(generation["w01_stays_auxiliary"])
        self.assertIn("not execution", generation["notes"])
        self.assertEqual(generation["mutators"]["E02"], ["M07"])
        self.assertEqual(generation["mutators"]["E03"], ["M06", "M07"])
        self.assertEqual(generation["mutators"]["G01"], ["M08"])
        self.assertEqual(generation["mutators"]["W01"], ["M04"])
        for sid in ("E02", "E03", "G01"):
            self.assertEqual(matrix["surface_bindings"][sid]["status"], "capability_pending_f2")
            self.assertFalse(matrix["surface_bindings"][sid]["binding_known"])

    def test_mutator_catalog_marks_implemented_but_unregistered(self):
        for mutator_id in ("M04", "M05", "M06", "M07", "M08"):
            entry = catalog.MUTATOR_CATALOG[mutator_id]
            self.assertNotEqual(entry["status"], "registered")
            self.assertIn("implemented", entry["status"])
            self.assertIn(mutator_id, catalog.IMPLEMENTED_F2_MUTATOR_IDS)
            self.assertNotIn(mutator_id, catalog.IMPLEMENTED_MUTATOR_IDS)

    def test_unknown_f2_generation_surface_is_named(self):
        with self.assertRaises(catalog.UnsupportedCapabilityError) as caught:
            catalog.f2_generation_surface("P99")
        self.assertIn("P99", str(caught.exception))


class F2CorpusIsNotF1LiveCorpus(unittest.TestCase):
    def test_f1_profile_loader_rejects_f2_surfaces(self):
        from r2f.campaign.corpus import CorpusProfileError, corpus_from_profile
        # Declared executable just to get past the F1 parser's own check; the
        # capability gate must still refuse the pending F2 surface.
        profile = f2_profile("discovery.interruption_recovery.fuzz_v1",
                             "E02", "E02", "M07", "count_boundary")
        profile["cycles"][0]["seed_specs"][0]["executable"] = True
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(profile, cycle_id="cycle-e02")
        self.assertIn("capability pending", str(caught.exception))

    def test_f2_generation_corpus_is_not_selectable(self):
        from r2f.campaign.corpus import f2_generation_corpus_from_profile
        for sid, target, mutator, boundary in (
                ("E02", "discovery.interruption_recovery.fuzz_v1", "M07", "count_boundary"),
                ("E03", "discovery.interruption_recovery.fuzz_v1", "M06", "anchor_boundary"),
                ("G01", "discovery.match_compatibility.fuzz_v1", "M08", "relation_false"),
                ("W01", "discovery.late_join_delivery.fuzz_v1", "M04", "depth_gt_post")):
            with self.subTest(sid=sid):
                profile = f2_profile(target, sid, sid, mutator, boundary)
                corpus = f2_generation_corpus_from_profile(profile, cycle_id=f"cycle-{sid.lower()}")
                self.assertEqual(len(corpus.entries), 1)
                # The F1 live selector never returns generation-only records.
                self.assertEqual(corpus.selectable(), [])
                record = list(corpus.entries.values())[0]
                self.assertIs(record["executable"], False)
                self.assertIs(record["execution_admitted"], False)
                self.assertIs(record["planned_only"], True)
                self.assertEqual(record["activation"], "F2")
                self.assertEqual(record["capability_status"], "generatable_pending_execution")
                self.assertEqual(record["mutator_id"], mutator)
                self.assertNotIn("raw_path", record)
                self.assertNotIn("oracle_verdict", record)
                self.assertNotIn("mutation_receipt", record)

    def test_f2_profile_requires_executable_false_and_explicit_cycle(self):
        from r2f.campaign.corpus import CorpusProfileError, f2_generation_corpus_from_profile
        profile = f2_profile("discovery.interruption_recovery.fuzz_v1",
                             "E02", "E02", "M07", "count_boundary")
        profile["cycles"][0]["seed_specs"][0]["executable"] = True
        with self.assertRaises(CorpusProfileError) as caught:
            f2_generation_corpus_from_profile(profile, cycle_id="cycle-e02")
        self.assertIn("executable=false", str(caught.exception))
        good = f2_profile("discovery.interruption_recovery.fuzz_v1",
                          "E02", "E02", "M07", "count_boundary")
        with self.assertRaises(CorpusProfileError) as caught:
            f2_generation_corpus_from_profile(good)
        self.assertIn("cycle_id", str(caught.exception))

    def test_f2_profile_rejects_f1_only_surfaces(self):
        from r2f.campaign.corpus import CorpusProfileError, f2_generation_corpus_from_profile
        profile = f2_profile("discovery.late_endpoint.fuzz_v1",
                             "E01", "E01", "M07", "log_range")
        with self.assertRaises(CorpusProfileError) as caught:
            f2_generation_corpus_from_profile(profile, cycle_id="cycle-e01")
        self.assertIn("no F2 generation path", str(caught.exception))


class PlannedOnlyToRuntime(unittest.TestCase):
    def test_multi_window_plan_is_unsupported_without_multi_window_capability(self):
        from r2f.core.mutation_plan import ExplorationContract
        from r2f.core import mutators as m
        surface = catalog.f2_generation_surface("E02")
        prop = m.mutate_f2({"drop_duration_sec": 0.2}, surface, m.make_rng(1),
                           {"window_count": 2, "post_match_observed": True}, "M07")
        # A contract that does not contain the F2 surface must refuse the plan;
        # this proves the F2 projection is not silently part of a sealed contract.
        empty = catalog.exploration_contract(())
        from r2f.core.mutation_plan import compose
        with self.assertRaises(Exception) as caught:
            compose({"drop_duration_sec": 0.2}, [prop], empty, rng_seed=1)
        self.assertIn("E02", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
