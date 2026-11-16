"""Offline contracts for the S1 fuzz catalog and initial corpus (V03)."""
import copy
import json
from pathlib import Path
import unittest

from r2f.modules.s1 import fuzz_catalog as catalog
from r2f.modules.s1.facts import FACT_SPECS


REPO = Path(__file__).resolve().parents[1]
SEEDS = REPO / "config" / "s1_fuzz" / "seeds"


class CatalogCounts(unittest.TestCase):
    def test_ten_core_two_auxiliary_and_never_twelve_core(self):
        counts = catalog.surface_counts()
        self.assertEqual(counts["core"], 10)
        self.assertEqual(counts["auxiliary"], 2)
        self.assertEqual(counts["total"], 12)
        self.assertEqual(set(counts["core_ids"]),
                         {"P01", "P02", "P03", "P04", "P05", "P06", "E01", "E02", "E03", "G01"})
        self.assertEqual(set(counts["auxiliary_ids"]), {"W01", "W02"})
        report = catalog.validate_catalog()
        self.assertEqual(report["counts"]["core"], 10)
        self.assertNotEqual(report["counts"]["core"], report["counts"]["total"])

    def test_whole_catalog_references_resolve(self):
        report = catalog.validate_catalog()
        for sid, surface in catalog.SURFACES.items():
            for fact_id in surface.fact_ids:
                self.assertIn(fact_id, FACT_SPECS, sid)
            self.assertIn(surface.effect_id, catalog.EFFECTS, sid)
        self.assertIn("G01", report["unsupported_surface_ids"])
        self.assertIn("s1.discovery.compatibility_guard.v1", catalog.RULES)

    def test_p04_and_p06_declare_zero_finite_infinite_separately(self):
        for sid in ("P04", "P06"):
            surface = catalog.SURFACES[sid]
            self.assertEqual(surface.special_values["zero"], 0)
            self.assertIn("finite", surface.boundary_classes)
            self.assertIn("infinite", surface.boundary_classes)
            self.assertGreater(surface.special_values["infinite"], 10 ** 12)
        self.assertEqual(catalog.SURFACES["P06"].value_relations,
                         ("lease_configuration_is_not_lease_lifecycle_coverage",))

    def test_w01_is_auxiliary_and_w02_is_not_throughput(self):
        self.assertEqual(catalog.SURFACES["W01"].layer, "W")
        self.assertIn("depth_is_not_a_required_sedp_wire_field", catalog.SURFACES["W01"].value_relations)
        self.assertEqual(catalog.SURFACES["W02"].layer, "W")
        self.assertIn("not_a_high_throughput_claim", catalog.SURFACES["W02"].value_relations)

    def test_tool_budget_is_not_a_normative_bound(self):
        for surface in catalog.SURFACES.values():
            self.assertIn("engineering sampling budget", " ".join(surface.source_refs))
            self.assertFalse(any("spec" in ref.lower() for ref in surface.source_refs), surface.surface_id)


class SurfaceCapability(unittest.TestCase):
    def _surface(self, **overrides):
        base = catalog.SURFACES["E01"]
        data = base.to_dict()
        data.update(overrides)
        for key in ("generatable", "executable", "capability_status"):
            data.pop(key)
        for key, value in list(data.items()):
            if isinstance(value, list):
                data[key] = tuple(value)
        return catalog.SurfaceSpec(**data)

    def test_v03_field_without_binding_is_generatable_but_not_executable(self):
        surface = self._surface(runtime_binding=None)
        self.assertTrue(surface.generatable)
        self.assertFalse(surface.executable)
        self.assertEqual(surface.capability_status, "unsupported_capability")

    def test_v03_unsupported_surface_is_not_in_live_queue(self):
        surfaces = dict(catalog.SURFACES)
        surfaces["E01"] = self._surface(runtime_binding=None)
        report = catalog.validate_catalog(surfaces=surfaces)
        self.assertEqual(report["surfaces"]["E01"]["capability_status"], "unsupported_capability")
        self.assertNotIn("E01", report["executable_surface_ids"])
        self.assertIn("E01", report["unsupported_surface_ids"])

    def test_unknown_runtime_binding_is_rejected_from_execution(self):
        surface = self._surface(runtime_binding="not.a.real.binding")
        self.assertFalse(surface.executable)
        self.assertEqual(surface.capability_status, "unsupported_capability")

    def test_unknown_references_name_the_exact_id(self):
        surface = self._surface(fact_ids=("no.such.fact.v9",))
        surfaces = dict(catalog.SURFACES)
        surfaces["E01"] = surface
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_catalog(surfaces=surfaces)
        self.assertIn("no.such.fact.v9", str(caught.exception))

        surface = self._surface(mutator_ids=("M99",))
        surfaces = dict(catalog.SURFACES)
        surfaces["E01"] = surface
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_catalog(surfaces=surfaces)
        self.assertIn("M99", str(caught.exception))

        surface = self._surface(effect_id="no.such.effect")
        surfaces = dict(catalog.SURFACES)
        surfaces["E01"] = surface
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_catalog(surfaces=surfaces)
        self.assertIn("no.such.effect", str(caught.exception))

        surface = self._surface(rule_ids=("no.such.rule.v1",))
        surfaces = dict(catalog.SURFACES)
        surfaces["E01"] = surface
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_catalog(surfaces=surfaces)
        self.assertIn("no.such.rule.v1", str(caught.exception))

    def test_unknown_related_rule_and_composition_ids_rejected(self):
        rules = dict(catalog.RULES)
        rules["bad.rule.v1"] = catalog.RuleSpec(
            id="bad.rule.v1", version=1, spec_refs=("x",), applies_when={"a": 1},
            required_fact_ids=("missing.fact.v1",),
            evaluator_id="modules.s1.oracles.evaluate_primary",
            allowed_effects=("reader_created_after_writer",),
            forbidden_residuals=("post_match_delivery_loss",),
            limitations=(), responsibility_layer="discovery", primary_or_supporting="primary")
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_catalog(rules=rules)
        self.assertIn("missing.fact.v1", str(caught.exception))

        surface = self._surface(allowed_with=("Z99",))
        surfaces = dict(catalog.SURFACES)
        surfaces["E01"] = surface
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_catalog(surfaces=surfaces)
        self.assertIn("Z99", str(caught.exception))

    def test_primary_rule_without_forbidden_residual_rejected(self):
        rules = dict(catalog.RULES)
        base = catalog.RULES["s1.discovery.compatibility_guard.v1"]
        rules[base.id] = catalog.RuleSpec(
            id=base.id, version=base.version, spec_refs=base.spec_refs,
            applies_when=base.applies_when, required_fact_ids=base.required_fact_ids,
            evaluator_id=base.evaluator_id, allowed_effects=base.allowed_effects,
            forbidden_residuals=(), limitations=base.limitations,
            responsibility_layer=base.responsibility_layer,
            primary_or_supporting="primary")
        with self.assertRaises(catalog.CatalogError):
            catalog.validate_catalog(rules=rules)


ALL_SEEDS = ["E01", "E02", "E03", "G01", "P01", "P02", "P03", "P04", "P05", "P06", "W01", "W02"]


class SeedCorpus(unittest.TestCase):
    def test_all_twelve_seeds_are_authored_and_load(self):
        loaded = catalog.load_seeds(SEEDS)
        self.assertEqual(loaded["authored"], ALL_SEEDS)
        self.assertEqual(loaded["pending"], [])
        self.assertEqual(len(loaded["seeds"]), 12)
        self.assertEqual(sorted(loaded["seeds"]), ALL_SEEDS)
        # "authored" is a corpus-file fact, not an execution or capability claim.
        self.assertFalse(loaded["seeds"]["E02"]["capability"]["f1_routable"])

    def test_each_seed_file_passes_validate_seed(self):
        for sid in ALL_SEEDS:
            with self.subTest(seed=sid):
                path = SEEDS / f"{sid.lower()}.json"
                self.assertTrue(path.is_file(), sid)
                seed = json.loads(path.read_text())
                catalog.validate_seed(seed)
                self.assertEqual(seed["seed_id"], sid)
                self.assertEqual(seed["surface_refs"], [sid])
                self.assertFalse(seed["provenance"]["derived_from_execution"])
                self.assertNotIn("rng_seed", seed)
                self.assertNotIn("parent_content_sha256", seed)
                self.assertNotIn("content_sha256", seed)

    def test_g01_seed_is_capability_pending_not_executed(self):
        loaded = catalog.load_seeds(SEEDS)
        seed = loaded["seeds"]["G01"]
        self.assertEqual(seed["capability"]["status"], "capability_pending")
        self.assertFalse(seed["capability"]["f1_routable"])
        self.assertFalse(seed["provenance"]["derived_from_execution"])
        self.assertNotIn("executed", json.dumps(seed).lower())

    def test_seed_identity_separates_corpus_from_rng_and_parent_hash(self):
        seed = json.loads((SEEDS / "p01.json").read_text())
        self.assertNotIn("rng_seed", seed)
        self.assertNotIn("parent_content_sha256", seed)
        self.assertIn("rng_seed", seed["parent_content_sha256_rule"])
        tampered = copy.deepcopy(seed)
        tampered["rng_seed"] = 12345
        with self.assertRaises(catalog.CatalogError):
            catalog.validate_seed(tampered)
        tampered = copy.deepcopy(seed)
        tampered["parent_content_sha256"] = "0" * 64
        with self.assertRaises(catalog.CatalogError):
            catalog.validate_seed(tampered)

    def test_content_fingerprint_ignores_rng_seed_and_run_identity(self):
        seed = json.loads((SEEDS / "e01.json").read_text())
        base = catalog.seed_content_sha256(seed)
        self.assertEqual(base, catalog.seed_content_sha256(copy.deepcopy(seed)))
        changed = copy.deepcopy(seed)
        changed["logical_config"]["drop_duration_sec"] = 3.0
        self.assertNotEqual(base, catalog.seed_content_sha256(changed))
        window = copy.deepcopy(seed)
        window["operation_windows"][2]["duration_sec"] = 1.0
        self.assertNotEqual(base, catalog.seed_content_sha256(window))

    def test_seed_requires_known_surface_and_schema(self):
        seed = json.loads((SEEDS / "g01.json").read_text())
        bad = copy.deepcopy(seed)
        bad["surface_refs"] = ["G99"]
        with self.assertRaises(catalog.CatalogError) as caught:
            catalog.validate_seed(bad)
        self.assertIn("G99", str(caught.exception))
        bad = copy.deepcopy(seed)
        bad["version"] = 99
        with self.assertRaises(catalog.CatalogError):
            catalog.validate_seed(bad)

    def test_missing_authored_file_is_an_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            copied = Path(folder)
            for path in SEEDS.glob("*.json"):
                (copied / path.name).write_text(path.read_text())
            (copied / "p02.json").unlink()
            with self.assertRaises(catalog.CatalogError) as caught:
                catalog.load_seeds(copied)
            self.assertIn("P02", str(caught.exception))
            self.assertIn("missing", str(caught.exception))

    def test_present_but_invalid_seed_is_an_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            copied = Path(folder)
            for path in SEEDS.glob("*.json"):
                (copied / path.name).write_text(path.read_text())
            (copied / "p02.json").write_text("{}")
            with self.assertRaises(catalog.CatalogError) as caught:
                catalog.load_seeds(copied)
            self.assertIn("P02", str(caught.exception))

    def test_pending_status_is_still_enforced(self):
        """The pending mechanism survives: a declared-pending file must not load."""
        import tempfile
        from unittest import mock
        with tempfile.TemporaryDirectory() as folder:
            copied = Path(folder)
            for path in SEEDS.glob("*.json"):
                (copied / path.name).write_text(path.read_text())
            pending_map = dict(catalog.SEED_STATUS)
            pending_map["P02"] = catalog.PENDING
            with mock.patch.object(catalog, "SEED_STATUS", pending_map):
                with self.assertRaises(catalog.CatalogError) as caught:
                    catalog.load_seeds(copied)
                self.assertIn("P02", str(caught.exception))
                self.assertIn("unexpectedly exists", str(caught.exception))

            # A pending surface with no file is legitimately "pending", not an error.
            (copied / "p02.json").unlink()
            with mock.patch.object(catalog, "SEED_STATUS", pending_map):
                loaded = catalog.load_seeds(copied)
            self.assertIn("P02", loaded["pending"])
            self.assertNotIn("P02", loaded["seeds"])


class SeedContentBoundaries(unittest.TestCase):
    """V08/V09-style content checks: every seed is a bounded plan, not a label."""

    @classmethod
    def setUpClass(cls):
        cls.seeds = catalog.load_seeds(SEEDS)["seeds"]

    def test_p04_and_p06_declare_zero_finite_infinite_separately(self):
        for sid, field in (("P04", "deadline_ns"),
                           ("P06", "liveliness_lease_duration_ns")):
            with self.subTest(seed=sid):
                seed = self.seeds[sid]
                classes = seed["logical_config"]["special_value_classes"]
                self.assertEqual(set(classes), {"zero", "finite", "infinite"})
                self.assertEqual(classes["zero"][field], 0)
                self.assertGreater(classes["infinite"][field], 10 ** 12)
                self.assertLess(classes["finite"][field], classes["infinite"][field])
                self.assertGreater(classes["finite"][field], 0)
                # the specialised classes are not collapsed into one numeric range
                self.assertNotEqual(classes["zero"][field], classes["finite"][field])
                self.assertNotEqual(classes["finite"][field], classes["infinite"][field])
                # the live config stays a finite representative, not the sentinel
                self.assertEqual(seed["logical_config"]["qos"][field],
                                 classes["finite"][field])

    def test_p06_is_lease_configuration_not_lease_lifecycle(self):
        seed = self.seeds["P06"]
        text = json.dumps(seed).lower()
        self.assertIn("not lease lifecycle", text)
        self.assertEqual(seed["logical_config"]["special_value_classes"]["finite"]["liveliness_lease_duration_ns"], 1000000000)
        self.assertNotIn("lease_expired", text)

    def test_w01_declares_a_depth_vs_post_relation(self):
        seed = self.seeds["W01"]
        relation = seed["logical_config"]["workload_relation"]
        self.assertEqual(relation["class"], "depth_gt_post")
        self.assertGreater(relation["depth"], relation["post_count"])
        self.assertIn("depth_is_not_a_required_sedp_wire_field",
                      catalog.SURFACES["W01"].value_relations)
        self.assertEqual(catalog.SURFACES["W01"].layer, "W")

    def test_w02_is_publish_interval_not_throughput(self):
        seed = self.seeds["W02"]
        self.assertGreater(seed["logical_config"]["publish_interval_sec"], 0)
        self.assertIn("not_a_high_throughput_claim",
                      catalog.SURFACES["W02"].value_relations)
        # "throughput" may only appear inside an explicit negation; the seed
        # must not declare a throughput target, load level or measurement.
        text = json.dumps(seed).lower()
        self.assertNotIn("throughput_target", text)
        self.assertNotIn("messages_per_second", text)
        self.assertNotIn("load_level", text)
        for occurrence in [i for i in range(len(text)) if text.startswith("throughput", i)]:
            window = text[max(0, occurrence - 24):occurrence]
            self.assertTrue("not a high-" in window or "not a " in window,
                            f"bare throughput claim near: ...{window}throughput")

    def test_e02_declares_bounded_windows_and_restore_intervals(self):
        seed = self.seeds["E02"]
        windows = seed["logical_config"]["environment_windows"]
        self.assertIn(windows["count"], (1, 2, 3))
        self.assertEqual(seed["logical_config"]["window_count_domain"], [1, 2, 3])
        self.assertEqual(seed["logical_config"]["restore_interval_domain_s"],
                         [0.0, 0.02, 0.2, 1.0])
        self.assertIn(windows["restore_interval_sec"],
                      seed["logical_config"]["restore_interval_domain_s"])
        drops = [w for w in seed["operation_windows"] if w["step"] == "drop_discovery"]
        restores = [w for w in seed["operation_windows"]
                    if w["step"] == "restore_and_confirm"]
        self.assertEqual(len(drops), windows["count"])
        self.assertEqual(len(restores), windows["count"])
        # a second window may only start after the first one restores
        self.assertIn("previous_window_restored", drops[1]["requires"])

    def test_e03_declares_anchor_and_offset_domains(self):
        seed = self.seeds["E03"]
        cfg = seed["logical_config"]
        self.assertEqual(cfg["anchor_domain"], ["fresh_match", "previous_restore_confirmed"])
        self.assertEqual(cfg["anchor_offset_domain_s"], [0.0, 0.01, 0.1, 0.5])
        self.assertIn(cfg["environment_windows"]["anchor"], cfg["anchor_domain"])
        self.assertIn(cfg["environment_windows"]["anchor_offset_sec"],
                      cfg["anchor_offset_domain_s"])
        wait = [w for w in seed["operation_windows"] if w["step"] == "wait_anchor_offset"]
        self.assertEqual(len(wait), 1)
        self.assertEqual(wait[0]["anchor_event"], cfg["environment_windows"]["anchor"])

    def test_g01_keeps_the_four_relations_and_br_is_incompatible(self):
        from r2f.modules.s1 import fuzz_catalog
        surface = fuzz_catalog.SURFACES["G01"]
        self.assertEqual(surface.domain["pairs"], ["RR", "RB", "BB", "BR"])
        self.assertEqual(surface.domain["incompatible_pairs"], ["BR"])
        seed = self.seeds["G01"]
        self.assertEqual(seed["logical_config"]["pair_relation"], "BR")
        self.assertEqual(seed["logical_config"]["writer_qos"]["reliability"], "best_effort")
        self.assertEqual(seed["logical_config"]["reader_qos"]["reliability"], "reliable")

    def test_f2_seeds_stay_capability_pending(self):
        for sid in ("E02", "E03", "G01"):
            with self.subTest(seed=sid):
                seed = self.seeds[sid]
                self.assertEqual(seed["status"], "capability_pending")
                self.assertEqual(seed["capability"]["status"], "capability_pending")
                self.assertFalse(seed["capability"]["f1_routable"])
                self.assertIsNone(seed["capability"]["runtime_binding"])
                self.assertIsNone(seed["capability_requirements"]["runtime_binding"])
                self.assertIsNone(catalog.SURFACES[sid].runtime_binding)
                self.assertIn(sid, catalog.F2_SURFACE_IDS)
                self.assertIn(sid, catalog.validate_catalog()["unsupported_surface_ids"])

    def test_f1_seeds_are_f1_routable_but_not_execution_evidence(self):
        for sid in ("P01", "P02", "P03", "P04", "P05", "P06", "E01", "W01", "W02"):
            with self.subTest(seed=sid):
                seed = self.seeds[sid]
                self.assertTrue(seed["capability"]["f1_routable"])
                self.assertFalse(seed["provenance"]["derived_from_execution"])
                self.assertEqual(seed["provenance"]["kind"], "template")
                self.assertIn(sid, catalog.validate_catalog()["executable_surface_ids"])

    def test_corpus_seed_id_rng_and_parent_hash_are_separate(self):
        for sid, seed in self.seeds.items():
            with self.subTest(seed=sid):
                self.assertEqual(seed["seed_id"], sid)
                self.assertIn("chosen per attempt", seed["rng_seed_role"])
                self.assertIn("rng_seed", seed["parent_content_sha256_rule"])
                for forbidden in ("rng_seed", "parent_content_sha256", "content_sha256",
                                  "run_id", "domain_id", "repeat_index"):
                    self.assertNotIn(forbidden, seed)

    def test_seed_fingerprints_are_deterministic_and_content_bound(self):
        fingerprints = {}
        for sid, seed in self.seeds.items():
            with self.subTest(seed=sid):
                base = catalog.seed_content_sha256(seed)
                fingerprints[sid] = base
                self.assertEqual(len(base), 64)
                self.assertEqual(base, catalog.seed_content_sha256(copy.deepcopy(seed)))

                # rng/run identity never enters the content fingerprint
                dirty = copy.deepcopy(seed)
                dirty["rng_seed"] = 987654
                dirty["run_id"] = "run-not-real"
                dirty["domain_id"] = 77
                dirty["repeat_index"] = 9
                self.assertEqual(base, catalog.seed_content_sha256(dirty))

                # but a real change to the operation plan must change it
                changed = copy.deepcopy(seed)
                changed["operation_windows"][-1]["window"] = "mutated_window"
                self.assertNotEqual(base, catalog.seed_content_sha256(changed))

        # no two templates collapse to the same content identity
        self.assertEqual(len(set(fingerprints.values())), len(fingerprints))


if __name__ == "__main__":
    unittest.main()
