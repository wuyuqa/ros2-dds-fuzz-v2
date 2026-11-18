"""Offline profile coverage is not runtime, hit, candidate or seal evidence."""
from copy import deepcopy
import json
from pathlib import Path
import unittest

from r2f.campaign.corpus import available_cycles, corpus_from_profile

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ROOT / "config/s1_fuzz/profiles"
RMW = {"rmw_cyclonedds_cpp", "rmw_fastrtps_cpp"}
ORIGINAL_TARGETS = [
    "discovery.late_endpoint.fuzz_v1",
    "discovery.late_join_delivery.fuzz_v1",
    "discovery.interruption_recovery.fuzz_v1",
]


def profile(name):
    return json.loads((PROFILES / f"{name}.json").read_text())


def seed_key(spec):
    # Include variant overrides: a matching seed label alone is insufficient.
    return json.dumps(spec, sort_keys=True)


class ProfileCoverage(unittest.TestCase):
    def assert_declared_cycle_union(self, doc):
        cycles = doc["cycles"]
        self.assertEqual(len({c["cycle_id"] for c in cycles}), len(cycles))
        self.assertEqual(set(doc["target_ids"]), {c["target_id"] for c in cycles})
        self.assertEqual(set(doc["surfaces"]),
                         {sid for c in cycles for sid in c["surfaces"]})
        flattened = [s for c in cycles for s in c["seed_specs"]]
        expected = sorted(seed_key(s) for s in flattened)
        self.assertEqual(len(expected), len(set(expected)), "duplicate cycle seed spec")
        for name in ("seed_specs", "declared_seed_specs_union"):
            self.assertEqual(sorted(seed_key(s) for s in doc[name]), expected, name)
        for cycle in cycles:
            for spec in cycle["seed_specs"]:
                self.assertEqual(spec["target_id"], cycle["target_id"])
                self.assertIn(spec["seed_id"], cycle["surfaces"])
                self.assertIs(spec["executable"], True)

    def test_composed_retains_all_original_cycles_and_semantics(self):
        doc = profile("composed")
        self.assertEqual(doc["target_ids"], ORIGINAL_TARGETS)
        self.assertEqual(doc["surfaces"], ["P01", "W02", "E01"])
        self.assertEqual(doc.get("chain_length", 1), 1)
        self.assertEqual(doc["composition"], {
            "mode": "declared_parameter_x_single_window_environment_seed_coverage",
            "parameter_surfaces": ["P01", "W02"],
            "environment_surfaces": ["E01"],
            "max_environment_windows": 1,
            "status": "declared_seed_coverage_only; per-case multi-surface composition is not claimed in this slice",
        })
        self.assertEqual(doc["pending_seed_ids"], ["E02", "E03", "G01"])
        self.assertEqual(doc["pending_surfaces"], ["E02", "E03", "G01"])
        expected = [
            ("late-endpoint-composed", ORIGINAL_TARGETS[0], ["P01", "W02"], "P01", "finite"),
            ("late-join-composed", ORIGINAL_TARGETS[1], ["P01", "W02"], "W02", "finite"),
            ("recovery-composed", ORIGINAL_TARGETS[2], ["E01"], "E01", "log_range"),
        ]
        self.assertEqual(doc["cycles"], [
            {"cycle_id": cid, "target_id": target, "surfaces": surfaces,
             "runner_slice": "endpoint_pair_slice", "seed_specs": [
                 {"seed_id": seed, "target_id": target,
                  "boundary_class": boundary, "executable": True}]}
            for cid, target, surfaces, seed, boundary in expected
        ])
        self.assert_declared_cycle_union(doc)

    def test_owned_profiles_have_exact_declared_cycle_seed_unions(self):
        for name in ("composed", "endpoint-chains"):
            with self.subTest(profile=name):
                self.assert_declared_cycle_union(profile(name))

    def test_removed_recovery_cycle_is_detected_even_when_top_level_keeps_e01(self):
        broken = deepcopy(profile("composed"))
        broken["cycles"] = [c for c in broken["cycles"]
                            if c["cycle_id"] != "recovery-composed"]
        with self.assertRaises(AssertionError):
            self.assert_declared_cycle_union(broken)

    def test_declared_seed_union_cannot_hide_different_boundary_or_override(self):
        for field, value in (("boundary_class", "zero"),
                             ("overrides", {"logical_config": {"reader_delay_sec": 0.5}})):
            broken = deepcopy(profile("composed"))
            broken["declared_seed_specs_union"][0][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                self.assert_declared_cycle_union(broken)

    def test_each_owned_cycle_builds_only_its_own_declared_offline_corpus(self):
        for name in ("composed", "endpoint-chains"):
            doc = profile(name)
            self.assertEqual({c["cycle_id"] for c in available_cycles(doc)},
                             {c["cycle_id"] for c in doc["cycles"]})
            for cycle in doc["cycles"]:
                with self.subTest(profile=name, cycle=cycle["cycle_id"]):
                    corpus = corpus_from_profile(doc, cycle_id=cycle["cycle_id"])
                    entries = corpus.selectable()
                    self.assertEqual({e["target_id"] for e in entries}, {cycle["target_id"]})
                    self.assertEqual({e["rmw"] for e in entries}, RMW)
                    self.assertEqual({e["seed_id"] for e in entries},
                                     {s["seed_id"] for s in cycle["seed_specs"]})
                    self.assertTrue(all(e["derived_from_execution"] is False for e in entries))

    def test_endpoint_chain_scope_is_separate_from_environment_coverage(self):
        doc = profile("endpoint-chains")
        self.assert_declared_cycle_union(doc)
        self.assertEqual(doc["target_ids"], ORIGINAL_TARGETS[:2])
        self.assertEqual(doc["surfaces"], ["P01", "W02"])
        self.assertEqual(doc["chain_length"], 2)
        self.assertEqual(doc["composition"]["environment_surfaces"], [])
        self.assertEqual(doc["composition"]["max_environment_windows"], 0)
        self.assertNotIn("runtime_capability", doc)
        self.assertEqual(doc["pending_seed_ids"], ["E02", "E03", "G01"])
        self.assertEqual(doc["pending_surfaces"], ["E02", "E03", "G01"])

    def test_all_profiles_remain_disabled_and_outside_required_catalog(self):
        for path in sorted(PROFILES.glob("*.json")):
            with self.subTest(profile=path.name):
                doc = json.loads(path.read_text())
                self.assertIs(doc["enabled"], False)
                self.assertIs(doc["required_catalog_included"], False)
                self.assertEqual(doc["campaign_kind"], "exploratory")


if __name__ == "__main__":
    unittest.main()
