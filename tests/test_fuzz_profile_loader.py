"""T02/T05: profile -> offline seed corpus construction, one cycle at a time.

These tests build corpora and, for runner-supported cycles, generate planned
cases through the shared runner (execute=False).  They never start ROS, a
campaign, a network namespace or nft, and no built entry is execution evidence.
"""
import inspect
import json
from pathlib import Path
import tempfile
import unittest

from r2f.campaign.corpus import (
    CAPACITY, Corpus, CorpusProfileError, available_cycles, corpus_from_profile,
)
from r2f.modules.s1 import fuzz_catalog

REPO = Path(__file__).resolve().parents[1]
PROFILES = REPO / "config/s1_fuzz/profiles"

F1_ROUTABLE = set(fuzz_catalog.F1_SURFACE_IDS)
F2_PENDING = set(fuzz_catalog.F2_SURFACE_IDS)

#: The runner's current gate, read from the runner itself so this test never
#: claims support the runner does not have.
from r2f.campaign.runner import _F1_CYCLE_TARGETS

SUPPORTED_TARGETS = set(_F1_CYCLE_TARGETS)


def load_profile(name):
    return json.loads((PROFILES / f"{name}.json").read_text())


def all_seeds():
    return fuzz_catalog.load_seeds()["seeds"]


def buildable_cycles():
    rows = []
    for name in ("parameter", "environment", "composed"):
        profile = load_profile(name)
        for cycle in available_cycles(profile):
            if cycle["buildable"]:
                rows.append((name, profile, cycle))
    return rows


class ProfileDocumentContracts(unittest.TestCase):
    def test_profiles_are_disabled_and_declare_real_catalog_ids(self):
        self.assertEqual(F1_ROUTABLE,
                         {"P01", "P02", "P03", "P04", "P05", "P06", "E01", "W01", "W02"})
        self.assertEqual(F2_PENDING, {"E02", "E03", "G01"})
        for name in ("parameter", "environment", "composed"):
            data = load_profile(name)
            self.assertIs(data["enabled"], False)
            self.assertEqual(data["seed_catalog_status"], "authored_t02")
            self.assertFalse(data["required_catalog_included"])
            self.assertEqual(data["campaign_kind"], "exploratory")
            self.assertIn("owner GO", data["execution_boundary"])
            self.assertTrue(data["surfaces"])
            for surface_id in data["surfaces"]:
                self.assertIn(surface_id, fuzz_catalog.SURFACES)
                self.assertIn(surface_id, F1_ROUTABLE)
            for surface_id in data.get("pending_surfaces", []):
                self.assertIn(surface_id, F2_PENDING)
                self.assertNotIn(surface_id, data["surfaces"])
            text = json.dumps(data)
            for legacy in ("late_joiner.reader_delay\"", "discovery_drop.window_duration",
                           "\"L01\"", "\"D01\"", "pending_t02"):
                self.assertNotIn(legacy, text)

    def test_each_profile_declares_cycles_with_one_target_each(self):
        for name in ("parameter", "environment", "composed"):
            data = load_profile(name)
            self.assertTrue(data["cycles"], name)
            ids = [cycle["cycle_id"] for cycle in data["cycles"]]
            self.assertEqual(len(ids), len(set(ids)), name)
            for cycle in data["cycles"]:
                self.assertIn(cycle["target_id"], data["target_ids"])
                self.assertTrue(cycle["surfaces"])
                self.assertTrue(cycle["seed_specs"])
                # One cycle, one target: a corpus built from it can never mix
                # targets.
                for spec in cycle["seed_specs"]:
                    self.assertEqual(spec["target_id"], cycle["target_id"])
                    self.assertTrue(spec["executable"])
                    self.assertIn(spec["seed_id"], {
                        s["seed_id"] for s in data["seed_specs"]
                    })

    def test_parameter_profile_splits_late_and_projection_cycles(self):
        data = load_profile("parameter")
        cycles = {cycle["cycle_id"]: cycle for cycle in data["cycles"]}
        self.assertEqual(set(cycles), {
            "late-endpoint-parameter", "late-join-parameter", "projection-parameter",
        })
        late = cycles["late-endpoint-parameter"]
        self.assertEqual(late["target_id"], "discovery.late_endpoint.fuzz_v1")
        self.assertEqual(late["runner_slice"], "endpoint_pair_slice")
        join = cycles["late-join-parameter"]
        self.assertEqual(join["target_id"], "discovery.late_join_delivery.fuzz_v1")
        self.assertEqual(join["runner_slice"], "endpoint_pair_slice")
        projection = cycles["projection-parameter"]
        self.assertEqual(
            projection["target_id"],
            "builtin.discovery_projection_reconciliation.fuzz_v1")
        self.assertIn(projection["runner_slice"], {"endpoint_pair_slice", "projection_slice_pending"})
        self.assertEqual(set(projection["surfaces"]),
                         {"P02", "P03", "P04", "P05", "P06"})
        # Every declared parameter surface still appears in exactly one cycle.
        declared = [s for cycle in data["cycles"] for s in cycle["surfaces"]]
        self.assertEqual(set(declared), {
            "P01", "P02", "P03", "P04", "P05", "P06", "W02",
        })
        self.assertNotIn("W01", set(declared))

    def test_environment_profile_is_single_window_e01_only(self):
        data = load_profile("environment")
        self.assertEqual(data["surfaces"], ["E01"])
        self.assertEqual(len(data["cycles"]), 1)
        cycle = data["cycles"][0]
        self.assertEqual(cycle["surfaces"], ["E01"])
        specs = cycle["seed_specs"]
        self.assertTrue(specs)
        self.assertEqual({s["seed_id"] for s in specs}, {"E01"})
        self.assertEqual(len({s.get("variant_id", s["seed_id"]) for s in specs}), len(specs))
        self.assertTrue(all(0 < s["overrides"]["logical_config"]["drop_duration_sec"] <= 8
                            for s in specs if s.get("overrides")))
        self.assertNotIn("E02", cycle["surfaces"])

    def test_composed_profile_declares_parameter_and_single_window_environment(self):
        data = load_profile("composed")
        composition = data["composition"]
        self.assertEqual(composition["max_environment_windows"], 1)
        self.assertEqual(set(composition["environment_surfaces"]), {"E01"})
        self.assertTrue(set(composition["parameter_surfaces"]) <= set(data["surfaces"]))
        self.assertEqual(set(composition["parameter_surfaces"]), {"P01", "W02"})
        self.assertIn("not claimed", composition["status"])


class PerCycleBuildContracts(unittest.TestCase):
    def test_mixed_build_is_refused_without_a_cycle_selector(self):
        for name in ("parameter", "environment", "composed"):
            with self.assertRaises(CorpusProfileError) as caught:
                corpus_from_profile(PROFILES / f"{name}.json")
            self.assertIn("mixed corpus", str(caught.exception))

    def test_each_supported_cycle_builds_a_single_target_corpus(self):
        built = buildable_cycles()
        self.assertTrue(built)
        for name, profile, cycle in built:
            corpus = corpus_from_profile(profile, cycle_id=cycle["cycle_id"])
            self.assertIsInstance(corpus, Corpus)
            self.assertTrue(corpus.entries, cycle["cycle_id"])
            self.assertEqual(corpus.capacity, CAPACITY)
            self.assertEqual(corpus.to_dict()["entry_count"], len(corpus.entries))
            self.assertTrue(corpus.to_dict()["capacity_controlled"])
            self.assertEqual({record["target_id"] for record in corpus.entries.values()},
                             {cycle["target_id"]}, cycle["cycle_id"])
            self.assertEqual({record["cycle_id"] for record in corpus.entries.values()},
                             {cycle["cycle_id"]})
            self.assertEqual({record["seed_id"] for record in corpus.entries.values()},
                             {spec["seed_id"] for spec in cycle["seed_specs"]})
            for record in corpus.entries.values():
                self.assertIn(record["target_id"], SUPPORTED_TARGETS)
                self.assertIs(record["executable"], True)
                self.assertIn(record["surface_id"], F1_ROUTABLE)
                self.assertIn(record["surface_id"], cycle["surfaces"])
                self.assertIn(record["rmw"], profile["rmws"])
                self.assertTrue(record["pinned_seed"])
                self.assertEqual(record["admission_reason"], "initial_seed")
                surface = fuzz_catalog.SURFACES[record["surface_id"]]
                self.assertIn(record["boundary_class"], surface.boundary_classes)
                self.assertIn(record["mutator_id"], surface.mutator_ids)

    def test_target_id_selector_is_equivalent_for_unique_targets(self):
        profile = load_profile("parameter")
        by_cycle = corpus_from_profile(profile, cycle_id="late-endpoint-parameter")
        by_target = corpus_from_profile(
            profile, target_id="discovery.late_endpoint.fuzz_v1")
        self.assertEqual(by_cycle.to_dict()["entries"], by_target.to_dict()["entries"])

    def test_cycle_and_target_selectors_must_agree(self):
        profile = load_profile("parameter")
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(
                profile, cycle_id="late-endpoint-parameter",
                target_id="discovery.late_join_delivery.fuzz_v1")
        self.assertIn("not", str(caught.exception))

    def test_seed_templates_are_not_execution_evidence(self):
        for name, profile, cycle in buildable_cycles():
            corpus = corpus_from_profile(profile, cycle_id=cycle["cycle_id"])
            for record in corpus.entries.values():
                self.assertIs(record["derived_from_execution"], False)
                self.assertIs(record["seed_template"], True)
                self.assertEqual(record["seed_provenance_kind"], "template")
                for forbidden in ("raw_path", "raw_sha256", "runtime_result",
                                  "execution_stage", "oracle_verdict"):
                    self.assertNotIn(forbidden, record)
            self.assertTrue(corpus.history)
            self.assertTrue({row["action"] for row in corpus.history} <= {"admitted", "retained"})
            self.assertEqual({row["reason"] for row in corpus.history}, {"initial_seed"})

    def test_builder_has_no_execution_parameters(self):
        signature = inspect.signature(corpus_from_profile)
        self.assertEqual(
            [p for p in signature.parameters if p != "self"],
            ["profile", "seeds", "cycle_id", "target_id"])
        self.assertTrue(all(
            signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
            for name in ("cycle_id", "target_id")))
        for forbidden in ("runtime", "execute", "run", "max_inputs", "output_dir"):
            self.assertNotIn(forbidden, signature.parameters)

    def test_disabled_profile_still_builds_without_running(self):
        profile = load_profile("parameter")
        self.assertIs(profile["enabled"], False)
        corpus = corpus_from_profile(profile, cycle_id="late-endpoint-parameter")
        self.assertTrue(corpus.selectable())
        for record in corpus.entries.values():
            self.assertNotIn("execution_stage", record)
            self.assertNotIn("run_id", record)

    def test_build_is_deterministic_per_cycle(self):
        profile = load_profile("parameter")
        for cycle in available_cycles(profile):
            if not cycle["buildable"]:
                continue
            first = corpus_from_profile(profile, cycle_id=cycle["cycle_id"])
            second = corpus_from_profile(profile, cycle_id=cycle["cycle_id"])
            self.assertEqual(first.to_dict()["entries"], second.to_dict()["entries"])
            self.assertEqual(first.to_dict()["history"], second.to_dict()["history"])

    def test_seed_fingerprint_is_bound_and_stable(self):
        corpus = corpus_from_profile(load_profile("parameter"),
                                     cycle_id="late-endpoint-parameter")
        record = next(iter(corpus.entries.values()))
        seed = all_seeds()["P01"]
        self.assertEqual(record["seed_content_sha256"],
                         fuzz_catalog.seed_content_sha256(seed))
        from r2f.campaign.corpus import content_identity
        relabeled = dict(record, seed_id="X99", boundary_class="other",
                         observed_edge="edge", pinned_seed=False)
        self.assertEqual(content_identity(record), content_identity(relabeled))


class RunnerConsumesEachCycle(unittest.TestCase):
    """Every buildable cycle must yield planned cases, never a rejection row."""

    def _run(self, profile, cycle, max_inputs=3):
        from r2f.campaign.feedback import FeedbackLog
        from r2f.campaign.runner import run_exploratory_cycle
        from r2f.campaign.scheduler import ExploratoryScheduler

        corpus = corpus_from_profile(profile, cycle_id=cycle["cycle_id"])
        with tempfile.TemporaryDirectory() as temporary:
            result = run_exploratory_cycle(
                Path(temporary) / "cycle", corpus=corpus, feedback=FeedbackLog(),
                scheduler=ExploratoryScheduler(corpus, rng_seed=7, warmup=0),
                profile=profile, execute=False, max_inputs=max_inputs,
                run_id="profile-" + cycle["cycle_id"])
        return result, corpus

    def test_each_buildable_cycle_generates_planned_cases(self):
        built = buildable_cycles()
        self.assertTrue(built)
        for name, profile, cycle in built:
            result, corpus = self._run(profile, cycle)
            self.assertEqual(result["rejected"], [], cycle["cycle_id"])
            self.assertGreaterEqual(len(result["rows"]), 1, cycle["cycle_id"])
            self.assertEqual(result["stop_reason"], "execute_false_planned_only")
            for row in result["rows"]:
                self.assertEqual(row["execution_stage"], "planned", cycle["cycle_id"])
                self.assertIs(row["actual_run"], False)
                self.assertEqual(row["target_id"], cycle["target_id"])
                self.assertIsNone(row.get("reason_code"), cycle["cycle_id"])
                self.assertIsNotNone(row.get("mutation_plan"))
                self.assertIn(row["target_id"], SUPPORTED_TARGETS)

    def test_projection_and_recovery_cycles_generate_planned_only(self):
        cases = (("parameter", "projection-parameter",
                  "builtin.discovery_projection_reconciliation.fuzz_v1"),
                 ("environment", "recovery-single-window",
                  "discovery.interruption_recovery.fuzz_v1"),
                 ("composed", "recovery-composed",
                  "discovery.interruption_recovery.fuzz_v1"))
        for name, cycle_id, target_id in cases:
            profile = load_profile(name)
            corpus = corpus_from_profile(profile, cycle_id=cycle_id)
            result, _ = self._run(profile, {"cycle_id": cycle_id,
                                            "target_id": target_id}, max_inputs=1)
            self.assertEqual(result["rejected"], [], cycle_id)
            row = result["rows"][0]
            self.assertEqual(row["target_id"], target_id)
            self.assertEqual(row["execution_stage"], "planned")
            self.assertIs(row["actual_run"], False)
            self.assertIsNotNone(row["mutation_plan"])
            self.assertEqual(row["binding"]["status"], "preserved", row["binding"])

    def test_available_cycles_never_overstates_runner_support(self):
        for name in ("parameter", "environment", "composed"):
            profile = load_profile(name)
            for cycle in available_cycles(profile):
                self.assertEqual(
                    cycle["buildable"],
                    cycle["runner_slice"] == "endpoint_pair_slice"
                    and cycle["target_id"] in SUPPORTED_TARGETS)
                if not cycle["buildable"]:
                    self.assertTrue(cycle["reason"].startswith("unsupported_capability:"))


class PendingAndUnknownContracts(unittest.TestCase):
    def test_f2_seed_specs_are_rejected(self):
        for seed_id in ("E02", "E03", "G01"):
            data = load_profile("composed")
            for cycle in data["cycles"]:
                if cycle["cycle_id"] == "late-endpoint-composed":
                    cycle["seed_specs"] = [{
                        "seed_id": seed_id,
                        "target_id": cycle["target_id"],
                        "boundary_class": "finite",
                        "executable": True,
                    }]
            with self.assertRaises(CorpusProfileError) as caught:
                corpus_from_profile(data, cycle_id="late-endpoint-composed")
            self.assertIn(seed_id, str(caught.exception))

    def test_f2_surfaces_are_rejected_as_live(self):
        for surface_id in ("E02", "E03", "G01"):
            data = load_profile("environment")
            data["surfaces"] = ["E01", surface_id]
            data["cycles"][0]["surfaces"] = ["E01", surface_id]
            with self.assertRaises(CorpusProfileError) as caught:
                corpus_from_profile(data, cycle_id="recovery-single-window")
            self.assertIn(surface_id, str(caught.exception))

    def test_pending_ids_listed_as_metadata_are_accepted_but_not_built(self):
        for name, profile, cycle in buildable_cycles():
            pending = [s for s in profile["seed_specs"] if s["seed_id"] in ("E02", "E03", "G01")]
            corpus = corpus_from_profile(profile, cycle_id=cycle["cycle_id"])
            seeds = {record["seed_id"] for record in corpus.entries.values()}
            for pending_seed in pending:
                self.assertNotIn(pending_seed["seed_id"], seeds)
            for pending_id in ("E02", "E03", "G01"):
                self.assertNotIn(pending_id, seeds)

    def test_pending_metadata_must_be_genuinely_pending(self):
        data = load_profile("parameter")
        data["pending_seed_ids"] = ["P01"]
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("P01", str(caught.exception))

    def test_live_and_pending_surface_overlap_is_rejected(self):
        # Use a runner-supported cycle so the capability gate is not what
        # trips first; the contradiction must be what is reported.
        data = load_profile("parameter")
        data["pending_surfaces"] = ["P01"]
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("P01", str(caught.exception))

    def test_unknown_cycle_or_target_selector_is_rejected(self):
        profile = load_profile("parameter")
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(profile, cycle_id="no-such-cycle")
        self.assertIn("no-such-cycle", str(caught.exception))
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(profile, target_id="no.such.target")
        self.assertIn("no.such.target", str(caught.exception))

    def test_unknown_surface_is_rejected(self):
        data = load_profile("parameter")
        data["surfaces"] = data["surfaces"] + ["NOPE99"]
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                cycle["surfaces"] = list(cycle["surfaces"]) + ["NOPE99"]
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("NOPE99", str(caught.exception))

    def test_unknown_seed_is_rejected(self):
        data = load_profile("parameter")
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                cycle["seed_specs"] = [{
                    "seed_id": "Z99",
                    "target_id": cycle["target_id"],
                    "boundary_class": "finite",
                    "executable": True,
                }]
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("Z99", str(caught.exception))

    def test_seed_missing_from_provided_map_is_rejected(self):
        profile = load_profile("parameter")
        seeds = {key: value for key, value in all_seeds().items() if key != "P01"}
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(profile, seeds=seeds, cycle_id="late-endpoint-parameter")
        self.assertIn("P01", str(caught.exception))

    def test_seed_target_family_mismatch_is_rejected(self):
        profile = load_profile("parameter")
        seeds = all_seeds()
        mismatched = json.loads(json.dumps(seeds))
        mismatched["P01"]["target_family"] = "discovery.late_join_delivery"
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(profile, seeds=mismatched,
                                cycle_id="late-endpoint-parameter")
        self.assertTrue(any(text in str(caught.exception)
                            for text in ("target_family", "not declared in the selected cycle")))

    def test_seed_cross_cycle_target_is_rejected(self):
        data = load_profile("parameter")
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                # P02 belongs to the projection target; reuse it here only to
                # prove the seed's target_family is checked.
                cycle["seed_specs"] = [{
                    "seed_id": "P02",
                    "target_id": cycle["target_id"],
                    "boundary_class": "enum_exhaustive",
                    "executable": True,
                }]
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertTrue(any(text in str(caught.exception)
                            for text in ("target_family", "not declared in the selected cycle")))

    def test_surface_not_declared_by_target_is_rejected(self):
        # P02 is a valid F1 surface, but the late-endpoint target does not
        # declare it.  Put it in this cycle only; every other cycle stays
        # consistent with the top-level surface union.
        data = load_profile("parameter")
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                cycle["surfaces"] = ["P01", "P02"]
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("P02", str(caught.exception))

    def test_boundary_class_must_be_declared_by_surface(self):
        data = load_profile("parameter")
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                cycle["seed_specs"][0]["boundary_class"] = "no_such_boundary"
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("no_such_boundary", str(caught.exception))

    def test_profile_must_stay_disabled_and_exploratory(self):
        data = load_profile("parameter")
        data["enabled"] = True
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("enabled=false", str(caught.exception))

        data = load_profile("parameter")
        data["required_catalog_included"] = True
        with self.assertRaises(CorpusProfileError):
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")

        data = load_profile("parameter")
        data["campaign_kind"] = "required"
        with self.assertRaises(CorpusProfileError):
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")

        data = load_profile("parameter")
        data["seed_catalog_status"] = "pending_t02"
        with self.assertRaises(CorpusProfileError):
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")

    def test_non_executable_seed_spec_is_rejected(self):
        data = load_profile("parameter")
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                cycle["seed_specs"][0]["executable"] = False
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("executable", str(caught.exception))

    def test_cycle_may_not_contain_a_seed_for_another_target(self):
        data = load_profile("parameter")
        for cycle in data["cycles"]:
            if cycle["cycle_id"] == "late-endpoint-parameter":
                cycle["seed_specs"].append({
                    "seed_id": "W02",
                    "target_id": "discovery.late_join_delivery.fuzz_v1",
                    "boundary_class": "finite",
                    "executable": True,
                })
        with self.assertRaises(CorpusProfileError) as caught:
            corpus_from_profile(data, cycle_id="late-endpoint-parameter")
        self.assertIn("different target", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
