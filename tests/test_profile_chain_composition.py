"""Real offline input layers, kept distinct from ROS execution and E01 coverage."""
from copy import deepcopy
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import Mock, patch

from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.exploratory_input import validate_f2_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import MutationPlan
from r2f.core.mutators import mutate, mutate_wide
from r2f.modules.s1.targets import exploratory_target_declarations

ROOT = Path(__file__).resolve().parents[1]


def profile(name="endpoint-chains"):
    return json.loads((ROOT / f"config/s1_fuzz/profiles/{name}.json").read_text())


class EndpointChainProfile(unittest.TestCase):
    def planned_case(self, doc, cycle_id, rmw, rng_seed=3):
        doc = deepcopy(doc)
        doc["rmws"] = [rmw]
        corpus = corpus_from_profile(doc, cycle_id=cycle_id)
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=rng_seed, warmup=0)
        runtime = Mock(side_effect=AssertionError("offline chain test must never run DDS"))
        proposals = []

        def capture_proposal(*args, **kwargs):
            proposal = mutate_wide(*args, **kwargs)
            proposals.append(proposal.to_dict())
            return proposal

        with tempfile.TemporaryDirectory() as tmp:
            with patch("r2f.campaign.runner.mutate_wide", side_effect=capture_proposal), patch(
                    "r2f.campaign.runner.source_manifest", return_value={
                    "source_repo": str(ROOT), "head": "a" * 40,
                    "behavior_tree_sha256": "b" * 64, "source_frozen": True,
                    "dirty_behavior_paths": []}):
                report = run_exploratory_cycle(
                    Path(tmp) / "out", corpus=corpus, feedback=feedback,
                    scheduler=scheduler, profile=doc, run_id="chain",
                    max_inputs=1, execute=False, runtime=runtime)
            self.assertEqual(len(report["rows"]), 1)
            row = report["rows"][0]
            saved_plan = json.loads((Path(row["replay_path"]).parent / "mutation_plan.json").read_text())
            config = json.loads(Path(row["runtime_config_path"]).read_text())
        runtime.assert_not_called()
        self.assertEqual(row["mutation_plan"], saved_plan)
        self.assertEqual(config["mutation_plan"], saved_plan)
        self.assertEqual(config["final_input"], saved_plan["final_config"])
        return row, proposals

    def assert_real_two_layers(self, row, proposals, surfaces):
        self.assertEqual(row["execution_stage"], "planned")
        self.assertIs(row["actual_run"], False)
        self.assertEqual(row["oracle_verdict"], "unknown")
        self.assertEqual(row["binding"]["status"], "preserved")
        self.assertIs(row["binding"]["silent_normalization"], False)
        self.assertTrue(all(value is False for value in row["mutation_receipt"].values()))
        self.assertEqual(row["chain_depth"], 2)
        plan = MutationPlan.from_dict(row["mutation_plan"])
        ops = row["mutation_plan"]["operations"]
        self.assertEqual([op["surface_id"] for op in ops], surfaces)
        self.assertEqual(len(ops), 2)
        self.assertEqual(row["mutation_chain"], ops)
        self.assertEqual(row["mutation_plan_sha256"], canonical_hash(plan.to_dict()))
        # Check both raw proposal snapshots, not just a claimed chain length.
        self.assertEqual(len(proposals), 2)
        self.assertEqual(ops[0]["input_before_sha256"], plan.parent_content_sha256)
        for op, proposal in zip(ops, proposals):
            self.assertEqual(op["input_before_sha256"], canonical_hash(proposal["input_before"]))
            self.assertEqual(op["input_after_sha256"], canonical_hash(proposal["input_after"]))
            self.assertNotEqual(proposal["input_before"], proposal["input_after"])
            self.assertNotEqual(op["input_before_sha256"], op["input_after_sha256"])
        self.assertEqual(proposals[0]["input_after"], proposals[1]["input_before"])
        self.assertEqual(ops[0]["input_after_sha256"], ops[1]["input_before_sha256"])
        self.assertEqual(plan.final_config, proposals[-1]["input_after"])
        self.assertEqual(ops[-1]["input_after_sha256"], canonical_hash(plan.final_config))
        self.assertEqual(plan.runtime_windows, [])
        self.assertNotIn("environment_windows", plan.final_config)

    def test_both_endpoint_cycles_generate_two_real_contiguous_layers(self):
        doc = profile()
        self.assertEqual(doc["chain_length"], 2)
        expected = {
            "late-endpoint-chain": ["P01", "W02"],
            "late-join-chain": ["W02", "P01"],
        }
        self.assertEqual({c["cycle_id"] for c in doc["cycles"]}, set(expected))
        for cycle_id, surfaces in expected.items():
            for rmw in doc["rmws"]:
                for rng_seed in (3, 11, 29):
                    with self.subTest(cycle=cycle_id, rmw=rmw, rng_seed=rng_seed):
                        row, proposals = self.planned_case(doc, cycle_id, rmw, rng_seed)
                        self.assert_real_two_layers(row, proposals, surfaces)
                        if surfaces[0] == "P01":
                            first = proposals[0]
                            self.assertGreater(first["input_before"]["reader_delay_sec"], 0)
                            self.assertEqual(first["parameters"]["boundary_class"], "zero")
                            self.assertEqual(first["input_after"]["reader_delay_sec"], 0.0)

    def test_named_finite_layer_is_not_counted_as_a_real_content_change(self):
        # This deliberately reintroduces the old no-op recipe in memory.  The
        # runner may represent it, but our two-real-layer evidence check must
        # reject it instead of changing M01's legacy semantics to make it pass.
        doc = profile()
        for specs in (doc["seed_specs"], doc["declared_seed_specs_union"],
                      doc["cycles"][0]["seed_specs"]):
            specs[0]["boundary_class"] = "finite"
        row, proposals = self.planned_case(doc, "late-endpoint-chain", doc["rmws"][0])
        self.assertEqual(row["chain_depth"], 2)
        self.assertEqual(proposals[0]["input_before"], proposals[0]["input_after"])
        with self.assertRaises(AssertionError):
            self.assert_real_two_layers(row, proposals, ["P01", "W02"])

    def test_same_profile_and_rng_reproduce_identical_plans(self):
        doc = profile()
        for cycle in doc["cycles"]:
            with self.subTest(cycle=cycle["cycle_id"]):
                first, first_proposals = self.planned_case(doc, cycle["cycle_id"], doc["rmws"][0])
                second, second_proposals = self.planned_case(doc, cycle["cycle_id"], doc["rmws"][0])
                self.assertEqual(first_proposals, second_proposals)
                for key in ("mutation_plan", "mutation_plan_sha256", "effective_input_sha256",
                            "input_layer_receipts", "final_input"):
                    self.assertEqual(first[key], second[key], key)

    def test_f2_profile_remains_chain1_and_rejects_endpoint_chain_opt_in(self):
        f2 = profile("f2-windows")
        self.assertEqual(f2["chain_length"], 1)
        self.assertEqual(f2["surfaces"], ["E02", "E03"])
        self.assertEqual(f2["mutator_whitelist"], {"E02": ["M07"], "E03": ["M06"]})
        self.assertTrue(validate_f2_profile(f2))
        f2["chain_length"] = 2
        with self.assertRaisesRegex(ValueError, "f2_windows_requires_chain_length_1"):
            validate_f2_profile(f2)


class LegacyFiniteBoundary(unittest.TestCase):
    """A named finite witness is not permission to change legacy input bytes."""
    def setUp(self):
        targets = exploratory_target_declarations()
        self.surface = targets["discovery.late_endpoint.fuzz_v1"].exploration_contract.surface_by_id["P01"]

    def test_finite_authored_witness_and_rng_state_remain_legacy_compatible(self):
        for value in (0.001, 0.02, 0.15, 1.0, 2):
            for seed in (0, 3, 29):
                with self.subTest(value=value, seed=seed):
                    parent = {"reader_delay_sec": value, "unchanged": {"field": True}}
                    rng = random.Random(seed)
                    result = mutate(parent, self.surface, rng,
                                    {"boundary_class": "finite"}, "M01")
                    legacy_rng = random.Random(seed)
                    legacy_rng.randrange(1)
                    self.assertEqual(rng.getstate(), legacy_rng.getstate())
                    self.assertEqual(result.input_before, parent)
                    self.assertEqual(result.input_after, parent)
                    self.assertEqual(result.changes, {"reader_delay_sec": value})
                    self.assertEqual(result.parameters, {"boundary_class": "finite"})
                    self.assertEqual(result.notes, {"selected_class": "finite"})
                    wide = mutate_wide(parent, self.surface, random.Random(seed),
                                       {"boundary_class": "finite"}, "M01")
                    self.assertEqual(wide.input_after, parent)
                    self.assertEqual(wide.changes, result.changes)

    def test_explicit_zero_boundary_changes_finite_parent_without_new_interface(self):
        parent = {"reader_delay_sec": 0.15}
        result = mutate(parent, self.surface, random.Random(3),
                        {"boundary_class": "zero"}, "M01")
        self.assertEqual(parent, {"reader_delay_sec": 0.15})
        self.assertEqual(result.input_after, {"reader_delay_sec": 0.0})
        self.assertNotEqual(canonical_hash(result.input_before), canonical_hash(result.input_after))


if __name__ == "__main__":
    unittest.main()
