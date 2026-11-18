"""V22: offline corpus capacity, fixed seeds and candidate pinning."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from r2f.campaign.corpus import CAPACITY, Corpus, content_identity
from r2f.campaign.exploratory_input import validate_profile_fields

REPO = Path(__file__).resolve().parents[1]


def entry(index, *, seed_id=None, boundary=None, relation=None, edge=None, target="discovery.late_endpoint"):
    value = {
        "target_id": target,
        "target_revision": 0,
        "rmw": "rmw_cyclonedds_cpp",
        "contract_hash": "contract-v1",
        "surface_id": "late_joiner.reader_delay",
        "boundary_class": boundary or f"boundary-{index}",
        "relation_class": relation or f"relation-{index}",
        "final_config": {"reader_delay_sec": index / 1000},
        "executable": True,
    }
    if seed_id:
        value["seed_id"] = seed_id
    if edge:
        value["observed_edge"] = edge
    return value


class FuzzCorpusContracts(unittest.TestCase):
    def test_profiles_explicit_exploratory_disabled_and_not_required_catalog(self):
        expected = {"parameter", "environment", "composed", "f2-windows",
                    "endpoint-chains", "topology-chains", "common-baseline-routes"}
        f2_target = "discovery.interruption_recovery.fuzz_windows_v1"
        f2_mutators = {"E02": ["M07"], "E03": ["M06"]}
        seen = set()
        for path in sorted((REPO / "config/s1_fuzz/profiles").glob("*.json")):
            data = json.loads(path.read_text())
            seen.add(path.stem)
            self.assertEqual(data["campaign_kind"], "exploratory")
            self.assertTrue(data["profile_id"])
            self.assertIs(data["enabled"], False)
            self.assertIs(data["required_catalog_included"], False)
            self.assertTrue(data["seed_specs"])
            self.assertNotIn(path.stem, ("s1_scope",))
            if path.stem == "f2-windows":
                # A new explicit opt-in is not blanket F2/default activation.
                self.assertEqual(data["runtime_capability"], "f2_windows_v1")
                self.assertIs(type(data["chain_length"]), int)
                self.assertEqual(data["chain_length"], 1)
                self.assertEqual(data["target_ids"], [f2_target])
                self.assertEqual(data["surfaces"], ["E02", "E03"])
                self.assertEqual(data["mutator_whitelist"], f2_mutators)
                self.assertEqual(data["anchor_timing_policy"], "f2_anchor_not_before_v1")
                self.assertEqual(data["anchor_lateness_tolerance_sec"], 0.5)
                self.assertEqual([cycle["surfaces"] for cycle in data["cycles"]],
                                 [["E02"], ["E03"]])
                for cycle in data["cycles"]:
                    self.assertEqual(cycle["target_id"], f2_target)
                    self.assertTrue(cycle["seed_specs"])
                    for spec in cycle["seed_specs"]:
                        self.assertEqual(spec["target_id"], f2_target)
                        self.assertEqual(spec["seed_id"], cycle["surfaces"][0])
                for spec in data["seed_specs"]:
                    self.assertEqual(spec["target_id"], f2_target)
                    self.assertIn(spec["seed_id"], f2_mutators)
            elif path.stem == "topology-chains":
                # This dedicated opt-in does not widen F2 multiwindow or defaults.
                self.assertEqual(data["profile_id"], "s1-topology-chains")
                self.assertEqual(data["runtime_capability"], "topology_v1")
                self.assertIs(type(data["chain_length"]), int)
                self.assertEqual(data["chain_length"], 2)
                self.assertEqual(data["target_ids"], ["discovery.endpoint_topology.fuzz_v1"])
                self.assertNotIn(f2_target, data["target_ids"])
                self.assertEqual(data["surfaces"], ["S1.topology.program"])
                self.assertEqual(data["mutator_whitelist"], ["I2.topology_program.v1"])
                self.assertTrue(data["operator_whitelist"])
                self.assertEqual(data["source_schema"], "r2f.s1-seed-corpus/v2")
                self.assertEqual(data["seed_catalog_status"], "authored_v2")
                self.assertEqual(data["seed_specs"], data["declared_seed_specs_union"])
                self.assertEqual([spec for cycle in data["cycles"] for spec in cycle["seed_specs"]],
                                 data["declared_seed_specs_union"])
                validate_profile_fields(data)
            else:
                self.assertNotIn("runtime_capability", data)
                self.assertNotIn(f2_target, data["target_ids"])
            if path.stem == "common-baseline-routes":
                self.assertEqual(data["composition"]["mutation_routes_schema"],
                                 "r2f.mutation-routes/v1")
                self.assertEqual(data["target_ids"], ["discovery.late_endpoint.fuzz_v1"])
                self.assertEqual(data["surfaces"], ["P01", "W02"])
                self.assertEqual(data["chain_length"], 1)
                self.assertEqual(data["seed_specs"], data["declared_seed_specs_union"])
                self.assertEqual(data["cycles"][0]["seed_specs"], data["seed_specs"])
                self.assertEqual([r["surface_id"] for r in data["seed_specs"][0]["mutation_routes"]],
                                 ["P01", "W02"])
                validate_profile_fields(data)
            if path.stem == "endpoint-chains":
                # Explicit new offline profile, not blanket F2/runtime activation.
                # The generic exploratory/disabled/not-required assertions above
                # apply unchanged to this profile and all four original ones.
                self.assertEqual(data["profile_id"], "s1-fuzz-endpoint-chains")
                self.assertIs(type(data["chain_length"]), int)
                self.assertEqual(data["chain_length"], 2)
                self.assertEqual(data["target_ids"], [
                    "discovery.late_endpoint.fuzz_v1",
                    "discovery.late_join_delivery.fuzz_v1",
                ])
                self.assertEqual(data["surfaces"], ["P01", "W02"])
                self.assertEqual([cycle["cycle_id"] for cycle in data["cycles"]],
                                 ["late-endpoint-chain", "late-join-chain"])
                self.assertEqual(data["composition"]["environment_surfaces"], [])
                self.assertEqual(data["composition"]["max_environment_windows"], 0)
        self.assertEqual(seen, expected)

    def test_topology_chain_lengths_1_to_4_do_not_widen_f2_chain1(self):
        profiles = REPO / "config/s1_fuzz/profiles"
        topology = json.loads((profiles / "topology-chains.json").read_text())
        self.assertEqual(topology["chain_length"], 2)
        for length in (1, 2, 3, 4):
            with self.subTest(topology_chain_length=length):
                trial = deepcopy(topology)
                trial["chain_length"] = length
                validate_profile_fields(trial)
        for length in (0, 5, -1, 2.0, True, "2", None):
            with self.subTest(invalid_topology_chain_length=length):
                trial = deepcopy(topology)
                trial["chain_length"] = length
                with self.assertRaisesRegex(ValueError, "topology_requires_chain_length_1_to_4"):
                    validate_profile_fields(trial)
        f2 = json.loads((profiles / "f2-windows.json").read_text())
        self.assertEqual(f2["chain_length"], 1)
        validate_profile_fields(f2)
        for length in (0, 2, 3, 4):
            with self.subTest(invalid_f2_chain_length=length):
                trial = deepcopy(f2)
                trial["chain_length"] = length
                with self.assertRaisesRegex(ValueError, "f2_windows_requires_chain_length_1"):
                    validate_profile_fields(trial)

    def test_topology_profile_requires_exact_capability_and_offline_scope(self):
        topology = json.loads((REPO / "config/s1_fuzz/profiles/topology-chains.json").read_text())
        for capability in (None, "f2_windows_v1", "topology", "discovery.endpoint_topology.fuzz_v1"):
            with self.subTest(capability=capability):
                trial = deepcopy(topology)
                if capability is None:
                    trial.pop("runtime_capability")
                else:
                    trial["runtime_capability"] = capability
                with self.assertRaisesRegex(ValueError, "topology_runtime_capability_required"):
                    validate_profile_fields(trial)
        for key, value in (("enabled", True), ("required_catalog_included", True),
                           ("campaign_kind", "required")):
            with self.subTest(field=key):
                trial = deepcopy(topology)
                trial[key] = value
                with self.assertRaisesRegex(ValueError, "topology_profile_contract_mismatch:" + key):
                    validate_profile_fields(trial)

    def test_capacity_256_fixed_seeds_and_candidate_inputs_survive(self):
        corpus = Corpus()
        first = corpus.add_seed(entry(0, seed_id="L01", boundary="seed"))
        second = corpus.add_seed(entry(1, seed_id="L02", boundary="seed"))
        first_sha, second_sha = first["content_sha256"], second["content_sha256"]
        for index in range(2, 302):
            outcome = corpus.offer(entry(index), executed=True, evidence_complete=True,
                                   oracle_verdict="unknown")
            self.assertTrue(outcome["admitted"], outcome)
        self.assertEqual(CAPACITY, 256)
        self.assertEqual(len(corpus._evictable()), CAPACITY)
        self.assertEqual(len(corpus.entries), CAPACITY + 2)
        self.assertTrue(corpus.to_dict()["capacity_controlled"])
        self.assertIn(first_sha, corpus.entries)
        self.assertIn(second_sha, corpus.entries)
        self.assertTrue(corpus.entries[first_sha]["pinned_seed"])
        self.assertTrue(corpus.entries[second_sha]["pinned_seed"])
        candidate = entry(999, boundary="candidate", relation="candidate-relation")
        pinned = corpus.pin_candidate(candidate, "candidate-1")
        self.assertEqual(pinned["candidate_ids"], ["candidate-1"])
        self.assertIn(pinned["content_sha256"], corpus.entries)
        self.assertTrue(corpus.entries[first_sha]["pinned_seed"])
        self.assertIn(first_sha, corpus.entries)
        self.assertIn(second_sha, corpus.entries)

    def test_content_identity_whitelists_input_not_observation_labels(self):
        base = entry(7)
        base_hash = content_identity(base)
        relabeled = dict(base, observed_edge="edge-A", boundary_class="other",
                         surface_id="other.surface", relation_class="other-relation",
                         contract_branch="other-branch", oracle_verdict="fail",
                         seed_id="L99", pinned_seed=True, source="candidate",
                         bucket_key=["x"], admission_reason="new_state_edge",
                         run_id="run-2", domain_id=99, timestamp="later", repeat=2,
                         rng_seed=55)
        self.assertEqual(base_hash, content_identity(relabeled))
        changed = dict(base, final_config={"reader_delay_sec": 0.999})
        self.assertNotEqual(base_hash, content_identity(changed))
        changed_contract = dict(base, contract_hash="contract-v2")
        self.assertNotEqual(base_hash, content_identity(changed_contract))

    def test_incomplete_fail_is_rejected_but_complete_fail_with_new_edge_is_admitted(self):
        corpus = Corpus()
        value = entry(10, boundary="incomplete", edge="edge-incomplete")
        incomplete = corpus.offer(value, executed=True, evidence_complete=False,
                                  observed_edge="edge-incomplete", oracle_verdict="fail")
        self.assertFalse(incomplete["admitted"], incomplete)
        self.assertEqual(incomplete["reason"], "incomplete_observation")
        self.assertNotIn(incomplete["content_sha256"], corpus.entries)
        complete_value = entry(11, boundary="complete", edge="edge-new")
        complete_value.pop("relation_class")
        complete = corpus.offer(complete_value, executed=True, evidence_complete=True,
                                observed_edge="edge-new", oracle_verdict="fail")
        self.assertTrue(complete["admitted"], complete)
        self.assertEqual(complete["reason"], "new_state_edge")
        self.assertIn(complete["content_sha256"], corpus.entries)

    def test_duplicate_content_is_deduplicated_and_success_alone_is_not_admitted(self):
        corpus = Corpus()
        value = entry(7)
        digest = content_identity(value)
        self.assertEqual(digest, content_identity(dict(value, run_id="different", domain_id=99)))
        first = corpus.offer(value, executed=True, evidence_complete=True, oracle_verdict="unknown")
        self.assertTrue(first["admitted"], first)
        duplicate = corpus.offer(dict(value, run_id="different", domain_id=99),
                                 executed=True, evidence_complete=True, oracle_verdict="unknown")
        self.assertFalse(duplicate["admitted"])
        self.assertEqual(duplicate["reason"], "duplicate_content")
        # A completed pass with no new relation/edge/contract branch is not
        # an infinite corpus admission reason.
        pass_value = entry(8, boundary="new-boundary", relation=None)
        pass_value.pop("relation_class")
        result = corpus.offer(pass_value, executed=True, evidence_complete=True,
                              oracle_verdict="pass")
        self.assertFalse(result["admitted"], result)
        self.assertEqual(result["reason"], "success_only")

    def test_bucket_keeps_at_most_three_representatives_preferring_complete_short(self):
        corpus = Corpus()
        for index, (complete, cost) in enumerate(((True, 3), (False, 1), (True, 2),
                                                  (True, 4), (True, 5))):
            value = entry(index, boundary="same", relation=f"r{index}", edge="same-edge")
            value["cost"] = cost
            corpus.offer(value, executed=True, evidence_complete=complete,
                         observed_edge="same-edge", oracle_verdict="unknown")
        records = [r for r in corpus.entries.values() if r["boundary_class"] == "same"]
        self.assertEqual(len(records), 3)
        self.assertTrue(all(r["evidence_complete"] for r in records))
        self.assertEqual(sorted(r["cost"] for r in records), [2, 3, 4])
        actions = [h["action"] for h in corpus.to_dict()["history"]]
        self.assertIn("evicted", actions)
        self.assertTrue(any(h["reason"] == "bucket_representative_limit" for h in corpus.to_dict()["history"]))


if __name__ == "__main__":
    unittest.main()
