"""V22: offline corpus capacity, fixed seeds and candidate pinning."""
import json
from pathlib import Path
import tempfile
import unittest

from r2f.campaign.corpus import CAPACITY, Corpus, content_identity

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
        expected = {"parameter", "environment", "composed"}
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
        self.assertEqual(seen, expected)

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
