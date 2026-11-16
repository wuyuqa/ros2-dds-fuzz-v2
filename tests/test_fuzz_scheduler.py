"""V21/V23: exploratory feedback selection is deterministic and replayable."""
import unittest

from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler, first_slice_cells, replay_selection


def seed(index, *, surface="late_joiner.reader_delay", boundary="baseline", edge=None):
    value = {
        "seed_id": f"L{index:02d}",
        "target_id": "discovery.late_endpoint",
        "target_revision": 0,
        "rmw": "rmw_cyclonedds_cpp",
        "contract_hash": "contract-v1",
        "surface_id": surface,
        "boundary_class": boundary,
        "final_config": {"reader_delay_sec": index / 1000},
        "executable": True,
    }
    if edge:
        value["observed_edge"] = edge
    return value


def observed(content_sha256, *, surface, boundary, edge):
    return {
        "content_sha256": content_sha256,
        "run_id": "run-1",
        "target_id": "discovery.late_endpoint",
        "execution_stage": "judged",
        "actual_run": True,
        "oracle_verdict": "pass",
        "surface_id": surface,
        "boundary_class": boundary,
        "observed_edge": edge,
        "typed_facts": {"environment_healthy": True, "process_healthy": True,
                        "preconditions_reached": True, "endpoint_identity_complete": True,
                        "window": {"complete": True}},
    }


class FuzzSchedulerContracts(unittest.TestCase):
    def make_corpus(self):
        corpus = Corpus()
        for index in range(4):
            corpus.add_seed(seed(index, boundary=("baseline", "short", "long", "baseline")[index]))
        return corpus

    def test_v21_new_edge_feedback_influences_next_cycle_and_uniform_branch_is_reachable(self):
        corpus = self.make_corpus()
        rows = corpus.selectable()
        # Feedback makes the third seed's edge observable and rare; selection
        # must prefer an entry carrying that edge when the rare-edge phase runs.
        edge_row = rows[2]
        log = FeedbackLog()
        log.record(observed(edge_row["content_sha256"], surface=edge_row["surface_id"],
                            boundary=edge_row["boundary_class"], edge="edge-A"))
        scheduler = ExploratoryScheduler(corpus, log, rng_seed=9, warmup=0)
        plan = scheduler.plan(5)
        self.assertEqual([p["requested_pool"] for p in plan],
                         ["uniform", "uncovered", "uncovered", "rare_edge", "rare_edge"])
        rare = [p for p in plan if p["requested_pool"] == "rare_edge"]
        self.assertTrue(rare)
        self.assertTrue(all(p["content_sha256"] == edge_row["content_sha256"] for p in rare))
        self.assertEqual(plan[0]["pool"], "uniform")
        self.assertEqual(plan[0]["fallback_chain"], [])
        # Without a recorded edge the rare pool is empty and falls back in order.
        empty = ExploratoryScheduler(corpus, FeedbackLog(), rng_seed=9, warmup=0)
        fallback_plan = empty.plan(4)
        self.assertEqual(fallback_plan[3]["requested_pool"], "rare_edge")
        self.assertEqual(fallback_plan[3]["fallback_chain"], ["rare_edge", "uniform"])
        self.assertEqual(fallback_plan[3]["used_pool"], "uniform")

    def test_v21_empty_uncovered_pool_falls_back_to_uniform(self):
        corpus = self.make_corpus()
        log = FeedbackLog()
        for row in corpus.selectable():
            log.record(observed(row["content_sha256"], surface=row["surface_id"],
                                boundary=row["boundary_class"], edge="edge-" + row["seed_id"]))
        self.assertEqual(len(log.covered_surfaces()), 3)
        scheduler = ExploratoryScheduler(corpus, log, rng_seed=11, warmup=0)
        plan = scheduler.plan(2)
        self.assertEqual(plan[1]["requested_pool"], "uncovered")
        self.assertEqual(plan[1]["fallback_chain"], ["uncovered", "uniform"])
        self.assertEqual(plan[1]["used_pool"], "uniform")
        self.assertGreater(plan[1]["candidate_count"], 0)

    def test_v23_same_feedback_and_rng_replay_selection_order_changes_identity(self):
        corpus = self.make_corpus()
        rows = corpus.selectable()
        first = observed(rows[0]["content_sha256"], surface=rows[0]["surface_id"],
                         boundary=rows[0]["boundary_class"], edge="edge-A")
        second = observed(rows[1]["content_sha256"], surface=rows[1]["surface_id"],
                          boundary=rows[1]["boundary_class"], edge="edge-B")
        log = FeedbackLog()
        log.record(first)
        log.record(second)
        scheduler = ExploratoryScheduler(corpus, log, rng_seed=77, warmup=1)
        selections = scheduler.plan(7)
        recorded = scheduler.to_dict()
        replayed = replay_selection(recorded, corpus, log.events)
        self.assertTrue(replayed["valid"])
        self.assertEqual([s["selection_sha256"] for s in replayed["selections"]],
                         [s["selection_sha256"] for s in selections])
        swapped = FeedbackLog()
        swapped.record(second)
        swapped.record(first)
        self.assertNotEqual(log.identity(), swapped.identity())
        with self.assertRaises(ValueError):
            replay_selection(recorded, corpus, swapped.events)

    def test_v23_replay_survives_feedback_appended_after_selection(self):
        corpus = self.make_corpus()
        rows = corpus.selectable()
        log = FeedbackLog()
        log.record(observed(rows[0]["content_sha256"], surface=rows[0]["surface_id"],
                            boundary=rows[0]["boundary_class"], edge="edge-A"))
        log.record(observed(rows[1]["content_sha256"], surface=rows[1]["surface_id"],
                            boundary=rows[1]["boundary_class"], edge="edge-B"))
        scheduler = ExploratoryScheduler(corpus, log, rng_seed=53, warmup=0)
        selections = scheduler.plan(3)
        recorded = scheduler.to_dict()
        identity_before = recorded["selection_identity"]
        self.assertEqual(recorded["feedback_event_count"], 2)

        # (1) Append an event after selection, then replay against the extended
        # full log.  The frozen prefix, not the extended log, drives rebuilding.
        log.record(observed(rows[2]["content_sha256"], surface=rows[2]["surface_id"],
                            boundary=rows[2]["boundary_class"], edge="edge-C"))
        extended = replay_selection(recorded, corpus, log.events)
        self.assertTrue(extended["valid"])
        self.assertEqual([s["selection_sha256"] for s in extended["selections"]],
                         [s["selection_sha256"] for s in selections])

        # (2) Explicitly truncating to the recorded prefix must also replay.
        prefix = replay_selection(recorded, corpus, log.events[:recorded["feedback_event_count"]])
        self.assertTrue(prefix["valid"])
        self.assertEqual([s["selection_sha256"] for s in prefix["selections"]],
                         [s["selection_sha256"] for s in selections])

        # (3) Appending feedback must not mutate the already-recorded run identity.
        self.assertEqual(scheduler.to_dict()["selection_identity"], identity_before)
        self.assertEqual(recorded["feedback_identity"], scheduler.feedback_identity_at_start)

        # Negative: reordering two events inside the recorded prefix must fail.
        reordered = [log.events[1], log.events[0], log.events[2]]
        with self.assertRaises(ValueError):
            replay_selection(recorded, corpus, reordered)

    def test_required_matrix_and_control_references_remain_unchanged(self):
        rows = first_slice_cells()
        self.assertEqual(len(rows), 24)
        self.assertEqual({r["cell_id"] for r in rows if r["campaign_kind"] == "required"}.__len__(), 24)
        refs = __import__("r2f.campaign.scheduler", fromlist=["control_references"]).control_references(rows)
        self.assertEqual({k: len(v) for k, v in refs.items()},
                         {"health": 6, "mutation": 6, "transparency": 6, "fault": 6})


if __name__ == "__main__":
    unittest.main()
