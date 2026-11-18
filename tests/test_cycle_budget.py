"""Monotonic cycle budgets and accounting, no real runtime/campaign."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import ExploratoryCycleError, run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from test_fuzz_feedback_loop import make_corpus, profile
from test_feedback_epoch_runner import FIXTURE_SOURCE, adapter_runtime


class CycleBudget(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.clock = 100.0
        for p in (patch("r2f.campaign.runner.source_manifest", return_value=FIXTURE_SOURCE),
                  patch("r2f.campaign.runner.time.monotonic", side_effect=lambda: self.clock)):
            p.start(); self.addCleanup(p.stop)
        self.index = 0

    def run_cycle(self, p=None, *, scheduler=None, feedback=None, corpus=None, **kwargs):
        c = corpus if corpus is not None else make_corpus()
        f = feedback if feedback is not None else FeedbackLog()
        s = scheduler if scheduler is not None else ExploratoryScheduler(c, f, rng_seed=7, warmup=0)
        self.index += 1
        return run_exploratory_cycle(self.root/str(self.index), corpus=c, feedback=f, scheduler=s,
                                     profile=p or profile(), run_id="budget"+str(self.index), **kwargs)

    def test_profile_limits_and_explicit_override(self):
        p = profile(); p["budget"] = {"max_inputs": 2, "warmup_inputs": 1}
        result = self.run_cycle(p)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["budget"]["selected"], 2)
        self.assertEqual([s["phase"] for s in result["selection_log"]["selections"]],
                         ["warmup", "exploratory"])
        result = self.run_cycle(p, max_inputs=1)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["budget"]["actual"], 0)

    def test_deadline_during_generation_aborts_before_runtime(self):
        p = profile(); p["budget"] = {"max_inputs": 2, "max_seconds": 1}
        calls = []
        def hook(payload):
            if payload["phase"] == "planned":
                self.clock += 2
        def runtime(config, output_dir):
            calls.append(config)
            return adapter_runtime(config, output_dir)
        result = self.run_cycle(p, execute=True, runtime=runtime, checkpoint_hook=hook)
        self.assertEqual(calls, [])
        self.assertEqual(result["stop_reason"], "cycle_deadline_reached")
        row = result["rows"][0]
        self.assertEqual(row["execution_stage"], "aborted")
        self.assertEqual(row["abort_reason"], "cycle_deadline_reached")
        self.assertFalse(row["actual_run"])
        self.assertEqual(result["budget"]["attempted"], 0)
        self.assertEqual(result["budget"]["actual"], 0)
        self.assertEqual(result["budget"]["unique_actual"], 0)

    def test_checkpoint_time_is_part_of_deadline_before_carrier(self):
        p = profile(); p["budget"] = {"max_inputs": 2, "max_seconds": 1}
        calls = []
        def hook(payload):
            if payload.get("submission_state") == "submitted":
                self.clock += 2
        def forbidden(config, output_dir):
            calls.append(config)
            raise AssertionError("no runtime after checkpoint consumed deadline")
        result = self.run_cycle(p, execute=True, runtime=forbidden, checkpoint_hook=hook)
        self.assertEqual(calls, [])
        self.assertEqual(result["stop_reason"], "cycle_deadline_reached")
        self.assertEqual(result["budget"]["attempted"], 0)
        self.assertEqual(result["budget"]["actual"], 0)

    def test_completed_raw_retained_no_next_runtime_after_deadline(self):
        p = profile(); p["budget"] = {"max_inputs": 3, "max_seconds": 1}
        calls = []
        def runtime(config, output_dir):
            calls.append(config)
            observed = adapter_runtime(config, output_dir)
            self.clock += 2
            return observed
        result = self.run_cycle(p, execute=True, runtime=runtime)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["stop_reason"], "cycle_deadline_reached")
        self.assertEqual(result["budget"]["actual"], 1)
        self.assertTrue(Path(result["rows"][0]["raw_path"]).is_file())
        self.assertEqual(result["budget"]["elapsed_sec"], 2)

    def test_cumulative_deadline_no_selection_on_resume(self):
        p = profile(); p["budget"] = {"max_inputs": 3, "max_seconds": 1}
        c, f = make_corpus(), FeedbackLog()
        s = ExploratoryScheduler(c, f, rng_seed=7, warmup=0)
        state = {"budget": {"selected": 1, "elapsed_sec": 1.1}}
        result = self.run_cycle(p, corpus=c, feedback=f, scheduler=s, continuation=state)
        self.assertEqual(result["rows"], [])
        self.assertEqual(s.selection_log, [])
        self.assertEqual(result["stop_reason"], "cycle_deadline_reached")

    def test_rejections_count_selection_not_actual_and_rows_not_duplicated(self):
        p = profile(); p["surfaces"] = ["not-in-corpus"]
        result = self.run_cycle(p, max_inputs=2)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["budget"]["selected"], 2)
        self.assertEqual(result["budget"]["planned"], 2)
        self.assertEqual(result["budget"]["generated"], 0)
        self.assertEqual(result["budget"]["unique_inputs"], 0)
        self.assertEqual(result["budget"]["actual"], 0)
        self.assertEqual(result["budget"]["unique_actual"], 0)

    def test_repeated_input_is_not_a_distinct_input(self):
        corpus = make_corpus()
        first = next(iter(corpus.entries))
        corpus.entries = {first: corpus.entries[first]}
        with patch("r2f.campaign.runner._derive_rng_seed", return_value=123):
            result = self.run_cycle(corpus=corpus, max_inputs=2)
        self.assertEqual(result["budget"]["selected"], 2)
        self.assertEqual(result["budget"]["generated"], 2)
        self.assertEqual(result["budget"]["unique_inputs"], 1)
        self.assertEqual(result["budget"]["unique_actual"], 0)

    def test_invalid_budgets_fail_closed(self):
        for budget in ({"max_inputs": 0}, {"max_inputs": True}, {"max_seconds": 0},
                       {"max_seconds": float("inf")}, {"max_seconds": None, "warmup_inputs": -1},
                       {"warmup_inputs": True}):
            with self.subTest(budget=budget):
                p = profile(); p["budget"] = budget
                with self.assertRaises((ExploratoryCycleError, ValueError)):
                    self.run_cycle(p)

    def test_chain_whitelist_rejects_undeclared_later_layers(self):
        p = profile(); p.update(surfaces=["P01"], chain_length=2)
        c = make_corpus()
        # Remove other entry surfaces; contract itself still has compatible W02.
        c.entries = {h: r for h, r in c.entries.items() if r["surface_id"] == "P01"}
        result = self.run_cycle(p, corpus=c, max_inputs=1)
        self.assertEqual(result["rows"][0]["generation_status"], "rejected")
        self.assertIn("chain_length_not_composable", result["rows"][0]["reason_code"])
        self.assertEqual(result["budget"]["actual"], 0)


if __name__ == "__main__":
    unittest.main()
