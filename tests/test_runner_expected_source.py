"""Caller source pins at public runner intake; all source/runtime data is simulated.

Unique /dev/shm outputs contain offline fixture documents only. No Git mutation,
DDS/native process, live campaign, build or replay is performed.
"""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.runner import plan_campaign, run_campaign, run_exploratory_cycle
from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler
from test_fuzz_feedback_loop import make_effective_corpus, parent_config, profile, seed
from test_campaign_evidence import fixture_pair
from test_feedback_epoch_runner import adapter_runtime


REPO = Path(__file__).resolve().parents[1]


class RunnerExpectedSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="r2f-expected-source-", dir="/dev/shm")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = {
            "source_repo": str(REPO), "head": "a" * 40,
            "behavior_tree_sha256": "b" * 64,
            "source_frozen": True, "dirty_behavior_paths": [],
        }
        observer = patch("r2f.campaign.runner.source_manifest",
                         side_effect=lambda repo: deepcopy(self.source))
        self.observer = observer.start()
        self.addCleanup(observer.stop)

    def test_required_plan_rejects_another_head_before_writing_plans(self):
        expected = dict(self.source, head="c" * 40)
        output = self.root / "required"
        with self.assertRaisesRegex(ValueError, "^expected_source_manifest_mismatch$"):
            plan_campaign(output, repo=REPO, run_id="pin", expected_source_manifest=expected)
        self.assertFalse((output / "manifest.json").exists())
        self.assertFalse(list(output.rglob("*.json")))

    def test_exploratory_rejects_another_head_without_state_or_runtime_effects(self):
        corpus, feedback = make_effective_corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0)
        before = deepcopy((corpus.to_dict(), feedback.to_dict(), scheduler.to_dict()))
        calls = []
        output = self.root / "exploratory"
        with self.assertRaisesRegex(ValueError, "^expected_source_manifest_mismatch$"):
            run_exploratory_cycle(
                output, corpus=corpus, feedback=feedback, scheduler=scheduler,
                profile=profile(), repo=REPO, run_id="pin", max_inputs=1,
                execute=True, runtime=lambda *args, **kwargs: calls.append(args),
                expected_source_manifest=dict(self.source, head="c" * 40))
        self.assertEqual(calls, [])
        self.assertEqual((corpus.to_dict(), feedback.to_dict(), scheduler.to_dict()), before)
        self.assertFalse(list(output.rglob("*.json")))

    def exploratory(self, output, **options):
        corpus, feedback = make_effective_corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0)
        result = run_exploratory_cycle(
            output, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=profile(), repo=REPO, run_id="pin", max_inputs=1, **options)
        return result, corpus, feedback

    def test_entire_expected_dictionary_must_match_for_both_planners(self):
        variants = {
            "head": dict(self.source, head="c" * 40),
            "behavior": dict(self.source, behavior_tree_sha256="d" * 64),
            "repo": dict(self.source, source_repo="/another/repo"),
            "frozen": dict(self.source, source_frozen=False),
            "dirty": dict(self.source, dirty_behavior_paths=["src/changed.py"]),
            "extra": dict(self.source, unexpected="not observed"),
            "missing": {key: value for key, value in self.source.items() if key != "head"},
            "empty": {},
        }
        for label, expected in variants.items():
            for entry in ("required", "exploratory"):
                with self.subTest(field=label, entry=entry):
                    output = self.root / (entry + "-" + label)
                    with self.assertRaisesRegex(ValueError, "^expected_source_manifest_mismatch$"):
                        if entry == "required":
                            plan_campaign(output, repo=REPO, run_id="pin",
                                          expected_source_manifest=expected)
                        else:
                            self.exploratory(output, expected_source_manifest=expected)
                    self.assertFalse(list(output.rglob("*.json")))

    def test_exact_pin_records_observed_source_with_one_intake_snapshot(self):
        self.source["future_source_detail"] = {"status": "observed", "paths": ["src/a.py"]}
        expected = deepcopy(dict(reversed(list(self.source.items()))))
        result = plan_campaign(self.root / "exact", repo=REPO, run_id="pin",
                               expected_source_manifest=expected)
        self.assertEqual(self.observer.call_count, 1)
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(manifest["source_manifest"], self.source)
        self.assertEqual(expected, self.source)
        self.assertNotIn("expected_source_manifest", manifest)
        self.assertEqual(len(result["rows"]), 24)
        self.assertTrue(all(row["execution_stage"] == "planned" for row in result["rows"]))

    def test_exact_pin_is_accepted_by_exploratory_intake(self):
        expected = deepcopy(self.source)
        result, _, feedback = self.exploratory(self.root / "exact", expected_source_manifest=expected)
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(manifest["source_manifest"], self.source)
        self.assertEqual(result["source_manifest"], self.source)
        self.assertNotIn("expected_source_manifest", manifest)
        self.assertEqual(result["budget"]["planned"], 1)
        self.assertEqual(result["budget"]["attempted"], 0)
        self.assertEqual(feedback.events, [])

    def test_new_committed_source_before_intake_is_rejected_by_both_runners(self):
        approved = deepcopy(self.source)
        # A wrapper approved A. Intake sees clean committed B, not a dirty tree.
        self.source = dict(self.source, head="c" * 40, behavior_tree_sha256="d" * 64)
        for entry in ("required", "exploratory"):
            with self.subTest(entry=entry):
                calls = []
                output = self.root / entry
                with self.assertRaisesRegex(ValueError, "^expected_source_manifest_mismatch$"):
                    options = dict(execute=True, runtime=lambda *args, **kwargs: calls.append(args),
                                   expected_source_manifest=approved)
                    if entry == "required":
                        run_campaign(output, repo=REPO, run_id="pin", **options)
                    else:
                        self.exploratory(output, **options)
                self.assertEqual(calls, [])
                self.assertFalse(list(output.rglob("*.json")))
        self.assertTrue(self.source["source_frozen"])
        self.assertEqual(approved["head"], "a" * 40)

    def test_required_omitted_none_and_exact_pins_preserve_plan_artifacts(self):
        outputs = [self.root / name for name in ("omitted", "none", "exact")]
        options = [{}, {"expected_source_manifest": None},
                   {"expected_source_manifest": deepcopy(self.source)}]
        plans = [plan_campaign(out, repo=REPO, run_id="pin", **kw)
                 for out, kw in zip(outputs, options)]
        manifests = [json.loads(Path(plan["manifest_path"]).read_text()) for plan in plans]
        self.assertEqual(manifests[0], manifests[1])
        self.assertEqual(manifests[0], manifests[2])
        for reference, plan in zip(plans, plans[1:]):
            self.assertEqual(reference["identity"], plan["identity"])
            for left, right in zip(reference["rows"], plan["rows"]):
                self.assertEqual(left["effective_input_sha256"], right["effective_input_sha256"])
                for key in ("runtime_config_path", "replay_path"):
                    self.assertEqual(json.loads(Path(left[key]).read_text()),
                                     json.loads(Path(right[key]).read_text()))

    def test_exploratory_omitted_none_and_exact_pins_preserve_input_identities(self):
        cycles = [self.exploratory(self.root / label, **options)[0]
                  for label, options in (("omitted", {}), ("none", {"expected_source_manifest": None}),
                                        ("exact", {"expected_source_manifest": deepcopy(self.source)}))]
        for result in cycles[1:]:
            self.assertEqual(cycles[0]["selection_log"], result["selection_log"])
            self.assertEqual(cycles[0]["source_manifest"], result["source_manifest"])
            reference, row = cycles[0]["rows"][0], result["rows"][0]
            for key in ("effective_input_sha256", "content_sha256", "mutation_plan_sha256"):
                self.assertEqual(reference[key], row[key])
            self.assertEqual(json.loads(Path(cycles[0]["manifest_path"]).read_text()),
                             json.loads(Path(result["manifest_path"]).read_text()))

    def test_required_drift_after_intake_still_aborts_before_runtime(self):
        approved = deepcopy(self.source)
        changed = dict(approved, behavior_tree_sha256="d" * 64)
        self.observer.side_effect = [approved, changed]
        calls = []
        result = run_campaign(self.root / "drift", repo=REPO, run_id="pin", execute=True,
                              runtime=lambda *args, **kwargs: calls.append(args),
                              expected_source_manifest=approved)
        self.assertEqual(calls, [])
        self.assertEqual(result["abort_reason"], "source_repo_HEAD_or_behavior_tree_changed")
        self.assertTrue(all(row["actual_run"] is False for row in result["rows"]))

    def test_required_drift_during_runtime_still_aborts_and_retains_raw(self):
        approved = deepcopy(self.source)
        calls = []
        def runtime(config, output_dir):
            calls.append(config)
            result = fixture_pair(config, output_dir, fresh_matches=True)
            self.source = dict(self.source, behavior_tree_sha256="d" * 64)
            return result
        result = run_campaign(self.root / "drift", repo=REPO, run_id="pin", execute=True,
                              runtime=runtime, expected_source_manifest=approved)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["abort_reason"], "source_repo_HEAD_or_behavior_tree_changed")
        self.assertTrue(Path(result["rows"][0]["raw_path"]).is_file())
        self.assertEqual(sum(row["actual_run"] for row in result["rows"]), 1)

    def test_exploratory_drift_after_intake_still_aborts_before_runtime(self):
        calls = []
        def checkpoint(payload):
            if payload["phase"] == "planned":
                self.source = dict(self.source, behavior_tree_sha256="d" * 64)
        result, _, feedback = self.exploratory(
            self.root / "drift", expected_source_manifest=deepcopy(self.source),
            execute=True, runtime=lambda *args, **kwargs: calls.append(args),
            checkpoint_hook=checkpoint)
        self.assertEqual(calls, [])
        self.assertEqual(result["abort"]["reason_code"], "source_repo_HEAD_or_behavior_tree_changed")
        self.assertEqual(result["budget"]["attempted"], 0)
        self.assertEqual(feedback.events, [])

    def test_exploratory_drift_during_runtime_still_blocks_healthy_admission(self):
        calls = []
        def runtime(config, output_dir):
            calls.append(config)
            result = adapter_runtime(config, output_dir)
            self.source = dict(self.source, behavior_tree_sha256="d" * 64)
            return result
        result, _, feedback = self.exploratory(
            self.root / "drift", expected_source_manifest=deepcopy(self.source),
            execute=True, runtime=runtime)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["abort"]["reason_code"], "source_repo_HEAD_or_behavior_tree_changed")
        row = result["rows"][0]
        self.assertFalse(row["candidate_eligible"])
        self.assertFalse(row["feedback_eligible"])
        self.assertFalse(row["corpus_offer"].get("admitted"))
        self.assertTrue(Path(row["raw_path"]).is_file())
        self.assertEqual(feedback.events[-1]["recipe_attempt"]["outcome"], "global_invalid")
        self.assertFalse(feedback.events[-1]["coverage"])

    def test_matching_dirty_pin_cannot_bypass_existing_formal_frozen_gates(self):
        from r2f.runtime.ros2_pair import run_pair
        self.source.update(source_frozen=False, dirty_behavior_paths=["src/offline_fixture.py"])
        required = run_campaign(self.root / "required", repo=REPO, run_id="pin", execute=True,
                                runtime=run_pair, expected_source_manifest=deepcopy(self.source))
        exploratory, _, feedback = self.exploratory(
            self.root / "exploratory", execute=True, runtime=run_pair,
            expected_source_manifest=deepcopy(self.source))
        self.assertEqual(required["abort_reason"], "source_behavior_not_tracked_and_clean_at_HEAD")
        self.assertTrue(all(row["actual_run"] is False for row in required["rows"]))
        self.assertEqual(exploratory["abort"]["reason_code"],
                         "source_behavior_not_tracked_and_clean_at_HEAD")
        self.assertEqual(exploratory["budget"]["attempted"], 0)
        self.assertEqual(exploratory["budget"]["generated"], 0)
        self.assertEqual(feedback.events, [])

    def test_intake_mismatch_preserves_prior_tool_failure_streak_and_evidence(self):
        corpus, feedback = Corpus(), FeedbackLog()
        entry = seed("failure-prefix", "W02", "finite",
                     dict(parent_config(), publish_interval_sec=0.0105))
        entry["mutator_id"] = "M03"
        corpus.add_seed(entry)
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
        def tool_failure(config, output_dir):
            raise RuntimeError("explicit offline tool failure")
        prior_output = self.root / "prior"
        prior = run_exploratory_cycle(
            prior_output, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=profile(), repo=REPO, run_id="prior", max_inputs=1,
            execute=True, runtime=tool_failure, expected_source_manifest=deepcopy(self.source))
        recipe = feedback.events[0]["recipe_attempt"]["recipe"]
        self.assertEqual(scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"], 1)
        before = deepcopy((corpus.to_dict(), feedback.to_dict(), scheduler.to_dict()))
        original_files = {p: p.read_bytes() for p in prior_output.rglob("*") if p.is_file()}
        calls = []
        output = self.root / "mismatch"
        with self.assertRaisesRegex(ValueError, "^expected_source_manifest_mismatch$"):
            run_exploratory_cycle(
                output, corpus=corpus, feedback=feedback, scheduler=scheduler,
                profile=profile(), repo=REPO, run_id="mismatch", max_inputs=2,
                execute=True, runtime=lambda *args, **kwargs: calls.append(args),
                continuation=prior["continuation"],
                expected_source_manifest=dict(self.source, head="c" * 40))
        self.assertEqual(calls, [])
        self.assertEqual((corpus.to_dict(), feedback.to_dict(), scheduler.to_dict()), before)
        self.assertEqual(scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"], 1)
        self.assertEqual({p: p.read_bytes() for p in original_files}, original_files)
        self.assertFalse(list(output.rglob("*.json")))
