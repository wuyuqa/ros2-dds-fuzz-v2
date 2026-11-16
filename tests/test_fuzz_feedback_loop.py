"""F1 minimal exploratory feedback loop on the existing campaign execution path.

Scope of these tests: generation, plan binding, v2 input identity, selection and
feedback bookkeeping.  Two things are deliberately NOT claimed here:

* No ROS/DDS process is started.  Any runtime passed in is an offline fixture.
* The execution test injects an explicitly labelled offline fixture.  It proves
  plan->carrier->ingest->feedback plumbing only; it is not ROS/DDS evidence and
  can never become a natural candidate or seal.

Synthetic feedback used below to exercise the scheduler is labelled as such.  It
never becomes execution evidence, a candidate or a seal.
"""
import copy
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from r2f.campaign.corpus import Corpus, content_identity, content_sha256
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import (
    run_exploratory_cycle,
)
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core import Registry
from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.modules.s1.module import register_exploratory
from test_campaign_evidence import fixture_pair
from test_campaign_evidence import fixture_pair

TARGET = "discovery.late_endpoint.fuzz_v1"


def profile():
    return {
        "schema_version": "r2f.s1-fuzz-profile/v1",
        "profile_id": "s1-fuzz-feedback-loop-fixture",
        "campaign_kind": "exploratory",
        "enabled": False,
        "required_catalog_included": False,
        "target_ids": [TARGET],
        "surfaces": ["P01", "W02"],
        "rmws": ["rmw_cyclonedds_cpp"],
        "seed_specs": [],
        "execution_boundary": "offline planning only; no ROS",
        "budget": {"max_inputs": 4},
    }


def contract():
    registry = Registry()
    register_exploratory(registry, include_required=False)
    return registry.get_target(TARGET).exploration_contract


def parent_config(delay=0.15):
    return {
        "topic": "/r2f/s1/late_endpoint",
        "message_type": "std_msgs/msg/String",
        "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64},
        "reader_delay_sec": delay,
        "baseline_reader_delay_sec": 0.02,
        "pre_match_count": 2,
        "post_match_count": 6,
        "publish_interval_sec": 0.03,
        "match_timeout_sec": 8.0,
        "delivery_timeout_sec": 5.0,
        "startup_timeout_sec": 10.0,
    }


#: Two genuinely different surfaces so an observation of one class leaves a
#: non-empty "uncovered" pool of the other.  ``P01`` mutates reader_delay_sec;
#: ``W02`` mutates publish_interval_sec, so their contents never alias.
P01_BOUNDARIES = ("finite", "quantized_neighbor")
W02_BOUNDARIES = ("zero", "finite")


def seed(seed_id, surface_id, boundary, config):
    entry = {
        "seed_id": seed_id,
        "target_id": TARGET,
        "target_revision": 0,
        "rmw": "rmw_cyclonedds_cpp",
        "contract_hash": contract().contract_hash(),
        "surface_id": surface_id,
        "boundary_class": boundary,
        # Input-level relation class only; it is excluded from content identity
        # and lets the corpus record a genuinely new observed relation class.
        "relation_class": surface_id + "-relation",
        "executable": True,
        "final_config": config,
    }
    if surface_id == "P01":
        entry["mutator_id"] = "M01"
    return entry


def make_corpus():
    """Four DISTINCT parents across two surfaces (identical configs dedupe)."""
    corpus = Corpus()
    for index, (boundary, delay) in enumerate(zip(P01_BOUNDARIES, (0.15, 0.25)), 1):
        corpus.add_seed(seed(f"L{index:02d}", "P01", boundary, parent_config(delay)))
    for index, (boundary, interval) in enumerate(zip(W02_BOUNDARIES, (0.01, 0.05)), 3):
        config = parent_config()
        config["publish_interval_sec"] = interval
        corpus.add_seed(seed(f"L{index:02d}", "W02", boundary, config))
    assert len(corpus.entries) == 4, corpus.entries
    return corpus





def synthetic_observation(content, *, boundary, edge):
    """Labelled synthetic feedback for scheduler tests only. Never evidence."""
    return {
        "content_sha256": content,
        "run_id": "synthetic-feedback-fixture",
        "target_id": TARGET,
        "execution_stage": "judged",
        "actual_run": True,
        "evidence_origin": "synthetic_feedback_fixture",
        "surface_id": "P01",
        "boundary_class": boundary,
        "observed_edge": edge,
        "oracle_verdict": "pass",
        "typed_facts": {"environment_healthy": True, "process_healthy": True,
                        "preconditions_reached": True,
                        "endpoint_identity_complete": True,
                        "window": {"complete": True}},
    }


class ExploratoryFeedbackLoop(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def cycle(self, name, corpus, feedback, scheduler, *, execute=False,
              runtime=None, max_inputs=1):
        out = self.root / name
        result = run_exploratory_cycle(
            out, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=profile(), runtime=runtime, execute=execute,
            run_id="loop-" + name, max_inputs=max_inputs)
        return out, result

    # -- identity -----------------------------------------------------------

    def test_v2_identity_is_the_only_digest_and_binds_plan_and_windows(self):
        corpus = make_corpus()
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0)
        out, result = self.cycle("identity", corpus, feedback, scheduler)
        row = result["rows"][0]
        self.assertEqual(row["execution_stage"], "planned")
        self.assertFalse(row["actual_run"])
        self.assertEqual(result["input_identity_schema"], INPUT_SCHEMA_V2)
        self.assertEqual(row["input_identity_schema"], INPUT_SCHEMA_V2)
        self.assertNotIn("effective_input_v2_sha256", row)
        self.assertNotIn("carrier_effective_input_sha256", row)
        self.assertNotIn("identity_note", row)

        capsule = json.loads((out / "cases" / row["cell_id"] / "capsule.json").read_text())
        config = json.loads((out / "cases" / row["cell_id"] / "runtime_config.json").read_text())
        plan = row["mutation_plan"]

        # Capsule, config and row carry the same single v2 digest.
        self.assertEqual(capsule["input"]["input_identity_schema"], INPUT_SCHEMA_V2)
        self.assertEqual(capsule["input"]["effective_input_sha256"],
                         row["effective_input_sha256"])
        self.assertEqual(config["effective_input_sha256"], row["effective_input_sha256"])
        recomputed = effective_input_hash(config["effective_input"], schema=INPUT_SCHEMA_V2)
        self.assertEqual(recomputed, row["effective_input_sha256"])

        # final_input / plan / operations / windows agree everywhere.
        effective = capsule["input"]["effective_input"]
        self.assertEqual(effective["final_input"], plan["final_config"])
        self.assertEqual(effective["mutation_plan"], plan)
        self.assertEqual(effective["operations"], plan["operations"])
        self.assertEqual(effective["runtime_windows"], plan["runtime_windows"])
        self.assertEqual(config["final_input"], plan["final_config"])
        self.assertEqual(config["mutation_plan"], plan)
        for key, value in plan["final_config"].items():
            if key in ("qos",):
                continue
            self.assertEqual(config[key], value, key)

    # -- selection ----------------------------------------------------------

    def test_two_rounds_selection_follows_feedback_and_content_really_changes(self):
        corpus = make_corpus()
        feedback = FeedbackLog()
        first_scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
        _, first = self.cycle("round1", corpus, feedback, first_scheduler)
        first_row = first["rows"][0]
        self.assertEqual(first_row["execution_stage"], "planned")
        self.assertEqual(feedback.events, [])

        parent = first_row["parent_content_sha256"]
        self.assertEqual(first["selection_log"]["selections"][0]["content_sha256"], parent)
        # A finite boundary may legally preserve the authored parent value
        # (no-op).  The real requirement is that selection can produce a
        # genuine content change, checked across the two rounds.
        parent_config_value = next(rec["final_config"] for rec in corpus.entries.values()
                                   if rec["content_sha256"] == parent)

        # Changing only seed_id/boundary labels must not create new content.
        relabelled = dict(seed("L99", "P01", "another-label", parent_config()),
                          seed_id="L99")
        self.assertEqual(content_sha256(relabelled), content_identity(relabelled))

        # Synthetic feedback (explicitly labelled; never evidence) covers the
        # executed surface x boundary class and the rest of that surface, so the
        # uncovered pool contains only the OTHER surface.  This isolates the
        # assertion from final-content aliasing between parents.
        observed_surface = first_row["surface_id"]
        observed_boundaries = sorted({
            rec["boundary_class"] for rec in corpus.entries.values()
            if rec["surface_id"] == observed_surface})
        self.assertTrue(observed_boundaries)
        for boundary in observed_boundaries:
            feedback.record(synthetic_observation(
                first_row["content_sha256"], boundary=boundary, edge="edge-" + boundary))
        self.assertTrue(feedback.events)
        self.assertTrue(all(e["coverage"] for e in feedback.events))

        second_scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
        # Consume the cycle's uniform slot offline so the following selection is
        # driven by the uncovered pool.
        second_scheduler.next()
        _, second = self.cycle("round2", corpus, feedback, second_scheduler)
        selections = second["selection_log"]["selections"]
        self.assertEqual(len(selections), 2)  # offline uniform + the cycle selection
        sel = selections[-1]
        self.assertEqual(sel["sequence"], 1)
        self.assertEqual(sel["requested_pool"], "uncovered")
        self.assertEqual(sel["used_pool"], "uncovered")
        self.assertLess(sel["candidate_count"], len(corpus.selectable()))

        second_row = second["rows"][0]
        self.assertEqual(second_row["execution_stage"], "planned")
        self.assertNotEqual(second_row["surface_id"], first_row["surface_id"],
                            "uncovered pool must exclude the covered surface classes")
        self.assertTrue(
            second_row["content_sha256"] != first_row["content_sha256"]
            or second_row["final_input"] != first_row["final_input"])
        second_parent = next(rec["final_config"] for rec in corpus.entries.values()
                             if rec["content_sha256"] == second_row["parent_content_sha256"])
        self.assertNotEqual(second_row["final_input"], second_parent)

    # -- no execution hijack ------------------------------------------------

    def test_planned_only_never_calls_runtime_or_records_feedback(self):
        corpus = make_corpus()
        before_entries = sorted(corpus.entries)
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=3, warmup=0)

        def forbidden(*args, **kwargs):
            raise AssertionError("runtime must not be called when execute=False")

        _, result = self.cycle("planned", corpus, feedback, scheduler,
                               runtime=forbidden)
        row = result["rows"][0]
        self.assertEqual(row["execution_stage"], "planned")
        self.assertFalse(row["actual_run"])
        self.assertEqual(feedback.events, [])
        self.assertEqual(result["budget"]["executed"], 0)
        self.assertEqual(result["budget"]["attempted"], 0)
        self.assertEqual(sorted(corpus.entries), before_entries)
        self.assertEqual(result["stop_reason"], "execute_false_planned_only")
        self.assertEqual(result["last_state"], {
            "planned": 1, "executed": 0, "cell_id": row["cell_id"],
            "sequence": 0, "execution_stage": "planned"})

    def test_execute_true_reaches_injected_fixture_and_records_feedback(self):
        corpus = make_corpus()
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=4, warmup=0)
        calls = []

        def runtime(config, output_dir):
            calls.append(config)
            return fixture_pair(config, pathlib.Path(output_dir) / "fixture", fresh_matches=True)

        _, result = self.cycle("executed", corpus, feedback, scheduler,
                               execute=True, runtime=runtime)
        row = result["rows"][0]
        self.assertEqual(len(calls), 1)
        self.assertEqual(row["execution_stage"], "judged", row.get("abort_reason"))
        self.assertTrue(row["actual_run"])
        self.assertEqual(row["evidence_origin"], "offline_fixture")
        self.assertEqual(row["input_identity_schema"], INPUT_SCHEMA_V2)
        self.assertEqual(len(feedback.events), 1)
        self.assertTrue(feedback.events[0]["coverage"])
        self.assertGreaterEqual(result["budget"]["executed"], 1)
        offer = row["corpus_offer"]
        # Success alone is deliberately not a corpus admission reason.
        self.assertFalse(offer["admitted"], offer)
        self.assertEqual(offer["reason"], "success_only")
        self.assertNotIn(offer["content_sha256"], corpus.entries)
        self.assertEqual(result["corpus"]["entry_count"], len(corpus.entries))
        # An offline injected fixture is plumbing evidence, never a SUT candidate.
        self.assertEqual(result["budget"].get("candidates", 0), 0)
        self.assertNotEqual(result.get("seal"), True)
        self.assertEqual(row.get("candidate_status") in (None, "not_candidate"), True)

    def test_runtime_tool_error_aborts_without_coverage_or_corpus_admission(self):
        corpus = make_corpus()
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=5, warmup=0)
        before_entries = sorted(corpus.entries)
        calls = []

        def runtime(config, output_dir):
            calls.append(config)
            raise RuntimeError("synthetic fixture failure")

        _, result = self.cycle("runtime-error", corpus, feedback, scheduler,
                               execute=True, runtime=runtime)
        row = result["rows"][0]
        self.assertEqual(len(calls), 1)
        self.assertEqual(row["execution_stage"], "aborted")
        self.assertFalse(row["actual_run"])
        self.assertEqual(row["runtime_status"], "tool_error")
        self.assertIsNone(row["raw_path"])
        self.assertEqual(row["evidence_origin"], "injected_runtime")
        self.assertEqual(result["budget"]["executed"], 0)
        self.assertEqual(len(feedback.events), 1)
        self.assertFalse(feedback.events[0]["coverage"])
        self.assertTrue(feedback.events[0]["tool_error"])
        self.assertFalse(row["corpus_offer"]["admitted"])
        self.assertIn(row["corpus_offer"]["reason"], ("tool_error", "not_executed"))
        self.assertEqual(sorted(corpus.entries), before_entries)

    # -- tool error / source change -----------------------------------------

    def test_tool_error_feedback_gives_no_coverage_regardless_of_verdict(self):
        feedback = FeedbackLog()
        digest = content_identity(seed("L01", "P01", "p01_a", parent_config(0.15)))
        record = synthetic_observation(digest, boundary="min", edge="edge-X")
        feedback.record(dict(record, tool_error=True, runtime_status="tool_error"))
        event = feedback.events[0]
        self.assertTrue(event["tool_error"])
        self.assertFalse(event["coverage"])
        self.assertIsNone(event["edge_id"])
        self.assertEqual(feedback.covered_surfaces(), set())

        # A judgeable row whose window is incomplete also gives no coverage.
        other = FeedbackLog()
        incomplete = synthetic_observation(digest, boundary="min", edge="edge-Y")
        incomplete["typed_facts"]["window"]["complete"] = False
        other.record(incomplete)
        self.assertFalse(other.events[0]["coverage"])

    def test_source_change_aborts_and_last_planned_state_is_auditable(self):
        corpus = make_corpus()
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=2, warmup=0)
        import r2f.campaign.runner as runner_module
        from r2f.campaign.identity import source_manifest
        repo = pathlib.Path(__file__).resolve().parents[1]
        base = source_manifest(repo)
        changed = copy.deepcopy(base)
        changed["behavior_tree_sha256"] = "0" * 64
        changed["source_frozen"] = False
        calls = {"count": 0}

        def evolving(_repo):
            calls["count"] += 1
            return copy.deepcopy(base if calls["count"] == 1 else changed)

        with mock.patch.object(runner_module, "source_manifest", side_effect=evolving):
            out, result = self.cycle("source-change", corpus, feedback, scheduler)
        self.assertEqual(result["abort"]["reason_code"],
                         "source_repo_HEAD_or_behavior_tree_changed")
        self.assertEqual(result["abort"]["after_row_index"], 0)
        self.assertEqual(result["rows"][0]["execution_stage"], "planned")
        self.assertEqual(result["budget"]["executed"], 0)
        self.assertEqual(result["last_state"], {
            "planned": 1, "executed": 0,
            "cell_id": result["rows"][-1]["cell_id"], "sequence": 0,
            "execution_stage": "planned"})
        stored = json.loads((out / "cycle.json").read_text())
        self.assertEqual(stored["last_state"], result["last_state"])
        self.assertEqual(stored["abort"], result["abort"])
        self.assertEqual(stored["selection_log"]["selections"][0]["selection_sha256"],
                         result["selection_log"]["selections"][0]["selection_sha256"])


if __name__ == "__main__":
    unittest.main()
