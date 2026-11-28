"""Offline adapter-shaped raw -> state -> corpus -> feedback -> next selection.

No ingest/projection mock and no hand-written observed_edge. These are labelled
fixture executions, not ROS qualification or natural candidates.
"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core.evidence import raw_sha256
from r2f.core.state_signature import observation_edge
from test_campaign_evidence import fixture_pair
from test_fuzz_feedback_loop import make_corpus, profile


def changing_corpus():
    """Keep the four finite parents, explicitly use local range sampling.

    M01 finite is a valid no-change witness, not a generated execution case.
    These split/resume fixtures exercise actual raw-backed feedback instead.
    """
    corpus = Corpus()
    for parent in make_corpus().selectable():
        entry = copy.deepcopy(parent)
        entry["mutator_id"] = "M03"
        corpus.add_seed(entry)
    return corpus


def adapter_runtime(config, output_dir):
    """Extend the existing raw fixture with command-bound graph/QoS snapshots."""
    runtime = fixture_pair(config, Path(output_dir) / "fixture", fresh_matches=True)
    original = runtime["events"]
    events, index_map = [], {}
    for event in original:
        if event["event"] == "runtime_end":
            clock = event["monotonic_ns"]
            for role, cid in (("writer", 141), ("reader", 142)):
                for kind in ("graph_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"):
                    template_kind = ("match_snapshot" if kind == "graph_snapshot" else
                                     "match_snapshot_requested" if kind == "graph_snapshot_requested" else kind)
                    snapshot = copy.deepcopy(next(e for e in original
                                                  if e["event"] == template_kind and e.get("role") == role))
                    clock += 10_000
                    snapshot.update(event=kind, command_id=cid, monotonic_ns=clock)
                    if kind == "graph_snapshot_requested":
                        snapshot["requested_monotonic_ns"] = clock
                    if kind == "match_snapshot":
                        snapshot["sampled_monotonic_ns"] = clock
                    if kind == "graph_snapshot":
                        snapshot.update(publishers=[{"endpoint_gid": "writer-gid", "qos": config["qos"]}],
                                        subscriptions=[{"endpoint_gid": "reader-gid",
                                                        "qos": config.get("reader_qos", config["qos"])}])
                    events.append(snapshot)
            event["monotonic_ns"] = clock + 10_000
        index_map[event["event_index"]] = len(events)
        events.append(event)
        if event["event"] == "match_snapshot":
            graph = copy.deepcopy(event)
            graph.update(event="graph_snapshot", monotonic_ns=event["monotonic_ns"] + 1,
                         publishers=[{"endpoint_gid": "writer-gid", "qos": config["qos"]}],
                         subscriptions=[{"endpoint_gid": "reader-gid",
                                         "qos": config.get("reader_qos", config["qos"])}])
            events.append(graph)
    for index, event in enumerate(events):
        event["event_index"] = index
        for ref in event.get("precondition_receipt_refs", []):
            for key in ("raw_before_index", "raw_after_index"):
                ref[key] = index_map[ref[key]]
    runtime["events"] = events
    path = Path(runtime["raw_path"])
    path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
    runtime["raw_sha256"] = raw_sha256(path)
    return runtime


# Stable mocked source only avoids treating concurrent shared-tree edits as a
# fixture abort. It is explicitly not source-freeze or real execution evidence.
FIXTURE_SOURCE = {"source_repo": "/offline", "head": "a"*40,
                  "behavior_tree_sha256": "b"*64, "source_frozen": False,
                  "dirty_behavior_paths": ["offline-fixture"]}


class FeedbackEpochRunner(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = patch("r2f.campaign.runner.source_manifest", return_value=FIXTURE_SOURCE)
        self.source.start(); self.addCleanup(self.source.stop)

    def run_cycle(self, name, corpus, feedback, scheduler, **kwargs):
        return run_exploratory_cycle(self.root/name, corpus=corpus, feedback=feedback,
                                     scheduler=scheduler, profile=profile(), run_id=name, **kwargs)

    def test_raw_adapter_observation_refreshes_next_selection(self):
        corpus, feedback = make_corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=4, warmup=0)
        result = self.run_cycle("epoch", corpus, feedback, scheduler, execute=True,
                                runtime=adapter_runtime, max_inputs=2)
        rows = result["rows"]
        self.assertEqual(len(rows), 2)
        for row, event in zip(rows, feedback.events):
            self.assertEqual(row["execution_stage"], "judged", row.get("abort_reason"))
            receipt = row["typed_facts"]["state_projection_receipt"]
            self.assertEqual(receipt["status"], "observed", receipt["reasons"])
            self.assertEqual(event["edge_id"], observation_edge(row)[1])
            self.assertTrue(event["coverage"])
            self.assertTrue(row["corpus_offer"]["admitted"], row["corpus_offer"])
        self.assertEqual(scheduler.feedback.events, feedback.events)
        contexts = result["selection_log"]["selection_contexts"]
        self.assertEqual([c["feedback_event_count"] for c in contexts], [0, 1])
        self.assertNotEqual(contexts[0]["feedback_events_sha256"], contexts[1]["feedback_events_sha256"])
        second = result["selection_log"]["selections"][1]
        self.assertEqual(second["used_pool"], "uncovered")
        self.assertLess(second["candidate_count"], len(make_corpus().selectable()))

    def test_split_cycle_continues_global_sequence_and_same_inputs(self):
        full_c, full_f = changing_corpus(), FeedbackLog()
        full_s = ExploratoryScheduler(full_c, full_f, rng_seed=7, warmup=1)
        full = self.run_cycle("full", full_c, full_f, full_s, execute=True,
                              runtime=adapter_runtime, max_inputs=4)
        c, f = changing_corpus(), FeedbackLog()
        s = ExploratoryScheduler(c, f, rng_seed=7, warmup=1)
        first = self.run_cycle("first", c, f, s, execute=True, runtime=adapter_runtime, max_inputs=2)
        restored = ExploratoryScheduler.from_dict(s.to_dict(), c, f)
        second = self.run_cycle("second", c, f, restored, execute=True, runtime=adapter_runtime,
                                max_inputs=4, continuation=first["continuation"])
        rows = first["rows"] + second["rows"]
        self.assertEqual([r["selection_sequence"] for r in first["row_sequences"] + second["row_sequences"]], [0, 1, 2, 3])
        self.assertEqual([r["content_sha256"] for r in rows], [r["content_sha256"] for r in full["rows"]])
        # Classification participates in feedback RNG identity. A valid M03
        # sample can now hit no-op: compare its explicit refusal as well as
        # generated execution identities, without inventing a replay input.
        for resumed_row, whole_row in zip(rows, full["rows"]):
            if whole_row.get("generation_status") == "rejected":
                for row in (resumed_row, whole_row):
                    self.assertEqual(row["generation_status"], "rejected")
                    self.assertEqual(row["reason_code"], "no_effective_input")
                    self.assertEqual(row["execution_stage"], "planned")
                    self.assertFalse(row["actual_run"])
                    self.assertFalse(row["candidate_eligible"])
                    self.assertFalse(row["feedback_eligible"])
                    self.assertNotIn("effective_input_sha256", row)
                    self.assertIn("active no_op layer", row["no_effective_input_reason"])
                self.assertEqual(resumed_row["no_effective_input_reason"],
                                 whole_row["no_effective_input_reason"])
            else:
                self.assertEqual(resumed_row["execution_stage"], "judged")
                self.assertTrue(resumed_row["actual_run"])
                self.assertEqual(resumed_row["effective_input_sha256"],
                                 whole_row["effective_input_sha256"])
        self.assertTrue(any(row["actual_run"] for row in rows))
        self.assertEqual([e["selection_sha256"] for e in restored.selection_log],
                         [e["selection_sha256"] for e in full_s.selection_log])
        self.assertEqual(second["cycle_budget"]["selected"], 2)
        self.assertEqual(second["budget"]["selected"], 4)

    def test_pending_plan_checkpoint_resumes_without_new_selection(self):
        c, f = make_corpus(), FeedbackLog()
        s = ExploratoryScheduler(c, f, rng_seed=7, warmup=0)
        checkpoints = []
        def stop_after_plan(payload):
            checkpoints.append(copy.deepcopy(payload))
            if payload["phase"] == "planned":
                raise RuntimeError("fixture interruption before runtime")
        with self.assertRaisesRegex(RuntimeError, "fixture interruption"):
            self.run_cycle("interrupted", c, f, s, execute=False, runtime=adapter_runtime,
                           max_inputs=1, checkpoint_hook=stop_after_plan)
        state = checkpoints[-1]
        self.assertEqual(state["feedback"]["events"], [])
        continuation = {k: state[k] for k in ("budget", "seen_input_hashes", "seen_actual_hashes",
                                             "pending_selection", "last_case_ref", "submission_state", "execute_requested", "producer")}
        result = self.run_cycle("resumed", c, f, s, execute=True, runtime=adapter_runtime,
                                max_inputs=1, continuation=continuation)
        self.assertEqual(len(s.selection_log), 1)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["budget"]["planned"], 1)
        self.assertEqual(result["budget"]["actual"], 1)
        self.assertIsNone(result["continuation"]["pending_selection"])

    def test_submission_fence_precedes_carrier_and_blocks_automatic_resume(self):
        c, f = make_corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(c, f, rng_seed=7, warmup=0)
        checkpoints, calls = [], []
        def hook(payload):
            checkpoints.append(copy.deepcopy(payload))
            if payload.get("submission_state") == "submitted":
                marker = Path(payload["last_case_ref"]["submission_marker_path"])
                self.assertTrue(marker.is_file())
                fence = json.loads(marker.read_text())
                self.assertEqual(fence["sequence"], 0)
                self.assertFalse(fence["actual_run_claimed"])
                self.assertTrue(fence["owner"]["start_ticks"])
                raise RuntimeError("fixture death after durable submission fence")
        def forbidden(config, output_dir):
            calls.append(config)
            raise AssertionError("must not submit carrier")
        with self.assertRaisesRegex(RuntimeError, "durable submission"):
            self.run_cycle("fenced", c, f, scheduler, execute=True, runtime=forbidden,
                           max_inputs=1, checkpoint_hook=hook)
        state = checkpoints[-1]
        case = Path(state["last_case_ref"]["case_dir"])
        before = {p.name: p.read_bytes() for p in case.iterdir() if p.is_file()}
        continuation = {k: state[k] for k in ("budget", "seen_input_hashes", "seen_actual_hashes",
                                             "pending_selection", "last_case_ref", "submission_state",
                                             "execute_requested", "producer")}
        result = self.run_cycle("refused", c, f, scheduler, execute=True, runtime=forbidden,
                                max_inputs=1, continuation=continuation)
        self.assertEqual(calls, [])
        self.assertEqual(result["rows"], [])
        self.assertEqual(result["abort"]["reason_code"], "resume_pending_execution_uncertain")
        self.assertEqual(result["budget"]["actual"], 0)
        self.assertEqual(before, {p.name: p.read_bytes() for p in case.iterdir() if p.is_file()})


if __name__ == "__main__":
    unittest.main()
