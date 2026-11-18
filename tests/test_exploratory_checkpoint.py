"""Offline continuation fixtures; no ROS, campaign qualification, or live seal."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.campaign.corpus import Corpus, content_identity, corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core.evidence import canonical_hash

REPO = Path(__file__).resolve().parents[1]
SOURCE = {"source_repo": "/offline-fixture-only", "head": "fixture-head",
          "behavior_tree_sha256": "f" * 64, "source_frozen": True,
          "dirty_behavior_paths": []}
CYCLE = "late-endpoint-parameter"


def profile():
    doc = json.loads((REPO / "config/s1_fuzz/profiles/parameter.json").read_text())
    doc["budget"] = {"max_inputs": 6, "warmup_inputs": 2}
    return doc


def fresh(doc):
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=17,
                                     warmup=doc["budget"]["warmup_inputs"])
    return {"corpus": corpus, "feedback": feedback, "scheduler": scheduler,
            "continuation": None}


def run_cycle(out, doc, state=None, *, maximum=None, runtime=None, hook_wrapper=None):
    state = state or fresh(doc)
    hook = make_checkpoint_hook(out, profile=doc, cycle_id=CYCLE,
                               source_manifest=SOURCE, continuation=state["continuation"])
    if hook_wrapper:
        hook = hook_wrapper(hook)
    with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
        result = run_exploratory_cycle(
            out, **state, profile=doc, max_inputs=maximum,
            runtime=runtime, execute=runtime is not None,
            checkpoint_hook=hook, run_id=out.name)
    return result


def restore(path, output, doc=None, source=None):
    return load_checkpoint(path, profile=doc or profile(), cycle_id=CYCLE,
                           source_manifest=source or SOURCE, output_dir=output)


def rehash_checkpoint(data):
    data["checkpoint_sha256"] = canonical_hash({k: v for k, v in data.items() if k != "checkpoint_sha256"})


def projection(result):
    return [(r["parent_content_sha256"], r["content_sha256"], r["final_input"], r["mutation_plan"])
            for r in result["rows"]]


def test_corpus_restore_keeps_records_pins_lineage_and_history(tmp_path):
    corpus = fresh(profile())["corpus"]
    parent = corpus.selectable()[0]
    child = deepcopy(parent)
    for key in ("content_sha256", "pinned_seed", "candidate_ids", "bucket_key"):
        child.pop(key, None)
    child["final_config"]["reader_delay_sec"] = 0.1234
    child["parent_content_sha256"] = parent["content_sha256"]
    admitted = corpus.offer(child, executed=True, evidence_complete=True,
                            observed_edge="fixture-edge", candidate_id="fixture-candidate")
    assert admitted["admitted"]
    original = corpus.to_dict()
    path = tmp_path / "corpus.json"
    path.write_text(json.dumps(original))
    loaded = Corpus.load(path)
    assert loaded.to_dict() == original
    assert loaded.entries[admitted["content_sha256"]]["pinned_seed"] is False
    assert loaded.candidates["fixture-candidate"] == {admitted["content_sha256"]}
    loaded.entries[admitted["content_sha256"]]["final_config"]["reader_delay_sec"] = 99
    assert corpus.to_dict() == original


@pytest.mark.parametrize("damage", ["content", "hash", "count", "index", "candidate", "history", "capacity", "parent"])
def test_corpus_loader_rejects_damage_without_repair(damage):
    corpus = fresh(profile())["corpus"]
    data = corpus.to_dict()
    record = data["entries"][0]
    if damage == "content":
        record["final_config"]["reader_delay_sec"] = 9
    elif damage == "hash":
        record["content_sha256"] = "a" * 64
    elif damage == "count":
        data["entry_count"] += 1
    elif damage == "index":
        data["indexes"]["buckets"][0]["members"] = []
    elif damage == "candidate":
        record["candidate_ids"] = ["lost-reference"]
    elif damage == "history":
        data["history"][0]["sequence"] = 1
    elif damage == "capacity":
        data["capacity"] = 0
    else:
        record["parent_content_sha256"] = "b" * 64
    with pytest.raises(ValueError):
        Corpus.from_dict(data)


def test_split_resume_matches_continuous_input_plan_and_warmup(tmp_path):
    doc = profile()
    whole = run_cycle(tmp_path / "whole", doc)
    first = run_cycle(tmp_path / "first", doc, maximum=3)
    old = {str(p.relative_to(tmp_path / "first")): p.read_bytes()
           for p in (tmp_path / "first").rglob("*") if p.is_file()}
    state = restore(tmp_path / "first", tmp_path / "second", doc)
    second = run_cycle(tmp_path / "second", doc, state)
    assert projection(whole) == projection(first) + projection(second)
    assert second["selection_log"] == whole["selection_log"]
    assert second["budget"]["selected"] == 6
    assert second["budget"]["executed"] == 0
    assert second["feedback"]["events"] == []
    assert [s["phase"] for s in second["selection_log"]["selections"]] == ["warmup"] * 2 + ["exploratory"] * 4
    assert old == {str(p.relative_to(tmp_path / "first")): p.read_bytes()
                   for p in (tmp_path / "first").rglob("*") if p.is_file()}


@pytest.mark.parametrize("field", ["source", "profile", "target", "catalog", "entry", "event", "sequence", "payload"])
def test_checkpoint_preflight_rejects_identity_or_hash_tamper(tmp_path, field):
    doc = profile()
    run_cycle(tmp_path / "old", doc, maximum=1)
    path = tmp_path / "old/checkpoint.json"
    original = path.read_bytes()
    data = json.loads(original)
    kwargs = {}
    if field == "source":
        kwargs["source"] = dict(SOURCE, behavior_tree_sha256="a" * 64)
    elif field == "profile":
        kwargs["doc"] = dict(doc, profile_id="other")
    elif field == "target":
        data["identity"]["target_manifest"][0]["target_revision"] += 1
    elif field == "catalog":
        data["identity"]["required_catalog_sha256"] = "0" * 64
    elif field == "entry":
        record = data["state"]["corpus"]["entries"][0]
        record["final_config"]["reader_delay_sec"] = 7
        record["content_sha256"] = content_identity(record)
    elif field == "event":
        data["state"]["feedback"]["events"].append({"event_sha256": "x" * 64})
    elif field == "sequence":
        data["state"]["next_selection_sequence"] += 1
    else:
        data["state"]["continuation"]["budget"]["executed"] += 1
    if field not in {"source", "profile"}:
        if field != "payload":
            rehash_checkpoint(data)
        path.write_text(json.dumps(data))
    evidence = path.read_bytes()
    with pytest.raises((ValueError, KeyError)):
        restore(path, tmp_path / "new", **kwargs)
    assert path.read_bytes() == evidence
    assert not (tmp_path / "new").exists()


def test_resume_rejects_reusing_old_output_and_plain_corpus(tmp_path):
    run_cycle(tmp_path / "old", profile(), maximum=1)
    with pytest.raises(ValueError, match="independent"):
        restore(tmp_path / "old", tmp_path / "old")
    plain = tmp_path / "old/corpus-only.json"
    plain.write_text(json.dumps(fresh(profile())["corpus"].to_dict()))
    with pytest.raises(ValueError, match="schema"):
        restore(plain, tmp_path / "new")


def test_persisted_pending_selection_is_not_execution_and_replays_once(tmp_path):
    doc = profile()

    class Interrupted(Exception):
        pass

    def interrupt(hook):
        def wrapped(payload):
            hook(payload)
            if payload["phase"] == "planned":
                raise Interrupted()
        return wrapped

    with pytest.raises(Interrupted):
        run_cycle(tmp_path / "interrupted", doc, maximum=3, hook_wrapper=interrupt)
    checkpoint = json.loads((tmp_path / "interrupted/checkpoint.json").read_text())
    pending = checkpoint["state"]["continuation"]
    assert pending["phase"] == "planned"
    assert pending["budget"]["executed"] == pending["budget"]["attempted"] == 0
    assert pending["pending_selection"]["sequence"] == 0
    state = restore(tmp_path / "interrupted", tmp_path / "resumed", doc)
    resumed = run_cycle(tmp_path / "resumed", doc, state, maximum=3)
    whole = run_cycle(tmp_path / "whole", doc, maximum=3)
    assert projection(resumed) == projection(whole)
    assert resumed["budget"]["selected"] == resumed["budget"]["planned"] == 3
    assert resumed["selection_log"] == whole["selection_log"]


@pytest.mark.parametrize("evidence", ["submission", "runtime", "raw", "changed_row", "active_owner", "missing_artifact"])
def test_pending_resume_uncertainty_is_preserved_and_never_auto_executed(tmp_path, evidence):
    doc = profile()

    class StopBeforeCarrier(BaseException):
        pass

    def stop(hook):
        def wrapped(payload):
            hook(payload)
            if payload["phase"] == "planned":
                raise StopBeforeCarrier()
        return wrapped

    with pytest.raises(StopBeforeCarrier):
        run_cycle(tmp_path / "old", doc, maximum=1, hook_wrapper=stop)
    path = tmp_path / "old/checkpoint.json"
    data = json.loads(path.read_text())
    attempt = data["state"]["continuation"]["pending_attempt"]
    case = Path(attempt["case_dir"])
    if evidence == "submission":
        Path(attempt["submission_marker_path"]).write_text('{"status":"submitted_or_uncertain"}')
    elif evidence == "runtime":
        Path(attempt["runtime_dir"]).mkdir()
    elif evidence == "raw":
        (case / "raw.jsonl").write_text('{"event":"synthetic_uncertain_submission"}\n')
    elif evidence == "changed_row":
        row_path = case / "row.json"
        row = json.loads(row_path.read_text())
        row.update(execution_stage="judged", actual_run=True)
        row_path.write_text(json.dumps(row))
    elif evidence == "missing_artifact":
        (case / "capsule.json").unlink()
    else:
        attempt["execute_requested"] = True
        rehash_checkpoint(data)
        path.write_text(json.dumps(data))
    original = {str(p.relative_to(tmp_path / "old")): p.read_bytes()
                for p in (tmp_path / "old").rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="resume_pending_execution_uncertain"):
        restore(path, tmp_path / "new", doc)
    audit = json.loads((tmp_path / "new/resume-refusal.json").read_text())
    assert audit["automatic_runtime_called"] is False
    assert audit["disposition"] == "interrupted_attempt_requires_explicit_review"
    assert original == {str(p.relative_to(tmp_path / "old")): p.read_bytes()
                        for p in (tmp_path / "old").rglob("*") if p.is_file()}


def test_submitted_checkpoint_cannot_replay_as_never_executed(tmp_path):
    doc = profile()
    calls = []

    class CrashBeforeCarrier(BaseException):
        pass

    def stop_at_fence(hook):
        def wrapped(payload):
            hook(payload)
            if payload.get("submission_state") == "submitted":
                raise CrashBeforeCarrier()
        return wrapped

    def runtime(*args):
        calls.append(args)
        raise AssertionError("fence must persist before runtime")

    with pytest.raises(CrashBeforeCarrier):
        run_cycle(tmp_path / "old", doc, maximum=1, runtime=runtime, hook_wrapper=stop_at_fence)
    assert calls == []
    saved = json.loads((tmp_path / "old/checkpoint.json").read_text())
    pending = saved["state"]["continuation"]
    assert pending["submission_state"] == "submitted"
    assert Path(pending["pending_attempt"]["submission_marker_path"]).is_file()
    assert pending["budget"]["executed"] == pending["budget"]["actual"] == 0
    with pytest.raises(ValueError, match="resume_pending_execution_uncertain"):
        restore(tmp_path / "old", tmp_path / "new", doc)
    assert calls == []


def test_ingested_fixture_feedback_changes_next_pool_and_split_resume(tmp_path):
    from test_feedback_epoch_runner import adapter_runtime
    doc = profile()
    doc["budget"].update(max_inputs=5, warmup_inputs=0)

    def runtime(config, output):
        return adapter_runtime(config, output)

    whole = run_cycle(tmp_path / "whole", doc, runtime=runtime)
    first = run_cycle(tmp_path / "first", doc, runtime=runtime, maximum=2)
    restored = restore(tmp_path / "first", tmp_path / "second", doc)
    second = run_cycle(tmp_path / "second", doc, restored, runtime=runtime)
    assert projection(whole) == projection(first) + projection(second)
    assert second["budget"]["actual"] == second["budget"]["attempted"] == 5
    assert all(e["coverage"] and e["edge_id"] and e["edge_provenance"] for e in whole["feedback"]["events"])
    assert any(r["corpus_offer"]["admitted"] for r in whole["rows"])
    assert whole["corpus"]["entry_count"] > len(fresh(doc)["corpus"].entries)
    assert all(r["evidence_origin"] == "offline_fixture" for r in whole["rows"])
    assert all(r.get("candidate_status") in (None, "not_candidate") for r in whole["rows"])
    selections = whole["selection_log"]["selections"]
    # Coverage from an ingested raw fixture excludes at least one authored
    # surface/variant from the very next uncovered pool. No manual edge input.
    assert selections[1]["used_pool"] == "uncovered"
    assert selections[1]["candidate_count"] < selections[0]["candidate_count"]
    contexts = whole["selection_log"]["selection_contexts"]
    assert [c["feedback_event_count"] for c in contexts] == list(range(5))
    assert [c["feedback_events_sha256"] for c in contexts] == [
        canonical_hash(whole["feedback"]["events"][:n]) for n in range(5)]
    assert second.get("seal") is not True  # offline plumbing is not qualification


@pytest.mark.parametrize("counter", ["actual", "unique_actual", "unique_inputs"])
def test_checkpoint_rehashed_budget_cannot_invent_execution_or_unique_inputs(tmp_path, counter):
    doc = profile()
    run_cycle(tmp_path / "old", doc, maximum=1)
    path = tmp_path / "old/checkpoint.json"
    data = json.loads(path.read_text())
    budget = data["state"]["continuation"]["budget"]
    budget[counter] = 2
    if counter == "unique_actual":
        data["state"]["continuation"]["seen_actual_hashes"] = ["a" * 64, "b" * 64]
    elif counter == "unique_inputs":
        data["state"]["continuation"]["seen_input_hashes"] = ["a" * 64, "b" * 64]
    rehash_checkpoint(data)
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="budget"):
        restore(path, tmp_path / "new", doc)


def test_self_consistent_rehashed_seed_cannot_replace_profile_input(tmp_path):
    from r2f.campaign.corpus import corpus_identity
    doc = profile()
    run_cycle(tmp_path / "old", doc, maximum=1)
    path = tmp_path / "old/checkpoint.json"
    data = json.loads(path.read_text())
    corpus = Corpus.from_dict(data["state"]["corpus"])
    old_digest = sorted(corpus.entries)[0]
    record = corpus.entries.pop(old_digest)
    record["final_config"]["reader_delay_sec"] = 11
    record["content_sha256"] = content_identity(record)
    new_digest = record["content_sha256"]
    corpus.entries[new_digest] = record
    for event in corpus.history:
        if event["content_sha256"] == old_digest:
            event["content_sha256"] = new_digest
    corpus._rebuild_indexes()
    data["state"]["corpus"] = corpus.to_dict()
    data["state"]["corpus_identity"] = corpus_identity(corpus)
    assert Corpus.from_dict(corpus.to_dict()).to_dict() == corpus.to_dict()
    rehash_checkpoint(data)
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="fixed seed"):
        restore(path, tmp_path / "new", doc)


@pytest.mark.parametrize("damage", ["nonhex", "flags", "provenance"])
def test_checkpoint_rehash_cannot_validate_invalid_feedback_edge(tmp_path, damage):
    from test_feedback_epoch_runner import adapter_runtime
    doc = profile()
    run_cycle(tmp_path / "old", doc, maximum=1, runtime=adapter_runtime)
    path = tmp_path / "old/checkpoint.json"
    data = json.loads(path.read_text())
    events = data["state"]["feedback"]["events"]
    assert events[0]["edge_id"]
    if damage == "nonhex":
        events[0]["edge_id"] = "g" * 64
    elif damage == "flags":
        events[0]["executed"] = False
    else:
        events[0]["edge_provenance"]["observation"]["raw_sha256"] = "0" * 64
    events[0]["event_sha256"] = canonical_hash({k: v for k, v in events[0].items() if k != "event_sha256"})
    data["state"]["feedback"] = FeedbackLog.from_events(events).to_dict()
    rehash_checkpoint(data)
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="feedback"):
        restore(path, tmp_path / "new", doc)
