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


def legacy_noop_profile():
    doc = json.loads((REPO / "config/s1_fuzz/profiles/parameter.json").read_text())
    doc["budget"] = {"max_inputs": 6, "warmup_inputs": 2}
    return doc


def profile():
    """Finite parents plus an explicit range route, not M01's identity witness.

    M01(finite) deliberately preserves a positive finite parent. Checkpoint
    tests need a real input change before the planned/submission hooks exist.
    M03 samples a bounded, quantized neighbourhood; seed 17 below is replayed
    unchanged on resume, so the original deterministic assertions still apply.
    """
    doc = legacy_noop_profile()
    cycle = next(c for c in doc["cycles"] if c["cycle_id"] == CYCLE)
    doc["cycles"] = [cycle]
    doc["target_ids"] = [cycle["target_id"]]
    doc["surfaces"] = cycle["surfaces"]
    doc["composition"] = {"mutation_routes_schema": "r2f.mutation-routes/v1"}
    for spec in cycle["seed_specs"]:
        spec["mutation_routes"] = [{"surface_id": "P01",
                                    "boundary_class": "finite", "mutator_id": "M03"}]
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
    """Compare every selected task, including terminal no-change audits."""
    rows = []
    for row in result["rows"]:
        plan = row["mutation_plan"]
        common = {"parent_content_sha256": row["parent_content_sha256"],
                  "content_sha256": row["content_sha256"], "mutation_plan": plan,
                  "final_config": plan["final_config"]}
        if row.get("generation_status") == "rejected":
            assert row["reason_code"] == "no_effective_input"
            assert row["actual_run"] is False
            assert row["candidate_eligible"] is row["feedback_eligible"] is False
            assert row["execution_stage"] == "planned"
            assert canonical_hash(plan["final_config"]) == plan["parent_content_sha256"]
            assert row["no_effective_input_reason"]
            case = Path(result["manifest_path"]).parent / "cases" / row["cell_id"]
            assert not (case / "capsule.json").exists()
            assert not (case / "runtime.json").exists()
            assert not list(case.glob("submission*.json"))
            for field in ("raw_path", "raw_sha256", "capsule_execution", "execution_metadata"):
                assert row.get(field) is None
            common.update(kind="rejected", generation_status=row["generation_status"],
                          reason_code=row["reason_code"],
                          no_effective_input_reason=row["no_effective_input_reason"],
                          execution_stage=row["execution_stage"], actual_run=row["actual_run"])
        else:
            # Missing fields on a generated row are errors, not optional values.
            common.update(kind="generated", final_input=row["final_input"],
                          effective_input_sha256=row["effective_input_sha256"])
            assert len(common["effective_input_sha256"]) == 64
        rows.append(common)
    return rows


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
    assert whole["budget"]["generated"] == whole["budget"]["planned"] == 6
    assert whole["rejected"] == []
    assert all(canonical_hash(row["mutation_plan"]["final_config"]) !=
               row["mutation_plan"]["parent_content_sha256"] for row in whole["rows"])
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
    # One zero replacement covers its authored class; subsequent M03-derived
    # children and the zero child's legitimate no-op revisits are BOTH audited.
    for spec in doc["cycles"][0]["seed_specs"]:
        if spec["variant_id"] == "P01-quantized-020":
            spec["boundary_class"] = "zero"
            spec["mutation_routes"] = [{"surface_id": "P01", "boundary_class": "zero",
                                        "mutator_id": "M01"}]

    def runtime(config, output):
        return adapter_runtime(config, output)

    whole = run_cycle(tmp_path / "whole", doc, runtime=runtime)
    first = run_cycle(tmp_path / "first", doc, runtime=runtime, maximum=2)
    restored = restore(tmp_path / "first", tmp_path / "second", doc)
    second = run_cycle(tmp_path / "second", doc, restored, runtime=runtime)
    assert projection(whole) == projection(first) + projection(second)
    assert len(whole["rows"]) == second["budget"]["selected"] == 5
    assert whole["budget"]["generated"] == second["budget"]["generated"] == 3
    assert whole["budget"]["actual"] == whole["budget"]["attempted"] == 3
    assert second["budget"]["actual"] == second["budget"]["attempted"] == 3
    assert [r.get("generation_status") == "rejected" for r in whole["rows"]] == [False] * 3 + [True] * 2
    executed = [r for r in whole["rows"] if r["actual_run"] is True]
    assert len(executed) == len(whole["feedback"]["events"]) == 3
    assert all(e["coverage"] and e["edge_id"] and e["edge_provenance"] for e in whole["feedback"]["events"])
    assert any(r["corpus_offer"]["admitted"] for r in executed)
    assert whole["corpus"]["entry_count"] > len(fresh(doc)["corpus"].entries)
    assert all(r["evidence_origin"] == "offline_fixture" for r in executed)
    assert all(r.get("candidate_status") in (None, "not_candidate") for r in whole["rows"])
    selections = whole["selection_log"]["selections"]
    # Coverage from an ingested raw fixture excludes at least one authored
    # surface/variant from the very next uncovered pool. No manual edge input.
    assert selections[1]["used_pool"] == "uncovered"
    assert selections[1]["candidate_count"] < selections[0]["candidate_count"]
    contexts = whole["selection_log"]["selection_contexts"]
    assert [c["feedback_event_count"] for c in contexts] == [0, 1, 2, 3, 3]
    assert [c["feedback_events_sha256"] for c in contexts] == [
        canonical_hash(whole["feedback"]["events"][:n]) for n in [0, 1, 2, 3, 3]]
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
    doc = legacy_noop_profile()
    # Use a changing non-route boundary case here: the attack is specifically
    # against the fixed-seed gate, not the route-origin binding gate.
    for spec in next(c for c in doc["cycles"] if c["cycle_id"] == CYCLE)["seed_specs"]:
        spec["boundary_class"] = "zero"
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


@pytest.mark.parametrize("execute", [False, True])
def test_legacy_finite_noop_stays_rejected_before_capsule_and_submission(tmp_path, execute):
    doc = legacy_noop_profile()
    calls, phases = [], []

    def forbidden(config, output):
        calls.append(config)
        raise AssertionError("no-op must never submit the offline adapter")

    def record(hook):
        def wrapped(payload):
            phases.append(payload["phase"])
            hook(payload)
        return wrapped

    out = tmp_path / "noop"
    result = run_cycle(out, doc, maximum=1, runtime=forbidden if execute else None,
                       hook_wrapper=record)
    assert calls == []
    assert "planned" not in phases and "submitted" not in phases
    assert result["budget"]["selected"] == len(result["rejected"]) == 1
    for key in ("generated", "attempted", "actual", "executed", "unique_actual"):
        assert result["budget"][key] == 0
    assert result["feedback"]["events"] == []
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["reason_code"] == "no_effective_input"
    assert row["candidate_eligible"] is row["feedback_eligible"] is False
    assert row.get("actual_run") is not True
    assert canonical_hash(row["mutation_plan"]["final_config"]) == row["mutation_plan"]["parent_content_sha256"]
    assert not list(out.rglob("capsule.json"))
    assert not list(out.rglob("runtime.json"))
    assert not list(out.rglob("submission*.json"))
    checkpoint = json.loads((out / "checkpoint.json").read_text())
    assert checkpoint["state"]["continuation"]["pending_attempt"] is None


def test_rejected_prefix_then_planned_interrupt_resumes_exactly(tmp_path):
    """Do not hide rejected-selection checkpoint accounting behind all-valid seeds."""
    doc = profile()
    # The same finite parents/order as the old fixture. Only the last authored
    # parent gets a changing route; the first selected finite witness is no-op.
    for spec in doc["cycles"][0]["seed_specs"][:-1]:
        spec["mutation_routes"][0]["mutator_id"] = "M01"
    phases = []

    class Interrupted(BaseException):
        pass

    def interrupt(hook):
        def wrapped(payload):
            hook(payload)
            phases.append(payload["phase"])
            if payload["phase"] == "planned":
                raise Interrupted()
        return wrapped

    whole = run_cycle(tmp_path / "whole", doc)
    with pytest.raises(Interrupted):
        run_cycle(tmp_path / "old", doc, hook_wrapper=interrupt)
    checkpoint = json.loads((tmp_path / "old/checkpoint.json").read_text())
    pending = checkpoint["state"]["continuation"]
    assert "recorded" in phases[:phases.index("planned")]
    assert pending["pending_selection"]["sequence"] > 0
    assert pending["budget"]["generated"] == 1
    assert pending["budget"]["planned"] == pending["budget"]["selected"]
    assert pending["budget"]["attempted"] == pending["budget"]["executed"] == 0
    assert pending["pending_selection"]["sequence"] == pending["budget"]["selected"] - 1
    assert pending["pending_attempt"] is not None
    before = {str(p.relative_to(tmp_path / "old")): p.read_bytes()
              for p in (tmp_path / "old").rglob("*") if p.is_file()}
    state = restore(tmp_path / "old", tmp_path / "resumed", doc)
    resumed = run_cycle(tmp_path / "resumed", doc, state)
    prefix = whole["rows"][:pending["pending_selection"]["sequence"]]
    assert prefix and all(r["reason_code"] == "no_effective_input" for r in prefix)

    def identity(row):
        return (row.get("reason_code"), row["content_sha256"],
                row.get("effective_input_sha256"), row["mutation_plan"])

    assert [identity(r) for r in prefix + resumed["rows"]] == [identity(r) for r in whole["rows"]]
    assert resumed["selection_log"] == whole["selection_log"]
    assert resumed["budget"]["selected"] == whole["budget"]["selected"] == 6
    for key in ("generated", "planned", "attempted", "actual", "executed", "unique_inputs"):
        assert resumed["budget"][key] == whole["budget"][key]
    assert before == {str(p.relative_to(tmp_path / "old")): p.read_bytes()
                      for p in (tmp_path / "old").rglob("*") if p.is_file()}
