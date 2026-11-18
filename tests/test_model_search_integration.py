"""Shared-runner and checkpoint integration, strictly offline planning fixtures."""
from copy import deepcopy
import json
from unittest.mock import Mock, patch

import pytest

from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.campaign.search_frontier import InputSearchFrontier
from r2f.core.evidence import canonical_hash
from test_model_search_frontier import CYCLE, ROOT, fresh, generate, profile

SOURCE = {"source_repo": "/input-search-offline-fixture", "head": "fixture-head",
          "behavior_tree_sha256": "f" * 64, "source_frozen": False,
          "dirty_behavior_paths": ["offline-fixture"]}


def run(out, doc, state=None, maximum=6, checkpoint=True, wrapper=None):
    state = state or fresh(doc)
    hook = make_checkpoint_hook(out, profile=doc, cycle_id=CYCLE, source_manifest=SOURCE,
        continuation=state["continuation"]) if checkpoint else None
    if wrapper:
        hook = wrapper(hook)
    runtime = Mock(side_effect=AssertionError("offline input search must not invoke runtime"))
    with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
        result = run_exploratory_cycle(out, **state, profile=doc, max_inputs=maximum,
            checkpoint_hook=hook, execute=False, runtime=runtime, run_id=out.name)
    runtime.assert_not_called()
    return result


def restore(path, out, doc):
    return load_checkpoint(path, profile=doc, cycle_id=CYCLE, source_manifest=SOURCE, output_dir=out)


def projection(result):
    return [(r["parent_content_sha256"], r["content_sha256"], r["mutation_plan"],
             r["effective_input_sha256"], r["input_search"]) for r in result["rows"]]


def test_runner_plans_descendants_outside_novelty_corpus_without_execution(tmp_path):
    doc = profile()
    result = run(tmp_path / "offline", doc)
    assert not result["abort"] and not result["rejected"]
    assert len(result["rows"]) == 6
    assert result["budget"]["actual"] == 0
    assert result["feedback"]["events"] == []
    assert result["corpus"]["entry_count"] == 1
    nodes = result["selection_log"]["frontier"]["nodes"]
    assert len(nodes) > 1
    selections = result["selection_log"]["selections"]
    assert any(e["derivation_depth"] > 1 for e in selections)
    for row in result["rows"]:
        assert row["execution_stage"] == "planned" and row["actual_run"] is False
        assert row["input_search"]["actual"] is False and row["input_search"]["execution"] is None
        capsule = json.loads((tmp_path / "offline" / "cases" / row["cell_id"] / "capsule.json").read_text())
        assert capsule["execution"] is None
    assert {e["mutation_route"]["surface_id"] for e in selections[:4]} == {"P01", "W02"}


def test_checkpoint_split_resume_matches_continuous_generation_and_full_replay_sha(tmp_path):
    doc = profile()
    continuous = run(tmp_path / "continuous", doc, maximum=9)
    part = run(tmp_path / "part", doc, maximum=5)
    restored = restore(tmp_path / "part/checkpoint.json", tmp_path / "resumed", doc)
    resumed = run(tmp_path / "resumed", doc, restored, maximum=9)
    assert projection(part) + projection(resumed) == projection(continuous)
    assert resumed["selection_log"] == continuous["selection_log"]
    assert resumed["budget"]["actual"] == 0


def test_pending_derived_parent_not_in_corpus_resumes_without_second_selection(tmp_path):
    doc = profile()
    class StopAfterSavedPlan(Exception):
        pass
    def interrupt(hook):
        def callback(payload):
            hook(payload)
            if payload["phase"] == "planned" and payload["pending_selection"]["derivation_depth"] == 2:
                raise StopAfterSavedPlan()
        return callback
    with pytest.raises(StopAfterSavedPlan):
        run(tmp_path / "interrupted", doc, maximum=6, wrapper=interrupt)
    data = json.loads((tmp_path / "interrupted/checkpoint.json").read_text())
    pending = data["state"]["continuation"]["pending_selection"]
    corpus_ids = {entry["content_sha256"] for entry in data["state"]["corpus"]["entries"]}
    assert pending["parent_content_sha256"] not in corpus_ids
    restored = restore(tmp_path / "interrupted/checkpoint.json", tmp_path / "resumed", doc)
    result = run(tmp_path / "resumed", doc, restored, maximum=6)
    continuous = run(tmp_path / "continuous", doc, maximum=6)
    assert result["selection_log"] == continuous["selection_log"]
    assert result["budget"]["selected"] == 6
    assert not result["abort"]


def rehash(data):
    scheduler = data["state"]["scheduler"]
    scheduler["selection_identity"] = canonical_hash({k:v for k,v in scheduler.items() if k != "selection_identity"})
    data["checkpoint_sha256"] = canonical_hash({k:v for k,v in data.items() if k != "checkpoint_sha256"})


@pytest.mark.parametrize("damage", ["schema", "task_rng", "root_target", "route", "status", "journal"])
def test_checkpoint_tamper_fails_even_if_outer_hashes_recomputed(tmp_path, damage):
    doc = profile()
    run(tmp_path / "original", doc, maximum=3)
    data = json.loads((tmp_path / "original/checkpoint.json").read_text())
    f = data["state"]["scheduler"]["frontier"]
    if damage == "schema":
        f["schema"] = "unknown"
    elif damage == "task_rng":
        f["tasks"][0]["rng_seed"] += 1
    elif damage == "root_target":
        f["roots"][0]["target_id"] = "discovery.late_join_delivery.fuzz_v1"
    elif damage == "route":
        f["tasks"][0]["mutation_route"]["surface_id"] = "E01"
    elif damage == "status":
        f["tasks"][0]["status"] = "pending"
    else:
        f["journal"].pop()
    rehash(data)
    damaged = tmp_path / "tampered.json"
    damaged.write_text(json.dumps(data))
    with pytest.raises((ValueError, KeyError)):
        restore(damaged, tmp_path / "refused", doc)
    assert not (tmp_path / "refused").exists()


def test_self_consistent_alternate_generated_plan_rejected_by_model_not_hash(tmp_path):
    doc = profile()
    state = fresh(doc)
    selected = state["scheduler"].next()
    # A valid plan from a different RNG is not the declared slot's plan.
    altered = deepcopy(selected)
    altered["rng_seed"] += 1
    generate(state["scheduler"], doc, altered)
    frontier = InputSearchFrontier.from_dict(state["scheduler"].frontier.to_dict())
    from r2f.core import Registry
    from r2f.modules.s1.module import register_exploratory
    registry = Registry()
    register_exploratory(registry, include_required=False)
    with pytest.raises(ValueError, match="model-generatable"):
        frontier.validate_model(profile=doc, seeds=state["corpus"], registry=registry)


def test_chain_length_is_not_cross_generation_depth(tmp_path):
    doc = profile()
    doc["chain_length"] = 2
    doc["search"].update(max_depth=1, samples_per_class=1)
    result = run(tmp_path / "chain", doc, maximum=10)
    assert result["stop_reason"] == "input_search_depth_or_tasks_exhausted"
    assert len(result["rows"]) == 2
    assert all(r["chain_depth"] == 2 and r["input_search"]["derivation_depth"] == 1
               for r in result["rows"])


def test_duplicate_content_never_reaches_fixture_runtime_or_inflates_attempts(tmp_path):
    """Raw-producing offline fixture only, not a ROS/DDS qualification run."""
    from test_feedback_epoch_runner import adapter_runtime
    doc = profile()
    doc["search"].update(max_depth=1, samples_per_class=4)
    doc["sampling_kinds"] = ["representative"]
    state = fresh(doc)
    calls = []
    def fixture_runtime(config, output):
        calls.append(config["content_sha256"])
        return adapter_runtime(config, output)
    with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
        result = run_exploratory_cycle(tmp_path / "duplicate-fixture", **state,
            profile=doc, max_inputs=8, execute=True, runtime=fixture_runtime,
            run_id="offline-duplicate-fixture")
    assert not result["abort"]
    duplicates = [r for r in result["rows"] if r.get("reason_code") == "model_duplicate_content"]
    assert duplicates
    summary = result["search_summary"]
    tasks = result["selection_log"]["frontier"]["tasks"]
    assert summary["duplicate_content"] == len(duplicates)
    assert summary["rejected"] == sum(t["status"] == "rejected" for t in tasks)
    assert summary["actual"] == summary["executed"] == summary["submitted"] == len(calls)
    assert summary["completed"] + summary["rejected"] + summary["tasks_pending"] == len(tasks)
    assert len(calls) == len(set(calls))
    assert result["budget"]["selected"] == result["budget"]["planned"] == 8
    assert result["budget"]["generated"] == sum(r.get("mutation_plan") is not None for r in result["rows"])
    assert result["budget"]["attempted"] == len(calls)
    assert result["budget"]["executed"] == sum(
        r.get("actual_run") is True and r["execution_stage"] == "judged" for r in result["rows"])
    assert len(result["feedback"]["events"]) == len(calls)
    for row in duplicates:
        assert row["generation_status"] == "rejected" and row["actual_run"] is False
        assert row["input_search"]["execution"] is None
        assert row["content_sha256"] in calls
        task_id = row["input_search"]["task_id"]
        assert not any(e["action"] == "submit" and e["task_id"] == task_id
                       for e in result["selection_log"]["frontier"]["journal"])
    for row in result["rows"]:
        if row.get("actual_run"):
            assert row["evidence_origin"] == "offline_fixture"
            assert row["input_search"]["execution"]["formal_runtime"] is False


def test_loader_receives_only_model_verified_frontier_parents(tmp_path):
    """A retained fixture observation may reference a model-only ancestor."""
    from r2f.campaign.corpus import Corpus
    from r2f.campaign.checkpoint import _identity, _validate_state
    doc = profile()
    doc["search"]["samples_per_class"] = 1
    state = fresh(doc)
    result = run(tmp_path / "planned", doc, state, maximum=3)
    frontier = state["scheduler"].frontier
    task = frontier.task(state["scheduler"].selection_log[-1]["task_id"])
    grandchild = task["generated_record"]
    parent = grandchild["parent_content_sha256"]
    assert parent not in state["corpus"].entries
    # Explicit corpus admission unit fixture, NOT an executed runner row,
    # candidate or formal feedback. The checkpoint still has actual=0.
    admitted = state["corpus"].offer(grandchild, executed=True, evidence_complete=True,
                                    observed_edge="offline-loader-test-edge", oracle_verdict="unknown")
    assert admitted["admitted"]
    data = state["corpus"].to_dict()
    with pytest.raises(ValueError, match="parent is missing"):
        Corpus.from_dict(data)
    identity, seeds, registry = _identity(doc, CYCLE, SOURCE)
    envelope = json.loads((tmp_path / "planned/checkpoint.json").read_text())
    envelope["state"]["corpus"] = data
    from r2f.campaign.corpus import corpus_identity
    envelope["state"]["corpus_identity"] = corpus_identity(data)
    loaded, _, _, _ = _validate_state(envelope["state"], identity, seeds, registry)
    assert loaded.to_dict() == data
    assert parent not in loaded.entries
    assert loaded.history == state["corpus"].history
    # A hash-consistent foreign model record must fail before external loader
    # qualification. An empty/unknown frontier is not a trusted-parent list.
    envelope["state"]["scheduler"]["frontier"]["schema"] = "untrusted-future"
    with pytest.raises(ValueError, match="schema"):
        _validate_state(envelope["state"], identity, seeds, registry)
