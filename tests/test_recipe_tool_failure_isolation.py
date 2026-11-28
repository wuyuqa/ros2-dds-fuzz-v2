"""Main-adopted recipe isolation through public runner; offline fixtures only.

Not ROS/DDS, natural SUT candidates, live qualification or maturity evidence.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler, required_catalog_hash
from r2f.core.evidence import raw_sha256, canonical_hash
from test_feedback_epoch_runner import FIXTURE_SOURCE, adapter_runtime
from test_fuzz_feedback_loop import parent_config, profile, seed


def one_recipe(name="recipe-A", interval=0.0105, rmw="rmw_cyclonedds_cpp"):
    corpus = Corpus()
    entry = seed(name, "W02", "finite", dict(parent_config(), publish_interval_sec=interval))
    entry.update(mutator_id="M03", rmw=rmw)
    corpus.add_seed(entry)
    return corpus


class ToolFailure:
    def __init__(self):
        self.calls = []

    def __call__(self, config, output_dir):
        self.calls.append(deepcopy(config))
        raise RuntimeError("controlled offline recipe tool failure")


def cycle(out, *, count=6, corpus=None, feedback=None, scheduler=None,
          runtime=None, execute=True, source=None, **kwargs):
    corpus = corpus if corpus is not None else one_recipe()
    feedback = feedback if feedback is not None else FeedbackLog()
    scheduler = scheduler or ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
    with patch("r2f.campaign.runner.source_manifest", return_value=source or FIXTURE_SOURCE):
        result = run_exploratory_cycle(out, corpus=corpus, feedback=feedback,
            scheduler=scheduler, profile=profile(), runtime=runtime,
            execute=execute, max_inputs=count, run_id=out.name, **kwargs)
    return result, corpus, feedback, scheduler


@pytest.mark.parametrize("count,submissions", [(4, 4), (5, 5), (6, 5)])
def test_fifth_tool_failure_isolates_only_further_submissions(tmp_path, count, submissions):
    catalog = required_catalog_hash()
    runtime = ToolFailure()
    result, _, feedback, _ = cycle(tmp_path / f"attempts-{count}", count=count, runtime=runtime)
    assert len(runtime.calls) == submissions
    assert len(feedback.events) == submissions
    assert result["budget"]["attempted"] == submissions
    assert all(e["tool_error"] and not e["coverage"] for e in feedback.events)
    assert result["abort"] is None
    assert required_catalog_hash() == catalog
    ledger = json.loads((tmp_path / f"attempts-{count}" / "candidate_ledger.json").read_text())
    assert ledger["natural_candidate_count"] == ledger["confirmed_candidates"] == 0
    assert ledger["candidates"] == []
    if count == 6:
        row = result["rows"][-1]
        assert row["reason_code"] == "recipe_tool_failure_isolated"
        assert row["execution_stage"] == "planned" and row["actual_run"] is False
        assert row["recipe_tool_failure_policy"]["consecutive_tool_failures"] == 5
        case = tmp_path / f"attempts-{count}" / "cases" / row["cell_id"]
        assert not (case / "submission_intent.json").exists()
        assert not (case / "runtime").exists()
    # Original failures remain independently queryable and never admitted.
    for row in result["rows"][:submissions]:
        saved = json.loads((tmp_path / f"attempts-{count}" / "cases" / row["cell_id"] / "row.json").read_text())
        assert saved["runtime_status"] == "tool_error"
        assert saved["runner_exception"]["detail"] == "controlled offline recipe tool failure"
        assert saved.get("candidate_status") not in {"candidate", "confirmed"}


def test_semantic_recipe_groups_sampled_contents_but_partitions_declared_axes(tmp_path):
    runtime = ToolFailure()
    result, _, feedback, scheduler = cycle(tmp_path / "grouped", runtime=runtime)
    recipes = [e["recipe_attempt"]["recipe"] for e in feedback.events]
    assert all(recipe == recipes[0] for recipe in recipes)
    recipe = recipes[0]
    assert recipe["seed"] == {"seed_id": "recipe-A", "variant_id": None}
    assert recipe["ordered_route"] == [{"surface_id": "W02", "mutator_id": "M03", "application_phase": "compile_time", "parameter_class": "finite"}]
    assert recipe["boundary_class"] == recipe["parameter_class"] == "finite"
    assert recipe["carrier"] and recipe["mode"]["runtime_boundary"] == "host"
    for section, field, value in [
        ("source", "head", "c" * 40), ("source", "behavior_tree_sha256", "d" * 64),
        ("target", "contract_hash", "e" * 64), ("target", "target_revision", 1),
        ("seed", "seed_id", "recipe-B"), ("mode", "observation_mode", "instrumentation")]:
        other = deepcopy(recipe)
        other[section][field] = value
        assert scheduler.recipe_submission_policy(other)["submission_allowed"]
    for field, value in [("rmw", "rmw_fastrtps_cpp"), ("boundary_class", "zero"),
                         ("parameter_class", "neighbor"), ("carrier", "other-carrier")]:
        other = deepcopy(recipe)
        other[field] = value
        assert scheduler.recipe_submission_policy(other)["submission_allowed"]
    other = deepcopy(recipe)
    other["ordered_route"][0]["mutator_id"] = "M01"
    assert scheduler.recipe_submission_policy(other)["submission_allowed"]
    # New actual inputs and a domain change never clear this recipe's isolation.
    blocked_runtime = ToolFailure()
    blocked, _, _, _ = cycle(tmp_path / "new-content", count=1,
        corpus=one_recipe(interval=0.0505), feedback=feedback, runtime=blocked_runtime, domain_id=174)
    assert not blocked_runtime.calls
    assert blocked["rows"][0]["content_sha256"] != result["rows"][0]["content_sha256"]
    assert blocked["rows"][0]["recipe_tool_failure_policy"]["status"] == "isolated"
    independent_runtime = ToolFailure()
    other_result, _, _, _ = cycle(tmp_path / "other-recipe", count=1,
        corpus=one_recipe("recipe-B"), feedback=feedback, runtime=independent_runtime)
    assert len(independent_runtime.calls) == 1 and not other_result["abort"]
    changed_source_runtime = ToolFailure()
    changed_source, _, _, _ = cycle(tmp_path / "new-source", count=1,
        feedback=feedback, runtime=changed_source_runtime,
        source=dict(FIXTURE_SOURCE, head="c" * 40, behavior_tree_sha256="d" * 64))
    assert len(changed_source_runtime.calls) == 1
    assert changed_source["rows"][0]["recipe_tool_failure_policy"]["consecutive_tool_failures"] == 0


def test_healthy_original_resets_its_streak_and_plans_do_not(tmp_path):
    failure = ToolFailure()
    first, corpus, feedback, scheduler = cycle(tmp_path / "four", count=4, runtime=failure)
    healthy, corpus, feedback, scheduler = cycle(tmp_path / "healthy", count=5,
        corpus=corpus, feedback=feedback, scheduler=scheduler,
        continuation=first["continuation"], runtime=adapter_runtime)
    assert feedback.events[-1]["recipe_attempt"]["outcome"] == "healthy"
    original_count = len(feedback.events)
    planned, corpus, feedback, scheduler = cycle(tmp_path / "planning", count=6,
        corpus=corpus, feedback=feedback, scheduler=scheduler,
        continuation=healthy["continuation"], execute=False)
    assert len(feedback.events) == original_count
    after = ToolFailure()
    final, _, feedback, _ = cycle(tmp_path / "after-reset", count=6, feedback=feedback,
                                  runtime=after)
    assert len(after.calls) == 5
    assert final["rows"][-1]["reason_code"] == "recipe_tool_failure_isolated"


def test_feedback_and_scheduler_restore_do_not_clear_isolation(tmp_path):
    _, corpus, feedback, scheduler = cycle(tmp_path / "before", runtime=ToolFailure())
    old = deepcopy(feedback.to_dict())
    restored = FeedbackLog.from_dict(json.loads(json.dumps(old)))
    resumed = ExploratoryScheduler.from_dict(json.loads(json.dumps(scheduler.to_dict())), corpus, restored)
    runtime = ToolFailure()
    result, _, _, _ = cycle(tmp_path / "restored", corpus=corpus, feedback=restored,
                            scheduler=resumed, count=1, runtime=runtime)
    assert not runtime.calls
    assert result["rows"][-1]["reason_code"] == "recipe_tool_failure_isolated"
    assert restored.to_dict() == old


@pytest.mark.parametrize("reason", ["domain_conflict:lock:fixture", "source_identity_invalid:fixture", "global_observer_invalid:fixture",
                                    "source_valid", "locks_valid", "observer_valid",
                                    "source_identity_changed_during_execution"])
def test_global_invalidity_aborts_first_attempt_without_waiting_for_five(tmp_path, reason):
    calls = []
    def invalid(config, output_dir):
        calls.append(config)
        if reason.endswith("_valid"):
            return {reason: False, "runtime_status": "unknown", "real_execution": False}
        raise RuntimeError(reason)
    result, _, feedback, _ = cycle(tmp_path / "invalid", runtime=invalid)
    assert len(calls) == len(result["rows"]) == len(feedback.events) == 1
    assert result["abort"]["reason_code"] == "global_execution_invalidity"
    assert result["abort"]["detail"] == ("global_" + reason + "_false" if reason.endswith("_valid") else reason)
    assert feedback.events[0]["recipe_attempt"]["outcome"] == "global_invalid"


@pytest.mark.parametrize("partition", ["recipe", "source", "rmw"])
def test_legacy_unknown_history_preserves_references_without_blocking_partitions(tmp_path, partition):
    feedback = FeedbackLog()
    feedback.record({"content_sha256": "a" * 64, "runtime_status": "tool_error", "actual_run": False})
    legacy = deepcopy(feedback.to_dict())
    restored = FeedbackLog.from_dict(legacy)
    runtime = ToolFailure()
    declared = profile()
    declared["rmws"] = ["rmw_cyclonedds_cpp", "rmw_fastrtps_cpp"]
    corpus = one_recipe("independent-B" if partition == "recipe" else "recipe-A",
                        rmw="rmw_fastrtps_cpp" if partition == "rmw" else "rmw_cyclonedds_cpp")
    scheduler = ExploratoryScheduler(corpus, restored, rng_seed=11, warmup=0)
    source = (dict(FIXTURE_SOURCE, head="c" * 40, behavior_tree_sha256="d" * 64)
              if partition == "source" else FIXTURE_SOURCE)
    path = tmp_path / ("legacy-" + partition)
    with patch("r2f.campaign.runner.source_manifest", return_value=source):
        result = run_exploratory_cycle(path, corpus=corpus, feedback=restored,
            scheduler=scheduler, profile=declared, runtime=runtime,
            execute=True, max_inputs=1, run_id=path.name)
    row = result["rows"][0]
    policy = row["recipe_tool_failure_policy"]
    (tmp_path / "legacy-partition-observed.json").write_text(json.dumps({
        "partition": partition, "calls": len(runtime.calls), "policy": policy,
        "feedback": restored.to_dict()}, indent=2) + "\n")
    assert len(runtime.calls) == 1, "unknown legacy attribution cannot isolate every new recipe"
    assert policy["submission_allowed"] and policy["status"] == "eligible"
    assert policy["consecutive_tool_failures"] == 0
    assert policy["unattributed_tool_event_refs"] == [legacy["events"][0]["event_sha256"]]
    assert feedback.to_dict() == legacy and restored.events[:1] == legacy["events"]
    assert "recipe_attempt" not in restored.events[0]
    recipe = restored.events[-1]["recipe_attempt"]["recipe"]
    state = scheduler.recipe_submission_policy(recipe)
    assert state["consecutive_tool_failures"] == 1
    assert state["unattributed_tool_event_refs"] == policy["unattributed_tool_event_refs"]
    roundtrip = FeedbackLog.from_dict(json.loads(json.dumps(restored.to_dict())))
    resumed = ExploratoryScheduler.from_dict(json.loads(json.dumps(scheduler.to_dict())), corpus, roundtrip)
    assert resumed.recipe_submission_policy(recipe) == state


def checkpoint_profile():
    from test_exploratory_checkpoint import profile as declared_profile
    doc = declared_profile()
    doc["budget"] = {"max_inputs": 7, "warmup_inputs": 0}
    doc["rmws"] = ["rmw_cyclonedds_cpp"]
    spec = doc["cycles"][0]["seed_specs"][0]
    spec["seed_id"] = "P01"
    spec["variant_id"] = "recipe-checkpoint"
    spec["overrides"]["logical_config"]["reader_delay_sec"] = 0.1505
    doc["cycles"][0]["seed_specs"] = [spec]
    return doc


def test_checkpoint_restores_isolation_and_explicit_owner_release_preserves_history(tmp_path):
    from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
    from r2f.campaign.corpus import corpus_from_profile
    doc = checkpoint_profile()
    cycle_id = doc["cycles"][0]["cycle_id"]
    corpus = corpus_from_profile(doc, cycle_id=cycle_id)
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
    first = tmp_path / "checkpoint-first"
    with patch("r2f.campaign.runner.source_manifest", return_value=FIXTURE_SOURCE):
        result = run_exploratory_cycle(first, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=doc, runtime=ToolFailure(), execute=True, max_inputs=5, run_id=first.name,
            checkpoint_hook=make_checkpoint_hook(first, profile=doc, cycle_id=cycle_id, source_manifest=FIXTURE_SOURCE))
    assert len(feedback.events) == 5
    assert all(e["recipe_attempt"]["outcome"] == "tool_failure" for e in feedback.events)
    original = {str(p.relative_to(first)): p.read_bytes() for p in first.rglob("*") if p.is_file()}
    restored = load_checkpoint(first, profile=doc, cycle_id=cycle_id, source_manifest=FIXTURE_SOURCE,
                               output_dir=tmp_path / "checkpoint-resume")
    runtime = ToolFailure()
    resumed_path = tmp_path / "checkpoint-resume"
    with patch("r2f.campaign.runner.source_manifest", return_value=FIXTURE_SOURCE):
        blocked = run_exploratory_cycle(resumed_path, **restored, profile=doc, runtime=runtime,
            execute=True, max_inputs=6, run_id=resumed_path.name,
            checkpoint_hook=make_checkpoint_hook(resumed_path, profile=doc, cycle_id=cycle_id,
                source_manifest=FIXTURE_SOURCE, continuation=restored["continuation"]))
    assert not runtime.calls
    assert blocked["rows"][0]["reason_code"] == "recipe_tool_failure_isolated"
    recipe = restored["feedback"].events[0]["recipe_attempt"]["recipe"]
    prefix = deepcopy(restored["feedback"].events)
    for fields in [{"owner": "", "reason": "revalidated", "revalidation_basis": {}},
                   {"owner": "main", "reason": "", "revalidation_basis": {}},
                   {"owner": "main", "reason": "revalidated", "revalidation_basis": {}}]:
        with pytest.raises(ValueError, match="owner revalidation"):
            restored["feedback"].release_recipe(recipe, **fields)
        assert restored["feedback"].events == prefix
    basis_path = tmp_path / "fixture-owner-revalidation.json"
    basis_path.write_text(json.dumps({"fixture_only": True, "owner": "main-review-owner",
        "reason": "controlled offline adapter revalidation", "original_attempt_count": 5}) + "\n")
    basis = {"path": str(basis_path), "sha256": raw_sha256(basis_path)}
    for bad_basis in [dict(basis, sha256="0" * 64), dict(basis, path=str(tmp_path / "missing.json"))]:
        with pytest.raises(ValueError, match="revalidation"):
            restored["feedback"].release_recipe(recipe, owner="main-review-owner",
                reason="controlled fixture revalidation", revalidation_basis=bad_basis)
        assert restored["feedback"].events == prefix
    release_request = {"recipe": recipe, "owner": "main-review-owner",
        "reason": "controlled fixture adapter revalidated", "revalidation_basis": basis}
    released_path = tmp_path / "checkpoint-released"
    restored = load_checkpoint(resumed_path, profile=doc, cycle_id=cycle_id, source_manifest=FIXTURE_SOURCE,
                               output_dir=released_path)
    released_runtime = ToolFailure()
    with patch("r2f.campaign.runner.source_manifest", return_value=FIXTURE_SOURCE):
        released = run_exploratory_cycle(released_path, **restored, profile=doc, runtime=released_runtime,
            execute=True, max_inputs=7, run_id=released_path.name, recipe_releases=[release_request],
            checkpoint_hook=make_checkpoint_hook(released_path, profile=doc, cycle_id=cycle_id,
                source_manifest=FIXTURE_SOURCE, continuation=restored["continuation"]))
    release_history = deepcopy(restored["feedback"].to_dict())
    basis["sha256"] = "0" * 64
    assert restored["feedback"].to_dict() == release_history
    release = restored["feedback"].events[5]
    assert release["recipe_policy_release"]["isolating_event_sha256"] == prefix[-1]["event_sha256"]
    assert len(released_runtime.calls) == 1
    assert released["budget"]["attempted"] == 6
    last = load_checkpoint(released_path, profile=doc, cycle_id=cycle_id, source_manifest=FIXTURE_SOURCE,
                           output_dir=tmp_path / "release-restored")
    assert last["feedback"].events[:5] == prefix
    # Rehashing a stale release cannot make it refer to the first failure.
    forged = last["feedback"].to_dict()
    event = forged["events"][5]
    event["recipe_policy_release"]["isolating_event_sha256"] = prefix[0]["event_sha256"]
    event["event_sha256"] = canonical_hash({k: v for k, v in event.items() if k != "event_sha256"})
    forged["identity"] = FeedbackLog.from_events(forged["events"]).identity()
    with pytest.raises(ValueError, match="current isolation"):
        FeedbackLog.from_dict(forged)
    assert len(last["feedback"].events) == 7  # six attempts, one owner release
    assert original == {str(p.relative_to(first)): p.read_bytes() for p in first.rglob("*") if p.is_file()}


def test_interleaved_recipes_have_independent_streaks(tmp_path):
    corpus = one_recipe()
    for entry in one_recipe("recipe-B", interval=0.0505).selectable():
        corpus.add_seed(entry)
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=100)
    runtime = ToolFailure()
    result, _, feedback, _ = cycle(tmp_path / "interleaved", count=12, corpus=corpus,
        feedback=feedback, scheduler=scheduler, runtime=runtime)
    assert len(runtime.calls) == 10
    keys = [e["recipe_attempt"]["recipe"]["seed"]["seed_id"] for e in feedback.events]
    assert keys == ["recipe-A", "recipe-B"] * 5
    assert [r["reason_code"] for r in result["rows"][-2:]] == ["recipe_tool_failure_isolated"] * 2


def test_expected_api_rejection_preserves_streak_without_counting_or_resetting(tmp_path):
    first, corpus, feedback, scheduler = cycle(tmp_path / "before-rejection", count=4, runtime=ToolFailure())
    def expected_rejection(config, output_dir):
        # Normalized runtime-boundary fixture classification; never an exception
        # string, coverage, successful effect, or natural candidate claim.
        return {"runtime_status": "expected_api_rejection", "real_execution": False,
                "reason": "controlled fixture expected API rejection"}
    rejected, corpus, feedback, scheduler = cycle(tmp_path / "api-rejection", count=5,
        corpus=corpus, feedback=feedback, scheduler=scheduler,
        continuation=first["continuation"], runtime=expected_rejection)
    assert feedback.events[-1]["recipe_attempt"]["outcome"] == "expected_api_rejection"
    recipe = feedback.events[0]["recipe_attempt"]["recipe"]
    assert scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"] == 4
    runtime = ToolFailure()
    final, _, _, _ = cycle(tmp_path / "after-rejection", count=7,
        corpus=corpus, feedback=feedback, scheduler=scheduler,
        continuation=rejected["continuation"], runtime=runtime)
    assert len(runtime.calls) == 1
    assert final["rows"][-1]["reason_code"] == "recipe_tool_failure_isolated"


def test_independent_replay_does_not_advance_original_streak_or_rewrite_originals(tmp_path):
    from r2f.campaign.replay import replay_cell
    replay_source = dict(FIXTURE_SOURCE, source_frozen=True, dirty_behavior_paths=[],
                         source_repo=str(Path(__file__).resolve().parents[1]))
    original, _, feedback, scheduler = cycle(tmp_path / "replay-original", count=4,
        runtime=ToolFailure(), source=replay_source)
    prefix = deepcopy(feedback.to_dict())
    files = {p: p.read_bytes() for p in (tmp_path / "replay-original").rglob("*") if p.is_file()}
    runtime = ToolFailure()
    with patch("r2f.campaign.replay.source_manifest", return_value=replay_source):
        replay = replay_cell(original["rows"][0]["replay_path"], output_dir=tmp_path / "independent-replay",
                             domain_id=174, runtime=runtime)
    assert len(runtime.calls) == 1
    assert replay["status"] != "pass"
    assert feedback.to_dict() == prefix
    recipe = feedback.events[0]["recipe_attempt"]["recipe"]
    assert scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"] == 4
    assert all(p.read_bytes() == data for p, data in files.items())


def test_declared_topology_operator_and_parameter_class_are_in_recipe(tmp_path):
    from r2f.campaign.corpus import corpus_from_profile
    repo = Path(__file__).resolve().parents[1]
    doc = json.loads((repo / "config/s1_fuzz/profiles/model-bfs-topology-runtime-smoke.json").read_text())
    corpus = corpus_from_profile(doc, cycle_id="topology-authored")
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=41, warmup=0, policy="bfs",
        sampling_kinds=doc.get("sampling_kinds"), sampling_weight=doc.get("sampling_weight"))
    with patch("r2f.campaign.runner.source_manifest", return_value=FIXTURE_SOURCE):
        result = run_exploratory_cycle(tmp_path / "topology-recipes", corpus=corpus, feedback=feedback,
            scheduler=scheduler, profile=doc, execute=True, runtime=ToolFailure(),
            max_inputs=4, run_id="topology-recipes")
    recipes = []
    for row in result["rows"]:
        if "generation_recipe" not in row:
            continue
        recipe = row["generation_recipe"]
        operation = row["mutation_plan"]["operations"][0]
        assert recipe["ordered_route"][0]["operator_id"] == operation["parameters"]["operator_id"]
        request = operation["parameters"]["request"]
        assert recipe["ordered_route"][0]["parameter_class"] == row["input_search"]["class_id"]
        recipes.append(recipe)
    assert len(recipes) >= 2
    assert len({r["ordered_route"][0]["operator_id"] for r in recipes}) >= 2
    assert all(not event["coverage"] for event in feedback.events)


def test_source_drift_aborts_even_when_recipe_is_already_isolated(tmp_path):
    _, _, feedback, _ = cycle(tmp_path / "isolated-source", runtime=ToolFailure())
    before = deepcopy(feedback.to_dict())
    corpus = one_recipe()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
    runtime = ToolFailure()
    reads = []
    def drifting_source(repo):
        reads.append(repo)
        return FIXTURE_SOURCE if len(reads) == 1 else dict(FIXTURE_SOURCE, head="f" * 40)
    with patch("r2f.campaign.runner.source_manifest", side_effect=drifting_source):
        result = run_exploratory_cycle(tmp_path / "source-drift", corpus=corpus,
            feedback=feedback, scheduler=scheduler, profile=profile(), runtime=runtime,
            execute=True, max_inputs=1, run_id="source-drift")
    assert not runtime.calls
    assert result["abort"]["reason_code"] == "source_repo_HEAD_or_behavior_tree_changed"
    assert feedback.to_dict() == before


@pytest.mark.parametrize("invalid_flag", ["source_valid", "locks_valid", "observer_valid"])
def test_global_invalidity_never_admits_otherwise_complete_observation(tmp_path, invalid_flag):
    calls = []
    def invalid(config, output_dir):
        calls.append(config)
        result = adapter_runtime(config, output_dir)
        result[invalid_flag] = False
        return result
    result, _, feedback, scheduler = cycle(tmp_path / "invalid-complete", runtime=invalid)
    assert len(calls) == len(result["rows"]) == 1
    assert result["abort"]["reason_code"] == "global_execution_invalidity"
    event = feedback.events[0]
    assert event["recipe_attempt"]["outcome"] == "global_invalid"
    assert not event["coverage"] and not event["complete"]
    assert not feedback.covered_surfaces() and not feedback.edge_counts()
    row = result["rows"][0]
    assert row["actual_run"] is True and row["execution_stage"] == "judged"
    assert raw_sha256(row["raw_path"]) == row["raw_sha256"]
    assert row["candidate_eligible"] is row["feedback_eligible"] is row["seal_eligible"] is False
    assert not row["corpus_offer"].get("admitted")
    assert row["corpus_offer"]["reason"] == "global_execution_invalidity"
    policy = scheduler.recipe_submission_policy(event["recipe_attempt"]["recipe"])
    assert policy["consecutive_tool_failures"] == 0


@pytest.mark.parametrize("runtime_outcome", ["complete", "wrapper_failure"])
def test_runtime_source_drift_preserves_failures_without_healthy_admission(tmp_path, runtime_outcome):
    # Borrow only this checkout's existing HEAD metadata, read-only. All fixture
    # behavior bytes live here, missing HEAD files keep source_frozen=false.
    # This exercises the real content-based source_manifest, not a validity flag.
    source_repo = tmp_path / "source"
    (source_repo / "src").mkdir(parents=True)
    import subprocess
    git_dir = subprocess.check_output(["git", "-C", str(Path(__file__).resolve().parents[1]),
                                       "rev-parse", "--absolute-git-dir"], text=True).strip()
    (source_repo / ".git").write_text("gitdir: " + git_dir + "\n")
    marker = source_repo / "src" / "offline_fixture.py"
    marker.write_text("before runtime\n")
    from r2f.campaign.identity import source_manifest
    before_source = source_manifest(source_repo)
    corpus, feedback = one_recipe(), FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0)
    first_path = tmp_path / "four-original-failures"
    first = run_exploratory_cycle(first_path, repo=source_repo, corpus=corpus,
        feedback=feedback, scheduler=scheduler, profile=profile(), runtime=ToolFailure(),
        execute=True, max_inputs=4, run_id=first_path.name)
    original = {str(p): raw_sha256(p) for p in first_path.rglob("*") if p.is_file()}
    prefix = deepcopy(feedback.events)
    recipe = prefix[0]["recipe_attempt"]["recipe"]
    assert scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"] == 4
    entries_before = set(corpus.entries)
    calls, returned = [], []
    def drifting_runtime(config, output_dir):
        calls.append(config)
        runtime = adapter_runtime(config, output_dir)
        returned.append({"raw_path": runtime["raw_path"], "raw_sha256": runtime["raw_sha256"]})
        marker.write_text("changed during runtime\n")
        if runtime_outcome == "wrapper_failure":
            (Path(output_dir) / "runtime_result.json").write_text('{"immutable_original": true}\n')
        return runtime
    path = tmp_path / "runtime-source-drift"
    result = run_exploratory_cycle(path, repo=source_repo, corpus=corpus,
        feedback=feedback, scheduler=scheduler, profile=profile(), runtime=drifting_runtime,
        execute=True, max_inputs=6, continuation=first["continuation"], run_id=path.name)
    row, event = result["rows"][0], feedback.events[-1]
    observed = {"abort": result["abort"], "row": row, "last_event": event,
                "policy": scheduler.recipe_submission_policy(recipe), "calls": len(calls),
                "source_before": before_source, "source_after": source_manifest(source_repo),
                "corpus_before": sorted(entries_before), "corpus_after": sorted(corpus.entries)}
    (tmp_path / "source-drift-observed.json").write_text(json.dumps(observed, indent=2) + "\n")
    assert not event["coverage"], "source-invalid runtime must not earn coverage before abort"
    assert not event["complete"] and event["recipe_attempt"]["outcome"] == "global_invalid"
    assert scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"] == 4
    assert len(calls) == len(result["rows"]) == 1
    assert result["abort"]["reason_code"] == "source_repo_HEAD_or_behavior_tree_changed"
    assert result["abort"]["detail"] == "source_repo_HEAD_or_behavior_tree_changed"
    assert row["candidate_eligible"] is row["feedback_eligible"] is row["seal_eligible"] is False
    assert not row["corpus_offer"].get("admitted") and set(corpus.entries) == entries_before
    assert not feedback.covered_surfaces() and not feedback.edge_counts()
    assert raw_sha256(returned[0]["raw_path"]) == returned[0]["raw_sha256"]
    if runtime_outcome == "complete":
        assert row["actual_run"] is True and row["execution_stage"] == "judged"
        assert raw_sha256(row["raw_path"]) == row["raw_sha256"] == returned[0]["raw_sha256"]
        assert row["raw_path"] == returned[0]["raw_path"]
    else:
        assert row["runner_exception"]["detail"] == "runtime_result_differs_from_returned_evidence"
        assert Path(row["runtime_result_path"]).read_text() == '{"immutable_original": true}\n'
    assert feedback.events[:-1] == prefix
    assert all(raw_sha256(Path(p)) == digest for p, digest in original.items())
    assert before_source["head"] == observed["source_after"]["head"]
    assert before_source["behavior_tree_sha256"] != observed["source_after"]["behavior_tree_sha256"]
    ledger = json.loads((path / "candidate_ledger.json").read_text())
    assert ledger["natural_candidate_count"] == ledger["confirmed_candidates"] == 0


def test_expected_api_label_cannot_hide_immutable_persistence_failure(tmp_path):
    calls, originals = [], {}
    original_bytes = b'{"independent_existing_result": true}\n'
    def conflicting_runtime(config, output_dir):
        calls.append(config)
        result_path = Path(output_dir) / "runtime_result.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_bytes(original_bytes)
        originals[str(result_path)] = raw_sha256(result_path)
        return {"runtime_status": "expected_api_rejection", "real_execution": False,
                "reason": "offline API rejection with an actual immutable artifact conflict"}
    path = tmp_path / "persistence-conflict"
    result, _, feedback, scheduler = cycle(path, count=6, runtime=conflicting_runtime)
    observed = {"calls": len(calls), "rows": result["rows"], "events": feedback.events,
                "originals": originals}
    (tmp_path / "persistence-conflict-observed.json").write_text(json.dumps(observed, indent=2) + "\n")
    assert len(calls) == 5, "real persistence failures must isolate before sixth submission"
    assert result["abort"] is None and result["budget"]["attempted"] == 5
    assert len(feedback.events) == 5
    assert all(e["recipe_attempt"]["outcome"] == "tool_failure" for e in feedback.events)
    assert all(e["tool_error"] and not e["coverage"] for e in feedback.events)
    for row, event in zip(result["rows"], feedback.events):
        assert row["runner_exception"]["detail"] == "runtime_result_differs_from_returned_evidence"
        assert row["runtime_status"] == "tool_error"
        ref = event["recipe_attempt"]["diagnostic"]["runtime_result"]
        assert ref["sha256"] == originals[ref["path"]] == raw_sha256(Path(ref["path"]))
        assert Path(ref["path"]).read_bytes() == original_bytes
    blocked = result["rows"][-1]
    assert blocked["reason_code"] == "recipe_tool_failure_isolated"
    assert blocked["execution_stage"] == "planned" and blocked["actual_run"] is False
    case = path / "cases" / blocked["cell_id"]
    assert not (case / "submission_intent.json").exists() and not (case / "runtime").exists()
    recipe = feedback.events[0]["recipe_attempt"]["recipe"]
    assert scheduler.recipe_submission_policy(recipe)["consecutive_tool_failures"] == 5
    assert all(raw_sha256(Path(p)) == digest for p, digest in originals.items())


def test_unknown_legacy_refs_survive_keyed_isolation_and_named_owner_release(tmp_path):
    feedback = FeedbackLog()
    feedback.record({"content_sha256": "a" * 64, "runtime_status": "tool_error", "actual_run": False})
    legacy = deepcopy(feedback.events[0])
    runtime = ToolFailure()
    original_path = tmp_path / "legacy-and-keyed-isolation"
    result, corpus, feedback, scheduler = cycle(original_path, feedback=feedback, runtime=runtime)
    assert len(runtime.calls) == 5 and result["rows"][-1]["reason_code"] == "recipe_tool_failure_isolated"
    recipe = feedback.events[-1]["recipe_attempt"]["recipe"]
    isolated = scheduler.recipe_submission_policy(recipe)
    assert isolated["status"] == "isolated" and not isolated["submission_allowed"]
    assert isolated["unattributed_tool_event_refs"] == [legacy["event_sha256"]]
    original_files = {str(p): raw_sha256(p) for p in original_path.rglob("*") if p.is_file()}
    prefix = deepcopy(feedback.to_dict())
    restored = FeedbackLog.from_dict(json.loads(json.dumps(prefix)))
    basis = tmp_path / "owner-revalidation.json"
    basis.write_text('{"offline_adapter_revalidated": true}\n')
    fields = {"reason": "controlled offline adapter revalidation",
              "revalidation_basis": {"path": str(basis), "sha256": raw_sha256(basis)}}
    with pytest.raises(ValueError, match="named owner"):
        restored.release_recipe(recipe, owner="", **fields)
    assert restored.to_dict() == prefix
    restored.release_recipe(recipe, owner="main-offline-fixture", **fields)
    resumed_log = FeedbackLog.from_dict(json.loads(json.dumps(restored.to_dict())))
    resumed_scheduler = ExploratoryScheduler(corpus, resumed_log, rng_seed=11, warmup=0)
    released = resumed_scheduler.recipe_submission_policy(recipe)
    assert released["submission_allowed"] and released["consecutive_tool_failures"] == 0
    assert released["unattributed_tool_event_refs"] == isolated["unattributed_tool_event_refs"]
    resumed_runtime = ToolFailure()
    _, _, resumed_log, resumed_scheduler = cycle(tmp_path / "released-legacy", count=1,
        corpus=corpus, feedback=resumed_log, scheduler=resumed_scheduler, runtime=resumed_runtime)
    assert len(resumed_runtime.calls) == 1
    final = resumed_scheduler.recipe_submission_policy(recipe)
    assert final["consecutive_tool_failures"] == 1 and final["submission_allowed"]
    assert final["unattributed_tool_event_refs"] == [legacy["event_sha256"]]
    assert resumed_log.events[:len(prefix["events"])] == prefix["events"]
    assert resumed_log.events[0] == legacy and "recipe_attempt" not in resumed_log.events[0]
    assert resumed_log.events[-2]["recipe_policy_release"]["owner"] == "main-offline-fixture"
    assert all(raw_sha256(Path(p)) == digest for p, digest in original_files.items())
