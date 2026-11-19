"""Canonical runner/CLI terminal wiring, strictly offline synthetic fixtures.

Mock frozen manifests exercise gates; they do not freeze this checkout or
certify runtime/effect/candidate/maturity. No ROS/DDS/campaign is launched.
"""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.feedback_aggregate import FeedbackAggregate
from r2f.campaign.runner import run_exploratory_cycle, _terminal_feedback_aggregate
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core import Registry
from r2f.modules.s1.module import register_exploratory
from test_feedback_epoch_runner import changing_corpus, adapter_runtime
from test_fuzz_feedback_loop import profile, TARGET

ROOT = Path(__file__).resolve().parents[1]
SOURCE = {"source_repo": str(ROOT), "head": "a" * 40, "behavior_tree_sha256": "b" * 64,
          "source_frozen": True, "dirty_behavior_paths": []}  # gate fixture only


@pytest.fixture
def stable_source(monkeypatch):
    monkeypatch.setattr("r2f.campaign.runner.source_manifest", lambda repo: deepcopy(SOURCE))


def run(tmp_path, name, *, aggregate=None, source=None, execute=True, state=None, continuation=None, max_inputs=2):
    c, f, s = state or (changing_corpus(), FeedbackLog(), None)
    s = s or ExploratoryScheduler(c, f, rng_seed=4, warmup=0)
    result = run_exploratory_cycle(tmp_path / name, corpus=c, feedback=f, scheduler=s,
        profile=profile(), run_id=name, execute=execute, runtime=adapter_runtime if execute else None,
        max_inputs=max_inputs, feedback_aggregate=aggregate, continuation=continuation)
    return result, (c, f, s)


def test_opt_in_uses_real_runner_terminal_and_canonical_feedback_api(tmp_path, stable_source):
    path = tmp_path / "aggregate.json"
    first, state = run(tmp_path, "one", aggregate=path)
    summary = first["feedback_aggregate"]
    assert summary["status"] == "merged", summary
    assert summary["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    local_count = len(first["feedback"]["events"])
    assert local_count > 0
    assert summary["statistics"]["observation_events"] == local_count
    assert summary["statistics"]["global_unique_edges"] == len(first["feedback"]["edge_counts"])
    # Terminal aggregation is not a scheduler epoch or input-identity input.
    plain, _ = run(tmp_path, "plain")
    assert "feedback_aggregate" not in plain
    assert [r["content_sha256"] for r in first["rows"]] == [r["content_sha256"] for r in plain["rows"]]
    assert [r.get("effective_input_sha256") for r in first["rows"]] == [r.get("effective_input_sha256") for r in plain["rows"]]
    # Raw paths differ across run directories, so full feedback-event hashes
    # and audit contexts legitimately differ. Actual choices/RNG are identical.
    assert first["selection_log"]["selections"] == plain["selection_log"]["selections"]
    assert first["selection_log"]["feedback_identity"] == plain["selection_log"]["feedback_identity"]
    assert state[2].to_dict() == first["selection_log"]
    second, _ = run(tmp_path, "two", aggregate=path)
    assert second["feedback_aggregate"]["status"] == "merged"
    assert second["feedback_aggregate"]["statistics"]["observation_events"] == local_count + len(second["feedback"]["events"])
    assert second["feedback_aggregate"]["import_receipt"]["global_new_edge_ids"] == []
    persisted = json.loads((tmp_path / "two/cycle.json").read_text())
    assert persisted["feedback_aggregate"] == second["feedback_aggregate"]
    assert state[2].feedback.events == state[1].events
    assert len(state[1].events) == local_count  # not swapped for global feedback


def test_feedbacklog_entrypoints_and_persisted_reimport_are_idempotent(tmp_path, stable_source):
    path = tmp_path / "aggregate.json"
    result, _ = run(tmp_path, "one", aggregate=path)
    gate = result["feedback_aggregate"]["compatibility"]
    log = FeedbackLog.from_partitions([
        {"run_id": "one", "feedback": result["feedback"], "compatibility": gate},
        {"run_id": "one", "feedback": result["feedback"], "compatibility": gate}], compatibility=gate)
    assert log.to_dict() == result["feedback"]
    before = path.read_bytes()
    restored, receipt = FeedbackLog.merge_partition_file(path, compatibility=gate, run_id="one", feedback=result["feedback"])
    assert receipt["imported_events"] == 0
    assert restored.to_dict() == log.to_dict()
    assert path.read_bytes() == before


def test_resume_does_not_reimport_inherited_feedback_as_a_new_run(tmp_path, stable_source):
    path = tmp_path / "aggregate.json"
    first, state = run(tmp_path, "first", aggregate=path)
    c, f, s = state
    restored = ExploratoryScheduler.from_dict(s.to_dict(), c, f)
    second, _ = run(tmp_path, "resumed", aggregate=path, state=(c, f, restored),
                    continuation=first["continuation"], max_inputs=4)
    summary = second["feedback_aggregate"]
    assert summary["status"] == "merged", summary
    inherited = len(first["feedback"]["events"])
    total = len(second["feedback"]["events"])
    assert total > inherited
    assert summary["inherited_feedback_events_excluded"] == inherited
    assert summary["import_receipt"]["imported_events"] == total - inherited
    assert summary["statistics"]["observation_events"] == total


def test_plan_and_unfrozen_source_never_create_aggregate(tmp_path, stable_source, monkeypatch):
    planned, _ = run(tmp_path, "planned", execute=False, aggregate=tmp_path / "plan.json")
    assert planned["feedback_aggregate"]["status"] == "skipped"
    assert not (tmp_path / "plan.json").exists()
    monkeypatch.setattr("r2f.campaign.runner.source_manifest", lambda repo: dict(SOURCE, source_frozen=False))
    result, _ = run(tmp_path, "unfrozen", aggregate=tmp_path / "unfrozen.json")
    assert result["feedback_aggregate"]["status"] == "error"
    assert "frozen" in result["feedback_aggregate"]["detail"]
    assert not (tmp_path / "unfrozen.json").exists()
    assert len(result["feedback"]["events"]) > 0
    assert result["abort"] is None


def registry():
    reg = Registry(); register_exploratory(reg, include_required=False)
    return reg


def terminal(tmp_path, path, result):
    return _terminal_feedback_aggregate(path, result, registry(), output_dir=tmp_path / "one", repo=ROOT)


def test_failure_does_not_mask_original_abort_or_feedback(tmp_path, stable_source, monkeypatch):
    result, _ = run(tmp_path, "one")
    result["abort"] = {"reason_code": "fixture_original_abort"}
    original = deepcopy(result)
    import r2f.campaign.feedback_aggregate as module
    def fail(*args, **kwargs):
        raise OSError("fixture disk full")
    monkeypatch.setattr(module.os, "replace", fail)
    summary = terminal(tmp_path, tmp_path / "aggregate.json", result)
    assert summary["status"] == "error" and "fixture disk full" in summary["detail"]
    assert summary["original_abort_preserved"] is True
    assert result == original
    assert not (tmp_path / "aggregate.json").exists()


@pytest.mark.parametrize("damage", ["multiple_targets", "rule", "source", "state_schema", "unknown_coverage"])
def test_scope_and_unknown_fail_closed_preserving_prior_file(tmp_path, stable_source, damage):
    path = tmp_path / "aggregate.json"
    result, _ = run(tmp_path, "one", aggregate=path)
    original = path.read_bytes()
    bad = deepcopy(result)
    if damage == "multiple_targets":
        bad["profile"]["target_ids"].append("other.target")
    elif damage == "rule":
        bad["rows"][0]["rule_id"] = "other.rule"
    elif damage == "source":
        bad["source_manifest"]["head"] = "d" * 40
    elif damage == "state_schema":
        bad["profile"]["target_ids"] = ["unsupported.target"]
    else:
        from r2f.core.evidence import canonical_hash
        log = FeedbackLog.from_events(bad["feedback"]["events"])
        log.events[0]["oracle_verdict"] = "unknown"
        log.events[0]["event_sha256"] = canonical_hash({k: v for k, v in log.events[0].items() if k != "event_sha256"})
        bad["feedback"] = log.to_dict()
    summary = terminal(tmp_path, path, bad)
    assert summary["status"] == "error", summary
    assert path.read_bytes() == original


def test_path_collisions_symlinks_and_existing_artifacts_never_overwritten(tmp_path, stable_source):
    result, _ = run(tmp_path, "one")
    original = (tmp_path / "one/feedback.json").read_bytes()
    assert terminal(tmp_path, tmp_path / "one/feedback.json", result)["status"] == "error"
    assert (tmp_path / "one/feedback.json").read_bytes() == original
    protected = tmp_path / "old-result.json"; protected.write_text('{"original":true}')
    before = protected.read_bytes()
    assert terminal(tmp_path, protected, result)["status"] == "error"
    assert protected.read_bytes() == before
    symlink = tmp_path / "symlink.json"; symlink.symlink_to(protected)
    assert terminal(tmp_path, symlink, result)["status"] == "error"
    assert protected.read_bytes() == before
    source_path = ROOT / "src/r2f/campaign/feedback.py"
    source_bytes = source_path.read_bytes()
    assert terminal(tmp_path, source_path, result)["status"] == "error"
    assert source_path.read_bytes() == source_bytes


def test_cli_exposes_flag_passes_it_and_outputs_terminal_summary(tmp_path, monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location("feedback_cli_fixture", ROOT / "scripts/run_s1.py")
    cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
    seen = {}
    def fake_runner(*args, **kwargs):
        seen.update(kwargs)
        return {"run_id": "cli-fixture", "rows": [], "runtime_boundary": "host", "runtime_formal": False,
                "feedback_aggregate": {"status": "skipped", "reason_code": "fixture_no_execution"}}
    monkeypatch.setattr(cli, "run_exploratory_cycle", fake_runner)
    path = ROOT / "config/s1_fuzz/profiles/model-bfs-routes.json"
    aggregate = tmp_path / "aggregate.json"
    assert cli.main(["--output-dir", str(tmp_path / "out"), "--exploratory-profile", str(path),
        "--cycle-id", "common-baseline", "--feedback-aggregate", str(aggregate)]) == 0
    assert seen["feedback_aggregate"] == aggregate
    assert json.loads(capsys.readouterr().out)["feedback_aggregate"]["status"] == "skipped"
    with pytest.raises(SystemExit) as error:
        cli.main(["--output-dir", str(tmp_path / "required"), "--feedback-aggregate", str(aggregate)])
    assert error.value.code == 2
