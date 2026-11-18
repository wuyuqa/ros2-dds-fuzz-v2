"""CLI offline contract checks: saved state, shared log and explicit budget cap."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_exploratory_checkpoint import CYCLE, REPO, SOURCE, profile, run_cycle


@pytest.fixture
def cli(monkeypatch):
    spec = importlib.util.spec_from_file_location("checkpoint_run_s1_cli", REPO / "scripts/run_s1.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "source_manifest", lambda _repo: deepcopy(SOURCE))
    monkeypatch.setattr("r2f.campaign.runner.source_manifest", lambda _repo: deepcopy(SOURCE))
    return module


def arguments(tmp_path, doc, output="new"):
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(doc))
    return ["--output-dir", str(tmp_path / output), "--exploratory-profile", str(profile_path),
            "--cycle-id", CYCLE]


def test_cli_uses_profile_cap_warmup_and_outputs_checkpoint_without_ros(cli, tmp_path):
    doc = profile()
    doc["budget"].update(max_inputs=3, warmup_inputs=1)
    assert cli.main(arguments(tmp_path, doc)) == 0
    result = json.loads((tmp_path / "new/cycle.json").read_text())
    checkpoint = json.loads((tmp_path / "new/checkpoint.json").read_text())
    assert result["budget"]["selected"] == result["budget"]["max_inputs"] == 3
    assert checkpoint["state"]["scheduler"]["warmup"] == 1
    assert result["execute"] is False
    assert all(r["execution_stage"] == "planned" and not r["actual_run"] for r in result["rows"])
    assert result["feedback"]["events"] == []


def test_cli_explicit_cap_is_distinct_from_profile_and_shared_feedback(cli, tmp_path, monkeypatch):
    seen = {}
    original = cli.ExploratoryScheduler

    def scheduler(corpus, feedback, **kwargs):
        seen["feedback"] = feedback
        seen["warmup"] = kwargs["warmup"]
        return original(corpus, feedback, **kwargs)

    def runner(output, **kwargs):
        assert kwargs["feedback"] is seen["feedback"]
        seen.update(kwargs)
        return {"run_id": "offline-stub", "rows": []}

    monkeypatch.setattr(cli, "ExploratoryScheduler", scheduler)
    monkeypatch.setattr(cli, "run_exploratory_cycle", runner)
    doc = profile()
    doc["budget"]["warmup_inputs"] = 7
    assert cli.main(arguments(tmp_path, doc) + ["--max-inputs", "1"]) == 0
    assert seen["warmup"] == 7  # not min(max_inputs, seed_count)
    assert seen["max_inputs"] == 1
    assert callable(seen["checkpoint_hook"])
    seen.clear()
    assert cli.main(arguments(tmp_path, doc, "other")) == 0
    assert seen["max_inputs"] is None


def test_cli_resume_reads_checkpoint_not_corpus_and_preserves_old_evidence(cli, tmp_path):
    doc = profile()
    run_cycle(tmp_path / "old", doc, maximum=2)
    old = (tmp_path / "old/checkpoint.json").read_bytes()
    args = arguments(tmp_path, doc) + ["--resume-from", str(tmp_path / "old/checkpoint.json"), "--max-inputs", "4"]
    assert cli.main(args) == 0
    checkpoint = json.loads((tmp_path / "new/checkpoint.json").read_text())
    assert checkpoint["state"]["continuation"]["budget"]["selected"] == 4
    assert [e["sequence"] for e in checkpoint["state"]["scheduler"]["selections"]] == [0, 1, 2, 3]
    assert (tmp_path / "old/checkpoint.json").read_bytes() == old


@pytest.mark.parametrize("mismatch", ["source", "profile", "seed", "plain_corpus"])
def test_cli_resume_preflight_failure_never_invokes_runner(cli, tmp_path, monkeypatch, mismatch):
    doc = profile()
    run_cycle(tmp_path / "old", doc, maximum=1)
    resume = tmp_path / "old/checkpoint.json"
    extra = []
    if mismatch == "source":
        monkeypatch.setattr(cli, "source_manifest", lambda _repo: dict(SOURCE, behavior_tree_sha256="0" * 64))
    elif mismatch == "profile":
        doc["profile_id"] = "changed-profile"
    elif mismatch == "seed":
        extra = ["--seed", "999"]
    else:
        resume = tmp_path / "old/corpus-only.json"
        resume.write_text(json.dumps({"schema": "r2f.s1-corpus/v1"}))
    callback = Mock(side_effect=AssertionError("must not execute after invalid resume"))
    monkeypatch.setattr(cli, "run_exploratory_cycle", callback)
    with pytest.raises(SystemExit) as error:
        cli.main(arguments(tmp_path, doc) + ["--resume-from", str(resume)] + extra)
    assert error.value.code == 2
    callback.assert_not_called()
    assert not (tmp_path / "new").exists()


def test_cli_required_dispatch_is_not_given_exploratory_budget(cli, tmp_path, monkeypatch):
    callback = Mock(return_value={"run_id": "required-offline", "seal": {"sealed": False}})
    monkeypatch.setattr(cli, "run_campaign", callback)
    assert cli.main(["--output-dir", str(tmp_path / "required")]) == 0
    kwargs = callback.call_args.kwargs
    assert "max_inputs" not in kwargs
    assert kwargs["seed"] == 0


def test_cli_uncertain_pending_emits_audit_without_runner_callback(cli, tmp_path, monkeypatch):
    doc = profile()

    class PlannedStop(BaseException):
        pass

    def stop(hook):
        def wrapped(payload):
            hook(payload)
            raise PlannedStop()
        return wrapped

    with pytest.raises(PlannedStop):
        run_cycle(tmp_path / "old", doc, maximum=1, hook_wrapper=stop)
    checkpoint = json.loads((tmp_path / "old/checkpoint.json").read_text())
    attempt = checkpoint["state"]["continuation"]["pending_attempt"]
    Path(attempt["submission_marker_path"]).write_text('{"status":"submitted_or_uncertain"}')
    callback = Mock(side_effect=AssertionError("uncertain pending cannot execute automatically"))
    monkeypatch.setattr(cli, "run_exploratory_cycle", callback)
    with pytest.raises(SystemExit) as error:
        cli.main(arguments(tmp_path, doc) + ["--resume-from", str(tmp_path / "old")])
    assert error.value.code == 2
    callback.assert_not_called()
    audit = json.loads((tmp_path / "new/resume-refusal.json").read_text())
    assert audit["reason_code"] == "resume_pending_execution_uncertain"
