"""Scheduler policy CLI contracts through the existing unified offline cycle."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_exploratory_checkpoint import CYCLE, REPO, SOURCE, profile
from test_scheduler_policy_runner import input_projection


@pytest.fixture
def cli(monkeypatch):
    spec = importlib.util.spec_from_file_location("scheduler_policy_run_s1_cli", REPO / "scripts/run_s1.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "source_manifest", lambda _repo: deepcopy(SOURCE))
    monkeypatch.setattr("r2f.campaign.runner.source_manifest", lambda _repo: deepcopy(SOURCE))
    return module


def args_for(tmp_path, output, *, maximum=3):
    doc = profile()
    doc["budget"] = {"max_inputs": 4, "warmup_inputs": 1}
    # Keep the strategy/resume zero-boundary setup; profile() now supplies
    # an explicit changing M03 route rather than the M01 identity witness.
    for spec in doc["seed_specs"] + doc.get("declared_seed_specs_union", []):
        if spec["seed_id"] == "P01":
            spec["boundary_class"] = "zero"
    for cycle in doc["cycles"]:
        for spec in cycle["seed_specs"]:
            if spec["seed_id"] == "P01":
                spec["boundary_class"] = "zero"
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(doc))
    return ["--output-dir", str(tmp_path / output), "--exploratory-profile", str(path),
            "--cycle-id", CYCLE, "--max-inputs", str(maximum)]


@pytest.mark.parametrize("policy", [None, "fair", "feedback"])
def test_cli_new_policy_reaches_unified_plan_and_checkpoint(cli, tmp_path, policy):
    args = args_for(tmp_path, "new")
    if policy is not None:
        args += ["--scheduler-policy", policy]
    assert cli.main(args) == 0
    result = json.loads((tmp_path / "new/cycle.json").read_text())
    state = json.loads((tmp_path / "new/checkpoint.json").read_text())["state"]["scheduler"]
    assert result["selection_log"] == state
    assert len(result["rows"]) == 3 and not result["rejected"]
    assert all(row["execution_stage"] == "planned" and not row["actual_run"] for row in result["rows"])
    assert result["feedback"]["events"] == []
    if policy is None:
        assert "policy" not in state
        assert all("policy" not in e and "decision_sha256" not in e for e in state["selections"])
    else:
        assert state["policy"] == policy
        assert all(e["policy"] == policy for e in state["selections"])
        assert all(("decision_sha256" in e) == (policy == "fair") for e in state["selections"])


@pytest.mark.parametrize("policy", ["fair", "feedback"])
@pytest.mark.parametrize("execute", [False, True])
def test_cli_required_explicit_policy_is_error_before_dispatch(cli, tmp_path, monkeypatch, policy, execute):
    campaign = Mock(side_effect=AssertionError("required policy must fail before runner"))
    cycle = Mock(side_effect=AssertionError("no exploratory profile"))
    monkeypatch.setattr(cli, "run_campaign", campaign)
    monkeypatch.setattr(cli, "run_exploratory_cycle", cycle)
    args = ["--output-dir", str(tmp_path / "required"), "--scheduler-policy", policy]
    if execute:
        args.append("--execute")
    with pytest.raises(SystemExit) as exc:
        cli.main(args)
    assert exc.value.code == 2
    campaign.assert_not_called()
    cycle.assert_not_called()
    assert not (tmp_path / "required").exists()


@pytest.mark.parametrize("stored", [None, "fair", "feedback"])
@pytest.mark.parametrize("explicit_same", [False, True])
def test_cli_resume_inherits_policy_or_accepts_same_without_rewriting_checkpoint(
        cli, tmp_path, stored, explicit_same):
    whole = args_for(tmp_path, "whole", maximum=4)
    first = args_for(tmp_path, "first", maximum=2)
    if stored is not None:
        whole += ["--scheduler-policy", stored]
        first += ["--scheduler-policy", stored]
    assert cli.main(whole) == 0
    assert cli.main(first) == 0
    old = {str(p.relative_to(tmp_path / "first")): p.read_bytes()
           for p in (tmp_path / "first").rglob("*") if p.is_file()}
    args = args_for(tmp_path, "resumed", maximum=4) + ["--resume-from", str(tmp_path / "first")]
    if explicit_same:
        args += ["--scheduler-policy", stored or "feedback"]
    assert cli.main(args) == 0
    one = json.loads((tmp_path / "whole/cycle.json").read_text())
    part = json.loads((tmp_path / "first/cycle.json").read_text())
    rest = json.loads((tmp_path / "resumed/cycle.json").read_text())
    assert input_projection(one) == input_projection(part) + input_projection(rest)
    assert one["selection_log"] == rest["selection_log"]
    assert rest["selection_log"].get("policy", "feedback") == (stored or "feedback")
    assert old == {str(p.relative_to(tmp_path / "first")): p.read_bytes()
                   for p in (tmp_path / "first").rglob("*") if p.is_file()}


@pytest.mark.parametrize("stored,requested", [("fair", "feedback"), ("feedback", "fair"), (None, "fair")])
@pytest.mark.parametrize("execute", [False, True])
def test_cli_resume_policy_mismatch_fails_before_runtime_or_checkpoint_write(
        cli, tmp_path, monkeypatch, stored, requested, execute, capsys):
    first = args_for(tmp_path, "first", maximum=1)
    if stored is not None:
        first += ["--scheduler-policy", stored]
    assert cli.main(first) == 0
    old = {str(p.relative_to(tmp_path / "first")): p.read_bytes()
           for p in (tmp_path / "first").rglob("*") if p.is_file()}
    runner = Mock(side_effect=AssertionError("policy mismatch must not invoke runner"))
    boundary = Mock(side_effect=AssertionError("policy mismatch must not resolve live runtime"))
    monkeypatch.setattr(cli, "run_exploratory_cycle", runner)
    monkeypatch.setattr(cli, "runtime_for_boundary", boundary)
    args = args_for(tmp_path, "new") + ["--resume-from", str(tmp_path / "first"),
                                       "--scheduler-policy", requested]
    if execute:
        args.append("--execute")
    with pytest.raises(SystemExit) as exc:
        cli.main(args)
    assert exc.value.code == 2
    assert "scheduler-policy" in capsys.readouterr().err
    runner.assert_not_called()
    boundary.assert_not_called()
    assert not (tmp_path / "new").exists()
    assert old == {str(p.relative_to(tmp_path / "first")): p.read_bytes()
                   for p in (tmp_path / "first").rglob("*") if p.is_file()}


def test_cli_rejects_noncanonical_policy(cli, tmp_path):
    # bfs is now a declared opt-in policy; keep testing a truly unknown value.
    with pytest.raises(SystemExit) as exc:
        cli.main(args_for(tmp_path, "new") + ["--scheduler-policy", "bfs-invalid"])
    assert exc.value.code == 2
    assert not (tmp_path / "new").exists()


def test_cli_bfs_without_search_settings_is_error_before_output(cli, tmp_path):
    with pytest.raises(SystemExit) as exc:
        cli.main(args_for(tmp_path, "new") + ["--scheduler-policy", "bfs"])
    assert exc.value.code == 2
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize("policy", [None, "fair", "feedback"])
@pytest.mark.parametrize("execute", [False, True])
def test_cli_core_noop_rejected_with_plan_audit_before_prepare_or_runtime(
        cli, tmp_path, monkeypatch, policy, execute):
    from r2f.campaign.corpus import corpus_from_profile
    from r2f.modules.s1.providers import S1ExploratoryMutationProvider

    doc = profile()
    # One explicit P01 finite baseline equal to the M01 representative.
    spec = {"seed_id": "P01", "target_id": "discovery.late_endpoint.fuzz_v1",
            "boundary_class": "finite", "executable": True,
            "mutation_routes": [{"surface_id": "P01", "boundary_class": "finite",
                                 "mutator_id": "M01"}],
            "overrides": {"logical_config": {"reader_delay_sec": 0.01}}}
    doc["seed_specs"] = [deepcopy(spec)]
    doc["declared_seed_specs_union"] = [deepcopy(spec)]
    doc["cycles"] = [dict(next(c for c in doc["cycles"] if c["cycle_id"] == CYCLE),
                          seed_specs=[deepcopy(spec)])]
    doc["budget"] = {"max_inputs": 3, "warmup_inputs": 1}
    # Validate this is a declared applicable baseline, not a malformed fixture.
    assert len(corpus_from_profile(doc, cycle_id=CYCLE).entries) > 0
    path = tmp_path / "noop-profile.json"
    path.write_text(json.dumps(doc))
    runtime = Mock(side_effect=AssertionError("no-op must not reach runtime"))
    prepare = Mock(side_effect=AssertionError("no-op must not be prepared"))
    monkeypatch.setattr(cli, "runtime_for_boundary", lambda _boundary: runtime)
    monkeypatch.setattr(S1ExploratoryMutationProvider, "prepare_plan", prepare)
    argv = ["--output-dir", str(tmp_path / "noop"), "--exploratory-profile", str(path),
            "--cycle-id", CYCLE, "--max-inputs", "3"]
    if policy is not None:
        argv += ["--scheduler-policy", policy]
    if execute:
        argv += ["--execute"]
    assert cli.main(argv) == (1 if execute else 0)
    data = json.loads((tmp_path / "noop/cycle.json").read_text())
    assert not data["abort"] and len(data["rows"]) == len(data["rejected"]) == 3
    assert data["budget"]["planned"] == 3
    assert data["budget"]["generated"] == data["budget"]["unique_inputs"] == 0
    assert data["budget"]["attempted"] == data["budget"]["actual"] == data["budget"]["executed"] == 0
    assert not data["feedback"]["events"]
    for row in data["rows"]:
        assert row["generation_status"] == "rejected" and row["reason_code"] == "no_effective_input"
        assert row["mutation_plan"] and row["actual_run"] is False
        case = tmp_path / "noop/cases" / row["cell_id"]
        assert json.loads((case / "mutation_plan.json").read_text()) == row["mutation_plan"]
        assert not (case / "capsule.json").exists()
        assert not (case / "submission_intent.json").exists()
    runtime.assert_not_called()
    prepare.assert_not_called()
