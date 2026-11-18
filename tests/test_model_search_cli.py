"""BFS CLI/profile wiring through the one existing runner, offline only."""
import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_model_search_frontier import CYCLE, ROOT, profile
from test_model_search_integration import SOURCE


@pytest.fixture
def cli(monkeypatch):
    spec = importlib.util.spec_from_file_location("model_search_cli", ROOT / "scripts/run_s1.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "source_manifest", lambda _repo: deepcopy(SOURCE))
    monkeypatch.setattr("r2f.campaign.runner.source_manifest", lambda _repo: deepcopy(SOURCE))
    return module


def args(tmp_path, output, *, maximum=5):
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(profile()))
    return ["--output-dir", str(tmp_path / output), "--exploratory-profile", str(path),
            "--cycle-id", CYCLE, "--max-inputs", str(maximum)]


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_bfs_explicit_and_profile_inference(cli, tmp_path, explicit, capsys):
    argv = args(tmp_path, "planned")
    if explicit:
        argv += ["--scheduler-policy", "bfs"]
    assert cli.main(argv) == 0
    result = json.loads((tmp_path / "planned/cycle.json").read_text())
    assert result["selection_log"]["policy"] == "bfs"
    assert result["selection_log"]["frontier"]["mode"] == "input_search_bfs"
    assert len(result["rows"]) == 5 and not result["rejected"]
    assert result["budget"]["actual"] == 0 and not result["feedback"]["events"]
    summary = result["search_summary"]
    assert json.loads(capsys.readouterr().out)["search_summary"] == summary
    assert summary["root_nodes"] == 1 and summary["derived_nodes"] > 0
    assert summary["completed"] == summary["proposal_generated"] == 5
    assert summary["actual"] == summary["executed"] == summary["submitted"] == 0
    assert summary["rejected"] == summary["tasks_in_flight"] == 0
    assert summary["tasks_pending"] + summary["completed"] == summary["tasks_total"]
    assert summary["stop_reason"] == result["stop_reason"]
    assert summary["retention_role"] == "novelty_evidence_only"
    ref = summary["frontier_ref"]
    assert ref["json_pointer"] == "/frontier"
    assert json.loads(Path(ref["path"]).read_text())["frontier"] == result["selection_log"]["frontier"]


def test_cli_resume_preserves_frontier(cli, tmp_path):
    assert cli.main(args(tmp_path, "part", maximum=5)) == 0
    assert cli.main(args(tmp_path, "resumed", maximum=7) +
                    ["--resume-from", str(tmp_path / "part/checkpoint.json")]) == 0
    data = json.loads((tmp_path / "resumed/checkpoint.json").read_text())
    assert data["state"]["scheduler"]["policy"] == "bfs"
    assert data["state"]["continuation"]["budget"]["selected"] == 7


def test_cli_bfs_without_profile_is_error_not_required_dispatch(cli, tmp_path, monkeypatch):
    campaign = Mock(side_effect=AssertionError("must not dispatch required"))
    monkeypatch.setattr(cli, "run_campaign", campaign)
    with pytest.raises(SystemExit) as exc:
        cli.main(["--scheduler-policy", "bfs", "--output-dir", str(tmp_path / "refused")])
    assert exc.value.code == 2
    campaign.assert_not_called()


@pytest.mark.parametrize("damage", ["missing", "schema", "unbounded", "unknown"])
def test_cli_refuses_invalid_search_config_before_runner(cli, tmp_path, monkeypatch, damage):
    doc = profile()
    if damage == "missing":
        del doc["search"]
    elif damage == "schema":
        doc["search"]["schema"] = "future"
    elif damage == "unbounded":
        doc["search"]["max_depth"] = None
    else:
        doc["search"]["extra"] = True
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(doc))
    cycle = Mock(side_effect=AssertionError("bad search must not reach runner"))
    monkeypatch.setattr(cli, "run_exploratory_cycle", cycle)
    with pytest.raises(SystemExit) as exc:
        cli.main(["--scheduler-policy", "bfs", "--exploratory-profile", str(path),
                  "--cycle-id", CYCLE, "--output-dir", str(tmp_path / "refused")])
    assert exc.value.code == 2
    cycle.assert_not_called()
