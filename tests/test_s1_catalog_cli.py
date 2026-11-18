"""Read-only catalog listing must bypass all campaign/source/runtime activity."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest

from r2f.modules.s1.catalog import target_catalog

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run_s1.py"


@pytest.fixture
def cli():
    spec = importlib.util.spec_from_file_location("s1_catalog_run_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("extra", [[], ["--execute", "--runtime-boundary", "isolated_netns"]])
def test_listing_without_output_directory_never_checks_source_or_runs(cli, monkeypatch, capsys, extra):
    forbidden = Mock(side_effect=AssertionError("list-targets must not enter execution/planning/source"))
    for name in ("run_campaign", "run_exploratory_cycle", "runtime_for_boundary", "source_manifest",
                 "corpus_from_profile", "FeedbackLog", "ExploratoryScheduler", "load_checkpoint",
                 "make_checkpoint_hook"):
        monkeypatch.setattr(cli, name, forbidden)
    monkeypatch.setattr("r2f.core.Registry.register", forbidden)
    assert cli.main(["--list-targets"] + extra) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == "r2f.s1-target-catalog/v1"
    assert result["declaration_only"] is True
    assert result["targets"] == list(target_catalog())
    assert not {"execution_stage", "mutation_receipt", "seal"} & result.keys()
    forbidden.assert_not_called()


def test_listing_consumes_common_catalog_and_does_not_read_inputs_or_create_output(cli, monkeypatch, tmp_path, capsys):
    entries = list(target_catalog())
    spy = Mock(return_value=entries)
    monkeypatch.setattr(cli, "target_catalog", spy)
    forbidden = Mock(side_effect=AssertionError("read-only listing must not read inputs/create artifacts"))
    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)
    assert cli.main([
        "--list-targets", "--output-dir", str(tmp_path / "no-output"), "--execute",
        "--parameters-json", str(tmp_path / "missing-parameters"),
        "--exploratory-profile", str(tmp_path / "missing-profile"),
        "--resume-from", str(tmp_path / "missing-checkpoint"),
    ]) == 0
    assert json.loads(capsys.readouterr().out)["targets"] == entries
    spy.assert_called_once_with()
    forbidden.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_cold_cli_lists_without_ros_subprocesses_sockets_or_output(tmp_path):
    guard = r'''
import importlib.abc, runpy, sys
from unittest.mock import patch
class NoROS(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'rclpy', 'ros2cli', 'std_msgs', 'rmw_fastrtps_cpp', 'rmw_cyclonedds_cpp'}:
            raise AssertionError('ROS import: ' + fullname)
sys.meta_path.insert(0, NoROS())
sys.argv = [sys.argv[1], '--list-targets']
with patch('subprocess.Popen', side_effect=AssertionError('subprocess')), \
     patch('socket.socket', side_effect=AssertionError('socket')):
    runpy.run_path(sys.argv[0], run_name='__main__')
'''
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("ROS_", "RMW_", "AMENT_", "COLCON_"))}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run([sys.executable, "-B", "-c", guard, str(SCRIPT)], cwd=tmp_path,
                            env=env, text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert json.loads(result.stdout)["targets"] == list(target_catalog())
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("args", [[], ["--execute"], ["--parameters-json", "missing"]])
def test_ordinary_commands_still_require_output_directory_before_io(cli, monkeypatch, capsys, args):
    forbidden = Mock(side_effect=AssertionError("missing output must fail before execution/source/input IO"))
    monkeypatch.setattr(cli, "run_campaign", forbidden)
    monkeypatch.setattr(cli, "source_manifest", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2
    assert "the following arguments are required: --output-dir" in capsys.readouterr().err
    forbidden.assert_not_called()


def test_ordinary_required_cli_forwarding_is_unchanged(cli, monkeypatch, tmp_path, capsys):
    run = Mock(return_value={"run_id": "catalog-forwarding", "seal": {"sealed": False}})
    monkeypatch.setattr(cli, "run_campaign", run)
    output = tmp_path / "not-created-by-mock"
    assert cli.main(["--output-dir", str(output)]) == 0
    assert run.call_args.kwargs == {
        "output_dir": output, "execute": False, "domain_id": 173, "run_id": None,
        "seed": 0, "target_id": "discovery.late_endpoint", "runtime_boundary": "host",
    }
    assert json.loads(capsys.readouterr().out)["run_id"] == "catalog-forwarding"
    assert not output.exists()


@pytest.mark.parametrize("target", ["discovery.late_endpoint.fuzz_v1",
                                    "discovery.interruption_recovery.fuzz_windows_v1",
                                    "discovery.endpoint_topology.fuzz_v1"])
def test_listing_does_not_expand_ordinary_required_target_choices(cli, tmp_path, target):
    with pytest.raises(SystemExit) as error:
        cli.main(["--output-dir", str(tmp_path / "unused"), "--target", target])
    assert error.value.code == 2
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("sealed", [False, True])
def test_ordinary_explicit_required_arguments_and_exit_status_are_unchanged(cli, monkeypatch, tmp_path, capsys, sealed):
    run = Mock(return_value={"run_id": "catalog-explicit", "seal": {"sealed": sealed}})
    monkeypatch.setattr(cli, "run_campaign", run)
    output = tmp_path / "not-created-by-mock"
    parameters = tmp_path / "parameters.json"
    parameters.write_text(json.dumps({"reader_delay_sec": 0.25}))
    assert cli.main([
        "--output-dir", str(output), "--execute", "--domain-id", "81", "--run-id", "catalog-explicit",
        "--seed", "17", "--runtime-boundary", "isolated_netns",
        "--target", "discovery.late_join_delivery", "--parameters-json", str(parameters),
    ]) == (0 if sealed else 1)
    assert run.call_args.kwargs == {
        "output_dir": output, "execute": True, "domain_id": 81, "run_id": "catalog-explicit",
        "seed": 17, "target_id": "discovery.late_join_delivery", "runtime_boundary": "isolated_netns",
        "parameters": {"reader_delay_sec": 0.25},
    }
    assert json.loads(capsys.readouterr().out)["seal"] == {"sealed": sealed}
    assert not output.exists()
