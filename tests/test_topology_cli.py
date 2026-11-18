"""Existing CLI entrypoints consume the real opted-in topology Corpus."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign import replay, runner
from test_topology_cycle import REPO, SOURCE, CYCLE, offline_runtime, profile


def script(name):
    spec = importlib.util.spec_from_file_location('topology_cli_' + name, REPO / 'scripts' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_existing_run_cli_plans_and_resumes_topology_without_runtime(tmp_path, capsys):
    cli = script('run_s1')
    path = tmp_path / 'profile.json'; path.write_text(json.dumps(profile(4)))
    first = tmp_path / 'first'; second = tmp_path / 'second'
    args = ['--exploratory-profile', str(path), '--cycle-id', CYCLE, '--domain-id', '80']
    with patch.object(cli, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(runner, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(cli, 'runtime_for_boundary', side_effect=AssertionError('unrequested runtime')):
        assert cli.main(args + ['--output-dir', str(first), '--max-inputs', '1', '--seed', '17']) == 0
        assert cli.main(args + ['--output-dir', str(second), '--max-inputs', '2', '--resume-from', str(first)]) == 0
    initial = json.loads((first / 'cycle.json').read_text())
    resumed = json.loads((second / 'cycle.json').read_text())
    assert initial['rows'][0]['chain_depth'] == 4
    assert resumed['budget']['selected'] == 2 and resumed['budget']['attempted'] == 0
    assert all('topology_program' in r['final_input'] for r in initial['rows'] + resumed['rows'])
    assert json.loads((second / 'checkpoint.json').read_text())


def test_existing_cli_injected_fixture_execute_and_replay_no_real_claim(tmp_path, capsys):
    cli, replay_cli = script('run_s1'), script('replay_s1')
    path = tmp_path / 'profile.json'; path.write_text(json.dumps(profile(1)))
    out = tmp_path / 'execute'; captured = []
    runtime = offline_runtime(captured)
    with patch.object(cli, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(runner, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(cli, 'runtime_for_boundary', return_value=runtime):
        code = cli.main(['--exploratory-profile', str(path), '--cycle-id', CYCLE,
                         '--output-dir', str(out), '--execute', '--max-inputs', '1', '--domain-id', '80'])
    assert code == 1  # no actual ROS run: do not turn a fixture into CLI success
    runtime.assert_called_once()
    result = json.loads((out / 'cycle.json').read_text()); row = result['rows'][0]
    assert result['runtime_formal'] is False and row['candidate_eligible'] is False
    rerun = offline_runtime([])
    # Keep exact entrypoint parsing and replay pipeline. Only route selection
    # supplies an injected offline callable; source observations are explicit mocks.
    original = replay.replay_cell
    def invoke(capsule, **kw):
        return original(capsule, **kw, runtime=rerun)
    with patch.object(replay_cli, 'replay_cell', side_effect=invoke), \
         patch.object(replay, 'source_manifest', return_value=deepcopy(SOURCE)):
        assert replay_cli.main([row['replay_path'], '--output', str(tmp_path / 'replay'), '--domain', '81']) == 2
        report_path = tmp_path / 'replay/replay_report.json'
        if not report_path.exists():
            report_path = next((tmp_path / 'replay').glob('*report.json'))
        assert replay_cli.main(['--verify', str(report_path)]) == 0
    rerun.assert_called_once()


def test_cli_does_not_add_topology_to_required_target_choices(tmp_path):
    cli = script('run_s1')
    with pytest.raises(SystemExit) as exc:
        cli.main(['--output-dir', str(tmp_path / 'unused'), '--target', 'discovery.endpoint_topology.fuzz_v1'])
    assert exc.value.code == 2
    assert not (tmp_path / 'unused').exists()
