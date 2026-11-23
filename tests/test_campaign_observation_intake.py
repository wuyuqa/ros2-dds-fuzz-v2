"""CP1 public intake contracts; every carrier and source is an offline fixture."""
import json
from pathlib import Path

import pytest

from r2f.campaign.runner import plan_campaign

SELECTION = {'observation_mode': 'instrumentation', 'observe_adopted_config': True}
CELL = 'rmw_cyclonedds_cpp.baseline.0'


def test_observation_plan_preserves_input_identity_and_scopes_runtime_config(tmp_path):
    plain = plan_campaign(tmp_path / 'plain', run_id='offline-cp1', seed=41)
    selected = plan_campaign(tmp_path / 'selected', run_id='offline-cp1', seed=41,
                             observation=SELECTION)
    declaration = dict(SELECTION, cell_id=CELL)
    assert selected['observation'] == declaration
    assert json.loads(Path(selected['manifest_path']).read_text())['observation'] == declaration
    assert len(selected['rows']) == 24
    assert selected['required_cells'] == 18
    assert selected['full_scope_required_cells'] == 72
    for old, new in zip(plain['rows'], selected['rows']):
        assert Path(old['replay_path']).read_bytes() == Path(new['replay_path']).read_bytes()
        assert old['effective_input_sha256'] == new['effective_input_sha256']
        config = json.loads(Path(new['runtime_config_path']).read_text())
        if new['cell_id'] == CELL:
            assert {key: config[key] for key in SELECTION} == SELECTION
        else:
            assert not set(SELECTION).intersection(config)
        assert new['execution_stage'] == 'planned'
        assert new['actual_run'] is False
        assert 'runtime_execution_metadata' not in new


def test_observation_execution_rejects_broader_budget_before_outputs(tmp_path):
    from r2f.campaign.runner import run_campaign

    calls = []
    output = tmp_path / 'rejected'
    with pytest.raises(ValueError, match='observation.*max_executions=1'):
        run_campaign(output, run_id='offline-cp1-reject', execute=True,
                     runtime=lambda config, directory: calls.append(config),
                     observation=SELECTION, max_executions=2)
    assert calls == []
    assert not output.exists()


def _cli():
    import importlib.util
    path = Path(__file__).resolve().parents[1] / 'scripts' / 'run_s1.py'
    spec = importlib.util.spec_from_file_location('offline_cp1_run_s1', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main


def test_required_cli_carries_observation_and_existing_budget(tmp_path, capsys):
    path = tmp_path / 'selection.json'
    path.write_text(json.dumps(SELECTION))
    output = tmp_path / 'campaign'
    assert _cli()(['--output-dir', str(output), '--run-id', 'offline-cp1-cli',
                   '--observation-json', str(path), '--max-executions', '1']) == 0
    report = json.loads((output / 'report.json').read_text())
    assert report['observation'] == dict(SELECTION, cell_id=CELL)
    assert report['execution_budget'] == {
        'max_executions': 1, 'scope': 'original_carrier_dispatch_attempts',
        'attempts': 0, 'stop_reason': 'planning_only'}
    assert all(row['actual_run'] is False for row in report['rows'])
    assert json.loads(capsys.readouterr().out)['seal']['sealed'] is False


def test_cli_rejects_duplicate_observation_keys_before_outputs(tmp_path):

    path = tmp_path / 'duplicate.json'
    path.write_text('{"observation_mode":"blackbox", "observation_mode":"instrumentation",'
                    '"observe_adopted_config":true}')
    output = tmp_path / 'rejected'
    with pytest.raises(SystemExit) as error:
        _cli()(['--output-dir', str(output), '--observation-json', str(path)])
    assert error.value.code == 2
    assert not output.exists()


def test_semantic_parameter_injection_rejected_before_outputs(tmp_path):

    for key, value in SELECTION.items():
        output = tmp_path / key
        with pytest.raises(ValueError, match='unknown semantic parameter'):
            plan_campaign(output, parameters={key: value})
        assert not output.exists()


@pytest.mark.parametrize('selection', [
    None, True, False, [], '', 0, {},
    {'observation_mode': 'instrumentation'},
    {'observe_adopted_config': True},
    dict(SELECTION, unexpected=True),
    dict(SELECTION, instrumentation_roots=['/tmp/offline']),
    dict(SELECTION, observation_mode=None),
    dict(SELECTION, observation_mode=True),
    dict(SELECTION, observation_mode=[]),
    dict(SELECTION, observation_mode='INSTRUMENTATION'),
    dict(SELECTION, observe_adopted_config=None),
    dict(SELECTION, observe_adopted_config=0),
    dict(SELECTION, observe_adopted_config=1),
    dict(SELECTION, observe_adopted_config='true'),
    dict(SELECTION, observe_adopted_config={}),
    dict(SELECTION, observation_mode='blackbox'),
])
def test_malformed_observation_rejected_without_outputs(tmp_path, selection):
    from r2f.campaign.runner import run_campaign

    for operation in (plan_campaign, run_campaign):
        output = tmp_path / operation.__name__
        with pytest.raises(ValueError, match='observation|observe_adopted_config'):
            operation(output, observation=selection)
        assert not output.exists()


@pytest.mark.parametrize('target', ['discovery.late_join_delivery',
                                  'discovery.interruption_recovery',
                                  'builtin.discovery_projection_reconciliation'])
def test_other_targets_rejected_before_outputs(tmp_path, target):
    with pytest.raises(ValueError, match='observation CP1'):
        plan_campaign(tmp_path / 'rejected', target_id=target, observation=SELECTION)
    assert not (tmp_path / 'rejected').exists()


@pytest.mark.parametrize('mode,adopted', [('blackbox', False), ('instrumentation', False),
                                       ('instrumentation', True)])
def test_supported_modes_reach_existing_runtime_prepare(tmp_path, mode, adopted):
    from r2f.runtime.instrumentation_bridge import prepare
    from r2f.runtime.ros2_pair import normalize_config

    selection = {'observation_mode': mode, 'observe_adopted_config': adopted}
    report = plan_campaign(tmp_path / 'plan', run_id='offline-cp1-prepare', observation=selection)
    config = normalize_config(json.loads(Path(report['rows'][0]['runtime_config_path']).read_text()))
    context = prepare(config, tmp_path / 'runtime-fixture', {})
    prepared = json.loads(Path(context.prepared['raw_path']).read_text())
    assert prepared['observation_mode'] == mode
    assert prepared['observe_adopted_config'] is adopted
    assert prepared['execution_metadata_only'] is True
    assert set(context.trace_configs) == ({'reader', 'writer'} if mode == 'instrumentation' else set())
    assert 'observation_mode' not in prepared['execution_input']['final_input']
    assert 'observe_adopted_config' not in prepared['execution_input']['final_input']


@pytest.mark.parametrize('budget', [None, True, 0, 2, 24])
def test_explicit_observation_never_dispatches_outside_cp1(tmp_path, budget):
    from r2f.campaign.runner import run_campaign

    calls = []
    output = tmp_path / 'rejected'
    with pytest.raises(ValueError, match='observation.*max_executions=1'):
        run_campaign(output, execute=True, observation=SELECTION, max_executions=budget,
                     runtime=lambda config, directory: calls.append(config))
    assert calls == []
    assert not output.exists()


def test_single_budget_dispatch_receives_selection_without_semantic_leak(tmp_path):
    from r2f.campaign.runner import run_campaign

    configs = []

    def offline_failure(config, directory):
        configs.append(config)
        raise RuntimeError('offline-cp1-deliberate-fixture-failure')

    report = run_campaign(tmp_path / 'campaign', run_id='offline-cp1-dispatch',
                          execute=True, runtime=offline_failure,
                          max_executions=1, observation=SELECTION)
    assert len(configs) == 1
    config = configs[0]
    assert {key: config[key] for key in SELECTION} == SELECTION
    assert (config['rmw'], config['arm'], config['repeat']) == ('rmw_cyclonedds_cpp', 'baseline', 0)
    assert not set(SELECTION).intersection(config['final_input'])
    assert report['execution_budget']['attempts'] == 1
    assert report['execution_budget']['stop_reason'] == 'max_executions_reached'
    assert len(report['rows']) == 24
    assert all(row['execution_stage'] == 'planned' for row in report['rows'][1:])
    assert all('runtime_execution_metadata' not in row for row in report['rows'][1:])
    assert report['seal']['sealed'] is False


@pytest.mark.parametrize('contents', ['null', '[]', '{', '{"unknown":true}',
                                      '{"observation_mode":"instrumentation","observe_adopted_config":1}'])
def test_cli_invalid_observation_rejected_before_outputs(tmp_path, contents, capsys):
    path = tmp_path / 'invalid.json'
    path.write_text(contents)
    output = tmp_path / 'rejected'
    with pytest.raises(SystemExit) as error:
        _cli()(['--output-dir', str(output), '--observation-json', str(path)])
    assert error.value.code == 2
    assert not output.exists()


def test_exploratory_branch_rejects_observation_before_reading_profile(tmp_path, capsys):
    output = tmp_path / 'rejected'
    with pytest.raises(SystemExit) as error:
        _cli()(['--output-dir', str(output), '--observation-json', str(tmp_path / 'unused'),
                '--exploratory-profile', str(tmp_path / 'unused-profile'), '--cycle-id', 'offline'])
    assert error.value.code == 2
    assert not output.exists()


def test_cli_budget_without_observation_preserves_issue16_lease_flag(tmp_path, capsys):
    path = tmp_path / 'parameters.json'
    path.write_text('{"observe_participant_leases":true}')
    output = tmp_path / 'campaign'
    assert _cli()(['--output-dir', str(output), '--run-id', 'offline-cp1-lease-cli',
                   '--target', 'discovery.interruption_recovery',
                   '--parameters-json', str(path), '--max-executions', '1']) == 0
    report = json.loads((output / 'report.json').read_text())
    assert 'observation' not in report
    assert report['execution_budget']['max_executions'] == 1
    assert report['execution_budget']['attempts'] == 0
    for row in report['rows']:
        config = json.loads(Path(row['runtime_config_path']).read_text())
        capsule = json.loads(Path(row['replay_path']).read_text())
        assert config['observe_participant_leases'] is True
        assert capsule['input']['effective_input']['final_input']['observe_participant_leases'] is True
        assert not set(SELECTION).intersection(config)


@pytest.fixture
def offline_trace_runtime():
    """Public runtime double: empty trace files, no ROS/DDS or qualification."""
    from r2f.instrumentation.cyclonedds import build_trace_config, collect_trace

    captured = {}

    def runtime(config, directory):
        directory = Path(directory)
        trace_path = directory / 'offline.trace.log'
        trace_config = build_trace_config(
            trace_path, domain_id=config['domain_id'],
            observe_adopted_config=config['observe_adopted_config'])
        trace_path.write_text('')
        result = {
            'runtime_status': 'blocked', 'real_execution': False,
            'reason': 'offline_fixture_no_ROS_DDS_execution',
            'observation': {'offline_fixture': True, 'traces': [collect_trace(
                trace_config, processes=[], control_task='offline_fixture')]},
        }
        path = directory / 'runtime_result.json'
        path.write_text(json.dumps(result))
        captured.update(returned=result, path=path, original_bytes=path.read_bytes())
        return result

    return runtime, captured


@pytest.mark.parametrize('adopted', [False, True])
@pytest.mark.parametrize('different_evidence', [False, True])
def test_offline_trace_runtime_persistence_contract(
        tmp_path, offline_trace_runtime, adopted, different_evidence):
    from r2f.campaign.runner import run_campaign

    runtime, captured = offline_trace_runtime

    def dispatch(config, directory):
        result = runtime(config, directory)
        if different_evidence:
            result['reason'] = 'different_offline_evidence_after_publication'
        return result

    report = run_campaign(
        tmp_path / 'campaign', run_id='offline-trace-persistence', execute=True,
        runtime=dispatch, max_executions=1,
        observation={'observation_mode': 'instrumentation', 'observe_adopted_config': adopted})
    row = report['rows'][0]
    persisted = json.loads(captured['path'].read_text())
    assert captured['path'].read_bytes() == captured['original_bytes']
    assert json.loads((tmp_path / 'campaign' / 'report.json').read_text())['rows'][0] == row
    assert report['full_scope_required_cells'] == 72
    assert report['formal_runtime'] is False
    assert report['seal']['sealed'] is False
    assert all(item['actual_run'] is False for item in report['rows'])
    ledger = json.loads((tmp_path / 'campaign' / 'candidate_ledger.json').read_text())
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0
    if different_evidence:
        assert row['abort_reason'] == 'runner_wrapper_failure'
        assert row['runner_exception']['detail'] == 'runtime_result_differs_from_returned_evidence'
        assert persisted['reason'] == 'offline_fixture_no_ROS_DDS_execution'
    else:
        assert 'runner_exception' not in row
        assert persisted == captured['returned']
        categories = captured['returned']['observation']['traces'][0]['config']['categories']
        assert categories == (['discovery', 'plist', 'config'] if adopted else ['discovery', 'plist'])
