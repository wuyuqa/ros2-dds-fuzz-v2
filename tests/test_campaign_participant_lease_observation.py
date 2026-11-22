"""Canonical supporting observations; all carriers here are offline fixtures."""
import json
from pathlib import Path

import pytest

from r2f.campaign.identity import runtime_config, semantic_input
from r2f.campaign.runner import run_campaign
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.modules.s1.targets import EXECUTION_ORDER

TARGET = 'discovery.interruption_recovery'
FLAG = 'observe_participant_leases'


@pytest.mark.parametrize('enabled', [True, False])
def test_explicit_supporting_flag_survives_public_input_and_runtime(enabled):
    submitted = semantic_input('baseline', {FLAG: enabled}, target_id=TARGET)
    assert submitted[FLAG] is enabled
    config = runtime_config(submitted, target_id=TARGET, arm='baseline',
                            rmw='rmw_cyclonedds_cpp', run_id='offline-lease', domain_id=173)
    assert config[FLAG] is enabled
    assert canonical_hash(submitted) != canonical_hash(
        semantic_input('baseline', target_id=TARGET))


@pytest.mark.parametrize('malformed', [None, 0, 1, 'true', 'false', [], {}])
def test_supporting_flag_requires_strict_bool(malformed):
    with pytest.raises(ValueError, match='observe_participant_leases'):
        semantic_input('baseline', {FLAG: malformed}, target_id=TARGET)


@pytest.mark.parametrize('target', [t for t in EXECUTION_ORDER if t != TARGET])
def test_other_required_targets_reject_supporting_flag(target):
    with pytest.raises(ValueError, match='observe_participant_leases'):
        semantic_input('baseline', {FLAG: True}, target_id=target)


def test_one_original_attempt_keeps_full_plan_and_supporting_identity(tmp_path):
    calls = []

    def blocked_fixture(config, output_dir):
        calls.append(config)
        raw = Path(output_dir) / 'raw.jsonl'
        raw.write_text('{"event":"offline_fixture_blocked"}\n')
        return {'runtime_status': 'blocked', 'abort_reason': 'offline_fixture_blocked',
                'raw_path': str(raw), 'raw_sha256': raw_sha256(raw), 'events': []}

    report = run_campaign(tmp_path, target_id=TARGET, run_id='offline-budget',
                          execute=True, runtime=blocked_fixture, auto_replay=None,
                          max_executions=1, parameters={FLAG: True})
    assert len(calls) == 1
    assert (calls[0]['rmw'], calls[0]['arm'], calls[0]['repeat']) == (
        'rmw_cyclonedds_cpp', 'baseline', 0)
    assert calls[0][FLAG] is True
    assert calls[0]['final_input'][FLAG] is True
    assert len(report['rows']) == 24
    assert sum(row['execution_stage'] == 'planned' for row in report['rows']) == 23
    assert report['execution_budget'] == {
        'max_executions': 1, 'scope': 'original_carrier_dispatch_attempts',
        'attempts': 1, 'stop_reason': 'max_executions_reached'}
    for row in report['rows']:
        capsule = json.loads(Path(row['replay_path']).read_text())
        assert capsule['input']['effective_input']['final_input'] == semantic_input(
            row['arm'], {FLAG: True}, target_id=TARGET)
        assert capsule['input']['effective_input']['final_input'][FLAG] is True
        assert capsule['input']['effective_input_sha256'] == row['effective_input_sha256']
        config = json.loads(Path(row['runtime_config_path']).read_text())
        assert config[FLAG] is True
    assert report['required_cells'] == 18
    assert report['independent_fault_controls'] == 6
    assert report['full_scope_required_cells'] == 72
    assert report['maturity_denominator_changed'] is False
    assert not report['seal']['sealed']
    assert not report['formal_runtime']
    assert report['rows'][0]['raw_sha256'] == raw_sha256(report['rows'][0]['raw_path'])
    assert not any('raw_sha256 must' in error or 'report_hash_mismatch' in error
                   or 'matrix_missing' in error for error in report['seal']['errors'])
    ledger = json.loads(Path(report['candidate_ledger_path']).read_text())
    assert ledger['natural_candidate_count'] == 0
    assert Path(report['rows'][0]['runtime_result_path']).is_file()
    assert (Path(report['rows'][0]['runtime_result_path']).parent / 'raw.jsonl').read_text() == (
        '{"event":"offline_fixture_blocked"}\n')


@pytest.mark.parametrize('budget', [True, False, 0, -1, 25, 1.0, '1', [], {}])
def test_invalid_budget_is_rejected_before_output_or_dispatch(tmp_path, budget):
    output = tmp_path / 'must-not-exist'
    calls = []
    with pytest.raises(ValueError, match='max_executions'):
        run_campaign(output, target_id=TARGET, execute=True,
                     runtime=lambda *args: calls.append(args), max_executions=budget)
    assert not calls
    assert not output.exists()


@pytest.mark.parametrize('target,flag', [(TARGET, None), (TARGET, 1),
                                        (EXECUTION_ORDER[0], True),
                                        (EXECUTION_ORDER[1], False),
                                        (EXECUTION_ORDER[3], True)])
def test_invalid_flag_rejected_before_output_or_dispatch(tmp_path, target, flag):
    output = tmp_path / 'must-not-exist'
    calls = []
    with pytest.raises(ValueError, match=FLAG):
        run_campaign(output, target_id=target, parameters={FLAG: flag}, execute=True,
                     runtime=lambda *args: calls.append(args))
    assert not calls
    assert not output.exists()


def test_original_budget_rejects_automatic_replay_before_side_effects(tmp_path):
    output = tmp_path / 'must-not-exist'
    with pytest.raises(ValueError, match='auto_replay must be None'):
        run_campaign(output, max_executions=1, auto_replay={}, execute=True,
                     runtime=lambda *_: pytest.fail('unexpected dispatch'))
    assert not output.exists()


def test_planning_budget_never_dispatches_and_keeps_all_controls(tmp_path):
    report = run_campaign(tmp_path, target_id=TARGET, run_id='offline-plan-budget',
                          execute=False, max_executions=1,
                          runtime=lambda *_: pytest.fail('planning dispatched'))
    assert report['execution_budget']['attempts'] == 0
    assert report['execution_budget']['stop_reason'] == 'planning_only'
    assert len(report['rows']) == 24
    assert all(r['execution_stage'] == 'planned' and not r['actual_run'] for r in report['rows'])
    assert {k: len(v) for k, v in report['control_references'].items()} == {
        'health': 6, 'fault': 6, 'mutation': 6, 'transparency': 6}
    assert not report['seal']['sealed']
    assert not any('matrix_missing' in e or 'control_reference' in e for e in report['seal']['errors'])


@pytest.mark.parametrize('budget', [1, 19, 24, None])
def test_failed_and_negative_blocked_attempts_cannot_bypass_budget(tmp_path, budget):
    calls = []

    def fixture(config, output_dir):
        calls.append(config['cell_id'])
        if config['arm'] == 'negative':
            return {'runtime_status': 'blocked', 'abort_reason': 'offline_negative_blocked'}
        raise OSError('offline deliberate failed dispatch')

    report = run_campaign(tmp_path, target_id=TARGET, run_id='offline-attempts',
                          execute=True, runtime=fixture, max_executions=budget)
    expected = 24 if budget is None else budget
    assert len(calls) == expected
    assert calls == [r['cell_id'] for r in report['rows'][:expected]]
    assert sum(r['execution_stage'] == 'planned' for r in report['rows']) == 24 - expected
    if expected >= 19:
        assert report['rows'][18]['arm'] == 'negative'
        assert report['rows'][18]['runtime_status'] == 'blocked'
    if budget is None:
        assert 'execution_budget' not in report
    else:
        assert report['execution_budget']['attempts'] == expected
        assert report['execution_budget']['stop_reason'] == 'max_executions_reached'
    assert not report['seal']['sealed']
    ledger = json.loads(Path(report['candidate_ledger_path']).read_text())
    assert ledger['natural_candidate_count'] == 0
    assert not any(r.get('candidate_eligible') for r in report['rows'])


def test_explicit_true_false_and_omission_have_distinct_hash_bound_capsules(tmp_path):
    from r2f.core.replay import ReplayCapsule, effective_input_hash
    from r2f.reports.run_report import audit_report

    inputs = []
    configs = []
    for name, params in [('omitted', None), ('false', {FLAG: False}), ('true', {FLAG: True})]:
        report = run_campaign(tmp_path / name, target_id=TARGET, run_id='offline-identities',
                              parameters=params, max_executions=1)
        row = report['rows'][0]
        capsule = ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
        inputs.append(capsule.input)
        configs.append(json.loads(Path(row['runtime_config_path']).read_text()))
        assert effective_input_hash(capsule.input['effective_input']) == row['effective_input_sha256']
        assert capsule.execution is None
        assert not audit_report(tmp_path / name / 'report.json')['sealed']
    assert FLAG not in inputs[0]['effective_input']['final_input']
    assert configs[0][FLAG] is False  # Existing namespace default, outside submitted input.
    assert inputs[1]['effective_input']['final_input'][FLAG] is False
    assert inputs[2]['effective_input']['final_input'][FLAG] is True
    assert len({item['effective_input_sha256'] for item in inputs}) == 3
