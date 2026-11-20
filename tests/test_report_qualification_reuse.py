"""Offline orchestration/dependency contracts; no runtime or live evidence.

The expensive qualification is explicitly mocked. Content guards are real file
reads; these tests cannot establish DDS, boundary or source maturity.
"""
from copy import deepcopy
import json
import os
from unittest.mock import Mock

import pytest

from r2f.reports import run_report as audit
from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET


@pytest.fixture
def setup_case(tmp_path, monkeypatch):
    root = tmp_path / 'report'
    case = root / 'cases' / 'one'
    case.mkdir(parents=True)
    for name in ('raw.jsonl', 'capsule.json', 'runtime_config.json',
                 'runtime_result.json', 'worker.json', 'manifest.json', 'boundary.json'):
        (case / name).write_text('{"a":1}\n')
    source = dict(source_repo=str(tmp_path / 'source'), head='a' * 40,
                  behavior_tree_sha256='b' * 64, source_frozen=True, dirty_behavior_paths=[])
    row = dict(cell_id='one', target_id=TOPOLOGY_TARGET, actual_run=True,
               execution_stage='judged', oracle_verdict='pass', observer_complete=True)
    report = dict(schema_version=audit.EXPLORATORY_SCHEMA, campaign_kind='exploratory',
                  run_id='offline-only', source_manifest=source, rows=[row])
    expensive = Mock(return_value=dict(real_ros_execution=True, qualification_errors=[],
                                      nested={'receipt': ['raw-derived-mock']}))
    monkeypatch.setattr(audit, '_topology_artifact_qualification', expensive)
    current_source = Mock(side_effect=lambda _: deepcopy(source))
    monkeypatch.setattr(audit, 'source_manifest', current_source)
    return root, case, row, report, expensive, current_source


def test_same_row_reuses_complete_result_without_mutable_alias(setup_case):
    root, _, row, report, expensive, source = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    first = reuse.qualify(root, row, report)
    first['nested']['receipt'].append('consumer-change')
    second = reuse.qualify(root, row, report)
    second['nested']['receipt'].append('another-change')
    assert reuse.qualify(root, row, report)['nested']['receipt'] == ['raw-derived-mock']
    reuse.finish()
    assert expensive.call_count == 1
    assert source.call_count == 4  # Original source gate, including final stability.


@pytest.mark.parametrize('name', ['raw.jsonl', 'capsule.json', 'runtime_config.json',
                                 'runtime_result.json', 'worker.json', 'manifest.json', 'boundary.json'])
@pytest.mark.parametrize('when', ['reuse', 'finish'])
def test_dependency_byte_change_even_same_size_and_mtime_fails(setup_case, name, when):
    root, case, row, report, expensive, _ = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    path = case / name
    stat = path.stat()
    path.write_text('{"a":2}\n')
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(ValueError, match='artifact_changed'):
        if when == 'reuse':
            reuse.qualify(root, row, report)
        else:
            reuse.finish()
    assert expensive.call_count == 1


@pytest.mark.parametrize('change', ['add', 'delete', 'symlink'])
def test_case_membership_and_resolution_are_dependencies(setup_case, change):
    root, case, row, report, _, _ = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    if change == 'add':
        (case / 'new').write_text('new')
    elif change == 'delete':
        (case / 'worker.json').unlink()
    else:
        path = case / 'worker.json'
        original = path.read_bytes()
        path.unlink()
        outside = root / 'outside'
        outside.write_bytes(original)
        path.symlink_to(outside)
    with pytest.raises(ValueError):
        reuse.finish()


@pytest.mark.parametrize('key,value', [('head', 'c' * 40), ('behavior_tree_sha256', 'd' * 64),
                                      ('source_frozen', False), ('dirty_behavior_paths', ['changed'])])
@pytest.mark.parametrize('when', ['reuse', 'finish'])
def test_source_head_and_behavior_changes_fail(setup_case, key, value, when):
    root, _, row, report, _, source = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    changed = deepcopy(report['source_manifest'])
    changed[key] = value
    source.side_effect = lambda _: changed
    with pytest.raises(ValueError, match='source_changed'):
        if when == 'reuse':
            reuse.qualify(root, row, report)
        else:
            reuse.finish()


def test_dependency_changes_during_first_calculation_fail(setup_case):
    root, case, row, report, expensive, _ = setup_case
    def change(*args):
        (case / 'raw.jsonl').write_text('tampered')
        return dict(real_ros_execution=True, qualification_errors=[])
    expensive.side_effect = change
    reuse = audit._TopologyQualificationReuse(root)
    with pytest.raises(ValueError, match='artifact_changed'):
        reuse.qualify(root, row, report)
    assert not reuse.entries


@pytest.mark.parametrize('result', [dict(real_ros_execution=False, qualification_errors=['unknown']),
                                    dict(real_ros_execution=True, qualification_errors=['bad']),
                                    dict(real_ros_execution=None)])
def test_unknown_and_inconsistent_results_never_cached(setup_case, result):
    root, _, row, report, expensive, _ = setup_case
    expensive.return_value = result
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    reuse.qualify(root, row, report)
    assert expensive.call_count == 2 and not reuse.entries


def test_exceptions_never_cached(setup_case):
    root, _, row, report, expensive, _ = setup_case
    expensive.side_effect = ValueError('unknown')
    reuse = audit._TopologyQualificationReuse(root)
    for _ in range(2):
        with pytest.raises(ValueError, match='unknown'):
            reuse.qualify(root, row, report)
    assert expensive.call_count == 2 and not reuse.entries


@pytest.mark.parametrize('change', ['row_input', 'report_input', 'row_id', 'root'])
def test_distinct_input_identity_is_not_reused(setup_case, change, tmp_path):
    root, _, row, report, expensive, _ = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    row, report = deepcopy(row), deepcopy(report)
    if change == 'row_input':
        row['mutation_plan'] = {'different': True}
    elif change == 'report_input':
        report['formal_runtime'] = True
    elif change == 'row_id':
        row['cell_id'] = 'two'
    else:
        root = tmp_path
    reuse.qualify(root, row, report)
    assert expensive.call_count == 2


def test_unenumerable_dependencies_use_original_full_path(setup_case):
    root, case, row, report, expensive, _ = setup_case
    (case / 'alias').symlink_to(case / 'worker.json')
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    reuse.qualify(root, row, report)
    assert expensive.call_count == 2 and not reuse.entries


def wire_offline_audit(monkeypatch):
    """Exercise real audit orchestration with explicit mocked unrelated gates."""
    monkeypatch.setattr(audit, '_validate_exploratory_identity', lambda *a: None)
    monkeypatch.setattr(audit, '_validate_exploratory_profile', lambda *a: None)
    def verifier(root, row, report, errors, *, qualification_reuse=None):
        qualification_reuse.qualify(root, row, report)
        return True
    def ledger(root, report, rows, errors, details, *, qualification_reuse=None):
        for row in rows:
            qualification_reuse.qualify(root, row, report)
    monkeypatch.setattr(audit, '_verify_exploratory_row', verifier)
    monkeypatch.setattr(audit, '_audit_exploratory_ledger', ledger)
    monkeypatch.setattr(audit, '_audit_exploratory_budget', lambda *a, **kw: None)
    monkeypatch.setattr(audit, '_audit_exploratory_controls', lambda *a: None)
    monkeypatch.setattr(audit, 'boundary_qualification_for_row', lambda *a: {'status': 'not_applicable'})
    monkeypatch.setattr(audit, 'exploratory_coverage_fields', lambda *a: None)


def test_audit_local_orchestration_three_consumers_and_cross_call(setup_case, monkeypatch):
    root, _, _, report, expensive, _ = setup_case
    wire_offline_audit(monkeypatch)
    report['report_sha256'] = audit.exploratory_report_digest(report)
    for expected in (1, 2):
        result = audit.audit_exploratory_report(root / 'report.json', report=report)
        assert result['evidence_valid'], result['errors']
        assert expensive.call_count == expected  # Three consumers, one derivation per audit.


def test_final_guard_fails_if_last_consumer_changes_boundary(setup_case, monkeypatch):
    root, case, _, report, _, _ = setup_case
    wire_offline_audit(monkeypatch)
    def coverage(*args):
        (case / 'boundary.json').write_text('changed after final cache lookup')
    monkeypatch.setattr(audit, 'exploratory_coverage_fields', coverage)
    report['report_sha256'] = audit.exploratory_report_digest(report)
    result = audit.audit_exploratory_report(root / 'report.json', report=report)
    assert result['evidence_valid'] is False
    assert any('artifact_changed' in error for error in result['errors'])


def test_public_ledger_default_does_not_reuse(setup_case, monkeypatch):
    root, _, row, report, expensive, _ = setup_case
    from r2f.reports.candidate_report import exploratory_candidate_ledger
    monkeypatch.setattr(audit, 'boundary_qualification_for_row', lambda *a: {'status': 'not_applicable'})
    monkeypatch.setattr(audit, '_f1_adapter_artifact_qualification', lambda *a: None)
    ledger = exploratory_candidate_ledger([row])
    path = root / 'ledger.json'
    path.write_text(json.dumps(ledger))
    report.update(candidate_ledger_path=str(path), candidate_ledger_sha256=audit.canonical_hash(ledger))
    for _ in range(2):
        errors = []
        audit._audit_exploratory_ledger(root, report, [row], errors, {})
        assert not errors
    assert expensive.call_count == 2


@pytest.mark.parametrize('where', ['row', 'report'])
def test_in_memory_input_change_after_last_consumer_fails(setup_case, where):
    root, _, row, report, _, _ = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    (row if where == 'row' else report)['unexpected_after_validation'] = True
    with pytest.raises(ValueError, match='input_changed'):
        reuse.finish()


def test_report_saved_qualification_does_not_supply_result(setup_case):
    root, _, row, report, expensive, _ = setup_case
    row['execution_qualification'] = {'real_ros_execution': True, 'forged': True}
    report['artifact_qualifications'] = {'one': row['execution_qualification']}
    reuse = audit._TopologyQualificationReuse(root)
    assert 'forged' not in reuse.qualify(root, row, report)
    assert expensive.call_count == 1


def test_source_read_failure_fails_closed_at_finish(setup_case):
    root, _, row, report, _, source = setup_case
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    source.side_effect = ValueError('HEAD changed while collecting source manifest')
    with pytest.raises(ValueError, match='HEAD changed'):
        reuse.finish()


def test_synthetic_three_consumer_before_after_counts(setup_case):
    root, _, row, report, expensive, _ = setup_case
    # Count-only contract, not a latency benchmark or real-report validation.
    for _ in range(3):
        audit._topology_artifact_qualification(root, row, report)
    assert expensive.call_count == 3
    expensive.reset_mock()
    reuse = audit._TopologyQualificationReuse(root)
    for _ in range(3):
        reuse.qualify(root, row, report)
    reuse.finish()
    assert expensive.call_count == 1


def test_ledger_hash_gate_still_runs_after_qualification_reuse(setup_case, monkeypatch):
    root, _, row, report, expensive, _ = setup_case
    from r2f.reports.candidate_report import exploratory_candidate_ledger
    monkeypatch.setattr(audit, 'boundary_qualification_for_row', lambda *a: {'status': 'not_applicable'})
    monkeypatch.setattr(audit, '_f1_adapter_artifact_qualification', lambda *a: None)
    ledger = exploratory_candidate_ledger([row])
    path = root / 'ledger.json'
    path.write_text(json.dumps(ledger))
    report.update(candidate_ledger_path=str(path), candidate_ledger_sha256=audit.canonical_hash(ledger))
    reuse = audit._TopologyQualificationReuse(root)
    reuse.qualify(root, row, report)
    ledger['natural_candidate_count'] += 1
    path.write_text(json.dumps(ledger))
    errors = []
    audit._audit_exploratory_ledger(root, report, [row], errors, {}, qualification_reuse=reuse)
    assert 'candidate_ledger_hash_mismatch' in errors
    assert expensive.call_count == 1
    reuse.finish()  # The ledger is deliberately not cached by the context.
