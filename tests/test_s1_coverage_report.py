"""Public report queries: offline artifacts never establish real S1 maturity."""
import json
from pathlib import Path

import pytest

from r2f.campaign.runner import run_campaign
from r2f.reports.run_report import audit_report
# Reuse the automatic-runner's offline-only source fixture; it does not qualify ROS.
from test_runner_auto_replay import frozen_fixture_source


def test_planned_report_queries_all_four_inventories_without_execution_credit(tmp_path):
    run_campaign(tmp_path / 'plan', run_id='cp3-plan', execute=False)
    path = tmp_path / 'plan' / 'report.json'
    original = path.read_bytes()
    audit = audit_report(path)
    coverage = audit['s1_coverage']
    assert len(coverage['cases']) == 15
    assert {row['id'] for row in coverage['obligations']} == {f'C{i:02d}' for i in range(1, 19)}
    assert {row['id'] for row in coverage['user_stories']} == set(range(1, 43))
    cells = coverage['required_baseline']['cells']
    assert len(cells) == 72
    assert len({(row['target_id'], row['cell_id']) for row in cells}) == 72
    assert coverage['required_baseline']['cell_count'] == 72
    assert all(row['status'] == 'unverified' and row['named_gaps'] for row in cells)
    assert all(row['current_source_status'] == 'unverified' and row['named_gaps']
               for row in coverage['obligations'])
    c01 = next(row for row in coverage['obligations'] if row['id'] == 'C01')
    assert c01['model_links'] and c01['supporting_execution_refs']
    assert c01['capability_layers']['input']['status'] == 'model_input_only'
    assert c01['capability_layers']['semantic_contract']['status'] == 'unverified'
    assert coverage['report_evidence']['rows'][0]['qualification'] == 'input_only'
    assert path.read_bytes() == original
    assert audit['sealed'] is False


def test_fixture_raw_and_replay_are_reopened_and_never_count_as_real(tmp_path):
    from test_campaign_evidence import fixture_pair

    run_campaign(tmp_path / 'fixture', run_id='cp3-fixture', execute=True, runtime=fixture_pair)
    path = tmp_path / 'fixture' / 'report.json'
    report = json.loads(path.read_text())
    audit = audit_report(path)
    trace = audit['s1_coverage']['report_evidence']
    manifest = json.loads(Path(report['manifest_path']).read_text())
    assert trace['source_manifest'] == manifest['source_manifest']
    evidence = trace['rows'][0]
    assert evidence['raw']['status'] == 'verified'
    assert evidence['replay']['status'] == 'verified_input_capsule'
    assert evidence['qualification'] == 'fixture'
    assert evidence['current_execution_qualified'] is False
    assert all(row['status'] == 'unverified' for row in audit['s1_coverage']['required_baseline']['cells'])
    Path(report['rows'][0]['raw_path']).write_text('damaged\n')
    damaged = audit_report(path)
    raw = damaged['s1_coverage']['report_evidence']['rows'][0]
    assert raw['raw']['status'] == 'invalid'
    assert any('raw' in gap for gap in raw['named_gaps'])
    assert damaged['s1_coverage']['query_valid'] is False
    assert damaged['sealed'] is False


def test_wrong_input_config_and_capsule_bindings_are_named_even_for_plans(tmp_path):
    from r2f.reports.run_report import report_digest, write_json

    run_campaign(tmp_path / 'plan', run_id='cp3-bindings', execute=False)
    path = tmp_path / 'plan' / 'report.json'
    report = json.loads(path.read_text())
    first, second = report['rows'][:2]
    first['replay_path'] = second['replay_path']
    config_path = Path(second['runtime_config_path'])
    config = json.loads(config_path.read_text())
    config['target_revision'] = 99
    write_json(config_path, config)
    report['report_sha256'] = report_digest(report)
    write_json(path, report)
    coverage = audit_report(path)['s1_coverage']
    assert coverage['query_valid'] is False
    assert any('capsule_row_identity_mismatch:repeat' in gap for gap in coverage['named_gaps'])
    assert any('config_row_identity_mismatch:target_revision' in gap for gap in coverage['named_gaps'])
    assert all(not row['current_execution_qualified'] for row in coverage['report_evidence']['rows'])


def test_historical_and_tool_evidence_keep_context_and_never_complete_obligations(tmp_path):
    from test_f1_exploratory_report import plan

    def broken_adapter(config, directory):
        raise RuntimeError('cp3 controlled offline tool failure')

    cycle, _ = plan(tmp_path, runtime=broken_adapter)
    path = Path(cycle['report_path'])
    original = path.read_bytes()
    coverage = audit_report(path)['s1_coverage']
    trace = coverage['report_evidence']
    assert trace['source_current'] is False
    assert 'source_not_current' in trace['named_gaps']
    assert trace['rows'][0]['qualification'] == 'tool_error'
    assert trace['rows'][0]['current_execution_qualified'] is False
    assert 'tool_error_not_sut_observation' in trace['rows'][0]['named_gaps']
    capture = coverage['evidence_captures'][0]
    assert capture['qualified_for_current_source'] is False
    assert capture['record']['source_manifest']['head'] == '32759d89c2b6036b0314cb27e9b87e70bc0b3c7c'
    assert capture['replay']['status'] == 'not_in_capture'
    assert all(row['current_source_status'] == 'unverified' for row in coverage['obligations'])
    assert path.read_bytes() == original


def test_saved_execution_and_coverage_labels_cannot_promote_offline_rows(tmp_path):
    from test_f1_exploratory_report import plan
    from test_feedback_epoch_runner import adapter_runtime
    from r2f.reports.run_report import exploratory_report_digest, write_json

    cycle, report = plan(tmp_path, runtime=adapter_runtime)
    path = Path(cycle['report_path'])
    # These claims must be independently reopened; labels never qualify a run.
    report['formal_runtime'] = True
    report['artifact_qualifications'][report['rows'][0]['cell_id']]['qualification']['real_ros_execution'] = True
    report['s1_coverage'] = {'current_source_status': 'declared_contract_complete'}
    report['report_sha256'] = exploratory_report_digest(report)
    write_json(path, report)
    audit = audit_report(path)
    coverage = audit['s1_coverage']
    assert audit['evidence_valid'] is False
    assert coverage['report_evidence']['evidence_audit_valid'] is False
    assert not any(row['current_execution_qualified'] for row in coverage['report_evidence']['rows'])
    assert all(row['capability_layers']['semantic_contract']['status'] == 'unverified'
               for row in coverage['obligations'])
    assert all(row['current_source_status'] == 'unverified' for row in coverage['cases'])


def test_existing_exploratory_production_and_legacy_audit_share_detached_query(tmp_path):
    from test_f1_exploratory_report import plan
    from r2f.reports.run_report import audit_exploratory_report, exploratory_report_digest, write_json

    cycle, report = plan(tmp_path)
    assert len(report['audit']['s1_coverage']['obligations']) == 18
    path = Path(cycle['report_path'])
    # Model a historical report predating automatic replay attachments. A new
    # retained summary correctly rejects edits to the original report bytes.
    (path.parent / 'replay-summary.json').unlink()
    # Historical reports have no CP3 fields; they require no migration.
    report.pop('audit')
    report['report_sha256'] = exploratory_report_digest(report)
    write_json(path, report)
    original = path.read_bytes()
    direct = audit_exploratory_report(path)
    assert direct['evidence_valid'], direct['errors']
    assert direct['s1_coverage']['report_evidence']['rows'][0]['qualification'] == 'input_only'
    assert direct['s1_coverage']['query_valid'], direct['s1_coverage']['named_gaps']
    direct['s1_coverage']['obligations'][0]['model_links'].clear()
    fresh = audit_report(path)
    assert fresh['s1_coverage']['obligations'][0]['model_links']
    assert path.read_bytes() == original


def test_malformed_reports_and_unavailable_observation_remain_named_unknowns(tmp_path):
    from r2f.reports.run_report import write_json

    path = tmp_path / 'broken.json'
    write_json(path, {'rows': None})
    result = audit_report(path)
    assert result['sealed'] is False
    coverage = result['s1_coverage']
    assert coverage['query_valid'] is False
    assert 'report_rows_not_a_list' in coverage['named_gaps']
    assert len(coverage['required_baseline']['cells']) == 72
    assert all(row['applicability']['status'] == 'applicable' for row in coverage['obligations'])
    assert {row['current_source_status'] for row in coverage['obligations']} == {'unverified'}
    c17 = next(row for row in coverage['obligations'] if row['id'] == 'C17')
    assert c17['model_links'] == [] and c17['unmapped_model_reason']
    assert c17['capability_layers']['input']['status'] == 'unverified'
    assert set(coverage['status_vocabulary']) == {
        'model_input_only', 'wired_pending_real_validation', 'partial_real_validation',
        'declared_contract_complete', 'unsupported', 'not_applicable', 'unverified'}


def test_replay_dispositions_are_traceable_to_existing_hash_checked_ledger(tmp_path):
    from r2f.reports.run_report import write_json

    run_campaign(tmp_path / 'plan', run_id='cp3-replay-trace', execute=False)
    path = tmp_path / 'plan' / 'report.json'
    report = json.loads(path.read_text())
    coverage = audit_report(path)['s1_coverage']
    ref = coverage['report_evidence']['replay_trace']
    assert ref['status'] == 'verified_ledger_reference'
    assert ref['path'] == report['candidate_ledger_path']
    assert ref['sha256'] == report['candidate_ledger_sha256']
    assert ref['dispositions'] == []
    case = next(row for row in coverage['cases'] if row['case_id'] == 'core215')
    assert 'supporting_execution_refs' in case and case['source_ref']
    # Even when no SUT candidate exists, a damaged replay ledger is a named gap.
    write_json(Path(report['candidate_ledger_path']), {'candidates': []})
    damaged = audit_report(path)
    assert damaged['s1_coverage']['query_valid'] is False
    assert 'replay_ledger:hash_mismatch' in damaged['s1_coverage']['named_gaps']


def test_corrupt_lineage_resource_fails_closed_at_public_audit(tmp_path, monkeypatch):
    from r2f.modules.s1 import coverage

    run_campaign(tmp_path / 'plan', run_id='cp3-bad-sidecar', execute=False)
    damaged = tmp_path / 'damaged-lineage.json'
    damaged.write_text('{"schema":"unknown"}')
    monkeypatch.setattr(coverage, '_CAPTURE', damaged)
    audit = audit_report(tmp_path / 'plan' / 'report.json')
    assert audit['sealed'] is False
    assert audit['s1_coverage']['query_valid'] is False
    assert any('coverage_query_error:' in error for error in audit['errors'])
    assert audit['s1_coverage']['inventory_status'] == 'unavailable'


@pytest.mark.parametrize('ledger,reason', [
    ([], 'ledger_not_an_object'),
    ({}, 'candidates_not_a_list'),
    ({'candidates': None}, 'candidates_not_a_list'),
    ({'candidates': {}}, 'candidates_not_a_list'),
    ({'candidates': 'invalid'}, 'candidates_not_a_list'),
    ({'candidates': [None]}, 'candidate_not_an_object:0'),
    ({'candidates': [42]}, 'candidate_not_an_object:0'),
    ({'candidates': [{'replay_attempts': {}}]}, 'replay_attempts_not_a_list:0'),
    ({'candidates': [{'replay_attempts': None}]}, 'replay_attempts_not_a_list:0'),
    ({'candidates': [{'replay_attempts': [None]}]}, 'replay attempt must be a dict'),
])
def test_malformed_replay_ledger_preserves_core_errors_and_all_inventories(tmp_path, ledger, reason):
    from r2f.core.evidence import canonical_hash
    from r2f.reports.run_report import write_json

    ledger_path = tmp_path / 'candidates.json'
    write_json(ledger_path, ledger)
    path = tmp_path / 'report.json'
    write_json(path, {'rows': [], 'candidate_ledger_path': str(ledger_path),
                      'candidate_ledger_sha256': canonical_hash(ledger)})
    original = path.read_bytes(), ledger_path.read_bytes()
    result = audit_report(path)
    assert "report_artifact_error:'manifest_path'" in result['errors']
    coverage = result['s1_coverage']
    assert coverage['query_valid'] is False
    assert coverage['report_evidence']['replay_trace']['status'] == 'invalid'
    assert 'replay_ledger:' + reason in coverage['named_gaps']
    assert len(coverage['cases']) == 15
    assert len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42
    assert len(coverage['required_baseline']['cells']) == 72
    assert all(item['named_gaps'] and item['current_source_status'] == 'unverified'
               for item in coverage['obligations'])
    assert all(item['status'] == 'unverified'
               for item in coverage['required_baseline']['cells'])
    assert not any(item['current_execution_qualified']
                   for item in coverage['report_evidence']['rows'])
    assert result['sealed'] is False
    assert (path.read_bytes(), ledger_path.read_bytes()) == original


def test_config_digest_mismatch_invalidates_query_without_rewriting_input(tmp_path):
    from r2f.reports.run_report import write_json

    run_campaign(tmp_path / 'plan', run_id='cp3-config-digest', execute=False)
    path = tmp_path / 'plan' / 'report.json'
    report = json.loads(path.read_text())
    row = report['rows'][0]
    config_path = Path(row['runtime_config_path'])
    capsule_path = Path(row['replay_path'])
    original = path.read_bytes(), capsule_path.read_bytes()
    healthy = audit_report(path)
    assert healthy['s1_coverage']['query_valid'] is True
    config = json.loads(config_path.read_text())
    config['effective_input_sha256'] = '0' * 64
    write_json(config_path, config)
    damaged_config = config_path.read_bytes()
    result = audit_report(path)
    coverage = result['s1_coverage']
    evidence = coverage['report_evidence']['rows'][0]
    assert coverage['query_valid'] is False
    assert evidence['config']['status'] == 'invalid'
    assert 'config:config_row_identity_mismatch:effective_input_sha256' in evidence['reference_errors']
    assert evidence['replay']['status'] == 'verified_input_capsule'
    assert result['sealed'] is False
    assert evidence['current_execution_qualified'] is False
    assert len(coverage['cases']) == 15 and len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42
    assert len(coverage['required_baseline']['cells']) == 72
    assert all(item['status'] == 'unverified' for item in coverage['required_baseline']['cells'])
    assert (path.read_bytes(), capsule_path.read_bytes()) == original
    assert config_path.read_bytes() == damaged_config


@pytest.mark.parametrize('kind', ['required', 'exploratory'])
@pytest.mark.parametrize('field,reason', [
    ('effective_input_sha256', 'config_row_identity_mismatch:effective_input_sha256'),
    ('post_match_count', 'config_submitted_input_mismatch:post_match_count'),
    ('final_input', 'config_capsule_input_mismatch:final_input'),
    ('input_layer_receipts', 'config_capsule_input_mismatch:input_layer_receipts'),
    ('effective_input', 'config_capsule_input_mismatch:effective_input'),
])
def test_config_matches_saved_capsule_input_and_final_values(tmp_path, kind, field, reason):
    from r2f.reports.run_report import write_json

    if kind == 'required':
        run_campaign(tmp_path / 'plan', run_id='cp3-final-config', execute=False)
        path = tmp_path / 'plan' / 'report.json'
        report = json.loads(path.read_text())
    else:
        from test_f1_exploratory_report import plan
        cycle, report = plan(tmp_path)
        path = Path(cycle['report_path'])
    row = report['rows'][0]
    config_path = Path(row['runtime_config_path'])
    capsule_path = Path(row['replay_path'])
    original = path.read_bytes(), capsule_path.read_bytes()
    healthy = audit_report(path)['s1_coverage']
    assert healthy['query_valid'] is True
    assert healthy['report_evidence']['rows'][0]['config']['status'] == 'verified'
    config = json.loads(config_path.read_text())
    if field == 'effective_input_sha256':
        config[field] = '0' * 64
    elif field == 'post_match_count':
        config[field] += 1
    elif field == 'final_input':
        config[field]['post_match_count'] += 1
    elif field == 'input_layer_receipts':
        config[field] = []
    else:
        config[field] = {'final_input': {'payload': 'unbound input'}}
    write_json(config_path, config)
    config_bytes = config_path.read_bytes()
    result = audit_report(path)
    coverage = result['s1_coverage']
    evidence = coverage['report_evidence']['rows'][0]
    assert coverage['query_valid'] is False
    assert evidence['config']['status'] == 'invalid'
    assert 'config:' + reason in evidence['reference_errors']
    assert evidence['replay']['status'] == 'verified_input_capsule'
    assert result['sealed'] is False
    assert all(not item['current_execution_qualified'] for item in coverage['report_evidence']['rows'])
    assert all(item['current_source_status'] == 'unverified' for item in coverage['obligations'])
    assert (path.read_bytes(), capsule_path.read_bytes()) == original
    assert config_path.read_bytes() == config_bytes


@pytest.fixture
def automatic_fixture_report(tmp_path, frozen_fixture_source):
    import hashlib
    from test_campaign_replay import healthy

    root = tmp_path / 'original'
    originals = {}

    def independent_fixture(config, directory):
        assert not Path(directory).is_relative_to(root)
        originals.update({p: hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in root.rglob('*') if p.is_file()})
        return healthy(config, directory)

    report = run_campaign(root, run_id='cp3-public-automatic-replay', execute=True,
                          runtime=healthy, domain_id=80, replay_runtime=independent_fixture,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    assert originals
    return root, report, originals


def test_public_report_shows_validated_non_candidate_automatic_attempt(automatic_fixture_report):
    import hashlib
    from r2f.reports.candidate_report import query_automatic_replays

    root, report, originals = automatic_fixture_report
    ledger = json.loads((root / 'candidate_ledger.json').read_text())
    assert ledger['natural_candidate_count'] == 0
    result = audit_report(root / 'report.json')
    coverage = result['s1_coverage']
    assert coverage['query_valid'] is True
    automatic = coverage['report_evidence']['automatic_replay']
    assert automatic == query_automatic_replays(root)
    assert automatic['attachment_valid'] is True
    entry, = [item for item in automatic['entries'] if item.get('replay_attempt')]
    assert entry['status'] == 'pass'
    assert entry['candidate_eligible'] is False and entry['original_confirmed'] is False
    assert entry['real_ros_execution'] is False
    assert not any(item['current_execution_qualified'] for item in coverage['report_evidence']['rows'])
    assert all(item['status'] == 'unverified' for item in coverage['required_baseline']['cells'])
    assert result['sealed'] is False
    # The public audit also ran while the original report/index were being sealed.
    assert report['seal']['s1_coverage']['query_valid'] is True
    pending = report['seal']['s1_coverage']['report_evidence']['automatic_replay']
    assert pending['reason_code'] == 'automatic_replay_not_requested'
    assert pending['entries'] == []
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals
    assert json.loads((root / 'report.json').read_text()) == report


def test_public_report_rejects_damaged_automatic_attempt_and_preserves_core_gaps(automatic_fixture_report):
    import hashlib

    root, report, _ = automatic_fixture_report
    before = audit_report(root / 'report.json')
    assert before['errors']  # Existing fixture/source audit failures remain visible.
    automatic = before['s1_coverage']['report_evidence']['automatic_replay']
    entry, = [item for item in automatic['entries'] if item.get('replay_attempt')]
    originals = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in root.rglob('*') if p.is_file()}
    Path(entry['replay_attempt']['path']).write_text('{}\n')
    result = audit_report(root / 'report.json')
    coverage = result['s1_coverage']
    assert coverage['query_valid'] is False
    assert set(before['errors']).issubset(result['errors'])
    assert coverage['report_evidence']['automatic_replay']['attachment_valid'] is False
    assert any(gap.startswith('automatic_replay:runner_replay_invalid:')
               for gap in coverage['named_gaps'])
    assert len(coverage['cases']) == 15 and len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42
    assert len(coverage['required_baseline']['cells']) == 72
    assert all(item['named_gaps'] and item['current_source_status'] == 'unverified'
               for item in coverage['obligations'])
    assert all(item['status'] == 'unverified' for item in coverage['required_baseline']['cells'])
    assert not any(item['current_execution_qualified'] for item in coverage['report_evidence']['rows'])
    assert result['sealed'] is False
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals
    assert json.loads((root / 'report.json').read_text()) == report


def test_public_legacy_report_without_summary_names_no_automatic_request(tmp_path):
    from r2f.reports.run_report import write_json

    path = tmp_path / 'legacy.json'
    write_json(path, {'rows': []})
    original = path.read_bytes()
    result = audit_report(path)
    automatic = result['s1_coverage']['report_evidence']['automatic_replay']
    assert automatic['reason_code'] == 'automatic_replay_not_requested'
    assert automatic['status'] == 'not_run' and automatic['entries'] == []
    assert "report_artifact_error:'manifest_path'" in result['errors']
    assert not any(gap.startswith('automatic_replay:') for gap in result['s1_coverage']['named_gaps'])
    assert not (tmp_path / 'evidence-index.json').exists()
    assert not (tmp_path / 'replay-summary.json').exists()
    assert path.read_bytes() == original
    assert result['sealed'] is False


def test_public_report_keeps_valid_automatic_failure_named_and_unqualified(tmp_path, frozen_fixture_source):
    import hashlib
    from test_campaign_replay import healthy

    root = tmp_path / 'original'
    originals = {}

    def failed_fixture(config, directory):
        originals.update({p: hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in root.rglob('*') if p.is_file()})
        raise RuntimeError('offline controlled automatic replay failure')

    report = run_campaign(root, run_id='cp3-public-replay-failure', execute=True,
                          runtime=healthy, domain_id=80, replay_runtime=failed_fixture,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    result = audit_report(root / 'report.json')
    coverage = result['s1_coverage']
    assert coverage['query_valid'] is True  # An intact failed attempt is valid evidence.
    automatic = coverage['report_evidence']['automatic_replay']
    assert automatic['attachment_valid'] is True
    entry, = [item for item in automatic['entries'] if item.get('replay_attempt')]
    assert entry['reason_code'] == 'runtime_tool_failure'
    assert entry['status'] != 'pass'
    assert entry['candidate_eligible'] is False and entry['original_confirmed'] is False
    assert entry['real_ros_execution'] is False
    assert result['sealed'] is False
    assert not any(item['current_execution_qualified'] for item in coverage['report_evidence']['rows'])
    assert all(item['status'] == 'unverified' for item in coverage['required_baseline']['cells'])
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals
    assert json.loads((root / 'report.json').read_text()) == report


def test_public_report_does_not_attach_another_runs_automatic_attempts(automatic_fixture_report):
    import hashlib
    from r2f.reports.run_report import write_json

    root, _, _ = automatic_fixture_report
    originals = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in root.rglob('*') if p.is_file()}
    path = root / 'unrelated-legacy.json'
    write_json(path, {'run_id': 'unrelated-legacy-run', 'rows': []})
    result = audit_report(path)
    coverage = result['s1_coverage']
    automatic = coverage['report_evidence']['automatic_replay']
    assert coverage['query_valid'] is False
    assert automatic['attachment_valid'] is False
    assert automatic['entries'] == []
    assert 'automatic_replay:report_run_id_mismatch' in coverage['named_gaps']
    assert "report_artifact_error:'manifest_path'" in result['errors']
    assert len(coverage['cases']) == 15 and len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42
    assert len(coverage['required_baseline']['cells']) == 72
    assert all(item['named_gaps'] and item['current_source_status'] == 'unverified'
               for item in coverage['obligations'])
    assert result['sealed'] is False
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals


@pytest.mark.parametrize('fault,reason', [
    ('empty_rows', 'report_cell_set_mismatch'),
    ('duplicate_cell', 'report_cell_set_mismatch'),
    ('other_source', 'report_source_mismatch'),
    ('missing_source', 'report_source_mismatch'),
    ('unindexed_copy', 'report_not_indexed'),
    ('changed_indexed_report', 'indexed_report_hash_mismatch'),
])
def test_public_automatic_attachment_requires_report_cells_source_and_index(
        automatic_fixture_report, fault, reason):
    from copy import deepcopy
    import hashlib
    from r2f.reports.run_report import report_digest, write_json

    root, report, _ = automatic_fixture_report
    assert audit_report(root / 'report.json')['s1_coverage']['query_valid'] is True
    altered = deepcopy(report)
    path = root / 'legacy-copy.json'
    if fault in {'empty_rows', 'duplicate_cell'}:
        altered['rows'] = [] if fault == 'empty_rows' else altered['rows'] + [deepcopy(altered['rows'][0])]
    elif fault in {'other_source', 'missing_source'}:
        altered.pop('manifest_path')
        if fault == 'other_source':
            altered['source_manifest'] = {'head': 'unrelated-source'}
        else:
            altered.pop('source_manifest', None)
    elif fault == 'changed_indexed_report':
        path = root / 'report.json'
        altered['unindexed_change'] = True
    altered['report_sha256'] = report_digest(altered)
    write_json(path, altered)
    originals = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in root.rglob('*') if p.is_file()}
    result = audit_report(path)
    coverage = result['s1_coverage']
    automatic = coverage['report_evidence']['automatic_replay']
    assert coverage['query_valid'] is False
    assert automatic['attachment_valid'] is False
    assert automatic['entries'] == []
    assert 'automatic_replay:' + reason in coverage['named_gaps']
    assert len(coverage['cases']) == 15 and len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42
    assert len(coverage['required_baseline']['cells']) == 72
    assert all(item['current_source_status'] == 'unverified' and item['named_gaps']
               for item in coverage['obligations'])
    assert not any(item['current_execution_qualified'] for item in coverage['report_evidence']['rows'])
    assert result['sealed'] is False
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals


@pytest.mark.parametrize('with_failure', [False, True])
def test_public_report_preserves_associated_unknown_request_without_summary(automatic_fixture_report, with_failure):
    import hashlib
    from r2f.reports.candidate_report import query_automatic_replays
    from r2f.reports.run_report import write_json

    root, report, _ = automatic_fixture_report
    (root / 'replay-summary.json').unlink()
    if with_failure:
        write_json(root / 'replay-failure.json', {'kind': 'automatic_replay_attachment_failure',
                   'run_id': report['run_id'], 'error_type': 'RuntimeError', 'detail': 'offline failure'})
    originals = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in root.rglob('*') if p.is_file()}
    direct = query_automatic_replays(root)
    assert direct['attachment_valid'] is False
    request, = direct['entries']
    assert request['status'] == 'unknown' and request['request']['sha256']
    result = audit_report(root / 'report.json')
    coverage = result['s1_coverage']
    automatic = coverage['report_evidence']['automatic_replay']
    print(json.dumps({'public_automatic_replay': automatic, 'core_errors': result['errors']}))
    assert automatic == direct
    assert 'automatic_replay:runner_replay_invalid:automatic_replay_attachment_failure' in coverage['named_gaps']
    assert not any('report_run_id_mismatch' in gap for gap in coverage['named_gaps'])
    assert set(coverage['report_evidence']['audit_errors']).issubset(result['errors'])
    assert coverage['query_valid'] is False and result['sealed'] is False
    assert len(coverage['cases']) == 15 and len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42 and len(coverage['required_baseline']['cells']) == 72
    assert all(row['current_source_status'] == 'unverified' for row in coverage['obligations'])
    assert not any(row['current_execution_qualified'] for row in coverage['report_evidence']['rows'])
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals


@pytest.mark.parametrize('fault,reason', [
    ('foreign_report', 'report_run_id_mismatch'),
    ('foreign_request', 'request_run_id_mismatch'),
    ('request_index', 'request_identity_mismatch'),
    ('request_intake', 'request_identity_mismatch'),
    ('foreign_failure', 'failure_run_id_mismatch'),
])
def test_public_partial_request_keeps_nonassociated_provenance(automatic_fixture_report, fault, reason):
    import hashlib
    from r2f.reports.candidate_report import query_automatic_replays
    from r2f.reports.run_report import write_json

    root, _, _ = automatic_fixture_report
    (root / 'replay-summary.json').unlink()
    path = root / 'report.json'
    request_path, = (root / 'replay-scheduling').glob('*/request.json')
    request = json.loads(request_path.read_text())
    if fault == 'foreign_report':
        path = root / 'foreign-legacy.json'
        write_json(path, {'run_id': 'foreign-run', 'rows': []})
    elif fault == 'foreign_request':
        request['run_id'] = 'foreign-run'
        write_json(request_path, request)
    elif fault in {'request_index', 'request_intake'}:
        request['original_index' if fault == 'request_index' else 'intake_identity']['sha256'] = '0' * 64
        write_json(request_path, request)
    else:
        write_json(root / 'replay-failure.json', {'kind': 'automatic_replay_attachment_failure',
                   'run_id': 'foreign-run', 'error_type': 'RuntimeError', 'detail': 'offline failure'})
    direct = query_automatic_replays(root)
    originals = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in root.rglob('*') if p.is_file()}
    result = audit_report(path)
    coverage = result['s1_coverage']
    automatic = coverage['report_evidence']['automatic_replay']
    assert automatic['status'] == 'nonassociated' and automatic['attachment_valid'] is False
    assert automatic['entries'] == []
    assert automatic['nonassociated_entries'] == direct['entries']
    assert automatic['nonassociated_entries'][0]['status'] == 'unknown'
    assert automatic['reason_code'] == 'automatic_replay_attachment_failure'
    assert automatic['association_error'] == reason
    assert set(direct['errors']).issubset(automatic['errors'])
    if fault == 'foreign_failure':
        assert automatic['failure'] == direct['failure']
    assert 'automatic_replay:' + reason in coverage['named_gaps']
    assert coverage['query_valid'] is False and result['sealed'] is False
    assert set(coverage['report_evidence']['audit_errors']).issubset(result['errors'])
    if fault == 'foreign_report':
        assert "report_artifact_error:'manifest_path'" in result['errors']
    assert len(coverage['cases']) == 15 and len(coverage['obligations']) == 18
    assert len(coverage['user_stories']) == 42 and len(coverage['required_baseline']['cells']) == 72
    assert all(row['current_source_status'] == 'unverified' for row in coverage['obligations'])
    assert not any(row['current_execution_qualified'] for row in coverage['report_evidence']['rows'])
    assert {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals} == originals
