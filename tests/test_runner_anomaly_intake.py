"""CP2 public runner evidence queries, offline fixtures only; no ROS qualification."""
import json
from pathlib import Path

import pytest

from r2f.campaign.runner import run_campaign
from r2f.reports.run_report import audit_report


def test_normal_planning_retains_report_and_queryable_diagnostic_packets(tmp_path):
    from r2f.reports.candidate_report import query_runner_evidence

    report = run_campaign(tmp_path, run_id='intake-planned')
    evidence = query_runner_evidence(tmp_path)
    assert evidence['evidence_valid'], evidence
    assert len(evidence['packets']) == len(report['rows']) == 24
    assert {p['runner_diagnostic']['kind'] for p in evidence['packets']} == {'planned'}
    assert all(p['runner_diagnostic']['candidate_eligible'] is False for p in evidence['packets'])
    assert all(p['runner_diagnostic']['replay_status'] == 'not_run' for p in evidence['packets'])
    assert evidence['tool_problems'] == []
    assert json.loads((tmp_path / 'report.json').read_text()) == report
    assert audit_report(tmp_path / 'report.json')['sealed'] is False


def test_worker_exception_preserves_pre_action_identity_and_original_artifacts(tmp_path):
    import subprocess
    from r2f.reports.candidate_report import query_runner_evidence

    originals = {}

    def worker_exits(config, directory):
        identity = json.loads((tmp_path / 'evidence' / config['cell_id'] / 'identity.json').read_text())
        assert identity['run_id'] == config['run_id']
        assert identity['source_manifest'] == config['source_manifest']
        assert identity['effective_input_sha256'] == config['effective_input_sha256']
        assert identity['rule_id']
        for name, data in [('raw.jsonl', '{"partial": true}\n'),
                           ('stdout.log', 'fixture stdout'), ('stderr.log', 'fixture stderr'),
                           ('runtime_result.json', '{"partial_result": true}')]:
            path = Path(directory) / name
            path.write_text(data)
            originals[path] = path.read_bytes()
        raise subprocess.CalledProcessError(17, ['fixture-worker'],
                                            output='fixture stdout', stderr='fixture stderr')

    report = run_campaign(tmp_path, run_id='intake-worker', execute=True, runtime=worker_exits)
    evidence = query_runner_evidence(tmp_path)
    assert evidence['evidence_valid'], evidence
    assert {p['runner_diagnostic']['kind'] for p in evidence['packets']} == {'worker_exit'}
    for packet in evidence['packets']:
        diagnostic = packet['runner_diagnostic']
        assert diagnostic['error']['returncode'] == 17
        assert diagnostic['error']['stdout'] == 'fixture stdout'
        assert diagnostic['error']['stderr'] == 'fixture stderr'
        assert diagnostic['actual_run'] is None
        assert diagnostic['cleanup'] == {'status': 'unknown', 'reason': 'no_cleanup_receipt'}
        assert packet['raw']['raw_path'] is None  # Retained partial bytes are not qualified raw.
        assert 'execution.actual_run' in packet['incomplete_fields']
        assert len(diagnostic['artifacts']) == 4
    assert all(row['actual_run'] is False for row in report['rows'])
    assert {path: path.read_bytes() for path in originals} == originals


def test_exploratory_timeout_is_queryable_and_preserves_normal_report_entry(tmp_path):
    from test_f1_exploratory_report import plan
    from r2f.reports.candidate_report import query_runner_evidence
    from r2f.reports.run_report import audit_exploratory_report

    def timeout(config, directory):
        root = Path(directory).parents[2]
        identity = json.loads((root / 'evidence' / config['cell_id'] / 'identity.json').read_text())
        assert identity['effective_input_sha256'] == config['effective_input_sha256']
        raise TimeoutError('fixture deadline expired')

    cycle, report = plan(tmp_path, runtime=timeout)
    evidence = query_runner_evidence(tmp_path / 'run')
    assert evidence['evidence_valid'], evidence
    packet, = evidence['packets']
    assert packet['runner_diagnostic']['kind'] == 'timeout'
    assert packet['runner_diagnostic']['error']['detail'] == 'fixture deadline expired'
    assert packet['runner_diagnostic']['actual_run'] is None
    assert cycle['rows'][0]['actual_run'] is False
    assert Path(cycle['report_path']).is_file()
    assert report['schema_version']
    assert audit_exploratory_report(cycle['report_path'])['sealed'] is False


def test_rule_failure_and_missing_observation_remain_distinct_fixture_diagnostics(tmp_path):
    from test_campaign_evidence import fixture_pair
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    originals = {}

    def controlled(config, directory):
        result = fixture_pair(config, directory, fresh_matches=True)
        if config['arm'] == 'baseline':
            victim = config['pre_match_count'] + 1
            result['events'] = [e for e in result['events']
                                if not (e['event'] == 'receive' and e.get('send_sequence') == victim)]
        elif config['arm'] == 'mutated':
            result['events'] = [e for e in result['events'] if e['event'] != 'window_close']
        for index, event in enumerate(result['events']):
            event['event_index'] = index
        path = Path(result['raw_path'])
        path.write_text(''.join(json.dumps(e) + '\n' for e in result['events']))
        result['raw_sha256'] = raw_sha256(path)
        originals[path] = path.read_bytes()
        return result

    report = run_campaign(tmp_path, run_id='intake-rule-and-observation', execute=True, runtime=controlled)
    evidence = query_runner_evidence(tmp_path)
    assert evidence['evidence_valid'], evidence
    by_cell = {p['packet_id'].split(':', 1)[1]: p for p in evidence['packets']}
    for row in report['rows']:
        packet = by_cell[row['cell_id']]
        diagnostic = packet['runner_diagnostic']
        if row['arm'] == 'baseline':
            assert row['oracle_verdict'] == 'fail', row
            assert diagnostic['kind'] == 'rule_failure'
        elif row['arm'] == 'mutated':
            assert diagnostic['kind'] == 'observation_insufficient'
        elif row['arm'] == 'transparent':
            assert diagnostic['kind'] == 'normal'
        else:  # The intentional negative fault retains its failing oracle.
            assert diagnostic['kind'] == 'rule_failure'
        assert diagnostic['runtime']['evidence_origin'] == 'offline'
        assert diagnostic['candidate_eligible'] is False
    ledger = json.loads((tmp_path / 'candidate_ledger.json').read_text())
    assert ledger['natural_candidate_count'] == 0
    assert {p: p.read_bytes() for p in originals} == originals


def test_packaging_failure_is_returned_and_queryable_without_rewriting_published_report(tmp_path, monkeypatch):
    from r2f.reports.candidate_report import query_runner_evidence

    published = {}
    original_open = Path.open

    def failing_markdown(path, *args, **kwargs):
        if path == tmp_path / 'report.md':
            published['report'] = (tmp_path / 'report.json').read_bytes()
            raise OSError('fixture markdown packaging failure')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', failing_markdown)
    report = run_campaign(tmp_path, run_id='intake-packaging')
    assert report['anomaly_intake']['tool_problems'][0]['kind'] == 'packaging_failure'
    evidence = query_runner_evidence(tmp_path)
    assert evidence['evidence_valid'], evidence
    assert evidence['tool_problems'][0]['detail'] == 'fixture markdown packaging failure'
    assert (tmp_path / 'report.json').read_bytes() == published['report']
    assert len(evidence['packets']) == 24


def test_existing_report_published_by_a_failing_wrapper_is_never_overwritten(tmp_path):
    from r2f.reports.candidate_report import query_runner_evidence

    original = b'{"run_id": "foreign-existing-report", "original": true}\n'

    def wrapper(config, directory):
        path = tmp_path / 'report.json'
        if not path.exists():
            path.write_bytes(original)
        raise RuntimeError('fixture wrapper failed after saving a report')

    report = run_campaign(tmp_path, run_id='intake-existing-report', execute=True, runtime=wrapper)
    assert (tmp_path / 'report.json').read_bytes() == original
    assert report['anomaly_intake']['tool_problems'][0]['kind'] == 'packaging_failure'
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'] is False  # The foreign report cannot bind to this run.
    assert any('identity_mismatch' in error for error in queried['errors'])


def test_public_query_rejects_changed_retained_runtime_bytes(tmp_path):
    from test_f1_exploratory_report import plan
    from test_feedback_epoch_runner import adapter_runtime
    from r2f.reports.candidate_report import query_runner_evidence

    cycle, _ = plan(tmp_path, runtime=adapter_runtime)
    root = Path(cycle['report_path']).parent
    assert query_runner_evidence(root)['evidence_valid']
    raw = Path(cycle['rows'][0]['raw_path'])
    raw.write_bytes(raw.read_bytes() + b'\n')
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'] is False
    assert any('hash_mismatch' in e for e in queried['errors'])


@pytest.mark.parametrize('damage', ['source', 'target', 'rule', 'input', 'source_shape', 'report_source'])
def test_public_query_rejects_rehashed_misbound_identity(tmp_path, damage):
    from test_f1_exploratory_report import plan
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence
    from r2f.reports.run_report import exploratory_report_digest

    cycle, _ = plan(tmp_path)
    root = Path(cycle['report_path']).parent
    index_path = root / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    if damage == 'report_source':
        report_path = Path(cycle['report_path'])
        report = json.loads(report_path.read_text())
        report['source_manifest'] = ['malformed-source']
        report['report_sha256'] = exploratory_report_digest(report)
        report_path.write_text(json.dumps(report))
        next(ref for ref in index['reports'] if ref['path'] == 'report.json')['sha256'] = raw_sha256(report_path)
    else:
        ref = index['packets'][0]
        packet_path = root / ref['path']
        packet = json.loads(packet_path.read_text())
        if damage == 'input':
            packet['input']['effective_input_sha256'] = '0' * 64
        elif damage == 'source_shape':
            packet['identity'] = []
        else:
            key = {'target': 'target_id', 'rule': 'rule', 'source': 'source'}[damage]
            packet['identity'][key] = 'foreign-identity'
        packet_path.write_text(json.dumps(packet))
        ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'] is False, queried
    assert queried['errors']


def test_packet_export_failure_remains_queryable_with_original_identities(tmp_path, monkeypatch):
    import os
    from r2f.reports.candidate_report import query_runner_evidence

    original_link = os.link

    def deny_packet(source, destination, *args, **kwargs):
        if Path(destination).name == 'review-packet.json':
            raise PermissionError('fixture packet export denied')
        return original_link(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, 'link', deny_packet)
    report = run_campaign(tmp_path, run_id='intake-export-failure')
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'], queried
    assert queried['packets'] == []
    assert len(queried['identities']) == 24
    assert len(queried['tool_problems']) == 24
    assert {e['kind'] for e in queried['tool_problems']} == {'packet_export_failure'}
    assert {e['error_type'] for e in queried['tool_problems']} == {'PermissionError'}
    assert report['anomaly_intake']['tool_problems'] == queried['tool_problems']
    assert (tmp_path / 'report.json').is_file()


@pytest.mark.parametrize('entry', ['required', 'exploratory'])
def test_malformed_wrapper_return_preserves_identity_and_partial_files(tmp_path, entry):
    from test_f1_exploratory_report import plan
    from r2f.reports.candidate_report import query_runner_evidence

    originals = {}

    def malformed(config, directory):
        for name, data in [('runtime_result.json', '{"original_partial_result": true}'),
                           ('raw.jsonl', '{"partial":true}\n')]:
            path = Path(directory) / name
            path.write_text(data)
            originals[path] = path.read_bytes()
        return ['invalid-runtime-result']

    if entry == 'required':
        result = run_campaign(tmp_path, run_id='intake-malformed', execute=True, runtime=malformed)
        root = tmp_path
    else:
        result, _ = plan(tmp_path, runtime=malformed)
        root = tmp_path / 'run'
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'], queried
    assert {p['runner_diagnostic']['kind'] for p in queried['packets']} == {'tool_failure'}
    assert all(p['runner_diagnostic']['actual_run'] is None for p in queried['packets'])
    assert all(p['runner_diagnostic']['error']['error_type'] == 'ValueError' for p in queried['packets'])
    assert all(row['actual_run'] is False for row in result['rows'])
    assert {p: p.read_bytes() for p in originals} == originals


def test_exploratory_packaging_error_keeps_terminal_cycle_and_preexisting_packet(tmp_path, monkeypatch):
    import os
    from r2f.reports.candidate_report import query_runner_evidence

    original_link = os.link

    def deny_report(source, destination, *args, **kwargs):
        if Path(destination).name == 'report.json':
            raise OSError('fixture report publication failure')
        return original_link(source, destination, *args, **kwargs)

    # Use the public runner directly: plan() also assumes a report to load.
    from test_fuzz_exploratory_targets import _profile, _corpus, LATE
    from test_exploratory_checkpoint import SOURCE
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler
    from r2f.campaign.runner import run_exploratory_cycle
    from unittest.mock import patch

    corpus = _corpus(LATE, 'P01', boundary='finite', mutator_id='M03')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=42)
    monkeypatch.setattr(os, 'link', deny_report)
    with patch('r2f.campaign.runner.source_manifest', return_value=SOURCE):
        result = run_exploratory_cycle(tmp_path, corpus=corpus, feedback=feedback,
                                      scheduler=scheduler, profile=_profile(LATE, ['P01']),
                                      run_id='intake-exploratory-packaging', max_inputs=1)
    assert 'report_path' not in result
    assert json.loads((tmp_path / 'cycle.json').read_text()) == result
    evidence = query_runner_evidence(tmp_path)
    assert evidence['evidence_valid'], evidence
    assert evidence['tool_problems'][0]['kind'] == 'packaging_failure'
    assert len(evidence['packets']) == 1
    assert not (tmp_path / 'report.json').exists()
    assert evidence['reports'][0]['path'] == 'cycle.json'


@pytest.mark.parametrize('failure', ['worker_exit', 'timeout'])
def test_returned_runtime_failure_keeps_original_status_and_exit_evidence(tmp_path, failure):
    from test_f1_exploratory_report import plan
    from test_feedback_epoch_runner import adapter_runtime
    from r2f.reports.candidate_report import query_runner_evidence

    def fails(config, directory):
        result = adapter_runtime(config, directory)
        result['runtime_status'] = 'failed'
        result['reason'] = ('worker_shutdown_failed' if failure == 'worker_exit'
                            else 'timeout waiting for reader')
        if failure == 'worker_exit':
            result['execution_metadata']['workers'][0]['returncode'] = 23
        return result

    cycle, _ = plan(tmp_path, runtime=fails)
    queried = query_runner_evidence(Path(cycle['report_path']).parent)
    assert queried['evidence_valid'], queried
    packet, = queried['packets']
    diagnostic = packet['runner_diagnostic']
    assert diagnostic['kind'] == failure
    assert diagnostic['runtime']['runtime_status'] == 'failed'
    assert diagnostic['candidate_eligible'] is False
    if failure == 'worker_exit':
        assert diagnostic['runtime']['execution_metadata']['workers'][0]['returncode'] == 23


def test_public_query_rejects_rehashed_raw_reference_from_another_cell(tmp_path):
    from test_campaign_evidence import fixture_pair
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    report = run_campaign(tmp_path, run_id='intake-cross-cell', execute=True,
                          runtime=lambda c, d: fixture_pair(c, d, fresh_matches=True))
    index_path = tmp_path / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    ref = index['packets'][0]
    packet_path = tmp_path / ref['path']
    packet = json.loads(packet_path.read_text())
    other = report['rows'][1]
    packet['raw'] = {'raw_path': other['raw_path'], 'raw_sha256': other['raw_sha256']}
    packet_path.write_text(json.dumps(packet))
    ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'] is False, queried
    assert any('raw_identity_mismatch' in e for e in queried['errors'])


def test_rehashed_packet_and_saved_input_cannot_contradict_existing_report(tmp_path):
    from test_f1_exploratory_report import plan
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    cycle, _ = plan(tmp_path)
    root = Path(cycle['report_path']).parent
    index_path = root / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    identity_ref = index['identities'][0]
    saved_path = root / identity_ref['path']
    saved = json.loads(saved_path.read_text())
    saved['effective_input_sha256'] = '0' * 64
    saved_path.write_text(json.dumps(saved))
    identity_ref['sha256'] = raw_sha256(saved_path)
    packet_ref = index['packets'][0]
    packet_path = root / packet_ref['path']
    packet = json.loads(packet_path.read_text())
    packet['input']['effective_input_sha256'] = '0' * 64
    packet['intake_identity']['sha256'] = identity_ref['sha256']
    packet_path.write_text(json.dumps(packet))
    packet_ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'] is False, queried
    assert any('report_row_identity_mismatch' in e for e in queried['errors'])


def test_terminal_cycle_publication_failure_remains_queryable(tmp_path, monkeypatch):
    import os
    from unittest.mock import patch
    from test_exploratory_checkpoint import SOURCE
    from test_fuzz_exploratory_targets import _profile, _corpus, LATE
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler
    from r2f.campaign.runner import run_exploratory_cycle
    from r2f.reports.candidate_report import query_runner_evidence

    original_link = os.link

    def deny_terminal(source, destination, *args, **kwargs):
        if Path(destination).name == 'cycle.json':
            raise OSError('fixture cycle publication failure')
        return original_link(source, destination, *args, **kwargs)

    corpus = _corpus(LATE, 'P01', boundary='finite', mutator_id='M03')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=42)
    monkeypatch.setattr(os, 'link', deny_terminal)
    with patch('r2f.campaign.runner.source_manifest', return_value=SOURCE):
        result = run_exploratory_cycle(tmp_path, corpus=corpus, feedback=feedback,
                                      scheduler=scheduler, profile=_profile(LATE, ['P01']),
                                      run_id='intake-terminal-failure', max_inputs=1)
    assert Path(result['report_path']).is_file()
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'], queried
    assert queried['tool_problems'][0]['kind'] == 'terminal_index_failure'
    assert queried['tool_problems'][0]['detail'] == 'fixture cycle publication failure'
    assert not (tmp_path / 'cycle.json').exists()


def test_unavailable_exception_evidence_has_named_missing_reasons(tmp_path):
    from test_f1_exploratory_report import plan
    from r2f.reports.candidate_report import query_runner_evidence

    def no_output(config, directory):
        raise TimeoutError('fixture no streams or process receipt')

    cycle, _ = plan(tmp_path, runtime=no_output)
    queried = query_runner_evidence(Path(cycle['report_path']).parent)
    packet, = queried['packets']
    missing = packet['runner_diagnostic']['missing_evidence']
    for field in ('raw', 'stdout', 'stderr', 'exit_code', 'actual_run', 'cleanup'):
        assert missing[field]
        assert 'runner_diagnostic.' + field in packet['incomplete_fields']


def test_preexisting_markdown_report_is_retained_and_export_failure_named(tmp_path):
    from r2f.reports.candidate_report import query_runner_evidence

    original = b'fixture wrapper saved diagnostic report\n'

    def wrapper(config, directory):
        path = tmp_path / 'report.md'
        if not path.exists():
            path.write_bytes(original)
        raise TimeoutError('fixture failed after writing report')

    result = run_campaign(tmp_path, run_id='intake-existing-markdown', execute=True, runtime=wrapper)
    assert (tmp_path / 'report.md').read_bytes() == original
    assert result['anomaly_intake']['tool_problems'][0]['kind'] == 'packaging_failure'
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'], queried
    assert queried['tool_problems'][0]['error_type'] == 'FileExistsError'


def test_final_evidence_index_export_failure_does_not_hide_the_original_report(tmp_path, monkeypatch):
    import os
    from r2f.reports.candidate_report import query_runner_evidence

    original_link = os.link

    def deny_index(source, destination, *args, **kwargs):
        if Path(destination).name == 'evidence-index.json':
            raise OSError('fixture final evidence index failure')
        return original_link(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, 'link', deny_index)
    report = run_campaign(tmp_path, run_id='intake-index-failure')
    assert report['anomaly_intake']['tool_problems'][0]['kind'] == 'evidence_index_export_failure'
    assert (tmp_path / 'report.json').is_file()
    assert len(list((tmp_path / 'evidence').glob('*/review-packet.json'))) == 24
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'] is False
    assert queried['tool_problems'][0]['kind'] == 'evidence_index_unavailable'
    assert audit_report(tmp_path / 'report.json')['sealed'] is False


def test_query_rejects_rehashed_diagnostics_from_another_sealed_cell(tmp_path):
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    def timeout(config, directory):
        (Path(directory) / 'stderr.log').write_text('timeout in ' + config['cell_id'])
        raise TimeoutError('original timeout in ' + config['cell_id'])

    run_campaign(tmp_path, run_id='cross-cell-diagnostic', execute=True, runtime=timeout)
    assert query_runner_evidence(tmp_path)['evidence_valid']
    index_path = tmp_path / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    first, foreign = index['packets'][:2]
    packet_path = tmp_path / first['path']
    original = packet_path.read_bytes()
    other = json.loads((tmp_path / foreign['path']).read_text())
    for name in ('runtime_result.json', 'stderr.log'):
        packet = json.loads(original)
        packet['runner_diagnostic']['artifacts'] = [
            ref for ref in other['runner_diagnostic']['artifacts']
            if Path(ref['path']).name == name]
        assert packet['runner_diagnostic']['artifacts']
        packet_path.write_text(json.dumps(packet))
        first['sha256'] = raw_sha256(packet_path)
        index_path.write_text(json.dumps(index))
        queried = query_runner_evidence(tmp_path)
        assert queried['evidence_valid'] is False, (name, queried)
        assert any('diagnostic_cell_mismatch' in error for error in queried['errors'])


@pytest.mark.parametrize('damage', ['substitute', 'corrupt_known'])
def test_query_binds_raw_to_retained_report_and_runtime(tmp_path, damage):
    from test_f1_exploratory_report import plan
    from test_feedback_epoch_runner import adapter_runtime
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    cycle, _ = plan(tmp_path, runtime=adapter_runtime)
    root = Path(cycle['report_path']).parent
    assert query_runner_evidence(root)['evidence_valid']
    report_bytes = Path(cycle['report_path']).read_bytes()
    runtime_path = Path(cycle['rows'][0]['runtime_result_path'])
    runtime_bytes = runtime_path.read_bytes()
    index_path = root / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    ref = index['packets'][0]
    packet_path = root / ref['path']
    packet = json.loads(packet_path.read_text())
    raw = Path(cycle['rows'][0]['raw_path'])
    if damage == 'substitute':
        raw = runtime_path.parent / 'foreign-raw.jsonl'
    raw.write_text('{"run_id":"foreign-run","cell_id":"foreign-cell"}\n')
    packet['raw'] = {'raw_path': str(raw), 'raw_sha256': raw_sha256(raw)}
    for artifact in packet['runner_diagnostic']['artifacts']:
        if (root / artifact['path']).resolve() == raw.resolve():
            artifact['sha256'] = raw_sha256(raw)
    packet_path.write_text(json.dumps(packet))
    ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'] is False, queried
    assert any('raw_binding_mismatch' in error for error in queried['errors'])
    assert Path(cycle['report_path']).read_bytes() == report_bytes
    assert runtime_path.read_bytes() == runtime_bytes


@pytest.mark.parametrize('field', ['target_sha256', 'rule_sha256'])
def test_query_binds_definition_hashes_to_pre_action_identity(tmp_path, field):
    from test_f1_exploratory_report import plan
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    cycle, _ = plan(tmp_path)
    root = Path(cycle['report_path']).parent
    assert query_runner_evidence(root)['evidence_valid']
    index_path = root / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    ref = index['packets'][0]
    packet_path = root / ref['path']
    packet = json.loads(packet_path.read_text())
    packet['identity'][field] = '0' * 64
    packet_path.write_text(json.dumps(packet))
    ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'] is False, queried
    assert any('definition_mismatch:' + field in error for error in queried['errors'])


@pytest.mark.parametrize('entry', ['required', 'exploratory'])
def test_returned_timeout_names_missing_process_evidence(tmp_path, entry):
    from test_f1_exploratory_report import plan
    from r2f.reports.candidate_report import query_runner_evidence

    def timeout(config, directory):
        return {'real_execution': False, 'runtime_status': 'failed',
                'reason': 'timeout waiting for reader'}

    if entry == 'required':
        run_campaign(tmp_path, run_id='returned-timeout', execute=True, runtime=timeout)
        root = tmp_path
    else:
        cycle, _ = plan(tmp_path, runtime=timeout)
        root = Path(cycle['report_path']).parent
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'], queried
    for packet in queried['packets']:
        diagnostic = packet['runner_diagnostic']
        assert diagnostic['kind'] == 'timeout'
        assert diagnostic['error'] is None
        for field in ('stdout', 'stderr', 'exit_code'):
            assert diagnostic['missing_evidence'].get(field), diagnostic
            assert 'runner_diagnostic.' + field in packet['incomplete_fields']
        assert diagnostic['cleanup'] == {'status': 'unknown', 'reason': 'no_cleanup_receipt'}
        assert diagnostic['candidate_eligible'] is False


@pytest.mark.parametrize('failure_at', ['required_capsule_timeout', 'required_capsule_raw',
                                      'feedback', 'exploratory_capsule'])
def test_post_runtime_packaging_failure_keeps_original_evidence_queryable(tmp_path, monkeypatch, failure_at):
    import os
    from test_campaign_evidence import fixture_pair
    from test_f1_exploratory_report import plan
    from test_feedback_epoch_runner import adapter_runtime
    from r2f.reports.candidate_report import query_runner_evidence

    started = False
    originals = {}
    root = tmp_path if failure_at.startswith('required') else tmp_path / 'run'
    original_write = Path.write_text
    original_replace = os.replace
    detail = 'fixture denied post-runtime ' + failure_at

    def deny_write(path, *args, **kwargs):
        if started and failure_at.startswith('required') and path.name == 'capsule.json':
            raise OSError(detail)
        return original_write(path, *args, **kwargs)

    def deny_replace(source, destination, *args, **kwargs):
        if started and ((failure_at == 'feedback' and Path(destination) == root / 'feedback.json')
                        or (failure_at == 'exploratory_capsule' and Path(destination).name == 'capsule.json')):
            raise OSError(detail)
        return original_replace(source, destination, *args, **kwargs)

    def runtime(config, directory):
        nonlocal started
        if failure_at == 'required_capsule_timeout':
            path = Path(directory) / 'stderr.log'
            path.write_text('original timeout detail')
            originals[path] = path.read_bytes()
            started = True
            raise TimeoutError('original worker timed out')
        result = (fixture_pair(config, directory, fresh_matches=True)
                  if failure_at.startswith('required') else adapter_runtime(config, directory))
        raw = Path(result['raw_path'])
        originals[raw] = raw.read_bytes()
        started = True
        return result

    monkeypatch.setattr(Path, 'write_text', deny_write)
    monkeypatch.setattr(os, 'replace', deny_replace)
    if failure_at.startswith('required'):
        result = run_campaign(root, run_id='post-runtime-export', execute=True, runtime=runtime)
    else:
        result, _ = plan(tmp_path, runtime=runtime)
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'], queried
    assert queried['identities'] and queried['packets']
    assert (root / 'report.json').is_file()
    failure, = [problem for problem in queried['tool_problems'] if problem.get('detail') == detail]
    assert failure['kind'] == 'packaging_failure'
    assert failure['error_type'] == 'OSError'
    assert failure['path'].endswith('feedback.json' if failure_at == 'feedback' else 'capsule.json')
    assert result['anomaly_intake']['tool_problems'] == queried['tool_problems']
    assert all(path.read_bytes() == data for path, data in originals.items())
    if failure_at == 'required_capsule_timeout':
        packet = queried['packets'][0]
        assert packet['runner_diagnostic']['kind'] == 'timeout'
        assert packet['runner_diagnostic']['error']['detail'] == 'original worker timed out'
        assert packet['runner_diagnostic']['actual_run'] is None
    for packet in queried['packets']:
        assert packet['runner_diagnostic']['candidate_eligible'] is False
        assert packet['runner_diagnostic']['replay_status'] == 'not_run'
        assert packet['runner_diagnostic']['cleanup']['status'] == 'unknown'


def test_query_rejects_runtime_embedded_identity_even_with_updated_artifact_hash(tmp_path):
    from test_f1_exploratory_report import plan
    from test_feedback_epoch_runner import adapter_runtime
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    cycle, _ = plan(tmp_path, runtime=adapter_runtime)
    root = Path(cycle['report_path']).parent
    assert query_runner_evidence(root)['evidence_valid']
    path = Path(cycle['rows'][0]['runtime_result_path'])
    runtime = json.loads(path.read_text())
    runtime['execution_metadata']['run_id'] = 'foreign-run'
    path.write_text(json.dumps(runtime))
    index_path = root / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    ref = index['packets'][0]
    packet_path = root / ref['path']
    packet = json.loads(packet_path.read_text())
    for artifact in packet['runner_diagnostic']['artifacts']:
        if (root / artifact['path']).resolve() == path.resolve():
            artifact['sha256'] = raw_sha256(path)
    packet_path.write_text(json.dumps(packet))
    ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(root)
    assert queried['evidence_valid'] is False, queried
    assert any('runtime_identity_mismatch:run_id' in error for error in queried['errors'])


def test_query_checks_known_runtime_raw_when_report_export_and_artifact_list_are_absent(tmp_path, monkeypatch):
    from test_campaign_evidence import fixture_pair
    from r2f.core.evidence import raw_sha256
    from r2f.reports.candidate_report import query_runner_evidence

    original_link = __import__('os').link

    def deny_report(source, destination, *args, **kwargs):
        if Path(destination).name == 'report.json':
            raise OSError('fixture report export denied')
        return original_link(source, destination, *args, **kwargs)

    monkeypatch.setattr('os.link', deny_report)
    run_campaign(tmp_path, run_id='runtime-raw-anchor', execute=True,
                 runtime=lambda config, directory: fixture_pair(config, directory, fresh_matches=True))
    assert query_runner_evidence(tmp_path)['evidence_valid']
    index_path = tmp_path / 'evidence-index.json'
    index = json.loads(index_path.read_text())
    assert index['reports'] == []
    ref = index['packets'][0]
    packet_path = tmp_path / ref['path']
    packet = json.loads(packet_path.read_text())
    raw = Path(packet['raw']['raw_path']).with_name('foreign-raw.jsonl')
    raw.write_text('{"run_id":"foreign"}\n')
    packet['raw'] = {'raw_path': str(raw), 'raw_sha256': raw_sha256(raw)}
    packet['runner_diagnostic']['artifacts'] = []
    packet_path.write_text(json.dumps(packet))
    ref['sha256'] = raw_sha256(packet_path)
    index_path.write_text(json.dumps(index))
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'] is False, queried
    assert any('raw_binding_mismatch' in error for error in queried['errors'])


@pytest.mark.parametrize('partial', [b'{"partial_result":', b'[]', b'null'])
def test_unreadable_worker_result_retains_diagnostics_without_qualification(tmp_path, partial):
    import subprocess
    from r2f.reports.candidate_report import query_runner_evidence

    originals = {}

    def worker(config, directory):
        path = Path(directory) / 'runtime_result.json'
        path.write_bytes(partial)
        originals[path] = partial
        (Path(directory) / 'stderr.log').write_text('worker exited during result publication')
        raise subprocess.CalledProcessError(17, ['fixture-worker'], stderr='original worker exit 17')

    report = run_campaign(tmp_path, run_id='intake-unreadable-result', execute=True, runtime=worker)
    evidence = query_runner_evidence(tmp_path)
    assert evidence['evidence_valid'] is False
    assert any('runner_evidence_runtime_unreadable:' in error for error in evidence['errors'])
    assert len(evidence['packets']) == len(report['rows']) == 24
    assert len(evidence['identities']) == 24
    partials = [problem for problem in evidence['tool_problems']
                if problem['kind'] == 'unreadable_runtime_result']
    assert len(partials) == 24
    assert {problem['cell_id'] for problem in partials} == {row['cell_id'] for row in report['rows']}
    for packet in evidence['packets']:
        diagnostic = packet['runner_diagnostic']
        assert diagnostic['kind'] == 'worker_exit'
        assert diagnostic['error']['returncode'] == 17
        assert diagnostic['candidate_eligible'] is False
        assert diagnostic['actual_run'] is None
        assert packet['raw']['raw_path'] is None
    assert all(row['actual_run'] is False for row in report['rows'])
    assert all(path.read_bytes() == original for path, original in originals.items())
