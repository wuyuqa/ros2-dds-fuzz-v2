"""CP3 public runner/query/replay seam; injected fixtures grant no ROS evidence."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.runner import run_campaign
from r2f.reports.candidate_report import query_runner_evidence


def test_plan_only_has_queryable_replay_limitations_and_never_invokes_runtime(tmp_path):
    def forbidden(config, directory):
        pytest.fail('plan-only must not invoke a runtime')

    original = tmp_path / 'original'
    report = run_campaign(original, run_id='auto-plan', replay_runtime=forbidden,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    summary = evidence['automatic_replay']
    assert len(summary['entries']) == len(report['rows']) == 24
    assert {entry['reason_code'] for entry in summary['entries']} == {'original_execution_not_requested'}
    assert all(entry['candidate_eligible'] is False for entry in summary['entries'])
    assert summary['original_artifacts_unchanged'] is True
    assert json.loads((original / 'report.json').read_text()) == report
    assert not (tmp_path / 'attempts').exists()

@pytest.fixture
def frozen_fixture_source(monkeypatch):
    """Mock filesystem source observation only, never runtime qualification."""
    from r2f.campaign import replay, runner
    source = {'source_repo': str(Path(__file__).resolve().parents[1]),
              'head': 'offline-auto-replay-fixture-head', 'behavior_tree_sha256': 'a' * 64,
              'source_frozen': True, 'dirty_behavior_paths': []}
    monkeypatch.setattr(runner, 'source_manifest', lambda repo: deepcopy(source))
    monkeypatch.setattr(replay, 'source_manifest', lambda repo: deepcopy(source))
    return source


def test_normal_fixture_runner_replays_independently_and_preserves_published_originals(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy
    from r2f.campaign.replay import audit_replay

    original = tmp_path / 'original'
    snapshots = {}

    def independent_fixture(config, directory):
        assert config['domain_id'] == 81
        assert not Path(directory).is_relative_to(original)
        snapshots.update({p: p.read_bytes() for p in original.rglob('*') if p.is_file()})
        # These were published before the automatic runtime call.
        assert original / 'evidence-index.json' in snapshots
        assert original / 'report.json' in snapshots
        return healthy(config, directory)

    report = run_campaign(original, run_id='auto-normal', execute=True, runtime=healthy,
                          domain_id=80, replay_runtime=independent_fixture,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    completed = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_report')]
    assert len(completed) == 1
    entry, = completed
    replay = json.loads(Path(entry['replay_report']['path']).read_text())
    assert replay['status'] == 'pass', replay
    assert replay['oracle_verdict'] == 'pass'
    assert replay['execution_origin'] == 'injected_runtime'
    assert replay['real_ros_execution'] is False
    assert replay['formal_runtime'] is False
    assert audit_replay(replay['report_path'])['valid'] is True
    assert {p: p.read_bytes() for p in snapshots} == snapshots
    assert json.loads((original / 'report.json').read_text()) == report
    assert json.loads((original / 'candidate_ledger.json').read_text())['natural_candidate_count'] == 0

def test_query_rejects_corrupted_independent_attempt_without_touching_originals(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    run_campaign(original, run_id='auto-corrupt-attempt', execute=True, runtime=healthy,
                 domain_id=80, replay_runtime=healthy,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    before = query_runner_evidence(original)
    assert before['evidence_valid'], before
    entry, = [e for e in before['automatic_replay']['entries'] if e.get('replay_attempt')]
    originals = {p: p.read_bytes() for p in original.rglob('*') if p.is_file()}
    Path(entry['replay_attempt']['path']).write_text('{}\n')
    after = query_runner_evidence(original)
    assert after['evidence_valid'] is False, after
    assert any('replay' in error for error in after['errors'])
    assert {p: p.read_bytes() for p in originals} == originals

def test_exploratory_runner_publishes_replay_limitations_after_immutable_cycle(
        tmp_path, frozen_fixture_source):
    from r2f.campaign.runner import run_exploratory_cycle
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler
    from test_fuzz_exploratory_targets import _corpus, _profile, LATE

    corpus = _corpus(LATE, 'P01', boundary='finite', mutator_id='M03')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=42)
    original = tmp_path / 'exploratory'
    cycle = run_exploratory_cycle(original, corpus=corpus, feedback=feedback,
                                  scheduler=scheduler, profile=_profile(LATE, ['P01']),
                                  max_inputs=1, auto_replay={'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = evidence['automatic_replay']['entries']
    assert entry['reason_code'] == 'original_execution_not_requested'
    assert json.loads((original / 'cycle.json').read_text()) == cycle
    assert any(ref['path'] == 'cycle.json' for ref in evidence['automatic_replay']['original_artifacts'])

def test_later_pass_is_inconclusive_and_retains_original_failure_and_zero_fixture_candidates(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy, missing_middle
    from r2f.campaign.replay import audit_replay

    original = tmp_path / 'original'
    snapshots = {}

    def later_health(config, directory):
        snapshots.update({p: p.read_bytes() for p in original.rglob('*') if p.is_file()})
        return healthy(config, directory)

    report = run_campaign(original, execute=True, runtime=missing_middle, domain_id=80,
                          replay_runtime=later_health,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_report')]
    replay = json.loads(Path(entry['replay_report']['path']).read_text())
    assert replay['oracle_verdict'] == 'pass'
    assert replay['status'] == 'inconclusive'
    assert replay['comparison_matches_original'] is False
    original_row, = [row for row in report['rows'] if row['cell_id'] == entry['cell_id']]
    assert original_row['oracle_verdict'] == 'fail'
    assert entry['original_confirmed'] is False
    assert entry['internal_state_recovery'] is False
    assert audit_replay(replay['report_path'])['valid']
    assert {p: p.read_bytes() for p in snapshots} == snapshots
    assert json.loads((original / 'candidate_ledger.json').read_text())['natural_candidate_count'] == 0


@pytest.mark.parametrize('fault,expected', [
    ('same_domain', 'fresh_domain_required'),
    ('wrong_source', 'source_identity_mismatch'),
    ('unfrozen_source', 'source_not_frozen'),
    ('tool_error', 'runtime_tool_failure'),
    ('domain_conflict', 'runtime_preflight_blocked'),
    ('cleanup_missing', 'ingestion_rejected'),
])
def test_replay_gate_failures_have_queryable_independent_attempts(
        tmp_path, frozen_fixture_source, monkeypatch, fault, expected):
    from test_campaign_replay import healthy, blocked
    from r2f.campaign import replay

    original = tmp_path / 'original'
    observed = {}
    if fault in {'wrong_source', 'unfrozen_source'}:
        wrong = dict(frozen_fixture_source)
        wrong.update({'head': 'other-head'} if fault == 'wrong_source' else {'source_frozen': False})
        monkeypatch.setattr(replay, 'source_manifest', lambda repo: deepcopy(wrong))

    def failed_replay(config, directory):
        observed.update({p: p.read_bytes() for p in original.rglob('*') if p.is_file()})
        if fault == 'tool_error':
            raise RuntimeError('offline injected replay tool failed')
        if fault == 'domain_conflict':
            return blocked(config, directory)
        if fault == 'cleanup_missing':
            return {'runtime_status': 'completed', 'real_execution': True, 'events': [],
                    'evidence_origin': 'offline', 'reason': 'fixture lacks cleanup and raw'}
        pytest.fail('fresh-domain/source preflight must reject before runtime')

    run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                 replay_runtime=failed_replay,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [80 if fault == 'same_domain' else 81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_attempt')]
    assert entry['reason_code'] == expected, entry
    attempt = json.loads(Path(entry['replay_attempt']['path']).read_text())
    assert attempt['reason_code'] == expected
    assert entry['candidate_eligible'] is False
    assert entry['real_ros_execution'] is False
    assert {p: p.read_bytes() for p in observed} == observed

@pytest.mark.parametrize('missing,expected', [
    ('budget', 'replay_budget_not_declared'),
    ('domain', 'fresh_domain_not_configured'),
    ('output', 'replay_output_not_configured'),
    ('fixture_runtime', 'fixture_replay_runtime_not_supplied'),
])
def test_missing_replay_configuration_remains_queryable_without_implicit_live_calls(
        tmp_path, frozen_fixture_source, missing, expected):
    from test_campaign_replay import healthy

    policy = {'output_dir': str(tmp_path / 'attempts'), 'domain_ids': [81], 'max_attempts': 1}
    for key, name in [('max_attempts', 'budget'), ('domain_ids', 'domain'), ('output_dir', 'output')]:
        if missing == name:
            del policy[key]

    def forbidden(config, directory):
        pytest.fail('insufficient policy must not invoke replay')

    original = tmp_path / 'original'
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, auto_replay=policy,
                 replay_runtime=None if missing == 'fixture_runtime' else forbidden)
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    first = evidence['automatic_replay']['entries'][0]
    assert first['reason_code'] == expected
    assert first['status'] == 'not_run'
    assert not (tmp_path / 'attempts').exists()


def test_missing_original_report_after_packaging_failure_is_named_without_replay(
        tmp_path, frozen_fixture_source, monkeypatch):
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    import os
    original_link = os.link

    def packaging_failure(src, dst, *args, **kwargs):
        if Path(dst) == original / 'report.json':
            raise OSError('offline fixture cannot export original report')
        return original_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, 'link', packaging_failure)
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=healthy,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    assert evidence['tool_problems'][0]['kind'] == 'packaging_failure'
    assert {e['reason_code'] for e in evidence['automatic_replay']['entries']} == {'original_report_unavailable'}
    assert not (tmp_path / 'attempts').exists()


def test_index_export_failure_is_queryable_instead_of_crashing_automatic_dispatch(
        tmp_path, frozen_fixture_source, monkeypatch):
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    import os
    original_link = os.link

    def index_failure(src, dst, *args, **kwargs):
        if Path(dst) == original / 'evidence-index.json':
            raise OSError('offline fixture index export failure')
        return original_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, 'link', index_failure)
    report = run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=healthy,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'] is False
    assert report['anomaly_intake']['tool_problems'][-1]['kind'] == 'evidence_index_export_failure'
    assert {e['reason_code'] for e in evidence['automatic_replay']['entries']} == {'original_evidence_invalid'}
    assert not (tmp_path / 'attempts').exists()

def test_automatic_attempts_require_distinct_domains_and_never_reuse_attempt_paths(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy

    calls = []

    def offline_replay(config, directory):
        calls.append((config['domain_id'], str(directory)))
        return healthy(config, directory)

    original = tmp_path / 'original'
    run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                 replay_runtime=offline_replay,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81, 81], 'max_attempts': 2})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    assert len(calls) == 1
    assert evidence['automatic_replay']['entries'][1]['reason_code'] == 'fresh_domain_reused'

def test_two_configured_attempts_use_distinct_outputs_and_fresh_domains(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=healthy,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81, 82], 'max_attempts': 2})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entries = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_attempt')]
    assert len(entries) == 2
    assert evidence['automatic_replay']['policy']['max_attempts'] == 2
    requests = [json.loads((original / e['request']['path']).read_text()) for e in entries]
    assert {request['domain_id'] for request in requests} == {81, 82}
    assert [request['attempt_ordinal'] for request in requests] == [1, 2]
    assert all(request['max_attempts'] == 2 for request in requests)
    assert len({request['output_dir'] for request in requests}) == 2
    assert len({e['replay_attempt']['path'] for e in entries}) == 2
    assert all(e['original_artifacts_unchanged'] is True for e in entries)


def test_original_worker_timeout_can_replay_without_being_reclassified_as_a_sut_candidate(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy

    def worker_timeout(config, directory):
        raise TimeoutError('offline original worker timeout')

    original = tmp_path / 'original'
    report = run_campaign(original, execute=True, runtime=worker_timeout, domain_id=80,
                          replay_runtime=healthy,
                          auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                       'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_report')]
    assert entry['real_ros_execution'] is False
    assert entry['candidate_eligible'] is False
    assert report['rows'][0]['execution_stage'] == 'aborted'
    assert evidence['packets'][0]['runner_diagnostic']['kind'] == 'timeout'
    assert entry['capsule_binding'] == {
        'input_bound': True, 'execution_bound': False,
        'reason_code': 'original_capsule_execution_unavailable'}
    assert json.loads((original / 'report.json').read_text()) == report
    assert json.loads((original / 'candidate_ledger.json').read_text())['natural_candidate_count'] == 0


def test_replay_source_change_is_named_and_preserves_original_artifact_hashes(
        tmp_path, frozen_fixture_source, monkeypatch):
    from test_campaign_replay import healthy
    from r2f.campaign import replay

    original = tmp_path / 'original'
    snapshots = {}

    def source_changes(config, directory):
        snapshots.update({p: p.read_bytes() for p in original.rglob('*') if p.is_file()})
        wrong = dict(frozen_fixture_source, behavior_tree_sha256='b' * 64)
        monkeypatch.setattr(replay, 'source_manifest', lambda repo: deepcopy(wrong))
        return healthy(config, directory)

    run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                 replay_runtime=source_changes,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_report')]
    assert entry['reason_code'] == 'source_identity_changed'
    assert entry['real_ros_execution'] is False
    assert {p: p.read_bytes() for p in snapshots} == snapshots


def test_nonindependent_output_has_durable_request_and_named_dispatch_failure(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=healthy,
                 auto_replay={'output_dir': str(original / 'unsafe-attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('request')]
    assert entry['reason_code'] == 'replay_dispatch_failed'
    assert entry['error_type'] == 'ValueError'
    assert 'independent' in entry['detail']
    assert not entry.get('replay_report')
    assert (original / entry['request']['path']).is_file()
    assert (original / entry['outcome']['path']).is_file()


def test_changed_original_artifact_is_detected_and_prevents_later_automatic_calls(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    calls = []

    def corrupting_fixture(config, directory):
        calls.append(config['domain_id'])
        # A deliberately faulty offline tool; the query must expose corruption.
        (original / 'report.json').write_text('{"corrupted_by_offline_fixture": true}\n')
        return healthy(config, directory)

    run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                 replay_runtime=corrupting_fixture,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81, 82], 'max_attempts': 2})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'] is False
    summary = evidence['automatic_replay']
    assert summary['original_artifacts_unchanged'] is False
    assert summary['entries'][0]['reason_code'] == 'original_artifact_changed'
    assert summary['entries'][1]['reason_code'] == 'original_artifact_changed'
    assert calls == [81]


def test_callable_injection_cannot_invoke_the_canonical_real_runtime(
        tmp_path, frozen_fixture_source):
    from test_campaign_replay import healthy
    from r2f.runtime.ros2_pair import run_pair

    original = tmp_path / 'original'
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=run_pair,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('request')]
    assert entry['status'] == 'scheduling_failed'
    assert 'offline-only' in entry['detail']
    assert not (tmp_path / 'attempts').exists()

def test_failed_dispatch_request_export_is_named_and_cannot_start_replay(
        tmp_path, frozen_fixture_source, monkeypatch):
    import os
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    original_link = os.link

    def disk_failure(src, dst, *args, **kwargs):
        if Path(dst).name == 'request.json' and 'replay-scheduling' in Path(dst).parts:
            raise OSError('offline request export failed')
        return original_link(src, dst, *args, **kwargs)

    def forbidden(config, directory):
        pytest.fail('replay cannot start without its durable dispatch request')

    monkeypatch.setattr(os, 'link', disk_failure)
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=forbidden,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    assert evidence['automatic_replay']['entries'][0]['reason_code'] == 'replay_request_export_failed'
    assert not (tmp_path / 'attempts').exists()

@pytest.mark.parametrize('failed_export', ['outcome.json', 'replay-summary.json', 'replay_report.json'])
def test_replay_packaging_failure_is_queryable_and_keeps_original_report(
        tmp_path, frozen_fixture_source, monkeypatch, failed_export):
    import os
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    original_link, original_open = os.link, Path.open

    def fail_link(src, dst, *args, **kwargs):
        if Path(dst).name == failed_export:
            raise OSError('offline replay packaging failure:' + failed_export)
        return original_link(src, dst, *args, **kwargs)

    def fail_open(path, *args, **kwargs):
        if path.name == failed_export and args and args[0] == 'x':
            raise OSError('offline replay packaging failure:' + failed_export)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(os, 'link', fail_link)
    monkeypatch.setattr(Path, 'open', fail_open)
    snapshots = {}

    def independent_fixture(config, directory):
        snapshots.update({p: p.read_bytes() for p in original.rglob('*') if p.is_file()})
        return healthy(config, directory)

    run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                 replay_runtime=independent_fixture,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    assert snapshots
    assert {p: p.read_bytes() for p in snapshots} == snapshots
    evidence = query_runner_evidence(original)
    summary = evidence['automatic_replay']
    if failed_export == 'replay_report.json':
        assert evidence['evidence_valid'], evidence
        entry, = [e for e in summary['entries'] if e.get('request')]
        assert entry['reason_code'] == 'replay_dispatch_failed'
        assert 'replay_report.json' in entry['detail']
    else:
        assert summary['reason_code'] == 'automatic_replay_attachment_failure', evidence
        assert len(summary['entries']) == 1
        assert summary['entries'][0]['candidate_eligible'] is False


def test_capsule_missing_during_packaging_is_a_named_limitation_without_fabricated_attempt(
        tmp_path, frozen_fixture_source, monkeypatch):
    import os
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    original_link = os.link
    removed = []

    def capsule_export_lost(src, dst, *args, **kwargs):
        if Path(dst) == original / 'report.json':
            capsule = sorted((original / 'cells').glob('*/capsule.json'))[0]
            removed.append(capsule)
            capsule.unlink()
        return original_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, 'link', capsule_export_lost)
    run_campaign(original, execute=True, runtime=healthy, domain_id=80, replay_runtime=healthy,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    missing_cell = removed[0].parent.name
    entry, = [e for e in evidence['automatic_replay']['entries'] if e['cell_id'] == missing_cell]
    assert entry['reason_code'] == 'original_capsule_unavailable'
    assert 'replay_report' not in entry


def test_wrong_cell_capsule_is_rejected_before_automatic_runtime(
        tmp_path, frozen_fixture_source, monkeypatch):
    import os
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    publish = os.link
    changed = {}
    calls = []

    def wrong_capsule(src, dst, *args, **kwargs):
        if Path(dst) == original / 'report.json':
            first, donor = json.loads(Path(src).read_text())['rows'][:2]
            changed['cell'] = first['cell_id']
            Path(first['replay_path']).write_bytes(Path(donor['replay_path']).read_bytes())
        return publish(src, dst, *args, **kwargs)

    def replay_fixture(config, directory):
        calls.append(config['repeat'])
        return healthy(config, directory)

    monkeypatch.setattr(os, 'link', wrong_capsule)
    run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                 replay_runtime=replay_fixture,
                 auto_replay={'output_dir': str(tmp_path / 'attempts'),
                              'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    entry, = [e for e in evidence['automatic_replay']['entries']
              if e['cell_id'] == changed['cell']]
    assert entry['reason_code'] == 'original_capsule_identity_mismatch', evidence
    assert 'replay_report' not in entry
    assert calls == [1]  # Only the next, correctly bound repeat may consume budget.
    assert evidence['evidence_valid'], evidence


def _binding_campaign(original, schema, replay_runtime, **options):
    """Use the two agreed public runner seams with offline injected runtimes."""
    from test_campaign_replay import healthy
    if schema == 'v1':
        return run_campaign(original, execute=True, runtime=healthy, domain_id=80,
                            replay_runtime=replay_runtime, **options)
    from r2f.campaign.runner import run_exploratory_cycle
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler
    from test_fuzz_exploratory_targets import _corpus, _profile, LATE

    corpus = _corpus(LATE, 'P01', boundary='finite', mutator_id='M03')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=42)
    return run_exploratory_cycle(original, corpus=corpus, feedback=feedback,
                                 scheduler=scheduler, profile=_profile(LATE, ['P01']),
                                 max_inputs=1, execute=True, runtime=healthy,
                                 domain_id=80, replay_runtime=replay_runtime, **options)


def _misbound_capsule(document, fault):
    """Create valid but wrongly associated artifacts, without forging ROS facts."""
    from r2f.core import ReplayCapsule, Scenario

    document = deepcopy(document)
    if fault.startswith('execution.'):
        key = fault.split('.')[1]
        if key in ('raw_path', 'raw_sha256'):
            from r2f.core import raw_sha256
            raw = Path(document['execution']['raw_path'])
            alternate = raw.with_name('other-fixture-' + raw.name)
            alternate.write_bytes(raw.read_bytes() + (b'\n' if key == 'raw_sha256' else b''))
            document['execution'].update(raw_path=str(alternate), raw_sha256=raw_sha256(alternate))
        else:
            document['execution'][key] = 'offline-other-execution'
    elif fault == 'input_only':
        document['execution'] = None
    else:
        data = document['input']
        scenario = deepcopy(data['scenario'])
        source = deepcopy(data['source_manifest'])
        final = deepcopy(data['effective_input']['final_input'])
        if fault == 'source':
            source['head'] = 'offline-other-source'
        elif fault == 'rule':
            scenario['target']['rule_contract']['primary_rule'] = 'OFFLINE-OTHER-RULE'
        elif fault == 'target':
            scenario['target']['target_id'] = 'offline.other_target'
        elif fault == 'target_revision':
            scenario['target']['target_revision'] += 1
        elif fault == 'input':
            final['post_match_count'] = final.get('post_match_count', 4) + 1
        else:
            scenario[fault] = {'repeat': scenario['repeat'] + 1,
                               'arm': 'transparent', 'rmw': 'rmw_fastrtps_cpp'}[fault]
        document = ReplayCapsule.from_scenario(
            Scenario.from_dict(scenario), source_manifest=source, final_input=final,
            input_layer_receipts=data['effective_input']['input_layer_receipts'],
            origin_domain=data['origin_domain']).to_dict()
    # Each wrong artifact is internally valid; integrity alone cannot reject it.
    ReplayCapsule.from_dict(document)
    return document


@pytest.mark.parametrize('schema,fault,reason', [
    ('v1', fault, 'original_capsule_identity_mismatch')
    for fault in ('source', 'rule', 'target', 'target_revision', 'input', 'repeat', 'arm', 'rmw',
                  'execution.run_id', 'execution.cell_id', 'execution.raw_path', 'execution.raw_sha256')
] + [
    ('v1', 'input_only', 'original_capsule_execution_unavailable'),
    ('v1', 'corrupt', 'original_capsule_invalid'),
    ('v2', 'repeat', 'original_capsule_identity_mismatch'),
    ('v2', 'execution.cell_id', 'original_capsule_identity_mismatch'),
    ('v2', 'input_only', 'original_capsule_execution_unavailable'),
])
def test_dispatch_binds_capsule_content_and_execution_to_sealed_intake(
        tmp_path, frozen_fixture_source, monkeypatch, schema, fault, reason):
    import os
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    publish, changed, replayed = os.link, {}, []

    def replace_at_publication(src, dst, *args, **kwargs):
        if Path(dst) == original / 'report.json':
            row = json.loads(Path(src).read_text())['rows'][0]
            path = Path(row['replay_path'])
            changed.update(cell=row['cell_id'], input=row['effective_input_sha256'])
            if fault == 'corrupt':
                path.write_text('{')
            else:
                replacement = _misbound_capsule(json.loads(path.read_text()), fault)
                path.write_text(json.dumps(replacement))
        return publish(src, dst, *args, **kwargs)

    def replay_fixture(config, directory):
        replayed.append(config['effective_input_sha256'])
        return healthy(config, directory)

    monkeypatch.setattr(os, 'link', replace_at_publication)
    _binding_campaign(original, schema, replay_fixture,
                      auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                   'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    entry, = [e for e in evidence['automatic_replay']['entries'] if e['cell_id'] == changed['cell']]
    assert entry['reason_code'] == reason, evidence
    assert 'request' not in entry and 'replay_report' not in entry
    assert changed['input'] not in replayed
    assert evidence['evidence_valid'], evidence
    assert entry['candidate_eligible'] is False


@pytest.mark.parametrize('schema', ['v1', 'v2'])
def test_correct_capsule_keeps_original_execution_binding_without_ros_credit(
        tmp_path, frozen_fixture_source, schema):
    from test_campaign_replay import healthy
    original = tmp_path / 'original'
    _binding_campaign(original, schema, healthy,
                      auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                   'domain_ids': [81], 'max_attempts': 1})
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_report')]
    assert entry['capsule_binding'] == {'input_bound': True, 'execution_bound': True,
                                        'reason_code': 'original_execution_bound'}
    assert entry['real_ros_execution'] is False
    assert entry['original_confirmed'] is False


@pytest.mark.parametrize('schema,fault', [
    ('v1', 'repeat'), ('v1', 'execution.run_id'), ('v1', 'execution.cell_id'),
    ('v1', 'input_only'), ('v1', 'source'), ('v1', 'rule'),
    ('v1', 'execution.raw_path'), ('v1', 'execution.raw_sha256'),
    ('v2', 'repeat'), ('v2', 'execution.cell_id'),
])
def test_query_independently_rejects_self_consistent_rehashed_wrong_association(
        tmp_path, frozen_fixture_source, schema, fault):
    from r2f.core import canonical_hash, raw_sha256
    from r2f.campaign.replay import replay_cell, audit_replay
    from r2f.reports.candidate_report import query_automatic_replays
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    _binding_campaign(original, schema, healthy,
                      auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                   'domain_ids': [81], 'max_attempts': 1})
    assert query_runner_evidence(original)['evidence_valid']
    summary_path = original / 'replay-summary.json'
    summary = json.loads(summary_path.read_text())
    entry, = [e for e in summary['entries'] if e.get('replay_report')]
    request = json.loads((original / entry['request']['path']).read_text())
    capsule_path = original / request['capsule']['path']
    capsule_path.write_text(json.dumps(_misbound_capsule(json.loads(capsule_path.read_text()), fault)))

    # Build an internally valid independent replay via the public replay seam.
    # Then rehash the derived association, leaving sealed intake/report untouched.
    # Neither dispatch's prior decision nor these consistent hashes prove binding.
    dispatch = 'offline-forged-association'
    output = tmp_path / 'attempts' / dispatch
    replay = replay_cell(capsule_path, output_dir=output, domain_id=81, runtime=healthy)
    assert audit_replay(replay['report_path'])['valid']
    request.update(dispatch_id=dispatch, output_dir=str(output))
    request['capsule']['sha256'] = raw_sha256(capsule_path)
    directory = original / 'replay-scheduling' / dispatch
    directory.mkdir()
    (directory / 'request.json').write_text(json.dumps(request))
    entry.update(dispatch_id=dispatch, status=replay['status'],
                 reason_code=replay.get('reason_code') or 'replay_completed')
    for key in ('runtime_invoked', 'real_runtime_attempt', 'real_ros_execution', 'qualification_errors'):
        entry[key] = replay[key]
    for key, path in [('request', directory / 'request.json'),
                      ('replay_report', output / 'replay_report.json'),
                      ('replay_attempt', output / 'attempt.json')]:
        entry[key] = {'path': str(path.relative_to(original)) if key == 'request' else str(path),
                      'sha256': raw_sha256(path)}
    (directory / 'outcome.json').write_text(json.dumps({k: v for k, v in entry.items() if k != 'outcome'}))
    entry['outcome'] = {'path': str((directory / 'outcome.json').relative_to(original)),
                        'sha256': raw_sha256(directory / 'outcome.json')}
    for ref in summary['original_artifacts']:
        ref['sha256'] = raw_sha256(original / ref['path'])
    summary['summary_sha256'] = canonical_hash({k: v for k, v in summary.items() if k != 'summary_sha256'})
    summary_path.write_text(json.dumps(summary))
    originals = {p: p.read_bytes() for p in original.rglob('*') if p.is_file()}
    for result in (query_runner_evidence(original), query_automatic_replays(original)):
        assert any('original_capsule_identity_mismatch' in error
                   or 'original_capsule_execution_unavailable' in error for error in result['errors']), result
    assert {p: p.read_bytes() for p in originals} == originals


@pytest.mark.parametrize('schema', ['v1', 'v2'])
def test_query_rechecks_older_attachment_without_derived_binding_field(
        tmp_path, frozen_fixture_source, schema):
    from r2f.core import canonical_hash, raw_sha256
    from test_campaign_replay import healthy

    original = tmp_path / 'original'
    _binding_campaign(original, schema, healthy,
                      auto_replay={'output_dir': str(tmp_path / 'attempts'),
                                   'domain_ids': [81], 'max_attempts': 1})
    summary_path = original / 'replay-summary.json'
    summary = json.loads(summary_path.read_text())
    entry, = [e for e in summary['entries'] if e.get('replay_report')]
    del entry['capsule_binding']
    outcome = original / entry['outcome']['path']
    outcome.write_text(json.dumps({k: v for k, v in entry.items() if k != 'outcome'}))
    entry['outcome']['sha256'] = raw_sha256(outcome)
    summary['summary_sha256'] = canonical_hash({k: v for k, v in summary.items() if k != 'summary_sha256'})
    summary_path.write_text(json.dumps(summary))
    originals = {p: p.read_bytes() for p in original.rglob('*') if p.is_file()}
    evidence = query_runner_evidence(original)
    assert evidence['evidence_valid'], evidence
    entry, = [e for e in evidence['automatic_replay']['entries'] if e.get('replay_report')]
    assert entry['capsule_binding']['execution_bound'] is True
    assert {p: p.read_bytes() for p in originals} == originals
