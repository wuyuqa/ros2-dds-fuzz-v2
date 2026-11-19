"""Offline boundary gates; fixture raw is never real ROS/DDS evidence.

Source observation is explicitly mocked; the boundary auditor reads fabricated
artifacts only. No timing threshold, namespace, DDS endpoint or live campaign.
"""
from copy import deepcopy
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from r2f.campaign import runner
from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.replay import ReplayCapsule
from r2f.core.scenario import Scenario
from r2f.reports import run_report
from test_campaign_evidence import fixture_pair
from test_fuzz_audit import base_row, build_report
from test_fuzz_feedback_loop import make_corpus, profile

REPO = Path(__file__).resolve().parents[1]


def inner_success_cleanup_failure(config, directory, *, anomaly=False):
    """Realistic return contract: successful inner raw, failed outer wrapper.

    `real_execution` is a fixture assertion, not real execution in this test.
    Keep origin=offline so no test can turn this fixture into formal evidence.
    """
    result = fixture_pair(config, directory, fresh_matches=True)
    if anomaly:
        victim = config['pre_match_count'] + 1
        result['events'] = [e for e in result['events']
                            if not (e['event'] == 'receive' and e.get('send_sequence') == victim)]
        for index, event in enumerate(result['events']):
            event['event_index'] = index
        Path(result['raw_path']).write_text(''.join(json.dumps(e) + '\n' for e in result['events']))
        result['raw_sha256'] = raw_sha256(result['raw_path'])
    result.update(runtime_status='failed', status='failed',
                  reason='execution_boundary:namespace_cleanup_unconfirmed',
                  execution_boundary={'schema': 'r2f.execution-boundary/v1',
                                      'boundary_id': 'offline-boundary',
                                      'cleanup_confirmed': False,
                                      'host_clearance_claimed': False, 'maturity_credit': False})
    return result


def complete_offline_boundary(config, directory):
    """Fabricate complete v1 sidecars around valid inner raw; never run anything.

    Reuses Darwin's writer-contract fixture, but binds the real submitted test
    config. All evidence stays explicitly offline even behind a canonical mock.
    """
    from test_execution_boundary_audit import SyntheticBoundary, digest
    from r2f.runtime.ros2_pair import normalize_config
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    fixture = SyntheticBoundary(directory)
    fixture.config = deepcopy(config)
    normalized = normalize_config(deepcopy(config))
    lock_root = Path(normalized.get('lock_root', '/tmp/r2f-runtime-locks-1000'))
    fixture.inner_root = lock_root / 'execution-boundaries' / fixture.boundary_id / 'inner'

    def leases(root, state, pid, host=False):
        names = ['campaign-' + hashlib.sha256(config['campaign_id'].encode()).hexdigest() + '.lock',
                 'domain-' + str(config['domain_id']) + '.lock',
                 'state-' + hashlib.sha256(str(state).encode()).hexdigest() + '.lock']
        rows = []
        for index, name in enumerate(names):
            owner = {'pid': pid, 'campaign_id': config['campaign_id'], 'domain_id': config['domain_id'],
                     'state_dir': str(state), 'acquired_time_ns': 100 + index}
            row = {'path': str(root / name), 'owner': owner, 'acquired': True}
            if host:
                owner.update(start_ticks=fixture.host['start_ticks'],
                             pid_namespace=fixture.host['namespaces']['pid'],
                             lease_scope='host_supervisor', boundary_id=fixture.boundary_id)
                row.update(device=1, inode=100 + index)
            rows.append(row)
        return rows

    fixture.host_leases = leases(lock_root, directory, 1000, True)
    fixture.inner_leases = leases(fixture.inner_root, directory / 'inner-runtime', 1)
    inner_config = dict(normalized, lock_root=str(fixture.inner_root))
    identity = {'config_sha256': digest(config), 'source_manifest': deepcopy(config['source_manifest']),
                'target_sha256': digest(config['scenario']['target'], ensure_ascii=False),
                'effective_input_sha256': config['effective_input_sha256'], 'dependency_lock_sha256': 'a' * 64}
    fixture.request.update(config=inner_config, identity=deepcopy(identity),
                           submitted_config_sha256=digest(normalized), inner_config_sha256=digest(inner_config),
                           host_leases=deepcopy(fixture.host_leases))
    fixture.manifest.update(identity=identity, host_leases=deepcopy(fixture.host_leases),
                            inner_leases=deepcopy(fixture.inner_leases), inner_lock_root=str(fixture.inner_root),
                            source_after=deepcopy(config['source_manifest']))
    fixture.context.update(host_leases=deepcopy(fixture.host_leases),
                           inner_leases=deepcopy(fixture.inner_leases), inner_lock_root=str(fixture.inner_root))
    fixture.guardian_events[0]['receipts'] = deepcopy(fixture.host_leases)
    fixture.guardian_events[4]['config_sha256'] = digest(inner_config)
    fixture.inner = fixture_pair(config, directory / 'inner-runtime', fresh_matches=True)
    original = fixture.inner['events']
    starts, exits = [], []
    for index, worker in enumerate(fixture.inner['execution_metadata']['workers']):
        start = dict(original[0], event='process_started', role=worker['role'], pid=worker['pid'],
                     monotonic_ns=original[0]['monotonic_ns'] - 2 + index)
        start.pop('locks', None)
        starts.append(start)
        exits.append(dict(start, event='process_exit', returncode=0,
                          monotonic_ns=original[-1]['monotonic_ns'] - 2 + index))
    for event in original:
        if event.get('role') == 'controller':
            event.update(pid=1, pgid=1)
        if event['event'] == 'isolation_locks_acquired':
            event['locks'] = deepcopy(fixture.inner_leases)
        for ref in event.get('precondition_receipt_refs', []):
            for key in ('raw_before_index', 'raw_after_index'):
                ref[key] += len(starts)
    events = starts + original[:-1] + exits + original[-1:]
    for index, event in enumerate(events):
        event['event_index'] = index
    fixture.inner['events'] = events
    fixture.raw_path.write_text(''.join(json.dumps(e) + '\n' for e in events))
    fixture.inner['raw_sha256'] = raw_sha256(fixture.raw_path)
    fixture.raw_id.update(raw_path=str(fixture.raw_path), raw_sha256=fixture.inner['raw_sha256'])
    run_report.write_json(fixture.raw_path.parent / 'environment.json', fixture.inner['environment'])
    fixture.write()
    return fixture


class IsolatedReportGate(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = {'source_repo': str(REPO), 'head': 'a' * 40,
                       'behavior_tree_sha256': 'b' * 64, 'source_frozen': True,
                       'dirty_behavior_paths': []}
        for module in (runner, run_report):
            patched = patch.object(module, 'source_manifest', return_value=deepcopy(self.source))
            patched.start()
            self.addCleanup(patched.stop)

    def load(self, path):
        return json.loads(Path(path).read_text())

    def cycle(self, name, *, boundary='host', runtime=None, execute=True):
        corpus, feedback = make_corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0)
        before = (deepcopy(corpus.to_dict()), deepcopy(feedback.to_dict()))
        result = runner.run_exploratory_cycle(
            self.root / name, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=profile(), runtime=runtime, execute=execute,
            runtime_boundary=boundary, max_inputs=2, run_id='offline-gate')
        return result, corpus, feedback, scheduler, before

    def executed_required_fixture(self, name='executed', *, anomaly=False):
        report = runner.plan_campaign(self.root / name, run_id='offline-required')
        row = report['rows'][0]
        config = self.load(row['runtime_config_path'])
        capsule = ReplayCapsule.from_dict(self.load(row['replay_path']))
        result = inner_success_cleanup_failure(
            config, Path(row['runtime_result_path']).parent, anomaly=anomaly)
        execution = ingest_execution(Scenario.from_dict(capsule.input['scenario']),
                                     config, result, capsule)
        self.assertEqual(execution['execution_stage'], 'judged', execution.get('abort_reason'))
        self.assertTrue(execution['actual_run'])
        row.update(execution)
        row['actual_submitted_payload_sha256'] = canonical_hash(capsule.input['effective_input']['final_input'])
        capsule.record_execution(row)
        run_report.write_json(row['replay_path'], capsule.to_dict())
        run_report.write_json(row['runtime_result_path'], result)
        return report, row, config, result

    def test_required_isolated_plan_only_preserves_full_matrix_and_input_identity(self):
        host = runner.plan_campaign(self.root / 'host-plan', run_id='offline-required')
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime') as runtime:
            report = runner.run_campaign(self.root / 'isolated', run_id='offline-required',
                                         execute=False, runtime_boundary='isolated_netns')
        runtime.assert_not_called()
        self.assertNotIn('abort_reason', report)
        self.assertFalse(report['formal_runtime'])
        self.assertEqual(report['full_scope_required_cells'], 72)
        self.assertEqual(len(report['rows']), 24)
        for old, row in zip(host['rows'], report['rows']):
            self.assertEqual(row['execution_stage'], 'planned')
            self.assertFalse(row['actual_run'])
            self.assertEqual(row['boundary_qualification']['status'], 'unknown')
            self.assertFalse(Path(row['runtime_result_path']).exists())
            config = self.load(row['runtime_config_path'])
            self.assertEqual(config['execution_boundary'], {'enabled': True})
            self.assertEqual(self.load(old['replay_path'])['input'], self.load(row['replay_path'])['input'])
            self.assertEqual(old['effective_input_sha256'], row['effective_input_sha256'])
            self.assertEqual(config['effective_input_sha256'], row['effective_input_sha256'])
        ledger = self.load(report['candidate_ledger_path'])
        self.assertEqual(ledger['natural_candidate_count'], 0)
        self.assertEqual(ledger['candidates'], [])
        self.assertFalse(report['seal']['sealed'])
        self.assertEqual(report['seal']['required_expected'], 18)
        self.assertEqual(report['seal']['fault_expected'], 6)
        self.assertTrue(any(run_report.BOUNDARY_AUDIT_PENDING in e for e in report['seal']['errors']))

    def test_exploratory_isolated_plan_only_has_no_attempt_or_feedback(self):
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime') as runtime:
            result, corpus, feedback, scheduler, before = self.cycle(
                'isolated-cycle', boundary='isolated_netns', runtime=runtime, execute=False)
        runtime.assert_not_called()
        self.assertIsNone(result['abort'])
        self.assertEqual(len(result['rows']), 2)
        row = result['rows'][0]
        self.assertEqual(row['execution_stage'], 'planned')
        self.assertFalse(row['actual_run'])
        self.assertEqual(result['budget']['attempted'], 0)
        self.assertEqual(result['budget']['actual'], 0)
        self.assertEqual(result['budget']['executed'], 0)
        self.assertEqual(feedback.to_dict(), before[1])
        self.assertEqual(corpus.to_dict(), before[0])
        self.assertEqual(scheduler.feedback_event_count, 0)
        self.assertFalse(Path(row['runtime_result_path']).exists())
        self.assertIs(self.load(row['runtime_config_path'])['execution_boundary']['enabled'], True)
        self.assertEqual(self.load(row['replay_path'])['input']['effective_input_sha256'],
                         row['effective_input_sha256'])

    def test_required_unexpected_wrapper_preserves_actual_run_and_raw_without_candidate(self):
        runtime = Mock(side_effect=lambda c, d: inner_success_cleanup_failure(c, d, anomaly=True))
        report = runner.run_campaign(self.root / 'wrapper-required', execute=True, runtime=runtime)
        runtime.assert_called_once()
        row = report['rows'][0]
        self.assertEqual(row['execution_stage'], 'judged')
        self.assertTrue(row['actual_run'])
        self.assertEqual(row['oracle_verdict'], 'fail')
        self.assertEqual(row['boundary_qualification']['status'], 'invalid')
        self.assertIn('boundary_runtime_not_completed', row['boundary_qualification']['errors'])
        self.assertFalse(row['candidate_eligible'])
        saved = self.load(row['runtime_result_path'])
        self.assertTrue(saved['real_execution'])
        self.assertEqual(saved['runtime_status'], 'failed')
        self.assertEqual(saved['events'][-1]['runtime_status'], 'completed')
        self.assertFalse(saved['execution_boundary']['cleanup_confirmed'])
        self.assertEqual(self.load(report['candidate_ledger_path'])['candidates'], [])
        self.assertFalse(report['seal']['sealed'])
        self.assertTrue(all(r['execution_stage'] == 'planned' for r in report['rows'][1:]))
        raw_before = Path(row['raw_path']).read_bytes()
        audit = run_report.audit_report(self.root / 'wrapper-required/report.json')
        self.assertEqual(Path(row['raw_path']).read_bytes(), raw_before)
        self.assertFalse(audit['sealed'])
        self.assertIn('boundary_runtime_not_completed', audit['boundary_qualifications'][row['cell_id']]['errors'])

    def assert_unqualified_attempt_without_novelty(self, result, corpus, feedback, scheduler, before):
        """Attempt accounting is not complete observation or novelty credit."""
        after = feedback.to_dict()
        self.assertEqual(len(after['events']), len(before[1]['events']) + 1)
        self.assertEqual(after['events'][:-1], before[1]['events'])
        attempt = after['events'][-1]
        self.assertTrue(attempt['executed'])
        self.assertIs(attempt['coverage'], False)
        self.assertIs(attempt['complete'], False)
        self.assertIsNone(attempt['edge_id'])
        self.assertNotIn('edge_provenance', attempt)
        self.assertEqual(after['edge_counts'], before[1]['edge_counts'])
        self.assertEqual(after['covered_surfaces'], before[1]['covered_surfaces'])
        self.assertEqual(corpus.to_dict(), before[0])
        self.assertEqual(scheduler.feedback_event_count, len(before[1]['events']) + 1)
        self.assertEqual(scheduler.feedback_identity_at_start, feedback.identity())
        self.assertEqual(result['budget']['actual'], 1)
        self.assertEqual(result['budget']['attempted'], 1)
        self.assertFalse(result['rows'][0].get('candidate_eligible'))

    def test_exploratory_unexpected_wrapper_keeps_actual_counts_but_no_novelty(self):
        runtime = Mock(side_effect=inner_success_cleanup_failure)
        result, corpus, feedback, scheduler, before = self.cycle('wrapper-cycle', runtime=runtime)
        runtime.assert_called_once()
        row = result['rows'][0]
        self.assertTrue(row['actual_run'])
        self.assertEqual(row['execution_stage'], 'judged')
        self.assertEqual(result['budget']['attempted'], 1)
        self.assertEqual(result['budget']['actual'], 1)
        self.assertEqual(result['budget']['executed'], 1)  # actual inner run, NOT maturity
        self.assertEqual(row['boundary_qualification']['status'], 'invalid')
        self.assertFalse(row['feedback_eligible'])
        self.assert_unqualified_attempt_without_novelty(
            result, corpus, feedback, scheduler, before)
        self.assertFalse(row['corpus_offer']['accepted'])
        self.assertEqual(self.load(row['runtime_result_path'])['runtime_status'], 'failed')

    def test_required_independent_audit_does_not_trust_rehashed_qualified_labels(self):
        report, row, config, result = self.executed_required_fixture(anomaly=True)
        report.update(runtime_boundary='isolated_netns', formal_runtime=True,
                      boundary_qualification={'status': 'qualified'})
        row.update(boundary_qualification={'status': 'qualified'}, candidate_eligible=True)
        config['execution_boundary'] = {'enabled': True}
        run_report.write_json(row['runtime_config_path'], config)
        raw_before = Path(row['raw_path']).read_bytes()
        runtime_before = Path(row['runtime_result_path']).read_bytes()
        # Deliberately bypass runner._persist: the auditor must find the issue independently.
        from r2f.reports.candidate_report import candidate_ledger
        ledger = candidate_ledger(report['rows'])
        report.update(candidate_ledger_path=str(self.root / 'executed/candidate_ledger.json'),
                      candidate_ledger_sha256=canonical_hash(ledger))
        run_report.write_json(report['candidate_ledger_path'], ledger)
        report['report_sha256'] = run_report.report_digest(report)
        path = self.root / 'executed/report.json'
        run_report.write_json(path, report)
        audit = run_report.audit_report(path)
        self.assertFalse(audit['sealed'])
        q = audit['boundary_qualifications'][row['cell_id']]
        self.assertEqual(q['status'], 'invalid')
        self.assertIn('boundary_runtime_not_completed', q['errors'])
        self.assertIn('candidate_ledger_dropped_or_invented_anomaly', audit['errors'])
        self.assertEqual(Path(row['raw_path']).read_bytes(), raw_before)
        self.assertEqual(Path(row['runtime_result_path']).read_bytes(), runtime_before)
        self.assertTrue(self.load(path)['rows'][0]['actual_run'])
        self.assertEqual(self.load(path)['rows'][0]['execution_stage'], 'judged')

    def test_no_context_or_allowlist_string_can_qualify_without_explicit_root(self):
        qualification = run_report.execution_boundary_qualification(
            'isolated_netns', config={'execution_boundary': {'enabled': True}},
            formal_runtime=True,
            result={'real_execution': True, 'runtime_status': 'completed',
                    'execution_boundary': {'cleanup': {'confirmed': True},
                                           'implementation': 'allowlisted-looking-name'}})
        self.assertEqual(qualification['status'], 'invalid')
        self.assertIn('boundary_output_root_missing', qualification['errors'])
        self.assertFalse(qualification['eligible'])

    def test_boundary_context_and_execution_switch_cannot_hide_behind_host_label(self):
        for config, result in (
            ({'execution_boundary': {'enabled': True}}, {}),
            ({}, {'execution_boundary': {}}),
        ):
            with self.subTest(config=config, result=result):
                qualification = run_report.execution_boundary_qualification('host', config=config, result=result)
                self.assertEqual(qualification['status'], 'invalid')
                self.assertIn('boundary_route_context_mismatch', qualification['errors'])
                self.assertIn('injected_runtime_is_offline_contract_only', qualification['errors'])

    def test_malformed_boundary_inputs_fail_closed(self):
        for label in ('config', 'row', 'result'):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, 'boundary_' + label + '_not_object'):
                    run_report.execution_boundary_qualification('host', **{label: []})
        qualification = run_report.execution_boundary_qualification(
            'isolated_netns', config={'source_manifest': ['malformed']},
            result={}, formal_runtime=True)
        self.assertEqual(qualification['status'], 'invalid')
        self.assertIn('boundary_source_manifest_not_object', qualification['errors'])
        self.assertFalse(qualification['eligible'])

    def test_route_only_coverage_stays_pending_without_rewriting_actual_counts(self):
        row = {'target_id': 'fixture', 'mutation_plan': {'operations': []},
               'sampling_kind': 'boundary', 'chain_depth': 1,
               'actual_run': True, 'execution_stage': 'judged',
               'mutation_receipt': {'hit': True}}
        before = deepcopy(row)
        coverage = run_report.exploratory_coverage_fields(
            {'runtime_boundary': 'isolated_netns', 'rows': [row]})
        self.assertEqual(coverage[0]['generated'], 1)
        self.assertEqual(coverage[0]['executed'], 1)
        self.assertEqual(coverage[0]['judged'], 0)
        self.assertEqual(coverage[0]['hit'], 0)
        self.assertEqual(row, before)

    def test_exploratory_auditor_detects_saved_wrapper_not_row_claim_and_excludes_coverage(self):
        row = base_row(self.root, 'exploratory-isolated', verdict='fail',
                       sampling_kind='boundary', chain_depth=1)
        row['boundary_qualification'] = {'status': 'qualified'}
        row['runtime_config_path'] = str(self.root / 'runtime_config.json')
        row['runtime_result_path'] = str(self.root / 'runtime_result.json')
        run_report.write_json(row['runtime_config_path'], {'execution_boundary': {'enabled': True}})
        runtime = {'real_execution': True, 'runtime_status': 'failed',
                   'execution_boundary': {'cleanup_confirmed': False}}
        run_report.write_json(row['runtime_result_path'], runtime)
        path, report = build_report(self.root, [row])
        report['runtime_boundary'] = 'isolated_netns'
        report['report_sha256'] = run_report.exploratory_report_digest(report)
        run_report.write_json(path, report)
        original = (path.read_bytes(), Path(row['raw_path']).read_bytes())
        audit = run_report.audit_report(path)
        self.assertFalse(audit['sealed'])
        self.assertFalse(audit['evidence_valid'])
        self.assertEqual(audit['rows_verified'], 0)
        self.assertEqual(audit['coverage_entries'], 0)
        self.assertEqual(audit['coverage'][0]['executed'], 1)
        self.assertEqual(audit['coverage'][0]['generated'], 1)
        self.assertEqual(audit['coverage'][0]['judged'], 0)
        self.assertEqual(audit['coverage'][0]['hit'], 0)
        self.assertIn('boundary_runtime_not_completed', audit['boundary_qualifications'][row['cell_id']]['errors'])
        self.assertEqual((path.read_bytes(), Path(row['raw_path']).read_bytes()), original)

    def test_canonical_required_isolated_executes_then_excludes_structurally_valid_offline(self):
        fixtures = []
        def runtime_fixture(config, directory):
            fixture = complete_offline_boundary(config, directory)
            fixtures.append(fixture)
            return fixture.result
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime', side_effect=runtime_fixture) as runtime:
            report = runner.run_campaign(self.root / 'canonical-required', execute=True,
                                         runtime_boundary='isolated_netns')
        runtime.assert_called_once()
        self.assertTrue(report['formal_runtime'])
        self.assertEqual(report['abort_reason'], 'offline_fixture_cannot_establish_real_ros')
        row = report['rows'][0]
        self.assertEqual(row['execution_stage'], 'judged')
        self.assertTrue(row['actual_run'])
        qualification = row['boundary_qualification']
        self.assertEqual(qualification['errors'], ['offline_fixture_cannot_establish_real_ros'])
        self.assertTrue(qualification['boundary_audit']['valid'])
        self.assertFalse(qualification['boundary_audit']['real_execution_attested'])
        self.assertFalse(qualification['eligible'])
        self.assertFalse(row['candidate_eligible'])
        self.assertEqual(self.load(report['candidate_ledger_path'])['candidates'], [])
        from r2f.campaign.boundary_audit import audit_execution_boundary
        with patch('r2f.campaign.boundary_audit.audit_execution_boundary', wraps=audit_execution_boundary) as helper:
            audit = run_report.audit_report(self.root / 'canonical-required/report.json')
        self.assertGreaterEqual(helper.call_count, 1)
        self.assertTrue(all(call.kwargs == {'output_root': Path(row['runtime_result_path']).parent}
                            for call in helper.call_args_list))
        self.assertFalse(audit['sealed'])
        self.assertEqual(audit['boundary_qualifications'][row['cell_id']]['errors'], qualification['errors'])
        metadata = self.load(row['runtime_result_path'])['execution_metadata']
        self.assertEqual(metadata['pid_namespace'], fixtures[0].controller['pid_scope'])
        self.assertTrue(all(worker['pid_namespace'] == metadata['pid_namespace'] for worker in metadata['workers']))
        self.assertEqual(metadata['pid_interpretation'], 'namespace_PID_not_host_PID')
        self.assertTrue(all(r['execution_stage'] == 'planned' for r in report['rows'][1:]))

    def test_canonical_exploratory_executes_once_but_offline_never_enters_novelty(self):
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime',
                   side_effect=lambda c, d: complete_offline_boundary(c, d).result) as runtime:
            result, corpus, feedback, scheduler, before = self.cycle(
                'canonical-exploratory', boundary='isolated_netns', runtime=runtime)
        runtime.assert_called_once()
        self.assertEqual(result['budget']['attempted'], 1)
        self.assertEqual(result['budget']['actual'], 1)
        self.assertEqual(result['budget']['executed'], 1)
        self.assert_unqualified_attempt_without_novelty(
            result, corpus, feedback, scheduler, before)
        row = result['rows'][0]
        self.assertEqual(row['execution_stage'], 'judged')
        self.assertTrue(row['actual_run'])
        self.assertEqual(row['boundary_qualification']['errors'], ['offline_fixture_cannot_establish_real_ros'])
        self.assertTrue(row['boundary_qualification']['boundary_audit']['valid'])
        self.assertEqual(result['abort']['reason_code'], 'execution_boundary_qualification_failed')
        recomputed = run_report.boundary_qualification_for_row(
            self.root / 'canonical-exploratory', dict(row, boundary_qualification={'status': 'qualified'}), result)
        self.assertEqual(recomputed['errors'], ['offline_fixture_cannot_establish_real_ros'])
        self.assertFalse(recomputed['eligible'])

    def test_isolated_canonical_unfrozen_source_blocks_before_runtime(self):
        with patch.object(runner, 'source_manifest', return_value=dict(self.source, source_frozen=False)):
            with patch('r2f.runtime.execution_boundary.run_isolated_runtime') as runtime:
                report = runner.run_campaign(self.root / 'unfrozen-isolated', execute=True,
                                             runtime_boundary='isolated_netns')
        runtime.assert_not_called()
        self.assertEqual(report['abort_reason'], 'source_behavior_not_tracked_and_clean_at_HEAD')
        self.assertTrue(all(row['execution_stage'] == 'planned' for row in report['rows']))
        self.assertTrue(all(not row['actual_run'] for row in report['rows']))

    def test_isolated_inconsistent_frozen_declarations_block_before_submission(self):
        for index, change in enumerate(({'dirty_behavior_paths': ['dirty.py']}, {'head': None},
                                        {'behavior_tree_sha256': None})):
            with self.subTest(change=change):
                with patch.object(runner, 'source_manifest', return_value=dict(self.source, **change)):
                    with patch('r2f.runtime.execution_boundary.run_isolated_runtime') as runtime:
                        report = runner.run_campaign(self.root / ('bad-frozen-' + str(index)),
                                                     execute=True, runtime_boundary='isolated_netns')
                runtime.assert_not_called()
                self.assertEqual(report['abort_reason'], 'source_behavior_not_tracked_and_clean_at_HEAD')
                self.assertTrue(all(row['execution_stage'] == 'planned' for row in report['rows']))

    def test_complete_fixture_each_independent_gate_stays_required(self):
        from r2f.campaign.boundary_audit import audit_execution_boundary
        from r2f.campaign.replay import _qualification, ISOLATED_RUNTIME
        report = runner.plan_campaign(self.root / 'gate-fixtures')
        row = report['rows'][0]
        config = runner._boundary_execution_config(self.load(row['runtime_config_path']), 'isolated_netns')
        capsule = ReplayCapsule.from_dict(self.load(row['replay_path']))
        for case in ('baseline', 'runtime_failed', 'environment', 'worker', 'namespace_annotation',
                     'nonDDS', 'cleanup', 'source', 'manifest_hash', 'raw_hash', 'manifest_missing', 'manifest_escape'):
            with self.subTest(case=case):
                directory = self.root / ('gate-' + case)
                fixture = complete_offline_boundary(config, directory)
                if case == 'runtime_failed':
                    fixture.inner.update(runtime_status='failed', status='failed')
                    fixture.write()
                elif case == 'environment':
                    fixture.inner['environment']['ok'] = False
                    run_report.write_json(fixture.raw_path.parent / 'environment.json', fixture.inner['environment'])
                    fixture.write()
                elif case == 'worker':
                    fixture.inner['execution_metadata']['workers'][0]['fresh_process'] = False
                    fixture.write()
                elif case == 'namespace_annotation':
                    fixture.result['execution_metadata']['workers'][0]['pid_namespace'] = 'pid:[999]'
                    run_report.write_json(directory / 'runtime_result.json', fixture.result)
                elif case == 'nonDDS':
                    fixture.context['nonDDS'] = fixture.manifest['nonDDS'] = True
                    fixture.write()
                elif case == 'cleanup':
                    fixture.cleanup['confirmed'] = False
                    fixture.context['cleanup']['confirmed'] = False
                    fixture.write()
                elif case == 'source':
                    fixture.manifest['source_after']['head'] = 'c' * 40
                    fixture.write()
                elif case == 'manifest_hash':
                    with (fixture.directory / 'manifest.json').open('a') as stream:
                        stream.write('\n')
                elif case == 'raw_hash':
                    with fixture.raw_path.open('a') as stream:
                        stream.write('{}\n')
                elif case == 'manifest_missing':
                    (fixture.directory / 'manifest.json').unlink()
                elif case == 'manifest_escape':
                    fixture.result['execution_boundary']['manifest_path'] = str(self.root / 'outside.json')
                    run_report.write_json(self.root / 'outside.json', fixture.manifest)
                    run_report.write_json(directory / 'runtime_result.json', fixture.result)
                audit = audit_execution_boundary(config, fixture.result, output_root=directory)
                raw_before = fixture.raw_path.read_bytes()
                execution = ingest_execution(Scenario.from_dict(capsule.input['scenario']),
                                             config, fixture.result, capsule)
                qualification = _qualification(ISOLATED_RUNTIME, config, fixture.result,
                                               execution, output_root=directory)
                self.assertFalse(qualification['real_ros_execution'])
                self.assertIn('offline_fixture_cannot_establish_real_ros', qualification['qualification_errors'])
                if case == 'baseline':
                    self.assertTrue(audit['valid'], audit['errors'])
                    self.assertEqual(qualification['qualification_errors'], ['offline_fixture_cannot_establish_real_ros'])
                elif case in ('runtime_failed', 'environment', 'worker'):
                    self.assertTrue(audit['valid'], audit['errors'])  # consistency alone is insufficient
                    required = {'runtime_failed': 'boundary_runtime_not_completed',
                                'environment': 'environment_manifest_invalid',
                                'worker': 'worker_health_receipt_missing:writer'}[case]
                    self.assertIn(required, qualification['qualification_errors'])
                else:
                    self.assertFalse(audit['valid'])
                    self.assertIn('execution_boundary_artifact_audit_failed', qualification['qualification_errors'])
                self.assertEqual(fixture.raw_path.read_bytes(), raw_before)

    def test_helper_valid_and_qualified_label_do_not_replace_formal_caller(self):
        report = runner.plan_campaign(self.root / 'informal')
        row = report['rows'][0]
        config = runner._boundary_execution_config(self.load(row['runtime_config_path']), 'isolated_netns')
        fixture = complete_offline_boundary(config, self.root / 'informal-runtime')
        qualification = run_report.execution_boundary_qualification(
            'isolated_netns', config=config, result=fixture.result,
            row={'execution_stage': 'judged', 'actual_run': True,
                 'boundary_qualification': {'status': 'qualified', 'eligible': True}},
            formal_runtime=False, output_root=self.root / 'informal-runtime')
        self.assertTrue(qualification['boundary_audit']['valid'])
        self.assertIn('injected_runtime_is_offline_contract_only', qualification['errors'])
        self.assertIn('boundary_route_context_mismatch', qualification['errors'])
        self.assertFalse(qualification['eligible'])

    def test_current_source_drift_cannot_qualify_consistent_artifacts(self):
        report = runner.plan_campaign(self.root / 'source-drift')
        row = report['rows'][0]
        config = runner._boundary_execution_config(self.load(row['runtime_config_path']), 'isolated_netns')
        fixture = complete_offline_boundary(config, self.root / 'source-runtime')
        with patch.object(run_report, 'source_manifest', return_value=dict(self.source, head='c' * 40)):
            qualification = run_report.execution_boundary_qualification(
                'isolated_netns', config=config, result=fixture.result,
                row={'execution_stage': 'judged', 'actual_run': True}, formal_runtime=True,
                output_root=self.root / 'source-runtime')
        self.assertTrue(qualification['boundary_audit']['valid'])
        self.assertIn('boundary_current_source_mismatch', qualification['errors'])
        self.assertFalse(qualification['eligible'])

    def test_independent_report_pins_layout_not_sidecar_selected_output_root(self):
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime',
                   side_effect=lambda c, d: complete_offline_boundary(c, d).result):
            report = runner.run_campaign(self.root / 'layout', execute=True, runtime_boundary='isolated_netns')
        row = report['rows'][0]
        # Same bytes, different path within campaign: ordinary containment is
        # not enough. The runner's actual output directory remains the anchor.
        replacement = self.root / 'layout/copied-runtime-result.json'
        replacement.write_bytes(Path(row['runtime_result_path']).read_bytes())
        forged = dict(row, runtime_result_path=str(replacement), boundary_qualification={'status': 'qualified'})
        qualification = run_report.boundary_qualification_for_row(self.root / 'layout', forged, report)
        self.assertEqual(qualification['reason_code'], 'boundary_runtime_artifact_layout_mismatch')
        self.assertFalse(qualification['eligible'])

    def test_independent_report_recomputes_helper_after_rehashing_qualified_label(self):
        with patch('r2f.runtime.execution_boundary.run_isolated_runtime',
                   side_effect=lambda c, d: complete_offline_boundary(c, d).result):
            report = runner.run_campaign(self.root / 'rehash', execute=True, runtime_boundary='isolated_netns')
        row = report['rows'][0]
        raw_before = Path(row['raw_path']).read_bytes()
        result = self.load(row['runtime_result_path'])
        result.update(runtime_status='failed', status='failed')  # inner runtime_end remains completed
        result['execution_boundary']['cleanup']['confirmed'] = False
        run_report.write_json(row['runtime_result_path'], result)
        row.update(boundary_qualification={'status': 'qualified', 'eligible': True}, seal_eligible=True)
        report['report_sha256'] = run_report.report_digest(report)
        path = self.root / 'rehash/report.json'
        run_report.write_json(path, report)
        audit = run_report.audit_report(path)
        qualification = audit['boundary_qualifications'][row['cell_id']]
        self.assertFalse(audit['sealed'])
        self.assertFalse(qualification['boundary_audit']['valid'])
        self.assertIn('boundary_runtime_not_completed', qualification['errors'])
        self.assertEqual(Path(row['raw_path']).read_bytes(), raw_before)
        self.assertEqual(row['execution_stage'], 'judged')
        self.assertTrue(row['actual_run'])
        self.assertEqual(result['events'][-1]['runtime_status'], 'completed')

    def test_host_default_fixture_route_and_legacy_report_stay_compatible(self):
        runtime = Mock(side_effect=lambda c, d: fixture_pair(c, d, fresh_matches=True))
        report = runner.run_campaign(self.root / 'host', execute=True, runtime=runtime)
        self.assertEqual(runtime.call_count, 24)
        self.assertNotIn('abort_reason', report)
        self.assertEqual(report['runtime_boundary'], 'host')
        self.assertFalse(report['formal_runtime'])
        self.assertNotIn('boundary_qualifications', report['seal'])
        self.assertTrue(all(r['actual_run'] for r in report['rows']))
        self.assertTrue(all('boundary_qualification' not in r for r in report['rows']))
        self.assertFalse(report['seal']['sealed'])  # offline origin is still excluded
        self.assertTrue(any('offline_fixture_cannot_seal' in e for e in report['seal']['errors']))
        host_fixture = self.root / 'legacy'
        host_fixture.mkdir()
        path, _ = build_report(host_fixture, [base_row(host_fixture, 'legacy-host')])
        self.assertTrue(run_report.audit_report(path)['evidence_valid'])

    def test_host_formal_source_gate_is_not_relaxed(self):
        self.source['source_frozen'] = False
        with patch.object(runner, 'source_manifest', return_value=self.source):
            with patch('r2f.runtime.ros2_pair.run_pair') as runtime:
                report = runner.run_campaign(self.root / 'unfrozen', execute=True)
        runtime.assert_not_called()
        self.assertEqual(report['abort_reason'], 'source_behavior_not_tracked_and_clean_at_HEAD')
        self.assertTrue(all(r['execution_stage'] == 'planned' for r in report['rows']))


if __name__ == '__main__':
    unittest.main()
