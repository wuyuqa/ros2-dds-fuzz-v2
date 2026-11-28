"""CP3 offline fixture: actual Python calls, zero DDS executions/natural cases."""
import copy
import json
from pathlib import Path
import unittest

from r2f.campaign.identity import source_manifest
from r2f.campaign.runtime_adapter import rebuild_saved_case_facts
from r2f.core import Registry, compile_scenario
from r2f.core.candidate import candidate_execution_identity
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.interfaces import ExecutionServices
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.providers import S1FactProducer
from r2f.reports.capability_reconnect import evaluate_reconnect
import test_capability_validation as cp2

ROOT = Path(__file__).resolve().parents[1]


class FixtureOracle:
    """Inspectable registered fixture; state only steers adversarial test outcomes."""
    def __init__(self):
        self.calls = 0
        self.mode = 'normal'
        self.hook = None

    def evaluate(self, scenario, facts):
        self.calls += 1
        if self.hook:
            self.hook(scenario, facts)
        if self.mode == 'exception':
            raise RuntimeError('fixture oracle error')
        from r2f.modules.s1.oracles import evaluate_primary
        result = evaluate_primary(facts, scenario.target_id)
        if self.mode in ('pass', 'fail', 'unknown'):
            result['verdict'] = self.mode
        return result


def saved_runtime(root, row, scenario, capsule, *, defect):
    """Literal historical-format tokens; no backend, carrier, runner or processes."""
    config = dict(capsule.input['effective_input']['final_input'], run_id=row['run_id'],
                  domain_id=199, arm=row['arm'], rmw=row['rmw'], target_id=row['target_id'])
    events = []
    def emit(event, **fields):
        index = len(events)
        events.append(dict(event=event, event_index=index, monotonic_ns=index + 1,
                           time_ns=index + 1, run_id=row['run_id'], domain_id=199, **fields))
        return events[-1]
    emit('environment_preflight', ok=True)
    emit('isolation_locks_acquired', locks=['fixture-only'])
    preconditions, refs = [], []
    for step in scenario.state_steps:
        before = emit('state_step_before', step_id=step.step_id, before={})
        after = emit('state_step_after', step_id=step.step_id, after={}, satisfied=True)
        preconditions.append(dict(step_id=step.step_id, before={}, after={}, satisfied=True,
                                  requires=[c.condition_id for c in step.requires]))
        refs.append(dict(step_id=step.step_id, raw_before_index=before['event_index'],
                         raw_after_index=after['event_index']))
    emit('mutation_begin' if row['arm'] == 'mutated' else 'control_begin',
         preconditions_snapshot=dict(writer_created=True, writer_prejoin_published=True,
                                     reader_created=False), precondition_receipt_refs=refs)
    for role, pid in [('writer', 101), ('reader', 102)]:
        stamp = len(events) + 1
        emit('endpoint_created', role=role, pid=pid, endpoint_gid=role+'-gid',
             endpoint_generation=0, created_monotonic_ns=stamp,
             rmw_identifier=row['rmw'], topic=config['topic'], qos=config['qos'])
    emit('reader_created_after_writer', configured_delay_sec=config['reader_delay_sec'])
    observations = []
    for name in ['reader_created', 'endpoints_matched', 'graph_identity_verified']:
        emit('state_step_before', step_id=name, before={})
        emit('state_step_after', step_id=name, after={}, satisfied=True)
        observations.append(dict(step_id=name, before={}, after={}, satisfied=True))
    for role in ['writer', 'reader']:
        emit('match', role=role, matched_count=1)
        emit('graph_snapshot', role=role, publishers=[dict(endpoint_gid='writer-gid')],
             subscriptions=[dict(endpoint_gid='reader-gid')])
    emit('window_open', window='post_match_delivery')
    emit('delivery_contract', expected_sequences=[0, 1, 2], sentinel_sequence=2,
         prejoin_in_delivery_contract=False, qos=config['qos'])
    for seq in [0, 1, 2]:
        emit('send', phase='post_match', send_sequence=seq, publish_returned=True)
        if not defect or seq != 1:
            emit('receive', phase='post_match', send_sequence=seq, payload_run_id=row['run_id'],
                 publisher_gid='writer-gid', reader_gid='reader-gid', endpoint_generation=0)
    emit('window_close', window='post_match_delivery')
    emit('runtime_end', mutation_receipt=row['mutation_receipt'])
    path = Path(row['raw_path'])
    path.write_text(''.join(json.dumps(e)+'\n' for e in events))
    row['raw_sha256'] = raw_sha256(path)
    metadata = dict(run_id=row['run_id'], domain_id=199, target_id=row['target_id'],
                    target_revision=row['target_revision'], rmw=row['rmw'], arm=row['arm'],
                    repeat=scenario.repeat, source=capsule.input['source_manifest'],
                    source_after=capsule.input['source_manifest'],
                    workers=[dict(role='writer', pid=101, returncode=0),
                             dict(role='reader', pid=102, returncode=0)])
    runtime = dict(raw_path=str(path), raw_sha256=row['raw_sha256'], events=events,
                   mutation_receipt=row['mutation_receipt'], real_execution=True,
                   runtime_status='completed', environment={'ok': True}, execution_metadata=metadata,
                   precondition_receipts=preconditions, observation_receipts=observations)
    return runtime


class ReconnectEvaluation(unittest.TestCase):
    def setUp(self):
        self.cp = cp2.CapabilityValidation('runTest')
        self.cp.setUp()
        self.addCleanup(self.cp.doCleanups)
        self.root = self.cp.root
        owner = S1Module()
        self.target_id = 'discovery.late_endpoint'
        self.oracle = FixtureOracle()
        previous = owner.services[self.target_id]
        owner.services[self.target_id] = ExecutionServices(
            previous.mutation_provider, previous.carrier, S1FactProducer(), self.oracle)
        self.registry = Registry()
        self.registry.register(owner)
        self.saved = {}
        self.source = source_manifest(ROOT)
        self.cp.models['after'].update(path=str(Path(__file__).resolve()),
                                      sha256=raw_sha256(__file__), source=self.source)
        self.runtimes = []
        for index, receipt in enumerate(self.cp.receipts):
            row = receipt['row']
            scenario = compile_scenario(owner.targets[self.target_id], row['rmw'], row['arm'], 0)
            final = dict(topic='/fixture', message_type='std_msgs/msg/String',
                         qos=dict(reliability='reliable', durability='volatile', depth=64),
                         reader_delay_sec=0.1, pre_match_count=0, post_match_count=3)
            capsule = ReplayCapsule.from_scenario(scenario, source_manifest=self.cp.original['source'],
                                                  final_input=final)
            capsule_path = self.root / ('saved-input-'+str(index)+'.json')
            capsule_path.write_text(json.dumps(capsule.to_dict()))
            row.update(replay_path=str(capsule_path), final_input=final, target=scenario.target.to_dict(),
                       input_layer_receipts=capsule.input['effective_input']['input_layer_receipts'],
                       effective_input_sha256=capsule.input['effective_input_sha256'])
            receipt['input_sha256'] = row['effective_input_sha256']
            # Independently specified expected primary results for this literal fixture.
            receipt['expected'] = dict(rule_id=row['rule_id'], rule_role='primary',
                verdict='fail' if index == 0 else 'pass',
                reason_codes=['post_match_delivery_loss'] if index == 0 else ['matched_postjoin_delivery_contract_satisfied'],
                forbidden_residuals=['post_match_delivery_loss'] if index == 0 else [],
                closure=dict(kind='delivered_later_sequence', proven_gaps=[1] if index == 0 else [], tail_inflight=[], sentinel_sequence=2))
            receipt['before'] = dict(receipt['expected'], verdict='pass') if index == 0 else receipt['expected']
            receipt['after'] = receipt['expected']
            runtime = saved_runtime(self.root, row, scenario, capsule, defect=index == 0)
            from r2f.campaign.runtime_adapter import canonical_receipts
            row['state_steps'] = scenario.to_dict()['state_steps']
            row['precondition_receipts'] = canonical_receipts(scenario, runtime, runtime['events'])
            runtime_path = self.root / ('saved-runtime-'+str(index)+'.json')
            runtime_path.write_text(json.dumps(runtime))
            self.runtimes.append((runtime_path, runtime))
        self.cp.original = candidate_execution_identity(self.cp.row)
        self.cp.case.refs['capsule'] = self.cp.row['replay_path']
        self.cp.case.log = self.root / 'cp3-reviews.jsonl'
        self.cp.parent = self.cp.case.review(record_id='bound-analysis')
        self.cp.task.update(review_record_id=self.cp.parent['record_id'],
                            review_record_sha256=self.cp.parent['record_sha256'])
        self.cp.bundle.update(original_ref=self.cp.original)
        self.proposal = self.cp.proposal()
        for case_ref, (runtime_path, _) in zip(self.cp.bundle['cases'], self.runtimes):
            case = json.loads(Path(case_ref['path']).read_text())
            self.saved[case_ref['sha256']] = dict(runtime=self.ref(runtime_path),
                                                 capsule=self.ref(case['row']['replay_path']))

    def ref(self, path):
        return dict(path=str(path), sha256=raw_sha256(path))

    def evaluate(self):
        return evaluate_reconnect(self.proposal['capability_validation'],
            evidence_refs=self.proposal['evidence_refs'], original_ref=self.cp.original,
            assessment=self.proposal['capability_assessment'], task_review=self.cp.parent,
            registry=self.registry, source=self.source, saved_cases=self.saved)

    def test_actual_call_rebuilds_raw_and_keeps_originals_immutable(self):
        paths = [p for p in self.root.rglob('*') if p.is_file()]
        before = {p: p.read_bytes() for p in paths}
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'evaluated', artifact)
        self.assertEqual(self.oracle.calls, 3)
        self.assertFalse(artifact['implemented'])
        self.assertFalse(artifact['new_execution'])
        self.assertEqual(artifact['natural_candidates'], 0)
        self.assertEqual(artifact['qualification'], 'fixture')
        self.assertEqual(artifact['capture_source'], self.cp.original['source'])
        self.assertEqual(artifact['evaluation_source'], self.source)
        self.assertEqual(len(artifact['call_receipts']), 3)
        self.assertTrue(all(r['before_execution'] == 'recorded_only' for r in artifact['call_receipts']))
        self.assertEqual(before, {p: p.read_bytes() for p in paths})
        for forbidden in ['execution_stage', 'actual_run', 'candidate', 'feedback', 'seal']:
            self.assertNotIn(forbidden, artifact)

    def test_call_receipts_bind_actual_facts_result_and_measured_sources(self):
        artifact = self.evaluate()
        for receipt in artifact['call_receipts']:
            self.assertEqual(receipt['facts_sha256'], canonical_hash(receipt['facts']))
            self.assertEqual(receipt['result_sha256'], canonical_hash(receipt['after']))
            self.assertEqual(receipt['evaluation_source_before'], self.source)
            self.assertEqual(receipt['evaluation_source_after'], self.source)
            self.assertEqual(receipt['binding_sha256'], canonical_hash(artifact['binding']))
        digest = artifact.pop('evaluation_sha256')
        self.assertEqual(digest, canonical_hash(artifact))
        artifact['call_receipts'][0]['facts']['sent'].clear()
        second = self.evaluate()
        self.assertEqual(second['status'], 'evaluated')
        self.assertEqual(self.oracle.calls, 6)

    def refresh(self):
        self.proposal = self.cp.proposal()
        self.saved = {}
        for ref, (runtime_path, _) in zip(self.cp.bundle['cases'], self.runtimes):
            case = json.loads(Path(ref['path']).read_text())
            self.saved[ref['sha256']] = dict(runtime=self.ref(runtime_path),
                                            capsule=self.ref(case['row']['replay_path']))

    def change_runtime(self, change, index=0):
        path, runtime = self.runtimes[index]
        change(runtime)
        path.write_text(json.dumps(runtime))
        self.refresh()

    def test_row_domain_must_match_capture_domain(self):
        self.cp.receipts[1]['row']['domain_id'] = 198
        self.refresh()
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'rejected')
        self.assertIn('saved_case_runtime_identity_mismatch', str(artifact['diagnostics']))
        self.assertEqual(self.oracle.calls, 1)

    def test_revision_and_repeat_use_strict_json_integer_identity(self):
        original = copy.deepcopy(self.runtimes[0][1])
        for key, value in [('target_revision', False), ('target_revision', 0.0),
                           ('repeat', False), ('repeat', 0.0)]:
            with self.subTest(key=key, value=value):
                self.change_runtime(lambda r: r.update(copy.deepcopy(original)))
                self.change_runtime(lambda r: r['execution_metadata'].update({key: value}))
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertEqual(self.oracle.calls, 0)

    def test_capture_source_after_is_required_and_must_be_unchanged(self):
        original = copy.deepcopy(self.runtimes[0][1])
        for after in (None, {'source_commit': 'changed'}, False):
            with self.subTest(after=after):
                self.change_runtime(lambda r: r.update(copy.deepcopy(original)))
                def change(runtime):
                    runtime['execution_metadata'].pop('source_after')
                    if after is not None:
                        runtime['execution_metadata']['source_after'] = after
                self.change_runtime(change)
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertEqual(self.oracle.calls, 0)

    def test_null_capsule_fault_id_cannot_mask_capture_fault_control(self):
        # Null is an intentional normal configuration value, not permission to
        # discard evidence that the captured execution ran a harness fault.
        for receipt in self.cp.receipts[1:]:
            row = receipt['row']
            path = Path(row['replay_path'])
            scenario = compile_scenario(S1Module().targets[self.target_id], row['rmw'], row['arm'], 0)
            final = dict(row['final_input'], fault_id=None)
            capsule = ReplayCapsule.from_scenario(scenario, source_manifest=self.cp.original['source'],
                                                  final_input=final)
            path.write_text(json.dumps(capsule.to_dict()))
            row.update(final_input=final, effective_input_sha256=capsule.input['effective_input_sha256'],
                       input_layer_receipts=capsule.input['effective_input']['input_layer_receipts'])
            receipt['input_sha256'] = row['effective_input_sha256']
        self.change_runtime(lambda r: r['execution_metadata'].update(fault_id='injected-fault'), index=1)
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'rejected')
        self.assertIn('saved_case_control_contamination', str(artifact['diagnostics']))
        self.assertEqual(self.oracle.calls, 1)

    def test_worker_health_must_belong_to_raw_endpoint_processes(self):
        self.change_runtime(lambda r: r['execution_metadata']['workers'][0].update(pid=998))
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'rejected')
        self.assertIn('saved_case_worker_identity_mismatch', str(artifact['diagnostics']))
        self.assertEqual(self.oracle.calls, 0)

    def test_boolean_and_float_exit_codes_are_not_integer_success(self):
        for value in (False, 0.0):
            with self.subTest(value=value):
                self.change_runtime(lambda r: r['execution_metadata']['workers'][0].update(returncode=value))
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertEqual(self.oracle.calls, 0)

    def test_canonical_rows_bind_capsule_without_duplicating_final_input_fields(self):
        case = copy.deepcopy(self.cp.receipts[1])
        case['row'].pop('final_input')
        case['row'].pop('input_layer_receipts')
        capsule = ReplayCapsule.from_dict(json.loads(Path(case['row']['replay_path']).read_text()))
        scenario, facts = rebuild_saved_case_facts(case, runtime=self.runtimes[1][1], capsule=capsule,
                                                  services=self.registry.get_services(self.target_id))
        self.assertTrue(facts['process_healthy'])
        self.assertEqual(scenario.final_input['topic'], '/fixture')
        self.assertEqual(self.oracle.calls, 0)

    def test_runtime_digest_source_is_bound_to_canonical_row_capture(self):
        case = copy.deepcopy(self.cp.receipts[1])
        row = case['row']
        scenario = compile_scenario(S1Module().targets[self.target_id], row['rmw'], row['arm'], 0)
        source = dict(source_repo='/fixture', head='capture-head', behavior_tree_sha256='b'*64,
                      source_frozen=True, dirty_behavior_paths=[])
        capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source, final_input=row['final_input'])
        row.update(source_identity=source, effective_input_sha256=capsule.input['effective_input_sha256'])
        case['input_sha256'] = row['effective_input_sha256']
        runtime = copy.deepcopy(self.runtimes[1][1])
        runtime_source = dict(source_repo='/fixture', head='capture-head', runtime_source_sha256='a'*64,
                              observer_source_sha256='c'*64, worktree_status=' M docs/acceptance.md')
        runtime['execution_metadata'].update(source=runtime_source, source_after=runtime_source)
        row['runtime_execution_metadata'] = copy.deepcopy(runtime['execution_metadata'])
        _, facts = rebuild_saved_case_facts(case, runtime=runtime, capsule=capsule,
                                            services=self.registry.get_services(self.target_id))
        self.assertTrue(facts['endpoint_identity_complete'])
        self.assertEqual(self.oracle.calls, 0)
        original = copy.deepcopy(runtime)
        for change in ('missing_after', 'changed_after', 'wrong_head', 'missing_row_capture'):
            with self.subTest(change=change):
                runtime = copy.deepcopy(original)
                row['runtime_execution_metadata'] = copy.deepcopy(runtime['execution_metadata'])
                if change == 'missing_after':
                    runtime['execution_metadata'].pop('source_after')
                elif change == 'changed_after':
                    runtime['execution_metadata']['source_after']['runtime_source_sha256'] = 'd'*64
                elif change == 'wrong_head':
                    runtime['execution_metadata']['source']['head'] = 'different-head'
                    row['runtime_execution_metadata'] = copy.deepcopy(runtime['execution_metadata'])
                else:
                    row.pop('runtime_execution_metadata')
                with self.assertRaises(ValueError):
                    rebuild_saved_case_facts(case, runtime=runtime, capsule=capsule,
                                             services=self.registry.get_services(self.target_id))
                self.assertEqual(self.oracle.calls, 0)

    def test_noncanonical_or_failed_runtime_status_cannot_evaluate(self):
        for status in ('ok', 'failed', 'blocked', True, 1):
            with self.subTest(status=status):
                self.change_runtime(lambda r: r.update(runtime_status=status))
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertEqual(self.oracle.calls, 0)

    def test_capture_config_overlap_requires_exact_json_identity(self):
        for value in ('/different', None, False):
            with self.subTest(value=value):
                self.change_runtime(lambda r: r['execution_metadata'].update(topic=value))
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertIn('saved_case_capture_config_mismatch', str(artifact['diagnostics']))
                self.assertEqual(self.oracle.calls, 0)
        self.change_runtime(lambda r: r['execution_metadata'].update(topic='/fixture', fault_id=None))
        self.assertEqual(self.evaluate()['status'], 'evaluated')
        self.assertEqual(self.oracle.calls, 3)

    def test_worker_roles_and_pid_scalars_must_match_exact_raw_processes(self):
        original = copy.deepcopy(self.runtimes[0][1])
        changes = [dict(role='reader'), dict(role=None), dict(pid=True), dict(pid=101.0),
                   dict(pid=0), dict(returncode=1)]
        for change in changes:
            with self.subTest(change=change):
                self.change_runtime(lambda r: r.update(copy.deepcopy(original)))
                self.change_runtime(lambda r: r['execution_metadata']['workers'][0].update(change))
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertIn('saved_case_worker_identity_mismatch', str(artifact['diagnostics']))
                self.assertEqual(self.oracle.calls, 0)
        self.change_runtime(lambda r: r.update(copy.deepcopy(original)))
        self.change_runtime(lambda r: r['execution_metadata']['workers'].reverse())
        self.assertEqual(self.evaluate()['status'], 'evaluated')
        self.assertEqual(self.oracle.calls, 3)

    def test_row_scalar_identity_cannot_use_float_or_boolean_equivalence(self):
        for key, value in [('domain_id', 199.0), ('domain_id', False), ('repeat', False)]:
            with self.subTest(key=key, value=value):
                case = copy.deepcopy(self.cp.receipts[1])
                case['row'][key] = value
                capsule = ReplayCapsule.from_dict(json.loads(Path(case['row']['replay_path']).read_text()))
                with self.assertRaisesRegex(ValueError, 'saved_case_runtime_identity_mismatch'):
                    rebuild_saved_case_facts(case, runtime=self.runtimes[1][1], capsule=capsule,
                                             services=self.registry.get_services(self.target_id))
                self.assertEqual(self.oracle.calls, 0)

    def test_raw_domain_scalar_identity_is_strict(self):
        runtime = self.runtimes[1][1]
        runtime['events'][0]['domain_id'] = 199.0
        raw = Path(runtime['raw_path'])
        raw.write_text(''.join(json.dumps(e)+'\n' for e in runtime['events']))
        runtime['raw_sha256'] = raw_sha256(raw)
        self.cp.receipts[1]['row']['raw_sha256'] = runtime['raw_sha256']
        self.change_runtime(lambda r: None, index=1)
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'rejected')
        self.assertIn('saved_case_raw_identity_mismatch', str(artifact['diagnostics']))
        self.assertEqual(self.oracle.calls, 1)

    def test_forged_proposal_after_cannot_replace_actual_output(self):
        for mode in ('pass', 'unknown', 'exception'):
            with self.subTest(mode=mode):
                self.oracle.mode = mode
                self.oracle.calls = 0
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertEqual(self.oracle.calls, 1)
                self.assertEqual(artifact['call_receipts'][0]['after']['verdict'],
                                 'unknown' if mode == 'exception' else mode)
                self.assertFalse(artifact['implemented'])
                self.assertTrue(artifact['diagnostics'])
                if mode == 'exception':
                    self.assertEqual(artifact['call_receipts'][0]['oracle_tool_failure']['reason'],
                                     'fixture oracle error')

    def test_supplied_row_typed_facts_and_runtime_summaries_are_not_oracle_inputs(self):
        # Only a normal row can be altered without invalidating original_ref.
        self.cp.receipts[1]['row']['typed_facts'] = {'forged': 'row facts'}
        path, runtime = self.runtimes[1]
        runtime['facts'] = {'forged': 'runtime summary'}
        path.write_text(json.dumps(runtime))
        self.refresh()
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'evaluated', artifact['diagnostics'])
        self.assertNotIn('forged', artifact['call_receipts'][1]['facts'])
        self.assertEqual(artifact['call_receipts'][1]['facts']['received'][1]['sequence'], 1)

    def test_wrong_model_file_is_not_loaded_or_called(self):
        path = self.root / 'proposal.py'
        path.write_text("raise AssertionError('must never import proposal')\n")
        self.cp.models['after'].update(path=str(path), sha256=raw_sha256(path))
        self.refresh()
        with self.assertRaisesRegex(ValueError, 'unbound_model_artifact'):
            self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_wrong_model_source_is_not_current_measurement(self):
        self.cp.models['after']['source'] = {'head': 'metadata-only'}
        self.refresh()
        with self.assertRaisesRegex(ValueError, 'unbound_model_artifact'):
            self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_source_short_head_or_wrong_behavior_identity_is_rejected(self):
        source = copy.deepcopy(self.source)
        for field, value in [('head', source['head'][:7]), ('behavior_tree_sha256', 'f'*64),
                             ('source_repo', '/unrelated/repository')]:
            with self.subTest(field=field):
                self.source = dict(source, **{field: value})
                with self.assertRaisesRegex(ValueError, 'evaluation_source_changed_or_mismatched'):
                    self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_source_measurement_requires_exact_json_types(self):
        self.source = dict(self.source, source_frozen=0)
        with self.assertRaisesRegex(ValueError, 'evaluation_source_changed_or_mismatched'):
            self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_unregistered_target_has_no_after_call(self):
        self.registry = Registry()
        with self.assertRaises(KeyError):
            self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_target_facts_or_rule_contract_migration_is_explicitly_unsupported(self):
        for contract in ('observation_contract', 'rule_contract'):
            with self.subTest(contract=contract):
                owner = S1Module()
                target = owner.targets[self.target_id]
                getattr(target, contract)['unsupported_change'] = 'v-next'
                previous = owner.services[self.target_id]
                owner.services[self.target_id] = ExecutionServices(
                    previous.mutation_provider, previous.carrier, previous.fact_producer, self.oracle)
                self.registry = Registry()
                self.registry.register(owner)
                with self.assertRaisesRegex(ValueError, 'unsupported_contract_migration'):
                    self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_cp2_task_raw_hash_and_independent_controls_remain_authoritative(self):
        with self.subTest('task'):
            self.proposal['capability_validation']['task_ref']['task_id'] = 'borrowed-task'
            with self.assertRaisesRegex(ValueError, 'capability_validation_task_mismatch'):
                self.evaluate()
        self.cp.task['task_id'] = 'rule-gap'
        self.refresh()
        with self.subTest('case bytes'):
            path = Path(self.cp.bundle['cases'][1]['path'])
            path.write_text(path.read_text()+' ')
            with self.assertRaisesRegex(ValueError, 'SHA256'):
                self.evaluate()
        self.refresh()
        with self.subTest('duplicate controls'):
            self.cp.receipts[2]['row'] = copy.deepcopy(self.cp.receipts[1]['row'])
            self.refresh()
            with self.assertRaisesRegex(ValueError, 'duplicate_evaluation'):
                self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_borrowed_runtime_and_changed_capsule_hash_are_rejected(self):
        keys = list(self.saved)
        self.saved[keys[1]]['runtime'] = self.saved[keys[2]]['runtime']
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'rejected')
        self.assertIn('saved_case_runtime_identity_mismatch', str(artifact['diagnostics']))
        self.assertEqual(self.oracle.calls, 1)
        self.refresh()
        self.oracle.calls = 0
        self.saved[list(self.saved)[1]]['capsule']['sha256'] = 'f'*64
        with self.assertRaisesRegex(ValueError, 'SHA256'):
            self.evaluate()
        self.assertEqual(self.oracle.calls, 0)

    def test_raw_control_contamination_is_diagnostic_failure(self):
        path, runtime = self.runtimes[1]
        runtime['events'][-1]['event'] = 'fault_control_receipt'
        raw = Path(runtime['raw_path'])
        raw.write_text(''.join(json.dumps(e)+'\n' for e in runtime['events']))
        runtime['raw_sha256'] = raw_sha256(raw)
        self.cp.receipts[1]['row']['raw_sha256'] = raw_sha256(raw)
        path.write_text(json.dumps(runtime))
        self.refresh()
        artifact = self.evaluate()
        self.assertEqual(artifact['status'], 'rejected')
        self.assertIn('control_contamination', str(artifact['diagnostics']))
        self.assertEqual(self.oracle.calls, 1)

    def test_mutating_oracle_facts_or_scenario_cannot_succeed(self):
        for hook in (lambda s, f: f['sent'].clear(),
                     lambda s, f: s.final_input.update(topic='changed')):
            with self.subTest(hook=hook):
                self.oracle.calls = 0
                self.oracle.hook = hook
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertIn('oracle_mutated_facts_or_scenario', str(artifact['diagnostics']))
                self.assertEqual(self.oracle.calls, 1)

    def test_method_replacement_during_call_is_rejected(self):
        from types import FunctionType, MethodType
        original = self.oracle.evaluate
        def replace(scenario, facts):
            function = FunctionType(FixtureOracle.evaluate.__code__, FixtureOracle.evaluate.__globals__,
                                    FixtureOracle.evaluate.__name__)
            function.__qualname__ = FixtureOracle.evaluate.__qualname__
            self.oracle.evaluate = MethodType(function, self.oracle)
        self.oracle.hook = replace
        try:
            artifact = self.evaluate()
            self.assertEqual(artifact['status'], 'rejected')
            self.assertIn('service_binding_changed', str(artifact['diagnostics']))
        finally:
            self.oracle.evaluate = original

    def test_loaded_code_replacement_during_call_is_rejected(self):
        original = FixtureOracle.evaluate.__code__
        def alternate(self, scenario, facts):
            return {}
        self.oracle.hook = lambda s, f: setattr(FixtureOracle.evaluate, '__code__', alternate.__code__)
        try:
            artifact = self.evaluate()
            self.assertEqual(artifact['status'], 'rejected')
            self.assertIn('service_binding_changed', str(artifact['diagnostics']))
        finally:
            FixtureOracle.evaluate.__code__ = original

    def test_behavior_source_drift_during_call_is_rejected_and_restored(self):
        path = ROOT / 'src/r2f/campaign/runtime_adapter.py'
        original = path.read_bytes()
        self.oracle.hook = lambda s, f: path.write_bytes(original+b'\n# offline drift fixture\n')
        try:
            artifact = self.evaluate()
            self.assertEqual(artifact['status'], 'rejected')
            self.assertIn('evaluation_source_changed_or_mismatched', str(artifact['diagnostics']))
            self.assertEqual(self.oracle.calls, 1)
            self.assertNotEqual(artifact['call_receipts'][0]['evaluation_source_after'], self.source)
        finally:
            path.write_bytes(original)

    def test_registered_service_instance_replacement_is_rejected(self):
        services = self.registry.get_services(self.target_id)
        original = services.oracle
        self.oracle.hook = lambda s, f: object.__setattr__(services, 'oracle', FixtureOracle())
        try:
            artifact = self.evaluate()
            self.assertEqual(artifact['status'], 'rejected')
            self.assertIn('service_binding_changed', str(artifact['diagnostics']))
        finally:
            object.__setattr__(services, 'oracle', original)

    def test_mutating_fact_producer_or_raw_evidence_is_rejected_before_oracle(self):
        services = self.registry.get_services(self.target_id)
        original = services.fact_producer
        try:
            for mode in ('facts', 'raw', 'exception'):
                with self.subTest(mode=mode):
                    object.__setattr__(services, 'fact_producer', FixtureBadFacts(mode))
                    artifact = self.evaluate()
                    self.assertEqual(artifact['status'], 'rejected')
                    self.assertEqual(self.oracle.calls, 0)
                    self.assertIn('saved_case_fact_producer_', str(artifact['diagnostics']))
        finally:
            object.__setattr__(services, 'fact_producer', original)

    def test_unstable_normal_actual_result_fails_even_when_cp2_after_is_pass(self):
        for mode in ('unknown', 'fail'):
            with self.subTest(mode=mode):
                self.oracle.mode, self.oracle.calls = 'normal', 0
                def unstable(scenario, facts):
                    if scenario.arm == 'baseline':
                        self.oracle.mode = mode
                self.oracle.hook = unstable
                artifact = self.evaluate()
                self.assertEqual(artifact['status'], 'rejected')
                self.assertEqual(self.oracle.calls, 2)
                self.assertEqual(artifact['call_receipts'][0]['status'], 'matched')
                self.assertEqual(artifact['call_receipts'][1]['after']['verdict'], mode)


class FixtureBadFacts:
    def __init__(self, mode):
        self.mode = mode

    def produce(self, raw):
        if self.mode == 'exception':
            raise RuntimeError('fixture fact producer error')
        result = S1FactProducer().produce(raw)
        if self.mode == 'facts':
            result['sent'].clear()
        else:
            raw['events'].clear()
        return result
