"""Portable historical raw projections and adversarial offline evidence checks.

Fixtures retain original event indexes/chronology and selected raw fields, but
are field-pruned test artifacts, never new execution/qualification evidence.
Every test writes and hashes a private raw; no results/ROS/campaign dependency.
"""
from copy import deepcopy
import hashlib
import inspect
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.state_projection import attach_state_observations
from r2f.core.state_signature import STATE_COMPONENTS, abstract_state, observation_edge

FIXTURES = json.loads((Path(__file__).parent / 'fixtures' / 'state_projection_receipts.json').read_text())['fixtures']


class StateProjectionReceipts(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def fixture(self, label='late-pair-judged'):
        item = deepcopy(FIXTURES[label])
        self.events, self.runtime, self.config, self.facts = [item[k] for k in ('events', 'runtime', 'config', 'facts')]
        self.runtime['raw_path'] = str(Path(self.directory.name) / 'raw.jsonl')
        return item

    def project(self, rewrite=True):
        if rewrite:
            payload = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in self.events).encode()
            Path(self.runtime['raw_path']).write_bytes(payload)
            self.runtime.update(events=self.events, raw_sha256=hashlib.sha256(payload).hexdigest())
        return attach_state_observations(self.facts, self.events, self.runtime, self.config)

    def event(self, kind, **fields):
        return next(e for e in self.events if e['event'] == kind
                    and all(e.get(k) == v for k, v in fields.items()))

    def row(self, facts):
        return dict(execution_stage='judged', actual_run=True, workspace_executed=True,
                    raw_path=self.runtime['raw_path'], raw_sha256=self.runtime['raw_sha256'],
                    typed_facts=facts, operation_family='test-public-api', parameter_class='boundary')

    def assert_refs(self, facts):
        receipt = facts['state_projection_receipt']
        self.assertEqual(receipt['status'], 'observed', receipt['reasons'])
        self.assertTrue(facts['observation_complete'])
        self.assertEqual(receipt['scope'], 'canonical_pair')
        self.assertEqual(receipt['reasons'], [])
        for side in ('before', 'after'):
            part = receipt[side]
            boundary = part['boundary_ref']
            self.assertEqual(boundary['path'], self.runtime['raw_path'])
            self.assertEqual(boundary['sha256'], self.runtime['raw_sha256'])
            self.assertEqual(set(part['component_refs']), set(STATE_COMPONENTS))
            self.assertTrue(set(STATE_COMPONENTS).issubset(facts['state_' + side]))
            self.assertEqual(abstract_state(facts['state_' + side])['observation_completeness'], 'complete')
            for component in part['component_refs'].values():
                self.assertIn(component['status'], ('observed', 'not_applicable', 'unavailable'))
                self.assertTrue(component['refs'])
                for ref in component['refs']:
                    self.assertEqual(ref['path'], self.runtime['raw_path'])
                    self.assertEqual(ref['sha256'], hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest())
                    self.assertEqual(self.events[ref['event_index']]['event_index'], ref['event_index'])
                    self.assertLessEqual(ref['event_index'], boundary['event_index'])
                    self.assertLessEqual(self.events[ref['event_index']]['monotonic_ns'],
                                         self.events[boundary['event_index']]['monotonic_ns'])
        self.assertIsNotNone(observation_edge(self.row(facts)))

    def assert_incomplete(self, facts, *, base=None):
        self.assertNotEqual(facts['state_projection_receipt']['status'], 'observed')
        self.assertTrue(facts['state_projection_receipt']['reasons'])
        self.assertIsNone(observation_edge(self.row(facts)))
        for side in ('before', 'after'):
            self.assertFalse(facts['state_' + side]['observation_complete'])
        if base is not None:
            self.assertIs(facts['observation_complete'], base)

    def test_portable_historical_raw_all_three_adapters_and_multiwindow(self):
        for label in FIXTURES:
            with self.subTest(label=label):
                self.fixture(label)
                original = deepcopy(self.facts)
                facts = self.project()
                self.assert_refs(facts)
                self.assertEqual(self.facts, original)
                if label == 'recovery-multi-judged':
                    self.assertEqual(facts['state_after']['recovery_cycle_bucket'], '2plus')
                    refs = facts['state_projection_receipt']['after']['component_refs']['network_phase']['refs']
                    restore = [self.events[r['event_index']] for r in refs
                               if self.events[r['event_index']]['event'] == 'discovery_restore_boundary']
                    self.assertEqual(len(restore), len(self.config['environment_windows']))
                if label == 'projection-multi-judged':
                    self.assertEqual(len(facts['state_projection_receipt']['endpoint_inventory']), 8)
                    self.assertEqual(facts['state_after']['observed_match_class'], 'unknown')

    def test_late_join_delivery_is_supported_including_fuzz_variant(self):
        for target in ('discovery.late_join_delivery', 'discovery.late_join_delivery.fuzz_v1'):
            with self.subTest(target=target):
                self.fixture()
                self.config['target_id'] = target
                self.assert_refs(self.project())

    def test_cross_worker_ack_merge_uses_real_controller_boundary(self):
        self.fixture('late-join-delivery-judged')
        writer = self.events[87]
        reader = self.events[90]
        self.assertEqual((writer['event'], reader['event']), ('command_done', 'command_done'))
        self.assertLess(writer['event_index'], reader['event_index'])
        self.assertGreater(writer['monotonic_ns'], reader['monotonic_ns'])
        original = deepcopy(self.events)
        facts = self.project()
        self.assert_refs(facts)
        boundary = facts['state_projection_receipt']['after']['boundary_ref']
        self.assertEqual(boundary['event_index'], 91)
        self.assertEqual(self.events[91]['event'], 'identity_observation')
        self.assertEqual(self.events[91]['role'], 'controller')
        self.assertEqual(self.events, original)  # No timestamp sorting or fabrication.

    def test_recovery_after_uses_real_final_observation_step(self):
        for label, index in (('recovery-single-judged', 124), ('recovery-multi-judged', 612)):
            with self.subTest(label=label):
                self.fixture(label)
                facts = self.project()
                self.assert_refs(facts)
                self.assertEqual(facts['state_projection_receipt']['after']['boundary_ref']['event_index'], index)
                boundary = self.events[index]
                self.assertEqual(boundary['event'], 'state_step_after')
                self.assertEqual(boundary['role'], 'controller')
                self.assertEqual(boundary['step_id'], 'observe_final_graph')
                self.assertEqual(boundary['receipt_kind'], 'observation')

    def test_inverted_ack_merge_needs_real_postproof_live_boundary(self):
        for defect in ('missing_boundary', 'boundary_before_ack', 'boundary_after_shutdown', 'stale_sample'):
            with self.subTest(defect=defect):
                self.fixture('late-join-delivery-judged')
                boundary = self.events[91]
                if defect == 'missing_boundary':
                    boundary['event'] = 'fixture_missing_boundary'
                elif defect == 'boundary_before_ack':
                    boundary['monotonic_ns'] = self.events[87]['monotonic_ns'] - 1
                elif defect == 'boundary_after_shutdown':
                    boundary['monotonic_ns'] = self.event('worker_shutdown', role='writer')['monotonic_ns'] + 1
                else:
                    self.events[85]['sampled_monotonic_ns'] = self.event('window_close', window='post_match_delivery')['monotonic_ns'] - 1
                self.assert_incomplete(self.project())

    def test_before_absence_does_not_borrow_reader_qos_or_match(self):
        for label, live in (('late-pair-judged', 'writer'), ('projection-pair-judged', 'none')):
            with self.subTest(label=label):
                self.fixture(label)
                facts = self.project()
                self.assert_refs(facts)
                before = facts['state_before']
                self.assertEqual(before['live_roles'], live)
                self.assertEqual(before['compatibility_class'], 'unknown')
                self.assertEqual(before['creation_order_class'], 'unresolved')
                self.assertEqual(before['observed_match_class'], 'none')
                receipt = facts['state_projection_receipt']['before']
                self.assertNotIn('reader', receipt['identity'])
                compat = receipt['component_refs']['compatibility_class']
                self.assertEqual(compat['status'], 'not_applicable')
                self.assertIn('not_yet_created', compat['reason'])

    def test_legacy_missing_qos_policies_remain_unknown_not_whole_qos_compatible(self):
        self.fixture()
        facts = self.project()
        self.assert_refs(facts)
        self.assertEqual(facts['state_after']['compatibility_class'], 'unknown')
        scope = facts['state_projection_receipt']['after']['component_refs']['compatibility_class']['policy_scope']
        self.assertIn('deadline', scope['unknown_policies'])

    def test_actual_graph_deadline_and_reliability_inequalities(self):
        for offered, requested, reliable, expected in ((10, 20, True, 'compatible'),
                                                       (20, 10, True, 'incompatible'),
                                                       (10, 20, False, 'incompatible')):
            with self.subTest(offered=offered, requested=requested, reliable=reliable):
                self.fixture('projection-pair-judged')
                for e in self.events:
                    if e['event'] == 'graph_snapshot':
                        for member in e.get('publishers', []):
                            member['qos'].update(deadline_ns=offered, reliability='reliable' if reliable else 'best_effort')
                        for member in e.get('subscriptions', []):
                            member['qos'].update(deadline_ns=requested, reliability='reliable')
                    elif e['event'] in ('endpoint_create_begin', 'endpoint_created'):
                        e['qos'].update(deadline_ns=offered if e['role'] == 'writer' else requested,
                                        reliability='best_effort' if e['role'] == 'writer' and not reliable else 'reliable')
                facts = self.project()
                self.assert_refs(facts)
                self.assertEqual(facts['state_after']['compatibility_class'], expected)
                policy = facts['state_projection_receipt']['after']['component_refs']['compatibility_class']
                self.assertIn('history', policy['excluded_policies'])

    def test_create_graph_policy_disagreement_stays_visible_not_fake_compatible(self):
        self.fixture('projection-pair-judged')
        for e in self.events:
            if e['event'] == 'graph_snapshot':
                for member in e.get('publishers', []):
                    member['qos']['deadline_ns'] = 1
        facts = self.project()
        self.assert_refs(facts)
        self.assertEqual(facts['state_after']['compatibility_class'], 'unknown')
        component = facts['state_projection_receipt']['after']['component_refs']['compatibility_class']
        self.assertEqual(component['reason'], 'create_graph_policy_disagreement')
        self.assertIn('writer:deadline_ns', component['create_graph_disagreements'])

    def test_sentinel_does_not_close_missing_middle_and_fail_is_not_incomplete(self):
        self.fixture()
        contract = self.event('delivery_contract')
        expected = contract['expected_sequences']
        middle = expected[len(expected) // 2]
        for e in self.events:
            if e['event'] == 'receive' and e.get('send_sequence') == middle:
                e['event'] = 'fixture_missing_receive'  # Preserve real indexes/chronology.
        self.facts['oracle_verdict'] = 'fail'
        facts = self.project()
        self.assert_refs(facts)
        self.assertEqual(facts['state_after']['delivery_closure_class'], 'open')
        closure = facts['state_projection_receipt']['after']['component_refs']['delivery_closure_class']
        self.assertTrue(closure['sentinel_received'])
        self.assertEqual(closure['missing_sequences'], [middle])
        self.assertEqual(facts['oracle_verdict'], 'fail')

    def test_missing_required_fields_do_not_get_config_defaults(self):
        for field in ('pid', 'endpoint_generation', 'endpoint_gid'):
            with self.subTest(field=field):
                self.fixture()
                self.event('endpoint_created', role='reader').pop(field)
                self.assert_incomplete(self.project(), base=False)

    def test_forged_identity_and_wrong_transport_generation_are_rejected(self):
        for key, value in (('pid', 987654), ('endpoint_generation', 'other-generation'),
                           ('endpoint_gid', 'other-endpoint')):
            with self.subTest(key=key):
                self.fixture()
                token = self.event('endpoint_created', role='writer')
                query = [e for e in self.events if e['event'] == 'match_snapshot' and e['role'] == 'writer'][-1]
                query[key] = value
                self.assert_incomplete(self.project(), base=False)

    def test_old_snapshot_and_wrong_command_ack_do_not_fall_back(self):
        for mode in ('old_sample', 'ack', 'boundary'):
            with self.subTest(mode=mode):
                self.fixture()
                latest = [e for e in self.events if e['event'] == 'match_snapshot' and e['role'] == 'writer'][-1]
                if mode == 'old_sample':
                    latest['sampled_monotonic_ns'] = 0
                elif mode == 'ack':
                    self.event('command_done', role='writer', command_id=latest['command_id'])['command_id'] = -1
                else:
                    self.event('window_close', window='post_match_delivery')['event_index'] = 0
                self.assert_incomplete(self.project())

    def test_recovery_old_precondition_and_wrong_window_chain_are_rejected(self):
        for mode in ('snapshot_ref', 'match_ref', 'missing_restore', 'window_generation'):
            with self.subTest(mode=mode):
                self.fixture('recovery-multi-judged')
                snapshot = self.event('discovery_precondition_snapshot')
                if mode == 'snapshot_ref':
                    self.event('mutation_begin')['precondition_snapshot_ref'] = 0
                elif mode == 'match_ref':
                    snapshot['matched']['matches']['writer'] = snapshot['matched']['matches']['reader']
                elif mode == 'missing_restore':
                    self.event('discovery_restore_boundary')['event'] = 'fixture_missing_restore'
                else:
                    self.event('discovery_window_restore_verified')['window_index'] = 99
                self.assert_incomplete(self.project(), base=False)

    def test_component_timestamp_cannot_cross_its_boundary(self):
        self.fixture()
        begin = self.event('mutation_begin')
        self.event('environment_preflight')['monotonic_ns'] = begin['monotonic_ns'] + 1
        self.assert_incomplete(self.project(), base=False)

    def test_raw_hash_content_run_domain_and_indexes_are_checked(self):
        for mode in ('hash', 'runtime_events', 'domain', 'index'):
            with self.subTest(mode=mode):
                self.fixture()
                self.project()
                if mode == 'hash':
                    self.runtime['raw_sha256'] = '0' * 64
                elif mode == 'runtime_events':
                    self.runtime['events'] = []
                elif mode == 'domain':
                    self.events[0]['domain_id'] = -1
                    self.project()
                else:
                    self.events[0]['event_index'] = 10
                    self.project()
                self.assert_incomplete(self.project(rewrite=False), base=False)

    def test_nonexecution_summary_only_and_oracle_pass_never_create_complete(self):
        for mode in ('nonexecution', 'missing_raw', 'summary_only', 'bad_adapter'):
            with self.subTest(mode=mode):
                self.fixture()
                self.project()
                self.facts['oracle_verdict'] = 'pass'
                if mode == 'nonexecution':
                    self.runtime['real_execution'] = False
                elif mode == 'missing_raw':
                    Path(self.runtime['raw_path']).unlink()
                elif mode == 'summary_only':
                    self.runtime['events'] = []
                    self.events = []
                else:
                    self.facts['evidence_error'] = 'invalid endpoint evidence'
                self.assert_incomplete(self.project(rewrite=False), base=False)

    def test_state_unsupported_or_missing_before_preserves_audited_base_identity(self):
        for mode in ('unsupported', 'missing_before'):
            with self.subTest(mode=mode):
                self.fixture()
                self.facts['endpoint_identity_consistent'] = True
                if mode == 'unsupported':
                    self.config['target_id'] = 'other.target'
                else:
                    self.event('mutation_begin').pop('preconditions_snapshot')
                facts = self.project()
                self.assert_incomplete(facts, base=True)
                self.assertTrue(facts['endpoint_identity_consistent'])

    def test_projection_never_overwrites_incomplete_base_flags(self):
        for flag in ('window_complete', 'endpoint_identity_consistent'):
            with self.subTest(flag=flag):
                self.fixture()
                self.facts[flag] = False
                facts = self.project()
                self.assert_incomplete(facts, base=False)
                self.assertIs(facts[flag], False)

    def test_adapter_declared_pid_must_match_raw_endpoint(self):
        self.fixture('projection-pair-judged')
        self.facts['writer']['pid'] += 1
        self.assert_incomplete(self.project(), base=False)

    def test_projection_primary_pair_alias_binds_real_alias_tokens(self):
        self.fixture('projection-multi-judged')
        original = self.event('endpoint_created', role='writer')
        alternate = self.event('endpoint_created', role='writer2')
        self.facts['writer'] = dict(id=alternate['endpoint_gid'], generation=alternate['endpoint_generation'],
                                    created_ns=alternate['monotonic_ns'], pid=alternate['pid'])
        self.assertNotEqual(original['endpoint_gid'], alternate['endpoint_gid'])
        facts = self.project()
        self.assert_refs(facts)
        identity = facts['state_projection_receipt']['after']['identity']['writer']
        self.assertEqual(identity['actual_role'], 'writer2')
        self.assertEqual(identity['endpoint_gid'], alternate['endpoint_gid'])

    def test_recovery_rebuild_resolves_boundary_generations_with_causal_raw_chain(self):
        self.fixture('recovery-single-judged')
        old = self.event('endpoint_created', role='reader')
        new = self.event('endpoint_created', role='reader2')
        new_index = new['event_index']
        # This fixture variation models a canonical role restart, not a mere
        # extra reader stimulus. Existing endpoints/indices remain historical.
        new['role'] = 'reader'
        self.facts['reader'] = dict(id=new['endpoint_gid'], generation=new['endpoint_generation'],
                                    created_ns=new['created_monotonic_ns'])
        for worker in self.runtime['execution_metadata']['workers']:
            if worker['pid'] == new['pid']:
                worker['role'] = 'reader'
        floor = new['monotonic_ns']
        stop_id = 800
        template = {k: old[k] for k in ('run_id', 'domain_id', 'pid', 'endpoint_generation')}
        stop = dict(template, event='worker_command_requested', role='reader', command='stop',
                    command_id=stop_id, event_index=-1, monotonic_ns=floor - 4,
                    raw_event_ref='fixture-restart-stop')
        shutdown = dict(template, event='worker_shutdown', role='reader', stop_command_id=stop_id,
                        event_index=-1, monotonic_ns=floor - 3, raw_event_ref='fixture-restart-shutdown')
        exited = dict(template, event='process_exit', role='reader', returncode=0,
                      event_index=-1, monotonic_ns=floor - 2, raw_event_ref='fixture-restart-exit')
        started = dict(event='process_started', role='reader', pid=new['pid'],
                       endpoint_generation=new['endpoint_generation'], run_id=new['run_id'],
                       domain_id=new['domain_id'], event_index=-1, monotonic_ns=floor - 1,
                       raw_event_ref='fixture-restart-start')
        # Rebind only post-restart public queries/deliveries to the new worker.
        for event in self.events[new_index + 1:]:
            if event.get('role') == 'reader' and event['event'] in ('match_snapshot', 'graph_snapshot', 'command_done', 'receive'):
                event.update(pid=new['pid'], endpoint_generation=new['endpoint_generation'])
                if 'endpoint_gid' in event:
                    event['endpoint_gid'] = new['endpoint_gid']
                if 'reader_gid' in event:
                    event['reader_gid'] = new['endpoint_gid']
            if event['event'] == 'graph_snapshot':
                for entry in event.get('subscriptions', []):
                    if entry.get('endpoint_gid') == old['endpoint_gid']:
                        entry['endpoint_gid'] = new['endpoint_gid']
        self.events[new_index:new_index] = [stop, shutdown, exited, started]
        for index, event in enumerate(self.events):
            event['event_index'] = index
        facts = self.project()
        self.assert_refs(facts)
        receipt = facts['state_projection_receipt']
        self.assertEqual(receipt['before']['identity']['reader']['endpoint_generation'], old['endpoint_generation'])
        self.assertEqual(receipt['after']['identity']['reader']['endpoint_generation'], new['endpoint_generation'])
        self.assertEqual(len(receipt['generation_transitions']), 1)
        transition = receipt['generation_transitions'][0]
        indexes = [transition[key]['event_index'] for key in
                   ('old_endpoint_ref', 'shutdown_ref', 'process_ref', 'new_endpoint_ref')]
        self.assertEqual(indexes, sorted(indexes))
        baseline = deepcopy(self.events)
        for defect in ('missing_stop', 'stop_generation', 'start_generation', 'exit_generation', 'clock'):
            with self.subTest(defect=defect):
                self.events = deepcopy(baseline)
                if defect == 'missing_stop':
                    self.event('worker_shutdown', stop_command_id=stop_id).pop('stop_command_id')
                elif defect == 'stop_generation':
                    self.event('worker_command_requested', command_id=stop_id)['endpoint_generation'] = 'wrong'
                elif defect == 'start_generation':
                    self.event('process_started', raw_event_ref='fixture-restart-start')['endpoint_generation'] = 'wrong'
                elif defect == 'exit_generation':
                    self.event('process_exit', raw_event_ref='fixture-restart-exit')['endpoint_generation'] = 'wrong'
                else:
                    self.event('worker_shutdown', stop_command_id=stop_id)['monotonic_ns'] = floor + 1
                self.assert_incomplete(self.project())

    def test_adapter_rejected_canonical_generation_history_is_explicitly_unsupported(self):
        self.fixture('recovery-single-judged')
        new = self.event('endpoint_created', role='reader2')
        new['role'] = 'reader'
        self.facts['evidence_error'] = 'expected one endpoint_created:reader; observed 2'
        self.facts['endpoint_identity_complete'] = False
        facts = self.project()
        self.assert_incomplete(facts, base=False)
        self.assertEqual(facts['state_projection_receipt']['status'], 'unsupported')
        self.assertIn('adapter_canonical_generation_history_not_supported', facts['state_projection_receipt']['reasons'][0])

    def test_all_adapters_call_shared_projector_on_their_fact_return_paths(self):
        from r2f.campaign import runtime_adapter, recovery_adapter, projection_adapter
        for producer in (runtime_adapter.facts_from_events, recovery_adapter.recovery_facts,
                         recovery_adapter._multi_window_recovery_facts, projection_adapter.projection_facts):
            with self.subTest(producer=producer.__name__):
                self.assertIn('return attach_state_observations(facts, events, runtime, config)', inspect.getsource(producer))
        with patch('r2f.campaign.state_projection.attach_state_observations',
                   side_effect=lambda facts, events, runtime, config: dict(facts, shared_projection_called=True)) as call:
            for target in ('discovery.late_endpoint', 'discovery.late_join_delivery',
                           'discovery.interruption_recovery', 'builtin.discovery_projection_reconciliation'):
                facts = runtime_adapter.facts_from_events([], {}, {'target_id': target})
                self.assertTrue(facts['shared_projection_called'])
            self.assertEqual(call.call_count, 4)


if __name__ == '__main__':
    unittest.main()
