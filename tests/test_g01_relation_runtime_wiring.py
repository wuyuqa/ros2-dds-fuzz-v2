"""Offline G01 wiring tests: all worker/environment/source APIs are doubles.

No rclpy, ROS process, namespace, campaign or real DDS traffic is started.
Fixtures deliberately do not qualify execution evidence.
"""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json
import hashlib
import tempfile
import time
import unittest
from unittest.mock import patch

from r2f.modules.s1.oracles import evaluate_match_compatibility_guard
from r2f.runtime import ros2_pair as pair
from r2f.runtime.relation_observer import g01_runtime_observation

TARGET = 'discovery.match_compatibility.fuzz_v1'


def config(kind='RR', **overrides):
    reliabilities = {'R': 'reliable', 'B': 'best_effort'}
    w, r = (reliabilities[k] for k in kind)
    return dict(target_id=TARGET, carrier='ros2_endpoint', mutation_kind='endpoint_qos_variation',
                fuzz_surfaces=['G01'], writer_qos={'reliability': w}, reader_qos={'reliability': r},
                submitted_parameters={'writer_qos.reliability': w, 'reader_qos.reliability': r},
                run_id='g01-offline-fixture', domain_id=173, match_timeout_sec=0,
                publish_interval_sec=0, delivery_timeout_sec=0, **overrides)


class OfflinePool:
    """A pipe-shaped deterministic double, not a ROS execution provider."""
    matched = 1
    deliver = True
    wrong_readback = False
    extra_graph = False
    actual_receipts = False
    graph_depth_zero = False
    instances = []

    def __init__(self, c, env, output_dir, emit, **kwargs):
        self.config, self.emit = c, emit
        self.metadata, self.tokens, self.serial = [], {}, 0
        self.instances.append(self)

    def row(self, role, event, **fields):
        token = self.tokens[role]
        self.emit(event, role=role, pid=token['pid'], pgid=token['pid'],
                  endpoint_generation=token['endpoint_generation'],
                  rmw_identifier=pair.role_rmw(self.config, role), **fields)

    def graph_qos(self, role):
        qos = deepcopy(self.config[role + '_qos'])
        if self.graph_depth_zero:
            qos.update(depth=0, history='unknown')
        return qos

    def graph(self):
        return {'publishers': [{'endpoint_gid': 'writer-gid', 'topic_type': 'std_msgs/msg/String',
                                  'qos': self.graph_qos('writer')}] + (
                    [{'endpoint_gid': 'foreign-writer'}] if self.extra_graph else []),
                'subscriptions': [{'endpoint_gid': 'reader-gid', 'topic_type': 'std_msgs/msg/String',
                                     'qos': self.graph_qos('reader')}]}

    def start(self, role):
        self.tokens[role] = {'pid': 4001 if role == 'writer' else 4002,
                             'endpoint_generation': role + '-generation'}
        self.metadata.append(dict(role=role, pid=self.tokens[role]['pid'], returncode=None))
        self.emit('process_started', role=role, pid=self.tokens[role]['pid'], worker_generation=1)
        self.row(role, 'endpoint_created', endpoint_gid=role + '-gid',
                 created_monotonic_ns=time.monotonic_ns(), topic=self.config['topic'],
                 message_type='std_msgs/msg/String', qos=self.config[role + '_qos'])
        qos = self.graph_qos(role)
        if self.wrong_readback:
            qos['reliability'] = 'invalid'
        extra = {}
        if self.actual_receipts:
            actual = deepcopy(self.config[role + '_qos'])
            actual.setdefault('history', 'keep_last')
            actual.setdefault('deadline_ns', 0)
            actual.setdefault('liveliness', 'automatic')
            actual.setdefault('liveliness_lease_duration_ns', 0)
            # Explicit fixture only: not a real C-API call/library proof.
            actual['deadline_ns'] = 9223372036854775807
            actual['liveliness_lease_duration_ns'] = 9223372036854775807
            extra['actual_endpoint_qos'] = {
                'schema': 'r2f.public-endpoint-qos/v1', 'status': 'observed',
                'api': 'rcl_' + ('publisher' if role == 'writer' else 'subscription') + '_get_actual_qos',
                'pid': self.tokens[role]['pid'], 'sampled_monotonic_ns': time.monotonic_ns(),
                'qos': actual, 'library': {'path': '/offline/fixture/librcl.so', 'sha256': 'a' * 64},
                'abi': {'verified': True}, 'offline_fixture': True}
        self.row(role, 'qos_binding_observed', observed_self=qos, **extra,
                 observed_self_gid=role + '-gid',
                 observation_api='get_' + ('publishers' if role == 'writer' else 'subscriptions') + '_info_by_topic')

    def command(self, role, command, **fields):
        self.serial += 1
        cid = self.serial
        token = self.tokens[role]
        self.emit('worker_command_requested', role=role, pid=token['pid'],
                  endpoint_generation=token['endpoint_generation'], command_id=cid,
                  command=command, origin='controller_pipe', requested_monotonic_ns=time.monotonic_ns())
        if command == 'snapshot':
            self.row(role, 'match_snapshot', endpoint_gid=role + '-gid', command_id=cid,
                     count_api='get_subscription_count' if role == 'writer' else 'get_publisher_count',
                     matched_count=self.matched, sampled_monotonic_ns=time.monotonic_ns())
            self.row(role, 'graph_snapshot', command_id=cid, **self.graph())
        elif command == 'publish':
            self.row(role, 'send', writer_gid='writer-gid', publish_returned=True,
                     send_sequence=fields['send_sequence'], phase=fields['phase'])
            if self.deliver and self.matched:
                self.row('reader', 'receive', reader_gid='reader-gid', publisher_gid='writer-gid',
                         payload_run_id=self.config['run_id'], identity_basis='message_info_gid',
                         send_sequence=fields['send_sequence'], phase=fields['phase'])
        self.row(role, 'command_done', command_id=cid)
        return cid

    def wait(self, predicate, timeout, name):
        if not predicate():
            raise TimeoutError('offline_fixture_timeout:' + name)

    def close(self):
        for meta in self.metadata:
            self.row(meta['role'], 'worker_shutdown')
            meta['returncode'] = 0
            self.emit('process_exit', role=meta['role'], pid=meta['pid'], returncode=0, worker_generation=1)


class G01RuntimeWiring(unittest.TestCase):
    def run_offline(self, kind='RR', *, matched=1, deliver=True, wrong_readback=False,
                    extra_graph=False, actual_receipts=False, graph_depth_zero=False, **overrides):
        pool_type = type('ConfiguredOfflinePool', (OfflinePool,),
                         dict(matched=matched, deliver=deliver, wrong_readback=wrong_readback,
                              extra_graph=extra_graph, actual_receipts=actual_receipts,
                              graph_depth_zero=graph_depth_zero))
        sidecar = SimpleNamespace(base_env={}, role_env={}, capture_worker=lambda *a: {},
                                  collect=lambda *a: {'status': 'offline_fixture', 'candidate_count': 0})
        def acquire(c, out, emit, **kwargs):
            owner = {'pid': __import__('os').getpid(), 'campaign_id': c['campaign_id'],
                     'domain_id': c['domain_id'], 'state_dir': str(out)}
            names = ['campaign-' + hashlib.sha256(c['campaign_id'].encode()).hexdigest() + '.lock',
                     'domain-' + str(c['domain_id']) + '.lock',
                     'state-' + hashlib.sha256(str(out).encode()).hexdigest() + '.lock']
            emit('isolation_locks_acquired', locks=[{'path': '/offline/' + name,
                 'owner': owner, 'acquired': True, 'offline_fixture': True} for name in names])
            emit('environment_preflight', ok=True, environment={'ok': True, 'offline_fixture': True})
            return SimpleNamespace(__exit__=lambda *a: None), {'ok': True, 'offline_fixture': True}, {}
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(pair, 'WorkerPool', pool_type), \
                patch.object(pair, 'RoleRmwWorkerPool', pool_type), \
                patch.object(pair, 'acquire_pair_environment', side_effect=acquire), \
                patch('r2f.runtime.instrumentation_bridge.prepare', return_value=sidecar), \
                patch('r2f.runtime.namespace_pair.execution_chain_identity', return_value={'offline': True}), \
                patch('r2f.runtime.namespace_pair.same_source', return_value=True):
            c = config(kind, **overrides)
            result = pair.run_pair(c, Path(tmp) / 'state')
            # Check actual persisted bytes; no summary-only success assertions.
            if result['raw_path']:
                raw = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
                self.assertEqual(raw, result['events'])
            return result, pair.normalize_config(c)

    def test_actual_qos_fixture_preserves_graph_depth_and_default_resolution(self):
        from r2f.runtime.qos_binding import compare_public_qos
        for kind in ('RR', 'RB', 'BB', 'BR'):
            with self.subTest(kind=kind):
                result, c = self.run_offline(kind, actual_receipts=True, graph_depth_zero=True,
                                            matched=0 if kind == 'BR' else 1)
                self.assertEqual(result['runtime_status'], 'completed', result['reason'])
                self.assertTrue(result['relation_observation']['oracle_input'], result['relation_observation'])
                for row in result['events']:
                    if row['event'] == 'qos_binding_observed':
                        self.assertEqual(row['observed_self']['depth'], 0)
                        actual = row['actual_endpoint_qos']['qos']
                        self.assertEqual(actual['depth'], c[row['role'] + '_qos']['depth'])
                        compared = compare_public_qos(dict(c[row['role'] + '_qos'], deadline_ns=0), actual)
                        self.assertEqual(compared['status'], 'matched')
                        self.assertTrue(compared['default_resolutions'])

    def test_graph_depth_zero_without_actual_receipt_keeps_legacy_failure(self):
        result, _ = self.run_offline(graph_depth_zero=True)
        self.assertNotEqual(result['runtime_status'], 'completed')
        self.assertIn('actual_qos_readback_mismatch', result['reason'])
        self.assertFalse(result['mutation_receipt']['hit'])

    def test_actual_receipt_tampering_and_graph_rxo_remain_fail_closed(self):
        result, c = self.run_offline(actual_receipts=True, graph_depth_zero=True)
        self.assertEqual(result['runtime_status'], 'completed', result['reason'])
        for damage in ('pid', 'sample_before_create', 'sample_after_event', 'api', 'schema',
                       'library', 'abi', 'missing_field', 'explicit_depth', 'graph_reliability',
                       'binding_identity', 'binding_time', 'null_receipt'):
            with self.subTest(damage=damage):
                rows = deepcopy(result['events'])
                binding = next(e for e in rows if e['event'] == 'qos_binding_observed')
                receipt = binding['actual_endpoint_qos']
                token = next(e for e in rows if e['event'] == 'endpoint_created')
                if damage == 'pid': receipt['pid'] += 1
                elif damage == 'sample_before_create': receipt['sampled_monotonic_ns'] = token['created_monotonic_ns'] - 1
                elif damage == 'sample_after_event': receipt['sampled_monotonic_ns'] = binding['monotonic_ns'] + 1
                elif damage == 'api': receipt['api'] = 'rcl_subscription_get_actual_qos'
                elif damage == 'schema': receipt['schema'] = 'wrong'
                elif damage == 'library': receipt['library']['sha256'] = 'bad'
                elif damage == 'abi': receipt['abi']['verified'] = False
                elif damage == 'missing_field': receipt['qos'].pop('history')
                elif damage == 'explicit_depth': receipt['qos']['depth'] += 1
                elif damage == 'binding_identity': binding['pid'] += 1
                elif damage == 'binding_time': binding['monotonic_ns'] = token['monotonic_ns'] - 1
                elif damage == 'null_receipt': binding['actual_endpoint_qos'] = None
                else:
                    for row in rows:
                        if row['event'] == 'graph_snapshot':
                            row['publishers'][0]['qos']['reliability'] = 'best_effort'
                observed = g01_runtime_observation(rows, c)
                self.assertFalse(observed['oracle_input'], observed)
                self.assertFalse(observed['relation_effect_observed'])
                self.assertFalse(observed['candidate_eligible'])

    def test_all_four_pairs_reach_observer_and_existing_guard(self):
        for kind in ('RR', 'RB', 'BB', 'BR'):
            with self.subTest(kind=kind):
                result, c = self.run_offline(kind, matched=0 if kind == 'BR' else 1)
                self.assertEqual(result['runtime_status'], 'completed', result['reason'])
                self.assertEqual([r['step_id'] for r in result['precondition_receipts']], ['admit_environment'])
                self.assertEqual([r['step_id'] for r in result['observation_receipts']], ['writer_created', 'reader_created'])
                begin = next(e for e in result['events'] if e['event'] == 'mutation_begin')
                self.assertLess(begin['precondition_receipt_refs'][0]['raw_after_index'], begin['event_index'])
                rel = result['relation_observation']
                self.assertTrue(rel['oracle_input'], rel['reasons'])
                self.assertEqual(rel['reliability_prediction']['compatible'], kind != 'BR')
                self.assertIsNotNone(rel['observed_relation'])
                verdict = evaluate_match_compatibility_guard(rel['facts'])
                self.assertEqual(verdict['verdict'], 'unknown' if kind == 'BR' else 'pass')
                self.assertTrue(result['mutation_receipt']['hit'])  # real-readback-shaped config gate
                self.assertEqual(result['mutation_receipt']['effect_observed'], kind != 'BR')
                self.assertFalse(rel['execution_evidence'])
                self.assertFalse(rel['candidate_eligible'])
                if kind == 'BR':
                    self.assertFalse(rel['facts']['observed']['observer_complete'])

    def test_incompatible_pair_with_actual_match_fails_guard_even_without_data(self):
        result, _ = self.run_offline('BR', deliver=False)
        verdict = evaluate_match_compatibility_guard(result['facts'])
        self.assertEqual(verdict['verdict'], 'fail')
        self.assertIn('prohibited_match_observed', verdict['reason_codes'])

    def test_incompatible_pair_with_delivery_is_prohibited(self):
        result, _ = self.run_offline('BR')
        self.assertEqual(evaluate_match_compatibility_guard(result['facts'])['reason_codes'],
                         ['prohibited_delivery_observed'])

    def test_all_pairs_no_match_are_unknown_not_sut_failure(self):
        for kind in ('RR', 'RB', 'BB', 'BR'):
            with self.subTest(kind=kind):
                result, _ = self.run_offline(kind, matched=0)
                self.assertEqual(result['runtime_status'], 'completed')
                verdict = evaluate_match_compatibility_guard(result['facts'])
                self.assertEqual(verdict['verdict'], 'unknown')
                self.assertTrue(result['mutation_receipt']['hit'])
                self.assertFalse(result['mutation_receipt']['effect_observed'])
                self.assertFalse(result['relation_observation']['relation_effect_observed'])
                self.assertEqual(verdict['reason_codes'], [
                    'prohibited_event_absence_not_established' if kind == 'BR'
                    else 'compatible_pair_unmatched_within_finite_window'])

    def test_compatible_match_without_delivery_does_not_add_delivery_obligation(self):
        for kind in ('RR', 'RB', 'BB'):
            with self.subTest(kind=kind):
                result, _ = self.run_offline(kind, deliver=False)
                self.assertEqual(evaluate_match_compatibility_guard(result['facts'])['verdict'], 'pass')
                self.assertIsNone(result['facts']['delivery'])

    def test_actual_readback_not_requested_qos_is_the_configuration_gate(self):
        result, _ = self.run_offline(wrong_readback=True)
        self.assertEqual(result['runtime_status'], 'failed')
        self.assertIn('actual_qos_readback_mismatch', result['reason'])
        self.assertFalse(result['mutation_receipt']['hit'])
        self.assertFalse(result['mutation_receipt']['effect_observed'])

    def test_foreign_graph_endpoint_keeps_unknown_not_runtime_abort(self):
        result, _ = self.run_offline(extra_graph=True)
        self.assertEqual(result['runtime_status'], 'completed')
        self.assertFalse(result['relation_observation']['oracle_input'])
        self.assertFalse(result['mutation_receipt']['effect_observed'])
        self.assertTrue(any(e['event'] == 'discovery_observation_unknown' for e in result['events']))

    def test_controls_do_not_claim_mutation_hit(self):
        for arm in ('baseline', 'transparent'):
            with self.subTest(arm=arm):
                result, _ = self.run_offline(arm=arm)
                self.assertEqual(result['runtime_status'], 'completed')
                self.assertFalse(result['mutation_receipt']['hit'])
                self.assertFalse(result['mutation_receipt']['effect_observed'])
        result, _ = self.run_offline(arm='negative')
        self.assertEqual(result['reason'], 'blocked:g01_negative_control_contract_not_implemented')
        self.assertFalse(result['real_execution'])

    def test_missing_evidence_and_wrong_identities_fail_closed(self):
        result, c = self.run_offline()
        events = result['events']
        def without(name):
            return [deepcopy(e) for e in events if e['event'] != name]
        cases = {name: without(name) for name in
                 ('qos_binding_observed', 'endpoint_created', 'command_done',
                  'worker_command_requested', 'match_snapshot', 'graph_snapshot', 'window_close')}
        for label, event, key, value in (
                ('wrong_pid', 'receive', 'pid', 9999),
                ('wrong_generation', 'send', 'endpoint_generation', 'other'),
                ('wrong_gid', 'receive', 'publisher_gid', 'foreign'),
                ('wrong_run', 'receive', 'payload_run_id', 'foreign'),
                ('wrong_type', 'endpoint_created', 'message_type', 'wrong'),
                ('wrong_topic', 'endpoint_created', 'topic', '/foreign'),
                ('wrong_qos', 'qos_binding_observed', 'observed_self', {'reliability': 'best_effort'}),
                ('aggregate_count', 'match_snapshot', 'matched_count', 2),
                ('wrong_count_api', 'match_snapshot', 'count_api', 'aggregate_count'),
                ('wrong_request_identity', 'worker_command_requested', 'pid', 9999),
                ('unclosed_send', 'receive', 'send_sequence', 99),
                ('old_sample', 'match_snapshot', 'sampled_monotonic_ns', 0)):
            rows = deepcopy(events)
            for row in rows:
                if row['event'] == event:
                    row[key] = value
            cases[label] = rows
        for label, rows in cases.items():
            with self.subTest(label=label):
                for i, row in enumerate(rows):
                    row['event_index'] = i
                observed = g01_runtime_observation(rows, c)
                self.assertFalse(observed['oracle_input'], (label, observed))
                self.assertFalse(observed['relation_effect_observed'])
                self.assertIsNone(observed['facts'])

    def test_chronology_duplicates_and_graph_type_are_rejected(self):
        result, c = self.run_offline()
        for damage in ('duplicate_ack', 'duplicate_request', 'duplicate_open', 'postclose_ack',
                       'graph_type', 'graph_qos', 'late_send', 'missing_receive_identity'):
            with self.subTest(damage=damage):
                rows = deepcopy(result['events'])
                if damage.startswith('duplicate_'):
                    name = {'duplicate_ack': 'command_done', 'duplicate_request': 'worker_command_requested',
                            'duplicate_open': 'window_open'}[damage]
                    row = next(e for e in rows if e['event'] == name)
                    rows.insert(rows.index(row) + 1, deepcopy(row))
                elif damage == 'postclose_ack':
                    close = next(e['monotonic_ns'] for e in rows if e['event'] == 'window_close')
                    for row in rows:
                        if row['event'] == 'command_done':
                            row['monotonic_ns'] = close + 1
                elif damage in ('graph_type', 'graph_qos'):
                    for row in rows:
                        if row['event'] == 'graph_snapshot':
                            if damage == 'graph_type':
                                row['publishers'][0]['topic_type'] = 'other'
                            else:
                                row['publishers'][0]['qos']['reliability'] = 'best_effort'
                elif damage == 'late_send':
                    close = next(e['monotonic_ns'] for e in rows if e['event'] == 'window_close')
                    next(e for e in rows if e['event'] == 'send')['monotonic_ns'] = close + 1
                else:
                    for row in rows:
                        if row['event'] == 'receive':
                            row['publisher_gid'] = None
                            row.pop('identity_basis', None)
                for i, row in enumerate(rows):
                    row['event_index'] = i
                observed = g01_runtime_observation(rows, c)
                self.assertFalse(observed['oracle_input'], observed)
                self.assertFalse(observed['relation_effect_observed'])

    def test_unsupported_public_qos_stays_blocked_before_execution(self):
        c = config()
        c['writer_qos']['ownership'] = 'exclusive'
        with tempfile.TemporaryDirectory() as tmp:
            result = pair.run_pair(c, Path(tmp))
        self.assertFalse(result['real_execution'])
        self.assertIn('unsupported_qos_field:writer_qos:ownership', result['reason'])
        self.assertFalse(result['mutation_receipt']['hit'])

    def test_unavailable_sample_gid_retains_explicit_correlation_limitation(self):
        result, c = self.run_offline()
        rows = deepcopy(result['events'])
        for row in rows:
            if row['event'] == 'receive':
                row.update(publisher_gid=None, publisher_gid_status='unavailable_api',
                           identity_basis='endpoint_correlated')
        observed = g01_runtime_observation(rows, c)
        self.assertTrue(observed['oracle_input'], observed['reasons'])
        self.assertTrue(observed['facts']['match']['identity_correlated'])
        self.assertIn('publisher_gid_unavailable_api_endpoint_correlated_not_direct_sample_gid',
                      observed['identity_limitations'])
        self.assertFalse(any(row.get('publisher_gid') for row in rows if row['event'] == 'receive'))


if __name__ == '__main__':
    unittest.main()
