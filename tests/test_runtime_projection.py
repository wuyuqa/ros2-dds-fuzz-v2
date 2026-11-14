"""Fourth runtime contracts, offline only; never manufacture execution metadata."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from r2f.runtime.ros2_pair import normalize_config, run_pair
from r2f.runtime.projection_pair import (TARGET, DEFAULT_QOS, INFINITE_NS,
    normalize_projection_config, validate_qos, export_graph, controlled_graph,
    finalize_projection, namespace_body)
from r2f.runtime.worker import create_public_qos


def config(arm='baseline', **extra):
    selected = dict(DEFAULT_QOS, deadline_ns=1250000000) if arm == 'mutated' else dict(DEFAULT_QOS)
    return normalize_projection_config(normalize_config({
        'target_id': TARGET, 'mutation_kind': 'endpoint_qos_variation', 'carrier': 'ros2_endpoint',
        'arm': arm, 'run_id': 'projection-offline', 'qos': selected, **extra}))


class ProjectionRuntime(unittest.TestCase):
    def test_normalization_preserves_each_submitted_policy_and_control(self):
        for arm in ('baseline', 'transparent', 'negative', 'mutated'):
            c = config(arm)
            self.assertEqual(c['writer_qos'], c['qos'])
            self.assertEqual(c['reader_qos'], c['qos'])
            self.assertEqual(c['qos']['deadline_ns'], 1250000000 if arm == 'mutated' else INFINITE_NS)
            self.assertEqual(c['fault_id'], 'controlled_projection_field_mismatch' if arm == 'negative' else None)
            self.assertTrue(c['observe_extended_qos'])
        # First and second profiles retain the original defaults.
        self.assertEqual(normalize_config({})['qos'], {'reliability': 'reliable', 'durability': 'volatile', 'depth': 32})

    def test_bad_or_silently_ineffective_inputs_are_rejected(self):
        for key in ('deadline_ns', 'liveliness_lease_duration_ns'):
            for bad in (True, -1, 1.2, '2', INFINITE_NS + 1):
                with self.subTest(key=key, value=bad), self.assertRaises(ValueError):
                    validate_qos({key: bad})
        for values in ({'unimplemented_qos': 1}, {'liveliness': 'manual_by_participant'}):
            with self.assertRaises(ValueError):
                validate_qos(values)
        with self.assertRaisesRegex(ValueError, 'does_not_change'):
            config('mutated', qos=DEFAULT_QOS)
        with self.assertRaisesRegex(ValueError, 'neutral'):
            config('baseline', qos={**DEFAULT_QOS, 'deadline_ns': 2})
        with self.assertRaisesRegex(ValueError, 'submitted_qos_mismatch'):
            config('mutated', mutation_parameters={'submitted_qos': DEFAULT_QOS})

    def test_public_qos_constructor_retains_ns_and_infinity(self):
        duration = Mock(side_effect=lambda **kw: kw['nanoseconds'])
        profile = Mock(side_effect=lambda **kw: kw)
        policies = SimpleNamespace(QoSProfile=profile,
            ReliabilityPolicy=SimpleNamespace(RELIABLE='RELIABLE'), DurabilityPolicy=SimpleNamespace(VOLATILE='VOLATILE'),
            HistoryPolicy=SimpleNamespace(KEEP_LAST='KEEP_LAST'), LivelinessPolicy=SimpleNamespace(AUTOMATIC='AUTOMATIC'))
        with patch.dict('sys.modules', {'rclpy.qos': policies, 'rclpy.duration': SimpleNamespace(Duration=duration)}):
            result = create_public_qos({**DEFAULT_QOS, 'deadline_ns': 2123456789})
        self.assertEqual(result['deadline'], 2123456789)
        self.assertEqual(result['liveliness_lease_duration'], INFINITE_NS)
        self.assertEqual(result['liveliness'], 'AUTOMATIC')
        self.assertEqual(duration.call_count, 2)

    def test_failed_host_preflight_cannot_bypass_via_projection_route(self):
        with tempfile.TemporaryDirectory() as temp, patch('r2f.runtime.namespace_pair.check_environment',
                return_value={'ok': False, 'reason': 'offline_missing_environment'}), \
                patch('r2f.runtime.namespace_pair._supervise') as start:
            c = config(lock_root=temp + '/locks')
            result = run_pair(c, Path(temp) / 'state')
            self.assertFalse(result['real_execution'])
            self.assertIsNone(result['execution'])
            self.assertIsNone(result['execution_metadata'])
            self.assertEqual(result['reason'], 'offline_missing_environment')
            start.assert_not_called()

    def test_unready_observer_prevents_endpoint_creation(self):
        c = config()
        with tempfile.TemporaryDirectory() as temp:
            c['source_repo'] = str(Path(temp))
            request = {'config': c, 'output_dir': temp, 'leases': [], 'source': {}}
            observer = Mock()
            observer.start.return_value = {'status': 'unknown'}
            observer.stop.return_value = {'health': {'status': 'unknown'}}
            with patch('r2f.runtime.namespace_pair.validate_host_leases', return_value={'net_namespace': 'isolated'}), \
                 patch('r2f.runtime.namespace_pair.same_source', return_value=True), \
                 patch('r2f.runtime.namespace_pair.probe_rtps_ports', return_value={'ok': True}), \
                 patch('r2f.runtime.namespace_pair.write_profiles', return_value=({}, {})), \
                 patch('r2f.instrumentation.sedp.build_capture_spec', return_value={}), \
                 patch('r2f.instrumentation.sedp.PassiveSedpObserver', return_value=observer), \
                 patch('r2f.runtime.namespace_pair.OwnerCheckedPool') as pool, \
                 patch('builtins.print') as output:
                result = namespace_body(request, Mock())
            self.assertEqual(result['runtime_status'], 'blocked')
            self.assertIn('observer_not_ready', result['reason'])
            pool.assert_not_called()
            events = [json.loads(call.args[0]) for call in output.call_args_list]
            self.assertFalse(any(e['event'] in ('mutation_begin', 'control_begin', 'endpoint_created') for e in events))
            self.assertEqual([s['step_id'] for s in result['precondition_receipts']], ['namespace_ready', 'observer_ready'])
            self.assertFalse(result['precondition_receipts'][1]['satisfied'])
            self.assertNotIn('execution', result)

    def test_graph_export_and_control_preserve_independent_original_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            writer, reader = 'a'*32+'0'*16, 'b'*32+'0'*16
            publishers = [{'endpoint_gid': writer, 'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}]
            subscriptions = [{'endpoint_gid': reader, 'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}]
            graph_event = {'time_ns': 30, 'publishers': publishers, 'subscriptions': subscriptions}
            proof = {role: {'graph': deepcopy(graph_event)} for role in ('writer', 'reader')}
            endpoints = {'writer': {'endpoint_gid': writer}, 'reader': {'endpoint_gid': reader}}
            graph = export_graph(proof, endpoints, '/offline', root / 'graph.json')
            original = (root / 'graph.json').read_bytes()
            capture = {'raw': {'path': str(root / 'wire.pcap'), 'sha256': 'independent-wire'}}
            receipt = controlled_graph(graph, capture, root / 'controlled.json')
            self.assertEqual(receipt['field_path'], '/endpoints/0/qos/reliability')
            self.assertFalse(receipt['candidate_eligible'])
            self.assertEqual(receipt['responsibility_layer'], 'harness')
            derived = json.loads((root / 'controlled.json').read_text())
            expected = json.loads(original)
            expected['endpoints'][0]['qos']['reliability'] = 'best_effort'
            self.assertEqual(derived, expected)
            self.assertEqual((root / 'graph.json').read_bytes(), original)
            self.assertEqual(hashlib.sha256(original).hexdigest(), graph['raw']['sha256'])
            bad = deepcopy(proof)
            bad['reader']['graph']['publishers'] = []
            with self.assertRaisesRegex(RuntimeError, 'not_complete'):
                export_graph(bad, endpoints, '/offline', root / 'bad.json')

    def test_final_raw_bindings_do_not_create_execution_record(self):
        events = []
        for role, letter in (('writer', 'a'), ('reader', 'b')):
            common = {'role': role, 'endpoint_generation': letter, 'pid': 1 if role == 'writer' else 2,
                      'process_start_ticks': 4, 'endpoint_gid': letter*32+'0'*16}
            for kind in ('endpoint_create_begin', 'endpoint_created'):
                events.append({'event': kind, 'event_index': len(events), **common})
        observation = {'event': 'projection_observation', 'capture_receipt': {}, 'graph_snapshot': {},
                       'observation_window': {}, 'field_policy': {}}
        result = {'events': [*events, observation], 'raw_path': '/offline.jsonl', 'raw_sha256': 'offline-unit-hash'}
        finalize_projection(result)
        bindings = result['discovery_projection']['endpoint_bindings']
        self.assertEqual(bindings[0]['begin_raw_ref']['event_index'], 0)
        self.assertEqual(bindings[1]['return_raw_ref']['event_index'], 3)
        self.assertEqual(bindings[0]['endpoint_guid'], 'a'*32)
        self.assertNotIn('execution', result)
        self.assertNotIn('real_execution', result)


if __name__ == '__main__':
    unittest.main()
