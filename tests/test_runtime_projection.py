"""Fourth runtime contracts, offline only; never manufacture execution metadata."""
from copy import deepcopy
import hashlib
import json
import struct
from contextlib import ExitStack
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from r2f.runtime.ros2_pair import normalize_config, run_pair
from r2f.runtime.projection_pair import (TARGET, DEFAULT_QOS, INFINITE_NS,
    normalize_projection_config, validate_qos, export_graph, controlled_graph,
    finalize_projection, namespace_body, verify_projection_prefix_chain)
from r2f.runtime.worker import create_public_qos


def config(arm='baseline', **extra):
    selected = dict(DEFAULT_QOS, deadline_ns=1250000000) if arm == 'mutated' else dict(DEFAULT_QOS)
    return normalize_projection_config(normalize_config({
        'target_id': TARGET, 'mutation_kind': 'endpoint_qos_variation', 'carrier': 'ros2_endpoint',
        'arm': arm, 'run_id': 'projection-offline', 'qos': selected, **extra}))


# These offline pcap bytes go through the production prefix and final decoders.
from test_instrumentation_sedp_projection import param, cdr, pcap
from r2f.instrumentation.sedp import parse_sedp_capture, parse_sedp_capture_prefix, reconcile_discovery_projection
from r2f.modules.s1.oracles import evaluate_primary

GUIDS = {'writer': '010f' + '11'*10 + '00000603', 'reader': '010f' + '22'*10 + '00000604'}


def sedp_frame(guid, role, *, source=None, dispose=False, inline_only=False, complete=True,
               reliability=2, kind=0x15, topic=True, sequence=1, builtin=None):
    data = param(0x5a, bytes.fromhex(guid))
    if topic:
        data += param(5, cdr('rt/offline'))
    data += param(7, cdr('std_msgs::msg::dds_::String_'))
    data += param(0x1a, struct.pack('<iiI', reliability, 0, 0))
    if complete:
        data += struct.pack('<HH', 1, 0)
    inline = (param(0x71, b'\0\0\0\3') + param(0x70, bytes.fromhex(guid))
              + struct.pack('<HH', 1, 0)) if dispose else b''
    body = (b'\0\0' + struct.pack('<H', 16) + bytes(4)
            + bytes.fromhex(builtin or ('000003c2' if role == 'writer' else '000004c2'))
            + struct.pack('<iI', 0, sequence) + inline
            + (b'' if inline_only else b'\0\3\0\0' + data))
    flags = 1 | (2 if dispose else 0) | (0 if inline_only else 4)
    rtps = (b'RTPS\x02\x03\x01\x0f' + bytes.fromhex(source or guid[:24])
            + bytes([kind, flags]) + struct.pack('<H', len(body)) + body)
    udp = struct.pack('!HHHH', 53410, 53412, len(rtps) + 8, 0) + rtps
    ip = (bytes.fromhex('4500') + struct.pack('!H', len(udp) + 20) + bytes(5)
          + b'\x11' + bytes(2) + b'\x7f\0\0\1'*2)
    return bytes(12) + b'\x08\0' + ip + udp


class ProjectionClock:
    def __init__(self):
        self.ns = 10**9

    def monotonic_ns(self):
        self.ns += 1000
        return self.ns

    def time_ns(self):
        return 1700000000000000000 + self.monotonic_ns()


class OfflineProjection:
    """Production namespace controller/query/parser, without worker processes or ROS."""
    def __init__(self, root, *, mode='complete', arm='baseline', rmw='rmw_fastrtps_cpp'):
        self.root, self.mode = root, mode
        self.clock = ProjectionClock()
        self.c = config(arm, rmw=rmw, topic='/offline', projection_timeout_sec=.2)
        self.events, self.records, self.pending = [], [], []
        self.workers = {}
        self.metadata = [{'role': role, 'returncode': 0} for role in ('writer', 'reader')]
        self.count = self.queries = 0
        self.query_timeouts = []
        self.closed = self.stopped = False
        self.path = root / 'wire.pcap'
        self.path.write_bytes(pcap([]))

    def start(self, role=None):
        if role is None:
            self.ready = {'status': 'ready', 'unix_ns': self.clock.time_ns()}
            return self.ready
        common = {'role': role, 'pid': 71 if role == 'writer' else 72,
                  'process_start_ticks': 800, 'endpoint_generation': role + '-generation'}
        self.emit('endpoint_create_begin', **common)
        self.emit('endpoint_created', **common, endpoint_gid=GUIDS[role],
                  rmw_identifier=self.c['rmw'], qos=self.c[role + '_qos'])
        self.workers[role] = Mock(returncode=None)
        self.workers[role].poll.return_value = None

    def command(self, role, name):
        self.queries += 1
        self.pending.append((role, self.queries))
        return self.queries

    def pump(self, timeout=0):
        self.clock.ns += int(timeout * 10**9)
        pending, self.pending = self.pending, []
        for role, command_id in pending:
            self.emit('match_snapshot', role=role, command_id=command_id,
                      sampled_monotonic_ns=self.clock.monotonic_ns())
            publishers = [{'endpoint_gid': GUIDS['writer'],
                           'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}]
            subscriptions = [{'endpoint_gid': GUIDS['reader'],
                              'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}]
            if self.mode == 'graph_never_ready' or (self.mode == 'graph_retry' and command_id <= 2):
                subscriptions = []
            if self.mode == 'late_graph':
                self.clock.ns += 10**9
            self.emit('graph_snapshot', role=role, command_id=command_id,
                      publishers=publishers, subscriptions=subscriptions)
            if self.mode == 'late_ack':
                self.clock.ns += 10**9
            self.emit('command_done', role=role, command_id=command_id)

    def wait(self, predicate, timeout, label):
        if label == 'acknowledged_graph_query':
            self.query_timeouts.append(timeout)
        if not predicate():
            self.pump(0)
        if not predicate():
            raise TimeoutError(label)

    def close(self):
        self.closed = True

    def snapshot_activity(self, *, since_ns, participant_prefixes):
        self.count += 1
        if self.mode == 'late_snapshot':
            self.clock.ns += 10**9
        for role, guid in GUIDS.items():
            options = {}
            if role == 'reader':
                if self.mode in ('wrong_guid', 'retry') and (self.mode != 'retry' or self.count == 1):
                    guid = guid[:24] + '00000704'
                if self.mode == 'wrong_role':
                    role = 'writer'
                if self.mode == 'wrong_source':
                    options['source'] = 'ab'*12
                if self.mode == 'heartbeat':
                    options['kind'] = 7
                if self.mode == 'spdp':
                    options['builtin'] = '000100c2'
                if self.mode == 'missing_topic':
                    options['topic'] = False
                if self.mode == 'incomplete_list':
                    options['complete'] = False
                if self.mode == 'qos_mismatch':
                    options['reliability'] = 1
            stamp = self.clock.time_ns()
            if self.mode == 'predates_create':
                stamp = since_ns - 1
            self.records.append((stamp, sedp_frame(guid, role, sequence=self.count, **options)))
        if self.mode == 'dispose':
            self.records.append((self.clock.time_ns(), sedp_frame(GUIDS['reader'], 'reader',
                                dispose=True, inline_only=True, sequence=self.count + 100)))
        self.path.write_bytes(pcap(self.records))
        data, st = self.path.read_bytes(), self.path.stat()
        prefix = {'path': str(self.path), 'sha256': hashlib.sha256(data).hexdigest(),
                  'size': len(data), 'final': False, 'file_identity': {'device': st.st_dev, 'inode': st.st_ino}}
        snapshot = {'status': 'observed', 'observation_completeness': 'complete_for_known_target_prefixes',
                    'since_ns': since_ns, 'until_ns': self.clock.time_ns(), 'clock': 'unix_ns_realtime',
                    'monotonic_ns': self.clock.monotonic_ns(), 'participant_prefixes': participant_prefixes,
                    'all_participants_observed': True, 'window_buffer_retained': True, 'health': {'status': 'ok'},
                    'filtered_rtps': {'metadata_complete': True, 'metadata_overflow': 0, 'target_related': [],
                                      'completeness': 'complete_for_known_target_prefixes'},
                    'raw_prefix': prefix, 'ready': self.ready,
                    'events': parse_sedp_capture_prefix(prefix)['activity']}
        if self.mode == 'coverage':
            snapshot['window_buffer_retained'] = False
        if self.mode == 'metadata':
            snapshot['filtered_rtps'].pop('metadata_complete')
        if self.mode == 'snapshot_chronology':
            snapshot['monotonic_ns'] = 1
        if self.mode == 'missing_ready':
            snapshot.pop('ready')
        if self.mode == 'health':
            snapshot['health']['status'] = 'degraded'
        if self.mode == 'tampered_prefix':
            snapshot['raw_prefix']['sha256'] = '0'*64
        if self.mode == 'worker_exit':
            self.workers['reader'].poll.return_value = 9
            self.workers['reader'].returncode = 9
        if self.mode == 'owner_closed':
            self.pump = Mock(side_effect=RuntimeError('host_owner_channel_closed'))
        return snapshot

    def stop(self):
        self.stopped = True
        if self.mode == 'tampered_after_ready':
            data = bytearray(self.path.read_bytes())
            data[-8] ^= 1
            self.path.write_bytes(data)
        path = self.path
        if self.mode == 'unrelated_final':
            path = self.root / 'unrelated.pcap'
            path.write_bytes(self.path.read_bytes())
        raw = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'size': path.stat().st_size}
        request = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        empty = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        sampled = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        ended = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        receipt = {'ready': self.ready, 'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns(),
                   'raw': raw, 'complete_through_unix_ns': request['unix_ns'],
                   'drain': {'status': 'drained', 'reason': 'socket_empty', 'nonblocking': True,
                             'request': request, 'cutoff_unix_ns': request['unix_ns'],
                             'deadline_monotonic_ns': request['monotonic_ns'] + 10**8,
                             'empty_confirmed': empty, 'empty_statistics': {'packets': len(self.records), 'drops': 0, 'sampled': sampled},
                             'unread_packets_at_empty_boundary': 0, 'ended': ended},
                   'thread_stopped': True, 'socket_closed': True, 'health': {'status': 'ok', 'kernel_drop_status': 'known_at_stop'},
                   'counters': {'kernel_drops': 0}, 'limit_reason': None, 'error': None}
        data = json.dumps(receipt, sort_keys=True).encode()
        receipt_path = self.root / 'capture.json'
        receipt_path.write_bytes(data)
        return {**receipt, 'receipt_raw': {'path': str(receipt_path), 'sha256': hashlib.sha256(data).hexdigest()}}

    def run(self):
        def pool_factory(c, env, out, emit, **kwargs):
            self.emit = emit
            return self
        with ExitStack() as stack:
            for name, value in (
                ('r2f.runtime.projection_pair.time.monotonic_ns', self.clock.monotonic_ns),
                ('r2f.runtime.projection_pair.time.time_ns', self.clock.time_ns),
                ('r2f.runtime.namespace_pair.validate_host_leases', lambda *a: {'net_namespace': 'offline'}),
                ('r2f.runtime.namespace_pair.same_source', lambda *a: True),
                ('r2f.runtime.namespace_pair.execution_chain_identity', lambda: {}),
                ('r2f.runtime.namespace_pair.probe_rtps_ports', lambda *a: {'ok': True}),
                ('r2f.runtime.namespace_pair.write_profiles', lambda *a: ({}, {})),
                ('r2f.runtime.namespace_pair.OwnerCheckedPool', pool_factory),
                ('r2f.instrumentation.sedp.PassiveSedpObserver', lambda *a: self),
                ('r2f.instrumentation.sedp.build_capture_spec', lambda *a, **kw: {}),
                ('r2f.runtime.projection_pair.capture_libraries', lambda *a: {})):
                stack.enter_context(patch(name, value))
            if self.mode == 'late_parse':
                def late_parse(*args, **kwargs):
                    parsed = parse_sedp_capture_prefix(*args, **kwargs)
                    self.clock.ns += 10**9
                    return parsed
                stack.enter_context(patch('r2f.instrumentation.sedp.parse_sedp_capture_prefix', late_parse))
            output = stack.enter_context(patch('builtins.print'))
            result = namespace_body({'config': self.c, 'output_dir': str(self.root), 'leases': [], 'source': {}}, Mock())
        self.events = [json.loads(call.args[0]) for call in output.call_args_list]
        for i, event in enumerate(self.events):
            event['event_index'] = i
        raw = b''.join(json.dumps(e, sort_keys=True).encode() + b'\n' for e in self.events)
        path = self.root / 'raw.jsonl'
        path.write_bytes(raw)
        result.update(events=self.events, raw_path=str(path), raw_sha256=hashlib.sha256(raw).hexdigest())
        finalize_projection(result)
        return result


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


class ProjectionReadinessContracts(unittest.TestCase):
    def test_live_exact_endpoints_precede_fresh_both_role_graph_queries(self):
        for mode in ('complete', 'retry', 'graph_retry'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                f = OfflineProjection(Path(temp), mode=mode)
                result = f.run()
                self.assertEqual(result['runtime_status'], 'completed', result['reason'])
                self.assertTrue(f.closed and f.stopped)
                requests = [e for e in f.events if e['event'] == 'graph_snapshot_requested']
                readies = [e for e in f.events if e['event'] == 'projection_wire_ready']
                self.assertEqual(len(requests), 4 if mode == 'graph_retry' else 2)
                for event in requests:
                    ready = max((e for e in readies if e['event_index'] < event['event_index']), key=lambda e: e['event_index'])
                    self.assertGreater(event['requested_monotonic_ns'], ready['monotonic_ns'])
                    for role, selected in ready['selected_events'].items():
                        self.assertEqual(selected['endpoint_guid'], GUIDS[role])
                        self.assertTrue(selected['parameter_list']['complete'])
                if mode == 'retry':
                    attempts = [e for e in f.events if e['event'] == 'projection_readiness_attempt']
                    self.assertEqual([a['ready'] for a in attempts], [False, True])
                    self.assertIn('reader:exact_endpoint_missing', attempts[0]['reasons'])
                chain = next(e for e in f.events if e['event'] == 'projection_prefix_chain_verified')
                self.assertTrue(all(p['verified'] for p in chain['prefix_chain']))
                self.assertTrue(all(0 < t <= min(f.c['projection_timeout_sec'], f.c['startup_timeout_sec'])
                                    for t in f.query_timeouts))
                self.assertEqual(len({r['deadline_monotonic_ns'] for r in readies}), 1)
                if mode == 'graph_retry':
                    self.assertLess(f.query_timeouts[1], f.query_timeouts[0] - .04)
                projection = result['discovery_projection']
                wire = parse_sedp_capture(projection['capture_receipt'], field_policy=projection['field_policy'])
                reconciled = reconcile_discovery_projection(wire, projection['graph_snapshot'],
                    endpoint_bindings=projection['endpoint_bindings'], observation_window=projection['observation_window'])
                self.assertEqual(reconciled['comparison_status'], 'consistent', reconciled)
                self.assertEqual(len(reconciled['comparisons']), 18)
                self.assertNotIn('execution', result)

    def test_cyclone_uses_the_same_exact_endpoint_and_final_parser_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            f = OfflineProjection(Path(temp), rmw='rmw_cyclonedds_cpp')
            result = f.run()
            self.assertEqual(result['runtime_status'], 'completed', result['reason'])
            self.assertEqual(f.count, 1)
            self.assertIn('0110', result['discovery_projection']['field_policy']['vendor_provenance'])

    def test_graph_retries_cannot_restart_the_projection_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            f = OfflineProjection(Path(temp), mode='graph_never_ready')
            result = f.run()
            self.assertEqual(result['runtime_status'], 'failed')
            self.assertIn('deadline_exceeded', result['reason'])
            ready = [e for e in f.events if e['event'] == 'projection_wire_ready']
            self.assertGreater(len(ready), 1)
            self.assertEqual(len({e['deadline_monotonic_ns'] for e in ready}), 1)
            self.assertLess(f.clock.ns - ready[0]['monotonic_ns'], 300000000)
            self.assertTrue(f.closed and f.stopped)

    def test_missing_or_untrusted_wire_never_opens_graph_or_negative_fault(self):
        modes = ('wrong_guid', 'wrong_role', 'wrong_source', 'heartbeat', 'spdp', 'dispose', 'missing_topic',
                 'incomplete_list', 'predates_create', 'coverage', 'metadata', 'health', 'tampered_prefix',
                 'worker_exit', 'owner_closed', 'late_snapshot', 'late_parse', 'snapshot_chronology', 'missing_ready')
        for mode in modes:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                f = OfflineProjection(Path(temp), mode=mode, arm='negative')
                result = f.run()
                self.assertEqual(result['runtime_status'], 'failed', (mode, result))
                self.assertEqual(f.queries, 0)
                self.assertTrue(f.closed and f.stopped)
                self.assertTrue(any(e['event'] == 'projection_readiness_snapshot' for e in f.events))
                self.assertFalse(any(e['event'] in ('projection_observation', 'fault_control_receipt') for e in f.events))
                self.assertTrue(any(e['event'] == 'runtime_abort' for e in f.events))
                if mode == 'dispose':
                    attempts = [e for e in f.events if e['event'] == 'projection_readiness_attempt']
                    self.assertEqual(attempts[0]['selected_events']['reader']['lifecycle'], 'dispose_or_unregister')

    def test_successful_but_late_graph_or_ack_is_rejected_and_capture_is_closed(self):
        for mode in ('late_graph', 'late_ack'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                f = OfflineProjection(Path(temp), mode=mode)
                result = f.run()
                self.assertEqual(result['runtime_status'], 'failed')
                self.assertIn('deadline_exceeded', result['reason'])
                self.assertEqual(f.queries, 2)
                self.assertTrue(f.closed and f.stopped)
                self.assertFalse(any(e['event'] == 'projection_graph_exported' for e in f.events))

    def test_changed_or_unrelated_final_prefix_cannot_enable_harness_fault(self):
        for mode in ('tampered_after_ready', 'unrelated_final'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                f = OfflineProjection(Path(temp), mode=mode, arm='negative')
                result = f.run()
                self.assertEqual(result['runtime_status'], 'failed')
                self.assertTrue(f.closed and f.stopped)
                self.assertTrue(any(e['event'] == 'projection_graph_exported' for e in f.events))
                self.assertFalse(any(e['event'] in ('projection_observation', 'fault_control_receipt') for e in f.events))

    def test_observable_qos_mismatch_reaches_existing_reconcile_and_oracle(self):
        with tempfile.TemporaryDirectory() as temp:
            f = OfflineProjection(Path(temp), mode='qos_mismatch')
            result = f.run()
            self.assertEqual(result['runtime_status'], 'completed', result['reason'])
            self.assertEqual(f.count, 1)
            projection = result['discovery_projection']
            wire = parse_sedp_capture(projection['capture_receipt'], field_policy=projection['field_policy'])
            reconciled = reconcile_discovery_projection(wire, projection['graph_snapshot'],
                endpoint_bindings=projection['endpoint_bindings'], observation_window=projection['observation_window'])
            self.assertEqual(reconciled['comparison_status'], 'mismatch', reconciled)
            endpoint_facts = {role: {'id': GUIDS[role], 'generation': role + '-generation', 'created_ns':
                next(e['time_ns'] for e in f.events if e['event'] == 'endpoint_created' and e['role'] == role)}
                for role in ('writer', 'reader')}
            facts = {**endpoint_facts, 'projection': reconciled,
                     'window': {**projection['observation_window'], 'complete': True}}
            self.assertEqual(evaluate_primary(facts, TARGET)['verdict'], 'fail')

    def test_hash_bound_prefix_accepts_append_and_rejects_replacement_or_wrong_final_hash(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / 'wire.pcap'
            path.write_bytes(pcap([(100, sedp_frame(GUIDS['writer'], 'writer'))]))
            st = path.stat()
            prefix = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                      'size': st.st_size, 'final': False, 'file_identity': {'device': st.st_dev, 'inode': st.st_ino}}
            appended = pcap([(200, sedp_frame(GUIDS['reader'], 'reader'))])[24:]
            with path.open('ab') as stream:
                stream.write(appended)
            capture = {'raw': {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}}
            self.assertTrue(verify_projection_prefix_chain(capture, [prefix], {})[0]['verified'])
            bad = deepcopy(capture)
            bad['raw']['sha256'] = '0'*64
            with self.assertRaisesRegex(ValueError, 'sha256_mismatch'):
                verify_projection_prefix_chain(bad, [prefix], {})
            replacement = root / 'replacement.pcap'
            replacement.write_bytes(path.read_bytes())
            replacement.replace(path)
            with self.assertRaisesRegex(ValueError, 'inode_changed'):
                verify_projection_prefix_chain(capture, [prefix], {})


if __name__ == '__main__':
    unittest.main()
