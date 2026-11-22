"""Offline multi-endpoint S1 projection contracts. No ROS, workers or campaign."""
from copy import deepcopy
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from r2f.campaign.projection_adapter import projection_facts
from r2f.instrumentation.sedp import parse_sedp_capture, reconcile_discovery_projection
from r2f.modules.s1.oracles import evaluate_primary
from r2f.runtime.projection_pair import (DEFAULT_QOS, FUZZ_TARGET, TARGET,
    normalize_projection_config, projection_roles, validate_projection_roles)
from r2f.runtime.ros2_pair import normalize_config
from test_runtime_projection import GUIDS, OfflineProjection, ProjectionClock, sedp_frame

MULTI_ROLES = ['writer', 'reader', 'writer2', 'reader2', 'writer3']
ROLE_GUIDS = {
    'writer': '010f' + '11' * 10 + '00000603',
    'reader': '010f' + '22' * 10 + '00000604',
    'writer2': '010f' + '33' * 10 + '00000603',
    'reader2': '010f' + '44' * 10 + '00000604',
    'writer3': '010f' + '55' * 10 + '00000603',
    'reader3': '010f' + '66' * 10 + '00000604',
    'writer4': '010f' + '77' * 10 + '00000603',
    'reader4': '010f' + '88' * 10 + '00000604',
}


def role_kind(role):
    return 'writer' if role.startswith('writer') else 'reader'


def base_config(arm='baseline', **extra):
    return normalize_projection_config(normalize_config({
        'target_id': TARGET, 'mutation_kind': 'endpoint_qos_variation',
        'carrier': 'ros2_endpoint', 'arm': arm, 'run_id': 'projection-multi',
        'qos': dict(DEFAULT_QOS), 'topic': '/offline',
        'projection_timeout_sec': .2, **extra}))


class MultiRolePool:
    """Production controller calls, fake worker graph responses."""

    def __init__(self, owner, config):
        self.owner, self.config = owner, config
        self.workers, self.metadata, self.pending = {}, [], []
        self.serial, self.timeouts, self.closed = 0, [], False

    def start(self, role):
        self.owner.emit('endpoint_create_begin', role=role, pid=self.owner.pid(role),
                        process_start_ticks=800, endpoint_generation=role + '-generation',
                        topic=self.config['topic'], qos=self.config[role + '_qos'],
                        message_type=self.config['message_type'])
        self.owner.emit('endpoint_created', role=role, pid=self.owner.pid(role),
                        process_start_ticks=800, endpoint_generation=role + '-generation',
                        endpoint_gid=ROLE_GUIDS[role], rmw_identifier=self.config['rmw'],
                        topic=self.config['topic'], message_type=self.config['message_type'],
                        qos=self.config[role + '_qos'])
        worker = Mock(returncode=None)
        worker.poll.return_value = None
        self.workers[role] = worker
        self.metadata.append({'role': role, 'pid': self.owner.pid(role), 'returncode': 0})

    def command(self, role, command, **fields):
        self.serial += 1
        self.pending.append((role, self.serial))
        return self.serial

    def wait(self, predicate, timeout, label):
        if label == 'acknowledged_graph_query':
            self.timeouts.append(timeout)
        if not predicate():
            self.pump(0)
        if not predicate():
            raise TimeoutError(label)

    def pump(self, timeout=0):
        self.owner.clock.ns += int(timeout * 1e9)
        pending, self.pending = self.pending, []
        for role, command_id in pending:
            common = {'role': role, 'command_id': command_id,
                      'pid': self.owner.pid(role), 'process_start_ticks': 800,
                      'endpoint_generation': role + '-generation'}
            self.owner.emit('match_snapshot', endpoint_gid=ROLE_GUIDS[role],
                            sampled_monotonic_ns=self.owner.clock.monotonic_ns(),
                            matched_count=self.owner.expected_match_count,
                            count_api=('get_subscription_count'
                                       if role_kind(role) == 'writer' else 'get_publisher_count'),
                            **common)
            publishers, subscriptions = self.owner.graph_rows()
            self.owner.emit('graph_snapshot', publishers=publishers,
                            subscriptions=subscriptions,
                            time_ns=self.owner.clock.time_ns(), **common)
            self.owner.emit('command_done', **common)

    def close(self):
        self.closed = True


class MultiRoleProjection:
    """Reuse production namespace_body with a fake pool and fake wire observer."""

    def __init__(self, root, *, roles=None, mode='complete', role_qos=None,
                 arm='baseline', expected_match_count=None):
        self.root = Path(root)
        self.mode = mode
        self.clock = ProjectionClock()
        self.c = base_config(arm, projection_roles=roles or list(MULTI_ROLES),
                             projection_role_qos=role_qos or {})
        self.events, self.records, self.workers = [], [], {}
        self.pool = self.query_timeouts = None
        self.queries = 0
        self.stopped = False
        self.path = self.root / 'wire.pcap'
        from test_instrumentation_sedp_projection import pcap
        self.path.write_bytes(pcap([]))
        self.expected_match_count = (expected_match_count
                                     if expected_match_count is not None
                                     else len(roles or MULTI_ROLES) - 1)
        self.count = 0

    def pid(self, role):
        return 100 + list(self.c['projection_roles']).index(role)

    def emit(self, event, **fields):
        token = {'event': event, 'role': 'controller', 'pid': 4242, 'pgid': 4242,
                 'run_id': self.c['run_id'], 'domain_id': self.c['domain_id'],
                 'time_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns(),
                 **fields}
        token['namespace_event_index'] = len(self.events)
        self.events.append(token)
        return token

    def graph_rows(self):
        writers = [r for r in self.c['projection_roles'] if role_kind(r) == 'writer']
        readers = [r for r in self.c['projection_roles'] if role_kind(r) == 'reader']
        if self.mode == 'missing' and self.count >= 1:
            writers = writers[:-1]
        if self.mode == 'extra':
            writers = [*writers, 'writer4']
        rows = {}
        for role in self.c['projection_roles']:
            qos = dict(self.c[role + '_qos'])
            if self.mode == 'qos' and role == 'writer2':
                qos['reliability'] = ('best_effort' if qos['reliability'] == 'reliable' else 'reliable')
            rows[role] = qos
        gid = lambda role: (ROLE_GUIDS[role] if role in ROLE_GUIDS
                            else '010f' + '99' * 10 + '00000603')
        publishers = []
        for role in writers:
            if role == 'writer4' and self.mode == 'extra':
                gid_ = '010f' + '99' * 10 + '00000603'
            else:
                gid_ = ROLE_GUIDS[role]
            publishers.append({'endpoint_gid': gid_, 'node_name': role,
                               'topic_type': self.c['message_type'],
                               'qos': rows.get(role, dict(DEFAULT_QOS))})
        subscriptions = []
        for role in readers:
            topic_type = self.c['message_type']
            if self.mode == 'topic' and role == 'reader2':
                topic_type = 'std_msgs/msg/Int32'
            subscriptions.append({'endpoint_gid': ROLE_GUIDS[role], 'node_name': role,
                                  'topic_type': topic_type,
                                  'qos': rows.get(role, dict(DEFAULT_QOS))})
        return publishers, subscriptions

    def snapshot_activity(self, *, since_ns, participant_prefixes):
        self.count += 1
        from test_instrumentation_sedp_projection import pcap
        for role in self.c['projection_roles']:
            qos = dict(self.c[role + '_qos'])
            options = {}
            if self.mode == 'topic' and role == 'reader2':
                options['topic'] = 'different'
            # SEDP QoS parameters are part of the wire fact, so the announcement
            # carries the role's actual submitted profile. A graph-side
            # divergence (mode='qos') therefore stays a real wire/graph mismatch.
            if qos['reliability'] == 'best_effort':
                options['reliability'] = 1
            self.records.append((self.clock.time_ns(),
                                 sedp_frame(ROLE_GUIDS[role], role_kind(role),
                                            sequence=self.count, **options)))
        self.path.write_bytes(pcap(self.records))
        data, st = self.path.read_bytes(), self.path.stat()
        prefix = {'path': str(self.path), 'sha256': hashlib.sha256(data).hexdigest(),
                  'size': len(data), 'final': False,
                  'file_identity': {'device': st.st_dev, 'inode': st.st_ino}}
        snapshot = {'status': 'observed',
                    'observation_completeness': 'complete_for_known_target_prefixes',
                    'since_ns': since_ns, 'until_ns': self.clock.time_ns(),
                    'clock': 'unix_ns_realtime',
                    'monotonic_ns': self.clock.monotonic_ns(),
                    'participant_prefixes': participant_prefixes,
                    'all_participants_observed': True,
                    'window_buffer_retained': True, 'health': {'status': 'ok'},
                    'filtered_rtps': {'metadata_complete': True, 'metadata_overflow': 0,
                                      'target_related': [],
                                      'completeness': 'complete_for_known_target_prefixes'},
                    'raw_prefix': prefix, 'ready': self.ready,
                    'events': __import__('r2f.instrumentation.sedp',
                                         fromlist=['parse_sedp_capture_prefix'])
                    .parse_sedp_capture_prefix(prefix)['activity']}
        return snapshot

    def start(self, role=None):
        if role is None:
            self.ready = {'status': 'ready', 'unix_ns': self.clock.time_ns()}
            return self.ready
        raise AssertionError('pool.start must be called by namespace_body only')

    def stop(self):
        self.stopped = True
        raw = {'path': str(self.path),
               'sha256': hashlib.sha256(self.path.read_bytes()).hexdigest(),
               'size': self.path.stat().st_size}
        request = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        empty = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        sampled = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        ended = {'unix_ns': self.clock.time_ns(), 'monotonic_ns': self.clock.monotonic_ns()}
        receipt = {'ready': self.ready, 'unix_ns': self.clock.time_ns(),
                   'monotonic_ns': self.clock.monotonic_ns(), 'raw': raw,
                   'complete_through_unix_ns': request['unix_ns'],
                   'drain': {'status': 'drained', 'reason': 'socket_empty',
                             'nonblocking': True, 'request': request,
                             'cutoff_unix_ns': request['unix_ns'],
                             'deadline_monotonic_ns': request['monotonic_ns'] + 10**8,
                             'empty_confirmed': empty,
                             'empty_statistics': {'packets': len(self.records), 'drops': 0,
                                                  'sampled': sampled},
                             'unread_packets_at_empty_boundary': 0, 'ended': ended},
                   'thread_stopped': True, 'socket_closed': True,
                   'health': {'status': 'ok', 'kernel_drop_status': 'known_at_stop'},
                   'counters': {'kernel_drops': 0}, 'limit_reason': None, 'error': None}
        data = json.dumps(receipt, sort_keys=True).encode()
        receipt_path = self.root / 'capture.json'
        receipt_path.write_bytes(data)
        return {**receipt, 'receipt_raw': {'path': str(receipt_path),
                                            'sha256': hashlib.sha256(data).hexdigest()}}

    def run(self):
        from r2f.runtime.projection_pair import namespace_body

        def pool_factory(config, env, out, emit, **kwargs):
            self.emit = emit
            self.pool = MultiRolePool(self, config)
            return self.pool

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
            output = stack.enter_context(patch('builtins.print'))
            result = namespace_body({'config': self.c, 'output_dir': str(self.root),
                                     'leases': [], 'source': {}}, Mock())
        self.events = [json.loads(call.args[0]) for call in output.call_args_list]
        for i, event in enumerate(self.events):
            event['event_index'] = i
        raw = b''.join(json.dumps(e, sort_keys=True).encode() + b'\n' for e in self.events)
        path = self.root / 'raw.jsonl'
        path.write_bytes(raw)
        result.update(events=self.events, raw_path=str(path),
                      raw_sha256=hashlib.sha256(raw).hexdigest())
        from r2f.runtime.projection_pair import finalize_projection
        finalize_projection(result)
        return result


def completed_fixture(root, **kwargs):
    fixture = MultiRoleProjection(root, **kwargs)
    runtime = fixture.run()
    runtime.update(runtime_status='completed', real_execution=True,
                   environment={'ok': True},
                   carrier_implementation='isolated_netns_passive_sedp_public_qos',
                   execution_metadata={'workers': [
                       {'role': role, 'pid': fixture.pid(role), 'returncode': 0}
                       for role in fixture.c['projection_roles']]})
    runtime['events'] = fixture.events
    fixture.events.append({'event': 'namespace_controller_exited',
                           'event_index': len(fixture.events),
                           'returncode': 0, 'role': 'controller'})
    for index, event in enumerate(fixture.events):
        event['event_index'] = index
    raw = b''.join(json.dumps(e, sort_keys=True).encode() + b'\n' for e in fixture.events)
    Path(runtime['raw_path']).write_bytes(raw)
    runtime['raw_sha256'] = hashlib.sha256(raw).hexdigest()
    for binding in runtime['discovery_projection']['endpoint_bindings']:
        for key in ('begin_raw_ref', 'return_raw_ref'):
            binding[key]['sha256'] = runtime['raw_sha256']
    return fixture, runtime


class ProjectionRoleValidation(unittest.TestCase):
    def test_absent_key_keeps_the_legacy_pair(self):
        config = base_config()
        self.assertNotIn('projection_roles', config)
        self.assertEqual(projection_roles(config), ('writer', 'reader'))
        self.assertEqual(config['writer_qos'], config['qos'])
        self.assertEqual(config['reader_qos'], config['qos'])

    def test_invalid_role_sets_fail_with_the_field_name(self):
        for roles in (['writer'], ['writer', 'writer'], ['writer', 'reader', 'probe'],
                      ['writer'] * 9, 'writer,reader', ['writer', 'reader', 'reader5']):
            with self.subTest(roles=roles), self.assertRaisesRegex(ValueError,
                                                                   'projection_roles'):
                validate_projection_roles(roles)
        with self.assertRaisesRegex(ValueError, 'projection_roles'):
            base_config(projection_roles=['writer', 'reader', 'reader5'])

    def test_eight_unique_endpoints_are_admitted(self):
        roles = ['writer', 'writer2', 'writer3', 'writer4',
                 'reader', 'reader2', 'reader3', 'reader4']
        self.assertEqual(validate_projection_roles(roles), tuple(roles))

    def test_unknown_role_qos_key_names_the_field(self):
        with self.assertRaisesRegex(ValueError, 'projection_role_qos'):
            base_config(projection_roles=['writer', 'reader'],
                        projection_role_qos={'writer5': {'depth': 2}})
        with self.assertRaisesRegex(ValueError, 'projection_role_qos'):
            base_config(projection_roles=['writer', 'reader'],
                        projection_role_qos=['writer'])


class ProjectionMultiEndpoint(unittest.TestCase):
    def test_three_writers_two_readers_have_independent_chains_and_full_inventory(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture, runtime = completed_fixture(Path(temp))
            self.assertEqual(runtime['runtime_status'], 'completed', runtime['reason'])
            requests = [e for e in runtime['events'] if e['event'] == 'graph_snapshot_requested']
            self.assertEqual([e['role'] for e in requests], MULTI_ROLES)
            command_ids = [e['command_id'] for e in requests]
            self.assertEqual(len(set(command_ids)), len(MULTI_ROLES))
            events = runtime['events']
            for request in requests:
                role = request['role']
                chain = [e for e in events if e.get('command_id') == request['command_id']
                         and e.get('role') == role]
                self.assertEqual([e['event'] for e in chain],
                                 ['graph_snapshot_requested', 'match_snapshot',
                                  'graph_snapshot', 'command_done'])
                self.assertEqual([e['monotonic_ns'] for e in chain],
                                 sorted(e['monotonic_ns'] for e in chain))
                self.assertEqual(chain[0]['role'], role)
                for event in chain[1:]:
                    self.assertEqual(event['pid'], fixture.pid(role))
                    self.assertEqual(event.get('endpoint_generation', event.get('generation')), role + '-generation')
            facts = projection_facts(events, runtime, fixture.c)
            self.assertNotIn('evidence_error', facts)
            inventory = facts['projection_endpoint_inventory']
            self.assertEqual(set(inventory), set(MULTI_ROLES))
            for role, row in inventory.items():
                self.assertEqual(row['gid'], ROLE_GUIDS[role][:32])
                self.assertTrue(row['observed_in_graph'], role)
                self.assertTrue(row['wire_seen'], role)
                self.assertTrue(row['qos_equal'], role)
                self.assertEqual(row['type'], 'std_msgs/msg/String', role)
            result = evaluate_primary(facts, TARGET)
            self.assertEqual(result['verdict'], 'pass', result)
            self.assertFalse(facts['projection_graph_reasons'])

    def test_missing_endpoint_is_a_named_projection_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = MultiRoleProjection(Path(temp), mode='missing')
            runtime = fixture.run()
            self.assertEqual(runtime['runtime_status'], 'completed', runtime['reason'])
            facts = projection_facts(runtime['events'], runtime, fixture.c)
            missing = ROLE_GUIDS['writer3'][:32]
            self.assertIn('projection_graph_missing_endpoint:' + missing,
                          facts['projection_graph_reasons'])
            self.assertFalse(facts['projection_endpoint_inventory']['writer3']
                             ['observed_in_graph'])
            result = evaluate_primary(facts, TARGET)
            self.assertEqual(result['verdict'], 'fail', result)
            self.assertIn('builtin_graph_field_mismatch', result['forbidden_residuals'])

    def test_extra_unknown_endpoint_is_a_named_projection_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = MultiRoleProjection(Path(temp), mode='extra')
            runtime = fixture.run()
            self.assertEqual(runtime['runtime_status'], 'completed', runtime['reason'])
            facts = projection_facts(runtime['events'], runtime, fixture.c)
            extra = '010f' + '99' * 10 + '00000603'
            self.assertIn('projection_graph_extra_endpoint:' + extra,
                          facts['projection_graph_reasons'])
            result = evaluate_primary(facts, TARGET)
            self.assertEqual(result['verdict'], 'fail', result)
            self.assertIn('builtin_graph_field_mismatch', result['forbidden_residuals'])

    def test_per_role_topic_and_qos_mismatch_fail_independently(self):
        for mode, role in (('topic', 'reader2'), ('qos', 'writer2')):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                fixture = MultiRoleProjection(Path(temp), mode=mode)
                runtime = fixture.run()
                self.assertEqual(runtime['runtime_status'], 'completed', runtime['reason'])
                facts = projection_facts(runtime['events'], runtime, fixture.c)
                self.assertNotIn('evidence_error', facts)
                if mode == 'topic':
                    self.assertNotEqual(facts['projection_endpoint_inventory'][role]['type'],
                                        'std_msgs/msg/String')
                else:
                    self.assertFalse(facts['projection_endpoint_inventory'][role]['qos_equal'])
                result = evaluate_primary(facts, TARGET)
                self.assertEqual(result['verdict'], 'fail', result)
                self.assertIn('builtin_graph_field_mismatch', result['forbidden_residuals'])

    def test_projection_role_qos_is_applied_and_checked_per_role(self):
        from r2f.runtime.worker import create_public_qos
        override = {'reliability': 'best_effort', 'depth': 3}
        with tempfile.TemporaryDirectory() as temp:
            fixture = MultiRoleProjection(Path(temp), arm='mutated',
                                          role_qos={'writer2': override})
            self.assertEqual(fixture.c['writer2_qos']['reliability'], 'best_effort')
            self.assertEqual(fixture.c['writer2_qos']['depth'], 3)
            self.assertEqual(fixture.c['writer3_qos'], fixture.c['qos'])
            runtime = fixture.run()
            self.assertEqual(runtime['runtime_status'], 'completed', runtime['reason'])
            facts = projection_facts(runtime['events'], runtime, fixture.c)
            self.assertTrue(facts['projection_endpoint_inventory']['writer2']['qos_equal'])
            self.assertEqual(evaluate_primary(facts, TARGET)['verdict'], 'pass')
            # The environment has no rclpy in this offline check; the profile
            # contract itself is covered by the existing worker unit test.
            self.assertEqual(fixture.c['writer2_qos']['depth'], 3)
            self.assertEqual(fixture.c['writer3_qos']['depth'], 64)


def legacy_norm(value, root):
    """Remove only run-local file identity; behavior fields stay byte-exact."""
    if isinstance(value, dict):
        return {key: ({'device': '<DEV>', 'inode': '<INODE>'}
                      if key == 'file_identity' and isinstance(item, dict)
                      else legacy_norm(item, root))
                for key, item in value.items()}
    if isinstance(value, list):
        return [legacy_norm(item, root) for item in value]
    if isinstance(value, str):
        return value.replace(str(root), '<OUT>')
    return value


class ProjectionGoldenLegacy(unittest.TestCase):
    """Default (no projection_roles) must stay behavior-identical to HEAD.

    Both hashes below were measured twice on a pristine HEAD export and on the
    working tree with the same fixed output directory. File device/inode values
    are the only stripped fields; they identify the tempfile, not behavior.
    """

    def test_default_pair_event_stream_hash_is_unchanged(self):
        root = Path('/tmp/r2f-projection-golden')
        import shutil
        if root.exists():
            for child in root.iterdir():
                shutil.rmtree(child) if child.is_dir() else child.unlink()
        else:
            root.mkdir()
        with patch('r2f.runtime.projection_pair.os.getpid', return_value=4242), \
             patch('r2f.runtime.projection_pair.os.getpgrp', return_value=4242):
            fixture = OfflineProjection(root)
            result = fixture.run()
        self.assertEqual(result['runtime_status'], 'completed', result['reason'])
        self.assertNotIn('projection_roles', fixture.c)
        self.assertNotIn('projection_role_qos', fixture.c)
        self.assertEqual(len(fixture.events), 31)
        names = json.dumps([e['event'] for e in fixture.events], separators=(',', ':')).encode()
        self.assertEqual(hashlib.sha256(names).hexdigest(),
                         'd546054706c64110ff79d7314d4986616604c2cac72d12896ce3e969fc80c56c')
        events = legacy_norm(fixture.events, root)
        raw = b''.join(json.dumps(e, sort_keys=True, separators=(',', ':')).encode() + b'\n'
                       for e in events)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),
                         'a6f08dc647c122ad05f4687991d04c1347f692a6c72a10a8bf1dbd4072e9a9eb')
        self.assertEqual([e['event'] for e in fixture.events],
                         ['namespace_host_admission_verified', 'state_step_before', 'namespace_ready',
                          'state_step_after', 'state_step_before', 'discovery_observer_ready',
                          'state_step_after', 'control_begin', 'window_open', 'endpoint_create_begin',
                          'endpoint_created', 'endpoint_create_begin', 'endpoint_created',
                          'neutral_projection_control', 'projection_readiness_requested',
                          'projection_readiness_snapshot', 'projection_readiness_attempt',
                          'projection_wire_ready', 'graph_snapshot_requested',
                          'graph_snapshot_requested', 'match_snapshot', 'graph_snapshot',
                          'command_done', 'match_snapshot', 'graph_snapshot', 'command_done',
                          'projection_graph_exported', 'window_close',
                          'discovery_observer_stopped', 'projection_prefix_chain_verified',
                          'projection_observation'])


if __name__ == '__main__':
    unittest.main()


class GraphSchemaExportCP1(unittest.TestCase):
    """Simulated worker snapshots exercise the public export artifact boundary."""

    def test_export_preserves_each_observer_time_without_claiming_builtin_layers(self):
        from r2f.runtime.projection_pair import export_graph
        endpoints = {role: {'endpoint_gid': ROLE_GUIDS[role]} for role in ('writer', 'reader')}
        event = {
            'time_ns': 100,
            'publishers': [{'endpoint_gid': ROLE_GUIDS['writer'],
                            'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}],
            'subscriptions': [{'endpoint_gid': ROLE_GUIDS['reader'],
                               'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}]}
        proof = {role: {'graph': deepcopy(event)} for role in endpoints}
        proof['reader']['graph']['time_ns'] = 145
        with tempfile.TemporaryDirectory() as temp:
            result = export_graph(proof, endpoints, '/offline', Path(temp)/'graph.json',
                                  include_schema_check=True)
            persisted = json.loads(Path(result['raw']['path']).read_text())
        self.assertEqual(persisted['schema_layers']['participant']['status'], 'unavailable_api')
        self.assertEqual(persisted['schema_layers']['topic']['status'], 'endpoint_derived_only')
        self.assertEqual(persisted['observer_snapshot_times_unix_ns'], {'writer': 100, 'reader': 145})
        self.assertIsNone(persisted['observer_snapshot_spread_ns'])
        self.assertEqual(persisted['observer_timing_status'], 'unknown')
        self.assertEqual(persisted['source'], 'ros_graph')
        self.assertEqual(persisted['captured_at_unix_ns'], 100)
        self.assertEqual(len(persisted['endpoints']), 2)


class GraphClockExportContract(unittest.TestCase):
    """Offline raw snapshots, including alias observers, use the default export."""

    def test_default_export_binds_only_unanimous_well_typed_realtime_clocks(self):
        from r2f.runtime.projection_pair import export_graph
        roles = ('writer', 'reader', 'writer2')
        endpoints = {role: {'endpoint_gid': ROLE_GUIDS[role]} for role in roles}
        event = {
            'time_ns': 100, 'timestamp_clock': 'unix_ns_realtime',
            'publishers': [{'endpoint_gid': ROLE_GUIDS[role],
                            'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}
                           for role in ('writer', 'writer2')],
            'subscriptions': [{'endpoint_gid': ROLE_GUIDS['reader'],
                               'topic_type': 'std_msgs/msg/String', 'qos': dict(DEFAULT_QOS)}]}
        proof = {role: {'graph': deepcopy(event)} for role in roles}
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'graph.json'
            result = export_graph(proof, endpoints, '/offline', path)
            persisted = path.read_bytes()
            self.assertEqual(json.loads(persisted)['timestamp_clock'], 'unix_ns_realtime')
            self.assertEqual(result['raw']['sha256'], hashlib.sha256(persisted).hexdigest())
            self.assertNotIn('schema_layers', result)
            legacy = deepcopy(proof)
            for row in legacy.values():
                row['graph'].pop('timestamp_clock')
            legacy_path = Path(temp)/'legacy.json'
            old = export_graph(legacy, endpoints, '/offline', legacy_path)
            legacy_bytes = legacy_path.read_bytes()
            self.assertEqual(set(old), {'source', 'captured_at_unix_ns', 'endpoints', 'raw'})
            for index, bad in enumerate((None, True, 1, [], {}, 'monotonic_ns', 'unix_ns_realtime ')):
                with self.subTest(bad=bad):
                    conflicting = deepcopy(proof)
                    conflicting['writer2']['graph']['timestamp_clock'] = bad
                    bad_path = Path(temp)/f'bad-{index}.json'
                    result = export_graph(conflicting, endpoints, '/offline', bad_path)
                    self.assertNotIn('timestamp_clock', result)
                    self.assertEqual(bad_path.read_bytes(), legacy_bytes)
            partial = deepcopy(proof)
            partial['writer2']['graph'].pop('timestamp_clock')
            self.assertNotIn('timestamp_clock', export_graph(partial, endpoints, '/offline', Path(temp)/'partial.json'))
            for index, bad_time in enumerate((True, 100.0, '100', None)):
                with self.subTest(bad_time=bad_time):
                    invalid = deepcopy(proof)
                    invalid['writer2']['graph']['time_ns'] = bad_time
                    self.assertNotIn('timestamp_clock', export_graph(invalid, endpoints, '/offline', Path(temp)/f'time-{index}.json'))
