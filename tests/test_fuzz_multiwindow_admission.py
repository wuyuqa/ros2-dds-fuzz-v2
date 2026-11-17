"""Offline T-D contracts: E02/E03 1..3 window generation and admission.

Nothing here starts ROS, a namespace, nft, a worker or a campaign.  The single
window golden sample pins the pre-T-D output shape; the window-chain fixtures
are synthetic raw tokens and prove only the adapter/runtime contract, never a
real SUT execution or a hit.
"""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign import recovery_adapter
from r2f.campaign.recovery_adapter import recovery_facts
from r2f.campaign.runner import _recovery_windows
from r2f.runtime.discovery_profiles import CYCLONE_XML
from r2f.runtime.ros2_pair import (
    recovery_fuzz_admission_error,
)

RECOVERY = 'discovery.interruption_recovery.fuzz_v1'


def core_window(index, duration=0.2, *, restore=0.02, offset=0.0, anchor=None,
                window_id=None, schema=True, first=None):
    if first is None:
        first = index == 0
    if anchor is None:
        anchor = 'fresh_match' if first else 'previous_restore_confirmed'
    window = {
        'window_id': window_id or 'post_match_window_%d' % index,
        'window_index': index,
        'trigger_anchor': anchor,
        'trigger_offset_sec': offset,
        'drop_duration_sec': duration,
        'restore_interval_sec': restore,
    }
    if schema:
        window = {'schema': 'r2f.runtime-window/v1', **window}
    return window


class SingleWindowGolden(unittest.TestCase):
    def test_default_single_window_is_byte_identical_to_head(self):
        core, runtime = _recovery_windows(0.2)
        self.assertEqual(core, {
            'schema': 'r2f.runtime-window/v1', 'window_id': 'w0', 'window_index': 0,
            'trigger_anchor': 'fresh_match', 'trigger_offset_sec': 0.0,
            'drop_duration_sec': 0.2, 'restore_interval_sec': 0.0})
        self.assertEqual(runtime, {
            'window_id': 'w0', 'window_index': 0, 'trigger_anchor': 'fresh_match',
            'trigger_offset_sec': 0.0, 'drop_duration_sec': 0.2,
            'restore_interval_sec': 0.0})
        self.assertEqual(json.dumps(core, sort_keys=True),
                         '{"drop_duration_sec": 0.2, "restore_interval_sec": 0.0, '
                         '"schema": "r2f.runtime-window/v1", "trigger_anchor": "fresh_match", '
                         '"trigger_offset_sec": 0.0, "window_id": "w0", "window_index": 0}')
        self.assertEqual(json.dumps(runtime, sort_keys=True),
                         '{"drop_duration_sec": 0.2, "restore_interval_sec": 0.0, '
                         '"trigger_anchor": "fresh_match", "trigger_offset_sec": 0.0, '
                         '"window_id": "w0", "window_index": 0}')

    def test_default_single_window_rejects_invalid_duration(self):
        for bad in (0, -1, 8.1, True, 'x'):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                _recovery_windows(bad)


class ExplicitWindowGeneration(unittest.TestCase):
    def test_two_and_three_window_plans_keep_order_and_per_window_values(self):
        plans = [
            [core_window(1, 0.1, restore=0.0, offset=0.01, first=True),
             core_window(2, 0.2, restore=0.02, offset=0.5)],
            [core_window(0, 0.1, restore=0.0, offset=0.0),
             core_window(1, 0.2, restore=0.2, offset=0.0),
             core_window(2, 0.3, restore=1.0, offset=0.0)],
        ]
        for plan in plans:
            with self.subTest(count=len(plan)):
                core, runtime = _recovery_windows(0.2, plan)
                self.assertEqual(len(core), len(plan))
                self.assertEqual(len(runtime), len(plan))
                for position, (c, r, source) in enumerate(zip(core, runtime, plan)):
                    self.assertEqual(c['schema'], 'r2f.runtime-window/v1')
                    self.assertNotIn('schema', r)
                    self.assertEqual({k: v for k, v in c.items() if k != 'schema'}, r)
                    # Both shapes agree and indices are normalized to 0..n-1.
                    self.assertEqual(c['window_index'], position)
                    self.assertEqual(r['window_index'], position)
                    self.assertEqual(c['window_id'], source['window_id'])
                    self.assertEqual(c['trigger_anchor'], source['trigger_anchor'])
                    self.assertEqual(c['trigger_offset_sec'], source['trigger_offset_sec'])
                    self.assertEqual(c['drop_duration_sec'], source['drop_duration_sec'])
                    self.assertEqual(c['restore_interval_sec'], source['restore_interval_sec'])

    def test_indices_may_start_at_one_and_are_normalized(self):
        core, runtime = _recovery_windows(0.2, [
            core_window(1, 0.1, schema=False, first=True),
             core_window(2, 0.1, schema=False)])
        self.assertEqual([w['window_index'] for w in core], [0, 1])
        self.assertEqual([w['window_index'] for w in runtime], [0, 1])

    def test_anchor_offsets_and_restore_intervals_survive(self):
        core, runtime = _recovery_windows(0.2, [
            core_window(1, 0.1, offset=0.5, first=True),
            core_window(2, 0.1, restore=1.0)])
        self.assertEqual(core[0]['trigger_offset_sec'], 0.5)
        self.assertEqual(runtime[0]['trigger_offset_sec'], 0.5)
        self.assertEqual(core[1]['restore_interval_sec'], 1.0)
        self.assertEqual(runtime[1]['restore_interval_sec'], 1.0)


class WindowConstraintRejection(unittest.TestCase):
    def assert_rejected(self, windows, field, **kwargs):
        with self.assertRaises(ValueError) as caught:
            _recovery_windows(0.2, windows, **kwargs)
        self.assertIn(field, str(caught.exception))

    def test_four_windows_rejected(self):
        plan = [core_window(i, 0.1, first=(i == 0)) for i in range(4)]
        self.assert_rejected(plan, 'window')

    def test_first_window_must_anchor_fresh_match(self):
        plan = [core_window(1, 0.1, anchor='previous_restore_confirmed', first=True)]
        self.assert_rejected(plan, 'trigger_anchor')

    def test_later_window_must_anchor_previous_restore(self):
        plan = [core_window(1, 0.1, first=True),
                core_window(2, 0.1, anchor='fresh_match')]
        self.assert_rejected(plan, 'trigger_anchor')

    def test_drop_duration_bounds(self):
        for value in (0, -0.1, 8.0001):
            with self.subTest(value=value):
                self.assert_rejected([core_window(1, value, first=True)], 'drop_duration_sec')

    def test_offset_and_restore_bounds(self):
        for key, value in (('trigger_offset_sec', -0.01), ('trigger_offset_sec', 8.01),
                           ('restore_interval_sec', -0.01), ('restore_interval_sec', 8.01)):
            with self.subTest(key=key, value=value):
                option = 'offset' if key == 'trigger_offset_sec' else 'restore'
                self.assert_rejected([core_window(1, 0.1, **{option: value}, first=True)], key)

    def test_non_consecutive_or_mixed_index_rejected(self):
        plan = [core_window(1, 0.1, first=True), core_window(3, 0.1)]
        self.assert_rejected(plan, 'window_index')
        plan = [core_window(0, 0.1), core_window(2, 0.1)]
        self.assert_rejected(plan, 'window_index')

    def test_duplicate_window_id_rejected(self):
        plan = [core_window(1, 0.1, first=True),
                core_window(2, 0.1, window_id='post_match_window_1')]
        self.assert_rejected(plan, 'window_id')

    def test_budget_over_max_seconds_rejected(self):
        plan = [core_window(1, 8.0, first=True), core_window(2, 8.0)]
        self.assert_rejected(plan, 'max_seconds', recovery_timeout_sec=2.0, max_seconds=10.0)
        # Exactly at the budget is allowed.
        core, runtime = _recovery_windows(0.2, [core_window(1, 0.5, restore=0.0, first=True)],
                                          recovery_timeout_sec=1.0, max_seconds=1.5)
        self.assertEqual(len(core), 1)


class AdmissionTests(unittest.TestCase):
    def config(self, surfaces=('E02',), windows=None):
        windows = [core_window(1, 0.1, schema=False, first=True)] if windows is None else windows
        return {'run_id': 'admission-multiwindow', 'domain_id': 173,
                'target_id': RECOVERY, 'carrier': 'wire_proxy',
                'mutation_kind': 'discovery_drop', 'arm': 'mutated',
                'fuzz_surfaces': list(surfaces),
                'environment_windows': windows}

    def test_e02_e03_one_to_three_windows_are_admitted(self):
        for surface in ('E02', 'E03'):
            for count in (1, 2, 3):
                windows = [core_window(i + 1, 0.1, schema=False, first=(i == 0))
                           for i in range(count)]
                with self.subTest(surface=surface, count=count):
                    self.assertIsNone(recovery_fuzz_admission_error(
                        self.config((surface,), windows)))

    def test_g01_is_not_admitted(self):
        self.assertEqual(recovery_fuzz_admission_error(self.config(('E02', 'G01'))),
                         'unsupported_capability:surface_not_admitted:G01')

    def test_more_than_three_windows_rejected(self):
        windows = [core_window(i + 1, 0.1, schema=False, first=(i == 0))
                   for i in range(4)]
        self.assertIn('window_count_not_admitted:4',
                      recovery_fuzz_admission_error(self.config(('E02',), windows)))

    def test_zero_windows_rejected(self):
        self.assertIn('window_count_not_admitted:0',
                      recovery_fuzz_admission_error(self.config(('E02',), [])))

    def test_admission_is_scoped_to_the_recovery_target(self):
        config = self.config(('E02',))
        config['target_id'] = 'discovery.interruption_recovery'
        self.assertIsNone(recovery_fuzz_admission_error(config))


# ---------------------------------------------------------------------------
# Synthetic window-chain raw.  This is fixture data, not execution evidence.
# ---------------------------------------------------------------------------

def _iso():
    return {'isolated': True, 'interfaces': ['lo'], 'net_namespace': 'net:[2]',
            'user_namespace': 'user:[2]', 'observed_net_namespace': 'net:[2]',
            'observed_user_namespace': 'user:[2]', 'host_net_namespace': 'net:[1]',
            'host_user_namespace': 'user:[1]', 'host_uid': 1000,
            'uid_map': [[0, 1000, 1]], 'euid': 0,
            'cap_eff': (1 << 12) | (1 << 13), 'host_cap_eff': 0}


class WindowChainFixture:
    def __init__(self, root, windows):
        self.root = Path(root)
        self.windows = copy.deepcopy(windows)
        self.events = []
        self.isolation = _iso()
        self.config = {'run_id': 'offline-chain', 'domain_id': 80, 'arm': 'mutated',
                       'rmw': 'rmw_cyclonedds_cpp', 'topic': '/offline/chain',
                       'message_type': 'std_msgs/msg/String',
                       'qos': {'reliability': 'reliable'},
                       'reader_delay_sec': 0.1, 'drop_duration_sec': 0.1,
                       'pre_match_count': 1, 'post_match_count': 3, 'fault_id': None,
                       'environment_windows': copy.deepcopy(windows)}
        # Bindings and capture must share the same reviewed unicast ports.
        base = 7400 + 250 * self.config['domain_id']
        self.ports = {base, *(base + 10 + 2 * p for p in range(32))}
        self.capture = {'ready': {'status': 'ready'}, 'data': b'offline-pcap', 'packets': {},
                        'stopped': {'raw': {'path': '/offline/capture.pcap'}}}
        self.tokens = {
            'writer': self.emit('endpoint_created', role='writer', pid=301,
                                endpoint_gid='1' * 24 + '00000603',
                                endpoint_generation=1, rmw_identifier=self.config['rmw'],
                                topic=self.config['topic'], qos=self.config['qos'],
                                message_type=self.config['message_type']),
            'reader': self.emit('endpoint_created', role='reader', pid=302,
                                endpoint_gid='2' * 24 + '00000603',
                                endpoint_generation=1, rmw_identifier=self.config['rmw'],
                                topic=self.config['topic'], qos=self.config['qos'],
                                message_type=self.config['message_type']),
        }
        self.emit('namespace_controller_exited', returncode=0)
        self.emit('namespace_host_admission_verified', isolation=self.isolation)
        self.emit('discovery_network_setup', receipt={
            'health_ok': True, 'drop_installed': False,
            'initial_nft': {'nftables': []}})
        for token in self.tokens.values():
            token['created_monotonic_ns'] = token['monotonic_ns']
        # The primary oracle consumes the identity-correlated match proof, so the
        # fixture must carry the same request -> snapshot -> ACK chain real raw has.
        reader_created = self.tokens['reader']['created_monotonic_ns']
        for number, role in enumerate(('writer', 'reader')):
            command_id = 10 + number
            token = self.tokens[role]
            request = self.emit('match_snapshot_requested', role=role,
                                command_id=command_id,
                                requested_monotonic_ns=reader_created + 1,
                                reader_created_monotonic_ns=reader_created)
            self.emit('match_snapshot', role=role, command_id=command_id,
                      matched_count=1, endpoint_gid=token['endpoint_gid'],
                      sampled_monotonic_ns=request['monotonic_ns'],
                      count_api=('get_subscription_count' if role == 'writer'
                                 else 'get_publisher_count'),
                      pid=token['pid'], endpoint_generation=token['endpoint_generation'],
                      rmw_identifier=token['rmw_identifier'])
            self.emit('command_done', role=role, command_id=command_id,
                      pid=token['pid'], endpoint_generation=token['endpoint_generation'],
                      rmw_identifier=token['rmw_identifier'])
        self.snapshot = self.emit('discovery_precondition_snapshot',
                                  bindings=self._bindings(),
                                  worker_pids={'writer': 301, 'reader': 302})

    def _bindings(self):
        records = []
        for number, role in enumerate(('writer', 'reader')):
            records.append({'port': 7400 + 250 * self.config['domain_id'] + 10 + 2 * number,
                            'roles': [role], 'selected_discovery_port': True})
        return {'only_pair_bound_udp_sockets': True,
                'worker_pids': {'writer': 301, 'reader': 302},
                'isolation': self.isolation,
                'worker_start_ticks': {'writer': '41', 'reader': '42'},
                'bindings': records}

    def emit(self, name, **fields):
        event = {'event': name, 'event_index': len(self.events),
                 'raw_event_ref': 'offline#%d' % len(self.events),
                 'monotonic_ns': (len(self.events) + 1) * 100_000_000,
                 'time_ns': 10**12 + (len(self.events) + 1) * 100_000_000, **fields}
        self.events.append(event)
        return event

    def command(self, argv, payload):
        return {'argv': argv, 'returncode': 0,
                'net_namespace': self.isolation['net_namespace'],
                'started_monotonic_ns': 1, 'ended_monotonic_ns': 2,
                'stdout': json.dumps(payload)}

    def activity(self, window, since, after_ns):
        index = len(self.capture['packets'])
        event = self.emit('discovery_activity_observation', window=window,
                          activity_observed=True, after_monotonic_ns=after_ns,
                          clock_guard_ns=50_000_000)
        witnesses = []
        for number, role in enumerate(('writer', 'reader')):
            packet = index + number
            body = {'source_guid_prefix': self.tokens[role]['endpoint_gid'][:24],
                    'source_port': 7400 + 250 * self.config['domain_id'] + 10 + 2 * number,
                    'destination_port': 7400 + 250 * self.config['domain_id'],
                    'kind': 'SEDP_HEARTBEAT', 'builtin_writer_id': '000003c2'}
            unix_ns = event['time_ns'] - 2 + number
            ref = {'packet_index': packet, 'frame_offset': 0, 'captured_length': 1}
            entry = {'unix_ns': unix_ns, 'received_monotonic_ns': event['monotonic_ns'] - 2 + number,
                     'clock': 'kernel_timestampns', 'raw_ref': ref}
            self.capture['packets'][packet] = (entry, [body])
            witnesses.append({**body, **entry})
        event['activity'] = {
            'status': 'observed', 'since_ns': since, 'until_ns': event['time_ns'] + 1,
            'clock': 'unix_ns_realtime',
            'participant_prefixes': sorted(t['endpoint_gid'][:24] for t in self.tokens.values()),
            'ready': self.capture['ready'], 'window_buffer_retained': True,
            'health': {'status': 'ok'},
            'raw_prefix': {'path': '/offline/capture.pcap', 'size': len(self.capture['data']),
                           'sha256': hashlib.sha256(self.capture['data']).hexdigest()},
            'events': witnesses}
        return event

    def build(self):
        previous = self.snapshot['raw_event_ref']
        total = 0
        for index, plan in enumerate(self.windows):
            self.emit('discovery_window_begin', window_index=index,
                      window_id=plan['window_id'], trigger_anchor=plan['trigger_anchor'],
                      anchor_ref=previous)
            table = 'r2f_discovery_%032d' % index
            begin_snapshot = {'packets': total, 'bytes': total * 10}
            total += 4 + index
            end_snapshot = {'packets': total, 'bytes': total * 10}
            packets_delta = end_snapshot['packets'] - begin_snapshot['packets']
            bytes_delta = end_snapshot['bytes'] - begin_snapshot['bytes']
            self.emit('discovery_drop_begin', window_index=index, receipt={
                'scope': 'ros2_pair', 'ros2_execution': True, 'configured': True,
                'applied': True, 'table': table, 'pair_bindings': self._bindings(),
                'precondition': {'evidence_ref': previous, 'match_established': True},
                'install_command': self.command(['nft', '-f', '-'], {}),
                'counter_snapshot': begin_snapshot})
            self.emit('window_open', window='discovery_interruption', window_index=index)
            self.emit('window_close', window='discovery_interruption', window_index=index)
            delta = {'table': table, 'window_index': None,
                     'packets_before': begin_snapshot['packets'],
                     'packets_after': end_snapshot['packets'],
                     'bytes_before': begin_snapshot['bytes'],
                     'bytes_after': end_snapshot['bytes'],
                     'packets_delta': packets_delta, 'bytes_delta': bytes_delta,
                     'counter_observed': True, 'hit': True,
                     'counter_semantics': 'per_window_delta_not_cumulative'}
            end_mono = self.events[-1]['monotonic_ns']
            self.emit('discovery_drop_end', window_index=index, receipt={
                'table': table, 'restored': True, 'table_absent': True,
                'counter_snapshot': end_snapshot,
                'delete_command': self.command(['nft', 'delete', 'table', 'inet', table], {}),
                'verify_command': self.command(['nft', '-j', 'list', 'ruleset'], {'nftables': []}),
                'final_nft': {'nftables': []},
                'drop_end_monotonic_ns': end_mono})
            self.emit('discovery_window_counter', **delta)
            boundary = self.emit('discovery_restore_boundary', window_index=index,
                                 restored=True, drop_installed=False,
                                 clock_guard_ns=50_000_000)
            boundary.update(post_restore_monotonic_ns=boundary['monotonic_ns'],
                            post_restore_unix_ns=boundary['time_ns'],
                            activity_since_unix_ns=boundary['time_ns'] + 50_000_000)
            observed = self.activity('post_restore_%d' % index,
                                     boundary['activity_since_unix_ns'],
                                     boundary['monotonic_ns'])
            confirmed = self.emit('discovery_recovery_confirmed', window_index=index, receipt={
                'recovery_verified': True,
                'restore_end_monotonic_ns': end_mono,
                'activity_receipt': {'evidence_ref': observed['raw_event_ref']},
                'pair_bindings': self._bindings()})
            self.emit('discovery_window_restore_verified', window_index=index,
                      window_id=plan['window_id'], restored=True,
                      recovery_confirmed=True, counter_hit=True)
            previous = confirmed['raw_event_ref']
        expected = list(range(self.config['pre_match_count'],
                              self.config['pre_match_count'] + self.config['post_match_count']))
        self.emit('delivery_contract', expected_sequences=expected,
                  sentinel_sequence=expected[-1], phase='post_restore',
                  fault_id=None, qos=self.config['qos'],
                  prejoin_in_delivery_contract=False)
        self.emit('window_open', window='post_restore_delivery', expected_sequences=expected)
        for sequence in expected:
            self.emit('send', role='writer', phase='post_restore',
                      send_sequence=sequence, writer_gid=self.tokens['writer']['endpoint_gid'],
                      endpoint_generation=1, publish_returned=True)
            self.emit('receive', role='reader', phase='post_restore',
                      send_sequence=sequence, reader_gid=self.tokens['reader']['endpoint_gid'],
                      endpoint_generation=1, publisher_gid=self.tokens['writer']['endpoint_gid'],
                      payload_run_id=self.config['run_id'])
        self.emit('window_close', window='post_restore_delivery')
        for role in ('writer', 'reader'):
            self.emit('graph_snapshot', role=role, publishers=[
                {'endpoint_gid': self.tokens['writer']['endpoint_gid']}],
                subscriptions=[{'endpoint_gid': self.tokens['reader']['endpoint_gid']}])
        self.emit('discovery_cleanup_restore', receipt={'restored': True})
        return self

    def runtime(self):
        root = self.root
        profile = root / 'cyclone.xml'
        profile.write_text(CYCLONE_XML)
        record = {'path': str(profile), 'sha256': hashlib.sha256(profile.read_bytes()).hexdigest()}
        return {
            'environment': {'ok': True,
                            'discovery_namespace': {'profiles': {'writer': record,
                                                                 'reader': record}}},
            'carrier_implementation': 'isolated_netns_nft_discovery_udp_drop',
            'precondition_receipts': [{}, {}, {}],
            'observation_receipts': [],
            'execution_metadata': {'workers': [{'role': 'writer', 'pid': 301, 'returncode': 0},
                                              {'role': 'reader', 'pid': 302, 'returncode': 0}]},
            'raw_path': str(root / 'raw.jsonl')}


class PerWindowFacts(unittest.TestCase):
    def fixture(self, windows):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        return WindowChainFixture(temp.name, windows).build()

    def test_all_windows_closed_is_complete(self):
        windows = [core_window(1, 0.1, schema=False, first=True),
             core_window(2, 0.1, schema=False)]
        fixture = self.fixture(windows)
        with patch.object(recovery_adapter, '_capture', return_value=fixture.capture):
            facts = recovery_facts(fixture.events, fixture.runtime(), fixture.config)
        self.assertNotIn('evidence_error', facts)
        self.assertTrue(facts['window']['complete'])
        self.assertTrue(facts['window_complete'])
        self.assertEqual(len(facts['windows']), 2)
        for index, window in enumerate(facts['windows']):
            self.assertEqual(window['window_index'], index)
            self.assertTrue(window['restored'])
            self.assertTrue(window['recovery_confirmed'])
            self.assertTrue(window['activity_observed'])
            self.assertGreater(window['drop']['packets_delta'], 0)
            self.assertEqual(window['drop']['counter_semantics'], 'per_window_delta_only')

    def test_missing_second_window_counter_is_unknown_with_window_index(self):
        windows = [core_window(1, 0.1, schema=False, first=True),
             core_window(2, 0.1, schema=False)]
        fixture = self.fixture(windows)
        counters = [event for event in fixture.events
                    if event.get('event') == 'discovery_window_counter']
        counters[1]['event'] = 'offline_removed_counter'
        with patch.object(recovery_adapter, '_capture', return_value=fixture.capture):
            facts = recovery_facts(fixture.events, fixture.runtime(), fixture.config)
        self.assertFalse(facts['window']['complete'])
        self.assertFalse(facts['window_complete'])
        self.assertIn('window[1]', facts['evidence_error'])

    def test_missing_second_window_activity_is_unknown(self):
        windows = [core_window(1, 0.1, schema=False, first=True),
             core_window(2, 0.1, schema=False)]
        fixture = self.fixture(windows)
        for event in fixture.events:
            if event.get('event') == 'discovery_activity_observation' and event.get('window') == 'post_restore_1':
                # Remove the second window's independent activity entirely.
                event['event'] = 'offline_removed_activity'
        with patch.object(recovery_adapter, '_capture', return_value=fixture.capture):
            facts = recovery_facts(fixture.events, fixture.runtime(), fixture.config)
        self.assertFalse(facts['window']['complete'])
        self.assertIn('window[1]', facts['evidence_error'])

    def test_single_window_legacy_path_shape_is_unchanged(self):
        # The golden assertion in SingleWindowGolden pins the generation shape;
        # the legacy facts path is exercised by test_recovery_trajectory_admission.
        core, runtime = _recovery_windows(0.2)
        self.assertNotIn('windows', core)
        self.assertNotIn('schema', runtime)


if __name__ == '__main__':
    unittest.main()
