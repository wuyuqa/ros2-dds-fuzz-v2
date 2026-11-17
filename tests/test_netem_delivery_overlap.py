"""Offline contracts for ``netem_covers_delivery`` (T-H).

Nothing here starts ROS, a namespace or tc.  The mock-carrier test drives the
real ``_run_netem_window``; the adapter test replays a synthetic raw token
stream through the unmodified ``recovery_facts`` admission code.
"""
import copy
import json
import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

from r2f.campaign import recovery_adapter
from r2f.runtime import discovery_network as network
from r2f.runtime.namespace_pair import (
    _run_netem_window, _run_post_restore_delivery, normalize_discovery_config,
    summarize_discovery,
)
from r2f.runtime.ros2_pair import normalize_config
from test_recovery_trajectory_admission import OfflineRecoveryTrajectory

SPEC = {'delay_ms': 50, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 3, 'scope': 'data'}

DELIVERY_EVENTS = [
    {'contract_includes_harness_omission': True, 'event': 'delivery_contract',
     'expected_sequences': [2, 3, 4, 5, 6, 7], 'fault_id': None, 'phase': 'post_restore',
     'prejoin_in_delivery_contract': False,
     'qos': {'depth': 32, 'durability': 'volatile', 'reliability': 'reliable'},
     'raw_event_ref': 'golden#0', 'sentinel_sequence': 7},
    {'event': 'window_open', 'expected_sequences': [2, 3, 4, 5, 6, 7],
     'raw_event_ref': 'golden#1', 'window': 'post_restore_delivery'},
]
for _seq in range(2, 8):
    _cid = 800 + _seq - 1
    DELIVERY_EVENTS.extend([
        {'command_id': _cid, 'event': 'command_done', 'role': 'writer'},
        {'command_id': _cid, 'event': 'send', 'phase': 'post_restore', 'role': 'writer',
         'send_sequence': _seq},
        {'command_id': _cid, 'event': 'receive', 'phase': 'post_restore', 'role': 'reader',
         'send_sequence': _seq},
    ])
DELIVERY_EVENTS.append({'deadline_expired': False, 'event': 'window_close',
                        'raw_event_ref': f'golden#{len(DELIVERY_EVENTS)}',
                        'window': 'post_restore_delivery'})


def delivery_config(**overrides):
    value = {'pre_match_count': 2, 'post_match_count': 6, 'fault_id': None,
             'qos': {'reliability': 'reliable', 'durability': 'volatile', 'depth': 32},
             'startup_timeout_sec': 10.0, 'publish_interval_sec': 0.03,
             'delivery_timeout_sec': 5.0}
    value.update(overrides)
    return value


class RecordingPool:
    """Commands append the same ACK/send/receive tokens the workers emit."""

    def __init__(self, events, start=800, drop_receives=()):
        self.events, self.next_id, self.drop_receives = events, start, set(drop_receives)

    def command(self, role, name, **fields):
        self.next_id += 1
        self.events.append({'event': 'command_done', 'role': role, 'command_id': self.next_id})
        if 'phase' in fields and not fields.get('omit'):
            self.events.append({'event': 'send', 'role': 'writer', 'phase': fields['phase'],
                                'send_sequence': fields['send_sequence'], 'command_id': self.next_id})
            if fields['send_sequence'] not in self.drop_receives:
                self.events.append({'event': 'receive', 'role': 'reader', 'phase': fields['phase'],
                                    'send_sequence': fields['send_sequence'],
                                    'command_id': self.next_id})
        return self.next_id

    def wait(self, predicate, timeout, label):
        return True


class GoldenLegacyDeliveryTests(unittest.TestCase):
    """``netem_covers_delivery=false`` must stay byte-identical to HEAD."""

    def test_helper_reproduces_head_event_stream_exactly(self):
        events = []

        def emit(event, **fields):
            events.append({'event': event, 'raw_event_ref': f'golden#{len(events)}', **fields})
            return events[-1]

        config = delivery_config()
        receipt = _run_post_restore_delivery(config, emit, RecordingPool(events), events, lambda _s: None)
        self.assertEqual(events, DELIVERY_EVENTS)
        self.assertEqual(receipt['expected_sequences'], [2, 3, 4, 5, 6, 7])
        self.assertEqual(receipt['omitted_sequence'], None)
        self.assertFalse(receipt['timed_out'])

    def test_negative_arm_still_omits_only_the_middle_sequence(self):
        events = []

        def emit(event, **fields):
            events.append({'event': event, 'raw_event_ref': f'golden#{len(events)}', **fields})
            return events[-1]

        config = delivery_config(fault_id='post_restore_delivery_omission')
        receipt = _run_post_restore_delivery(config, emit, RecordingPool(events), events, lambda _s: None)
        # expected = [2..7]; the middle index is 3 -> sequence 5.
        self.assertEqual(receipt['omitted_sequence'], 5)
        self.assertEqual([e['send_sequence'] for e in events if e['event'] == 'send'],
                         [2, 3, 4, 6, 7])

    def test_legacy_drop_facts_keep_their_exact_key_set(self):
        base = summarize_discovery([], active=True, counter_hit=True, restored=True,
                                   activity_ok=True, fresh_recovery=True, drop_activity_ok=True)
        self.assertNotIn('delivery_inside_mutation_window', base)
        self.assertNotIn('data_plane_observed', base)
        netem_legacy = summarize_discovery([], active=True, counter_hit=True, restored=True,
                                           activity_ok=True, fresh_recovery=True,
                                           drop_activity_ok=True, carrier_kind='discovery_netem',
                                           carrier_implementation=network.NETEM_IMPLEMENTATION,
                                           netem_scope='data')
        self.assertNotIn('delivery_inside_mutation_window', netem_legacy)


class FakeNetemCarrier:
    def __init__(self, plan, data_packets=8, delivery_packets=None):
        # ``delivery_packets`` lets a test prove that the reported data-plane
        # count comes from the delivery segment, not the stimulus one.
        self.plan, self.data_packets = plan, data_packets
        self.delivery_packets = data_packets if delivery_packets is None else delivery_packets
        self.starts, self.restores = 0, 0
        self._segment = 'stimulus'

    def _counter(self):
        packets = (self.delivery_packets if self._segment == 'delivery'
                   else self.data_packets)
        groups = {'data': {'packets': packets, 'bytes': packets * 10}}
        return {'group_counters': groups, 'group_counters_available': True,
                'group_counters_reason': None, 'mutation_hit': packets > 0,
                'filter': {'packets': packets},
                'affected_packets': packets, 'affected_bytes': 0,
                'port_groups': self.plan['groups'], 'expected_ports': []}

    def start(self, spec):
        self.starts += 1
        self._segment = 'stimulus' if self.starts == 1 else 'delivery'
        return {'configured': True, 'installed': True, 'applied': not spec['neutral'],
                'neutral': spec['neutral'], 'hit': bool(self.data_packets),
                'counter_observed': True, 'counter_snapshot': self._counter(),
                'tc_dump': {'qdisc': [], 'filter': []}, 'tc_dump_sha256': 'b' * 64,
                'groups': self.plan['groups'], 'scope': spec['scope'], 'spec': dict(spec)}

    def counters(self):
        return self._counter()

    def restore(self):
        self.restores += 1
        return {'restored': True, 'only_noqueue_root': True, 'residual_qdisc': [],
                'residual_filter': [], 'tc_dump_after_sha256': 'a' * 64,
                'counter_observed': True, 'counter_snapshot': self._counter()}

    def confirm_recovery(self, *, activity_receipt):
        return {'recovery_verified': True}


def run_netem(*, covers, scope='data', arm='mutated', delivery_packets=None):
    events, clock = [], [0.0]

    def emit(event, **fields):
        token = {'event': event, 'raw_event_ref': f'fake#{len(events)}', **fields}
        events.append(token)
        return token

    class Clock:
        @staticmethod
        def time():
            clock[0] += 0.05
            return clock[0]

        @staticmethod
        def time_ns():
            return int(clock[0] * 10 ** 9)

        @staticmethod
        def monotonic():
            return clock[0]

        @staticmethod
        def monotonic_ns():
            return int(clock[0] * 10 ** 9)

    def pause(seconds):
        clock[0] += max(0.0, float(seconds))

    spec = network.normalize_netem_spec(dict(SPEC, scope=scope))
    plan = network.build_netem_plan(network.DiscoveryPorts(173, 2), spec)
    carrier = FakeNetemCarrier(plan, delivery_packets=delivery_packets)
    receipt = dict.fromkeys(('planned', 'submitted', 'carrier_admitted', 'configured',
                             'applied', 'hit', 'effect_observed'), False)
    receipt['planned'] = arm == 'mutated'
    config = {'environment_mutation': {'kind': 'discovery_netem', 'spec': spec},
              'arm': arm, 'drop_duration_sec': 0.2, 'counter_interval_sec': 0.05,
              'recovery_timeout_sec': 0.2, 'netem_covers_delivery': covers,
              **delivery_config()}
    endpoints = {'writer': {'pid': 11, 'process_start_ticks': 110, 'endpoint_generation': 1,
                            'rmw_identifier': 'fake', 'run_id': 'fake', 'domain_id': 173,
                            'endpoint_gid': 'aa'},
                 'reader': {'pid': 12, 'process_start_ticks': 120, 'endpoint_generation': 1,
                            'rmw_identifier': 'fake', 'run_id': 'fake', 'domain_id': 173,
                            'endpoint_gid': 'bb'}}
    with patch('r2f.runtime.namespace_pair.time', Clock):
        result = _run_netem_window(
            config, arm == 'mutated', receipt, emit,
            lambda name, requires, action, observation=False: action(),
            lambda since, label, after_monotonic_ns=None: {
                'activity_observed': True, 'activity': {'status': 'observed'},
                'raw_event_ref': f'fake-activity#{label}'},
            lambda value, after_monotonic_ns=None: True,
            lambda anchor: {'matches': {}, 'graphs': {}}, pause, carrier,
            RecordingPool(events), events, endpoints, {},
            {'raw_event_ref': 'fake-precondition'}, {'domain_id': 173})
    return {'events': events, 'result': result, 'receipt': receipt, 'carrier': carrier}


def index_of(events, name, **match):
    return next(i for i, e in enumerate(events)
                if e['event'] == name and all(e.get(k) == v for k, v in match.items()))


class NetemCoversDeliveryTests(unittest.TestCase):
    def test_delivery_runs_inside_the_installed_class(self):
        out = run_netem(covers=True)
        events = out['events']
        install = index_of(events, 'delivery_netem_begin')
        open_ = index_of(events, 'window_open', window='post_restore_delivery')
        close = next(i for i, e in enumerate(events)
                     if e['event'] == 'window_close' and e.get('window') == 'post_restore_delivery'
                     and 'deadline_expired' in e)
        restore = index_of(events, 'delivery_netem_end')
        boundary = index_of(events, 'delivery_restore_boundary')
        self.assertLess(install, open_)
        self.assertLess(open_, close)
        self.assertLess(close, restore)
        self.assertLess(restore, boundary)
        self.assertTrue(out['result']['delivery_inside_mutation_window'])
        self.assertEqual(out['result']['delivery_window_refs'],
                         {'open_event_ref': events[open_]['raw_event_ref'],
                          'close_event_ref': events[close]['raw_event_ref']})
        self.assertEqual(out['result']['netem_install_ref'], events[install]['raw_event_ref'])
        self.assertEqual(out['result']['netem_restore_ref'], events[restore]['raw_event_ref'])

    def test_delivery_events_are_the_same_helper_output(self):
        out = run_netem(covers=True)
        names = [(e['event'], e.get('role'), e.get('send_sequence'), e.get('window'))
                 for e in out['events']
                 if e['event'] in ('send', 'receive', 'delivery_contract', 'window_open', 'window_close')
                 and e.get('window') in (None, 'post_restore_delivery')
                 and (e['event'] != 'window_close' or 'deadline_expired' in e)]
        expected = [(e['event'], e.get('role'), e.get('send_sequence'), e.get('window'))
                    for e in DELIVERY_EVENTS
                    if e['event'] in ('send', 'receive', 'delivery_contract', 'window_open', 'window_close')]
        self.assertEqual(names, expected)

    def test_data_end_precedes_restore_and_counter_after_delivery(self):
        out = run_netem(covers=True)
        events = out['events']
        begin = index_of(events, 'data_mutation_begin', window='data_netem_delivery')
        end = index_of(events, 'data_mutation_end', window='data_netem_delivery')
        restore = index_of(events, 'delivery_netem_end')
        self.assertLess(begin, end)
        self.assertLess(end, restore)
        counter_windows = [e['window'] for e in events if e['event'] == 'data_mutation_counter']
        self.assertIn('data_netem_delivery', counter_windows)

    def test_overlap_event_binds_stimulus_and_delivery_segments(self):
        out = run_netem(covers=True)
        overlap = next(e for e in out['events'] if e['event'] == 'netem_delivery_overlap')
        self.assertTrue(overlap['delivery_inside_mutation_window'])
        self.assertEqual(overlap['chronology'], 'install<delivery_open<delivery_close<restore<boundary')
        for key in ('install_event_ref', 'delivery_open_ref', 'delivery_close_ref',
                    'restore_event_ref', 'boundary_event_ref', 'stimulus_install_ref',
                    'stimulus_restore_ref', 'stimulus_boundary_ref'):
            self.assertTrue(overlap[key])
        self.assertNotEqual(overlap['install_event_ref'], overlap['stimulus_install_ref'])

    def test_stimulus_segment_keeps_the_sealed_restore_boundary(self):
        out = run_netem(covers=True)
        events = out['events']
        stimulus_begin = index_of(events, 'discovery_netem_begin')
        stimulus_end = index_of(events, 'discovery_netem_end')
        boundary = index_of(events, 'discovery_restore_boundary')
        self.assertLess(stimulus_begin, stimulus_end)
        self.assertLess(stimulus_end, boundary)
        self.assertLess(boundary, index_of(events, 'window_open', window='post_restore_delivery'))

    def test_two_carrier_segments_are_installed_and_restored(self):
        out = run_netem(covers=True)
        self.assertEqual(out['carrier'].starts, 2)
        self.assertEqual(out['carrier'].restores, 2)

    def test_flag_absent_keeps_single_segment_legacy_behaviour(self):
        out = run_netem(covers=False)
        events = out['events']
        self.assertEqual(out['carrier'].starts, 1)
        self.assertFalse(out['result'].get('delivery_inside_mutation_window', False))
        self.assertNotIn('netem_delivery_overlap', [e['event'] for e in events])
        self.assertNotIn('delivery_netem_begin', [e['event'] for e in events])
        # Legacy shape: the netem window restores before _body runs delivery.
        self.assertIn('discovery_restore_boundary', [e['event'] for e in events])
        self.assertLess(index_of(events, 'discovery_netem_end'),
                        index_of(events, 'discovery_restore_boundary'))

    def test_control_arm_never_claims_an_overlap(self):
        out = run_netem(covers=True, arm='transparent')
        self.assertEqual(out['carrier'].starts, 0)
        self.assertFalse(out['result'].get('delivery_inside_mutation_window', False))

    def test_data_plane_count_is_the_delivery_segment_not_the_stimulus(self):
        out = run_netem(covers=True, delivery_packets=0)
        self.assertEqual(out['result']['data_group_packets'], 0)
        self.assertFalse(out['result']['data_plane_counter_hit'])
        close = next(e for e in out['events'] if e['event'] == 'window_close'
                     and e.get('window') == 'discovery_netem')
        self.assertFalse(close['data_plane_counter_hit'])

    def test_delivery_packets_are_reported_when_the_window_covers_them(self):
        out = run_netem(covers=True, delivery_packets=3)
        self.assertEqual(out['result']['data_group_packets'], 3)
        self.assertTrue(out['result']['data_plane_counter_hit'])


class SummarizedFactsTests(unittest.TestCase):
    """Runtime facts for the covered window, and the data-plane distinction."""

    def test_covered_window_reports_refs_and_inside_true(self):
        out = run_netem(covers=True)
        events = out['events']
        overlap = next(e for e in events if e['event'] == 'netem_delivery_overlap')
        facts = summarize_discovery(
            events, active=True, counter_hit=True, restored=True, activity_ok=True,
            fresh_recovery=True, drop_activity_ok=True,
            carrier_kind='discovery_netem',
            carrier_implementation=network.NETEM_IMPLEMENTATION, netem_scope='data',
            delivery_overlap={'inside': True,
                              'window_refs': out['result']['delivery_window_refs'],
                              'install_ref': overlap['install_event_ref'],
                              'restore_ref': overlap['restore_event_ref'],
                              'boundary_ref': overlap['boundary_event_ref'],
                              'chronology': overlap['chronology']},
            data_plane_observed=False,
            data_plane_reason='data_plane_not_observed_inside_covered_window:'
                              'possibly_shm_or_non_udp_transport')
        self.assertTrue(facts['delivery_inside_mutation_window'])
        self.assertEqual(facts['delivery_window_refs'], out['result']['delivery_window_refs'])
        self.assertEqual(facts['netem_install_ref'], overlap['install_event_ref'])
        self.assertEqual(facts['netem_restore_ref'], overlap['restore_event_ref'])
        self.assertFalse(facts['data_plane_observed'])
        self.assertIn('possibly_shm_or_non_udp_transport',
                      facts['data_plane_observation_reason'])

    def test_zero_data_packets_is_never_reported_as_mutation_ineffective(self):
        out = run_netem(covers=True)
        facts = summarize_discovery(
            [], active=True, counter_hit=True, restored=True, activity_ok=True,
            fresh_recovery=True, drop_activity_ok=True,
            carrier_kind='discovery_netem',
            carrier_implementation=network.NETEM_IMPLEMENTATION, netem_scope='data',
            delivery_overlap={'inside': True, 'window_refs': {}, 'install_ref': 'i',
                              'restore_ref': 'r', 'boundary_ref': 'b',
                              'chronology': 'install<delivery_open<delivery_close<restore<boundary'},
            data_plane_observed=False,
            data_plane_reason='data_plane_not_observed_inside_covered_window:'
                              'possibly_shm_or_non_udp_transport')
        self.assertNotIn('mutation_ineffective', facts)
        self.assertTrue(facts['discovery_netem_counter_hit'])

    def test_unavailable_grouping_records_an_observation_limit(self):
        out = run_netem(covers=True)
        # Synthetic substitution at the fact boundary only: grouping failed, so
        # the runtime must not claim a data hit and must name the reason.
        facts = summarize_discovery(
            [], active=True, counter_hit=False, restored=True, activity_ok=True,
            fresh_recovery=True, drop_activity_ok=True,
            carrier_kind='discovery_netem',
            carrier_implementation=network.NETEM_IMPLEMENTATION, netem_scope='data',
            delivery_overlap={'inside': True, 'window_refs': {}, 'install_ref': 'i',
                              'restore_ref': 'r', 'boundary_ref': 'b',
                              'chronology': 'install<delivery_open<delivery_close<restore<boundary'},
            data_plane_observed=False,
            data_plane_reason='tc_action_group_counters_unavailable')
        self.assertEqual(facts['data_plane_observation_reason'],
                         'tc_action_group_counters_unavailable')

    def test_legacy_netem_facts_have_no_overlap_or_data_plane_keys(self):
        out = run_netem(covers=False)
        facts = summarize_discovery(out['events'], active=True, counter_hit=True, restored=True,
                                    activity_ok=True, fresh_recovery=True, drop_activity_ok=True,
                                    carrier_kind='discovery_netem',
                                    carrier_implementation=network.NETEM_IMPLEMENTATION,
                                    netem_scope='data')
        for key in ('delivery_inside_mutation_window', 'delivery_window_refs',
                    'netem_install_ref', 'netem_restore_ref', 'data_plane_observed',
                    'data_plane_observation_reason'):
            self.assertNotIn(key, facts)


class ConfigTests(unittest.TestCase):
    def config(self, **overrides):
        value = {'target_id': 'discovery.interruption_recovery', 'carrier': 'wire_proxy',
                 'mutation_kind': 'discovery_drop', 'domain_id': 173}
        value.update(overrides)
        return normalize_discovery_config(normalize_config(value))

    def test_default_is_false_and_drop_unchanged(self):
        self.assertFalse(self.config()['netem_covers_delivery'])

    def test_requires_netem_carrier(self):
        self.assertTrue(self.config(netem_covers_delivery=True,
                                    environment_mutation={'kind': 'discovery_netem',
                                                          'spec': dict(SPEC)})['netem_covers_delivery'])
        with self.assertRaisesRegex(ValueError, 'netem_covers_delivery_requires_discovery_netem'):
            self.config(netem_covers_delivery=True)
        with self.assertRaises(ValueError):
            self.config(netem_covers_delivery='yes',
                        environment_mutation={'kind': 'discovery_netem', 'spec': dict(SPEC)})


class NetemCoveredRecoveryTrajectory(OfflineRecoveryTrajectory):
    """Replay the runtime's two-segment netem order through the real adapter."""

    def __init__(self, root):
        super().__init__(root)
        self._rebuild_tail()

    def _rebuild_tail(self):
        snapshot_index = next(i for i, e in enumerate(self.events)
                              if e['event'] == 'discovery_precondition_snapshot')
        self.events = self.events[:snapshot_index + 1]
        self.runtime['observation_receipts'] = []
        self.waiting = True
        self.emit('mutation_begin', mutation_kind='discovery_netem')
        counter = {'group_counters': {'data': {'packets': 5, 'bytes': 50}},
                   'group_counters_available': True, 'group_counters_reason': None,
                   'mutation_hit': True, 'filter': {'packets': 5},
                   'affected_packets': 5, 'affected_bytes': 50, 'port_groups': [], 'expected_ports': []}
        begin = self.emit('discovery_netem_begin', receipt={
            'implementation': recovery_adapter.NETEM_IMPLEMENTATION, 'configured': True,
            'applied': True, 'neutral': False, 'hit': True, 'counter_observed': True,
            'counter_snapshot': counter, 'tc_dump_sha256': 'b' * 64, 'groups': [],
            'scope': 'data'})
        self.emit('data_mutation_begin', window='data_netem')
        self.emit('discovery_netem_counter', receipt=counter)
        inter_open = self.emit('window_open', window='discovery_netem', scope='data')
        self.activity('discovery_netem_window', inter_open['time_ns'])
        end = self.emit('discovery_netem_end', receipt={
            'restored': True, 'only_noqueue_root': True, 'counter_snapshot': counter})
        self.emit('data_mutation_end', window='data_netem')
        self.boundary = self.emit('discovery_restore_boundary', restored=True, neutral=False,
                                  clock_guard_ns=50_000_000)
        self.boundary.update(post_restore_monotonic_ns=self.boundary['monotonic_ns'],
                             post_restore_unix_ns=self.boundary['time_ns'],
                             activity_since_unix_ns=self.boundary['time_ns'] + 50_000_000)
        observed = self.activity('post_restore', self.boundary['activity_since_unix_ns'])
        observed.update(after_monotonic_ns=self.boundary['monotonic_ns'], clock_guard_ns=50_000_000)
        self.emit('discovery_netem_recovery_confirmed', receipt={'recovery_verified': True})
        self.query_pair(1)
        self.observe('observe_recovery_graph')
        delivery_begin = self.emit('delivery_netem_begin', receipt={
            'implementation': recovery_adapter.NETEM_IMPLEMENTATION, 'configured': True,
            'applied': True, 'neutral': False, 'hit': True, 'counter_observed': True,
            'counter_snapshot': counter, 'tc_dump_sha256': 'b' * 64, 'groups': [],
            'scope': 'data'})
        self.emit('data_mutation_begin', window='data_netem_delivery')
        expected = [1, 2, 3]
        self.emit('delivery_contract', expected_sequences=expected, fault_id=None,
                  sentinel_sequence=3, phase='post_restore', qos=self.config['qos'],
                  prejoin_in_delivery_contract=False)
        self.opening = self.emit('window_open', window='post_restore_delivery',
                                 expected_sequences=expected)
        for sequence in expected:
            self.emit('send', role='writer', phase='post_restore', send_sequence=sequence,
                      writer_gid=self.tokens['writer']['endpoint_gid'], publish_returned=True)
            self.emit('receive', role='reader', phase='post_restore', send_sequence=sequence,
                      reader_gid=self.tokens['reader']['endpoint_gid'],
                      publisher_gid=self.tokens['writer']['endpoint_gid'],
                      payload_run_id=self.config['run_id'])
        self.closing = self.emit('window_close', window='post_restore_delivery')
        self.emit('data_mutation_counter', window='data_netem_delivery')
        delivery_end = self.emit('delivery_netem_end', receipt={
            'restored': True, 'only_noqueue_root': True, 'counter_snapshot': counter})
        self.emit('data_mutation_end', window='data_netem_delivery')
        delivery_boundary = self.emit('delivery_restore_boundary', restored=True,
                                      netem_installed=False, clock_guard_ns=50_000_000)
        delivery_boundary.update(post_restore_monotonic_ns=delivery_boundary['monotonic_ns'],
                                 post_restore_unix_ns=delivery_boundary['time_ns'],
                                 activity_since_unix_ns=delivery_boundary['time_ns'] + 50_000_000)
        self.emit('netem_delivery_overlap', delivery_inside_mutation_window=True,
                  install_event_ref=delivery_begin['raw_event_ref'],
                  delivery_open_ref=self.opening['raw_event_ref'],
                  delivery_close_ref=self.closing['raw_event_ref'],
                  restore_event_ref=delivery_end['raw_event_ref'],
                  boundary_event_ref=delivery_boundary['raw_event_ref'],
                  stimulus_install_ref=begin['raw_event_ref'],
                  stimulus_restore_ref=end['raw_event_ref'],
                  stimulus_boundary_ref=self.boundary['raw_event_ref'])
        # The runtime closes its own netem window only after the delivery segment.
        self.emit('window_close', window='discovery_netem', scope='data')
        for role in self.tokens:
            self.emit('graph_snapshot', role=role, **self.graph(1, role))
        self.observe('observe_final_graph')
        self.emit('discovery_cleanup_restore', receipt={'restored': True})
        self.emit('namespace_controller_exited', returncode=0)
        self.runtime['carrier_implementation'] = recovery_adapter.NETEM_IMPLEMENTATION
        setup = next(e for e in self.events if e['event'] == 'discovery_network_setup')
        setup['receipt'] = {'health_ok': True, 'mutation_installed': False, 'initial_filter': []}


class NetemCoveredAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = NetemCoveredRecoveryTrajectory(self.temp.name)

    def facts(self):
        with patch.object(recovery_adapter, '_capture', return_value=self.fixture.capture):
            return recovery_adapter.recovery_facts(self.fixture.events, self.fixture.runtime,
                                                   self.fixture.config)

    def test_adapter_accepts_the_covered_order_and_reports_the_fact(self):
        facts = self.facts()
        self.assertNotIn('evidence_error', facts)
        self.assertTrue(facts['delivery_inside_mutation_window'])
        self.assertEqual(facts['delivery_window_refs'],
                         {'open_event_ref': self.fixture.opening['raw_event_ref'],
                          'close_event_ref': self.fixture.closing['raw_event_ref']})
        self.assertTrue(facts['window']['complete'])
        self.assertTrue(facts['recovery']['identity_stable'])

    def test_recovery_is_still_anchored_to_the_stimulus_boundary(self):
        facts = self.facts()
        self.assertEqual(facts['recovery']['boundary_ns'],
                         self.fixture.boundary['monotonic_ns'])
        self.assertLess(facts['recovery']['activity_ns'],
                        facts['window']['start_ns'])

    def test_missing_overlap_event_keeps_the_sealed_chronology(self):
        fixture = self.fixture
        fixture.events = [e for e in fixture.events if e['event'] != 'netem_delivery_overlap']
        facts = self.facts()
        self.assertIn('evidence_error', facts)
        self.assertFalse(facts['endpoint_identity_complete'])

    def test_unbound_overlap_reference_is_rejected(self):
        overlap = next(e for e in self.fixture.events if e['event'] == 'netem_delivery_overlap')
        overlap['delivery_open_ref'] = 'offline#nope'
        facts = self.facts()
        self.assertIn('evidence_error', facts)

    def test_json_serializable(self):
        json.dumps(self.facts(), sort_keys=True)


if __name__ == '__main__':
    unittest.main()
