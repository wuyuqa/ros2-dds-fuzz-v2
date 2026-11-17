"""Offline contracts for the netem ``scope`` extension (discovery/data/both).

Everything here is synthetic: no namespace, no tc call, no ROS. The fake
emitter-based window test exercises the real adapter code path with stubbed
carrier/step/pool objects, so event names and receipt fields are checked without
creating execution evidence.
"""
import json
import unittest
from unittest.mock import patch

from r2f.runtime import discovery_network as network
from r2f.runtime.discovery_network import DiscoveryNetworkError, NetemScopeError
from r2f.runtime.namespace_pair import (
    _netem_counter_hit, _netem_scope, _run_netem_window, normalize_discovery_config,
    normalize_environment_mutation,
)
from r2f.runtime.ros2_pair import normalize_config

LEGACY_SPEC = {'delay_ms': 50, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 3}


def spec(**overrides):
    value = dict(LEGACY_SPEC)
    value.update(overrides)
    return value


def netem_qdisc(packets=7, byte_count=700, root_packets=9, root_bytes=900):
    return [
        {'kind': 'prio', 'handle': '1:', 'root': True, 'options': {'bands': 2},
         'packets': root_packets, 'bytes': root_bytes, 'drops': 0, 'overlimits': 0},
        {'kind': 'netem', 'handle': '2:', 'parent': '1:2',
         'options': {'limit': 10000, 'delay': {'delay': 50000, 'jitter': 0, 'correlation': 0}},
         'packets': packets, 'bytes': byte_count, 'drops': 0, 'overlimits': 0},
    ]


def netem_filters(port_counts):
    """Owned flowid filters with per-port action counters from ``port_counts``."""
    result = []
    for port, (packets, byte_count) in port_counts.items():
        stats = {'packets': packets, 'bytes': byte_count, 'drops': 0,
                 'overlimits': 0, 'backlog': 0, 'qlen': 0}
        for family, off in (('ip', 20), ('ipv6', 40)):
            result.append({'parent': '1:', 'protocol': family, 'pref': 1, 'kind': 'u32',
                           'options': {'flowid': '1:2',
                                       'match': {'value': format(port, 'x'), 'mask': 'ffff',
                                                 'off': off},
                                       'actions': [{'kind': 'gact', 'stats': dict(stats)}]}})
    return result


class ScopeNormalizationTests(unittest.TestCase):
    def test_default_scope_is_discovery(self):
        normalized = network.normalize_netem_spec(spec())
        self.assertEqual(normalized['scope'], 'discovery')
        self.assertEqual(network.normalize_netem_spec(dict(spec(), scope='data'))['scope'], 'data')
        self.assertEqual(network.normalize_netem_spec(dict(spec(), scope='both'))['scope'], 'both')

    def test_invalid_scope_rejected_with_field_name(self):
        for bad in ('user', 'DATA', '', None, 1, True, ['data']):
            with self.subTest(scope=bad):
                with self.assertRaises(NetemScopeError) as failure:
                    network.normalize_netem_spec(dict(spec(), scope=bad))
                self.assertEqual(failure.exception.field, 'scope')
                self.assertIn('scope', str(failure.exception))
                self.assertEqual(failure.exception.receipt['field'], 'scope')
                self.assertEqual(failure.exception.receipt['allowed'],
                                 ['discovery', 'data', 'both'])

    def test_scope_error_is_a_carrier_error(self):
        self.assertTrue(issubclass(NetemScopeError, DiscoveryNetworkError))

    def test_environment_mutation_preserves_scope_and_names_field(self):
        for scope in ('discovery', 'data', 'both'):
            with self.subTest(scope=scope):
                value = normalize_environment_mutation(
                    {'kind': 'discovery_netem', 'spec': dict(spec(), scope=scope)})
                self.assertEqual(value['spec']['scope'], scope)
        with self.assertRaisesRegex(ValueError, 'scope'):
            normalize_environment_mutation(
                {'kind': 'discovery_netem', 'spec': dict(spec(), scope='user')})

    def test_delay_reorder_duplicate_rules_unchanged_and_loss_defaults_zero(self):
        with self.assertRaisesRegex(DiscoveryNetworkError, 'reorder_pct'):
            network.normalize_netem_spec(dict(spec(), delay_ms=0, reorder_pct=10))
        with self.assertRaisesRegex(DiscoveryNetworkError, 'delay_ms'):
            network.normalize_netem_spec(dict(spec(), delay_ms=2001))
        with self.assertRaisesRegex(DiscoveryNetworkError, 'duplicate_pct'):
            network.normalize_netem_spec(dict(spec(), duplicate_pct=-1))
        plan = network.build_netem_plan(network.DiscoveryPorts(1, 2),
                                        dict(spec(), scope='data'))
        self.assertIn('loss 0%', plan['netem_args'])
        self.assertEqual(plan['loss_pct'], 0.0)
        self.assertTrue(plan['loss_defaulted'])
        self.assertNotIn('corrupt', plan['netem_args'])
        for group in plan['groups']:
            self.assertIn('group_id', group)


class UserDataDerivationTests(unittest.TestCase):
    def test_default_rtps_user_data_formula_is_auditable(self):
        derivation = network.rtps_user_data_ports(173, 4)
        base = 7400 + 250 * 173
        expected = [base + 1, base + 11, base + 13, base + 15, base + 17]
        self.assertEqual(derivation['ports'], expected)
        self.assertEqual(derivation['multicast_port'], base + 1)
        self.assertEqual(derivation['unicast_ports'], [base + 11, base + 13, base + 15, base + 17])
        self.assertEqual(derivation['PB'], 7400)
        self.assertEqual(derivation['DG'], 250)
        self.assertEqual(derivation['d1'], 1)
        self.assertEqual(derivation['d2'], 11)
        self.assertEqual(derivation['user_multicast_offset'], 1)
        self.assertEqual(derivation['user_unicast_base_offset'], 11)
        self.assertEqual(derivation['port_count'], 5)
        self.assertEqual(derivation['mapping'], 'default_rtps_udp_user_data_ports')
        # The derivation must agree with the already-audited DiscoveryPorts map
        # and never overlap the discovery set.
        ports = network.DiscoveryPorts(173, 4)
        self.assertEqual(derivation['ports'], list(ports.user_data))
        self.assertFalse(set(derivation['ports']) & set(ports.discovery))

    def test_all_domains_and_safe_participant_counts(self):
        for domain in (0, 1, 173, 231, 232):
            count = min(120, (65535 - (7400 + 250 * domain) - 11) // 2 + 1)
            derivation = network.rtps_user_data_ports(domain, count)
            ports = network.DiscoveryPorts(domain, count)
            self.assertEqual(derivation['ports'], list(ports.user_data))
            self.assertLessEqual(max(derivation['ports']), 65535)

    def test_invalid_hosts_rejected(self):
        for bad in ((True, 2), (-1, 2), (233, 2), (1, 0), (1, 121)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                network.rtps_user_data_ports(*bad)


class ScopePortGroupTests(unittest.TestCase):
    def test_scope_selects_the_expected_port_set(self):
        ports = network.DiscoveryPorts(173, 4)
        discovery = network.netem_port_groups(ports, 'discovery')
        data = network.netem_port_groups(ports, 'data')
        both = network.netem_port_groups(ports, 'both')
        self.assertEqual([g['group_id'] for g in discovery], ['discovery'])
        self.assertEqual([g['group_id'] for g in data], ['data'])
        self.assertEqual([g['group_id'] for g in both], ['discovery', 'data'])
        self.assertEqual(discovery[0]['ports'], list(ports.discovery))
        self.assertEqual(data[0]['ports'], list(ports.user_data))
        self.assertEqual(both[0]['ports'], list(ports.discovery))
        self.assertEqual(both[1]['ports'], list(ports.user_data))
        # ``both`` must keep two distinguishable groups, not one merged set.
        self.assertNotEqual(both[0]['port_class'], both[1]['port_class'])
        self.assertIn('derivation_receipt', data[0])

    def test_invalid_scope_rejected(self):
        with self.assertRaises(NetemScopeError) as failure:
            network.netem_port_groups(network.DiscoveryPorts(173, 4), 'user')
        self.assertEqual(failure.exception.field, 'scope')


class GoldenDiscoveryCompatibilityTests(unittest.TestCase):
    """scope absent must reproduce the pre-change discovery plan byte for byte."""

    @staticmethod
    def legacy_plan(ports, normalized_spec):
        delay = normalized_spec['delay_ms'] / 1000.0
        netem = f'netem limit 10000 delay {delay:g}s'
        if normalized_spec['reorder_pct']:
            netem += f' reorder {normalized_spec["reorder_pct"]:g}% 0%'
        if normalized_spec['duplicate_pct']:
            netem += f' duplicate {normalized_spec["duplicate_pct"]:g}%'
        netem += ' loss 0%'
        lines = [
            'qdisc add dev lo root handle 1: prio bands 2 '
            'priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0',
            f'qdisc add dev lo parent 1:2 handle 2: {netem}',
        ]
        entries = []
        for port in ports.discovery:
            for prio, family, key in ((1, 'ip', 'ip'), (2, 'ipv6', 'ip6')):
                lines.append(
                    f'filter add dev lo protocol {family} parent 1:0 prio {prio} u32 '
                    f'match {key} protocol 17 0xff match {key} dport {port} 0xffff '
                    f'flowid 1:2 action gact pass')
                entries.append({'family': family, 'port': port, 'prio': prio})
        return lines, entries, netem

    def test_absent_scope_matches_legacy_script_entries_and_counts(self):
        for count in (2, 4, 32):
            with self.subTest(participant_count=count):
                ports = network.DiscoveryPorts(173, count)
                normalized = network.normalize_netem_spec(spec())
                plan = network.build_netem_plan(ports, spec())
                lines, entries, netem = self.legacy_plan(ports, normalized)
                self.assertEqual(plan['lines'], lines)
                self.assertEqual(plan['script'], '\n'.join(lines) + '\n')
                self.assertEqual(plan['netem_args'], netem)
                self.assertEqual([{k: v for k, v in e.items() if k != 'group_id'}
                                  for e in plan['entries']], entries)
                self.assertEqual(len(plan['entries']), 2 * len(ports.discovery))
                self.assertEqual(plan['matched_ports'], list(ports.discovery))
                self.assertFalse(set(plan['matched_ports']) & set(ports.user_data))
                self.assertEqual(len(plan['groups']), 1)
                self.assertEqual(plan['groups'][0]['group_id'], 'discovery')
                self.assertEqual(plan['groups'][0]['filter_count'], 2 * len(ports.discovery))
                self.assertEqual(plan['groups'][0]['filter_count_per_port'], 2)
                self.assertEqual(plan['groups'][0]['class_id'], '1:2')
                self.assertEqual(plan['applies_to'],
                                 'udp_dport_exact_derived_discovery_ports_only')

    def test_data_and_both_scripts_cover_the_expected_ports_only(self):
        ports = network.DiscoveryPorts(173, 4)
        data = network.build_netem_plan(ports, dict(spec(), scope='data'))
        both = network.build_netem_plan(ports, dict(spec(), scope='both'))
        self.assertEqual(data['matched_ports'], list(ports.user_data))
        self.assertFalse(set(data['matched_ports']) & set(ports.discovery))
        self.assertEqual(both['matched_ports'], [*ports.discovery, *ports.user_data])
        self.assertEqual(len(data['entries']), 2 * len(ports.user_data))
        self.assertEqual(len(both['entries']), 2 * (len(ports.discovery) + len(ports.user_data)))
        for plan in (data, both):
            self.assertNotIn('nft', plan['script'])
            self.assertIn('loss 0%', plan['netem_args'])
            for line in plan['lines']:
                self.assertNotIn('flush', line)
        self.assertEqual(data['applies_to'], 'udp_dport_exact_derived_user_data_ports_only')
        self.assertEqual(both['applies_to'],
                         'udp_dport_exact_derived_discovery_and_user_data_ports')
        # Group labels follow the entries so filters stay attributable.
        data_groups = {entry['port']: entry['group_id'] for entry in data['entries']}
        self.assertEqual(set(data_groups.values()), {'data'})
        both_groups = {entry['port']: entry['group_id'] for entry in both['entries']}
        for port in ports.discovery:
            self.assertEqual(both_groups[port], 'discovery')
        for port in ports.user_data:
            self.assertEqual(both_groups[port], 'data')


class GroupCounterTests(unittest.TestCase):
    def test_grouped_counters_are_separate_and_real(self):
        ports = network.DiscoveryPorts(173, 2)
        plan = network.build_netem_plan(ports, dict(spec(), scope='both'))
        counts = {port: (2, 200) for port in ports.discovery}
        counts.update({port: (5, 500) for port in ports.user_data})
        filters = netem_filters(counts)
        parsed = network.parse_netem_counters(netem_qdisc(packets=13, root_packets=21), filters,
                                              neutral=False, groups=plan['groups'],
                                              scope='both')
        self.assertTrue(parsed['group_counters_available'])
        self.assertIsNone(parsed['group_counters_reason'])
        # netem_filters installs one filter per (port, family); each family
        # keeps its own gact action counters, so the group sum is 2x per port.
        families = 2
        self.assertEqual(parsed['group_counters']['discovery']['packets'],
                         families * 2 * len(ports.discovery))
        self.assertEqual(parsed['group_counters']['data']['packets'],
                         families * 5 * len(ports.user_data))
        self.assertEqual(parsed['group_counters']['data']['bytes'],
                         families * 500 * len(ports.user_data))
        self.assertEqual(parsed['hit_semantics'],
                         'netem_matched_discovery_and_data_plane_packets_not_sut_defect')
        self.assertEqual(parsed['responsibility_layer'], 'environment_harness')
        self.assertEqual(parsed['loss_pct'], 0.0)

    def test_grouping_unavailable_is_declared_not_invented(self):
        ports = network.DiscoveryPorts(173, 2)
        plan = network.build_netem_plan(ports, dict(spec(), scope='both'))
        filters = netem_filters({port: (1, 100) for port in plan['matched_ports']})
        filters[0]['options']['actions'] = []  # one filter loses action stats
        parsed = network.parse_netem_counters(netem_qdisc(), filters, neutral=False,
                                              groups=plan['groups'], scope='both')
        self.assertFalse(parsed['group_counters_available'])
        self.assertEqual(parsed['group_counters_reason'], 'tc_action_counters_missing')

    def test_missing_declared_port_is_unavailable(self):
        ports = network.DiscoveryPorts(173, 2)
        plan = network.build_netem_plan(ports, dict(spec(), scope='both'))
        counts = {port: (1, 100) for port in ports.discovery}  # no data filters at all
        parsed = network.parse_netem_counters(netem_qdisc(), netem_filters(counts),
                                              neutral=False, groups=plan['groups'],
                                              scope='both')
        self.assertFalse(parsed['group_counters_available'])
        self.assertEqual(parsed['group_counters_reason'], 'declared_port_filter_missing')

    def test_undeclared_filter_port_is_unavailable(self):
        ports = network.DiscoveryPorts(173, 2)
        plan = network.build_netem_plan(ports, dict(spec(), scope='data'))
        counts = {port: (1, 100) for port in plan['matched_ports']}
        counts[65000] = (1, 100)  # stray filter outside the declared groups
        parsed = network.parse_netem_counters(netem_qdisc(), netem_filters(counts),
                                              neutral=False, groups=plan['groups'],
                                              scope='data')
        self.assertFalse(parsed['group_counters_available'])
        self.assertEqual(parsed['group_counters_reason'],
                         'filter_port_outside_declared_groups')

    def test_legacy_call_without_groups_still_reports_flat_totals(self):
        parsed = network.parse_netem_counters(netem_qdisc(packets=4),
                                              netem_filters({50650: (2, 200)}),
                                              neutral=False)
        self.assertEqual(parsed['affected_packets'], 4)
        self.assertFalse(parsed['group_counters_available'])
        self.assertEqual(parsed['group_counters_reason'], 'port_groups_not_declared')
        self.assertEqual(parsed['hit_semantics'], 'netem_matched_packets_not_drop_hit')


def _counter(groups_packets, available=True, reason=None, neutral=False, scope='data'):
    """A carrier counter receipt as ``DiscoveryNetemController.counters`` returns."""
    group_counters = {
        'discovery': {'packets': groups_packets.get('discovery', 0), 'bytes': 0,
                      'filter_entries': 0, 'ports_observed': [], 'ports_expected': [],
                      'changed_packets': 0},
        'data': {'packets': groups_packets.get('data', 0), 'bytes': 0,
                 'filter_entries': 0, 'ports_observed': [], 'ports_expected': [],
                 'changed_packets': 0},
    }
    total = group_counters['data']['packets']
    return {'mutation_hit': bool(not neutral and total > 0),
            'affected_packets': total, 'affected_bytes': 0,
            'group_counters': group_counters,
            'group_counters_available': available,
            'group_counters_reason': reason,
            'neutral': neutral, 'scope': scope,
            'port_groups': [], 'expected_ports': [],
            'packets': total, 'bytes': 0, 'filter': {'entries': []}}


class FakeCarrier:
    """Stub carrier: ``start``/``counters``/``restore`` never touch tc."""

    def __init__(self, *, plan, data_packets=0, group_available=True, groups=None,
                 neutral=False):
        self.plan = plan
        self.data_packets = data_packets
        self.group_available = group_available
        self.groups = groups if groups is not None else plan['groups']
        self.neutral = neutral
        self.started, self.restored = [], []

    def start(self, spec):
        self.started.append(spec)
        snapshot = self.counters()
        return {'event': 'discovery_netem_begin', 'configured': True, 'installed': True,
                'applied': not spec['neutral'], 'neutral': spec['neutral'],
                'hit': bool(not spec['neutral'] and snapshot['mutation_hit']),
                'counter_observed': True, 'counter_snapshot': snapshot,
                'tc_dump': {'qdisc': [], 'filter': []},
                'tc_dump_sha256': 'b' * 64, 'groups': self.groups,
                'scope': spec['scope'], 'derivation': {}, 'spec': dict(spec)}

    def counters(self):
        return _counter({'data': self.data_packets}, available=self.group_available,
                        reason=None if self.group_available else 'tc_action_counters_missing',
                        neutral=self.neutral)

    def restore(self):
        self.restored.append(True)
        return {'event': 'discovery_netem_end', 'restored': True,
                'only_noqueue_root': True, 'residual_qdisc': [], 'residual_filter': [],
                'tc_dump_after_sha256': 'a' * 64, 'counter_observed': True,
                'counter_snapshot': self.counters()}

    def confirm_recovery(self, *, activity_receipt):
        return {'event': 'discovery_netem_recovery_confirmed', 'recovery_verified': True}


class _FakeStep:
    def __call__(self, name, requires, action, *, observation=False):
        return action()


def fake_endpoints():
    """Minimal endpoint tokens; only the trajectory identity keys are read."""
    def token(pid, gid):
        return {'pid': pid, 'process_start_ticks': pid * 10, 'endpoint_generation': 1,
                'rmw_identifier': 'fake_rmw', 'run_id': 'fake_run', 'domain_id': 173,
                'endpoint_gid': gid}

    return {'writer': token(11, 'aa'), 'reader': token(12, 'bb')}


class _FakePool:
    def __init__(self):
        self.commands = 0

    def command(self, role, name, **fields):
        self.commands += 1
        return self.commands

    def wait(self, predicate, timeout, label):
        return True


def run_window(*, scope='data', data_packets=4, group_available=True,
               arm='mutated', spec_overrides=None, groups=None, neutral=None):
    """Execute the real ``_run_netem_window`` with fake collaborators."""
    emitted = []

    def emit(event, **fields):
        token = {'event': event, 'raw_event_ref': f'fake#{len(emitted)}', **fields}
        emitted.append(token)
        return token

    ports = network.DiscoveryPorts(173, 2)
    full_spec = network.normalize_netem_spec(
        dict(spec(**(spec_overrides or {})), scope=scope))
    neutral = full_spec['neutral'] if neutral is None else neutral
    plan = network.build_netem_plan(ports, full_spec)
    carrier = FakeCarrier(plan=plan, data_packets=data_packets,
                          group_available=group_available, groups=groups,
                          neutral=neutral)
    receipt = dict.fromkeys(('planned', 'submitted', 'carrier_admitted', 'configured',
                             'applied', 'hit', 'effect_observed'), False)
    receipt['planned'] = arm == 'mutated'

    clock = [0.0]

    class Clock:
        """Virtual monotonic clock so the window never really sleeps."""

        @staticmethod
        def time():
            clock[0] += 0.05
            return clock[0]

        @staticmethod
        def time_ns():
            return int(clock[0] * 10**9)

        @staticmethod
        def monotonic():
            return clock[0]

        @staticmethod
        def monotonic_ns():
            return int(clock[0] * 10**9)

    def pause(seconds):
        if hasattr(seconds, 'item'):
            seconds = seconds.item()
        clock[0] += max(0.0, float(seconds))

    def activity(since_ns, label, after_monotonic_ns=None):
        return {'activity_observed': True, 'activity': {'status': 'observed'},
                'raw_event_ref': f'fake-activity#{label}'}

    c = {'environment_mutation': {'kind': 'discovery_netem', 'spec': full_spec},
         'arm': arm, 'drop_duration_sec': 0.2, 'counter_interval_sec': 0.05,
         'recovery_timeout_sec': 0.2}
    with patch('r2f.runtime.namespace_pair.time', Clock):
        result = _run_netem_window(
            c, arm == 'mutated', receipt, emit, _FakeStep(), activity,
            lambda value, after_monotonic_ns=None: True,
            lambda anchor: {'matches': {}, 'graphs': {}}, pause, carrier, _FakePool(),
            [], fake_endpoints(), {},
            {'raw_event_ref': 'fake-precondition'}, {'domain_id': 173})
    return {'events': emitted, 'receipt': receipt, 'result': result,
            'carrier': carrier, 'spec': full_spec}


class DataWindowEventTests(unittest.TestCase):
    def test_data_scope_emits_the_three_data_events_alongside_discovery_ones(self):
        out = run_window(scope='data', data_packets=4)
        names = [event['event'] for event in out['events']]
        for required in ('data_mutation_begin', 'data_mutation_counter', 'data_mutation_end',
                         'discovery_netem_begin', 'discovery_netem_end',
                         'window_open', 'window_close'):
            self.assertIn(required, names)
        # Data events are additive: discovery events still exist.
        self.assertLess(names.index('data_mutation_begin'), names.index('data_mutation_counter'))
        self.assertLess(names.index('data_mutation_counter'), names.index('data_mutation_end'))
        begin = next(e for e in out['events'] if e['event'] == 'data_mutation_begin')
        counter = next(e for e in out['events'] if e['event'] == 'data_mutation_counter')
        end = next(e for e in out['events'] if e['event'] == 'data_mutation_end')
        self.assertEqual(begin['spec']['scope'], 'data')
        self.assertEqual(begin['port_groups'], out['carrier'].plan['groups'])
        self.assertEqual(begin['tc_dump_sha256'], 'b' * 64)
        self.assertEqual(end['tc_dump_after_sha256'], 'a' * 64)
        self.assertEqual(end['tc_dump_sha256'], 'b' * 64)
        self.assertTrue(end['restored'])
        for event in (begin, counter, end):
            self.assertEqual(event['hit_semantics'],
                             'netem_matched_data_plane_packets_not_sut_defect')
            self.assertEqual(event['responsibility_layer'], 'environment_harness')
            self.assertEqual(event['loss_pct'], 0.0)
            self.assertTrue(event['loss_defaulted'])
        self.assertEqual(counter['group_counters']['data']['packets'], 4)
        self.assertEqual(counter['hit_groups'], ['data'])

    def test_discovery_scope_emits_no_data_events(self):
        out = run_window(scope='discovery', data_packets=0)
        names = [event['event'] for event in out['events']]
        self.assertNotIn('data_mutation_begin', names)
        self.assertNotIn('data_mutation_counter', names)
        self.assertNotIn('data_mutation_end', names)

    def test_both_scope_keeps_groups_distinguishable_in_events(self):
        out = run_window(scope='both', data_packets=3)
        begin = next(e for e in out['events'] if e['event'] == 'data_mutation_begin')
        self.assertEqual(begin['port_group_ids'], ['discovery', 'data'])
        self.assertEqual([group['port_class'] for group in begin['port_groups']],
                         ['discovery_udp_ports', 'user_data_udp_ports'])
        # ``both`` counts either port group, so its wording must not claim a
        # data-only match; per-group attribution stays in the counter receipt.
        counter = next(e for e in out['events'] if e['event'] == 'data_mutation_counter')
        self.assertEqual(counter['hit_semantics'],
                         'netem_matched_discovery_and_data_plane_packets_not_sut_defect')
        self.assertEqual(counter['hit_groups'], ['data'])

    def test_both_scope_grouping_unavailable_is_not_a_hit(self):
        out = run_window(scope='both', data_packets=4, group_available=False)
        self.assertFalse(out['receipt']['hit'])
        self.assertIsNone(out['result']['data_plane_counter_hit'])

    def test_hit_is_true_only_for_real_counted_data_packets(self):
        hit = run_window(scope='data', data_packets=2)
        self.assertTrue(hit['receipt']['hit'])
        self.assertTrue(hit['result']['data_plane_counter_hit'])
        zero = run_window(scope='data', data_packets=0)
        self.assertFalse(zero['receipt']['hit'])
        self.assertFalse(zero['result']['data_plane_counter_hit'])

    def test_neutral_data_spec_never_hits(self):
        out = run_window(scope='data', data_packets=10,
                         spec_overrides={'delay_ms': 0, 'reorder_pct': 0,
                                         'duplicate_pct': 0})
        self.assertFalse(out['receipt']['hit'])
        self.assertFalse(out['carrier'].started[0]['neutral'] is False)

    def test_control_arm_installs_nothing_and_reports_no_hit(self):
        out = run_window(scope='data', data_packets=10, arm='transparent')
        names = [event['event'] for event in out['events']]
        self.assertIn('control_begin', names)
        self.assertNotIn('data_mutation_begin', names)
        self.assertEqual(out['carrier'].started, [])
        self.assertFalse(out['receipt']['hit'])

    def test_unavailable_grouping_never_becomes_a_data_hit(self):
        out = run_window(scope='data', data_packets=9, group_available=False)
        self.assertFalse(out['receipt']['hit'])
        self.assertIsNone(out['result']['data_plane_counter_hit'])
        counter = next(e for e in out['events']
                       if e['event'] == 'data_mutation_counter')
        self.assertFalse(counter['group_counters_available'])
        self.assertEqual(counter['group_counters_reason'], 'tc_action_counters_missing')
        self.assertEqual(counter['hit_groups'], [])

    def test_scope_is_passed_through_untouched(self):
        out = run_window(scope='both', data_packets=1)
        self.assertEqual(out['carrier'].started[0]['scope'], 'both')
        self.assertEqual(out['result']['scope'], 'both')


class ScopeAdmissionTests(unittest.TestCase):
    def discovery_config(self, **overrides):
        config = {'target_id': 'discovery.interruption_recovery', 'carrier': 'wire_proxy',
                  'mutation_kind': 'discovery_drop', 'domain_id': 173}
        config.update(overrides)
        return normalize_discovery_config(normalize_config(config))

    def test_data_scope_survives_config_normalization(self):
        c = self.discovery_config(environment_mutation={
            'kind': 'discovery_netem',
            'spec': dict(spec(), scope='data')})
        self.assertEqual(c['environment_mutation']['spec']['scope'], 'data')
        self.assertEqual(c['carrier_implementation'],
                         'isolated_netns_tc_netem_discovery_udp')

    def test_invalid_scope_blocks_before_execution(self):
        with self.assertRaisesRegex(ValueError, 'scope'):
            self.discovery_config(environment_mutation={
                'kind': 'discovery_netem', 'spec': dict(spec(), scope='user')})

    def test_helper_scope_and_hit_semantics(self):
        self.assertEqual(_netem_scope(spec()), 'discovery')
        self.assertEqual(_netem_scope(dict(spec(), scope='both')), 'both')
        self.assertTrue(_netem_counter_hit(_counter({'data': 1}), 'data'))
        self.assertFalse(_netem_counter_hit(_counter({'data': 0}), 'data'))
        self.assertFalse(_netem_counter_hit(_counter({'data': 5}, available=False), 'data'))
        self.assertTrue(_netem_counter_hit(_counter({'data': 5}), 'data', key='mutation_hit'))
        legacy = {'mutation_hit': True, 'group_counters_available': False}
        self.assertTrue(_netem_counter_hit(legacy, 'discovery'))

    def test_payloads_are_json_serializable(self):
        out = run_window(scope='both', data_packets=2)
        for event in out['events']:
            json.dumps(event, sort_keys=True)


if __name__ == '__main__':
    unittest.main()
