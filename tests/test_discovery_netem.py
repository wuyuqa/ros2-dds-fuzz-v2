"""Offline netem carrier contracts. Synthetic tc JSON here is not execution evidence."""
import json
import unittest
from unittest.mock import patch

from r2f.runtime import discovery_network as network
from r2f.runtime.discovery_network import DiscoveryNetworkError
from r2f.runtime.namespace_pair import (
    discovery_carrier_kind, normalize_discovery_config, normalize_environment_mutation,
)
from r2f.runtime.ros2_pair import normalize_config


def spec(**overrides):
    value = {'delay_ms': 50, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 3}
    value.update(overrides)
    return value


def netem_qdisc(packets=7, byte_count=700, drops=0, overlimits=0, root_packets=9, root_bytes=900,
                delay=None, duplicate=None, reorder=None):
    options = {'limit': 10000, 'ecn': False, 'gap': 0}
    if delay is not None:
        options['delay'] = {'delay': delay, 'jitter': 0, 'correlation': 0}
    if duplicate is not None:
        options['duplicate'] = {'duplicate': duplicate, 'correlation': 0}
    if reorder is not None:
        options['reorder'] = {'reorder': reorder, 'correlation': 0}
    return [
        {'kind': 'prio', 'handle': '1:', 'root': True, 'options': {'bands': 2},
         'packets': root_packets, 'bytes': root_bytes, 'drops': 0, 'overlimits': 0},
        {'kind': 'netem', 'handle': '2:', 'parent': '1:2', 'options': options,
         'packets': packets, 'bytes': byte_count, 'drops': drops, 'overlimits': overlimits},
    ]


def netem_filters(ports=(50650, 50660), packets=3, byte_count=300):
    """Owned flowid filters plus the protocol-level rows real tc -j prints."""
    stats = {'packets': packets, 'bytes': byte_count, 'drops': 0,
             'overlimits': 0, 'backlog': 0, 'qlen': 0}
    result = []
    for port in ports:
        result.append({'parent': '1:', 'protocol': 'ip', 'pref': 1, 'kind': 'u32',
                       'options': {'flowid': '1:2',
                                   'match': {'value': format(port, 'x'), 'mask': 'ffff',
                                             'off': 20},
                                   'actions': [{'kind': 'gact', 'stats': dict(stats)}]}})
        result.append({'parent': '1:', 'protocol': 'ip', 'pref': 1, 'kind': 'u32'})
    return result


class NetemSpecTests(unittest.TestCase):
    def test_spec_fields_are_bounded_and_typed(self):
        normalized = network.normalize_netem_spec(spec(delay_ms=0, duplicate_pct=100))
        self.assertEqual(normalized['delay_ms'], 0.0)
        self.assertFalse(normalized['neutral'])
        for bad in (None, [], {'delay_ms': 0}, spec(delay_ms=-1), spec(delay_ms=2001),
                    spec(reorder_pct=101), spec(duplicate_pct=-1), spec(restore_sec=-1),
                    spec(delay_ms=True), spec(delay_ms='50'), spec(delay_ms=float('nan')),
                    spec(delay_ms=float('inf')), spec(restore_sec=float('inf')),
                    spec(restore_sec=float('nan')), spec(restore_sec=True)):
            with self.subTest(bad=bad), self.assertRaises(DiscoveryNetworkError):
                network.normalize_netem_spec(bad)

    def test_each_invalid_field_is_named(self):
        for field in ('delay_ms', 'reorder_pct', 'duplicate_pct', 'restore_sec'):
            with self.subTest(field=field), self.assertRaisesRegex(DiscoveryNetworkError, field):
                network.normalize_netem_spec(spec(**{field: -1}))
        with self.assertRaisesRegex(DiscoveryNetworkError, 'unknown field'):
            network.normalize_netem_spec(spec(injected=1))
        with self.assertRaisesRegex(DiscoveryNetworkError, 'missing field'):
            network.normalize_netem_spec({'delay_ms': 1})

    def test_reorder_requires_delay_because_netem_does(self):
        with self.assertRaisesRegex(DiscoveryNetworkError, 'reorder_pct'):
            network.normalize_netem_spec(spec(delay_ms=0, reorder_pct=50))
        self.assertTrue(network.normalize_netem_spec(
            spec(delay_ms=1, reorder_pct=50))['neutral'] is False)

    def test_neutral_definition_and_receipt_reparse(self):
        neutral = network.normalize_netem_spec(
            spec(delay_ms=0, reorder_pct=0, duplicate_pct=0))
        self.assertTrue(neutral['neutral'])
        self.assertTrue(network.normalize_netem_spec(neutral)['neutral'])
        self.assertFalse(network.normalize_netem_spec(spec(delay_ms=1))['neutral'])
        self.assertFalse(network.normalize_netem_spec(spec(duplicate_pct=1))['neutral'])


class NetemPlanTests(unittest.TestCase):
    def test_filters_are_exact_derived_discovery_ports_only(self):
        ports = network.DiscoveryPorts(173, 4)
        plan = network.build_netem_plan(ports, spec())
        self.assertEqual(plan['implementation'], 'isolated_netns_tc_netem_discovery_udp')
        self.assertEqual(set(plan['matched_ports']), set(ports.discovery))
        self.assertFalse(set(plan['matched_ports']) & set(ports.user_data))
        self.assertEqual(len(plan['entries']), 2 * len(ports.discovery))
        for entry in plan['entries']:
            self.assertIn(entry['port'], ports.discovery)
            self.assertEqual(entry['family'], 'ip' if entry['prio'] == 1 else 'ipv6')
        for line in plan['lines']:
            self.assertNotIn('flush', line)
            self.assertNotIn('drop', line)
        self.assertEqual(sum('filter add' in line for line in plan['lines']),
                         2 * len(ports.discovery))
        self.assertNotIn('nft', plan['script'])
        for port in plan['entries']:
            self.assertIn(str(port['port']), plan['script'])

    def test_protocol_families_use_distinct_priorities(self):
        plan = network.build_netem_plan(network.DiscoveryPorts(1, 2), spec())
        prios = {(e['family'], e['prio']) for e in plan['entries']}
        self.assertEqual(prios, {('ip', 1), ('ipv6', 2)})

    def test_netem_arguments_are_transparent_without_a_spec(self):
        plan = network.build_netem_plan(network.DiscoveryPorts(1), spec(
            delay_ms=0, reorder_pct=0, duplicate_pct=0))
        self.assertEqual(plan['netem_args'], 'netem limit 10000 delay 0s loss 0%')
        self.assertNotIn('reorder', plan['netem_args'])
        self.assertNotIn('duplicate', plan['netem_args'])

    def test_netem_arguments_never_drop(self):
        for override in (spec(), spec(delay_ms=0, duplicate_pct=100),
                         spec(delay_ms=1, reorder_pct=100)):
            with self.subTest(override=override):
                args = network.build_netem_plan(network.DiscoveryPorts(1), override)['netem_args']
                self.assertIn('loss 0%', args)
                self.assertNotIn('loss 1', args)
                self.assertNotIn('corrupt', args)

    def test_delay_and_duplicate_are_rendered_from_the_spec(self):
        args = network.build_netem_plan(network.DiscoveryPorts(1),
                                        spec(delay_ms=250, duplicate_pct=25))['netem_args']
        self.assertIn('delay 0.25s', args)
        self.assertIn('duplicate 25%', args)
        args = network.build_netem_plan(network.DiscoveryPorts(1),
                                        spec(delay_ms=20, reorder_pct=40))['netem_args']
        self.assertIn('reorder 40% 0%', args)


class NetemCounterTests(unittest.TestCase):
    def test_per_class_numbers_come_from_tc(self):
        parsed = network.parse_netem_counters(
            netem_qdisc(packets=7, byte_count=700, root_packets=9, root_bytes=900),
            netem_filters(packets=3, byte_count=300), neutral=False)
        self.assertEqual(parsed['classes']['1:2']['packets'], 7)
        self.assertEqual(parsed['classes']['1:1']['packets'], 2)
        self.assertEqual(parsed['classes']['root']['packets'], 9)
        self.assertEqual(parsed['filter']['packets'], 6)
        self.assertEqual(parsed['affected_packets'], 7)
        self.assertTrue(parsed['mutation_hit'])
        self.assertEqual(parsed['hit_semantics'], 'netem_matched_packets_not_drop_hit')

    def test_both_protocol_families_report_action_counters(self):
        filters = []
        for prio, family, key in ((1, 'ip', 'ip'), (2, 'ipv6', 'ip6')):
            filters.append({'parent': '1:', 'protocol': family, 'pref': prio, 'kind': 'u32',
                            'options': {'flowid': '1:2',
                                        'match': {'value': format(50650, 'x'), 'mask': 'ffff',
                                                  'off': 20 if key == 'ip' else 40},
                                        'actions': [{'kind': 'gact', 'stats': {
                                            'packets': 2, 'bytes': 200, 'drops': 0,
                                            'overlimits': 0}}]}})
        parsed = network.parse_netem_counters(netem_qdisc(), filters, neutral=False)
        self.assertEqual({e['family'] for e in parsed['filter']['entries']}, {'ip', 'ipv6'})
        self.assertEqual(parsed['filter']['packets'], 4)
        self.assertEqual(parsed['filter']['bytes'], 400)
        self.assertTrue(parsed['filter']['stats_available'])

    def test_neutral_counter_is_never_a_hit(self):
        parsed = network.parse_netem_counters(
            netem_qdisc(packets=4), netem_filters(), neutral=True)
        self.assertFalse(parsed['mutation_hit'])
        self.assertEqual(parsed['affected_packets'], 4)

    def test_missing_or_malformed_counters_never_become_zero(self):
        for qdisc, filters in ((netem_qdisc()[:1], netem_filters()),
                               (netem_qdisc() + netem_qdisc()[1:], netem_filters()),
                               (netem_qdisc(), []),
                               (netem_qdisc(), [{'parent': '1:', 'kind': 'u32'}]),
                               (netem_qdisc(packets=-1), netem_filters()),
                               (netem_qdisc(root_packets=1), netem_filters())):
            with self.subTest(qdisc=qdisc, filters=filters), \
                    self.assertRaises(DiscoveryNetworkError):
                network.parse_netem_counters(qdisc, filters, neutral=False)

    def test_filter_without_action_stats_is_reported_not_invented(self):
        filters = netem_filters()
        filters[0]['options']['actions'] = []
        parsed = network.parse_netem_counters(netem_qdisc(), filters, neutral=False)
        self.assertFalse(parsed['filter']['stats_available'])
        self.assertIsNone(parsed['filter']['packets'])


class NetemIsolationTests(unittest.TestCase):
    def test_host_missing_proof_never_invokes_tc(self):
        with patch.dict(network.os.environ, {}, clear=True), \
                patch.object(network.subprocess, 'run') as run:
            with self.assertRaises(network.NetworkIsolationError):
                network.DiscoveryNetemController(173).setup()
            run.assert_not_called()

    def test_start_requires_setup_then_valid_spec(self):
        controller = network.DiscoveryNetemController(173)
        with patch.object(network.subprocess, 'run') as run, \
                self.assertRaises(DiscoveryNetworkError):
            controller.start(spec())
        run.assert_not_called()

    def test_guard_rechecked_on_counters_and_restore(self):
        guard = patch.object(network, 'assert_isolated_namespace',
                             return_value={'isolated': True, 'observed_net_namespace': 'net:[t]'})
        guard.start()
        self.addCleanup(guard.stop)
        controller = network.DiscoveryNetemController(173)
        controller.active = controller._attempted = True
        controller._begin = {'netem_begin_monotonic_ns': 0}
        controller.spec = network.normalize_netem_spec(spec())
        with patch.object(network, 'assert_isolated_namespace',
                          side_effect=network.NetworkIsolationError('changed')), \
                patch.object(network, '_execute') as execute:
            for call in (controller.counters, controller.restore):
                with self.assertRaises(network.NetworkIsolationError):
                    call()
            execute.assert_not_called()


class NetemLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.guard = patch.object(network, 'assert_isolated_namespace', return_value={
            'isolated': True, 'observed_net_namespace': 'net:[test]'})
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.controller = network.DiscoveryNetemController(173)

    def dump(self, qdisc=None, filters=None):
        return {'qdisc': qdisc or netem_qdisc(), 'filter': filters if filters is not None
                else netem_filters()}

    def setup_controller(self, qdisc=None, filters=None):
        qdisc = qdisc if qdisc is not None else [
            {'kind': 'noqueue', 'handle': '0:', 'root': True, 'options': {}}]
        filters = filters if filters is not None else []
        with patch.object(network, '_tc_json', side_effect=[(qdisc, {}), (filters, {})]):
            return self.controller.setup()

    def start(self, value=None):
        self.setup_controller()
        with patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(self.controller, '_dump', return_value=(
                    self.dump(), 'c' * 64, {'qdisc_command': {}, 'filter_command': {}})), \
                patch.object(self.controller, 'counters', return_value={
                    'mutation_hit': False, 'affected_packets': 0}):
            return self.controller.start(value or spec())

    def test_setup_installs_nothing_and_reports_no_hit(self):
        health = self.setup_controller()
        self.assertTrue(health['health_ok'])
        self.assertFalse(health['mutation_installed'])
        self.assertFalse(health['hit'])
        self.assertEqual(health['implementation'], 'isolated_netns_tc_netem_discovery_udp')

    def test_setup_rejects_residual_qdisc_or_filter(self):
        cases = (([{'kind': 'prio', 'handle': '1:', 'root': True, 'options': {}}], []),
                 ([{'kind': 'noqueue', 'handle': '0:', 'root': True, 'options': {}}],
                  [{'parent': '1:', 'kind': 'u32'}]))
        for qdisc, filters in cases:
            with self.subTest(qdisc=qdisc, filters=filters), \
                    patch.object(network, '_tc_json',
                                 side_effect=[(qdisc, {}), (filters, {})]), \
                    self.assertRaises(DiscoveryNetworkError):
                network.DiscoveryNetemController(173).setup()

    def test_start_receipt_shape_and_tc_dump(self):
        self.setup_controller()
        with patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(self.controller, '_dump', return_value=(
                    self.dump(), 'a' * 64, {'qdisc_command': {}, 'filter_command': {}})), \
                patch.object(self.controller, 'counters', return_value={
                    'mutation_hit': True, 'affected_packets': 3}):
            begin = self.controller.start(spec())
        for key in ('configured', 'applied', 'implementation', 'spec', 'derivation',
                    'tc_dump_sha256', 'tc_dump', 'neutral'):
            self.assertIn(key, begin)
        self.assertTrue(begin['configured'])
        self.assertTrue(begin['applied'])
        self.assertFalse(begin['neutral'])
        self.assertTrue(begin['hit'])
        self.assertEqual(begin['tc_dump_sha256'], 'a' * 64)
        self.assertEqual(begin['derivation']['floor_default_class'], '1:1')
        self.assertEqual(self.controller.active, True)

    def test_neutral_start_installs_but_is_not_applied_or_hit(self):
        neutral = spec(delay_ms=0, reorder_pct=0, duplicate_pct=0)
        self.setup_controller()
        with patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(self.controller, '_dump', return_value=(
                    self.dump(), 'b' * 64, {'qdisc_command': {}, 'filter_command': {}})), \
                patch.object(self.controller, 'counters', return_value={
                    'mutation_hit': False, 'affected_packets': 2}):
            begin = self.controller.start(neutral)
        self.assertTrue(begin['installed'])
        self.assertTrue(begin['neutral'])
        self.assertFalse(begin['applied'])
        self.assertFalse(begin['hit'])
        self.assertNotIn('hit', json.dumps({'applied': begin['applied']}))

    def test_install_failure_restores_and_keeps_false(self):
        self.setup_controller()
        def execute(argv, **kwargs):
            if '-batch' in argv:
                raise DiscoveryNetworkError('tc failure', {'returncode': 1})
            return {'returncode': 0}
        def tc_json(argv):
            # A failed first line installed nothing, so lo must read back bare.
            if 'qdisc' in argv:
                return [{'kind': 'noqueue', 'handle': '0:', 'root': True, 'options': {}}], {}
            return [], {}
        with patch.object(network, '_execute', side_effect=execute), \
                patch.object(network, '_tc_json', side_effect=tc_json):
            with self.assertRaises(DiscoveryNetworkError) as failure:
                self.controller.start(spec())
        receipt = failure.exception.receipt
        self.assertFalse(receipt['applied'])
        self.assertFalse(receipt['hit'])
        self.assertFalse(receipt['configured'])
        self.assertTrue(receipt['restore_after_failure']['restored'])

    def test_restore_requires_bare_noqueue_root(self):
        self.start()
        controller = self.controller
        with patch.object(controller, 'counters', return_value={'mutation_hit': True}), \
                patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(network, '_tc_json', side_effect=[
                    ([{'kind': 'noqueue', 'handle': '0:', 'root': True, 'options': {}}], {}),
                    ([], {})]):
            end = controller.restore()
        self.assertTrue(end['restored'])
        self.assertTrue(end['only_noqueue_root'])
        self.assertEqual(end['residual_qdisc'], [])
        self.assertEqual(end['residual_filter'], [])

    def test_restore_failure_is_explicit_and_retryable(self):
        self.start()
        controller = self.controller
        with patch.object(controller, 'counters', return_value={'mutation_hit': False}), \
                patch.object(network, '_execute',
                             side_effect=DiscoveryNetworkError('delete failed')), \
                patch.object(network, '_tc_json', side_effect=[
                    ([{'kind': 'prio', 'handle': '1:', 'root': True, 'options': {}}], {}),
                    ([{'parent': '1:', 'kind': 'u32'}], {})]):
            with self.assertRaises(DiscoveryNetworkError) as failure:
                controller.restore()
        self.assertFalse(failure.exception.receipt['restored'])
        self.assertTrue(failure.exception.receipt['residual_qdisc'])
        self.assertTrue(controller.active)
        self.assertTrue(controller._attempted)

    def test_restore_without_attempt_observes_real_residual_state(self):
        self.setup_controller()
        with patch.object(network, '_tc_json', side_effect=[
                ([{'kind': 'noqueue', 'handle': '0:', 'root': True, 'options': {}}], {}),
                ([], {})]):
            first = self.controller.restore()
        self.assertTrue(first['restored'])
        self.assertTrue(first['no_netem_attempted'])
        self.assertEqual(first['residual_qdisc'], [])
        self.assertIsNotNone(first['tc_dump_after_sha256'])
        with patch.object(network, '_tc_json', side_effect=[
                ([{'kind': 'prio', 'handle': '1:', 'root': True, 'options': {}}], {}),
                ([], {})]):
            dirty = network.DiscoveryNetemController(173).restore()
        self.assertFalse(dirty['restored'])
        self.assertTrue(dirty['residual_qdisc'])

    def test_restore_alone_never_confirms_recovery(self):
        self.start()
        controller = self.controller
        with patch.object(controller, 'counters', return_value={'mutation_hit': True}), \
                patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(network, '_tc_json', side_effect=[
                    ([{'kind': 'noqueue', 'handle': '0:', 'root': True, 'options': {}}], {}),
                    ([], {})]):
            end = controller.restore()
        self.assertFalse(end['recovery_verified'])
        evidence = {'domain_id': 173, 'activity_observed': True, 'source': 'public_ros2_pair',
                    'evidence_ref': 'offline:recovery', 'monotonic_ns': network.time.monotonic_ns()}
        for bad in (dict(evidence, monotonic_ns=end['netem_begin_monotonic_ns']),
                    dict(evidence, activity_observed=False),
                    dict(evidence, source='counter_before_restore'),
                    dict(evidence, evidence_ref='   ')):
            with self.assertRaises(DiscoveryNetworkError):
                controller.confirm_recovery(activity_receipt=bad)
        confirmed = controller.confirm_recovery(activity_receipt=evidence)
        self.assertTrue(confirmed['recovery_verified'])
        self.assertFalse(confirmed['s1_execution'])
        self.assertEqual(confirmed['implementation'], 'isolated_netns_tc_netem_discovery_udp')


class NamespacePairWiringTests(unittest.TestCase):
    def discovery_config(self, **overrides):
        config = {'target_id': 'discovery.interruption_recovery', 'carrier': 'wire_proxy',
                  'mutation_kind': 'discovery_drop', 'domain_id': 173}
        config.update(overrides)
        return normalize_discovery_config(normalize_config(config))

    def test_absent_environment_mutation_keeps_legacy_drop(self):
        c = self.discovery_config()
        self.assertIsNone(c['environment_mutation'])
        self.assertEqual(discovery_carrier_kind(c), 'discovery_drop')
        self.assertEqual(c['carrier_implementation'],
                         'isolated_netns_nft_discovery_udp_drop')
        explicit = self.discovery_config(environment_mutation={'kind': 'discovery_drop'})
        self.assertEqual(discovery_carrier_kind(explicit), 'discovery_drop')
        self.assertEqual(explicit['carrier_implementation'],
                         'isolated_netns_nft_discovery_udp_drop')

    def test_netem_kind_selects_netem_implementation(self):
        c = self.discovery_config(environment_mutation={'kind': 'discovery_netem',
                                                       'spec': spec()})
        self.assertEqual(discovery_carrier_kind(c), 'discovery_netem')
        self.assertEqual(c['carrier_implementation'],
                         'isolated_netns_tc_netem_discovery_udp')
        self.assertEqual(c['environment_mutation']['spec']['delay_ms'], 50.0)

    def test_invalid_selection_is_rejected_before_execution(self):
        for bad in ({'kind': 'discovery_drop', 'spec': spec()},
                    {'kind': 'netem'}, {'spec': spec()},
                    {'kind': 'discovery_netem'},
                    {'kind': 'discovery_netem', 'spec': spec(delay_ms=5000)},
                    {'kind': 'discovery_netem', 'spec': spec(), 'extra': 1}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.discovery_config(environment_mutation=bad)
        with self.assertRaises(ValueError):
            self.discovery_config(environment_mutation='discovery_netem')

    def test_netem_with_window_chain_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'environment_windows_not_supported'):
            self.discovery_config(environment_mutation={'kind': 'discovery_netem',
                                                       'spec': spec()},
                                  environment_windows=[{'drop_duration_sec': 0.2}])

    def test_netem_never_claims_drop_hit_in_facts(self):
        from r2f.runtime.namespace_pair import summarize_discovery
        facts = summarize_discovery([], active=True, counter_hit=True, restored=True,
                                    activity_ok=True, fresh_recovery=True,
                                    drop_activity_ok=True,
                                    carrier_implementation='isolated_netns_tc_netem_discovery_udp',
                                    carrier_kind='discovery_netem', neutral_spec=False)
        self.assertFalse(facts['discovery_drop_counter_hit'])
        self.assertTrue(facts['discovery_netem_counter_hit'])
        self.assertIsNone(facts['discovery_drop_applied'])
        self.assertTrue(facts['discovery_carrier_mutation_applied'])
        self.assertTrue(facts['netem_hit_is_not_drop_hit'])

    def test_drop_carrier_facts_still_report_drop_semantics(self):
        from r2f.runtime.namespace_pair import summarize_discovery
        facts = summarize_discovery([], active=True, counter_hit=True, restored=True,
                                    activity_ok=True, fresh_recovery=True,
                                    drop_activity_ok=True)
        self.assertTrue(facts['discovery_drop_counter_hit'])
        self.assertTrue(facts['discovery_drop_applied'])
        self.assertNotIn('discovery_netem_counter_hit', facts)
        self.assertNotIn('netem_hit_is_not_drop_hit', facts)
        self.assertEqual(facts['carrier_implementation'],
                         'isolated_netns_nft_discovery_udp_drop')


class NetemNormalizationTests(unittest.TestCase):
    def test_entries_have_no_nft_and_reuse_port_derivation(self):
        ports = network.DiscoveryPorts(0, 2)
        plan = network.build_netem_plan(ports, spec())
        self.assertIn(str(ports.base), plan['script'])
        self.assertNotIn('iptables', plan['script'])
        self.assertEqual(normalize_environment_mutation(
            {'kind': 'discovery_netem', 'spec': spec()})['kind'], 'discovery_netem')


if __name__ == '__main__':
    unittest.main()
