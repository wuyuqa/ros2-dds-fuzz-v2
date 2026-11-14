"""Offline carrier contracts. Synthetic nft JSON here is not execution evidence."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.runtime import discovery_network as network


class DiscoveryPortsTests(unittest.TestCase):
    def test_rtps_formulas_all_domains_and_safe_participants(self):
        for domain in range(233):
            base = 7400 + 250 * domain
            count = min(120, (65535 - base - 11) // 2 + 1)
            ports = network.DiscoveryPorts(domain, count)
            self.assertEqual(ports.multicast, base)
            self.assertEqual(ports.unicast, tuple(base + 10 + 2 * p for p in range(count)))
            self.assertFalse(set(ports.discovery) & set(ports.user_data))
            self.assertLess(max(ports.discovery), base + 250)
            self.assertLess(max(ports.user_data), 65536)

    def test_high_domain_udp_boundary(self):
        self.assertEqual(max(network.DiscoveryPorts(232, 63).user_data), 65535)
        with self.assertRaises(ValueError):
            network.DiscoveryPorts(232, 64)

    def test_invalid_domains_and_overlapping_participants(self):
        for value in (True, -1, 233, 1.0, '1', None):
            with self.subTest(domain=value), self.assertRaises(ValueError):
                network.DiscoveryPorts(value)
        for value in (True, 0, 121, -1, '32', 2.5):
            with self.subTest(count=value), self.assertRaises(ValueError):
                network.DiscoveryPorts(1, value)

    def test_rule_exact_set_no_ranges_and_no_user_ports(self):
        ports = network.DiscoveryPorts(173)
        table = 'r2f_discovery_0123456789abcdef'
        rules = network.build_drop_rules(ports, table)
        selected = rules.split('udp dport { ')[1].split(' }')[0]
        self.assertEqual(set(map(int, selected.split(', '))), set(ports.discovery))
        self.assertIn('hook input', rules)
        self.assertIn('iifname "lo" meta l4proto udp', rules)
        self.assertIn('counter name discovery_drop drop', rules)
        self.assertNotIn('flush', rules)
        self.assertNotIn('prerouting', rules)
        self.assertNotIn('masquerade', rules)
        with self.assertRaises(ValueError):
            network.build_drop_rules(ports, 'other; flush ruleset')

    def test_wrapper_argv_preserves_literal_arguments(self):
        host = {'host_uid': 1000, 'host_cap_eff': 0, 'host_net_namespace': 'net:[1]',
                'host_user_namespace': 'user:[1]', 'launcher_pid': 9}
        with patch.object(network, '_host_identity', return_value=host), patch.dict(os.environ, {}, clear=True):
            command = network.namespace_command(['/bin/echo', 'a; $(touch sentinel)'], python_executable='/python')
        self.assertEqual(command[:5], ['unshare', '--user', '--map-root-user', '--net', '--'])
        self.assertEqual(command[-3:], ['--', '/bin/echo', 'a; $(touch sentinel)'])
        self.assertEqual(json.loads(command[10]), host)
        for invalid in ('echo x', [], ['echo', 1], ['echo', 'x\0y']):
            with self.assertRaises(ValueError):
                network.namespace_command(invalid)

    def test_privileged_or_nested_launcher_fails(self):
        for identity in ({'host_uid': 0, 'host_cap_eff': 0},
                         {'host_uid': 1000, 'host_cap_eff': 1 << 12}):
            with patch.object(network, '_host_identity', return_value=identity), self.assertRaises(network.NetworkIsolationError):
                network.namespace_command(['/bin/true'])
        with patch.object(network, '_host_identity', return_value={'host_uid': 1000, 'host_cap_eff': 0}), \
                patch.dict(os.environ, {network._PROOF_ENV: '{}'}), self.assertRaises(network.NetworkIsolationError):
            network.namespace_command(['/bin/true'])


class IsolationTests(unittest.TestCase):
    def test_host_missing_proof_never_invokes_network_command(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(network.subprocess, 'run') as run:
            with self.assertRaises(network.NetworkIsolationError):
                network.DiscoveryDropController(173).setup()
            with self.assertRaises(network.NetworkIsolationError):
                network._execute(['nft', 'delete', 'table', 'inet', 'anything'])
            run.assert_not_called()

    def test_forged_host_proof_fails_before_command(self):
        proof = {'schema_version': 1, 'host_uid': 1000, 'host_cap_eff': 0,
                 'host_net_namespace': network._namespace('net'),
                 'host_user_namespace': network._namespace('user'),
                 'net_namespace': network._namespace('net'), 'user_namespace': network._namespace('user')}
        with patch.dict(os.environ, {network._PROOF_ENV: json.dumps(proof)}), \
                patch.object(network.subprocess, 'run') as run:
            with self.assertRaises(network.NetworkIsolationError):
                network._execute(['ip', 'link', 'set', 'lo', 'down'])
            run.assert_not_called()

    def test_changed_namespace_identity_fails(self):
        proof = {'host_uid': 1000, 'host_cap_eff': 0, 'host_net_namespace': 'net:[1]',
                 'host_user_namespace': 'user:[1]', 'net_namespace': 'net:[2]', 'user_namespace': 'user:[2]'}
        with patch.object(network, '_namespace', side_effect=['net:[3]', 'user:[2]']), \
                self.assertRaises(network.NetworkIsolationError):
            network._validate_identity(proof)

    def test_non_loopback_interface_fails(self):
        proof = {'host_uid': 1000, 'host_cap_eff': 0, 'host_net_namespace': 'net:[1]',
                 'host_user_namespace': 'user:[1]', 'net_namespace': 'net:[2]', 'user_namespace': 'user:[2]'}
        with patch.object(network, '_namespace', side_effect=['net:[2]', 'user:[2]']), \
                patch.object(network.os, 'geteuid', return_value=0), \
                patch.object(network.Path, 'read_text', return_value='0 1000 1\n'), \
                patch.object(network.socket, 'if_nameindex', return_value=[(1, 'lo'), (2, 'eth0')]), \
                self.assertRaises(network.NetworkIsolationError):
            network._validate_identity(proof)


class PairBindingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.carrier = network.DiscoveryDropController(173)
        self.pids = {'writer': 1001, 'reader': 1002}
        self.ports = self.carrier.ports.unicast[:2]
        self.rows = []
        for index, (role, pid) in enumerate(self.pids.items()):
            process = self.root / str(pid)
            (process / 'ns').mkdir(parents=True)
            (process / 'fd').mkdir()
            (process / 'ns/net').symlink_to('net:[local]')
            (process / 'ns/user').symlink_to('user:[local]')
            (process / 'stat').write_text(f'{pid} (worker with spaces) S ' + '0 ' * 18 + str(pid))
            inode = str(pid + 10)
            (process / 'fd/3').symlink_to(f'socket:[{inode}]')
            self.rows.append(f'0: 0100007F:{self.ports[index]:04X} 00000000:0000 07 0 0 0 0 0 {inode}')
        (self.root / 'self/net').mkdir(parents=True)
        self.write_udp()
        self.guard = patch.object(network, 'assert_isolated_namespace', return_value={
            'observed_net_namespace': 'net:[local]', 'observed_user_namespace': 'user:[local]'})
        self.guard.start()
        self.addCleanup(self.guard.stop)
        real_path = Path
        def proc_path(value):
            path = real_path(value)
            return self.root / path.relative_to('/proc') if str(path).startswith('/proc/') else path
        self.paths = patch.object(network, 'Path', side_effect=proc_path)
        self.paths.start()
        self.addCleanup(self.paths.stop)

    def write_udp(self):
        (self.root / 'self/net/udp').write_text('header\n' + '\n'.join(self.rows) + '\n')
        (self.root / 'self/net/udp6').write_text('header\n')

    def test_two_workers_own_all_udp_and_each_has_unicast_discovery_binding(self):
        proof = self.carrier.pair_bindings(self.pids)
        self.assertTrue(proof['only_pair_bound_udp_sockets'])
        self.assertEqual(proof['worker_start_ticks'], {'writer': '1001', 'reader': '1002'})
        self.assertEqual({b['port'] for b in proof['bindings']}, set(self.ports))

    def test_foreign_udp_socket_fails_closed(self):
        self.rows.append('1: 0100007F:1234 00000000:0000 07 0 0 0 0 0 9999')
        self.write_udp()
        with self.assertRaises(network.DiscoveryNetworkError) as failure:
            self.carrier.pair_bindings(self.pids)
        self.assertIn('unknown_bindings', failure.exception.receipt)

    def test_multicast_binding_alone_does_not_prove_predictable_unicast(self):
        self.rows[0] = self.rows[0].replace(f'{self.ports[0]:04X}', f'{self.carrier.ports.multicast:04X}')
        self.write_udp()
        with self.assertRaises(network.DiscoveryNetworkError):
            self.carrier.pair_bindings(self.pids)

    def test_worker_in_other_netns_fails_closed(self):
        namespace = self.root / '1001/ns/net'
        namespace.unlink()
        namespace.symlink_to('net:[host]')
        with self.assertRaises(network.DiscoveryNetworkError):
            self.carrier.pair_bindings(self.pids)

    def test_duplicate_or_missing_worker_pids_rejected(self):
        for pids in (None, {}, {'writer': 1001, 'reader': 1001}, {'writer': True, 'reader': 1002}):
            with self.subTest(pids=pids), self.assertRaises(network.DiscoveryNetworkError):
                self.carrier.pair_bindings(pids)


class CarrierTests(unittest.TestCase):
    def setUp(self):
        self.guard = patch.object(network, 'assert_isolated_namespace', return_value={'isolated': True, 'observed_net_namespace': 'net:[test]'})
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.controller = network.DiscoveryDropController(173)
        self.match = {'domain_id': 173, 'match_established': True, 'evidence_ref': 'offline_test:tokens',
                      'writer_gid': '11' * 16, 'reader_gid': '22' * 16,
                      'worker_pids': {'writer': 101, 'reader': 102}}
        self.bindings = patch.object(self.controller, 'pair_bindings', return_value={
            'worker_start_ticks': {'writer': '1', 'reader': '2'}, 'only_pair_bound_udp_sockets': True})
        self.bindings.start()
        self.addCleanup(self.bindings.stop)

    def payload(self, packets=0, byte_count=0):
        return {'nftables': [
            {'table': {'family': 'inet', 'name': self.controller.table}},
            {'counter': {'family': 'inet', 'table': self.controller.table,
                         'name': 'discovery_drop', 'packets': packets, 'bytes': byte_count}},
        ]}

    def setup_controller(self):
        with patch.object(network, '_json_command', return_value=({'nftables': []}, {'returncode': 0})):
            return self.controller.setup()

    def start_controller(self):
        self.setup_controller()
        with patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(self.controller, 'counters', return_value={'hit': False, 'packets': 0, 'bytes': 0}):
            return self.controller.start(match_receipt=self.match)

    def test_start_requires_setup_then_real_match_receipt(self):
        with patch.object(network, '_execute') as execute:
            with self.assertRaises(network.DiscoveryNetworkError):
                self.controller.start(match_receipt=self.match)
            self.setup_controller()
            for receipt in (None, {}, {'domain_id': 173},
                            dict(self.match, domain_id=172), dict(self.match, match_established=False),
                            dict(self.match, evidence_ref=''), dict(self.match, writer_gid=''),
                            dict(self.match, reader_gid=self.match['writer_gid'])):
                with self.subTest(receipt=receipt), self.assertRaises(network.DiscoveryNetworkError):
                    self.controller.start(match_receipt=receipt)
            execute.assert_not_called()
            self.assertFalse(self.controller.active)

    def test_setup_detects_preexisting_rules(self):
        with patch.object(network, '_json_command', return_value=({'nftables': [{'table': {'name': 'foreign'}}]}, {})), \
                self.assertRaises(network.DiscoveryNetworkError):
            self.controller.setup()
        self.assertIsNone(self.controller.health)

    def test_applied_with_zero_counter_is_not_hit(self):
        begin = self.start_controller()
        self.assertTrue(begin['applied'])
        self.assertFalse(begin['hit'])
        self.assertTrue(self.controller.active)
        with patch.object(network, '_json_command', return_value=(self.payload(), {})):
            self.assertFalse(self.controller.counters()['hit'])

    def test_real_counter_value_only_no_fallback_hit(self):
        self.start_controller()
        with patch.object(network, '_json_command', return_value=(self.payload(4, 128), {})):
            receipt = self.controller.counters()
        self.assertEqual((receipt['packets'], receipt['bytes']), (4, 128))
        self.assertTrue(receipt['hit'])
        for payload in ({'nftables': []}, {'nftables': [{'counter': 'malformed'}]},
                        {'nftables': [None]}, self.payload(-1), self.payload(True), self.payload(0, '0')):
            with self.subTest(payload=payload), self.assertRaises(network.DiscoveryNetworkError):
                network.parse_counter(payload, self.controller.table)

    def test_counter_unavailable_does_not_prevent_delete_or_invent_hit(self):
        self.start_controller()
        with patch.object(self.controller, 'counters', side_effect=network.DiscoveryNetworkError('missing counter')), \
                patch.object(network, '_execute', return_value={'returncode': 0}) as execute, \
                patch.object(network, '_json_command', return_value=({'nftables': []}, {})):
            end = self.controller.restore()
        self.assertEqual(execute.call_args.args[0][:3], ['nft', 'delete', 'table'])
        self.assertTrue(end['restored'])
        self.assertFalse(end['hit'])
        self.assertFalse(end['counter_observed'])
        self.assertIn('counter_error', end)

    def test_restore_retains_snapshot_and_is_idempotent(self):
        self.start_controller()
        with patch.object(self.controller, 'counters', return_value={'hit': True, 'packets': 3, 'bytes': 96}), \
                patch.object(network, '_execute', return_value={'returncode': 0}), \
                patch.object(network, '_json_command', return_value=({'nftables': []}, {})):
            end = self.controller.restore()
        self.assertTrue(end['restored'])
        self.assertTrue(end['hit'])
        self.assertTrue(self.controller.restore()['idempotent'])
        self.assertFalse(self.controller.active)

    def test_restore_failure_is_explicit_and_retryable(self):
        self.start_controller()
        with patch.object(self.controller, 'counters', return_value={'hit': False}), \
                patch.object(network, '_execute', side_effect=network.DiscoveryNetworkError('delete failed')), \
                patch.object(network, '_json_command', return_value=(self.payload(), {})):
            with self.assertRaises(network.DiscoveryNetworkError) as failure:
                self.controller.restore()
        self.assertFalse(failure.exception.receipt['restored'])
        self.assertTrue(self.controller.active)
        self.assertTrue(self.controller._attempted)

    def test_install_failure_keeps_applied_and_hit_false_and_attempts_restore(self):
        self.setup_controller()
        with patch.object(network, '_execute', side_effect=network.DiscoveryNetworkError('nft failure', {'returncode': 1})), \
                patch.object(network, '_json_command', return_value=({'nftables': []}, {})):
            with self.assertRaises(network.DiscoveryNetworkError) as failure:
                self.controller.start(match_receipt=self.match)
        receipt = failure.exception.receipt
        self.assertFalse(receipt['applied'])
        self.assertFalse(receipt['hit'])
        self.assertFalse(receipt['counter_observed'])
        self.assertTrue(receipt['restore_after_failure']['restored'])

    def test_non_dds_control_does_not_claim_ros2_match(self):
        self.setup_controller()
        health = {'domain_id': 173, 'healthy': True, 'probe_kind': 'non_dds_loopback_udp', 'evidence_ref': 'unit:health'}
        with patch.object(network, '_execute', return_value={}), \
                patch.object(self.controller, 'counters', return_value={'hit': False}):
            begin = self.controller.start_control(health_receipt=health)
        self.assertFalse(begin['ros2_execution'])
        self.assertEqual(begin['scope'], 'carrier_control_non_dds')
        self.assertNotIn('match_established', begin['precondition'])

    def test_guard_rechecked_on_counter_and_restore(self):
        self.start_controller()
        with patch.object(network, 'assert_isolated_namespace', side_effect=network.NetworkIsolationError('changed')), \
                patch.object(network, '_execute') as execute:
            for call in (self.controller.counters, self.controller.restore):
                with self.assertRaises(network.NetworkIsolationError):
                    call()
            execute.assert_not_called()

    def test_unproven_worker_port_ownership_prevents_drop(self):
        self.setup_controller()
        with patch.object(self.controller, 'pair_bindings', side_effect=network.DiscoveryNetworkError('unowned UDP')), \
                patch.object(network, '_execute') as execute:
            with self.assertRaises(network.DiscoveryNetworkError):
                self.controller.start(match_receipt=self.match)
            execute.assert_not_called()

    def test_restore_alone_never_confirms_recovery(self):
        self.start_controller()
        with patch.object(self.controller, 'counters', return_value={'hit': False}), \
                patch.object(network, '_execute', return_value={}), \
                patch.object(network, '_json_command', return_value=({'nftables': []}, {})):
            end = self.controller.restore()
        self.assertFalse(end['recovery_verified'])
        evidence = {'domain_id': 173, 'activity_observed': True, 'source': 'public_ros2_pair',
                    'evidence_ref': 'offline:recovery', 'monotonic_ns': network.time.monotonic_ns(),
                    'writer_gid': self.match['writer_gid'], 'reader_gid': self.match['reader_gid']}
        for bad in (dict(evidence, monotonic_ns=end['drop_begin_monotonic_ns']),
                    dict(evidence, activity_observed=False), dict(evidence, source='counter_before_restore'),
                    dict(evidence, writer_gid='33' * 16)):
            with self.assertRaises(network.DiscoveryNetworkError):
                self.controller.confirm_recovery(activity_receipt=bad)
        confirmed = self.controller.confirm_recovery(activity_receipt=evidence)
        self.assertTrue(confirmed['recovery_verified'])
        self.assertFalse(confirmed['s1_execution'])
        self.assertEqual(confirmed['evidence_kind'], 'caller_independent_activity')

    def test_invalid_json_is_not_zero_counter(self):
        with patch.object(network, '_execute', return_value={'stdout': 'not JSON'}), \
                self.assertRaises(network.DiscoveryNetworkError):
            network._json_command(['nft', '-j', 'list', 'ruleset'])


if __name__ == '__main__':
    unittest.main()
