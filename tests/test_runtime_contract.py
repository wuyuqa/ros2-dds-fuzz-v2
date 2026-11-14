import json
import pathlib
import socket
import tempfile
import unittest
from unittest.mock import patch

from r2f.runtime.domains import IsolationError, LeaseSet, probe_rtps_ports, scan_processes
from r2f.runtime.ros2_pair import normalize_config, run_pair, summarize_events, runtime_capabilities


class RuntimeContract(unittest.TestCase):
    def test_static_network_implementation_is_not_live_admission(self):
        with patch('r2f.runtime.ros2_pair.shutil.which', return_value='/tool'):
            capability = runtime_capabilities()['discovery_network']
        self.assertTrue(capability['implemented'])
        self.assertIsNone(capability['available'])
        self.assertFalse(capability['admitted'])
        self.assertTrue(capability['admission_requires_live_preflight'])
        self.assertTrue(capability['dependency_check']['available'])
        self.assertFalse(capability['execution_evidence'])

    def test_missing_network_tool_keeps_implementation_but_not_availability(self):
        with patch('r2f.runtime.ros2_pair.shutil.which', return_value=None):
            capability = runtime_capabilities()['discovery_network']
        self.assertTrue(capability['implemented'])
        self.assertFalse(capability['available'])
        self.assertEqual(capability['availability_status'], 'missing_dependency')

    def test_default_contract_is_volatile(self):
        c = normalize_config({'domain_id': 173, 'run_id': 'offline'})
        self.assertEqual(c['qos']['durability'], 'volatile')
        self.assertEqual(c['rmw'], 'rmw_cyclonedds_cpp')
        self.assertGreater(c['post_match_count'], 1)

    def test_invalid_domain_and_bad_fault(self):
        for cfg in ({'domain_id': 232}, {'domain_id': True}, {'repeat': True}, {'target_revision': True}, {'domain_id': 233}, {'fault_id': 'invented'}, {'arm': 'baseline', 'fault_id': 'late_endpoint_delivery_omission'}):
            with self.subTest(cfg=cfg), self.assertRaises(ValueError):
                normalize_config(cfg)

    def test_conflicting_leases_do_not_truncate_owner(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            a = LeaseSet(root, root / 'state', 'a', 172)
            with a:
                before = (root / 'domain-172.lock').read_bytes()
                with self.assertRaises(IsolationError):
                    with LeaseSet(root, root / 'other', 'b', 172):
                        pass
                self.assertEqual(before, (root / 'domain-172.lock').read_bytes())
            with LeaseSet(root, root / 'state', 'a', 172):
                pass

    def test_rtps_conflict(self):
        port = 7400 + 250 * 171
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(('0.0.0.0', port))
            result = probe_rtps_ports(171)
        self.assertFalse(result['ok'])
        self.assertIn(port, result['busy_ports'])

    def test_proc_scan_redacts_cmdline(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            p = root / '999999'
            p.mkdir()
            (p / 'cmdline').write_bytes(b'python\x00/home/old/scripts/campaign.py\x00--token=SECRET\x00')
            (p / 'environ').write_bytes(b'ROS_DOMAIN_ID=170\x00PASSWORD=SECRET\x00')
            data = scan_processes(pathlib.Path('/home/old'), 170, root, skip_pids=set())
            self.assertTrue(data['conflicts'])
            self.assertNotIn('SECRET', json.dumps(data))

    def test_prejoin_not_missing_and_omission_is_harness(self):
        events = [
            {'event': 'send', 'phase': 'prejoin', 'send_sequence': 0},
            {'event': 'send', 'phase': 'post_match', 'send_sequence': 1},
            {'event': 'receive', 'phase': 'post_match', 'send_sequence': 1, 'receive_sequence': 1},
            {'event': 'harness_omission', 'phase': 'post_match', 'send_sequence': 2},
        ]
        facts = summarize_events(events)
        self.assertEqual(facts['missing_sequences'], [])
        self.assertEqual(facts['harness_omitted_sequences'], [2])
        self.assertEqual(facts['missing_expected_sequences'], [2])
        self.assertEqual(facts['fault_control_primary_expected'], 'fail')
        self.assertFalse(facts['candidate_eligible'])
        self.assertEqual(facts['prejoin_sent_sequences'], [0])

    def test_offline_fixture_rejected_without_execution(self):
        with tempfile.TemporaryDirectory() as d:
            r = run_pair({'fixture': {'events': []}}, pathlib.Path(d))
            self.assertFalse(r['real_execution'])
            self.assertIsNone(r['execution'])
            self.assertNotEqual(r['runtime_status'], 'completed')

    def test_unsupported_carriers_are_named_blocked(self):
        for target in ('discovery.interruption_recovery', 'builtin.discovery_projection_reconciliation'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as d:
                r = run_pair({'target_id': target}, pathlib.Path(d))
                self.assertEqual(r['runtime_status'], 'blocked')
                self.assertFalse(r['real_execution'])
                self.assertIsNone(r['execution'])
                self.assertFalse(r['mutation_receipt']['hit'])

    def test_baseline_transparent_do_not_claim_mutation(self):
        for arm in ('baseline', 'transparent'):
            with self.subTest(arm=arm), tempfile.TemporaryDirectory() as d:
                with patch('r2f.runtime.ros2_pair.check_environment', return_value={'ok': False, 'reason': 'test_unavailable'}):
                    r = run_pair({'arm': arm}, pathlib.Path(d))
                self.assertFalse(r['mutation_receipt']['hit'])
                self.assertFalse(r['real_execution'])
                self.assertIsNone(r['execution'])


if __name__ == '__main__':
    unittest.main()
