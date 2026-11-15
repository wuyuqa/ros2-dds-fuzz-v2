import json
import pathlib
import shutil
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

    @staticmethod
    def _write_proc_state(entry, state='S', start_ticks=700, pid=None):
        pid = int(entry.name) if pid is None else pid
        fields = [state, '1', str(pid)] + ['0'] * 16 + [str(start_ticks)]
        (entry / 'stat').write_text(f'{pid} (offline worker) ' + ' '.join(fields))
        (entry / 'status').write_text(f'Name:\tworker\nState:\t{state}\nPid:\t{pid}\n')

    def _scan_denied_environment(self, transition=None, *, denied_rechecks=(),
                                 initial_stat_denied=False, directory_denied=False,
                                 executable=b'python', comm='worker', during_recheck=None):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            entry = root / '999999'
            entry.mkdir()
            self._write_proc_state(entry)
            (entry / 'cmdline').write_bytes(executable + b'\0/home/old/campaign.py\0--token=ARGV_SECRET\0')
            (entry / 'environ').write_bytes(b'ROS_DOMAIN_ID=170\0PASSWORD=ENV_SECRET\0')
            (entry / 'comm').write_text(comm)
            reads = []
            original_bytes = pathlib.Path.read_bytes
            original_text = pathlib.Path.read_text
            original_stat = pathlib.Path.stat

            def read_bytes(path, *args, **kwargs):
                if path == entry / 'environ':
                    reads.append('environ_denied')
                    if transition:
                        transition(entry)
                    raise PermissionError('fixture environment denied')
                return original_bytes(path, *args, **kwargs)

            def read_text(path, *args, **kwargs):
                if path.parent == entry and ((reads and path.name in denied_rechecks)
                        or (not reads and initial_stat_denied and path.name == 'stat')):
                    raise PermissionError('fixture identity/state denied')
                text = original_text(path, *args, **kwargs)
                if reads and path == entry / 'status' and during_recheck:
                    during_recheck(entry)
                return text

            def stat(path, *args, **kwargs):
                if path == entry and reads and directory_denied:
                    raise PermissionError('fixture process directory denied')
                return original_stat(path, *args, **kwargs)

            with patch.object(pathlib.Path, 'read_bytes', read_bytes), \
                    patch.object(pathlib.Path, 'read_text', read_text), \
                    patch.object(pathlib.Path, 'stat', stat):
                result = scan_processes(pathlib.Path('/home/old'), 170, root, skip_pids=set())
            self.assertEqual(reads, ['environ_denied'])
            self.assertNotIn('ARGV_SECRET', json.dumps(result))
            self.assertNotIn('ENV_SECRET', json.dumps(result))
            return result

    def test_proc_scan_denied_environment_excludes_confirmed_same_zombie(self):
        result = self._scan_denied_environment(lambda entry: self._write_proc_state(entry, 'Z'))
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['unreadable_same_uid_pids'], [])
        self.assertEqual(result['excluded_processes'], [{'pid': 999999, 'state': 'Z', 'start_ticks': 700,
            'reason': 'same_process_zombie_after_environ_denied'}])

    def test_proc_scan_denied_environment_excludes_confirmed_disappearance(self):
        result = self._scan_denied_environment(shutil.rmtree)
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['unreadable_same_uid_pids'], [])
        self.assertEqual(result['excluded_processes'], [{'pid': 999999, 'state': 'gone', 'start_ticks': 700,
            'reason': 'process_disappeared_after_environ_denied'}])

    def test_proc_scan_denied_environment_live_process_still_blocks(self):
        result = self._scan_denied_environment()
        self.assertFalse(result['ok'])
        self.assertEqual(result['unreadable_same_uid_pids'], [999999])
        self.assertEqual(result['excluded_processes'], [])

    def test_proc_scan_denied_environment_unreadable_recheck_still_blocks(self):
        for denied in ('status', 'stat', 'directory'):
            with self.subTest(denied=denied):
                result = self._scan_denied_environment(lambda entry: self._write_proc_state(entry, 'Z'),
                    denied_rechecks=(denied,), directory_denied=denied == 'directory')
                self.assertFalse(result['ok'], result)
                self.assertEqual(result['unreadable_same_uid_pids'], [999999])
                self.assertEqual(result['excluded_processes'], [])

    def test_proc_scan_zombie_recheck_requires_initial_process_identity(self):
        result = self._scan_denied_environment(lambda entry: self._write_proc_state(entry, 'Z'),
                                               initial_stat_denied=True)
        self.assertFalse(result['ok'], result)
        self.assertEqual(result['unreadable_same_uid_pids'], [999999])
        self.assertEqual(result['excluded_processes'], [])

    def test_proc_scan_denied_environment_pid_reuse_or_changed_identity_still_blocks(self):
        def replace_directory(entry):
            entry.rename(entry.with_name('retired-process'))
            entry.mkdir()
            self._write_proc_state(entry, 'Z')

        transitions = {'start_ticks': lambda entry: self._write_proc_state(entry, 'Z', start_ticks=701),
                       'directory': replace_directory,
                       'stat_pid': lambda entry: self._write_proc_state(entry, 'Z', pid=999998)}
        for name, transition in transitions.items():
            with self.subTest(identity=name):
                result = self._scan_denied_environment(transition)
                self.assertFalse(result['ok'], result)
                self.assertEqual(result['unreadable_same_uid_pids'], [999999])
                self.assertEqual(result['excluded_processes'], [])

    def test_proc_scan_pid_reuse_during_exit_recheck_still_blocks(self):
        def replace_directory(entry):
            entry.rename(entry.with_name('retired-process'))
            entry.mkdir()
            self._write_proc_state(entry, 'Z')
        result = self._scan_denied_environment(lambda entry: self._write_proc_state(entry, 'Z'),
                                               during_recheck=replace_directory)
        self.assertFalse(result['ok'], result)
        self.assertEqual(result['unreadable_same_uid_pids'], [999999])
        self.assertEqual(result['excluded_processes'], [])

    def test_proc_scan_denied_environment_incomplete_or_conflicting_exit_evidence_still_blocks(self):
        def transition(entry, case):
            self._write_proc_state(entry, 'Z')
            if case == 'status_live':
                (entry / 'status').write_text('State:\tS\nPid:\t999999\n')
            elif case == 'stat_live':
                text = (entry / 'stat').read_text().replace(') Z ', ') S ')
                (entry / 'stat').write_text(text)
            elif case == 'status_pid':
                (entry / 'status').write_text('State:\tZ\nPid:\t999998\n')
            else:
                (entry / case).unlink()
        for case in ('status_live', 'stat_live', 'status_pid', 'status', 'stat'):
            with self.subTest(evidence=case):
                result = self._scan_denied_environment(lambda entry: transition(entry, case))
                self.assertFalse(result['ok'], result)
                self.assertEqual(result['unreadable_same_uid_pids'], [999999])
                self.assertEqual(result['excluded_processes'], [])

    def test_proc_scan_fixed_os_helper_exceptions_are_not_expanded(self):
        for executable, comm, accepted in ((b'(sd-pam)', '(sd-pam)', True),
                (b'/usr/bin/vmtoolsd', 'vmtoolsd', True), (b'/tmp/vmtoolsd', 'vmtoolsd', False),
                (b'/usr/bin/vmtoolsd-helper', 'vmtoolsd', False), (b'(sd-pam)', 'worker', False)):
            with self.subTest(executable=executable, comm=comm):
                result = self._scan_denied_environment(executable=executable, comm=comm)
                self.assertEqual(result['ok'], accepted, result)
                self.assertEqual(result['unreadable_same_uid_pids'], [] if accepted else [999999])
                if accepted:
                    self.assertEqual(result['excluded_processes'][0]['reason'], 'identified_os_session_helper')

    def test_proc_scan_live_owner_gates_remain_independent(self):
        cases = ((b'python\0/home/old/campaign.py\0', b'', 'old_repo_active_process'),
                 (b'python\0/home/old-v2/worker.py\0', b'ROS_DOMAIN_ID=170\0', 'domain_owner_process'),
                 (b'python\0/home/old-v2/worker.py\0--domain-id\0' + b'170\0', b'', 'domain_owner_argument'))
        for argv, env, reason in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as d:
                root = pathlib.Path(d)
                entry = root / '999999'; entry.mkdir()
                self._write_proc_state(entry)
                (entry / 'cmdline').write_bytes(argv)
                (entry / 'environ').write_bytes(env)
                result = scan_processes(pathlib.Path('/home/old'), 170, root, skip_pids=set())
                self.assertFalse(result['ok'], result)
                self.assertIn(reason, result['conflicts'][0]['reasons'])
                self.assertEqual(result['unreadable_same_uid_pids'], [])

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
