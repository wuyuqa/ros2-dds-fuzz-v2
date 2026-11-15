"""Offline third-target contracts. No ROS contexts, namespace launches or packet captures."""
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from r2f.runtime.domains import scan_processes
from r2f.runtime.namespace_pair import (
    RECOVERY_ACTIVITY_LIMIT, acknowledged_graphs, activity_is_pair_observed,
    normalize_discovery_config, recovery_capture_spec, same_source,
    summarize_discovery, validate_host_leases,
)
from r2f.runtime.ros2_pair import normalize_config, run_pair
from r2f.runtime.worker import extended_graph_qos

CAPTURE_PROOF = {'host_net_namespace': 'net:[1]', 'host_user_namespace': 'user:[2]',
                 'net_namespace': 'net:[3]', 'user_namespace': 'user:[4]',
                 'host_uid': 1000, 'host_cap_eff': 0}


class RuntimeDiscovery(unittest.TestCase):
    def config(self, **overrides):
        return normalize_config({'target_id': 'discovery.interruption_recovery', 'carrier': 'wire_proxy',
                                 'mutation_kind': 'discovery_drop', 'domain_id': 173, **overrides})

    def test_neutral_negative_has_distinct_harness_fault(self):
        c = self.config(arm='negative')
        self.assertEqual(c['fault_id'], 'post_restore_delivery_omission')
        self.assertIsNone(self.config(arm='mutated')['fault_id'])
        self.assertEqual(normalize_config({'arm': 'negative'})['fault_id'], 'late_endpoint_delivery_omission')

    def test_discovery_parameter_validation(self):
        for key in ('drop_duration_sec', 'recovery_timeout_sec', 'counter_interval_sec'):
            for value in (True, 0, -1, float('nan'), 31, '3'):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    normalize_discovery_config(self.config(**{key: value}))
        self.assertTrue(normalize_discovery_config(self.config())['observe_extended_qos'])

    def test_recovery_capture_spec_has_bounded_higher_activity_limit(self):
        with tempfile.TemporaryDirectory() as d:
            first = recovery_capture_spec(output_dir=pathlib.Path(d) / 'observer', run_id='offline-recovery',
                                          domain_id=173, netns_identity=CAPTURE_PROOF)
            second = recovery_capture_spec(output_dir=pathlib.Path(d) / 'observer', run_id='offline-recovery',
                                           domain_id=173, netns_identity=CAPTURE_PROOF)
        self.assertEqual(RECOVERY_ACTIVITY_LIMIT, 32768)
        self.assertEqual(first['limits']['activity_limit'], RECOVERY_ACTIVITY_LIMIT)
        self.assertGreater(first['limits']['activity_limit'], 4096)
        self.assertLessEqual(first['limits']['activity_limit'], 1000000)
        self.assertEqual(first, second)

    def test_recovery_capture_spec_hash_and_bounds_still_apply(self):
        from r2f.instrumentation.sedp import PassiveSedpObserver
        with tempfile.TemporaryDirectory() as d:
            output = pathlib.Path(d) / 'observer'
            spec = recovery_capture_spec(output_dir=output, run_id='offline-recovery', domain_id=173,
                                         netns_identity=CAPTURE_PROOF)
            changed = dict(spec)
            changed['limits'] = dict(spec['limits'], activity_limit=RECOVERY_ACTIVITY_LIMIT + 1)
            with self.assertRaisesRegex(ValueError, 'capture_spec_hash_mismatch'):
                PassiveSedpObserver(changed)
            with self.assertRaisesRegex(ValueError, 'unbounded capture limit'):
                recovery_capture_spec(output_dir=output, run_id='offline-recovery', domain_id=173,
                                      netns_identity=CAPTURE_PROOF, limits={'activity_limit': 1000001})

    def test_host_preflight_failure_never_starts_namespace(self):
        with tempfile.TemporaryDirectory() as d, patch('r2f.runtime.namespace_pair.check_environment',
                return_value={'ok': False, 'reason': 'domain_conflict:test'}), patch('r2f.runtime.namespace_pair._supervise') as launch:
            result = run_pair(self.config(lock_root=d + '/locks'), pathlib.Path(d) / 'state')
            self.assertFalse(result['real_execution'])
            self.assertIsNone(result['execution_metadata'])
            self.assertEqual(result['runtime_status'], 'blocked')
            self.assertEqual(result['reason'], 'domain_conflict:test')
            self.assertTrue(pathlib.Path(result['raw_path']).is_file())
            launch.assert_not_called()

    def test_private_entry_rejects_missing_host_admission(self):
        with patch('r2f.runtime.namespace_pair.assert_isolated_namespace', return_value={'launcher_pid': 17}), \
                patch('r2f.runtime.namespace_pair.os.getppid', return_value=17):
            with self.assertRaisesRegex(RuntimeError, 'host_preflight_missing'):
                validate_host_leases({'host_pid': 17, 'leases': []})

    def test_source_checks_observer_and_runtime_and_head(self):
        base = dict(head='a', runtime_source_sha256='b', observer_source_sha256='c')
        self.assertTrue(same_source(base, dict(base)))
        for key in base:
            self.assertFalse(same_source(base, {**base, key: 'changed'}))

    def test_discovery_missing_activity_is_not_recovery_or_hit(self):
        self.assertFalse(activity_is_pair_observed({}))
        self.assertFalse(activity_is_pair_observed({'activity_observed': None}))
        self.assertFalse(activity_is_pair_observed({'activity_observed': 1}))
        facts = summarize_discovery([], active=True, counter_hit=True, restored=True,
                                    activity_ok=False, fresh_recovery=True, drop_activity_ok=False)
        self.assertEqual(facts['discovery_recovery_status'], 'unknown')
        self.assertFalse(facts['discovery_recovery_activity_observed'])

    def test_activity_requires_both_real_prefixes_and_kernel_packet_refs(self):
        prefixes = ['a' * 24, 'b' * 24]
        activity = {'status': 'observed', 'health': {'status': 'ok'}, 'window_buffer_retained': True,
                    'participant_prefixes': prefixes, 'since_ns': 10, 'until_ns': 30,
                    'events': [{'source_guid_prefix': prefix, 'unix_ns': 20, 'clock': 'kernel_timestampns',
                                'builtin_writer_id': '000100c2', 'kind': 'SPDP_DATA',
                                'raw_ref': {'frame_sha256': 'digest'}} for prefix in prefixes]}
        self.assertTrue(activity_is_pair_observed(activity))
        self.assertFalse(activity_is_pair_observed(activity, after_monotonic_ns=15))
        for event in activity['events']:
            event['received_monotonic_ns'] = 21
        self.assertTrue(activity_is_pair_observed(activity, after_monotonic_ns=15))
        self.assertFalse(activity_is_pair_observed(activity, after_monotonic_ns=21))
        activity['since_ns'] = 20  # a kernel timestamp at the guarded boundary is not post-boundary
        self.assertFalse(activity_is_pair_observed(activity, after_monotonic_ns=15))
        activity['since_ns'] = 10
        activity['events'].pop()
        self.assertFalse(activity_is_pair_observed(activity))
        activity['events'].append({**activity['events'][0], 'source_guid_prefix': prefixes[1], 'clock': 'userspace_fallback'})
        self.assertFalse(activity_is_pair_observed(activity))

    def test_neutral_omission_contract_has_later_sentinel(self):
        events = [{'event': 'delivery_contract', 'expected_sequences': [2, 3, 4]}]
        for seq in (2, 4):
            events.extend([{'event': 'send', 'phase': 'post_restore', 'send_sequence': seq},
                           {'event': 'receive', 'phase': 'post_restore', 'send_sequence': seq}])
        events.append({'event': 'harness_omission', 'phase': 'post_restore', 'send_sequence': 3})
        original = json.dumps(events, sort_keys=True)
        facts = summarize_discovery(events, active=False, counter_hit=False, restored=False,
                                    activity_ok=True, fresh_recovery=True, drop_activity_ok=True)
        self.assertEqual(facts['missing_expected_sequences'], [3])
        self.assertEqual(facts['missing_sequences'], [])
        self.assertTrue(facts['sentinel_received'])
        self.assertFalse(facts['candidate_eligible'])
        self.assertFalse(facts['discovery_drop_applied'])
        self.assertIsNone(facts['discovery_restore_verified'])
        self.assertEqual(json.dumps(events, sort_keys=True), original)

    def test_graph_requires_same_command_unique_ack(self):
        tokens = {'writer': {'endpoint_gid': 'w'}, 'reader': {'endpoint_gid': 'r'}}
        matches = {role: {'command_id': i} for i, role in enumerate(tokens)}
        events = []
        for role, match in matches.items():
            events.append({'event': 'graph_snapshot', 'role': role, **match, 'monotonic_ns': 10,
                           'publishers': [{'endpoint_gid': 'w'}], 'subscriptions': [{'endpoint_gid': 'r'}]})
            events.append({'event': 'command_done', 'role': role, **match, 'monotonic_ns': 11})
        self.assertEqual(set(acknowledged_graphs(events, matches, tokens)), set(tokens))
        with self.assertRaises(RuntimeError):
            acknowledged_graphs(events[:-1], matches, tokens)
        with self.assertRaises(RuntimeError):
            acknowledged_graphs(events + [events[-1]], matches, tokens)
        events[0]['subscriptions'] = [{'endpoint_gid': 'wrong'}]
        with self.assertRaises(RuntimeError):
            acknowledged_graphs(events, matches, tokens)

    def test_qos_extension_opt_in_preserves_first_targets(self):
        q = SimpleNamespace(history=SimpleNamespace(name='KEEP_LAST'), deadline=SimpleNamespace(nanoseconds=12),
                            liveliness=SimpleNamespace(name='AUTOMATIC'), liveliness_lease_duration=SimpleNamespace(nanoseconds=13))
        self.assertEqual(extended_graph_qos(q), {})
        self.assertEqual(extended_graph_qos(q, True), {'history': 'keep_last', 'deadline_ns': 12,
                         'liveliness': 'automatic', 'liveliness_lease_duration_ns': 13})


class RepositoryProcessBoundary(unittest.TestCase):
    def test_new_sibling_is_not_old_repo_but_true_old_paths_still_conflict(self):
        old = pathlib.Path('/home/user/ros2-dds-fuzz')
        cases = [('/home/user/ros2-dds-fuzz-v2/scripts/run_pair.py', False),
                 ('--repo=/home/user/ros2-dds-fuzz-v2', False),
                 ('/home/user/ros2-dds-fuzz/scripts/run_pair.py', True),
                 ('--repo=/home/user/ros2-dds-fuzz', True),
                 ('python /home/user/ros2-dds-fuzz/scripts/campaign.py', True)]
        for arg, expected in cases:
            with self.subTest(arg=arg), tempfile.TemporaryDirectory() as d:
                proc = pathlib.Path(d) / '123'
                proc.mkdir()
                (proc / 'cmdline').write_bytes(b'python3\0' + arg.encode() + b'\0')
                (proc / 'environ').write_bytes(b'PASSWORD=not-emitted\0')
                result = scan_processes(old, proc_root=pathlib.Path(d), skip_pids=set())
                self.assertEqual(bool(result['conflicts']), expected)
                self.assertNotIn('not-emitted', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
