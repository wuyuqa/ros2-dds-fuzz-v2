"""Sidecar contracts + opt-in real kernel/non-DDS lifecycle tests.

R2F_BOUNDARY_KERNEL_TESTS=1 enables actual namespaces, not ROS contexts.
All locks live below TemporaryDirectory; no production domain lock is touched.
"""
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from r2f.runtime import execution_boundary as boundary


class OfflineContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='r2f-boundary-offline-')
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)

    def test_default_path_is_inert(self):
        with patch.object(boundary, '_source', side_effect=AssertionError('not selected')), patch.object(boundary, '_supervise') as supervise:
            for config in ({}, {'execution_boundary': {'enabled': False}},
                           {'execution_boundary': {'enabled': 1}}, {'execution_boundary': None}):
                result = boundary.run_isolated_runtime(config, self.out)
                self.assertEqual(result['reason'], 'blocked:execution_boundary_not_enabled')
                self.assertFalse(result['real_execution'])
            supervise.assert_not_called()
        self.assertEqual(list(self.out.iterdir()), [])

    def test_config_must_be_dictionary(self):
        self.assertIn('not_object', boundary.run_isolated_runtime([], self.out)['reason'])

    def test_unfrozen_source_rejected_before_locks_or_backend(self):
        with patch.object(boundary, '_HostLeases', side_effect=AssertionError('no lease allowed')):
            for source in (None, {}, {'source_frozen': False}, {'source_frozen': 1}):
                result = boundary.run_isolated_runtime({'execution_boundary': {'enabled': True},
                    'source_manifest': source}, self.out)
                self.assertIn('source_frozen_required', result['reason'])
                self.assertFalse(result['real_execution'])

    def test_source_drift_and_unfreezing_fail_closed(self):
        source = {'source_frozen': True, 'head': 'a' * 40, 'source_repo': str(boundary.REPO),
                  'behavior_tree_sha256': 'b' * 64}
        for field, value in (('head', 'c' * 40), ('source_repo', '/tmp/other'),
                             ('behavior_tree_sha256', 'd' * 64), ('source_frozen', False)):
            with self.subTest(field=field), patch.object(boundary, '_source', return_value=dict(source, **{field: value})):
                with self.assertRaisesRegex(boundary.BoundaryError, 'identity_changed'):
                    boundary._same_source(source)
        with patch.object(boundary, '_source', return_value=source):
            self.assertEqual(boundary._same_source(source), source)

    def config(self):
        from r2f.core import Registry
        from r2f.modules.s1.module import register
        from r2f.core.scenario import compile_scenario
        from r2f.campaign.identity import runtime_config, semantic_input
        from r2f.core.replay import ReplayCapsule
        registry = Registry()
        register(registry)
        target = registry.get_target('discovery.late_endpoint')
        services = registry.get_services(target.target_id)
        scenario = compile_scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0, 19,
            {'mutation_kind': 'late_joiner', 'carrier': 'ros2_endpoint',
             'mutation_parameters': {'reader_delay_sec': .15}})
        prepared = services.mutation_provider.prepare(scenario)
        source = {'source_frozen': True, 'source_repo': str(boundary.REPO), 'head': 'a' * 40,
                  'behavior_tree_sha256': 'b' * 64, 'dirty_behavior_paths': []}
        submitted = semantic_input('mutated')
        capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source, final_input=submitted,
            input_layer_receipts=prepared['input_layer_receipts'])
        return runtime_config(submitted, run_id='offline', campaign_id='offline', target_id=target.target_id,
            target_revision=target.target_revision, expected_target_revision=target.target_revision,
            rmw=scenario.rmw, arm=scenario.arm, repeat=scenario.repeat, seed=scenario.seed,
            mutation_kind='late_joiner', carrier='ros2_endpoint', domain_id=181, scenario=scenario.to_dict(),
            source_repo=str(boundary.REPO), source_manifest=source, expected_head=source['head'],
            effective_input_sha256=capsule.input['effective_input_sha256'], final_input=submitted,
            input_layer_receipts=prepared['input_layer_receipts'], execution_boundary={'enabled': True})

    def test_frozen_formal_config_identity_uses_existing_capsule_and_registry(self):
        config = self.config()
        with patch.object(boundary, '_source', return_value=config['source_manifest']):
            identity = boundary._formal_identity(config)
        self.assertEqual(identity['effective_input_sha256'], config['effective_input_sha256'])
        self.assertEqual(identity['config_sha256'], boundary._hash(config))
        self.assertEqual(len(identity['target_sha256']), 64)
        self.assertEqual(len(identity['dependency_lock_sha256']), 64)

    def test_identity_mismatches_never_reach_supervisor(self):
        original = self.config()
        bad = [('expected_head', 'd' * 40), ('source_repo', '/tmp/wrong'), ('target_revision', 99),
               ('expected_target_revision', 99), ('rmw', 'rmw_fastrtps_cpp'), ('seed', 42),
               ('effective_input_sha256', '0' * 64), ('topic', '/different'),
               ('mutation_parameters', {'reader_delay_sec': 99})]
        for field, value in bad:
            config = copy.deepcopy(original)
            config[field] = value
            with self.subTest(field=field), patch.object(boundary, '_source', return_value=original['source_manifest']), patch.object(boundary, '_supervise') as supervise:
                result = boundary.run_isolated_runtime(config, self.out)
                self.assertEqual(result['runtime_status'], 'blocked')
                supervise.assert_not_called()

    def test_injected_target_schema_is_not_accepted(self):
        config = self.config()
        config['scenario']['target']['intent_and_scope'] += ' injected'
        with patch.object(boundary, '_source', return_value=config['source_manifest']):
            with self.assertRaisesRegex(boundary.BoundaryError, 'registered_target_manifest_mismatch'):
                boundary._formal_identity(config)

    def test_explicit_callable_preserves_backend_config_and_never_patches_scanner(self):
        config = self.config()
        with patch.object(boundary, '_source', return_value=config['source_manifest']), patch.object(boundary, '_supervise', return_value={'sentinel': True}) as supervise:
            self.assertEqual(boundary.run_isolated_runtime(config, self.out), {'sentinel': True})
        self.assertIsNone(supervise.call_args.kwargs['probe'])
        self.assertEqual(supervise.call_args.args[0]['scenario'], config['scenario'])
        self.assertEqual(supervise.call_args.args[0]['effective_input_sha256'], config['effective_input_sha256'])
        self.assertEqual(supervise.call_args.kwargs['submitted_config'], config)

    def test_real_repository_source_check_rejects_without_mocked_freeze(self):
        config = self.config()
        actual = boundary._source()
        config['source_manifest'] = dict(actual, source_frozen=True)
        config['expected_head'] = actual['head']
        if actual['source_frozen']:
            config['source_manifest']['behavior_tree_sha256'] = '0' * 64
        # _source / git / filesystem are real here; only assert no admission.
        with patch.object(boundary, '_supervise') as supervise:
            result = boundary.run_isolated_runtime(config, self.out)
        self.assertIn('source_not_frozen_or_identity_changed', result['reason'])
        self.assertFalse(result['real_execution'])
        supervise.assert_not_called()
        self.assertEqual(list(self.out.iterdir()), [])

    def test_options_reject_fixture_backend_and_scanner_exemptions(self):
        for field in ('backend', 'fixture', 'skip_pids', 'host_clearance', 'whitelist', 'proc_root'):
            with self.subTest(field=field), self.assertRaisesRegex(boundary.BoundaryError, 'unknown_'):
                boundary._options({'enabled': True, field: 'anything'})
        for value in (False, 0, -1, float('nan'), float('inf'), '1', None, 999999):
            with self.subTest(value=value), self.assertRaisesRegex(boundary.BoundaryError, 'timeout'):
                boundary._options({'timeout_sec': value})

    def test_fixture_does_not_admit_arbitrary_code_or_production_lockroot(self):
        for mode in ('ros', 'exec', '/bin/bash'):
            with self.assertRaisesRegex(ValueError, 'invalid_nonDDS'):
                boundary.probe_execution_boundary(self.out, mode=mode)
        with self.assertRaisesRegex(ValueError, 'invalid_probe_rmw'):
            boundary.probe_execution_boundary(self.out, preflight_rmws=('fake',))
        with self.assertRaisesRegex(ValueError, 'invalid_probe_domain'):
            boundary.probe_execution_boundary(self.out, domain_id=True)
        with patch.object(boundary, '_source', return_value={'source_frozen': False}), patch.object(boundary, '_supervise', return_value={}) as supervise:
            boundary.probe_execution_boundary(self.out)
        config = supervise.call_args.args[0]
        self.assertEqual(Path(config['lock_root']), self.out / 'test-only-lockroot')
        self.assertIsNone(supervise.call_args.args[3]['effective_input_sha256'])
        self.assertFalse(supervise.call_args.args[3]['source_frozen_required'])

    def test_nested_pid_proc_is_fixed_nonDDS_mode_only(self):
        with patch.object(boundary, '_source', return_value={'source_frozen': False}), patch.object(boundary, '_supervise', return_value={}) as supervise:
            boundary.probe_execution_boundary(self.out, mode='nested_pid_proc', timeout_sec=8)
        self.assertEqual(supervise.call_args.kwargs['probe']['mode'], 'nested_pid_proc')
        with self.assertRaisesRegex(boundary.BoundaryError, 'unknown_'):
            boundary._options({'enabled': True, 'nested_pid_proc': True})

    def test_kernel_credentials_not_payload_pid(self):
        parent, child = boundary._pair()
        self.addCleanup(parent.close)
        self.addCleanup(child.close)
        boundary._send(child, {'reported_host_pid': 1, 'reported_uid': 0})
        value, credentials, fds = boundary._recv(parent, 1)
        self.assertEqual(value['reported_host_pid'], 1)
        self.assertEqual(credentials, (os.getpid(), os.getuid(), os.getgid()))
        self.assertEqual(fds, [])

    def test_ancillary_pidfd_is_kernel_bound(self):
        parent, child = boundary._pair()
        self.addCleanup(parent.close)
        self.addCleanup(child.close)
        fd = os.pidfd_open(os.getpid())
        self.addCleanup(os.close, fd)
        boundary._send(child, {'kind': 'pidfd'}, [fd])
        _, credentials, received = boundary._recv(parent, 1)
        self.addCleanup(os.close, received[0])
        self.assertEqual(boundary._pidfd_pid(received[0]), credentials[0])
        self.assertFalse(os.get_inheritable(received[0]))
        self.assertFalse(boundary._exited(received[0]))

    def test_non_pidfd_cannot_supply_process_proof(self):
        with open('/dev/null') as null:
            with self.assertRaisesRegex(boundary.BoundaryError, 'kernel_pidfd'):
                boundary._pidfd_pid(null.fileno())

    def test_missing_credentials_and_non_object_control_rejected(self):
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        right.send(b'{}')
        with self.assertRaisesRegex(boundary.BoundaryError, 'credentials'):
            boundary._recv(left, 1)
        left.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        right.send(b'[]')
        with self.assertRaisesRegex(boundary.BoundaryError, 'not_object'):
            boundary._recv(left, 1)

    def test_namespace_fd_type_cannot_be_substituted(self):
        # Isolate the later FD/namespace gate from the runner's UID.
        host = dict(boundary._identity(), uid=1000, cap_eff=0)
        fds = [os.pidfd_open(os.getpid())] + [os.open('/proc/self/ns/' + name, os.O_RDONLY)
                                            for name in boundary.NAMESPACES]
        for fd in fds:
            self.addCleanup(os.close, fd)
        fds[1], fds[2] = fds[2], fds[1]
        with patch.object(boundary, '_identity', return_value=host), \
             self.assertRaisesRegex(boundary.BoundaryError, 'namespace_fd_type_mismatch:user'):
            boundary._authenticate({'reported_inner': host},
                                   (os.getpid(), host['uid'], os.getgid()), fds, host)

    def test_host_namespaces_cannot_be_reported_as_isolated(self):
        # Isolate the later FD/namespace gate from the runner's UID.
        host = dict(boundary._identity(), uid=1000, cap_eff=0)
        fds = [os.pidfd_open(os.getpid())] + [os.open('/proc/self/ns/' + name, os.O_RDONLY) for name in boundary.NAMESPACES]
        for fd in fds:
            self.addCleanup(os.close, fd)
        with patch.object(boundary, '_identity', return_value=host), \
             self.assertRaisesRegex(boundary.BoundaryError, 'namespace_not_independent'):
            boundary._authenticate({'reported_inner': host}, (os.getpid(), host['uid'], os.getgid()), fds, host)
        with self.assertRaisesRegex(boundary.BoundaryError, 'pidfd_mismatch'):
            boundary._authenticate({'reported_inner': host}, (1, host['uid'], os.getgid()), fds, host)
        with self.assertRaisesRegex(boundary.BoundaryError, 'fd_count'):
            boundary._authenticate({}, (os.getpid(), host['uid'], os.getgid()), fds[:1], host)

    def test_host_owner_start_ticks_and_three_inode_receipts(self):
        host = boundary._identity()
        root = self.out / 'locks'
        with boundary._HostLeases(root, self.out / 'state', 'offline', 181, identity=host, boundary_id='offline') as lease:
            self.assertEqual(len(lease.receipts), 3)
            for receipt in lease.receipts:
                owner = json.loads(Path(receipt['path']).read_text())
                self.assertEqual(owner['pid'], os.getpid())
                self.assertEqual(owner['start_ticks'], boundary._ticks(os.getpid()))
                self.assertEqual(owner['lease_scope'], 'host_supervisor')
                self.assertEqual(receipt['inode'], Path(receipt['path']).stat().st_ino)
            boundary._check_inherited_leases({'host_leases': lease.receipts}, [h.fileno() for h in lease.handles])
            wrong = copy.deepcopy(lease.receipts)
            wrong[0]['inode'] += 1
            with self.assertRaisesRegex(boundary.BoundaryError, 'inode_changed'):
                boundary._check_inherited_leases({'host_leases': wrong}, [h.fileno() for h in lease.handles])
        for receipt in lease.receipts:
            self.assertTrue(Path(receipt['path']).exists(), 'never unlink cooperative lock inodes')
            with open(receipt['path']) as contender:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_same_inode_independent_fd_is_not_inherited_locked_ofd(self):
        host = boundary._identity()
        with boundary._HostLeases(self.out / 'locks', self.out / 'state', 'offline', 181,
                                  identity=host, boundary_id='offline') as lease:
            with open(lease.receipts[0]['path']) as independent:
                handles = [independent.fileno()] + [h.fileno() for h in lease.handles[1:]]
                with self.assertRaisesRegex(boundary.BoundaryError, 'not_locked_ofd'):
                    boundary._check_inherited_leases({'host_leases': lease.receipts}, handles)
            # A rejected independent descriptor must neither release nor change owners.
            for receipt in lease.receipts:
                self.assertEqual(json.loads(Path(receipt['path']).read_text()), receipt['owner'])
                with open(receipt['path']) as contender:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_scm_rights_retains_original_lease_ofds_until_last_copy_closed(self):
        host = boundary._identity()
        lease = boundary._HostLeases(self.out / 'locks', self.out / 'state', 'offline', 181,
                                     identity=host, boundary_id='offline')
        left, right = boundary._pair()
        received = []
        try:
            lease.__enter__()
            before = {r['path']: Path(r['path']).read_bytes() for r in lease.receipts}
            boundary._send(left, {'host_leases': lease.receipts}, [h.fileno() for h in lease.handles])
            request, credentials, received = boundary._recv(right, 1)
            self.assertEqual(credentials, (os.getpid(), os.getuid(), os.getgid()))
            # Simulate caller death: close originals, never LOCK_UN the shared OFDs.
            for handle in lease.handles:
                handle.close()
            lease.handles.clear()
            boundary._check_inherited_leases(request, received)
            for receipt in lease.receipts:
                with open(receipt['path']) as contender:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            while received:
                os.close(received.pop())
            for receipt in lease.receipts:
                with open(receipt['path']) as contender:
                    fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(before, {r['path']: Path(r['path']).read_bytes() for r in lease.receipts})
        finally:
            for fd in received:
                os.close(fd)
            lease.__exit__(None, None, None)
            left.close()
            right.close()

    def test_unheld_or_replaced_owner_is_not_proof(self):
        host = boundary._identity()
        lease = boundary._HostLeases(self.out / 'locks', self.out / 'state', 'offline', 181, identity=host, boundary_id='offline')
        with lease:
            receipt = lease.receipts[0]
            owner = json.loads(Path(receipt['path']).read_text())
            Path(receipt['path']).write_text('{}')
            with self.assertRaisesRegex(boundary.BoundaryError, 'owner_changed'):
                boundary._check_inherited_leases({'host_leases': lease.receipts}, [h.fileno() for h in lease.handles])
            Path(receipt['path']).write_text(json.dumps(owner))
            fcntl.flock(lease.handles[0], fcntl.LOCK_UN)
            with self.assertRaisesRegex(boundary.BoundaryError, 'not_held'):
                boundary._check_inherited_leases({'host_leases': lease.receipts}, [h.fileno() for h in lease.handles])

    def test_contender_never_rewrites_existing_owner(self):
        from r2f.runtime.domains import IsolationError
        host = boundary._identity()
        args = (self.out / 'locks', self.out / 'state', 'offline', 181)
        with boundary._HostLeases(*args, identity=host, boundary_id='first') as first:
            before = {r['path']: Path(r['path']).read_bytes() for r in first.receipts}
            with self.assertRaises(IsolationError):
                with boundary._HostLeases(*args, identity=host, boundary_id='second'):
                    self.fail('conflict must not succeed')
            self.assertEqual(before, {r['path']: Path(r['path']).read_bytes() for r in first.receipts})

    def test_full_boundary_refuses_privileged_host_and_existing_output(self):
        source = {'source_frozen': False}
        for uid, cap_eff in ((0, 0), (1001, 0), (1000, 1)):
            with self.subTest(uid=uid, cap_eff=cap_eff), \
                 patch.object(boundary, '_identity', return_value={'uid': uid, 'cap_eff': cap_eff}):
                with self.assertRaisesRegex(boundary.BoundaryError, 'uid1000_cap0'):
                    boundary._supervise({}, self.out, boundary._options({}), source, probe={})
        (self.out / 'runtime_result.json').write_text('{}')
        with self.assertRaisesRegex(boundary.BoundaryError, 'already_contains'):
            boundary._supervise({}, self.out, boundary._options({}), source, probe={})

    def test_disconnected_guardian_quarantines_real_temporary_leases(self):
        config = {'run_id': 'offline', 'campaign_id': 'offline', 'domain_id': 181,
                  'lock_root': str(self.out / 'test-lockroot')}
        fake = unittest.mock.Mock(pid=99999999)
        fake.poll.return_value = 0
        before = len(boundary._QUARANTINED_LEASES)
        # Only the disconnected guardian is under test; retain real lock FDs.
        host = dict(boundary._identity(), uid=1000, cap_eff=0)
        try:
            with patch.object(boundary, '_identity', return_value=host), patch.object(boundary.subprocess, 'Popen', return_value=fake), patch.object(boundary, '_send'), patch.object(boundary, '_recv', side_effect=EOFError('intentional_test_disconnect')):
                result = boundary._supervise(config, self.out, boundary._options({}), {}, probe={'mode': 'success'})
            self.assertEqual(result['runtime_status'], 'blocked')
            self.assertFalse(result['real_execution'])
            self.assertFalse(result['candidate_eligible'])
            self.assertTrue(result['execution_boundary']['nonDDS'])
            self.assertFalse(result['execution_boundary']['maturity_credit'])
            self.assertFalse(result['execution_boundary']['cleanup_confirmed'])
            self.assertEqual(len(boundary._QUARANTINED_LEASES), before + 1)
            lease = boundary._QUARANTINED_LEASES[-1]
            self.assertEqual(len(lease.handles), 3)
            for receipt in lease.receipts:
                with open(receipt['path']) as contender:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            final = json.loads(next(self.out.glob('execution-boundary-*/supervisor-final.json')).read_text())
            self.assertFalse(final['leases_released'])
            self.assertTrue(final['leases_quarantined'])
        finally:
            while len(boundary._QUARANTINED_LEASES) > before:
                # No process was launched in this fake-worker offline test.
                boundary._QUARANTINED_LEASES.pop().__exit__(None, None, None)

    def test_control_transport_total_size_is_bounded(self):
        left, right = boundary._pair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        self.assertEqual(boundary._CONTROL_INLINE_LIMIT, 60000)
        self.assertEqual(boundary._CONTROL_MAX_JSON_BYTES, 16 * 1024 * 1024)
        # The separate socket transport tests prove the inline/envelope boundary.
        with self.assertRaisesRegex(boundary.BoundaryError, 'too_large'):
            boundary._send(left, {'large': 'x' * (16 * 1024 * 1024)})


@unittest.skipUnless(os.environ.get('R2F_BOUNDARY_KERNEL_TESTS') == '1', 'opt-in non-DDS kernel namespace tests')
class KernelNonDDSLifecycle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='r2f-boundary-kernel-')
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name) / 'probe'
        self.before = boundary._identity()
        self.net_before = self.host_net_snapshot()

    def host_net_snapshot(self):
        import struct
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            flags = {name: struct.unpack('16sH', fcntl.ioctl(sock, 0x8913,
                struct.pack('16sH', name.encode(), 0))[:18])[1] for _, name in socket.if_nameindex()}
        return {'interfaces': socket.if_nameindex(), 'flags': flags,
                'route': Path('/proc/net/route').read_text()}

    def manifest(self):
        paths = list(self.out.glob('execution-boundary-*/manifest.json'))
        self.assertEqual(len(paths), 1)
        return json.loads(paths[0].read_text()), paths[0].parent

    def locks_held(self, receipts, held=True):
        for receipt in receipts:
            with open(receipt['path']) as handle:
                if held:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def check_probe_result(self, result):
        self.assertFalse(result['real_execution'])
        self.assertFalse(result['candidate_eligible'])
        self.assertTrue(result['execution_boundary']['nonDDS'])
        self.assertFalse(result['execution_boundary']['maturity_credit'])
        self.assertFalse(result['execution_boundary']['host_clearance_claimed'])
        self.assertIsNone(result['raw_path'])
        self.assertFalse(any(result['mutation_receipt'].values()))

    def check_clean(self, manifest):
        self.assertTrue(manifest['cleanup']['confirmed'], manifest)
        self.assertTrue(manifest['cleanup']['pid1_exited'])
        self.assertTrue(manifest['cleanup']['launcher_exited'])
        self.assertEqual(len(manifest['cleanup']['descendants']), 2)
        for descendant in manifest['cleanup']['descendants']:
            self.assertTrue(descendant['pidfd_exited'])
        self.locks_held(manifest['host_leases'], held=False)
        self.assertEqual(boundary._identity()['namespaces'], self.before['namespaces'])
        self.assertEqual(self.host_net_snapshot(), self.net_before)
        self.assertFalse(manifest['host_clearance_claimed'])
        self.assertTrue(manifest['nonDDS'])
        self.assertFalse(manifest['source_freeze_credit'])

    def test_success_dual_lease_and_deep_tree_cleanup(self):
        result = boundary.probe_execution_boundary(self.out, timeout_sec=12)
        self.check_probe_result(result)
        self.assertEqual(result['runtime_status'], 'nonDDS_probe_complete', result)
        self.assertFalse(result['real_execution'])
        self.assertFalse(result['execution_boundary']['maturity_credit'])
        manifest, audit = self.manifest()
        self.check_clean(manifest)
        inner = manifest['controller_binding']
        self.assertEqual(inner['inner_pid'], 1)
        self.assertNotEqual(inner['host']['pid'], 1)
        self.assertEqual(inner['host']['uid'], 1000)
        self.assertEqual(inner['host']['cap_eff'], 0)
        self.assertTrue(inner['loopback_report']['up'])
        for name in boundary.NAMESPACES:
            self.assertNotEqual(inner['host']['namespaces'][name], self.before['namespaces'][name])
        self.assertEqual(len(manifest['host_leases']), 3)
        self.assertEqual(len(manifest['inner_leases']), 3)
        self.assertTrue(all(x['owner']['pid'] == 1 for x in manifest['inner_leases']))
        self.assertTrue(all(x['owner']['pid'] == os.getpid() for x in manifest['host_leases']))
        self.assertEqual({x['path'] for x in manifest['host_leases']} & {x['path'] for x in manifest['inner_leases']}, set())
        nested = json.loads((audit / 'nonDDS-nested-discovery.json').read_text())
        self.assertEqual(nested['returncode'], 0)
        self.assertIn('net_namespace', json.loads(nested['stdout']))
        raw_identity = json.loads((audit / 'raw-identity.json').read_text())
        self.assertEqual(raw_identity['pid_namespace'], inner['pid_scope'])
        self.assertIsNone(raw_identity['raw_path'])
        self.assertTrue(manifest['owned_process_observations'])
        final = json.loads((audit / 'supervisor-final.json').read_text())
        self.assertTrue(final['leases_released'])
        self.assertLess(manifest['cleanup']['monotonic_ns'], final['monotonic_ns'])
        for name, digest in manifest['artifact_hashes'].items():
            self.assertEqual(hashlib.sha256((audit / name).read_bytes()).hexdigest(), digest)

    def test_timeout_kills_ignoring_descendant_tree_before_unlock(self):
        result = boundary.probe_execution_boundary(self.out, mode='hang', timeout_sec=3)
        self.check_probe_result(result)
        self.assertIn('boundary_runtime_timeout', result['reason'])
        manifest, audit = self.manifest()
        self.check_clean(manifest)
        events = [json.loads(line) for line in (audit / 'guardian-events.jsonl').read_text().splitlines()]
        names = [e['event'] for e in events]
        self.assertIn('owned_pid1_term', names)
        self.assertIn('owned_pid1_kill', names)
        self.assertLess(names.index('cleanup_observed'), names.index('guardian_lease_copies_closing_after_cleanup'))

    def test_namespace_init_crash_cleans_sessions(self):
        result = boundary.probe_execution_boundary(self.out, mode='crash', timeout_sec=12)
        self.check_probe_result(result)
        self.assertIn('failed:73', result['reason'])
        manifest, _ = self.manifest()
        self.check_clean(manifest)

    def test_inner_error_retains_original_error_and_cleanup(self):
        result = boundary.probe_execution_boundary(self.out, mode='controller_error', timeout_sec=12)
        self.check_probe_result(result)
        self.assertIn('failed:127', result['reason'])
        manifest, audit = self.manifest()
        self.check_clean(manifest)
        self.assertIn('intentional_nonDDS_controller_error', (audit / 'inner-failure.json').read_text())

    def start_hang(self):
        # A separate fixture caller is necessary to exercise real parent death.
        process = subprocess.Popen([sys.executable, '-B', str(boundary.REPO / 'scripts/probe_execution_boundary.py'),
            '--output-dir', str(self.out), '--mode', 'hang', '--timeout-sec', '25'],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True, env=boundary._python_env())
        self.addCleanup(self.stop_fixture, process)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            markers = list(self.out.glob('execution-boundary-*/fixture-tree-ready.json'))
            if markers:
                audit = markers[0].parent
                request = json.loads((audit / 'request.json').read_text())
                self.locks_held(request['host_leases'])
                self.locks_held(json.loads((audit / 'fixture-inner-leases-held.json').read_text())['receipts'])
                return process, request, audit
            if process.poll() is not None:
                self.fail('fixture exited before ready: ' + process.stderr.read().decode())
            time.sleep(.02)
        self.fail('nonDDS fixture tree readiness timeout')

    def stop_fixture(self, process):
        if process.poll() is None:
            process.kill()
        process.wait(timeout=15)
        if process.stderr:
            process.stderr.close()
        # No broad kill, PID enumeration, or production process is touched.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            manifests = list(self.out.glob('execution-boundary-*/manifest.json'))
            if manifests or list(self.out.glob('execution-boundary-*/supervisor-rescue.json')):
                return
            time.sleep(.02)

    def wait_cleanup(self, audit):
        deadline = time.monotonic() + 15
        # The guardian creates the file before its JSON write completes.
        # Completion is a readable manifest, not merely a directory entry.
        while True:
            try:
                manifest = json.loads((audit / 'manifest.json').read_text())
                break
            except (FileNotFoundError, json.JSONDecodeError):
                if time.monotonic() > deadline:
                    self.fail('guardian did not record complete cleanup manifest after caller termination')
                time.sleep(.02)
        # Guardian writes manifest before closing its inherited copies.
        while time.monotonic() < deadline:
            try:
                self.locks_held(manifest['host_leases'], held=False)
                break
            except BlockingIOError:
                time.sleep(.01)
        self.check_clean(manifest)
        return manifest

    def test_supervisor_SIGKILL_guardian_keeps_leases_until_pidns_exit(self):
        process, request, audit = self.start_hang()
        owner = request['host_leases'][0]['owner']
        self.assertEqual(owner['pid'], process.pid)
        self.assertEqual(owner['start_ticks'], boundary._ticks(process.pid))
        child_pids = [b['host']['pid'] for b in json.loads((audit / 'fixture-tree-ready.json').read_text())['bindings']]
        pidfds = [os.pidfd_open(pid) for pid in child_pids]
        for fd in pidfds:
            self.addCleanup(os.close, fd)
        process.kill()
        process.wait(timeout=3)
        # At least until the descendants exit, every host lease remains held.
        deadline = time.monotonic() + 15
        while not all(boundary._exited(fd) for fd in pidfds):
            self.assertLess(time.monotonic(), deadline, 'owned descendants did not exit after caller SIGKILL')
            self.locks_held(request['host_leases'])
            time.sleep(.002)
        manifest = self.wait_cleanup(audit)
        self.assertTrue('parent_died' in manifest['error'] or 'peer_closed' in manifest['error'])
        self.assertEqual(json.loads(Path(request['host_leases'][0]['path']).read_text())['pid'], process.pid)

    def test_supervisor_SIGINT_cancels_without_early_unlock(self):
        process, _, audit = self.start_hang()
        process.send_signal(signal.SIGINT)
        process.wait(timeout=15)
        manifest = self.wait_cleanup(audit)
        self.assertIn('KeyboardInterrupt', manifest['error'])
        self.assertTrue(json.loads((audit / 'supervisor-final.json').read_text())['leases_released'])

    def test_supervisor_death_during_gated_startup_still_cleans_before_release(self):
        process = subprocess.Popen([sys.executable, '-B', str(boundary.REPO / 'scripts/probe_execution_boundary.py'),
            '--output-dir', str(self.out), '--mode', 'hang', '--timeout-sec', '25'],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True, env=boundary._python_env())
        self.addCleanup(self.stop_fixture, process)
        deadline = time.monotonic() + 15
        audit = None
        while time.monotonic() < deadline:
            event_paths = list(self.out.glob('execution-boundary-*/guardian-events.jsonl'))
            if event_paths and 'owned_launcher_started' in event_paths[0].read_text():
                audit = event_paths[0].parent
                break
            time.sleep(.002)
        self.assertIsNotNone(audit)
        request = json.loads((audit / 'request.json').read_text())
        self.locks_held(request['host_leases'])
        process.kill()
        process.wait(timeout=3)
        deadline = time.monotonic() + 15
        while not (audit / 'manifest.json').exists():
            self.assertLess(time.monotonic(), deadline)
            self.locks_held(request['host_leases'])
            time.sleep(.01)
        manifest = json.loads((audit / 'manifest.json').read_text())
        self.assertTrue(manifest['cleanup']['confirmed'], manifest)
        while time.monotonic() < deadline:
            try:
                self.locks_held(request['host_leases'], held=False)
                return
            except BlockingIOError:
                time.sleep(.01)
        self.fail('startup death cleanup did not release host leases')

    def test_guardian_SIGKILL_parent_rescues_only_prebound_pidfds(self):
        process, request, audit = self.start_hang()
        events = [json.loads(line) for line in (audit / 'guardian-events.jsonl').read_text().splitlines()]
        guardian = next(e['guardian'] for e in events if e['event'] == 'host_lease_copies_authenticated')
        self.assertEqual(boundary._ticks(guardian['pid']), guardian['start_ticks'])
        fd = os.pidfd_open(guardian['pid'])
        self.addCleanup(os.close, fd)
        descendants = json.loads((audit / 'fixture-tree-ready.json').read_text())['bindings']
        child_fds = [os.pidfd_open(item['host']['pid']) for item in descendants]
        for child_fd in child_fds:
            self.addCleanup(os.close, child_fd)
        boundary._signal_pidfd(fd, signal.SIGKILL)
        deadline = time.monotonic() + 15
        while not all(boundary._exited(child_fd) for child_fd in child_fds):
            self.assertLess(time.monotonic(), deadline, 'owned descendants did not exit after guardian SIGKILL')
            self.locks_held(request['host_leases'])
            time.sleep(.002)
        process.wait(timeout=15)
        rescue = json.loads((audit / 'supervisor-rescue.json').read_text())
        self.assertTrue(rescue['guardian_failed'])
        self.assertTrue(rescue['cleanup']['confirmed'])
        self.assertFalse(rescue['execution_credit'])
        self.assertFalse((audit / 'manifest.json').exists(), 'do not forge dead guardian final receipt')
        self.locks_held(request['host_leases'], held=False)
        final = json.loads((audit / 'supervisor-final.json').read_text())
        self.assertTrue(final['leases_released'])
        result = json.loads((self.out / 'runtime_result.json').read_text())
        self.assertEqual(result['runtime_status'], 'blocked')
        self.assertFalse(result['real_execution'])

    def test_launcher_SIGKILL_pdeathsig_cleans_bound_tree(self):
        process, _, audit = self.start_hang()
        events = [json.loads(line) for line in (audit / 'guardian-events.jsonl').read_text().splitlines()]
        launch = next(e for e in events if e['event'] == 'owned_launcher_started')
        self.assertEqual(boundary._ticks(launch['host_pid']), launch['start_ticks'])
        fd = os.pidfd_open(launch['host_pid'])
        self.addCleanup(os.close, fd)
        boundary._signal_pidfd(fd, signal.SIGKILL)
        process.wait(timeout=15)
        manifest = self.wait_cleanup(audit)
        self.assertIn('failed:-9', manifest['error'])


if __name__ == '__main__':
    unittest.main()
