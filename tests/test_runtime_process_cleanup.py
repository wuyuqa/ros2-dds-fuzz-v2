"""Non-DDS subprocess cleanup contracts at WorkerPool.start/close."""
import contextlib
import ctypes
import errno
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch



def identity(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    except FileNotFoundError:
        return None
    return {'pid': pid, 'state': fields[0], 'pgid': int(fields[2]),
            'sid': int(fields[3]), 'start_ticks': int(fields[19])}


def record(event, **fields):
    root = os.environ.get('R2F_CLEANUP_TEST_EVIDENCE')
    if root:
        with (Path(root) / 'helper-events.jsonl').open('a') as output:
            output.write(json.dumps({'event': event, **fields}, sort_keys=True) + '\n')


def helper(root, ignore_term, close_stdin=False):
    ready_read, ready_write = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(ready_read)
        if close_stdin:
            os.close(0)  # Keep the group live without holding the command pipe reader.
        if ignore_term:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.write(ready_write, b'1')
        os.close(ready_write)
        time.sleep(30)  # Failsafe; the test's independent finally is the cleanup.
        os._exit(0)
    os.close(ready_write)
    os.read(ready_read, 1)
    os.close(ready_read)
    Path(root, 'ready.tmp').write_text(json.dumps({'leader': identity(os.getpid()),
                                                'child': identity(child)}))
    Path(root, 'ready.tmp').rename(Path(root, 'ready.json'))
    while not Path(root, 'exit').exists():
        time.sleep(.005)
    os._exit(0)


@unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'pidfd_open'), 'Linux pidfds required')
class WorkerCleanup(unittest.TestCase):
    def exercise(self, ignore_term=False, *, deny_signal=False, reused_leader=False,
                 bind_failure=False, unsupported_after_start=False, generations=1, stream_failure=False):
        from r2f.runtime.process import WorkerPool
        libc = ctypes.CDLL(None, use_errno=True)
        previous = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        actual_popen = subprocess.Popen
        actual_pidfd_open = os.pidfd_open
        bound = []
        pool = None
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            helper_roots = []
            def launch(argv, **kwargs):
                helper_root = root / ('helper-' + str(len(helper_roots)))
                helper_root.mkdir()
                helper_roots.append(helper_root)
                # Substitute only the executable transport; production start/close run.
                return actual_popen([sys.executable, '-B', __file__, 'helper', str(helper_root),
                                     str(int(ignore_term)), str(int(stream_failure))], **kwargs)
            try:
                pool = WorkerPool({'domain_id': 0, 'rmw': 'non_dds_fixture',
                                   'source_repo': str(Path.cwd())}, dict(os.environ), root,
                                  lambda event, **kw: record('pool.' + event, **kw))
                def open_pidfd(pid):
                    if bind_failure and pid != os.getpid():
                        raise OSError(errno.EMFILE, 'fixture_target_pidfd_failed')
                    return actual_pidfd_open(pid)
                for generation in range(generations):
                    with patch('r2f.runtime.process.subprocess.Popen', side_effect=launch), \
                         patch('r2f.runtime.process.os.pidfd_open', side_effect=open_pidfd):
                        if bind_failure:
                            with self.assertRaisesRegex(RuntimeError, 'worker_group_pidfd_bind_failed'):
                                pool.start('writer')
                        else:
                            pool.start('writer')
                    helper_root = helper_roots[-1]
                    deadline = time.monotonic() + 5
                    while not (helper_root / 'ready.json').exists() and time.monotonic() < deadline:
                        time.sleep(.005)
                    data = json.loads((helper_root / 'ready.json').read_text())
                    for item in data.values():
                        fd = actual_pidfd_open(item['pid'])
                        self.assertEqual(identity(item['pid'])['start_ticks'], item['start_ticks'])
                        bound.append((item, fd))
                    record('fixture_bound', identities=data, ignore_term=ignore_term, generation=generation)
                    (helper_root / 'exit').touch()
                    self.assertEqual(pool.workers['writer'].wait(timeout=5), 0)
                for item, fd in bound[1::2]:
                    self.assertFalse(select.select([fd], [], [], 0)[0])
                if stream_failure:
                    # Real buffered OS pipe: its reader has exited, so close flushes EPIPE.
                    pool.instances[0]['process'].stdin.write(b'pending stop command\n')
                actual_read = Path.read_text
                actual_signal = signal.pidfd_send_signal
                sent = []
                def read_stat(path, *args, **kwargs):
                    if reused_leader and str(path) == f"/proc/{data['leader']['pid']}/stat":
                        # Model a recycled numeric leader identity at the procfs
                        # boundary; do not create or signal an unrelated task.
                        item = data['leader']
                        fields = ['S', '1', str(item['pgid']), str(item['sid'])] + ['0'] * 15
                        fields.append(str(item['start_ticks'] + 1))
                        return str(item['pid']) + ' (replacement) ' + ' '.join(fields)
                    return actual_read(path, *args, **kwargs)
                denied_fd = None
                def send(fd, sig, *args):
                    nonlocal denied_fd
                    sent.append(int(sig))
                    if deny_signal and (denied_fd is None or fd == denied_fd):
                        denied_fd = fd
                        raise PermissionError('fixture_signal_denied')
                    if unsupported_after_start:
                        raise OSError(errno.EINVAL, 'fixture_group_flag_unavailable')
                    return actual_signal(fd, sig, *args)
                with patch.object(Path, 'read_text', read_stat), \
                     patch('r2f.runtime.process.signal.pidfd_send_signal', side_effect=send), \
                     patch('r2f.runtime.process.os.killpg', side_effect=AssertionError('numeric killpg forbidden')):
                    if stream_failure:
                        with self.assertRaisesRegex(RuntimeError, 'worker_cleanup_unconfirmed') as caught:
                            try:
                                pool.close()
                            except Exception as exc:
                                record('close_error_before_finally', error_type=type(exc).__name__,
                                       error=str(exc), metadata=pool.metadata,
                                       descendant_exited=[bool(select.select([fd], [], [], 0)[0])
                                                          for item, fd in bound[1::2]])
                                raise
                        error = caught.exception
                        record('stream_close_failure', error=str(error), metadata=pool.metadata,
                               descendant_exited=[bool(select.select([fd], [], [], 0)[0])
                                                  for item, fd in bound[1::2]])
                        self.assertIn('stdin', str(error))
                        self.assertIn('Broken pipe', str(error))
                        self.assertFalse(pool.closed)
                        self.assertFalse(pool.metadata[0]['cleanup_confirmed'])
                        self.assertEqual(pool.metadata[0]['returncode'], 0)
                        self.assertEqual(pool.metadata[0]['stdio_cleanup_confirmed'], False)
                        self.assertEqual(pool.metadata[0]['group_cleanup_confirmed'], not deny_signal)
                        for index, (item, fd) in enumerate(bound[1::2]):
                            self.assertEqual(bool(select.select([fd], [], [], 0)[0]),
                                             not (deny_signal and index == 0))
                            if index:
                                self.assertTrue(pool.metadata[index]['cleanup_confirmed'])
                        if deny_signal:
                            self.assertIn('fixture_signal_denied', str(error))
                        # Feed this actual aggregate failure through run_pair's approved
                        # non-DDS hook seam; all three independent lease opens must fail.
                        from test_runtime_bridge_hooks import BridgeHooks
                        from r2f.runtime.domains import IsolationError, LeaseSet
                        from r2f.runtime.execution_boundary import _QUARANTINED_LEASES
                        from unittest.mock import Mock
                        import gc
                        lease_root = root / 'lease'
                        lease_root.mkdir()
                        before = len(_QUARANTINED_LEASES)
                        try:
                            observation = Mock(base_env={}, role_env={})
                            result, _, _ = BridgeHooks().invoke(
                                lease_root, Mock(return_value=observation), close_error=error)
                            gc.collect()
                            self.assertEqual(result['status'], 'failed')
                            self.assertEqual(result['reason'], 'worker_cleanup_error:' + str(error))
                            self.assertEqual(result['cleanup'], {'confirmed': False, 'lease_retained': True})
                            for state, campaign, domain in ((lease_root / 'other', 'hook-offline', 174),
                                                          (lease_root / 'other', 'other', 173),
                                                          (lease_root / 'state', 'other', 174)):
                                with self.assertRaises(IsolationError):
                                    with LeaseSet(lease_root / 'locks', state, campaign, domain):
                                        pass
                            saved = json.loads((lease_root / 'state/runtime_result.json').read_text())
                            self.assertEqual(saved['cleanup'], result['cleanup'])
                            record('stream_close_quarantine', reason=result['reason'],
                                   cleanup=result['cleanup'], all_three_leases_held=True)
                        finally:
                            for lease in _QUARANTINED_LEASES[before:]:
                                lease.__exit__(None, None, None)
                            del _QUARANTINED_LEASES[before:]
                    elif deny_signal or reused_leader or bind_failure or unsupported_after_start:
                        with self.assertRaisesRegex(RuntimeError, 'worker_cleanup_unconfirmed'):
                            pool.close()
                        self.assertFalse(select.select([bound[1][1]], [], [], 0)[0])
                        self.assertFalse(pool.metadata[0]['cleanup_confirmed'])
                        if generations > 1:
                            self.assertTrue(select.select([bound[3][1]], [], [], 0)[0])
                            self.assertTrue(pool.metadata[1]['cleanup_confirmed'])
                        if reused_leader:
                            self.assertEqual([sig for sig in sent if sig], [])
                    else:
                        pool.close()
                        self.assertTrue(select.select([bound[1][1]], [], [], 0)[0],
                                        'close returned with same-PGID descendant alive after leader rc0')
                        for item, fd in bound[1::2]:
                            self.assertTrue(select.select([fd], [], [], 0)[0])
                        pool.close()  # Successful close must be idempotent.
                        with self.assertRaisesRegex(RuntimeError, 'worker_pool_closed'):
                            pool.start('writer')
                        self.assertIn(signal.SIGTERM, sent)
                        if ignore_term:
                            self.assertIn(signal.SIGKILL, sent)
                record('close_observation', sent=sent, ignore_term=ignore_term,
                       deny_signal=deny_signal, reused_leader=reused_leader, bind_failure=bind_failure,
                       unsupported_after_start=unsupported_after_start, generations=generations,
                       child=identity(data['child']['pid']))
            finally:
                for item, fd in bound:
                    with contextlib.suppress(ProcessLookupError):
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                if pool is not None:
                    for process in (instance['process'] for instance in pool.instances):
                        if process.poll() is None:
                            process.kill()
                        process.wait(timeout=5)
                    pool.selector.close()
                    for process in (instance['process'] for instance in pool.instances):
                        for stream in (process.stdin, process.stdout):
                            with contextlib.suppress(OSError):
                                stream.close()
                    for handle in pool.stderr_handles:
                        handle.close()

                for item, fd in bound:
                    self.assertTrue(select.select([fd], [], [], 5)[0], 'finally failed to stop helper')
                    with contextlib.suppress(ChildProcessError):
                        os.waitpid(item['pid'], 0)
                    current = identity(item['pid'])
                    record('finally_cleanup', original=item, current=current, pidfd_exited=True)
                    self.assertIsNone(current, 'fixture identity still present after independent reap')
                    os.close(fd)
                if pool is not None:
                    for instance in pool.instances:
                        fd = instance.get('group_pidfd')
                        if fd is not None:
                            os.close(fd)
                self.assertEqual(libc.prctl(36, previous.value, 0, 0, 0), 0)

    def test_stdin_close_epipe_does_not_skip_later_generation_and_retains_leases(self):
        self.exercise(generations=2, stream_failure=True)

    def test_group_and_stdin_failures_are_aggregated_after_later_generation_cleanup(self):
        self.exercise(generations=2, stream_failure=True, deny_signal=True)

    def test_late_pidfd_bind_failure_keeps_started_instance_for_failed_cleanup(self):
        self.exercise(bind_failure=True)

    def test_group_capability_lost_after_start_does_not_fallback(self):
        self.exercise(unsupported_after_start=True)

    def test_all_same_role_generations_are_cleaned(self):
        self.exercise(generations=2)

    def test_first_group_failure_does_not_skip_later_generation_cleanup(self):
        self.exercise(deny_signal=True, generations=2)

    def test_missing_pidfd_capability_never_starts_worker(self):
        from r2f.runtime.process import WorkerPool
        for boundary in ('pidfd_open', 'pidfd_send_signal'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as tmp:
                pool = WorkerPool({'domain_id': 0, 'rmw': 'non_dds_fixture',
                                   'source_repo': str(Path.cwd())}, {}, tmp, lambda *a, **kw: None)
                error = OSError(errno.ENOSYS if boundary == 'pidfd_open' else errno.EINVAL,
                                'fixture_unsupported')
                target = ('r2f.runtime.process.os.pidfd_open' if boundary == 'pidfd_open'
                          else 'r2f.runtime.process.signal.pidfd_send_signal')
                try:
                    with patch(target, side_effect=error), \
                         patch('r2f.runtime.process.subprocess.Popen',
                               side_effect=RuntimeError('worker_spawn_before_capability_check')) as spawn:
                        with self.assertRaisesRegex(RuntimeError, 'worker_group_pidfd_capability_unavailable'):
                            pool.start('writer')
                        spawn.assert_not_called()
                    self.assertEqual(pool.metadata, [])
                finally:
                    pool.selector.close()
                    for handle in pool.stderr_handles:
                        handle.close()

    def test_close_waits_for_descendant_after_leader_rc0(self):
        self.exercise()

    def test_term_ignoring_descendant_requires_kill_and_exit(self):
        self.exercise(ignore_term=True)

    def test_signal_denial_is_not_successful_cleanup(self):
        self.exercise(deny_signal=True)

    def test_reused_group_leader_identity_is_never_signalled(self):
        self.exercise(reused_leader=True)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'helper':
        helper(sys.argv[2], bool(int(sys.argv[3])),
               bool(int(sys.argv[4])) if len(sys.argv) > 4 else False)
    else:
        unittest.main()
