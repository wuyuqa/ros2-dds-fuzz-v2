"""Offline hook lifecycle checks: no endpoints and no execution evidence."""
import gc
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import weakref
from unittest.mock import Mock, patch

from r2f.runtime.domains import IsolationError, LeaseSet
from r2f.runtime.ros2_pair import run_pair


class BridgeHooks(unittest.TestCase):
    def invoke(self, root, prepare, *, close_error=None):
        config = {'run_id': 'hook-offline', 'campaign_id': 'hook-offline',
                  'domain_id': 173, 'lock_root': str(root / 'locks')}
        pool = Mock()
        pool.metadata = []
        pool.close.side_effect = close_error
        # Never synthesize an endpoint_created or an execution record.
        pool.start.side_effect = RuntimeError('offline_worker_not_started')
        with patch('r2f.runtime.ros2_pair.check_environment', return_value={'ok': True}), \
             patch('r2f.runtime.ros2_pair.activated_environment', return_value=({}, {'RAW': 'original'})), \
             patch('r2f.runtime.instrumentation_bridge.prepare', prepare), \
             patch('r2f.runtime.ros2_pair.WorkerPool', return_value=pool) as constructor:
            result = run_pair(config, root / 'state')
        self.assertFalse(result['real_execution'])
        self.assertIsNone(result['execution'])
        self.assertIsNone(result['execution_metadata'])
        return result, constructor, pool

    def test_prepare_failure_does_not_start_unsanitized_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, constructor, pool = self.invoke(Path(tmp), Mock(side_effect=ValueError('bad_observation_profile')))
            constructor.assert_not_called()
            self.assertEqual(result['reason'], 'bad_observation_profile')
            self.assertNotIn('observation', result)

    def test_collect_after_pool_close_and_raw_hash_while_lease_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            context = Mock(base_env={'CLEAN': 'yes'}, role_env={'reader': {'ROLE': 'reader'}})
            prepare = Mock(return_value=context)
            def collect(result):
                data = Path(result['raw_path']).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), result['raw_sha256'])
                self.assertEqual(json.loads(data.splitlines()[-1])['event'], 'runtime_end')
                with self.assertRaises(IsolationError):
                    with LeaseSet(root / 'locks', root / 'other', 'other', 173):
                        pass
                return {'status': 'incomplete', 'verdict': 'unknown', 'intake': []}
            context.collect.side_effect = collect
            result, constructor, pool = self.invoke(root, prepare)
            self.assertEqual(constructor.call_args.args[1], {'CLEAN': 'yes'})
            self.assertEqual(constructor.call_args.kwargs['role_environments'], context.role_env)
            pool.close.assert_called_once()
            context.collect.assert_called_once()
            self.assertEqual(result['reason'], 'offline_worker_not_started')
            self.assertEqual(result['observation']['verdict'], 'unknown')
            with LeaseSet(root / 'locks', root / 'state', 'hook-offline', 173):
                pass

    def test_cleanup_failure_retains_all_leases_after_result_and_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            leases = []
            def acquire(*args, **kwargs):
                lease = LeaseSet(*args, **kwargs)
                leases.append(weakref.ref(lease))
                return lease
            context = Mock(base_env={}, role_env={})
            context.collect.return_value = {'status': 'incomplete', 'verdict': 'unknown', 'intake': []}
            try:
                with patch('r2f.runtime.ros2_pair.LeaseSet', side_effect=acquire):
                    result, _, pool = self.invoke(root, Mock(return_value=context),
                                                  close_error=OSError('offline_cleanup_denied'))
                self.assertEqual(result['reason'], 'worker_cleanup_error:offline_cleanup_denied')
                gc.collect()
                # Check each of campaign/domain/state through independent opens.
                for state, campaign, domain in ((root / 'other', 'hook-offline', 174),
                                                (root / 'other', 'other', 173),
                                                (root / 'state', 'other', 174)):
                    with self.assertRaises(IsolationError):
                        with LeaseSet(root / 'locks', state, campaign, domain):
                            pass
                self.assertTrue(result['cleanup']['lease_retained'])
                self.assertFalse(result['cleanup']['confirmed'])
                saved = json.loads((root / 'state' / 'runtime_result.json').read_text())
                self.assertEqual(saved['cleanup'], result['cleanup'])
            finally:
                # This injected close failure owns no child processes. Explicit
                # test-only release avoids relying on GC or unlocking host files.
                for reference in leases:
                    lease = reference()
                    if lease is not None:
                        lease.__exit__(None, None, None)

    def test_collection_exception_is_observation_unknown_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            context = Mock(base_env={}, role_env={})
            context.collect.side_effect = OSError('unavailable_sidecar')
            result, _, _ = self.invoke(Path(tmp), Mock(return_value=context))
            self.assertEqual(result['reason'], 'offline_worker_not_started')
            self.assertEqual(result['observation']['verdict'], 'unknown')
            self.assertEqual(result['observation']['intake'], [])
            self.assertEqual(result['observation']['hook_diagnostics'], [{'hook': 'collect', 'reason': 'OSError'}])


if __name__ == '__main__':
    unittest.main()
