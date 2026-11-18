"""Offline contracts for explicit host/isolated execution routing.

No test starts ROS, a DDS endpoint, a campaign, or the non-DDS probe.  The
fixture callable is deliberately injected and therefore never formal evidence.
"""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign import runner as campaign_runner
from r2f.campaign import replay as campaign_replay
from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler


class RoutingContracts(unittest.TestCase):
    def setUp(self):
        self.source = {
            'source_repo': '/home/wuyuqa/ros2-dds-fuzz-v2', 'head': 'a' * 40,
            'behavior_tree_sha256': 'b' * 64, 'source_frozen': True,
            'dirty_behavior_paths': [],
        }
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_default_and_explicit_boundaries_resolve_to_exact_callables(self):
        from r2f.runtime.ros2_pair import run_pair
        from r2f.runtime.execution_boundary import run_isolated_runtime
        self.assertIs(campaign_runner.runtime_for_boundary(), run_pair)
        self.assertIs(campaign_runner.runtime_for_boundary('host'), run_pair)
        self.assertIs(campaign_runner.runtime_for_boundary('isolated_netns'),
                      run_isolated_runtime)
        self.assertTrue(campaign_runner._is_formal_runtime(run_pair, 'host'))
        self.assertTrue(campaign_runner._is_formal_runtime(
            run_isolated_runtime, 'isolated_netns'))
        self.assertFalse(campaign_runner._is_formal_runtime(run_pair, 'isolated_netns'))
        with self.assertRaises(ValueError):
            campaign_runner.runtime_for_boundary('fixture')

    def test_isolated_switch_changes_only_execution_copy(self):
        # Existing profile prose is descriptive and is replaced only in the
        # execution config copy; a real final_input remains byte-identical.
        capsule = {'final_input': {'reader_delay_sec': 0.2},
                   'mutation_plan': {'operations': [1]},
                   'input_layer_receipts': {'layer': 'x'},
                   'effective_input_sha256': 'c' * 64}
        config = {'run_id': 'r', 'final_input': deepcopy(capsule['final_input']),
                  'mutation_plan': deepcopy(capsule['mutation_plan']),
                  'input_layer_receipts': deepcopy(capsule['input_layer_receipts']),
                  'effective_input_sha256': capsule['effective_input_sha256'],
                  'execution_boundary': 'profile prose is not a switch'}
        before = deepcopy(config)
        isolated = campaign_runner._boundary_execution_config(config, 'isolated_netns')
        self.assertEqual(isolated['execution_boundary'], {'enabled': True})
        for key in ('final_input', 'mutation_plan', 'input_layer_receipts',
                    'effective_input_sha256', 'run_id'):
            self.assertEqual(isolated[key], before[key])
        self.assertEqual(config, before)
        self.assertIs(campaign_runner._boundary_execution_config(config, 'host'), config)
        config['execution_boundary'] = {'enabled': True, 'timeout_sec': 5}
        self.assertEqual(campaign_runner._boundary_execution_config(
            config, 'isolated_netns')['execution_boundary'],
            {'enabled': True, 'timeout_sec': 5})
        config['execution_boundary'] = {'enabled': True, 'foreign': 1}
        with self.assertRaises(ValueError):
            campaign_runner._boundary_execution_config(config, 'isolated_netns')
        config['execution_boundary'] = {'enabled': False}
        with self.assertRaises(ValueError):
            campaign_runner._boundary_execution_config(config, 'isolated_netns')

    def test_isolated_requires_canonical_runtime_and_injected_is_not_formal(self):
        def injected(config, directory):
            return {'real_execution': False}
        # These are valid, prefix-consistent offline objects. An invalid
        # Corpus would fail before the runtime gate and prove nothing.
        corpus, feedback = Corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=0, warmup=0)
        with patch.object(campaign_runner, '_load_profile') as load_profile:
            with self.assertRaisesRegex(campaign_runner.ExploratoryCycleError,
                                        'requires the canonical execution boundary runtime'):
                campaign_runner.run_exploratory_cycle(
                    self.root / 'x', corpus=corpus, feedback=feedback,
                    scheduler=scheduler, profile={}, execute=True,
                    runtime=injected, runtime_boundary='isolated_netns')
        load_profile.assert_not_called()
        self.assertFalse((self.root / 'x').exists())
        self.assertFalse(campaign_runner._is_formal_runtime(injected, 'host'))

    def test_required_injected_isolated_rejects_before_planning(self):
        def injected(config, directory):
            self.fail('injected callable must never be invoked')
        with patch.object(campaign_runner, 'plan_campaign') as plan:
            with self.assertRaisesRegex(ValueError,
                                        'requires the canonical execution boundary runtime'):
                campaign_runner.run_campaign(self.root / 'required', execute=True,
                    runtime=injected, runtime_boundary='isolated_netns')
        plan.assert_not_called()
        self.assertFalse((self.root / 'required').exists())

    def test_callable_names_modules_and_equality_cannot_grant_formality(self):
        from r2f.runtime.execution_boundary import run_isolated_runtime
        class Impostor:
            __module__ = run_isolated_runtime.__module__
            __name__ = run_isolated_runtime.__name__
            def __call__(self, *args, **kwargs):
                raise AssertionError('must not execute')
            def __eq__(self, other):
                return True
        spoof = Impostor()
        self.assertFalse(campaign_runner._is_formal_runtime(spoof, 'isolated_netns'))
        self.assertIsNone(campaign_runner.runtime_formality(spoof, 'isolated_netns'))

    def test_formal_replay_origins_are_exact_and_injected_stays_informal(self):
        self.assertEqual(campaign_replay.FORMAL_RUNTIMES, {
            campaign_replay.DEFAULT_RUNTIME, campaign_replay.ISOLATED_RUNTIME})
        self.assertNotEqual(campaign_replay.DEFAULT_RUNTIME,
                            campaign_replay.ISOLATED_RUNTIME)
        self.assertFalse(campaign_replay._qualification(
            'injected_runtime', {'run_id': 'r'}, {}, {})['real_runtime_attempt'])
        self.assertEqual(campaign_replay._qualification(
            campaign_replay.ISOLATED_RUNTIME, {'run_id': 'r'}, {}, {})['execution_origin'],
            campaign_replay.ISOLATED_RUNTIME)

    def test_replay_rejects_unknown_boundary_before_reserving_output(self):
        output = self.root / 'replay'
        with self.assertRaises(ValueError):
            campaign_replay.replay_cell(self.root / 'missing-capsule.json',
                                        output_dir=output, domain_id=81,
                                        runtime_boundary='fixture')
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
