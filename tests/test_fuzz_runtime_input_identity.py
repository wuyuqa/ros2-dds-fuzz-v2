"""Offline regressions for submitted controls, ordered windows and freeze gates.

No ROS, namespaces, workers, real campaigns or mutation execution are started.
Injected runtime failures below are explicitly fixtures, not SUT evidence.
"""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from r2f.campaign.corpus import Corpus
from r2f.campaign.exploratory_input import apply_profile_controls, runtime_input
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import (
    _compose_recovery_inputs, _recovery_windows, run_exploratory_cycle,
)
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core import Registry
from r2f.core.evidence import INPUT_SCHEMA_V2, canonical_hash, effective_input_hash
from r2f.core.mutation_plan import compose
from r2f.core.mutators import make_rng, mutate
from r2f.core.replay import ReplayCapsule
from r2f.core.scenario import Scenario
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import register_exploratory

CYCLONE = 'rmw_cyclonedds_cpp'
FAST = 'rmw_fastrtps_cpp'
RECOVERY = 'discovery.interruption_recovery.fuzz_v1'
PROJECTION = 'builtin.discovery_projection_reconciliation.fuzz_v1'
LATE = 'discovery.late_endpoint.fuzz_v1'
SOURCE = {'source_repo': '/offline/fixture', 'head': 'offline-head',
          'behavior_tree_sha256': 'a' * 64, 'source_frozen': True,
          'dirty_behavior_paths': []}


def window(index, duration=0.2):
    return {'window_id': 'window-%d' % index, 'window_index': index,
            'trigger_anchor': 'fresh_match' if index == 0 else 'previous_restore_confirmed',
            'trigger_offset_sec': index * 0.01, 'drop_duration_sec': duration,
            'restore_interval_sec': index * 0.02}


def netem(delay=10):
    return {'kind': 'discovery_netem', 'spec': {
        'delay_ms': delay, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 3}}


class RuntimeInputIdentity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.reg = Registry()
        register_exploratory(self.reg, include_required=False)
        self.index = 0
        self.source = deepcopy(SOURCE)
        self.source_patch = patch('r2f.campaign.runner.source_manifest',
                                  side_effect=lambda repo: deepcopy(self.source))
        self.source_patch.start()
        self.addCleanup(self.source_patch.stop)

    def parent(self, surface='E01'):
        return deepcopy(fuzz_catalog.load_seeds()['seeds'][surface]['logical_config'])

    def plan(self, controls=None, *, target=RECOVERY, surface='E01', parent=None,
             domain=171, run_id='offline', execute=False, runtime=None, **profile_extra):
        self.index += 1
        contract = self.reg.get_target(target).exploration_contract
        spec = contract.surface_by_id[surface]
        corpus = Corpus()
        corpus.add_seed({'seed_id': surface, 'target_id': target, 'target_revision': 0,
                         'rmw': CYCLONE, 'contract_hash': contract.contract_hash(),
                         'surface_id': surface, 'boundary_class': spec.boundary_classes[0],
                         'mutator_id': spec.mutator_ids[0], 'executable': True,
                         'final_config': deepcopy(parent if parent is not None else self.parent(surface))})
        profile = {'profile_id': 'offline-control-identity', 'campaign_kind': 'exploratory',
                   'required_catalog_included': False, 'target_ids': [target],
                   'surfaces': (list(contract.surface_by_id) if profile_extra.get('chain_length', 1) > 1
                                else [surface]),
                   'rmws': [CYCLONE], **(controls or {}), **profile_extra}
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0,
                                         sampling_kinds=profile.get('sampling_kinds'))
        directory = self.root / str(self.index)
        result = run_exploratory_cycle(
            directory, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=profile, execute=execute, runtime=runtime, run_id=run_id, domain_id=domain)
        if not result['rows']:
            return result, None, None
        row = result['rows'][0]
        config = json.loads(Path(row['runtime_config_path']).read_text())
        capsule = json.loads(Path(row['replay_path']).read_text())
        return result, config, capsule

    def assert_bound(self, result, config, capsule):
        row = result['rows'][0]
        self.assertEqual(row['binding']['status'], 'preserved', row['binding'])
        final = config['final_input']
        plan = config['mutation_plan']
        self.assertEqual(final, plan['final_config'])
        self.assertEqual(final, capsule['input']['effective_input']['final_input'])
        self.assertEqual(capsule, ReplayCapsule.from_dict(capsule).to_dict())
        self.assertEqual(plan['operations'][-1]['input_after_sha256'], canonical_hash(final))
        self.assertEqual(plan['parent_content_sha256'], plan['operations'][0]['input_before_sha256'])
        for previous, current in zip(plan['operations'], plan['operations'][1:]):
            self.assertEqual(previous['input_after_sha256'], current['input_before_sha256'])
        digest = effective_input_hash(capsule['input']['effective_input'], schema=INPUT_SCHEMA_V2)
        self.assertEqual(digest, row['effective_input_sha256'])
        self.assertEqual(digest, config['effective_input_sha256'])
        self.assertEqual(digest, capsule['input']['effective_input_sha256'])
        backend = config['runtime_route']['backend']
        rebuilt = runtime_input(final, backend=backend)
        for key, value in final.items():
            self.assertEqual(config[key], value, key)
            self.assertEqual(rebuilt[key], value, key)
        return digest

    def assert_runtime_tamper_rejected(self, config, capsule, key, replacement):
        parsed = ReplayCapsule.from_dict(capsule)
        scenario = Scenario.from_dict(parsed.input['scenario'])
        changed = deepcopy(config)
        changed[key] = deepcopy(replacement)
        runtime = Mock(side_effect=AssertionError('fixture must not be called'))
        with self.assertRaisesRegex(ValueError, 'carrier final input mismatch:' + key):
            self.reg.get_services(config['target_id']).carrier.apply(
                scenario, parsed.input['effective_input'],
                execution={'config': changed, 'output_dir': self.root, 'runtime': runtime})
        runtime.assert_not_called()

    def test_each_behavior_control_changes_hash_and_roundtrips_from_capsule(self):
        cases = [
            ('reader_recreate_in_window', {}, False, True),
            ('reader_recreate_delay_sec', {'reader_recreate_in_window': True}, 0.01, 0.02),
            ('reader_recreate_at_fraction', {'reader_recreate_in_window': True}, 0.1, 0.2),
            ('reader_recreate_count', {'reader_recreate_in_window': True}, 1, 2),
            ('reader_recreate_interval_sec', {'reader_recreate_in_window': True,
                                             'reader_recreate_count': 2}, 0.01, 0.02),
            ('reader_recreate_fractions', {'reader_recreate_in_window': True,
                                          'reader_recreate_count': 2}, [0.1, 0.3], [0.2, 0.4]),
            ('reader_destroy_fractions', {'reader_recreate_in_window': True,
                                         'reader_recreate_fractions': [0.1]}, [0.8], [0.9]),
            ('writer_churn_in_window', {}, False, True),
            ('publish_burst_in_window', {}, False, True),
            ('publish_burst_count', {'publish_burst_in_window': True}, 16, 17),
            ('publish_burst_interval_sec', {'publish_burst_in_window': True}, 0.01, 0.02),
            ('environment_mutation', {}, {'kind': 'discovery_drop'}, netem()),
            ('netem_covers_delivery', {'environment_mutation': netem()}, False, True),
            ('role_rmws', {}, {'writer': CYCLONE, 'reader': CYCLONE},
             {'writer': CYCLONE, 'reader': FAST}),
        ]
        for field, base, first, second in cases:
            with self.subTest(field=field):
                a = self.plan(dict(base, **{field: first}))
                b = self.plan(dict(base, **{field: second}))
                self.assertNotEqual(self.assert_bound(*a), self.assert_bound(*b))
                key = 'rmws' if field == 'role_rmws' else field
                self.assertIn(key, a[1]['final_input'])
                self.assertNotIn('role_rmws', a[1]['final_input'])
                self.assert_runtime_tamper_rejected(a[1], a[2], key, b[1]['final_input'][key])

    def test_nested_environment_spec_is_input_not_runtime_override(self):
        a = self.plan({'environment_mutation': netem(10)})
        b = self.plan({'environment_mutation': netem(20)})
        self.assertNotEqual(self.assert_bound(*a), self.assert_bound(*b))
        self.assertEqual(a[1]['final_input']['environment_mutation'],
                         a[1]['environment_mutation'])

    def test_projection_controls_and_role_qos_roundtrip(self):
        cases = [('projection_roles', ['writer', 'reader'], ['writer', 'reader', 'reader2']),
                 ('projection_role_qos', {'reader': {'depth': 16}}, {'reader': {'depth': 32}})]
        for field, first, second in cases:
            with self.subTest(field=field):
                a = self.plan({field: first}, target=PROJECTION, surface='P04')
                b = self.plan({field: second}, target=PROJECTION, surface='P04')
                self.assertNotEqual(self.assert_bound(*a), self.assert_bound(*b))
                self.assert_runtime_tamper_rejected(a[1], a[2], field, second)

    def test_domain_run_id_profile_provenance_do_not_change_input_identity(self):
        controls = {'reader_recreate_in_window': True, 'writer_churn_in_window': True,
                    'role_rmws': {'writer': CYCLONE, 'reader': FAST}}
        a = self.plan(controls, domain=171, run_id='first', description='before')
        b = self.plan(controls, domain=172, run_id='second', description='after')
        self.assertEqual(self.assert_bound(*a), self.assert_bound(*b))
        self.assertNotEqual(a[2]['input']['origin_domain'], b[2]['input']['origin_domain'])
        effective = deepcopy(a[2]['input']['effective_input'])
        effective.update(domain=231, domain_id=231, campaign_kind='required', timestamp='later')
        self.assertEqual(effective_input_hash(effective, schema=INPUT_SCHEMA_V2),
                         a[1]['effective_input_sha256'])

    def test_tampered_capsule_and_top_level_runtime_controls_are_rejected(self):
        result, config, capsule = self.plan({'writer_churn_in_window': True})
        broken = deepcopy(capsule)
        broken['input']['effective_input']['final_input']['writer_churn_in_window'] = False
        with self.assertRaises(ValueError):
            ReplayCapsule.from_dict(broken)
        parsed = ReplayCapsule.from_dict(capsule)
        scenario = Scenario.from_dict(parsed.input['scenario'])
        broken_config = deepcopy(config)
        broken_config['writer_churn_in_window'] = False
        runtime = Mock(side_effect=AssertionError('fixture must not be called'))
        with self.assertRaisesRegex(ValueError, 'carrier final input mismatch:writer_churn_in_window'):
            self.reg.get_services(RECOVERY).carrier.apply(
                scenario, parsed.input['effective_input'],
                execution={'config': broken_config, 'output_dir': self.root, 'runtime': runtime})
        runtime.assert_not_called()

    def test_two_three_window_parent_and_mutated_duration_are_single_source(self):
        for count in (2, 3):
            with self.subTest(count=count):
                parent = self.parent()
                parent['environment_windows'] = [window(i, 0.11 + i * 0.1) for i in range(count)]
                result, config, capsule = self.plan(parent=parent)
                self.assert_bound(result, config, capsule)
                windows = config['environment_windows']
                self.assertEqual(len(windows), count)
                self.assertNotEqual(config['drop_duration_sec'], parent['drop_duration_sec'])
                self.assertEqual([w['drop_duration_sec'] for w in windows],
                                 [config['drop_duration_sec']] * count)
                self.assertEqual(canonical_hash(config['runtime_windows']), canonical_hash(windows))
                self.assertEqual([{k: v for k, v in w.items() if k != 'schema'}
                                  for w in config['mutation_plan']['runtime_windows']], windows)
                for index, observed in enumerate(windows):
                    for field in ('window_id', 'trigger_anchor', 'trigger_offset_sec', 'restore_interval_sec'):
                        self.assertEqual(observed[field], parent['environment_windows'][index][field])

    def test_multwindow_order_and_per_window_values_change_identity(self):
        parent = self.parent()
        parent['environment_windows'] = [window(i) for i in range(3)]
        first = self.plan(parent=parent)
        # Reorder identity/value pairs while retaining valid positional anchors.
        changed = deepcopy(parent)
        for position, source in enumerate(reversed(parent['environment_windows'])):
            changed['environment_windows'][position].update(
                window_id=source['window_id'], restore_interval_sec=source['restore_interval_sec'])
        second = self.plan(parent=changed)
        self.assertNotEqual(self.assert_bound(*first), self.assert_bound(*second))
        self.assertEqual([w['window_id'] for w in second[1]['environment_windows']],
                         ['window-2', 'window-1', 'window-0'])

    def test_declarative_parent_count_materializes_before_compose(self):
        parent = self.parent()
        parent['environment_windows'] = {'count': 3, 'restore_interval_sec': 0.2}
        result, config, capsule = self.plan(parent=parent)
        self.assert_bound(result, config, capsule)
        self.assertEqual(len(config['environment_windows']), 3)
        self.assertEqual([w['restore_interval_sec'] for w in config['environment_windows']], [0.2] * 3)

    def test_non_duration_mutation_preserves_distinct_window_durations(self):
        from dataclasses import replace
        parent = self.parent()
        parent['environment_windows'] = [window(0, 0.1), window(1, 0.4)]
        contract = self.reg.get_target(RECOVERY).exploration_contract
        proposal = mutate(parent, contract.surface_by_id['E01'], make_rng(7), mutator_id='M03')
        # Synthetic no-op only tests the projection; it is not run evidence.
        proposal = replace(proposal, changes={}, input_after=deepcopy(parent))
        proposals, core, runtime, _ = _compose_recovery_inputs(parent, [proposal])
        plan = compose(parent, proposals, contract, runtime_windows=core)
        self.assertEqual([w['drop_duration_sec'] for w in runtime], [0.1, 0.4])
        self.assertEqual(plan.final_config['environment_windows'], runtime)
        self.assertEqual(plan.operations[-1].input_after_sha256, canonical_hash(plan.final_config))

    def test_unknown_null_empty_invalid_and_inapplicable_controls_fail_closed(self):
        bad = [
            {'reader_recreate_typo': True}, {'role_rmws': {}}, {'role_rmws': {'reader': 'unknown'}},
            {'role_rmws': []}, {'reader_recreate_in_window': 1},
            {'reader_recreate_in_window': None}, {'reader_recreate_count': 2},
            {'reader_recreate_in_window': True, 'reader_recreate_count': 0},
            {'reader_recreate_in_window': True, 'reader_recreate_fractions': []},
            {'reader_recreate_in_window': True, 'reader_recreate_fractions': [0.2, 0.1]},
            {'reader_recreate_in_window': True, 'reader_recreate_delay_sec': float('nan')},
            {'reader_recreate_in_window': True, 'reader_recreate_at_fraction': 1.0},
            {'reader_recreate_in_window': True, 'reader_recreate_fractions': [0.2],
             'reader_recreate_at_fraction': 0.1},
            {'reader_recreate_in_window': True, 'reader_destroy_fractions': [0.0]},
            {'publish_burst_count': 5}, {'publish_burst_in_window': 'yes'},
            {'publish_burst_in_window': True, 'publish_burst_count': True},
            {'publish_burst_in_window': True, 'publish_burst_interval_sec': -1},
            {'environment_mutation': {}}, {'environment_mutation': None},
            {'environment_mutation': {'kind': 'discovery_netem', 'spec': {'typo': 1}}},
            {'netem_covers_delivery': True}, {'netem_covers_delivery': 'yes'},
            {'netem_covers_delivery': True, 'environment_mutation': None},
        ]
        for controls in bad:
            with self.subTest(controls=controls), self.assertRaises(ValueError):
                self.plan(controls)
        for controls in ({'projection_roles': []}, {'projection_role_qos': {}},
                         {'projection_roles': ['writer', 'reader'],
                          'projection_role_qos': {'reader': {'typo': 1}}}):
            with self.subTest(controls=controls), self.assertRaises(ValueError):
                self.plan(controls, target=PROJECTION, surface='P04')
        with self.assertRaisesRegex(ValueError, 'unsupported controls'):
            self.plan({'writer_churn_in_window': True}, target=LATE, surface='P01')

    def test_null_environment_with_delivery_coverage_is_a_named_value_error(self):
        for projection in (
                lambda value: apply_profile_controls({}, value, backend='discovery_namespace'),
                lambda value: runtime_input(value, backend='discovery_namespace')):
            with self.assertRaisesRegex(ValueError, 'environment_mutation must not be null'):
                projection({'netem_covers_delivery': True, 'environment_mutation': None})

    def test_runtime_reconstruction_rejects_noncanonical_environment_controls(self):
        with self.assertRaisesRegex(ValueError, 'normalize before composition'):
            runtime_input({'environment_mutation': netem()}, backend='discovery_namespace')

    def test_single_window_budget_is_validated_without_changing_legacy_shape(self):
        with self.assertRaisesRegex(ValueError, 'budget exceeds'):
            _recovery_windows(0.2, recovery_timeout_sec=8, max_seconds=1)
        for invalid in (0, True, float('nan')):
            with self.subTest(max_seconds=invalid), self.assertRaises(ValueError):
                _recovery_windows(0.2, max_seconds=invalid)

    def test_invalid_inherited_windows_fail_closed_instead_of_single_window_fallback(self):
        invalid = [None, [], {}, {'count': 0}, {'count': True},
                   [window(0), dict(window(1), trigger_anchor='fresh_match')],
                   [dict(window(0), drop_duration_sec=0)],
                   [dict(window(0), trigger_offset_sec=float('inf'))],
                   [dict(window(0), unexpected=True)],
                   {'count': 2, 'windows': []}, {'count': 2, 'windows': None},
                   {'count': 2, 'restore_interval_sec': 'bad'},
                   {'count': 2, 'anchor_offset_sec': True},
                   {'count': 2, 'unexpected': True}]
        for windows in invalid:
            parent = self.parent()
            parent['environment_windows'] = windows
            with self.subTest(windows=windows), self.assertRaises(ValueError):
                self.plan(parent=parent)

    def test_real_default_runtime_refuses_dirty_source_and_persists_abort(self):
        from r2f.runtime.ros2_pair import run_pair
        self.source.update(source_frozen=False, dirty_behavior_paths=['src/example.py'])
        # This is the real callable, but the gate must return before invoking it.
        result, config, capsule = self.plan(execute=True, runtime=run_pair)
        self.assertIsNone(config)
        self.assertIsNone(capsule)
        self.assertEqual(result['abort']['reason_code'], 'source_behavior_not_tracked_and_clean_at_HEAD')
        self.assertEqual(result['budget']['attempted'], 0)
        self.assertEqual(result['budget']['generated'], 0)
        manifest = json.loads(Path(result['manifest_path']).read_text())
        self.assertEqual(manifest['abort'], result['abort'])
        persisted = json.loads((Path(result['manifest_path']).parent / 'cycle.json').read_text())
        self.assertEqual(persisted['abort'], result['abort'])
        self.assertEqual(result['selection_log']['selections'], [])

    def test_injected_fixture_is_not_blocked_or_mislabeled_real_execution(self):
        self.source.update(source_frozen=False, dirty_behavior_paths=['src/example.py'])
        calls = []

        def fixture(config, output_dir):
            calls.append(config)
            return {'real_execution': False, 'runtime_status': 'tool_error',
                    'reason': 'intentional offline fixture', 'evidence_origin': 'injected_runtime'}

        result, config, capsule = self.plan(execute=True, runtime=fixture)
        self.assertEqual(len(calls), 1)
        self.assertIsNone(result['abort'])
        row = result['rows'][0]
        self.assertFalse(row['actual_run'])
        self.assertEqual(row['evidence_origin'], 'injected_runtime')
        self.assertEqual(result['budget']['executed'], 0)

    def test_source_drift_during_planning_never_submits_to_runtime(self):
        changed = dict(SOURCE, behavior_tree_sha256='b' * 64)
        # A drift failure stays latched even if a later snapshot would return
        # to the original source; never launch a second case after this abort.
        for later in (changed, SOURCE):
            with self.subTest(later=later['behavior_tree_sha256']):
                runtime = Mock(side_effect=AssertionError('must not be submitted'))
                with patch('r2f.campaign.runner.source_manifest',
                           side_effect=[SOURCE, changed, later]) as read_source:
                    result, config, capsule = self.plan(execute=True, runtime=runtime)
                runtime.assert_not_called()
                self.assertEqual(read_source.call_count, 2)
                reason = 'source_repo_HEAD_or_behavior_tree_changed'
                row = result['rows'][0]
                self.assertEqual(row['execution_stage'], 'aborted')
                self.assertEqual(row['abort_reason'], reason)
                self.assertEqual(result['abort']['reason_code'], row['abort_reason'])
                self.assertEqual(result['stop_reason'], reason)
                self.assertEqual(result['last_state']['execution_stage'], 'aborted')
                self.assertFalse(row['actual_run'])
                self.assertNotIn('runtime_execution_metadata', row)
                self.assertNotIn('raw_path', row)
                self.assertFalse(Path(row['runtime_result_path']).exists())
                self.assertIsNone(capsule.get('execution'))
                self.assertEqual(result['budget']['attempted'], 0)
                self.assertEqual(result['budget']['executed'], 0)
                self.assertEqual(result['budget']['aborted'], 1)
                persisted_row = json.loads((Path(row['runtime_config_path']).parent / 'row.json').read_text())
                self.assertEqual(persisted_row, row)
                persisted_cycle = json.loads((Path(result['manifest_path']).parent / 'cycle.json').read_text())
                self.assertEqual(persisted_cycle['abort'], result['abort'])
                self.assertEqual(persisted_cycle['rows'][0]['execution_stage'], 'aborted')

    def test_controls_are_already_bound_for_wide_chain(self):
        result, config, capsule = self.plan(
            {'role_rmws': {'writer': CYCLONE, 'reader': FAST}},
            target=LATE, surface='P01', sampling_kinds=['boundary'], chain_length=2)
        self.assert_bound(result, config, capsule)
        self.assertEqual(len(config['mutation_plan']['operations']), 2)
        self.assertIn('rmws', config['final_input'])


    def test_all_chain_surfaces_bind_like_replay(self):
        from r2f.campaign.replay import _replay_config
        from r2f.runtime.ros2_pair import exploratory_bindings

        cases = [(LATE, 'P01', 2, ['P01', 'W02']),
                 (LATE, 'W02', 2, ['W02', 'P01']),
                 (PROJECTION, 'P02', 3, ['P02', 'P04', 'P05']),
                 (PROJECTION, 'P04', 3, ['P04', 'P02', 'P05'])]
        for target, surface, depth, expected_surfaces in cases:
            with self.subTest(target=target, surface=surface, depth=depth):
                result, config, capsule = self.plan(
                    target=target, surface=surface, chain_length=depth)
                digest = self.assert_bound(result, config, capsule)
                operations = config['mutation_plan']['operations']
                self.assertEqual([op['surface_id'] for op in operations], expected_surfaces)
                self.assertEqual(config['fuzz_surfaces'], expected_surfaces)
                contract = self.reg.get_target(target).exploration_contract
                expected_values = {}
                for sid in expected_surfaces:
                    for path in contract.surface_by_id[sid].input_paths:
                        value = config['final_input']
                        for part in path.split('.'):
                            value = value[part]
                        expected_values[path] = value
                self.assertEqual(config['submitted_parameters'], expected_values)
                self.assertEqual(result['rows'][0]['submitted_parameters'], expected_values)
                replay = _replay_config(ReplayCapsule.from_dict(capsule),
                                        'offline-replay', 230, SOURCE)
                self.assertEqual(replay['fuzz_surfaces'], expected_surfaces)
                self.assertEqual(replay['submitted_parameters'], expected_values)
                first_bindings = exploratory_bindings(config)
                self.assertEqual(first_bindings, exploratory_bindings(replay))
                self.assertEqual([item['surface_id'] for item in first_bindings],
                                 expected_surfaces)
                # Audit projections do not become a second hashed input layer.
                for field in ('fuzz_surfaces', 'submitted_parameters'):
                    self.assertNotIn(field, config['mutation_plan']['final_config'])
                    self.assertNotIn(field, capsule['input']['effective_input'])
                self.assertEqual(digest, replay['effective_input_sha256'])
                self.assertEqual(config['mutation_plan'], replay['mutation_plan'])

    def test_later_chain_parameter_tampering_fails_for_initial_and_replay(self):
        from r2f.campaign.replay import _replay_config
        from r2f.runtime.ros2_pair import exploratory_bindings, require_binding_preserved

        for target, surface, depth in ((LATE, 'P01', 2), (LATE, 'W02', 2),
                                       (PROJECTION, 'P02', 3), (PROJECTION, 'P04', 3)):
            result, initial, capsule = self.plan(
                target=target, surface=surface, chain_length=depth)
            self.assert_bound(result, initial, capsule)
            replay = _replay_config(ReplayCapsule.from_dict(capsule),
                                    'offline-replay', 230, SOURCE)
            later = [op['surface_id'] for op in initial['mutation_plan']['operations'][1:]]
            for kind, config in (('initial', initial), ('replay', replay)):
                bindings = exploratory_bindings(config)
                self.assertEqual([b['surface_id'] for b in bindings[1:]], later)
                for binding in bindings[1:]:
                    with self.subTest(surface=surface, kind=kind, field=binding['field']):
                        original = binding['submitted']
                        replacement = (original + 1 if type(original) in (int, float)
                                       else original + '-tampered')
                        changed = deepcopy(config)
                        value = changed
                        for part in binding['path'][:-1]:
                            value = value[part]
                        value[binding['path'][-1]] = replacement
                        with self.assertRaisesRegex(ValueError, 'exploratory_binding_normalized_away'):
                            require_binding_preserved(exploratory_bindings(changed), changed)
                        self.assert_runtime_tamper_rejected(
                            config, capsule, binding['path'][0], changed[binding['path'][0]])
                        changed = deepcopy(config)
                        changed['submitted_parameters'][binding['field']] = replacement
                        with self.assertRaisesRegex(ValueError, 'exploratory_binding_normalized_away'):
                            require_binding_preserved(exploratory_bindings(changed), changed)
                        del changed['submitted_parameters'][binding['field']]
                        with self.assertRaisesRegex(ValueError, 'submitted_parameter_missing'):
                            exploratory_bindings(changed)

    def test_plan_surface_projection_deduplicates_and_refuses_missing_values(self):
        from types import SimpleNamespace
        from r2f.campaign.runner import _plan_surface_bindings

        contract = self.reg.get_target(PROJECTION).exploration_contract
        final = self.parent('P04')
        plan = SimpleNamespace(
            operations=[SimpleNamespace(surface_id=sid)
                        for sid in ('P04', 'P02', 'P04', 'P05')], final_config=final)
        surfaces, submitted = _plan_surface_bindings(plan, contract)
        self.assertEqual(surfaces, ['P04', 'P02', 'P05'])
        self.assertEqual(list(submitted), ['qos.deadline_ns', 'qos.reliability', 'qos.liveliness'])
        self.assertEqual(submitted['qos.deadline_ns'], final['qos']['deadline_ns'])
        for broken in ({}, {'qos': None}, {'qos': {}}, {'qos': {'deadline_ns': None}}):
            with self.subTest(final=broken), self.assertRaisesRegex(
                    ValueError, 'submitted_parameter_(missing|null):qos.deadline_ns'):
                _plan_surface_bindings(SimpleNamespace(operations=plan.operations,
                                                       final_config=broken), contract)
        with self.assertRaisesRegex(ValueError, 'submitted_surface_not_in_contract:unknown'):
            _plan_surface_bindings(SimpleNamespace(
                operations=[SimpleNamespace(surface_id='unknown')], final_config=final), contract)
        with self.assertRaisesRegex(ValueError, 'submitted_surfaces_missing'):
            _plan_surface_bindings(SimpleNamespace(operations=[], final_config=final), contract)

    def test_plan_surface_projection_rejects_shared_path_conflicts(self):
        from types import SimpleNamespace
        from r2f.campaign.runner import _plan_surface_bindings

        # An isolated contradictory projection is a fixture, not a valid plan.
        contract = SimpleNamespace(surface_by_id={
            sid: SimpleNamespace(surface_id=sid, input_paths=['shared']) for sid in ('a', 'b')})
        plan = SimpleNamespace(operations=[SimpleNamespace(surface_id=sid) for sid in ('a', 'b')],
                               final_config={'shared': 1})
        self.assertEqual(_plan_surface_bindings(plan, contract), (['a', 'b'], {'shared': 1}))
        for contradictory in (2, True):
            with self.subTest(value=contradictory), patch(
                    'r2f.campaign.runner._flatten_surface_values',
                    side_effect=[{'shared': 1}, {'shared': contradictory}]):
                with self.assertRaisesRegex(ValueError, 'submitted_parameter_conflict:shared'):
                    _plan_surface_bindings(plan, contract)


if __name__ == '__main__':
    unittest.main()
