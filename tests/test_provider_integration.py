"""Offline integration of registered services; fixtures never prove ROS execution."""
import ast
from contextlib import ExitStack, contextmanager
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from r2f.core import ExecutionServices, Registry, ReplayCapsule, Scenario, compile_scenario
from r2f.core.evidence import canonical_hash, effective_input_hash
from r2f.campaign.identity import semantic_input
from r2f.campaign.runner import plan_campaign, run_campaign
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.campaign.replay import audit_replay, replay_cell
from r2f.modules.s1.module import S1Module, register, resolve_services
from r2f.modules.s1.providers import S1Carrier, S1FactProducer, S1MutationProvider, S1Oracle
from r2f.modules.s1.targets import EXECUTION_ORDER
from r2f.reports.run_report import audit_report
from test_campaign_evidence import fixture_pair


REPO = Path(__file__).resolve().parents[1]
SOURCE = {'source_repo': str(REPO), 'head': 'provider-fixed-source-fixture',
          'behavior_tree_sha256': 'a' * 64, 'source_frozen': True, 'dirty_behavior_paths': []}
TARGET_HASHES = (
    'c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940',
    '1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d',
    'fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce',
    'eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0',
)
CATALOG_HASH = '947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6'


def load(path):
    return json.loads(Path(path).read_text())


def legacy_capsule(target, row, parameters, seed):
    """Independent copy of 147af87's planning algorithm, with a fixed source."""
    submitted = semantic_input(row['arm'], parameters, target_id=target.target_id)
    recovery = target.target_id == EXECUTION_ORDER[2]
    projection = target.target_id == EXECUTION_ORDER[3]
    kind = 'discovery_drop' if recovery else 'endpoint_qos_variation' if projection else 'late_joiner'
    carrier = 'wire_proxy' if recovery else 'ros2_endpoint'
    mutation = ({key: submitted[key] for key in ('drop_duration_sec', 'recovery_timeout_sec', 'counter_interval_sec')}
                if recovery else {'baseline_qos': submitted['baseline_qos'], 'submitted_qos': submitted['qos']}
                if projection else {'reader_delay_sec': submitted['reader_delay_sec']})
    layers = ([{'layer': carrier, 'declared_mutation': kind,
                'implementation': 'isolated_netns_nft_discovery_udp_drop', 'parameters': mutation}]
              if recovery else [{'layer': carrier, 'declared_mutation': kind, 'parameters': mutation}]
              if projection else [{'layer': carrier, 'declared_mutation': kind,
                                    'submitted_reader_delay_sec': submitted['reader_delay_sec']}])
    scenario = compile_scenario(target, row['rmw'], row['arm'], row['repeat'], seed,
                                {'mutation_kind': kind, 'carrier': carrier, 'mutation_parameters': mutation})
    return ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE, final_input=submitted,
                                      input_layer_receipts=layers, origin_domain=93)


@contextmanager
def record_calls(trace):
    with ExitStack() as stack:
        for cls, name in ((S1MutationProvider, 'prepare'), (S1Carrier, 'accepts'),
                          (S1Carrier, 'apply'), (S1FactProducer, 'produce'), (S1Oracle, 'evaluate')):
            original = getattr(cls, name)

            def wrapper(self, value, *args, _original=original, _name=name, **kwargs):
                cell = (value['config']['cell_id'] if _name == 'produce' else
                        f'{value.rmw}.{value.arm}.{value.repeat}')
                trace.append((_name, cell))
                return _original(self, value, *args, **kwargs)

            stack.enter_context(patch.object(cls, name, wrapper))
        yield


class ProviderRegistryTests(unittest.TestCase):
    def test_resolves_actual_services_without_changing_catalog_or_targets(self):
        registry = Registry()
        module = register(registry)
        self.assertEqual(canonical_hash(module.to_dict()), CATALOG_HASH)
        self.assertNotIn('services', module.to_dict())
        for target_id, digest in zip(EXECUTION_ORDER, TARGET_HASHES):
            services = registry.get_services(target_id)
            self.assertIs(services, module.services[target_id])
            self.assertIsInstance(services, ExecutionServices)
            self.assertEqual(canonical_hash(registry.get_target(target_id).to_dict()), digest)
            self.assertEqual(registry.get_target(target_id).target_revision, 0)

    def test_invalid_services_fail_atomically(self):
        for invalid in ('missing_target', 'extra_target', 'not_mapping', 'not_binding',
                        'prepare', 'accepts', 'apply', 'produce', 'evaluate'):
            with self.subTest(invalid=invalid):
                module = S1Module()
                key = EXECUTION_ORDER[0]
                if invalid == 'missing_target':
                    del module.services[key]
                elif invalid == 'extra_target':
                    module.services['unregistered'] = module.services[key]
                elif invalid == 'not_mapping':
                    module.services = None
                elif invalid == 'not_binding':
                    module.services[key] = object()
                else:
                    binding = module.services[key]
                    names = ('mutation_provider', 'carrier', 'fact_producer', 'oracle')
                    values = {name: getattr(binding, name) for name in names}
                    slot = {'prepare': 'mutation_provider', 'accepts': 'carrier', 'apply': 'carrier',
                            'produce': 'fact_producer', 'evaluate': 'oracle'}[invalid]
                    methods = {'mutation_provider': ('prepare',), 'carrier': ('accepts', 'apply'),
                               'fact_producer': ('produce',), 'oracle': ('evaluate',)}[slot]
                    values[slot] = SimpleNamespace(**{m: (None if m == invalid else lambda *a: None)
                                                     for m in methods})
                    module.services[key] = ExecutionServices(**values)
                registry = Registry()
                with self.assertRaises(ValueError):
                    registry.register(module)
                self.assertEqual(dict(registry.modules), {})
                self.assertEqual(dict(registry.targets), {})
                with self.assertRaises(KeyError):
                    registry.get_services(key)

    def test_metadata_only_module_and_duplicate_registration(self):
        registry = Registry()
        target = S1Module().targets[EXECUTION_ORDER[0]]
        module = SimpleNamespace(id='declarations_only', targets={target.target_id: target})
        registry.register(module)
        with self.assertRaisesRegex(ValueError, 'no execution services'):
            registry.get_services(target.target_id)
        with self.assertRaises(ValueError):
            registry.register(module)
        self.assertEqual(list(registry.modules), ['declarations_only'])

    def test_clean_import_orders_have_no_s1_dependency_in_core_or_runtime_start(self):
        for module in ('r2f.core', 'r2f.modules.s1.module', 'r2f.campaign.runtime_adapter',
                       'r2f.campaign.runner', 'r2f.campaign.replay'):
            script = """
import importlib, sys
from unittest.mock import patch
with patch('socket.socket', side_effect=AssertionError('socket')), \\
     patch('subprocess.Popen', side_effect=AssertionError('participant')):
    importlib.import_module(sys.argv[1])
    if sys.argv[1] == 'r2f.core':
        assert not any(n.startswith('r2f.modules.') for n in sys.modules)
    else:
        from r2f.modules.s1.module import S1Module
        S1Module()
assert 'rclpy' not in sys.modules
"""
            result = subprocess.run([sys.executable, '-B', '-c', script, module],
                                    capture_output=True, text=True, env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
            self.assertEqual(result.returncode, 0, result.stderr)
        for path in (REPO / 'src/r2f/core').glob('*.py'):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    self.assertFalse((node.module or '').startswith('r2f.modules'), str(path))


class ProviderPlanningTests(unittest.TestCase):
    def test_four_target_default_and_representative_inputs_match_147_algorithm(self):
        checked = 0
        with tempfile.TemporaryDirectory() as tmp, patch('r2f.campaign.runner.source_manifest', return_value=SOURCE):
            for index, target_id in enumerate(EXECUTION_ORDER):
                parameters = ({'reader_delay_sec': .731, 'baseline_reader_delay_sec': .123,
                               'pre_match_count': 3, 'post_match_count': 9, 'delivery_timeout_sec': 2.31,
                               'publish_interval_sec': .041}
                              if index < 2 else {'drop_duration_sec': .9, 'recovery_timeout_sec': 4.2,
                                                'counter_interval_sec': .1, 'post_match_count': 9}
                              if index == 2 else {'variant_qos': {'reliability': 'reliable',
                                  'durability': 'transient_local', 'depth': 64, 'deadline_ns': 1250000000,
                                  'liveliness': 'manual_by_topic', 'liveliness_lease_duration_ns': 9223372036854775807}})
                for variant, values in enumerate((None, parameters)):
                    report = plan_campaign(Path(tmp) / f'{index}-{variant}', target_id=target_id,
                                           parameters=values, seed=7, domain_id=93)
                    target = S1Module().targets[target_id]
                    by_arm = {}
                    for row in report['rows']:
                        capsule = ReplayCapsule.from_dict(load(row['replay_path']))
                        reference = legacy_capsule(target, row, values, 7)
                        self.assertEqual(capsule.input, reference.input)
                        config = load(row['runtime_config_path'])
                        effective = capsule.input['effective_input']
                        self.assertEqual(config['final_input'], effective['final_input'])
                        self.assertEqual(config['input_layer_receipts'], effective['input_layer_receipts'])
                        self.assertEqual(row['effective_input_sha256'], reference.input['effective_input_sha256'])
                        self.assertIsNone(capsule.execution)
                        self.assertFalse(row['actual_run'])
                        self.assertEqual(row['execution_stage'], 'planned')
                        self.assertEqual(report['full_scope_required_cells'], 72)
                        by_arm[row['arm']] = config['final_input']
                        checked += 1
                    self.assertEqual(by_arm['baseline'], by_arm['transparent'])
        self.assertEqual(checked, 192)

    def test_prepare_only_describes_input_and_never_touches_io_or_runtime(self):
        module = S1Module()
        for target in module.targets.values():
            row = {'arm': 'mutated', 'rmw': 'rmw_cyclonedds_cpp', 'repeat': 0}
            capsule = legacy_capsule(target, row, None, 0)
            scenario = Scenario.from_dict(capsule.input['scenario'])
            with patch('builtins.open', side_effect=AssertionError('I/O')), \
                 patch.object(Path, 'open', side_effect=AssertionError('I/O')), \
                 patch('socket.socket', side_effect=AssertionError('socket')):
                prepared = module.services[target.target_id].mutation_provider.prepare(scenario)
                again = module.services[target.target_id].mutation_provider.prepare(scenario)
            self.assertEqual(prepared, again)
            self.assertEqual(prepared['input_layer_receipts'], capsule.input['effective_input']['input_layer_receipts'])
            self.assertNotIn('mutation_receipt', prepared)
            self.assertFalse({'hit', 'execution', 'effect_observed'} & set(prepared))

    def test_plan_never_calls_apply_produce_or_evaluate(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch('r2f.campaign.runner.source_manifest', return_value=SOURCE), \
             patch.object(S1Carrier, 'apply', side_effect=AssertionError('execution')), \
             patch.object(S1FactProducer, 'produce', side_effect=AssertionError('facts')), \
             patch.object(S1Oracle, 'evaluate', side_effect=AssertionError('oracle')):
            report = plan_campaign(tmp)
        self.assertEqual(len(report['rows']), 24)


class ProviderExecutionTests(unittest.TestCase):
    def make_plan(self, root):
        with patch('r2f.campaign.runner.source_manifest', return_value=SOURCE):
            return plan_campaign(root, run_id='provider-plan', domain_id=93)

    def test_actual_campaign_calls_all_services_and_preserves_control_paths(self):
        trace = []
        def runtime(config, directory):
            trace.append(('runtime', config['cell_id']))
            return fixture_pair(config, directory, fresh_matches=True)
        with tempfile.TemporaryDirectory() as tmp, record_calls(trace), \
             patch('r2f.campaign.runner.source_manifest', return_value=SOURCE):
            report = run_campaign(tmp, execute=True, runtime=runtime, run_id='provider-execution')
            for row in report['rows']:
                cell = row['cell_id']
                first = {name: trace.index((name, cell)) for name in ('prepare', 'accepts', 'apply',
                                                                    'runtime', 'produce', 'evaluate')}
                self.assertEqual(list(first.values()), sorted(first.values()))
                self.assertEqual(trace.count(('apply', cell)), 1)
                self.assertEqual(trace.count(('runtime', cell)), 1)
                self.assertEqual(row['execution_stage'], 'judged', row.get('abort_reason'))
                self.assertEqual(row['oracle_verdict'], 'fail' if row['arm'] == 'negative' else 'pass')
                if row['arm'] != 'mutated':
                    self.assertFalse(row['mutation_receipt']['hit'])
                    self.assertNotIn('mutation_applied', [h['execution_stage'] for h in row['stage_history']])
            self.assertEqual(load(report['candidate_ledger_path'])['natural_candidate_count'], 0)
            self.assertEqual(report['full_scope_required_cells'], 72)

    def test_default_runtime_is_reached_through_carrier(self):
        calls = []
        def run_pair(config, directory):
            calls.append(config['cell_id'])
            return fixture_pair(config, directory, fresh_matches=True)
        run_pair.__module__ = 'r2f.runtime.ros2_pair'
        with tempfile.TemporaryDirectory() as tmp, \
             patch('r2f.campaign.runner.source_manifest', return_value=SOURCE), \
             patch('r2f.runtime.ros2_pair.run_pair', run_pair), \
             patch.object(S1Carrier, 'apply', autospec=True, side_effect=ValueError('carrier denied')) as carrier:
            report = run_campaign(tmp, execute=True, run_id='default-runtime-refusal')
            self.assertEqual(len(calls), 0)
            self.assertEqual(carrier.call_count, 24)
            self.assertTrue(all(r['execution_stage'] == 'aborted' for r in report['rows']))
            self.assertTrue(all(load(r['runtime_result_path'])['runtime_status'] == 'tool_error' for r in report['rows']))
        with tempfile.TemporaryDirectory() as tmp, \
             patch('r2f.campaign.runner.source_manifest', return_value=SOURCE), \
             patch('r2f.runtime.ros2_pair.run_pair', run_pair):
            report = run_campaign(tmp, execute=True, run_id='default-runtime-fixture')
            self.assertEqual(len(calls), 24)
            self.assertTrue(all(r['execution_stage'] == 'judged' for r in report['rows']))

    def test_carrier_refuses_unsupported_scenarios_before_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self.make_plan(tmp)['rows'][0]
            capsule = load(row['replay_path'])['input']
            original = Scenario.from_dict(capsule['scenario'])
            carrier = resolve_services(original.target_id).carrier
            runtime = Mock(side_effect=AssertionError('must not launch'))
            context = {'config': load(row['runtime_config_path']), 'runtime': runtime, 'output_dir': Path(tmp)/'runtime'}
            cases = []
            for key, value in (('rmw', 'unsupported'),):
                bad = deepcopy(original); setattr(bad, key, value); cases.append(bad)
            bad = deepcopy(original); bad.parameters['carrier'] = 'wire_proxy'; cases.append(bad)
            bad = deepcopy(original); bad.parameters['mutation_kind'] = 'undeclared'; cases.append(bad)
            bad = deepcopy(original); bad.target.intent_and_scope += ' different'; cases.append(bad)
            bad = deepcopy(original); bad.target.target_id = 'unregistered'; cases.append(bad)
            third = S1Module().targets[EXECUTION_ORDER[2]]
            cases.append(compile_scenario(third, original.rmw, 'mutated', 0,
                parameters={'mutation_kind': 'restore_after_discovery_interruption', 'carrier': 'wire_proxy'}))
            for scenario in cases:
                self.assertFalse(carrier.accepts(scenario))
                with self.assertRaises(ValueError):
                    carrier.apply(scenario, capsule['effective_input'], execution=context)
            runtime.assert_not_called()

    def test_carrier_binds_semantic_config_and_execution_context_separately(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self.make_plan(tmp)['rows'][0]
            capsule = load(row['replay_path'])['input']
            scenario = Scenario.from_dict(capsule['scenario'])
            carrier = resolve_services(scenario.target_id).carrier
            prepared, config = capsule['effective_input'], load(row['runtime_config_path'])
            digest = effective_input_hash(prepared)
            for domain in (94, 95):
                config['domain_id'] = domain
                marker = {'runtime_status': 'fixture_response', 'mutation_receipt': {'hit': False}}
                runtime = Mock(return_value=marker)
                result = carrier.apply(scenario, prepared, execution={'config': config, 'runtime': runtime,
                                                                     'output_dir': Path(tmp)/str(domain)})
                self.assertIs(result, marker)
                runtime.assert_called_once()
                self.assertEqual(effective_input_hash(prepared), digest)
            runtime = Mock()
            for case in ('hash', 'layer', 'final', 'scenario', 'identity'):
                bad = deepcopy(config)
                if case == 'hash': bad['effective_input_sha256'] = '0'*64
                elif case == 'layer': bad['input_layer_receipts'] = []
                elif case == 'final': bad['topic'] += '_tampered'
                elif case == 'scenario': bad['scenario']['seed'] += 1
                else: bad['repeat'] += 1
                with self.assertRaises(ValueError):
                    carrier.apply(scenario, prepared, execution={'config': bad, 'runtime': runtime,
                                                                 'output_dir': Path(tmp)/'bad'})
            runtime.assert_not_called()

    def test_carrier_rejects_equal_python_values_with_different_json_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = next(r for r in self.make_plan(tmp)['rows'] if r['arm'] == 'mutated' and r['repeat'] == 0)
            capsule = load(row['replay_path'])['input']
            scenario = Scenario.from_dict(capsule['scenario'])
            carrier = resolve_services(scenario.target_id).carrier
            original = load(row['runtime_config_path'])
            prepared = deepcopy(capsule['effective_input'])
            runtime = Mock(side_effect=AssertionError('type tampering must not launch'))
            changes = [(['repeat'], False), (['repeat'], 0.0), (['seed'], False),
                       (['scenario', 'repeat'], False), (['scenario', 'repeat'], 0.0),
                       (['final_input', 'qos', 'depth'], 64.0),
                       (['qos', 'depth'], 64.0), (['pre_match_count'], 2.0)]
            for keys, value in changes:
                with self.subTest(keys=keys, value=value):
                    config = deepcopy(original)
                    node = config
                    for key in keys[:-1]:
                        node = node[key]
                    self.assertEqual(node[keys[-1]], value)  # Python would accept it.
                    self.assertNotEqual(canonical_hash(node[keys[-1]]), canonical_hash(value))
                    node[keys[-1]] = value
                    with self.assertRaises(ValueError):
                        carrier.apply(scenario, prepared, execution={'config': config, 'runtime': runtime,
                                                                    'output_dir': Path(tmp)/'no-runtime'})
            runtime.assert_not_called()
            self.assertEqual(canonical_hash(prepared), canonical_hash(capsule['effective_input']))

    def test_raw_integrity_and_failed_facts_cannot_bypass_record_gates(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self.make_plan(Path(tmp)/'plan')['rows'][0]
            config = load(row['runtime_config_path'])
            capsule = ReplayCapsule.from_dict(load(row['replay_path']))
            scenario = Scenario.from_dict(capsule.input['scenario'])
            runtime = fixture_pair(config, Path(tmp)/'runtime', fresh_matches=True)
            broken = deepcopy(runtime); broken['raw_sha256'] = '0'*64
            with patch.object(S1FactProducer, 'produce', side_effect=AssertionError('unverified raw')) as produce:
                failed = ingest_execution(scenario, config, broken, capsule)
            produce.assert_not_called()
            self.assertEqual(failed['execution_stage'], 'aborted')
            self.assertEqual(failed['raw_path'], runtime['raw_path'])
            with patch.object(S1FactProducer, 'produce', side_effect=ValueError('fact failure')):
                failed = ingest_execution(scenario, config, runtime, capsule)
            self.assertEqual(failed['execution_stage'], 'aborted')
            self.assertIn('fact failure', failed['abort_reason'])
            self.assertEqual(failed['raw_sha256'], runtime['raw_sha256'])

    def test_fact_provider_failures_preserve_complete_campaign(self):
        cases = (("runtime_error", RuntimeError("fact runtime failure"), None),
                 ("attribute_error", AttributeError("fact attribute failure"), None),
                 ("non_mapping", None, ["invalid facts"]),
                 ("non_finite", None, {"nested": {"value": float("nan")}}),
                 ("invalid_key", None, {"nested": {1: "invalid JSON key"}}),
                 ("non_json", None, {"nested": {"value": object()}}))
        for name, exception, response in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as tmp:
                runtime_calls = []
                def runtime(config, directory):
                    runtime_calls.append(config['cell_id'])
                    return fixture_pair(config, directory, fresh_matches=True)
                options = {'side_effect': exception} if exception is not None else {'return_value': response}
                with patch('r2f.campaign.runner.source_manifest', return_value=SOURCE), \
                     patch.object(S1FactProducer, 'produce', **options) as produce, \
                     patch.object(S1Oracle, 'evaluate', side_effect=AssertionError('invalid facts reached oracle')) as evaluate:
                    report = run_campaign(tmp, execute=True, runtime=runtime, run_id='facts-failure-' + name)
                self.assertEqual(len(runtime_calls), 24)
                self.assertEqual(produce.call_count, 24)
                self.assertEqual(evaluate.call_count, 0)
                self.assertEqual(load(Path(tmp)/'report.json')['rows'], report['rows'])
                self.assertEqual(load(report['candidate_ledger_path'])['natural_candidate_count'], 0)
                self.assertFalse(report['seal']['sealed'])
                for row in report['rows']:
                    self.assertEqual(row['execution_stage'], 'aborted')
                    self.assertEqual(row['oracle_verdict'], 'unknown')
                    self.assertEqual(row['oracle']['reason_codes'], ['fact_producer_tool_failure'])
                    self.assertEqual(row['oracle']['forbidden_residuals'], [])
                    self.assertEqual(row['typed_facts'], {})
                    self.assertIn('fact_producer_tool_failure', row['abort_reason'])
                    self.assertTrue(row['actual_run'])
                    self.assertTrue(row['tool_error'])
                    self.assertEqual(row['runtime_status'], 'completed')
                    failure = row['fact_producer_tool_failure']
                    self.assertEqual(failure['kind'], 'exception' if exception else 'invalid_result')
                    if exception:
                        self.assertEqual(failure['exception_type'], type(exception).__name__)
                    runtime_result = load(row['runtime_result_path'])
                    from r2f.core.evidence import verify_raw
                    verify_raw(row['raw_path'], row['raw_sha256'])
                    self.assertEqual(row['raw_sha256'], runtime_result['raw_sha256'])
                    self.assertEqual(row['raw_path'], runtime_result['raw_path'])
                    self.assertEqual(row['runtime_execution_metadata'], runtime_result['execution_metadata'])
                    execution = ReplayCapsule.from_dict(load(row['replay_path'])).execution
                    self.assertIsNotNone(execution)
                    self.assertEqual(execution['execution_stage'], 'aborted')
                    self.assertEqual(execution['raw_sha256'], row['raw_sha256'])
                    self.assertTrue(execution['actual_run'])
                    self.assertEqual(execution['fact_producer_tool_failure'], failure)
                    self.assertNotIn('judged', [h['execution_stage'] for h in row['stage_history']])

    def test_fact_provider_base_exceptions_propagate_and_mapping_is_accepted(self):
        from types import MappingProxyType
        with tempfile.TemporaryDirectory() as tmp:
            report = self.make_plan(tmp)
            row = report['rows'][0]
            capsule = ReplayCapsule.from_dict(load(row['replay_path']))
            scenario = Scenario.from_dict(capsule.input['scenario'])
            config = load(row['runtime_config_path'])
            runtime = fixture_pair(config, Path(tmp)/'runtime', fresh_matches=True)
            original = S1FactProducer.produce
            with patch.object(S1FactProducer, 'produce', autospec=True,
                              side_effect=lambda self, evidence: MappingProxyType(original(self, evidence))):
                accepted = ingest_execution(scenario, config, runtime, capsule)
            self.assertEqual(accepted['execution_stage'], 'judged')
            self.assertEqual(accepted['oracle_verdict'], 'pass')
            for exception in (KeyboardInterrupt('stop'), SystemExit('stop')):
                with self.subTest(exception=type(exception).__name__), \
                     patch.object(S1FactProducer, 'produce', side_effect=exception), \
                     patch.object(S1Oracle, 'evaluate') as evaluate:
                    with self.assertRaises(type(exception)):
                        ingest_execution(scenario, config, runtime, capsule)
                    self.assertEqual(evaluate.call_count, 0)

    def test_oracle_exceptions_and_invalid_results_preserve_complete_campaign(self):
        cases = (("value_error", ValueError("oracle provider broken"), None),
                 ("type_error", TypeError("oracle provider type failure"), None),
                 ("non_mapping", None, ["invalid oracle response"]),
                 ("missing_contract", None, {"verdict": "pass"}))
        for name, exception, response in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as tmp:
                runtime_calls = []
                def runtime(config, directory):
                    runtime_calls.append(config['cell_id'])
                    return fixture_pair(config, directory, fresh_matches=True)
                options = {'side_effect': exception} if exception is not None else {'return_value': response}
                with patch('r2f.campaign.runner.source_manifest', return_value=SOURCE), \
                     patch.object(S1Oracle, 'evaluate', **options) as evaluate:
                    report = run_campaign(tmp, execute=True, runtime=runtime, run_id='oracle-failure-' + name)
                self.assertEqual(len(runtime_calls), 24)
                self.assertEqual(evaluate.call_count, 24)  # Never retry a failed provider.
                self.assertEqual(load(Path(tmp)/'report.json')['rows'], report['rows'])
                ledger = load(report['candidate_ledger_path'])
                self.assertEqual(ledger['natural_candidate_count'], 0)
                self.assertFalse(report['seal']['sealed'])
                for row in report['rows']:
                    self.assertEqual(row['execution_stage'], 'aborted')
                    self.assertEqual(row['oracle_verdict'], 'unknown')
                    self.assertEqual(row['oracle']['forbidden_residuals'], [])
                    self.assertEqual(row['oracle']['reason_codes'], ['oracle_tool_failure'])
                    self.assertIn('oracle_tool_failure', row['abort_reason'])
                    self.assertTrue(row['actual_run'])
                    self.assertTrue(row['tool_error'])
                    self.assertEqual(row['runtime_status'], 'completed')
                    self.assertEqual(row['oracle_tool_failure']['kind'], 'exception' if exception else 'invalid_result')
                    if exception:
                        self.assertEqual(row['oracle_tool_failure']['exception_type'], type(exception).__name__)
                    else:
                        self.assertEqual(row['oracle_tool_failure']['returned_result'], response)
                    runtime_result = load(row['runtime_result_path'])
                    from r2f.core.evidence import verify_raw
                    verify_raw(row['raw_path'], row['raw_sha256'])
                    self.assertEqual(row['raw_sha256'], runtime_result['raw_sha256'])
                    self.assertEqual(row['raw_path'], runtime_result['raw_path'])
                    execution = ReplayCapsule.from_dict(load(row['replay_path'])).execution
                    self.assertIsNotNone(execution)
                    self.assertEqual(execution['execution_stage'], 'aborted')
                    self.assertEqual(execution['raw_sha256'], row['raw_sha256'])
                    self.assertTrue(execution['actual_run'])
                    self.assertEqual(execution['oracle_tool_failure'], row['oracle_tool_failure'])
                    self.assertNotIn('judged', [h['execution_stage'] for h in row['stage_history']])

    def test_verifier_recomputes_facts_oracle_without_preparing_or_executing(self):
        # A real retained report supplies the raw/identity/control/provenance
        # preconditions. Only source observation is fixed to its original
        # manifest, as a read-only regression fixture; this is not a new seal
        # for the current changed source. Never relabel an offline runtime.
        supplied = os.environ.get('R2F_PROVIDER_AUDIT_REPORT')
        if not supplied:
            self.skipTest('set R2F_PROVIDER_AUDIT_REPORT to a retained real report')
        report_path = Path(supplied).resolve()
        report = load(report_path)
        manifest = load(report['manifest_path'])
        source = manifest['source_manifest']
        self.assertTrue(source['source_frozen'])
        self.assertEqual(source['dirty_behavior_paths'], [])
        self.assertEqual(len(report['rows']), 24)
        self.assertTrue(report['seal']['sealed'])
        paths = sorted(p for p in report_path.parent.rglob('*') if p.is_file())
        from r2f.core.evidence import raw_sha256
        before = {str(p): raw_sha256(p) for p in paths}
        original_produce = S1FactProducer.produce
        original_evaluate = S1Oracle.evaluate
        with patch('r2f.reports.run_report.source_manifest', return_value=source), \
             patch.object(S1MutationProvider, 'prepare', side_effect=AssertionError('prepare')) as prepare, \
             patch.object(S1Carrier, 'apply', side_effect=AssertionError('apply')) as apply, \
             patch.object(S1FactProducer, 'produce', autospec=True, side_effect=original_produce) as facts, \
             patch.object(S1Oracle, 'evaluate', autospec=True, side_effect=original_evaluate) as oracle:
            audit = audit_report(report_path)
        self.assertTrue(audit['sealed'], audit['errors'])
        self.assertEqual(audit['errors'], [])
        self.assertEqual(facts.call_count, len(report['rows']))
        self.assertEqual(oracle.call_count, len(report['rows']))
        prepare.assert_not_called()
        apply.assert_not_called()
        self.assertEqual(before, {str(p): raw_sha256(p) for p in paths})


class ProviderReplayTests(unittest.TestCase):
    def prepare_original(self, root):
        with patch('r2f.campaign.runner.source_manifest', return_value=SOURCE):
            report = plan_campaign(root/'original', run_id='provider-origin', domain_id=93,
                                   parameters={'reader_delay_sec': .731, 'post_match_count': 9})
        row = next(r for r in report['rows'] if r['arm'] == 'mutated')
        return Path(row['replay_path']), ReplayCapsule.from_dict(load(row['replay_path']))

    def test_replay_and_audit_use_original_capsule_and_shared_services_without_prepare(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path, capsule = self.prepare_original(root)
            original = {p: p.read_bytes() for p in (root/'original').rglob('*') if p.is_file()}
            seen, trace = [], []
            def runtime(config, directory):
                seen.append(deepcopy(config))
                trace.append(('runtime', config['cell_id']))
                return fixture_pair(config, directory, fresh_matches=True)
            with patch('r2f.campaign.replay.source_manifest', return_value=SOURCE), \
                 patch.object(S1MutationProvider, 'prepare', side_effect=AssertionError('replay prepare')), \
                 patch('r2f.campaign.identity.semantic_input', side_effect=AssertionError('rebuild semantics')), \
                 patch('r2f.campaign.identity.projection_input', side_effect=AssertionError('rebuild QoS')), \
                 record_calls(trace):
                report = replay_cell(path, output_dir=root/'replay', domain_id=94, runtime=runtime)
                with patch.object(S1Carrier, 'apply', side_effect=AssertionError('audit execution')):
                    audit = audit_replay(report['report_path'])
            self.assertTrue(audit['valid'], audit)
            self.assertEqual(report['status'], 'pass', report)
            self.assertTrue(report['runtime_invoked'])
            self.assertEqual(report['execution_origin'], 'injected_runtime')
            self.assertFalse(report['real_runtime_attempt'])
            self.assertFalse(report['real_ros_execution'])
            self.assertEqual(len(seen), 1)
            config = seen[0]
            for key in ('final_input', 'input_layer_receipts'):
                self.assertEqual(config[key], capsule.input['effective_input'][key])
            for key in ('effective_input_sha256', 'source_manifest'):
                self.assertEqual(config[key], capsule.input[key])
            self.assertEqual(ReplayCapsule.from_dict(load(root/'replay/derived_capsule.json')).input, capsule.input)
            self.assertEqual(original, {p: p.read_bytes() for p in original})
            cell = config['cell_id']
            positions = [trace.index((name, cell)) for name in ('accepts', 'apply', 'runtime', 'produce', 'evaluate')]
            self.assertEqual(positions, sorted(positions))
            self.assertFalse(any(name == 'prepare' for name, _ in trace))

    def test_replay_carrier_refusal_does_not_claim_runtime_invocation(self):
        for refusal in ('accepts', 'apply'):
            with self.subTest(refusal=refusal), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); path, _ = self.prepare_original(root)
                runtime = Mock(side_effect=AssertionError('refused runtime'))
                replacement = {'return_value': False} if refusal == 'accepts' else {'side_effect': ValueError('carrier refusal')}
                with patch('r2f.campaign.replay.source_manifest', return_value=SOURCE), \
                     patch.object(S1Carrier, refusal, **replacement):
                    report = replay_cell(path, output_dir=root/'replay', domain_id=94, runtime=runtime)
                runtime.assert_not_called()
                self.assertFalse(report['runtime_invoked'])
                self.assertNotEqual(report['status'], 'pass')
                self.assertEqual(len(load(root/'replay/derived_capsule.json')['replay_attempts']), 1)
                self.assertTrue(audit_replay(report['report_path'])['valid'])

    def test_replay_runtime_error_and_malformed_response_keep_existing_classification(self):
        for case in ('raise', 'non_dict'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); path, _ = self.prepare_original(root)
                runtime = Mock(side_effect=RuntimeError('provider runtime fault')) if case == 'raise' else Mock(return_value=['malformed'])
                with patch('r2f.campaign.replay.source_manifest', return_value=SOURCE):
                    report = replay_cell(path, output_dir=root/'replay', domain_id=94, runtime=runtime)
                self.assertTrue(report['runtime_invoked'])
                runtime.assert_called_once()
                self.assertEqual(report['status'], 'fail_environment' if case == 'raise' else 'fail_artifact')
                self.assertEqual(report['reason_code'], 'runtime_tool_failure' if case == 'raise' else 'invalid_runtime_result')
                if case == 'non_dict':
                    self.assertEqual(load(root/'replay/runtime_response.json'), ['malformed'])
                self.assertTrue(audit_replay(report['report_path'])['valid'])

    def test_replay_preflight_still_rejects_source_and_original_domain_before_carrier(self):
        for case in ('source', 'domain'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); path, _ = self.prepare_original(root)
                source = dict(SOURCE, head='different') if case == 'source' else SOURCE
                with patch('r2f.campaign.replay.source_manifest', return_value=source), \
                     patch.object(S1Carrier, 'apply', side_effect=AssertionError('preflight must stop')):
                    report = replay_cell(path, output_dir=root/'replay', domain_id=93 if case == 'domain' else 94,
                                         runtime=Mock(side_effect=AssertionError('runtime')))
                self.assertFalse(report['runtime_invoked'])
                self.assertEqual(report['reason_code'], 'source_identity_mismatch' if case == 'source' else 'fresh_domain_required')


if __name__ == '__main__':
    unittest.main()
