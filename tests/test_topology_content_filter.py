"""Initial CFT configuration contracts over explicitly NON-ROS fixtures.

Fake Node/Subscription calls are not RMW qualification, sample-filter effects,
mutation execution, real match evidence, natural candidates, or target seals.
No ROS context, DDS endpoint, network namespace or subprocess is started here.
"""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import sys
from types import ModuleType, SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from r2f.runtime import filter_backend as fb
from r2f.runtime import topology_worker as tw


class FakeOptions:
    def __init__(self, filter_expression, expression_parameters):
        self.filter_expression = filter_expression
        self.expression_parameters = list(expression_parameters)


class FakeSubscription:
    def __init__(self, expression='data = %0', parameters=None, enabled=True, fail_get=None):
        self.options = FakeOptions(expression, ['keep'] if parameters is None else parameters)
        self.enabled = enabled
        self.fail_get = fail_get
        self.calls = []

    def get_content_filter(self):
        self.calls.append('get_content_filter')
        if self.fail_get is not None:
            raise self.fail_get
        return self.options

    @property
    def is_cft_enabled(self):
        self.calls.append('is_cft_enabled')
        return self.enabled

    def set_content_filter(self, *_):
        raise AssertionError('dynamic updates are outside this card')


class FakeNode:
    def __init__(self, subscription=None, fail=None):
        self.subscription = subscription if subscription is not None else FakeSubscription()
        self.fail = fail
        self.calls = []

    def create_subscription(self, message_type, topic, callback, qos, *, content_filter_options=None):
        self.calls.append((message_type, topic, callback, qos, content_filter_options))
        if self.fail is not None:
            raise self.fail
        return self.subscription


class LegacyNode(FakeNode):
    def create_subscription(self, message_type, topic, callback, qos):
        self.calls.append((message_type, topic, callback, qos))
        return self.subscription


class RCLError(RuntimeError):
    """Fixture exception ONLY, never an imported/observed RMW exception."""


def spec(role='reader', filter_value=None):
    value = {
        'logical_id': 'r0' if role == 'reader' else 'w0', 'role': role,
        'topic': '/r2f/cft_fixture', 'type_name': 'std_msgs/msg/String',
        'qos': {'reliability': 'reliable', 'durability': 'volatile', 'history': 'keep_last',
                'depth': 16, 'deadline_sec': 'infinite', 'liveliness': 'automatic',
                'lease_duration_sec': 'infinite', 'ownership': 'shared', 'partitions': []},
        'vendor': None, 'participant_id': None,
    }
    if filter_value is not None:
        value['filter'] = filter_value
    return value


def initial(expression='data = %0', parameters=None, evaluation=False):
    return {'expression': expression, 'parameters': ['keep'] if parameters is None else parameters,
            'evaluation': evaluation}


class InitialFilterBinding(unittest.TestCase):
    def test_capability_is_pure_stable_json_and_only_conditional_initial_configuration(self):
        expected = {
            'status': 'conditional', 'scope': 'initial_only', 'roles': ['reader'],
            'message_type': 'std_msgs/msg/String', 'field_types': {'data': 'string'},
            'requirements': ['public_creation_api', 'rmw_acceptance',
                             'get_content_filter', 'is_cft_enabled'],
            'sample_filter_effect': False, 'dynamic_update': False, 'lease': False,
        }
        with patch('builtins.__import__', wraps=__import__) as imports:
            first = tw.initial_content_filter_capability()
        self.assertEqual(first, expected)
        self.assertEqual(json.loads(json.dumps(first, allow_nan=False)), expected)
        self.assertFalse(any(c.args[0].split('.')[0] == 'rclpy' for c in imports.call_args_list))
        first['requirements'].clear()
        first['field_types']['data'] = 'number'
        self.assertEqual(tw.initial_content_filter_capability(), expected)

    def test_binding_is_exact_input_only_and_evaluation_is_not_api_configuration(self):
        expression = "  data = %0 OR data LIKE %1  "
        parameters = ["'keep'", "'%sentinel%'"]
        bindings = []
        for evaluation in (True, False, None):
            endpoint = spec(filter_value=initial(expression, parameters, evaluation))
            before = deepcopy(endpoint)
            bound = tw.endpoint_expressibility(endpoint, {})
            self.assertEqual(endpoint, before)
            self.assertEqual(bound['initial_content_filter'],
                             {'expression': expression, 'parameters': parameters})
            self.assertFalse(bound['binding']['execution_evidence'])
            self.assertFalse(bound['binding']['candidate_eligible'])
            self.assertEqual(bound['binding']['initial_content_filter']['rmw_support'], 'unverified')
            self.assertNotIn('evaluation', bound['initial_content_filter'])
            bindings.append(bound['binding'])
        self.assertEqual(bindings[0], bindings[1])
        self.assertEqual(bindings[1], bindings[2])

    def test_illegal_string_fields_still_reject_precisely_including_both_legacy_cases(self):
        for expression, field in [('data.x > 1', 'data.x'), ('d.x', 'd.x')]:
            with self.subTest(expression=expression), self.assertRaises(tw.TopologyUnsupported) as caught:
                tw.endpoint_expressibility(spec(filter_value=initial(expression, [])), {})
            self.assertEqual(caught.exception.code, 'content_filter_invalid_field')
            self.assertEqual(caught.exception.details['layer'], 'unsupported_field')
            self.assertEqual(str(caught.exception), 'unknown_field:' + field)
            self.assertNotIn('no public', str(caught.exception))

    def test_invalid_syntax_types_indices_and_budgets_never_reach_node(self):
        cases = [initial('data =', []), initial('data > %0', ['1']),
                 initial('data = %1', ['x']), initial('data = %0', 'x'),
                 initial('data = %0', [1]), initial('data = %0', ['x'] * 17),
                 initial('data = %0', ['x' * 257]), initial('x' * 1025, []),
                 initial(7, []), initial('data = 1', [], evaluation='true')]
        for value in cases:
            node = FakeNode()
            with self.subTest(value=value), self.assertRaises(tw.TopologyUnsupported):
                tw._create_reader_with_initial_filter(node, object, '/topic', lambda *_: None,
                                                     object(), value, options_factory=FakeOptions,
                                                     fixture=True)
            self.assertEqual(node.calls, [])
        with self.assertRaises(tw.TopologyUnsupported) as caught:
            tw.endpoint_expressibility(spec('writer', initial()), {})
        self.assertEqual(caught.exception.code, 'content_filter_role_not_supported')

    def test_parent_passes_exact_filter_to_worker_but_does_not_backfill_observations(self):
        configs = []
        class FixtureProcess:
            def __init__(self, config, *_):
                configs.append(config)
                self.process = SimpleNamespace(pid=424242)
                self.events = [{'event': 'endpoint_created', 'pid': 424242, 'pgid': 424242,
                                'endpoint_gid': 'aa11', 'endpoint_generation': 'fake-generation',
                                'process_start_ticks': 17, 'fixture': True, 'real_ros': False,
                                'candidate_eligible': False}]
            def start(self):
                pass
            def wait(self, predicate, *_):
                assert predicate()
            def has(self, event, **fields):
                return any(row['event'] == event and all(row.get(k) == v for k, v in fields.items())
                           for row in self.events)
            def close(self):
                return {'closed': True}
        with tempfile.TemporaryDirectory() as directory:
            backend = tw.RosTopologyBackend({'run_id': 'fixture', 'domain_id': 191,
                                            'rmw': 'fixture_no_rmw'}, directory)
            backend.environment = {}  # Never prepare/start any actual runtime.
            with patch.object(tw, '_EndpointProcess', FixtureProcess), \
                    patch.object(backend, '_start_ticks', return_value=17):
                handle = backend.create_endpoint(spec(filter_value=initial()), 1, 'reader')
        self.assertEqual(configs[0]['initial_content_filter'],
                         {'expression': 'data = %0', 'parameters': ['keep']})
        self.assertIsNone(handle['initial_content_filter'])  # No child readback means unknown.


class InitialFilterCreation(unittest.TestCase):
    def configure(self, node=None, value=None):
        node = FakeNode() if node is None else node
        records = []
        endpoint, result = tw._create_reader_with_initial_filter(
            node, object, '/topic', lambda *_: None, object(), initial() if value is None else value,
            options_factory=FakeOptions, fixture=True,
            emit=lambda event, **fields: records.append({'event': event, **fields}))
        return node, endpoint, result, records

    def test_nonempty_creation_uses_exact_options_and_reads_both_actual_api_results(self):
        expression, parameters = '  data = %0  ', ["'001'"]
        subscription = FakeSubscription(expression, parameters)
        node, endpoint, result, records = self.configure(FakeNode(subscription), initial(expression, parameters))
        self.assertIs(endpoint, subscription)
        options = node.calls[0][-1]
        self.assertEqual((options.filter_expression, options.expression_parameters), (expression, parameters))
        self.assertEqual(subscription.calls, ['get_content_filter', 'is_cft_enabled'])
        self.assertEqual(result['observed'], {'expression': expression, 'parameters': parameters})
        self.assertTrue(result['is_cft_enabled'])
        self.assertTrue(result['configured'])
        self.assertEqual([r['event'] for r in records], ['content_filter_initial_begin',
                         'content_filter_initial_submitted', 'content_filter_initial_configuration'])
        for record in records:
            receipt = record['filter_receipt']
            self.assertTrue(receipt['fixture'])
            self.assertFalse(receipt['candidate_eligible'])
            self.assertFalse(receipt['counts_toward_maturity'])
            self.assertFalse(receipt['execution_evidence'])
            self.assertIsNone(receipt['filter_effect_observed'])
            self.assertFalse(receipt['filter_false_implies_unmatch'])
            self.assertNotIn('evaluation', receipt.get('requested', {}))
        self.assertLessEqual(result['creation_returned_monotonic_ns'], result['monotonic_ns'])

    def test_filterless_creation_keeps_legacy_call_and_does_not_import_ros(self):
        node = LegacyNode()
        with patch.object(fb, 'content_filter_options', side_effect=AssertionError('not needed')):
            endpoint, receipt = tw._create_reader_with_initial_filter(
                node, object, '/topic', lambda *_: None, object())
        self.assertIs(endpoint, node.subscription)
        self.assertIsNone(receipt)
        self.assertEqual(len(node.calls[0]), 4)

    def test_empty_expression_creates_without_cft_kwarg_and_observes_disabled_not_unmatched(self):
        node, _, result, _ = self.configure(
            LegacyNode(FakeSubscription('', [], enabled=False)), initial('', [], evaluation=True))
        self.assertEqual(len(node.calls[0]), 4)
        self.assertEqual(result['observed'], {'expression': '', 'parameters': []})
        self.assertFalse(result['is_cft_enabled'])
        self.assertTrue(result['configured'])
        self.assertFalse(result['filter_false_implies_unmatch'])
        self.assertNotIn('matched', result)
        self.assertIsNone(result['filter_effect_observed'])

    def test_missing_creation_api_rejects_before_call_without_unfiltered_fallback(self):
        node = LegacyNode()
        with self.assertRaises(tw.TopologyUnsupported) as caught:
            self.configure(node)
        self.assertEqual(caught.exception.code, 'content_filter_submission_api')
        self.assertEqual(caught.exception.details['layer'], 'submission_api')
        self.assertFalse(caught.exception.details['submitted'])
        self.assertEqual(node.calls, [])

    def test_missing_options_module_is_api_layer_not_rmw_rejection(self):
        node = FakeNode()
        with patch.object(fb, 'content_filter_options', side_effect=ImportError('fixture missing module')):
            with self.assertRaises(tw.TopologyUnsupported) as caught:
                self.configure(node)
        self.assertEqual(caught.exception.details['layer'], 'submission_api')
        self.assertFalse(caught.exception.details['submitted'])
        self.assertEqual(node.calls, [])

    def test_rcl_error_and_python_api_error_are_separate_failed_submissions(self):
        class DerivedRCLError(RCLError):
            pass
        for error, layer in [(DerivedRCLError('fixture RMW rejected CFT'), 'submission_rcl'),
                             (TypeError('fixture kwarg error'), 'submission_api'),
                             (RuntimeError('fixture other failure'), 'submission_api')]:
            node = FakeNode(fail=error)
            with self.subTest(layer=layer, error=error), self.assertRaises(tw.TopologyUnsupported) as caught:
                self.configure(node)
            value = caught.exception.details
            self.assertEqual(caught.exception.code, 'content_filter_' + layer)
            self.assertEqual(value['layer'], layer)
            self.assertEqual(value['error_type'], type(error).__name__)
            self.assertEqual(value['error'], str(error))
            self.assertTrue(value['submitted'])
            self.assertFalse(value['accepted'])
            self.assertIsNone(value['configured'])
            self.assertEqual(len(node.calls), 1)

    def test_api_mismatch_is_retained_not_replaced_by_input_evaluation(self):
        _, _, result, _ = self.configure(FakeNode(FakeSubscription('data = %0', ['different'], False)),
                                         initial(evaluation=True))
        self.assertFalse(result['is_cft_enabled'])
        self.assertEqual(result['observed']['parameters'], ['different'])
        self.assertFalse(result['configured'])
        self.assertEqual(result['readback_status'], 'mismatched')
        self.assertFalse(result['candidate_eligible'])
        self.assertIsNone(result['filter_effect_observed'])


    def test_worker_entry_wires_creation_and_raw_readback_without_changing_match_counts(self):
        # Every rclpy module here is a local fake; no installed ROS module or
        # actual init/context/executor/socket/process is ever used.
        modules = {name: ModuleType(name) for name in [
            'rclpy', 'rclpy.context', 'rclpy.executors', 'rclpy.node', 'rclpy.qos',
            'rclpy.subscription_content_filter_options', 'std_msgs', 'std_msgs.msg']}
        modules['rclpy'].init = lambda **_: None
        modules['rclpy'].get_rmw_implementation_identifier = lambda: 'offline_fixture_no_rmw'
        class FixtureContext:
            def get_domain_id(self):
                return 191
            def shutdown(self):
                pass
        class FixtureExecutor:
            def __init__(self, **_):
                pass
            def add_node(self, _):
                pass
            def spin_once(self, **_):
                pass
            def shutdown(self):
                pass
        class FixtureNode(FakeNode):
            def __init__(self, name, **_):
                super().__init__(FakeSubscription(enabled=False))
                self.name = name
                self.subscription.topic_name = '/topic'
                self.subscription.get_publisher_count = lambda: 7
            def get_publishers_info_by_topic(self, _):
                return []
            def get_subscriptions_info_by_topic(self, _):
                policy = lambda name: SimpleNamespace(name=name)
                duration = SimpleNamespace(nanoseconds=tw.INFINITE_NS)
                qos = SimpleNamespace(reliability=policy('RELIABLE'), durability=policy('VOLATILE'),
                                      depth=16, history=policy('KEEP_LAST'), deadline=duration,
                                      liveliness=policy('AUTOMATIC'), liveliness_lease_duration=duration)
                return [SimpleNamespace(node_name=self.name, node_namespace='/',
                        topic_type='std_msgs/msg/String', endpoint_gid=b'\xaa\xbb', qos_profile=qos)]
            def destroy_node(self):
                pass
        modules['rclpy.context'].Context = FixtureContext
        modules['rclpy.executors'].SingleThreadedExecutor = FixtureExecutor
        modules['rclpy.node'].Node = FixtureNode
        modules['rclpy.qos'].QoSProfile = lambda **kwargs: kwargs
        modules['rclpy.subscription_content_filter_options'].ContentFilterOptions = FakeOptions
        modules['std_msgs.msg'].String = type('FixtureString', (), {})
        config = {'run_id': 'offline-worker-fixture', 'domain_id': 191, 'logical_id': 'r0',
                  'generation': 1, 'topic': '/topic', 'type_name': 'std_msgs/msg/String',
                  'qos': {}, 'initial_content_filter': initial(evaluation=True)}
        real_helper = tw._create_reader_with_initial_filter
        def fixture_helper(*args, **kwargs):
            return real_helper(*args, fixture=True, **kwargs)
        stdout = io.StringIO()
        with patch.dict(sys.modules, modules), patch.object(tw, '_public_qos_kwargs', return_value={}), \
                patch.object(tw, 'selectors_for_stdin', return_value=True), \
                patch.object(tw.sys, 'stdin', SimpleNamespace(fileno=lambda: 12345)), \
                patch.object(tw.os, 'read', return_value=b'{"command":"stop"}\n'), \
                patch.object(tw, '_create_reader_with_initial_filter', side_effect=fixture_helper), \
                redirect_stdout(stdout):
            code = tw._worker_main(['fixture', json.dumps(config), 'reader'])
        events = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(code, 0)
        result = next(e for e in events if e['event'] == 'content_filter_initial_configuration')
        self.assertEqual((result['role'], result['logical_id'], result['generation']), ('reader', 'r0', 1))
        self.assertTrue(result['filter_receipt']['fixture'])
        self.assertFalse(result['filter_receipt']['is_cft_enabled'])
        self.assertFalse(result['filter_receipt']['configured'])  # Actual API mismatch, not evaluation.
        self.assertTrue(any(e['event'] == 'endpoint_created' for e in events))
        self.assertEqual(next(e['matched_count'] for e in events if e['event'] == 'match'), 7)
        self.assertFalse(any(e['event'] == 'worker_error' for e in events))


class InitialFilterReadback(unittest.TestCase):
    def observe(self, subscription):
        adapter = fb.ContentFilterAdapter()
        adapter.attach(subscription, created_with={'expression': 'data = %0',
                                                  'parameters': ['keep'], 'enabled': True})
        return adapter.initial_readback(fixture=True)

    def test_unattached_adapter_does_not_invent_a_submitted_creation(self):
        result = fb.ContentFilterAdapter().initial_readback(fixture=True)
        self.assertFalse(result['submitted'])
        self.assertFalse(result['accepted'])
        self.assertIsNone(result['configured'])
        self.assertEqual(result['reason'], 'no_attached_subscription')

    def test_missing_getter_preserves_independent_enabled_observation(self):
        result = self.observe(SimpleNamespace(is_cft_enabled=True))
        self.assertEqual(result['api_results']['get_content_filter']['status'], 'unavailable_api')
        self.assertTrue(result['is_cft_enabled'])
        self.assertIsNone(result['observed'])
        self.assertIsNone(result['configured'])
        self.assertEqual(result['configuration_status'], 'unverified')

    def test_failed_getter_still_observes_enabled_flag_with_accurate_error_layer(self):
        for error, layer in [(RCLError('fixture read failure'), 'submission_rcl'),
                             (RuntimeError('fixture Python failure'), 'submission_api')]:
            result = self.observe(FakeSubscription(enabled=False, fail_get=error))
            self.assertEqual(result['api_results']['get_content_filter']['layer'], layer)
            self.assertFalse(result['is_cft_enabled'])
            self.assertIsNone(result['configured'])
            self.assertEqual(result['configuration_status'], 'unverified')

    def test_missing_or_failed_enabled_property_does_not_fill_from_input(self):
        class FailedEnabled(FakeSubscription):
            @property
            def is_cft_enabled(self):
                raise RCLError('fixture enabled read failure')
        for subscription, status in [(SimpleNamespace(get_content_filter=lambda: FakeOptions('data = %0', ['keep'])),
                                      'unavailable_api'), (FailedEnabled(), 'failed')]:
            result = self.observe(subscription)
            self.assertEqual(result['api_results']['is_cft_enabled']['status'], status)
            self.assertIsNone(result['is_cft_enabled'])
            self.assertEqual(result['observed'], {'expression': 'data = %0', 'parameters': ['keep']})
            self.assertIsNone(result['configured'])

    def test_malformed_or_missing_readback_fields_are_not_defaulted_to_empty_parameters(self):
        for raw in [None, {}, {'filter_expression': 'data = %0'},
                    {'filter_expression': 1, 'expression_parameters': []},
                    {'filter_expression': 'data = %0', 'expression_parameters': 'keep'}]:
            result = self.observe(SimpleNamespace(get_content_filter=lambda: raw, is_cft_enabled=True))
            self.assertEqual(result['api_results']['get_content_filter']['status'], 'invalid_result')
            self.assertIsNone(result['configured'])
            self.assertEqual(result['configuration_status'], 'unverified')
        result = self.observe(SimpleNamespace(get_content_filter=lambda: {'filter_expression': ''},
                                             is_cft_enabled=False))
        self.assertIsNone(result['observed']['parameters'])

    def test_non_boolean_enabled_flag_stays_unknown_not_truthy(self):
        for enabled in [None, 0, 1, 'false']:
            result = self.observe(FakeSubscription(enabled=enabled))
            self.assertIsNone(result['is_cft_enabled'])
            self.assertIsNone(result['configured'])
            self.assertEqual(result['api_results']['is_cft_enabled']['status'], 'invalid_result')
            self.assertEqual(result['api_results']['is_cft_enabled']['raw_result'], enabled)

    def test_offline_backend_never_models_filter_effect_or_derives_unmatch_from_evaluation(self):
        records = []
        backend = tw.OfflineTopologyBackend(event_sink=lambda event, **fields:
                                            records.append({'event': event, **fields}))
        writer = backend.create_endpoint(spec('writer'), 1, 'writer')
        reader = backend.create_endpoint(spec(filter_value=initial(evaluation=False)), 1, 'reader')
        backend.publish_batch(writer, [0, 1, 2], 0, 'fixture')
        self.assertTrue(backend.reader_sees[('w0', 'r0')])
        self.assertEqual([r['send_sequence'] for r in records if r['event'] == 'receive'], [0, 1, 2])
        value = reader['initial_content_filter']
        self.assertTrue(value['fixture'])
        self.assertFalse(value['real_ros'])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['execution_evidence'])
        self.assertIsNone(value['configured'])
        self.assertIsNone(value['observed'])
        self.assertIsNone(value['is_cft_enabled'])
        self.assertEqual(value['configuration_status'], 'unverified')
        self.assertIsNone(value['filter_effect_observed'])

    def test_filter_presence_including_empty_disabled_cft_does_not_create_all_send_delivery_obligation(self):
        from r2f.campaign.topology_adapter import _pair_facts
        from r2f.modules.s1.topology_input import (
            TOPOLOGY_TARGET, TOPOLOGY_FACT_PRODUCER, TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY,
        )
        from r2f.modules.s1.topology_oracles import FACTS_SCHEMA, evaluate_topology
        # Pure oracle inputs, explicitly fixtures, NOT hand-repaired raw execution.
        # Hold other reliable/window premises true to isolate the filter_free gate.
        graphs = [{'event': 'graph_snapshot',
                   'publishers': [{'endpoint_gid': 'aa', 'qos': {'reliability': 'reliable',
                                  'history': 'keep_last', 'depth': 16}}],
                   'subscriptions': [{'endpoint_gid': 'bb', 'qos': {'reliability': 'reliable',
                                     'history': 'keep_last', 'depth': 16}}]}] * 2
        proof = {'refs': [], 'detail': {'expected': [0, 1, 2], 'missing': [1]}}
        projection = {
            'receipt': {'after': {'component_refs': {
                '/pairs/w0/r0/compatibility': {'refs': [0, 1]},
                '/pairs/w0/r0/delivery_closure': proof},
                'identity': {'w0': {'endpoint_gid': 'aa', 'generation': 1},
                             'r0': {'endpoint_gid': 'bb', 'generation': 1}},
                'boundary_ref': {'event_index': 10}}},
            'state_after': {'pairs': [{'writer': 'w0', 'reader': 'r0',
                'compatibility': 'compatible', 'observed_match': 'bidirectional',
                'delivery_closure': 'closed_with_gap'}]},
        }
        for value in [initial(evaluation=False), initial('', [], evaluation=True)]:
            audit = SimpleNamespace(specs={'w0': spec('writer'), 'r0': spec(filter_value=value)},
                                    resolve=lambda ref: graphs[ref], events=[])
            pair = _pair_facts(audit, projection, [])[0]
            self.assertFalse(pair['filter_free'])
            self.assertTrue(pair['reliable_history_contract'])
            pair['delivery_window_identity_complete'] = True  # Pure oracle premise only.
            facts = {'schema': FACTS_SCHEMA, 'producer_id': TOPOLOGY_FACT_PRODUCER,
                     'target_id': TOPOLOGY_TARGET, 'fact_values': {}, 'diagnostic_valid': True,
                     'endpoint_identity_complete': True, 'fixture': True, 'pairs': [pair]}
            scenario = SimpleNamespace(target_id=TOPOLOGY_TARGET, arm='mutated')
            result = evaluate_topology(scenario, facts)
            self.assertEqual(result['verdict'], 'unknown')
            self.assertTrue(result['fact_values'][TOPOLOGY_FACT_GRAPH])
            self.assertIsNone(result['fact_values'][TOPOLOGY_FACT_DELIVERY])
            self.assertEqual(result['forbidden_residuals'], [])
            self.assertFalse(result['candidate_eligible'])
            # A paired control shows this tests the filter gate, not a missing
            # reliable/window fixture prerequisite or an unconditional unknown.
            unfiltered = deepcopy(facts)
            unfiltered['pairs'][0]['filter_free'] = True
            self.assertEqual(evaluate_topology(scenario, unfiltered)['verdict'], 'fail')


if __name__ == '__main__':
    unittest.main()
