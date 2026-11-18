"""Offline contracts for the public content-filter adapter.

    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover \
        -s tests -p test_filter_backend.py -v

No ROS import, no subscription, no participant: the adapter is exercised with
a fake endpoint that records the public calls it received.
"""
import unittest

from r2f.runtime import filter_backend as fb


class FakeOptions:
    def __init__(self, expression, parameters):
        self.filter_expression = expression
        self.expression_parameters = parameters


class FakeSubscription:
    def __init__(self, *, fail_set=None, fail_get=None, readback=None):
        self.expression = ''
        self.parameters = []
        self.calls = []
        self.fail_set = fail_set
        self.fail_get = fail_get
        self.readback = readback
        self.is_cft_enabled = False

    def set_content_filter(self, expression, parameters):
        self.calls.append(('set_content_filter', expression, list(parameters)))
        if self.fail_set is not None:
            raise self.fail_set
        self.expression = expression
        self.parameters = list(parameters)
        self.is_cft_enabled = bool(expression)

    def get_content_filter(self):
        self.calls.append(('get_content_filter',))
        if self.fail_get is not None:
            raise self.fail_get
        if self.readback is not None:
            return self.readback
        return FakeOptions(self.expression, self.parameters)


class ValidationTests(unittest.TestCase):
    def test_empty_expression_disables_filter_without_unmatch_claim(self):
        value = fb.validate_filter('   ', [])
        self.assertTrue(value['ok'])
        self.assertFalse(value['enabled'])
        self.assertEqual(value['disable_semantics'], 'empty_expression_disables_filter')
        self.assertFalse(value['filter_false_implies_unmatch'])
        self.assertIsNone(fb.content_filter_options('', []))

    def test_empty_expression_with_parameters_is_rejected(self):
        value = fb.validate_filter('', ['x'])
        self.assertFalse(value['ok'])
        self.assertEqual(value['layer'], 'parameter_unexpected')
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['sut_bug'])

    def test_expression_and_parameter_type_layers_are_distinct(self):
        self.assertEqual(fb.validate_filter(12)['layer'], 'expression_type')
        self.assertEqual(fb.validate_filter('data = %0', 'x')['layer'], 'parameter_type')
        self.assertEqual(fb.validate_filter('data = %0', [1])['layer'], 'parameter_type')

    def test_syntax_errors_are_layered_not_sut_bugs(self):
        for expression, reason in (('data =', 'operand_expected:None'),
                                   ('data %0', 'operator_expected:%0'),
                                   ('(data = %0', 'missing_rparen'),
                                   ('data = %0 extra', 'trailing_token:extra'),
                                   # '==' is not part of the DDS SQL subset; it
                                   # tokenizes as two comparison operators.
                                   ('data == %0', 'operand_expected:=')):
            with self.subTest(expression=expression):
                value = fb.validate_filter(expression, ['x'])
                self.assertFalse(value['ok'])
                self.assertEqual(value['layer'], 'expression_syntax')
                self.assertEqual(value['reason'], reason)
                self.assertFalse(value['sut_bug'])

    def test_parameter_index_out_of_range(self):
        value = fb.validate_filter('data = %1', ['x'])
        self.assertEqual(value['layer'], 'parameter_index')
        self.assertEqual(value['indices'], [1])
        good = fb.validate_filter('a = %0 AND b = %1', ['x', 'y'])
        self.assertEqual(good['parameter_indices'], [0, 1])
        self.assertEqual(fb.validate_filter('data = %0', ['x', 'y'])['unused_parameters'], [1])

    def test_type_checks_need_declared_field_types(self):
        value = fb.validate_filter('data > %0', ['1'], field_types={'data': 'string'})
        self.assertEqual(value['layer'], 'expression_type')
        self.assertEqual(fb.validate_filter('data > %0', ['1'])['ok'], True)

    def test_unsupported_field_and_feature_are_guard_receipts(self):
        unknown = fb.validate_filter('missing = %0', ['x'], field_types={'data': 'string'})
        self.assertEqual(unknown['layer'], 'unsupported_field')
        field_to_field = fb.validate_filter('a = b', [])
        self.assertEqual(field_to_field['layer'], 'unsupported_feature')
        indexed = fb.validate_filter('arr[0] = %0', ['x'])
        self.assertEqual(indexed['layer'], 'unsupported_feature')

    def test_validation_never_claims_conformance(self):
        value = fb.validate_filter('data = %0', ['x'])
        self.assertFalse(value['conformance_claim'])
        self.assertEqual(value['validation_scope'], 'harness_subset')
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['counts_toward_maturity'])


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.records = []
        self.adapter = fb.ContentFilterAdapter(emit=self.records.append)

    def test_update_records_readback_and_applied_flag(self):
        subscription = FakeSubscription()
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        value = self.adapter.update("data = 'y'", [])
        self.assertTrue(value['applied'])
        self.assertEqual(value['readback_status'], 'matched')
        self.assertEqual(value['stage'], 'applied')
        self.assertEqual(subscription.calls[-2], ('set_content_filter', "data = 'y'", []))
        self.assertFalse(value['hit'])
        self.assertFalse(value['mutation_receipt'])

    def test_disable_resets_expression_and_keeps_sample_and_graph_separate(self):
        subscription = FakeSubscription()
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        value = self.adapter.disable()
        self.assertTrue(value['applied'])
        self.assertEqual(subscription.calls[-2], ('set_content_filter', '', []))

    def test_readback_mismatch_is_reported_not_hidden(self):
        subscription = FakeSubscription(readback=FakeOptions('other', []))
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        value = self.adapter.update("data = 'y'", [])
        self.assertFalse(value['applied'])
        self.assertEqual(value['reason'], 'readback_mismatch')
        self.assertEqual(value['layer'], 'readback_mismatch')
        self.assertFalse(value['sut_bug'])

    def test_api_failure_layers(self):
        subscription = FakeSubscription(fail_set=RuntimeError('boom'))
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        value = self.adapter.update("data = 'y'", [])
        self.assertEqual(value['layer'], 'submission_api')
        self.assertFalse(value['applied'])

        class RclError(Exception):
            pass

        subscription = FakeSubscription(fail_get=RclError('rcl'))
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        value = self.adapter.readback()
        self.assertEqual(value['layer'], 'submission_rcl')
        self.assertFalse(value['candidate_eligible'])

    def test_no_subscription_is_a_precondition_guard(self):
        value = self.adapter.update("data = 'y'", [])
        self.assertEqual(value['reason'], 'no_attached_subscription')
        self.assertEqual(value['layer'], 'submission_api')
        self.assertFalse(value['accepted'])

    def test_invalid_update_does_not_reach_the_api(self):
        subscription = FakeSubscription()
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        before = len(subscription.calls)
        value = self.adapter.update('data =', [])
        self.assertEqual(value['layer'], 'expression_syntax')
        self.assertEqual(len(subscription.calls), before)

    def test_history_counts_no_mutation_and_no_candidate(self):
        subscription = FakeSubscription()
        self.adapter.create(subscription, "data = 'x'", [], options_factory=FakeOptions)
        self.adapter.update('data =', [])
        summary = fb.summarize_history(self.records)
        self.assertEqual(summary['mutations'], 0)
        self.assertEqual(summary['candidates'], 0)
        self.assertEqual(summary['guard_steps'], ['operand_expected:None'])


class ObservationTests(unittest.TestCase):
    def test_samples_and_match_graph_stay_separate(self):
        value = fb.summarize_observations(
            [{'sequence': 1}, {'sequence': 2}, {'sequence': 3}], [1, 3],
            match_before=1, match_after=1)
        self.assertEqual(value['delivered_sequences'], [1, 3])
        self.assertEqual(value['suppressed_sequences'], [2])
        self.assertFalse(value['filter_false_implies_unmatch'])
        self.assertTrue(value['cross_channel_inference_forbidden'])
        self.assertEqual(value['match_graph_channel']['inference'], 'match_graph_only')
        self.assertIsNone(value['filter_effect_observed'])

    def test_expected_map_produces_sample_only_oracle(self):
        value = fb.summarize_observations(
            [{'sequence': 1}, {'sequence': 2}], [2],
            expected={1: False, 2: True})
        self.assertTrue(value['filter_effect_observed'])
        self.assertEqual(value['oracle_scope'], 'sample_admission_only')
        wrong = fb.summarize_observations(
            [{'sequence': 1}, {'sequence': 2}], [1],
            expected={1: False, 2: True})
        self.assertFalse(wrong['filter_effect_observed'])
        self.assertEqual(wrong['unexpected_delivered'], [1])
        self.assertEqual(wrong['unexpected_suppressed'], [2])

    def test_unmatch_is_never_inferred_from_filter_state(self):
        value = fb.summarize_observations([{'sequence': 1}], [], match_before=1,
                                          match_after=1, expected={1: False})
        self.assertEqual(value['match_graph_channel']['after'], 1)
        self.assertTrue(value['filter_effect_observed'])


if __name__ == '__main__':
    unittest.main()
