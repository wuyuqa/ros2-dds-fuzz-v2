"""Offline contracts for the exploratory multi-window drop->restore chain.

V15/V16/V17 from docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md.
Nothing here applies an nft rule, opens a namespace, or starts ROS: the
receipts are fixture dictionaries and the assertions are contract checks.
"""
import json
import unittest

from r2f.runtime import discovery_network as network
from r2f.runtime.discovery_network import (
    MAX_RUNTIME_WINDOWS, WindowGateError, WindowLedger, WindowPlanError,
    validate_window_plan, window_counter_delta,
)
from r2f.runtime.namespace_pair import normalize_discovery_config
from r2f.runtime.ros2_pair import normalize_config


def window(index, duration=0.2, **overrides):
    record = {'window_id': 'w%d' % index, 'window_index': index,
              'trigger_anchor': 'fresh_match' if index == 0 else 'previous_restore_confirmed',
              'trigger_offset_sec': 0.0, 'drop_duration_sec': duration,
              'restore_interval_sec': 0.02}
    record.update(overrides)
    return record


def begin_receipt(table, packets=0, byte_count=0, index=0):
    return {'event': 'discovery_drop_begin', 'table': table, 'window_index': index,
            'applied': True, 'configured': True, 'hit': packets > 0,
            'counter_snapshot': {'packets': packets, 'bytes': byte_count}}


def end_receipt(table, packets=0, byte_count=0, index=0):
    return {'event': 'discovery_drop_end', 'table': table, 'window_index': index,
            'restored': True, 'hit': packets > 0,
            'counter_snapshot': {'packets': packets, 'bytes': byte_count}}


def recovery_receipt():
    return {'event': 'discovery_recovery_confirmed', 'recovery_verified': True,
            'evidence_kind': 'caller_independent_activity', 's1_execution': False,
            'requires_runtime_semantic_adjudication': True}


class WindowPlanTests(unittest.TestCase):
    def test_one_to_three_windows_accepted(self):
        for count in (1, 2, 3):
            plan = validate_window_plan([window(i) for i in range(count)])
            self.assertEqual(len(plan), count)
            self.assertEqual([w['window_index'] for w in plan], list(range(count)))
            self.assertEqual(MAX_RUNTIME_WINDOWS, 3)

    def test_zero_and_four_windows_rejected(self):
        for windows in ([], [window(i) for i in range(4)]):
            with self.subTest(count=len(windows)), self.assertRaises(WindowPlanError):
                validate_window_plan(windows)

    def test_first_window_must_anchor_on_fresh_match(self):
        with self.assertRaisesRegex(WindowPlanError, 'first window'):
            validate_window_plan([window(0, trigger_anchor='previous_restore_confirmed')])
        with self.assertRaisesRegex(WindowPlanError, 'unsupported window trigger_anchor'):
            validate_window_plan([window(0, trigger_anchor='post_delivery')])

    def test_window_index_and_ids_are_unambiguous(self):
        with self.assertRaisesRegex(WindowPlanError, 'sequential'):
            validate_window_plan([window(0), window(1, window_index=0)])
        with self.assertRaisesRegex(WindowPlanError, 'duplicate runtime window_id'):
            validate_window_plan([window(0), window(1, window_id='w0')])

    def test_unknown_or_invalid_fields_rejected(self):
        with self.assertRaisesRegex(WindowPlanError, 'unknown runtime window field'):
            validate_window_plan([dict(window(0), injected='x')])
        for bad in ({'drop_duration_sec': 0.0}, {'drop_duration_sec': -1.0},
                    {'drop_duration_sec': True}, {'drop_duration_sec': float('nan')},
                    {'drop_duration_sec': 31.0}, {'restore_interval_sec': 31.0},
                    {'trigger_offset_sec': 31.0}):
            with self.subTest(bad=bad), self.assertRaises(WindowPlanError):
                validate_window_plan([dict(window(0), **bad)])

    def test_plan_does_not_claim_execution(self):
        plan = validate_window_plan([window(0), window(1)])
        encoded = json.dumps(plan)
        for forbidden in ('applied', 'hit', 'restored', 'evidence'):
            self.assertNotIn(forbidden, encoded)


class CounterDeltaTests(unittest.TestCase):
    """V15: each window's hit is its own delta, never a running total."""

    def test_second_window_does_not_inherit_first_window_packets(self):
        ledger = WindowLedger([window(0), window(1)])
        ledger.start(0, begin_receipt('t1', 0, 0))
        first = ledger.end(0, end_receipt('t1', 4, 256))
        self.assertEqual(first['packets_delta'], 4)
        self.assertTrue(first['hit'])
        ledger.confirm_recovery(0, recovery_receipt())

        # New table, new counter: zero packets in window 2 despite window 1's 4.
        ledger.start(1, begin_receipt('t2', 0, 0, index=1))
        second = ledger.end(1, end_receipt('t2', 0, 0, index=1))
        self.assertEqual(second['packets_delta'], 0)
        self.assertFalse(second['hit'])
        self.assertEqual(second['counter_semantics'], 'per_window_delta_not_cumulative')

        summary = ledger.summary()
        self.assertTrue(summary['hit'])  # window 1 did hit
        self.assertEqual([w['counter']['packets_delta'] for w in summary['windows']], [4, 0])
        # Verified restore alone is not the end of the chain: window 2 still
        # needs its own post-restore recovery confirmation.
        self.assertFalse(summary['chain_complete'])
        ledger.confirm_recovery(1, recovery_receipt())
        self.assertTrue(ledger.summary()['chain_complete'])

    def test_cumulative_style_table_reuse_is_rejected(self):
        with self.assertRaisesRegex(network.DiscoveryNetworkError, 'different window tables'):
            window_counter_delta(begin_receipt('t1', 2, 10), end_receipt('t2', 6, 40))

    def test_counter_reset_is_not_attributed_to_window(self):
        with self.assertRaisesRegex(network.DiscoveryNetworkError, 'reset'):
            window_counter_delta(begin_receipt('t1', 5, 50), end_receipt('t1', 2, 20))

    def test_missing_or_malformed_counters_fail_closed(self):
        for before, after in (({'table': 't1'}, end_receipt('t1', 1, 1)),
                              (begin_receipt('t1', 0, 0), {'table': 't1', 'restored': True}),
                              (begin_receipt('t1', 0, 0),
                               end_receipt('t1', -1, 0))):
            with self.subTest(before=before), self.assertRaises(network.DiscoveryNetworkError):
                window_counter_delta(before, after)


class ChainGateTests(unittest.TestCase):
    """V16: verified deletion alone never opens the next window."""

    def test_next_window_blocked_without_recovery_confirmation(self):
        ledger = WindowLedger([window(0), window(1)])
        ledger.start(0, begin_receipt('t1'))
        ledger.end(0, end_receipt('t1'))
        with self.assertRaisesRegex(WindowGateError, 'previous window restore is not confirmed'):
            ledger.start(1, begin_receipt('t2', index=1))

    def test_restore_without_activity_leaves_window_unconfirmed(self):
        ledger = WindowLedger([window(0), window(1)])
        ledger.start(0, begin_receipt('t1'))
        ledger.end(0, end_receipt('t1'))
        with self.assertRaisesRegex(WindowGateError, 'recovery_verified receipt required'):
            ledger.confirm_recovery(0, {'event': 'discovery_drop_end', 'restored': True})
        with self.assertRaisesRegex(WindowGateError, 'previous window restore is not confirmed'):
            ledger.start(1, begin_receipt('t2', index=1))

    def test_confirmed_recovery_permits_next_window(self):
        ledger = WindowLedger([window(0), window(1)])
        ledger.start(0, begin_receipt('t1'))
        ledger.end(0, end_receipt('t1', 1, 10))
        ledger.confirm_recovery(0, recovery_receipt())
        record = ledger.start(1, begin_receipt('t2', index=1))
        self.assertEqual(record['window_index'], 1)
        ledger.end(1, end_receipt('t2', 0, 0, index=1))
        ledger.confirm_recovery(1, recovery_receipt())
        self.assertTrue(ledger.summary()['chain_complete'])

    def test_out_of_order_and_double_close_rejected(self):
        ledger = WindowLedger([window(0), window(1)])
        with self.assertRaisesRegex(WindowGateError, 'sequentially'):
            ledger.start(1, begin_receipt('t2', index=1))
        ledger.start(0, begin_receipt('t1'))
        with self.assertRaisesRegex(WindowGateError, 'without matching open window'):
            ledger.end(1, end_receipt('t2', index=1))
        ledger.end(0, end_receipt('t1'))
        with self.assertRaisesRegex(WindowGateError, 'without matching open window'):
            ledger.end(0, end_receipt('t1'))

    def test_unverified_restore_cannot_close_window(self):
        ledger = WindowLedger([window(0)])
        ledger.start(0, begin_receipt('t1'))
        with self.assertRaisesRegex(WindowGateError, 'verified restore receipt required'):
            ledger.end(0, {'event': 'discovery_drop_end', 'restored': False})

    def test_begin_must_be_applied(self):
        ledger = WindowLedger([window(0)])
        with self.assertRaisesRegex(WindowGateError, 'applied drop begin receipt required'):
            ledger.start(0, dict(begin_receipt('t1'), applied=False))

    def test_aborted_chain_refuses_further_windows(self):
        ledger = WindowLedger([window(0), window(1)])
        ledger.start(0, begin_receipt('t1'))
        ledger.end(0, end_receipt('t1'))
        ledger.confirm_recovery(0, recovery_receipt())
        abort = ledger.abort('observer_stopped')
        self.assertTrue(abort['cleanup_required'])
        self.assertTrue(abort['rules_must_be_removed'])
        with self.assertRaisesRegex(WindowGateError, 'already aborted'):
            ledger.start(1, begin_receipt('t2', index=1))
        self.assertFalse(ledger.summary()['chain_complete'])


class SupervisorChainTests(unittest.TestCase):
    """V17: an exception in a later window still restores and keeps the cause."""

    def test_carrier_restored_on_later_window_failure(self):
        class FakeCarrier:
            def __init__(self):
                self.calls = 0
                self.active = False
                self.restores = 0

            def start(self, *, match_receipt=None):
                self.calls += 1
                self.active = True
                if self.calls == 2:
                    raise RuntimeError('second_window_carrier_failure')
                return {'event': 'discovery_drop_begin', 'applied': True,
                        'configured': True, 'restored': False, 'hit': False,
                        'counter_snapshot': {'packets': 0, 'bytes': 0}, 'table': 't'}

            def restore(self):
                self.restores += 1
                self.active = False
                return {'event': 'discovery_drop_end', 'restored': True, 'hit': False,
                        'counter_snapshot': {'packets': 0, 'bytes': 0}, 'table': 't'}

        carrier = FakeCarrier()
        ledger = WindowLedger([window(0), window(1)])
        original_error = None
        try:
            try:
                ledger.start(0, carrier.start(match_receipt={}))
                ledger.end(0, carrier.restore())
                ledger.confirm_recovery(0, recovery_receipt())
                ledger.start(1, carrier.start(match_receipt={}))
            except RuntimeError as exc:
                original_error = str(exc)
        finally:
            if carrier.active:
                carrier.restore()
            ledger.abort(original_error or 'unknown')
        self.assertEqual(original_error, 'second_window_carrier_failure')
        self.assertFalse(carrier.active)
        self.assertEqual(carrier.restores, 2)
        summary = ledger.summary()
        self.assertFalse(summary['chain_complete'])
        self.assertEqual(summary['aborted'], 'second_window_carrier_failure')


class ConfigWiringTests(unittest.TestCase):
    def discovery_config(self, windows):
        return normalize_config({'run_id': 'offline-windows', 'domain_id': 173,
                                 'target_id': 'discovery.interruption_recovery.fuzz_v1',
                                 'carrier': 'wire_proxy', 'mutation_kind': 'discovery_drop',
                                 'fuzz_surfaces': ['E02'],
                                 'submitted_parameters': {
                                     'drop_duration_sec': 0.2,
                                     'environment_windows.count': len(windows),
                                     'environment_windows.restore_interval_sec': 0.02},
                                 'environment_windows': windows})

    def test_exploratory_windows_are_normalized_and_budgeted(self):
        c = self.discovery_config([window(0), window(1, 1.0)])
        # normalize_config validates the input plan only; chain activation and
        # supervisor budget belong to normalize_discovery_config.
        self.assertEqual(len(c['environment_windows']), 2)
        normalized = normalize_discovery_config(c)
        self.assertTrue(normalized['window_chain_requested'])
        self.assertEqual(normalized['window_budget_sec'],
                         (0.2 + 0.02) + (1.0 + 0.02) + 2 * normalized['recovery_timeout_sec'])

    def test_legacy_single_window_keeps_previous_shape(self):
        legacy = normalize_config({'run_id': 'offline-legacy', 'domain_id': 173,
                                   'target_id': 'discovery.interruption_recovery',
                                   'carrier': 'wire_proxy', 'mutation_kind': 'discovery_drop',
                                   'drop_duration_sec': 3.0})
        normalized = normalize_discovery_config(legacy)
        self.assertFalse(normalized['window_chain_requested'])
        self.assertEqual(len(normalized['runtime_windows']), 1)
        self.assertEqual(normalized['runtime_windows'][0]['drop_duration_sec'], 3.0)

    def test_windows_rejected_for_non_discovery_targets(self):
        with self.assertRaisesRegex(ValueError, 'environment_windows_only_supported'):
            normalize_config({'run_id': 'offline-wrong', 'domain_id': 173,
                              'target_id': 'discovery.late_endpoint',
                              'environment_windows': [window(0)]})
        with self.assertRaisesRegex(ValueError, 'environment_windows_only_supported'):
            normalize_config({'run_id': 'offline-proj', 'domain_id': 173,
                              'target_id': 'builtin.discovery_projection_reconciliation',
                              'carrier': 'ros2_endpoint',
                              'mutation_kind': 'endpoint_qos_variation',
                              'environment_windows': [window(0)]})

    def test_window_count_binding_mismatch_fails(self):
        c = self.discovery_config([window(0), window(1)])
        c['submitted_parameters'] = dict(c['submitted_parameters'],
                                         **{'environment_windows.count': 3})
        from r2f.runtime.ros2_pair import exploratory_bindings
        with self.assertRaisesRegex(ValueError, 'environment_window_count_not_preserved'):
            exploratory_bindings(c)


if __name__ == '__main__':
    unittest.main()
