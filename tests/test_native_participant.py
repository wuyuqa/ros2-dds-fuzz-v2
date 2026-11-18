"""Native CycloneDDS ctypes backend: ABI/provenance contracts + gated lifecycle.

    R2F_NATIVE_DDS_TESTS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover \
        -s tests -p test_native_participant.py -v

Default run is fully offline: library load + symbol/ABI identity only, no
participant.  Set R2F_NATIVE_DDS_TESTS=1 to also exercise a real bare DDS
participant create/delete in this process (non-ROS), which is still **not**
gated live ROS execution and is only a capability check.
"""
import ctypes
import os
import unittest
from unittest import mock

from r2f.runtime import native_participant as np


class AbiTests(unittest.TestCase):
    def test_declared_layout_matches_headers(self):
        # dds_sample_info_t is 64 bytes on this ABI with these offsets; a wrong
        # declaration would silently corrupt builtin samples.
        self.assertEqual(np.ABI_LAYOUT['dds_sample_info_t'], 64)
        self.assertEqual(np.ABI_LAYOUT['dds_sample_info_t_offsets'],
                         [0, 4, 8, 12, 16, 24, 32, 40, 44, 48, 52, 56])
        self.assertEqual(np.ABI_LAYOUT['dds_guid_t'], 16)
        self.assertEqual(np.ABI_LAYOUT['dds_builtintopic_participant_t'], 24)
        self.assertEqual(np.ABI_LAYOUT['dds_entity_t'], 4)
        self.assertEqual(len(np.abi_sha256()), 64)

    def test_constants_come_from_the_installed_header(self):
        self.assertEqual(np.DDS_DOMAIN_DEFAULT, 0xFFFFFFFF)
        self.assertEqual(np.BUILTIN_PARTICIPANT_TOPIC, 0x7FFF0001)
        self.assertEqual(sorted(np.INSTANCE_STATE), [0x10, 0x20, 0x40])
        self.assertEqual(np.INSTANCE_STATE[0x20], 'NOT_ALIVE_DISPOSED')
        self.assertEqual(np.INSTANCE_STATE[0x40], 'NOT_ALIVE_NO_WRITERS')

    def test_provenance_has_no_compiled_artifact(self):
        value = np.provenance()
        self.assertEqual(value['backend'], 'ctypes-cyclonedds')
        self.assertFalse(value['build']['compiler_required'])
        self.assertIsNone(value['build']['binary'])
        self.assertIsNone(value['build']['build_root'])
        headers = value['abi']['source_headers']
        self.assertEqual(sorted(headers), ['dds.h', 'dds_public_impl.h', 'dds_public_qos.h'])
        self.assertTrue(all(len(digest) == 64 for digest in headers.values()))
        self.assertEqual(len(value['module_sha256']), 64)


class CapabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = np.capability()

    def test_capability_is_observed_without_entities(self):
        self.assertTrue(self.value['ok'], self.value)
        self.assertEqual(self.value['capability_status'], 'observed')
        self.assertFalse(self.value['participant_created'])
        self.assertFalse(self.value['ros_context_created'])
        self.assertEqual(self.value['live_execution'], 'not_run_no_live_go')
        self.assertFalse(self.value['counts_toward_maturity'])

    def test_library_identity_and_symbols(self):
        self.assertTrue(self.value['available'])
        self.assertTrue(self.value['library'].endswith('libddsc.so.0.10.5'))
        self.assertEqual(len(self.value['library_sha256']), 64)
        self.assertEqual(sorted(self.value['symbols']), sorted(np.REQUIRED_SYMBOLS))
        preloads = {item['name'] for item in self.value['preloads']}
        self.assertTrue({'libiceoryx_hoofs.so', 'libiceoryx_binding_c.so'} <= preloads)

    def test_missing_library_is_a_capability_gap(self):
        value = np.capability('/nonexistent/libddsc.so.0')
        self.assertFalse(value['ok'])
        self.assertEqual(value['capability_status'], 'capability_gap')
        self.assertEqual(value['layer'], 'capability_gap')
        self.assertFalse(value['participant_created'])

    def test_summary_keeps_probe_non_evidence(self):
        summary = np.summarize_capability(self.value)
        self.assertFalse(summary['counts_toward_maturity'])
        self.assertFalse(summary['ros_context_created'])
        self.assertFalse(summary['participant_created'])


class GateTests(unittest.TestCase):
    def setUp(self):
        self.backend = np.NativeParticipantBackend()

    def test_disabled_by_default(self):
        for config in ({}, {'enabled': False}, {'enabled': 1}, None, []):
            with self.subTest(config=config):
                value = self.backend.run(config)
                self.assertFalse(value['real_execution'])
                self.assertFalse(value['participant_created'])
                self.assertIsNone(self.backend.library)

    def test_live_authorization_is_required_even_when_enabled(self):
        value = self.backend.run({'enabled': True})
        self.assertEqual(value['reason'], 'live_authorization_required')
        self.assertEqual(value['live_execution'], 'not_run_no_live_go')
        self.assertIsNone(self.backend.library)

    def test_library_identity_must_match_authorization(self):
        value = self.backend.run({'enabled': True,
                                  'live_authorization': {'granted': True,
                                                         'library_sha256': 'b' * 64}})
        self.assertEqual(value['reason'], 'library_identity_mismatch')
        self.assertEqual(value['layer'], 'identity')
        self.assertIsNone(self.backend.library)

    def test_guard_receipts_are_never_sut_bugs(self):
        value = self.backend.run({'enabled': True})
        self.assertFalse(value['sut_bug'])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['mutation_receipt'])
        self.assertFalse(value['counts_toward_maturity'])


class BuiltinLoanTests(unittest.TestCase):
    """Only Python-owned ctypes fixtures and mocks; never create DDS entities."""

    def setUp(self):
        self.records = []
        self.backend = np.NativeParticipantBackend(emit=self.records.append)
        self.library = mock.Mock()
        self.backend.library = self.library
        self.reader = 17
        self.take_rc = 2
        self.native_samples = [np._BuiltinParticipant() for _ in range(2)]
        for index, sample in enumerate(self.native_samples):
            sample.key.v[:] = bytes([index + 1]) * 16
        self.buffers = []
        self.library.dds_take.side_effect = self.fake_take
        self.library.dds_return_loan.return_value = 0

    def fake_take(self, reader, buffer, info, capacity, maximum):
        self.assertEqual(reader.value, self.reader)
        self.assertEqual(capacity.value, len(buffer))
        self.assertEqual(maximum.value, len(buffer))
        self.buffers.append(buffer)
        if self.take_rc > 0:
            count = min(self.take_rc, len(buffer))
            for index, sample in enumerate(self.native_samples[:count]):
                buffer[index] = ctypes.addressof(sample)
                info[index].instance_state = 0x10 if index == 0 else 0x20
                info[index].valid_data = index == 0
                info[index].source_timestamp = 123 + index
        return self.take_rc

    def tearDown(self):
        # These are reader-owned loans, not standalone get_builtin allocations.
        self.library.dds_builtintopic_free_participant.assert_not_called()
        self.library.dds_delete.assert_not_called()

    def assert_returned_once(self, count):
        self.library.dds_return_loan.assert_called_once()
        reader, buffer, actual_count = self.library.dds_return_loan.call_args.args
        self.assertEqual(reader.value, self.reader)
        self.assertIs(buffer, self.buffers[0])
        self.assertEqual(actual_count.value, count)

    def assert_blocked(self, value, reason):
        self.assertEqual(value['receipt_kind'], 'native_blocked')
        self.assertEqual(value['reason'], reason)
        self.assertEqual(value['stage'], 'aborted')
        self.assertEqual(value['responsibility_layer'], 'harness')
        self.assertFalse(value['ok'])
        self.assertFalse(value['accepted'])
        self.assertFalse(value['effect_observed'])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['sut_bug'])
        self.assertFalse(value['counts_toward_maturity'])
        self.assertEqual(self.records, [value])

    def test_success_returns_loan_without_manual_free(self):
        value = self.backend.take_participants(self.reader, max_samples=2)
        self.assertEqual(value['receipt_kind'], 'native_builtin_participants')
        self.assertTrue(value['ok'])
        self.assertTrue(value['accepted'])
        self.assertEqual(value['stage'], 'effect_observed')
        self.assertTrue(value['effect_observed'])
        self.assertEqual(value['sample_count'], 2)
        self.assertEqual(value['samples'][0], {
            'guid': '01' * 16, 'instance_state': 'ALIVE', 'valid_data': True,
            'source_timestamp_ns': 123, 'observation': 'builtin_participant_sample'})
        self.assertEqual(value['samples'][1]['guid'], '02' * 16)
        self.assertEqual(value['samples'][1]['instance_state'], 'NOT_ALIVE_DISPOSED')
        self.assertFalse(value['samples'][1]['valid_data'])
        self.assertTrue(value['loan_returned'])
        self.assertTrue(value['loan_return_attempted'])
        self.assertTrue(value['loan_cleanup_complete'])
        self.assertFalse(value['loan_cleanup_uncertain'])
        self.assertIsNone(value['abi_contract_violation'])
        self.assertEqual(value['return_loan_code'], 0)
        self.assertEqual(value['free_errors'], [])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['counts_toward_maturity'])
        self.assert_returned_once(2)

    def test_field_parse_failure_returns_loan_and_blocks_partial_observation(self):
        for good_count in (0, 1):
            with self.subTest(good_count=good_count):
                self.library.dds_return_loan.reset_mock()
                self.records.clear()
                self.buffers.clear()
                values = ['01' * 16] * good_count + [ValueError('offline conversion fault')]
                with mock.patch.object(np, '_guid_text', side_effect=values):
                    value = self.backend.take_participants(self.reader)
                self.assert_returned_once(2)
                self.assert_blocked(value, 'builtin_sample_parse_failed')
                self.assertNotIn('return_code', value)
                self.assertEqual(value['sample_count'], good_count)
                self.assertEqual(value['parse_error'], {
                    'sample_index': good_count, 'error': 'ValueError:offline conversion fault'})
                self.assertTrue(value['loan_returned'])
                self.assertTrue(value['loan_cleanup_complete'])
                self.assertEqual(value['free_errors'], [])

    def test_parse_and_return_failure_preserve_both_errors(self):
        self.library.dds_return_loan.side_effect = RuntimeError('offline return fault')
        with mock.patch.object(np, '_guid_text', side_effect=ValueError('offline parse fault')):
            value = self.backend.take_participants(self.reader)
        self.assert_returned_once(2)
        self.assert_blocked(value, 'builtin_sample_parse_failed')
        self.assertIn('offline parse fault', value['parse_error']['error'])
        self.assertEqual(value['free_errors'], ['return_loan:RuntimeError:offline return fault'])
        self.assertFalse(value['loan_returned'])
        self.assertFalse(value['loan_cleanup_complete'])

    def test_return_exception_blocks_effect(self):
        self.library.dds_return_loan.side_effect = RuntimeError('offline return fault')
        value = self.backend.take_participants(self.reader)
        self.assert_returned_once(2)
        self.assert_blocked(value, 'builtin_return_loan_failed')
        self.assertEqual(value['take_return_code'], 2)
        self.assertIsNone(value['return_loan_code'])
        self.assertTrue(value['loan_return_attempted'])
        self.assertFalse(value['loan_returned'])
        self.assertFalse(value['loan_cleanup_complete'])
        self.assertEqual(value['free_errors'], ['return_loan:RuntimeError:offline return fault'])

    def test_nonzero_return_codes_block_effect_without_retry_or_manual_free(self):
        for rc in (-1, -3, 1):
            with self.subTest(return_code=rc):
                self.library.dds_return_loan.reset_mock()
                self.library.dds_return_loan.return_value = rc
                self.records.clear()
                self.buffers.clear()
                value = self.backend.take_participants(self.reader)
                self.assert_returned_once(2)
                self.assert_blocked(value, 'builtin_return_loan_failed')
                self.assertEqual(value['return_loan_code'], rc)
                self.assertFalse(value['loan_returned'])
                self.assertFalse(value['loan_cleanup_complete'])
                self.assertEqual(value['free_errors'], ['return_loan:return_code=' + str(rc)])

    def test_take_negative_is_failure_without_returning_nonexistent_loan(self):
        self.take_rc = -3
        with mock.patch.object(np, '_guid_text') as parse:
            value = self.backend.take_participants(self.reader)
        self.assert_blocked(value, 'builtin_take_failed')
        self.assertEqual(value['return_code'], -3)
        self.assertEqual(value['take_return_code'], -3)
        self.assertTrue(value['real_execution'])
        self.assertFalse(value['loan_return_attempted'])
        self.assertTrue(value['loan_cleanup_complete'])
        parse.assert_not_called()
        self.library.dds_return_loan.assert_not_called()

    def test_take_exception_is_receipted_without_inventing_a_loan(self):
        self.library.dds_take.side_effect = RuntimeError('offline take fault')
        value = self.backend.take_participants(self.reader)
        self.assert_blocked(value, 'builtin_take_failed')
        self.assertEqual(value['take_error'], 'RuntimeError:offline take fault')
        self.assertIsNone(value['take_return_code'])
        self.assertFalse(value['loan_return_attempted'])
        self.library.dds_return_loan.assert_not_called()

    def test_zero_take_is_empty_observation_not_effect_or_loan(self):
        self.take_rc = 0
        with mock.patch.object(np, '_guid_text') as parse:
            value = self.backend.take_participants(self.reader)
        self.assertEqual(value['receipt_kind'], 'native_builtin_participants')
        self.assertTrue(value['ok'])
        self.assertEqual(value['stage'], 'carrier_admitted')
        self.assertFalse(value['effect_observed'])
        self.assertEqual(value['sample_count'], 0)
        self.assertEqual(value['samples'], [])
        self.assertIsNone(value['return_loan_code'])
        self.assertFalse(value['loan_return_attempted'])
        self.assertFalse(value['loan_returned'])
        self.assertTrue(value['loan_cleanup_complete'])
        parse.assert_not_called()
        self.library.dds_return_loan.assert_not_called()

    def test_capacity_type_rejected_before_allocation_and_native_calls(self):
        for capacity in (True, False, None, 1.0, '1', [], {}, ctypes.c_uint32(1)):
            with self.subTest(capacity=repr(capacity)):
                self.records.clear()
                value = self.backend.take_participants(self.reader, max_samples=capacity)
                self.assert_blocked(value, 'max_samples_must_be_integer')
                self.assertEqual(value['layer'], 'config_type')
                self.assertFalse(value['real_execution'])
        self.library.dds_take.assert_not_called()
        self.library.dds_return_loan.assert_not_called()

    def test_capacity_range_rejected_before_allocation_and_native_calls(self):
        for capacity in (0, -1, 1025, 2**32, 2**100):
            with self.subTest(capacity=capacity):
                self.records.clear()
                value = self.backend.take_participants(self.reader, max_samples=capacity)
                self.assert_blocked(value, 'max_samples_out_of_range')
                self.assertEqual(value['layer'], 'config_value')
                self.assertEqual(value['max_samples_limit'], 1024)
        self.library.dds_take.assert_not_called()
        self.library.dds_return_loan.assert_not_called()

    def test_capacity_boundaries_and_default_are_forwarded_exactly(self):
        for capacity in (1, 32, 1024):
            with self.subTest(capacity=capacity):
                self.take_rc = 1
                self.buffers.clear()
                self.library.dds_return_loan.reset_mock()
                value = (self.backend.take_participants(self.reader) if capacity == 32
                         else self.backend.take_participants(self.reader, max_samples=capacity))
                self.assertEqual(len(self.buffers[0]), capacity)
                self.assertTrue(value['ok'])
                self.assert_returned_once(1)

    def test_oversized_take_count_never_parses_or_returns_past_capacity(self):
        for count in (3, 2**31 - 1, 2**100):
            with self.subTest(take_return_code=count):
                self.take_rc = count
                self.records.clear()
                self.buffers.clear()
                self.library.dds_return_loan.reset_mock()
                with mock.patch.object(np, '_guid_text') as parse:
                    value = self.backend.take_participants(self.reader, max_samples=2)
                self.assert_blocked(value, 'builtin_take_count_out_of_range')
                self.assertEqual(value['take_return_code'], count)
                self.assertEqual(value['sample_count'], 0)
                self.assertEqual(value['loan_return_count'], 2)
                self.assertEqual(value['layer'], 'abi_contract')
                self.assertEqual(value['abi_contract_violation'],
                                 'dds_take_count_exceeds_capacity')
                self.assertEqual(value['loan_return_count_basis'],
                                 'capacity_bound_after_abi_violation')
                self.assertEqual(value['return_loan_code'], 0)
                self.assertTrue(value['loan_cleanup_uncertain'])
                self.assertFalse(value['loan_returned'])
                self.assertFalse(value['loan_cleanup_complete'])
                self.assert_returned_once(2)
                parse.assert_not_called()

    def test_oversized_take_and_failed_return_preserve_both_failures(self):
        self.take_rc = 2**100
        self.library.dds_return_loan.return_value = -3
        with mock.patch.object(np, '_guid_text') as parse:
            value = self.backend.take_participants(self.reader, max_samples=2)
        self.assert_blocked(value, 'builtin_take_count_out_of_range')
        self.assert_returned_once(2)
        self.assertFalse(value['loan_cleanup_complete'])
        self.assertFalse(value['loan_returned'])
        self.assertTrue(value['loan_cleanup_uncertain'])
        self.assertEqual(value['layer'], 'abi_contract')
        self.assertEqual(value['take_return_code'], 2**100)
        self.assertEqual(value['return_loan_code'], -3)
        self.assertEqual(value['free_errors'], ['return_loan:return_code=-3'])
        parse.assert_not_called()

    def test_missing_library_is_blocked_before_native_calls(self):
        self.backend.library = None
        value = self.backend.take_participants(self.reader)
        self.assert_blocked(value, 'library_not_loaded')
        self.assertFalse(value['real_execution'])
        self.library.dds_take.assert_not_called()
        self.library.dds_return_loan.assert_not_called()

    def test_null_sample_is_parse_failure_not_successful_observation(self):
        self.native_samples.clear()
        self.take_rc = 1
        self.library.dds_return_loan.return_value = -3  # BAD_PARAMETER: buf[0] is NULL.
        value = self.backend.take_participants(self.reader)
        self.assert_blocked(value, 'builtin_sample_parse_failed')
        self.assertIn('NULL', value['parse_error']['error'])
        self.assert_returned_once(1)

    def test_interrupt_during_parsing_still_returns_loan_and_propagates(self):
        with mock.patch.object(np, '_guid_text', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.backend.take_participants(self.reader)
        self.assert_returned_once(2)
        self.assertEqual(self.records, [])

    def test_emit_failure_occurs_only_after_loan_is_returned(self):
        self.backend.emit = mock.Mock(side_effect=RuntimeError('offline emit fault'))
        with self.assertRaisesRegex(RuntimeError, 'offline emit fault'):
            self.backend.take_participants(self.reader)
        self.assert_returned_once(2)


@unittest.skipUnless(os.environ.get('R2F_NATIVE_DDS_TESTS') == '1',
                     'opt-in bare non-ROS DDS participant lifecycle test')
class RealParticipantTests(unittest.TestCase):
    """Creates a real bare DDS participant (no ROS context, no ROS endpoint).

    This is a capability check in this process only: it is not gated live ROS
    execution, does not use a ROS domain lock, and produces no candidate.
    """

    def setUp(self):
        capability = np.capability()
        if not capability.get('available'):
            self.skipTest('libddsc unavailable: ' + str(capability.get('reason')))

    def test_create_read_builtin_and_destroy(self):
        records = []
        backend = np.NativeParticipantBackend(emit=records.append)
        value = backend.run({'enabled': True, 'domain_id': 0,
                             'live_authorization': {
                                 'granted': True,
                                 'library_sha256': np.capability()['library_sha256']}})
        kinds = [item['receipt_kind'] for item in records]
        self.assertIn('native_participant_created', kinds)
        self.assertIn('native_builtin_reader_created', kinds)
        self.assertIn('native_builtin_participants', kinds)
        closed = records[-1]
        self.assertEqual(closed['receipt_kind'], 'native_participant_closed')
        self.assertTrue(closed['cleanup_complete'])
        self.assertEqual(closed['failures'], [])
        created = records[0]
        self.assertTrue(created['real_execution'])
        self.assertFalse(created['ros_context_created'])
        self.assertEqual(created['identity_basis'], 'ddsc_dds_get_guid')
        self.assertEqual(len(created['guid']), 32)
        self.assertEqual(value['receipt_kind'], 'native_builtin_participants')
        self.assertGreaterEqual(value['sample_count'], 1)
        self.assertEqual(value['free_errors'], [])
        guids = {sample['guid'] for sample in value['samples']}
        self.assertIn(created['guid'], guids)
        states = {sample['instance_state'] for sample in value['samples']}
        self.assertTrue(states <= set(np.INSTANCE_STATE.values()))

    def test_close_reports_handle_failures_instead_of_hiding_them(self):
        backend = np.NativeParticipantBackend()
        backend.library, backend.identity = np.load_library()
        backend.created = [('bogus', 123456789)]
        value = backend.close()
        self.assertFalse(value['cleanup_complete'])
        self.assertEqual(value['failures'][0]['kind'], 'bogus')


if __name__ == '__main__':
    unittest.main()
