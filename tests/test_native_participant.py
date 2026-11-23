"""Native CycloneDDS ctypes backend: ABI/provenance contracts + gated lifecycle.

    R2F_NATIVE_DDS_TESTS=0 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover \
        -s tests -p test_native_participant.py -v

Default run is fully offline: temporary identity files and library doubles, no
native library load or participant.  Set R2F_NATIVE_DDS_TESTS=1 to also exercise
a real bare DDS participant create/delete in this process (non-ROS), which is still **not**
gated live ROS execution and is only a capability check.
"""
from contextlib import ExitStack, contextmanager
import ctypes
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from r2f.runtime import native_participant as np


@contextmanager
def native_probe_files():
    """Identity inputs only: these fixture bytes are never a compiled library."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for name in (*np.PRELOAD, 'libddsc.so.0', 'dds.h',
                     'dds_public_qos.h', 'dds_public_impl.h'):
            (root / name).write_bytes(('offline fixture:' + name).encode())
        with mock.patch.multiple(np, LIBRARY_DIR=root, HEADER=root / 'dds.h',
                                 QOS_HEADER=root / 'dds_public_qos.h',
                                 IMPL_HEADER=root / 'dds_public_impl.h'):
            yield root


@contextmanager
def native_topic_headers():
    """Minimal independent C declarations transcribed from the installed 0.10.5 headers.

    File I/O is the seam: these aren't a library and cannot be loaded.
    """
    headers = {
        'HEADER': ('dds.h', """
#define DDS_BUILTIN_TOPIC_DCPSTOPIC ((dds_entity_t) (DDS_MIN_PSEUDO_HANDLE + 2))
typedef struct dds_sample_info {
 dds_sample_state_t sample_state; dds_view_state_t view_state;
 dds_instance_state_t instance_state; bool valid_data; dds_time_t source_timestamp;
 dds_instance_handle_t instance_handle; dds_instance_handle_t publication_handle;
 uint32_t disposed_generation_count; uint32_t no_writers_generation_count;
 uint32_t sample_rank; uint32_t generation_rank; uint32_t absolute_generation_rank;
} dds_sample_info_t;
typedef struct dds_builtintopic_topic_key { unsigned char d[16]; } dds_builtintopic_topic_key_t;
typedef struct dds_builtintopic_topic {
 dds_builtintopic_topic_key_t key; char *topic_name; char *type_name; dds_qos_t *qos;
} dds_builtintopic_topic_t;
"""),
        'QOS_HEADER': ('dds_public_qos.h', 'DDS_EXPORT bool dds_qget_history (const dds_qos_t * __restrict qos, dds_history_kind_t *kind, int32_t *depth);'),
        'QOS_DEFS_HEADER': ('dds_public_qosdefs.h',
            'typedef enum dds_history_kind { DDS_HISTORY_KEEP_LAST, DDS_HISTORY_KEEP_ALL } dds_history_kind_t;'),
        'VERSION_HEADER': ('version.h', '#define DDS_VERSION "0.10.5"'),
        'FEATURES_HEADER': ('features.h', '#define DDS_HAS_TOPIC_DISCOVERY 1'),
    }
    with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
        paths = {}
        for attr, (name, text) in headers.items():
            path = Path(directory) / name
            path.write_text(text)
            stack.enter_context(mock.patch.object(np, attr, path, create=True))
            paths[attr] = path
        yield paths


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
        with native_probe_files() as root:
            value = np.provenance()
            expected_headers = {
                name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                for name in ('dds.h', 'dds_public_impl.h', 'dds_public_qos.h')}
        self.assertEqual(value['backend'], 'ctypes-cyclonedds')
        self.assertFalse(value['build']['compiler_required'])
        self.assertIsNone(value['build']['binary'])
        self.assertIsNone(value['build']['build_root'])
        headers = value['abi']['source_headers']
        self.assertEqual(sorted(headers), ['dds.h', 'dds_public_impl.h', 'dds_public_qos.h'])
        self.assertEqual(headers, expected_headers)
        self.assertEqual(len(value['module_sha256']), 64)

    def test_missing_headers_are_recorded_without_inventing_hashes(self):
        with native_probe_files() as root:
            for name in ('dds.h', 'dds_public_qos.h', 'dds_public_impl.h'):
                (root / name).unlink()
            value = np.provenance()
        self.assertEqual(value['abi']['source_headers'], {
            'dds.h': None, 'dds_public_qos.h': None, 'dds_public_impl.h': None})
        self.assertEqual(len(value['module_sha256']), 64)
        self.assertEqual(value['abi']['layout_sha256'], np.abi_sha256())


class CapabilityTests(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.root = stack.enter_context(native_probe_files())
        self.library = mock.Mock(spec=np.REQUIRED_SYMBOLS)
        self.loader = stack.enter_context(mock.patch.object(
            np.ctypes, 'CDLL', return_value=self.library))
        self.value = np.capability()

    def tearDown(self):
        # Inventory may bind signatures, but must never invoke a DDS operation.
        for name in np.REQUIRED_SYMBOLS:
            getattr(self.library, name).assert_not_called()

    def test_capability_is_observed_without_entities(self):
        self.assertTrue(self.value['ok'], self.value)
        self.assertEqual(self.value['capability_status'], 'observed')
        self.assertFalse(self.value['participant_created'])
        self.assertFalse(self.value['ros_context_created'])
        self.assertEqual(self.value['live_execution'], 'not_run_no_live_go')
        self.assertFalse(self.value['counts_toward_maturity'])

    def test_library_identity_and_symbols(self):
        self.assertTrue(self.value['available'])
        path = self.root / 'libddsc.so.0'
        self.assertEqual(self.value['library'], str(path.resolve()))
        self.assertEqual(self.value['library_sha256'],
                         hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(sorted(self.value['symbols']), sorted(np.REQUIRED_SYMBOLS))
        preloads = {item['name'] for item in self.value['preloads']}
        self.assertEqual(preloads, set(np.PRELOAD))
        self.assertEqual(self.loader.call_args_list, [
            mock.call(str(self.root / name), mode=ctypes.RTLD_GLOBAL)
            for name in (*np.PRELOAD, 'libddsc.so.0')])
        for item in self.value['preloads']:
            self.assertEqual(item['sha256'], hashlib.sha256(
                (self.root / item['name']).read_bytes()).hexdigest())

    def test_missing_library_is_a_capability_gap(self):
        self.loader.reset_mock()
        missing = self.root / 'missing' / 'libddsc.so.0'
        value = np.capability(str(missing))
        self.loader.assert_not_called()
        self.assertFalse(value['ok'])
        self.assertEqual(value['capability_status'], 'capability_gap')
        self.assertEqual(value['layer'], 'capability_gap')
        self.assertFalse(value['participant_created'])

    def test_unloadable_library_is_a_capability_gap(self):
        self.loader.side_effect = OSError('fixture load failure')
        value = np.capability()
        self.assertFalse(value['ok'])
        self.assertFalse(value['available'])
        self.assertEqual(value['capability_status'], 'capability_gap')
        self.assertEqual(value['layer'], 'capability_gap')
        self.assertEqual(value['reason'], 'preload_failed:' + np.PRELOAD[0])
        self.assertFalse(value['participant_created'])
        self.assertFalse(value['candidate_eligible'])

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
        with native_probe_files() as root:
            value = self.backend.run({'enabled': True,
                                      'live_authorization': {'granted': True,
                                                             'library_sha256': 'b' * 64}})
            observed = hashlib.sha256((root / 'libddsc.so.0').read_bytes()).hexdigest()
        self.assertEqual(value['observed'], observed)
        self.assertEqual(value['expected'], 'b' * 64)
        self.assertEqual(value['reason'], 'library_identity_mismatch')
        self.assertEqual(value['layer'], 'identity')
        self.assertIsNone(self.backend.library)

    def test_guard_receipts_are_never_sut_bugs(self):
        value = self.backend.run({'enabled': True})
        self.assertFalse(value['sut_bug'])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['mutation_receipt'])
        self.assertFalse(value['counts_toward_maturity'])


class BuiltinTopicTests(unittest.TestCase):
    """Public native carrier seam; every C operation is a Python fake."""

    def setUp(self):
        self.backend = np.NativeParticipantBackend()
        self.library = mock.Mock(spec=[*np.REQUIRED_SYMBOLS, 'dds_qget_history',
                                       'dds_builtintopic_free_topic'])
        self.backend.library = self.library
        self.backend.participant = 11  # Fake handle only; never create a participant.
        self.backend.identity = {'library_sha256':
            '9a63f94ee673c5efddb337a7dd012e4c98960b10ce256dcc3a7b4c331fabf5b5'}
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.topic_headers = stack.enter_context(native_topic_headers())
        self.library.dds_create_reader.return_value = 17

    def tearDown(self):
        self.library.dds_create_participant.assert_not_called()
        self.library.dds_builtintopic_free_topic.assert_not_called()
        self.library.dds_builtintopic_free_participant.assert_not_called()
        self.library.dds_qget_liveliness.assert_not_called()

    def topic_sample(self, *, kind=0, depth=7, valid=True, state=0x10):
        class Topic(ctypes.Structure):
            _fields_ = [('key', ctypes.c_ubyte * 16), ('topic_name', ctypes.c_char_p),
                        ('type_name', ctypes.c_char_p), ('qos', ctypes.c_void_p)]
        self.name = ctypes.create_string_buffer('温度'.encode())
        self.type_name = ctypes.create_string_buffer(b'Temperature')
        self.native = Topic((ctypes.c_ubyte * 16)(*range(16)),
                            ctypes.cast(self.name, ctypes.c_char_p),
                            ctypes.cast(self.type_name, ctypes.c_char_p), 0x1234)
        def take(reader, buffer, info, capacity, maximum):
            buffer[0] = ctypes.addressof(self.native)
            info[0].valid_data = valid
            info[0].instance_state = state
            info[0].source_timestamp = 987
            return 1
        def history(qos, actual_kind, actual_depth):
            self.assertEqual(qos.value, 0x1234)  # Sample QoS, never reader QoS.
            ctypes.cast(actual_kind, ctypes.POINTER(ctypes.c_int32))[0] = kind
            ctypes.cast(actual_depth, ctypes.POINTER(ctypes.c_int32))[0] = depth
            return True
        def return_loan(*args):
            self.name.value = b'gone'
            self.type_name.value = b'gone'
            self.native.key[:] = b'X' * 16
            return 0
        self.library.dds_take.side_effect = take
        self.library.dds_qget_history.side_effect = history
        self.library.dds_return_loan.side_effect = return_loan
        return self.backend.builtin_reader(topic='DCPSTopic')['reader_handle']

    def test_topic_take_copies_opaque_key_strings_and_sample_history_before_return(self):
        reader = self.topic_sample()
        value = self.backend.take_topics(reader, max_samples=1)
        self.assertTrue(value['ok'], value)
        self.assertEqual(value['receipt_kind'], 'native_builtin_topics')
        self.assertEqual(value['observation_basis'], 'dcpstopic_builtin_topic')
        sample = value['samples'][0]
        self.assertEqual(sample['topic_key'], '000102030405060708090a0b0c0d0e0f')
        self.assertEqual(sample['key_kind'], 'opaque16')
        self.assertNotIn('guid', sample)
        self.assertEqual((sample['topic_name'], sample['type_name']), ('温度', 'Temperature'))
        self.assertEqual((sample['valid_data'], sample['instance_state'],
                          sample['source_timestamp_ns']), (True, 'ALIVE', 987))
        self.assertEqual(sample['history'], {
            'status': 'observed', 'kind': 'KEEP_LAST', 'kind_raw': 0,
            'depth': 7, 'depth_raw': 7, 'depth_status': 'observed',
            'source': 'sample.qos:dds_qget_history'})
        self.assertTrue(value['loan_returned'])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['counts_toward_maturity'])
        self.library.dds_return_loan.assert_called_once()
        self.library.dds_builtintopic_free_participant.assert_not_called()

    def test_topic_missing_qos_and_getter_false_stay_unavailable(self):
        reader = self.topic_sample()
        self.native.qos = None
        value = self.backend.take_topics(reader)
        self.assertEqual(value['samples'][0]['history']['status'], 'unavailable_on_sample')
        self.assertEqual(value['samples'][0]['history']['reason'], 'qos_null')
        self.library.dds_qget_history.assert_not_called()
        reader = self.topic_sample()
        self.library.dds_qget_history.side_effect = None
        self.library.dds_qget_history.return_value = False
        value = self.backend.take_topics(reader)
        self.assertEqual(value['samples'][0]['history']['status'], 'unavailable_on_sample')
        self.assertEqual(value['samples'][0]['history']['reason'], 'getter_false')
        self.assertIsNone(value['samples'][0]['history']['depth'])

    def test_topic_lifecycle_samples_do_not_dereference_invalid_payload(self):
        for valid, state, status in [(False, 0x10, 'not_applicable_invalid_data'),
                                     (True, 0x20, 'not_applicable_instance_not_alive'),
                                     (True, 0x40, 'not_applicable_instance_not_alive')]:
            with self.subTest(valid=valid, state=state):
                reader = self.topic_sample(valid=valid, state=state)
                self.native.topic_name = None
                self.native.type_name = None
                self.native.qos = 1  # Poison pointer: it must never reach a getter.
                self.library.dds_qget_history.reset_mock()
                value = self.backend.take_topics(reader)
                self.assertTrue(value['ok'], value)
                sample = value['samples'][0]
                self.assertEqual(sample['history']['status'], status)
                self.assertIsNone(sample['topic_name'])
                self.assertIsNone(sample['type_name'])
                self.library.dds_qget_history.assert_not_called()
                self.assertTrue(value['loan_returned'])

    def test_keep_all_depth_is_diagnostic_not_a_finite_history_depth(self):
        reader = self.topic_sample(kind=1, depth=-1)
        value = self.backend.take_topics(reader)
        self.assertTrue(value['ok'], value)
        history = value['samples'][0]['history']
        self.assertEqual(history['kind'], 'KEEP_ALL')
        self.assertEqual(history['kind_raw'], 1)
        self.assertIsNone(history['depth'])
        self.assertEqual(history['depth_raw'], -1)
        self.assertEqual(history['depth_status'], 'not_applicable_keep_all')

    def test_malformed_history_blocks_observation_and_still_returns_loan(self):
        for kind, depth, getter in [(99, 7, True), (0, 0, True), (0, -1, True),
                                    (0, 7, 'not-a-C-bool')]:
            with self.subTest(kind=kind, depth=depth, getter=getter):
                reader = self.topic_sample(kind=kind, depth=depth)
                if getter is not True:
                    self.library.dds_qget_history.side_effect = None
                    self.library.dds_qget_history.return_value = getter
                self.library.dds_return_loan.reset_mock()
                value = self.backend.take_topics(reader)
                self.assertFalse(value['ok'], value)
                self.assertEqual(value['reason'], 'builtin_sample_parse_failed')
                self.assertFalse(value['effect_observed'])
                self.assertFalse(value['candidate_eligible'])
                self.assertTrue(value['loan_returned'])
                self.library.dds_return_loan.assert_called_once()

    def test_wrong_reader_sample_types_are_blocked_before_take(self):
        topic = self.backend.builtin_reader(topic='DCPSTopic')['reader_handle']
        value = self.backend.take_participants(topic)
        self.assertEqual(value['reason'], 'builtin_reader_topic_mismatch')
        self.library.dds_take.assert_not_called()
        self.library.dds_create_reader.return_value = 18
        participant = self.backend.builtin_reader()['reader_handle']
        for handle in (participant, 999):
            value = self.backend.take_topics(handle)
            self.assertEqual(value['reason'], 'builtin_reader_topic_mismatch')
            self.library.dds_take.assert_not_called()
        self.library.dds_take.return_value = 0
        self.assertTrue(self.backend.take_participants(999)['ok'])  # Legacy API.

    def test_missing_history_symbol_blocks_only_topic_capability(self):
        library = mock.Mock(spec=[name for name in np.REQUIRED_SYMBOLS
                                  if name != 'dds_qget_history'])
        library.dds_create_reader.return_value = 18
        with native_probe_files(), mock.patch.object(np.ctypes, 'CDLL', return_value=library):
            loaded, _ = np.load_library()
        self.backend.library = loaded
        value = self.backend.builtin_reader(topic='DCPSTopic')
        self.assertEqual(value['reason'], 'topic_symbols_missing')
        self.assertEqual(value['layer'], 'capability_gap')
        library.dds_create_reader.assert_not_called()
        self.assertEqual(self.backend.builtin_reader()['reader_handle'], 18)

    def test_history_getter_has_the_installed_bool_enum_int32_pointer_signature(self):
        with native_probe_files(), mock.patch.object(np.ctypes, 'CDLL',
                                                    return_value=self.library):
            loaded, _ = np.load_library()
        self.assertIs(loaded.dds_qget_history.restype, ctypes.c_bool)
        self.assertEqual(loaded.dds_qget_history.argtypes,
                         [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int32),
                          ctypes.POINTER(ctypes.c_int32)])
        loaded.dds_qget_history.assert_not_called()

    def test_unknown_abi_platform_blocks_topic_without_breaking_participant(self):
        with mock.patch('platform.machine', return_value='unverified-architecture'):
            value = self.backend.builtin_reader(topic='DCPSTopic')
            self.assertEqual(value['reason'], 'topic_abi_unsupported_platform')
            self.assertEqual(value['layer'], 'abi_contract')
            self.library.dds_create_reader.assert_not_called()
            self.assertEqual(self.backend.builtin_reader()['topic'], 'DCPSParticipant')

    def test_changed_topic_header_is_not_decoded_with_assumed_layout(self):
        path = self.topic_headers['HEADER']
        path.write_text(path.read_text().replace('unsigned char d[16]', 'unsigned char d[12]'))
        value = self.backend.builtin_reader(topic='DCPSTopic')
        self.assertEqual(value['reason'], 'topic_abi_header_mismatch')
        self.assertEqual(value['layer'], 'abi_contract')
        self.library.dds_create_reader.assert_not_called()
        self.assertEqual(self.backend.builtin_reader()['topic'], 'DCPSParticipant')

    def test_unknown_native_binary_cannot_use_the_topic_layout_profile(self):
        for identity in (None, {}, {'library_sha256': '0' * 64}):
            with self.subTest(identity=identity):
                self.backend.identity = identity
                value = self.backend.builtin_reader(topic='DCPSTopic')
                self.assertEqual(value['reason'], 'topic_abi_library_mismatch')
                self.assertEqual(value['layer'], 'abi_contract')
                self.library.dds_create_reader.assert_not_called()

    def test_topic_take_revalidates_headers_and_layout_before_native_read(self):
        reader = self.topic_sample()
        self.topic_headers['QOS_HEADER'].write_text('DDS_EXPORT int dds_qget_history(void);')
        value = self.backend.take_topics(reader)
        self.assertEqual(value['reason'], 'topic_abi_header_mismatch')
        self.library.dds_take.assert_not_called()
        self.library.dds_return_loan.assert_not_called()

    def test_unverified_native_alignment_blocks_topic_before_any_c_call(self):
        with mock.patch.object(ctypes, 'alignment', return_value=4):
            value = self.backend.builtin_reader(topic='DCPSTopic')
        self.assertEqual(value['reason'], 'topic_abi_layout_mismatch')
        self.library.dds_create_reader.assert_not_called()

    def test_topic_loans_return_once_on_null_sample_getter_error_and_oversized_count(self):
        for failure in ('null_sample', 'null_names', 'invalid_utf8', 'getter_error',
                        'oversized', 'return_error', 'parse_and_return_error'):
            with self.subTest(failure=failure):
                reader = self.topic_sample()
                self.library.dds_return_loan.reset_mock()
                if failure == 'null_sample':
                    def take(reader, buffer, info, capacity, maximum):
                        return 1
                    self.library.dds_take.side_effect = take
                elif failure == 'null_names':
                    self.native.topic_name = None
                    self.native.type_name = None
                elif failure == 'invalid_utf8':
                    self.native.topic_name = b'\xff'
                elif failure in ('getter_error', 'parse_and_return_error'):
                    self.library.dds_qget_history.side_effect = ValueError('offline getter fault')
                elif failure == 'oversized':
                    self.library.dds_take.side_effect = None
                    self.library.dds_take.return_value = 9
                if failure in ('return_error', 'parse_and_return_error'):
                    self.library.dds_return_loan.side_effect = RuntimeError('offline return fault')
                value = self.backend.take_topics(reader, max_samples=1)
                self.library.dds_return_loan.assert_called_once()
                args = self.library.dds_return_loan.call_args.args
                self.assertEqual((args[0].value, len(args[1]), args[2].value), (17, 1, 1))
                if failure == 'null_names':
                    self.assertTrue(value['ok'], value)
                    self.assertIsNone(value['samples'][0]['topic_name'])
                    self.assertIsNone(value['samples'][0]['type_name'])
                else:
                    self.assertFalse(value['ok'], value)
                    self.assertFalse(value['effect_observed'])
                    self.assertFalse(value['candidate_eligible'])
                if failure == 'oversized':
                    self.assertTrue(value['loan_cleanup_uncertain'])
                    self.assertEqual(value['abi_contract_violation'], 'dds_take_count_exceeds_capacity')
                if failure in ('return_error', 'parse_and_return_error'):
                    self.assertTrue(value['free_errors'])
                    self.assertFalse(value['loan_returned'])
                if failure == 'parse_and_return_error':
                    self.assertIsNotNone(value['parse_error'])

    def test_topic_interrupt_and_emit_error_happen_after_loan_cleanup(self):
        reader = self.topic_sample()
        self.library.dds_qget_history.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.backend.take_topics(reader)
        self.library.dds_return_loan.assert_called_once()
        reader = self.topic_sample()
        self.library.dds_return_loan.reset_mock()
        def emit(value):
            self.library.dds_return_loan.assert_called_once()
            raise RuntimeError('offline emit fault')
        self.backend.emit = emit
        with self.assertRaisesRegex(RuntimeError, 'offline emit fault'):
            self.backend.take_topics(reader)

    def test_topic_nonpositive_take_and_bad_capacity_do_not_return_nonexistent_loans(self):
        reader = self.topic_sample()
        self.library.dds_take.side_effect = None
        for count in (0, -1):
            self.library.dds_take.return_value = count
            value = self.backend.take_topics(reader)
            self.assertEqual(value['ok'], count == 0)
            self.assertFalse(value['effect_observed'])
            self.assertFalse(value['loan_return_attempted'])
            self.library.dds_return_loan.assert_not_called()
        self.library.dds_take.reset_mock()
        for capacity in (True, None, 1.5, '1', 0, -1, 1025):
            value = self.backend.take_topics(reader, capacity)
            self.assertFalse(value['ok'])
            self.library.dds_take.assert_not_called()
            self.library.dds_return_loan.assert_not_called()

    def test_changed_sample_info_type_blocks_topic_decoding(self):
        path = self.topic_headers['HEADER']
        path.write_text(path.read_text().replace('bool valid_data', 'int64_t valid_data'))
        value = self.backend.builtin_reader(topic='DCPSTopic')
        self.assertEqual(value['reason'], 'topic_abi_header_mismatch')
        self.library.dds_create_reader.assert_not_called()

    def test_topic_profile_rejects_near_miss_feature_and_version_defines(self):
        for attr, text in [('FEATURES_HEADER', '#define DDS_HAS_TOPIC_DISCOVERY 10'),
                           ('FEATURES_HEADER', '#define DDS_HAS_TOPIC_DISCOVERY 0'),
                           ('VERSION_HEADER', '#define DDS_VERSION "0.10.5-custom"')]:
            with self.subTest(attr=attr, text=text):
                with native_topic_headers() as paths:
                    paths[attr].write_text(text)
                    value = self.backend.builtin_reader(topic='DCPSTopic')
                self.assertEqual(value['reason'], 'topic_abi_header_mismatch')
                self.library.dds_create_reader.assert_not_called()

    def test_closed_topic_reader_cannot_be_reused_as_an_admitted_topic_handle(self):
        reader = self.topic_sample()
        self.library.dds_delete.return_value = 0
        self.assertTrue(self.backend.close()['cleanup_complete'])
        value = self.backend.take_topics(reader)
        self.assertEqual(value['reason'], 'builtin_reader_topic_mismatch')
        self.library.dds_take.assert_not_called()

    def test_invalid_topic_handles_are_receipted_without_native_calls(self):
        self.topic_sample()
        for handle in (True, False, {}, [], None, '17', ctypes.c_int32(17), 0, -1):
            with self.subTest(handle=handle):
                value = self.backend.take_topics(handle)
                self.assertEqual(value['reason'], 'builtin_reader_handle_invalid')
                self.assertFalse(value['effect_observed'])
                self.library.dds_take.assert_not_called()
        # Preserve the legacy API's exception for a non-convertible handle.
        with self.assertRaises(TypeError):
            self.backend.take_participants([])

    def test_topic_reader_uses_dcpstopic_and_preserves_participant_default(self):
        topic = self.backend.builtin_reader(topic='DCPSTopic')
        self.assertEqual((topic['topic'], topic['topic_handle'], topic['reader_handle']),
                         ('DCPSTopic', 0x7fff0002, 17))
        self.assertFalse(topic['candidate_eligible'])
        args = self.library.dds_create_reader.call_args.args
        self.assertEqual((args[0].value, args[1].value, args[2:]),
                         (11, 0x7fff0002, (None, None)))
        participant = self.backend.builtin_reader()
        self.assertEqual(participant['topic'], 'DCPSParticipant')
        self.assertEqual(participant['topic_handle'], 0x7fff0001)


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
