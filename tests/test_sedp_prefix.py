"""Live-prefix wire observations without ROS or final capture claims."""
import copy
import hashlib
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

from r2f.instrumentation.sedp import (parse_sedp_capture, parse_sedp_capture_prefix,
                                     reconcile_discovery_projection)
from r2f.instrumentation.sedp_wire import MAX_CAPTURE_PREFIX_BYTES
from r2f.instrumentation.rtps import udp_frame
from test_instrumentation_sedp_projection import BASE, GUID, PREFIX, fixture, packet, pcap, plist


def prefix(path, data):
    path.write_bytes(data)
    state = path.stat()
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data),
            'final': False, 'file_identity': {'device': state.st_dev, 'inode': state.st_ino}}


class SedpPrefixContracts(unittest.TestCase):
    def test_append_keeps_fixed_prefix_hash_identity_and_offsets(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'live.pcap'
            data = pcap([(BASE + 120, packet(plist()))])
            raw = prefix(path, data)
            first = parse_sedp_capture_prefix(raw)
            with path.open('ab') as out:
                out.write(pcap([(BASE + 180, packet(plist(), dispose=True, sequence=2))])[24:])
            parsed = parse_sedp_capture_prefix(raw)
            self.assertEqual(parsed, first)
            self.assertEqual(parsed['independent_source'], 'passive_rtps_wire_prefix')
            self.assertIsNone(parsed['capture_receipt'])
            self.assertFalse(parsed['graph_or_config_field_input'])
            self.assertFalse(parsed['diagnostics'])
            event = parsed['events'][0]
            self.assertEqual((event['endpoint_guid'], event['role'], event['lifecycle'], event['unix_ns']),
                             (GUID, 'writer', 'alive', BASE + 120))
            for ref in (event['raw_ref'], parsed['activity'][0]['raw_ref']):
                self.assertEqual({k: ref[k] for k in raw}, raw)
                self.assertEqual(ref['record_offset'], 24)
                self.assertEqual(ref['frame_offset'], 40)
                self.assertEqual(ref['frame_sha256'], hashlib.sha256(packet(plist())).hexdigest())
                self.assertTrue(Path(ref['path']).is_file())
            final = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
            self.assertEqual([e['lifecycle'] for e in parse_sedp_capture(final)['events']], ['alive', 'dispose_or_unregister'])

    def test_append_during_open_read_is_permitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'live.pcap'
            raw = prefix(path, pcap([(BASE + 120, packet(plist()))]))
            original = os.fstat
            calls = []
            def fstat(fd):
                calls.append(fd)
                if len(calls) == 2:
                    with path.open('ab') as out:
                        out.write(b'not part of the flushed prefix')
                return original(fd)
            with patch('r2f.instrumentation.sedp_wire.os.fstat', side_effect=fstat):
                parsed = parse_sedp_capture_prefix(raw)
            self.assertEqual(len(calls), 2)
            self.assertFalse(parsed['diagnostics'])
            self.assertEqual(parsed['events'][0]['endpoint_guid'], GUID)

    def test_prefix_rewrite_short_read_and_wrong_hash_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'live.pcap'
            data = pcap([(BASE + 120, packet(plist()))])
            raw = prefix(path, data)
            for changed in (data[:-1], data[:-1] + bytes([data[-1] ^ 1])):
                with self.subTest(size=len(changed)):
                    path.write_bytes(changed)
                    with self.assertRaises(ValueError): parse_sedp_capture_prefix(raw)
            path.write_bytes(data)
            with self.assertRaisesRegex(ValueError, 'sha256_mismatch'):
                parse_sedp_capture_prefix({**raw, 'sha256': '0' * 64})

    def test_path_inode_replacement_even_with_identical_bytes_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, replacement = Path(tmp) / 'live.pcap', Path(tmp) / 'replacement'
            data = pcap([(BASE + 120, packet(plist()))])
            raw = prefix(path, data)
            replacement.write_bytes(data)
            replacement.replace(path)
            with self.assertRaisesRegex(ValueError, 'path_or_inode_changed'):
                parse_sedp_capture_prefix(raw)

    def test_path_replacement_during_read_and_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, replacement = Path(tmp) / 'live.pcap', Path(tmp) / 'replacement'
            data = pcap([(BASE + 120, packet(plist()))])
            raw = prefix(path, data)
            replacement.write_bytes(data)
            original, calls = os.fstat, []
            def fstat(fd):
                calls.append(fd)
                if len(calls) == 2: replacement.replace(path)
                return original(fd)
            with patch('r2f.instrumentation.sedp_wire.os.fstat', side_effect=fstat):
                with self.assertRaisesRegex(ValueError, 'path_or_inode_changed'):
                    parse_sedp_capture_prefix(raw)
            raw = prefix(path, data)
            path.rename(replacement)
            path.symlink_to(replacement)
            with self.assertRaises(ValueError): parse_sedp_capture_prefix(raw)

    def test_size_identity_and_final_markers_are_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = prefix(Path(tmp) / 'live.pcap', pcap([]))
            for size in (True, False, None, '24', 24.0, -1, 0, 23, MAX_CAPTURE_PREFIX_BYTES + 1):
                with self.subTest(size=size), self.assertRaisesRegex(ValueError, 'prefix_size'):
                    parse_sedp_capture_prefix({**raw, 'size': size})
            for identity in (None, {}, {'device': 1, 'inode': True}, {'device': -1, 'inode': 3},
                             {'device': 1, 'inode': 0}, {'device': 1, 'inode': 3, 'extra': 0}):
                with self.subTest(identity=identity), self.assertRaises(ValueError):
                    parse_sedp_capture_prefix({**raw, 'file_identity': identity})
            for final in (True, None, 0, 1, 'false'):
                with self.subTest(final=final), self.assertRaises(ValueError):
                    parse_sedp_capture_prefix({**raw, 'final': final})
            with self.assertRaises(ValueError): parse_sedp_capture_prefix({**raw, 'path': 'relative.pcap'})
            self.assertEqual(parse_sedp_capture_prefix(raw)['events'], [])

    def test_partial_record_retains_diagnostic_and_never_invents_live_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = pcap([(BASE + 120, packet(plist()))])
            for truncated in (data[:25], data[:-1]):
                raw = prefix(Path(tmp) / 'live.pcap', truncated)
                parsed = parse_sedp_capture_prefix(raw)
                self.assertTrue(parsed['diagnostics'])
                self.assertFalse(parsed['events'])

    def test_actual_endpoint_guid_role_and_dispose_survive_shared_decoder(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = pcap([(BASE + 120, packet(plist())),
                         (BASE + 140, packet(plist(), dispose=True, sequence=2))])
            raw = prefix(Path(tmp) / 'live.pcap', data)
            parsed = parse_sedp_capture_prefix(raw, field_policy={'protocol_defaults': False})
            self.assertEqual([(e['endpoint_guid'], e['role'], e['lifecycle']) for e in parsed['events']],
                             [(GUID, 'writer', 'alive'), (GUID, 'writer', 'dispose_or_unregister')])
            final = parse_sedp_capture({'path': raw['path'], 'sha256': raw['sha256']}, field_policy={'protocol_defaults': False})
            for prefix_event, final_event in zip(parsed['events'], final['events']):
                for key in ('endpoint_guid', 'role', 'lifecycle', 'unix_ns', 'fields', 'source_guid_prefix'):
                    # Field provenance includes raw references which intentionally differ.
                    if key != 'fields': self.assertEqual(prefix_event[key], final_event[key])
            self.assertEqual(parsed['events'][-1]['status_info_hex'], '00000003')

    def test_writer_and_reader_use_actual_endpoint_guids(self):
        with tempfile.TemporaryDirectory() as tmp:
            reader_guid = PREFIX + '00001004'
            reader = bytearray(packet(plist().replace(bytes.fromhex(GUID), bytes.fromhex(reader_guid))))
            udp, diagnostic = udp_frame(reader)
            self.assertIsNone(diagnostic)
            pos = udp['payload_offset'] + 20 + 12
            reader[pos:pos + 4] = bytes.fromhex('000004c2')
            raw = prefix(Path(tmp) / 'live.pcap', pcap([(BASE + 120, packet(plist())), (BASE + 121, bytes(reader))]))
            parsed = parse_sedp_capture_prefix(raw)
            self.assertEqual([(e['endpoint_guid'], e['role'], e['lifecycle']) for e in parsed['events']],
                             [(GUID, 'writer', 'alive'), (reader_guid, 'reader', 'alive')])
            self.assertFalse(parsed['diagnostics'])

    def test_prefix_cannot_replace_final_parser_or_reconciliation_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wire, graph, bindings, window = fixture(root)
            data = Path(wire['raw']['path']).read_bytes()
            raw = prefix(root / 'prefix.pcap', data)
            parsed = parse_sedp_capture_prefix(raw)
            with self.assertRaisesRegex(ValueError, 'prefix_requires_prefix_parser'):
                parse_sedp_capture(raw)
            with self.assertRaises(ValueError): parse_sedp_capture({'raw': raw, 'ready': {'unix_ns': BASE}})
            for source in ('passive_rtps_wire_prefix', 'passive_rtps_wire'):
                supplied = copy.deepcopy(parsed)
                supplied['independent_source'] = source
                supplied['capture_receipt'] = {**wire['capture_receipt'], 'raw': parsed['raw']}
                result = reconcile_discovery_projection(supplied, graph, endpoint_bindings=bindings, observation_window=window)
                self.assertEqual(result['comparison_status'], 'unknown')
                self.assertIn('wire_raw_unavailable_or_changed', result['unknown_reasons'])
            self.assertEqual(reconcile_discovery_projection(wire, graph, endpoint_bindings=bindings,
                             observation_window=window)['comparison_status'], 'consistent')


if __name__ == '__main__':
    unittest.main()
