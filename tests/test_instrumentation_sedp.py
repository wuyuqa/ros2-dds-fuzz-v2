"""Offline protocol and receive-only observer contracts. No ROS or raw sockets."""
import hashlib
import json
from pathlib import Path
import socket
import struct
import tempfile
import time
import unittest
from unittest.mock import patch

from r2f.instrumentation.sedp import PassiveSedpObserver, build_capture_spec, parse_sedp_capture
from r2f.instrumentation.rtps import discovery_activity, udp_frame
from r2f.instrumentation.sedp_capture import _rtps_metadata
from r2f.instrumentation.sedp_wire import read_capture

PREFIX = '011000000000000000000001'
OTHER = '011000000000000000000002'
PROOF = {'host_net_namespace': 'net:[1]', 'host_user_namespace': 'user:[2]',
         'net_namespace': 'net:[3]', 'user_namespace': 'user:[4]',
         'host_uid': 1000, 'host_cap_eff': 0}


# Byte-for-byte multi-submessage packets from the interrupted third-target batch
# and the old fourth-target run. Tests are self-contained; extraction provenance
# (pcap path/hash and packet index) is saved in the new offline audit directory.
CAPTURED_BUILTIN_FRAMES = {
    'cyclone_mixed_builtin_heartbeats': (
        '650408c5476e79dfa266c78d85482bed8fbdf986b32b73ef296a1cd3b9318fc8',
        bytes.fromhex(
            '0000000000000000000000000800450000e01b414000401120ca7f0000017f000001c6636b1400ccfedf525450530201'
            '01100110d6b3560bf9a5f225c0060e010c0001101116fa61c8ce669a587007011c0000000000000003c2000000000100'
            '000000000000040000000100000007011c0000000000000004c200000000010000000000000002000000010000000701'
            '1c0000000000000200c2000000000100000000000000010000000100000007011c0000000000000300c3000000000100'
            '000000000000000000000100000007011c0000000000000301c30000000001000000000000000000000001000000'
        )),
    'cyclone_participant_ack': (
        '34d85043b4a5e8907befb7d0e431822edd3033af0a722edc42fa0ab82b8c5be9',
        bytes.fromhex(
            '00000000000000000000000008004500005c2c4140004011104e7f0000017f000001e4f16b120048fe5b525450530201'
            '011001101116fa61c8ce669a58700e010c000110d6b3560bf9a5f225c00606031800000200c7000200c2000000000300'
            '00000000000003000000'
        )),
    'cyclone_participant_data': (
        '081a848a5312d27a5a048686932dd0960881701f0814c758b8a280fe2cdd8b6c',
        bytes.fromhex(
            '0000000000000000000000000800450000902c4040004011101b7f0000017f000001c6636b14007cfe8f525450530201'
            '01100110d6b3560bf9a5f225c006090108001e07f96ad8d5a1c9150530000000100000000000000200c2000000000200'
            '0000000100000110d6b3560bf9a5f225c00600000001010000000000000007011c0000000000000200c2000000000200'
            '0000000000000200000003000000'
        )),
    'cyclone_participant_heartbeat': (
        '75463e585d2e965f31285df65dac8d2ca06737dbfdc9435e8ab732db48e4fa0c',
        bytes.fromhex(
            '0000000000000000000000000800450000601f13400040111d787f0000017f000001d81a6b14004cfe5f525450530201'
            '01100110a30a87978803372815f40e010c000110164d83f74ff3e8fcefb307011c00000200c7000200c2000000000100'
            '0000000000000100000002000000'
        )),
    'cyclone_typelookup_reply_ack': (
        '81653203f1ef55d78296b2826fb61d9c25f0d0a0f157bafb03d93db6c1c0ddb9',
        bytes.fromhex(
            '00000000000000000000000008004500005c1f18400040111d777f0000017f000001b0b26b120048fe5b525450530201'
            '01100110164d83f74ff3e8fcefb30e010c000110a30a87978803372815f406031800000301c4000301c3000000000100'
            '00000000000001000000'
        )),
    'cyclone_typelookup_request_ack': (
        '45bcc8966df279aff6073e188e9a0c3bf12f0d7a701ab0b9502a5ca9a8947754',
        bytes.fromhex(
            '00000000000000000000000008004500005c8fed40004011aca17f0000017f000001af4f6b120048fe5b525450530201'
            '01100110d0005b8325dd298fb8240e010c000110709c7cc4c0724ac4bc6606031800000300c4000300c3000000000100'
            '00000000000001000000'
        )),
    'fastdds_participant_ack_with_unknown_extension': (
        'f079f04f916622524900f7f6f573c9e5245c994af9aeeb1130b6aa613b428554',
        bytes.fromhex(
            '000000000000000000000000080045000098e70d4000401155457f0000017f000001b9376b140084fe97525450530203'
            '010f010f7f012868779c000000000e010c00010f7f013868bcd60100000006031800000200c7000200c2000000000100'
            '000000000000010000008001380001000000146b00000000000000000000000000007f0000010ccef86a4c6926220b00'
            '000000000000440d0000000000000000000000000000'
        )),
    'fastdds_participant_heartbeat_with_unknown_extension': (
        '1097dd32c05b6e30574ecd998dd59c24c2171828dbf512bed4672378bb56e24f',
        bytes.fromhex(
            '00000000000000000000000008004500009ce6b84000401155967f0000017f000001b9376b140088fe9b525450530203'
            '010f010f7f012868779c000000000e010c00010f7f013868bcd60100000007011c00000200c7000200c2000000000100'
            '00000000000000000000010000008001380001000000146b00000000000000000000000000007f0000010ccef86ac35c'
            'e31e0800000000000000d00b0000000000000000000000000000'
        )),
}


def frame(kind=0x15, writer='000003c2', prefix=PREFIX, sequence=1,
          source_port=53410, destination_port=53412, reader='00000000'):
    if kind in (0x15, 0x16):
        body = b'\0\0' + struct.pack('<H', 16 if kind == 0x15 else 28) + bytes.fromhex(reader) + bytes.fromhex(writer) + struct.pack('<iI', 0, sequence)
        if kind == 0x16:
            body += struct.pack('<IHHI', 1, 1, 4, 4) + b'data'
    else:
        body = bytes.fromhex(reader) + bytes.fromhex(writer) + struct.pack('<iI', 0, sequence)
        if kind == 0x07:
            body += struct.pack('<iII', 0, sequence, 1)
        elif kind == 0x06:
            body += struct.pack('<II', 0, 1)
        elif kind == 0x08:
            body += struct.pack('<iII', 0, sequence, 0)
        elif kind == 0x12:
            body += struct.pack('<III', 1, 0, 1)
        elif kind == 0x13:
            body += struct.pack('<II', 1, 1)
    rtps = b'RTPS\x02\x01\x01\x10' + bytes.fromhex(prefix) + bytes([kind, 1]) + struct.pack('<H', len(body)) + body
    udp = struct.pack('!HHHH', source_port, destination_port, len(rtps) + 8, 0) + rtps
    ip = bytes.fromhex('4500') + struct.pack('!H', len(udp) + 20) + bytes(5) + b'\x11' + bytes(2) + b'\x7f\x00\x00\x01' * 2
    return bytes(12) + b'\x08\x00' + ip + udp


def with_rtps_payload(packet, payload):
    """Replace payload while retaining a valid offline Ethernet/IP/UDP envelope."""
    packet = bytearray(packet[:42] + payload)
    struct.pack_into('!H', packet, 16, len(packet) - 14)
    struct.pack_into('!H', packet, 38, len(packet) - 34)
    return bytes(packet)


def submessage(kind, body, flags=1):
    return bytes([kind, flags]) + struct.pack('<H' if flags & 1 else '>H', len(body)) + body


def unassignable_rtps_frame(source_port=61000, destination_port=61002):
    """Valid Ethernet/IP/UDP envelope with an RTPS payload too short for GUIDs."""
    packet = bytearray(frame(source_port=source_port, destination_port=destination_port))
    payload_start = 14 + 20 + 8
    del packet[payload_start + 10:]
    udp_length = len(packet) - (14 + 20)
    ip_length = len(packet) - 14
    struct.pack_into('!H', packet, 14 + 2, ip_length)
    struct.pack_into('!H', packet, 14 + 20 + 4, udp_length)
    return bytes(packet)


class FakeSocket:
    def __init__(self, frames, statistics_error=False):
        self.frames = list(frames)
        self.closed = False
        self.statistics_error = statistics_error

    def setsockopt(self, *a): pass
    def bind(self, *a): pass
    def settimeout(self, *a): pass

    def recvmsg(self, *a):
        if self.frames:
            item = self.frames.pop(0)
            packet, stamp = item[:2]
            flags = item[2] if len(item) > 2 else 0
            stamp = time.time_ns() if stamp is None else stamp
            ancillary = [(socket.SOL_SOCKET, 35, struct.pack('=qq', stamp // 10**9, stamp % 10**9))]
            return packet, ancillary, flags, ('lo', 0, 4, 0, b'')
        time.sleep(.005)
        raise socket.timeout()

    def getsockopt(self, level, option, *a):
        if level == socket.SOL_SOCKET:
            return 8 * 1024 * 1024
        if self.statistics_error:
            raise OSError('unavailable statistics')
        return struct.pack('=III', 2, 0, 0)

    def close(self): self.closed = True


class PassiveCaptureContracts(unittest.TestCase):
    def spec(self, path, **kw):
        return build_capture_spec(run_id='offline', domain_id=184, netns_identity=PROOF,
                                  output_dir=str(path), **kw)

    def test_builder_is_pure_deterministic_and_rejects_host_or_unknown_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'new'
            a = self.spec(path)
            self.assertFalse(path.exists())
            self.assertEqual(a, self.spec(path))
            with self.assertRaises(ValueError): self.spec(path, limits={'wat': 1})
            with self.assertRaises(ValueError): self.spec(path, limits={'timeout_sec': float('nan')})
            with self.assertRaises(ValueError):
                build_capture_spec(run_id='x', domain_id=184, netns_identity={**PROOF, 'net_namespace': 'net:[1]'}, output_dir=str(path))

    def test_timeout_limit_is_finite_positive_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            for invalid in (float('nan'), float('inf'), -float('inf'), 0, -1, 3600.1, True):
                with self.subTest(value=invalid), self.assertRaises(ValueError):
                    self.spec(Path(tmp), limits={'timeout_sec': invalid})
            for valid in (.001, 1, 3600):
                self.assertEqual(self.spec(Path(tmp), limits={'timeout_sec': valid})['limits']['timeout_sec'], valid)

    def test_changed_spec_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec = self.spec(Path(tmp)); spec['domain_id'] = 185
            with self.assertRaises(ValueError): PassiveSedpObserver(spec)

    def test_host_proof_fails_before_opening_packet_socket(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = PassiveSedpObserver(self.spec(Path(tmp)))
            with patch('r2f.instrumentation.sedp_capture.socket.socket') as mocked:
                with self.assertRaises(RuntimeError): observer.start()
                mocked.assert_not_called()

    def test_discovery_builtin_controls_and_user_data_are_distinct(self):
        for kind, expected in [(0x15, 'SEDP_DATA'), (0x07, 'SEDP_HEARTBEAT'), (0x06, 'SEDP_ACKNACK')]:
            events, errors = discovery_activity(frame(kind))
            self.assertFalse(errors)
            self.assertEqual(events[0]['kind'], expected)
            self.assertEqual(events[0]['source_guid_prefix'], PREFIX)
        self.assertEqual(discovery_activity(frame(writer='00000903'))[0], [])
        self.assertEqual(discovery_activity(frame(writer='000100c2'))[0][0]['kind'], 'SPDP_DATA')

    def test_identity_time_dedup_and_shutdown_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp); now = time.time_ns() + 1000000
            packet = frame()
            fake = FakeSocket([(packet, None), (packet, None), (frame(prefix=OTHER), None)])
            observer = PassiveSedpObserver(self.spec(path))
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['packets_written'] < 3 and time.monotonic() < end: time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                self.assertEqual(result['deduplicated_event_count'], 1)
                self.assertEqual(result['packet_event_copies'], 2)
                self.assertEqual(result['counts_by_kind'], {'SEDP_DATA': 1})
                self.assertEqual(observer.snapshot_activity(since_ns=time.time_ns() + 1000000000, participant_prefixes=[PREFIX])['status'], 'unknown')
                stopped = observer.stop()
                self.assertEqual(stopped, observer.stop())
                self.assertTrue(fake.closed)
                self.assertEqual(stopped['raw']['sha256'], hashlib.sha256((path/'normal-rtps.pcap').read_bytes()).hexdigest())
                self.assertEqual(stopped['index']['sha256'], hashlib.sha256((path/'packet-index.jsonl').read_bytes()).hexdigest())
                self.assertEqual(stopped['filtered_rtps_index']['records'], 0)
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertFalse(stopped['participant_created'])
                self.assertFalse(stopped['packet_transmission'])
                self.assertEqual(observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])['raw_prefix']['sha256'], stopped['raw']['sha256'])
            with self.assertRaises(RuntimeError): observer.start()

    def retained_observation(self, packet, *, flags=0, participant_prefixes=(PREFIX,)):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            observer = PassiveSedpObserver(self.spec(path))
            fake = FakeSocket([(packet, None, flags)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['packets_written'] < 1 and time.monotonic() < end:
                    time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=participant_prefixes)
                stopped = observer.stop()
            self.assertEqual(stopped['counters']['packets_written'], 1)
            self.assertEqual(stopped['filtered_rtps_index']['records'], 0)
            self.assertEqual((path/'filtered-rtps-index.jsonl').read_bytes(), b'')
            index = (path/'packet-index.jsonl').read_bytes()
            entry = json.loads(index)
            self.assertEqual(stopped['index']['sha256'], hashlib.sha256(index).hexdigest())
            _, records, diagnostics = read_capture(stopped)
            self.assertFalse(diagnostics)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]['frame'], packet)
            self.assertEqual(entry['raw_ref']['frame_sha256'], hashlib.sha256(packet).hexdigest())
            for key in ('packet_index', 'record_offset', 'frame_offset', 'captured_length'):
                self.assertEqual(entry['raw_ref'][key], records[0][key])
            self.assertEqual(entry['truncated'], bool(flags & socket.MSG_TRUNC))
            self.assertEqual(entry['raw_ref']['original_length'], None if flags & socket.MSG_TRUNC else len(packet))
            self.assertEqual(entry['activity'], discovery_activity(records[0]['frame'])[0])
            parsed = parse_sedp_capture(stopped)
            self.assertEqual(parsed['activity'], [
                {**event, 'unix_ns': entry['unix_ns'], 'raw_ref': parsed['activity'][i]['raw_ref']}
                for i, event in enumerate(entry['activity'])])
            return result, stopped, {**entry, **(entry['rtps_metadata'] or {})}, parsed

    def test_standard_and_nonstandard_discovery_raw_and_index_are_reparseable(self):
        for ports in ((53410, 53412), (53411, 53413), (61000, 61002)):
            for writer in ('000100c2', '000003c2', '000004c2'):
                with self.subTest(ports=ports, writer=writer):
                    packet = frame(writer=writer, source_port=ports[0], destination_port=ports[1])
                    result, stopped, record, _ = self.retained_observation(packet)
                    self.assertEqual(record['classification'], 'discovery')
                    self.assertEqual((record['source_port'], record['destination_port']), ports)
                    self.assertEqual(result['status'], 'observed')
                    self.assertEqual(stopped['health']['status'], 'ok')

    def test_nonstandard_sedp_payload_survives_endpoint_decoding(self):
        guid = PREFIX + '00000103'
        parameter = struct.pack('<HH', 0x5a, 16) + bytes.fromhex(guid) + struct.pack('<HH', 1, 0)
        for ports in ((53410, 53412), (61000, 61002)):
            with self.subTest(ports=ports):
                packet = frame(source_port=ports[0], destination_port=ports[1])
                packet = with_rtps_payload(packet, packet[42:62] + submessage(0x15, packet[66:] + b'\0\3\0\0' + parameter, flags=5))
                result, stopped, record, parsed = self.retained_observation(packet)
                self.assertEqual(result['status'], 'observed')
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(parsed['events'][0]['endpoint_guid'], guid)
                self.assertEqual(parsed['events'][0]['parameter_list']['complete'], True)
                self.assertFalse(parsed['diagnostics'])

    def filtered_observation(self, packet, *, flags=0, retained=()):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            observer = PassiveSedpObserver(self.spec(path))
            fake = FakeSocket([(packet, None, flags), *((item, None) for item in retained)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while time.monotonic() < end:
                    with observer._lock:
                        complete = observer._counters['filtered_rtps'] == 1 and observer._counters['packets_written'] == len(retained)
                    if complete:
                        break
                    time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                stopped = observer.stop()
            self.assertEqual(stopped['counters']['filtered_rtps'], 1)
            self.assertEqual(stopped['counters'].get('packets_written', 0), len(retained))
            _, raw_records, diagnostics = read_capture(stopped)
            self.assertFalse(diagnostics)
            self.assertEqual([item['frame'] for item in raw_records], list(retained))
            index = (path/'filtered-rtps-index.jsonl').read_bytes()
            record = json.loads(index)
            self.assertEqual(stopped['filtered_rtps_index']['sha256'], hashlib.sha256(index).hexdigest())
            self.assertEqual(record['frame_sha256'], hashlib.sha256(packet).hexdigest())
            self.assertEqual(record['captured_length'], len(packet))
            self.assertEqual(record['truncated'], bool(flags & socket.MSG_TRUNC))
            self.assertEqual((record['source_port'], record['destination_port']), (61000, 61002))
            self.assertEqual(record['packet_type'], 4)
            return result, stopped, record

    def test_target_user_data_does_not_degrade_filtered_capture(self):
        for kind in (0x15, 0x16):
            for writer in ('00000103', '00000102'):
                with self.subTest(kind=kind, writer=writer):
                    packet = frame(kind, writer=writer, source_port=61000, destination_port=61002)
                    self.assertEqual(discovery_activity(packet), ([], []))
                    result, stopped, record = self.filtered_observation(packet)
                    self.assertEqual(result['status'], 'unknown')  # Empty discovery window stays unknown.
                    self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
                    self.assertEqual(result['filtered_rtps']['target_related'], [])
                    self.assertEqual(stopped['health']['status'], 'ok')
                    self.assertEqual(record['source_participant_prefix'], PREFIX)
                    self.assertEqual(record['classification'], 'proven_no_discovery')
                    self.assertEqual(record['submessage_entities'][0]['writer_entity_id'], writer)
                    self.assertEqual(record['submessage_entities'][0]['classification'], 'user')

    def test_target_user_controls_do_not_degrade_filtered_capture(self):
        for kind in (0x06, 0x07, 0x08, 0x12, 0x13):
            with self.subTest(kind=kind):
                packet = frame(kind, writer='00000103', reader='00000207', source_port=61000, destination_port=61002)
                self.assertEqual(discovery_activity(packet), ([], []))
                result, stopped, record = self.filtered_observation(packet)
                self.assertEqual(result['filtered_rtps']['target_related'], [])
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(record['classification'], 'proven_no_discovery')
                self.assertEqual(record['submessage_entities'][0]['reader_entity_id'], '00000207')

    def test_nonstandard_builtin_data_and_controls_are_retained(self):
        for kind, writer, reader in ((0x15, '000100c2', '00000000'),
                                      (0x15, '000004c2', '00000000'),
                                      (0x16, '000003c2', '00000000'),
                                      *((kind, '000003c2', '000003c7') for kind in (0x06, 0x07, 0x08, 0x12, 0x13)),
                                      (0x07, '00000000', '000003c7'),
                                      (0x15, '00000103', '000003c7')):
            with self.subTest(kind=kind, writer=writer, reader=reader):
                packet = frame(kind, writer=writer, reader=reader, source_port=61000, destination_port=61002)
                result, stopped, record, parsed = self.retained_observation(packet)
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(record['classification'], 'discovery')
                self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
                if kind == 0x16:
                    # Retention does not claim the offline parser supports reassembly.
                    self.assertIn('DATA_FRAG_endpoint_reassembly_unsupported', [d['reason'] for d in parsed['diagnostics']])

    def test_recorded_nonsecure_builtins_are_retained_without_fake_discovery(self):
        seen = set()
        for name, (digest, recorded) in CAPTURED_BUILTIN_FRAMES.items():
            if not name.startswith('cyclone_'):
                continue
            self.assertEqual(hashlib.sha256(recorded).hexdigest(), digest)
            for nonstandard in (False, True):
                packet = bytearray(recorded)
                if nonstandard:
                    struct.pack_into('!HH', packet, 34, 61000, 61002)
                packet = bytes(packet)
                with self.subTest(fixture=name, nonstandard=nonstandard):
                    udp, error = udp_frame(packet)
                    self.assertIsNone(error)
                    prefix = udp['payload'][8:20].hex()
                    result, stopped, record, parsed = self.retained_observation(
                        packet, participant_prefixes=[prefix])
                    expected_activity, errors = discovery_activity(packet)
                    self.assertFalse(errors)
                    self.assertEqual(record['activity'], expected_activity)
                    self.assertFalse(record['diagnostics'])
                    self.assertFalse(record['parse_diagnostics'])
                    self.assertEqual(record['classification'], 'discovery' if expected_activity else 'other_builtin')
                    self.assertEqual(result['status'], 'observed' if expected_activity else 'unknown')
                    self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
                    self.assertEqual(stopped['health']['status'], 'ok')
                    self.assertEqual(parsed['events'], [])  # These samples advertise no SEDP endpoint DATA.
                    others = [e for e in record['submessage_entities'] if e['classification'] == 'other_builtin']
                    self.assertTrue(others)
                    seen.update((e['kind'], e['writer_entity_id'], e['reader_entity_id']) for e in others)
                    self.assertTrue(all(e['builtin_writer_id'] in ('000003c2', '000004c2', '000100c2')
                                        for e in result['events']))
        self.assertIn(('DATA', '000200c2', '00000000'), seen)
        self.assertIn(('ACKNACK', '000200c2', '000200c7'), seen)
        self.assertIn(('HEARTBEAT', '000200c2', '000200c7'), seen)
        for writer, reader in (('000300c3', '000300c4'), ('000301c3', '000301c4')):
            self.assertIn(('ACKNACK', writer, reader), seen)
            self.assertIn(('HEARTBEAT', writer, '00000000'), seen)

    def test_recorded_fastdds_pairs_and_statistics_footer_are_retained(self):
        for name, (digest, recorded) in CAPTURED_BUILTIN_FRAMES.items():
            if not name.startswith('fastdds_'):
                continue
            for nonstandard in (False, True):
                packet = bytearray(recorded)
                if nonstandard:
                    struct.pack_into('!HH', packet, 34, 61000, 61002)
                packet = bytes(packet)
                with self.subTest(fixture=name, nonstandard=nonstandard):
                    self.assertEqual(hashlib.sha256(recorded).hexdigest(), digest)
                    result, stopped, record, _ = self.retained_observation(packet)
                    others = [e for e in record['submessage_entities'] if e['classification'] == 'other_builtin']
                    self.assertEqual(len(others), 1)
                    self.assertEqual(others[0]['writer_entity_id'], '000200c2')
                    self.assertEqual(others[0]['reader_entity_id'], '000200c7')
                    footers = [e for e in record['submessage_entities'] if e['classification'] == 'statistics']
                    self.assertEqual(len(footers), 1)
                    decoded = footers[0]['statistics']
                    self.assertEqual(decoded['destination'], {'kind': 1, 'port': 27412,
                        'address_hex': '0000000000000000000000007f000001'})
                    self.assertEqual(decoded['timestamp']['seconds'], 1794690572)
                    if 'ack_' in name:
                        self.assertEqual(decoded['timestamp']['fraction'], 572942668)
                        self.assertEqual(decoded['sequence'], {'sequence': 11, 'bytes': 3396, 'bytes_high': 0})
                    else:
                        self.assertEqual(decoded['timestamp']['fraction'], 518216899)
                        self.assertEqual(decoded['sequence'], {'sequence': 8, 'bytes': 3024, 'bytes_high': 0})
                    self.assertEqual(decoded['padding_hex'], '000000000000')
                    self.assertEqual(record['classification'], 'other_builtin')
                    self.assertFalse(record['parse_diagnostics'])
                    self.assertEqual(result['status'], 'unknown')  # No SPDP/SEDP observation in these frames.
                    self.assertFalse(result['activity_observed'])
                    self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
                    self.assertEqual(stopped['health']['status'], 'ok')

    def test_nonsecure_builtin_pairs_and_reader_routing_are_explicit(self):
        pairs = (('000200c2', '000200c7'), ('000300c3', '000300c4'), ('000301c3', '000301c4'))
        for vendor in ('010f', '0110'):
            for writer, reader in pairs:
                for kind in (0x15, 0x16, 0x06, 0x07, 0x08, 0x12, 0x13):
                    for destination in (reader, '00000000'):
                        with self.subTest(vendor=vendor, writer=writer, kind=kind, reader=destination):
                            packet = bytearray(frame(kind, writer=writer, reader=destination,
                                                     source_port=61000, destination_port=61002))
                            packet[48:50] = bytes.fromhex(vendor)
                            udp, _ = udp_frame(bytes(packet))
                            meta = _rtps_metadata(udp, truncated=False)
                            if destination == '00000000' and kind in (0x06, 0x12):
                                self.assertEqual(meta['classification'], 'unknown')
                                self.assertTrue(meta['parse_diagnostics'])
                            else:
                                self.assertEqual(meta['classification'], 'other_builtin')
                                self.assertEqual(meta['submessage_entities'][0]['classification'], 'other_builtin')
                                self.assertFalse(meta['parse_diagnostics'])
                            self.assertEqual(discovery_activity(bytes(packet)), ([], []))

    def test_other_builtin_counterparts_vendors_and_malformed_frames_stay_incomplete(self):
        packets = [frame(writer=writer, reader=reader, source_port=61000, destination_port=61002)
                   for writer, reader in (('000200c2', '00000107'),
                                          ('000200c2', '000300c4'),
                                          ('000200c2', '000003c7'),
                                          ('00000000', '000200c7'),
                                          ('000003c2', '000200c7'),
                                          ('00000103', '000200c7'),
                                          ('000302c3', '000302c4'),
                                          ('000201c3', '000201c4'),
                                          ('ff0200c2', 'ff0200c7'))]
        valid = frame(writer='000200c2', source_port=61000, destination_port=61002)
        vendor = bytearray(valid)
        vendor[48:50] = bytes.fromhex('ffff')
        offset = bytearray(valid)
        struct.pack_into('<H', offset, 68, 65535)
        ack = bytearray(frame(0x06, writer='000300c3', reader='000300c4',
                              source_port=61000, destination_port=61002))
        struct.pack_into('<I', ack, 82, 32)  # Advertises a bitmap word that is absent.
        packets += [bytes(vendor), bytes(offset), bytes(ack),
                    frame(writer='000200c2', prefix='0' * 24, source_port=61000, destination_port=61002),
                    with_rtps_payload(valid, valid[42:62] + submessage(0x30, valid[66:])),
                    with_rtps_payload(valid, valid[42:62] + submessage(0x15, valid[66:-4]))]
        for packet, flags in [*((p, 0) for p in packets), (valid, socket.MSG_TRUNC)]:
            with self.subTest(packet=packet.hex(), flags=flags):
                result, stopped, record, _ = self.retained_observation(packet, flags=flags)
                self.assertEqual(record['classification'], 'unknown')
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(stopped['health']['status'], 'degraded')

    def test_mixed_other_builtin_and_sedp_respects_info_source_and_destination(self):
        recorded = CAPTURED_BUILTIN_FRAMES['cyclone_participant_data'][1]
        other_messages = udp_frame(recorded)[0]['payload'][20:]
        base = frame(prefix=OTHER, source_port=61000, destination_port=61002)
        source = lambda prefix: submessage(0x0c, bytes(4) + b'\x02\x01\x01\x10' + bytes.fromhex(prefix))
        destination = submessage(0x0e, bytes.fromhex(OTHER))
        sedp = source(PREFIX) + frame()[62:]
        other = source(OTHER) + other_messages
        for messages in (sedp + other, other + sedp):
            with self.subTest(order=messages.hex()):
                packet = with_rtps_payload(base, base[42:62] + destination + messages)
                result, stopped, record, _ = self.retained_observation(packet)
                self.assertEqual(record['classification'], 'discovery')
                self.assertEqual(result['status'], 'observed')
                self.assertEqual(result['counts_by_kind'], {'SEDP_DATA': 1})
                self.assertEqual(result['events'][0]['source_guid_prefix'], PREFIX)
                self.assertEqual(result['events'][0]['destination_guid_prefix'], OTHER)
                self.assertEqual(stopped['health']['status'], 'ok')
                others = [e for e in record['submessage_entities'] if e['classification'] == 'other_builtin']
                self.assertTrue(others)
                self.assertTrue(all(e['source_guid_prefix'] == OTHER for e in others))
                self.assertFalse(record['parse_diagnostics'])

    def test_other_builtin_vendor_support_uses_effective_info_source(self):
        for header_vendor, effective_vendor in (('ffff', '010f'), ('ffff', '0110'), ('0110', 'ffff')):
            with self.subTest(header_vendor=header_vendor, effective_vendor=effective_vendor):
                base = bytearray(frame(writer='000200c2', source_port=61000, destination_port=61002))
                base[48:50] = bytes.fromhex(header_vendor)
                context = submessage(0x0c, bytes(4) + b'\x02\x01' + bytes.fromhex(effective_vendor) + bytes.fromhex(PREFIX))
                packet = with_rtps_payload(bytes(base), bytes(base[42:62]) + context + bytes(base[62:]))
                result, stopped, record, _ = self.retained_observation(packet)
                if effective_vendor == 'ffff':
                    self.assertEqual(record['classification'], 'unknown')
                    self.assertIn('unsupported_other_builtin_vendor', record['submessage_entities'][-1]['reason'])
                    self.assertEqual(result['observation_completeness'], 'incomplete')
                    self.assertEqual(stopped['health']['status'], 'degraded')
                else:
                    self.assertEqual(record['classification'], 'other_builtin')
                    self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(record['activity'], [])

    def statistics_packet(self, *, messages=b'', body=None, vendor='010f', kind=0x80,
                          flags=1, declared_length=56, suffix=b''):
        # Keep the exact recorded footer body unless a test explicitly changes it.
        if body is None:
            body = CAPTURED_BUILTIN_FRAMES['fastdds_participant_ack_with_unknown_extension'][1][-56:]
        base = frame(source_port=61000, destination_port=61002)
        header = bytearray(base[42:62])
        header[6:8] = bytes.fromhex(vendor)
        footer = bytes([kind, flags]) + struct.pack('<H' if flags & 1 else '>H', declared_length) + body
        return with_rtps_payload(base, bytes(header) + messages + footer + suffix)

    def test_statistics_standalone_and_mixed_user_are_retained(self):
        user = frame(writer='00000103')[62:]
        for messages in (b'', user, submessage(0x0e, bytes.fromhex(OTHER)) + user):
            with self.subTest(messages=messages.hex()):
                result, stopped, record, _ = self.retained_observation(self.statistics_packet(messages=messages))
                self.assertEqual(record['classification'], 'statistics')
                self.assertEqual(record['submessage_entities'][-1]['classification'], 'statistics')
                self.assertFalse(record['parse_diagnostics'])
                self.assertEqual(result['events'], [])
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
                self.assertEqual(stopped['health']['status'], 'ok')

    def test_statistics_mixed_sedp_preserves_only_discovery_activity(self):
        user = frame(writer='00000103')[62:]
        for writer, family in (('000100c2', 'SPDP'), ('000003c2', 'SEDP')):
            with self.subTest(writer=writer):
                packet = self.statistics_packet(messages=user + frame(writer=writer)[62:])
                result, stopped, record, _ = self.retained_observation(packet)
                self.assertEqual(record['classification'], 'discovery')
                self.assertEqual(result['status'], 'observed')
                self.assertEqual(result['counts_by_kind'], {family + '_DATA': 1})
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(len(record['activity']), 1)

    def test_statistics_both_endian_encodings_decode_field_boundaries(self):
        address = bytes(range(16))
        for endian, flags in (('<', 1), ('>', 0)):
            with self.subTest(endian=endian):
                # Exercise the official field widths without inferring transport
                # or target semantics from these supporting statistics values.
                body = struct.pack(endian + 'iI16siIQQH', 1, 0xffffffff, address,
                                   -0x80000000, 0xffffffff, 0xffffffffffffffff,
                                   0xffffffffffffffff, 0xffff) + bytes(6)
                result, stopped, record, _ = self.retained_observation(self.statistics_packet(body=body, flags=flags))
                decoded = record['submessage_entities'][0]['statistics']
                self.assertEqual(decoded['body_size'], 56)
                self.assertEqual(decoded['declared_length'], 56)
                self.assertEqual(decoded['destination'], {'kind': 1, 'port': 0xffffffff, 'address_hex': address.hex()})
                self.assertEqual(decoded['timestamp'], {'seconds': -0x80000000, 'fraction': 0xffffffff})
                self.assertEqual(decoded['sequence'], {'sequence': 0xffffffffffffffff,
                                                       'bytes': 0xffffffffffffffff, 'bytes_high': 0xffff})
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(result['events'], [])
        # The official add routine initially writes an all-zero body. Do not
        # impose positive timestamp/sequence/locator values that it never checks.
        _, stopped, record, _ = self.retained_observation(self.statistics_packet(body=bytes(56)))
        self.assertEqual(record['classification'], 'statistics')
        self.assertEqual(stopped['health']['status'], 'ok')

    def test_statistics_length_flags_position_vendor_and_padding_are_strict(self):
        body = CAPTURED_BUILTIN_FRAMES['fastdds_participant_ack_with_unknown_extension'][1][-56:]
        cases = [('length_zero_remaining_body_56', {'declared_length': 0}, 'invalid_statistics_length'),
                 ('length_52', {'declared_length': 52}, 'invalid_statistics_length'),
                 ('body_55', {'declared_length': 55, 'body': body[:-1]}, 'invalid_statistics_length'),
                 ('body_57', {'declared_length': 57, 'body': body + b'\0'}, 'invalid_statistics_length'),
                 ('length_60_body_56', {'declared_length': 60}, 'truncated_submessage'),
                 ('not_last', {'suffix': submessage(0x01, bytes(4))}, 'statistics_not_final_submessage'),
                 ('duplicate', {'suffix': submessage(0x80, body)}, 'statistics_not_final_submessage'),
                 ('cyclone_vendor', {'vendor': '0110'}, 'unsupported_statistics_vendor'),
                 ('unknown_vendor', {'vendor': 'ffff'}, 'unsupported_statistics_vendor'),
                 ('unknown_kind', {'kind': 0x81}, 'unsupported_submessage')]
        cases += [('flags_' + str(flags), {'flags': flags}, 'invalid_statistics_flags')
                  for flags in (2, 3, 0x40, 0x81, 0xff)]
        for offset in (50, 55):
            padding = bytearray(body)
            padding[offset] = 1
            cases.append(('padding_' + str(offset), {'body': bytes(padding)}, 'nonzero_statistics_padding'))
        for name, arguments, reason in cases:
            with self.subTest(case=name):
                result, stopped, record, _ = self.retained_observation(self.statistics_packet(**arguments))
                self.assertEqual(record['classification'], 'unknown')
                self.assertTrue(any(d.startswith(reason) for d in record['parse_diagnostics']), record['parse_diagnostics'])
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(stopped['health']['status'], 'degraded')

    def test_statistics_info_source_vendor_context_is_effective(self):
        for header_vendor, effective_vendor, expected in (('ffff', '010f', 'statistics'),
                                                         ('0110', '010f', 'statistics'),
                                                         ('010f', '0110', 'unknown'),
                                                         ('010f', 'ffff', 'unknown')):
            with self.subTest(header_vendor=header_vendor, effective_vendor=effective_vendor):
                context = (submessage(0x0c, bytes(4) + b'\x02\x01' + bytes.fromhex(effective_vendor) + bytes.fromhex(PREFIX))
                           + submessage(0x0e, bytes.fromhex(OTHER)))
                packet = self.statistics_packet(messages=context, vendor=header_vendor)
                result, stopped, record, _ = self.retained_observation(packet)
                self.assertEqual(record['classification'], expected)
                self.assertEqual(stopped['health']['status'], 'ok' if expected == 'statistics' else 'degraded')
                self.assertEqual(result['events'], [])
                if expected == 'unknown':
                    self.assertIn('unsupported_statistics_vendor', record['submessage_entities'][-1]['reason'])

    def test_statistics_cannot_hide_unknown_entity_kind_or_invalid_info_source(self):
        messages = (frame(writer='000202c2')[62:], submessage(0x30, bytes(16)),
                    submessage(0x0c, bytes(4)))
        for preceding in messages:
            with self.subTest(preceding=preceding.hex()):
                result, stopped, record, _ = self.retained_observation(self.statistics_packet(messages=preceding))
                self.assertEqual(record['submessage_entities'][-1]['classification'], 'statistics')
                self.assertEqual(record['classification'], 'unknown')
                self.assertTrue(record['parse_diagnostics'])
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(stopped['health']['status'], 'degraded')

    def test_statistics_truncation_is_retained_and_incomplete(self):
        packet = self.statistics_packet()
        result, stopped, record, _ = self.retained_observation(packet, flags=socket.MSG_TRUNC)
        self.assertEqual(record['submessage_entities'][0]['classification'], 'statistics')
        self.assertEqual(record['classification'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertIn('truncated_frames', stopped['health']['reasons'])
        packet = self.statistics_packet(body=packet[-56:-1])  # Header still declares 56.
        result, stopped, record, _ = self.retained_observation(packet)
        self.assertEqual(record['classification'], 'unknown')
        self.assertIn('truncated_submessage', record['parse_diagnostics'])
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertEqual(stopped['health']['status'], 'degraded')

    def test_other_builtin_plus_user_stays_retained_without_discovery_activity(self):
        builtin = frame(writer='000200c2', source_port=61000, destination_port=61002)
        user = frame(writer='00000103')
        packet = with_rtps_payload(builtin, builtin[42:] + user[62:])
        result, stopped, record, _ = self.retained_observation(packet)
        self.assertEqual(record['classification'], 'other_builtin')
        self.assertEqual(result['events'], [])
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
        self.assertEqual(stopped['health']['status'], 'ok')

    def test_auxiliary_prefixes_do_not_turn_user_traffic_into_discovery(self):
        packet = frame(writer='00000103', prefix=OTHER, source_port=61000, destination_port=61002)
        context = (submessage(0x0c, bytes(4) + b'\x02\x01\x01\x10' + bytes.fromhex(PREFIX)) +
                   submessage(0x0e, bytes.fromhex(PREFIX)) + submessage(0x09, bytes(8)))
        packet = with_rtps_payload(packet, packet[42:62] + context + packet[62:])
        result, stopped, record = self.filtered_observation(packet)
        self.assertEqual(stopped['health']['status'], 'ok')
        self.assertEqual(result['filtered_rtps']['target_related'], [])
        self.assertIn(PREFIX, record['source_participant_prefixes'])
        self.assertIn(PREFIX, record['destination_participant_prefixes'])
        self.assertEqual(record['submessage_entities'][-1]['source_guid_prefix'], PREFIX)
        self.assertEqual(record['classification'], 'proven_no_discovery')

    def test_discovery_effective_source_and_destination_are_retained(self):
        packet = frame(prefix=OTHER, source_port=61000, destination_port=61002)
        for kind, context in ((0x0c, submessage(0x0c, bytes(4) + b'\x02\x01\x01\x10' + bytes.fromhex(PREFIX))),
                              (0x0e, submessage(0x0e, bytes.fromhex(PREFIX)))):
            with self.subTest(kind=kind):
                result, stopped, record, _ = self.retained_observation(with_rtps_payload(packet, packet[42:62] + context + packet[62:]))
                self.assertEqual(record['classification'], 'discovery')
                self.assertEqual(stopped['health']['status'], 'ok')
                event = record['activity'][0]
                self.assertEqual(event['source_guid_prefix'] if kind == 0x0c else event['destination_guid_prefix'], PREFIX)
                self.assertEqual(result['status'], 'observed' if kind == 0x0c else 'unknown')

    def test_user_body_endianness_and_control_bitmaps_are_decoded(self):
        base = frame(writer='00000103', source_port=61000, destination_port=61002)
        reader, writer = bytes.fromhex('00000207'), bytes.fromhex('00000103')
        for endian, flags in (('<', 1), ('>', 0)):
            for kind in (0x15, 0x06, 0x07, 0x08, 0x12, 0x13):
                sn = struct.pack(endian + 'iI', 0, 1)
                if kind == 0x15:
                    body = bytes(2) + struct.pack(endian + 'H', 16) + reader + writer + sn
                else:
                    body = reader + writer + sn
                    if kind == 0x06:
                        body += struct.pack(endian + 'III', 32, 1, 1)
                    elif kind == 0x07:
                        body += sn + struct.pack(endian + 'I', 1)
                    elif kind == 0x08:
                        body += sn + struct.pack(endian + 'II', 32, 1)
                    elif kind == 0x12:
                        body += struct.pack(endian + 'IIII', 1, 32, 1, 1)
                    elif kind == 0x13:
                        body += struct.pack(endian + 'II', 1, 1)
                with self.subTest(endian=endian, kind=kind):
                    result, stopped, record = self.filtered_observation(with_rtps_payload(base, base[42:62] + submessage(kind, body, flags)))
                    self.assertEqual(record['classification'], 'proven_no_discovery')
                    self.assertEqual(result['filtered_rtps']['target_related'], [])
                    self.assertEqual(stopped['health']['status'], 'ok')

    def test_mixed_user_and_discovery_frame_is_not_missed(self):
        user = frame(writer='00000103', source_port=61000, destination_port=61002)
        discovery = frame()
        for messages in (user[62:] + discovery[62:], discovery[62:] + user[62:]):
            with self.subTest(messages=messages.hex()):
                result, stopped, record, _ = self.retained_observation(with_rtps_payload(user, user[42:62] + messages))
                self.assertEqual(record['classification'], 'discovery')
                self.assertEqual(result['status'], 'observed')
                self.assertEqual(result['deduplicated_event_count'], 1)
                self.assertEqual(stopped['health']['status'], 'ok')

    def test_discovery_attribution_uses_its_own_effective_prefix(self):
        user = frame(writer='00000103', source_port=61000, destination_port=61002)
        context = submessage(0x0c, bytes(4) + b'\x02\x01\x01\x10' + bytes.fromhex(OTHER))
        packet = with_rtps_payload(user, user[42:62] + user[62:] + context + frame()[62:])
        result, stopped, record, _ = self.retained_observation(packet)
        self.assertEqual(record['classification'], 'discovery')
        self.assertEqual(record['source_participant_prefix'], PREFIX)
        self.assertEqual(record['submessage_entities'][-1]['source_guid_prefix'], OTHER)
        self.assertEqual(result['filtered_rtps']['target_related'], [])
        self.assertEqual(stopped['health']['status'], 'ok')

    def test_unknown_entities_controls_and_malformed_user_packets_stay_incomplete(self):
        user = frame(writer='00000103', source_port=61000, destination_port=61002)
        heartbeat = frame(0x07, writer='00000103', source_port=61000, destination_port=61002)
        ack = bytearray(frame(0x06, writer='00000103', source_port=61000, destination_port=61002))
        struct.pack_into('<I', ack, 82, 32)  # Bitmap advertised without its word.
        invalid_offset = bytearray(user)
        struct.pack_into('<H', invalid_offset, 68, 65535)
        inline = bytearray(user)
        inline[63] |= 2  # Inline QoS flag without a sentinel.
        packets = [frame(writer=writer, source_port=61000, destination_port=61002)
                   for writer in ('00000000', '000001ff', '000002c2')]
        fragment = frame(0x16, writer='00000103', source_port=61000, destination_port=61002)
        packets += [frame(writer='00000103', prefix='0' * 24, source_port=61000, destination_port=61002),
                    with_rtps_payload(fragment, fragment[42:62] + submessage(0x16, fragment[66:-4])),
                    with_rtps_payload(user, user[42:62] + submessage(0x30, bytes(16))),
                    with_rtps_payload(user, user[42:62] + submessage(0x07, heartbeat[66:-4])),
                    with_rtps_payload(user, user[42:62] + submessage(0x15, user[66:-4])),
                    bytes(ack), bytes(invalid_offset), bytes(inline),
                    with_rtps_payload(user, user[42:] + b'\x15'),
                    with_rtps_payload(user, user[42:62] + submessage(0x0e, bytes(4)) + user[62:])]
        for packet in packets:
            with self.subTest(packet=packet.hex()):
                result, stopped, record, _ = self.retained_observation(packet)
                self.assertEqual(record['classification'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(stopped['health']['status'], 'degraded')

    def test_truncated_user_frame_stays_unknown_even_with_decodable_entities(self):
        packet = frame(writer='00000103', source_port=61000, destination_port=61002)
        result, stopped, record, _ = self.retained_observation(packet, flags=socket.MSG_TRUNC)
        self.assertEqual(record['submessage_entities'][0]['classification'], 'user')
        self.assertEqual(record['classification'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertEqual(stopped['health']['status'], 'degraded')

    def test_filtered_user_frame_cannot_taint_positive_discovery_activity(self):
        packet = frame(writer='00000103', source_port=61000, destination_port=61002)
        result, stopped, _ = self.filtered_observation(packet, retained=(frame(),))
        self.assertEqual(result['status'], 'observed')
        self.assertEqual(stopped['health']['status'], 'ok')
        self.assertEqual(result['deduplicated_event_count'], 1)

    def test_unrelated_discovery_is_retained_before_target_prefixes_are_known(self):
        packet = frame(prefix=OTHER, source_port=61000, destination_port=61002)
        result, stopped, record, _ = self.retained_observation(packet)
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
        self.assertEqual(record['activity'][0]['source_guid_prefix'], OTHER)
        self.assertEqual(stopped['health']['status'], 'ok')

    def test_unassignable_nonstandard_rtps_is_retained_and_incomplete(self):
        result, stopped, record, _ = self.retained_observation(unassignable_rtps_frame())
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertIsNone(record['source_participant_prefix'])
        self.assertTrue(record['parse_diagnostics'])
        self.assertEqual(stopped['health']['status'], 'degraded')

    def test_nonstandard_truncated_discovery_is_retained_and_incomplete(self):
        packet = frame(prefix=OTHER, source_port=61000, destination_port=61002)
        result, stopped, record, _ = self.retained_observation(packet, flags=socket.MSG_TRUNC)
        self.assertEqual(record['classification'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertEqual(stopped['health']['status'], 'degraded')
        self.assertIn('truncated_frames', stopped['health']['reasons'])

    def test_filtered_metadata_overflow_is_explicitly_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            packets = [frame(writer='00000103', prefix=OTHER, source_port=61000, destination_port=61002) for _ in range(2)]
            observer = PassiveSedpObserver(self.spec(path, limits={'max_packets': 1}))
            fake = FakeSocket([(packets[0], None), (packets[1], None)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['filtered_rtps'] < 2 and time.monotonic() < end: time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['health']['filtered_rtps_completeness'], 'incomplete')
                self.assertIn('filtered_rtps_metadata_overflow', result['health']['reasons'])
                stopped = observer.stop()
                self.assertEqual(stopped['filtered_rtps_index']['records'], 1)
                self.assertEqual(stopped['filtered_rtps_index']['overflow'], 1)

    def test_parse_gap_cannot_be_hidden_by_positive_discovery_activity(self):
        packet = frame(source_port=61000, destination_port=61002)
        packet = with_rtps_payload(packet, packet[42:] + submessage(0x30, bytes(16)))
        result, stopped, record, _ = self.retained_observation(packet)
        self.assertEqual(record['classification'], 'unknown')
        self.assertTrue(record['activity'])
        self.assertTrue(record['parse_diagnostics'])
        self.assertTrue(record['diagnostics'])
        self.assertTrue(result['activity_observed'])
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertIn('unsupported_or_invalid_framing', stopped['health']['reasons'])

    def test_truncated_transport_envelope_is_retained_and_incomplete(self):
        packet = frame(source_port=61000, destination_port=61002)[:-10]
        result, stopped, record, _ = self.retained_observation(packet)
        self.assertIsNone(record['rtps_metadata'])
        self.assertIn('truncated_or_invalid_ipv4', record['diagnostics'])
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['observation_completeness'], 'incomplete')
        self.assertEqual(stopped['health']['status'], 'degraded')

    def test_truncated_unrecognizable_payload_is_retained(self):
        base = frame(source_port=61000, destination_port=61002)
        for payload in (b'RTP', b''):
            with self.subTest(payload=payload):
                result, stopped, record, _ = self.retained_observation(
                    with_rtps_payload(base, payload), flags=socket.MSG_TRUNC)
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(stopped['health']['status'], 'degraded')
                self.assertIn('truncated_frames', stopped['health']['reasons'])
                self.assertIsNone(record['rtps_metadata'])

    def test_nonstandard_potential_discovery_respects_capture_limits(self):
        packet = frame(source_port=61000, destination_port=61002)
        for limit, limits, frames, expected_written in (
                ('max_packets', {'max_packets': 1}, [(packet, None)] * 2, 1),
                ('max_bytes', {'snaplen': 256, 'max_bytes': 296}, [(packet, None)] * 3, 2),
                ('timeout_sec', {'timeout_sec': .03}, [], 0)):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp)
                observer = PassiveSedpObserver(self.spec(path, limits=limits))
                fake = FakeSocket(frames)
                with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                    ready = observer.start()
                    observer._thread.join(timeout=1)
                    self.assertFalse(observer._thread.is_alive())
                    result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                    stopped = observer.stop()
                self.assertEqual(stopped['limit_reason'], limit)
                self.assertEqual(stopped['counters'].get('packets_written', 0), expected_written)
                self.assertEqual(stopped['filtered_rtps_index']['records'], 0)
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertIn('capture_limit:' + limit, stopped['health']['reasons'])
                _, records, diagnostics = read_capture(stopped)
                self.assertFalse(diagnostics)
                self.assertEqual([record['frame'] for record in records], [packet] * expected_written)
                self.assertLessEqual(stopped['raw']['size'], observer.spec['limits']['max_bytes'])

    def test_kernel_statistics_failure_is_not_healthy(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = PassiveSedpObserver(self.spec(Path(tmp)))
            fake = FakeSocket([], statistics_error=True)
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                observer.start(); stopped = observer.stop()
            self.assertEqual(stopped['health']['status'], 'degraded')
            self.assertIn('packet_statistics', stopped['error'])

    def test_existing_artifact_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)/'normal-rtps.pcap';p.write_bytes(b'preserve')
            observer = PassiveSedpObserver(self.spec(Path(tmp)))
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={}):
                with self.assertRaises(FileExistsError): observer.start()
            self.assertEqual(p.read_bytes(), b'preserve')

    def test_truncated_framing_is_explicit(self):
        events, errors = discovery_activity(frame()[:-10])
        self.assertFalse(events)
        self.assertIn('truncated_or_invalid_ipv4', errors)


if __name__ == '__main__': unittest.main()
