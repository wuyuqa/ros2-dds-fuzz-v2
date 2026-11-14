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

from r2f.instrumentation.sedp import PassiveSedpObserver, build_capture_spec
from r2f.instrumentation.rtps import discovery_activity

PREFIX = '011000000000000000000001'
OTHER = '011000000000000000000002'
PROOF = {'host_net_namespace': 'net:[1]', 'host_user_namespace': 'user:[2]',
         'net_namespace': 'net:[3]', 'user_namespace': 'user:[4]',
         'host_uid': 1000, 'host_cap_eff': 0}


def frame(kind=0x15, writer='000003c2', prefix=PREFIX, sequence=1,
          source_port=53410, destination_port=53412):
    if kind == 0x15:
        body = b'\0\0' + struct.pack('<H', 16) + bytes(4) + bytes.fromhex(writer) + struct.pack('<iI', 0, sequence)
    else:
        body = bytes(4) + bytes.fromhex(writer) + struct.pack('<iI', 0, sequence)
        if kind == 0x07:
            body += struct.pack('<iII', 0, sequence, 1)
        elif kind == 0x06:
            body += struct.pack('<II', 0, 1)
    rtps = b'RTPS\x02\x01\x01\x10' + bytes.fromhex(prefix) + bytes([kind, 1]) + struct.pack('<H', len(body)) + body
    udp = struct.pack('!HHHH', source_port, destination_port, len(rtps) + 8, 0) + rtps
    ip = bytes.fromhex('4500') + struct.pack('!H', len(udp) + 20) + bytes(5) + b'\x11' + bytes(2) + b'\x7f\x00\x00\x01' * 2
    return bytes(12) + b'\x08\x00' + ip + udp


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

    def test_filtered_target_rtps_is_indexed_and_makes_observation_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            packet = frame(prefix=PREFIX, source_port=61000, destination_port=61002)
            observer = PassiveSedpObserver(self.spec(path))
            fake = FakeSocket([(packet, None)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['filtered_rtps'] < 1 and time.monotonic() < end: time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(result['health']['filtered_rtps_completeness'], 'incomplete')
                self.assertIn('filtered_target_related_rtps', result['health']['reasons'])
                detail = result['filtered_rtps']['target_related'][0]
                self.assertEqual((detail['source_port'], detail['destination_port']), (61000, 61002))
                self.assertEqual(detail['source_participant_prefix'], PREFIX)
                self.assertEqual(detail['frame_sha256'], hashlib.sha256(packet).hexdigest())
                self.assertEqual(detail['captured_length'], len(packet))
                self.assertFalse(detail['truncated'])
                self.assertEqual(detail['domain_id'], 184)
                self.assertEqual(detail['domain_relation'], 'outside_domain_port_block')
                self.assertIn('DATA', detail['submessage_kinds'])
                stopped = observer.stop()
                index_path = path/'filtered-rtps-index.jsonl'
                self.assertEqual(stopped['filtered_rtps_index']['sha256'], hashlib.sha256(index_path.read_bytes()).hexdigest())
                self.assertEqual(json.loads(index_path.read_text())['filter_reason'], 'rtps_ports_outside_standard_discovery_set')
                self.assertEqual(stopped['health']['status'], 'degraded')

    def test_filtered_unrelated_rtps_is_audited_without_health_downgrade(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            packet = frame(prefix=OTHER, source_port=61000, destination_port=61002)
            observer = PassiveSedpObserver(self.spec(path))
            fake = FakeSocket([(packet, None)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['filtered_rtps'] < 1 and time.monotonic() < end: time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                self.assertEqual(result['status'], 'unknown')  # no positive target activity
                self.assertEqual(result['observation_completeness'], 'complete_for_known_target_prefixes')
                self.assertEqual(result['health']['status'], 'ok')
                self.assertEqual(result['health']['filtered_rtps_target_related_count'], 0)
                self.assertEqual(result['filtered_rtps']['target_related'], [])
                stopped = observer.stop()
                self.assertEqual(stopped['health']['status'], 'ok')
                self.assertEqual(stopped['filtered_rtps_index']['records'], 1)

    def test_unassignable_filtered_rtps_outside_domain_port_block_is_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            packet = unassignable_rtps_frame()
            observer = PassiveSedpObserver(self.spec(path))
            fake = FakeSocket([(packet, None)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['filtered_rtps'] < 1 and time.monotonic() < end: time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                self.assertEqual(result['status'], 'unknown')
                self.assertEqual(result['observation_completeness'], 'incomplete')
                self.assertEqual(result['health']['filtered_rtps_completeness'], 'incomplete')
                detail = result['filtered_rtps']['target_related'][0]
                self.assertIsNone(detail['source_participant_prefix'])
                self.assertTrue(detail['parse_diagnostics'])
                self.assertEqual(detail['domain_relation'], 'outside_domain_port_block')
                stopped = observer.stop()
                self.assertEqual(stopped['health']['status'], 'degraded')

    def test_filtered_frame_records_digest_length_and_truncation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            packet = frame(prefix=OTHER, source_port=61000, destination_port=61002)
            observer = PassiveSedpObserver(self.spec(path))
            fake = FakeSocket([(packet, None, socket.MSG_TRUNC)])
            with patch('r2f.instrumentation.sedp_capture.validate_capture_namespace', return_value={'isolated': True}), patch('r2f.instrumentation.sedp_capture.socket.socket', return_value=fake):
                ready = observer.start()
                end = time.monotonic() + 1
                while observer._counters['filtered_rtps'] < 1 and time.monotonic() < end: time.sleep(.005)
                result = observer.snapshot_activity(since_ns=ready['unix_ns'], participant_prefixes=[PREFIX])
                self.assertEqual(result['health']['filtered_rtps_target_related_count'], 0)
                record = json.loads((path/'filtered-rtps-index.jsonl').read_text())
                self.assertEqual(record['frame_sha256'], hashlib.sha256(packet).hexdigest())
                self.assertEqual(record['captured_length'], len(packet))
                self.assertTrue(record['truncated'])
                stopped = observer.stop()
                self.assertEqual(stopped['filtered_rtps_index']['sha256'], hashlib.sha256((path/'filtered-rtps-index.jsonl').read_bytes()).hexdigest())

    def test_filtered_metadata_overflow_is_explicitly_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            packets = [frame(prefix=OTHER, source_port=61000, destination_port=61002) for _ in range(2)]
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
