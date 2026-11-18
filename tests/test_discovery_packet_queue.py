"""Queue/parser contracts plus real loopback UDP, all synthetic and non-DDS.

    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover \
        -s tests -p test_discovery_packet_queue.py -v

No ROS imports, endpoints, domain/campaign locks or live mutation evidence.
"""
import hashlib
import select
import socket
import struct
import threading
import time
import unittest

from r2f.runtime.discovery_packet_queue import (
    IdentityBinding, PacketQueue, PacketQueueError, QueuePolicy,
    UDPDiscoveryCarrier, classify_rtps,
)

SOURCE = '0102030405060708090a0b0c'
DESTINATION = '1112131415161718191a1b1c'
SRC_ADDR = ('127.0.0.1', 19000)
DST_ADDR = ('127.0.0.1', 19001)


def submessage(kind, body=b'', *, endian='<', flags=None, zero=False):
    flags = (1 if endian == '<' else 0) if flags is None else flags
    return bytes([kind, flags]) + struct.pack(endian + 'H', 0 if zero else len(body)) + body


def header(prefix=SOURCE):
    return b'RTPS\x02\x03\x01\x10' + bytes.fromhex(prefix)


def data(sequence=1, *, writer='000003c2', reader='000003c7', endian='<',
         inline=None, zero=False):
    body = struct.pack(endian + 'HH', 0, 16) + bytes.fromhex(reader + writer)
    body += struct.pack(endian + 'iI', 0, sequence)
    flags = (1 if endian == '<' else 0) | 4
    if inline is not None:
        flags |= 2
        body += inline
    body += b'\x00\x01\x00\x00' + b'synthetic!\x00\x00'
    return submessage(21, body, endian=endian, flags=flags, zero=zero)


def packet(sequence=1, *, prefix=SOURCE, destination=DESTINATION, **kwargs):
    context = submessage(14, bytes.fromhex(destination)) if destination else b''
    return header(prefix) + context + data(sequence, **kwargs)


def control(kind, *, reader='000003c7', writer='000003c2', endian='<', bits=0):
    body = bytes.fromhex(reader + writer)
    if kind == 6:
        body += struct.pack(endian + 'iII', 0, 1, bits) + bytes(4 * ((bits + 31) // 32)) + struct.pack(endian + 'I', 1)
    elif kind == 7:
        body += struct.pack(endian + 'iIiII', 0, 1, 0, 2, 1)
    elif kind == 8:
        body += struct.pack(endian + 'iIiII', 0, 1, 0, 2, bits) + bytes(4 * ((bits + 31) // 32))
    elif kind == 18:
        body += struct.pack(endian + 'iIII', 0, 1, 1, bits) + bytes(4 * ((bits + 31) // 32)) + struct.pack(endian + 'I', 1)
    elif kind == 19:
        body += struct.pack(endian + 'iIII', 0, 1, 1, 1)
    return submessage(kind, body, endian=endian)


class Clock:
    def __init__(self, ns=1000):
        self.ns = ns

    def __call__(self):
        return self.ns

    def advance(self, ns):
        self.ns += ns


class ParserTests(unittest.TestCase):
    def test_exact_discovery_entity_pairs_both_endians(self):
        for endian in ('<', '>'):
            for writer, reader in (('000100c2', '000100c7'), ('000002c2', '000002c7'),
                                   ('000003c2', '000003c7'), ('000004c2', '000004c7')):
                with self.subTest(endian=endian, writer=writer):
                    parsed = classify_rtps(packet(writer=writer, reader=reader, endian=endian))
                    self.assertEqual(parsed['classification'], 'discovery', parsed)
                    self.assertEqual(parsed['diagnostics'], [])
                    item = parsed['submessages'][-1]
                    self.assertEqual(item['source_endpoint_guid'], SOURCE + writer)
                    self.assertEqual(item['destination_guid_prefix'], DESTINATION)
                    self.assertFalse(parsed['identity_is_authenticated'])

    def test_prefix_and_port_are_not_classifiers(self):
        self.assertEqual(classify_rtps(b'not-RTPS')['classification'], 'not_rtps')
        self.assertEqual(classify_rtps(b'RTPS fake')['classification'], 'unknown')
        user = classify_rtps(packet(writer='00000102', reader='00000107'))
        self.assertEqual(user['classification'], 'user')
        self.assertFalse(user['has_discovery'])
        bad = classify_rtps(packet(writer='000003c2', reader='000004c7'))
        self.assertEqual(bad['classification'], 'unknown')
        self.assertIn('builtin_counterpart_mismatch', str(bad['diagnostics']))

    def test_control_bitmap_and_reader_origin_identity(self):
        for endian in ('<', '>'):
            for kind in (6, 7, 8, 18, 19):
                with self.subTest(kind=kind, endian=endian):
                    parsed = classify_rtps(header() + submessage(14, bytes.fromhex(DESTINATION)) + control(kind, endian=endian, bits=33))
                    self.assertEqual(parsed['classification'], 'discovery', parsed)
                    sender = '000003c7' if kind in (6, 18) else '000003c2'
                    self.assertEqual(parsed['submessages'][-1]['source_endpoint_guid'], SOURCE + sender)
        bad = classify_rtps(header() + control(6, bits=257))
        self.assertEqual(bad['classification'], 'unknown')
        truncated = classify_rtps(header() + control(18, bits=64)[:-4])
        self.assertEqual(truncated['classification'], 'unknown')

    def test_INFO_SRC_DST_change_following_context(self):
        replacement = 'a1' * 12
        info_src = submessage(12, b'\0' * 4 + b'\x02\x03\x01\x10' + bytes.fromhex(replacement))
        body = header() + submessage(14, bytes.fromhex(DESTINATION)) + data(1) + info_src + submessage(14, bytes(12)) + data(2)
        parsed = classify_rtps(body)
        self.assertEqual(parsed['classification'], 'discovery', parsed)
        records = [m for m in parsed['submessages'] if m['kind'] == 21]
        self.assertEqual(records[0]['source_guid_prefix'], SOURCE)
        self.assertEqual(records[0]['destination_guid_prefix'], DESTINATION)
        self.assertEqual(records[1]['source_guid_prefix'], replacement)
        self.assertIsNone(records[1]['destination_guid_prefix'])

    def test_zero_length_PAD_and_INFO_TS_do_not_swallow_DATA(self):
        body = header() + submessage(1) + submessage(9, flags=3) + data(zero=True)
        parsed = classify_rtps(body)
        self.assertEqual(parsed['classification'], 'discovery', parsed)
        self.assertEqual([m['kind'] for m in parsed['submessages']], [1, 9, 21])

    def test_inline_qos_structural_bounds(self):
        valid = struct.pack('<HHIHH', 0x71, 4, 0, 1, 0)
        self.assertEqual(classify_rtps(packet(inline=valid))['classification'], 'discovery')
        for bad in (struct.pack('<HH', 0x71, 1000), struct.pack('<HH', 0x71, 3), b''):
            with self.subTest(inline=bad):
                self.assertEqual(classify_rtps(packet(inline=bad))['classification'], 'unknown')
        bad = bytearray(packet())
        struct.pack_into('<H', bad, 20 + 16 + 4 + 2, 65535)
        self.assertEqual(classify_rtps(bytes(bad))['classification'], 'unknown')

    def test_DATA_FRAG_requires_actual_fragment_fields_and_payload_bounds(self):
        body = struct.pack('<HH', 0, 28) + bytes.fromhex('000003c7000003c2')
        body += struct.pack('<iIIHHI', 0, 1, 1, 1, 8, 8) + b'abcdefgh'
        parsed = classify_rtps(header() + submessage(22, body))
        self.assertEqual(parsed['classification'], 'discovery', parsed)
        for bad in (body[:24], body[:-4], body[:20] + bytes(12) + body[32:]):
            self.assertEqual(classify_rtps(header() + submessage(22, bad))['classification'], 'unknown')

    def test_mixed_and_unsupported_contents_are_explicit(self):
        parsed = classify_rtps(header() + data() + data(2, writer='00000102', reader='00000107'))
        self.assertEqual(parsed['classification'], 'mixed')
        self.assertTrue(parsed['mixed'])
        self.assertTrue(parsed['has_discovery'])
        unknown = classify_rtps(header() + data() + submessage(0x99, b'\0' * 4))
        self.assertEqual(unknown['classification'], 'unknown')
        self.assertTrue(unknown['mixed'])
        self.assertTrue(unknown['has_discovery'])

    def test_malformed_version_length_context_alignment_and_limit(self):
        cases = [b'RTPS', header()[:4] + b'\x03\x00' + header()[6:],
                 header('00' * 12) + data(), header() + b'\x15\x01\xff\xff',
                 header() + submessage(12, b'\0' * 8) + data(),
                 header() + submessage(14, b'\0' * 4) + data(),
                 header() + submessage(9, b'\0' * 4) + data(),
                 header() + submessage(1, b'\0') + data(),
                 header() + data() + b'\x15']
        for body in cases:
            with self.subTest(body=body[:30]):
                parsed = classify_rtps(body)
                self.assertEqual(parsed['classification'], 'unknown', parsed)
                self.assertTrue(parsed['diagnostics'])
        self.assertEqual(classify_rtps(header() + submessage(1) * 3, max_submessages=2)['classification'], 'unknown')
        with self.assertRaises(TypeError):
            classify_rtps(bytearray(header()))


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.events = []
        self.sent = []

    def queue(self, mode='transparent', **kwargs):
        policy = QueuePolicy(mode=mode, ttl_ns=1000, reorder_window_ns=100, **kwargs)
        q = PacketQueue(policy, emit=self.events.append, clock_ns=self.clock)
        self.addCleanup(q.close)
        return q

    def send(self, payload, destination):
        self.sent.append((payload, destination, self.clock()))
        return len(payload)

    def records(self, event):
        return [r for r in self.events if r['event'] == event]

    def test_transparent_fifo_one_byte_exact_copy_for_all_datagrams(self):
        q = self.queue()
        payloads = [packet(1), b'unknown', packet(2), b'']
        for body in payloads:
            q.accept(body, SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 4)
        self.assertEqual([r[0] for r in self.sent], payloads)
        self.assertEqual(q.pending_copies, 0)
        for record, body in zip(self.records('packet_copy_sent'), payloads):
            self.assertEqual(record['original_bytes_sha256'], hashlib.sha256(body).hexdigest())
            self.assertEqual(record['sent_bytes_sha256'], record['original_bytes_sha256'])
            self.assertEqual(record['copy_index'], 0)
            self.assertEqual(record['operation'], 'transparent')
            self.assertEqual(record['source_address'], list(SRC_ADDR))
            self.assertEqual(record['destination_address'], list(DST_ADDR))
            self.assertFalse(record['pair_hit'])
            self.assertFalse(record['payload_modified'])

    def test_delay_not_before_and_actual_monotonic_release(self):
        q = self.queue('delay', delay_ns=200)
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        self.assertEqual(q.next_deadline_ns(), 1200)
        self.clock.advance(199)
        self.assertEqual(q.drain(self.send), 0)
        self.clock.advance(1)
        self.assertEqual(q.drain(self.send), 1)
        receipt = self.records('packet_copy_sent')[0]
        self.assertEqual(receipt['queue_residence_ns'], 200)
        self.assertEqual(receipt['released_monotonic_ns'], 1200)
        self.assertEqual(receipt['received_monotonic_ns'], 1000)
        self.assertEqual(receipt['operation'], 'delay')

    def test_reorder_reverses_bounded_batch_and_partial_window(self):
        q = self.queue('reorder', reorder_depth=3)
        bodies = [packet(i) for i in (1, 2, 3)]
        for body in bodies:
            q.accept(body, SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 3)
        self.assertEqual([r[0] for r in self.sent], list(reversed(bodies)))
        q.accept(packet(4), SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 0)
        self.clock.advance(100)
        self.assertEqual(q.drain(self.send), 1)
        batches = self.records('reorder_batch_released_to_scheduler')
        self.assertEqual(batches[0]['packet_ids'], [3, 2, 1])
        self.assertFalse(batches[1]['order_effect_expected'])
        self.assertEqual(q.counts['scheduled_reordered_batches'], 1)

    def test_duplicate_copy_indices_spacing_and_original_hash(self):
        q = self.queue('duplicate', copies=3, duplicate_gap_ns=10)
        body = packet()
        q.accept(body, SRC_ADDR, DST_ADDR)
        for delta in (0, 10, 10):
            self.clock.advance(delta)
            self.assertEqual(q.drain(self.send), 1)
        self.assertEqual([r[0] for r in self.sent], [body] * 3)
        receipts = self.records('packet_copy_sent')
        self.assertEqual([r['copy_index'] for r in receipts], [0, 1, 2])
        self.assertEqual([r['released_monotonic_ns'] for r in receipts], [1000, 1010, 1020])
        self.assertTrue(all(not r['correlation']['pair_hit'] for r in receipts))

    def test_reorder_new_packet_after_deadline_closes_previous_batch(self):
        q = self.queue('reorder')
        q.accept(packet(1), SRC_ADDR, DST_ADDR)
        self.clock.advance(100)
        q.accept(packet(2), SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 1)
        self.assertEqual(self.sent[0][0], packet(1))
        self.assertEqual(q.pending_copies, 1)

    def test_unknown_user_context_and_mixed_bypass_mutation_by_default(self):
        q = self.queue('delay', delay_ns=200)
        mixed = header() + data(1) + data(2, writer='00000102', reader='00000107')
        bodies = [b'RTPSinvalid', packet(writer='00000102', reader='00000107'), header(), mixed,
                  header() + data() + submessage(0x99, bytes(4))]
        for body in bodies:
            q.accept(body, SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), len(bodies))
        self.assertTrue(all(r['operation'] == 'transparent' for r in self.records('packet_received')))

    def test_mixed_optin_records_whole_datagram_collateral(self):
        q = self.queue('duplicate', copies=2, allow_mixed=True)
        body = header() + data(1) + data(2, writer='00000102', reader='00000107')
        q.accept(body, SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 2)
        self.assertEqual([x[0] for x in self.sent], [body, body])
        self.assertTrue(all(r['whole_datagram_collateral'] for r in self.records('packet_copy_sent')))
        unknown = header() + data() + submessage(0x99, bytes(4))
        q.accept(unknown, SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 1)
        self.assertEqual(self.records('packet_received')[-1]['operation'], 'transparent')

    def test_atomic_capacity_overflow_never_sends_partial_duplicate_set(self):
        q = self.queue('duplicate', copies=3, max_pending_copies=2)
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send), 0)
        self.assertEqual(q.pending_copies, 0)
        rejected = self.records('packet_rejected')[0]
        self.assertTrue(rejected['harness_failure'])
        self.assertFalse(rejected['pair_hit'])
        self.assertEqual(q.counts['overflow_packets'], 1)

    def test_bytes_and_datagram_bounds(self):
        for kwargs in ({'max_pending_bytes': 10}, {'max_datagram_bytes': 10}):
            with self.subTest(kwargs=kwargs):
                q = self.queue(**kwargs)
                q.accept(packet(), SRC_ADDR, DST_ADDR)
                self.assertEqual(q.pending_bytes, 0)
                self.assertEqual(q.drain(self.send), 0)
        with self.assertRaises(TypeError):
            q.accept(bytearray(b'bad'), SRC_ADDR, DST_ADDR)

    def test_TTL_expiration_in_heap_and_staged_batch_is_audited(self):
        for mode in ('delay', 'duplicate', 'reorder'):
            with self.subTest(mode=mode):
                q = self.queue(mode)
                q.accept(packet(), SRC_ADDR, DST_ADDR)
                self.clock.advance(1000)
                self.assertEqual(q.drain(self.send), 0)
                self.assertEqual(q.pending_copies, 0)
                self.assertEqual(q.pending_bytes, 0)
        self.assertTrue(all(r['reason'] == 'TTL_expired' for r in self.records('packet_copy_discarded')))

    def test_TTL_rechecked_before_each_actual_send(self):
        q = self.queue('duplicate', copies=2)
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        def send_and_advance(body, dest):
            self.send(body, dest)
            self.clock.advance(1000)
            return len(body)
        self.assertEqual(q.drain(send_and_advance), 1)
        self.assertEqual(q.pending_copies, 0)
        self.assertEqual(self.records('packet_copy_discarded')[0]['reason'], 'TTL_expired')

    def test_bounded_drain_preserves_transparent_order(self):
        q = self.queue()
        for index in range(4):
            q.accept(packet(index + 1), SRC_ADDR, DST_ADDR)
        self.assertEqual(q.drain(self.send, max_copies=2), 2)
        self.assertEqual(q.pending_copies, 2)
        self.assertEqual(q.drain(self.send, max_copies=2), 2)
        self.assertEqual([r[0] for r in self.sent], [packet(i) for i in (1, 2, 3, 4)])

    def test_cancellation_discards_every_pending_copy_and_rejects_late_accept(self):
        q = self.queue('duplicate', copies=3)
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        q.close(reason='test_cancel')
        q.close()
        self.assertTrue(q.closed)
        self.assertEqual(q.pending_copies, 0)
        self.assertEqual(len(self.records('packet_copy_discarded')), 3)
        self.assertEqual(len(self.records('queue_closed')), 1)
        self.assertIsNone(q.next_deadline_ns())
        with self.assertRaisesRegex(PacketQueueError, 'closed'):
            q.accept(packet(), SRC_ADDR, DST_ADDR)

    def test_send_failure_and_short_send_close_remaining_queue(self):
        for fail in (lambda *_: 0, lambda *_: (_ for _ in ()).throw(OSError('fixture send failure'))):
            with self.subTest(fail=fail):
                q = self.queue('duplicate')
                q.accept(packet(), SRC_ADDR, DST_ADDR)
                with self.assertRaises((OSError, PacketQueueError)):
                    q.drain(fail)
                self.assertTrue(q.closed)
                self.assertEqual(q.pending_copies, 0)
        self.assertEqual(self.records('packet_copy_sent'), [])
        self.assertIn('send_failed', str(self.records('packet_copy_discarded')))

    def test_monotonic_regression_closes_drain_without_sending(self):
        q = self.queue()
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        self.clock.ns = 999
        with self.assertRaisesRegex(PacketQueueError, 'monotonic_clock'):
            q.drain(self.send)
        self.assertTrue(q.closed)
        self.assertEqual(q.pending_copies, 0)
        self.assertEqual(self.sent, [])

    def test_clock_failure_after_send_retains_unknown_completion_receipt(self):
        q = self.queue()
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        def send_then_break_clock(body, destination):
            self.send(body, destination)
            self.clock.ns = 1
            return len(body)
        with self.assertRaisesRegex(PacketQueueError, 'monotonic_clock'):
            q.drain(send_then_break_clock)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.records('packet_copy_sent'), [])
        record = self.records('packet_send_completed_time_unknown')[0]
        self.assertIsNone(record['released_monotonic_ns'])
        self.assertTrue(record['harness_failure'])

    def test_receipt_sink_failure_is_fail_closed_not_silent_success(self):
        calls = []
        def broken_sink(record):
            calls.append(record)
            if record['event'] == 'packet_scheduled':
                raise OSError('fixture raw writer unavailable')
        q = PacketQueue(QueuePolicy(), emit=broken_sink, clock_ns=self.clock)
        with self.assertRaisesRegex(PacketQueueError, 'receipt_sink_failed'):
            q.accept(packet(), SRC_ADDR, DST_ADDR)
        self.assertTrue(q.closed)
        self.assertIn('fixture raw writer', q.audit_error)
        self.assertEqual(q.pending_copies, 0)
        self.assertEqual(q.pending_bytes, 0)

    def test_emit_cannot_mutate_internal_packet_identity(self):
        def tamper(record):
            record['destination_address'] = ['127.0.0.1', 1]
            if 'classification' in record:
                record['classification']['submessages'].clear()
        q = PacketQueue(QueuePolicy(), emit=tamper, clock_ns=self.clock)
        q.accept(packet(), SRC_ADDR, DST_ADDR)
        q.drain(self.send)
        self.assertEqual(self.sent[0][1], DST_ADDR)
        q.close()

    def test_binding_correlation_never_authenticates_or_claims_pair_hit(self):
        binding = IdentityBinding(SOURCE, DESTINATION, SRC_ADDR, DST_ADDR, 'raw://fresh-endpoint-snapshot')
        q = PacketQueue(QueuePolicy(), emit=self.events.append, clock_ns=self.clock, binding=binding)
        for body in (packet(), packet(destination=None), packet(prefix='ee' * 12)):
            q.accept(body, SRC_ADDR, DST_ADDR)
        q.drain(self.send)
        correlations = [r['correlation'] for r in self.records('packet_copy_sent')]
        self.assertEqual([c['confidence'] for c in correlations], [
            'wire_and_configured_tuple_correlated', 'source_only_receiver_unproven', 'mismatch_or_unknown'])
        self.assertTrue(all(not c['pair_hit'] for c in correlations))
        q.close()

    def test_policy_and_binding_validation(self):
        for kwargs in ({'mode': 'drop'}, {'copies': True}, {'copies': 17}, {'ttl_ns': 0},
                       {'max_pending_bytes': -1}, {'delay_ns': -1}, {'allow_mixed': 1},
                       {'mode': 'delay', 'delay_ns': 100, 'ttl_ns': 100},
                       {'mode': 'reorder', 'reorder_window_ns': 100, 'ttl_ns': 100},
                       {'mode': 'duplicate', 'duplicate_gap_ns': 100, 'ttl_ns': 100}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                QueuePolicy(**kwargs)
        for prefix in ('00' * 12, 'x' * 24, 'aa'):
            with self.assertRaises(ValueError):
                IdentityBinding(prefix, DESTINATION, SRC_ADDR, DST_ADDR, 'raw')
        for address in (('0.0.0.0', 1), ('224.0.0.1', 1), ('127.0.0.1', 0), ('localhost', 1)):
            with self.subTest(address=address), self.assertRaises(ValueError):
                self.queue().accept(packet(), address, DST_ADDR)


class RealLoopbackNonDDSTests(unittest.TestCase):
    """Actual UDP bytes, not DDS discovery traffic or live pair evidence."""
    def setUp(self):
        self.sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sink.bind(('127.0.0.1', 0))
        self.sink.setblocking(False)
        self.addCleanup(self.sink.close)
        self.sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sender.bind(('127.0.0.1', 0))
        self.addCleanup(self.sender.close)
        self.events = []

    def carrier(self, policy, **kwargs):
        carrier = UDPDiscoveryCarrier(('127.0.0.1', 0), self.sink.getsockname(), policy,
            emit=self.events.append, **kwargs)
        self.addCleanup(carrier.close)
        return carrier

    def receive_all(self):
        result = []
        while select.select([self.sink], [], [], 0)[0]:
            result.append(self.sink.recvfrom(65535))
        return result

    def test_real_transparent_payload_order_single_copies_and_tuple_receipts(self):
        carrier = self.carrier(QueuePolicy())
        bodies = [packet(1), b'nonRTPS', packet(2), b'']
        for body in bodies:
            self.sender.sendto(body, carrier.address)
        receipt = carrier.pump(timeout_sec=.1)
        self.assertEqual(receipt, {'received': 4, 'sent_copies': 4, 'closed': False})
        observed = self.receive_all()
        self.assertEqual([payload for payload, _ in observed], bodies)
        self.assertEqual([source for _, source in observed], [carrier.address] * 4)
        sends = [e for e in self.events if e['event'] == 'packet_copy_sent']
        for sent, body in zip(sends, bodies):
            self.assertEqual(sent['source_address'], list(self.sender.getsockname()))
            self.assertEqual(sent['actual_forward_source'], list(carrier.address))
            self.assertEqual(sent['original_bytes_sha256'], hashlib.sha256(body).hexdigest())
            self.assertLessEqual(sent['received_monotonic_ns'], sent['released_monotonic_ns'])
            self.assertFalse(sent['original_source_tuple_preserved'])
            self.assertFalse(sent['pair_hit'])

    def test_real_delay_with_controlled_clock_and_original_bytes(self):
        clock = Clock(1_000_000)
        carrier = self.carrier(QueuePolicy(mode='delay', delay_ns=5000), clock_ns=clock)
        body = packet(4)
        self.sender.sendto(body, carrier.address)
        self.assertEqual(carrier.pump(timeout_sec=.1)['received'], 1)
        self.assertEqual(self.receive_all(), [])
        clock.advance(4999)
        self.assertEqual(carrier.pump()['sent_copies'], 0)
        clock.advance(1)
        self.assertEqual(carrier.pump()['sent_copies'], 1)
        self.assertEqual(self.receive_all()[0][0], body)
        record = next(e for e in self.events if e['event'] == 'packet_copy_sent')
        self.assertEqual(record['queue_residence_ns'], 5000)

    def test_real_reorder_reverse_bytes_not_reconstructed_payload(self):
        carrier = self.carrier(QueuePolicy(mode='reorder', reorder_depth=3))
        bodies = [packet(i) for i in (1, 2, 3)]
        for body in bodies:
            self.sender.sendto(body, carrier.address)
        self.assertEqual(carrier.pump(timeout_sec=.1)['sent_copies'], 3)
        self.assertEqual([body for body, _ in self.receive_all()], list(reversed(bodies)))

    def test_real_duplicate_three_byte_identical_datagrams(self):
        carrier = self.carrier(QueuePolicy(mode='duplicate', copies=3))
        body = packet(3)
        self.sender.sendto(body, carrier.address)
        self.assertEqual(carrier.pump(timeout_sec=.1)['sent_copies'], 3)
        self.assertEqual([body_ for body_, _ in self.receive_all()], [body] * 3)
        self.assertEqual([e['copy_index'] for e in self.events if e['event'] == 'packet_copy_sent'], [0, 1, 2])

    def test_cancel_discards_pending_and_closes_all_sockets(self):
        carrier = self.carrier(QueuePolicy(mode='delay', delay_ns=1_000_000_000))
        self.sender.sendto(packet(), carrier.address)
        carrier.pump(timeout_sec=.1)
        self.assertEqual(carrier.queue.pending_copies, 1)
        carrier.cancel()
        self.assertTrue(carrier.pump()['closed'])
        self.assertEqual(carrier.queue.pending_copies, 0)
        self.assertEqual(self.receive_all(), [])
        for sock in (carrier.socket, carrier._wake_read, carrier._wake_write):
            self.assertEqual(sock.fileno(), -1)
        record = next(e for e in self.events if e['event'] == 'packet_copy_discarded')
        self.assertEqual(record['reason'], 'cancelled')

    def test_cancel_wakes_blocked_pump(self):
        carrier = self.carrier(QueuePolicy())
        result = []
        thread = threading.Thread(target=lambda: result.append(carrier.pump(timeout_sec=1)))
        thread.start()
        time.sleep(.02)
        carrier.cancel()
        thread.join(timeout=.5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result[0]['closed'])
        self.assertTrue(carrier.closed)

    def test_real_socket_send_failure_closes_and_records_tool_failure(self):
        carrier = self.carrier(QueuePolicy())
        self.sender.sendto(packet(), carrier.address)
        # Same socket receives first; close from callback simulates an egress fault.
        original_emit = carrier.queue.emit
        def fault(record):
            original_emit(record)
            if record['event'] == 'packet_send_started':
                carrier.socket.close()
        carrier.queue.emit = fault
        with self.assertRaises(OSError):
            carrier.pump(timeout_sec=.1)
        self.assertTrue(carrier.closed)
        self.assertEqual(carrier._wake_read.fileno(), -1)
        discarded = [e for e in self.events if e['event'] == 'packet_copy_discarded']
        self.assertIn('send_failed', discarded[0]['reason'])
        self.assertFalse(discarded[0]['pair_hit'])

    def test_explicit_route_validation_and_bounded_wait(self):
        with self.assertRaises(ValueError):
            UDPDiscoveryCarrier(('0.0.0.0', 0), self.sink.getsockname(), QueuePolicy(), emit=self.events.append)
        carrier = self.carrier(QueuePolicy())
        for value in (True, -1, 2, float('inf'), float('nan')):
            with self.assertRaises(ValueError):
                carrier.pump(timeout_sec=value)
        carrier.close()
        with self.assertRaisesRegex(PacketQueueError, 'closed'):
            carrier.pump()


if __name__ == '__main__':
    unittest.main()
