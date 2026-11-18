"""Bounded, byte-preserving UDP discovery queue; no default-runtime integration.

Embedding example (caller must establish namespace, leases, endpoint routing and
pair provenance before using the real socket component)::

    receipts = []  # production: stream each receipt to existing raw writer
    with UDPDiscoveryCarrier(('127.0.0.1', 19001), ('127.0.0.1', 19002),
            QueuePolicy(mode='delay', delay_ns=50_000_000),
            emit=receipts.append) as carrier:
        # The endpoint/proxy route must actually send to carrier.address.
        while window_open():
            carrier.pump(timeout_sec=0.05)
        carrier.close(reason='window_closed')  # pending copies audited, not sent

Use one component per explicitly routed direction. This is not interception,
transparent IP/source-port spoofing, multicast membership, or a DDS locator
rewriter. The outgoing UDP source is the relay's socket, recorded in receipts;
"transparent" means identical payloads, FIFO and one copy, NOT unchanged tuple
semantics or proved DDS interoperability. No ROS contexts or threads are made.

Parser basis: OMG DDSI-RTPS 2.5, sections 8.5 and 9.4.5. A deliberately bounded
subset is recognized; encrypted/vendor/unsupported/malformed submessages are
unknown and never mutation-eligible. Classification is not sender authentication.
All effects are harness observations, never an oracle/candidate or pair-hit.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
import hashlib
import heapq
import ipaddress
import math
import select
import socket
import struct
import threading
import time
from typing import Callable

Address = tuple[str, int]
SCHEMA = 'r2f.discovery-packet-queue/v1'
# Exact standard writer-reader entity pairs, not suffix heuristics/port guesses.
_BUILTINS = {
    '000100c2': ('000100c7', 'SPDP'),
    '000002c2': ('000002c7', 'SEDP_topics'),
    '000003c2': ('000003c7', 'SEDP_publications'),
    '000004c2': ('000004c7', 'SEDP_subscriptions'),
}
_NAMES = {1: 'PAD', 6: 'ACKNACK', 7: 'HEARTBEAT', 8: 'GAP', 9: 'INFO_TS',
          12: 'INFO_SRC', 14: 'INFO_DST', 18: 'NACK_FRAG', 19: 'HEARTBEAT_FRAG',
          21: 'DATA', 22: 'DATA_FRAG'}


class PacketQueueError(RuntimeError):
    pass


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f'{name} must be integer {minimum}..{maximum}')
    return value


def _address(value, *, allow_zero_port=False):
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError('address must be (numeric IP, port)')
    ip = ipaddress.ip_address(value[0])
    if ip.is_unspecified or ip.is_multicast:
        raise ValueError('specific unicast addresses required; no wildcard/multicast routing')
    port = _integer(value[1], 'port', 0 if allow_zero_port else 1, 65535)
    return str(ip), port


def _prefix(value):
    if not isinstance(value, str) or len(value) != 24:
        raise ValueError('GUID prefix must be 12 bytes of nonzero hex')
    try:
        raw = bytes.fromhex(value)
    except ValueError:
        raise ValueError('GUID prefix must be hex') from None
    if len(raw) != 12 or not any(raw):
        raise ValueError('GUID prefix must be nonzero')
    return raw.hex()


def classify_rtps(payload: bytes, *, max_submessages=512) -> dict:
    """Parse framing/entity routing only, not serialized DDS data or QoS.

    Return per-submessage effective INFO_SRC/INFO_DST, reader/writer IDs and
    source entity. Mixed means whole-datagram transformations can also affect
    user/unsupported content; the queue never splits/reconstructs submessages.
    """
    if not isinstance(payload, bytes):
        raise TypeError('payload must be immutable bytes')
    _integer(max_submessages, 'max_submessages', 1, 4096)
    result = {'classification': 'not_rtps', 'submessages': [], 'diagnostics': [],
              'mixed': False, 'has_discovery': False, 'identity_is_authenticated': False}
    if payload[:4] != b'RTPS':
        return result
    errors, messages = result['diagnostics'], result['submessages']
    if len(payload) < 20:
        result.update(classification='unknown', diagnostics=['truncated_rtps_header'])
        return result
    version = tuple(payload[4:6])
    source, destination = payload[8:20].hex(), None
    result.update(protocol_version=list(version), vendor_id=payload[6:8].hex(),
                  header_source_prefix=source)
    if version[0] != 2 or not 1 <= version[1] <= 5:
        errors.append('unsupported_protocol_version')
    cursor = 20
    while cursor < len(payload):
        if len(messages) >= max_submessages:
            errors.append('submessage_limit')
            break
        if cursor + 4 > len(payload):
            errors.append(f'truncated_submessage_header@{cursor}')
            break
        kind, flags = payload[cursor:cursor + 2]
        endian = '<' if flags & 1 else '>'
        length = struct.unpack_from(endian + 'H', payload, cursor + 2)[0]
        # DDSI 9.4.5.1.3: PAD/INFO_TS zero length does NOT consume the rest.
        end = cursor + 4 + length if length or kind in (1, 9) else len(payload)
        item = {'kind': kind, 'name': _NAMES.get(kind, f'UNKNOWN_{kind:02x}'),
                'flags': flags, 'offset': cursor, 'end': end, 'classification': 'context',
                'source_guid_prefix': source, 'destination_guid_prefix': destination}
        messages.append(item)
        if end > len(payload) or end < cursor + 4:
            errors.append(f'truncated_submessage@{cursor}')
            item['classification'] = 'unknown'
            break
        body = payload[cursor + 4:end]
        issues = []
        if end < len(payload) and end % 4:
            issues.append('unaligned_next_submessage')
        reader = writer = None
        if kind == 1:
            pass
        elif kind == 9:
            if len(body) != (0 if flags & 2 else 8):
                issues.append('invalid_INFO_TS_length')
        elif kind == 12:
            if len(body) != 20:
                issues.append('invalid_INFO_SRC_length')
            else:
                version = tuple(body[4:6])
                source = body[8:20].hex()
                if version[0] != 2 or not 1 <= version[1] <= 5:
                    issues.append('unsupported_INFO_SRC_version')
                item['source_guid_prefix'] = source
        elif kind == 14:
            if len(body) != 12:
                issues.append('invalid_INFO_DST_length')
            else:
                destination = body.hex() if any(body) else None
                item['destination_guid_prefix'] = destination
        elif kind in (21, 22):
            minimum = 20 if kind == 21 else 32
            if len(body) < minimum:
                issues.append('truncated_DATA_fields')
            else:
                reader, writer = body[4:8].hex(), body[8:12].hex()
                item['writer_sequence_number'] = (struct.unpack_from(endian + 'i', body, 12)[0] << 32) + struct.unpack_from(endian + 'I', body, 16)[0]
                qos = 4 + struct.unpack_from(endian + 'H', body, 2)[0]
                if qos < minimum or qos > len(body) or qos % 4:
                    issues.append('invalid_octetsToInlineQos')
                else:
                    data_start = qos
                    if flags & 2:
                        while data_start + 4 <= len(body):
                            pid, size = struct.unpack_from(endian + 'HH', body, data_start)
                            data_start += 4
                            if pid == 1:
                                if size:
                                    issues.append('inline_qos_sentinel_size')
                                break
                            if size % 4 or data_start + size > len(body):
                                issues.append('truncated_or_unaligned_inline_qos')
                                break
                            data_start += size
                        else:
                            issues.append('inline_qos_sentinel_missing')
                    if kind == 21:
                        if flags & 4 and flags & 8:
                            issues.append('DATA_both_data_and_key')
                        if flags & 12 and len(body) - data_start < 4:
                            issues.append('serialized_payload_header_missing')
                    else:
                        start, count, size, total = struct.unpack_from(endian + 'IHHI', body, 20)
                        if not start or not count or not size or not total or (start - 1) * size >= total:
                            issues.append('invalid_fragment_fields')
                        else:
                            expected = min(count * size, total - (start - 1) * size)
                            if count > (total + size - 1) // size - (start - 1) or not expected <= len(body) - data_start <= expected + 3:
                                issues.append('fragment_payload_size_mismatch')
        elif kind in (6, 7, 8, 18, 19):
            minimum = {6: 24, 7: 28, 8: 28, 18: 28, 19: 24}[kind]
            if len(body) < minimum:
                issues.append('truncated_control_fields')
            else:
                reader, writer = body[:4].hex(), body[4:8].hex()
                if kind in (6, 8, 18):
                    bit_offset = {6: 16, 8: 24, 18: 20}[kind]
                    bits = struct.unpack_from(endian + 'I', body, bit_offset)[0]
                    if bits > 256 or len(body) != minimum + 4 * ((bits + 31) // 32):
                        issues.append('invalid_sequence_or_fragment_bitmap')
                elif len(body) != minimum:
                    issues.append('unsupported_control_extension')
        else:
            item['classification'] = 'unknown'
            issues.append('unsupported_submessage')
        if reader is not None:
            item.update(reader_id=reader, writer_id=writer)
            pair = _BUILTINS.get(writer)
            if pair and (reader == pair[0] or reader == '00000000' and kind not in (6, 18)):
                item.update(classification='discovery', discovery_protocol=pair[1])
            elif pair or reader in {v[0] for v in _BUILTINS.values()}:
                item['classification'] = 'unknown'
                issues.append('builtin_counterpart_mismatch')
            elif writer[-2:] in ('02', '03') and (reader == '00000000' or reader[-2:] in ('04', '07')):
                item['classification'] = 'user'
            else:
                item['classification'] = 'unknown'
                issues.append('unsupported_entity_pair')
            sender = reader if kind in (6, 18) else writer
            item['source_entity_id'] = sender
            item['source_endpoint_guid'] = source + sender
            if source == '0' * 24:
                issues.append('source_prefix_unknown')
        if issues:
            item['classification'] = 'unknown'
            errors.extend(f'{issue}@{cursor}' for issue in issues)
        cursor = end
    classes = {item['classification'] for item in messages} - {'context'}
    result['has_discovery'] = 'discovery' in classes
    result['mixed'] = result['has_discovery'] and bool(classes - {'discovery'})
    result['classification'] = ('unknown' if errors else 'mixed' if result['mixed'] else
                                'discovery' if result['has_discovery'] else
                                'user' if classes == {'user'} else 'context_only')
    return result


@dataclass(frozen=True)
class IdentityBinding:
    """External observation references, NOT attestation or authority to claim hit."""
    source_prefix: str
    destination_prefix: str
    source_address: Address
    destination_address: Address
    raw_event_ref: str

    def __post_init__(self):
        object.__setattr__(self, 'source_prefix', _prefix(self.source_prefix))
        object.__setattr__(self, 'destination_prefix', _prefix(self.destination_prefix))
        object.__setattr__(self, 'source_address', _address(self.source_address))
        object.__setattr__(self, 'destination_address', _address(self.destination_address))
        if not isinstance(self.raw_event_ref, str) or not self.raw_event_ref.strip():
            raise ValueError('binding requires external raw_event_ref')


def _correlation(parsed, source, destination, binding):
    receipt = {'confidence': 'unbound', 'pair_hit': False,
               'limitation': 'wire_identity_not_authenticated; external_pair_provenance_not_validated'}
    if binding is None:
        return receipt
    receipt['binding_raw_event_ref'] = binding.raw_event_ref
    messages = [m for m in parsed['submessages'] if m['classification'] == 'discovery']
    tuple_match = source == binding.source_address and destination == binding.destination_address
    source_match = bool(messages) and all(m['source_guid_prefix'] == binding.source_prefix for m in messages)
    destination_match = bool(messages) and all(m['destination_guid_prefix'] == binding.destination_prefix for m in messages)
    receipt.update(transport_tuple_matches=tuple_match, source_prefix_matches=source_match,
                   destination_prefix_matches=destination_match)
    receipt['confidence'] = ('wire_and_configured_tuple_correlated' if tuple_match and source_match and destination_match
                             and not parsed['diagnostics'] and not parsed['mixed'] else
                             'source_only_receiver_unproven' if tuple_match and source_match else 'mismatch_or_unknown')
    return receipt


@dataclass(frozen=True)
class QueuePolicy:
    mode: str = 'transparent'
    delay_ns: int = 0
    reorder_depth: int = 2
    reorder_window_ns: int = 50_000_000
    copies: int = 2
    duplicate_gap_ns: int = 0
    ttl_ns: int = 2_000_000_000
    max_pending_copies: int = 1024
    max_pending_bytes: int = 4 * 1024 * 1024
    max_datagram_bytes: int = 65507
    allow_mixed: bool = False

    def __post_init__(self):
        if self.mode not in ('transparent', 'delay', 'reorder', 'duplicate'):
            raise ValueError('unsupported queue mode')
        for key in ('delay_ns', 'duplicate_gap_ns'):
            _integer(getattr(self, key), key, 0, 60_000_000_000)
        for key in ('reorder_window_ns', 'ttl_ns'):
            _integer(getattr(self, key), key, 1, 60_000_000_000)
        _integer(self.reorder_depth, 'reorder_depth', 2, 256)
        _integer(self.copies, 'copies', 1, 16)
        _integer(self.max_pending_copies, 'max_pending_copies', 1, 65536)
        _integer(self.max_pending_bytes, 'max_pending_bytes', 1, 64 * 1024 * 1024)
        _integer(self.max_datagram_bytes, 'max_datagram_bytes', 1, 65507)
        if type(self.allow_mixed) is not bool:
            raise ValueError('allow_mixed must be bool')
        if self.mode == 'delay' and self.delay_ns >= self.ttl_ns:
            raise ValueError('delay must fit strictly inside TTL')
        if self.mode == 'reorder' and self.reorder_window_ns >= self.ttl_ns:
            raise ValueError('reorder window must fit strictly inside TTL')
        if self.mode == 'duplicate' and (self.copies - 1) * self.duplicate_gap_ns >= self.ttl_ns:
            raise ValueError('duplicate schedule must fit strictly inside TTL')


@dataclass
class _Packet:
    payload: bytes
    receipt: dict
    received_ns: int
    expires_ns: int


@dataclass(order=True)
class _Copy:
    due_ns: int
    order: int
    packet: _Packet = field(compare=False)
    copy_index: int = field(compare=False)


class PacketQueue:
    """Single-owner deterministic scheduler. No sockets, sleeping or hidden worker.

    emit must durably serialize to the existing raw system in production. A sink
    failure closes/discards the queue and raises; no success/hit is synthesized.
    send(payload, destination) must return datagram bytes sent, like sendto().
    """
    def __init__(self, policy: QueuePolicy, *, emit: Callable[[dict], None],
                 clock_ns=time.monotonic_ns, binding: IdentityBinding | None = None):
        if not isinstance(policy, QueuePolicy) or not callable(emit) or not callable(clock_ns):
            raise TypeError('policy, receipt sink and monotonic clock required')
        if binding is not None and not isinstance(binding, IdentityBinding):
            raise TypeError('binding must be IdentityBinding or None')
        self.policy, self.emit, self.clock_ns, self.binding = policy, emit, clock_ns, binding
        self._heap, self._batch = [], []
        self._batch_deadline = None
        self._last_ns = None
        self._next_id = self._order = 0
        self.pending_copies = self.pending_bytes = 0
        self.closed = False
        self.audit_error = None
        self._io_origin = 'caller_supplied_datagram_not_kernel_attested'
        self.counts = {'received': 0, 'admitted': 0, 'sent_copies': 0, 'discarded_copies': 0,
                       'overflow_packets': 0, 'scheduled_reordered_batches': 0}

    def _now(self):
        now = self.clock_ns()
        if type(now) is not int or now < 0 or self._last_ns is not None and now < self._last_ns:
            raise PacketQueueError('monotonic_clock_invalid_or_regressed')
        self._last_ns = now
        return now

    def _emit(self, event, packet=None, **fields):
        record = {'schema': SCHEMA, 'event': event, 'pair_hit': False,
                  'responsibility_layer': 'environment_harness', **(packet.receipt if packet else {}), **fields}
        try:
            self.emit(copy.deepcopy(record))
        except BaseException as exc:
            self.audit_error = type(exc).__name__ + ':' + str(exc)
            self.closed = True
            self._heap.clear()
            self._batch.clear()
            self._batch_deadline = None
            self.pending_bytes = self.pending_copies = 0
            raise PacketQueueError('receipt_sink_failed:' + self.audit_error) from exc

    def _check_open(self):
        if self.closed:
            raise PacketQueueError('packet_queue_closed')

    def _push(self, packet, due, index):
        self._order += 1
        heapq.heappush(self._heap, _Copy(due, self._order, packet, index))

    def _flush_batch(self, now, reason):
        if not self._batch:
            return
        batch = self._batch
        self._batch, self._batch_deadline = [], None
        reverse_ids = [p.receipt['packet_id'] for p in reversed(batch)]
        self._emit('reorder_batch_released_to_scheduler', packet_ids=reverse_ids,
                   reason=reason, scheduled_ns=now, batch_size=len(batch),
                   order_effect_expected=len(batch) > 1)
        for packet in reversed(batch):
            self._push(packet, now, 0)
        if len(batch) > 1:
            self.counts['scheduled_reordered_batches'] += 1  # scheduling only, not sent/hit

    def accept(self, payload: bytes, source: Address, destination: Address, *, ingress: Address | None = None) -> int:
        self._check_open()
        if not isinstance(payload, bytes):
            raise TypeError('payload must be immutable bytes')
        source, destination = _address(source), _address(destination)
        ingress = _address(ingress) if ingress is not None else None
        try:
            now = self._now()
        except BaseException:
            self.close(reason='accept_clock_failed')
            raise
        self._expire(now)
        self._next_id += 1
        self.counts['received'] += 1
        parsed = classify_rtps(payload)
        eligible = parsed['classification'] == 'discovery' or (
            self.policy.allow_mixed and parsed['classification'] == 'mixed')
        operation = self.policy.mode if eligible else 'transparent'
        receipt = {'packet_id': self._next_id, 'original_bytes_sha256': hashlib.sha256(payload).hexdigest(),
            'original_size_bytes': len(payload), 'transport_origin': self._io_origin,
            'source_address': list(source),
            'destination_address': list(destination),
            'destination_basis': 'explicit_forward_route_not_wire_original_destination',
            'ingress_address': list(ingress) if ingress else None,
            'actual_forward_source': list(ingress) if ingress else None,
            'original_source_tuple_preserved': False if ingress else None,
            'received_monotonic_ns': now, 'expires_monotonic_ns': now + self.policy.ttl_ns,
            'operation': operation, 'requested_operation': self.policy.mode,
            'classification': parsed, 'correlation': _correlation(parsed, source, destination, self.binding),
            'whole_datagram_collateral': eligible and parsed['mixed'],
            'mutation_eligible': eligible, 'payload_modified': False}
        packet = _Packet(payload, receipt, now, now + self.policy.ttl_ns)
        self._emit('packet_received', packet)
        copies = self.policy.copies if operation == 'duplicate' else 1
        if (len(payload) > self.policy.max_datagram_bytes or
                self.pending_copies + copies > self.policy.max_pending_copies or
                self.pending_bytes + copies * len(payload) > self.policy.max_pending_bytes):
            self.counts['overflow_packets'] += 1
            self._emit('packet_rejected', packet, reason='datagram_or_queue_bound',
                       harness_failure=True, scheduled_copies=0)
            return packet.receipt['packet_id']
        self.pending_copies += copies
        self.pending_bytes += copies * len(payload)
        self.counts['admitted'] += 1
        self._emit('packet_scheduled', packet, scheduled_copies=copies,
                   not_before_ns=now + self.policy.delay_ns if operation == 'delay' else now)
        if operation == 'reorder':
            if self._batch and now >= self._batch_deadline:
                self._flush_batch(now, 'window_expired_before_next_packet')
            self._batch.append(packet)
            if len(self._batch) == 1:
                self._batch_deadline = now + self.policy.reorder_window_ns
            if len(self._batch) >= self.policy.reorder_depth:
                self._flush_batch(now, 'depth_reached')
        else:
            for index in range(copies):
                due = now + (self.policy.delay_ns if operation == 'delay' else
                             index * self.policy.duplicate_gap_ns if operation == 'duplicate' else 0)
                self._push(packet, due, index)
        return packet.receipt['packet_id']

    def _discard(self, packet, index, reason, now):
        self.pending_copies -= 1
        self.pending_bytes -= len(packet.payload)
        self.counts['discarded_copies'] += 1
        self._emit('packet_copy_discarded', packet, copy_index=index, reason=reason,
                   discarded_monotonic_ns=now, harness_failure=True)

    def _expire(self, now):
        # Queue bounds cap this O(n) scan, including copies whose due time is later.
        kept = []
        for item in self._heap:
            if now >= item.packet.expires_ns:
                self._discard(item.packet, item.copy_index, 'TTL_expired', now)
            else:
                kept.append(item)
        self._heap = kept
        heapq.heapify(self._heap)
        batch = []
        for packet in self._batch:
            if now >= packet.expires_ns:
                self._discard(packet, 0, 'TTL_expired', now)
            else:
                batch.append(packet)
        self._batch = batch
        if not batch:
            self._batch_deadline = None

    def drain(self, send: Callable[[bytes, Address], int], *, max_copies=256) -> int:
        self._check_open()
        _integer(max_copies, 'max_copies', 1, 65536)
        sent = 0
        try:
            now = self._now()
            self._expire(now)
            if self._batch and now >= self._batch_deadline:
                self._flush_batch(now, 'window_deadline')
            while self._heap and self._heap[0].due_ns <= now and sent < max_copies:
                item = heapq.heappop(self._heap)
                packet = item.packet
                # TTL is a pre-send admission cutoff, not a network delivery guarantee.
                started = self._now()
                if started >= packet.expires_ns:
                    self._discard(packet, item.copy_index, 'TTL_expired', started)
                    now = started
                    continue
                self._emit('packet_send_started', packet, copy_index=item.copy_index,
                           due_monotonic_ns=item.due_ns, release_started_monotonic_ns=started)
                try:
                    size = send(packet.payload, tuple(packet.receipt['destination_address']))
                    if type(size) is not int or size != len(packet.payload):
                        raise PacketQueueError('partial_or_invalid_datagram_send')
                except BaseException as exc:
                    self._discard(packet, item.copy_index, 'send_failed:' + type(exc).__name__ + ':' + str(exc), started)
                    raise
                self.pending_copies -= 1
                self.pending_bytes -= len(packet.payload)
                sent += 1
                self.counts['sent_copies'] += 1
                try:
                    completed = self._now()
                except BaseException:
                    self._emit('packet_send_completed_time_unknown', packet, copy_index=item.copy_index,
                               actual_sent_bytes=size, sent_bytes_sha256=hashlib.sha256(packet.payload).hexdigest(),
                               released_monotonic_ns=None, harness_failure=True)
                    raise
                self._emit('packet_copy_sent', packet, copy_index=item.copy_index,
                    due_monotonic_ns=item.due_ns, release_started_monotonic_ns=started,
                    released_monotonic_ns=completed, queue_residence_ns=started - packet.received_ns,
                    actual_sent_bytes=size, sent_bytes_sha256=hashlib.sha256(packet.payload).hexdigest(),
                    effect_basis='successful_send_call_not_remote_delivery_or_DDS_effect')
                now = completed
        except BaseException:
            self.close(reason='drain_failed')
            raise
        return sent

    def next_deadline_ns(self):
        if self.closed:
            return None
        deadlines = ([self._heap[0].due_ns] if self._heap else [])
        deadlines += ([self._batch_deadline] if self._batch else [])
        deadlines += [p.expires_ns for p in self._batch]
        deadlines += [item.packet.expires_ns for item in self._heap]
        return min(deadlines) if deadlines else None

    def close(self, *, reason='cancelled'):
        if self.closed:
            return
        self.closed = True
        try:
            now = self._now()
        except BaseException:
            now = self._last_ns
        pending, self._heap = self._heap, []
        batch, self._batch = self._batch, []
        self._batch_deadline = None
        try:
            for item in pending:
                self._discard(item.packet, item.copy_index, reason, now)
            for packet in batch:
                self._discard(packet, 0, reason, now)
            self._emit('queue_closed', reason=reason, closed_monotonic_ns=now, counts=self.counts)
        finally:
            self.pending_bytes = self.pending_copies = 0


class UDPDiscoveryCarrier:
    """Explicit-route UDP relay; pump on caller's existing event loop.

    cancel() may be called from another thread; it wakes pump via socketpair.
    Otherwise pump/close are single-owner. Received/released times come from the
    injected monotonic clock, sampled after recvmsg and around sendto. They are
    userspace times, not kernel timestamps. Receipts record the actual relay
    source; no source/destination preservation is falsely claimed.
    """
    def __init__(self, bind: Address, destination: Address, policy: QueuePolicy, *,
                 emit: Callable[[dict], None], clock_ns=time.monotonic_ns,
                 binding: IdentityBinding | None = None):
        bind, self.destination = _address(bind, allow_zero_port=True), _address(destination)
        family = socket.AF_INET if ipaddress.ip_address(bind[0]).version == 4 else socket.AF_INET6
        if ipaddress.ip_address(bind[0]).version != ipaddress.ip_address(self.destination[0]).version:
            raise ValueError('bind/forward address families must match')
        self.closed = False
        self._cancel = threading.Event()
        self.socket = self._wake_read = self._wake_write = None
        self.queue = PacketQueue(policy, emit=emit, clock_ns=clock_ns, binding=binding)
        try:
            self.socket = socket.socket(family, socket.SOCK_DGRAM)
            self.socket.bind(bind)
            self.socket.setblocking(False)
            self.address = self.socket.getsockname()[:2]
            self.queue._io_origin = 'UDPDiscoveryCarrier.recvmsg_sendto'
            if self.address == self.destination:
                raise ValueError('self-forward route would loop')
            self._wake_read, self._wake_write = socket.socketpair()
            self._wake_read.setblocking(False)
            self._wake_write.setblocking(False)
            self.queue._emit('udp_carrier_opened', ingress_address=list(self.address),
                actual_forward_source=list(self.address), destination_address=list(self.destination),
                time_basis='userspace_monotonic', no_default_runtime_integration=True,
                source_tuple_preserved=False)
        except BaseException:
            self.close(reason='open_failed')
            raise

    def _send(self, payload, destination):
        if self._cancel.is_set():
            raise PacketQueueError('cancelled_before_send')
        return self.socket.sendto(payload, destination)

    def pump(self, *, timeout_sec=0.0, max_packets=64):
        if self.closed:
            raise PacketQueueError('udp_carrier_closed')
        if isinstance(timeout_sec, bool) or not isinstance(timeout_sec, (int, float)) or not math.isfinite(timeout_sec) or not 0 <= timeout_sec <= 1:
            raise ValueError('timeout_sec must be finite 0..1 (bounded cancellation)')
        _integer(max_packets, 'max_packets', 1, 4096)
        received = sent = 0
        try:
            if self._cancel.is_set():
                self.close(reason='cancelled')
                return {'received': 0, 'sent_copies': 0, 'closed': True}
            sent += self.queue.drain(self._send)
            deadline = self.queue.next_deadline_ns()
            wait = timeout_sec if deadline is None else min(timeout_sec, max(0, (deadline - self.queue._now()) / 1e9))
            ready, _, _ = select.select([self.socket, self._wake_read], [], [], wait)
            if self._cancel.is_set() or self._wake_read in ready:
                self.close(reason='cancelled')
                return {'received': 0, 'sent_copies': sent, 'closed': True}
            if self.socket in ready:
                for _ in range(max_packets):
                    try:
                        payload, _, flags, source = self.socket.recvmsg(65535)
                    except BlockingIOError:
                        break
                    if flags & socket.MSG_TRUNC:
                        raise PacketQueueError('truncated_recvmsg_no_original_bytes_proof')
                    received += 1
                    self.queue.accept(payload, source[:2], self.destination, ingress=self.address)
            sent += self.queue.drain(self._send)
            return {'received': received, 'sent_copies': sent, 'closed': False}
        except BaseException:
            self.close(reason='pump_failed')
            raise

    def cancel(self):
        self._cancel.set()
        if self._wake_write is not None:
            try:
                self._wake_write.send(b'!')
            except (OSError, BlockingIOError):
                pass

    def close(self, *, reason='cancelled'):
        if self.closed:
            return
        self.closed = True
        try:
            self.queue.close(reason=reason)
        finally:
            for sock in (self.socket, self._wake_read, self._wake_write):
                if sock is not None:
                    sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close(reason='context_closed')
