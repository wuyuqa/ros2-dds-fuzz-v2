"""Receive-only AF_PACKET observer inside an existing validated user/net namespace.

The caller owns namespace creation, locks, ROS workers and endpoint attribution.
Capture cannot change network configuration, send traffic, or create a participant.
"""
from __future__ import annotations
from collections import Counter, deque
import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import struct
import threading
import time

from .rtps import BUILTIN_WRITERS, discovery_activity, rtps_submessages, udp_frame

SCHEMA = 'r2f.passive-sedp-capture/v1'
DEFAULT_LIMITS = {'timeout_sec': 120, 'max_bytes': 64 * 1024 * 1024, 'max_packets': 100000,
                  'snaplen': 65535, 'recv_buffer_bytes': 4 * 1024 * 1024,
                  'activity_limit': 4096}
SUBMESSAGE_NAMES = {
    0x01: 'PAD', 0x06: 'ACKNACK', 0x07: 'HEARTBEAT', 0x08: 'GAP',
    0x09: 'INFO_TS', 0x0c: 'INFO_SRC', 0x0d: 'INFO_REPLY_IP4',
    0x0e: 'INFO_DST', 0x0f: 'INFO_REPLY', 0x12: 'NACK_FRAG',
    0x13: 'HEARTBEAT_FRAG', 0x14: 'DATA', 0x15: 'DATA',
    0x16: 'DATA_FRAG', 0x17: 'DATA_FRAG',
}


def _filtered_submessage_entities(payload, submessages):
    """Prove ordinary user traffic only from decoded routing and body framing.

    Unknown kinds/entities and malformed bodies stay potentially discovery-related.
    Auxiliary identity messages carry context, never discovery activity themselves.
    """
    entities, diagnostics = [], []
    for sm in submessages:
        kind = sm['kind']
        body = payload[sm['offset'] + 4:sm['end']]
        entry = {key: sm[key] for key in ('offset', 'source_guid_prefix', 'destination_guid_prefix')}
        entry.update(kind=SUBMESSAGE_NAMES.get(kind, f'UNKNOWN_0x{kind:02x}'),
                     reader_entity_id=None, writer_entity_id=None,
                     classification='unknown', reason='unsupported_submessage')
        malformed = None
        if kind in (0x01, 0x09, 0x0c, 0x0e):
            expected = {0x09: 0 if sm['flags'] & 2 else 8, 0x0c: 20, 0x0e: 12}
            if kind != 0x01 and len(body) != expected[kind]:
                malformed = 'invalid_auxiliary_length'
            else:
                entry.update(classification='auxiliary', reason='context_only')
        elif kind in (0x15, 0x16, 0x06, 0x07, 0x08, 0x12, 0x13):
            data = kind in (0x15, 0x16)
            entity_offset = 4 if data else 0
            if len(body) >= entity_offset + 8:
                reader = body[entity_offset:entity_offset + 4].hex()
                writer = body[entity_offset + 4:entity_offset + 8].hex()
                entry.update(reader_entity_id=reader, writer_entity_id=writer)
            minimum = {0x15: 20, 0x16: 32, 0x06: 24, 0x07: 28,
                       0x08: 28, 0x12: 28, 0x13: 24}[kind]
            if len(body) < minimum:
                malformed = 'truncated_entity_submessage'
            elif data:
                inline_offset = 4 + struct.unpack_from(sm['endian'] + 'H', body, 2)[0]
                payload_offset = inline_offset
                if not minimum <= inline_offset <= len(body) or inline_offset % 4:
                    malformed = 'invalid_inline_qos_offset'
                elif sm['flags'] & 2:
                    # Inline QoS is a terminated, aligned ParameterList. A missing
                    # sentinel cannot become proof of an unrelated user packet.
                    cursor = inline_offset
                    while True:
                        if cursor + 4 > len(body):
                            malformed = 'truncated_inline_qos'
                            break
                        pid, size = struct.unpack_from(sm['endian'] + 'HH', body, cursor)
                        cursor += 4
                        if pid == 1:
                            payload_offset = cursor
                            break
                        cursor += (size + 3) & ~3
                if kind == 0x16 and malformed is None:
                    start, count, fragment_size, sample_size = struct.unpack_from(sm['endian'] + 'IHHI', body, 20)
                    if not start or not count or not fragment_size or not sample_size:
                        malformed = 'invalid_fragment_header'
                    elif start + count - 1 > (sample_size + fragment_size - 1) // fragment_size:
                        malformed = 'invalid_fragment_range'
                    else:
                        required = min((start + count - 1) * fragment_size, sample_size) - (start - 1) * fragment_size
                        if not required <= len(body) - payload_offset <= required + 3:
                            malformed = 'invalid_fragment_payload_length'
                elif kind == 0x15 and malformed is None:
                    if sm['flags'] & 12 == 12 or (sm['flags'] & 12 and len(body) - payload_offset < 4):
                        malformed = 'invalid_DATA_payload'
            else:
                # ACKNACK/GAP/NACK_FRAG contain variable bitmaps. Checking just
                # the fixed entity bytes would silently accept truncated control.
                bitmap_offset = {0x06: 16, 0x08: 24, 0x12: 20}.get(kind)
                expected_length = minimum
                if bitmap_offset is not None:
                    bits = struct.unpack_from(sm['endian'] + 'I', body, bitmap_offset)[0]
                    expected_length += 4 * ((bits + 31) // 32)
                    if bits > 256:
                        malformed = 'invalid_control_bitmap'
                if len(body) != expected_length:
                    malformed = 'invalid_control_length'
            if malformed is None:
                # Reader routing matters too, including conflicting identities.
                # Other builtin/extension identities stay unknown below.
                if writer in BUILTIN_WRITERS or reader in ('000100c7', '000003c7', '000004c7'):
                    entry.update(classification='discovery', reason='builtin_entity')
                elif writer[-2:] in ('02', '03') and (reader == '00000000' or reader[-2:] in ('04', '07')):
                    entry.update(classification='user', reason='ordinary_user_entities')
                else:
                    entry['reason'] = 'unknown_entity'
        if entry['classification'] in ('user', 'discovery') and entry['source_guid_prefix'] == '0' * 24:
            entry.update(classification='unknown', reason='unassignable_source_prefix')
        if malformed:
            entry['reason'] = malformed
            diagnostics.append(f'{malformed}@{sm["offset"]}')
        entities.append(entry)
    return entities, diagnostics


def _rtps_metadata(udp, *, truncated):
    """Read-only classification shared by retention and filtered-frame auditing."""
    payload = udp['payload']
    submessages, diagnostics = rtps_submessages(payload)
    entities, entity_diagnostics = _filtered_submessage_entities(payload, submessages)
    diagnostics.extend(entity_diagnostics)
    for item in entities:
        if item['classification'] == 'unknown':
            diagnostic = f'{item["reason"]}@{item["offset"]}'
            if diagnostic not in diagnostics:
                diagnostics.append(diagnostic)
    classification = ('unknown' if truncated or diagnostics else
                      'discovery' if any(item['classification'] == 'discovery' for item in entities) else
                      'proven_no_discovery')
    return {'source_port': udp['source_port'], 'destination_port': udp['destination_port'],
            'source_participant_prefix': payload[8:20].hex() if len(payload) >= 20 else None,
            'source_participant_prefixes': sorted({item['source_guid_prefix'] for item in submessages
                                                   if item.get('source_guid_prefix')}),
            'destination_participant_prefixes': sorted({item['destination_guid_prefix'] for item in submessages
                                                        if item.get('destination_guid_prefix')}),
            'submessage_kinds': [SUBMESSAGE_NAMES.get(item['kind'], f'UNKNOWN_0x{item["kind"]:02x}')
                                 for item in submessages],
            'submessage_entities': entities, 'classification': classification,
            'parse_diagnostics': diagnostics}


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def _now():
    return {'unix_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns()}


def _process_identity():
    return {'pid': os.getpid(), 'pgid': os.getpgid(0),
            'start_ticks': int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19])}


def build_capture_spec(output_dir=None, *, domain_id, interface='lo', participant_count=32,
                       run_id=None, netns_identity=None, limits=None):
    """No-write builder; explicit proof or runtime R2F_DISCOVERY_NETNS_PROOF.

    Supplying the proof explicitly makes the builder independent of environment.
    Existing keyword-only callers remain supported.
    """
    if output_dir is None:
        raise ValueError('output_dir required')
    if interface != 'lo':
        raise ValueError('only isolated loopback capture supported')
    if isinstance(participant_count, bool) or not isinstance(participant_count, int) or not 1 <= participant_count <= 120:
        raise ValueError('participant_count must be an integer in 1..120')
    if run_id is None:
        run_id = Path(output_dir).name
    if netns_identity is None:
        try:
            netns_identity = json.loads(os.environ['R2F_DISCOVERY_NETNS_PROOF'])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError('explicit namespace proof or R2F_DISCOVERY_NETNS_PROOF required') from exc
    if not isinstance(run_id, str) or not run_id or '\0' in run_id:
        raise ValueError('run_id must be a nonempty string')
    if isinstance(domain_id, bool) or not isinstance(domain_id, int) or not 0 <= domain_id <= 231:
        raise ValueError('domain_id must be an integer in 0..231')
    if not isinstance(netns_identity, dict):
        raise ValueError('netns_identity must be an explicit runtime namespace proof')
    proof = copy.deepcopy(netns_identity)
    for key in ('net_namespace', 'user_namespace', 'host_net_namespace', 'host_user_namespace'):
        if not isinstance(proof.get(key), str) or not re.fullmatch(r'(?:net|user):\[\d+\]', proof[key]):
            raise ValueError('missing or invalid namespace identity: ' + key)
    if proof['net_namespace'] == proof['host_net_namespace'] or proof['user_namespace'] == proof['host_user_namespace']:
        raise ValueError('capture requires isolated user and network namespaces')
    if isinstance(proof.get('host_uid'), bool) or not isinstance(proof.get('host_uid'), int) or proof['host_uid'] <= 0:
        raise ValueError('positive host_uid required')
    if isinstance(proof.get('host_cap_eff'), bool) or not isinstance(proof.get('host_cap_eff'), int) or proof['host_cap_eff'] < 0 or proof['host_cap_eff'] & (1 << 12):
        raise ValueError('host must not have CAP_NET_ADMIN')
    output = Path(output_dir)
    if not output.is_absolute() or '\0' in str(output):
        raise ValueError('output_dir must be absolute')
    merged = {**DEFAULT_LIMITS, **(limits or {})}
    if set(merged) != set(DEFAULT_LIMITS):
        raise ValueError('unknown capture limit')
    for name, value in merged.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError('invalid limit: ' + name)
        if name != 'timeout_sec' and not isinstance(value, int):
            raise ValueError('integer limit required: ' + name)
    if not 256 <= merged['snaplen'] <= 262144 or merged['max_bytes'] < merged['snaplen'] + 40:
        raise ValueError('snaplen/max_bytes bounds')
    if merged['timeout_sec'] > 3600 or merged['activity_limit'] > 1000000:
        raise ValueError('unbounded capture limit')
    spec = {'schema': SCHEMA, 'run_id': run_id, 'domain_id': domain_id,
            'netns_identity': proof, 'output_dir': str(output.resolve()), 'interface': 'lo',
            'limits': merged, 'participant_count': participant_count,
            'discovery_ports': [7400 + 250 * domain_id] + [7410 + 250 * domain_id + 2 * p for p in range(participant_count)],
            'port_range': [7400 + 250 * domain_id, 7400 + 250 * domain_id + 249],
            'capture_scope': 'normal_RTPS_in_owned_netns', 'participant_created': False,
            'network_mutation': False, 'packet_transmission': False}
    spec['spec_sha256'] = hashlib.sha256(_canonical(spec)).hexdigest()
    return spec


def validate_capture_namespace(proof):
    """Read-only validation immediately before opening AF_PACKET. No ip/ioctl writes."""
    net = os.readlink('/proc/self/ns/net')
    user = os.readlink('/proc/self/ns/user')
    uid_map = [list(map(int, line.split())) for line in Path('/proc/self/uid_map').read_text().splitlines()]
    cap = int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines()
                   if line.startswith('CapEff:')), 16)
    if net != proof['net_namespace'] or user != proof['user_namespace']:
        raise RuntimeError('capture_namespace_identity_mismatch')
    if net == proof['host_net_namespace'] or user == proof['host_user_namespace']:
        raise RuntimeError('capture_on_host_namespace_forbidden')
    if os.geteuid() != 0 or uid_map != [[0, proof['host_uid'], 1]] or not cap & (1 << 13):
        raise RuntimeError('capture_user_mapping_or_CAP_NET_RAW_unavailable')
    if [name for _, name in socket.if_nameindex()] != ['lo']:
        raise RuntimeError('capture_namespace_contains_non_loopback_interface')
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
        flags = struct.unpack('16sH', fcntl.ioctl(control.fileno(), 0x8913,
                              struct.pack('16sH', b'lo', 0))[:18])[1]
    if not flags & 1 or not flags & 8:
        raise RuntimeError('capture_loopback_not_up')
    return {'isolated': True, 'net_namespace': net, 'user_namespace': user,
            'uid_map': uid_map, 'cap_eff': cap, 'interfaces': ['lo'], 'loopback_flags': flags}


class PassiveSedpObserver:
    """Bounded capture. start returns ready; stop is idempotent and retains failures."""
    def __init__(self, spec):
        supplied = copy.deepcopy(spec)
        expected = supplied.pop('spec_sha256', None)
        if hashlib.sha256(_canonical(supplied)).hexdigest() != expected:
            raise ValueError('capture_spec_hash_mismatch')
        self.spec = copy.deepcopy(spec)
        self.output = Path(spec['output_dir'])
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._events = deque(maxlen=spec['limits']['activity_limit'])
        # Filtered-frame metadata is bounded by the capture packet limit. It is
        # intentionally separate from the retained pcap and never stores payloads.
        self._filtered = []
        self._target_prefixes = set()
        self._filtered_overflow = 0
        self._counters = Counter()
        self._diagnostics = Counter()
        self._thread = self._socket = self._raw = self._index = None
        self._ready = self._stopped = None
        self._error = self._limit_reason = None
        self._first_activity_evicted_ns = None
        self._raw_digest = hashlib.sha256()
        self._index_digest = hashlib.sha256()
        self._filtered_digest = hashlib.sha256()

    def start(self):
        if self._ready is not None or self._stopped is not None:
            raise RuntimeError('capture_start_only_once')
        namespace = validate_capture_namespace(self.spec['netns_identity'])
        self.output.mkdir(parents=True, exist_ok=True)
        try:
            self._raw = (self.output / 'normal-rtps.pcap').open('xb')
            self._index = (self.output / 'packet-index.jsonl').open('xb')
            self._filtered_index = (self.output / 'filtered-rtps-index.jsonl').open('xb')
            header = struct.pack('<IHHIIII', 0xa1b23c4d, 2, 4, 0, 0, self.spec['limits']['snaplen'], 1)
            self._raw.write(header)
            self._raw_digest.update(header)
            self._socket = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.spec['limits']['recv_buffer_bytes'])
            self._socket.setsockopt(socket.SOL_SOCKET, 35, 1)  # Linux SO_TIMESTAMPNS
            self._socket.bind(('lo', 0))
            self._socket.settimeout(.05)
            self._ready = {'schema': SCHEMA, 'status': 'ready', **_now(),
                           'run_id': self.spec['run_id'], 'domain_id': self.spec['domain_id'],
                           'spec_sha256': self.spec['spec_sha256'], 'namespace': namespace,
                           'process': _process_identity(), 'interface': 'lo', 'linktype': 1,
                           'timestamp_clock': 'kernel_SO_TIMESTAMPNS_realtime',
                           'recv_buffer_bytes': self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
                           'observer_source': {'path': str(Path(__file__).resolve()), 'sha256': _sha(__file__)},
                           'framing_source_sha256': _sha(Path(__file__).with_name('rtps.py'))}
            self._started_monotonic = time.monotonic()
            self._thread = threading.Thread(target=self._run, name='r2f-passive-sedp', daemon=True)
            self._thread.start()
            return copy.deepcopy(self._ready)
        except BaseException:
            for obj in (self._socket, self._raw, self._index, getattr(self, '_filtered_index', None)):
                if obj is not None:
                    obj.close()
            raise

    @property
    def ready(self):
        return copy.deepcopy(self._ready)

    def _run(self):
        try:
            while not self._stop_event.is_set():
                if time.monotonic() - self._started_monotonic >= self.spec['limits']['timeout_sec']:
                    self._limit_reason = 'timeout_sec'
                    break
                try:
                    frame, ancillary, flags, address = self._socket.recvmsg(self.spec['limits']['snaplen'], 128, socket.MSG_TRUNC)
                except socket.timeout:
                    continue
                # recvmsg with MSG_TRUNC still only returns snaplen bytes in Python;
                # flag is retained, original length is unavailable on this API.
                received = _now()
                timestamp, clock = received['unix_ns'], 'userspace_fallback'
                for level, kind, data in ancillary:
                    if level == socket.SOL_SOCKET and kind == 35 and len(data) >= 16:
                        sec, ns = struct.unpack('=qq', data[:16])
                        timestamp, clock = sec * 10**9 + ns, 'kernel_timestampns'
                with self._lock:
                    self._counters['frames_received'] += 1
                    if clock != 'kernel_timestampns':
                        self._counters['timestamp_fallback'] += 1
                    if flags & socket.MSG_TRUNC:
                        self._counters['truncated_frames'] += 1
                    udp, diagnostic = udp_frame(frame)
                    if diagnostic:
                        self._diagnostics[diagnostic] += 1
                    rtps_metadata = None
                    if udp is not None:
                        if udp['payload'][:4] != b'RTPS':
                            if not flags & socket.MSG_TRUNC:
                                self._counters['frames_skipped'] += 1
                                continue
                        else:
                            rtps_metadata = _rtps_metadata(udp, truncated=bool(flags & socket.MSG_TRUNC))
                        standard_port = bool({udp['source_port'], udp['destination_port']} & set(self.spec['discovery_ports']))
                        # Port numbers are only a hint. Retain every potentially
                        # discovery-related frame in this already validated netns.
                        if not standard_port and rtps_metadata is not None and rtps_metadata['classification'] == 'proven_no_discovery':
                            self._counters['frames_skipped'] += 1
                            self._record_filtered_rtps(frame, udp, timestamp, clock,
                                                       received['monotonic_ns'], address[2],
                                                       truncated=False, metadata=rtps_metadata)
                            continue
                    elif not diagnostic:
                        self._counters['frames_skipped'] += 1
                        continue
                    offset = self._raw.tell()
                    if offset + 16 + len(frame) > self.spec['limits']['max_bytes']:
                        self._limit_reason = 'max_bytes'
                        break
                    if self._counters['packets_written'] >= self.spec['limits']['max_packets']:
                        self._limit_reason = 'max_packets'
                        break
                    index = self._counters['packets_written']
                    record = struct.pack('<IIII', timestamp // 10**9, timestamp % 10**9, len(frame), len(frame)) + frame
                    self._raw.write(record)
                    self._raw_digest.update(record)
                    raw_ref = {'path': str(self.output / 'normal-rtps.pcap'), 'packet_index': index,
                               'record_offset': offset, 'frame_offset': offset + 16,
                               'captured_length': len(frame), 'original_length': None if flags & socket.MSG_TRUNC else len(frame),
                               'frame_sha256': hashlib.sha256(frame).hexdigest()}
                    activities, errors = discovery_activity(frame)
                    if rtps_metadata is not None:
                        errors = list(dict.fromkeys(errors + rtps_metadata['parse_diagnostics']))
                    self._diagnostics.update(errors)
                    entry = {'packet_index': index, 'unix_ns': timestamp, 'clock': clock,
                             'received_monotonic_ns': received['monotonic_ns'],
                             'packet_type': address[2], 'truncated': bool(flags & socket.MSG_TRUNC),
                             'raw_ref': raw_ref, 'activity': activities, 'diagnostics': errors,
                             'rtps_metadata': rtps_metadata}
                    line = _canonical(entry) + b'\n'
                    self._index.write(line)
                    self._index_digest.update(line)
                    for event in activities:
                        if len(self._events) == self._events.maxlen:
                            self._first_activity_evicted_ns = self._events[0]['unix_ns']
                            self._counters['activity_events_evicted'] += 1
                        self._events.append({**event, 'unix_ns': timestamp, 'clock': clock,
                                             'received_monotonic_ns': received['monotonic_ns'], 'raw_ref': raw_ref})
                    self._counters['packets_written'] += 1
        except Exception as exc:
            self._error = type(exc).__name__ + ':' + str(exc)
        finally:
            self._stop_event.set()

    def _health(self):
        reasons = []
        if self._error:
            reasons.append('capture_error:' + self._error)
        if self._limit_reason:
            reasons.append('capture_limit:' + self._limit_reason)
        if self._counters['kernel_drops']:
            reasons.append('kernel_drops')
        if self._counters['truncated_frames']:
            reasons.append('truncated_frames')
        if self._counters['timestamp_fallback']:
            reasons.append('timestamp_fallback')
        if self._diagnostics:
            reasons.append('unsupported_or_invalid_framing')
        related = [record for record in self._filtered
                   if self._record_may_match_target(record, self._target_prefixes)]
        if related:
            reasons.append('filtered_target_related_rtps')
        if self._filtered_overflow:
            reasons.append('filtered_rtps_metadata_overflow')
        return {'status': 'degraded' if reasons else 'ok', 'reasons': reasons,
                'kernel_drop_status': 'known_at_stop' if self._stopped is not None else 'pending_stop',
                'filtered_rtps_completeness': 'incomplete' if related or self._filtered_overflow else 'complete_for_known_target_prefixes',
                'filtered_rtps_count': self._counters['filtered_rtps'],
                'filtered_rtps_target_related_count': len(related),
                'filtered_rtps_metadata_overflow': self._filtered_overflow}

    @staticmethod
    def _record_may_match_target(record, prefixes):
        if record.get('truncated') or record.get('parse_diagnostics') or record.get('classification') == 'unknown':
            # Incomplete or unsupported decoding cannot prove a datagram unrelated,
            # even when its UDP ports sit outside this domain's usual block.
            return True
        return any(item['classification'] == 'discovery' and
                   {item['source_guid_prefix'], item['destination_guid_prefix']} & prefixes
                   for item in record['submessage_entities'])

    def _record_filtered_rtps(self, frame, udp, timestamp, clock, received_monotonic_ns,
                              packet_type, *, truncated, metadata):
        port_start, port_end = self.spec['port_range']
        ports = (udp['source_port'], udp['destination_port'])
        record = {'unix_ns': timestamp, 'clock': clock,
                  'received_monotonic_ns': received_monotonic_ns,
                  'frame_sha256': hashlib.sha256(frame).hexdigest(),
                  'captured_length': len(frame), 'truncated': bool(truncated),
                  'packet_type': packet_type, 'ip_version': udp.get('ip_version'),
                  'source_address': udp.get('source_address'),
                  'destination_address': udp.get('destination_address'),
                  'source_port': udp['source_port'], 'destination_port': udp['destination_port'],
                  'domain_id': self.spec['domain_id'],
                  'domain_relation': 'domain_port_block' if any(port_start <= p <= port_end for p in ports) else 'outside_domain_port_block',
                  **metadata,
                  'filter_reason': 'proven_no_discovery_outside_standard_discovery_ports'}
        self._counters['filtered_rtps'] += 1
        self._counters['filtered_rtps_by_domain_relation:' + record['domain_relation']] += 1
        for kind in record['submessage_kinds']:
            self._counters['filtered_rtps_submessage:' + kind] += 1
        if self._record_may_match_target(record, self._target_prefixes):
            self._counters['filtered_rtps_target_related'] += 1
        line = _canonical(record) + b'\n'
        if len(self._filtered) < self.spec['limits']['max_packets']:
            self._filtered.append(record)
            self._filtered_index.write(line)
            self._filtered_digest.update(line)
        else:
            self._filtered_overflow += 1
            self._counters['filtered_rtps_metadata_overflow'] += 1

    def snapshot_activity(self, *, since_ns, participant_prefixes):
        """Positive observed activity only. Empty window is unknown, never absence."""
        if self._ready is None:
            raise RuntimeError('capture_not_started')
        if isinstance(since_ns, bool) or not isinstance(since_ns, int) or since_ns < 0:
            raise ValueError('since_ns must be realtime unix nanoseconds')
        prefixes = list(participant_prefixes)
        if not prefixes or any(not isinstance(p, str) or not re.fullmatch('[0-9a-fA-F]{24}', p) for p in prefixes):
            raise ValueError('explicit observed 12-byte participant prefixes required')
        wanted = {p.lower() for p in prefixes}
        with self._lock:
            self._target_prefixes.update(wanted)
            if self._raw is not None and not self._raw.closed:
                self._raw.flush()
                self._index.flush()
                self._filtered_index.flush()
            end = time.time_ns()
            matches = [copy.deepcopy(e) for e in self._events
                       if since_ns <= e['unix_ns'] <= end and e['source_guid_prefix'] in wanted]
            # Dedup loopback copies/retransmissions only within identical body identity.
            seen, unique = set(), []
            for e in matches:
                key = (e['frame_sha256'], e['submessage_offset'])
                if key not in seen:
                    unique.append(e)
                    seen.add(key)
            covered = since_ns >= self._ready['unix_ns'] and (self._first_activity_evicted_ns is None or since_ns > self._first_activity_evicted_ns)
            health = self._health()
            incomplete_filtered = health['filtered_rtps_completeness'] == 'incomplete'
            return {'schema': SCHEMA, 'kind': 'bounded_discovery_activity',
                    'status': 'unknown' if incomplete_filtered or health['status'] != 'ok' or not unique else 'observed',
                    'activity_observed': bool(unique),
                    'observation_completeness': 'incomplete' if health['status'] != 'ok' else 'complete_for_known_target_prefixes',
                    'reason': ('capture_incomplete_filtered_target_related_rtps' if incomplete_filtered else
                               'capture_health_degraded' if health['status'] != 'ok' else
                               None if unique else 'no_attributed_activity_in_finite_window'),
                    'since_ns': since_ns, 'until_ns': end, 'clock': 'unix_ns_realtime',
                    'monotonic_ns': time.monotonic_ns(),
                    'counts_by_participant': dict(Counter(e['source_guid_prefix'] for e in unique)),
                    'all_participants_observed': all(any(e['source_guid_prefix'] == p for e in unique) for p in wanted),
                    'missing_participant_prefixes': sorted(wanted - {e['source_guid_prefix'] for e in unique}),
                    'participant_prefixes': sorted(wanted), 'events': unique,
                    'counts_by_kind': dict(Counter(e['kind'] for e in unique)),
                    'packet_event_copies': len(matches), 'deduplicated_event_count': len(unique),
                    'window_buffer_retained': covered, 'health': health,
                    'filtered_rtps': {'count': sum(1 for record in self._filtered if since_ns <= record['unix_ns'] <= end),
                        'target_related': [copy.deepcopy(record) for record in self._filtered
                            if since_ns <= record['unix_ns'] <= end and self._record_may_match_target(record, wanted)],
                        'metadata_overflow': self._filtered_overflow,
                        'metadata_complete': self._filtered_overflow == 0,
                        'completeness': 'incomplete' if incomplete_filtered else 'complete_for_known_target_prefixes'},
                    'raw_prefix': {'path': str(self.output / 'normal-rtps.pcap'),
                                   'sha256': self._raw_digest.hexdigest(),
                                   'size': self._raw.tell() if self._raw is not None and not self._raw.closed else (self._stopped or {}).get('raw', {}).get('size'),
                                   'final': self._stopped is not None},
                    'ready': copy.deepcopy(self._ready),
                    'assurance': 'wire transmit/control activity; not proof remote vendor installed endpoint'}

    def stop(self):
        if self._stopped is not None:
            return copy.deepcopy(self._stopped)
        if self._ready is None:
            raise RuntimeError('capture_not_started')
        self._stop_event.set()
        self._thread.join(timeout=2)
        if self._thread.is_alive():
            raise RuntimeError('capture_thread_did_not_stop')
        with self._lock:
            try:
                kernel = self._socket.getsockopt(263, 6, 12)  # SOL_PACKET/PACKET_STATISTICS
                vals = struct.unpack('=' + 'I' * (len(kernel) // 4), kernel)
                self._counters.update(kernel_packets=vals[0], kernel_drops=vals[1])
            except OSError as exc:
                self._error = (self._error + ';' if self._error else '') + 'packet_statistics:' + str(exc)
            self._socket.close()
            for f in (self._raw, self._index, self._filtered_index):
                f.flush()
                os.fsync(f.fileno())
                f.close()
            raw = {'path': str(self.output / 'normal-rtps.pcap'), 'sha256': self._raw_digest.hexdigest(),
                   'size': (self.output / 'normal-rtps.pcap').stat().st_size, 'format': 'pcap_ns_le', 'linktype': 1}
            self._stopped = {'schema': SCHEMA, 'status': 'stopped', **_now(), 'spec': copy.deepcopy(self.spec),
                             'ready': copy.deepcopy(self._ready), 'raw': raw,
                             'index': {'path': str(self.output / 'packet-index.jsonl'), 'sha256': self._index_digest.hexdigest()},
                             'filtered_rtps_index': {'path': str(self.output / 'filtered-rtps-index.jsonl'),
                                 'sha256': self._filtered_digest.hexdigest(),
                                 'records': len(self._filtered), 'overflow': self._filtered_overflow},
                             'counters': dict(self._counters), 'diagnostics': dict(self._diagnostics),
                             'error': self._error, 'limit_reason': self._limit_reason,
                             'thread_stopped': True, 'socket_closed': True,
                             'participant_created': False, 'packet_transmission': False}
            self._stopped['health'] = self._health()
            with (self.output / 'capture-receipt.json').open('x') as f:
                json.dump(self._stopped, f, indent=2, sort_keys=True)
                f.write('\n')
            receipt_path = self.output / 'capture-receipt.json'
            self._stopped['receipt_raw'] = {'path': str(receipt_path), 'sha256': _sha(receipt_path)}
            return copy.deepcopy(self._stopped)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()
