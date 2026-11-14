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

from .rtps import discovery_activity, udp_frame

SCHEMA = 'r2f.passive-sedp-capture/v1'
DEFAULT_LIMITS = {'timeout_sec': 120, 'max_bytes': 64 * 1024 * 1024, 'max_packets': 100000,
                  'snaplen': 65535, 'recv_buffer_bytes': 4 * 1024 * 1024,
                  'activity_limit': 4096}


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
        self._counters = Counter()
        self._diagnostics = Counter()
        self._thread = self._socket = self._raw = self._index = None
        self._ready = self._stopped = None
        self._error = self._limit_reason = None
        self._first_activity_evicted_ns = None
        self._raw_digest = hashlib.sha256()
        self._index_digest = hashlib.sha256()

    def start(self):
        if self._ready is not None or self._stopped is not None:
            raise RuntimeError('capture_start_only_once')
        namespace = validate_capture_namespace(self.spec['netns_identity'])
        self.output.mkdir(parents=True, exist_ok=True)
        try:
            self._raw = (self.output / 'normal-rtps.pcap').open('xb')
            self._index = (self.output / 'packet-index.jsonl').open('xb')
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
            for obj in (self._socket, self._raw, self._index):
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
                    if udp is not None:
                        lo, hi = self.spec['port_range']
                        if udp['payload'][:4] != b'RTPS' or not ({udp['source_port'], udp['destination_port']} & set(self.spec['discovery_ports'])):
                            self._counters['frames_skipped'] += 1
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
                    self._diagnostics.update(errors)
                    entry = {'packet_index': index, 'unix_ns': timestamp, 'clock': clock,
                             'received_monotonic_ns': received['monotonic_ns'],
                             'packet_type': address[2], 'truncated': bool(flags & socket.MSG_TRUNC),
                             'raw_ref': raw_ref, 'activity': activities, 'diagnostics': errors}
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
        return {'status': 'degraded' if reasons else 'ok', 'reasons': reasons,
                'kernel_drop_status': 'known_at_stop' if self._stopped is not None else 'pending_stop'}

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
            if self._raw is not None and not self._raw.closed:
                self._raw.flush()
                self._index.flush()
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
            return {'schema': SCHEMA, 'kind': 'bounded_discovery_activity',
                    'status': 'observed' if unique else 'unknown', 'activity_observed': bool(unique),
                    'reason': None if unique else 'no_attributed_activity_in_finite_window',
                    'since_ns': since_ns, 'until_ns': end, 'clock': 'unix_ns_realtime',
                    'monotonic_ns': time.monotonic_ns(),
                    'counts_by_participant': dict(Counter(e['source_guid_prefix'] for e in unique)),
                    'all_participants_observed': all(any(e['source_guid_prefix'] == p for e in unique) for p in wanted),
                    'missing_participant_prefixes': sorted(wanted - {e['source_guid_prefix'] for e in unique}),
                    'participant_prefixes': sorted(wanted), 'events': unique,
                    'counts_by_kind': dict(Counter(e['kind'] for e in unique)),
                    'packet_event_copies': len(matches), 'deduplicated_event_count': len(unique),
                    'window_buffer_retained': covered, 'health': self._health(),
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
            for f in (self._raw, self._index):
                f.flush()
                os.fsync(f.fileno())
                f.close()
            raw = {'path': str(self.output / 'normal-rtps.pcap'), 'sha256': self._raw_digest.hexdigest(),
                   'size': (self.output / 'normal-rtps.pcap').stat().st_size, 'format': 'pcap_ns_le', 'linktype': 1}
            self._stopped = {'schema': SCHEMA, 'status': 'stopped', **_now(), 'spec': copy.deepcopy(self.spec),
                             'ready': copy.deepcopy(self._ready), 'raw': raw,
                             'index': {'path': str(self.output / 'packet-index.jsonl'), 'sha256': self._index_digest.hexdigest()},
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
