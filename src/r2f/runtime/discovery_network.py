"""Local discovery-drop carrier; standard library only, no DDS participant.

Launch the *whole* public-API controller and its children with::

    subprocess.Popen(namespace_command([sys.executable, '-m', CONTROLLER]), ...)

Inside that controller::

    carrier = DiscoveryDropController(domain_id)
    health = carrier.setup()  # initial namespace/tool health; no drop installed
    # Wait for real worker match tokens; retain their raw evidence elsewhere.
    try:
        begin = carrier.start(match_receipt={
            'domain_id': domain_id, 'match_established': True,
            'evidence_ref': 'raw.jsonl:writer_and_reader_match',
            'writer_gid': WRITER_GID, 'reader_gid': READER_GID,
            'worker_pids': {'writer': WRITER_PID, 'reader': READER_PID},
        })
        # Observe public ROS2 API behavior. Never inject a synthetic DDS packet.
    finally:
        end = carrier.restore()  # rules restored, recovery_verified=False
    # Wait for fresh independent public-API activity, then confirm_recovery().

start() accepts an upstream match receipt, not an inference from firewall hits.
The separate start_control() gate is exclusively for non-DDS UDP calibration;
its receipts explicitly do not establish ROS2 match or S1 execution. Counters
only establish port-filter effects, never discovery semantics or SUT failures.
Ports assume the default RTPS mapping (no custom transport/discovery ports).
Integration config hints, to apply identically to baseline/transparent/mutated:
CycloneDDS: General/Transport=udp, Interfaces/NetworkInterface name=lo;
Discovery/ParticipantIndex=0 for writer, 1 for reader (not none), SPDPInterval=1s.
Fast DDS: explicit UDPv4 user transport on 127.0.0.1, builtin transports off;
rtps/participantID=0/1, builtin/discovery_config/leaseAnnouncement=1s.
These are hints from local installed schemas, not a verified ROS2 profile.
Record effective config bytes/hash per worker and keep the same lease period
across arms. A short drop with packets=0 stays hit=False; wait for real traffic
or report no hit. Do not inject calibration packets into a ROS2 evidence run.
Both endpoints must remain in this namespace; no setns, veth, host rule, NAT,
external interface, native participant, payload mutation, or package install.

A second, opt-in carrier (``DiscoveryNetemController``) applies bounded delay,
reorder and duplication with ``tc`` netem on a prio qdisc whose filters match
exact derived UDP destination ports (IPv4 and IPv6). The spec's ``scope`` field
selects the port classes: ``discovery`` (default; the derived discovery ports,
legacy behaviour), ``data`` (default-RTPS user-data ports via
``rtps_user_data_ports``) or ``both`` (union, with each class kept as its own
labelled group). It does not drop packets (``loss`` is always ``0%`` and is
declared), never installs nft rules, and therefore cannot produce
``discovery_drop`` counters or hits. A zero-delay/zero-reorder/zero-duplicate
spec installs a transparent class and reports ``neutral=True``; that arm is a
no-op control, not a mutation hit. This carrier is environmental capability
only: it creates no SUT candidate and is not part of the 72-cell maturity
denominator. Data-scope receipts carry ``hit_semantics =
netem_matched_data_plane_packets_not_sut_defect`` and
``responsibility_layer = environment_harness`` so a counted packet is never
mistaken for a SUT defect.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid


_PROOF_ENV = 'R2F_DISCOVERY_NETNS_PROOF'
_TABLE_PATTERN = re.compile(r'r2f_discovery_[0-9a-f]{16}')
_COUNTER = 'discovery_drop'


class NetworkIsolationError(RuntimeError):
    """No network command may run without verified local namespace isolation."""


class DiscoveryNetworkError(RuntimeError):
    def __init__(self, message, receipt=None):
        super().__init__(message)
        self.receipt = receipt or {}


@dataclass(frozen=True)
class DiscoveryPorts:
    domain_id: int
    participant_count: int = 32

    def __post_init__(self):
        for name, upper in (('domain_id', 232), ('participant_count', 120)):
            value = getattr(self, name)
            lower = 0 if name == 'domain_id' else 1
            if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
                raise ValueError(f'{name} must be an integer in {lower}..{upper}')
        if self.base + 11 + 2 * (self.participant_count - 1) > 65535:
            raise ValueError('participant range exceeds UDP port 65535 in this domain')

    @property
    def base(self):
        return 7400 + 250 * self.domain_id

    @property
    def multicast(self):
        return self.base

    @property
    def unicast(self):
        return tuple(self.base + 10 + 2 * p for p in range(self.participant_count))

    @property
    def discovery(self):
        return (self.multicast, *self.unicast)

    @property
    def user_data(self):
        return (self.base + 1, *(p + 1 for p in self.unicast))

    def receipt(self):
        return {
            'domain_id': self.domain_id, 'PB': 7400, 'DG': 250, 'PG': 2,
            'participant_ids': list(range(self.participant_count)),
            'builtin_multicast_offset': 0, 'builtin_unicast_offset': 10,
            'discovery_udp_ports': list(self.discovery),
            'excluded_user_data_udp_ports': list(self.user_data),
            'mapping': 'default_rtps_udp_ports_only',
        }


def build_drop_rules(ports: DiscoveryPorts, table: str) -> str:
    """An atomic nft transaction; exact dport set, input on lo, UDP only."""
    if not isinstance(ports, DiscoveryPorts) or not _TABLE_PATTERN.fullmatch(table):
        raise ValueError('invalid ports or owned table name')
    selected = ', '.join(map(str, ports.discovery))
    return (
        f'add table inet {table}\n'
        f'add counter inet {table} {_COUNTER}\n'
        f'add chain inet {table} discovery_input '
        '{ type filter hook input priority -10; policy accept; }\n'
        f'add rule inet {table} discovery_input iifname "lo" meta l4proto udp '
        f'udp dport {{ {selected} }} counter name {_COUNTER} drop '
        'comment "r2f discovery ports only"\n'
    )


def _namespace(kind):
    return os.readlink(f'/proc/self/ns/{kind}')


def _cap_eff():
    for line in Path('/proc/self/status').read_text().splitlines():
        if line.startswith('CapEff:'):
            return int(line.split()[1], 16)
    raise NetworkIsolationError('cannot read effective capabilities')


def _host_identity():
    return {
        'host_net_namespace': _namespace('net'),
        'host_user_namespace': _namespace('user'),
        'host_uid': os.geteuid(), 'host_cap_eff': _cap_eff(),
        'launcher_pid': os.getpid(),
    }


def namespace_command(argv, *, python_executable=None):
    """Build, never execute, an unshare wrapper. argv is an argument sequence.

    Keep the launch environment (including PYTHONPATH and ROS setup). The inner
    wrapper writes no stdout, preserving controller/worker JSONL protocols.
    The launcher must be an unprivileged host user without CAP_NET_ADMIN.
    """
    if not isinstance(argv, (list, tuple)) or not argv or any(
        not isinstance(a, str) or not a or '\0' in a for a in argv
    ):
        raise ValueError('argv must be a nonempty sequence of nonempty strings')
    host = _host_identity()
    if host['host_uid'] == 0 or host['host_cap_eff'] & (1 << 12):
        raise NetworkIsolationError('launcher must be nonroot without host CAP_NET_ADMIN')
    if _PROOF_ENV in os.environ:
        raise NetworkIsolationError('nested discovery namespace launch is not supported')
    return [
        'unshare', '--user', '--map-root-user', '--net', '--',
        python_executable or sys.executable, '-B', '-m', __name__, '_enter',
        json.dumps(host, sort_keys=True), '--', *argv,
    ]


def _validate_identity(proof, *, require_up=True):
    try:
        net, user = _namespace('net'), _namespace('user')
        host_uid = proof['host_uid']
        if isinstance(host_uid, bool) or not isinstance(host_uid, int) or host_uid <= 0:
            raise ValueError('invalid host uid')
        if proof['host_cap_eff'] & (1 << 12):
            raise ValueError('host had CAP_NET_ADMIN')
        if net == proof['host_net_namespace'] or user == proof['host_user_namespace']:
            raise ValueError('still in host namespace')
        if net != proof['net_namespace'] or user != proof['user_namespace']:
            raise ValueError('namespace changed since wrapper entry')
        uid_map = [tuple(map(int, line.split())) for line in Path('/proc/self/uid_map').read_text().splitlines()]
        if os.geteuid() != 0 or uid_map != [(0, host_uid, 1)]:
            raise ValueError('not the expected single-user root mapping')
        interfaces = socket.if_nameindex()
        if [name for _, name in interfaces] != ['lo']:
            raise ValueError('namespace has an interface other than lo')
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            flags = struct.unpack('16sH', fcntl.ioctl(
                probe.fileno(), 0x8913, struct.pack('16sH', b'lo', 0)
            )[:18])[1]
        if not flags & 8 or (require_up and not flags & 1):
            raise ValueError('loopback is not ready')
        if not _cap_eff() & (1 << 12):
            raise ValueError('no CAP_NET_ADMIN inside namespace')
        return {
            **proof, 'observed_net_namespace': net, 'observed_user_namespace': user,
            'uid_map': [list(row) for row in uid_map], 'euid': os.geteuid(),
            'cap_eff': _cap_eff(), 'interfaces': ['lo'], 'loopback_flags': flags,
            'isolated': True,
        }
    except (KeyError, OSError, TypeError, ValueError) as exc:
        raise NetworkIsolationError(f'namespace isolation check failed: {exc}') from exc


def assert_isolated_namespace(*, require_up=True):
    """Fail before any ip/nft command on missing/mismatched isolation evidence."""
    try:
        proof = json.loads(os.environ[_PROOF_ENV])
        if not isinstance(proof, dict) or proof.get('schema_version') != 1:
            raise ValueError('unsupported proof schema')
    except (KeyError, ValueError, TypeError) as exc:
        raise NetworkIsolationError('missing or invalid unshare wrapper proof') from exc
    return _validate_identity(proof, require_up=require_up)


def _execute(argv, *, input_text=None, require_up=True):
    isolation = assert_isolated_namespace(require_up=require_up)
    command = [str(x) for x in argv]
    executable = shutil.which(command[0])
    record = {'argv': command, 'started_monotonic_ns': time.monotonic_ns(),
              'net_namespace': isolation['observed_net_namespace']}
    if executable is None:
        raise DiscoveryNetworkError('network tool unavailable', {**record, 'error': 'tool_missing'})
    try:
        result = subprocess.run([executable, *command[1:]], input=input_text, text=True,
                                capture_output=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DiscoveryNetworkError('network command failed', {
            **record, 'error': type(exc).__name__, 'returncode': None,
        }) from exc
    record.update(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr,
                  ended_monotonic_ns=time.monotonic_ns())
    if result.returncode:
        raise DiscoveryNetworkError('network command returned failure', record)
    return record


def _json_command(argv):
    record = _execute(argv)
    try:
        payload = json.loads(record['stdout'])
        if not isinstance(payload, dict) or not isinstance(payload.get('nftables'), list):
            raise ValueError('missing nftables list')
        if any(not isinstance(entry, dict) for entry in payload['nftables']):
            raise ValueError('nftables entries must be objects')
    except (ValueError, TypeError) as exc:
        raise DiscoveryNetworkError('invalid nft JSON', record) from exc
    return payload, record


def _has_table(payload, table):
    for item in payload['nftables']:
        if 'table' not in item:
            continue
        entry = item['table']
        if not isinstance(entry, dict):
            raise DiscoveryNetworkError('malformed nft table observation')
        if entry.get('family') == 'inet' and entry.get('name') == table:
            return True
    return False


def parse_counter(payload, table):
    """Missing/malformed data is an error, never a synthetic zero or hit."""
    try:
        entries = []
        for entry in payload['nftables']:
            if not isinstance(entry, dict):
                raise ValueError('malformed nft entry')
            if 'counter' not in entry:
                continue
            counter = entry['counter']
            if not isinstance(counter, dict):
                raise ValueError('malformed nft counter')
            if (counter.get('family') == 'inet' and counter.get('table') == table
                    and counter.get('name') == _COUNTER):
                entries.append(counter)
        if len(entries) != 1:
            raise ValueError('expected exactly one owned counter')
        counter = entries[0]
        for field in ('packets', 'bytes'):
            value = counter[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError('invalid counter value')
        return {key: counter[key] for key in ('packets', 'bytes')}
    except (KeyError, TypeError, ValueError) as exc:
        raise DiscoveryNetworkError(f'nft counter unavailable: {exc}') from exc


MAX_RUNTIME_WINDOWS = 3
WINDOW_ANCHORS = ('fresh_match', 'previous_restore_confirmed')


class WindowPlanError(ValueError):
    pass


class WindowGateError(RuntimeError):
    pass


def validate_window_plan(windows):
    """Validate an exploratory 1..3 window drop->restore plan.

    This is input validation only: it neither applies a rule nor proves a hit.
    The first window must anchor on a fresh match; later windows may only use
    the previous verified restore, which is what makes the chain auditable.
    """
    if not isinstance(windows, (list, tuple)):
        raise WindowPlanError('runtime_windows must be a list')
    if not 1 <= len(windows) <= MAX_RUNTIME_WINDOWS:
        raise WindowPlanError('runtime_windows must contain 1..%d windows' % MAX_RUNTIME_WINDOWS)
    normalized = []
    fields = {'window_id', 'window_index', 'trigger_anchor', 'trigger_offset_sec',
              'drop_duration_sec', 'restore_interval_sec'}
    for index, window in enumerate(windows):
        if not isinstance(window, dict):
            raise WindowPlanError('runtime window must be a dict')
        unknown = set(window) - fields
        if unknown:
            raise WindowPlanError('unknown runtime window field: ' + ','.join(sorted(unknown)))
        window_id = window.get('window_id', 'w%d' % index)
        if not isinstance(window_id, str) or not window_id.strip():
            raise WindowPlanError('runtime window_id required')
        if window.get('window_index', index) != index:
            raise WindowPlanError('runtime window_index must be sequential from zero')
        anchor = window.get('trigger_anchor', 'fresh_match' if index == 0 else 'previous_restore_confirmed')
        if anchor not in WINDOW_ANCHORS:
            raise WindowPlanError('unsupported window trigger_anchor: ' + str(anchor))
        if index == 0 and anchor != 'fresh_match':
            raise WindowPlanError('first window must anchor on fresh_match')
        values = {}
        for key in ('trigger_offset_sec', 'drop_duration_sec', 'restore_interval_sec'):
            value = window.get(key, 0.0)
            if isinstance(value, bool) or not isinstance(value, (int, float)) \
                    or not math.isfinite(value) or not 0.0 <= value <= 30.0:
                raise WindowPlanError('invalid runtime window %s' % key)
            values[key] = float(value)
        if values['drop_duration_sec'] <= 0:
            raise WindowPlanError('drop_duration_sec must be positive')
        normalized.append({'window_id': window_id, 'window_index': index,
                           'trigger_anchor': anchor, **values})
    ids = [item['window_id'] for item in normalized]
    if len(ids) != len(set(ids)):
        raise WindowPlanError('duplicate runtime window_id')
    return normalized


def window_counter_delta(begin, end):
    """Per-window counter delta; a later window can never inherit earlier packets."""
    if not isinstance(begin, dict) or not isinstance(end, dict):
        raise DiscoveryNetworkError('begin and end receipts required')
    if begin.get('table') != end.get('table'):
        raise DiscoveryNetworkError('counter receipts belong to different window tables')
    before, after = begin.get('counter_snapshot'), end.get('counter_snapshot')
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise DiscoveryNetworkError('counter snapshot missing for window delta')
    values = {}
    for label, snapshot in (('before', before), ('after', after)):
        packets, byte_count = snapshot.get('packets'), snapshot.get('bytes')
        if isinstance(packets, bool) or not isinstance(packets, int) or packets < 0 \
                or isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise DiscoveryNetworkError('invalid %s counter values' % label)
        values[label] = (packets, byte_count)
    delta_packets = values['after'][0] - values['before'][0]
    delta_bytes = values['after'][1] - values['before'][1]
    if delta_packets < 0 or delta_bytes < 0:
        raise DiscoveryNetworkError('window counter reset detected; delta cannot be attributed')
    return {'event': 'discovery_window_counter', 'table': begin['table'],
            'window_index': begin.get('window_index'),
            'packets_before': values['before'][0], 'packets_after': values['after'][0],
            'bytes_before': values['before'][1], 'bytes_after': values['after'][1],
            'packets_delta': delta_packets, 'bytes_delta': delta_bytes,
            'counter_observed': True, 'hit': delta_packets > 0,
            'counter_semantics': 'per_window_delta_not_cumulative'}


class WindowLedger:
    """Ordering gate for a 1..3 window drop->restore chain.

    Pure bookkeeping: callers perform the real nft/namespace work. The ledger
    guarantees that a window cannot start before the previous window's restore
    was verified and its fresh post-restore activity was confirmed.
    """

    def __init__(self, windows):
        self.plan = validate_window_plan(windows)
        self.records = []
        self.aborted = None

    def start(self, index, begin_receipt):
        if self.aborted is not None:
            raise WindowGateError('window chain already aborted: ' + self.aborted)
        if index != len(self.records):
            raise WindowGateError('windows must start sequentially')
        if index and not self.records[-1]['recovery_confirmed']:
            raise WindowGateError('previous window restore is not confirmed')
        if index >= len(self.plan):
            raise WindowGateError('window index exceeds plan')
        if not isinstance(begin_receipt, dict) or begin_receipt.get('applied') is not True:
            raise WindowGateError('applied drop begin receipt required')
        self.records.append({'window_index': index, 'window_id': self.plan[index]['window_id'],
                             'begin': json.loads(json.dumps(begin_receipt, allow_nan=False)),
                             'end': None, 'counter': None, 'recovery': None,
                             'recovery_confirmed': False, 'restored': False, 'error': None})
        return self.records[-1]

    def end(self, index, end_receipt):
        if index >= len(self.records) or self.records[index]['end'] is not None:
            raise WindowGateError('window end without matching open window')
        record = self.records[index]
        if not isinstance(end_receipt, dict) or end_receipt.get('restored') is not True:
            raise WindowGateError('verified restore receipt required to close a window')
        record['end'] = json.loads(json.dumps(end_receipt, allow_nan=False))
        record['restored'] = True
        record['counter'] = window_counter_delta(record['begin'], record['end'])
        return record['counter']

    def confirm_recovery(self, index, recovery_receipt):
        if index >= len(self.records) or not self.records[index]['restored']:
            raise WindowGateError('recovery confirmation requires a closed window')
        record = self.records[index]
        if not isinstance(recovery_receipt, dict) or recovery_receipt.get('recovery_verified') is not True:
            raise WindowGateError('recovery_verified receipt required before next window')
        record['recovery'] = json.loads(json.dumps(recovery_receipt, allow_nan=False))
        record['recovery_confirmed'] = True
        return record

    def abort(self, reason):
        self.aborted = str(reason)
        for record in self.records:
            record['error'] = self.aborted
        return {'event': 'discovery_window_chain_aborted', 'reason': self.aborted,
                'open_windows': [r['window_index'] for r in self.records if r['end'] is None],
                'cleanup_required': True, 'rules_must_be_removed': True}

    def summary(self):
        return {'event': 'discovery_window_ledger', 'plan': self.plan,
                'window_count': len(self.records),
                'windows': [{'window_index': r['window_index'], 'window_id': r['window_id'],
                             'restored': r['restored'], 'recovery_confirmed': r['recovery_confirmed'],
                             'counter': r['counter'], 'error': r['error']} for r in self.records],
                'aborted': self.aborted,
                'chain_complete': self.aborted is None and len(self.records) == len(self.plan)
                                  and all(r['recovery_confirmed'] for r in self.records),
                'hit': any(r['counter'] and r['counter']['hit'] for r in self.records),
                'counter_semantics': 'each_window_delta_only'}


class DiscoveryDropController:
    """One temporary discovery drop window, with optional subsequent windows.

    Every returned receipt is JSON serializable. Persist begin/end receipts in
    the caller's raw/execution metadata, independently of deterministic hashes.
    No counter evidence means hit=False and counter_observed=False. Even a
    successful hit is only carrier evidence, not ROS2 semantic evidence.
    """

    def __init__(self, domain_id, participant_count=32):
        self.ports = DiscoveryPorts(domain_id, participant_count)
        self.table = 'r2f_discovery_' + uuid.uuid4().hex[:16]
        self.health = None
        self._attempted = False
        self.active = False
        self._begin = None
        self._end = None

    def setup(self):
        if self._attempted:
            raise DiscoveryNetworkError('restore before setup')
        isolation = assert_isolated_namespace()
        payload, command = _json_command(['nft', '-j', 'list', 'ruleset'])
        if any('table' in entry for entry in payload['nftables']):
            raise DiscoveryNetworkError('initial namespace nft ruleset is not empty', command)
        self.health = {
            'event': 'discovery_network_setup', 'health_ok': True,
            'monotonic_ns': time.monotonic_ns(), 'isolation': isolation,
            'ports': self.ports.receipt(), 'initial_nft': payload,
            'command': command, 'drop_installed': False, 'hit': False,
            'counter_observed': False,
        }
        return self.health

    def pair_bindings(self, worker_pids):
        """Read Linux socket ownership, never generate traffic or inspect DDS.

        All bound UDP sockets in this netns must be owned by exactly these two
        live workers. Each worker must own a selected unicast discovery port. Caller
        still supplies raw public-API GID/match evidence; port ownership alone
        does not prove a DDS discovery packet or endpoint match.
        """
        isolation = assert_isolated_namespace()
        if not isinstance(worker_pids, dict) or set(worker_pids) != {'writer', 'reader'}:
            raise DiscoveryNetworkError('writer and reader worker_pids required')
        pids = list(worker_pids.values())
        if any(isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 or pid == os.getpid()
               for pid in pids) or len(set(pids)) != 2:
            raise DiscoveryNetworkError('two distinct live worker PIDs required')
        try:
            owners, identities = {}, {}
            for role, pid in worker_pids.items():
                root = Path(f'/proc/{pid}')
                if os.readlink(root / 'ns/net') != isolation['observed_net_namespace']:
                    raise DiscoveryNetworkError('worker is outside controller network namespace')
                if os.readlink(root / 'ns/user') != isolation['observed_user_namespace']:
                    raise DiscoveryNetworkError('worker is outside controller user namespace')
                identities[role] = (root / 'stat').read_text().rsplit(')', 1)[1].split()[19]
                for descriptor in (root / 'fd').iterdir():
                    try:
                        target = os.readlink(descriptor)
                    except FileNotFoundError:
                        continue
                    if target.startswith('socket:[') and target.endswith(']'):
                        inode = target[8:-1]
                        owners.setdefault(inode, set()).add(role)
            bindings, unknown = [], []
            for family, filename in (('ipv4', 'udp'), ('ipv6', 'udp6')):
                for line in Path(f'/proc/self/net/{filename}').read_text().splitlines()[1:]:
                    columns = line.split()
                    local, inode = columns[1], columns[9]
                    port = int(local.rsplit(':', 1)[1], 16)
                    roles = sorted(owners.get(inode, ()))
                    record = {'family': family, 'local_address_hex': local, 'port': port,
                              'socket_inode': inode, 'roles': roles,
                              'selected_discovery_port': port in self.ports.discovery,
                              'selected_user_data_port': port in self.ports.user_data}
                    bindings.append(record)
                    if not roles:
                        unknown.append(record)
            if unknown:
                raise DiscoveryNetworkError('UDP sockets exist outside the two workers', {'unknown_bindings': unknown})
            for role, pid in worker_pids.items():
                root = Path(f'/proc/{pid}')
                if ((root / 'stat').read_text().rsplit(')', 1)[1].split()[19] != identities[role]
                        or os.readlink(root / 'ns/net') != isolation['observed_net_namespace']):
                    raise DiscoveryNetworkError('worker identity changed during binding snapshot')
                if not any(role in b['roles'] and b['port'] in self.ports.unicast for b in bindings):
                    raise DiscoveryNetworkError(f'{role} has no selected unicast discovery UDP binding')
            return {'event': 'discovery_pair_bindings', 'monotonic_ns': time.monotonic_ns(),
                    'worker_pids': dict(worker_pids), 'worker_start_ticks': identities,
                    'only_pair_bound_udp_sockets': True, 'bindings': bindings, 'isolation': isolation}
        except (OSError, ValueError, IndexError) as exc:
            raise DiscoveryNetworkError(f'cannot prove worker UDP bindings: {exc}') from exc

    def _start(self, precondition, evidence_kind):
        if self.health is None or self._attempted:
            raise DiscoveryNetworkError('setup required and previous window must be restored')
        assert_isolated_namespace()
        if not isinstance(precondition, dict) or precondition.get('domain_id') != self.ports.domain_id:
            raise DiscoveryNetworkError('precondition receipt domain mismatch or missing')
        if not isinstance(precondition.get('evidence_ref'), str) or not precondition['evidence_ref'].strip():
            raise DiscoveryNetworkError('raw precondition evidence_ref required')
        pair_bindings = None
        if evidence_kind == 'ros2_pair':
            if precondition.get('match_established') is not True:
                raise DiscoveryNetworkError('real established match receipt required before drop')
            gids = [precondition.get(role + '_gid') for role in ('writer', 'reader')]
            if any(not isinstance(gid, str) or not re.fullmatch(r'[0-9a-fA-F]{16,128}', gid)
                   or not int(gid, 16) for gid in gids) or gids[0].lower() == gids[1].lower():
                raise DiscoveryNetworkError('distinct nonzero matched writer/reader GIDs required')
            pair_bindings = self.pair_bindings(precondition.get('worker_pids'))
        elif (precondition.get('healthy') is not True or
              precondition.get('probe_kind') != 'non_dds_loopback_udp'):
            raise DiscoveryNetworkError('non-DDS loopback health receipt required')
        # Deep copy before applying anything, so unserializable receipts cannot
        # leave installed rules behind or mutate after the gate was evaluated.
        precondition = json.loads(json.dumps(precondition, allow_nan=False))
        self.table = 'r2f_discovery_' + uuid.uuid4().hex[:16]
        self._end = None
        self._begin = {
            'event': 'discovery_drop_begin', 'scope': evidence_kind,
            'ros2_execution': evidence_kind == 'ros2_pair',
            'domain_id': self.ports.domain_id, 'table': self.table,
            'drop_begin_monotonic_ns': time.monotonic_ns(),
            'precondition': precondition, 'pair_bindings': pair_bindings, 'ports': self.ports.receipt(),
            'configured': False, 'applied': False, 'hit': False,
            'counter_observed': False,
        }
        self._attempted = True
        try:
            self._begin['install_command'] = _execute(
                ['nft', '-f', '-'], input_text=build_drop_rules(self.ports, self.table))
            self.active = True
            self._begin.update(configured=True, applied=True,
                               drop_applied_monotonic_ns=time.monotonic_ns())
            self._begin['counter_snapshot'] = self.counters()
            self._begin['counter_observed'] = True
            self._begin['hit'] = self._begin['counter_snapshot']['hit']
        except (DiscoveryNetworkError, NetworkIsolationError) as exc:
            self._begin['error'] = str(exc)
            self._begin['failed_command'] = getattr(exc, 'receipt', {})
            try:
                self._begin['restore_after_failure'] = self.restore()
            except (DiscoveryNetworkError, NetworkIsolationError) as cleanup:
                self._begin['restore_error'] = str(cleanup)
                self._begin['restore_failure_receipt'] = getattr(cleanup, 'receipt', {})
            raise DiscoveryNetworkError('discovery drop failed', self._begin) from exc
        return self._begin

    def start(self, *, match_receipt=None):
        return self._start(match_receipt, 'ros2_pair')

    def start_control(self, *, health_receipt):
        """Explicit non-DDS calibration; never substitute for an ROS2 match."""
        return self._start(health_receipt, 'carrier_control_non_dds')

    def counters(self):
        assert_isolated_namespace()
        if not self.active:
            raise DiscoveryNetworkError('no installed drop window')
        payload, command = _json_command(['nft', '-j', 'list', 'table', 'inet', self.table])
        counts = parse_counter(payload, self.table)
        return {'event': 'discovery_drop_counter', 'monotonic_ns': time.monotonic_ns(),
                'table': self.table, **counts, 'counter_observed': True,
                'hit': counts['packets'] > 0, 'nft_json': payload, 'command': command}

    def restore(self):
        isolation = assert_isolated_namespace()
        if not self._attempted:
            return dict(self._end, idempotent=True) if self._end else {
                'event': 'discovery_drop_end', 'restored': True, 'no_drop_attempted': True,
                'hit': False, 'counter_observed': False, 'recovery_verified': False, 'isolation': isolation,
            }
        end = {'event': 'discovery_drop_end', 'scope': self._begin['scope'],
               'domain_id': self.ports.domain_id, 'table': self.table,
               'restore_begin_monotonic_ns': time.monotonic_ns(),
               'drop_begin_monotonic_ns': self._begin['drop_begin_monotonic_ns'],
               'isolation': isolation, 'restored': False, 'recovery_verified': False, 'hit': False,
               'counter_observed': False}
        if self.active:
            try:
                snapshot = self.counters()
                end.update(counter_snapshot=snapshot, hit=snapshot['hit'], counter_observed=True)
            except DiscoveryNetworkError as exc:
                # Deletion is still mandatory if counter observation failed.
                end.update(counter_error=str(exc), counter_failure_receipt=exc.receipt)
        try:
            end['delete_command'] = _execute(['nft', 'delete', 'table', 'inet', self.table])
        except DiscoveryNetworkError as exc:
            end.update(delete_error=str(exc), delete_failure_receipt=exc.receipt)
        try:
            payload, command = _json_command(['nft', '-j', 'list', 'ruleset'])
            end.update(verify_command=command, final_nft=payload,
                       table_absent=not _has_table(payload, self.table))
            end['restored'] = end['table_absent']
        except DiscoveryNetworkError as exc:
            end.update(verify_error=str(exc), verify_failure_receipt=exc.receipt)
        end['drop_end_monotonic_ns'] = time.monotonic_ns()
        self._end = end
        if not end['restored']:
            raise DiscoveryNetworkError('namespace drop restoration not verified', end)
        self.active = False
        self._attempted = False
        return end

    def confirm_recovery(self, *, activity_receipt):
        """Require independent activity observed *after* verified rule deletion.

        This validates the caller's evidence contract, not raw ROS2 facts. For
        ROS2 use fresh public-API tokens with matching GIDs and bound workers.
        Counter snapshots from before restore, arm names, or elapsed time are
        not recovery evidence. Non-DDS control stays explicitly control-only.
        """
        assert_isolated_namespace()
        if not self._end or not self._end['restored'] or self._attempted:
            raise DiscoveryNetworkError('verified restore required before recovery confirmation')
        if not isinstance(activity_receipt, dict):
            raise DiscoveryNetworkError('independent post-restore activity receipt required')
        receipt = json.loads(json.dumps(activity_receipt, allow_nan=False))
        observed = receipt.get('monotonic_ns')
        if (receipt.get('activity_observed') is not True
                or receipt.get('domain_id') != self.ports.domain_id
                or not isinstance(receipt.get('evidence_ref'), str) or not receipt['evidence_ref'].strip()
                or isinstance(observed, bool) or not isinstance(observed, int)
                or not self._end['drop_end_monotonic_ns'] < observed <= time.monotonic_ns()):
            raise DiscoveryNetworkError('fresh post-restore activity and raw evidence_ref required')
        pair_bindings = None
        if self._begin['scope'] == 'ros2_pair':
            before = self._begin['precondition']
            if (receipt.get('source') != 'public_ros2_pair'
                    or any(receipt.get(role + '_gid') != before[role + '_gid'] for role in ('writer', 'reader'))):
                raise DiscoveryNetworkError('post-restore activity must retain matched pair GIDs')
            pair_bindings = self.pair_bindings(before['worker_pids'])
            if pair_bindings['worker_start_ticks'] != self._begin['pair_bindings']['worker_start_ticks']:
                raise DiscoveryNetworkError('worker process generation changed after drop')
        elif receipt.get('source') != 'non_dds_loopback_udp':
            raise DiscoveryNetworkError('carrier control needs independent non-DDS UDP recovery')
        return {'event': 'discovery_recovery_confirmed', 'scope': self._begin['scope'],
                'recovery_verified': True, 'evidence_kind': 'caller_independent_activity',
                's1_execution': False, 'requires_runtime_semantic_adjudication': True,
                'restore_end_monotonic_ns': self._end['drop_end_monotonic_ns'],
                'monotonic_ns': time.monotonic_ns(), 'activity_receipt': receipt,
                'pair_bindings': pair_bindings}

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.restore()


NETEM_IMPLEMENTATION = 'isolated_netns_tc_netem_discovery_udp'
NETEM_SPEC_FIELDS = ('delay_ms', 'reorder_pct', 'duplicate_pct', 'restore_sec')
#: ``scope`` selects which derived UDP destination ports the netem class matches.
#: It is optional on input (absent means ``discovery``) and always echoed in the
#: normalized spec so every receipt states exactly what was classified.
NETEM_SCOPE_FIELD = 'scope'
NETEM_SCOPES = ('discovery', 'data', 'both')
DEFAULT_NETEM_SCOPE = 'discovery'
NETEM_HANDLE = '2:'
NETEM_PARENT = '1:2'
# Local default RTPS mapping offsets (PB=7400, DG=250). They are the same
# offsets already used by DiscoveryPorts, restated here so the user-data
# derivation is auditable on its own instead of being implicit in `unicast + 1`.
RTPS_USER_MULTICAST_OFFSET = 1     # PB + DG*domain + 1
RTPS_USER_UNICAST_BASE_OFFSET = 11  # PB + DG*domain + 11 + 2*participant_id
#: Applies to every netem receipt: the carrier is an environment harness effect.
NETEM_RESPONSIBILITY_LAYER = 'environment_harness'
#: Loss is deliberately absent from the spec fields and rendered as an explicit
#: `loss 0%`. This carrier may only delay, reorder and duplicate packets.
NETEM_LOSS_PCT = 0.0
NETEM_SEMANTICS_NOTE = (
    'Only network-layer delay/reorder/duplicate is applied and loss is fixed at '
    '0%; these are environment effects, not SUT defects and not candidate '
    'admission. For a reliable+ordered DDS contract, reordering and duplication '
    'on the wire must not surface as reader-side order violations or duplicate '
    'deliveries: the primary delivery oracle keeps delivery_order_violation and '
    'duplicate sequences as real defect signals, and no adapter may relax those '
    'residuals as "expected network effects". Candidates stay oracle-decided.'
)


def netem_hit_semantics(scope):
    """Truthful hit wording for a scope: the counted packets are network effects.

    A data window must name data-plane matched packets; ``both`` names the union
    because its single netem class counts either port group, with per-group
    attribution kept in ``group_counters``/``hit_groups``.
    """
    if scope == 'data':
        return 'netem_matched_data_plane_packets_not_sut_defect'
    if scope == 'both':
        return 'netem_matched_discovery_and_data_plane_packets_not_sut_defect'
    return 'netem_matched_packets_not_drop_hit'


class NetemScopeError(DiscoveryNetworkError):
    """Invalid ``scope`` value; carries the offending field name."""

    def __init__(self, message, *, field=NETEM_SCOPE_FIELD, receipt=None):
        super().__init__(message, receipt)
        self.field = field
        self.receipt = {'field': field, 'reason': message, **(receipt or {})}


def _bounded_number(value, field, lower, upper):
    if isinstance(value, bool) or type(value) not in (int, float):
        raise DiscoveryNetworkError(f'{field} must be a number in {lower}..{upper}')
    if not math.isfinite(float(value)) or not lower <= float(value) <= upper:
        raise DiscoveryNetworkError(f'{field} must be in {lower}..{upper}')
    return float(value)


def rtps_user_data_ports(domain_id, participant_count):
    """Auditable default-RTPS user-data port derivation; installs nothing.

    The local RTPS mapping reserves PG=2 ports per participant, so with
    PB=7400 and DG=250 the user-data block is::

        multicast user port = PB + DG*domain + 1
        unicast  user port   = PB + DG*domain + 11 + 2*participant_id

    i.e. the multicast user port is ``PB + DG*domain + d1`` and the unicast
    user ports are ``PB + DG*domain + d2 + 2*p`` for p in 0..count-1.  The
    derivation is returned as a record (offsets, formulas, ports) so a receipt
    can be recomputed from the domain id alone.  Interleaved discovery ports are
    never included: the parity of discovery/unicast user ports differs.
    """
    ports = DiscoveryPorts(domain_id, participant_count)  # validates the range
    multicast_port = ports.base + RTPS_USER_MULTICAST_OFFSET
    unicast_ports = tuple(ports.base + RTPS_USER_UNICAST_BASE_OFFSET + 2 * p
                          for p in range(participant_count))
    selected = (multicast_port, *unicast_ports)
    # Cross-check against the already-audited DiscoveryPorts mapping.
    if selected != ports.user_data:
        raise DiscoveryNetworkError('user_data derivation disagrees with DiscoveryPorts')
    if not set(selected).isdisjoint(ports.discovery):
        raise DiscoveryNetworkError('user_data derivation overlaps discovery ports')
    if max(selected) > 65535:
        raise DiscoveryNetworkError('user_data derivation exceeds UDP port 65535')
    return {
        'mapping': 'default_rtps_udp_user_data_ports',
        'domain_id': domain_id, 'participant_count': participant_count,
        'PB': 7400, 'DG': 250, 'PG': 2,
        # d1 / d2 in PB + DG*domain + d1 (+ 2*p for d2): the offsets are named
        # explicitly so the audit can recompute the set without this module.
        'd1': RTPS_USER_MULTICAST_OFFSET,
        'd2': RTPS_USER_UNICAST_BASE_OFFSET,
        'user_multicast_offset': RTPS_USER_MULTICAST_OFFSET,
        'user_unicast_base_offset': RTPS_USER_UNICAST_BASE_OFFSET,
        'formulas': [
            'multicast = PB + DG*domain_id + d1',
            'unicast[p] = PB + DG*domain_id + d2 + 2*p',
        ],
        'participant_ids': list(range(participant_count)),
        'multicast_port': multicast_port,
        'unicast_ports': list(unicast_ports),
        'ports': list(selected),
        'port_count': len(selected),
        'assumption': 'local default RTPS UDP mapping; no custom transport/discovery ports',
    }


def netem_port_groups(ports, scope):
    """Explicit, labelled port groups for one ``scope`` value.

    ``discovery`` and ``data`` are always separate group records and are never
    merged into an indistinguishable set; ``both`` returns one record per group
    so every filter and counter can be attributed to exactly one of them.
    """
    if not isinstance(ports, DiscoveryPorts):
        raise DiscoveryNetworkError('DiscoveryPorts required')
    if scope not in NETEM_SCOPES:
        raise NetemScopeError('unsupported scope: ' + repr(scope),
                              receipt={'scope': scope, 'allowed': list(NETEM_SCOPES)})
    groups = []
    if scope in ('discovery', 'both'):
        groups.append({
            'group_id': 'discovery', 'port_class': 'discovery_udp_ports',
            'derivation': 'PB + DG*domain_id (multicast) and PB + DG*domain_id + 10 + 2*p (unicast)',
            'ports': list(ports.discovery),
        })
    if scope in ('data', 'both'):
        derivation = rtps_user_data_ports(ports.domain_id, ports.participant_count)
        groups.append({
            'group_id': 'data', 'port_class': 'user_data_udp_ports',
            'derivation': ' + '.join(derivation['formulas']),
            'derivation_receipt': derivation,
            'ports': list(derivation['ports']),
        })
    seen = set()
    for group in groups:
        if not group['ports']:
            raise DiscoveryNetworkError('empty netem port group: ' + group['group_id'])
        if seen.intersection(group['ports']):
            raise DiscoveryNetworkError('overlapping netem port groups: ' + group['group_id'])
        seen.update(group['ports'])
    return groups


def normalize_netem_spec(spec):
    """Validate a netem spec field by field; no defaults are invented.

    Reorder needs a nonzero delay in netem itself, so that combination is
    rejected here instead of failing later after a partial qdisc install.
    ``restore_sec`` is a declared upper bound: 0 means the caller owns restore.
    """
    if not isinstance(spec, dict):
        raise DiscoveryNetworkError('spec must be a dict')
    # A receipt re-normalizes its own stored spec; 'neutral' is a derived flag
    # and 'scope' is optional (absent == 'discovery', byte-identical legacy).
    unknown = set(spec) - set(NETEM_SPEC_FIELDS) - {NETEM_SCOPE_FIELD, 'neutral'}
    if unknown:
        raise DiscoveryNetworkError('spec has unknown field: ' + ','.join(sorted(unknown)))
    missing = [field for field in NETEM_SPEC_FIELDS if field not in spec]
    if missing:
        raise DiscoveryNetworkError('spec missing field: ' + ','.join(missing))
    scope = spec.get(NETEM_SCOPE_FIELD, DEFAULT_NETEM_SCOPE)
    if scope not in NETEM_SCOPES:
        raise NetemScopeError('unsupported scope: ' + repr(scope),
                              receipt={'scope': scope, 'allowed': list(NETEM_SCOPES)})
    delay_ms = _bounded_number(spec['delay_ms'], 'delay_ms', 0, 2000)
    reorder_pct = _bounded_number(spec['reorder_pct'], 'reorder_pct', 0, 100)
    duplicate_pct = _bounded_number(spec['duplicate_pct'], 'duplicate_pct', 0, 100)
    restore_sec = spec['restore_sec']
    if isinstance(restore_sec, bool) or type(restore_sec) not in (int, float) \
            or not math.isfinite(float(restore_sec)) or float(restore_sec) < 0:
        raise DiscoveryNetworkError('restore_sec must be a finite number >= 0')
    restore_sec = float(restore_sec)
    if reorder_pct and not delay_ms:
        raise DiscoveryNetworkError('reorder_pct requires delay_ms > 0')
    return {'delay_ms': delay_ms, 'reorder_pct': reorder_pct,
            'duplicate_pct': duplicate_pct, 'restore_sec': restore_sec,
            'scope': scope,
            'neutral': delay_ms == 0 and reorder_pct == 0 and duplicate_pct == 0}


def netem_spec_is_neutral(spec):
    return normalize_netem_spec(spec)['neutral']


def build_netem_plan(ports, spec):
    """Deterministic tc script and derivation receipt; applies nothing.

    Filters are one exact dport match per selected port and address family. No
    range or wildcard is used. With ``scope='discovery'`` (the default, and the
    byte-identical legacy behaviour) the selected set is exactly the derived
    discovery ports, so interleaved user-data ports are never classified into
    the netem class. ``scope='data'`` selects only the derived user-data ports
    and ``scope='both'`` selects the union while keeping one labelled group per
    port class, so filters and counters stay attributable.
    """
    if not isinstance(ports, DiscoveryPorts):
        raise DiscoveryNetworkError('DiscoveryPorts required')
    normalized = normalize_netem_spec(spec)
    scope = normalized[NETEM_SCOPE_FIELD]
    delay = normalized['delay_ms'] / 1000.0
    netem = f'netem limit 10000 delay {delay:g}s'
    if normalized['reorder_pct']:
        # Correlation is fixed at 0 rather than invented per spec: the reorder
        # percentage is the only caller-controlled field.
        netem += f' reorder {normalized["reorder_pct"]:g}% 0%'
    if normalized['duplicate_pct']:
        netem += f' duplicate {normalized["duplicate_pct"]:g}%'
    netem += f' loss {NETEM_LOSS_PCT:g}%'
    lines = [
        'qdisc add dev lo root handle 1: prio bands 2 '
        'priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0',
        f'qdisc add dev lo parent {NETEM_PARENT} handle {NETEM_HANDLE} {netem}',
    ]
    entries = []
    groups = []
    # prio must differ per protocol family: the kernel rejects two filter
    # priorities bound to different protocols on the same parent.
    for group in netem_port_groups(ports, scope):
        ports_in_group = group['ports']
        for port in ports_in_group:
            for prio, family, key in ((1, 'ip', 'ip'), (2, 'ipv6', 'ip6')):
                lines.append(
                    f'filter add dev lo protocol {family} parent 1:0 prio {prio} u32 '
                    f'match {key} protocol 17 0xff match {key} dport {port} 0xffff '
                    f'flowid {NETEM_PARENT} action gact pass')
                entries.append({'family': family, 'port': port, 'prio': prio,
                                'group_id': group['group_id']})
        groups.append({
            'group_id': group['group_id'], 'port_class': group['port_class'],
            'derivation': group['derivation'], 'ports': list(ports_in_group),
            'filter_count_per_port': 2, 'filter_count': 2 * len(ports_in_group),
            'class_id': NETEM_PARENT,
            **({'derivation_receipt': group['derivation_receipt']}
               if 'derivation_receipt' in group else {}),
        })
    selected = [port for group in groups for port in group['ports']]
    data_selected = any(group['group_id'] == 'data' for group in groups)
    if scope == 'discovery':
        applies_to = 'udp_dport_exact_derived_discovery_ports_only'
    elif scope == 'data':
        applies_to = 'udp_dport_exact_derived_user_data_ports_only'
    else:
        applies_to = 'udp_dport_exact_derived_discovery_and_user_data_ports'
    return {'lines': lines, 'script': '\n'.join(lines) + '\n',
            'spec': {k: v for k, v in normalized.items()},
            'netem_args': netem, 'entries': entries,
            'scope': scope, 'groups': groups,
            'matched_ports': selected,
            'excluded_user_data_ports': [] if data_selected else list(ports.user_data),
            'excluded_discovery_ports': [] if scope != 'data' else list(ports.discovery),
            'implementation': NETEM_IMPLEMENTATION,
            'applies_to': applies_to,
            'loss_pct': NETEM_LOSS_PCT, 'loss_defaulted': True,
            'responsibility_layer': NETEM_RESPONSIBILITY_LAYER,
            'semantics_note': NETEM_SEMANTICS_NOTE,
            'floor_default_class': '1:1'}


def _tc_json(argv):
    record = _execute(argv)
    try:
        payload = json.loads(record['stdout'])
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise ValueError('expected a JSON list of objects')
    except (KeyError, ValueError, TypeError) as exc:
        raise DiscoveryNetworkError('invalid tc JSON', record) from exc
    return payload, record


def _qdisc_stats(entry):
    stats = {}
    for field in ('packets', 'bytes', 'drops', 'overlimits'):
        value = entry.get(field, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise DiscoveryNetworkError('invalid tc qdisc counter: ' + field)
        stats[field] = value
    return stats


def parse_netem_counters(qdisc_payload, filter_payload, *, neutral, groups=None,
                        scope=None):
    """Real tc counters only. Missing data is an error, never a synthetic zero.

    ``groups`` carries the labelled port groups that were actually installed
    (``[{'group_id': 'discovery'|'data', 'ports': [...]}]``). When it is given,
    every filter is attributed to exactly one group and per-group packets/bytes
    are reported. When tc cannot support that attribution the receipt says
    ``group_counters_available=False`` with a reason instead of inventing
    numbers; the flat totals used by older receipts remain available.
    """
    netem = [entry for entry in qdisc_payload if entry.get('kind') == 'netem'
             and entry.get('handle') == NETEM_HANDLE]
    root = [entry for entry in qdisc_payload if entry.get('kind') == 'prio' and entry.get('root') is True]
    if len(netem) != 1 or len(root) != 1:
        raise DiscoveryNetworkError('expected exactly one prio root and one netem class')
    netem_stats, root_stats = _qdisc_stats(netem[0]), _qdisc_stats(root[0])
    if netem_stats['packets'] > root_stats['packets'] or netem_stats['bytes'] > root_stats['bytes']:
        raise DiscoveryNetworkError('netem counter exceeds root counter')
    default_stats = {field: root_stats[field] - netem_stats[field]
                     for field in ('packets', 'bytes', 'drops', 'overlimits')}
    entries, stats_available, totals = [], True, {'packets': 0, 'bytes': 0}
    for entry in filter_payload:
        options = entry.get('options')
        if not isinstance(options, dict) or options.get('flowid') != NETEM_PARENT:
            continue
        match = options.get('match')
        if not isinstance(match, dict) or not isinstance(match.get('value'), str):
            raise DiscoveryNetworkError('owned netem filter without a dport match')
        try:
            port = int(match['value'], 16)
        except ValueError as exc:
            raise DiscoveryNetworkError('malformed dport match value') from exc
        # Real tc -j nests action stats under options.actions; accept either
        # placement but never synthesize missing counters.
        actions = options.get('actions')
        if actions is None:
            actions = entry.get('actions')
        record = {'family': entry.get('protocol'), 'port': port}
        if isinstance(actions, list) and actions and isinstance(actions[0].get('stats'), dict):
            action_stats = _qdisc_stats(actions[0]['stats'])
            record.update(action_stats)
            totals['packets'] += action_stats['packets']
            totals['bytes'] += action_stats['bytes']
        else:
            stats_available = False
        entries.append(record)
    if not entries:
        raise DiscoveryNetworkError('no owned netem filters observed')
    scope = scope or (groups[0].get('group_id') if groups else DEFAULT_NETEM_SCOPE)
    group_counters, group_available, group_reason = _group_counters(entries, groups)
    receipt = {'classes': {NETEM_PARENT: netem_stats, '1:1': default_stats, 'root': root_stats},
               'filter': {'entries': entries, 'stats_available': stats_available,
                          'packets': totals['packets'] if stats_available else None,
                          'bytes': totals['bytes'] if stats_available else None},
               'affected_packets': netem_stats['packets'], 'affected_bytes': netem_stats['bytes'],
               'drops': netem_stats['drops'], 'overlimits': netem_stats['overlimits'],
               'neutral': bool(neutral), 'mutation_hit': bool(not neutral and netem_stats['packets'] > 0),
               'hit_semantics': netem_hit_semantics(scope),
               'scope': scope,
               'group_counters': group_counters,
               'group_counters_available': group_available,
               'group_counters_reason': group_reason,
               'responsibility_layer': NETEM_RESPONSIBILITY_LAYER,
               'loss_pct': NETEM_LOSS_PCT, 'loss_defaulted': True,
               'semantics_note': NETEM_SEMANTICS_NOTE}
    return receipt


def _group_counters(entries, groups):
    """Attribute real filter action counters to declared, labelled port groups.

    Returns ``(counters, available, reason)``. ``available`` is False whenever a
    real number cannot be attributed (missing action stats, a filter on an
    undeclared port, or a declared port with no filter), and the reason names
    the defect. No zero is synthesized for a missing counter.
    """
    if not groups:
        return {}, False, 'port_groups_not_declared'
    port_group = {}
    for group in groups:
        group_id = group.get('group_id')
        if group_id not in ('discovery', 'data'):
            return {}, False, 'unknown_port_group_id'
        for port in group.get('ports', []):
            if port in port_group and port_group[port] != group_id:
                return {}, False, 'port_assigned_to_multiple_groups'
            port_group[port] = group_id
    counters = {group['group_id']: {'packets': 0, 'bytes': 0, 'filter_entries': 0,
                                    'ports_observed': [], 'ports_expected': list(group['ports']),
                                    'changed_packets': 0}
                for group in groups}
    reason = None
    for entry in entries:
        port = entry['port']
        group_id = port_group.get(port)
        if group_id is None:
            reason = 'filter_port_outside_declared_groups'
            continue
        counters[group_id]['filter_entries'] += 1
        counters[group_id]['ports_observed'].append(port)
        if 'packets' in entry and 'bytes' in entry:
            counters[group_id]['packets'] += entry['packets']
            counters[group_id]['bytes'] += entry['bytes']
            if entry['packets'] > 0:
                counters[group_id]['changed_packets'] += entry['packets']
        else:
            reason = reason or 'tc_action_counters_missing'
    for group in groups:
        observed = set(counters[group['group_id']]['ports_observed'])
        if set(group['ports']) - observed:
            reason = reason or 'declared_port_filter_missing'
    available = reason is None
    return counters, available, reason


def _parse_noqueue(qdisc_text):
    """Return residual qdisc descriptions; only a bare noqueue root is clean."""
    residual = []
    for line in str(qdisc_text).splitlines():
        line = line.strip()
        if not line:
            continue
        fields = line.split()
        if fields[:2] != ['qdisc', 'noqueue']:
            residual.append(line)
    return residual


class DiscoveryNetemController:
    """One bounded tc-netem window over derived discovery and/or data UDP ports.

    Reuses the drop controller's namespace assertion, health gate and restore
    discipline, but never installs nft rules and never reports a drop hit.
    ``spec['scope']`` chooses the matched destination ports: ``discovery``
    (default, byte-identical legacy behaviour), ``data`` (default-RTPS user-data
    ports so the primary delivery oracle can be exercised) or ``both``. Each
    port class is a separately labelled group in every receipt, so a data-plane
    match is never confused with a discovery match.

    Semantics contract (also embedded in receipts): only network-layer
    delay/reorder/duplicate is applied, with loss fixed at 0% and declared.
    These are environment-harness effects. A reliable+ordered DDS implementation
    must not expose wire reordering or duplication as reader-side order
    violations or duplicate deliveries, so the primary delivery oracle keeps
    ``delivery_order_violation`` and duplicate sequences as real defect
    signals; nothing in this adapter may reinterpret such residuals as an
    expected network effect, and candidate admission stays oracle-decided.
    A ``data``/``both`` hit therefore only proves that the harness classified
    data-plane packets; it never pre-excludes or pre-admits a candidate.
    """

    def __init__(self, domain_id, participant_count=32):
        self.ports = DiscoveryPorts(domain_id, participant_count)
        self.health = None
        self.spec = None
        self.plan = None
        self.active = False
        self._attempted = False
        self._begin = None
        self._end = None

    @property
    def neutral(self):
        return bool(self.spec and self.spec['neutral'])

    def setup(self):
        """Tool and namespace health only; installs no qdisc."""
        if self._attempted:
            raise DiscoveryNetworkError('restore before setup')
        isolation = assert_isolated_namespace()
        qdisc_payload, qdisc_command = _tc_json(['tc', '-j', 'qdisc', 'show', 'dev', 'lo'])
        filter_payload, filter_command = _tc_json(['tc', '-j', 'filter', 'show', 'dev', 'lo'])
        residual_qdisc = _parse_noqueue(
            '\n'.join(f"qdisc {entry.get('kind')} {entry.get('handle')}" for entry in qdisc_payload))
        if residual_qdisc or filter_payload:
            raise DiscoveryNetworkError('initial loopback qdisc/filter is not empty',
                                        {'residual_qdisc': residual_qdisc,
                                         'residual_filter': filter_payload})
        self.health = {
            'event': 'discovery_netem_setup', 'health_ok': True,
            'monotonic_ns': time.monotonic_ns(), 'isolation': isolation,
            'ports': self.ports.receipt(), 'implementation': NETEM_IMPLEMENTATION,
            'initial_qdisc': qdisc_payload, 'initial_filter': filter_payload,
            'qdisc_command': qdisc_command, 'filter_command': filter_command,
            'supported_scopes': list(NETEM_SCOPES),
            'default_scope': DEFAULT_NETEM_SCOPE,
            'user_data_derivation': rtps_user_data_ports(
                self.ports.domain_id, self.ports.participant_count),
            'responsibility_layer': NETEM_RESPONSIBILITY_LAYER,
            'loss_pct': NETEM_LOSS_PCT,
            'mutation_installed': False, 'hit': False, 'counter_observed': False,
        }
        return self.health

    def pair_bindings(self, worker_pids):
        """Delegate to the shared, read-only socket ownership proof."""
        return DiscoveryDropController.pair_bindings(self, worker_pids)

    def _dump(self):
        qdisc_payload, qdisc_command = _tc_json(['tc', '-j', '-s', 'qdisc', 'show', 'dev', 'lo'])
        filter_payload, filter_command = _tc_json(['tc', '-j', '-s', 'filter', 'show', 'dev', 'lo'])
        dump = {'qdisc': qdisc_payload, 'filter': filter_payload}
        digest = hashlib.sha256(json.dumps(dump, sort_keys=True).encode()).hexdigest()
        return dump, digest, {'qdisc_command': qdisc_command, 'filter_command': filter_command}

    def start(self, spec):
        """Install the qdisc/filters once; validate the full spec before any tc call."""
        if self.health is None or self._attempted:
            raise DiscoveryNetworkError('setup required and previous window must be restored')
        normalized = normalize_netem_spec(spec)
        assert_isolated_namespace()
        plan = build_netem_plan(self.ports, normalized)
        self.spec, self.plan = normalized, plan
        self._end = None
        self._begin = {
            'event': 'discovery_netem_begin', 'domain_id': self.ports.domain_id,
            'implementation': NETEM_IMPLEMENTATION,
            'netem_begin_monotonic_ns': time.monotonic_ns(),
            'spec': {k: normalized[k] for k in (*NETEM_SPEC_FIELDS, NETEM_SCOPE_FIELD)},
            'scope': plan['scope'],
            'groups': plan['groups'],
            'derivation': {'netem_args': plan['netem_args'],
                           'entry_count': len(plan['entries']),
                           'reorder_correlation_pct': 0.0,
                           'applies_to': plan['applies_to'],
                           'floor_default_class': plan['floor_default_class'],
                           'matched_ports': plan['matched_ports'],
                           'excluded_user_data_ports': plan['excluded_user_data_ports'],
                           'excluded_discovery_ports': plan['excluded_discovery_ports'],
                           'port_groups': [
                               {key: group[key] for key in
                                ('group_id', 'port_class', 'ports', 'filter_count',
                                 'filter_count_per_port', 'class_id')}
                               for group in plan['groups']],
                           'loss_pct': plan['loss_pct'],
                           'loss_defaulted': plan['loss_defaulted'],
                           'responsibility_layer': plan['responsibility_layer'],
                           'semantics_note': plan['semantics_note']},
            'responsibility_layer': NETEM_RESPONSIBILITY_LAYER,
            'loss_pct': NETEM_LOSS_PCT, 'loss_defaulted': True,
            'hit_semantics': netem_hit_semantics(plan['scope']),
            'semantics_note': NETEM_SEMANTICS_NOTE,
            'configured': False, 'installed': False, 'applied': False,
            'neutral': normalized['neutral'], 'hit': False, 'counter_observed': False,
        }
        self._attempted = True
        try:
            self._begin['install_command'] = _execute(['tc', '-batch', '-'], input_text=plan['script'])
            self.active = True
            self._begin.update(configured=True, installed=True,
                               netem_applied_monotonic_ns=time.monotonic_ns())
            self._begin['applied'] = not normalized['neutral']
            if normalized['neutral']:
                self._begin['no_op_reason'] = 'neutral_spec'
            dump, digest, commands = self._dump()
            self._begin.update(tc_dump=dump, tc_dump_sha256=digest, tc_commands=commands)
            snapshot = self.counters()
            self._begin['counter_snapshot'] = snapshot
            self._begin['counter_observed'] = True
            self._begin['hit'] = bool(snapshot['mutation_hit'])
        except (DiscoveryNetworkError, NetworkIsolationError) as exc:
            self._begin['error'] = str(exc)
            self._begin['failed_command'] = getattr(exc, 'receipt', {})
            try:
                self._begin['restore_after_failure'] = self.restore()
            except (DiscoveryNetworkError, NetworkIsolationError) as cleanup:
                self._begin['restore_error'] = str(cleanup)
                self._begin['restore_failure_receipt'] = getattr(cleanup, 'receipt', {})
            raise DiscoveryNetworkError('discovery netem installation failed', self._begin) from exc
        return self._begin

    def counters(self):
        assert_isolated_namespace()
        if not self.active:
            raise DiscoveryNetworkError('no installed netem window')
        qdisc_payload, _ = _tc_json(['tc', '-j', '-s', 'qdisc', 'show', 'dev', 'lo'])
        filter_payload, _ = _tc_json(['tc', '-j', '-s', 'filter', 'show', 'dev', 'lo'])
        groups = (self.plan or {}).get('groups')
        scope = (self.spec or {}).get(NETEM_SCOPE_FIELD, DEFAULT_NETEM_SCOPE)
        parsed = parse_netem_counters(qdisc_payload, filter_payload, neutral=self.neutral,
                                      groups=groups, scope=scope)
        expected = (self.plan or {}).get('matched_ports', list(self.ports.discovery))
        return {'event': 'discovery_netem_counter', 'monotonic_ns': time.monotonic_ns(),
                'implementation': NETEM_IMPLEMENTATION, 'domain_id': self.ports.domain_id,
                'scope': scope, 'port_groups': groups,
                'spec': self.spec, 'counter_observed': True,
                'expected_ports': list(expected), **parsed}

    def restore(self):
        isolation = assert_isolated_namespace()
        if not self._attempted:
            if self._end:
                return dict(self._end, idempotent=True)
            # Nothing was ever installed; still report real residual state.
            try:
                qdisc_payload, _ = _tc_json(['tc', '-j', 'qdisc', 'show', 'dev', 'lo'])
                filter_payload, _ = _tc_json(['tc', '-j', '-s', 'filter', 'show', 'dev', 'lo'])
                residual_qdisc = _parse_noqueue(
                    '\n'.join(f"qdisc {e.get('kind')} {e.get('handle')}" for e in qdisc_payload))
                dump = {'qdisc': qdisc_payload, 'filter': filter_payload}
                dump_sha = hashlib.sha256(json.dumps(dump, sort_keys=True).encode()).hexdigest()
            except DiscoveryNetworkError as exc:
                residual_qdisc, filter_payload, dump_sha = [], [], None
                observation_error = str(exc)
            else:
                observation_error = None
            return {
                'event': 'discovery_netem_end', 'restored': not residual_qdisc and not filter_payload,
                'no_netem_attempted': True, 'implementation': NETEM_IMPLEMENTATION, 'neutral': None,
                'residual_qdisc': residual_qdisc, 'residual_filter': filter_payload,
                'tc_dump_after_sha256': dump_sha, 'observation_error': observation_error,
                'counter_observed': False, 'recovery_verified': False, 'isolation': isolation,
            }
        end = {'event': 'discovery_netem_end', 'domain_id': self.ports.domain_id,
               'implementation': NETEM_IMPLEMENTATION, 'neutral': self.neutral,
               'netem_begin_monotonic_ns': self._begin['netem_begin_monotonic_ns'],
               'restore_begin_monotonic_ns': time.monotonic_ns(), 'isolation': isolation,
               'restored': False, 'residual_qdisc': [], 'residual_filter': [],
               'counter_observed': False, 'recovery_verified': False}
        if self.active:
            try:
                end['counter_snapshot'] = self.counters()
                end['counter_observed'] = True
            except DiscoveryNetworkError as exc:
                end.update(counter_error=str(exc), counter_failure_receipt=exc.receipt)
        try:
            end['delete_command'] = _execute(['tc', 'qdisc', 'del', 'dev', 'lo', 'root'])
        except DiscoveryNetworkError as exc:
            end.update(delete_error=str(exc), delete_failure_receipt=exc.receipt)
        try:
            qdisc_payload, qdisc_command = _tc_json(['tc', '-j', 'qdisc', 'show', 'dev', 'lo'])
            filter_payload, filter_command = _tc_json(['tc', '-j', '-s', 'filter', 'show', 'dev', 'lo'])
            residual_qdisc = _parse_noqueue(
                '\n'.join(f"qdisc {entry.get('kind')} {entry.get('handle')}" for entry in qdisc_payload))
            dump = {'qdisc': qdisc_payload, 'filter': filter_payload}
            end.update(final_qdisc=qdisc_payload, final_filter=filter_payload,
                       qdisc_command=qdisc_command, filter_command=filter_command,
                       residual_qdisc=residual_qdisc, residual_filter=filter_payload,
                       tc_dump_after_sha256=hashlib.sha256(
                           json.dumps(dump, sort_keys=True).encode()).hexdigest())
            end['only_noqueue_root'] = (
                len(qdisc_payload) == 1 and qdisc_payload[0].get('kind') == 'noqueue')
            end['restored'] = end['only_noqueue_root'] and not filter_payload
        except DiscoveryNetworkError as exc:
            end.update(verify_error=str(exc), verify_failure_receipt=exc.receipt)
        end['netem_end_monotonic_ns'] = time.monotonic_ns()
        self._end = end
        if not end['restored']:
            raise DiscoveryNetworkError('namespace netem restoration not verified', end)
        self.active = False
        self._attempted = False
        return end

    def confirm_recovery(self, *, activity_receipt):
        """Bounded post-restore gate; the caller still owns semantic adjudication."""
        assert_isolated_namespace()
        if not self._end or not self._end['restored'] or self._attempted:
            raise DiscoveryNetworkError('verified restore required before recovery confirmation')
        if not isinstance(activity_receipt, dict):
            raise DiscoveryNetworkError('independent post-restore activity receipt required')
        receipt = json.loads(json.dumps(activity_receipt, allow_nan=False))
        observed = receipt.get('monotonic_ns')
        if (receipt.get('activity_observed') is not True
                or receipt.get('domain_id') != self.ports.domain_id
                or receipt.get('source') not in ('public_ros2_pair', 'non_dds_loopback_udp')
                or not isinstance(receipt.get('evidence_ref'), str) or not receipt['evidence_ref'].strip()
                or isinstance(observed, bool) or not isinstance(observed, int)
                or not self._end['netem_end_monotonic_ns'] < observed <= time.monotonic_ns()):
            raise DiscoveryNetworkError('fresh post-restore activity and raw evidence_ref required')
        return {'event': 'discovery_netem_recovery_confirmed', 'recovery_verified': True,
                'implementation': NETEM_IMPLEMENTATION, 'neutral': self._end['neutral'],
                'evidence_kind': 'caller_independent_activity', 's1_execution': False,
                'requires_runtime_semantic_adjudication': True,
                'restore_end_monotonic_ns': self._end['netem_end_monotonic_ns'],
                'monotonic_ns': time.monotonic_ns(), 'activity_receipt': receipt}

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.restore()


def _enter(host, argv):
    # Read namespace identities before enabling lo or invoking any network tool.
    if not argv:
        raise ValueError('inner controller argv required')
    proof = {**host, 'schema_version': 1, 'net_namespace': _namespace('net'),
             'user_namespace': _namespace('user')}
    _validate_identity(proof, require_up=False)
    os.environ[_PROOF_ENV] = json.dumps(proof, sort_keys=True)
    _execute(['ip', 'link', 'set', 'dev', 'lo', 'up'], require_up=False)
    # Local multicast routes/flag are needed by default ROS2 UDP discovery.
    # No interface other than lo exists; these cannot route outside the netns.
    _execute(['ip', 'link', 'set', 'dev', 'lo', 'multicast', 'on'])
    _execute(['ip', '-4', 'route', 'add', '224.0.0.0/4', 'dev', 'lo'])
    assert_isolated_namespace()
    os.execvpe(argv[0], argv, os.environ)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['_enter'])
    parser.add_argument('host_json')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    try:
        _enter(json.loads(args.host_json), command)
    except (NetworkIsolationError, DiscoveryNetworkError, OSError, ValueError) as exc:
        print(json.dumps({'event': 'discovery_namespace_setup_failed', 'error': str(exc),
                          'receipt': getattr(exc, 'receipt', {})}), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
