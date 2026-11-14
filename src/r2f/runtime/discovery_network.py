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
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import json
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
