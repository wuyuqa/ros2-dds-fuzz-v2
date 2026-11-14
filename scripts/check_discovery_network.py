#!/usr/bin/env python3
"""Real isolated non-DDS UDP carrier control, never an S1/ROS2 matrix run.

Run from any cwd: python3 -B /ABS/scripts/check_discovery_network.py
JSON stdout contains namespace proof, exact counter snapshots and restoration.
No ROS environment, package install, native DDS participant or host rule needed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import time
import traceback

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / 'src'))
from r2f.runtime.discovery_network import (  # noqa: E402
    DiscoveryDropController, DiscoveryNetworkError, NetworkIsolationError,
    assert_isolated_namespace, namespace_command,
)


def _phase(receivers, sender, phase, blocked=()):
    """One ordinary UDP datagram per bound port. No RTPS bytes are generated."""
    payloads = {port: f'r2f-carrier-control:{phase}:{port}'.encode() for port in receivers}
    for port, payload in payloads.items():
        sender.sendto(payload, ('127.0.0.1', port))
    remaining = {sock: port for port, sock in receivers.items()}
    received, unexpected = [], []
    deadline = time.monotonic() + 0.35
    while remaining and time.monotonic() < deadline:
        readable, _, _ = select.select(list(remaining), [], [], max(0, deadline - time.monotonic()))
        if not readable:
            break
        for sock in readable:
            port = remaining.pop(sock)
            data, _ = sock.recvfrom(65535)
            if data == payloads[port]:
                received.append(port)
            else:
                unexpected.append(port)
    expected = set(receivers) - set(blocked)
    return {
        'phase': phase, 'healthy': not unexpected and set(received) == expected,
        'probe_kind': 'non_dds_loopback_udp', 'destination_address': '127.0.0.1',
        'sent_ports': sorted(receivers), 'received_ports': sorted(received),
        'blocked_ports': sorted(blocked), 'unexpected_payload_ports': unexpected,
        'expected_counter_packets': len(blocked),
        'expected_counter_ipv4_bytes': sum(28 + len(payloads[p]) for p in blocked),
        'monotonic_ns': time.monotonic_ns(),
    }


def _binding_control(carrier):
    """Exercise /proc ownership with two plain-UDP children, without fake GIDs."""
    children = {}
    if len(carrier.ports.unicast) < 2:
        raise ValueError('two-process binding control requires participant_count >= 2')
    try:
        for role, port in zip(('writer', 'reader'), carrier.ports.unicast[:2]):
            child = subprocess.Popen([
                sys.executable, '-B', '-c',
                'import json,os,socket,sys; '
                's=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); '
                's.bind(("127.0.0.1",int(sys.argv[1]))); '
                'print(json.dumps({"pid":os.getpid(),"port":s.getsockname()[1]}),flush=True); '
                'sys.stdin.readline()', str(port)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            children[role] = child
            readable, _, _ = select.select([child.stdout], [], [], 5)
            if not readable or not child.stdout.readline().strip():
                raise RuntimeError('plain-UDP binding child did not become ready')
        proof = carrier.pair_bindings({role: child.pid for role, child in children.items()})
        return {'scope': 'carrier_control_non_dds', 'ros2_match_established': False,
                'endpoint_gids': None, 'proof': proof}
    finally:
        for child in children.values():
            if child.stdin:
                child.stdin.close()
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)
            child.stdout.close()
            child.stderr.close()


def _inside(domain, participant_count):
    # Guard before even binding sockets: --inside on host must fail closed.
    isolation = assert_isolated_namespace()
    carrier = DiscoveryDropController(domain, participant_count)
    ports = carrier.ports
    adjacent = []
    for other in (domain - 1, domain + 1):
        if 0 <= other <= 232:
            adjacent.extend([7400 + 250 * other, 7410 + 250 * other])
    all_ports = sorted(set((*ports.discovery, *ports.user_data, *adjacent)))
    receivers = {}
    sender = None
    result = {
        'scope': 'carrier_control_non_dds', 's1_execution': False, 'ros2_execution': False,
        'domain_id': domain, 'isolation': isolation, 'ports': ports.receipt(),
        'adjacent_domain_probe_ports': adjacent, 'ok': False,
    }
    try:
        result['setup'] = carrier.setup()
        result['binding_control'] = _binding_control(carrier)
        for port in all_ports:
            receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            receivers[port] = receiver
            receiver.bind(('127.0.0.1', port))
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        result['health'] = _phase(receivers, sender, 'health')
        if not result['health']['healthy']:
            raise RuntimeError('initial loopback UDP health failed')
        # Prove spawned controller children inherit this exact namespace.
        child = subprocess.run(
            [sys.executable, '-B', '-c',
             'import json; from r2f.runtime.discovery_network import assert_isolated_namespace; '
             'print(json.dumps(assert_isolated_namespace()))'],
            text=True, capture_output=True, timeout=5, check=True,
        )
        result['child_isolation'] = json.loads(child.stdout)
        if result['child_isolation']['observed_net_namespace'] != isolation['observed_net_namespace']:
            raise RuntimeError('child did not inherit controller namespace')
        result['begin'] = carrier.start_control(health_receipt={
            'domain_id': domain, 'healthy': True, 'probe_kind': 'non_dds_loopback_udp',
            'evidence_ref': 'control:health',
        })
        try:
            result['drop'] = _phase(receivers, sender, 'drop', ports.discovery)
            result['counter'] = carrier.counters()
            if not result['drop']['healthy']:
                raise RuntimeError('exact UDP drop/transparency check failed')
            if result['counter']['packets'] != result['drop']['expected_counter_packets']:
                raise RuntimeError('nft packet counter differs from sent discovery-port probes')
            if result['counter']['bytes'] != result['drop']['expected_counter_ipv4_bytes']:
                raise RuntimeError('nft byte counter differs from sent IPv4 datagram bytes')
        finally:
            result['restore'] = carrier.restore()
        result['recovery'] = _phase(receivers, sender, 'recovery')
        if not result['restore']['restored'] or not result['recovery']['healthy']:
            raise RuntimeError('restore or post-restore UDP recovery failed')
        result['recovery_confirmation'] = carrier.confirm_recovery(activity_receipt={
            'domain_id': domain, 'activity_observed': True, 'source': 'non_dds_loopback_udp',
            'evidence_ref': 'control:recovery', 'monotonic_ns': result['recovery']['monotonic_ns'],
        })
        result['ok'] = True
    except Exception as exc:
        result.update(error=str(exc), error_type=type(exc).__name__, traceback=traceback.format_exc())
        if isinstance(exc, DiscoveryNetworkError):
            result['failure_receipt'] = exc.receipt
    finally:
        if carrier._attempted:
            try:
                result['emergency_restore'] = carrier.restore()
            except Exception as exc:
                result['emergency_restore_error'] = str(exc)
                result['ok'] = False
        for receiver in receivers.values():
            receiver.close()
        if sender is not None:
            sender.close()
    return result


def _host_snapshot():
    """Read-only structural fingerprint; omits volatile counters/lifetimes."""
    links = json.loads(subprocess.run(['ip', '-j', 'address', 'show'], text=True,
                                      capture_output=True, check=True, timeout=5).stdout)
    normalized_links = []
    for link in links:
        normalized_links.append({
            **{key: link.get(key) for key in ('ifindex', 'ifname', 'flags', 'mtu', 'address', 'operstate')},
            'addresses': [{key: addr.get(key) for key in ('family', 'local', 'prefixlen', 'scope')}
                          for addr in link.get('addr_info', [])],
        })
    routes = {}
    for family in ('-4', '-6'):
        data = json.loads(subprocess.run(['ip', '-j', family, 'route', 'show', 'table', 'all'],
                                         text=True, capture_output=True, check=True, timeout=5).stdout)
        routes[family] = [{k: v for k, v in route.items() if k not in ('expires', 'cache')} for route in data]
    structural = {'links': normalized_links, 'routes': routes}
    return {
        'net_namespace': os.readlink('/proc/self/ns/net'),
        'user_namespace': os.readlink('/proc/self/ns/user'),
        'uid': os.geteuid(), 'network': structural,
        'network_sha256': hashlib.sha256(json.dumps(structural, sort_keys=True).encode()).hexdigest(),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--domain-id', type=int, default=173)
    parser.add_argument('--participant-count', type=int, default=32)
    parser.add_argument('--inside', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.inside:
        try:
            result = _inside(args.domain_id, args.participant_count)
        except Exception as exc:
            result = {'ok': False, 'scope': 'carrier_control_non_dds', 's1_execution': False,
                      'error': str(exc), 'error_type': type(exc).__name__}
    else:
        before = _host_snapshot()
        try:
            DiscoveryDropController(args.domain_id, args.participant_count).setup()
        except NetworkIsolationError as exc:
            denied = {'denied': True, 'error': str(exc)}
        else:
            raise RuntimeError('host namespace setup unexpectedly allowed')
        command = namespace_command([
            sys.executable, '-B', str(Path(__file__).resolve()), '--inside',
            '--domain-id', str(args.domain_id), '--participant-count', str(args.participant_count),
        ])
        env = dict(os.environ)
        env['PYTHONPATH'] = str(_REPO / 'src') + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
        child = subprocess.Popen(command, env=env, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        timed_out = False
        try:
            stdout, stderr = child.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            timed_out = True
            # Only the new process group allocated above can be killed here.
            os.killpg(child.pid, signal.SIGKILL)
            stdout, stderr = child.communicate(timeout=5)
        after = _host_snapshot()
        try:
            result = json.loads(stdout)
        except ValueError:
            result = {'ok': False, 'scope': 'carrier_control_non_dds', 's1_execution': False,
                      'error': 'namespace child did not return JSON', 'stdout': stdout}
        result['namespace_process'] = {'argv': command, 'returncode': child.returncode,
                                       'stderr': stderr, 'timed_out': timed_out}
        result['host_setup_denied'] = denied
        result['host_before'] = before
        result['host_after'] = after
        result['host_network_unchanged'] = before == after
        result['ok'] = bool(result.get('ok') and child.returncode == 0 and not timed_out and before == after)
    print(json.dumps(result, sort_keys=True))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
