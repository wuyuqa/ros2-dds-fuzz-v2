#!/usr/bin/env python3
"""Real isolated non-DDS netem carrier control; never an S1/ROS2 matrix run.

Run from any cwd: python3 -B /ABS/scripts/check_discovery_netem.py

Everything inside the namespace is a plain loopback UDP probe bound to derived
discovery ports. No RTPS bytes, ROS participant, nft rule, host route or host
interface is touched. Measured latencies, duplicate counts and arrival order
come from ``time.monotonic_ns()`` and real socket reads; nothing is synthesized.
If the environment cannot support a step the script prints ``blocked:<reason>``
and exits non-zero instead of reporting a pass.
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
import struct
import subprocess
import sys
import time
import traceback

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / 'src'))
from r2f.runtime.discovery_network import (  # noqa: E402
    DiscoveryNetemController, DiscoveryNetworkError, NetworkIsolationError,
    assert_isolated_namespace, namespace_command,
)

NEUTRAL = {'delay_ms': 0, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 5}
DELAY = {'delay_ms': 50, 'reorder_pct': 0, 'duplicate_pct': 0, 'restore_sec': 5}
DUPLICATE = {'delay_ms': 20, 'reorder_pct': 0, 'duplicate_pct': 100, 'restore_sec': 5}
REORDER = {'delay_ms': 20, 'reorder_pct': 50, 'duplicate_pct': 0, 'restore_sec': 5}


class Blocked(RuntimeError):
    """Environment cannot support this check; never a pass."""


def _receiver(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', port))
    sock.setblocking(False)
    return sock


def _drain(sock):
    try:
        while True:
            sock.recvfrom(65535)
    except BlockingIOError:
        return


def _one_way(sender, receivers, payload, timeout=3.0):
    """One datagram, one measured receive; returns monotonic-ns latency."""
    for sock in receivers:
        _drain(sock)
    ready = select.select(receivers, [], [], 0)[0]
    if ready:
        raise RuntimeError('receiver not empty before timed send')
    sent_ns = time.monotonic_ns()
    sender.sendto(payload, ('127.0.0.1', receivers[0].getsockname()[1]))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for sock in receivers:
            try:
                data, _ = sock.recvfrom(65535)
            except BlockingIOError:
                continue
            received_ns = time.monotonic_ns()
            if data != payload:
                raise RuntimeError('unexpected payload on discovery probe port')
            return received_ns - sent_ns
        select.select(receivers, [], [], 0.002)
    raise TimeoutError('timed probe did not arrive')


def _send_series(sender, port, count, spacing=0.002):
    payloads = {}
    sent_ns = []
    for seq in range(count):
        payload = b'r2f-netem-order:' + struct.pack('!I', seq)
        payloads[payload] = seq
        sender.sendto(payload, ('127.0.0.1', port))
        sent_ns.append(time.monotonic_ns())
        time.sleep(spacing)
    return payloads, sent_ns


def _collect(sock, payloads, expected, timeout=3.0):
    order = []
    deadline = time.monotonic() + timeout
    while len(order) < expected and time.monotonic() < deadline:
        select.select([sock], [], [], 0.05)
        try:
            data, _ = sock.recvfrom(65535)
        except BlockingIOError:
            continue
        seq = payloads.get(data)
        if seq is None:
            raise RuntimeError('unexpected payload during order probe')
        order.append(seq)
    return order


def _phase_delay(sender, receiver, payload):
    values = [_one_way(sender, [receiver], payload + bytes([i % 256])) for i in range(5)]
    values.sort()
    return {'runs_ns': values, 'median_ns': values[len(values) // 2],
            'min_ns': values[0], 'max_ns': values[-1]}


def _inside(domain, participant_count):
    isolation = assert_isolated_namespace()
    carrier = DiscoveryNetemController(domain, participant_count)
    port = carrier.ports.unicast[0]
    result = {
        'scope': 'carrier_control_non_dds', 's1_execution': False, 'ros2_execution': False,
        'capability': 'discovery_netem_delay_reorder_duplicate',
        'implementation': 'isolated_netns_tc_netem_discovery_udp',
        'domain_id': domain, 'isolation': isolation, 'ports': carrier.ports.receipt(),
        'probe_port': port, 'checks': {}, 'ok': False,
    }
    receiver = sender = None
    try:
        receiver = _receiver(port)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        result['setup'] = carrier.setup()

        baseline = _phase_delay(sender, receiver, b'r2f-netem-baseline:')
        result['checks']['baseline'] = baseline
        if baseline['median_ns'] >= 5_000_000:
            raise Blocked('baseline_loopback_delay_not_under_5ms')

        # (a) neutral path: a transparent class must add no measurable delay.
        begin = carrier.start(NEUTRAL)
        result['checks']['neutral_begin'] = {
            'configured': begin['configured'], 'applied': begin['applied'],
            'neutral': begin['neutral'], 'implementation': begin['implementation'],
            'tc_dump_sha256': begin['tc_dump_sha256'],
            'tc_dump': begin['tc_dump'], 'derivation': begin['derivation'],
            'spec': begin['spec']}
        try:
            neutral = _phase_delay(sender, receiver, b'r2f-netem-neutral:')
            neutral_counter = carrier.counters()
            result['checks']['neutral'] = {
                'latency': neutral, 'counter': neutral_counter,
                'added_vs_baseline_ns': neutral['median_ns'] - baseline['median_ns']}
            if begin['neutral'] is not True:
                raise RuntimeError('neutral spec did not report neutral=True')
            if begin['applied'] is not False:
                raise RuntimeError('neutral spec must not report applied=True')
            if neutral_counter['mutation_hit'] is not False:
                raise RuntimeError('neutral spec must not report a hit')
            if neutral['median_ns'] - baseline['median_ns'] >= 20_000_000:
                raise RuntimeError('neutral path added a measurable delay')
        finally:
            neutral_end = carrier.restore()
            result['checks']['neutral_restore'] = {
                'restored': neutral_end['restored'],
                'only_noqueue_root': neutral_end.get('only_noqueue_root'),
                'residual_qdisc': neutral_end['residual_qdisc'],
                'residual_filter': neutral_end['residual_filter'],
                'tc_dump_after_sha256': neutral_end['tc_dump_after_sha256']}

        # (b) 50 ms delay must be visible on the discovery destination port.
        begin = carrier.start(DELAY)
        result['checks']['delay_begin'] = {
            'applied': begin['applied'], 'neutral': begin['neutral'],
            'tc_dump_sha256': begin['tc_dump_sha256'], 'counter': begin['counter_snapshot']}
        try:
            delayed = _phase_delay(sender, receiver, b'r2f-netem-delay:')
            delay_counter = carrier.counters()
            result['checks']['delay'] = {
                'latency': delayed, 'counter': delay_counter,
                'added_vs_baseline_ns': delayed['median_ns'] - baseline['median_ns']}
            if delayed['median_ns'] < 40_000_000:
                raise RuntimeError('50ms netem delay not observed (>=40ms expected)')
            if not delay_counter['mutation_hit']:
                raise RuntimeError('delay counter saw no matched discovery packets')
            if delay_counter['classes']['1:2']['packets'] < 1:
                raise RuntimeError('netem class counter did not increase')
        finally:
            delayed_end = carrier.restore()
            result['checks']['delay_restore'] = {
                'restored': delayed_end['restored'],
                'only_noqueue_root': delayed_end.get('only_noqueue_root'),
                'residual_qdisc': delayed_end['residual_qdisc'],
                'residual_filter': delayed_end['residual_filter'],
                'tc_dump_after_sha256': delayed_end['tc_dump_after_sha256']}

        # (e) restore must return latency to <5 ms with an empty qdisc tree.
        after_restore = _phase_delay(sender, receiver, b'r2f-netem-restored:')
        result['checks']['post_restore'] = {
            'latency': after_restore, 'added_vs_baseline_ns':
                after_restore['median_ns'] - baseline['median_ns']}
        if after_restore['median_ns'] >= 5_000_000:
            raise RuntimeError('restore did not return loopback latency to <5ms')

        # (c) duplicate 100% must deliver two copies of one datagram.
        begin = carrier.start(DUPLICATE)
        result['checks']['duplicate_begin'] = {
            'applied': begin['applied'], 'tc_dump_sha256': begin['tc_dump_sha256']}
        try:
            payload = b'r2f-netem-duplicate:single'
            _drain(receiver)
            sender.sendto(payload, ('127.0.0.1', port))
            copies, deadline = 0, time.monotonic() + 1.0
            while time.monotonic() < deadline:
                select.select([receiver], [], [], 0.05)
                try:
                    data, _ = receiver.recvfrom(65535)
                except BlockingIOError:
                    continue
                if data != payload:
                    raise RuntimeError('unexpected payload during duplicate probe')
                copies += 1
                if copies >= 2:
                    break
            duplicate_counter = carrier.counters()
            result['checks']['duplicate'] = {
                'copies_received': copies, 'counter': duplicate_counter,
                'delivered_packets': duplicate_counter['classes']['1:2']['packets']}
            if copies != 2:
                raise RuntimeError('duplicate 100%% did not deliver exactly two copies')
        finally:
            duplicate_end = carrier.restore()
            result['checks']['duplicate_restore'] = {
                'restored': duplicate_end['restored'],
                'only_noqueue_root': duplicate_end.get('only_noqueue_root'),
                'residual_qdisc': duplicate_end['residual_qdisc'],
                'residual_filter': duplicate_end['residual_filter']}

        # (d) reorder must change arrival order at least once.
        begin = carrier.start(REORDER)
        result['checks']['reorder_begin'] = {
            'applied': begin['applied'], 'tc_dump_sha256': begin['tc_dump_sha256']}
        try:
            count = 40
            payloads, sent_ns = _send_series(sender, port, count)
            order = _collect(receiver, payloads, count)
            inversions = sum(1 for a, b in zip(order, order[1:]) if b < a)
            reorder_counter = carrier.counters()
            result['checks']['reorder'] = {
                'sent_count': count, 'received_count': len(order), 'order': order,
                'inversions': inversions, 'counter': reorder_counter,
                'first_send_monotonic_ns': sent_ns[0], 'last_send_monotonic_ns': sent_ns[-1]}
            if len(order) != count:
                raise RuntimeError('reorder probe lost packets with loss 0%')
            if inversions < 1:
                raise RuntimeError('reorder produced no changed arrival order')
        finally:
            reorder_end = carrier.restore()
            result['checks']['reorder_restore'] = {
                'restored': reorder_end['restored'],
                'only_noqueue_root': reorder_end.get('only_noqueue_root'),
                'residual_qdisc': reorder_end['residual_qdisc'],
                'residual_filter': reorder_end['residual_filter'],
                'tc_dump_after_sha256': reorder_end['tc_dump_after_sha256']}

        final = _phase_delay(sender, receiver, b'r2f-netem-final:')
        result['checks']['final'] = final
        if final['median_ns'] >= 5_000_000:
            raise RuntimeError('final loopback latency did not return to <5ms')
        result['ok'] = True
    except Blocked as exc:
        result.update(blocked='blocked:' + str(exc), ok=False)
    except Exception as exc:
        result.update(error=str(exc), error_type=type(exc).__name__,
                      traceback=traceback.format_exc())
        if isinstance(exc, (DiscoveryNetworkError, NetworkIsolationError)):
            result['failure_receipt'] = exc.receipt
    finally:
        if carrier._attempted:
            try:
                result['emergency_restore'] = carrier.restore()
            except Exception as exc:
                result['emergency_restore_error'] = str(exc)
                result['ok'] = False
        for sock in (receiver, sender):
            if sock is not None:
                sock.close()
    return result


def _host_snapshot():
    links = json.loads(subprocess.run(['ip', '-j', 'address', 'show'], text=True,
                                      capture_output=True, check=True, timeout=5).stdout)
    normalized_links = [{**{key: link.get(key) for key in ('ifindex', 'ifname', 'flags', 'mtu', 'address', 'operstate')},
                         'addresses': [{key: addr.get(key) for key in ('family', 'local', 'prefixlen', 'scope')}
                                       for addr in link.get('addr_info', [])]}
                        for link in links]
    routes = {}
    for family in ('-4', '-6'):
        data = json.loads(subprocess.run(['ip', '-j', family, 'route', 'show', 'table', 'all'],
                                         text=True, capture_output=True, check=True, timeout=5).stdout)
        routes[family] = [{k: v for k, v in route.items() if k not in ('expires', 'cache')} for route in data]
    structural = {'links': normalized_links, 'routes': routes}
    return {'net_namespace': os.readlink('/proc/self/ns/net'),
            'user_namespace': os.readlink('/proc/self/ns/user'),
            'uid': os.geteuid(), 'network': structural,
            'network_sha256': hashlib.sha256(json.dumps(structural, sort_keys=True).encode()).hexdigest()}


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
            DiscoveryNetemController(args.domain_id, args.participant_count).setup()
        except NetworkIsolationError as exc:
            denied = {'denied': True, 'error': str(exc)}
        else:
            raise RuntimeError('host namespace setup unexpectedly allowed')
        command = namespace_command([
            sys.executable, '-B', str(Path(__file__).resolve()), '--inside',
            '--domain-id', str(args.domain_id), '--participant-count', str(args.participant_count)])
        env = dict(os.environ)
        env['PYTHONPATH'] = str(_REPO / 'src') + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
        child = subprocess.Popen(command, env=env, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        timed_out = False
        try:
            stdout, stderr = child.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(child.pid, signal.SIGKILL)
            stdout, stderr = child.communicate(timeout=5)
        after = _host_snapshot()
        try:
            result = json.loads(stdout)
        except ValueError:
            result = {'ok': False, 'scope': 'carrier_control_non_dds', 's1_execution': False,
                      'error': 'namespace child did not return JSON', 'stdout': stdout[:4000]}
        result['namespace_process'] = {'argv': command, 'returncode': child.returncode,
                                       'stderr': stderr, 'timed_out': timed_out}
        result['host_setup_denied'] = denied
        result['host_network_unchanged'] = before == after
        result['ok'] = bool(result.get('ok') and child.returncode == 0
                            and not timed_out and before == after)
    print(json.dumps(result, sort_keys=True))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
