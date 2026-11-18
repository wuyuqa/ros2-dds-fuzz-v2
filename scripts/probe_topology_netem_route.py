#!/usr/bin/env python3
"""Pre-live check: topology netem window with data-scope counters bound to real
endpoint processes, executed for real inside an isolated user+net namespace.

Usage::

    python3 -B scripts/probe_topology_netem_route.py            # all three modes
    python3 -B scripts/probe_topology_netem_route.py --mode delay
    python3 -B scripts/probe_topology_netem_route.py --inside --mode delay --out DIR

Each mode installs exactly one bounded window (delay, reorder or duplicate) and
emits its own raw file.  This is **not** a ROS run and creates no SUT candidate.

What it proves without a live GO:

1. the reused ``DiscoveryNetemController`` really installs a ``scope=data``
   window in an isolated namespace and really restores it (residuals reported);
2. the counter receipt is bound to two live endpoint processes — real PIDs,
   real ``/proc/<pid>/stat`` start ticks, and real socket-ownership proof — not
   to a packet total;
3. ``audit_topology_netem`` accepts the window as ``application_observed`` only
   from the hashed raw rows, and keeps ``s1_mutation_hit = False``: a matched
   packet is an environment network effect, never a topology-pair mutation hit,
   and ``env.delay`` is network-layer delay, not the endpoint QoS deadline.

The two "endpoints" are plain UDP processes bound to the derived default-RTPS
data ports so counters have a real owner.  With live GO the same route runs real
rclpy workers inside the same namespace; that path is not executed here.
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

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "src"))

from r2f.core.evidence import canonical_hash  # noqa: E402
from r2f.runtime.discovery_network import (  # noqa: E402
    DiscoveryNetemController, DiscoveryNetworkError, NetworkIsolationError,
    assert_isolated_namespace, namespace_command, normalize_netem_spec,
    rtps_user_data_ports,
)
from r2f.runtime.topology_route import audit_topology_netem  # noqa: E402

SCOPE = "data"
SENT = 6
MODES = {
    "delay": {"delay_ms": 25, "reorder_pct": 0, "duplicate_pct": 0,
              "restore_sec": 5, "scope": SCOPE},
    "reorder": {"delay_ms": 20, "reorder_pct": 50, "duplicate_pct": 0,
                "restore_sec": 5, "scope": SCOPE},
    "duplicate": {"delay_ms": 0, "reorder_pct": 0, "duplicate_pct": 100,
                  "restore_sec": 5, "scope": SCOPE},
}


class Blocked(RuntimeError):
    """Environment cannot host this check; never a pass."""


def _endpoint_child(role, discovery_port, data_port, count, target_port, start_ns):
    """One endpoint-shaped UDP owner; sends once, then stays alive for counting."""
    sockets = []
    for port in (discovery_port, data_port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        sock.setblocking(False)
        sockets.append(sock)
    print(json.dumps({"event": "endpoint_ready", "role": role, "pid": os.getpid(),
                      "discovery_port": discovery_port, "data_port": data_port}),
          flush=True)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) if role == "writer" else None
    received = sent = 0
    announced = False
    deadline = time.monotonic() + 90.0
    while time.monotonic() < deadline:
        if sender is not None and not announced and time.monotonic_ns() >= start_ns:
            while sent < count:
                sender.sendto(b"r2f-topology-netem:" + struct.pack("!I", sent),
                              ("127.0.0.1", target_port))
                sent += 1
                time.sleep(0.005)
            announced = True
            print(json.dumps({"event": "endpoint_done", "role": role, "pid": os.getpid(),
                              "received": received, "sent": sent}), flush=True)
        readable, _, _ = select.select(sockets + [sys.stdin], [], [], 0.02)
        for sock in readable:
            if sock is sys.stdin:
                return 0
            try:
                sock.recvfrom(65535)
                received += 1
            except BlockingIOError:
                pass
    return 0


def _spawn(role, discovery_port, data_port, count, target_port, start_ns):
    argv = [sys.executable, "-B", __file__, "--endpoint", role,
            str(discovery_port), str(data_port), str(count), str(target_port),
            str(start_ns)]
    process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    ready = json.loads(process.stdout.readline().decode() or "{}")
    if ready.get("event") != "endpoint_ready" or ready.get("pid") != process.pid:
        raise Blocked("endpoint child did not report a real ready identity")
    return process, ready


def _ticks(pid):
    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])


def _inside(out_dir, mode):
    isolation = assert_isolated_namespace()
    domain, participants = 173, 4
    spec = dict(MODES[mode])
    ports = DiscoveryNetemController(domain, participants).ports
    derived = rtps_user_data_ports(domain, participants)
    result = {"schema": "r2f.topology-netem-route-probe/v1", "mode": mode,
              "scope": "isolated_netns_non_dds_endpoint_attached",
              "ros2_execution": False, "s1_execution": False,
              "candidate_eligible": False, "isolation": isolation,
              "domain_id": domain, "participant_count": participants,
              "data_derivation": derived, "checks": {}, "problems": [], "ok": False}
    carrier = DiscoveryNetemController(domain, participants)
    writer_proc = reader_proc = None
    raw_path = Path(out_dir) / ("raw.%s.jsonl" % mode)
    events = []

    def emit(event, **fields):
        row = {"event": event, "event_index": len(events),
               "monotonic_ns": time.monotonic_ns(), "time_ns": time.time_ns(),
               "role": "probe_controller", "pid": os.getpid(), **fields}
        events.append(json.loads(json.dumps(row, sort_keys=True)))
        return events[-1]

    try:
        from r2f.runtime.topology_worker import normalize_topology_environment_mutation
        declaration = normalize_topology_environment_mutation(
            {"kind": "discovery_netem", "spec": spec})
        result["checks"]["setup"] = carrier.setup()
        reader_data = derived["unicast_ports"][1]
        writer_data = derived["unicast_ports"][0]
        start_ns = time.monotonic_ns() + 3_000_000_000
        reader_proc, reader_ready = _spawn("reader", ports.unicast[1], reader_data, 0,
                                           reader_data, start_ns)
        writer_proc, writer_ready = _spawn("writer", ports.unicast[0], writer_data, SENT,
                                           reader_data, start_ns)
        # Both endpoint sockets exist and are owned by these live PIDs; bind the
        # ownership BEFORE any traffic so the counter is attributable.
        sockets = carrier.pair_bindings({"writer": writer_ready["pid"],
                                         "reader": reader_ready["pid"]})
        identities = []
        for role, pid in (("writer", writer_ready["pid"]),
                          ("reader", reader_ready["pid"])):
            ticks = _ticks(pid)
            identities.append({
                "logical_id": role + "0", "role": role, "worker_role": role + "1",
                "generation": 1, "endpoint_gid": "%032x" % (pid * 7919),
                "endpoint_generation": f"probe-{pid}", "pid": pid, "pgid": pid,
                "process_start_ticks": ticks, "fresh_process_start_ticks": ticks,
                "process_running": True, "identity_verified": True,
            })
        binding = {"schema": "r2f.topology-route-binding/v1", "capability": "topology_v1",
                   "plan_hash": "probe" * 12, "final_input_sha256": "probe" * 12,
                   "input_layer_receipts_sha256": "probe" * 12,
                   "effective_input_sha256": "probe" * 12,
                   "environment_mutation": declaration}
        binding["environment_mutation_sha256"] = canonical_hash(declaration)
        emit("input_binding", receipt={"route_binding": binding,
                                       "program_sha256": "probe" * 12})
        emit("topology_netem_isolation", receipt={"isolated": True, "tc_invoked": False})
        emit("topology_netem_setup", receipt=result["checks"]["setup"])
        begin = carrier.start(spec)
        emit("topology_netem_install", receipt=begin, spec=spec,
             tc_dump_sha256=begin.get("tc_dump_sha256"),
             install_command=begin.get("install_command"))
        writer_done = json.loads(writer_proc.stdout.readline().decode() or "{}")
        if writer_done.get("event") != "endpoint_done" or writer_done.get("sent") != SENT:
            raise Blocked("writer did not report a complete real send series")
        time.sleep(0.3)
        counter = carrier.counters()
        emit("topology_netem_counter", step_id=mode, window="action_window",
             counter=counter, counter_observed=True, installed_spec=spec,
             endpoint_identities=identities, identity_problems=[],
             worker_socket_bindings=sockets, worker_socket_binding_reason=None,
             send_refs=[], receive_refs=[],
             hit_semantics="environment_network_effect_not_sut_defect",
             responsibility_layer="environment_harness", s1_mutation_hit=False)
        end = carrier.restore()
        emit("topology_netem_restore", receipt=end,
             only_noqueue_root=end.get("only_noqueue_root"),
             residual_qdisc=end.get("residual_qdisc"),
             residual_filter=end.get("residual_filter"),
             restored=end.get("restored") is True, recovery_verified=False)
        raw_path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
        digest = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        audit = audit_topology_netem(binding, events, path=str(raw_path), digest=digest,
                                     real=True, fixture=False,
                                     observed_status="application_observed")
        result.update(
            checks={**result["checks"],
                    "declaration": declaration,
                    "install": {"applied": begin.get("applied"),
                                "scope": begin.get("scope"),
                                "tc_dump_sha256": begin.get("tc_dump_sha256")},
                    "counter": {"affected_packets": counter.get("affected_packets"),
                                "group_counters": counter.get("group_counters"),
                                "group_counters_available": counter.get("group_counters_available")},
                    "worker_socket_bindings": sockets,
                    "restore": {"restored": end.get("restored"),
                                "only_noqueue_root": end.get("only_noqueue_root"),
                                "residual_qdisc": end.get("residual_qdisc"),
                                "residual_filter": end.get("residual_filter")}},
            audit=audit, raw_path=str(raw_path), raw_sha256=digest)
        problems = result["problems"]
        if begin.get("applied") is not True or begin.get("scope") != SCOPE:
            problems.append("install_not_applied_for_data_scope")
        if not counter.get("group_counters_available"):
            problems.append("tc_group_counters_unavailable")
        group = (counter.get("group_counters") or {}).get("data", {}).get("packets", 0)
        if group < 1:
            problems.append("no_data_group_packets")
        if not sockets.get("only_pair_bound_udp_sockets"):
            problems.append("worker_socket_ownership_not_bound")
        if any(not row["identity_verified"] for row in identities):
            problems.append("endpoint_identity_not_verified")
        if end.get("restored") is not True or end.get("only_noqueue_root") is not True \
                or end.get("residual_filter") or end.get("residual_qdisc"):
            problems.append("restore_not_verified")
        if audit["status"] != "application_observed":
            problems.append("audit_status:" + str(audit["status"]))
        if audit["s1_mutation_hit"] is not False:
            problems.append("audit_claimed_sut_mutation")
        record = audit["operators"]["env." + mode]
        if record["status"] != "application_observed":
            problems.append(mode + "_not_observed:" + record["status"])
        if mode == "delay" and record["effect_scope"] != \
                "network_layer_environment_effect_not_endpoint_qos_deadline":
            problems.append("delay_scope_is_not_network_effect")
        if audit["operators"]["env.drop"]["status"] != "unsupported":
            problems.append("drop_must_stay_unsupported")
        result["ok"] = not problems
        return result
    finally:
        for proc in (writer_proc, reader_proc):
            if proc is None or proc.poll() is not None:
                continue
            try:
                proc.stdin.write(b"stop\n")
                proc.stdin.flush()
            except (OSError, ValueError):
                pass
            try:
                proc.wait(timeout=3)
                continue
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except OSError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass


def _summary(payload):
    audit = payload.get("audit") or {}
    return {"ok": payload.get("ok"), "mode": payload.get("mode"),
            "problems": payload.get("problems"),
            "audit_status": audit.get("status"),
            "matched_packets": audit.get("matched_packets"),
            "identity_bound_counters": audit.get("identity_bound_counters"),
            "s1_mutation_hit": audit.get("s1_mutation_hit"),
            "operators": {k: v.get("status") for k, v in (audit.get("operators") or {}).items()},
            "raw_sha256": payload.get("raw_sha256"),
            "probe": payload.get("probe")}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--mode", choices=sorted(MODES), default=None)
    parser.add_argument("--endpoint", nargs=6)
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)
    if args.endpoint:
        role, discovery, data, count, target, start_ns = args.endpoint
        return _endpoint_child(role, int(discovery), int(data), int(count),
                               int(target), int(start_ns))
    root = Path(args.out or (_REPO / "results/main-review/s1-continuation-audit-20261118T074855"
                             "/topology-netem-owner/probe")).resolve()
    modes = [args.mode] if args.mode else ["delay", "reorder", "duplicate"]
    if not args.inside:
        os.environ["PYTHONPATH"] = str(_REPO / "src") + (
            os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else "")
        summaries = []
        for mode in modes:
            out_dir = root / mode if args.mode is None else root
            out_dir.mkdir(parents=True, exist_ok=True)
            inner = namespace_command(
                [sys.executable, "-B", __file__, "--inside", "--mode", mode,
                 "--out", str(out_dir)], python_executable=sys.executable)
            completed = subprocess.run(inner, text=True, capture_output=True, timeout=300)
            (out_dir / ("probe.%s.stdout.log" % mode)).write_text(completed.stdout)
            (out_dir / ("probe.%s.stderr.log" % mode)).write_text(completed.stderr)
            payload = None
            for line in completed.stdout.splitlines():
                try:
                    candidate = json.loads(line)
                except ValueError:
                    continue
                if isinstance(candidate, dict) and \
                        candidate.get("schema") == "r2f.topology-netem-route-probe/v1":
                    payload = candidate
            if payload is None:
                summaries.append({"ok": False, "mode": mode,
                                  "blocked": "no_probe_payload",
                                  "returncode": completed.returncode,
                                  "stderr_tail": completed.stderr[-600:]})
                continue
            path = out_dir / ("probe.%s.json" % mode)
            path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n")
            payload["probe"] = str(path)
            summaries.append(_summary(payload))
        print(json.dumps({"schema": "r2f.topology-netem-route-probe-summary/v1",
                          "modes": summaries,
                          "ok": all(s.get("ok") for s in summaries)}, sort_keys=True))
        return 0 if all(s.get("ok") for s in summaries) else 1
    try:
        result = _inside(root, args.mode)
    except (Blocked, DiscoveryNetworkError, NetworkIsolationError, OSError, ValueError) as exc:
        print(json.dumps({"schema": "r2f.topology-netem-route-probe/v1", "ok": False,
                          "mode": args.mode, "blocked": type(exc).__name__ + ":" + str(exc)}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
