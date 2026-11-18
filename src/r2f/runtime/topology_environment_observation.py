"""Measured, read-only N-state inputs for the canonical topology carrier.

No runner, namespace launcher, verdict or mutation lives here.  In particular
an execution-boundary environment flag is NOT discovery isolation.  Only the
existing discovery wrapper's *kernel-revalidated* proof permits network reads
or a named 1W1R control.  Missing measurements stay unknown.  An explicit
injected probe is an offline test seam and can never qualify live evidence.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import time

from . import discovery_network as network

IMPLEMENTATION = "isolated_netns_readonly_tc_nft_inventory/v1"
_IDENTITY = ("logical_id", "generation", "endpoint_gid", "pid",
             "process_start_ticks", "endpoint_generation")
_COMMANDS = (
    ["tc", "-j", "qdisc", "show", "dev", "lo"],
    ["tc", "-j", "filter", "show", "dev", "lo"],
    ["nft", "-j", "list", "ruleset"],
)


class ObservationUnavailable(RuntimeError):
    def __init__(self, reason, receipt=None):
        super().__init__(reason)
        self.receipt = receipt or {}


def _gid(value):
    if not isinstance(value, str) or not value:
        return None
    value = value.lower()
    return value[:32] if len(value) == 48 and value[32:] == "0" * 16 else value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_network_json_key:" + key)
        result[key] = value
    return result


def _constant(value):
    raise ValueError("nonfinite_network_json:" + value)


def audit_neutral_readback(commands):
    """Strictly audit the captured readbacks, not a declaration of no mutation."""
    if not isinstance(commands, list) or len(commands) != len(_COMMANDS):
        raise ObservationUnavailable("network_readbacks_missing", {"commands": commands})
    payloads = []
    try:
        for argv, row in zip(_COMMANDS, commands):
            if (row.get("argv") != argv or row.get("returncode") != 0
                    or not isinstance(row.get("stdout"), str)
                    or type(row.get("started_monotonic_ns")) is not int
                    or type(row.get("ended_monotonic_ns")) is not int
                    or not 0 < row["started_monotonic_ns"] <= row["ended_monotonic_ns"]
                    or not re.fullmatch(r"net:\[[0-9]+\]", row.get("net_namespace", ""))):
                raise ValueError("network_command_identity_or_result_invalid")
            payloads.append(json.loads(row["stdout"], object_pairs_hook=_object,
                                       parse_constant=_constant))
        qdiscs, filters, nft = payloads
        if (not isinstance(qdiscs, list) or len(qdiscs) != 1
                or not isinstance(qdiscs[0], dict) or qdiscs[0].get("kind") != "noqueue"
                or qdiscs[0].get("root") is not True or qdiscs[0].get("handle") != "0:"):
            raise ValueError("qdisc_not_exclusively_noqueue")
        if filters != []:
            raise ValueError("residual_tc_filter")
        if (not isinstance(nft, dict) or set(nft) != {"nftables"}
                or not isinstance(nft["nftables"], list)
                or any(not isinstance(row, dict) or set(row) != {"metainfo"}
                       or not isinstance(row["metainfo"], dict) for row in nft["nftables"])):
            # Even an empty table/chain is conservatively unknown.  There is no
            # inference from a zero counter or from our own absence of rules.
            raise ValueError("nft_ruleset_not_empty")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ObservationUnavailable(str(exc), {"commands": commands}) from exc
    return {"audit_neutral": True, "qdisc": qdiscs, "filters": filters,
            "nftables": nft["nftables"], "commands": deepcopy(commands),
            "readback_sha256": _digest(commands)}


def _process_identity(pid):
    """Kernel values only; no cmdline, environment, or caller-supplied PID map."""
    def stat():
        text = Path(f"/proc/{pid}/stat").read_text()
        fields = text.rsplit(")", 1)[1].split()
        if fields[0] in {"Z", "X"}:
            raise ObservationUnavailable("process_not_live:" + str(pid))
        return int(fields[19])
    first = stat()
    ns = {name: os.readlink(f"/proc/{pid}/ns/{name}") for name in ("net", "user", "pid")}
    if first != stat():
        raise ObservationUnavailable("process_start_ticks_changed:" + str(pid))
    return {"pid": pid, "process_start_ticks": first, "namespaces": ns}


def _netns_members(netns):
    """Conservative visible-/proc inventory; unreadable or racing rows block it.

    This is NOT host clearance.  The namespace must independently pass the
    discovery wrapper gate.  We don't hide processes, inspect arguments, or
    treat aggregate graph counts as a process/peer inventory.
    """
    members, unavailable = [], []
    for entry in sorted(Path("/proc").iterdir(), key=lambda p: p.name):
        if not entry.name.isdecimal():
            continue
        pid = int(entry.name)
        try:
            if os.readlink(entry / "ns/net") == netns:
                members.append(_process_identity(pid))
        except OSError as exc:
            unavailable.append({"pid": pid, "error": type(exc).__name__, "errno": exc.errno})
        except (ValueError, IndexError, ObservationUnavailable) as exc:
            unavailable.append({"pid": pid, "error": str(exc)})
    if unavailable:
        raise ObservationUnavailable("visible_proc_inventory_incomplete", {"unavailable": unavailable,
                                                                               "members": members})
    return sorted(members, key=lambda row: row["pid"])


class _KernelProbe:
    def namespace(self):
        # This verifies euid0, the actual single-root UID map, independent user
        # and network namespaces, lo-only inventory and CAP_NET_ADMIN each time.
        return network.assert_isolated_namespace()

    def read_network(self):
        commands = []
        try:
            for argv in _COMMANDS:
                commands.append(network._execute(argv))
        except (network.DiscoveryNetworkError, network.NetworkIsolationError, OSError) as exc:
            raise ObservationUnavailable("network_readback_failed:" + str(exc), {
                "commands": commands, "failed_command": getattr(exc, "receipt", {})}) from exc
        return commands

    def inventory(self, backend, handles, namespace):
        # Only the backend's actual process objects supply the managed set.
        # Config/caller `inventory_complete=True` is never an input here.
        from .topology_worker import RosTopologyBackend
        if not isinstance(backend, RosTopologyBackend):
            raise ObservationUnavailable("backend_owned_inventory_unavailable")
        managed = {eid: handle for eid, handle in backend.handles.items() if handle.get("alive") is True}
        if set(managed) != set(handles) or any(
                managed[eid] is not handles[eid] for eid in handles):
            raise ObservationUnavailable("backend_handle_inventory_differs_from_controller")
        live_processes = []
        for owned in backend.processes:
            process = owned.process
            if process is None:
                raise ObservationUnavailable("backend_process_inventory_incomplete")
            if process.poll() is None:
                live_processes.append(owned)
        if len(live_processes) != len(handles) or {id(p) for p in live_processes} != {
                id(handle.get("process")) for handle in handles.values()}:
            raise ObservationUnavailable("unbound_or_extra_live_backend_worker")
        controller = _process_identity(os.getpid())
        netns, userns = namespace["observed_net_namespace"], namespace["observed_user_namespace"]
        if (controller["namespaces"]["net"] != netns or controller["namespaces"]["user"] != userns):
            raise ObservationUnavailable("controller_namespace_mismatch")
        bindings = []
        for eid, handle in sorted(handles.items()):
            process = handle["process"].process
            if type(handle.get("pid")) is not int or handle["pid"] != process.pid:
                raise ObservationUnavailable("backend_worker_pid_mismatch:" + eid)
            actual = _process_identity(process.pid)
            if (actual["process_start_ticks"] != handle.get("process_start_ticks")
                    or actual["namespaces"] != controller["namespaces"]
                    or not all(handle.get(key) is not None for key in _IDENTITY)):
                raise ObservationUnavailable("backend_worker_kernel_identity_mismatch:" + eid)
            bindings.append({**{key: handle[key] for key in _IDENTITY},
                             "namespaces": actual["namespaces"]})
        members = _netns_members(netns)
        expected = {controller["pid"], *(row["pid"] for row in bindings)}
        if len(members) != len(expected) or {row["pid"] for row in members} != expected:
            raise ObservationUnavailable("foreign_namespace_process", {"members": members,
                                                                          "expected_pids": sorted(expected)})
        for row in [controller, *bindings]:
            observed = next(m for m in members if m["pid"] == row["pid"])
            if (observed["process_start_ticks"] != row["process_start_ticks"]
                    or observed["namespaces"] != row["namespaces"]):
                raise ObservationUnavailable("namespace_inventory_identity_changed")
        # Recheck backend process liveness after the /proc scan. No PID equality
        # is ever interpreted as a host/inner PID translation.
        if any(owned.process.poll() is not None for owned in live_processes):
            raise ObservationUnavailable("backend_worker_exited_during_inventory")
        return {"complete": True, "empty": not handles, "controller": controller,
                "workers": bindings, "members": members,
                "source": "backend_process_objects_and_kernel_visible_proc_netns",
                "scope": "current_exclusive_discovery_netns_visible_processes_not_host_clearance"}


class TopologyEnvironmentObservation:
    def __init__(self, backend, events, emit, *, _probe=None):
        if _probe is not None and backend.real_ros:
            raise ValueError("injected_observation_probe_is_offline_only")
        self.backend, self.events, self.emit = backend, events, emit
        self.fixture = _probe is not None or not backend.real_ros
        self.enabled = backend.real_ros or _probe is not None
        self.probe = _probe or _KernelProbe()

    def observe(self, phase, handles, chains, *, network_mutated=False):
        """Emit additive raw records at the actual before/after measurement point."""
        result = {"network_neutral_verified": False, "inventory_complete": False,
                  "empty_inventory_verified": False, "isolation_ref": None,
                  "measurement_ref": None, "limitations": []}
        if not self.enabled:
            result["limitations"] = ["offline_backend_has_no_environment_measurements"]
            return result
        namespace, readback, inventory, failures = None, None, None, []
        began = time.monotonic_ns()
        try:
            namespace = self.probe.namespace()
        except (network.NetworkIsolationError, OSError, RuntimeError, ValueError, TypeError) as exc:
            failures.append({"component": "namespace", "reason": str(exc),
                             "receipt": getattr(exc, "receipt", {})})
        if namespace is not None:
            if network_mutated:
                failures.append({"component": "network", "reason": "mutated_or_recovery_network_not_a_neutral_audit"})
            else:
                try:
                    commands = self.probe.read_network()
                    readback = audit_neutral_readback(commands)
                    if any(row["net_namespace"] != namespace["observed_net_namespace"] for row in commands):
                        raise ObservationUnavailable("network_readback_namespace_mismatch", readback)
                except (OSError, RuntimeError, ValueError, TypeError, KeyError) as exc:
                    readback = None
                    failures.append({"component": "network", "reason": str(exc),
                                     "receipt": getattr(exc, "receipt", {})})
            try:
                inventory = self.probe.inventory(self.backend, handles, namespace)
            except (OSError, RuntimeError, ValueError, TypeError, KeyError, AttributeError) as exc:
                failures.append({"component": "inventory", "reason": str(exc),
                                 "receipt": getattr(exc, "receipt", {})})
            try:
                after = self.probe.namespace()
                if any(after.get(key) != namespace.get(key) for key in
                       ("observed_net_namespace", "observed_user_namespace", "uid_map", "euid")):
                    raise ObservationUnavailable("namespace_changed_during_measurement")
            except (OSError, RuntimeError, ValueError, TypeError) as exc:
                readback, inventory = None, None
                failures.append({"component": "namespace_after", "reason": str(exc)})
        inventory_ok = bool(namespace is not None and inventory and inventory.get("complete") is True)
        neutral = readback is not None
        measurement = self.emit(
            "topology_environment_measurement", phase=phase, implementation=IMPLEMENTATION,
            measured_begin_monotonic_ns=began, namespace=namespace, network=readback,
            inventory=inventory, failures=failures, inventory_complete=inventory_ok,
            audit_neutral=neutral, fixture=self.fixture, candidate_eligible=False,
            execution_evidence=not self.fixture)
        result.update(network_neutral_verified=neutral, inventory_complete=inventory_ok,
                      empty_inventory_verified=inventory_ok and inventory.get("empty") is True and not handles,
                      measurement_ref=measurement["event_index"],
                      limitations=[f["component"] + ":" + f["reason"] for f in failures])
        # A failed current read invalidates a prior normal phase.  Never leave a
        # stale positive carrier record as the latest state input after failure.
        carrier = self.emit("topology_network_phase", network_phase="normal" if neutral else "unknown",
                            recovery_cycle=0, observed=neutral, audit_neutral=neutral,
                            implementation=IMPLEMENTATION, measurement_ref=measurement["event_index"],
                            fixture=self.fixture, execution_evidence=not self.fixture,
                            s1_mutation_hit=False, candidate_eligible=False)
        self.emit("topology_network_observation", network_phase=carrier["network_phase"],
                  recovery_cycle=0, source="carrier_audit", carrier_ref=carrier["event_index"],
                  measurement_ref=measurement["event_index"], fixture=self.fixture,
                  execution_evidence=not self.fixture, candidate_eligible=False)
        if inventory_ok and len(handles) == 2:
            try:
                proof = self._single_pair(handles, chains, inventory, namespace)
            except (ObservationUnavailable, KeyError, TypeError, ValueError) as exc:
                result["limitations"].append("controlled_isolation:" + str(exc))
                self.emit("topology_controlled_isolation_unavailable", phase=phase,
                          measurement_ref=measurement["event_index"], reason=str(exc),
                          fixture=self.fixture, candidate_eligible=False, execution_evidence=False)
            else:
                event = self.emit("topology_controlled_isolation", **proof,
                                  inventory_ref=measurement["event_index"], fixture=self.fixture,
                                  execution_evidence=not self.fixture, candidate_eligible=False)
                result["isolation_ref"] = event["event_index"]
        elif handles:
            result["limitations"].append("controlled_isolation_requires_exact_1w1r_inventory")
        return result

    def _single_pair(self, handles, chains, inventory, namespace):
        writers = [h for h in handles.values() if h.get("role") == "writer"]
        readers = [h for h in handles.values() if h.get("role") == "reader"]
        if len(writers) != 1 or len(readers) != 1 or len(inventory["workers"]) != 2:
            raise ObservationUnavailable("not_exactly_one_writer_and_reader")
        w, r = writers[0], readers[0]
        if w.get("topic") != r.get("topic") or w.get("type_name") != r.get("type_name"):
            raise ObservationUnavailable("isolated_pair_topic_or_type_differs")
        graph_refs, snapshot_refs = [], []
        for token in (w, r):
            eid = token["logical_id"]
            binding = [b for b in inventory["workers"] if b.get("logical_id") == eid]
            if len(binding) != 1 or any(binding[0].get(k) != token.get(k) for k in _IDENTITY):
                raise ObservationUnavailable("inventory_worker_generation_mismatch:" + eid)
            chain = chains.get(eid, ())
            if len(chain) != 4:
                raise ObservationUnavailable("fresh_snapshot_chain_missing:" + eid)
            if [e["event"] for e in chain] != ["topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"]:
                raise ObservationUnavailable("fresh_snapshot_chain_kinds_invalid:" + eid)
            refs = [e["event_index"] for e in chain]
            if refs != sorted(set(refs)) or any(self.events[i] != e for i, e in zip(refs, chain)):
                raise ObservationUnavailable("snapshot_refs_not_raw_backed:" + eid)
            if any(any(e.get(k) != token.get(k) for k in _IDENTITY if k != "endpoint_gid") for e in chain):
                raise ObservationUnavailable("snapshot_worker_generation_mismatch:" + eid)
            request, match, graph, ack = chain
            command_id = request.get("command_id")
            if (command_id is None or request.get("command") != "snapshot"
                    or any(row.get("command_id") != command_id for row in chain)
                    or _gid(match.get("endpoint_gid")) != _gid(token.get("endpoint_gid"))
                    or any(type(row.get("monotonic_ns")) is not int for row in chain)
                    or not all(a["monotonic_ns"] <= b["monotonic_ns"] for a, b in zip(chain, chain[1:]))
                    or ack["monotonic_ns"] > time.monotonic_ns()):
                raise ObservationUnavailable("snapshot_command_or_clock_invalid:" + eid)
            related = [e for e in self.events if e.get("logical_id") == eid
                       and e.get("generation") == token["generation"] and e.get("command_id") == command_id]
            if any(len([e for e in related if e.get("event") == row["event"]]) != 1 for row in chain):
                raise ObservationUnavailable("snapshot_command_reply_duplicate:" + eid)
            created = [e for e in self.events if e.get("event") == "endpoint_created"
                       and all(e.get(k) == token.get(k) for k in _IDENTITY)]
            if (len(created) != 1 or created[0]["event_index"] >= request["event_index"]
                    or created[0]["monotonic_ns"] > request["monotonic_ns"]
                    or any(e.get("run_id") != created[0].get("run_id") for e in chain)):
                raise ObservationUnavailable("snapshot_not_fresh_after_current_creation:" + eid)
            if (type(match.get("matched_count")) is not int or match["matched_count"] not in (0, 1)):
                raise ObservationUnavailable("isolated_count_not_zero_or_one:" + eid)
            for role, expected in (("publishers", w), ("subscriptions", r)):
                rows = graph.get(role)
                if (not isinstance(rows, list) or len(rows) != 1
                        or _gid(rows[0].get("endpoint_gid")) != _gid(expected.get("endpoint_gid"))
                        or rows[0].get("topic_type") != expected.get("type_name")):
                    raise ObservationUnavailable("graph_inventory_not_exact_1w1r:" + eid)
            graph_refs.append(graph["event_index"])
            snapshot_refs.extend(refs)
        return {"writer": w["logical_id"], "reader": r["logical_id"],
                "writer_generation": w["generation"], "reader_generation": r["generation"],
                "writer_gid": w["endpoint_gid"], "reader_gid": r["endpoint_gid"],
                "isolation_id": _digest({"namespace": namespace["observed_net_namespace"],
                                          "controller": inventory["controller"]}),
                "mechanism": "kernel_netns_backend_inventory_and_exact_public_graph_1w1r",
                "inventory_complete": True, "graph_refs": graph_refs, "snapshot_refs": snapshot_refs,
                "worker_kernel_bindings": inventory["workers"],
                "match_proof_scope": "current_command_snapshots_only_not_delivery_or_persistent_match"}
