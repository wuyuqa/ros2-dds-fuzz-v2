"""Read-only measurement protocol unit tests; no namespace/tool/ROS execution."""
from copy import deepcopy
import json
import time
from types import SimpleNamespace

import pytest

from r2f.runtime import discovery_network
from r2f.runtime import topology_environment_observation as observation
from r2f.runtime.topology_worker import OfflineTopologyBackend, RosTopologyBackend


def command_readbacks():
    payloads = [[{"kind": "noqueue", "handle": "0:", "root": True}], [],
                {"nftables": [{"metainfo": {"json_schema_version": 1}}]}]
    now = time.monotonic_ns()
    return [{"argv": list(argv), "returncode": 0, "stdout": json.dumps(payload),
             "stderr": "", "started_monotonic_ns": now, "ended_monotonic_ns": now + 1,
             "net_namespace": "net:[91]"}
            for argv, payload in zip(observation._COMMANDS, payloads)]


def namespace():
    return {"isolated": True, "observed_net_namespace": "net:[91]",
            "observed_user_namespace": "user:[92]", "uid_map": [[0, 1000, 1]], "euid": 0}


class FixtureProbe:
    """Explicit non-DDS seam; injected probes cannot be used by real backends."""
    def __init__(self, *, failure=None, commands=None, foreign=False):
        self.failure = failure
        self.commands = commands
        self.foreign = foreign
        self.calls = []

    def namespace(self):
        self.calls.append("namespace")
        if self.failure == "namespace":
            raise discovery_network.NetworkIsolationError("fixture_missing_kernel_proof")
        return namespace()

    def read_network(self):
        self.calls.append("network")
        if self.failure == "network":
            raise observation.ObservationUnavailable("fixture_command_failed", {"returncode": 2})
        return deepcopy(self.commands) if self.commands is not None else command_readbacks()

    def inventory(self, backend, handles, ns):
        self.calls.append("inventory")
        if self.failure == "inventory" or self.foreign:
            raise observation.ObservationUnavailable("fixture_foreign_or_incomplete_inventory")
        workers = [{key: h[key] for key in observation._IDENTITY} for h in handles.values()]
        return {"complete": True, "empty": not workers, "workers": workers,
                "controller": {"pid": 42, "process_start_ticks": 9,
                               "namespaces": {"net": "net:[91]"}},
                "source": "explicit_offline_fixture_inventory"}


def emitter(events):
    def emit(kind, **fields):
        row = {"event": kind, "event_index": len(events), "monotonic_ns": time.monotonic_ns(),
               "run_id": "fixture", **deepcopy(fields)}
        events.append(row)
        return deepcopy(row)
    return emit


def test_neutral_audit_requires_all_three_actual_readbacks():
    result = observation.audit_neutral_readback(command_readbacks())
    assert result["audit_neutral"] and len(result["readback_sha256"]) == 64
    assert result["commands"][2]["argv"] == ["nft", "-j", "list", "ruleset"]


@pytest.mark.parametrize("index,payload,reason", [
    (0, [], "qdisc_not_exclusively_noqueue"),
    (0, [{"kind": "netem", "handle": "0:", "root": True}], "qdisc_not_exclusively_noqueue"),
    (0, [{"kind": "noqueue", "handle": "0:"}], "qdisc_not_exclusively_noqueue"),
    (1, [{"kind": "u32"}], "residual_tc_filter"),
    (2, {"nftables": [{"chain": {"policy": "accept"}}]}, "nft_ruleset_not_empty"),
    (2, {"nftables": [{"table": {"name": "unused"}}]}, "nft_ruleset_not_empty"),
    (2, {"nftables": [{"rule": {"packets": 0}}]}, "nft_ruleset_not_empty"),
    (2, {}, "nft_ruleset_not_empty"),
])
def test_residuals_and_missing_reads_stay_unknown(index, payload, reason):
    rows = command_readbacks()
    rows[index]["stdout"] = json.dumps(payload)
    with pytest.raises(observation.ObservationUnavailable, match=reason):
        observation.audit_neutral_readback(rows)


@pytest.mark.parametrize("defect", ["returncode", "argv", "clock", "duplicate_json", "nan", "missing", "namespace"])
def test_malformed_or_failed_command_is_not_neutral(defect):
    rows = command_readbacks()
    if defect == "returncode":
        rows[0]["returncode"] = 1
    elif defect == "argv":
        rows[0]["argv"] = ["echo", "noqueue"]
    elif defect == "clock":
        rows[0]["ended_monotonic_ns"] = 0
    elif defect == "duplicate_json":
        rows[2]["stdout"] = '{"nftables":[],"nftables":[]}'
    elif defect == "nan":
        rows[2]["stdout"] = '{"nftables":[{"metainfo":{"x":NaN}}]}'
    elif defect == "missing":
        rows.pop()
    else:
        rows[0]["net_namespace"] = "caller_claimed"
    with pytest.raises(observation.ObservationUnavailable):
        observation.audit_neutral_readback(rows)


def test_host_and_boundary_flags_cannot_enable_network_commands(monkeypatch):
    monkeypatch.delenv("R2F_DISCOVERY_NETNS_PROOF", raising=False)
    monkeypatch.setenv("R2F_EXECUTION_BOUNDARY", "true")
    monkeypatch.setattr(discovery_network, "_execute", lambda *a, **k: pytest.fail("network tool called"))
    backend = SimpleNamespace(real_ros=True)
    events = []
    result = observation.TopologyEnvironmentObservation(backend, events, emitter(events)).observe("before", {}, {})
    assert not result["network_neutral_verified"] and not result["inventory_complete"]
    assert events[-2]["observed"] is False and events[-2]["network_phase"] == "unknown"
    assert "missing or invalid unshare wrapper proof" in events[0]["failures"][0]["reason"]


def test_explicit_fixture_probe_cannot_be_promoted_to_live():
    with pytest.raises(ValueError, match="offline_only"):
        observation.TopologyEnvironmentObservation(SimpleNamespace(real_ros=True), [], None,
                                                    _probe=FixtureProbe())


def test_default_offline_run_does_not_manufacture_network_evidence():
    events = []
    result = observation.TopologyEnvironmentObservation(OfflineTopologyBackend(), events, emitter(events)).observe("before", {}, {})
    assert not events and not result["empty_inventory_verified"]


@pytest.mark.parametrize("failure", ["namespace", "network", "inventory"])
def test_components_fail_independently_and_keep_raw_failure(failure):
    events, probe = [], FixtureProbe(failure=failure)
    result = observation.TopologyEnvironmentObservation(OfflineTopologyBackend(), events, emitter(events), _probe=probe).observe("before", {}, {})
    assert events[0]["failures"]
    assert all(e["fixture"] and not e["candidate_eligible"] and not e["execution_evidence"] for e in events)
    if failure == "namespace":
        assert probe.calls == ["namespace"]
        assert not result["inventory_complete"] and not result["network_neutral_verified"]
    elif failure == "network":
        assert result["empty_inventory_verified"] and not result["network_neutral_verified"]
    else:
        assert result["network_neutral_verified"] and not result["empty_inventory_verified"]


def test_mutated_network_is_not_normal_or_restored_even_after_rule_removal():
    events, probe = [], FixtureProbe()
    result = observation.TopologyEnvironmentObservation(OfflineTopologyBackend(), events, emitter(events), _probe=probe).observe("after", {}, {}, network_mutated=True)
    assert not result["network_neutral_verified"] and "network" not in probe.calls
    assert events[-1]["network_phase"] == "unknown"
    assert not any(e.get("network_phase") == "restored_verified" for e in events)


def test_fresh_failure_revokes_prior_normal_for_projector():
    events, probe = [], FixtureProbe()
    observer = observation.TopologyEnvironmentObservation(OfflineTopologyBackend(), events, emitter(events), _probe=probe)
    assert observer.observe("before", {}, {})["network_neutral_verified"]
    probe.failure = "network"
    assert not observer.observe("after", {}, {})["network_neutral_verified"]
    assert events[-1]["network_phase"] == "unknown"
    assert events[events[-1]["carrier_ref"]]["observed"] is False


def _fake_process(pid):
    return SimpleNamespace(process=SimpleNamespace(pid=pid, poll=lambda: None))


def _kernel_inventory_fixture(monkeypatch):
    backend = RosTopologyBackend({}, "/tmp/offline-no-process-started")
    ns = {"net": "net:[91]", "user": "user:[92]", "pid": "pid:[93]"}
    controller = {"pid": 42, "process_start_ticks": 9, "namespaces": ns}
    owned = _fake_process(43)
    handle = {"logical_id": "w0", "generation": 1, "endpoint_gid": "aa",
              "pid": 43, "process_start_ticks": 10, "endpoint_generation": "wg",
              "alive": True, "process": owned}
    backend.processes, backend.handles = [owned], {"w0": handle}
    rows = [controller, {"pid": 43, "process_start_ticks": 10, "namespaces": ns}]
    monkeypatch.setattr(observation.os, "getpid", lambda: 42)
    monkeypatch.setattr(observation, "_process_identity", lambda pid: deepcopy(next(r for r in rows if r["pid"] == pid)))
    monkeypatch.setattr(observation, "_netns_members", lambda netns: deepcopy(rows))
    return backend, handle, rows


def test_kernel_inventory_checks_backend_process_objects_not_caller_boolean(monkeypatch):
    backend, handle, rows = _kernel_inventory_fixture(monkeypatch)
    proof = observation._KernelProbe().inventory(backend, {"w0": handle}, namespace())
    assert proof["complete"] and not proof["empty"]
    assert proof["workers"][0]["namespaces"] == rows[0]["namespaces"]


@pytest.mark.parametrize("defect", ["foreign_process", "extra_worker", "start_ticks", "worker_pid", "netns", "unbound_handle"])
def test_kernel_inventory_refuses_ambiguity(monkeypatch, defect):
    backend, handle, rows = _kernel_inventory_fixture(monkeypatch)
    handles = {"w0": handle}
    if defect == "foreign_process":
        rows.append({"pid": 44, "process_start_ticks": 11, "namespaces": rows[0]["namespaces"]})
    elif defect == "extra_worker":
        backend.processes.append(_fake_process(44))
    elif defect == "start_ticks":
        handle["process_start_ticks"] = 99
    elif defect == "worker_pid":
        handle["pid"] = 44
    elif defect == "netns":
        rows[1]["namespaces"] = {**rows[1]["namespaces"], "net": "net:[99]"}
    else:
        handles["w0"] = dict(handle)
    with pytest.raises(observation.ObservationUnavailable):
        observation._KernelProbe().inventory(backend, handles, namespace())


def test_network_read_failure_retains_completed_and_failed_commands(monkeypatch):
    calls = []
    def execute(argv):
        calls.append(argv)
        if len(calls) == 2:
            raise discovery_network.DiscoveryNetworkError("fixture tc error", {"argv": argv, "returncode": 2})
        return command_readbacks()[0]
    monkeypatch.setattr(discovery_network, "_execute", execute)
    with pytest.raises(observation.ObservationUnavailable) as failure:
        observation._KernelProbe().read_network()
    assert len(failure.value.receipt["commands"]) == 1
    assert failure.value.receipt["failed_command"]["returncode"] == 2
    assert len(calls) == 2


@pytest.mark.parametrize("euid,uidmap", [(1000, "1000 1000 1\n"), (0, "0 999 1\n")])
def test_outer_boundary_uid_mapping_cannot_satisfy_discovery_assert(monkeypatch, euid, uidmap):
    proof = {"schema_version": 1, "host_uid": 1000, "host_cap_eff": 0,
             "host_net_namespace": "net:[1]", "host_user_namespace": "user:[2]",
             "net_namespace": "net:[91]", "user_namespace": "user:[92]"}
    monkeypatch.setenv("R2F_DISCOVERY_NETNS_PROOF", json.dumps(proof))
    monkeypatch.setattr(discovery_network, "_namespace", lambda name: proof[name + "_namespace"])
    monkeypatch.setattr(discovery_network.os, "geteuid", lambda: euid)
    original = observation.Path.read_text
    monkeypatch.setattr(observation.Path, "read_text", lambda path, *a, **k:
                        uidmap if str(path) == "/proc/self/uid_map" else original(path, *a, **k))
    monkeypatch.setattr(discovery_network, "_execute", lambda *a, **k: pytest.fail("network tool called"))
    events = []
    result = observation.TopologyEnvironmentObservation(SimpleNamespace(real_ros=True), events, emitter(events)).observe("before", {}, {})
    assert not result["network_neutral_verified"] and not result["inventory_complete"]
    assert "not the expected single-user root mapping" in events[0]["failures"][0]["reason"]


def test_empty_owned_backend_inventory_is_measured_not_assumed(monkeypatch):
    backend, handle, rows = _kernel_inventory_fixture(monkeypatch)
    backend.handles, backend.processes = {}, []
    rows.pop()
    measured = observation._KernelProbe().inventory(backend, {}, namespace())
    assert measured["complete"] and measured["empty"] and measured["workers"] == []
    rows.append({"pid": 44, "process_start_ticks": 12, "namespaces": rows[0]["namespaces"]})
    with pytest.raises(observation.ObservationUnavailable, match="foreign_namespace_process"):
        observation._KernelProbe().inventory(backend, {}, namespace())


def test_caller_inventory_booleans_do_not_substitute_for_backend_objects():
    with pytest.raises(observation.ObservationUnavailable, match="backend_owned_inventory_unavailable"):
        observation._KernelProbe().inventory(SimpleNamespace(inventory_complete=True, handles={}), {}, namespace())


def test_namespace_changing_during_measurement_revokes_network_and_inventory():
    class ChangingProbe(FixtureProbe):
        def namespace(self):
            self.calls.append("namespace")
            proof = namespace()
            if self.calls.count("namespace") > 1:
                proof["observed_net_namespace"] = "net:[99]"
            return proof
    events = []
    measured = observation.TopologyEnvironmentObservation(OfflineTopologyBackend(), events, emitter(events),
                                                         _probe=ChangingProbe()).observe("before", {}, {})
    assert not measured["network_neutral_verified"] and not measured["inventory_complete"]
    assert events[0]["namespace"] and events[0]["failures"][-1]["component"] == "namespace_after"


def test_namespace_readback_binding_must_match_proven_namespace():
    commands = command_readbacks()
    commands[1]["net_namespace"] = "net:[99]"
    events = []
    measured = observation.TopologyEnvironmentObservation(OfflineTopologyBackend(), events, emitter(events),
        _probe=FixtureProbe(commands=commands)).observe("before", {}, {})
    assert not measured["network_neutral_verified"]
    assert "network_readback_namespace_mismatch" in events[0]["failures"][0]["reason"]
    assert measured["empty_inventory_verified"]


def test_visible_proc_unreadable_or_racing_member_blocks_inventory(monkeypatch):
    entries = [observation.Path("/fixture-proc/1"), observation.Path("/fixture-proc/2")]
    monkeypatch.setattr(observation.Path, "iterdir", lambda path: entries)
    def readlink(path):
        if "/2/" in str(path):
            raise PermissionError("fixture_unreadable_namespace")
        return "net:[91]"
    monkeypatch.setattr(observation.os, "readlink", readlink)
    monkeypatch.setattr(observation, "_process_identity", lambda pid: {"pid": pid})
    with pytest.raises(observation.ObservationUnavailable, match="visible_proc_inventory_incomplete") as failure:
        observation._netns_members("net:[91]")
    assert failure.value.receipt["unavailable"][0]["pid"] == 2
    assert failure.value.receipt["members"] == [{"pid": 1}]
