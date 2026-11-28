"""Offline worker JSON/transport contracts, never ROS/DDS execution evidence.

External ROS, pipe, QoS readback and observer boundaries are explicit fixtures.
The real worker command handler, backend snapshot collection and pair projector
run unchanged. No shared libraries, processes, sockets or campaigns are started.
"""
from contextlib import redirect_stdout
from copy import deepcopy
import ctypes
import io
import hashlib
from pathlib import Path
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from r2f.runtime import topology_worker as worker
from r2f.runtime.topology_pair import pair_graph_facts


UNAVAILABLE = "rmw_has_no_public_peer_observer"
QOS = dict(history="keep_last", depth=16, reliability="reliable",
           durability="volatile", deadline_ns=worker.INFINITE_NS,
           liveliness="automatic", liveliness_lease_duration_ns=worker.INFINITE_NS)
GIDS = {"writer": "1" * 32, "reader": "2" * 32}


@pytest.fixture
def snapshot_worker(monkeypatch):
    """Run the public worker entry with authored external API/pipe responses."""
    def launch(kind, *, rmw="rmw_fastrtps_cpp", extra_peer=False, count=1,
               observer_case=None, count_after=None, observe_native_peers=None, expected_code=0):
        names = ["rclpy", "rclpy.context", "rclpy.executors", "rclpy.node",
                 "rclpy.qos", "rclpy.duration", "rclpy.event_handler",
                 "std_msgs", "std_msgs.msg", "r2f.runtime.actual_qos",
                 "r2f.runtime.rcl_take_bridge", "r2f.runtime.rmw_publish_observer"]
        modules = {name: ModuleType(name) for name in names}
        modules["rclpy"].init = lambda **_: None
        modules["rclpy"].get_rmw_implementation_identifier = lambda: rmw
        modules["rclpy.context"].Context = lambda: SimpleNamespace(
            get_domain_id=lambda: 196, shutdown=lambda: None)
        modules["rclpy.executors"].SingleThreadedExecutor = lambda **_: SimpleNamespace(
            add_node=lambda _: None, spin_once=lambda **_: None, shutdown=lambda: None)
        policy = lambda name: SimpleNamespace(name=name)
        for name, members in {
                "HistoryPolicy": ["KEEP_LAST"],
                "ReliabilityPolicy": ["RELIABLE", "BEST_EFFORT"],
                "DurabilityPolicy": ["VOLATILE", "TRANSIENT_LOCAL"],
                "LivelinessPolicy": ["AUTOMATIC", "MANUAL_BY_TOPIC"]}.items():
            setattr(modules["rclpy.qos"], name,
                    SimpleNamespace(**{member: policy(member) for member in members}))
        modules["rclpy.qos"].QoSProfile = lambda **kwargs: SimpleNamespace(**kwargs)
        modules["rclpy.duration"].Duration = lambda **kwargs: SimpleNamespace(**kwargs)
        from topology_ros_message_fixture import add_generated_message_fixtures
        add_generated_message_fixtures(modules)
        modules["r2f.runtime.actual_qos"].read_public_endpoint_qos = lambda *_: {
            "qos": deepcopy(QOS), "status": "fixture_not_actual_readback",
            "fixture": True, "execution_evidence": False}

        class BridgeUnavailable(Exception):
            capability_receipt = {"status": "unavailable", "reason": "offline_fixture"}

        def no_take_bridge(**_):
            if observer_case is not None:
                return SimpleNamespace(add_node=lambda _: None, spin_once=lambda **_: None,
                    shutdown=lambda: None, take_bridge=SimpleNamespace(
                        abi_receipt={"fixture": True}, close=lambda: None))
            raise BridgeUnavailable()

        modules["r2f.runtime.rcl_take_bridge"].BridgeUnavailable = BridgeUnavailable
        modules["r2f.runtime.rcl_take_bridge"].make_full_info_executor = no_take_bridge
        modules["r2f.runtime.rmw_publish_observer"].PublishOccurrenceObserver = (
            lambda *_: SimpleNamespace(close=lambda: {"fixture": True}))

        snapshot_reads = 0

        def matched_count():
            nonlocal snapshot_reads
            if snapshot_reads:
                snapshot_reads += 1
            return count_after if count_after is not None and snapshot_reads > 2 else count

        def commands(*_):
            nonlocal snapshot_reads
            snapshot_reads = 1
            return b'{"command":"snapshot","command_id":1}\n{"command":"stop"}\n'

        class Node:
            def __init__(self, name, **_):
                self.name = name

            def create_publisher(self, message_type, topic, qos, **_):
                self.qos = qos
                return SimpleNamespace(topic_name=topic, get_subscription_count=matched_count)

            def create_subscription(self, message_type, topic, callback, qos, **_):
                self.qos = qos
                return SimpleNamespace(topic_name=topic, get_publisher_count=matched_count)

            def infos(self, role):
                info = SimpleNamespace(
                    node_name=(self.name if role == kind else
                               "r2f_topo_" + ("w0" if role == "writer" else "r0")
                               + "_g1_" + ("a" if role == "writer" else "b") * 8),
                    node_namespace="/", topic_type="std_msgs/msg/String",
                    endpoint_gid=bytes.fromhex(GIDS[role]), qos_profile=self.qos)
                rows = [info]
                if extra_peer and role == "writer":
                    rows.append(SimpleNamespace(**{**vars(info), "node_name": "offline_extra",
                                                 "endpoint_gid": bytes.fromhex("3" * 32)}))
                return rows

            def get_publishers_info_by_topic(self, _):
                return self.infos("writer")

            def get_subscriptions_info_by_topic(self, _):
                return self.infos("reader")

            def destroy_node(self):
                pass

        modules["rclpy.node"].Node = Node
        config = dict(run_id="offline-peer-absence-fixture", domain_id=196,
                      logical_id="w0" if kind == "writer" else "r0", generation=1,
                      topic="/offline/peer-contract", type_name="std_msgs/msg/String",
                      qos=deepcopy(QOS), observation_mode="blackbox")
        if observer_case is not None and rmw == "rmw_fastrtps_cpp":
            config.update(rmw=rmw, observation_mode="instrumentation",
                          fastdds_peer_instrumentation={
                              "manifest_path": "/offline/peer-manifest.json",
                              "manifest_sha256": "a" * 64})
        if observe_native_peers is not None:
            config["observe_native_peers"] = observe_native_peers
        stdout = io.StringIO()
        with monkeypatch.context() as patch:
            for name, module in modules.items():
                patch.setitem(sys.modules, name, module)
            # The pipe/selector and process identity are transport boundary fakes.
            patch.setattr(sys, "argv", ["offline-worker", json.dumps(config), kind])
            patch.setattr(sys, "stdin", SimpleNamespace(fileno=lambda: 12345))
            patch.setattr("select.select", lambda *_: ([sys.stdin], [], []))
            patch.setattr(worker.os, "read", commands)
            patch.setattr(worker.os, "getpid", lambda: 101 if kind == "writer" else 102)
            # Coherent cross-worker public graph names at the randomness boundary.
            patch.setattr(worker.uuid, "uuid4", lambda: SimpleNamespace(
                hex=("a" if kind == "writer" else "b") * 32))
            # No fixture may accidentally load ROS/vendor libraries.
            def no_native_load(*_, **__):
                raise AssertionError("offline fixture must not load native libraries")
            patch.setattr(ctypes, "CDLL", no_native_load)
            # Mapping is an external observation boundary, never a real mapping.
            from r2f.instrumentation.fastdds_peer import activation
            patch.setattr(activation, "verify_vendor_mapping", lambda *_: {"fixture": True})
            if observer_case is not None:
                # Authored manifest/observer responses substitute external build
                # files and native APIs, not projection/worker decision logic.
                manifest = {"binaries": {"provider": {"fixture": True},
                                         "vendor": {"fixture": True}},
                            "provider_source_sha256": "f" * 64,
                            "patch": {"abi": {"fixture": True}}}
                patch.setattr(activation, "manifest_for_reference", lambda _: manifest)
                if observer_case == "pdp_init_error":
                    manifest["pdp_trace"] = {"fixture": True}
                    from r2f.instrumentation.fastdds_pdp_trace import observer as pdp
                    def fail_pdp(*_):
                        raise ValueError("offline_pdp_initialization_failed")
                    patch.setattr(pdp, "PdpLifecycleObserver", fail_pdp)
                schema = "r2f.fastdds-native-matched-peer-observation/v1"
                class Observer:
                    SCHEMA = schema

                    def __init__(self, *_, **__):
                        if observer_case in ("constructor_error", "constructor_absence_text"):
                            raise ValueError(UNAVAILABLE if observer_case.endswith("absence_text")
                                             else "offline_constructor_failed")

                    def sample(self, domain, gid, role):
                        if observer_case in ("sample_error", "sample_absence_text"):
                            raise ValueError(UNAVAILABLE if observer_case.endswith("absence_text")
                                             else "offline_sample_failed")
                        if observer_case == "missing":
                            return {}
                        return {"schema": schema, "domain_id": domain, "local_gid": gid,
                                "role_kind": role, "complete": observer_case == "complete",
                                "matched_peer_gids": [GIDS["reader" if role == "writer" else "writer"]],
                                "reason": "offline_capture_diagnostic",
                                "native_result": {"fixture": True, "copied": 1}}
                if rmw == "rmw_fastrtps_cpp":
                    module = ModuleType("r2f.runtime.fastdds_peer_observer")
                    module.FastddsMatchedPeerObserver = Observer
                    patch.setitem(sys.modules, module.__name__, module)
                else:
                    patch.setattr(worker, "CycloneMatchedPeerObserver", Observer)
            with redirect_stdout(stdout):
                code = worker.worker_main()
        rows = [json.loads(line) for line in stdout.getvalue().splitlines()]
        assert code == expected_code, rows
        return rows
    return launch


class SnapshotTransport:
    """Pre-recorded offline worker pipe replies; no worker process is spawned."""
    def __init__(self, rows):
        self.events = rows

    def command(self, command):
        assert command == "snapshot"
        return 1

    def has(self, event, **fields):
        return any(row.get("event") == event and all(row.get(k) == v for k, v in fields.items())
                   for row in self.events)

    def wait(self, predicate, timeout, label):
        if not predicate():
            raise worker.TopologyBackendTimeout("offline_transport_missing_ACK:" + label)


def project_pair(tmp_path, writer_rows, reader_rows):
    """Use the existing public backend snapshot and pair projection seams."""
    handles = {}
    for kind, rows in (("writer", writer_rows), ("reader", reader_rows)):
        created = next(row for row in rows if row["event"] == "endpoint_created")
        handles[kind] = {**created, "process": SnapshotTransport(rows)}
    backend = worker.RosTopologyBackend({}, tmp_path)
    # snapshot() consumes supplied transports only; prepare()/create() never run.
    result = backend.snapshot(handles)
    return pair_graph_facts(handles["writer"], handles["reader"],
                            result["snapshots"]["writer"], result["snapshots"]["reader"],
                            real_ros=False)


def test_blackbox_absence_keeps_named_diagnostic_and_qualified_snapshot_fallback(
        snapshot_worker, tmp_path):
    writer_rows, reader_rows = [snapshot_worker(kind) for kind in ("writer", "reader")]
    fact = project_pair(tmp_path, writer_rows, reader_rows)
    assert fact["status"] == "matched", fact
    assert fact["identity_basis"] == "unique_bidirectional_graph_and_counts"
    assert fact["limitations"] == ["not_independent_per_peer_match_ack",
                                   "snapshot_scope_only_not_delivery_window"]
    assert not fact["real_ros"] and not fact["execution_evidence"] and not fact["candidate_eligible"]
    for rows in (writer_rows, reader_rows):
        match = next(row for row in rows if row["event"] == "match_snapshot")
        assert match["peer_observation_limitation"] == UNAVAILABLE
        assert (match["public_count_before"], match["public_count_after"], match["matched_count"]) == (1, 1, 1)
        assert all(key not in match for key in
                   ("native_peer_observation", "peer_gids_complete",
                    "peer_proof_source", "matched_peer_gids"))


@pytest.mark.parametrize("extra_peer", ["writer", "reader", "both"])
def test_absent_api_does_not_turn_N_graph_counts_into_pair_proof(
        snapshot_worker, tmp_path, extra_peer):
    rows = [snapshot_worker(kind, extra_peer=extra_peer in (kind, "both"))
            for kind in ("writer", "reader")]
    fact = project_pair(tmp_path, *rows)
    assert fact["status"] == "unknown"
    assert fact["reasons"] == ["aggregate_counts_do_not_identify_directed_pair"]


@pytest.mark.parametrize("kind", ["writer", "reader"])
@pytest.mark.parametrize("count", [0, 2])
def test_absence_fallback_still_requires_both_counts_one(snapshot_worker, tmp_path, kind, count):
    rows = [snapshot_worker(role, count=count if role == kind else 1)
            for role in ("writer", "reader")]
    fact = project_pair(tmp_path, *rows)
    assert fact["status"] == "unknown"
    assert fact["reasons"] == ["single_pair_requires_both_counts_one"]


@pytest.mark.parametrize("kind", ["writer", "reader"])
def test_worker_snapshot_without_command_ACK_cannot_be_projected(snapshot_worker, tmp_path, kind):
    rows = [snapshot_worker(role) for role in ("writer", "reader")]
    index = 0 if kind == "writer" else 1
    # Explicit transport loss, not a modification to any persisted actual raw.
    rows[index] = [row for row in rows[index] if row["event"] != "command_done"]
    with pytest.raises(worker.TopologyBackendTimeout, match="offline_transport_missing_ACK"):
        project_pair(tmp_path, *rows)


@pytest.mark.parametrize("damage", ["command", "generation"])
@pytest.mark.parametrize("kind", ["writer", "reader"])
def test_stale_snapshot_cannot_establish_current_pair(snapshot_worker, tmp_path, kind, damage):
    rows = [snapshot_worker(role) for role in ("writer", "reader")]
    selected = rows[0 if kind == "writer" else 1]
    if damage == "command":
        match = next(row for row in selected if row["event"] == "match_snapshot")
        match["command_id"] = 0
    else:
        # A successor endpoint must have a fresh generation AND GID. Its old
        # snapshot cannot be used to establish the successor's match.
        created = next(row for row in selected if row["event"] == "endpoint_created")
        created.update(generation=2, endpoint_gid="9" * 32, endpoint_generation="successor")
    fact = project_pair(tmp_path, *rows)
    assert fact["status"] == "unknown"
    assert fact["reasons"] == (["incomplete_snapshot"] if damage == "command" else
                                [kind + "_snapshot_gid_mismatch"])


@pytest.mark.parametrize("kind", ["writer", "reader"])
@pytest.mark.parametrize("rmw", ["rmw_fastrtps_cpp", "rmw_cyclonedds_cpp"])
@pytest.mark.parametrize("case", ["missing", "incomplete", "constructor_error",
                                  "constructor_absence_text", "sample_error", "sample_absence_text"])
def test_selected_observer_failures_never_use_absence_fallback(
        snapshot_worker, tmp_path, kind, rmw, case):
    rows = [snapshot_worker(role, rmw=rmw if role == kind else "rmw_fastrtps_cpp",
                            observer_case=case if role == kind else None)
            for role in ("writer", "reader")]
    selected = rows[0 if kind == "writer" else 1]
    match = next(row for row in selected if row["event"] == "match_snapshot")
    assert match["peer_gids_complete"] is False
    assert "peer_proof_source" not in match
    if case == "missing":
        assert match["native_peer_observation"] == {}
    elif case == "incomplete":
        observation = match["native_peer_observation"]
        assert observation["complete"] is False
        assert observation["matched_peer_gids"] == [GIDS["reader" if kind == "writer" else "writer"]]
        assert observation["native_result"] == {"fixture": True, "copied": 1}
    else:
        assert match["peer_observation_limitation"] == (
            UNAVAILABLE if case.endswith("absence_text") else
            "offline_constructor_failed" if case.startswith("constructor") else "offline_sample_failed")
    fact = project_pair(tmp_path, *rows)
    assert fact["status"] == "unknown"
    assert "per_peer_api_incomplete" in fact["reasons"]


@pytest.mark.parametrize("kind", ["writer", "reader"])
def test_complete_native_worker_snapshot_preserves_existing_proof_fields(snapshot_worker, kind):
    rows = snapshot_worker(kind, observer_case="complete")
    match = next(row for row in rows if row["event"] == "match_snapshot")
    assert match["peer_gids_complete"] is True
    assert match["peer_proof_source"] == "fastdds_native_stateful_peer_snapshot"
    assert match["matched_peer_gids"] == [GIDS["reader" if kind == "writer" else "writer"]]
    assert match["native_peer_observation"]["native_result"] == {"fixture": True, "copied": 1}
    assert match["native_peer_observation"]["complete"] is True
    assert sum(row["event"] == "native_peer_provider_selected" for row in rows) == 1


def test_unadapted_rmw_keeps_explicit_absence_diagnostic(snapshot_worker, tmp_path):
    rows = [snapshot_worker(kind, rmw="offline_unadapted_rmw") for kind in ("writer", "reader")]
    fact = project_pair(tmp_path, *rows)
    assert fact["status"] == "matched"
    assert fact["identity_basis"] == "unique_bidirectional_graph_and_counts"
    for emitted in rows:
        match = next(row for row in emitted if row["event"] == "match_snapshot")
        assert match["peer_observation_limitation"] == UNAVAILABLE
        assert "peer_gids_complete" not in match


@pytest.mark.parametrize("damage", [None, "missing_proof", "incomplete", "missing_observation",
                                  "wrong_ack_generation"])
def test_native_pair_projection_retains_raw_receipt_and_generation_gates(monkeypatch, damage):
    # Reuse the repository's authored native raw/manifest fixture, not actual
    # DDS output. It remains separate from the worker JSON transport fixture.
    from test_fastdds_peer_receipt import instrumented_window
    from r2f.instrumentation.fastdds_peer import receipt
    rows, opened, close = instrumented_window()
    vendor = dict(path="/offline/lib/libfastrtps.so.2.14.7", sha256="b" * 64)
    provider = dict(path="/offline/lib/libr2f_fastdds_peer.so", sha256="c" * 64)
    manifest = dict(binaries=dict(vendor=vendor, provider=provider),
        provider_source_sha256=hashlib.sha256(
            Path(receipt.__file__).with_name("provider.cpp").read_bytes()).hexdigest(),
        patch={"abi": dict(schema_version=1, result_size=64, guid_size=16,
                           layout_changed=False, virtual_slots_added=False)})
    monkeypatch.setattr(receipt, "manifest_for_reference", lambda _: deepcopy(manifest))
    tokens = rows[:2]
    snapshots = []
    for token in tokens:
        chain = [rows[index] for index in opened["snapshot_refs"][token["logical_id"]]]
        if token is tokens[0]:
            if damage == "missing_proof":
                for key in ("peer_gids_complete", "peer_proof_source", "matched_peer_gids"):
                    chain[1].pop(key)
            elif damage == "incomplete":
                chain[1]["peer_gids_complete"] = False
            elif damage == "missing_observation":
                chain[1].pop("native_peer_observation")
            elif damage == "wrong_ack_generation":
                chain[3]["generation"] += 1
        snapshots.append(dict(complete=True, command_id=chain[0]["command_id"],
                              match_snapshot=chain[1], graph_snapshot=chain[2]))
    fact = pair_graph_facts(*tokens, *snapshots, raw_events=rows, before_index=close["event_index"])
    if damage is None:
        assert fact["status"] == "matched", fact
        assert fact["identity_basis"] == "direct_bidirectional_matched_peer_api"
        assert fact["observation_mode"] == "instrumentation"
    else:
        assert fact["status"] == "unknown", fact
        reason = {"missing_proof": "native_peer_proof_missing",
                  "incomplete": "per_peer_api_incomplete",
                  "missing_observation": "native_peer_observation_missing",
                  "wrong_ack_generation": "per_peer_chain_invalid:delivery_snapshot_reply_missing_or_duplicate:command_done"}
        assert reason[damage] in fact["reasons"], fact
    assert not fact["execution_evidence"] and not fact["candidate_eligible"]


def test_old_incomplete_emission_shape_remains_unknown(snapshot_worker, tmp_path):
    # Authored historical shape only; never rewrite/relabel an actual old run.
    rows = [snapshot_worker(kind) for kind in ("writer", "reader")]
    for emitted in rows:
        match = next(row for row in emitted if row["event"] == "match_snapshot")
        match["peer_gids_complete"] = False
    fact = project_pair(tmp_path, *rows)
    assert fact["status"] == "unknown"
    assert fact["reasons"] == ["per_peer_api_incomplete", "per_peer_api_incomplete"]
    assert not fact["candidate_eligible"]
