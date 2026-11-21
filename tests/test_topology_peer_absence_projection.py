"""Offline public worker JSON -> transport -> pair and full typed projection.

ROS/pipe/observer inputs use our worker fixture. Harness input binding, isolation,
network and delivery rows below are authored fixtures, never execution evidence.
No historical raw/report, native library, ROS process or network is used.
"""
from copy import deepcopy
import json

import pytest

from r2f.campaign.topology_state_projection import project_topology_state
from r2f.core.evidence import canonical_hash
from r2f.runtime.topology_pair import pair_graph_facts
from r2f.runtime.topology_worker import RosTopologyBackend
from test_topology_peer_unavailable import (
    SnapshotTransport, UNAVAILABLE, snapshot_worker,
)


def worker_projection(tmp_path, rows, *, damage=None):
    """Consume worker JSON at the real backend seam, then persist a fresh fixture."""
    handles = {}
    for emitted in rows:
        token = next(row for row in emitted if row["event"] == "endpoint_created")
        handles[token["logical_id"]] = {**token, "process": SnapshotTransport(emitted)}
    transport = RosTopologyBackend({}, tmp_path).snapshot(handles)
    w, r = handles["w0"], handles["r0"]
    snapshots = transport["snapshots"]
    fact = pair_graph_facts(w, r, snapshots["w0"], snapshots["r0"], real_ros=False)
    program = {
        "schema": "r2f.s1-topology-program/v1",
        "endpoints": [{"logical_id": eid, "role": token["role"],
                       "topic": token["topic"], "type_name": token["message_type"]}
                      for eid, token in handles.items()],
        "actions": [{"action_id": "create-" + eid, "op": "create",
                     "endpoint_id": eid, "generation": 1} for eid in handles],
    }
    events = []

    def add(event, **fields):
        row = deepcopy(fields)
        row.update(event=event, event_index=len(events),
                   monotonic_ns=(len(events) + 1) * 100,
                   backend="offline_fake", fixture=True,
                   run_id=w["run_id"], real_ros=False)
        events.append(row)
        return row["event_index"]

    def identity(token):
        return {k: token[k] for k in ("logical_id", "generation", "pid",
                                      "process_start_ticks", "endpoint_generation")}

    def copy_row(row):
        return add(row["event"], **{k: v for k, v in row.items()
                                   if k not in ("event", "process")})

    add("input_binding", receipt={"program_sha256": canonical_hash(program)})
    add("topology_backend_prepared", real_ros=False)
    carrier = add("topology_network_phase", network_phase="normal", recovery_cycle=0,
                  observed=True, implementation="authored-offline-no-network", audit_neutral=True)
    add("topology_network_observation", network_phase="normal", recovery_cycle=0,
        source="carrier_audit", carrier_ref=carrier)
    before = add("topology_state_boundary", lifecycle_complete=True,
                 observation_complete=True, window_complete=True)
    for token in handles.values():
        copy_row(token)
    graphs = []
    for emitted in rows:
        for row in emitted:
            if row["event"] == "native_peer_provider_selected":
                copy_row(row)
    for eid, token in handles.items():
        snapshot = snapshots[eid]
        # Retain older command replies too: a qualified historical count must
        # never rescue an incomplete current interval in either consumer.
        replies = token["process"].events
        command_ids = dict.fromkeys(row["command_id"] for row in replies
                                    if row["event"] == "match_snapshot")
        for command_id in command_ids:
            add("worker_command_requested", **identity(token), command="snapshot",
                command_id=command_id)
            for row in replies:
                if (row.get("command_id") == command_id
                        and row["event"] in ("match_snapshot", "graph_snapshot", "command_done")):
                    ref = copy_row(row)
                    if row["event"] == "graph_snapshot" and command_id == snapshot["command_id"]:
                        graphs.append(ref)
    pair = dict(writer="w0", reader="r0", writer_generation=1, reader_generation=1)
    add("topology_controlled_isolation", **pair, isolation_id="offline-worker-single-pair",
        mechanism="authored_fixture_inventory", inventory_complete=True, graph_refs=graphs)
    opened = add("topology_delivery_window_open", **pair)
    for seq in range(3):
        add("send", **identity(w), writer_gid=w["endpoint_gid"],
            send_sequence=seq, publish_returned=True)
        add("receive", **identity(r), reader_gid=r["endpoint_gid"],
            publisher_gid=w["endpoint_gid"], send_sequence=seq)
    add("topology_delivery_window_closed", **pair, open_ref=opened,
        window_complete=True, sentinel_sequence=2)
    after = add("topology_state_boundary", lifecycle_complete=True,
                observation_complete=True, window_complete=True)
    if damage is not None:
        damage(events)
    raw = tmp_path / "offline-worker-projection.jsonl"
    raw.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events))
    result = project_topology_state(program, raw, before_ref=before, after_ref=after)
    assert result["eligible"] is False
    assert result["receipt"]["fixture"] is True
    assert result["receipt"]["execution_evidence"] is False
    return fact, result


def test_blackbox_worker_absence_projects_through_transport_pair_and_typed_state(
        snapshot_worker, tmp_path):
    rows = [snapshot_worker(kind) for kind in ("writer", "reader")]
    fact, result = worker_projection(tmp_path, rows)
    assert fact["status"] == "matched", fact
    assert fact["identity_basis"] == "unique_bidirectional_graph_and_counts"
    assert not fact["execution_evidence"] and not fact["candidate_eligible"]
    assert result["state_after"]["pairs"][0]["observed_match"] == "bidirectional", result["receipt"]["reasons"]
    assert result["state_after"]["pairs"][0]["delivery_closure"] == "closed"
    assert result["state_after"]["observation_completeness"] == "complete"
    assert result["receipt"]["status"] == "observed", result["receipt"]["reasons"]
    assert result["receipt"]["reasons"] == []
    component = result["receipt"]["after"]["component_refs"]["/pairs/w0/r0/observed_match"]
    assert component["detail"]["proof_kind"] == "controlled_isolation"
    for emitted in rows:
        row = next(row for row in emitted if row["event"] == "match_snapshot")
        assert row["peer_observation_limitation"] == UNAVAILABLE
        assert all(key not in row for key in ("native_peer_observation", "peer_proof_source",
                                              "peer_gids_complete", "matched_peer_gids"))


@pytest.mark.parametrize("rmw", ["rmw_fastrtps_cpp", "rmw_cyclonedds_cpp"])
@pytest.mark.parametrize("kind", ["writer", "reader"])
@pytest.mark.parametrize("case", ["missing", "incomplete", "constructor_error",
                                  "constructor_absence_text", "sample_error", "sample_absence_text"])
def test_selected_observer_failure_cannot_use_typed_isolation_fallback(
        snapshot_worker, tmp_path, rmw, kind, case):
    rows = [snapshot_worker(role, rmw=rmw, observer_case=case if role == kind else None)
            for role in ("writer", "reader")]
    fact, result = worker_projection(tmp_path, rows)
    assert fact["status"] == "unknown"
    assert result["state_after"]["pairs"][0]["observed_match"] == "unknown"
    assert result["receipt"]["status"] == "incomplete"


@pytest.mark.parametrize("declaration", ["legacy_absence", "null", "unknown_schema",
                                        "missing_observation", "incomplete", "forged_complete"])
def test_native_claims_cannot_be_rescued_by_typed_isolation(
        snapshot_worker, tmp_path, declaration):
    rows = [snapshot_worker(kind) for kind in ("writer", "reader")]

    def damage(events):
        match = next(row for row in events if row["event"] == "match_snapshot")
        if declaration == "legacy_absence":
            match["native_peer_observation"] = {"complete": False, "reason": UNAVAILABLE}
        elif declaration == "null":
            match["native_peer_observation"] = None
        elif declaration == "unknown_schema":
            match["native_peer_observation"] = {"schema": "unrecognized", "complete": True}
        else:
            match.update(peer_proof_source="fastdds_native_stateful_peer_snapshot",
                         peer_gids_complete=declaration != "incomplete",
                         matched_peer_gids=["2" * 32])
            if declaration != "missing_observation":
                match["native_peer_observation"] = {"complete": declaration == "forged_complete"}

    _, result = worker_projection(tmp_path, rows, damage=damage)
    assert result["state_after"]["pairs"][0]["observed_match"] == "unknown"
    assert result["receipt"]["status"] == "incomplete"


def test_N_peer_graph_ambiguity_cannot_use_typed_single_pair_isolation(
        snapshot_worker, tmp_path):
    rows = [snapshot_worker(kind, extra_peer=True) for kind in ("writer", "reader")]
    fact, result = worker_projection(tmp_path, rows)
    assert fact["status"] == "unknown"
    assert fact["reasons"] == ["aggregate_counts_do_not_identify_directed_pair"]
    assert result["state_after"]["pairs"][0]["observed_match"] == "unknown"
    assert any("controlled_isolation_inventory_has_other_peers" in reason
               for reason in result["receipt"]["reasons"])


@pytest.mark.parametrize("kind", ["writer", "reader"])
@pytest.mark.parametrize("damage_kind", ["missing_ack", "stale_generation", "ack_before_graph"])
def test_typed_snapshot_chain_stays_strict_after_absence_correction(
        snapshot_worker, tmp_path, damage_kind, kind):
    rows = [snapshot_worker(kind) for kind in ("writer", "reader")]

    def damage(events):
        ack = next(row for row in events if row["event"] == "command_done" and row["role"] == kind)
        if damage_kind == "missing_ack":
            ack["event"] = "authored_transport_loss"
        elif damage_kind == "stale_generation":
            ack["generation"] += 1
        else:
            graph = next(row for row in events if row["event"] == "graph_snapshot" and row["role"] == kind)
            ack["monotonic_ns"] = graph["monotonic_ns"] - 1

    _, result = worker_projection(tmp_path, rows, damage=damage)
    assert result["state_after"]["pairs"][0]["observed_match"] == "unknown"
    assert result["receipt"]["status"] == "incomplete"


@pytest.mark.parametrize("kind", ["writer", "reader"])
def test_native_capture_count_race_stays_unknown_in_pair_and_typed_projection(
        snapshot_worker, tmp_path, kind):
    rows = [snapshot_worker(role, observer_case="complete", count_after=2 if role == kind else None)
            for role in ("writer", "reader")]
    fact, result = worker_projection(tmp_path, rows)
    match = next(row for row in rows[0 if kind == "writer" else 1]
                 if row["event"] == "match_snapshot")
    assert match["public_count_before"] == 1
    assert match["public_count_after"] == 2
    assert match["matched_count"] is None
    assert match["peer_gids_complete"] is False
    assert fact["status"] == "unknown"
    assert result["state_after"]["pairs"][0]["observed_match"] == "unknown"
    assert result["receipt"]["status"] == "incomplete"


@pytest.mark.parametrize("kind", ["writer", "reader"])
@pytest.mark.parametrize("before,after", [(0, 1), (2, 1), (1, 0), (2, 0), (0, 2), (1, 2)])
def test_absent_observer_count_race_stays_unknown_through_typed_projection(
        snapshot_worker, tmp_path, kind, before, after):
    rows = [snapshot_worker(role, count=before if role == kind else 1,
                            count_after=after if role == kind else None)
            for role in ("writer", "reader")]
    fact, result = worker_projection(tmp_path, rows)
    assert (fact["status"], result["state_after"]["pairs"][0]["observed_match"],
            result["receipt"]["status"]) == ("unknown", "unknown", "incomplete")
    match = next(row for row in rows[0 if kind == "writer" else 1]
                 if row["event"] == "match_snapshot")
    assert match["public_count_before"] == before
    assert match["public_count_after"] == after
    assert match["matched_count"] is None
    assert match["peer_observation_limitation"] == UNAVAILABLE
    assert all(key not in match for key in ("native_peer_observation", "peer_proof_source",
                                           "peer_gids_complete", "matched_peer_gids"))


@pytest.mark.parametrize("kind", ["writer", "reader"])
@pytest.mark.parametrize("before,after", [(0, 1), (2, 1), (1, 0)])
def test_old_stable_snapshot_cannot_rescue_current_count_race(
        snapshot_worker, tmp_path, kind, before, after):
    rows = [snapshot_worker(role, count=before if role == kind else 1,
                            count_after=after if role == kind else None)
            for role in ("writer", "reader")]
    old = [row for row in snapshot_worker(kind)
           if row["event"] in ("match_snapshot", "graph_snapshot", "command_done")]
    for row in old:
        row["command_id"] = 0
    selected = rows[0 if kind == "writer" else 1]
    pos = next(i for i, row in enumerate(selected) if row["event"] == "match_snapshot")
    selected[pos:pos] = old
    fact, result = worker_projection(tmp_path, rows)
    assert (fact["status"], result["state_after"]["pairs"][0]["observed_match"],
            result["receipt"]["status"]) == ("unknown", "unknown", "incomplete")
    persisted = [json.loads(line) for line in
                 (tmp_path / "offline-worker-projection.jsonl").read_text().splitlines()]
    samples = [row for row in persisted if row["event"] == "match_snapshot"
               and row["role"] == kind]
    assert [(row["command_id"], row["public_count_before"], row["public_count_after"],
             row["matched_count"]) for row in samples] == [(0, 1, 1, 1), (1, before, after, None)]


def test_stable_zero_counts_remain_zero_in_pair_and_typed_projection(snapshot_worker, tmp_path):
    rows = [snapshot_worker(kind, count=0) for kind in ("writer", "reader")]
    fact, result = worker_projection(tmp_path, rows)
    assert fact["status"] == "unknown"
    assert fact["reasons"] == ["zero_count_contradicts_graph_entry"]
    # Named controlled isolation supports the typed local zero observation;
    # the pair-only graph fallback correctly remains ambiguous.
    assert result["state_after"]["pairs"][0]["observed_match"] == "none"
    for emitted in rows:
        match = next(row for row in emitted if row["event"] == "match_snapshot")
        assert (match["public_count_before"], match["public_count_after"], match["matched_count"]) == (0, 0, 0)
