"""P0 regressions: aggregate N-graph ambiguity and raw payload attribution.

All observations here are explicit offline fixtures, not ROS/SUT candidates.
"""
from copy import deepcopy
from pathlib import Path
import json

import pytest

from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import pair_graph_facts, pair_delivery_fact, payload_delivery_witness, run_topology
from r2f.runtime.topology_worker import OfflineTopologyBackend


def ambiguous_graph():
    handles = {eid: {"logical_id": eid, "role": "writer" if eid[0] == "w" else "reader",
                     "generation": 1, "endpoint_gid": eid + "-gid", "pid": index + 100,
                     "topic": "/same", "created_monotonic_ns": 1}
               for index, eid in enumerate(("w0", "w1", "r0", "r1"))}
    graph = {"publishers": [{"endpoint_gid": handles[e]["endpoint_gid"]} for e in ("w0", "w1")],
             "subscriptions": [{"endpoint_gid": handles[e]["endpoint_gid"]} for e in ("r0", "r1")]}
    snapshots = {eid: {"command_id": "cmd-"+eid, "complete": True,
                       "match_snapshot": {"endpoint_gid": h["endpoint_gid"], "matched_count": 1, "command_id": "cmd-"+eid},
                       "graph_snapshot": deepcopy(graph)} for eid, h in handles.items()}
    return handles, snapshots


def test_two_distinct_match_graphs_have_identical_observables_but_no_pair_proof():
    diagonal = {("w0", "r0"), ("w1", "r1")}
    crossed = {("w0", "r1"), ("w1", "r0")}
    assert diagonal != crossed
    handles, snapshots = ambiguous_graph()
    for graph in (diagonal, crossed):
        assert all(sum(eid in pair for pair in graph) == 1 for eid in handles)
        for writer in ("w0", "w1"):
            for reader in ("r0", "r1"):
                fact = pair_graph_facts(handles[writer], handles[reader], snapshots[writer], snapshots[reader])
                assert fact["status"] == "unknown"
                assert "aggregate_counts_do_not_identify_directed_pair" in fact["reasons"]
                assert not fact["candidate_eligible"]


@pytest.mark.parametrize("real_ros", [False, True])
def test_no_candidate_flag_does_not_excuse_ambiguous_status(real_ros):
    handles, snapshots = ambiguous_graph()
    fact = pair_graph_facts(handles["w0"], handles["r1"], snapshots["w0"], snapshots["r1"], real_ros=real_ros)
    assert fact["status"] == "unknown"


def test_unique_single_pair_is_expressible_but_not_per_peer_ack():
    handles, snapshots = ambiguous_graph()
    for snap in snapshots.values():
        snap["graph_snapshot"]["publishers"] = [{"endpoint_gid": "w0-gid"}]
        snap["graph_snapshot"]["subscriptions"] = [{"endpoint_gid": "r0-gid"}]
    fact = pair_graph_facts(handles["w0"], handles["r0"], snapshots["w0"], snapshots["r0"])
    assert fact["status"] == "matched"
    assert fact["identity_basis"] == "unique_bidirectional_graph_and_counts"
    assert "not_independent_per_peer_match_ack" in fact["limitations"]


@pytest.mark.parametrize("count", [0, 2])
def test_single_pair_requires_both_counts_exactly_one(count):
    handles, snapshots = ambiguous_graph()
    for snap in snapshots.values():
        snap["graph_snapshot"]["publishers"] = [{"endpoint_gid": "w0-gid"}]
        snap["graph_snapshot"]["subscriptions"] = [{"endpoint_gid": "r0-gid"}]
    snapshots["r0"]["match_snapshot"]["matched_count"] = count
    assert pair_graph_facts(handles["w0"], handles["r0"], snapshots["w0"], snapshots["r0"])["status"] == "unknown"


class NoGidFixture(OfflineTopologyBackend):
    def _emit(self, event, **fields):
        if event == "receive":
            fields.update(publisher_gid=None, publisher_gid_status="unavailable_api", identity_basis="endpoint_correlated")
        return super()._emit(event, **fields)


def payload_run(tmp_path):
    program = model.generate_program(7, writers=2, readers=2)
    for endpoint in program["endpoints"]:
        endpoint.update(topic="/same", qos=model._qos(), vendor=None, participant_id=None)
    result = run_topology(program, {"backend": "offline_fake", "run_id": "payload-fixture", "domain_id": 191}, tmp_path,
                          backend=NoGidFixture())
    assert result["status"] == "executed", result["reason"]
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    pair = next(row for row in result["pair_facts"] if row["writer"] == "w0" and row["reader"] == "r0")
    receives = [e for e in events if e["event"] == "receive" and e.get("logical_id") == "r0" and e.get("payload_writer_id") == "w0"]
    assert receives
    return pair, receives[0], events, result


def test_multiwriter_unique_payload_witness_is_not_direct_gid(tmp_path):
    pair, receive, events, result = payload_run(tmp_path)
    witness, reason = payload_delivery_witness(pair, receive, events)
    assert reason is None and witness["identity_basis"] == "endpoint_correlated_payload"
    assert witness["direct_gid_match"] is None
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=2)
    assert fact["status"] == "complete", fact
    assert fact["identity_basis"] == "endpoint_correlated_payload"
    assert fact["communication_observed"] and fact["direct_gid_receipts"] == 0
    assert not fact["candidate_eligible"] and not result["real_execution"]
    # This witnesses only the communicated pair, not any unobserved N edge.
    assert pair["status"] == "unknown"


@pytest.mark.parametrize("mutation,reason", [
    ("missing_send", "payload_send_mapping_not_unique"),
    ("duplicate_send", "payload_send_mapping_not_unique"),
    ("wrong_run", "payload_send_mapping_not_unique"),
    ("wrong_writer", "payload_run_writer_generation_sequence_or_gid_mismatch"),
    ("wrong_generation", "payload_run_writer_generation_sequence_or_gid_mismatch"),
    ("wrong_sequence", "payload_run_writer_generation_sequence_or_gid_mismatch"),
    ("wrong_gid", "payload_run_writer_generation_sequence_or_gid_mismatch"),
    ("wrong_worker_pid", "payload_worker_identity_not_bound"),
    ("missing_created", "payload_worker_identity_not_bound"),
    ("missing_hash", "payload_hash_or_run_missing"),
])
def test_payload_ambiguity_and_identity_fail_closed(tmp_path, mutation, reason):
    pair, receive, events, _ = payload_run(tmp_path)
    events = deepcopy(events)
    receive = next(e for e in events if e["event_index"] == receive["event_index"])
    send = next(e for e in events if e["event"] == "send" and e.get("payload_sha256") == receive["payload_sha256"])
    if mutation == "missing_send": events.remove(send)
    elif mutation == "duplicate_send": events.append(deepcopy(send))
    elif mutation == "wrong_run": receive["payload_run_id"] = "another-run"
    elif mutation == "wrong_writer": receive["payload_writer_id"] = "w1"
    elif mutation == "wrong_generation": receive["payload_writer_generation"] = 2
    elif mutation == "wrong_sequence": receive["send_sequence"] = 999
    elif mutation == "wrong_gid": receive["payload_endpoint_gid"] = "other-gid"
    elif mutation == "wrong_worker_pid": receive["pid"] += 1
    elif mutation == "missing_created": events = [e for e in events if not(e["event"] == "endpoint_created" and e.get("logical_id") == "w0")]
    elif mutation == "missing_hash": receive["payload_sha256"] = None
    witness, actual = payload_delivery_witness(pair, receive, events)
    assert witness is None and actual == reason


def test_payload_no_receive_is_not_match_or_violation(tmp_path):
    pair, _, events, _ = payload_run(tmp_path)
    events = [e for e in events if e["event"] != "receive"]
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=2)
    assert not fact["communication_observed"]
    assert fact["status"] != "complete"
    assert "finite_trailing_missing_is_not_a_violation" in fact["limitations"]


def _single_writer_fixture():
    pair = {"writer": "w0", "reader": "r0", "writer_generation": 1,
            "reader_generation": 1, "writer_gid": "aa", "reader_gid": "bb",
            "status": "matched", "reader_created_monotonic_ns": 0}
    events = [
        {"event": "endpoint_created", "logical_id": "w0", "role": "writer",
         "generation": 1, "topic": "/t", "monotonic_ns": 0},
        {"event": "send", "logical_id": "w0", "generation": 1,
         "send_sequence": 0, "monotonic_ns": 1},
        {"event": "receive", "logical_id": "r0", "generation": 1,
         "send_sequence": 0, "publisher_gid": None},
    ]
    return pair, events


@pytest.mark.parametrize("active", [None, 0, 2])
def test_single_writer_label_requires_an_explicit_one_writer_inventory(active):
    """A missing or non-singleton count is never coerced to "one writer"."""
    pair, events = _single_writer_fixture()
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=active)
    assert fact["status"] == "unknown"
    assert fact["identity_basis"] == "unknown"
    assert "active_writer_inventory_not_exactly_one" in fact["attribution_reasons"]


def test_single_writer_label_is_explicit_and_not_direct_gid():
    pair, events = _single_writer_fixture()
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=1)
    assert fact["status"] == "complete"
    assert fact["identity_basis"] == "endpoint_correlated_single_writer"
    assert fact["single_writer_receipts"] == 1
    assert fact["direct_gid_receipts"] == 0
    assert "single_writer_correlation_is_not_direct_publisher_gid" in fact["limitations"]


def test_single_writer_label_fails_closed_on_raw_two_writer_inventory():
    """Caller-supplied 1 cannot override captured evidence of two live writers."""
    pair, events = _single_writer_fixture()
    events = deepcopy(events)
    events.insert(1, {"event": "endpoint_created", "logical_id": "w1", "role": "writer",
                      "generation": 1, "topic": "/t", "monotonic_ns": 0})
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=1)
    assert fact["status"] == "unknown"
    assert fact["identity_basis"] == "unknown"
    assert "raw_writer_inventory_contradicts_single_writer" in fact["attribution_reasons"]


def test_single_writer_label_fails_closed_on_competing_send_sequence():
    pair, events = _single_writer_fixture()
    events = deepcopy(events)
    events.insert(1, {"event": "send", "logical_id": "w1", "generation": 1,
                      "send_sequence": 0, "monotonic_ns": 1})
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=1)
    assert fact["status"] == "unknown"
    assert "sequence_send_inventory_not_unique_to_pair_writer" in fact["attribution_reasons"]


def test_single_writer_count_without_raw_identity_is_no_longer_proof():
    pair = {"writer": "w0", "reader": "r0", "writer_generation": 1, "reader_generation": 1,
            "writer_gid": "a", "reader_gid": "b", "status": "matched"}
    events = [{"event": "send", "logical_id": "w0", "generation": 1, "send_sequence": 0},
              {"event": "receive", "logical_id": "r0", "generation": 1, "send_sequence": 0, "publisher_gid": None}]
    fact = pair_delivery_fact(pair, events, active_writers_on_topic=1)
    assert fact["status"] == "unknown" and fact["identity_basis"] == "unknown"


def test_visible_graph_and_two_zero_counts_is_ambiguous_unknown():
    """Graph visibility is discovery, not match: zero counts plus a visible peer
    is an observational ambiguity, never a positive non-match conclusion."""
    handles, snapshots = ambiguous_graph()
    snapshots["w0"]["match_snapshot"]["matched_count"] = 0
    snapshots["r1"]["match_snapshot"]["matched_count"] = 0
    fact = pair_graph_facts(handles["w0"], handles["r1"], snapshots["w0"], snapshots["r1"])
    assert fact["writer_graph_contains_reader"] and fact["reader_graph_contains_writer"]
    assert fact["status"] == "unknown"
    assert fact["reasons"] == ["zero_count_contradicts_graph_entry"]
    assert "graph_visibility_is_not_per_peer_match_proof" in fact["limitations"]
    # Other edges are NOT inferred matched from this ambiguous observation.
    assert pair_graph_facts(handles["w1"], handles["r0"], snapshots["w1"], snapshots["r0"])["status"] == "unknown"


def test_invisible_graph_and_two_zero_counts_is_local_unmatched():
    """The opposite fixture keeps the unambiguous zero/zero non-match path."""
    handles, snapshots = ambiguous_graph()
    snapshots["w0"]["match_snapshot"]["matched_count"] = 0
    snapshots["r1"]["match_snapshot"]["matched_count"] = 0
    snapshots["w0"]["graph_snapshot"]["publishers"] = []
    snapshots["w0"]["graph_snapshot"]["subscriptions"] = []
    snapshots["r1"]["graph_snapshot"]["publishers"] = []
    snapshots["r1"]["graph_snapshot"]["subscriptions"] = []
    fact = pair_graph_facts(handles["w0"], handles["r1"], snapshots["w0"], snapshots["r1"])
    assert not fact["writer_graph_contains_reader"] and not fact["reader_graph_contains_writer"]
    assert fact["status"] == "unmatched"
    assert fact["reasons"] == ["both_public_counts_zero"]
    assert "local_snapshot_counts_only_not_persistent_nonmatch" in fact["limitations"]


def test_offline_backend_defaults_preserve_the_legacy_contract():
    """Defaults are the legacy contract; ambiguous fixtures must opt in."""
    backend = OfflineTopologyBackend()
    assert backend.auto_match is True
    assert backend.auto_delivery is True
    assert backend.match_policy == "all"
    assert backend.graph_visibility == "matched"
    assert backend.publisher_gid_available is True
    controls = backend.prepare({"environment": {}}, {"run_id": "defaults"})[
        "fixture_observation_controls"]
    assert controls["match_policy"] == "all"
    assert controls["graph_visibility"] == "matched"
    assert controls["publisher_gid_available"] is True
    assert controls["scope"] == "explicit_offline_fixture_control_not_live_semantics"

    backend.prepare({"environment": {}}, {"run_id": "defaults"})
    program = model.generate_program(7, writers=2, readers=2)
    for spec in program["endpoints"]:
        spec["topic"] = "/same"
    handles = {spec["logical_id"]: backend.create_endpoint(spec, 1, spec["role"])
               for spec in program["endpoints"]}
    snapshots = backend.snapshot(handles)["snapshots"]
    # Legacy "all" policy: every writer matches every reader.
    assert all(row["match_snapshot"]["matched_count"] == 2 for row in snapshots.values())
    # Legacy "matched" visibility keeps same-role inventory fully visible.
    for logical_id, row in snapshots.items():
        role = handles[logical_id]["role"]
        expected = 2
        key = "publishers" if role == "writer" else "subscriptions"
        assert len(row["graph_snapshot"][key]) == expected
    backend.close()


def test_fixture_graph_visibility_matching_and_aggregate_counts_are_independent():
    # Explicit opt-in: the legacy default filters graph by match relation.
    backend = OfflineTopologyBackend(auto_match=False, graph_visibility="all_live_topic",
                                     publisher_gid_available=True)
    backend.prepare({"environment": {}}, {"run_id": "independent-observation-fixture"})
    program = model.generate_program(7, writers=1, readers=1)
    for spec in program["endpoints"]:
        spec["topic"] = "/same"
    handles = {spec["logical_id"]: backend.create_endpoint(spec, 1, spec["role"]) for spec in program["endpoints"]}
    first = backend.snapshot(handles)["snapshots"]
    assert first["w0"]["match_snapshot"]["matched_count"] == 0
    assert len(first["w0"]["graph_snapshot"]["subscriptions"]) == 1
    backend.set_match_edge("w0", "r0")
    second = backend.snapshot(handles)["snapshots"]
    assert second["w0"]["match_snapshot"]["matched_count"] == 1
    assert second["w0"]["graph_snapshot"] == first["w0"]["graph_snapshot"]
    backend.set_graph_visibility("w0", "r0", visible=False)
    third = backend.snapshot(handles)["snapshots"]
    assert third["w0"]["match_snapshot"]["matched_count"] == 1
    assert third["w0"]["graph_snapshot"]["subscriptions"] == []
    backend.set_matched_count("w0", 3)
    assert backend.snapshot(handles)["snapshots"]["w0"]["match_snapshot"]["matched_count"] == 3
    backend.set_matched_count("w0", None)
    assert backend.snapshot(handles)["snapshots"]["w0"]["match_snapshot"]["matched_count"] == 1
    backend.close()
