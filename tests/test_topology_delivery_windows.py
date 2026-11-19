"""Offline producer -> immutable JSONL -> existing delivery-window consumer.

Every backend here is an explicit contract fixture, not ROS/DDS evidence. Faults
and stdout-drain ordering are generated at the backend boundary, never patched
into already persisted raw. No fixture can qualify as a real SUT candidate.
"""
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from r2f.campaign.topology_state_projection import project_topology_state
from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import run_topology
from r2f.runtime.topology_worker import (
    OfflineTopologyBackend, TopologyBackendError, TopologyBackendTimeout,
)


class WindowFixture(OfflineTopologyBackend):
    """Named non-DDS producer faults and cross-worker stdout drain ordering."""
    def __init__(self, *, omit=(), reverse_drain=False, snapshot_ack=None,
                 corrupt_payload=False, fail_publish=False, abort_snapshot=False,
                 defer_receive=False, **kwargs):
        super().__init__(**kwargs)
        self.omit = set(omit)
        self.reverse_drain = reverse_drain
        self.snapshot_ack = snapshot_ack
        self.corrupt_payload = corrupt_payload
        self.fail_publish = fail_publish
        self.abort_snapshot = abort_snapshot
        self.defer_receive = defer_receive
        self.pending_sends = []
        self.pending_receives = []
        self.snapshot_commands = set()
        self.published = False

    def _emit(self, event, **fields):
        fields = {"fixture": True, **deepcopy(fields)}
        command = (fields.get("logical_id"), fields.get("command_id"))
        if event == "topology_snapshot_requested":
            self.snapshot_commands.add(command)
        if (event == "command_done" and command in self.snapshot_commands
                and fields.get("logical_id") == "r0"):
            if self.snapshot_ack == "missing":
                return {"event": event, **fields}
            if self.snapshot_ack == "duplicate":
                super()._emit(event, **fields)
        if event == "send" and self.reverse_drain:
            self.pending_sends.append({"monotonic_ns": time.monotonic_ns(), **fields})
            return {"event": event, **fields}
        if event == "receive":
            if self.corrupt_payload:
                fields["payload_writer_generation"] = 99
            if fields.get("send_sequence") not in self.omit:
                if self.defer_receive:
                    self.pending_receives.append({"monotonic_ns": time.monotonic_ns(), **fields})
                else:
                    super()._emit(event, **fields)
            while self.pending_sends:
                super()._emit("send", **self.pending_sends.pop(0))
            return {"event": event, **fields}
        return super()._emit(event, **fields)

    def publish_batch(self, *args, **kwargs):
        value = super().publish_batch(*args, **kwargs)
        self.published = True
        if self.fail_publish:
            raise TopologyBackendTimeout("fixture_publish_command_ack_timeout",
                                        command_id=value["command_id"])
        return value

    def snapshot(self, handles):
        if self.abort_snapshot and self.published:
            raise TopologyBackendError("fixture_abnormal_observation_termination")
        return super().snapshot(handles)

    def pump(self, timeout=0.0):
        while self.pending_receives:
            super()._emit("receive", **self.pending_receives.pop(0))
        return 0


def _program(writers=1, readers=1):
    return model.generate_program(7, writers=writers, readers=readers)


def _action(aid, op, eid=None, gen=None, **params):
    return {"action_id": aid, "op": op, "endpoint_id": eid,
            "generation": gen, "params": params, "depends_on": []}


def _run(tmp_path, program=None, backend=None):
    program = program if program is not None else _program()
    assert model.validate_program(program)["status"] == "valid"
    runtime = run_topology(
        program, {"backend": "offline_fake", "run_id": "delivery-window-fixture",
                  "domain_id": 191}, tmp_path / "runtime", backend=backend)
    raw = Path(runtime["raw_path"])
    events = [json.loads(line) for line in raw.read_text().splitlines()]
    assert events == runtime["events"]
    boundaries = [e for e in events if e["event"] == "topology_state_boundary"]
    projection = project_topology_state(
        program, raw, before_ref=boundaries[0]["event_index"],
        after_ref=boundaries[-1]["event_index"], expected_sha256=runtime["raw_sha256"])
    (tmp_path / "projection.json").write_text(json.dumps(projection, indent=2) + "\n")
    assert not runtime["real_execution"] and not projection["eligible"]
    assert not any(runtime[k] for k in ("candidate_eligible", "mature", "sealed"))
    assert projection["state_after"]["network_phase"] == "unknown"
    assert not any(e["event"] in {"topology_network_phase", "topology_network_observation",
                                 "topology_controlled_isolation"} for e in events)
    return runtime, events, projection, boundaries[-1]


def _closed(events):
    rows = [e for e in events if e["event"] == "topology_delivery_window_closed"]
    assert rows, "runtime producer has no raw delivery-window close chain"
    return rows[-1]


def test_current_pair_opens_only_after_fresh_both_snapshot_acks(tmp_path):
    runtime, events, projection, boundary = _run(tmp_path)
    close = _closed(events)
    opened = events[close["open_ref"]]
    assert opened["event"] == "topology_delivery_window_open"
    assert opened["fixture"] and close["fixture"]
    assert not opened["candidate_eligible"] and not close["candidate_eligible"]
    for eid in ("w0", "r0"):
        refs = opened["snapshot_refs"][eid]
        chain = [events[i] for i in refs]
        assert [r["event"] for r in chain] == [
            "topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"]
        assert all(r["logical_id"] == eid and r["generation"] == 1 for r in chain)
        assert len({r["command_id"] for r in chain}) == 1
        assert refs == sorted(set(refs)) and refs[-1] < opened["event_index"]
        assert all(a["monotonic_ns"] <= b["monotonic_ns"] for a, b in zip(chain, chain[1:]))
    assert all(opened["event_index"] < e["event_index"] < close["event_index"]
               for e in events if e["event"] == "send")
    assert close["window_complete"] and close["sentinel_sequence"] == 2
    assert boundary["window_complete"]
    assert close["event_index"] in boundary["delivery_window_refs"]
    pair = projection["state_after"]["pairs"][0]
    assert pair["delivery_closure"] == "closed"
    # Count+graph is still not independent per-peer evidence.
    assert pair["observed_match"] == "unknown"
    assert not projection["eligible"]


def test_missing_gid_uses_unique_raw_payload_generation_not_direct_gid(tmp_path):
    _, events, projection, _ = _run(tmp_path, backend=WindowFixture(publisher_gid_available=False))
    close = _closed(events)
    assert close["window_complete"]
    assert all(e["publisher_gid"] is None for e in events if e["event"] == "receive")
    assert all(e["publisher_gid_status"] == "unavailable_api" for e in events if e["event"] == "receive")
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"
    assert close["attribution"] == "endpoint_correlated_payload"


def test_receive_raw_before_send_raw_is_valid_without_cross_worker_order(tmp_path):
    _, events, projection, boundary = _run(
        tmp_path, backend=WindowFixture(reverse_drain=True, publisher_gid_available=False))
    assert all(next(e["event_index"] for e in events if e["event"] == "receive" and e["send_sequence"] == seq)
               < next(e["event_index"] for e in events if e["event"] == "send" and e["send_sequence"] == seq)
               for seq in range(3))
    assert _closed(events)["window_complete"] and boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"


def test_middle_gap_only_has_actual_later_sentinel_refs(tmp_path):
    _, events, projection, boundary = _run(tmp_path, backend=WindowFixture(omit={1}))
    close = _closed(events)
    assert close["window_complete"] and boundary["window_complete"]
    assert close["closure_status"] == "closed_with_gap"
    assert close["missing_sequences"] == [1]
    gap, = close["gap_evidence"]
    assert gap["sequence"] == 1
    assert events[gap["send_ref"]]["send_sequence"] == 1
    assert events[gap["sentinel_send_ref"]]["send_sequence"] == 2
    assert events[gap["sentinel_receive_ref"]]["event"] == "receive"
    assert events[gap["sentinel_receive_ref"]]["send_sequence"] == 2
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed_with_gap"


@pytest.mark.parametrize("backend", [WindowFixture(omit={2}), WindowFixture(auto_delivery=False)])
def test_trailing_inflight_and_command_done_are_never_delivery_ack(tmp_path, backend):
    _, events, projection, boundary = _run(tmp_path, backend=backend)
    assert any(e["event"] == "command_done" and "burst_count" in e for e in events)
    close = _closed(events)
    assert not close["window_complete"] and not boundary["window_complete"]
    assert close["gap_evidence"] == []
    assert "sentinel_sequence" not in close
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "unknown"


@pytest.mark.parametrize("mode", ["unmatched", "incompatible", "multiwriter"])
def test_no_match_proof_does_not_forbid_original_publish(tmp_path, mode):
    program = _program(writers=2 if mode == "multiwriter" else 1)
    backend = WindowFixture(auto_match=mode != "unmatched", publisher_gid_available=False,
                            graph_visibility="all_live_topic", match_policy="one_to_one")
    if mode == "incompatible":
        next(e for e in program["endpoints"] if e["role"] == "reader")["topic"] += "/other"
    runtime, events, projection, boundary = _run(tmp_path, program, backend)
    assert runtime["status"] == "executed"
    assert any(e["event"] == "send" for e in events)
    assert not any(r["code"] == "publish_batch_error" for r in runtime["guard_receipts"])
    if mode == "incompatible":
        claims = [e for e in events if e["event"] == "topology_delivery_no_obligation"]
        assert len(claims) == 1 and claims[0]["reason"] == "actual_endpoint_topic_mismatch"
        claim = claims[0]
        created = {role: [e for e in events if e["event"] == "endpoint_created"
                         and e.get("logical_id") == claim[role]
                         and e.get("generation") == claim[role + "_generation"]]
                   for role in ("writer", "reader")}
        assert all(len(rows) == 1 for rows in created.values())
        assert created["writer"][0]["topic"] != created["reader"][0]["topic"]
        for role in ("writer", "reader"):
            token = created[role][0]
            assert token["endpoint_gid"] == claim[role + "_gid"]
            refs = claim["snapshot_refs"][token["logical_id"]]
            chain = [events[index] for index in refs]
            assert [row["event"] for row in chain] == [
                "topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"]
            assert len({row["command_id"] for row in chain}) == 1
            assert chain[-1]["event_index"] < claim["open_ref"] < claim["event_index"]
            own = [row for row in chain[2]["publishers" if role == "writer" else "subscriptions"]
                   if row["endpoint_gid"] == token["endpoint_gid"]]
            assert len(own) == 1 and own[0]["topic_type"] == token["message_type"]
        assert _closed(events)["window_complete"] and boundary["window_complete"]
        assert _closed(events)["expected_sequences"] == []
        assert _closed(events)["no_obligation_ref"] == claim["event_index"]
        assert all(p["delivery_closure"] == "no_obligation" for p in projection["state_after"]["pairs"])
        assert not runtime["candidate_eligible"] and not projection["eligible"]
    else:
        assert not _closed(events)["window_complete"] and not boundary["window_complete"]
        assert all(p["delivery_closure"] == "unknown" for p in projection["state_after"]["pairs"])


@pytest.mark.parametrize("ack", ["missing", "duplicate"])
def test_unverified_snapshot_ack_is_tool_failure_not_published_obligation(tmp_path, ack):
    runtime, events, _, boundary = _run(tmp_path, backend=WindowFixture(snapshot_ack=ack))
    assert not any(e["event"] == "send" for e in events)
    assert runtime["guard_receipts"]
    assert not boundary["window_complete"]


def test_multiple_batches_use_current_generation_window_and_actual_send_inventory(tmp_path):
    program = _program()
    program["actions"].insert(-1, _action("publish_again", "publish_batch", "w0", 1,
                                         count=2, start_sequence=3, interval_sec=0.0))
    _, events, projection, _ = _run(tmp_path, program)
    assert len([e for e in events if e["event"] == "topology_delivery_window_open"]) == 1
    close = _closed(events)
    assert close["expected_sequences"] == list(range(5))
    assert close["sentinel_sequence"] == 4 and close["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"


def test_no_send_and_no_reader_never_fabricate_completed_windows(tmp_path):
    program = _program()
    program["actions"] = [a for a in program["actions"] if a["op"] != "publish_batch"]
    result, events, projection, boundary = _run(tmp_path, program)
    assert not any(e["event"] in {"send", "topology_delivery_window_open", "topology_delivery_window_closed"}
                   for e in events)
    claims = [e for e in events if e["event"] == "topology_delivery_no_sends"]
    assert len(claims) == 1 and claims[0]["schema"] == "r2f.topology-delivery-no-sends/v1"
    assert claims[0]["closure_status"] == "not_applicable_no_sends"
    assert boundary["lifecycle_complete"] and boundary["observation_complete"] and boundary["window_complete"]
    assert claims[0]["event_index"] in boundary["delivery_window_refs"]
    seal = events[claims[0]["inventory_end_ref"]]
    assert seal["event"] == "topology_send_inventory_end"
    for refs in claims[0]["snapshot_refs"].values():
        chain = [events[i] for i in refs]
        assert [e["event"] for e in chain] == ["topology_snapshot_requested", "match_snapshot", "graph_snapshot", "command_done"]
        assert seal["event_index"] < refs[0] < refs[1] < refs[2] < refs[3] < claims[0]["event_index"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "no_obligation"
    proof = projection["receipt"]["after"]["component_refs"]["/pairs/w0/r0/delivery_closure"]
    assert proof["status"] == "observed"
    assert proof["detail"]["applicability_kind"] == "not_applicable_no_sends"
    assert proof["detail"]["no_sends_ref"] in proof["refs"]
    assert result["runtime_status"] == "completed"


def test_reader_generation_replacement_does_not_borrow_old_sentinel(tmp_path):
    program = _program()
    program["actions"].insert(-1, _action("replace_reader", "recreate", "r0", 2))
    _, events, projection, boundary = _run(tmp_path, program, backend=WindowFixture(omit={2}))
    close = _closed(events)
    assert close["reader_generation"] == 1 and not close["window_complete"]
    assert "endpoint_lifecycle_changed" in close["reasons"]
    assert not boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "unknown"


def test_payload_generation_contradiction_is_incomplete_not_candidate(tmp_path):
    _, events, projection, boundary = _run(
        tmp_path, backend=WindowFixture(publisher_gid_available=False, corrupt_payload=True))
    assert not _closed(events)["window_complete"] and not boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "unknown"


def test_publish_tool_timeout_stays_incomplete_even_if_sentinel_arrived(tmp_path):
    _, events, _, boundary = _run(tmp_path, backend=WindowFixture(fail_publish=True))
    assert any(e["event"] == "receive" and e["send_sequence"] == 2 for e in events)
    assert not _closed(events)["window_complete"] and not boundary["window_complete"]


def test_abnormal_program_exit_closes_active_windows_incomplete(tmp_path):
    runtime, events, _, boundary = _run(tmp_path, backend=WindowFixture(abort_snapshot=True))
    assert runtime["status"] == "aborted"
    assert not _closed(events)["window_complete"] and not boundary["window_complete"]
    assert any(e["event"] == "runtime_abort" for e in events)


def test_final_observation_pumps_actual_pending_receive_not_just_snapshot_ack(tmp_path):
    _, events, projection, _ = _run(tmp_path, backend=WindowFixture(defer_receive=True))
    assert _closed(events)["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"


def test_live_worker_graph_outer_gid_is_optional_not_a_publish_gate(tmp_path):
    class PublicGraphShape(WindowFixture):
        def _emit(self, event, **fields):
            # Real public worker graph_snapshot has identity + graph rows, but
            # no outer endpoint_gid. The command-bound match supplies that GID.
            if event == "graph_snapshot":
                fields.pop("endpoint_gid", None)
            return super()._emit(event, **fields)
    runtime, events, projection, boundary = _run(tmp_path, backend=PublicGraphShape())
    assert runtime["status"] == "executed"
    assert _closed(events)["window_complete"] and boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"


def test_worker_alias_writer2_is_in_raw_active_writer_inventory(tmp_path):
    class WorkerAliases(WindowFixture):
        def _emit(self, event, **fields):
            if fields.get("logical_id") == "w1":
                fields["role"] = "writer2"
            return super()._emit(event, **fields)
    program = _program(writers=2)
    runtime, events, _, boundary = _run(tmp_path, program, WorkerAliases(publisher_gid_available=False))
    assert runtime["status"] == "executed"
    assert any(e["event"] == "send" and e["logical_id"] == "w1" for e in events)
    assert not boundary["window_complete"]
    closes = [e for e in events if e["event"] == "topology_delivery_window_closed"]
    assert closes and not any(c["window_complete"] for c in closes)
    assert all("active_writer_generation_not_raw_unique" in c["reasons"] for c in closes)


def test_writer_without_reader_keeps_original_send_actions(tmp_path):
    program = _program(readers=0)
    # Keep the three-send assertion with an explicit three-send input. The
    # generator's two-send default is not a license to truncate this action.
    batch, = [a for a in program["actions"] if a["op"] == "publish_batch"]
    batch["params"]["count"] = 3
    runtime, events, _, boundary = _run(tmp_path, program)
    assert runtime["status"] == "executed"
    assert len([e for e in events if e["event"] == "send"]) == 3
    assert not any(e["event"] in {"receive", "topology_delivery_window_open",
                                 "topology_delivery_window_closed"} for e in events)
    assert not boundary["window_complete"] and boundary["delivery_window_refs"] == []
    assert "no_current_pair_delivery_window" in boundary["limitations"]
    assert runtime["runtime_status"] == "completed_with_unknown"
    missing, = [row for row in runtime["unknowns"] if row["reason"] == "delivery_window_incomplete"]
    assert missing["delivery_status"] == "no_obligation"
    assert missing["raw_event_ref"] == boundary["event_index"]


def test_failed_cleanup_adds_incomplete_terminal_boundary_without_rewriting_close(tmp_path):
    class FailedCleanup(WindowFixture):
        def close(self):
            super().close()
            return {"closed": False, "fixture": True, "reason": "fixture_cleanup_failure"}
    runtime, events, _, boundary = _run(tmp_path, backend=FailedCleanup())
    assert runtime["status"] == "aborted"
    # Earlier finite-window evidence stays immutable. Terminal boundary is not
    # allowed to assert complete after the actual abnormal cleanup.
    assert _closed(events)["window_complete"]
    assert boundary["phase"] == "aborted_cleanup" and not boundary["window_complete"]
    assert any(e["event"] == "runtime_abort" and e.get("phase") == "cleanup" for e in events)


def test_content_filter_capability_is_conditional_initial_only_not_rmw_pass():
    from r2f.runtime.topology_pair import topology_capabilities
    from r2f.runtime.topology_worker import initial_content_filter_capability
    capability = topology_capabilities()["supported_bypass_fields"]["content_filter"]
    assert capability == initial_content_filter_capability()
    assert capability["status"] == "conditional" and capability["scope"] == "initial_only"
    assert capability["roles"] == ["reader"]
    assert not any(capability[k] for k in ("sample_filter_effect", "dynamic_update", "lease"))
    assert "rmw_acceptance" in capability["requirements"]


def test_partial_send_inventory_cannot_close_even_when_last_actual_send_received(tmp_path):
    class PartialSend(WindowFixture):
        def publish_batch(self, handle, sequences, *args, **kwargs):
            return super().publish_batch(handle, sequences[:-1], *args, **kwargs)
    _, events, projection, boundary = _run(tmp_path, backend=PartialSend())
    close = _closed(events)
    assert close["received_sequences"] == [0, 1]
    assert "requested_publish_inventory_incomplete" in close["reasons"]
    assert not close["window_complete"] and not boundary["window_complete"]
    assert not close["gap_evidence"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "unknown"


def test_pre_reader_send_is_not_borrowed_by_later_window(tmp_path):
    program = _program()
    program["actions"] = [
        _action("writer_first", "create", "w0", 1),
        _action("before_reader", "publish_batch", "w0", 1, count=3, start_sequence=0, interval_sec=0.0),
        _action("reader_late", "create", "r0", 1),
        _action("after_reader", "publish_batch", "w0", 1, count=2, start_sequence=3, interval_sec=0.0),
        _action("final", "snapshot")]
    _, events, projection, boundary = _run(tmp_path, program)
    close = _closed(events)
    assert close["expected_sequences"] == [3, 4]
    assert close["window_complete"] and boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"


def test_new_generation_delivery_cannot_erase_old_interrupted_window(tmp_path):
    program = _program()
    program["actions"][-1:-1] = [
        _action("replace_reader", "recreate", "r0", 2),
        _action("second_generation_data", "publish_batch", "w0", 1,
                count=2, start_sequence=3, interval_sec=0.0)]
    _, events, projection, boundary = _run(tmp_path, program, backend=WindowFixture(omit={2}))
    closes = [e for e in events if e["event"] == "topology_delivery_window_closed"]
    assert [(e["reader_generation"], e["window_complete"]) for e in closes] == [(1, False), (2, True)]
    assert not boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == "closed"
    assert projection["state_after"]["observation_completeness"] == "incomplete"


@pytest.mark.parametrize("fault", ["old_clock", "bad_generation", "malformed_graph", "invalid_ack_clock"])
def test_malformed_or_stale_snapshot_is_not_a_fresh_open_gate(tmp_path, fault):
    class SnapshotFault(WindowFixture):
        def _emit(self, event, **fields):
            if fields.get("logical_id") == "r0":
                if fault == "old_clock" and event == "topology_snapshot_requested":
                    fields["monotonic_ns"] = 1
                elif fault == "bad_generation" and event == "command_done":
                    fields["endpoint_generation"] = "wrong-worker"
                elif fault == "malformed_graph" and event == "graph_snapshot":
                    fields["publishers"] = [None]
                elif fault == "invalid_ack_clock" and event == "command_done":
                    fields["monotonic_ns"] = None
            return super()._emit(event, **fields)
    runtime, events, _, boundary = _run(tmp_path, backend=SnapshotFault())
    assert runtime["status"] == "executed"  # A tool-rejected publish does not erase the remaining API actions.
    assert not any(e["event"] in {"send", "topology_delivery_window_open"} for e in events)
    assert any(r["kind"] == "tool" for r in runtime["guard_receipts"])
    assert not boundary["window_complete"]
