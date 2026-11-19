"""Producer retirement contracts; OfflineTopologyBackend only, never DDS evidence."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.topology_state_projection import project_topology_state
from r2f.runtime.topology_pair import _delivery_window_evidence, run_topology
from test_multipeer_no_obligation import DirectPeerContractFixture, prepared
from test_topology_pair import action, endpoint, program


def scenario(*, terminal="recreate", next_publish=False):
    endpoints = [endpoint("w0", "writer"), endpoint("w1", "writer"),
                 endpoint("r0", "reader"), endpoint("r1", "reader")]
    actions = [action("create_" + e["logical_id"], "create", e["logical_id"], 1)
               for e in endpoints]
    actions += [action("send_" + eid, "publish_batch", eid, 1,
                       count=2, start_sequence=0, interval_sec=0.0) for eid in ("w0", "w1")]
    actions += [action("retire_w0", terminal, "w0", 2 if terminal == "recreate" else 1)]
    if next_publish:
        actions += [action("send_g2", "publish_batch", "w0", 2,
                           count=2, start_sequence=0, interval_sec=0.0)]
    return program(endpoints, actions)


class RetirementBackend(DirectPeerContractFixture):
    """Real fixture event production, with explicitly named observation faults."""
    def __init__(self, fault=None, **kwargs):
        super().__init__(**kwargs)
        self.fault = fault
        self.pending = []
        self.snapshot_commands = set()

    def _emit(self, kind, **fields):
        command = (fields.get("logical_id"), fields.get("command_id"))
        if kind == "topology_snapshot_requested":
            self.snapshot_commands.add(command)
        if (kind == "command_done" and command in self.snapshot_commands
                and fields.get("logical_id") == "r0" and self.fault == "missing_ack"):
            return {"event": kind, **fields}
        if kind == "receive":
            if self.fault == "missing_sentinel" and fields.get("send_sequence") == 1:
                return {"event": kind, **fields}
            if self.fault == "late_receive":
                self.pending.append(deepcopy(fields))
                return {"event": kind, **fields}
        return super()._emit(kind, **fields)

    def destroy_endpoint(self, handle):
        result = super().destroy_endpoint(handle)
        # Intentionally captured only AFTER actual retirement, never backdated.
        while self.pending:
            super()._emit("receive", **self.pending.pop(0))
        return result


def run(tmp_path, *, fault=None, terminal="recreate", next_publish=False, prog=None, **backend_options):
    prog = scenario(terminal=terminal, next_publish=next_publish) if prog is None else prog
    result = run_topology(prog, {"backend": "offline_fake", "run_id": "lifecycle-retirement-fixture",
                               "domain_id": 192}, tmp_path / "runtime", backend=RetirementBackend(fault, **backend_options))
    events = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    assert events == result["events"]
    assert not result["real_execution"] and not result["candidate_eligible"]
    assert not result["mature"] and not result["sealed"]
    boundaries = [e for e in events if e["event"] == "topology_state_boundary"]
    projection = project_topology_state(prog, result["raw_path"],
        before_ref=boundaries[0]["event_index"], after_ref=boundaries[-1]["event_index"],
        expected_sha256=result["raw_sha256"])
    assert not projection["eligible"]
    return result, events, projection


def closes(events):
    return [e for e in events if e["event"] == "topology_delivery_window_closed"]


@pytest.mark.parametrize("terminal", ["destroy", "recreate"])
def test_retirement_closes_proven_windows_before_side_effect_not_just_endpoint(tmp_path, terminal):
    _, events, _ = run(tmp_path, terminal=terminal)
    retirement = next(e for e in events if e["event"] == "endpoint_destroyed")
    old = [e for e in closes(events) if e["writer_generation"] == 1]
    assert len(old) == 4
    assert all(e["window_complete"] and e["closure_status"] == "closed" for e in old)
    assert all(e["event_index"] < retirement["event_index"] for e in old)
    assert {e["writer"] for e in old} == {"w0", "w1"}
    assert not any(e["event"] == "topology_delivery_window_interrupted" for e in events)
    for close in old:
        assert close["sentinel_receive_ref"] < close["event_index"]
        assert close["sentinel_send_ref"] < close["event_index"]
        evidence = _delivery_window_evidence(events[close["open_ref"]], events,
                                             before_index=close["event_index"])
        assert evidence["window_complete"] and evidence["reasons"] == []


def test_closed_g1_history_is_not_final_g2_delivery_or_a_fake_publish(tmp_path):
    _, events, projection = run(tmp_path)
    historical = [e for e in closes(events) if e["writer"] == "w0"]
    assert historical and all(e["writer_generation"] == 1 and e["window_complete"] for e in historical)
    assert not any(e["event"] == "send" and e.get("logical_id") == "w0"
                   and e.get("generation") == 2 for e in events)
    pairs = projection["state_after"]["pairs"]
    current = [p for p in pairs if p["writer"] == "w0"]
    # Abstract pair state has no generation field; use its raw lifecycle token.
    created = [e for e in events if e["event"] == "endpoint_created" and e.get("logical_id") == "w0"]
    assert current and [e["generation"] for e in created] == [1, 2]
    # V2 is a fresh same-generation absence proof, NEVER old delivery as g2.
    assert all(p["delivery_closure"] == "no_obligation" for p in current)
    claims = [e for e in events if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0"]
    assert claims and all(e["schema"] == "r2f.topology-delivery-no-sends/v2" for e in claims)
    assert not any(e["writer_generation"] == 2 for e in closes(events) if e["writer"] == "w0")
    # Preserve the old UNKNOWN assertion for an actually unclosed old sentinel.
    _, broken, negative = run(tmp_path / "missing-sentinel", fault="missing_sentinel")
    assert any(not e["window_complete"] for e in closes(broken))
    current = [p for p in negative["state_after"]["pairs"] if p["writer"] == "w0"]
    assert all(p["delivery_closure"] == "unknown" for p in current)


@pytest.mark.parametrize("fault", ["missing_sentinel", "late_receive"])
def test_unclosed_obligation_stays_incomplete_no_late_retirement_backfill(tmp_path, fault):
    result, events, _ = run(tmp_path, fault=fault)
    old = [e for e in closes(events) if e["writer_generation"] == 1]
    assert old and all(not e["window_complete"] for e in old)
    assert all("endpoint_lifecycle_changed" in e["reasons"] for e in old)
    assert result["runtime_status"] == "completed_with_unknown"
    if fault == "late_receive":
        retirement = next(e for e in events if e["event"] == "endpoint_destroyed")
        receives = [e for e in events if e["event"] == "receive"]
        assert receives and all(e["event_index"] > retirement["event_index"] for e in receives)
        assert all(e["event_index"] < retirement["event_index"] for e in old)


def test_missing_snapshot_ack_cannot_become_a_complete_retired_window(tmp_path):
    result, events, _ = run(tmp_path, fault="missing_ack")
    assert not any(e["window_complete"] for e in closes(events))
    assert not any(e["event"] == "send" for e in events)
    assert any(e["code"] == "publish_batch_error" for e in result["guard_receipts"])
    assert not result["candidate_eligible"]


def test_new_generation_requires_own_publish_window_identity(tmp_path):
    _, events, _ = run(tmp_path, next_publish=True)
    old = [e for e in closes(events) if e["writer"] == "w0" and e["writer_generation"] == 1]
    new = [e for e in closes(events) if e["writer"] == "w0" and e["writer_generation"] == 2]
    assert old and new and all(e["window_complete"] for e in old + new)
    assert {e["open_ref"] for e in old}.isdisjoint(e["open_ref"] for e in new)
    for close in new:
        assert all(events[i]["generation"] == 2 for i in close["send_refs"])
        assert all(events[i]["generation"] == 1 for i in close["receive_refs"])


@pytest.mark.parametrize("reason", ["topic", "direct_none"])
def test_no_obligation_inventory_interruption_is_never_a_sentinel_closure(reason):
    data, opened = prepared(reason)
    events, _, _, _, emit = data
    from r2f.runtime.topology_pair import _delivery_no_obligation_basis
    basis = _delivery_no_obligation_basis(opened, events)
    assert basis is not None
    emit("topology_delivery_no_obligation", open_ref=opened["event_index"],
         **{k:opened[k] for k in ("writer", "reader", "writer_generation", "reader_generation")}, **basis)
    evidence = _delivery_window_evidence(opened, events)
    assert evidence["window_complete"] and evidence["closure_status"] == "no_obligation"
    # Snapshot NONE/inventory-dependent absence is not a delivered sentinel.
    emit("topology_delivery_window_interrupted", open_ref=opened["event_index"],
         reason="endpoint_lifecycle_changed")
    evidence = _delivery_window_evidence(opened, events)
    assert not evidence["window_complete"]
    assert "endpoint_lifecycle_changed" in evidence["reasons"]
    assert not evidence["send_refs"] and not evidence["receive_refs"]


def test_snapshot_none_history_is_not_reused_for_recreated_same_topic_writer(tmp_path):
    _, events, projection = run(tmp_path, match_policy="one_to_one")
    none = [e for e in closes(events) if e["closure_status"] == "no_obligation"]
    assert none and all(e["window_complete"] for e in none)
    retirement = next(e for e in events if e["event"] == "endpoint_destroyed")
    assert all(e["event_index"] < retirement["event_index"] for e in none)
    assert all(e["writer_generation"] == 1 for e in none)
    # Old NONE is not a g2 capture: independently bind fresh g2 creation/ACKs.
    assert not any(e["event"] == "topology_delivery_no_obligation"
                   and e.get("writer") == "w0" and e.get("writer_generation") == 2 for e in events)
    created = next(e for e in events if e["event"] == "endpoint_created"
                   and e.get("logical_id") == "w0" and e.get("generation") == 2)
    claims = [e for e in events if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0"]
    assert len(claims) == 2
    assert all(c["schema"] == "r2f.topology-delivery-no-sends/v2"
               and c["writer_creation_ref"] == created["event_index"]
               and all(ref > created["event_index"] for ref in c["snapshot_refs"]["w0"]) for c in claims)
    assert all(p["delivery_closure"] == "no_obligation" for p in projection["state_after"]["pairs"]
               if p["writer"] == "w0")
    # Missing fresh ACKs cannot inherit either old NONE or an absent g2 claim.
    _, events, projection = run(tmp_path / "missing-ack", match_policy="one_to_one", fault="missing_ack")
    assert not any(e["event"] in {"topology_delivery_no_sends", "topology_delivery_no_obligation"}
                   and e.get("writer") == "w0" and e.get("writer_generation") == 2 for e in events)
    assert all(p["delivery_closure"] == "unknown" for p in projection["state_after"]["pairs"]
               if p["writer"] == "w0")


def test_no_send_generation_change_keeps_strict_applicability_fence(tmp_path):
    prog = scenario()
    prog["actions"] = [a for a in prog["actions"] if a["op"] != "publish_batch"]
    result, events, projection = run(tmp_path, prog=prog)
    assert not any(e["event"] in {"send", "receive"} for e in events)
    assert not closes(events)  # no invented delivery capture for either generation
    claims = [e for e in events if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0"]
    assert claims and all(e["schema"] == "r2f.topology-delivery-no-sends/v2" for e in claims)
    assert result["runtime_status"] == "completed"
    assert all(p["delivery_closure"] == "no_obligation" for p in projection["state_after"]["pairs"]
               if p["writer"] == "w0")
    # V1 fence remains; v2 without complete fresh observations remains UNKNOWN.
    result, events, projection = run(tmp_path / "missing-ack", prog=prog, fault="missing_ack")
    assert not any(e["event"] == "topology_delivery_no_sends"
                   and e.get("writer") == "w0" for e in events)
    assert result["runtime_status"] == "completed_with_unknown"
    assert all(p["delivery_closure"] == "unknown" for p in projection["state_after"]["pairs"]
               if p["writer"] == "w0")


@pytest.mark.parametrize("new_data", [False, True])
def test_genuinely_missing_g1_sentinel_keeps_original_cross_generation_contract(tmp_path, new_data):
    from test_topology_delivery_windows import WindowFixture, _action, _program, _run
    prog = _program()
    tail = [_action("replace_reader", "recreate", "r0", 2)]
    if new_data:
        tail += [_action("second_generation_data", "publish_batch", "w0", 1,
                         count=2, start_sequence=3, interval_sec=0.0)]
    prog["actions"][-1:-1] = tail
    _, events, projection, boundary = _run(tmp_path, prog, backend=WindowFixture(omit={2}))
    windows = closes(events)
    expected = [(1, False), (2, True)] if new_data else [(1, False)]
    assert [(e["reader_generation"], e["window_complete"]) for e in windows] == expected
    assert "endpoint_lifecycle_changed" in windows[0]["reasons"]
    assert not boundary["window_complete"]
    assert projection["state_after"]["pairs"][0]["delivery_closure"] == ("closed" if new_data else "unknown")
    assert projection["state_after"]["observation_completeness"] == "incomplete"
