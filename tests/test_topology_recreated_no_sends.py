"""Explicit regenerated-generation no-send inventory; offline, never DDS proof."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.topology_state_projection import project_topology_state
from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import run_topology
from test_topology_lifecycle_window_retirement import RetirementBackend
from test_topology_pair import action, endpoint, program

V2 = "r2f.topology-delivery-no-sends/v2"


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("no ROS/DDS backend preparation")
    monkeypatch.setattr("r2f.runtime.topology_worker.RosTopologyBackend.prepare", forbidden)


def authored(*, missing_old=False, no_old_publish=False, generation=2, new_publish=False):
    endpoints = [endpoint(f"{role}{i}", "writer" if role == "w" else "reader",
                          topic="/retired/" + str(i % 2))
                 for role in ("w", "r") for i in range(4)]
    acts = [action("create_" + e["logical_id"], "create", e["logical_id"], 1) for e in endpoints]
    if not no_old_publish:
        acts += [action("publish_" + f"w{i}", "publish_batch", f"w{i}", 1,
                        count=2, start_sequence=0, interval_sec=0.0) for i in range(4)]
    for gen in range(2, generation + 1):
        acts += [action("recreate_w0_" + str(gen), "recreate", "w0", gen)]
    if new_publish:
        acts += [action("publish_current", "publish_batch", "w0", generation,
                        count=2, start_sequence=0, interval_sec=0.0)]
    return program(endpoints, acts)


def project(result, prog):
    events = [json.loads(x) for x in Path(result["raw_path"]).read_text().splitlines()]
    bs = [e for e in events if e["event"] == "topology_state_boundary"]
    return project_topology_state(prog, result["raw_path"], before_ref=bs[0]["event_index"],
                                  after_ref=bs[-1]["event_index"], expected_sha256=result["raw_sha256"])


def produce(tmp_path, *, fault=None, **options):
    prog = authored(**options)
    result = run_topology(prog, {"backend": "offline_fake", "run_id": "recreated-no-sends-fixture",
                               "domain_id": 192}, tmp_path / "runtime", backend=RetirementBackend(fault))
    assert not result["real_execution"] and not result["candidate_eligible"]
    assert not result["mature"] and not result["sealed"]
    return result, prog, project(result, prog)


def w0_pairs(projection):
    return [p for p in projection["state_after"]["pairs"] if p["writer"] == "w0"]


@pytest.mark.parametrize("generation", [2, 3])
def test_recreated_current_generation_without_publish_has_independent_v2_proof(tmp_path, generation):
    result, prog, projection = produce(tmp_path, generation=generation)
    claims = [e for e in result["events"] if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0"]
    assert len(claims) == 4 and all(e["schema"] == V2 and e["writer_generation"] == generation for e in claims)
    closed = [e for e in result["events"] if e["event"] == "topology_delivery_window_closed"]
    assert len(closed) == 16 and all(e["window_complete"] for e in closed)
    assert not any(e["event"] == "send" and e.get("logical_id") == "w0" and e.get("generation") == generation for e in result["events"])
    assert all(p["delivery_closure"] == "no_obligation" for p in w0_pairs(projection))
    for r in range(4):
        proof = projection["receipt"]["after"]["component_refs"][f"/pairs/w0/r{r}/delivery_closure"]
        assert proof["detail"]["applicability_kind"] == "not_applicable_no_sends"
        assert proof["detail"]["no_sends_schema"] == V2
        assert proof["detail"]["prior_window_close_refs"]
    assert result["runtime_status"] == "completed" and not result["unknowns"]
    assert not projection["eligible"]  # offline fixture remains disqualified


def test_recreated_unsent_writer_keeps_entire_lifecycle_inventory(tmp_path):
    result, _, projection = produce(tmp_path, no_old_publish=True)
    assert all(p["delivery_closure"] == "no_obligation" for p in w0_pairs(projection))
    assert not any(e["event"] == "send" for e in result["events"])
    assert any(e["event"] == "topology_delivery_no_sends" and e.get("schema") == V2 for e in result["events"])


@pytest.mark.parametrize("fault", ["missing_sentinel", "late_receive"])
def test_v2_none_cannot_erase_real_old_generation_unclosed_obligation(tmp_path, fault):
    result, _, projection = produce(tmp_path, fault=fault)
    assert not any(e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0" for e in result["events"])
    assert all(p["delivery_closure"] == "unknown" for p in w0_pairs(projection))
    assert result["runtime_status"] == "completed_with_unknown"


def test_current_generation_publication_uses_real_windows_not_no_send_claim(tmp_path):
    result, _, projection = produce(tmp_path, new_publish=True)
    assert not any(e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0" for e in result["events"])
    assert any(e["event"] == "send" and e.get("logical_id") == "w0" and e.get("generation") == 2 for e in result["events"])


def rewrite_fixture(result, mutate):
    events = deepcopy(result["events"])
    mutate(events)
    # Recommit the mutated prefix so independent gates, not just seal SHA, are tested.
    for seal in (e for e in events if e["event"] == "topology_send_inventory_end"):
        prefix = events[:seal["event_index"]]
        digest = hashlib.sha256(json.dumps(prefix, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        seal.update(inventory_count=len(prefix), inventory_sha256=digest)
        for e in events:
            if e["event"] == "topology_delivery_no_sends":
                e.update(inventory_count=len(prefix), inventory_sha256=digest)
    data = "".join(json.dumps(e, sort_keys=True) + "\n" for e in events)
    Path(result["raw_path"]).write_text(data)
    result["events"] = events
    result["raw_sha256"] = hashlib.sha256(data.encode()).hexdigest()


@pytest.mark.parametrize("fault", ["v1_forgery", "wrong_generation", "history_ref", "retire_missing", "retire_identity", "reused_gid", "multiple_create", "action_generation", "phase_end", "snapshot_ack", "current_late_send", "ambiguous_send", "cleanup_identity", "runtime_end", "retire_exit_missing", "retire_exit_identity", "cleanup_after_end", "ambiguous_receive", "create_role", "create_chronology", "late_create", "late_retire"])
def test_consumer_rejects_tampered_recreated_inventory_even_with_resealed_prefix(tmp_path, fault):
    result, prog, _ = produce(tmp_path)
    assert any(e["event"] == "topology_delivery_no_sends" and e.get("schema") == V2 for e in result["events"])
    def mutate(events):
        claims = [e for e in events if e["event"] == "topology_delivery_no_sends" and e.get("writer") == "w0"]
        creation = next(e for e in events if e["event"] == "endpoint_created" and e.get("logical_id") == "w0" and e.get("generation") == 2)
        retire = next(e for e in events if e["event"] in {"endpoint_destroyed", "topology_endpoint_destroyed"} and e.get("logical_id") == "w0")
        if fault == "v1_forgery":
            for c in claims: c["schema"] = "r2f.topology-delivery-no-sends/v1"
        elif fault == "wrong_generation":
            for c in claims: c["writer_generation"] = 1
        elif fault == "history_ref":
            for c in claims: c["generation_history_refs"] = {}
        elif fault == "retire_missing": retire["event"] = "uncaptured_retirement"
        elif fault == "retire_identity": retire["process_start_ticks"] += 1
        elif fault == "reused_gid": creation["endpoint_gid"] = next(e["endpoint_gid"] for e in events if e["event"] == "endpoint_created" and e.get("logical_id") == "w0" and e.get("generation") == 1)
        elif fault == "multiple_create":
            e = next(e for e in events if e["event"] == "topology_generation_retired" and e.get("logical_id") == "w0")
            e.update({k:v for k,v in creation.items() if k not in {"event_index", "monotonic_ns", "time_ns"}})
        elif fault == "action_generation":
            next(e for e in events if e["event"] == "topology_action_done" and e.get("action", {}).get("op") == "recreate")["action"]["generation"] = 3
        elif fault == "phase_end": next(e for e in events if e["event"] == "topology_send_inventory_end")["event"] = "uncaptured_phase_end"
        elif fault == "snapshot_ack":
            ref = claims[0]["snapshot_refs"]["w0"][-1]
            events[ref]["event"] = "uncaptured_ack"
        elif fault in {"current_late_send", "ambiguous_send"}:
            e = next(e for e in events if e["event"] == "topology_execution_metadata")
            e.update(event="send", logical_id="w0" if fault == "current_late_send" else "unknown-writer", generation=2, send_sequence=99, publish_returned=True,
                     **{k:creation[k] for k in ("pid", "process_start_ticks", "endpoint_generation")})
        elif fault == "cleanup_identity":
            e = next(e for e in events if e["event"] == "topology_worker_exit" and e.get("logical_id") == "w0" and e.get("generation") == 2)
            e["process_start_ticks"] += 1
        elif fault == "runtime_end": events[-1]["cleanup_complete"] = False
        elif fault in {"retire_exit_missing", "retire_exit_identity"}:
            e = next(e for e in events if e["event"] == "topology_worker_exit" and e.get("logical_id") == "w0" and e.get("generation") == 1)
            if fault == "retire_exit_missing": e["event"] = "uncaptured_exit"
            else: e["pid"] += 1
        elif fault == "cleanup_after_end":
            e = next(e for e in events if e["event"] == "topology_worker_exit" and e.get("logical_id") == "w0" and e.get("generation") == 2)
            e["monotonic_ns"] = events[-1]["monotonic_ns"] + 1
        elif fault == "ambiguous_receive":
            e = next(e for e in events if e["event"] == "topology_execution_metadata")
            e.update(event="receive", payload_writer_id="w0", payload_writer_generation=2,
                     payload_sha256="0" * 64, payload_run_id=creation["run_id"])
        elif fault in {"late_create", "late_retire"}:
            e = next(e for e in events if e["event"] == "topology_execution_metadata")
            e.update({k:v for k,v in creation.items() if k not in {"event_index", "monotonic_ns", "time_ns"}})
            e["event"] = "endpoint_created" if fault == "late_create" else "topology_endpoint_destroyed"
        elif fault == "create_role": creation["role"] = "reader"
        elif fault == "create_chronology": creation["monotonic_ns"] = 1
    rewrite_fixture(result, mutate)
    projection = project(result, prog)
    assert all(p["delivery_closure"] == "unknown" for p in w0_pairs(projection)), (fault, projection)
    assert not projection["eligible"]


@pytest.mark.parametrize("fault", ["late_send", "exit_missing", "publish_attempt"])
def test_shutdown_cannot_upgrade_provisional_v2_after_unproven_generation_end(tmp_path, fault):
    class ShutdownFault(RetirementBackend):
        def close(self):
            handle = self.handles["w0"]
            if fault == "late_send":
                self.publish_batch(handle, [99], 0.0, "late_shutdown_fixture")
            if fault == "publish_attempt":
                self._emit("publish_batch_done", logical_id="w0", generation=2, count=0)
            receipt = super().close()
            return receipt

        def _emit(self, kind, **fields):
            if (fault == "exit_missing" and kind == "topology_worker_exit"
                    and fields.get("logical_id") == "w0" and fields.get("generation") == 2):
                return {"event": kind, **fields}
            return super()._emit(kind, **fields)

    prog = authored()
    result = run_topology(prog, {"backend": "offline_fake", "run_id": "shutdown-v2-fixture", "domain_id": 192},
                          tmp_path / "runtime", backend=ShutdownFault())
    assert any(e["event"] == "topology_delivery_no_sends" and e.get("schema") == V2 for e in result["events"])
    assert result["runtime_status"] == "completed_with_unknown"
    assert any(u["reason"] == "no_sends_generation_end_incomplete" for u in result["unknowns"])
    projection = project(result, prog)
    assert all(p["delivery_closure"] == "unknown" for p in w0_pairs(projection))
    assert not projection["eligible"] and not result["candidate_eligible"]
