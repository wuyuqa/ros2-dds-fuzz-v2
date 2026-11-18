"""Offline producer -> unchanged projector -> feedback exclusion contract.

All kernel/network inputs here are explicitly injected fixture measurements.
No namespace, ROS process, real network command, campaign or replay is started.
The new runtime code still writes every row to immutable JSONL before consume.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.topology_state_projection import project_topology_state
from r2f.core.state_signature import observation_edge
from r2f.core.topology_state import topology_edge_id, topology_state_id
from r2f.modules.s1 import topology_model as model
from r2f.runtime import topology_pair
from r2f.runtime.topology_environment_observation import TopologyEnvironmentObservation
from r2f.runtime.topology_worker import OfflineTopologyBackend

from test_topology_environment_observation import FixtureProbe

QOS = {"reliability": "reliable", "durability": "volatile", "liveliness": "automatic",
       "deadline_ns": 9223372036854775807, "liveliness_lease_duration_ns": 9223372036854775807}


class MeasuredFixtureBackend(OfflineTopologyBackend):
    """Named fixture alters worker replies at production, never saved raw."""
    def __init__(self, *, defect=None, omit=()):
        super().__init__()
        self.defect, self.omit = defect, set(omit)
        self.snapshot_ids = set()

    def _emit(self, event, **fields):
        fields = deepcopy(fields)
        fields["fixture"] = True
        eid = fields.get("logical_id")
        if event == "topology_snapshot_requested":
            self.snapshot_ids.add((eid, fields.get("command_id")))
        if event == "graph_snapshot":
            for key in ("publishers", "subscriptions"):
                for row in fields[key]:
                    row["qos"] = deepcopy(QOS)
            if self.defect == "foreign_graph" and eid == "r0":
                fields["publishers"].append({"endpoint_gid": "foreign", "topic_type": "std_msgs/msg/String", "qos": deepcopy(QOS)})
        if event == "match_snapshot" and eid == "r0" and self.defect == "aggregate_count":
            fields["matched_count"] = 2
        if (event == "command_done" and eid == "r0"
                and (eid, fields.get("command_id")) in self.snapshot_ids):
            if self.defect == "missing_ack":
                return {"event": event, **fields}
            if self.defect == "duplicate_ack":
                super()._emit(event, **fields)
            if self.defect == "wrong_generation":
                fields["generation"] = 19
            if self.defect == "wrong_ticks":
                fields["process_start_ticks"] += 1
            if self.defect == "wrong_command":
                fields["command_id"] = "foreign-ack"
            if self.defect == "wrong_run":
                fields["run_id"] = "different-run"
        if event == "receive" and fields.get("send_sequence") in self.omit:
            return {"event": event, **fields}
        return super()._emit(event, **fields)


def run_fixture(tmp_path, monkeypatch, *, backend=None, probe=None, program=None):
    probe = probe if probe is not None else FixtureProbe()
    program = program or model.generate_program(7, writers=1, readers=1)
    monkeypatch.setattr(topology_pair, "TopologyEnvironmentObservation",
                        lambda backend, events, emit: TopologyEnvironmentObservation(backend, events, emit, _probe=probe))
    runtime = topology_pair.run_topology(program, {"backend": "offline_fake", "domain_id": 191,
                                                  "run_id": "n-observation-fixture"},
                                         tmp_path / "runtime", backend=backend or MeasuredFixtureBackend())
    raw = Path(runtime["raw_path"])
    saved = raw.read_bytes()
    events = [json.loads(line) for line in saved.splitlines()]
    assert events == runtime["events"]
    assert hashlib.sha256(saved).hexdigest() == runtime["raw_sha256"]
    boundaries = [e for e in events if e["event"] == "topology_state_boundary"]
    projection = project_topology_state(program, raw, before_ref=boundaries[0]["event_index"],
                                        after_ref=boundaries[-1]["event_index"],
                                        expected_sha256=runtime["raw_sha256"])
    assert not runtime["real_execution"] and not projection["eligible"]
    assert projection["receipt"]["fixture"] and not projection["receipt"]["execution_evidence"]
    assert not runtime["candidate_eligible"] and not runtime["sealed"] and not runtime["mature"]
    assert raw.read_bytes() == saved
    return runtime, events, boundaries, projection


def test_canonical_pair_produces_consumed_neutral_and_named_isolation(tmp_path, monkeypatch):
    runtime, events, boundaries, projection = run_fixture(tmp_path, monkeypatch)
    assert runtime["status"] == "executed"
    assert projection["receipt"]["status"] == "observed"
    before, after = projection["state_before"], projection["state_after"]
    assert before["observation_completeness"] == after["observation_completeness"] == "complete"
    assert before["network_phase"] == after["network_phase"] == "normal"
    assert before["pairs"][0]["observed_match"] == "not_applicable"
    assert after["pairs"][0]["observed_match"] == "bidirectional"
    assert after["pairs"][0]["delivery_closure"] == "closed"
    delivery_detail = projection["receipt"]["after"]["component_refs"]["/pairs/w0/r0/delivery_closure"]["detail"]
    assert delivery_detail["observed_match_used"] == "bidirectional"
    assert boundaries[0]["observation_complete"] and boundaries[0]["window_complete"]
    assert not any(e["event"] == "send" for e in events[:boundaries[0]["event_index"]])
    proofs = [e for e in events if e["event"] == "topology_controlled_isolation"]
    assert len(proofs) >= 2  # delivery-open and final snapshots each get fresh proof
    proof = proofs[-1]
    opened = next(e for e in events if e["event"] == "topology_delivery_window_open")
    first = proofs[0]
    assert max(first["snapshot_refs"]) < first["inventory_ref"] < first["event_index"] < opened["event_index"]
    inventory = events[proof["inventory_ref"]]
    assert inventory["inventory_complete"] and len(inventory["inventory"]["workers"]) == 2
    assert proof["fixture"] and not proof["execution_evidence"]
    for graph_ref in proof["graph_refs"]:
        graph = events[graph_ref]
        assert graph["event"] == "graph_snapshot"
        ack_ref = next(ref for ref in proof["snapshot_refs"] if events[ref]["event"] == "command_done"
                       and events[ref]["logical_id"] == graph["logical_id"])
        assert graph_ref < ack_ref < proof["inventory_ref"] < proof["event_index"] < boundaries[-1]["event_index"]
    for side in ("before", "after"):
        refs = projection["receipt"][side]["component_refs"]["/network_phase"]["refs"]
        assert all(ref["path"] == runtime["raw_path"] and ref["sha256"] == runtime["raw_sha256"] for ref in refs)
    # State/edge identities are valid inputs, not execution/feedback credit.
    assert len(topology_edge_id(topology_state_id(before), "snapshot", "neutral", topology_state_id(after))) == 64
    record = {"content_sha256": "a" * 64, "execution_stage": "judged", "actual_run": False,
              "raw_path": runtime["raw_path"], "raw_sha256": runtime["raw_sha256"],
              "surface_id": "topology", "boundary_class": "neutral", "operation_family": "snapshot",
              "parameter_class": "1w1r", "typed_facts": {
                  "environment_healthy": False, "process_healthy": True, "preconditions_reached": False,
                  "endpoint_identity_complete": True, "window_complete": True, "observation_complete": True,
                  "state_before": before, "state_after": after, "state_projection_receipt": projection["receipt"]}}
    log = FeedbackLog()
    row = log.record(record)
    assert not row["coverage"] and row["edge_id"] is None and observation_edge(record) is None
    assert FeedbackLog.from_events(json.loads(json.dumps(log.events))).identity() == log.identity()


@pytest.mark.parametrize("defect", ["foreign_graph", "aggregate_count", "missing_ack", "duplicate_ack",
                                     "wrong_generation", "wrong_ticks", "wrong_command", "wrong_run"])
def test_no_isolation_if_peer_or_ack_is_not_exact(tmp_path, monkeypatch, defect):
    runtime, events, boundaries, projection = run_fixture(tmp_path, monkeypatch, backend=MeasuredFixtureBackend(defect=defect))
    assert not any(e["event"] == "topology_controlled_isolation" for e in events)
    assert projection["state_after"]["observation_completeness"] == "incomplete"
    assert projection["state_after"]["pairs"][0]["observed_match"] == "unknown"
    assert events and Path(runtime["raw_path"]).exists()  # failure retained, never patched green


@pytest.mark.parametrize("writers,readers", [(2, 1), (1, 2), (2, 2)])
def test_normal_network_never_turns_n_counts_into_directed_peer_edges(tmp_path, monkeypatch, writers, readers):
    _, events, _, projection = run_fixture(tmp_path, monkeypatch,
        program=model.generate_program(7, writers=writers, readers=readers))
    assert projection["state_after"]["network_phase"] == "normal"
    assert not any(e["event"] == "topology_controlled_isolation" for e in events)
    assert all(pair["observed_match"] == "unknown" for pair in projection["state_after"]["pairs"])
    assert projection["state_after"]["observation_completeness"] == "incomplete"


@pytest.mark.parametrize("failure", ["namespace", "inventory", "network"])
def test_missing_independent_measurement_leaves_only_that_component_unknown(tmp_path, monkeypatch, failure):
    _, events, boundaries, projection = run_fixture(tmp_path, monkeypatch, probe=FixtureProbe(failure=failure))
    assert projection["state_before"]["observation_completeness"] == "incomplete"
    if failure == "network":
        assert projection["state_after"]["network_phase"] == "unknown"
        assert projection["state_after"]["pairs"][0]["observed_match"] == "bidirectional"
    else:
        assert not boundaries[0]["observation_complete"]
        assert projection["state_after"]["pairs"][0]["observed_match"] == "unknown"
        assert not any(e["event"] == "topology_controlled_isolation" for e in events)
        if failure == "inventory":
            assert projection["state_after"]["network_phase"] == "normal"


def test_match_is_not_delivery_and_trailing_missing_does_not_close(tmp_path, monkeypatch):
    _, events, _, projection = run_fixture(tmp_path, monkeypatch, backend=MeasuredFixtureBackend(omit=(2,)))
    assert any(e["event"] == "topology_controlled_isolation" for e in events)
    pair = projection["state_after"]["pairs"][0]
    assert pair["observed_match"] == "bidirectional" and pair["delivery_closure"] == "unknown"
    assert projection["state_after"]["observation_completeness"] == "incomplete"


def test_middle_missing_has_later_sentinel_without_fabricated_receive(tmp_path, monkeypatch):
    _, events, _, projection = run_fixture(tmp_path, monkeypatch, backend=MeasuredFixtureBackend(omit=(1,)))
    pair = projection["state_after"]["pairs"][0]
    assert pair["observed_match"] == "bidirectional" and pair["delivery_closure"] == "closed_with_gap"
    assert not any(e["event"] == "receive" and e.get("send_sequence") == 1 for e in events)
    assert any(e["event"] == "receive" and e.get("send_sequence") == 2 for e in events)


def test_uniform_route_and_fact_producer_consume_new_measurements(tmp_path, monkeypatch):
    from r2f.runtime.ros2_pair import run_pair
    from r2f.campaign.topology_adapter import facts_from_events
    from test_topology_runtime_route import config_for
    _, config = config_for("timing.shift")
    monkeypatch.setattr(topology_pair, "TopologyEnvironmentObservation",
                        lambda backend, events, emit: TopologyEnvironmentObservation(
                            backend, events, emit, _probe=FixtureProbe()))
    runtime = run_pair(config, tmp_path / "canonical-route", topology_backend=MeasuredFixtureBackend())
    assert runtime["status"] == "executed" and runtime["application_audit"]["status"] == "fixture_application_observed"
    facts = facts_from_events(runtime["events"], runtime, config)
    assert facts["diagnostic_valid"]
    assert facts["state_before"]["network_phase"] == facts["state_after"]["network_phase"] == "normal"
    assert facts["state_after"]["pairs"][0]["observed_match"] == "bidirectional"
    assert facts["state_projection_receipt"]["status"] == "observed"
    assert facts["fixture"] and not facts["execution_evidence"]
    assert not facts["environment_healthy"] and not facts["candidate_eligible"]
    # The canonical facts and raw are fed intact into feedback. Fixture flags
    # are not rewritten to obtain a fake edge or execution credit.
    record = {"execution_stage": "judged", "actual_run": False, "typed_facts": facts,
              "content_sha256": "b" * 64, "surface_id": "topology", "boundary_class": "neutral",
              "raw_path": runtime["raw_path"], "raw_sha256": runtime["raw_sha256"]}
    feedback = FeedbackLog().record(record)
    assert feedback["edge_id"] is None and not feedback["coverage"]
