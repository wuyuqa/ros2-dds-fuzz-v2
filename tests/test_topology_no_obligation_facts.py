"""Offline producer -> persisted raw -> projector -> facts -> primary oracle.

Synthetic backend only, never real DDS evidence or candidate qualification.
Tampered bytes are new test artifacts, never edits to historical run artifacts.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.campaign.topology_adapter import facts_from_events
from r2f.campaign.topology_state_projection import project_topology_state
from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.modules.s1.topology_oracles import evaluate_topology
from r2f.runtime.topology_route import run_topology_route, TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
from test_multipeer_no_obligation import DirectPeerContractFixture
from test_topology_delivery_windows import WindowFixture


class ObservedGraph:
    """Explicit non-DDS observed-contract fixture, not config-derived facts.

    The stock offline backend intentionally exposes empty graph QoS. This
    separate API double supplies a fixed, known observed QoS contract to BOTH
    persisted events and returned snapshots; production adapter must read raw.
    """
    @staticmethod
    def graph_contract(fields):
        for key in ("publishers", "subscriptions"):
            for row in fields.get(key, []):
                row["qos"] = {"reliability": "reliable", "durability": "volatile",
                    "history": "keep_last", "depth": 64, "liveliness": "automatic",
                    "deadline_ns": 9223372036854775807,
                    "liveliness_lease_duration_ns": 9223372036854775807}
    def _emit(self, event, **fields):
        if event == "graph_snapshot": self.graph_contract(fields)
        return super()._emit(event, **fields)
    def snapshot(self, handles):
        result = super().snapshot(handles)
        for snap in result["snapshots"].values(): self.graph_contract(snap["graph_snapshot"])
        return result


class ObservedPeers(ObservedGraph, DirectPeerContractFixture):
    pass


@pytest.fixture(autouse=True)
def no_live_backend(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline contract must not prepare ROS")
    monkeypatch.setattr("r2f.runtime.topology_worker.RosTopologyBackend.prepare", forbidden)


def config_for(program):
    plan = make_topology_plan(program, [{"operator_id": "action.insert"}], rng_seed=5)
    identity = {"input_identity_schema": INPUT_SCHEMA_V2, "target_id": TOPOLOGY_TARGET,
                "target_revision": 0, "mutation_kind": "endpoint_program", "carrier": "ros2_endpoint",
                "mutation_parameters": {}, "rmw": "rmw_cyclonedds_cpp", "repeat": 0,
                "seed": 5, "arm": "mutated", "mutation_layer_order": ["endpoint_program"],
                "input_layer_receipts": topology_input_layer_receipts(plan), "final_input": plan.final_config,
                "source_manifest": {}, "mutation_plan": plan.to_dict(),
                "operations": [op.to_dict() for op in plan.operations], "runtime_windows": [],
                "contract_hash": plan.contract_hash}
    return {**deepcopy(identity), "effective_input": deepcopy(identity),
            "effective_input_sha256": effective_input_hash(identity, schema=INPUT_SCHEMA_V2),
            "topology_program": deepcopy(plan.final_config["topology_program"]),
            "plan_hash": plan.plan_hash(), "runtime_capability": TOPOLOGY_CAPABILITY,
            "backend": "offline_fake", "run_id": "no-obligation-facts-offline", "domain_id": 191}


def produce(tmp_path, *, backend=None, writers=4, readers=4, split_topics=True, program=None):
    if program is None:
        program = model.generate_program(7, writers=writers, readers=readers)
        for endpoint in program["endpoints"]:
            group = int(endpoint["logical_id"][1:]) % 2 if split_topics else 0
            endpoint["topic"] = "/offline/actual/" + str(group)
    config = config_for(program)
    runtime = run_topology_route(config, tmp_path / "runtime", backend=backend or ObservedPeers())
    assert runtime["status"] == "executed", runtime.get("reason")
    assert not runtime["real_execution"] and not runtime["candidate_eligible"]
    return runtime, config


def project(runtime, config):
    boundaries = [e for e in runtime["events"] if e["event"] == "topology_state_boundary"]
    return project_topology_state(config["topology_program"], runtime["raw_path"],
        before_ref=boundaries[0]["event_index"], after_ref=boundaries[-1]["event_index"],
        expected_sha256=runtime["raw_sha256"])


def consume(runtime, config):
    facts = facts_from_events(runtime["events"], runtime, config)
    oracle = evaluate_topology(SimpleNamespace(target_id=TOPOLOGY_TARGET, arm="mutated"), facts)
    assert not facts["candidate_eligible"] and not facts["seal_eligible"]
    assert oracle["diagnostic_only"]
    return facts, oracle


def rewrite(runtime, transform):
    events = deepcopy(runtime["events"])
    transform(events)
    path = Path(runtime["raw_path"])
    path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
    runtime["events"] = events
    runtime["raw_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()


def test_4w4r_matched_and_cross_topic_keep_projected_no_obligation_and_pass(tmp_path):
    runtime, config = produce(tmp_path)
    projection = project(runtime, config)
    assert all(component["status"] == "observed" for path, component in
               projection["receipt"]["after"]["component_refs"].items()
               if path.endswith("/delivery_closure"))
    facts, oracle = consume(runtime, config)
    assert facts["diagnostic_valid"] and facts["endpoint_identity_complete"]
    assert len(facts["pairs"]) == 16
    exempt = [p for p in facts["pairs"] if p["compatibility"] == "incompatible"]
    matched = [p for p in facts["pairs"] if p["compatibility"] == "compatible"]
    assert len(exempt) == len(matched) == 8
    for pair in exempt:
        assert pair["observed_match"] == "none"
        assert pair["delivery_closure"] == "no_obligation"
        assert pair["delivery_window_identity_complete"] is True
        detail = pair["proofs"]["delivery_closure"]["detail"]
        assert detail["expected"] == detail["received"] == detail["missing"] == []
        assert detail["no_obligation_reason"] == "actual_endpoint_topic_mismatch"
        refs = pair["proofs"]["delivery_closure"]["refs"]
        assert detail["no_obligation_ref"] in refs
        assert detail["open_ref"] in refs and detail["close_ref"] in refs
        assert all(ref["sha256"] == runtime["raw_sha256"] for ref in refs)
    assert all(p["delivery_closure"] == "closed" for p in matched)
    assert oracle["verdict"] == "pass", oracle
    assert oracle["reason_codes"] == ["bounded_observed_topology_consistency"]


class NoPair(ObservedPeers):
    def __init__(self, *, directional=False):
        super().__init__(auto_match=False)
        self.directional = directional
    def prepare(self, program, config):
        result = super().prepare(program, config)
        for w in range(4):
            for r in range(4):
                self.set_match_edge("w" + str(w), "r" + str(r),
                    writer_sees=self.directional, reader_sees=False)
        return result


@pytest.mark.parametrize("split_topics", [False, True])
def test_4w4r_complete_api_none_has_no_delivery_missing_but_compatible_graph_stays_unknown(tmp_path, split_topics):
    runtime, config = produce(tmp_path, backend=NoPair(), split_topics=split_topics)
    projection = project(runtime, config)
    assert all(p["delivery_closure"] == "no_obligation" for p in projection["state_after"]["pairs"])
    facts, oracle = consume(runtime, config)
    assert all(p["delivery_closure"] == "no_obligation" for p in facts["pairs"])
    assert all(p["proofs"]["delivery_closure"]["detail"]["missing"] == [] for p in facts["pairs"])
    assert oracle["verdict"] == "unknown"
    assert "directed_peer_or_observed_contract_unknown" in oracle["reason_codes"]
    assert "finite_tail_or_delivery_contract_unknown" not in oracle["reason_codes"]
    assert facts["unresolved_historical_obligations"] == []


@pytest.mark.parametrize("fault", ["missing_receipt", "reason", "gid", "scope", "claim_ref",
    "expected", "close_incomplete", "ack_identity", "opening_generation", "chronology",
    "snapshot_binding", "forged_matched_close"])
def test_tampered_no_obligation_never_bypasses_raw_projector(tmp_path, fault):
    runtime, config = produce(tmp_path)
    def corrupt(events):
        claim = next(e for e in events if e["event"] == "topology_delivery_no_obligation")
        opened = events[claim["open_ref"]]
        close = next(e for e in events if e["event"] == "topology_delivery_window_closed"
                     and e["open_ref"] == opened["event_index"])
        if fault == "missing_receipt": claim["event"] = "untrusted_caller_no_obligation"
        elif fault == "reason": claim["reason"] = "predicted_graph_incompatible"
        elif fault == "gid": claim["writer_gid"] = "f" * 32
        elif fault == "scope": claim["scope"] = "persistent_nonmatch"
        elif fault == "claim_ref": close["no_obligation_ref"] = opened["event_index"]
        elif fault == "expected": close["expected_sequences"] = [0]
        elif fault == "close_incomplete": close["window_complete"] = False
        elif fault == "ack_identity": events[opened["snapshot_refs"][claim["reader"]][-1]]["process_start_ticks"] = -1
        elif fault == "opening_generation": opened["reader_generation"] += 1
        elif fault == "chronology": claim["monotonic_ns"] = opened["monotonic_ns"] - 1
        elif fault == "snapshot_binding": claim["snapshot_refs"] = {}
        elif fault == "forged_matched_close":
            close = next(e for e in events if e["event"] == "topology_delivery_window_closed" and e["closure_status"] == "closed")
            close.update(closure_status="no_obligation", window_complete=True, no_obligation_ref=claim["event_index"])
    rewrite(runtime, corrupt)
    # Neither runtime/caller helper flags nor a recomputed test hash are proof.
    runtime.update(facts={"delivery_closure": "no_obligation", "oracle": "pass"}, window_complete=True)
    facts, oracle = consume(runtime, config)
    assert oracle["verdict"] != "pass", (fault, oracle)
    if fault != "forged_matched_close":
        pair = next((p for p in facts["pairs"] if p["writer"] == "w0" and p["reader"] == "r1"), None)
        if pair is not None:
            assert pair["delivery_closure"] != "no_obligation"


def test_directional_matching_does_not_become_empty_obligation(tmp_path):
    runtime, config = produce(tmp_path, backend=NoPair(directional=True), split_topics=False)
    facts, oracle = consume(runtime, config)
    assert all(p["delivery_closure"] != "no_obligation" for p in facts["pairs"])
    assert oracle["verdict"] == "unknown"


@pytest.mark.parametrize("omit,expected", [({1}, "fail"), ({1, 2}, "unknown"), ({2}, "unknown")])
def test_middle_gap_requires_later_received_sentinel_tail_stays_unknown(tmp_path, omit, expected):
    # Preserve reliable/history primary rules, including a gap without sentinel.
    class DirectWindow(ObservedGraph, WindowFixture):
        def _emit(self, event, **fields):
            if event == "match_snapshot":
                eid = fields["logical_id"]
                peers = [h["endpoint_gid"] for other, h in self.handles.items()
                         if h.get("alive") and h["role"] != self.handles[eid]["role"]]
                fields.update(peer_proof_source="dds_matched_endpoint_api", peer_gids_complete=True,
                              matched_peer_gids=peers, matched_count=len(peers))
            return super()._emit(event, **fields)
        def snapshot(self, handles):
            result = super().snapshot(handles)
            for eid, snap in result["snapshots"].items():
                peers = [h["endpoint_gid"] for h in self.handles.values()
                         if h.get("alive") and h["role"] != self.handles[eid]["role"]]
                snap["match_snapshot"].update(peer_proof_source="dds_matched_endpoint_api",
                    peer_gids_complete=True, matched_peer_gids=peers, matched_count=len(peers))
            return result
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program["endpoints"]:
        endpoint["qos"].update(reliability="reliable", history="keep_last", depth=64)
    runtime, config = produce(tmp_path, program=program, backend=DirectWindow(omit=omit))
    facts, oracle = consume(runtime, config)
    assert oracle["verdict"] == expected, (facts["pairs"], oracle)
    assert all(p["delivery_closure"] != "no_obligation" for p in facts["pairs"])


def test_fresh_second_window_match_is_not_exempted_by_first_none(tmp_path):
    class BecomesMatched(ObservedPeers):
        def __init__(self): super().__init__(auto_match=False)
        def publish_batch(self, *args, **kwargs):
            result = super().publish_batch(*args, **kwargs)
            self.set_match_edge("w0", "r0", writer_sees=True, reader_sees=True)
            return result
    program = model.generate_program(7, writers=1, readers=1)
    first = next(a for a in program["actions"] if a["op"] == "publish_batch")
    later = deepcopy(first)
    later["action_id"] = "fresh-second-window"
    later["depends_on"] = [first["action_id"]]
    later["params"]["start_sequence"] += first["params"]["count"]
    program["actions"].insert(program["actions"].index(first) + 1, later)
    runtime, config = produce(tmp_path, program=program, backend=BecomesMatched())
    closes = [e for e in runtime["events"] if e["event"] == "topology_delivery_window_closed"]
    assert [e["closure_status"] for e in closes] == ["no_obligation", "closed"]
    facts, oracle = consume(runtime, config)
    assert facts["pairs"][0]["delivery_closure"] == "closed"
    assert facts["pairs"][0]["proofs"]["delivery_closure"]["detail"]["expected"] == list(range(
        later["params"]["start_sequence"], later["params"]["start_sequence"] + later["params"]["count"]))
    assert oracle["verdict"] == "pass", oracle


@pytest.mark.parametrize("fault", ["incomplete_api", "unknown_peer", "api_absent"])
def test_direct_none_requires_complete_known_api_scope_through_facts(tmp_path, fault):
    runtime, config = produce(tmp_path, backend=NoPair(), split_topics=False)
    def corrupt(events):
        for event in events:
            if event["event"] != "match_snapshot": continue
            if fault == "incomplete_api": event["peer_gids_complete"] = False
            elif fault == "unknown_peer":
                event.update(matched_peer_gids=["f" * 32], matched_count=1)
            else:
                for key in ("peer_proof_source", "peer_gids_complete", "matched_peer_gids"):
                    event.pop(key, None)
    rewrite(runtime, corrupt)
    facts, oracle = consume(runtime, config)
    assert all(p["delivery_closure"] != "no_obligation" for p in facts["pairs"])
    assert oracle["verdict"] == "unknown"
    assert all(p["proofs"]["delivery_closure"]["status"] == "unknown" for p in facts["pairs"])


def test_orphan_topic_writer_sends_are_covered_by_all_current_empty_edges(tmp_path):
    program = model.generate_program(7, writers=4, readers=4)
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/offline/orphan" if endpoint["logical_id"] == "w3" else "/offline/matched"
    runtime, config = produce(tmp_path, program=program)
    facts, oracle = consume(runtime, config)
    assert len([p for p in facts["pairs"] if p["delivery_closure"] == "no_obligation"]) == 4
    assert any(e["event"] == "send" and e["logical_id"] == "w3" for e in runtime["events"])
    assert facts["unresolved_historical_obligations"] == []
    assert oracle["verdict"] == "pass", oracle


def test_pre_reader_historical_sends_are_not_erased_by_later_none_windows(tmp_path):
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/offline/" + endpoint["role"]
    first = model._action("after-reader", "publish_batch", "w0", 1,
                          {"count": 3, "start_sequence": 3, "interval_sec": 0.0})
    program["actions"] = [model._action("writer-first", "create", "w0", 1),
        model._action("uncovered-before-reader", "publish_batch", "w0", 1,
                      {"count": 3, "start_sequence": 0, "interval_sec": 0.0}),
        model._action("reader-later", "create", "r0", 1), first,
        model._action("last-snapshot", "snapshot")]
    runtime, config = produce(tmp_path, program=program)
    facts, oracle = consume(runtime, config)
    pre_refs = [e["event_index"] for e in runtime["events"] if e["event"] == "send"
                and e["send_sequence"] < first["params"]["start_sequence"]]
    assert pre_refs
    assert [r["event_index"] for r in facts["unresolved_historical_obligations"]] == pre_refs
    assert facts["pairs"][0]["delivery_closure"] == "no_obligation"
    assert oracle["verdict"] == "unknown"


def test_old_reader_generation_sends_are_not_erased_by_latest_none_window(tmp_path):
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/offline/" + endpoint["role"]
    first = next(a for a in program["actions"] if a["op"] == "publish_batch")
    later = deepcopy(first)
    later["action_id"] = "after-reader-generation-change"
    later["params"]["start_sequence"] += first["params"]["count"]
    replace = model._action("replace-reader", "recreate", "r0", 2)
    replace["depends_on"] = [first["action_id"]]
    later["depends_on"] = [replace["action_id"]]
    pos = program["actions"].index(first) + 1
    program["actions"][pos:pos] = [replace, later]
    runtime, config = produce(tmp_path, program=program)
    facts, oracle = consume(runtime, config)
    assert facts["pairs"][0]["reader_generation"] == 2
    assert facts["pairs"][0]["delivery_closure"] == "no_obligation"
    old = [e["event_index"] for e in runtime["events"] if e["event"] == "send"
           and e["send_sequence"] < later["params"]["start_sequence"]]
    assert old and [r["event_index"] for r in facts["unresolved_historical_obligations"]] == old
    assert oracle["verdict"] == "unknown"


def test_earlier_unclosed_window_is_not_erased_by_latest_empty_window(tmp_path):
    # First batch genuinely matched but lost its tail. The second snapshot sees
    # complete API NONE. Its empty scope must not erase the older delivery debt.
    class LosesMatch(ObservedGraph, DirectPeerContractFixture):
        def __init__(self): super().__init__(auto_match=False)
        def prepare(self, program, config):
            result = super().prepare(program, config)
            self.set_match_edge("w0", "r0", writer_sees=True, reader_sees=True)
            return result
        def _emit(self, event, **fields):
            if event == "receive" and fields.get("send_sequence") == 2:
                return {"event": event, **fields}
            return super()._emit(event, **fields)
        def publish_batch(self, *args, **kwargs):
            result = super().publish_batch(*args, **kwargs)
            self.set_match_edge("w0", "r0", writer_sees=False, reader_sees=False)
            return result
    program = model.generate_program(7, writers=1, readers=1)
    first = next(a for a in program["actions"] if a["op"] == "publish_batch")
    later = deepcopy(first)
    later["action_id"] = "later-api-none"
    later["params"]["start_sequence"] += first["params"]["count"]
    replace = model._action("close-old-unfinished-window", "recreate", "r0", 2)
    replace["depends_on"] = [first["action_id"]]
    later["depends_on"] = [replace["action_id"]]
    program["actions"][program["actions"].index(first) + 1:program["actions"].index(first) + 1] = [replace, later]
    runtime, config = produce(tmp_path, program=program, backend=LosesMatch())
    closes = [e for e in runtime["events"] if e["event"] == "topology_delivery_window_closed"]
    assert closes[0]["window_complete"] is False
    assert closes[-1]["closure_status"] == "no_obligation"
    facts, oracle = consume(runtime, config)
    assert facts["unresolved_historical_obligations"]
    assert oracle["verdict"] == "unknown"


@pytest.mark.parametrize("size", [1, 4])
def test_full_no_obligation_branch_all_actual_topics_differ_passes_without_missing(tmp_path, size):
    program = model.generate_program(7, writers=size, readers=size)
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/offline/observed/" + endpoint["role"]
    runtime, config = produce(tmp_path, program=program)
    projection = project(runtime, config)
    facts, oracle = consume(runtime, config)
    assert len(facts["pairs"]) == size * size
    assert all(p["delivery_closure"] == "no_obligation" for p in projection["state_after"]["pairs"])
    for pair in facts["pairs"]:
        assert pair["compatibility"] == "incompatible" and pair["observed_match"] == "none"
        assert pair["delivery_closure"] == "no_obligation"
        detail = pair["proofs"]["delivery_closure"]["detail"]
        assert detail["expected"] == detail["received"] == detail["missing"] == []
        assert detail["no_obligation_ref"] in pair["proofs"]["delivery_closure"]["refs"]
    assert facts["unresolved_historical_obligations"] == []
    assert oracle["verdict"] == "pass", oracle
