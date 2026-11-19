"""Shadow/offline only: generation-wide no-send applicability is not DDS evidence."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_oracles import evaluate_topology
from r2f.runtime.topology_pair import _delivery_no_sends_basis
from test_topology_no_obligation_facts import (
    ObservedGraph, ObservedPeers, consume, produce, project, rewrite,
)
from test_topology_delivery_windows import WindowFixture
from test_multipeer_no_obligation import DirectPeerContractFixture


@pytest.fixture(autouse=True)
def forbid_live(monkeypatch):
    def forbidden(*a, **kw):
        raise AssertionError("zero-send tests may not prepare ROS")
    monkeypatch.setattr("r2f.runtime.topology_worker.RosTopologyBackend.prepare", forbidden)


def program_without_publish(size=1, zero=None):
    p = model.generate_program(7, writers=size, readers=size)
    zero = {"w" + str(i) for i in range(size)} if zero is None else zero
    removed = {a["action_id"] for a in p["actions"]
               if a["op"] == "publish_batch" and a["endpoint_id"] in zero}
    # Match legal dependency-closed action deletion, including final snapshot.
    while True:
        expanded = removed | {a["action_id"] for a in p["actions"] if set(a["depends_on"]) & removed}
        if expanded == removed:
            break
        removed = expanded
    p["actions"] = [a for a in p["actions"] if a["action_id"] not in removed]
    for ep in p["endpoints"]:
        ep["topic"] = "/zero-send/" + str(int(ep["logical_id"][1:]) % 2)
    return p


@pytest.mark.parametrize("size,zero", [(1, None), (4, {"w1"}), (4, None)])
def test_zero_send_only_exempts_exact_generation_delivery(tmp_path, size, zero):
    r, c = produce(tmp_path, program=program_without_publish(size, zero))
    assert r["runtime_status"] == "completed", r.get("unknowns")
    f, oracle = consume(r, c)
    claims = [e for e in r["events"] if e["event"] == "topology_delivery_no_sends"]
    zero = {"w" + str(i) for i in range(size)} if zero is None else zero
    assert len(claims) == len(zero) * size
    for pair in f["pairs"]:
        if pair["writer"] in zero:
            assert pair["delivery_closure"] == "no_obligation"
            assert pair["delivery_applicability"] == "not_applicable_no_sends"
            assert pair["delivery_window_identity_complete"] is True
            detail = pair["proofs"]["delivery_closure"]["detail"]
            assert detail["expected"] == detail["received"] == detail["missing"] == []
        else:
            assert pair["delivery_closure"] in {"closed", "no_obligation"}
            assert pair.get("delivery_applicability") != "not_applicable_no_sends"
    assert f["unresolved_historical_obligations"] == []
    assert oracle["verdict"] == "pass", oracle
    assert not r["mutation_receipt"]["hit"] and not r["mutation_receipt"]["effect_observed"]
    assert all(e["mutation_hit"] is False and e["semantic_effect"] is False
               and e["candidate_eligible"] is False for e in claims)
    assert not any(e.get("writer") in zero and e["event"].startswith("topology_delivery_window")
                   for e in r["events"])


@pytest.mark.parametrize("fault", ["seal_missing", "seal_hash", "receipt_missing", "receipt_schema",
    "receipt_identity", "receipt_generation", "receipt_inventory", "receipt_snapshots", "ack_missing",
    "ack_identity", "ack_before_seal", "lifecycle_incomplete", "action_terminal_missing",
    "created_identity", "worker_end_missing", "worker_end_identity", "end_missing", "cleanup_incomplete",
    "fake_empty_receipt", "late_send", "ambiguous_send", "opened_window"])
def test_incomplete_or_forged_zero_send_proof_is_unknown(tmp_path, fault):
    r, c = produce(tmp_path, program=program_without_publish())
    assert consume(r, c)[1]["verdict"] == "pass"
    def corrupt(events):
        claim = next(e for e in events if e["event"] == "topology_delivery_no_sends")
        seal = events[claim["inventory_end_ref"]]
        ack = events[claim["snapshot_refs"]["r0"][-1]]
        boundary = next(e for e in events if e["event"] == "topology_state_boundary" and e.get("phase") == "after_program")
        if fault == "seal_missing": seal["event"] = "uncaptured_inventory_end"
        elif fault == "seal_hash": seal["inventory_sha256"] = "0" * 64
        elif fault == "receipt_missing": claim["event"] = "untrusted_caller_claim"
        elif fault == "receipt_schema": claim["schema"] = "invented/v0"
        elif fault == "receipt_identity": claim["writer_identity"]["pid"] += 1
        elif fault == "receipt_generation": claim["writer_generation"] = 2
        elif fault == "receipt_inventory": claim["inventory_count"] = 0
        elif fault == "receipt_snapshots": claim["snapshot_refs"]["r0"] = []
        elif fault == "ack_missing": ack["event"] = "uncaptured_ack"
        elif fault == "ack_identity": ack["process_start_ticks"] += 1
        elif fault == "ack_before_seal": ack["monotonic_ns"] = seal["monotonic_ns"] - 1
        elif fault == "lifecycle_incomplete": boundary["lifecycle_complete"] = False
        elif fault == "action_terminal_missing":
            next(e for e in events if e["event"] == "topology_action_done")["event"] = "uncaptured_action_terminal"
        elif fault == "created_identity":
            next(e for e in events if e["event"] == "endpoint_created" and e.get("logical_id") == "w0")["endpoint_generation"] = "forged"
        elif fault in {"worker_end_missing", "worker_end_identity"}:
            exit_ = next(e for e in events if e["event"] == "topology_worker_exit" and e.get("logical_id") == "w0")
            if fault == "worker_end_missing": exit_["event"] = "uncaptured_worker_end"
            else: exit_["endpoint_generation"] = "forged"
        elif fault == "end_missing": events[-1]["event"] = "uncaptured_runtime_end"
        elif fault == "cleanup_incomplete": events[-1]["cleanup_complete"] = False
        elif fault == "fake_empty_receipt":
            # A boolean/empty list is no substitute for a full raw receipt.
            for key in list(claim):
                if key not in {"event", "event_index", "monotonic_ns", "run_id", "time_ns", "writer", "reader", "writer_generation", "reader_generation"}:
                    claim.pop(key)
            claim.update(window_complete=True, expected_sequences=[], closure_status="not_applicable_no_sends")
        else:
            # Replace a late diagnostic event in NEW fixture bytes; old artifacts
            # are never touched. A post-boundary send must invalidate the proof.
            target = next(e for e in events if e["event"] == "topology_execution_metadata")
            if fault == "opened_window":
                target.update(event="topology_delivery_window_open", writer="w0", reader="r0", writer_generation=1, reader_generation=1)
            else:
                target.update(event="send", logical_id="w0", generation=1, send_sequence=99,
                              **{k: claim["writer_identity"][k] for k in ("pid", "process_start_ticks", "endpoint_generation")})
                if fault == "ambiguous_send": target["logical_id"] = "unknown-writer"
    rewrite(r, corrupt)
    f, oracle = consume(r, c)
    assert oracle["verdict"] == "unknown", (fault, oracle)
    assert not f["candidate_eligible"] and not f["seal_eligible"]


class ObservedWindow(ObservedGraph, WindowFixture, DirectPeerContractFixture):
    pass


@pytest.mark.parametrize("omit,verdict", [((2,), "unknown"), ((1,), "fail"), ((), "pass")])
def test_sent_generation_keeps_existing_tail_and_middle_gap_rules(tmp_path, omit, verdict):
    p = model.generate_program(7, writers=1, readers=1)
    r, c = produce(tmp_path, program=p, backend=ObservedWindow(omit=omit))
    assert not any(e["event"] == "topology_delivery_no_sends" for e in r["events"])
    assert consume(r, c)[1]["verdict"] == verdict


@pytest.mark.parametrize("which", ["writer", "reader"])
def test_regeneration_cannot_erase_preexisting_sends(tmp_path, which):
    p = model.generate_program(7, writers=1, readers=1)
    first = next(a for a in p["actions"] if a["op"] == "publish_batch")
    change = model._action("rebuild-after-send", "recreate", "w0" if which == "writer" else "r0", 2)
    change["depends_on"] = [first["action_id"]]
    p["actions"].insert(p["actions"].index(first)+1, change)
    r, c = produce(tmp_path, program=p)
    if which == "writer":
        # Completed g1 sentinel is retained, not borrowed as a g2 publish.
        facts, oracle = consume(r, c)
        assert r["runtime_status"] == "completed"
        assert facts["unresolved_historical_obligations"] == []
        assert facts["historical_send_coverage"]
        assert oracle["verdict"] == "pass"
        assert not any(e["event"] == "send" and e.get("generation") == 2 for e in r["events"])
        # Now make the ORIGINAL old tail genuinely incomplete, preserving all
        # original conservative assertions rather than blanket-flipping verdicts.
        seqs = range(first["params"]["start_sequence"], first["params"]["start_sequence"] + first["params"]["count"])
        r, c = produce(tmp_path / "old-tail-missing", program=p, backend=ObservedWindow(omit=tuple(seqs)))
    assert r["runtime_status"] == "completed_with_unknown"
    assert not any(e["event"] == "topology_delivery_no_sends" for e in r["events"])
    assert consume(r, c)[1]["verdict"] == "unknown"


@pytest.mark.parametrize("which", ["writer", "reader"])
def test_unsent_rebuilt_generation_is_conservatively_unknown(tmp_path, which):
    p = program_without_publish()
    p["actions"].append(model._action("rebuild-without-send", "recreate", "w0" if which == "writer" else "r0", 2))
    r, c = produce(tmp_path, program=p)
    if which == "writer":
        facts, oracle = consume(r, c)
        assert r["runtime_status"] == "completed" and oracle["verdict"] == "pass"
        assert facts["unresolved_historical_obligations"] == []
        assert not any(e["event"] in {"send", "topology_delivery_window_open"} for e in r["events"])
        assert any(e["event"] == "topology_delivery_no_sends"
                   and e.get("schema") == "r2f.topology-delivery-no-sends/v2" for e in r["events"])
        from test_topology_recreated_history_accounting import HistoryBackend
        r, c = produce(tmp_path / "missing-ack", program=p, backend=HistoryBackend(fault="missing_ack"))
    # Reader-only regeneration still lacks a v2 writer proof; absent ACKs also
    # retain the original UNKNOWN/no-receipt contract for the writer branch.
    assert r["runtime_status"] == "completed_with_unknown"
    assert not any(e["event"] == "topology_delivery_no_sends" for e in r["events"])
    assert consume(r, c)[1]["verdict"] == "unknown"


def test_no_send_does_not_exempt_graph_forbidden_residual(tmp_path):
    class BadMatchedContract(ObservedPeers):
        @staticmethod
        def graph_contract(fields):
            ObservedGraph.graph_contract(fields)
            for row in fields.get("subscriptions", []):
                row["qos"]["durability"] = "transient_local"
    r, c = produce(tmp_path, program=program_without_publish(), backend=BadMatchedContract())
    f, oracle = consume(r, c)
    assert f["pairs"][0]["delivery_applicability"] == "not_applicable_no_sends"
    assert f["pairs"][0]["compatibility"] == "incompatible"
    assert oracle["verdict"] == "fail"
    assert oracle["forbidden_residuals"] == ["topology.pair_graph_mismatch"]


def test_oracle_requires_facts_acknowledge_no_send_branch(tmp_path):
    r, c = produce(tmp_path, program=program_without_publish())
    facts, oracle = consume(r, c)
    assert oracle["verdict"] == "pass"
    facts["pairs"][0]["delivery_window_identity_complete"] = False
    assert evaluate_topology(SimpleNamespace(target_id=c["target_id"], arm="mutated"), facts)["verdict"] == "unknown"


def test_send_before_reader_with_no_window_cannot_be_forgiven(tmp_path):
    p = model.generate_program(7, writers=1, readers=1)
    send = next(a for a in p["actions"] if a["op"] == "publish_batch")
    create_w = next(a for a in p["actions"] if a["op"] == "create" and a["endpoint_id"] == "w0")
    create_r = next(a for a in p["actions"] if a["op"] == "create" and a["endpoint_id"] == "r0")
    p["actions"] = [create_w, send, create_r]
    r, c = produce(tmp_path, program=p)
    assert any(e["event"] == "send" for e in r["events"])
    assert not any(e["event"] == "topology_delivery_no_sends" for e in r["events"])
    f, oracle = consume(r, c)
    assert f["unresolved_historical_obligations"]
    assert oracle["verdict"] == "unknown"


@pytest.mark.parametrize("which", ["tool_error", "endpoint_identity_complete"])
def test_no_sends_cannot_override_existing_health_identity_gates(tmp_path, which):
    r, c = produce(tmp_path, program=program_without_publish())
    f, _ = consume(r, c)
    f[which] = True if which == "tool_error" else False
    oracle = evaluate_topology(SimpleNamespace(target_id=c["target_id"], arm="mutated"), f)
    assert oracle["verdict"] == "unknown" and not oracle["candidate_eligible"]


def test_late_send_during_cleanup_retracts_provisional_producer_qualification(tmp_path):
    class LateCleanupSend(ObservedPeers):
        def close(self):
            writer = self.handles["w0"]
            self._emit("send", **{k: writer[k] for k in
                ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")},
                writer_gid=writer["endpoint_gid"], send_sequence=99, publish_returned=True)
            return super().close()
    r, c = produce(tmp_path, program=program_without_publish(), backend=LateCleanupSend())
    assert r["runtime_status"] == "completed_with_unknown"
    assert any(u["reason"] == "no_sends_generation_end_incomplete" for u in r["unknowns"])
    assert consume(r, c)[1]["verdict"] == "unknown"
