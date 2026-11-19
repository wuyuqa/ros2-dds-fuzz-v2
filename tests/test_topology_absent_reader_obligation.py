"""Bounded offline absent-reader regression; never ROS/DDS/replay evidence.

Use the existing ObservedPeers/produce/consume and WindowFixture contracts.
Corruptions rewrite only freshly produced fixture bytes, not retained live raw.
"""
from copy import deepcopy

import pytest

from r2f.campaign.topology_adapter import _Audit, _no_obligation_send_covered
from r2f.modules.s1 import topology_model as model
from test_topology_no_obligation_facts import (
    ObservedGraph, ObservedPeers, consume, produce, rewrite,
)
from test_topology_delivery_windows import WindowFixture
from test_multipeer_no_obligation import DirectPeerContractFixture


@pytest.fixture(autouse=True)
def forbid_live(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("absent-reader regression may not prepare ROS")
    monkeypatch.setattr("r2f.runtime.topology_worker.RosTopologyBackend.prepare", forbidden)


def program(readers=2, writers=1):
    p = model.generate_program(7, writers=writers, readers=readers)
    for endpoint in p["endpoints"]:
        endpoint["topic"] = "/absent-reader/" + endpoint["role"]
    for action in p["actions"]:
        if action["op"] == "publish_batch":
            action["params"]["count"] = 3
    return p


def delete_action(p, aid):
    removed = {aid}
    while True:
        expanded = removed | {a["action_id"] for a in p["actions"]
                              if set(a["depends_on"]) & removed}
        if expanded == removed:
            break
        removed = expanded
    p["actions"] = [a for a in p["actions"] if a["action_id"] not in removed]
    return removed


def inventory(runtime, config, facts):
    audit = _Audit(runtime["events"], runtime, config)
    audit.identity()
    return [(s, _no_obligation_send_covered(audit, facts["pairs"], s))
            for s in runtime["events"] if s["event"] == "send"]


def assert_offline_pass(runtime, config):
    facts, oracle = consume(runtime, config)
    assert facts["diagnostic_valid"], facts.get("evidence_error")
    assert facts["unresolved_historical_obligations"] == []
    assert oracle["verdict"] == "pass", oracle
    assert not facts["execution_evidence"] and not facts["candidate_eligible"]
    return facts


def test_dependency_closed_delete_reader_not_a_historical_obligation(tmp_path):
    p = program(readers=4, writers=4)
    for endpoint in p["endpoints"]:
        endpoint["topic"] = "/absent-reader/" + ("isolated" if endpoint["logical_id"] == "w3" else "common")
    next(a for a in p["actions"] if a["action_id"] == "snapshot_final")["depends_on"] = ["create_r2"]
    assert delete_action(p, "create_r2") == {"create_r2", "snapshot_final"}
    r, c = produce(tmp_path, program=p)
    facts = assert_offline_pass(r, c)
    absent = next(pair for pair in facts["pairs"] if pair["reader"] == "r2")
    assert absent["reader_generation"] is None
    assert absent["delivery_closure"] == "not_applicable"
    assert all(covered for s, covered in inventory(r, c, facts) if s["logical_id"] == "w3")
    assert len([e for e in r["events"] if e["event"] == "endpoint_created"
                and e.get("logical_id", "").startswith("r")]) == 3


def test_declared_reader_never_scheduled_cannot_pollute_live_set(tmp_path):
    p = program(readers=1)
    unused = deepcopy(next(e for e in p["endpoints"] if e["role"] == "reader"))
    unused["logical_id"] = "r1"
    p["endpoints"].append(unused)
    r, c = produce(tmp_path, program=p)
    assert not any(e["event"] == "endpoint_created" and e.get("logical_id") == "r1"
                   for e in r["events"])
    assert_offline_pass(r, c)


def test_reader_retired_before_send_does_not_pollute_live_set(tmp_path):
    p = program()
    send = next(a for a in p["actions"] if a["op"] == "publish_batch")
    p["actions"].insert(p["actions"].index(send), model._action("retire-before", "destroy", "r1", 1))
    r, c = produce(tmp_path, program=p)
    assert_offline_pass(r, c)


@pytest.mark.parametrize("change", ["destroy", "recreate"])
def test_reader_active_at_send_cannot_be_erased_by_later_lifecycle(tmp_path, change):
    p = program()
    send = next(a for a in p["actions"] if a["op"] == "publish_batch")
    p["actions"].insert(p["actions"].index(send) + 1,
        model._action("change-after", change, "r1", 2 if change == "recreate" else 1))
    r, c = produce(tmp_path, program=p)
    facts, oracle = consume(r, c)
    assert facts["diagnostic_valid"], facts.get("evidence_error")
    assert facts["unresolved_historical_obligations"]
    assert all(not covered for _, covered in inventory(r, c, facts))
    assert oracle["verdict"] == "unknown"


def test_recreated_reader_before_send_requires_current_generation_window(tmp_path):
    p = program()
    send = next(a for a in p["actions"] if a["op"] == "publish_batch")
    p["actions"].insert(p["actions"].index(send), model._action("recreate-before", "recreate", "r1", 2))
    r, c = produce(tmp_path, program=p)
    facts = assert_offline_pass(r, c)
    assert next(pair for pair in facts["pairs"] if pair["reader"] == "r1")["reader_generation"] == 2


def test_later_reader_creation_does_not_invent_earlier_obligation(tmp_path):
    p = program()
    create = next(a for a in p["actions"] if a["op"] == "create" and a["endpoint_id"] == "r1")
    p["actions"].remove(create)
    send = next(a for a in p["actions"] if a["op"] == "publish_batch")
    p["actions"].insert(p["actions"].index(send) + 1, create)
    r, c = produce(tmp_path, program=p)
    facts, oracle = consume(r, c)
    assert facts["diagnostic_valid"], facts.get("evidence_error")
    # Final r1 lacks a delivery window and remains unknown overall, but r1 did
    # not exist at these sends; only the earlier live r0 must cover them.
    assert facts["unresolved_historical_obligations"] == []
    assert all(covered for _, covered in inventory(r, c, facts))
    assert oracle["verdict"] == "unknown"


@pytest.mark.parametrize("fault", ["missing_pair", "unknown", "generation", "duplicate", "identity"])
def test_active_reader_requires_one_exact_generation_proven_pair(tmp_path, fault):
    r, c = produce(tmp_path, program=program())
    facts = assert_offline_pass(r, c)
    pairs = deepcopy(facts["pairs"])
    target = next(p for p in pairs if p["reader"] == "r1")
    if fault == "missing_pair":
        pairs.remove(target)
    elif fault == "unknown":
        target["delivery_closure"] = "unknown"
    elif fault == "generation":
        target["reader_generation"] = 2
    elif fault == "duplicate":
        pairs.append(deepcopy(target))
    elif fault == "identity":
        target["delivery_window_identity_complete"] = False
    audit = _Audit(r["events"], r, c)
    audit.identity()
    assert all(not _no_obligation_send_covered(audit, pairs, s)
               for s, _ in inventory(r, c, facts))


@pytest.mark.parametrize("fault", ["missing_claim", "missing_ack", "missing_close", "overlap",
                                   "close_before_send", "open_after_send"])
def test_alive_reader_raw_proof_and_window_guards_remain_strict(tmp_path, fault):
    p = program(readers=3)
    delete_action(p, "create_r2")
    r, c = produce(tmp_path, program=p)
    assert_offline_pass(r, c)
    def corrupt(events):
        opened = next(e for e in events if e["event"] == "topology_delivery_window_open"
                      and e["reader"] == "r1")
        close = next(e for e in events if e["event"] == "topology_delivery_window_closed"
                     and e["open_ref"] == opened["event_index"])
        sends = [e for e in events if e["event"] == "send"]
        if fault == "missing_claim":
            claim = next(e for e in events if e["event"] == "topology_delivery_no_obligation"
                         and e["open_ref"] == opened["event_index"])
            claim["event"] = "fixture_removed_claim"
        elif fault == "missing_ack":
            events[opened["snapshot_refs"]["r1"][-1]]["event"] = "fixture_removed_ack"
        elif fault == "missing_close":
            close["event"] = "fixture_removed_close"
        elif fault == "overlap":
            # Preserve indices/references; insert an unclosed overlapping open
            # at an unrelated pre-send fixture event, never a live artifact.
            spare = next(e for e in events if e["event"] == "topology_provenance")
            index, clock, wall = (spare[k] for k in ("event_index", "monotonic_ns", "time_ns"))
            spare.update(deepcopy(opened), event_index=index, monotonic_ns=clock, time_ns=wall)
        elif fault == "close_before_send":
            close["monotonic_ns"] = sends[0]["monotonic_ns"] - 1
        elif fault == "open_after_send":
            opened["monotonic_ns"] = sends[-1]["monotonic_ns"] + 1
    rewrite(r, corrupt)
    facts, oracle = consume(r, c)
    assert oracle["verdict"] == "unknown", (fault, oracle)
    assert facts.get("unresolved_historical_obligations") or not facts["diagnostic_valid"]
    assert not facts["candidate_eligible"]


@pytest.mark.parametrize("reader_when", ["later", "never"])
def test_send_before_any_reader_remains_unknown_even_with_final_nonmatch(tmp_path, reader_when):
    p = program(readers=1)
    create_w = next(a for a in p["actions"] if a["op"] == "create" and a["endpoint_id"] == "w0")
    create_r = next(a for a in p["actions"] if a["op"] == "create" and a["endpoint_id"] == "r0")
    send = next(a for a in p["actions"] if a["op"] == "publish_batch")
    send["depends_on"] = [create_w["action_id"]]
    p["actions"] = [create_w, send] + ([create_r] if reader_when == "later" else [])
    r, c = produce(tmp_path, program=p)
    facts, oracle = consume(r, c)
    assert facts["diagnostic_valid"], facts.get("evidence_error")
    assert facts["unresolved_historical_obligations"]
    assert all(not covered for _, covered in inventory(r, c, facts))
    assert oracle["verdict"] == "unknown"


class ObservedWindow(ObservedGraph, WindowFixture, DirectPeerContractFixture):
    pass


@pytest.mark.parametrize("omit, verdict", [((), "pass"), ((2,), "unknown"),
                                           ((1, 2), "unknown"), ((1,), "fail")])
def test_absent_reader_never_exempts_sent_tail_or_closed_middle_gap(tmp_path, omit, verdict):
    p = program()
    # One real obligated pair remains; absent r1 must not excuse its sends.
    for ep in p["endpoints"]:
        ep["topic"] = "/absent-reader/matched"
    delete_action(p, "create_r1")
    r, c = produce(tmp_path, program=p, backend=ObservedWindow(omit=omit))
    facts, oracle = consume(r, c)
    assert facts["diagnostic_valid"], facts.get("evidence_error")
    assert oracle["verdict"] == verdict, oracle
    assert not any(e["event"] == "topology_delivery_no_sends" for e in r["events"])
    if verdict == "fail":
        assert oracle["forbidden_residuals"]
    assert not facts["candidate_eligible"]
