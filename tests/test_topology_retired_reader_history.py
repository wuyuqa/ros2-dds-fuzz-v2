"""Persisted canonical route: retiring a reader preserves its own closed history.

Offline fixture evidence has no DDS, candidate, or seal credit.
"""
import pytest

from test_topology_lifecycle_window_retirement import RetirementBackend
from test_topology_no_obligation_facts import ObservedGraph, produce, consume, rewrite
from test_topology_pair import action, endpoint, program


class ReaderHistoryBackend(ObservedGraph, RetirementBackend):
    pass


def reader_lifecycle(*, readers=1):
    endpoints = [endpoint("w0", "writer")] + [endpoint("r" + str(i), "reader") for i in range(readers)]
    actions = [action("create_" + e["logical_id"], "create", e["logical_id"], 1) for e in endpoints]
    actions += [action("send_old", "publish_batch", "w0", 1, count=2, start_sequence=0, interval_sec=0.0),
                action("replace_reader", "recreate", "r0", 2),
                action("send_new", "publish_batch", "w0", 1, count=2, start_sequence=2, interval_sec=0.0)]
    return program(endpoints, actions)


@pytest.fixture(autouse=True)
def no_live(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline reader history regression must never prepare ROS")
    monkeypatch.setattr("r2f.runtime.topology_worker.RosTopologyBackend.prepare", forbidden)


@pytest.mark.parametrize("readers", [1, 2])
def test_closed_retired_reader_history_survives_recreation(tmp_path, readers):
    runtime, cfg = produce(tmp_path, program=reader_lifecycle(readers=readers), backend=ReaderHistoryBackend())
    facts, oracle = consume(runtime, cfg)
    assert facts["diagnostic_valid"]
    assert facts["unresolved_historical_obligations"] == []
    assert oracle["verdict"] == "pass"
    old_sends = [e for e in runtime["events"] if e["event"] == "send" and e["send_sequence"] < 2]
    coverage = {c["send_ref"]["event_index"]: c for c in facts["historical_send_coverage"]}
    assert all(e["event_index"] in coverage for e in old_sends)
    assert all(any(w["reader"] == "r0" and w["reader_generation"] == 1
                   for w in coverage[e["event_index"]]["reader_windows"]) for e in old_sends)
    assert all(p["writer_generation"] == 1 for p in facts["pairs"])


@pytest.mark.parametrize("readers", [1, 2])
@pytest.mark.parametrize("damage", ["missing_close", "incomplete_close", "missing_exit", "failed_exit", "wrong_exit_generation", "wrong_exit_gid", "late_close"])
def test_current_generation_cannot_waive_incomplete_reader_history(tmp_path, readers, damage):
    runtime, cfg = produce(tmp_path, program=reader_lifecycle(readers=readers), backend=ReaderHistoryBackend())
    def tamper(events):
        close = next(e for e in events if e["event"] == "topology_delivery_window_closed"
                     and e["reader"] == "r0" and e["reader_generation"] == 1)
        exited = next(e for e in events if e["event"] == "topology_worker_exit"
                      and e.get("logical_id") == "r0" and e.get("generation") == 1)
        if damage == "missing_close": close["event"] = "untrusted_old_window_close"
        elif damage == "incomplete_close": close["window_complete"] = False
        elif damage == "missing_exit": exited["event"] = "untrusted_old_worker_exit"
        elif damage == "failed_exit": exited["returncode"] = 1
        elif damage == "wrong_exit_generation": exited["generation"] = 2
        elif damage == "wrong_exit_gid": exited["endpoint_gid"] = "not-the-retired-reader"
        else: close["monotonic_ns"] = exited["monotonic_ns"] + 1
    rewrite(runtime, tamper)
    facts, oracle = consume(runtime, cfg)
    assert oracle["verdict"] == "unknown"
    assert facts.get("unresolved_historical_obligations") or not facts["diagnostic_valid"]
    assert not facts["candidate_eligible"] and not facts["seal_eligible"]


def test_missing_old_sentinel_remains_unknown_after_reader_recreation(tmp_path):
    runtime, cfg = produce(tmp_path, program=reader_lifecycle(readers=2),
                           backend=ReaderHistoryBackend(fault="missing_sentinel"))
    facts, oracle = consume(runtime, cfg)
    assert oracle["verdict"] == "unknown"
    assert facts["unresolved_historical_obligations"]


def test_replaced_reader_requires_fresh_process_identity(tmp_path):
    runtime, cfg = produce(tmp_path, program=reader_lifecycle(), backend=ReaderHistoryBackend())
    created = [e for e in runtime["events"] if e["event"] == "endpoint_created" and e["logical_id"] == "r0"]
    old, new = created
    def tamper(events):
        def reuse(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {"pid", "process_start_ticks"} and item == new[key]:
                        value[key] = old[key]
                    else:
                        reuse(item)
            elif isinstance(value, list):
                for item in value: reuse(item)
        reuse(events)
    rewrite(runtime, tamper)
    facts, oracle = consume(runtime, cfg)
    assert oracle["verdict"] == "unknown"
    assert facts.get("unresolved_historical_obligations") or not facts["diagnostic_valid"]
