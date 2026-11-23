"""Offline guard-consumer regression on immutable P16 raw; not new DDS evidence."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path

import pytest
from r2f.campaign import topology_adapter as adapter

P16_RELATIVE = Path("results/main-review/s1-final-breadth-prep-20261120/continuation-db4f12e/live-remaining-fifteen/partition-16")
LOCAL_ROOT = Path(__file__).resolve().parents[1]
# The pinned installer preserves the historical absolute raw/source bindings.
ROOT = Path(os.environ.get("R2F_GUARD_REPOSITORY",
    LOCAL_ROOT if (LOCAL_ROOT / P16_RELATIVE).is_dir() else "/home/wuyuqa/ros2-dds-fuzz-v2"))
P16 = ROOT / P16_RELATIVE


def original():
    case = next(P16.glob("cases/*/runtime_config.json")).parent
    config = json.loads((case / "runtime_config.json").read_text())
    runtime = json.loads((case / "runtime/runtime_result.json").read_text())
    events = [json.loads(line) for line in Path(runtime["raw_path"]).read_text().splitlines()]
    return config, runtime, events


def test_p16_exact_rejected_actions_are_audited_without_inventing_success():
    config, runtime, events = original()
    audit = adapter._Audit(events, runtime, config)
    assert len(audit.identity()) == 7
    records = audit.actions()
    assert len(records) == 13
    rejected = {r["action_id"]: r for r in records if r["terminal"] in {"capability_reject", "guard_reject"}}
    assert set(rejected) == {"create_w1", "publish_w1"}
    assert rejected["create_w1"]["terminal"] == "capability_reject"
    assert rejected["publish_w1"]["terminal"] == "guard_reject"
    for record in rejected.values():
        assert record["before"] == record["after"] == {"alive": False, "generation": 0, "next_sequence": 0}
        assert record["requires"][0]["status"] == "unsatisfied"
        for key in ("raw_begin", "raw_state_before", "raw_guard", "raw_after", "raw_done"):
            ref = record[key]
            assert audit.resolve(ref) == events[ref["event_index"]]
            assert ref["path"] == runtime["raw_path"] and ref["sha256"] == runtime["raw_sha256"]
    assert runtime["mutation_receipt"] == dict(planned=True, submitted=True, carrier_admitted=True,
        configured=False, applied=False, hit=False, effect_observed=False)


def find(events, kind, aid):
    return next(e for e in events if e["event"] == kind and
                (e.get("action_id") == aid or e.get("step_id") == "action:" + aid))


def persisted_copy(tmp_path, mutate):
    config, runtime, events = original()
    mutate(events)
    for i, event in enumerate(events):
        event["event_index"] = i
    data = "".join(json.dumps(e, sort_keys=True) + "\n" for e in events).encode()
    path = tmp_path / "corrupted-copy.jsonl"
    path.write_bytes(data)
    runtime.update(raw_path=str(path), raw_sha256=hashlib.sha256(data).hexdigest(), events=events)
    return adapter._Audit(events, runtime, config)


def insert_event(events, index, event_kind, **fields):
    envelope = {k: events[index][k] for k in ("run_id", "domain_id", "pid", "pgid", "role", "backend", "monotonic_clock_identity")}
    envelope.update(event=event_kind, monotonic_ns=(events[index-1]["monotonic_ns"] + events[index]["monotonic_ns"]) // 2,
                    time_ns=(events[index-1]["time_ns"] + events[index]["time_ns"]) // 2, **fields)
    events.insert(index, envelope)


def test_unbound_extra_guard_is_not_accepted(tmp_path):
    def mutate(events):
        template = deepcopy(find(events, "topology_guard_reject", "create_w1"))
        template.pop("event_index")
        for k in ("event", "monotonic_ns", "time_ns", "run_id", "domain_id", "pid", "pgid", "role", "backend", "monotonic_clock_identity"):
            template.pop(k)
        template["step_id"] = "action:orphan"
        insert_event(events, len(events)-1, "topology_guard_reject", **template)
    audit = persisted_copy(tmp_path, mutate)
    with pytest.raises(ValueError, match="action_guard_unbound_or_duplicate"):
        audit.actions()


def test_after_extra_effect_claim_is_not_hidden_by_receipt_subset(tmp_path):
    def mutate(events):
        find(events, "state_step_after", "create_w1")["effect_observed"] = True
    audit = persisted_copy(tmp_path, mutate)
    with pytest.raises(ValueError, match="action_guard_receipt_not_raw_backed"):
        audit.actions()


@pytest.mark.parametrize("damage,reason", [
    ("missing_guard", "raw_missing_or_duplicate:topology_guard_reject"),
    ("missing_before", "raw_missing_or_duplicate:state_step_before"),
    ("missing_after", "raw_missing_or_duplicate:state_step_after"),
    ("missing_done", "action_trace_extra_missing_or_reordered"),
    ("wrong_before", "action_before_not_observed_lifecycle"),
    ("wrong_after", "action_guard_terminal_not_recomputed"),
    ("wrong_guard_code", "action_guard_receipt_not_raw_backed"),
    ("false_unsupported_reason", "action_guard_terminal_not_recomputed"),
    ("supported_endpoint_capability", "action_guard_capability_not_reproduced"),
    ("live_writer_dead_guard", "action_guard_writer_predicate_not_reproduced"),
    ("unknown_rejection", "action_guard_terminal_not_recomputed"),
    ("tool_error", "action_requires_or_terminal_not_reached"),
    ("wrong_requires", "action_guard_terminal_not_recomputed"),
    ("wrong_step", "action_terminal_receipt_missing"),
    ("wrong_action_index", "action_program_binding_mismatch"),
    ("guard_time_reversed", "raw_chronology_invalid"),
    ("guard_controller_mismatch", "action_guard_controller_mismatch"),
    ("hidden_create", "action_guard_side_effect_or_unattributed_event"),
    ("hidden_send", "action_guard_side_effect_or_unattributed_event"),
    ("hidden_command", "action_guard_side_effect_or_unattributed_event"),
    ("escaped_command", "action_guard_effect_outside_terminal"),
    ("success_sibling_missing_send", "action_publish_sequence_binding_mismatch"),
    ("success_sibling_missing_creation", "raw_missing_or_duplicate:endpoint_created"),
])
def test_narrow_rejections_fail_closed_without_skipping_success_actions(tmp_path, damage, reason):
    def mutate(events):
        begin = find(events, "topology_action_begin", "create_w1")
        guard = find(events, "topology_guard_reject", "create_w1")
        after = find(events, "state_step_after", "create_w1")
        done = find(events, "topology_action_done", "create_w1")
        if damage.startswith("missing_"):
            kind = {"missing_guard": "topology_guard_reject", "missing_before": "state_step_before",
                    "missing_after": "state_step_after", "missing_done": "topology_action_done"}[damage]
            events.remove(find(events, kind, "create_w1"))
        elif damage == "wrong_before":
            begin["before"]["generation"] = 1
        elif damage == "wrong_after":
            after["after"]["alive"] = True
            done["receipt"]["after"]["alive"] = True
        elif damage == "wrong_guard_code":
            guard["code"] = "invented_guard"
        elif damage == "false_unsupported_reason":
            for value in (guard, after, done["receipt"]):
                value["details"]["value"] = "automatic"
        elif damage == "supported_endpoint_capability":
            supported = find(events, "topology_action_done", "create_w0")
            supported["receipt"]["disposition"] = "capability_reject"
        elif damage == "live_writer_dead_guard":
            find(events, "topology_action_done", "publish_w0")["receipt"]["disposition"] = "guard_reject"
        elif damage == "unknown_rejection":
            for value in (guard, after, done["receipt"]):
                value["code"] = "not_independently_supported"
        elif damage == "tool_error":
            done["receipt"]["disposition"] = "tool_error"
        elif damage == "wrong_requires":
            for value in (after, done["receipt"]):
                value["requires"][0]["status"] = "satisfied"
        elif damage == "wrong_step":
            done["receipt"]["step_id"] = "action:some_other_action"
        elif damage == "wrong_action_index":
            done["action_index"] = 0
        elif damage == "guard_time_reversed":
            guard["monotonic_ns"] = begin["monotonic_ns"] - 1
        elif damage == "guard_controller_mismatch":
            guard["pid"] += 100
        elif damage in {"hidden_create", "hidden_send", "hidden_command"}:
            event_kind = {"hidden_create": "endpoint_created", "hidden_send": "send",
                          "hidden_command": "worker_command_requested"}[damage]
            insert_event(events, events.index(guard), event_kind, logical_id="w1", generation=1)
        elif damage == "escaped_command":
            d = find(events, "topology_action_done", "publish_w1")
            insert_event(events, events.index(d) + 1, "worker_command_requested", logical_id="w1", generation=1)
        elif damage == "success_sibling_missing_send":
            events.remove(next(e for e in events if e["event"] == "send" and e.get("logical_id") == "w2"
                               and e.get("send_sequence") == 1))
        elif damage == "success_sibling_missing_creation":
            events.remove(next(e for e in events if e["event"] == "endpoint_created" and e.get("logical_id") == "w2"))
        else:
            raise AssertionError(damage)
    # The action-consumer seam is tested directly on real persisted corrupt copies;
    # no verifier is mocked and identity/projection cannot reject first by accident.
    audit = persisted_copy(tmp_path, mutate)
    with pytest.raises(ValueError, match=reason):
        audit.actions()


def test_real_p16_public_facts_remain_nohit_not_setter_or_mutation_success():
    config, runtime, events = original()
    facts = adapter.TopologyFactProducer().produce(dict(config=config, runtime=runtime, events=events))
    assert facts["diagnostic_valid"] is True, facts.get("evidence_error")
    assert facts["tool_error"] is False
    from r2f.campaign.identity import source_manifest
    expected_source = config["source_manifest"]
    assert facts["source_provenance_qualified"] is (expected_source == source_manifest(ROOT))
    assert all(facts[k] is True for k in ["execution_evidence",
        "environment_healthy", "process_healthy", "endpoint_identity_complete"])
    # Canonical admission independently reopens the current source. Historical
    # raw recorded source_identity_ok=True, so changed source must also prevent
    # those preconditions from being requalified, not only candidate admission.
    if facts["source_provenance_qualified"]:
        assert facts["preconditions_reached"] is True
        assert "canonical_receipt_error" not in facts
    else:
        assert facts["preconditions_reached"] is False
        assert facts["canonical_receipt_error"] == "canonical_state_before_not_observed"
    assert len(facts["action_receipts"]) == 13
    assert facts["application_audit"]["status"] == "incomplete"
    assert facts["application_audit"]["canonical_receipt_matches"] is True
    assert facts["application_audit"]["mutation_receipt"] == runtime["mutation_receipt"]
    assert facts["candidate_eligible"] is False
    assert facts["seal_eligible"] is False


def test_unchanged_nohit_gate_accepts_only_a_complete_staged_consumer_preview():
    from r2f.campaign.runtime_adapter import topology_nohit_terminal
    config, runtime, events = original()
    facts = adapter.facts_from_events(events, runtime, config)
    case = next(P16.glob("cases/*/runtime_config.json")).parent
    row = json.loads((case / "row.json").read_text())
    # In-memory hypothetical consumer preview ONLY. Never written as a new run or
    # substituted into the immutable historical aborted/unknown row.
    row.update(execution_stage="reached", typed_facts=facts, candidate_eligible=False)
    for key in ("abort_reason", "ingestion_error", "tool_error"):
        row.pop(key, None)
    row["stage_history"] = row["stage_history"][:-1]
    if facts["source_provenance_qualified"]:
        terminal = topology_nohit_terminal(config, runtime, row)
        assert terminal["qualified"] is False
        assert terminal["execution_stage"] == "reached"
        assert terminal["mutation_receipt"] == runtime["mutation_receipt"]
    else:
        # A new consumer may diagnose historical raw, but cannot requalify it
        # under a different frozen source. The original no-hit gate stays shut.
        with pytest.raises(ValueError, match="topology_nohit_terminal_evidence_incomplete"):
            topology_nohit_terminal(config, runtime, row)
    for field in ("diagnostic_valid", "endpoint_identity_complete", "process_healthy"):
        damaged = deepcopy(row)
        damaged["typed_facts"][field] = False
        with pytest.raises(ValueError, match="topology_nohit_terminal_evidence_incomplete"):
            topology_nohit_terminal(config, runtime, damaged)


def test_guard_before_cannot_downgrade_boolean_to_integer(tmp_path):
    def mutate(events):
        find(events, "topology_action_begin", "create_w1")["before"]["alive"] = 0
    audit = persisted_copy(tmp_path, mutate)
    with pytest.raises(ValueError, match="action_guard_before_not_raw_backed"):
        audit.actions()
