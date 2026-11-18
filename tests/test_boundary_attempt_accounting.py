"""Injected/offline attempt accounting; no real boundary, ROS or qualification.

Use the unified runner and unmodified standard checkpoint writer/loader. Runtime
claims of actual execution below are fixture inputs to the counter contract,
never evidence that this test invoked ROS or a namespace boundary.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest

from r2f.campaign import runner
from r2f.campaign.checkpoint import CheckpointResumeError, load_checkpoint, make_checkpoint_hook
from r2f.core.evidence import canonical_hash
import test_model_search_topology as topology
import test_exploratory_checkpoint as parameter
from test_feedback_epoch_runner import adapter_runtime


def setup_state(kind):
    if kind == "topology":
        doc = topology.narrowed()
        doc["search"].update(max_depth=1, samples_per_class=1)
        state = topology.fresh(doc)
        cycle, source = topology.CYCLE, topology.SOURCE
    else:
        doc = parameter.profile()
        doc["budget"]["warmup_inputs"] = 0
        state = parameter.fresh(doc)
        cycle, source = parameter.CYCLE, parameter.SOURCE
    return doc, state, cycle, source


def execute(out, kind="topology", *, status="unknown", actual=True, raises=False,
            state=None, execute_requested=True, wrapper=None, maximum=1):
    doc, initial, cycle, source = setup_state(kind)
    state = initial if state is None else state
    hook = make_checkpoint_hook(out, profile=doc, cycle_id=cycle,
                                source_manifest=source, continuation=state["continuation"])
    if wrapper:
        hook = wrapper(hook)
    calls, returned, qualifications = [], [], []
    original_qualification = runner.execution_boundary_qualification

    def runtime(config, directory):
        calls.append(deepcopy(config))
        if raises:
            raise RuntimeError("injected carrier failure; no runtime execution")
        if kind == "parameter" and actual:
            result = adapter_runtime(config, directory)
        else:
            # Deliberately incomplete raw. Ingest must retain it and unknown,
            # not turn a simulated actual flag into a qualified execution.
            path = Path(directory) / "injected-unknown.jsonl"
            path.write_text(json.dumps({"event": "injected_unknown",
                "run_id": config["run_id"], "domain_id": config["domain_id"],
                "event_index": 0, "unknown_reasons": ["injected_missing_worker_evidence"]}) + "\n")
            result = {"real_execution": actual, "runtime_status": "completed_with_unknown",
                      "raw_path": str(path), "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                      "events": [json.loads(path.read_text())],
                      "reason": "injected_missing_worker_evidence", "evidence_origin": "offline_fixture"}
        returned.append(deepcopy(result))
        return result

    def qualification(boundary=None, **kwargs):
        if kwargs.get("result") is None:
            return original_qualification(boundary, **kwargs)
        if status == "not_applicable":
            return original_qualification(boundary, **kwargs)
        result = {"status": status, "eligible": False,
                  "reason_code": "injected_boundary_" + status,
                  "errors": ["injected_missing_worker_evidence"],
                  "qualification_basis": "offline_injection_not_a_runtime_grant"}
        qualifications.append(deepcopy(result))
        return result

    with patch.object(runner, "source_manifest", return_value=deepcopy(source)), \
         patch.object(runner, "execution_boundary_qualification", side_effect=qualification):
        result = runner.run_exploratory_cycle(out, **state, profile=doc, max_inputs=maximum,
            runtime=runtime, execute=execute_requested, checkpoint_hook=hook, run_id=out.name)
    return result, calls, returned, qualifications


def restore(path, out, kind="topology"):
    doc, _, cycle, source = setup_state(kind)
    return load_checkpoint(path, profile=doc, cycle_id=cycle, source_manifest=source, output_dir=out)


def assert_accounting(result, actual):
    assert result["budget"]["attempted"] == 1
    assert result["budget"]["actual"] == int(actual)
    assert result["runtime_formal"] is False
    events = result["feedback"]["events"]
    assert len(events) == 1
    assert events[0]["executed"] is actual
    assert events[0]["complete"] is events[0]["coverage"] is False
    assert events[0]["edge_id"] is None
    assert "edge_provenance" not in events[0]
    assert result["feedback"]["covered_surfaces"] == []
    assert result["feedback"]["edge_counts"] == {}
    row = result["rows"][0]
    assert row["actual_run"] is actual
    assert row["candidate_eligible"] is row["feedback_eligible"] is row["seal_eligible"] is False
    assert row["corpus_offer"]["accepted"] is False
    assert row.get("observed_edge") is None
    assert result.get("seal") is not True
    assert result["selection_log"]["feedback_event_count"] == 1
    assert result["selection_log"]["feedback_identity"] == result["feedback"]["identity"]
    if "search_summary" in result:
        assert result["search_summary"]["actual"] == int(actual)
        assert result["search_summary"]["submitted"] == 1
    return row


@pytest.mark.parametrize("kind", ["topology", "parameter"])
@pytest.mark.parametrize("status", ["unknown", "invalid"])
@pytest.mark.parametrize("actual", [True, False])
def test_unqualified_attempt_reaches_final_checkpoint_and_retains_evidence(tmp_path, kind, status, actual):
    out = tmp_path / "first"
    doc, initial, _, _ = setup_state(kind)
    before_corpus = initial["corpus"].to_dict()
    result, calls, returned, qualifications = execute(out, kind, status=status, actual=actual, state=initial)
    row = assert_accounting(result, actual)
    assert len(calls) == len(returned) == len(qualifications) == 1
    assert result["corpus"] == before_corpus  # no offer, pins, retention or novelty
    assert row["boundary_qualification"] == qualifications[0]
    assert row["boundary_eligible"] is False
    assert row["raw_path"] == returned[0]["raw_path"]
    assert row["raw_sha256"] == returned[0]["raw_sha256"]
    assert hashlib.sha256(Path(row["raw_path"]).read_bytes()).hexdigest() == row["raw_sha256"]
    assert json.loads(Path(row["runtime_result_path"]).read_text()) == returned[0]
    assert "evidence_errors" not in row  # only the feedback projection is incomplete
    assert result["abort"]["reason_code"] == "execution_boundary_qualification_failed"
    assert result["abort"]["qualification_reason"] == "injected_boundary_" + status
    assert result["stop_reason"] == "execution_boundary_qualification_failed"
    if kind == "parameter" and actual:
        # This fixture normally produces a fully judged observation. The added
        # feedback projection must suppress coverage even for complete facts.
        assert row["execution_stage"] == "judged"
        from r2f.core.observation_status import observation_complete
        assert observation_complete(row)
    else:
        assert row["oracle_verdict"] == "unknown"
    saved = json.loads((out / "checkpoint.json").read_text())
    assert saved["state"]["continuation"]["phase"] == "final"
    assert saved["state"]["continuation"]["pending_selection"] is None
    assert saved["state"]["continuation"]["pending_attempt"] is None
    assert (out / "cycle.json").is_file()
    assert list((out / "checkpoints").glob("*-recorded.json"))
    assert list((out / "checkpoints").glob("*-final.json"))
    # Cold reconstruction, with no warmed validation memo, preserves the
    # completed attempt. Same cumulative budget must not submit it again.
    resumed_state = restore(out / "checkpoint.json", tmp_path / "second", kind)
    resumed, repeated, _, _ = execute(tmp_path / "second", kind, status=status, actual=actual,
                                     state=resumed_state)
    assert repeated == []
    assert resumed["budget"]["attempted"] == 1
    assert resumed["budget"]["actual"] == int(actual)
    assert resumed["cycle_budget"]["attempted"] == resumed["cycle_budget"]["actual"] == 0
    assert resumed["feedback"] == result["feedback"]
    assert resumed["selection_log"]["selections"] == result["selection_log"]["selections"]
    report = json.loads(Path(result["report_path"]).read_text())
    # Qualification/report errors are retained, not weakened to obtain green.
    assert report["audit"]["sealed"] is False
    if actual:
        assert report["audit"]["evidence_valid"] is False
    ledger = json.loads((out / "candidate_ledger.json").read_text())
    assert ledger["candidates"] == []
    if (kind, status, actual) == ("topology", "unknown", True):
        # A genuinely cold Python process uses the same standard loader and
        # runner. No in-process cache can conceal a lost feedback epoch/task.
        code = """
import json, sys
from pathlib import Path
from test_boundary_attempt_accounting import restore, execute
checkpoint, destination = map(Path, sys.argv[1:])
state = restore(checkpoint, destination)
assert state['continuation']['pending_selection'] is None
result, calls, _, _ = execute(destination, state=state)
assert calls == []
assert result['budget']['attempted'] == result['budget']['actual'] == 1
assert result['cycle_budget']['attempted'] == result['cycle_budget']['actual'] == 0
assert len(result['feedback']['events']) == 1
print(json.dumps({'calls': len(calls), 'attempted': 1, 'actual': 1, 'cold': True}))
"""
        cold = subprocess.run([sys.executable, "-c", code, str(out / "checkpoint.json"),
                               str(tmp_path / "cold-resumed")], text=True, capture_output=True,
                              cwd=Path(__file__).resolve().parents[1], timeout=90)
        (out / "cold-process.json").write_text(json.dumps({"returncode": cold.returncode,
            "stdout": cold.stdout, "stderr": cold.stderr}, indent=2))
        assert cold.returncode == 0, cold.stderr
        assert json.loads(cold.stdout)["calls"] == 0


def test_carrier_exception_is_one_zero_actual_attempt(tmp_path):
    result, calls, returned, _ = execute(tmp_path / "tool", status="invalid", raises=True)
    row = assert_accounting(result, False)
    assert len(calls) == 1 and returned == []
    runtime = json.loads(Path(row["runtime_result_path"]).read_text())
    assert runtime["real_execution"] is False
    assert runtime["runtime_status"] == "tool_error"
    assert "injected carrier failure" in runtime["reason"]
    assert row["oracle_verdict"] == "unknown"
    assert restore(tmp_path / "tool/checkpoint.json", tmp_path / "loaded")["feedback"].events


@pytest.mark.parametrize("execute_requested", [False, True])
def test_prepared_only_and_unsubmitted_interrupt_have_no_attempt(tmp_path, execute_requested):
    class PreparedInterrupt(Exception):
        pass

    def wrapper(hook):
        def stop(payload):
            hook(payload)
            if payload["phase"] == "planned":
                raise PreparedInterrupt()
        return stop

    out = tmp_path / "prepared"
    with pytest.raises(PreparedInterrupt):
        execute(out, execute_requested=execute_requested, wrapper=wrapper)
    saved = json.loads((out / "checkpoint.json").read_text())
    continuation = saved["state"]["continuation"]
    assert continuation["submission_state"] == "prepared"
    assert continuation["budget"]["attempted"] == continuation["budget"]["actual"] == 0
    assert saved["state"]["feedback"]["events"] == []
    assert not list(out.glob("cases/*/submission_intent.json"))
    if execute_requested:
        # A still-active producer is conservatively refused even before the
        # invoke fence. Preserve that independent gate; do not weaken it here.
        with pytest.raises(CheckpointResumeError, match="pending_owner_still_active"):
            restore(out / "checkpoint.json", tmp_path / "refused")
        return
    loaded = restore(out / "checkpoint.json", tmp_path / "resumed")
    result, calls, _, _ = execute(tmp_path / "resumed", state=loaded, execute_requested=False)
    assert calls == []
    assert result["budget"]["attempted"] == result["budget"]["actual"] == 0
    assert result["feedback"]["events"] == []


@pytest.mark.parametrize("damage", ["event_actual", "event_missing", "budget_actual", "budget_attempted"])
def test_resigned_feedback_and_actual_budget_tampering_still_fail_closed(tmp_path, damage):
    out = tmp_path / "original"
    execute(out)
    data = json.loads((out / "checkpoint.json").read_text())
    state = data["state"]
    if damage.startswith("event_"):
        from r2f.campaign.feedback import FeedbackLog
        events = state["feedback"]["events"]
        if damage == "event_actual":
            events[0]["executed"] = False
            events[0]["event_sha256"] = canonical_hash({k:v for k,v in events[0].items() if k != "event_sha256"})
        else:
            events.clear()
        state["feedback"] = FeedbackLog.from_events(events).to_dict()
        state["scheduler"]["feedback_event_count"] = len(events)
        state["scheduler"]["feedback_identity"] = state["feedback"]["identity"]
        state["scheduler"]["feedback_events_sha256"] = canonical_hash(events)
        epochs = state["scheduler"]["feedback_epochs"]
        epochs[:] = [e for e in epochs if e["feedback_event_count"] <= len(events)]
        for epoch in epochs:
            prefix = FeedbackLog.from_events(events[:epoch["feedback_event_count"]])
            epoch.update(feedback_identity=prefix.identity(),
                         feedback_events_sha256=canonical_hash(prefix.events))
        state["scheduler"]["selection_identity"] = canonical_hash(
            {k:v for k,v in state["scheduler"].items() if k != "selection_identity"})
    else:
        key = damage.removeprefix("budget_")
        state["continuation"]["budget"][key] = 0
    data["checkpoint_sha256"] = canonical_hash({k:v for k,v in data.items() if k != "checkpoint_sha256"})
    # The standard loader also refuses nested/reused output directories. Keep
    # the forged checkpoint in a sibling tree so the counts gates are reached.
    tampered = tmp_path / damage / "checkpoint.json"
    tampered.parent.mkdir()
    tampered.write_text(json.dumps(data))
    if damage.startswith("event_"):
        # The re-signed log AND scheduler reconstruct independently. Rejection
        # below must be the strict counts equation, not a stale outer hash.
        from r2f.campaign.feedback import FeedbackLog
        from r2f.campaign.scheduler import ExploratoryScheduler
        original_state = restore(out / "checkpoint.json", tmp_path / "reconstructed")
        feedback = FeedbackLog.from_events(state["feedback"]["events"])
        assert ExploratoryScheduler.from_dict(state["scheduler"], original_state["corpus"],
                                             feedback).to_dict() == state["scheduler"]
    error = ("checkpoint feedback/attempt/actual counts mismatch" if damage.startswith("event_")
             else "checkpoint .*(budget mismatch|counts mismatch)")
    with pytest.raises(ValueError, match=error):
        restore(tampered, tmp_path / "refused")


@pytest.mark.parametrize("actual", [True, False])
def test_existing_topology_qualification_rejection_keeps_attempt_only_parity(tmp_path, actual):
    result, calls, _, _ = execute(tmp_path / "topology-gate", status="not_applicable", actual=actual)
    row = assert_accounting(result, actual)
    assert len(calls) == 1
    assert result["abort"] is None
    assert not row["execution_qualification"]["real_ros_execution"]
    assert row["corpus_offer"]["reason"] == "topology_execution_not_qualified"
