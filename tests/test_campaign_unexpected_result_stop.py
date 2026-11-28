"""Canonical per-cell stop policy; persisted offline fixtures, never DDS credit."""
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.runner import run_campaign
from test_campaign_evidence import fixture_pair


def unknown_tail(config, directory):
    result = fixture_pair(config, directory, fresh_matches=True)
    # A finite missing tail is unknown, not permanent delivery loss. Preserve
    # indexes/receipts while authoring NEW fixture bytes, never historical raw.
    last = next(e for e in reversed(result["events"]) if e["event"] == "receive")
    last["event"] = "fixture_missing_tail"
    raw = Path(result["raw_path"])
    raw.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in result["events"]))
    result["raw_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
    return result


def test_explicit_policy_stops_after_first_judged_unknown_and_retains_full_matrix(tmp_path):
    calls = []
    def runtime(config, directory):
        calls.append(config["cell_id"])
        return unknown_tail(config, directory)
    report = run_campaign(tmp_path, execute=True, runtime=runtime,
                          stop_on_unexpected_result=True, run_id="offline-stop-unknown")
    saved = json.loads((tmp_path / "report.json").read_text())
    assert len(calls) == 1
    assert len(saved["rows"]) == len(report["rows"]) == 24
    actual = [r for r in saved["rows"] if r["execution_stage"] != "planned"]
    assert len(actual) == 1
    assert actual[0]["execution_stage"] == "judged" and actual[0]["oracle_verdict"] == "unknown"
    assert saved["abort_reason"] == "unexpected_cell_result"
    assert saved["execution_stop_policy"]["stopped_cell_id"] == calls[0]
    assert saved["full_scope_required_cells"] == 72 and not saved["seal"]["sealed"]
    assert all(Path(actual[0][k]).is_file() for k in ("runtime_result_path", "replay_path", "raw_path"))


def test_expected_negative_fail_does_not_stop_normal_required_controls(tmp_path):
    calls = []
    def runtime(config, directory):
        calls.append(config["cell_id"])
        return fixture_pair(config, directory, fresh_matches=True)
    report = run_campaign(tmp_path, execute=True, runtime=runtime,
                          stop_on_unexpected_result=True, run_id="offline-stop-normal")
    assert len(calls) == 24 and report.get("abort_reason") is None
    assert all(r["oracle_verdict"] == ("fail" if r["arm"] == "negative" else "pass") for r in report["rows"])
    assert not report["seal"]["sealed"]  # Synthetic fixture never seals ROS evidence.


@pytest.mark.parametrize("value", [None, 0, 1, "true", {}, []])
def test_invalid_stop_policy_rejected_before_creating_plan(tmp_path, value):
    output = tmp_path / "invalid"
    with pytest.raises(ValueError, match="stop_on_unexpected_result must be a boolean"):
        run_campaign(output, stop_on_unexpected_result=value)
    assert not output.exists()


def test_policy_stops_on_tool_failure_with_diagnostics_retained(tmp_path):
    calls = []
    def broken(config, directory):
        calls.append(config["cell_id"])
        raise RuntimeError("authored offline carrier failure")
    report = run_campaign(tmp_path, execute=True, runtime=broken,
                          stop_on_unexpected_result=True, run_id="offline-stop-tool-error")
    assert len(calls) == 1 and report["abort_reason"] == "unexpected_cell_result"
    actual = next(r for r in report["rows"] if r["execution_stage"] != "planned")
    assert Path(actual["runtime_result_path"]).is_file()
    assert not report["seal"]["sealed"] and len(report["rows"]) == 24


def test_default_campaign_keeps_legacy_full_matrix_behavior(tmp_path):
    calls = []
    def runtime(config, directory):
        calls.append(config["cell_id"])
        return unknown_tail(config, directory)
    report = run_campaign(tmp_path, execute=True, runtime=runtime, run_id="offline-legacy-unknown")
    assert len(calls) == 24 and "execution_stop_policy" not in report


def test_stop_policy_is_execution_metadata_not_input_identity(tmp_path):
    legacy = run_campaign(tmp_path / "legacy", run_id="offline-policy-identity")
    strict = run_campaign(tmp_path / "strict", run_id="offline-policy-identity",
                          stop_on_unexpected_result=True)
    assert all(r["execution_stage"] == "planned" for r in strict["rows"])
    assert [r["effective_input_sha256"] for r in legacy["rows"]] == [r["effective_input_sha256"] for r in strict["rows"]]
    assert not strict["seal"]["sealed"]


def test_stop_policy_cannot_dispatch_automatic_replay_after_abort(tmp_path):
    output = tmp_path / "conflict"
    with pytest.raises(ValueError, match="stop_on_unexpected_result requires auto_replay=None"):
        run_campaign(output, stop_on_unexpected_result=True, auto_replay={})
    assert not output.exists()
