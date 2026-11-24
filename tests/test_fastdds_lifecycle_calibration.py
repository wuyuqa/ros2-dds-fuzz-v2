"""CP1 knowledge calibration through the existing runner/report boundary.

These offline checks cannot establish a PDP race observation or DDS execution.
"""
import json

import pytest

from r2f.campaign.runner import run_campaign
from r2f.reports.run_report import audit_report


def test_planned_report_exposes_source_lifecycle_obligations_without_runtime_credit(tmp_path):
    run_campaign(tmp_path / "plan", run_id="issue14-cp1-plan", execute=False)
    report = tmp_path / "plan" / "report.json"
    original = report.read_bytes()
    audit = audit_report(report)
    coverage = audit["s1_coverage"]
    assert coverage["query_valid"] is True
    case = next(row for row in coverage["cases"] if row["case_id"] == "Fast-DDS-issue3770")
    calibration = case["lifecycle_calibration"]

    assert calibration["checkpoint"] == "source_lifecycle_obligations"
    assert calibration["status"] == "requirements_identified_not_validated"
    assert calibration["obligation_ids"] == ["C01", "C02", "C06", "C08", "C16"]
    assert calibration["story_ids"] == [1, 5, 8, 14, 20, 21, 37]
    assert calibration["test_ids"] == ["T08", "T09", "T11"]
    assert {row["id"] for row in calibration["requirements"]} == {
        "proxy_lifetime", "entity_incarnation", "independent_observation",
        "normal_and_observer_controls", "historical_comparison",
    }
    historical = calibration["historical_comparison"]
    assert historical["pre"] == "8c3e3b30e769ef94bdbc04da519ed193855ad05c"
    assert historical["post"] == "305383295a9bbb9f12c5c0aa303667872d65ea33"
    assert historical["status"] == "proposed_not_executed"
    assert calibration["execution_evidence"] is False
    assert calibration["maturity_evidence"] is False
    assert calibration["new_discovery_credit"] == 0
    assert coverage["required_baseline"]["cell_count"] == 72
    assert len(coverage["required_baseline"]["target_revisions"]) == 4
    assert all(row["status"] == "unverified" for row in coverage["required_baseline"]["cells"])
    assert audit["sealed"] is False
    assert report.read_bytes() == original


def test_fixture_pass_preserves_unknown_internal_lifecycle_and_original_artifacts(tmp_path):
    from test_campaign_evidence import fixture_pair

    run_campaign(tmp_path / "fixture", run_id="issue14-cp1-fixture", execute=True, runtime=fixture_pair)
    report = tmp_path / "fixture" / "report.json"
    original = report.read_bytes()
    coverage = audit_report(report)["s1_coverage"]
    case = next(row for row in coverage["cases"] if row["case_id"] == "Fast-DDS-issue3770")
    calibration = case["lifecycle_calibration"]
    policy = calibration["assessment_policy"]
    assert policy["internal_race_status"] == "not_observed"
    assert policy["historical_reproduction_status"] == "unverified"
    assert policy["tool_or_control_result"] == "not_natural_sut_candidate"
    assert all(row["current_source_status"] == "unverified" and row["named_gaps"]
               for row in calibration["requirements"])
    assert coverage["report_evidence"]["rows"][0]["qualification"] == "fixture"
    assert all(row["status"] == "unverified" for row in coverage["required_baseline"]["cells"])
    assert case["reproduced_here"] is False and case["new_discovery"] is False
    assert report.read_bytes() == original


@pytest.mark.parametrize("fault", [
    "missing_calibration", "execution_credit", "maturity_credit", "new_credit",
    "race_observed", "historical_reproduced", "exact_parent", "unknown_source",
    "missing_lifecycle", "unknown_obligation", "missing_generation", "erased_gap",
    "wrong_parent_source", "wrong_parent_pointer", "null_reference", "null_policy", "null_history",
])
def test_report_fails_closed_on_invalid_or_overclaimed_lifecycle_capture(tmp_path, monkeypatch, fault):
    from r2f.modules.s1 import coverage

    run_campaign(tmp_path / "plan", run_id="issue14-invalid-capture", execute=False)
    data = json.loads(coverage._CAPTURE.read_text())
    case = next(row for row in data["cases"] if row["case_id"] == "Fast-DDS-issue3770")
    calibration = case["lifecycle_calibration"]
    if fault == "missing_calibration":
        case.pop("lifecycle_calibration")
    elif fault in {"execution_credit", "maturity_credit"}:
        calibration[fault.replace("_credit", "_evidence")] = True
    elif fault == "new_credit":
        calibration["new_discovery_credit"] = 1
    elif fault == "race_observed":
        calibration["assessment_policy"]["internal_race_status"] = "observed"
    elif fault == "historical_reproduced":
        calibration["assessment_policy"]["historical_reproduction_status"] = "reproduced"
    elif fault == "exact_parent":
        calibration["historical_comparison"]["pre"] = case["fix"]["api_base_sha_not_assumed_parent"]
    elif fault == "unknown_source":
        calibration["requirements"][0]["source_refs"][0]["artifact_id"] = "not-captured"
    elif fault == "missing_lifecycle":
        calibration["requirements"] = calibration["requirements"][1:]
    elif fault == "unknown_obligation":
        calibration["requirements"][0]["obligation_ids"] = ["C99"]
    elif fault == "wrong_parent_source":
        calibration["historical_comparison"]["pre_source_ref"]["artifact_id"] = "Fast-DDS-3770-issue.json"
    elif fault == "wrong_parent_pointer":
        calibration["historical_comparison"]["pre_source_ref"]["json_pointer"] = "/base/sha"
    elif fault == "null_policy":
        calibration["assessment_policy"] = None
    elif fault == "null_history":
        calibration["historical_comparison"] = None
    elif fault == "null_reference":
        calibration["requirements"][0]["source_refs"] = [None]
    elif fault == "missing_generation":
        calibration["execution_binding_required"].remove("generation")
    else:
        calibration["requirements"][0]["named_gaps"] = []
    capture = tmp_path / "invalid-coverage.json"
    capture.write_text(json.dumps(data))
    monkeypatch.setattr(coverage, "_CAPTURE", capture)
    result = audit_report(tmp_path / "plan" / "report.json")
    assert result["s1_coverage"]["query_valid"] is False
    assert result["sealed"] is False
    assert any("lifecycle calibration" in reason for reason in result["s1_coverage"]["named_gaps"])


def test_registry_lineage_resolves_captured_sources_and_model_context_without_execution():
    from r2f.core import Registry
    from r2f.modules.s1.module import register_exploratory

    registry = Registry()
    register_exploratory(registry, include_required=True, runtime_capability="topology_v1")
    record = registry.get_rule_lineage("discovery.endpoint_topology.fuzz_v1",
                                       "s1.topology.program_consistency.v1")
    lineage = record["coverage_lineage"]
    case = next(row for row in lineage["cases"] if row["case_id"] == "Fast-DDS-issue3770")
    calibration = case["lifecycle_calibration"]
    captures = lineage["source_captures"]
    assert captures["Fast-DDS-3770-issue.json"]["artifact_sha256"] == (
        "86e0c74f4580290db3824035b4461369a949d799d6776275af2f011e61faf7fc")
    assert captures["Fast-DDS-4220-merge.json"]["artifact_sha256"] == (
        "7d37380fde3abf6f23ff4ff7482f4899a3b7fdfc1461cd59e55788261b125683")
    obligations = {row["id"]: row for row in lineage["obligations"]}
    for requirement in calibration["requirements"]:
        for ref in requirement["source_refs"]:
            assert captures[ref["artifact_id"]]["verification_scope"] == "captured_artifact_only"
        for identifier in requirement["obligation_ids"]:
            assert obligations[identifier]["model_links"]
            assert all(link["target_id"] in lineage["model_bindings"]
                       for link in obligations[identifier]["model_links"])
    peer = next(row for row in calibration["observation_layers"] if row["layer"] == "endpoint_peer_snapshot")
    assert "Not a PDP proxy" in peer["limitation"]
    assert record["execution_evidence"] is False and case["reproduced_here"] is False
    calibration["requirements"][0]["named_gaps"].clear()
    other = registry.get_rule_lineage("discovery.endpoint_topology.fuzz_v1",
                                      "s1.topology.program_consistency.v1")
    case = next(row for row in other["coverage_lineage"]["cases"] if row["case_id"] == "Fast-DDS-issue3770")
    assert case["lifecycle_calibration"]["requirements"][0]["named_gaps"] == [
        "pdp_proxy_lifecycle_trace_not_observed"]
