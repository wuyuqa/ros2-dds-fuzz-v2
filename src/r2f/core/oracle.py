"""Four-state verdicts, ordered attribution and control/unknown gates."""

from dataclasses import dataclass
from enum import Enum

from .mutation import MutationReceipt
from .execution import validate_neutral, validate_fault_control_execution


class OracleVerdict(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True)
class TriageResult:
    triage_disposition: str
    responsibility_layer: str
    reason_code: str

    def __post_init__(self):
        if self.triage_disposition not in {"candidate", "pass", "deferred", "rejected"}:
            raise ValueError("invalid triage_disposition")
        if self.responsibility_layer not in {"sut", "observation", "harness", "mutation"}:
            raise ValueError("invalid responsibility_layer")
        if not isinstance(self.reason_code, str) or not self.reason_code.strip():
            raise ValueError("reason_code is required")
        if self.triage_disposition == "candidate" and self.responsibility_layer != "sut":
            raise ValueError("SUT candidate requires SUT responsibility")

    def to_dict(self):
        return dict(vars(self))


def _effect_names(observed):
    if isinstance(observed, str):
        return {observed}
    if isinstance(observed, list):
        return {v for item in observed for v in _effect_names(item)}
    if isinstance(observed, dict):
        names = set()
        for key in ("effect_id", "effects", "observations", "residuals", "forbidden_residuals"):
            names.update(_effect_names(observed.get(key)))
        # Also accept a typed map of observation-id -> strict boolean.
        names.update(key for key, value in observed.items() if value is True)
        return names
    return set()


def attribute_responsibility(record):
    def result(disposition, layer, reason):
        return TriageResult(disposition, layer, reason)
    if record.get("environment_healthy") is not True:
        return result("rejected", "harness", "environment_or_process")
    if record.get("preconditions_reached") is not True or record.get("execution_stage") not in {"effect_observed", "judged"}:
        return result("rejected", "harness", "reach_or_planning")
    if record.get("tool_error"):
        return result("rejected", "harness", "tool_error")
    if record.get("arm") == "negative":
        try:
            validate_fault_control_execution(record)
        except ValueError:
            return result("rejected", "harness", "tool_error")
        return result("rejected", "harness", "control_excluded")
    try:
        receipt = MutationReceipt.from_record(record)
        if record.get("arm") in {"baseline", "transparent"}:
            validate_neutral(record)
        elif not all((receipt.carrier_admitted, receipt.configured, receipt.applied, receipt.hit)):
            return result("rejected", "mutation", "mutation_ineffective")
    except ValueError:
        return result("rejected", "harness", "tool_error")
    if record.get("identity_complete") is not True or not (record.get("endpoint_identity") or record.get("instrumentation_identity")):
        return result("deferred", "observation", "unknown_evidence")
    expected = record.get("expected_effect_set")
    if (record.get("window_complete") is not True or record.get("expected_effect_evaluable") is not True
            or not isinstance(expected, dict) or "allowed_observations" not in expected or "forbidden_residuals" not in expected):
        return result("deferred", "observation", "window_incomplete")
    verdict = record.get("oracle_verdict")
    if verdict == "unknown" or verdict not in {v.value for v in OracleVerdict}:
        return result("deferred", "observation", "unknown_evidence")
    if verdict == "pass":
        return result("pass", "observation", "oracle_pass")
    if verdict == "not_applicable":
        return result("rejected", "observation", "not_applicable")
    names = _effect_names(record.get("observed_effect"))
    residuals = names.intersection(expected["forbidden_residuals"])
    # A forbidden residual outranks simultaneous allowed effects.
    if not residuals and record.get("primary_rule_violated") is not True and names.intersection(expected["allowed_observations"]):
        return result("rejected", "mutation", "mutation_expected_effect")
    if not residuals and record.get("primary_rule_violated") is not True:
        return result("deferred", "observation", "attribution_insufficient")
    if record.get("control_kind") or record.get("candidate_source") == "control":
        return result("rejected", "harness", "control_excluded")
    if record.get("controls_support_sut") is True and record.get("provenance_supports_sut") is True:
        return result("candidate", "sut", "sut_contract_violation")
    return result("deferred", "observation", "attribution_insufficient")


def validate_control(record):
    verdict = record.get("oracle_verdict")
    if verdict in {"unknown", "not_applicable"}:
        return False
    kind, arm = record.get("control_kind"), record.get("arm")
    if kind == "health":
        return arm == "baseline" and verdict == "pass"
    if kind == "fault":
        try:
            validate_fault_control_execution(record)
        except ValueError:
            return False
        return record.get("execution_stage") == "judged" and verdict == "fail"
    if kind == "mutation":
        try:
            receipt = MutationReceipt.from_record(record)
            return arm == "mutated" and receipt.hit
        except ValueError:
            return False
    if kind == "transparency":
        try:
            validate_neutral(record)
        except ValueError:
            return False
        return (arm == "transparent" and verdict == "pass"
                and record.get("transparency_expected") == "no_effect_or_expected_pass")
    raise ValueError("unknown control_kind")


def unknown_gate(rows, *, exploratory_fraction=0.1, exploratory_per_target=1):
    if not 0 <= exploratory_fraction <= 1 or type(exploratory_per_target) is not int or exploratory_per_target < 0:
        raise ValueError("invalid exploratory unknown budget")
    rows = list(rows)
    if any(r.get("control_kind") and r.get("campaign_kind", "required") == "required"
           and r.get("oracle_verdict") == "unknown" for r in rows):
        raise ValueError("required control unknown is a failure")
    applicable = [r for r in rows if r.get("applicable", True) is not False
                  and r.get("oracle_verdict") != "not_applicable" and not r.get("control_kind")]
    unknown = [r for r in applicable if r.get("oracle_verdict") == "unknown"]
    if any(r.get("campaign_kind", "required") == "required" for r in unknown):
        raise ValueError("required unknown exceeds zero budget")
    exploratory = [r for r in applicable if r.get("campaign_kind") == "exploratory"]
    exploratory_unknown = [r for r in unknown if r.get("campaign_kind") == "exploratory"]
    if exploratory and len(exploratory_unknown) > len(exploratory) * exploratory_fraction:
        raise ValueError("exploratory unknown fraction exceeded")
    for target_id in {r.get("target_id") for r in exploratory_unknown}:
        if sum(r.get("target_id") == target_id for r in exploratory_unknown) > exploratory_per_target:
            raise ValueError("per-target exploratory unknown budget exceeded")
    return True
