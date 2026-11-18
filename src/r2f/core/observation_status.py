"""Shared fail-closed completeness gate; no execution or raw-file claims.

Flat completeness flags supplement (never override) the legacy nested window.
A complete observation is not necessarily a raw-backed state transition.
"""


def assess_facts(facts):
    reasons = []
    if not isinstance(facts, dict):
        return {"complete": False, "reasons": ["facts_not_mapping"]}
    for key in ("environment_healthy", "process_healthy", "preconditions_reached",
                "endpoint_identity_complete"):
        if facts.get(key) is not True:
            reasons.append(key + "_not_true")
    for key in ("observation_complete", "identity_consistent",
                "endpoint_identity_consistent"):
        if key in facts and facts[key] is not True:
            reasons.append(key + "_not_true")
    windows = []
    if "window" in facts:
        window = facts["window"]
        windows.append(window.get("complete") if isinstance(window, dict) else None)
    if "window_completion" in facts:
        window = facts["window_completion"]
        windows.append(window.get("complete") if isinstance(window, dict) else None)
    if "window_complete" in facts:
        windows.append(facts["window_complete"])
    if not windows or any(value is not True for value in windows):
        reasons.append("window_complete_not_true")
    for key in ("evidence_error", "evidence_errors"):
        if facts.get(key) not in (None, False, "", [], {}):
            reasons.append(key)
    return {"complete": not reasons, "reasons": reasons}


def tool_error(record):
    if not isinstance(record, dict):
        return True
    return bool(record.get("tool_error") or record.get("oracle_tool_failure")
                or record.get("runtime_status") == "tool_error"
                or record.get("reason_code") == "tool_error")


def assess_observation(record):
    if not isinstance(record, dict):
        return {"complete": False, "reasons": ["record_not_mapping"]}
    assessment = assess_facts(record.get("typed_facts"))
    reasons = list(assessment["reasons"])
    if record.get("execution_stage") != "judged":
        reasons.append("execution_stage_not_judged")
    if not (record.get("actual_run") is True or record.get("workspace_executed") is True):
        reasons.append("execution_not_observed")
    for key in ("actual_run", "workspace_executed"):
        if key in record and type(record[key]) is not bool:
            reasons.append(key + "_not_boolean")
    if ("actual_run" in record and "workspace_executed" in record
            and record["actual_run"] is not record["workspace_executed"]):
        reasons.append("execution_flags_conflict")
    if tool_error(record):
        reasons.append("tool_error")
    for key in ("evidence_error", "evidence_errors"):
        if record.get(key) not in (None, False, "", [], {}):
            reasons.append(key)
    return {"complete": not reasons, "reasons": reasons}


def observation_complete(record):
    return assess_observation(record)["complete"]
