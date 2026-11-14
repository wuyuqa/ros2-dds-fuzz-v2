"""Core lifecycle ledger retaining anomalies, failed runs, and zero-candidate evidence."""
from datetime import datetime, timezone
from r2f.core.candidate import CandidateLifecycle
from r2f.core.oracle import TriageResult


def candidate_ledger(rows, instrumentation_summary=None):
    rows = list(rows)
    candidates, failures, controls = [], [], []
    for row in rows:
        if row.get("arm") == "negative" or row.get("control_kind"):
            controls.append({"cell_id": row["cell_id"], "oracle_verdict": row.get("oracle_verdict"), "candidate_eligible": False, "reason_code": "control_observation"})
            continue
        if row.get("execution_stage") == "aborted" or row.get("oracle_verdict") in ("unknown", "not_applicable"):
            failures.append({key: row.get(key) for key in ("cell_id", "execution_stage", "oracle_verdict", "abort_reason", "reason_code", "raw_path", "raw_sha256")})
        if row.get("oracle_verdict") != "fail":
            continue
        evidence = dict(row, timestamp=row.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                        candidate_source="blackbox", evidence_tier="blackbox",
                        endpoint_identity={key: row.get("typed_facts", {}).get(key) for key in ("writer", "reader")})
        try:
            lifecycle = CandidateLifecycle(f"{row['run_id']}:{row['cell_id']}", evidence)
        except ValueError as exc:
            failures.append({"cell_id": row["cell_id"], "reason_code": "anomaly_without_minimum_observation_identity", "detail": str(exc)})
            continue
        admission_error = None
        try:
            lifecycle.admit()
            lifecycle.triage(TriageResult(*(row[key] for key in ("triage_disposition", "responsibility_layer", "reason_code"))))
        except ValueError as exc:
            # Never discard an observed anomaly when its admission fails.
            admission_error = str(exc)
        for attempt in row.get("replay_attempts", []):
            lifecycle.record_replay(attempt["status"], failure_reason=attempt.get("failure_reason", ""),
                                    **{k: v for k, v in attempt.items() if k not in {"status", "failure_reason", "timestamp", "attempt_id"}})
        if row.get("non_replayable"):
            lifecycle.mark_non_replayable(row["non_replayable"]["disposition"], row["non_replayable"]["reason"])
        if row.get("confirmation"):
            try:
                lifecycle.confirm(**row["confirmation"])
            except ValueError as exc:
                admission_error = str(exc)
        candidate = dict(lifecycle.to_dict(), cell_id=row["cell_id"], admission_error=admission_error)
        candidates.append(candidate)
    natural = [c for c in candidates if c.get("responsibility_layer") == "sut" and c.get("triage_disposition") == "candidate"]
    instrumentation_status, instrumentation_count = "not_run", None
    if instrumentation_summary is not None:
        expected_runs = {r["run_id"] for r in rows}
        expected_sources = [r.get("source_manifest") for r in rows if r.get("source_manifest")]
        if (expected_runs != {instrumentation_summary.get("run_id")} or not expected_sources
                or any(s != instrumentation_summary.get("source_manifest") for s in expected_sources)
                or instrumentation_summary.get("actual_run") is not True
                or instrumentation_summary.get("status") != "completed"
                or type(instrumentation_summary.get("candidate_count")) is not int
                or instrumentation_summary["candidate_count"] < 0):
            raise ValueError("instrumentation summary must reference actual same-run/source evidence")
        from r2f.core.evidence import verify_raw
        verify_raw(instrumentation_summary["raw_path"], instrumentation_summary["raw_sha256"])
        instrumentation_status, instrumentation_count = "completed", instrumentation_summary["candidate_count"]
    return {"schema_version": 1, "candidates": candidates, "failures": failures, "controls": controls,
            "instrumentation_status": instrumentation_status, "instrumentation_candidates": instrumentation_count,
            "instrumentation_linkage": "not_linked_to_this_blackbox_run" if instrumentation_summary is None else instrumentation_summary,
            "confirmed_candidates": sum(c["candidate_status"] == "confirmed" for c in candidates), "natural_candidate_count": len(natural),
            "zero_candidate_explanation": None if natural else "No independently supported SUT candidate was established. Controlled omissions are excluded; incomplete observations and failed executions remain in the ledger. This does not establish full S1 maturity."}


def audit_projection(candidate):
    """Ignore only lifecycle-generated times when replaying the same core API."""
    from copy import deepcopy
    item = deepcopy(candidate)
    item.get("evidence", {}).pop("timestamp", None)
    item.pop("adjudicated_at", None)
    for event in item.get("history", []):
        event.pop("timestamp", None)
        event.pop("adjudicated_at", None)
    for attempt in item.get("replay_attempts", []):
        attempt.pop("timestamp", None)
    if item.get("non_replayable"):
        item["non_replayable"].pop("timestamp", None)
    return item
