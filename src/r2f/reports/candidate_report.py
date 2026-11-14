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
        instrumented = row.get("instrumentation_observation", {}).get("observation_mode") == "instrumentation"
        evidence = dict(row, timestamp=row.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                        candidate_source="instrumentation" if instrumented else "blackbox",
                        evidence_tier="instrumented" if instrumented else "blackbox",
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
        candidate = dict(lifecycle.to_dict(), cell_id=row["cell_id"], observation_kind="primary", admission_error=admission_error)
        candidates.append(candidate)
    # Internal observations enrich the same canonical row, then enter the same
    # lifecycle. Unknown sidecars retain their intake count and never prove zero.
    observations=[]
    from r2f.campaign.observations import instrumentation_evidence, observation_links
    for row in rows:
        qualification=row.get("instrumentation_observation")
        if qualification is None:continue
        observations.append({"cell_id":row["cell_id"], "run_id":row["run_id"], **qualification})
        for index,item in enumerate(qualification.get("qualified_intake",[])):
            evidence=instrumentation_evidence(row,item)
            admission_error=None
            try:
                lifecycle=CandidateLifecycle(f"{row['run_id']}:{row['cell_id']}:instrumentation:{index}",evidence)
            except ValueError as exc:
                failures.append({"cell_id":row["cell_id"],"candidate_source":"instrumentation",
                                 "reason_code":"instrumentation_observation_identity_invalid","detail":str(exc),"evidence":evidence})
                continue
            try:
                lifecycle.admit()
                from r2f.campaign.triage import attribute
                diagnostic={"verdict":"fail","forbidden_residuals":[]}
                triage=attribute(evidence,evidence["typed_facts"],diagnostic)
                lifecycle.triage(TriageResult(*(triage[key] for key in ("triage_disposition","responsibility_layer","reason_code"))))
            except ValueError as exc:
                admission_error=str(exc)
            candidates.append(dict(lifecycle.to_dict(),cell_id=row["cell_id"],observation_kind="internal_contract",admission_error=admission_error))
    natural = [c for c in candidates if c.get("responsibility_layer") == "sut" and c.get("triage_disposition") == "candidate"]
    instrumentation_status, instrumentation_count = "not_run", None
    instrumented=[o for o in observations if o.get("observation_mode")=="instrumentation"]
    if instrumented:
        complete=all(o.get("qualification")=="qualified" for o in instrumented)
        instrumentation_status="completed" if complete else "incomplete"
        instrumentation_count=sum(o["admissible_count"] for o in instrumented) if complete else None
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
        if not instrumented or instrumentation_status != "completed" or instrumentation_summary["candidate_count"] != instrumentation_count:
            raise ValueError("instrumentation summary needs independently qualified per-execution observations")
    return {"schema_version": 1, "candidates": candidates, "failures": failures, "controls": controls,
            "instrumentation_status": instrumentation_status, "instrumentation_candidates": instrumentation_count,
            "instrumentation_linkage": observation_links(rows) if observations else "not_linked_to_this_blackbox_run",
            "instrumentation_observations":observations,
            "instrumentation_intake_count":sum(o["intake_count"] for o in instrumented) if instrumented and all(o.get("intake_count") is not None for o in instrumented) else None,
            "instrumentation_qualification":"qualified" if instrumentation_status=="completed" else "not_evaluated",
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
