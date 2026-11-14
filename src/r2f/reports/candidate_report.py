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
    instrumentation_admission_failures=[]
    instrumentation_admitted_count=0
    from r2f.campaign.observations import instrumentation_evidence, observation_links
    for row in rows:
        qualification=row.get("instrumentation_observation")
        if qualification is None:continue
        observation_record={"cell_id":row["cell_id"], "run_id":row["run_id"], **qualification}
        observations.append(observation_record)
        if qualification.get("qualification") != "qualified":
            observation_record["lifecycle_accounting"]={
                "qualified_intake_count":None,"admitted_count":0,"status":"not_evaluated","admission_failures":[]}
            continue
        intake=qualification.get("qualified_intake")
        count=qualification.get("admissible_count")
        if not isinstance(intake,list) or type(count) is not int or count<0 or count!=len(intake):
            detail="qualified intake count does not match qualified intake records"
            if isinstance(intake,list) and intake:
                # The batch count is inconsistent, so reject every actual intake
                # independently; none may proceed to lifecycle admission.
                row_failures=[{"cell_id":row["cell_id"],"run_id":row["run_id"],
                               "candidate_source":"instrumentation","intake_index":index,
                               "reason_code":"instrumentation_qualified_intake_count_mismatch",
                               "detail":detail,"evidence":{"intake":item,"admissible_count":count}}
                              for index,item in enumerate(intake)]
            else:
                # No concrete intake exists to index (for example, a claimed
                # positive count with an empty intake); preserve the row-level
                # mismatch without inventing a candidate intake.
                row_failures=[{"cell_id":row["cell_id"],"run_id":row["run_id"],
                               "candidate_source":"instrumentation","intake_index":None,
                               "reason_code":"instrumentation_qualified_intake_count_mismatch",
                               "detail":detail,"evidence":{"intake_count":len(intake) if isinstance(intake,list) else None,
                                                               "admissible_count":count}}]
            failures.extend(row_failures)
            instrumentation_admission_failures.extend(row_failures)
            observation_record["lifecycle_accounting"]={
                "qualified_intake_count":None,"admitted_count":0,"status":"incomplete",
                "admission_failures":row_failures}
            continue
        admitted_before=instrumentation_admitted_count
        row_failures=[]
        for index,item in enumerate(intake):
            try:
                evidence=instrumentation_evidence(row,item)
            except (ValueError,KeyError,TypeError) as exc:
                failure={"cell_id":row["cell_id"],"run_id":row["run_id"],"candidate_source":"instrumentation",
                         "intake_index":index,"reason_code":"instrumentation_intake_identity_mismatch","detail":str(exc),
                         "evidence":{"intake":item,"canonical_identity":{k:row.get(k) for k in
                                    ("run_id","target_id","target_revision","rmw","arm","repeat","effective_input_sha256","raw_path","raw_sha256")}}}
                failures.append(failure)
                instrumentation_admission_failures.append(failure)
                row_failures.append(failure)
                continue
            admission_error=None
            try:
                lifecycle=CandidateLifecycle(f"{row['run_id']}:{row['cell_id']}:instrumentation:{index}",evidence)
            except ValueError as exc:
                failure={"cell_id":row["cell_id"],"candidate_source":"instrumentation",
                         "reason_code":"instrumentation_lifecycle_observation_failed","detail":str(exc),
                         "lifecycle_status":"observed_rejected","evidence":evidence}
                failures.append(failure)
                admission_failure={"cell_id":row["cell_id"],"run_id":row["run_id"],"intake_index":index,
                                   "reason_code":failure["reason_code"],"detail":str(exc),"evidence":evidence}
                instrumentation_admission_failures.append(admission_failure)
                row_failures.append(admission_failure)
                continue
            try:
                lifecycle.admit()
                instrumentation_admitted_count += 1
                from r2f.campaign.triage import attribute
                diagnostic={"verdict":"fail","forbidden_residuals":[]}
                triage=attribute(evidence,evidence["typed_facts"],diagnostic)
                lifecycle.triage(TriageResult(*(triage[key] for key in ("triage_disposition","responsibility_layer","reason_code"))))
            except ValueError as exc:
                admission_error=str(exc)
                if lifecycle.candidate_status == "observed":
                    failure={"cell_id":row["cell_id"],"run_id":row["run_id"],"candidate_source":"instrumentation",
                             "intake_index":index,"reason_code":"instrumentation_lifecycle_admission_failed","detail":admission_error,
                             "lifecycle":lifecycle.to_dict(),"evidence":evidence}
                    failures.append(failure)
                    instrumentation_admission_failures.append(failure)
                    row_failures.append(failure)
                    continue
            candidates.append(dict(lifecycle.to_dict(),cell_id=row["cell_id"],observation_kind="internal_contract",admission_error=admission_error))
        observation_record["lifecycle_accounting"]={
            "qualified_intake_count":len(intake),
            "admitted_count":instrumentation_admitted_count-admitted_before,
            "status":"admission_failed" if row_failures else "completed",
            "admission_failures":row_failures}
    natural = [c for c in candidates if c.get("responsibility_layer") == "sut" and c.get("triage_disposition") == "candidate"]
    instrumentation_status = "not_run"
    instrumented=[o for o in observations if o.get("observation_mode")=="instrumentation"]
    if instrumented:
        counts=[]
        complete=True
        for observation in instrumented:
            intake=observation.get("qualified_intake")
            count=observation.get("admissible_count")
            if (observation.get("qualification")!="qualified" or not isinstance(intake,list)
                    or type(count) is not int or count<0 or count!=len(intake)):
                complete=False
            else:
                counts.append(len(intake))
        instrumentation_status="completed" if complete else "incomplete"
        instrumentation_qualified_count=sum(counts) if complete else None
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
        if not instrumented or instrumentation_status != "completed" or instrumentation_summary["candidate_count"] != instrumentation_qualified_count:
            raise ValueError("instrumentation summary needs independently qualified per-execution observations")
    return {"schema_version": 1, "candidates": candidates, "failures": failures, "controls": controls,
            "instrumentation_status": instrumentation_status,
            "instrumentation_candidates": instrumentation_admitted_count if instrumented else None,
            "instrumentation_admitted_count": instrumentation_admitted_count if instrumented else None,
            "instrumentation_qualified_intake_count": instrumentation_qualified_count if instrumented else None,
            "instrumentation_lifecycle_status": (
                "not_run" if not instrumented else "incomplete" if instrumentation_status != "completed" else
                "admission_failed" if instrumentation_admission_failures else "completed"),
            "instrumentation_admission_failures": instrumentation_admission_failures,
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
