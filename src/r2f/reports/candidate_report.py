"""Core lifecycle ledger retaining anomalies, failed runs, and zero-candidate evidence.

Two report kinds share one lifecycle and one ledger implementation:

* ``candidate_ledger`` is the required-campaign projection.  Its behaviour is
  unchanged.
* ``exploratory_candidate_ledger`` records budget-driven fuzz observations.  It
  additionally separates tool errors, incomplete observations and controls from
  natural candidates, and it never confirms a candidate without an independent
  review that names concrete source/raw/capsule references.

Review packets are attachments to the candidate lifecycle, not a second
candidate state machine.
"""
import fcntl
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from r2f.core.candidate import (CandidateLifecycle, REVIEW_EVIDENCE_KEYS, topology_observation_identity,
                                validate_human_history, validate_human_record, candidate_execution_identity,
                                HUMAN_ADJUDICATION_SCHEMA, validate_root_cause, validate_tool_repair_handoff,
                                validate_review_evidence, validate_capability_assessment)
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.oracle import TriageResult
from r2f.reports.root_cause import current_review_projection, root_cause_accounting

REVIEW_PACKET_SCHEMA = "r2f.s1-review-packet/v1"
REVIEW_RECORD_SCHEMA = "r2f.s1-review-record/v1"
REVIEW_ATTRIBUTIONS = frozenset({"sut_candidate", "tool", "environment", "allowed", "unknown"})
REVIEW_CONCLUSIONS = frozenset({"supported", "rejected", "needs_evidence"})
REVIEW_PACKET_KEYS = (
    "schema_version", "packet_id", "candidate_id", "identity", "parent", "input", "chain",
    "layer_receipts", "raw", "rules", "facts", "controls", "observation_health",
    "negative_explanations", "replay", "dedupe", "review_status",
)


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


EXPLORATORY_INPUT_KEYS = ("surface_ids", "boundary_class", "state_edge", "mutation_plan",
                           "profile_id", "seed_id", "parent_content_sha256")


def _exploratory_evidence(row):
    instrumented = (row.get("instrumentation_observation") or {}).get("observation_mode") == "instrumentation"
    return dict(row, timestamp=row.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                candidate_source="instrumentation" if instrumented else "blackbox",
                evidence_tier="instrumented" if instrumented else "blackbox",
                endpoint_identity=(topology_observation_identity(row)
                    if (row.get("typed_facts") or {}).get("schema") == "r2f.s1-topology-facts/v1"
                    else {key: (row.get("typed_facts") or {}).get(key)
                          for key in ("writer", "reader")}))


def _review_errors(review, candidate_id):
    """Validate a review record before it can influence lifecycle state."""
    problems = []
    if not isinstance(review, dict):
        return ["review_record_not_an_object"]
    if review.get("schema_version") not in (None, REVIEW_RECORD_SCHEMA):
        problems.append("review_schema_unknown")
    if review.get("candidate_id") not in (None, candidate_id):
        problems.append("review_candidate_mismatch")
    if not isinstance(review.get("reviewer_id"), str) or not review["reviewer_id"].strip():
        problems.append("review_without_reviewer")
    if not isinstance(review.get("conclusion"), str) or review["conclusion"] not in REVIEW_CONCLUSIONS:
        problems.append("review_conclusion_invalid")
    refs = review.get("evidence_refs")
    if not isinstance(refs, dict) or not refs:
        problems.append("review_without_evidence_references")
    else:
        for key in ("source", "raw", "capsule"):
            if not isinstance(refs.get(key), str) or not refs[key].strip():
                problems.append("review_missing_reference:" + key)
        if set(refs) - REVIEW_EVIDENCE_KEYS:
            problems.append("review_reference_unknown")
        if any(not isinstance(value, str) or not value.strip() for value in refs.values()):
            problems.append("review_reference_invalid")
    independent = review.get("independent_of", [])
    if not isinstance(independent, list) or any(
            not isinstance(item, str) or not item.strip() for item in independent):
        problems.append("review_independence_invalid")
    elif review.get("reviewer_id") in independent:
        problems.append("reviewer_not_independent")
    return problems


def exploratory_candidate_ledger(rows, reviews=None, *, human_records=None, human_authority=None):
    """Ledger for exploratory runs.

    Natural candidates are only rows that were actually executed, judged
    ``fail``, are not controls, are not tool errors and have a complete
    observation.  Every admitted anomaly keeps its raw identity; nothing is
    dropped because admission or confirmation fails.
    """
    rows = list(rows)
    human_records = list(human_records or [])
    human_errors, applied_human = [], []
    reviews = list(reviews or [])
    candidates, failures, controls, review_errors = [], [], [], []
    originals = {f"{row.get('run_id')}:{row.get('cell_id')}": row for row in rows}
    # Index claims before validation: even a rejected record can claim a later
    # ID/predecessor. Accepted-prefix lookup makes qualification order-dependent.
    claims_by_id = {}
    def claimed_candidates(review):
        affected = set()
        if isinstance(review, dict):
            if isinstance(review.get("candidate_id"), str):
                affected.add(review["candidate_id"])
            binding = review.get("execution_identity")
            if isinstance(binding, dict):
                bound_id = f"{binding.get('run_id')}:{binding.get('cell_id')}"
                if bound_id in originals:
                    affected.add(bound_id)
        return affected
    for review in reviews:
        if isinstance(review, dict) and isinstance(review.get("record_id"), str):
            claims_by_id.setdefault(review["record_id"], []).append(review)
    imported, by_id = [], {}
    for index, review in enumerate(reviews):
        candidate_id = review.get("candidate_id") if isinstance(review, dict) else None
        row = originals.get(candidate_id) if isinstance(candidate_id, str) else None
        problems = _review_import_errors(review, row)
        if row is None:
            problems.append("review_original_execution_missing")
        if not problems:
            previous = by_id.get(review["record_id"])
            if previous is not None:
                if previous == review:
                    continue
                problems.append("review_record_id_conflict")
            else:
                problems.extend(_review_history_errors(review, imported))
        if problems:
            affected = claimed_candidates(review)
            if isinstance(review, dict):
                for key in ("record_id", "supersedes_record_id"):
                    ref = review.get(key)
                    for claim in claims_by_id.get(ref, []) if isinstance(ref, str) else []:
                        affected.update(claimed_candidates(claim))
            for affected_id in sorted(affected) if affected else [None]:
                review_errors.append({"index": index, "candidate_id": affected_id,
                                      "reason_code": "review_rejected", "detail": ",".join(problems)})
            continue
        imported.append(review)
        by_id[review["record_id"]] = review
    invalid_review_candidates = {e["candidate_id"] for e in review_errors
                                 if isinstance(e["candidate_id"], str)}
    # Relationships depend on the complete referenced chain, never an accepted
    # prefix. Rejection propagates only along explicit cross-case relationships.
    while True:
        dependent = {r["candidate_id"] for r in imported if any(
            claimed_candidates(linked) & invalid_review_candidates
            for ref in (r.get("root_cause") or {}).get("related_record_ids", [])
            for linked in claims_by_id.get(ref, []))}
        added = dependent - invalid_review_candidates
        if not added:
            break
        invalid_review_candidates.update(added)
        review_errors.extend({"candidate_id": candidate_id, "reason_code": "review_rejected",
                              "detail": "root_cause_related_history_invalid"}
                             for candidate_id in sorted(added))
    superseded = {r["supersedes_record_id"] for r in imported if r.get("supersedes_record_id")}
    by_candidate = {}
    for review in imported:
        if review["record_id"] not in superseded and review["candidate_id"] not in invalid_review_candidates:
            by_candidate.setdefault(review["candidate_id"], []).append(review)
    validated_human = []
    for index, record in enumerate(human_records):
        candidate_id = record.get("candidate_id") if isinstance(record, dict) else None
        original = originals.get(candidate_id) if isinstance(candidate_id, str) else None
        try:
            validate_human_record(record, original, human_authority=human_authority)
            if validate_human_history(record, [r for _, r in validated_human]):
                validated_human.append((index, record))
        except (ValueError, TypeError, KeyError, OSError) as exc:
            human_errors.append({"index": index, "candidate_id": candidate_id, "detail": str(exc)})
    # A rejected record invalidates its candidate's full chain for current
    # qualification. Never restore an accepted prefix as a replacement decision.
    invalid_human_candidates = {error["candidate_id"] for error in human_errors
                                if isinstance(error["candidate_id"], str)}
    for row in rows:
        cell_id = row.get("cell_id")
        if row.get("arm") == "negative" or row.get("control_kind"):
            controls.append({
                "run_id": row.get("run_id"), "cell_id": cell_id, "oracle_verdict": row.get("oracle_verdict"),
                "arm": row.get("arm"), "kind": row.get("control_kind"),
                "candidate_eligible": False, "reason_code": "control_observation",
                "source": ("harness" if (row.get("arm") == "negative" or row.get("control_kind") == "fault")
                           and isinstance(row.get("fault_control_receipt"), dict) else
                           "unattributed_control" if (row.get("arm") == "negative"
                                                      or row.get("control_kind") == "fault") else
                           "declared_control"),
                "harness_fault": (row.get("fault_control_receipt") or {}).get("fault_id"),
                "raw_sha256": row.get("raw_sha256"),
            })
            continue
        if row.get("tool_error") is True or row.get("runtime_status") == "tool_error":
            failures.append({key: row.get(key) for key in (
                "run_id", "cell_id", "execution_stage", "oracle_verdict", "abort_reason", "reason_code",
                "raw_path", "raw_sha256", "tool_error_detail")} | {"reason_code": "tool_error"})
            continue
        if row.get("observer_dropped") is True or row.get("observer_complete") is False:
            failures.append({key: row.get(key) for key in (
                "run_id", "cell_id", "execution_stage", "oracle_verdict", "abort_reason", "raw_path", "raw_sha256")}
                | {"reason_code": "observer_incomplete"})
            continue
        if row.get("execution_stage") == "aborted" or row.get("oracle_verdict") in (
                "unknown", "not_applicable") or row.get("execution_stage") != "judged":
            failures.append({key: row.get(key) for key in (
                "run_id", "cell_id", "execution_stage", "oracle_verdict", "abort_reason", "reason_code",
                "raw_path", "raw_sha256")})
            continue
        if row.get("oracle_verdict") != "fail":
            continue
        evidence = _exploratory_evidence(row)
        try:
            lifecycle = CandidateLifecycle(f"{row['run_id']}:{cell_id}", evidence)
        except (ValueError, KeyError) as exc:
            failures.append({"run_id": row.get("run_id"), "cell_id": cell_id, "reason_code": "anomaly_without_minimum_observation_identity",
                             "detail": str(exc), "raw_path": row.get("raw_path"),
                             "raw_sha256": row.get("raw_sha256")})
            continue
        admission_error = None
        try:
            lifecycle.admit()
            lifecycle.triage(TriageResult(*(row[key] for key in (
                "triage_disposition", "responsibility_layer", "reason_code"))))
        except (ValueError, KeyError) as exc:
            admission_error = str(exc)
        candidate_id = lifecycle.candidate_id
        applied_reviews = []
        candidate_human = [(index, record) for index, record in validated_human
                           if record["candidate_id"] == candidate_id]
        lifecycle_reviews = ([] if candidate_id in invalid_review_candidates else
                             [review for review in imported if review["candidate_id"] == candidate_id]
                             if candidate_human else by_candidate.get(candidate_id, []))
        for review in lifecycle_reviews:
            problems = _review_errors(review, candidate_id)
            if problems:
                review_errors.append({"candidate_id": candidate_id,
                                      "reason_code": "review_rejected",
                                      "detail": ",".join(problems)})
                continue
            try:
                lifecycle.record_review(reviewer_id=review["reviewer_id"],
                                        conclusion=review["conclusion"],
                                        evidence_refs=review["evidence_refs"],
                                        independent_of=review.get("independent_of") or [],
                                        notes=review.get("notes", ""),
                                        attribution=review.get("attribution"),
                                        root_cause=review.get("root_cause"),
                                        tool_repair_handoff=review.get("tool_repair_handoff"),
                                        capability_assessment=review.get("capability_assessment"))
                if review["record_id"] not in superseded:
                    applied_reviews.append(review)
            except ValueError as exc:
                review_errors.append({"candidate_id": candidate_id,
                                      "reason_code": "review_rejected", "detail": str(exc)})
        for attempt in row.get("replay_attempts") or []:
            lifecycle.record_replay(attempt["status"], failure_reason=attempt.get("failure_reason", ""),
                                    **{k: v for k, v in attempt.items()
                                       if k not in {"status", "failure_reason", "timestamp", "attempt_id"}})
        if row.get("non_replayable"):
            lifecycle.mark_non_replayable(row["non_replayable"]["disposition"],
                                          row["non_replayable"]["reason"])
        confirmation_error = None
        if row.get("confirmation"):
            try:
                lifecycle.confirm(**row["confirmation"], require_review=True)
            except ValueError as exc:
                # A supported review never confirms by itself; only an explicit
                # adjudication with a valid independent review may.
                confirmation_error = str(exc)
        human_qualification_error = None
        if candidate_id in invalid_review_candidates:
            human_qualification_error = "review_history_invalid"
            invalid_human_candidates.add(candidate_id)
        elif candidate_id in invalid_human_candidates:
            human_qualification_error = "human_history_invalid"
        elif candidate_human:
            try:
                lifecycle.restore_human_history([record for _, record in candidate_human],
                                                original_row=row, human_authority=human_authority)
                applied_human.extend(record for _, record in candidate_human)
                try:
                    lifecycle.record_human_adjudication(candidate_human[-1][1], original_row=row,
                                                        human_authority=human_authority)
                except (ValueError, TypeError, KeyError, OSError) as exc:
                    human_qualification_error = str(exc)
            except (ValueError, TypeError, KeyError, OSError) as exc:
                invalid_human_candidates.add(candidate_id)
                human_qualification_error = "human_history_invalid"
                for index, record in candidate_human:
                    human_errors.append({"index": index, "candidate_id": candidate_id, "detail": str(exc)})
        review_status = "not_reviewed"
        if applied_reviews:
            last = applied_reviews[-1]
            review_status = {"supported": "supported_awaiting_adjudication",
                             "rejected": "review_rejected_by_independent_agent",
                             "needs_evidence": "needs_evidence"}[last["conclusion"]]
        if lifecycle.candidate_status == "confirmed":
            review_status = "confirmed_after_review"
        entry = dict(lifecycle.to_dict(), run_id=row.get("run_id"), cell_id=cell_id, observation_kind="primary",
                     admission_error=admission_error, confirmation_error=confirmation_error,
                     review_status=review_status,
                     review_errors=[e["detail"] for e in review_errors if e["candidate_id"] == candidate_id],
                     **{key: row.get(key) for key in EXPLORATORY_INPUT_KEYS if key in row})
        if human_qualification_error:
            entry["human_qualification_error"] = human_qualification_error
        candidates.append(entry)
    for index, record in validated_human:
        if record not in applied_human and not any(e["index"] == index for e in human_errors):
            human_errors.append({"index": index, "candidate_id": record["candidate_id"],
                                 "detail": ("human_history_invalid" if record["candidate_id"]
                                            in invalid_human_candidates else "human_candidate_not_eligible")})
    applied_human.sort(key=lambda r: next(i for i, v in validated_human if v == r))
    rows_by_execution = {(row.get("run_id"), row.get("cell_id")): row for row in rows}
    for entry in [*candidates, *failures, *controls]:
        original = rows_by_execution[(entry["run_id"], entry["cell_id"])]
        identity = f"{original.get('run_id')}:{original.get('cell_id')}"
        active = by_candidate.get(identity, [])
        entry["original_result"] = {key: original.get(key) for key in (
            "run_id", "cell_id", "oracle_verdict", "rule_id", "triage_disposition", "responsibility_layer",
            "reason_code", "raw_path", "raw_sha256")}
        entry["analysis"] = {**current_review_projection(active),
                             "history": snapshot_record([
                                 r for index, r in enumerate(reviews if identity in invalid_review_candidates else imported)
                                 if isinstance(r, dict) and (r.get("candidate_id") == identity or
                                     (identity in invalid_review_candidates and any(
                                         e.get("index") == index and e["candidate_id"] == identity
                                         for e in review_errors)))])}
        if identity in invalid_human_candidates:
            # Preserve rejected records and their suffix for audit/display only;
            # none of this chain has current human confirmation authority.
            entry["human_adjudication"] = {
                "current": None,
                "history": snapshot_record([record for record in human_records
                                            if isinstance(record, dict)
                                            and record.get("candidate_id") == identity])}
        entry.setdefault("human_adjudication", {"current": None, "history": []})
    natural = [c for c in candidates if c.get("responsibility_layer") == "sut"
               and c.get("triage_disposition") == "candidate"]
    return {
        "schema_version": 1,
        "campaign_kind": "exploratory",
        "root_cause_accounting": root_cause_accounting([*candidates, *failures, *controls], originals),
        "candidates": candidates,
        "failures": failures,
        "controls": controls,
        "review_errors": review_errors,
        "review_records": imported,
        "human_adjudication_schema": HUMAN_ADJUDICATION_SCHEMA,
        "human_records": applied_human,
        "human_errors": human_errors,
        "human_confirmed_candidates": sum(
            (c["human_adjudication"]["current"] or {}).get("action") == "confirm"
            and not c.get("human_qualification_error") for c in candidates),
        "confirmed_candidates": sum(c.get("candidate_status") == "confirmed" for c in candidates),
        "natural_candidate_count": len(natural),
        "tool_error_count": sum(1 for f in failures if f.get("reason_code") == "tool_error"),
        "observer_incomplete_count": sum(1 for f in failures if f.get("reason_code") == "observer_incomplete"),
        "control_count": len(controls),
        "zero_candidate_explanation": None if natural else (
            "No independently supported SUT candidate was established. Controls, tool errors and "
            "incomplete observations are excluded and retained separately. This does not establish "
            "exploratory or required S1 maturity."),
    }


def review_packet(row, *, identity=None, chain=None):
    """Build a review packet attached to an already observed candidate row.

    The packet is descriptive evidence for an independent reviewer.  It never
    creates a candidate, never changes lifecycle state and never rewrites raw.
    """
    if not isinstance(row, dict):
        raise ValueError("review packet requires a candidate row")
    for key in ("run_id", "target_id", "cell_id"):
        if not isinstance(row.get(key), str) or not row[key]:
            raise ValueError("review packet row is missing " + key)
    facts = row.get("typed_facts") or {}
    packet = {
        "schema_version": REVIEW_PACKET_SCHEMA,
        "packet_id": f"{row['run_id']}:{row['cell_id']}",
        "candidate_id": f"{row['run_id']}:{row['cell_id']}",
        "identity": {
            "source": (identity or {}).get("source_sha256") or row.get("source_identity"),
            "target_id": row.get("target_id"),
            "target_revision": row.get("target_revision"),
            "target_sha256": (identity or {}).get("target_sha256"),
            "model": (identity or {}).get("model_sha256") or row.get("model_sha256"),
            "rule": row.get("rule_id"),
            "rule_sha256": (identity or {}).get("rule_sha256"),
            "mutator": row.get("mutator_id"),
            "mutation_plan_sha256": row.get("mutation_plan_sha256"),
            "profile": row.get("profile_id"),
            "profile_sha256": (identity or {}).get("profile_sha256"),
        },
        "parent": {"seed_id": row.get("seed_id"),
                   "parent_content_sha256": row.get("parent_content_sha256")},
        "input": {"effective_input_sha256": row.get("effective_input_sha256"),
                  "final_input": row.get("final_input") or (row.get("effective_input") or {}).get("final_input")},
        "chain": chain if chain is not None else row.get("mutation_chain") or [],
        "layer_receipts": row.get("input_layer_receipts") or [],
        "raw": {"raw_path": row.get("raw_path"), "raw_sha256": row.get("raw_sha256")},
        "rules": {"primary": row.get("rule_id"),
                  "supporting": list(row.get("supporting_rule_ids") or []),
                  "oracle_verdict": row.get("oracle_verdict")},
        "facts": {"typed": facts, "missing": list(row.get("missing_facts") or [])},
        "controls": list(row.get("control_references") or []),
        "observation_health": {
            "observer_complete": row.get("observer_complete"),
            "observer_dropped": row.get("observer_dropped"),
            "tool_error": row.get("tool_error") is True,
            "window_complete": row.get("window_complete"),
        },
        "negative_explanations": list(row.get("negative_explanations") or []),
        "replay": {
            "capsule_path": row.get("replay_path"),
            "command": row.get("replay_command"),
            "attempts": len(row.get("replay_attempts") or []),
            "non_replayable": row.get("non_replayable"),
            "not_executed_reason": None if (row.get("replay_attempts") or row.get("replay_path"))
            else "no replay artifact was produced for this observation",
        },
        "dedupe": {
            "content_sha256": row.get("content_sha256"),
            "signature_sha256": row.get("signature_sha256"),
            "similar_candidate_ids": list(row.get("similar_candidate_ids") or []),
            "merge_policy": "never merge without matching root-cause evidence",
        },
        "review_status": "awaiting_independent_review",
    }
    def _empty(value):
        if value in (None, "", []):
            return True
        if isinstance(value, dict):
            return all(_empty(item) for item in value.values())
        return False

    # Some packet sections need more than "non-empty": a raw reference without
    # its hash, or an input without an effective hash, is still missing evidence.
    requirements = {
        "identity": ("source", "target_id", "target_revision", "model", "rule"),
        "input": ("effective_input_sha256",),
        "raw": ("raw_path", "raw_sha256"),
        "rules": ("primary",),
        "replay": ("capsule_path",),
    }
    missing = [key for key in REVIEW_PACKET_KEYS
               if key not in {"schema_version", "review_status"} and _empty(packet.get(key))]
    for key, members in requirements.items():
        section = packet.get(key)
        if key in missing or not isinstance(section, dict):
            continue
        if any(_empty(section.get(member)) for member in members):
            missing.append(key)
    packet["incomplete_fields"] = missing
    if not packet["raw"].get("raw_sha256"):
        packet["raw"]["identity_missing"] = True
    return packet


def _write_immutable_json(path, value):
    """Publish complete JSON atomically without replacing any directory entry."""
    encoded = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        # link() is an atomic create-if-absent operation. Unlike replace(), it
        # cannot overwrite a competing publisher or even a dangling symlink.
        os.link(tmp, path)
    finally:
        tmp.unlink()
    return path


def write_review_packet(path, packet):
    """Write one packet atomically; an existing packet is never overwritten."""
    if not isinstance(packet, dict) or packet.get("schema_version") != REVIEW_PACKET_SCHEMA:
        raise ValueError("invalid review packet schema")
    return _write_immutable_json(path, packet)


def _review_record_digest(record):
    return canonical_hash({k: v for k, v in record.items() if k != "record_sha256"})


def _review_execution_identity(row):
    return candidate_execution_identity(row)


def _review_import_errors(record, original_row=None):
    """Check integrity and binding; a digest does not authenticate the reviewer."""
    if not isinstance(record, dict):
        return ["review_record_not_an_object"]
    problems = _review_errors(record, record.get("candidate_id"))
    if record.get("schema_version") != REVIEW_RECORD_SCHEMA:
        problems.append("review_schema_unknown")
    if not isinstance(record.get("attribution"), str) or record["attribution"] not in REVIEW_ATTRIBUTIONS:
        problems.append("review_attribution_invalid")
    if not isinstance(record.get("reason"), str) or not record["reason"].strip():
        problems.append("review_reason_missing")
    try:
        recorded_at = datetime.fromisoformat(record["recorded_at"])
        if recorded_at.utcoffset() is None:
            raise ValueError("timezone required")
    except (KeyError, TypeError, ValueError):
        problems.append("review_recorded_at_invalid")
    if not isinstance(record.get("record_id"), str) or not record["record_id"].strip():
        problems.append("review_record_id_missing")
    try:
        if record.get("record_sha256") != _review_record_digest(record):
            problems.append("review_record_digest_mismatch")
    except (TypeError, ValueError):
        problems.append("review_record_not_json")
    identity = record.get("execution_identity")
    if not isinstance(identity, dict) or not identity:
        problems.append("review_execution_identity_missing")
    else:
        if record.get("candidate_id") != f"{identity.get('run_id')}:{identity.get('cell_id')}":
            problems.append("review_candidate_identity_mismatch")
        for key in ("run_id", "cell_id", "target_id", "rule_id", "rmw", "raw_path"):
            if not isinstance(identity.get(key), str) or not identity[key].strip():
                problems.append("review_execution_identity_missing:" + key)
        for key in ("effective_input_sha256", "raw_sha256", "execution_sha256"):
            value = identity.get(key)
            if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                problems.append("review_execution_identity_hash_invalid:" + key)
        if type(identity.get("target_revision")) is not int or identity["target_revision"] < 0:
            problems.append("review_execution_identity_revision_invalid")
        if not isinstance(identity.get("source"), (str, dict)) or not identity["source"]:
            problems.append("review_execution_identity_source_missing")
    if original_row is not None and identity != _review_execution_identity(original_row):
        problems.append("review_execution_identity_mismatch")
    refs = record.get("evidence_refs")
    hashes = record.get("evidence_sha256")
    if not isinstance(hashes, dict):
        problems.append("review_reference_hashes_missing")
    elif isinstance(refs, dict):
        if set(hashes) != set(refs):
            problems.append("review_reference_hashes_mismatch")
        for key in refs:
            try:
                actual = raw_sha256(refs[key])
                if hashes.get(key) != actual:
                    problems.append("review_reference_hash_mismatch:" + str(key))
            except (OSError, KeyError, TypeError, ValueError):
                problems.append("review_reference_unavailable:" + str(key))
        if isinstance(identity, dict) and (
                refs.get("raw") != identity.get("raw_path")
                or hashes.get("raw") != identity.get("raw_sha256")):
            problems.append("review_raw_identity_mismatch")
    if original_row is not None and original_row.get("replay_path") and (
            not isinstance(refs, dict) or refs.get("capsule") != original_row["replay_path"]):
        problems.append("review_capsule_identity_mismatch")
    try:
        resolved = validate_review_evidence(refs)
        validate_root_cause(record.get("root_cause"), resolved)
        validate_capability_assessment(record.get("capability_assessment"), resolved)
        validate_tool_repair_handoff(record.get("tool_repair_handoff"), record.get("attribution"), resolved)
    except (ValueError, TypeError, OSError) as exc:
        problems.append(str(exc))
    return problems


def _review_history_errors(record, previous):
    """A revision points backwards within one execution; forks stay explicit."""
    supersedes = record.get("supersedes_record_id")
    if supersedes is not None:
        reason = record.get("revision_reason")
        if not isinstance(reason, str) or not reason.strip():
            return ["review_revision_reason_missing"]
        parent = next((r for r in previous if r["record_id"] == supersedes), None)
        if parent is None or parent.get("execution_identity") != record.get("execution_identity"):
            return ["review_supersedes_identity_unknown"]
        if any(r.get("supersedes_record_id") == supersedes for r in previous):
            return ["review_supersedes_already_replaced"]
    elif record.get("revision_reason") is not None or any(
            r["candidate_id"] == record["candidate_id"] and r["reviewer_id"] == record["reviewer_id"]
            for r in previous):
        return ["review_revision_requires_supersedes"]
    cause = record.get("root_cause")
    if cause is not None:
        parent = next((r for r in previous if r["record_id"] == supersedes), {})
        old_root = (parent.get("root_cause") or {}).get("id")
        root, operation = cause["id"], cause["operation"]
        related = [next((r for r in previous if r["record_id"] == ref), None)
                   for ref in cause["related_record_ids"]]
        if any(r is None for r in related):
            return ["root_cause_related_record_unknown"]
        if operation in {"merge", "split", "revoke"} and not old_root:
            return ["root_cause_relationship_requires_predecessor"]
        if operation in {"merge", "split"}:
            if not root or root == old_root or not related:
                return ["root_cause_relationship_invalid"]
            related_roots = {(r.get("root_cause") or {}).get("id") for r in related}
            if operation == "merge" and related_roots != {root}:
                return ["root_cause_merge_destination_not_evidenced"]
            if operation == "split" and (related_roots != {old_root} or any(
                    (r.get("root_cause") or {}).get("id") == root for r in previous)):
                return ["root_cause_split_requires_new_destination"]
        if operation == "assign" and old_root and root != old_root:
            return ["root_cause_change_requires_merge_split_or_revoke"]
        if operation in {"assign", "revoke"} and related:
            return ["root_cause_unexpected_relationship"]
    return []


def append_review_record(path, record, *, original_row=None):
    """Append analysis against original_row without changing execution or confirmation.

    New records receive schema/time, reference hashes and a deterministic ID.
    Imported records retain their digest and must match the original saved row.
    Revisions require supersedes_record_id and revision_reason; old bytes stay.
    """
    path = Path(path)
    if not isinstance(record, dict):
        raise ValueError("review record must be an object")
    record = dict(record)
    if "record_sha256" in record and record["record_sha256"] != _review_record_digest(record):
        raise ValueError("review record digest mismatch")
    if not isinstance(original_row, dict):
        raise ValueError("review import requires original execution identity row")
    record.setdefault("schema_version", REVIEW_RECORD_SCHEMA)
    record.setdefault("execution_identity", _review_execution_identity(original_row))
    problems = _review_errors(record, record.get("candidate_id"))
    if problems:
        raise ValueError("invalid review record: " + ",".join(problems))
    if not isinstance(record.get("candidate_id"), str) or not record["candidate_id"]:
        raise ValueError("review record requires a candidate_id")
    if "evidence_sha256" not in record:
        try:
            record["evidence_sha256"] = {
                key: raw_sha256(record["evidence_refs"][key])
                for key in record["evidence_refs"]}
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise ValueError("review reference unavailable") from exc
    record.setdefault("record_id", "review-" + canonical_hash({
        k: v for k, v in record.items() if k not in {"record_sha256", "recorded_at", "record_id"}}))
    def check(value, previous):
        problems = _review_import_errors(value, original_row) + _review_history_errors(value, previous)
        if problems:
            raise ValueError("invalid review record: " + ",".join(problems))
    return _append_bound_record(path, record, decode=_decode_review_records, check=check)


def _append_bound_record(path, record, *, decode, check):
    """Shared append-only transport; validation is always performed under lock."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.seek(0)
        existing = decode(stream.read())
        for index, previous in enumerate(existing):
            if previous["record_id"] == record["record_id"]:
                record.setdefault("recorded_at", previous["recorded_at"])
                record.setdefault("record_sha256", _review_record_digest(record))
                check(record, existing[:index])
                if record != previous:
                    raise ValueError("record ID conflict")
                return snapshot_record(previous)
        record.setdefault("recorded_at", datetime.now(timezone.utc).isoformat())
        record.setdefault("record_sha256", _review_record_digest(record))
        check(record, existing)
        stream.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
        return snapshot_record(record)


def snapshot_record(record):
    return json.loads(json.dumps(record, allow_nan=False))


def _decode_human_records(text, *, original_rows, reviews, human_authority):
    if text and not text.endswith("\n"):
        raise ValueError("human log has an incomplete final line")
    records = [json.loads(line) for line in text.splitlines() if line.strip()]
    ledger = exploratory_candidate_ledger(original_rows, reviews=reviews, human_records=records,
                                          human_authority=human_authority)
    if ledger["human_errors"]:
        raise ValueError("invalid human record: " + ";".join(e["detail"] for e in ledger["human_errors"]))
    return ledger["human_records"]


def append_human_record(path, record, *, original_rows, reviews=None, human_authority=None):
    """Append a host-authenticated instruction; no strings/flags authenticate a human.

    All original executions and independent review records must be supplied for
    the log. Rechecking an existing ID also rechecks authority and eligibility.
    """
    if not isinstance(record, dict):
        raise ValueError("human record must be an object")
    rows, reviews = list(original_rows), list(reviews or [])
    def decode(text):
        return _decode_human_records(text, original_rows=rows, reviews=reviews,
                                     human_authority=human_authority)
    def check(value, previous):
        decode("".join(json.dumps(r, allow_nan=False) + "\n" for r in [*previous, value]))
        if value.get("action") == "confirm":
            ledger = exploratory_candidate_ledger(rows, reviews=reviews,
                human_records=[*previous, value], human_authority=human_authority)
            candidate = next(c for c in ledger["candidates"] if c["candidate_id"] == value["candidate_id"])
            if candidate.get("human_qualification_error"):
                raise ValueError(candidate["human_qualification_error"])
    # Unlike review analysis, human operations must carry an explicit ID and a
    # digest attested by the host verifier. Do not synthesize a human instruction.
    if not record.get("record_id") or not record.get("record_sha256"):
        raise ValueError("human instruction requires explicit record ID and digest")
    return _append_bound_record(path, snapshot_record(record), decode=decode, check=check)


def read_human_records(path, *, original_rows, reviews=None, human_authority=None):
    """Read with the same integrity, host authority and lifecycle gates as import."""
    with Path(path).open(encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        return _decode_human_records(stream.read(), original_rows=list(original_rows),
                                     reviews=reviews, human_authority=human_authority)


def read_review_records(path, *, original_rows=None):
    """Verify log digests/references; supply original_rows to recheck execution binding."""
    with Path(path).open(encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        return _decode_review_records(stream.read(), original_rows=original_rows)


def _decode_review_records(text, *, original_rows=None):
    if text and not text.endswith("\n"):
        raise ValueError("review log has an incomplete final line")
    records = []
    originals = {f"{row['run_id']}:{row['cell_id']}": row for row in original_rows or []}
    for line in text.splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        original_row = (originals.get(record.get("candidate_id"))
                        if isinstance(record, dict) and isinstance(record.get("candidate_id"), str)
                        else None)
        problems = _review_import_errors(record, original_row)
        if not problems:
            duplicate = next((r for r in records if r["record_id"] == record["record_id"]), None)
            if duplicate is not None:
                if record != duplicate:
                    raise ValueError("review record ID conflict")
                continue
            problems += _review_history_errors(record, records)
        if problems:
            raise ValueError("invalid review record: " + ",".join(problems))
        if original_rows is not None and original_row is None:
            raise ValueError("review execution identity mismatch")
        records.append(record)
    return records


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
    for review in item.get("reviews", []):
        review.pop("reviewed_at", None)
    if item.get("non_replayable"):
        item["non_replayable"].pop("timestamp", None)
    return item


def query_runner_evidence(output_dir):
    """Verify derived runner attachments without granting execution or candidates.

    The existing report/auditor remains the source of verdict and seal decisions.
    Missing exports are named tool problems; changed referenced bytes are errors.
    """
    from r2f.core.evidence import raw_sha256
    root = Path(output_dir).resolve()
    packets, identities, errors, problems = [], [], [], []

    def checked_path(ref):
        if not isinstance(ref, dict) or not isinstance(ref.get('path'), str):
            raise ValueError('runner_evidence_invalid_reference')
        path = (root / ref['path']).resolve()
        if not path.is_relative_to(root):
            raise ValueError('runner_evidence_reference_outside_root')
        if raw_sha256(path) != ref.get('sha256'):
            raise ValueError('runner_evidence_hash_mismatch:' + ref['path'])
        return path

    def read_ref(ref):
        data = json.loads(checked_path(ref).read_text())
        if not isinstance(data, dict):
            raise ValueError('runner_evidence_object_required')
        return data

    def bind_raw(raw, known):
        if known.get('raw_path') is None and known.get('raw_sha256') is None:
            return
        if (raw.get('raw_path') is None or known.get('raw_path') is None
                or (root / raw['raw_path']).resolve() != (root / known['raw_path']).resolve()
                or raw.get('raw_sha256') != known.get('raw_sha256')):
            raise ValueError('runner_evidence_raw_binding_mismatch')
        checked_path({'path': known['raw_path'], 'sha256': known.get('raw_sha256')})

    try:
        try:
            index = json.loads((root / 'evidence-index.json').read_text())
        except OSError as exc:
            problems.append({'kind': 'evidence_index_unavailable',
                             'error_type': type(exc).__name__, 'detail': str(exc)})
            raise
        if not isinstance(index, dict) or index.get('schema_version') != 'r2f.runner-evidence-index/v1':
            raise ValueError('runner_evidence_index_schema')
        manifest = read_ref(index.get('manifest'))
        source = index.get('source_manifest')
        if not isinstance(source, dict) or source != manifest.get('source_manifest'):
            raise ValueError('runner_evidence_source_mismatch')
        if index.get('run_id') != manifest.get('run_id'):
            raise ValueError('runner_evidence_run_mismatch')
        problems = index['tool_problems']
        if any(not isinstance(index.get(key), list)
               for key in ('packets', 'identities', 'reports', 'tool_problems')):
            raise ValueError('runner_evidence_index_shape')
        by_cell = {}
        for ref in index['identities']:
            saved = read_ref(ref)
            cell = saved.get('cell_id')
            if (not isinstance(cell, str) or cell in by_cell or cell != ref.get('cell_id')
                    or saved.get('run_id') != index['run_id']
                    or saved.get('source_manifest') != source):
                raise ValueError('runner_evidence_saved_identity_mismatch')
            by_cell[cell] = (saved, ref)
            identities.append(saved)
        for ref in index['packets']:
            packet = read_ref(ref)
            if packet.get('schema_version') != REVIEW_PACKET_SCHEMA:
                raise ValueError('runner_evidence_review_packet_schema')
            saved = read_ref(packet.get('intake_identity'))
            if (saved.get('source_manifest') != source
                    or saved.get('run_id') != index['run_id']
                    or saved.get('cell_id') != ref.get('cell_id')
                    or packet.get('packet_id') != f"{saved['run_id']}:{saved['cell_id']}"):
                raise ValueError('runner_evidence_identity_mismatch')
            expected, identity_ref = by_cell[saved['cell_id']]
            if (expected != saved or packet['intake_identity'] !=
                    {key: identity_ref[key] for key in ('path', 'sha256')}):
                raise ValueError('runner_evidence_identity_reference_mismatch')
            identity = packet.get('identity')
            if not isinstance(identity, dict):
                raise ValueError('runner_evidence_packet_identity_shape')
            if identity.get('source') != canonical_hash(source):
                raise ValueError('runner_evidence_source_mismatch')
            for key in ('target_id', 'target_revision'):
                if identity.get(key) != saved.get(key):
                    raise ValueError('runner_evidence_identity_mismatch:' + key)
            for key in ('target_sha256', 'rule_sha256'):
                if identity.get(key) != saved.get('identity', {}).get(key):
                    raise ValueError('runner_evidence_definition_mismatch:' + key)
            if (packet.get('input', {}).get('effective_input_sha256') != saved.get('effective_input_sha256')
                    or identity.get('rule') != saved.get('rule_id')):
                raise ValueError('runner_evidence_input_or_rule_mismatch')
            diagnostic = packet.get('runner_diagnostic')
            if not isinstance(diagnostic, dict) or not isinstance(diagnostic.get('artifacts'), list):
                raise ValueError('runner_evidence_diagnostic_shape')
            result_path = saved.get('planned_row', {}).get('runtime_result_path')
            runtime_dir = (Path(result_path).resolve().parent
                           if isinstance(result_path, str) else None)
            runtime = None
            for artifact in diagnostic['artifacts']:
                artifact_path = checked_path(artifact)
                if runtime_dir is None or not artifact_path.is_relative_to(runtime_dir):
                    raise ValueError('runner_evidence_diagnostic_cell_mismatch')
            # The sealed plan names the runtime result independently of the
            # packet's artifact list, including when report export failed.
            if runtime_dir is not None and Path(result_path).is_file():
                try:
                    runtime = json.loads(Path(result_path).read_text())
                    if not isinstance(runtime, dict):
                        raise ValueError('runner_evidence_runtime_shape')
                except (OSError, ValueError) as exc:
                    # A worker can exit while publishing its result. Retain
                    # its hash-checked diagnostic, without qualifying a run.
                    runtime = None
                    errors.append('runner_evidence_runtime_unreadable:' + saved['cell_id'])
                    problems.append({'kind': 'unreadable_runtime_result',
                                     'cell_id': saved['cell_id'], 'path': result_path,
                                     'error_type': type(exc).__name__, 'detail': str(exc)})
            if runtime is not None:
                for embedded in (runtime, runtime.get('execution_metadata'), runtime.get('execution')):
                    if not isinstance(embedded, dict):
                        continue
                    for key in ('run_id', 'cell_id', 'target_id', 'target_revision',
                                'rmw', 'arm', 'repeat', 'effective_input_sha256', 'rule_id'):
                        if key in embedded and embedded[key] != saved.get(key):
                            raise ValueError('runner_evidence_runtime_identity_mismatch:' + key)
            raw = packet.get('raw')
            if not isinstance(raw, dict):
                raise ValueError('runner_evidence_raw_shape')
            if raw.get('raw_path') is not None:
                raw_path = checked_path({'path': raw['raw_path'], 'sha256': raw.get('raw_sha256')})
                result_path = saved.get('planned_row', {}).get('runtime_result_path')
                if (not isinstance(result_path, str)
                        or not raw_path.is_relative_to(Path(result_path).resolve().parent)):
                    raise ValueError('runner_evidence_raw_identity_mismatch')
                if runtime is not None:
                    bind_raw(raw, runtime)
            packets.append(packet)
        packets_by_cell = {p['packet_id']: p for p in packets}
        for ref in index['reports']:
            report = read_ref(ref)
            if report.get('run_id') != index['run_id']:
                raise ValueError('runner_evidence_report_identity_mismatch')
            if 'source_manifest' in report and (not isinstance(report['source_manifest'], dict)
                                                or report['source_manifest'] != source):
                raise ValueError('runner_evidence_report_source_mismatch')
            rows = report.get('rows')
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError('runner_evidence_report_rows_shape')
            if {row.get('cell_id') for row in rows} != set(by_cell) or len(rows) != len(by_cell):
                raise ValueError('runner_evidence_report_row_set_mismatch')
            for row in rows:
                saved = by_cell[row['cell_id']][0]
                packet = packets_by_cell.get(f"{saved['run_id']}:{saved['cell_id']}")
                if packet is not None:
                    bind_raw(packet['raw'], row)
                for key in ('run_id', 'cell_id', 'target_id', 'target_revision', 'rmw', 'arm',
                            'repeat', 'effective_input_sha256'):
                    if row.get(key) != saved.get(key):
                        raise ValueError('runner_evidence_report_row_identity_mismatch:' + key)
                if row.get('rule_id') is not None and row['rule_id'] != saved.get('rule_id'):
                    raise ValueError('runner_evidence_report_row_identity_mismatch:rule_id')
        replay_summary = query_automatic_replays(root, index=index)
        errors.extend(replay_summary.get('errors', []))
        return {'evidence_valid': not errors, 'errors': errors, 'run_id': index['run_id'],
                'packets': packets, 'identities': identities,
                'tool_problems': problems, 'reports': index['reports'],
                'automatic_replay': replay_summary}
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        errors.append('runner_evidence_invalid:' + str(exc))
        return {'evidence_valid': False, 'errors': errors, 'packets': packets,
                'tool_problems': problems, 'identities': identities,
                'automatic_replay': query_automatic_replays(root)}


def validate_runner_replay_capsule(path, saved, original_row):
    """Bind a validated v1/v2 capsule to sealed intake and original execution.

    This is artifact identity only. An input-only capsule cannot establish an
    original execution, source qualification, or a SUT candidate.
    """
    from r2f.core.replay import ReplayCapsule

    try:
        capsule = ReplayCapsule.from_dict(json.loads(Path(path).read_text()))
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise ValueError('original_capsule_invalid:' + str(exc)) from exc

    def same(actual, expected, field):
        if canonical_hash(actual) != canonical_hash(expected):
            raise ValueError('original_capsule_identity_mismatch:' + field)

    data, planned = capsule.input, saved['planned_row']
    for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'effective_input_sha256'):
        same(data.get(key), saved[key], 'input.' + key)
    same(data['source_manifest'], saved['source_manifest'], 'input.source_manifest')
    for key, digest_key in (('source_manifest', 'source_sha256'), ('target', 'target_sha256')):
        same(canonical_hash(data[key]), saved['identity'][digest_key],
             'input.' + key + '_sha256')
    rule = data['target']['rule_contract']
    same(rule.get('primary_rule'), saved['rule_id'], 'input.rule_id')
    same(canonical_hash(rule), saved['identity']['rule_sha256'], 'input.rule_sha256')
    for key in ('input_identity_schema', 'target', 'seed', 'mutation_kind', 'carrier',
                'state_steps', 'mutation_parameters', 'mutation_plan'):
        if key in planned:
            same(data.get(key), planned[key], 'input.' + key)
    for key in ('run_id', 'cell_id', 'target_id', 'target_revision', 'rmw', 'arm',
                'repeat', 'effective_input_sha256'):
        same(original_row.get(key), saved[key], 'original.' + key)

    execution = capsule.execution
    if execution is None:
        if original_row.get('actual_run') is True:
            raise ValueError('original_capsule_execution_unavailable')
        return {'input_bound': True, 'execution_bound': False,
                'reason_code': 'original_capsule_execution_unavailable'}
    for key in ('run_id', 'cell_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat',
                'effective_input_sha256', 'source_manifest', 'rule_id', 'target',
                'domain_id', 'raw_path', 'raw_sha256', 'execution_stage', 'actual_run',
                'execution_source', 'runtime_result_path', 'runtime_config_path', 'replay_path',
                'oracle_verdict', 'oracle'):
        same(execution.get(key), original_row.get(key), 'execution.' + key)
    return {'input_bound': True, 'execution_bound': True, 'reason_code': 'original_execution_bound'}


def query_automatic_replays(root, *, index=None):
    """Audit derived replay associations independently of original SUT verdicts."""
    from r2f.core.evidence import raw_sha256
    from r2f.campaign.replay import audit_replay

    summary = {'status': 'not_run', 'reason_code': 'automatic_replay_not_requested', 'entries': []}
    errors = []

    def check(ref, *, expected=None, external=False):
        path = (root / ref['path']).resolve()
        if ((not external and not path.is_relative_to(root))
                or expected is not None and path != Path(expected).resolve()
                or raw_sha256(path) != ref['sha256']):
            raise ValueError('reference_path_or_hash_mismatch:' + str(ref['path']))
        return path

    try:
        path = root / 'replay-summary.json'
        if not path.exists():
            failure_path = root / 'replay-failure.json'
            requests = sorted((root / 'replay-scheduling').glob('*/request.json'))
            if not failure_path.exists() and not requests:
                return summary
            summary['reason_code'] = 'automatic_replay_attachment_failure'
            summary['attachment_valid'] = False
            if failure_path.is_file():
                summary['failure'] = json.loads(failure_path.read_text())
            for request_path in requests:
                request = json.loads(request_path.read_text())
                summary['entries'].append({
                    'cell_id': request.get('cell_id'), 'dispatch_id': request.get('dispatch_id'),
                    'requested_output_dir': request.get('output_dir'),
                    'request': {'path': str(request_path.relative_to(root)),
                                'sha256': raw_sha256(request_path)},
                    'status': 'unknown', 'reason_code': 'automatic_replay_attachment_failure',
                    'candidate_eligible': False, 'original_confirmed': False,
                    'internal_state_recovery': False, 'real_ros_execution': False})
            errors.append('runner_replay_invalid:automatic_replay_attachment_failure')
            return dict(summary, errors=errors)
        summary = json.loads(path.read_text())
        if index is None:
            index = json.loads((root / 'evidence-index.json').read_text())
        if (summary.get('schema_version') != 'r2f.runner-auto-replay/v1'
                or summary.get('run_id') != index['run_id']
                or summary.get('summary_sha256') != canonical_hash(
                    {key: value for key, value in summary.items() if key != 'summary_sha256'})):
            raise ValueError('summary_identity_or_hash_mismatch')
        check(summary['original_index'], expected=root / 'evidence-index.json')
        for ref in summary['original_artifacts']:
            check(ref)
        if summary['original_artifacts_unchanged'] is not True:
            raise ValueError('original_artifact_changed')
        identities = {ref['cell_id']: ref for ref in index['identities']}
        entries = summary['entries']
        if ({entry['cell_id'] for entry in entries} != set(identities)
                or len(entries) != len(identities)):
            raise ValueError('cell_set_mismatch')
        ordinal, domains = 0, []
        policy = summary.get('policy')
        for entry in entries:
            if (entry['candidate_eligible'] is not False or entry['original_confirmed'] is not False
                    or entry['internal_state_recovery'] is not False):
                raise ValueError('derived_replay_cannot_confirm_original')
            if 'request' not in entry:
                if entry['status'] != 'not_run' or not entry['reason_code']:
                    raise ValueError('limitation_not_named')
                if entry['reason_code'] == 'replay_request_export_failed':
                    ordinal += 1
                continue
            ordinal += 1
            if (not isinstance(policy, dict) or type(policy.get('max_attempts')) is not int
                    or ordinal > policy['max_attempts']):
                raise ValueError('undeclared_or_exceeded_replay_budget')
            dispatch = root / 'replay-scheduling' / entry['dispatch_id']
            request = json.loads(check(entry['request'], expected=dispatch / 'request.json').read_text())
            outcome = json.loads(check(entry['outcome'], expected=dispatch / 'outcome.json').read_text())
            if outcome != {key: value for key, value in entry.items() if key != 'outcome'}:
                raise ValueError('outcome_mismatch')
            identity = identities[entry['cell_id']]
            saved = json.loads(check(identity).read_text())
            if (request['dispatch_id'] != entry['dispatch_id']
                    or request['run_id'] != index['run_id'] or request['cell_id'] != entry['cell_id']
                    or request['intake_identity'] != {key: value for key, value in identity.items() if key != 'cell_id'}
                    or request['original_index'] != summary['original_index']):
                raise ValueError('request_identity_mismatch')
            if (request['attempt_ordinal'] != ordinal or request['max_attempts'] != policy['max_attempts']
                    or request['domain_id'] != policy['domain_ids'][ordinal - 1]
                    or request['domain_id'] in domains
                    or Path(request['output_dir']).resolve()
                    != Path(policy['output_dir']).resolve() / entry['dispatch_id']):
                raise ValueError('request_policy_mismatch')
            domains.append(request['domain_id'])
            capsule = check(request['capsule'], expected=saved['planned_row']['replay_path'])
            original_ref, = [ref for ref in index['reports'] if ref['path'] == 'report.json']
            original = json.loads(check(original_ref, expected=root / 'report.json').read_text())
            original_row, = [row for row in original['rows'] if row['cell_id'] == entry['cell_id']]
            binding = validate_runner_replay_capsule(capsule, saved, original_row)
            if 'capsule_binding' in entry and entry['capsule_binding'] != binding:
                raise ValueError('capsule_binding_mismatch')
            # Historical v1 attachments have no derived binding field. Recheck
            # their content too; expose limitations without rewriting evidence.
            entry['capsule_binding'] = binding
            if 'replay_report' not in entry:
                if entry['reason_code'] != 'replay_dispatch_failed' or not entry.get('error_type'):
                    raise ValueError('dispatch_failure_not_named')
                continue
            output = Path(request['output_dir']).resolve()
            if output.is_relative_to(root) or root.is_relative_to(output):
                raise ValueError('output_not_independent')
            report_path = check(entry['replay_report'], expected=output / 'replay_report.json', external=True)
            check(entry['replay_attempt'], expected=output / 'attempt.json', external=True)
            report = json.loads(report_path.read_text())
            audited = audit_replay(report_path)
            if not audited['valid']:
                raise ValueError('audit_failed:' + str(audited['errors']))
            if (Path(report['capsule_path']).resolve() != capsule
                    or report['original'].get('capsule_sha256') != request['capsule']['sha256']
                    or report['domain_id'] != request['domain_id']
                    or report['runtime_boundary'] != request['runtime_boundary']
                    or report['formal_runtime'] is request['injected_runtime']
                    or report['status'] != entry['status']
                    or bool(report['real_ros_execution']) != entry['real_ros_execution']
                    or bool(report['real_runtime_attempt']) != entry['real_runtime_attempt']
                    or report['qualification_errors'] != entry['qualification_errors']
                    or report['runtime_invoked'] != entry['runtime_invoked']):
                raise ValueError('report_request_or_outcome_mismatch')
    except (OSError, ValueError, KeyError, IndexError, TypeError, AttributeError) as exc:
        errors.append('runner_replay_invalid:' + str(exc))
    if not isinstance(summary, dict):
        summary = {'entries': []}
    return dict(summary, attachment_valid=not errors, errors=errors)
