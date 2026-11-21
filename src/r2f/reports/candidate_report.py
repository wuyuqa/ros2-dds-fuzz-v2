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
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from r2f.core.candidate import CandidateLifecycle, topology_observation_identity
from r2f.core.evidence import canonical_hash
from r2f.core.oracle import TriageResult

REVIEW_PACKET_SCHEMA = "r2f.s1-review-packet/v1"
REVIEW_RECORD_SCHEMA = "r2f.s1-review-record/v1"
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
    if review.get("conclusion") not in REVIEW_CONCLUSIONS:
        problems.append("review_conclusion_invalid")
    refs = review.get("evidence_refs")
    if not isinstance(refs, dict) or not refs:
        problems.append("review_without_evidence_references")
    else:
        for key in ("source", "raw", "capsule"):
            if not isinstance(refs.get(key), str) or not refs[key].strip():
                problems.append("review_missing_reference:" + key)
    if review.get("reviewer_id") in set(review.get("independent_of") or []):
        problems.append("reviewer_not_independent")
    return problems


def exploratory_candidate_ledger(rows, reviews=None):
    """Ledger for exploratory runs.

    Natural candidates are only rows that were actually executed, judged
    ``fail``, are not controls, are not tool errors and have a complete
    observation.  Every admitted anomaly keeps its raw identity; nothing is
    dropped because admission or confirmation fails.
    """
    rows = list(rows)
    reviews = list(reviews or [])
    candidates, failures, controls, review_errors = [], [], [], []
    by_candidate = {}
    for review in reviews:
        if isinstance(review, dict) and review.get("candidate_id"):
            by_candidate.setdefault(review["candidate_id"], []).append(review)
        else:
            review_errors.append({"candidate_id": None, "reason_code": "review_without_candidate",
                                  "detail": "review record has no candidate_id"})
    for row in rows:
        cell_id = row.get("cell_id")
        if row.get("arm") == "negative" or row.get("control_kind"):
            controls.append({
                "cell_id": cell_id, "oracle_verdict": row.get("oracle_verdict"),
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
                "cell_id", "execution_stage", "oracle_verdict", "abort_reason", "reason_code",
                "raw_path", "raw_sha256", "tool_error_detail")} | {"reason_code": "tool_error"})
            continue
        if row.get("observer_dropped") is True or row.get("observer_complete") is False:
            failures.append({key: row.get(key) for key in (
                "cell_id", "execution_stage", "oracle_verdict", "abort_reason", "raw_path", "raw_sha256")}
                | {"reason_code": "observer_incomplete"})
            continue
        if row.get("execution_stage") == "aborted" or row.get("oracle_verdict") in (
                "unknown", "not_applicable") or row.get("execution_stage") != "judged":
            failures.append({key: row.get(key) for key in (
                "cell_id", "execution_stage", "oracle_verdict", "abort_reason", "reason_code",
                "raw_path", "raw_sha256")})
            continue
        if row.get("oracle_verdict") != "fail":
            continue
        evidence = _exploratory_evidence(row)
        try:
            lifecycle = CandidateLifecycle(f"{row['run_id']}:{cell_id}", evidence)
        except (ValueError, KeyError) as exc:
            failures.append({"cell_id": cell_id, "reason_code": "anomaly_without_minimum_observation_identity",
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
        for review in by_candidate.get(candidate_id, []):
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
                                        notes=review.get("notes", ""))
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
        review_status = "not_reviewed"
        if applied_reviews:
            last = applied_reviews[-1]
            review_status = {"supported": "supported_awaiting_adjudication",
                             "rejected": "review_rejected_by_independent_agent",
                             "needs_evidence": "needs_evidence"}[last["conclusion"]]
        if lifecycle.candidate_status == "confirmed":
            review_status = "confirmed_after_review"
        entry = dict(lifecycle.to_dict(), cell_id=cell_id, observation_kind="primary",
                     admission_error=admission_error, confirmation_error=confirmation_error,
                     review_status=review_status,
                     review_errors=[e["detail"] for e in review_errors if e["candidate_id"] == candidate_id],
                     **{key: row.get(key) for key in EXPLORATORY_INPUT_KEYS if key in row})
        candidates.append(entry)
    natural = [c for c in candidates if c.get("responsibility_layer") == "sut"
               and c.get("triage_disposition") == "candidate"]
    return {
        "schema_version": 1,
        "campaign_kind": "exploratory",
        "candidates": candidates,
        "failures": failures,
        "controls": controls,
        "review_errors": review_errors,
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


def append_review_record(path, record):
    """Append one immutable review decision to a review log.

    The log is a JSONL file; existing lines are never rewritten.  A record must
    name the reviewer, the candidate, a conclusion and evidence references.
    """
    path = Path(path)
    record = dict(record or {})
    record.setdefault("schema_version", REVIEW_RECORD_SCHEMA)
    record.setdefault("recorded_at", datetime.now(timezone.utc).isoformat())
    problems = _review_errors(record, record.get("candidate_id"))
    if problems:
        raise ValueError("invalid review record: " + ",".join(problems))
    if not isinstance(record.get("candidate_id"), str) or not record["candidate_id"]:
        raise ValueError("review record requires a candidate_id")
    record["record_sha256"] = _review_record_digest(record)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
    return record


def read_review_records(path):
    """Read an append-only review log, verifying every recorded digest."""
    records = []
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("record_sha256") != _review_record_digest(record):
            raise ValueError("review record digest mismatch")
        problems = _review_errors(record, record.get("candidate_id"))
        if problems:
            raise ValueError("invalid review record: " + ",".join(problems))
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
