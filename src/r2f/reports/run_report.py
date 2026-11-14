"""Independent, hash-checked offline acceptance of persisted campaign evidence."""
from collections import Counter
import json
from pathlib import Path

from r2f.core.evidence import canonical_hash, effective_input_hash, verify_raw
from r2f.core.execution import validate_execution
from r2f.core.oracle import validate_control
from r2f.core.replay import ReplayCapsule, validate_replay_attempt
from r2f.core.scenario import Scenario
from r2f.campaign.identity import reject_execution_identity, source_manifest
from r2f.campaign.runtime_adapter import ingest_execution, read_events
from r2f.campaign.scheduler import first_slice_cells, control_references
from r2f.campaign.triage import unknown_gate
from r2f.modules.s1.targets import RMWS, EXECUTION_ORDER


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")


def report_digest(report):
    return canonical_hash({k: v for k, v in report.items() if k not in {"report_sha256", "seal"}})


def _path(root, value):
    if not isinstance(value, str) or not value:
        raise ValueError("evidence path missing")
    path = Path(value)
    path = (path if path.is_absolute() else root / path).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("evidence path escapes campaign directory")
    if not path.is_file():
        raise ValueError("evidence file missing: " + str(path))
    return path


def identity_hashes(manifest):
    return {"manifest_sha256": canonical_hash(manifest),
            "target_sha256": canonical_hash(manifest["target"]),
            "catalog_sha256": canonical_hash(manifest["catalog"]),
            "rule_sha256": canonical_hash(manifest["rule"]),
            "source_sha256": canonical_hash(manifest["source_manifest"]),
            "scope_sha256": canonical_hash(manifest["scope"])}


def audit_report(report_path):
    """Recompute evidence and controls. A status string is never seal evidence."""
    report_path = Path(report_path).resolve()
    root = report_path.parent
    errors = []
    details = {"controls": {}, "unknown_gate": {}, "required_expected": 18, "fault_expected": 6}
    try:
        report = json.loads(report_path.read_text())
        manifest = json.loads(_path(root, report["manifest_path"]).read_text())
        identity = identity_hashes(manifest)
        if report.get("report_sha256") != report_digest(report):
            errors.append("report_hash_mismatch")
        if report.get("identity") != identity:
            errors.append("report_manifest_identity_mismatch")
        target_id = manifest["target"]["target_id"]
        revision = manifest["target"]["target_revision"]
        if manifest["rule"] != manifest["target"]["rule_contract"]:
            errors.append("rule_target_contract_mismatch")
        if manifest["catalog"]["targets"].get(target_id) != manifest["target"]:
            errors.append("catalog_target_mismatch")
        if manifest["scope"]["target_revision"].get(target_id) != revision:
            errors.append("scope_revision_mismatch")
        if manifest["scope"].get("full_scope_required_cells") != 72 or report.get("full_scope_required_cells") != 72:
            errors.append("full_scope_denominator_changed")
        source = manifest["source_manifest"]
        reject_execution_identity(source)
        if not source.get("head") or not source.get("behavior_tree_sha256"):
            errors.append("unfrozen_source_identity")
        if source_manifest(Path(source["source_repo"])) != source:
            errors.append("stale_source_identity")
        if source.get("source_frozen") is not True or source.get("dirty_behavior_paths") != []:
            errors.append("source_behavior_not_tracked_and_clean_at_HEAD")
        # Validate against current target contract too; stale revisions cannot seal.
        from r2f.modules.s1.module import S1Module
        current = S1Module().targets[target_id].to_dict()
        if current != manifest["target"]:
            errors.append("stale_target_manifest_or_revision")
        rows = report["rows"]
        if not isinstance(rows, list):
            raise ValueError("rows must be a list")
        expected = first_slice_cells(target_id)
        expected_by_id = {r["cell_id"]: r for r in expected}
        if Counter(r.get("cell_id") for r in rows) != Counter(expected_by_id.keys()):
            errors.append("matrix_missing_duplicate_or_extra_cell")
        required = [r for r in rows if r.get("campaign_kind") == "required" and not r.get("control_kind") and r.get("arm") != "negative"]
        if len(required) != 18:
            errors.append("required_matrix_not_18")
        reference = control_references(expected)
        if report.get("control_references") != reference:
            errors.append("control_reference_set_mismatch")
        unique_raw = set()
        valid_ids = set()
        for row in rows:
            cell_id = row.get("cell_id", "<missing>")
            try:
                expected_row = expected_by_id[cell_id]
                for key in ("rmw", "arm", "repeat", "campaign_kind", "control_kind"):
                    if row.get(key) != expected_row.get(key):
                        raise ValueError("matrix tuple mismatch: " + key)
                if type(row.get("repeat")) is not int or type(row.get("target_revision")) is not int:
                    raise ValueError("integer identity required")
                if row.get("identity") != identity or row.get("target_id") != target_id or row.get("target_revision") != revision or row.get("run_id") != report["run_id"]:
                    raise ValueError("cell_identity_mismatch")
                if row.get("execution_stage") != "judged" or row.get("actual_run") is not True:
                    raise ValueError("cell_not_actually_judged")
                runtime = json.loads(_path(root, row["runtime_result_path"]).read_text())
                config = json.loads(_path(root, row["runtime_config_path"]).read_text())
                if runtime.get("real_execution") is not True or runtime.get("execution_metadata") is None:
                    raise ValueError("real_runtime_execution_metadata_missing")
                if runtime.get("evidence_origin") in {"fixture", "synthetic", "offline"}:
                    raise ValueError("offline_fixture_cannot_seal")
                raw_path = _path(root, row["raw_path"])
                if raw_path in unique_raw:
                    raise ValueError("raw_artifact_reused_across_cells")
                unique_raw.add(raw_path)
                verify_raw(raw_path, row["raw_sha256"])
                if runtime.get("raw_path") != str(raw_path) or runtime.get("raw_sha256") != row["raw_sha256"]:
                    raise ValueError("runtime_raw_binding_mismatch")
                for key in ("run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "domain_id"):
                    if config.get(key) != row.get(key):
                        raise ValueError("config_cell_identity_mismatch:" + key)
                metadata = runtime["execution_metadata"]
                for key in ("run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "domain_id"):
                    if metadata.get(key) != config.get(key):
                        raise ValueError("execution_metadata_identity_mismatch:" + key)
                environment = runtime.get("environment", {})
                if environment.get("ok") is not True or environment.get("environment_mode") != "ros_overlay" or environment.get("ros_distro") != "jazzy":
                    raise ValueError("environment_manifest_invalid")
                if environment.get("source", {}).get("head") != source["head"] or environment.get("source", {}).get("source_repo") != source["source_repo"]:
                    raise ValueError("environment_source_identity_mismatch")
                if environment.get("rmw", {}).get(row["rmw"], {}).get("available") is not True:
                    raise ValueError("rmw_not_available_in_manifest")
                if not environment.get("dependency_lock_sha256") or not environment.get("process_scan", {}).get("ok") or not environment.get("old_lock_scan", {}).get("ok"):
                    raise ValueError("preflight_receipts_missing")
                capsule = ReplayCapsule.from_dict(json.loads(_path(root, row["replay_path"]).read_text()))
                effective = capsule.input["effective_input"]
                reject_execution_identity(effective["final_input"])
                reject_execution_identity(effective["source_manifest"])
                if effective_input_hash(effective) != row.get("effective_input_sha256"):
                    raise ValueError("effective_input_hash_mismatch")
                if effective["source_manifest"] != source or capsule.input["target"] != manifest["target"]:
                    raise ValueError("capsule_source_or_target_identity_mismatch")
                submitted = effective["final_input"]
                for key, value in submitted.items():
                    if config.get(key) != value:
                        raise ValueError("submitted_input_runtime_config_mismatch:" + key)
                if target_id in EXECUTION_ORDER[:2]:
                    actual_delay = config["reader_delay_sec"] if row["arm"] == "mutated" else config["baseline_reader_delay_sec"]
                    if actual_delay != submitted["reader_delay_sec"]:
                        raise ValueError("submitted_delay_not_executed")
                if canonical_hash(submitted) != row.get("actual_submitted_payload_sha256"):
                    raise ValueError("submitted_payload_hash_mismatch")
                reconstructed = ingest_execution(Scenario.from_dict(capsule.input["scenario"]), config, runtime, capsule)
                for key in ("execution_stage", "precondition_receipts", "mutation_receipt", "typed_facts", "oracle", "oracle_verdict", "raw_sha256", "rule_id", "fault_control_receipt", "instrumentation_observation"):
                    if reconstructed.get(key) != row.get(key):
                        raise ValueError("raw_recomputed_cell_mismatch:" + key)
                validate_execution(row)
                if capsule.execution is None:
                    raise ValueError("capsule_execution_missing")
                for key in ("run_id", "target_id", "target_revision", "effective_input_sha256", "precondition_receipts", "mutation_receipt", "typed_facts", "raw_path", "raw_sha256", "oracle_verdict", "fault_control_receipt"):
                    if capsule.execution.get(key) != row.get(key):
                        raise ValueError("capsule_execution_mismatch:" + key)
                valid_ids.add(cell_id)
            except (KeyError, ValueError, TypeError, OSError) as exc:
                errors.append(f"{cell_id}: {exc}")
        details["unknown_gate"] = unknown_gate(required)
        if not details["unknown_gate"]["passed"]:
            errors.append("required_unknown_gate_failed")
        by_id = {r.get("cell_id"): r for r in rows}
        for kind, ids in reference.items():
            good = 0
            for cell_id in ids:
                row = by_id.get(cell_id, {})
                record = dict(row, control_kind=kind)
                if kind == "transparency":
                    baseline = by_id.get(cell_id.replace(".transparent.", ".baseline."), {})
                    equal = bool(row.get("actual_submitted_payload_sha256")) and row.get("actual_submitted_payload_sha256") == baseline.get("actual_submitted_payload_sha256")
                    observation = lambda r: {k: r.get("oracle", {}).get(k) for k in ("verdict", "forbidden_residuals", "closure")}
                    equal = equal and observation(row) == observation(baseline)
                    record["transparency_expected"] = "no_effect_or_expected_pass" if equal else "mismatch"
                try:
                    passed = cell_id in valid_ids and validate_control(record)
                except ValueError:
                    passed = False
                if passed:
                    good += 1
            details["controls"][kind] = {"passed": good == 6, "satisfied": good, "expected": 6}
            if good != 6:
                errors.append(f"{kind}_control_failed:{good}/6")
        ledger = json.loads(_path(root, report["candidate_ledger_path"]).read_text())
        if canonical_hash(ledger) != report.get("candidate_ledger_sha256"):
            errors.append("candidate_ledger_hash_mismatch")
        actual_anomalies = {r["cell_id"] for r in rows if r.get("oracle_verdict") == "fail" and r.get("arm") != "negative" and not r.get("control_kind")}
        if {c.get("cell_id") for c in ledger["candidates"] if c.get("observation_kind", "primary") == "primary"} != actual_anomalies:
            errors.append("candidate_ledger_dropped_or_invented_anomaly")
        if not ledger["candidates"] and not ledger.get("zero_candidate_explanation"):
            errors.append("zero_candidate_explanation_missing")
        from r2f.reports.candidate_report import candidate_ledger, audit_projection
        reconstructed_ledger = candidate_ledger(rows)
        expected_candidates = {c["candidate_id"]: c for c in reconstructed_ledger["candidates"]}
        if Counter(c.get("candidate_id") for c in ledger["candidates"]) != Counter(expected_candidates.keys()):
            errors.append("candidate_ledger_dropped_or_invented_instrumentation")
        for key in ("instrumentation_status", "instrumentation_candidates", "instrumentation_intake_count", "instrumentation_qualification", "instrumentation_linkage", "instrumentation_observations"):
            if ledger.get(key) != reconstructed_ledger.get(key):
                errors.append("instrumentation_ledger_not_bound_to_qualified_observation:" + key)
        if ledger.get("confirmed_candidates") != sum(c.get("candidate_status") == "confirmed" for c in ledger["candidates"]):
            errors.append("confirmed_count_not_derived_from_ledger")
        if ledger.get("instrumentation_status") == "not_run" and ledger.get("instrumentation_candidates") is not None:
            errors.append("unexecuted_instrumentation_count_must_be_null")
        for candidate in ledger["candidates"]:
            expected_candidate = expected_candidates.get(candidate.get("candidate_id"))
            if expected_candidate is None or audit_projection(candidate) != audit_projection(expected_candidate):
                errors.append("candidate_evidence_or_lifecycle_not_bound_to_verified_cell")
            if candidate.get("evidence", {}).get("arm") == "negative" or candidate.get("candidate_source") == "control":
                errors.append("control_entered_candidate_ledger")
            if not all(candidate.get(key) for key in ("triage_disposition", "responsibility_layer", "reason_code")):
                errors.append("candidate_attribution_missing")
            if not candidate.get("replay_attempts") and not all((candidate.get("non_replayable") or {}).get(k) for k in ("disposition", "reason")):
                errors.append("candidate_replay_disposition_missing")
            for attempt in candidate.get("replay_attempts", []):
                validate_replay_attempt(attempt)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        errors.append("report_artifact_error:" + str(exc))
    return {"sealed": not errors, "errors": errors, **details, "maturity_scope": "per-target only; full S1 denominator remains 72"}
