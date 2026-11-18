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


EXPLORATORY_SCHEMA = "r2f.s1-exploratory-report/v1"
EXPLORATORY_SEAL = "not_applicable_exploratory"
SUPPORTED_CAMPAIGN_KINDS = frozenset({"required", "exploratory"})


BOUNDARY_AUDIT_PENDING = "isolated_boundary_qualification_pending"


def execution_boundary_qualification(runtime_boundary=None, *, config=None, result=None,
                                     row=None, formal_runtime=False, output_root=None):
    """Combine caller selection and complete execution evidence after runtime.

    The boundary helper only establishes artifact consistency. It does not
    replace canonical selection, source freeze, nonfixture raw/env/worker or
    successful outer-runtime gates. Planned rows remain explicitly pending.
    """
    for label, value in (("row", row), ("config", config), ("result", result)):
        if value is not None and not isinstance(value, dict):
            raise ValueError("boundary_" + label + "_not_object")
    result_present = result is not None
    row, config, result = row or {}, config or {}, result or {}
    options = config.get("execution_boundary")
    isolated = (runtime_boundary == "isolated_netns" or row.get("runtime_boundary") == "isolated_netns"
                or isinstance(options, dict) and options.get("enabled") is True
                or "execution_boundary" in result or isinstance(row.get("execution_boundary"), dict)
                or any(isinstance(item, dict) and item.get("pid_interpretation") == "namespace_PID_not_host_PID"
                       for item in (result.get("execution_metadata"), result.get("execution"),
                                    row.get("runtime_execution_metadata"))))
    if not isolated:
        if runtime_boundary not in {None, "host"}:
            return {"status": "invalid", "reason_code": "unsupported_runtime_boundary",
                    "errors": ["unsupported_runtime_boundary"], "eligible": False}
        return {"status": "not_applicable", "reason_code": "host_runtime", "errors": []}
    if not result_present:
        return {"status": "unknown", "reason_code": BOUNDARY_AUDIT_PENDING,
                "errors": [], "eligible": False}
    from r2f.campaign.replay import _qualification, ISOLATED_RUNTIME
    origin = ISOLATED_RUNTIME if runtime_boundary == "isolated_netns" and formal_runtime is True else "injected_runtime"
    try:
        qualified = _qualification(origin, config, result, row, output_root=output_root)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        qualified = {"real_ros_execution": False, "real_runtime_attempt": False,
                     "qualification_errors": ["invalid_boundary_runtime_evidence:" + type(exc).__name__ + ":" + str(exc)]}
    errors = qualified["qualification_errors"]
    source = config.get("source_manifest") or {}
    if not isinstance(source, dict):
        errors.append("boundary_source_manifest_not_object")
    else:
        try:
            if not source.get("source_repo") or canonical_hash(source_manifest(Path(source["source_repo"]))) != canonical_hash(source):
                errors.append("boundary_current_source_mismatch")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append("boundary_current_source_unavailable:" + type(exc).__name__)
    return {"status": "qualified" if not errors else "invalid",
            "reason_code": "boundary_execution_qualified" if not errors else errors[0],
            "errors": errors, "eligible": not errors,
            "boundary_audit": qualified.get("boundary_audit"),
            "real_runtime_attempt": qualified["real_runtime_attempt"],
            "qualification_basis": "canonical_source_artifacts_completed_raw_environment_workers"}


def boundary_qualification_for_row(root, row, report):
    """Reopen evidence, pin the runner layout, and recompute; never trust labels.

    For isolated artifacts the caller-owned campaign root and validated cell
    identity fix the runtime directory. A sidecar or raw path cannot choose it.
    Legacy host reports without additive artifact paths keep their contract.
    """
    root = Path(root).resolve()
    config, result = None, None
    if row.get("runtime_config_path"):
        config = json.loads(_path(root, row["runtime_config_path"]).read_text())
    result_path = None
    if row.get("runtime_result_path"):
        candidate = Path(row["runtime_result_path"])
        candidate = candidate if candidate.is_absolute() else root / candidate
        if candidate.exists() or row.get("actual_run") is True:
            result_path = _path(root, row["runtime_result_path"])
            result = json.loads(result_path.read_text())
    # No artifact root is needed to describe a genuinely unexecuted plan.
    if result is None:
        return execution_boundary_qualification(report.get("runtime_boundary"), config=config, row=row)
    cell = row.get("cell_id")
    if not isinstance(cell, str) or cell in {"", ".", ".."} or Path(cell).name != cell or "\\" in cell:
        raise ValueError("boundary_cell_identity_not_path_component")
    directory = "cases" if row.get("campaign_kind") == "exploratory" else "cells"
    output_root = root / directory / cell / "runtime"
    qualification = execution_boundary_qualification(
        report.get("runtime_boundary"), config=config, result=result, row=row,
        formal_runtime=report.get("formal_runtime", report.get("runtime_formal")) is True,
        output_root=output_root)
    if qualification["status"] != "not_applicable":
        expected_config = root / directory / cell / "runtime_config.json"
        if (result_path != output_root.resolve() / "runtime_result.json"
                or _path(root, row.get("runtime_config_path")) != expected_config.resolve()
                or not output_root.resolve().is_relative_to(root)):
            qualification.update(status="invalid", eligible=False, reason_code="boundary_runtime_artifact_layout_mismatch")
            qualification["errors"].append("boundary_runtime_artifact_layout_mismatch")
        expected_source = report.get("source_manifest")
        if expected_source is None and report.get("manifest_path"):
            expected_source = json.loads(_path(root, report["manifest_path"]).read_text()).get("source_manifest")
        if expected_source is not None and canonical_hash((config or {}).get("source_manifest")) != canonical_hash(expected_source):
            qualification.update(status="invalid", eligible=False, reason_code="boundary_report_source_binding_mismatch")
            qualification["errors"].append("boundary_report_source_binding_mismatch")
    return qualification


def _coverage_hit(row):
    """A hit is only the plan-bearing mutation receipt's own boolean."""
    receipt = row.get("mutation_receipt")
    return isinstance(receipt, dict) and receipt.get("hit") is True


def exploratory_coverage_fields(report):
    """Coverage summary for the exploratory report, or None for legacy reports.

    An exploratory report only grows this field when at least one row carries a
    T-E coverage identity (sampler or chain depth).  Reports generated before
    T-E therefore keep their historical bytes, and a legacy audit never fails
    because an additive field is absent.
    """
    rows = report.get("rows")
    if not isinstance(rows, list):
        return None
    if not any(isinstance(row, dict) and ("sampling_kind" in row or "chain_depth" in row)
               for row in rows):
        return None
    projected_rows = []
    for row in rows:
        qualification = row.get("boundary_qualification") or execution_boundary_qualification(report.get("runtime_boundary"), row=row)
        if qualification["status"] not in {"not_applicable", "qualified"}:
            row = dict(row, boundary_qualification=qualification)
        projected_rows.append(row)
    return exploratory_coverage_table(projected_rows)


def exploratory_coverage_table(rows):
    """Additive coverage ledger keyed by target × surface × sampler × chain depth.

    Four independent counters are reported and never merged:

    * ``generated`` — a plan-bearing case was produced (not a rejected stub);
    * ``executed`` — the row recorded a real runtime run;
    * ``judged`` — a boundary-eligible row reached ``judged`` after a real run;
    * ``hit`` — a boundary-eligible row's own receipt reports a real hit.

    Pending/invalid isolated evidence retains generated/actual execution counts,
    but cannot contribute qualified judged/hit coverage. Original rows and raw
    execution history are never rewritten to manufacture an unexecuted run.

    A planned-only case therefore contributes to ``generated`` but never to the
    other three.  Missing sampler/depth values are kept as ``None`` instead of
    being guessed or folded into another class.
    """
    table = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("mutation_plan") is None or row.get("generation_status") == "rejected":
            # A rejected stub was never generated; it has no sampler/depth and
            # belongs in the rejection list, not in the coverage counters.
            continue
        key = (row.get("target_id") or None, row.get("surface_id") or None,
               row.get("sampling_kind"), row.get("chain_depth"))
        entry = table.setdefault(key, {"target_id": key[0], "surface_id": key[1],
                                       "sampling_kind": key[2], "chain_depth": key[3],
                                       "generated": 0, "executed": 0,
                                       "judged": 0, "hit": 0})
        entry["generated"] += 1
        if row.get("actual_run") is True:
            entry["executed"] += 1
        qualification = row.get("boundary_qualification") or execution_boundary_qualification(row=row)
        from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
        if (row.get("target_id") == TOPOLOGY_TARGET
                and (row.get("execution_qualification") or {}).get("real_ros_execution") is not True):
            continue
        if qualification["status"] not in {"not_applicable", "qualified"}:
            continue
        if row.get("execution_stage") == "judged" and row.get("actual_run") is True:
            entry["judged"] += 1
        if _coverage_hit(row):
            entry["hit"] += 1
    return sorted(table.values(), key=lambda item: (
        item["target_id"] or "", item["surface_id"] or "",
        item["sampling_kind"] or "", -1 if item["chain_depth"] is None
        else item["chain_depth"]))


def _empty_required_result(errors):
    return {"sealed": False, "errors": list(errors), "controls": {}, "unknown_gate": {},
            "required_expected": 18, "fault_expected": 6,
            "maturity_scope": "per-target only; full S1 denominator remains 72"}


def audit_report(report_path):
    """Dispatch audits by campaign kind; the required 72/18 contract is unchanged.

    Exploratory reports never borrow the required seal.  Unknown kinds are an
    audit error rather than a silent fallback to the required matrix.
    """
    path = Path(report_path).resolve()
    try:
        report = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        return _empty_required_result(["report_artifact_error:" + str(exc)])
    if not isinstance(report, dict):
        return _empty_required_result(["report_artifact_error:report must be an object"])
    kind = report.get("campaign_kind", "required")
    if report.get("schema_version") == EXPLORATORY_SCHEMA or kind == "exploratory":
        return audit_exploratory_report(path, report=report)
    if kind not in SUPPORTED_CAMPAIGN_KINDS:
        return _empty_required_result(["unknown_campaign_kind:" + repr(kind)])
    return _audit_required_report(path, report)


def _audit_required_report(report_path, report):
    """Recompute evidence and controls. A status string is never seal evidence."""
    root = report_path.parent
    errors = []
    details = {"controls": {}, "unknown_gate": {}, "required_expected": 18, "fault_expected": 6}
    try:
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
                qualification = boundary_qualification_for_row(root, row, report)
                if qualification["status"] != "not_applicable":
                    details.setdefault("boundary_qualifications", {})[cell_id] = qualification
                if qualification["status"] not in {"not_applicable", "qualified"}:
                    raise ValueError(qualification["reason_code"])
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
        eligible_rows = [r for r in rows if boundary_qualification_for_row(root, r, report)["status"] in {"not_applicable", "qualified"}]
        actual_anomalies = {r["cell_id"] for r in eligible_rows if r.get("oracle_verdict") == "fail" and r.get("arm") != "negative" and not r.get("control_kind")}
        if {c.get("cell_id") for c in ledger["candidates"] if c.get("observation_kind", "primary") == "primary"} != actual_anomalies:
            errors.append("candidate_ledger_dropped_or_invented_anomaly")
        if not ledger["candidates"] and not ledger.get("zero_candidate_explanation"):
            errors.append("zero_candidate_explanation_missing")
        from r2f.reports.candidate_report import candidate_ledger, audit_projection
        reconstructed_ledger = candidate_ledger(eligible_rows)
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


def exploratory_report_digest(report):
    """Digest excludes only derived audit output, never evidence fields."""
    return canonical_hash({k: v for k, v in report.items() if k not in {"report_sha256", "audit", "seal"}})


def _sha_or_none(value):
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    return value if len(value) == 64 and all(c in "0123456789abcdef" for c in value) else None


def _exploratory_control_key(ref):
    """A control is only shareable inside one source/RMW/contract/recipe class."""
    source = ref.get("source_sha256") or ref.get("source_identity") or ref.get("source")
    contract = ref.get("target_contract") or ref.get("contract") or ref.get("target_id")
    return (source, ref.get("rmw"), contract, ref.get("recipe"))


def _validate_exploratory_profile(report, errors):
    profile = report.get("profile")
    if not isinstance(profile, dict):
        errors.append("exploratory_profile_missing")
        return None
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
    if (TOPOLOGY_TARGET in (profile.get("target_ids") or [])
            or profile.get("runtime_capability") == "topology_v1"):
        from r2f.campaign.exploratory_input import validate_topology_profile
        try:
            validate_topology_profile(profile)
        except (ValueError, TypeError, KeyError) as exc:
            errors.append("exploratory_topology_profile_invalid:" + str(exc))
        return profile
    try:
        from r2f.modules.s1.fuzz_catalog import SURFACES
    except (ImportError, ValueError) as exc:  # pragma: no cover - catalog is required
        errors.append("exploratory_catalog_unavailable:" + str(exc))
        return profile
    if profile.get("campaign_kind") != "exploratory":
        errors.append("exploratory_profile_campaign_kind_invalid")
    if profile.get("required_catalog_included") is not False:
        errors.append("exploratory_profile_must_not_include_required_catalog")
    surfaces = profile.get("surfaces")
    if not isinstance(surfaces, list) or not surfaces:
        errors.append("exploratory_profile_surfaces_missing")
    else:
        unknown = sorted({s for s in surfaces if s not in SURFACES})
        if unknown:
            errors.append("exploratory_profile_unknown_surfaces:" + ",".join(unknown))
    rmws = profile.get("rmws")
    if not isinstance(rmws, list) or not rmws or any(r not in RMWS for r in rmws):
        errors.append("exploratory_profile_rmw_invalid")
    target_ids = profile.get("target_ids")
    if not isinstance(target_ids, list) or not target_ids:
        errors.append("exploratory_profile_targets_missing")
    return profile


def _validate_exploratory_identity(report, errors):
    identity = report.get("identity")
    if not isinstance(identity, dict):
        errors.append("exploratory_identity_missing")
        return
    for key in ("profile_sha256", "model_sha256", "rule_sha256", "target_sha256"):
        if _sha_or_none(identity.get(key)) is None:
            errors.append("exploratory_identity_invalid:" + key)
    source = report.get("source_manifest")
    if not isinstance(source, dict):
        errors.append("exploratory_source_manifest_missing")
    else:
        reject_execution_identity(source)
        if not source.get("head") or not source.get("behavior_tree_sha256"):
            errors.append("exploratory_source_identity_unfrozen")
        if source.get("source_frozen") is not True or source.get("dirty_behavior_paths") != []:
            errors.append("exploratory_source_not_frozen")
        if identity.get("source_sha256") not in {None, canonical_hash(source)}:
            errors.append("exploratory_source_identity_mismatch")
    model = report.get("model")
    if not isinstance(model, dict):
        errors.append("exploratory_model_identity_missing")
    else:
        from r2f.modules.s1.fuzz_catalog import catalog_digest
        if model.get("catalog_digest") not in {None, catalog_digest()}:
            errors.append("exploratory_model_catalog_digest_mismatch")
        mutators = model.get("mutator_catalog")
        if not isinstance(mutators, dict) or not mutators:
            errors.append("exploratory_mutator_catalog_missing")
        elif model.get("mutator_catalog_sha256") not in {None, canonical_hash(mutators)}:
            errors.append("exploratory_mutator_catalog_hash_mismatch")
    from r2f.campaign.scheduler import required_catalog_hash
    if report.get("required_catalog_sha256") not in {None, required_catalog_hash()}:
        errors.append("required_catalog_identity_changed")
    # Exploratory reports may never claim the required matrix or its denominator.
    if report.get("full_scope_required_cells") == 72 or report.get("required_matrix") is not None:
        errors.append("exploratory_report_cannot_claim_required_matrix")


def _topology_artifact_qualification(root, row, report):
    """Reopen the unified case layout; saved labels never grant qualification."""
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
    from r2f.campaign.replay import _registered_target, _qualification, DEFAULT_RUNTIME, ISOLATED_RUNTIME
    from r2f.runtime.topology_route import validate_topology_route
    root = Path(root).resolve()
    cell = row.get("cell_id")
    if (not isinstance(cell, str) or cell in {"", ".", ".."}
            or Path(cell).name != cell or "\\" in cell):
        raise ValueError("topology_cell_identity_not_path_component")
    case = root / "cases" / cell
    paths = {"replay_path": case / "capsule.json", "runtime_config_path": case / "runtime_config.json",
             "runtime_result_path": case / "runtime" / "runtime_result.json"}
    for key, expected in paths.items():
        if key != "runtime_result_path" or expected.exists() or row.get("actual_run") is True:
            if _path(root, row.get(key)) != expected.resolve():
                raise ValueError("topology_artifact_path_mismatch:" + key)
    capsule = ReplayCapsule.from_dict(json.loads(paths["replay_path"].read_text()))
    target, services, _ = _registered_target(capsule)
    if (target.target_id != TOPOLOGY_TARGET
            or target.observation_contract.get("runtime_capability") != TOPOLOGY_CAPABILITY):
        raise ValueError("topology_registered_capability_mismatch")
    config = json.loads(paths["runtime_config_path"].read_text())
    plan = validate_topology_route(config)
    if json.loads(_path(root, str(case / "mutation_plan.json")).read_text()) != plan.to_dict():
        raise ValueError("topology_plan_artifact_mismatch")
    identity = capsule.input
    for key in ("scenario", "mutation_plan", "effective_input", "effective_input_sha256", "source_manifest"):
        if canonical_hash(config.get(key)) != canonical_hash(identity.get(key)):
            raise ValueError("topology_config_capsule_mismatch:" + key)
    for key in ("run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "seed",
                "effective_input_sha256", "input_layer_receipts", "mutation_plan", "final_input"):
        if canonical_hash(config.get(key)) != canonical_hash(row.get(key)):
            raise ValueError("topology_config_row_mismatch:" + key)
    if config.get("cell_id") != cell or config.get("profile_id") != report.get("profile", {}).get("profile_id"):
        raise ValueError("topology_config_report_identity_mismatch")
    if canonical_hash(config.get("source_manifest")) != canonical_hash(report.get("source_manifest")):
        raise ValueError("topology_report_source_mismatch")
    if not paths["runtime_result_path"].exists():
        if row.get("actual_run") is True or row.get("execution_stage") not in {"planned", "aborted"}:
            raise ValueError("topology_execution_result_missing")
        qualified = {"real_ros_execution": False, "qualification_errors": ["topology_not_executed"]}
    else:
        result = json.loads(paths["runtime_result_path"].read_text())
        original = Scenario.from_dict(identity["scenario"])
        recalculated = ingest_execution(original, config, result, capsule, services=services)
        for key in ("execution_stage", "actual_run", "abort_reason", "typed_facts", "oracle", "oracle_verdict",
                    "rule_id", "mutation_receipt", "precondition_receipts", "observation_receipts",
                    "raw_path", "raw_sha256", "runtime_status", "runtime_reason", "runtime_execution_metadata"):
            if canonical_hash(row.get(key)) != canonical_hash(recalculated.get(key)):
                raise ValueError("topology_recomputed_ingest_differs:" + key)
        formal = report.get("formal_runtime", report.get("runtime_formal")) is True
        boundary = report.get("runtime_boundary", "host")
        origin = (ISOLATED_RUNTIME if boundary == "isolated_netns" else DEFAULT_RUNTIME) if formal else "injected_runtime"
        qualified = _qualification(origin, config, result, recalculated, output_root=case / "runtime")
        source = config.get("source_manifest") or {}
        if (not source.get("source_repo")
                or canonical_hash(source_manifest(Path(source["source_repo"]))) != canonical_hash(source)):
            qualified["qualification_errors"].append("topology_current_source_mismatch")
            qualified["real_ros_execution"] = False
    if not qualified["real_ros_execution"]:
        if any(row.get(key) is True for key in ("candidate_eligible", "feedback_eligible", "seal_eligible")):
            raise ValueError("topology_unqualified_execution_claims_eligibility")
        if row.get("execution_stage") == "judged" or row.get("actual_run") is True:
            raise ValueError("topology_execution_not_qualified:" + ",".join(qualified["qualification_errors"]))
    return qualified


def _verify_exploratory_row(root, row, report, errors):
    cell = row.get("cell_id", "<missing>")
    try:
        for key in ("cell_id", "run_id", "target_id", "profile_id", "rmw"):
            if not isinstance(row.get(key), str) or not row.get(key):
                raise ValueError("identity field missing: " + key)
        if type(row.get("target_revision")) is not int:
            raise ValueError("integer target_revision required")
        if row.get("campaign_kind") != "exploratory":
            raise ValueError("row campaign_kind must be exploratory")
        if row.get("run_id") != report.get("run_id"):
            raise ValueError("row run_id differs from report")
        if row.get("arm") not in {"baseline", "transparent", "mutated", "negative"}:
            raise ValueError("invalid arm")
        if row.get("execution_stage") not in {
                "modelled", "expressible", "planned", "precondition_executed", "reached",
                "mutation_applied", "effect_observed", "judged", "aborted"}:
            raise ValueError("invalid execution_stage")
        qualification = boundary_qualification_for_row(root, row, report)
        if qualification["status"] not in {"not_applicable", "qualified"}:
            raise ValueError(qualification["reason_code"])
        from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
        if row["target_id"] == TOPOLOGY_TARGET:
            if row.get("generation_status") == "rejected":
                if (row.get("actual_run") is not False or row.get("mutation_plan") is not None
                        or row.get("execution_stage") != "planned"
                        or row.get("attribution") != "harness_input_generation"
                        or not row.get("reason_code", "").startswith("topology_input_rejected:")):
                    raise ValueError("topology_generation_rejection_claim_invalid")
                return True
            _topology_artifact_qualification(root, row, report)
        plan = row.get("mutation_plan")
        if plan is not None:
            if not isinstance(plan, dict):
                raise ValueError("mutation_plan must be an object")
            if row.get("mutation_plan_sha256") != canonical_hash(plan):
                raise ValueError("mutation_plan_sha256_mismatch")
        receipts = row.get("input_layer_receipts")
        if not isinstance(receipts, list):
            raise ValueError("input_layer_receipts missing")
        if row.get("effective_input") is not None:
            effective_input_hash(row["effective_input"])
        elif row.get("effective_input_sha256") is not None:
            _sha_or_none(row["effective_input_sha256"]) or (_ for _ in ()).throw(
                ValueError("effective_input_sha256 must be a sha256 digest"))
        binding = row.get("binding")
        if not isinstance(binding, dict):
            raise ValueError("runtime binding receipt missing")
        expected = binding.get("expected_config_sha256")
        observed = binding.get("observed_config_sha256")
        normalized = binding.get("silent_normalization") is True
        binding_ok = _sha_or_none(expected) is not None and expected == observed and not normalized
        executed = row.get("execution_stage") in {"effect_observed", "judged"}
        tool_error = row.get("tool_error") is True or row.get("runtime_status") == "tool_error"
        if executed and tool_error:
            raise ValueError("tool_error row cannot be an executed SUT observation")
        if executed and row.get("observer_dropped") is True:
            raise ValueError("observer_dropped row cannot be an executed SUT observation")
        if executed and not binding_ok:
            raise ValueError("executed row lacks a clean adapter binding (silent normalization "
                             "or config hash mismatch)")
        if row.get("actual_run") is True and row.get("execution_stage") in {"modelled", "expressible", "planned"}:
            raise ValueError("planned row cannot claim actual_run")
        if executed:
            if row.get("actual_run") is not True:
                raise ValueError("observed row must record actual_run")
            raw = _path(root, row.get("raw_path"))
            verify_raw(raw, row.get("raw_sha256"))
            if row.get("oracle_verdict") not in {"pass", "fail", "unknown", "not_applicable"}:
                raise ValueError("judged row requires a four-state oracle verdict")
        if row.get("control_kind") in {"fault", "negative"} and row.get("arm") != "negative":
            raise ValueError("fault control must use the negative arm")
        return True
    except (KeyError, ValueError, TypeError, OSError) as exc:
        errors.append(f"{cell}: {exc}")
        return False


def _audit_exploratory_budget(report, rows, errors):
    budget = report.get("budget")
    if not isinstance(budget, dict):
        errors.append("exploratory_budget_missing")
        return
    counters = {}
    for key in ("planned", "generated", "attempted", "executed", "aborted", "interrupted"):
        value = budget.get(key)
        if value is not None and (type(value) is not int or value < 0):
            errors.append("exploratory_budget_invalid:" + key)
            return
        counters[key] = 0 if value is None else value
    if not (counters["planned"] >= counters["generated"] >= counters["attempted"] >= counters["executed"]):
        errors.append("exploratory_budget_counters_inconsistent")
    actual_executed = sum(1 for r in rows
                          if r.get("execution_stage") == "judged" and r.get("actual_run") is True)
    if counters["executed"] != actual_executed:
        errors.append(f"exploratory_budget_executed_mismatch:{counters['executed']}!={actual_executed}")
    planned_rows = [r for r in rows if r.get("execution_stage") == "planned"]
    if len(planned_rows) > counters["planned"]:
        errors.append("exploratory_planned_rows_exceed_budget")
    last = report.get("last_state")
    if not isinstance(last, dict):
        errors.append("exploratory_last_state_missing")
    else:
        if last.get("planned") != counters["planned"] or last.get("executed") != counters["executed"]:
            errors.append("exploratory_last_state_counters_mismatch")
        if last.get("cell_id") and last["cell_id"] not in {r.get("cell_id") for r in rows}:
            errors.append("exploratory_last_state_cell_unknown")
    abort = report.get("abort")
    if abort is not None:
        if not isinstance(abort, dict) or abort.get("reason_code") not in {
                "source_repo_HEAD_or_behavior_tree_changed", "config_changed", "cli_contract_changed",
                "observer_unhealthy", "budget_exhausted", "operator_interrupt",
                BOUNDARY_AUDIT_PENDING, "boundary_route_context_mismatch",
                "execution_boundary_qualification_failed"}:
            errors.append("exploratory_abort_reason_unrecognized")
        else:
            index = abort.get("after_row_index")
            if type(index) is not int or index < 0 or index >= len(rows):
                errors.append("exploratory_abort_index_invalid")
            else:
                for row in rows[index + 1:]:
                    if row.get("actual_run") is True or row.get("execution_stage") in {"effect_observed", "judged"}:
                        errors.append("exploratory_row_executed_after_abort:" + str(row.get("cell_id")))


def _audit_exploratory_controls(report, rows, errors):
    refs = report.get("control_references")
    if not isinstance(refs, list):
        errors.append("exploratory_control_references_missing")
        return
    seen = {}
    for ref in refs:
        if not isinstance(ref, dict):
            errors.append("exploratory_control_reference_invalid")
            continue
        control_id = ref.get("control_id")
        if not isinstance(control_id, str) or not control_id:
            errors.append("exploratory_control_reference_without_id")
            continue
        key = _exploratory_control_key(ref)
        if any(member in {None, ""} for member in key):
            errors.append("exploratory_control_reference_incomplete:" + control_id)
            continue
        if control_id in seen and seen[control_id] != key:
            errors.append("control_reference_shared_across_incompatible_contract:" + control_id)
        seen[control_id] = key
        if ref.get("applies") is not True:
            errors.append("control_reference_not_applicable:" + control_id)
        if not _sha_or_none(ref.get("raw_sha256")):
            errors.append("control_reference_raw_identity_missing:" + control_id)
        if not isinstance(ref.get("cell_ids"), list) or not ref["cell_ids"]:
            errors.append("control_reference_cell_list_missing:" + control_id)
        else:
            known = {r.get("cell_id") for r in rows}
            for cell in ref["cell_ids"]:
                if cell not in known:
                    errors.append("control_reference_unknown_cell:" + control_id + ":" + str(cell))
        if ref.get("counts_as_exploratory_input") is True:
            errors.append("control_counted_as_exploratory_input:" + control_id)
    referenced = {cell for ref in refs if isinstance(ref, dict) for cell in (ref.get("cell_ids") or [])}
    for row in rows:
        if row.get("control_kind") and row.get("actual_run") is True and row.get("cell_id") not in referenced:
            errors.append("unreferenced_control_execution:" + str(row.get("cell_id")))
    budget = report.get("budget") or {}
    declared = budget.get("control_executions")
    if declared is not None and (type(declared) is not int or declared != len(seen)):
        errors.append("control_execution_count_not_deduplicated")


def _audit_exploratory_ledger(root, report, rows, errors, details):
    from r2f.reports.candidate_report import exploratory_candidate_ledger, audit_projection
    ledger_path = _path(root, report.get("candidate_ledger_path"))
    ledger = json.loads(ledger_path.read_text())
    if canonical_hash(ledger) != report.get("candidate_ledger_sha256"):
        errors.append("candidate_ledger_hash_mismatch")
    eligible_rows, exclusions = [], []
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
    for row in rows:
        if boundary_qualification_for_row(root, row, report)["status"] not in {"not_applicable", "qualified"}:
            continue
        if row.get("target_id") == TOPOLOGY_TARGET:
            try:
                qualified = _topology_artifact_qualification(root, row, report)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                qualified = {"real_ros_execution": False, "qualification_errors": [str(exc)]}
            if not qualified["real_ros_execution"]:
                exclusions.append({"cell_id": row.get("cell_id"), "actual_run": row.get("actual_run"),
                                   "execution_stage": row.get("execution_stage"), "raw_path": row.get("raw_path"),
                                   "raw_sha256": row.get("raw_sha256"),
                                   "qualification_errors": qualified["qualification_errors"]})
                continue
        eligible_rows.append(row)
    expected = exploratory_candidate_ledger(eligible_rows, reviews=report.get("review_records") or [])
    if exclusions and ledger.get("execution_exclusions") != exclusions:
        errors.append("topology_ledger_execution_exclusions_mismatch")
    if Counter(c.get("candidate_id") for c in ledger.get("candidates", [])) != Counter(
            c.get("candidate_id") for c in expected["candidates"]):
        errors.append("candidate_ledger_dropped_or_invented_anomaly")
    for key in ("natural_candidate_count", "confirmed_candidates", "tool_error_count",
                "observer_incomplete_count", "control_count"):
        if ledger.get(key) != expected.get(key):
            errors.append("exploratory_ledger_mismatch:" + key)
    if ledger.get("natural_candidate_count") == 0 and not ledger.get("zero_candidate_explanation"):
        errors.append("zero_candidate_explanation_missing")
    # Zero natural candidates, or an anomaly that failed admission and stayed
    # observed, is a normal exploratory outcome rather than a damaged report.
    expected_natural = sum(1 for c in ledger.get("candidates", [])
                           if c.get("responsibility_layer") == "sut"
                           and c.get("triage_disposition") == "candidate")
    if ledger.get("natural_candidate_count") != expected_natural:
        errors.append("natural_candidate_count_not_derived_from_ledger")
    if ledger.get("confirmed_candidates") != sum(
            c.get("candidate_status") == "confirmed" for c in ledger.get("candidates", [])):
        errors.append("confirmed_count_not_derived_from_ledger")
    for candidate in ledger.get("candidates", []):
        match = next((c for c in expected["candidates"]
                      if c.get("candidate_id") == candidate.get("candidate_id")), None)
        if match is None or audit_projection(candidate) != audit_projection(match):
            errors.append("candidate_evidence_or_lifecycle_not_bound_to_verified_cell")
        evidence = candidate.get("evidence") or {}
        if evidence.get("arm") == "negative" or evidence.get("candidate_source") == "control":
            errors.append("control_entered_candidate_ledger")
        if evidence.get("tool_error") is True or evidence.get("observer_dropped") is True:
            errors.append("tool_error_entered_candidate_ledger")
        admitted = candidate.get("candidate_status") in {"admitted", "triaged", "deferred", "confirmed"}
        if admitted and not all(candidate.get(key) for key in
                                ("triage_disposition", "responsibility_layer", "reason_code")):
            errors.append("candidate_attribution_missing")
    for control in ledger.get("controls", []):
        # Negative/fault controls must retain harness provenance; ordinary
        # declared health/mutation/transparency controls are not harness faults.
        if (control.get("arm") == "negative" or control.get("kind") == "fault") \
                and control.get("source") != "harness":
            errors.append("negative_control_harness_provenance_lost")
    # A malformed or unreferenced review is rejected by the reviewer gate.  It
    # cannot raise a candidate's status, but its mere presence does not mean the
    # report's own evidence chain is corrupt.
    rejected = []
    for index, review in enumerate(report.get("review_records") or []):
        if not isinstance(review, dict):
            rejected.append({"index": index, "reason": "review_record_not_an_object"})
            continue
        problems = []
        if review.get("conclusion") not in {"supported", "rejected", "needs_evidence"}:
            problems.append("review_conclusion_invalid")
        if not isinstance(review.get("reviewer_id"), str) or not review["reviewer_id"].strip():
            problems.append("review_without_reviewer")
        if not isinstance(review.get("evidence_refs"), dict) or not review["evidence_refs"]:
            problems.append("review_without_evidence_references")
        if review.get("reviewer_id") in set(review.get("independent_of") or []):
            problems.append("reviewer_not_independent")
        if problems:
            rejected.append({"index": index, "candidate_id": review.get("candidate_id"),
                             "reason": ",".join(problems)})
    details["rejected_reviews"] = rejected
    details["accepted_reviews"] = len(report.get("review_records") or []) - len(rejected)
    details["candidate_ledger"] = ledger_path.name


def audit_exploratory_report(report_path, *, report=None):
    """Audit an exploratory report without borrowing required maturity or seal.

    ``evidence_valid`` answers whether the report and its artefacts recompute;
    ``all_oracles_pass`` is a separate observation outcome.  A real SUT anomaly
    therefore leaves evidence valid.  Exploratory runs never return
    ``sealed=True``: no frozen exploratory maturity matrix exists yet.
    """
    report_path = Path(report_path).resolve()
    root = report_path.parent
    errors = []
    details = {"audit_kind": "exploratory", "required_expected": 18, "fault_expected": 6}
    try:
        if report is None:
            report = json.loads(report_path.read_text())
        if not isinstance(report, dict):
            raise ValueError("report must be an object")
        if report.get("schema_version") != EXPLORATORY_SCHEMA:
            errors.append("exploratory_schema_version_invalid")
        if report.get("campaign_kind") != "exploratory":
            errors.append("exploratory_campaign_kind_invalid")
        if report.get("report_sha256") != exploratory_report_digest(report):
            errors.append("report_hash_mismatch")
        if report.get("seal") is True or report.get("sealed") is True:
            errors.append("exploratory_report_cannot_borrow_required_seal")
        if not isinstance(report.get("run_id"), str) or not report["run_id"]:
            errors.append("run_id_missing")
        _validate_exploratory_identity(report, errors)
        profile = _validate_exploratory_profile(report, errors)
        rows = report.get("rows")
        if not isinstance(rows, list) or not rows:
            errors.append("exploratory_rows_missing")
            rows = []
        cell_ids = [r.get("cell_id") for r in rows]
        if len(cell_ids) != len(set(cell_ids)):
            errors.append("exploratory_cell_ids_duplicated")
        if profile:
            known_targets = set(profile.get("target_ids") or [])
            allowed_rmws = profile.get("rmws") or []
            for row in rows:
                if row.get("target_id") not in known_targets:
                    errors.append("exploratory_row_target_not_in_profile:" + str(row.get("cell_id")))
                if row.get("rmw") not in allowed_rmws:
                    errors.append("exploratory_row_rmw_not_in_profile:" + str(row.get("cell_id")))
        valid = [r for r in rows if _verify_exploratory_row(root, r, report, errors)]
        details["rows_verified"] = len(valid)
        details["rows_total"] = len(rows)
        _audit_exploratory_budget(report, rows, errors)
        _audit_exploratory_controls(report, rows, errors)
        _audit_exploratory_ledger(root, report, rows, errors, details)
        coverage = []
        for row in valid:
            if row.get("execution_stage") != "judged" or row.get("actual_run") is not True:
                continue
            if row.get("tool_error") is True or row.get("observer_dropped") is True:
                continue
            if row.get("observer_complete") is not True:
                continue
            coverage.append(row)
        details["coverage_entries"] = len(coverage)
        details["coverage_surfaces"] = sorted({s for row in coverage for s in (row.get("surface_ids") or [])})
        qualified_coverage_rows = []
        for row in rows:
            from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
            if row.get("target_id") == TOPOLOGY_TARGET:
                try:
                    recomputed = _topology_artifact_qualification(root, row, report)
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    recomputed = {"real_ros_execution": False, "qualification_errors": [str(exc)]}
                details.setdefault("topology_qualifications", {})[row.get("cell_id", "<missing>")] = recomputed
                row = dict(row, execution_qualification=recomputed)
            qualification = boundary_qualification_for_row(root, row, report)
            if qualification["status"] != "not_applicable":
                details.setdefault("boundary_qualifications", {})[row.get("cell_id", "<missing>")] = qualification
                row = dict(row, boundary_qualification=qualification)
            qualified_coverage_rows.append(row)
        coverage_table = exploratory_coverage_fields(dict(report, rows=qualified_coverage_rows))
        if coverage_table is not None:
            details["coverage"] = coverage_table
        observed = [r for r in valid if r.get("execution_stage") == "judged"]
        details["unknown_count"] = sum(1 for r in observed if r.get("oracle_verdict") == "unknown")
        details["oracle_fail_count"] = sum(
            1 for r in observed if r.get("oracle_verdict") == "fail"
            and r.get("arm") != "negative" and not r.get("control_kind"))
        all_pass = bool(observed) and all(r.get("oracle_verdict") in {"pass", "not_applicable"} for r in observed)
        details["all_oracles_pass"] = all_pass
        details["evidence_valid"] = not errors
        return {"sealed": False, "seal": EXPLORATORY_SEAL, "audit_kind": "exploratory",
                "evidence_valid": not errors, "all_oracles_pass": all_pass,
                "errors": errors, **details,
                "maturity_scope": "exploratory only; required 72 denominator unchanged and not claimed"}
    except (OSError, ValueError, TypeError, KeyError) as exc:
        errors.append("report_artifact_error:" + str(exc))
    return {"sealed": False, "seal": EXPLORATORY_SEAL, "audit_kind": "exploratory",
            "evidence_valid": False, "all_oracles_pass": False, "errors": errors, **details,
            "maturity_scope": "exploratory only; required 72 denominator unchanged and not claimed"}
