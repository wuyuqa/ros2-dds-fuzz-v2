"""Independent, hash-checked offline acceptance of persisted campaign evidence."""
from collections import Counter
from copy import deepcopy
import hashlib
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


def topology_nohit_continuation(config, result, row, qualified):
    """Separate non-success terminal audit; strict qualification stays failed.

    Do not filter errors or promote a row to judged. In particular, every
    independent boundary/source/raw/worker/cleanup failure is still terminal
    for the campaign, even if an input_terminal label was supplied.
    """
    if "input_terminal" not in row:
        return False
    from r2f.campaign.runtime_adapter import topology_nohit_terminal
    expected = topology_nohit_terminal(config, result, row)
    if (row["input_terminal"] != expected or row.get("oracle_verdict") != "not_applicable"
            or row.get("oracle") != {"rule_id": row.get("rule_id"), "rule_role": "primary",
                "verdict": "not_applicable", "reason_codes": ["topology_mutation_nohit"],
                "forbidden_residuals": [], "closure": {}}
            or not isinstance(row.get("diagnostic_oracle"), dict)
            or any(row.get(k) is not False for k in ("candidate_eligible", "feedback_eligible", "seal_eligible"))
            or qualified.get("real_ros_execution") is not False
            or qualified.get("real_runtime_attempt") is not True
            or qualified.get("qualification_errors") != ["real_judged_execution_missing"]):
        raise ValueError("topology_nohit_terminal_not_independently_valid")
    isolated = isinstance(config.get("execution_boundary"), dict) and config["execution_boundary"].get("enabled") is True
    if isolated and (qualified.get("boundary_audit") or {}).get("valid") is not True:
        raise ValueError("topology_nohit_terminal_boundary_audit_invalid")
    source = config["source_manifest"]
    if canonical_hash(source_manifest(Path(source["source_repo"]))) != canonical_hash(source):
        raise ValueError("topology_nohit_terminal_current_source_mismatch")
    return True


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
    terminal = False
    if "input_terminal" in row:
        try:
            terminal = topology_nohit_continuation(config, result, row, qualified)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append("topology_nohit_terminal_invalid:" + str(exc))
    return {"status": "terminal_unqualified" if terminal else ("qualified" if not errors else "invalid"),
            "continuation_eligible": terminal,
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


def audit_report(report_path, *, human_authority=None):
    """Audit original evidence and query the existing S1 lineage in one result.

    The added projection is detached and never participates in report hashes or
    changes the per-target seal. Old reports need no new persisted fields.
    """
    path = Path(report_path).resolve()
    report = {}
    try:
        report = json.loads(path.read_text())
        if not isinstance(report, dict):
            raise ValueError("report must be an object")
    except (OSError, ValueError) as exc:
        report = {}
        result = _empty_required_result(["report_artifact_error:" + str(exc)])
    else:
        kind = report.get("campaign_kind", "required")
        rows = report.get("rows", [])
        if isinstance(rows, list) and any(not isinstance(row, dict) for row in rows):
            result = _empty_required_result(["report_artifact_error:rows must contain objects"])
        elif report.get("schema_version") == EXPLORATORY_SCHEMA or kind == "exploratory":
            return audit_exploratory_report(path, report=report, human_authority=human_authority)
        elif kind not in SUPPORTED_CAMPAIGN_KINDS:
            result = _empty_required_result(["unknown_campaign_kind:" + repr(kind)])
        else:
            result = _audit_required_report(path, report)
    return _with_s1_coverage(path, report, result)


def _with_s1_coverage(path, report, result):
    try:
        result["s1_coverage"] = _s1_coverage_fields(path, report, result)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        result["s1_coverage"] = {"query_valid": False, "inventory_status": "unavailable",
            "named_gaps": ["coverage_query_error:" + str(exc)],
            "report_evidence": {"report_path": str(path), "source_current": False}}
    if (result.get("sealed") is True
            and not result["s1_coverage"]["report_evidence"]["source_current"]):
        result["sealed"] = False
        result["errors"].append("s1_coverage:source_not_current")
    if not result["s1_coverage"]["query_valid"]:
        result["sealed"] = False
        if "evidence_valid" in result:
            result["evidence_valid"] = False
        result["errors"].extend("s1_coverage:" + error for error in result["s1_coverage"]["named_gaps"])
    return result


def _s1_coverage_fields(path, report, audit):
    """Project the CP2 sidecar, without treating a target link as a proof."""
    from r2f.modules.s1.catalog import coverage_lineage
    from r2f.reports.candidate_report import query_automatic_replays

    data = coverage_lineage()
    query_errors = []
    source = report.get("source_manifest")
    if report.get("manifest_path"):
        try:
            manifest = json.loads(_path(path.parent, report["manifest_path"]).read_text())
            source = manifest["source_manifest"]
            if identity_hashes(manifest) != report.get("identity"):
                raise ValueError("report_manifest_identity_mismatch")
        except (OSError, ValueError, TypeError, KeyError) as exc:
            query_errors.append("manifest:" + str(exc))
    current = source_manifest(Path(__file__).resolve().parents[3])
    source_current = (source == current and current.get("source_frozen") is True)
    rows = []
    report_rows = report.get("rows", [])
    if not isinstance(report_rows, list):
        query_errors.append("report_rows_not_a_list")
        report_rows = []
    for row in report_rows:
        if not isinstance(row, dict):
            query_errors.append("report_row_not_an_object")
            continue
        evidence = _s1_row_evidence(path.parent, row, source, data["model_bindings"])
        _s1_execution_context(path.parent, row, report, audit, evidence, source_current)
        rows.append(evidence)
        query_errors.extend(str(row.get("cell_id")) + ":" + error for error in evidence["reference_errors"])
    data["report_evidence"] = {"report_path": str(path), "run_id": report.get("run_id"),
        "source_manifest": source, "current_source_manifest": current,
        "source_current": source_current,
        "evidence_audit_valid": not audit["errors"],
        "named_gaps": [] if source_current else ["source_not_current"],
        "audit_errors": list(audit["errors"]), "rows": rows}
    replay_trace = {"status": "not_observed", "named_gaps": ["replay_disposition_not_observed"]}
    if report.get("candidate_ledger_path") is not None:
        try:
            ledger_path = _path(path.parent, report["candidate_ledger_path"])
            ledger = json.loads(ledger_path.read_text())
            if canonical_hash(ledger) != report.get("candidate_ledger_sha256"):
                raise ValueError("hash_mismatch")
            if not isinstance(ledger, dict):
                raise ValueError("ledger_not_an_object")
            candidates = ledger.get("candidates")
            if not isinstance(candidates, list):
                raise ValueError("candidates_not_a_list")
            dispositions = []
            for index, candidate in enumerate(candidates):
                if not isinstance(candidate, dict):
                    raise ValueError("candidate_not_an_object:" + str(index))
                attempts = candidate.get("replay_attempts", [])
                if not isinstance(attempts, list):
                    raise ValueError("replay_attempts_not_a_list:" + str(index))
                for attempt in attempts:
                    validate_replay_attempt(attempt)
                dispositions.append({key: deepcopy(candidate.get(key)) for key in
                    ("candidate_id", "cell_id", "replay_attempts", "non_replayable")})
            replay_trace = {"status": "verified_ledger_reference", "path": str(ledger_path),
                "sha256": report["candidate_ledger_sha256"], "dispositions": dispositions,
                "limitation": "Existing replay dispositions; this query executes no replay."}
        except (OSError, ValueError, TypeError, KeyError) as exc:
            query_errors.append("replay_ledger:" + str(exc))
            replay_trace = {"status": "invalid", "reason": str(exc)}
    data["report_evidence"]["replay_trace"] = replay_trace
    automatic = query_automatic_replays(path.parent)
    if "attachment_valid" in automatic:
        partial_requests = (automatic.get("attachment_valid") is False
                            and automatic.get("reason_code") == "automatic_replay_attachment_failure"
                            and not (path.parent / "replay-summary.json").exists())
        try:
            if (not isinstance(report.get("run_id"), str) or not report["run_id"]
                    or (not partial_requests and automatic.get("run_id") != report["run_id"])):
                raise ValueError("report_run_id_mismatch")
            # Directory-level validation does not associate an attachment with
            # this report. Bind it to the saved index and exact report bytes.
            index_path = _path(path.parent, "evidence-index.json")
            index_bytes = index_path.read_bytes()
            index_ref = ({"path": "evidence-index.json",
                          "sha256": hashlib.sha256(index_bytes).hexdigest()}
                         if partial_requests else automatic.get("original_index"))
            if (not isinstance(index_ref, dict)
                    or _path(path.parent, index_ref.get("path")) != index_path
                    or hashlib.sha256(index_bytes).hexdigest() != index_ref.get("sha256")):
                raise ValueError("report_index_reference_mismatch")
            index = json.loads(index_bytes)
            if not isinstance(index, dict) or index.get("run_id") != report["run_id"]:
                raise ValueError("report_run_id_mismatch")
            if not isinstance(source, dict) or source != index.get("source_manifest"):
                raise ValueError("report_source_mismatch")
            cells = [row["cell_id"] for row in report_rows]
            indexed_cells = [ref["cell_id"] for ref in index["identities"]]
            if (len(cells) != len(set(cells)) or len(indexed_cells) != len(set(indexed_cells))
                    or set(cells) != set(indexed_cells)):
                raise ValueError("report_cell_set_mismatch")
            members = [ref for ref in index["reports"] if isinstance(ref, dict)
                       and isinstance(ref.get("path"), str)
                       and (path.parent / ref["path"]).resolve() == path]
            if len(members) != 1:
                raise ValueError("report_not_indexed")
            if hashlib.sha256(path.read_bytes()).hexdigest() != members[0].get("sha256"):
                raise ValueError("indexed_report_hash_mismatch")
            if partial_requests:
                # Missing summary means unknown requests, not verified attempts.
                # Attribute them only through the indexed original and intake.
                if "failure" in automatic and (not isinstance(automatic["failure"], dict)
                        or automatic["failure"].get("run_id") != report["run_id"]):
                    raise ValueError("failure_run_id_mismatch")
                identities = {ref["cell_id"]: ref for ref in index["identities"]}
                for entry in automatic["entries"]:
                    request_ref = entry["request"]
                    request_path = _path(path.parent, request_ref["path"])
                    expected = path.parent / "replay-scheduling" / entry["dispatch_id"] / "request.json"
                    if (request_path != expected.resolve() or not request_path.is_relative_to(path.parent)
                            or hashlib.sha256(request_path.read_bytes()).hexdigest() != request_ref["sha256"]):
                        raise ValueError("request_reference_mismatch")
                    request = json.loads(request_path.read_text())
                    if not isinstance(request, dict) or request.get("run_id") != report["run_id"]:
                        raise ValueError("request_run_id_mismatch")
                    identity = identities.get(entry["cell_id"])
                    if (identity is None or request.get("cell_id") != entry["cell_id"]
                            or request.get("dispatch_id") != entry["dispatch_id"]
                            or request.get("original_index") != index_ref
                            or request.get("intake_identity") != {k: v for k, v in identity.items() if k != "cell_id"}):
                        raise ValueError("request_identity_mismatch")
                    identity_path = _path(path.parent, identity["path"])
                    if (not identity_path.is_relative_to(path.parent)
                            or hashlib.sha256(identity_path.read_bytes()).hexdigest() != identity["sha256"]):
                        raise ValueError("request_intake_reference_mismatch")
                    saved = json.loads(identity_path.read_text())
                    if (not isinstance(saved, dict) or saved.get("run_id") != report["run_id"]
                            or saved.get("cell_id") != entry["cell_id"]
                            or saved.get("source_manifest") != source):
                        raise ValueError("request_intake_identity_mismatch")
        except (OSError, ValueError, TypeError, KeyError) as exc:
            if partial_requests:
                automatic = dict(automatic, status="nonassociated", entries=[],
                    nonassociated_entries=automatic.get("entries", []), association_error=str(exc),
                    errors=[*automatic.get("errors", []), str(exc)])
            else:
                automatic = {"status": "nonassociated", "attachment_valid": False, "entries": [],
                    "reason_code": str(exc), "errors": [*automatic.get("errors", []), str(exc)]}
    data["report_evidence"]["automatic_replay"] = automatic
    query_errors.extend("automatic_replay:" + error for error in automatic.get("errors", []))
    data["query_valid"] = not query_errors
    data["named_gaps"] = query_errors
    for row in data["obligations"]:
        targets = {link["target_id"] for link in row["model_links"]}
        row["supporting_execution_refs"] = [item["cell_id"] for item in rows
                                              if item["target_id"] in targets]
        row["named_gaps"] = list(row["remaining_obligations"])
        related = [item for item in rows if item["target_id"] in targets]
        qualified = any(item["current_execution_qualified"] for item in related)
        row["capability_layers"] = {
            "input": {"status": "model_input_only" if targets else "unverified",
                      "target_ids": sorted(targets)},
            "related_execution": {"status": "partial_real_validation" if qualified else "unverified",
                                  "scope": "target_context_not_full_obligation_coverage"},
            "semantic_contract": {"status": "unverified",
                                  "reason": "obligation_specific_proof_missing"}}
    obligations = {row["id"]: row for row in data["obligations"]}
    for key, links in (("cases", "coverage_rows"), ("user_stories", "obligation_ids")):
        for row in data[key]:
            row["current_source_status"] = "unverified"
            row["named_gaps"] = ["obligation_specific_proof_missing:" + cid for cid in row[links]]
            row["supporting_execution_refs"] = sorted({ref for cid in row[links]
                for ref in obligations[cid]["supporting_execution_refs"]})
            row["supporting_evidence_refs"] = sorted({ref for cid in row[links]
                for ref in obligations[cid]["evidence_refs"]})
    cells = []
    for target, revision in data["required_baseline"]["target_revisions"].items():
        for cell in first_slice_cells(target):
            if cell["arm"] == "negative" or cell["control_kind"]:
                continue
            supporting = [row for row in rows if row["target_id"] == target
                          and row["cell_id"] == cell["cell_id"]]
            complete = (audit.get("sealed") is True and source_current and not query_errors
                        and len(supporting) == 1 and supporting[0]["current_execution_qualified"])
            cells.append(dict(cell, target_revision=revision,
                status="declared_contract_complete" if complete else "unverified",
                supporting_execution_refs=[row["cell_id"] for row in supporting],
                named_gaps=[] if complete else ["current_source_required_cell_not_qualified"]))
    data["required_baseline"]["cells"] = cells
    return data


def _s1_execution_context(root, row, report, audit, evidence, source_current):
    """Qualify related target execution, never the linked semantic obligations."""
    if row.get("arm") == "negative" or row.get("control_kind"):
        evidence["named_gaps"].append("control_not_semantic_coverage")
        return
    if (evidence["reference_errors"] or evidence["qualification"] in {"fixture", "tool_error"}
            or row.get("actual_run") is not True or row.get("execution_stage") != "judged"):
        return
    if not source_current:
        evidence["qualification"] = "historical_source"
        evidence["named_gaps"].append("source_not_current")
        return
    if audit["errors"]:
        evidence["named_gaps"].append("canonical_evidence_audit_failed")
        return
    if report.get("formal_runtime", report.get("runtime_formal")) is not True:
        evidence["named_gaps"].append("formal_runtime_not_qualified")
        return
    try:
        from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
        if row["target_id"] == TOPOLOGY_TARGET:
            proof = audit.get("topology_qualifications", {}).get(row["cell_id"])
        else:
            proof = _f1_adapter_artifact_qualification(root, row, report)
            if proof is None and audit.get("sealed") is True:
                from r2f.campaign.replay import _qualification, DEFAULT_RUNTIME, ISOLATED_RUNTIME
                config = json.loads(_path(root, row["runtime_config_path"]).read_text())
                result_path = _path(root, row["runtime_result_path"])
                result = json.loads(result_path.read_text())
                origin = ISOLATED_RUNTIME if report.get("runtime_boundary") == "isolated_netns" else DEFAULT_RUNTIME
                proof = _qualification(origin, config, result, row, output_root=result_path.parent)
        evidence["execution_qualification"] = proof
        if proof and proof.get("real_ros_execution") is True:
            evidence.update(current_execution_qualified=True, qualification="related_execution",
                            status="partial_real_validation")
            evidence["named_gaps"] = ["context_link_does_not_prove_semantic_obligation"]
        else:
            evidence["named_gaps"].extend((proof or {}).get("qualification_errors") or ["execution_qualification_missing"])
    except (OSError, ValueError, TypeError, KeyError) as exc:
        evidence["reference_errors"].append("execution_qualification:" + str(exc))
        evidence["named_gaps"].append("execution_qualification:" + str(exc))


def _s1_row_evidence(root, row, source, models):
    """Reopen advertised references even when a fixture cannot pass seal gates."""
    evidence = {key: deepcopy(row.get(key)) for key in (
        "cell_id", "target_id", "target_revision", "rmw", "arm", "repeat",
        "domain_id", "execution_stage", "actual_run", "rule_id",
        "effective_input_sha256", "raw_path", "raw_sha256", "replay_path",
        "precondition_receipts", "mutation_receipt", "typed_facts", "oracle_verdict",
        "runtime_status", "reason_code", "abort_reason", "observation_receipts")}
    errors = []
    evidence.update(reference_errors=errors, named_gaps=[], current_execution_qualified=False,
                    qualification="unverified", status="unverified")
    model = models.get(row.get("target_id"))
    if not model or row.get("target_revision") != model["target_revision"]:
        errors.append("target_or_revision_not_in_current_model")
    if model and row.get("rule_id") not in {None, *(ref["rule_id"] for ref in model["rule_lineage_refs"])}:
        errors.append("rule_not_in_current_model")
    evidence["raw"] = {"status": "not_observed"}
    if row.get("raw_path") is not None:
        try:
            raw = _path(root, row["raw_path"])
            verify_raw(raw, row.get("raw_sha256"))
            evidence["raw"] = {"status": "verified", "path": str(raw), "sha256": row["raw_sha256"]}
        except (OSError, ValueError, TypeError, KeyError) as exc:
            evidence["raw"] = {"status": "invalid", "reason": str(exc)}
            errors.append("raw:" + str(exc))
    evidence["replay"] = {"status": "not_observed", "attempt_status": row.get("replay_status", "unknown")}
    capsule_input = None
    if row.get("replay_path") is not None:
        try:
            capsule_path = _path(root, row["replay_path"])
            capsule = ReplayCapsule.from_dict(json.loads(capsule_path.read_text()))
            for key in ("target_id", "target_revision", "rmw", "arm", "repeat", "effective_input_sha256"):
                if capsule.input.get(key) != row.get(key):
                    raise ValueError("capsule_row_identity_mismatch:" + key)
            if capsule.input["source_manifest"] != source:
                raise ValueError("capsule_source_identity_mismatch")
            if model and canonical_hash(capsule.input["target"]) != model["manifest_sha256"]:
                raise ValueError("capsule_target_manifest_mismatch")
            evidence["replay"].update(status="verified_input_capsule", path=str(capsule_path),
                sha256=hashlib.sha256(capsule_path.read_bytes()).hexdigest(),
                execution_present=capsule.execution is not None,
                limitation="input capsule is not an independent replay attempt")
            capsule_input = capsule.input
        except (OSError, ValueError, TypeError, KeyError) as exc:
            evidence["replay"].update(status="invalid", reason=str(exc))
            errors.append("replay:" + str(exc))
    evidence["config"] = {"status": "not_observed"}
    if row.get("runtime_config_path") is not None:
        try:
            config_path = _path(root, row["runtime_config_path"])
            config = json.loads(config_path.read_text())
            if not isinstance(config, dict):
                raise ValueError("runtime config must be an object")
            for key in ("run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "domain_id",
                        "effective_input_sha256"):
                # Exploratory input-only rows need not carry execution domain.
                if key == "domain_id" and key not in row and row.get("actual_run") is not True:
                    continue
                if config.get(key) != row.get(key):
                    raise ValueError("config_row_identity_mismatch:" + key)
            if config.get("source_manifest") != source:
                raise ValueError("config_source_identity_mismatch")
            if capsule_input is None:
                raise ValueError("config_input_capsule_unverified")
            # The validated persisted capsule binds its digest to actual input;
            # compare saved config values without regenerating a plan or input.
            effective = capsule_input["effective_input"]
            for key in ("final_input", "input_layer_receipts"):
                if canonical_hash(config.get(key)) != canonical_hash(effective[key]):
                    raise ValueError("config_capsule_input_mismatch:" + key)
            if ("effective_input" in config
                    and canonical_hash(config["effective_input"]) != canonical_hash(effective)):
                raise ValueError("config_capsule_input_mismatch:effective_input")
            for key, value in effective["final_input"].items():
                if canonical_hash(config.get(key)) != canonical_hash(value):
                    raise ValueError("config_submitted_input_mismatch:" + key)
            evidence["config"] = {"status": "verified", "path": str(config_path),
                                  "sha256": hashlib.sha256(config_path.read_bytes()).hexdigest()}
        except (OSError, ValueError, TypeError, KeyError) as exc:
            evidence["config"] = {"status": "invalid", "reason": str(exc)}
            errors.append("config:" + str(exc))
    runtime = {}
    if row.get("runtime_result_path") and row.get("execution_stage") != "planned":
        try:
            loaded = json.loads(_path(root, row["runtime_result_path"]).read_text())
            if not isinstance(loaded, dict):
                raise ValueError("runtime result must be an object")
            runtime = loaded
            if row.get("raw_path") and (runtime.get("raw_path") != row["raw_path"]
                                       or runtime.get("raw_sha256") != row.get("raw_sha256")):
                raise ValueError("runtime_raw_binding_mismatch")
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors.append("runtime:" + str(exc))
    origin = runtime.get("evidence_origin", row.get("evidence_origin"))
    evidence["evidence_origin"] = origin
    if row.get("tool_error") is True or row.get("runtime_status") == "tool_error" or runtime.get("runtime_status") == "tool_error":
        evidence["qualification"] = "tool_error"
        evidence["named_gaps"].append("tool_error_not_sut_observation")
    elif origin in {"fixture", "synthetic", "offline", "injected_runtime"}:
        evidence.update(qualification="fixture", status="wired_pending_real_validation")
        evidence["named_gaps"].append("offline_fixture_not_real_execution")
    elif row.get("execution_stage") == "planned" and not row.get("actual_run"):
        evidence.update(qualification="input_only", status="model_input_only")
        evidence["named_gaps"].append("execution_not_run")
    else:
        evidence["named_gaps"].append("execution_not_qualified")
    evidence["named_gaps"].extend(errors)
    return evidence


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


def _verify_topology_binding(config, plan, row, report):
    """Recompute the runner's topology binding, not the F1 projection receipt.

    Topology normalization is validate-and-copy. Only the two execution-copy
    fields added AFTER binding by the canonical runner are outside its digest.
    Their exact shape/intent is checked here; boundary/raw qualification remains
    independent. Target identity, not an optional marker, selects this contract.
    """
    profile = report.get("profile", {})
    for key in ("observation_mode", "fastdds_peer_instrumentation", "observe_native_peers"):
        if key in config or key in row or key in profile:
            if (canonical_hash(config.get(key)) != canonical_hash(row.get(key))
                    or canonical_hash(config.get(key)) != canonical_hash(profile.get(key))):
                raise ValueError("topology_observation_selection_mismatch:" + key)
    binding = row.get("binding")
    if not isinstance(binding, dict):
        raise ValueError("topology_binding_missing")
    if (binding.get("status") != "preserved"
            or binding.get("silent_normalization") is not False
            or binding.get("numeric_widening_fields") != []):
        raise ValueError("topology_binding_not_preserved")
    normalized = dict(config)
    boundary = report.get("runtime_boundary", "host")
    if boundary == "isolated_netns":
        if normalized.pop("execution_boundary", None) != {"enabled": True}:
            raise ValueError("topology_binding_execution_boundary_mismatch")
    elif boundary != "host" or "execution_boundary" in normalized:
        raise ValueError("topology_binding_execution_boundary_mismatch")
    if "live_execution_go" in normalized:
        if (normalized.pop("live_execution_go") is not True
                or report.get("formal_runtime", report.get("runtime_formal")) is not True):
            raise ValueError("topology_binding_execution_intent_mismatch")
    for key, value in plan.final_config.items():
        if canonical_hash(normalized.get(key)) != canonical_hash(value):
            raise ValueError("topology_binding_normalization_changed:" + key)
    if (canonical_hash(config.get("final_input")) != canonical_hash(plan.final_config)
            or canonical_hash(row.get("final_input")) != canonical_hash(plan.final_config)):
        raise ValueError("topology_binding_final_input_mismatch")
    for key, expected in (("submitted_config_sha256", canonical_hash(plan.final_config)),
                          ("normalized_config_sha256", canonical_hash(normalized))):
        if _sha_or_none(binding.get(key)) is None or binding[key] != expected:
            raise ValueError("topology_binding_hash_mismatch:" + key)


def _verify_topology_generation_rejection(root, row, report):
    """Authenticate an unsubmitted model rejection, never downgrade an attempt.

    The existing checkpoint supplies a profile/source-bound, replayable input
    search journal. Verify just this task and its parent generation ancestry,
    not every frontier task. No runtime callable is invoked; missing runtime
    evidence alone is NOT rejection proof.
    """
    from r2f.campaign.topology_search import generate, task_requests
    if (row.get("actual_run") is not False or row.get("mutation_plan") is not None
            or row.get("execution_stage") != "planned"
            or row.get("oracle_verdict") != "not_applicable"
            or row.get("arm") != "mutated" or row.get("control_kind") is not None
            or row.get("attribution") != "harness_input_generation"
            or row.get("input_layer_receipts") != []
            or not isinstance(row.get("reason_code"), str)
            or not row["reason_code"].startswith("topology_input_rejected:")):
        raise ValueError("topology_generation_rejection_claim_invalid")
    # Fail closed even for all-false synthetic receipts or dead/missing paths:
    # these fields belong to a prepared/submitted case, not pre-plan rejection.
    forbidden = ("raw_path", "raw_sha256", "replay_path", "runtime_config_path",
                 "runtime_result_path", "runtime_status", "runtime_reason",
                 "runtime_execution_metadata", "execution_metadata", "execution",
                 "execution_qualification", "boundary_qualification", "mutation_receipt",
                 "precondition_receipts", "observation_receipts", "typed_facts", "oracle",
                 "final_input", "effective_input", "effective_input_sha256",
                 "mutation_plan_sha256", "content_sha256")
    if any(key in row for key in forbidden):
        raise ValueError("topology_generation_rejection_has_execution_evidence")
    for key in ("candidate_eligible", "feedback_eligible", "seal_eligible", "boundary_eligible",
                "workspace_executed", "real_ros_execution", "real_runtime_attempt",
                "effect_observed", "judged", "adapter_accepted", "observer_complete"):
        if key in row and row[key] is not False:
            raise ValueError("topology_generation_rejection_claims_eligibility:" + key)
    allowed = {"cell_id", "run_id", "target_id", "target_revision", "profile_id", "rmw",
               "campaign_kind", "arm", "execution_stage", "actual_run", "oracle_verdict",
               "control_kind", "mutation_plan", "input_layer_receipts", "binding",
               "generation_status", "reason_code", "topology_generation_attempts",
               "candidate_eligible", "feedback_eligible", "seal_eligible", "boundary_eligible",
               "workspace_executed", "real_ros_execution", "real_runtime_attempt",
               "effect_observed", "judged", "adapter_accepted", "observer_complete",
               "attribution", "input_search", "topology_chain_accounting"}
    if set(row) - allowed:
        raise ValueError("topology_generation_rejection_unrecognized_claim")
    binding = row.get("binding")
    if binding != {"status": "not_executed", "silent_normalization": False,
                   "detail": row["reason_code"]}:
        raise ValueError("topology_generation_rejection_binding_invalid")
    cell = row["cell_id"]
    if Path(cell).name != cell or "\\" in cell or cell in {".", ".."}:
        raise ValueError("topology_cell_identity_not_path_component")
    case = Path(root) / "cases" / cell
    # The runner persists row.json even for a generation stub, but never a
    # capsule/config/submission/runtime directory. Do not infer nonexecution
    # from a missing runtime result on an otherwise prepared case.
    if case.exists():
        if case.is_symlink() or {p.name for p in case.iterdir()} != {"row.json"}:
            raise ValueError("topology_generation_rejection_has_case_artifacts")
        saved_row = json.loads(_path(root, str(case / "row.json")).read_text())
        if canonical_hash(saved_row) != canonical_hash(row):
            raise ValueError("topology_generation_rejection_saved_row_mismatch")
    task, parent, history, desc = _topology_generation_provenance(root, row, report)
    if [e["action"] for e in history] != ["select", "generation_rejected", "finish"]:
        raise ValueError("topology_rejection_has_submission_or_plan_history")
    try:
        generate(parent, task, desc)
    except ValueError as exc:
        detail = str(exc)
        if task.get("chain_schema") and task.get("chain_accounting") != getattr(exc, "accounting", None):
            raise ValueError("topology_rejection_chain_accounting_not_model_generatable") from exc
    else:
        raise ValueError("topology_rejection_is_model_generatable")
    if (row["reason_code"] != "topology_input_rejected:" + detail
            or history[1].get("reason") != detail or history[1].get("attempts") != task_requests(task)
            or history[2].get("reason") != row["reason_code"]
            or history[2].get("rejected") is not True or history[2].get("actual") is not False
            or history[2].get("execution") is not None
            or task.get("rejection") != {"reason": detail, "attempts": task_requests(task)}):
        raise ValueError("topology_rejection_model_reason_mismatch")
    if (task.get("generation_outcome") != "generation_rejected"
            or task.get("rejection", {}).get("attempts") != row.get("topology_generation_attempts")):
        raise ValueError("topology_rejection_model_provenance_mismatch")


def _topology_generation_provenance(root, row, report, *, prepared_duplicate=False):
    """Shared source/profile/task/parent proof, never execution qualification."""
    from r2f.campaign.checkpoint import _identity, SCHEMA
    from r2f.campaign.search_frontier import search_settings, derived_record
    from r2f.campaign.topology_search import descriptor, selection_metadata, validate_task, generate
    cell = row["cell_id"]
    checkpoint = json.loads(_path(root, "checkpoint.json").read_text())
    if (not isinstance(checkpoint, dict) or checkpoint.get("schema") != SCHEMA or checkpoint.get("checkpoint_sha256") !=
            canonical_hash({k: v for k, v in checkpoint.items() if k != "checkpoint_sha256"})):
        raise ValueError("topology_rejection_checkpoint_hash_mismatch")
    saved_identity = checkpoint["identity"]
    identity, seeds, registry = _identity(report["profile"], saved_identity["cycle_id"],
                                         report["source_manifest"])
    if canonical_hash(saved_identity) != canonical_hash(identity):
        raise ValueError("topology_rejection_checkpoint_identity_mismatch")
    state = checkpoint["state"]
    if state["continuation"].get("run_id") != row["run_id"]:
        raise ValueError("topology_rejection_run_identity_mismatch")
    frontier = state["scheduler"]["frontier"]
    if not isinstance(frontier, dict):
        raise ValueError("topology_rejection_frontier_not_object")
    settings = search_settings(report["profile"]["search"])
    desc = descriptor(report["profile"]["topology_search"], report["profile"]["operator_whitelist"])
    roots = {r["content_sha256"]: r for r in seeds.selectable()}
    if (frontier.get("settings") != settings or frontier.get("topology_descriptor") != desc
            or frontier.get("roots") != sorted(roots.values(), key=lambda r: r["content_sha256"])
            or type(frontier.get("rng_seed")) is not int or frontier["rng_seed"] < 0):
        raise ValueError("topology_rejection_frontier_identity_mismatch")
    tasks, events, plans = {}, {}, {}
    for item in frontier["tasks"]:
        if not isinstance(item, dict) or item["task_id"] in tasks:
            raise ValueError("topology_rejection_task_identity_invalid")
        tasks[item["task_id"]] = item
    for event in frontier["journal"]:
        if not isinstance(event, dict):
            raise ValueError("topology_rejection_journal_invalid")
        events.setdefault(event["task_id"], []).append(event)
        if event["action"] == "plan":
            plans.setdefault(event["record"]["content_sha256"], []).append(event)
    # Rebuild only the generation ancestry needed for this rejection. This is
    # input proof, not execution evidence and not an alternate frontier engine.
    visiting, verified = set(), {}

    def checked_task(task):
        parent = checked_parent(task["parent_content_sha256"])
        validate_task(task, parent, desc)
        key = dict(selection_metadata(task), **{k: task[k] for k in
                   ("baseline_content_sha256", "parent_content_sha256", "sample_slot", "derivation_depth")})
        depth = frontier["nodes"][task["parent_content_sha256"]]["derivation_depth"] + 1
        if (task["task_id"] != canonical_hash(key) or type(task["derivation_depth"]) is not int
                or task["derivation_depth"] != depth
                or not 1 <= depth <= settings["max_depth"]
                or type(task["sample_slot"]) is not int
                or not 0 <= task["sample_slot"] < settings["samples_per_class"]
                or task["baseline_content_sha256"] != frontier["nodes"][task["parent_content_sha256"]]["baseline_content_sha256"]
                or task["rng_seed"] != int(canonical_hash({"mode": "input_search_bfs",
                    "rng_seed": frontier["rng_seed"], "task_id": task["task_id"]})[:16], 16)):
            raise ValueError("topology_rejection_task_hash_mismatch")
        return parent

    def checked_parent(digest):
        if digest in verified:
            return verified[digest]
        if digest in visiting or len(visiting) >= settings["max_depth"]:
            raise ValueError("topology_rejection_parent_cycle_or_depth")
        visiting.add(digest)
        node = frontier["nodes"][digest]
        if digest in roots:
            expected = roots[digest]
            if node["derivation_depth"] != 0 or node["baseline_content_sha256"] != digest:
                raise ValueError("topology_rejection_root_identity_mismatch")
        else:
            proofs = plans.get(digest, [])
            if len(proofs) != 1:
                raise ValueError("topology_rejection_parent_generation_proof_missing")
            proof = proofs[0]
            prior = tasks[proof["task_id"]]
            prior_parent = checked_task(prior)
            expected = derived_record(prior_parent, generate(prior_parent, prior, desc))
            if canonical_hash(proof["record"]) != canonical_hash(expected):
                raise ValueError("topology_rejection_parent_generation_mismatch")
        if canonical_hash(node["entry"]) != canonical_hash(expected):
            raise ValueError("topology_rejection_parent_identity_mismatch")
        visiting.remove(digest)
        verified[digest] = expected
        return expected

    search = row.get("input_search")
    if not isinstance(search, dict):
        raise ValueError("topology_rejection_input_search_missing")
    task = tasks[search["task_id"]]
    parent = checked_task(task)
    history = events.get(task["task_id"], [])
    if canonical_hash(row.get("topology_chain_accounting")) != canonical_hash(task.get("chain_accounting")):
        raise ValueError("topology_rejection_chain_accounting_mismatch")
    for key in ("target_id", "target_revision", "rmw"):
        if row[key] != parent[key]:
            raise ValueError("topology_rejection_parent_identity_mismatch:" + key)
    if row["profile_id"] != report["profile"]["profile_id"]:
        raise ValueError("topology_rejection_profile_mismatch")
    expected = dict(selection_metadata(task), mode="input_search_bfs", task_id=task["task_id"],
                    **{k: task[k] for k in ("baseline_content_sha256", "parent_content_sha256",
                                           "derivation_depth", "sample_slot", "rng_seed")},
                    status="rejected", actual=False, execution=None)
    if canonical_hash(search) != canonical_hash(expected):
        raise ValueError("topology_rejection_input_search_mismatch")
    # Submission/plan history is stronger evidence of an attempted case than
    # missing/deleted runtime artifacts or a forged rejected task label. Keep
    # this specific laundering denial ahead of rejection-only status checks.
    if (row.get("attribution") == "harness_input_generation"
            and [e["action"] for e in history] != ["select", "generation_rejected", "finish"]):
        raise ValueError("topology_rejection_has_submission_or_plan_history")
    selections = [s for s in state["scheduler"]["selections"] if s.get("task_id") == task["task_id"]]
    suffix = parent["surface_id"] if prepared_duplicate else "rejected"
    if (len(selections) != 1 or cell != f"{row['run_id']}.{selections[0]['sequence']:04d}.{suffix}"
            or task.get("status") != "rejected" or task.get("actual") is not False
            or task.get("execution") is not None or task.get("reason") != row["reason_code"]):
        raise ValueError("topology_rejection_model_provenance_mismatch")

    return task, parent, history, desc


def _verify_topology_model_terminal(root, row, report):
    """Only the runner's unprepared generated-plan terminal shape is admitted.

    Prepared duplicate_content/model_* rows carry config/capsule and are NOT
    generation-guard stubs. They require their own prepared-artifact contract.
    """
    from r2f.campaign.topology_search import generate
    from r2f.campaign.search_frontier import derived_record
    from r2f.core.mutation_plan import mutation_plan_noop_reason
    allowed = {"cell_id", "run_id", "target_id", "target_revision", "profile_id", "rmw",
               "campaign_kind", "arm", "execution_stage", "actual_run", "oracle_verdict",
               "control_kind", "mutation_plan", "input_layer_receipts", "binding",
               "generation_status", "reason_code", "content_sha256", "parent_content_sha256",
               "no_effective_input_reason", "candidate_eligible", "feedback_eligible",
               "input_search", "topology_chain_accounting"}
    if (set(row) - allowed or row.get("actual_run") is not False
            or row.get("execution_stage") != "planned" or row.get("arm") != "mutated"
            or row.get("control_kind") is not None or row.get("oracle_verdict") != "not_applicable"
            or row.get("candidate_eligible") is not False or row.get("feedback_eligible") is not False
            or row.get("input_layer_receipts") != [] or not isinstance(row.get("mutation_plan"), dict)
            or row.get("reason_code") not in {"model_guard_terminal", "no_effective_input"}):
        raise ValueError("topology_model_terminal_shape_invalid")
    if row.get("binding") != {"status": "not_executed", "silent_normalization": False,
                              "detail": row["reason_code"]}:
        raise ValueError("topology_model_terminal_binding_invalid")
    cell = row["cell_id"]
    if Path(cell).name != cell or "\\" in cell or cell in {".", ".."}:
        raise ValueError("topology_cell_identity_not_path_component")
    case = Path(root) / "cases" / cell
    if (case.is_symlink() or not case.is_dir()
            or {p.name for p in case.iterdir()} != {"row.json", "mutation_plan.json"}):
        raise ValueError("topology_model_terminal_has_prepared_or_execution_artifacts")
    if canonical_hash(json.loads(_path(root, str(case / "row.json")).read_text())) != canonical_hash(row):
        raise ValueError("topology_model_terminal_saved_row_mismatch")
    task, parent, history, desc = _topology_generation_provenance(root, row, report)
    if [e["action"] for e in history] != ["select", "plan", "finish"]:
        raise ValueError("topology_model_terminal_has_submission_history")
    plan = generate(parent, task, desc)
    record = derived_record(parent, plan)
    noop = mutation_plan_noop_reason(plan)
    compile_noop = bool(record["operations"]) and all(
        op["application_phase"] == "compile_time" and op["input_before_sha256"] == op["input_after_sha256"]
        for op in record["operations"])
    if record.get("executable") is False:
        outcome, reason, detail = "guard_terminal", "model_guard_terminal", "topology_guard_terminal"
    elif noop is not None:
        outcome = ("compile_time_no_change" if compile_noop and
                   record["runtime_windows"] == parent.get("runtime_windows", []) else "no_effective_input")
        reason, detail = "no_effective_input", noop
    else:
        raise ValueError("topology_model_terminal_not_generated_guard_or_noop")
    saved_plan = json.loads(_path(root, str(case / "mutation_plan.json")).read_text())
    if (row["reason_code"] != reason or row.get("no_effective_input_reason") != detail
            or task.get("generation_outcome") != outcome
            or canonical_hash(saved_plan) != canonical_hash(plan.to_dict())
            or canonical_hash(row["mutation_plan"]) != canonical_hash(plan.to_dict())
            or canonical_hash(task.get("generated_record")) != canonical_hash(record)
            or canonical_hash(history[1].get("record")) != canonical_hash(record)
            or row.get("content_sha256") != record["content_sha256"]
            or row.get("parent_content_sha256") != parent["content_sha256"]
            or history[2].get("rejected") is not True or history[2].get("actual") is not False
            or history[2].get("execution") is not None or history[2].get("reason") != reason):
        raise ValueError("topology_model_terminal_regeneration_mismatch")



def _verify_topology_prepared_duplicate(root, row, report):
    """A prepared duplicate is neither an unprepared guard nor an execution."""
    from r2f.campaign.checkpoint import _identity, _validate_state
    from r2f.campaign.topology_search import generate
    from r2f.campaign.search_frontier import derived_record
    from r2f.core.replay import ReplayCapsule
    case = Path(root) / 'cases' / row['cell_id']
    if (case.is_symlink() or not case.is_dir() or {p.name for p in case.iterdir()} != {
            'row.json', 'mutation_plan.json', 'runtime_config.json', 'capsule.json'}):
        raise ValueError('topology_prepared_duplicate_has_execution_artifacts')
    if (row.get('generation_status') != 'rejected' or row.get('reason_code') != 'model_duplicate_content'
            or row.get('blocked_reason') != 'model_duplicate_content'
            or row.get('execution_stage') != 'planned' or row.get('actual_run') is not False
            or row.get('oracle_verdict') != 'not_applicable' or row.get('arm') != 'mutated'
            or row.get('control_kind') is not None
            or any(row.get(k) is not False for k in ('candidate_eligible', 'feedback_eligible', 'seal_eligible'))
            or row.get('mutation_receipt') != dict.fromkeys((
                'planned', 'submitted', 'carrier_admitted', 'configured', 'applied', 'hit', 'effect_observed'), False)
            or any(row.get(k) is not None for k in ('raw_path', 'raw_sha256', 'typed_facts', 'oracle',
                'runtime_execution_metadata', 'execution_metadata', 'execution', 'input_terminal'))):
        raise ValueError('topology_prepared_duplicate_execution_claim_invalid')
    saved = json.loads(_path(root, str(case / 'row.json')).read_text())
    if canonical_hash(saved) != canonical_hash(row):
        raise ValueError('topology_prepared_duplicate_saved_row_mismatch')
    checkpoint = json.loads(_path(root, 'checkpoint.json').read_text())
    identity, seeds, registry = _identity(report['profile'], checkpoint['identity']['cycle_id'],
                                         report['source_manifest'])
    _validate_state(checkpoint['state'], identity, seeds, registry)
    task, parent, history, desc = _topology_generation_provenance(root, row, report, prepared_duplicate=True)
    if ([e['action'] for e in history] != ['select', 'plan', 'finish']
            or task.get('generation_outcome') != 'duplicate_content'):
        raise ValueError('topology_prepared_duplicate_model_history_invalid')
    plan = generate(parent, task, desc)
    record = derived_record(parent, plan)
    if (canonical_hash(task.get('generated_record')) != canonical_hash(record)
            or canonical_hash(row.get('mutation_plan')) != canonical_hash(plan.to_dict())
            or row.get('mutation_plan_sha256') != canonical_hash(plan.to_dict())
            or row.get('content_sha256') != record['content_sha256']
            or row.get('parent_content_sha256') != parent['content_sha256']):
        raise ValueError('topology_prepared_duplicate_model_regeneration_mismatch')
    config = json.loads(_path(root, str(case / 'runtime_config.json')).read_text())
    capsule = ReplayCapsule.from_dict(json.loads(_path(root, str(case / 'capsule.json')).read_text()))
    if (capsule.execution is not None or capsule.execution_history
            or config.get('source_manifest') != report['source_manifest']
            or row.get('source_manifest', report['source_manifest']) != report['source_manifest']):
        raise ValueError('topology_prepared_duplicate_capsule_source_or_execution_invalid')
    qualification = _topology_artifact_qualification(root, row, report)
    if qualification.get('real_ros_execution') is not False:
        raise ValueError('topology_prepared_duplicate_cannot_be_execution')


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
    _verify_topology_binding(config, plan, row, report)
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
                    "raw_path", "raw_sha256", "runtime_status", "runtime_reason", "runtime_execution_metadata",
                    "input_terminal", "diagnostic_oracle"):
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
        if "input_terminal" in row:
            # Compare the complete regenerated terminal; never accept a label.
            topology_nohit_continuation(config, result, row, qualified)
            qualified["continuation_eligible"] = True
    if not qualified["real_ros_execution"]:
        if any(row.get(key) is True for key in ("candidate_eligible", "feedback_eligible", "seal_eligible")):
            raise ValueError("topology_unqualified_execution_claims_eligibility")
        if ((row.get("execution_stage") == "judged" or row.get("actual_run") is True)
                and qualified.get("continuation_eligible") is not True):
            raise ValueError("topology_execution_not_qualified:" + ",".join(qualified["qualification_errors"]))
    return qualified


def _f1_adapter_artifact_qualification(root, row, report):
    """Strict additive F1 receipt check; never trust saved qualification labels.

    All paths retain the existing caller-owned report-root containment gate.
    This does not grant an external historical report a path exemption.
    """
    from r2f.campaign.runner import _F1_CYCLE_TARGETS
    binding = row.get("binding")
    model = report.get("model")
    targets = model.get("targets") if isinstance(model, dict) else {}
    marked = ("generation_binding" in row
              or isinstance(binding, dict) and binding.get("schema") == "r2f.f1-adapter-binding/v1"
              or isinstance(targets, dict) and row.get("target_id") in targets)
    observed = (row.get("actual_run") is True
                or row.get("execution_stage") in {"effect_observed", "judged"})
    if row.get("target_id") not in _F1_CYCLE_TARGETS or not (marked or observed):
        return None  # Legacy unexecuted plans do not grant live/candidate status.
    from r2f.reports.exploratory_builder import qualify_f1_artifacts
    if "generation_binding" not in row:
        raise ValueError("f1_generation_binding_missing")
    proof = qualify_f1_artifacts(root, row, report)
    saved = (report.get("artifact_qualifications") or {}).get(row.get("cell_id"))
    if canonical_hash(saved) != canonical_hash(proof):
        raise ValueError("f1_reopened_artifact_proof_mismatch")
    qualified = proof["qualification"]
    if not qualified["real_ros_execution"]:
        if any(row.get(key) is True for key in ("candidate_eligible", "feedback_eligible", "seal_eligible")):
            raise ValueError("f1_unqualified_execution_claims_eligibility")
        if row.get("actual_run") is True or row.get("execution_stage") in {"effect_observed", "judged"}:
            raise ValueError("f1_execution_not_qualified:" + ",".join(qualified["qualification_errors"]))
    return qualified


class _TopologyQualificationReuse:
    """One exploratory audit, successful topology proofs only.

    This deliberately snapshots the entire *case*, not a hand-maintained list
    of raw/worker/boundary filenames. Successful canonical qualification pins
    their file reads under that case; source reads are checked separately via
    the original double-content/HEAD source_manifest gate. No process-global
    cache, runtime summary trust, or mtime-based validity is involved.
    """

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.entries = {}

    def _snapshot(self, case):
        if case.is_symlink() or case.resolve(strict=True) != case or not case.is_dir():
            raise ValueError("topology_reuse_case_not_canonical")
        contents = {}
        for path in sorted(case.rglob("*")):
            # Symlinks/special files are not an enumerable dependency closure.
            # First use falls back to the original full public audit.
            if path.is_symlink():
                raise ValueError("topology_reuse_symlink")
            if path.is_dir():
                contents[str(path.relative_to(case))] = None
            elif path.is_file() and path.resolve(strict=True).is_relative_to(case):
                contents[str(path.relative_to(case))] = hashlib.sha256(path.read_bytes()).hexdigest()
            else:
                raise ValueError("topology_reuse_special_file")
        return contents

    def _check(self, entry):
        case, contents, source, _, row, report, identity = entry
        if (canonical_hash(report), canonical_hash(row)) != identity:
            raise ValueError("topology_reuse_input_changed")
        if self._snapshot(case) != contents:
            raise ValueError("topology_reuse_artifact_changed:" + str(case))
        if canonical_hash(source_manifest(Path(source["source_repo"]))) != canonical_hash(source):
            raise ValueError("topology_reuse_source_changed")

    def qualify(self, root, row, report):
        # Rejected/planned/unknown/terminal cases retain their complete path.
        if row.get("actual_run") is not True or row.get("execution_stage") != "judged":
            return _topology_artifact_qualification(root, row, report)
        root = Path(root).resolve()
        cell = row.get("cell_id")
        source = report.get("source_manifest")
        if (root != self.root or not isinstance(cell, str) or cell in {"", ".", ".."}
                or Path(cell).name != cell or "\\" in cell
                or not isinstance(source, dict) or not source.get("source_repo")):
            return _topology_artifact_qualification(root, row, report)
        # Bind the entire caller-owned input, including formal/boundary mode,
        # rows, manifest identities and receipt declarations, not just cell_id.
        key = (canonical_hash(report), canonical_hash(row))
        if key in self.entries:
            entry = self.entries[key]
            self._check(entry)
            return deepcopy(entry[3])
        case = root / "cases" / cell
        try:
            contents = self._snapshot(case)
        except (OSError, ValueError, RuntimeError):
            return _topology_artifact_qualification(root, row, report)
        result = _topology_artifact_qualification(root, row, report)
        # Never cache unknowns, exceptions, no-hit terminals or failed proofs.
        if result.get("real_ros_execution") is True and result.get("qualification_errors") == []:
            entry = (case, contents, deepcopy(source), deepcopy(result), row, report, key)
            self._check(entry)  # Detect changes during the initial computation.
            self.entries[key] = entry
        return result

    def finish(self):
        # A ledger/coverage consumer may run after the last reuse. Recheck at
        # audit exit as well, even if no subsequent lookup would see a change.
        for entry in self.entries.values():
            self._check(entry)


def _verify_exploratory_row(root, row, report, errors, *, qualification_reuse=None):
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
        from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
        topology = row["target_id"] == TOPOLOGY_TARGET
        if topology and row.get("generation_status") == "rejected":
            if row.get("reason_code") == "model_duplicate_content":
                _verify_topology_prepared_duplicate(root, row, report)
            elif row.get("mutation_plan") is None:
                _verify_topology_generation_rejection(root, row, report)
            else:
                _verify_topology_model_terminal(root, row, report)
            return True
        from r2f.campaign.runner import G01_TARGET
        if row['target_id'] == G01_TARGET and row.get('mutation_plan') is None:
            # This exception is granted by reopened, regenerated artifacts, not
            # by planned/rejected labels. It confers no execution qualification.
            if _f1_adapter_artifact_qualification(root, row, report) is None:
                raise ValueError('f1_generation_rejection_proof_missing')
            return True
        qualification = boundary_qualification_for_row(root, row, report)
        if qualification["status"] not in {"not_applicable", "qualified", "terminal_unqualified"}:
            raise ValueError(qualification["reason_code"])
        if topology:
            if qualification_reuse is None:
                _topology_artifact_qualification(root, row, report)
            else:
                qualification_reuse.qualify(root, row, report)
        _f1_adapter_artifact_qualification(root, row, report)
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
        if executed and not topology and not binding_ok:
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


def _exploratory_cycle_counters(report, root):
    """Validate cumulative-minus-origin counters against model-valid snapshots.

    A cycle counter is not evidence by itself. Nonzero inherited counters need
    an independent origin copy and final checkpoint with an immutable prefix.
    """
    import math
    from r2f.campaign.checkpoint import SCHEMA, _identity, _validate_state
    keys = ('selected', 'planned', 'generated', 'attempted', 'actual',
            'executed', 'aborted', 'interrupted')
    cumulative, cycle = report['budget'], report['cycle_budget']
    if not isinstance(cycle, dict):
        raise ValueError('exploratory_cycle_budget_invalid')
    for values in (cumulative, cycle):
        for key in keys:
            if type(values.get(key)) is not int or values[key] < 0:
                raise ValueError('exploratory_cycle_budget_invalid:' + key)
        elapsed = values.get('elapsed_sec')
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError('exploratory_cycle_elapsed_invalid')
    inherited = {key: cumulative[key] - cycle[key] for key in keys}
    if any(value < 0 for value in inherited.values()):
        raise ValueError('exploratory_cycle_exceeds_cumulative_budget')
    reference = report.get('resume_origin')
    if reference is None:
        if any(inherited.values()) or cycle['elapsed_sec'] != cumulative['elapsed_sec']:
            raise ValueError('exploratory_resume_origin_missing')
        return cycle
    if root is None or not isinstance(reference, dict) or reference.get('path') != 'resume-origin-checkpoint.json':
        raise ValueError('exploratory_resume_origin_reference_invalid')
    root = Path(root).resolve()

    def reopen(name):
        data = json.loads(_path(root, name).read_text())
        if (data.get('schema') != SCHEMA or data.get('checkpoint_sha256') != canonical_hash({
                k: v for k, v in data.items() if k != 'checkpoint_sha256'})):
            raise ValueError('exploratory_resume_checkpoint_hash_invalid')
        seq, previous = data.get('snapshot_sequence'), data.get('previous_checkpoint_sha256')
        if (type(seq) is not int or seq < 0 or (seq == 0) != (previous is None)
                or previous is not None and not _sha_or_none(previous)):
            raise ValueError('exploratory_resume_checkpoint_chain_invalid')
        return data

    origin, final = reopen(reference['path']), reopen('checkpoint.json')
    if reference.get('checkpoint_sha256') != origin['checkpoint_sha256']:
        raise ValueError('exploratory_resume_origin_hash_mismatch')
    identity, seeds, registry = _identity(report['profile'], origin['identity']['cycle_id'],
                                         report['source_manifest'])
    for data in (origin, final):
        if data.get('identity') != identity:
            raise ValueError('exploratory_resume_source_profile_identity_mismatch')
        _validate_state(data['state'], identity, seeds, registry)
    before, after = origin['state'], final['state']
    initial = before['continuation']['budget']
    if any(initial.get(key, 0) != value for key, value in inherited.items()):
        raise ValueError('exploratory_resume_inherited_budget_mismatch')
    if after['continuation']['budget'] != cumulative:
        raise ValueError('exploratory_resume_final_budget_mismatch')
    expected_elapsed = cumulative['elapsed_sec'] - initial.get('elapsed_sec', 0.0)
    if not math.isclose(cycle['elapsed_sec'], expected_elapsed, rel_tol=1e-12, abs_tol=1e-9):
        raise ValueError('exploratory_resume_elapsed_mismatch')
    resumed_from = after['continuation'].get('resumed_from') or {}
    if resumed_from.get('checkpoint_sha256') != origin['checkpoint_sha256']:
        raise ValueError('exploratory_resume_final_origin_mismatch')
    for name, old, new in (
            ('selections', before['scheduler']['selections'], after['scheduler']['selections']),
            ('contexts', before['scheduler']['selection_contexts'], after['scheduler']['selection_contexts']),
            ('feedback', before['feedback']['events'], after['feedback']['events'])):
        if new[:len(old)] != old:
            raise ValueError('exploratory_resume_prefix_changed:' + name)
    if before['scheduler'].get('policy') == 'bfs':
        old = before['scheduler']['frontier']['journal']
        if after['scheduler']['frontier']['journal'][:len(old)] != old:
            raise ValueError('exploratory_resume_prefix_changed:frontier')
    # Bind this segment's rows to final selections; inherited observations must
    # not inflate local coverage. An unfinished prepared selection may recur.
    sequences = report.get('row_sequences')
    rows = report['rows']
    if not isinstance(sequences, list) or len(sequences) != len(rows):
        raise ValueError('exploratory_resume_row_sequences_missing')
    minimum = inherited['selected'] - (before['continuation'].get('pending_selection') is not None)
    seen = set()
    for index, (row, receipt) in enumerate(zip(rows, sequences)):
        sequence = receipt.get('selection_sequence')
        if (receipt.get('cycle_row_index') != index or receipt.get('cell_id') != row.get('cell_id')
                or type(sequence) is not int or sequence in seen
                or not minimum <= sequence < cumulative['selected']
                or not row['cell_id'].startswith(f"{report['run_id']}.{sequence:04d}.")):
            raise ValueError('exploratory_resume_row_sequence_mismatch')
        seen.add(sequence)
    return cycle


def _audit_exploratory_budget(report, rows, errors, *, root=None):
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
    local = counters
    if 'cycle_budget' in report:
        try:
            local = _exploratory_cycle_counters(report, root)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors.append('exploratory_cycle_budget_error:' + str(exc))
            local = counters
    elif report.get('resume_origin') is not None:
        errors.append('exploratory_cycle_budget_missing_for_resume')
    actual_executed = sum(1 for r in rows
                          if r.get("execution_stage") == "judged" and r.get("actual_run") is True)
    if local["executed"] != actual_executed:
        errors.append(f"exploratory_budget_executed_mismatch:{local['executed']}!={actual_executed}")
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


def _audit_exploratory_ledger(root, report, rows, errors, details, *, qualification_reuse=None,
                              human_authority=None):
    from r2f.reports.candidate_report import exploratory_candidate_ledger, audit_projection
    ledger_path = _path(root, report.get("candidate_ledger_path"))
    ledger = json.loads(ledger_path.read_text())
    if canonical_hash(ledger) != report.get("candidate_ledger_sha256"):
        errors.append("candidate_ledger_hash_mismatch")
    eligible_rows, exclusions = [], []
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
    for row in rows:
        if (row.get("target_id") == TOPOLOGY_TARGET and row.get("generation_status") == "rejected"
                and row.get("actual_run") is False and row.get("execution_stage") == "planned"):
            continue  # Already independently certified by the strict row verifier.
        if row.get("target_id") == TOPOLOGY_TARGET:
            try:
                qualified = (_topology_artifact_qualification(root, row, report)
                             if qualification_reuse is None else qualification_reuse.qualify(root, row, report))
            except (OSError, ValueError, KeyError, TypeError) as exc:
                qualified = {"real_ros_execution": False, "qualification_errors": [str(exc)]}
            if not qualified["real_ros_execution"]:
                exclusions.append({"cell_id": row.get("cell_id"), "actual_run": row.get("actual_run"),
                                   "execution_stage": row.get("execution_stage"), "raw_path": row.get("raw_path"),
                                   "raw_sha256": row.get("raw_sha256"),
                                   "qualification_errors": qualified["qualification_errors"]})
                continue
        if boundary_qualification_for_row(root, row, report)["status"] not in {"not_applicable", "qualified", "terminal_unqualified"}:
            continue
        _f1_adapter_artifact_qualification(root, row, report)
        eligible_rows.append(row)
    expected = exploratory_candidate_ledger(
        eligible_rows, reviews=report.get("review_records") or [],
        human_records=report.get("human_records") or [], human_authority=human_authority)
    # CP3 is derived from the complete source-bound review/human histories.
    # Legacy reports predate this optional projection; new analysis cannot shed
    # the accounting field to bypass independent reconstruction.
    accounting = expected["root_cause_accounting"]
    has_accounting_analysis = any(isinstance(record, dict) and any(
        key in record for key in ("root_cause", "tool_repair_handoff"))
        for record in report.get("review_records") or [])
    if ("root_cause_accounting" in ledger or has_accounting_analysis) and (
            canonical_hash(ledger.get("root_cause_accounting")) != canonical_hash(accounting)):
        errors.append("exploratory_ledger_mismatch:root_cause_accounting")
    details["root_cause_accounting"] = accounting
    for key in ("confirmed_root_causes", "independent_new_root_causes",
                "historical_root_causes", "unknown_novelty_root_causes",
                "unresolved_root_cause_cases"):
        details[key] = accounting[key]
    if ledger.get("execution_exclusions", []) != exclusions:
        errors.append("topology_ledger_execution_exclusions_mismatch")
    if Counter(c.get("candidate_id") for c in ledger.get("candidates", [])) != Counter(
            c.get("candidate_id") for c in expected["candidates"]):
        errors.append("candidate_ledger_dropped_or_invented_anomaly")
    human_schema = ledger.get("human_adjudication_schema")
    if human_schema is not None and human_schema != expected["human_adjudication_schema"]:
        errors.append("human_adjudication_schema_unknown")
    if human_schema is None and (report.get("human_records") or any(
            key in ledger for key in ("human_records", "human_errors", "human_confirmed_candidates"))
            or any(any(key in entry for key in ("original_result", "analysis", "human_adjudication"))
                   for key in ("candidates", "failures", "controls") for entry in ledger.get(key, []))):
        errors.append("human_adjudication_schema_missing")
    if human_schema is not None:
        for key in ("failures", "controls"):
            if ledger.get(key) != expected[key]:
                errors.append("exploratory_ledger_mismatch:" + key)
    # Accepted reviews include superseded history; rejection details must also
    # match the rebuilt projection, even when the outer hashes are consistent.
    for key in ("natural_candidate_count", "confirmed_candidates", "tool_error_count",
                "observer_incomplete_count", "control_count", "review_records", "review_errors"):
        if ledger.get(key) != expected.get(key):
            errors.append("exploratory_ledger_mismatch:" + key)
    for key, default in (("human_records", []), ("human_errors", []), ("human_confirmed_candidates", 0)):
        if ledger.get(key, default) != expected[key]:
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
        actual_projection = audit_projection(candidate)
        expected_projection = audit_projection(match) if match is not None else None
        if human_schema is None and not report.get("human_records") and expected_projection is not None:
            # CP1 reports predate these derived fields. Preserve their original
            # contract; a legacy confirmation still grants no human confirmation.
            for key in ("original_result", "analysis", "human_adjudication"):
                expected_projection.pop(key, None)
            if "run_id" not in actual_projection:
                expected_projection.pop("run_id", None)
            for review in expected_projection.get("reviews", []):
                review.pop("attribution", None)
                review.pop("evidence_sha256", None)
        if match is None or actual_projection != expected_projection:
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
    # Reuse the independently rebuilt CP1 projection, including binding/hash
    # rejection reasons and ID deduplication. Review acceptance remains separate
    # from report evidence validity and never grants candidate confirmation.
    details["rejected_reviews"] = [
        {"index": failure.get("index"), "candidate_id": failure["candidate_id"],
         "reason": failure["detail"]}
        for failure in expected["review_errors"]]
    details["accepted_reviews"] = len(expected["review_records"])
    details["accepted_human_records"] = len(expected["human_records"])
    details["rejected_human_records"] = expected["human_errors"]
    details["human_confirmed_candidates"] = expected["human_confirmed_candidates"]
    details["candidate_ledger"] = ledger_path.name


def audit_exploratory_report(report_path, *, report=None, human_authority=None):
    """Keep producer and standalone audit queries on the same CP2 projection."""
    path = Path(report_path).resolve()
    if report is None:
        try:
            report = json.loads(path.read_text())
        except (OSError, ValueError):
            report = {}
    if (isinstance(report, dict) and isinstance(report.get("rows"), list)
            and any(not isinstance(row, dict) for row in report["rows"])):
        result = {"sealed": False, "evidence_valid": False,
                  "errors": ["report_artifact_error:rows must contain objects"]}
    else:
        result = _audit_exploratory_report(path, report=report, human_authority=human_authority)
    return _with_s1_coverage(path, report if isinstance(report, dict) else {}, result)


def _audit_exploratory_report(report_path, *, report=None, human_authority=None):
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
        qualification_reuse = _TopologyQualificationReuse(root)
        valid = [r for r in rows if _verify_exploratory_row(
            root, r, report, errors, qualification_reuse=qualification_reuse)]
        details["rows_verified"] = len(valid)
        details["rows_total"] = len(rows)
        _audit_exploratory_budget(report, rows, errors, root=root)
        _audit_exploratory_controls(report, rows, errors)
        _audit_exploratory_ledger(root, report, rows, errors, details,
                                  qualification_reuse=qualification_reuse,
                                  human_authority=human_authority)
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
                    recomputed = qualification_reuse.qualify(root, row, report)
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    recomputed = {"real_ros_execution": False, "qualification_errors": [str(exc)]}
                details.setdefault("topology_qualifications", {})[row.get("cell_id", "<missing>")] = recomputed
                row = dict(row, execution_qualification=recomputed)
            from r2f.campaign.runner import G01_TARGET
            if row.get('target_id') == G01_TARGET and row.get('mutation_plan') is None:
                qualified_coverage_rows.append(row)
                continue  # Already fail-closed audited above; not execution coverage.
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
        qualification_reuse.finish()
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
