"""Registry-based finite campaign runner for declared normal ROS2 API checks."""
from pathlib import Path
import json
import re
import uuid

from r2f.core import Registry, compile_scenario
from r2f.core.evidence import canonical_hash
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import register
from r2f.modules.s1.targets import EXECUTION_ORDER
from r2f.reports.candidate_report import candidate_ledger
from r2f.reports.run_report import audit_report, identity_hashes, report_digest, write_json
from .identity import runtime_config, semantic_input, source_manifest
from .runtime_adapter import ingest_execution
from .scheduler import first_slice_cells, control_references
from .triage import attribute


def plan_campaign(output_dir, *, repo=None, run_id=None, domain_id=173, seed=0,
                  target_id=EXECUTION_ORDER[0], parameters=None):
    """Build input capsules offline; planning never implies reached or executed."""
    repo = Path(repo or Path(__file__).resolve().parents[3]).resolve()
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("campaign output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or "s1-" + uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", run_id):
        raise ValueError("invalid run_id")
    if target_id not in EXECUTION_ORDER:
        raise ValueError("target requires capability activation and a reviewed target-specific pipeline")
    registry = Registry()
    module = register(registry)
    target = registry.get_target(target_id)
    services = registry.get_services(target_id)
    scope = json.loads((repo / "config/s1_scope.json").read_text())
    manifest = {"schema_version": 1, "run_id": run_id, "target": target.to_dict(),
                "catalog": module.to_dict(), "scope": scope, "rule": target.rule_contract,
                "source_manifest": source_manifest(repo), "environment_mode": "ros_overlay",
                "execution_order": list(EXECUTION_ORDER), "requested_domain": domain_id,
                "maturity_denominator_changed": False, "applicable_rmws_requested": list(target.rmw_applicability)}
    identity = identity_hashes(manifest)
    write_json(output_dir / "manifest.json", manifest)
    rows = []
    for cell in first_slice_cells(target_id):
        cell = dict(cell, target_id=target_id)
        submitted = semantic_input(cell["arm"], parameters, target_id=target_id)
        recovery = target_id == EXECUTION_ORDER[2]
        projection = target_id == EXECUTION_ORDER[3]
        mutation_kind = "discovery_drop" if recovery else "endpoint_qos_variation" if projection else "late_joiner"
        carrier = "wire_proxy" if recovery else "ros2_endpoint"
        mutation_parameters = ({key: submitted[key] for key in ("drop_duration_sec", "recovery_timeout_sec", "counter_interval_sec")}
                               if recovery else {"baseline_qos":submitted["baseline_qos"], "submitted_qos":submitted["qos"]} if projection else {"reader_delay_sec": submitted["reader_delay_sec"]})
        scenario = compile_scenario(target, cell["rmw"], cell["arm"], cell["repeat"], seed,
                                    {"mutation_kind": mutation_kind, "carrier": carrier, "mutation_parameters": mutation_parameters})
        prepared = services.mutation_provider.prepare(scenario)
        if not services.carrier.accepts(scenario):
            raise ValueError("registered carrier does not accept planned scenario")
        layer_receipts = prepared["input_layer_receipts"]
        capsule = ReplayCapsule.from_scenario(scenario, source_manifest=manifest["source_manifest"],
                                             final_input=submitted,
                                             input_layer_receipts=layer_receipts,
                                             origin_domain=domain_id)
        config = runtime_config(submitted, run_id=run_id, campaign_id=run_id,
                                cell_id=cell["cell_id"], target_id=target_id,
                                target_revision=target.target_revision, campaign_kind=cell["campaign_kind"],
                                rmw=cell["rmw"], arm=cell["arm"], repeat=cell["repeat"], seed=seed,
                                mutation_kind=mutation_kind, carrier=carrier,
                                scenario=scenario.to_dict(), domain_id=domain_id,
                                environment_mode="ros_overlay", source_repo=str(repo),
                                expected_head=manifest["source_manifest"]["head"],
                                expected_target_revision=target.target_revision,
                                source_manifest=manifest["source_manifest"],
                                final_input=submitted, effective_input_sha256=capsule.input["effective_input_sha256"],
                                input_layer_receipts=capsule.input["effective_input"]["input_layer_receipts"],
                                control_kind=cell.get("control_kind"))
        directory = output_dir / "cells" / cell["cell_id"]
        replay_path = directory / "capsule.json"
        config_path = directory / "runtime_config.json"
        write_json(replay_path, capsule.to_dict())
        write_json(config_path, config)
        rows.append(dict(scenario.to_dict(), **cell, run_id=run_id, domain_id=domain_id,
                         identity=identity, effective_input_sha256=capsule.input["effective_input_sha256"],
                         execution_stage="planned", replay_path=str(replay_path), runtime_config_path=str(config_path),
                         runtime_result_path=str(directory / "runtime" / "runtime_result.json"),
                         expected_effect_set=target.expected_effect_set,
                         mutation_receipt=dict.fromkeys(("planned", "submitted", "carrier_admitted", "configured", "applied", "hit", "effect_observed"), False),
                         oracle_verdict="unknown", replay_status="not_run", candidate_status="none", actual_run=False))
    return {"schema_version": 1, "run_id": run_id, "manifest_path": str(output_dir / "manifest.json"),
            "identity": identity, "rows": rows, "control_references": control_references(rows),
            "required_cells": 18, "independent_fault_controls": 6, "full_scope_required_cells": 72,
            "scope_dispositions": module.dispositions, "maturity_denominator_changed": False,
            "target_id": target_id, "target_revision": target.target_revision,
            "execution_stage_mapping": {
                "planned": "offline scenario/input capsule only", "precondition_executed": "raw step before/after tokens mapped one-to-one into core receipts",
                "reached": "core preconditions evaluate true", "mutation_applied": "core strict receipt flags true; neutral arms skip this stage",
                "effect_observed": "raw typed facts and expected/residual effect", "judged": "typed primary oracle evaluated through core execution gate"}}


def _persist(output_dir, report):
    ledger = candidate_ledger(report["rows"])
    ledger_path = output_dir / "candidate_ledger.json"
    write_json(ledger_path, ledger)
    report.update(candidate_ledger_path=str(ledger_path), candidate_ledger_sha256=canonical_hash(ledger))
    report["report_sha256"] = report_digest(report)
    report_path = output_dir / "report.json"
    write_json(report_path, report)
    report["seal"] = audit_report(report_path)
    write_json(report_path, report)
    lines = [f"# Campaign {report['run_id']}", "", f"Target: {report['target_id']} revision {report['target_revision']}",
             f"Per-target seal: {report['seal']['sealed']}", "Full S1 required denominator: 72; this report does not establish full S1 maturity.",
             f"Natural candidates: {ledger['natural_candidate_count']}; confirmed: {ledger['confirmed_candidates']}", "",
             ledger.get("zero_candidate_explanation") or "Candidate dispositions are retained in candidate_ledger.json.", "",
             "| Cell | Stage | Primary verdict | Reason |", "|---|---|---|---|"]
    for row in report["rows"]:
        lines.append(f"| {row['cell_id']} | {row['execution_stage']} | {row['oracle_verdict']} | {row.get('abort_reason') or row.get('reason_code', '')} |")
    lines += ["", "Seal errors:"] + [f"- {error}" for error in report["seal"]["errors"]]
    (output_dir / "report.md").write_text("\n".join(lines) + "\n")
    return report


def run_campaign(output_dir, *, runtime=None, execute=False, **options):
    """Sequential cells, never parallel campaigns. Default is offline plan only.

    The owner coordinates actual execution; execute=True is an explicit action.
    An injected runner is for contract testing and is not proof of ROS execution.
    """
    output_dir = Path(output_dir).resolve()
    report = plan_campaign(output_dir, **options)
    if not execute:
        return _persist(output_dir, report)
    if runtime is None:
        from r2f.runtime.ros2_pair import run_pair
        runtime = run_pair
    manifest = json.loads(Path(report["manifest_path"]).read_text())
    repo = Path(manifest["source_manifest"]["source_repo"])
    if runtime.__module__ == "r2f.runtime.ros2_pair" and manifest["source_manifest"].get("source_frozen") is not True:
        report["abort_reason"] = "source_behavior_not_tracked_and_clean_at_HEAD"
        return _persist(output_dir, report)
    registry = Registry()
    register(registry)
    services = registry.get_services(report["target_id"])
    for index, planned in enumerate(report["rows"]):
        if source_manifest(repo) != manifest["source_manifest"]:
            report["abort_reason"] = "source_repo_HEAD_or_behavior_tree_changed"
            break
        config = json.loads(Path(planned["runtime_config_path"]).read_text())
        capsule = ReplayCapsule.from_dict(json.loads(Path(planned["replay_path"]).read_text()))
        from r2f.core.scenario import Scenario
        scenario = Scenario.from_dict(capsule.input["scenario"])
        runtime_dir = Path(planned["runtime_result_path"]).parent
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            if not services.carrier.accepts(scenario):
                raise ValueError("registered carrier does not accept execution scenario")
            result = services.carrier.apply(scenario, capsule.input["effective_input"],
                                            execution={"config": config, "output_dir": runtime_dir, "runtime": runtime})
        except Exception as exc:
            result = {"real_execution": False, "runtime_status": "tool_error", "reason": f"{type(exc).__name__}:{exc}"}
        # Preserve the result, including failures. run_pair already writes it; this
        # also gives injected contract runners the same artifact boundary.
        write_json(planned["runtime_result_path"], result)
        execution = ingest_execution(scenario, config, result, capsule, services=services)
        row = dict(planned, **execution)
        row["actual_submitted_payload_sha256"] = canonical_hash(capsule.input["effective_input"]["final_input"])
        row["replay_status"] = "not_run"
        row["provenance"] = {"source_manifest": manifest["source_manifest"], "runtime_execution": row.get("runtime_execution_metadata"), "raw_sha256": row.get("raw_sha256"), "observation_path": "isolated_discovery_activity_and_public_pair" if report["target_id"] == EXECUTION_ORDER[2] else "independent_passive_sedp_and_public_graph" if report["target_id"] == EXECUTION_ORDER[3] else "public_rclpy_graph_and_message_info"}
        row.update(attribute(row, row["typed_facts"], row["oracle"]))
        if row.get("actual_run"):
            try:
                capsule.record_execution(row)
            except ValueError as exc:
                row["capsule_execution_error"] = str(exc)
        write_json(planned["replay_path"], capsule.to_dict())
        report["rows"][index] = row
        if source_manifest(repo) != manifest["source_manifest"]:
            report["abort_reason"] = "source_repo_HEAD_or_behavior_tree_changed"
            break
    # Attribute only after all independent control evidence exists; no early
    # candidate attribution from a promised future control.
    from r2f.core.oracle import validate_control
    by_id = {row["cell_id"]: row for row in report["rows"]}
    for row in report["rows"]:
        rmw, repeat = row["rmw"], row["repeat"]
        controls = [by_id.get(f"{rmw}.{arm}.{repeat}", {}) for arm in ("baseline", "transparent", "mutated", "negative")]
        passed = all(c.get("execution_stage") == "judged" and validate_control(dict(c, control_kind=kind, transparency_expected="no_effect_or_expected_pass"))
                     for c, kind in zip(controls, ("health", "transparency", "mutation", "fault")))
        equal_payload = controls[0].get("actual_submitted_payload_sha256") == controls[1].get("actual_submitted_payload_sha256")
        row.update(controls_support_sut=passed and equal_payload, provenance_supports_sut=passed and equal_payload)
        if "oracle" in row:
            row.update(attribute(row, row.get("typed_facts", {}), row["oracle"]))
    return _persist(output_dir, report)
