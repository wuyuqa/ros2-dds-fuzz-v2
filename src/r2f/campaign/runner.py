"""Registry-based finite campaign runner for declared normal ROS2 API checks."""
from pathlib import Path
import json
import math
import re
import uuid
import time
import os
from copy import deepcopy

from r2f.core import Registry, compile_scenario
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import register
from r2f.modules.s1.targets import EXECUTION_ORDER
from r2f.reports.candidate_report import candidate_ledger, _write_immutable_json
from r2f.reports.run_report import (
    audit_report, identity_hashes, report_digest, write_json,
    boundary_qualification_for_row, execution_boundary_qualification,
)
from .identity import runtime_config, semantic_input, source_manifest
from .runtime_adapter import ingest_execution
from .scheduler import first_slice_cells, control_references
from .triage import attribute
from .anomalies import (prepare_identity, finish_evidence, runtime_exception,
                        persist_runtime_result, wrapper_failure, record_packaging_failure)

# The normal host path stays the historical default.  `isolated_netns` is an
# explicit host-side supervisor; it never turns an injected fixture into live
# evidence. These choices resolve exact callable identities; choosing a formal
# callable does not establish namespace/cleanup or real-execution qualification.
HOST_RUNTIME = 'host'
ISOLATED_RUNTIME = 'isolated_netns'
RUNTIME_BOUNDARIES = (HOST_RUNTIME, ISOLATED_RUNTIME)


def _validate_expected_source_manifest(source, expected_source_manifest):
    """Bind a caller's optional pin to the measured, complete intake source."""
    if expected_source_manifest is not None and source != expected_source_manifest:
        raise ValueError("expected_source_manifest_mismatch")


def plan_campaign(output_dir, *, repo=None, run_id=None, domain_id=173, seed=0,
                  target_id=EXECUTION_ORDER[0], parameters=None,
                  expected_source_manifest=None):
    """Build input capsules offline; planning never implies reached or executed.

    An optional expected_source_manifest must equal the entire measured intake
    source; a mismatch raises before any plan artifacts are written.
    """
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
    source = source_manifest(repo)
    _validate_expected_source_manifest(source, expected_source_manifest)
    manifest = {"schema_version": 1, "run_id": run_id, "target": target.to_dict(),
                "catalog": module.to_dict(), "scope": scope, "rule": target.rule_contract,
                "source_manifest": source, "environment_mode": "ros_overlay",
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
    eligible_rows, boundary_exclusions = [], []
    for row in report["rows"]:
        qualification = boundary_qualification_for_row(output_dir, row, report)
        if qualification["status"] in {"not_applicable", "qualified"}:
            if qualification["status"] == "qualified":
                # Boundary eligibility is only a prerequisite. Candidate/control
                # and seal admission still belong to the existing core gates.
                row.update(boundary_qualification=qualification, boundary_eligible=True)
            eligible_rows.append(row)
        else:
            # Keep raw/judged/actual_run history intact. Only admission changes.
            row.update(boundary_qualification=qualification, candidate_eligible=False,
                       feedback_eligible=False, seal_eligible=False)
            boundary_exclusions.append({"cell_id": row["cell_id"],
                                        "qualification": qualification,
                                        "actual_run": row.get("actual_run"),
                                        "raw_path": row.get("raw_path"),
                                        "raw_sha256": row.get("raw_sha256")})
    ledger = candidate_ledger(eligible_rows)
    if boundary_exclusions:
        ledger["boundary_exclusions"] = boundary_exclusions

    ledger_path = output_dir / "candidate_ledger.json"
    _write_immutable_json(ledger_path, ledger)
    report.update(candidate_ledger_path=str(ledger_path), candidate_ledger_sha256=canonical_hash(ledger))
    report["report_sha256"] = report_digest(report)
    report_path = output_dir / "report.json"
    audit_input = output_dir / "report-audit-input.json"
    _write_immutable_json(audit_input, report)
    report["seal"] = audit_report(audit_input)
    _write_immutable_json(report_path, report)
    lines = [f"# Campaign {report['run_id']}", "", f"Target: {report['target_id']} revision {report['target_revision']}",
             f"Per-target seal: {report['seal']['sealed']}", "Full S1 required denominator: 72; this report does not establish full S1 maturity.",
             f"Natural candidates: {ledger['natural_candidate_count']}; confirmed: {ledger['confirmed_candidates']}", "",
             ledger.get("zero_candidate_explanation") or "Candidate dispositions are retained in candidate_ledger.json.", "",
             "| Cell | Stage | Primary verdict | Reason |", "|---|---|---|---|"]
    for row in report["rows"]:
        lines.append(f"| {row['cell_id']} | {row['execution_stage']} | {row['oracle_verdict']} | {row.get('abort_reason') or row.get('reason_code', '')} |")
    lines += ["", "Seal errors:"] + [f"- {error}" for error in report["seal"]["errors"]]
    with (output_dir / "report.md").open("x") as stream:
        stream.write("\n".join(lines) + "\n")
    return report


def runtime_for_boundary(runtime_boundary=HOST_RUNTIME):
    """Resolve the exact formal runtime callable for an explicit boundary.

    No profile/capsule field grants this capability: only the caller's
    validated host/isolated choice selects a real runtime.  An injected
    callable is handled by the callers and remains offline contract evidence.
    """
    if runtime_boundary == HOST_RUNTIME:
        from r2f.runtime.ros2_pair import run_pair
        return run_pair
    if runtime_boundary == ISOLATED_RUNTIME:
        from r2f.runtime.execution_boundary import run_isolated_runtime
        return run_isolated_runtime
    raise ValueError('unsupported runtime_boundary: ' + repr(runtime_boundary))


def _boundary_execution_config(config, runtime_boundary):
    """Return an execution-only config copy with an explicit boundary switch.

    The deterministic capsule/final input/plan are untouched.  The isolated
    switch lives only in `execution_boundary`; a descriptive profile string is
    never consulted here.
    """
    if runtime_boundary == HOST_RUNTIME:
        return config
    if runtime_boundary != ISOLATED_RUNTIME:
        raise ValueError('unsupported runtime_boundary: ' + repr(runtime_boundary))
    copied = deepcopy(config)
    current = copied.get('execution_boundary')
    if current is None:
        existing = {}
    elif isinstance(current, str):
        # Profile prose is descriptive only.  It is replaced in the execution
        # copy, never interpreted as a switch.
        existing = {}
    elif not isinstance(current, dict):
        raise ValueError('execution_boundary must be prose or an options object')
    else:
        existing = current
    unknown = set(existing) - {'enabled', 'timeout_sec', 'term_grace_sec',
                               'kill_wait_sec', 'startup_timeout_sec'}
    if unknown:
        raise ValueError('execution boundary options conflict with profile text: '
                         + ','.join(sorted(unknown)))
    enabled = existing.get('enabled', True)
    if enabled is not True:
        raise ValueError('execution boundary switch must be exactly true')
    options = {key: existing[key] for key in ('timeout_sec', 'term_grace_sec', 'kill_wait_sec',
                                               'startup_timeout_sec') if key in existing}
    copied['execution_boundary'] = dict(options, enabled=True)
    return copied


def _runtime_execution_config(config, *, execute, runtime, runtime_boundary=HOST_RUNTIME):
    """Bind topology execute intent, never live GO or execution qualification.

    Caller-supplied intent is not authoritative. Only an explicit execute and
    the exact canonical callable for this boundary may set it, in a deep copy
    separate from semantic input/capsule identity. All runtime gates still run.
    """
    execution_config = _boundary_execution_config(config, runtime_boundary)
    from r2f.runtime.topology_route import topology_route_requested
    intended = (execute is True and topology_route_requested(execution_config)
                and _is_formal_runtime(runtime, runtime_boundary))
    if not intended and "live_execution_go" not in execution_config:
        return execution_config
    copied = deepcopy(execution_config)
    copied.pop("live_execution_go", None)
    if intended:
        copied["live_execution_go"] = True
    return copied


def runtime_formality(runtime, runtime_boundary=HOST_RUNTIME):
    """Exact canonical callable for the selected boundary, or None."""
    if runtime_boundary not in RUNTIME_BOUNDARIES:
        raise ValueError('unsupported runtime_boundary: ' + repr(runtime_boundary))
    canonical = runtime_for_boundary(runtime_boundary)
    return canonical if runtime is canonical else None


def _is_formal_runtime(runtime, runtime_boundary=HOST_RUNTIME):
    """Only the exact canonical callable for the selected boundary is formal."""
    if not callable(runtime):
        return False
    canonical = runtime_for_boundary(runtime_boundary)
    return runtime is canonical


def _resolve_execution_runtime(runtime=None, runtime_boundary=HOST_RUNTIME):
    """Select a callable by identity, before producing any execution artifacts.

    Names, module strings, wrappers and equality overloads grant no formality.
    An arbitrary injected callable is admitted only on the offline host path.
    """
    canonical = runtime_for_boundary(runtime_boundary)
    selected = canonical if runtime is None else runtime
    if not callable(selected):
        raise ValueError('runtime must be callable')
    formal = selected is canonical
    if runtime_boundary == ISOLATED_RUNTIME and not formal:
        raise ValueError('isolated_netns requires the canonical execution boundary runtime')
    return selected, formal


def run_campaign(output_dir, *, runtime=None, execute=False,
                 runtime_boundary=HOST_RUNTIME, auto_replay=None, replay_runtime=None, **options):
    """Sequential cells, never parallel campaigns. Default is offline plan only.

    The owner coordinates actual execution; execute=True is an explicit action.
    An injected runner is for contract testing and is not proof of ROS execution.
    auto_replay declares output_dir, domain_ids and max_attempts; attempts run
    after original evidence sealing. replay_runtime injection is offline-only.
    query_runner_evidence exposes associations without revising this report.
    """
    output_dir = Path(output_dir).resolve()
    if runtime_boundary not in RUNTIME_BOUNDARIES:
        raise ValueError('unsupported runtime_boundary: ' + repr(runtime_boundary))
    formal = False
    if execute:
        runtime, formal = _resolve_execution_runtime(runtime, runtime_boundary)
    report = plan_campaign(output_dir, **options)
    report.update(runtime_boundary=runtime_boundary, formal_runtime=formal)
    intake_manifest = json.loads(Path(report["manifest_path"]).read_text())
    intake_source = intake_manifest["source_manifest"]
    intake_rule = intake_manifest["rule"]["primary_rule"]
    for row in report["rows"]:
        prepare_identity(output_dir, row, intake_source, identity=report["identity"], rule=intake_rule)

    def finish():
        def persist_report():
            _persist(output_dir, report)
            return output_dir / "report.json"
        return finish_evidence(output_dir, report, persist_report, source=intake_source,
                               automatic_replay={"policy": auto_replay, "runtime": replay_runtime,
                                                 "execute": execute, "runtime_boundary": runtime_boundary,
                                                 "fixture_original": not formal})

    # Execution switches are persisted separately from capsule/input identity.
    # Planned qualification is pending; it is NOT an execution admission veto.
    for planned in report["rows"]:
        config_path = Path(planned["runtime_config_path"])
        config = json.loads(config_path.read_text())
        execution_config = _runtime_execution_config(
            config, execute=execute, runtime=runtime, runtime_boundary=runtime_boundary)
        if execution_config != config:
            write_json(config_path, execution_config)
        if runtime_boundary == ISOLATED_RUNTIME:
            planned["boundary_qualification"] = execution_boundary_qualification(runtime_boundary)
    if not execute:
        return finish()
    manifest = json.loads(Path(report["manifest_path"]).read_text())
    repo = Path(manifest["source_manifest"]["source_repo"])
    source = manifest["source_manifest"]
    if formal and (source.get("source_frozen") is not True
                   or runtime_boundary == ISOLATED_RUNTIME and (
                       source.get("dirty_behavior_paths") != [] or not source.get("head")
                       or not source.get("behavior_tree_sha256"))):
        report["abort_reason"] = "source_behavior_not_tracked_and_clean_at_HEAD"
        report["runtime_boundary"] = runtime_boundary
        return finish()
    registry = Registry()
    register(registry)
    services = registry.get_services(report["target_id"])
    for index, planned in enumerate(report["rows"]):
        if source_manifest(repo) != manifest["source_manifest"]:
            report["abort_reason"] = "source_repo_HEAD_or_behavior_tree_changed"
            break
        config = json.loads(Path(planned["runtime_config_path"]).read_text())
        config = _runtime_execution_config(
            config, execute=execute, runtime=runtime, runtime_boundary=runtime_boundary)
        write_json(planned["runtime_config_path"], config)
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
            result = runtime_exception(exc)
        # Preserve the result, including failures. run_pair already writes it; this
        # also gives injected contract runners the same artifact boundary.
        try:
            persist_runtime_result(planned["runtime_result_path"], result)
            execution = ingest_execution(scenario, config, result, capsule, services=services)
            row = dict(planned, **execution)
            if result.get("runner_exception"):
                row["runner_exception"] = result["runner_exception"]
        except Exception as exc:
            row = wrapper_failure(planned, exc)
            result = runtime_exception(exc)
        row["actual_submitted_payload_sha256"] = canonical_hash(capsule.input["effective_input"]["final_input"])
        row["replay_status"] = "not_run"
        row["provenance"] = {"source_manifest": manifest["source_manifest"], "runtime_execution": row.get("runtime_execution_metadata"), "raw_sha256": row.get("raw_sha256"), "observation_path": "isolated_discovery_activity_and_public_pair" if report["target_id"] == EXECUTION_ORDER[2] else "independent_passive_sedp_and_public_graph" if report["target_id"] == EXECUTION_ORDER[3] else "public_rclpy_graph_and_message_info"}
        row.update(attribute(row, row["typed_facts"], row["oracle"]))
        if row.get("actual_run"):
            try:
                capsule.record_execution(row)
            except ValueError as exc:
                row["capsule_execution_error"] = str(exc)
        report["rows"][index] = row
        try:
            write_json(planned["replay_path"], capsule.to_dict())
        except Exception as exc:
            record_packaging_failure(row, exc, planned["replay_path"])
            report["abort_reason"] = "packaging_failure"
            break
        qualification = execution_boundary_qualification(runtime_boundary, config=config,
                                                      result=result, row=row, formal_runtime=formal,
                                                      output_root=runtime_dir)
        if qualification["status"] != "not_applicable":
            row["boundary_qualification"] = qualification
        if qualification["status"] not in {"not_applicable", "qualified"}:
            row.update(boundary_qualification=qualification, candidate_eligible=False,
                       feedback_eligible=False, seal_eligible=False)
            report["abort_reason"] = qualification["reason_code"]
            break
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
        passed = all(c.get("execution_stage") == "judged"
                     and c.get("boundary_qualification", {}).get("status", "not_applicable") in {"not_applicable", "qualified"}
                     and validate_control(dict(c, control_kind=kind, transparency_expected="no_effect_or_expected_pass"))
                     for c, kind in zip(controls, ("health", "transparency", "mutation", "fault")))
        equal_payload = controls[0].get("actual_submitted_payload_sha256") == controls[1].get("actual_submitted_payload_sha256")
        row.update(controls_support_sut=passed and equal_payload, provenance_supports_sut=passed and equal_payload)
        if "oracle" in row:
            row.update(attribute(row, row.get("typed_facts", {}), row["oracle"]))
    return finish()




# ---------------------------------------------------------------------------
# Exploratory cycle: one feedback-driven input on the same execution services.
#
# This is not a second runner.  ``plan_campaign``/``run_campaign`` above are the
# required matrix and stay unchanged.  The exploratory entry below reuses the
# registry, providers, carrier, ingest_execution and replay capsule boundaries,
# but it is strictly opt-in and never starts ROS from import or planning.
#
# Identity rule (plan 5.4/5.5): the plan, its operations and runtime windows are
# part of the v2 effective input.  There is exactly ONE exploratory input
# digest: ``effective_input_hash(scenario.effective_input_v2(...), v2)``.
# Capsule, runtime config and row all carry that same digest.
# ---------------------------------------------------------------------------

import os as _os
import tempfile as _tempfile

from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.core.mutators import (
    MUTATORS, WIDE_SAMPLING_KINDS, _wide_kinds, make_rng, mutate, mutate_f2, mutate_wide,
)
from r2f.core.mutation_plan import compose, compose_chain
from r2f.core.scenario import Scenario
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.targets import EXPLORATORY_TARGET_ORDER
from r2f.modules.s1.fuzz_catalog import catalog_digest
from .corpus import Corpus, content_identity
from .mutation_routes import (has_routes, inherit_routes, routes_enabled,
                              validate_route_entry, validate_route_selection)
from .feedback import FeedbackLog, observation_complete, tool_error
from .scheduler import ExploratoryScheduler, required_catalog_hash
from .exploratory_input import (CONTROL_FIELDS, PROFILE_CONTROL_FIELDS,
                                apply_profile_controls, runtime_input,
                                validate_profile_fields, validate_f2_profile,
                                F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET,
                                F2_SURFACE_MUTATORS)

from r2f.modules.s1.topology_targets import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, TOPOLOGY_SURFACE, TOPOLOGY_MUTATOR,
)

EXPLORATORY_CYCLE_SCHEMA = "r2f.s1-exploratory-cycle/v1"

#: Legacy alias kept for callers that still import it.  It is NOT the runner
#: gate any more: ``_F1_CYCLE_TARGETS`` (derived from ``_EXPLORATORY_ROUTES``)
#: is the single authority, so adding a route cannot silently widen this set.
_ENDPOINT_CYCLE_TARGETS = frozenset({
    "discovery.late_endpoint.fuzz_v1",
    "discovery.late_join_delivery.fuzz_v1",
})

#: Explicit exploratory target -> runtime backend route.  The target id is
#: never rewritten; the route only selects which existing runtime adapter and
#: carrier receive the generated plan, and how the corpus parent is projected
#: into that adapter's config shape.  ``execution_admitted`` is deliberately
#: False where only planned generation has been verified: execute=True then
#: returns an explicit unsupported_capability instead of a fabricated run.
_EXPLORATORY_ROUTES = {
    "discovery.late_endpoint.fuzz_v1": {
        "family": "endpoint_pair", "backend": "endpoint_pair",
        "carrier": "ros2_endpoint", "mutation_kind": "late_joiner",
        "execution_admitted": True,
    },
    "discovery.late_join_delivery.fuzz_v1": {
        "family": "endpoint_pair", "backend": "endpoint_pair",
        "carrier": "ros2_endpoint", "mutation_kind": "late_joiner",
        "execution_admitted": True,
    },
    "discovery.interruption_recovery.fuzz_v1": {
        "family": "discovery_namespace", "backend": "discovery_namespace",
        "carrier": "wire_proxy", "mutation_kind": "discovery_drop",
        # Verified single-window live on 6fcc8fd: namespace preflight passed,
        # window ledger began/ended, restore completed and runtime_status
        # completed.  Multi-window remains refused below.
        "execution_admitted": True,
    },
    "builtin.discovery_projection_reconciliation.fuzz_v1": {
        "family": "projection_pair", "backend": "projection_pair",
        "carrier": "ros2_endpoint", "mutation_kind": "endpoint_qos_variation",
        # Verified live on 7bccef7: projection_pair reached real_execution=true,
        # runtime_status=completed, two role QoS bindings, wire readiness,
        # graph export and projection_observation.  Admission is evidence-based,
        # not inferred from planned config alone.
        "execution_admitted": True,
    },
}

#: Default-capability exploratory routes (legacy name _F1_CYCLE_TARGETS).
#: Exact revision1 G01 is a local F2/M08 exception, not global F1 activation.
#: A route remains dispatch knowledge, never live/source qualification.
_EXPLORATORY_ROUTES[F2_WINDOWS_TARGET] = {
    "family": "discovery_namespace", "backend": "discovery_namespace",
    "carrier": "wire_proxy", "mutation_kind": "discovery_drop",
    "runtime_capability": F2_WINDOWS_CAPABILITY,
    # Dispatch admission is explicit, not a claim that this source is live
    # qualified. Runtime observations/locks/source gates remain mandatory.
    "execution_admitted": True,
}
_EXPLORATORY_ROUTES[TOPOLOGY_TARGET] = {
    "family": "topology_program", "backend": "topology_pair",
    "carrier": "ros2_endpoint", "mutation_kind": "endpoint_program",
    "runtime_capability": TOPOLOGY_CAPABILITY,
    # Admission selects the existing canonical dispatcher, not live qualification.
    "execution_admitted": True,
}
G01_TARGET = "discovery.match_compatibility.fuzz_v1"
_EXPLORATORY_ROUTES[G01_TARGET] = {
    "family": "relation_pair", "backend": "relation_pair",
    "carrier": "ros2_endpoint", "mutation_kind": "endpoint_qos_variation",
    "execution_admitted": True,  # dispatch only; raw/source gates are unchanged
    "target_revision": 1,
}
_F1_CYCLE_TARGETS = frozenset(t for t, route in _EXPLORATORY_ROUTES.items()
                              if "runtime_capability" not in route)
_F2_CYCLE_TARGETS = frozenset({F2_WINDOWS_TARGET})

#: Public projection QoS fields the projection_pair runtime actually consumes.
#: Catalog annotations (deadline_class, special_value_classes) are input-model
#: metadata, not runtime fields, and are dropped with an audit receipt.
_PROJECTION_QOS_FIELDS = (
    "reliability", "durability", "depth", "deadline_ns",
    "liveliness", "liveliness_lease_duration_ns",
)
_PROJECTION_BASELINE_QOS = {
    "reliability": "reliable", "durability": "volatile", "depth": 64,
    "deadline_ns": 9223372036854775807, "liveliness": "automatic",
    "liveliness_lease_duration_ns": 9223372036854775807,
}

class ExploratoryCycleError(ValueError):
    """Raised when an exploratory cycle cannot be constructed honestly."""


class _NoEffectiveInputRejection(ExploratoryCycleError):
    """Core-classified ineffective proposal; retained audit, never prepared."""

    def __init__(self, reason, plan):
        super().__init__(reason)
        self.plan = plan


class _F2InputRejection(ExploratoryCycleError):
    """Pre-compose F2 guard failure; never an executed/runtime result."""


def _atomic_json(path, value, *, durable=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    fd, temporary = _tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with _os.fdopen(fd, "w") as handle:
            handle.write(payload)
            if durable:
                handle.flush()
                _os.fsync(handle.fileno())
        _os.replace(temporary, path)
        if durable:
            directory = _os.open(str(path.parent), _os.O_RDONLY | _os.O_DIRECTORY)
            try:
                _os.fsync(directory)
            finally:
                _os.close(directory)
    except BaseException:
        try:
            _os.unlink(temporary)
        except OSError:
            pass
        raise


def _load_profile(profile):
    if not isinstance(profile, dict):
        raise ExploratoryCycleError("exploratory cycle requires an explicit profile object")
    if profile.get("campaign_kind") != "exploratory":
        raise ExploratoryCycleError("profile campaign_kind must be exploratory")
    if profile.get("required_catalog_included") is not False:
        raise ExploratoryCycleError("exploratory profile must not include the required catalog")
    if not isinstance(profile.get("profile_id"), str) or not profile["profile_id"]:
        raise ExploratoryCycleError("profile_id is required")
    for key in ("target_ids", "surfaces", "rmws"):
        if not isinstance(profile.get(key), list) or not profile[key]:
            raise ExploratoryCycleError("profile " + key + " is required")
    _profile_wide_settings(profile)
    try:
        validate_profile_fields(profile)
        routes_enabled(profile)
    except ValueError as exc:
        raise ExploratoryCycleError(str(exc)) from exc
    return json.loads(json.dumps(profile, allow_nan=False))


def _profile_wide_settings(profile):
    """Validate T-E optional fields without adding defaults to the profile.

    Absence of every T-E field must leave the profile document (and therefore
    its manifest bytes) exactly as it was.  Validation is therefore a pure
    read; a missing field is never materialised here.
    """
    keys = {"sampling_kinds", "sampling_weight", "chain_length"} & set(profile)
    if not keys:
        return None, None, 1
    if "sampling_weight" in profile and "sampling_kinds" not in profile:
        raise ExploratoryCycleError("sampling_weight requires sampling_kinds")
    kinds = profile.get("sampling_kinds")
    weights = None
    if kinds is not None:
        if not isinstance(kinds, list) or not kinds:
            raise ExploratoryCycleError("sampling_kinds must be a nonempty list")
        if any(not isinstance(kind, str) or not kind for kind in kinds):
            raise ExploratoryCycleError("sampling_kinds must contain nonempty strings")
        if len(set(kinds)) != len(kinds):
            raise ExploratoryCycleError("sampling_kinds contains duplicates")
        unknown = sorted(set(kinds) - set(WIDE_SAMPLING_KINDS))
        if unknown:
            raise ExploratoryCycleError("unsupported sampling kind(s): " + ",".join(unknown))
        raw = profile.get("sampling_weight") or {}
        if not isinstance(raw, dict):
            raise ExploratoryCycleError("sampling_weight must be an object")
        unknown = sorted(set(raw) - set(kinds))
        if unknown:
            raise ExploratoryCycleError(
                "sampling_weight names undeclared kind(s): " + ",".join(unknown))
        weights = {}
        for kind in kinds:
            weight = raw.get(kind, 1)
            if type(weight) is not int or weight < 0:
                raise ExploratoryCycleError(
                    "sampling_weight must be nonnegative integers")
            weights[kind] = weight
        if not any(weights.values()):
            raise ExploratoryCycleError("sampling_weight must not be all zero")
    chain_length = profile.get("chain_length", 1)
    if type(chain_length) is not int or not 1 <= chain_length <= 4:
        raise ExploratoryCycleError("chain_length must be an integer within 1..4")
    return kinds, weights, chain_length


def _sync_sampling(scheduler, profile):
    """Bind the opt-in profile sampling config to the caller's scheduler.

    The runner is the only authority for what a profile declares; a scheduler
    built with a different (or missing) sampling config is refused instead of
    silently selecting kinds the profile never asked for.
    """
    kinds, weights, _ = _profile_wide_settings(profile)
    if kinds is None:
        if scheduler.sampling_kinds is not None:
            raise ExploratoryCycleError(
                "scheduler declares sampling kinds that the profile does not")
        return
    if scheduler.sampling_kinds is None:
        if scheduler.selection_log:
            raise ExploratoryCycleError(
                "scheduler already selected entries before sampling was configured")
        try:
            scheduler.configure_sampling(kinds, weights)
        except ValueError as exc:
            raise ExploratoryCycleError(str(exc)) from exc
        return
    if scheduler.sampling_kinds != list(kinds) or scheduler.sampling_weight != weights:
        raise ExploratoryCycleError(
            "scheduler sampling config differs from the profile declaration")


def _application_phase(surface):
    if surface.carrier_id == "wire_proxy":
        return "runtime_window"
    return "compile_time"


def _derive_rng_seed(scheduler, selection, entry):
    if getattr(scheduler, "policy", None) == "bfs":
        sequence = selection.get("sequence")
        if type(sequence) is not int or not 0 <= sequence < len(scheduler.selection_log):
            raise ExploratoryCycleError("input search task is not an accepted selection")
        event = {key: value for key, value in selection.items() if key != "entry"}
        accepted = scheduler.selection_log[sequence]
        if (event != accepted or entry != scheduler.frontier.entry(accepted)):
            raise ExploratoryCycleError("input search selection/parent binding mismatch")
        return accepted["rng_seed"]
    if getattr(scheduler, "policy", None) == "fair":
        # Fair's audit selection hash deliberately retains feedback identity.
        # Only its scheduler-authenticated model decision may drive generation.
        decision = selection.get("decision_sha256")
        if not isinstance(decision, str) or not re.fullmatch(r"[0-9a-f]{64}", decision):
            raise ExploratoryCycleError("fair scheduler requires a valid decision_sha256")
        sequence = selection.get("sequence")
        if type(sequence) is not int or not 0 <= sequence < len(scheduler.selection_log):
            raise ExploratoryCycleError("fair decision_sha256 has no accepted selection")
        accepted = scheduler.selection_log[sequence]
        event = {key: value for key, value in selection.items() if key != "entry"}
        if (event != accepted or entry.get("content_sha256") != accepted.get("content_sha256")
                or not isinstance(accepted.get("decision_context"), dict)
                or canonical_hash(accepted["decision_context"]) != decision):
            raise ExploratoryCycleError("fair decision_sha256 differs from the accepted scheduler event")
        material = canonical_hash({
            "cycle_rng_seed": scheduler.rng_seed,
            "decision_sha256": decision,
            "sequence": sequence,
            "parent_content_sha256": entry["content_sha256"],
        })
        return int(material[:16], 16)
    material = canonical_hash({
        "cycle_rng_seed": scheduler.rng_seed,
        "selection_sha256": selection["selection_sha256"],
        "sequence": selection["sequence"],
        "parent_content_sha256": entry["content_sha256"],
    })
    return int(material[:16], 16)


_OFFLINE_EVIDENCE_ORIGINS = frozenset({"fixture", "synthetic", "offline"})


class _TopologyInputRejection(ValueError):
    def __init__(self, detail, attempts):
        super().__init__(detail)
        self.attempts = attempts


def _topology_generation(parent, profile, rng_seed):
    """Bounded explicit S1 bridge, not a default MUTATORS entry.

    Candidate failures are input generation diagnostics, never runtime attempts.
    Every accepted layer is rebuilt by the core bridge from the original parent
    and the saved ordered requests, using its documented seed+i scheme.
    """
    from r2f.modules.s1.topology_input import make_topology_plan
    from .exploratory_input import validate_topology_profile
    validate_topology_profile(profile)
    if not isinstance(parent, dict) or set(parent) != {"topology_program"}:
        raise _TopologyInputRejection("topology_parent_shape_mismatch", [])
    requests, attempts, plan = [], [], None
    rng = make_rng(rng_seed)
    for index in range(profile["chain_length"]):
        operators = list(profile["operator_whitelist"])
        rng.shuffle(operators)
        for operator in operators:
            request = {"operator_id": operator}
            try:
                proposed = make_topology_plan(parent["topology_program"], requests + [request], rng_seed=rng_seed)
                if any(op.applicability_status in {"rejected", "blocked"} for op in proposed.operations):
                    raise ValueError("topology_deliberate_guard_or_blocked_layer_not_executable")
            except ValueError as exc:
                attempts.append({"layer_index": index, "operator_id": operator,
                                 "status": "rejected", "reason": str(exc)})
                continue
            attempts.append({"layer_index": index, "operator_id": operator, "status": "accepted"})
            requests.append(request)
            plan = proposed
            break
        else:
            raise _TopologyInputRejection("topology_no_applicable_content_changing_operator:layer=" + str(index), attempts)
    return plan, {"schema": "r2f.topology-generation/v1", "rng_seed": rng_seed,
                  "operator_whitelist": list(profile["operator_whitelist"]),
                  "requests": requests, "attempts": attempts,
                  "chain_length": len(plan.operations), "execution_evidence": False}


def _evidence_origin(runtime_result, *, runtime=None, runtime_boundary=HOST_RUNTIME):
    """Label the selected callable's source, never its execution qualification.

    Result-supplied strings/real flags cannot attest a callable. Explicit
    fixtures stay offline even when returned by a canonical callable. A formal
    source label also covers failed/blocked attempts: artifact qualification,
    actual execution, candidate admission and seal gates remain independent.
    """
    origin = runtime_result.get("evidence_origin")
    if isinstance(origin, str) and origin in (_OFFLINE_EVIDENCE_ORIGINS | {"offline_fixture"}):
        return "offline_fixture"
    if _is_formal_runtime(runtime, runtime_boundary):
        from .replay import DEFAULT_RUNTIME, ISOLATED_RUNTIME as ISOLATED_ORIGIN
        return ISOLATED_ORIGIN if runtime_boundary == ISOLATED_RUNTIME else DEFAULT_RUNTIME
    return "injected_runtime"


def _flatten_surface_values(surface, config):
    values = {}
    for path in surface.input_paths:
        current = config
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                raise ValueError("submitted_parameter_missing:" + path)
            current = current[part]
        if current is None:
            raise ValueError("submitted_parameter_null:" + path)
        values[path] = current
    return values


def _plan_surface_bindings(plan, contract):
    """Derive audit metadata for every saved operation, never a new hash layer.

    Keep first-occurrence operation order and each surface's input-path order.
    Shared paths must agree exactly; a missing declaration is not a no-op.
    A valid single-operation input retains its historical projection bytes.
    """
    surfaces = list(dict.fromkeys(op.surface_id for op in plan.operations))
    if not surfaces:
        raise ValueError("submitted_surfaces_missing")
    submitted = {}
    for surface_id in surfaces:
        surface = contract.surface_by_id.get(surface_id)
        if surface is None:
            raise ValueError("submitted_surface_not_in_contract:" + surface_id)
        if not surface.input_paths:
            raise ValueError("submitted_surface_input_paths_missing:" + surface_id)
        if plan.final_config.get("runtime_capability") == F2_WINDOWS_CAPABILITY and surface_id in F2_SURFACE_MUTATORS:
            windows = _validate_explicit_recovery_windows(plan.final_config.get("environment_windows"))
            operation = next(op for op in plan.operations if op.surface_id == surface_id)
            anchor = operation.parameters.get("anchor")
            applicable = [w for w in windows if w["trigger_anchor"] == anchor]
            if not applicable:
                raise ValueError("f2_previous_restore_anchor_requires_multiple_windows")
            offset_key = "anchor_offset_sec" if surface_id == "E02" else "offset_sec"
            offset = operation.parameters.get(offset_key)
            if type(offset) not in (int, float) or any(
                    w["trigger_offset_sec"] != offset for w in applicable):
                raise ValueError("f2_affected_window_offset_binding_mismatch")
            projected = {"environment_windows.count": len(windows),
                         "environment_windows.restore_interval_sec": windows[0]["restore_interval_sec"],
                         "environment_windows.anchor": anchor,
                         "environment_windows.anchor_offset_sec": offset}
            values = {path: projected[path] for path in surface.input_paths}
        else:
            values = _flatten_surface_values(surface, plan.final_config)
        for path, value in values.items():
            if path in submitted and canonical_hash(submitted[path]) != canonical_hash(value):
                raise ValueError("submitted_parameter_conflict:" + path)
            submitted[path] = value
    return surfaces, submitted


def _wide_generation_requested(profile):
    """True only when the profile explicitly opts into T-E generation.

    An explicit ``chain_length=1`` alone selects nothing new: the historical
    single-mutation path is kept byte for byte.  Chains are multi-layer by
    definition (``chain_length > 1``), and ``sampling_kinds`` always opts in.
    """
    if profile.get("sampling_kinds") is not None:
        return True
    depth = profile.get("chain_length")
    return type(depth) is int and depth > 1


def _reject_all_zero_supported(profile, surface, parent):
    """True when the profile weight excludes every kind this surface supports."""
    kinds, weights, _ = _profile_wide_settings(profile)
    if kinds is None:
        return False
    supported = set(_wide_kinds(surface, parent))
    return not [kind for kind in kinds
                if kind in supported and weights.get(kind, 1) > 0]


def _layer_rng(rng_seed, layer_index, surface_id):
    """Per-layer RNG derived from the case seed; never shared across layers."""
    material = canonical_hash({"rng_seed": rng_seed, "layer_index": layer_index,
                               "surface_id": surface_id})
    return make_rng(int(material[:16], 16))


def _chain_surfaces(contract, surface, chain_length, allowed_surfaces=None):
    """Deterministic ordered layer surfaces for one chain length.

    The entry surface is always layer 0.  Additional layers are the other
    declared surfaces that compose with every already-chosen layer, in sorted
    surface-id order.  When the contract cannot supply enough compatible
    layers the request is rejected explicitly rather than repeating a surface or
    silently shortening the chain.
    """
    if type(chain_length) is not int or not 1 <= chain_length <= 4:
        raise ExploratoryCycleError("chain_length must be an integer within 1..4")
    order = [surface]
    if chain_length == 1:
        return order
    others = sorted(s for s in contract.surface_by_id if s != surface.surface_id
                    and (allowed_surfaces is None or s in allowed_surfaces))
    while len(order) < chain_length:
        selected = None
        from r2f.core.mutation_plan import (
            CompositionRejection, _precheck_chain_constraints,
        )
        for candidate in others:
            if candidate in {item.surface_id for item in order}:
                continue
            probe = order + [contract.surface_by_id[candidate]]
            try:
                _precheck_chain_constraints(probe, contract)
            except CompositionRejection:
                continue
            selected = candidate
            break
        if selected is None:
            raise ExploratoryCycleError(
                "unsupported_capability:chain_length_not_composable:"
                f"{surface.surface_id}:{chain_length}")
        order.append(contract.surface_by_id[selected])
    return order


def _declared_route_layers(entry, contract, chain_length):
    routes = validate_route_entry(entry, contract=contract)
    selected = entry.get("mutation_route")
    if routes is None or selected not in routes:
        raise ExploratoryCycleError("selected mutation route missing")
    surfaces = _chain_surfaces(contract, contract.surface_by_id[selected["surface_id"]],
                               chain_length, {route["surface_id"] for route in routes})
    return [(surface, selected if index == 0 else next(
        route for route in routes if route["surface_id"] == surface.surface_id))
        for index, surface in enumerate(surfaces)]


def _layer_mutator(surface, entry, layer_index, sampling_kind=None):
    """Pick one mutator compatible with the layer's sampling class.

    The wide namespace is explicit about which mutator may emit a class:
    M01 only produces ``boundary``; M03 produces the numeric classes; M02 emits
    enum representatives; M04 emits relation boundaries.  Layer 0 otherwise
    keeps the runner's historical entry-mutator choice, and later layers use the
    first registered mutator the layer surface declares.  A chain is therefore
    a fixed function of the contract, entry and requested sampling class.
    """
    available = [mid for mid in surface.mutator_ids if mid in MUTATORS]
    if surface.surface_kind == "relation" and "M04" in surface.mutator_ids:
        if sampling_kind is None or sampling_kind == "boundary":
            return "M04"
        raise ExploratoryCycleError(
            "unsupported_capability:relation_surface_kind:" + str(sampling_kind))
    if surface.surface_kind == "parameter_enum" and "M02" in available:
        if sampling_kind is None or sampling_kind == "representative":
            return "M02"
        raise ExploratoryCycleError(
            "unsupported_capability:enum_surface_kind:" + str(sampling_kind))
    if not available:
        raise ExploratoryCycleError(
            "unsupported_capability:no_implemented_mutator:" + surface.surface_id)
    if sampling_kind == "boundary":
        return "M01" if "M01" in available else available[0]
    if sampling_kind in {"representative", "log_uniform", "linear_uniform",
                        "neighbor", "jump"}:
        return "M03" if "M03" in available else available[0]
    if layer_index == 0:
        requested = entry.get("mutator_id")
        if requested in available:
            return requested
        boundary = entry.get("boundary_class")
        preferred = "M03" if boundary in {"log_range", "sampled"} else "M01"
        if preferred in available:
            return preferred
    return available[0]


def _layer_context(entry, layer_index, sampling_kind):
    context = {"application_phase": "compile_time",
               "boundary_class": entry.get("boundary_class")}
    if layer_index > 0:
        # Later layers must not reuse a parent boundary class that belongs to
        # another surface; a wide class is chosen explicitly instead.
        context.pop("boundary_class", None)
    if sampling_kind is not None:
        context["sampling_kind"] = sampling_kind
    return context


def _route(target_id):
    """Explicit target -> backend/carrier route, or None when not F1-routed."""
    route = _EXPLORATORY_ROUTES.get(target_id)
    return dict(route) if route is not None else None


def _projection_qos_view(qos):
    """Split catalog QoS input into runtime-consumable fields and annotations.

    Catalog seeds carry modelling annotations (``deadline_class``,
    ``liveliness_lease_class``, ``special_value_classes``).  They describe the
    input model and are not public QoS fields; dropping them is recorded in the
    adaptation receipt instead of being silently normalized away.
    """
    if not isinstance(qos, dict):
        raise ValueError("projection parent requires a qos mapping")
    dropped = sorted(set(qos) - set(_PROJECTION_QOS_FIELDS))
    runtime_qos = {key: qos[key] for key in _PROJECTION_QOS_FIELDS if key in qos}
    return runtime_qos, dropped


def _adapt_parent(target_id, route, parent):
    """Project a corpus seed into the config shape its runtime backend consumes.

    The mutation always happens on this adapted parent, so the plan's parent
    hash and its ``final_config`` describe exactly the payload handed to the
    runtime.  The returned receipt states every derivation explicitly.
    """
    adapted = json.loads(json.dumps(parent))
    receipt = {"target_id": target_id, "backend": route["backend"],
               "derived": False, "dropped_fields": []}
    if route["backend"] == "relation_pair":
        if "qos" not in adapted:
            adapted["qos"] = {"reliability": "reliable", "durability": "volatile", "depth": 32}
            receipt.update(derived=True, added_fields=["qos"],
                           note="explicit v2 envelope; role QoS remains authoritative")
    if route["backend"] == "projection_pair":
        qos, dropped = _projection_qos_view(adapted.get("qos"))
        baseline = adapted.get("baseline_qos")
        if not isinstance(baseline, dict):
            baseline = dict(_PROJECTION_BASELINE_QOS)
        else:
            baseline, baseline_dropped = _projection_qos_view(baseline)
            dropped = sorted(set(dropped) | {"baseline_qos." + key for key in baseline_dropped})
        adapted["qos"] = qos
        adapted["baseline_qos"] = baseline
        receipt.update(derived=True, dropped_fields=dropped,
                       added_fields=["baseline_qos"],
                       derived_after_mutation=["writer_qos", "reader_qos"],
                       note="catalog QoS annotations are input-model metadata, not runtime "
                            "fields; role QoS is derived from the mutated qos after the plan")
    elif route["backend"] == "discovery_namespace":
        missing = [key for key in ("drop_duration_sec", "recovery_timeout_sec",
                                   "counter_interval_sec") if key not in adapted]
        if missing:
            raise ValueError("recovery parent is missing: " + ",".join(missing))
        receipt.update(required_fields=["drop_duration_sec", "recovery_timeout_sec",
                                        "counter_interval_sec"])
    if target_id == F2_WINDOWS_TARGET and isinstance(adapted.get("environment_windows"), list):
        # M06/M07 write declared logical paths. Preserve the concrete parent
        # windows in an audited pre-compose projection; never patch a finished
        # operation or re-label its input_after hash.
        windows = _validate_explicit_recovery_windows(adapted["environment_windows"])
        adapted["environment_windows"] = {
            "count": len(windows), "windows": windows,
            "anchor": "fresh_match", "anchor_offset_sec": windows[0]["trigger_offset_sec"],
            "restore_interval_sec": windows[0]["restore_interval_sec"],
        }
        receipt["f2_logical_parent_projection"] = {
            "concrete_windows_preserved": True, "before_compose": True,
            "source_windows_sha256": canonical_hash(parent["environment_windows"]),
            "logical_windows_sha256": canonical_hash(adapted["environment_windows"]),
        }
    return adapted, receipt


_RUNTIME_WINDOW_FIELDS = frozenset({
    "schema", "window_id", "window_index", "trigger_anchor",
    "trigger_offset_sec", "drop_duration_sec", "restore_interval_sec",
})


def _validate_explicit_recovery_windows(windows, *, recovery_timeout_sec=None,
                                        max_seconds=None):
    """Validate a 1..3 window plan from M07 and normalize its indices.

    The core plan schema accepts any integer window_index, while the namespace
    validator requires positions 0..n-1.  Explicit plans may be written from
    either 0 or 1; both are accepted, and the returned plan is normalized to
    0-based positions so the two emitted shapes agree.  Every failure names the
    offending field; no plan is silently truncated or repaired.
    """
    if not isinstance(windows, (list, tuple)):
        raise ValueError("runtime_windows must be a list of 1..3 windows")
    if not 1 <= len(windows) <= 3:
        raise ValueError("runtime_windows must contain 1..3 windows")
    if recovery_timeout_sec is not None and (
            type(recovery_timeout_sec) not in (int, float)
            or isinstance(recovery_timeout_sec, bool)
            or not 0 <= recovery_timeout_sec <= 120):
        raise ValueError("recovery_timeout_sec must be a number in [0,120]")

    result = []
    for position, window in enumerate(windows):
        if not isinstance(window, dict):
            raise ValueError(f"runtime_window[{position}] must be a dict")
        unknown = sorted(set(window) - _RUNTIME_WINDOW_FIELDS)
        if unknown:
            raise ValueError("runtime_window." + unknown[0] + " is not supported")
        schema = window.get("schema")
        if schema is not None and schema != "r2f.runtime-window/v1":
            raise ValueError("runtime_window.schema must be r2f.runtime-window/v1")
        window_id = window.get("window_id")
        if not isinstance(window_id, str) or not window_id.strip():
            raise ValueError("runtime_window.window_id must be a nonempty string")
        index = window.get("window_index")
        if type(index) is not int or isinstance(index, bool):
            raise ValueError("runtime_window.window_index must be an integer")
        anchor = window.get("trigger_anchor")
        if not isinstance(anchor, str) or not anchor:
            raise ValueError("runtime_window.trigger_anchor is required")
        if position == 0 and anchor != "fresh_match":
            raise ValueError("runtime_window.trigger_anchor must be fresh_match for the first window")
        if position > 0 and anchor != "previous_restore_confirmed":
            raise ValueError("runtime_window.trigger_anchor must be previous_restore_confirmed after the first window")
        values = {}
        for key in ("trigger_offset_sec", "drop_duration_sec", "restore_interval_sec"):
            value = window.get(key)
            if type(value) not in (int, float) or isinstance(value, bool):
                raise ValueError(f"runtime_window.{key} must be a number")
            if not math.isfinite(value):
                raise ValueError(f"runtime_window.{key} must be finite")
            values[key] = float(value)
        if not 0 < values["drop_duration_sec"] <= 8:
            raise ValueError("runtime_window.drop_duration_sec must be in (0,8]")
        if not 0 <= values["trigger_offset_sec"] <= 8:
            raise ValueError("runtime_window.trigger_offset_sec must be in [0,8]")
        if not 0 <= values["restore_interval_sec"] <= 8:
            raise ValueError("runtime_window.restore_interval_sec must be in [0,8]")
        result.append({"window_id": window_id, "window_index": index, "trigger_anchor": anchor,
                       **values})

    base = result[0]["window_index"]
    if base not in (0, 1):
        raise ValueError("runtime_window.window_index must be consecutive from 0 or 1")
    for position, window in enumerate(result):
        if window["window_index"] != base + position:
            raise ValueError("runtime_window.window_index must be consecutive from 0 or 1")
    ids = [window["window_id"] for window in result]
    if len(ids) != len(set(ids)):
        raise ValueError("runtime_window.window_id must be unique")

    if max_seconds is not None:
        if type(max_seconds) not in (int, float) or isinstance(max_seconds, bool) \
                or not math.isfinite(max_seconds) or max_seconds < 0:
            raise ValueError("max_seconds must be a non-negative finite number")
        timeout = float(recovery_timeout_sec or 0.0)
        duration = sum(window["trigger_offset_sec"] + window["drop_duration_sec"]
                       + window["restore_interval_sec"] for window in result)
        budget = duration + timeout * len(result)
        if budget > max_seconds:
            raise ValueError(
                "runtime_windows budget exceeds config max_seconds:"
                f" budget={budget}, max_seconds={max_seconds}")

    return [{"window_id": window["window_id"], "window_index": position,
             "trigger_anchor": window["trigger_anchor"],
             "trigger_offset_sec": window["trigger_offset_sec"],
             "drop_duration_sec": window["drop_duration_sec"],
             "restore_interval_sec": window["restore_interval_sec"]}
            for position, window in enumerate(result)]


def _recovery_windows(drop_duration_sec, windows=None, *, recovery_timeout_sec=None,
                      max_seconds=None):
    """Build the core/runtime window documents.

    With no explicit plan the historical single-window return shape is kept
    byte-for-byte: ``(core_window, runtime_window)``.  With an M07
    ``proposal.runtime_windows`` plan this returns two 1..3 element lists.
    The two shapes differ only by the core-only schema field.
    """
    if windows is not None:
        normalized = _validate_explicit_recovery_windows(
            windows, recovery_timeout_sec=recovery_timeout_sec,
            max_seconds=max_seconds)
        core_windows = [{"schema": "r2f.runtime-window/v1", **window}
                        for window in normalized]
        runtime_windows = [dict(window) for window in normalized]
        return core_windows, runtime_windows
    if type(drop_duration_sec) not in (int, float) or isinstance(drop_duration_sec, bool) \
            or not 0 < drop_duration_sec <= 8.0:
        raise ValueError("recovery drop_duration_sec must be a positive finite window")
    core_window = {
        "schema": "r2f.runtime-window/v1", "window_id": "w0", "window_index": 0,
        "trigger_anchor": "fresh_match", "trigger_offset_sec": 0.0,
        "drop_duration_sec": drop_duration_sec, "restore_interval_sec": 0.0,
    }
    runtime_window = {key: value for key, value in core_window.items() if key != "schema"}
    # Same bounds/budget checks as explicit plans, without changing the legacy
    # single-window representation or its historical hashes.
    _validate_explicit_recovery_windows(
        [runtime_window], recovery_timeout_sec=recovery_timeout_sec, max_seconds=max_seconds)
    return core_window, runtime_window


def _materialize_recovery_windows(config):
    """Resolve inherited/catalog window input without discarding its order."""
    value = config["environment_windows"]
    if isinstance(value, list):
        return _validate_explicit_recovery_windows(value)
    if not isinstance(value, dict) or not value:
        raise ValueError("environment_windows must be a nonempty window plan")
    allowed = {"count", "restore_interval_sec", "windows", "anchor", "anchor_offset_sec"}
    unknown = set(value) - allowed
    if unknown:
        raise ValueError("unsupported environment_windows field:" + ",".join(sorted(unknown)))
    count = value.get("count")
    if type(count) is not int or not 1 <= count <= 3:
        raise ValueError("environment_windows.count must be an integer in 1..3")
    for field in ("restore_interval_sec", "anchor_offset_sec"):
        if field in value and (type(value[field]) not in (int, float)
                               or not math.isfinite(value[field]) or not 0 <= value[field] <= 8):
            raise ValueError("invalid environment_windows." + field)
    f2_windows = config.get("runtime_capability") == F2_WINDOWS_CAPABILITY
    anchor = value.get("anchor", "fresh_match")
    if anchor != "fresh_match" and not (f2_windows and anchor == "previous_restore_confirmed"):
        raise ValueError("environment_windows.anchor must be fresh_match")
    if anchor == "previous_restore_confirmed" and count == 1:
        raise ValueError("f2_previous_restore_anchor_requires_multiple_windows")
    declared = value.get("windows")
    if "windows" in value and (not isinstance(declared, list) or len(declared) != count):
        raise ValueError("environment_windows.windows must match count")
    windows = []
    for position in range(count):
        window = (dict(declared[position]) if declared is not None
                  and isinstance(declared[position], dict) else {})
        if declared is not None and not window:
            raise ValueError("environment_windows.windows requires nonempty window objects")
        # M07 catalog metadata is not an executable begin/end receipt.
        for key in ("begin", "end", "restore", "requires_previous_restore"):
            if key in window:
                expected = position > 0 if key == "requires_previous_restore" else True
                if window.pop(key) is not expected:
                    raise ValueError("invalid environment_windows.windows." + key)
        window.setdefault("window_id", "post_match_window_%d" % (position + 1))
        window.setdefault("window_index", position)
        window.setdefault("trigger_anchor", "fresh_match" if position == 0 else "previous_restore_confirmed")
        window.setdefault("trigger_offset_sec", value.get("anchor_offset_sec", 0.0)
                          if position == 0 and anchor == "fresh_match" else 0.0)
        if f2_windows and window["trigger_anchor"] == anchor:
            window["trigger_offset_sec"] = value.get("anchor_offset_sec", 0.0)
        window.setdefault("drop_duration_sec", config.get("drop_duration_sec"))
        window.setdefault("restore_interval_sec", value.get("restore_interval_sec", 0.0))
        windows.append(window)
    return _validate_explicit_recovery_windows(windows)


def _compose_recovery_inputs(parent, proposals, *, max_seconds=None):
    """Synchronize proposals before compose; never rewrite a finished plan.

    An E01 duration write applies to every inherited window.  Other mutations
    preserve per-window durations/order.  The extra top-level changes are part
    of the same operation's input_after hash, not a post-compose repair.
    """
    from dataclasses import replace
    from r2f.core.mutation_plan import _apply_changes
    from r2f.runtime.namespace_pair import validate_reader_churn_config

    current = json.loads(json.dumps(parent))
    if "environment_windows" in current:
        _materialize_recovery_windows(current)  # reject invalid inherited plans
    rebased = []
    origin = "single_window_default"
    for proposal in proposals:
        changes = dict(proposal.changes)
        after = _apply_changes(current, changes)
        explicit = proposal.runtime_windows
        if explicit:
            _, resolved = _recovery_windows(after.get("drop_duration_sec"), explicit)
            origin = "m07_explicit"
        elif "environment_windows" in after:
            resolved = _materialize_recovery_windows(after)
            origin = "final_environment_windows"
        else:
            resolved = None
        if resolved is not None:
            if "drop_duration_sec" in changes:
                resolved = [dict(window, drop_duration_sec=after["drop_duration_sec"])
                            for window in resolved]
            # Replace the declarative mapping/dotted writes with one concrete
            # submitted list, including its mutation in the operation chain.
            resolved = _validate_explicit_recovery_windows(resolved)
            changes = {key: value for key, value in changes.items()
                       if not key.startswith("environment_windows.")}
            changes["environment_windows"] = resolved
            after = _apply_changes(current, changes)
        rebased.append(replace(proposal, input_before=current, input_after=after,
                               changes=changes, runtime_windows=[]))
        current = after
    explicit = current.get("environment_windows") if "environment_windows" in current else None
    built = _recovery_windows(
        current.get("drop_duration_sec"), explicit,
        recovery_timeout_sec=current.get("recovery_timeout_sec"), max_seconds=max_seconds)
    core, runtime = ([built[0]], [built[1]]) if isinstance(built[0], dict) else built
    # Validate every stimulus against every window, not just the first window
    # used by the runtime's historical input checker.
    for window in runtime:
        try:
            validate_reader_churn_config(dict(current, runtime_windows=[window]))
        except RuntimeError as exc:
            raise ValueError(str(exc)) from exc
    return rebased, core, runtime, origin


def _producer_identity():
    pid = os.getpid()
    try:
        start = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[19]
    except (OSError, IndexError):
        start = None
    return {"pid": pid, "start_ticks": start}


def _producer_active(owner):
    """True/False/None: unknown owner identity is not permission to resume."""
    if (not isinstance(owner, dict) or type(owner.get("pid")) is not int
            or owner["pid"] <= 0 or not isinstance(owner.get("start_ticks"), str)):
        return None
    try:
        start = Path(f"/proc/{owner['pid']}/stat").read_text().rsplit(") ", 1)[1].split()[19]
    except FileNotFoundError:
        return False
    except (OSError, IndexError):
        return None
    return start == owner["start_ticks"]


def _persist_topology_exploratory_report(output_dir, cycle, registry):
    """Use the existing exploratory report/ledger schema, never required seal."""
    from r2f.reports.candidate_report import exploratory_candidate_ledger
    from r2f.reports.run_report import (EXPLORATORY_SCHEMA, exploratory_report_digest,
                                       audit_exploratory_report, _topology_artifact_qualification)
    target = registry.get_target(TOPOLOGY_TARGET)
    from r2f.core.mutators import TOPOLOGY_MUTATOR_SPECS
    mutators = {TOPOLOGY_MUTATOR: TOPOLOGY_MUTATOR_SPECS[TOPOLOGY_MUTATOR].to_dict()}
    model = {"catalog_digest": catalog_digest(), "mutator_catalog": mutators,
             "mutator_catalog_sha256": canonical_hash(mutators)}
    report = {"schema_version": EXPLORATORY_SCHEMA, "campaign_kind": "exploratory",
              "run_id": cycle["run_id"], "profile": cycle["profile"],
              "source_manifest": cycle["source_manifest"], "model": model,
              "identity": {"profile_sha256": canonical_hash(cycle["profile"]),
                           "source_sha256": canonical_hash(cycle["source_manifest"]),
                           "model_sha256": canonical_hash(model), "rule_sha256": canonical_hash(target.rule_contract),
                           "target_sha256": canonical_hash(target.to_dict())},
              "required_catalog_sha256": required_catalog_hash(), "rows": cycle["rows"],
              "budget": cycle["budget"], "last_state": cycle["last_state"], "abort": cycle["abort"],
              **{key: deepcopy(cycle[key]) for key in ("cycle_budget", "resume_origin", "row_sequences")
                 if key in cycle},
              "runtime_boundary": cycle["runtime_boundary"], "runtime_formal": cycle["runtime_formal"],
              "control_references": [], "review_records": []}
    eligible, exclusions = [], []
    for row in cycle["rows"]:
        if (row.get("generation_status") == "rejected" and row.get("actual_run") is False
                and row.get("execution_stage") == "planned"):
            # Generation is audited separately, never an execution exclusion or
            # natural-candidate input. The standalone row verifier reopens it.
            continue
        try:
            qualified = _topology_artifact_qualification(output_dir, row, report)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            qualified = {"real_ros_execution": False, "qualification_errors": [str(exc)]}
        if qualified["real_ros_execution"]:
            eligible.append(row)
        else:
            exclusions.append({"cell_id": row.get("cell_id"), "actual_run": row.get("actual_run"),
                               "execution_stage": row.get("execution_stage"), "raw_path": row.get("raw_path"),
                               "raw_sha256": row.get("raw_sha256"),
                               "qualification_errors": qualified["qualification_errors"]})
    ledger = exploratory_candidate_ledger(eligible)
    if exclusions:
        ledger["execution_exclusions"] = exclusions
    _write_immutable_json(output_dir / "candidate_ledger.json", ledger)
    report.update(candidate_ledger_path="candidate_ledger.json", candidate_ledger_sha256=canonical_hash(ledger))
    report["report_sha256"] = exploratory_report_digest(report)
    path = output_dir / "report.json"
    report["audit"] = audit_exploratory_report(path, report=report)
    _write_immutable_json(path, report)
    return path


def _persist_exploratory_report(output_dir, cycle, registry):
    """One report/ledger protocol; preserve the existing topology contract."""
    if cycle["profile"].get("runtime_capability") == TOPOLOGY_CAPABILITY:
        return _persist_topology_exploratory_report(output_dir, cycle, registry)
    from r2f.reports.exploratory_builder import persist_f1_exploratory_report
    return persist_f1_exploratory_report(output_dir, cycle, registry)


def _terminal_feedback_aggregate(path, cycle, registry, *, output_dir, repo):
    """Optional terminal accounting only; failures never rewrite run/abort facts.

    Import only events belonging to this invocation's rows, not a checkpoint's
    inherited feedback prefix. Per-run offsets are assigned after that filter;
    no global log is injected into scheduler/corpus or input identity.
    """
    reference = {"path": str(path), "status": "error"}
    try:
        from .feedback_aggregate import feedback_compatibility
        from r2f.core.state_signature import STATE_ABSTRACTION_VERSION, observation_edge
        from r2f.core.topology_state import SCHEMA as topology_schema
        destination = Path(path).absolute()
        reference["path"] = str(destination)
        resolved = destination.resolve()
        if resolved == output_dir or output_dir in resolved.parents:
            raise ValueError("feedback aggregate must be outside this run output tree")
        if (resolved == repo or repo in resolved.parents) and not (repo / "results" in resolved.parents):
            raise ValueError("feedback aggregate inside repository must be under results")
        source = cycle["source_manifest"]
        if source != source_manifest(repo):
            raise ValueError("feedback aggregate source changed after execution")
        targets = cycle["profile"].get("target_ids")
        if not isinstance(targets, list) or len(targets) != 1:
            raise ValueError("feedback aggregate requires exactly one target; grouped multi-target unsupported")
        target = registry.get_target(targets[0])
        contract = target.rule_contract
        rule = contract.get("primary_rule")
        if targets[0] == G01_TARGET and target.target_revision == 1:
            from r2f.modules.s1.oracles import COMPATIBILITY_RULE_ID
            compatibility = contract.get("compatibility_rule")
            if compatibility != COMPATIBILITY_RULE_ID or rule not in (None, compatibility):
                raise ValueError("feedback aggregate G01 compatibility rule mismatch")
            rule = compatibility
        if not isinstance(rule, str) or not rule:
            raise ValueError("feedback aggregate requires one explicit primary rule")
        if "rule_ids" in contract and contract["rule_ids"] != [rule]:
            raise ValueError("feedback aggregate multiple rules unsupported")
        if "rules" in contract and (not isinstance(contract["rules"], list)
                or len(contract["rules"]) != 1 or contract["rules"][0].get("id") != rule):
            raise ValueError("feedback aggregate rule contract ambiguous")
        if targets[0] == TOPOLOGY_TARGET and cycle["profile"].get("runtime_capability") == TOPOLOGY_CAPABILITY:
            state_schema = topology_schema
        elif targets[0] in _F1_CYCLE_TARGETS:
            state_schema = STATE_ABSTRACTION_VERSION
        else:
            raise ValueError("feedback aggregate state signature not explicitly supported")
        gate = feedback_compatibility(source, target_id=targets[0], target_revision=target.target_revision,
                                      rule_id=rule, rule_sha256=canonical_hash(contract),
                                      state_signature_version=state_schema)
        if cycle.get("execute") is not True:
            return dict(reference, status="skipped", reason_code="planned_cycle_no_feedback_import")
        canonical = FeedbackLog.from_dict(cycle["feedback"])
        rows = {}
        for row in cycle["rows"]:
            if row.get("target_id") != targets[0] or row.get("target_revision") != target.target_revision:
                raise ValueError("feedback aggregate row target/revision mismatch")
            if row.get("rule_id") not in (None, rule):
                raise ValueError("feedback aggregate row rule mismatch")
            if row.get("source_manifest", source) != source:
                raise ValueError("feedback aggregate row source mismatch")
            if row.get("target") is not None and canonical_hash(row["target"]["rule_contract"]) != gate["rule_sha256"]:
                raise ValueError("feedback aggregate row rule contract mismatch")
            content = row.get("content_sha256")
            if content is not None:
                if content in rows:
                    raise ValueError("feedback aggregate ambiguous run-local content identity")
                rows[content] = row
        selected = []
        seen = set()
        for event in canonical.events:
            row = rows.get(event["content_sha256"])
            if row is None:
                continue  # retained checkpoint prefix, not this invocation
            if event["content_sha256"] in seen:
                raise ValueError("feedback aggregate ambiguous repeated run-local observation")
            seen.add(event["content_sha256"])
            if event["executed"] != (row.get("actual_run") is True or row.get("workspace_executed") is True):
                raise ValueError("feedback aggregate row execution flag mismatch")
            if event["edge_id"] is not None:
                observed = observation_edge(row)
                provenance = event["edge_provenance"]["observation"]
                if (observed is None or observed[1] != event["edge_id"]
                        or provenance.get("raw_path") != row.get("raw_path")
                        or provenance.get("raw_sha256") != row.get("raw_sha256")):
                    raise ValueError("feedback aggregate edge not bound to this run row")
            selected.append(event)
        local_feedback = FeedbackLog.from_events(selected).to_dict()
        _, receipt = FeedbackLog.merge_partition_file(destination, compatibility=gate,
                                                      run_id=cycle["run_id"], feedback=local_feedback)
        return dict(reference, status="merged", sha256=receipt["aggregate_file_sha256"],
                    aggregate_identity=receipt["aggregate_identity"], compatibility=gate,
                    statistics=receipt["statistics"], import_receipt=receipt,
                    inherited_feedback_events_excluded=len(canonical.events) - len(selected))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, RuntimeError) as exc:
        # Not an execution abort: original reason, rows and feedback are intact.
        return dict(reference, reason_code="feedback_aggregate_failed", detail=str(exc),
                    original_abort_preserved=True)


def run_exploratory_cycle(output_dir, *, corpus, feedback, scheduler, profile, runtime=None,
                          execute=False, repo=None, run_id=None, max_inputs=None,
                          domain_id=173, continuation=None, checkpoint_hook=None,
                          runtime_boundary=HOST_RUNTIME, feedback_aggregate=None,
                          auto_replay=None, replay_runtime=None, recipe_releases=None,
                          expected_source_manifest=None):
    """Run one bounded feedback-driven exploratory cycle.

    ``execute=False`` performs no runtime call: rows stay ``planned`` and no
    feedback is recorded.  ``execute=True`` requires a runtime callable; the
    carrier dispatches the plan-bearing v2 identity, so the runner passes the
    same single digest it wrote to the capsule and runtime config.
    An optional expected_source_manifest pins the entire measured intake source;
    it does not replace the existing frozen-source or later drift checks.
    auto_replay/replay_runtime use the same bounded policy as run_campaign,
    after both original report and terminal cycle have been sealed.
    """
    if not isinstance(corpus, Corpus):
        raise ExploratoryCycleError("exploratory cycle requires a Corpus")
    if not isinstance(feedback, FeedbackLog):
        raise ExploratoryCycleError("exploratory cycle requires a FeedbackLog")
    if not isinstance(scheduler, ExploratoryScheduler):
        raise ExploratoryCycleError("exploratory cycle requires an ExploratoryScheduler")
    if scheduler.feedback_event_count != len(feedback.events):
        raise ExploratoryCycleError("scheduler feedback prefix does not match the supplied log")
    if scheduler.feedback_identity_at_start != feedback.identity():
        raise ExploratoryCycleError("scheduler feedback identity drift")
    if runtime_boundary not in RUNTIME_BOUNDARIES:
        raise ExploratoryCycleError("unsupported runtime_boundary: " + repr(runtime_boundary))
    if execute and not callable(runtime):
        raise ExploratoryCycleError("execute=True requires a runtime callable")
    formal_runtime = False
    if execute:
        try:
            runtime, formal_runtime = _resolve_execution_runtime(runtime, runtime_boundary)
        except ValueError as exc:
            raise ExploratoryCycleError(str(exc)) from exc
    profile = _load_profile(profile)
    if routes_enabled(profile) or any(has_routes(entry) for entry in corpus.entries.values()):
        try:
            for entry in corpus.entries.values():
                validate_route_entry(entry, profile=profile)
        except ValueError as exc:
            raise ExploratoryCycleError(str(exc)) from exc
    declared_budget = profile.get("budget", {})
    if not isinstance(declared_budget, dict):
        raise ExploratoryCycleError("profile budget must be an object")
    max_inputs = declared_budget.get("max_inputs", 1) if max_inputs is None else max_inputs
    if type(max_inputs) is not int or max_inputs < 1:
        raise ExploratoryCycleError("max_inputs must be a positive integer")
    max_seconds = declared_budget.get("max_seconds")
    if max_seconds is not None and (type(max_seconds) not in (int, float)
            or not math.isfinite(max_seconds) or max_seconds <= 0):
        raise ExploratoryCycleError("budget.max_seconds must be finite and positive")
    warmup = declared_budget.get("warmup_inputs", scheduler.warmup)
    if type(warmup) is not int or warmup < 0:
        raise ExploratoryCycleError("budget.warmup_inputs must be a nonnegative integer")
    if scheduler.selection_log and scheduler.warmup != warmup:
        raise ExploratoryCycleError("cannot change warmup after selection")
    scheduler.warmup = warmup
    if continuation is not None and not isinstance(continuation, dict):
        raise ExploratoryCycleError("continuation must be an object")
    continuation = deepcopy(continuation or {})
    if checkpoint_hook is not None and not callable(checkpoint_hook):
        raise ExploratoryCycleError("checkpoint_hook must be callable")
    started = time.monotonic()
    _sync_sampling(scheduler, profile)
    if profile.get("scheduler_policy", scheduler.policy) != scheduler.policy:
        raise ExploratoryCycleError("scheduler policy differs from profile declaration")
    if scheduler.policy == "bfs":
        topology_descriptor = None
        if "topology_search" in profile:
            from .topology_search import descriptor
            topology_descriptor = descriptor(profile["topology_search"], profile["operator_whitelist"])
        scheduler.configure_search(profile.get("search"), topology_descriptor)
    elif "search" in profile:
        raise ExploratoryCycleError("search profile requires bfs scheduler policy")
    sampling_kinds, sampling_weight, chain_length = _profile_wide_settings(profile)

    repo = Path(repo or Path(__file__).resolve().parents[3]).resolve()
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ExploratoryCycleError("exploratory cycle output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or "s1f-" + uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", run_id):
        raise ExploratoryCycleError("invalid run_id")

    source = source_manifest(repo)
    _validate_expected_source_manifest(source, expected_source_manifest)
    registry = Registry()
    register_exploratory(registry, include_required=False,
                         runtime_capability=profile.get("runtime_capability"))
    manifest = {
        "schema_version": 1, "campaign_kind": "exploratory", "run_id": run_id,
        "profile": profile, "profile_id": profile["profile_id"],
        "source_manifest": source, "requested_domain": domain_id,
        "catalog_digest": catalog_digest(),
        "required_catalog_sha256": required_catalog_hash(),
        "target_ids": list(profile["target_ids"]),
        "exploration_order": list(EXPLORATORY_TARGET_ORDER) + (
            [F2_WINDOWS_TARGET] if profile.get("runtime_capability") == F2_WINDOWS_CAPABILITY else
            [TOPOLOGY_TARGET] if profile.get("runtime_capability") == TOPOLOGY_CAPABILITY else []),
        "input_identity_schema": INPUT_SCHEMA_V2,
        "required_matrix_changed": False, "maturity_denominator_changed": False,
        "execution_boundary": profile.get("execution_boundary"),
        "runtime_boundary": runtime_boundary,
    }
    _atomic_json(output_dir / "manifest.json", manifest)

    counters = ("selected", "planned", "generated", "attempted", "actual",
                "executed", "aborted", "interrupted")
    previous = continuation.get("budget", {})
    if not isinstance(previous, dict):
        raise ExploratoryCycleError("continuation budget must be an object")
    budget = {}
    for key in counters:
        value = previous.get(key, 0)
        if type(value) is not int or value < 0:
            raise ExploratoryCycleError("invalid continuation budget " + key)
        budget[key] = value
    elapsed_before = previous.get("elapsed_sec", 0.0)
    if (type(elapsed_before) not in (int, float) or not math.isfinite(elapsed_before)
            or elapsed_before < 0):
        raise ExploratoryCycleError("invalid continuation elapsed_sec")
    seen, seen_actual = set(), set()
    for key, destination in (("seen_input_hashes", seen), ("seen_actual_hashes", seen_actual)):
        values = continuation.get(key, [])
        if not isinstance(values, list) or any(not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{64}", v) for v in values):
            raise ExploratoryCycleError("invalid continuation " + key)
        destination.update(values)
    if not seen_actual <= seen:
        raise ExploratoryCycleError("actual input hashes must be selected input hashes")
    initial_counts = dict(budget)
    budget.update(max_inputs=max_inputs, max_seconds=max_seconds,
                  warmup_inputs=warmup, unique_inputs=len(seen), unique_actual=len(seen_actual))
    rows, rejected, row_sequences = [], [], []
    pending = continuation.get("pending_selection")
    if pending is not None:
        if (not isinstance(pending, dict) or not isinstance(pending.get("entry"), dict)
                or not scheduler.selection_log
                or pending.get("selection_sha256") != scheduler.selection_log[-1].get("selection_sha256")
                or pending.get("sequence") != scheduler.selection_log[-1].get("sequence")):
            raise ExploratoryCycleError("pending selection is not the last accepted selection")
    last_case_ref = continuation.get("last_case_ref")
    submission_state = continuation.get("submission_state")
    producer = _producer_identity()
    resume_uncertainty = None
    if pending is not None:
        if submission_state != "prepared" or not isinstance(last_case_ref, dict):
            resume_uncertainty = "pending_submission_state_or_owner_unknown"
        else:
            try:
                old_case = Path(last_case_ref["case_dir"]).resolve()
                old_output = Path(last_case_ref["output_dir"]).resolve()
                marker = old_case / "submission_intent.json"
                runtime_dir = old_case / "runtime"
                if (old_case != old_output / "cases" / last_case_ref["cell_id"]
                        or last_case_ref["cell_id"] != pending.get("cell_id", last_case_ref["cell_id"])
                        or not (old_case / "row.json").is_file()
                        or marker.exists() or (old_case / "submission.json").exists()
                        or runtime_dir.exists()):
                    resume_uncertainty = "pending_has_submission_runtime_or_unknown_case"
                old_row = json.loads((old_case / "row.json").read_text())
                old_capsule = json.loads((old_case / "capsule.json").read_text())
                if (old_row.get("execution_stage") != "planned" or old_row.get("actual_run") is not False
                        or old_row.get("workspace_executed") is True
                        or old_capsule.get("execution") is not None or old_capsule.get("execution_history")):
                    resume_uncertainty = "pending_has_execution_evidence"
                if continuation.get("execute_requested") is not False:
                    if _producer_active(continuation.get("producer")) is not False:
                        resume_uncertainty = "pending_runtime_owner_alive_or_unknown"
            except (KeyError, TypeError, OSError, ValueError):
                resume_uncertainty = "pending_case_owner_cannot_be_verified"
    cycle_ids = {entry.get("cycle_id") for entry in corpus.entries.values() if entry.get("cycle_id")}
    cycle_id = continuation.get("cycle_id") or (next(iter(cycle_ids)) if len(cycle_ids) == 1 else None)
    resume_origin = None
    if continuation.get("resumed_from") is not None:
        # Keep a self-contained, immutable origin; never depend on an old run's
        # mutable pathname when auditing this continuation later.
        from .checkpoint import SCHEMA as checkpoint_schema, _identity, _validate_state
        reference = continuation["resumed_from"]
        origin = json.loads(Path(reference["path"]).read_text())
        identity, seeds, origin_registry = _identity(profile, cycle_id, source)
        if (origin.get("schema") != checkpoint_schema
                or origin.get("checkpoint_sha256") != reference.get("checkpoint_sha256")
                or origin.get("checkpoint_sha256") != canonical_hash({
                    k: v for k, v in origin.items() if k != "checkpoint_sha256"})
                or origin.get("identity") != identity):
            raise ExploratoryCycleError("resume origin checkpoint identity mismatch")
        _validate_state(origin["state"], identity, seeds, origin_registry)
        expected_continuation = deepcopy(continuation)
        expected_continuation.pop("resumed_from")
        for key, value in (("corpus", corpus.to_dict()), ("feedback", feedback.to_dict()),
                           ("scheduler", scheduler.to_dict()),
                           ("continuation", expected_continuation)):
            saved_value = deepcopy(origin["state"][key])
            if key == "continuation":
                saved_value.pop("resumed_from", None)
            if saved_value != value:
                raise ExploratoryCycleError("resume origin state mismatch:" + key)
        _atomic_json(output_dir / "resume-origin-checkpoint.json", origin)
        resume_origin = {"path": "resume-origin-checkpoint.json",
                         "checkpoint_sha256": origin["checkpoint_sha256"]}

    # Apply owner decisions only after validating the untouched resume origin.
    # They are append-only feedback events, not an origin/identity exemption.
    if recipe_releases is not None:
        if not isinstance(recipe_releases, list):
            raise ExploratoryCycleError("recipe releases must be a list of named owner decisions")
        for release in recipe_releases:
            if not isinstance(release, dict) or set(release) != {"recipe", "owner", "reason", "revalidation_basis"}:
                raise ExploratoryCycleError("incomplete recipe owner release")
            feedback.release_recipe(**release)
        scheduler.advance_feedback(feedback)

    def update_elapsed():
        budget["elapsed_sec"] = elapsed_before + max(0.0, time.monotonic() - started)
        return max_seconds is not None and budget["elapsed_sec"] >= max_seconds

    def checkpoint(phase):
        update_elapsed()
        if checkpoint_hook is not None:
            checkpoint_hook({
                "schema": "r2f.exploratory-checkpoint-state/v1", "phase": phase,
                "run_id": run_id, "cycle_id": cycle_id, "profile": profile,
                "source_manifest": source, "corpus": corpus.to_dict(),
                "feedback": feedback.to_dict(), "scheduler": scheduler.to_dict(),
                "budget": deepcopy(budget), "seen_input_hashes": sorted(seen),
                "seen_actual_hashes": sorted(seen_actual),
                "next_selection_sequence": len(scheduler.selection_log),
                "pending_selection": deepcopy(pending), "last_case_ref": deepcopy(last_case_ref),
                "submission_state": submission_state, "execute_requested": bool(execute),
                "producer": producer,
                "target_manifest": [registry.get_target(t).to_dict() for t in profile["target_ids"]],
                "required_catalog_sha256": required_catalog_hash(),
                "row_count": len(rows), "stop_reason": stop_reason,
            })

    from .topology_search import selection_metadata

    def planned(row, selection):
        nonlocal pending, last_case_ref, submission_state
        if scheduler.policy == "bfs":
            from .search_frontier import derived_record
            from r2f.core.mutation_plan import MutationPlan
            record = derived_record(selection["entry"], MutationPlan.from_dict(row["mutation_plan"]))
            if record["content_sha256"] != row["content_sha256"]:
                raise ExploratoryCycleError("frontier/capsule content identity mismatch")
            scheduler.frontier.planned(selection["task_id"], record)
            row["input_search"] = {"mode": "input_search_bfs", "task_id": selection["task_id"],
                "baseline_content_sha256": selection["baseline_content_sha256"],
                "parent_content_sha256": selection["parent_content_sha256"],
                **(selection_metadata(selection)
                   if selection.get("task_kind") == "topology_request" else {
                       "mutation_route": deepcopy(selection["mutation_route"]), "sampling_kind": selection["sampling_kind"]}),
                "derivation_depth": selection["derivation_depth"], "sample_slot": selection["sample_slot"],
                "rng_seed": selection["rng_seed"], "status": "planned", "actual": False, "execution": None}
            if selection.get("chain_schema"):
                row["topology_chain_accounting"] = deepcopy(scheduler.frontier.task(selection["task_id"])["chain_accounting"])
            _atomic_json(output_dir / "cases" / row["cell_id"] / "row.json", row)
        pending = deepcopy(selection)
        case = output_dir / "cases" / row["cell_id"]
        last_case_ref = {"cell_id": row["cell_id"], "output_dir": str(output_dir),
                         "case_dir": str(case), "row_path": str(case / "row.json"),
                         "runtime_dir": str(case / "runtime"), "capsule_path": str(case / "capsule.json"),
                         "runtime_config_path": str(case / "runtime_config.json"),
                         "submission_marker_path": str(case / "submission_intent.json")}
        submission_state = "prepared"
        digest = row.get("content_sha256")
        if digest:
            seen.add(digest)
        budget["unique_inputs"] = len(seen)
        checkpoint("planned")

    def submitted(row, selection):
        nonlocal submission_state
        submission_state = "submitted"
        if scheduler.policy == "bfs":
            scheduler.frontier.submitted(selection["task_id"])
        _atomic_json(Path(last_case_ref["submission_marker_path"]), {
            "schema": "r2f.exploratory-submission/v1", "status": "submitted_or_uncertain",
            "cell_id": row["cell_id"], "sequence": selection["sequence"],
            "content_sha256": row["content_sha256"], "execute_requested": True,
            "owner": producer, "selection_sha256": selection["selection_sha256"],
            "effective_input_sha256": row["effective_input_sha256"],
            "source_manifest": source, "case_ref": deepcopy(last_case_ref),
            "actual_run_claimed": False}, durable=True)
        checkpoint("planned")

    abort = None
    stop_reason = None
    if resume_uncertainty is not None:
        abort = {"reason_code": "resume_pending_execution_uncertain", "detail": resume_uncertainty,
                 "last_case_ref": deepcopy(last_case_ref)}
        stop_reason = abort["reason_code"]
        manifest["abort"] = abort
        _atomic_json(output_dir / "manifest.json", manifest)
    if not abort and formal_runtime and (
            source.get("source_frozen") is not True
            or (runtime_boundary == ISOLATED_RUNTIME or profile.get("runtime_capability") == TOPOLOGY_CAPABILITY) and (
                source.get("dirty_behavior_paths") != [] or not source.get("head")
                or not source.get("behavior_tree_sha256"))):
        abort = {"reason_code": "source_behavior_not_tracked_and_clean_at_HEAD",
                 "detail": "real runtime requires tracked behavior clean at HEAD"}
        stop_reason = abort["reason_code"]
        manifest["abort"] = abort
        _atomic_json(output_dir / "manifest.json", manifest)
    last_state = {"planned": budget["planned"], "executed": budget["executed"],
                  "cell_id": None, "sequence": None}

    def reject(sequence, entry, detail):
        row = {
            "cell_id": f"{run_id}.{sequence:04d}.rejected",
            "run_id": run_id, "target_id": entry.get("target_id"),
            "target_revision": entry.get("target_revision"),
            "profile_id": profile["profile_id"], "rmw": entry.get("rmw"),
            "campaign_kind": "exploratory", "arm": "mutated",
            "execution_stage": "planned", "actual_run": False,
            "oracle_verdict": "not_applicable", "control_kind": None,
            "mutation_plan": None, "input_layer_receipts": [],
            "binding": {"status": "not_executed", "silent_normalization": False,
                        "detail": detail},
            "generation_status": "rejected",
            "reason_code": detail,
        }
        rejected.append(row)
        budget["planned"] += 1
        return row

    while not abort and (pending is not None or budget["selected"] < max_inputs):
        if update_elapsed():
            stop_reason = "cycle_deadline_reached"
            break
        if scheduler.policy != "bfs" and not corpus.selectable():
            stop_reason = "selection_exhausted"
            break
        try:
            if pending is not None:
                selection = deepcopy(pending)
                # Regenerate the already persisted plan in the new output tree;
                # this is not a second selection or a second planned input.
                budget["planned"] -= 1
                budget["generated"] -= 1
            else:
                selection = scheduler.next()
                budget["selected"] += 1
        except ValueError as exc:
            stop_reason = (scheduler.frontier.stop_reason if scheduler.policy == "bfs"
                           and scheduler.frontier.stop_reason else "selection_error:" + str(exc))
            break
        sequence = selection["sequence"]
        entry = selection["entry"]
        if has_routes(entry):
            try:
                validate_route_selection(entry, selection, profile=profile)
            except ValueError as exc:
                raise ExploratoryCycleError(str(exc)) from exc
        target_id = entry.get("target_id")
        rmw = entry.get("rmw")

        if target_id not in manifest["exploration_order"]:
            row = reject(sequence, entry, "target_not_in_exploratory_catalog")
        elif target_id not in ({TOPOLOGY_TARGET} if profile.get("runtime_capability") == TOPOLOGY_CAPABILITY
                               else _F2_CYCLE_TARGETS if profile.get("runtime_capability") == F2_WINDOWS_CAPABILITY
                               else _F1_CYCLE_TARGETS | {G01_TARGET}):
            row = reject(sequence, entry,
                         "unsupported_capability:f1_cycle_target_not_enabled")
        elif rmw not in profile["rmws"]:
            row = reject(sequence, entry, "rmw_not_in_profile")
        else:
            target = registry.get_target(target_id)
            contract = target.exploration_contract
            if contract is None:
                raise ExploratoryCycleError(target_id + ": no exploration contract")
            if entry.get("contract_hash") != contract.contract_hash():
                row = reject(sequence, entry, "contract_hash_mismatch")
            else:
                surface_id = entry.get("surface_id")
                surface = contract.surface_by_id.get(surface_id)
                if surface is None:
                    row = reject(sequence, entry, "surface_not_in_contract:" + str(surface_id))
                elif surface_id not in profile["surfaces"]:
                    row = reject(sequence, entry, "surface_not_in_profile:" + str(surface_id))
                else:
                    parent = entry.get("final_config")
                    if not isinstance(parent, dict) or not parent:
                        raise ExploratoryCycleError(
                            "corpus entry requires a nonempty parent final_config")
                    f2_windows = profile.get("runtime_capability") == F2_WINDOWS_CAPABILITY
                    mutator_ids = (["M08"] if target_id == G01_TARGET and surface_id == "G01" else
                                   [TOPOLOGY_MUTATOR] if target_id == TOPOLOGY_TARGET
                                   and profile.get("runtime_capability") == TOPOLOGY_CAPABILITY
                                   else [F2_SURFACE_MUTATORS[surface_id]] if f2_windows
                                   and surface_id in F2_SURFACE_MUTATORS
                                   and F2_SURFACE_MUTATORS[surface_id] in surface.mutator_ids
                                   else [mid for mid in surface.mutator_ids if mid in MUTATORS])
                    # A sampled-range boundary (e.g. E01 log_range) is M03;
                    # endpoint boundaries are M01.  Never feed a sampling class
                    # to M01 or silently use the first mutator.
                    boundary = entry.get("boundary_class")
                    preferred = ("M03" if boundary in {"log_range", "sampled"} else "M01")
                    if boundary in {"zero", "finite", "infinite", "quantized_neighbor"} and "M01" in mutator_ids:
                        preferred = "M01"
                    requested = entry.get("mutator_id")
                    # A corpus template's default M01 is not an explicit user
                    # override for a sampled-range boundary; prefer M03 there.
                    if preferred in mutator_ids and boundary in {"log_range", "sampled"}:
                        mutator_id = preferred
                    else:
                        mutator_id = requested if requested in mutator_ids else (
                            preferred if preferred in mutator_ids else (mutator_ids[0] if mutator_ids else None))
                    if has_routes(entry):
                        # Explicit route mutators cannot be replaced by a catalog
                        # default or a boundary/sampling preference.
                        mutator_id = entry["mutation_route"]["mutator_id"]
                    row = None
                    wide_profile = _wide_generation_requested(profile) and target_id not in (TOPOLOGY_TARGET, G01_TARGET)
                    declared_kinds, _, declared_depth = _profile_wide_settings(profile)
                    if wide_profile and declared_kinds is not None:
                        supported = set(_wide_kinds(surface, parent))
                        if has_routes(entry) and selection.get("sampling_error"):
                            row = reject(
                                sequence, entry,
                                "unsupported_capability:no_declared_sampling_kind_for_mutation_route")
                        elif not [kind for kind in declared_kinds if kind in supported]:
                            row = reject(
                                sequence, entry,
                                "unsupported_capability:no_declared_sampling_kind_for_surface")
                        elif _reject_all_zero_supported(profile, surface, parent):
                            row = reject(
                                sequence, entry,
                                "unsupported_capability:all_supported_sampling_kinds_zero_weight")
                    if row is None and wide_profile and declared_depth > 1:
                        try:
                            if has_routes(entry):
                                _declared_route_layers(entry, contract, declared_depth)
                            else:
                                _chain_surfaces(contract, surface, declared_depth, profile["surfaces"])
                        except ExploratoryCycleError as exc:
                            row = reject(sequence, entry, str(exc))
                    if row is None:
                        if not mutator_ids:
                            row = reject(sequence, entry,
                                         "unsupported_capability:no_implemented_mutator")
                        elif mutator_id not in mutator_ids:
                            row = reject(sequence, entry, "mutator_not_declared:" + str(mutator_id))
                        else:
                            try:
                                row = _generate_case(sequence, entry, target, contract, surface,
                                                    parent, mutator_id, target_id, rmw, selection,
                                                    scheduler, source, repo, run_id, profile,
                                                    domain_id, registry, output_dir, budget,
                                                    execute, runtime, corpus, feedback,
                                                    planned_hook=planned, submission_hook=submitted,
                                                    deadline_reached=update_elapsed,
                                                    runtime_boundary=runtime_boundary)
                            except _TopologyInputRejection as exc:
                                if selection.get("task_kind") == "topology_request":
                                    scheduler.frontier.generation_rejected(selection["task_id"], str(exc), exc.attempts)
                                row = reject(sequence, entry, "topology_input_rejected:" + str(exc))
                                row.update(topology_generation_attempts=exc.attempts,
                                           candidate_eligible=False, attribution="harness_input_generation")
                            except _F2InputRejection as exc:
                                row = reject(sequence, entry, str(exc))
                            except _NoEffectiveInputRejection as exc:
                                # A generated proposal is auditable, but has no
                                # prepared capsule, submission or execution.
                                from .search_frontier import derived_record
                                record = derived_record(entry, exc.plan)
                                row = reject(sequence, entry, "model_guard_terminal" if str(exc) == "topology_guard_terminal" else "no_effective_input")
                                row.update(mutation_plan=exc.plan.to_dict(),
                                           content_sha256=record["content_sha256"],
                                           parent_content_sha256=entry["content_sha256"],
                                           no_effective_input_reason=str(exc),
                                           candidate_eligible=False, feedback_eligible=False)
                                if target_id == G01_TARGET:
                                    # Rejected proposals have no prepared capsule/config.
                                    # Retain the proposal explicitly, not as an executed plan.
                                    row["rejected_mutation_plan"] = row.pop("mutation_plan")
                                    row["mutation_plan"] = None
                                    row["rejected_mutation_plan_sha256"] = exc.plan.plan_hash()
                                if scheduler.policy == "bfs":
                                    scheduler.frontier.planned(selection["task_id"], record)
                                case_dir = output_dir / "cases" / row["cell_id"]
                                _atomic_json(case_dir / "mutation_plan.json", exc.plan.to_dict())
                                _atomic_json(case_dir / "row.json", row)
        if row is None:
            stop_reason = "generation_failed"
            break
        if scheduler.policy == "bfs":
            actual = row.get("actual_run") is True
            rejected_task = row.get("generation_status") == "rejected"
            if rejected_task and row not in rejected:
                rejected.append(row)
            execution = ({"cell_id": row["cell_id"], "execution_stage": row["execution_stage"],
                          "content_sha256": row["content_sha256"],
                          "effective_input_sha256": row.get("effective_input_sha256"),
                          "evidence_origin": row.get("evidence_origin"), "formal_runtime": bool(formal_runtime),
                          "raw_path": row.get("raw_path"), "raw_sha256": row.get("raw_sha256")}
                         if actual else None)
            scheduler.frontier.finish(selection["task_id"], rejected=rejected_task,
                reason=row.get("reason_code"), actual=actual, execution=execution)
            row["input_search"] = {"mode": "input_search_bfs", "task_id": selection["task_id"],
                "baseline_content_sha256": selection["baseline_content_sha256"],
                "parent_content_sha256": selection["parent_content_sha256"],
                **(selection_metadata(selection)
                   if selection.get("task_kind") == "topology_request" else {
                       "mutation_route": deepcopy(selection["mutation_route"]), "sampling_kind": selection["sampling_kind"]}),
                "derivation_depth": selection["derivation_depth"], "sample_slot": selection["sample_slot"],
                "rng_seed": selection["rng_seed"], "status": "rejected" if rejected_task else "completed",
                "actual": actual, "execution": execution}
        if selection.get("chain_schema"):
            row["topology_chain_accounting"] = deepcopy(scheduler.frontier.task(selection["task_id"])["chain_accounting"])
        row_sequences.append({"cycle_row_index": len(rows), "selection_sequence": sequence,
                              "cell_id": row["cell_id"]})
        rows.append(row)
        if row.get("actual_run") is True or row.get("workspace_executed") is True:
            budget["actual"] += 1
            if row.get("content_sha256"):
                seen_actual.add(row["content_sha256"])
        budget["unique_actual"] = len(seen_actual)
        pending = None
        submission_state = "recorded"
        if not isinstance(last_case_ref, dict) or last_case_ref.get("cell_id") != row["cell_id"]:
            last_case_ref = {"cell_id": row["cell_id"], "output_dir": str(output_dir),
                             "case_dir": str(output_dir / "cases" / row["cell_id"])}
        # Preserve the row annotations before checkpointing accepted feedback.
        _atomic_json(output_dir / "cases" / row["cell_id"] / "row.json", row)
        checkpoint("recorded")
        last_state = {"planned": budget["planned"], "executed": budget["executed"],
                      "cell_id": row["cell_id"], "sequence": sequence,
                      "execution_stage": row["execution_stage"]}
        attempt = row.get("recipe_attempt") or {}
        if attempt.get("outcome") == "global_invalid":
            reason = attempt["diagnostic"]["reason"]
            abort = {"reason_code": (reason if reason == "source_repo_HEAD_or_behavior_tree_changed"
                                     else "global_execution_invalidity"),
                     "detail": reason, "after_row_index": len(rows) - 1}
            stop_reason = abort["reason_code"]
            manifest["abort"] = abort
            _atomic_json(output_dir / "manifest.json", manifest)
            break
        qualification = row.get("boundary_qualification", {})
        if execute and qualification.get("status") in {"unknown", "invalid"}:
            abort = {"reason_code": "execution_boundary_qualification_failed",
                     "qualification_reason": qualification["reason_code"],
                     "after_row_index": len(rows) - 1,
                     "detail": "runtime evidence retained; boundary qualification failed"}
            stop_reason = abort["reason_code"]
            manifest["abort"] = abort
            manifest["boundary_qualification"] = qualification
            _atomic_json(output_dir / "manifest.json", manifest)
            break
        if row.get("abort_reason") == "cycle_deadline_reached":
            stop_reason = "cycle_deadline_reached"
            break
        # Latch the pre-submit drift failure: a subsequent source snapshot
        # returning to its old value cannot turn an aborted row into a live
        # cycle.  No runtime/execution receipt exists on this branch.
        if (row.get("abort_reason") == "source_repo_HEAD_or_behavior_tree_changed"
                or source_manifest(repo) != source):
            abort = {"reason_code": "source_repo_HEAD_or_behavior_tree_changed",
                     "after_row_index": len(rows) - 1,
                     "detail": "source identity changed during the cycle"}
            stop_reason = abort["reason_code"]
            break

    if not execute and stop_reason is None:
        stop_reason = "execute_false_planned_only"
    if stop_reason is None and budget["selected"] >= max_inputs:
        stop_reason = "max_inputs_reached"
    update_elapsed()
    checkpoint("final")
    cycle_budget = {key: budget[key] - initial_counts[key] for key in counters}
    cycle_budget["elapsed_sec"] = budget["elapsed_sec"] - elapsed_before
    result = {
        "schema_version": EXPLORATORY_CYCLE_SCHEMA,
        "campaign_kind": "exploratory",
        "run_id": run_id,
        "profile": profile,
        "profile_id": profile["profile_id"],
        "source_manifest": source,
        "input_identity_schema": INPUT_SCHEMA_V2,
        "manifest_path": str(output_dir / "manifest.json"),
        "rows": rows,
        "rejected": rejected,
        "budget": budget,
        "cycle_budget": cycle_budget,
        "resume_origin": resume_origin,
        "row_sequences": row_sequences,
        "continuation": {"cycle_id": cycle_id, "budget": deepcopy(budget),
                         "seen_input_hashes": sorted(seen), "seen_actual_hashes": sorted(seen_actual),
                         "pending_selection": deepcopy(pending), "last_case_ref": deepcopy(last_case_ref),
                         "submission_state": submission_state, "execute_requested": bool(execute),
                         "producer": producer},
        "last_state": last_state,
        "abort": abort,
        "stop_reason": stop_reason,
        "execute": bool(execute),
        "selection_log": scheduler.to_dict(),
        "feedback": feedback.to_dict(),
        "corpus": corpus.to_dict(),
        "maturity_scope": "exploratory only; required 72 denominator unchanged and not claimed",
        "runtime_boundary": runtime_boundary,
        "runtime_formal": bool(formal_runtime),
    }
    if scheduler.policy == "bfs":
        # Cumulative ledger counts survive resume. Completed input tasks are NOT
        # execution evidence; the novelty corpus intentionally retains less.
        frontier = scheduler.frontier
        tasks = frontier.tasks
        result["search_summary"] = {
            "mode": "input_search_bfs",
            "root_nodes": len(frontier.roots),
            "derived_nodes": len(frontier.nodes) - len(frontier.roots),
            "tasks_total": len(tasks),
            "tasks_pending": sum(t["status"] == "pending" for t in tasks),
            "tasks_in_flight": sum(t["status"] in {"selected", "planned", "submitted"} for t in tasks),
            "proposal_generated": sum(t.get("generated_record") is not None for t in tasks),
            "effective_unique_derived_nodes": len(frontier.nodes) - len(frontier.roots),
            "submitted": sum(e["action"] == "submit" for e in frontier.journal),
            "completed": sum(t["status"] == "completed" for t in tasks),
            "rejected": sum(t["status"] == "rejected" for t in tasks),
            "actual": sum(t.get("actual") is True for t in tasks),
            "executed": sum(t.get("actual") is True and
                            (t.get("execution") or {}).get("execution_stage") == "judged" for t in tasks),
            "duplicate_content": sum(t.get("generation_outcome") == "duplicate_content" for t in tasks),
            "stop_reason": stop_reason,
            "frontier_stop_reason": frontier.stop_reason,
            "retention_role": "novelty_evidence_only",
            "frontier_ref": {"path": str(output_dir / "selection_log.json"), "json_pointer": "/frontier"},
            "count_scope": "cumulative_task_ledger_including_resume",
            "completed_meaning": "terminal_generated_or_planned_task_not_execution",
            "coverage_scope": "declared_routes_and_finite_rng_sample_slots_not_all_continuous_values",
        }
    if runtime_boundary == ISOLATED_RUNTIME:
        result["boundary_qualifications"] = {
            row["cell_id"]: row.get("boundary_qualification", execution_boundary_qualification(runtime_boundary))
            for row in rows}
    for name, value in (("selection_log.json", scheduler.to_dict()),
                        ("feedback.json", feedback.to_dict()), ("corpus.json", corpus.to_dict())):
        try:
            _atomic_json(output_dir / name, value)
        except Exception as exc:
            record_packaging_failure(result, exc, output_dir / name)
    def persist_report():
        if (profile.get("runtime_capability") == TOPOLOGY_CAPABILITY
                or set(profile["target_ids"]).issubset(_F1_CYCLE_TARGETS)):
            path = _persist_exploratory_report(output_dir, result, registry)
            result["report_path"] = str(path)
            return path

    def persist_terminal():
        if feedback_aggregate is not None:
            result["feedback_aggregate"] = _terminal_feedback_aggregate(
                feedback_aggregate, result, registry, output_dir=output_dir, repo=repo)
        _write_immutable_json(output_dir / "cycle.json", result)

    return finish_evidence(output_dir, result, persist_report, source=source,
                           terminal=persist_terminal,
                           automatic_replay={"policy": auto_replay, "runtime": replay_runtime,
                                             "execute": execute, "runtime_boundary": runtime_boundary,
                                             "fixture_original": not formal_runtime})


def _build_case_plan(entry, target, contract, surface, parent, mutator_id,
                     profile, rng_seed, selection):
    """Pure shared generation; input planning is never execution evidence.

    The frontier checkpoint verifier invokes this same generation path from its
    retained parent, declared route and stable sample-slot RNG.
    """
    target_id = target.target_id
    route = _route(target_id)
    if route is None:
        raise ExploratoryCycleError(target_id + ": not in the exploratory route table")
    mutation_kind = target.mutation_slots[0].mutation_kind
    carrier = target.mutation_slots[0].carrier
    if (mutation_kind, carrier) != (route["mutation_kind"], route["carrier"]):
        raise ExploratoryCycleError(
            "route/target contract mismatch for " + target_id + ": target declares "
            f"{mutation_kind}/{carrier}, route declares "
            f"{route['mutation_kind']}/{route['carrier']}")

    adapted_parent, adaptation = _adapt_parent(target_id, route, parent)
    before_controls = canonical_hash(adapted_parent)
    # Action-program profiles have their own validated envelope; pair/F2
    # controls must never be projected into the submitted program identity.
    if target_id != TOPOLOGY_TARGET:
        adapted_parent = apply_profile_controls(
            adapted_parent, profile, backend=route["backend"])
    if CONTROL_FIELDS & set(adapted_parent):
        adaptation["runtime_control_projection"] = {
            "profile_fields": sorted(PROFILE_CONTROL_FIELDS & set(profile)),
            "submitted_fields": sorted(CONTROL_FIELDS & set(adapted_parent)),
            "parent_before_controls_sha256": before_controls,
            "parent_after_controls_sha256": canonical_hash(adapted_parent),
            "normalized_before_compose": True,
        }
    sampling_kinds, sampling_weight, chain_length = _profile_wide_settings(profile)
    topology = target_id == TOPOLOGY_TARGET
    wide = _wide_generation_requested(profile) and not topology and target_id != G01_TARGET
    sampling_kind = selection.get("sampling_kind") if wide else None
    sampling_filtered = []
    if topology:
        if entry.get("executable") is False or entry.get("case_kind") == "guard_negative":
            raise _TopologyInputRejection("topology_guard_seed_non_executable", [])
        if "topology_search" in profile:
            from .topology_search import descriptor, generate, validate_chain_length, task_requests, is_chain, plan_accounting
            desc = descriptor(profile["topology_search"], profile["operator_whitelist"])
            validate_chain_length(profile, desc)
            if selection.get("task_kind") != "topology_request":
                raise _TopologyInputRejection("topology_search requires exact declared task", [])
            try:
                plan = generate(entry, selection, desc)
            except ValueError as exc:
                rejection = _TopologyInputRejection(str(exc), task_requests(selection))
                if is_chain(desc):
                    rejection.chain_accounting = getattr(exc, "accounting", None)
                raise rejection from exc
            from .search_frontier import derived_record
            if derived_record(entry, plan).get("executable") is False:
                raise _NoEffectiveInputRejection("topology_guard_terminal", plan)
            topology_generation = ({"schema": "r2f.topology-explicit-chain-generation/v1",
                "requests": task_requests(selection), "layer_samples": deepcopy(selection["layer_samples"]),
                "chain_accounting": plan_accounting(plan, desc["chain_length"]),
                "task_id": selection["task_id"], "execution_evidence": False, "fallback": False}
                if is_chain(desc) else {"schema": "r2f.topology-explicit-generation/v1",
                "request": deepcopy(selection["mutation_request"]), "task_id": selection["task_id"],
                "execution_evidence": False, "fallback": False})
        else:
            plan, topology_generation = _topology_generation(adapted_parent, profile, rng_seed)
        proposal = plan.operations[0]  # metadata only; actual plan is core-authored
        mutator_id = TOPOLOGY_MUTATOR
    elif wide:
        supported = set(_wide_kinds(surface, adapted_parent))
        if sampling_kinds is not None:
            # A profile may declare kinds this surface cannot honour.  Every
            # such kind is filtered (recorded, never attempted) while the
            # scheduler still picks from the supported subset.
            sampling_filtered = [kind for kind in sampling_kinds
                                 if kind not in supported]
        if sampling_kind is not None and sampling_kind not in supported:
            sampling_kind = None
        if sampling_kind is None and sampling_kinds is not None \
                and selection.get("sampling_error") is None \
                and selection.get("sampling_kind") is not None:
            # The scheduler always chooses a supported kind for this surface;
            # a mismatch means the declaration and corpus disagree.
            raise ExploratoryCycleError(
                "sampling kind was filtered but the scheduler declared one")
        if has_routes(entry):
            layers = _declared_route_layers(entry, contract, chain_length)
        else:
            layers = [(item, None) for item in _chain_surfaces(
                contract, surface, profile.get("chain_length", 1), profile["surfaces"])]
        proposals = []
        current = adapted_parent
        for index, (layer_surface, layer_route) in enumerate(layers):
            layer_kind = sampling_kind if index == 0 else None
            layer_mutator = (layer_route["mutator_id"] if layer_route is not None
                             else _layer_mutator(layer_surface, entry, index, layer_kind))
            context = _layer_context(entry, index, layer_kind)
            if layer_route is not None:
                context["boundary_class"] = layer_route["boundary_class"]
            proposal = mutate_wide(
                current, layer_surface, _layer_rng(rng_seed, index, layer_surface.surface_id),
                context=context, mutator_id=layer_mutator)
            proposals.append(proposal)
            current = proposal.input_after
        proposal = proposals[0]
        mutator_id = proposal.mutator_id
        sampling_kind = proposal.parameters.get("sampling_kind")
    else:
        rng = make_rng(rng_seed)
        context = {"application_phase": _application_phase(surface),
                   "boundary_class": entry.get("boundary_class")}
        if target_id == G01_TARGET:
            from .exploratory_input import validate_g01_profile
            if not validate_g01_profile(profile) or mutator_id != "M08" or target.target_revision != 1:
                raise ExploratoryCycleError("g01_exact_revision1_pair_route_required")
            if profile.get("scheduler_policy") == "bfs":
                slot = selection.get("sample_slot")
                if selection.get("sampling_kind") != "representative" or type(slot) is not int or not 0 <= slot < 4:
                    raise ExploratoryCycleError("g01_requires_declared_pair_task")
                context["pair_relation"] = ("RR", "RB", "BB", "BR")[slot]
            proposal = mutate_f2(adapted_parent, surface, rng, context=context, mutator_id="M08")
        elif target_id == F2_WINDOWS_TARGET:
            validate_f2_profile(profile)
            if mutator_id != F2_SURFACE_MUTATORS.get(surface.surface_id):
                raise ExploratoryCycleError("f2_windows_mutator_not_enabled")
            # M06/M07 stay unregistered in the global default mutator API.
            # No anchor_observed/post_match_observed is supplied by planning.
            proposal = mutate_f2(adapted_parent, surface, rng, context=context,
                                 mutator_id=mutator_id)
        else:
            proposal = mutate(adapted_parent, surface, rng, context=context,
                              mutator_id=mutator_id)

    windows, runtime_windows = [], []
    if route["backend"] == "discovery_namespace":
        max_seconds = adapted_parent.get("max_seconds")
        try:
            synchronized, windows, runtime_windows, window_source = _compose_recovery_inputs(
                adapted_parent, proposals if wide else [proposal], max_seconds=max_seconds)
        except ValueError as exc:
            if target_id == F2_WINDOWS_TARGET:
                raise _F2InputRejection("f2_input_rejected:" + str(exc)) from exc
            raise
        if wide:
            proposals = synchronized
        else:
            proposal = synchronized[0]
        adaptation["runtime_windows"] = runtime_windows
        adaptation["runtime_window_source"] = window_source
    if topology:
        pass  # _topology_generation already returns the validated core plan
    elif wide:
        # The runner is the single authority that normalizes the explicit plan
        # to 0-based core/runtime shapes; strip proposal windows so they are not
        # appended twice by compose_chain.
        from dataclasses import replace as _replace
        proposals = [_replace(item, runtime_windows=[]) for item in proposals]
        plan = compose_chain(proposals, contract, parent=adapted_parent,
                             rng_seed=rng_seed, runtime_windows=windows)
    else:
        compose_proposal = proposal
        if windows:
            from dataclasses import replace as _replace
            compose_proposal = _replace(proposal, runtime_windows=[])
        plan = compose(adapted_parent, [compose_proposal], contract, rng_seed=rng_seed,
                       runtime_windows=windows)
    from r2f.core.mutation_plan import mutation_plan_noop_reason
    noop_reason = mutation_plan_noop_reason(plan)
    if noop_reason is not None:
        raise _NoEffectiveInputRejection(noop_reason, plan)
    return {"plan": plan, "proposal": proposal, "route": route,
            "mutation_kind": mutation_kind, "carrier": carrier,
            "adaptation": adaptation, "runtime_windows": runtime_windows,
            "topology": topology, "wide": wide, "sampling_kind": sampling_kind,
            "sampling_kinds": sampling_kinds, "sampling_weight": sampling_weight,
            "chain_length": chain_length, "sampling_filtered": sampling_filtered,
            "adapted_parent": adapted_parent, "mutator_id": mutator_id,
            "topology_generation": topology_generation if topology else None}


def _generate_case(sequence, entry, target, contract, surface, parent, mutator_id,
                  target_id, rmw, selection, scheduler, source, repo, run_id, profile,
                  domain_id, registry, output_dir, budget, execute, runtime,
                  corpus, feedback, *, planned_hook=None, submission_hook=None,
                  deadline_reached=None, runtime_boundary=HOST_RUNTIME):
    """Generate one planned case; optionally advance the execution gates."""
    target_id = target.target_id
    rng_seed = _derive_rng_seed(scheduler, selection, entry)
    generated = _build_case_plan(entry, target, contract, surface, parent,
                                 mutator_id, profile, rng_seed, selection)
    plan = generated["plan"]
    proposal = generated["proposal"]
    route = generated["route"]
    mutation_kind = generated["mutation_kind"]
    carrier = generated["carrier"]
    adaptation = generated["adaptation"]
    runtime_windows = generated["runtime_windows"]
    topology = generated["topology"]
    wide = generated["wide"]
    sampling_kind = generated["sampling_kind"]
    sampling_kinds = generated["sampling_kinds"]
    sampling_weight = generated["sampling_weight"]
    chain_length = generated["chain_length"]
    sampling_filtered = generated["sampling_filtered"]
    adapted_parent = generated["adapted_parent"]
    mutator_id = generated["mutator_id"]
    topology_generation = generated["topology_generation"]
    scenario = Scenario(target, rmw, "mutated", 0, rng_seed,
                        {"mutation_kind": mutation_kind, "carrier": carrier,
                         "mutation_parameters": plan.final_config}, plan)
    services = registry.get_services(target_id)
    prepared = services.mutation_provider.prepare_plan(scenario, plan)

    # Exactly one authority for input identity: plan-bearing v2.
    effective = scenario.effective_input_v2(
        final_input=plan.final_config,
        input_layer_receipts=prepared["input_layer_receipts"],
        source_manifest=source)
    digest = effective_input_hash(effective, schema=INPUT_SCHEMA_V2)

    capsule = ReplayCapsule.from_scenario(
        scenario, source_manifest=source, final_input=plan.final_config,
        input_layer_receipts=prepared["input_layer_receipts"], origin_domain=domain_id)
    if capsule.input.get("input_identity_schema") != INPUT_SCHEMA_V2:
        raise ExploratoryCycleError("plan-bearing capsule did not bind v2 identity")
    if capsule.input.get("effective_input_sha256") != digest:
        raise ExploratoryCycleError("capsule and scenario v2 digests differ")
    if canonical_hash(capsule.input["effective_input"]) != canonical_hash(effective):
        raise ExploratoryCycleError("capsule effective input differs from the v2 plan input")

    child_content = content_identity({
        "target_id": target_id, "target_revision": target.target_revision,
        "rmw": rmw, "contract_hash": contract.contract_hash(),
        "surface_id": surface.surface_id,
        "mutation_plan": plan.to_dict(), "final_config": plan.final_config,
        "operations": [op.to_dict() for op in plan.operations],
        "runtime_windows": plan.runtime_windows,
        "seed_id": entry.get("seed_id"),
        "variant_id": entry.get("variant_id"),
    })

    cell_id = f"{run_id}.{sequence:04d}.{surface.surface_id}"
    case_dir = output_dir / "cases" / cell_id
    plan_path = case_dir / "mutation_plan.json"
    capsule_path = case_dir / "capsule.json"
    config_path = case_dir / "runtime_config.json"
    result_path = case_dir / "runtime" / "runtime_result.json"
    row_path = case_dir / "row.json"

    fuzz_surfaces, submitted_parameters = _plan_surface_bindings(plan, contract)
    config = {
        "run_id": run_id, "campaign_id": run_id, "cell_id": cell_id,
        "target_id": target_id, "target_revision": target.target_revision,
        "campaign_kind": "exploratory", "profile_id": profile["profile_id"],
        "rmw": rmw, "arm": "mutated", "repeat": 0, "seed": rng_seed,
        "mutation_kind": mutation_kind, "carrier": carrier,
        "mutation_parameters": dict(plan.final_config),
        "domain_id": domain_id, "environment_mode": "ros_overlay",
        "source_repo": str(repo), "expected_head": source.get("head"),
        "expected_target_revision": target.target_revision,
        "source_manifest": source, "scenario": scenario.to_dict(),
        "final_input": plan.final_config,
        "input_identity_schema": INPUT_SCHEMA_V2,
        "effective_input": effective,
        "effective_input_sha256": digest,
        "input_layer_receipts": effective["input_layer_receipts"],
        "mutation_plan": plan.to_dict(),
        "mutation_plan_sha256": canonical_hash(plan.to_dict()),
        "fuzz_surfaces": fuzz_surfaces,
        "submitted_parameters": submitted_parameters,
        "content_sha256": child_content,
        "runtime_route": route,
        "parent_adaptation": adaptation,
    }
    if has_routes(entry):
        config["mutation_route"] = deepcopy(selection["mutation_route"])
        config["mutation_route_selection_sha256"] = selection["selection_sha256"]
        config["declared_mutation_routes"] = deepcopy(entry["mutation_routes"])
    if wide:
        # Additive provenance appears only when the profile opts into T-E, so
        # the undeclared (legacy) config/row keeps its historical bytes.
        supported = sorted(set(_wide_kinds(surface, adapted_parent)))
        config["fuzz_generation"] = {
            "wide": True,
            "declared_sampling_kinds": (list(sampling_kinds)
                                        if sampling_kinds is not None else None),
            "supported_sampling_kinds": supported,
            "sampling_weight": (dict(sampling_weight)
                                if sampling_weight is not None else None),
            "filtered_sampling_kinds": sampling_filtered,
            "chain_length": chain_length,
            "chain_surfaces": [op.surface_id for op in plan.operations],
        }
        config["sampling_kind"] = sampling_kind
        config["chain_depth"] = len(plan.operations)
    config.update(json.loads(json.dumps(plan.final_config)))
    # Same submitted controls in plan, capsule and runtime; the only derived
    # projection is public role QoS, never a post-hash profile override.
    config.update(runtime_input(plan.final_config, backend=route["backend"],
                                runtime_capability=profile.get("runtime_capability")))
    if topology:
        config.update(runtime_capability=TOPOLOGY_CAPABILITY, plan_hash=plan.plan_hash(),
                      topology_generation=topology_generation)
        # Observation/build selection is explicit execution metadata, never a
        # post-hash mutation of the submitted topology program or input capsule.
        for key in ("observation_mode", "fastdds_peer_instrumentation"):
            if key in profile:
                config[key] = deepcopy(profile[key])
    if runtime_windows and "environment_windows" not in plan.final_config:
        # Preserve the historical E01 single-window input shape.
        config["environment_windows"] = json.loads(json.dumps(runtime_windows))

    binding = {"status": "not_checked", "silent_normalization": False,
               "backend": route["backend"], "carrier": carrier}
    try:
        from r2f.runtime.ros2_pair import (
            exploratory_bindings, normalize_config, require_binding_preserved,
        )
        if topology:
            from r2f.runtime.topology_route import validate_topology_route
            validate_topology_route(config)
            normalized = deepcopy(config)
        else:
            normalized = normalize_config(config)
        if route["backend"] == "projection_pair":
            from r2f.runtime.projection_pair import normalize_projection_config
            normalized = normalize_projection_config(normalized)
        elif route["backend"] == "discovery_namespace":
            from r2f.runtime.namespace_pair import normalize_discovery_config
            normalized = normalize_discovery_config(normalized)
        if not topology:
            expected = exploratory_bindings(normalized)
            require_binding_preserved(expected, normalized)
        # The normalized config must not change the submitted plan values.
        # Numeric widening (int -> float) by the runtime normalizer is recorded,
        # not treated as a new input; any other change is a binding error.
        numeric_widening = []
        for key, value in plan.final_config.items():
            observed = normalized.get(key)
            same = canonical_hash(observed) == canonical_hash(value)
            if not same and type(value) in (int, float) and type(observed) is float \
                    and float(value) == observed:
                numeric_widening.append(key)
                same = True
            if not same:
                raise ValueError("normalization changed submitted field:" + key)
        config = normalized
        # The runtime normalizer widens int -> float for duration fields.  The
        # carrier compares submitted plan values with the execution config, so
        # restore exactly the submitted representation for those widening-only
        # fields instead of silently changing the input identity.
        for key in numeric_widening:
            config[key] = plan.final_config[key]
        config["input_identity_schema"] = INPUT_SCHEMA_V2
        config["effective_input_sha256"] = digest
        config["effective_input"] = effective
        binding = {"status": "preserved", "silent_normalization": False,
                   "submitted_config_sha256": canonical_hash(plan.final_config),
                   "normalized_config_sha256": canonical_hash(normalized),
                   "numeric_widening_fields": sorted(numeric_widening)}
    except (ValueError, TypeError) as exc:
        binding = {"status": "binding_error", "silent_normalization": False,
                   "detail": str(exc)}

    row = {
        "cell_id": cell_id, "run_id": run_id, "target_id": target_id,
        "target_revision": target.target_revision,
        "profile_id": profile["profile_id"], "campaign_kind": "exploratory",
        "rmw": rmw, "arm": "mutated", "repeat": 0, "seed": rng_seed,
        "execution_stage": "planned", "actual_run": False,
        "oracle_verdict": "unknown", "control_kind": None,
        "surface_id": surface.surface_id, "surface_ids": [surface.surface_id],
        "boundary_class": proposal.parameters.get("boundary_class") or "sampled",
        "parameter_class": proposal.parameters.get("boundary_class") or "sampled",
        "seed_id": entry.get("seed_id"),
        "variant_id": entry.get("variant_id"),
        "parent_final_input": entry.get("final_config"),
        "parent_content_sha256": entry["content_sha256"],
        "plan_parent_content_sha256": plan.parent_content_sha256,
        "content_sha256": child_content,
        "mutator_id": mutator_id,
        "mutation_chain": [op.to_dict() for op in plan.operations],
        "mutation_plan": plan.to_dict(),
        "mutation_plan_sha256": canonical_hash(plan.to_dict()),
        "input_identity_schema": INPUT_SCHEMA_V2,
        "effective_input_sha256": digest,
        "input_layer_receipts": effective["input_layer_receipts"],
        "final_input": plan.final_config,
        "submitted_parameters": dict(config["submitted_parameters"]),
        "binding": binding,
        "replay_path": str(capsule_path),
        "runtime_config_path": str(config_path),
        "runtime_result_path": str(result_path),
        "mutation_receipt": dict.fromkeys(
            ("planned", "submitted", "carrier_admitted", "configured",
             "applied", "hit", "effect_observed"), False),
    }
    if has_routes(entry):
        row.update(mutation_route=deepcopy(selection["mutation_route"]),
                   route_selection=deepcopy(selection["route_selection"]),
                   selection_sha256=selection["selection_sha256"],
                   declared_mutation_routes=deepcopy(entry["mutation_routes"]))
    if wide:
        # T-E coverage fields are additive opt-in: the legacy row stays byte
        # identical when neither sampling_kinds nor chain_length is declared.
        row["sampling_kind"] = sampling_kind
        row["chain_depth"] = len(plan.operations)
        row["fuzz_generation"] = dict(config["fuzz_generation"])

    if topology:
        row.update(runtime_capability=TOPOLOGY_CAPABILITY, topology_generation=topology_generation,
                   chain_depth=len(plan.operations), candidate_eligible=False)
        if "observation_mode" in config:
            row["observation_mode"] = config["observation_mode"]
        if "fastdds_peer_instrumentation" in config:
            row["fastdds_peer_instrumentation"] = deepcopy(config["fastdds_peer_instrumentation"])

    if execute:
        from .tool_failures import generation_recipe
        row["generation_recipe"] = generation_recipe(
            entry, target, plan, source=source, carrier=carrier, mutation_kind=mutation_kind,
            runtime_boundary=runtime_boundary, profile=profile, sampling_kind=sampling_kind,
            selection=selection, observation_mode=config.get("observation_mode", "black_box"))
        row["recipe_tool_failure_policy"] = scheduler.recipe_submission_policy(row["generation_recipe"])
    config = _runtime_execution_config(
        config, execute=execute, runtime=runtime, runtime_boundary=runtime_boundary)
    qualification = execution_boundary_qualification(runtime_boundary, config=config)
    if qualification["status"] not in {"not_applicable", "qualified"}:
        row.update(runtime_boundary=runtime_boundary, boundary_qualification=qualification)
        if not execute:
            row.update(candidate_eligible=False, feedback_eligible=False, seal_eligible=False)

    prepare_identity(output_dir, row, source,
                     identity={"source_sha256": canonical_hash(source),
                               "target_sha256": canonical_hash(target.to_dict()),
                               "rule_sha256": canonical_hash(target.rule_contract)},
                     rule=target.rule_contract.get("primary_rule"))
    _atomic_json(plan_path, plan.to_dict())
    _atomic_json(capsule_path, capsule.to_dict())
    _atomic_json(config_path, config)
    budget["planned"] += 1
    budget["generated"] += 1

    _atomic_json(row_path, row)
    if planned_hook is not None:
        planned_hook(row, selection)
    if scheduler.policy == "bfs":
        outcome = scheduler.frontier.task(selection["task_id"]).get("generation_outcome")
        if outcome in {"compile_time_no_change", "duplicate_content", "guard_terminal"}:
            reason = "model_" + outcome
            row.update(generation_status="rejected", reason_code=reason,
                       blocked_reason=reason, candidate_eligible=False,
                       feedback_eligible=False, seal_eligible=False,
                       oracle_verdict="not_applicable")
            if topology and outcome == "duplicate_content":
                # Input dedupe never submits a carrier. This terminal is
                # separately regenerated/reopened by the standalone consumer.
                row["boundary_qualification"] = {
                    "status": "not_applicable", "eligible": False,
                    "reason_code": "model_duplicate_content_no_execution", "errors": []}
            _atomic_json(row_path, row)
            return row
    if not execute:
        return row
    if route["execution_admitted"] is not True:
        # Planned generation is verified for this backend; live execution is not.
        # Refuse explicitly instead of producing an unlabelled pseudo-run.
        row["reason_code"] = ("unsupported_capability:execution_backend_not_admitted:"
                              + route["backend"])
        row["blocked_reason"] = row["reason_code"]
        row["execution_blocker"] = {"owner": "runtime owner", "backend": route["backend"],
                                    "detail": "planned-only route; runtime execution not verified"}
        _atomic_json(row_path, row)
        return row

    if source_manifest(repo) != source:
        row["execution_stage"] = "aborted"
        row["abort_reason"] = "source_repo_HEAD_or_behavior_tree_changed"
        row["reason_code"] = row["abort_reason"]
        row["blocked_reason"] = row["abort_reason"]
        budget["aborted"] += 1
        _atomic_json(row_path, row)
        return row
    if binding["status"] != "preserved":
        row["reason_code"] = "binding_not_preserved"
        row["blocked_reason"] = "binding_not_preserved:" + str(binding.get("detail"))
        _atomic_json(row_path, row)
        return row
    if deadline_reached is not None and deadline_reached():
        row.update(execution_stage="aborted", abort_reason="cycle_deadline_reached",
                   reason_code="cycle_deadline_reached", blocked_reason="cycle_deadline_reached")
        budget["aborted"] += 1
        _atomic_json(row_path, row)
        return row
    policy = row["recipe_tool_failure_policy"]
    if not policy["submission_allowed"]:
        reason = "recipe_tool_failure_isolated"
        row.update(reason_code=reason, blocked_reason=reason, candidate_eligible=False,
                   feedback_eligible=False, seal_eligible=False)
        _atomic_json(row_path, row)
        return row
    if submission_hook is not None:
        submission_hook(row, selection)
    # Snapshot persistence may itself take time or overlap a source edit.  The
    # fence is deliberately conservative; it is not an execution receipt.
    reason = None
    if source_manifest(repo) != source:
        reason = "source_repo_HEAD_or_behavior_tree_changed"
    elif deadline_reached is not None and deadline_reached():
        reason = "cycle_deadline_reached"
    if reason is not None:
        row.update(execution_stage="aborted", abort_reason=reason,
                   reason_code=reason, blocked_reason=reason)
        budget["aborted"] += 1
        _atomic_json(row_path, row)
        return row
    budget["attempted"] += 1
    case_dir.joinpath("runtime").mkdir(parents=True, exist_ok=True)
    try:
        runtime_result = services.carrier.apply(
            scenario, effective,
            execution={"config": config,
                       "output_dir": result_path.parent,
                       "runtime": runtime})
    except Exception as exc:
        runtime_result = runtime_exception(exc)
    # Runtime-supplied validity flags cannot attest to the actual checkout.
    # Latch this fence before any healthy receipt, coverage or admission; still
    # persist and ingest the original result to retain its stage and raw facts.
    post_runtime_source = source_manifest(repo)
    try:
        persist_runtime_result(result_path, runtime_result)
        execution = ingest_execution(scenario, config, runtime_result, capsule, services=services)
        row.update(execution)
        if runtime_result.get("runner_exception"):
            row["runner_exception"] = runtime_result["runner_exception"]
    except Exception as exc:
        row.update(wrapper_failure(row, exc))
        if not isinstance(runtime_result, dict):
            runtime_result = runtime_exception(exc)
        elif runtime_result.get("runner_exception"):
            row["runner_exception"] = runtime_result["runner_exception"]
    if post_runtime_source != source:
        # Apply the latched failure after ingest/wrapper annotations: neither a
        # successful result nor a persistence failure can hide source drift.
        row.update(abort_reason="source_repo_HEAD_or_behavior_tree_changed",
                   post_runtime_source_manifest=post_runtime_source)
    # Deterministic/input fields keep the plan identity; ingest cannot change them.
    row.update({
        "surface_id": surface.surface_id, "surface_ids": [surface.surface_id],
        "boundary_class": proposal.parameters.get("boundary_class") or "sampled",
        "parameter_class": proposal.parameters.get("boundary_class") or "sampled",
        "seed_id": entry.get("seed_id"), "mutator_id": mutator_id,
        "mutation_plan": plan.to_dict(),
        "mutation_plan_sha256": canonical_hash(plan.to_dict()),
        "mutation_chain": [op.to_dict() for op in plan.operations],
        "content_sha256": child_content,
        "input_identity_schema": INPUT_SCHEMA_V2,
        "effective_input_sha256": digest,
        "binding": binding,
        "evidence_origin": _evidence_origin(runtime_result, runtime=runtime,
                                            runtime_boundary=runtime_boundary),
    })
    from .tool_failures import attempt_receipt
    try:
        result_reference = {"path": str(result_path), "sha256": raw_sha256(result_path),
                            "basis": "persisted_file", "status": "available"}
    except OSError as exc:
        result_reference = {"path": str(result_path), "sha256": canonical_hash(runtime_result),
                            "basis": "returned_value", "status": "unavailable", "error": str(exc)}
    row["recipe_attempt"] = attempt_receipt(
        row["generation_recipe"], row, runtime_result, result_reference)
    global_invalid = row["recipe_attempt"]["outcome"] == "global_invalid"
    if global_invalid:
        # Preserve actual stage/raw facts, but never qualify globally invalid
        # source/lock/observer evidence as coverage, novelty or a SUT candidate.
        row.update(tool_error=True, reason_code="global_execution_invalidity",
                   candidate_eligible=False, feedback_eligible=False, seal_eligible=False,
                   evidence_errors=list(row.get("evidence_errors") or []) +
                       [row["recipe_attempt"]["diagnostic"]["reason"]])
    if wide:
        row.update({"sampling_kind": sampling_kind,
                    "chain_depth": len(plan.operations),
                    "fuzz_generation": dict(config["fuzz_generation"])})
    # Capsule's strict successful/failure execution schema cannot represent a
    # reached non-success terminal. Keep its sealed input unchanged; the unified
    # row/runtime artifacts retain this versioned actual attempt in full.
    if row.get("actual_run") is True and "input_terminal" not in row:
        try:
            capsule.record_execution(row)
        except ValueError as exc:
            row["capsule_execution_error"] = str(exc)
    try:
        _atomic_json(capsule_path, capsule.to_dict())
    except Exception as exc:
        record_packaging_failure(row, exc, capsule_path)
        row.update(candidate_eligible=False, feedback_eligible=False, seal_eligible=False,
                   corpus_offer={"accepted": False, "reason": "packaging_failure"})
        if not global_invalid:
            row["recipe_attempt"]["outcome"] = "tool_failure"
        row["tool_error"] = True
        feedback.record(row)
        scheduler.advance_feedback(feedback)
        return row
    if row.get("execution_stage") == "judged" and row.get("actual_run") is True:
        budget["executed"] += 1
    if global_invalid:
        row["corpus_offer"] = {"accepted": False, "reason": "global_execution_invalidity"}
        feedback.record(row)
        scheduler.advance_feedback(feedback)
        _atomic_json(row_path, row)
        return row

    def persist_topology_candidate_assessment(boundary, formal):
        if not topology:
            return
        from .topology_candidate_assessment import finalize_assessment, capsule_attachment
        finalize_assessment(scenario, config, capsule, row, boundary=boundary, qualification=formal)
        _atomic_json(capsule_path, capsule_attachment(capsule, row))

    qualification = execution_boundary_qualification(runtime_boundary, config=config,
                                                  result=runtime_result, row=row,
                                                  formal_runtime=_is_formal_runtime(runtime, runtime_boundary),
                                                  output_root=result_path.parent)
    if qualification["status"] != "not_applicable":
        row.update(boundary_qualification=qualification, boundary_eligible=qualification["eligible"])
    if qualification["status"] not in {"not_applicable", "qualified", "terminal_unqualified"}:
        # An unexpected boundary wrapper result may still carry a real inner
        # run. Preserve its stage, raw and actual counters. The append-only
        # feedback epoch accounts for attempts, not just qualified coverage.
        row.update(boundary_qualification=qualification, candidate_eligible=False,
                   feedback_eligible=False, seal_eligible=False,
                   corpus_offer={"accepted": False, "reason": qualification["reason_code"]})
        # Only this ledger projection is incomplete; do not rewrite raw facts
        # or qualify the execution row. No coverage, edge or corpus novelty.
        persist_topology_candidate_assessment(qualification, None)
        feedback.record(dict(row, evidence_errors=["boundary_execution_not_qualified"]))
        scheduler.advance_feedback(feedback)
        _atomic_json(row_path, row)
        return row

    if topology:
        # N-endpoint evidence has its own raw/environment/worker gates. A
        # successful inner result or caller-supplied qualification is not enough.
        from .replay import _qualification, DEFAULT_RUNTIME, ISOLATED_RUNTIME as ISOLATED_ORIGIN
        origin = (ISOLATED_ORIGIN if runtime_boundary == ISOLATED_RUNTIME else DEFAULT_RUNTIME
                  ) if _is_formal_runtime(runtime, runtime_boundary) else "injected_runtime"
        qualified = _qualification(origin, config, runtime_result, row, output_root=result_path.parent)
        row["execution_qualification"] = qualified
        if "input_terminal" in row:
            from r2f.reports.run_report import topology_nohit_continuation
            try:
                terminal = topology_nohit_continuation(config, runtime_result, row, qualified)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                terminal = False
                qualified["qualification_errors"].append("topology_nohit_terminal_invalid:" + str(exc))
            row.update(candidate_eligible=False, feedback_eligible=False, seal_eligible=False,
                       corpus_offer={"accepted": False, "reason": "topology_mutation_nohit"})
            if not terminal:
                row["boundary_qualification"] = {"status": "invalid", "eligible": False,
                    "reason_code": "topology_nohit_terminal_invalid",
                    "errors": qualified["qualification_errors"]}
            persist_topology_candidate_assessment(qualification, qualified)
            # Account an actual attempt, not completeness, coverage or novelty.
            feedback.record(dict(row, evidence_errors=["topology_mutation_nohit"]))
            scheduler.advance_feedback(feedback)
            _atomic_json(row_path, row)
            return row
        if not qualified["real_ros_execution"]:
            if (runtime_result.get("real_execution") is True
                    and (runtime_result.get("mutation_receipt") or {}).get("hit") is False):
                # A damaged no-hit attempt without a valid terminal must not
                # silently proceed on host either. This is not a label waiver.
                row["boundary_qualification"] = {"status": "invalid", "eligible": False,
                    "reason_code": "topology_nohit_terminal_missing_or_invalid",
                    "errors": qualified["qualification_errors"]}
            row.update(candidate_eligible=False, feedback_eligible=False, seal_eligible=False,
                       corpus_offer={"accepted": False, "reason": "topology_execution_not_qualified"})
            # The append-only feedback ledger also accounts for attempts. Add
            # an explicitly incomplete evidence projection, not a rewritten
            # execution row: no coverage/edge/novelty, actual flags preserved.
            persist_topology_candidate_assessment(qualification, qualified)
            feedback.record(dict(row, evidence_errors=["topology_execution_not_qualified"]))
            scheduler.advance_feedback(feedback)
            _atomic_json(row_path, row)
            return row

        persist_topology_candidate_assessment(qualification, qualified)

    complete = False
    row["observed_edge"] = None
    try:
        from r2f.core.state_signature import observation_edge
        complete = observation_complete(row) and not tool_error(row)
        observed = observation_edge(row) if complete else None
        row["observed_edge"] = observed[1] if observed else None
    except (ValueError, KeyError, TypeError):
        complete = False
    offer_record = {
        "target_id": target_id, "target_revision": target.target_revision,
        "rmw": rmw, "contract_hash": contract.contract_hash(),
        "surface_id": surface.surface_id, "boundary_class": row["boundary_class"],
        "relation_class": entry.get("relation_class"),
        "contract_branch": entry.get("contract_branch"),
        "seed_id": entry.get("seed_id"),
        "parent_content_sha256": entry["content_sha256"],
        "final_config": plan.final_config, "mutation_plan": plan.to_dict(),
        "operations": [op.to_dict() for op in plan.operations],
        "runtime_windows": plan.runtime_windows,
        "cost": len(plan.operations), "executable": True,
    }
    if wide:
        offer_record.update({"sampling_kind": sampling_kind,
                             "chain_depth": len(plan.operations)})
    inherit_routes(entry, offer_record)
    offer = corpus.offer(
        offer_record,
        executed=row.get("actual_run") is True, evidence_complete=complete,
        observed_edge=row.get("observed_edge"),
        oracle_verdict=row.get("oracle_verdict"), tool_error=tool_error(row))
    row["corpus_offer"] = offer
    _atomic_json(row_path, row)
    feedback.record(row)
    scheduler.advance_feedback(feedback)
    _atomic_json(row_path, row)
    return row


def reevaluate_capability(output_dir, *, review_log, validation_record_id, original_row, saved_cases):
    """Publish immutable canonical saved-case oracle reevaluation attachments.

    No runtime is acquired and no capture row is rerun or admitted as a new
    candidate. Current registered code is measured; proposal version labels do
    not choose code. Activation and real feedback remain pending.
    """
    from r2f.reports.capability_reconnect import evaluate_review_reconnect
    from r2f.reports.candidate_report import _write_immutable_json, capability_reconnect_report

    evaluation = evaluate_review_reconnect(review_log=review_log,
        validation_record_id=validation_record_id, original_row=original_row, saved_cases=saved_cases)
    root = Path(output_dir)
    def publish(name, value):
        path = _write_immutable_json(root / name, value)
        return {'path': name, 'sha256': raw_sha256(path)}
    evaluation_ref = publish('capability-reevaluation.json', evaluation)
    report = capability_reconnect_report(evaluation, evaluation_ref)
    report_ref = publish('candidate-report.json', report)
    manifest_ref = publish('manifest.json', dict(run_id=report['run_id'],
        source_manifest=evaluation['evaluation_source'], new_execution=False,
        interpretation='offline_reinterpretation'))
    publish('evidence-index.json', dict(schema_version='r2f.runner-evidence-index/v1',
        run_id=report['run_id'], source_manifest=evaluation['evaluation_source'],
        manifest=manifest_ref, reports=[report_ref], packets=[], identities=[], tool_problems=[]))
    return report
