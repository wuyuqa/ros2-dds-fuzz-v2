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
from r2f.core.mutators import MUTATORS, make_rng, mutate
from r2f.core.mutation_plan import compose
from r2f.core.scenario import Scenario
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.targets import EXPLORATORY_TARGET_ORDER
from r2f.modules.s1.fuzz_catalog import catalog_digest
from .corpus import Corpus, content_identity
from .feedback import FeedbackLog, observation_complete, tool_error
from .scheduler import ExploratoryScheduler, required_catalog_hash

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

#: F1 exploratory targets whose declared carrier/backend can be generated by
#: this runner.  Derived from the route table so a target cannot be enabled
#: without a concrete backend/carrier pair.
_F1_CYCLE_TARGETS = frozenset(_EXPLORATORY_ROUTES)

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


def _atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    fd, temporary = _tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with _os.fdopen(fd, "w") as handle:
            handle.write(payload)
        _os.replace(temporary, path)
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
    return json.loads(json.dumps(profile))


def _application_phase(surface):
    if surface.carrier_id == "wire_proxy":
        return "runtime_window"
    return "compile_time"


def _derive_rng_seed(scheduler, selection, entry):
    material = canonical_hash({
        "cycle_rng_seed": scheduler.rng_seed,
        "selection_sha256": selection["selection_sha256"],
        "sequence": selection["sequence"],
        "parent_content_sha256": entry["content_sha256"],
    })
    return int(material[:16], 16)


_OFFLINE_EVIDENCE_ORIGINS = frozenset({"fixture", "synthetic", "offline"})


def _evidence_origin(runtime_result):
    """Preserve real provenance and relabel offline fixtures explicitly.

    ``offline_fixture`` is still an offline contract result: it can never be a
    real SUT candidate or seal.  The label is distinct from ``injected_runtime``
    so downstream reports do not confuse a labelled fixture with an unknown
    injected callable.
    """
    origin = runtime_result.get("evidence_origin")
    if origin in _OFFLINE_EVIDENCE_ORIGINS:
        return "offline_fixture"
    if isinstance(origin, str) and origin:
        return origin
    return "injected_runtime"


def _flatten_surface_values(surface, config):
    values = {}
    for path in surface.input_paths:
        current = config
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current is not None:
            values[path] = current
    return values


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
    return adapted, receipt


def _recovery_windows(drop_duration_sec):
    """Single-window drop->restore derived from the mutated drop duration.

    One window only: E02/E03 multi-window generation stays capability pending.
    Two shapes are produced from the same numbers because the core plan schema
    and the namespace validator require different window documents.
    """
    if type(drop_duration_sec) not in (int, float) or isinstance(drop_duration_sec, bool) \
            or not 0 < drop_duration_sec <= 8.0:
        raise ValueError("recovery drop_duration_sec must be a positive finite window")
    core_window = {
        "schema": "r2f.runtime-window/v1", "window_id": "w0", "window_index": 0,
        "trigger_anchor": "fresh_match", "trigger_offset_sec": 0.0,
        "drop_duration_sec": drop_duration_sec, "restore_interval_sec": 0.0,
    }
    runtime_window = {key: value for key, value in core_window.items() if key != "schema"}
    return core_window, runtime_window


def run_exploratory_cycle(output_dir, *, corpus, feedback, scheduler, profile, runtime=None,
                          execute=False, repo=None, run_id=None, max_inputs=1,
                          domain_id=173):
    """Run one bounded feedback-driven exploratory cycle.

    ``execute=False`` performs no runtime call: rows stay ``planned`` and no
    feedback is recorded.  ``execute=True`` requires an injected runtime; the
    carrier dispatches the plan-bearing v2 identity, so the runner passes the
    same single digest it wrote to the capsule and runtime config.
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
    if type(max_inputs) is not int or max_inputs < 1:
        raise ExploratoryCycleError("max_inputs must be a positive integer")
    if execute and not callable(runtime):
        raise ExploratoryCycleError("execute=True requires an injected runtime callable")
    profile = _load_profile(profile)

    repo = Path(repo or Path(__file__).resolve().parents[3]).resolve()
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ExploratoryCycleError("exploratory cycle output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or "s1f-" + uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", run_id):
        raise ExploratoryCycleError("invalid run_id")

    source = source_manifest(repo)
    registry = Registry()
    register_exploratory(registry, include_required=False)
    manifest = {
        "schema_version": 1, "campaign_kind": "exploratory", "run_id": run_id,
        "profile": profile, "profile_id": profile["profile_id"],
        "source_manifest": source, "requested_domain": domain_id,
        "catalog_digest": catalog_digest(),
        "required_catalog_sha256": required_catalog_hash(),
        "target_ids": list(profile["target_ids"]),
        "exploration_order": list(EXPLORATORY_TARGET_ORDER),
        "input_identity_schema": INPUT_SCHEMA_V2,
        "required_matrix_changed": False, "maturity_denominator_changed": False,
        "execution_boundary": profile.get("execution_boundary"),
    }
    _atomic_json(output_dir / "manifest.json", manifest)

    rows, rejected, budget = [], [], {
        "planned": 0, "generated": 0, "attempted": 0, "executed": 0,
        "aborted": 0, "interrupted": 0, "max_inputs": max_inputs,
    }
    abort = None
    stop_reason = None
    last_state = {"planned": 0, "executed": 0, "cell_id": None, "sequence": None}

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
        rows.append(row); rejected.append(row)
        budget["planned"] += 1
        return row

    for sequence in range(max_inputs):
        if not corpus.selectable():
            stop_reason = "selection_exhausted"
            break
        try:
            selection = scheduler.next()
        except ValueError as exc:
            stop_reason = "selection_error:" + str(exc)
            break
        entry = selection["entry"]
        target_id = entry.get("target_id")
        rmw = entry.get("rmw")

        if target_id not in EXPLORATORY_TARGET_ORDER:
            row = reject(sequence, entry, "target_not_in_exploratory_catalog")
        elif target_id not in _F1_CYCLE_TARGETS:
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
                    mutator_ids = [mid for mid in surface.mutator_ids if mid in MUTATORS]
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
                    if not mutator_ids:
                        row = reject(sequence, entry,
                                     "unsupported_capability:no_implemented_mutator")
                    elif mutator_id not in mutator_ids:
                        row = reject(sequence, entry, "mutator_not_declared:" + str(mutator_id))
                    else:
                        row = _generate_case(sequence, entry, target, contract, surface,
                                            parent, mutator_id, target_id, rmw, selection,
                                            scheduler, source, repo, run_id, profile,
                                            domain_id, registry, output_dir, budget,
                                            execute, runtime, corpus, feedback)
        if row is None:
            stop_reason = "generation_failed"
            break
        rows.append(row)
        last_state = {"planned": budget["planned"], "executed": budget["executed"],
                      "cell_id": row["cell_id"], "sequence": sequence,
                      "execution_stage": row["execution_stage"]}
        if source_manifest(repo) != source:
            abort = {"reason_code": "source_repo_HEAD_or_behavior_tree_changed",
                     "after_row_index": len(rows) - 1,
                     "detail": "source identity changed during the cycle"}
            break

    if not execute and stop_reason is None:
        stop_reason = "execute_false_planned_only"
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
        "last_state": last_state,
        "abort": abort,
        "stop_reason": stop_reason,
        "execute": bool(execute),
        "selection_log": scheduler.to_dict(),
        "feedback": feedback.to_dict(),
        "corpus": corpus.to_dict(),
        "maturity_scope": "exploratory only; required 72 denominator unchanged and not claimed",
    }
    _atomic_json(output_dir / "selection_log.json", scheduler.to_dict())
    _atomic_json(output_dir / "feedback.json", feedback.to_dict())
    _atomic_json(output_dir / "corpus.json", corpus.to_dict())
    _atomic_json(output_dir / "cycle.json", result)
    return result


def _generate_case(sequence, entry, target, contract, surface, parent, mutator_id,
                  target_id, rmw, selection, scheduler, source, repo, run_id, profile,
                  domain_id, registry, output_dir, budget, execute, runtime,
                  corpus, feedback):
    """Generate one planned case; optionally advance the execution gates."""
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
    rng_seed = _derive_rng_seed(scheduler, selection, entry)
    rng = make_rng(rng_seed)
    proposal = mutate(adapted_parent, surface, rng,
                      context={"application_phase": _application_phase(surface),
                               "boundary_class": entry.get("boundary_class")},
                      mutator_id=mutator_id)

    windows, runtime_windows = [], []
    if route["backend"] == "discovery_namespace":
        dropped = proposal.input_after.get("drop_duration_sec")
        core_window, runtime_window = _recovery_windows(dropped)
        windows, runtime_windows = [core_window], [runtime_window]
        adaptation["runtime_windows"] = runtime_windows
    plan = compose(adapted_parent, [proposal], contract, rng_seed=rng_seed,
                   runtime_windows=windows)
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
        "fuzz_surfaces": [surface.surface_id],
        "submitted_parameters": _flatten_surface_values(surface, plan.final_config),
        "content_sha256": child_content,
        "runtime_route": route,
        "parent_adaptation": adaptation,
    }
    config.update(json.loads(json.dumps(plan.final_config)))
    # F3 exploratory stimulus: start a second reader generation inside the
    # drop window.  This is a runtime action, not part of the submitted config
    # identity.
    if profile.get('reader_recreate_in_window') is True:
        config['reader_recreate_in_window'] = True
    if route["backend"] == "projection_pair":
        # Both roles must run the mutated public profile.  Deriving them from the
        # plan's final qos keeps one authoritative submitted value instead of
        # letting a stale pre-mutation copy survive in the role fields.
        role_qos = json.loads(json.dumps(plan.final_config.get("qos")))
        config["writer_qos"] = json.loads(json.dumps(role_qos))
        config["reader_qos"] = json.loads(json.dumps(role_qos))
    if runtime_windows:
        # namespace_pair reads environment_windows; validate_window_plan rejects
        # zero/oversized plans, so a multi-window E02/E03 input cannot slip in.
        config["environment_windows"] = json.loads(json.dumps(runtime_windows))

    binding = {"status": "not_checked", "silent_normalization": False,
               "backend": route["backend"], "carrier": carrier}
    try:
        from r2f.runtime.ros2_pair import (
            exploratory_bindings, normalize_config, require_binding_preserved,
        )
        normalized = normalize_config(config)
        if route["backend"] == "projection_pair":
            from r2f.runtime.projection_pair import normalize_projection_config
            normalized = normalize_projection_config(normalized)
        elif route["backend"] == "discovery_namespace":
            from r2f.runtime.namespace_pair import normalize_discovery_config
            normalized = normalize_discovery_config(normalized)
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
        if profile.get('reader_recreate_in_window') is True:
            config['reader_recreate_in_window'] = True
            if 'reader_recreate_delay_sec' in profile:
                config['reader_recreate_delay_sec'] = profile['reader_recreate_delay_sec']
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

    _atomic_json(plan_path, plan.to_dict())
    _atomic_json(capsule_path, capsule.to_dict())
    _atomic_json(config_path, config)
    budget["planned"] += 1
    budget["generated"] += 1

    if not execute:
        _atomic_json(row_path, row)
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

    budget["attempted"] += 1
    case_dir.joinpath("runtime").mkdir(parents=True, exist_ok=True)
    if binding["status"] != "preserved":
        row["reason_code"] = "binding_not_preserved"
        row["blocked_reason"] = "binding_not_preserved:" + str(binding.get("detail"))
        _atomic_json(row_path, row)
        return row
    try:
        runtime_result = services.carrier.apply(
            scenario, effective,
            execution={"config": config, "output_dir": result_path.parent,
                       "runtime": runtime})
    except Exception as exc:
        import traceback as _traceback
        runtime_result = {"real_execution": False, "runtime_status": "tool_error",
                          "reason": f"{type(exc).__name__}:{exc}",
                          "evidence_origin": "injected_runtime",
                          "tool_error_traceback": _traceback.format_exc()}
    _atomic_json(result_path, runtime_result)
    execution = ingest_execution(scenario, config, runtime_result, capsule, services=services)
    row.update(execution)
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
        "evidence_origin": _evidence_origin(runtime_result),
    })
    if row.get("actual_run") is True:
        try:
            capsule.record_execution(row)
        except ValueError as exc:
            row["capsule_execution_error"] = str(exc)
    _atomic_json(capsule_path, capsule.to_dict())
    if row.get("execution_stage") == "judged" and row.get("actual_run") is True:
        budget["executed"] += 1

    complete = False
    row["observed_edge"] = None
    try:
        from r2f.core.state_signature import observation_edge
        complete = observation_complete(row) and not tool_error(row)
        observed = observation_edge(row) if complete else None
        row["observed_edge"] = observed[1] if observed else None
    except (ValueError, KeyError, TypeError):
        complete = False
    offer = corpus.offer(
        {"target_id": target_id, "target_revision": target.target_revision,
         "rmw": rmw, "contract_hash": contract.contract_hash(),
         "surface_id": surface.surface_id, "boundary_class": row["boundary_class"],
         "relation_class": entry.get("relation_class"),
         "contract_branch": entry.get("contract_branch"),
         "seed_id": entry.get("seed_id"),
         "parent_content_sha256": entry["content_sha256"],
         "final_config": plan.final_config, "mutation_plan": plan.to_dict(),
         "operations": [op.to_dict() for op in plan.operations],
         "runtime_windows": plan.runtime_windows,
         "cost": len(plan.operations), "executable": True},
        executed=row.get("actual_run") is True, evidence_complete=complete,
        observed_edge=row.get("observed_edge"),
        oracle_verdict=row.get("oracle_verdict"), tool_error=tool_error(row))
    row["corpus_offer"] = offer
    feedback.record(row)
    _atomic_json(row_path, row)
    return row
