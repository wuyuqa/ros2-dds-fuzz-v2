"""Versioned, offline-verifiable continuation of the shared exploratory runner.

A checkpoint is input/selection state, not ROS qualification or execution proof.
Only a fresh output directory may receive new snapshots; resume sources are read
only.  Full payload hashes supplement (never replace) semantic input validation.
"""
from copy import deepcopy
import json
import hashlib
import math
import os
from pathlib import Path
import re
import tempfile

from r2f.core import Registry
from r2f.core.evidence import canonical_hash
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.fuzz_catalog import catalog_digest
from .corpus import Corpus, corpus_from_profile, corpus_identity
from .search_frontier import (_frontier_validation_scope,
                              _remember_model_validated_frontier, _memo_key,
                              _hook_model_validation_scope)
from .mutation_routes import (_route_validation_scope, has_routes, route_view, routes_enabled,
                              validate_route_entry, validate_route_selection)
from .scheduler import (ExploratoryScheduler, required_catalog_hash, validated_feedback,
                        _restore_checkpoint_corpus)
from .exploratory_input import (F2_WINDOWS_TARGET, F2_SURFACE_MUTATORS,
                                validate_f2_profile, runtime_input)

SCHEMA = "r2f.exploratory-checkpoint/v1"


class CheckpointResumeError(ValueError):
    """A preserved pending attempt requires an explicit human disposition."""


def _file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _process_identity(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"pid": pid, "start_ticks": stat[19]}
    except (OSError, IndexError):
        return None


def _pending_attempt(output, state, payload):
    ref = payload.get("last_case_ref")
    cell = ref.get("cell_id") if isinstance(ref, dict) else ref
    if not isinstance(cell, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", cell):
        raise ValueError("planned checkpoint requires a safe case reference")
    case = output / "cases" / cell
    paths = {"row_path": case / "row.json", "capsule_path": case / "capsule.json",
             "config_path": case / "runtime_config.json", "plan_path": case / "mutation_plan.json"}
    return {"schema": "r2f.pending-attempt/v1", "cell_id": cell,
            "output_dir": str(output), "case_dir": str(case),
            "runtime_dir": str(case / "runtime"), "submission_marker_path": str(case / "submission_intent.json"),
            "artifacts": {name: {"path": str(path), "sha256": _file_hash(path)} for name, path in paths.items()},
            "execute_requested": payload.get("execute_requested", payload.get("execute", False)),
            "producer": _process_identity(os.getpid()),
            "submission_state": payload.get("submission_state", "prepared")}


def _pending_refusal(continuation):
    attempt = continuation.get("pending_attempt")
    reasons = []
    if not isinstance(attempt, dict) or attempt.get("schema") != "r2f.pending-attempt/v1":
        return ["pending_attempt_provenance_missing"]
    if attempt.get("submission_state") != "prepared" or continuation.get("submission_state", "prepared") != "prepared":
        reasons.append("submission_already_started_or_unknown")
    if continuation.get("stop_reason") == "resume_pending_execution_uncertain":
        reasons.append("previous_uncertain_disposition_required")
    try:
        case = Path(attempt["case_dir"])
        output = Path(attempt["output_dir"])
        if case != output / "cases" / attempt["cell_id"]:
            reasons.append("pending_case_identity_mismatch")
        if attempt.get("submission_marker_path") != str(case / "submission_intent.json") or attempt.get("runtime_dir") != str(case / "runtime"):
            reasons.append("pending_runtime_reference_mismatch")
        if (case / "submission_intent.json").exists() or (case / "submission.json").exists():
            reasons.append("submission_marker_exists")
        if (case / "runtime").exists():
            reasons.append("runtime_directory_exists")
        if any(p.is_file() and ("raw" in p.name.lower() or "submission" in p.name.lower()) for p in case.rglob("*")):
            reasons.append("runtime_or_raw_artifacts_exist")
        for name, filename in (("row_path", "row.json"), ("capsule_path", "capsule.json"),
                               ("config_path", "runtime_config.json"), ("plan_path", "mutation_plan.json")):
            artifact = attempt["artifacts"][name]
            if artifact["path"] != str(case / filename) or not _sha(artifact["sha256"]) or _file_hash(artifact["path"]) != artifact["sha256"]:
                reasons.append("prepared_artifact_changed:" + name)
        row = json.loads((case / "row.json").read_text())
        capsule = json.loads((case / "capsule.json").read_text())
        if row.get("execution_stage") != "planned" or row.get("actual_run") is not False or row.get("workspace_executed") is True or capsule.get("execution") is not None or capsule.get("execution_history"):
            reasons.append("prepared_artifact_claims_execution")
    except (OSError, ValueError, KeyError, TypeError):
        reasons.append("pending_artifact_unreadable_or_unknown")
    if attempt.get("execute_requested") is not False:
        owner = attempt.get("producer")
        if (not isinstance(owner, dict) or type(owner.get("pid")) is not int or
                not isinstance(owner.get("start_ticks"), str) or not owner["start_ticks"].isdigit()):
            reasons.append("pending_owner_unknown")
        else:
            current = _process_identity(owner["pid"])
            if current == owner:
                reasons.append("pending_owner_still_active")
            elif current is None and Path(f"/proc/{owner['pid']}").exists():
                reasons.append("pending_owner_identity_unreadable")
    return reasons


def _sha(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _identity(profile, cycle_id, source):
    if not isinstance(source, dict) or not source.get("head") or not _sha(source.get("behavior_tree_sha256")):
        raise ValueError("checkpoint requires a complete source identity")
    # Use the same profile builder/registry as ordinary execution, not a second
    # target catalog or a permissive target-id fallback.
    seeds = corpus_from_profile(profile, cycle_id=cycle_id)
    registry = Registry()
    register_exploratory(registry, include_required=False,
                         runtime_capability=profile.get("runtime_capability"))
    targets = [registry.get_target(t).to_dict() for t in profile["target_ids"]]
    identity = {"profile": deepcopy(profile), "profile_sha256": canonical_hash(profile),
                "cycle_id": cycle_id, "source_manifest": deepcopy(source),
                "target_manifest": targets, "catalog_digest": catalog_digest(),
                "required_catalog_sha256": required_catalog_hash(),
                "initial_corpus_sha256": canonical_hash(seeds.to_dict())}
    return identity, seeds, registry


def _serialized(value):
    return deepcopy(value.to_dict() if hasattr(value, "to_dict") else value)


def _validate_corpus_binding(corpus, identity, seeds, registry, *, _records=None):
    allowed = {r["target_id"] for r in seeds.entries.values()}
    for record in corpus.entries.values():
        # This set belongs to ONE _validate_state invocation: registry/profile/
        # source are fixed, and every distinct full record is checked in full.
        # Never skip the per-corpus pinned-seed/history checks below.
        record_key = _memo_key(record, allow_alias=True) if _records is not None else None
        if record_key is not None and record_key in _records:
            continue
        if record["target_id"] not in allowed:
            raise ValueError("checkpoint corpus target/cycle mismatch")
        target = registry.get_target(record["target_id"])
        contract = target.exploration_contract
        if record["target_revision"] != target.target_revision or record["contract_hash"] != contract.contract_hash():
            raise ValueError("checkpoint corpus target revision/contract mismatch")
        if record["rmw"] not in identity["profile"]["rmws"]:
            raise ValueError("checkpoint corpus RMW mismatch")
        if has_routes(record) or routes_enabled(identity["profile"]):
            validate_route_entry(record, contract=contract, profile=identity["profile"])
        f2_windows = record["target_id"] == F2_WINDOWS_TARGET
        if f2_windows:
            if not validate_f2_profile(identity["profile"]):
                raise ValueError("checkpoint F2 capability/profile mismatch")
            if (record.get("surface_id") not in identity["profile"]["surfaces"] or
                    record.get("surface_id") not in F2_SURFACE_MUTATORS):
                raise ValueError("checkpoint F2 surface mismatch")
            runtime_input(record["final_config"], backend="discovery_namespace")
        if "mutation_plan" in record:
            from r2f.core.mutation_plan import MutationPlan
            plan = MutationPlan.from_dict(record["mutation_plan"])
            plan.validate(contract)
            if (plan.final_config != record["final_config"] or
                    plan.to_dict()["operations"] != record.get("operations") or
                    plan.runtime_windows != record.get("runtime_windows") or
                    plan.contract_hash != record["contract_hash"]):
                raise ValueError("checkpoint corpus plan/input binding mismatch")
            if f2_windows:
                from r2f.modules.s1.providers import validate_exploratory_plan
                validate_exploratory_plan(plan, contract, target_id=target.target_id,
                    carrier="wire_proxy", mutation_kind="discovery_drop")
                if len(plan.operations) != 1 or any(
                        op.surface_id != record["surface_id"] or
                        op.mutator_id != F2_SURFACE_MUTATORS[record["surface_id"]] or
                        op.applicability_status != "needs_observation" for op in plan.operations):
                    raise ValueError("checkpoint F2 operation/observation guard mismatch")
        if record_key is not None:
            _records.add(record_key)
    pinned = {d for d, r in corpus.entries.items() if r.get("pinned_seed")}
    if pinned != set(seeds.entries):
        raise ValueError("checkpoint fixed seed identities differ from profile")
    for digest, seed in seeds.entries.items():
        record = corpus.entries[digest]
        for key, value in seed.items():
            if key != "candidate_ids" and record.get(key) != value:
                raise ValueError("checkpoint fixed seed record differs from profile")
    if corpus.history[:len(seeds.history)] != seeds.history:
        raise ValueError("checkpoint initial corpus history differs from profile")


@_route_validation_scope()
@_frontier_validation_scope()
def _validate_state(state, identity, seeds, registry):
    if not isinstance(state, dict):
        raise ValueError("checkpoint state must be a dict")
    model_parents = None
    profile_policy = identity["profile"].get("scheduler_policy", "bfs" if "search" in identity["profile"] else None)
    if profile_policy is not None and state["scheduler"].get("policy") != profile_policy:
        raise ValueError("checkpoint scheduler policy differs from profile search declaration")
    if state["scheduler"].get("policy") == "bfs":
        from .search_frontier import InputSearchFrontier
        from .exploratory_input import validate_search_profile
        validate_search_profile(identity["profile"])
        verified_frontier = InputSearchFrontier.from_dict(state["scheduler"].get("frontier"))
        # Qualification order matters: hashes alone do not prove generation.
        # No caller-supplied external parent is trusted before this succeeds.
        verified_frontier.validate_model(profile=identity["profile"], seeds=seeds, registry=registry)
        _remember_model_validated_frontier(state["scheduler"]["frontier"], verified_frontier)
        model_parents = {digest: deepcopy(node["entry"])
                         for digest, node in verified_frontier.nodes.items()}
    corpus = _restore_checkpoint_corpus(state["corpus"], model_parents=model_parents)
    feedback_data = state["feedback"]
    feedback = validated_feedback(feedback_data.get("events"))
    if feedback.to_dict() != feedback_data:
        raise ValueError("checkpoint feedback identity/summaries mismatch")
    for event in feedback.events:
        if event.get("edge_id") is not None:
            if not _sha(event["edge_id"]):
                raise ValueError("checkpoint feedback edge must be a canonical hash")
            provenance = event.get("edge_provenance") or {}
            if provenance.get("schema") != "r2f.s1-feedback-edge-provenance/v1":
                raise ValueError("checkpoint feedback edge is missing provenance")
            from r2f.core.state_signature import observation_edge
            observed = observation_edge(provenance.get("observation") or {})
            if not observed or observed[1] != event["edge_id"]:
                raise ValueError("checkpoint feedback edge provenance mismatch")
    scheduler = ExploratoryScheduler.from_dict(state["scheduler"], corpus, feedback,
                                               model_parents=model_parents)
    expected_warmup = identity["profile"].get("budget", {}).get("warmup_inputs", scheduler.warmup)
    if scheduler.warmup != expected_warmup:
        raise ValueError("checkpoint warmup differs from profile budget")
    if scheduler.feedback.events != feedback.events:
        raise ValueError("checkpoint feedback has not been advanced to scheduler")
    if state.get("corpus_identity") != corpus_identity(corpus):
        raise ValueError("checkpoint corpus supporting identity mismatch")
    if state.get("next_selection_sequence") != len(scheduler.selection_log):
        raise ValueError("checkpoint next sequence mismatch")
    validated_records = set()
    _validate_corpus_binding(corpus, identity, seeds, registry, _records=validated_records)
    for context in scheduler.selection_contexts:
        _validate_corpus_binding(_restore_checkpoint_corpus(context["corpus"], model_parents=model_parents),
                                identity, seeds, registry, _records=validated_records)
    continuation = deepcopy(state.get("continuation"))
    if not isinstance(continuation, dict):
        raise ValueError("checkpoint continuation must be a dict")
    pending = continuation.get("pending_selection")
    if isinstance(pending, dict) and has_routes(pending.get("entry", {})):
        validate_route_selection(pending["entry"], pending, profile=identity["profile"])
    if continuation.get("cycle_id") != identity["cycle_id"]:
        raise ValueError("checkpoint continuation cycle mismatch")
    budget = continuation.get("budget")
    if not isinstance(budget, dict):
        raise ValueError("checkpoint requires cumulative budget")
    for key, value in budget.items():
        if key in {"elapsed_sec", "max_seconds"}:
            if value is not None and (isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0):
                raise ValueError("invalid checkpoint elapsed budget")
        elif value is not None and (type(value) is not int or value < 0):
            raise ValueError("invalid checkpoint budget " + key)
    selected = budget.get("selected", budget.get("planned"))
    if selected != len(scheduler.selection_log):
        raise ValueError("checkpoint selected budget differs from sequence")
    previous = selected
    for key in ("planned", "generated", "attempted", "executed"):
        value = budget.get(key)
        if type(value) is not int or value > previous:
            raise ValueError("checkpoint planned/generated/attempted/executed budget mismatch")
        previous = value
    for name, counter in (("seen_input_hashes", "unique_inputs"), ("seen_actual_hashes", "unique_actual")):
        hashes = continuation.get(name, [])
        if not isinstance(hashes, list) or any(not _sha(h) for h in hashes) or len(hashes) != len(set(hashes)):
            raise ValueError("invalid checkpoint seen input identities")
        if counter in budget and budget[counter] != len(hashes):
            raise ValueError("checkpoint unique input budget mismatch")
    actual = budget.get("actual", budget["executed"])
    if type(actual) is not int or not budget["executed"] <= actual <= budget["attempted"]:
        raise ValueError("checkpoint actual execution budget mismatch")
    if scheduler.policy == "bfs":
        tasks = scheduler.frontier.tasks
        if sum(t["actual"] for t in tasks) != actual:
            raise ValueError("checkpoint input search actual-task budget mismatch")
        executable_generated = sum(t.get("generated_record") is not None and
                                   t.get("generation_outcome") not in
                                   {"compile_time_no_change", "no_effective_input", "guard_terminal"} for t in tasks)
        if executable_generated != budget["generated"]:
            raise ValueError("checkpoint input search generated-task budget mismatch")
        for task in tasks:
            if task["actual"]:
                execution = task.get("execution")
                if (task["status"] != "completed" or not isinstance(execution, dict) or
                        not isinstance(execution.get("cell_id"), str) or
                        execution.get("content_sha256") != task["generated_record"]["content_sha256"]):
                    raise ValueError("checkpoint input search execution/task binding mismatch")
    if (budget.get("unique_inputs", 0) > budget["generated"] or
            budget.get("unique_actual", 0) > actual):
        raise ValueError("checkpoint unique/generated/actual budget mismatch")
    if len(feedback.events) != budget["attempted"] or sum(e["executed"] for e in feedback.events) != actual:
        raise ValueError("checkpoint feedback/attempt/actual counts mismatch")
    if not set(continuation.get("seen_actual_hashes", [])) <= set(continuation.get("seen_input_hashes", [])):
        raise ValueError("checkpoint actual hashes are not generated inputs")
    if budget.get("warmup_inputs", scheduler.warmup) != scheduler.warmup:
        raise ValueError("checkpoint warmup budget mismatch")
    pending = continuation.get("pending_selection")
    phase = continuation.get("phase")
    if phase not in {"planned", "recorded", "final"}:
        raise ValueError("invalid checkpoint phase")
    if pending is not None:
        if phase not in {"planned", "final"} or not scheduler.selection_log:
            raise ValueError("invalid checkpoint pending phase")
        expected = deepcopy(scheduler.selection_log[-1])
        if scheduler.policy == "bfs":
            expected["entry"] = scheduler.frontier.entry(expected)
            task = scheduler.frontier.task(expected["task_id"])
            if task["status"] not in {"planned", "submitted"}:
                raise ValueError("checkpoint pending input search task is not planned/submitted")
        else:
            snapshot = scheduler.selection_contexts[-1]["corpus"]
            expected["entry"] = next(r for r in snapshot["entries"] if r["content_sha256"] == expected["content_sha256"])
            if "mutation_route" in expected:
                expected["entry"] = route_view(expected["entry"], expected["mutation_route"])
        if pending != expected:
            raise ValueError("checkpoint pending selection identity mismatch")
        if phase == "final" and continuation.get("stop_reason") not in {"resume_pending_execution_uncertain", "cycle_deadline_reached"}:
            raise ValueError("final checkpoint cannot silently discard pending work")
        if not isinstance(continuation.get("pending_attempt"), dict):
            raise ValueError("checkpoint pending attempt provenance missing")
    elif phase == "planned":
        raise ValueError("planned checkpoint is missing pending selection")
    if scheduler.policy == "bfs":
        active = [t for t in scheduler.frontier.tasks if t["status"] in {"selected", "planned", "submitted"}]
        if len(active) != (1 if pending is not None else 0):
            raise ValueError("checkpoint input search active task/pending mismatch")
        if pending is not None and active[0]["task_id"] != pending["task_id"]:
            raise ValueError("checkpoint input search pending task identity mismatch")
    return corpus, feedback, scheduler, continuation


def _atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_text(path, json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")


def _atomic_text(path, encoded):
    """Write pre-encoded JSON without changing atomic replace/fsync semantics."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".checkpoint-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def make_checkpoint_hook(output_dir, *, profile, cycle_id, source_manifest, continuation=None):
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("checkpoint requires a new empty output directory")
    identity, seeds, registry = _identity(profile, cycle_id, source_manifest)
    sequence, last_hash = 0, None
    inherited = deepcopy(continuation or {})
    # Owned exclusively by this writer and fixed full source/profile/registry
    # identity. It never grants acceptance to a supplied state. Cold loaders do
    # not receive it, even if invoked from a writer's call stack.
    model_memo = {"identity_key": _memo_key(identity, allow_alias=True),
                  "plans": {}, "node": {}, "noop": {}}

    def hook(payload):
        nonlocal sequence, last_hash
        source = payload.get("source_manifest", payload.get("source", source_manifest))
        if source != source_manifest or payload.get("profile", profile) != profile:
            raise ValueError("checkpoint source/profile changed during cycle")
        if payload.get("cycle_id", cycle_id) != cycle_id:
            raise ValueError("checkpoint cycle changed")
        for field in ("target_manifest", "required_catalog_sha256"):
            if field in payload and payload[field] != identity[field]:
                raise ValueError("checkpoint " + field + " differs from registry")
        state = {k: _serialized(payload[k]) for k in ("corpus", "feedback", "scheduler")}
        state["corpus_identity"] = corpus_identity(state["corpus"])
        state["next_selection_sequence"] = len(state["scheduler"]["selections"])
        next_state = deepcopy(inherited)
        for key, value in payload.items():
            if key not in {"corpus", "feedback", "scheduler", "source", "source_manifest", "profile", "target_manifest", "required_catalog_sha256"}:
                next_state[key] = deepcopy(value)
        next_state["cycle_id"] = cycle_id
        if next_state.get("pending_selection") is not None and payload.get("phase") == "planned":
            next_state["pending_attempt"] = _pending_attempt(output, state, payload)
        elif next_state.get("pending_selection") is None:
            next_state["pending_attempt"] = None
        state["continuation"] = next_state
        try:
            with _hook_model_validation_scope(model_memo):
                _validate_state(state, identity, seeds, registry)
        except BaseException:
            # A failed write is never a trusted validation checkpoint.
            for name in ("plans", "node", "noop"):
                model_memo[name].clear()
            raise
        envelope = {"schema": SCHEMA, "identity": identity, "state": state,
                    "snapshot_sequence": sequence, "previous_checkpoint_sha256": last_hash}
        envelope["checkpoint_sha256"] = canonical_hash(envelope)
        path = output / "checkpoint.json"
        if path.exists():
            prior = json.loads(path.read_text())
            if (last_hash is None or prior.get("checkpoint_sha256") != last_hash or
                    canonical_hash({k: v for k, v in prior.items() if k != "checkpoint_sha256"}) != last_hash):
                raise ValueError("checkpoint output was modified outside this writer")
        archive = output / "checkpoints" / f"{sequence:08d}-{next_state['phase']}.json"
        if archive.exists():
            raise ValueError("checkpoint immutable snapshot already exists")
        # Both durable destinations contain exactly the same envelope bytes.
        # Encode once; do not stream/re-encode the full growing frontier twice.
        encoded = json.dumps(envelope, sort_keys=True, indent=2, allow_nan=False) + "\n"
        _atomic_text(archive, encoded)
        _atomic_text(path, encoded)
        sequence += 1
        last_hash = envelope["checkpoint_sha256"]
        return path

    return hook


def load_checkpoint(path, *, profile, cycle_id, source_manifest, output_dir):
    source = Path(path).resolve()
    if source.is_dir():
        source = source / "checkpoint.json"
    output = Path(output_dir).resolve()
    if output == source.parent or output in source.parents or source.parent in output.parents:
        raise ValueError("resume output must be independent of old checkpoint evidence")
    if output.exists() and any(output.iterdir()):
        raise ValueError("resume requires a new empty output directory")
    data = json.loads(source.read_text())
    if data.get("schema") != SCHEMA or data.get("checkpoint_sha256") != canonical_hash({k: v for k, v in data.items() if k != "checkpoint_sha256"}):
        raise ValueError("checkpoint payload hash/schema mismatch")
    number = data.get("snapshot_sequence")
    previous = data.get("previous_checkpoint_sha256")
    if type(number) is not int or number < 0 or (previous is not None and not _sha(previous)) or (number == 0) != (previous is None):
        raise ValueError("checkpoint snapshot chain metadata mismatch")
    identity, seeds, registry = _identity(profile, cycle_id, source_manifest)
    if data.get("identity") != identity:
        raise ValueError("checkpoint source/target/profile/catalog identity mismatch")
    with _hook_model_validation_scope(None):
        corpus, feedback, scheduler, continuation = _validate_state(data["state"], identity, seeds, registry)
    if continuation.get("pending_selection") is not None:
        reasons = _pending_refusal(continuation)
        if reasons:
            audit = {"schema": "r2f.exploratory-resume-refusal/v1",
                     "reason_code": "resume_pending_execution_uncertain",
                     "disposition": "interrupted_attempt_requires_explicit_review",
                     "automatic_runtime_called": False, "reasons": reasons,
                     "checkpoint_path": str(source), "checkpoint_sha256": data["checkpoint_sha256"],
                     "pending_attempt": deepcopy(continuation.get("pending_attempt"))}
            _atomic_json(output / "resume-refusal.json", audit)
            raise CheckpointResumeError("resume_pending_execution_uncertain: " + ";".join(reasons))
    continuation["resumed_from"] = {"path": str(source), "checkpoint_sha256": data["checkpoint_sha256"]}
    return {"corpus": corpus, "feedback": feedback, "scheduler": scheduler, "continuation": continuation}
