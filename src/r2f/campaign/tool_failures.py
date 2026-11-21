"""Pure projections of canonical feedback, never a separate persisted ledger.

Five/reset/partition/release semantics are main's issue10 CP1 engineering
adoption (2026-11-21), not a new SUT rule or a quotation of the specification.
"""
from copy import deepcopy
import re

from r2f.core.evidence import canonical_hash
from r2f.core.observation_status import observation_complete, tool_error

RECIPE_SCHEMA = "r2f.generation-recipe/v1"
ATTEMPT_SCHEMA = "r2f.recipe-original-attempt/v1"
RELEASE_SCHEMA = "r2f.recipe-isolation-release/v1"
THRESHOLD = 5


def _operator_route(op, entry, declared_class=None):
    parameters = op.parameters
    request = parameters.get("request") or {}
    context = request.get("context") or {}
    # Concrete topology operators share one bridge mutator, so retain their
    # declared identity and parameter class, in order, rather than the bridge
    # label alone. Numeric sampled values and full receipt contents stay out.
    result = {"surface_id": op.surface_id, "mutator_id": op.mutator_id,
              "application_phase": op.application_phase}
    for key in ("operator_id", "slot_id"):
        if parameters.get(key) is not None:
            result[key] = deepcopy(request.get(key, parameters[key]))
    for key in ("boundary_class", "sampling_kind", "parameter_class"):
        value = context.get(key, parameters.get(key))
        if value is not None:
            result[key] = deepcopy(value)
    result.setdefault("parameter_class", declared_class or context.get("boundary_class") or context.get("pair") or
                      parameters.get("boundary_class") or entry.get("parameter_class") or
                      entry.get("boundary_class"))
    return result


def generation_recipe(entry, target, plan, *, source, carrier, mutation_kind,
                      runtime_boundary, profile, sampling_kind, selection=None, observation_mode="black_box"):
    """Declare semantic generation axes; sampled values/content are not keys.

    Operation order/identity and parameter *class* remain explicit. RNG, input
    hashes, domain, clock, run id and parent contents are deliberately absent.
    """
    selection = selection or {}
    samples = selection.get("layer_samples") or []
    classes = [sample.get("class_id") for sample in samples]
    if not classes:
        classes = [selection.get("class_id")] * len(plan.operations)
    return {
        "schema": RECIPE_SCHEMA,
        "source": {k: deepcopy(source.get(k)) for k in
                   ("source_repo", "head", "behavior_tree_sha256")},
        "rmw": entry["rmw"],
        "seed": {"seed_id": entry.get("seed_id"), "variant_id": entry.get("variant_id")},
        "target": {"target_id": target.target_id, "target_revision": target.target_revision,
                   "contract_hash": plan.contract_hash,
                   "target_sha256": canonical_hash(target.to_dict()),
                   "rule_sha256": canonical_hash(target.rule_contract)},
        "ordered_route": [_operator_route(op, entry, classes[i] if i < len(classes) else None)
                          for i, op in enumerate(plan.operations)],
        "parameter_class": entry.get("parameter_class") or entry.get("boundary_class"),
        "boundary_class": entry.get("boundary_class"),
        "sampling_kind": sampling_kind,
        "carrier": carrier, "mutation_kind": mutation_kind,
        "mode": {"runtime_boundary": runtime_boundary,
                 "runtime_capability": profile.get("runtime_capability"),
                 "observation_mode": observation_mode},
    }


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _sha(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def validate_recipe(recipe):
    if not isinstance(recipe, dict) or recipe.get("schema") != RECIPE_SCHEMA:
        raise ValueError("invalid semantic generation recipe")
    if set(recipe) != {"schema", "source", "rmw", "seed", "target", "ordered_route",
                       "parameter_class", "boundary_class", "sampling_kind", "carrier",
                       "mutation_kind", "mode"}:
        raise ValueError("incomplete semantic generation recipe")
    source, target, seed, mode = (recipe.get(k) for k in ("source", "target", "seed", "mode"))
    if (not isinstance(source, dict) or set(source) != {"source_repo", "head", "behavior_tree_sha256"}
            or not all(_text(source.get(k)) for k in ("source_repo", "head"))
            or not _sha(source.get("behavior_tree_sha256"))):
        raise ValueError("recipe requires source partition")
    if (not isinstance(target, dict) or set(target) != {"target_id", "target_revision", "contract_hash", "target_sha256", "rule_sha256"}
            or not _text(target.get("target_id")) or type(target.get("target_revision")) is not int
            or target["target_revision"] < 0
            or not all(_sha(target.get(k)) for k in ("contract_hash", "target_sha256", "rule_sha256"))):
        raise ValueError("recipe requires target contract partition")
    if (not isinstance(seed, dict) or set(seed) != {"seed_id", "variant_id"}
            or not _text(seed.get("seed_id"))
            or seed.get("variant_id") is not None and not _text(seed["variant_id"])):
        raise ValueError("recipe requires explicit seed identity")
    if (not isinstance(mode, dict) or set(mode) != {"runtime_boundary", "runtime_capability", "observation_mode"}
            or not all(_text(mode.get(k)) for k in ("runtime_boundary", "observation_mode"))
            or mode.get("runtime_capability") is not None and not _text(mode["runtime_capability"])):
        raise ValueError("recipe requires explicit execution mode")
    for name in ("rmw", "parameter_class", "boundary_class", "carrier", "mutation_kind"):
        if not _text(recipe.get(name)):
            raise ValueError("recipe requires " + name)
    if recipe["sampling_kind"] is not None and not _text(recipe["sampling_kind"]):
        raise ValueError("invalid recipe sampling class")
    route = recipe["ordered_route"]
    if not isinstance(route, list) or not route:
        raise ValueError("recipe requires ordered operator route")
    for op in route:
        required = {"surface_id", "mutator_id", "application_phase", "parameter_class"}
        optional = {"operator_id", "slot_id", "boundary_class", "sampling_kind"}
        if (not isinstance(op, dict) or not required <= set(op) or set(op) - required - optional
                or not all(_text(v) for v in op.values())):
            raise ValueError("invalid recipe operator identity")
    return canonical_hash(recipe)


def global_invalidity(row, runtime):
    """Named global invalidity outranks local streak and expected rejection.

    Ordinary incomplete observations do not imply a globally invalid observer.
    Keep the exact reason; no failed conflict is retried up to the threshold.
    """
    for name in ("source_valid", "locks_valid", "observer_valid"):
        if runtime.get(name) is False or row.get(name) is False:
            return "global_" + name + "_false"
    environment = runtime.get("environment")
    values = [row.get("abort_reason"), runtime.get("reason"),
              environment.get("reason") if isinstance(environment, dict) else None,
              (runtime.get("runner_exception") or {}).get("detail")]
    prefixes = ("domain_conflict", "lock_conflict", "source_repo_HEAD_or_behavior_tree_changed",
                "source_identity_invalid", "source_identity_changed", "global_observer_invalid", "observer_invalid",
                "observer_contract_invalid", "observer_source_invalid")
    for value in values:
        if isinstance(value, str) and value.startswith(prefixes):
            return value
    return None


def attempt_receipt(recipe, row, runtime, result_reference):
    # Explicit API rejection is neutral only when the wrapper itself worked.
    # Persist/ingest tool failure outranks a returned rejection label; exception
    # text alone never declares an expected API rejection.
    rejected = (runtime.get("runtime_status") == "expected_api_rejection"
                and not runtime.get("runner_exception"))
    global_reason = global_invalidity(row, runtime)
    outcome = ("global_invalid" if global_reason else
               "tool_failure" if tool_error(row) or row.get("runner_exception") else
               "expected_api_rejection" if rejected else
               "healthy" if observation_complete(row) else "incomplete")
    return {"schema": ATTEMPT_SCHEMA, "recipe": deepcopy(recipe),
            "recipe_sha256": validate_recipe(recipe), "attempt_kind": "original",
            "submitted": True, "run_id": row["run_id"], "cell_id": row["cell_id"],
            "outcome": outcome,
            "diagnostic": {"runtime_status": runtime.get("runtime_status"),
                           "reason": global_reason or runtime.get("reason") or row.get("abort_reason"),
                           "runtime_result": deepcopy(result_reference),
                           "raw_path": row.get("raw_path"), "raw_sha256": row.get("raw_sha256")}}


def validate_policy_event(event):
    """Validate optional extensions without rewriting legacy feedback bytes."""
    attempt = event.get("recipe_attempt")
    release = event.get("recipe_policy_release")
    if attempt is not None and release is not None:
        raise ValueError("recipe attempt and release are distinct feedback events")
    if attempt is not None:
        if (not isinstance(attempt, dict) or attempt.get("schema") != ATTEMPT_SCHEMA
                or attempt.get("recipe_sha256") != validate_recipe(attempt.get("recipe"))
                or attempt.get("attempt_kind") != "original" or attempt.get("submitted") is not True
                or not all(_text(attempt.get(k)) for k in ("run_id", "cell_id"))
                or attempt.get("outcome") not in {"tool_failure", "healthy", "incomplete", "expected_api_rejection", "global_invalid"}):
            raise ValueError("invalid original recipe attempt receipt")
        diagnostic = attempt.get("diagnostic")
        reference = diagnostic.get("runtime_result") if isinstance(diagnostic, dict) else None
        if (not isinstance(reference, dict) or not _text(reference.get("path"))
                or not _sha(reference.get("sha256"))
                or (reference.get("basis"), reference.get("status")) not in {
                    ("persisted_file", "available"), ("returned_value", "unavailable")}):
            raise ValueError("recipe attempt requires runtime diagnostic reference")
        if attempt["outcome"] == "global_invalid" and event["complete"]:
            raise ValueError("globally invalid recipe evidence cannot claim coverage")
        if attempt["outcome"] == "healthy" and not event["complete"]:
            raise ValueError("recipe reset requires completed healthy observation")
        if attempt["outcome"] == "tool_failure" and not event["tool_error"]:
            raise ValueError("recipe tool failure requires tool attribution")
    if release is not None:
        basis = release.get("revalidation_basis") if isinstance(release, dict) else None
        if (not isinstance(release, dict) or release.get("schema") != RELEASE_SCHEMA
                or release.get("recipe_sha256") != validate_recipe(release.get("recipe"))
                or not all(_text(release.get(k)) for k in ("owner", "reason"))
                or not isinstance(basis, dict) or set(basis) != {"path", "sha256"}
                or not _text(basis.get("path")) or not _sha(basis.get("sha256"))
                or not _sha(release.get("isolating_event_sha256"))
                or event["executed"] or event["coverage"] or event["tool_error"]):
            raise ValueError("recipe release requires named owner revalidation and isolation reference")


def submission_policy(events, recipe):
    """Fold the accepted chronological feedback prefix; preserve all history."""
    key = validate_recipe(recipe)
    streak, isolated, cause = 0, False, None
    references, unkeyed, seen = [], [], {}
    for event in events:
        validate_policy_event(event)
        attempt, release = event.get("recipe_attempt"), event.get("recipe_policy_release")
        if attempt is None and release is None:
            if event.get("tool_error"):
                unkeyed.append(event["event_sha256"])
            continue
        if release is not None:
            if release["recipe_sha256"] == key:
                if not isolated or release["isolating_event_sha256"] != cause:
                    raise ValueError("recipe release does not reference current isolation")
                streak, isolated, cause = 0, False, None
            continue
        identity = (attempt["run_id"], attempt["cell_id"])
        if identity in seen:
            if seen[identity] != attempt:
                raise ValueError("conflicting original recipe attempt identity")
            continue
        seen[identity] = attempt
        if attempt["recipe_sha256"] != key:
            continue
        references.append(event["event_sha256"])
        if isolated:
            continue  # only a named release can clear isolation
        if attempt["outcome"] == "tool_failure":
            streak += 1
            if streak >= THRESHOLD:
                isolated, cause = True, event["event_sha256"]
        elif attempt["outcome"] == "healthy":
            streak = 0
    return {"schema": "r2f.recipe-tool-failure-policy/v1", "recipe": deepcopy(recipe),
            "recipe_sha256": key, "threshold": THRESHOLD,
            "consecutive_tool_failures": streak,
            # Unknown legacy attribution is diagnostic uncertainty, not proof
            # of failures in this key. Preserve its references independently.
            "status": "isolated" if isolated else "eligible",
            "submission_allowed": not isolated,
            "isolating_event_sha256": cause, "attempt_event_refs": references,
            "unattributed_tool_event_refs": unkeyed}


def validate_release_history(events):
    """A rehashed stale release cannot clear a different/truncated isolation."""
    for index, event in enumerate(events):
        release = event.get("recipe_policy_release")
        if release is not None:
            submission_policy(events[:index + 1], release["recipe"])
