"""Episode-level replay: whole-program first, dependency-preserving reduction.

Replay is performed only through injected callables.  It never imports ROS,
never writes a candidate and never mutates the original episode record.  A
reduction is accepted only when the attribution signature of the reduced
program equals the original signature; otherwise the report keeps the
mismatch as a failed reduction instead of rewriting the anomaly.
"""
from __future__ import annotations

from copy import deepcopy

from r2f.core.evidence import canonical_hash
from .episode import (Episode, EpisodeError, EXECUTION_SCHEMA, run_episode)

SCHEMA = "r2f.s1-episode-replay/v1"
REDUCTION_SCHEMA = "r2f.s1-episode-reduction/v1"
REPLAY_KINDS = frozenset({"health", "tool_failure"})


class EpisodeReplayError(EpisodeError):
    """Replay/refusal contract violation."""


def attribution_signature(execution):
    """Identity of the anomaly a reduction must preserve.

    It is derived only from the terminal receipt plus the stop reason.  Action
    ordering, timing or unrelated successful actions do not enter identity.
    """
    if not isinstance(execution, dict) or execution.get("schema") != EXECUTION_SCHEMA:
        raise EpisodeReplayError("attribution requires an episode execution record")
    receipts = execution.get("receipts")
    if not isinstance(receipts, list) or not receipts:
        raise EpisodeReplayError("attribution requires at least one action receipt")
    terminal = receipts[-1]
    signature = {
        "schema": "r2f.s1-episode-attribution/v1",
        "stop_reason": execution.get("stop_reason"),
        "action_id": terminal.get("action_id"),
        "sequence": terminal.get("sequence"),
        "action": terminal.get("action"),
        "rule_id": terminal.get("rule_id"),
        "oracle_verdict": terminal.get("oracle_verdict"),
        "reason_code": terminal.get("reason_code"),
        "responsibility_layer": terminal.get("responsibility_layer"),
        "tool_error": terminal.get("tool_error") is True,
    }
    return signature


def _check_original(original):
    if not isinstance(original, dict) or original.get("schema") != EXECUTION_SCHEMA:
        raise EpisodeReplayError("original execution record is required")
    received = original.get("execution_sha256")
    if received != canonical_hash({k: v for k, v in original.items() if k != "execution_sha256"}):
        raise EpisodeReplayError("original execution hash mismatch")


def _compare_full(episode, replay, original):
    expected = [item["action_id"] for item in original["receipts"]]
    actual = [item["action_id"] for item in replay["receipts"]]
    order_equal = actual == expected[:len(actual)]
    intervals = []
    for receipt in replay["receipts"]:
        timing = receipt.get("timing") or {}
        intervals.append({"action_id": receipt["action_id"],
                          "logical_order": timing.get("logical_order"),
                          "started_monotonic": timing.get("started_monotonic"),
                          "finished_monotonic": timing.get("finished_monotonic")})
    return {
        "schema": "r2f.s1-episode-full-replay/v1",
        "action_sequence_equal": order_equal,
        "stop_reason": replay.get("stop_reason"),
        "stop_reason_match": replay.get("stop_reason") == original.get("stop_reason"),
        "replayed_actions": actual,
        "expected_actions": expected,
        "replay_execution_sha256": replay["execution_sha256"],
        "timeline": intervals,
        "os_concurrency_claimed": False,
        "logical_order_preserved": all(
            intervals[index]["logical_order"] == index for index in range(len(intervals))),
    }


def _dependency_closure(episode, keep_ids):
    """Keep closure over explicit action dependencies and provides/requires."""
    by_id = {item.action_id: item for item in episode.actions}
    sequence_to_id = {item.sequence: item.action_id for item in episode.actions}
    providers = {}
    for item in episode.actions:
        for symbol in item.provides:
            providers.setdefault(symbol, []).append(item.action_id)
    selected, queue = set(), list(keep_ids)
    while queue:
        identifier = queue.pop()
        if identifier in selected:
            continue
        action = by_id.get(identifier)
        if action is None:
            raise EpisodeReplayError("reduction references unknown action: " + str(identifier))
        selected.add(identifier)
        for sequence in action.depends_on:
            dependency = sequence_to_id.get(sequence)
            if dependency is None:
                raise EpisodeReplayError("reduction dependency has no action: " + str(sequence))
            queue.append(dependency)
        for symbol in action.requires:
            candidates = providers.get(symbol)
            if not candidates:
                raise EpisodeReplayError("reduction dependency has no provider: " + symbol)
            if len(candidates) > 1:
                raise EpisodeReplayError("reduction dependency provider is ambiguous: " + symbol)
            queue.extend(candidates)
    # Emit in original program order.
    return [item.action_id for item in episode.actions if item.action_id in selected]


def reduce_actions(episode, original):
    """Return the action-id order of the smallest attribution-preserving plan."""
    signature = attribution_signature(original)
    if signature["action_id"] is None:
        raise EpisodeReplayError("attribution requires a terminal action id")
    reduced_ids = _dependency_closure(episode, [signature["action_id"]])
    return {"action_ids": reduced_ids, "attribution": signature}


def _subset_episode(episode, action_ids):
    by_id = {item.action_id: item for item in episode.actions}
    if any(identifier not in by_id for identifier in action_ids):
        raise EpisodeReplayError("subset contains unknown action ids")
    actions = [deepcopy(by_id[identifier]) for identifier in action_ids]
    actions.sort(key=lambda item: item.sequence)
    sequences = [item.sequence for item in actions]
    if len(set(sequences)) != len(sequences):
        raise EpisodeReplayError("subset action sequences collide")
    # Preserve original sequence numbers: dependencies index earlier sequences.
    return Episode(
        episode_id=episode.episode_id + ".reduced",
        target_id=episode.target_id,
        target_revision=episode.target_revision,
        rmw=episode.rmw,
        contract_hash=episode.contract_hash,
        actions=actions,
        initial_state=deepcopy(episode.initial_state),
        limits=episode.limits,
        allowed_state_ids=None if episode.allowed_state_ids is None else list(episode.allowed_state_ids),
        parent_episode_ids=list(episode.parent_episode_ids),
        tags=deepcopy(episode.tags),
        isolation_mode=episode.isolation_mode,
    )


def _require_signature_match(original, reduced, *, mode):
    expected, actual = attribution_signature(original), attribution_signature(reduced)
    if expected != actual:
        raise EpisodeReplayError(
            "reduced replay changed anomaly attribution: " + canonical_hash(
                {"expected": expected, "actual": actual}))
    if mode == "tool_failure" and actual["tool_error"] is not True:
        raise EpisodeReplayError("tool-failure replay did not reproduce a harness tool failure")
    if mode == "health" and actual["tool_error"] is True:
        raise EpisodeReplayError("health replay unexpectedly reported a tool failure")
    return {"equal": True, "attribution_sha256": canonical_hash(expected)}


def replay_episode(episode, original, *, mode, perform, clock, health=None,
                   reduce=True, store=None):
    """Full replay followed by a dependency-preserving reduction.

    ``mode`` separates a healthy replay from a controlled tool-failure replay;
    both use the same injected backend but never share a report status.
    """
    if not isinstance(episode, Episode):
        raise EpisodeReplayError("replay requires an Episode")
    if mode not in REPLAY_KINDS:
        raise EpisodeReplayError("replay mode must be health or tool_failure")
    _check_original(original)
    if original.get("episode_id") != episode.episode_id:
        raise EpisodeReplayError("original execution belongs to a different episode")
    if original.get("input_sha256") != episode.input_identity():
        raise EpisodeReplayError("original execution input identity differs")
    if not callable(perform) or not callable(clock):
        raise EpisodeReplayError("replay requires injected perform and clock callables")
    full = run_episode(episode, perform=perform, clock=clock, health=health, store=store)
    full_report = _compare_full(episode, full, original)
    report = {
        "schema": SCHEMA,
        "replay_kind": mode,
        "episode_id": episode.episode_id,
        "episode_input_sha256": episode.input_identity(),
        "original_execution_sha256": original["execution_sha256"],
        "original_attribution": attribution_signature(original),
        "full": full_report,
        "full_execution": deepcopy(full),
        "reduced": None,
        "candidate_writes": 0,
        "maturity_denominator_changed": False,
        "os_concurrency_claimed": False,
    }
    if mode == "tool_failure":
        report["candidate_eligible"] = False
        report["responsibility_layer"] = "harness"
    full_status = "full_replay"
    if full.get("action_sequence_equal") is not True:
        full_status = "full_replay_diverged"
    if full.get("stop_reason") is not None and full.get("stop_reason") != original.get("stop_reason"):
        full_status = "full_replay_diverged"
    if mode == "health" and any(item.get("tool_error") is True for item in full.get("receipts", [])):
        full_status = "health_replay_backend_tool_failure"
        report["candidate_eligible"] = False
        report["responsibility_layer"] = "harness"
    report["status"] = full_status
    if reduce:
        reduction = reduce_actions(episode, original)
        reduced_episode = _subset_episode(episode, reduction["action_ids"])
        reduced_execution = run_episode(reduced_episode, perform=perform, clock=clock,
                                        health=health, store=None)
        try:
            match = _require_signature_match(original, reduced_execution, mode=mode)
            reduction_report = {
                "schema": REDUCTION_SCHEMA,
                "action_ids": reduction["action_ids"],
                "original_action_count": len(episode.actions),
                "reduced_action_count": len(reduction["action_ids"]),
                "attribution": match,
                "reduced_execution": deepcopy(reduced_execution),
                "accepted": True,
                "os_concurrency_claimed": False,
            }
        except EpisodeReplayError as exc:
            reduction_report = {
                "schema": REDUCTION_SCHEMA,
                "action_ids": reduction["action_ids"],
                "original_action_count": len(episode.actions),
                "reduced_action_count": len(reduction["action_ids"]),
                "attribution": {"equal": False, "reason": str(exc)},
                "reduced_execution": deepcopy(reduced_execution),
                "accepted": False,
                "os_concurrency_claimed": False,
            }
        reduction_report["reduction_sha256"] = canonical_hash(
            {k: v for k, v in reduction_report.items() if k != "reduction_sha256"})
        report["reduced"] = reduction_report
    report["report_sha256"] = canonical_hash({k: v for k, v in report.items() if k != "report_sha256"})
    return report


def replay_attempt_record(report, *, attempt_id):
    """Project an offline replay into the core replay-attempt field set.

    Offline injected callables are never a live ROS replay; the status is
    labelled inconclusive unless a caller with a real qualification layer
    replaces it.  This function does not write candidates.
    """
    if not isinstance(report, dict) or report.get("schema") != SCHEMA:
        raise EpisodeReplayError("replay report is required")
    if not isinstance(attempt_id, str) or not attempt_id:
        raise EpisodeReplayError("attempt_id must be a nonempty string")
    full = report.get("full") or {}
    accepted = bool((report.get("reduced") or {}).get("accepted"))
    return {
        "attempt_id": attempt_id,
        "status": "inconclusive",
        "failure_reason": ("offline injected replay is not a live ROS qualification"
                           if not accepted else "offline reduction matches attribution; live replay still required"),
        "replay_kind": report.get("replay_kind"),
        "logical_order_preserved": full.get("logical_order_preserved") is True,
        "reduction_accepted": accepted,
        "report_sha256": report.get("report_sha256"),
        "live_ros_claimed": False,
        "candidate_eligible": False,
    }
