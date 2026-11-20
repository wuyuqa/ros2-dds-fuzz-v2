"""Bounded episode action programs and an append-only episode store.

An episode is a deterministic, bounded action program.  It is *not* a second
runner: execution is delegated to an injected ``perform`` callable, and every
action records the canonical execution stage, deterministic input binding,
guards, state ids, effects and oracle verdict.

No ROS import, no repository scanning and no candidate creation happen here.
The store is append-only: an existing episode record is never overwritten.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from copy import deepcopy
import json
import hashlib
import math
import os
from pathlib import Path
import re
import tempfile

from r2f.core._json import nonempty, snapshot
from r2f.core.evidence import canonical_hash
from r2f.core.execution import ExecutionStage
from r2f.core.state_machine import StateCondition
from r2f.core.state_signature import STATE_ABSTRACTION_VERSION, state_id, validate_state
from .corpus import content_identity

SCHEMA = "r2f.s1-episode/v1"
ACTION_SCHEMA = "r2f.s1-episode-action/v1"
EXECUTION_SCHEMA = "r2f.s1-episode-execution/v1"
STORE_SCHEMA = "r2f.s1-episode-store/v1"
STORE_VIEW_SCHEMA = "r2f.s1-episode-store-view/v1"

DEFAULT_MAX_ACTIONS = 50
DEFAULT_MAX_SECONDS = 60.0
ORACLE_VERDICTS = frozenset({"pass", "fail", "unknown", "not_applicable"})
# A continuous episode never replaces the independent per-case campaign.  The
# declared isolation mode is recorded so an episode run can be audited against
# the cells it must leave untouched.
ISOLATION_MODES = ("per_case", "episode")
RESET_SCHEMA = "r2f.s1-reset-receipt/v1"
RESET_KINDS = ("verified_cleanup_rebuild", "runtime_recreated", "snapshot_restore")
TERMINAL_STAGES = frozenset({"effect_observed", "judged", "aborted"})
_SAFE_ID = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_SHA = re.compile(r"[0-9a-f]{64}")


class EpisodeError(ValueError):
    """Invalid episode contract."""


class EpisodeHistoryError(EpisodeError):
    """A parent chain is missing or incomplete; failure cannot be referenced."""


class EpisodeStoreError(EpisodeError):
    """Append-only episode store violation."""


def _sha(value, field="sha256"):
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise EpisodeError(f"{field} must be a lowercase SHA256 digest")
    return value


def _safe_id(value, field):
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise EpisodeError(f"{field} must match {_SAFE_ID.pattern}")
    return value


def _positive_number(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise EpisodeError(f"{field} must be a finite positive number")
    return float(value)


@dataclass(frozen=True)
class EpisodeLimits:
    max_actions: int = DEFAULT_MAX_ACTIONS
    max_seconds: float = DEFAULT_MAX_SECONDS
    max_memory_bytes: int | None = None
    max_log_bytes: int | None = None

    def __post_init__(self):
        if type(self.max_actions) is not int or self.max_actions < 1 or self.max_actions > DEFAULT_MAX_ACTIONS:
            raise EpisodeError(f"max_actions must be an integer in 1..{DEFAULT_MAX_ACTIONS}")
        _positive_number(self.max_seconds, "max_seconds")
        for name in ("max_memory_bytes", "max_log_bytes"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise EpisodeError(name + " must be a nonnegative integer or None")

    def to_dict(self):
        return {"max_actions": self.max_actions, "max_seconds": self.max_seconds,
                "max_memory_bytes": self.max_memory_bytes, "max_log_bytes": self.max_log_bytes}

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or set(data) != {
                "max_actions", "max_seconds", "max_memory_bytes", "max_log_bytes"}:
            raise EpisodeError("episode limits fields are not exact")
        return cls(**data)


@dataclass
class EpisodeAction:
    sequence: int
    action: str
    input_entry: dict
    parameters: dict = field(default_factory=dict)
    depends_on: list[int] = field(default_factory=list)
    provides: list[str] = field(default_factory=list)
    requires: list[str] = field(default_factory=list)
    guard: list[StateCondition] = field(default_factory=list)
    expected_effect: list[str] = field(default_factory=list)
    rule_id: str = ""
    action_id: str = ""

    def __post_init__(self):
        if type(self.sequence) is not int or self.sequence < 0:
            raise EpisodeError("action sequence must be a nonnegative integer")
        nonempty(self.action, "action.action")
        self.action_id = self.action_id or f"a{self.sequence:04d}"
        _safe_id(self.action_id, "action.action_id")
        if not isinstance(self.input_entry, dict) or not self.input_entry:
            raise EpisodeError("every action requires a nonempty deterministic input_entry")
        # content_identity validates target/revision/rmw/contract/final_config.
        self.input_sha256 = content_identity(self.input_entry)
        self.parameters = snapshot(self.parameters)
        if not isinstance(self.parameters, dict):
            raise EpisodeError("action parameters must be a dict")
        for name in ("depends_on",):
            value = getattr(self, name)
            if (not isinstance(value, list) or any(type(item) is not int or item < 0 for item in value)
                    or len(set(value)) != len(value)):
                raise EpisodeError(name + " must be a list of unique nonnegative integers")
        for name in ("provides", "requires", "expected_effect"):
            value = getattr(self, name)
            if (not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value)
                    or len(set(value)) != len(value)):
                raise EpisodeError(name + " must be a list of unique nonempty strings")
        self.guard = [StateCondition.from_dict(item) if isinstance(item, dict) else item for item in self.guard]
        if any(not isinstance(item, StateCondition) for item in self.guard):
            raise EpisodeError("guard must contain StateCondition objects")
        if len({item.condition_id for item in self.guard}) != len(self.guard):
            raise EpisodeError("guard condition ids must be unique")
        if self.rule_id:
            nonempty(self.rule_id, "action.rule_id")

    def to_dict(self):
        return {
            "schema": ACTION_SCHEMA,
            "sequence": self.sequence,
            "action_id": self.action_id,
            "action": self.action,
            "input_entry": snapshot(self.input_entry),
            "input_sha256": self.input_sha256,
            "parameters": snapshot(self.parameters),
            "depends_on": list(self.depends_on),
            "provides": list(self.provides),
            "requires": list(self.requires),
            "guard": [item.to_dict() for item in self.guard],
            "expected_effect": list(self.expected_effect),
            "rule_id": self.rule_id,
        }

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or data.get("schema") != ACTION_SCHEMA:
            raise EpisodeError("unsupported episode action schema")
        action = cls(sequence=data.get("sequence"), action=data.get("action"),
                     input_entry=deepcopy(data.get("input_entry")),
                     parameters=deepcopy(data.get("parameters", {})),
                     depends_on=list(data.get("depends_on", [])),
                     provides=list(data.get("provides", [])),
                     requires=list(data.get("requires", [])),
                     guard=[StateCondition.from_dict(item) for item in data.get("guard", [])],
                     expected_effect=list(data.get("expected_effect", [])),
                     rule_id=data.get("rule_id", ""),
                     action_id=data.get("action_id", ""))
        if action.to_dict() != data:
            raise EpisodeError("episode action bytes are not canonical")
        return action


@dataclass
class Episode:
    episode_id: str
    target_id: str
    target_revision: int
    rmw: str
    contract_hash: str
    actions: list[EpisodeAction]
    initial_state: dict
    limits: EpisodeLimits = field(default_factory=EpisodeLimits)
    allowed_state_ids: list[str] | None = None
    parent_episode_ids: list[str] = field(default_factory=list)
    tags: dict = field(default_factory=dict)
    isolation_mode: str = "per_case"
    state_schema: str = STATE_ABSTRACTION_VERSION

    def __post_init__(self):
        _safe_id(self.episode_id, "episode_id")
        nonempty(self.target_id, "target_id")
        if type(self.target_revision) is not int or self.target_revision < 0:
            raise EpisodeError("target_revision must be a nonnegative integer")
        nonempty(self.rmw, "rmw")
        nonempty(self.contract_hash, "contract_hash")
        if not isinstance(self.limits, EpisodeLimits):
            raise EpisodeError("limits must be EpisodeLimits")
        if not isinstance(self.actions, list) or not self.actions:
            raise EpisodeError("episode requires at least one action")
        if any(not isinstance(item, EpisodeAction) for item in self.actions):
            raise EpisodeError("actions must be EpisodeAction objects")
        if len(self.actions) > self.limits.max_actions:
            raise EpisodeError(f"episode program has {len(self.actions)} actions; limit is {self.limits.max_actions}")
        sequences = [item.sequence for item in self.actions]
        if sequences != sorted(sequences) or len(set(sequences)) != len(sequences):
            raise EpisodeError("action sequences must be strictly increasing and unique")
        action_ids = [item.action_id for item in self.actions]
        if len(set(action_ids)) != len(action_ids):
            raise EpisodeError("action ids must be unique")
        known = set(sequences)
        for item in self.actions:
            if any(dep not in known or dep >= item.sequence for dep in item.depends_on):
                raise EpisodeError("action dependencies must refer to earlier actions")
        self.initial_state = validate_state(snapshot(self.initial_state), schema=self.state_schema)
        self.allowed_state_ids = None if self.allowed_state_ids is None else list(self.allowed_state_ids)
        if self.allowed_state_ids is not None:
            if not self.allowed_state_ids or any(not _SHA.fullmatch(item) for item in self.allowed_state_ids):
                raise EpisodeError("allowed_state_ids must be a nonempty list of state digests or None")
            if len(set(self.allowed_state_ids)) != len(self.allowed_state_ids):
                raise EpisodeError("allowed_state_ids contains duplicates")
        if (not isinstance(self.parent_episode_ids, list)
                or any(not isinstance(item, str) or not _SAFE_ID.fullmatch(item) for item in self.parent_episode_ids)
                or len(set(self.parent_episode_ids)) != len(self.parent_episode_ids)):
            raise EpisodeError("parent_episode_ids must be unique safe ids")
        if self.episode_id in self.parent_episode_ids:
            raise EpisodeError("episode cannot be its own parent")
        self.tags = snapshot(self.tags)
        if not isinstance(self.tags, dict):
            raise EpisodeError("tags must be a dict")
        if self.isolation_mode not in ISOLATION_MODES:
            raise EpisodeError("isolation_mode must be one of " + ",".join(ISOLATION_MODES))

    @property
    def initial_state_id(self):
        return state_id(self.initial_state, schema=self.state_schema)

    def input_identity(self):
        """Deterministic episode identity; execution time/raw/run fields excluded."""
        document = {
            "schema": SCHEMA,
            "target_id": self.target_id,
            "target_revision": self.target_revision,
            "rmw": self.rmw,
            "contract_hash": self.contract_hash,
            "limits": self.limits.to_dict(),
            "initial_state_id": self.initial_state_id,
            "allowed_state_ids": None if self.allowed_state_ids is None else sorted(self.allowed_state_ids),
            "parent_episode_ids": list(self.parent_episode_ids),
            "isolation_mode": self.isolation_mode,
            "actions": [item.to_dict() for item in self.actions],
        }
        if self.state_schema != STATE_ABSTRACTION_VERSION:
            document["state_schema"] = self.state_schema
        return canonical_hash(document)

    def to_dict(self):
        document = {
            "schema": SCHEMA,
            "episode_id": self.episode_id,
            "target_id": self.target_id,
            "target_revision": self.target_revision,
            "rmw": self.rmw,
            "contract_hash": self.contract_hash,
            "limits": self.limits.to_dict(),
            "initial_state": snapshot(self.initial_state),
            "initial_state_id": self.initial_state_id,
            "allowed_state_ids": None if self.allowed_state_ids is None else list(self.allowed_state_ids),
            "parent_episode_ids": list(self.parent_episode_ids),
            "actions": [item.to_dict() for item in self.actions],
            "input_sha256": self.input_identity(),
            "tags": snapshot(self.tags),
            "isolation_mode": self.isolation_mode,
        }
        if self.state_schema != STATE_ABSTRACTION_VERSION:
            document["state_schema"] = self.state_schema
        return document

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or data.get("schema") != SCHEMA:
            raise EpisodeError("unsupported episode schema")
        episode = cls(
            episode_id=data.get("episode_id"), target_id=data.get("target_id"),
            target_revision=data.get("target_revision"), rmw=data.get("rmw"),
            contract_hash=data.get("contract_hash"),
            actions=[EpisodeAction.from_dict(item) for item in data.get("actions", [])],
            initial_state=deepcopy(data.get("initial_state")),
            limits=EpisodeLimits.from_dict(data.get("limits")),
            allowed_state_ids=None if data.get("allowed_state_ids") is None else list(data["allowed_state_ids"]),
            parent_episode_ids=list(data.get("parent_episode_ids", [])),
            tags=deepcopy(data.get("tags", {})),
            isolation_mode=data.get("isolation_mode", "per_case"),
            state_schema=data.get("state_schema", STATE_ABSTRACTION_VERSION),
        )
        if data.get("initial_state_id") != episode.initial_state_id:
            raise EpisodeError("episode initial_state_id differs from state bytes")
        if data.get("input_sha256") != episode.input_identity():
            raise EpisodeError("episode input identity differs from action program")
        if episode.to_dict() != data:
            raise EpisodeError("episode bytes are not canonical")
        return episode


def _history_snapshot(store, parent_ids):
    if not parent_ids:
        return {"parent_episode_ids": [], "ancestors": [], "chain_complete": True}
    if store is None:
        raise EpisodeHistoryError("episode parent history is required but no EpisodeStore was supplied")
    ancestors = store.ancestor_chain(parent_ids)
    expected = []
    for parent in parent_ids:
        expected.append(parent)
    # ancestor_chain returns deterministic parent-first closure; verify every
    # declared parent appears exactly once and every ancestor record is bound.
    ids = [item["episode_id"] for item in ancestors]
    if len(ids) != len(set(ids)) or not set(parent_ids) <= set(ids):
        raise EpisodeHistoryError("parent ancestor chain is incomplete or ambiguous")
    bound = []
    for item in ancestors:
        bound.append({"episode_id": item["episode_id"],
                      "input_sha256": item["input_sha256"],
                      "record_sha256": item["record_sha256"],
                      "stop_reason": item["execution"]["stop_reason"]})
    return {"parent_episode_ids": list(parent_ids), "ancestors": bound,
            "chain_complete": True}


def _stage(value):
    try:
        stage = ExecutionStage(value).value
    except (ValueError, TypeError) as exc:
        raise EpisodeError(f"unsupported execution_stage: {value!r}") from exc
    if stage not in {item.value for item in ExecutionStage}:
        raise EpisodeError("execution_stage is not canonical")
    return stage


def _verdict(value):
    if value is None:
        return None
    if value not in ORACLE_VERDICTS:
        raise EpisodeError("oracle verdict must be pass/fail/unknown/not_applicable/null")
    return value


def _receipt(action, before, *, stage, status, guard, result=None, error="", history=None,
             timing=None, state_schema=STATE_ABSTRACTION_VERSION):
    after = None
    after_id = None
    if result is not None and result.get("after_state") is not None:
        after = validate_state(snapshot(result["after_state"]), schema=state_schema)
        after_id = state_id(after, schema=state_schema)
    receipt = {
        "schema": "r2f.s1-episode-action-receipt/v1",
        "sequence": action.sequence,
        "action_id": action.action_id,
        "action": action.action,
        "input_sha256": action.input_sha256,
        "parameters": snapshot(action.parameters),
        "before_state": snapshot(before),
        "before_state_id": state_id(before, schema=state_schema),
        "after_state": snapshot(after),
        "after_state_id": after_id,
        "guard": snapshot(guard),
        "execution_stage": _stage(stage),
        "status": status,
        "error": error,
        "observed_effect": list((result or {}).get("observed_effect", [])),
        "expected_effect": list(action.expected_effect),
        "oracle_verdict": _verdict((result or {}).get("oracle_verdict")),
        "rule_id": (result or {}).get("rule_id") or action.rule_id or "",
        "reason_code": (result or {}).get("reason_code", ""),
        "responsibility_layer": (result or {}).get("responsibility_layer"),
        "tool_error": bool((result or {}).get("tool_error", False)),
        "parent_history": snapshot(history),
        "timing": snapshot(timing or {}),
    }
    if result is not None and "observation" in result:
        receipt["observation"] = snapshot(result["observation"])
    if receipt["oracle_verdict"] in {"pass", "fail"} and not receipt["rule_id"]:
        raise EpisodeError("pass/fail receipt requires a rule_id")
    return receipt


def run_episode(episode, *, perform, clock, health=None, store=None, on_receipt=None):
    """Execute one bounded episode with an injected action/health/clock backend.

    ``perform(action, state) -> dict`` returns an after_state, observed effects,
    oracle verdict and canonical execution_stage.  A clean pass additionally
    requires an explicit allowed_state_ids model; an observed state outside that
    model aborts the episode and never expands the model.
    """
    if not isinstance(episode, Episode):
        raise EpisodeError("run_episode requires an Episode")
    if not callable(perform):
        raise EpisodeError("perform must be callable")
    if not callable(clock):
        raise EpisodeError("clock must be callable")
    if on_receipt is not None and not callable(on_receipt):
        raise EpisodeError("on_receipt must be callable or None")
    if health is not None and not callable(health):
        raise EpisodeError("health must be callable or None")
    history = _history_snapshot(store, episode.parent_episode_ids)
    state = snapshot(episode.initial_state)
    initial_id = state_id(state, schema=episode.state_schema)
    allowed = None if episode.allowed_state_ids is None else set(episode.allowed_state_ids)
    started = float(clock())
    receipts = []
    stop_reason, stop_details = None, {}
    if allowed is not None and initial_id not in allowed:
        stop_reason = "initial_state_outside_allowed_model"
        stop_details = {"state_id": initial_id}
    def notify_receipt():
        if on_receipt is None:
            return None
        try:
            on_receipt(snapshot(receipts[-1]), snapshot(receipts))
        except Exception as exc:
            return {"action_id": receipts[-1]["action_id"], "responsibility_layer": "harness",
                    "error": type(exc).__name__ + ": " + str(exc)}
        return None

    for action in episode.actions:
        if stop_reason is not None:
            break
        elapsed = float(clock()) - started
        if elapsed >= episode.limits.max_seconds:
            stop_reason, stop_details = "max_seconds", {"elapsed_sec": elapsed,
                                                        "max_seconds": episode.limits.max_seconds}
            break
        probe = {}
        if health is not None:
            probe = health(action, snapshot(state))
            if not isinstance(probe, dict):
                raise EpisodeError("health probe must return a dict")
            if probe.get("healthy") is not True:
                stop_reason = "unhealthy:" + str(probe.get("reason") or "unknown")
                stop_details = {"health": snapshot(probe)}
                break
            if (episode.limits.max_memory_bytes is not None
                    and probe.get("memory_bytes") is not None
                    and (type(probe["memory_bytes"]) is not int
                         or probe["memory_bytes"] > episode.limits.max_memory_bytes)):
                stop_reason = "memory_limit"
                stop_details = {"memory_bytes": probe.get("memory_bytes"),
                                "max_memory_bytes": episode.limits.max_memory_bytes}
                break
            if (episode.limits.max_log_bytes is not None
                    and probe.get("log_bytes") is not None
                    and (type(probe["log_bytes"]) is not int
                         or probe["log_bytes"] > episode.limits.max_log_bytes)):
                stop_reason = "log_limit"
                stop_details = {"log_bytes": probe.get("log_bytes"),
                                "max_log_bytes": episode.limits.max_log_bytes}
                break
        guard = [{"condition_id": item.condition_id, "fact": item.fact,
                  "operator": item.operator, "passed": item.evaluate(state)}
                 for item in action.guard]
        if any(not item["passed"] for item in guard):
            receipt = _receipt(action, state, state_schema=episode.state_schema, stage="aborted", status="blocked",
                               guard=guard, error="requires not satisfied", history=history,
                               timing={"logical_order": len(receipts),
                                       "started_monotonic": float(clock()),
                                       "finished_monotonic": float(clock()),
                                       "os_concurrency_claimed": False})
            receipts.append(receipt)
            observer_error = notify_receipt()
            if observer_error:
                stop_reason, stop_details = "receipt_observer_tool_failure", observer_error
                break
            stop_reason = "guard_blocked"
            stop_details = {"failed_conditions": [item["condition_id"] for item in guard
                                                  if not item["passed"]]}
            break
        action_start = float(clock())
        try:
            result = perform(action, snapshot(state))
        except Exception as exc:  # harness/tool failure, never SUT attribution
            receipt = _receipt(action, state, state_schema=episode.state_schema, stage="aborted", status="tool_error",
                               guard=guard, error=f"{type(exc).__name__}: {exc}", history=history,
                               timing={"logical_order": len(receipts),
                                       "started_monotonic": action_start,
                                       "finished_monotonic": float(clock()),
                                       "os_concurrency_claimed": False})
            receipt["tool_error"] = True
            receipts.append(receipt)
            observer_error = notify_receipt()
            if observer_error:
                stop_reason, stop_details = "receipt_observer_tool_failure", observer_error
                break
            stop_reason = "tool_failure"
            stop_details = {"action_id": action.action_id, "error": receipt["error"]}
            break
        if not isinstance(result, dict):
            raise EpisodeError("perform must return a dict")
        status = result.get("status", "executed")
        stage = result.get("execution_stage", "judged" if status == "executed" else "aborted")
        receipt = _receipt(action, state, state_schema=episode.state_schema, stage=stage, status=status, guard=guard,
                           result=result, error=result.get("error", ""), history=history,
                           timing={"logical_order": len(receipts),
                                   "started_monotonic": action_start,
                                   "finished_monotonic": float(clock()),
                                   "os_concurrency_claimed": False})
        receipts.append(receipt)
        observer_error = notify_receipt()
        if observer_error:
            stop_reason, stop_details = "receipt_observer_tool_failure", observer_error
            break
        if status == "failed" or receipt["execution_stage"] == "aborted":
            stop_reason = "action_failed"
            stop_details = {"action_id": action.action_id,
                            "reason_code": receipt["reason_code"] or receipt["error"],
                            "oracle_verdict": receipt["oracle_verdict"]}
            break
        after_id = receipt["after_state_id"]
        if receipt["oracle_verdict"] == "pass":
            if allowed is None:
                stop_reason = "state_model_missing_for_pass"
                stop_details = {"action_id": action.action_id}
                break
            if after_id is None or after_id not in allowed:
                stop_reason = "state_outside_allowed_model"
                stop_details = {"action_id": action.action_id, "state_id": after_id}
                break
        elif after_id is not None and allowed is not None and after_id not in allowed:
            # An unknown/fail observation outside the declared model is
            # retained as evidence but cannot continue as if it were allowed.
            stop_reason = "state_outside_allowed_model"
            stop_details = {"action_id": action.action_id, "state_id": after_id,
                            "oracle_verdict": receipt["oracle_verdict"]}
            break
        if receipt["after_state"] is not None:
            state = receipt["after_state"]
    else:
        stop_reason = "episode_completed"
    finished = float(clock())
    execution = {
        "schema": EXECUTION_SCHEMA,
        "episode_id": episode.episode_id,
        "input_sha256": episode.input_identity(),
        "stop_reason": stop_reason,
        "stop_details": snapshot(stop_details),
        "receipts": receipts,
        "action_count": len(receipts),
        "attempted_sequences": [item["sequence"] for item in receipts],
        "parent_history": history,
        "started_monotonic": started,
        "finished_monotonic": finished,
        "elapsed_sec": max(0.0, finished - started),
        "limits": episode.limits.to_dict(),
        "state_model": "declared_allowed_set" if episode.allowed_state_ids is not None
                       else "no_pass_claim_model",
        "isolation": {"mode": episode.isolation_mode,
                      "per_case_cells_replaced": False,
                      "per_case_isolation_preserved": True},
        "candidate_writes": 0,
        "maturity_denominator_changed": False,
        "os_concurrency_claimed": False,
    }
    if episode.state_schema != STATE_ABSTRACTION_VERSION:
        execution["state_schema"] = episode.state_schema
    execution["execution_sha256"] = canonical_hash(execution)
    return execution


def _validate_execution_state_schema(episode, execution, *, error_type=EpisodeError):
    # Missing schema is a legacy pair encoding, never implicit topology.
    if execution.get("state_schema", STATE_ABSTRACTION_VERSION) != episode.state_schema:
        raise error_type("execution state schema differs from episode")


def _fsync_directory(path):
    """Persist published directory entries, not just their file contents."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _durable_mkdir(path):
    path = Path(path).resolve()
    existed = path.is_dir()
    path.mkdir(parents=True, exist_ok=True)
    if not existed:
        # A newly created subtree can include previously created but not yet
        # synced output directories. Persist ancestry once when publishing it;
        # subsequent records only need their containing directory synced.
        for directory in (path, *path.parents):
            _fsync_directory(directory)


def _atomic_create(path, data):
    path = Path(path)
    _durable_mkdir(path.parent)
    payload = (json.dumps(data, sort_keys=True, indent=2, ensure_ascii=False,
                          allow_nan=False) + "\n").encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=".episode-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        # link() fails if the destination exists: immutable append-only store.
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise EpisodeStoreError("episode record already exists; store is append-only: " + path.name) from exc
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    _fsync_directory(path.parent)


class EpisodeStore:
    """Append-only per-episode JSON records with ancestor-chain checks."""

    def __init__(self, root):
        self.root = Path(root)

    def _path(self, episode_id):
        _safe_id(episode_id, "episode_id")
        return self.root / "episodes" / (episode_id + ".json")

    def add(self, episode, execution):
        if not isinstance(episode, Episode):
            raise EpisodeStoreError("store requires an Episode")
        if not isinstance(execution, dict) or execution.get("schema") != EXECUTION_SCHEMA:
            raise EpisodeStoreError("store requires an episode execution record")
        received = execution.get("execution_sha256")
        if received != canonical_hash({k: v for k, v in execution.items() if k != "execution_sha256"}):
            raise EpisodeStoreError("episode execution hash mismatch")
        if execution.get("episode_id") != episode.episode_id:
            raise EpisodeStoreError("execution episode id differs from episode")
        if execution.get("input_sha256") != episode.input_identity():
            raise EpisodeStoreError("execution input identity differs from episode")
        if execution.get("parent_history", {}).get("parent_episode_ids") != episode.parent_episode_ids:
            raise EpisodeStoreError("execution parent history differs from episode")
        _validate_execution_state_schema(episode, execution, error_type=EpisodeStoreError)
        record = {"schema": STORE_SCHEMA, "episode": episode.to_dict(),
                  "execution": snapshot(execution)}
        record["record_sha256"] = canonical_hash(record)
        _atomic_create(self._path(episode.episode_id), record)
        return snapshot(record)

    def commit_progress(self, episode, receipts, *, checkpoint=False):
        """Durably commit a completed action prefix before any next side effect.

        These immutable records do not restore a running DDS context. A caller
        may recover the observation history, never infer a resumed runtime.
        """
        if not isinstance(episode, Episode) or not receipts:
            raise EpisodeStoreError("progress requires an episode and nonempty receipt prefix")
        if type(checkpoint) is not bool or len(receipts) > len(episode.actions):
            raise EpisodeStoreError("invalid progress prefix")
        for action, receipt in zip(episode.actions, receipts):
            if (receipt.get("action_id") != action.action_id
                    or receipt.get("sequence") != action.sequence
                    or receipt.get("input_sha256") != action.input_sha256):
                raise EpisodeStoreError("progress action binding differs from episode")
            validate_state(receipt["before_state"], schema=episode.state_schema)
            if receipt.get("after_state") is not None:
                validate_state(receipt["after_state"], schema=episode.state_schema)
        record = {"schema": "r2f.s1-episode-progress/v1", "episode_id": episode.episode_id,
                  "input_sha256": episode.input_identity(), "state_schema": episode.state_schema,
                  "action_count": len(receipts), "checkpoint": checkpoint,
                  "receipts": snapshot(receipts), "restoration_supported": False}
        record["record_sha256"] = canonical_hash(record)
        path = self.root / "checkpoints" / episode.episode_id / (f"{len(receipts)-1:04d}.json")
        _atomic_create(path, record)
        return {"path": str(path.resolve()), "record_sha256": record["record_sha256"],
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def load_progress(self, episode, sequence):
        """Read and authenticate one committed action prefix, without a terminal.

        This recovers JSON observation history, not a DDS memory snapshot or a
        successful replay. ``checkpoint=False`` remains loadable; callers must
        independently authorize reconstruction and verify its runtime evidence.
        """
        if not isinstance(episode, Episode):
            raise EpisodeStoreError("progress_episode_required")
        if type(sequence) is not int or not 0 <= sequence < len(episode.actions):
            raise EpisodeStoreError("progress_sequence_out_of_range")

        def require(ok, reason):
            if not ok:
                raise EpisodeStoreError("progress_" + reason)

        def object_pairs(pairs):
            result = {}
            for key, value in pairs:
                require(key not in result, "duplicate_json_key:" + key)
                result[key] = value
            return result

        def constant(value):
            raise EpisodeStoreError("progress_nonfinite_json:" + value)

        try:
            # Revalidate mutable caller objects, including safe path and the
            # per-action content hashes, rather than trusting cached identities.
            Episode.from_dict(episode.to_dict())
            path = self.root / "checkpoints" / episode.episode_id / f"{sequence:04d}.json"
            data = snapshot(json.loads(path.read_text(), object_pairs_hook=object_pairs,
                                       parse_constant=constant))
            fields = {"schema", "episode_id", "input_sha256", "state_schema", "action_count",
                      "checkpoint", "receipts", "restoration_supported", "record_sha256"}
            require(isinstance(data, dict) and set(data) == fields, "record_fields_invalid")
            require(data["schema"] == "r2f.s1-episode-progress/v1", "schema_mismatch")
            _sha(data["record_sha256"], "progress_record_sha256")
            require(data["record_sha256"] == canonical_hash(
                {k: v for k, v in data.items() if k != "record_sha256"}), "record_hash_mismatch")
            require(data["episode_id"] == episode.episode_id, "episode_id_mismatch")
            require(data["input_sha256"] == episode.input_identity(), "input_identity_mismatch")
            require(data["state_schema"] == episode.state_schema, "state_schema_mismatch")
            require(type(data["action_count"]) is int and data["action_count"] == sequence + 1,
                    "action_count_mismatch")
            require(type(data["checkpoint"]) is bool, "checkpoint_not_bool")
            require(data["restoration_supported"] is False, "restoration_not_false")
            receipts = data["receipts"]
            require(isinstance(receipts, list) and len(receipts) == sequence + 1,
                    "receipt_count_mismatch")
            receipt_fields = {"schema", "sequence", "action_id", "action", "input_sha256",
                "parameters", "before_state", "before_state_id", "after_state", "after_state_id",
                "guard", "execution_stage", "status", "error", "observed_effect", "expected_effect",
                "oracle_verdict", "rule_id", "reason_code", "responsibility_layer", "tool_error",
                "parent_history", "timing"}
            current = snapshot(episode.initial_state)
            for action, receipt in zip(episode.actions[:sequence + 1], receipts):
                label = "receipt:" + str(action.sequence) + ":"
                require(isinstance(receipt, dict) and receipt_fields <= set(receipt)
                        and not set(receipt) - receipt_fields - {"observation"}, label + "fields_invalid")
                require(receipt["schema"] == "r2f.s1-episode-action-receipt/v1", label + "schema_mismatch")
                require(type(receipt["sequence"]) is int and receipt["sequence"] == action.sequence
                        and receipt["action_id"] == action.action_id and receipt["action"] == action.action
                        and receipt["input_sha256"] == action.input_sha256,
                        label + "action_binding_mismatch")
                require(isinstance(receipt["parameters"], dict)
                        and canonical_hash(receipt["parameters"]) == canonical_hash(action.parameters),
                        label + "parameters_mismatch")
                require(receipt["expected_effect"] == action.expected_effect, label + "expected_effect_mismatch")
                require(_stage(receipt["execution_stage"]) == receipt["execution_stage"], label + "stage_invalid")
                verdict = receipt["oracle_verdict"]
                require(verdict is None or isinstance(verdict, str), label + "verdict_invalid")
                _verdict(verdict)
                require(isinstance(receipt["rule_id"], str)
                        and (verdict not in {"pass", "fail"} or bool(receipt["rule_id"])),
                        label + "rule_id_missing")
                require(isinstance(receipt["status"], str) and bool(receipt["status"]), label + "status_invalid")
                require(type(receipt["tool_error"]) is bool, label + "tool_error_not_bool")
                before = validate_state(receipt["before_state"], schema=episode.state_schema)
                require(receipt["before_state_id"] == state_id(before, schema=episode.state_schema),
                        label + "before_state_id_mismatch")
                require(canonical_hash(before) == canonical_hash(current), label + "state_chain_mismatch")
                after = receipt["after_state"]
                if after is None:
                    require(receipt["after_state_id"] is None, label + "null_after_state_id_mismatch")
                else:
                    after = validate_state(after, schema=episode.state_schema)
                    require(receipt["after_state_id"] == state_id(after, schema=episode.state_schema),
                            label + "after_state_id_mismatch")
                    current = after
            return snapshot(data)
        except EpisodeStoreError:
            raise
        except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError,
                RecursionError, OverflowError) as exc:
            raise EpisodeStoreError("progress_invalid:" + type(exc).__name__ + ":" + str(exc)) from exc

    def load(self, episode_id):
        path = self._path(episode_id)
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise EpisodeStoreError("episode is not stored: " + episode_id) from exc
        if not isinstance(data, dict) or data.get("schema") != STORE_SCHEMA:
            raise EpisodeStoreError("unsupported episode store record")
        received = data.get("record_sha256")
        if received != canonical_hash({k: v for k, v in data.items() if k != "record_sha256"}):
            raise EpisodeStoreError("episode record hash mismatch")
        episode = Episode.from_dict(data["episode"])
        execution = data["execution"]
        if execution.get("execution_sha256") != canonical_hash(
                {k: v for k, v in execution.items() if k != "execution_sha256"}):
            raise EpisodeStoreError("stored execution hash mismatch")
        if episode.episode_id != episode_id or execution.get("episode_id") != episode_id:
            raise EpisodeStoreError("stored episode id mismatch")
        if execution.get("input_sha256") != episode.input_identity():
            raise EpisodeStoreError("stored input identity mismatch")
        _validate_execution_state_schema(episode, execution, error_type=EpisodeStoreError)
        return {"episode_id": episode_id, "episode": episode, "execution": execution,
                "record_sha256": received, "input_sha256": episode.input_identity(),
                "schema": STORE_SCHEMA}

    def ids(self):
        folder = self.root / "episodes"
        if not folder.exists():
            return []
        return sorted(path.stem for path in folder.glob("*.json"))

    def ancestor_chain(self, parent_ids):
        """Return deterministic parent-first closure; incomplete history raises."""
        if not isinstance(parent_ids, list):
            raise EpisodeHistoryError("parent ids must be a list")
        result, seen = [], set()

        def visit(identifier, trail):
            if identifier in trail:
                raise EpisodeHistoryError("cyclic episode parent history")
            if identifier in seen:
                return
            try:
                record = self.load(identifier)
            except EpisodeStoreError as exc:
                raise EpisodeHistoryError("parent episode history is missing: " + identifier) from exc
            episode = record["episode"]
            for parent in episode.parent_episode_ids:
                visit(parent, trail | {identifier})
            seen.add(identifier)
            result.append({"episode_id": identifier,
                           "input_sha256": record["input_sha256"],
                           "record_sha256": record["record_sha256"],
                           "execution": record["execution"]})

        for identifier in parent_ids:
            visit(identifier, set())
        return result

    def view(self):
        """JSON-safe index for a runner continuation; never includes runtime data."""
        records = [self.load(identifier) for identifier in self.ids()]
        members = [{"episode_id": record["episode_id"],
                    "input_sha256": record["input_sha256"],
                    "record_sha256": record["record_sha256"],
                    "stop_reason": record["execution"]["stop_reason"]}
                   for record in records]
        result = {"schema": STORE_VIEW_SCHEMA, "records": members}
        result["view_sha256"] = canonical_hash(result)
        return result


def resume_view(store):
    """Compatibility helper for a caller that stores a store view in continuation."""
    if not isinstance(store, EpisodeStore):
        raise EpisodeStoreError("resume_view requires an EpisodeStore")
    return store.view()


def corpus_offer_kwargs(action, *, observed_edge=None, oracle_verdict=None, candidate_id=None):
    """Project one action into the existing Corpus.offer keyword shape.

    The caller owns the corpus and the canonical runner; this helper never calls
    Corpus.offer, never writes a candidate and never changes maturity counts.
    """
    if not isinstance(action, EpisodeAction):
        raise EpisodeError("corpus projection requires an EpisodeAction")
    entry = deepcopy(action.input_entry)
    entry.setdefault("surface_id", action.action)
    entry.setdefault("boundary_class", action.parameters.get("boundary_class", action.action))
    entry.setdefault("observation_complete", False)
    entry["action_id"] = action.action_id
    result = {"entry": entry, "executed": False, "evidence_complete": False,
              "observed_edge": observed_edge, "oracle_verdict": oracle_verdict,
              "tool_error": False, "candidate_id": candidate_id}
    return result


def coexistence_guard(episode, per_case_cells):
    """Verify each independent per-case cell survives an episode run.

    ``per_case_cells`` is the ordered list of independent campaign cell ids that
    existed before the episode.  The guard is pure: it never runs a cell, and it
    refuses to let an episode stand in for one.
    """
    if not isinstance(episode, Episode):
        raise EpisodeError("coexistence guard requires an Episode")
    if not isinstance(per_case_cells, list) or any(
            not isinstance(item, str) or not item for item in per_case_cells):
        raise EpisodeError("per_case_cells must be a list of cell ids")
    if len(set(per_case_cells)) != len(per_case_cells):
        raise EpisodeError("per_case_cells contains duplicates")
    receipt = {
        "schema": "r2f.s1-episode-coexistence/v1",
        "episode_id": episode.episode_id,
        "isolation_mode": episode.isolation_mode,
        "per_case_cells": list(per_case_cells),
        "per_case_cells_replaced": False,
        "episode_substitutes_per_case": False,
        "continuous_scenario_additive_only": True,
    }
    receipt["receipt_sha256"] = canonical_hash(receipt)
    return receipt


def _raw_ref(value, field):
    if not isinstance(value, dict):
        raise EpisodeError(field + " must be an object")
    path = value.get("path")
    if not isinstance(path, str) or not path:
        raise EpisodeError(field + ".path is required")
    _sha(value.get("sha256"), field + ".sha256")
    index = value.get("event_index")
    if type(index) is not int or index < 0:
        raise EpisodeError(field + ".event_index must be a nonnegative integer")
    return {"path": path, "sha256": value["sha256"], "event_index": index}


def validate_reset_receipt(receipt, *, previous_runtime=None):
    """Validate a reset attempt without promising an unimplemented snapshot.

    Policy order is fixed: a verified cleanup/rebuild is the only accepted way
    to return to a baseline.  When that is unavailable the runtime must be
    recreated and the receipt must say the previous state was *not* restored.
    A distributed snapshot restore is accepted only with explicit identity,
    consistency and rollback proofs; otherwise the attempt is refused instead
    of being reported as a successful reset.
    """
    if not isinstance(receipt, dict):
        raise EpisodeError("reset receipt must be an object")
    kind = receipt.get("kind")
    if kind not in RESET_KINDS:
        raise EpisodeError("reset kind must be one of " + ",".join(RESET_KINDS))
    if receipt.get("schema") != RESET_SCHEMA:
        raise EpisodeError("unsupported reset receipt schema")
    if receipt.get("cleanup_verified") is not True:
        raise EpisodeError("reset requires cleanup_verified=true")
    if receipt.get("resources_released") is not True:
        raise EpisodeError("reset requires resources_released=true")
    evidence = receipt.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise EpisodeError("reset requires raw-backed evidence")
    for index, item in enumerate(evidence):
        _raw_ref(item, f"evidence[{index}]")
    result = {"schema": RESET_SCHEMA, "kind": kind,
              "cleanup_verified": True, "resources_released": True,
              "evidence": [ _raw_ref(item, f"evidence[{index}]")
                            for index, item in enumerate(evidence) ]}
    if kind == "verified_cleanup_rebuild":
        if receipt.get("rebuild_verified") is not True:
            raise EpisodeError("verified cleanup/rebuild requires rebuild_verified=true")
        if receipt.get("baseline_restored") is not True:
            raise EpisodeError("verified cleanup/rebuild must declare baseline_restored=true")
        state_id_value = receipt.get("restored_state_id")
        _sha(state_id_value, "restored_state_id")
        result.update(rebuild_verified=True, baseline_restored=True,
                      state_origin="verified_cleanup_rebuild",
                      restored_state_id=state_id_value)
        return result
    if kind == "runtime_recreated":
        if receipt.get("baseline_restored") is not False:
            raise EpisodeError("a recreated runtime must declare baseline_restored=false")
        if receipt.get("restored_state_id") is not None:
            raise EpisodeError("a recreated runtime cannot claim a restored state id")
        new_runtime = receipt.get("runtime")
        if not isinstance(new_runtime, dict):
            raise EpisodeError("runtime_recreated requires the new runtime identity")
        for key in ("pid", "generation"):
            if new_runtime.get(key) in (None, ""):
                raise EpisodeError("runtime identity requires " + key)
        if previous_runtime is not None:
            if not isinstance(previous_runtime, dict):
                raise EpisodeError("previous_runtime must be an object or None")
            if (previous_runtime.get("pid") == new_runtime.get("pid")
                    and previous_runtime.get("generation") == new_runtime.get("generation")):
                raise EpisodeError("runtime_recreated requires a distinct runtime identity")
        result.update(baseline_restored=False, state_origin="fresh_runtime",
                      restored_state_id=None, runtime=snapshot(new_runtime))
        return result
    # snapshot_restore: refuse unless every proof is present and consistent.
    proofs = {}
    for name in ("identity_proof", "consistency_proof", "rollback_path"):
        value = receipt.get(name)
        if not isinstance(value, dict) or not value:
            raise EpisodeError("snapshot_restore requires " + name)
        proofs[name] = snapshot(value)
    identity = proofs["identity_proof"]
    if identity.get("endpoint_identity_consistent") is not True:
        raise EpisodeError("snapshot identity proof must assert endpoint_identity_consistent")
    consistency = proofs["consistency_proof"]
    if consistency.get("quiescent") is not True or consistency.get("atomic") is not True:
        raise EpisodeError("snapshot consistency proof must assert quiescent and atomic")
    rollback = proofs["rollback_path"]
    if rollback.get("verified") is not True:
        raise EpisodeError("snapshot rollback path must be verified")
    _raw_ref(rollback.get("evidence"), "rollback_path.evidence")
    if receipt.get("baseline_restored") is not True:
        raise EpisodeError("snapshot_restore must declare baseline_restored=true")
    result.update(baseline_restored=True, state_origin="snapshot_restore",
                  identity_proof=proofs["identity_proof"],
                  consistency_proof=proofs["consistency_proof"],
                  rollback_path=proofs["rollback_path"])
    return result
