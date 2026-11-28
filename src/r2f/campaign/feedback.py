"""Deterministic exploratory feedback for coverage and transition rarity.

Feedback is an append-only observation log.  It never executes inputs, never
creates candidates and never awards coverage for plans or tool failures.
Replaying the same ordered log with the same RNG seed reproduces selection.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, field
import re

from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.core.observation_status import observation_complete, tool_error
from r2f.core.state_signature import observation_edge

SCHEMA = "r2f.s1-feedback/v1"


def surface_class(record):
    """Coverage key: surface × boundary × variant.

    A seed variant is a distinct declared input class; folding all variants of
    one boundary into a single class made the uncovered pool collapse after one
    observation and stopped further exploration.
    """
    surface = record.get("surface_id") or record.get("boundary_class")
    boundary = record.get("boundary_class") or record.get("boundary")
    variant = record.get("variant_id")
    if not isinstance(surface, str) or not surface or not isinstance(boundary, str) or not boundary:
        return None
    if isinstance(variant, str) and variant:
        return (surface, boundary, variant)
    return (surface, boundary)


def edge_id(record):
    observed = observation_edge(record)
    return observed[1] if observed else None


def feedback_identity(events):
    """Bind ordered scheduler inputs separately from the full event hashes.

    Events already store the canonical coverage class. Re-parsing them as raw
    observations discards that class because surface_id/boundary_class are not
    top-level event fields. Full event integrity is checked on restore.
    """
    members = [{"content_sha256": e.get("content_sha256"),
                "surface_class": e.get("surface_class"),
                "edge_id": e.get("edge_id"),
                "complete": bool(e.get("coverage")),
                "tool_error": bool(e.get("tool_error"))} for e in events]
    # Optional scheduling metadata is bound without changing historical v1 bytes.
    for member, event in zip(members, events):
        attempt = event.get("recipe_attempt")
        if attempt is not None:
            member["recipe_attempt"] = {key: attempt[key] for key in
                ("recipe_sha256", "attempt_kind", "submitted", "outcome")}
        release = event.get("recipe_policy_release")
        if release is not None:
            member["recipe_policy_release"] = {"recipe_sha256": release["recipe_sha256"]}
        # Full event hashes bind diagnostic paths/attempt ids separately. They
        # must not perturb generation RNG across equivalent split/resumed runs.
    return canonical_hash({"schema": SCHEMA, "ordered_events": members})


@dataclass
class FeedbackLog:
    events: list = field(default_factory=list)

    def record(self, record):
        """Record one attempted input; incomplete/tool-error rows give no coverage."""
        content = record.get("content_sha256")
        if not isinstance(content, str) or len(content) != 64:
            raise ValueError("feedback requires a 64-hex content_sha256")
        complete = observation_complete(record) and not tool_error(record)
        event = {
            "content_sha256": content,
            "executed": (record.get("workspace_executed") is True or record.get("actual_run") is True),
            "coverage": bool(complete and record.get("execution_stage") == "judged"),
            "complete": bool(complete),
            "tool_error": tool_error(record),
            "surface_class": list(surface_class(record)) if surface_class(record) else None,
            "edge_id": edge_id(record) if complete else None,
            "oracle_verdict": record.get("oracle_verdict"),
        }
        if "recipe_attempt" in record:
            event["recipe_attempt"] = deepcopy(record["recipe_attempt"])
            if event["recipe_attempt"]["outcome"] == "healthy" and not event["complete"]:
                event["recipe_attempt"]["outcome"] = "incomplete"
            from .tool_failures import validate_policy_event
            validate_policy_event(event)
        if event["edge_id"] is not None:
            fields = ("execution_stage", "actual_run", "workspace_executed", "surface_id",
                      "operation_family", "parameter_class", "boundary_class", "raw_path", "raw_sha256")
            observation = {key: deepcopy(record[key]) for key in fields if key in record}
            fact_fields = ("environment_healthy", "process_healthy", "preconditions_reached",
                           "endpoint_identity_complete", "observation_complete", "identity_consistent",
                           "endpoint_identity_consistent", "window", "window_complete", "window_completion",
                           "evidence_error", "evidence_errors", "state_before", "state_after",
                           "state_projection_receipt")
            observation["typed_facts"] = {key: deepcopy(record["typed_facts"][key])
                                         for key in fact_fields if key in record["typed_facts"]}
            event["edge_provenance"] = {"schema": "r2f.s1-feedback-edge-provenance/v1",
                                        "observation": observation}
        event["event_sha256"] = canonical_hash(event)
        self.events.append(event)
        return deepcopy(event)

    def release_recipe(self, recipe, *, owner, reason, revalidation_basis):
        """Append an explicit main/owner decision; never edit original failures.

        The basis is a traceable evidence reference supplied by the named owner,
        not execution proof or a user-confirmation claim.
        """
        from .tool_failures import submission_policy, validate_policy_event, RELEASE_SCHEMA
        policy = submission_policy(self.events, recipe)
        if policy["status"] != "isolated":
            raise ValueError("release requires an attributed isolated recipe")
        event = {"content_sha256": self.events[-1]["content_sha256"],
                 "executed": False, "coverage": False, "complete": False,
                 "tool_error": False, "surface_class": None, "edge_id": None,
                 "oracle_verdict": None,
                 "recipe_policy_release": {"schema": RELEASE_SCHEMA,
                     "recipe": deepcopy(recipe), "recipe_sha256": policy["recipe_sha256"],
                     "owner": owner, "reason": reason, "revalidation_basis": deepcopy(revalidation_basis),
                     "isolating_event_sha256": policy["isolating_event_sha256"]}}
        validate_policy_event(event)
        from pathlib import Path
        basis = event["recipe_policy_release"]["revalidation_basis"]
        path = Path(basis["path"])
        try:
            valid = path.is_absolute() and path.is_file() and raw_sha256(path) == basis["sha256"]
        except OSError:
            valid = False
        if not valid:
            raise ValueError("recipe owner revalidation evidence missing or hash mismatch")
        event["event_sha256"] = canonical_hash(event)
        self.events.append(event)
        return deepcopy(event)

    def identity(self):
        return feedback_identity(self.events)

    def covered_surfaces(self):
        return {tuple(e["surface_class"]) for e in self.events if e.get("coverage") and e.get("surface_class")}

    def edge_counts(self):
        return Counter(e["edge_id"] for e in self.events if e.get("coverage") and e.get("edge_id"))

    def to_dict(self):
        return {"schema": SCHEMA, "identity": self.identity(), "events": deepcopy(self.events),
                "covered_surfaces": [list(x) for x in sorted(self.covered_surfaces())],
                "edge_counts": dict(sorted(self.edge_counts().items()))}

    @classmethod
    def from_events(cls, events):
        log = cls()
        for event in events:
            log.events.append(deepcopy(event))
        return log

    @classmethod
    def from_dict(cls, data):
        """Strict artifact restore, including edge derivation from provenance.

        Hashes detect corruption, not authenticity. This checks the canonical
        projection contract but does not open raw files or certify execution.
        The legacy ``from_events`` copy helper intentionally remains unchanged.
        """
        if not isinstance(data, dict) or data.get("schema") != SCHEMA:
            raise ValueError("unsupported feedback schema")
        events = data.get("events")
        if not isinstance(events, list):
            raise ValueError("feedback events must be a list")
        for event in events:
            if not isinstance(event, dict):
                raise ValueError("feedback event must be a dict")
            for key in ("event_sha256", "content_sha256"):
                if not isinstance(event.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", event[key]):
                    raise ValueError("invalid feedback " + key)
            if canonical_hash({k: v for k, v in event.items() if k != "event_sha256"}) != event["event_sha256"]:
                raise ValueError("feedback event hash mismatch")
            if any(type(event.get(k)) is not bool for k in ("executed", "coverage", "complete", "tool_error")):
                raise ValueError("invalid feedback flags")
            if event["coverage"] != event["complete"] or (event["complete"] and (
                    not event["executed"] or event["tool_error"])):
                raise ValueError("feedback coverage requires complete executed observation")
            surface = event.get("surface_class")
            if surface is not None and (not isinstance(surface, list) or len(surface) not in (2, 3)
                    or any(not isinstance(x, str) or not x for x in surface)):
                raise ValueError("invalid feedback surface class")
            edge = event.get("edge_id")
            if edge is not None:
                provenance = event.get("edge_provenance")
                if (not event["coverage"] or not isinstance(edge, str)
                        or not re.fullmatch(r"[0-9a-f]{64}", edge)
                        or not isinstance(provenance, dict)
                        or provenance.get("schema") != "r2f.s1-feedback-edge-provenance/v1"
                        or not isinstance(provenance.get("observation"), dict)):
                    raise ValueError("feedback edge provenance required")
                observation = provenance["observation"]
                if (edge_id(observation) != edge or surface is None
                        or tuple(surface[:2]) != surface_class(observation)):
                    raise ValueError("feedback edge provenance mismatch")
            elif event.get("edge_provenance") is not None:
                raise ValueError("edge provenance without an edge")
        from .tool_failures import validate_policy_event
        for event in events:
            validate_policy_event(event)
        from .tool_failures import validate_release_history
        validate_release_history(events)
        log = cls.from_events(events)
        if data != log.to_dict():
            raise ValueError("feedback identity or summary mismatch")
        return log

    @classmethod
    def from_partitions(cls, partitions, *, compatibility):
        """Restore compatible run-local artifacts as one canonical log.

        Each member supplies run_id, feedback.to_dict() and compatibility from
        its authoritative manifest/contract. No scheduler is changed implicitly.
        """
        from .feedback_aggregate import FeedbackAggregate
        aggregate = FeedbackAggregate(compatibility)
        for partition in partitions:
            aggregate.import_run(partition["run_id"], partition["feedback"],
                                 compatibility=partition["compatibility"])
        return aggregate.feedback_log()

    @classmethod
    def merge_partition_file(cls, path, *, compatibility, run_id, feedback):
        """Persist terminal feedback without replacing any live scheduler log."""
        from .feedback_aggregate import merge_feedback_file
        aggregate, receipt = merge_feedback_file(path, compatibility=compatibility,
                                                 run_id=run_id, feedback=feedback)
        return aggregate.feedback_log(), receipt
