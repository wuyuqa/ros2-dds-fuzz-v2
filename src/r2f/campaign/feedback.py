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

from r2f.core.evidence import canonical_hash
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
    """Feedback order is an identity input; duplicate events remain ordered."""
    members = [{"content_sha256": e.get("content_sha256"),
                "surface_class": list(surface_class(e)) if surface_class(e) else None,
                "edge_id": e.get("edge_id"),
                "complete": bool(e.get("coverage")),
                "tool_error": bool(e.get("tool_error"))} for e in events]
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
