"""Deterministic exploratory feedback for coverage and transition rarity.

Feedback is an append-only observation log.  It never executes inputs, never
creates candidates and never awards coverage for plans or tool failures.
Replaying the same ordered log with the same RNG seed reproduces selection.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass, field

from r2f.core.evidence import canonical_hash

SCHEMA = "r2f.s1-feedback/v1"


def tool_error(record):
    return bool(record.get("tool_error") or record.get("reason_code") == "tool_error"
                or record.get("runtime_status") == "tool_error"
                or record.get("oracle_tool_failure"))


def observation_complete(record):
    facts = record.get("typed_facts") or {}
    if record.get("execution_stage") != "judged":
        return False
    if facts.get("environment_healthy") is not True or facts.get("process_healthy") is not True:
        return False
    if facts.get("preconditions_reached") is not True or facts.get("endpoint_identity_complete") is not True:
        return False
    window = facts.get("window") or {}
    return window.get("complete") is True


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
    explicit = record.get("edge_id") or record.get("observed_edge")
    if isinstance(explicit, str) and explicit:
        return explicit
    facts = record.get("typed_facts") or {}
    edge = facts.get("observed_edge") or facts.get("transition_id")
    if isinstance(edge, str) and edge:
        return edge
    before, after = facts.get("state_before"), facts.get("state_after")
    if isinstance(before, str) and isinstance(after, str) and before and after:
        family = record.get("operation_family") or record.get("surface_id") or "unknown"
        parameter_class = record.get("parameter_class") or record.get("boundary_class") or "unknown"
        return canonical_hash({"before": before, "operation_family": family,
                               "parameter_class": parameter_class, "after": after})
    return None


def feedback_identity(events):
    """Feedback order is an identity input; duplicate events remain ordered."""
    members = [{"content_sha256": e.get("content_sha256"),
                "surface_class": list(surface_class(e)) if surface_class(e) else None,
                "edge_id": edge_id(e),
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
            "executed": record.get("workspace_executed", record.get("actual_run")) is True,
            "coverage": bool(complete and record.get("execution_stage") == "judged"),
            "complete": bool(complete),
            "tool_error": tool_error(record),
            "surface_class": list(surface_class(record)) if surface_class(record) else None,
            "edge_id": edge_id(record) if complete else None,
            "oracle_verdict": record.get("oracle_verdict"),
        }
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
