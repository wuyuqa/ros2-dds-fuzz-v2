"""Strict typed primary-oracle facts; absence is never a successful observation."""
from dataclasses import dataclass


class FactError(ValueError):
    pass


def integer(value, name):
    if type(value) is not int or value < 0:
        raise FactError(f"{name}: expected nonnegative integer")
    return value


def text(value, name):
    if not isinstance(value, str) or not value:
        raise FactError(f"{name}: expected nonempty identity")
    return value


def generation(value):
    if type(value) is int:
        return integer(value, "generation")
    return text(value, "generation")


def boolean(value, name):
    if type(value) is not bool:
        raise FactError(f"{name}: expected boolean")
    return value


@dataclass(frozen=True)
class Endpoint:
    id: str
    generation: int | str
    created_ns: int

    @classmethod
    def parse(cls, data):
        return cls(text(data["id"], "endpoint.id"), generation(data["generation"]), integer(data["created_ns"], "endpoint.created_ns"))


@dataclass(frozen=True)
class Delivery:
    sequence: int
    at_ns: int
    writer_id: str
    writer_generation: int | str
    reader_id: str | None = None
    reader_generation: int | str | None = None

    @classmethod
    def parse(cls, data, received=False):
        return cls(integer(data["sequence"], "sequence"), integer(data["at_ns"], "delivery.at_ns"),
                   text(data["writer_id"], "delivery.writer_id"),
                   generation(data["writer_generation" if received else "generation"]),
                   text(data["reader_id"], "delivery.reader_id") if received else None,
                   generation(data["reader_generation"]) if received else None)


@dataclass(frozen=True)
class PrimaryFacts:
    writer: Endpoint
    reader: Endpoint
    matched_writer: str
    matched_reader: str
    matched_ns: int
    window_start_ns: int
    window_end_ns: int
    window_complete: bool
    sent: tuple[Delivery, ...]
    received: tuple[Delivery, ...]

    @classmethod
    def parse(cls, data):
        writer, reader = Endpoint.parse(data["writer"]), Endpoint.parse(data["reader"])
        match, window = data["match"], data["window"]
        if not isinstance(match, dict):
            raise FactError("match_not_observed")
        if not isinstance(data["sent"], list) or not isinstance(data["received"], list):
            raise FactError("delivery collections must be lists")
        result = cls(writer, reader, text(match["writer_id"], "match.writer_id"),
                     text(match["reader_id"], "match.reader_id"), integer(match["at_ns"], "match.at_ns"),
                     integer(window["start_ns"], "window.start_ns"), integer(window["end_ns"], "window.end_ns"),
                     boolean(window["complete"], "window.complete"),
                     tuple(Delivery.parse(item) for item in data["sent"]),
                     tuple(Delivery.parse(item, True) for item in data["received"]))
        if result.window_end_ns <= result.window_start_ns:
            raise FactError("invalid observation window")
        if result.matched_ns < max(writer.created_ns, reader.created_ns):
            raise FactError("match timestamp precedes endpoint creation")
        if result.window_start_ns < result.matched_ns:
            raise FactError("window precedes match")
        if any(item.at_ns > result.window_end_ns for item in result.sent + result.received):
            raise FactError("delivery outside recorded observation boundary")
        return result


@dataclass(frozen=True)
class RecoveryFacts:
    delivery: PrimaryFacts
    boundary_ns: int
    activity_ns: int
    rules_restored: bool
    identity_stable: bool

    @classmethod
    def parse(cls, data):
        delivery = PrimaryFacts.parse(data)
        recovery = data["recovery"]
        boundary = integer(recovery["boundary_ns"], "recovery.boundary_ns")
        activity = integer(recovery["activity_ns"], "recovery.activity_ns")
        rules = boolean(recovery["rules_restored"], "recovery.rules_restored")
        stable = boolean(recovery["identity_stable"], "recovery.identity_stable")
        if not rules or not boundary < activity <= delivery.window_start_ns:
            raise FactError("independent post-boundary discovery recovery not established")
        if recovery.get("activity_identity_correlated") is not True:
            raise FactError("discovery activity is not correlated to the matched endpoints")
        if recovery.get("fresh_match_and_graph") is not True:
            raise FactError("fresh post-boundary count and graph proof missing")
        if delivery.matched_ns < boundary:
            raise FactError("recovery match proof precedes restoration boundary")
        if recovery.get("match_continuity") not in ("retained", "reestablished"):
            raise FactError("match continuity observation missing")
        drop = data["drop"]
        if boolean(drop["active"], "drop.active"):
            if not all(drop.get(k) is True for k in ("configured", "applied", "pair_bound", "counter_observed")):
                raise FactError("drop carrier proof incomplete")
            if integer(drop["packets"], "drop.packets") < 1 or drop.get("hit") is not True:
                raise FactError("drop has no actual pair discovery packet hit")
        elif any(drop.get(k) is True for k in ("applied", "hit")):
            raise FactError("neutral arm claimed discovery drop")
        return cls(delivery, boundary, activity, rules, stable)


PROJECTION_FIELDS = ("endpoint_guid", "role", "topic", "type", "reliability", "durability", "deadline", "liveliness.kind", "liveliness.lease_duration")


@dataclass(frozen=True)
class ProjectionFacts:
    writer: Endpoint
    reader: Endpoint
    comparisons: tuple
    differences: tuple

    @classmethod
    def parse(cls, data):
        writer, reader = Endpoint.parse(data["writer"]), Endpoint.parse(data["reader"])
        if writer.id == reader.id:
            raise FactError("projection endpoint identities must be distinct")
        window, projection = data["window"], data["projection"]
        if boolean(window["complete"], "window.complete") is not True or not integer(window["start_ns"], "window.start") < integer(window["end_ns"], "window.end"):
            raise FactError("projection window incomplete")
        if projection.get("unknown_reasons") or projection.get("comparison_status") not in ("consistent", "mismatch"):
            raise FactError("projection observation unknown")
        if projection.get("history_depth") != "not_applicable_by_endpoint_schema" or tuple(projection["baseline_fields"]) != PROJECTION_FIELDS:
            raise FactError("projection baseline field contract differs")
        sources = projection["independent_sources"]
        if not all(isinstance(sources.get(k), dict) and sources[k].get("path") and sources[k].get("sha256") for k in ("wire", "graph")) or sources["wire"]["path"] == sources["graph"]["path"]:
            raise FactError("independent wire and graph sources required")
        rows = projection["comparisons"]
        expected = {(e.id, e.generation, field) for e in (writer, reader) for field in PROJECTION_FIELDS}
        if not isinstance(rows, list) or len(rows) != len(expected) or {(r["endpoint_guid"], r["generation"], r["field"]) for r in rows} != expected:
            raise FactError("projection field/generation coverage incomplete or duplicated")
        differences = []
        for row in rows:
            if not isinstance(row.get("wire"), dict) or row["wire"].get("status") != "observed" or row["wire"].get("value") is None or row.get("graph_value") is None:
                raise FactError("projection requires independently observed wire and graph values")
            if not boolean(row["equal"], "comparison.equal"):
                differences.append(row)
        if projection.get("differences") != differences or (projection["comparison_status"] == "mismatch") != bool(differences):
            raise FactError("projection status/differences contradict comparison facts")
        return cls(writer, reader, tuple(rows), tuple(differences))

# --- Fuzz expansion fact declarations (T02) ----------------------------------
#
# Declaring a fact is not execution evidence: every entry lists the independent
# sources and identity binding that must back a real observation.  This module
# only declares the specs; producers live in runtime/campaign adapters.

@dataclass(frozen=True)
class FactSpec:
    id: str
    version: int
    value_type: str
    unit: str
    scope: str
    sources: tuple
    producer_id: str
    freshness: str
    identity_binding: str
    missing_policy: str

    def __post_init__(self):
        for name in ("id", "value_type", "unit", "scope", "producer_id",
                     "freshness", "identity_binding", "missing_policy"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise FactError(f"FactSpec.{name}: expected nonempty text")
        if type(self.version) is not int or self.version < 1:
            raise FactError("FactSpec.version: expected positive integer")
        if not isinstance(self.sources, tuple) or not self.sources:
            raise FactError("FactSpec.sources: expected nonempty tuple")
        if any(not isinstance(item, str) or not item for item in self.sources):
            raise FactError("FactSpec.sources: expected nonempty source ids")
        if self.missing_policy not in ("unknown_not_pass", "blocked_not_pass"):
            raise FactError("FactSpec.missing_policy must not default to pass")

    def to_dict(self):
        return {"id": self.id, "version": self.version, "value_type": self.value_type,
                "unit": self.unit, "scope": self.scope, "sources": list(self.sources),
                "producer_id": self.producer_id, "freshness": self.freshness,
                "identity_binding": self.identity_binding, "missing_policy": self.missing_policy}


def _spec(fact_id, value_type, unit, scope, sources, producer_id, freshness, identity_binding):
    return FactSpec(fact_id, 1, value_type, unit, scope, tuple(sources), producer_id,
                    freshness, identity_binding, "unknown_not_pass")


FACT_SPECS = {spec.id: spec for spec in (
    _spec("endpoint.submitted_qos.v1", "qos_config", "mixed", "endpoint",
          ("campaign.identity", "input_capsule"), "campaign.identity.semantic_input",
          "per_input", "submitted endpoint role and value"),
    _spec("endpoint.observed_qos.v1", "qos_observation", "mixed", "endpoint",
          ("ros_graph", "passive_rtps_wire"), "runtime.worker",
          "per_observation", "endpoint id plus generation"),
    _spec("endpoint.create_identity.v1", "endpoint_identity", "none", "endpoint",
          ("endpoint_tokens", "process_lifecycle"), "runtime.worker",
          "per_create_call", "endpoint id, role and generation"),
    _spec("late.writer_prejoin.v1", "chronology", "ns", "run",
          ("endpoint_tokens", "publish_tokens"), "runtime.ros2_pair",
          "per_input", "writer identity plus raw send token"),
    _spec("late.reader_create_order.v1", "chronology", "ns", "run",
          ("endpoint_tokens", "create_call_receipts"), "runtime.ros2_pair",
          "per_input", "reader create call and generation"),
    _spec("discovery.drop_window.v1", "window_counter", "packets", "window",
          ("namespace_bindings", "discovery_drop_counter", "raw_events"),
          "runtime.namespace_pair", "per_window",
          "window id plus writer/reader pair identities"),
    _spec("discovery.restore_activity.v1", "recovery_activity", "ns", "window",
          ("independent_discovery_activity", "fresh_graph_count", "raw_events"),
          "campaign.recovery_adapter", "per_window",
          "restored pair identity plus fresh post-boundary activity"),
    _spec("discovery.pair_match.v1", "pair_relation", "none", "pair",
          ("match_snapshot", "graph_snapshot"), "campaign.runtime_adapter",
          "per_snapshot", "writer/reader id plus generation"),
    _spec("observation.completeness.v1", "completeness", "none", "window",
          ("observer_health", "log_closure", "raw_events"), "campaign.runtime_adapter",
          "per_window", "observer and window ids"),
    _spec("mutation.actual_effect.v1", "effect_status", "none", "layer",
          ("mutation_receipt", "before_after_raw"), "campaign.runtime_adapter",
          "per_step", "operation id plus input before/after identity"),
    _spec("delivery.send_sequence.v1", "sequence_set", "count", "window",
          ("publish_tokens", "raw_events"), "runtime.ros2_pair",
          "per_window", "writer identity plus generation"),
    _spec("delivery.receive_sequence.v1", "sequence_set", "count", "window",
          ("receive_tokens", "raw_events"), "runtime.ros2_pair",
          "per_window", "reader and writer identity plus generation"),
    _spec("delivery.closure.v1", "closure", "count", "window",
          ("delivery.send_sequence.v1", "delivery.receive_sequence.v1"),
          "modules.s1.oracles", "per_window", "both endpoint generations"),
    _spec("compatibility.relation_guard.v1", "relation", "none", "pair",
          ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1"),
          "modules.s1.oracles", "per_window",
          "offered and requested endpoint identities plus generations"),
)}
