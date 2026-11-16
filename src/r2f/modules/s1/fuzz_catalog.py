"""Versioned S1 fuzz surfaces, fact/rule/effect references and seed corpus.

This module is declarative.  A surface being listed here means the input
dimension and its contract are reviewable, not that a live ROS carrier has
executed it.  Capability evidence is tracked separately by the campaign layer.

Strictness rules:
  * every mutator/fact/rule/effect reference must resolve in this catalog;
  * a surface without a registered runtime binding is generatable but NOT
    executable and is reported as ``unsupported_capability``;
  * the seed corpus distinguishes authored files from pending placeholders so a
    missing seed can never be counted as an existing one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path

from .facts import FACT_SPECS, FactError
from .facts import FactSpec  # re-exported for callers building fixtures

CATALOG_VERSION = 1
SEED_SCHEMA_VERSION = 1
SEED_DIR = Path(__file__).resolve().parents[4] / "config" / "s1_fuzz" / "seeds"
PENDING = "pending_not_authored"

SURFACE_FIELDS = (
    "surface_id", "version", "layer", "kind", "source_refs", "normative_refs",
    "input_paths", "value_type", "unit", "domain", "special_values",
    "boundary_classes", "mutator_ids", "value_relations", "tool_requires",
    "sut_guards", "carrier_id", "runtime_phase", "runtime_binding",
    "allowed_with", "conflicts_with", "order_after", "fact_ids", "rule_ids",
    "effect_id", "missing_policy", "unsupported_policy",
)

CORE_LAYERS = ("P", "E", "G")
AUXILIARY_LAYERS = ("W",)


class CatalogError(ValueError):
    """Raised with the exact offending id, never a generic failure."""


def _texts(values, name):
    values = tuple(values)
    if not values or any(not isinstance(v, str) or not v for v in values):
        raise CatalogError(f"{name}: expected nonempty string ids")
    return values


@dataclass(frozen=True)
class SurfaceSpec:
    surface_id: str
    version: int
    layer: str
    kind: str
    source_refs: tuple
    normative_refs: tuple
    input_paths: tuple
    value_type: str
    unit: str
    domain: dict
    special_values: dict
    boundary_classes: tuple
    mutator_ids: tuple
    value_relations: tuple
    tool_requires: tuple
    sut_guards: tuple
    carrier_id: str
    runtime_phase: str
    runtime_binding: str | None
    allowed_with: tuple
    conflicts_with: tuple
    order_after: tuple
    fact_ids: tuple
    rule_ids: tuple
    effect_id: str
    missing_policy: str
    unsupported_policy: str

    def __post_init__(self):
        for name in ("surface_id", "kind", "value_type", "unit", "carrier_id",
                     "runtime_phase", "effect_id", "missing_policy", "unsupported_policy"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise CatalogError(f"{self.surface_id}.{name}: expected nonempty text")
        if self.layer not in CORE_LAYERS + AUXILIARY_LAYERS:
            raise CatalogError(f"{self.surface_id}.layer: unsupported layer {self.layer}")
        if type(self.version) is not int or self.version < 1:
            raise CatalogError(f"{self.surface_id}.version: expected positive integer")
        if self.missing_policy not in ("unknown_not_pass", "blocked_not_pass"):
            raise CatalogError(f"{self.surface_id}.missing_policy must not default to pass")
        if not isinstance(self.domain, dict) or not self.domain:
            raise CatalogError(f"{self.surface_id}.domain: expected nonempty dict")
        if not isinstance(self.special_values, dict):
            raise CatalogError(f"{self.surface_id}.special_values: expected dict")
        for name in ("source_refs", "normative_refs", "input_paths", "boundary_classes",
                     "mutator_ids", "value_relations", "tool_requires", "sut_guards",
                     "allowed_with", "conflicts_with", "order_after", "fact_ids", "rule_ids"):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                raise CatalogError(f"{self.surface_id}.{name}: expected tuple")
            if any(not isinstance(item, str) or not item for item in value):
                raise CatalogError(f"{self.surface_id}.{name}: expected nonempty strings")
        if self.runtime_binding is not None and (not isinstance(self.runtime_binding, str) or not self.runtime_binding):
            raise CatalogError(f"{self.surface_id}.runtime_binding: expected id or None")

    @property
    def generatable(self):
        return bool(self.input_paths and self.mutator_ids and self.fact_ids and self.rule_ids)

    @property
    def executable(self):
        return self.generatable and self.runtime_binding in RUNTIME_BINDINGS

    @property
    def capability_status(self):
        return "executable" if self.executable else "unsupported_capability"

    def to_dict(self):
        data = {name: getattr(self, name) for name in SURFACE_FIELDS}
        for key, value in list(data.items()):
            if isinstance(value, tuple):
                data[key] = list(value)
            elif isinstance(value, dict):
                data[key] = json.loads(json.dumps(value))
        data["generatable"] = self.generatable
        data["executable"] = self.executable
        data["capability_status"] = self.capability_status
        return data


@dataclass(frozen=True)
class RuleSpec:
    id: str
    version: int
    spec_refs: tuple
    applies_when: dict
    required_fact_ids: tuple
    evaluator_id: str
    allowed_effects: tuple
    forbidden_residuals: tuple
    limitations: tuple
    responsibility_layer: str
    primary_or_supporting: str

    def __post_init__(self):
        for name in ("id", "evaluator_id", "responsibility_layer", "primary_or_supporting"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise CatalogError(f"RuleSpec.{name}: expected nonempty text")
        if type(self.version) is not int or self.version < 1:
            raise CatalogError(f"{self.id}.version: expected positive integer")
        if self.primary_or_supporting not in ("primary", "supporting"):
            raise CatalogError(f"{self.id}.primary_or_supporting: invalid value")
        if not isinstance(self.applies_when, dict) or not self.applies_when:
            raise CatalogError(f"{self.id}.applies_when: expected nonempty dict")
        for name in ("spec_refs", "required_fact_ids", "allowed_effects", "forbidden_residuals", "limitations"):
            values = getattr(self, name)
            if not isinstance(values, tuple) or any(not isinstance(v, str) or not v for v in values):
                raise CatalogError(f"{self.id}.{name}: expected tuple of nonempty strings")

    def to_dict(self):
        return {name: (list(getattr(self, name)) if isinstance(getattr(self, name), tuple) else getattr(self, name))
                for name in ("id", "version", "spec_refs", "applies_when", "required_fact_ids",
                             "evaluator_id", "allowed_effects", "forbidden_residuals",
                             "limitations", "responsibility_layer", "primary_or_supporting")}


# Runtime binding ids that already exist in the checked-out runtime/identity
# code.  A binding claim here is still not execution evidence.
RUNTIME_BINDINGS = {
    "late_joiner.reader_delay_sec": {
        "runtime_field": "reader_delay_sec",
        "wired_in": ("campaign.identity.semantic_input", "runtime.ros2_pair.normalize_config"),
        "phase": "endpoint_create",
    },
    "projection.qos_profile": {
        "runtime_field": "qos",
        "wired_in": ("campaign.identity.projection_input", "runtime.projection_pair.validate_qos"),
        "phase": "endpoint_create",
    },
    "discovery.drop_duration_sec": {
        "runtime_field": "drop_duration_sec",
        "wired_in": ("runtime.namespace_pair.normalize_discovery_config",),
        "phase": "post_match_window",
    },
    "pair.publish_interval_sec": {
        "runtime_field": "publish_interval_sec",
        "wired_in": ("runtime.ros2_pair.normalize_config",),
        "phase": "publish_window",
    },
    "pair.history_and_sample_counts": {
        "runtime_field": "qos.depth,pre_match_count,post_match_count",
        "wired_in": ("runtime.ros2_pair.normalize_config",),
        "phase": "publish_window",
    },
}

REGISTERED_EVALUATORS = {
    "modules.s1.oracles.evaluate_primary": "primary",
    "modules.s1.oracles.evaluate_match_compatibility_guard": "g01",
}

_EFFECTS = {
    "s1.late_join.delivery": {
        "allowed_effects": ("reader_created_after_writer", "match_established", "post_join_delivery"),
        "forbidden_residuals": ("post_match_delivery_loss", "wrong_endpoint_match",
                                "identity_generation_mixup", "delivery_order_violation"),
    },
    "s1.projection.qos_field": {
        "allowed_effects": ("expected_qos_projection",),
        "forbidden_residuals": ("builtin_graph_field_mismatch",),
    },
    "s1.discovery.drop_restore": {
        "allowed_effects": ("discovery_temporarily_unavailable", "discovery_restored"),
        "forbidden_residuals": ("post_restore_discovery_inconsistency", "post_restore_delivery_loss",
                                "wrong_endpoint_match", "identity_generation_mixup",
                                "delivery_order_violation"),
    },
    "s1.discovery.compatibility_guard": {
        "allowed_effects": ("incompatible_pair_not_matched", "compatible_pair_matched"),
        "forbidden_residuals": ("prohibited_match_observed", "prohibited_delivery_observed"),
    },
    "s1.workload.bounded_send": {
        "allowed_effects": ("bounded_send_window_completed",),
        "forbidden_residuals": ("send_receive_closure_violation",),
    },
}

_MUTATORS = ("M01 boundary_pick", "M02 enum_replace", "M03 range_sample",
             "M04 relation_boundary", "M05 special_value", "M06 timing_offset",
             "M07 environment_windows", "M08 compatibility_pair")
MUTATOR_IDS = {item.split()[0] for item in _MUTATORS}
MUTATOR_CATALOG = {item.split()[0]: {"name": item.split()[1], "version": 1,
                                     "status": "declared_not_implemented_in_this_slice"}
                   for item in _MUTATORS}

_BUDGET = ("docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md#3.1 "
           "(engineering sampling budget, not a DDS normative bound)")


def _surface(surface_id, layer, kind, input_paths, value_type, unit, domain,
             special_values, boundary_classes, mutator_ids, value_relations,
             tool_requires, sut_guards, carrier_id, runtime_phase, runtime_binding,
             allowed_with, conflicts_with, order_after, fact_ids, rule_ids,
             effect_id, missing_policy="unknown_not_pass",
             unsupported_policy="reject_not_executable", normative_refs=()):
    return SurfaceSpec(
        surface_id=surface_id, version=1, layer=layer, kind=kind,
        source_refs=(_BUDGET,), normative_refs=tuple(normative_refs),
        input_paths=tuple(input_paths), value_type=value_type, unit=unit,
        domain=domain, special_values=dict(special_values),
        boundary_classes=tuple(boundary_classes), mutator_ids=tuple(mutator_ids),
        value_relations=tuple(value_relations), tool_requires=tuple(tool_requires),
        sut_guards=tuple(sut_guards), carrier_id=carrier_id, runtime_phase=runtime_phase,
        runtime_binding=runtime_binding, allowed_with=tuple(allowed_with),
        conflicts_with=tuple(conflicts_with), order_after=tuple(order_after),
        fact_ids=tuple(fact_ids), rule_ids=tuple(rule_ids), effect_id=effect_id,
        missing_policy=missing_policy, unsupported_policy=unsupported_policy,
    )


_SURFACES = (
    _surface("P01", "P", "late_reader_delay",
             ("reader_delay_sec",), "duration", "s",
             {"kind": "quantized_duration", "representatives": [0, 0.001, 0.01, 0.02, 0.15, 1.0, 3.0],
              "sampled_range_s": [0, 3], "quantum": 0.001},
             {"zero": 0.0}, ("zero", "finite", "quantized_neighbor"),
             ("M01", "M03", "M06"),
             ("reader_delay < post_match_delivery_window",),
             ("public_ros2_api", "writer_prejoin_before_reader_create"),
             ("reader_created_after_writer",),
             "ros2_endpoint", "endpoint_create", "late_joiner.reader_delay_sec",
             ("W02", "E01"), (), (),
             ("late.writer_prejoin.v1", "late.reader_create_order.v1",
              "delivery.send_sequence.v1", "delivery.receive_sequence.v1",
              "delivery.closure.v1", "observation.completeness.v1"),
             ("s1.late_join.delivery.v1",), "s1.late_join.delivery"),
    _surface("P02", "P", "projection_reliability",
             ("qos.reliability",), "enum", "policy",
             {"values": ["reliable", "best_effort"]}, {},
             ("enum_exhaustive",), ("M02",), (),
             ("public_ros2_api",), ("endpoint_created_before_observation",),
             "ros2_endpoint", "endpoint_create", "projection.qos_profile",
             (), ("P03",), (),
             ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
              "endpoint.create_identity.v1", "observation.completeness.v1"),
             ("s1.projection.qos_field.v1",), "s1.projection.qos_field"),
    _surface("P03", "P", "projection_durability",
             ("qos.durability",), "enum", "policy",
             {"values": ["volatile", "transient_local"]}, {},
             ("enum_exhaustive",), ("M02",), (),
             ("public_ros2_api",), ("endpoint_created_before_observation",),
             "ros2_endpoint", "endpoint_create", "projection.qos_profile",
             (), ("P02",), (),
             ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
              "endpoint.create_identity.v1", "observation.completeness.v1"),
             ("s1.projection.qos_field.v1",), "s1.projection.qos_field"),
    _surface("P04", "P", "projection_deadline",
             ("qos.deadline_ns",), "duration_ns", "ns",
             {"kind": "integer_ns_with_special_classes",
              "finite_representatives": [1, 999999999, 1000000000, 1000000001, 1250000000],
              "sampled_range_ns": [1, 9000000000]},
             {"zero": 0, "infinite": 9223372036854775807},
             ("zero", "finite", "infinite", "quantized_neighbor"),
             ("M01", "M03", "M05"),
             ("infinite_class_is_not_a_large_finite_number",),
             ("public_ros2_api",), ("endpoint_created_before_observation",),
             "ros2_endpoint", "endpoint_create", "projection.qos_profile",
             (), (), (),
             ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
              "endpoint.create_identity.v1", "observation.completeness.v1"),
             ("s1.projection.qos_field.v1",), "s1.projection.qos_field"),
    _surface("P05", "P", "projection_liveliness",
             ("qos.liveliness",), "enum", "policy",
             {"values": ["automatic", "manual_by_topic"]}, {},
             ("enum_exhaustive",), ("M02",), (),
             ("public_ros2_api",), ("endpoint_created_before_observation",),
             "ros2_endpoint", "endpoint_create", "projection.qos_profile",
             (), (), (),
             ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
              "endpoint.create_identity.v1", "observation.completeness.v1"),
             ("s1.projection.qos_field.v1",), "s1.projection.qos_field"),
    _surface("P06", "P", "projection_liveliness_lease_duration",
             ("qos.liveliness_lease_duration_ns",), "duration_ns", "ns",
             {"kind": "integer_ns_with_special_classes",
              "finite_representatives": [1, 100000000, 1000000000, 5000000000],
              "sampled_range_ns": [1, 9000000000]},
             {"zero": 0, "infinite": 9223372036854775807},
             ("zero", "finite", "infinite", "quantized_neighbor"),
             ("M01", "M03", "M05"),
             ("lease_configuration_is_not_lease_lifecycle_coverage",),
             ("public_ros2_api",), ("endpoint_created_before_observation",),
             "ros2_endpoint", "endpoint_create", "projection.qos_profile",
             (), (), (),
             ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
              "endpoint.create_identity.v1", "observation.completeness.v1"),
             ("s1.projection.qos_field.v1",), "s1.projection.qos_field"),
    _surface("E01", "E", "discovery_drop_duration",
             ("drop_duration_sec",), "duration", "s",
             {"kind": "log_range_duration", "representatives": [0.05, 0.2, 1.0, 3.0, 8.0],
              "sampled_range_s": [0.05, 8.0]},
             {"zero_window_is_neutral_not_hit": 0.0},
             ("short_window_may_be_ineffective", "log_range"),
             ("M01", "M03", "M07"),
             ("next_window_requires_previous_restore",),
             ("isolated_network_namespace", "pair_bound_drop_counter"),
             ("discovery_activity_observed_after_restore",),
             "wire_proxy", "post_match_window", "discovery.drop_duration_sec",
             ("G01",), (), (),
             ("discovery.drop_window.v1", "discovery.restore_activity.v1",
              "discovery.pair_match.v1", "delivery.send_sequence.v1",
              "delivery.receive_sequence.v1", "delivery.closure.v1",
              "observation.completeness.v1"),
             ("s1.discovery.drop_restore.v1",), "s1.discovery.drop_restore"),
    _surface("E02", "E", "discovery_multiwindow_restore_interval",
             ("environment_windows.count", "environment_windows.restore_interval_sec"),
             "sequence_count_and_duration", "count,s",
             {"window_counts": [1, 2, 3],
              "restore_interval_representatives_s": [0.0, 0.02, 0.2, 1.0]},
             {}, ("count_boundary", "interval_boundary"),
             ("M04", "M07"),
             ("restore_before_next_drop", "max_three_windows"),
             ("isolated_network_namespace",), ("previous_window_restored",),
             "wire_proxy", "post_match_window", None,
             ("E03",), ("E01",), (),
             ("discovery.drop_window.v1", "discovery.restore_activity.v1",
              "observation.completeness.v1"),
             ("s1.discovery.drop_restore.v1",), "s1.discovery.drop_restore",
             unsupported_policy="unsupported_capability:multi_window_runner_not_wired"),
    _surface("E03", "E", "discovery_anchor_offset",
             ("environment_windows.anchor", "environment_windows.anchor_offset_sec"),
             "event_anchor_and_duration", "event,s",
             {"anchors": ["fresh_match", "previous_restore_confirmed"],
              "offsets_s": [0.0, 0.01, 0.1, 0.5]},
             {}, ("phase_offset", "anchor_boundary"),
             ("M06", "M07"),
             ("anchor_event_must_precede_drop_begin",),
             ("isolated_network_namespace",), ("anchor_event_observed",),
             "wire_proxy", "post_match_window", None,
             ("E02",), ("E01",), ("E02",),
             ("discovery.drop_window.v1", "discovery.pair_match.v1",
              "discovery.restore_activity.v1"),
             ("s1.discovery.drop_restore.v1",), "s1.discovery.drop_restore",
             unsupported_policy="unsupported_capability:event_anchor_runner_not_wired"),
    _surface("G01", "G", "offered_requested_reliability_compatibility",
             ("writer_qos.reliability", "reader_qos.reliability"),
             "pair_relation", "policy_pair",
             {"pairs": ["RR", "RB", "BB", "BR"],
              "incompatible_pairs": ["BR"]},
             {}, ("relation_false", "relation_true"),
             ("M02", "M08"),
             ("BR_is_incompatible_not_an_invalid_api_value",),
             ("public_ros2_api", "independent_role_qos"),
             ("identity_backed_match_or_delivery_only",),
             "ros2_endpoint", "endpoint_create", None,
             ("E01",), (), (),
             ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
              "discovery.pair_match.v1", "delivery.send_sequence.v1",
              "delivery.receive_sequence.v1", "observation.completeness.v1"),
             ("s1.discovery.compatibility_guard.v1",),
             "s1.discovery.compatibility_guard",
             unsupported_policy="unsupported_capability:per_role_qos_runtime_not_wired"),
    _surface("W01", "W", "history_depth_and_sample_count",
             ("qos.depth", "pre_match_count", "post_match_count"),
             "integer_count", "count",
             {"depth": [1, 2, 3, 8, 32, 64, 65, 128],
              "pre_count": [1, 2, 8],
              "post_count": [3, 6, 32, 64, 65, 128]},
             {}, ("depth_lt_post", "depth_eq_post", "depth_gt_post"),
             ("M04", "M03"),
             ("depth_is_not_a_required_sedp_wire_field",),
             ("public_ros2_api",), ("delivery_obligation_reviewed",),
             "ros2_endpoint", "publish_window", "pair.history_and_sample_counts",
             ("P01",), (), (),
             ("delivery.send_sequence.v1", "delivery.receive_sequence.v1",
              "delivery.closure.v1", "observation.completeness.v1"),
             ("s1.workload.bounded_send.v1",), "s1.workload.bounded_send"),
    _surface("W02", "W", "publish_interval",
             ("publish_interval_sec",), "duration", "s",
             {"representatives": [0.0, 0.001, 0.01, 0.03, 0.1],
              "sampled_range_s": [0, 0.1]},
             {"zero": 0.0}, ("zero", "finite"),
             ("M02", "M03"),
             ("not_a_high_throughput_claim",),
             ("public_ros2_api",), ("actual_send_interval_observed",),
             "ros2_endpoint", "publish_window", "pair.history_and_sample_counts",
             ("P01",), (), (),
             ("delivery.send_sequence.v1", "observation.completeness.v1"),
             ("s1.workload.bounded_send.v1",), "s1.workload.bounded_send"),
)

SURFACES = {surface.surface_id: surface for surface in _SURFACES}
CORE_SURFACE_IDS = tuple(s.surface_id for s in _SURFACES if s.layer in CORE_LAYERS)
AUXILIARY_SURFACE_IDS = tuple(s.surface_id for s in _SURFACES if s.layer in AUXILIARY_LAYERS)


def _rule(rule_id, fact_ids, evaluator_id, allowed, forbidden, applies, limitations,
          layer="discovery", role="primary"):
    return RuleSpec(id=rule_id, version=1,
                    spec_refs=("docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md#5.3",),
                    applies_when=applies, required_fact_ids=tuple(fact_ids),
                    evaluator_id=evaluator_id, allowed_effects=tuple(allowed),
                    forbidden_residuals=tuple(forbidden), limitations=tuple(limitations),
                    responsibility_layer=layer, primary_or_supporting=role)


_RULES = (
    _rule("s1.late_join.delivery.v1",
          ("late.writer_prejoin.v1", "late.reader_create_order.v1",
           "delivery.send_sequence.v1", "delivery.receive_sequence.v1",
           "delivery.closure.v1", "observation.completeness.v1"),
          "modules.s1.oracles.evaluate_primary",
          ("reader_created_after_writer", "match_established", "post_join_delivery"),
          ("post_match_delivery_loss", "wrong_endpoint_match", "identity_generation_mixup",
           "delivery_order_violation"),
          {"target_id": "discovery.late_endpoint", "reader_after_writer": True},
          ("an in-flight tail is unknown; a finite unmatched window is not a violation",)),
    _rule("s1.projection.qos_field.v1",
          ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
           "endpoint.create_identity.v1", "observation.completeness.v1"),
          "modules.s1.oracles.evaluate_primary",
          ("expected_qos_projection",), ("builtin_graph_field_mismatch",),
          {"target_id": "builtin.discovery_projection_reconciliation"},
          ("wire and graph must be independent; history depth is not an endpoint wire field",)),
    _rule("s1.discovery.drop_restore.v1",
          ("discovery.drop_window.v1", "discovery.restore_activity.v1",
           "discovery.pair_match.v1", "delivery.closure.v1",
           "observation.completeness.v1"),
          "modules.s1.oracles.evaluate_primary",
          ("discovery_temporarily_unavailable", "discovery_restored"),
          ("post_restore_discovery_inconsistency", "post_restore_delivery_loss",
           "wrong_endpoint_match", "identity_generation_mixup", "delivery_order_violation"),
          {"target_id": "discovery.interruption_recovery"},
          ("rule deletion is not recovery; per-window counters may not be accumulated",)),
    _rule("s1.discovery.compatibility_guard.v1",
          ("endpoint.submitted_qos.v1", "endpoint.observed_qos.v1",
           "discovery.pair_match.v1", "delivery.send_sequence.v1",
           "delivery.receive_sequence.v1", "observation.completeness.v1"),
          "modules.s1.oracles.evaluate_match_compatibility_guard",
          ("incompatible_pair_not_matched", "compatible_pair_matched"),
          ("prohibited_match_observed", "prohibited_delivery_observed"),
          {"pair_relation": "offered_requested_reliability", "incompatible": ["BR"]},
          ("creation success is never itself a violation; compatible finite-window no-match is unknown",),
          layer="compatibility"),
    _rule("s1.workload.bounded_send.v1",
          ("delivery.send_sequence.v1", "delivery.receive_sequence.v1",
           "delivery.closure.v1", "observation.completeness.v1"),
          "modules.s1.oracles.evaluate_primary",
          ("bounded_send_window_completed",), ("send_receive_closure_violation",),
          {"workload": "bounded"},
          ("supporting workload only; does not assert throughput",),
          role="supporting"),
)

RULES = {rule.id: rule for rule in _RULES}
EFFECTS = {key: {"effect_id": key, **value} for key, value in _EFFECTS.items()}

#: The catalogue-level rule -> effect relation.  A rule is declared together
#: with exactly one effect by every surface that cites it; this map records
#: that binding so a rule can be resolved even when its surface cannot yet be
#: projected into the core contract (F2 capability pending).
RULE_EFFECT_ID = {}
for _surface_spec in _SURFACES:
    for _rule_id in _surface_spec.rule_ids:
        previous = RULE_EFFECT_ID.setdefault(_rule_id, _surface_spec.effect_id)
        if previous != _surface_spec.effect_id:
            raise CatalogError(
                f"{_rule_id}: declared with conflicting effects {previous} and {_surface_spec.effect_id}")
ALLOWED_EFFECT_VOCAB = frozenset(e for spec in EFFECTS.values() for e in spec["allowed_effects"])
FORBIDDEN_RESIDUAL_VOCAB = frozenset(r for spec in EFFECTS.values() for r in spec["forbidden_residuals"])


def core_count():
    return len(CORE_SURFACE_IDS)


def auxiliary_count():
    return len(AUXILIARY_SURFACE_IDS)


def surface_counts():
    """Programmatic counts; auxiliary layers are never folded into core."""
    return {"core": core_count(), "auxiliary": auxiliary_count(),
            "total": len(SURFACES), "core_ids": list(CORE_SURFACE_IDS),
            "auxiliary_ids": list(AUXILIARY_SURFACE_IDS)}


def _resolve(surface, surfaces):
    for mutator_id in surface.mutator_ids:
        if mutator_id not in MUTATOR_IDS:
            raise CatalogError(f"{surface.surface_id}: unknown mutator id {mutator_id}")
    for fact_id in surface.fact_ids:
        if fact_id not in FACT_SPECS:
            raise CatalogError(f"{surface.surface_id}: unknown fact id {fact_id}")
    if surface.effect_id not in EFFECTS:
        raise CatalogError(f"{surface.surface_id}: unknown effect id {surface.effect_id}")
    for rule_id in surface.rule_ids:
        if rule_id not in RULES:
            raise CatalogError(f"{surface.surface_id}: unknown rule id {rule_id}")
    for other in surface.allowed_with + surface.conflicts_with + surface.order_after:
        if other not in surfaces:
            raise CatalogError(f"{surface.surface_id}: unknown related surface {other}")
        if other in surface.conflicts_with and other in surface.allowed_with:
            raise CatalogError(f"{surface.surface_id}: {other} both allowed and conflicting")
    if surface.surface_id in surface.allowed_with + surface.conflicts_with + surface.order_after:
        raise CatalogError(f"{surface.surface_id}: self reference in composition relations")
    for related in surface.order_after:
        if surface.surface_id not in surfaces[related].allowed_with:
            raise CatalogError(f"{surface.surface_id}: order_after {related} not mutually allowed")


def validate_catalog(surfaces=None, rules=None, effects=None, fact_specs=None):
    """Validate the whole catalog and return an execution/capability report."""
    surfaces = SURFACES if surfaces is None else dict(surfaces)
    rules = RULES if rules is None else dict(rules)
    effects = EFFECTS if effects is None else dict(effects)
    fact_specs = FACT_SPECS if fact_specs is None else dict(fact_specs)
    if len(surfaces) != 12:
        raise CatalogError(f"catalog: expected 12 surfaces, found {len(surfaces)}")
    core = [s for s in surfaces.values() if s.layer in CORE_LAYERS]
    auxiliary = [s for s in surfaces.values() if s.layer in AUXILIARY_LAYERS]
    if len(core) != 10 or len(auxiliary) != 2:
        raise CatalogError(f"catalog: expected 10 core + 2 auxiliary, found {len(core)} + {len(auxiliary)}")
    for surface in surfaces.values():
        _resolve(surface, surfaces)
    for rule in rules.values():
        if rule.evaluator_id not in REGISTERED_EVALUATORS:
            raise CatalogError(f"{rule.id}: unknown evaluator id {rule.evaluator_id}")
        for fact_id in rule.required_fact_ids:
            if fact_id not in fact_specs:
                raise CatalogError(f"{rule.id}: unknown fact id {fact_id}")
        for effect in rule.allowed_effects:
            if effect not in ALLOWED_EFFECT_VOCAB:
                raise CatalogError(f"{rule.id}: unknown allowed effect {effect}")
        for residual in rule.forbidden_residuals:
            if residual not in FORBIDDEN_RESIDUAL_VOCAB:
                raise CatalogError(f"{rule.id}: unknown forbidden residual {residual}")
        if rule.primary_or_supporting == "primary" and not rule.forbidden_residuals:
            raise CatalogError(f"{rule.id}: primary rule requires forbidden residuals")
    for effect_id, spec in effects.items():
        if spec.get("effect_id") != effect_id:
            raise CatalogError(f"{effect_id}: effect_id mismatch")
    report = {"catalog_version": CATALOG_VERSION,
              "counts": surface_counts(),
              "surfaces": {sid: {"generatable": s.generatable, "executable": s.executable,
                                 "capability_status": s.capability_status,
                                 "runtime_binding": s.runtime_binding}
                           for sid, s in surfaces.items()},
              "rules": list(rules), "effects": list(effects),
              "facts": list(fact_specs)}
    report["executable_surface_ids"] = sorted(k for k, v in report["surfaces"].items() if v["executable"])
    report["unsupported_surface_ids"] = sorted(k for k, v in report["surfaces"].items() if not v["executable"])
    return report


# --- initial corpus ----------------------------------------------------------

# Every catalog surface now has an authored template file.  "authored" means the
# JSON template exists and validates; it does NOT mean the surface is executable
# or that anything ran.  Capability is tracked separately: each surface's
# ``executable`` property still reports E02/E03/G01 as
# ``unsupported_capability``.
#
# The PENDING mechanism is deliberately retained: ``load_seeds`` still refuses a
# missing authored file and still rejects a file that appears for a surface
# declared pending, so an absent/extra corpus entry can never be silently
# counted.  Tests exercise that path with a synthetic status map.
AUTHORED_SEEDS = ("P01", "P02", "P03", "P04", "P05", "P06",
                  "E01", "E02", "E03", "G01", "W01", "W02")
SEED_STATUS = {sid: ("authored" if sid in AUTHORED_SEEDS else PENDING) for sid in SURFACES}

_SEED_REQUIRED = ("seed_id", "version", "surface_refs", "target_family", "capability",
                  "status", "logical_config", "operation_windows", "provenance",
                  "capability_requirements", "parent_content_sha256_rule")


def seed_content_sha256(seed):
    """Fingerprint the semantic content, excluding rng seeds and run identity."""
    payload = {"schema": SEED_SCHEMA_VERSION,
               "version": seed["version"],
               "surface_refs": sorted(seed["surface_refs"]),
               "target_family": seed["target_family"],
               "logical_config": seed["logical_config"],
               "operation_windows": seed["operation_windows"]}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_seed(seed, surfaces=None):
    surfaces = SURFACES if surfaces is None else surfaces
    if not isinstance(seed, dict):
        raise CatalogError("seed: expected object")
    missing = [key for key in _SEED_REQUIRED if key not in seed]
    if missing:
        raise CatalogError(f"seed: missing fields {','.join(missing)}")
    if seed["seed_id"] not in AUTHORED_SEEDS:
        raise CatalogError(f"seed: {seed['seed_id']} is not in the authored set")
    if type(seed["version"]) is not int or seed["version"] != SEED_SCHEMA_VERSION:
        raise CatalogError(f"seed {seed['seed_id']}: unsupported schema version")
    if seed.get("rng_seed") is not None:
        raise CatalogError(f"seed {seed['seed_id']}: rng_seed must be chosen per attempt, not stored")
    if seed.get("parent_content_sha256") is not None:
        raise CatalogError(f"seed {seed['seed_id']}: parent hash is derived, not authored")
    if "content_sha256" in seed:
        raise CatalogError(f"seed {seed['seed_id']}: content hash is derived, not authored")
    for ref in seed["surface_refs"]:
        if ref not in surfaces:
            raise CatalogError(f"seed {seed['seed_id']}: unknown surface {ref}")
    for status in (seed["status"], seed["capability"]["status"]):
        if status not in ("authored", "capability_pending", "unsupported_capability"):
            raise CatalogError(f"seed {seed['seed_id']}: unsupported status {status}")
    if not isinstance(seed["parent_content_sha256_rule"], str) or "rng_seed" not in seed["parent_content_sha256_rule"]:
        raise CatalogError(f"seed {seed['seed_id']}: parent hash rule must state rng_seed exclusion")
    return seed


def load_seeds(directory=None):
    directory = Path(SEED_DIR if directory is None else directory)
    seeds, missing, invalid = {}, [], []
    for sid, status in SEED_STATUS.items():
        path = directory / f"{sid.lower()}.json"
        if status == PENDING:
            if path.exists():
                invalid.append(f"{sid}: pending seed file unexpectedly exists")
            else:
                missing.append(sid)
            continue
        if not path.is_file():
            invalid.append(f"{sid}: authored seed file missing")
            continue
        try:
            seed = json.loads(path.read_text())
            validate_seed(seed)
        except (OSError, ValueError, KeyError) as exc:
            invalid.append(f"{sid}: {exc}")
            continue
        if seed["seed_id"] != sid:
            invalid.append(f"{sid}: seed_id mismatch {seed['seed_id']}")
            continue
        seeds[sid] = seed
    if invalid:
        raise CatalogError("seed corpus invalid: " + "; ".join(invalid))
    return {"seeds": seeds, "pending": missing, "status": dict(SEED_STATUS),
            "authored": sorted(seeds)}


def catalog_digest():
    report = validate_catalog()
    payload = {"version": CATALOG_VERSION,
               "surfaces": [SURFACES[k].to_dict() for k in sorted(SURFACES)],
               "rules": [RULES[k].to_dict() for k in sorted(RULES)],
               "effects": EFFECTS,
               "facts": {k: FACT_SPECS[k].to_dict() for k in sorted(FACT_SPECS)},
               "seed_status": SEED_STATUS}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Projection into the core mutation-plan contracts (T02/T03/T04 seam).
#
# The catalog is the authoritative, versioned declaration.  The core
# ``MutationSurface``/``FactSpec``/``RuleSpec``/``EffectSpec`` vocabulary is a
# narrower v1 schema, so the projection below is explicit and lossy in named
# places instead of silently coercing.  A surface that cannot be expressed
# without lying about its value type or its mutator is reported as
# ``unsupported_capability`` and is excluded from a core contract.
#
# Nothing here executes anything.  A contract is an input-space declaration.
# ---------------------------------------------------------------------------

IMPLEMENTED_MUTATOR_IDS = frozenset({"M01", "M02", "M03"})

#: Catalog boundary classes that describe a special value rather than a
#: numeric range endpoint.  The core ``M01`` only understands ``min``/
#: ``min_plus_one``/``max_minus_one``/``max``; a boundary class such as
#: ``zero``/``infinite``/``finite`` would be silently reinterpreted, so it is
#: pruned from the *generated core contract* and explicitly reported.
_NUMERIC_BOUNDARY_CLASSES = frozenset({"min", "min_plus_one", "max_minus_one", "max"})
_SPECIAL_BOUNDARY_CLASSES = frozenset({"zero", "finite", "infinite"})
F1_SURFACE_IDS = ("P01", "P02", "P03", "P04", "P05", "P06", "E01", "W01", "W02")
F2_SURFACE_IDS = ("E02", "E03", "G01")

#: catalog ``kind`` -> core ``surface_kind``; chosen so that every retained
#: (implemented) mutator for the surface accepts that kind.
_CORE_SURFACE_KIND = {
    "late_reader_delay": "parameter_numeric",
    "projection_reliability": "parameter_enum",
    "projection_durability": "parameter_enum",
    "projection_deadline": "parameter_numeric",
    "projection_liveliness": "parameter_enum",
    "projection_liveliness_lease_duration": "parameter_numeric",
    "discovery_drop_duration": "parameter_numeric",
    "history_depth_and_sample_count": "parameter_numeric",
    "publish_interval": "parameter_numeric",
}

_CORE_VALUE_TYPE = {
    "P01": "duration_sec",
    "P02": "enum",
    "P03": "enum",
    "P04": "duration_ns",
    "P05": "enum",
    "P06": "duration_ns",
    "E01": "duration_sec",
    "W01": "int",
    "W02": "duration_sec",
}

#: catalog fact value_type -> core v1 value_type.  ``core``'s FactSpec has no
#: structured-schema slot, so rich values project to their closest primitive
#: and the catalog entry remains authoritative for producers.
_CORE_FACT_VALUE_TYPE = {
    "qos_config": "str",
    "qos_observation": "str",
    "endpoint_identity": "str",
    "chronology": "duration_ns",
    "window_counter": "int",
    "recovery_activity": "duration_ns",
    "pair_relation": "pair_relation",
    "completeness": "bool",
    "effect_status": "enum",
    "sequence_set": "int",
    "closure": "int",
    "relation": "relation",
}

_EFFECT_WINDOW = {
    "s1.late_join.delivery": "before_reader_match",
    "s1.projection.qos_field": "endpoint_creation",
    "s1.discovery.drop_restore": "discovery_interruption",
    "s1.discovery.compatibility_guard": "pair_compatibility_window",
    "s1.workload.bounded_send": "publish_window",
}

_RULE_RESPONSIBILITY = {
    "discovery": "s1",
    "compatibility": "s1",
    "workload": "s1",
}


class UnsupportedCapabilityError(CatalogError):
    """A surface or target is declared but has no wired runtime binding."""


def _numeric_core_domain(surface):
    domain = surface.domain
    if "sampled_range_s" in domain:
        low, high = domain["sampled_range_s"]
        step = domain.get("quantum", 0.001)
    elif "sampled_range_ns" in domain:
        low, high = domain["sampled_range_ns"]
        step = 1
    elif "window_counts" in domain:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: multi-window surface has no F1 core projection")
    elif isinstance(domain.get("depth"), list):
        values = domain["depth"]
        low, high, step = min(values), max(values), 1
    elif "values" in domain:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: enum surface cannot project to a numeric domain")
    else:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: domain has no numeric range for a core projection")
    return {"min": low, "max": high, "step": step}


def core_mutator_ids(surface):
    """Implemented mutators a surface can actually be generated with today."""
    return tuple(m for m in surface.mutator_ids if m in IMPLEMENTED_MUTATOR_IDS)


def surface_to_core(surface):
    """Project one catalog surface into a validated core MutationSurface.

    Raises ``UnsupportedCapabilityError`` when the projection would have to
    invent a value type or claim an unimplemented mutator.
    """
    from r2f.core.mutation_plan import MutationSurface

    if surface.surface_id in F2_SURFACE_IDS:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: capability pending (declared, not F1 routable)")
    if surface.surface_id == 'W01':
        raise UnsupportedCapabilityError(
            "W01: relation_boundary mutator (M04) is not implemented; not substituting uniform sampling")
    if surface.kind not in _CORE_SURFACE_KIND:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: kind {surface.kind} has no core surface-kind projection")
    if surface.surface_id not in _CORE_VALUE_TYPE:
        raise UnsupportedCapabilityError(f"{surface.surface_id}: no projected value type")
    kind = _CORE_SURFACE_KIND[surface.kind]
    from r2f.core.mutators import MUTATOR_SPECS

    mutators = tuple(m for m in surface.mutator_ids
                     if m in IMPLEMENTED_MUTATOR_IDS and m in MUTATOR_SPECS
                     and kind in MUTATOR_SPECS[m].accepted_surface_kinds)
    if not mutators:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: no implemented mutator accepts projected kind {kind}")
    if kind == "parameter_enum":
        if "values" not in surface.domain:
            raise UnsupportedCapabilityError(f"{surface.surface_id}: enum kind without values")
        domain = {"values": list(surface.domain["values"])}
    else:
        domain = _numeric_core_domain(surface)
    boundary_classes = list(surface.boundary_classes)
    value_relations = list(surface.value_relations)
    if kind == "parameter_numeric":
        # Core M01 understands numeric endpoints plus declared special classes;
        # M03 handles sampled ranges.  Catalog classes that are sampling modes
        # (``log_range``) are not M01 boundary values and must not be passed to
        # M01.  Keep only classes the implementation can actually honour.
        m01_classes = set(_NUMERIC_BOUNDARY_CLASSES) | _SPECIAL_BOUNDARY_CLASSES | {"quantized_neighbor"}
        m03_classes = {"log_range"}
        annotation_classes = {"short_window_may_be_ineffective"}
        supported = m01_classes | m03_classes
        unsupported = [c for c in boundary_classes
                       if c not in supported and c not in annotation_classes]
        if unsupported:
            raise UnsupportedCapabilityError(
                f"{surface.surface_id}: boundary classes require an unimplemented mutator: "
                + ",".join(unsupported))
        # Annotation classes stay in the catalogue only; core generation uses
        # classes it can actually honour.
        boundary_classes = [c for c in boundary_classes if c not in annotation_classes] or ["min", "max"]
    if surface.runtime_binding is None or surface.runtime_binding not in RUNTIME_BINDINGS:
        raise UnsupportedCapabilityError(
            f"{surface.surface_id}: runtime binding "
            f"{surface.runtime_binding!r} is not present in RUNTIME_BINDINGS")
    return MutationSurface(
        surface_id=surface.surface_id,
        version=surface.version,
        source_refs=list(surface.source_refs),
        surface_kind=kind,
        input_paths=list(surface.input_paths),
        value_type=_CORE_VALUE_TYPE[surface.surface_id],
        unit=surface.unit,
        domain=domain,
        special_values=[{"name": k, "value": v} for k, v in sorted(surface.special_values.items())],
        # ``value_relations`` below carries the catalog prose relations; the
        # pruned-class note is added there so the projection is never silent.
        boundary_classes=boundary_classes,
        mutator_ids=list(mutators),
        value_relations=value_relations,
        tool_requires={name: {"required": True} for name in surface.tool_requires},
        sut_guards={name: "unknown" for name in surface.sut_guards},
        carrier_id=surface.carrier_id,
        runtime_phase=surface.runtime_phase,
        runtime_binding=surface.runtime_binding,
        allowed_with=list(surface.allowed_with),
        conflicts_with=list(surface.conflicts_with),
        order_after=list(surface.order_after),
        fact_ids=list(surface.fact_ids),
        rule_ids=list(surface.rule_ids),
        effect_id=surface.effect_id,
        missing_policy="unknown",
        unsupported_policy="reject",
        activation="F1" if surface.surface_id in F1_SURFACE_IDS else "F2",
    )


def fact_to_core(spec):
    from r2f.core.mutation_plan import FactSpec as CoreFactSpec

    value_type = _CORE_FACT_VALUE_TYPE.get(spec.value_type)
    if value_type is None:
        raise UnsupportedCapabilityError(
            f"{spec.id}: fact value type {spec.value_type} has no core projection")
    if spec.scope not in ("run", "endpoint", "pair", "window", "sample"):
        raise UnsupportedCapabilityError(f"{spec.id}: fact scope {spec.scope} has no core projection")
    return CoreFactSpec(
        id=spec.id, version=spec.version, value_type=value_type, unit=spec.unit,
        scope=spec.scope, sources=list(spec.sources), producer_id=spec.producer_id,
        freshness=spec.freshness, identity_binding=spec.identity_binding,
        missing_policy="unknown",
    )


def rule_to_core(rule, effect_id):
    from r2f.core.mutation_plan import RuleSpec as CoreRuleSpec

    layer = _RULE_RESPONSIBILITY.get(rule.responsibility_layer)
    if layer is None:
        raise UnsupportedCapabilityError(
            f"{rule.id}: responsibility layer {rule.responsibility_layer} has no core projection")
    return CoreRuleSpec(
        id=rule.id, version=rule.version, spec_refs=list(rule.spec_refs),
        applies_when=dict(rule.applies_when), required_fact_ids=list(rule.required_fact_ids),
        evaluator_id=rule.evaluator_id, allowed_effects=[effect_id],
        forbidden_residuals=list(rule.forbidden_residuals),
        limitations="; ".join(rule.limitations) if rule.limitations else "declared in catalog",
        responsibility_layer=layer, primary_or_supporting=rule.primary_or_supporting,
    )


def effect_to_core(effect_id):
    from r2f.core.mutation_plan import EffectSpec as CoreEffectSpec

    spec = EFFECTS[effect_id]
    window = _EFFECT_WINDOW.get(effect_id)
    if window is None:
        raise UnsupportedCapabilityError(f"{effect_id}: no declared effect window")
    return CoreEffectSpec(
        effect_id=effect_id, version=1, window=window,
        allowed_observations=list(spec["allowed_effects"]),
        forbidden_residuals=list(spec["forbidden_residuals"]),
    )


def exploration_contract(surface_ids=(), *, extra_rule_ids=()):
    """Build a validated core contract for the given catalog surface subset.

    Facts, rules and effects are pulled in transitively from the selected
    surfaces.  ``extra_rule_ids`` adds a declared rule (its facts and effect)
    without adding a surface, which is how an F2-only rule such as the
    compatibility guard stays visible while its surface is not generatable.

    Every reference is resolved through this catalog, never a free-form string.
    """
    from r2f.core.mutation_plan import ExplorationContract

    surface_ids = tuple(dict.fromkeys(surface_ids))
    unknown = [sid for sid in surface_ids if sid not in SURFACES]
    if unknown:
        raise CatalogError("unknown surface id(s): " + ",".join(unknown))
    extra_rule_ids = tuple(dict.fromkeys(extra_rule_ids))
    unknown_rules = [r for r in extra_rule_ids if r not in RULES]
    if unknown_rules:
        raise CatalogError("unknown rule id(s): " + ",".join(unknown_rules))

    surfaces = [surface_to_core(SURFACES[sid]) for sid in sorted(surface_ids)]
    selected = {surface.surface_id for surface in surfaces}
    # A sub-contract may only reference surfaces it actually contains.  The
    # catalog keeps the complete composition graph; the projection prunes
    # edges to surfaces outside the requested subset instead of silently
    # emitting dangling references.
    for surface in surfaces:
        # Sort after pruning so the same subset always yields the same
        # contract hash regardless of the caller's input order.
        surface.allowed_with = sorted(s for s in surface.allowed_with if s in selected)
        surface.conflicts_with = sorted(s for s in surface.conflicts_with if s in selected)
        surface.order_after = sorted(s for s in surface.order_after if s in selected)

    fact_ids, rule_ids, effect_ids = [], [], []
    for surface in surfaces:
        fact_ids.extend(surface.fact_ids)
        rule_ids.extend(surface.rule_ids)
        effect_ids.append(surface.effect_id)
    for rule_id in extra_rule_ids:
        rule_ids.append(rule_id)
        effect_ids.append(RULE_EFFECT_ID[rule_id])
    rule_ids = sorted(dict.fromkeys(rule_ids))
    effect_ids = sorted(dict.fromkeys(effect_ids))
    for rule_id in rule_ids:
        fact_ids.extend(RULES[rule_id].required_fact_ids)
    fact_ids = sorted(dict.fromkeys(fact_ids))

    missing_facts = [f for f in fact_ids if f not in FACT_SPECS]
    if missing_facts:
        raise CatalogError("unknown fact id(s): " + ",".join(missing_facts))
    missing_effects = [e for e in effect_ids if e not in EFFECTS]
    if missing_effects:
        raise CatalogError("unknown effect id(s): " + ",".join(missing_effects))

    contract = ExplorationContract(
        surfaces=surfaces,
        facts=[fact_to_core(FACT_SPECS[f]) for f in fact_ids],
        rules=[rule_to_core(RULES[r], RULE_EFFECT_ID[r]) for r in rule_ids],
        effects=[effect_to_core(e) for e in effect_ids],
    )
    contract.validate()
    return contract


def capability_matrix():
    """Static capability view: routable F1 surfaces versus pending F2 ones."""
    return {
        "contract_version": CATALOG_VERSION,
        "f1_routable_surface_ids": list(F1_SURFACE_IDS),
        "f2_pending_surface_ids": list(F2_SURFACE_IDS),
        "surface_bindings": {
            sid: {
                "runtime_binding": SURFACES[sid].runtime_binding,
                "binding_known": SURFACES[sid].runtime_binding in RUNTIME_BINDINGS,
                "implemented_mutators": list(core_mutator_ids(SURFACES[sid])),
                "activation": "F1" if sid in F1_SURFACE_IDS else "F2",
                "status": ("f1_routable" if sid in F1_SURFACE_IDS
                           else "capability_pending_f2"),
            } for sid in SURFACES
        },
        "declared_only_mutator_ids": sorted(MUTATOR_IDS - IMPLEMENTED_MUTATOR_IDS),
    }
