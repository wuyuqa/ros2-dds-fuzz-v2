"""Offline contracts for fuzz surfaces, facts, rules and exploration targets.

Fixtures describe inputs and applicability only.  They are not ROS runs, DDS
coverage, mutation hits, or oracle evidence.
"""
import copy
import json
import unittest

from r2f.core import SemanticTarget, canonical_hash
from r2f.core.mutation_plan import (
    EFFECT_SPEC_SCHEMA, EXPLORATION_CONTRACT_SCHEMA, FACT_SPEC_SCHEMA, MUTATION_SURFACE_SCHEMA,
    RULE_SPEC_SCHEMA, EffectSpec, ExplorationContract, FactSpec, MutationSurface, RuleSpec,
)
from r2f.modules.s1.targets import EXECUTION_ORDER, target_declarations

GOLDEN_TARGET_HASHES = (
    "c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940",
    "1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d",
    "fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce",
    "eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0",
)


def fact(fid, scope="run", value_type="bool", unit="boolean", sources=None, missing="unknown"):
    return {"schema": FACT_SPEC_SCHEMA, "id": fid, "version": 1, "value_type": value_type,
            "unit": unit, "scope": scope, "sources": sources or [fid + "_source"],
            "producer_id": "s1.observer", "freshness": "window", "identity_binding": "pair",
            "missing_policy": missing}


def rule(rid, facts, effect, applies="declared window"):
    return {"schema": RULE_SPEC_SCHEMA, "id": rid, "version": 1, "spec_refs": ["DDS"],
            "applies_when": {"window": applies}, "required_fact_ids": list(facts),
            "evaluator_id": rid + ".evaluate", "allowed_effects": [effect],
            "forbidden_residuals": [effect + ".forbidden"], "limitations": "fixture only",
            "responsibility_layer": "s1", "primary_or_supporting": "primary"}


def effect(eid, window="window"):
    return {"schema": EFFECT_SPEC_SCHEMA, "effect_id": eid, "version": 1, "window": window,
            "allowed_observations": [eid + ".allowed"],
            "forbidden_residuals": [eid + ".forbidden"]}


def surface(sid, kind, paths, value_type, unit, domain, boundaries, mutators,
            fact_ids, rule_ids, effect_id, binding="ros2_endpoint", conflicts=(), order=()):
    return {"schema": MUTATION_SURFACE_SCHEMA, "surface_id": sid, "version": 1,
            "source_refs": ["fixture:" + sid], "surface_kind": kind,
            "input_paths": list(paths), "value_type": value_type, "unit": unit,
            "domain": domain, "special_values": [], "boundary_classes": boundaries,
            "mutator_ids": list(mutators), "value_relations": [], "tool_requires": {},
            "sut_guards": {}, "carrier_id": "ros2_endpoint", "runtime_phase": "endpoint_creation",
            "runtime_binding": binding, "allowed_with": [], "conflicts_with": list(conflicts),
            "order_after": list(order), "fact_ids": list(fact_ids), "rule_ids": list(rule_ids),
            "effect_id": effect_id, "missing_policy": "unknown",
            "unsupported_policy": "reject", "activation": "F1"}


def fixture_contract():
    facts = [fact("writer_prejoin"), fact("reader_create_order"),
             fact("endpoint_submitted"), fact("endpoint_observed")]
    rules = [
        rule("s1.late_join", ["writer_prejoin", "reader_create_order"], "eff.late_join"),
        rule("s1.projection", ["endpoint_submitted", "endpoint_observed"], "eff.projection"),
    ]
    effects = [effect("eff.late_join", "before_reader_match"),
               effect("eff.projection", "endpoint_creation")]
    surfaces = [
        surface("P01", "parameter_timing", ["reader_delay_sec"], "duration_sec", "s",
                {"min": 0.0, "max": 3.0, "step": 0.001}, ["min", "max"],
                ["M03"], ["writer_prejoin", "reader_create_order"], ["s1.late_join"],
                "eff.late_join", conflicts=["P02"]),
        surface("P02", "parameter_enum", ["qos.reliability"], "enum", "string",
                {"values": ["reliable", "best_effort"]}, [], ["M02"],
                ["endpoint_submitted"], ["s1.projection"], "eff.projection", conflicts=["P01"]),
        surface("P04", "parameter_numeric", ["qos.deadline_ns"], "duration_ns", "ns",
                {"min": 0, "max": 1000000001, "step": 1},
                ["min", "min_plus_one", "max_minus_one", "max"],
                ["M01", "M03"], ["endpoint_observed"], ["s1.projection"], "eff.projection"),
        # Two surfaces writing the same field: used for conflict/override tests.
        surface("X01", "parameter_numeric", ["qos.deadline_ns"], "duration_ns", "ns",
                {"min": 0, "max": 100, "step": 1}, ["min", "max"], ["M01"],
                ["endpoint_observed"], ["s1.projection"], "eff.projection", order=["P04"]),
        surface("X02", "parameter_numeric", ["qos.deadline_ns"], "duration_ns", "ns",
                {"min": 0, "max": 100, "step": 1}, ["min", "max"], ["M01"],
                ["endpoint_observed"], ["s1.projection"], "eff.projection", order=["P04"]),
    ]
    return ExplorationContract(surfaces=surfaces, facts=facts, rules=rules, effects=effects)


PARENT = {
    "reader_delay_sec": 0.15,
    "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64,
            "deadline_ns": 0},
    "pre_match_count": 2, "post_match_count": 6,
}


class FuzzModelContracts(unittest.TestCase):
    def test_v01_original_target_serialization_is_unchanged(self):
        targets = target_declarations()
        self.assertEqual(tuple(targets), EXECUTION_ORDER)
        hashes = tuple(canonical_hash(targets[target_id].to_dict()) for target_id in EXECUTION_ORDER)
        self.assertEqual(hashes, GOLDEN_TARGET_HASHES)
        for target in targets.values():
            data = target.to_dict()
            self.assertNotIn("exploration_contract", data)
            self.assertEqual(SemanticTarget.from_dict(data).to_dict(), data)

    def test_empty_exploration_contract_is_not_serialized(self):
        target = target_declarations()[EXECUTION_ORDER[0]]
        self.assertIsNone(target.exploration_contract)
        self.assertNotIn("exploration_contract", json.loads(json.dumps(target.to_dict())))

    def test_contract_has_required_fields_and_roundtrips(self):
        contract = fixture_contract()
        required = {
            "surface_id", "version", "source_refs", "input_paths", "value_type", "unit", "domain",
            "special_values", "boundary_classes", "mutator_ids", "value_relations", "tool_requires",
            "sut_guards", "carrier_id", "runtime_phase", "runtime_binding", "allowed_with",
            "conflicts_with", "order_after", "fact_ids", "rule_ids", "effect_id",
            "missing_policy", "unsupported_policy",
        }
        for item in contract.surfaces:
            self.assertTrue(required.issubset(item.to_dict()))
        for item in contract.facts:
            self.assertTrue({"id", "version", "value_type", "unit", "scope", "sources",
                             "producer_id", "freshness", "identity_binding", "missing_policy"}.issubset(item.to_dict()))
        for item in contract.rules:
            self.assertTrue({"id", "version", "spec_refs", "applies_when", "required_fact_ids",
                             "evaluator_id", "allowed_effects", "forbidden_residuals", "limitations",
                             "responsibility_layer", "primary_or_supporting"}.issubset(item.to_dict()))
        self.assertEqual(contract.to_dict()["schema"], EXPLORATION_CONTRACT_SCHEMA)
        self.assertEqual(ExplorationContract.from_dict(contract.to_dict()).to_dict(), contract.to_dict())

    def test_v02_unknown_references_are_rejected(self):
        data = fixture_contract().to_dict()
        broken = copy.deepcopy(data)
        broken["surfaces"][0]["mutator_ids"] = ["M99"]
        with self.assertRaisesRegex(ValueError, "unknown mutator reference"):
            ExplorationContract.from_dict(broken)
        broken = copy.deepcopy(data)
        broken["surfaces"][0]["fact_ids"] = ["missing_fact"]
        with self.assertRaisesRegex(ValueError, "unknown fact reference"):
            ExplorationContract.from_dict(broken)
        broken = copy.deepcopy(data)
        broken["surfaces"][0]["rule_ids"] = ["missing_rule"]
        with self.assertRaisesRegex(ValueError, "unknown rule reference"):
            ExplorationContract.from_dict(broken)
        broken = copy.deepcopy(data)
        broken["rules"][0]["required_fact_ids"] = ["missing_fact"]
        with self.assertRaisesRegex(ValueError, "unknown rule fact reference"):
            ExplorationContract.from_dict(broken)
        broken = copy.deepcopy(data)
        broken["surfaces"][0]["order_after"] = ["missing_surface"]
        with self.assertRaisesRegex(ValueError, "unknown order_after reference"):
            ExplorationContract.from_dict(broken)

    def test_duplicate_ids_and_cycles_are_rejected(self):
        data = fixture_contract().to_dict()
        data["surfaces"].append(copy.deepcopy(data["surfaces"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate surface id"):
            ExplorationContract.from_dict(data)
        data = fixture_contract().to_dict()
        for item in data["surfaces"]:
            if item["surface_id"] == "P04":
                item["order_after"] = ["X01"]
            if item["surface_id"] == "X01":
                item["order_after"] = ["P04"]
        with self.assertRaisesRegex(ValueError, "cyclic order dependency"):
            ExplorationContract.from_dict(data)

    def test_conflict_relations_must_be_symmetric(self):
        data = fixture_contract().to_dict()
        for item in data["surfaces"]:
            if item["surface_id"] == "P02":
                item["conflicts_with"] = []
        with self.assertRaisesRegex(ValueError, "conflict relation must be symmetric"):
            ExplorationContract.from_dict(data)

    def test_strict_unknown_fields_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown field"):
            ExplorationContract.from_dict(dict(fixture_contract().to_dict(),
                                               surfaces=[dict(fixture_contract().to_dict()["surfaces"][0],
                                                              mystery=True)]))
        with self.assertRaisesRegex(ValueError, "unknown field"):
            FactSpec.from_dict(dict(fact("x"), extra=1))
        with self.assertRaisesRegex(ValueError, "unknown field"):
            RuleSpec.from_dict(dict(rule("r", ["f"], "e"), extra=1))
        with self.assertRaisesRegex(ValueError, "unknown field"):
            EffectSpec.from_dict(dict(effect("e"), extra=1))

    def test_nonfinite_domain_is_rejected(self):
        data = fixture_contract().to_dict()
        data["surfaces"][0]["domain"]["max"] = float("inf")
        with self.assertRaises(ValueError):
            ExplorationContract.from_dict(data)

    def test_sut_guards_are_three_valued(self):
        data = fixture_contract().to_dict()["surfaces"][0]
        data["sut_guards"] = {"precondition": "maybe"}
        with self.assertRaisesRegex(ValueError, "sut_guards"):
            MutationSurface.from_dict(data)
        for value in (True, False, "unknown"):
            data["sut_guards"] = {"precondition": value}
            MutationSurface.from_dict(data)

    def test_exploration_contract_enters_target_hash_and_roundtrips(self):
        base = target_declarations()[EXECUTION_ORDER[0]]
        data = base.to_dict()
        data["exploration_contract"] = fixture_contract().to_dict()
        extended = SemanticTarget.from_dict(data)
        self.assertNotEqual(canonical_hash(extended.to_dict()), canonical_hash(base.to_dict()))
        self.assertEqual(SemanticTarget.from_dict(extended.to_dict()).to_dict(), extended.to_dict())
        self.assertIn("exploration_contract", extended.to_dict())

    def test_bad_exploration_contract_on_target_is_rejected(self):
        data = target_declarations()[EXECUTION_ORDER[0]].to_dict()
        data["exploration_contract"] = {"schema": "r2f.exploration-contract/v1"}
        with self.assertRaises(ValueError):
            SemanticTarget.from_dict(data)


if __name__ == "__main__":
    unittest.main()
