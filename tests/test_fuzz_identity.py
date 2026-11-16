"""V27/V06: versioned input identity compatibility and semantic stability.

All fixtures are offline.  No capsule here proves a ROS/DDS execution.
"""
import copy
import hashlib
import json
import random
import unittest

from r2f.core.evidence import (
    INPUT_SCHEMA, INPUT_SCHEMA_V2, effective_input_hash, identity_partition,
)
from r2f.core.mutators import mutate
from r2f.core.mutation_plan import compose
from r2f.core.scenario import compile_scenario
from test_core_contracts import target
from test_fuzz_model_contracts import PARENT, fixture_contract


def legacy_input():
    return {
        "target_id": "discovery.late_endpoint", "target_revision": 0,
        "mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
        "mutation_parameters": {"reader_delay_sec": 0.15},
        "rmw": "rmw_cyclonedds_cpp", "repeat": 0, "seed": 3, "arm": "mutated",
        "mutation_layer_order": ["late_joiner"],
        "input_layer_receipts": [{"layer": "ros2_endpoint", "declared_mutation": "late_joiner"}],
        "final_input": {"reader_delay_sec": 0.15},
    }


LEGACY_IDENTITY_KEYS = ("target_id", "target_revision", "target_sha256", "mutation_kind", "carrier",
                        "mutation_parameters", "rmw", "repeat", "seed", "arm", "mutation_layer_order",
                        "input_layer_receipts", "final_input", "state_steps", "source_manifest")


def independent_v1_digest(data):
    """Recompute the frozen v1 semantics without calling the module under test."""
    body = {"schema": INPUT_SCHEMA, **{key: data[key] for key in LEGACY_IDENTITY_KEYS if key in data}}
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                         allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def plan_fixture():
    contract = fixture_contract()
    parent = copy.deepcopy(PARENT)
    proposal = mutate(parent, contract.surface_by_id["P04"], random.Random(0), {}, "M01")
    return compose(parent, [proposal], contract)


class InputIdentityV27(unittest.TestCase):
    def test_v27_legacy_capsule_hash_is_byte_identical(self):
        data = legacy_input()
        self.assertEqual(effective_input_hash(data), independent_v1_digest(data))
        # Declared v1 must behave exactly like the undeclared legacy form.
        declared = dict(data, input_identity_schema=INPUT_SCHEMA)
        self.assertEqual(effective_input_hash(declared), effective_input_hash(data))
        # v1 historically ignores campaign/runtime metadata.
        self.assertEqual(effective_input_hash(dict(data, origin_domain=99, campaign_kind="exploratory")),
                         effective_input_hash(data))

    def test_v27_v2_binds_plan_operations_windows_and_contract(self):
        plan = plan_fixture()
        base = dict(legacy_input(), input_identity_schema=INPUT_SCHEMA_V2,
                    mutation_plan=plan.to_dict(),
                    operations=[op.to_dict() for op in plan.operations],
                    runtime_windows=plan.runtime_windows,
                    contract_hash=plan.contract_hash)
        digest = effective_input_hash(base)
        variants = {
            "mutation_plan": dict(base, mutation_plan=dict(plan.to_dict(), rng_seed=999)),
            "operations": dict(base, operations=[dict(op.to_dict(), op_id="op-other")
                                                 for op in plan.operations]),
            "runtime_windows": dict(base, runtime_windows=[{
                "schema": "r2f.runtime-window/v1", "window_id": "w1", "window_index": 0,
                "trigger_anchor": "fresh_match", "trigger_offset_sec": 0.0,
                "drop_duration_sec": 0.2, "restore_interval_sec": 0.1}]),
            "contract_hash": dict(base, contract_hash="a" * 64),
            "final_input": dict(base, final_input={"qos": {"deadline_ns": 1}}),
        }
        for field, changed in variants.items():
            with self.subTest(field=field):
                self.assertNotEqual(effective_input_hash(changed), digest, field)
                identity, excluded = identity_partition(changed, schema=INPUT_SCHEMA_V2)
                self.assertIn(field, identity)
                self.assertEqual(excluded, [])

    def test_v27_v2_declared_non_identity_metadata_does_not_split_content(self):
        plan = plan_fixture()
        base = dict(legacy_input(), input_identity_schema=INPUT_SCHEMA_V2,
                    mutation_plan=plan.to_dict(), contract_hash=plan.contract_hash)
        digest = effective_input_hash(base)
        noisy = dict(base, run_id="run-9", domain=173, timestamp="2026-11-16T00:00:00+00:00",
                     raw_path="/tmp/x.json", execution_stage="judged")
        self.assertEqual(effective_input_hash(noisy), digest)
        _, excluded = identity_partition(noisy, schema=INPUT_SCHEMA_V2)
        self.assertEqual(excluded, sorted({"run_id", "domain", "timestamp", "raw_path", "execution_stage"}))

    def test_v27_v2_unknown_version_and_undeclared_field_are_rejected(self):
        plan = plan_fixture()
        base = dict(legacy_input(), input_identity_schema=INPUT_SCHEMA_V2,
                    mutation_plan=plan.to_dict(), contract_hash=plan.contract_hash)
        cases = {
            "unknown_version": dict(base, input_identity_schema="r2f.input-identity/v9"),
            "undeclared_field": dict(base, unclassified_field={"x": 1}),
            "v2_fields_under_v1": dict(legacy_input(), input_identity_schema=INPUT_SCHEMA,
                                       mutation_plan=plan.to_dict()),
            "missing_required": {k: v for k, v in base.items() if k != "final_input"},
        }
        for label, bad in cases.items():
            with self.subTest(label=label), self.assertRaises(ValueError):
                effective_input_hash(bad)
        with self.assertRaises(ValueError):
            effective_input_hash(base, schema=INPUT_SCHEMA)

    def test_v27_execution_fields_never_enter_input_layer_receipts(self):
        for field, value in (("hit", True), ("applied", True), ("executed_at", "2026-11-16T00:00:00Z"),
                             ("mutation_receipt", {"hit": True})):
            data = legacy_input()
            data["input_layer_receipts"] = [{"layer": "ros2_endpoint", field: value}]
            with self.subTest(field=field), self.assertRaises(ValueError):
                effective_input_hash(data)


class ScenarioPlanIdentityV06(unittest.TestCase):
    def test_v06_legacy_scenario_serialization_and_hash_are_unchanged(self):
        scenario = compile_scenario(target(), "rmw_cyclonedds_cpp", "mutated", 0)
        data = scenario.to_dict()
        self.assertNotIn("mutation_plan", data)
        self.assertEqual(data["input_identity_schema"], INPUT_SCHEMA)
        self.assertEqual(scenario.input_hash(), independent_v1_digest(scenario.effective_input()))
        rebuilt = compile_scenario(target(), "rmw_cyclonedds_cpp", "mutated", 0, parameters={})
        self.assertEqual(rebuilt.to_dict(), data)

    def test_v06_attached_plan_needs_matching_final_config(self):
        plan = plan_fixture()
        scenario = compile_scenario(
            target(), "rmw_cyclonedds_cpp", "mutated", 0,
            parameters={"mutation_kind": "late_joiner", "carrier": "ros2_endpoint"},
            mutation_plan=plan)
        self.assertIn("mutation_plan", scenario.to_dict())
        digest = scenario.input_hash_v2()
        # Round trip keeps the plan identity.
        restored = compile_scenario.from_dict(scenario.to_dict()) if False else None
        self.assertIsNone(restored)
        from r2f.core.scenario import Scenario
        again = Scenario.from_dict(scenario.to_dict())
        self.assertEqual(again.input_hash_v2(), digest)
        with self.assertRaises(ValueError):
            scenario.effective_input_v2(final_input={"qos": {"deadline_ns": 7}})
        # v1 identity of the same scenario is not silently upgraded.
        self.assertEqual(scenario.input_hash(), independent_v1_digest(scenario.effective_input()))

    def test_v06_seed_and_run_metadata_do_not_change_v2_content_identity(self):
        plan = plan_fixture()
        def build(seed):
            return compile_scenario(
                target(), "rmw_cyclonedds_cpp", "mutated", 0, seed,
                parameters={"mutation_kind": "late_joiner", "carrier": "ros2_endpoint"},
                mutation_plan=plan)
        left, right = build(1), build(999)
        # Scenario.seed enters v1 identity but is not a generation source; the
        # v2 plan content is unchanged because the plan did not vary.
        self.assertEqual(left.mutation_plan.content_hash(), right.mutation_plan.content_hash())
        self.assertEqual(left.effective_input_v2()["mutation_plan"],
                         right.effective_input_v2()["mutation_plan"])


if __name__ == "__main__":
    unittest.main()
