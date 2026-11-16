"""Exploratory carrier identity: v1 required path unchanged, v2 plan path bound.

These are offline contract tests with a fake runtime.  They start no ROS
process, write no campaign output and produce no execution evidence, oracle
verdict, candidate or seal.
"""
import copy
import unittest
from unittest.mock import Mock

from r2f.core.evidence import (INPUT_SCHEMA, INPUT_SCHEMA_V2, canonical_hash,
                               effective_input_hash)
from r2f.core.mutation_plan import compose
from r2f.core.mutators import make_rng, mutate
from r2f.core.scenario import Scenario, compile_scenario
from r2f.modules.s1.module import register
from r2f.modules.s1.providers import (ExploratoryInputError,
                                      S1ExploratoryMutationProvider,
                                      exploratory_service_bindings,
                                      service_bindings)
from r2f.modules.s1.targets import (EXECUTION_ORDER,
                                    exploratory_target_declarations,
                                    target_declarations)

SOURCE = {"source_repo": "/tmp/repo", "head": "a" * 40,
          "behavior_sha256": "b" * 64, "source_frozen": True}

LATE_BASE = {
    "topic": "/r2f/s1/late_endpoint",
    "message_type": "std_msgs/msg/String",
    "qos": {"reliability": "reliable", "durability": "volatile", "depth": 64},
    "reader_delay_sec": 0.15,
    "baseline_reader_delay_sec": 0.02,
    "pre_match_count": 2,
    "post_match_count": 6,
    "publish_interval_sec": 0.03,
    "match_timeout_sec": 8.0,
    "delivery_timeout_sec": 5.0,
    "startup_timeout_sec": 10.0,
}

TARGET_ID = "discovery.late_endpoint.fuzz_v1"


def _required_scenario():
    targets = target_declarations()
    target = targets[EXECUTION_ORDER[0]]
    return compile_scenario(target, "rmw_cyclonedds_cpp", "mutated", 0, 0,
                            {"mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
                             "mutation_parameters": {"reader_delay_sec": 0.15}})


def _v2_case():
    targets = exploratory_target_declarations()
    provider = S1ExploratoryMutationProvider(targets)
    contract = provider.contract_for(TARGET_ID)
    surface = contract.surface_by_id["P01"]
    proposal = mutate(LATE_BASE, surface, make_rng(7), mutator_id="M03")
    plan = compose(LATE_BASE, [proposal], contract, rng_seed=7)
    scenario = Scenario(targets[TARGET_ID], "rmw_cyclonedds_cpp", "mutated", 0, 7,
                        {"mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
                         "mutation_parameters": plan.final_config}, plan)
    layer = provider.prepare_plan(scenario, plan)
    prepared = scenario.effective_input_v2(
        final_input=plan.final_config,
        input_layer_receipts=layer["input_layer_receipts"],
        source_manifest=SOURCE)
    digest = effective_input_hash(prepared, schema=INPUT_SCHEMA_V2)
    config = {
        "target_id": TARGET_ID, "target_revision": 0,
        "rmw": "rmw_cyclonedds_cpp", "arm": "mutated", "repeat": 0, "seed": 7,
        "mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
        "campaign_kind": "exploratory", "domain_id": 173,
        "input_identity_schema": INPUT_SCHEMA_V2,
        "effective_input_sha256": digest,
        "effective_input": copy.deepcopy(prepared),
        "final_input": copy.deepcopy(prepared["final_input"]),
        "input_layer_receipts": copy.deepcopy(prepared["input_layer_receipts"]),
        "source_manifest": copy.deepcopy(SOURCE),
        "scenario": scenario.to_dict(),
        "mutation_plan": plan.to_dict(),
        "mutation_plan_sha256": canonical_hash(plan.to_dict()),
    }
    config.update(copy.deepcopy(prepared["final_input"]))
    carrier = exploratory_service_bindings(targets)[TARGET_ID].carrier
    return scenario, plan, prepared, config, digest, carrier


class RequiredCarrierV1Unchanged(unittest.TestCase):
    def setUp(self):
        self.scenario = _required_scenario()
        services = service_bindings({self.scenario.target_id: self.scenario.target})[
            self.scenario.target_id]
        self.provider = services.mutation_provider
        self.carrier = services.carrier
        layer = self.provider.prepare(self.scenario)
        # Reconstruct the exact v1 capsule payload; provider.prepare returns
        # only the layer description and is not itself an effective input.
        self.prepared = self.scenario.effective_input(
            final_input=LATE_BASE,
            input_layer_receipts=layer["input_layer_receipts"],
            source_manifest=SOURCE)
        self.digest = effective_input_hash(self.prepared)
        self.config = {
            "target_id": self.scenario.target_id,
            "target_revision": self.scenario.target_revision,
            "rmw": self.scenario.rmw, "arm": self.scenario.arm,
            "repeat": self.scenario.repeat, "seed": self.scenario.seed,
            "mutation_kind": self.scenario.mutation_kind,
            "carrier": self.scenario.carrier,
            "effective_input_sha256": self.digest,
            "final_input": copy.deepcopy(self.prepared["final_input"]),
            "input_layer_receipts": copy.deepcopy(self.prepared["input_layer_receipts"]),
            "source_manifest": copy.deepcopy(SOURCE),
            "scenario": self.scenario.to_dict(),
        }
        self.config.update(copy.deepcopy(self.prepared["final_input"]))

    def test_v1_requires_no_schema_field_and_keeps_its_digest(self):
        self.assertNotIn("input_identity_schema", self.prepared)
        self.assertNotIn("input_identity_schema", self.config)
        marker = {"runtime_status": "fake"}
        runtime = Mock(return_value=marker)
        result = self.carrier.apply(self.scenario, self.prepared,
                                    execution={"config": self.config, "runtime": runtime,
                                               "output_dir": "/tmp/never"})
        self.assertIs(result, marker)
        passed = runtime.call_args[0][0]
        self.assertEqual(passed["effective_input_sha256"], self.digest)

    def test_v1_still_rejects_tampered_digest_and_payload(self):
        runtime = Mock(side_effect=AssertionError("must not launch"))
        for mutate_config in (lambda c: c.update(effective_input_sha256="0" * 64),
                              lambda c: c.update(topic="/tampered"),
                              lambda c: c.update(final_input=[])):
            config = copy.deepcopy(self.config)
            mutate_config(config)
            with self.assertRaises(ValueError):
                self.carrier.apply(self.scenario, self.prepared,
                                   execution={"config": config, "runtime": runtime,
                                              "output_dir": "/tmp/never"})
        runtime.assert_not_called()

    def test_v1_prepared_cannot_be_pair_with_v2_config(self):
        config = copy.deepcopy(self.config)
        config["input_identity_schema"] = INPUT_SCHEMA_V2
        with self.assertRaises(ValueError) as caught:
            self.carrier.apply(self.scenario, self.prepared,
                               execution={"config": config,
                                          "runtime": Mock(side_effect=AssertionError("no")),
                                          "output_dir": "/tmp/never"})
        self.assertIn("v1", str(caught.exception))


class ExploratoryCarrierV2(unittest.TestCase):
    def test_prepared_v2_identity_is_explicit(self):
        _, plan, prepared, _, digest, _ = _v2_case()
        self.assertEqual(prepared["input_identity_schema"], INPUT_SCHEMA_V2)
        self.assertEqual(prepared["mutation_plan"]["rng_seed"], plan.rng_seed)
        self.assertEqual(prepared["contract_hash"], plan.contract_hash)
        self.assertRegex(digest, r"^[0-9a-f]{64}$")
        # The digest is the v2 identity, not a v1 summary of the same payload.
        self.assertNotEqual(digest, effective_input_hash(self.scenario_v1_view(prepared)))

    @staticmethod
    def scenario_v1_view(prepared):
        return {key: value for key, value in prepared.items()
                if key not in {"input_identity_schema", "mutation_plan", "operations",
                               "runtime_windows", "contract", "contract_hash"}}

    def test_carrier_executes_with_the_v2_digest_and_plan_final_config(self):
        scenario, plan, prepared, config, digest, carrier = _v2_case()
        marker = {"runtime_status": "fake"}
        runtime = Mock(return_value=marker)
        result = carrier.apply(scenario, prepared,
                               execution={"config": config, "runtime": runtime,
                                          "output_dir": "/tmp/never"})
        self.assertIs(result, marker)
        runtime.assert_called_once()
        passed = runtime.call_args[0][0]
        self.assertEqual(passed["effective_input_sha256"], digest)
        self.assertEqual(passed["input_identity_schema"], INPUT_SCHEMA_V2)
        self.assertEqual(canonical_hash(passed["final_input"]),
                         canonical_hash(plan.final_config))
        self.assertEqual(canonical_hash(passed["mutation_plan"]), canonical_hash(plan.to_dict()))
        # The carrier must not fall back to the v1 digest anywhere.
        self.assertNotEqual(digest, effective_input_hash({
            "target_id": scenario.target_id, "target_revision": scenario.target_revision,
            "mutation_kind": scenario.mutation_kind, "carrier": scenario.carrier,
            "mutation_parameters": prepared["mutation_parameters"], "rmw": scenario.rmw,
            "repeat": scenario.repeat, "seed": scenario.seed, "arm": scenario.arm,
            "mutation_layer_order": [scenario.mutation_kind],
            "input_layer_receipts": prepared["input_layer_receipts"],
            "final_input": prepared["final_input"],
            "state_steps": [s.to_dict() for s in scenario.state_steps],
            "source_manifest": SOURCE}))

    def test_plan_operations_windows_and_contract_tampering_is_refused(self):
        scenario, plan, prepared, config, _, carrier = _v2_case()
        runtime = Mock(side_effect=AssertionError("tampered input must not launch"))
        tamper = [
            lambda p: p["mutation_plan"].update(rng_seed=8),
            lambda p: p["operations"][0].update(mutator_id="M01"),
            lambda p: p["runtime_windows"].append({"window": 2}),
            lambda p: p.update(contract_hash="0" * 64),
        ]
        for edit in tamper:
            bad = copy.deepcopy(prepared)
            edit(bad)
            with self.assertRaises(ValueError):
                carrier.apply(scenario, bad,
                              execution={"config": config, "runtime": runtime,
                                         "output_dir": "/tmp/never"})
        runtime.assert_not_called()

    def test_final_input_and_source_tampering_is_refused(self):
        scenario, _, prepared, config, _, carrier = _v2_case()
        runtime = Mock(side_effect=AssertionError("tampered input must not launch"))
        bad_final = copy.deepcopy(prepared)
        bad_final["final_input"]["reader_delay_sec"] = 0.5
        bad_source = copy.deepcopy(prepared)
        bad_source["source_manifest"] = {"head": "c" * 40}
        missing_schema = copy.deepcopy(prepared)
        del missing_schema["input_identity_schema"]
        for bad in (bad_final, bad_source, missing_schema):
            with self.assertRaises(ValueError):
                carrier.apply(scenario, bad,
                              execution={"config": config, "runtime": runtime,
                                         "output_dir": "/tmp/never"})
        runtime.assert_not_called()

    def test_config_digest_plan_and_effective_input_tampering_is_refused(self):
        scenario, plan, prepared, config, _, carrier = _v2_case()
        runtime = Mock(side_effect=AssertionError("tampered config must not launch"))
        cases = []
        bad = copy.deepcopy(config); bad["effective_input_sha256"] = "0" * 64
        cases.append(bad)
        bad = copy.deepcopy(config); bad["input_identity_schema"] = INPUT_SCHEMA
        cases.append(bad)
        bad = copy.deepcopy(config); bad["mutation_plan"]["rng_seed"] = 8
        cases.append(bad)
        bad = copy.deepcopy(config); bad["effective_input"]["final_input"]["reader_delay_sec"] = 0.9
        cases.append(bad)
        bad = copy.deepcopy(config); bad["mutation_plan_sha256"] = "0" * 64
        cases.append(bad)
        bad = copy.deepcopy(config); bad["final_input"]["reader_delay_sec"] = 0.9
        cases.append(bad)
        for bad in cases:
            with self.assertRaises(ValueError):
                carrier.apply(scenario, prepared,
                              execution={"config": bad, "runtime": runtime,
                                         "output_dir": "/tmp/never"})
        runtime.assert_not_called()
        self.assertTrue(prepared["input_identity_schema"] == INPUT_SCHEMA_V2)

    def test_exploratory_carrier_refuses_planless_and_non_exploratory_targets(self):
        targets = exploratory_target_declarations()
        carrier = exploratory_service_bindings(targets)[TARGET_ID].carrier
        planless = Scenario(targets[TARGET_ID], "rmw_cyclonedds_cpp", "mutated", 0, 7,
                            {"mutation_kind": "late_joiner", "carrier": "ros2_endpoint"})
        with self.assertRaises(ExploratoryInputError):
            carrier.apply(planless, {}, execution={"config": {}, "runtime": Mock(),
                                                   "output_dir": "/tmp/never"})
        scenario, _, prepared, config, _, _ = _v2_case()
        scenario.target.target_id = "discovery.late_endpoint"
        with self.assertRaises(ExploratoryInputError):
            carrier.apply(scenario, prepared, execution={"config": config,
                                                         "runtime": Mock(),
                                                         "output_dir": "/tmp/never"})

    def test_v2_payload_cannot_be_replayed_as_v1(self):
        scenario, _, prepared, config, _, carrier = _v2_case()
        downgraded = copy.deepcopy(config)
        downgraded.pop("input_identity_schema", None)
        with self.assertRaises(ValueError):
            carrier.apply(scenario, prepared,
                          execution={"config": downgraded,
                                     "runtime": Mock(side_effect=AssertionError("no")),
                                     "output_dir": "/tmp/never"})


if __name__ == "__main__":
    unittest.main()
