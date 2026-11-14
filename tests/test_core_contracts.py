import copy
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace

from r2f.core import (
    SemanticTarget, StateCondition, StateStep, StateExecutor, MutationSlot,
    compile_scenario, MutationReceipt, canonical_hash, effective_input_hash,
    raw_sha256, verify_raw, ExecutionStage, ExecutionRecord, Registry,
)


def target():
    return SemanticTarget(
        target_id="discovery.late_endpoint", intent_and_scope="Public endpoint conformance",
        state_preconditions=[StateCondition("writer_ready", "writer_ready")],
        state_steps=[StateStep("create_writer", "create_writer")],
        mutation_slots=[MutationSlot("late_joiner", "ros2_endpoint")],
        observation_contract={"sources": ["endpoint_tokens"]},
        expected_effect_set={"mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
                             "allowed_observations": ["reader_created_after_writer"],
                             "forbidden_residuals": ["post_match_delivery_loss"]},
        rule_contract={"primary_rule": "discovery.late_endpoint.match_and_receive_after_join"},
        evidence_policy={"raw_required": True},
        control_contract={"fault": {
            "arm": "negative", "fault_id": "late_endpoint_delivery_omission",
            "expected": "primary_fail",
            "omission": "one intermediate postmatch sequence followed by delivered sentinel",
            "candidate_eligible": False,
        }},
    )


def scenario():
    return compile_scenario(target(), "rmw_cyclonedds_cpp", "mutated", 0)


class CoreContracts(unittest.TestCase):
    def test_target_and_scenario_are_json(self):
        s = scenario()
        self.assertEqual(json.loads(json.dumps(s.to_dict()))["target_id"], target().target_id)
        self.assertEqual(s.execution_stage, ExecutionStage.PLANNED)
        self.assertEqual(s.to_dict()["state_steps"][0]["step_id"], "create_writer")

    def test_bad_target_and_parameters(self):
        for kwargs in ({"target_revision": 2}, {"target_revision": True}, {"target_id": ""}):
            data = target().to_dict()
            data.update(kwargs)
            with self.assertRaises(ValueError):
                SemanticTarget.from_dict(data)
        for arm, repeat in (("other", 0), ("baseline", -1), ("baseline", True)):
            with self.assertRaises(ValueError):
                compile_scenario(target(), "rmw_cyclonedds_cpp", arm, repeat)

    def test_registry_is_generic_and_rejects_duplicate(self):
        r = Registry()
        module = SimpleNamespace(id="s1", targets={target().target_id: target()})
        r.register(module)
        self.assertEqual(r.get_target("discovery.late_endpoint").target_revision, 0)
        with self.assertRaises(ValueError):
            r.register(module)
        with self.assertRaises(ValueError):
            r.register(SimpleNamespace(id="s2", targets={"bad": target()}))
        self.assertEqual(list(r.modules), ["s1"])

    def test_receipt_is_strict_nested_boolean_and_ordered(self):
        valid = MutationReceipt(True, True, True, True, True, True, True)
        valid.validate()
        self.assertEqual(MutationReceipt.from_record({"mutation_receipt": valid.to_dict()}), valid)
        for record in ({"hit": True}, {"mutation_receipt": valid.to_dict(), "hit": True},
                       {"mutation_receipt": {"planned": True}},
                       {"mutation_receipt": dict(valid.to_dict(), applied=False)},
                       {"mutation_receipt": dict(valid.to_dict(), hit=1)}):
            with self.assertRaises(ValueError):
                MutationReceipt.from_record(record)

    def test_hash_tracks_final_config_and_layers_but_not_run_metadata(self):
        data = scenario().effective_input(final_input={"delay_ms": 12})
        original = effective_input_hash(data)
        self.assertEqual(original, effective_input_hash(dict(data, campaign_kind="exploratory", origin_domain=99)))
        for field, value in (("target_revision", 1), ("repeat", 1), ("rmw", "other"),
                             ("final_input", {"delay_ms": 13}), ("mutation_layer_order", ["b", "a"]),
                             ("input_layer_receipts", [{"layer_id": "reader_config", "config": {"depth": 7}}])):
            modified = copy.deepcopy(data)
            modified[field] = value
            self.assertNotEqual(original, effective_input_hash(modified), field)
        self.assertEqual(canonical_hash({"b": 2, "a": 1}), canonical_hash({"a": 1, "b": 2}))
        with self.assertRaises(ValueError):
            canonical_hash({"v": float("nan")})

    def test_evidence_hashes_raw_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "raw.json"
            path.write_bytes(b'{ "raw": true }\n')
            digest = raw_sha256(path)
            verify_raw(path, digest)
            path.write_bytes(b'{"raw":true}\n')
            with self.assertRaises(ValueError):
                verify_raw(path, digest)

    def test_plan_does_not_execute_and_requires_are_checked(self):
        calls = []
        executor = StateExecutor()
        steps = [StateStep("reader", "create_reader", requires=[StateCondition("ready", "ready")])]
        self.assertEqual(executor.plan(steps)[0]["step_id"], "reader")
        self.assertEqual(calls, [])
        receipts = executor.execute(steps, {}, lambda action, parameters, state: calls.append(action))
        self.assertEqual(calls, [])
        self.assertEqual(receipts[0].status, "blocked")
        self.assertIn("ready", receipts[0].requires)

    def test_steps_record_before_after_and_failed_actions(self):
        def perform(action, parameters, state):
            state["writer_ready"] = True
            return {"endpoint_id": "writer-1"}
        receipts = StateExecutor().execute(target().state_steps, {}, perform)
        self.assertEqual(receipts[0].before, {})
        self.assertTrue(receipts[0].after["writer_ready"])
        self.assertEqual(receipts[0].status, "executed")
        def fail(*args):
            raise RuntimeError("cannot create")
        receipts = StateExecutor().execute(target().state_steps, {}, fail)
        self.assertEqual(receipts[0].status, "failed")
        self.assertIn("cannot create", receipts[0].error)

    def test_stage_cannot_jump_or_resume_abort(self):
        record = ExecutionRecord("run-1", scenario())
        with self.assertRaises(ValueError):
            record.advance(ExecutionStage.REACHED)
        record.advance(ExecutionStage.EXPRESSIBLE, adapter_accepted=True)
        record.advance(ExecutionStage.PLANNED)
        with self.assertRaises(ValueError):
            record.advance(ExecutionStage.PRECONDITION_EXECUTED)
        record.abort("missing receipt")
        with self.assertRaises(ValueError):
            record.advance(ExecutionStage.EXPRESSIBLE)
        self.assertEqual(record.to_dict()["abort_reason"], "missing receipt")

    def test_typed_conditions_do_not_coerce_bool_or_strings(self):
        for op in ("gt", "ge", "lt", "le"):
            for actual, expected in ((True, 0), (0, True), ("true", 1), ("1", 1)):
                self.assertFalse(StateCondition("typed", "v", op, expected).evaluate({"v": actual}))
        for actual, expected in ((True, [1]), (1, [True]), ("true", [True]), ("1", [1])):
            self.assertFalse(StateCondition("typed", "v", "in", expected).evaluate({"v": actual}))
        self.assertTrue(StateCondition("typed", "v", "in", [True]).evaluate({"v": True}))
        self.assertTrue(StateCondition("typed", "v", "gt", 0).evaluate({"v": 1}))

    def test_scenario_owns_a_copy_of_parameters(self):
        parameters = {"qos": {"depth": 3}}
        s = compile_scenario(target(), "rmw_cyclonedds_cpp", "mutated", 0, parameters=parameters)
        parameters["qos"]["depth"] = 4
        self.assertEqual(s.parameters["qos"]["depth"], 3)


if __name__ == "__main__":
    unittest.main()
