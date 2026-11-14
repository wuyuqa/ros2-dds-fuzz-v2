import copy
import pathlib
import tempfile
import unittest

from r2f.core import (
    CandidateLifecycle, ExecutionRecord, ExecutionStage, MutationReceipt, ReplayCapsule,
    attribute_responsibility, effective_input_hash, raw_sha256, rerun_conformance,
    unknown_gate, validate_control, validate_execution, validate_replay_input, StateExecutor,
)
from test_core_contracts import scenario


def hit():
    return MutationReceipt(True, True, True, True, True, True, True).to_dict()


def neutral():
    return {"kind": "no_op", "reason": "baseline public API scenario", "before_input_sha256": "a" * 64,
            "after_input_sha256": "a" * 64}


def executed(raw, arm="mutated"):
    s = scenario()
    s.arm = arm
    receipts = StateExecutor().execute(s.state_steps, {}, lambda a, p, state: state.update(writer_ready=True))
    return dict(s.to_dict(), run_id="run-1", execution_stage="judged", actual_run=True,
                execution_source="ros2_public_api", adapter_accepted=True,
                precondition_receipts=[r.to_dict() for r in receipts],
                mutation_receipt=hit() if arm == "mutated" else MutationReceipt().to_dict(),
                neutral_receipt=neutral() if arm != "mutated" else None,
                observed_effect={"effects": ["reader_created_after_writer"]},
                typed_facts={"matched": True}, raw_path=str(raw), raw_sha256=raw_sha256(raw),
                oracle_verdict="pass")


def observation(raw):
    return dict(executed(raw), candidate_source="blackbox", evidence_tier="blackbox",
                endpoint_identity="writer-1/reader-1", timestamp="2026-11-14T00:00:00+00:00",
                rule_id="discovery.late_endpoint.match_and_receive_after_join",
                effective_input_sha256=scenario().input_hash(), provenance={"source_commit": "example"})


class AuditContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.raw = pathlib.Path(self.tmp.name) / "raw.json"
        self.raw.write_text('{"matched":true}\n')

    def tearDown(self):
        self.tmp.cleanup()

    def test_neutral_arms_skip_mutation_stage_and_cannot_fake_hit(self):
        for arm in ("baseline", "transparent"):
            record = executed(self.raw, arm)
            validate_execution(record)
            record["mutation_receipt"] = hit()
            with self.assertRaises(ValueError):
                validate_execution(record)
            record["mutation_receipt"] = MutationReceipt().to_dict()
            record["neutral_receipt"]["after_input_sha256"] = "b" * 64
            with self.assertRaises(ValueError):
                validate_execution(record)

    def test_neutral_stage_path_is_audited(self):
        s = scenario()
        s.arm = "baseline"
        record = ExecutionRecord("run-baseline", s)
        record.advance("expressible", adapter_accepted=True)
        record.advance("planned")
        receipts = executed(self.raw, "baseline")["precondition_receipts"]
        record.advance("precondition_executed", precondition_receipts=receipts)
        record.advance("reached")
        record.advance("effect_observed", neutral_receipt=neutral(), raw_path=str(self.raw),
                       raw_sha256=raw_sha256(self.raw), typed_facts={"matched": True})
        record.advance("judged", oracle_verdict="pass")
        self.assertNotIn("mutation_applied", [r["execution_stage"] for r in record.to_dict()["stage_history"]])
        with self.assertRaises(ValueError):
            ExecutionRecord("bad", s).advance("mutation_applied", mutation_receipt=hit())

    def test_forged_reach_and_plan_only_receipts_fail(self):
        data = executed(self.raw)
        data["precondition_receipts"][0]["status"] = "planned"
        with self.assertRaises(ValueError):
            validate_execution(data)
        data = executed(self.raw)
        data["precondition_receipts"][0]["after"] = {"writer_ready": False}
        with self.assertRaises(ValueError):
            validate_execution(data)

    def test_execution_receipts_do_not_change_input_identity(self):
        data = scenario().effective_input(final_input={"reader_delay_ms": 10},
                                           input_layer_receipts=[{"layer_id": "endpoint", "config": {"depth": 3}}])
        digest = effective_input_hash(data)
        self.assertEqual(digest, effective_input_hash(dict(data, mutation_receipt=hit(),
                                                          timestamp="runtime", campaign_kind="required",
                                                          origin_domain=50)))
        data["input_layer_receipts"][0]["hit"] = True
        with self.assertRaises(ValueError):
            effective_input_hash(data)

    def test_residual_has_precedence_over_allowed_effect(self):
        data = dict(executed(self.raw), environment_healthy=True, preconditions_reached=True,
                    window_complete=True, endpoint_identity="writer-1/reader-1", identity_complete=True,
                    expected_effect_evaluable=True, oracle_verdict="fail", controls_support_sut=True,
                    provenance_supports_sut=True,
                    expected_effect_set=scenario().target.expected_effect_set,
                    observed_effect={"effects": ["reader_created_after_writer"],
                                     "residuals": ["post_match_delivery_loss"]})
        result = attribute_responsibility(data)
        self.assertEqual(result.to_dict(), {"triage_disposition": "candidate", "responsibility_layer": "sut",
                                           "reason_code": "sut_contract_violation"})
        data["observed_effect"]["residuals"] = []
        self.assertEqual(attribute_responsibility(data).reason_code, "mutation_expected_effect")

    def test_primary_rule_violation_is_not_swallowed_by_allowed_effect(self):
        data = dict(executed(self.raw), environment_healthy=True, preconditions_reached=True,
                    window_complete=True, endpoint_identity="writer-1/reader-1", identity_complete=True,
                    expected_effect_evaluable=True, oracle_verdict="fail", controls_support_sut=True,
                    provenance_supports_sut=True, primary_rule_violated=True,
                    expected_effect_set=scenario().target.expected_effect_set)
        self.assertEqual(attribute_responsibility(data).reason_code, "sut_contract_violation")

    def test_responsibility_tree_is_ordered_and_unknown_is_not_pass(self):
        base = dict(executed(self.raw), environment_healthy=True, preconditions_reached=True,
                    identity_complete=True, endpoint_identity="writer-1/reader-1", window_complete=True,
                    expected_effect_evaluable=True, expected_effect_set=scenario().target.expected_effect_set)
        for changes, reason in (({"environment_healthy": False}, "environment_or_process"),
                                ({"preconditions_reached": False}, "reach_or_planning"),
                                ({"mutation_receipt": MutationReceipt().to_dict()}, "mutation_ineffective"),
                                ({"tool_error": True}, "tool_error"),
                                ({"identity_complete": False}, "unknown_evidence"),
                                ({"window_complete": False}, "window_incomplete"),
                                ({"oracle_verdict": "unknown"}, "unknown_evidence"),
                                ({"oracle_verdict": "pass"}, "oracle_pass"),
                                ({"oracle_verdict": "not_applicable"}, "not_applicable")):
            with self.subTest(reason=reason):
                self.assertEqual(attribute_responsibility(dict(base, **changes)).reason_code, reason)

    def test_controls_and_unknown_gate(self):
        row = dict(executed(self.raw, "baseline"), control_kind="health")
        self.assertTrue(validate_control(row))
        row["oracle_verdict"] = "unknown"
        self.assertFalse(validate_control(row))
        with self.assertRaises(ValueError):
            unknown_gate([dict(row, campaign_kind="required")])
        for kind, arm in (("fault", "negative"), ("mutation", "mutated"), ("transparency", "transparent")):
            row = dict(executed(self.raw, arm), control_kind=kind, oracle_verdict="fail" if kind == "fault" else "pass")
            if kind == "fault":
                self.assertFalse(validate_control(row))
                row["fault_control_receipt"] = {
                    "fault_id": "late_endpoint_delivery_omission", "applied": True,
                    "omitted_sequence": 1, "raw_event_ref": 17,
                    "responsibility_layer": "harness", "candidate_eligible": False,
                }
            if kind == "transparency":
                self.assertFalse(validate_control(row))
                row.update(transparency_expected="no_effect_or_expected_pass", neutral_receipt=neutral())
            self.assertTrue(validate_control(row))
        with self.assertRaises(ValueError):
            unknown_gate([{"target_id": "a", "campaign_kind": "exploratory", "oracle_verdict": "unknown"}])

    def test_candidate_admission_checks_evidence_and_control_exclusion(self):
        candidate = CandidateLifecycle("c1", observation(self.raw))
        candidate.admit()
        self.assertEqual(candidate.candidate_status, "admitted")
        for changes in ({"provenance": {}}, {"control_kind": "fault"}, {"candidate_source": "control"},
                        {"mutation_receipt": MutationReceipt().to_dict()}, {"effective_input_sha256": "bad"},
                        {"responsibility_layer": "harness"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                CandidateLifecycle("invalid", dict(observation(self.raw), **changes)).admit()

    def test_replay_failure_keeps_candidate_and_requires_disposition(self):
        c = CandidateLifecycle("c1", observation(self.raw))
        c.admit()
        with self.assertRaises(ValueError):
            c.validate(require_disposition=True)
        c.record_replay("fail_timing", failure_reason="reader setup exceeded window")
        self.assertEqual(c.candidate_status, "admitted")
        self.assertIn("replay_unconfirmed", c.to_dict()["derived_labels"])
        c.triage({"triage_disposition": "deferred", "responsibility_layer": "observation",
                  "reason_code": "unknown_evidence"})
        c.validate(require_disposition=True)
        self.assertEqual(c.candidate_status, "deferred")
        self.assertEqual(len(c.to_dict()["replay_attempts"]), 1)

    def test_confirm_requires_adjudication_and_named_basis_if_replay_failed(self):
        c = CandidateLifecycle("c1", observation(self.raw))
        c.admit()
        c.record_replay("fail_environment", failure_reason="domain unavailable")
        c.triage({"triage_disposition": "candidate", "responsibility_layer": "sut",
                  "reason_code": "sut_contract_violation"})
        with self.assertRaises(ValueError):
            c.confirm(adjudicator_id="reviewer", adjudication_method="peer_review")
        c.confirm(adjudicator_id="reviewer", adjudication_method="peer_review",
                  confirmation_basis="non_replayable_evidence", reason="recorded independent endpoint evidence")
        self.assertNotIn("replay_unconfirmed", c.to_dict()["derived_labels"])
        self.assertEqual(c.candidate_status, "confirmed")
        c.record_replay("fail_artifact", failure_reason="later artifact unavailable")
        self.assertEqual(c.candidate_status, "confirmed")

    def test_instrumentation_requires_named_provenance(self):
        data = dict(observation(self.raw), candidate_source="instrumentation", evidence_tier="instrumented")
        with self.assertRaises(ValueError):
            CandidateLifecycle("i1", data).admit()
        data["provenance"].update(instrumentation_point="endpoint_created", binary_identity="build-1",
                                  event_time="2026-11-14T00:00:00+00:00", control="health-1")
        CandidateLifecycle("i2", data).admit()

    def test_capsule_input_is_offline_execution_is_not(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"},
                                              final_input={"topic": "/contract", "message_type": "std_msgs/msg/String", "qos": {}},
                                              origin_domain=51)
        data = capsule.to_dict()
        self.assertIsNone(data["execution"])
        validate_replay_input(data["input"])
        data["input"]["seed"] = 4
        with self.assertRaises(ValueError):
            validate_replay_input(data["input"])
        offline = executed(self.raw)
        offline["actual_run"] = False
        with self.assertRaises(ValueError):
            capsule.record_execution(offline)
        capsule.record_execution(dict(executed(self.raw), effective_input_sha256=capsule.input["effective_input_sha256"], source_manifest=capsule.input["source_manifest"]))
        self.assertEqual(capsule.to_dict()["execution"]["execution_stage"], "judged")

    def test_capsule_metadata_mismatch_and_raw_tamper_fail(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        for changes in ({"rmw": "other"}, {"target_revision": 1}, {"seed": 5}):
            with self.assertRaises(ValueError):
                capsule.record_execution(dict(executed(self.raw), **changes))
        self.raw.write_text("changed")
        data = observation(self.raw)
        data["raw_sha256"] = "f" * 64
        with self.assertRaises(ValueError):
            CandidateLifecycle("bad", data)

    def test_invalid_capsule_input_and_malformed_runtime_result_are_retained(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        capsule.input["seed"] = 99
        calls = []
        result = rerun_conformance(capsule, lambda s, d: calls.append(d), domain=52)
        self.assertEqual(result["status"], "fail_artifact")
        self.assertEqual(calls, [])
        self.assertEqual(len(capsule.replay_attempts), 1)
        capsule.input["seed"] = 0
        result = rerun_conformance(capsule, lambda s, d: object(), domain=52)
        self.assertEqual(result["status"], "fail_artifact")
        self.assertEqual(len(capsule.replay_attempts), 2)

    def test_timeout_is_timing_failure_and_capsule_roundtrips(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        def timeout(s, domain):
            raise TimeoutError("public API observation timed out")
        self.assertEqual(rerun_conformance(capsule, timeout, domain=52)["status"], "fail_timing")
        capsule.mark_non_replayable("environment_unavailable", "worker currently unavailable")
        restored = ReplayCapsule.from_dict(capsule.to_dict())
        self.assertEqual(restored.to_dict(), capsule.to_dict())

    def test_replay_compares_original_failure_and_preserves_original(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"}, origin_domain=51)
        original = dict(executed(self.raw), oracle_verdict="fail", typed_facts={"missing_sequences": [1]},
                        effective_input_sha256=capsule.input["effective_input_sha256"],
                        source_manifest=capsule.input["source_manifest"])
        capsule.record_execution(original)
        def healthy(s, domain):
            return dict(original, oracle_verdict="pass", typed_facts={"missing_sequences": []}, domain=domain)
        result = rerun_conformance(capsule, healthy, domain=52)
        self.assertEqual(result["status"], "inconclusive")
        self.assertEqual(result["comparison_basis"], "original_execution_signature")
        self.assertFalse(result["comparison_matches_original"])
        self.assertEqual(capsule.execution, original)
        self.assertEqual(capsule.execution_history[0]["oracle_verdict"], "pass")
        def same_failure(s, domain):
            return dict(original, domain=domain, run_id="normal-rerun")
        result = rerun_conformance(capsule, same_failure, domain=53)
        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["comparison_matches_original"])
        self.assertEqual(result["original_signature"], result["replay_signature"])
        self.assertEqual(capsule.execution, original)
        restored = ReplayCapsule.from_dict(capsule.to_dict())
        self.assertEqual(restored.to_dict(), capsule.to_dict())

    def test_different_failure_signature_is_inconclusive(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        original = dict(executed(self.raw), oracle_verdict="fail", typed_facts={"missing_sequences": [1]},
                        effective_input_sha256=capsule.input["effective_input_sha256"],
                        source_manifest=capsule.input["source_manifest"])
        capsule.record_execution(original)
        result = rerun_conformance(capsule, lambda s, d: dict(original, domain=d, typed_facts={"missing_sequences": [2]}), domain=52)
        self.assertEqual(result["status"], "inconclusive")
        self.assertEqual(capsule.execution, original)
        result = rerun_conformance(capsule, lambda s, d: dict(original, domain=d, raw_sha256="f" * 64), domain=53)
        self.assertEqual(result["status"], "fail_artifact")
        self.assertEqual(capsule.execution, original)
        self.assertEqual(len(capsule.replay_attempts), 2)

    def test_matching_failure_can_confirm_only_its_original_candidate(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        original = dict(observation(self.raw), oracle_verdict="fail", typed_facts={"missing_sequences": [1]},
                        effective_input_sha256=capsule.input["effective_input_sha256"],
                        source_manifest=capsule.input["source_manifest"])
        capsule.record_execution(original)
        result = rerun_conformance(capsule, lambda s, d: dict(original, domain=d), domain=52)
        self.assertEqual(result["status"], "pass")
        for candidate_id, evidence in (("correct", original), ("wrong", dict(original, run_id="other-original"))):
            candidate = CandidateLifecycle(candidate_id, evidence)
            candidate.admit()
            candidate.triage({"triage_disposition": "candidate", "responsibility_layer": "sut", "reason_code": "sut_contract_violation"})
            candidate.record_replay(result["status"], **{k: v for k, v in result.items() if k not in {"status", "failure_reason"}})
            if candidate_id == "wrong":
                with self.assertRaises(ValueError):
                    candidate.confirm(adjudicator_id="reviewer", adjudication_method="peer_review")
            else:
                candidate.confirm(adjudicator_id="reviewer", adjudication_method="peer_review")
                self.assertEqual(candidate.candidate_status, "confirmed")

    def test_replay_typed_signature_never_equates_bool_and_integer(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        original = dict(executed(self.raw), oracle_verdict="fail", typed_facts={"missing_sequences": [1]},
                        effective_input_sha256=capsule.input["effective_input_sha256"],
                        source_manifest=capsule.input["source_manifest"])
        capsule.record_execution(original)
        result = rerun_conformance(capsule, lambda s, d: dict(original, domain=d, typed_facts={"missing_sequences": [True]}), domain=52)
        self.assertEqual(result["status"], "inconclusive")

    def test_execution_only_replay_cannot_confirm_existing_anomaly(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"})
        def healthy(s, d):
            return dict(executed(self.raw), domain=d, effective_input_sha256=s.effective_input_sha256,
                        source_manifest=s.source_manifest)
        result = rerun_conformance(capsule, healthy, domain=52)
        self.assertEqual(result["comparison_basis"], "execution_success_only")
        c = CandidateLifecycle("c1", dict(observation(self.raw), oracle_verdict="fail", typed_facts={"missing_sequences": [1]}))
        c.admit()
        c.triage({"triage_disposition": "candidate", "responsibility_layer": "sut", "reason_code": "sut_contract_violation"})
        c.record_replay(result["status"], comparison_basis=result["comparison_basis"])
        with self.assertRaises(ValueError):
            c.confirm(adjudicator_id="reviewer", adjudication_method="peer_review")

    def test_normal_rerun_requires_fresh_domain_and_preserves_failures(self):
        capsule = ReplayCapsule.from_scenario(scenario(), source_manifest={"source_commit": "example"}, origin_domain=51)
        calls = []
        def runner(s, domain):
            calls.append((s.target_id, domain))
            result = executed(self.raw)
            result["domain"] = domain
            result["effective_input_sha256"] = s.effective_input_sha256
            result["source_manifest"] = s.source_manifest
            return result
        with self.assertRaises(ValueError):
            rerun_conformance(capsule, runner, domain=51)
        result = rerun_conformance(capsule, runner, domain=52)
        self.assertEqual(result["status"], "pass")
        self.assertEqual(calls[0][1], 52)
        def failed(s, domain):
            raise RuntimeError("runtime unavailable")
        result = rerun_conformance(capsule, failed, domain=53)
        self.assertEqual(result["status"], "fail_environment")
        self.assertIn("runtime unavailable", result["failure_reason"])
        self.assertEqual(len(capsule.to_dict()["replay_attempts"]), 2)
        self.assertIsNotNone(capsule.to_dict()["execution"])


if __name__ == "__main__":
    unittest.main()
