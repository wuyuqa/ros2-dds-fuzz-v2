"""Declared v1 fault receipts and the bounded legacy contract, all offline."""

import copy
import pathlib
import tempfile
import unittest

from r2f.core import (
    CandidateLifecycle, ExecutionRecord, MutationReceipt, SemanticTarget,
    compile_scenario, raw_sha256, validate_control, validate_execution,
    validate_fault_control_execution,
)
from r2f.core.oracle import attribute_responsibility
from r2f.modules.s1.module import S1Module
from test_core_contracts import target


SCHEMA = "r2f.fault-control-receipt/v1"
FIRST_TWO = ("discovery.late_endpoint", "discovery.late_join_delivery")
THIRD = "discovery.interruption_recovery"
FOURTH = "builtin.discovery_projection_reconciliation"


def control_record(projection=False):
    manifest = target().to_dict()
    manifest["target_id"] = FOURTH if projection else THIRD
    fault_id = "controlled_projection_field_mismatch" if projection else "post_restore_delivery_omission"
    kind = "projection_field_mismatch" if projection else "delivery_omission"
    contract = {"arm": "negative", "fault_id": fault_id, "receipt_kind": kind,
                "expected": "primary_fail", "candidate_eligible": False}
    receipt = {"schema": SCHEMA, "fault_id": fault_id, "receipt_kind": kind,
               "applied": True, "raw_event_ref": 17, "responsibility_layer": "harness",
               "candidate_eligible": False}
    if projection:
        receipt.update(field_path="writer.qos.reliability", projection_source="builtin",
                       before="reliable", after="best_effort",
                       original_sources={
                           "builtin": {"raw_sha256": "a" * 64, "raw_ref": "builtin-capture.json#writer"},
                           "graph": {"raw_sha256": "b" * 64, "raw_ref": "graph-capture.json#writer"},
                       }, controlled_projection_ref="controlled-projection.json#writer")
    else:
        contract["window"] = "post_restore_delivery"
        receipt.update(window="post_restore_delivery", omitted_sequence=3,
                       window_refs={"open_event_ref": 12, "close_event_ref": 25})
    manifest["control_contract"] = {"fault": contract}
    return {"target": manifest, "target_id": manifest["target_id"], "target_revision": 0,
            "arm": "negative", "control_kind": "fault", "execution_stage": "judged",
            "mutation_receipt": MutationReceipt().to_dict(), "fault_control_receipt": receipt,
            "oracle_verdict": "fail"}


class DeclaredFaultControlContracts(unittest.TestCase):
    def test_valid_delivery_and_projection_receipts(self):
        for projection in (False, True):
            row = control_record(projection)
            with self.subTest(projection=projection):
                self.assertTrue(validate_fault_control_execution(row))
                self.assertTrue(validate_control(row))
        self.assertNotIn("omitted_sequence", control_record(True)["fault_control_receipt"])

    def test_sealed_first_two_manifests_keep_legacy_validation(self):
        module = S1Module()
        for identifier in FIRST_TWO:
            manifest = module.targets[identifier].to_dict()
            before = copy.deepcopy(manifest)
            receipt = {"fault_id": "late_endpoint_delivery_omission", "applied": True,
                       "omitted_sequence": 1, "raw_event_ref": 0,
                       "responsibility_layer": "harness", "candidate_eligible": False}
            row = dict(control_record(), target=manifest, target_id=identifier, fault_control_receipt=receipt)
            with self.subTest(target=identifier):
                self.assertTrue(validate_fault_control_execution(row))
                self.assertEqual(manifest, before)
                self.assertNotIn("receipt_kind", manifest["control_contract"]["fault"])

    def test_legacy_requires_explicit_exact_fault_declaration(self):
        module = S1Module()
        for identifier in FIRST_TWO:
            for defect in ("missing_control_contract", "missing_fault", "missing_fault_id",
                           "null_fault_id", "empty_fault_id", "wrong_fault_id"):
                manifest = module.targets[identifier].to_dict()
                if defect == "missing_control_contract":
                    del manifest["control_contract"]
                elif defect == "missing_fault":
                    del manifest["control_contract"]["fault"]
                elif defect == "missing_fault_id":
                    del manifest["control_contract"]["fault"]["fault_id"]
                else:
                    manifest["control_contract"]["fault"]["fault_id"] = {
                        "null_fault_id": None, "empty_fault_id": "", "wrong_fault_id": "other_fault",
                    }[defect]
                receipt = {"fault_id": "late_endpoint_delivery_omission", "applied": True,
                           "omitted_sequence": 1, "raw_event_ref": 0,
                           "responsibility_layer": "harness", "candidate_eligible": False}
                row = dict(control_record(), target=manifest, target_id=identifier,
                           fault_control_receipt=receipt)
                with self.subTest(target=identifier, defect=defect), self.assertRaises(ValueError):
                    validate_fault_control_execution(row)

    def test_legacy_does_not_extend_to_other_targets_or_fault_ids(self):
        for identifier in (THIRD, FOURTH, "unregistered.target"):
            row = control_record()
            row["target_id"] = row["target"]["target_id"] = identifier
            row["target"]["control_contract"]["fault"].pop("receipt_kind")
            row["fault_control_receipt"]["fault_id"] = "late_endpoint_delivery_omission"
            with self.subTest(target=identifier), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        row = control_record()
        row["target_id"] = row["target"]["target_id"] = FIRST_TWO[0]
        row["target"]["control_contract"]["fault"].pop("receipt_kind")
        with self.assertRaises(ValueError):
            validate_fault_control_execution(row)

    def test_new_receipt_requires_exact_declared_fault_kind_and_window(self):
        for key, value in (("fault_id", "arbitrary_fault"), ("receipt_kind", "projection_field_mismatch"),
                           ("window", "post_match_delivery"), ("schema", "other/v1")):
            row = control_record()
            row["fault_control_receipt"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        for key in ("fault_id", "receipt_kind", "window"):
            row = control_record()
            del row["target"]["control_contract"]["fault"][key]
            with self.subTest(missing_declaration=key), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        row = control_record()
        row["target"]["control_contract"]["fault"]["receipt_kind"] = "unknown_kind"
        row["fault_control_receipt"]["receipt_kind"] = "unknown_kind"
        with self.assertRaises(ValueError):
            validate_fault_control_execution(row)

    def test_malformed_kind_or_projection_source_is_a_contract_error(self):
        for kind in (None, [], {}, True, 1):
            row = control_record()
            row["target"]["control_contract"]["fault"]["receipt_kind"] = kind
            row["fault_control_receipt"]["receipt_kind"] = kind
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        for source in (None, [], {}, True, 1):
            row = control_record(True)
            row["fault_control_receipt"]["projection_source"] = source
            with self.subTest(source=source), self.assertRaises(ValueError):
                validate_fault_control_execution(row)

    def test_target_identity_or_missing_manifest_cannot_select_a_contract(self):
        for changes in ({"target_id": FIRST_TWO[0]}, {"target": {}}, {"target": None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_fault_control_execution(dict(control_record(), **changes))

    def test_delivery_requires_valid_window_references_and_integer_omission(self):
        for key in ("window", "omitted_sequence", "window_refs"):
            row = control_record()
            del row["fault_control_receipt"][key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        for sequence in (True, -1, "3", None):
            row = control_record()
            row["fault_control_receipt"]["omitted_sequence"] = sequence
            with self.subTest(sequence=sequence), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        for refs in ({}, {"open_event_ref": 12}, {"open_event_ref": True, "close_event_ref": 25},
                     {"open_event_ref": 12, "close_event_ref": " "},
                     {"open_event_ref": 25, "close_event_ref": 12}):
            row = control_record()
            row["fault_control_receipt"]["window_refs"] = refs
            with self.subTest(refs=refs), self.assertRaises(ValueError):
                validate_fault_control_execution(row)

    def test_projection_requires_before_after_fields_and_original_sources(self):
        required = ("field_path", "projection_source", "before", "after", "original_sources", "controlled_projection_ref")
        for key in required:
            row = control_record(True)
            del row["fault_control_receipt"][key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        for key, value in (("field_path", " "), ("projection_source", "config"),
                           ("controlled_projection_ref", {}), ("after", "reliable")):
            row = control_record(True)
            row["fault_control_receipt"][key] = value
            with self.subTest(field=key, value=value), self.assertRaises(ValueError):
                validate_fault_control_execution(row)
        row = control_record(True)
        row["fault_control_receipt"].update(before=True, after=1)
        self.assertTrue(validate_fault_control_execution(row))  # distinct JSON types

    def test_projection_requires_each_source_hash_and_ref(self):
        for source in ("builtin", "graph"):
            for key, value in (("raw_sha256", None), ("raw_sha256", "f" * 63),
                               ("raw_ref", None), ("raw_ref", " "), ("raw_ref", True)):
                row = control_record(True)
                row["fault_control_receipt"]["original_sources"][source][key] = value
                with self.subTest(source=source, field=key, value=value), self.assertRaises(ValueError):
                    validate_fault_control_execution(row)
            row = control_record(True)
            del row["fault_control_receipt"]["original_sources"][source]
            with self.subTest(missing_source=source), self.assertRaises(ValueError):
                validate_fault_control_execution(row)

    def test_new_control_still_rejects_hit_mutation_stage_and_candidate(self):
        for projection in (False, True):
            row = control_record(projection)
            for changes in ({"arm": "mutated"}, {"control_kind": None},
                            {"execution_stage": "mutation_applied"},
                            {"mutation_receipt": MutationReceipt(True, True, True, True, True, True, True).to_dict()}):
                with self.subTest(projection=projection, changes=changes), self.assertRaises(ValueError):
                    validate_fault_control_execution(dict(row, **changes))
            row.update(environment_healthy=True, preconditions_reached=True)
            self.assertEqual(attribute_responsibility(row).reason_code, "control_excluded")

    def test_declared_controls_use_existing_execution_and_lifecycle_gates(self):
        from r2f.core import StateExecutor
        for projection in (False, True):
            row = control_record(projection)
            scenario = compile_scenario(SemanticTarget.from_dict(row["target"]), "rmw_cyclonedds_cpp", "negative", 0)
            record = ExecutionRecord("offline-contract", scenario)
            record.advance("expressible", adapter_accepted=True).advance("planned")
            receipts = StateExecutor().execute(scenario.state_steps, {}, lambda a, p, state: state.update(writer_ready=True))
            record.advance("precondition_executed", precondition_receipts=[r.to_dict() for r in receipts]).advance("reached")
            with tempfile.TemporaryDirectory() as tmp:
                raw = pathlib.Path(tmp) / "offline-control.json"
                raw.write_text('{"source":"synthetic-contract-only"}\n')
                record.advance("effect_observed", control_kind="fault", fault_control_receipt=row["fault_control_receipt"],
                               raw_path=str(raw), raw_sha256=raw_sha256(raw), typed_facts={"control": True})
                record.advance("judged", oracle_verdict="fail")
                data = record.to_dict()
                self.assertTrue(validate_execution(data))
                self.assertTrue(validate_control(data))
                self.assertNotIn("mutation_applied", [event["execution_stage"] for event in data["stage_history"]])
                evidence = dict(data, timestamp="2026-11-15T00:00:00+08:00", endpoint_identity="writer/reader",
                                candidate_source="blackbox", evidence_tier="blackbox", provenance={"offline": True})
                with self.assertRaisesRegex(ValueError, "control"):
                    CandidateLifecycle("offline-control", evidence).admit()


if __name__ == "__main__":
    unittest.main()
