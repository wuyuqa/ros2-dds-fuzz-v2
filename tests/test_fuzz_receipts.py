"""V12/V13: execution receipts are separate from deterministic input layers."""
import pathlib
import tempfile
import unittest

from r2f.core.evidence import (
    aggregate_layer_receipts, execution_chain_status, validate_chain_receipt,
)
from r2f.core.mutation import MutationReceipt


def full():
    return MutationReceipt(True, True, True, True, True, True, True).to_dict()


def miss():
    return MutationReceipt(True, True, True, True, False, False, False).to_dict()


class ReceiptChainV12(unittest.TestCase):
    def test_v12_one_layer_hit_other_missed_does_not_pass_chain(self):
        chain = execution_chain_status([{"layer_id": "param", "hit": True},
                                        {"layer_id": "environment", "hit": False}])
        self.assertFalse(chain["chain_hit"])
        self.assertEqual(chain["hit_layers"], ["param"])
        self.assertEqual(chain["missed_layers"], ["environment"])
        # Partial results are preserved rather than overwritten.
        aggregate = aggregate_layer_receipts([
            {"layer_id": "param", "receipt": full()},
            {"layer_id": "environment", "receipt": miss()},
        ])
        self.assertFalse(aggregate["applied"])
        self.assertFalse(aggregate["hit"])
        self.assertFalse(aggregate["effect_observed"])
        self.assertTrue(aggregate["configured"])
        # A lying aggregate cannot claim applied/hit on the same layers.
        for field in ("applied", "hit", "effect_observed"):
            lying = dict(aggregate, **{field: True})
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_chain_receipt([{"layer_id": "param", "hit": True},
                                        {"layer_id": "environment", "hit": False}], lying)

    def test_v12_both_layers_hit_passes(self):
        aggregate = aggregate_layer_receipts([
            {"layer_id": "param", "receipt": full()},
            {"layer_id": "environment", "receipt": full()},
        ])
        self.assertTrue(all(aggregate.values()))
        self.assertTrue(execution_chain_status([{"layer_id": "a", "hit": True},
                                                {"layer_id": "b", "hit": True}])["chain_hit"])

    def test_v12_overridden_layer_is_provenance_not_a_hit(self):
        chain = execution_chain_status([
            {"layer_id": "param", "hit": True},
            {"layer_id": "environment", "hit": False, "overridden": True},
        ])
        self.assertTrue(chain["chain_hit"])
        self.assertEqual(chain["overridden_layers"], ["environment"])

    def test_v12_no_required_layer_is_rejected(self):
        with self.assertRaises(ValueError):
            execution_chain_status([{"layer_id": "a", "hit": True, "required": False}])


class InputLayerReceiptV13(unittest.TestCase):
    def test_v13_execution_fields_in_input_layer_receipts_are_rejected(self):
        # The v1 identity guard already rejects hit/applied/time; verify via the
        # v2 path too, including nested receipts.
        from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
        base = {"target_id": "discovery.late_endpoint", "target_revision": 0,
                "mutation_kind": "late_joiner", "carrier": "ros2_endpoint",
                "mutation_parameters": {}, "rmw": "rmw_cyclonedds_cpp", "repeat": 0,
                "seed": 0, "arm": "mutated", "mutation_layer_order": ["late_joiner"],
                "input_layer_receipts": [{"layer": "ros2_endpoint"}],
                "final_input": {"reader_delay_sec": 0.15},
                "input_identity_schema": INPUT_SCHEMA_V2}
        self.assertTrue(effective_input_hash(base))
        for bad in ({"hit": True}, {"applied": True}, {"execution_stage": "judged"},
                    {"nested": [{"timestamp": "2026-11-16T00:00:00Z"}]},
                    {"mutation_receipt": full()}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                effective_input_hash(dict(base, input_layer_receipts=[{"layer": "x", **bad}]))


if __name__ == "__main__":
    unittest.main()
