"""Offline schema/identity contracts. These fixtures prove no DDS execution."""
from copy import deepcopy
import itertools
import unittest

from r2f.core.evidence import canonical_hash
from r2f.core import state_signature as pair
from r2f.core.topology_state import (
    SCHEMA, abstract_topology_state, validate_topology_state, topology_state_id,
    topology_edge_id, logical_endpoints,
)


def program():
    return {"schema": "r2f.s1-topology-program/v1", "endpoints": [
        {"logical_id": eid, "role": role} for eid, role in
        (("w0", "writer"), ("w1", "writer"), ("r0", "reader"), ("r1", "reader"))],
        "actions": [{"op": "create", "endpoint_id": eid, "generation": 1}
                    for eid in ("w0", "w1", "r0", "r1")]}


def facts():
    return {"endpoints": {e["logical_id"]: {"generation": 1, "liveness": "alive"}
                          for e in program()["endpoints"]},
            "pairs": [{"writer": w, "reader": r, "compatibility": "compatible",
                       "observed_match": "bidirectional" if w[-1] == r[-1] else "none",
                       "delivery_closure": "closed"} for w in ("w0", "w1") for r in ("r0", "r1")],
            "network_phase": "normal", "recovery_cycle_bucket": "0", "observation_complete": True}


class TopologyStateTests(unittest.TestCase):
    def test_all_endpoint_declaration_permutations_preserve_state_and_hash(self):
        p, f = program(), facts()
        expected = abstract_topology_state(p, f)
        for order in itertools.permutations(p["endpoints"]):
            altered = {**p, "endpoints": list(order)}
            self.assertEqual(expected, abstract_topology_state(altered, f))
            self.assertEqual(topology_state_id(expected), topology_state_id(abstract_topology_state(altered, f)))

    def test_pair_and_endpoint_fact_order_is_canonical(self):
        f = facts()
        altered = deepcopy(f)
        altered["endpoints"] = dict(reversed(list(altered["endpoints"].items())))
        altered["pairs"].reverse()
        self.assertEqual(abstract_topology_state(program(), f), abstract_topology_state(program(), altered))
        state = abstract_topology_state(program(), f)
        reordered = deepcopy(state)
        reordered["endpoints"].reverse()
        reordered["pairs"].reverse()
        self.assertEqual(topology_state_id(state), topology_state_id(reordered))

    def test_raw_identity_and_history_noise_does_not_enter_hash(self):
        clean = abstract_topology_state(program(), facts())
        noisy = facts()
        noisy.update(raw_path="/other/raw", raw_sha256="f" * 64, pid=500, domain_id=55,
                     timestamp=1020, run_id="noise", predicted_graph={"status": "allowed"})
        for e in noisy["endpoints"].values():
            e.update(endpoint_gid="fresh", pid=500, history=[{"generation": 44}], process_start_ticks=4)
        for p in noisy["pairs"]:
            p.update(writer_gid="noise", receive_count=42, raw_event_ref=600)
        self.assertEqual(clean, abstract_topology_state(program(), noisy))
        self.assertEqual(topology_state_id(clean), topology_state_id(abstract_topology_state(program(), noisy)))

    def test_directed_logical_graph_changes_identity(self):
        first = abstract_topology_state(program(), facts())
        f = facts()
        for p in f["pairs"]:
            p["observed_match"] = "none" if p["observed_match"] == "bidirectional" else "bidirectional"
        self.assertNotEqual(topology_state_id(first), topology_state_id(abstract_topology_state(program(), f)))

    def test_current_logical_generation_is_semantic_but_history_is_not(self):
        first = abstract_topology_state(program(), facts())
        f = facts()
        f["endpoints"]["r0"]["generation"] = 2
        second = abstract_topology_state(program(), f)
        self.assertNotEqual(topology_state_id(first), topology_state_id(second))
        f["endpoints"]["r0"]["history"] = ["raw-noise"]
        self.assertEqual(second, abstract_topology_state(program(), f))

    def test_missing_data_not_filled_from_program_or_prediction(self):
        state = abstract_topology_state(program(), {"observation_complete": True,
            "predicted_graph": facts()["pairs"], "state": facts()})
        self.assertEqual(state["observation_completeness"], "incomplete")
        self.assertTrue(all(e["generation"] is None and e["liveness"] == "unknown" for e in state["endpoints"]))
        self.assertTrue(all(p["observed_match"] == "unknown" for p in state["pairs"]))

    def test_completeness_is_derived_not_labelled(self):
        for changed in ({"observation_complete": False}, {"window_complete": False}, {"network_phase": "unknown"}):
            state = abstract_topology_state(program(), {**facts(), **changed, "observation_completeness": "complete"})
            self.assertEqual(state["observation_completeness"], "incomplete")
        f = facts()
        del f["pairs"][0]["compatibility"]
        self.assertEqual(abstract_topology_state(program(), f)["observation_completeness"], "incomplete")

    def test_identity_contradiction_raises_not_new_state(self):
        with self.assertRaises(ValueError):
            abstract_topology_state(program(), facts(), identity_consistent=False)
        for name in ("identity_consistent", "endpoint_identity_consistent"):
            with self.assertRaises(ValueError):
                abstract_topology_state(program(), {**facts(), name: False})

    def test_invalid_component_shapes_fail_as_value_errors(self):
        for field, value in (("liveness", []), ("liveness", "dead"), ("generation", True),
                             ("generation", -1), ("generation", 1.5)):
            f = facts()
            f["endpoints"]["w0"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                abstract_topology_state(program(), f)
        for field in ("compatibility", "observed_match", "delivery_closure"):
            f = facts()
            f["pairs"][0][field] = {}
            with self.subTest(field=field), self.assertRaises(ValueError):
                abstract_topology_state(program(), f)

    def test_duplicate_or_unmapped_ids_rejected(self):
        for mutate in (lambda p: p["endpoints"].append(deepcopy(p["endpoints"][0])),
                       lambda p: p["endpoints"][0].update(role=[])):
            p = program()
            mutate(p)
            with self.assertRaises(ValueError):
                logical_endpoints(p)
        f = facts()
        f["pairs"].append(deepcopy(f["pairs"][0]))
        with self.assertRaises(ValueError):
            abstract_topology_state(program(), f)
        f = facts()
        f["endpoints"]["raw-generated-id"] = {"generation": 1, "liveness": "alive"}
        with self.assertRaises(ValueError):
            abstract_topology_state(program(), f)

    def test_strict_state_rejects_raw_identity_and_missing_pairs(self):
        state = abstract_topology_state(program(), facts())
        for changed in ({**state, "pid": 5}, {**state, "schema": "other"}, {**state, "pairs": state["pairs"][:-1]}):
            with self.assertRaises(ValueError):
                validate_topology_state(changed)
        state["endpoints"][0]["liveness"] = "absent"
        with self.assertRaises(ValueError):
            validate_topology_state(state)

    def test_live_pair_cannot_be_not_applicable(self):
        f = facts()
        f["pairs"][0]["observed_match"] = "not_applicable"
        with self.assertRaises(ValueError):
            abstract_topology_state(program(), f)

    def test_zero_endpoints_and_no_pair_are_explicit_not_unknown(self):
        p = {"schema": "r2f.s1-topology-program/v1", "endpoints": [], "actions": []}
        state = abstract_topology_state(p, {"network_phase": "normal", "recovery_cycle_bucket": "0", "observation_complete": True})
        self.assertEqual(state["endpoints"], [])
        self.assertEqual(state["pairs"], [])
        self.assertEqual(state["observation_completeness"], "complete")

    def test_explicit_schema_dispatch_no_implicit_detection(self):
        state = abstract_topology_state(program(), facts())
        self.assertEqual(state, pair.abstract_state(facts(), schema=SCHEMA, program=program()))
        self.assertEqual(state, pair.validate_state(state, schema=SCHEMA))
        self.assertEqual(topology_state_id(state), pair.state_id(state, schema=SCHEMA))
        self.assertEqual(topology_edge_id("a", "snapshot", "neutral", "b"), pair.edge_id("a", "snapshot", "neutral", "b", schema=SCHEMA))
        with self.assertRaises(ValueError):
            pair.state_id(state)
        with self.assertRaises(ValueError):
            pair.abstract_state(facts(), schema=SCHEMA)
        for call in (lambda: pair.state_id(state, schema="bad"),
                     lambda: pair.validate_state(state, schema="bad"),
                     lambda: pair.abstract_state({}, schema="bad"),
                     lambda: pair.edge_id("a", "b", "c", "d", schema="bad")):
            with self.assertRaises(ValueError):
                call()

    def test_legacy_pair_golden_identity_and_eight_components_unchanged(self):
        f = {"live_roles": "both", "creation_order_class": "writer_first", "compatibility_class": "compatible",
             "observed_match_class": "bidirectional", "network_phase": "normal", "recovery_cycle_bucket": "0",
             "delivery_closure_class": "closed_within_contract", "observation_complete": True}
        state = pair.abstract_state(f)
        self.assertEqual(len(state), 8)
        self.assertEqual(set(state), set(pair.STATE_COMPONENTS))
        digest = pair.state_id(state)
        self.assertEqual(digest, "ca840ab7a2d9feee352c8a13c37ba25c54480e1b1a508347a1f326e26aedbae8")
        self.assertEqual(pair.edge_id(digest, "snapshot", "nominal", digest),
                         "56b10b1a44026b95fd5e566188f0a4efc110fb7697ec81c9561dec64bdba60b9")
        self.assertEqual(digest, canonical_hash({"abstraction_version": pair.STATE_ABSTRACTION_VERSION, **state}))
        self.assertEqual(digest, pair.state_id(state, schema=pair.STATE_ABSTRACTION_VERSION))

    def test_edge_is_stable_versioned_and_parameter_sensitive(self):
        edge = topology_edge_id("a", "snapshot", "neutral", "b")
        self.assertEqual(edge, topology_edge_id("a", "snapshot", "neutral", "b"))
        self.assertNotEqual(edge, pair.edge_id("a", "snapshot", "neutral", "b"))
        self.assertNotEqual(edge, topology_edge_id("a", "snapshot", "mutated", "b"))
        for value in ("", None, 0, []):
            with self.assertRaises(ValueError):
                topology_edge_id(value, "snapshot", "neutral", "b")
