"""Offline contracts for the S1 input-domain model (r2f.s1-input-domain/v1).

Nothing here starts ROS or claims an execution/hit.  These tests only verify
that the declaration, projection and input classification behave as stated.
"""
import json
import unittest

from r2f.modules.s1 import fuzz_catalog as catalog
from r2f.modules.s1 import input_model as model
from r2f.modules.s1 import topology_model
from r2f.modules.s1 import input_model


def domain():
    return model.default_domain()


class SchemaRoundTrip(unittest.TestCase):
    def test_default_domain_is_valid_and_round_trips(self):
        catalog_domain = domain()
        self.assertTrue(catalog_domain.validate())
        payload = catalog_domain.to_dict()
        self.assertEqual(payload["schema"], model.DOMAIN_SCHEMA)
        rebuilt = model.DomainCatalog.from_dict(json.loads(json.dumps(payload)))
        self.assertEqual(rebuilt.digest(), catalog_domain.digest())

    def test_manifest_separates_declaration_from_evidence(self):
        manifest = domain().manifest()
        self.assertFalse(manifest["execution_evidence"])
        self.assertFalse(manifest["candidate_eligible"])
        self.assertRegex(manifest["domain_sha256"], r"^[0-9a-f]{64}$")
        self.assertIn("S1.topology", manifest["surface_ids"])
        self.assertEqual(manifest["surface_count"], len(manifest["surface_ids"]))

    def test_digest_changes_when_a_declared_domain_changes(self):
        base = domain()
        payload = base.to_dict()
        for surface in payload["surfaces"]:
            if surface["surface_id"] == "P01":
                surface["slots"][0]["domain"]["max"] = 4
        changed = model.DomainCatalog.from_dict(payload)
        self.assertNotEqual(changed.digest(), base.digest())

    def test_unknown_and_missing_fields_are_named(self):
        surface = model.topology_surface().to_dict()
        surface["bogus_field"] = 1
        with self.assertRaisesRegex(ValueError, "unknown field"):
            model.SurfaceDomain.from_dict(surface)
        del surface["bogus_field"]
        del surface["slots"]
        with self.assertRaisesRegex(ValueError, "missing field"):
            model.SurfaceDomain.from_dict(surface)

    def test_undeclared_operator_and_guard_refs_are_rejected(self):
        surface = model.topology_surface().to_dict()
        surface["slots"][0]["operators"] = ["bogus.operator"]
        with self.assertRaisesRegex(ValueError, "unknown operator"):
            model.SurfaceDomain.from_dict(surface)
        surface = model.topology_surface().to_dict()
        surface["slots"][0]["guards"] = ["no_such_guard"]
        with self.assertRaisesRegex(ValueError, "unknown guards"):
            model.SurfaceDomain.from_dict(surface)

    def test_combination_cannot_reference_an_undeclared_operator(self):
        surface = model.topology_surface().to_dict()
        surface["combinations"].append(
            {"operators": ["param.enum", "env.reorder"], "rationale": "x", "status": "declared"})
        with self.assertRaisesRegex(ValueError, "undeclared operators"):
            model.SurfaceDomain.from_dict(surface)

    def test_chain_length_is_bounded(self):
        surface = model.topology_surface().to_dict()
        surface["chain_max"] = 9
        with self.assertRaisesRegex(ValueError, "chain_max"):
            model.SurfaceDomain.from_dict(surface)


class CatalogProjection(unittest.TestCase):
    def test_projection_preserves_catalog_ids_and_bindings(self):
        projected = model.domain_from_catalog()
        for surface_id, surface in catalog.SURFACES.items():
            entry = projected.get(surface_id)
            self.assertEqual(entry.version, surface.version, surface_id)
            self.assertEqual(entry.layer, surface.layer, surface_id)
            self.assertEqual(len(entry.slots), max(1, len(surface.input_paths)), surface_id)
            for slot in entry.slots:
                self.assertIn(slot.input_path, surface.input_paths or ["input"])
        # A surface with a known runtime binding reports bound; a pending one does not.
        self.assertEqual(projected.get("P01").slots[0].binding["status"], "bound")
        self.assertEqual(projected.get("E02").slots[0].binding["status"], "generation_only")

    def test_audit_refs_are_preserved_but_not_generated_as_inputs(self):
        entry = model.domain_from_catalog().get("P01")
        slot = entry.slots[0]
        self.assertEqual(slot.audit_refs["effect_id"], catalog.SURFACES["P01"].effect_id)
        self.assertEqual(slot.audit_refs["rule_ids"], list(catalog.SURFACES["P01"].rule_ids))
        # Audit refs never become input paths.
        for ref in slot.audit_refs["fact_ids"]:
            self.assertNotIn(ref, slot.input_path)

    def test_projection_does_not_mutate_the_catalog(self):
        before = catalog.catalog_digest()
        model.default_domain()
        self.assertEqual(catalog.catalog_digest(), before)


class InputClassification(unittest.TestCase):
    def test_authored_catalog_seeds_are_legal_or_explicitly_pending(self):
        seeds = catalog.load_seeds()
        catalog_domain = domain()
        for surface_id in seeds["authored"]:
            surface = catalog_domain.get(surface_id)
            result = model.classify_input(seeds["seeds"][surface_id]["logical_config"], surface)
            self.assertIn(result["expected_disposition"],
                          {"expect_pass", "unknown_not_pass"}, surface_id)
            self.assertEqual(result["unbound_fields"], [], surface_id)
            self.assertFalse(result["execution_evidence"])
            self.assertFalse(result["candidate_eligible"])
        # E02/E03/G01 remain unknown: declared, not executable here.
        for surface_id in ("E02", "E03", "G01"):
            surface = catalog_domain.get(surface_id)
            result = model.classify_input(seeds["seeds"][surface_id]["logical_config"], surface)
            self.assertEqual(result["expected_disposition"], "unknown_not_pass", surface_id)

    def test_unknown_leaf_fields_are_reported_not_dropped(self):
        surface = domain().get("P01")
        document = {"reader_delay_sec": 0.15, "mystery_knob": 7}
        result = model.classify_input(document, surface)
        self.assertEqual(result["expected_disposition"], "unknown_not_pass")
        self.assertEqual(result["unbound_fields"], ["mystery_knob"])

    def test_out_of_domain_value_is_a_guard_expectation(self):
        surface = domain().get("P02")
        document = {"qos": {"reliability": "sometimes"}}
        result = model.classify_input(document, surface)
        self.assertEqual(result["expected_disposition"], "expect_guard_reject")
        self.assertFalse(result["candidate_eligible"])

    def test_topology_program_classification_matches_the_model(self):
        program = topology_model.generate_program(3, writers=1, readers=1)
        result = model.classify_input(program, domain().get("S1.topology"))
        self.assertEqual(result["expected_disposition"], "expect_pass")
        self.assertEqual(result["validation"]["status"], "valid")

    def test_topology_guard_negative_is_an_expectation_not_a_bug(self):
        program = topology_model.generate_program(3, writers=1, readers=1)
        broken = topology_model.mutate_program(program, "dependency_guard", seed=1)["program"]
        result = model.classify_input(broken, domain().get("S1.topology"))
        self.assertEqual(result["expected_disposition"], "expect_guard_reject")
        self.assertEqual(result["validation"]["guard_disposition"], "guard_violation")

    def test_topology_unknown_field_is_unknown_not_pass(self):
        program = topology_model.generate_program(3, writers=1, readers=1)
        program["surprise"] = True
        result = model.classify_input(program, domain().get("S1.topology"))
        self.assertEqual(result["expected_disposition"], "unknown_not_pass")
        self.assertIn("program.surprise", result["unbound_fields"])

    def test_delivery_plan_without_runtime_consumer_is_unknown(self):
        from r2f.modules.s1 import fuzz_mutators_v2 as mutators
        surface = domain().get("S1.delivery")
        result = model.classify_input(mutators.delivery_plan_template(), surface,
                                      document_kind="delivery")
        self.assertEqual(result["expected_disposition"], "unknown_not_pass")
        self.assertTrue(result["runtime_observation_required"])

    def test_delivery_plan_out_of_domain_value_is_rejected(self):
        from r2f.modules.s1 import fuzz_mutators_v2 as mutators
        surface = domain().get("S1.delivery")
        document = mutators.delivery_plan_template()
        document["delivery_plan"]["delay_sec"] = 10_000.0
        result = model.classify_input(document, surface, document_kind="delivery")
        self.assertEqual(result["expected_disposition"], "expect_guard_reject")

    def test_classification_never_claims_execution(self):
        surface = domain().get("P01")
        for result in (model.classify_input({"reader_delay_sec": 0.15}, surface),
                       model.classify_input({"nope": 1}, surface),
                       model.classify_input([], surface)):
            self.assertEqual(result["evidence_status"], "not_executed")
            self.assertFalse(result["execution_evidence"])
            self.assertFalse(result["candidate_eligible"])


class PathHelpers(unittest.TestCase):
    def test_dotted_and_indexed_paths(self):
        document = {"endpoints": [{"qos": {"depth": 8}}, {"qos": {"depth": 16}}]}
        self.assertEqual(model.get_path(document, "endpoints[1].qos.depth"), 16)
        model.set_path(document, "endpoints[0].qos.depth", 64)
        self.assertEqual(document["endpoints"][0]["qos"]["depth"], 64)
        with self.assertRaises(KeyError):
            model.get_path(document, "endpoints[9].qos.depth")

    def test_leaf_paths_and_coverage(self):
        document = {"a": 1, "b": {"c": [1, 2]}}
        self.assertEqual(model.leaf_paths(document), ["a", "b.c[0]", "b.c[1]"])
        self.assertTrue(model.covered("b.c[0]", ["b"]))
        self.assertTrue(model.covered("endpoints[2].qos.depth", ["endpoints[*].qos.depth"]))
        self.assertFalse(model.covered("endpoints[2].qos.reliability",
                                       ["endpoints[*].qos.depth"]))

    def test_slot_selector_is_explicit_about_ambiguity(self):
        surface = domain().get("W01")
        slot = model.slot_for(surface, input_path="qos.depth")
        self.assertEqual(slot.slot_id, "W01.qos.depth")
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            model.slot_for(surface, operator_id="param.boundary")
        with self.assertRaises(KeyError):
            model.slot_for(surface, slot_id="no.such.slot")


class OracleAuditBindings(unittest.TestCase):
    def test_new_surfaces_name_oracles_without_claiming_verdict_evidence(self):
        domain = input_model.default_domain()
        manifest = domain.manifest()
        self.assertEqual(manifest["oracle_audit"], {
            "kind": "bindings_only", "binding_resolution": "unresolved_no_resolver",
            "independent_verdict": False})
        self.assertFalse(manifest["execution_evidence"])
        self.assertFalse(manifest["candidate_eligible"])
        for surface_id in ("S1.topology", "S1.delivery"):
            surface = domain.get(surface_id)
            ids = {oid for slot in surface.slots
                   for oid in slot.audit_refs.get("oracle_ids", [])}
            self.assertTrue(ids, surface_id)
            for value in ids:
                self.assertTrue(value.startswith("r2f.oracle."))

    def test_oracle_reference_is_not_an_input_leaf(self):
        domain = input_model.default_domain()
        for surface_id in ("S1.topology", "S1.delivery"):
            surface = domain.get(surface_id)
            leaves = {slot.input_path for slot in surface.slots}
            self.assertFalse(any("oracle" in path for path in leaves), surface_id)


if __name__ == "__main__":
    unittest.main()
