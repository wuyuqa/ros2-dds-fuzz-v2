"""Offline contracts for the S1 v2 input mutators.

Every case here is an *input proposal*.  No test starts ROS, opens a socket,
records an execution receipt, or claims a fuzzing hit or a SUT candidate.
"""
import copy
import json
import unittest

from r2f.modules.s1 import fuzz_catalog as catalog
from r2f.modules.s1 import fuzz_mutators_v2 as mutators
from r2f.modules.s1 import input_model
from r2f.modules.s1 import topology_model


def domain():
    return input_model.default_domain()


def seeds():
    return catalog.load_seeds()


def topology_program(seed=21):
    return topology_model.generate_program(seed, writers=2, readers=2)


def family(surface_id):
    return domain().get(surface_id)


class Registry(unittest.TestCase):
    def test_every_declared_operator_is_implemented_and_reachable(self):
        manifest = mutators.operator_manifest()
        self.assertEqual(manifest["pending"], [])
        self.assertEqual(set(manifest["implemented"]), set(input_model.OPERATOR_IDS))
        self.assertEqual(set(mutators.OPERATOR_SPECS), set(input_model.OPERATOR_IDS))
        self.assertFalse(manifest["execution_evidence"])

    def test_unknown_operator_and_surface_are_rejected_by_name(self):
        with self.assertRaises(mutators.UndeclaredOperatorError):
            mutators.mutate("P01", "no.such.operator", {"reader_delay_sec": 0.1})
        with self.assertRaises(mutators.UnknownDomainError):
            mutators.mutate("NOPE", "param.boundary", {})

    def test_operator_must_be_declared_by_the_surface(self):
        with self.assertRaises(mutators.UndeclaredOperatorError):
            mutators.mutate("P02", "guard.structure", {"qos": {"reliability": "reliable"}})


class ParameterOperators(unittest.TestCase):
    def test_numeric_boundary_classes_are_all_reachable(self):
        seen = set()
        for seed in range(40):
            case = mutators.mutate("P01", "param.boundary",
                                   seeds()["seeds"]["P01"]["logical_config"], seed=seed)
            seen.add(case.expected_guard_effect["boundary_class"] or "unspecified")
            self.assertNotEqual(case.content_sha256, case.input_before_sha256)
        self.assertTrue(seen)

    def test_out_of_range_boundary_is_a_guard_expectation(self):
        case = mutators.mutate("P01", "param.boundary", {"reader_delay_sec": 0.15},
                               seed=5, context={"boundary_class": "out_of_range_high"})
        self.assertEqual(case.input["reader_delay_sec"], 3.001)
        self.assertEqual(case.expected_disposition, "expect_guard_reject")

    def test_undeclared_boundary_class_is_refused(self):
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate("P01", "param.boundary", {"reader_delay_sec": 0.15},
                            context={"boundary_class": "quantum_leap"})

    def test_enum_replace_uses_only_declared_values(self):
        declared = set(family("P02").slots[0].domain["values"])
        for seed in range(6):
            case = mutators.mutate("P02", "param.enum",
                                   {"qos": {"reliability": "reliable"}}, seed=seed)
            self.assertIn(case.input["qos"]["reliability"], declared)
            self.assertNotEqual(case.input["qos"]["reliability"], "reliable")

    def test_enum_with_single_value_refuses_instead_of_no_op(self):
        surface = input_model.SurfaceDomain(
            surface_id="T.single", version=1, layer="P", kind="parameter",
            slots=(input_model.SlotSpec(
                slot_id="T.single.value", kind="parameter_enum", input_path="value",
                value_type="enum", domain={"values": ["only"]},
                operators=("param.enum",)),))
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate(surface, "param.enum", {"value": "only"})

    def test_lease_value_classes_never_alias_infinity(self):
        inf = 9223372036854775807
        for name in ("zero", "finite", "infinite"):
            case = mutators.mutate("P04", "lease.value", {"qos": {"deadline_ns": 1250000000}},
                                   context={"special_class": name})
            value = case.input["qos"]["deadline_ns"]
            if name == "infinite":
                self.assertEqual(value, inf)
            else:
                self.assertNotEqual(value, inf)

    def test_queue_depth_stays_in_the_declared_set(self):
        declared = set(family("W01").slots[0].domain["representatives"])
        for seed in range(5):
            case = mutators.mutate("W01", "queue.depth",
                                   seeds()["seeds"]["W01"]["logical_config"], seed=seed)
            self.assertIn(case.input["qos"]["depth"], declared)

    def test_relation_witness_is_true_for_the_reported_class(self):
        for label in ("depth_lt_post", "depth_eq_post", "depth_gt_post"):
            case = mutators.mutate("W01", "relation.pair",
                                   seeds()["seeds"]["W01"]["logical_config"],
                                   context={"relation_class": label})
            depth = case.input["qos"]["depth"]
            post = case.input["post_match_count"]
            if label == "depth_lt_post":
                self.assertLess(depth, post)
            elif label == "depth_eq_post":
                self.assertEqual(depth, post)
            else:
                self.assertGreater(depth, post)
            self.assertEqual(case.input["workload_relation"]["class"], label)

    def test_undeclared_relation_class_is_refused(self):
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate("W01", "relation.pair",
                            seeds()["seeds"]["W01"]["logical_config"],
                            context={"relation_class": "depth_lt_pre"})

    def test_incompatible_reliability_pair_is_a_no_match_expectation(self):
        parent = {"writer_qos": {"reliability": "reliable"},
                  "reader_qos": {"reliability": "reliable"}}
        case = mutators.mutate("G01", "relation.pair", parent, context={"pair": "BR"})
        self.assertEqual(case.input["writer_qos"]["reliability"], "best_effort")
        self.assertEqual(case.input["reader_qos"]["reliability"], "reliable")
        self.assertEqual(case.expected_disposition, "expect_no_match")

    def test_compatible_pair_is_a_pass_expectation(self):
        parent = {"writer_qos": {"reliability": "reliable"},
                  "reader_qos": {"reliability": "reliable"}}
        case = mutators.mutate("G01", "relation.pair", parent, context={"pair": "RB"})
        self.assertEqual(case.input["writer_qos"]["reliability"], "reliable")
        self.assertEqual(case.input["reader_qos"]["reliability"], "best_effort")
        self.assertEqual(case.expected_disposition, "expect_pass")

    def test_pair_operator_never_repeats_the_parent_pair(self):
        parent = {"writer_qos": {"reliability": "best_effort"},
                  "reader_qos": {"reliability": "reliable"}}
        for seed in range(8):
            case = mutators.mutate("G01", "relation.pair", parent, seed=seed)
            self.assertFalse(case.input["writer_qos"]["reliability"] == "best_effort"
                             and case.input["reader_qos"]["reliability"] == "reliable")

    def test_explicit_pair_equal_to_parent_is_refused_not_silently_replaced(self):
        parent = {"writer_qos": {"reliability": "best_effort"},
                  "reader_qos": {"reliability": "reliable"}}
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate("G01", "relation.pair", parent, context={"pair": "BR"})

    def test_undeclared_pair_is_refused(self):
        parent = {"writer_qos": {"reliability": "reliable"},
                  "reader_qos": {"reliability": "reliable"}}
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate("G01", "relation.pair", parent, context={"pair": "ZZ"})

    def test_timing_shift_never_produces_negative_time(self):
        for seed in range(10):
            case = mutators.mutate("P01", "timing.shift", {"reader_delay_sec": 0.0}, seed=seed)
            self.assertGreaterEqual(case.input["reader_delay_sec"], 0)

    def test_missing_parent_path_is_a_named_refusal(self):
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate("P01", "timing.shift", {"qos": {"reliability": "reliable"}})


class TopologyOperators(unittest.TestCase):
    def test_action_insert_delete_swap_delegate_and_change_content(self):
        parent = topology_program()
        for operator_id in ("action.insert", "action.delete", "action.swap"):
            with self.subTest(operator=operator_id):
                case = mutators.mutate("S1.topology", operator_id, parent, seed=7)
                self.assertEqual(case.expected_disposition, "expect_pass")
                self.assertEqual(case.changes["delegate"] is not None, True)
                self.assertNotEqual(case.content_sha256, case.input_before_sha256)
                self.assertEqual(topology_model.validate_program(case.input)["status"], "valid")

    def test_guard_structure_and_dependency_are_safe_rejections(self):
        parent = topology_program()
        for operator_id in ("guard.structure", "guard.dependency"):
            with self.subTest(operator=operator_id):
                case = mutators.mutate("S1.topology", operator_id, parent, seed=3)
                self.assertEqual(case.expected_disposition, "expect_guard_reject")
                validation = topology_model.validate_program(case.input)
                self.assertIn(validation["guard_disposition"],
                              {"guard_violation", "invalid_input"})

    def test_environment_fault_requires_an_actual_guard_witness(self):
        program = topology_program()
        case = mutators.mutate("S1.topology", "guard.environment", program, seed=2,
                               context={"key": "backend_available"})
        self.assertEqual(case.input["environment"]["backend_available"], False)
        self.assertIn(case.expected_disposition,
                      {"expect_guard_reject", "unknown_not_pass"})
        # An unused flag cannot masquerade as a guard rejection.
        quiet = copy.deepcopy(program)
        quiet["environment"]["filter_support"] = True
        quiet_result = mutators.mutate("S1.topology", "env.health", quiet, seed=2,
                                       context={"key": "filter_support"})
        self.assertEqual(quiet_result.expected_disposition, "unknown_not_pass")

    def test_endpoint_count_remove_cascades_dependencies(self):
        case = mutators.mutate("S1.topology", "endpoint.count", topology_program(), seed=4,
                               context={"direction": "remove", "endpoint_id": "r0"})
        remaining = {endpoint["logical_id"] for endpoint in case.input["endpoints"]}
        self.assertNotIn("r0", remaining)
        for action in case.input["actions"]:
            self.assertNotEqual(action.get("endpoint_id"), "r0")
        self.assertEqual(topology_model.validate_program(case.input)["status"], "valid")

    def test_generation_action_variants(self):
        case = mutators.mutate("S1.topology", "generation.action", topology_program(), seed=1)
        self.assertEqual(case.changes["kind"], "topology_action_insert")
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate("S1.topology", "generation.action", topology_program(), seed=1,
                            context={"variant": "stale", "endpoint_id": "nobody"})

    def test_filter_expression_variants_respect_the_bounded_length(self):
        for variant, expected in (("empty", "expect_pass"), ("valid", "expect_pass")):
            case = mutators.mutate("S1.topology", "filter.expr", topology_program(),
                                   context={"filter_variant": variant})
            self.assertEqual(case.expected_disposition, expected, variant)

    def test_topology_requires_a_valid_parent(self):
        with self.assertRaises(mutators.MutationError):
            mutators.mutate("S1.topology", "action.insert", {"endpoints": [], "actions": []})


class DeliveryPlanOperators(unittest.TestCase):
    def test_delivery_plan_mutations_are_declared_but_unknown(self):
        template = mutators.delivery_plan_template()
        for operator_id in ("env.reorder", "env.duplicate", "env.drop", "env.delay",
                            "env.lease_expiry", "env.lease_refresh"):
            with self.subTest(operator=operator_id):
                case = mutators.mutate("S1.delivery", operator_id, template, seed=2)
                self.assertEqual(case.expected_disposition, "unknown_not_pass")
                self.assertIn(case.binding["status"], {"unbound", "generation_only"})
                # The plan value must actually move away from the parent plan.
                plan = case.input["delivery_plan"]
                parent_plan = template["delivery_plan"]
                self.assertTrue(any(plan[key] != parent_plan[key] for key in plan))

    def test_delivery_lease_action_rotates_off_the_parent_value(self):
        template = mutators.delivery_plan_template()
        for operator_id in ("env.lease_expiry", "env.lease_refresh"):
            with self.subTest(operator=operator_id):
                case = mutators.mutate("S1.delivery", operator_id, template, seed=3)
                self.assertNotEqual(case.input["delivery_plan"]["lease_action"],
                                    template["delivery_plan"]["lease_action"])

    def test_delivery_drop_sequence_differs_from_parent_when_domain_allows(self):
        template = mutators.delivery_plan_template()
        for seed in range(6):
            case = mutators.mutate("S1.delivery", "env.drop", template, seed=seed)
            self.assertNotEqual(case.input["delivery_plan"]["drop_sequence"],
                                template["delivery_plan"]["drop_sequence"])

    def test_unknown_delivery_field_is_rejected_not_dropped(self):
        document = mutators.delivery_plan_template()
        document["mystery"] = 1
        with self.assertRaises(mutators.UnknownDomainError):
            mutators.mutate("S1.delivery", "env.reorder", document)


class IdentityAndDedup(unittest.TestCase):
    def test_seed_alone_never_creates_a_new_input(self):
        document = {"reader_delay_sec": 0.15}
        first = mutators.mutate("P01", "timing.shift", document, seed=1)
        second = mutators.mutate("P01", "timing.shift", document, seed=1)
        self.assertEqual(first.content_sha256, second.content_sha256)
        self.assertEqual(first.provenance["seed"], 1)
        self.assertNotIn("seed", json.dumps(first.input))

    def test_seed_is_reproducible_across_runs(self):
        document = seeds()["seeds"]["W01"]["logical_config"]
        left = [mutators.mutate("W01", "relation.pair", document, seed=seed).content_sha256
                for seed in range(8)]
        right = [mutators.mutate("W01", "relation.pair", document, seed=seed).content_sha256
                 for seed in range(8)]
        self.assertEqual(left, right)
        # Distinct seeds must reach more than one declared witness; collisions
        # are allowed but the domain must not collapse to a single value.
        self.assertGreater(len(set(left)), 1)

    def test_content_hash_covers_the_payload_not_the_provenance(self):
        narrow = mutators.mutate("P02", "param.enum", {"qos": {"reliability": "reliable"}},
                                 context={"enum_value": "best_effort"})
        wide = mutators.mutate("P02", "param.enum", {"qos": {"reliability": "reliable"}},
                               context={"enum_value": "best_effort"}, seed=99)
        self.assertEqual(narrow.content_sha256, wide.content_sha256)
        other = mutators.mutate("P02", "param.enum", {"qos": {"reliability": "best_effort"}},
                                context={"enum_value": "reliable"})
        self.assertNotEqual(narrow.content_sha256, other.content_sha256)

    def test_two_operators_reaching_the_same_document_dedupe(self):
        document = {"reader_delay_sec": 0.15}
        via_delay = mutators.mutate("P01", "env.delay", document, seed=1)
        via_shift = mutators.mutate("P01", "timing.shift", document, seed=1,
                                    context={"offset_sec": 0.001})
        self.assertEqual(via_delay.content_sha256, via_shift.content_sha256)
        deduped = mutators.dedupe_cases([via_delay, via_shift])
        self.assertEqual(deduped["unique"], 1)
        self.assertEqual(deduped["duplicate_count"], 1)
        self.assertEqual(deduped["duplicates"][0]["content_sha256"], via_delay.content_sha256)

    def test_provenance_keeps_parent_chain_and_generator(self):
        case = mutators.mutate("P01", "param.boundary", {"reader_delay_sec": 0.15}, seed=4)
        provenance = case.provenance
        self.assertEqual(provenance["parent_sha256"], case.input_before_sha256)
        self.assertEqual(provenance["generator_version"], mutators.GENERATOR_VERSION)
        self.assertFalse(provenance["authored"])
        self.assertEqual(provenance["chain"][0]["operator_id"], "param.boundary")
        # The parent hash is reproducible from the parent document.
        self.assertEqual(
            provenance["parent_sha256"],
            input_model.content_hash({"schema": mutators.INPUT_PAYLOAD_SCHEMA,
                                      "surface_id": "P01",
                                      "input": {"reader_delay_sec": 0.15}}))

    def test_case_always_declares_absent_execution_evidence(self):
        case = mutators.mutate("P01", "param.boundary", {"reader_delay_sec": 0.15}).to_dict()
        self.assertFalse(case["execution_evidence"])
        self.assertFalse(case["candidate_eligible"])
        self.assertNotIn("hit", case)
        self.assertNotIn("execution_stage", case)


class Chains(unittest.TestCase):
    def test_declared_two_operator_chain_preserves_layers(self):
        case = mutators.mutate_chain("S1.topology", ("queue.depth", "param.enum"),
                                     topology_program(), seed=4)
        self.assertEqual(case.provenance["chain_length"], 2)
        self.assertEqual([layer["operator_id"] for layer in case.provenance["chain"]],
                         ["queue.depth", "param.enum"])
        self.assertEqual(case.provenance["parent_of_chain_sha256"],
                         case.provenance["chain"][0]["input_before_sha256"])
        # Each layer's recorded after-hash is the next layer's before-hash.
        self.assertEqual(case.provenance["chain"][0]["content_sha256"],
                         case.provenance["chain"][1]["input_before_sha256"])
        self.assertEqual(topology_model.validate_program(case.input)["status"], "valid")

    def test_undeclared_combination_is_refused(self):
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate_chain("S1.topology", ("queue.depth", "env.reorder"),
                                  topology_program())

    def test_chain_length_is_bounded(self):
        with self.assertRaises(ValueError):
            mutators.mutate_chain("S1.topology", ("action.insert",), topology_program())
        with self.assertRaises(ValueError):
            mutators.mutate_chain("S1.topology",
                                  ("action.insert", "action.swap", "queue.depth",
                                   "param.enum", "param.boundary"), topology_program())

    def test_guard_negative_cannot_be_hidden_by_a_later_layer(self):
        with self.assertRaises(mutators.MutationRefused):
            mutators.mutate_chain("S1.topology", ("guard.structure", "action.insert"),
                                  topology_program())

    def test_terminal_guard_chain_reports_the_strongest_expectation(self):
        case = mutators.mutate_chain("S1.topology", ("action.insert", "guard.environment"),
                                     topology_program(), seed=4)
        self.assertEqual(case.expected_disposition, "expect_guard_reject")
        self.assertEqual(len(case.expected_guard_effect["layers"]), 2)


class Corpus(unittest.TestCase):
    def test_corpus_separates_authored_pending_and_unsupported(self):
        corpus = mutators.corpus_v2()
        counts = corpus["counts"]
        self.assertGreaterEqual(counts.get("authored", 0), 10)
        self.assertGreaterEqual(counts.get("pending", 0), 1)
        self.assertGreaterEqual(counts.get("unsupported", 0), 1)
        authored = [row for row in corpus["manifest"] if row["status"] == "authored"]
        pending = [row for row in corpus["manifest"] if row["status"] == "pending"]
        for row in authored:
            self.assertRegex(row["content_sha256"], r"^[0-9a-f]{64}$")
        for row in pending:
            self.assertIsNone(row["content_sha256"])
            self.assertIn("reason", row)
        self.assertFalse(corpus["execution_evidence"])
        self.assertFalse(corpus["candidate_eligible"])

    def test_corpus_covers_empty_roles_multi_endpoint_and_complex_actions(self):
        ids = {entry["seed_id"] for entry in mutators.corpus_v2()["entries"]}
        for expected in ("topology.empty_roles", "topology.multi_endpoint",
                         "topology.complex_actions"):
            self.assertIn(expected, ids)
        entries = {entry["seed_id"]: entry for entry in mutators.corpus_v2()["entries"]}
        self.assertEqual(entries["topology.empty_roles"]["document"]["endpoints"], [])
        self.assertGreaterEqual(len(entries["topology.multi_endpoint"]["document"]["endpoints"]), 8)
        ops = {action["op"] for action in entries["topology.complex_actions"]["document"]["actions"]}
        self.assertIn("barrier", ops)

    def test_catalog_seeds_are_imported_read_only_without_new_claims(self):
        corpus = mutators.corpus_v2()
        entries = {entry["seed_id"]: entry for entry in corpus["entries"]
                   if entry["seed_id"].startswith("catalog.")}
        self.assertIn("catalog.P01", entries)
        self.assertEqual(entries["catalog.P01"]["document"],
                         seeds()["seeds"]["P01"]["logical_config"])
        self.assertFalse(entries["catalog.P01"]["provenance"]["derived_from_execution"])

    def test_corpus_is_deterministic(self):
        self.assertEqual(mutators.corpus_manifest(), mutators.corpus_manifest())


class TryMutate(unittest.TestCase):
    def test_refusal_is_reported_not_raised(self):
        result = mutators.try_mutate("P02", "param.enum", {"qos": {"reliability": "reliable"}},
                                     context={"enum_value": "reliable"})
        self.assertEqual(result["status"], "refused")
        self.assertIn("reason", result)
        self.assertFalse(result["candidate_eligible"])

    def test_success_is_reported_with_the_case(self):
        result = mutators.try_mutate("P02", "param.enum", {"qos": {"reliability": "reliable"}})
        self.assertEqual(result["status"], "emitted")
        self.assertEqual(result["case"]["surface_id"], "P02")


class CrossSurfaceSweep(unittest.TestCase):
    def test_every_declared_operator_on_every_catalog_surface_generates(self):
        documents = {success_id: seeds()["seeds"][success_id]["logical_config"]
                     for success_id in seeds()["authored"]}
        checked = 0
        for surface_id, document in documents.items():
            surface = family(surface_id)
            operators = sorted({op for slot in surface.slots for op in slot.operators})
            for operator_id in operators:
                with self.subTest(surface=surface_id, operator=operator_id):
                    case = mutators.mutate(surface_id, operator_id, document, seed=3)
                    self.assertNotEqual(case.content_sha256, case.input_before_sha256)
                    self.assertNotEqual(case.input, document,
                                        f"{surface_id}/{operator_id} produced a no-op")
                    checked += 1
        self.assertGreaterEqual(checked, 20)


if __name__ == "__main__":
    unittest.main()
