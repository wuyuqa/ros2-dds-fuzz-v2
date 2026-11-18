"""Offline contract tests for the G01/W01 relation surface input model.

No ROS, no DDS, no network, no campaign and no execution receipt is produced
here.  The synthetic policy values exercise input modelling only.
"""
import unittest

from r2f.modules.s1 import relation_surfaces as rs


class FieldCapability(unittest.TestCase):
    def test_unavailable_fields_are_named_not_invented(self):
        caps = rs.field_capabilities()
        self.assertEqual(set(caps["unavailable_fields"]),
                         {"ownership", "ownership_strength", "partitions"})
        for name in caps["unavailable_fields"]:
            field = caps["fields"][name]
            self.assertFalse(field["public_api"])
            self.assertEqual(field["disposition"], "unavailable_api")
            self.assertIn("QoSProfile", field["reason"])

    def test_match_policy_split_excludes_history_and_depth(self):
        caps = rs.field_capabilities()
        self.assertIn("reliability", caps["match_policy_fields"])
        self.assertIn("durability", caps["match_policy_fields"])
        self.assertIn("deadline_ns", caps["match_policy_fields"])
        self.assertIn("liveliness", caps["match_policy_fields"])
        self.assertIn("liveliness_lease_duration_ns", caps["match_policy_fields"])
        self.assertEqual(set(caps["non_match_policy_fields"]), {"history", "depth"})

    def test_no_ownership_through_any_public_slot(self):
        for field in ("ownership", "ownership_strength", "partitions"):
            self.assertNotIn(field, rs.POLICY_FIELDS[:7])


class PolicyRequestDisposition(unittest.TestCase):
    def test_legal_enum_and_duration_values_are_accepted_for_observation(self):
        for policy in (
            {"reliability": "best_effort"},
            {"durability": "transient_local"},
            {"deadline_ns": 1},
            {"deadline_ns": rs.INFINITE_QOS_NS},
            {"liveliness": "manual_by_topic"},
            {"liveliness_lease_duration_ns": 1_000_000},
            {"history": "keep_last", "depth": 1},
        ):
            result = rs.evaluate_policy_request("writer", policy)
            self.assertEqual(result["disposition"], "accepted", policy)
            self.assertFalse(result["sut_failure"])
            self.assertTrue(result["candidate_eligible"])

    def test_illegal_inputs_are_guard_rejected_not_sut_failures(self):
        cases = {
            "invalid_enum_value": {"reliability": "eventual"},
            "invalid_integer_type": {"depth": "8"},
            "depth_zero_keep_last_system_default": {"depth": 0},
            "integer_out_of_range": {"depth": 10001},
            "duration_out_of_range": {"deadline_ns": -1},
            "invalid_duration_type": {"deadline_ns": 1.5},
            "unknown_policy_field": {"lifespan_ns": 5},
            "depth_not_applicable_for_keep_all": {"history": "keep_all", "depth": 8},
            "depth_missing_for_keep_last": {"history": "keep_last"},
        }
        for expected_code, policy in cases.items():
            result = rs.evaluate_policy_request("reader", policy)
            self.assertEqual(result["disposition"], "guard_reject", policy)
            self.assertIn(expected_code, [reason["code"] for reason in result["reasons"]])
            self.assertFalse(result["sut_failure"])
            self.assertFalse(result["candidate_eligible"])

    def test_unavailable_field_is_not_silently_dropped(self):
        result = rs.evaluate_policy_request("writer", {"reliability": "reliable",
                                                       "ownership": "exclusive"})
        self.assertEqual(result["disposition"], "unavailable_api")
        self.assertEqual(result["unavailable_fields"], ["ownership"])
        self.assertFalse(result["sut_failure"])
        self.assertFalse(result["candidate_eligible"])
        self.assertIn("field_not_exposed_by_public_api",
                      [reason["code"] for reason in result["reasons"]])
        record = [item for item in result["field_receipts"] if item["field"] == "ownership"][0]
        self.assertFalse(record["accepted"])
        self.assertEqual(record["disposition"], "unavailable_api")

    def test_keep_all_is_binding_pending_until_worker_supports_it(self):
        result = rs.evaluate_policy_request("writer", {"history": "keep_all"})
        self.assertEqual(result["disposition"], "binding_pending")
        self.assertIn("history_keep_all_requires_worker_binding",
                      [reason["code"] for reason in result["reasons"]])
        self.assertFalse(result["candidate_eligible"])
        supported = rs.evaluate_policy_request("writer", {"history": "keep_all"},
                                               supported_history=("keep_last", "keep_all"))
        self.assertEqual(supported["disposition"], "accepted")

    def test_binding_receipts_name_real_api_and_hash_content(self):
        receipts = rs.policy_binding_receipts("writer", {"reliability": "best_effort",
                                                         "deadline_ns": 7})
        self.assertEqual(receipts[0]["api"], "rclpy.qos.QoSProfile.deadline")
        self.assertEqual(receipts[1]["api"], "rclpy.qos.QoSProfile.reliability")
        for receipt in receipts:
            self.assertEqual(receipt["value_sha256"], rs.canonical_sha256(receipt["value"]))
            self.assertTrue(receipt["config_path"].startswith("writer_qos."))


class G01Matrix(unittest.TestCase):
    def test_all_four_pairs_are_enumerated_with_distinct_identity(self):
        plans = rs.enumerate_g01_matrix()
        self.assertEqual([plan["pair_class"] for plan in plans],
                         ["RR", "RB", "BB", "BR"])
        self.assertEqual(len({plan["pair_key_sha256"] for plan in plans}), 4)
        for plan in plans:
            self.assertFalse(plan["execution_evidence"])
            self.assertFalse(plan["candidate_eligible"])
            self.assertTrue(plan["creation_success_is_not_a_bug"])
            self.assertTrue(plan["input_accepted"])
            self.assertEqual(plan["pair_class"] in rs.G01_INCOMPATIBLE_PAIRS,
                             plan["guard_expectation"] == "no_match_or_delivery")

    def test_br_is_an_incompatible_relation_not_an_invalid_value(self):
        plans = {plan["pair_class"]: plan for plan in rs.enumerate_g01_matrix()}
        self.assertEqual(plans["BR"]["writer_request"]["disposition"], "accepted")
        self.assertEqual(plans["BR"]["reader_request"]["disposition"], "accepted")
        self.assertEqual(plans["BR"]["declared_relation"]["status"], "incompatible")
        self.assertEqual(plans["RR"]["declared_relation"]["status"], "compatible")
        self.assertEqual(plans["RB"]["declared_relation"]["status"], "compatible")

    def test_durability_liveliness_deadline_not_silently_ignored(self):
        plan = rs.enumerate_g01_matrix(
            pairs=("RR",),
            base_writer={"durability": "volatile", "liveliness": "automatic"},
            base_reader={"durability": "transient_local", "liveliness": "manual_by_topic"},
        )[0]
        fields = plan["declared_relation"]["fields"]
        self.assertEqual(fields["durability"]["status"], "incompatible")
        self.assertEqual(fields["liveliness"]["status"], "incompatible")
        self.assertEqual(fields["reliability"]["status"], "compatible")
        self.assertEqual(plan["declared_relation"]["status"], "incompatible")

    def test_missing_side_is_unknown_never_compatible(self):
        view = rs.field_relation("durability", "transient_local", None)
        self.assertEqual(view["status"], "unknown")
        both_missing = rs.relation_compatibility({"reliability": "reliable"},
                                                 {"reliability": "reliable"},
                                                 fields=["durability"])
        self.assertEqual(both_missing["status"], "unknown")

    def test_unavailable_fields_in_plan_are_marked(self):
        plan = rs.enumerate_g01_matrix(pairs=("RR",), include_unavailable=True)[0]
        self.assertEqual(plan["writer_request"]["disposition"], "unavailable_api")
        self.assertFalse(plan["input_accepted"])
        self.assertIn("ownership", plan["writer_request"]["unavailable_fields"])

    def test_depth_cannot_masquerade_as_match_policy(self):
        view = rs.field_relation("depth", 8, 8)
        self.assertEqual(view["status"], "not_a_match_policy")
        self.assertIn("cannot prove or disprove", view["reason"])


class W01Plans(unittest.TestCase):
    BASE = {"qos": {"depth": 8, "reliability": "reliable", "durability": "volatile"},
            "pre_match_count": 2, "post_match_count": 6}

    def test_relation_classes_change_content_and_record_binding(self):
        for depth, expected in ((3, "depth_lt_post"), (6, "depth_eq_post"),
                                (9, "depth_gt_post")):
            plan = rs.build_w01_mutation(self.BASE, {"depth": depth,
                                                     "pre_match_count": 2,
                                                     "post_match_count": 6})
            self.assertTrue(plan["content_changed"])
            self.assertNotEqual(plan["input_before_sha256"],
                                plan["input_after_sha256"])
            self.assertIn("qos.depth", plan["change_paths"])
            self.assertEqual(plan["guard"]["relation_class"], expected)
            self.assertEqual(plan["after"]["qos"]["depth"], depth)
            # Only depth_lt_post depends on the unreviewed delivery obligation.
            if expected == "depth_lt_post":
                self.assertEqual(plan["guard"]["oracle_applicability"], "review_required")
                self.assertFalse(plan["guard"]["candidate_eligible"])
            else:
                self.assertEqual(plan["guard"]["oracle_applicability"], "applicable")
                self.assertTrue(plan["guard"]["candidate_eligible"])
            self.assertTrue(rs.audit_plan(plan)["ok"])

    def test_equivalent_relation_reports_override_not_fake_change(self):
        plan = rs.build_w01_mutation(self.BASE, {"depth": 8,
                                                 "pre_match_count": 2,
                                                 "post_match_count": 6})
        self.assertFalse(plan["content_changed"])
        self.assertEqual(plan["input_before_sha256"], plan["input_after_sha256"])
        self.assertEqual(plan["change_paths"], [])
        self.assertTrue(rs.audit_plan(plan)["ok"])

    def test_reviewed_delivery_rule_makes_depth_lt_post_applicable(self):
        plan = rs.build_w01_mutation(self.BASE, {"depth": 3, "pre_match_count": 2,
                                                 "post_match_count": 6},
                                     delivery_rule_reviewed=True)
        self.assertEqual(plan["guard"]["oracle_applicability"], "applicable")
        self.assertTrue(plan["guard"]["candidate_eligible"])

    def test_depth_zero_and_bad_counts_are_guard_rejected(self):
        for request, code in (
            ({"depth": 0, "pre_match_count": 2, "post_match_count": 6},
             "depth_zero_keep_last_system_default"),
            ({"depth": 1, "pre_match_count": 0, "post_match_count": 6},
             "w01_count_below_minimum"),
            ({"depth": 1, "pre_match_count": 2, "post_match_count": 2},
             "w01_count_below_minimum"),
            ({"depth": 10001, "pre_match_count": 2, "post_match_count": 6},
             "w01_depth_out_of_range"),
        ):
            result = rs.evaluate_w01_request(request)
            self.assertEqual(result["disposition"], "guard_reject", request)
            self.assertIn(code, [reason["code"] for reason in result["reasons"]])
            self.assertFalse(result["sut_failure"])

    def test_keep_all_is_binding_pending_and_depth_must_be_omitted(self):
        pending = rs.evaluate_w01_request({"history": "keep_all", "pre_match_count": 2,
                                           "post_match_count": 6})
        self.assertEqual(pending["disposition"], "binding_pending")
        rejected = rs.evaluate_w01_request({"history": "keep_all", "depth": 8,
                                            "pre_match_count": 2, "post_match_count": 6})
        self.assertEqual(rejected["disposition"], "guard_reject")
        self.assertIn("depth_not_applicable_for_keep_all",
                      [reason["code"] for reason in rejected["reasons"]])

    def test_declared_relation_class_must_match_values(self):
        result = rs.evaluate_w01_request({"depth": 3, "pre_match_count": 2,
                                          "post_match_count": 6,
                                          "relation_class": "depth_gt_post"})
        self.assertEqual(result["disposition"], "guard_reject")
        self.assertIn("relation_class_mismatch",
                      [reason["code"] for reason in result["reasons"]])

    def test_unsupported_mutator_is_rejected_explicitly(self):
        with self.assertRaises(rs.RelationSurfaceError) as ctx:
            rs.build_w01_mutation(self.BASE, {"depth": 3, "pre_match_count": 2,
                                              "post_match_count": 6},
                                  mutator_id="M03")
        self.assertEqual(ctx.exception.reason_code, "unimplemented_w01_mutator")


class Chains(unittest.TestCase):
    BASE = {"qos": {"depth": 8}, "writer_qos": {"reliability": "reliable"},
            "reader_qos": {"reliability": "reliable"},
            "pre_match_count": 2, "post_match_count": 6}

    def test_g01_then_w01_chain_records_each_layer(self):
        chain = rs.plan_relation_chain(self.BASE, [
            {"surface_id": "G01", "mutator_id": "M08",
             "parameters": {"pair_relation": "BR"}},
            {"surface_id": "W01", "mutator_id": "M04",
             "parameters": {"depth": 3, "pre_match_count": 1, "post_match_count": 6}},
        ])
        self.assertEqual(chain["disposition"], "accepted")
        self.assertEqual([step["surface_id"] for step in chain["steps"]], ["G01", "W01"])
        self.assertTrue(chain["all_steps_changed_content"])
        self.assertTrue(chain["steps"][0]["changes"]["writer_qos.reliability"]["after"] == "best_effort")
        self.assertEqual(chain["final_state"]["qos"]["depth"], 3)
        self.assertFalse(chain["execution_evidence"])
        self.assertTrue(rs.audit_plan(chain)["ok"])

    def test_overriding_layer_is_flagged_not_double_counted(self):
        chain = rs.plan_relation_chain(self.BASE, [
            {"surface_id": "G01", "mutator_id": "M08",
             "parameters": {"pair_relation": "BR"}},
            {"surface_id": "G01", "mutator_id": "M08",
             "parameters": {"pair_relation": "RR"}},
        ])
        self.assertEqual(chain["steps"][0]["guard"]["pair_class"], "BR")
        self.assertEqual(chain["steps"][1]["guard"]["pair_class"], "RR")
        self.assertTrue(chain["steps"][0]["content_changed"])
        self.assertTrue(chain["steps"][1]["content_changed"])

    def test_unknown_operation_surface_is_preserved_not_dropped(self):
        chain = rs.plan_relation_chain(self.BASE, [
            {"surface_id": "P99", "mutator_id": "M99", "parameters": {}},
        ])
        self.assertEqual(chain["disposition"], "rejected_unsupported_operation")
        self.assertEqual(chain["unsupported_operations"][0]["status"], "unknown_surface")
        self.assertEqual(chain["steps"], [])

    def test_declined_catalog_mutator_is_named_with_reason(self):
        chain = rs.plan_relation_chain(self.BASE, [
            {"surface_id": "W01", "mutator_id": "M03", "parameters": {}},
        ])
        entry = chain["unsupported_operations"][0]
        self.assertEqual(entry["status"], "declined_by_design")
        self.assertIn("must_not_substitute", entry["reason"])
        self.assertEqual(entry["implemented"], ["M04"])
        self.assertEqual(sorted(entry["declined_mutators"]), ["M03"])

    def test_g01_enum_mutator_is_declined(self):
        chain = rs.plan_relation_chain(self.BASE, [
            {"surface_id": "G01", "mutator_id": "M02", "parameters": {}},
        ])
        entry = chain["unsupported_operations"][0]
        self.assertEqual(entry["status"], "declined_by_design")
        self.assertIn("offered_requested", entry["reason"])

    def test_guard_blocked_chain_stops_before_following_layer(self):
        chain = rs.plan_relation_chain(self.BASE, [
            {"surface_id": "W01", "mutator_id": "M04",
             "parameters": {"depth": 0, "pre_match_count": 2, "post_match_count": 6}},
            {"surface_id": "G01", "mutator_id": "M08",
             "parameters": {"pair_relation": "RR"}},
        ])
        self.assertEqual(chain["disposition"], "guard_blocked")
        self.assertEqual(chain["steps"][0]["status"], "guard_blocked")
        self.assertEqual(len(chain["steps"]), 1)


class ReplacementPoints(unittest.TestCase):
    def test_projection_names_legacy_refusals_without_deleting_them(self):
        projection = rs.relation_surface_core_projection()
        self.assertEqual(projection["status"], "adapter_ready_pending_main_wiring")
        self.assertIn("fuzz_catalog.py:884", projection["replacement_points"]["W01"]["legacy_refusal"])
        self.assertIn("ros2_pair.py:990", projection["replacement_points"]["G01"]["legacy_refusal"])
        self.assertEqual(projection["unchanged_by_this_module"],
                         ["src/r2f/modules/s1/fuzz_catalog.py",
                          "src/r2f/runtime/ros2_pair.py",
                          "src/r2f/modules/s1/providers.py"])
        self.assertFalse(projection["execution_evidence"])

    def test_module_does_not_import_catalog_runtime_or_ros(self):
        import ast
        with open(rs.__file__, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
        self.assertEqual(imported, {"__future__", "copy", "hashlib", "json"})
        for banned in ("rclpy", "r2f.runtime.ros2_pair", "r2f.modules.s1.fuzz_catalog",
                       "r2f.modules.s1.providers"):
            self.assertNotIn(banned, imported)


if __name__ == "__main__":
    unittest.main()
