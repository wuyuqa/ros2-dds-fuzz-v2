"""Offline contracts for the F3 topology pair/scenario carrier.

Only the offline contract double is exercised.  No ROS context, DDS domain,
namespace, or network namespace is created, and no SUT verdict is produced.
"""
import json
from pathlib import Path
import tempfile
import unittest

from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import (
    INPUT_SCHEMA,
    RESULT_SCHEMA,
    build_input_receipts,
    pair_delivery_fact,
    pair_graph_facts,
    run_topology,
    topology_capabilities,
)
from r2f.runtime.topology_worker import OfflineTopologyBackend


def qos(**overrides):
    value = model._qos()
    value.update(overrides)
    return value


def endpoint(logical_id, role, topic="/r2f/topology/a", **overrides):
    value = {"logical_id": logical_id, "role": role, "topic": topic,
             "type_name": "std_msgs/msg/String", "qos": qos(),
             "vendor": None, "participant_id": None}
    value.update(overrides)
    return value


def action(action_id, op, endpoint_id, generation, **params):
    return {"action_id": action_id, "op": op, "endpoint_id": endpoint_id,
            "generation": generation, "params": params}


def program(endpoints, actions, **environment):
    env = {"backend_available": True, "transport_ready": True, "discovery_enabled": True,
           "type_matching": "exact_name", "filter_support": None, "filter_validity": None}
    env.update(environment)
    return {"schema": model.SCHEMA, "endpoints": endpoints, "actions": actions,
            "environment": env}


CFG = {"backend": "offline_fake", "run_id": "offline-topology", "domain_id": 191,
       "source_repo": "/tmp/offline-src"}


class Harness(unittest.TestCase):
    def run_program(self, prog, backend=None, **overrides):
        cfg = {**CFG, **overrides}
        output = tempfile.mkdtemp()
        return run_topology(prog, cfg, output, backend=backend)


class CardinalityAndBoundary(Harness):
    def test_zero_endpoint_program_executes_without_fake_facts(self):
        prog = model.generate_program(seed=1, writers=0, readers=0)
        result = self.run_program(prog)
        self.assertEqual(result["status"], "executed")
        self.assertEqual(result["pair_facts"], [])
        self.assertEqual(result["delivery_facts"], [])
        self.assertFalse(result["real_execution"])
        self.assertFalse(result["facts"]["execution_evidence"])

    def test_every_role_cardinality_including_empty_roles_executes(self):
        for writers, readers in ((0, 1), (1, 0), (1, 2), (2, 1), (4, 4)):
            prog = model.generate_program(seed=writers * 10 + readers,
                                          writers=writers, readers=readers)
            result = self.run_program(prog)
            self.assertEqual(result["status"], "executed", (writers, readers, result["reason"]))
            self.assertEqual(len(result["pair_facts"]), writers * readers)
            self.assertEqual(len(result["delivery_facts"]), writers * readers)
            self.assertTrue(all(row["status"] in
                                ("matched", "unmatched", "incompatible", "unknown")
                                for row in result["pair_facts"]))

    def test_mixed_vendor_and_mismatched_topics_are_per_pair_facts(self):
        prog = program(
            [endpoint("w0", "writer"), endpoint("r0", "reader"),
             endpoint("r1", "reader", topic="/r2f/topology/b")],
            [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
             action("c3", "create", "r1", 1),
             action("s", "snapshot", None, None, label="probe")])
        result = self.run_program(prog)
        pairs = {(row["writer"], row["reader"]): row for row in result["pair_facts"]}
        self.assertEqual(pairs[("w0", "r0")]["status"], "matched")
        self.assertEqual(pairs[("w0", "r1")]["status"], "incompatible")
        self.assertEqual(pairs[("w0", "r1")]["reasons"], ["topic_mismatch"])
        for row in result["pair_facts"]:
            self.assertFalse(row["candidate_eligible"])
            # Offline facts are contract-test observations, never evidence.
            self.assertFalse(row["real_ros"])
            self.assertFalse(row["execution_evidence"])
            self.assertEqual(row["source"], "offline_fake_backend_not_ros")


class LifecycleAndGenerations(Harness):
    def test_recreate_rebinds_fresh_identity_and_keeps_generations_separate(self):
        prog = program(
            [endpoint("w0", "writer"), endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
             action("p1", "publish_batch", "w0", 1, count=3, start_sequence=0),
             action("s1", "snapshot", None, None, label="gen1"),
             action("rc", "recreate", "r0", 2),
             action("p2", "publish_batch", "w0", 1, count=2, start_sequence=3),
             action("s2", "snapshot", None, None, label="gen2")])
        result = self.run_program(prog)
        self.assertEqual(result["status"], "executed", result["reason"])
        history = result["facts"]["endpoints"]["r0"]["history"]
        created = [row for row in history if row["event"] == "created"]
        retired = [row for row in history if row["event"] == "retired"]
        self.assertEqual([row["generation"] for row in created], [1, 2])
        self.assertEqual([row["generation"] for row in retired], [1])
        self.assertNotEqual(created[0]["endpoint_gid"], created[1]["endpoint_gid"])
        self.assertNotEqual(created[0]["pid"], created[1]["pid"])
        # Neither generation may borrow the other's identity.
        gids = [row["endpoint_gid"] for row in created]
        self.assertEqual(len(set(gids)), 2)
        deliveries = result["delivery_facts"]
        self.assertTrue(deliveries)
        for row in deliveries:
            self.assertEqual(row["writer_generation"], 1)
            self.assertIn(row["reader_generation"], (1, 2))
        self.assertEqual(result["facts"]["endpoints"]["r0"]["current_generation"], 2)

    def test_create_on_second_generation_is_rejected_without_side_effects(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1), action("c2", "create", "w0", 2)])
        result = self.run_program(prog)
        self.assertEqual(result["status"], "executed")
        receipts = [r for r in result["precondition_receipts"]
                    if r["step_id"] == "action:c2"]
        self.assertEqual(receipts[0]["disposition"], "guard_reject")
        self.assertTrue(receipts[0]["no_side_effect"])
        self.assertEqual(receipts[0]["code"], "first_generation_guard")
        self.assertEqual(result["facts"]["endpoints"]["w0"]["current_generation"], 1)

    def test_recreate_without_a_previous_generation_is_rejected(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("rc", "recreate", "w0", 1)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:rc")
        self.assertEqual(receipt["disposition"], "guard_reject")
        self.assertEqual(receipt["code"], "next_generation_guard")
        self.assertFalse(result["facts"]["endpoints"]["w0"]["alive"])

    def test_recreate_must_advance_exactly_one_generation(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1), action("rc", "recreate", "w0", 3)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:rc")
        self.assertEqual(receipt["disposition"], "guard_reject")
        self.assertEqual(result["facts"]["endpoints"]["w0"]["current_generation"], 1)

    def test_destroy_then_create_same_name_cannot_reuse_the_generation(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1), action("d1", "destroy", "w0", 1),
             action("c2", "create", "w0", 1)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:c2")
        self.assertEqual(receipt["disposition"], "guard_reject")
        self.assertEqual(receipt["code"], "first_generation_guard")
        self.assertFalse(result["facts"]["endpoints"]["w0"]["alive"])

    def test_destroy_of_a_stale_generation_is_rejected(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1), action("d1", "destroy", "w0", 2)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:d1")
        self.assertEqual(receipt["code"], "current_generation_alive_guard")
        self.assertTrue(result["facts"]["endpoints"]["w0"]["alive"])

    def test_destroy_cleans_up_and_is_recorded(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1), action("d1", "destroy", "w0", 1)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:d1")
        self.assertEqual(receipt["disposition"], "applied")
        self.assertEqual(receipt["after"]["alive"], False)
        self.assertIn("cleanup_status", receipt["cleanup"])
        self.assertFalse(result["facts"]["endpoints"]["w0"]["alive"])


class PublicationAndDelivery(Harness):
    def batch_program(self, payload=None, interval=None, count=4, start=0, second=None):
        params = {"count": count, "start_sequence": start}
        if payload is not None:
            params["payload"] = payload
        if interval is not None:
            params["interval_sec"] = interval
        actions = [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
                   action("p1", "publish_batch", "w0", 1, **params),
                   action("s1", "snapshot", None, None, label="after")]
        if second:
            actions.append(action("p2", "publish_batch", "w0", 1, **second))
        return program([endpoint("w0", "writer"), endpoint("r0", "reader")], actions)

    def test_batch_publish_records_sequences_per_writer_generation(self):
        result = self.run_program(self.batch_program())
        publication = next(r for r in result["precondition_receipts"]
                           if r["step_id"] == "action:p1")
        self.assertEqual(publication["disposition"], "applied")
        receipt = publication["publish_receipt"]
        self.assertEqual(receipt["sequences"], [0, 1, 2, 3])
        self.assertEqual(receipt["order_scope"], "writer_generation")
        self.assertFalse(receipt["cross_writer_order_required"])
        self.assertEqual(receipt["controller_ack_count"], 1)
        self.assertFalse(receipt["per_sample_ack"])
        self.assertEqual(result["facts"]["cross_writer_order_required"], False)
        self.assertEqual(result["facts"]["per_sample_controller_ack"], False)

    def test_payload_is_bound_rather_than_silently_rewritten(self):
        result = self.run_program(self.batch_program(payload="topology"))
        publication = next(r for r in result["precondition_receipts"]
                           if r["step_id"] == "action:p1")
        self.assertEqual(publication["publish_receipt"]["payload_binding"],
                         "declared_payload_embedded")

    def test_overlapping_sequence_range_is_rejected(self):
        result = self.run_program(self.batch_program(count=3, second={"count": 2,
                                                                    "start_sequence": 2}))
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:p2")
        self.assertEqual(receipt["disposition"], "guard_reject")
        self.assertEqual(receipt["code"], "nonoverlapping_writer_generation_sequence")
        self.assertTrue(receipt["no_side_effect"])

    def test_interval_beyond_bounded_input_is_a_guard_not_a_sut_bug(self):
        # The default model budget caps interval_sec at 30s, so only a widened
        # (still bounded) budget can express an interval this worker refuses.
        prog = self.batch_program(interval=60.0)
        prog["budget"] = {"max_timeout_sec": 120}
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:p1")
        self.assertEqual(receipt["disposition"], "capability_reject")
        self.assertEqual(receipt["code"], "publish_interval_out_of_bounds")
        self.assertTrue(receipt["no_side_effect"])

    def test_publish_on_a_reader_is_rejected(self):
        prog = program(
            [endpoint("r0", "reader")],
            [action("c1", "create", "r0", 1),
             action("p1", "publish_batch", "r0", 1, count=1, start_sequence=0)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:p1")
        self.assertEqual(receipt["code"], "writer_generation_alive_guard")

    def test_multi_writer_sequences_are_per_writer_and_not_globally_ordered(self):
        prog = program(
            [endpoint("w0", "writer"), endpoint("w1", "writer"), endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1), action("c2", "create", "w1", 1),
             action("c3", "create", "r0", 1),
             action("p1", "publish_batch", "w0", 1, count=2, start_sequence=0),
             action("p2", "publish_batch", "w1", 1, count=2, start_sequence=0),
             action("s", "snapshot", None, None, label="both")])
        result = self.run_program(prog)
        streams = {}
        for row in result["precondition_receipts"]:
            if row.get("publish_receipt"):
                streams[row["publish_receipt"]["writer_id"]] = \
                    row["publish_receipt"]["sequences"]
        self.assertEqual(streams, {"w0": [0, 1], "w1": [0, 1]})
        for row in result["delivery_facts"]:
            self.assertFalse(row["cross_writer_order_required"])

    def test_expected_delivery_is_not_padded_from_aggregate_counts(self):
        # A reader that never receives must report missing sequences, not a
        # count-derived success.
        prog = program(
            [endpoint("w0", "writer"), endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
             action("p1", "publish_batch", "w0", 1, count=3, start_sequence=0),
             action("s", "snapshot", None, None, label="x")])
        backend = OfflineTopologyBackend(auto_delivery=False)
        result = self.run_program(prog, backend=backend)
        delivery = result["delivery_facts"]
        self.assertTrue(delivery)
        for row in delivery:
            self.assertEqual(row["status"], "missing")
            self.assertEqual(row["received_sequences"], [])
            self.assertEqual(row["missing_sequences"], [0, 1, 2])
            self.assertEqual(row["identity_basis"], "unknown")

    def test_ambiguous_attribution_stays_unknown_without_message_info_gid(self):
        pair = {"writer": "w0", "reader": "r0", "writer_generation": 1,
                "reader_generation": 1, "writer_gid": "aa", "reader_gid": "bb",
                "status": "matched", "reader_created_monotonic_ns": 0}
        events = [{"event": "send", "logical_id": "w0", "generation": 1,
                   "send_sequence": 0, "monotonic_ns": 1},
                  {"event": "receive", "logical_id": "r0", "generation": 1,
                   "send_sequence": 0, "publisher_gid": None}]
        fact = pair_delivery_fact(pair, events, active_writers_on_topic=2)
        self.assertEqual(fact["status"], "unknown")
        self.assertEqual(fact["identity_basis"], "unknown")
        self.assertEqual(fact["unknown_attribution_receipts"], 1)

    def test_single_writer_correlation_is_labelled_as_correlation(self):
        pair = {"writer": "w0", "reader": "r0", "writer_generation": 1,
                "reader_generation": 1, "writer_gid": "aa", "reader_gid": "bb",
                "status": "matched", "reader_created_monotonic_ns": 0}
        events = [{"event": "send", "logical_id": "w0", "generation": 1,
                   "send_sequence": 0, "monotonic_ns": 1},
                  {"event": "receive", "logical_id": "r0", "generation": 1,
                   "send_sequence": 0, "publisher_gid": None}]
        fact = pair_delivery_fact(pair, events, active_writers_on_topic=1)
        self.assertEqual(fact["status"], "complete")
        self.assertEqual(fact["identity_basis"], "endpoint_correlated_single_writer")

    def test_direct_gid_mismatch_never_counts_as_delivery(self):
        pair = {"writer": "w0", "reader": "r0", "writer_generation": 1,
                "reader_generation": 1, "writer_gid": "aa", "reader_gid": "bb",
                "status": "matched", "reader_created_monotonic_ns": 0}
        events = [{"event": "send", "logical_id": "w0", "generation": 1,
                   "send_sequence": 0, "monotonic_ns": 1},
                  {"event": "receive", "logical_id": "r0", "generation": 1,
                   "send_sequence": 0, "publisher_gid": "cc"}]
        fact = pair_delivery_fact(pair, events, active_writers_on_topic=1)
        self.assertEqual(fact["received_sequences"], [])
        self.assertEqual(fact["status"], "missing")


class GraphFacts(Harness):
    def handles(self, backend):
        backend.prepare({"environment": {}}, {"run_id": "offline"})
        writer = backend.create_endpoint(endpoint("w0", "writer"), 1, "writer")
        reader = backend.create_endpoint(endpoint("r0", "reader"), 1, "reader")
        return writer, reader

    def test_incomplete_snapshot_is_unknown_not_unmatched(self):
        backend = OfflineTopologyBackend()
        writer, reader = self.handles(backend)
        snapshot = backend.snapshot({"w0": writer, "r0": reader})
        fact = pair_graph_facts(writer, reader, {"match_snapshot": None,
                                                 "graph_snapshot": None,
                                                 "complete": False},
                                snapshot["snapshots"]["r0"])
        self.assertEqual(fact["status"], "unknown")
        self.assertEqual(fact["reasons"], ["incomplete_snapshot"])

    def test_positive_count_without_bidirectional_gid_is_not_matched(self):
        backend = OfflineTopologyBackend(auto_match=False)
        writer, reader = self.handles(backend)
        backend.set_graph_edge("w0", "r0", writer_sees=True, reader_sees=False)
        snapshot = backend.snapshot({"w0": writer, "r0": reader})
        fact = pair_graph_facts(writer, reader, snapshot["snapshots"]["w0"],
                                snapshot["snapshots"]["r0"])
        self.assertEqual(fact["status"], "unknown")
        self.assertIn("count_positive_without_bidirectional_gid_evidence", fact["reasons"])

    def test_zero_count_with_a_visible_peer_is_unknown_not_unmatched(self):
        backend = OfflineTopologyBackend(auto_match=False)
        writer, reader = self.handles(backend)
        # Graph shows the peer, but the public count still reads zero.
        backend.set_graph_edge("w0", "r0", writer_sees=False, reader_sees=False)
        snapshot = backend.snapshot({"w0": writer, "r0": reader})
        # Force a graph entry without a matching count.
        snapshot["snapshots"]["w0"]["graph_snapshot"]["subscriptions"] = [
            {"endpoint_gid": reader["endpoint_gid"], "node_name": "offline_fake",
             "node_namespace": "/", "topic_type": reader["type_name"], "qos": {}}]
        fact = pair_graph_facts(writer, reader, snapshot["snapshots"]["w0"],
                                snapshot["snapshots"]["r0"])
        self.assertEqual(fact["status"], "unknown")
        self.assertIn("zero_count_contradicts_graph_entry", fact["reasons"])

    def test_matched_requires_both_public_counts(self):
        backend = OfflineTopologyBackend()
        writer, reader = self.handles(backend)
        snapshot = backend.snapshot({"w0": writer, "r0": reader})
        fact = pair_graph_facts(writer, reader, snapshot["snapshots"]["w0"],
                                snapshot["snapshots"]["r0"])
        self.assertEqual(fact["status"], "matched")
        self.assertEqual(fact["writer_count"], 1)
        self.assertEqual(fact["reader_count"], 1)
        self.assertFalse(fact["candidate_eligible"])


class Barriers(Harness):
    def test_matched_barrier_passes_and_delivery_barrier_uses_pairs(self):
        prog = program(
            [endpoint("w0", "writer"), endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
             action("b1", "barrier", None, None, condition="matched",
                    timeout_sec=1.0, endpoint_ids=["w0", "r0"]),
             action("p1", "publish_batch", "w0", 1, count=2, start_sequence=0),
             action("b2", "barrier", None, None, condition="delivery",
                    timeout_sec=1.0, endpoint_ids=["w0", "r0"])])
        result = self.run_program(prog)
        barriers = [r for r in result["observation_receipts"]
                    if r["step_id"] in ("action:b1", "action:b2")]
        self.assertEqual(len(barriers), 2)
        for receipt in barriers:
            self.assertTrue(receipt["satisfied"], receipt)
            self.assertFalse(receipt["candidate_eligible"])

    def test_barrier_timeout_is_an_observation_gap_not_a_verdict(self):
        prog = program(
            [endpoint("w0", "writer"), endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
             action("b1", "barrier", None, None, condition="delivery",
                    timeout_sec=0.05, endpoint_ids=["w0", "r0"])])
        result = self.run_program(prog)
        receipt = next(r for r in result["observation_receipts"]
                       if r["step_id"] == "action:b1")
        self.assertFalse(receipt["satisfied"])
        self.assertEqual(receipt["classification"],
                         "observation_timeout_not_sut_violation")
        self.assertEqual(receipt["cancellation_cleanup"], "no_blocking_worker_threads")
        self.assertTrue(any(entry["action_id"] == "b1" for entry in result["unknowns"]))
        self.assertEqual(result["runtime_status"], "completed_with_unknown")

    def test_barrier_over_destroyed_endpoints_is_a_guard_not_a_crash(self):
        prog = program(
            [endpoint("w0", "writer"), endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1), action("c2", "create", "r0", 1),
             action("d1", "destroy", "r0", 1),
             action("b1", "barrier", None, None, condition="matched",
                    timeout_sec=1.0, endpoint_ids=["w0", "r0"])])
        result = self.run_program(prog)
        self.assertEqual(result["status"], "executed", result["reason"])
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:b1")
        self.assertEqual(receipt["code"], "barrier_target_alive_guard")
        self.assertTrue(receipt["no_side_effect"])

    def test_snapshot_of_a_destroyed_generation_is_rejected(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1), action("d1", "destroy", "w0", 1),
             action("s1", "snapshot", "w0", 1, label="stale")])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:s1")
        self.assertEqual(receipt["code"], "snapshot_target_alive_guard")
        self.assertTrue(receipt["no_side_effect"])

    def test_quiescent_barrier_requires_two_stable_snapshots(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1),
             action("b1", "barrier", None, None, condition="quiescent",
                    timeout_sec=2.0, endpoint_ids=["w0"])])
        result = self.run_program(prog)
        receipt = next(r for r in result["observation_receipts"]
                       if r["step_id"] == "action:b1")
        self.assertTrue(receipt["satisfied"], receipt)
        self.assertGreaterEqual(receipt["after"]["attempts"], 1)


class CapabilityGuards(Harness):
    def test_each_unsupported_endpoint_field_is_a_capability_reject(self):
        cases = {
            "content_filter_invalid_field": endpoint(
                "r0", "reader", filter={"expression": "d.x", "parameters": [], "evaluation": True}),
            "qos_not_expressible": endpoint("w0", "writer", qos=qos(ownership="exclusive")),
            "partitions_not_expressible": endpoint("w0", "writer", qos=qos(partitions=["/p"])),
            "message_type_not_expressible": endpoint("w0", "writer",
                                                     type_name="std_msgs/msg/Int32"),
        }
        for code, ep in cases.items():
            prog = program([ep], [action("c1", "create", ep["logical_id"], 1)])
            result = self.run_program(prog)
            self.assertEqual(result["status"], "executed")
            receipts = [r for r in result["precondition_receipts"]
                        if r["step_id"] == "action:c1"]
            self.assertEqual(receipts[0]["disposition"], "capability_reject")
            self.assertEqual(receipts[0]["code"], code)
            self.assertTrue(receipts[0]["no_side_effect"])
            self.assertEqual(result["facts"]["endpoints"][ep["logical_id"]]["alive"], False)

    def test_assert_liveliness_requires_manual_by_topic(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1),
             action("a1", "assert_liveliness", "w0", 1)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:a1")
        self.assertEqual(receipt["disposition"], "guard_reject")
        self.assertEqual(receipt["code"], "public_writer_manual_by_topic")

    def test_assert_liveliness_on_manual_writer_reaches_the_public_api(self):
        prog = program(
            [endpoint("w0", "writer", qos=qos(liveliness="manual_by_topic"))],
            [action("c1", "create", "w0", 1),
             action("a1", "assert_liveliness", "w0", 1)])
        result = self.run_program(prog)
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:a1")
        self.assertEqual(receipt["disposition"], "effect_observed")
        self.assertTrue(receipt["satisfied"])
        self.assertEqual(receipt["effect_scope"], "assert_liveliness_api_action_only")
        lifecycle = receipt["lifecycle_evidence"]
        self.assertEqual(lifecycle["status"], "unknown")
        self.assertIsNone(lifecycle["loss_observed"])
        self.assertIsNone(lifecycle["recovery_observed"])
        self.assertIsNone(lifecycle["effect_observed"])
        self.assertFalse(lifecycle["candidate_eligible"])
        self.assertFalse(receipt["execution_evidence"])

    def test_final_facts_keep_liveliness_consumer_fail_closed(self):
        prog = program(
            [endpoint("w0", "writer", qos=qos(liveliness="manual_by_topic")),
             endpoint("r0", "reader")],
            [action("c1", "create", "w0", 1),
             action("c2", "create", "r0", 1),
             action("a1", "assert_liveliness", "w0", 1)])
        result = self.run_program(prog)
        observations = result["facts"]["liveliness_evidence"]
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["status"], "unknown")
        self.assertFalse(observations[0]["execution_evidence"])
        self.assertFalse(observations[0]["candidate_eligible"])
        self.assertTrue(any(row["event"] == "topology_liveliness_evidence"
                            for row in result["events"]))

    def test_assert_caller_claims_and_aggregate_counts_do_not_prove_recovery(self):
        class ClaimingBackend(OfflineTopologyBackend):
            def assert_liveliness(self, handle):
                value = super().assert_liveliness(handle)
                return {**value, "loss_observed": True, "recovery_observed": True,
                        "raw_ref": "caller-supplied", "alive_count": 1,
                        "not_alive_count_change": -1}
        prog = program(
            [endpoint("w0", "writer", qos=qos(liveliness="manual_by_topic"))],
            [action("c1", "create", "w0", 1),
             action("a1", "assert_liveliness", "w0", 1)])
        result = self.run_program(prog, backend=ClaimingBackend())
        receipt = next(r for r in result["precondition_receipts"]
                       if r["step_id"] == "action:a1")
        self.assertEqual(receipt["disposition"], "effect_observed")  # legacy API only
        self.assertEqual(receipt["lifecycle_evidence"]["status"], "unknown")
        self.assertIsNone(receipt["lifecycle_evidence"]["recovery_observed"])
        self.assertFalse(receipt["lifecycle_evidence"]["candidate_eligible"])

    def test_danging_endpoint_reference_is_blocked_by_structure_validation(self):
        prog = program(
            [endpoint("w0", "writer")],
            [action("c1", "create", "w0", 1),
             {"action_id": "x1", "op": "publish_batch", "endpoint_id": "missing",
              "generation": 1, "params": {"count": 1, "start_sequence": 0}}])
        self.assertEqual(model.validate_program(prog)["status"], "invalid")
        result = self.run_program(prog)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "blocked:invalid_program")
        self.assertEqual(result["events"], [])
        self.assertFalse(result["real_execution"])


class InputBindingAndEvidence(Harness):
    def test_input_receipt_binds_program_fingerprints_and_config(self):
        prog = model.generate_program(seed=2, writers=1, readers=1)
        receipts = build_input_receipts(prog, {"domain_id": 191, "rmw": "rmw_cyclonedds_cpp"})
        self.assertEqual(receipts["schema"], INPUT_SCHEMA)
        self.assertEqual(receipts["validation_status"], "valid")
        self.assertEqual(receipts["fingerprints"], model.fingerprints(prog))
        self.assertEqual(receipts["program_sha256"], model.fingerprints(prog)["input_sha256"])
        self.assertFalse(receipts["execution_evidence"])
        self.assertFalse(receipts["candidate_eligible"])

    def test_input_mutations_are_bound_before_execution(self):
        prog = model.generate_program(seed=4, writers=1, readers=1)
        result = self.run_program(prog, input_mutations=["change_topic"], seed=1)
        self.assertEqual(result["status"], "executed", result["reason"])
        chain = result["mutation_chain"]
        self.assertEqual(chain["layers"], 1)
        self.assertNotEqual(chain["initial_sha256"], chain["final_sha256"])
        self.assertEqual(result["input_receipts"]["program_sha256"], chain["final_sha256"])
        receipt = result["input_receipts"]["mutation_receipts"][0]
        self.assertEqual(receipt["mutator"], "change_topic")
        self.assertFalse(receipt["execution_evidence"])

    def test_structure_guard_mutation_never_reaches_a_side_effect(self):
        prog = model.generate_program(seed=6, writers=1, readers=1)
        result = self.run_program(prog, input_mutations=["structure_guard"], seed=1)
        if result["status"] == "blocked":
            self.assertEqual(result["reason"], "blocked:invalid_program")
            self.assertEqual(result["events"], [])
        else:
            # If the structural mutation happened to stay valid, execution is
            # still bound to the mutated hash rather than the original input.
            self.assertEqual(result["input_receipts"]["program_sha256"],
                             result["mutation_chain"]["final_sha256"])

    def test_offline_observation_receipts_never_claim_execution_evidence(self):
        prog = model.generate_program(seed=13, writers=1, readers=1)
        result = self.run_program(prog)
        for receipt in result["observation_receipts"]:
            self.assertFalse(receipt.get("real_ros", False), receipt["step_id"])
            if "execution_evidence" in receipt:
                self.assertFalse(receipt["execution_evidence"], receipt["step_id"])
        for receipt in result["precondition_receipts"]:
            if receipt.get("disposition") in ("applied", "effect_observed"):
                self.assertFalse(receipt["execution_evidence"], receipt["step_id"])

    def test_offline_result_is_never_execution_evidence_or_a_candidate(self):
        prog = model.generate_program(seed=8, writers=2, readers=2)
        result = self.run_program(prog)
        self.assertEqual(result["schema"], RESULT_SCHEMA)
        self.assertFalse(result["real_execution"])
        self.assertFalse(result["facts"]["execution_evidence"])
        self.assertFalse(result["candidate_eligible"])
        self.assertFalse(result["facts"]["candidate_eligible"])
        self.assertFalse(result["mature"])
        self.assertFalse(result["sealed"])
        self.assertIsNone(result["maturity_denominator"])
        self.assertFalse(result["facts"]["backend_is_ros"])
        self.assertTrue(result["facts"]["offline_fake"])
        self.assertFalse(result["mutation_receipt"]["applicable"])
        self.assertEqual(result["mutation_receipt"]["reason"],
                         "topology_backend_is_a_scenario_carrier_not_a_mutation")

    def test_raw_is_written_once_and_a_second_run_is_blocked(self):
        prog = model.generate_program(seed=9, writers=1, readers=1)
        output = tempfile.mkdtemp()
        first = run_topology(prog, CFG, output)
        self.assertEqual(first["status"], "executed")
        raw = Path(first["raw_path"])
        self.assertTrue(raw.is_file())
        lines = [json.loads(line) for line in raw.read_text().splitlines() if line.strip()]
        self.assertEqual(len(lines), len(first["events"]))
        self.assertEqual(first["raw_sha256"],
                         __import__("hashlib").sha256(raw.read_bytes()).hexdigest())
        second = run_topology(prog, CFG, output)
        self.assertEqual(second["status"], "blocked")
        self.assertEqual(second["reason"], "blocked:state_directory_already_executed")

    def test_execution_metadata_separates_input_and_execution(self):
        prog = model.generate_program(seed=10, writers=1, readers=1)
        result = self.run_program(prog)
        metadata = result["execution_metadata"]
        self.assertEqual(metadata["backend"], "offline_fake")
        self.assertFalse(metadata["real_ros"])
        self.assertFalse(metadata["execution_evidence"])
        self.assertEqual(metadata["input_program_sha256"],
                         result["input_receipts"]["program_sha256"])
        self.assertEqual(metadata["run_id"], CFG["run_id"])

    def test_capabilities_advertise_bounded_cardinality_and_honest_limits(self):
        caps = topology_capabilities()
        self.assertEqual(caps["max_endpoints"], 8)
        self.assertTrue(caps["variable_cardinality"])
        self.assertTrue(caps["empty_roles"])
        self.assertEqual(caps["supported_bypass_fields"]["content_filter"], {
            "status": "conditional", "scope": "initial_only", "roles": ["reader"],
            "message_type": "std_msgs/msg/String", "field_types": {"data": "string"},
            "requirements": ["public_creation_api", "rmw_acceptance",
                             "get_content_filter", "is_cft_enabled"],
            "sample_filter_effect": False, "dynamic_update": False, "lease": False,
        })
        self.assertFalse(caps["candidate_eligible"])
        self.assertFalse(caps["execution_evidence"])

    def test_invalid_program_is_blocked_before_any_endpoint(self):
        prog = model.generate_program(seed=11, writers=1, readers=1)
        prog["schema"] = "wrong-schema"
        result = self.run_program(prog)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "blocked:invalid_program")
        self.assertEqual(result["events"], [])
        self.assertFalse(result["real_execution"])

    def test_live_backend_is_refused_without_an_explicit_live_go(self):
        prog = model.generate_program(seed=12, writers=1, readers=1)
        result = self.run_program(prog, backend="ros2_public_api")
        self.assertEqual(result["backend"], "ros2_public_api")
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "blocked:no_live_ros_go")
        self.assertFalse(result["real_execution"])
        self.assertEqual(result["events"], [])
        self.assertFalse(result["live_go_receipt"]["provided"])
        self.assertFalse(result["live_go_receipt"]["execution_evidence"])


if __name__ == "__main__":
    unittest.main()
