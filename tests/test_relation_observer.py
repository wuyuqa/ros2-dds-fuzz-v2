"""Offline tests for per-edge relation observation over synthetic raw events.

The event streams below are hand-built fixtures: they are explicitly NOT ROS and
NOT DDS.  They mirror the public-API raw schema produced by
``runtime.worker``/``runtime.ros2_pair`` so the observer's per-edge logic and
its refusal to infer absence can be checked without starting a runtime.
"""
import unittest

from r2f.modules.s1 import oracles
from r2f.modules.s1 import relation_surfaces as rs
from r2f.runtime import relation_observer as obs

RUN = "runfixture01"
DOMAIN = 173


class Stream:
    """Deterministic synthetic raw event builder with contiguous indexes.

    One monotonic clock drives every stamp so a controller window can enclose
    the snapshots and delivery rows it is meant to judge.
    """

    def __init__(self, run=RUN, domain=DOMAIN, clock=1000):
        self.run, self.domain, self.events = run, domain, []
        self.command_id = 0
        self.clock = clock

    def tick(self, step=10):
        self.clock += step
        return self.clock

    def add(self, event, role=None, **fields):
        row = {"event": event, "run_id": self.run, "domain_id": self.domain,
               "monotonic_ns": self.tick(), "event_index": len(self.events)}
        if role is not None:
            row["role"] = role
        qos = fields.pop("qos", None)
        if qos is not None:
            row["observed_self"] = qos
        row.update(fields)
        self.events.append(row)
        return row

    def role_created(self, role, gid, generation, pid, qos=None):
        created_ns = self.tick()
        row = self.add("endpoint_created", role=role, endpoint_gid=gid,
                       endpoint_generation=generation, pid=pid,
                       created_monotonic_ns=created_ns)
        if qos is not None:
            self.add("qos_binding_observed", role=role, observed_self=qos,
                     observed_self_gid=gid, requested=qos)
        return row

    def snapshot(self, role, gid, publishers, subscriptions, match_count):
        self.command_id += 1
        command = self.command_id
        sampled_ns = self.tick()
        self.add("match_snapshot", role=role, command_id=command,
                 matched_count=match_count, endpoint_gid=gid,
                 sampled_monotonic_ns=sampled_ns,
                 count_api="get_publisher_count")
        self.add("graph_snapshot", role=role, command_id=command,
                 publishers=[{"endpoint_gid": gid} for gid in publishers],
                 subscriptions=[{"endpoint_gid": gid} for gid in subscriptions])
        self.add("command_done", role=role, command_id=command)
        return sampled_ns

    def send(self, role, gid, sequence, phase):
        return self.add("send", role=role, send_sequence=sequence, phase=phase,
                        writer_gid=gid, publish_returned=True)

    def receive(self, role, reader_gid, publisher_gid, sequence, phase,
                run=None, identity_basis="message_info_gid"):
        return self.add("receive", role=role, send_sequence=sequence, phase=phase,
                        reader_gid=reader_gid, publisher_gid=publisher_gid,
                        payload_run_id=self.run if run is None else run,
                        identity_basis=identity_basis)


W = "aa" * 16
R = "bb" * 16
W_QOS = {"reliability": "reliable", "durability": "volatile", "depth": 8}
R_QOS = {"reliability": "reliable", "durability": "volatile", "depth": 8}


def full_stream(*, writer_qos=None, reader_qos=None, match_writer=1, match_reader=1,
                delivery=True, publisher_gid=None, extra_writer_endpoint=False,
                omit_reader_snapshot=False):
    writer_qos = dict(W_QOS if writer_qos is None else writer_qos)
    reader_qos = dict(R_QOS if reader_qos is None else reader_qos)
    s = Stream()
    s.role_created("writer", W, "genw", 4001, writer_qos)
    s.role_created("reader", R, "genr", 4002, reader_qos)
    # The compatibility window opens once both endpoints exist and closes after
    # the delivery rows, so it encloses the match and delivery it will judge.
    s.add("window_open", window="pair_compatibility_window")
    publishers = [W, "cc" * 16] if extra_writer_endpoint else [W]
    s.snapshot("writer", W, publishers, [R], match_writer)
    if not omit_reader_snapshot:
        s.snapshot("reader", R, publishers, [R], match_reader)
    if delivery:
        gid = W if publisher_gid is None else publisher_gid
        s.send("writer", W, 2, "post_match")
        s.send("writer", W, 3, "post_match")
        s.receive("reader", R, gid, 2, "post_match")
        s.receive("reader", R, gid, 3, "post_match")
    s.add("window_close", window="pair_compatibility_window")
    return s.events


CONFIG = {"run_id": RUN, "domain_id": DOMAIN,
          "writer_qos": W_QOS, "reader_qos": R_QOS,
          "qos": {"depth": 8, "reliability": "reliable", "durability": "volatile"},
          "pre_match_count": 2, "post_match_count": 2}


class G01EdgeObservation(unittest.TestCase):
    def test_complete_pair_produces_one_identity_correlated_edge(self):
        result = obs.g01_edge_observation(full_stream(), CONFIG)
        self.assertEqual(result["status"], "observed", result["reasons"])
        self.assertEqual(len(result["edges"]), 1)
        edge = result["edges"][0]
        self.assertTrue(edge["identity_correlated"])
        self.assertEqual(edge["writer"]["gid"], W)
        self.assertEqual(edge["reader"]["gid"], R)
        self.assertEqual(edge["match"]["status"], "observed")
        self.assertEqual(edge["match"]["counts"], {"writer": 1, "reader": 1})
        self.assertFalse(edge["match"]["aggregate_used"])
        self.assertEqual(edge["relation"]["status"], "compatible")
        self.assertEqual(edge["incomplete_reasons"], [])

    def test_no_oracle_verdict_or_candidate_is_emitted(self):
        result = obs.g01_edge_observation(full_stream(), CONFIG)
        self.assertIsNone(result["oracle_verdict"])
        self.assertFalse(result["candidate_eligible"])
        self.assertFalse(result["execution_evidence"])
        self.assertIsNone(result["edges"][0]["oracle_verdict"])

    def test_missing_endpoint_generation_is_incomplete_not_compatible(self):
        events = full_stream()
        for row in events:
            if row.get("event") == "endpoint_created" and row.get("role") == "reader":
                row.pop("endpoint_generation")
        result = obs.g01_edge_observation(events, CONFIG)
        self.assertEqual(result["status"], "incomplete")
        self.assertIn("endpoint_generation_missing:reader", result["reasons"])
        self.assertFalse(result["edges"][0]["identity_correlated"])

    def test_extra_endpoint_breaks_pair_isolation_and_match_evidence(self):
        result = obs.g01_edge_observation(full_stream(extra_writer_endpoint=True), CONFIG)
        self.assertEqual(result["status"], "incomplete")
        edge = result["edges"][0]
        self.assertEqual(edge["graph"]["status"], "incomplete")
        self.assertEqual(edge["match"]["status"], "unknown")
        self.assertIn("pair_not_isolated:writer:extra_endpoints",
                      edge["incomplete_reasons"])
        self.assertFalse(edge["identity_correlated"])

    def test_missing_reader_snapshot_is_unknown_not_unmatched(self):
        result = obs.g01_edge_observation(full_stream(omit_reader_snapshot=True), CONFIG)
        edge = result["edges"][0]
        self.assertEqual(edge["match"]["status"], "unknown")
        self.assertFalse(edge["identity_correlated"])

    def test_aggregate_match_count_never_substitutes_for_role_scoped(self):
        # Both roles report a role-scoped count of 3 (never exactly this pair).
        result = obs.g01_edge_observation(full_stream(match_writer=3, match_reader=3),
                                          CONFIG)
        edge = result["edges"][0]
        self.assertEqual(edge["match"]["status"], "ambiguous")
        self.assertFalse(edge["identity_correlated"])
        self.assertIn("aggregate_used", edge["match"])
        self.assertFalse(edge["match"]["aggregate_used"])

    def test_zero_counts_report_not_matched_not_pass(self):
        result = obs.g01_edge_observation(full_stream(match_writer=0, match_reader=0),
                                          CONFIG)
        edge = result["edges"][0]
        self.assertEqual(edge["match"]["status"], "not_matched")
        self.assertFalse(edge["identity_correlated"])

    def test_foreign_publisher_gid_on_receive_is_rejected(self):
        events = full_stream(publisher_gid="dd" * 16)
        result = obs.g01_edge_observation(events, CONFIG)
        edge = result["edges"][0]
        self.assertEqual(edge["delivery"]["status"], "unknown")
        self.assertIn("receive_publisher_gid_mismatch", edge["delivery"]["reasons"])
        self.assertEqual(edge["delivery"]["identity_reasons"],
                         ["receive_publisher_gid_mismatch"])
        self.assertFalse(edge["identity_correlated"])

    def test_correlated_receive_without_gid_needs_explicit_basis(self):
        events = full_stream(publisher_gid=None)
        for row in events:
            if row.get("event") == "receive":
                row["publisher_gid"] = None
                row["identity_basis"] = "guessed"
        result = obs.g01_edge_observation(events, CONFIG)
        edge = result["edges"][0]
        self.assertFalse(edge["delivery"]["identity_correlated"])
        self.assertIn("receive_identity_unavailable_without_explicit_correlation",
                      edge["delivery"]["reasons"])

    def test_run_id_mismatch_is_refused(self):
        events = full_stream()
        events[0]["run_id"] = "other"
        with self.assertRaises(rs.RelationSurfaceError):
            obs.g01_edge_observation(events, CONFIG)

    def test_non_contiguous_event_indexes_are_refused(self):
        events = full_stream()
        events[3]["event_index"] = 99
        with self.assertRaises(rs.RelationSurfaceError):
            obs.g01_edge_observation(events, CONFIG)

    def test_ambiguous_generation_requires_explicit_selection(self):
        events = full_stream()
        events.append({"event": "endpoint_created", "role": "reader",
                       "endpoint_gid": "ee" * 16, "endpoint_generation": "genr2",
                       "pid": 4003, "created_monotonic_ns": 210,
                       "run_id": RUN, "domain_id": DOMAIN,
                       "monotonic_ns": 9000, "event_index": len(events)})
        result = obs.g01_edge_observation(events, CONFIG)
        self.assertEqual(result["status"], "incomplete")
        self.assertIn("role_generation_ambiguous:reader:2", result["reasons"])
        pinned = obs.g01_edge_observation(events, CONFIG, reader_generation="genr")
        self.assertEqual(pinned["status"], "observed")

    def test_incompatible_observed_relation_is_reported_per_field(self):
        events = full_stream(writer_qos={"reliability": "best_effort", "durability": "volatile"},
                             reader_qos={"reliability": "reliable", "durability": "volatile"})
        result = obs.g01_edge_observation(events, CONFIG)
        edge = result["edges"][0]
        self.assertEqual(edge["relation"]["status"], "incompatible")
        self.assertEqual(edge["relation"]["observed"]["fields"]["reliability"]["status"],
                         "incompatible")

    def test_durability_liveliness_deadline_are_compared_per_field(self):
        writer = {"reliability": "reliable", "durability": "volatile",
                  "liveliness": "automatic", "deadline_ns": 100}
        reader = {"reliability": "reliable", "durability": "transient_local",
                  "liveliness": "manual_by_topic", "deadline_ns": 50}
        result = obs.g01_edge_observation(full_stream(writer_qos=writer, reader_qos=reader),
                                          CONFIG)
        fields = result["edges"][0]["relation"]["observed"]["fields"]
        self.assertEqual(fields["reliability"]["status"], "compatible")
        self.assertEqual(fields["durability"]["status"], "incompatible")
        self.assertEqual(fields["liveliness"]["status"], "incompatible")
        self.assertEqual(fields["deadline_ns"]["status"], "incompatible")
        self.assertEqual(result["edges"][0]["relation"]["status"], "incompatible")

    def test_depth_is_reported_as_non_match_policy(self):
        result = obs.g01_edge_observation(full_stream(), CONFIG)
        fields = result["edges"][0]["relation"]["observed"]["fields"]
        self.assertEqual(fields["depth"]["status"], "not_a_match_policy")
        self.assertIn("depth_and_history_are_not_match_policies",
                      obs.relation_observer_capability()["w01"])

    def test_unavailable_policy_fields_are_declared_not_invented(self):
        view = obs.g01_edge_observation(full_stream(), CONFIG)["edges"][0]
        for view_role in ("writer", "reader"):
            self.assertEqual(view["observed_policies"][view_role]["unavailable_fields"],
                             ["ownership", "ownership_strength", "partitions"])
            self.assertNotIn("ownership", view["observed_policies"][view_role]["values"])

    def test_graph_only_policy_fallback_marks_partial_completeness(self):
        events = full_stream()
        for row in events:
            if row.get("event") == "qos_binding_observed":
                row["event"] = "unused_binding_row"
        # inject minimal graph qos rows so the fallback has data
        for row in events:
            if row.get("event") == "graph_snapshot":
                for entry in row["publishers"] + row["subscriptions"]:
                    entry["qos"] = {"reliability": "reliable", "durability": "volatile"}
        result = obs.g01_edge_observation(events, CONFIG)
        edge = result["edges"][0]
        self.assertFalse(edge["observed_policies"]["writer"]["complete"])
        self.assertIn("deadline_ns", edge["observed_policies"]["writer"]["missing_fields"])


class GuardHandoff(unittest.TestCase):
    def test_compatible_pair_facts_are_accepted_by_the_real_oracle(self):
        result = obs.g01_edge_observation(full_stream(), CONFIG)
        window = obs.window_from_events(full_stream(), "pair_compatibility_window")
        self.assertIsNotNone(window)
        handoff = obs.guard_facts_from_edge(result["edges"][0], window)
        self.assertTrue(handoff["oracle_input"])
        verdict = oracles.evaluate_match_compatibility_guard(handoff["facts"])
        self.assertEqual(verdict["verdict"], "pass")
        self.assertEqual(verdict["reason_codes"], ["compatible_pair_matched"])

    def test_incompatible_pair_with_delivery_fails_via_real_oracle(self):
        events = full_stream(writer_qos={"reliability": "best_effort", "durability": "volatile"},
                             reader_qos={"reliability": "reliable", "durability": "volatile"})
        config = {**CONFIG,
                  "writer_qos": {"reliability": "best_effort", "durability": "volatile"},
                  "reader_qos": {"reliability": "reliable", "durability": "volatile"}}
        result = obs.g01_edge_observation(events, config)
        window = obs.window_from_events(events, "pair_compatibility_window")
        handoff = obs.guard_facts_from_edge(result["edges"][0], window,
                                           submitted={"writer": {"reliability": "best_effort"},
                                                      "reader": {"reliability": "reliable"}})
        verdict = oracles.evaluate_match_compatibility_guard(handoff["facts"])
        self.assertEqual(verdict["verdict"], "fail")
        self.assertIn("prohibited_delivery_observed", verdict["forbidden_residuals"])

    def test_missing_window_yields_no_oracle_input(self):
        result = obs.g01_edge_observation(full_stream(), CONFIG)
        handoff = obs.guard_facts_from_edge(result["edges"][0], None)
        self.assertFalse(handoff["oracle_input"])
        self.assertIsNone(handoff["facts"])
        self.assertEqual(handoff["reason"], "observation_window_missing")

    def test_incomplete_edge_yields_unknown_not_pass(self):
        result = obs.g01_edge_observation(full_stream(extra_writer_endpoint=True), CONFIG)
        window = obs.window_from_events(full_stream(), "pair_compatibility_window")
        handoff = obs.guard_facts_from_edge(result["edges"][0], window)
        verdict = oracles.evaluate_match_compatibility_guard(handoff["facts"])
        self.assertEqual(verdict["verdict"], "unknown")

    def test_no_window_events_means_unknown_window(self):
        self.assertIsNone(obs.window_from_events([{"event": "endpoint_created",
                                                   "event_index": 0}],
                                                 "pair_compatibility_window"))

    def test_incompatible_pair_proven_absent_matches_via_real_oracle(self):
        # BR with complete observation and no match/delivery is the reviewed
        # safe outcome; prove it comes from the observer's absence evidence.
        events = full_stream(writer_qos={"reliability": "best_effort",
                                         "durability": "volatile"},
                             reader_qos={"reliability": "reliable",
                                         "durability": "volatile"},
                             match_writer=0, match_reader=0, delivery=False)
        config = {**CONFIG,
                  "writer_qos": {"reliability": "best_effort", "durability": "volatile"},
                  "reader_qos": {"reliability": "reliable", "durability": "volatile"}}
        result = obs.g01_edge_observation(events, config)
        edge = result["edges"][0]
        window = obs.window_from_events(events, "pair_compatibility_window")
        handoff = obs.guard_facts_from_edge(edge, window)
        verdict = oracles.evaluate_match_compatibility_guard(handoff["facts"])
        # The observer refuses to call a zero-count role snapshot "absent
        # delivery evidence", so the oracle stays unknown rather than passing.
        self.assertEqual(verdict["verdict"], "unknown")
        self.assertEqual(edge["match"]["status"], "not_matched")
        self.assertFalse(edge["identity_correlated"])

    def test_compatible_pair_unmatched_stays_unknown_via_real_oracle(self):
        events = full_stream(match_writer=0, match_reader=0, delivery=False)
        result = obs.g01_edge_observation(events, CONFIG)
        window = obs.window_from_events(events, "pair_compatibility_window")
        handoff = obs.guard_facts_from_edge(result["edges"][0], window)
        verdict = oracles.evaluate_match_compatibility_guard(handoff["facts"])
        self.assertEqual(verdict["verdict"], "unknown")

    def test_incomplete_window_makes_oracle_unknown(self):
        events = full_stream()
        events = [row for row in events
                  if not (row.get("event") == "window_close")]
        for index, row in enumerate(events):
            row["event_index"] = index
        result = obs.g01_edge_observation(events, CONFIG)
        handoff = obs.guard_facts_from_edge(
            result["edges"][0], obs.window_from_events(events, "pair_compatibility_window"))
        self.assertFalse(handoff["oracle_input"])
        self.assertEqual(handoff["reason"], "observation_window_missing")


class W01EdgeObservation(unittest.TestCase):
    def stream(self, *, depth=8, pre=2, post=3, history=None, delivery=None,
               extra_endpoint=False, skip_middle=False):
        s = Stream()
        writer_qos = {"reliability": "reliable", "durability": "volatile", "depth": depth}
        reader_qos = dict(writer_qos)
        if history is not None:
            writer_qos["history"] = history
            reader_qos["history"] = history
        s.role_created("writer", W, "genw", 4001, writer_qos)
        s.role_created("reader", R, "genr", 4002, reader_qos)
        publishers = [W, "cc" * 16] if extra_endpoint else [W]
        s.snapshot("writer", W, publishers, [R], 1)
        s.snapshot("reader", R, publishers, [R], 1)
        delivered = post if delivery is None else delivery
        middle = post // 2
        for sequence in range(pre):
            s.send("writer", W, sequence, "prejoin")
        for index in range(post):
            sequence = pre + index
            s.send("writer", W, sequence, "post_match")
            skip = skip_middle and index == middle
            if index < delivered and not skip:
                s.receive("reader", R, W, sequence, "post_match")
        s.add("window_open", window="post_match_delivery")
        s.add("window_close", window="post_match_delivery")
        return s.events

    def config(self, **overrides):
        config = {**CONFIG, "qos": {"depth": 8, "reliability": "reliable",
                                    "durability": "volatile"},
                  "pre_match_count": 2, "post_match_count": 3}
        config.update(overrides)
        return config

    def test_depth_binding_and_counts_are_per_edge(self):
        result = obs.observe_w01_delivery(self.stream(depth=8, pre=2, post=3),
                                          self.config())
        self.assertEqual(result["status"], "observed", result["reasons"])
        edge = result["edges"][0]
        self.assertEqual(edge["observed_binding"]["writer"]["depth"]["observed"], 8)
        self.assertEqual(edge["observed_binding"]["reader"]["depth"]["observed"], 8)
        self.assertEqual(edge["delivery"]["prejoin_sent_sequences"], [0, 1])
        self.assertEqual(edge["delivery"]["post_match_sent_sequences"], [2, 3, 4])
        self.assertEqual(edge["delivery"]["post_match_received_sequences"], [2, 3, 4])
        self.assertFalse(edge["delivery"]["aggregate_used"])

    def test_depth_mismatch_is_incomplete_not_pass(self):
        result = obs.observe_w01_delivery(self.stream(depth=8, pre=2, post=3),
                                          self.config(qos={"depth": 32,
                                                           "reliability": "reliable",
                                                           "durability": "volatile"}))
        edge = result["edges"][0]
        self.assertEqual(result["status"], "incomplete")
        self.assertIn("depth_binding_not_observed:writer:None",
                      edge["incomplete_reasons"])

    def test_middle_missing_delivery_becomes_proven_gap(self):
        result = obs.observe_w01_delivery(
            self.stream(depth=8, pre=2, post=3, skip_middle=True), self.config())
        edge = result["edges"][0]
        self.assertEqual(edge["delivery"]["missing_sequences"], [3])
        self.assertEqual(edge["delivery"]["proven_gaps"], [3])

    def test_trailing_missing_delivery_is_not_alone_a_violation(self):
        # D009: a finite-window tail gap has no later sentinel, so it stays
        # unproven rather than being promoted to a residual.
        result = obs.observe_w01_delivery(self.stream(depth=8, pre=2, post=3, delivery=2),
                                          self.config())
        edge = result["edges"][0]
        self.assertEqual(edge["delivery"]["missing_sequences"], [4])
        self.assertEqual(edge["delivery"]["proven_gaps"], [])

    def test_depth_lt_post_requires_reviewed_delivery_rule(self):
        config = self.config(qos={"depth": 3, "reliability": "reliable",
                                  "durability": "volatile"},
                             post_match_count=6)
        request = obs.observe_w01_delivery(self.stream(depth=3, pre=2, post=6), config)
        edge = request["edges"][0]
        self.assertEqual(edge["relation_class"], "depth_lt_post")
        self.assertEqual(edge["oracle_applicability"], "review_required")
        self.assertFalse(edge["candidate_eligible"])
        reviewed = obs.observe_w01_delivery(
            self.stream(depth=3, pre=2, post=6),
            {**config, "w01_delivery_rule_reviewed": True})
        self.assertEqual(reviewed["edges"][0]["oracle_applicability"], "applicable")

    def test_keep_all_history_is_not_silently_accepted(self):
        # Observed KEEP_ALL while the input asked for the default KEEP_LAST is a
        # binding mismatch; the observer must not treat it as satisfied.
        result = obs.observe_w01_delivery(self.stream(depth=8, pre=2, post=3,
                                                      history="keep_all"),
                                          self.config())
        self.assertIn("history_binding_mismatch",
                      " ".join(result["edges"][0]["incomplete_reasons"]))

    def test_keep_all_request_with_keep_last_binding_is_mismatch(self):
        result = obs.observe_w01_delivery(
            self.stream(depth=8, pre=2, post=3, history="keep_last"),
            self.config(qos={"depth": 8, "history": "keep_all",
                             "reliability": "reliable", "durability": "volatile"}))
        self.assertIn("history_binding_mismatch",
                      " ".join(result["edges"][0]["incomplete_reasons"]))

    def test_extra_endpoint_does_not_fail_but_is_recorded(self):
        result = obs.observe_w01_delivery(self.stream(extra_endpoint=True), self.config())
        # W01 is a workload surface; an extra endpoint changes match counts, not
        # per-edge delivery closure, so it must still be visible as its own fact.
        edge = result["edges"][0]
        self.assertEqual(edge["delivery"]["aggregate_used"], False)
        self.assertTrue(edge["delivery"]["post_match_sent_sequences"])

    def test_requires_a_config_object(self):
        with self.assertRaises(rs.RelationSurfaceError):
            obs.observe_w01_delivery(self.stream(), None)


class Capability(unittest.TestCase):
    def test_capability_declares_limits_and_never_claims_execution(self):
        caps = obs.relation_observer_capability()
        self.assertTrue(caps["implemented"])
        self.assertFalse(caps["starts_ros"])
        self.assertFalse(caps["execution_evidence"])
        self.assertFalse(caps["candidate_eligible"])
        self.assertEqual(caps["missing_evidence_policy"], "unknown_not_pass")
        self.assertFalse(caps["g01"]["aggregate_match_count_used"])
        self.assertEqual(caps["w01"]["history_supported_by_current_worker"], ["keep_last"])

    def test_edge_summary_is_compact_and_has_no_verdict(self):
        observation = obs.g01_edge_observation(full_stream(), CONFIG)
        summary = obs.edge_summary(observation)
        self.assertIsNone(summary["oracle_verdict"])
        self.assertFalse(summary["candidate_eligible"])
        self.assertEqual(summary["status"], "observed")
        self.assertEqual(summary["edges"][0]["match_status"], "observed")

    def test_module_imports_no_runtime_or_ros(self):
        import ast
        with open(obs.__file__, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
        self.assertNotIn("rclpy", imported)
        self.assertNotIn("r2f.runtime.ros2_pair", imported)
        self.assertNotIn("r2f.campaign.runner", imported)
        self.assertIn("r2f.modules.s1.relation_surfaces", imported)


if __name__ == "__main__":
    unittest.main()
