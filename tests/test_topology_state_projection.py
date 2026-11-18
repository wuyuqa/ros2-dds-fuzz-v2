"""Synthetic, offline raw fixtures only; NOT real ROS or SUT candidates.

The real-provenance variants below retain fixture=True. Structural edge-gate
unit tests edit copies of receipts, never produce real execution evidence.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from r2f.campaign.topology_state_projection import project_topology_state
from r2f.core.evidence import canonical_hash
from r2f.core.state_signature import observation_edge
from r2f.core.topology_state import topology_state_id


QOS = {"reliability": "reliable", "durability": "volatile", "liveliness": "automatic",
       "deadline_ns": 9223372036854775807, "liveliness_lease_duration_ns": 9223372036854775807}


class RawFixture:
    """A raw contract double with explicit fixture markers on EVERY event."""
    def __init__(self, writers=1, readers=1, *, direct=True, isolation=False,
                 missing=(), payload=False, publisher_gid=True, recreate=False):
        self.events, self.tokens, self.graphs = [], {}, {}
        self.backend = "offline_fake"
        self.roles = {**{"w" + str(i): "writer" for i in range(writers)},
                      **{"r" + str(i): "reader" for i in range(readers)}}
        self.program = {"schema": "r2f.s1-topology-program/v1",
                        "endpoints": [{"logical_id": eid, "role": role, "topic": "/topic",
                                       "type_name": "std_msgs/msg/String"} for eid, role in self.roles.items()],
                        "actions": [{"action_id": "c-" + eid, "op": "create", "endpoint_id": eid,
                                     "generation": 1} for eid in self.roles]}
        if recreate and self.roles:
            eid = next(iter(self.roles))
            self.program["actions"].extend([
                {"action_id": "d-" + eid, "op": "destroy", "endpoint_id": eid, "generation": 1},
                {"action_id": "rc-" + eid, "op": "recreate", "endpoint_id": eid, "generation": 2}])
        self.add("input_binding", receipt={"program_sha256": canonical_hash(self.program)})
        self.add("topology_backend_prepared", real_ros=False)
        carrier = self.add("topology_network_phase", network_phase="normal", recovery_cycle=0,
                           observed=True, implementation="fixture_neutral_carrier", audit_neutral=True)
        self.add("topology_network_observation", network_phase="normal", recovery_cycle=0,
                 source="carrier_audit", carrier_ref=carrier)
        self.before = self.boundary()
        for index, (eid, role) in enumerate(self.roles.items()):
            self.create(eid, 1, index)
        if recreate and self.roles:
            eid = next(iter(self.roles))
            old = self.tokens[eid]
            self.add("topology_endpoint_destroyed", **self.identity(old), endpoint_gid=old["endpoint_gid"], cleanup_status="closed")
            self.create(eid, 2, 50)
        for eid, token in self.tokens.items():
            request = self.add("worker_command_requested", **self.identity(token), command="snapshot", command_id="snap-" + eid)
            peer_ids = [e for e in self.tokens if self.roles[e] != self.roles[eid]]
            params = {"matched_peer_gids": [self.tokens[p]["endpoint_gid"] for p in peer_ids],
                      "peer_gids_complete": True, "peer_proof_source": "dds_matched_endpoint_api"} if direct else {}
            self.add("match_snapshot", **self.identity(token), endpoint_gid=token["endpoint_gid"],
                     command_id="snap-" + eid, matched_count=len(peer_ids) if direct else 1, **params)
            self.graphs[eid] = self.add("graph_snapshot", **self.identity(token), command_id="snap-" + eid,
                publishers=self.graph_rows("writer"), subscriptions=self.graph_rows("reader"))
            self.add("command_done", **self.identity(token), command_id="snap-" + eid)
        self.add("topology_pair_observation", pairs=[{"writer": w, "reader": r, "status": "matched"}
                 for w in self.roles if self.roles[w] == "writer" for r in self.roles if self.roles[r] == "reader"])
        next_sequence = {eid: 0 for eid in self.roles}
        self.windows = []
        for w in (e for e in self.roles if self.roles[e] == "writer"):
            for r in (e for e in self.roles if self.roles[e] == "reader"):
                pair = {"writer": w, "reader": r, "writer_generation": self.tokens[w]["generation"],
                        "reader_generation": self.tokens[r]["generation"]}
                if isolation:
                    self.add("topology_controlled_isolation", **pair, isolation_id="fixture-single-pair",
                             mechanism="synthetic_fixture_inventory", inventory_complete=True,
                             graph_refs=[self.graphs[w], self.graphs[r]])
                opened = self.add("topology_delivery_window_open", **pair)
                sends, receives = [], []
                for local in range(3):
                    seq = next_sequence[w]
                    next_sequence[w] += 1
                    body = {"writer_id": w, "writer_generation": self.tokens[w]["generation"],
                            "send_sequence": seq, "run_id": "fixture-run", "window": opened}
                    digest = canonical_hash(body)
                    sends.append(self.add("send", **self.identity(self.tokens[w]), writer_gid=self.tokens[w]["endpoint_gid"],
                        send_sequence=seq, publish_returned=True, payload_sha256=digest))
                    if local not in missing:
                        payload_fields = {"payload_writer_id": w, "payload_writer_generation": self.tokens[w]["generation"],
                                          "payload_run_id": "fixture-run", "payload_sha256": digest} if payload else {}
                        receives.append(self.add("receive", **self.identity(self.tokens[r]), reader_gid=self.tokens[r]["endpoint_gid"],
                            publisher_gid=self.tokens[w]["endpoint_gid"] if publisher_gid else None,
                            send_sequence=seq, **payload_fields))
                close = self.add("topology_delivery_window_closed", **pair, open_ref=opened,
                                 window_complete=True, sentinel_sequence=next_sequence[w] - 1)
                self.windows.append({"writer": w, "reader": r, "open": opened, "close": close, "sends": sends, "receives": receives})
        self.after = self.boundary()

    def add(self, event, **fields):
        index = len(self.events)
        self.events.append({"event": event, "event_index": index, "monotonic_ns": (index + 1) * 100,
                            "backend": self.backend, "fixture": True, "run_id": "fixture-run", **deepcopy(fields)})
        return index

    def create(self, eid, generation, serial):
        token = {"logical_id": eid, "generation": generation, "pid": 1000 + serial,
                 "process_start_ticks": 9000 + serial, "endpoint_generation": "worker-" + str(serial),
                 "endpoint_gid": "gid-" + eid + "-" + str(generation), "topic": "/topic",
                 "message_type": "std_msgs/msg/String", "role": self.roles[eid]}
        index = self.add("endpoint_created", **token)
        self.tokens[eid] = self.events[index]

    def graph_rows(self, role):
        return [{"endpoint_gid": t["endpoint_gid"], "topic_type": t["message_type"], "qos": deepcopy(QOS)}
                for eid, t in self.tokens.items() if self.roles[eid] == role]

    @staticmethod
    def identity(token):
        return {k: token[k] for k in ("logical_id", "generation", "pid", "endpoint_generation")}

    def boundary(self):
        return self.add("topology_state_boundary", observation_complete=True, lifecycle_complete=True, window_complete=True)

    def event(self, kind, **filters):
        return next(e for e in self.events if e["event"] == kind and all(e.get(k) == v for k, v in filters.items()))


class TopologyProjectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "raw.jsonl"

    def project(self, fixture, **kwargs):
        self.path.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in fixture.events))
        return project_topology_state(fixture.program, self.path,
            before_ref=kwargs.pop("before_ref", fixture.before), after_ref=kwargs.pop("after_ref", fixture.after), **kwargs)

    def pair(self, result, w="w0", r="r0", side="after"):
        return next(p for p in result["state_" + side]["pairs"] if p["writer"] == w and p["reader"] == r)

    def test_complete_fixture_projects_but_is_never_execution_or_coverage(self):
        result = self.project(RawFixture())
        self.assertEqual(result["receipt"]["status"], "observed", result["receipt"]["reasons"])
        self.assertFalse(result["eligible"])
        self.assertFalse(result["receipt"]["execution_evidence"])
        self.assertTrue(result["receipt"]["fixture"])
        self.assertEqual(result["state_before"]["endpoints"][0]["liveness"], "absent")
        self.assertEqual(self.pair(result)["delivery_closure"], "closed")
        self.assertEqual(self.pair(result)["observed_match"], "bidirectional")

    def test_component_refs_are_raw_bound_and_not_after_boundary(self):
        f = RawFixture(writers=2, readers=2)
        result = self.project(f)
        self.assertEqual(result["receipt"]["status"], "observed", result["receipt"]["reasons"])
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        for side in ("before", "after"):
            binding = result["receipt"][side]
            self.assertEqual(binding["state_id"], topology_state_id(result["state_" + side]))
            for component in binding["component_refs"].values():
                self.assertTrue(component["refs"])
                for ref in component["refs"]:
                    self.assertEqual(ref["path"], str(self.path))
                    self.assertEqual(ref["sha256"], digest)
                    self.assertLessEqual(ref["event_index"], binding["boundary_ref"]["event_index"])
        self.assertEqual(result["receipt"]["chronology"]["snapshot_ack_kind"], "command_completion_not_dds_delivery_ack")

    def test_main_2w2r_counterexample_never_accepts_helper_status(self):
        # Both diagonal and antidiagonal actual match graphs produce these SAME
        # raw counts/visible graph. No per-peer evidence distinguishes them.
        for backend in ("offline_fake", "ros2_public_api"):
            f = RawFixture(writers=2, readers=2, direct=False)
            for e in f.events:
                e["backend"] = backend
                if e["event"] == "topology_backend_prepared":
                    e["real_ros"] = backend == "ros2_public_api"
            result = self.project(f)
            self.assertEqual(self.pair(result, "w0", "r1")["observed_match"], "unknown")
            self.assertEqual(result["receipt"]["status"], "incomplete")
            proof = result["receipt"]["after"]["component_refs"]["/pairs/w0/r1/observed_match"]
            self.assertIs(proof["detail"]["helper_status_accepted"], False)
            self.assertIn("no_per_peer", proof["reason"])
            self.assertFalse(result["eligible"])
            self.assertEqual(self.pair(result, "w0", "r1")["delivery_closure"], "closed")

    def test_predicted_graph_cannot_replace_raw_matching_or_contracts(self):
        f = RawFixture(direct=False)
        f.program["predicted_graph"] = {"w0->r0": "matched"}
        f.events[0]["receipt"]["program_sha256"] = canonical_hash(f.program)
        f.event("graph_snapshot", logical_id="w0")["publishers"][0]["qos"] = {}
        result = self.project(f)
        self.assertEqual(self.pair(result)["observed_match"], "unknown")
        self.assertEqual(self.pair(result)["compatibility"], "unknown")

    def test_named_controlled_single_pair_isolation_works_but_not_multi_peer(self):
        result = self.project(RawFixture(direct=False, isolation=True, publisher_gid=False))
        self.assertEqual(self.pair(result)["observed_match"], "bidirectional", result["receipt"]["reasons"])
        self.assertEqual(self.pair(result)["delivery_closure"], "closed")
        detail = result["receipt"]["after"]["component_refs"]["/pairs/w0/r0/delivery_closure"]["detail"]
        self.assertEqual(detail["attribution"], "controlled_isolation_endpoint_correlated")
        result = self.project(RawFixture(writers=2, readers=2, direct=False, isolation=True))
        self.assertEqual(self.pair(result)["observed_match"], "unknown")
        self.assertIn("other_peers", result["receipt"]["after"]["component_refs"]["/pairs/w0/r0/observed_match"]["reason"])

    def test_multiwriter_without_gid_or_payload_chain_remains_unknown_no_total_fill(self):
        result = self.project(RawFixture(writers=2, readers=1, publisher_gid=False))
        for w in ("w0", "w1"):
            self.assertEqual(self.pair(result, w)["delivery_closure"], "unknown")
        self.assertFalse(result["eligible"])

    def test_multiwriter_unique_actual_payload_chain_is_correlated_not_direct_gid(self):
        result = self.project(RawFixture(writers=2, readers=2, direct=False, publisher_gid=False, payload=True))
        for w in ("w0", "w1"):
            for r in ("r0", "r1"):
                self.assertEqual(self.pair(result, w, r)["delivery_closure"], "closed", result["receipt"]["reasons"])
                self.assertEqual(self.pair(result, w, r)["observed_match"], "unknown")
                detail = result["receipt"]["after"]["component_refs"][f"/pairs/{w}/{r}/delivery_closure"]["detail"]
                self.assertEqual(detail["attribution"], "endpoint_correlated_payload")
                self.assertTrue(detail["directed_pair_communication"])
                self.assertFalse(detail["cross_writer_order_required"])

    def test_payload_fields_hash_run_generation_and_unique_send_all_required(self):
        mutations = [lambda e: e.pop("payload_writer_id"),
                     lambda e: e.update(payload_writer_generation=44),
                     lambda e: e.update(payload_run_id="other-run"),
                     lambda e: e.update(payload_sha256="f" * 64),
                     lambda e: e.update(reader_gid="old-reader"),
                     lambda e: e.update(pid=8000)]
        for mutate in mutations:
            f = RawFixture(writers=2, publisher_gid=False, payload=True)
            mutate(f.event("receive"))
            result = self.project(f)
            self.assertEqual(self.pair(result)["delivery_closure"], "unknown", result["receipt"]["reasons"])
        f = RawFixture(writers=2, publisher_gid=False, payload=True)
        first = f.event("send")
        # Different actual send with same hash destroys uniqueness, even if a
        # hand-authored summary claims a unique matching writer/sequence.
        duplicate = f.events[f.windows[0]["sends"][1]]
        duplicate["payload_sha256"] = first["payload_sha256"]
        self.assertEqual(self.pair(self.project(f))["delivery_closure"], "unknown")

    def test_middle_gap_needs_later_sentinel_tail_gap_stays_open(self):
        middle = self.project(RawFixture(missing={1}))
        self.assertEqual(self.pair(middle)["delivery_closure"], "closed_with_gap")
        tail = self.project(RawFixture(missing={2}))
        self.assertEqual(self.pair(tail)["delivery_closure"], "open")
        self.assertFalse(middle["eligible"])
        self.assertNotIn("candidate", middle)
        f = RawFixture(missing={1})
        f.event("topology_delivery_window_closed").pop("sentinel_sequence")
        self.assertEqual(self.pair(self.project(f))["delivery_closure"], "open")

    def test_no_cross_writer_global_sequence_order_is_assumed(self):
        f = RawFixture(writers=2, readers=1, publisher_gid=False, payload=True)
        # Writer 1's sequence starts at 0 AFTER writer 0's sentinel 2. This is
        # not a total-order violation and both individual windows remain closed.
        result = self.project(f)
        self.assertEqual([self.pair(result, w)["delivery_closure"] for w in ("w0", "w1")], ["closed", "closed"])

    def test_raw_identity_pid_time_domain_noise_preserves_state_ids(self):
        f = RawFixture(writers=2, readers=2, recreate=True, publisher_gid=False, payload=True)
        first = self.project(f)
        for event in f.events:
            for key in ("endpoint_gid", "reader_gid", "writer_gid", "publisher_gid"):
                if isinstance(event.get(key), str):
                    event[key] = "changed-" + event[key]
            for key in ("publishers", "subscriptions"):
                for row in event.get(key, []):
                    row["endpoint_gid"] = "changed-" + row["endpoint_gid"]
            if "matched_peer_gids" in event:
                event["matched_peer_gids"] = ["changed-" + g for g in event["matched_peer_gids"]]
            if "pid" in event:
                event["pid"] += 20000
            if "endpoint_generation" in event:
                event["endpoint_generation"] = "changed-" + event["endpoint_generation"]
            if "process_start_ticks" in event:
                event["process_start_ticks"] += 10000
            event["domain_id"] = 219
            event["monotonic_ns"] += 100000
        second = self.project(f)
        self.assertEqual(first["receipt"]["status"], "observed", first["receipt"]["reasons"])
        self.assertEqual(second["receipt"]["status"], "observed", second["receipt"]["reasons"])
        for side in ("before", "after"):
            self.assertEqual(topology_state_id(first["state_" + side]), topology_state_id(second["state_" + side]))
        self.assertNotEqual(first["receipt"]["raw_sha256"], second["receipt"]["raw_sha256"])

    def test_old_generation_snapshot_or_window_is_not_reused(self):
        f = RawFixture(recreate=True)
        f.event("match_snapshot", logical_id="w0")["generation"] = 1
        result = self.project(f)
        self.assertEqual(self.pair(result)["observed_match"], "unknown")
        f = RawFixture(recreate=True)
        f.event("topology_delivery_window_closed")["writer_generation"] = 1
        self.assertEqual(self.pair(self.project(f))["delivery_closure"], "unknown")
        f = RawFixture(recreate=True)
        created = [e for e in f.events if e["event"] == "endpoint_created" and e["logical_id"] == "w0"]
        created[1]["endpoint_gid"] = created[0]["endpoint_gid"]
        result = self.project(f)
        w = next(e for e in result["state_after"]["endpoints"] if e["logical_id"] == "w0")
        self.assertEqual(w["liveness"], "unknown")

    def test_snapshot_request_match_graph_ack_identity_and_order_audited(self):
        mutations = [lambda f: f.event("worker_command_requested").update(event="ignored_request"),
                     lambda f: f.event("command_done").update(event="ignored_ack"),
                     lambda f: f.event("command_done").update(command_id="other-command"),
                     lambda f: f.event("graph_snapshot").update(pid=999),
                     lambda f: f.event("graph_snapshot").update(process_start_ticks=999),
                     lambda f: f.event("match_snapshot").update(endpoint_generation="old-worker"),
                     lambda f: f.event("match_snapshot").update(monotonic_ns=1),
                     lambda f: f.event("command_done").update(monotonic_ns=1)]
        for mutate in mutations:
            f = RawFixture()
            mutate(f)
            result = self.project(f)
            self.assertEqual(self.pair(result)["observed_match"], "unknown", result["receipt"]["reasons"])
            self.assertFalse(result["eligible"])

    def test_per_peer_count_contradiction_is_unknown_not_new_edge(self):
        f = RawFixture()
        f.event("match_snapshot")["matched_count"] = 2
        result = self.project(f)
        self.assertEqual(self.pair(result)["observed_match"], "unknown")
        self.assertIn("contradiction", result["receipt"]["after"]["component_refs"]["/pairs/w0/r0/observed_match"]["reason"])

    def test_before_uses_only_prefix_not_future_creation_or_future_match(self):
        result = self.project(RawFixture())
        self.assertTrue(all(e["liveness"] == "absent" for e in result["state_before"]["endpoints"]))
        self.assertTrue(all(p["observed_match"] == "not_applicable" for p in result["state_before"]["pairs"]))
        f = RawFixture()
        bad = self.project(f, before_ref=f.after, after_ref=f.before)
        self.assertEqual(bad["receipt"]["status"], "invalid")
        self.assertFalse(bad["eligible"])

    def test_malformed_unbound_or_future_refs_fail_closed(self):
        for badref in (-1, True, "1", {"path": "wrong", "sha256": "0" * 64, "event_index": 0}, 99999):
            with self.subTest(ref=badref):
                result = self.project(RawFixture(), before_ref=badref)
                self.assertEqual(result["receipt"]["status"], "invalid")
                self.assertFalse(result["eligible"])
        for badref in (-1, True, {"event_index": 1}, {"path": str(self.path), "sha256": "f" * 64, "event_index": 1}):
            f = RawFixture()
            f.event("topology_delivery_window_closed")["open_ref"] = badref
            self.assertEqual(self.pair(self.project(f))["delivery_closure"], "unknown")
        f = RawFixture()
        f.event("topology_network_observation")["carrier_ref"] = f.after
        result = self.project(f)
        self.assertEqual(result["state_after"]["network_phase"], "unknown")

    def test_external_hash_and_dict_boundary_refs_bind_to_actual_bytes(self):
        f = RawFixture()
        result = self.project(f)
        digest = result["receipt"]["raw_sha256"]
        ref = lambda index: {"path": str(self.path), "sha256": digest, "event_index": index}
        result = project_topology_state(f.program, self.path, before_ref=ref(f.before), after_ref=ref(f.after), expected_sha256=digest)
        self.assertEqual(result["receipt"]["status"], "observed")
        result = project_topology_state(f.program, self.path, before_ref=ref(f.before), after_ref=ref(f.after), expected_sha256="f" * 64)
        self.assertIn("raw_sha256_mismatch", result["receipt"]["reasons"])

    def test_raw_malformed_index_clock_duplicate_json_and_missing_file_are_invalid(self):
        for field, value in (("event_index", True), ("event_index", 500), ("monotonic_ns", None), ("monotonic_ns", 0)):
            f = RawFixture()
            f.events[1][field] = value
            self.assertEqual(self.project(f)["receipt"]["status"], "invalid")
        f = RawFixture()
        for text in ('{"event":"x","event":"y"}\n', 'NaN\n', '\n', '{bad\n'):
            self.path.write_text(text)
            result = project_topology_state(f.program, self.path, before_ref=0, after_ref=1)
            self.assertEqual(result["receipt"]["status"], "invalid")
        self.path.unlink()
        self.assertEqual(project_topology_state(f.program, self.path, before_ref=0, after_ref=1)["receipt"]["status"], "invalid")

    def test_missing_global_or_completeness_fields_stay_unknown(self):
        for kind, field in (("topology_network_phase", "observed"), ("topology_network_phase", "audit_neutral"),
                            ("topology_state_boundary", "lifecycle_complete")):
            f = RawFixture()
            if kind == "topology_state_boundary":
                f.events[f.after].pop(field)
            else:
                f.event(kind).pop(field)
            result = self.project(f)
            self.assertEqual(result["state_after"]["observation_completeness"], "incomplete")
        f = RawFixture()
        f.events[0]["receipt"]["program_sha256"] = "f" * 64
        self.assertEqual(self.project(f)["receipt"]["status"], "invalid")

    def test_restoration_rules_removed_or_cached_graph_alone_is_not_verified(self):
        f = RawFixture()
        carrier = f.event("topology_network_phase")
        carrier.update(network_phase="restore_pending", recovery_cycle=1, rules_removed=True)
        observed = f.event("topology_network_observation")
        observed.update(network_phase="restored_verified", recovery_cycle=1)
        result = self.project(f)
        self.assertEqual(result["state_after"]["network_phase"], "unknown")
        observed["network_phase"] = "restore_pending"
        result = self.project(f)
        self.assertEqual(result["state_after"]["network_phase"], "restore_pending")
        self.assertEqual(result["state_after"]["recovery_cycle_bucket"], "1")

    def structural_record(self):
        result = self.project(RawFixture())
        # Pure validator input only. Editing these booleans does NOT attest that
        # the fixture ran DDS; no such record is written as campaign evidence.
        receipt = deepcopy(result["receipt"])
        receipt.update(fixture=False, execution_evidence=True)
        facts = {"environment_healthy": True, "process_healthy": True, "preconditions_reached": True,
                 "endpoint_identity_complete": True, "window_complete": True, "observation_complete": True,
                 "state_projection_receipt": receipt, "state_before": result["state_before"], "state_after": result["state_after"]}
        return {"execution_stage": "judged", "actual_run": True, "typed_facts": facts,
                "raw_path": receipt["raw_path"], "raw_sha256": receipt["raw_sha256"],
                "operation_family": "snapshot", "parameter_class": "neutral"}

    def test_explicit_observation_edge_receipt_dispatch_and_mutation_gates(self):
        record = self.structural_record()
        self.assertIsNotNone(observation_edge(record))
        mutations = [lambda r: r["typed_facts"]["state_projection_receipt"].update(schema="unknown"),
                     lambda r: r["typed_facts"]["state_projection_receipt"].update(fixture=True),
                     lambda r: r["typed_facts"]["state_projection_receipt"].update(execution_evidence=False),
                     lambda r: r.update(raw_sha256="f" * 64),
                     lambda r: r["typed_facts"]["state_after"]["pairs"][0].update(observed_match="none"),
                     lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["component_refs"].pop("/network_phase"),
                     lambda r: r["typed_facts"]["state_projection_receipt"]["after"]["identity"]["w0"].update(generation=9),
                     lambda r: r["typed_facts"]["state_projection_receipt"]["chronology"].update(status="invalid"),
                     lambda r: r.update(tool_error=True)]
        for mutate in mutations:
            changed = deepcopy(record)
            mutate(changed)
            self.assertIsNone(observation_edge(changed))
        fixture = self.project(RawFixture())
        record["typed_facts"]["state_projection_receipt"] = fixture["receipt"]
        self.assertIsNone(observation_edge(record))

    def test_projection_order_invariance_for_program_endpoint_order(self):
        f = RawFixture(writers=2, readers=2)
        before = self.project(f)
        f.program["endpoints"].reverse()
        f.events[0]["receipt"]["program_sha256"] = canonical_hash(f.program)
        after = self.project(f)
        for side in ("before", "after"):
            self.assertEqual(topology_state_id(before["state_" + side]), topology_state_id(after["state_" + side]))

    def test_raw_rows_can_log_receive_before_send_without_global_causal_claim(self):
        f = RawFixture(writers=2, payload=True, publisher_gid=False)
        a, b = f.windows[0]["sends"][0], f.windows[0]["receives"][0]
        f.events[a], f.events[b] = f.events[b], f.events[a]
        for index in (a, b):
            f.events[index]["event_index"] = index
            f.events[index]["monotonic_ns"] = (index + 1) * 100
        result = self.project(f)
        self.assertEqual(self.pair(result)["delivery_closure"], "closed", result["receipt"]["reasons"])
        detail = result["receipt"]["after"]["component_refs"]["/pairs/w0/r0/delivery_closure"]["detail"]
        self.assertEqual(detail["cross_worker_log_order"], "not_assumed")

    def test_future_send_does_not_retroactively_change_earlier_boundary(self):
        f = RawFixture(writers=2, payload=True, publisher_gid=False)
        earlier = self.project(f)
        send = f.event("send")
        extra = deepcopy(send)
        for key in ("event", "event_index", "monotonic_ns", "backend", "fixture", "run_id"):
            extra.pop(key)
        f.add("send", **extra)
        second = self.project(f)
        self.assertEqual(earlier["state_after"], second["state_after"])
        self.assertEqual(self.pair(second)["delivery_closure"], "closed")

    def test_retirement_keeps_current_logical_generation_and_raw_history_separate(self):
        f = RawFixture()
        f.events.pop()
        token = f.tokens["r0"]
        f.add("topology_endpoint_destroyed", **f.identity(token), endpoint_gid=token["endpoint_gid"], cleanup_status="closed")
        f.after = f.boundary()
        result = self.project(f)
        ep = next(e for e in result["state_after"]["endpoints"] if e["logical_id"] == "r0")
        self.assertEqual((ep["liveness"], ep["generation"]), ("retired", 1))
        self.assertNotIn("history", ep)
        self.assertEqual(len(result["receipt"]["after"]["history"]["r0"]), 2)
        self.assertEqual(self.pair(result)["observed_match"], "not_applicable")

    def test_boolean_generation_worker_identity_is_not_an_integer_alias(self):
        for kind, key in (("match_snapshot", "generation"), ("topology_delivery_window_closed", "writer_generation")):
            f = RawFixture()
            f.event(kind)[key] = True
            result = self.project(f)
            self.assertFalse(result["eligible"])
            name = "observed_match" if kind == "match_snapshot" else "delivery_closure"
            self.assertEqual(self.pair(result)[name], "unknown")

    def test_no_obligation_is_backed_by_actual_empty_complete_window(self):
        f = RawFixture()
        for e in f.events:
            if e["event"] in ("send", "receive"):
                e["event"] = "fixture_non_sample_diagnostic"
        result = self.project(f)
        self.assertEqual(self.pair(result)["delivery_closure"], "no_obligation")
        f.event("topology_delivery_window_closed")["window_complete"] = False
        self.assertEqual(self.pair(self.project(f))["delivery_closure"], "unknown")

    def test_zero_endpoint_boundary_is_observable_not_predicted(self):
        result = self.project(RawFixture(writers=0, readers=0))
        self.assertEqual(result["receipt"]["status"], "observed")
        self.assertEqual(result["state_after"]["pairs"], [])
        self.assertFalse(result["eligible"])

    def test_observed_incompatible_qos_does_not_overwrite_observed_match(self):
        f = RawFixture()
        for event in f.events:
            if event["event"] == "graph_snapshot":
                event["publishers"][0]["qos"]["reliability"] = "best_effort"
        result = self.project(f)
        self.assertEqual(self.pair(result)["compatibility"], "incompatible")
        self.assertEqual(self.pair(result)["observed_match"], "bidirectional")
        # Fact disagreement is retained for the separate oracle, never a
        # harness-generated SUT candidate or a rewritten expected graph.
        self.assertEqual(self.pair(result)["delivery_closure"], "closed")

    def test_raw_nonfinite_float_overflow_is_invalid(self):
        f = RawFixture()
        self.path.write_text('{"event":"x","event_index":0,"monotonic_ns":1,"noise":1e999}\n')
        result = project_topology_state(f.program, self.path, before_ref=0, after_ref=1)
        self.assertEqual(result["receipt"]["status"], "invalid")
        self.assertTrue(any("nonfinite" in r for r in result["receipt"]["reasons"]))

    def restored_fixture(self, *, carrier_after_snapshot=False):
        f = RawFixture()
        # Insert a measured restore carrier before fresh commands, preserving
        # every embedded index reference when reindexing this SYNTHETIC fixture.
        insertion = next(e["event_index"] for e in f.events if e["event"] ==
                         ("topology_pair_observation" if carrier_after_snapshot else "worker_command_requested"))
        carrier = {"event": "topology_network_phase", "event_index": insertion, "monotonic_ns": 1,
                   "backend": "offline_fake", "fixture": True, "run_id": "fixture-run",
                   "network_phase": "restore_pending", "recovery_cycle": 2, "observed": True,
                   "implementation": "fixture_restore", "rules_removed": True}
        for e in f.events:
            for key in ("open_ref", "carrier_ref"):
                if isinstance(e.get(key), int) and e[key] >= insertion:
                    e[key] += 1
        f.events.insert(insertion, carrier)
        f.events.pop()  # old after boundary
        for index, e in enumerate(f.events):
            e["event_index"], e["monotonic_ns"] = index, (index + 1) * 100
        discovery = []
        for token in f.tokens.values():
            discovery.append(f.add("topology_discovery_activity", **f.identity(token),
                endpoint_gid=token["endpoint_gid"], packet_count=1, source="endpoint_discovery_capture"))
        f.add("topology_network_observation", network_phase="restored_verified", recovery_cycle=2,
              source="carrier_audit", carrier_ref=insertion, discovery_refs=discovery)
        f.after = f.boundary()
        return f

    def test_restored_requires_fresh_pair_discovery_and_postrestore_delivery(self):
        f = self.restored_fixture()
        result = self.project(f)
        self.assertEqual(result["state_after"]["network_phase"], "restored_verified", result["receipt"]["reasons"])
        self.assertEqual(result["state_after"]["recovery_cycle_bucket"], "2plus")
        self.assertFalse(result["eligible"])
        f = self.restored_fixture(carrier_after_snapshot=True)
        result = self.project(f)
        self.assertEqual(result["state_after"]["network_phase"], "unknown")
        self.assertTrue(any("not_fresh" in r for r in result["receipt"]["reasons"]))
        f = self.restored_fixture()
        f.event("topology_discovery_activity")["endpoint_gid"] = "stale-generation"
        self.assertEqual(self.project(f)["state_after"]["network_phase"], "unknown")

    def test_drop_requires_observed_discovery_counter_not_synthetic_udp(self):
        f = RawFixture()
        carrier = f.event("topology_network_phase")
        observation = f.event("topology_network_observation")
        carrier.update(network_phase="drop_active", recovery_cycle=0, applied=True, discovery_packets=0, synthetic_udp_packets=99)
        observation["network_phase"] = "drop_active"
        self.assertEqual(self.project(f)["state_after"]["network_phase"], "unknown")
        carrier["discovery_packets"] = 1
        self.assertEqual(self.project(f)["state_after"]["network_phase"], "drop_active")

    def test_component_and_identity_refs_cannot_be_moved_after_boundary_in_edge_gate(self):
        record = self.structural_record()
        receipt = record["typed_facts"]["state_projection_receipt"]
        after = receipt["after"]["boundary_ref"]["event_index"]
        for target in (receipt["before"]["component_refs"]["/network_phase"]["refs"][0],
                       receipt["after"]["identity"]["w0"]["creation_ref"]):
            # Modify the original selected ref temporarily, then validate the
            # original; restore it for the second independent case.
            old = target["event_index"]
            target["event_index"] = after + 1
            self.assertIsNone(observation_edge(record))
            target["event_index"] = old
        self.assertIsNotNone(observation_edge(record))

    def test_cross_run_worker_evidence_cannot_borrow_current_logical_identity(self):
        for kind in ("endpoint_created", "match_snapshot", "receive"):
            f = RawFixture()
            f.event(kind)["run_id"] = "different-run"
            result = self.project(f)
            self.assertEqual(result["state_after"]["observation_completeness"], "incomplete")
            self.assertFalse(result["eligible"])

    def test_feedback_preserves_new_receipt_and_revalidates_edge_without_file_changes(self):
        from r2f.campaign.feedback import FeedbackLog
        record = self.structural_record()
        record.update(content_sha256="a" * 64, surface_id="topology", boundary_class="snapshot")
        log = FeedbackLog()
        event = log.record(record)
        observation = event["edge_provenance"]["observation"]
        self.assertEqual(observation["typed_facts"]["state_projection_receipt"]["schema"],
                         "r2f.s1-topology-state-projection/v1")
        self.assertEqual(observation_edge(observation)[1], event["edge_id"])
        restored = FeedbackLog.from_events(json.loads(json.dumps(log.events)))
        self.assertEqual(restored.identity(), log.identity())
        self.assertEqual(restored.edge_counts(), log.edge_counts())

    def test_direct_topology_edge_helper_keeps_shared_observation_gate(self):
        from r2f.core.topology_state import topology_observation_edge
        record = self.structural_record()
        self.assertIsNotNone(topology_observation_edge(record))
        record["actual_run"] = False
        self.assertIsNone(topology_observation_edge(record))

    def test_known_raw_worker_error_cannot_be_overridden_by_boundary_flags(self):
        f = RawFixture()
        f.events.pop()
        f.add("worker_command_error", **f.identity(f.tokens["w0"]), error="fixture_tool_failure")
        f.after = f.boundary()
        result = self.project(f)
        self.assertEqual(result["state_before"]["observation_completeness"], "complete")
        self.assertEqual(result["state_after"]["observation_completeness"], "incomplete")
        self.assertFalse(result["eligible"])
        self.assertTrue(any("raw_worker_or_backend_error" in r for r in result["receipt"]["reasons"]))
