"""Strict immutable JSONL fixtures only. No ROS/DDS/netns/campaign is run."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.campaign.topology_adapter import TopologyFactProducer, facts_from_events, canonical_receipts
from r2f.core import Scenario
from r2f.core.evidence import canonical_hash, effective_input_hash, INPUT_SCHEMA_V2
from r2f.core.mutation import MutationReceipt
from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_input import (
    make_topology_plan, topology_input_layer_receipts, TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY,
)
from r2f.modules.s1.topology_targets import topology_target_declaration


class RawFixture:
    """Authored protocol fixture. Every event explicitly marked non-real."""
    def __init__(self, *, writers=1, readers=1, missing=(), publisher_gid=True,
                 direct=True, incompatible=False, depth=64):
        self.events, self.tokens, self.raw = [], {}, None
        self.missing, self.publisher_gid, self.direct = set(missing), publisher_gid, direct
        parent = model.generate_program(3, writers=writers, readers=readers)
        for e in parent["endpoints"]:
            e["topic"] = "/fixture/topic"
            e["qos"].update(reliability="reliable", durability="volatile", history="keep_last", depth=depth)
        parent["actions"] = [model._action("create_" + e["logical_id"], "create", e["logical_id"], 1)
                             for e in parent["endpoints"]]
        parent["actions"] += [model._action("publish_" + e["logical_id"], "publish_batch", e["logical_id"], 1,
                                          {"count": 3, "start_sequence": 1})
                              for e in parent["endpoints"] if e["role"] == "writer"]
        parent["actions"].append(model._action("snapshot_final", "snapshot", params={"label": "end"}))
        self.plan = make_topology_plan(parent, [{"operator_id": "action.insert"}], rng_seed=7)
        self.program = self.plan.final_config["topology_program"]
        self.specs = {e["logical_id"]: e for e in self.program["endpoints"]}
        self.scenario = Scenario(topology_target_declaration(), "rmw_cyclonedds_cpp", "mutated", 0,
                                 seed=7, mutation_plan=self.plan)
        self.layers = topology_input_layer_receipts(self.plan)
        source = {"source_repo": "/nonexistent/offline/fixture", "head": None, "source_frozen": False}
        eff = self.scenario.effective_input_v2(final_input=self.plan.final_config,
                      input_layer_receipts=self.layers, source_manifest=source)
        self.config = {**deepcopy(eff), "effective_input": eff,
                       "effective_input_sha256": effective_input_hash(eff, schema=INPUT_SCHEMA_V2),
                       "plan_hash": self.plan.plan_hash(), "run_id": "fixture-topology", "domain_id": 191,
                       "runtime_capability": TOPOLOGY_CAPABILITY,
                       "topology_program": deepcopy(self.program)}
        self.route = {"schema": "r2f.topology-route-binding/v1", "target_id": TOPOLOGY_TARGET,
                      "capability": TOPOLOGY_CAPABILITY, "plan_hash": self.plan.plan_hash(),
                      "final_input_sha256": canonical_hash(self.plan.final_config),
                      "input_layer_receipts_sha256": canonical_hash(self.layers),
                      "effective_input_sha256": self.config["effective_input_sha256"]}
        self.add("input_binding", receipt={"program_sha256": canonical_hash(self.program), "route_binding": self.route})
        self.add("environment_preflight", ok=True)
        self.add("isolation_locks_acquired", locks=["domain", "registry", "directory"])
        self.add("topology_provenance", runtime_kind="offline_fixture", source=None)
        self.add("topology_backend_prepared", real_ros=False)
        carrier = self.add("topology_network_phase", network_phase="normal", recovery_cycle=0,
                           observed=True, implementation="explicit_fixture_neutral", audit_neutral=True)
        self.add("topology_network_observation", network_phase="normal", recovery_cycle=0,
                 source="carrier_audit", carrier_ref=carrier)
        self.before_boundary = self.boundary()
        self.add("mutation_begin", mutation_kind="endpoint_program", route_binding=self.route, input_binding_ref=0)
        self.next_seq = {eid: 0 for eid in self.specs}
        self.incompatible = incompatible
        self.observed = {}
        for i, action in enumerate(self.program["actions"]):
            self.action(action, i)
        self.after_boundary = self.boundary()
        for token in self.tokens.values():
            self.add("topology_worker_exit", **self.identity(token), returncode=0)
        self.add("topology_cleanup", receipt={"closed": True})
        self.add("runtime_end", mutation_receipt=MutationReceipt().to_dict(),
                 cleanup_complete=True, real_execution=False)

    def add(self, event, **fields):
        i = len(self.events)
        self.events.append({"event": event, "event_index": i, "time_ns": 1700000000000000000 + i * 1000,
                            "monotonic_ns": 1000 + i * 1000, "fixture": True, "backend": "offline_fake",
                            "run_id": "fixture-topology", "domain_id": 191, **deepcopy(fields)})
        return i

    def boundary(self):
        return self.add("topology_state_boundary", lifecycle_complete=True, observation_complete=True, window_complete=True)

    @staticmethod
    def identity(token):
        return {k: token[k] for k in ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")}

    def snapshot(self):
        for eid, token in self.tokens.items():
            cid = "snapshot-" + str(len(self.events))
            other_role = "reader" if self.specs[eid]["role"] == "writer" else "writer"
            peers = [t["endpoint_gid"] for name, t in self.tokens.items() if self.specs[name]["role"] == other_role]
            fields = {"matched_peer_gids": peers, "peer_gids_complete": True,
                      "peer_proof_source": "dds_matched_endpoint_api"} if self.direct else {}
            ident = self.identity(token)
            self.add("worker_command_requested", **ident, command="snapshot", command_id=cid)
            self.add("match_snapshot", **ident, command_id=cid, matched_count=len(peers) if self.direct else 1,
                     endpoint_gid=token["endpoint_gid"], **fields)
            self.add("graph_snapshot", **ident, command_id=cid,
                     publishers=self.graph("writer"), subscriptions=self.graph("reader"))
            self.add("command_done", **ident, command_id=cid)

    def graph(self, role):
        rows = []
        for eid, token in self.tokens.items():
            if self.specs[eid]["role"] != role:
                continue
            q = self.specs[eid]["qos"]
            qos = {"reliability": "best_effort" if self.incompatible and role == "writer" else q["reliability"],
                   "durability": q["durability"], "history": q["history"], "depth": q["depth"],
                   "liveliness": q["liveliness"], "deadline_ns": 9223372036854775807,
                   "liveliness_lease_duration_ns": 9223372036854775807}
            rows.append({"endpoint_gid": token["endpoint_gid"], "topic_type": token["message_type"], "qos": qos})
        return rows

    def action(self, action, index):
        eid, op, gen = action.get("endpoint_id"), action["op"], action.get("generation")
        before = ({"generation": 1 if eid in self.tokens else 0, "alive": eid in self.tokens,
                   "next_sequence": self.next_seq.get(eid, 0)} if eid is not None
                  else {"alive": sorted(self.tokens)})
        self.add("topology_action_begin", action_id=action["action_id"], action_index=index,
                 action=action, before=before)
        if op == "create":
            spec = self.specs[eid]
            serial = len(self.tokens) + 1
            token = {"logical_id": eid, "generation": gen, "pid": 1000 + serial,
                     "process_start_ticks": 9000 + serial, "endpoint_generation": "worker-" + str(serial),
                     "endpoint_gid": "gid-" + eid, "topic": spec["topic"], "message_type": spec["type_name"],
                     "qos": deepcopy(spec["qos"]), "role": spec["role"]}
            n = self.add("endpoint_created", **token)
            self.tokens[eid] = self.events[n]
            after = {**self.identity(token), "endpoint_gid": token["endpoint_gid"], "alive": True}
            requires = [{"id": "fresh_endpoint_identity", "status": "satisfied"}]
        elif op == "publish_batch":
            self.snapshot()
            readers = [r for r in self.tokens if self.specs[r]["role"] == "reader"]
            windows = {r: self.add("topology_delivery_window_open", writer=eid, reader=r,
                                  writer_generation=gen, reader_generation=1) for r in readers}
            start, count = action["params"]["start_sequence"], action["params"]["count"]
            for seq in range(start, start + count):
                token = self.tokens[eid]
                payload = canonical_hash({"run": "fixture-topology", "writer": eid, "generation": gen, "sequence": seq})
                self.add("send", **self.identity(token), writer_gid=token["endpoint_gid"], send_sequence=seq,
                         publish_returned=True, payload_sha256=payload)
                for r in readers:
                    if seq not in self.missing:
                        self.add("receive", **self.identity(self.tokens[r]), reader_gid=self.tokens[r]["endpoint_gid"],
                                 send_sequence=seq, publisher_gid=token["endpoint_gid"] if self.publisher_gid else None,
                                 publisher_gid_status="available" if self.publisher_gid else "unavailable_api",
                                 payload_run_id="fixture-topology", payload_writer_id=eid, payload_writer_generation=gen,
                                 payload_sha256=payload)
            for r in readers:
                self.add("topology_delivery_window_closed", writer=eid, reader=r, writer_generation=gen,
                         reader_generation=1, open_ref=windows[r], sentinel_sequence=start + count - 1, window_complete=True)
            self.next_seq[eid] = start + count
            after = {"generation": gen, "alive": True, "next_sequence": start + count}
            requires = [{"id": "writer_generation_alive", "status": "satisfied"}]
        else:
            assert op == "snapshot", op
            self.snapshot()
            self.add("topology_pair_observation", pairs=[{"status": "matched"}], helper_status_is_diagnostic=True)
            after = {"complete": True, "snapshot_command_id": "fixture"}
            requires = [{"id": "runtime_snapshot_receipt", "status": "satisfied"}]
        receipt = {"step_id": "action:" + action["action_id"], "action": action, "before": before,
                   "after": after, "requires": requires, "satisfied": True, "disposition": "applied"}
        self.add("state_step_after", step_id=receipt["step_id"], before=before, after=after, satisfied=True)
        self.add("topology_action_done", action_id=action["action_id"], action_index=index, action=action, receipt=receipt)

    def save(self, tmp_path):
        self.raw = Path(tmp_path) / "topology-raw.jsonl"
        self.raw.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in self.events))
        self.runtime = {"raw_path": str(self.raw), "raw_sha256": hashlib.sha256(self.raw.read_bytes()).hexdigest(),
                        "events": deepcopy(self.events), "runtime_status": "completed", "real_execution": False,
                        "facts": {"bogus": "must never be used", "environment_healthy": True},
                        "mutation_receipt": deepcopy(self.events[-1]["mutation_receipt"])}
        return self

    def facts(self, tmp_path):
        self.save(tmp_path)
        return TopologyFactProducer().produce({"events": self.events, "runtime": self.runtime, "config": self.config})


def test_strict_fixture_produces_typed_observations_but_never_real_eligibility(tmp_path):
    f = RawFixture()
    facts = f.facts(tmp_path)
    assert facts["diagnostic_valid"], facts
    assert facts["pairs"][0]["observed_match"] == "bidirectional"
    assert facts["pairs"][0]["delivery_closure"] == "closed"
    assert facts["endpoint_identity_complete"]
    assert len(facts["deliveries"]) == 3
    assert not facts["candidate_eligible"] and not facts["seal_eligible"] and not facts["execution_evidence"]
    assert not facts["environment_healthy"]
    assert "bogus" not in facts


def test_real_flag_or_runtime_facts_never_upgrade_fixture(tmp_path):
    f = RawFixture().save(tmp_path)
    f.runtime.update(real_execution=True, facts={"candidate_eligible": True, "observed_match": "matched"})
    facts = facts_from_events(f.events, f.runtime, f.config)
    assert facts["fixture"] and not facts["candidate_eligible"]
    assert not facts["environment_healthy"]


@pytest.mark.parametrize("field", ["pid", "process_start_ticks", "endpoint_generation", "generation", "reader_gid"])
def test_receive_worker_lineage_and_gid_are_independently_checked(tmp_path, field):
    f = RawFixture()
    receive = next(e for e in f.events if e["event"] == "receive")
    receive[field] = "wrong"
    facts = f.facts(tmp_path)
    assert facts.get("evidence_error")
    assert not facts["endpoint_identity_complete"] and not facts["candidate_eligible"]


def test_no_result_facts_fallback_for_missing_raw_state(tmp_path):
    f = RawFixture()
    e = next(e for e in f.events if e["event"] == "topology_action_done")
    del e["receipt"]["after"]
    facts = f.facts(tmp_path)
    assert "action_state_or_requires_missing" in facts["evidence_error"]


@pytest.mark.parametrize("kind", ["target", "capability", "program", "layers", "route", "chronology", "old_raw"])
def test_binding_and_chronology_tampering_fail_closed(tmp_path, kind):
    f = RawFixture()
    if kind == "target": f.config["target_id"] += ".alias"
    if kind == "capability": f.config["runtime_capability"] = "topology"
    if kind == "program": f.config["final_input"]["topology_program"]["endpoints"][0]["topic"] = "/other"
    if kind == "layers": f.config["input_layer_receipts"][0]["seed"] += 1
    if kind == "route": f.events[0]["receipt"]["route_binding"]["plan_hash"] = "bad"
    if kind == "chronology":
        e = next(e for e in f.events if e["event"] == "topology_action_done")
        e["monotonic_ns"] = 1
    if kind == "old_raw": f.events[-1]["run_id"] = "previous-run"
    facts = f.facts(tmp_path)
    assert facts.get("evidence_error"), kind
    assert not facts["candidate_eligible"]


def test_events_and_sha_must_match_immutable_file(tmp_path):
    f = RawFixture().save(tmp_path)
    f.runtime["raw_sha256"] = "0" * 64
    assert facts_from_events(f.events, f.runtime, f.config)["evidence_error"] == "raw_sha256_mismatch"
    f.save(tmp_path)
    f.runtime["events"][0]["run_id"] = "old"
    assert "raw_events_differ" in facts_from_events(f.events, f.runtime, f.config)["evidence_error"]


def test_aggregate_counts_and_graphs_do_not_resolve_2w2r_directed_match(tmp_path):
    facts = RawFixture(writers=2, readers=2, direct=False).facts(tmp_path)
    assert facts["diagnostic_valid"], facts
    assert len(facts["pairs"]) == 4
    assert all(p["observed_match"] == "unknown" for p in facts["pairs"])
    assert all(not p["proofs"]["observed_match"].get("detail", {}).get("helper_status_accepted", False) for p in facts["pairs"])


def test_multiwriter_missing_gid_can_use_unique_raw_payload_not_totals(tmp_path):
    facts = RawFixture(writers=2, readers=2, publisher_gid=False).facts(tmp_path)
    assert facts["diagnostic_valid"], facts
    assert len(facts["deliveries"]) == 12
    assert {d["basis"] for d in facts["deliveries"]} == {"endpoint_correlated_payload"}
    assert all(d["publisher_gid"] is None and d["publisher_gid_status"] == "unavailable_api"
               and not d["direct_gid_match"] for d in facts["deliveries"])
    assert all(p["delivery_closure"] == "closed" for p in facts["pairs"])


@pytest.mark.parametrize("fault", ["writer", "generation", "run", "missing_hash", "duplicate_hash", "unavailable"])
def test_payload_correlation_requires_unique_complete_current_lineage(tmp_path, fault):
    f = RawFixture(writers=2, publisher_gid=False)
    recv = next(e for e in f.events if e["event"] == "receive")
    if fault == "writer": recv["payload_writer_id"] = "nonexistent"
    if fault == "generation": recv["payload_writer_generation"] = 99
    if fault == "run": recv["payload_run_id"] = "old-run"
    if fault == "missing_hash": del recv["payload_sha256"]
    if fault == "unavailable": recv["publisher_gid_status"] = "available"
    if fault == "duplicate_hash":
        sends = [e for e in f.events if e["event"] == "send"]
        sends[-1]["payload_sha256"] = recv["payload_sha256"]
    facts = f.facts(tmp_path)
    assert facts["diagnostic_valid"], facts
    assert facts["unknown_reasons_sample_identity"]
    assert any(p["delivery_closure"] == "unknown" for p in facts["pairs"])
    assert not facts["candidate_eligible"]


def test_canonical_receipts_do_not_read_helper_bool_or_accept_alias(tmp_path):
    f = RawFixture().save(tmp_path)
    f.runtime["precondition_receipts"] = [{"satisfied": True}] * 4
    with pytest.raises(ValueError, match="raw_missing_or_duplicate:state_step_before"):
        canonical_receipts(f.scenario, f.runtime, f.events, config=f.config)
    with pytest.raises(ValueError, match="exact_target"):
        canonical_receipts(SimpleNamespace(target_id=TOPOLOGY_TARGET + ".alias"), f.runtime, f.events, config=f.config)


def test_shared_dispatch_uses_exact_target_only(tmp_path, monkeypatch):
    from r2f.campaign import runtime_adapter
    calls = []
    monkeypatch.setattr("r2f.campaign.topology_adapter.facts_from_events", lambda *args: calls.append(args) or {"dispatched": True})
    f = RawFixture().save(tmp_path)
    assert runtime_adapter.facts_from_events(f.events, f.runtime, f.config) == {"dispatched": True}
    alias = dict(f.config, target_id=TOPOLOGY_TARGET + ".alias")
    assert "dispatched" not in runtime_adapter.facts_from_events(f.events, f.runtime, alias)
    assert len(calls) == 1


def _registry_offline_execution(tmp_path, *, missing_primary=False):
    """Real registry/provider/capsule/carrier/runtime; no service replacement."""
    from r2f.core import Registry, ReplayCapsule, SemanticTarget
    from r2f.modules.s1.module import register_exploratory
    from r2f.runtime.ros2_pair import run_pair
    from r2f.runtime.topology_worker import OfflineTopologyBackend
    registry = Registry()
    register_exploratory(registry, runtime_capability=TOPOLOGY_CAPABILITY)
    target = registry.get_target(TOPOLOGY_TARGET)
    plan = make_topology_plan(model.generate_program(0, writers=1, readers=1),
                              [{"operator_id": "action.insert"}], rng_seed=7)
    scenario = Scenario(target, "rmw_cyclonedds_cpp", "mutated", 0, 7, {}, plan)
    services = registry.get_services(TOPOLOGY_TARGET)
    prepared = services.mutation_provider.prepare(scenario)
    capsule = ReplayCapsule.from_scenario(scenario,
        source_manifest={"source_repo": "/offline/fixture", "head": None, "source_frozen": False},
        final_input=prepared["final_input"], input_layer_receipts=prepared["input_layer_receipts"])
    capsule = ReplayCapsule.from_dict(json.loads(json.dumps(capsule.to_dict())))
    effective = capsule.input["effective_input"]
    config = dict(deepcopy(effective), effective_input=deepcopy(effective),
                  effective_input_sha256=capsule.input["effective_input_sha256"],
                  plan_hash=plan.plan_hash(), scenario=scenario.to_dict(),
                  runtime_capability=TOPOLOGY_CAPABILITY,
                  topology_program=deepcopy(plan.final_config["topology_program"]),
                  run_id="topology-offline-ingest", domain_id=191, backend="offline_fake")
    runtime = services.carrier.apply(scenario, effective, execution={
        "config": config, "output_dir": tmp_path,
        "runtime": lambda cfg, out: run_pair(cfg, out, topology_backend=OfflineTopologyBackend())})
    if missing_primary:
        declaration = target.to_dict()
        declaration["rule_contract"].pop("primary_rule", None)
        target = SemanticTarget.from_dict(declaration)
        scenario = Scenario(target, "rmw_cyclonedds_cpp", "mutated", 0, 7, {}, plan)
    return services, scenario, capsule, config, runtime


@pytest.mark.parametrize("missing_primary", [False, True])
def test_real_ingest_raw_mismatch_unknown_without_masking(tmp_path, missing_primary):
    from r2f.campaign.runtime_adapter import ingest_execution
    services, scenario, capsule, config, runtime = _registry_offline_execution(
        tmp_path, missing_primary=missing_primary)
    raw = Path(runtime["raw_path"]).read_bytes()
    runtime["events"] = deepcopy(runtime["events"])
    runtime["events"][0]["returned_only_tamper"] = True
    row = ingest_execution(scenario, config, runtime, capsule, services=services)
    assert row["execution_stage"] == "aborted"
    assert row["ingestion_error"] == "returned events differ from raw"
    assert row["oracle_verdict"] == "unknown" and row["actual_run"] is False
    assert Path(runtime["raw_path"]).read_bytes() == raw
    assert hashlib.sha256(raw).hexdigest() == row["raw_sha256"]
    if missing_primary:
        assert row["rule_id"] == "unresolved_primary_rule"
        assert row["oracle_tool_failure"]["reason"] == "target_primary_rule_missing_or_invalid"
    else:
        assert row["rule_id"] == scenario.target.rule_contract["primary_rule"]
    assert not runtime["real_execution"] and not any(runtime["mutation_receipt"].values())


def test_real_ingest_offline_fixture_cannot_qualify(tmp_path):
    from r2f.campaign.runtime_adapter import ingest_execution
    services, scenario, capsule, config, runtime = _registry_offline_execution(tmp_path)
    row = ingest_execution(scenario, config, runtime, capsule, services=services)
    assert row["execution_stage"] == "aborted" and row["actual_run"] is False
    assert row["oracle_verdict"] == "unknown"
    assert not row["typed_facts"].get("candidate_eligible", False)
    assert not row["typed_facts"].get("execution_evidence", False)
    assert not any(runtime["mutation_receipt"].values())


def test_produced_projection_feedback_roundtrip_excludes_fixture(tmp_path):
    from r2f.campaign.feedback import FeedbackLog
    facts = RawFixture().facts(tmp_path)
    assert facts["state_projection_receipt"]["schema"] == "r2f.s1-topology-state-projection/v1"
    assert "state_observation_receipt" not in facts
    assert facts["state_projection_receipt"]["status"] == "observed"
    row = {"execution_stage": "aborted", "actual_run": False, "typed_facts": facts,
           "content_sha256": "a" * 64, "raw_path": facts["raw"]["path"],
           "raw_sha256": facts["raw"]["sha256"], "surface_id": "S1.topology.program",
           "boundary_class": "snapshot", "operation_family": "snapshot", "parameter_class": "neutral"}
    log = FeedbackLog()
    event = log.record(row)
    assert event["edge_id"] is None and event["coverage"] is False
    restored = FeedbackLog.from_events(json.loads(json.dumps(log.to_dict()))["events"])
    assert restored.identity() == log.identity()
    assert not restored.edge_counts() and not facts["candidate_eligible"]


def test_projection_feedback_qualified_shape_roundtrip_contract_only(tmp_path):
    """PURE CONTRACT TEST: crafted eligibility is not runtime/provenance proof.

    Uses actual producer states/receipt, but edits flags only as validator input.
    No such record is saved/admitted as execution, candidate or campaign evidence.
    """
    from r2f.campaign.feedback import FeedbackLog
    from r2f.core.state_signature import observation_edge
    facts = RawFixture().facts(tmp_path)
    contract = deepcopy(facts)
    contract.update(environment_healthy=True, process_healthy=True, preconditions_reached=True,
                    endpoint_identity_complete=True, observation_complete=True)
    contract["state_projection_receipt"].update(fixture=False, execution_evidence=True)
    row = {"execution_stage": "judged", "actual_run": True, "typed_facts": contract,
           "content_sha256": "b" * 64, "raw_path": facts["raw"]["path"],
           "raw_sha256": facts["raw"]["sha256"], "surface_id": "S1.topology.program",
           "boundary_class": "snapshot", "operation_family": "snapshot", "parameter_class": "neutral"}
    expected = observation_edge(row)
    assert expected is not None
    log = FeedbackLog()
    event = log.record(row)
    assert event["edge_id"] == expected[1]
    stored = json.loads(json.dumps(log.to_dict()))
    restored = FeedbackLog.from_events(stored["events"])
    observation = restored.events[0]["edge_provenance"]["observation"]
    assert observation["typed_facts"]["state_projection_receipt"] == contract["state_projection_receipt"]
    assert observation_edge(observation) == expected
    assert restored.identity() == log.identity()
    observation["typed_facts"]["state_projection_receipt"]["fixture"] = True
    assert observation_edge(observation) is None
    assert facts["state_projection_receipt"]["fixture"] is True
    assert not facts["candidate_eligible"]


def test_canonical_premutation_receipt_rejects_backfill_before_source_checks(tmp_path):
    f = RawFixture()
    end = f.events.pop()
    f.add("state_step_before", step_id="admit_topology", before={}, requires={})
    f.add("state_step_after", step_id="admit_topology", after={}, satisfied=True)
    f.add("runtime_end", **{k: end[k] for k in ("mutation_receipt", "cleanup_complete", "real_execution")})
    f.save(tmp_path)
    with pytest.raises(ValueError, match="raw_chronology_invalid"):
        canonical_receipts(f.scenario, f.runtime, f.events, config=f.config)


def test_real_ingest_resolves_opt_in_services_without_injection(tmp_path):
    from r2f.campaign.runtime_adapter import ingest_execution
    _, scenario, capsule, config, runtime = _registry_offline_execution(tmp_path)
    row = ingest_execution(scenario, config, runtime, capsule)
    assert row["execution_stage"] == "aborted"
    assert row["oracle_verdict"] == "unknown" and not row["actual_run"]


def test_typed_tool_health_does_not_promote_fixture_cleanup_to_environment_proof(tmp_path):
    from r2f.modules.s1.topology_input import TOPOLOGY_FACT_HEALTH
    facts = RawFixture().facts(tmp_path)
    assert facts["diagnostic_valid"] and facts["process_healthy"]
    assert not facts["environment_healthy"]
    assert facts["fact_values"][TOPOLOGY_FACT_HEALTH] is None
