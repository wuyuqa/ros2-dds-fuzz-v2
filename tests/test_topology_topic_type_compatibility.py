"""CP2 public input/raw/facts/oracle contracts; no ROS or DDS is executed."""
import pytest

from r2f.modules.s1 import topology_model as model
from test_topology_no_obligation_facts import ObservedPeers, consume, produce, rewrite

STRING = "std_msgs/msg/String"
ARRAY = "std_msgs/msg/UInt8MultiArray"


class TopicTypePeers(ObservedPeers):
    """External graph/match fixture with explicitly chosen diagonal edges.

    The edge table is independent of the production compatibility evaluator.
    Neither the fixture nor its diagnostic verdict qualifies an actual SUT.
    """
    def __init__(self, *, matched_pairs=(("w0", "r0"), ("w1", "r1"))):
        super().__init__(auto_match=False)
        self.matched_pairs = matched_pairs

    def prepare(self, program, config):
        result = super().prepare(program, config)
        for writer, reader in self.matched_pairs:
            self.set_match_edge(writer, reader)
        return result


def mixed_program():
    program = model.generate_program(7, writers=2, readers=2)
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/offline/topic-type/a"
        endpoint["type_name"] = STRING if endpoint["logical_id"].endswith("0") else ARRAY
    return program


def test_mixed_type_graph_retains_each_endpoint_type_and_normal_controls(tmp_path):
    runtime, config = produce(tmp_path, program=mixed_program(), backend=TopicTypePeers())
    created = {row["endpoint_gid"]: row for row in runtime["events"]
               if row["event"] == "endpoint_created"}
    graphs = [row for row in runtime["events"] if row["event"] == "graph_snapshot"]
    assert graphs
    for graph in graphs:
        for row in graph["publishers"] + graph["subscriptions"]:
            assert row["topic_type"] == created[row["endpoint_gid"]]["message_type"]
    facts, oracle = consume(runtime, config)
    pairs = {(p["writer"], p["reader"]): p for p in facts["pairs"]}
    for pair in (("w0", "r0"), ("w1", "r1")):
        assert pairs[pair]["compatibility"] == "compatible"
        assert pairs[pair]["observed_match"] == "bidirectional"
        assert pairs[pair]["delivery_closure"] == "closed"
    for pair in (("w0", "r1"), ("w1", "r0")):
        assert pairs[pair]["compatibility"] == "incompatible"
        assert pairs[pair]["observed_match"] == "none"
        assert pairs[pair]["delivery_closure"] == "no_obligation"
    assert oracle["verdict"] == "pass", oracle
    assert not facts["candidate_eligible"] and not runtime["real_execution"]


@pytest.mark.parametrize("qos_available", [True, False])
def test_type_refusal_is_named_separately_from_qos(tmp_path, qos_available):
    runtime, config = produce(tmp_path, program=mixed_program(), backend=TopicTypePeers())
    if not qos_available:
        def remove_qos(events):
            for event in events:
                if event["event"] == "graph_snapshot":
                    for row in event["publishers"] + event["subscriptions"]:
                        row.pop("qos")
        rewrite(runtime, remove_qos)
    facts, oracle = consume(runtime, config)
    pair = next(p for p in facts["pairs"] if (p["writer"], p["reader"]) == ("w0", "r1"))
    proof = pair["proofs"]["compatibility"]
    policy = proof["detail"]["policy_scope"]
    assert policy["reason"] == "observed_type_mismatch"
    assert policy["writer_type"] == STRING and policy["reader_type"] == ARRAY
    assert policy["actual_match"] == "not_evaluated"
    assert proof["refs"] and all(ref["sha256"] == runtime["raw_sha256"] for ref in proof["refs"])
    assert oracle["verdict"] == ("pass" if qos_available else "unknown")
    assert not oracle["forbidden_residuals"]


def test_consistent_graph_type_cannot_override_created_endpoint_binding(tmp_path):
    runtime, config = produce(tmp_path, program=mixed_program(), backend=TopicTypePeers())
    def replace_types(events):
        for event in events:
            if event["event"] == "graph_snapshot":
                for row in event["publishers"] + event["subscriptions"]:
                    row["topic_type"] = STRING
    rewrite(runtime, replace_types)
    facts, oracle = consume(runtime, config)
    pair = next(p for p in facts["pairs"] if (p["writer"], p["reader"]) == ("w1", "r1"))
    assert pair["compatibility"] == "unknown"
    assert pair["proofs"]["compatibility"]["reason"] == "observed_endpoint_type_binding_mismatch"
    assert oracle["verdict"] == "unknown"
    assert not oracle["candidate_eligible"] and not oracle["forbidden_residuals"]


def test_four_by_four_types_and_topics_keep_normal_and_refused_pairs_separate(tmp_path):
    program = model.generate_program(7, writers=4, readers=4)
    for endpoint in program["endpoints"]:
        suffix = endpoint["logical_id"][-1]
        endpoint["topic"] = "/offline/topic-type/" + ("a" if suffix in ("0", "1") else "b")
        endpoint["type_name"] = STRING if suffix in ("0", "2") else ARRAY
    matched = {("w0", "r0"), ("w1", "r1"), ("w2", "r2"), ("w3", "r3")}
    type_refused = {("w0", "r1"), ("w1", "r0"), ("w2", "r3"), ("w3", "r2")}
    runtime, config = produce(tmp_path, program=program, backend=TopicTypePeers(matched_pairs=matched))
    facts, oracle = consume(runtime, config)
    assert len(facts["pairs"]) == 16
    for pair in facts["pairs"]:
        ids = pair["writer"], pair["reader"]
        proof = pair["proofs"]["compatibility"]
        if ids in matched:
            assert pair["compatibility"] == "compatible"
            assert pair["observed_match"] == "bidirectional"
            assert pair["delivery_closure"] == "closed"
        else:
            assert pair["compatibility"] == "incompatible"
            assert pair["observed_match"] == "none"
            assert pair["delivery_closure"] == "no_obligation"
            assert proof["detail"]["policy_scope"]["reason"] == (
                "observed_type_mismatch" if ids in type_refused else "observed_topic_mismatch")
        assert all(ref["sha256"] == runtime["raw_sha256"] for ref in proof["refs"])
    assert facts["unresolved_historical_obligations"] == []
    assert oracle["verdict"] == "pass", oracle
    assert oracle["diagnostic_only"] and not oracle["candidate_eligible"]


def test_qos_refusal_keeps_requested_offered_attribution(tmp_path):
    runtime, config = produce(tmp_path, program=mixed_program(),
                              backend=TopicTypePeers(matched_pairs=(("w1", "r1"),)))
    writer_gid = next(row["endpoint_gid"] for row in runtime["events"]
                      if row["event"] == "endpoint_created" and row["logical_id"] == "w0")
    def change_offered_reliability(events):
        for event in events:
            if event["event"] == "graph_snapshot":
                for row in event["publishers"]:
                    if row["endpoint_gid"] == writer_gid:
                        row["qos"]["reliability"] = "best_effort"
    rewrite(runtime, change_offered_reliability)
    facts, oracle = consume(runtime, config)
    pair = next(p for p in facts["pairs"] if (p["writer"], p["reader"]) == ("w0", "r0"))
    assert pair["compatibility"] == "incompatible" and pair["observed_match"] == "none"
    policy = pair["proofs"]["compatibility"]["detail"]["policy_scope"]
    assert policy["kind"] == "offered_requested_compatibility"
    assert policy["predicates"]["reliability"]["compatible"] is False
    assert oracle["verdict"] == "pass" and not oracle["forbidden_residuals"]


def test_incompatible_type_match_remains_residual_and_never_a_fixture_candidate(tmp_path):
    runtime, config = produce(tmp_path, program=mixed_program(), backend=TopicTypePeers(
        matched_pairs=(("w0", "r0"), ("w1", "r1"), ("w0", "r1"))))
    facts, oracle = consume(runtime, config)
    pair = next(p for p in facts["pairs"] if (p["writer"], p["reader"]) == ("w0", "r1"))
    assert pair["compatibility"] == "incompatible"
    assert pair["observed_match"] == "bidirectional"
    assert oracle["verdict"] == "fail"
    assert "topology.pair_graph_mismatch" in oracle["forbidden_residuals"]
    assert oracle["diagnostic_only"] and not oracle["candidate_eligible"]
