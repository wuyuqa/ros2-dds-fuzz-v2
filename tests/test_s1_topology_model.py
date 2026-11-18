"""F3 pure topology-model contracts; no runtime/backend/campaign or ROS imports.

All fixtures are authored input assumptions, not endpoint observations.  Mutation
receipts below describe input transformations only: deterministic regeneration is
not an execution replay, and an ``allowed`` pair is never a matched pair.  The
public model API permits 1..16 mutation layers; 1..4 are each checked for
identity continuity here, without invoking runtime ``config.input_mutations``.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys

import pytest

from r2f.modules.s1 import topology_model as model


def digest(value):
    """Independent public-wire JSON identity, not the model's private hash helper."""
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def qos(**overrides):
    # Authored explicit input values; deliberately do not consult SDK defaults.
    result = {"reliability": "reliable", "durability": "volatile",
              "deadline_sec": "infinite", "liveliness": "automatic",
              "lease_duration_sec": "infinite", "ownership": "shared",
              "partitions": [], "history": "keep_last", "depth": 16}
    result.update(overrides)
    return result


def endpoint(eid, role, **overrides):
    result = {"logical_id": eid, "role": role, "topic": "/authored/topic",
              "type_name": "std_msgs/msg/String", "qos": qos(),
              "vendor": None, "participant_id": None}
    result.update(overrides)
    return result


def action(aid, op, eid=None, generation=None, *, depends_on=(), **params):
    return {"action_id": aid, "op": op, "endpoint_id": eid,
            "generation": generation, "params": params,
            "depends_on": list(depends_on)}


def program(endpoints=None, actions=None, **environment):
    if endpoints is None:
        endpoints = [endpoint("w0", "writer"), endpoint("r0", "reader")]
    if actions is None:
        actions = [action("create_" + ep["logical_id"], "create", ep["logical_id"], 1)
                   for ep in endpoints]
    env = {"backend_available": True, "transport_ready": True,
           "discovery_enabled": True, "type_matching": "exact_name",
           "filter_support": None, "filter_validity": None}
    env.update(environment)
    return {"schema": model.SCHEMA, "endpoints": endpoints,
            "actions": actions, "environment": env}


def pair_result(p, prefix=None):
    graph = model.predict_graph(p, prefix)
    assert graph["status"] == "valid", graph
    assert len(graph["pairs"]) == 1
    return graph["pairs"][0]


def requirement(row, name):
    matches = [r for r in row["requires"] if r["id"] == name]
    assert len(matches) == 1, row
    return matches[0]


def assert_invalid(p, *, code=None, path=None):
    result = model.validate_program(p)
    assert result["status"] == "invalid", result
    assert result["guard_disposition"] == "invalid_input"
    assert result["execution_evidence"] is False
    assert result["errors"]
    if code is not None or path is not None:
        assert any((code is None or e["code"] == code)
                   and (path is None or e["path"] == path) for e in result["errors"]), result
    assert model.predict_graph(p)["status"] == "invalid"
    with pytest.raises(ValueError, match="structurally valid"):
        model.fingerprints(p)
    return result


def mutation_base():
    p = program()
    p["actions"].extend([
        action("publish_w0", "publish_batch", "w0", 1, count=2, start_sequence=0),
        action("final", "snapshot", label="authored final"),
    ])
    return p


def test_model_import_is_pure_in_fresh_interpreter():
    # Audit imports in an isolated Python child, not a runtime worker.  In
    # particular this test must not import the existing topology runtime tests.
    src = str(Path(model.__file__).resolve().parents[3])
    code = r'''
import builtins, json, sys
sys.path.insert(0, sys.argv[1])
original = builtins.__import__
blocked = ("rclpy", "rcl_interfaces", "r2f.runtime", "r2f.campaign", "r2f.instrumentation")
def checked(name, *args, **kwargs):
    if any(name == x or name.startswith(x + ".") for x in blocked):
        raise AssertionError("forbidden import: " + name)
    return original(name, *args, **kwargs)
builtins.__import__ = checked
from r2f.modules.s1 import topology_model
p = topology_model.generate_program(7, writers=4, readers=4)
assert topology_model.predict_graph(p)["execution_evidence"] is False
assert not any(any(n == x or n.startswith(x + ".") for x in blocked) for n in sys.modules)
print(json.dumps({"pure_model_import": True}))
'''
    proc = subprocess.run([sys.executable, "-I", "-c", code, src],
                          capture_output=True, text=True, timeout=15, check=False)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == {"pure_model_import": True}


@pytest.mark.parametrize("writers", range(5))
@pytest.mark.parametrize("readers", range(5))
def test_generate_all_role_cardinalities_zero_through_four(writers, readers):
    p = model.generate_program(19, writers=writers, readers=readers)
    before = copy.deepcopy(p)
    v = model.validate_program(p)
    g = model.predict_graph(p)
    assert v["status"] == g["status"] == "valid"
    assert sum(e["role"] == "writer" for e in p["endpoints"]) == writers
    assert sum(e["role"] == "reader" for e in p["endpoints"]) == readers
    assert len(p["endpoints"]) == writers + readers <= 8
    assert len(g["pairs"]) == writers * readers
    assert len(v["runtime_bindings"]) == writers + readers
    assert all(b["status"] == "unbound" and b["required"] == ["guid", "pid"]
               for b in v["runtime_bindings"])
    assert all(e["vendor"] is None and e["participant_id"] is None for e in p["endpoints"])
    assert v["execution_evidence"] is v["candidate_eligible"] is False
    assert p == before


@pytest.mark.parametrize("writers,readers", [(5, 0), (0, 5), (-1, 0), (0, -1),
                                              (True, 0), (0, False), (1.0, 0)])
def test_generate_rejects_role_cardinality_outside_exact_integer_bounds(writers, readers):
    with pytest.raises(ValueError, match="cardinality"):
        model.generate_program(0, writers=writers, readers=readers)


def test_total_eight_and_independent_role_budget_edges():
    full = model.generate_program(1, writers=4, readers=4)
    assert len(model.predict_graph(full)["pairs"]) == 16
    with pytest.raises(ValueError, match="cardinality"):
        model.generate_program(1, writers=4, readers=4, budget={"max_endpoints": 7})
    over_total = copy.deepcopy(full)
    over_total["endpoints"].append(endpoint("extra", "reader"))
    assert_invalid(over_total, path="program.endpoints", code="budget")
    # Eight total is insufficient if the per-role cap is exceeded.
    over_role = program([endpoint(f"w{i}", "writer") for i in range(5)]
                        + [endpoint(f"r{i}", "reader") for i in range(3)])
    invalid = assert_invalid(over_role, code="budget")
    assert any("writer count" in e["message"] for e in invalid["errors"])
    empty = model.generate_program(0, budget={"max_writers": 0, "max_readers": 0,
                                            "max_endpoints": 0})
    assert empty["endpoints"] == []
    assert model.predict_graph(empty)["pairs"] == []


@pytest.mark.parametrize("budget", [{"max_writers": 5}, {"max_readers": 5},
                                    {"max_endpoints": 9}, {"max_actions": True},
                                    {"max_timeout_sec": float("nan")},
                                    {"max_generations": 0}, {"unknown": 1}])
def test_budget_ceiling_and_type_rejection(budget):
    with pytest.raises(ValueError, match="budget"):
        model.generate_program(0, budget=budget)


def test_allowed_is_policy_prediction_not_match_or_execution_evidence():
    p = program()
    v, g = model.validate_program(p), model.predict_graph(p)
    assert v["guard_disposition"] == "satisfied_under_declared_assumptions"
    assert v["environment_is_declared_assumption"] is True
    assert g["prediction_kind"] == "policy_only_not_observed"
    assert len(g["allowed"]) == 1
    assert g["execution_evidence"] is False
    edge = g["allowed"][0]
    assert edge["status"] == "allowed" and edge["execution_evidence"] is False
    assert "matched" not in edge and "guid" not in edge
    assert edge["rule_refs"]
    assert all(r["prediction_only"] for r in g["actions"])
    assert all(b["status"] == "unbound" for b in g["runtime_bindings"])


@pytest.mark.parametrize("prefix", [-1, 3, True, 1.0, "not_an_action"])
def test_invalid_prefix_never_becomes_prediction(prefix):
    p = program()
    g = model.predict_graph(p, prefix)
    assert g["status"] == "invalid" and g["pairs"] == []
    assert g["errors"][0]["code"] == "prefix"
    with pytest.raises(ValueError, match="prefix"):
        model.fingerprints(p, prefix)


def test_prefix_action_id_is_inclusive_and_zero_is_before_creation():
    p = program()
    assert pair_result(p, 0)["status"] == "forbidden"
    assert pair_result(p, "create_w0")["status"] == "forbidden"
    assert pair_result(p, "create_r0")["status"] == "allowed"
    assert model.fingerprints(p, "create_w0") == model.fingerprints(p, 1)
    assert model.fingerprints(p, None) == model.fingerprints(p, 2)


def test_identical_state_different_prefix_history_and_input_identity():
    p = program()
    p["actions"].append(action("observe", "snapshot", label="no logical transition"))
    left, right = model.fingerprints(p, 2), model.fingerprints(p, 3)
    assert left["input_sha256"] == right["input_sha256"] == digest(p)
    assert left["state_sha256"] == right["state_sha256"]
    assert left["history_sha256"] != right["history_sha256"]
    swapped = copy.deepcopy(p)
    swapped["actions"][:2] = reversed(swapped["actions"][:2])
    other = model.fingerprints(swapped, 2)
    assert left["state_sha256"] == other["state_sha256"]
    assert left["history_sha256"] != other["history_sha256"]
    assert left["input_sha256"] != other["input_sha256"]


def test_future_suffix_and_budget_change_input_not_prefix_state_or_history():
    p, changed = program(), program()
    changed["actions"].append(action("future", "destroy", "w0", 1))
    changed["budget"] = {"max_actions": 129}
    a, b = model.fingerprints(p, 2), model.fingerprints(changed, 2)
    assert a["input_sha256"] != b["input_sha256"]
    assert a["state_sha256"] == b["state_sha256"]
    assert a["history_sha256"] == b["history_sha256"]


def test_destroy_recreate_logical_generations_reset_only_that_writer_sequence():
    p = program()
    p["actions"].extend([
        action("pub_g1", "publish_batch", "w0", 1, count=2, start_sequence=7),
        action("destroy_w0", "destroy", "w0", 1),
        action("recreate_w0", "recreate", "w0", 2),
        action("pub_g2", "publish_batch", "w0", 2, count=2, start_sequence=0),
    ])
    v = model.validate_program(p)
    assert v["status"] == "valid"
    assert v["guard_disposition"] == "satisfied_under_declared_assumptions"
    assert pair_result(p, "destroy_w0")["status"] == "forbidden"
    new_pair = pair_result(p, "recreate_w0")
    assert new_pair["status"] == "allowed"
    assert (new_pair["writer_generation"], new_pair["reader_generation"]) == (2, 1)
    assert v["delivery_streams"] == [
        {"writer_id": "w0", "generation": 1, "planned_sequences": [7, 8],
         "action_ids": ["pub_g1"], "prediction_only": True, "order_scope": "writer_generation"},
        {"writer_id": "w0", "generation": 2, "planned_sequences": [0, 1],
         "action_ids": ["pub_g2"], "prediction_only": True, "order_scope": "writer_generation"},
    ]
    assert [(b["logical_id"], b["generation"]) for b in v["runtime_bindings"]] == [
        ("w0", 1), ("r0", 1), ("w0", 2)]
    assert all(set(b) == {"logical_id", "generation", "required", "status"}
               for b in v["runtime_bindings"])
    assert model.fingerprints(p, 2)["state_sha256"] != model.fingerprints(p, 5)["state_sha256"]


@pytest.mark.parametrize("bad,requires", [
    (action("again", "create", "w0", 1), "first_generation"),
    (action("skip", "recreate", "w0", 3), "next_generation"),
    (action("stale", "publish_batch", "w0", 2, count=1, start_sequence=0), "current_generation"),
    (action("reader_pub", "publish_batch", "r0", 1, count=1, start_sequence=0), "writer_role"),
    (action("assert_auto", "assert_liveliness", "w0", 1), "public_writer_manual_by_topic"),
])
def test_lifecycle_and_role_negative_is_valid_guard_not_structural_error(bad, requires):
    p = program()
    p["actions"].append(bad)
    v = model.validate_program(p)
    assert v["status"] == "valid" and v["guard_disposition"] == "guard_violation"
    assert requirement(v["actions"][-1], requires)["status"] == "unsatisfied"
    assert v["actions"][-1]["disposition"] == "expected_guard_reject"
    assert v["candidate_eligible"] is False


def test_destroyed_endpoint_and_initial_generation_guard_do_not_apply():
    p = program()
    p["actions"].extend([action("destroy", "destroy", "w0", 1),
                         action("dead_pub", "publish_batch", "w0", 1, count=1, start_sequence=0)])
    v = model.validate_program(p)
    assert requirement(v["actions"][-1], "endpoint_alive")["status"] == "unsatisfied"
    assert v["delivery_streams"] == []
    p["actions"] = [action("wrong_first", "create", "w0", 2),
                    action("no_previous", "recreate", "w0", 1)]
    v = model.validate_program(p)
    assert requirement(v["actions"][0], "first_generation")["status"] == "unsatisfied"
    assert requirement(v["actions"][1], "previous_generation_exists")["status"] == "unsatisfied"
    assert v["runtime_bindings"] == []


@pytest.mark.parametrize("policy,status", [("manual_by_topic", "satisfied"), (None, "unknown")])
def test_public_liveliness_assertion_requires_explicit_writer_policy(policy, status):
    p = program()
    p["endpoints"][0]["qos"]["liveliness"] = policy
    p["actions"].append(action("assert", "assert_liveliness", "w0", 1))
    v = model.validate_program(p)
    assert requirement(v["actions"][-1], "public_writer_manual_by_topic")["status"] == status


@pytest.mark.parametrize("key,offered,requested,status", [
    ("reliability", offered, requested, "allowed" if wi >= ri else "forbidden")
    for wi, offered in enumerate(("best_effort", "reliable"))
    for ri, requested in enumerate(("best_effort", "reliable"))
] + [
    (key, offered, requested, "allowed" if wi >= ri else "forbidden")
    for key, policies in (("durability", ("volatile", "transient_local", "transient", "persistent")),
                          ("liveliness", ("automatic", "manual_by_participant", "manual_by_topic")))
    for wi, offered in enumerate(policies) for ri, requested in enumerate(policies)
])
def test_requested_offered_enums_are_directional(key, offered, requested, status):
    p = program()
    p["endpoints"][0]["qos"][key] = offered
    p["endpoints"][1]["qos"][key] = requested
    edge = pair_result(p)
    assert edge["status"] == status
    if status == "forbidden":
        assert key + "_requested_offered" in edge["reasons"]


@pytest.mark.parametrize("key", ["deadline_sec", "lease_duration_sec"])
@pytest.mark.parametrize("offered,requested,status", [
    (0, 0, "allowed"), (1, 10, "allowed"), (10, 1, "forbidden"),
    (1, "infinite", "allowed"), ("infinite", 1, "forbidden"),
    ("infinite", "infinite", "allowed"), (86400, 86400, "allowed"),
])
def test_requested_offered_durations_use_shorter_offered_not_greater_rank(key, offered, requested, status):
    p = program()
    p["endpoints"][0]["qos"][key] = offered
    p["endpoints"][1]["qos"][key] = requested
    assert pair_result(p)["status"] == status


@pytest.mark.parametrize("key", ["reliability", "durability", "liveliness", "deadline_sec",
                                  "lease_duration_sec", "ownership", "partitions"])
@pytest.mark.parametrize("side", [0, 1])
@pytest.mark.parametrize("missing", [False, True], ids=["null", "omitted"])
def test_unknown_qos_never_uses_sdk_defaults_or_endpoint_counts(key, side, missing):
    p = program()
    if missing:
        del p["endpoints"][side]["qos"][key]
    else:
        p["endpoints"][side]["qos"][key] = None
    edge = pair_result(p)
    assert edge["status"] == "unknown"
    assert "unknown_" + key in edge["reasons"]
    assert edge["execution_evidence"] is False


@pytest.mark.parametrize("mode,status", [("exact_name", "forbidden"), ("assignable", "unknown"),
                                         (None, "unknown")])
def test_type_name_mismatch_only_forbidden_under_explicit_exact_name_contract(mode, status):
    p = program(type_matching=mode)
    p["endpoints"][1]["type_name"] = "custom_msgs/msg/Other"
    edge = pair_result(p)
    assert edge["status"] == status
    assert "type_name_mismatch" in edge["reasons"]


def test_topic_mismatch_and_known_incompatibility_win_over_unknowns():
    p = program(transport_ready=None, discovery_enabled=None)
    p["endpoints"][1]["topic"] = "/other"
    p["endpoints"][0]["qos"]["deadline_sec"] = None
    edge = pair_result(p)
    assert edge["status"] == "forbidden"
    assert "topic_mismatch" in edge["reasons"]
    assert "unknown_deadline_sec" in edge["reasons"]
    assert "unverified_environment_transport_ready" in edge["reasons"]


@pytest.mark.parametrize("writer,reader,status,reason", [
    ([], [""], "allowed", None), (["a"], ["a", "b"], "allowed", None),
    (["a"], ["b"], "forbidden", "partition_mismatch"),
    (["prefix*"], ["prefix1"], "unknown", "partition_pattern_not_evaluated"),
])
def test_partition_default_literal_intersection_and_unsupported_wildcards(writer, reader, status, reason):
    p = program()
    p["endpoints"][0]["qos"]["partitions"] = writer
    p["endpoints"][1]["qos"]["partitions"] = reader
    edge = pair_result(p)
    assert edge["status"] == status
    if reason:
        assert reason in edge["reasons"]


def test_identical_partition_pattern_is_not_proven_literal_compatibility():
    p = program()
    for ep in p["endpoints"]:
        ep["qos"]["partitions"] = ["prefix*"]
    edge = pair_result(p)
    assert edge["status"] == "unknown"
    assert "partition_pattern_not_evaluated" in edge["reasons"]


@pytest.mark.parametrize("ownership", ["shared", "exclusive"])
def test_ownership_requires_equality_not_requested_offered_rank(ownership):
    p = program()
    for ep in p["endpoints"]:
        ep["qos"]["ownership"] = ownership
    assert pair_result(p)["status"] == "allowed"
    p["endpoints"][1]["qos"]["ownership"] = "exclusive" if ownership == "shared" else "shared"
    assert "ownership_mismatch" in pair_result(p)["reasons"]


def test_history_depth_filter_evaluation_and_vendor_labels_are_not_match_evidence():
    p = program(filter_support=True, filter_validity=True)
    w, r = p["endpoints"]
    w.update(vendor="fastdds", participant_id="declared_writer_participant")
    r.update(vendor="cyclonedds", participant_id="declared_reader_participant")
    w["qos"].update(history="keep_all", depth=None)
    r["qos"].update(history="keep_last", depth=1)
    r["filter"] = {"expression": "seq >= %0", "parameters": ["100"], "evaluation": False}
    edge = pair_result(p)
    assert edge["status"] == "allowed" and edge["filter_changes_match"] is False
    assert edge["execution_evidence"] is False
    assert all(b["status"] == "unbound" for b in model.validate_program(p)["runtime_bindings"])


def test_pair_identity_cannot_be_replaced_by_equal_match_counts():
    p = program([endpoint("w0", "writer", topic="/a"), endpoint("w1", "writer", topic="/b"),
                 endpoint("r0", "reader", topic="/a"), endpoint("r1", "reader", topic="/b")])
    swapped = copy.deepcopy(p)
    swapped["endpoints"][2]["topic"], swapped["endpoints"][3]["topic"] = "/b", "/a"
    g, other = model.predict_graph(p), model.predict_graph(swapped)
    assert len(g["pairs"]) == len(other["pairs"]) == 4
    assert len(g["allowed"]) == len(other["allowed"]) == 2
    assert {(x["writer_id"], x["reader_id"]) for x in g["allowed"]} == {("w0", "r0"), ("w1", "r1")}
    assert {(x["writer_id"], x["reader_id"]) for x in other["allowed"]} == {("w0", "r1"), ("w1", "r0")}
    assert g["fingerprints"]["state_sha256"] != other["fingerprints"]["state_sha256"]
    assert g["fingerprints"]["history_sha256"] != other["fingerprints"]["history_sha256"]


@pytest.mark.parametrize("key", ["transport_ready", "discovery_enabled"])
@pytest.mark.parametrize("value", [False, None])
def test_unverified_pair_environment_remains_unknown(key, value):
    p = program(**{key: value})
    edge = pair_result(p)
    assert edge["status"] == "unknown"
    assert "unverified_environment_" + key in edge["reasons"]


@pytest.mark.parametrize("value,disposition,alive_status", [
    (False, "expected_guard_reject", "forbidden"), (None, "requires_runtime", "unknown")])
def test_backend_false_vs_unknown_environment_guards(value, disposition, alive_status):
    p = program(backend_available=value)
    v = model.validate_program(p)
    assert v["status"] == "valid"
    assert all(r["disposition"] == disposition for r in v["actions"])
    assert pair_result(p)["status"] == alive_status
    assert v["execution_evidence"] is v["candidate_eligible"] is False


@pytest.mark.parametrize("support,validity,disposition", [(None, None, "requires_runtime"),
                                                         (False, True, "expected_guard_reject"),
                                                         (True, False, "expected_guard_reject"),
                                                         (True, True, "expected_apply")])
def test_filter_support_and_validity_are_separate_environment_guards(support, validity, disposition):
    p = program(filter_support=support, filter_validity=validity)
    p["endpoints"][1]["filter"] = {"expression": "seq >= %0", "parameters": ["1"], "evaluation": None}
    row = model.validate_program(p)["actions"][1]
    assert row["disposition"] == disposition
    assert {r["id"] for r in row["requires"]} >= {"filter_support", "filter_validity"}


def test_observation_receipts_and_dependencies_are_not_fabricated_offline():
    p = program()
    p["actions"].extend([
        action("barrier", "barrier", condition="matched", timeout_sec=1,
               endpoint_ids=["w0", "r0"]),
        action("after", "snapshot", depends_on=["barrier"]),
        action("bad", "snapshot", depends_on=["future"]),
        action("future", "snapshot"),
    ])
    v = model.validate_program(p)
    assert v["status"] == "valid" and v["guard_disposition"] == "guard_violation"
    rows = {r["action_id"]: r for r in v["actions"]}
    assert requirement(rows["barrier"], "runtime_barrier:matched")["status"] == "unknown"
    assert requirement(rows["after"], "action:barrier")["status"] == "unknown"
    assert requirement(rows["bad"], "action:future")["status"] == "unsatisfied"
    assert rows["after"]["disposition"] == "requires_runtime"
    assert rows["bad"]["disposition"] == "expected_guard_reject"


def test_writer_scoped_order_allows_interleaving_and_shared_sequence_numbers():
    p = program([endpoint("w0", "writer"), endpoint("w1", "writer"), endpoint("r0", "reader")])
    p["actions"].extend([
        action("w0_first", "publish_batch", "w0", 1, count=2, start_sequence=0),
        action("w1_first", "publish_batch", "w1", 1, count=1, start_sequence=0),
        action("w0_later", "publish_batch", "w0", 1, count=1, start_sequence=9),
        action("w1_later", "publish_batch", "w1", 1, count=2, start_sequence=1),
    ])
    g = model.predict_graph(p)
    assert g["status"] == "valid" and g["cross_writer_order_required"] is False
    assert all(r["disposition"] == "expected_apply" for r in g["actions"])
    streams = {(s["writer_id"], s["generation"]): s for s in g["delivery_streams"]}
    assert streams[("w0", 1)]["planned_sequences"] == [0, 1, 9]
    assert streams[("w1", 1)]["planned_sequences"] == [0, 1, 2]
    assert all(s["order_scope"] == "writer_generation" for s in streams.values())
    p["actions"].append(action("overlap", "publish_batch", "w0", 1, count=1, start_sequence=1))
    v = model.validate_program(p)
    assert v["guard_disposition"] == "guard_violation"
    assert requirement(v["actions"][-1], "nonoverlapping_writer_generation_sequence")["status"] == "unsatisfied"
    assert v["delivery_streams"] == g["delivery_streams"]


@pytest.mark.parametrize("mutator,status,intent,guard", [
    ("structure_guard", "invalid", "invalid_structure", "invalid_input"),
    ("parameter_guard", "invalid", "invalid_parameters", "invalid_input"),
    ("dependency_guard", "valid", "guard_negative", "guard_violation"),
    ("environment_guard", "valid", "guard_negative", "guard_violation"),
])
def test_guard_mutators_separate_structure_parameters_dependencies_environment(mutator, status, intent, guard):
    base = mutation_base()
    before = copy.deepcopy(base)
    result = model.mutate_program(base, mutator, seed=3)
    assert result["intent"] == intent
    assert result["validation"]["status"] == status
    assert result["expected_guard_disposition"] == guard
    assert result["execution_evidence"] is result["candidate_eligible"] is False
    assert result["receipt"]["execution_evidence"] is False
    assert result["receipt"]["before_sha256"] == digest(base)
    assert result["receipt"]["after_sha256"] == digest(result["program"]) == result["input_sha256"]
    assert base == before


@pytest.mark.parametrize("seed", [0, 1, -17, 20261118, 2**40])
def test_generator_seed_is_deterministic_and_does_not_consume_global_rng(seed):
    before = random.getstate()
    a = model.generate_program(seed)
    assert a == model.generate_program(seed)
    assert model.validate_program(a)["input_sha256"] == digest(a)
    assert random.getstate() == before


@pytest.mark.parametrize("random_cases", [0, 8, 64])
def test_seed_corpus_is_deterministic_deduplicated_bounded_input_only(random_cases):
    before = random.getstate()
    a = model.seed_corpus(23, random_cases=random_cases)
    assert a == model.seed_corpus(23, random_cases=random_cases)
    assert random.getstate() == before
    assert len({c["input_sha256"] for c in a}) == len(a)
    counts = {(sum(e["role"] == "writer" for e in c["program"]["endpoints"]),
               sum(e["role"] == "reader" for e in c["program"]["endpoints"])) for c in a}
    assert {(0, 0), (4, 0), (0, 4), (4, 4), (1, 1)} <= counts
    for case in a:
        assert case["validation"]["status"] == "valid"
        assert case["input_sha256"] == digest(case["program"])
        assert case["execution_evidence"] is case["candidate_eligible"] is False
        assert case["receipt"] is None


def test_seed_corpus_honors_zero_role_budget_and_deduplicates_identical_empty_inputs():
    cases = model.seed_corpus(1, random_cases=8,
                             budget={"max_writers": 0, "max_readers": 0, "max_endpoints": 0})
    assert len(cases) == 1
    assert cases[0]["program"]["endpoints"] == []


@pytest.mark.parametrize("mutator", model.MUTATORS)
def test_each_mutator_is_deterministic_non_aliasing_and_hashes_actual_final_input(mutator):
    base = mutation_base()
    before, rng = copy.deepcopy(base), random.getstate()
    a = model.mutate_program(base, mutator, seed=11)
    b = model.mutate_program(base, mutator, seed=11)
    assert a == b and base == before and random.getstate() == rng
    receipt = a["receipt"]
    assert receipt["mutator"] == mutator and receipt["seed"] == 11
    assert receipt["before_sha256"] == digest(base)
    assert receipt["after_sha256"] == a["input_sha256"] == digest(a["program"])
    assert receipt["before_sha256"] != receipt["after_sha256"]
    assert receipt["execution_evidence"] is a["execution_evidence"] is a["candidate_eligible"] is False
    a["program"]["endpoints"][0]["qos"]["depth"] = 99
    assert base == before and b["program"]["endpoints"][0]["qos"]["depth"] == 16


@pytest.mark.parametrize("length", [1, 2, 3, 4])
def test_one_to_four_input_layers_are_continuous_and_bind_final_program_not_runtime_replay(length):
    base = mutation_base()
    before = copy.deepcopy(base)
    mutators = ["change_topic", "change_qos", "insert_action", "change_filter"][:length]
    result = model.mutate_chain(base, mutators, seed=37)
    assert result == model.mutate_chain(base, mutators, seed=37)
    assert result["initial_sha256"] == digest(base)
    assert len(result["layers"]) == length
    current, previous = copy.deepcopy(base), digest(base)
    for i, (name, receipt) in enumerate(zip(mutators, result["layers"])):
        single = model.mutate_program(current, name, seed=37 + i)
        assert receipt == single["receipt"]
        assert receipt["before_sha256"] == previous == digest(current)
        assert receipt["seed"] == 37 + i
        current = single["program"]
        previous = receipt["after_sha256"]
        assert previous == digest(current)
        assert receipt["execution_evidence"] is False
    assert current == result["program"]
    assert previous == result["input_sha256"] == digest(result["program"])
    assert result["receipt"] is None  # Nested layers, no fabricated execution receipt.
    assert result["execution_evidence"] is result["candidate_eligible"] is False
    assert base == before
    frozen_input = copy.deepcopy(result["program"])
    model.predict_graph(frozen_input)
    assert frozen_input == result["program"] and digest(frozen_input) == previous


def test_chain_actual_api_limit_is_sixteen_not_four():
    result = model.mutate_chain(mutation_base(), ["insert_action"] * 16, seed=3)
    assert len(result["layers"]) == 16
    assert result["input_sha256"] == result["layers"][-1]["after_sha256"]
    for bad in ([], ["insert_action"] * 17, ["not_a_mutator"], "change_topic"):
        with pytest.raises(ValueError, match="1..16"):
            model.mutate_chain(mutation_base(), bad)


def test_structurally_invalid_chain_output_is_allowed_only_at_terminal_layer():
    base = mutation_base()
    result = model.mutate_chain(base, ["change_topic", "parameter_guard"], seed=5)
    assert result["validation"]["status"] == "invalid"
    assert result["layers"][0]["after_sha256"] == result["layers"][1]["before_sha256"]
    assert result["input_sha256"] == digest(result["program"])
    with pytest.raises(ValueError, match="invalid input is terminal"):
        model.mutate_chain(base, ["parameter_guard", "change_topic"])
    assert model.mutate_chain(base, ["dependency_guard"])["expected_guard_disposition"] == "guard_violation"


def test_delete_action_preserves_dependency_closure_and_does_not_relabel_guard_negatives():
    base = mutation_base()
    for seed in range(16):
        case = model.mutate_program(base, "delete_action", seed=seed)
        kept = {a["action_id"] for a in case["program"]["actions"]}
        removed = set(case["receipt"]["details"]["removed_action_ids"])
        assert kept.isdisjoint(removed)
        assert kept | removed == {a["action_id"] for a in base["actions"]}
        assert case["validation"]["guard_disposition"] != "guard_violation"
        assert all(set(a["depends_on"]) <= kept for a in case["program"]["actions"])


def test_swap_preserves_writer_lifecycle_and_cannot_cross_global_barrier():
    p = program([endpoint("w0", "writer")])
    p["actions"].extend([
        action("pub", "publish_batch", "w0", 1, count=1, start_sequence=0),
        action("fence", "barrier", condition="all_created", timeout_sec=1),
        action("after", "snapshot"),
    ])
    with pytest.raises(ValueError, match="no adjacent independent"):
        model.mutate_program(p, "swap_independent")
    mixed = mutation_base()
    result = model.mutate_program(mixed, "swap_independent", seed=0)
    assert result["validation"]["guard_disposition"] != "guard_violation"
    swapped = result["receipt"]["details"]["swapped_action_ids"]
    old_ids = [a["action_id"] for a in mixed["actions"]]
    new_ids = [a["action_id"] for a in result["program"]["actions"]]
    first = old_ids.index(swapped[0])
    assert old_ids[first:first + 2] == swapped
    expected_ids = old_ids[:]
    expected_ids[first:first + 2] = swapped[::-1]
    assert new_ids == expected_ids
    assert new_ids.index("create_w0") < new_ids.index("publish_w0")
    assert new_ids[-1] == "final"
    # Reader creation and another writer's publish may also be independent;
    # do not invent an all-endpoints-created or cross-writer ordering fence.
    old_actions = {a["action_id"]: a for a in mixed["actions"]}
    assert old_actions[swapped[0]]["endpoint_id"] != old_actions[swapped[1]]["endpoint_id"]
    assert {a["action_id"]: a for a in result["program"]["actions"]} == old_actions


@pytest.mark.parametrize("mutator,p,reason", [
    ("add_endpoint", model.generate_program(1, writers=4, readers=4), "budget exhausted"),
    ("change_filter", program([endpoint("w0", "writer")]), "no applicable endpoint"),
    ("parameter_guard", program([], []), "no publish_batch"),
    ("delete_action", program([], []), "empty program"),
    ("change_topic", program([], []), "no applicable endpoint"),
    ("environment_guard", program(backend_available=False), "already false"),
])
def test_inapplicable_mutations_fail_explicitly_not_silent_noop(mutator, p, reason):
    before = copy.deepcopy(p)
    with pytest.raises(ValueError, match=reason):
        model.mutate_program(p, mutator)
    assert p == before


@pytest.mark.parametrize("seed", [True, False, 1.0, "1", None])
def test_seed_arguments_require_exact_integer(seed):
    with pytest.raises(ValueError):
        model.generate_program(seed)
    with pytest.raises(ValueError):
        model.seed_corpus(seed)
    with pytest.raises(ValueError):
        model.mutate_program(mutation_base(), "change_topic", seed=seed)
    with pytest.raises(ValueError):
        model.mutate_chain(mutation_base(), ["change_topic"], seed=seed)


@pytest.mark.parametrize("random_cases", [-1, 65, True, 1.0])
def test_seed_corpus_rejects_unbounded_or_noninteger_case_count(random_cases):
    with pytest.raises(ValueError, match="random_cases"):
        model.seed_corpus(0, random_cases=random_cases)


@pytest.mark.parametrize("change,path", [
    (lambda p: p.update(unknown=True), "program.unknown"),
    (lambda p: p.update(schema="wrong"), "program.schema"),
    (lambda p: p.update(budget=None), "program.budget"),
    (lambda p: p["environment"].update(backend_available=1), "program.environment.backend_available"),
    (lambda p: p["endpoints"].append(copy.deepcopy(p["endpoints"][0])), "program.endpoints[2].logical_id"),
    (lambda p: p["endpoints"][0].update(role="publisher"), "program.endpoints[0].role"),
    (lambda p: p["endpoints"][0]["qos"].update(depth=True), "program.endpoints[0].qos.depth"),
    (lambda p: p["endpoints"][0]["qos"].pop("depth"), "program.endpoints[0].qos.depth"),
    (lambda p: p["endpoints"][0]["qos"].update(deadline_sec=-1), "program.endpoints[0].qos.deadline_sec"),
    (lambda p: p["endpoints"][0].update(filter={"expression": "x", "parameters": [], "evaluation": None}),
     "program.endpoints[0].filter"),
    (lambda p: p["actions"][0].update(endpoint_id="absent"), "program.actions[0].endpoint_id"),
    (lambda p: p["actions"][0].update(generation=True), "program.actions[0].generation"),
    (lambda p: p["actions"][0].update(generation=17), "program.actions[0].generation"),
    (lambda p: p["actions"][0].update(depends_on=["x", "x"]), "program.actions[0].depends_on"),
    (lambda p: p["actions"][0]["params"].update(extra=1), "program.actions[0].params.extra"),
])
def test_structural_guard_errors_have_explicit_paths(change, path):
    p = program()
    change(p)
    assert_invalid(p, path=path)


@pytest.mark.parametrize("count", [0, -1, 65, True, 1.5])
def test_publish_parameter_guard_rejects_invalid_batch_sizes(count):
    p = mutation_base()
    p["actions"][2]["params"]["count"] = count
    assert_invalid(p, path="program.actions[2].params.count", code="budget")


@pytest.mark.parametrize("malformed", [None, [], {}, {"schema": model.SCHEMA},
                                       {"schema": model.SCHEMA, "endpoints": [], "actions": None}])
def test_malformed_top_level_returns_invalid_result_not_execution(malformed):
    assert_invalid(malformed)


@pytest.mark.parametrize("nonfinite", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_json_returns_structural_error(nonfinite):
    p = program()
    p["endpoints"][0]["qos"]["lease_duration_sec"] = nonfinite
    assert_invalid(p, path="program", code="json")


@pytest.mark.parametrize("role", [[], {}], ids=["array-role", "object-role"])
def test_json_container_role_is_reported_as_invalid_not_raised(role):
    p = program()
    p["endpoints"][0]["role"] = role
    assert_invalid(p, path="program.endpoints[0].role", code="value")


def test_omitted_environment_does_not_infer_sdk_or_host_readiness():
    p = program()
    del p["environment"]
    v = model.validate_program(p)
    assert v["status"] == "valid" and v["guard_disposition"] == "requires_runtime"
    assert all(requirement(row, "backend_available")["status"] == "unknown"
               for row in v["actions"])
    edge = pair_result(p)
    assert edge["status"] == "unknown"
    assert {"endpoint_existence_pending_runtime_guard", "unverified_environment_transport_ready",
            "unverified_environment_discovery_enabled"} <= set(edge["reasons"])


def test_mutation_delete_closes_explicit_cross_endpoint_dependencies():
    p = program()
    p["actions"][1]["depends_on"] = ["create_w0"]
    p["actions"].extend([
        action("pub", "publish_batch", "w0", 1, depends_on=["create_r0"], count=1, start_sequence=0),
        action("observe", "snapshot", depends_on=["pub"]),
    ])
    observed_root_deletion = False
    for seed in range(16):
        result = model.mutate_program(p, "delete_action", seed=seed)
        removed = result["receipt"]["details"]["removed_action_ids"]
        if "create_w0" in removed:
            observed_root_deletion = True
            assert set(removed) == {a["action_id"] for a in p["actions"]}
            assert result["program"]["actions"] == []
        assert result["validation"]["guard_disposition"] != "guard_violation"
    assert observed_root_deletion
    with pytest.raises(ValueError, match="no adjacent independent"):
        model.mutate_program(p, "swap_independent")


def test_model_chain_api_allows_sixteen_layers_not_a_runtime_four_layer_limit():
    base = mutation_base()
    result = model.mutate_chain(base, ["insert_action"] * 16, seed=13)
    assert len(result["layers"]) == 16
    assert result["validation"]["status"] == "valid"
    previous = result["initial_sha256"]
    for receipt in result["layers"]:
        assert receipt["before_sha256"] == previous
        previous = receipt["after_sha256"]
    assert previous == result["input_sha256"] == digest(result["program"])
    assert result["execution_evidence"] is False


@pytest.mark.parametrize("invalid_seed", [True, 1.5, "7", None])
def test_all_seeded_apis_reject_noninteger_seed_instead_of_coercing(invalid_seed):
    base = mutation_base()
    for call in (
        lambda: model.generate_program(invalid_seed),
        lambda: model.seed_corpus(invalid_seed),
        lambda: model.mutate_program(base, "change_topic", seed=invalid_seed),
        lambda: model.mutate_chain(base, ["change_topic"], seed=invalid_seed),
    ):
        with pytest.raises(ValueError):
            call()


@pytest.mark.parametrize("value", [-1, 65, True, 1.5])
def test_corpus_random_case_bound_is_explicit(value):
    with pytest.raises(ValueError, match="random_cases=0..64"):
        model.seed_corpus(7, random_cases=value)


@pytest.mark.parametrize("writers,readers", [(-1, 0), (0, -1), (5, 0), (0, 5), (True, 1), (1, 1.5)])
def test_generation_rejects_out_of_role_cardinality(writers, readers):
    with pytest.raises(ValueError, match="cardinality"):
        model.generate_program(0, writers=writers, readers=readers)


def test_total_endpoint_budget_is_independent_of_role_budgets():
    with pytest.raises(ValueError, match="cardinality"):
        model.generate_program(0, writers=4, readers=4, budget={"max_endpoints": 7})
    p = model.generate_program(0, writers=4, readers=4)
    assert len(p["endpoints"]) == 8
    p["budget"]["max_endpoints"] = 7
    assert_invalid(p, path="program.endpoints", code="budget")
    p = program([endpoint("w" + str(i), "writer") for i in range(5)]
                + [endpoint("r" + str(i), "reader") for i in range(3)])
    assert len(p["endpoints"]) == 8
    assert_invalid(p, path="program.endpoints", code="budget")
    p = program([endpoint("w" + str(i), "writer") for i in range(4)]
                + [endpoint("r" + str(i), "reader") for i in range(5)])
    assert_invalid(p, path="program.endpoints", code="budget")


@pytest.mark.parametrize("params,path", [
    ({"count": 1, "start_sequence": -1}, "start_sequence"),
    ({"count": 1, "start_sequence": True}, "start_sequence"),
    ({"count": 1, "start_sequence": 0, "interval_sec": -0.1}, "interval_sec"),
    ({"count": 1, "start_sequence": 0, "interval_sec": 31}, "interval_sec"),
    ({"count": 1, "start_sequence": 0, "payload": "x" * 1025}, "payload"),
])
def test_other_publish_parameter_bounds_are_structural_guards(params, path):
    p = program()
    p["actions"].append(action("publish", "publish_batch", "w0", 1, **params))
    assert_invalid(p, path="program.actions[2].params." + path)


@pytest.mark.parametrize("timeout", [0, -1, 31, True])
def test_barrier_timeout_parameter_bounds(timeout):
    p = program()
    p["actions"].append(action("wait", "barrier", condition="matched", timeout_sec=timeout))
    assert_invalid(p, path="program.actions[2].params.timeout_sec", code="budget")


def test_total_publication_budget_is_not_only_per_batch_budget():
    p = program()
    p["budget"] = {"max_total_samples": 2}
    p["actions"].extend([
        action("pub0", "publish_batch", "w0", 1, count=2, start_sequence=0),
        action("pub1", "publish_batch", "w0", 1, count=1, start_sequence=2),
    ])
    assert_invalid(p, path="program.actions", code="budget")


@pytest.mark.parametrize("path", [
    ("schema",),
    ("environment", "type_matching"),
    ("endpoints", 0, "role"),
    *[("endpoints", 0, "qos", name)
      for name in ("reliability", "durability", "liveliness", "ownership", "history")],
    ("actions", 0, "op"),
    ("actions", 2, "params", "condition"),
])
@pytest.mark.parametrize("malformed", [[], {}, ["writer"], {"role": "writer"},
                                       [[]], {"nested": []}],
                         ids=["array", "object", "string-array", "string-object",
                              "nested-array", "nested-object"])
def test_json_container_enum_values_return_invalid_json_without_hash_errors(path, malformed):
    p = program()
    p["actions"].append(action("barrier", "barrier", condition="matched", timeout_sec=1))
    current = p
    for key in path[:-1]:
        current = current[key]
    current[path[-1]] = copy.deepcopy(malformed)
    # Round-trip ensures these are malformed JSON input values, not exotic
    # Python objects whose equality/hash behavior is outside this contract.
    p = json.loads(json.dumps(p))
    before = copy.deepcopy(p)
    result = assert_invalid(p)
    assert any(e["code"] in ("value", "schema") for e in result["errors"])
    assert json.loads(json.dumps(result)) == result
    assert p == before


@pytest.mark.parametrize("role", [None, False, True, 0, 1.0, "", "participant", ["reader"], {"reader": True}])
def test_non_role_json_values_are_explicit_guard_errors(role):
    p = program()
    p["endpoints"][0]["role"] = role
    assert_invalid(p, path="program.endpoints[0].role", code="value")


@pytest.mark.parametrize("writer,reader,status", [
    (["*"], ["*"], "unknown"),
    (["prefix?"], ["prefix?"], "unknown"),
    (["[ab]"], ["[ab]"], "unknown"),
    (["prefix\\x"], ["prefix\\x"], "unknown"),
    (["prefix*", "a"], ["prefix*", "b"], "unknown"),
    (["prefix*"], ["prefix1"], "unknown"),
    (["a", "b*"], ["z"], "unknown"),
    ([], ["*"], "unknown"),
    (["a", "prefix*"], ["a", "other*"], "allowed"),
    (["a"], ["a", "*"], "allowed"),
    (["a", "*"], ["a", "*"], "allowed"),
    ([], ["", "*"], "allowed"),
    (["*", ""], [""], "allowed"),
    (["a", "b"], ["b", "c"], "allowed"),
    (["a"], ["b"], "forbidden"),
])
@pytest.mark.parametrize("reverse", [False, True], ids=["offered-requested", "requested-offered"])
def test_partition_policy_requires_proven_nonpattern_literal_overlap(writer, reader, status, reverse):
    p = program()
    if reverse:
        writer, reader = reader, writer
    p["endpoints"][0]["qos"]["partitions"] = writer
    p["endpoints"][1]["qos"]["partitions"] = reader
    edge = pair_result(p)
    assert edge["status"] == status
    if status == "unknown":
        assert "partition_pattern_not_evaluated" in edge["reasons"]
        assert "partition_mismatch" not in edge["reasons"]
    elif status == "allowed":
        assert "partition_pattern_not_evaluated" not in edge["reasons"]
        assert "partition_mismatch" not in edge["reasons"]
    else:
        assert "partition_mismatch" in edge["reasons"]
    assert edge["execution_evidence"] is False


def test_unevaluated_partition_pattern_does_not_override_known_qos_failure():
    p = program()
    for ep in p["endpoints"]:
        ep["qos"]["partitions"] = ["prefix*"]
    p["endpoints"][0]["qos"]["reliability"] = "best_effort"
    edge = pair_result(p)
    assert edge["status"] == "forbidden"
    assert {"reliability_requested_offered", "partition_pattern_not_evaluated"} <= set(edge["reasons"])
