"""Schema-bound filter INPUT contracts; no ROS, DDS, netns or execution.

The unchanged checked-in profile/corpus and real I2 planning/provider path are
used below.  Runtime filter validation is an independent offline harness check,
not a claim of RMW support, observed matching, sample filtering or mutation hit.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.runner import _topology_generation
from r2f.core import Registry, Scenario
from r2f.core.evidence import canonical_hash, effective_input_hash
from r2f.core.mutation_plan import MutationPlan
from r2f.core.mutators import TOPOLOGY_MUTATOR_ID
from r2f.modules.s1 import fuzz_mutators_v2 as v2, input_model, topology_model
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_input import make_topology_plan, validate_topology_plan
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET
from r2f.runtime.filter_backend import validate_filter

REPO = Path(__file__).resolve().parents[1]
PROFILE = REPO / "config/s1_fuzz/profiles/topology-chains.json"
STRING_TYPE = "std_msgs/msg/String"
# These are input templates, not observations or a sample-delivery oracle.
STRING_SPECS = (
    ("data <> ''", ()),
    ("data = 'r2f'", ()),
    ("data = %0", ("'r2f'",)),
)


@pytest.fixture(scope="module")
def profile_corpus_registry():
    profile = json.loads(PROFILE.read_text())
    corpus = corpus_from_profile(profile, cycle_id="topology-authored")
    registry = Registry()
    register_exploratory(registry, include_required=False,
                         runtime_capability=TOPOLOGY_CAPABILITY)
    return profile, corpus, registry


def corpus_parent(data, rmw="rmw_fastrtps_cpp"):
    _, corpus, _ = data
    return next(row for row in corpus.selectable()
                if row["seed_id"] == "topology.multi_endpoint" and row["rmw"] == rmw)


def provider_prepare(data, plan, rmw="rmw_fastrtps_cpp"):
    _, _, registry = data
    scenario = Scenario(
        registry.get_target(TOPOLOGY_TARGET), rmw, "mutated", 0, plan.rng_seed,
        {"mutation_kind": "endpoint_program", "carrier": "ros2_endpoint",
         "mutation_parameters": plan.final_config}, plan)
    prepared = registry.get_services(TOPOLOGY_TARGET).mutation_provider.prepare(scenario)
    return scenario, prepared


def filter_request(variant, **context):
    return {"operator_id": "filter.expr", "slot_id": "S1.topology.filter",
            "context": {"filter_variant": variant, **context}}


def case_endpoint_index(case):
    prefix = "S1.topology.filter#"
    slot = case["slot_id"]
    assert slot.startswith(prefix), slot
    return int(slot[len(prefix):])


def reader_filter(program, index=None):
    if index is None:
        index = next(i for i, endpoint in enumerate(program["endpoints"])
                     if endpoint["role"] == "reader" and "filter" in endpoint)
    endpoint = program["endpoints"][index]
    assert endpoint["role"] == "reader"
    return endpoint["filter"]


def string_spec(value):
    return value["expression"], tuple(value["parameters"])


def assert_string_input(value):
    assert string_spec(value) in STRING_SPECS
    assert value["evaluation"] is None  # never a filtering/matching fact
    assert all(type(parameter) is str and parameter == "'r2f'"
               for parameter in value["parameters"])
    validation = validate_filter(value["expression"], value["parameters"],
                                 field_types={"data": "string"})
    assert validation["ok"], validation
    assert validation["enabled"] is True
    assert set(validation["fields"]) == {"data"}
    assert validation["conformance_claim"] is False
    assert validation["candidate_eligible"] is False
    assert validation["filter_false_implies_unmatch"] is False


@pytest.mark.parametrize("seed", [49, 50])
@pytest.mark.parametrize("rmw", ["rmw_fastrtps_cpp", "rmw_cyclonedds_cpp"])
def test_unchanged_profile_corpus_real_i2_provider_positive_filter(
        profile_corpus_registry, seed, rmw):
    data = profile_corpus_registry
    profile, _, _ = data
    frozen_profile = deepcopy(profile)
    entry = corpus_parent(data, rmw)
    original = deepcopy(entry["final_config"])
    # No patched generator or narrowed whitelist: the actual profile selects
    # filter.expr in a two-layer chain at these reproducible seeds.
    plan, generation = _topology_generation(entry["final_config"], profile, seed)
    assert generation["operator_whitelist"] == profile["operator_whitelist"]
    assert generation["execution_evidence"] is False
    assert len(plan.operations) == profile["chain_length"] == 2
    assert all(operation.mutator_id == TOPOLOGY_MUTATOR_ID for operation in plan.operations)
    filters = [operation for operation in plan.operations
               if operation.parameters["operator_id"] == "filter.expr"]
    assert len(filters) == 1
    receipt = filters[0].parameters["input_layer_receipts"][0]
    case = receipt["v2_case"]
    assert case["expected_guard_effect"]["filter_variant"] == "valid"
    assert case["expected_disposition"] == "expect_pass"
    _, prepared = provider_prepare(data, plan, rmw)
    assert prepared["final_input"] == plan.final_config
    assert prepared["execution_admitted"] is False
    program = prepared["final_input"]["topology_program"]
    changed_index = next(i for i, endpoint in enumerate(program["endpoints"])
                         if "filter" in endpoint)
    assert program["endpoints"][changed_index]["type_name"] == STRING_TYPE
    assert_string_input(reader_filter(program, changed_index))
    assert case["slot_id"] == "S1.topology.filter#" + str(changed_index)
    assert set(case["changes"]) == {f"endpoints[{changed_index}].filter"}
    assert case["execution_evidence"] is False and case["candidate_eligible"] is False
    assert receipt["execution_evidence"] is False and receipt["candidate_eligible"] is False
    assert case["binding"].get("status") not in {"qualified", "mature", "executed"}
    assert profile == frozen_profile and entry["final_config"] == original
    assert validate_topology_plan(MutationPlan.from_dict(plan.to_dict()))


@pytest.mark.parametrize("variant", ["empty", "valid", "invalid"])
@pytest.mark.parametrize("two_layers", [False, True])
def test_real_i2_plan_provider_preserves_all_filter_classes(
        profile_corpus_registry, variant, two_layers):
    data = profile_corpus_registry
    entry = corpus_parent(data)
    requests = ([{"operator_id": "action.insert"}] if two_layers else [])
    requests.append(filter_request(variant))
    plan = make_topology_plan(entry["final_config"]["topology_program"], requests, rng_seed=7)
    assert len(plan.operations) == len(requests)
    operation = plan.operations[-1]
    receipt = operation.parameters["input_layer_receipts"][0]
    case = receipt["v2_case"]
    selected_index = case_endpoint_index(case)
    value = reader_filter(plan.final_config["topology_program"], selected_index)
    assert case["expected_guard_effect"]["filter_variant"] == variant
    assert value["evaluation"] is None
    assert case["execution_evidence"] is False and case["candidate_eligible"] is False
    if variant == "invalid":
        assert len(value["expression"]) == 1025
        assert case["expected_disposition"] == "expect_guard_reject"
        assert operation.applicability_status == "rejected"
        with pytest.raises(ValueError, match="topology_plan_not_applicable"):
            provider_prepare(data, plan)
    else:
        assert operation.applicability_status == "applicable"
        _, prepared = provider_prepare(data, plan)
        assert prepared["execution_admitted"] is False
        if variant == "empty":
            assert value == {"expression": "", "parameters": [], "evaluation": None}
            checked = validate_filter(value["expression"], value["parameters"],
                                      field_types={"data": "string"})
            assert checked["ok"] is True and checked["enabled"] is False
        else:
            assert_string_input(value)
    assert MutationPlan.from_dict(plan.to_dict()).to_dict() == plan.to_dict()


@pytest.mark.parametrize("operators", [
    ("action.insert", "filter.expr"), ("filter.expr", "action.insert"),
    ("filter.expr", "filter.expr"),
])
def test_two_layer_composition_changes_actual_filter_content_and_retains_hash_chain(
        profile_corpus_registry, operators):
    data = profile_corpus_registry
    program = corpus_parent(data)["final_config"]["topology_program"]
    requests = [filter_request("valid") if op == "filter.expr" else {"operator_id": op}
                for op in operators]
    plan = make_topology_plan(program, requests, rng_seed=7)
    previous = deepcopy(program)
    for operation in plan.operations:
        receipt = operation.parameters["input_layer_receipts"][0]
        assert receipt["input_before"] == previous
        assert receipt["input_before_sha256"] == canonical_hash(previous)
        assert receipt["input_after_sha256"] == canonical_hash(receipt["input_after"])
        assert operation.input_before_sha256 != operation.input_after_sha256
        if operation.parameters["operator_id"] == "filter.expr":
            value = reader_filter(receipt["input_after"])
            assert_string_input(value)
            selected_index = case_endpoint_index(receipt["v2_case"])
            old = previous["endpoints"][selected_index].get("filter", {})
            assert (value["expression"], value["parameters"]) != (
                old.get("expression"), old.get("parameters"))
        previous = receipt["input_after"]
    assert plan.final_config == {"topology_program": previous}
    assert provider_prepare(data, plan)[1]["execution_admitted"] is False


def test_invalid_filter_is_terminal_and_never_hidden_by_a_later_positive():
    parent = topology_model.generate_program(0, writers=1, readers=1)
    with pytest.raises(ValueError, match="guard negative"):
        make_topology_plan(parent, [filter_request("invalid"), {"operator_id": "action.insert"}])


@pytest.mark.parametrize("unknown_type", [
    "std_msgs/msg/Int32", "geometry_msgs/msg/Pose", "example_interfaces/msg/String",
    "custom_msgs/msg/Unknown", "std_msgs/String",
])
def test_unknown_reader_type_named_refusal_even_with_schema_hints(unknown_type):
    parent = topology_model.generate_program(0, writers=1, readers=1)
    parent["endpoints"][1]["type_name"] = unknown_type
    original = deepcopy(parent)
    context = {"filter_variant": "valid", "field_types": {"seq": "number"},
               "type_name": STRING_TYPE}
    result = v2.try_mutate("S1.topology", "filter.expr", parent, seed=7, context=context)
    assert result["status"] == "refused"
    assert result["reason"] == "filter_schema_unknown"
    assert result["details"]["type_name"] == unknown_type
    assert result["details"]["logical_id"] == parent["endpoints"][1]["logical_id"]
    assert result["execution_evidence"] is False and result["candidate_eligible"] is False
    with pytest.raises(v2.MutationRefused, match="filter_schema_unknown"):
        make_topology_plan(parent, [filter_request("valid", endpoint_index=1)], rng_seed=7)
    assert parent == original


@pytest.mark.parametrize("variant", ["empty", "invalid"])
def test_unknown_type_does_not_erase_empty_or_invalid_input_classes(variant):
    parent = topology_model.generate_program(0, writers=1, readers=1)
    parent["endpoints"][1]["type_name"] = "custom_msgs/msg/Unknown"
    case = v2.mutate("S1.topology", "filter.expr", parent,
                     context={"filter_variant": variant})
    assert case.expected_disposition == ("expect_guard_reject" if variant == "invalid"
                                         else "expect_pass")
    assert case.to_dict()["execution_evidence"] is False
    assert case.to_dict()["candidate_eligible"] is False


def test_only_the_selected_reader_type_binds_the_filter_not_writer_or_other_reader():
    parent = topology_model.generate_program(0, writers=1, readers=2)
    parent["endpoints"][0]["type_name"] = "std_msgs/msg/Int32"
    parent["endpoints"][1]["type_name"] = "custom_msgs/msg/Unknown"
    with pytest.raises(v2.MutationRefused, match="filter_schema_unknown"):
        make_topology_plan(parent, [filter_request("valid", endpoint_index=1)], rng_seed=7)
    case = v2.mutate("S1.topology", "filter.expr", parent, seed=7,
                     context={"filter_variant": "valid", "endpoint_index": 2})
    assert_string_input(reader_filter(case.input, 2))
    assert case.slot_id == "S1.topology.filter#2"
    assert case.input["endpoints"][:2] == parent["endpoints"][:2]
    assert case.input["actions"] == parent["actions"]
    assert set(case.changes) == {"endpoints[2].filter"}
    with pytest.raises(v2.MutationRefused, match="filter_schema_unknown"):
        v2.mutate("S1.topology", "filter.expr", parent, seed=7,
                  context={"filter_variant": "valid", "endpoint_index": 1})


@pytest.mark.parametrize("index", [True, False, 1.0, -1, 0, 99, "1"])
def test_filter_endpoint_selector_rejects_non_reader_and_non_integer_indices(index):
    with pytest.raises(v2.MutationRefused, match="endpoint_index"):
        v2.mutate("S1.topology", "filter.expr",
                  topology_model.generate_program(0, writers=1, readers=1),
                  context={"filter_variant": "valid", "endpoint_index": index})


def test_string_parameters_and_literals_are_typed_and_deterministic():
    parent = topology_model.generate_program(0, writers=1, readers=1)
    specs, hashes = set(), set()
    for seed in range(18):
        case = v2.mutate("S1.topology", "filter.expr", parent, seed=seed,
                         context={"filter_variant": "valid"})
        replay = v2.mutate("S1.topology", "filter.expr", parent, seed=seed,
                           context={"filter_variant": "valid"})
        value = reader_filter(case.input)
        assert_string_input(value)
        assert case.to_dict() == replay.to_dict()
        assert case.input_before_sha256 != case.content_sha256
        specs.add(string_spec(value))
        hashes.add(case.content_sha256)
    assert specs == set(STRING_SPECS)
    assert len(hashes) == len(specs)  # not seed-only identity inflation


@pytest.mark.parametrize("old_expression,old_parameters", STRING_SPECS)
def test_valid_does_not_count_only_an_evaluation_reset_as_content_change(
        old_expression, old_parameters):
    parent = topology_model.generate_program(0, writers=1, readers=1)
    parent["endpoints"][1]["filter"] = {
        "expression": old_expression, "parameters": list(old_parameters), "evaluation": True}
    original = deepcopy(parent)
    for seed in range(6):
        case = v2.mutate("S1.topology", "filter.expr", parent, seed=seed,
                         context={"filter_variant": "valid"})
        value = reader_filter(case.input)
        assert_string_input(value)
        assert string_spec(value) != (old_expression, old_parameters)
    assert parent == original


def test_all_valid_string_filter_templates_survive_real_i2_provider_final_program(
        profile_corpus_registry):
    data = profile_corpus_registry
    parent = corpus_parent(data)["final_config"]["topology_program"]
    observed = set()
    for seed in range(32):
        plan = make_topology_plan(parent, [filter_request("valid")], rng_seed=seed)
        _, prepared = provider_prepare(data, plan)
        case = plan.operations[0].parameters["input_layer_receipts"][0]["v2_case"]
        endpoint = prepared["final_input"]["topology_program"]["endpoints"][
            case_endpoint_index(case)]
        assert endpoint["type_name"] == STRING_TYPE and endpoint["role"] == "reader"
        value = endpoint["filter"]
        assert_string_input(value)
        assert all(type(parameter) is str for parameter in value["parameters"])
        assert prepared["execution_admitted"] is False
        observed.add(string_spec(value))
        if observed == set(STRING_SPECS):
            break
    assert observed == set(STRING_SPECS)


def test_provider_plan_and_effective_input_hash_reproduce_without_execution(
        profile_corpus_registry):
    data = profile_corpus_registry
    profile, _, _ = data
    parent = corpus_parent(data)["final_config"]
    first, receipt = _topology_generation(parent, profile, 49)
    second, repeated = _topology_generation(parent, profile, 49)
    assert first.to_dict() == second.to_dict() and receipt == repeated
    assert first.plan_hash() == second.plan_hash()
    assert first.content_hash() == second.content_hash()
    # Bind this offline check to generator bytes, not an invented frozen HEAD.
    source = {"source_frozen": False, "scope": "offline_generator_only",
              "generator_sha256": hashlib.sha256(Path(v2.__file__).read_bytes()).hexdigest()}
    identities = []
    for plan in (first, second):
        scenario, prepared = provider_prepare(data, plan)
        identity = scenario.effective_input_v2(
            final_input=prepared["final_input"],
            input_layer_receipts=prepared["input_layer_receipts"], source_manifest=source)
        identities.append(effective_input_hash(identity))
        assert identity["source_manifest"]["source_frozen"] is False
        identity.update(campaign_kind="exploratory", domain=199)
        assert effective_input_hash(identity) == identities[-1]
    assert identities[0] == identities[1]


def test_schema_repair_does_not_promote_binding_or_expand_operator_set():
    slot = input_model.topology_surface().slot_by_id["S1.topology.filter"]
    assert slot.binding["status"] == "generation_only"
    assert len(v2.OPERATOR_SPECS) == 24
    assert v2.operator_manifest()["execution_evidence"] is False
    assert v2.operator_manifest()["candidate_eligible"] is False
