"""Offline content contracts; these tests never start DDS/ROS or a campaign."""
import copy
import json
from types import SimpleNamespace

import pytest

from r2f.core.evidence import canonical_hash, effective_input_hash, INPUT_SCHEMA_V2
from r2f.core.mutation_plan import MutationPlan, compose, validate_plan
from r2f.core.mutators import make_rng
from r2f.modules.s1 import fuzz_mutators_v2 as v2, topology_model
from r2f.modules.s1.topology_input import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, TOPOLOGY_MUTATION_KIND, TOPOLOGY_CARRIER,
    TOPOLOGY_SURFACE, topology_contract, make_topology_plan,
    topology_input_layer_receipts, validate_topology_plan, mutate_topology,
)


def parent():
    return topology_model.generate_program(0, writers=1, readers=1)


def requests(length):
    return [{"operator_id": "action.insert", "slot_id": "S1.topology.actions"}] * length


def build(length=1):
    return make_topology_plan(parent(), requests(length), rng_seed=7)


@pytest.mark.parametrize("length", [1, 2, 3, 4])
def test_core_roundtrip_and_every_layer_has_actual_content_and_both_hash_chains(length):
    original = parent()
    frozen = copy.deepcopy(original)
    plan = make_topology_plan(original, requests(length), rng_seed=7)
    assert type(plan) is MutationPlan
    assert original == frozen
    assert set(plan.final_config) == {"topology_program"}
    assert plan.chain["chain_length"] == length
    assert len(plan.operations) == length
    assert plan.parent_content_sha256 == canonical_hash({"topology_program": original})
    current = original
    for index, operation in enumerate(plan.operations):
        receipt = operation.parameters["input_layer_receipts"][0]
        assert receipt["input_before"] == current
        assert receipt["input_before"] != receipt["input_after"]
        assert receipt["input_before_sha256"] == canonical_hash(current)
        assert receipt["input_after_sha256"] == canonical_hash(receipt["input_after"])
        assert operation.input_before_sha256 == canonical_hash({"topology_program": current})
        assert operation.input_after_sha256 == canonical_hash({"topology_program": receipt["input_after"]})
        assert receipt["input_after_sha256"] != operation.input_after_sha256
        assert receipt["v2_case"]["content_sha256"] != operation.input_after_sha256
        assert receipt["v2_case"]["content_sha256"] != receipt["input_after_sha256"]
        assert receipt["seed"] == 7 + index
        assert receipt["execution_evidence"] is False
        assert receipt["candidate_eligible"] is False
        assert operation.applicability_status == "applicable"  # not overridden
        current = receipt["input_after"]
    assert plan.final_config == {"topology_program": current}
    restored = MutationPlan.from_dict(json.loads(json.dumps(plan.to_dict())))
    assert restored.to_dict() == plan.to_dict()
    assert restored.plan_hash() == plan.plan_hash()
    assert restored.validate(contract=topology_contract())
    assert build(length).to_dict() == plan.to_dict()


def test_main_constants_and_no_runtime_or_oracle_qualification():
    assert TOPOLOGY_TARGET == "discovery.endpoint_topology.fuzz_v1"
    assert TOPOLOGY_CAPABILITY == "topology_v1"
    assert TOPOLOGY_MUTATION_KIND == "endpoint_program"
    assert TOPOLOGY_CARRIER == "ros2_endpoint"
    contract = topology_contract()
    assert contract.surfaces[0].value_type == "action_program"
    assert contract.surfaces[0].runtime_binding == "topology.program"
    assert contract.surfaces[0].executable is True  # declaration only, not admission
    assert len(contract.rules) == 1 and len(contract.facts) == 6
    assert all(f.missing_policy == "unknown" for f in contract.facts)
    assert validate_plan(build(), capabilities={"runtime_bindings": []}, contract=contract).status == "unsupported_capability"


def test_real_multi_operator_program_content_delegates_v2(monkeypatch):
    seen = []
    delegate = v2.mutate
    def spy(*args, **kwargs):
        seen.append((copy.deepcopy(args), copy.deepcopy(kwargs)))
        return delegate(*args, **kwargs)
    monkeypatch.setattr(v2, "mutate", spy)
    reqs = [
        {"operator_id": "endpoint.count", "slot_id": "S1.topology.endpoints"},
        {"operator_id": "action.insert", "slot_id": "S1.topology.actions"},
        {"operator_id": "param.enum", "slot_id": "S1.topology.qos.reliability"},
        {"operator_id": "action.delete", "slot_id": "S1.topology.actions"},
    ]
    plan = make_topology_plan(parent(), reqs, rng_seed=9)
    assert len(seen) == 4
    assert [call[1]["seed"] for call in seen] == [9, 10, 11, 12]
    for i in range(1, 4):
        assert seen[i][0][2] == plan.operations[i-1].parameters["input_layer_receipts"][0]["input_after"]
    assert len(plan.final_config["topology_program"]["endpoints"]) == 3
    assert "#" in plan.operations[2].parameters["slot_id"]
    assert plan.validate()


def test_validate_and_roundtrip_do_not_mutate_again(monkeypatch):
    plan = build(4)
    def no_mutation(*args, **kwargs):
        pytest.fail("validation/replay must consume frozen content, never mutate")
    monkeypatch.setattr(v2, "mutate", no_mutation)
    monkeypatch.setattr(topology_model, "mutate_program", no_mutation)
    assert plan.validate()
    assert validate_topology_plan(plan)
    restored = MutationPlan.from_dict(plan.to_dict())
    assert validate_topology_plan(restored)
    assert restored.to_dict() == plan.to_dict()
    assert topology_input_layer_receipts(plan)


@pytest.mark.parametrize("bad", [[], requests(5), None, {}, "action.insert"])
def test_layer_budget_closed(bad):
    with pytest.raises(ValueError):
        make_topology_plan(parent(), bad)


@pytest.mark.parametrize("mutation_request", [
    {"operator_id": "no_such_operator"}, {"operator_id": "env.drop"},
    {"operator_id": "action.insert", "slot_id": "no_such_slot"},
    {"operator_id": "action.insert", "slot_id": None},
    {"operator_id": "action.insert", "unknown": True},
    {"operator_id": "action.insert", "context": []},
    {"operator_id": "action.insert", "context": {"_operator_id": "guard.structure"}},
    {"operator_id": "action.insert", "context": {"seed": 3}},
    {"operator_id": "action.insert", "context": {"rng_seed": True}},
    {"operator_id": "action.insert", "context": {"parameters": {"seed": 0}}},
    {"operator_id": "action.insert", "context": {"parameters": []}},
    {"operator_id": "action.insert", "context": {"rng_algorithm": "other"}},
    {"operator_id": "action.insert", "context": {"hit": True}},
    {}, None,
])
def test_unknown_operator_slot_context_or_seed_is_closed(mutation_request):
    with pytest.raises(ValueError):
        make_topology_plan(parent(), [mutation_request], rng_seed=7)


@pytest.mark.parametrize("seed", [-1, True, 1.5, "0"])
def test_base_seed_requires_nonnegative_integer(seed):
    with pytest.raises(ValueError):
        make_topology_plan(parent(), requests(1), rng_seed=seed)


def test_valid_explicit_context_seed():
    plan = make_topology_plan(parent(), [{"operator_id": "action.insert", "context": {
        "seed": 7, "parameters": {"seed": 7}}}], rng_seed=7)
    assert plan.validate()


def test_rng_state_is_bound_not_just_recorded():
    context = {"rng_seed": 7, "layer_index": 0, "mutation_request": requests(1)[0]}
    with pytest.raises(ValueError, match="seed/state"):
        mutate_topology({"topology_program": parent()}, topology_contract().surfaces[0], make_rng(8), context)
    advanced = make_rng(7); advanced.random()
    with pytest.raises(ValueError, match="seed/state"):
        mutate_topology({"topology_program": parent()}, topology_contract().surfaces[0], advanced, context)


@pytest.mark.parametrize("change", ["actions", "endpoints", "budget", "batch", "byte", "unknown"])
def test_invalid_or_overbudget_parent_is_never_admitted(change):
    p = parent()
    if change == "actions": p["budget"]["max_actions"] = 1
    if change == "endpoints": p["budget"]["max_endpoints"] = 1
    if change == "budget": p["budget"]["max_actions"] = 513
    if change == "batch": p["actions"][2]["params"]["count"] = 999999999
    if change == "byte": p["actions"][0]["params"]["payload"] = "x" * (1024 * 1024)
    if change == "unknown": p["pretend_runtime"] = True
    with pytest.raises(ValueError):
        make_topology_plan(p, requests(1))


def test_noop_delegate_is_closed(monkeypatch):
    monkeypatch.setattr(v2, "mutate", lambda surface, op, doc, **kw: SimpleNamespace(
        input=doc, expected_disposition="expect_pass", operator_id=op))
    with pytest.raises(ValueError, match="no content change"):
        build()


def test_exhausted_mutation_budget_is_closed():
    p = parent(); p["budget"]["max_actions"] = len(p["actions"])
    with pytest.raises(ValueError, match="budget"):
        make_topology_plan(p, requests(1))


@pytest.mark.parametrize("operator_id", ["guard.structure", "guard.parameter", "guard.dependency", "guard.environment"])
def test_guard_negatives_remain_named_terminal_rejected_inputs(operator_id):
    plan = make_topology_plan(parent(), [{"operator_id": operator_id}], rng_seed=7)
    op = plan.operations[0]
    assert op.applicability_status == "rejected"
    assert op.applicability_result["expected_disposition"] == "expect_guard_reject"
    assert validate_plan(plan).status == "invalid"
    assert op.parameters["input_layer_receipts"][0]["candidate_eligible"] is False
    assert MutationPlan.from_dict(plan.to_dict()).to_dict() == plan.to_dict()
    with pytest.raises(ValueError, match="guard negative"):
        make_topology_plan(parent(), [{"operator_id": operator_id}, *requests(1)])


TAMPERS = [
    ("rng_seed",), ("rng_algorithm",), ("parent_content_sha256",), ("contract_hash",),
    ("operations", 0, "input_before_sha256"), ("operations", 0, "input_after_sha256"),
    ("operations", 0, "parameters", "seed"),
    ("operations", 0, "parameters", "slot_id"),
    ("operations", 0, "parameters", "operator_id"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "input_before_sha256"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "input_after_sha256"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "seed"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "layer_index"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "case_hash_role"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "execution_evidence"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "candidate_eligible"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "v2_case", "content_sha256"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "v2_case", "input_before_sha256"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "v2_case", "seed"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "v2_case", "provenance", "seed"),
    ("operations", 0, "parameters", "input_layer_receipts", 0, "v2_case", "provenance", "domain_sha256"),
]


@pytest.mark.parametrize("path", TAMPERS)
def test_serialized_tampering_fails_closed(path):
    data = build(2).to_dict()
    value = data
    for key in path[:-1]: value = value[key]
    old = value[path[-1]]
    value[path[-1]] = (not old) if type(old) is bool else old + 1 if type(old) is int else "tampered"
    with pytest.raises(ValueError):
        validate_topology_plan(MutationPlan.from_dict(data))


def test_reordering_edited_program_and_final_extra_key_fail_closed():
    plan = build(2)
    variants = []
    data = plan.to_dict(); data["operations"].reverse(); variants.append(data)
    data = plan.to_dict(); data["operations"][0]["parameters"]["input_layer_receipts"][0]["input_after"]["actions"].reverse(); variants.append(data)
    data = plan.to_dict(); data["final_config"]["extra"] = "not allowed"; variants.append(data)
    data = plan.to_dict(); data.pop("chain"); variants.append(data)
    data = plan.to_dict(); data["operations"][0]["parameters"]["request"]["context"]["seed"] = 100; variants.append(data)
    for data in variants:
        with pytest.raises(ValueError): validate_topology_plan(MutationPlan.from_dict(data))


def test_dedupe_envelope_cannot_replace_raw_or_core_or_effective_input_hash():
    plan = build()
    receipt = topology_input_layer_receipts(plan)[0]
    data = plan.to_dict()
    data["operations"][0]["input_after_sha256"] = receipt["v2_case"]["content_sha256"]
    with pytest.raises(ValueError): MutationPlan.from_dict(data)
    effective = {
        "input_identity_schema": INPUT_SCHEMA_V2, "target_id": TOPOLOGY_TARGET,
        "target_revision": 0, "mutation_kind": TOPOLOGY_MUTATION_KIND,
        "carrier": TOPOLOGY_CARRIER, "mutation_parameters": {}, "rmw": "offline_unrun",
        "repeat": 0, "mutation_layer_order": [op.op_id for op in plan.operations],
        "input_layer_receipts": topology_input_layer_receipts(plan), "final_input": plan.final_config,
        "mutation_plan": plan.to_dict(), "contract_hash": plan.contract_hash,
    }
    identity = effective_input_hash(effective)
    assert len({identity, receipt["v2_case"]["content_sha256"],
                canonical_hash(plan.final_config), canonical_hash(receipt["input_after"])}) == 4
    effective["domain"] = 120
    assert effective_input_hash(effective) == identity


def test_receipt_access_is_detached():
    plan = build()
    receipts = topology_input_layer_receipts(plan)
    receipts[0]["input_after"]["actions"].clear()
    assert plan.validate()


def test_existing_compose_rejects_forged_action_program_proposals():
    p = {"topology_program": parent()}; c = topology_contract()
    ctx = {"rng_seed": 7, "layer_index": 0, "mutation_request": requests(1)[0]}
    proposal = mutate_topology(p, c.surfaces[0], make_rng(7), ctx)
    proposal.input_before["topology_program"]["actions"].reverse()
    with pytest.raises(ValueError, match="before/after"):
        compose(p, [proposal], c, rng_seed=7, chain_length=1)


def generic_action_proposals():
    """A non-S1 schema exercises core without any module-specific validator."""
    from r2f.core.mutation_plan import (
        MutationProposal, MutationSurface, ExplorationContract, EffectSpec,
        ACTION_PROGRAM_LAYER_SCHEMA,
    )
    from r2f.core.mutators import TOPOLOGY_MUTATOR_ID
    surface = MutationSurface(
        surface_id="demo.program", version=1, source_refs=["demo"],
        surface_kind="action_program", input_paths=["program"], value_type="action_program",
        unit="program", domain={"program_schema": "demo.program/v1", "max_program_bytes": 4096},
        special_values=[], boundary_classes=[], mutator_ids=[TOPOLOGY_MUTATOR_ID],
        value_relations=[], tool_requires={}, sut_guards={}, carrier_id="demo",
        runtime_phase="compile_time", runtime_binding="demo.program", allowed_with=[],
        conflicts_with=[], order_after=[], fact_ids=[], rule_ids=[], effect_id="demo.effect",
        missing_policy="reject", unsupported_policy="reject", activation="F2")
    contract = ExplorationContract(surfaces=[surface], effects=[EffectSpec("demo.effect", 1, "input_only", [], [])])
    parent_config = {"program": {"schema": "demo.program/v1", "actions": []}, "other_config": 42}
    current = copy.deepcopy(parent_config)
    proposals = []
    for index in range(2):
        before = copy.deepcopy(current)
        after = copy.deepcopy(current)
        after["program"]["actions"].append({"op": "demo", "index": index})
        receipt = {
            "schema": ACTION_PROGRAM_LAYER_SCHEMA, "layer_index": index, "seed": index,
            "input_path": "program", "program_schema": "demo.program/v1",
            "input_before": before["program"], "input_after": after["program"],
            "input_before_sha256": canonical_hash(before["program"]),
            "input_after_sha256": canonical_hash(after["program"]),
        }
        proposals.append(MutationProposal(
            surface_id="demo.program", mutator_id=TOPOLOGY_MUTATOR_ID,
            parameters={"input_layer_receipts": [receipt]}, changes={"program": after["program"]},
            input_before=before, input_after=after, application_phase="compile_time",
            applicability_result={"status": "applicable"}))
        current = after
    return parent_config, proposals, contract


def test_core_compose_validate_roundtrip_need_no_s1_import_or_validator(monkeypatch):
    import builtins
    original_import = builtins.__import__
    def guarded_import(name, *args, **kwargs):
        if name.startswith("r2f.modules") or name in {"topology_input", "topology_model", "fuzz_mutators_v2"}:
            raise AssertionError("core attempted a module-specific import: " + name)
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    p, proposals, contract = generic_action_proposals()
    plan = compose(p, proposals, contract, chain_length=2)
    assert plan.final_config["other_config"] == 42
    assert len(plan.final_config["program"]["actions"]) == 2
    assert all(op.applicability_status == "applicable" for op in plan.operations)
    assert MutationPlan.from_dict(json.loads(json.dumps(plan.to_dict()))).validate(contract=contract)


@pytest.mark.parametrize("field", ["input_before", "input_after", "changes", "receipt_before", "receipt_after", "path", "schema"])
def test_action_program_type_alone_cannot_bypass_existing_conflicts(field):
    p, proposals, contract = generic_action_proposals()
    later = proposals[1]
    if field in {"input_before", "input_after", "changes"}:
        getattr(later, field)["program"]["actions"].clear()
    elif field == "receipt_before":
        later.parameters["input_layer_receipts"][0]["input_before"]["actions"].clear()
    elif field == "receipt_after":
        later.parameters["input_layer_receipts"][0]["input_after"]["actions"].clear()
    elif field == "path":
        later.parameters["input_layer_receipts"][0]["input_path"] = "other_config"
    else:
        later.parameters["input_layer_receipts"][0]["program_schema"] = "other/v1"
    with pytest.raises(ValueError): compose(p, proposals, contract, chain_length=2)


@pytest.mark.parametrize("mode", ["byte", "depth", "schema", "nochange"])
def test_core_bounded_json_checks_are_module_independent(mode):
    p, proposals, contract = generic_action_proposals()
    proposal = proposals[0]
    receipt = proposal.parameters["input_layer_receipts"][0]
    after = copy.deepcopy(receipt["input_after"])
    if mode == "byte": after["actions"] = ["x" * 4096]
    if mode == "depth":
        after["actions"] = cursor = []
        for _ in range(40):
            child = []; cursor.append(child); cursor = child
    if mode == "schema": after["schema"] = "other/v1"
    if mode == "nochange": after = copy.deepcopy(receipt["input_before"])
    receipt["input_after"] = after
    receipt["input_after_sha256"] = canonical_hash(after)
    proposal.input_after["program"] = copy.deepcopy(after)
    proposal.changes["program"] = copy.deepcopy(after)
    with pytest.raises(ValueError): compose(p, proposals[:1], contract, chain_length=1)


def test_s1_provenance_audit_is_explicit_not_automatically_run_by_core():
    data = build().to_dict()
    data["operations"][0]["parameters"]["input_layer_receipts"][0]["v2_case"]["provenance"]["seed"] = 999
    generic_plan = MutationPlan.from_dict(data)  # generic content contract is intact
    assert generic_plan.validate()
    with pytest.raises(ValueError, match="provenance"):
        validate_topology_plan(generic_plan)


def test_filter_guard_negative_keeps_named_disposition_without_execution():
    plan = make_topology_plan(parent(), [{
        "operator_id": "filter.expr", "context": {"filter_variant": "invalid"}}])
    assert validate_topology_plan(plan)
    assert plan.operations[0].applicability_status == "rejected"
    assert plan.operations[0].parameters["input_layer_receipts"][0]["expected_disposition"] == "expect_guard_reject"
    assert validate_plan(plan).status == "invalid"


def test_direct_object_chain_tampering_is_rejected_not_only_deserialization():
    plan = build(2)
    plan.chain["proposal_order"].reverse()
    with pytest.raises(ValueError): plan.validate()
    plan = build()
    plan.rng_seed = True
    with pytest.raises(ValueError): plan.validate()
