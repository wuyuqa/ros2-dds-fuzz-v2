"""Offline input-generation admission; no ROS, runtime, namespace or campaign.

Equal inputs say nothing about historical carrier raw; these tests exercise
only new-plan admission and future execution-claim validation.
"""
from copy import deepcopy
from unittest.mock import Mock

import pytest

from r2f.core import MutationReceipt, Registry, compile_scenario
from r2f.core.evidence import canonical_hash
from r2f.core.execution import validate_execution
from r2f.core.mutation_plan import (
    ExplorationContract, MutationPlan, MutationProposal, RUNTIME_WINDOW_SCHEMA, compose,
    is_unwindowed_compile_time_noop, mutation_plan_noop_reason, validate_plan,
)
from r2f.core.mutators import make_rng, mutate
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.providers import (
    ExploratoryInputError, S1ExploratoryMutationProvider, validate_exploratory_plan,
)
from test_core_contracts import target


@pytest.fixture
def case():
    registry = Registry()
    register_exploratory(registry, include_required=False)
    manifest = registry.get_target("discovery.late_endpoint.fuzz_v1")
    contract = manifest.exploration_contract
    parent = deepcopy(fuzz_catalog.load_seeds()["seeds"]["P01"]["logical_config"])
    parent["reader_delay_sec"] = 0.01
    return manifest, contract, parent


def generated(case, mutator="M01", seed=1):
    _, contract, parent = case
    proposal = mutate(parent, contract.surface_by_id["P01"], make_rng(seed),
                      context={"boundary_class": "finite"}, mutator_id=mutator)
    return compose(parent, [proposal], contract, rng_seed=seed)


def proposal(parent, *, value=None, sid="P01", path="reader_delay_sec",
             phase="compile_time", status="applicable", override=False, pid=""):
    changes = {} if value is None else {path: value}
    after = dict(parent, **changes)
    return MutationProposal(sid, "M03" if sid == "W02" else "M01", {}, changes, parent, after, phase,
                            {"status": status}, override=override, proposal_id=pid)


def execution(plan, *, arm="mutated", stage="mutation_applied", claims=True):
    """Synthetic core fixture, deliberately not live execution evidence."""
    row = compile_scenario(target(), "rmw_cyclonedds_cpp", arm, 0).to_dict()
    row.update(run_id="offline-noop-contract", execution_stage=stage,
               adapter_accepted=True, mutation_plan=plan.to_dict(),
               precondition_receipts=[{
                   "step_id": "create_writer", "action": "create_writer",
                   "status": "executed", "before": {}, "after": {"writer_ready": True},
                   "requires": {}, "started_at": "offline-start", "finished_at": "offline-end",
               }], mutation_receipt=MutationReceipt(*([claims] * 7)).to_dict())
    if stage in {"effect_observed", "judged"}:
        row.update(raw_path="offline-fixture-not-live.json", raw_sha256="a" * 64,
                   observed_effect={"fixture_only": True}, oracle_verdict="pass")
    return row


@pytest.mark.parametrize("mutator,seed", [("M01", 1), ("M03", 12)])
def test_known_equal_generation_is_auditable_but_not_live_routable(case, mutator, seed):
    manifest, contract, parent = case
    plan = generated(case, mutator, seed)
    assert plan.final_config == parent
    assert plan.operations[0].applicability_status == "no_op"
    assert plan.validate(contract)
    assert MutationPlan.from_dict(plan.to_dict()).to_dict() == plan.to_dict()
    assert validate_plan(plan, contract=contract).status == "invalid"
    runtime = Mock(side_effect=AssertionError("must never call runtime"))
    provider = S1ExploratoryMutationProvider({manifest.target_id: manifest})
    scenario = compile_scenario(manifest, "rmw_cyclonedds_cpp", "mutated", 0, seed=seed)
    with pytest.raises(ExploratoryInputError, match="no effective input change"):
        prepared = provider.prepare_plan(scenario, plan)
        runtime(prepared)
    runtime.assert_not_called()


@pytest.mark.parametrize("status", ["applicable", "needs_observation", "no_op"])
def test_equal_hash_cannot_be_smuggled_through_provider_by_status(case, status):
    manifest, contract, _ = case
    data = generated(case).to_dict()
    data["operations"][0]["applicability_result"] = {"status": status}
    plan = MutationPlan.from_dict(data)
    assert validate_plan(plan, contract=contract).status == "invalid"
    with pytest.raises(ExploratoryInputError):
        validate_exploratory_plan(plan, contract, target_id=manifest.target_id)


def test_explicit_no_op_cannot_claim_effect_even_with_different_hashes(case):
    _, contract, parent = case
    plan = compose(parent, [proposal(parent, value=0.7, status="no_op")], contract)
    assert plan.operations[0].input_before_sha256 != plan.operations[0].input_after_sha256
    assert validate_plan(plan, contract=contract).status == "invalid"
    with pytest.raises(ValueError, match="effective input generation"):
        validate_execution(execution(plan), verify_raw_evidence=False)


def test_m03_changed_generation_is_still_admitted(case):
    _, contract, _ = case
    for seed in range(64):
        plan = generated(case, "M03", seed)
        if plan.operations[0].input_before_sha256 != plan.operations[0].input_after_sha256:
            break
    else:
        pytest.fail("no deterministic changed M03 sample")
    assert validate_plan(plan, contract=contract).status == "valid"
    assert validate_execution(execution(plan), verify_raw_evidence=False)


def test_noop_sibling_invalidates_live_chain_without_deleting_audit_layer(case):
    _, contract, parent = case
    plan = compose(parent, [proposal(parent),
                           proposal(parent, sid="W02", path="publish_interval_sec", value=0.2)],
                   contract, chain_length=2)
    assert [op.applicability_status for op in plan.operations] == ["no_op", "applicable"]
    assert validate_plan(plan, contract=contract).status == "invalid"
    manifest = case[0]
    with pytest.raises(ExploratoryInputError, match="active no_op"):
        validate_exploratory_plan(plan, contract, target_id=manifest.target_id)
    with pytest.raises(ValueError, match="active no_op"):
        validate_execution(execution(plan), verify_raw_evidence=False)
    assert len(plan.operations) == 2  # audit lineage is not silently shortened
    forged = plan.to_dict()
    forged["operations"][0]["applicability_result"] = {"status": "applicable"}
    assert validate_plan(forged, contract=contract).status == "invalid"
    with pytest.raises(ValueError, match="active no_op"):
        validate_execution(execution(MutationPlan.from_dict(forged)), verify_raw_evidence=False)


def test_changed_two_layer_chain_remains_admissible(case):
    _, contract, parent = case
    plan = compose(parent, [proposal(parent, value=0.4),
                           proposal(parent, sid="W02", path="publish_interval_sec", value=0.2)],
                   contract, chain_length=2)
    assert all(op.applicability_status == "applicable" for op in plan.operations)
    assert validate_plan(plan, contract=contract).status == "valid"


def override_contract(contract):
    data = contract.to_dict()
    for surface in data["surfaces"]:
        surface["allowed_with"] = []
    return ExplorationContract.from_dict(data)


def test_override_is_provenance_and_net_return_to_parent_is_refused(case):
    _, contract, parent = case
    contract = override_contract(contract)
    first = proposal(parent, value=0.4, pid="first")
    for final, expected in [(0.8, "valid"), (parent["reader_delay_sec"], "invalid")]:
        plan = compose(parent, [first, proposal(parent, value=final, override=True, pid="last")],
                       contract, chain_length=2)
        assert plan.operations[0].applicability_status == "overridden"
        assert plan.operations[0].applicability_result["overridden_by"] == "last"
        assert plan.operations[1].applicability_status == "applicable"
        assert validate_plan(plan, contract=contract).status == expected
        if expected == "invalid":
            assert "no net input change" in mutation_plan_noop_reason(plan)
            with pytest.raises(ValueError, match="no net input change"):
                validate_execution(execution(plan), verify_raw_evidence=False)


def test_forged_nonoverride_compiletime_reversion_is_still_rejected(case):
    _, contract, parent = case
    contract = override_contract(contract)
    plan = compose(parent, [proposal(parent, value=0.4, pid="first"),
                           proposal(parent, value=parent["reader_delay_sec"], override=True, pid="last")],
                   contract)
    plan.operations[0].applicability_result = {"status": "applicable"}
    assert validate_plan(plan, contract=contract).status == "invalid"


def window():
    return {"schema": RUNTIME_WINDOW_SCHEMA, "window_id": "runtime-drop", "window_index": 0,
            "trigger_anchor": "fresh_match", "trigger_offset_sec": 0.1,
            "drop_duration_sec": 0.2, "restore_interval_sec": 0.3}


@pytest.mark.parametrize("phase", ["post_match", "pre_create", "runtime_window", "post_restore"])
def test_equal_config_runtime_action_is_not_compiletime_noop(case, phase):
    _, contract, parent = case
    plan = compose(parent, [proposal(parent, phase=phase)], contract)
    assert not is_unwindowed_compile_time_noop(plan.operations[0])
    assert validate_plan(plan, contract=contract).status == "valid"
    # Exemption is not a hit: unchanged receipt gates must still reject misses.
    with pytest.raises(ValueError, match="admitted, configured, applied and hit"):
        validate_execution(execution(plan, claims=False), verify_raw_evidence=False)


def test_environment_windows_are_not_mistaken_for_no_input_change(case):
    _, contract, parent = case
    plan = compose(parent, [proposal(parent)], contract, runtime_windows=[window()])
    assert plan.final_config == parent
    assert plan.operations[0].applicability_status == "applicable"
    assert mutation_plan_noop_reason(plan) is None
    assert validate_plan(plan, contract=contract).status == "valid"
    with pytest.raises(ValueError, match="admitted, configured, applied and hit"):
        validate_execution(execution(plan, claims=False), verify_raw_evidence=False)


def test_compiletime_change_then_runtime_restore_is_not_net_noop(case):
    _, contract, parent = case
    contract = override_contract(contract)
    plan = compose(parent, [proposal(parent, value=0.4, pid="first"),
                           proposal(parent, value=parent["reader_delay_sec"], phase="post_match",
                                    override=True, pid="last")], contract)
    assert plan.final_config == parent
    assert mutation_plan_noop_reason(plan) is None
    assert validate_plan(plan, contract=contract).status == "valid"


@pytest.mark.parametrize("stage", ["reached", "mutation_applied", "effect_observed", "judged", "aborted"])
def test_fake_hit_claim_cannot_bypass_execution_gate(case, stage):
    row = execution(generated(case), stage=stage)
    row["abort_reason"] = "offline-test"
    with pytest.raises(ValueError, match="effective input generation"):
        validate_execution(row, verify_raw_evidence=False)


def test_noop_plan_description_can_reach_without_claiming_applied(case):
    plan = generated(case)
    row = execution(plan, stage="reached", claims=False)
    assert validate_execution(row, verify_raw_evidence=False)
    for stage in ("mutation_applied", "effect_observed", "judged"):
        row["execution_stage"] = stage
        with pytest.raises(ValueError, match="effective input generation"):
            validate_execution(row, verify_raw_evidence=False)


@pytest.mark.parametrize("arm", ["baseline", "transparent"])
def test_neutral_noop_path_is_unchanged(case, arm):
    row = execution(generated(case), arm=arm, stage="judged", claims=False)
    row["neutral_receipt"] = {"kind": "no_op", "reason": "offline neutral fixture",
                              "before_input_sha256": "b" * 64, "after_input_sha256": "b" * 64}
    assert validate_execution(row, verify_raw_evidence=False)
    row["mutation_receipt"] = MutationReceipt(*([True] * 7)).to_dict()
    with pytest.raises(ValueError, match="baseline/transparent/negative"):
        validate_execution(row, verify_raw_evidence=False)


def test_negative_control_path_is_unchanged(case):
    row = execution(generated(case), arm="negative", stage="judged", claims=False)
    row["oracle_verdict"] = "fail"
    row["control_kind"] = "fault"
    row["fault_control_receipt"] = {
        "fault_id": "late_endpoint_delivery_omission", "applied": True,
        "omitted_sequence": 1, "raw_event_ref": 0,
        "responsibility_layer": "harness", "candidate_eligible": False,
    }
    assert validate_execution(row, verify_raw_evidence=False)


def test_input_hash_algorithm_is_not_replaced_with_final_config_hash(case):
    _, contract, parent = case
    plan = generated(case)
    assert plan.content_hash() != canonical_hash(parent)
    assert plan.content_hash() == MutationPlan.from_dict(plan.to_dict()).content_hash()
    changed_seed = plan.to_dict()
    changed_seed["rng_seed"] += 1
    assert MutationPlan.from_dict(changed_seed).content_hash() == plan.content_hash()


@pytest.mark.parametrize("fake", [["junk"], "junk", {"window": True}, [window()]])
def test_final_config_window_payload_does_not_exempt_compiletime_noop(case, fake):
    _, contract, parent = case
    parent = dict(parent, environment_windows=fake)
    plan = compose(parent, [proposal(parent)], contract)
    assert plan.operations[0].applicability_status == "no_op"
    assert validate_plan(plan, contract=contract).status == "invalid"
    # A forged applicable label is no escape either.
    plan.operations[0].applicability_result = {"status": "applicable"}
    assert validate_plan(plan, contract=contract).status == "invalid"
    with pytest.raises(ValueError, match="effective input generation"):
        validate_execution(execution(plan), verify_raw_evidence=False)


def test_mutable_plan_cannot_smuggle_malformed_runtime_windows(case):
    _, contract, _ = case
    plan = generated(case)
    plan.operations[0].applicability_result = {"status": "applicable"}
    plan.runtime_windows = ["junk"]
    with pytest.raises(ValueError, match="runtime_window"):
        is_unwindowed_compile_time_noop(plan.operations[0], runtime_windows=["junk"])
    assert validate_plan(plan, contract=contract).status == "invalid"
    row = execution(plan)
    with pytest.raises(ValueError, match="runtime_window"):
        validate_execution(row, verify_raw_evidence=False)


def test_validated_window_only_plan_preserves_existing_window_gates(case):
    _, contract, parent = case
    plan = compose(parent, [], contract, runtime_windows=[window()])
    assert validate_plan(plan, contract=contract).status == "valid"
    assert mutation_plan_noop_reason(plan) is None
