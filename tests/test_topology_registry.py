"""Explicit offline topology registration, never execution qualification."""
from copy import deepcopy

import pytest

from r2f.core import Registry, SemanticTarget, canonical_hash
from r2f.modules.s1.module import S1Module, register_exploratory, resolve_services
from r2f.modules.s1.targets import (
    EXPLORATORY_TARGET_ORDER, F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET,
    exploratory_target_declarations, target_declarations,
)
from r2f.modules.s1.topology_targets import (
    TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, topology_target_declaration,
)
from r2f.modules.s1.topology_input import (
    topology_contract, topology_effect_spec, topology_rule_spec, TOPOLOGY_FACT_IDS,
)
from r2f.modules.s1.topology_services import TopologyMutationProvider


def test_default_old_five_and_each_capability_is_exact_independent_opt_in():
    default = exploratory_target_declarations()
    assert tuple(default) == EXPLORATORY_TARGET_ORDER
    assert len(default) == 5
    for cap, added, denied in [(TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET, F2_WINDOWS_TARGET),
                              (F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET, TOPOLOGY_TARGET)]:
        registry = Registry()
        register_exploratory(registry, runtime_capability=cap)
        assert set(registry.targets) == set(default) | {added}
        for key, target in default.items():
            assert registry.get_target(key).to_dict() == target.to_dict()
        with pytest.raises(KeyError):
            registry.get_target(denied)
        with pytest.raises(KeyError):
            registry.get_services(denied)
    registry = Registry()
    register_exploratory(registry)
    for denied in [TOPOLOGY_TARGET, F2_WINDOWS_TARGET]:
        with pytest.raises(KeyError):
            registry.get_services(denied)


@pytest.mark.parametrize('cap', [TOPOLOGY_TARGET, 'topology', 'f2_windows', '', True, ['topology_v1']])
def test_no_name_or_prefix_based_capability_authorization(cap):
    with pytest.raises(ValueError, match='unsupported exploratory runtime_capability'):
        register_exploratory(Registry(), runtime_capability=cap)


def test_required_four_manifests_identical_before_after_both_opt_ins():
    before = S1Module().to_dict()
    # Frozen required manifests at HEAD 42fcec65, not topology self-comparison.
    assert {k: canonical_hash(v) for k, v in before['targets'].items()} == {
        'discovery.late_endpoint': 'c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940',
        'discovery.late_join_delivery': '1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d',
        'discovery.interruption_recovery': 'fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce',
        'builtin.discovery_projection_reconciliation': 'eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0',
    }
    for cap in [None, TOPOLOGY_CAPABILITY, F2_WINDOWS_CAPABILITY]:
        registry = Registry()
        register_exploratory(registry, include_required=True, runtime_capability=cap)
        assert registry.get_module('s1').to_dict() == before
        assert {key: registry.get_target(key).to_dict() for key in before['targets']} == before['targets']
    assert {k: t.to_dict() for k, t in target_declarations().items()} == before['targets']


def test_topology_complete_manifest_reuses_canonical_core_specs():
    target = topology_target_declaration()
    data = target.to_dict()
    for key in ['intent_and_scope', 'state_preconditions', 'state_steps', 'mutation_slots',
                'observation_contract', 'expected_effect_set', 'rule_contract', 'evidence_policy']:
        assert data[key]
    assert SemanticTarget.from_dict(data).to_dict() == data
    assert target.exploration_contract.to_dict() == topology_contract().to_dict()
    assert len(topology_contract().facts) == 6
    assert target.rule_contract['primary_rule'] == topology_rule_spec().id
    assert topology_rule_spec().primary_or_supporting == 'primary'
    assert target.rule_contract['rules'] == [topology_rule_spec().to_dict()]
    assert target.rule_contract['fact_ids'] == list(TOPOLOGY_FACT_IDS)
    for key, value in topology_effect_spec().to_dict().items():
        assert target.expected_effect_set[key] == value
    assert [s.step_id for s in target.state_steps] == [
        'admit_topology', 'bind_topology_input']
    assert all(s.requires for s in target.state_steps)
    ready = {'environment_ok', 'locks_held', 'source_identity_ok', 'topology_capability_admitted'}
    assert {c.fact for c in target.state_steps[0].requires} == ready
    assert {c.fact for c in target.state_steps[1].requires} == ready | {'topology_admitted'}
    assert {c.fact for c in target.state_preconditions} == ready | {'topology_admitted', 'topology_input_bound'}
    assert target.state_steps[0].parameters['after_updates'] == {'topology_admitted': True}
    assert target.state_steps[1].parameters['after_updates'] == {'topology_input_bound': True}
    assert target.observation_contract['input_mutation_phase'] == 'compile_time'
    obligations = target.observation_contract['post_mutation_obligations']
    assert [o['obligation_id'] for o in obligations] == [
        'apply_endpoint_program', 'observe_topology', 'judge_topology']
    assert all(o['evidence'] for o in obligations)
    assert not {'topology_program_applied', 'topology_observation_complete'} & {
        c.fact for s in target.state_steps for c in s.requires}
    assert target.evidence_policy['input_receipt_is_execution_receipt'] is False
    assert target.control_contract['required_maturity_denominator_changed'] is False


def test_service_partition_and_no_implicit_resolution():
    registry = Registry()
    register_exploratory(registry, runtime_capability=TOPOLOGY_CAPABILITY)
    topology = registry.get_services(TOPOLOGY_TARGET)
    assert type(topology.mutation_provider) is TopologyMutationProvider
    assert set(topology.mutation_provider._targets) == {TOPOLOGY_TARGET}
    for key in EXPLORATORY_TARGET_ORDER:
        legacy = registry.get_services(key)
        assert TOPOLOGY_TARGET not in legacy.mutation_provider._targets
        assert TOPOLOGY_TARGET not in legacy.carrier._targets
    with pytest.raises(KeyError):
        resolve_services(TOPOLOGY_TARGET, exploratory=True)
    assert isinstance(resolve_services(TOPOLOGY_TARGET, exploratory=True,
                                      runtime_capability=TOPOLOGY_CAPABILITY).mutation_provider,
                      TopologyMutationProvider)
    changed = deepcopy(topology_target_declaration().to_dict())
    changed['observation_contract']['runtime_capability'] = None
    with pytest.raises(ValueError, match='exact_opted_in_target'):
        TopologyMutationProvider({TOPOLOGY_TARGET: SemanticTarget.from_dict(changed)})
