"""Real core planning/capsule binding with injected offline callbacks, not DDS."""
from copy import deepcopy
from pathlib import Path
import json
from unittest.mock import Mock

import pytest

from r2f.core import Registry, canonical_hash
from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.core.replay import ReplayCapsule
from r2f.core.scenario import Scenario
from r2f.campaign.exploratory_input import runtime_input
from r2f.modules.s1 import topology_input, topology_model, fuzz_mutators_v2
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
from r2f.modules.s1.topology_services import _ServiceDependency

SOURCE_FIXTURE = {'source_repo': '/offline/fixture', 'head': 'a' * 40,
                  'behavior_sha256': 'b' * 64, 'source_frozen': False}


def case(length=2):
    registry = Registry()
    register_exploratory(registry, runtime_capability=TOPOLOGY_CAPABILITY)
    plan = topology_input.make_topology_plan(
        topology_model.generate_program(0, writers=1, readers=1),
        [{'operator_id': 'action.insert', 'slot_id': 'S1.topology.actions'}] * length,
        rng_seed=7)
    scenario = Scenario(registry.get_target(TOPOLOGY_TARGET), 'rmw_cyclonedds_cpp',
                        'mutated', 0, 7, {}, plan)
    return registry.get_services(TOPOLOGY_TARGET), scenario, plan


def capsule_config(scenario, descriptor):
    capsule = ReplayCapsule.from_scenario(
        scenario, source_manifest=SOURCE_FIXTURE, final_input=descriptor['final_input'],
        input_layer_receipts=descriptor['input_layer_receipts'])
    capsule = ReplayCapsule.from_dict(json.loads(json.dumps(capsule.to_dict())))
    prepared = capsule.input['effective_input']
    config = {k: deepcopy(prepared[k]) for k in [
        'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
        'mutation_kind', 'carrier', 'input_layer_receipts', 'source_manifest', 'final_input']}
    config.update(input_identity_schema=INPUT_SCHEMA_V2,
                  effective_input_sha256=capsule.input['effective_input_sha256'],
                  effective_input=deepcopy(prepared), scenario=scenario.to_dict(),
                  mutation_plan=scenario.mutation_plan.to_dict(),
                  plan_hash=scenario.mutation_plan.plan_hash(),
                  mutation_plan_sha256=canonical_hash(scenario.mutation_plan.to_dict()),
                  runtime_capability=TOPOLOGY_CAPABILITY)
    config.update(runtime_input(prepared['final_input'], backend='topology_pair',
                                runtime_capability=TOPOLOGY_CAPABILITY))
    return capsule, prepared, config


@pytest.mark.parametrize('length', [1, 2, 3, 4])
def test_registry_provider_capsule_runtime_input_each_real_layer_no_rng_replay(length, monkeypatch, tmp_path):
    services, scenario, plan = case(length)
    # Build once with the real mutator. Neither preparation nor capsule/replay
    # may recompute content from a seed.
    monkeypatch.setattr(fuzz_mutators_v2, 'mutate', Mock(side_effect=AssertionError('RNG rerun')))
    monkeypatch.setattr(topology_input, 'make_topology_plan', Mock(side_effect=AssertionError('plan rerun')))
    audit = Mock(wraps=topology_input.validate_topology_plan)
    monkeypatch.setattr(topology_input, 'validate_topology_plan', audit)
    descriptor = services.mutation_provider.prepare_plan(scenario, plan)
    assert audit.called
    assert descriptor['execution_admitted'] is False
    assert descriptor['plan_content_sha256'] == plan.content_hash()
    capsule, prepared, config = capsule_config(scenario, descriptor)
    assert capsule.input['input_identity_schema'] == INPUT_SCHEMA_V2
    assert prepared == scenario.effective_input_v2(
        final_input=plan.final_config, input_layer_receipts=descriptor['input_layer_receipts'],
        source_manifest=SOURCE_FIXTURE)
    assert prepared['final_input'] == config['final_input'] == plan.final_config
    assert prepared['input_layer_receipts'] == descriptor['input_layer_receipts']
    assert effective_input_hash(prepared, schema=INPUT_SCHEMA_V2) == config['effective_input_sha256']
    last = plan.parent_content_sha256
    for i, (operation, receipt) in enumerate(zip(plan.operations, descriptor['input_layer_receipts'])):
        assert operation.input_before_sha256 == last
        assert receipt['schema'] == 'r2f.action-program-input-layer/v1'
        assert receipt == operation.parameters['input_layer_receipts'][0]
        assert receipt['input_before'] != receipt['input_after']
        assert receipt['input_before_sha256'] == canonical_hash(receipt['input_before'])
        assert receipt['input_after_sha256'] == canonical_hash(receipt['input_after'])
        assert receipt['input_before_sha256'] != receipt['input_after_sha256']
        assert operation.input_after_sha256 == canonical_hash({'topology_program': receipt['input_after']})
        assert operation.input_after_sha256 != receipt['input_after_sha256']
        assert receipt['seed'] == 7 + i
        last = operation.input_after_sha256
    assert last == canonical_hash(plan.final_config)
    from r2f.runtime.topology_route import validate_topology_route
    assert validate_topology_route(config).to_dict() == plan.to_dict()
    # Fake callback only proves exact dispatch/binding, not effects or admission.
    callback = Mock(return_value={'fixture': True, 'execution_evidence': False})
    result = services.carrier.apply(scenario, prepared,
                                   execution={'config': config, 'output_dir': tmp_path, 'runtime': callback})
    assert result == {'fixture': True, 'execution_evidence': False}
    callback.assert_called_once_with(config, tmp_path)
    assert callback.call_args.args[0]['topology_program'] == plan.final_config['topology_program']
    # detached copies, no mutation of serialized source receipts
    descriptor['input_layer_receipts'][0]['input_after'].clear()
    assert topology_input.topology_input_layer_receipts(plan)[0]['input_after']


@pytest.mark.parametrize('field', ['runtime_capability', 'topology_program', 'final_input',
                                  'effective_input_sha256', 'mutation_plan', 'input_layer_receipts'])
def test_carrier_tamper_rejected_before_callback(field, tmp_path):
    services, scenario, plan = case()
    capsule, prepared, config = capsule_config(scenario, services.mutation_provider.prepare(scenario))
    if field == 'runtime_capability':
        config.pop(field)
    elif field == 'effective_input_sha256':
        config[field] = '0' * 64
    elif field == 'input_layer_receipts':
        config[field] = []
    else:
        config[field] = {}
    callback = Mock(side_effect=AssertionError('must not call'))
    with pytest.raises(ValueError):
        services.carrier.apply(scenario, prepared,
                               execution={'config': config, 'output_dir': tmp_path, 'runtime': callback})
    callback.assert_not_called()


def test_provider_performs_s1_specific_audit_not_just_core_shape(monkeypatch):
    services, scenario, plan = case()
    def reject(_):
        raise ValueError('sentinel_explicit_s1_audit')
    monkeypatch.setattr(topology_input, 'validate_topology_plan', reject)
    with pytest.raises(ValueError, match='sentinel_explicit_s1_audit'):
        services.mutation_provider.prepare_plan(scenario, plan)


@pytest.mark.parametrize('change', ['seed', 'plan', 'arm', 'target'])
def test_provider_rejects_mismatched_scenario(change):
    services, scenario, plan = case()
    if change == 'seed':
        scenario.seed += 1
    elif change == 'plan':
        scenario.mutation_plan = case(3)[2]
    elif change == 'arm':
        scenario.arm = 'baseline'
    else:
        scenario.target.observation_contract['runtime_capability'] = None
    with pytest.raises(ValueError):
        services.mutation_provider.prepare_plan(scenario, plan)


def test_capsule_plan_final_receipts_and_runtime_capability_tamper():
    services, scenario, plan = case()
    capsule, prepared, config = capsule_config(scenario, services.mutation_provider.prepare(scenario))
    for field in ['final_input', 'input_layer_receipts', 'mutation_plan', 'contract_hash']:
        damaged = capsule.to_dict()
        damaged['input']['effective_input'][field] = {} if field != 'contract_hash' else '0' * 64
        with pytest.raises(ValueError):
            ReplayCapsule.from_dict(damaged)
    for cap in [None, 'f2_windows_v1', TOPOLOGY_TARGET]:
        with pytest.raises(ValueError, match='capability_required'):
            runtime_input(plan.final_config, backend='topology_pair', runtime_capability=cap)
    with pytest.raises(ValueError, match='keys_mismatch'):
        runtime_input({**plan.final_config, 'topic': '/not_allowed'}, backend='topology_pair',
                      runtime_capability=TOPOLOGY_CAPABILITY)


def test_services_resolve_independent_fact_oracle_and_missing_dependency_fails(monkeypatch):
    services, scenario, plan = case()
    from r2f.campaign.topology_adapter import TopologyFactProducer
    from r2f.modules.s1.topology_oracles import TopologyOracle
    raw = {'events': [], 'runtime': {}, 'config': {}}
    facts = services.fact_producer.produce(raw)
    assert facts['evidence_error']
    assert facts['diagnostic_valid'] is False
    assert facts['execution_evidence'] is False and facts['candidate_eligible'] is False
    assert all(value is None for value in facts['fact_values'].values())
    assert services.oracle.evaluate(scenario, facts)['verdict'] == 'unknown'
    for symbol, method, service, args in [
        (TopologyFactProducer, 'produce', services.fact_producer, (raw,)),
        (TopologyOracle, 'evaluate', services.oracle, (scenario, {})),
    ]:
        sentinel = {'verdict': 'unknown', 'fixture': True}
        spy = Mock(return_value=sentinel)
        monkeypatch.setattr(symbol, method, spy)
        assert getattr(service, method)(*args) is sentinel
        spy.assert_called_once_with(*args)
    with pytest.raises(ValueError, match='topology_service_dependency_unavailable'):
        _ServiceDependency('r2f.no_such_dependency', 'Absent', 'produce').produce(raw)


def test_coordinated_receipt_rehash_cannot_bypass_s1_carrier(tmp_path):
    services, scenario, plan = case()
    descriptor = services.mutation_provider.prepare(scenario)
    # All generic identities consistently rehashed with a false layer list.
    # An adapter must still bind receipts back to the real serialized plan.
    prepared = scenario.effective_input_v2(final_input=plan.final_config,
        input_layer_receipts=[], source_manifest=SOURCE_FIXTURE)
    config = {'runtime_capability': TOPOLOGY_CAPABILITY,
              'effective_input_sha256': effective_input_hash(prepared, schema=INPUT_SCHEMA_V2)}
    callback = Mock(side_effect=AssertionError('must not call'))
    with pytest.raises(ValueError, match='topology_input_layer_receipts_mismatch'):
        services.carrier.apply(scenario, prepared,
            execution={'config': config, 'runtime': callback, 'output_dir': tmp_path})
    callback.assert_not_called()
    assert descriptor['input_layer_receipts']


@pytest.mark.parametrize('length', [1, 2, 3, 4])
def test_provider_layers_independently_of_capsule_layout(length, monkeypatch):
    services, scenario, plan = case(length)
    monkeypatch.setattr(fuzz_mutators_v2, 'mutate', Mock(side_effect=AssertionError('RNG rerun')))
    descriptor = services.mutation_provider.prepare(scenario)
    receipts = descriptor['input_layer_receipts']
    assert len(receipts) == length
    assert descriptor['final_input'] == plan.final_config
    assert descriptor['execution_admitted'] is False
    current_hash = plan.parent_content_sha256
    for index, operation in enumerate(plan.operations):
        receipt = receipts[index]
        assert operation.input_before_sha256 == current_hash
        assert receipt['input_before'] != receipt['input_after']
        assert canonical_hash(receipt['input_before']) == receipt['input_before_sha256']
        assert canonical_hash(receipt['input_after']) == receipt['input_after_sha256']
        assert operation.input_after_sha256 == canonical_hash({'topology_program': receipt['input_after']})
        assert operation.input_before_sha256 != operation.input_after_sha256
        assert receipt['execution_evidence'] is False and receipt['candidate_eligible'] is False
        current_hash = operation.input_after_sha256
    assert current_hash == canonical_hash(descriptor['final_input'])


def test_provider_rejects_s1_tamper_that_generic_core_intentionally_does_not_interpret():
    from r2f.core.mutation_plan import MutationPlan
    services, scenario, plan = case()
    changed = plan.to_dict()
    changed['operations'][0]['parameters']['input_layer_receipts'][0]['v2_case']['provenance']['seed'] = 999
    generic = MutationPlan.from_dict(changed)
    assert generic.validate()  # generic action-program core does not import S1
    scenario.mutation_plan = generic
    with pytest.raises(ValueError):
        services.mutation_provider.prepare_plan(scenario, generic)


def test_capsule_generic_payload_layout_version_exact_and_tamper_closed():
    services, scenario, plan = case()
    capsule, prepared, config = capsule_config(scenario, services.mutation_provider.prepare(scenario))
    layout = capsule.input['payload_layout']
    assert layout == {
        'schema': 'r2f.replay-action-program-payload/v1', 'surface_id': 'S1.topology.program',
        'input_path': 'topology_program', 'program_schema': topology_model.SCHEMA,
        'contract_hash': plan.contract_hash, 'payload_sha256': canonical_hash(config['topology_program']),
    }
    assert set(prepared['final_input']) == {'topology_program'}
    assert not {'topic', 'message_type', 'qos'} & set(capsule.input)
    assert 'payload_layout' not in prepared
    for field in layout:
        broken = capsule.to_dict()
        broken['input']['payload_layout'][field] = 'tampered'
        with pytest.raises(ValueError, match='layout mismatch'):
            ReplayCapsule.from_dict(broken)
    broken = capsule.to_dict()
    broken['input'].pop('payload_layout')
    with pytest.raises(ValueError, match='layout mismatch'):
        ReplayCapsule.from_dict(broken)


def test_canonical_runtime_raw_mismatch_ingest_aborts_with_primary_rule_not_keyerror(tmp_path):
    """Actual registry services and canonical run_pair, only its backend is offline.

    Deliberately damage RETURNED events, leaving raw bytes/hash intact. This is
    tool/artifact failure containment, never synthetic SUT execution/effect.
    """
    import hashlib
    from r2f.campaign.runtime_adapter import ingest_execution
    from r2f.modules.s1.topology_input import TOPOLOGY_RULE
    from r2f.runtime.ros2_pair import run_pair
    from r2f.runtime.topology_worker import OfflineTopologyBackend
    services, scenario, plan = case(1)
    capsule, prepared, config = capsule_config(scenario, services.mutation_provider.prepare(scenario))
    config.update(run_id='topology-ingest-negative-fixture', domain_id=191, backend='offline_fake')
    runtime = services.carrier.apply(scenario, prepared, execution={
        'config': config, 'output_dir': tmp_path,
        'runtime': lambda cfg, out: run_pair(cfg, out, topology_backend=OfflineTopologyBackend()),
    })
    assert runtime['real_execution'] is False and runtime['fixture'] is True
    assert runtime['events'] and runtime['raw_path']
    raw = Path(runtime['raw_path']).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    assert digest == runtime['raw_sha256']
    runtime['events'] = deepcopy(runtime['events'])
    runtime['events'][0]['returned_only_tamper'] = True
    row = ingest_execution(scenario, config, runtime, capsule, services=services)
    assert row['execution_stage'] == 'aborted'
    assert row['ingestion_error'] == 'returned events differ from raw'
    assert row['rule_id'] == row['oracle']['rule_id'] == TOPOLOGY_RULE
    assert row['oracle']['rule_role'] == 'primary'
    assert row['oracle_verdict'] == 'unknown'
    assert row['actual_run'] is False
    assert row['raw_path'] == runtime['raw_path'] and row['raw_sha256'] == digest
    assert Path(runtime['raw_path']).read_bytes() == raw
    assert not any(runtime['mutation_receipt'][key] for key in ('applied', 'hit', 'effect_observed'))
