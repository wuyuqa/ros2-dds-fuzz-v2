"""Saved observation-selection binding; authored fixtures grant no runtime credit."""
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from r2f.campaign import replay
from r2f.core import Registry, ReplayCapsule, Scenario, MutationReceipt, canonical_hash, raw_sha256
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_input import make_topology_plan
from test_topology_cycle import SOURCE
from test_topology_participant_control_multi import multi_program


def saved_observation(tmp_path, *, grouped=True):
    program = multi_program()
    if not grouped:
        program.pop('participant_control')
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability='topology_v1')
    target = registry.get_target('discovery.endpoint_topology.fuzz_v1')
    plan = make_topology_plan(program, [{'operator_id': 'action.insert'}], rng_seed=7)
    scenario = Scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0, 7,
        {'mutation_kind': 'endpoint_program', 'carrier': 'ros2_endpoint',
         'mutation_parameters': plan.final_config}, plan)
    prepared = registry.get_services(target.target_id).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
        final_input=plan.final_config, input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
    execution = {key: deepcopy(capsule.input[key]) for key in (
        'target_id', 'target_revision', 'target', 'seed', 'rmw', 'arm', 'repeat', 'mutation_kind',
        'carrier', 'effective_input_sha256', 'input_identity_schema', 'source_manifest',
        'mutation_parameters', 'state_steps', 'mutation_plan')}
    execution.update(run_id='authored-original', domain_id=80, observation_mode='blackbox',
        final_input=deepcopy(plan.final_config), input_layer_receipts=deepcopy(prepared['input_layer_receipts']),
        raw_path=str(tmp_path / 'raw.jsonl'))
    context = {key: execution[key] for key in (
        'run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat')}
    context.update(input_identity_schema=capsule.input['input_identity_schema'],
                   effective_input_sha256=capsule.input['effective_input_sha256'], plan_hash=plan.plan_hash())
    metadata = {key: execution[key] for key in (
        'run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat', 'raw_path')}
    metadata.update(schema='r2f.topology-execution-metadata/v2' if grouped else 'r2f.topology-execution-metadata/v1',
        sealed_execution_context=context, context_basis='sealed_input_declaration_not_sut_observation',
        raw_refs_hash_binding='enclosing_result.raw_sha256', source_manifest=deepcopy(SOURCE),
        input_program_sha256=canonical_hash(plan.final_config['topology_program']), observation_mode='blackbox')
    execution['runtime_execution_metadata'] = metadata
    capsule.execution = execution  # Explicit authored data, never actual ROS evidence.
    fixture = SimpleNamespace(capsule=capsule, execution=execution, metadata=metadata,
        raw_metadata=deepcopy(metadata), raw_path=tmp_path / 'raw.jsonl')
    persist(fixture)
    return fixture


def persist(fixture):
    fixture.raw_path.write_text(json.dumps(dict(event='topology_execution_metadata',
        metadata=fixture.raw_metadata, fixture=True)) + '\n')
    fixture.execution['raw_sha256'] = raw_sha256(fixture.raw_path)


def save_original(fixture, tmp_path, monkeypatch):
    original = tmp_path / 'original'
    (original / 'runtime').mkdir(parents=True)
    source = original / 'capsule.json'
    # Authored aborted attachment exercises the serialized importer. These saved
    # transport labels are never qualification: the injected runtime is offline.
    fixture.execution.update(actual_run=True, execution_source='ros2_public_api',
                             execution_stage='aborted', abort_reason='authored offline fixture',
                             mutation_receipt=MutationReceipt().to_dict())
    source.write_text(json.dumps(fixture.capsule.to_dict()))
    for path in ('report.json', 'runtime_config.json', 'runtime/runtime_result.json'):
        (original / path).write_text('{}')
    before = source.read_bytes(), fixture.raw_path.read_bytes()
    monkeypatch.setattr(replay, 'source_manifest', lambda _: deepcopy(SOURCE))
    return source, before


def test_grouped_v2_saved_observation_restores_fresh_domain_without_changing_original(tmp_path):
    fixture = saved_observation(tmp_path)
    before = deepcopy(fixture.capsule.to_dict())
    raw_before = fixture.raw_path.read_bytes()
    config = replay._replay_config(fixture.capsule, 'authored-replay', 81, SOURCE)
    assert config['observation_mode'] == 'blackbox'
    assert config['run_id'] == 'authored-replay' and config['domain_id'] == 81
    assert config['topology_program'] == before['input']['effective_input']['final_input']['topology_program']
    assert config['effective_input_sha256'] == before['input']['effective_input_sha256']
    assert fixture.capsule.to_dict() == before and fixture.raw_path.read_bytes() == raw_before


@pytest.mark.parametrize('grouped', [False, True], ids=['legacy-v1', 'grouped-v2'])
def test_saved_schema_restores_selection_through_public_replay(tmp_path, monkeypatch, grouped):
    fixture = saved_observation(tmp_path, grouped=grouped)
    source, before = save_original(fixture, tmp_path, monkeypatch)
    runtime = Mock(return_value={'real_execution': False, 'runtime_status': 'tool_error',
                                 'reason': 'explicit offline fixture', 'events': []})
    output = tmp_path / 'replay'
    report = replay.replay_cell(source, output_dir=output, domain_id=81, runtime=runtime)
    runtime.assert_called_once()
    config = json.loads((output / 'runtime_config.json').read_text())
    assert config['domain_id'] == 81 and config['run_id'] != 'authored-original'
    assert config['observation_mode'] == 'blackbox'
    assert config['effective_input_sha256'] == fixture.capsule.input['effective_input_sha256']
    assert config['topology_program'] == fixture.capsule.input['effective_input']['final_input']['topology_program']
    assert report['runtime_invoked'] is True
    assert report['real_ros_execution'] is False and report['status'] != 'pass'
    assert (source.read_bytes(), fixture.raw_path.read_bytes()) == before


@pytest.mark.parametrize('grouped', [False, True], ids=['legacy-v1', 'grouped-v2'])
@pytest.mark.parametrize('surface', ['outer', 'raw', 'both'])
@pytest.mark.parametrize('damage', ['wrong_version', 'unknown_version', 'sealed_context', 'program_hash', 'source'])
def test_saved_metadata_rejects_schema_or_identity_damage(tmp_path, monkeypatch, grouped, surface, damage):
    fixture = saved_observation(tmp_path, grouped=grouped)
    records = {'outer': [fixture.metadata], 'raw': [fixture.raw_metadata],
               'both': [fixture.metadata, fixture.raw_metadata]}[surface]
    for record in records:
        if damage == 'wrong_version':
            record['schema'] = ('r2f.topology-execution-metadata/v1' if grouped
                                else 'r2f.topology-execution-metadata/v2')
        elif damage == 'unknown_version':
            record['schema'] = 'r2f.topology-execution-metadata/v99'
        elif damage == 'sealed_context':
            record['sealed_execution_context']['domain_id'] = 99
        elif damage == 'program_hash':
            record['input_program_sha256'] = '0' * 64
        else:
            record['source_manifest']['head'] = '0' * 40
    persist(fixture)  # Rehash the altered raw: integrity alone must not authorize it.
    source, before = save_original(fixture, tmp_path, monkeypatch)
    runtime = Mock(side_effect=AssertionError('damaged input must not invoke runtime'))
    report = replay.replay_cell(source, output_dir=tmp_path / 'replay', domain_id=81, runtime=runtime)
    runtime.assert_not_called()
    assert report['status'] == 'fail_artifact' and report['phase'] == 'preflight'
    assert report['reason_code'] == 'observation_execution_metadata_invalid'
    assert report['failure_reason'] == 'observation_original_sealed_context_mismatch'
    assert report['runtime_invoked'] is False and report['real_ros_execution'] is False
    assert (source.read_bytes(), fixture.raw_path.read_bytes()) == before
