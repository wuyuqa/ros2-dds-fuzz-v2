"""Issue #13: sealed native type-support inputs cross canonical replay ingest."""
from copy import deepcopy
import json
from unittest.mock import patch

from r2f.campaign import replay
from r2f.core import Registry, ReplayCapsule, Scenario
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_input import make_topology_plan
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET
from test_topology_cycle import SOURCE, offline_runtime
from test_topology_scoped_lookup import lookup_program


def test_sealed_native_lookup_artifact_reaches_replay_without_changing_input_identity(tmp_path):
    program = lookup_program()
    reference = {'manifest_path': '/qualified/lookup-input.json',
                 'manifest_sha256': 'a' * 64, 'placement': 'other_participant'}
    program['actions'][1]['params']['native_type_support'] = reference
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability=TOPOLOGY_CAPABILITY)
    target = registry.get_target(TOPOLOGY_TARGET)
    plan = make_topology_plan(program, [{'operator_id': 'queue.depth'}], rng_seed=7)
    scenario = Scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0, 7,
                        {'mutation_kind': 'endpoint_program', 'carrier': 'ros2_endpoint',
                         'mutation_parameters': plan.final_config}, plan)
    prepared = registry.get_services(TOPOLOGY_TARGET).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
        final_input=plan.final_config, input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
    source = tmp_path / 'original' / 'capsule.json'
    source.parent.mkdir()
    source.write_text(json.dumps(capsule.to_dict()))
    original = source.read_bytes()
    captured = []
    runtime = offline_runtime(captured)
    with patch.object(replay, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch('r2f.modules.s1.topology_input.make_topology_plan', side_effect=AssertionError('remutation')):
        report = replay.replay_cell(source, output_dir=tmp_path / 'replay', domain_id=81, runtime=runtime)
        audit = replay.audit_replay(report['report_path'])
    runtime.assert_called_once()
    actual = captured[0]
    assert actual['topology_program'] == plan.final_config['topology_program']
    assert actual['effective_input_sha256'] == capsule.input['effective_input_sha256']
    assert actual['input_layer_receipts'] == capsule.input['effective_input']['input_layer_receipts']
    assert actual['domain_id'] == 81 and source.read_bytes() == original
    assert not report['real_ros_execution'] and not report['real_runtime_attempt']
    assert audit['valid'], audit


def test_input_artifact_exception_does_not_change_the_generic_metadata_contract():
    import pytest
    from r2f.campaign.identity import reject_execution_identity
    for key in ('manifest_path', 'raw_path', 'run_id', 'domain_id'):
        with pytest.raises(ValueError, match='execution metadata'):
            reject_execution_identity({'nested': {key: '/not/a/submitted/build/input'}})


def test_native_reference_cannot_hide_execution_fields_or_move_to_another_action():
    import pytest
    reference = {'manifest_path': '/qualified/lookup-input.json',
                 'manifest_sha256': 'a' * 64, 'placement': 'same_participant'}
    for damage in ('run_id', 'raw_path', 'missing_hash', 'bad_hash', 'other_action', 'other_location'):
        program = lookup_program()
        parameters = program['actions'][1]['params']
        parameters['native_type_support'] = deepcopy(reference)
        if damage in ('run_id', 'raw_path'):
            parameters['native_type_support'][damage] = 'execution-only'
        elif damage == 'missing_hash':
            parameters['native_type_support'].pop('manifest_sha256')
        elif damage == 'bad_hash':
            parameters['native_type_support']['manifest_sha256'] = 'not-a-digest'
        elif damage == 'other_action':
            program['actions'][1].update(op='snapshot', params={'native_type_support': reference})
        else:
            parameters.pop('native_type_support')
            program['environment']['native_type_support'] = reference
        with pytest.raises(ValueError):
            make_topology_plan(program, [{'operator_id': 'queue.depth'}], rng_seed=7)


def test_replay_admission_does_not_disable_native_artifact_identity_checks(tmp_path, monkeypatch):
    import hashlib
    from pathlib import Path
    from r2f.campaign.identity import source_manifest
    from r2f.runtime import native_lookup_support as support
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    from test_topology_scoped_lookup import native_fixture

    def unexpected_load(*args, **kwargs):
        raise AssertionError('Invalid build input must be rejected before DSO load')
    monkeypatch.setattr(support.c, 'CDLL', unexpected_load)
    for damage in ('manifest', 'source', 'sdk', 'schema', 'binary', 'missing'):
        native, _, calls = native_fixture()
        native.VERIFIED_C_HEADERS = {}
        binary = tmp_path / (damage + '.so')
        binary.write_bytes(b'not an executable DSO: rejected-input control')
        manifest = {'schema': support.SCHEMA,
            'source': source_manifest(Path(support.__file__).resolve().parents[3]),
            'source_sha256': {n: hashlib.sha256(t.encode()).hexdigest() for n, t in support.SOURCES.items()},
            'sdk_sha256': {}, 'library_path': str(binary),
            'library_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
        if damage == 'source': manifest['source']['head'] = '0' * 40
        elif damage == 'sdk': manifest['sdk_sha256'] = {'/unqualified/header.h': 'f' * 64}
        elif damage == 'schema': manifest['source_sha256'] = {}
        path = tmp_path / (damage + '.json')
        path.write_text(json.dumps(manifest))
        reference = {'manifest_path': str(path),
                     'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                     'placement': 'same_participant'}
        if damage == 'manifest': reference['manifest_sha256'] = '0' * 64
        elif damage == 'binary': binary.write_bytes(b'changed artifact')
        elif damage == 'missing': path.unlink()
        receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer', 'participant',
            'rt/r2f/topology/a', native_type_support=reference, ros_type_name='std_msgs/msg/String')
        assert receipt['status'] == 'unknown' and not receipt['complete'], (damage, receipt)
        assert receipt['attribution'] == 'binding_adaptation_or_observation_error'
        assert not receipt['candidate_eligible'] and receipt['cleanup_confirmed']
        assert calls == []


def test_other_target_cannot_replay_a_native_lookup_shaped_metadata_exception(tmp_path):
    from unittest.mock import Mock
    from r2f.campaign.identity import semantic_input
    from r2f.core import compile_scenario
    from r2f.modules.s1.module import S1Module
    target = S1Module().targets['discovery.late_endpoint']
    scenario = compile_scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0, seed=7)
    submitted = semantic_input('mutated')
    submitted['native_type_support'] = {'manifest_path': '/qualified/lookup-input.json',
        'manifest_sha256': 'a' * 64, 'placement': 'same_participant'}
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
                                         final_input=submitted, origin_domain=80)
    source = tmp_path / 'original' / 'capsule.json'
    source.parent.mkdir()
    source.write_text(json.dumps(capsule.to_dict()))
    original = source.read_bytes()
    runtime = Mock(side_effect=AssertionError('No admission outside topology lookup'))
    with patch.object(replay, 'source_manifest', return_value=deepcopy(SOURCE)):
        report = replay.replay_cell(source, output_dir=tmp_path / 'replay', domain_id=81, runtime=runtime)
        audit = replay.audit_replay(report['report_path'])
    runtime.assert_not_called()
    assert report['status'] == 'fail_artifact' and report['phase'] == 'ingest'
    assert 'manifest_path is execution metadata' in report['failure_reason']
    assert not report['runtime_invoked'] and source.read_bytes() == original
    assert audit['valid'], audit
