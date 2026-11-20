"""Offline replay selection binding; no ROS, DDS, native load or vendor build.

Authored metadata/binary fixtures below test integrity gates, NOT execution or
membership. Optional live-capsule tests only read existing artifacts and replace
all runtime calls before invocation; old evidence is never edited.
"""
from copy import deepcopy
import ctypes
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from r2f.campaign import replay
from r2f.core import ReplayCapsule, Scenario, canonical_hash, raw_sha256
from r2f.instrumentation.fastdds_peer import activation, probe
from test_topology_cycle import SOURCE, capsule_from_registry

KEYS = ('observation_mode', 'fastdds_peer_instrumentation')
LIVE = Path(__file__).resolve().parents[1] / 'results/main-review/s1-fastdds-peer-runtime-20261120/live-v2/original/cases'


@pytest.fixture(autouse=True)
def no_native_load(monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError('native_loader_forbidden_in_offline_replay_test')
    monkeypatch.setattr(ctypes, 'CDLL', forbidden)
    monkeypatch.setattr(ctypes, 'PyDLL', forbidden)


@pytest.fixture
def bound(tmp_path):
    registry, original_scenario, plan, _ = capsule_from_registry(2)
    scenario = Scenario(original_scenario.target, 'rmw_fastrtps_cpp', 'mutated', 0, 7,
                        {'mutation_kind': 'endpoint_program', 'carrier': 'ros2_endpoint',
                         'mutation_parameters': plan.final_config}, plan)
    prepared = registry.get_services(scenario.target_id).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
        final_input=plan.final_config, input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
    lib = tmp_path / 'install/lib'
    lib.mkdir(parents=True)
    binaries = {}
    for role, name in [('vendor', 'libfastrtps.so.2.14.7'), ('provider', 'libr2f_fastdds_peer.so')]:
        path = lib / name
        path.write_bytes(('offline non-ELF fixture ' + role).encode())
        binaries[role] = {'path': str(path), 'sha256': raw_sha256(path)}
    manifest = {'schema': probe.BUILD_SCHEMA, 'status': 'built', 'observation_mode': 'instrumentation',
        'vendor_version': '2.14.7', 'verification': {'compilation': 'passed'},
        'provider_source_sha256': raw_sha256(Path(probe.__file__).with_name('provider.cpp')),
        'install': str(lib.parent), 'binaries': binaries,
        'patch': {'abi': {'schema_version': 1, 'result_size': 64, 'guid_size': 16,
                          'layout_changed': False, 'virtual_slots_added': False}}}
    manifest_path = tmp_path / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest))
    reference = {'manifest_path': str(manifest_path), 'manifest_sha256': raw_sha256(manifest_path)}
    execution = {key: deepcopy(capsule.input[key]) for key in (
        'target_id', 'target_revision', 'target', 'seed', 'rmw', 'arm', 'repeat', 'mutation_kind',
        'carrier', 'effective_input_sha256', 'input_identity_schema', 'source_manifest',
        'mutation_parameters', 'state_steps', 'mutation_plan')}
    raw_path = tmp_path / 'original/raw.jsonl'
    raw_path.parent.mkdir()
    execution.update(run_id='offline-original', domain_id=80, raw_path=str(raw_path),
        observation_mode='instrumentation', fastdds_peer_instrumentation=deepcopy(reference),
        final_input=deepcopy(plan.final_config),
        input_layer_receipts=deepcopy(prepared['input_layer_receipts']))
    context = {key: execution[key] for key in ('run_id', 'target_id', 'target_revision',
                                             'rmw', 'domain_id', 'arm', 'repeat')}
    context.update(input_identity_schema=capsule.input['input_identity_schema'],
                   effective_input_sha256=capsule.input['effective_input_sha256'], plan_hash=plan.plan_hash())
    metadata = {key: execution[key] for key in ('run_id', 'target_id', 'target_revision',
                                              'rmw', 'domain_id', 'arm', 'repeat', 'raw_path')}
    metadata.update(schema='r2f.topology-execution-metadata/v1', sealed_execution_context=context,
        context_basis='sealed_input_declaration_not_sut_observation',
        raw_refs_hash_binding='enclosing_result.raw_sha256',
        source_manifest=deepcopy(SOURCE), input_program_sha256=canonical_hash(plan.final_config['topology_program']),
        observation_mode='instrumentation', fastdds_peer_instrumentation=deepcopy(reference))
    events = [dict(event='topology_execution_metadata', metadata=deepcopy(metadata), fixture=True)]
    execution['runtime_execution_metadata'] = deepcopy(metadata)
    capsule.execution = execution  # authored attachment, NOT a real execution claim
    fixture = SimpleNamespace(capsule=capsule, execution=execution, metadata=metadata,
        events=events, raw_path=raw_path, reference=reference, manifest=manifest,
        manifest_path=manifest_path, plan=plan)
    _persist_raw(fixture)
    return fixture


def _persist_raw(fixture):
    fixture.raw_path.write_text(''.join(json.dumps(event) + '\n' for event in fixture.events))
    fixture.execution['raw_sha256'] = raw_sha256(fixture.raw_path)


def project(fixture):
    return replay._replay_config(fixture.capsule, 'offline-replay', 81, SOURCE)


def test_bound_selection_restored_exactly_without_touching_input_or_old_evidence(bound):
    before = deepcopy(bound.capsule.to_dict())
    raw_before = bound.raw_path.read_bytes()
    config = project(bound)
    assert config['observation_mode'] == 'instrumentation'
    assert config['fastdds_peer_instrumentation'] == bound.reference
    assert config['final_input'] == bound.capsule.input['effective_input']['final_input']
    assert config['effective_input_sha256'] == bound.capsule.input['effective_input_sha256']
    assert config['target_id'] == before['input']['target_id']
    assert config['target_revision'] == before['input']['target_revision']
    assert config['domain_id'] == 81 and config['run_id'] == 'offline-replay'
    assert bound.capsule.to_dict() == before and bound.raw_path.read_bytes() == raw_before
    config['fastdds_peer_instrumentation']['manifest_sha256'] = '0' * 64
    assert bound.execution['fastdds_peer_instrumentation'] == bound.reference


def test_wrapper_pid_annotations_do_not_destroy_exact_selection_binding(bound):
    metadata = bound.execution['runtime_execution_metadata']
    metadata.update(pid_namespace='pid:[fixture]', pid_interpretation='namespace_PID_not_host_PID')
    metadata['workers'] = [{'pid_namespace': 'pid:[fixture]'}]
    assert project(bound)['fastdds_peer_instrumentation'] == bound.reference


@pytest.mark.parametrize('location', ['execution', 'runtime_metadata', 'raw_metadata'])
@pytest.mark.parametrize('damage', ['mode', 'ref_hash', 'ref_path', 'drop_mode', 'drop_ref', 'drop_both'])
def test_tampered_or_deleted_selection_cannot_silently_fall_back(bound, location, damage):
    record = {'execution': bound.execution,
              'runtime_metadata': bound.execution['runtime_execution_metadata'],
              'raw_metadata': bound.events[0]['metadata']}[location]
    if damage == 'mode': record['observation_mode'] = 'blackbox'
    elif damage == 'ref_hash': record['fastdds_peer_instrumentation']['manifest_sha256'] = '0' * 64
    elif damage == 'ref_path': record['fastdds_peer_instrumentation']['manifest_path'] += '.other'
    elif damage == 'drop_mode': record.pop('observation_mode')
    elif damage == 'drop_ref': record.pop('fastdds_peer_instrumentation')
    else:
        for key in KEYS: record.pop(key)
    if location == 'raw_metadata': _persist_raw(bound)
    with pytest.raises(ValueError): project(bound)


@pytest.mark.parametrize('damage', ['missing_raw', 'raw_hash', 'raw_bytes', 'duplicate_metadata',
    'missing_metadata', 'raw_not_object', 'runtime_not_object', 'raw_metadata_not_object'])
def test_raw_hash_unique_metadata_required(bound, damage):
    if damage == 'missing_raw': bound.raw_path.unlink()
    elif damage == 'raw_hash': bound.execution['raw_sha256'] = '0' * 64
    elif damage == 'raw_bytes': bound.raw_path.write_text(bound.raw_path.read_text() + ' ')
    elif damage == 'duplicate_metadata':
        bound.events.append(deepcopy(bound.events[0])); _persist_raw(bound)
    elif damage == 'missing_metadata':
        bound.events.clear(); _persist_raw(bound)
    elif damage == 'raw_not_object':
        bound.events[:] = ['bad-row']; _persist_raw(bound)
    elif damage == 'runtime_not_object': bound.execution['runtime_execution_metadata'] = ['bad']
    else:
        bound.events[0]['metadata'] = ['bad']; _persist_raw(bound)
    with pytest.raises(ValueError): project(bound)


@pytest.mark.parametrize('location', ['runtime_metadata', 'raw_metadata'])
@pytest.mark.parametrize('field', ['run_id', 'domain_id', 'rmw', 'target_id', 'target_revision',
    'arm', 'repeat', 'sealed_execution_context', 'source_manifest', 'input_program_sha256',
    'context_basis', 'schema', 'raw_path', 'raw_refs_hash_binding'])
def test_exact_original_input_runtime_context_binding(bound, location, field):
    record = bound.execution['runtime_execution_metadata'] if location == 'runtime_metadata' else bound.events[0]['metadata']
    record[field] = 'tampered'
    if location == 'raw_metadata': _persist_raw(bound)
    with pytest.raises(ValueError): project(bound)


@pytest.mark.parametrize('field', ['target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
                                  'carrier', 'mutation_kind', 'effective_input_sha256', 'source_manifest'])
def test_execution_cannot_be_rebound_to_another_input(bound, field):
    bound.execution[field] = 'tampered'
    with pytest.raises(ValueError): project(bound)


@pytest.mark.parametrize('damage', ['manifest_changed', 'binary_changed', 'missing_manifest',
                                  'relative', 'extra_ref_key', 'unknown_mode', 'blackbox_ref'])
def test_pinned_manifest_revalidated_not_just_copied(bound, damage):
    if damage == 'manifest_changed': bound.manifest_path.write_text('{}')
    elif damage == 'binary_changed': Path(bound.manifest['binaries']['vendor']['path']).write_text('changed')
    elif damage == 'missing_manifest': bound.manifest_path.unlink()
    else:
        for record in (bound.execution, bound.execution['runtime_execution_metadata'], bound.events[0]['metadata']):
            if damage == 'relative': record['fastdds_peer_instrumentation']['manifest_path'] = 'relative'
            elif damage == 'extra_ref_key': record['fastdds_peer_instrumentation']['extra'] = 1
            elif damage == 'unknown_mode': record['observation_mode'] = 'unknown'
            else: record['observation_mode'] = 'blackbox'
        _persist_raw(bound)
    with pytest.raises(ValueError): project(bound)


def test_runtime_config_profile_and_history_cannot_choose_selection(bound, tmp_path):
    config_path = tmp_path / 'untrusted-runtime-config.json'
    config_path.write_text(json.dumps({'observation_mode': 'blackbox'}))
    bound.execution['runtime_config_path'] = str(config_path)
    bound.execution['profile'] = {'observation_mode': 'blackbox'}
    bound.capsule.execution_history.append({'observation_mode': 'blackbox'})
    assert project(bound)['observation_mode'] == 'instrumentation'
    bound.capsule.execution = None
    assert not set(project(bound)).intersection(KEYS)  # no guessing from history


@pytest.mark.parametrize('explicit', [False, True])
def test_default_and_explicit_blackbox_never_restore_native_override(bound, explicit):
    for record in (bound.execution, bound.execution['runtime_execution_metadata'], bound.events[0]['metadata']):
        for key in KEYS: record.pop(key, None)
    # Runtime topology defaults declare blackbox and a null native ref.
    for record in (bound.execution['runtime_execution_metadata'], bound.events[0]['metadata']):
        record.update(observation_mode='blackbox', fastdds_peer_instrumentation=None)
    if explicit: bound.execution['observation_mode'] = 'blackbox'
    _persist_raw(bound)
    config = project(bound)
    assert config.get('observation_mode', 'blackbox') == 'blackbox'
    assert 'fastdds_peer_instrumentation' not in config
    assert ('observation_mode' in config) is explicit


def test_native_worker_receipt_cannot_replace_missing_topology_metadata(bound):
    bound.execution['runtime_execution_metadata'] = {}
    for key in KEYS: bound.execution.pop(key)
    bound.events[:] = [dict(event='native_peer_provider_selected', fixture=True)]
    _persist_raw(bound)
    with pytest.raises(ValueError, match='observation_raw_metadata_not_unique'): project(bound)


def test_selection_not_allowed_in_hashed_final_input(bound):
    bound.capsule.input['effective_input']['final_input']['observation_mode'] = 'instrumentation'
    with pytest.raises(ValueError, match='execution_metadata_not_input'):
        replay._replay_observation_config(bound.capsule)


def test_bind_new_execution_retains_mode_ref_without_input_identity_change(bound):
    config = project(bound)
    before = canonical_hash(bound.capsule.input)
    row = replay._bind_execution_input({}, config, bound.capsule)
    assert row['observation_mode'] == 'instrumentation'
    assert row['fastdds_peer_instrumentation'] == bound.reference
    assert row['actual_submitted_payload_sha256'] == canonical_hash(config['final_input'])
    assert canonical_hash(bound.capsule.input) == before


def test_replay_declaration_and_audit_require_exact_selection(bound, tmp_path):
    config = project(bound)
    declaration = replay._observation_replay_declaration(bound.capsule, config)
    assert declaration['selection'] == {key: bound.execution[key] for key in KEYS}
    assert declaration['basis'] == 'original_execution_and_sha256_bound_raw_metadata'
    assert declaration['original_raw_sha256'] == bound.execution['raw_sha256']
    assert declaration['input_identity_unchanged'] is True
    (tmp_path / 'runtime_config.json').write_text(json.dumps(config))
    report = dict(domain_id=81, replay_id='offline-replay', execution_origin='injected_runtime',
                  formal_runtime=False, runtime_boundary='host', observation_replay=declaration)
    errors = []
    replay._check_audit_config(tmp_path, bound.capsule, report, errors)
    assert not errors, errors
    report['observation_replay']['selection']['observation_mode'] = 'blackbox'
    replay._check_audit_config(tmp_path, bound.capsule, report, errors)
    assert 'replay observation declaration differs from original execution selection' in errors


def test_fresh_execution_cannot_claim_blackbox_for_native_replay(bound):
    config = project(bound)
    result = {'execution_metadata': {'observation_mode': 'blackbox', 'fastdds_peer_instrumentation': None}}
    errors = replay._topology_context_errors(config, result, output_root=None)
    assert 'execution_metadata_observation_selection_mismatch:observation_mode' in errors
    assert 'execution_metadata_observation_selection_mismatch:fastdds_peer_instrumentation' in errors


@pytest.fixture
def live_capsule():
    paths = list(LIVE.glob('*/capsule.json'))
    if not paths: pytest.skip('read-only live-v2 capsule unavailable')
    assert len(paths) == 1
    path = paths[0]
    before = path.read_bytes()
    capsule = ReplayCapsule.from_dict(json.loads(before))
    yield path, capsule
    assert path.read_bytes() == before


def test_real_live_v2_original_execution_selection_is_restored_read_only(live_capsule):
    _, capsule = live_capsule
    before = deepcopy(capsule.to_dict())
    row = capsule.execution
    assert row['observation_mode'] == row['runtime_execution_metadata']['observation_mode'] == 'instrumentation'
    assert not set(capsule.input['effective_input']['final_input']).intersection(KEYS)
    assert raw_sha256(row['raw_path']) == row['raw_sha256']
    config = replay._replay_config(capsule, 'offline-live-capsule-projection', 157,
                                   capsule.input['source_manifest'])
    assert {key: config[key] for key in KEYS} == {key: row[key] for key in KEYS}
    assert capsule.to_dict() == before


def test_real_live_capsule_replay_boundary_preserves_selection_but_never_runs_dds(live_capsule, tmp_path, monkeypatch):
    path, capsule = live_capsule
    from test_campaign_exploratory_replay import blocked
    runtime = Mock(side_effect=blocked)  # writes a fixture failure, never ROS
    monkeypatch.setattr(replay, 'source_manifest', lambda _: deepcopy(capsule.input['source_manifest']))
    report = replay.replay_cell(path, output_dir=tmp_path / 'replay', domain_id=157, runtime=runtime)
    runtime.assert_called_once()
    config = runtime.call_args.args[0]
    assert {key: config[key] for key in KEYS} == {key: capsule.execution[key] for key in KEYS}
    assert report['observation_replay']['selection'] == {key: config[key] for key in KEYS}
    assert report['real_ros_execution'] is False and report['status'] != 'pass'
    assert replay.audit_replay(report['report_path'])['valid']
