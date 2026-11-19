"""Consumer path tests use labelled synthetic artifacts, never live evidence."""
from copy import deepcopy
from functools import lru_cache
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign import replay
from r2f.core import canonical_hash
from test_execution_boundary_audit import write
from test_topology_namespace_audit import SyntheticNested, file_ref, reseal_manifest


@pytest.fixture
def nested(tmp_path):
    return SyntheticNested(tmp_path)


def qualify(f, *, origin=replay.ISOLATED_RUNTIME, result=None, row=None, root=None):
    return replay._qualification(origin, f.config, f.result if result is None else result,
        {'actual_run': True, 'execution_stage': 'judged'} if row is None else row,
        output_root=f.root if root is None else root)


def test_audited_nested_uses_original_child_metadata_environment_and_config(nested):
    f = nested
    original_config, original_child = deepcopy(f.config), deepcopy(f.child)
    with patch('r2f.campaign.topology_adapter.facts_from_events', return_value={}) as facts:
        result = qualify(f)
    assert result['boundary_audit']['valid'], result
    errors = result['qualification_errors']
    assert not any(e.startswith(('execution_metadata_', 'persisted_execution_metadata_',
                    'raw_execution_metadata_', 'persisted_environment_', 'raw_environment_',
                    'topology_runtime_artifact_', 'topology_worker_lineage_', 'worker_pid_namespace_'))
                   for e in errors), errors
    events_arg, child_arg, config_arg = facts.call_args.args
    assert child_arg == original_child and events_arg == original_child['events']
    assert config_arg == original_config  # no lock-root, input, plan/hash rewrite
    assert 'execution_boundary' not in child_arg and 'topology_namespace' not in child_arg
    assert not (f.root / 'execution_metadata.json').exists()
    assert not (f.root / 'environment.json').exists()
    assert not (f.root / 'raw.jsonl').exists()
    assert not result['real_ros_execution'] and not result['real_runtime_attempt']
    assert 'offline_fixture_cannot_establish_real_ros' in errors
    assert 'real_judged_execution_missing' in errors


def test_real_facts_gate_still_rejects_synthetic_artifacts(nested):
    result = qualify(nested)
    assert result['boundary_audit']['valid']
    assert not result['real_ros_execution']
    assert 'topology_fixture_cannot_establish_real_execution' in result['qualification_errors']


@pytest.mark.parametrize('damage', ['missing_manifest', 'bad_manifest', 'outer_disk', 'outer_argument',
    'missing_boundary', 'fake_inner_dict', 'wrong_root', 'wrong_raw_root', 'escaped_result_ref',
    'symlink_raw', 'bad_source', 'raw_hash', 'cleanup', 'annotation_scope', 'fake_annotation'])
def test_good_inner_cannot_bypass_bad_outer_audit(nested, tmp_path, damage):
    f = nested
    if damage == 'missing_manifest': (f.side / 'manifest.json').unlink()
    elif damage == 'bad_manifest':
        f.ns_manifest['error'] = 'synthetic failure'; reseal_manifest(f)
    elif damage == 'outer_disk':
        outer = deepcopy(f.result); outer['status'] = 'not-the-returned-result'
        write(f.root / 'runtime_result.json', outer)
    elif damage == 'outer_argument': f.result['runtime_status'] = 'other'
    elif damage == 'missing_boundary': f.result.pop('execution_boundary')
    elif damage == 'fake_inner_dict':
        f.result['inner_artifact_view'] = {'output_root': str(f.child_root), 'result': f.child}
    elif damage == 'wrong_root':
        other = tmp_path / 'caller-root'; other.mkdir()
        result = qualify(f, root=other)
        assert not result['real_ros_execution']
        assert 'execution_boundary_artifact_audit_failed' in result['qualification_errors']
        return
    elif damage == 'wrong_raw_root':
        other = f.root / 'raw.jsonl'; other.write_bytes(f.raw_path.read_bytes())
        f.child['raw_path'] = str(other); f.sync()
    elif damage == 'escaped_result_ref':
        outside = f.root / 'escaped.json'; write(outside, f.child)
        f.ns_manifest['inner_result_ref'] = file_ref(outside); reseal_manifest(f)
    elif damage == 'symlink_raw':
        outside = f.root / 'outside-raw.jsonl'; outside.write_bytes(f.raw_path.read_bytes())
        f.raw_path.unlink(); f.raw_path.symlink_to(outside)
    elif damage == 'bad_source':
        f.ns_manifest['source_manifest']['head'] = 'e' * 40; reseal_manifest(f)
    elif damage == 'raw_hash':
        f.raw_path.write_bytes(f.raw_path.read_bytes() + b'\n')
    elif damage == 'cleanup':
        f.ns_manifest['cleanup']['controller_exited'] = False; f.sync()
    elif damage == 'annotation_scope':
        f.result['execution_metadata']['workers'][0]['pid_namespace'] = f.controller['pid_scope']
        write(f.root / 'runtime_result.json', f.result)
    elif damage == 'fake_annotation':
        f.child['execution_metadata']['pid_namespace'] = f.nested_binding['pid_scope']; f.sync()
    result = qualify(f)
    assert not result['real_ros_execution'], damage
    assert result['boundary_audit']['valid'] is False
    assert 'inner_artifact_view' not in result['boundary_audit']
    assert 'execution_boundary_artifact_audit_failed' in result['qualification_errors']


@pytest.mark.parametrize('damage', ['metadata_disk', 'environment_disk', 'environment_ref',
    'raw_metadata', 'raw_environment', 'child_result', 'child_events'])
def test_nested_metadata_and_original_child_artifacts_still_checked(nested, damage):
    f = nested
    if damage == 'metadata_disk':
        metadata = deepcopy(f.child['execution_metadata']); metadata['plan_hash'] = 'x'
        write(f.child_root / 'execution_metadata.json', metadata)
    elif damage == 'environment_disk':
        write(f.child_root / 'environment.json', dict(f.env, domain_id=99))
    elif damage == 'environment_ref':
        f.child['execution_metadata']['environment_manifest']['path'] = str(f.root / 'environment.json')
        f.sync()
    elif damage == 'raw_metadata':
        next(e for e in f.raw if e['event'] == 'topology_execution_metadata')['metadata']['run_id'] = 'tamper'
        f.sync()
    elif damage == 'raw_environment':
        next(e for e in f.raw if e['event'] == 'environment_preflight')['environment']['domain_id'] = 99
        f.sync()
    elif damage == 'child_result':
        write(f.child_root / 'runtime_result.json', dict(f.child, status='tamper'))
    else:
        child = json.loads((f.side / 'child-runtime-result.json').read_text())
        child['events'] = []; write(f.side / 'child-runtime-result.json', child)
    result = qualify(f)
    assert not result['real_ros_execution'], damage
    assert any('metadata' in e or 'environment' in e or 'artifact' in e for e in result['qualification_errors']), result


@pytest.mark.parametrize('gate', ['injected', 'wrong_origin', 'not_judged', 'not_actual', 'not_real',
                                'not_frozen', 'unavailable_rmw', 'preflight_missing'])
def test_nested_audit_never_replaces_existing_qualification_gates(nested, gate):
    f = nested; row = {'actual_run': True, 'execution_stage': 'judged'}; origin = replay.ISOLATED_RUNTIME
    if gate == 'injected': origin = 'injected_runtime'
    elif gate == 'wrong_origin': origin = replay.DEFAULT_RUNTIME
    elif gate == 'not_judged': row['execution_stage'] = 'effect_observed'
    elif gate == 'not_actual': row['actual_run'] = False
    elif gate == 'not_real': f.child['real_execution'] = False; f.sync()
    elif gate == 'not_frozen': f.config['source_manifest']['source_frozen'] = False
    elif gate == 'unavailable_rmw':
        f.child['environment']['rmw'][f.config['rmw']]['available'] = False; f.sync()
    else:
        f.child['environment']['process_scan']['ok'] = False; f.sync()
    result = qualify(f, origin=origin, row=row)
    assert not result['real_ros_execution']
    expected = {'injected': 'injected_runtime_is_offline_contract_only', 'wrong_origin': 'boundary_route_context_mismatch',
        'not_judged': 'real_judged_execution_missing', 'not_actual': 'real_judged_execution_missing',
        'not_real': 'real_judged_execution_missing', 'not_frozen': 'frozen_source_binding_missing',
        'unavailable_rmw': 'rmw_not_available_in_manifest', 'preflight_missing': 'preflight_receipts_missing'}[gate]
    assert expected in result['qualification_errors'], result


def test_unvalidated_wrapper_fields_not_stripped_from_child(nested):
    f = nested
    f.child['execution_metadata']['workers'][0]['pid_namespace'] = f.nested_binding['pid_scope']
    f.sync()
    result = qualify(f)
    assert 'execution_boundary_artifact_audit_failed' in result['qualification_errors']
    assert not result['real_ros_execution']


def test_boundary_presence_alone_does_not_strip_namespace_annotations(nested):
    f = nested
    # Direct helper has not audited any wrapper, so arbitrary boundary flags
    # must not normalize caller-supplied namespace labels into raw metadata.
    errors = replay._topology_context_errors(f.config, f.result, output_root=f.child_root)
    assert 'raw_execution_metadata_mismatch' in errors


@lru_cache(maxsize=1)
def _intent_capsule_bytes():
    from test_topology_cycle import capsule_from_registry
    return json.dumps(capsule_from_registry(1)[3].to_dict(), sort_keys=True)


def intent_capsule():
    # One real provider/capsule authoring, detached validated copies per case.
    from r2f.core import ReplayCapsule
    return ReplayCapsule.from_dict(json.loads(_intent_capsule_bytes()))


class BeforeCanonicalRuntime(BaseException):
    """Intercept at the carrier boundary; canonical callable never executes."""


@pytest.mark.parametrize('boundary,kind', [('host', 'canonical'), ('host', 'injected_wrapper'),
                                          ('isolated_netns', 'canonical')])
def test_replay_execution_callsite_binds_intent_without_mutating_capsule(tmp_path, boundary, kind):
    from r2f.campaign import runner
    from r2f.modules.s1.providers import S1Carrier
    from test_topology_cycle import SOURCE
    capsule = intent_capsule()
    original = tmp_path / 'original' / 'capsule.json'; original.parent.mkdir()
    write(original, capsule.to_dict()); before = original.read_bytes()
    canonical = runner.runtime_for_boundary(boundary)
    def wrapper(*args, **kwargs):
        raise AssertionError('injected wrapper must never execute')
    runtime = canonical if kind == 'canonical' else wrapper
    captured = []
    def intercept(self, scenario, prepared, *, execution):
        captured.append(deepcopy(execution['config']))
        raise BeforeCanonicalRuntime()
    output = tmp_path / 'replay'
    with patch.object(replay, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(S1Carrier, 'apply', intercept), pytest.raises(BeforeCanonicalRuntime):
        replay.replay_cell(original, output_dir=output, domain_id=81, runtime=runtime, runtime_boundary=boundary)
    assert len(captured) == 1
    config = captured[0]
    assert config.get('live_execution_go') is (True if kind == 'canonical' else None)
    assert original.read_bytes() == before
    assert 'live_execution_go' not in capsule.input['effective_input']['final_input']
    assert config['final_input'] == capsule.input['effective_input']['final_input']
    assert config['effective_input_sha256'] == capsule.input['effective_input_sha256']
    assert json.loads((output / 'runtime_config.json').read_text()) == config


@pytest.mark.parametrize('boundary', ['host', 'isolated_netns'])
@pytest.mark.parametrize('damage', ['none', 'missing_flag', 'false_flag', 'integer_flag', 'injected_true_flag',
    'formal_only_forgery', 'origin_only_forgery', 'boundary_origin_mismatch', 'semantic_change'])
def test_replay_audit_reconstructs_intent_from_origin_not_config_flag(tmp_path, boundary, damage):
    from r2f.campaign import runner
    from test_topology_cycle import SOURCE
    capsule = intent_capsule()
    before = canonical_hash(capsule.input)
    base = replay._replay_config(capsule, 'synthetic-replay-audit', 81, SOURCE)
    config = runner._runtime_execution_config(base, execute=True,
        runtime=runner.runtime_for_boundary(boundary), runtime_boundary=boundary)
    origin = replay.ISOLATED_RUNTIME if boundary == 'isolated_netns' else replay.DEFAULT_RUNTIME
    report = {'replay_id': 'synthetic-replay-audit', 'domain_id': 81, 'runtime_boundary': boundary,
              'execution_origin': origin, 'formal_runtime': True, 'runtime_invoked': True}
    if damage == 'missing_flag': config.pop('live_execution_go')
    elif damage == 'false_flag': config['live_execution_go'] = False
    elif damage == 'integer_flag': config['live_execution_go'] = 1
    elif damage == 'injected_true_flag': report.update(execution_origin='injected_runtime', formal_runtime=False)
    elif damage == 'formal_only_forgery': report['execution_origin'] = 'injected_runtime'
    elif damage == 'origin_only_forgery': report['formal_runtime'] = False
    elif damage == 'boundary_origin_mismatch':
        report['execution_origin'] = replay.DEFAULT_RUNTIME if boundary == 'isolated_netns' else replay.ISOLATED_RUNTIME
    elif damage == 'semantic_change': config['final_input']['unexpected_input'] = True
    write(tmp_path / 'runtime_config.json', config)
    errors = []
    replay._check_audit_config(tmp_path, capsule, report, errors)
    if damage == 'none': assert errors == []
    else: assert errors, damage
    assert canonical_hash(capsule.input) == before


@pytest.mark.parametrize('origin', [replay.DEFAULT_RUNTIME, 'injected_runtime'])
def test_true_intent_flag_cannot_assert_canonical_origin_or_real_execution(nested, origin):
    result = qualify(nested, origin=origin)
    assert nested.config['live_execution_go'] is True
    assert not result['real_ros_execution'] and not result['real_runtime_attempt']
    assert 'boundary_route_context_mismatch' in result['qualification_errors']
    assert 'topology_execution_intent_origin_mismatch' in result['qualification_errors']


def test_missing_intent_is_rejected_even_with_valid_nested_artifact_audit(nested):
    f = nested
    f.config.pop('live_execution_go')
    f.inner_config.pop('live_execution_go')
    f.request['config'].pop('live_execution_go')
    # Rebuild every declaration/hash rather than relying on a transport mismatch.
    from test_execution_boundary_audit import digest
    f.normalized.pop('live_execution_go')
    f.request['submitted_config_sha256'] = digest(f.normalized)
    f.request['inner_config_sha256'] = digest(f.inner_config)
    f.manifest['identity']['config_sha256'] = digest(f.config)
    f.request['identity'] = deepcopy(f.manifest['identity'])
    f.child_request['config'].pop('live_execution_go')
    f.child_request['config_sha256'] = digest(f.inner_config)
    f.ns_manifest['config_sha256'] = digest(f.inner_config)
    f.raw[1]['config_sha256'] = digest(f.inner_config)
    f.guardian_events[4]['config_sha256'] = digest(f.inner_config)
    f.sync()
    result = qualify(f)
    assert result['boundary_audit']['valid'], result['boundary_audit']['errors']
    assert 'topology_execution_intent_origin_mismatch' in result['qualification_errors']
    assert not result['real_ros_execution']


def test_isolated_injected_wrapper_rejected_before_reserve_carrier_or_invoke(tmp_path):
    from unittest.mock import Mock
    from r2f.modules.s1.providers import S1Carrier
    capsule = intent_capsule()
    original = tmp_path / 'original' / 'capsule.json'; original.parent.mkdir()
    write(original, capsule.to_dict()); before = original.read_bytes()
    wrapper = Mock(side_effect=AssertionError('injected runtime must not execute'))
    output = tmp_path / 'replay'
    with patch.object(S1Carrier, 'apply') as carrier, \
         patch.object(replay, 'source_manifest') as source, \
         pytest.raises(ValueError, match='isolated_netns requires the canonical execution boundary runtime'):
        replay.replay_cell(original, output_dir=output, domain_id=81,
                           runtime=wrapper, runtime_boundary='isolated_netns')
    carrier.assert_not_called(); wrapper.assert_not_called(); source.assert_not_called()
    assert not output.exists()
    assert original.read_bytes() == before


@pytest.mark.parametrize('damage', [
    'foreign_logical_id', 'endpoint_role', 'metadata_alias',
    'raw_worker_alias', 'raw_topology_alias', 'high_alias', 'wrong_ordinal',
])
def test_declared_worker_binding_rejects_role_and_logical_id_forgery(nested, damage):
    """Exercise the real strict matcher, without mocking any lineage gate."""
    f = nested
    for original in f.workers:
        worker = deepcopy(original)
        token = deepcopy(f.raw[worker['token_raw_ref']['event_index']])
        assert replay._topology_worker_role_matches(f.config, worker, token)
        if damage == 'foreign_logical_id':
            worker['logical_id'] = token['logical_id'] = 'foreign_synthetic'
        elif damage == 'endpoint_role':
            worker['endpoint_role'] = 'reader' if worker['endpoint_role'] == 'writer' else 'writer'
        elif damage == 'metadata_alias':
            worker['role'] = 'topology_' + worker['endpoint_role']
        elif damage == 'raw_worker_alias':
            token['worker_role'] = 'topology_' + worker['endpoint_role']
        elif damage == 'raw_topology_alias':
            token['topology_worker_role'] = 'topology_' + worker['endpoint_role']
        else:
            alias = worker['endpoint_role'] + ('99' if damage == 'high_alias' else '4')
            if alias == worker['role']:
                alias = worker['endpoint_role'] + '2'
            worker['role'] = token['role'] = token['worker_role'] = alias
        assert not replay._topology_worker_role_matches(f.config, worker, token), damage
        child = deepcopy(f.child)
        child['events'][worker['token_raw_ref']['event_index']] = token
        index = f.workers.index(original)
        child['execution_metadata']['workers'][index] = worker
        assert 'topology_worker_lineage_invalid:worker_role_mismatch' in (
            replay._topology_worker_errors(f.config, child)), damage


@pytest.mark.parametrize('field', ['worker_role', 'topology_worker_role', 'role'])
def test_rehashed_raw_alias_conflict_still_fails_lineage_qualification(nested, field):
    f = nested
    token = f.raw[f.workers[0]['token_raw_ref']['event_index']]
    token[field] = 'writer99'
    f.sync()  # Recompute refs/hashes: rejection must be semantic, not stale bytes.
    result = qualify(f)
    assert result['boundary_audit']['valid'], result
    assert 'topology_worker_lineage_invalid:worker_role_mismatch' in result['qualification_errors']
    assert 'offline_fixture_cannot_establish_real_ros' in result['qualification_errors']
    assert f.child['evidence_origin'] == 'fixture' and f.child['real_execution'] is False
    assert not result['real_ros_execution'] and not result['real_runtime_attempt']
