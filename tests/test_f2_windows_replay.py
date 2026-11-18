"""F2 saved-plan replay contracts; injected callbacks never qualify real ROS.

The source-freeze observation is explicitly simulated around the same source
manifest. No DDS context, campaign, netns, socket or runtime process is started.
Synthetic execution metadata below tests failure retention, not actual evidence.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.exploratory_input import F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET
from r2f.campaign.identity import source_manifest
from r2f.campaign.replay import (
    REPO, _bind_execution_input, _registered_target, _replay_config, audit_replay, replay_cell,
)
from r2f.campaign.runner import _adapt_parent, _compose_recovery_inputs, _route
from r2f.core import Registry, ReplayCapsule, Scenario, canonical_hash, raw_sha256
from r2f.core.evidence import INPUT_SCHEMA_V2
from r2f.core.execution import ExecutionRecord
from r2f.core.mutation_plan import compose
from r2f.core.mutators import make_rng, mutate_f2
from r2f.modules.s1.module import register_exploratory


def bytes_under(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}


@pytest.fixture
def saved(tmp_path):
    source = source_manifest(REPO)
    # This is a unit-test source observation, not a claim that dirty HEAD is frozen.
    source.update(source_frozen=True, dirty_behavior_paths=[])
    with patch('r2f.campaign.replay.source_manifest', return_value=deepcopy(source)):
        def create(sid='E03', count=3, anchor='previous_restore_confirmed'):
            profile = json.loads((REPO / 'config/s1_fuzz/profiles/f2-windows.json').read_text())
            cycle = next(c for c in profile['cycles'] if c['surfaces'] == [sid])
            cycle['seed_specs'] = [deepcopy(cycle['seed_specs'][0])]
            cycle['seed_specs'][0]['overrides']['logical_config']['environment_windows']['count'] = count
            corpus = corpus_from_profile(profile, cycle_id=cycle['cycle_id'])
            entry = next(iter(corpus.entries.values()))
            registry = Registry()
            register_exploratory(registry, runtime_capability=F2_WINDOWS_CAPABILITY)
            target = registry.get_target(F2_WINDOWS_TARGET)
            contract = target.exploration_contract
            parent, _ = _adapt_parent(target.target_id, _route(target.target_id), entry['final_config'])
            context = ({'window_count': count, 'restore_interval_sec': 0.2} if sid == 'E02'
                       else {'anchor': anchor, 'offset_sec': 0.25})
            proposal = mutate_f2(parent, contract.surface_by_id[sid], make_rng(7), context,
                                 'M07' if sid == 'E02' else 'M06')
            proposals, windows, _, _ = _compose_recovery_inputs(parent, [proposal])
            plan = compose(parent, proposals, contract, rng_seed=7, runtime_windows=windows)
            scenario = Scenario(target, entry['rmw'], 'mutated', 0, 7,
                {'mutation_kind': 'discovery_drop', 'carrier': 'wire_proxy',
                 'mutation_parameters': plan.final_config}, plan)
            prepared = registry.get_services(target.target_id).mutation_provider.prepare_plan(scenario, plan)
            capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source,
                final_input=plan.final_config, input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
            origin = tmp_path / 'original'
            origin.mkdir()
            path = origin / 'capsule.json'
            path.write_text(json.dumps(capsule.to_dict()))
            (origin / 'runtime_config.json').write_text(json.dumps({
                'profile_path': '/tmp/absent-f2-profile.json', 'runtime_capability': 'untrusted',
                'environment_windows': [], 'anchor_lateness_tolerance_sec': 999}))
            return capsule, path, source
        yield create


def run_replay(path, output, callback):
    original = bytes_under(path.parent)
    report = replay_cell(path, output_dir=output, domain_id=81, runtime=callback)
    assert bytes_under(path.parent) == original
    audit = audit_replay(report['report_path'])
    assert audit['valid'], audit
    assert not report['real_ros_execution'] and not report['real_runtime_attempt']
    assert not audit['real_ros_execution']
    assert not report['maturity_denominator_changed']
    return report


@pytest.mark.parametrize('sid,count,anchor', [
    ('E02', 1, 'fresh_match'), ('E02', 3, 'fresh_match'),
    ('E03', 1, 'fresh_match'), ('E03', 3, 'fresh_match'),
    ('E03', 2, 'previous_restore_confirmed'), ('E03', 3, 'previous_restore_confirmed'),
])
def test_same_source_plan_only_capsule_rebuilds_exact_input_before_retained_tool_failure(tmp_path, saved, sid, count, anchor):
    capsule, path, source = saved(sid, count, anchor)
    snapshot = deepcopy(capsule.to_dict())
    config = _replay_config(capsule, 'offline-roundtrip', 81, source)
    final = capsule.input['effective_input']['final_input']
    assert config['final_input'] == final
    assert config['mutation_plan'] == capsule.input['mutation_plan']
    assert config['effective_input_sha256'] == capsule.input['effective_input_sha256']
    assert config['input_identity_schema'] == INPUT_SCHEMA_V2
    assert config['domain_id'] == 81 and capsule.input['origin_domain'] == 80
    assert config['runtime_capability'] == F2_WINDOWS_CAPABILITY
    assert 'profile_path' not in config
    for key, value in final.items():
        assert config[key] == value
    if sid == 'E03':
        assert config['submitted_parameters']['environment_windows.anchor'] == anchor
        indices = [i for i, w in enumerate(final['environment_windows']) if w['trigger_anchor'] == anchor]
        assert indices == ([0] if anchor == 'fresh_match' else list(range(1, count)))
        assert all(final['environment_windows'][i]['trigger_offset_sec'] == 0.25 for i in indices)
    calls = []
    def tool_error(actual, directory):
        calls.append(deepcopy(actual))
        raise RuntimeError('offline_fixture_no_ROS')
    with patch('r2f.core.mutators.mutate_f2', side_effect=AssertionError('no regeneration')):
        report = run_replay(path, tmp_path / 'replayed', tool_error)
    assert len(calls) == 1
    assert calls[0]['effective_input'] == capsule.input['effective_input']
    assert report['status'] == 'fail_environment'
    assert report['reason_code'] == 'runtime_tool_failure'
    assert report['runtime_invoked']
    assert capsule.to_dict() == snapshot
    derived = ReplayCapsule.from_dict(json.loads((tmp_path / 'replayed/derived_capsule.json').read_text()))
    assert derived.input == capsule.input and derived.execution is None
    assert len(derived.replay_attempts) == 1
    assert derived.replay_attempts[0]['status'] == report['status']


def author_target(capsule, change):
    scenario = capsule.input['scenario']
    data = deepcopy(scenario)
    change(data['target'])
    updated = Scenario.from_dict(data)
    return ReplayCapsule.from_scenario(updated, source_manifest=capsule.input['source_manifest'],
        final_input=updated.mutation_plan.final_config,
        input_layer_receipts=capsule.input['effective_input']['input_layer_receipts'], origin_domain=80)


@pytest.mark.parametrize('damage', ['missing_capability', 'unknown_capability', 'target_prefix', 'revision', 'contract'])
def test_rehashed_foreign_target_manifest_never_infers_capability_or_migrates(tmp_path, saved, damage):
    capsule, path, _ = saved()
    def change(target):
        if damage == 'missing_capability': target['observation_contract'].pop('runtime_capability')
        elif damage == 'unknown_capability': target['observation_contract']['runtime_capability'] = 'f2_all'
        elif damage == 'target_prefix': target['target_id'] += '_unknown'
        elif damage == 'revision': target['target_revision'] += 1
        else: target['observation_contract']['anchor_timing_policy'] = 'relaxed'
    foreign = author_target(capsule, change)
    # Self-consistent hashes are necessary, never sufficient for registration.
    ReplayCapsule.from_dict(foreign.to_dict())
    path.write_text(json.dumps(foreign.to_dict()))
    callback = Mock(side_effect=AssertionError('invalid identity cannot submit'))
    report = run_replay(path, tmp_path / 'refused', callback)
    callback.assert_not_called()
    assert report['status'] == 'fail_artifact'
    assert report['reason_code'] == 'target_contract_mismatch'


@pytest.mark.parametrize('damage', ['source', 'not_frozen', 'domain', 'hash', 'runtime_windows'])
def test_existing_source_domain_hash_and_structural_gates_remain_before_callback(tmp_path, saved, damage):
    capsule, path, source = saved()
    if damage == 'source':
        other = dict(source, behavior_tree_sha256='0' * 64)
        patcher = patch('r2f.campaign.replay.source_manifest', return_value=other)
    elif damage == 'not_frozen':
        patcher = patch('r2f.campaign.replay.source_manifest', return_value=dict(source, source_frozen=False))
    else:
        patcher = patch('r2f.campaign.replay.source_manifest', return_value=source)
    if damage == 'domain':
        capsule.input['origin_domain'] = 81
    elif damage == 'hash':
        capsule.input['effective_input_sha256'] = '0' * 64
    elif damage == 'runtime_windows':
        capsule.input['runtime_windows'][0]['trigger_offset_sec'] += 0.1
    path.write_text(json.dumps(capsule.to_dict()))
    callback = Mock(side_effect=AssertionError('preflight cannot submit'))
    with patcher:
        report = run_replay(path, tmp_path / 'refused', callback)
    callback.assert_not_called()
    assert report['status'] == ('fail_environment' if damage in ('source', 'not_frozen', 'domain') else 'fail_artifact')


def test_synthetic_actual_abort_baseline_and_raw_survive_failed_replay(tmp_path, saved):
    capsule, path, source = saved()
    scenario = Scenario.from_dict(capsule.input['scenario'])
    config = _replay_config(capsule, 'offline-original', 80, source)
    raw = path.parent / 'raw.jsonl'
    raw.write_text(json.dumps({'event': 'runtime_end', 'evidence_origin': 'fixture',
                               'reason': 'offline_fixture_interrupted'}) + '\n')
    record = ExecutionRecord('offline-original', scenario)
    record.abort('offline_fixture_interrupted')
    row = dict(record.to_dict(), actual_run=True, execution_source='ros2_public_api',
        source_manifest=source, effective_input_sha256=capsule.input['effective_input_sha256'],
        raw_path=str(raw), raw_sha256=raw_sha256(raw), evidence_origin='fixture', domain=80, domain_id=80)
    _bind_execution_input(row, config, capsule)
    capsule.record_execution(row)
    path.write_text(json.dumps(capsule.to_dict()))
    original = deepcopy(capsule.to_dict())
    report = run_replay(path, tmp_path / 'failed', Mock(side_effect=RuntimeError('fixture failure')))
    assert report['status'] == 'fail_environment'
    derived = ReplayCapsule.from_dict(json.loads((tmp_path / 'failed/derived_capsule.json').read_text()))
    assert derived.execution == original['execution']
    assert derived.execution_history == original['execution_history']
    assert derived.input == original['input']
    assert len(derived.replay_attempts) == 1
    # A damaged original actual raw must also stop before invoking a callback.
    raw.write_text(raw.read_text() + '{}\n')
    callback = Mock()
    report = run_replay(path, tmp_path / 'damaged-raw', callback)
    callback.assert_not_called()
    assert report['reason_code'] == 'original_raw_invalid'


def test_blocked_f2_attempt_preserves_new_raw_without_creating_execution(tmp_path, saved):
    capsule, path, _ = saved()
    def blocked(config, directory):
        raw = directory / 'raw.jsonl'
        event = {'event': 'runtime_end', 'event_index': 0, 'run_id': config['run_id'],
                 'domain_id': config['domain_id'], 'real_execution': False,
                 'reason': 'offline_fixture_domain_conflict'}
        raw.write_text(json.dumps(event) + '\n')
        return {'real_execution': False, 'runtime_status': 'blocked', 'reason': event['reason'],
                'events': [event], 'raw_path': str(raw), 'raw_sha256': raw_sha256(raw),
                'evidence_origin': 'fixture', 'environment': {'ok': False}}
    report = run_replay(path, tmp_path / 'blocked', blocked)
    assert report['status'] == 'fail_environment' and report['runtime_invoked']
    assert report['reason_code'] == 'runtime_preflight_blocked'
    returned = json.loads((tmp_path / 'blocked/runtime_returned.json').read_text())
    assert returned['raw_sha256'] == raw_sha256(tmp_path / 'blocked/runtime/raw.jsonl')
    row = json.loads((tmp_path / 'blocked/execution_metadata.json').read_text())
    assert row['environment_raw_evidence']['verified']
    derived = ReplayCapsule.from_dict(json.loads((tmp_path / 'blocked/derived_capsule.json').read_text()))
    assert derived.input == capsule.input
    assert derived.execution is None and derived.execution_history == []
    assert len(derived.replay_attempts) == 1


def test_saved_capability_does_not_replace_current_route_admission(tmp_path, saved):
    from r2f.runtime.ros2_pair import exploratory_runtime_capabilities
    _, path, _ = saved()
    routes = exploratory_runtime_capabilities()
    routes[F2_WINDOWS_TARGET].pop('runtime_capability')
    callback = Mock(side_effect=AssertionError('lost route cannot submit'))
    with patch('r2f.runtime.ros2_pair.exploratory_runtime_capabilities', return_value=routes):
        report = run_replay(path, tmp_path / 'unsupported', callback)
    callback.assert_not_called()
    assert report['status'] == 'fail_artifact'
    assert report['reason_code'] == 'exploratory_plan_not_routable'
