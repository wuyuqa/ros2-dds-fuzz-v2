"""G01 revision1 shared cycle, offline raw-backed fixtures only (no ROS)."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core.evidence import canonical_hash
from r2f.runtime import ros2_pair as pair
import test_g01_relation_runtime_wiring as wiring

ROOT = Path(__file__).resolve().parents[1]
TARGET = wiring.TARGET
SOURCE = {'source_repo': str(ROOT), 'head': '0' * 40,
          'behavior_tree_sha256': 'f' * 64, 'source_frozen': False,
          'dirty_behavior_paths': ['offline_fixture']}


def profile(bfs=False):
    doc = json.loads((ROOT / 'config/s1_fuzz/g01-pair.json').read_text())
    if not bfs:
        for key in ('composition', 'search', 'scheduler_policy', 'sampling_kinds'):
            doc.pop(key, None)
        for spec in doc['seed_specs'] + doc['declared_seed_specs_union'] + doc['cycles'][0]['seed_specs']:
            spec.pop('mutation_routes', None)
    return doc


def offline_runtime(*, prohibited_match=False, damage=None):
    """Inject transport only; use real run_pair, persisted raw and canonical ingest."""
    def runtime(config, output_dir):
        pool_type = type('CycleOfflinePool', (wiring.OfflinePool,), {
            'matched': 1 if config['pair_relation'] != 'BR' or prohibited_match else 0,
            'deliver': True})
        sidecar = SimpleNamespace(base_env={}, role_env={}, capture_worker=lambda *a: {},
                                 collect=lambda *a: {'status': 'offline_fixture', 'candidate_count': 0})
        def acquire(c, out, emit, **kwargs):
            owner = {'pid': __import__('os').getpid(), 'campaign_id': c['campaign_id'],
                     'domain_id': c['domain_id'], 'state_dir': str(out)}
            names = ['campaign-' + hashlib.sha256(c['campaign_id'].encode()).hexdigest() + '.lock',
                     'domain-' + str(c['domain_id']) + '.lock',
                     'state-' + hashlib.sha256(str(out).encode()).hexdigest() + '.lock']
            emit('isolation_locks_acquired', locks=[{'path': '/offline/' + name, 'owner': owner,
                'acquired': True, 'offline_fixture': True} for name in names])
            emit('environment_preflight', ok=True, environment={'ok': True, 'offline_fixture': True})
            return SimpleNamespace(__exit__=lambda *a: None), {'ok': True, 'offline_fixture': True}, {}
        # Zero waits are test-only observation clocks, not changes to the plan/config.
        with patch.object(pair, 'RoleRmwWorkerPool', pool_type), patch.object(pair, 'WorkerPool', pool_type), \
                patch.object(pair, 'acquire_pair_environment', side_effect=acquire), \
                patch('r2f.runtime.instrumentation_bridge.prepare', return_value=sidecar), \
                patch('r2f.runtime.namespace_pair.execution_chain_identity', return_value={'offline': True}), \
                patch('r2f.runtime.namespace_pair.same_source', return_value=True):
            result = pair.run_pair(config, output_dir)
        if damage == 'missing_raw':
            Path(result['raw_path']).unlink()
        elif damage == 'forged_summary':
            result['relation_guard_facts'] = {'incompatible': True, 'identity_backed_delivery': True}
        elif damage == 'tamper':
            with Path(result['raw_path']).open('a') as stream:
                stream.write('{}\n')
        return result
    return runtime


def run(tmp_path, *, bfs=False, seed=1, execute=False, prohibited_match=False, damage=None, maximum=4, initial_pair=None, aggregate=None, source=None):
    doc = profile(bfs)
    # Observation timing is an explicit input override before hashing, never a post-plan patch.
    overrides = {'match_timeout_sec': 0.0, 'publish_interval_sec': 0.0,
                 'delivery_timeout_sec': 0.0}
    if initial_pair is not None:
        policies = {'R': 'reliable', 'B': 'best_effort'}
        template = json.loads((ROOT / 'config/s1_fuzz/seeds/g01.json').read_text())['logical_config']
        overrides.update(pair_relation=initial_pair)
        for role, letter in zip(('writer', 'reader'), initial_pair):
            overrides[role + '_qos'] = {**template[role + '_qos'], 'reliability': policies[letter]}
    for spec in doc['seed_specs'] + doc['declared_seed_specs_union'] + doc['cycles'][0]['seed_specs']:
        spec['overrides'] = {'logical_config': overrides}
    corpus = corpus_from_profile(doc, cycle_id='g01-pair')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=seed, policy='bfs' if bfs else 'fair')
    with patch('r2f.campaign.runner.source_manifest', return_value=deepcopy(source or SOURCE)):
        result = run_exploratory_cycle(tmp_path, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=doc, runtime=offline_runtime(prohibited_match=prohibited_match, damage=damage),
            execute=execute, max_inputs=maximum, run_id=tmp_path.name, feedback_aggregate=aggregate)
    return result


def test_exact_revision1_generation_prepare_binding_and_report(tmp_path):
    cycle = run(tmp_path / 'planned')
    assert not cycle['abort']
    rows = [r for r in cycle['rows'] if r.get('mutation_plan')]
    assert rows
    for row in rows:
        cfg = json.loads(Path(row['runtime_config_path']).read_text())
        assert row['target_revision'] == 1
        assert cfg['final_input'] == row['mutation_plan']['final_config']
        assert cfg['final_input'] == row['final_input']
        assert row['binding']['status'] == 'preserved'
        assert row['actual_run'] is False
    report = json.loads((tmp_path / 'planned/report.json').read_text())
    assert report['audit']['rows_verified'] == len(cycle['rows']), report['audit']
    assert report['audit']['errors'] == ['exploratory_source_not_frozen']
    assert report['audit']['sealed'] is False


def test_shared_cycle_raw_guard_success_and_unknown_not_candidates(tmp_path):
    cycle = run(tmp_path / 'observed', execute=True, maximum=12)
    assert not cycle['abort']
    actual = [r for r in cycle['rows'] if r.get('actual_run')]
    assert actual, cycle['rows']
    assert any(r['oracle_verdict'] == 'pass' for r in actual), actual
    assert all(not r.get('candidate_eligible') for r in actual)
    for row in actual:
        assert Path(row['raw_path']).is_file()
        assert row['rule_id'] == 's1.discovery.compatibility_guard.v1'


def test_all_pair_proposals_and_noop_are_explicit_without_rewriting_input(tmp_path):
    cycle = run(tmp_path / 'four-pair-proposals', execute=True, maximum=12)
    accounted = set()
    for row in cycle['rows']:
        proposal = row.get('mutation_plan') or row.get('rejected_mutation_plan')
        assert proposal
        kind = proposal['final_config']['pair_relation']
        accounted.add(kind)
        if kind == 'BR':  # the authored seed is BR: do not rewrite it to make hit
            assert row['reason_code'] == 'no_effective_input'
            assert row['actual_run'] is False
            assert row['mutation_plan'] is None
            assert row.get('mutation_receipt', {}).get('hit', False) is False
        else:
            assert row['oracle_verdict'] == 'pass'
            assert row['mutation_receipt']['hit'] is True
    assert accounted == {'RR', 'RB', 'BB', 'BR'}
    report = json.loads((tmp_path / 'four-pair-proposals/report.json').read_text())
    assert report['audit']['sealed'] is False
    assert not any('generation_binding_missing' in error for error in report['audit']['errors'])


@pytest.mark.parametrize('prohibited_match,expected', [(False, 'unknown'), (True, 'fail')])
def test_br_raw_observation_is_separate_from_prediction_in_shared_cycle(tmp_path, prohibited_match, expected):
    cycle = run(tmp_path / ('br-' + expected), bfs=True, execute=True, maximum=16,
                initial_pair='RR', prohibited_match=prohibited_match)
    rows = [r for r in cycle['rows'] if r.get('actual_run') and r.get('final_input', {}).get('pair_relation') == 'BR']
    assert rows
    for row in rows:
        assert row['oracle_verdict'] == expected, row
        assert row['mutation_receipt']['hit'] is True
        assert row['mutation_receipt']['effect_observed'] is prohibited_match
        assert row['execution_stage'] == ('judged' if prohibited_match else 'mutation_applied')
        assert row['candidate_eligible'] is False  # synthetic fixture, not a SUT candidate
        assert Path(row['raw_path']).is_file()


@pytest.mark.parametrize('damage', ['missing_raw', 'tamper', 'forged_summary'])
def test_cycle_never_trusts_missing_tampered_or_runtime_returned_facts(tmp_path, damage):
    cycle = run(tmp_path / damage, execute=True, maximum=1, damage=damage)
    row = cycle['rows'][0]
    assert not row.get('candidate_eligible')
    if damage == 'forged_summary':
        # The persisted runtime result and returned evidence must agree before
        # ingest. A forged return is a tool failure, even when raw is healthy.
        assert row['oracle_verdict'] == 'unknown'
        assert row['execution_stage'] == 'aborted'
        assert row['abort_reason'] == 'runner_wrapper_failure'
        assert row['runner_exception']['detail'] == 'runtime_result_differs_from_returned_evidence'
        saved = json.loads(Path(row['runtime_result_path']).read_text())
        assert saved.get('relation_guard_facts') != {
            'incompatible': True, 'identity_backed_delivery': True}
        raw = Path(saved['raw_path'])
        assert hashlib.sha256(raw.read_bytes()).hexdigest() == saved['raw_sha256']
    else:
        assert row['oracle_verdict'] != 'pass'
        assert row['execution_stage'] == 'aborted'
        assert row.get('ingestion_error') or row.get('abort_reason')


def test_exact_profile_does_not_admit_other_targets_or_f2_surfaces():
    from r2f.campaign.exploratory_input import validate_profile_fields
    doc = profile()
    doc['surfaces'] = ['G01', 'E02']
    with pytest.raises(ValueError, match='g01_requires_exact'):
        validate_profile_fields(doc)
    doc = profile()
    doc['target_ids'] = ['discovery.late_endpoint.fuzz_v1']
    doc['cycles'][0]['target_id'] = doc['target_ids'][0]
    for spec in doc['seed_specs'] + doc['cycles'][0]['seed_specs']:
        spec['target_id'] = doc['target_ids'][0]
    with pytest.raises(ValueError, match='capability pending'):
        corpus_from_profile(doc, cycle_id='g01-pair')


def test_old_revision_capsule_cannot_qualify_as_new_target(tmp_path):
    from r2f.campaign.replay import _registered_target
    from r2f.core.replay import ReplayCapsule
    cycle = run(tmp_path / 'old-capsule', maximum=1)
    capsule = ReplayCapsule.from_dict(json.loads(Path(cycle['rows'][0]['replay_path']).read_text()))
    target, _, _ = _registered_target(capsule)
    assert target.target_revision == 1
    stale = capsule.to_dict()
    # Exact-manifest gate must reject v0 even if the capsule was otherwise well formed.
    for manifest in (stale['input']['target'], stale['input']['scenario']['target']):
        manifest['target_revision'] = 0
    # Deliberately use the gate directly: capsule parsing has additional hash guards.
    capsule.input = stale['input']
    with pytest.raises(Exception, match='target/revision differs'):
        _registered_target(capsule)


def test_bfs_enumerates_tasks_four_pairs_noop_and_successors(tmp_path):
    cycle = run(tmp_path / 'bfs', bfs=True, execute=True, maximum=16)
    assert not cycle['abort'], cycle
    assert cycle['search_summary']['mode'] == 'input_search_bfs'
    rows = cycle['rows']
    assert all(row.get('input_search', {}).get('task_id') for row in rows)
    pairs = set()
    for row in rows:
        proposal = row.get('mutation_plan') or row.get('rejected_mutation_plan')
        assert proposal, row
        pairs.add(proposal['final_config']['pair_relation'])
        assert proposal['final_config']['pair_relation'] == ('RR', 'RB', 'BB', 'BR')[
            row['input_search']['sample_slot']]
        if row.get('mutation_plan'):
            cfg = json.loads(Path(row['runtime_config_path']).read_text())
            assert cfg['final_input'] == proposal['final_config'] == row['final_input']
            assert row['binding']['status'] == 'preserved'
        else:
            assert row['reason_code'] == 'no_effective_input'
            assert not row.get('actual_run')
            assert not row.get('mutation_receipt', {}).get('hit')
    assert pairs == {'RR', 'RB', 'BB', 'BR'}
    assert any(row['input_search']['derivation_depth'] > 1 for row in rows)
    # Successors are task-ledger work, not novelty-corpus admissions.
    rejected_novelty = {row['content_sha256'] for row in rows
                        if row.get('corpus_offer', {}).get('admitted') is False}
    assert rejected_novelty
    assert any(row.get('parent_content_sha256') in rejected_novelty
               and row['input_search']['derivation_depth'] > 1 for row in rows)


def test_replay_config_exact_gate_preserves_saved_input(tmp_path):
    from r2f.campaign.replay import _replay_config, _registered_target
    from r2f.core.replay import ReplayCapsule
    cycle = run(tmp_path / 'replay-config', bfs=True, maximum=1)
    row = cycle['rows'][0]
    capsule = ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
    _registered_target(capsule)
    config = _replay_config(capsule, 'offline-replay', 172, SOURCE)
    assert config['final_input'] == capsule.input['effective_input']['final_input']
    assert config['input_layer_receipts'] == capsule.input['effective_input']['input_layer_receipts']
    assert config['effective_input_sha256'] == capsule.input['effective_input_sha256']
    assert config['target_revision'] == 1


def test_declared_capability_is_not_live_qualification():
    cap = pair.exploratory_runtime_capabilities()[TARGET]
    assert cap['implemented'] is True and cap['runner_executable'] is True
    assert cap['runtime_implemented'] is True and cap['target_revision'] == 1
    assert cap['live_qualified'] is False


def test_exact_g01_terminal_aggregate_uses_explicit_compatibility_rule(tmp_path):
    path = tmp_path / 'aggregate.json'
    # Synthetic frozen identity exercises persistence gates; checkout remains dirty.
    frozen_fixture = dict(SOURCE, source_frozen=True, dirty_behavior_paths=[])
    cycle = run(tmp_path / 'aggregated', bfs=True, execute=True, maximum=4,
                aggregate=path, source=frozen_fixture)
    receipt = cycle['feedback_aggregate']
    assert receipt['status'] == 'merged', receipt
    assert receipt['compatibility']['rule_id'] == 's1.discovery.compatibility_guard.v1'
    assert path.is_file()


def test_foreign_m08_and_revision0_routes_and_sampling_are_refused():
    from r2f.campaign.mutation_routes import validate_route_entry
    from r2f.core.mutators import MUTATORS
    assert 'M08' not in MUTATORS
    entry = corpus_from_profile(profile(bfs=True), cycle_id='g01-pair').selectable()[0]
    validate_route_entry(entry)
    stale = deepcopy(entry)
    stale['target_revision'] = 0
    stale.pop('mutation_route_origins', None)  # isolate authoritative revision gate
    with pytest.raises(ValueError, match='revision/contract'):
        validate_route_entry(stale)
    assert ExploratoryScheduler._supported_sampling_kinds(dict(stale, mutator_id='M08')) == []
    other = json.loads((ROOT / 'config/s1_fuzz/profiles/common-baseline-routes.json').read_text())
    foreign = corpus_from_profile(other, cycle_id='common-baseline').selectable()[0]
    for route in foreign['mutation_routes']:
        route['mutator_id'] = 'M08'
    foreign.pop('mutation_route_origins', None)  # bind edited declaration to itself
    with pytest.raises(ValueError, match='mutator not in implemented'):
        validate_route_entry(foreign)
    assert ExploratoryScheduler._supported_sampling_kinds(dict(foreign, mutator_id='M08')) == []


def test_aggregate_dirty_source_and_foreign_compatibility_alias_stay_refused(tmp_path):
    from r2f.campaign.runner import _terminal_feedback_aggregate
    cycle = run(tmp_path / 'dirty-aggregate', maximum=1, aggregate=tmp_path / 'dirty.json')
    assert cycle['feedback_aggregate']['status'] == 'error'
    assert 'requires frozen source' in cycle['feedback_aggregate']['detail']
    frozen = dict(SOURCE, source_frozen=True, dirty_behavior_paths=[])
    foreign = 'discovery.late_endpoint.fuzz_v1'
    bad = dict(cycle, source_manifest=frozen, profile={'target_ids': [foreign]})
    registry = SimpleNamespace(get_target=lambda key: SimpleNamespace(target_revision=1,
        rule_contract={'compatibility_rule': 's1.discovery.compatibility_guard.v1'}))
    with patch('r2f.campaign.runner.source_manifest', return_value=frozen):
        receipt = _terminal_feedback_aggregate(tmp_path / 'foreign.json', bad, registry,
                    output_dir=tmp_path / 'dirty-aggregate', repo=ROOT)
    assert receipt['status'] == 'error'
    assert 'explicit primary rule' in receipt['detail']


def test_replay_raw_tamper_stays_rejected_before_runtime(tmp_path):
    from r2f.campaign.replay import replay_cell
    cycle = run(tmp_path / 'original', bfs=True, execute=True, maximum=1)
    row = cycle['rows'][0]
    raw = Path(row['raw_path'])
    raw.write_text(raw.read_text() + '{}\n')  # only this disposable fixture
    def forbidden(*args):
        pytest.fail('replay raw gate must reject before calling runtime')
    result = replay_cell(row['replay_path'], output_dir=tmp_path / 'replay',
                         domain_id=172, runtime=forbidden)
    assert result['reason_code'] == 'original_artifact_invalid', result
    assert result['failure_reason'] == 'ValueError: raw SHA256 mismatch'
    assert result['real_ros_execution'] is not True


def test_cli_explicit_enabled_g01_copy_plans_through_shared_bfs(tmp_path, monkeypatch, capsys):
    import importlib.util
    import sys
    spec = importlib.util.spec_from_file_location('g01_offline_cli', ROOT / 'scripts/run_s1.py')
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    doc = profile(bfs=True)
    doc['enabled'] = True  # separate copy, authored profile stays disabled
    path = tmp_path / 'g01-enabled.json'
    path.write_text(json.dumps(doc))
    monkeypatch.setattr(sys, 'argv', ['run_s1.py', '--exploratory-profile', str(path),
        '--cycle-id', 'g01-pair', '--scheduler-policy', 'bfs', '--max-inputs', '4',
        '--seed', '1', '--run-id', 'g01-cli-offline', '--output-dir', str(tmp_path / 'cli')])
    cli.main()  # no --execute, never selects or starts a runtime
    result = json.loads(capsys.readouterr().out)
    cycle = json.loads(Path(result['cycle_path']).read_text())
    assert cycle['execute'] is False and cycle['search_summary']['proposal_generated'] == 4
    assert all(not row.get('actual_run') for row in cycle['rows'])
    assert len({(row.get('mutation_plan') or row['rejected_mutation_plan'])['final_config']['pair_relation']
                for row in cycle['rows']}) == 4
    foreign = json.loads((ROOT / 'config/s1_fuzz/profiles/common-baseline-routes.json').read_text())
    foreign['enabled'] = True
    with pytest.raises(ValueError, match='must stay enabled=false'):
        corpus_from_profile(foreign, cycle_id='common-baseline')


def test_isolated_g01_formal_identity_gate_is_offline_compatible(tmp_path):
    from r2f.campaign.runner import _boundary_execution_config
    from r2f.runtime.execution_boundary import _formal_identity
    cycle = run(tmp_path / 'isolated-input', bfs=True, maximum=1,
                source=dict(SOURCE, source_frozen=True, dirty_behavior_paths=[]))
    row = cycle['rows'][0]
    saved = json.loads(Path(row['runtime_config_path']).read_text())
    submitted = _boundary_execution_config(saved, 'isolated_netns')
    assert submitted['execution_boundary']['enabled'] is True
    assert submitted['final_input'] == saved['final_input']
    with patch('r2f.runtime.execution_boundary._same_source', return_value=saved['source_manifest']):
        identity = _formal_identity(submitted)  # no namespace/process/lease operation
    assert identity['effective_input_sha256'] == saved['effective_input_sha256']
    assert identity['target_sha256'] == canonical_hash(saved['scenario']['target'])
    assert not row.get('actual_run')
