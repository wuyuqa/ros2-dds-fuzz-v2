"""Authored bounded episode input/planner checks, never ROS or execution proof."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.core import canonical_hash
from r2f.modules.s1 import fuzz_mutators_v2 as v2, topology_model

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / 'config/s1_fuzz/profiles/model-bfs-topology-continuous-episode.json'


def test_episode_seed_is_an_explicit_opt_in_not_execution_evidence():
    program = v2.continuous_episode_program()
    validation = topology_model.validate_program(program)
    assert validation['status'] == 'valid'
    assert validation['execution_evidence'] is False
    assert validation['candidate_eligible'] is False
    assert program['episode'] == {
        'schema': 'r2f.s1-topology-episode-config/v1', 'max_seconds': 120.0,
        'checkpoint_action_ids': ['checkpoint_1', 'checkpoint_2'],
        'reset_action_ids': ['recreate_w2', 'recreate_r2'],
    }
    legacy = v2.corpus_v2()
    assert canonical_hash(legacy) == '7c420c8cc5425828bb3e15b3a7655904d2ba069fb6aa0059ff99db787f883ecd'
    opted = v2.corpus_v2(include_episode=True)
    entries = [e for e in opted['entries'] if e['seed_id'] == v2.CONTINUOUS_EPISODE_SEED]
    assert len(entries) == 1 and entries[0]['document'] == program
    assert entries[0]['execution_evidence'] is False
    assert entries[0]['candidate_eligible'] is False
    assert entries[0]['provenance']['derived_from_execution'] is False
    assert [e for e in opted['entries'] if e['seed_id'] != v2.CONTINUOUS_EPISODE_SEED] == legacy['entries']
    assert [e for e in opted['manifest'] if e['seed_id'] != v2.CONTINUOUS_EPISODE_SEED] == legacy['manifest']


def test_seed_has_two_serial_generations_and_explicit_finite_budgets():
    program = v2.continuous_episode_program()
    assert len(program['endpoints']) == 2
    assert {e['logical_id']: e['role'] for e in program['endpoints']} == {'w0': 'writer', 'r0': 'reader'}
    assert all(e['qos']['reliability'] == 'reliable' and e['qos']['history'] == 'keep_last'
               and e['qos']['depth'] == 16 for e in program['endpoints'])
    assert len({e['topic'] for e in program['endpoints']}) == 1
    assert program['budget']['max_actions'] == 50
    assert program['budget']['max_total_samples'] == 6
    actions = program['actions']
    assert [(a['op'], a['generation']) for a in actions] == [
        ('create', 1), ('create', 1), ('barrier', None), ('publish_batch', 1),
        ('barrier', None), ('snapshot', None), ('destroy', 1), ('destroy', 1),
        ('recreate', 2), ('recreate', 2), ('barrier', None), ('publish_batch', 2),
        ('barrier', None), ('snapshot', None)]
    assert [a['params']['condition'] for a in actions if a['op'] == 'barrier'] == [
        'matched', 'delivery', 'matched', 'delivery']
    assert all(a['params']['timeout_sec'] == 5 for a in actions if a['op'] == 'barrier')
    assert all(a['params']['count'] == 3 and a['params']['start_sequence'] == 0
               for a in actions if a['op'] == 'publish_batch')
    assert not any('concurrency_group' in a['params'] for a in actions)
    assert all(b['depends_on'] == [a['action_id']] for a, b in zip(actions, actions[1:]))
    assert [s['planned_sequences'] for s in topology_model.validate_program(program)['delivery_streams']] == [[0, 1, 2], [0, 1, 2]]


@pytest.mark.parametrize('seconds', [False, True, None, '120', 0, -1, 180.01,
                                     float('nan'), float('inf'), float('-inf')])
def test_episode_requires_finite_positive_nonbool_time(seconds):
    program = v2.continuous_episode_program()
    program['episode']['max_seconds'] = seconds
    assert topology_model.validate_program(program)['status'] == 'invalid'


@pytest.mark.parametrize('seconds', [0.001, 1, 180])
def test_episode_time_inclusive_upper_boundary(seconds):
    program = v2.continuous_episode_program()
    program['episode']['max_seconds'] = seconds
    assert topology_model.validate_program(program)['status'] == 'valid'


@pytest.mark.parametrize('config', [None, {}, [],
    {'schema': 'wrong', 'max_seconds': 120, 'checkpoint_action_ids': ['checkpoint_1']},
    {'schema': 'r2f.s1-topology-episode-config/v1', 'max_seconds': 120},
    {'schema': 'r2f.s1-topology-episode-config/v1', 'max_seconds': 120,
     'checkpoint_action_ids': ['checkpoint_1'], 'observed': True}])
def test_episode_shape_is_exact_and_null_is_not_omission(config):
    program = v2.continuous_episode_program()
    program['episode'] = deepcopy(config)
    assert topology_model.validate_program(program)['status'] == 'invalid'


@pytest.mark.parametrize('field,refs', [
    ('checkpoint_action_ids', []), ('checkpoint_action_ids', None),
    ('checkpoint_action_ids', 'checkpoint_1'), ('checkpoint_action_ids', ['missing']),
    ('checkpoint_action_ids', ['publish_1']), ('checkpoint_action_ids', ['checkpoint_1', 'checkpoint_1']),
    ('checkpoint_action_ids', [True]), ('checkpoint_action_ids', [{}]),
    ('reset_action_ids', None), ('reset_action_ids', ['missing']),
    ('reset_action_ids', ['create_w1']), ('reset_action_ids', ['checkpoint_2']),
    ('reset_action_ids', ['recreate_w2', 'recreate_w2']), ('reset_action_ids', [{}]),
])
def test_episode_references_are_unique_known_and_typed(field, refs):
    program = v2.continuous_episode_program()
    program['episode'][field] = deepcopy(refs)
    result = topology_model.validate_program(program)
    assert result['status'] == 'invalid'
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('reset', [None, []])
def test_reset_ids_are_optional_and_do_not_certify_reset(reset):
    program = v2.continuous_episode_program()
    if reset is None:
        program['episode'].pop('reset_action_ids')
    else:
        program['episode']['reset_action_ids'] = reset
    result = topology_model.validate_program(program)
    assert result['status'] == 'valid' and result['execution_evidence'] is False
    assert all(row['prediction_only'] is True for row in result['actions'])


def test_episode_action_ceiling_and_snapshot_insert_refusal_preserve_refs():
    program = v2.continuous_episode_program()
    program['budget']['max_actions'] = 60
    while len(program['actions']) < 50:
        program['actions'].append(topology_model._action('extra_' + str(len(program['actions'])), 'snapshot'))
    assert topology_model.validate_program(program)['status'] == 'valid'
    frozen = deepcopy(program)
    with pytest.raises(ValueError):
        topology_model.mutate_program(program, 'insert_action', seed=1)
    assert program == frozen
    program['actions'].append(topology_model._action('fifty_first', 'snapshot'))
    errors = topology_model.validate_program(program)['errors']
    assert any(e['code'] == 'episode_budget' for e in errors)


def test_concurrency_rejection_is_episode_only_and_l02_is_unchanged():
    program = v2.concurrent_writers_program()
    assert topology_model.validate_program(program)['status'] == 'valid'
    program['episode'] = {'schema': topology_model.EPISODE_CONFIG_SCHEMA,
                          'max_seconds': 120, 'checkpoint_action_ids': ['final']}
    errors = topology_model.validate_program(program)['errors']
    assert any(e['code'] == 'episode_concurrency' for e in errors)


def test_omitted_episode_keeps_legacy_program_bytes_and_validation_identity():
    program = topology_model.generate_program(17, writers=1, readers=1)
    frozen = json.dumps(program, sort_keys=True, separators=(',', ':'))
    assert 'episode' not in program
    assert canonical_hash(program) == '1f88b104ec6cbc0e7c33a36f73f2c5213740678ef19e41b1313781b923a70272'
    result = topology_model.validate_program(program)
    assert result['input_sha256'] == canonical_hash(program)
    assert json.dumps(program, sort_keys=True, separators=(',', ':')) == frozen
    assert v2.corpus_v2(include_episode=False) == v2.corpus_v2()


def test_explicit_profile_loads_one_authored_seed_and_does_not_claim_execution(monkeypatch):
    from r2f.campaign.corpus import Corpus, corpus_from_profile
    from r2f.campaign.exploratory_input import validate_topology_profile
    profile = json.loads(PROFILE.read_text())
    assert validate_topology_profile(profile) is True
    assert profile['enabled'] is False and profile['rmws'] == ['rmw_cyclonedds_cpp']
    assert profile['budget'] == {'max_inputs': 1, 'max_seconds': 180}
    calls = []
    original = v2.corpus_v2
    def source(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)
    monkeypatch.setattr(v2, 'corpus_v2', source)
    corpus = corpus_from_profile(profile, cycle_id='topology-authored')
    assert calls == [{'include_concurrent': False, 'include_cft_matrix': False, 'include_episode': True}]
    assert len(corpus.entries) == 1
    restored = Corpus.from_dict(json.loads(json.dumps(corpus.to_dict())))
    assert restored.to_dict() == corpus.to_dict()
    entry = next(iter(corpus.entries.values()))
    assert entry['final_config']['topology_program'] == v2.continuous_episode_program()
    assert entry['planned_only'] is True and entry['execution_admitted'] is False


def test_loader_opt_in_is_derived_from_selected_declared_cycle_not_union(monkeypatch):
    from r2f.campaign.corpus import corpus_from_profile
    profile = json.loads(PROFILE.read_text())
    other = deepcopy(profile['seed_specs'][0])
    other['seed_id'] = 'topology.multi_endpoint'
    profile['cycles'].append(dict(profile['cycles'][0], cycle_id='legacy', seed_specs=[other]))
    profile['seed_specs'].append(deepcopy(other))
    profile['declared_seed_specs_union'].append(deepcopy(other))
    calls = []
    original = v2.corpus_v2
    def source(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)
    monkeypatch.setattr(v2, 'corpus_v2', source)
    corpus = corpus_from_profile(profile, cycle_id='legacy')
    assert calls[0]['include_episode'] is False
    assert len(corpus.entries) == 1
    assert 'episode' not in next(iter(corpus.entries.values()))['final_config']['topology_program']


def test_explicit_source_without_episode_seed_is_not_silently_filled():
    from r2f.campaign.corpus import CorpusProfileError, topology_corpus_from_profile
    profile = json.loads(PROFILE.read_text())
    with pytest.raises(CorpusProfileError, match='topology_seed_not_connected'):
        topology_corpus_from_profile(profile, v2.corpus_v2())


def test_bfs_snapshot_plan_roundtrip_and_runtime_input_preserve_episode_exactly():
    from r2f.campaign.corpus import corpus_from_profile
    from r2f.campaign.exploratory_input import runtime_input
    from r2f.core.mutation_plan import MutationPlan
    from r2f.modules.s1.topology_input import make_topology_plan, validate_topology_plan
    profile = json.loads(PROFILE.read_text())
    entry = next(iter(corpus_from_profile(profile, cycle_id='topology-authored').entries.values()))
    parent = entry['final_config']['topology_program']
    frozen = deepcopy(parent)
    sample = profile['topology_search']['samples'][0]
    plan = make_topology_plan(parent, [{k: v for k, v in sample.items() if k != 'class_id'}], rng_seed=17)
    assert parent == frozen
    final = plan.final_config['topology_program']
    assert final['episode'] == parent['episode']
    assert len(final['actions']) == len(parent['actions']) + 1
    added = [a for a in final['actions'] if a['action_id'] not in {p['action_id'] for p in parent['actions']}]
    assert len(added) == 1 and added[0]['op'] == 'snapshot'
    assert added[0]['action_id'] not in final['episode']['checkpoint_action_ids']
    restored = MutationPlan.from_dict(json.loads(json.dumps(plan.to_dict())))
    validate_topology_plan(restored)
    assert restored.final_config == plan.final_config and restored.plan_hash() == plan.plan_hash()
    projected = runtime_input(restored.final_config, backend='topology_pair', runtime_capability='topology_v1')
    assert projected == restored.final_config
    projected['topology_program']['episode']['checkpoint_action_ids'].append('mutation')
    assert restored.final_config['topology_program']['episode'] == parent['episode']
    assert all(operation.parameters['input_layer_receipts'][0]['execution_evidence'] is False
               for operation in plan.operations)
