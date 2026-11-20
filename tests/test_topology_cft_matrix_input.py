"""Offline canonical CFT input integration; never infer a DDS filter effect."""
from copy import deepcopy
import json
import hashlib
import re

import pytest
from pathlib import Path
from unittest.mock import patch

from r2f.modules.s1 import fuzz_mutators_v2 as mutators, topology_model
from r2f.modules.s1.topology_cft_matrix import (
    CFT_MATRIX_SEED, SENTINEL_TOKEN, cft_matrix_program)
from r2f.campaign.corpus import topology_corpus_from_profile
from r2f.campaign import runner
from test_topology_cli import script
from test_topology_cycle import SOURCE

REPO = Path(__file__).resolve().parents[1]
PROFILE = REPO / 'config/s1_fuzz/profiles/model-bfs-topology-cft-matrix.json'


def assert_matrix(program):
    assert topology_model.validate_program(program)['status'] == 'valid'
    eps = {e['logical_id']: e for e in program['endpoints']}
    assert set(eps) == {'w0', 'r0', 'r1'}
    assert len({e['topic'] for e in eps.values()}) == 1
    assert all(e['qos']['reliability'] == 'reliable' and e['qos']['depth'] >= 6 for e in eps.values())
    assert eps['r0']['filter'] == {'expression': '', 'parameters': [], 'evaluation': None}
    assert 'filter' not in eps['r1']
    actions = program['actions']; by_id = {a['action_id']: a for a in actions}
    updates = [a for a in actions if a['op'] == 'update_filter']
    assert [a['action_id'] for a in updates] == ['allow_all', 'reject_original', 'restore_allow_all']
    assert updates[1]['params'] == {
        'expression': 'data LIKE %0', 'parameters': ["'%" + SENTINEL_TOKEN + "%'"]}
    assert all(a['endpoint_id'] == 'r0' for a in updates)
    assert all(a['generation'] in (None, 1) for a in actions)
    assert not any('concurrency_group' in a['params'] for a in actions)
    for aid, gate in [('repeat_rejected', 'reject_original'), ('repeat_restored', 'restore_allow_all')]:
        a = by_id[aid]
        assert a['params']['repeat_data_from'] == 'original'
        assert a['params']['count'] == 1 and 'payload' not in a['params']
        assert set(a['depends_on']) == {'original', gate}
        assert actions.index(by_id['original']) < actions.index(a)
        assert actions.index(by_id[gate]) < actions.index(a)
    assert by_id['original']['params']['count'] == 1
    assert by_id['original']['depends_on'] == ['matched']
    assert by_id['published_prior']['depends_on'] == ['allow_all']
    assert by_id['reject_snapshot']['depends_on'] == ['reject_sentinel']
    assert by_id['restore_snapshot']['depends_on'] == ['restore_sentinel']


def test_seed_opt_in_and_matrix():
    assert_matrix(cft_matrix_program())
    legacy = mutators.corpus_v2()
    opted = mutators.corpus_v2(include_cft_matrix=True)
    assert not any(e['seed_id'] == CFT_MATRIX_SEED for e in legacy['entries'])
    assert [e for e in opted['entries'] if e['seed_id'] != CFT_MATRIX_SEED] == legacy['entries']
    entry = next(e for e in opted['entries'] if e['seed_id'] == CFT_MATRIX_SEED)
    assert entry['execution_evidence'] is False and entry['candidate_eligible'] is False
    profile = json.loads(PROFILE.read_text())
    assert profile['enabled'] is False
    corpus = topology_corpus_from_profile(profile)
    assert len(corpus.entries) == 1


def test_canonical_cli_bfs_plan_and_checkpoint_resume(tmp_path):
    cli = script('run_s1')
    first, second = tmp_path / 'first', tmp_path / 'second'
    args = ['--exploratory-profile', str(PROFILE), '--cycle-id', 'topology-authored', '--domain-id', '80']
    # --execute is a store_true flag: absence is the canonical execute=False.
    with patch.object(cli, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(runner, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(cli, 'runtime_for_boundary', side_effect=AssertionError('runtime forbidden')):
        assert cli.main(args + ['--output-dir', str(first), '--max-inputs', '1', '--seed', '17']) == 0
        assert cli.main(args + ['--output-dir', str(second), '--max-inputs', '1', '--resume-from', str(first)]) == 0
    cycle = json.loads((first / 'cycle.json').read_text())
    assert cycle['execute'] is False and cycle['budget']['attempted'] == 0
    assert cycle['rows']
    for row in cycle['rows']:
        assert_matrix(row['final_input']['topology_program'])
        assert row['chain_depth'] == 1
    assert (second / 'checkpoint.json').exists()


@pytest.mark.parametrize('metadata', [
    {'run_id': 's1f-normal-run', 'send_sequence': 0, 'phase': 'original',
     'writer_id': 'w0', 'writer_generation': 1, 'endpoint_gid': '0123456789abcdef'},
    {'run_id': 'run-234', 'send_sequence': 937, 'phase': 'another-normal-phase',
     'writer_id': 'w0', 'writer_generation': 1, 'endpoint_gid': 'f' * 32},
    {'run_id': 'ascii-run', 'send_sequence': 42, 'phase': 'publish',
     'writer_id': 'w0', 'writer_generation': 1, 'endpoint_gid': None},
])
def test_parameter_like_matches_complete_serialized_envelope(metadata):
    actions = {a['action_id']: a for a in cft_matrix_program()['actions']}
    predicate = actions['reject_original']['params']
    assert predicate['expression'] == 'data LIKE %0'
    assert len(predicate['parameters']) == 1
    quoted = predicate['parameters'][0]
    assert quoted == "'%" + SENTINEL_TOKEN + "%'" and quoted.isascii()
    assert not any(c in quoted[1:-1] for c in "_'\\")
    # Independent SQL LIKE subset interpretation, not a runtime effect fixture.
    pattern = re.compile(r"\A" + '.*'.join(
        re.escape(part) for part in quoted[1:-1].split('%')) + r"\Z", re.S)

    def envelope(payload, attributes):
        body = dict(attributes, declared_payload=payload,
                    declared_payload_sha256=hashlib.sha256(payload.encode()).hexdigest())
        return json.dumps(body, sort_keys=True)

    assert SENTINEL_TOKEN not in json.dumps(metadata, sort_keys=True)
    original_payload = actions['original']['params']['payload']
    original_data = envelope(original_payload, metadata)
    sentinel_data = envelope(actions['reject_sentinel']['params']['payload'], metadata)
    assert pattern.fullmatch(original_data) is None
    assert pattern.fullmatch(sentinel_data) is not None
    assert original_data != original_payload and sentinel_data != SENTINEL_TOKEN
    # Prove the predicate is about full String.data: a token in metadata alone
    # also matches. This is NOT normal original metadata and grants no effect.
    assert pattern.fullmatch(envelope(original_payload, dict(metadata, run_id=SENTINEL_TOKEN)))
