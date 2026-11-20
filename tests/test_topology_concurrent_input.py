"""Explicit authored concurrency input through the canonical corpus/CLI only."""
import json
from pathlib import Path
import subprocess
import sys

from r2f.campaign.corpus import Corpus, corpus_from_profile
from r2f.campaign.checkpoint import _identity
from r2f.campaign.identity import source_manifest
from r2f.core import canonical_hash
from r2f.modules.s1 import fuzz_mutators_v2 as v2, topology_model
from r2f.core.mutation_plan import MutationPlan

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / 'config/s1_fuzz/profiles/model-bfs-topology-concurrent-writers.json'


def test_concurrency_is_opt_in_and_default_corpus_identity_is_unchanged():
    legacy = v2.corpus_v2()
    assert canonical_hash(legacy) == '7c420c8cc5425828bb3e15b3a7655904d2ba069fb6aa0059ff99db787f883ecd'
    assert v2.CONCURRENT_WRITERS_SEED not in {e['seed_id'] for e in legacy['entries']}
    extended = v2.corpus_v2(include_concurrent=True)
    assert [e for e in extended['entries'] if e['seed_id'] != v2.CONCURRENT_WRITERS_SEED] == legacy['entries']
    assert [m for m in extended['manifest'] if m['seed_id'] != v2.CONCURRENT_WRITERS_SEED] == legacy['manifest']
    entry = next(e for e in extended['entries'] if e['seed_id'] == v2.CONCURRENT_WRITERS_SEED)
    assert entry['execution_evidence'] is False and entry['candidate_eligible'] is False
    assert entry['provenance']['derived_from_execution'] is False
    program = entry['document']
    assert topology_model.validate_program(program)['status'] == 'valid'
    pubs = [a for a in program['actions'] if a['params'].get('concurrency_group')]
    assert len(pubs) == 2 and len({a['endpoint_id'] for a in pubs}) == 2
    assert all(a['params']['count'] == 64 and a['params']['interval_sec'] == 0 for a in pubs)
    assert all(a['depends_on'] == ['matched_before_group'] for a in pubs)
    assert all(e['qos']['reliability'] == 'reliable' and e['qos']['depth'] == 128 for e in program['endpoints'])
    for a in pubs:
        sentinel = next(s for s in program['actions'] if s['action_id'] == 'later_sentinel_' + a['endpoint_id'])
        assert sentinel['params']['start_sequence'] == 64
        assert sentinel['generation'] == a['generation']


def test_explicit_profile_corpus_and_checkpoint_independently_reconstruct_input():
    p = json.loads(PROFILE.read_text())
    assert p['enabled'] is False and p['required_catalog_included'] is False
    corpus = corpus_from_profile(p, cycle_id='topology-authored')
    assert len(corpus.entries) == 1
    assert Corpus.from_dict(json.loads(json.dumps(corpus.to_dict()))).to_dict() == corpus.to_dict()
    identity, seeds, _ = _identity(p, 'topology-authored', source_manifest(ROOT))
    assert seeds.to_dict() == corpus.to_dict()
    assert identity['initial_corpus_sha256'] == canonical_hash(corpus.to_dict())
    entry = next(iter(corpus.entries.values()))
    assert entry['final_config']['topology_program'] == v2.concurrent_writers_program()
    assert entry['planned_only'] is True and entry['execution_admitted'] is False


def test_canonical_cli_plans_concurrency_and_sentinels_without_execution(tmp_path):
    result = subprocess.run([sys.executable, '-B', str(ROOT/'scripts/run_s1.py'),
        '--exploratory-profile', str(PROFILE), '--cycle-id', 'topology-authored',
        '--seed', '5', '--max-inputs', '1', '--output-dir', str(tmp_path/'planned')],
        cwd=ROOT, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stderr
    cycle = json.loads((tmp_path/'planned/cycle.json').read_text())
    assert cycle['abort'] is None and cycle['execute'] is False
    assert cycle['budget']['actual'] == cycle['budget']['attempted'] == 0
    assert len(cycle['rows']) == 1
    row = cycle['rows'][0]
    assert row['execution_stage'] == 'planned' and row['actual_run'] is False
    plan = MutationPlan.from_dict(row['mutation_plan'])
    final = plan.final_config['topology_program']
    assert topology_model.validate_program(final)['status'] == 'valid'
    assert len([a for a in final['actions'] if a['params'].get('concurrency_group')]) == 2
    assert len([a for a in final['actions'] if a['action_id'].startswith('later_sentinel_')]) == 2
    assert any(a['action_id'].startswith('snapshot_added') for a in final['actions'])
    capsule = json.loads(Path(row['replay_path']).read_text())
    assert capsule['execution'] is None
