"""Authored seed normalization into the shared Corpus, with no live execution."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.core import canonical_hash
from r2f.campaign.corpus import (
    Corpus, CorpusProfileError, content_identity, corpus_from_profile,
    topology_corpus_from_profile, normalize_topology_seed_corpus,
)
from r2f.campaign.exploratory_input import validate_profile_fields, validate_topology_profile, runtime_input
from r2f.modules.s1 import fuzz_mutators_v2, input_model
from r2f.modules.s1.topology_input import make_topology_plan
from r2f.modules.s1.topology_targets import (
    TOPOLOGY_TARGET, TOPOLOGY_SURFACE, TOPOLOGY_MUTATOR, TOPOLOGY_CAPABILITY,
)

PROFILE = Path(__file__).resolve().parents[1] / 'config/s1_fuzz/profiles/topology-chains.json'


def profile():
    return json.loads(PROFILE.read_text())


def test_profile_declared_cycle_union_disabled_and_bounded_chain_contract():
    p = profile()
    assert validate_profile_fields(p) is None  # standard validation dispatch
    assert validate_topology_profile(p) is True
    assert p['enabled'] is False and p['required_catalog_included'] is False
    assert p['campaign_kind'] == 'exploratory' and p['runtime_capability'] == TOPOLOGY_CAPABILITY
    assert p['target_ids'] == [TOPOLOGY_TARGET] and p['surfaces'] == [TOPOLOGY_SURFACE]
    assert p['mutator_whitelist'] == [TOPOLOGY_MUTATOR]
    assert p['seed_specs'] == p['declared_seed_specs_union']
    assert {canonical_hash(s) for c in p['cycles'] for s in c['seed_specs']} == {
        canonical_hash(s) for s in p['seed_specs']}
    for length in range(1, 5):
        assert validate_topology_profile(dict(p, chain_length=length))
    assert 0 < p['budget']['max_inputs'] <= 256
    assert 0 < p['budget']['max_seconds'] <= 300
    assert p['operator_whitelist']


@pytest.mark.parametrize('key,value', [
    ('enabled', True), ('required_catalog_included', True), ('campaign_kind', 'required'),
    ('runtime_capability', None), ('runtime_capability', 'f2_windows_v1'),
    ('chain_length', 0), ('chain_length', 5), ('chain_length', True),
    ('operator_whitelist', []), ('operator_whitelist', ['unknown']),
    ('operator_whitelist', ['action.insert', 'action.insert']),
    ('budget', {'max_inputs': 257, 'max_seconds': 1}),
    ('budget', {'max_inputs': 1, 'max_seconds': float('inf')}),
    ('rmws', [[]]), ('declared_seed_specs_union', []), ('source_schema', 'legacy'),
])
def test_profile_rejects_contract_scope_or_unbounded_budget(key, value):
    p = profile()
    p[key] = value
    with pytest.raises(ValueError):
        validate_topology_profile(p)


def test_all_source_entries_accounted_for_without_silent_non_topology_selection():
    source = fuzz_mutators_v2.corpus_v2()
    normalized, receipt = normalize_topology_seed_corpus(source)
    assert receipt['source_entry_count'] == len(source['entries'])
    assert len(receipt['entries']) == len(source['entries'])
    rows = {row['seed_id']: row for row in receipt['entries']}
    assert len(rows) == len(source['entries'])
    assert len(normalized) == 8
    assert receipt['execution_evidence'] is False and receipt['candidate_eligible'] is False
    for entry in source['entries']:
        row = rows[entry['seed_id']]
        assert row['source_content_sha256'] == entry['content_sha256']
        if entry['surface_id'] != 'S1.topology':
            assert row['disposition'] == 'not_connected_non_topology_surface'
            assert entry['seed_id'] not in normalized
        else:
            negative = entry['expected_disposition'] == 'expect_guard_reject'
            assert row['disposition'] == ('retained_guard_negative' if negative else 'normalized_authored_topology')
    not_connected = next(e['seed_id'] for e in source['entries'] if e['surface_id'] != 'S1.topology')
    p = profile()
    for spec in [p['seed_specs'][0], p['declared_seed_specs_union'][0], p['cycles'][0]['seed_specs'][0]]:
        spec['seed_id'] = not_connected
    with pytest.raises(CorpusProfileError, match='not_connected_non_topology_surface'):
        topology_corpus_from_profile(p, source)


def test_real_shared_corpus_roundtrip_guards_never_selectable_or_candidates():
    c = corpus_from_profile(PROFILE)
    assert type(c) is Corpus
    assert len(c.entries) == 16
    assert len(c.selectable()) == 10
    assert Corpus.from_dict(json.loads(json.dumps(c.to_dict()))).to_dict() == c.to_dict()
    for record in c.entries.values():
        assert record['source_schema'] == 'r2f.s1-seed-corpus/v2'
        assert record['source_entry_schema'] == 'r2f.s1-seed-entry/v1'
        assert record['source_provenance']['authored'] is True
        assert record['candidate_eligible'] is False and record['execution_admitted'] is False
        assert record['planned_only'] is True and record['derived_from_execution'] is False
        negative = record['expected_disposition'] == 'expect_guard_reject'
        assert record['case_kind'] == ('guard_negative' if negative else 'authored_topology')
        assert record['executable'] is (not negative)
        if negative:
            assert record['content_sha256'] not in {r['content_sha256'] for r in c.selectable()}
            with pytest.raises(ValueError, match='not_applicable'):
                runtime_input(record['final_config'], backend='topology_pair',
                              runtime_capability=TOPOLOGY_CAPABILITY)


def test_hash_domains_preserved_raw_v2_case_core_envelope_and_corpus_identity():
    c = corpus_from_profile(PROFILE)
    record = next(r for r in c.entries.values() if r['seed_id'] == 'topology.multi_endpoint')
    program = record['final_config']['topology_program']
    raw = input_model.content_hash(program)
    envelope = canonical_hash({'topology_program': program})
    v2_case = input_model.content_hash({'schema': fuzz_mutators_v2.INPUT_PAYLOAD_SCHEMA,
                                       'surface_id': 'S1.topology', 'input': program})
    assert len({raw, envelope, v2_case, record['content_sha256']}) == 4
    assert raw == record['source_content_sha256']
    assert envelope == record['normalization_receipt']['core_envelope_sha256']
    assert record['content_sha256'] == content_identity(record)
    plan = make_topology_plan(program, [{'operator_id': 'action.insert'}], rng_seed=7)
    assert plan.parent_content_sha256 == envelope
    assert plan.final_config != record['final_config']
    receipt = plan.operations[0].parameters['input_layer_receipts'][0]
    assert receipt['input_before_sha256'] == raw
    assert plan.operations[0].input_before_sha256 == envelope


@pytest.mark.parametrize('mutation', ['no_ingestion', 'no_normalization', 'raw_hash', 'envelope_hash',
                                    'ingestion_hash', 'executable', 'candidate', 'source_schema',
                                    'normalization_kind', 'authored_provenance', 'expected_disposition',
                                    'source_entry_schema'])
def test_checkpoint_rejects_normalization_or_classification_tampering(mutation):
    data = corpus_from_profile(PROFILE).to_dict()
    record = data['entries'][0]
    if mutation == 'no_ingestion':
        data.pop('source_ingestion_receipt')
    elif mutation == 'no_normalization':
        record.pop('normalization_receipt')
    elif mutation == 'raw_hash':
        record['source_content_sha256'] = '0' * 64
    elif mutation == 'envelope_hash':
        record['normalization_receipt']['core_envelope_sha256'] = '0' * 64
    elif mutation == 'ingestion_hash':
        data['source_ingestion_receipt']['entries'][0]['disposition'] = 'tampered'
    elif mutation == 'executable':
        record['executable'] = not record['executable']
    elif mutation == 'candidate':
        record['candidate_eligible'] = True
    elif mutation == 'source_schema':
        record['source_schema'] = 'not_source'
    elif mutation == 'authored_provenance':
        record['source_provenance']['authored'] = False
    elif mutation == 'expected_disposition':
        record['expected_disposition'] = 'wrong'
    elif mutation == 'source_entry_schema':
        record['source_entry_schema'] = 'wrong'
    else:
        record['normalization_receipt']['normalization'] = 'silent_defaults'
    with pytest.raises(ValueError):
        Corpus.from_dict(data)


@pytest.mark.parametrize('mutation', ['hash', 'schema', 'duplicate', 'observed', 'expected', 'program'])
def test_malformed_or_non_authored_source_is_named_rejection(mutation):
    source = fuzz_mutators_v2.corpus_v2()
    entry = next(e for e in source['entries'] if e['seed_id'] == 'topology.multi_endpoint')
    if mutation == 'hash':
        entry['content_sha256'] = '0' * 64
    elif mutation == 'schema':
        source['schema'] = 'unknown'
    elif mutation == 'duplicate':
        source['entries'].append(deepcopy(entry))
    elif mutation == 'observed':
        entry['execution_evidence'] = True
    elif mutation == 'expected':
        entry['expected_disposition'] = 'expect_guard_reject'
    else:
        entry['document']['schema'] = 'not_topology'
        entry['content_sha256'] = input_model.content_hash(entry['document'])
    with pytest.raises(CorpusProfileError, match='topology_'):
        normalize_topology_seed_corpus(source)


def test_explicit_cycle_and_missing_seed_errors_and_legacy_serialization():
    p = profile()
    with pytest.raises(CorpusProfileError, match='cycle_not_found'):
        topology_corpus_from_profile(p, cycle_id='absent')
    p['cycles'].append(dict(deepcopy(p['cycles'][0]), cycle_id='another'))
    with pytest.raises(CorpusProfileError, match='explicit_cycle_id_required'):
        topology_corpus_from_profile(p)
    assert len(topology_corpus_from_profile(p, cycle_id='another').entries) == 16
    source = fuzz_mutators_v2.corpus_v2()
    source['entries'] = [e for e in source['entries'] if e['seed_id'] != p['seed_specs'][0]['seed_id']]
    with pytest.raises(CorpusProfileError, match='missing_seed'):
        topology_corpus_from_profile(profile(), source)
    legacy = Corpus().to_dict()
    assert 'source_ingestion_receipt' not in legacy
    assert Corpus.from_dict(legacy).to_dict() == legacy
