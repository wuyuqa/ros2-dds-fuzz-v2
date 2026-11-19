"""Offline identity fixtures only: never evidence of natural SUT admission."""
from copy import deepcopy
import json

import pytest

from r2f.core.candidate import (
    CandidateLifecycle, TOPOLOGY_IDENTITY_SCHEMA, _validate_observation_identity,
    _validate_topology_admission,
)
from r2f.core.evidence import raw_sha256
from r2f.reports.candidate_report import _exploratory_evidence, exploratory_candidate_ledger


@pytest.fixture
def row(tmp_path):
    events, lineage = [], []
    roles = {'w0': 'writer', 'w1': 'writer', 'r0': 'reader'}
    raw = tmp_path / 'fixture.jsonl'
    for i, (logical_id, role) in enumerate(roles.items()):
        endpoint = dict(logical_id=logical_id, generation=1, endpoint_gid='gid-' + logical_id,
                        endpoint_generation='instance-' + logical_id, pid=10 + i, process_start_ticks=20 + i)
        lineage.append(deepcopy(endpoint))
        events.append(dict(endpoint, event='endpoint_created', event_index=i, role=role,
                           run_id='fixture-run', fixture=True))
    raw.write_text(''.join(json.dumps(e) + '\n' for e in events))
    digest = raw_sha256(raw)
    refs = [dict(path=str(raw), sha256=digest, event_index=i) for i in range(3)]
    for e, ref in zip(lineage, refs):
        e['creation_ref'] = ref
    pairs = [dict(writer=w, reader='r0', writer_generation=1, reader_generation=1,
                  compatibility='compatible', observed_match='bidirectional', delivery_closure='closed',
                  proofs={'observed_match': {'refs': [refs[i], refs[2]], 'status': 'observed'}})
             for i, w in enumerate(['w0', 'w1'])]
    facts = dict(schema='r2f.s1-topology-facts/v1', raw=dict(path=str(raw), sha256=digest),
                 endpoint_lineage=lineage, pairs=pairs, fixture=True, control_excluded=False,
                 candidate_eligible=False)
    return dict(run_id='fixture-run', cell_id='fixture-cell', target_id='discovery.endpoint_topology.fuzz_v1',
                target_revision=0, rmw='fixture-not-DDS', timestamp='2026-11-19T00:00:00Z',
                raw_path=str(raw), raw_sha256=digest, oracle_verdict='pass', execution_stage='judged',
                arm='mutated', candidate_eligible=False, typed_facts=facts,
                oracle={'candidate_eligible': False},
                final_input={'topology_program': {'endpoints': [dict(logical_id=k, role=v) for k, v in roles.items()]}})


def validate(row):
    evidence = _exploratory_evidence(row)
    _validate_observation_identity(evidence)
    return evidence


def test_versioned_n_identity_without_scalar_pair(row):
    evidence = validate(row)
    identity = evidence['endpoint_identity']
    assert identity['schema'] == TOPOLOGY_IDENTITY_SCHEMA
    assert len(identity['endpoint_lineage']) == 3 and len(identity['pairs']) == 2
    assert 'writer' not in identity and 'writer' not in row['typed_facts']
    assert identity['endpoint_lineage'] == row['typed_facts']['endpoint_lineage']


@pytest.mark.parametrize('damage', [
    'lineage_missing', 'duplicate_generation', 'duplicate_gid', 'duplicate_instance', 'bad_pid',
    'bad_generation', 'cross_generation', 'missing_reader', 'role_swap', 'duplicate_pair',
    'counts_only', 'missing_proofs', 'empty_refs', 'bad_index', 'bad_sha', 'other_raw',
    'forged_lineage', 'other_run', 'ambiguous_role', 'missing_role', 'nested_ref',
])
def test_rejects_ambiguous_or_unbound_identity(row, damage):
    facts = row['typed_facts']; lineage = facts['endpoint_lineage']; pair = facts['pairs'][0]
    if damage == 'lineage_missing': facts['endpoint_lineage'] = []
    elif damage == 'duplicate_generation': lineage.append(deepcopy(lineage[0]))
    elif damage == 'duplicate_gid': lineage[1]['endpoint_gid'] = lineage[0]['endpoint_gid']
    elif damage == 'duplicate_instance': lineage[1]['endpoint_generation'] = lineage[0]['endpoint_generation']
    elif damage == 'bad_pid': lineage[0]['pid'] = True
    elif damage == 'bad_generation': lineage[0]['generation'] = True
    elif damage == 'cross_generation': pair['reader_generation'] = 2
    elif damage == 'missing_reader': pair.pop('reader')
    elif damage == 'role_swap': pair['writer'] = 'r0'
    elif damage == 'duplicate_pair': facts['pairs'].append(deepcopy(pair))
    elif damage == 'counts_only': facts['pairs'] = {'writer_count': 2, 'reader_count': 1}
    elif damage == 'missing_proofs': pair.pop('proofs')
    elif damage == 'empty_refs': pair['proofs']['observed_match']['refs'] = []
    elif damage == 'bad_index': lineage[0]['creation_ref']['event_index'] = 9999
    elif damage == 'bad_sha': lineage[0]['creation_ref']['sha256'] = '0' * 64
    elif damage == 'other_raw': lineage[0]['creation_ref']['path'] += '.other'
    elif damage == 'forged_lineage': lineage[0]['endpoint_generation'] = 'forged'
    elif damage == 'other_run': row['run_id'] = 'another-run'
    elif damage == 'ambiguous_role': row['final_input']['topology_program']['endpoints'].append({'logical_id': 'w0', 'role': 'reader'})
    elif damage == 'missing_role': row['final_input']['topology_program']['endpoints'][0].pop('role')
    elif damage == 'nested_ref': pair['proofs']['observed_match']['detail'] = {'close_ref': {'path': 'other', 'event_index': 0}}
    with pytest.raises(ValueError): validate(row)


def test_identity_cannot_be_hidden_by_instrumentation(row):
    evidence = _exploratory_evidence(row)
    evidence.pop('endpoint_identity')
    evidence['instrumentation_identity'] = 'unrelated'
    with pytest.raises(ValueError): _validate_observation_identity(evidence)


def test_tampered_projection_rejected(row):
    evidence = _exploratory_evidence(row)
    evidence['endpoint_identity']['pairs'][0]['reader_generation'] = 9
    with pytest.raises(ValueError, match='differs'): _validate_observation_identity(evidence)


def test_unknown_schema_is_not_legacy_pair(row):
    evidence = _exploratory_evidence(row)
    evidence['endpoint_identity']['schema'] += '-unknown'
    with pytest.raises(ValueError): _validate_observation_identity(evidence)


def test_raw_tamper_is_rejected(row):
    from pathlib import Path
    Path(row['raw_path']).write_text('{}\n')
    with pytest.raises(ValueError): validate(row)


def test_fail_without_specific_anomaly_subject_rejected(row):
    row['oracle_verdict'] = 'fail'
    row['oracle']['closure'] = {'pairs': []}
    with pytest.raises(ValueError, match='applicable'): validate(row)
    row['oracle']['closure']['pairs'] = [dict(writer='w0', writer_generation=1, reader='r0', reader_generation=1)]
    with pytest.raises(ValueError, match='applicable'): validate(row)


def fixture_anomaly(row):
    # Entirely synthetic schema fixture, never a rewritten real pass artifact.
    row['oracle_verdict'] = 'fail'
    pair = row['typed_facts']['pairs'][0]
    pair['compatibility'] = 'incompatible'
    row['oracle'].update(verdict='fail', closure={'pairs': [dict(writer='w0', reader='r0', writer_generation=1, reader_generation=1)]})
    return row


def test_qualification_denials_retain_observed_not_admitted(row):
    fixture_anomaly(row)
    candidate = CandidateLifecycle('fixture', validate(row))
    with pytest.raises(ValueError, match='qualification not granted'): candidate.admit()
    assert candidate.candidate_status == 'observed'
    ledger = exploratory_candidate_ledger([row])
    assert ledger['candidates'][0]['candidate_status'] == 'observed'
    assert ledger['candidates'][0]['admission_error']
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0


def test_fixture_cannot_admit_even_with_forged_eligibility(row):
    fixture_anomaly(row)
    row['candidate_eligible'] = row['typed_facts']['candidate_eligible'] = row['oracle']['candidate_eligible'] = True
    with pytest.raises(ValueError, match='natural evidence'): _validate_topology_admission(row)


@pytest.mark.parametrize('kind', ['pass', 'unknown', 'control', 'tool_error'])
def test_no_natural_candidate_for_non_anomalies(row, kind):
    if kind == 'unknown': row['oracle_verdict'] = 'unknown'
    elif kind == 'control': row['arm'] = 'negative'
    elif kind == 'tool_error': row['tool_error'] = True
    ledger = exploratory_candidate_ledger([row])
    assert ledger['natural_candidate_count'] == 0 and ledger['candidates'] == []


def test_legacy_pair_extraction_is_unchanged():
    row = {'typed_facts': {'writer': {'id': 'w', 'generation': 'g1'}, 'reader': {'id': 'r', 'generation': 0}}}
    evidence = validate(row)
    assert evidence['endpoint_identity'] == row['typed_facts']
