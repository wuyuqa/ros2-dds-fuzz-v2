"""Authored offline signature models, NOT real runs, admission or candidates.

Only tmp_path raw is written. Modeled qualification flags exercise the consumer;
no historical live-v5 report is loaded, relabeled or changed.
"""
from copy import deepcopy
import json

import pytest

from r2f.campaign.topology_sample_attribution import native_delivery_identity
from r2f.core.evidence import canonical_hash
from r2f.core.replay import conformance_signature
from test_topology_candidate_identity import row
from test_topology_conformance_signature import model, read, rewrite
from test_topology_publish_occurrence import prepared
from test_fastdds_peer_receipt import instrumented_window, authored_manifest_loader


def authored(row, rows, *, graph=False):
    model(row, 'graph' if graph else 'gap')
    row['run_id'] = rows[0]['run_id']
    row['rmw'] = 'rmw_fastrtps_cpp'
    for ev in rows:
        ev.update(fixture=False, test_fixture='authored-native-signature-no-DDS')
        ev.setdefault('run_id', row['run_id'])
    creations = [e for e in rows if e['event'] == 'endpoint_created']
    def ref(ev):
        return dict(path=row['raw_path'], sha256=row['raw_sha256'], event_index=ev['event_index'])
    program = dict(schema='r2f.s1-topology-program/v1', endpoints=[dict(logical_id=e['logical_id'], role='writer' if e['role'].startswith('writer') else 'reader',
                        topic=e['topic'], type_name=e['message_type'], qos={'reliability': 'reliable'})
                        for e in creations],
                   actions=[dict(op='create', endpoint_id=e['logical_id'], generation=1) for e in creations])
    row['final_input']['topology_program'] = program
    lineage = [{**{k: e[k] for k in ('logical_id', 'generation', 'endpoint_gid', 'endpoint_generation',
                                     'pid', 'process_start_ticks')}, 'creation_ref': ref(e)} for e in creations]
    pair = row['typed_facts']['pairs'][0]
    pair.update(delivery_closure='not_applicable', compatibility='incompatible' if graph else 'compatible')
    creation_refs = [e['creation_ref'] for e in lineage]
    pair['proofs'] = dict(compatibility=dict(status='observed', value=pair['compatibility'], refs=creation_refs),
                         observed_match=dict(status='observed', value='bidirectional', refs=creation_refs,
                                             detail={'proof_kind': 'direct_native_per_peer' if graph else 'direct_per_peer_api'}))
    sends = [e for e in rows if e['event'] == 'send']
    pair['communication_witnesses'] = [native_delivery_identity(e, sends, rows, ref=ref) for e in rows
                                      if e['event'] == 'receive' and e['logical_id'] == 'r0'] if not graph else []
    row['typed_facts'].update(endpoint_lineage=lineage, pairs=[pair])
    if not graph:
        row['oracle_verdict'] = row['oracle']['verdict'] = 'pass'
        row['oracle']['forbidden_residuals'] = []
    rewrite(row, rows)
    return row


def test_pass_native_repeated_body_uses_actual_0_2_4_and_independent_control(row):
    rows, _ = prepared()
    authored(row, rows)
    receives = [e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0']
    assert [e['send_sequence'] for e in receives] == [0, 0, 0]
    samples = row['typed_facts']['pairs'][0]['communication_witnesses']
    assert [s['sequence'] for s in samples] == [0, 2, 4]
    assert [s['publication_sequence_number'] for s in samples] == [41, 88, 102]
    result = conformance_signature(row)
    assert result and result['oracle_verdict'] == 'pass'
    assert result['typed_anomaly_signature'] == []


def peer_model(row):
    rows, opened, closed = instrumented_window()
    # An authored input-binding and boundary, not a claim about live artifacts.
    binding = dict(event='input_binding', event_index=0, run_id=rows[0]['run_id'],
                   monotonic_ns=rows[0]['monotonic_ns'] - 1)
    rows.insert(0, binding)
    for i, e in enumerate(rows): e['event_index'] = i
    authored(row, rows, graph=True)
    binding['receipt'] = dict(program_sha256=canonical_hash(row['final_input']['topology_program']))
    pair = row['typed_facts']['pairs'][0]
    pair['proofs']['observed_match']['refs'] = [dict(path=row['raw_path'], sha256=row['raw_sha256'],
                                                   event_index=e['event_index']) for e in rows
        if e['event'] in ('endpoint_created', 'native_peer_provider_selected', 'topology_snapshot_requested',
                          'match_snapshot', 'graph_snapshot', 'command_done')]
    row['typed_facts']['state_projection_receipt'] = {'after': {'boundary_ref': dict(
        path=row['raw_path'], sha256=row['raw_sha256'], event_index=opened['event_index'])}}
    rewrite(row, rows)
    return rows


def test_graph_fail_native_peer_proof_without_samples(row, authored_manifest_loader):
    peer_model(row)
    result = conformance_signature(row)
    assert result and result['oracle_verdict'] == 'fail'
    assert result['typed_anomaly_signature'][0]['residual'] == 'topology.pair_graph_mismatch'
    assert result['typed_anomaly_signature'][0]['communication_observed'] is False


def test_incompatible_native_none_is_valid_pass_not_positive_match(row, authored_manifest_loader):
    rows = peer_model(row)
    pair = row['typed_facts']['pairs'][0]
    pair['observed_match'] = pair['proofs']['observed_match']['value'] = 'none'
    for ev in rows:
        if ev['event'] == 'match_snapshot':
            ev.update(matched_count=0, public_count_before=0, matched_peer_gids=[])
            obs = ev['native_peer_observation']
            obs['matched_peer_gids'] = []
            obs['native_result'].update(required_count=0, copied=0)
    row['oracle_verdict'] = row['oracle']['verdict'] = 'pass'
    row['oracle']['forbidden_residuals'] = []
    rewrite(row, rows)
    result = conformance_signature(row)
    assert result and result['oracle_verdict'] == 'pass'
    assert result['typed_anomaly_signature'] == []
    # The same valid empty-set proof cannot support a graph-fail claim.
    row['oracle_verdict'] = row['oracle']['verdict'] = 'fail'
    row['oracle']['forbidden_residuals'] = ['topology.pair_graph_mismatch']
    assert conformance_signature(row) is None


@pytest.mark.parametrize('field', ['sequence', 'send_ref', 'control_ref', 'source_timestamp',
                                  'publication_sequence_number', 'reader_gid', 'publisher_gid_source',
                                  'reader_identity', 'native_witness', 'direct_gid_match'])
def test_claimed_native_witness_must_equal_raw_recomputation(row, field):
    rows, _ = prepared()
    authored(row, rows)
    sample = row['typed_facts']['pairs'][0]['communication_witnesses'][1]
    if field in ('sequence', 'source_timestamp', 'publication_sequence_number'):
        sample[field] += 1
    elif field.endswith('_ref'):
        sample[field] = deepcopy(sample[field])
        sample[field]['event_index'] = 0
    elif field == 'direct_gid_match':
        sample[field] = 1  # JSON int is not the native verifier's strict bool.
    elif field == 'reader_identity':
        sample[field]['endpoint_generation'] = 'wrong-instance'
    elif field == 'native_witness':
        sample[field]['scope_refs']['trace_ref']['event_index'] = 0
    else:
        sample[field] = 'wrong'
    assert conformance_signature(row) is None


@pytest.mark.parametrize('damage', ['bad_receipt', 'missing_receipt', 'timestamp_collision',
                                   'wrong_generation', 'missing_control', 'no_native_declaration'])
def test_bad_native_raw_never_downgrades_even_when_body_seq_zero_matches(row, damage):
    rows, _ = prepared()
    authored(row, rows)
    sent = [e for e in rows if e['event'] == 'send']
    if damage == 'bad_receipt': sent[0]['publish_occurrence_receipt']['schema'] = 'bad'
    elif damage == 'missing_receipt': sent[0].pop('publish_occurrence_receipt')
    elif damage == 'timestamp_collision':
        sent[1]['publish_occurrence_receipt']['source_timestamp'] = sent[0]['publish_occurrence_receipt']['source_timestamp']
    elif damage == 'wrong_generation': sent[0]['generation'] = 2
    elif damage == 'missing_control': rows[2].pop('initial_content_filter')
    else:
        for ev in rows:
            ev.pop('publish_occurrence_receipt', None)
            if ev['event'] in ('publish_call_begin', 'rmw_publish_trace', 'publish_call_end'):
                ev['event'] = 'offline_removed_scope'
    rewrite(row, rows)
    assert conformance_signature(row) is None


@pytest.mark.parametrize('damage', ['receipt', 'ordinal', 'graph', 'ack', 'inventory', 'manifest', 'downgrade'])
def test_bad_native_peer_cannot_be_rescued_by_simple_gid_list(row, authored_manifest_loader, damage):
    rows = peer_model(row)
    match = next(e for e in rows if e['event'] == 'match_snapshot')
    if damage == 'receipt': match.pop('native_peer_observation')
    elif damage == 'ordinal': match['native_peer_observation']['native_result']['snapshot_ordinal'] = 0
    elif damage == 'graph':
        next(e for e in rows if e['event'] == 'graph_snapshot')['subscriptions'] = []
    elif damage == 'ack':
        next(e for e in rows if e['event'] == 'command_done')['command_id'] = 'wrong'
    elif damage == 'inventory':
        match['matched_peer_gids'].append('ff' * 16)
    elif damage == 'manifest':
        match['native_peer_observation']['manifest_reference']['manifest_sha256'] = 'broken'
    else:
        row['typed_facts']['pairs'][0]['proofs']['observed_match']['detail']['proof_kind'] = 'direct_per_peer_api'
    rewrite(row, rows)
    assert conformance_signature(row) is None


def test_mixed_public_and_native_peer_proof_keeps_distinct_name(row, authored_manifest_loader):
    rows = peer_model(row)
    # The reader is public in this separate authored capture, not a relabeled
    # instrumented worker: remove its provider declaration and native metadata.
    selection = next(e for e in rows if e['event'] == 'native_peer_provider_selected' and e['logical_id'] == 'r0')
    selection['event'] = 'offline_no_provider_selected'
    for ev in rows:
        if ev.get('logical_id') == 'r0':
            ev.pop('observation_mode', None)
            ev.pop('native_peer_observation', None)
            if ev['event'] == 'match_snapshot': ev['peer_proof_source'] = 'dds_matched_endpoint_api'
    pair = row['typed_facts']['pairs'][0]
    pair['proofs']['observed_match']['detail']['proof_kind'] = 'direct_mixed_per_peer'
    rewrite(row, rows)
    result = conformance_signature(row)
    assert result and result['oracle_verdict'] == 'fail'


def test_native_signature_excludes_absolute_refs_and_domain(row, tmp_path):
    rows, _ = prepared()
    authored(row, rows)
    original = conformance_signature(row)
    assert original
    for ev in rows: ev['domain_id'] = 202
    row['raw_path'] = str(tmp_path / 'second-authored-capture.jsonl')
    rewrite(row, rows)
    assert conformance_signature(row) == original
    encoded = json.dumps(original)
    assert row['raw_path'] not in encoded
    assert rows[0]['endpoint_gid'] not in encoded


def test_duplicate_claim_of_same_native_receive_is_not_an_additional_sample(row):
    rows, _ = prepared()
    authored(row, rows)
    samples = row['typed_facts']['pairs'][0]['communication_witnesses']
    samples.append(deepcopy(samples[0]))
    assert conformance_signature(row) is None
