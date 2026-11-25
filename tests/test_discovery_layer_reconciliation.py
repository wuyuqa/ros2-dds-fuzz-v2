"""Literal independent wire/native evidence at the public projection seam."""
from copy import deepcopy
import json
import pytest
from pathlib import Path

from test_discovery_layers_projection import parts_with_layers, reconcile
from test_instrumentation_sedp_projection import jsonraw


def test_topic_fields_are_compared_without_inventing_instance_ownership(tmp_path):
    parts = parts_with_layers(tmp_path)
    layers = reconcile(parts)['schema_check']['discovery_layers']
    comparison, = layers['topic']['field_comparisons']
    assert comparison['status'] == 'consistent'
    assert comparison['scope'] == 'topic_type_field_projection_not_instance_identity'
    assert comparison['fields'] == {'history': {'builtin': 'KEEP_LAST', 'wire': 'KEEP_LAST', 'equal': True},
                                    'depth': {'builtin': 7, 'wire': 7, 'equal': True}}
    assert comparison['builtin_raw_ref']['path'] == parts[-1]['raw']['path']
    assert comparison['wire_raw_refs'][0]['path'] == parts[0]['raw']['path']
    assert layers['topic']['owner_generation'] == 'unknown'
    assert layers['all_layer_obligations_closed'] is False
    assert layers['candidate_eligible'] is False


def test_topic_mismatch_is_review_material_separate_from_endpoint_oracle(tmp_path):
    parts = parts_with_layers(tmp_path)
    baseline = reconcile(parts)
    raw = json.loads(Path(parts[-1]['raw']['path']).read_bytes())
    for row in raw['receipts']:
        if row['operation'] == 'take:DCPSTopic':
            for sample in row['receipt']['samples']:
                sample['history']['depth'] = 8
    raw['samples']['DCPSTopic'][0]['history']['depth'] = 8
    parts[-1]['raw'] = jsonraw(Path(parts[-1]['raw']['path']), raw)
    result = reconcile(parts)
    comparison, = result['schema_check']['discovery_layers']['topic']['field_comparisons']
    assert comparison['status'] == 'mismatch_requires_review'
    assert comparison['fields']['depth'] == {'builtin': 8, 'wire': 7, 'equal': False}
    assert result['comparisons'] == baseline['comparisons']
    assert result['comparison_status'] == baseline['comparison_status']


@pytest.mark.parametrize('role', ['writer', 'writer2', 'writer4'])
def test_process_correlation_requires_raw_native_ownership_and_independent_spdp(tmp_path, role):
    from test_instrumentation_sedp_projection import GUID, PREFIX, hashed
    from r2f.instrumentation.sedp import parse_sedp_capture
    parts = parts_with_layers(tmp_path)
    participant = PREFIX + '000001c1'
    capture = parts[0]['capture_receipt']
    data = Path(capture['raw']['path']).read_bytes().replace(
        bytes.fromhex('0102030405060708090a0b0c'), bytes.fromhex(PREFIX))
    capture['raw'] = hashed(Path(capture['raw']['path']), data)
    capture['receipt_raw'] = jsonraw(tmp_path/'capture.json',
        {k: v for k, v in capture.items() if k != 'receipt_raw'})
    raw = json.loads(Path(parts[-1]['raw']['path']).read_bytes())
    raw['samples']['DCPSParticipant'][0]['guid'] = participant
    for row in raw['receipts']:
        if row['operation'] == 'take:DCPSParticipant':
            for sample in row['receipt']['samples']:
                sample['guid'] = participant
    parts[-1]['raw'] = jsonraw(Path(parts[-1]['raw']['path']), raw)
    binding = parts[2][0]
    path = Path(binding['return_raw_ref']['path'])
    events = [json.loads(line) for line in path.read_text().splitlines()]
    binding['role'] = role
    for event in events:
        event['role'] = role
    events[1]['participant_ownership'] = {
        'schema': 'r2f.cyclone-participant-ownership/v2', 'complete': True,
        'status': 'observed', 'stable_sample_count': 2, 'candidate_eligible': False,
        'local_gid': GUID, 'participant_gid': participant, 'domain_id': raw['domain_id'],
        'role_kind': 'writer', 'local_handle': 14, 'participant_handle': 10,
        'parent_chain': [14, 10], 'started_monotonic_ns': 1002,
        'ended_monotonic_ns': 1015,
        'participant_root_observation': {'participant_handle': 10,
            'participant_self_return': 10, 'lookup_participants': [10]},
        'entity_identities': [{'handle': 14, 'gid': GUID}, {'handle': 10, 'gid': participant}],
        'provenance': {'abi': 'CycloneDDS-0.10.5-lp64-endpoint64'},
    }
    ref = hashed(path, b''.join(json.dumps(e).encode()+b'\n' for e in events))
    for key in ('begin_raw_ref', 'return_raw_ref'):
        binding[key].update(ref)
    parts = (parse_sedp_capture(capture), *parts[1:])
    layers = reconcile(parts)['schema_check']['discovery_layers']
    row, = layers['participant']['process_correlations']
    assert row['status'] == 'endpoint_process_correlated'
    assert row['role'] == role
    assert row['generation'] == 'generation-a'
    assert row['pid'] == 77
    assert row['participant_guid'] == participant
    assert row['participant_lifecycle_generation'] == 'unknown'
    assert row['wire_raw_refs']
    assert row['builtin_raw_refs']
    # A caller projection never overrides the actual raw worker observation.
    binding['participant_ownership'] = {'participant_gid': 'f'*32}
    assert reconcile(parts)['schema_check']['discovery_layers']['participant']['process_correlations'] == [row]
    events[1]['participant_ownership']['local_gid'] = 'f'*32
    ref = hashed(path, b''.join(json.dumps(e).encode()+b'\n' for e in events))
    for key in ('begin_raw_ref', 'return_raw_ref'):
        binding[key].update(ref)
    failed, = reconcile(parts)['schema_check']['discovery_layers']['participant']['process_correlations']
    assert failed['status'] == 'unknown'
    assert 'native_ownership_not_bound' in failed['reasons']


def test_topic_api_gap_does_not_erase_independent_participant_observation(tmp_path):
    parts = parts_with_layers(tmp_path)
    raw = json.loads(Path(parts[-1]['raw']['path']).read_bytes())
    raw.update(status='unknown', reasons=['builtin_samples_unavailable'])
    raw['samples']['DCPSTopic'] = []
    for row in raw['receipts']:
        if row['operation'] == 'take:DCPSTopic':
            row['receipt'].update(samples=[], take_return_code=0, loan_returned=False)
    parts[-1].update(status='unknown', raw=jsonraw(Path(parts[-1]['raw']['path']), raw))
    layers = reconcile(parts)['schema_check']['discovery_layers']
    assert layers['participant']['observation_status'] == 'observed'
    assert layers['topic']['observation_status'] == 'unknown'
    assert layers['status'] == 'unknown'
    assert layers['all_layer_obligations_closed'] is False
