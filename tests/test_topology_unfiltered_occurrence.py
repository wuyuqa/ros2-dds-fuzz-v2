"""Native raw consumer regressions; fixtures confer no ROS/DDS qualification."""
from copy import deepcopy

import pytest

from r2f.campaign.topology_sample_attribution import native_delivery_identity
from r2f.runtime.topology_pair import payload_delivery_witness, pair_delivery_fact
from test_topology_publish_occurrence import prepared


def multiple_unfiltered(reader):
    rows, pair = prepared()
    for token in rows[:3]:
        if token['logical_id'] in ('r0', 'r1'):
            token['initial_content_filter'] = None
    token = next(row for row in rows[:3] if row['logical_id'] == reader)
    pair = {key: value for key, value in pair.items() if not key.startswith('control')}
    pair.update(reader=reader, reader_generation=token['generation'],
                reader_gid=token['endpoint_gid'])
    samples = [row for row in rows if row['event'] == 'receive' and row['logical_id'] == reader]
    return rows, pair, samples


@pytest.mark.parametrize('reader', ['r0', 'r1'])
def test_each_explicitly_unfiltered_reader_authenticates_its_own_sample(reader):
    rows, pair, samples = multiple_unfiltered(reader)
    original = deepcopy(rows)
    for sample, sequence in zip(samples, [0, 2, 4]):
        witness, reason = payload_delivery_witness(pair, sample, rows)
        assert witness is not None, reason
        assert witness['send_sequence'] == sequence  # fixture body always says 0
        assert witness['control_ref'] == witness['receive_ref'] == sample['event_index']
        assert witness['control_identity']['logical_id'] == reader
    fact = pair_delivery_fact(pair, rows, active_writers_on_topic=1)
    assert fact['received_sequences'] == [0, 2, 4]
    assert fact['missing_sequences'] == []
    assert rows == original


@pytest.mark.parametrize('reader', ['r0', 'r1'])
def test_canonical_sample_adapter_accepts_each_unfiltered_readers_self_measurement(reader):
    rows, _, samples = multiple_unfiltered(reader)
    sends = [row for row in rows if row['event'] == 'send']
    ref = lambda event: {'path': 'offline://unfiltered/raw.jsonl',
                         'sha256': 'f' * 64, 'event_index': event['event_index']}
    identity = native_delivery_identity(samples[1], sends, rows, ref=ref)
    assert identity['sequence'] == 2
    assert identity['reader'] == reader
    assert identity['control_ref'] == identity['receive_ref']
    assert identity['native_witness']['identity_basis'] == 'direct_gid_scoped_publish_occurrence'


@pytest.mark.parametrize('fault', ['missing_config', 'conflicting_other_reader', 'duplicate_receive'])
def test_self_measurement_keeps_configuration_and_full_native_proof_gates(fault):
    rows, pair, samples = multiple_unfiltered('r0')
    sample = samples[0]
    if fault == 'missing_config':
        for token in rows[:3]:
            token.pop('initial_content_filter', None)
    elif fault == 'conflicting_other_reader':
        other = next(row for row in rows if row['event'] == 'receive' and row['logical_id'] == 'r1')
        other['message_info_receipt']['fields']['publication_sequence_number']['value'] += 1
        other['rcl_take_receipt']['message_info_receipt'] = deepcopy(other['message_info_receipt'])
    else:
        rows.append(dict(deepcopy(sample), event_index=len(rows)))
    witness, reason = payload_delivery_witness(pair, sample, rows)
    assert witness is None and reason is not None


@pytest.mark.parametrize('fault', ['missing_config', 'filtered', 'updated', 'transition', 'retired', 'future'])
def test_self_control_needs_explicit_configuration_and_current_historical_identity(fault):
    rows, pair, samples = multiple_unfiltered('r0')
    target = rows[1]
    rows[2].pop('initial_content_filter')  # no independently qualified fallback
    if fault == 'missing_config':
        target.pop('initial_content_filter')
    elif fault == 'filtered':
        target['initial_content_filter'] = {'configured': True}
    elif fault == 'future':
        target['monotonic_ns'] = samples[0]['monotonic_ns'] + 1
    else:
        kind = {'updated': 'content_filter_dynamic_update',
                'transition': 'endpoint_identity_transition',
                'retired': 'endpoint_destroyed'}[fault]
        change = dict(deepcopy(target), event=kind, monotonic_ns=rows[2]['monotonic_ns'])
        rows.insert(3, change)
        for index, row in enumerate(rows):
            row['event_index'] = index
    witness, reason = payload_delivery_witness(pair, samples[0], rows)
    assert witness is None and reason is not None


@pytest.mark.parametrize('kind', ['content_filter_dynamic_update', 'endpoint_destroyed'])
def test_later_change_does_not_erase_an_authenticated_historical_self_sample(kind):
    rows, pair, samples = multiple_unfiltered('r0')
    rows.append(dict(deepcopy(rows[1]), event=kind, event_index=len(rows),
                     monotonic_ns=100 + len(rows)))
    witness, reason = payload_delivery_witness(pair, samples[0], rows)
    assert witness is not None, reason
    assert witness['control_ref'] == witness['receive_ref'] == samples[0]['event_index']
