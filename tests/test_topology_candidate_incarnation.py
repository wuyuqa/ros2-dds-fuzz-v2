"""Offline current-incarnation identity contracts; no DDS or admission claims."""
from copy import deepcopy

import pytest

from test_endpoint_incarnation import trace, OLD, NEW
from test_topology_candidate_identity import row, validate
from test_topology_conformance_signature import read, rewrite


def incarnation_row(row, *, changed=True):
    rows = read(row)
    chain = trace() if changed else trace()[:1]
    for event in chain:
        event.update(run_id=row['run_id'], fixture=True,
                     test_fixture='authored-candidate-incarnation-no-DDS')
    rows[2] = chain[0]
    rows.extend(chain[1:])
    for i, event in enumerate(rows):
        event['event_index'] = i
    reader = row['typed_facts']['endpoint_lineage'][2]
    reader.update({k: chain[0][k] for k in
                   ('logical_id', 'generation', 'endpoint_generation', 'pid', 'process_start_ticks')})
    reader.update(endpoint_gid=NEW if changed else OLD,
                  incarnation_ordinal=1 if changed else 0,
                  identity_transition_ref=len(rows) - 2 if changed else None)
    rewrite(row, rows)
    return rows, reader


def test_complete_raw_transition_binds_current_summary_without_rewriting_creation(row):
    rows, reader = incarnation_row(row)
    before = deepcopy(row)
    evidence = validate(row)
    assert evidence['endpoint_identity']['endpoint_lineage'][2] == reader
    assert rows[2]['endpoint_gid'] == OLD and reader['endpoint_gid'] == NEW
    assert row == before


def test_explicit_zero_incarnation_and_legacy_creation_both_work(row):
    incarnation_row(row, changed=False)
    validate(row)


@pytest.mark.parametrize('damage', [
    'old_summary_gid', 'foreign_summary_gid', 'wrong_ordinal', 'bool_ordinal',
    'wrong_transition_ref', 'bool_transition_ref', 'missing_ordinal', 'missing_transition_ref',
    'hidden_transition', 'missing_ack', 'wrong_ack_gid', 'wrong_ack_worker',
    'wrong_before_gid', 'wrong_graph_gid', 'wrong_transition_clock', 'changed_creation_pid',
])
def test_incomplete_or_forged_current_identity_is_rejected(row, damage):
    rows, reader = incarnation_row(row)
    if damage == 'old_summary_gid': reader['endpoint_gid'] = OLD
    elif damage == 'foreign_summary_gid': reader['endpoint_gid'] = '04' * 16
    elif damage == 'wrong_ordinal': reader['incarnation_ordinal'] = 2
    elif damage == 'bool_ordinal': reader['incarnation_ordinal'] = True
    elif damage == 'wrong_transition_ref': reader['identity_transition_ref'] = 9
    elif damage == 'bool_transition_ref': reader['identity_transition_ref'] = True
    elif damage == 'missing_ordinal': reader.pop('incarnation_ordinal')
    elif damage == 'missing_transition_ref': reader.pop('identity_transition_ref')
    elif damage == 'hidden_transition':
        reader.pop('incarnation_ordinal'); reader.pop('identity_transition_ref')
        reader['endpoint_gid'] = OLD
    elif damage == 'missing_ack': rows[-1]['event'] = 'not_an_ack'
    elif damage == 'wrong_ack_gid': rows[-1]['reader_gid'] = OLD
    elif damage == 'wrong_ack_worker': rows[-1]['pid'] += 1
    elif damage == 'wrong_before_gid': rows[-2]['before_gid'] = NEW
    elif damage == 'wrong_graph_gid': rows[6]['subscriptions'][0]['endpoint_gid'] = OLD
    elif damage == 'wrong_transition_clock': rows[-2]['monotonic_ns'] = 100
    elif damage == 'changed_creation_pid': rows[2]['pid'] += 1
    rewrite(row, rows)
    with pytest.raises(ValueError):
        validate(row)
