"""Generic public scopes are not a declaration of a FastDDS UST contract."""
from copy import deepcopy

import pytest

from r2f.campaign.topology_sample_attribution import native_delivery_identity
from r2f.core.replay import conformance_signature
from test_topology_candidate_identity import row
from test_topology_conformance_signature import model, read, rewrite
from test_topology_sample_attribution import raw_ref, sample, sends
from test_topology_publish_occurrence import prepared


def public_scopes():
    rows, _ = prepared()
    rows = [row for row in rows if row['event'] != 'rmw_publish_trace']
    for index, row in enumerate(rows):
        row.update(event_index=index, rmw_identifier='rmw_cyclonedds_cpp', observation_mode='blackbox')
        if row['event'] == 'send':
            row.pop('publish_occurrence_receipt')
        if row['event'] == 'receive':
            row.pop('publisher_gid', None)
    return rows


def test_public_cyclone_scope_does_not_invent_native_direct_publisher_identity():
    rows = public_scopes()
    before = deepcopy(rows)
    assert native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref) is None
    assert rows == before


@pytest.mark.parametrize('declaration', ['receipt_null', 'receipt_empty', 'trace',
                                       'fastdds_scope', 'instrumented_scope'])
def test_explicit_native_claim_in_public_capture_still_cannot_downgrade(declaration):
    rows = public_scopes()
    if declaration.startswith('receipt_'):
        sends(rows)[0]['publish_occurrence_receipt'] = None if declaration == 'receipt_null' else {}
    else:
        scope = next(row for row in rows if row['event'] == 'publish_call_begin')
        if declaration == 'trace': scope['event'] = 'rmw_publish_trace'
        elif declaration == 'fastdds_scope': scope['rmw_identifier'] = 'rmw_fastrtps_cpp'
        else: scope['observation_mode'] = 'instrumentation'
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)


# An unreferenced Cyclone API snapshot is not a FastDDS declaration either.
# The pre-existing authored model still supplies all its ordinary signature proof.


def peer_signature_row(row, observation):
    model(row)
    events = read(row)
    events.append(dict(event='match_snapshot', event_index=len(events), run_id=row['run_id'],
                       logical_id='w0', generation=1, observation_mode='blackbox',
                       peer_proof_source='dds_matched_endpoint_api',
                       native_peer_observation=observation, fixture=False))
    rewrite(row, events)


def test_known_cyclone_peer_schema_does_not_select_fastdds_signature_contract(row):
    peer_signature_row(row, {'schema': 'r2f.cyclone-matched-peer-observation/v1'})
    signature = conformance_signature(row)
    assert signature is not None and signature['oracle_verdict'] == 'fail'


@pytest.mark.parametrize('observation', [None, {}, {'schema': 'unknown'},
                        {'schema': 'r2f.fastdds-native-matched-peer-observation/v1'}])
def test_bad_or_unknown_explicit_peer_claim_is_not_a_legacy_fallback(row, observation):
    peer_signature_row(row, observation)
    assert conformance_signature(row) is None
