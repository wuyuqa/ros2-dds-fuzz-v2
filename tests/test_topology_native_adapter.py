"""Native occurrence admission through canonical adapter; authored raw only."""
import json

import pytest

from r2f.campaign.topology_adapter import _Audit
from r2f.runtime.topology_worker import worker_role_kind
from test_topology_cft_occurrence_decoder import contrast_rows


def native_audit():
    rows = contrast_rows()
    for row in rows:
        if row['event'] == 'receive':
            row['payload_run_id'] = json.loads(row['string_data'])['run_id']
    audit = _Audit.__new__(_Audit)
    audit.events = rows
    audit.tokens = {(e['logical_id'], e['generation']): e for e in rows if e['event'] == 'endpoint_created'}
    audit.specs = {e['logical_id']: {'role': worker_role_kind(e['role'])} for e in audit.tokens.values()}
    audit.config = {'run_id': rows[0]['run_id']}
    audit.path, audit.sha = '/offline/native-adapter', 'a' * 64
    return audit, rows, [e for e in rows if e['event'] == 'send']


def test_canonical_adapter_uses_actual_send_refs_for_identical_body_and_incarnation():
    audit, rows, sends = native_audit()
    witnesses = [audit.delivery_identity(e, sends) for e in rows if e['event'] == 'receive']
    assert [w['sequence'] for w in witnesses] == [0, 0, 2, 4, 4]
    assert all(w['publisher_gid_source'] == 'public_rcl_take.rmw_message_info_t.publisher_gid'
               and w['basis'] == 'direct_gid_scoped_publish_occurrence' for w in witnesses)
    assert next(w for w in witnesses if w['reader'] == 'r0' and w['sequence'] == 4)[
        'reader_identity']['incarnation_ordinal'] == 1
    assert not any(w['reader'] == 'r0' and w['sequence'] == 2 for w in witnesses)


@pytest.mark.parametrize('damage', ['native_status', 'native_scope', 'old_run'])
def test_canonical_adapter_never_retries_legacy_hash_or_body_seq_after_native_failure(damage):
    audit, rows, sends = native_audit()
    receive = next(e for e in rows if e['event'] == 'receive')
    if damage == 'native_status': sends[0]['publish_occurrence_receipt']['status'] = 'unknown'
    elif damage == 'native_scope':
        next(e for e in rows if e['event'] == 'rmw_publish_trace')['publisher_rmw_handle'] += 1
    else: receive['payload_run_id'] = 'other-run'
    with pytest.raises(ValueError): audit.delivery_identity(receive, sends)
