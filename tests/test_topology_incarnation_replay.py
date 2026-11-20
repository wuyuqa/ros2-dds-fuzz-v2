"""Offline exit/metadata contracts only; fabricated workers are not execution evidence."""
from copy import deepcopy

import pytest

from r2f.campaign.replay import _topology_worker_errors
from test_endpoint_incarnation import trace, OLD, NEW


def artifacts(*, transitioned=True):
    rows = trace() if transitioned else trace()[:1]
    for row in rows:
        row.update(worker_generation=1, worker_role='reader', real_ros=True,
                   backend='ros2_public_api', observed_domain_id=191,
                   domain_observation_source='worker_environment')
    token = rows[0]
    start = dict(token, event='process_started', monotonic_ns=99,
                 fresh_process=True, separate_context=True)
    rows.insert(0, start)
    current = NEW if transitioned else OLD
    for kind in ('process_exit', 'topology_worker_exit', 'worker_exit'):
        row = dict(token, event=kind, endpoint_gid=current, returncode=0,
                   monotonic_ns=110 + len(rows))
        if kind == 'worker_exit':
            row.pop('pgid')
        rows.append(row)
    for i, row in enumerate(rows):
        row['event_index'] = i
    ref = lambda index: {'raw_path': '/fixture/raw.jsonl', 'event_index': index}
    worker = {k: token[k] for k in ('logical_id', 'generation', 'endpoint_gid', 'endpoint_generation',
                                   'pid', 'pgid', 'process_start_ticks', 'worker_generation', 'rmw_identifier')}
    worker.update(role='reader', endpoint_role='reader', fixture=False, real_ros=True,
                  rmw=token['rmw_identifier'], domain_id=191, execution_domain_id=191,
                  domain_observation_source='worker_environment', fresh_process=True,
                  separate_context=True, closed=True, returncode=0, cleanup_status='closed',
                  unknowns=[], token_raw_ref=ref(1), start_raw_refs=[ref(0)],
                  exit_raw_refs=[ref(i) for i, row in enumerate(rows)
                                 if row['event'] in {'process_exit', 'topology_worker_exit', 'worker_exit'}],
                  cleanup_raw_refs=[])
    if transitioned:
        worker.update(current_endpoint_gid=NEW, incarnation_ordinal=1,
                      identity_transition_raw_refs=[ref(7)])
    config = dict(run_id='run', rmw=token['rmw_identifier'], domain_id=191,
                  topology_program={'endpoints': [{'logical_id': 'r0', 'role': 'reader'}]})
    result = dict(events=rows, raw_path='/fixture/raw.jsonl', execution_metadata={'workers': [worker]})
    return config, result


def test_authenticated_exit_gid_with_immutable_origin_and_refs():
    config, result = artifacts()
    original = deepcopy(result)
    assert _topology_worker_errors(config, result) == []
    assert result == original
    worker = result['execution_metadata']['workers'][0]
    assert worker['endpoint_gid'] == OLD and worker['token_raw_ref']['event_index'] == 1


def test_legacy_without_incarnation_metadata_keeps_strict_origin_gid():
    config, result = artifacts(transitioned=False)
    assert _topology_worker_errors(config, result) == []
    result['events'][-1]['endpoint_gid'] = NEW
    assert _topology_worker_errors(config, result)


@pytest.mark.parametrize('fault', ['old_exit_gid', 'no_transition', 'wrong_ack_gid',
    'wrong_ack_command', 'missing_ack', 'wrong_worker', 'wrong_ref_kind', 'foreign_ref',
    'omitted_ref', 'duplicate_ref', 'current_gid', 'ordinal', 'bool_ordinal',
    'missing_current', 'missing_ordinal', 'missing_refs', 'origin_gid', 'backdated_exit'])
def test_exit_and_incarnation_metadata_fail_closed(fault):
    config, result = artifacts()
    rows = result['events']
    worker = result['execution_metadata']['workers'][0]
    if fault == 'old_exit_gid': rows[-1]['endpoint_gid'] = OLD
    elif fault == 'no_transition': rows[7]['event'] = 'ignored_transition'
    elif fault == 'wrong_ack_gid': rows[8]['endpoint_gid'] = OLD
    elif fault == 'wrong_ack_command': rows[8]['command'] = 'snapshot'
    elif fault == 'missing_ack': rows[8]['event'] = 'not_an_ack'
    elif fault == 'wrong_worker': rows[5]['pid'] += 1
    elif fault == 'wrong_ref_kind': worker['identity_transition_raw_refs'][0]['event_index'] = 5
    elif fault == 'foreign_ref': worker['identity_transition_raw_refs'][0]['raw_path'] = '/foreign/raw.jsonl'
    elif fault == 'omitted_ref': worker['identity_transition_raw_refs'] = []
    elif fault == 'duplicate_ref': worker['identity_transition_raw_refs'] *= 2
    elif fault == 'current_gid': worker['current_endpoint_gid'] = OLD
    elif fault == 'ordinal': worker['incarnation_ordinal'] = 2
    elif fault == 'bool_ordinal': worker['incarnation_ordinal'] = True
    elif fault == 'missing_current': worker.pop('current_endpoint_gid')
    elif fault == 'missing_ordinal': worker.pop('incarnation_ordinal')
    elif fault == 'missing_refs': worker.pop('identity_transition_raw_refs')
    elif fault == 'origin_gid': worker['endpoint_gid'] = NEW
    elif fault == 'backdated_exit': rows[-1]['monotonic_ns'] = rows[8]['monotonic_ns'] - 1
    assert _topology_worker_errors(config, result)


def test_legacy_with_explicit_current_metadata_is_still_strict():
    config, result = artifacts(transitioned=False)
    worker = result['execution_metadata']['workers'][0]
    worker.update(current_endpoint_gid=OLD, incarnation_ordinal=0, identity_transition_raw_refs=[])
    assert _topology_worker_errors(config, result) == []
    worker['current_endpoint_gid'] = NEW
    assert _topology_worker_errors(config, result) == [
        'topology_worker_lineage_invalid:worker_current_endpoint_gid_mismatch']


def test_exit_cannot_borrow_a_later_authenticated_transition():
    config, result = artifacts()
    rows = result['events']
    worker = result['execution_metadata']['workers'][0]
    # Exit before the command begins; final metadata still names the later
    # fully authenticated incarnation. Per-exit prefixes must not borrow it.
    early = deepcopy(rows.pop(9))
    early.update(endpoint_gid=OLD, monotonic_ns=100)
    rows.insert(2, early)
    for i, row in enumerate(rows):
        row['event_index'] = i
    worker['identity_transition_raw_refs'][0]['event_index'] = 8
    worker['exit_raw_refs'] = [{'raw_path': result['raw_path'], 'event_index': i}
                              for i, row in enumerate(rows) if row['event'] in
                              {'process_exit', 'topology_worker_exit', 'worker_exit'}]
    assert _topology_worker_errors(config, result) == [
        'topology_worker_lineage_invalid:worker_identity_transition_after_exit']


def test_metadata_transition_ref_must_cover_exact_authenticated_command():
    config, result = artifacts()
    worker = result['execution_metadata']['workers'][0]
    worker['identity_transition_raw_refs'] = []
    assert _topology_worker_errors(config, result) == [
        'topology_worker_lineage_invalid:worker_incarnation_refs_do_not_cover_actual_lineage']
