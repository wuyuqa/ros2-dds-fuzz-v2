"""Authored offline lineage contracts: these fixtures never establish execution."""
from copy import deepcopy

import pytest

from r2f.campaign import replay
from r2f.runtime.topology_worker import worker_role_for


def artifacts(tmp_path):
    # Deliberately interleave kinds; runtime aliases follow per-kind declaration
    # order, not the event order, PID, logical-ID suffix or a global ordinal.
    specs = [{'logical_id': f'{kind}-{ordinal}', 'role': kind}
             for ordinal in range(4) for kind in ('writer', 'reader')]
    config = {'topology_program': {'endpoints': specs},
              'rmw': 'rmw_cyclonedds_cpp', 'domain_id': 186}
    raw = str(tmp_path / 'authored-contract-only.jsonl')
    events, workers = [], []
    for index, spec in enumerate(reversed(specs)):
        ordinal = specs.index(spec) // 2
        alias = worker_role_for(spec['role'], ordinal)
        token = dict(logical_id=spec['logical_id'], generation=1,
                     endpoint_gid=f'authored-gid-{index}', endpoint_generation=f'authored-epoch-{index}',
                     pid=100 + index, pgid=100 + index, process_start_ticks=1000 + index,
                     worker_generation=1, role=alias, worker_role=alias, topology_worker_role=alias,
                     real_ros=True, backend='ros2_public_api', rmw_identifier=config['rmw'],
                     observed_domain_id=186)

        def emit(event, **extra):
            row = dict(token, event=event, event_index=len(events), **extra)
            events.append(row)
            return {'raw_path': raw, 'event_index': row['event_index']}

        start = emit('process_started', fresh_process=True, separate_context=True)
        created = emit('endpoint_created')
        exited = emit('process_exit', returncode=0)
        cleanup = emit('endpoint_destroyed', cleanup_status='closed')
        worker = {k: deepcopy(token[k]) for k in ('logical_id', 'generation', 'endpoint_gid',
                  'endpoint_generation', 'pid', 'pgid', 'process_start_ticks', 'worker_generation')}
        worker.update(role=alias, endpoint_role=spec['role'], fixture=False, real_ros=True,
                      rmw_identifier=config['rmw'], rmw=config['rmw'], domain_id=186,
                      execution_domain_id=186, domain_observation_source='authored_fixture_only',
                      fresh_process=True, separate_context=True, closed=True, returncode=0,
                      cleanup_status='closed', unknowns=[], token_raw_ref=created,
                      start_raw_refs=[start], exit_raw_refs=[exited], cleanup_raw_refs=[cleanup])
        workers.append(worker)
    return config, {'raw_path': raw, 'events': events, 'execution_metadata': {'workers': workers}}


def selected(result, role='writer2'):
    worker = next(w for w in result['execution_metadata']['workers'] if w['role'] == role)
    return worker, result['events'][worker['token_raw_ref']['event_index']]


def rejects(config, result, reason='worker_role_mismatch'):
    assert 'topology_worker_lineage_invalid:' + reason in replay._topology_worker_errors(config, result)


@pytest.mark.parametrize('kind', ['writer', 'reader'])
@pytest.mark.parametrize('ordinal', range(4))
def test_all_canonical_one_through_four_aliases(tmp_path, kind, ordinal):
    config, result = artifacts(tmp_path)
    worker, token = selected(result, worker_role_for(kind, ordinal))
    assert worker['endpoint_role'] == kind
    assert replay._topology_worker_errors(config, result) == []
    assert replay._topology_worker_role_matches(config, worker, token)


@pytest.mark.parametrize('absent', [(), ('worker_role',), ('topology_worker_role',)])
def test_optional_raw_alias_fields_cannot_mask_another(tmp_path, absent):
    config, result = artifacts(tmp_path)
    _, token = selected(result)
    for key in absent:
        token.pop(key)
    assert replay._topology_worker_errors(config, result) == []


@pytest.mark.parametrize('role', ['reader', 'writer2', 'unknown', None, ''])
def test_metadata_business_kind_is_not_worker_alias(tmp_path, role):
    config, result = artifacts(tmp_path)
    worker, _ = selected(result)
    worker['endpoint_role'] = role
    rejects(config, result)


@pytest.mark.parametrize('field', ['role', 'worker_role', 'topology_worker_role'])
@pytest.mark.parametrize('bad', ['writer', 'writer3', 'reader2', 'writer5', 'unknown', None])
def test_explicit_raw_alias_fields_must_all_agree(tmp_path, field, bad):
    config, result = artifacts(tmp_path)
    _, token = selected(result)
    token[field] = bad
    rejects(config, result)


@pytest.mark.parametrize('bad', ['writer1', 'writer0', 'writer5', 'writer9', 'writer10',
                                 'writer02', 'writer-2', 'writer2evil', ' writer2', 'Writer2',
                                 'topology_writer', 'reader5', 'reader1', '', None, 2, [], {}])
def test_unknown_noncanonical_or_out_of_bounds_alias_even_if_copied_everywhere(tmp_path, bad):
    config, result = artifacts(tmp_path)
    worker, token = selected(result)
    worker['role'] = bad
    for key in ('role', 'worker_role', 'topology_worker_role'):
        token[key] = bad
    rejects(config, result)


@pytest.mark.parametrize('bad', ['reader2', 'writer3'])
def test_alias_cannot_be_reassigned_to_another_declared_endpoint(tmp_path, bad):
    config, result = artifacts(tmp_path)
    worker, token = selected(result)
    worker['role'] = bad
    for key in ('role', 'worker_role', 'topology_worker_role'):
        token[key] = bad
    rejects(config, result)


@pytest.mark.parametrize('field', ['role', 'worker_role', 'topology_worker_role', 'both_worker_fields'])
def test_missing_required_or_all_worker_alias_fields(tmp_path, field):
    config, result = artifacts(tmp_path)
    _, token = selected(result)
    for key in ('worker_role', 'topology_worker_role') if field == 'both_worker_fields' else (field,):
        token.pop(key)
    if field in ('worker_role', 'topology_worker_role'):
        assert replay._topology_worker_errors(config, result) == []
    else:
        rejects(config, result)


@pytest.mark.parametrize('damage', ['missing_program', 'missing_endpoints', 'wrong_role',
                                   'duplicate_id', 'unknown_id', 'fifth_writer', 'malformed_spec'])
def test_program_role_and_declaration_binding_fail_closed(tmp_path, damage):
    config, result = artifacts(tmp_path)
    specs = config['topology_program']['endpoints']
    if damage == 'missing_program': config.pop('topology_program')
    elif damage == 'missing_endpoints': config['topology_program'].pop('endpoints')
    elif damage == 'wrong_role': specs[2]['role'] = 'reader'
    elif damage == 'duplicate_id': specs[2]['logical_id'] = specs[0]['logical_id']
    elif damage == 'unknown_id': specs[2]['logical_id'] = 'other-writer'
    elif damage == 'fifth_writer': specs.append({'logical_id': 'fifth', 'role': 'writer'})
    else: specs[2] = None
    rejects(config, result)


@pytest.mark.parametrize('field', ['logical_id', 'generation', 'endpoint_gid', 'endpoint_generation',
                                  'pid', 'pgid', 'process_start_ticks', 'worker_generation'])
@pytest.mark.parametrize('surface', ['metadata', 'token'])
def test_identity_fields_remain_strict_independently_of_valid_role(tmp_path, field, surface):
    config, result = artifacts(tmp_path)
    worker, token = selected(result)
    item = worker if surface == 'metadata' else token
    item[field] = item[field] + 1 if isinstance(item[field], int) else 'forged-' + item[field]
    rejects(config, result, 'worker_token_identity_mismatch:' + field)


@pytest.mark.parametrize('damage', ['gid_reuse', 'process_reuse', 'foreign_raw', 'cleanup',
                                   'missing_exit', 'start_ticks', 'exit_pgid', 'fixture',
                                   'domain', 'rmw', 'start_after_token', 'duplicate_worker'])
def test_non_role_lineage_gates_still_reject(tmp_path, damage):
    config, result = artifacts(tmp_path)
    worker, token = selected(result)
    others = next(w for w in result['execution_metadata']['workers'] if w is not worker)
    if damage in ('gid_reuse', 'process_reuse'):
        keys = ('endpoint_gid',) if damage == 'gid_reuse' else ('pid', 'process_start_ticks')
        for key in keys: worker[key] = token[key] = others[key]
    elif damage == 'foreign_raw': worker['token_raw_ref']['raw_path'] = '/foreign/raw.jsonl'
    elif damage == 'cleanup': worker['cleanup_status'] = 'unknown'
    elif damage == 'missing_exit': worker['exit_raw_refs'] = []
    elif damage == 'start_ticks':
        worker['process_start_ticks'] = token['process_start_ticks'] = 900000
    elif damage == 'exit_pgid':
        result['events'][worker['exit_raw_refs'][0]['event_index']]['pgid'] += 1
    elif damage == 'fixture': worker['fixture'] = True
    elif damage == 'domain': worker['domain_id'] = 1
    elif damage == 'rmw': worker['rmw'] = 'forged-rmw'
    elif damage == 'start_after_token':
        result['events'][worker['start_raw_refs'][0]['event_index']]['event_index'] = token['event_index'] + 1
    else: result['execution_metadata']['workers'][-1] = deepcopy(worker)
    assert replay._topology_worker_errors(config, result)


def test_same_endpoint_new_generation_keeps_alias_but_not_process_identity(tmp_path):
    config, result = artifacts(tmp_path)
    worker, token = selected(result)
    new_worker = deepcopy(worker)
    offset = len(result['events'])
    for key in ('start_raw_refs', 'token_raw_ref', 'exit_raw_refs', 'cleanup_raw_refs'):
        refs = [new_worker[key]] if key == 'token_raw_ref' else new_worker[key]
        for ref in refs:
            old = result['events'][ref['event_index']]
            new = deepcopy(old)
            new.update(event_index=len(result['events']), generation=2, worker_generation=2,
                       pid=800, pgid=800, process_start_ticks=9000,
                       endpoint_gid='new-generation-gid', endpoint_generation='new-generation-epoch')
            result['events'].append(new)
            ref['event_index'] = new['event_index']
    new_token = result['events'][new_worker['token_raw_ref']['event_index']]
    for key in ('generation', 'worker_generation', 'pid', 'pgid', 'process_start_ticks',
                'endpoint_gid', 'endpoint_generation'):
        new_worker[key] = new_token[key]
    result['execution_metadata']['workers'].append(new_worker)
    assert len(result['events']) == offset + 4
    assert replay._topology_worker_errors(config, result) == []


@pytest.mark.parametrize('bad', ['writer', 'writer3', 'reader2', 'topology_writer', None])
def test_metadata_worker_alias_must_match_exact_raw_alias(tmp_path, bad):
    config, result = artifacts(tmp_path)
    worker, _ = selected(result)
    worker['role'] = bad
    rejects(config, result)


@pytest.mark.parametrize('kind', ['worker_exit', 'topology_worker_exit'])
def test_public_exit_shape_with_valid_aliases_still_passes_lineage_only(tmp_path, kind):
    config, result = artifacts(tmp_path)
    worker, _ = selected(result)
    event = deepcopy(result['events'][worker['exit_raw_refs'][0]['event_index']])
    event.update(event=kind, event_index=len(result['events']))
    if kind == 'worker_exit': event.pop('pgid')
    result['events'].append(event)
    worker['exit_raw_refs'].append({'raw_path': result['raw_path'], 'event_index': event['event_index']})
    assert replay._topology_worker_errors(config, result) == []


def test_canonicalized_authored_fixture_still_cannot_qualify_real_execution(tmp_path):
    # Repair only the authored fixture's invented aliases/IDs, not production
    # acceptance rules. This is explicitly not a persisted or executed run.
    from test_topology_replay import worker_artifacts
    config, result = worker_artifacts(tmp_path / 'fixture')
    specs = config['topology_program']['endpoints']
    for worker in result['execution_metadata']['workers']:
        kind = worker['endpoint_role']
        spec = next(e for e in specs if e['role'] == kind)
        old_id = worker['logical_id']
        worker.update(role=kind, logical_id=spec['logical_id'])
        for event in result['events']:
            if event.get('logical_id') == old_id:
                event.update(logical_id=spec['logical_id'], role=kind, worker_role=kind)
    assert replay._topology_worker_errors(config, result) == []
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': True, 'execution_stage': 'judged'}, output_root=tmp_path / 'fixture')
    assert qualified['real_ros_execution'] is False
    assert 'offline_fixture_cannot_establish_real_ros' in qualified['qualification_errors']
