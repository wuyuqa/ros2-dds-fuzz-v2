"""Shared-process peer facts at the existing deterministic carrier boundary.

All captures are explicit offline protocol doubles, never DDS execution evidence.
"""
from copy import deepcopy

import pytest

from r2f.runtime.participant_groups import (
    CONTROL, ENDPOINT_KEYS, LINK_KEYS, OBSERVATION_SCHEMA,
    qualify_observation,
)
from r2f.runtime.topology_pair import pair_graph_facts


def shared_capture():
    events, tokens, snapshots = [], {}, {}
    process = dict(pid=58, pgid=58, process_start_ticks=9001,
                   group_id='group_a', worker_instance_id='worker-a',
                   context_instance_id='context-a')

    def emit(kind, **fields):
        row = dict(event=kind, event_index=len(events), monotonic_ns=1000+len(events),
                   run_id='offline-shared-peer', backend='offline_fake', fixture=True,
                   real_ros=False, domain_id=191, **process)
        row.update(fields)
        events.append(row)
        return row

    emit('input_binding', receipt={'participant_control': dict(CONTROL)})
    emit('process_started', fresh_process=True, separate_context=False)
    emit('group_context_created', ros_context_created=True,
         api='rclpy.context.Context+rclpy.init', observed_domain_id=191,
         rmw_identifier='rmw_cyclonedds_cpp')
    emit('group_worker_ready')
    for cid, eid in enumerate(('w0', 'r0'), 1):
        emit('group_command_requested', command='create_endpoint', command_id=cid,
             logical_id=eid, generation=1)
        token = emit('endpoint_created', logical_id=eid, generation=1,
                     endpoint_gid=f'{cid:032x}', endpoint_generation='incarnation-'+eid,
                     role='writer' if eid=='w0' else 'reader',
                     topic='/shared', message_type='std_msgs/msg/String')
        tokens[eid] = token
        address = {k: token[k] for k in ENDPOINT_KEYS}
        emit('endpoint_group_bound', endpoint=deepcopy(address))
        emit('command_done', command='create_endpoint', command_id=cid, **address)

    roots = dict(complete=True, count=1, domain_id=191,
                 provenance={'fixture': True, 'not_native': True}, roots=[dict(
                     participant_handle=10, participant_gid=f'{90:032x}', domain_id=191,
                     domain_return_code=0, parent=0, participant_self_return=10, valid=True)])
    ownership = []
    for token in tokens.values():
        handle = 20 if token['role']=='writer' else 30
        sample = dict(schema='r2f.cyclone-participant-ownership/v2', complete=True,
                      status='observed', candidate_eligible=False, stable_sample_count=2,
                      domain_id=191, local_gid=token['endpoint_gid'], role_kind=token['role'],
                      local_handle=handle, participant_handle=10, participant_gid=f'{90:032x}',
                      parent_chain=[handle, 10], entity_inventory=[[10, [handle]], [handle, []]],
                      entity_identities=[{'handle': handle, 'gid': token['endpoint_gid']},
                                         {'handle': 10, 'gid': f'{90:032x}'}],
                      participant_root_observation={'participant_handle': 10,
                          'participant_self_return': 10, 'lookup_participants': [10]},
                      provenance=deepcopy(roots['provenance']))
        ownership.append({**{k: token[k] for k in ENDPOINT_KEYS},
                          'samples': [sample, deepcopy(sample)]})
    address = {k: tokens['w0'][k] for k in ENDPOINT_KEYS}
    emit('group_command_requested', command='group_snapshot', command_id=3, **address)
    observation = dict(schema=OBSERVATION_SCHEMA, requested_grouping='group_a',
                       supporting_diagnostic=True, candidate_eligible=False,
                       roots_before=roots, roots_after=deepcopy(roots), endpoints=ownership)
    observation.update(qualify_observation(observation, list(tokens.values())))
    emit('participant_group_snapshot', command_id=3, **address, **observation)
    emit('command_done', command='group_snapshot', command_id=3, **address)

    for cid, (eid, token) in enumerate(tokens.items(), 4):
        address = {k: token[k] for k in ENDPOINT_KEYS}
        emit('group_command_requested', command='snapshot', command_id=cid, **address)
        emit('topology_snapshot_requested', command='snapshot', command_id=cid, **address)
        match = emit('match_snapshot', command_id=cid, **address, matched_count=1,
                     peer_proof_source='dds_matched_endpoint_api', peer_gids_complete=True,
                     matched_peer_gids=[t['endpoint_gid'] for t in tokens.values() if t!=token])
        graph = emit('graph_snapshot', command_id=cid, **address,
                     publishers=[dict(endpoint_gid=tokens['w0']['endpoint_gid'],
                                      topic_type='std_msgs/msg/String')],
                     subscriptions=[dict(endpoint_gid=tokens['r0']['endpoint_gid'],
                                         topic_type='std_msgs/msg/String')])
        emit('command_done', command='snapshot', command_id=cid, **address)
        snapshots[eid] = dict(complete=True, command_id=cid,
                              match_snapshot=deepcopy(match), graph_snapshot=deepcopy(graph))
    return events, tokens, snapshots


def peer_fact(data):
    events, tokens, snapshots = data
    return pair_graph_facts(tokens['w0'], tokens['r0'], snapshots['w0'], snapshots['r0'],
                            raw_events=events, before_index=len(events), real_ros=False)


def test_authenticated_shared_process_is_not_an_endpoint_identity_alias():
    fact = peer_fact(shared_capture())
    assert fact['status'] == 'matched', fact['reasons']
    assert fact['identity_basis'] == 'direct_bidirectional_matched_peer_api'
    assert not fact['execution_evidence'] and not fact['candidate_eligible']


def test_controller_endpoint_binding_is_not_a_worker_lineage_row():
    data = shared_capture()
    events, tokens, snapshots = data
    # The real controller includes the endpoint PID, but its own PGID and no
    # worker/context fields. This is distinct from endpoint_group_bound.
    events.insert(8, dict(event='topology_endpoint_bound', logical_id='w0',
                          pid=58, pgid=0, run_id='offline-shared-peer'))
    for i, row in enumerate(events):
        row.update(event_index=i, monotonic_ns=1000+i)
    for eid, snap in snapshots.items():
        for kind in ('match_snapshot', 'graph_snapshot'):
            snap[kind] = deepcopy(next(e for e in events if e['event']==kind
                                      and e.get('logical_id')==eid))
    fact = peer_fact(data)
    assert fact['status'] == 'matched', fact['reasons']


def test_match_identity_does_not_require_measured_dds_participant_sharing():
    data = shared_capture()
    capture = next(e for e in data[0] if e['event']=='participant_group_snapshot')
    capture.update(roots_before=None, roots_after=None, endpoints=[])
    capture.update(qualify_observation(capture, list(data[1].values())))
    assert not capture['group_relation_observed']
    fact = peer_fact(data)
    assert fact['status'] == 'matched', fact['reasons']
    assert not capture['group_relation_observed']  # Match cannot grant ownership.


def refresh_snapshots(data):
    events, _, snapshots = data
    for i, row in enumerate(events):
        row.update(event_index=i, monotonic_ns=1000+i)
    for eid, snap in snapshots.items():
        for kind in ('match_snapshot', 'graph_snapshot'):
            snap[kind] = deepcopy(next(e for e in events if e['event']==kind
                                      and e.get('logical_id')==eid))
        snap['command_id'] = snap['match_snapshot']['command_id']


@pytest.mark.parametrize('damage', [
    'opt_in_missing', 'process_start_missing', 'context_missing', 'ready_missing',
    'binding_missing', 'create_request_missing', 'create_ack_missing',
    'snapshot_request_missing', 'binding_future', 'binding_stale',
    'cross_group', 'cross_worker', 'cross_context', 'cross_start_ticks',
    'snapshot_worker', 'snapshot_context', 'snapshot_group', 'snapshot_start_ticks',
    'snapshot_request_generation', 'snapshot_request_incarnation',
    'create_ack_incarnation', 'snapshot_run', 'snapshot_domain',
    'raw_match_not_backed', 'raw_graph_not_backed', 'process_exited',
    'context_closed', 'pid_claim_without_lineage',
])
def test_pid_share_never_excuses_unbound_or_stale_group_identity(damage):
    data = shared_capture()
    events, tokens, snapshots = data
    def row(kind, eid=None):
        return next(e for e in events if e['event']==kind
                    and (eid is None or e.get('logical_id')==eid))
    missing = {
        'process_start_missing': ('process_started', None),
        'context_missing': ('group_context_created', None),
        'ready_missing': ('group_worker_ready', None),
        'binding_missing': ('endpoint_group_bound', None),
        'create_request_missing': ('group_command_requested', 'w0'),
        'create_ack_missing': ('command_done', 'w0'),
        'snapshot_request_missing': ('group_command_requested', 'r0'),
    }
    if damage in missing:
        kind, eid = missing[damage]
        selected = row(kind, eid)
        if damage=='snapshot_request_missing':
            selected = next(e for e in events if e['event']==kind and e.get('command')=='snapshot')
        selected['event'] = 'missing_lineage_event'
    elif damage=='opt_in_missing':
        events[0]['receipt'].pop('participant_control')
    elif damage=='pid_claim_without_lineage':
        for e in events:
            for key in LINK_KEYS:
                e.pop(key, None)
    elif damage=='binding_future':
        bound = row('endpoint_group_bound')
        events.remove(bound)
        events.append(bound)
    elif damage=='binding_stale':
        row('endpoint_group_bound')['endpoint']['endpoint_generation'] = 'old-incarnation'
    elif damage.startswith('cross_'):
        key = {'cross_group': 'group_id', 'cross_worker': 'worker_instance_id',
               'cross_context': 'context_instance_id', 'cross_start_ticks': 'process_start_ticks'}[damage]
        value = 9999 if key=='process_start_ticks' else 'foreign-identity'
        # Keep the endpoint command chain self-consistent. Only its relation to
        # the group process/context is contradictory.
        for e in events:
            if e.get('logical_id')=='r0':
                e[key] = value
    elif damage in ('snapshot_worker', 'snapshot_context', 'snapshot_group', 'snapshot_start_ticks'):
        key = {'snapshot_worker': 'worker_instance_id', 'snapshot_context': 'context_instance_id',
               'snapshot_group': 'group_id', 'snapshot_start_ticks': 'process_start_ticks'}[damage]
        row('graph_snapshot', 'w0')[key] = 9999 if key=='process_start_ticks' else 'foreign'
    elif damage in ('snapshot_request_generation', 'snapshot_request_incarnation'):
        req = next(e for e in events if e['event']=='group_command_requested'
                   and e.get('command')=='snapshot')
        req['generation' if damage.endswith('_generation') else 'endpoint_generation'] = (
            2 if damage.endswith('_generation') else 'stale')
    elif damage=='create_ack_incarnation':
        row('command_done', 'w0')['endpoint_generation'] = 'stale'
    elif damage=='snapshot_run':
        row('graph_snapshot', 'w0')['run_id'] = 'foreign-run'
    elif damage=='snapshot_domain':
        row('graph_snapshot', 'w0')['domain_id'] = 999
    elif damage in ('process_exited', 'context_closed'):
        closed = deepcopy(row('process_started'))
        closed['event'] = 'process_exit' if damage=='process_exited' else 'group_context_shutdown'
        events.insert(12, closed)
    elif damage=='raw_match_not_backed':
        snapshots['w0']['match_snapshot']['matched_peer_gids'] = ['f'*32]
    elif damage=='raw_graph_not_backed':
        snapshots['w0']['graph_snapshot']['publishers'][0]['topic_type'] = 'foreign/Type'
    if not damage.startswith('raw_'):
        refresh_snapshots(data)
    fact = peer_fact(data)
    assert fact['status'] == 'unknown', (damage, fact)
    assert not fact['candidate_eligible']


@pytest.mark.parametrize('field', ['logical_id', 'endpoint_gid', 'endpoint_generation'])
def test_shared_process_does_not_relax_endpoint_identity_uniqueness(field):
    data = shared_capture()
    events, tokens, _ = data
    for e in events:
        if e.get('logical_id')=='r0':
            e[field] = tokens['w0'][field]
    # Preserve the lookup keys while keeping the snapshots raw-backed, where
    # logical addressing still exists after aliasing.
    if field != 'logical_id':
        refresh_snapshots(data)
    fact = peer_fact(data)
    assert fact['status'] == 'unknown'
    if field == 'endpoint_generation':
        assert 'per_peer_live_identity_alias:endpoint_generation' in fact['reasons']
    assert not fact['candidate_eligible']


def test_independent_workers_keep_the_strict_pid_alias_guard():
    from test_multipeer_match_observation import capture, pair
    data = capture()
    assert pair(data)['status'] == 'matched'
    events, _, _, _, _ = data
    # A coherent PID alias with no group lineage cannot select the opt-in path.
    for e in events:
        if e.get('pid')==102:
            e['pid'] = 100
    fact = pair(data)
    assert fact['status'] == 'unknown'
    assert any(reason.startswith('per_peer_live_identity_alias:pid') for reason in fact['reasons'])


def test_match_identity_is_not_a_vendor_or_guid_prefix_ownership_test():
    data = shared_capture()
    events, tokens, _ = data
    old_reader_gid = tokens['r0']['endpoint_gid']
    for e in events:
        if e['event']=='group_context_created':
            e['rmw_identifier'] = 'rmw_fastrtps_cpp'
        if e.get('logical_id')=='r0' and e.get('endpoint_gid')==old_reader_gid:
            e['endpoint_gid'] = 'f'*32
        if e['event']=='participant_group_snapshot':
            e.update(roots_before=None, roots_after=None, endpoints=[])
            e.update(qualify_observation(e, list(tokens.values())))
        if e['event']=='match_snapshot' and e['logical_id']=='w0':
            e['matched_peer_gids'] = ['f'*32]
        if e['event']=='graph_snapshot':
            e['subscriptions'][0]['endpoint_gid'] = 'f'*32
        if e['event']=='endpoint_group_bound' and e['endpoint']['logical_id']=='r0':
            e['endpoint']['endpoint_gid'] = 'f'*32
    refresh_snapshots(data)
    fact = peer_fact(data)
    assert fact['status'] == 'matched', fact['reasons']
    assert not fact['candidate_eligible']


@pytest.mark.parametrize('eid', ['w0', 'r0'])
@pytest.mark.parametrize('damage', [
    'foreign_gid', 'missing_gid', 'null_gid',
    'wrong_command', 'missing_command', 'null_command',
])
def test_create_ack_binds_full_endpoint_identity_and_creation_operation(eid, damage):
    data = shared_capture()
    events, tokens, _ = data
    ack = next(e for e in events if e['event'] == 'command_done'
               and e.get('command') == 'create_endpoint' and e.get('logical_id') == eid)
    if damage == 'foreign_gid':
        peer = 'r0' if eid == 'w0' else 'w0'
        ack['endpoint_gid'] = tokens[peer]['endpoint_gid']
    elif damage == 'wrong_command':
        ack['command'] = 'publish_batch'
    elif damage.startswith('missing_'):
        ack.pop('endpoint_gid' if damage.endswith('gid') else 'command')
    else:
        ack['endpoint_gid' if damage.endswith('gid') else 'command'] = None
    fact = peer_fact(data)
    assert fact['status'] == 'unknown', (eid, damage, fact)
    assert any('group_lineage:create_request_binding_ack_order' in reason
               for reason in fact['reasons'])
    assert not fact['candidate_eligible']


def test_snapshot_ack_accepts_existing_sparse_raw_contract():
    data = shared_capture()
    for event in data[0]:
        if event['event'] == 'group_command_requested' and event['command'] == 'snapshot':
            event['endpoint_gid'] = None
        elif event['event'] == 'command_done' and event['command'] == 'snapshot':
            event['command'] = None
            event['endpoint_gid'] = None
    fact = peer_fact(data)
    assert fact['status'] == 'matched', fact['reasons']
    assert not fact['execution_evidence'] and not fact['candidate_eligible']
