"""Pure raw-contract fixtures, never ROS/DDS execution or candidate evidence."""
from copy import deepcopy
import json

import pytest

from r2f.campaign.topology_adapter import _Audit
from r2f.campaign.topology_state_projection import _Projection, project_topology_state
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.topology_route import _action_proof
from test_endpoint_incarnation import trace, OLD, NEW
from test_topology_state_projection import RawFixture


def append(rows, event, **fields):
    row = dict(event=event, event_index=len(rows), monotonic_ns=100 + len(rows), **fields)
    rows.append(row)
    return row


def receiver(rows, gid=NEW):
    fields = {k: v for k, v in rows[0].items()
              if k not in {'event', 'event_index', 'monotonic_ns', 'endpoint_gid'}}
    return append(rows, 'receive', **fields, reader_gid=gid)


def consumers(rows):
    audit = _Audit.__new__(_Audit)
    audit.events = rows
    audit.path, audit.sha = 'fixture.jsonl', 'digest'
    projector = _Projection.__new__(_Projection)
    projector.events = rows
    projector.receipt = {'input_ref': {'event_index': 0}}
    return audit, projector


def test_authenticated_receive_uses_current_gid_without_mutating_creation():
    rows = trace()
    token = deepcopy(rows[0])
    event = receiver(rows)
    audit, projector = consumers(rows)
    audit.bound(event, token)
    projector.bound(event, token, gid_field='reader_gid')
    assert rows[0] == token
    assert token['endpoint_gid'] == OLD and token['generation'] == 1
    # A boundary-resolved token must also rewind to the event's raw prefix.
    new_token = endpoint_identity_at(rows, token)
    old_event = dict(rows[1], event='receive', reader_gid=OLD)
    audit.bound(old_event, new_token)
    projector.bound(old_event, new_token, gid_field='reader_gid')


@pytest.mark.parametrize('fault', ['unreceipted', 'wrong_worker', 'no_ack', 'old_gid', 'future_gid'])
def test_consumer_negative_identity_boundaries(fault):
    rows = trace()
    if fault == 'unreceipted':
        rows = rows[:1]
    elif fault == 'wrong_worker':
        rows[4]['pid'] += 1
    elif fault == 'no_ack':
        rows.pop()
    elif fault == 'future_gid':
        rows = rows[:1]
    event = receiver(rows, OLD if fault == 'old_gid' else NEW)
    audit, projector = consumers(rows)
    with pytest.raises(ValueError):
        audit.bound(event, rows[0])
    with pytest.raises(ValueError):
        projector.bound(event, rows[0], gid_field='reader_gid')


def projection_fixture(tmp_path, *, retirement=False, stale_gid=False, unreceipted=False):
    rows = trace()
    token = deepcopy(rows[0])
    if unreceipted:
        rows.pop(6)
        for i, row in enumerate(rows):
            row['event_index'] = i
    if retirement:
        append(rows, 'endpoint_destroyed', **{k: token[k] for k in
               ('logical_id', 'generation', 'pid', 'run_id')},
               endpoint_gid=OLD if stale_gid else NEW, cleanup_status='closed')
    boundary = append(rows, 'topology_state_boundary', run_id='run', lifecycle_complete=True)
    program = {'schema': 'r2f.s1-topology-program/v1', 'endpoints': [{'logical_id': 'r0', 'role': 'reader'}],
               'actions': [{'op': 'create', 'endpoint_id': 'r0', 'generation': 1}]}
    receipt = {'input_ref': {'event_index': 0}, 'reasons': [],
               'after': {'boundary_ref': {'event_index': boundary['event_index']},
                         'history': {}, 'identity': {}, 'component_refs': {}}}
    projector = _Projection(program, 'fixture.jsonl', 'digest', rows, receipt)
    facts, tokens = projector.endpoints('after', boundary, True)
    return rows, token, facts, tokens, receipt


def test_lifecycle_changes_gid_not_logical_or_process_generation(tmp_path):
    rows, origin, facts, tokens, receipt = projection_fixture(tmp_path)
    assert facts['r0'] == {'generation': 1, 'liveness': 'alive'}
    identity = receipt['after']['identity']['r0']
    assert identity['endpoint_gid'] == NEW
    assert identity['incarnation_ordinal'] == 1 and identity['identity_transition_ref'] == 6
    assert identity['creation_ref']['event_index'] == 0
    assert tokens['r0']['monotonic_ns'] == origin['monotonic_ns']
    assert tokens['r0']['endpoint_generation'] == origin['endpoint_generation']
    assert rows[0] == origin


def test_projection_retirement_uses_authenticated_gid(tmp_path):
    _, _, facts, tokens, _ = projection_fixture(tmp_path, retirement=True)
    assert facts['r0'] == {'generation': 1, 'liveness': 'retired'}
    assert not tokens


def test_projection_retirement_rejects_old_gid(tmp_path):
    _, _, facts, tokens, receipt = projection_fixture(tmp_path, retirement=True, stale_gid=True)
    assert facts['r0']['liveness'] == 'unknown' and not tokens
    assert any('retirement_identity' in r for r in receipt['reasons'])


def destroy_trace(*, old=False):
    rows = trace()
    action = dict(action_id='destroy-r0', op='destroy', endpoint_id='r0', generation=1)
    append(rows, 'topology_action_begin', action_id=action['action_id'], action=action, before={})
    origin = rows[0]
    append(rows, 'endpoint_destroyed', **{k: origin[k] for k in
           ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')},
           endpoint_gid=OLD if old else NEW, cleanup_status='closed', fixture=True, real_ros=False)
    append(rows, 'topology_action_done', action_id=action['action_id'], action=action,
           receipt=dict(action=action, satisfied=True, before={}))
    return action, rows


def test_route_destroy_resolves_actual_raw_not_origin_gid():
    action, rows = destroy_trace()
    refs, reason = _action_proof(action, rows)
    assert refs and reason is None
    action, rows = destroy_trace(old=True)
    refs, reason = _action_proof(action, rows)
    assert not refs and reason == 'retirement_identity_or_cleanup_mismatch'


def transitioned_fixture():
    """Extend the existing complete state fixture with a real raw-contract chain."""
    fixture = RawFixture(payload=True)
    old = fixture.tokens['r0']
    # Share the real resolver fixture's worker fields, keeping original origin index/time.
    template = trace()
    for key in ('pgid', 'domain_id', 'monotonic_clock_identity', 'node_name', 'rmw_identifier'):
        old[key] = template[0][key]
    old_gid = old['endpoint_gid']
    # Normalized production GID throughout the older capture; all refs stay fixed.
    def replace(value):
        if isinstance(value, dict):
            return {k: replace(v) for k, v in value.items()}
        if isinstance(value, list):
            return [replace(v) for v in value]
        return OLD if value == old_gid else value
    fixture.events = replace(fixture.events)
    fixture.tokens = {eid: fixture.events[t['event_index']] for eid, t in fixture.tokens.items()}
    old = fixture.tokens['r0']
    # Adapt independently authenticated same-node before/API/after/dynamic/transition/ACK.
    start = (len(fixture.events) + 1) * 100
    for source in template[1:]:
        row = deepcopy(source)
        row.update({k: old[k] for k in ('logical_id', 'generation', 'pid', 'process_start_ticks',
                   'endpoint_generation', 'run_id', 'pgid', 'domain_id', 'monotonic_clock_identity',
                   'node_name', 'rmw_identifier')})
        if 'topic' in row:
            row['topic'] = old['topic']
        if row['event'] == 'content_filter_dynamic_update':
            row.update(update_started_ns=start, update_finished_ns=start + 4 * 100)
        fields = {k: v for k, v in row.items() if k not in ('event', 'event_index', 'monotonic_ns')}
        index = fixture.add(row['event'], **fields)
        if row['event'] == 'content_filter_api_call':
            fixture.events[index]['api_monotonic_ns'] = fixture.events[index]['monotonic_ns']
    current = dict(old, endpoint_gid=NEW)
    fixture.tokens['r0'] = current
    for eid, token in fixture.tokens.items():
        cid = 'fresh-' + eid
        fixture.add('worker_command_requested', **fixture.identity(token), command='snapshot', command_id=cid)
        peer = fixture.tokens['r0' if eid == 'w0' else 'w0']
        fixture.add('match_snapshot', **fixture.identity(token), endpoint_gid=token['endpoint_gid'],
                    command_id=cid, matched_count=1, matched_peer_gids=[peer['endpoint_gid']],
                    peer_gids_complete=True, peer_proof_source='dds_matched_endpoint_api')
        fixture.add('graph_snapshot', **fixture.identity(token), command_id=cid,
                    publishers=fixture.graph_rows('writer'), subscriptions=fixture.graph_rows('reader'))
        fixture.add('command_done', **fixture.identity(token), command_id=cid)
    fixture.after = fixture.boundary()
    return fixture


def test_complete_projection_matches_new_incarnation_without_generation_bump(tmp_path):
    fixture = transitioned_fixture()
    path = tmp_path / 'raw.jsonl'
    path.write_text(''.join(json.dumps(e) + '\n' for e in fixture.events))
    projected = project_topology_state(fixture.program, path,
                                      before_ref=fixture.before, after_ref=fixture.after)
    identity = projected['receipt']['after']['identity']['r0']
    assert identity['generation'] == 1 and identity['endpoint_gid'] == NEW
    pair = projected['state_after']['pairs'][0]
    assert pair['observed_match'] == 'bidirectional', projected['receipt']['reasons']
    assert not projected['eligible']  # remains an offline fixture


def test_payload_attribution_keeps_new_reader_incarnation_bound():
    rows = trace()
    writer = append(rows, 'endpoint_created', logical_id='w0', generation=1,
                    pid=200, process_start_ticks=7, endpoint_generation='writer-process',
                    endpoint_gid='03' * 16, run_id='run')
    send = append(rows, 'send', **{k: writer[k] for k in
                  ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')},
                  writer_gid=writer['endpoint_gid'], publish_returned=True,
                  send_sequence=4, payload_sha256='a' * 64)
    sample = receiver(rows)
    sample.update(payload_run_id='run', payload_writer_id='w0', payload_writer_generation=1,
                  payload_sha256='a' * 64, send_sequence=4,
                  publisher_gid=None, publisher_gid_status='unavailable_api')
    audit, _ = consumers(rows)
    audit.tokens = {('r0', 1): rows[0], ('w0', 1): writer}
    audit.config = {'run_id': 'run'}
    audit.specs = {'w0': {'role': 'writer'}, 'r0': {'role': 'reader'}}
    result = audit.delivery_identity(sample, [send])
    assert result['basis'] == 'endpoint_correlated_payload'
    assert not result['direct_gid_match']
    sample['reader_gid'] = OLD
    with pytest.raises(ValueError, match='receive_reader_gid_mismatch'):
        audit.delivery_identity(sample, [send])


def test_full_projection_new_window_receive_and_old_window_binding(tmp_path):
    fixture = transitioned_fixture()
    pair = dict(writer='w0', reader='r0', writer_generation=1, reader_generation=1)
    opened = fixture.add('topology_delivery_window_open', **pair)
    for seq in range(3, 6):
        fixture.add('send', **fixture.identity(fixture.tokens['w0']),
                    writer_gid=fixture.tokens['w0']['endpoint_gid'], publish_returned=True, send_sequence=seq)
        fixture.add('receive', **fixture.identity(fixture.tokens['r0']), reader_gid=NEW,
                    publisher_gid=fixture.tokens['w0']['endpoint_gid'], send_sequence=seq)
    fixture.add('topology_delivery_window_closed', **pair, open_ref=opened,
                window_complete=True, sentinel_sequence=5)
    fixture.after = fixture.boundary()
    path = tmp_path / 'windows.jsonl'
    path.write_text(''.join(json.dumps(e) + '\n' for e in fixture.events))
    result = project_topology_state(fixture.program, path,
                                    before_ref=fixture.before, after_ref=fixture.after)
    assert result['state_after']['pairs'][0]['delivery_closure'] == 'closed', result['receipt']['reasons']
    assert result['receipt']['after']['identity']['r0']['generation'] == 1
    assert fixture.events[fixture.tokens['r0']['event_index']]['endpoint_gid'] == OLD
    # The raw window itself is unchanged; a stale reader GID cannot bind the new callback.
    for row in fixture.events[opened:]:
        if row['event'] == 'receive':
            row['reader_gid'] = OLD
    path.write_text(''.join(json.dumps(e) + '\n' for e in fixture.events))
    result = project_topology_state(fixture.program, path,
                                    before_ref=fixture.before, after_ref=fixture.after)
    assert result['state_after']['pairs'][0]['delivery_closure'] == 'unknown'
    assert any('endpoint_gid_mismatch:reader_gid' in reason for reason in result['receipt']['reasons'])


def test_adapter_inventory_keeps_origins_and_reports_current_identity():
    fixture = transitioned_fixture()
    audit, _ = consumers(fixture.events)
    audit.tokens = {}
    from r2f.modules.s1.topology_model import generate_program
    qos = generate_program(3, writers=1, readers=1)['endpoints'][0]['qos']
    audit.specs = {e['logical_id']: dict(e, qos=deepcopy(qos)) for e in fixture.program['endpoints']}
    for event in fixture.events:
        if event['event'] == 'endpoint_created':
            event['qos'] = deepcopy(qos)
        if event.get('logical_id') in fixture.tokens:
            event.setdefault('process_start_ticks', fixture.tokens[event['logical_id']]['process_start_ticks'])
    audit.program = fixture.program
    audit.config = {}
    audit.fixture = True
    identities = audit.identity()
    reader = next(row for row in identities if row['logical_id'] == 'r0')
    assert reader['generation'] == 1 and reader['endpoint_gid'] == NEW
    assert reader['incarnation_ordinal'] == 1
    assert audit.tokens[('r0', 1)]['endpoint_gid'] == OLD
    assert reader['creation_ref']['event_index'] == audit.tokens[('r0', 1)]['event_index']


def test_post_transition_position_cannot_backdate_receive_clock():
    rows = trace()
    event = receiver(rows)
    event['monotonic_ns'] = rows[-2]['monotonic_ns'] - 1
    audit, projector = consumers(rows)
    with pytest.raises(ValueError, match='before_identity_ack'):
        audit.bound(event, rows[0])
    with pytest.raises(ValueError, match='before_identity_ack'):
        projector.bound(event, rows[0], gid_field='reader_gid')


def test_identity_ack_itself_is_an_inclusive_identity_boundary():
    rows = trace()
    audit, projector = consumers(rows)
    audit.bound(rows[-1], rows[0])
    projector.bound(rows[-1], rows[0], gid_field='endpoint_gid')
