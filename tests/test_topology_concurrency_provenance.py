"""Offline shapes only: no ROS, workers or claim of actual DDS execution."""
from copy import deepcopy

import pytest

from r2f.runtime.topology_concurrency import audit_concurrency


def raw_shape(*, serial=False):
    actions = [dict(action_id=f'publish_w{i}', endpoint_id=f'w{i}', generation=1,
                    params=dict(concurrency_group='g', start_sequence=0, count=1))
               for i in range(2)]
    events = []

    def add(event, ns, who=None, **fields):
        row = dict(event=event, monotonic_ns=ns, event_index=len(events),
                   run_id='offline-shape', domain_id=191, backend='ros2_public_api')
        if who is not None:
            row.update(logical_id=f'w{who}', generation=1, pid=100 + who,
                       pgid=100 + who, process_start_ticks=123,
                       endpoint_generation=f'epoch-{who}', endpoint_gid=f'gid-{who}',
                       monotonic_clock_identity={'boot_id': 'shape', 'inode': 1},
                       real_ros=True)
        row.update(fields)
        events.append(row)

    add('topology_backend_prepared', 1, real_ros=True)
    add('topology_provenance', 2, runtime_kind='ros2_public_api', fixture=False)
    for who in range(2):
        add('endpoint_created', 3, who)
    for event, ns in [('concurrent_publish_requested', 10),
                      ('concurrent_publish_ready', 20),
                      ('concurrent_publish_release', 30),
                      ('concurrent_publish_started', 40)]:
        for who in range(2):
            add(event, ns, who, command_id=who + 1, concurrency_group='g')
    for who in range(2):
        lo = 50 + (20 if serial else 1) * who
        add('send', lo + 10, who, command_id=who + 1,
            publish_begin_ns=lo, publish_return_ns=lo + 9,
            publish_returned=True, writer_gid=f'gid-{who}', send_sequence=0,
            phase=f'publish_w{who}')
        add('concurrent_publish_finished', 90, who, command_id=who + 1,
            concurrency_group='g')
        add('command_done', 100, who, command_id=who + 1)
    return events, actions


def audit(events, actions):
    return audit_concurrency(events, actions, 'g')


def test_explicit_healthy_public_runtime_shape_qualifies_only_shape():
    events, actions = raw_shape()
    result = audit(events, actions)
    assert result['concurrency_observed']
    assert not result['fixture'] and not result['fixture_overlap_observed']
    assert result['publish_api_overlaps'][0]['overlap_ns'] == 8
    assert result['candidate_eligible'] is False


def test_cross_writer_raw_release_before_peer_ready_is_rejected():
    events, actions = raw_shape()
    release = next(e for e in events if e['event'] == 'concurrent_publish_release'
                   and e['logical_id'] == 'w0')
    events.remove(release)
    peer_ready = next(i for i, e in enumerate(events)
                      if e['event'] == 'concurrent_publish_ready' and e['logical_id'] == 'w1')
    events.insert(peer_ready, release)
    for i, row in enumerate(events):
        row['event_index'] = i
    with pytest.raises(ValueError, match='raw_all_ready_before_release'):
        audit(events, actions)


@pytest.mark.parametrize('event,key,value', [
    ('topology_provenance', 'fixture', True),
    ('topology_provenance', 'fixture', None),
    ('topology_provenance', 'fixture', 0),
    ('topology_provenance', 'runtime_kind', 'offline_fake'),
    ('topology_provenance', 'runtime_kind', None),
    ('topology_provenance', 'backend', None),
    ('topology_backend_prepared', 'real_ros', False),
    ('topology_backend_prepared', 'real_ros', None),
    ('topology_backend_prepared', 'real_ros', 1),
    ('topology_backend_prepared', 'backend', 'offline_fake'),
    ('topology_provenance', 'run_id', 'foreign'),
    ('topology_backend_prepared', 'domain_id', 999),
    ('topology_provenance', 'monotonic_ns', 200),
    ('topology_provenance', 'event_index', 200),
    ('send', 'fixture', True),
    ('send', 'fixture', None),
    ('send', 'offline_fake', True),
    ('send', 'real_ros', None),
])
def test_unknown_or_contradictory_global_and_local_claims_never_real(event, key, value):
    events, actions = raw_shape()
    next(e for e in events if e['event'] == event)[key] = value
    result = audit(events, actions)
    assert not result['concurrency_observed']
    assert result['fixture'] and result['fixture_overlap_observed']
    assert result['publish_api_overlaps']
    assert not result['candidate_eligible']


@pytest.mark.parametrize('event,key', [
    ('topology_provenance', 'fixture'),
    ('topology_provenance', 'runtime_kind'),
    ('topology_provenance', 'backend'),
    ('topology_backend_prepared', 'real_ros'),
    ('topology_backend_prepared', 'backend'),
    ('topology_provenance', 'run_id'),
])
def test_missing_required_global_claim_never_real(event, key):
    events, actions = raw_shape()
    del next(e for e in events if e['event'] == event)[key]
    result = audit(events, actions)
    assert not result['concurrency_observed'] and result['fixture_overlap_observed']


@pytest.mark.parametrize('event', ['topology_provenance', 'topology_backend_prepared'])
@pytest.mark.parametrize('change', ['missing', 'duplicate'])
def test_global_provenance_must_be_unique_and_present(event, change):
    events, actions = raw_shape()
    row = next(e for e in events if e['event'] == event)
    if change == 'missing':
        events.remove(row)
    else:
        events.append(deepcopy(row))
    result = audit(events, actions)
    assert not result['concurrency_observed'] and result['fixture_overlap_observed']


def test_serial_fixture_stays_serial_and_nonreal():
    events, actions = raw_shape(serial=True)
    events[1].update(fixture=True, runtime_kind='offline_fixture')
    result = audit(events, actions)
    assert result['status'] == 'serial_or_no_api_overlap'
    assert not result['concurrency_observed'] and not result['fixture_overlap_observed']
    assert result['fixture'] and not result['publish_api_overlaps']


def test_global_fixture_counterexample_cannot_be_overruled_by_real_local_rows():
    events, actions = raw_shape()
    events[1].update(fixture=True, runtime_kind='offline_fake')
    result = audit(events, actions)
    assert result['fixture_overlap_observed'] and not result['concurrency_observed']
