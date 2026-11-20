from copy import deepcopy
import pytest
from r2f.runtime.liveliness_evidence import evaluate_liveliness, API


def sample():
    clock = {'boot_id': 'boot', 'device': 4, 'inode': 5}
    w = dict(role='writer', logical_id='w', generation=1, pid=10, process_start_ticks=20, endpoint_generation='wg', endpoint_gid='aa')
    r = dict(role='reader', logical_id='r', generation=1, pid=11, process_start_ticks=21, endpoint_generation='rg', endpoint_gid='bb')
    def row(event, token, t, **kw):
        return dict(event=event, **{k: token[k] for k in ('logical_id','generation','pid','process_start_ticks','endpoint_generation')}, run_id='run', monotonic_ns=t, monotonic_clock_identity=clock, **kw)
    def cb(token, t, kind, scope, values):
        return row('topology_liveliness_event', token, t, event_kind=kind, observation_scope=scope, endpoint_gid=token['endpoint_gid'], endpoint_gid_at_callback=token['endpoint_gid'], endpoint_identity_status='bound', buffered=False, during_cleanup=False, callback_clock_identity=clock, callback_monotonic_ns=t-1, status_fields=values)
    events = [row('endpoint_created', w, 1, endpoint_gid='aa'), row('endpoint_created', r, 2, endpoint_gid='bb'),
              cb(w, 11, 'liveliness_lost', 'local_writer', dict(total_count=1,total_count_change=1)),
              cb(r, 13, 'liveliness_changed', 'reader_aggregate', dict(alive_count=0,not_alive_count=1,alive_count_change=-1,not_alive_count_change=1)),
              row('liveliness_asserted', w, 15, command_id=1, api=API, endpoint_gid='aa'),
              row('command_done', w, 16, command_id=1),
              cb(r, 19, 'liveliness_changed', 'reader_aggregate', dict(alive_count=1,not_alive_count=0,alive_count_change=1,not_alive_count_change=-1)),
              row('topology_liveliness_peer_coverage', r, 25, scope='continuous_complete_peer_inventory', external_peers_known=True, peer_gids=['aa'], reader_gid='bb', coverage_clock_identity=clock, coverage_start_ns=3, coverage_end_ns=23)]
    events.insert(0, dict(event='topology_provenance', run_id='run', monotonic_ns=1, monotonic_clock_identity=clock, runtime_kind='ros2_public_api', fixture=False))
    for i, e in enumerate(events): e['event_index'] = i
    return events, w, r


def run(events, w, r, fixture=False):
    return evaluate_liveliness(events, writer=w, reader=r, run_id='run', fixture=fixture)


def test_complete_synthetic_chain_and_fixture_separation():
    events, w, r = sample()
    assert run(events,w,r)['status'] == 'observed'
    result = run(events,w,r,fixture=True)
    assert result['status'] == 'fixture_observed' and not result['execution_evidence'] and result['effect_observed'] is None
    assert not result['candidate_eligible']


@pytest.mark.parametrize('fault', ['loss','ack','up','multi','unknown_peer','clock','callback_clock','generation','recreate','buffered','order','coverage','run','api'])
def test_missing_or_ambiguous_evidence_is_unknown(fault):
    e,w,r = sample()
    e.pop(0)
    for i, row in enumerate(e): row['event_index'] = i
    if fault == 'loss': e[2]['event_kind'] = 'other'
    if fault == 'ack': e[5]['command_id'] = 2
    if fault == 'up': e[6]['status_fields']['alive_count_change'] = 0
    if fault == 'multi': e[7]['peer_gids'].append('external')
    if fault == 'unknown_peer': e[7]['external_peers_known'] = False
    if fault == 'clock': e[4]['monotonic_clock_identity'] = {'boot_id':'other','device':4,'inode':5}
    if fault == 'callback_clock': e[6]['callback_clock_identity'] = None
    if fault == 'generation': e[6]['generation'] = 2
    if fault == 'recreate': e.append({**deepcopy(e[0]),'generation':2,'event_index':len(e)})
    if fault == 'buffered': e[2]['buffered'] = True
    if fault == 'order': e[6]['callback_monotonic_ns'] = 14
    if fault == 'coverage': e[7]['coverage_end_ns'] = 17
    if fault == 'run': e[5]['run_id'] = 'other'
    if fault == 'api': e[4]['api'] = 'fake'
    assert run(e,w,r,fixture=True)['status'] == 'unknown'
