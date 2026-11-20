"""Offline interval regressions. Synthetic raw is never a real DDS run."""
from copy import deepcopy

import pytest

from test_liveliness_evidence import sample
from r2f.runtime.liveliness_evidence import evaluate_liveliness
from r2f.runtime.topology_pair import _liveliness_observation_facts


def evaluate(events, writer, reader, **kwargs):
    return evaluate_liveliness(events, writer=writer, reader=reader,
                               run_id='run', fixture=True, **kwargs)


def append(events, token, event, at, **fields):
    row = {**deepcopy(events[1]), **token, 'event': event,
           'event_index': len(events), 'monotonic_ns': at, **fields}
    events.append(row)
    return row


def test_completed_interval_survives_later_normal_cleanup():
    events, writer, reader = sample()
    for token in (writer, reader):
        append(events, token, 'topology_worker_exit', 30)
        append(events, token, 'topology_endpoint_destroyed', 31)
        append(events, token, 'topology_generation_retired', 32)
    observation = evaluate(events, writer, reader)
    assert observation['status'] == 'fixture_observed', observation
    assert observation['observation_window']['end_ns'] == 23
    assert observation['effect_observed'] is None
    assert not observation['execution_evidence'] and not observation['candidate_eligible']


@pytest.mark.parametrize('at', [2, 3, 12, 23])
@pytest.mark.parametrize('event', ['endpoint_destroyed', 'topology_endpoint_destroyed',
                                  'worker_exit', 'topology_worker_exit',
                                  'topology_generation_retired', 'worker_stopping'])
def test_retirement_before_interval_close_is_not_normal_cleanup(at, event):
    events, writer, reader = sample()
    append(events, reader, event, at)
    observation = evaluate(events, writer, reader)
    assert observation['status'] == 'unknown', observation
    assert observation['effect_observed'] is None


def test_new_generation_after_window_does_not_invalidate_old_observation():
    events, writer, reader = sample()
    successor = {**reader, 'generation': 2, 'endpoint_generation': 'r-next',
                 'pid': 12, 'process_start_ticks': 22, 'endpoint_gid': 'cc'}
    append(events, reader, 'topology_generation_retired', 30)
    append(events, successor, 'endpoint_created', 31)
    assert evaluate(events, writer, reader)['status'] == 'fixture_observed'


def test_other_generation_within_window_is_rejected():
    events, writer, reader = sample()
    successor = {**reader, 'generation': 2, 'endpoint_generation': 'r-next',
                 'pid': 12, 'process_start_ticks': 22, 'endpoint_gid': 'cc'}
    append(events, successor, 'endpoint_created', 14)
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_cleanup_callback_after_window_is_excluded_but_unclocked_callback_is_not():
    events, writer, reader = sample()
    cleanup = deepcopy(next(e for e in events if e.get('event_kind') == 'liveliness_changed'))
    cleanup.update(event_index=len(events), monotonic_ns=30, callback_monotonic_ns=29,
                   during_cleanup=True)
    events.append(cleanup)
    assert evaluate(events, writer, reader)['status'] == 'fixture_observed'
    cleanup.pop('callback_monotonic_ns')
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_callback_buffered_past_close_still_invalidates_its_actual_window():
    events, writer, reader = sample()
    delayed = deepcopy(next(e for e in events if e.get('event_kind') == 'liveliness_changed'))
    delayed.update(event_index=len(events), monotonic_ns=30, callback_monotonic_ns=20,
                   during_cleanup=True)
    events.append(delayed)
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_second_assertion_has_independent_interval_and_refs():
    events, writer, reader = sample()
    cycle = [deepcopy(e) for e in events if e['event'] not in
             ('topology_provenance', 'endpoint_created')]
    for row in cycle:
        row['monotonic_ns'] += 30
        if 'callback_monotonic_ns' in row:
            row['callback_monotonic_ns'] += 30
        if 'command_id' in row:
            row['command_id'] = 2
        for field in ('coverage_start_ns', 'coverage_end_ns'):
            if field in row:
                row[field] += 30
        row['event_index'] = len(events)
        events.append(row)
    first = evaluate(events, writer, reader, assertion_command_id=1)
    second = evaluate(events, writer, reader, assertion_command_id=2)
    assert first['status'] == second['status'] == 'fixture_observed'
    assert first['refs']['assert'] != second['refs']['assert']
    assert first['refs']['peer_coverage'] != second['refs']['peer_coverage']
    # Omitting the command selector remains fail-closed when ambiguous.
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_final_facts_uses_writer_topic_and_worker_alias():
    events, writer, reader = sample()
    for row in events:
        if row.get('logical_id') == 'w':
            row.update(role='writer1', topic='/same')
        elif row.get('logical_id') == 'r':
            row.update(role='reader1', topic='/same')
    foreign = {**reader, 'logical_id': 'foreign', 'endpoint_gid': 'dd',
               'pid': 13, 'process_start_ticks': 23, 'endpoint_generation': 'foreign-gen'}
    append(events, foreign, 'endpoint_created', 2, topic='/other')
    observations = _liveliness_observation_facts(events, run_id='run', real_ros=False)
    assert len(observations) == 1, observations
    assert observations[0]['writer_logical_id'] == 'w'
    assert observations[0]['reader_logical_id'] == 'r'
    assert observations[0]['status'] == 'fixture_observed'


def test_normal_cleanup_does_not_replace_missing_coverage_proof():
    events, writer, reader = sample()
    events[-1]['event'] = 'graph_snapshot'
    append(events, reader, 'topology_endpoint_destroyed', 30)
    observation = evaluate(events, writer, reader)
    assert observation['status'] == 'unknown'
    assert observation['effect_observed'] is None


def test_overlapping_coverage_cannot_be_used_to_choose_a_favorable_cycle():
    events, writer, reader = sample()
    proof = deepcopy(events[-1])
    proof.update(event_index=len(events), monotonic_ns=26)
    events.append(proof)
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_foreign_clock_cannot_exclude_late_retirement():
    events, writer, reader = sample()
    retirement = append(events, reader, 'topology_worker_exit', 30)
    retirement['monotonic_clock_identity']['inode'] += 1
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_callback_time_not_flush_time_controls_window_membership():
    events, writer, reader = sample()
    callback = deepcopy(next(e for e in events if e.get('event_kind') == 'liveliness_changed'))
    callback.update(event_index=len(events), monotonic_ns=30, callback_monotonic_ns=20)
    events.append(callback)
    assert evaluate(events, writer, reader)['status'] == 'unknown'


def test_initial_join_callback_before_coverage_is_not_a_recovery():
    events, writer, reader = sample()
    initial = deepcopy(next(e for e in events if e.get('event_kind') == 'liveliness_changed'))
    initial.update(event_index=3, monotonic_ns=2, callback_monotonic_ns=2,
                   status_fields={'alive_count': 1, 'not_alive_count': 0,
                                  'alive_count_change': 1, 'not_alive_count_change': 0})
    events.insert(3, initial)
    for index, row in enumerate(events):
        row['event_index'] = index
    assert evaluate(events, writer, reader)['status'] == 'fixture_observed'


@pytest.mark.parametrize('selector', [True, False, 0, -1, '1'])
def test_command_selector_requires_positive_integer(selector):
    events, writer, reader = sample()
    assert evaluate(events, writer, reader,
                    assertion_command_id=selector)['status'] == 'unknown'


def test_final_facts_does_not_bind_assertion_to_forged_writer_process():
    events, writer, reader = sample()
    for row in events:
        if row.get('logical_id') in ('w', 'r'):
            row['topic'] = '/same'
    assertion = next(e for e in events if e['event'] == 'liveliness_asserted')
    assertion['process_start_ticks'] += 1
    observations = _liveliness_observation_facts(events, run_id='run', real_ros=False)
    assert len(observations) == 1
    assert observations[0]['reason'] == 'writer_endpoint_token_missing_or_ambiguous'
    assert observations[0]['effect_observed'] is None
