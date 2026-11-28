"""Consumer-path regressions for exact participant-control captures.

Protocol fixtures are offline and never establish ROS execution or qualification.
"""
from copy import deepcopy

import pytest

from r2f.runtime.topology_pair import _execution_workers
from r2f.runtime.participant_groups import project_lineage
from test_topology_participant_control_public import group_fixture, resave


def test_worker_projection_consumes_exact_group_lineage(tmp_path):
    fixture = group_fixture(tmp_path, shared=True)
    workers = _execution_workers(fixture.events, fixture.raw, fixture.program)
    assert len(workers) == 2
    assert {row["logical_id"] for row in workers} == {
        endpoint["logical_id"] for endpoint in fixture.program["endpoints"]
    }
    assert all(row["separate_context"] is False for row in workers)
    assert all(row["unknowns"] == ["offline_fixture_not_actual_worker"] for row in workers)


def test_saved_lineage_rejects_group_process_exit_identity_drift(tmp_path):
    fixture = group_fixture(tmp_path, shared=True)
    exit_row = next(row for row in fixture.events if row["event"] == "process_exit")
    exit_row["worker_instance_id"] = "forged-process"
    resave(fixture)
    from r2f.runtime.participant_groups import lineage_metadata
    lineage = lineage_metadata(fixture.program, fixture.events, str(fixture.raw), config=fixture.config)
    assert lineage.get("error") == "group_lineage:group_exit"
    assert not lineage["workers"]


def test_legacy_worker_projection_keeps_existing_contract(tmp_path):
    from test_topology_adapter import RawFixture
    fixture = RawFixture().save(tmp_path)
    workers = _execution_workers(fixture.events, fixture.raw)
    assert len(workers) == 2
    assert all("group_id" not in row for row in workers)




def group_exit_boundary(fixture):
    first_cleanup = min(row['event_index'] for row in fixture.events
                        if row.get('event') == 'endpoint_destroyed')
    return {'event_index': first_cleanup - 1,
            'monotonic_ns': fixture.events[first_cleanup - 1]['monotonic_ns']}


def consume_group_termination(fixture):
    from r2f.runtime.topology_pair import group_termination_rows
    tokens = [row for row in fixture.events if row.get('event') == 'endpoint_created']
    return group_termination_rows(fixture.program, fixture.events, fixture.raw, tokens,
                                  group_exit_boundary(fixture))


@pytest.mark.parametrize('shared', [True, False])
def test_saved_group_exit_audit_consumes_actual_shutdown_and_distinct_endpoint_cleanup(tmp_path, shared):
    fixture = group_fixture(tmp_path, shared=shared)
    original = fixture.raw.read_bytes()
    rows = consume_group_termination(fixture)
    expected_groups = 1 if shared else 2
    assert sum(row['event'] == 'process_exit' for row in rows) == expected_groups
    assert sum(row['event'] == 'group_context_shutdown' for row in rows) == expected_groups
    assert sum(row['event'] == 'endpoint_destroyed' for row in rows) == 2
    assert all('logical_id' not in row for row in rows if row['event'] == 'process_exit')
    assert fixture.raw.read_bytes() == original


@pytest.mark.parametrize('damage', ['duplicate_exit', 'failed_exit', 'missing_shutdown', 'shutdown_identity',
                                    'exit_identity', 'cleanup_context', 'missing_cleanup'])
def test_saved_group_exit_audit_rejects_incomplete_or_forged_closure(tmp_path, damage):
    fixture = group_fixture(tmp_path, shared=True)
    exit_row = next(row for row in fixture.events if row.get('event') == 'process_exit')
    cleanup = next(row for row in fixture.events if row.get('event') == 'endpoint_destroyed')
    shutdown = next(row for row in fixture.events if row.get('event') == 'group_context_shutdown')
    if damage == 'duplicate_exit':
        duplicate = deepcopy(exit_row)
        duplicate['event_index'] = len(fixture.events)
        duplicate['monotonic_ns'] += 1
        fixture.events.append(duplicate)
    elif damage == 'failed_exit':
        exit_row['returncode'] = 9
    elif damage == 'missing_shutdown':
        shutdown['event'] = 'missing_shutdown'
    elif damage == 'shutdown_identity':
        shutdown['context_instance_id'] = 'foreign'
    elif damage == 'exit_identity':
        exit_row['worker_instance_id'] = 'foreign'
    elif damage == 'cleanup_context':
        cleanup['context_instance_id'] = 'foreign'
    else:
        cleanup['event'] = 'missing_cleanup'
    with pytest.raises(ValueError):
        consume_group_termination(fixture)
