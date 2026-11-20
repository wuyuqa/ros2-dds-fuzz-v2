"""Offline canonical application audit for explicitly grouped controller actions."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from r2f.modules.s1 import topology_model as model
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_route import audit_topology_application
from test_topology_concurrent_publish import SerialGroupFixture, program
from test_topology_runtime_route import config_for, rewrite_raw


def fixture_run(tmp_path):
    p = program()
    with patch.object(model, 'generate_program', return_value=p):
        plan, config = config_for()
    with patch('r2f.runtime.topology_worker.RosTopologyBackend.prepare',
               side_effect=AssertionError('ROS forbidden in fixture')):
        result = run_pair(config, tmp_path, topology_backend=SerialGroupFixture())
    return plan, config, result


def test_grouped_action_trace_retains_canonical_application_proof(tmp_path):
    plan, config, result = fixture_run(tmp_path)
    audit = audit_topology_application(plan, config['input_layer_receipts'], result)
    assert audit['status'] == 'fixture_application_observed', [a['reasons'] for a in audit['layers']]
    assert all(not a['hit'] for a in audit['layers'])
    assert not audit['execution_evidence'] and audit['fixture']


@pytest.mark.parametrize('damage', ['missing_ready', 'foreign_worker', 'done_action',
                                    'done_order', 'unrelated_overlap', 'before_group_begin',
                                    'early_group_done'])
def test_grouped_trace_exception_requires_its_exact_raw_contract(tmp_path, damage):
    plan, config, result = fixture_run(tmp_path)
    def corrupt(events):
        events = deepcopy(events)
        if damage == 'missing_ready':
            return [e for e in events if e['event'] != 'concurrent_publish_ready']
        if damage == 'foreign_worker':
            next(e for e in events if e['event'] == 'concurrent_publish_ready')['pid'] = 999999
        elif damage == 'done_action':
            next(e for e in events if e['event'] == 'topology_action_done')['action'] = {}
        elif damage == 'done_order':
            indices = [i for i, e in enumerate(events) if e['event'] == 'topology_action_done']
            events[indices[-1]], events[indices[-2]] = events[indices[-2]], events[indices[-1]]
        elif damage in {'before_group_begin', 'early_group_done'}:
            # Keep controller action order but forge the surrounding raw block.
            kind = ('concurrent_publish_requested' if damage == 'before_group_begin'
                    else 'topology_action_done')
            i = next(i for i, e in enumerate(events) if e['event'] == kind
                     and (kind != 'topology_action_done'
                          or e['action']['params'].get('concurrency_group')))
            moved = events.pop(i)
            marker = ('topology_action_begin' if damage == 'before_group_begin'
                      else 'concurrent_publish_finished')
            where = next(i for i, e in enumerate(events) if e['event'] == marker
                         and (marker != 'topology_action_begin'
                              or e['action']['params'].get('concurrency_group')))
            events.insert(where, moved)
        elif damage == 'unrelated_overlap':
            # Move a later ordinary snapshot begin into the concurrent block.
            i = next(i for i, e in enumerate(events) if e['event'] == 'topology_action_begin'
                     and e['action']['op'] == 'snapshot')
            begin = events.pop(i)
            where = next(i for i, e in enumerate(events) if e['event'] == 'concurrent_publish_requested')
            events.insert(where, begin)
        return events
    rewrite_raw(result, corrupt)  # Copies/fixtures only; never a real capture.
    audit = audit_topology_application(plan, config['input_layer_receipts'], result)
    assert audit['status'] not in {'application_observed', 'fixture_application_observed'}
    assert not any(a['hit'] for a in audit['layers'])
