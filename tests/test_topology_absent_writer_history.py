"""Absent declared writer is not a recreated generation; offline raw only."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from r2f.campaign.topology_adapter import _recreated_history_windows
from test_topology_absent_reader_obligation import program, delete_action, assert_offline_pass
from test_topology_no_obligation_facts import produce


@pytest.fixture(autouse=True)
def forbid_ros(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('offline absent-writer test must not start ROS')
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare', forbidden)


def test_deleted_writer_create_has_no_generation_and_no_history_credit(tmp_path):
    p = program(readers=2, writers=2)
    delete_action(p, 'create_w1')
    # The fixture's publish has no explicit dependency; remove its now-invalid
    # action as the production dependency-aware action.delete mutator does.
    delete_action(p, 'publish_w1')
    runtime, config = produce(tmp_path, program=p)
    facts = assert_offline_pass(runtime, config)
    absent = [r for r in facts['pairs'] if r['writer'] == 'w1']
    assert absent and all(r['writer_generation'] is None for r in absent)
    assert all(r['delivery_closure'] == 'not_applicable' for r in absent)
    assert not facts['historical_send_coverage']


def test_declared_unscheduled_writer_is_not_invented_as_generation_one(tmp_path):
    p = program(readers=1, writers=1)
    writer = deepcopy(next(e for e in p['endpoints'] if e['role'] == 'writer'))
    writer['logical_id'] = 'w1'
    p['endpoints'].append(writer)
    runtime, config = produce(tmp_path, program=p)
    facts = assert_offline_pass(runtime, config)
    assert not any(e.get('logical_id') == 'w1' and e['event'] == 'endpoint_created' for e in runtime['events'])
    assert all(r['writer_generation'] is None for r in facts['pairs'] if r['writer'] == 'w1')


@pytest.mark.parametrize('generation', [None, True, False, 0, 1, -1, '2', 2.0])
def test_non_recreated_generation_cannot_enter_history_certification(generation):
    class Audit:
        program = {}; path = 'offline'; sha = 'a' * 64; events = []; binding = {}
        def ref(self, event): return {}
        def resolve(self, ref): return {}
    pair = {'writer':'w0', 'reader':'r0', 'writer_generation':generation,
            'proofs':{'delivery_closure':{}}}
    with patch('r2f.campaign.topology_state_projection._Projection') as projection:
        projection.return_value.recreated_no_sends.side_effect = AssertionError('must not certify')
        assert _recreated_history_windows(Audit(), [pair], {'receipt':{'after':{'boundary_ref':{}}}}, []) == []
