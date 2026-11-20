"""Synchronous action-commit seam: callbacks run before the next real action."""
from copy import deepcopy
import pytest
from test_episode import episode, action, Clock, ok_perform
from r2f.campaign.episode import EpisodeError, run_episode


def test_action_commits_are_synchronous_and_copy_isolated():
    ep = episode(actions=[action(0), action(1)])
    order = []
    def perform(item, state):
        order.append(('perform', item.sequence))
        return ok_perform()
    def commit(receipt, receipts):
        order.append(('commit', receipt['sequence']))
        assert len(receipts) == receipt['sequence'] + 1
        receipt['sequence'] = 999
        receipts.clear()
    result = run_episode(ep, perform=perform, clock=Clock(), on_receipt=commit)
    assert order == [('perform', 0), ('commit', 0), ('perform', 1), ('commit', 1)]
    assert [r['sequence'] for r in result['receipts']] == [0, 1]


def test_checkpoint_error_stops_future_side_effects_and_keeps_committed_action():
    ep = episode(actions=[action(0), action(1)])
    calls = []
    def perform(item, state):
        calls.append(item.sequence)
        return ok_perform()
    def commit(receipt, receipts):
        raise OSError('checkpoint disk failed')
    result = run_episode(ep, perform=perform, clock=Clock(), on_receipt=commit)
    assert calls == [0]
    assert result['stop_reason'] == 'receipt_observer_tool_failure'
    assert len(result['receipts']) == 1
    assert result['receipts'][0]['status'] == 'executed'
    assert result['stop_details']['responsibility_layer'] == 'harness'


def test_tool_failure_also_commits_before_terminal_record():
    committed = []
    def perform(item, state):
        raise RuntimeError('controlled tool failure')
    result = run_episode(episode(), perform=perform, clock=Clock(),
                         on_receipt=lambda r, rs: committed.append(r))
    assert len(committed) == 1 and committed[0]['tool_error'] is True
    assert result['stop_reason'] == 'tool_failure'


def test_observation_payload_is_retained_but_never_promoted_to_pass():
    observation = {'raw_path': '/fixture/raw', 'state_quality': 'incomplete'}
    def perform(item, state):
        return ok_perform(observation=observation)
    result = run_episode(episode(), perform=perform, clock=Clock())
    assert result['receipts'][0]['observation'] == observation
    assert result['receipts'][0]['oracle_verdict'] == 'unknown'
    observation['state_quality'] = 'not-a-real-update'
    assert result['receipts'][0]['observation']['state_quality'] == 'incomplete'


def test_default_no_callback_does_not_add_execution_fields():
    ep = episode()
    first = run_episode(ep, perform=lambda a,s: ok_perform(), clock=Clock())
    second = run_episode(ep, perform=lambda a,s: ok_perform(), clock=Clock(), on_receipt=None)
    assert first == second
    assert 'observation' not in first['receipts'][0]


def test_invalid_commit_callback_rejected_before_execution():
    with pytest.raises(EpisodeError):
        run_episode(episode(), perform=lambda a,s: ok_perform(), clock=Clock(), on_receipt=True)
