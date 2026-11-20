"""Episode replay status must compare its comparison report, not absent execution keys."""
from copy import deepcopy
import pytest
from r2f.campaign.episode import run_episode
from r2f.campaign.episode_replay import replay_episode, _compare_full
from test_episode_replay import program, Clock, failing_backend, original_execution


def test_matching_full_replay_is_not_always_diverged():
    ep = program()
    original = original_execution(ep)
    report = replay_episode(ep, original, mode='health', perform=failing_backend(), clock=Clock(), reduce=False)
    assert report['full']['action_sequence_equal'] is True
    assert report['status'] == 'full_replay'


def test_same_stop_and_action_but_different_rule_is_diverged():
    ep = program()
    original = original_execution(ep)
    report = replay_episode(ep, original, mode='health', perform=failing_backend(rule='different-rule'), clock=Clock(), reduce=False)
    assert report['status'] == 'full_replay_diverged'
    assert report['full']['attribution_equal'] is False


def test_shortened_prefix_is_not_a_full_action_sequence_match():
    ep = program()
    original = original_execution(ep)
    shortened = deepcopy(original)
    shortened['receipts'] = shortened['receipts'][:-1]
    comparison = _compare_full(ep, shortened, original)
    assert comparison['action_sequence_equal'] is False


def test_same_tool_failure_replay_has_separate_matching_status():
    ep = program(1)
    def broken(item, current):
        raise RuntimeError('controlled failure')
    original = run_episode(ep, perform=broken, clock=Clock())
    report = replay_episode(ep, original, mode='tool_failure', perform=broken, clock=Clock(), reduce=False)
    assert report['status'] == 'full_replay'
    assert report['replay_kind'] == 'tool_failure'
    assert report['candidate_eligible'] is False


def test_health_replay_tool_failure_remains_harness_failure():
    ep = program()
    original = original_execution(ep)
    def broken(item, current):
        raise RuntimeError('controlled failure')
    report = replay_episode(ep, original, mode='health', perform=broken, clock=Clock(), reduce=False)
    assert report['status'] == 'health_replay_backend_tool_failure'
    assert report['candidate_eligible'] is False


@pytest.mark.parametrize('stop', ['health', 'time_limit'])
def test_zero_action_replay_stop_is_preserved_not_exception(stop):
    ep = program()
    original = original_execution(ep)
    calls = []
    def perform(item, current):
        calls.append(item)
        return failing_backend()(item, current)
    if stop == 'health':
        kwargs = dict(clock=Clock(), health=lambda action, state: dict(healthy=False, reason='controlled'))
    else:
        values = iter([0.0, 100.0, 100.1])
        kwargs = dict(clock=lambda: next(values))
    report = replay_episode(ep, original, mode='health', perform=perform, reduce=False, **kwargs)
    assert calls == []
    assert report['full_execution']['receipts'] == []
    assert report['status'] == 'full_replay_diverged'
    assert report['full']['attribution_equal'] is False
