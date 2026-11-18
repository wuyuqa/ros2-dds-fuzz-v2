"""Offline episode-replay contracts: full replay, reduction, attribution safety.

Every backend is an injected fake.  These tests are not ROS execution, not a
campaign run, not a candidate and not S1 maturity evidence.
"""
from copy import deepcopy

import pytest

from r2f.campaign.episode import (Episode, EpisodeAction, EpisodeLimits,
                                  run_episode)
from r2f.campaign.episode_replay import (EpisodeReplayError, attribution_signature,
                                         reduce_actions, replay_attempt_record,
                                         replay_episode)
from r2f.core.evidence import canonical_hash


def state(**overrides):
    value = {"live_roles": "both", "creation_order_class": "reader_first",
             "compatibility_class": "compatible", "observed_match_class": "bidirectional",
             "network_phase": "normal", "recovery_cycle_bucket": "0",
             "delivery_closure_class": "closed_within_contract",
             "observation_completeness": "complete"}
    value.update(overrides)
    return value


def entry(index):
    return {"target_id": "discovery.late_endpoint", "target_revision": 0,
            "rmw": "rmw_cyclonedds_cpp", "contract_hash": "contract-v1",
            "surface_id": "late_joiner.reader_delay", "boundary_class": "finite",
            "final_config": {"reader_delay_sec": index / 1000}}


def action(sequence, *, depends_on=None, provides=None, requires=None):
    return EpisodeAction(sequence=sequence, action="pair_step", input_entry=entry(sequence),
                         parameters={"step": sequence}, depends_on=list(depends_on or []),
                         provides=list(provides or []), requires=list(requires or []))


def program(count=4, *, allowed=None):
    actions = [action(index) for index in range(count)]
    return Episode(episode_id="ep-replay-001", target_id="discovery.late_endpoint",
                   target_revision=0, rmw="rmw_cyclonedds_cpp",
                   contract_hash="contract-v1", actions=actions,
                   initial_state=state(), limits=EpisodeLimits(max_actions=50, max_seconds=60),
                   allowed_state_ids=allowed)


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        self.value += 0.1
        return self.value


def failing_backend(fail_at=3, *, verdict="fail", rule="R-LATE-1"):
    def perform(item, current):
        if item.sequence == fail_at:
            return {"status": "failed", "execution_stage": "judged",
                    "after_state": state(observed_match_class="none"),
                    "observed_effect": ["unexpected_no_match"],
                    "oracle_verdict": verdict, "rule_id": rule,
                    "reason_code": "late_reader_not_matched"}
        return {"status": "executed", "execution_stage": "judged",
                "after_state": state(), "observed_effect": [],
                "oracle_verdict": "unknown", "rule_id": "R-LATE-1"}
    return perform


def original_execution(program_, *, fail_at=3):
    return run_episode(program_, perform=failing_backend(fail_at), clock=Clock())


def test_full_replay_keeps_order_and_records_logical_timeline():
    episode = program()
    original = original_execution(episode)
    report = replay_episode(episode, original, mode="health",
                            perform=failing_backend(3), clock=Clock(), reduce=True)
    assert report["schema"] == "r2f.s1-episode-replay/v1"
    assert report["replay_kind"] == "health"
    assert report["full"]["replayed_actions"] == ["a0000", "a0001", "a0002", "a0003"]
    assert report["full"]["logical_order_preserved"] is True
    assert report["full"]["os_concurrency_claimed"] is False
    assert [item["logical_order"] for item in report["full"]["timeline"]] == [0, 1, 2, 3]


def test_reduction_keeps_terminal_action_and_attribution_signature():
    episode = program()
    original = original_execution(episode)
    reduction = reduce_actions(episode, original)
    assert reduction["action_ids"] == ["a0003"]
    report = replay_episode(episode, original, mode="health",
                            perform=failing_backend(3), clock=Clock(), reduce=True)
    reduced = report["reduced"]
    assert reduced["accepted"] is True
    assert reduced["attribution"]["equal"] is True
    assert reduced["reduced_action_count"] <= reduced["original_action_count"]
    assert report["original_attribution"] == attribution_signature(original)


def test_dependency_preserving_reduction_drops_unrelated_prefix():
    # a0000 is unrelated, a0001 provides a symbol, a0002 requires it, a0003 fails.
    actions = [action(0),
               action(1, provides=["pair-ready"]),
               action(2, depends_on=[1], requires=["pair-ready"]),
               action(3, depends_on=[2])]
    episode = Episode(episode_id="ep-replay-002", target_id="discovery.late_endpoint",
                      target_revision=0, rmw="rmw_cyclonedds_cpp",
                      contract_hash="contract-v1", actions=actions, initial_state=state(),
                      limits=EpisodeLimits(max_actions=50, max_seconds=60))
    original = run_episode(episode, perform=failing_backend(3), clock=Clock())
    reduction = reduce_actions(episode, original)
    assert "a0000" not in reduction["action_ids"]
    assert reduction["action_ids"] == ["a0001", "a0002", "a0003"]
    report = replay_episode(episode, original, mode="health",
                            perform=failing_backend(3), clock=Clock(), reduce=True)
    assert report["reduced"]["accepted"] is True


def test_reduction_that_changes_anomaly_attribution_is_rejected_not_rewritten():
    episode = program()
    original = original_execution(episode)
    # The reduced replay fails at a different action, so attribution differs.
    report = replay_episode(episode, original, mode="health",
                            perform=failing_backend(1), clock=Clock(), reduce=False)
    assert report["replay_kind"] == "health"
    assert report["full_execution"]["stop_details"]["action_id"] == "a0001"
    # Passing the same divergent backend through a reduction must not be
    # accepted as an equivalent anomaly.
    divergent = replay_episode(episode, original, mode="health",
                               perform=failing_backend(1), clock=Clock(), reduce=True)
    assert divergent["reduced"]["accepted"] is False
    assert divergent["reduced"]["attribution"]["equal"] is False
    # The original execution hash is untouched by the failed rerun.
    assert original["execution_sha256"] == canonical_hash(
        {k: v for k, v in original.items() if k != "execution_sha256"})


def test_health_replay_never_reports_tool_failure():
    episode = program(2)
    original = original_execution(episode, fail_at=1)
    def broken(item, current):
        raise RuntimeError("no backend")
    report = replay_episode(episode, original, mode="health", perform=broken,
                            clock=Clock(), reduce=False)
    assert report["status"] == "health_replay_backend_tool_failure"
    assert report["candidate_eligible"] is False
    assert report["responsibility_layer"] == "harness"


def test_tool_failure_replay_is_separate_harness_evidence_and_not_candidate():
    episode = program(2)
    def broken(item, current):
        raise RuntimeError("backend unavailable")
    original = run_episode(episode, perform=broken, clock=Clock())
    report = replay_episode(episode, original, mode="tool_failure", perform=broken,
                            clock=Clock(), reduce=True)
    assert report["replay_kind"] == "tool_failure"
    assert report["candidate_eligible"] is False
    assert report["responsibility_layer"] == "harness"
    assert report["candidate_writes"] == 0
    attempt = replay_attempt_record(report, attempt_id="attempt-1")
    assert attempt["status"] == "inconclusive"
    assert attempt["live_ros_claimed"] is False
    assert attempt["candidate_eligible"] is False
    assert attempt["replay_kind"] == "tool_failure"


def test_health_replay_also_stays_offline_inconclusive():
    episode = program(2)
    original = original_execution(episode, fail_at=1)
    report = replay_episode(episode, original, mode="health",
                            perform=failing_backend(1), clock=Clock(), reduce=False)
    attempt = replay_attempt_record(report, attempt_id="attempt-2")
    assert attempt["live_ros_claimed"] is False
    assert attempt["status"] == "inconclusive"


def test_original_tamper_or_wrong_episode_is_rejected_before_callable_runs():
    episode = program(2)
    original = original_execution(episode, fail_at=1)
    damaged = deepcopy(original)
    damaged["stop_reason"] = "rewritten"
    with pytest.raises(EpisodeReplayError):
        replay_episode(episode, damaged, mode="health",
                       perform=lambda a, s: pytest.fail("must not run"), clock=Clock())
    other = program(2)
    other.episode_id = "ep-other-001"
    with pytest.raises(EpisodeReplayError):
        replay_episode(other, original, mode="health",
                       perform=lambda a, s: pytest.fail("must not run"), clock=Clock())


def test_reduction_rejects_unknown_or_cyclic_dependencies():
    episode = program(3)
    original = original_execution(episode, fail_at=2)
    tampered = deepcopy(episode)
    tampered.actions[-1].requires = ["never-provided"]
    tampered.__post_init__()
    with pytest.raises(EpisodeReplayError):
        reduce_actions(tampered, original)
