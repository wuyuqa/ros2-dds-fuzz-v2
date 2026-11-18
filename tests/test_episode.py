"""Offline episode contracts: bounded action history, guards, resume store.

All backends here are injected fake clocks/callables.  Nothing in this file
starts ROS, writes a required-matrix cell, creates a candidate or claims S1
maturity.
"""
from copy import deepcopy
import json

import pytest

from r2f.campaign.episode import (DEFAULT_MAX_ACTIONS, Episode, EpisodeAction, EpisodeError,
                                  EpisodeHistoryError, EpisodeLimits, EpisodeStore,
                                  EpisodeStoreError, corpus_offer_kwargs, run_episode)
from r2f.core.evidence import canonical_hash
from r2f.core.state_machine import StateCondition
from r2f.core.state_signature import state_id


def state(**overrides):
    value = {"live_roles": "both", "creation_order_class": "reader_first",
             "compatibility_class": "compatible", "observed_match_class": "bidirectional",
             "network_phase": "normal", "recovery_cycle_bucket": "0",
             "delivery_closure_class": "closed_within_contract",
             "observation_completeness": "complete"}
    value.update(overrides)
    return value


def input_entry(index=1):
    return {"target_id": "discovery.late_endpoint", "target_revision": 0,
            "rmw": "rmw_cyclonedds_cpp", "contract_hash": "contract-v1",
            "surface_id": "late_joiner.reader_delay", "boundary_class": "finite",
            "final_config": {"reader_delay_sec": index / 1000}}


def action(sequence, *, depends_on=None, provides=None, requires=None, guard=None,
           expected_effect=None):
    return EpisodeAction(sequence=sequence, action="pair_step",
                         input_entry=input_entry(sequence + 1),
                         parameters={"step": sequence},
                         depends_on=list(depends_on or []),
                         provides=list(provides or []),
                         requires=list(requires or []),
                         guard=list(guard or []),
                         expected_effect=list(expected_effect or ["no_residual"]))


def episode(actions=None, *, allowed=None, parents=None, limits=None, episode_id="ep-fixture-001"):
    return Episode(episode_id=episode_id, target_id="discovery.late_endpoint",
                   target_revision=0, rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                   actions=list(actions or [action(0)]), initial_state=state(),
                   limits=limits or EpisodeLimits(max_actions=10, max_seconds=60),
                   allowed_state_ids=allowed,
                   parent_episode_ids=list(parents or []))


class Clock:
    def __init__(self, step=0.25):
        self.value = 0.0
        self.step = step

    def __call__(self):
        self.value += self.step
        return self.value


def ok_perform(after=None, *, verdict="unknown", **extra):
    result = {"status": "executed", "execution_stage": "judged",
              "after_state": after or state(), "observed_effect": [],
              "oracle_verdict": verdict, "rule_id": "R-FIXTURE-1"}
    result.update(extra)
    return result


def test_bounded_defaults_action_program_and_identity_excludes_execution_fields():
    program = episode([action(0), action(1, depends_on=[0], provides=["p1"]),
                       action(2, depends_on=[1], requires=["p1"])])
    assert program.limits.max_actions == 10
    assert DEFAULT_MAX_ACTIONS == 50
    document = program.to_dict()
    assert document["input_sha256"] == program.input_identity()
    # execution-only labels never split episode identity
    spaced = deepcopy(document)
    spaced["tags"] = {"note": "changed"}
    assert spaced["input_sha256"] == program.input_identity()
    assert Episode.from_dict(document).to_dict() == document


def test_more_than_fifty_actions_and_nonmonotonic_sequences_are_rejected():
    actions = [action(index) for index in range(51)]
    with pytest.raises(EpisodeError):
        episode(actions, limits=EpisodeLimits(max_actions=50, max_seconds=60))
    with pytest.raises(EpisodeError):
        episode([action(1), action(0)], limits=EpisodeLimits(max_actions=50, max_seconds=60))
    with pytest.raises(EpisodeError):
        episode([action(1, depends_on=[1])], limits=EpisodeLimits(max_actions=5, max_seconds=60))


def test_action_requires_deterministic_input_identity():
    entry = input_entry()
    entry.pop("contract_hash")
    with pytest.raises(ValueError):
        EpisodeAction(sequence=0, action="pair_step", input_entry=entry)
    with pytest.raises(ValueError):
        EpisodeAction(sequence=0, action="pair_step", input_entry=dict(input_entry(),
                                                                      final_config={}))


def test_guard_block_is_receipted_and_stops_the_episode():
    guard = [StateCondition(condition_id="g1", fact="network_phase", expected="restored_verified")]
    program = episode([action(0, guard=guard), action(1)], limits=EpisodeLimits(max_actions=5, max_seconds=60))
    execution = run_episode(program, perform=lambda a, s: pytest.fail("blocked action must not run"),
                            clock=Clock())
    assert execution["stop_reason"] == "guard_blocked"
    assert execution["action_count"] == 1
    receipt = execution["receipts"][0]
    assert receipt["status"] == "blocked" and receipt["execution_stage"] == "aborted"
    assert receipt["guard"][0]["passed"] is False
    assert receipt["timing"]["os_concurrency_claimed"] is False


def test_max_actions_bounds_actual_execution_not_just_planning():
    program = episode([action(index) for index in range(4)],
                      limits=EpisodeLimits(max_actions=4, max_seconds=60))
    execution = run_episode(program, perform=lambda a, s: ok_perform(), clock=Clock(0.0))
    assert execution["stop_reason"] == "episode_completed"
    assert execution["action_count"] == 4


def test_elapsed_time_bound_stops_before_starting_next_action():
    clock = Clock(0.25)
    program = episode([action(0), action(1), action(2)],
                      limits=EpisodeLimits(max_actions=3, max_seconds=1.5))
    executed = []
    def perform(item, current):
        executed.append(item.sequence)
        return ok_perform()
    execution = run_episode(program, perform=perform, clock=clock)
    assert execution["stop_reason"] == "max_seconds"
    assert executed == [0, 1]
    assert execution["stop_details"]["max_seconds"] == 1.5


def test_health_memory_and_log_limits_end_the_episode_without_extra_action():
    program = episode([action(0), action(1)], limits=EpisodeLimits(
        max_actions=2, max_seconds=60, max_memory_bytes=1000, max_log_bytes=1000))

    def memory_health(item, current):
        return {"healthy": True, "memory_bytes": 1001, "log_bytes": 0}

    execution = run_episode(program, perform=lambda a, s: ok_perform(),
                            clock=Clock(0.0), health=memory_health)
    assert execution["stop_reason"] == "memory_limit"
    assert execution["action_count"] == 0

    def log_health(item, current):
        return {"healthy": True, "memory_bytes": 0, "log_bytes": 1001}

    execution = run_episode(program, perform=lambda a, s: ok_perform(),
                            clock=Clock(0.0), health=log_health)
    assert execution["stop_reason"] == "log_limit"

    def unhealthy(item, current):
        return {"healthy": False, "reason": "process_gone"}

    execution = run_episode(program, perform=lambda a, s: ok_perform(),
                            clock=Clock(0.0), health=unhealthy)
    assert execution["stop_reason"] == "unhealthy:process_gone"


def test_tool_exception_is_harness_attributed_and_never_a_candidate():
    program = episode([action(0)])

    def broken(item, current):
        raise RuntimeError("backend unavailable")

    execution = run_episode(program, perform=broken, clock=Clock())
    assert execution["stop_reason"] == "tool_failure"
    receipt = execution["receipts"][0]
    assert receipt["tool_error"] is True and receipt["status"] == "tool_error"
    assert receipt["responsibility_layer"] not in {"sut", "target"}
    assert execution["candidate_writes"] == 0
    assert execution["maturity_denominator_changed"] is False


def test_pass_without_allowed_state_model_is_refused_not_inferred():
    program = episode([action(0)], allowed=None)
    execution = run_episode(program, perform=lambda a, s: ok_perform(verdict="pass"),
                            clock=Clock(0.0))
    assert execution["stop_reason"] == "state_model_missing_for_pass"
    assert execution["state_model"] == "no_pass_claim_model"


def test_unknown_observation_cannot_expand_allowed_state_set():
    declared = state_id(state())
    outside = state(network_phase="restored_verified")
    program = episode([action(0)], allowed=[declared])
    execution = run_episode(program, perform=lambda a, s: ok_perform(after=outside,
                                                                     verdict="unknown"),
                            clock=Clock(0.0))
    assert execution["stop_reason"] == "state_outside_allowed_model"
    assert execution["receipts"][0]["oracle_verdict"] == "unknown"
    assert state_id(outside) != declared


def test_pass_inside_declared_allowed_set_completes():
    declared = [state_id(state())]
    program = episode([action(0), action(1)], allowed=declared,
                      limits=EpisodeLimits(max_actions=2, max_seconds=60))
    execution = run_episode(program, perform=lambda a, s: ok_perform(verdict="pass"),
                            clock=Clock(0.0))
    assert execution["stop_reason"] == "episode_completed"
    assert execution["receipts"][0]["rule_id"] == "R-FIXTURE-1"


def test_failure_parent_requires_complete_ancestor_history_and_store_is_append_only(tmp_path):
    store = EpisodeStore(tmp_path / "episodes")
    parent = episode([action(0)])
    parent_execution = run_episode(parent, perform=lambda a, s: ok_perform(verdict="unknown"),
                                   clock=Clock(0.0))
    stored = store.add(parent, parent_execution)
    assert store.load(parent.episode_id)["record_sha256"] == stored["record_sha256"]
    with pytest.raises(EpisodeStoreError):
        store.add(parent, parent_execution)  # append-only: no overwrite

    child = episode([action(0)], parents=[parent.episode_id], episode_id="ep-fixture-child")
    with pytest.raises(EpisodeHistoryError):
        run_episode(child, perform=lambda a, s: ok_perform(), clock=Clock(0.0))
    child_execution = run_episode(child, perform=lambda a, s: ok_perform(), clock=Clock(0.0),
                                  store=store)
    assert child_execution["parent_history"]["chain_complete"] is True
    assert [item["episode_id"] for item in child_execution["parent_history"]["ancestors"]] == [parent.episode_id]
    store.add(child, child_execution)
    view = store.view()
    assert [item["episode_id"] for item in view["records"]] == sorted(
        [parent.episode_id, child.episode_id])
    assert view["view_sha256"] == canonical_hash({k: v for k, v in view.items() if k != "view_sha256"})


def test_missing_parent_store_is_an_error_not_reconstructed():
    child = episode([action(0)], parents=["missing-parent"])
    with pytest.raises(EpisodeHistoryError):
        run_episode(child, perform=lambda a, s: ok_perform(), clock=Clock(0.0),
                    store=EpisodeStore("/nonexistent-fixture-store"))


def test_store_rejects_tampered_record(tmp_path):
    store = EpisodeStore(tmp_path)
    program = episode([action(0)])
    execution = run_episode(program, perform=lambda a, s: ok_perform(), clock=Clock(0.0))
    record = store.add(program, execution)
    path = tmp_path / "episodes" / (program.episode_id + ".json")
    data = json.loads(path.read_text())
    data["execution"]["stop_reason"] = "rewritten"
    path.write_text(json.dumps(data))
    with pytest.raises(EpisodeStoreError):
        store.load(program.episode_id)
    # original hash binding stays visible
    assert record["record_sha256"] != canonical_hash({k: v for k, v in data.items() if k != "record_sha256"})


def test_episode_record_rejects_action_tamper_and_identity_mismatch():
    document = episode([action(0), action(1, depends_on=[0])]).to_dict()
    document["actions"][1]["parameters"]["step"] = 99
    with pytest.raises(EpisodeError):
        Episode.from_dict(document)
    document = episode([action(0)]).to_dict()
    document["input_sha256"] = "a" * 64
    with pytest.raises(EpisodeError):
        Episode.from_dict(document)


def test_corpus_projection_is_inert_and_does_not_create_candidate():
    item = action(0)
    projection = corpus_offer_kwargs(item, oracle_verdict="unknown")
    assert projection["executed"] is False
    assert projection["entry"]["action_id"] == item.action_id
    assert projection["entry"]["surface_id"] == "late_joiner.reader_delay"
    assert projection["entry"]["observation_complete"] is False
    assert projection["candidate_id"] is None


def test_isolation_mode_is_declared_and_per_case_cells_are_never_replaced():
    from r2f.campaign.episode import ISOLATION_MODES, coexistence_guard
    assert ISOLATION_MODES == ("per_case", "episode")
    continuous = episode([action(0)])
    assert continuous.isolation_mode == "per_case"
    assert continuous.to_dict()["isolation_mode"] == "per_case"
    # A different isolation mode is a different deterministic input identity.
    episodic = Episode(episode_id="ep-fixture-001", target_id="discovery.late_endpoint",
                       target_revision=0, rmw="rmw_cyclonedds_cpp",
                       contract_hash="contract-v1", actions=[action(0)],
                       initial_state=state(), isolation_mode="episode")
    assert episodic.input_identity() != continuous.input_identity()
    cells = ["rmw_cyclonedds_cpp.mutated.0", "rmw_cyclonedds_cpp.baseline.1"]
    receipt = coexistence_guard(continuous, cells)
    assert receipt["per_case_cells_replaced"] is False
    assert receipt["episode_substitutes_per_case"] is False
    assert receipt["per_case_cells"] == cells
    assert receipt["receipt_sha256"] == canonical_hash(
        {k: v for k, v in receipt.items() if k != "receipt_sha256"})
    with pytest.raises(EpisodeError):
        coexistence_guard(continuous, cells + cells)
    with pytest.raises(EpisodeError):
        Episode(episode_id="ep-bad-mode", target_id="discovery.late_endpoint",
                target_revision=0, rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                actions=[action(0)], initial_state=state(), isolation_mode="global")


def test_episode_actions_project_into_existing_corpus_contract():
    """Episode actions feed the existing Corpus API; no second index is built."""
    from r2f.campaign.corpus import Corpus, content_identity
    corpus = Corpus(capacity=8, bucket_limit=3)
    parent = None
    for index in range(3):
        item = EpisodeAction(sequence=index, action="pair_step", input_entry=input_entry(index),
                             parameters={"step": index, "boundary_class": f"b{index}"})
        projection = corpus_offer_kwargs(item)
        entry = projection["entry"]
        entry["surface_id"] = "late_joiner.reader_delay"
        entry["boundary_class"] = f"b{index}"
        assert content_identity(entry) == item.input_sha256
        if parent is not None:
            entry["parent_content_sha256"] = parent
        outcome = corpus.offer(entry, executed=True, evidence_complete=True,
                               observed_edge=f"edge-{index}", oracle_verdict="unknown")
        assert outcome["admitted"] is True, outcome
        parent = outcome["content_sha256"]
    # Corpus remains the single retention owner: candidate set stays empty.
    assert corpus.candidates == {}
    assert all(record.get("candidate_ids") == [] for record in corpus.entries.values())
    assert corpus.to_dict()["entry_count"] == 3


def test_store_view_is_plain_json_and_reloads_identically_for_resume(tmp_path):
    """The view is JSON-only so a checkpoint continuation can carry it."""
    import json as _json
    store = EpisodeStore(tmp_path)
    parent = episode([action(0)])
    store.add(parent, run_episode(parent, perform=lambda a, s: ok_perform(), clock=Clock(0.0)))
    child = episode([action(0)], parents=[parent.episode_id], episode_id="ep-fixture-002")
    store.add(child, run_episode(child, perform=lambda a, s: ok_perform(), clock=Clock(0.0),
                                 store=store))
    view = store.view()
    encoded = _json.dumps(view, sort_keys=True, allow_nan=False)
    assert _json.loads(encoded) == view
    # A resumed process reads the same records from the same root and derives
    # the same ancestor chain and view identity; nothing is re-admitted.
    resumed = EpisodeStore(tmp_path)
    assert resumed.view() == view
    chain = resumed.ancestor_chain([child.episode_id])
    assert [item["episode_id"] for item in chain] == [parent.episode_id, child.episode_id]
    # The view excludes raw paths and timing, so it cannot leak execution data
    # into a deterministic checkpoint continuation.
    assert "raw_path" not in encoded and "started_monotonic" not in encoded


def _evidence(index=1):
    return {"path": "/fake/raw.jsonl", "sha256": "c" * 64, "event_index": index}


def _reset(**overrides):
    from r2f.campaign.episode import RESET_SCHEMA
    value = {"schema": RESET_SCHEMA, "kind": "verified_cleanup_rebuild",
             "cleanup_verified": True, "resources_released": True,
             "evidence": [_evidence()], "rebuild_verified": True,
             "baseline_restored": True, "restored_state_id": state_id(state())}
    value.update(overrides)
    return value


def test_verified_cleanup_rebuild_is_the_preferred_baseline_path():
    from r2f.campaign.episode import validate_reset_receipt
    accepted = validate_reset_receipt(_reset())
    assert accepted["state_origin"] == "verified_cleanup_rebuild"
    assert accepted["baseline_restored"] is True
    with pytest.raises(EpisodeError):
        validate_reset_receipt(_reset(cleanup_verified=False))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(_reset(rebuild_verified=False))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(_reset(baseline_restored=False))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(_reset(evidence=[{"path": "/x", "sha256": "b" * 64,
                                                "event_index": -1}]))


def test_runtime_recreation_never_claims_a_restored_baseline():
    from r2f.campaign.episode import validate_reset_receipt
    receipt = _reset(kind="runtime_recreated", baseline_restored=False,
                     restored_state_id=None, rebuild_verified=None,
                     runtime={"pid": 4242, "generation": "g2"})
    receipt.pop("rebuild_verified", None)
    accepted = validate_reset_receipt(receipt, previous_runtime={"pid": 4241, "generation": "g1"})
    assert accepted["state_origin"] == "fresh_runtime"
    assert accepted["baseline_restored"] is False
    assert accepted["restored_state_id"] is None
    with pytest.raises(EpisodeError):
        validate_reset_receipt(dict(receipt, baseline_restored=True))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(dict(receipt, restored_state_id="d" * 64))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(receipt, previous_runtime={"pid": 4242, "generation": "g2"})


def test_snapshot_restore_requires_identity_consistency_and_rollback_proofs():
    from r2f.campaign.episode import validate_reset_receipt
    base = {"schema": "r2f.s1-reset-receipt/v1", "kind": "snapshot_restore",
            "cleanup_verified": True, "resources_released": True,
            "evidence": [_evidence()], "baseline_restored": True,
            "identity_proof": {"endpoint_identity_consistent": True},
            "consistency_proof": {"quiescent": True, "atomic": True},
            "rollback_path": {"verified": True, "evidence": _evidence(2)}}
    accepted = validate_reset_receipt(base)
    assert accepted["state_origin"] == "snapshot_restore"
    for missing in ("identity_proof", "consistency_proof", "rollback_path"):
        damaged = dict(base)
        damaged.pop(missing)
        with pytest.raises(EpisodeError):
            validate_reset_receipt(damaged)
    with pytest.raises(EpisodeError):
        validate_reset_receipt(dict(base, identity_proof={"endpoint_identity_consistent": False}))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(dict(base, consistency_proof={"quiescent": False, "atomic": True}))
    with pytest.raises(EpisodeError):
        validate_reset_receipt(dict(base, rollback_path={"verified": False}))
