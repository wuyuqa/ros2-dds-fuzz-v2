"""Append-only feedback and reproducible historical corpus epochs (offline)."""
from copy import deepcopy

import pytest

from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler, replay_selection
from r2f.core.evidence import canonical_hash
from r2f.core.state_signature import abstract_state


def corpus():
    result = Corpus()
    for i in range(4):
        result.add_seed({"target_id": "fixture.target", "target_revision": 0,
                         "contract_hash": "fixture-contract", "rmw": "fixture-rmw",
                         "seed_id": f"seed-{i}", "surface_id": "fixture.surface",
                         "boundary_class": f"boundary-{i}",
                         "final_config": {"value": i}})
    return result


def observation(entry):
    # Explicit synthetic projection, not execution qualification.  State/edge
    # IDs are derived by the core rather than accepting a hand-written edge.
    state = abstract_state({"observation_complete": True, "creation_order_class": "unresolved"})
    raw = "f" * 64
    def binding(index):
        ref = {"path": "/offline/fixture.jsonl", "sha256": raw, "event_index": index}
        return {"boundary_ref": ref, "identity": {},
                "component_refs": {k: {"status": "observed", "refs": [ref]} for k in state}}
    return {"content_sha256": entry["content_sha256"], "execution_stage": "judged",
            "actual_run": True, "surface_id": entry["surface_id"],
            "boundary_class": entry["boundary_class"], "raw_path": "/offline/fixture.jsonl",
            "raw_sha256": raw, "oracle_verdict": "pass",
            "typed_facts": {"environment_healthy": True, "process_healthy": True,
                            "preconditions_reached": True, "endpoint_identity_complete": True,
                            "window": {"complete": True}, "state_before": state, "state_after": state,
                            "state_projection_receipt": {"schema": "r2f.s1-state-projection/v1",
                                "scope": "canonical_pair", "status": "observed", "reasons": [],
                                "raw_path": "/offline/fixture.jsonl", "raw_sha256": raw,
                                "before": binding(1), "after": binding(2)}}}


def test_append_advances_next_pool_but_frozen_selections_remain_auditable():
    inputs, feedback = corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(inputs, feedback, rng_seed=8, warmup=0)
    scheduler.next()
    frozen = scheduler.to_dict()
    entry = inputs.selectable()[0]
    event = feedback.record(observation(entry))
    assert event["edge_id"] and event["edge_provenance"]
    assert scheduler.to_dict() == frozen  # no implicit external mutation
    scheduler.advance_feedback(feedback)
    selected = scheduler.next()
    assert selected["requested_pool"] == "uncovered"
    assert selected["candidate_count"] == 3
    assert selected["content_sha256"] != entry["content_sha256"]
    scheduler.next()
    rare = scheduler.next()
    assert rare["used_pool"] == "rare_edge"
    assert rare["content_sha256"] == entry["content_sha256"]
    contexts = scheduler.to_dict()["selection_contexts"]
    assert contexts[0]["feedback_event_count"] == 0
    assert contexts[1]["feedback_event_count"] == 1
    assert contexts[0]["feedback_epoch"] == 0 and contexts[1]["feedback_epoch"] == 1
    assert replay_selection(frozen, inputs, feedback.events)["valid"]
    assert replay_selection(scheduler.to_dict(), inputs, feedback.events)["valid"]


@pytest.mark.parametrize("damage", ["truncate", "replace", "reorder", "hash", "nonhex"])
def test_advance_rejects_prefix_replacement_truncation_and_event_hash_drift(damage):
    inputs, feedback = corpus(), FeedbackLog()
    for entry in inputs.selectable()[:2]:
        feedback.record(observation(entry))
    scheduler = ExploratoryScheduler(inputs, feedback, warmup=0)
    before = scheduler.to_dict()
    bad = FeedbackLog.from_events(feedback.events)
    if damage == "truncate":
        bad.events.pop()
    elif damage == "reorder":
        bad.events.reverse()
    elif damage == "hash":
        bad.events[0]["oracle_verdict"] = "fail"
    elif damage == "nonhex":
        bad.events[0]["content_sha256"] = "q" * 64
    else:
        bad.events[0]["oracle_verdict"] = "fail"
        bad.events[0]["event_sha256"] = canonical_hash({k: v for k, v in bad.events[0].items() if k != "event_sha256"})
    with pytest.raises(ValueError):
        scheduler.advance_feedback(bad)
    assert scheduler.to_dict() == before


def test_selection_replay_uses_historical_corpus_not_final_members():
    inputs, feedback = corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(inputs, feedback, warmup=0)
    scheduler.plan(3)
    before = scheduler.to_dict()
    new = deepcopy(inputs.selectable()[0])
    for key in ("content_sha256", "bucket_key", "pinned_seed", "candidate_ids"):
        new.pop(key, None)
    new["final_config"] = {"value": 100}
    new["seed_id"] = "added-after-selection"
    inputs.add_seed(new)
    scheduler.plan(4)
    assert replay_selection(scheduler.to_dict(), inputs, feedback.events)["valid"]
    assert replay_selection(before, inputs, feedback.events)["valid"]
    restored = ExploratoryScheduler.from_dict(scheduler.to_dict(), Corpus.from_dict(inputs.to_dict()), feedback)
    assert restored.next() == scheduler.next()


def test_sampling_deficit_sequence_and_derived_rng_continue_after_restore():
    from test_exploratory_checkpoint import fresh, profile
    doc = profile()
    state = fresh(doc)
    inputs, feedback, scheduler = (state[k] for k in ("corpus", "feedback", "scheduler"))
    scheduler.configure_sampling(["representative", "boundary"], {"representative": 1, "boundary": 2})
    scheduler.plan(3)
    saved = scheduler.to_dict()
    restored = ExploratoryScheduler.from_dict(saved, Corpus.from_dict(inputs.to_dict()), feedback)
    assert restored.plan(9) == scheduler.plan(9)
    assert restored.to_dict() == scheduler.to_dict()


def test_v1_frozen_selection_logs_still_replay():
    inputs, feedback = corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(inputs, feedback, rng_seed=2, warmup=1)
    scheduler.plan(4)
    legacy = {"schema": "r2f.s1-selection-log/v1", "rng_seed": 2, "warmup": 1,
              "feedback_identity": feedback.identity(), "feedback_event_count": 0,
              "selections": deepcopy(scheduler.selection_log)}
    assert replay_selection(legacy, inputs, [])["valid"]
