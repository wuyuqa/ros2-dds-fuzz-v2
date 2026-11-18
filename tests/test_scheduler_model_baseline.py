"""Offline fair model traversal, not BFS/DFS or evidence of SUT state discovery."""
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.corpus import Corpus, corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler, replay_selection
from r2f.core.evidence import canonical_hash


REPO = Path(__file__).resolve().parents[1]
DIMENSIONS = ("target_id", "surface_id", "boundary_class", "variant_id",
              "chain_depth", "sampling_kind")


def entry(index, **classes):
    return {"seed_id": f"model-{index}", "target_id": "model.target",
            "target_revision": 0, "rmw": "rmw_fastrtps_cpp", "contract_hash": "offline",
            "surface_id": "model.surface", "boundary_class": "declared-boundary",
            "variant_id": "declared-variant", "chain_depth": 1,
            "final_config": {"value": index}, "executable": True, **classes}


def model_corpus(count=4):
    result = Corpus()
    for index in range(count):
        result.add_seed(entry(index))
    return result


def sampling_corpus():
    profile = json.loads((REPO / "config/s1_fuzz/profiles/parameter.json").read_text())
    return corpus_from_profile(profile, cycle_id="late-endpoint-parameter")


def feedback_for(inputs, *, verdict="pass", complete=True, reverse=False):
    # This existing helper builds explicit raw/projection fixtures, not live ROS.
    from test_scheduler_feedback_epoch import observation
    log = FeedbackLog()
    rows = inputs.selectable()[:2]
    for index, row in enumerate(reversed(rows) if reverse else rows):
        record = observation(row)
        record["parameter_class"] = "fixture-edge-" + str(index + (10 if reverse else 1))
        record["oracle_verdict"] = verdict
        if not complete:
            record["typed_facts"]["window"]["complete"] = False
        event = log.record(record)
        assert event["coverage"] is complete
        assert bool(event["edge_id"]) is complete
    return log


def decision(event):
    return (event["seed_id"], event["content_sha256"], event.get("sampling_kind"),
            event["decision_context"], event["decision_sha256"])


def rehash(state):
    state["selection_identity"] = canonical_hash(
        {key: value for key, value in state.items() if key != "selection_identity"})
    return state


@pytest.mark.parametrize("bad", ["", "uniform", "model", "FAIR", 0, True, [], {}])
def test_unknown_policy_is_rejected_at_construction(bad):
    with pytest.raises(ValueError, match="policy"):
        ExploratoryScheduler(model_corpus(), policy=bad)


def test_explicit_feedback_is_opt_in_and_default_keeps_historical_field_sets():
    inputs = model_corpus()
    legacy = ExploratoryScheduler(inputs, rng_seed=4)
    explicit = ExploratoryScheduler(inputs, rng_seed=4, policy="feedback")
    assert legacy.policy == explicit.policy == "feedback"
    for left, right in zip(legacy.plan(12), explicit.plan(12)):
        assert left["content_sha256"] == right["content_sha256"]
        assert "policy" not in left and "decision_sha256" not in left
        assert right["policy"] == "feedback" and "decision_sha256" not in right
    state = legacy.to_dict()
    assert "policy" not in state
    assert all("policy" not in c for c in state["selection_contexts"])
    explicit_state = explicit.to_dict()
    assert explicit_state["policy"] == "feedback"
    assert all(c["policy"] == "feedback" for c in explicit_state["selection_contexts"])
    assert ExploratoryScheduler.from_dict(explicit_state, inputs, FeedbackLog()).to_dict() == explicit_state
    assert replay_selection(explicit_state, inputs, [])['valid'] is True


@pytest.mark.parametrize("dimension", DIMENSIONS)
def test_fair_equal_sibling_shares_are_not_biased_by_number_of_inputs(dimension):
    inputs = Corpus()
    values = (1, 3) if dimension == "chain_depth" else ("declared-a", "declared-b")
    # One declared sibling has one content ID, the other has seven.  The
    # dimension quota is still 50/50; this is not uniform over corpus rows.
    for index in range(8):
        inputs.add_seed(entry(index, **{dimension: values[0 if index == 0 else 1]}))
    scheduler = ExploratoryScheduler(inputs, policy="fair", rng_seed=17, warmup=3)
    rows = scheduler.plan(24)
    counts = Counter(row["input_classes"][dimension] for row in rows)
    assert counts == {values[0]: 12, values[1]: 12}
    for row in rows:
        assert row["policy"] == row["pool"] == row["requested_pool"] == "fair"
        assert row["decision_sha256"] == canonical_hash(row["decision_context"])
        assert row["decision_context"]["policy"] == "fair"
    assert rows[0]["phase"] == "warmup" and rows[3]["phase"] == "exploratory"


def test_fair_content_leaves_are_balanced_and_seed_ties_are_stable():
    inputs = model_corpus(4)
    first = ExploratoryScheduler(inputs, rng_seed=15, policy="fair").plan(16)
    again = ExploratoryScheduler(inputs, rng_seed=15, policy="fair").plan(16)
    other = ExploratoryScheduler(inputs, rng_seed=16, policy="fair").plan(16)
    assert set(Counter(row["content_sha256"] for row in first).values()) == {4}
    assert first == again
    assert [r["content_sha256"] for r in first] != [r["content_sha256"] for r in other]
    reordered = Corpus()
    for row in reversed(inputs.selectable()):
        reordered.add_seed(row)
    assert [decision(r) for r in ExploratoryScheduler(reordered, rng_seed=15, policy="fair").plan(16)] == [decision(r) for r in first]


def test_fair_missing_depth_is_unknown_not_an_invented_chain_and_variant_uses_seed():
    inputs = model_corpus(2)
    for row in inputs.entries.values():
        row.pop("variant_id")
        row.pop("chain_depth")
    rows = ExploratoryScheduler(inputs, policy="fair").plan(4)
    assert {r["input_classes"]["variant_id"] for r in rows} == {"model-0", "model-1"}
    assert all(r["input_classes"]["chain_depth"] is None for r in rows)


def test_fair_sampling_uses_declared_weighted_shares_and_excludes_zero_weight():
    inputs = sampling_corpus()
    scheduler = ExploratoryScheduler(
        inputs, policy="fair", rng_seed=9,
        sampling_kinds=["boundary", "representative", "linear_uniform"],
        sampling_weight={"boundary": 1, "representative": 2, "linear_uniform": 0})
    rows = scheduler.plan(30)
    assert Counter(r["sampling_kind"] for r in rows) == {"boundary": 10, "representative": 20}
    assert all(r["sampling"]["supported"] for r in rows)
    assert all(r["decision_context"]["sampling"]["kind"] == r["sampling_kind"] for r in rows)


def test_fair_unsupported_sampling_is_reported_and_never_silently_substituted():
    scheduler = ExploratoryScheduler(model_corpus(), policy="fair",
                                     sampling_kinds=["boundary"])
    row = scheduler.next()
    assert row["sampling_kind"] is None and row["sampling_error"] == "no_supported_sampling_kind"
    assert row["decision_context"]["sampling"]["kind"] is None


@pytest.mark.parametrize("complete,verdict,reverse", [(True,"pass",False), (True,"fail",True), (False,"unknown",False)])
def test_fair_decisions_and_sampling_ignore_feedback_but_keep_execution_audit(complete, verdict, reverse):
    inputs = sampling_corpus()
    feedback = feedback_for(inputs, complete=complete, verdict=verdict, reverse=reverse)
    options = dict(policy="fair", rng_seed=33, warmup=2,
                   sampling_kinds=["boundary", "representative"],
                   sampling_weight={"boundary": 1, "representative": 2})
    empty = ExploratoryScheduler(inputs, FeedbackLog(), **options)
    observed = ExploratoryScheduler(inputs, feedback, **options)
    left, right = empty.plan(12), observed.plan(12)
    assert [decision(r) for r in left] == [decision(r) for r in right]
    assert [r["selection_sha256"] for r in left] != [r["selection_sha256"] for r in right]
    assert empty.to_dict()["feedback_identity"] != observed.to_dict()["feedback_identity"]
    assert replay_selection(empty.to_dict(), inputs, [])['valid']
    assert replay_selection(observed.to_dict(), inputs, feedback.events)['valid']
    for row in right:
        material = row["decision_context"]
        assert set(material) == {"policy", "rng_seed", "sequence", "eligible", "history", "chosen", "sampling"}
        text = json.dumps(material)
        assert all(key not in text for key in ("feedback", "oracle", "observed_edge", "coverage"))


def test_feedback_advance_only_changes_fair_audit_not_next_decision():
    inputs = sampling_corpus()
    scheduler = ExploratoryScheduler(inputs, rng_seed=9, policy="fair")
    control = ExploratoryScheduler(inputs, rng_seed=9, policy="fair")
    assert scheduler.plan(3) == control.plan(3)
    frozen = scheduler.to_dict()
    feedback = feedback_for(inputs, verdict="fail")
    scheduler.advance_feedback(feedback)
    left, right = scheduler.plan(8), control.plan(8)
    assert [decision(r) for r in left] == [decision(r) for r in right]
    assert frozen["feedback_event_count"] == 0
    assert scheduler.to_dict()["feedback_event_count"] == 2
    assert replay_selection(frozen, inputs, feedback.events)['valid']
    assert replay_selection(scheduler.to_dict(), inputs, feedback.events)['valid']


def test_fair_restore_and_continue_preserves_sampling_corpus_epochs_and_decisions():
    inputs, feedback = sampling_corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(inputs, feedback, rng_seed=51, policy="fair", warmup=2,
                                     sampling_kinds=["boundary", "representative"])
    scheduler.plan(5)
    feedback = feedback_for(inputs)
    scheduler.advance_feedback(feedback)
    scheduler.plan(3)
    # Legitimate corpus evolution is replayed against the historical snapshots.
    extra = deepcopy(inputs.selectable()[0])
    for key in ("content_sha256", "bucket_key", "candidate_ids", "pinned_seed"):
        extra.pop(key, None)
    extra["final_config"]["reader_delay_sec"] = 0.314
    extra["seed_id"] = "added-after-selection"
    inputs.add_seed(extra)
    scheduler.plan(2)
    saved = scheduler.to_dict()
    restored = ExploratoryScheduler.from_dict(saved, Corpus.from_dict(inputs.to_dict()), feedback)
    assert restored.to_dict() == saved and restored.policy == "fair"
    assert restored.plan(8) == scheduler.plan(8)
    assert restored.to_dict() == scheduler.to_dict()


@pytest.mark.parametrize("damage", ["unknown", "null", "omit", "switch", "context", "event", "token", "material"])
def test_fair_restore_rejects_policy_or_decision_injection_even_with_rehashed_outer_state(damage):
    inputs, feedback = model_corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(inputs, feedback, policy="fair")
    scheduler.plan(2)
    saved = scheduler.to_dict()
    if damage == "unknown":
        saved["policy"] = "uniform"
    elif damage == "null":
        saved["policy"] = None
    elif damage == "omit":
        saved.pop("policy")
    elif damage == "switch":
        saved["policy"] = "feedback"
    elif damage == "context":
        saved["selection_contexts"][0]["policy"] = "feedback"
        context = saved["selection_contexts"][0]
        context["context_sha256"] = canonical_hash({k:v for k,v in context.items() if k != "context_sha256"})
    elif damage == "event":
        saved["selections"][0]["policy"] = "feedback"
    elif damage == "token":
        saved["selections"][0]["decision_sha256"] = "a" * 64
    else:
        saved["selections"][0]["decision_context"]["feedback_identity"] = "injected"
    rehash(saved)
    with pytest.raises(ValueError):
        ExploratoryScheduler.from_dict(saved, inputs, feedback)
    with pytest.raises(ValueError):
        replay_selection(saved, inputs, feedback.events)


@pytest.mark.parametrize("policy", ["fair", "feedback"])
def test_empty_explicit_state_roundtrip_preserves_policy(policy):
    inputs = model_corpus()
    scheduler = ExploratoryScheduler(inputs, policy=policy)
    restored = ExploratoryScheduler.from_dict(scheduler.to_dict(), inputs, FeedbackLog())
    assert restored.to_dict() == scheduler.to_dict()
    assert restored.next() == scheduler.next()


@pytest.mark.parametrize("field,value", [("policy","fair"), ("policy","feedback"), ("decision_sha256","a"*64)])
def test_legacy_v1_cannot_smuggle_unverified_new_policy_or_decision_fields(field,value):
    inputs, feedback = model_corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(inputs, feedback, rng_seed=2, warmup=1)
    scheduler.plan(3)
    legacy = {"schema":"r2f.s1-selection-log/v1", "rng_seed":2, "warmup":1,
              "feedback_identity":feedback.identity(), "feedback_event_count":0,
              "selections":deepcopy(scheduler.selection_log)}
    assert replay_selection(legacy, inputs, [])['valid']
    legacy[field] = value
    with pytest.raises(ValueError, match="policy|decision"):
        replay_selection(legacy, inputs, [])


def test_fair_hierarchy_uses_conditional_sibling_shares_not_global_row_counts():
    inputs = Corpus()
    # A has one surface; B has two. Fairness is per declared parent, not
    # uniform over all surface labels or content density across targets.
    classes = [("a", "a.only"), ("b", "b.first")] + [("b", "b.second")] * 6
    for index, (target, surface) in enumerate(classes):
        inputs.add_seed(entry(index, target_id=target, surface_id=surface))
    rows = ExploratoryScheduler(inputs, rng_seed=13, policy="fair").plan(24)
    assert Counter(r["input_classes"]["target_id"] for r in rows) == {"a": 12, "b": 12}
    assert Counter(r["input_classes"]["surface_id"] for r in rows) == {
        "a.only": 12, "b.first": 6, "b.second": 6}


def test_fair_corpus_observation_retention_metadata_cannot_influence_decisions():
    inputs = sampling_corpus()
    observed = Corpus.from_dict(inputs.to_dict())
    for index, record in enumerate(observed.entries.values()):
        record.update(observed_edge=f"changed-edge-{index}", oracle_verdict="fail",
                      evidence_complete=True, cost=index * 100,
                      coverage={"untrusted": "audit only"})
    # The test changes only labels outside the declared class/content
    # projection. It does not re-admit corpus content or claim observations.
    options = dict(policy="fair", rng_seed=27, sampling_kinds=["boundary", "representative"])
    left = ExploratoryScheduler(inputs, **options)
    right = ExploratoryScheduler(observed, **options)
    assert [decision(r) for r in left.plan(12)] == [decision(r) for r in right.plan(12)]
    assert left.to_dict()["selection_contexts"] != right.to_dict()["selection_contexts"]


def test_fair_never_calls_runtime_feedback_choice_paths(monkeypatch):
    scheduler = ExploratoryScheduler(sampling_corpus(), policy="fair",
                                     sampling_kinds=["boundary", "representative"])

    def forbidden(*args, **kwargs):
        raise AssertionError("fair attempted a feedback-dependent random choice")

    for name in ("_rng", "_pool", "_uncovered", "_rare", "_edge_by_content", "_choose_sampling_kind"):
        monkeypatch.setattr(scheduler, name, forbidden)
    assert len(scheduler.plan(5)) == 5


def test_fair_token_return_is_detached_from_internal_selection_and_context():
    inputs = model_corpus()
    scheduler = ExploratoryScheduler(inputs, policy="fair")
    event = scheduler.next()
    before = scheduler.to_dict()
    event["decision_sha256"] = "0" * 64
    event["decision_context"]["chosen"]["content_sha256"] = "f" * 64
    event["entry"]["final_config"]["value"] = "modified by caller"
    assert scheduler.to_dict() == before
    assert ExploratoryScheduler.from_dict(before, inputs, FeedbackLog()).to_dict() == before


def test_fair_replay_still_requires_authentic_audit_prefix_even_when_decisions_ignore_it():
    inputs = sampling_corpus()
    feedback = feedback_for(inputs)
    scheduler = ExploratoryScheduler(inputs, feedback, policy="fair")
    scheduler.plan(3)
    frozen = scheduler.to_dict()
    with pytest.raises(ValueError):
        replay_selection(frozen, inputs, list(reversed(feedback.events)))
    bad = deepcopy(feedback.events)
    bad[0]["oracle_verdict"] = "fail"
    bad[0]["event_sha256"] = canonical_hash({k:v for k,v in bad[0].items() if k != "event_sha256"})
    with pytest.raises(ValueError):
        replay_selection(frozen, inputs, bad)


@pytest.mark.parametrize("class_name,value", [("chain_depth", True), ("chain_depth", 0),
                                              ("chain_depth", "2"), ("surface_id", [])])
def test_fair_does_not_invent_or_coerce_invalid_declared_class_metadata(class_name, value):
    inputs = model_corpus()
    for record in inputs.entries.values():
        record[class_name] = value
    with pytest.raises(ValueError, match="fair"):
        ExploratoryScheduler(inputs, policy="fair").next()


def test_actual_runner_rng_bridge_consumes_fair_decision_not_feedback_audit_hash():
    # This calls the pure RNG adapter only, not run_exploratory_cycle/campaign.
    from r2f.campaign.runner import _derive_rng_seed
    inputs = sampling_corpus()
    options = dict(policy="fair", rng_seed=39, sampling_kinds=["boundary", "representative"])
    empty = ExploratoryScheduler(inputs, **options)
    observed = ExploratoryScheduler(inputs, feedback_for(inputs, verdict="fail"), **options)
    left, right = empty.plan(5), observed.plan(5)
    assert [r["selection_sha256"] for r in left] != [r["selection_sha256"] for r in right]
    assert [_derive_rng_seed(empty, r, r["entry"]) for r in left] == [
        _derive_rng_seed(observed, r, r["entry"]) for r in right]
    for row in left:
        material = {"cycle_rng_seed": empty.rng_seed, "decision_sha256": row["decision_sha256"],
                    "sequence": row["sequence"], "parent_content_sha256": row["content_sha256"]}
        assert _derive_rng_seed(empty, row, row["entry"]) == int(canonical_hash(material)[:16], 16)


@pytest.mark.parametrize("damage", ["token", "material", "sequence"])
def test_runner_rng_bridge_refuses_user_supplied_unaccepted_fair_token(damage):
    from r2f.campaign.runner import ExploratoryCycleError, _derive_rng_seed
    scheduler = ExploratoryScheduler(model_corpus(), policy="fair")
    event = scheduler.next()
    bad = deepcopy(event)
    if damage == "token":
        bad["decision_sha256"] = "a" * 64
    elif damage == "material":
        bad["decision_context"]["rng_seed"] = 19
        bad["decision_sha256"] = canonical_hash(bad["decision_context"])
    else:
        bad["sequence"] = 100
    with pytest.raises(ExploratoryCycleError, match="fair"):
        _derive_rng_seed(scheduler, bad, bad["entry"])


def test_actual_runner_rng_bridge_preserves_omitted_feedback_formula():
    from r2f.campaign.runner import _derive_rng_seed
    scheduler = ExploratoryScheduler(model_corpus(), rng_seed=41)
    row = scheduler.next()
    material = {"cycle_rng_seed": scheduler.rng_seed, "selection_sha256": row["selection_sha256"],
                "sequence": row["sequence"], "parent_content_sha256": row["content_sha256"]}
    assert _derive_rng_seed(scheduler, row, row["entry"]) == int(canonical_hash(material)[:16], 16)
