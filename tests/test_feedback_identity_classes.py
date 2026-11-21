"""Offline feedback/scheduling contracts; fixtures confer no ROS/DDS evidence."""
from r2f.campaign.feedback import FeedbackLog


def feedback_for(*, surface="fixture.surface", boundary="finite", variant="variant-a",
                 content="a" * 64):
    log = FeedbackLog()
    log.record({
        "content_sha256": content,
        "surface_id": surface, "boundary_class": boundary, "variant_id": variant,
        "execution_stage": "judged", "actual_run": True, "oracle_verdict": "pass",
        "typed_facts": {
            "environment_healthy": True, "process_healthy": True,
            "preconditions_reached": True, "endpoint_identity_complete": True,
            "window": {"complete": True},
        },
    })
    return log


def test_same_input_different_coverage_classes_have_distinct_feedback_identities():
    logs = [feedback_for(), feedback_for(surface="other.surface"),
            feedback_for(boundary="zero"), feedback_for(variant="variant-b"),
            feedback_for(variant=None)]
    # One executed input and no edge difference: classification alone differs.
    assert {log.events[0]["content_sha256"] for log in logs} == {"a" * 64}
    assert all(log.events[0]["coverage"] and log.events[0]["edge_id"] is None for log in logs)
    assert len({log.identity() for log in logs}) == 5


def test_runner_rejects_same_input_feedback_bound_to_another_coverage_class(tmp_path):
    import json
    from pathlib import Path

    import pytest

    from r2f.campaign.corpus import corpus_from_profile
    from r2f.campaign.runner import ExploratoryCycleError, run_exploratory_cycle
    from r2f.campaign.scheduler import ExploratoryScheduler

    repo = Path(__file__).resolve().parents[1]
    profile = json.loads((repo / "config/s1_fuzz/profiles/parameter.json").read_text())
    profile["budget"] = {"max_inputs": 1, "warmup_inputs": 0}
    inputs = corpus_from_profile(profile, cycle_id="late-endpoint-parameter")
    content = inputs.selectable()[0]["content_sha256"]
    accepted = feedback_for(content=content)
    substituted = FeedbackLog.from_dict(feedback_for(variant="variant-b", content=content).to_dict())
    scheduler = ExploratoryScheduler(inputs, accepted, warmup=0)
    before = scheduler.to_dict()
    with pytest.raises(ExploratoryCycleError, match="scheduler feedback identity drift"):
        run_exploratory_cycle(tmp_path / "rejected", corpus=inputs, feedback=substituted,
                              scheduler=scheduler, profile=profile, execute=False, repo=repo)
    assert scheduler.to_dict() == before
    assert not (tmp_path / "rejected").exists()


def test_restored_selection_uses_each_coverage_class_and_explains_empty_pool_fallback():
    import json

    from r2f.campaign.corpus import Corpus
    from r2f.campaign.scheduler import ExploratoryScheduler, replay_selection

    inputs = Corpus()
    for index, variant in enumerate(("variant-a", "variant-b")):
        inputs.add_seed({"seed_id": variant, "target_id": "fixture.target", "target_revision": 0,
                         "contract_hash": "fixture", "rmw": "fixture-rmw",
                         "surface_id": "fixture.surface", "boundary_class": "finite",
                         "variant_id": variant, "final_config": {"value": index}})
    rows = {row["variant_id"]: row for row in inputs.selectable()}
    identities, choices = [], []
    for covered, expected in (("variant-a", "variant-b"), ("variant-b", "variant-a")):
        log = feedback_for(variant=covered, content=rows["variant-a"]["content_sha256"])
        scheduler = ExploratoryScheduler(inputs, log, rng_seed=17, warmup=0)
        scheduler.next()  # uniform; the next decision must use the uncovered pool
        saved = json.loads(json.dumps(scheduler.to_dict()))
        restored_feedback = FeedbackLog.from_dict(json.loads(json.dumps(log.to_dict())))
        restored = ExploratoryScheduler.from_dict(
            saved, Corpus.from_dict(inputs.to_dict()), restored_feedback)
        actual = restored.next()
        assert actual == scheduler.next()
        assert actual["content_sha256"] == rows[expected]["content_sha256"]
        assert actual["entry"]["variant_id"] == expected
        assert (actual["requested_pool"], actual["used_pool"], actual["candidate_count"],
                actual["fallback_chain"]) == ("uncovered", "uncovered", 1, [])
        assert replay_selection(restored.to_dict(), inputs, restored_feedback.events)["valid"]
        identities.append(saved["feedback_identity"])
        choices.append(actual["content_sha256"])

        # After accepting the other class, the same requested pool is empty.
        restored_feedback.record({
            "content_sha256": rows["variant-a"]["content_sha256"],
            "surface_id": "fixture.surface", "boundary_class": "finite", "variant_id": expected,
            "execution_stage": "judged", "actual_run": True, "oracle_verdict": "pass",
            "typed_facts": {"environment_healthy": True, "process_healthy": True,
                            "preconditions_reached": True, "endpoint_identity_complete": True,
                            "window": {"complete": True}},
        })
        restored.advance_feedback(restored_feedback)
        continued = ExploratoryScheduler.from_dict(restored.to_dict(), inputs, restored_feedback)
        fallback = continued.next()
        assert fallback == restored.next()
        assert (fallback["requested_pool"], fallback["used_pool"], fallback["candidate_count"],
                fallback["fallback_chain"]) == ("uncovered", "uniform", 2, ["uncovered", "uniform"])
    assert len(set(choices)) == 2
    assert len(set(identities)) == 2


def test_legacy_selection_replay_checks_full_event_hash_independently_of_semantic_identity():
    from copy import deepcopy

    import pytest

    from r2f.campaign.corpus import Corpus
    from r2f.campaign.scheduler import ExploratoryScheduler, replay_selection

    inputs = Corpus()
    inputs.add_seed({"seed_id": "fixture", "target_id": "fixture.target", "target_revision": 0,
                     "contract_hash": "fixture", "rmw": "fixture-rmw",
                     "final_config": {"value": 0}})
    feedback = feedback_for()
    scheduler = ExploratoryScheduler(inputs, feedback, rng_seed=17, warmup=0)
    scheduler.plan(2)
    legacy = {"schema": "r2f.s1-selection-log/v1", "rng_seed": 17, "warmup": 0,
              "feedback_identity": feedback.identity(), "feedback_event_count": 1,
              "selections": deepcopy(scheduler.selection_log)}
    assert replay_selection(legacy, inputs, feedback.events)["valid"]
    changed = deepcopy(feedback.events)
    changed[0]["oracle_verdict"] = "fail"  # not a scheduler input; event hash is now stale
    assert FeedbackLog.from_events(changed).identity() == feedback.identity()
    with pytest.raises(ValueError, match="feedback event hash mismatch"):
        replay_selection(legacy, inputs, changed)


def test_rehashed_nonscheduling_event_change_cannot_replace_a_saved_feedback_prefix():
    from copy import deepcopy

    import pytest

    from r2f.campaign.corpus import Corpus
    from r2f.campaign.scheduler import ExploratoryScheduler
    from r2f.core.evidence import canonical_hash

    inputs = Corpus()
    inputs.add_seed({"seed_id": "fixture", "target_id": "fixture.target", "target_revision": 0,
                     "contract_hash": "fixture", "rmw": "fixture-rmw",
                     "final_config": {"value": 0}})
    feedback = feedback_for()
    scheduler = ExploratoryScheduler(inputs, feedback, rng_seed=17, warmup=0)
    scheduler.next()
    saved = scheduler.to_dict()
    changed = deepcopy(feedback.to_dict())
    changed["events"][0]["oracle_verdict"] = "fail"
    with pytest.raises(ValueError, match="feedback event hash mismatch"):
        FeedbackLog.from_dict(changed)
    event = changed["events"][0]
    event["event_sha256"] = canonical_hash({k: v for k, v in event.items() if k != "event_sha256"})
    restored_feedback = FeedbackLog.from_dict(changed)
    assert restored_feedback.identity() == feedback.identity()
    assert restored_feedback.events[0]["event_sha256"] != feedback.events[0]["event_sha256"]
    with pytest.raises(ValueError, match="selection feedback prefix identity drift"):
        ExploratoryScheduler.from_dict(saved, inputs, restored_feedback)
    with pytest.raises(ValueError, match="feedback must be append-only; prefix drift/truncation"):
        scheduler.advance_feedback(restored_feedback)
    assert scheduler.to_dict() == saved


def test_persisted_aggregate_restores_coverage_identity_and_rejects_prefix_replacement(tmp_path):
    import pytest

    from r2f.campaign.feedback_aggregate import FeedbackAggregate
    from r2f.core.state_signature import EDGE_SCHEMA, STATE_ABSTRACTION_VERSION

    gate = {"source_head": "a" * 40, "source_behavior_tree_sha256": "b" * 64,
            "target_id": "fixture.target", "target_revision": 0, "rule_id": "fixture.rule",
            "rule_sha256": "c" * 64, "state_signature_version": STATE_ABSTRACTION_VERSION,
            "edge_schema": EDGE_SCHEMA}  # compatibility fixture, not a source freeze
    identities = []
    for variant in ("variant-a", "variant-b"):
        path = tmp_path / (variant + ".json")
        feedback = feedback_for(variant=variant)
        merged, receipt = FeedbackLog.merge_partition_file(
            path, compatibility=gate, run_id="fixture-run", feedback=feedback.to_dict())
        assert receipt["imported_events"] == 1
        identities.append(merged.identity())
        restored = FeedbackAggregate.load(path, compatibility=gate).feedback_log()
        assert restored.to_dict() == feedback.to_dict()
        assert restored.covered_surfaces() == {("fixture.surface", "finite", variant)}
        original = path.read_bytes()
        _, duplicate = FeedbackLog.merge_partition_file(
            path, compatibility=gate, run_id="fixture-run", feedback=feedback.to_dict())
        assert duplicate["imported_events"] == 0
        assert path.read_bytes() == original
        replacement = feedback_for(variant="other-variant")
        with pytest.raises(ValueError, match="feedback run prefix replaced or truncated"):
            FeedbackLog.merge_partition_file(
                path, compatibility=gate, run_id="fixture-run", feedback=replacement.to_dict())
        assert path.read_bytes() == original
    assert len(set(identities)) == 2


def test_class_blind_historical_feedback_identity_is_rejected_without_rewriting():
    from copy import deepcopy

    import pytest

    data = feedback_for().to_dict()
    # Captured from the unmodified 32759d89 red test, not recomputed by this test.
    data["identity"] = "2c4dd2cd3c246e4306e1f4f502d5c4335f28120e84356ffc077a307286079281"
    original = deepcopy(data)
    with pytest.raises(ValueError, match="feedback identity or summary mismatch"):
        FeedbackLog.from_dict(data)
    assert data == original
