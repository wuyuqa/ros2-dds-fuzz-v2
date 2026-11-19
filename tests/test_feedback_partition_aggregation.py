"""Offline artifact/aggregation contracts; synthetic receipts are not DDS proof."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.feedback_aggregate import FeedbackAggregate, merge_feedback_file, feedback_compatibility
from r2f.core.evidence import canonical_hash
from r2f.core.state_signature import abstract_state, STATE_ABSTRACTION_VERSION, EDGE_SCHEMA


def compatibility():
    return {"source_head": "a" * 40, "source_behavior_tree_sha256": "b" * 64,
            "target_id": "fixture.target", "target_revision": 0,
            "rule_id": "fixture.rule", "rule_sha256": "c" * 64,
            "state_signature_version": STATE_ABSTRACTION_VERSION, "edge_schema": EDGE_SCHEMA}


def observation(number=0, parameter="same"):
    state = abstract_state({"observation_complete": True, "creation_order_class": "unresolved"})
    path, digest = "/offline/fixture.jsonl", "f" * 64
    def binding(index):
        ref = {"path": path, "sha256": digest, "event_index": index}
        return {"boundary_ref": ref, "identity": {},
                "component_refs": {k: {"status": "observed", "refs": [ref]} for k in state}}
    return {"content_sha256": canonical_hash(number), "execution_stage": "judged", "actual_run": True,
            "surface_id": "fixture.surface", "boundary_class": "fixture.boundary",
            "parameter_class": parameter, "raw_path": path, "raw_sha256": digest,
            "oracle_verdict": "pass", "typed_facts": {
                "environment_healthy": True, "process_healthy": True, "preconditions_reached": True,
                "endpoint_identity_complete": True, "window": {"complete": True},
                "state_before": state, "state_after": state, "state_projection_receipt": {
                    "schema": "r2f.s1-state-projection/v1", "scope": "canonical_pair", "status": "observed",
                    "reasons": [], "raw_path": path, "raw_sha256": digest,
                    "before": binding(1), "after": binding(2)}}}


def feedback(parameters):
    log = FeedbackLog()
    for i, parameter in enumerate(parameters):
        log.record(observation(i, parameter))
    return log


def import_run(aggregate, run_id, log, gate=None):
    return aggregate.import_run(run_id, log.to_dict(), compatibility=gate or compatibility())


def rehash_event(event):
    event["event_sha256"] = canonical_hash({k: v for k, v in event.items() if k != "event_sha256"})


def test_partition_local_nine_is_global_six_not_sum():
    aggregate = FeedbackAggregate(compatibility())
    logs = [feedback(["common", "common", "a"]), feedback(["b", "common", "c", "common"]),
            feedback(["common"] * 4), feedback(["d", "common", "e"])]
    receipts = [import_run(aggregate, str(i), log) for i, log in enumerate(logs)]
    assert [len(r["global_new_edge_ids"]) for r in receipts] == [2, 2, 0, 2]
    stats = aggregate.statistics()
    assert stats["run_local_unique_edge_sum"] == 9
    assert stats["global_unique_edges"] == 6
    assert stats["cross_run_duplicate_edge_claims"] == 3
    assert stats["edge_observation_events"] == 14
    assert sorted(aggregate.feedback_log().edge_counts().values()) == [1, 1, 1, 1, 1, 9]


def test_resume_prefix_idempotence_and_cross_run_identical_events():
    aggregate = FeedbackAggregate(compatibility())
    whole = feedback(["a", "a", "b"])
    prefix = FeedbackLog.from_events(whole.events[:1])
    import_run(aggregate, "run-a", prefix)
    import_run(aggregate, "run-b", prefix)
    before = aggregate.to_dict()
    assert import_run(aggregate, "run-a", prefix)["imported_events"] == 0
    assert aggregate.to_dict() == before
    assert import_run(aggregate, "run-a", whole)["imported_events"] == 2
    assert len(aggregate.feedback_log().events) == 4
    assert len(aggregate.feedback_log().edge_counts()) == 2
    assert aggregate.feedback_log().events[:2] == prefix.events * 2
    assert FeedbackAggregate.from_dict(aggregate.to_dict(), compatibility=compatibility()).to_dict() == aggregate.to_dict()
    before = aggregate.to_dict()
    with pytest.raises(ValueError, match="prefix"):
        import_run(aggregate, "run-a", prefix)
    assert aggregate.to_dict() == before
    replaced = feedback(["different", "a", "b"])
    with pytest.raises(ValueError, match="prefix"):
        import_run(aggregate, "run-a", replaced)
    assert aggregate.to_dict() == before


@pytest.mark.parametrize("key,value", [
    ("source_head", "d" * 40), ("source_behavior_tree_sha256", "d" * 64),
    ("target_id", "other"), ("target_revision", 1), ("rule_id", "other"),
    ("rule_sha256", "d" * 64), ("state_signature_version", "unsupported/v2"),
    ("edge_schema", "unsupported/v2")])
def test_compatibility_gate_fails_without_partial_import(key, value):
    aggregate = FeedbackAggregate(compatibility())
    import_run(aggregate, "a", feedback(["a"]))
    before = aggregate.to_dict()
    gate = compatibility(); gate[key] = value
    with pytest.raises(ValueError):
        import_run(aggregate, "b", feedback(["b"]), gate)
    with pytest.raises(ValueError):
        FeedbackAggregate.from_dict(before, compatibility=gate)
    assert aggregate.to_dict() == before


@pytest.mark.parametrize("field", ["identity", "edge_counts", "covered_surfaces", "schema"])
def test_feedback_summary_tampering_rejected(field):
    data = feedback(["a"]).to_dict()
    data[field] = {} if field == "edge_counts" else "tampered"
    with pytest.raises(ValueError):
        FeedbackLog.from_dict(data)


def test_edge_recomputed_not_trusted_even_when_all_hashes_recomputed():
    log = feedback(["a"])
    log.events[0]["edge_id"] = "e" * 64
    rehash_event(log.events[0])
    with pytest.raises(ValueError, match="provenance"):
        FeedbackLog.from_dict(log.to_dict())
    log = feedback(["a"])
    del log.events[0]["edge_provenance"]
    rehash_event(log.events[0])
    with pytest.raises(ValueError, match="provenance"):
        FeedbackLog.from_dict(log.to_dict())


def test_attempts_and_no_projection_do_not_invent_edges():
    log = FeedbackLog()
    for change in ({"actual_run": False}, {"execution_stage": "planned"}, {"tool_error": True}):
        row = observation(); row.update(change); log.record(row)
    row = observation(); del row["typed_facts"]["state_projection_receipt"]; log.record(row)
    aggregate = FeedbackAggregate(compatibility())
    import_run(aggregate, "attempts", log)
    assert aggregate.statistics()["observation_events"] == 4
    assert aggregate.statistics()["global_unique_edges"] == 0
    assert len(aggregate.feedback_log().covered_surfaces()) == 1


def test_schema_mismatch_and_malformed_compatibility_fail_closed():
    gate = compatibility(); gate["state_signature_version"] = "r2f.s1-topology-state/v1"
    gate["edge_schema"] = "r2f.s1-topology-state-edge/v1"
    aggregate = FeedbackAggregate(gate)
    with pytest.raises(ValueError, match="signature"):
        import_run(aggregate, "pair-not-topology", feedback(["a"]), gate)
    for key in compatibility():
        bad = compatibility(); del bad[key]
        with pytest.raises(ValueError):
            FeedbackAggregate(bad)
    bad = compatibility(); bad["target_revision"] = True
    with pytest.raises(ValueError):
        FeedbackAggregate(bad)


def test_atomic_persistence_restart_and_duplicate_import(tmp_path):
    path = tmp_path / "aggregate.json"
    whole = feedback(["a", "b"])
    first, receipt = merge_feedback_file(path, compatibility=compatibility(), run_id="a", feedback=whole.to_dict())
    assert receipt["imported_events"] == 2
    original = path.read_bytes()
    again, receipt = merge_feedback_file(path, compatibility=compatibility(), run_id="a", feedback=whole.to_dict())
    assert receipt["imported_events"] == 0
    assert path.read_bytes() == original
    assert first.to_dict() == again.to_dict()
    assert FeedbackAggregate.load(path, compatibility=compatibility()).to_dict() == first.to_dict()
    gate = compatibility(); gate["source_head"] = "e" * 40
    with pytest.raises(ValueError):
        merge_feedback_file(path, compatibility=gate, run_id="b", feedback=whole.to_dict())
    assert path.read_bytes() == original


def test_failed_atomic_replace_retains_previous_bytes_and_removes_temp(tmp_path, monkeypatch):
    import r2f.campaign.feedback_aggregate as module
    path = tmp_path / "aggregate.json"
    merge_feedback_file(path, compatibility=compatibility(), run_id="a", feedback=feedback(["a"]).to_dict())
    original = path.read_bytes()
    def fail(*args):
        raise OSError("fixture replace failure")
    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(OSError, match="fixture replace failure"):
        merge_feedback_file(path, compatibility=compatibility(), run_id="b", feedback=feedback(["b"]).to_dict())
    assert path.read_bytes() == original
    assert {p.name for p in tmp_path.iterdir()} == {"aggregate.json", "aggregate.json.lock"}


def test_concurrent_file_imports_have_no_lost_updates(tmp_path):
    path = tmp_path / "aggregate.json"
    def apply(i):
        return merge_feedback_file(path, compatibility=compatibility(), run_id=str(i),
                                   feedback=feedback([str(i), "common"]).to_dict())
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(apply, range(8)))
    aggregate = FeedbackAggregate.load(path, compatibility=compatibility())
    assert aggregate.statistics()["observation_events"] == 16
    assert aggregate.statistics()["global_unique_edges"] == 9


def test_persisted_order_statistics_and_identity_are_validated():
    aggregate = FeedbackAggregate(compatibility())
    import_run(aggregate, "a", feedback(["a", "b"]))
    for field, value in [("statistics", {}), ("order", [["a", 0], ["a", 0]]), ("identity", "0" * 64)]:
        damaged = aggregate.to_dict(); damaged[field] = value
        if field != "identity":
            damaged["identity"] = canonical_hash({k: v for k, v in damaged.items() if k != "identity"})
        with pytest.raises(ValueError):
            FeedbackAggregate.from_dict(damaged, compatibility=compatibility())


def test_compatibility_factory_requires_actual_frozen_manifest():
    gate = compatibility()
    source = {"head": gate["source_head"], "behavior_tree_sha256": gate["source_behavior_tree_sha256"],
              "source_frozen": True, "dirty_behavior_paths": []}
    args = {k: gate[k] for k in ("target_id", "target_revision", "rule_id", "rule_sha256", "state_signature_version")}
    assert feedback_compatibility(source, **args) == gate
    for update in ({"source_frozen": False}, {"dirty_behavior_paths": ["changed.py"]},
                   {"head": None}, {"behavior_tree_sha256": "not-a-hash"}):
        with pytest.raises(ValueError):
            feedback_compatibility(dict(source, **update), **args)


def test_corrupt_durable_file_not_replaced(tmp_path):
    path = tmp_path / "aggregate.json"
    path.write_text('{"partial":')
    original = path.read_bytes()
    with pytest.raises(ValueError):
        merge_feedback_file(path, compatibility=compatibility(), run_id="a", feedback=feedback(["a"]).to_dict())
    assert path.read_bytes() == original


def test_aggregate_feedback_cannot_change_bfs_task_eligibility_order_or_rng():
    from pathlib import Path
    import json
    from r2f.campaign.corpus import corpus_from_profile
    from r2f.campaign.scheduler import ExploratoryScheduler
    doc = json.loads((Path(__file__).resolve().parents[1] / "config/s1_fuzz/profiles/model-bfs-routes.json").read_text())
    aggregate = FeedbackAggregate(compatibility())
    import_run(aggregate, "previous", feedback(["a", "b"]))
    inputs = corpus_from_profile(doc, cycle_id="common-baseline")
    def scheduler(log):
        return ExploratoryScheduler(inputs, log, policy="bfs", rng_seed=41, warmup=0,
            sampling_kinds=doc["sampling_kinds"], sampling_weight=doc.get("sampling_weight"), search=doc["search"])
    empty = scheduler(FeedbackLog())
    restored = scheduler(aggregate.feedback_log())
    for _ in range(6):
        left, right = empty.next(), restored.next()
        assert left == right
        # Explicit input-only rejection: no generation or execution evidence.
        for sched, selected in ((empty, left), (restored, right)):
            sched.frontier.finish(selected["task_id"], rejected=True, reason="offline_fixture")
    assert empty.frontier.to_dict() == restored.frontier.to_dict()
