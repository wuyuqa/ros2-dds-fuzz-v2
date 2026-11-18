"""Input-only model BFS tests: no ROS, runtime, reached state, hit or effect."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.corpus import content_identity, corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import _build_case_plan
from r2f.campaign.scheduler import ExploratoryScheduler, required_catalog_hash
from r2f.campaign.search_frontier import InputSearchFrontier, derived_record
from r2f.core import Registry
from r2f.core.evidence import canonical_hash
from r2f.modules.s1.module import register_exploratory

ROOT = Path(__file__).resolve().parents[1]
CYCLE = "common-baseline"


def profile():
    doc = json.loads((ROOT / "config/s1_fuzz/profiles/model-bfs-routes.json").read_text())
    doc["rmws"] = doc["rmws"][:1]
    doc["budget"].pop("max_seconds", None)
    return doc


def fresh(doc=None, feedback=None):
    doc = doc or profile()
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    feedback = feedback or FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, policy="bfs", rng_seed=41, warmup=0,
        sampling_kinds=doc["sampling_kinds"], sampling_weight=doc.get("sampling_weight"), search=doc["search"])
    return {"corpus": corpus, "feedback": feedback, "scheduler": scheduler, "continuation": None}


def generate(scheduler, doc, selection=None):
    selection = selection or scheduler.next()
    parent = selection["entry"]
    registry = Registry()
    register_exploratory(registry, include_required=False)
    target = registry.get_target(parent["target_id"])
    contract = target.exploration_contract
    route = selection["mutation_route"]
    from r2f.campaign.runner import _NoEffectiveInputRejection
    try:
        plan = _build_case_plan(parent, target, contract, contract.surface_by_id[route["surface_id"]],
                                parent["final_config"], route["mutator_id"], doc,
                                selection["rng_seed"], selection)["plan"]
    except _NoEffectiveInputRejection as exc:
        plan = exc.plan
    record = derived_record(parent, plan)
    scheduler.frontier.planned(selection["task_id"], record)
    outcome = scheduler.frontier.task(selection["task_id"])["generation_outcome"]
    rejected = outcome in {"compile_time_no_change", "duplicate_content", "no_effective_input"}
    scheduler.frontier.finish(selection["task_id"], rejected=rejected,
                              reason=outcome if rejected else None)
    return selection, record


def drain(scheduler, doc):
    result = []
    while scheduler.frontier.stop_reason is None:
        result.append(generate(scheduler, doc))
    return result


def test_non_novel_pass_child_remains_searchable_and_bfs_is_depth_ordered():
    doc = profile()
    doc["search"].update(max_depth=2, samples_per_class=1)
    state = fresh(doc)
    s, c = state["scheduler"], state["corpus"]
    selection, record = generate(s, doc)
    rejected = c.offer(record, executed=True, evidence_complete=True, oracle_verdict="pass")
    assert rejected["reason"] == "success_only"
    assert record["content_sha256"] not in c.entries
    remaining = drain(s, doc)
    assert any(item[0]["parent_content_sha256"] == record["content_sha256"] for item in remaining)
    depths = [selection["derivation_depth"]] + [it[0]["derivation_depth"] for it in remaining]
    assert depths == sorted(depths)
    assert max(depths) == 2
    assert s.frontier.stop_reason == "input_search_depth_or_tasks_exhausted"
    assert all(t["execution"] is None and t["actual"] is False for t in s.frontier.tasks)


def test_each_declared_route_kind_slot_once_per_parent():
    doc = profile()
    doc["sampling_kinds"] = ["neighbor", "jump"]
    doc["search"].update(max_depth=1, samples_per_class=3)
    s = fresh(doc)["scheduler"]
    pairs = drain(s, doc)
    assert len(pairs) == 2 * 2 * 3
    keys = [(it[0]["mutation_route"]["surface_id"], it[0]["sampling_kind"], it[0]["sample_slot"])
            for it in pairs]
    assert len(set(keys)) == len(keys)
    assert {k[0] for k in keys} == {"P01", "W02"}
    assert {k[1] for k in keys} == {"neighbor", "jump"}
    assert {k[2] for k in keys} == {0, 1, 2}


def test_no_change_and_duplicate_do_not_expand():
    doc = profile()
    s = fresh(doc)["scheduler"]
    first, child = generate(s, doc)
    count = len(s.frontier.nodes)
    # Next task uses the same root and route, different RNG slot. Reusing an
    # already generated content is a dedupe test, not execution evidence.
    second = s.next()
    duplicate = deepcopy(child)
    duplicate["parent_content_sha256"] = second["parent_content_sha256"]
    s.frontier.planned(second["task_id"], duplicate)
    with pytest.raises(ValueError, match="duplicate"):
        s.frontier.submitted(second["task_id"])
    s.frontier.finish(second["task_id"], rejected=True, reason="duplicate_content")
    assert len(s.frontier.nodes) == count
    assert s.frontier.task(second["task_id"])["generation_outcome"] == "duplicate_content"
    third = s.next()
    # Explicit malformed no-op proposal fixture: never executed or qualified.
    # Even before model verification, the worklist blocks its submission.
    unchanged = deepcopy(child)
    unchanged.pop("mutation_plan", None)
    unchanged["final_config"] = deepcopy(third["entry"]["final_config"])
    unchanged["parent_content_sha256"] = third["parent_content_sha256"]
    for op in unchanged["operations"]:
        op["input_after_sha256"] = op["input_before_sha256"]
    unchanged["content_sha256"] = content_identity(unchanged)
    s.frontier.planned(third["task_id"], unchanged)
    with pytest.raises(ValueError, match="no-change"):
        s.frontier.submitted(third["task_id"])
    s.frontier.finish(third["task_id"], rejected=True, reason="compile_time_no_change")
    assert len(s.frontier.nodes) == count
    assert s.frontier.task(third["task_id"])["generation_outcome"] == "compile_time_no_change"


def test_capacity_halts_explicitly_and_keeps_pending_tasks():
    doc = profile()
    doc["search"]["max_nodes"] = 2
    s = fresh(doc)["scheduler"]
    generate(s, doc)
    assert len(s.frontier.nodes) == 2
    assert s.frontier.stop_reason == "input_search_max_nodes_reached_work_retained"
    pending = [t for t in s.frontier.tasks if t["status"] == "pending"]
    assert pending
    with pytest.raises(ValueError, match="max_nodes"):
        s.next()
    restored = InputSearchFrontier.from_dict(s.frontier.to_dict())
    assert [t for t in restored.tasks if t["status"] == "pending"] == pending


def test_feedback_cannot_change_task_order_rng_or_generated_content():
    doc = profile()
    a, b = fresh(doc), fresh(doc)
    for i in range(9):
        chosen, child = generate(a["scheduler"], doc)
        b["feedback"].record({"target_id": "discovery.late_endpoint.fuzz_v1", "surface_id": "P01",
            "boundary_class": "finite", "content_sha256": child["content_sha256"],
            "actual_run": False, "execution_stage": "planned", "oracle_verdict": "unknown"})
        b["scheduler"].advance_feedback(b["feedback"])
        other, other_child = generate(b["scheduler"], doc)
        assert chosen == other
        assert child == other_child
    assert a["scheduler"].frontier.to_dict() == b["scheduler"].frontier.to_dict()


def test_scheduler_checkpoint_roundtrip_and_unknown_schema_fail_closed():
    doc = profile()
    st = fresh(doc)
    for _ in range(4):
        generate(st["scheduler"], doc)
    state = st["scheduler"].to_dict()
    restored = ExploratoryScheduler.from_dict(state, st["corpus"], st["feedback"])
    assert restored.to_dict() == state
    assert generate(restored, doc) == generate(st["scheduler"], doc)
    broken = deepcopy(state)
    broken["frontier"]["schema"] = "future"
    broken["selection_identity"] = canonical_hash({k:v for k,v in broken.items() if k != "selection_identity"})
    with pytest.raises(ValueError, match="schema"):
        ExploratoryScheduler.from_dict(broken, st["corpus"], st["feedback"])


@pytest.mark.parametrize("key,value", [("max_depth", 0), ("max_depth", True), ("max_nodes", 0),
                                       ("samples_per_class", 0), ("samples_per_class", 65)])
def test_strict_bounds(key, value):
    doc = profile()
    doc["search"][key] = value
    with pytest.raises(ValueError):
        fresh(doc)


def test_legacy_policies_remain_opt_in_and_required_catalog_unchanged():
    before = required_catalog_hash()
    c = fresh()["corpus"]
    for policy in (None, "feedback", "fair"):
        s = ExploratoryScheduler(c, policy=policy)
        assert "frontier" not in s.to_dict()
        assert "task_id" not in s.next()
    assert required_catalog_hash() == before


def test_task_budget_explicit_stop_preserves_unexpanded_model_nodes():
    doc = profile()
    doc["search"]["max_tasks"] = 1
    s = fresh(doc)["scheduler"]
    assert s.frontier.stop_reason == "input_search_max_tasks_reached_work_retained"
    assert len(s.frontier.nodes) == 1 and s.frontier.tasks == []
    root = next(iter(s.frontier.nodes.values()))
    assert root["expansion_status"] == "task_budget_blocked"
    assert root["required_task_count"] == 4
    assert InputSearchFrontier.from_dict(s.frontier.to_dict()).to_dict() == s.frontier.to_dict()
