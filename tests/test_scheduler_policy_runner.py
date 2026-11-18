"""Offline policy-to-generation wiring; fixtures are not execution evidence.

Fair is an input/model selection baseline for a fixed corpus, not a BFS/DFS
state planner. These tests do not disable the separate corpus admission path.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.campaign.corpus import Corpus, corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import (
    ExploratoryCycleError, _derive_rng_seed, run_exploratory_cycle,
)
from r2f.campaign.scheduler import ExploratoryScheduler, validated_feedback
from r2f.core.evidence import canonical_hash
from test_exploratory_checkpoint import CYCLE, SOURCE, profile as checkpoint_profile
from test_fuzz_feedback_loop import make_effective_corpus as make_corpus, profile as cycle_profile
from test_scheduler_feedback_epoch import observation


def different_feedback(corpus, variant):
    """Different, valid synthetic coverage/edge/oracle histories; never live."""
    log = FeedbackLog()
    entries = corpus.selectable()
    chosen = entries[:1] if variant == 0 else list(reversed(entries))[:3]
    for i, entry in enumerate(chosen):
        row = observation(entry)
        row["evidence_origin"] = "scheduler_policy_synthetic_fixture"
        if variant:
            row["oracle_verdict"] = "fail"
            row["typed_facts"]["state_after"]["creation_order_class"] = (
                "writer_first" if i % 2 else "reader_first")
        event = log.record(row)
        assert event["complete"] and event["coverage"] and event["edge_id"]
    assert validated_feedback(log.events).to_dict() == log.to_dict()
    return log


def input_projection(result):
    projected = []
    for row in result["rows"]:
        item = {key: deepcopy(row.get(key)) for key in (
            "parent_content_sha256", "seed", "sampling_kind", "chain_depth",
            "mutation_plan", "mutation_plan_sha256", "input_layer_receipts",
            "final_input", "content_sha256", "effective_input_sha256",
            "target_id", "target_revision", "rmw", "arm", "generation_status",
            "reason_code", "binding", "oracle_verdict", "actual_run", "execution_stage",
            "candidate_eligible", "feedback_eligible", "no_effective_input_reason",
            "mutation_receipt",
        )}
        if "normalized_config_sha256" in item["binding"]:
            # This is a runtime envelope digest (run_id/cell_id included), NOT
            # input identity. Verify it against its own persisted bytes before
            # comparing the remaining stable binding semantics across runs.
            config = json.loads(Path(row["runtime_config_path"]).read_text())
            assert item["binding"].pop("normalized_config_sha256") == canonical_hash(config)
        projected.append(item)
    return projected


def plan_cycle(out, doc, state, *, maximum=None, hook_wrapper=None):
    hook = make_checkpoint_hook(out, profile=doc, cycle_id=CYCLE,
                               source_manifest=SOURCE, continuation=state["continuation"])
    if hook_wrapper:
        hook = hook_wrapper(hook)
    with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
        return run_exploratory_cycle(
            out, **state, profile=doc, run_id=out.name, execute=False,
            max_inputs=maximum, checkpoint_hook=hook)


def policy_state(doc, policy):
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    # A new checkpoint starts with no attempts; historical feedback would
    # require its matching cumulative budget. Do not weaken that invariant.
    feedback = FeedbackLog()
    kwargs = {} if policy is None else {"policy": policy}
    scheduler = ExploratoryScheduler(
        corpus, feedback, rng_seed=17, warmup=doc["budget"]["warmup_inputs"], **kwargs)
    return {"corpus": corpus, "feedback": feedback, "scheduler": scheduler,
            "continuation": None}


@pytest.mark.parametrize("warmup", [0, 3])
@pytest.mark.parametrize("wide", [False, True])
def test_fair_unified_cycle_feedback_independent_inputs_sampling_and_rng(tmp_path, warmup, wide):
    inputs = make_corpus()
    doc = cycle_profile()
    doc["budget"] = {"max_inputs": 8, "warmup_inputs": warmup}
    if wide:
        doc.update(sampling_kinds=["representative", "boundary"],
                   sampling_weight={"representative": 1, "boundary": 2})
    outcomes = []
    logs = []
    for variant in (0, 1):
        corpus = Corpus.from_dict(inputs.to_dict())
        before = corpus.to_dict()
        feedback = different_feedback(corpus, variant)
        feedback_before = deepcopy(feedback.events)
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=71, warmup=warmup, policy="fair")
        runtime = Mock(side_effect=AssertionError("execute=False must never call runtime"))
        with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
            result = run_exploratory_cycle(
                tmp_path / str(variant), corpus=corpus, feedback=feedback, scheduler=scheduler,
                profile=doc, run_id="offline-fair-policy", execute=False, runtime=runtime)
        runtime.assert_not_called()
        assert not result["rejected"] and len(result["rows"]) == 8
        assert all(row["execution_stage"] == "planned" and not row["actual_run"]
                   for row in result["rows"])
        for row in result["rows"]:
            assert row["final_input"] != corpus.entries[row["parent_content_sha256"]]["final_config"]
            assert all(op["input_before_sha256"] != op["input_after_sha256"]
                       for op in row["mutation_plan"]["operations"])
        assert corpus.to_dict() == before  # fixed corpus is the comparison condition
        assert feedback.events == feedback_before
        assert not result["abort"]
        logs.append(result["selection_log"])
        outcomes.append(input_projection(result))
    left, right = (log["selections"] for log in logs)
    assert logs[0]["feedback_identity"] != logs[1]["feedback_identity"]
    assert [e["selection_sha256"] for e in left] != [e["selection_sha256"] for e in right]
    assert [e["decision_sha256"] for e in left] == [e["decision_sha256"] for e in right]
    assert [(e["content_sha256"], e.get("sampling_kind")) for e in left] == [
        (e["content_sha256"], e.get("sampling_kind")) for e in right]
    assert outcomes[0] == outcomes[1]
    if wide:
        assert all(row["sampling_kind"] in doc["sampling_kinds"] for row in outcomes[0])


@pytest.mark.parametrize("bad_token", [None, "", "a" * 63, "a" * 65, "g" * 64,
                                         "A" * 64, 123, False, [], {}])
def test_fair_rng_rejects_missing_or_malformed_decision_without_fallback(bad_token):
    corpus = make_corpus()
    scheduler = ExploratoryScheduler(corpus, FeedbackLog(), rng_seed=11, warmup=0, policy="fair")
    selection = scheduler.next()
    if bad_token is None:
        selection.pop("decision_sha256")
    else:
        selection["decision_sha256"] = bad_token
    assert selection["selection_sha256"]  # an audit hash is not a fallback
    with pytest.raises(ExploratoryCycleError, match="decision_sha256"):
        _derive_rng_seed(scheduler, selection, selection["entry"])


def test_fair_rng_rejects_well_formed_but_unaccepted_decision():
    corpus = make_corpus()
    scheduler = ExploratoryScheduler(corpus, FeedbackLog(), rng_seed=11, warmup=0, policy="fair")
    selection = scheduler.next()
    selection["decision_sha256"] = "f" * 64 if selection["decision_sha256"] != "f" * 64 else "0" * 64
    with pytest.raises(ExploratoryCycleError, match="decision_sha256"):
        _derive_rng_seed(scheduler, selection, selection["entry"])


def test_fair_rng_uses_only_accepted_decision_not_feedback_audit_hash():
    corpus = make_corpus()
    seeds, tokens, audit_hashes = [], [], []
    for variant in (0, 1):
        scheduler = ExploratoryScheduler(corpus, different_feedback(corpus, variant),
                                         rng_seed=11, warmup=0, policy="fair")
        selection = scheduler.next()
        expected = int(canonical_hash({"cycle_rng_seed": 11,
            "decision_sha256": selection["decision_sha256"], "sequence": selection["sequence"],
            "parent_content_sha256": selection["entry"]["content_sha256"]})[:16], 16)
        seeds.append(_derive_rng_seed(scheduler, selection, selection["entry"]))
        tokens.append(selection["decision_sha256"])
        audit_hashes.append(selection["selection_sha256"])
        assert seeds[-1] == expected
    assert audit_hashes[0] != audit_hashes[1]
    assert tokens[0] == tokens[1] and seeds[0] == seeds[1]


@pytest.mark.parametrize("damage", ["audit_hash", "phase", "context", "sequence", "parent"])
def test_fair_rng_rejects_event_or_parent_not_accepted_by_scheduler(damage):
    corpus = make_corpus()
    scheduler = ExploratoryScheduler(corpus, FeedbackLog(), rng_seed=11, warmup=0, policy="fair")
    selection = scheduler.next()
    parent = deepcopy(selection["entry"])
    if damage == "audit_hash":
        selection["selection_sha256"] = "a" * 64
    elif damage == "phase":
        selection["phase"] = "external-forged-event"
    elif damage == "context":
        selection["decision_context"]["rng_seed"] += 1
    elif damage == "sequence":
        selection["sequence"] = 999
    else:
        parent["content_sha256"] = "a" * 64
    with pytest.raises(ExploratoryCycleError, match="decision_sha256"):
        _derive_rng_seed(scheduler, selection, parent)


def test_fair_rng_checks_accepted_context_hash_not_token_format_alone():
    corpus = make_corpus()
    scheduler = ExploratoryScheduler(corpus, FeedbackLog(), rng_seed=11, warmup=0, policy="fair")
    selection = scheduler.next()
    # Even a copied event matching an altered log must keep its context/token
    # binding. This is a negative gate, not a way to validate outside events.
    scheduler.selection_log[0]["decision_context"]["rng_seed"] += 1
    selection["decision_context"] = deepcopy(scheduler.selection_log[0]["decision_context"])
    with pytest.raises(ExploratoryCycleError, match="decision_sha256"):
        _derive_rng_seed(scheduler, selection, selection["entry"])


@pytest.mark.parametrize("policy", [None, "feedback"])
def test_feedback_rng_keeps_original_hash_material(policy):
    corpus = make_corpus()
    kwargs = {} if policy is None else {"policy": policy}
    scheduler = ExploratoryScheduler(corpus, different_feedback(corpus, 1), rng_seed=41,
                                     warmup=0, **kwargs)
    for selection in scheduler.plan(7):
        entry = selection["entry"]
        expected = int(canonical_hash({"cycle_rng_seed": scheduler.rng_seed,
            "selection_sha256": selection["selection_sha256"], "sequence": selection["sequence"],
            "parent_content_sha256": entry["content_sha256"]})[:16], 16)
        assert _derive_rng_seed(scheduler, selection, entry) == expected
        if policy is None:
            assert "policy" not in selection and "decision_sha256" not in selection


@pytest.mark.parametrize("policy", [None, "fair", "feedback"])
@pytest.mark.parametrize("authored_noop", [False, True])
def test_policy_planning_interrupt_resume_matches_continuous_inputs(tmp_path, policy, authored_noop):
    # Mendel owns checkpoint_profile(). Independently retain the original on-disk
    # finite/no-op recipe as a mixed rejected+pending continuation regression.
    doc = (json.loads((Path(__file__).resolve().parents[1] /
            "config/s1_fuzz/profiles/parameter.json").read_text())
           if authored_noop else checkpoint_profile())
    doc["budget"] = {"max_inputs": 5, "warmup_inputs": 1}
    doc.update(sampling_kinds=["representative", "boundary"],
               sampling_weight={"representative": 1, "boundary": 2})
    whole = plan_cycle(tmp_path / "whole", doc, policy_state(doc, policy))

    class Interrupted(BaseException):
        pass

    recorded_prefix = []
    pending_sequence = []

    def interrupt(hook):
        def save_then_interrupt(payload):
            hook(payload)
            if payload["phase"] == "recorded":
                # Persisted rows are per invocation, whereas the budget and
                # selection log are cumulative. Do not include the pending row.
                ref = payload["last_case_ref"]
                recorded_prefix.append(json.loads(
                    (Path(ref["case_dir"]) / "row.json").read_text()))
                assert payload["row_count"] == len(recorded_prefix)
            if payload["phase"] == "planned":
                pending_sequence.append(payload["pending_selection"]["sequence"])
                assert pending_sequence[-1] == len(recorded_prefix)
                assert payload["row_count"] == len(recorded_prefix)
                raise Interrupted()
        return save_then_interrupt

    with pytest.raises(Interrupted):
        plan_cycle(tmp_path / "interrupted", doc, policy_state(doc, policy), hook_wrapper=interrupt)
    old = {str(p.relative_to(tmp_path / "interrupted")): p.read_bytes()
           for p in (tmp_path / "interrupted").rglob("*") if p.is_file()}
    state = load_checkpoint(tmp_path / "interrupted", profile=doc, cycle_id=CYCLE,
                            source_manifest=SOURCE, output_dir=tmp_path / "resumed")
    resumed = plan_cycle(tmp_path / "resumed", doc, state)
    combined = {"rows": recorded_prefix + resumed["rows"]}
    assert input_projection(whole) == input_projection(combined)
    assert len(combined["rows"]) == 5
    assert input_projection({"rows": whole["rejected"]}) == input_projection({
        "rows": [r for r in combined["rows"] if r.get("generation_status") == "rejected"]})
    assert len(resumed["rows"]) == 5 - pending_sequence[0]
    if authored_noop:
        assert whole["rejected"]  # never silently turn the legacy recipe valid
        if policy in (None, "feedback"):
            assert recorded_prefix and all(r["reason_code"] == "no_effective_input"
                                           for r in recorded_prefix)
        for row in whole["rejected"]:
            assert row["binding"]["status"] == "not_executed"
            assert not row["actual_run"] and not row["feedback_eligible"]
            assert "replay_path" not in row
        for path in (tmp_path / "whole", tmp_path / "interrupted", tmp_path / "resumed"):
            for row in list(recorded_prefix) + whole["rejected"] + resumed["rejected"]:
                assert not (path / "cases" / row["cell_id"] / "capsule.json").exists()
    assert whole["selection_log"] == resumed["selection_log"]
    assert resumed["budget"]["selected"] == 5 and resumed["budget"]["actual"] == 0
    assert old == {str(p.relative_to(tmp_path / "interrupted")): p.read_bytes()
                   for p in (tmp_path / "interrupted").rglob("*") if p.is_file()}
    if policy is None:
        assert "policy" not in resumed["selection_log"]
    else:
        assert resumed["selection_log"]["policy"] == policy


@pytest.mark.parametrize("damage", ["missing", "malformed", "unaccepted"])
def test_unified_fair_cycle_rejects_bad_decision_before_input_or_runtime(tmp_path, monkeypatch, damage):
    corpus, feedback = make_corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=11, warmup=0, policy="fair")
    original = scheduler.next
    def tampered_next():
        selection = original()
        if damage == "missing":
            selection.pop("decision_sha256")
        elif damage == "malformed":
            selection["decision_sha256"] = "not-a-decision"
        else:
            selection["decision_sha256"] = "0" * 64
        return selection
    monkeypatch.setattr(scheduler, "next", tampered_next)
    runtime = Mock(side_effect=AssertionError("bad decision cannot invoke runtime"))
    with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
        with pytest.raises(ExploratoryCycleError, match="decision_sha256"):
            run_exploratory_cycle(tmp_path / "bad", corpus=corpus, feedback=feedback,
                                  scheduler=scheduler, profile=cycle_profile(),
                                  execute=False, runtime=runtime, max_inputs=1)
    runtime.assert_not_called()
    assert not (tmp_path / "bad/cases").exists()
    assert feedback.events == []


@pytest.mark.parametrize("policy", ["fair", "feedback"])
def test_policy_keeps_fixture_feedback_ingest_and_corpus_admission_active(tmp_path, policy):
    # Public-adapter-shaped raw fixture only. No ROS/DDS, namespace, real
    # execution qualification or natural SUT candidate is established here.
    from test_feedback_epoch_runner import adapter_runtime
    corpus, feedback = make_corpus(), FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=4, warmup=0, policy=policy)
    with patch("r2f.campaign.runner.source_manifest", return_value=deepcopy(SOURCE)):
        result = run_exploratory_cycle(
            tmp_path / policy, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=cycle_profile(), run_id="policy-offline-adapter", execute=True,
            runtime=adapter_runtime, max_inputs=2)
    assert result["runtime_formal"] is False and not result["abort"]
    assert len(result["rows"]) == len(feedback.events) == 2
    assert all(row["corpus_offer"]["admitted"] for row in result["rows"])
    assert all(event["coverage"] and event["edge_id"] for event in feedback.events)
    assert scheduler.feedback.events == feedback.events
    assert [c["feedback_event_count"] for c in scheduler.selection_contexts] == [0, 1]
