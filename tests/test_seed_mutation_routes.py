"""Pure input metadata/planning contracts. Never invoke ROS/DDS or a runtime.

The three legacy byte digests were captured from main's read-only e35c913
reference before this card's edits. New end-to-end tests use the actual source
manifest (dirty/unfrozen is preserved), not a patched qualification gate.
"""
from copy import deepcopy
from itertools import permutations
import json
from pathlib import Path

import pytest

from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.campaign.corpus import Corpus, content_identity, corpus_from_profile, corpus_identity
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.identity import source_manifest
from r2f.campaign.mutation_routes import (
    ROUTES_SCHEMA, canonical_route_record, inherit_routes, normalize_routes, route_view,
    validate_route_entry,
)
from r2f.campaign.runner import _declared_route_layers, run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler, replay_selection, required_catalog_hash
from r2f.core.evidence import canonical_hash
from r2f.modules.s1.targets import exploratory_target_declarations

ROOT = Path(__file__).resolve().parents[1]
CYCLE = "common-baseline"


def profile():
    doc = json.loads((ROOT / "config/s1_fuzz/profiles/common-baseline-routes.json").read_text())
    doc["rmws"] = doc["rmws"][:1]
    doc["budget"].pop("max_seconds", None)
    return doc


def record():
    return corpus_from_profile(profile(), cycle_id=CYCLE).selectable()[0]


def fresh(doc, *, policy="fair", feedback=None):
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    feedback = feedback or FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=41, warmup=0, policy=policy)
    return {"corpus": corpus, "feedback": feedback, "scheduler": scheduler, "continuation": None}


def plan(output, doc, *, state=None, count=2, checkpoint=False):
    state = state or fresh(doc)
    # The actual current source identity must agree on both sides; no source
    # gate is patched, and execute=False cannot dispatch the tripwire runtime.
    source = source_manifest(ROOT)
    hook = make_checkpoint_hook(output, profile=doc, cycle_id=CYCLE,
                                source_manifest=source, continuation=state["continuation"]) if checkpoint else None

    def forbidden_runtime(*args, **kwargs):
        raise AssertionError("offline route planning must never dispatch a runtime")

    report = run_exploratory_cycle(
        output, **state, profile=doc, max_inputs=count, execute=False,
        runtime=forbidden_runtime, checkpoint_hook=hook, run_id=output.name)
    return report, source


def test_content_hash_excludes_routes_and_counts_one_payload():
    base = record()
    legacy = {key: value for key, value in base.items()
              if key not in {"mutation_routes", "mutation_routes_schema"}}
    assert content_identity(base) == content_identity(legacy)
    corpus = Corpus()
    for route in base["mutation_routes"]:
        entry = deepcopy(base)
        entry["mutation_routes"] = [route]
        entry.pop("mutation_route_origins", None)
        entry.update(route)
        corpus.add_seed(entry)
    assert len(corpus.entries) == len(corpus.selectable()) == 1
    assert corpus.to_dict()["pinned_seed_count"] == 1
    assert next(iter(corpus.entries.values()))["mutation_routes"] == base["mutation_routes"]


@pytest.mark.parametrize("method", ["add_seed", "offer"])
def test_union_order_independent_canonical_fields_and_schedule(method):
    base = record()
    states = []
    for order in permutations(base["mutation_routes"]):
        corpus = Corpus()
        for route in order:
            entry = deepcopy(base)
            entry["mutation_routes"] = [route, route]
            entry.pop("mutation_route_origins", None)
            entry.update(route)
            if method == "add_seed":
                corpus.add_seed(entry)
            else:
                entry.pop("pinned_seed")
                corpus.offer(entry, executed=True, evidence_complete=True,
                             observed_edge="offline-fixture-edge")
        assert len(corpus.entries) == 1
        canonical = next(iter(corpus.entries.values()))
        assert canonical["mutation_routes"] == normalize_routes(base["mutation_routes"])
        assert {key: canonical[key] for key in ("surface_id", "boundary_class", "mutator_id")} == base["mutation_routes"][0]
        assert corpus_identity(corpus) == corpus_identity(Corpus.from_dict(corpus.to_dict()))
        states.append((corpus.selectable(), ExploratoryScheduler(corpus, rng_seed=7, policy="fair").plan(8)))
    assert states[0] == states[1]


@pytest.mark.parametrize("policy", [None, "feedback", "fair"])
def test_route_survives_select_record_tail_and_context_restore(policy):
    base = record()
    corpus = Corpus()
    corpus.add_seed(base)
    scheduler = ExploratoryScheduler(corpus, rng_seed=17, warmup=0, policy=policy)
    selected = scheduler.plan(6)
    assert {event["entry"]["surface_id"] for event in selected} == {"P01", "W02"}
    assert len({event["content_sha256"] for event in selected}) == 1
    for event, context in zip(selected, scheduler.selection_contexts):
        assert event["entry"]["mutation_route"] == event["mutation_route"]
        assert event["entry"]["mutator_id"] == event["mutation_route"]["mutator_id"]
        assert context["mutation_route"] == event["mutation_route"]
        assert context["route_selection"]["observation_guards_discharged"] is False
    rebuilt = ExploratoryScheduler.from_dict(scheduler.to_dict(), Corpus.from_dict(corpus.to_dict()), FeedbackLog())
    assert rebuilt.to_dict() == scheduler.to_dict()
    assert rebuilt.next() == scheduler.next()
    assert replay_selection(scheduler.to_dict(), corpus, [])["valid"]


def test_corpus_persist_restore_and_tampered_routing_rejected(tmp_path):
    corpus = corpus_from_profile(profile(), cycle_id=CYCLE)
    path = tmp_path / "corpus.json"
    path.write_text(json.dumps(corpus.to_dict()))
    assert Corpus.load(path).to_dict() == corpus.to_dict()
    for mutate in (lambda entry: entry["mutation_routes"].reverse(),
                   lambda entry: entry["mutation_routes"][1].update(mutator_id="M01"),
                   lambda entry: entry.update(surface_id="W02"),
                   lambda entry: entry.update(mutation_routes_schema="r2f.mutation-routes/v99")):
        damaged = corpus.to_dict()
        mutate(damaged["entries"][0])
        with pytest.raises(ValueError):
            Corpus.from_dict(damaged)


@pytest.mark.parametrize("damage", ["version", "key", "missing_key", "target", "surface", "boundary", "mutator", "projected_mutator", "cycle_surface", "profile_surface", "capability", "revision", "contract"])
def test_invalid_profile_routes_fail_closed(damage):
    doc = profile()
    route = doc["cycles"][0]["seed_specs"][0]["mutation_routes"][1]
    if damage == "version":
        doc["composition"]["mutation_routes_schema"] = "r2f.mutation-routes/v2"
    elif damage == "key":
        route["unexpected"] = True
    elif damage == "missing_key":
        route.pop("mutator_id")
    elif damage == "target":
        doc["cycles"][0]["target_id"] = "discovery.interruption_recovery.fuzz_v1"
        doc["cycles"][0]["seed_specs"][0]["target_id"] = doc["cycles"][0]["target_id"]
        doc["target_ids"] = [doc["cycles"][0]["target_id"]]
    elif damage == "surface":
        route["surface_id"] = "P04"
    elif damage == "boundary":
        route["boundary_class"] = "infinite"
    elif damage == "mutator":
        route["mutator_id"] = "M99"
    elif damage == "projected_mutator":
        route["mutator_id"] = "M01"  # catalog W02 supports it; target projection does not
    elif damage == "cycle_surface":
        doc["cycles"][0]["surfaces"] = ["P01"]
    elif damage == "profile_surface":
        doc["surfaces"] = ["P01"]
    elif damage == "capability":
        doc["runtime_capability"] = "unverified_runtime"
    else:
        entry = record()
        entry["target_revision" if damage == "revision" else "contract_hash"] = 1 if damage == "revision" else "f" * 64
        with pytest.raises(ValueError):
            Corpus().add_seed(entry)
        return
    with pytest.raises(ValueError):
        corpus_from_profile(doc, cycle_id=CYCLE)


def test_missing_opt_in_or_routes_never_silently_ignored():
    doc = profile()
    doc["composition"].pop("mutation_routes_schema")
    with pytest.raises(ValueError, match="opt-in"):
        corpus_from_profile(doc, cycle_id=CYCLE)
    doc = profile()
    doc["cycles"][0]["seed_specs"][0].pop("mutation_routes")
    with pytest.raises(ValueError):
        corpus_from_profile(doc, cycle_id=CYCLE)


def test_template_surface_is_not_a_new_route_authorization_gate():
    doc = profile()
    doc["surfaces"] = doc["cycles"][0]["surfaces"] = ["W02"]
    doc["cycles"][0]["seed_specs"][0]["mutation_routes"] = [
        {"surface_id": "W02", "boundary_class": "finite", "mutator_id": "M03"}]
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    row = corpus.selectable()[0]
    assert row["seed_id"] == "P01"
    assert row["surface_id"] == "W02"
    assert row["final_config"] == record()["final_config"]


def test_noexecute_end_to_end_two_single_faces_unchanged_fields(tmp_path):
    required = required_catalog_hash()
    report, _ = plan(tmp_path / "singles", profile(), count=2)
    assert report["budget"]["selected"] == report["budget"]["generated"] == 2
    assert report["budget"]["actual"] == report["budget"]["attempted"] == 0
    assert report["feedback"]["events"] == []
    assert {row["surface_id"] for row in report["rows"]} == {"P01", "W02"}
    assert len({row["parent_content_sha256"] for row in report["rows"]}) == 1
    for row in report["rows"]:
        before, after = row["parent_final_input"], row["final_input"]
        field = "reader_delay_sec" if row["surface_id"] == "P01" else "publish_interval_sec"
        assert {key for key in before if before[key] != after[key]} == {field}
        assert row["mutation_route"]["surface_id"] == row["surface_id"]
        assert row["execution_stage"] == "planned" and row["actual_run"] is False
        assert all(flag is False for flag in row["mutation_receipt"].values())
        capsule = json.loads(Path(row["replay_path"]).read_text())
        assert capsule["execution"] is None
        assert not capsule.get("execution_history")
        config = json.loads(Path(row["runtime_config_path"]).read_text())
        assert config["mutation_route"] == row["mutation_route"]
        assert row["binding"]["status"] == "preserved"
    assert required_catalog_hash() == required


def test_compose_honors_every_declared_route_and_does_not_widen(tmp_path):
    doc = profile()
    doc["chain_length"] = 2
    result, _ = plan(tmp_path / "compose", doc)
    for row in result["rows"]:
        assert len(row["mutation_chain"]) == 2
        assert {op["surface_id"] for op in row["mutation_chain"]} == {"P01", "W02"}
        assert all(op["mutator_id"] == "M03" for op in row["mutation_chain"])
        assert row["parent_final_input"]["reader_delay_sec"] != row["final_input"]["reader_delay_sec"]
        assert row["parent_final_input"]["publish_interval_sec"] != row["final_input"]["publish_interval_sec"]
    # An extra profile surface cannot grant a route absent from the seed.
    entry = record()
    entry["mutation_routes"] = entry["mutation_routes"][:1]
    entry.pop("mutation_route_origins", None)
    view = route_view(entry, entry["mutation_routes"][0])
    contract = exploratory_target_declarations()[entry["target_id"]].exploration_contract
    with pytest.raises(ValueError, match="not_composable"):
        _declared_route_layers(view, contract, 2)


def test_projection_parameter_routes_are_generic(tmp_path):
    doc = profile()
    target = "builtin.discovery_projection_reconciliation.fuzz_v1"
    routes = [{"surface_id": sid, "boundary_class": "enum_exhaustive", "mutator_id": "M02"}
              for sid in ("P02", "P03")]
    spec = {"seed_id": "P02", "target_id": target, "boundary_class": "enum_exhaustive",
            "executable": True, "mutation_routes": routes}
    doc.update(target_ids=[target], surfaces=["P02", "P03"], sampling_kinds=["representative"], seed_specs=[spec])
    doc["cycles"][0].update(target_id=target, surfaces=["P02", "P03"], seed_specs=[spec])
    result, _ = plan(tmp_path / "projection", doc)
    assert {row["surface_id"] for row in result["rows"]} == {"P02", "P03"}
    assert len({row["parent_content_sha256"] for row in result["rows"]}) == 1
    assert all(row["binding"]["status"] == "preserved" for row in result["rows"])


def test_admitted_child_inherits_metadata_not_selected_view():
    parent = record()
    child = {key: deepcopy(parent[key]) for key in
             ("target_id", "target_revision", "rmw", "contract_hash", "seed_id", "final_config")}
    child["final_config"]["reader_delay_sec"] = 0.141
    child["parent_content_sha256"] = parent["content_sha256"]
    inherit_routes(parent, child)
    assert "mutation_route" not in child
    corpus = Corpus()
    corpus.add_seed(parent)
    offered = corpus.offer(child, executed=True, evidence_complete=True, observed_edge="fixture-edge")
    assert offered["admitted"]
    restored = Corpus.from_dict(corpus.to_dict())
    assert restored.entries[offered["content_sha256"]]["mutation_routes"] == parent["mutation_routes"]
    assert content_identity(child) == offered["content_sha256"]


def test_fair_route_sampling_and_mutation_rng_ignore_feedback(tmp_path):
    doc = profile()
    a = fresh(doc)
    log = FeedbackLog()
    base = a["corpus"].selectable()[0]
    log.record({**base, "actual_run": False, "oracle_verdict": "fail", "execution_stage": "planned"})
    b = fresh(doc, feedback=log)
    ra, _ = plan(tmp_path / "fair-empty", doc, state=a, count=4)
    rb, _ = plan(tmp_path / "fair-other", doc, state=b, count=4)
    keys = ("parent_content_sha256", "mutation_route", "sampling_kind", "seed", "mutation_plan", "final_input", "content_sha256")
    assert [{key: row[key] for key in keys} for row in ra["rows"]] == [
        {key: row[key] for key in keys} for row in rb["rows"]]
    assert [event["decision_sha256"] for event in a["scheduler"].selection_log] == [
        event["decision_sha256"] for event in b["scheduler"].selection_log]
    assert a["scheduler"].to_dict() != b["scheduler"].to_dict()  # audit still retains feedback


@pytest.mark.parametrize("policy", [None, "feedback", "fair"])
def test_real_checkpoint_load_resume_matches_whole_planning(tmp_path, policy):
    doc = profile()
    first_state = fresh(doc, policy=policy)
    first, source = plan(tmp_path / "first", doc, state=first_state, count=2, checkpoint=True)
    original = (tmp_path / "first/checkpoint.json").read_bytes()
    restored = load_checkpoint(tmp_path / "first/checkpoint.json", profile=doc, cycle_id=CYCLE,
                               source_manifest=source, output_dir=tmp_path / "resumed")
    resumed, _ = plan(tmp_path / "resumed", doc, state=restored, count=4, checkpoint=True)
    whole, _ = plan(tmp_path / "whole", doc, state=fresh(doc, policy=policy), count=4, checkpoint=True)
    keys = ("parent_content_sha256", "mutation_route", "content_sha256", "mutation_plan", "final_input", "seed")
    assert [{key: row[key] for key in keys} for row in first["rows"] + resumed["rows"]] == [
        {key: row[key] for key in keys} for row in whole["rows"]]
    assert resumed["selection_log"] == whole["selection_log"]
    assert (tmp_path / "first/checkpoint.json").read_bytes() == original
    damaged = json.loads(original)
    entry = damaged["state"]["corpus"]["entries"][0]
    entry["mutation_routes"][1]["mutator_id"] = "M01"
    damaged["checkpoint_sha256"] = canonical_hash({key: value for key, value in damaged.items() if key != "checkpoint_sha256"})
    path = tmp_path / "tampered-evidence" / "tampered.json"
    path.parent.mkdir()
    path.write_text(json.dumps(damaged))
    with pytest.raises(ValueError, match="mutation route"):
        load_checkpoint(path, profile=doc, cycle_id=CYCLE, source_manifest=source,
                        output_dir=tmp_path / "bad-resume")


@pytest.mark.parametrize("policy,corpus_hash,state_hash,events_hash", [
    (None, "8d2ac7ecaeb3533e25871de4d30838b02774960578758312247b1eb1cec7d449", "8472d2b3b8825a14a99d94dbf86fcf825adf6387b4feaf8c771720727b2c0043", "9085a8fe76876489aef015ef699e5fd8f2a25abfa012c4b0a794166db1555223"),
    ("feedback", "8d2ac7ecaeb3533e25871de4d30838b02774960578758312247b1eb1cec7d449", "8371017173bbf0ca520be5972340d7b1fe4e5d3dc54094907c596d9be31bf2ed", "45243eb19f13726d7cfbd1cec6565f08ae246e8982cefd0f9ba23c37721bb4e0"),
    ("fair", "8d2ac7ecaeb3533e25871de4d30838b02774960578758312247b1eb1cec7d449", "a39ff73ad8b5edec9109b999b7f82e4145f6fa2d6ef3a663379005dd8f722f34", "edeeaaa1f68ad98bee276ee34e90bc47a15f366a22c6bbcc4f37d9aea8262752"),
])
def test_legacy_corpus_hash_and_selection_bytes_unchanged(policy, corpus_hash, state_hash, events_hash):
    corpus = corpus_from_profile(ROOT / "config/s1_fuzz/profiles/composed.json", cycle_id="late-endpoint-composed")
    scheduler = ExploratoryScheduler(corpus, rng_seed=42, warmup=10, policy=policy)
    events = scheduler.plan(12)
    assert canonical_hash(corpus.to_dict()) == corpus_hash
    assert canonical_hash(scheduler.to_dict()) == state_hash
    assert canonical_hash(events) == events_hash


def test_multiref_template_only_legacy_requires_singleton():
    from r2f.modules.s1 import fuzz_catalog
    seeds = fuzz_catalog.load_seeds()["seeds"]
    original = deepcopy(seeds["P01"])
    seeds["P01"]["surface_refs"] = ["P01", "W02"]
    doc = profile()
    result = corpus_from_profile(doc, seeds=seeds, cycle_id=CYCLE)
    entry = result.selectable()[0]
    assert entry["seed_content_sha256"] == fuzz_catalog.seed_content_sha256(seeds["P01"])
    assert entry["seed_content_sha256"] != fuzz_catalog.seed_content_sha256(original)
    assert entry["final_config"] == original["logical_config"]
    assert len(entry["mutation_routes"]) == 2
    legacy = json.loads((ROOT / "config/s1_fuzz/profiles/parameter.json").read_text())
    with pytest.raises(ValueError, match="exactly one surface"):
        corpus_from_profile(legacy, seeds=seeds, cycle_id="late-endpoint-parameter")


def aliases_profile(*, variants=False, different_payload=False):
    from r2f.modules.s1 import fuzz_catalog
    doc = profile()
    target = "builtin.discovery_projection_reconciliation.fuzz_v1"
    seed_map = fuzz_catalog.load_seeds()["seeds"]
    payload = deepcopy(seed_map["P02"]["logical_config"])
    specs = []
    for index, sid in enumerate(("P02", "P03")):
        config = deepcopy(payload)
        if different_payload and index:
            config["topic"] += "_different_parent"
        specs.append({"seed_id": "P02" if variants else sid,
                      "variant_id": f"source-{index}", "target_id": target,
                      "boundary_class": "enum_exhaustive", "executable": True,
                      "overrides": {"logical_config": config},
                      "mutation_routes": [{"surface_id": sid, "boundary_class": "enum_exhaustive", "mutator_id": "M02"}]})
    doc.update(target_ids=[target], surfaces=["P02", "P03"], seed_specs=deepcopy(specs), sampling_kinds=["representative"])
    doc["cycles"][0].update(target_id=target, surfaces=["P02", "P03"], seed_specs=specs)
    return doc


@pytest.mark.parametrize("variants", [False, True])
def test_equal_payload_alias_origins_union_is_order_independent_and_checkpointed(tmp_path, variants):
    doc = aliases_profile(variants=variants)
    first = corpus_from_profile(doc, cycle_id=CYCLE)
    assert len(first.entries) == 1
    record = first.selectable()[0]
    assert len(record["mutation_routes"]) == len(record["mutation_route_origins"]) == 2
    assert {origin["baseline_content_sha256"] for origin in record["mutation_route_origins"]} == {record["content_sha256"]}
    validate_route_entry(record, profile=doc)
    reverse = deepcopy(doc)
    reverse["cycles"][0]["seed_specs"].reverse()
    second = corpus_from_profile(reverse, cycle_id=CYCLE)
    assert first.selectable() == second.selectable()
    assert corpus_identity(first) == corpus_identity(second)
    assert ExploratoryScheduler(first, policy="fair", rng_seed=3).plan(8) == ExploratoryScheduler(second, policy="fair", rng_seed=3).plan(8)
    result, source = plan(tmp_path / "aliases", doc, count=2, checkpoint=True)
    assert {row["surface_id"] for row in result["rows"]} == {"P02", "P03"}
    loaded = load_checkpoint(tmp_path / "aliases/checkpoint.json", profile=doc,
                             cycle_id=CYCLE, source_manifest=source, output_dir=tmp_path / "alias-resume")
    assert loaded["corpus"].selectable() == first.selectable()
    # Removing the alias origin while leaving its union route is not repairable.
    damaged = first.to_dict()
    damaged["entries"][0]["mutation_route_origins"].pop()
    with pytest.raises(ValueError, match="origin"):
        Corpus.from_dict(damaged)


@pytest.mark.parametrize("variants", [False, True])
def test_distinct_baseline_payloads_never_borrow_alias_routes(variants):
    doc = aliases_profile(variants=variants, different_payload=True)
    corpus = corpus_from_profile(doc, cycle_id=CYCLE)
    assert len(corpus.entries) == 2
    rows = corpus.selectable()
    assert all(len(entry["mutation_routes"]) == 1 for entry in rows)
    a, b = deepcopy(rows[0]), deepcopy(rows[1])
    # Pretend the other payload's declaration belongs to a; even a route that
    # is target/profile-valid cannot change its baseline origin binding.
    a["mutation_routes"] = normalize_routes(a["mutation_routes"] + b["mutation_routes"])
    a["mutation_route_origins"] += b["mutation_route_origins"]
    with pytest.raises(ValueError, match="different baseline"):
        Corpus().add_seed(a)
    # Rehashing the origin to this payload does not match the source spec.
    a["mutation_route_origins"][-1]["baseline_content_sha256"] = a["content_sha256"]
    with pytest.raises(ValueError, match="baseline differs"):
        validate_route_entry(a, profile=doc)


def test_each_layer_uses_its_declared_mutator_and_boundary(tmp_path):
    doc = profile()
    doc["sampling_kinds"] = ["boundary", "neighbor"]
    doc["chain_length"] = 2
    doc["cycles"][0]["seed_specs"][0]["mutation_routes"][0].update(
        boundary_class="zero", mutator_id="M01")
    result, _ = plan(tmp_path / "per-layer", doc)
    for row in result["rows"]:
        p01 = next(op for op in row["mutation_chain"] if op["surface_id"] == "P01")
        w02 = next(op for op in row["mutation_chain"] if op["surface_id"] == "W02")
        assert p01["mutator_id"] == "M01"
        assert p01["parameters"]["boundary_class"] == "zero"
        assert w02["mutator_id"] == "M03"
        assert row["final_input"]["reader_delay_sec"] == 0
    assert {row["surface_id"] for row in result["rows"]} == {"P01", "W02"}


def test_route_metadata_does_not_promote_execution_eligibility():
    entry = record()
    entry.update(executable=False, execution_admitted=False, candidate_eligible=False)
    corpus = Corpus()
    corpus.add_seed(entry)
    assert corpus.selectable() == []
    persisted = Corpus.from_dict(corpus.to_dict())
    retained = next(iter(persisted.entries.values()))
    assert retained["executable"] is False
    assert retained["execution_admitted"] is False
    assert retained["candidate_eligible"] is False


@pytest.mark.parametrize("damage", ["version", "origin_extra", "origin_missing", "origin_only", "origin_payload"])
def test_strict_origin_metadata_rejected(damage):
    entry = record()
    if damage == "version":
        entry["mutation_routes_schema"] = "unknown"
    elif damage == "origin_extra":
        entry["mutation_route_origins"][0]["extra"] = True
    elif damage == "origin_missing":
        entry["mutation_route_origins"][0].pop("variant_id")
    elif damage == "origin_only":
        entry.pop("mutation_routes")
        entry.pop("mutation_routes_schema")
    elif damage == "origin_payload":
        entry["mutation_route_origins"][0]["baseline_content_sha256"] = "f" * 64
    with pytest.raises(ValueError):
        Corpus().add_seed(entry)


def test_late_route_union_preserves_historical_contexts_and_resume():
    entry = record()
    partial = deepcopy(entry)
    partial["mutation_routes"] = partial["mutation_routes"][:1]
    partial.pop("mutation_route_origins")
    corpus = Corpus()
    corpus.add_seed(partial)
    scheduler = ExploratoryScheduler(corpus, rng_seed=4, warmup=0, policy="fair")
    scheduler.next()
    old = deepcopy(scheduler.selection_contexts)
    corpus.add_seed(entry)
    selected = scheduler.next()
    assert selected["mutation_route"]["surface_id"] == "W02"  # new route has zero uses
    assert scheduler.selection_contexts[:1] == old
    restored = ExploratoryScheduler.from_dict(scheduler.to_dict(), corpus, FeedbackLog())
    assert restored.next() == scheduler.next()


def test_route_sampling_incompatibility_rejects_instead_of_fallback(tmp_path):
    doc = profile()
    doc["sampling_kinds"] = ["boundary"]  # both explicit routes are M03, never M01
    result, _ = plan(tmp_path / "unsupported-kind", doc, count=2)
    assert result["budget"]["generated"] == result["budget"]["actual"] == 0
    assert all(row["generation_status"] == "rejected" for row in result["rows"])
    assert all(row["reason_code"] == "unsupported_capability:no_declared_sampling_kind_for_mutation_route"
               for row in result["rows"])
    assert all(event["sampling_error"] == "no_supported_sampling_kind"
               for event in result["selection_log"]["selections"])


def test_mixed_legacy_and_explicit_alias_merge_cannot_keep_undeclared_seed_label():
    doc = aliases_profile()
    aliases = []
    for spec in doc["cycles"][0]["seed_specs"]:
        one = deepcopy(doc)
        one["cycles"][0]["seed_specs"] = [spec]
        aliases.append(corpus_from_profile(one, cycle_id=CYCLE).selectable()[0])
    legacy, routed = aliases
    for key in ("mutation_routes", "mutation_routes_schema", "mutation_route_origins"):
        legacy.pop(key)
    canonical = []
    for order in ((legacy, routed), (routed, legacy)):
        corpus = Corpus()
        for entry in order:
            corpus.add_seed(entry)
        canonical.append(corpus.selectable())
        assert Corpus.from_dict(corpus.to_dict()).to_dict() == corpus.to_dict()
    assert canonical[0] == canonical[1]
    assert canonical[0][0]["seed_id"] == "P03"
    assert canonical[0][0]["mutation_routes"] == routed["mutation_routes"]


def test_planned_pending_checkpoint_keeps_route_view_and_rejects_forgery(tmp_path):
    doc = profile()
    first, source = plan(tmp_path / "pending-first", doc, count=2, checkpoint=True)
    snapshots = sorted((tmp_path / "pending-first/checkpoints").glob("*-planned.json"))
    original = snapshots[-1].read_bytes()
    restored = load_checkpoint(snapshots[-1], profile=doc, cycle_id=CYCLE,
                               source_manifest=source, output_dir=tmp_path / "pending-resume")
    pending = restored["continuation"]["pending_selection"]
    assert pending["entry"]["mutation_route"] == pending["mutation_route"]
    resumed, _ = plan(tmp_path / "pending-resume", doc, state=restored, count=2, checkpoint=True)
    assert len(resumed["rows"]) == 1  # regenerate pending, not a second selection
    assert resumed["selection_log"] == first["selection_log"]
    keys = ("mutation_route", "parent_content_sha256", "content_sha256", "mutation_plan", "final_input", "seed")
    assert {key: resumed["rows"][0][key] for key in keys} == {key: first["rows"][-1][key] for key in keys}
    damaged = json.loads(original)
    selection = damaged["state"]["continuation"]["pending_selection"]
    other = next(route for route in selection["entry"]["mutation_routes"] if route != selection["mutation_route"])
    selection["entry"] = route_view(selection["entry"], other)
    damaged["checkpoint_sha256"] = canonical_hash({key: value for key, value in damaged.items() if key != "checkpoint_sha256"})
    forgery = tmp_path / "forged-evidence" / "pending-forged.json"
    forgery.parent.mkdir()
    forgery.write_text(json.dumps(damaged))
    with pytest.raises(ValueError, match="route/view"):
        load_checkpoint(forgery, profile=doc, cycle_id=CYCLE, source_manifest=source,
                        output_dir=tmp_path / "pending-invalid")
    assert snapshots[-1].read_bytes() == original
