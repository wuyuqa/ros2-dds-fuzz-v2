"""Semantic corpus SHA is not the lineage-preserving effective replay SHA.

Pure schema/fixture tests only: never start ROS, DDS, netns or a campaign.
"""
from copy import deepcopy
import random

import pytest

from r2f.campaign.corpus import (
    CONTENT_IDENTITY_SCHEMA, SCHEMA, Corpus, content_identity, content_sha256,
)
from r2f.campaign.mutation_routes import ROUTES_SCHEMA
from r2f.core.evidence import canonical_hash, effective_input_hash
from r2f.core.mutation_plan import (
    RUNTIME_WINDOW_SCHEMA, compose,
)
from r2f.core.mutators import mutate
from test_fuzz_identity import legacy_input
from test_fuzz_model_contracts import PARENT, fixture_contract


def planned(parent_value=1, rng_seed=3, *, order=("P04",)):
    contract = fixture_contract()
    parent = deepcopy(PARENT)
    parent["qos"]["deadline_ns"] = parent_value
    proposals = []
    for sid in order:
        proposal = mutate(parent, contract.surface_by_id[sid], random.Random(0),
                          {"boundary_class": "max"}, "M01")
        proposals.append(proposal)
    plan = compose(parent, proposals, contract, rng_seed=rng_seed, allow_override=True)
    return {"target_id": "fixture", "target_revision": 0,
            "rmw": "rmw_cyclonedds_cpp", "contract_hash": contract.contract_hash(),
            "final_config": plan.final_config, "mutation_plan": plan.to_dict(),
            "operations": [op.to_dict() for op in plan.operations],
            "runtime_windows": plan.runtime_windows}


def baseline():
    return {key: value for key, value in planned().items()
            if key not in {"operations", "mutation_plan", "runtime_windows"}}


def attach_provider_layer(entry):
    plan = entry["mutation_plan"]
    entry["input_layer_receipts"] = [{
        "layer": "ros2_endpoint", "declared_mutation": "late_joiner",
        "plan_hash": canonical_hash(plan), "content_sha256": canonical_hash(plan),
        "rng_algorithm": plan["rng_algorithm"], "rng_seed": plan["rng_seed"],
        "operation_status": {op["op_id"]: op["applicability_result"]["status"]
                             for op in plan["operations"]},
        "runtime_windows": plan["runtime_windows"],
    }]
    return entry


def test_real_compose_same_final_input_different_parent_rng_and_provider_receipts():
    a, b = attach_provider_layer(planned(1, 3)), attach_provider_layer(planned(2, 99))
    assert a["final_config"] == b["final_config"]
    assert a["mutation_plan"]["parent_content_sha256"] != b["mutation_plan"]["parent_content_sha256"]
    assert a["input_layer_receipts"] != b["input_layer_receipts"]
    saved = deepcopy((a, b))
    assert content_identity(a) == content_identity(b)
    assert (a, b) == saved


@pytest.mark.parametrize("field,value", [
    ("parent_content_sha256", "a" * 64), ("seed_id", "other-seed"),
    ("profile_id", "renamed-scheduler-profile"), ("rng_seed", 99),
    ("repeat", 7), ("seed", 8), ("raw_path", "/other/raw"),
    ("timestamp", "later"), ("endpoint_guid", "actual-guid"),
    ("runtime_route", {"backend": "other-provenance"}),
])
def test_nonsemantic_envelope_labels_do_not_split_content(field, value):
    a = planned()
    assert content_identity(a) == content_identity(dict(a, **{field: value}))


@pytest.mark.parametrize("field,value", [
    ("target_id", "other"), ("target_revision", 1),
    ("rmw", "rmw_fastrtps_cpp"), ("contract_hash", "other-contract"),
])
def test_target_contract_and_rmw_are_semantic(field, value):
    a = baseline()
    assert content_identity(a) != content_identity(dict(a, **{field: value}))


@pytest.mark.parametrize("key", ["duration_sec", "time", "history", "seed", "rng_seed", "path", "profile"])
def test_payload_parameter_names_are_never_recursively_blacklisted(key):
    a = baseline()
    a["final_config"]["rmw_configuration"] = {key: "actual-input-a"}
    b = deepcopy(a)
    b["final_config"]["rmw_configuration"][key] = "actual-input-b"
    assert content_identity(a) != content_identity(b)


def test_config_and_environment_are_retained_even_if_profile_name_changes():
    a = baseline()
    a["profile_id"] = "profile-a"
    b = dict(deepcopy(a), profile_id="profile-b")
    assert content_identity(a) == content_identity(b)
    b["final_config"]["environment"] = {"transport": "tcp", "loss_percent": 3}
    assert content_identity(a) != content_identity(b)


@pytest.mark.parametrize("kind,parameters_a,parameters_b", [
    ("M01", {"boundary_class": "max"}, {"boundary_class": "finite", "wide_sampling": True}),
    ("M02", {"enum_size": 2}, {"enum_size": 5, "sampling_kind": "representative"}),
    ("M03", {"sampling": "local_bounded", "step": 1},
     {"sampling_kind": "neighbor", "parent": 9, "direction": -1, "step": 2}),
])
def test_compile_time_selection_metadata_does_not_split_final_content(kind, parameters_a, parameters_b):
    a = planned()
    a.pop("mutation_plan")
    a["operations"][0].update(mutator_id=kind, parameters=parameters_a)
    b = deepcopy(a)
    b["operations"][0]["parameters"] = parameters_b
    assert content_identity(a) == content_identity(b)
    b["operations"][0]["parameters"]["runtime_extra"] = {"step": 3}
    assert content_identity(a) != content_identity(b)


def test_actual_valid_operation_order_phase_and_override_references():
    a = planned(order=("P04", "X01", "X02"))
    b = planned(order=("P04", "X02", "X01"))
    assert a["final_config"] == b["final_config"]
    assert content_identity(a) != content_identity(b)
    c = deepcopy(a)
    c.pop("mutation_plan")
    c["operations"][0]["application_phase"] = "pre_create"
    assert content_identity(a) != content_identity(c)
    # Generated names/references change together; position and override remain.
    renamed = deepcopy(a)
    names = {op["op_id"]: "alias-" + str(i) for i, op in enumerate(a["operations"])}
    for op in renamed["mutation_plan"]["operations"]:
        op["op_id"] = names[op["op_id"]]
        if "overridden_by" in op["applicability_result"]:
            op["applicability_result"]["overridden_by"] = names[op["applicability_result"]["overridden_by"]]
    renamed["operations"] = deepcopy(renamed["mutation_plan"]["operations"])
    assert content_identity(a) == content_identity(renamed)


@pytest.mark.parametrize("key,value", [("trigger_offset_sec", .2), ("drop_duration_sec", .4),
                                      ("restore_interval_sec", .7), ("trigger_anchor", "pre_match")])
def test_runtime_window_inputs_not_generation_metadata(key, value):
    a = baseline()
    a["runtime_windows"] = [{"schema": RUNTIME_WINDOW_SCHEMA, "window_id": "w1",
                              "window_index": 0, "trigger_anchor": "fresh_match",
                              "trigger_offset_sec": 0, "drop_duration_sec": .1,
                              "restore_interval_sec": .2}]
    b = deepcopy(a)
    b["runtime_windows"][0][key] = value
    assert content_identity(a) != content_identity(b)


def test_runtime_operation_parameters_with_selection_like_names_are_not_erased():
    a = planned()
    a.pop("mutation_plan")
    a["operations"][0].update(mutator_id="M06", application_phase="runtime_window",
                               parameters={"anchor": "fresh_match", "offset_sec": .2})
    b = deepcopy(a)
    b["operations"][0]["parameters"]["offset_sec"] = .4
    assert content_identity(a) != content_identity(b)


def test_real_topology_generator_repeated_final_program_excludes_nested_v2_lineage():
    from r2f.modules.s1 import topology_model
    from r2f.modules.s1.topology_input import make_topology_plan, topology_contract, topology_input_layer_receipts
    seen = {}
    for seed in range(24):
        plan = make_topology_plan(topology_model.generate_program(0, writers=1, readers=1),
                                  [{"operator_id": "endpoint.count", "slot_id": "S1.topology.endpoints"}],
                                  rng_seed=seed)
        entry = {"target_id": "discovery.endpoint_topology.fuzz_v1", "target_revision": 0,
                 "rmw": "rmw_cyclonedds_cpp", "contract_hash": topology_contract().contract_hash(),
                 "final_config": plan.final_config, "mutation_plan": plan.to_dict(),
                 "input_layer_receipts": topology_input_layer_receipts(plan)}
        final = canonical_hash(plan.final_config)
        if final in seen:
            other = seen[final]
            assert entry["mutation_plan"] != other["mutation_plan"]
            assert content_identity(entry) == content_identity(other)
            return
        seen[final] = entry
    pytest.fail("fixture did not generate a repeated final program")


def test_content_v2_is_explicit_and_old_baseline_or_plan_digest_is_not_accepted(tmp_path):
    a = baseline()
    assert content_identity(a) == canonical_hash({"schema": CONTENT_IDENTITY_SCHEMA, "identity": a})
    legacy = canonical_hash({"schema": SCHEMA, "identity": a})
    assert legacy != content_identity(a)
    with pytest.raises(ValueError, match="content hash mismatch"):
        content_sha256(dict(a, content_sha256=legacy))
    with pytest.raises(ValueError, match="content hash mismatch"):
        Corpus().add_seed(dict(a, content_sha256=legacy))
    corpus = Corpus()
    corpus.add_seed(a)
    current = corpus.to_dict()
    assert Corpus.from_dict(current).to_dict() == current
    damaged = deepcopy(current)
    damaged["entries"][0]["content_sha256"] = legacy
    with pytest.raises(ValueError, match="content hash mismatch"):
        Corpus.from_dict(damaged)


def test_route_union_is_provenance_not_an_extra_payload_and_reload_is_strict():
    from test_seed_mutation_routes import record
    a = record()
    a.pop("mutation_route_origins")
    a.pop("content_sha256")
    a.update(seed_id="alias-a", mutation_routes_schema=ROUTES_SCHEMA,
             mutation_routes=[{"surface_id": "P01", "boundary_class": "finite", "mutator_id": "M03"}])
    b = deepcopy(a)
    b.update(seed_id="alias-b", mutation_routes=[{"surface_id": "W02", "boundary_class": "finite", "mutator_id": "M03"}])
    assert content_identity(a) == content_identity(b)
    corpus = Corpus()
    corpus.add_seed(a)
    corpus.add_seed(b)
    assert len(corpus.entries) == 1
    row = next(iter(corpus.entries.values()))
    assert len(row["mutation_routes"]) == len(row["mutation_route_origins"]) == 2
    assert Corpus.from_dict(corpus.to_dict()).to_dict() == corpus.to_dict()
    damaged = corpus.to_dict()
    damaged["entries"][0]["mutation_route_origins"][0]["baseline_content_sha256"] = "f" * 64
    with pytest.raises(ValueError):
        Corpus.from_dict(damaged)


def test_full_effective_replay_input_sha_retains_seed_and_receipts():
    a = legacy_input()
    b = deepcopy(a)
    b["seed"] += 1
    b["input_layer_receipts"][0]["submitted_reader_delay_sec"] = .16
    assert effective_input_hash(a) != effective_input_hash(b)


@pytest.mark.parametrize("damage", ["operations", "windows", "final", "unknown_layer", "provider_status"])
def test_mismatched_semantic_envelopes_fail_closed(damage):
    a = attach_provider_layer(planned())
    if damage == "operations":
        a["operations"][0]["parameters"]["injected"] = True
    elif damage == "windows":
        a["runtime_windows"] = [{}]
    elif damage == "final":
        a["final_config"]["reader_delay_sec"] = 99
    elif damage == "unknown_layer":
        a["input_layer_receipts"] = [{"schema": "unknown/v1", "new_execution_parameter": 1}]
    else:
        a["input_layer_receipts"][0]["operation_status"] = {"unknown-op": "applicable"}
    with pytest.raises(ValueError):
        content_identity(a)


def test_two_valid_m01_boundary_labels_for_same_declared_value_dedupe():
    contract = fixture_contract()
    surface = contract.surface_by_id["P04"]
    surface.domain = {"min": 7, "max": 7, "step": 1}
    entries = []
    for label in ("min", "max"):
        proposal = mutate(PARENT, surface, random.Random(0), {"boundary_class": label}, "M01")
        plan = compose(PARENT, [proposal], contract)
        entries.append({"target_id": "fixture", "target_revision": 0,
                        "rmw": "rmw_cyclonedds_cpp", "contract_hash": contract.contract_hash(),
                        "final_config": plan.final_config, "mutation_plan": plan.to_dict()})
    assert entries[0]["final_config"] == entries[1]["final_config"]
    assert content_identity(entries[0]) == content_identity(entries[1])


def test_topology_explicit_generator_seed_context_is_not_execution_content():
    from r2f.modules.s1 import topology_model
    from r2f.modules.s1.topology_input import make_topology_plan, topology_contract
    request = {"operator_id": "endpoint.count", "slot_id": "S1.topology.endpoints"}
    entries = []
    for context in ({}, {"seed": 7, "parameters": {"seed": 7}}):
        plan = make_topology_plan(topology_model.generate_program(0, writers=1, readers=1),
                                  [dict(request, context=context)], rng_seed=7)
        entries.append({"target_id": "discovery.endpoint_topology.fuzz_v1", "target_revision": 0,
                        "rmw": "rmw_cyclonedds_cpp", "contract_hash": topology_contract().contract_hash(),
                        "final_config": plan.final_config, "mutation_plan": plan.to_dict()})
    assert entries[0]["final_config"] == entries[1]["final_config"]
    assert content_identity(entries[0]) == content_identity(entries[1])


@pytest.mark.parametrize("field", ["operations", "runtime_windows"])
def test_null_instruction_envelopes_cannot_hide_as_absent(field):
    with pytest.raises(ValueError, match="must be a list"):
        content_identity(dict(baseline(), **{field: None}))


def test_full_v2_effective_replay_sha_still_binds_legal_different_lineage():
    from r2f.core.evidence import INPUT_SCHEMA_V2
    entries = [attach_provider_layer(planned(1, 3)), attach_provider_layer(planned(2, 99))]
    effective = []
    for entry in entries:
        effective.append(dict(legacy_input(), input_identity_schema=INPUT_SCHEMA_V2,
                              mutation_plan=entry["mutation_plan"], operations=entry["operations"],
                              runtime_windows=entry["runtime_windows"], contract_hash=entry["contract_hash"],
                              final_input=entry["final_config"], input_layer_receipts=entry["input_layer_receipts"]))
    assert content_identity(entries[0]) == content_identity(entries[1])
    assert effective_input_hash(effective[0]) != effective_input_hash(effective[1])


def test_checkpoint_rejects_old_content_hash_without_touching_old_artifact(tmp_path):
    import json
    from r2f.campaign.checkpoint import load_checkpoint
    from test_seed_mutation_routes import CYCLE, plan, profile
    doc = profile()
    _, source = plan(tmp_path / "new-input", doc, count=1, checkpoint=True)
    path = tmp_path / "new-input/checkpoint.json"
    old_bytes = path.read_bytes()
    data = json.loads(old_bytes)
    record = data["state"]["corpus"]["entries"][0]
    # This is an intentional old-schema content identity, not a damaged outer
    # checksum: the loader must reach and reject the semantic content mismatch.
    old_identity = {key: deepcopy(record[key]) for key in
                    ("target_id", "target_revision", "rmw", "contract_hash", "profile_id", "final_config")}
    record["content_sha256"] = canonical_hash({"schema": SCHEMA, "identity": old_identity})
    data["checkpoint_sha256"] = canonical_hash({key: value for key, value in data.items()
                                               if key != "checkpoint_sha256"})
    old_checkpoint = tmp_path / "old-copy/checkpoint.json"
    old_checkpoint.parent.mkdir()
    old_checkpoint.write_text(json.dumps(data))
    saved_old_copy = old_checkpoint.read_bytes()
    with pytest.raises(ValueError, match="content hash mismatch"):
        load_checkpoint(old_checkpoint, profile=doc, cycle_id=CYCLE, source_manifest=source,
                        output_dir=tmp_path / "rejected-resume")
    assert path.read_bytes() == old_bytes
    assert old_checkpoint.read_bytes() == saved_old_copy


def external_parent_fixture(*, parent_metadata=None):
    parent = baseline()
    parent["final_config"]["reader_delay_sec"] = .7
    parent.update(parent_metadata or {})
    digest = content_identity(parent)
    child = baseline()
    child["final_config"]["reader_delay_sec"] = .8
    child["parent_content_sha256"] = digest
    corpus = Corpus()
    corpus.add_seed(child)
    return corpus.to_dict(), {digest: parent}


def test_explicit_validated_model_parents_resolve_orphan_without_novelty_or_history(tmp_path):
    import json
    data, parents = external_parent_fixture()
    before = deepcopy((data, parents))
    with pytest.raises(ValueError, match="parent is missing"):
        Corpus.from_dict(data)
    restored = Corpus.from_dict(data, model_parents=parents)
    assert restored.to_dict() == data
    assert len(restored.entries) == 1
    assert set(restored.entries).isdisjoint(parents)
    assert (data, parents) == before
    path = tmp_path / "orphan.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="parent is missing"):
        Corpus.load(path)


@pytest.mark.parametrize("field,value", [("target_id", "other-target"), ("target_revision", 2),
                                        ("rmw", "rmw_fastrtps_cpp"), ("contract_hash", "other")])
def test_external_parent_edge_cannot_cross_target_revision_rmw_or_contract(field, value):
    data, parents = external_parent_fixture(parent_metadata={field: value})
    with pytest.raises(ValueError, match="target/revision/RMW/contract mismatch"):
        Corpus.from_dict(data, model_parents=parents)


@pytest.mark.parametrize("damage", ["key", "record", "embedded_digest", "orphan", "self_cycle", "list"])
def test_external_model_parent_validation_fails_closed(damage):
    data, parents = external_parent_fixture()
    digest, parent = next(iter(parents.items()))
    if damage == "key":
        parents = {"f" * 64: parent}
    elif damage == "record":
        parent["final_config"]["reader_delay_sec"] = .71
    elif damage == "embedded_digest":
        parent["content_sha256"] = "f" * 64
    elif damage == "orphan":
        parent["parent_content_sha256"] = "f" * 64
    elif damage == "self_cycle":
        parent["parent_content_sha256"] = digest
    else:
        parents = list(parents.values())
    with pytest.raises(ValueError):
        Corpus.from_dict(data, model_parents=parents)


def test_external_ancestry_cycle_and_cross_binding_in_ancestor_are_rejected():
    data, parents = external_parent_fixture()
    digest, parent = next(iter(parents.items()))
    ancestor = deepcopy(parent)
    ancestor["final_config"]["reader_delay_sec"] = .6
    ancestor_digest = content_identity(ancestor)
    parent["parent_content_sha256"] = ancestor_digest
    ancestor["parent_content_sha256"] = digest
    with pytest.raises(ValueError, match="cyclic"):
        Corpus.from_dict(data, model_parents={**parents, ancestor_digest: ancestor})
    ancestor.pop("parent_content_sha256")
    ancestor["rmw"] = "rmw_fastrtps_cpp"
    ancestor_digest = content_identity(ancestor)
    parent["parent_content_sha256"] = ancestor_digest
    with pytest.raises(ValueError, match="target/revision/RMW/contract mismatch"):
        Corpus.from_dict(data, model_parents={**parents, ancestor_digest: ancestor})
