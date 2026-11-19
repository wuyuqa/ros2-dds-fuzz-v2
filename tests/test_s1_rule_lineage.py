"""Offline metadata consumer tests; no DDS, normative downloads, or result fixtures."""
import json
from unittest.mock import Mock

import pytest

from r2f.core import Registry, canonical_hash
from r2f.modules.s1 import catalog, fuzz_catalog
from r2f.modules.s1.module import S1Module, S1ExploratoryModule, register, register_exploratory
from r2f.modules.s1.targets import EXECUTION_ORDER, F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET

FROZEN_REQUIRED_MODULE = "947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6"
FROZEN_TOPOLOGY = "0c4815b9b1b9b70ad639089ac3cdbf6a6ae9c4ba3997bcb4eba4ec8b09ab8234"


def _registry(capability=None):
    r = Registry()
    register_exploratory(r, include_required=True, runtime_capability=capability)
    return r


@pytest.mark.parametrize("target_index", range(4))
def test_required_runtime_ids_resolve_through_actual_owner(target_index, monkeypatch):
    r = _registry()
    target_id = EXECUTION_ORDER[target_index]
    rule_id = r.get_target(target_id).rule_contract["primary_rule"]
    module = r.get_module("s1")
    spy = Mock(wraps=module.resolve_rule_lineage)
    monkeypatch.setattr(module, "resolve_rule_lineage", spy)
    record = r.get_rule_lineage(target_id, rule_id)
    spy.assert_called_once_with(target_id, rule_id)
    assert record["target_id"] == target_id and record["rule_id"] == rule_id
    assert record["verdict_rule_ids"] == [rule_id]
    assert record["fact_fields"] and record["source_refs"]
    assert record["execution_evidence"] is False
    assert record["maturity_evidence"] is False
    assert json.loads(json.dumps(record)) == record


@pytest.mark.parametrize("rule_id,target_id,index", [
    ("s1.late_join.delivery.v1", "discovery.late_endpoint.fuzz_v1", 0),
    ("s1.late_join.delivery.v1", "discovery.late_join_delivery.fuzz_v1", 1),
    ("s1.projection.qos_field.v1", "builtin.discovery_projection_reconciliation.fuzz_v1", 3),
    ("s1.discovery.drop_restore.v1", "discovery.interruption_recovery.fuzz_v1", 2),
    ("s1.discovery.compatibility_guard.v1", "discovery.match_compatibility.fuzz_v1", None),
    ("s1.workload.bounded_send.v1", "discovery.late_endpoint.fuzz_v1", 0),
])
def test_five_catalog_rules_map_to_contextual_returned_id_and_facts(rule_id, target_id, index):
    r = _registry()
    data = r.get_rule_lineage(target_id, rule_id)
    expected = rule_id if index is None else r.get_target(EXECUTION_ORDER[index]).rule_contract["primary_rule"]
    assert data["verdict_rule_ids"] == [expected]
    spec = fuzz_catalog.RULES[rule_id]
    assert data["required_fact_ids"] == list(spec.required_fact_ids)
    assert data["rule_role"] == spec.primary_or_supporting
    assert data["scope"]["target_id"] == target_id
    assert data["independent_verdict"] is (index is None)
    if rule_id == "s1.workload.bounded_send.v1":
        assert data["verdict_semantics"] == "shared_evaluator_not_independent"


@pytest.mark.parametrize("capability,target_id", [
    (TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET), (F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET),
])
def test_opt_in_runtime_ids_resolve_only_after_registration(capability, target_id):
    r = _registry()
    with pytest.raises(KeyError):
        r.get_rule_lineage(target_id, "anything")
    r = _registry(capability)
    t = r.get_target(target_id)
    data = r.get_rule_lineage(target_id, t.rule_contract["primary_rule"])
    assert data["scope"]["runtime_capability"] == capability
    assert data["required_fact_ids"]


def test_wrong_target_unknown_rule_and_unregistered_target_fail_closed():
    r = _registry(TOPOLOGY_CAPABILITY)
    for target_id, rule_id in [
        ("unknown", "s1.late_join.delivery.v1"),
        (EXECUTION_ORDER[0], "s1.workload.bounded_send.v1"),
        (EXECUTION_ORDER[0], "builtin.discovery.builtin_matches_projection"),
        (TOPOLOGY_TARGET, "s1.discovery.compatibility_guard.v1"),
        (EXECUTION_ORDER[0], "no.mapping"),
    ]:
        with pytest.raises(KeyError):
            r.get_rule_lineage(target_id, rule_id)
    with pytest.raises(KeyError):
        r.get_module("s1").resolve_rule_lineage(TOPOLOGY_TARGET, "s1.topology.program_consistency.v1")


def test_result_is_detached_and_registry_does_not_search_other_modules(monkeypatch):
    r = _registry()
    target_id = EXECUTION_ORDER[0]
    rule_id = r.get_target(target_id).rule_contract["primary_rule"]
    original = r.get_rule_lineage(target_id, rule_id)
    changed = r.get_rule_lineage(target_id, rule_id)
    changed["source_refs"][0]["source"]["artifact_sha256"] = "bad"
    changed["fact_fields"].clear()
    assert r.get_rule_lineage(target_id, rule_id) == original
    monkeypatch.setattr(r.get_module("s1"), "resolve_rule_lineage", None)
    other = Mock(side_effect=AssertionError("must not fallback to other module"))
    monkeypatch.setattr(r.get_module("s1.exploratory"), "resolve_rule_lineage", other)
    with pytest.raises(ValueError, match="lineage"):
        r.get_rule_lineage(target_id, rule_id)
    other.assert_not_called()


@pytest.mark.parametrize("bad", [None, {}, {"target_id": "wrong", "rule_id": "x"}])
def test_registry_rejects_missing_or_wrong_scope_hook_result(bad):
    r = Registry()
    module = S1Module()
    module.resolve_rule_lineage = lambda *_: bad
    r.register(module)
    with pytest.raises(ValueError, match="lineage"):
        r.get_rule_lineage(EXECUTION_ORDER[0], "x")


def test_source_classification_hash_validation_and_unknown_status(monkeypatch):
    from r2f.modules.s1 import rule_lineage
    r = _registry(TOPOLOGY_CAPABILITY)
    data = r.get_rule_lineage(TOPOLOGY_TARGET, "s1.topology.program_consistency.v1")
    refs = data["source_refs"]
    assert {ref["source"]["kind"] for ref in refs} >= {"normative", "engineering", "issue_heuristic"}
    assert any(ref["source"]["verification_status"] == "unverified" for ref in refs)
    for ref in refs:
        source = ref["source"]
        assert source["source_id"] and source["version"] and ref["locator"]
        assert not source["locator"].startswith("/home/")
        if source["verification_status"] == "unverified":
            assert source["artifact_sha256"] is None
        else:
            assert len(source["artifact_sha256"]) == 64
        if source["kind"] == "issue_heuristic":
            assert ref["support"] == "heuristic_only"
    key = next(k for k, v in rule_lineage.SOURCES.items() if v["kind"] == "normative")
    with monkeypatch.context() as patch:
        patch.setitem(rule_lineage.SOURCES, key, {**rule_lineage.SOURCES[key], "artifact_sha256": "invalid"})
        with pytest.raises(ValueError, match="SHA256|sha256"):
            r.get_rule_lineage(TOPOLOGY_TARGET, "s1.topology.program_consistency.v1")
    with monkeypatch.context() as patch:
        patch.delitem(rule_lineage.SOURCES, key)
        with pytest.raises(KeyError):
            r.get_rule_lineage(TOPOLOGY_TARGET, "s1.topology.program_consistency.v1")


def test_existing_catalog_membership_is_consumed(monkeypatch):
    spy = Mock(wraps=catalog.resolve_target)
    monkeypatch.setattr(catalog, "resolve_target", spy)
    catalog.resolve_rule_lineage(EXECUTION_ORDER[0], "discovery.late_endpoint.match_and_receive_after_join")
    assert spy.call_args.args[0] == EXECUTION_ORDER[0]


def test_manifest_and_input_contracts_unchanged():
    required, fuzz = S1Module(), S1ExploratoryModule(runtime_capability=TOPOLOGY_CAPABILITY)
    assert canonical_hash(required.to_dict()) == FROZEN_REQUIRED_MODULE
    topology = fuzz.targets[TOPOLOGY_TARGET]
    assert canonical_hash(topology.to_dict()) == FROZEN_TOPOLOGY
    before = topology.to_dict()
    r = _registry(TOPOLOGY_CAPABILITY)
    r.get_rule_lineage(TOPOLOGY_TARGET, topology.rule_contract["primary_rule"])
    assert topology.to_dict() == before
    assert len(required.targets) * 2 * 3 * 3 == 72


def test_missing_alias_or_runtime_mapping_fails_closed(monkeypatch):
    from r2f.modules.s1 import rule_lineage
    r = _registry()
    with monkeypatch.context() as patch:
        patch.delitem(rule_lineage._CATALOG_VERDICTS, "s1.late_join.delivery.v1")
        with pytest.raises(KeyError):
            r.get_rule_lineage("discovery.late_endpoint.fuzz_v1", "s1.late_join.delivery.v1")
    with monkeypatch.context() as patch:
        patch.delitem(rule_lineage._RUNTIME, "discovery.late_endpoint.match_and_receive_after_join")
        with pytest.raises(KeyError):
            r.get_rule_lineage(EXECUTION_ORDER[0], "discovery.late_endpoint.match_and_receive_after_join")
    with monkeypatch.context() as patch:
        patch.setitem(rule_lineage._CATALOG_VERDICTS, "s1.late_join.delivery.v1", ())
        with pytest.raises(KeyError):
            r.get_rule_lineage("discovery.late_endpoint.fuzz_v1", "s1.late_join.delivery.v1")


def test_no_results_directory_required_and_fact_refs_exist(tmp_path, monkeypatch):
    r = _registry(TOPOLOGY_CAPABILITY)
    monkeypatch.chdir(tmp_path)
    assert not (tmp_path / "results").exists()
    for target_id, target in r.targets.items():
        ids = [target.rule_contract.get("primary_rule") or target.rule_contract.get("compatibility_rule")]
        if target.exploration_contract:
            ids += [spec.id for spec in target.exploration_contract.rules]
        for rule_id in set(ids):
            data = r.get_rule_lineage(target_id, rule_id)
            assert data["fact_fields"]
            if target.exploration_contract:
                declared = {f.id for f in target.exploration_contract.facts}
                assert set(data["required_fact_ids"]) <= declared
            if target_id == TOPOLOGY_TARGET:
                assert data["fact_schema"] == "r2f.s1-topology-facts/v1"
    assert len(fuzz_catalog._RULES) == 5


def test_original_capture_hashes_and_unverified_issues_are_not_upgraded():
    r = _registry(TOPOLOGY_CAPABILITY)
    data = r.get_rule_lineage(TOPOLOGY_TARGET, "s1.topology.program_consistency.v1")
    sources = {ref["source"]["source_id"]: ref["source"] for ref in data["source_refs"]}
    assert sources["dds-1.4"]["artifact_sha256"] == "16d6f8385c2ba79f7346dc18c867b624bc6dcc8fcf7c2ec52c55b7ae3dc113f2"
    assert sources["rtps-2.5"]["artifact_sha256"] == "c362eaa590c9c95fc6223359ce1ebfa57776cf7ea1e47824f58d48dc16907088"
    assert sources["cyclone-1097"]["verification_status"] == "unverified"
    assert sources["cyclone-1097"]["artifact_sha256"] is None
    assert sources["cyclone-2292"]["artifact_locator"] == "https://api.github.com/repos/eclipse-cyclonedds/cyclonedds/pulls/2292"
    assert sources["cyclone-2292"]["kind"] == "issue_heuristic"
    assert sources["cyclone-2292"]["verification_status"] == "verified_local_artifact"
