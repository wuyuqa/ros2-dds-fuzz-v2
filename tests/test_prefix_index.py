"""Offline prefix-index contracts: three real prefix kinds, receipts, guards.

Fixtures are explicitly fake raw evidence with fixture=true; they are excluded
from live selection.  No ROS process, discovery event or candidate is created.
"""
from copy import deepcopy

import pytest

from r2f.campaign.prefix_index import (PREFIX_KINDS, PrefixError, PrefixHistoryError,
                                       PrefixIndex, PrefixRecord, StateFingerprint,
                                       prefix_selection_provenance)
from r2f.core.evidence import canonical_hash


def fingerprint(**overrides):
    components = {"live_roles": "both", "observed_match_class": "bidirectional",
                  "network_phase": "normal", "endpoint_generations": "g1"}
    components.update(overrides)
    return StateFingerprint(components)


def receipt(role, event_index=1, *, fixture=True):
    return {"role": role, "raw_path": f"/fake/{role}.jsonl",
            "raw_sha256": "a" * 64, "event_index": event_index, "fixture": fixture}


def record(prefix_id, kind, *, receipts=None, coverage=None, cost=0, attempts=0, hits=0,
           depends_on=None, resources=None, fixture=True, state=None):
    roles = {
        "first_creation": ["writer_creation", "reader_creation"],
        "after_network_recovery": ["network_restore", "fresh_writer_count",
                                   "fresh_reader_count", "fresh_graph", "postrestore_delivery"],
        "after_delete_recreate": ["endpoint_delete", "endpoint_recreate"],
    }[kind]
    receipts = receipts if receipts is not None else [receipt(role) for role in roles]
    return PrefixRecord(
        prefix_id=prefix_id, kind=kind, target_id="discovery.late_endpoint",
        target_revision=0, rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
        state=state or fingerprint(), receipts=receipts,
        provenance={"source_identity": {"head": "fixture-head"},
                    "run_id": "fixture-run", "raw_evidence": "explicit_fixture"},
        cost=cost, depends_on=list(depends_on or []), resources=list(resources or []),
        coverage=list(coverage or []), attempts=attempts, hits=hits, fixture=fixture)


def test_three_real_prefix_kinds_require_their_own_receipt_roles():
    assert PREFIX_KINDS == {"first_creation", "after_network_recovery", "after_delete_recreate"}
    for kind in PREFIX_KINDS:
        assert record(kind, kind).kind == kind
    with pytest.raises(PrefixError):
        record("bad-recovery", "after_network_recovery",
               receipts=[receipt("network_restore"), receipt("fresh_writer_count")])
    with pytest.raises(PrefixError):
        record("bad-delete", "after_delete_recreate", receipts=[receipt("endpoint_delete")])


def test_prefix_requires_raw_receipts_and_provenance():
    with pytest.raises(PrefixError):
        record("empty", "first_creation", receipts=[])
    with pytest.raises(PrefixError):
        PrefixRecord(prefix_id="no-provenance", kind="first_creation",
                     target_id="discovery.late_endpoint", target_revision=0,
                     rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                     state=fingerprint(), receipts=[receipt("writer_creation")],
                     provenance={})
    bad = receipt("writer_creation")
    bad["raw_sha256"] = "not-a-hash"
    with pytest.raises(PrefixError):
        record("bad-raw", "first_creation", receipts=[bad, receipt("reader_creation")])


def test_state_fingerprint_rejects_count_only_identity():
    with pytest.raises(PrefixError):
        StateFingerprint({"count": 4})
    with pytest.raises(PrefixError):
        StateFingerprint({"counts": 4})
    with pytest.raises(PrefixError):
        StateFingerprint({"live_roles": "both", "count": 4, "counts": {"x": 1}})
    fingerprint_with_count = StateFingerprint({"live_roles": "both", "count": 4})
    assert fingerprint_with_count.fingerprint_id()


def test_selection_uses_coverage_hitrate_cost_and_records_provenance():
    index = PrefixIndex()
    index.add(record("low", "first_creation", coverage=["a"], cost=0, attempts=4, hits=1))
    index.add(record("high", "after_network_recovery", coverage=["a", "b"], cost=9,
                     attempts=4, hits=3))
    index.add(record("cheap", "after_delete_recreate", coverage=["a", "b"], cost=1,
                     attempts=4, hits=3))
    result = index.select(target_id="discovery.late_endpoint", target_revision=0,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                          feedback_identity="feedback-1", include_fixtures=True)
    assert [item["prefix_id"] for item in result["prefixes"]] == ["cheap"]
    selection = result["selection"]
    assert selection["reason"] == "coverage_hitrate_cost"
    assert [item["prefix_id"] for item in selection["considered"]] == ["cheap", "high", "low"]
    assert selection["selection_sha256"]
    provenance = prefix_selection_provenance(selection)
    assert provenance["selection_sha256"] == selection["selection_sha256"]
    assert provenance["feedback_identity"] == "feedback-1"


def test_fixture_prefixes_are_never_live_selected_and_no_random_fallback():
    index = PrefixIndex()
    index.add(record("fixture", "first_creation", fixture=True))
    with pytest.raises(PrefixHistoryError):
        index.select(target_id="discovery.late_endpoint", target_revision=0,
                     rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    assert index.selection_log[-1]["reason"] == "no_eligible_prefix"
    live = record("live", "first_creation", fixture=False)
    index.add(live)
    result = index.select(target_id="discovery.late_endpoint", target_revision=0,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    assert [item["prefix_id"] for item in result["prefixes"]] == ["live"]


def test_selection_honours_target_revision_rmw_state_and_resources():
    index = PrefixIndex()
    index.add(record("g1", "first_creation", resources=["carrier"], fixture=False))
    index.add(record("state-g2", "first_creation", state=fingerprint(network_phase="restored_verified"),
                     fixture=False))
    with pytest.raises(PrefixHistoryError):
        index.select(target_id="discovery.late_endpoint", target_revision=1,
                     rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    with pytest.raises(PrefixHistoryError):
        index.select(target_id="discovery.late_endpoint", target_revision=0,
                     rmw="rmw_fastrtps_cpp", contract_hash="contract-v1")
    with pytest.raises(PrefixHistoryError):
        index.select(target_id="discovery.late_endpoint", target_revision=0,
                     rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                     required_resources=["missing"])
    chosen = index.select(target_id="discovery.late_endpoint", target_revision=0,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                          state_fingerprint=fingerprint().fingerprint_id(),
                          required_resources=["carrier"])
    assert [item["prefix_id"] for item in chosen["prefixes"]] == ["g1"]


def test_insert_replace_delete_are_dependency_guarded():
    index = PrefixIndex()
    index.add(record("base", "first_creation", fixture=False))
    index.add(record("child", "after_network_recovery", depends_on=["base"], fixture=False))
    inserted = record("extra", "after_delete_recreate", fixture=False)
    result = index.transform([{"op": "insert", "record": inserted.to_dict(),
                               "requires_resources": ["carrier"]}],
                             resources={"carrier": True})
    assert result["changed"] == ["extra"]
    assert "extra" in result["prefix_ids"]
    with pytest.raises(PrefixHistoryError):
        index.transform([{"op": "delete", "prefix_id": "base"}])
    with pytest.raises(PrefixError):
        index.transform([{"op": "insert", "record": inserted.to_dict(),
                          "requires_resources": ["carrier"]}],
                        resources={"carrier": None})
    replacement = record("child", "after_network_recovery", depends_on=["base"],
                         coverage=["x"], fixture=False)
    result = index.transform([{"op": "replace", "prefix_id": "child",
                               "record": replacement.to_dict()}])
    assert result["closure_complete"] is True
    result = index.transform([{"op": "delete", "prefix_id": "extra"}])
    assert "extra" not in result["prefix_ids"]


def test_index_round_trip_and_hash_validation():
    index = PrefixIndex()
    index.add(record("one", "first_creation", fixture=False))
    document = index.to_dict()
    restored = PrefixIndex().load(deepcopy(document))
    assert restored.to_dict() == document
    damaged = deepcopy(document)
    damaged["records"][0]["coverage"] = ["tampered"]
    with pytest.raises(PrefixError):
        PrefixIndex().load(damaged)


def test_feedback_outcome_changes_next_prefix_selection():
    index = PrefixIndex()
    index.add(record("alpha", "first_creation", coverage=["a"], attempts=2, hits=2,
                     fixture=False))
    index.add(record("beta", "after_network_recovery", coverage=["a"], attempts=2, hits=0,
                     fixture=False))
    first = index.select(target_id="discovery.late_endpoint", target_revision=0,
                         rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    assert [item["prefix_id"] for item in first["prefixes"]] == ["alpha"]
    selection_sha = first["selection"]["selection_sha256"]
    outcome = index.record_outcome("beta", hit=True, coverage=["b", "c"],
                                   evidence=receipt("fresh_graph", 5),
                                   selection_sha256=selection_sha)
    assert outcome["hit"] is True
    assert outcome["coverage_added"] == ["b", "c"]
    second = index.select(target_id="discovery.late_endpoint", target_revision=0,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    assert [item["prefix_id"] for item in second["prefixes"]] == ["beta"]
    # Prior selection/outcome bytes remain visible: nothing is overwritten.
    assert index.selection_log[0] is not second["selection"]
    assert first["selection"]["selection_sha256"] in {
        event["selection_sha256"] for event in index.selection_log}


def test_outcome_requires_recorded_selection_and_raw_evidence():
    index = PrefixIndex()
    index.add(record("alpha", "first_creation", fixture=False))
    with pytest.raises(PrefixError):
        index.record_outcome("alpha", hit=True)
    with pytest.raises(PrefixHistoryError):
        index.record_outcome("alpha", hit=True, evidence=receipt("fresh_graph"),
                             selection_sha256="e" * 64)
    index.select(target_id="discovery.late_endpoint", target_revision=0,
                 rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    sha = index.selection_log[0]["selection_sha256"]
    with pytest.raises(PrefixHistoryError):
        index.record_outcome("unknown", hit=True, evidence=receipt("fresh_graph"),
                             selection_sha256=sha)


def test_outcome_log_survives_round_trip():
    index = PrefixIndex()
    index.add(record("alpha", "first_creation", fixture=False))
    index.select(target_id="discovery.late_endpoint", target_revision=0,
                 rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    index.record_outcome("alpha", hit=False, coverage=["b"], evidence=receipt("fresh_graph"),
                         selection_sha256=index.selection_log[0]["selection_sha256"])
    document = index.to_dict()
    assert PrefixIndex().load(deepcopy(document)).to_dict() == document


def test_prefix_variation_binds_to_existing_deterministic_input_identity():
    from r2f.campaign.corpus import content_identity
    from r2f.campaign.prefix_index import bind_prefix_input
    prefix = record("alpha", "first_creation", fixture=False)
    binding = bind_prefix_input(prefix, final_config={"reader_delay_sec": 0.01},
                                target_id="discovery.late_endpoint", target_revision=0,
                                rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                                environment={"network_phase": "normal"},
                                parameters={"reader_delay_sec": 0.01},
                                mutation_kind="late_joiner", carrier="r2f_runtime")
    expected = content_identity({"target_id": "discovery.late_endpoint", "target_revision": 0,
                                 "rmw": "rmw_cyclonedds_cpp", "contract_hash": "contract-v1",
                                 "final_config": {"reader_delay_sec": 0.01},
                                 "mutation_kind": "late_joiner", "carrier": "r2f_runtime"})
    assert binding["input_sha256"] == expected
    assert binding["prefix_record_sha256"] == prefix.to_dict()["record_sha256"]
    assert binding["binding_sha256"] == canonical_hash(
        {k: v for k, v in binding.items() if k != "binding_sha256"})


def test_unknown_variation_is_refused_not_defaulted():
    from r2f.campaign.prefix_index import bind_prefix_input
    prefix = record("alpha", "first_creation", fixture=False)
    with pytest.raises(PrefixError):
        bind_prefix_input(prefix, final_config={"reader_delay_sec": 0.01},
                          target_id="discovery.late_endpoint", target_revision=0,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1",
                          parameters={"reader_delay_sec": None})
    with pytest.raises(PrefixError):
        bind_prefix_input(prefix, final_config={},
                          target_id="discovery.late_endpoint", target_revision=0,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
    with pytest.raises(PrefixHistoryError):
        bind_prefix_input(prefix, final_config={"reader_delay_sec": 0.01},
                          target_id="discovery.late_endpoint", target_revision=1,
                          rmw="rmw_cyclonedds_cpp", contract_hash="contract-v1")
