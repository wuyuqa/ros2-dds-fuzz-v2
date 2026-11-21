"""Offline source/model crosswalk; neither case inventory nor fixtures are DDS proof."""
import json

import pytest

from r2f.modules.s1 import catalog


CASE_IDS = {
    "Fast-DDS-PR6153", "Fast-DDS-PR6336", "Fast-DDS-issue3770",
    "OpenDDS/OpenDDS#2057", "OpenDDS/OpenDDS#5162", "OpenDDS/OpenDDS#5202",
    "OpenDDS/OpenDDS#5221", "core215", "cxx294", "cyclonedds-PR2292",
    "cyclonedds-issue1097", "eProsima/Micro-XRCE-DDS#41", "py130", "py200", "xrce115",
}


def test_public_catalog_preserves_four_distinct_obligation_inventories():
    data = catalog.coverage_lineage()
    assert data["schema"] == "r2f.s1-coverage-lineage/v1"
    assert {row["id"] for row in data["obligations"]} == {f"C{i:02d}" for i in range(1, 19)}
    assert {row["id"] for row in data["user_stories"]} == set(range(1, 43))
    assert {row["case_id"] for row in data["cases"]} == CASE_IDS
    assert data["required_baseline"]["cell_count"] == 72
    assert data["required_baseline"]["target_revisions"] == {
        "discovery.late_endpoint": 0, "discovery.late_join_delivery": 0,
        "discovery.interruption_recovery": 0, "builtin.discovery_projection_reconciliation": 0,
    }
    assert data["execution_evidence"] is False and data["maturity_evidence"] is False


def test_registry_projection_lineage_links_python_case_to_declared_model_and_capture():
    from r2f.core import Registry
    from r2f.modules.s1.module import register_exploratory

    registry = Registry()
    register_exploratory(registry, include_required=True)
    record = registry.get_rule_lineage("builtin.discovery_projection_reconciliation.fuzz_v1",
                                       "s1.projection.qos_field.v1")
    lineage = record["coverage_lineage"]
    obligation = next(row for row in lineage["obligations"] if row["id"] == "C08")
    assert "py130" in obligation["case_ids"]
    case = next(row for row in lineage["cases"] if row["case_id"] == "py130")
    assert case["research_classification"] == "Python binding数据结构缺陷"
    assert case["reproduced_here"] is False
    assert case["source_ref"]["artifact_id"] == "case-matrix.json"
    source = lineage["source_captures"]["case-matrix.json"]
    assert source["verification_scope"] == "captured_artifact_only"
    assert len(source["artifact_sha256"]) == 64
    binding = lineage["model_binding"]
    assert binding["target_id"] == record["target_id"] and binding["target_revision"] == 0
    surface = next(row for row in binding["surfaces"] if row["surface_id"] == "P02")
    assert surface["carrier_id"] == "ros2_endpoint" and surface["mutator_ids"] == ["M02"]
    assert "s1.projection.qos_field.v1" in surface["rule_ids"]
    assert set(surface["fact_ids"]) <= {row["id"] for row in binding["facts"]}
    assert binding["relation"] == "declared_model_context_not_full_obligation_coverage"
    assert lineage["execution_evidence"] is False and lineage["maturity_evidence"] is False


def test_every_story_and_obligation_has_explicit_applicability_context_and_gap():
    data = catalog.coverage_lineage()
    obligations = {row['id']: row for row in data['obligations']}
    assert set().union(*(set(row['story_ids']) for row in obligations.values())) == set(range(1, 43))
    for story in data['user_stories']:
        assert story['obligation_ids']
        assert all(story['id'] in obligations[cid]['story_ids'] for cid in story['obligation_ids'])
    for row in obligations.values():
        assert row['applicability']['status'] == 'applicable'
        assert row['applicability']['basis']
        assert row['current_source_status'] == 'unverified'
        assert row['remaining_obligations'] and row['missing_observation_policy'] == 'unknown'
        assert row['model_links'] or row['unmapped_model_reason']
        for link in row['model_links']:
            binding = data['model_bindings'][link['target_id']]
            assert set(link['surface_ids']) <= {s['surface_id'] for s in binding['surfaces']}
            assert link['relation'] == 'context_not_full_coverage'
    assert obligations['C03']['semantic_boundary'] == 'participant_lease_not_writer_liveliness'
    assert obligations['C14']['semantic_boundary'] == 'filter_effect_not_endpoint_matching'
    assert obligations['C17']['model_links'] == []
    assert obligations['C18']['model_links'] == []
    assert data['model_bindings']['discovery.endpoint_topology.fuzz_v1']['requires_opt_in'] is True
    assert len(data['model_bindings']) == 11


def test_historical_normal_episode_links_raw_without_claiming_current_or_replay_evidence():
    data = catalog.coverage_lineage()
    rows = {row['id']: row for row in data['obligations']}
    evidence = data['evidence_captures'][0]
    assert rows['C01']['evidence_refs'] == [evidence['evidence_id']]
    assert rows['C18']['evidence_refs'] == [evidence['evidence_id']]
    assert rows['C03']['evidence_refs'] == []
    assert rows['C03']['evidence_status'] == 'unobserved'
    assert rows['C01']['evidence_status'] == 'source_captured_partial_real_validation'
    assert evidence['qualified_for_current_source'] is False
    assert evidence['replay']['status'] == 'not_in_capture' and evidence['replay']['attempts'] == []
    record = evidence['record']
    assert record['source_manifest']['head'] == '32759d89c2b6036b0314cb27e9b87e70bc0b3c7c'
    assert record['source_manifest']['behavior_tree_sha256'] == 'a2948340534d7dd713b83f9a24d35d331a791bc959be8fd34e9aa0731df23329'
    assert record['raw_sha256'] == 'be5348316111ebbab3cddbb17edfae9c7ec14bdefdae913d451d0604f26b51da'
    assert record['report_file_sha256'] == 'd280659686a8adbdc8ad48aa33def4cc97a4ae06f2ebe2c69551cd7f9a84448d'
    assert record['budget']['actual'] == 1 and record['candidate_counts']['natural_candidate_count'] == 0
    assert record['target_revision'] == 0 and record['domain'] == 104
    assert record['rmw'] == 'rmw_cyclonedds_cpp' and record['limitations']
    assert evidence['reuse_policy'] == 'historical_only_requires_new_source_validation'
    validation = data['captured_artifact_validation']
    assert validation['count'] == 140 and validation['all_match'] is True
    assert validation['rechecked_here'] is False
    assert rows['C18']['captured_assessment']['status'] == '有限重建实证；两项工具修复未实施'
    assert '753886c' in data['assessment_scope']['C18_followup']


def test_model_context_reuses_authored_seed_and_original_manifest_identity():
    data = catalog.coverage_lineage()
    binding = data['model_bindings']['builtin.discovery_projection_reconciliation.fuzz_v1']
    seed = next(row for row in binding['seed_refs'] if row['seed_id'] == 'P02')
    assert seed['provider'] == 'r2f.modules.s1.fuzz_catalog.load_seeds'
    assert seed['surface_refs'] == ['P02']
    assert len(seed['content_sha256']) == 64
    assert seed['status'] == 'authored' and seed['execution_evidence'] is False
    assert all(row['target_id'] == binding['target_id'] for row in binding['rule_lineage_refs'])
    topology = data['model_bindings']['discovery.endpoint_topology.fuzz_v1']
    assert topology['manifest_sha256'] == '0c4815b9b1b9b70ad639089ac3cdbf6a6ae9c4ba3997bcb4eba4ec8b09ab8234'
    assert topology['seed_sources'][0]['provider'] == 'r2f.modules.s1.fuzz_mutators_v2.corpus_v2'
    assert topology['seed_sources'][0]['runtime_qualification'] == 'unverified'
    assert binding['seed_sources'][0]['runtime_qualification'] == 'unverified'


def test_case_scope_preserves_feature_test_and_related_fix_distinctions():
    data = catalog.coverage_lineage()
    cases = {row['case_id']: row for row in data['cases']}
    assert cases['Fast-DDS-PR6336']['research_classification'] == '功能扩展'
    assert cases['Fast-DDS-PR6336']['new_discovery'] is False
    assert cases['core215']['versions']['affected_range'] == 'unknown'
    assert cases['core215']['versions']['first_fixed_release'] == 'unknown'
    assert cases['core215']['proposed_pre_post']['status'] == 'proposed_not_executed'
    assert cases['OpenDDS/OpenDDS#2057']['versions']['affected_release'] == 'unknown'
    for case_id in ('xrce115', 'eProsima/Micro-XRCE-DDS#41', 'OpenDDS/OpenDDS#5162'):
        assert cases[case_id]['runtime_applicability']['status'] == 'case_specific_validation_required'
        assert cases[case_id]['runtime_applicability']['basis'] == cases[case_id]['disposition']
        assert cases[case_id]['reproduced_here'] is False
    relation = next(row for row in data['case_relations'] if 'OpenDDS/OpenDDS#5202' in row['case_ids'])
    assert set(relation['case_ids']) == {'OpenDDS/OpenDDS#5202', 'OpenDDS/OpenDDS#5221'}
    assert relation['independent_discovery_credit'] == 0
    assert relation['kind'] == 'captured_related_fix_chain_not_confirmed_new_roots'


@pytest.mark.parametrize('fault', ['target', 'surface', 'source_hash', 'evidence', 'story', 'credit', 'applicability'])
def test_invalid_crosswalk_fails_closed_through_public_catalog(tmp_path, monkeypatch, fault):
    from r2f.modules.s1 import coverage

    data = json.loads(coverage._CAPTURE.read_text())
    if fault == 'target':
        data['obligations'][0]['model_links'][0]['target_id'] = 'undeclared'
    elif fault == 'surface':
        data['obligations'][0]['model_links'][0]['surface_ids'] = ['absent']
    elif fault == 'source_hash':
        data['source_captures']['case-matrix.json']['artifact_sha256'] = 'z' * 64
    elif fault == 'evidence':
        data['obligations'][0]['evidence_refs'] = ['missing-evidence']
    elif fault == 'story':
        data['obligations'][0]['story_ids'] = [999]
    elif fault == 'credit':
        data['evidence_captures'][0]['qualified_for_current_source'] = True
    else:
        data['obligations'][0]['applicability']['status'] = 'not_applicable'
    path = tmp_path / 'invalid-coverage.json'
    path.write_text(json.dumps(data))
    monkeypatch.setattr(coverage, '_CAPTURE', path)
    with pytest.raises(ValueError, match='coverage lineage'):
        catalog.coverage_lineage()


def test_source_references_distinguish_markdown_lines_from_json_pointers():
    data = catalog.coverage_lineage()
    assert data['source_captures']['parent-spec']['format'] == 'markdown'
    assert data['source_captures']['case-matrix.json']['format'] == 'json'
    assert data['user_stories'][0]['source_ref']['line'] == 25
    assert data['obligations'][0]['spec_ref']['line'] == 85
    for row in data['obligations']:
        assert row['source_ref']['json_pointer'].startswith('/rows/')
        assert 'json_pointer' not in row['spec_ref']


def test_new_seed_snapshot_is_distinct_from_strict_current_corpus_validation():
    data = catalog.coverage_lineage()
    lineage = data["seed_lineage"]
    assert lineage["capture"]["kind"] == "new_seed_metadata_snapshot"
    assert lineage["capture"]["source_head"] == "62d0b716a8ae6a2881c6b766b370cb6546b27d3d"
    assert lineage["capture"]["source_frozen"] is False
    assert lineage["reference_source"] == "strictly_validated_corpus"
    assert lineage["current_loadability"] == {"status": "validated", "reason": "strict_load_seeds_passed"}
    assert lineage["qualified_for_current_source"] is False
    assert lineage["runtime_qualification"] == "unverified"
    assert lineage["execution_evidence"] is False and lineage["maturity_evidence"] is False
    assert len(lineage["seeds"]) == 12 and len(data["model_bindings"]) == 11
    assert lineage["seeds"]["W01"]["status"] == "authored"
    assert lineage["seeds"]["W01"]["context"]["model_target_ids"] == []
    assert {sid for sid, seed in lineage["seeds"].items() if seed["status"] == "capability_pending"} == {
        "E02", "E03", "G01"}
    for binding in data["model_bindings"].values():
        for ref in binding["seed_refs"]:
            captured = lineage["seeds"][ref["seed_id"]]
            assert ref["seed_id"] != "W01"
            assert ref["metadata_source"] == "strictly_validated_corpus"
            assert ref["current_loadability"] == lineage["current_loadability"]
            assert ref["source_file"]["file_sha256"] == captured["source_file"]["file_sha256"]
            assert ref["content_sha256"] == captured["content_sha256"]
            assert ref["status"] == captured["status"]
            assert ref["capability"] == captured["capability"]
    lineage["seeds"]["P02"]["capability"]["status"] = "changed_by_caller"
    assert catalog.coverage_lineage()["seed_lineage"]["seeds"]["P02"]["capability"]["status"] == "authored"
