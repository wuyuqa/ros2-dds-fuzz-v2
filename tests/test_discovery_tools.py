"""Opt-in tools at the canonical input/artifact and external dependency seams."""
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.identity import semantic_input, runtime_config
from r2f.core import canonical_hash


TARGET = 'builtin.discovery_projection_reconciliation'


def test_tools_manifest_is_explicit_replayable_input_and_requires_layers():
    layers = {'projection_discovery_layers': 'participant_topic_v1'}
    ref = {'path': '/offline/dependencies.json', 'sha256': 'a'*64}
    enabled = semantic_input('baseline', {**layers, 'projection_discovery_tools': ref}, target_id=TARGET)
    assert enabled['projection_discovery_tools'] == ref
    assert canonical_hash(enabled) != canonical_hash(semantic_input('baseline', layers, target_id=TARGET))
    config = runtime_config(enabled, target_id=TARGET, rmw='rmw_cyclonedds_cpp', arm='baseline',
        run_id='offline', domain_id=19, mutation_kind='endpoint_qos_variation', carrier='ros2_endpoint')
    assert config['projection_discovery_tools'] == ref
    with pytest.raises(ValueError, match='requires_projection_discovery_layers'):
        semantic_input('baseline', {'projection_discovery_tools': ref}, target_id=TARGET)
    with pytest.raises(ValueError, match='reference'):
        semantic_input('baseline', {**layers, 'projection_discovery_tools': {**ref, 'samples': []}}, target_id=TARGET)


def test_dependency_manifest_detects_changes_before_any_binding_import(tmp_path):
    from r2f.runtime.discovery_tools import load_tools_environment
    root = tmp_path/'sdk'
    (root/'cyclonedds').mkdir(parents=True)
    for name in ('__init__.py', 'builtin.py', '_clayer.fixture.so'):
        (root/'cyclonedds'/name).write_bytes(b'offline external dependency identity')
    library = tmp_path/'libddsc.fixture.so'
    library.write_bytes(b'offline library; never loaded')
    files = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in [*root.rglob('*'), library] if p.is_file()}
    value = {'schema': 'r2f.discovery-python-environment/v1', 'python_root': str(root),
             'distribution': 'cyclonedds', 'version': '0.10.5', 'files': files,
             'library': {'path': str(library), 'sha256': files[str(library)]}}
    path = tmp_path/'manifest.json'
    path.write_text(json.dumps(value))
    ref = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    assert load_tools_environment(ref) == value
    (root/'additional.py').write_text('raise RuntimeError("must not execute")')
    with pytest.raises(ValueError, match='inventory_changed'):
        load_tools_environment(ref)
    (root/'additional.py').unlink()
    library.write_bytes(b'tampered')
    with pytest.raises(ValueError, match='dependency_changed'):
        load_tools_environment(ref)


def test_binding_schema_errors_are_raw_backed_review_material_at_projection_seam(tmp_path):
    from test_discovery_layers_projection import parts_with_layers
    from test_instrumentation_sedp_projection import jsonraw
    from r2f.instrumentation.sedp import reconcile_discovery_projection
    wire, graph, bindings, window, layers = parts_with_layers(tmp_path)
    raw = {'schema': 'r2f.discovery-tools/v1', 'status': 'unknown',
           'candidate_eligible': False, 'responsibility_layer': 'observation_tools',
           'run_id': 'offline', 'domain_id': 19, 'events': [], 'workers': [],
           'cleanup_complete': False, 'reasons': ['binding_observation_failed']}
    tools = {'schema': raw['schema'], 'status': raw['status'],
             'raw': jsonraw(tmp_path/'tools.json', raw)}
    result = reconcile_discovery_projection(wire, graph, endpoint_bindings=bindings,
        observation_window=window, discovery_layers=layers, discovery_tools=tools)
    auxiliary = result['schema_check']['discovery_tools']
    assert auxiliary['status'] == 'unknown'
    assert auxiliary['review_required'] is True
    assert auxiliary['candidate_eligible'] is False
    assert auxiliary['raw'] == tools['raw']
    assert auxiliary['responsibility_layer'] == 'observation_tools'
    assert auxiliary['reasons']
    assert result['comparisons']  # endpoint evidence is retained independently


def test_cli_projection_distinguishes_supported_fields_and_background_crash():
    from r2f.instrumentation.discovery_tools import compare_cli_projection
    participant = '0102030405060708090a0b0c000001c1'
    endpoint = '0102030405060708090a0b0c00001003'
    text = 'Participant 01020304-0506-0708-090a-0b0c000001c1\nrt/demo\nMsg\n01020304-0506-0708-090a-0b0c00001003'
    endpoints = [{'key': endpoint, 'participant_key': participant, 'topic_name': 'rt/demo', 'type_name': 'Msg'}]
    receipt = {'subcommand': 'ls', 'stdout': text, 'stderr': '', 'exit_code': 0, 'thread_errors': []}
    result = compare_cli_projection(receipt, endpoints)
    assert result['status'] == 'consistent'
    assert result['assurance'] == 'textual_field_visibility_not_atomic_row_or_instance_identity'
    assert result['candidate_eligible'] is False
    receipt.update(subcommand='ps', stdout='01020304-0506-0708-090a-0b0c000001c1 rt/demo')
    assert compare_cli_projection(receipt, endpoints)['status'] == 'consistent'
    assert compare_cli_projection(receipt, endpoints)['field_applicability']['type'] == 'not_presented_by_ps'
    receipt.update(thread_errors=[{'error_type': 'TypeError', 'error': 'historical CLI class mismatch'}])
    error = compare_cli_projection(receipt, endpoints)
    assert error['status'] == 'tool_error'
    assert error['responsibility_layer'] == 'observation_tool'
    assert error['sut_violation_established'] is False


def test_auxiliary_tool_problem_uses_existing_evidence_query_without_candidate(tmp_path):
    from r2f.campaign.anomalies import finish_evidence
    from r2f.reports.candidate_report import query_runner_evidence
    source = {'head': 'offline_fixture_no_execution'}
    manifest = {'run_id': 'offline-tools', 'source_manifest': source}
    (tmp_path/'manifest.json').write_text(json.dumps(manifest))
    tools = {'schema': 'r2f.discovery-tools-check/v1', 'status': 'unknown',
             'review_required': True, 'candidate_eligible': False,
             'responsibility_layer': 'observation_tools', 'reasons': ['binding_schema_mismatch']}
    row = {'run_id': 'offline-tools', 'cell_id': 'rmw_cyclonedds_cpp.baseline.0',
           'target_id': TARGET, 'target_revision': 0, 'rule_id': 'offline',
           'effective_input_sha256': 'a'*64, 'execution_stage': 'judged', 'actual_run': False,
           'oracle_verdict': 'pass', 'typed_facts': {'projection': {'schema_check': {'discovery_tools': tools}}}}
    report = {'run_id': 'offline-tools', 'rows': [row]}
    def persist():
        (tmp_path/'report.json').write_text(json.dumps(report))
        return tmp_path/'report.json'
    finish_evidence(tmp_path, report, persist, source=source)
    queried = query_runner_evidence(tmp_path)
    assert queried['evidence_valid'], queried
    packet, = queried['packets']
    assert packet['runner_diagnostic']['kind'] == 'observation_insufficient'
    assert packet['runner_diagnostic']['candidate_eligible'] is False
    assert packet['facts']['typed']['projection']['schema_check']['discovery_tools'] == tools
    assert packet['rules']['oracle_verdict'] == 'pass'


@pytest.mark.parametrize('tools', [None, {'review_required': False, 'status': 'consistent'}])
def test_native_topic_difference_reaches_existing_evidence_review_view(tmp_path, tools):
    from r2f.campaign.anomalies import finish_evidence
    from r2f.reports.candidate_report import query_runner_evidence
    source = {'head': 'offline_fixture_no_execution'}
    (tmp_path/'manifest.json').write_text(json.dumps({'run_id': 'offline-native', 'source_manifest': source}))
    schema = {'discovery_layers': {'topic': {'field_comparisons': [
        {'topic': 'non_control', 'status': 'mismatch_requires_review', 'candidate_eligible': False}]}}}
    if tools is not None:
        schema['discovery_tools'] = tools
    row = {'run_id': 'offline-native', 'cell_id': 'rmw_cyclonedds_cpp.baseline.0',
           'target_id': TARGET, 'target_revision': 0, 'rule_id': 'offline',
           'effective_input_sha256': 'a'*64, 'execution_stage': 'judged', 'actual_run': False,
           'oracle_verdict': 'pass', 'typed_facts': {'projection': {'schema_check': schema}}}
    report = {'run_id': 'offline-native', 'rows': [row]}
    def persist():
        (tmp_path/'report.json').write_text(json.dumps(report))
        return tmp_path/'report.json'
    finish_evidence(tmp_path, report, persist, source=source)
    result = query_runner_evidence(tmp_path)
    assert result['evidence_valid']
    packet, = result['packets']
    assert packet['runner_diagnostic']['kind'] == 'observation_insufficient'
    assert packet['runner_diagnostic']['candidate_eligible'] is False
    assert packet['rules']['oracle_verdict'] == 'pass'
