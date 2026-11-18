"""Offline-only producer/adapter checks on the fixed 5978 baseline.

No ROS, subprocess runtime, namespaces, campaign CLI or original-artifact writes.
Standalone-consumer closure is deliberately not claimed by these tests.
"""
from copy import deepcopy
import json
from pathlib import Path
import sys
from unittest.mock import patch

import pytest

from r2f.campaign.runner import run_exploratory_cycle, _persist_exploratory_report
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core import Registry
from r2f.core.evidence import canonical_hash
from r2f.modules.s1.module import register_exploratory
from r2f.reports import exploratory_builder as builder
from r2f.reports.run_report import EXPLORATORY_SCHEMA, exploratory_report_digest, audit_exploratory_report

TEST_ROOT = Path(__file__).resolve().parents[1]
SHADOW_OWNER = TEST_ROOT.parent if TEST_ROOT.name == 'shadow' else None
REPO = SHADOW_OWNER / 'baseline' if SHADOW_OWNER else TEST_ROOT
REVIEW_ROOT = (SHADOW_OWNER.parent if SHADOW_OWNER else
               REPO / 'results/main-review/s1-frontier-repair-20261118')
LIVE = REVIEW_ROOT / 'f1-control-owner/isolated-scope-v2/live-output'
sys.path.insert(0, str(REPO / 'tests'))
from test_fuzz_exploratory_targets import _profile, _corpus, LATE
from test_exploratory_checkpoint import SOURCE


def load(path):
    return json.loads(Path(path).read_text())


def registry():
    result = Registry()
    register_exploratory(result, include_required=False)
    return result


def plan(tmp_path, *, runtime=None):
    corpus = _corpus(LATE, 'P01', boundary='finite', mutator_id='M03')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=42)
    with patch('r2f.campaign.runner.source_manifest', return_value=deepcopy(SOURCE)):
        cycle = run_exploratory_cycle(tmp_path / 'run', corpus=corpus, feedback=feedback,
            scheduler=scheduler, profile=_profile(LATE, ['P01']), run_id='f1-report-fixture',
            execute=runtime is not None, runtime=runtime, max_inputs=1)
    return cycle, load(cycle['report_path'])


def test_actual_runner_generates_standard_plan_report_without_execution(tmp_path):
    cycle, report = plan(tmp_path)
    assert report['schema_version'] == EXPLORATORY_SCHEMA
    assert report['producer_valid'], report['producer_validation_errors']
    assert audit_exploratory_report(cycle['report_path'])['evidence_valid']
    assert cycle['budget']['attempted'] == cycle['budget']['executed'] == 0
    row = report['rows'][0]
    assert row['actual_run'] is False and row['execution_stage'] == 'planned'
    original = load(Path(row['runtime_config_path']).parent / 'row.json')
    assert row['generation_binding'] == original['binding'] == cycle['rows'][0]['binding']
    assert 'expected_config_sha256' not in original['binding']
    assert row['binding']['expected_config_sha256'] == canonical_hash(row['mutation_plan']['final_config'])
    config = load(row['runtime_config_path'])
    observed = {key: config[key] for key in row['binding']['projection_fields']}
    assert row['binding']['observed_config_sha256'] == canonical_hash(observed)
    proof = report['artifact_qualifications'][row['cell_id']]
    assert proof['qualification']['real_ros_execution'] is False
    ledger = load(Path(cycle['report_path']).parent / 'candidate_ledger.json')
    assert ledger['natural_candidate_count'] == 0
    assert report['control_references'] == []
    assert report['proof_provenance']['new_execution'] is False


def test_offline_fixture_cannot_gain_real_qualification(tmp_path):
    from test_feedback_epoch_runner import adapter_runtime
    cycle, report = plan(tmp_path, runtime=adapter_runtime)
    proofs = report['artifact_qualifications']
    assert not any(p['qualification']['real_ros_execution'] for p in proofs.values())
    assert report['runtime_formal'] is False
    assert load(Path(cycle['report_path']).parent / 'candidate_ledger.json')['natural_candidate_count'] == 0


def test_tool_failure_preserved_and_not_candidate(tmp_path):
    def runtime(config, output):
        raise RuntimeError('deliberate offline tool failure')
    cycle, report = plan(tmp_path, runtime=runtime)
    row = report['rows'][0]
    assert row['runtime_status'] == 'tool_error'
    assert row['execution_stage'] == cycle['rows'][0]['execution_stage']
    ledger = load(Path(cycle['report_path']).parent / 'candidate_ledger.json')
    assert ledger['natural_candidate_count'] == 0
    assert ledger['tool_error_count'] == 1


@pytest.mark.parametrize('field', ['final_input', 'mutation_plan', 'raw_sha256', 'binding', 'rmw'])
def test_forged_row_rejected_even_with_recomputed_outer_hash(tmp_path, field):
    cycle, report = plan(tmp_path)
    row = deepcopy(report['rows'][0])
    if field == 'final_input':
        row[field]['reader_delay_sec'] += 1
    elif field == 'mutation_plan':
        row[field]['final_config']['reader_delay_sec'] += 1
        row['mutation_plan_sha256'] = canonical_hash(row[field])
    elif field == 'binding':
        row[field]['expected_config_sha256'] = row[field]['observed_config_sha256'] = 'e' * 64
    else:
        row[field] = 'forged'
    report['rows'][0] = row
    report['report_sha256'] = exploratory_report_digest(report)
    assert report['report_sha256'] == exploratory_report_digest(report)
    with pytest.raises(ValueError):
        builder.qualify_f1_artifacts(Path(cycle['report_path']).parent, row, report)


@pytest.mark.parametrize('damage', ['runtime_projection', 'plan', 'capsule'])
def test_reopens_artifacts_not_saved_binding_labels(tmp_path, monkeypatch, damage):
    cycle, report = plan(tmp_path)
    row = report['rows'][0]
    actual_read = builder._read
    def read(path):
        value = actual_read(path)
        name = Path(path).name
        if damage == 'runtime_projection' and name == 'runtime_config.json':
            value['reader_delay_sec'] += 1
        if damage == 'plan' and name == 'mutation_plan.json':
            value['final_config']['reader_delay_sec'] += 1
        if damage == 'capsule' and name == 'capsule.json':
            value['input']['effective_input']['final_input']['reader_delay_sec'] += 1
        return value
    monkeypatch.setattr(builder, '_read', read)
    with pytest.raises(ValueError):
        builder.qualify_f1_artifacts(Path(cycle['report_path']).parent, row, report)


def test_preplan_rejection_is_retained_not_qualified(tmp_path):
    cycle, report = plan(tmp_path)
    row = deepcopy(cycle['rows'][0])
    for key in ('source_manifest', 'runtime_config_path', 'replay_path'):
        row.pop(key, None)
    row.update(mutation_plan=None, generation_status='rejected', actual_run=False,
               execution_stage='planned', reason_code='target_not_in_exploratory_catalog',
               oracle_verdict='not_applicable', input_layer_receipts=[],
               binding={'status': 'not_executed', 'silent_normalization': False})
    cycle['rows'] = [row]
    out = tmp_path / 'rejected'
    path = builder.persist_f1_exploratory_report(out, cycle, registry())
    result = load(path)
    assert result['rows'][0]['generation_status'] == 'rejected'
    assert not result['artifact_qualifications'][row['cell_id']]['qualification']['real_ros_execution']
    assert load(out / 'candidate_ledger.json')['natural_candidate_count'] == 0


def test_topology_dispatch_preserves_existing_persist_function(tmp_path):
    from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY
    cycle = {'profile': {'runtime_capability': TOPOLOGY_CAPABILITY}}
    reg = object()
    with patch('r2f.campaign.runner._persist_topology_exploratory_report', return_value='original') as old:
        assert _persist_exploratory_report(tmp_path, cycle, reg) == 'original'
        old.assert_called_once_with(tmp_path, cycle, reg)


@pytest.fixture(scope='module')
def six_cycle():
    # Missing evidence must fail, not skip or silently substitute fixtures.
    return load(LIVE / 'cycle.json')


def test_six_historical_real_rows_readonly_derived_report(tmp_path, six_cycle):
    before = (LIVE / 'cycle.json').read_bytes()
    path = builder.persist_f1_exploratory_report(tmp_path / 'derived', six_cycle, registry(), artifact_root=LIVE)
    report = load(path)
    assert report['source_manifest']['head'] == '5978d067e62bb1c9d4e1bfdf3a181333e3ace25d'
    assert len(report['rows']) == 6
    assert report['producer_valid'], report['producer_validation_errors']
    assert all(p['qualification']['real_ros_execution'] is True and p['qualification']['qualification_errors'] == []
               for p in report['artifact_qualifications'].values())
    for row, original in zip(report['rows'], six_cycle['rows']):
        assert row['generation_binding'] == original['binding']
        assert row['raw_path'] == original['raw_path'] and row['raw_sha256'] == original['raw_sha256']
        assert row['oracle_verdict'] == 'pass'
    # Existing consumer containment gate must not be bypassed to make this green.
    audit = audit_exploratory_report(path)
    assert not audit['evidence_valid']
    assert any('escapes campaign directory' in err for err in audit['errors'])
    assert (LIVE / 'cycle.json').read_bytes() == before
    assert load(path.parent / 'candidate_ledger.json')['natural_candidate_count'] == 0


@pytest.mark.parametrize('damage', ['raw_hash', 'guid', 'input_hash'])
def test_real_row_tamper_rejected_without_modifying_originals(six_cycle, monkeypatch, damage):
    row = deepcopy(six_cycle['rows'][0])
    if damage == 'raw_hash':
        row['raw_sha256'] = '0' * 64
        with pytest.raises(ValueError):
            builder.qualify_f1_artifacts(LIVE, row, six_cycle)
        return
    actual_read = builder._read
    def read(path):
        result = actual_read(path)
        if Path(path).name == 'runtime_result.json':
            if damage == 'input_hash':
                result['execution_metadata']['effective_input_sha256'] = '0' * 64
            else:
                # Change endpoint identity in persisted runtime events; raw and
                # independently recomputed ingest remain the authority.
                event = next(e for e in result['events'] if e.get('event') == 'endpoint_created')
                event['endpoint_gid'] = 'forged-guid'
        return result
    monkeypatch.setattr(builder, '_read', read)
    try:
        proof = builder.qualify_f1_artifacts(LIVE, row, six_cycle)
    except ValueError:
        return
    assert not proof['qualification']['real_ros_execution']


@pytest.mark.parametrize('damage', ['binding', 'final', 'plan', 'saved_qualification', 'original_row',
                                    'runtime_config', 'source', 'model', 'generation_receipt'])
def test_standalone_consumer_reopens_case_after_outer_digest_rehash(tmp_path, damage):
    cycle, report = plan(tmp_path)
    path = Path(cycle['report_path'])
    row = report['rows'][0]
    if damage == 'binding':
        row['binding']['expected_config_sha256'] = row['binding']['observed_config_sha256'] = '0' * 64
    elif damage == 'final':
        row['final_input']['reader_delay_sec'] += 1
    elif damage == 'plan':
        row['mutation_plan']['final_config']['reader_delay_sec'] += 1
        row['mutation_plan_sha256'] = canonical_hash(row['mutation_plan'])
    elif damage == 'saved_qualification':
        report['artifact_qualifications'][row['cell_id']]['qualification']['real_ros_execution'] = True
    elif damage == 'original_row':
        original_path = Path(row['runtime_config_path']).parent / 'row.json'
        original = load(original_path)
        original['binding']['normalized_config_sha256'] = '0' * 64
        original_path.write_text(json.dumps(original))
    elif damage == 'runtime_config':
        config = load(row['runtime_config_path'])
        config['reader_delay_sec'] += 1
        Path(row['runtime_config_path']).write_text(json.dumps(config))
    elif damage == 'source':
        report['source_manifest']['head'] = 'forged-head'
        report['identity']['source_sha256'] = canonical_hash(report['source_manifest'])
    elif damage == 'model':
        report['model']['targets'][LATE]['target_revision'] = 9
        report['identity']['target_sha256'] = canonical_hash(report['model']['targets'])
        report['identity']['model_sha256'] = canonical_hash(report['model'])
    else:
        row['generation_binding']['normalized_config_sha256'] = '0' * 64
        row['binding']['generation_binding_sha256'] = canonical_hash(row['generation_binding'])
    report['producer_valid'] = True  # Saved success labels never authorize.
    report['report_sha256'] = exploratory_report_digest(report)
    path.write_text(json.dumps(report))
    audit = audit_exploratory_report(path)
    assert not audit['evidence_valid'], audit
    assert any('f1_' in error for error in audit['errors']), audit


def test_fixture_judged_saved_labels_do_not_grant_consumer_qualification(tmp_path):
    from test_feedback_epoch_runner import adapter_runtime
    cycle, report = plan(tmp_path, runtime=adapter_runtime)
    assert not audit_exploratory_report(cycle['report_path'])['evidence_valid']
    assert any('f1_execution_not_qualified' in error for error in report['audit']['errors'])
    assert report['rows'][0]['execution_stage'] == cycle['rows'][0]['execution_stage']
    assert report['rows'][0]['actual_run'] == cycle['rows'][0]['actual_run']


def test_failed_qualification_never_admits_natural_candidate(tmp_path, monkeypatch):
    cycle, _ = plan(tmp_path)
    row = deepcopy(cycle['rows'][0])
    row.update(execution_stage='judged', actual_run=False, oracle_verdict='fail')
    cycle['rows'] = [row]
    # Malformed offline evidence remains in report and exclusions, never a
    # natural candidate even if someone supplies the success label.
    cycle['runtime_formal'] = False
    path = builder.persist_f1_exploratory_report(tmp_path / 'unqualified', cycle, registry(),
                                               artifact_root=Path(row['runtime_config_path']).parents[2])
    report = load(path)
    assert not report['producer_valid']
    assert report['rows'][0]['oracle_verdict'] == 'fail'
    assert report['qualification_exclusions']
    assert load(path.parent / 'candidate_ledger.json')['natural_candidate_count'] == 0
    assert not report['audit']['evidence_valid']


def strip_adapter_markers(report):
    for key in ('artifact_qualifications', 'producer_valid', 'producer_validation_errors',
                'proof_provenance', 'qualification_exclusions', 'limitations'):
        report.pop(key, None)
    report['model'].pop('targets', None)
    report['model'].pop('rules', None)
    for row in report['rows']:
        row.pop('generation_binding', None)
        row['binding'].pop('schema', None)
    report['identity']['model_sha256'] = canonical_hash(report['model'])
    report['report_sha256'] = exploratory_report_digest(report)
    return report


@pytest.mark.parametrize('claim', ['actual', 'observed', 'judged'])
def test_removing_all_new_markers_cannot_select_legacy_live_bypass(tmp_path, claim):
    from test_feedback_epoch_runner import adapter_runtime
    cycle, report = plan(tmp_path, runtime=adapter_runtime)
    report = strip_adapter_markers(report)
    row = report['rows'][0]
    if claim == 'actual':
        row.update(actual_run=True, execution_stage='aborted')
    else:
        row.update(actual_run=False, execution_stage='effect_observed' if claim == 'observed' else 'judged')
    # The attack retains equal expected/observed labels and valid raw hash,
    # removes every new marker, then recomputes outer identity/digest.
    assert row['binding']['expected_config_sha256'] == row['binding']['observed_config_sha256']
    report['report_sha256'] = exploratory_report_digest(report)
    path = Path(cycle['report_path'])
    path.write_text(json.dumps(report))
    audit = audit_exploratory_report(path)
    assert not audit['evidence_valid'], audit
    assert any('f1_generation_binding_missing' in error for error in audit['errors']), audit


def test_unmarked_legacy_f1_actual_without_canonical_layout_is_invalid(tmp_path):
    from test_fuzz_audit import base_row, build_report
    # Old synthetic fixture has a raw hash and matching binding labels but no
    # canonical case/capsule/config artifacts. Giving it an F1 identity cannot
    # convert it to a real execution, even with all report hashes recomputed.
    row = base_row(tmp_path, 'legacy-claim', verdict='pass', target_id=LATE)
    path, report = build_report(tmp_path, [row])
    report['profile']['target_ids'] = [LATE]
    report['identity']['profile_sha256'] = canonical_hash(report['profile'])
    report['report_sha256'] = exploratory_report_digest(report)
    path.write_text(json.dumps(report))
    audit = audit_exploratory_report(path)
    assert not audit['evidence_valid']
    assert any('f1_generation_binding_missing' in error for error in audit['errors'])


def test_unmarked_legacy_f1_plan_compatibility_never_grants_real_or_candidate(tmp_path):
    cycle, report = plan(tmp_path)
    report = strip_adapter_markers(report)
    path = Path(cycle['report_path'])
    path.write_text(json.dumps(report))
    audit = audit_exploratory_report(path)
    assert audit['evidence_valid'], audit
    assert report['rows'][0]['actual_run'] is False
    assert report['rows'][0]['execution_stage'] == 'planned'
    assert load(path.parent / 'candidate_ledger.json')['natural_candidate_count'] == 0


@pytest.mark.parametrize('damage', ['model_shape', 'mutator_spec', 'artifact_shape'])
def test_malformed_rehashed_evidence_returns_invalid_not_exception(tmp_path, damage):
    cycle, report = plan(tmp_path)
    path = Path(cycle['report_path'])
    if damage == 'model_shape':
        report['model'] = ['forged']
    elif damage == 'mutator_spec':
        report['model']['mutator_catalog']['M03']['version'] = 999
        report['model']['mutator_catalog_sha256'] = canonical_hash(report['model']['mutator_catalog'])
        report['identity']['model_sha256'] = canonical_hash(report['model'])
    else:
        Path(report['rows'][0]['runtime_config_path']).write_text('[]')
    report['report_sha256'] = exploratory_report_digest(report)
    path.write_text(json.dumps(report))
    audit = audit_exploratory_report(path)
    assert not audit['evidence_valid'], audit
