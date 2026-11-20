"""Offline raw-backed/report contracts; never starts ROS or grants live credit."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch
import pytest

from r2f.core.evidence import canonical_hash
from r2f.reports import run_report as reports
from r2f.reports.exploratory_builder import qualify_f1_artifacts
import test_exploratory_checkpoint as checkpoint_fixtures
import test_g01_fuzz_cycle as g01
from test_feedback_epoch_runner import adapter_runtime


def resumed_fixture(tmp_path):
    doc = checkpoint_fixtures.profile()
    first = checkpoint_fixtures.run_cycle(tmp_path / 'first', doc, maximum=1, runtime=adapter_runtime)
    assert first['budget']['executed'] == 1
    state = checkpoint_fixtures.restore(tmp_path / 'first', tmp_path / 'second', doc)
    cycle = checkpoint_fixtures.run_cycle(tmp_path / 'second', doc, state, maximum=3, runtime=adapter_runtime)
    report = json.loads((tmp_path / 'second/report.json').read_text())
    return cycle, report


def test_resumed_report_separates_cumulative_budget_from_new_rows(tmp_path):
    cycle, report = resumed_fixture(tmp_path)
    assert report['budget']['executed'] == 3
    assert report['cycle_budget']['executed'] == 2
    assert len(report['rows']) == 2
    assert report['resume_origin']['checkpoint_sha256']
    errors = []
    reports._audit_exploratory_budget(report, report['rows'], errors, root=tmp_path / 'second')
    assert errors == []
    saved = json.loads((tmp_path / 'second/resume-origin-checkpoint.json').read_text())
    original = json.loads((tmp_path / 'first/checkpoint.json').read_text())
    assert saved == original
    assert cycle['resume_origin'] == report['resume_origin']


@pytest.mark.parametrize('fault', ['cycle_executed', 'cycle_negative', 'cycle_bool',
                                  'missing_origin', 'changed_origin', 'source', 'prefix'])
def test_resumed_budget_does_not_trust_new_counter_or_origin_labels(tmp_path, fault):
    _, report = resumed_fixture(tmp_path)
    root = tmp_path / 'second'
    if fault == 'cycle_executed': report['cycle_budget']['executed'] += 1
    if fault == 'cycle_negative': report['cycle_budget']['attempted'] = -1
    if fault == 'cycle_bool': report['cycle_budget']['actual'] = True
    if fault == 'missing_origin': (root / 'resume-origin-checkpoint.json').unlink()
    if fault in ('changed_origin', 'source', 'prefix'):
        path = root / 'resume-origin-checkpoint.json'
        origin = json.loads(path.read_text())
        if fault == 'changed_origin': origin['state']['continuation']['budget']['executed'] = 0
        if fault == 'source': origin['identity']['source_manifest']['head'] = 'different'
        if fault == 'prefix': origin['state']['scheduler']['selections'][0]['selection_sha256'] = 'e' * 64
        origin['checkpoint_sha256'] = canonical_hash({k:v for k,v in origin.items() if k!='checkpoint_sha256'})
        path.write_text(json.dumps(origin))
        # A matching label is not proof of a model-valid source/prefix/counter.
        report['resume_origin']['checkpoint_sha256'] = origin['checkpoint_sha256']
    errors = []
    reports._audit_exploratory_budget(report, report['rows'], errors, root=root)
    assert errors


def test_legacy_fresh_budget_contract_still_checks_actual_rows():
    row = {'actual_run': True, 'execution_stage': 'judged', 'cell_id': 'one'}
    report = {'budget': {'planned':1,'generated':1,'attempted':1,'executed':1},
              'last_state': {'planned':1,'executed':1,'cell_id':'one'}, 'abort':None}
    errors=[]
    reports._audit_exploratory_budget(report,[row],errors)
    assert errors==[]
    report['budget']['executed']=0
    errors=[]
    reports._audit_exploratory_budget(report,[row],errors)
    assert any('executed_mismatch' in e for e in errors)


@pytest.fixture
def noop_report(tmp_path):
    cycle = g01.run(tmp_path / 'g01', bfs=True, maximum=4, execute=False)
    root = tmp_path / 'g01'
    report = json.loads((root / 'report.json').read_text())
    row = next(r for r in report['rows'] if r.get('rejected_mutation_plan'))
    assert row['rejected_mutation_plan']['final_config']['pair_relation'] == 'BR'
    return root, report, row


def test_g01_no_effective_proposal_is_audited_before_isolated_runtime_gate(noop_report):
    root, report, row = noop_report
    report['runtime_boundary'] = 'isolated_netns'
    errors=[]
    with patch.object(reports, 'boundary_qualification_for_row', side_effect=AssertionError('no execution gate for certified input rejection')):
        assert reports._verify_exploratory_row(root,row,report,errors),errors
    assert not errors
    proof=qualify_f1_artifacts(root,row,report)
    assert proof['qualification']['real_ros_execution'] is False
    assert proof['qualification']['real_runtime_attempt'] is False
    assert proof['artifact_references']


@pytest.mark.parametrize('fault', ['raw_claim','execution_claim','runtime_file','plan','task','saved_row'])
def test_g01_no_effective_proposal_cannot_hide_execution_or_fake_a_plan(noop_report, fault):
    root, report, original = noop_report
    row=deepcopy(original); case=root/'cases'/row['cell_id']
    if fault=='raw_claim': row['raw_path']='forged.jsonl'
    if fault=='execution_claim': row['actual_run']=True
    if fault=='runtime_file': (case/'runtime').mkdir()
    if fault=='plan': row['rejected_mutation_plan']['final_config']['pair_relation']='RR'
    if fault=='task': row['input_search']['task_id']='b'*64
    if fault=='saved_row': (case/'row.json').write_text('{}')
    with pytest.raises((ValueError,KeyError)):
        qualify_f1_artifacts(root,row,report)


def test_three_segments_keep_independent_origins_and_prefixes(tmp_path):
    doc = checkpoint_fixtures.profile()
    checkpoint_fixtures.run_cycle(tmp_path / 'first', doc, maximum=1, runtime=adapter_runtime)
    state = checkpoint_fixtures.restore(tmp_path / 'first', tmp_path / 'second', doc)
    checkpoint_fixtures.run_cycle(tmp_path / 'second', doc, state, maximum=2, runtime=adapter_runtime)
    state = checkpoint_fixtures.restore(tmp_path / 'second', tmp_path / 'third', doc)
    checkpoint_fixtures.run_cycle(tmp_path / 'third', doc, state, maximum=3, runtime=adapter_runtime)
    report = json.loads((tmp_path / 'third/report.json').read_text())
    assert report['budget']['executed'] == 3
    assert report['cycle_budget']['executed'] == len(report['rows']) == 1
    errors = []
    reports._audit_exploratory_budget(report, report['rows'], errors, root=tmp_path / 'third')
    assert not errors


@pytest.mark.parametrize('fault', ['local_history', 'missing_final', 'changed_final', 'missing_cycle'])
def test_resumed_budget_cannot_reuse_history_or_ignore_final_checkpoint(tmp_path, fault):
    _, report = resumed_fixture(tmp_path)
    root = tmp_path / 'second'
    if fault == 'local_history': report['row_sequences'][0]['selection_sequence'] = 0
    if fault == 'missing_final': (root / 'checkpoint.json').unlink()
    if fault == 'changed_final': report['budget']['elapsed_sec'] += 1
    if fault == 'missing_cycle': del report['cycle_budget']
    errors = []
    reports._audit_exploratory_budget(report, report['rows'], errors, root=root)
    assert errors


def test_noop_source_artifacts_cannot_jointly_forge_a_task(noop_report):
    root, report, row = noop_report
    row['input_search']['task_id'] = 'a' * 64
    original = deepcopy(row)
    original['binding'] = original.pop('generation_binding')
    (root / 'cases' / row['cell_id'] / 'row.json').write_text(json.dumps(original))
    with pytest.raises(ValueError, match='search_mismatch:task_id'):
        qualify_f1_artifacts(root, row, report)
