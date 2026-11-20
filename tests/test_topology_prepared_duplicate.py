"""Offline duplicate continuation: no runtime, namespace, or live credit."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock,patch
import pytest
from r2f.campaign import runner
from r2f.campaign.checkpoint import make_checkpoint_hook,load_checkpoint
from r2f.core.evidence import canonical_hash
from r2f.reports import run_report as reports
from test_model_search_topology import narrowed,fresh
from test_exploratory_checkpoint import SOURCE

@pytest.fixture
def duplicate(tmp_path):
    doc=narrowed(context={'boundary_class':'one'},seed='topology.writer_only')
    doc['search'].update(samples_per_class=2,max_depth=1)
    first=tmp_path/'first'; second=tmp_path/'second'; runtime=Mock(side_effect=AssertionError('must never execute'))
    with patch.object(runner,'source_manifest',return_value=deepcopy(SOURCE)), \
            patch.object(runner,'runtime_for_boundary',return_value=runtime):
        state=fresh(doc);hook=make_checkpoint_hook(first,profile=doc,cycle_id='topology-authored',source_manifest=SOURCE)
        runner.run_exploratory_cycle(first,**state,profile=doc,max_inputs=1,checkpoint_hook=hook,execute=False,runtime=runtime,run_id='dup-first',runtime_boundary='isolated_netns')
        state=load_checkpoint(first,profile=doc,cycle_id='topology-authored',source_manifest=SOURCE,output_dir=second)
        hook=make_checkpoint_hook(second,profile=doc,cycle_id='topology-authored',source_manifest=SOURCE,continuation=state['continuation'])
        cycle=runner.run_exploratory_cycle(second,**state,profile=doc,max_inputs=2,checkpoint_hook=hook,execute=True,runtime=runtime,run_id='dup-second',runtime_boundary='isolated_netns')
    runtime.assert_not_called()
    report=json.loads((second/'report.json').read_text());assert len(report['rows'])==1
    return second,cycle,report

def test_model_duplicate_report_has_a_separate_prepared_contract(duplicate):
    root,cycle,report=duplicate; row=report['rows'][0]
    assert row['reason_code']=='model_duplicate_content'
    assert row['oracle_verdict']=='not_applicable'
    assert cycle['abort'] is None
    result=reports.audit_exploratory_report(root/'report.json')
    assert result['evidence_valid'],result['errors']
    assert result['coverage_entries']==0 and not result['all_oracles_pass'] and not result['sealed']
    assert row['actual_run'] is False and all(row[k] is False for k in ('candidate_eligible','feedback_eligible','seal_eligible'))
    assert {p.name for p in (root/'cases'/row['cell_id']).iterdir()}=={'row.json','mutation_plan.json','runtime_config.json','capsule.json'}

@pytest.mark.parametrize('damage',['raw','runtime_dir','submission','plan','task','saved_row','candidate','effect'])
def test_prepared_duplicate_cannot_launder_artifacts_or_model_claims(duplicate,damage):
    root,_,report=duplicate;row=report['rows'][0];case=root/'cases'/row['cell_id']
    if damage=='raw': row['raw_path']='forged.jsonl'
    if damage=='runtime_dir': (case/'runtime').mkdir()
    if damage=='submission': (case/'submission_intent.json').write_text('{}')
    if damage=='plan': row['mutation_plan']['rng_seed']+=1
    if damage=='task': row['input_search']['task_id']='f'*64
    if damage=='saved_row': (case/'row.json').write_text('{}')
    if damage=='candidate':row['candidate_eligible']=True
    if damage=='effect':row['mutation_receipt']['effect_observed']=True
    errors=[]
    assert not reports._verify_exploratory_row(root,row,report,errors)
    assert errors


def test_topology_resume_report_preserves_and_binds_segment_counters(duplicate):
    root, cycle, report = duplicate
    assert report['budget']['selected'] == 2
    assert report['cycle_budget']['selected'] == 1
    assert report['row_sequences'] == cycle['row_sequences']
    assert report['resume_origin'] == cycle['resume_origin']
    assert json.loads((root / report['resume_origin']['path']).read_text())['checkpoint_sha256'] == report['resume_origin']['checkpoint_sha256']
    errors = []
    reports._audit_exploratory_budget(report, report['rows'], errors, root=root)
    assert errors == []


@pytest.mark.parametrize('damage', ['origin', 'local_count', 'local_bool', 'sequence', 'missing_sequence', 'prefix'])
def test_topology_resume_cannot_hide_inherited_or_tampered_state(duplicate, damage):
    root, _, report = duplicate
    if damage == 'origin':
        (root / report['resume_origin']['path']).unlink()
    elif damage == 'local_count':
        report['cycle_budget']['planned'] += 1
    elif damage == 'local_bool':
        report['cycle_budget']['selected'] = True
    elif damage == 'sequence':
        report['row_sequences'][0]['selection_sequence'] = 0
    elif damage == 'missing_sequence':
        report['row_sequences'] = []
    elif damage == 'prefix':
        path = root / report['resume_origin']['path']
        origin = json.loads(path.read_text())
        origin['state']['scheduler']['selections'][0]['selection_sha256'] = 'e' * 64
        origin['checkpoint_sha256'] = canonical_hash({k: v for k, v in origin.items() if k != 'checkpoint_sha256'})
        path.write_text(json.dumps(origin))
        report['resume_origin']['checkpoint_sha256'] = origin['checkpoint_sha256']
    errors = []
    reports._audit_exploratory_budget(report, report['rows'], errors, root=root)
    assert errors


def test_duplicate_generation_never_appears_as_an_execution_exclusion(duplicate):
    root, _, report = duplicate
    ledger_path = root / report['candidate_ledger_path']
    ledger = json.loads(ledger_path.read_text())
    assert ledger.get('execution_exclusions', []) == []
    ledger['execution_exclusions'] = [{'cell_id': report['rows'][0]['cell_id']}]
    ledger_path.write_text(json.dumps(ledger))
    report['candidate_ledger_sha256'] = canonical_hash(ledger)
    report['report_sha256'] = reports.exploratory_report_digest(report)
    (root / 'report.json').write_text(json.dumps(report))
    result = reports.audit_exploratory_report(root / 'report.json')
    assert not result['evidence_valid']
    assert 'topology_ledger_execution_exclusions_mismatch' in result['errors']
