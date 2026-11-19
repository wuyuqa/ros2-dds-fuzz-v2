"""SYNTHETIC contract tests. No ROS/DDS, namespaces or real campaign evidence.

The positive continuation tests deliberately inject facts/step and strict-gate
outputs. They test the canonical BFS/row/report plumbing, not live qualification.
Raw/receipt/cleanup validation is real file validation within these fixtures.
"""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.campaign import runtime_adapter as adapter, runner, replay
from r2f.core import ReplayCapsule, Scenario, canonical_hash
from r2f.core.evidence import raw_sha256
from r2f.reports import run_report
from test_topology_cycle import SOURCE, capsule_from_registry
import test_model_search_topology as bfs

RECEIPT = dict(planned=True, submitted=True, carrier_admitted=True,
               configured=False, applied=False, hit=False, effect_observed=False)
FACTS = dict(diagnostic_valid=True, execution_evidence=True,
             source_provenance_qualified=True, environment_healthy=True,
             process_healthy=True, preconditions_reached=True,
             endpoint_identity_complete=True, fixture=False)
ORACLE = dict(rule_id='synthetic-diagnostic-rule', rule_role='primary', verdict='fail',
              reason_codes=['synthetic_only'], forbidden_residuals=['diagnostic_only'], closure={})


def receipts(scenario, *_args, **_kwargs):
    state = {c.fact: c.expected for c in scenario.target.state_preconditions}
    rows = []
    for step in scenario.state_steps:
        rows.append(dict(step_id=step.step_id, action=step.action,
            before=deepcopy(state), after=deepcopy(state),
            requires={c.condition_id: c.evaluate(state) for c in step.requires},
            status='executed', started_at='synthetic-start', finished_at='synthetic-end',
            result={'synthetic_only': True}))
    return rows


def sync(runtime):
    path = Path(runtime['raw_path'])
    path.write_text(''.join(json.dumps(e) + '\n' for e in runtime['events']))
    runtime['raw_sha256'] = raw_sha256(path)
    (path.parent / 'runtime_result.json').write_text(json.dumps(runtime))


def synthetic(config, directory):
    """Fabricated flags, not a real runtime; never exported as execution proof."""
    root = Path(directory); root.mkdir(parents=True, exist_ok=True)
    cleanup = dict(closed=True, survivors=[], errors=[])
    events = [dict(event='topology_cleanup', receipt=deepcopy(cleanup)),
              dict(event='runtime_end', mutation_receipt=deepcopy(RECEIPT))]
    for i, e in enumerate(events):
        e.update(event_index=i, run_id=config['run_id'], domain_id=config['domain_id'],
                 synthetic_contract_only=True)
    runtime = dict(real_execution=True, runtime_status='completed', status='executed',
        mutation_receipt=deepcopy(RECEIPT), events=events,
        raw_path=str(root / 'synthetic.jsonl'), cleanup_receipt=cleanup,
        execution_metadata={'synthetic_contract_only': True}, evidence_origin='offline_fixture')
    sync(runtime)
    return runtime


def strict_gate(*_args, **_kwargs):
    # An injected gate result, NEVER proof of ROS/kernel qualification.
    return dict(real_ros_execution=False, real_runtime_attempt=True,
                qualification_errors=['real_judged_execution_missing'],
                boundary_audit={'valid': True, 'synthetic_contract_only': True})


def injected_facts(_services, evidence):
    # Deliberately simulated producer output. Real qualification is separately
    # shown to reject this file-only fixture below.
    runtime = evidence['runtime']; facts = deepcopy(FACTS)
    facts['application_audit'] = dict(status='incomplete', canonical_receipt_matches=True,
        mutation_receipt=deepcopy(runtime['mutation_receipt']), execution_evidence=True,
        fixture=False, raw_path=runtime['raw_path'], raw_sha256=runtime['raw_sha256'])
    return facts, None


@pytest.fixture
def injected():
    with patch.object(adapter, '_produce_facts', side_effect=injected_facts), \
         patch.object(adapter, '_evaluate_oracle', return_value=(deepcopy(ORACLE), None)), \
         patch.object(adapter, 'canonical_receipts', side_effect=receipts), \
         patch.object(run_report, 'source_manifest', return_value=deepcopy(SOURCE)), \
         patch.object(runner, 'source_manifest', return_value=deepcopy(SOURCE)):
        yield


def one(tmp_path):
    _, scenario, _, capsule = capsule_from_registry(1)
    config = replay._replay_config(capsule, 'synthetic-nohit', 81, SOURCE)
    runtime = synthetic(config, tmp_path / 'runtime')
    row = adapter.ingest_execution(scenario, config, runtime, capsule)
    return config, runtime, row, scenario, capsule


def test_versioned_reached_terminal_retains_original_receipt_and_diagnostics(tmp_path, injected):
    config, result, row, _, _ = one(tmp_path)
    assert row['execution_stage'] == 'reached', row
    assert row['mutation_receipt'] == RECEIPT
    assert row['diagnostic_oracle'] == ORACLE and row['oracle_verdict'] == 'not_applicable'
    assert row['oracle']['forbidden_residuals'] == []
    assert not any(x['execution_stage'] in {'mutation_applied', 'effect_observed', 'judged'} for x in row['stage_history'])
    assert row['input_terminal'] == adapter.topology_nohit_terminal(config, result, row)
    assert run_report.topology_nohit_continuation(config, result, row, strict_gate())
    assert all(row[k] is False for k in ('candidate_eligible', 'feedback_eligible', 'seal_eligible'))


@pytest.mark.parametrize('flags', [
    dict(configured=False, applied=False), dict(configured=True, applied=False),
    dict(configured=True, applied=True)])
def test_all_valid_nohit_prefixes(tmp_path, injected, flags):
    config, runtime, _, scenario, capsule = one(tmp_path)
    runtime['mutation_receipt'].update(flags)
    runtime['events'][-1]['mutation_receipt'] = deepcopy(runtime['mutation_receipt']); sync(runtime)
    row = adapter.ingest_execution(scenario, config, runtime, capsule)
    assert row['execution_stage'] == 'reached', row
    assert row['mutation_receipt'] == runtime['mutation_receipt']
    assert run_report.topology_nohit_continuation(config, runtime, row, strict_gate())


@pytest.mark.parametrize('damage', ['raw', 'receipt', 'cleanup', 'source', 'input', 'terminal_extra',
    'diagnostic', 'stage', 'history', 'candidate', 'health', 'application', 'boundary', 'extra_error', 'no_error'])
def test_terminal_label_never_waives_damaged_evidence(tmp_path, injected, damage):
    config, runtime, row, _, _ = one(tmp_path); q = strict_gate()
    if damage == 'raw': Path(runtime['raw_path']).write_text('tamper\n')
    elif damage == 'receipt': runtime['mutation_receipt']['submitted'] = False
    elif damage == 'cleanup': runtime['cleanup_receipt']['closed'] = False
    elif damage == 'source': config['source_manifest']['head'] = 'c' * 40
    elif damage == 'input': config['effective_input_sha256'] = '0' * 64
    elif damage == 'terminal_extra': row['input_terminal']['label_waiver'] = True
    elif damage == 'diagnostic': row['diagnostic_oracle']['verdict'] = 'pass'
    elif damage == 'stage': row['execution_stage'] = 'judged'
    elif damage == 'history': row['stage_history'].append({'execution_stage': 'judged'})
    elif damage == 'candidate': row['candidate_eligible'] = True
    elif damage == 'health': row['typed_facts']['process_healthy'] = False
    elif damage == 'application': row['typed_facts']['application_audit']['canonical_receipt_matches'] = False
    elif damage == 'boundary':
        config['execution_boundary'] = {'enabled': True}; q['boundary_audit']['valid'] = False
    elif damage == 'extra_error': q['qualification_errors'].append('runtime_raw_missing_or_invalid')
    else: q['qualification_errors'] = []
    with pytest.raises((ValueError, KeyError)):
        run_report.topology_nohit_continuation(config, runtime, row, q)


def run_two(out, *, damage=None):
    doc = bfs.narrowed(); doc['search'].update(max_depth=2, samples_per_class=1)
    state = bfs.fresh(doc); calls = []
    def runtime(config, directory):
        calls.append(deepcopy(config)); result = synthetic(config, directory)
        if damage == 'raw': Path(result['raw_path']).write_text('tamper\n')
        elif damage == 'receipt': result['events'][-1]['mutation_receipt']['configured'] = True; sync(result)
        elif damage == 'cleanup': result['cleanup_receipt']['closed'] = False; sync(result)
        return result
    observed_source = deepcopy(SOURCE)
    if damage == 'source': observed_source['head'] = 'd' * 40
    with patch.object(replay, '_qualification', side_effect=strict_gate), \
         patch.object(run_report, 'source_manifest', return_value=observed_source):
        result = runner.run_exploratory_cycle(out, **state, profile=doc, max_inputs=2,
            runtime=runtime, execute=True, run_id=out.name, domain_id=81)
    return result, calls, state


def test_decisive_two_input_bfs_and_parent_lineage(tmp_path, injected):
    result, calls, state = run_two(tmp_path / 'two')
    assert len(calls) == 2 and result['abort'] is None, result.get('abort')
    assert len(result['rows']) == 2 and result['budget']['actual'] == 2
    first, second = result['rows']
    assert first['execution_qualification']['real_ros_execution'] is False
    assert first['execution_qualification']['qualification_errors'] == ['real_judged_execution_missing']
    assert first['input_search']['status'] == 'completed'
    assert first['content_sha256'] in state['scheduler'].frontier.nodes
    assert second['input_search']['parent_content_sha256'] == first['content_sha256']
    assert not first['corpus_offer']['accepted']
    assert result['budget']['executed'] == 0
    for event in result['feedback']['events']:
        assert event['executed'] and not event['coverage'] and not event['complete']
        assert event['surface_class'] and event['edge_id'] is None
    assert result['feedback']['covered_surfaces'] == []
    assert result['feedback']['edge_counts'] == {}
    assert all(not r['candidate_eligible'] for r in result['rows'])


@pytest.mark.parametrize('damage', ['source', 'raw', 'receipt', 'cleanup'])
def test_damaged_first_input_cannot_continue(tmp_path, injected, damage):
    result, calls, _ = run_two(tmp_path / damage, damage=damage)
    assert len(calls) == 1
    assert result['abort']['reason_code'] == 'execution_boundary_qualification_failed'
    assert not result['feedback']['events'][0]['coverage']


def test_real_strict_gate_rejects_fabricated_fixture_even_with_terminal(tmp_path, injected):
    config, runtime, row, _, _ = one(tmp_path)
    q = replay._qualification('injected_runtime', config, runtime, row, output_root=tmp_path / 'runtime')
    assert not q['real_ros_execution']
    assert 'real_judged_execution_missing' in q['qualification_errors']
    with pytest.raises(ValueError): run_report.topology_nohit_continuation(config, runtime, row, q)


def test_boundary_terminal_remains_ineligible_and_audit_failure_aborts(tmp_path, injected):
    config, runtime, row, _, _ = one(tmp_path)
    config['execution_boundary'] = {'enabled': True}
    with patch.object(replay, '_qualification', side_effect=strict_gate):
        q = run_report.execution_boundary_qualification('isolated_netns', config=config, result=runtime,
            row=row, formal_runtime=True, output_root=tmp_path / 'runtime')
    assert q['status'] == 'terminal_unqualified' and q['continuation_eligible']
    assert not q['eligible'] and q['errors'] == ['real_judged_execution_missing']
    broken = strict_gate(); broken['qualification_errors'].append('execution_boundary_artifact_audit_failed')
    broken['boundary_audit']['valid'] = False
    with patch.object(replay, '_qualification', return_value=broken):
        q = run_report.execution_boundary_qualification('isolated_netns', config=config, result=runtime,
            row=row, formal_runtime=True, output_root=tmp_path / 'runtime')
    assert q['status'] == 'invalid' and not q['continuation_eligible']


def test_report_recomputes_terminal_instead_of_trusting_saved_label(tmp_path, injected):
    root = tmp_path / 'audit'; result, _, _ = run_two(root)
    report = dict(result, formal_runtime=False)
    row = result['rows'][0]
    with patch.object(replay, '_qualification', side_effect=strict_gate):
        qualified = run_report._topology_artifact_qualification(root, row, report)
        assert qualified['continuation_eligible'] and not qualified['real_ros_execution']
        changed = deepcopy(row); changed['diagnostic_oracle']['verdict'] = 'pass'
        changed['input_terminal']['diagnostic_oracle_sha256'] = canonical_hash(changed['diagnostic_oracle'])
        with pytest.raises(ValueError, match='recomputed_ingest_differs'):
            run_report._topology_artifact_qualification(root, changed, report)


@pytest.mark.parametrize('damage', ['receipt', 'raw', 'cleanup', 'source', 'missing_terminal'])
def test_report_nohit_artifact_tamper_not_label_waived(tmp_path, injected, damage):
    root = tmp_path / 'report'; result, _, _ = run_two(root)
    row = deepcopy(result['rows'][0]); report = dict(result, formal_runtime=False)
    result_path = Path(row['runtime_result_path'])
    runtime = json.loads(result_path.read_text())
    if damage == 'receipt':
        row['mutation_receipt']['configured'] = True
        row['input_terminal']['mutation_receipt']['configured'] = True
    elif damage == 'raw': Path(row['raw_path']).write_text('tampered\n')
    elif damage == 'cleanup':
        runtime['cleanup_receipt']['errors'] = ['unclosed worker']
        result_path.write_text(json.dumps(runtime))
    elif damage == 'source':
        config_path = Path(row['runtime_config_path'])
        config = json.loads(config_path.read_text()); config['source_manifest']['head'] = 'd' * 40
        config_path.write_text(json.dumps(config))
    else: row.pop('input_terminal')
    with patch.object(replay, '_qualification', side_effect=strict_gate):
        with pytest.raises((ValueError, KeyError)):
            run_report._topology_artifact_qualification(root, row, report)
