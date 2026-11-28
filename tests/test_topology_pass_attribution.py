"""Offline runner/report attribution regressions, never real DDS evidence.

Reuse authored raw identity/sample fixtures. Ingestion and formal qualification
are modeled boundaries; canonical record/input/raw validators and assessment,
registry/runner, persistence and candidate ledger remain real. No fixture earns
natural candidate or required maturity credit.
"""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign import runner, replay
from r2f.campaign import topology_candidate_assessment as assessment
from r2f.core import ExecutionRecord, canonical_hash
from r2f.modules.s1.topology_oracles import evaluate_topology
from r2f.modules.s1.topology_input import TOPOLOGY_FACT_PRODUCER
from r2f.reports import run_report
from test_topology_candidate_identity import row
from test_topology_conformance_signature import model, read, rewrite
from test_topology_cycle import SOURCE, profile, run_cycle
from test_topology_nohit_terminal import receipts


@pytest.fixture
def normal_report(row, tmp_path, monkeypatch):
    """Submit one real generated input; model only its execution observations."""
    model(row, kind='match')  # Authored complete send/receive chain, no gap.
    pair = row['typed_facts']['pairs'][0]
    pair.update(compatibility='compatible', delivery_closure='closed')
    pair['proofs']['compatibility']['value'] = 'compatible'
    pair['proofs']['delivery_closure']['value'] = 'closed'
    row['typed_facts'].update(target_id=assessment.TARGET, preconditions_reached=True,
                             unknown_reasons=[], window={'complete': True},
                             producer_id=TOPOLOGY_FACT_PRODUCER, fact_values={}, pairs=[pair])

    selected = {}
    real_qualification = replay._qualification

    def forbid_ros(*args, **kwargs):
        raise AssertionError('pass-attribution contracts must not prepare ROS')
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare', forbid_ros)

    def runtime(config, directory):
        observation = deepcopy(row)
        events = read(observation)
        for event in events:
            event.update(run_id=config['run_id'], domain_id=config['domain_id'],
                         test_fixture='offline-pass-attribution-no-DDS')
            if 'payload_run_id' in event:
                event['payload_run_id'] = config['run_id']
        observation['raw_path'] = str(Path(directory) / 'authored.jsonl')
        rewrite(observation, events)
        observation['typed_facts']['program_sha256'] = canonical_hash(config['topology_program'])
        receipt = dict(planned=True, submitted=True, carrier_admitted=True,
                       configured=True, applied=True, hit=True, effect_observed=True)
        return dict(real_execution=True, runtime_status='completed', status='executed',
                    evidence_origin='offline_modeled_execution_boundary',
                    mutation_receipt=receipt, events=events,
                    raw_path=observation['raw_path'], raw_sha256=observation['raw_sha256'],
                    modeled_facts=observation['typed_facts'],
                    cleanup_receipt={'closed': True, 'survivors': [], 'errors': []})

    def ingest(scenario, config, result, capsule, **kwargs):
        # This boundary double returns a canonical, raw-bound authored record.
        # It does not certify that the fact provider or any worker ran ROS.
        facts = deepcopy(result['modeled_facts'])
        damage = selected.get('damage')
        if damage == 'unknown':
            facts['pairs'][0]['compatibility'] = 'unknown'
        elif damage == 'sample_unknown':
            facts['unknown_reasons_sample_identity'] = ['unresolved_fixture_sample']
        elif damage == 'window_incomplete':
            facts.update(observation_complete=False, window={'complete': False})
        elif damage == 'window_malformed':
            facts['window'] = 'incomplete'
        elif damage == 'unknown_reason':
            facts['unknown_reasons'] = ['unresolved_fixture_observation']
        elif damage == 'tool_error':
            facts['tool_error'] = True
        elif damage == 'control':
            facts['control_excluded'] = True
        elif damage == 'fixture':
            facts['fixture'] = True
        elif damage == 'process_unhealthy':
            facts['process_healthy'] = False
        elif damage == 'typed_execution_unqualified':
            facts['candidate_eligible'] = False
        elif damage == 'raw_proof':
            facts['pairs'][0]['proofs']['observed_match']['refs'][0]['sha256'] = '0' * 64
        oracle = evaluate_topology(scenario, facts)
        record = ExecutionRecord(config['run_id'], scenario)
        record.advance('expressible', adapter_accepted=True)
        record.advance('planned')
        record.advance('precondition_executed', precondition_receipts=receipts(scenario))
        record.advance('reached')
        record.advance('mutation_applied', mutation_receipt=result['mutation_receipt'])
        record.advance('effect_observed', typed_facts=facts,
                       observed_effect={'effects': oracle.get('observed_effects', []),
                                        'forbidden_residuals': oracle['forbidden_residuals']},
                       raw_path=result['raw_path'], raw_sha256=result['raw_sha256'])
        record.advance('judged', oracle_verdict=oracle['verdict'], rule_id=oracle['rule_id'])
        execution = dict(record.to_dict(), actual_run=True, execution_source='ros2_public_api',
                         runtime_status='completed', oracle=oracle, domain_id=config['domain_id'],
                         source_manifest=deepcopy(config['source_manifest']),
                         effective_input_sha256=config['effective_input_sha256'],
                         input_identity_schema=config['input_identity_schema'],
                         final_input=deepcopy(config['final_input']),
                         input_layer_receipts=deepcopy(config['input_layer_receipts']),
                         test_fixture='offline-pass-attribution-no-DDS')
        if damage == 'tool_error':
            execution.update(tool_error=True, runtime_status='tool_error')
        elif damage == 'control':
            execution['control_kind'] = 'authored_harness_control'
        elif damage == 'source':
            execution['source_manifest']['source_frozen'] = False
        elif damage == 'missing_provider':
            execution['oracle'].pop('candidate_assessment')
        elif damage == 'provider_binding':
            execution['oracle']['candidate_assessment']['binding']['facts_sha256'] = '0' * 64
        return assessment.ingest_assessment(scenario, config, capsule, execution)

    monkeypatch.setattr(runner, 'ingest_execution', ingest)
    monkeypatch.setattr(run_report, 'ingest_execution', ingest)
    monkeypatch.setattr(run_report, 'source_manifest', lambda *_: deepcopy(SOURCE))

    def submit(*, damage=None, formal='modeled', boundary='host'):
        selected['damage'] = damage
        if formal == 'real':
            monkeypatch.setattr(replay, '_qualification', real_qualification)
        else:
            monkeypatch.setattr(replay, '_qualification', lambda *a, **kw:
                dict(real_ros_execution=formal != 'unqualified',
                     qualification_errors=['authored_formal_failure'] if formal == 'errors' else [],
                     test_fixture='modeled-formal-boundary-no-DDS'))
        if boundary == 'unqualified':
            monkeypatch.setattr(runner, 'execution_boundary_qualification', lambda *a, **kw:
                dict(status='invalid', eligible=False, reason_code='authored_boundary_unqualified', errors=[]))
        root = tmp_path / 'normal'
        doc = profile(1)
        doc['budget']['max_inputs'] = 1
        result = run_cycle(root, doc, runtime=runtime)
        saved = json.loads((root / 'report.json').read_text())
        assert saved['rows'] == result['rows']
        return root, saved
    return submit


def test_healthy_pass_is_reported_as_oracle_pass_without_candidate_credit(normal_report):
    root, report = normal_report()
    execution, = report['rows']
    assert execution['execution_stage'] == 'judged', execution
    assert execution['oracle_verdict'] == 'pass', execution.get('oracle')
    assert execution['typed_facts']['observation_complete'] is True
    assert execution['typed_facts']['window']['complete'] is True
    assert execution['typed_facts']['unknown_reasons'] == []
    assert (execution['triage_disposition'], execution['reason_code']) == ('pass', 'oracle_pass')
    assert execution['candidate_eligible'] is False
    assert execution['candidate_assessment']['final']['qualified'] is False
    ledger = json.loads((root / 'candidate_ledger.json').read_text())
    assert ledger['candidates'] == []
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0
    capsule = json.loads(Path(execution['replay_path']).read_text())
    assert capsule['candidate_assessment_attachment']['projection']['triage_disposition'] == 'pass'

    audited = run_report.audit_exploratory_report(root / 'report.json')
    assert audited['evidence_valid'], audited['errors']


@pytest.mark.parametrize('damage', [
    'unknown', 'sample_unknown', 'window_incomplete', 'window_malformed',
    'unknown_reason', 'tool_error', 'control', 'fixture', 'process_unhealthy',
    'typed_execution_unqualified', 'raw_proof', 'source', 'missing_provider', 'provider_binding',
])
def test_incomplete_or_excluded_observation_never_becomes_pass_or_candidate(normal_report, damage):
    root, report = normal_report(damage=damage)
    execution, = report['rows']
    assert execution['triage_disposition'] in {'deferred', 'rejected'}
    assert execution['reason_code'] != 'oracle_pass'
    assert execution['candidate_eligible'] is False
    if damage in {'unknown', 'sample_unknown', 'tool_error'}:
        assert execution['oracle_verdict'] == 'unknown'
    ledger = json.loads((root / 'candidate_ledger.json').read_text())
    assert ledger['candidates'] == []
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0


@pytest.mark.parametrize('formal,boundary', [
    ('unqualified', 'host'), ('errors', 'host'), ('modeled', 'unqualified'), ('real', 'host'),
])
def test_healthy_oracle_cannot_waive_formal_execution_boundary(normal_report, formal, boundary):
    root, report = normal_report(formal=formal, boundary=boundary)
    execution, = report['rows']
    assert execution['oracle_verdict'] == 'pass'
    assert (execution['triage_disposition'], execution['reason_code']) == ('deferred', 'window_incomplete')
    assert execution['candidate_eligible'] is False
    assert execution['candidate_assessment']['final']['qualified'] is not True
    ledger = json.loads((root / 'candidate_ledger.json').read_text())
    assert ledger['candidates'] == []


def test_offline_nohit_remains_unqualified_and_auditable(tmp_path, monkeypatch):
    from test_topology_cycle import offline_runtime
    root = tmp_path / 'nohit'
    doc = profile(1)
    doc['budget']['max_inputs'] = 1
    report = run_cycle(root, doc, runtime=offline_runtime([]))
    execution, = report['rows']
    assert execution['mutation_receipt']['hit'] is False
    assert execution['candidate_eligible'] is False
    assert execution['triage_disposition'] != 'pass'
    assert execution['execution_qualification']['real_ros_execution'] is False
    ledger = json.loads((root / 'candidate_ledger.json').read_text())
    assert ledger['candidates'] == []
    monkeypatch.setattr(run_report, 'source_manifest', lambda *_: deepcopy(SOURCE))
    audited = run_report.audit_exploratory_report(root / 'report.json')
    assert audited['evidence_valid'], audited['errors']


def test_backend_publish_timeout_is_tool_failure_without_candidate_credit(tmp_path):
    from r2f.runtime.ros2_pair import run_pair
    from test_topology_delivery_windows import WindowFixture

    def runtime(config, directory):
        return run_pair(config, directory, topology_backend=WindowFixture(fail_publish=True))

    root = tmp_path / 'timeout'
    doc = profile(1)
    doc['budget']['max_inputs'] = 1
    report = run_cycle(root, doc, runtime=runtime)
    execution, = report['rows']
    events = [json.loads(line) for line in Path(execution['raw_path']).read_text().splitlines()]
    assert any(event.get('code') == 'publish_batch_timeout' and event.get('disposition') == 'tool_error'
               for event in events)
    assert execution['execution_stage'] == 'aborted'
    assert execution['oracle_verdict'] == 'unknown'
    assert execution['triage_disposition'] != 'pass'
    assert execution['candidate_eligible'] is False
    ledger = json.loads((root / 'candidate_ledger.json').read_text())
    assert ledger['candidates'] == []
