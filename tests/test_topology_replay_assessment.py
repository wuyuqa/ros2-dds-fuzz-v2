"""Pure offline wiring/models, NOT real ROS, formal qualification or candidates.

Canonical/schema/formal boundaries are explicitly modeled by the imported
fixture. Raw identity and typed anomaly signature checks remain real. Separate
negative tests run the real qualifier and injected/offline paths.
"""
from copy import deepcopy
from types import SimpleNamespace
import json

import pytest
from test_topology_candidate_plumbing import setup, publish
from test_topology_candidate_identity import row
from r2f.campaign import topology_candidate_assessment as assessment
from r2f.campaign import replay
from r2f.core import replay as core
from r2f.core.evidence import canonical_hash


@pytest.fixture
def pending(setup, monkeypatch):
    publish(setup)
    scenario, config, capsule, execution, calls = setup
    source = capsule.input['source_manifest']
    source['source_repo'] = '/modeled-source-not-a-real-checkout'
    config['source_manifest'] = deepcopy(source)
    execution['source_manifest'] = deepcopy(source)
    capsule.input.update(target=deepcopy(execution['target']), target_id=assessment.TARGET, scenario={'modeled': True},
                         mutation_plan={'modeled': True}, origin_domain=80,
                         input_identity_schema='r2f.input-identity/v2')
    config.update(scenario=deepcopy(capsule.input['scenario']),
                  mutation_plan=deepcopy(capsule.input['mutation_plan']),
                  effective_input=deepcopy(capsule.input['effective_input']))
    for key in ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed'):
        config[key] = execution.get(key)
    capsule.execution_history = []
    capsule.replay_attempts = []
    scenario.to_dict = lambda: {'modeled': True}
    monkeypatch.setattr(replay.Scenario, 'from_dict', lambda data: deepcopy(scenario))
    monkeypatch.setattr(core, 'validate_replay_input', lambda *a, **kw: True)
    monkeypatch.setattr(core, '_v2_execution_input', lambda *a, **kw: True)
    monkeypatch.setattr(replay, '_registered_target', lambda c: (scenario.target, object(), True))
    monkeypatch.setattr(replay, 'source_manifest', lambda repo: deepcopy(source))
    assessment.ingest_assessment(scenario, config, capsule, execution)
    capsule.execution = deepcopy(execution)
    assert capsule.execution['candidate_eligible'] is None
    runtime = {'modeled': True}
    # Mock a fresh independent ingestion, NOT the saved assessment/attachment.
    def ingest(*args, **kwargs):
        return deepcopy(execution)
    monkeypatch.setattr(replay, 'ingest_execution', ingest)
    monkeypatch.setattr(replay, '_qualification', lambda *a, **kw: {
        'real_ros_execution': True, 'real_runtime_attempt': True, 'qualification_errors': []})
    return scenario, config, capsule, execution, runtime


def view(pending, *, execution=None, origin=replay.DEFAULT_RUNTIME):
    _, config, capsule, row, runtime = pending
    return replay._topology_comparison_view(capsule, row if execution is None else execution,
        config=config, result=runtime, origin=origin, output_root='/modeled-runtime')


def test_finalized_fail_is_comparable_without_rewriting_pending_execution(pending):
    scenario, config, capsule, execution, _ = pending
    original = deepcopy(capsule.execution)
    assessment.finalize_assessment(scenario, config, capsule, execution,
        boundary={'status': 'qualified', 'eligible': True},
        qualification={'real_ros_execution': True, 'qualification_errors': []})
    assert execution['candidate_eligible'] is True
    assert core.conformance_signature(original) is None  # actual old failure
    compared = view(pending, execution=original)
    assert core.conformance_signature(compared) is not None
    assert capsule.execution == original and capsule.execution_history == []
    assert compared['comparison_binding']['execution_sha256'] == canonical_hash(original)
    assert compared['comparison_binding']['input_sha256'] == canonical_hash(capsule.input)


def test_two_sides_use_core_comparison_hook_and_keep_history(pending, monkeypatch):
    _, _, capsule, execution, _ = pending
    original = deepcopy(capsule.execution)
    # This deliberately models only core's already-tested canonical record gate.
    def record(data):
        capsule.execution_history.append(deepcopy(data))
    capsule.record_execution = record
    def attempt(status, **kwargs):
        result = dict(status=status, **deepcopy(kwargs))
        capsule.replay_attempts.append(result)
        return result
    capsule.record_attempt = attempt
    observed = []
    def comparison(row, *, original):
        observed.append(original)
        return view(pending, execution=row)
    fresh = deepcopy(execution)
    fresh.update(domain=81, domain_id=81)
    result = core.rerun_conformance(capsule, lambda *args: deepcopy(fresh), domain=81,
                                    plan_aware=True, comparison_view=comparison)
    assert result['status'] == 'pass', result
    assert result['comparison_matches_original'] is True
    assert observed == [True, False]
    assert capsule.execution == original
    assert capsule.execution_history == [fresh]
    assert result['execution_metadata']['candidate_eligible'] is None


@pytest.mark.parametrize('damage', ['unknown', 'nohit', 'fixture', 'missing_raw', 'boundary',
    'source', 'config', 'raw_hash', 'facts', 'independent_qualification', 'injected', 'eligibility_label'])
def test_unqualified_or_changed_evidence_cannot_get_fail_signature(pending, monkeypatch, damage):
    _, config, capsule, row, _ = pending
    if damage == 'unknown': row['oracle_verdict'] = 'unknown'
    elif damage == 'nohit': row['input_terminal'] = {'forged': True}
    elif damage == 'fixture': row['typed_facts']['fixture'] = True
    elif damage == 'missing_raw': row['raw_path'] = '/nonexistent/raw.jsonl'
    elif damage == 'raw_hash': row['raw_sha256'] = '0' * 64
    elif damage == 'boundary':
        monkeypatch.setattr('r2f.reports.run_report.execution_boundary_qualification',
                            lambda *a, **kw: {'status': 'invalid', 'eligible': False})
    elif damage == 'source': config['source_manifest']['head'] = 'c' * 40
    elif damage == 'config': config['effective_input_sha256'] = '0' * 64
    elif damage == 'facts': row['typed_facts']['observation_complete'] = False
    elif damage == 'independent_qualification':
        monkeypatch.setattr(replay, '_qualification', lambda *a, **kw: {
            'real_ros_execution': False, 'qualification_errors': ['execution_boundary_artifact_audit_failed']})
    elif damage == 'eligibility_label':
        row['candidate_eligible'] = True
        row['typed_facts']['candidate_eligible'] = False
    try:
        compared = view(pending, origin='injected_runtime' if damage == 'injected' else replay.DEFAULT_RUNTIME)
    except (ValueError, OSError):
        return
    assert core.conformance_signature(compared) is None


def test_saved_attachment_and_eligibility_are_not_authority(pending):
    _, _, capsule, row, _ = pending
    row['candidate_eligible'] = False
    row['candidate_assessment'] = {'forged': 'blocked'}
    capsule.candidate_assessment_attachment = {'projection': {'candidate_eligible': True}}
    compared = view(pending)
    assert core.conformance_signature(compared) is not None
    assert compared['candidate_assessment']['final']['qualified'] is True
    assert row['candidate_eligible'] is False


def test_real_qualifier_rejects_modeled_raw_even_when_labels_claim_success(pending, monkeypatch):
    monkeypatch.undo()
    _, config, capsule, row, runtime = pending
    # Model source/registry/ingest only, leave the actual formal qualifier intact.
    monkeypatch.setattr(replay, 'source_manifest', lambda repo: deepcopy(config['source_manifest']))
    monkeypatch.setattr(core, 'validate_replay_input', lambda *a, **kw: True)
    monkeypatch.setattr(core, '_v2_execution_input', lambda *a, **kw: True)
    monkeypatch.setattr(replay.Scenario, 'from_dict', lambda data: pending[0])
    monkeypatch.setattr(replay, '_registered_target', lambda c: (pending[0].target, object(), True))
    monkeypatch.setattr(replay, 'ingest_execution', lambda *a, **kw: deepcopy(row))
    with pytest.raises((ValueError, KeyError, TypeError)):
        view(pending)


def test_original_layout_and_new_context_both_consumed(pending, monkeypatch, tmp_path):
    _, config, capsule, execution, runtime = pending
    case = tmp_path / 'campaign' / 'cases' / 'case-1'
    (case / 'runtime').mkdir(parents=True)
    (case / 'runtime_config.json').write_text(json.dumps(config))
    (case / 'runtime/runtime_result.json').write_text(json.dumps(runtime))
    (case.parents[1] / 'report.json').write_text(json.dumps({'runtime_formal': True, 'runtime_boundary': 'host'}))
    for original in (True, False):
        compared = replay._replay_comparison_view(capsule, execution, original=original,
            capsule_path=case / 'capsule.json', origin=replay.DEFAULT_RUNTIME,
            context=dict(config=config, runtime_result=runtime, output_root=case / 'runtime'))
        assert core.conformance_signature(compared) is not None
    assert capsule.execution['candidate_eligible'] is None


@pytest.mark.parametrize('damage', ['tool_error', 'no_effect', 'dropped', 'incomplete'])
def test_semantic_and_observer_denials_remain_closed(pending, damage):
    row = pending[3]
    if damage == 'tool_error': row['tool_error'] = True
    elif damage == 'no_effect': row['mutation_receipt']['effect_observed'] = False
    elif damage == 'dropped': row['observer_dropped'] = True
    else: row['observer_complete'] = False
    try:
        compared = view(pending)
    except ValueError:
        return
    assert core.conformance_signature(compared) is None


def test_independent_ingest_rejects_rewritten_original_not_just_attachment(pending, monkeypatch):
    original_produced = deepcopy(pending[3])
    monkeypatch.setattr(replay, 'ingest_execution', lambda *a, **kw: deepcopy(original_produced))
    pending[3]['oracle']['reason_codes'] = ['rewritten']
    with pytest.raises(ValueError, match='recomputed_ingest_differs:oracle'):
        view(pending)


def test_changed_snapshot_cannot_be_hidden_by_same_eligibility(pending, monkeypatch):
    monkeypatch.setattr(core, '_v2_execution_input', lambda *a, **kw: (_ for _ in ()).throw(
        ValueError('sealed input mismatch')))
    with pytest.raises(ValueError, match='sealed input mismatch'):
        view(pending)


def test_comparison_qualification_failure_never_turns_into_core_pass(pending):
    _, _, capsule, execution, _ = pending
    original = deepcopy(capsule.execution)
    fresh = deepcopy(execution)
    fresh.update(domain=81, domain_id=81)
    capsule.record_execution = lambda data: capsule.execution_history.append(deepcopy(data))
    capsule.record_attempt = lambda status, **kw: dict(status=status, **kw)
    def invalid(row, *, original):
        raise ValueError('independent boundary audit failed')
    result = core.rerun_conformance(capsule, lambda *a: fresh, domain=81,
                                    plan_aware=True, comparison_view=invalid)
    assert result['status'] == 'fail_artifact'
    assert result['comparison_matches_original'] is False
    assert capsule.execution == original and capsule.execution_history == [fresh]


def test_actual_capsule_roundtrip_ignores_attachment_but_revalidates_comparison(pending, monkeypatch):
    _, config, capsule, execution, runtime = pending
    # Core input/state validators modeled here exactly as in fixture setup;
    # actual capsule serialization/loading/history behavior is not replaced.
    monkeypatch.setattr(core, 'validate_execution', lambda data: True)
    for key in ('target_id', 'target_revision', 'seed', 'rmw', 'arm', 'repeat',
                'mutation_kind', 'carrier', 'target'):
        capsule.input[key] = deepcopy(execution.get(key))
    original = deepcopy(execution)
    document = {'schema': core.CAPSULE_SCHEMA, 'input': deepcopy(capsule.input),
                'execution': original, 'execution_history': [], 'replay_attempts': [],
                'candidate_assessment_attachment': {'projection': {'candidate_eligible': True},
                    'execution_snapshot_sha256': '0' * 64}}
    loaded = core.ReplayCapsule.from_dict(json.loads(json.dumps(document)))
    assert loaded.execution == original and loaded.execution['candidate_eligible'] is None
    assert core.conformance_signature(loaded.execution) is None
    compared = replay._topology_comparison_view(loaded, loaded.execution, config=config,
        result=runtime, origin=replay.DEFAULT_RUNTIME, output_root='/modeled-runtime')
    assert core.conformance_signature(compared) is not None
    assert loaded.execution == original and loaded.execution_history == []
    assert 'candidate_assessment_attachment' not in loaded.to_dict()


def test_campaign_replay_and_readonly_auditor_consume_both_views(pending, monkeypatch, tmp_path):
    """Actual orchestrator wiring, with explicit modeled canonical/formal gates.

    This is not an actual execution or an artifact-audit qualification proof.
    The captured independently reconstructed views/real signature comparison
    and immutable original bytes are the contract being tested.
    """
    from r2f.campaign import runner
    from unittest.mock import Mock
    _, config, capsule, execution, result = pending
    config = deepcopy(config)
    config['topology_program'] = deepcopy(execution['final_input']['topology_program'])
    result.update(runtime_status='completed', real_execution=True)
    case = tmp_path / 'original' / 'cases' / 'first'
    (case / 'runtime').mkdir(parents=True)
    source_path = case / 'capsule.json'
    source_path.write_text(json.dumps({'modeled_capsule': True}))
    (case / 'runtime_config.json').write_text(json.dumps(config))
    (case / 'runtime/runtime_result.json').write_text(json.dumps(result))
    (case.parents[1] / 'report.json').write_text(json.dumps({'runtime_formal': True, 'runtime_boundary': 'host'}))
    original_bytes = source_path.read_bytes()
    source = deepcopy(config['source_manifest'])
    original = deepcopy(capsule.execution)
    def record(data):
        capsule.execution_history.append(deepcopy(data))
    def record_attempt(status, **kw):
        attempt = dict(status=status, failure_reason=kw.pop('failure_reason', ''),
                       timestamp='modeled', attempt_id='attempt-1', **deepcopy(kw))
        capsule.replay_attempts.append(attempt)
        return deepcopy(attempt)
    capsule.record_execution = record
    capsule.record_attempt = record_attempt
    capsule.non_replayable = None
    capsule.to_dict = lambda: dict(schema=core.CAPSULE_SCHEMA, input=deepcopy(capsule.input),
        execution=deepcopy(capsule.execution), execution_history=deepcopy(capsule.execution_history),
        replay_attempts=deepcopy(capsule.replay_attempts), non_replayable=None)
    monkeypatch.setattr(replay.ReplayCapsule, 'from_dict', lambda doc: capsule)
    monkeypatch.setattr(replay, '_check_source', lambda *a: None)
    monkeypatch.setattr(replay, '_replay_config', lambda *a: deepcopy(config))
    monkeypatch.setattr(replay, '_bind_execution_input', lambda row, *a: row)
    monkeypatch.setattr(runner, '_runtime_execution_config', lambda config, **kw: deepcopy(config))
    runtime = Mock(return_value=deepcopy(result))
    monkeypatch.setattr(runner, '_resolve_execution_runtime', lambda *a: (runtime, True))
    carrier = SimpleNamespace(accepts=lambda s: True,
        apply=lambda scenario, effective, execution: execution['runtime'](execution['config'], execution['output_dir']))
    monkeypatch.setattr(replay, '_registered_target', lambda c: (pending[0].target,
        SimpleNamespace(carrier=carrier), True))
    seen = []
    actual = replay._replay_comparison_view
    def comparison(*args, **kwargs):
        seen.append(kwargs['original'])
        return actual(*args, **kwargs)
    monkeypatch.setattr(replay, '_replay_comparison_view', comparison)
    output = replay.replay_cell(source_path, output_dir=tmp_path / 'rerun', domain_id=81, runtime=runtime)
    assert output['status'] == 'pass', output
    assert output['comparison_matches_original'] is True
    assert seen == [True, False]
    assert runtime.call_count == 1
    assert capsule.execution == original
    assert source_path.read_bytes() == original_bytes
    saved = json.loads((tmp_path / 'rerun/attempt.json').read_text())
    assert saved['original_signature'] == saved['replay_signature']
    assert saved['execution_metadata']['candidate_eligible'] is None

    # Reopen original/derived copies independently, as the auditor does. Schema
    # and submission configuration are modeled; the audit's hash manifests,
    # re-ingest comparison and original/new signature consumption stay real.
    seed_document = deepcopy(capsule.to_dict())
    seed_document.update(execution=deepcopy(original), execution_history=[], replay_attempts=[])
    def load(doc):
        data = deepcopy(seed_document if doc.get('modeled_capsule') else doc)
        obj = SimpleNamespace(**{key: data.get(key) for key in
            ('input', 'execution', 'execution_history', 'replay_attempts', 'non_replayable')})
        obj.to_dict = lambda: deepcopy(dict(schema=core.CAPSULE_SCHEMA,
            input=obj.input, execution=obj.execution, execution_history=obj.execution_history,
            replay_attempts=obj.replay_attempts, non_replayable=obj.non_replayable))
        obj.record_execution = lambda value: obj.execution_history.append(deepcopy(value))
        return obj
    monkeypatch.setattr(replay.ReplayCapsule, 'from_dict', load)
    monkeypatch.setattr(replay, '_check_audit_config', lambda *a: None)
    seen.clear()
    audited = replay.audit_replay(output['report_path'])
    assert audited['valid'], audited
    assert seen == [True, False]
    assert runtime.call_count == 1  # auditor did not invoke it again
    assert source_path.read_bytes() == original_bytes
