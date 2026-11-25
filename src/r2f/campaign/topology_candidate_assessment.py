"""Scoped topology candidate assessments in the existing ingest/runner chain.

No execution, state machine or attribution shortcut lives here. Provider-known
checks, canonical ingestion and final formal qualification are distinct scopes.
Pending requirements are None, never false denials to be silently ignored later.
"""
from r2f.core._json import snapshot
from r2f.core.candidate import _validate_observation_identity, topology_observation_identity
from r2f.core.evidence import canonical_hash
from r2f.core.execution import validate_execution
from r2f.core.oracle import attribute_responsibility
from r2f.core.observation_status import observation_complete
from r2f.core.replay import (_v2_execution_input, conformance_signature,
                             validate_replay_input)

TARGET = 'discovery.endpoint_topology.fuzz_v1'
RULE = 's1.topology.program_consistency.v1'
SCHEMA = 'r2f.topology-candidate-assessment/v1'
PROVIDER_SCHEMA = 'r2f.topology-provider-candidate-assessment/v1'


def _stage(scope, checks, pending=(), binding=None):
    blockers = sorted(name for name, passed in checks.items() if passed is not True)
    pending = sorted(set(pending))
    return dict(scope=scope, checks=snapshot(checks), blockers=blockers, pending=pending,
                status='blocked' if blockers else 'pending' if pending else 'passed',
                eligible_at_scope=not blockers,
                qualified=False if blockers else None if pending else True,
                binding=snapshot(binding or {}))


def _oracle_semantics(oracle):
    return {k: oracle.get(k) for k in ('rule_id', 'rule_role', 'evaluator_id', 'verdict',
            'reason_codes', 'forbidden_residuals', 'closure', 'diagnostic_only')}


def provider_assessment(scenario, facts, oracle):
    """Publishable provider receipt; formal qualification is ALWAYS pending.

    The oracle owner must explicitly publish this receipt. Ingest recomputes it
    rather than treating the legacy default candidate_eligible=false as a veto
    or promoting it as if it already represented this scoped contract.
    """
    facts = facts if isinstance(facts, dict) else {}
    target = getattr(scenario, 'target', None)
    target_binding = target.to_dict() if target is not None else {'target_id': scenario.target_id}
    checks = {
        'exact_target': scenario.target_id == TARGET and facts.get('target_id') == TARGET,
        'typed_facts_schema': facts.get('schema') == 'r2f.s1-topology-facts/v1',
        'primary_fail': oracle.get('rule_id') == RULE and oracle.get('rule_role') == 'primary'
            and oracle.get('verdict') == 'fail',
        'not_diagnostic': oracle.get('diagnostic_only') is False,
        'not_fixture': facts.get('fixture') is False,
        'not_control': scenario.arm != 'negative' and facts.get('control_excluded') is False,
        'not_tool_error': facts.get('tool_error') is False and not facts.get('evidence_error'),
        'identity_observation_complete': facts.get('endpoint_identity_complete') is True
            and facts.get('observation_complete') is True,
        'typed_execution_health': all(facts.get(k) is True for k in
            ('diagnostic_valid', 'execution_evidence', 'environment_healthy', 'process_healthy',
             'source_provenance_qualified', 'preconditions_reached', 'candidate_eligible')),
        'typed_pair_objects': isinstance(facts.get('pairs'), list) and bool(facts['pairs'])
            and isinstance(facts.get('endpoint_lineage'), list) and bool(facts['endpoint_lineage']),
        'typed_residuals': isinstance(oracle.get('forbidden_residuals'), list)
            and bool(oracle['forbidden_residuals'])
            and all(isinstance(v, str) for v in oracle['forbidden_residuals'])
            and set(oracle['forbidden_residuals']) <= {'topology.closed_delivery_gap', 'topology.pair_graph_mismatch', 'topology.identity_mismatch'},
        'sample_identity_resolved': not facts.get('unknown_reasons_sample_identity')
            and not facts.get('unresolved_historical_obligations'),
    }
    stage = _stage('provider_typed_evidence', checks,
                   ('canonical_ingest', 'formal_boundary', 'formal_topology_execution'),
                   {'facts_sha256': canonical_hash(facts), 'oracle_semantics_sha256': canonical_hash(_oracle_semantics(oracle)),
                    'target_sha256': canonical_hash(target_binding)})
    return dict(schema=PROVIDER_SCHEMA, **stage)


def _checked(checks, name, call):
    try:
        call()
        checks[name] = True
    except (ValueError, KeyError, TypeError, OSError, IndexError, AttributeError):
        checks[name] = False


def ingest_assessment(scenario, config, capsule, row):
    """Shared-ingest attachment; cannot grant formal admission eligibility."""
    if scenario.target_id != TARGET:
        return row
    facts, oracle = row.get('typed_facts') or {}, row.get('oracle') or {}
    expected_provider = provider_assessment(scenario, facts, oracle)
    published = oracle.get('candidate_assessment')
    pending = ['formal_boundary', 'formal_topology_execution']
    checks = {}
    if published is None:
        pending.append('provider_assessment_contract_not_published')
    else:
        checks['provider_receipt_matches'] = canonical_hash(published) == canonical_hash(expected_provider)
        checks['provider_scoped_eligibility_declaration'] = oracle.get('candidate_eligible') is True
    checks.update({
        'provider_known_checks': not expected_provider['blockers'],
        'canonical_judged_fail': row.get('execution_stage') == 'judged' and row.get('oracle_verdict') == 'fail',
        'actual_public_execution': row.get('actual_run') is True and row.get('execution_source') == 'ros2_public_api',
        'not_control': row.get('arm') != 'negative' and not row.get('control_kind'),
        'not_tool_error': row.get('tool_error') is not True and row.get('runtime_status') != 'tool_error',
        'no_ingestion_error': not row.get('ingestion_error') and not row.get('capsule_execution_error'),
    })
    # Explicitly bind submitted input through the already raw-audited topology
    # adapter. No synthetic submission receipt or endpoint is introduced here.
    effective = capsule.input.get('effective_input', {})
    sealed_final = effective.get('final_input', {})
    checks['input_config_matches_capsule'] = (config.get('final_input') == sealed_final
        and config.get('effective_input_sha256') == capsule.input.get('effective_input_sha256')
        and config.get('input_layer_receipts') == effective.get('input_layer_receipts')
        and config.get('source_manifest') == capsule.input.get('source_manifest'))
    checks['raw_audited_program_binding'] = facts.get('program_sha256') == canonical_hash(sealed_final.get('topology_program'))
    source = row.get('source_manifest') or {}
    checks['source_frozen_binding'] = (source == capsule.input.get('source_manifest')
        and source.get('source_frozen') is True and source.get('dirty_behavior_paths') == []
        and bool(source.get('head')) and bool(source.get('behavior_tree_sha256')))
    _checked(checks, 'sealed_input_valid', lambda: validate_replay_input(capsule.input))
    _checked(checks, 'canonical_execution_valid', lambda: validate_execution(row))
    row['endpoint_identity'] = topology_observation_identity(row)
    identity_view = dict(row, final_input=sealed_final)
    _checked(checks, 'raw_lineage_pair_proofs_valid', lambda: _validate_observation_identity(identity_view))
    receipt = row.get('mutation_receipt') or {}
    checks['effective_mutation'] = (row.get('arm') in ('baseline', 'transparent') or
        row.get('arm') == 'mutated' and all(receipt.get(k) is True for k in
            ('planned', 'submitted', 'carrier_admitted', 'configured', 'applied', 'hit', 'effect_observed')))
    binding = dict(run_id=row.get('run_id'), target_id=row.get('target_id'), target_revision=row.get('target_revision'),
        source_sha256=canonical_hash(source), raw={'path': row.get('raw_path'), 'sha256': row.get('raw_sha256')},
        capsule_input_sha256=canonical_hash(capsule.input), effective_input_sha256=row.get('effective_input_sha256'),
        endpoint_identity_sha256=canonical_hash(row['endpoint_identity']))
    row['provenance'] = dict(kind='topology_observation_provenance', **snapshot(binding),
        raw_identity_verified=checks.get('raw_lineage_pair_proofs_valid') is True,
        source_binding_verified=checks.get('source_frozen_binding') is True,
        capsule_input_verified=checks.get('sealed_input_valid') is True,
        attribution='not_independent_sut_support', proof_refs_source='endpoint_identity.endpoint_lineage/pairs')
    row['candidate_assessment'] = dict(schema=SCHEMA, target_id=TARGET,
        provider=expected_provider, ingest=_stage('canonical_ingest', checks, pending, binding),
        final=_stage('runner_formal_qualification', {}, ('runner_not_finalized',), binding))
    # This value is scoped to the row's current assessment. None distinguishes
    # unfulfilled formal work from an explicit denial; lifecycle still fails closed.
    row['candidate_eligible'] = False if checks and any(v is not True for v in checks.values()) else None
    return row


def finalize_assessment(scenario, config, capsule, row, *, boundary, qualification):
    """Final strict conjunction AFTER runner's independent formal checks.

    Recompute upstream scopes, never accept a caller's assessment or qualifiers
    in lieu of the runner invoking its real qualification functions.
    """
    if scenario.target_id != TARGET:
        return row
    ingest_assessment(scenario, config, capsule, row)
    assessment = row['candidate_assessment']
    upstream = assessment['ingest']
    pending = [v for v in upstream['pending'] if v not in ('formal_boundary', 'formal_topology_execution')]
    checks = dict(ingest_known_checks=not upstream['blockers'])
    if boundary is None:
        pending.append('formal_boundary')
    else:
        checks['formal_boundary'] = (boundary.get('status') in ('qualified', 'not_applicable')
            and (boundary.get('status') == 'not_applicable' or boundary.get('eligible') is True))
    if qualification is None:
        pending.append('formal_topology_execution')
    else:
        checks['formal_topology_execution'] = (qualification.get('real_ros_execution') is True
            and qualification.get('qualification_errors') == [])
    _checked(checks, 'v2_execution_matches_sealed_input', lambda: _v2_execution_input(row, capsule.input))
    binding = dict(upstream['binding'], capsule_execution_sha256=canonical_hash(capsule.execution))
    stage = _stage('runner_formal_qualification', checks, pending, binding)
    assessment['final'] = stage
    # Known false blockers never become true merely because the formal check
    # succeeds. Provider's formal_pending is discharged only in THIS scope.
    row['candidate_eligible'] = stage['qualified'] is True
    if row['candidate_eligible']:
        checks['concrete_anomaly_signature'] = conformance_signature(row) is not None
        assessment['final'] = _stage('runner_formal_qualification', checks, pending, binding)
        row['candidate_eligible'] = assessment['final']['qualified'] is True
    # A healthy primary pass has no anomaly to admit. Retain every observation,
    # input/raw/source and formal gate, excluding only candidate-specific checks.
    facts, oracle = row.get('typed_facts') or {}, row.get('oracle') or {}
    healthy_pass_evaluable = (
        row.get('execution_stage') == 'judged' and row.get('oracle_verdict') == 'pass'
        and oracle.get('verdict') == 'pass' and oracle.get('rule_id') == RULE
        and oracle.get('rule_role') == 'primary' and oracle.get('forbidden_residuals') == []
        and not facts.get('unknown_reasons') and observation_complete(row)
        and not pending
        and all(passed is True for name, passed in assessment['provider']['checks'].items()
                if name not in ('primary_fail', 'typed_residuals'))
        and all(passed is True for name, passed in upstream['checks'].items()
                if name not in ('provider_known_checks', 'canonical_judged_fail',
                                'provider_scoped_eligibility_declaration'))
        and all(passed is True for name, passed in checks.items() if name != 'ingest_known_checks'))
    attribution = dict(row, environment_healthy=facts_value(row, 'environment_healthy'),
        preconditions_reached=facts_value(row, 'preconditions_reached'),
        identity_complete=upstream['checks'].get('raw_lineage_pair_proofs_valid') is True,
        window_complete=facts_value(row, 'observation_complete'),
        expected_effect_evaluable=row['candidate_eligible'] or healthy_pass_evaluable,
        expected_effect_set=scenario.target.expected_effect_set,
        primary_rule_violated=row.get('oracle_verdict') == 'fail' and row['candidate_eligible'],
        controls_support_sut=False, provenance_supports_sut=False)
    original_reason = (capsule.execution or {}).get('reason_code')
    triage = attribute_responsibility(attribution)
    row.update(triage.to_dict())
    assessment['attribution'] = dict(scope='existing_ordered_attribution', pending=['independent_control_and_sut_review'],
        blockers=[], result=triage.to_dict(), independent_sut_support=False,
        execution_reason_code=original_reason)
    return row


def facts_value(row, key):
    return (row.get('typed_facts') or {}).get(key) is True


def capsule_attachment(capsule, row):
    """Append descriptive assessment without rewriting first execution/history."""
    document = capsule.to_dict()
    if row.get('target_id') == TARGET and row.get('candidate_assessment'):
        document['candidate_assessment_attachment'] = {
            'schema': SCHEMA, 'scope': 'post_execution_candidate_assessment',
            'pending': snapshot(row['candidate_assessment']['final']['pending']),
            'blockers': snapshot(row['candidate_assessment']['final']['blockers']),
            'execution_snapshot_sha256': canonical_hash(document.get('execution')),
            'execution_history_sha256': canonical_hash(document.get('execution_history')),
            'assessment': snapshot(row['candidate_assessment']),
            'projection': {k: snapshot(row.get(k)) for k in
                ('candidate_eligible', 'provenance', 'triage_disposition', 'responsibility_layer', 'reason_code')},
        }
    return document
