"""Saved-case oracle reevaluation and derived review/host implementation qualification.

CP2 remains the authority for task, before/expected and independent controls.
This boundary actually calls a registry-pinned oracle on canonical raw-rebuilt
facts. It neither imports a proposal path nor executes the recorded old model.
"""
import json
import re
from pathlib import Path

from r2f.campaign.identity import source_manifest
from r2f.campaign.runtime_adapter import _evaluate_oracle, rebuild_saved_case_facts
from r2f.core._json import snapshot
from r2f.core.candidate import candidate_execution_identity, validate_capability_validation
from r2f.core.evidence import canonical_hash, verify_raw
from r2f.core.replay import ReplayCapsule


def _read(ref):
    if not isinstance(ref, dict) or set(ref) != {'path', 'sha256'}:
        raise ValueError('saved_case_artifact_reference_invalid')
    verify_raw(ref['path'], ref['sha256'])
    value = json.loads(Path(ref['path']).read_text())
    if not isinstance(value, dict):
        raise ValueError('saved_case_artifact_not_object')
    return value


def _scenario_digest(scenario):
    return canonical_hash({
        'scenario': scenario.to_dict(), 'final_input': scenario.final_input,
        'input_layer_receipts': scenario.input_layer_receipts,
        'source_manifest': scenario.source_manifest, 'input_sha256': scenario.effective_input_sha256})


def evaluate_reconnect(validation, *, evidence_refs, original_ref, assessment,
                       task_review, registry, source, saved_cases):
    """Actually reevaluate one CP2 defect and >=2 independent stable controls.

    ``saved_cases`` maps each exact CP2 case SHA256 to hash-bound ``runtime`` and
    ``capsule`` JSON references. Missing/changed capture contracts fail closed;
    this initial slice supports identical target/revision/manifest only. ``source``
    is an expected *current measurement*, checked before and after all calls.
    Fixture success proves the offline mechanism and always implemented=False.
    Invalid intake raises ValueError; actual call failures retain diagnostics in
    a rejected artifact. Returned JSON is detached and has its own content hash.
    """
    validation, evidence_refs, original_ref, assessment, task_review, source, saved_cases = snapshot(
        [validation, evidence_refs, original_ref, assessment, task_review, source, saved_cases])
    cp2 = validate_capability_validation(validation, evidence_refs, original_ref=original_ref,
                                        assessment=assessment, task_review=task_review)
    if cp2 is None:
        raise ValueError('capability_validation_required')
    bundle_ref = {'path': evidence_refs[validation['evidence_key']],
                  'sha256': validation['evidence_sha256']}
    bundle = _read(bundle_ref)
    case_refs = bundle['cases']
    if set(saved_cases) != {ref['sha256'] for ref in case_refs}:
        raise ValueError('saved_case_reference_set_mismatch')
    repository = Path(__file__).resolve().parents[3]

    def verify_source():
        measured = source_manifest(repository)
        if (not isinstance(source, dict) or source.get('source_repo') != str(repository)
                or canonical_hash(measured) != canonical_hash(source)):
            raise ValueError('evaluation_source_changed_or_mismatched')
        return measured

    verify_source()
    binding = registry.bind_services(original_ref['target_id'])
    descriptor = binding.descriptor
    if descriptor['target_revision'] != original_ref['target_revision']:
        raise ValueError('unsupported_contract_migration')
    model = bundle['models']['after']
    actual = descriptor['components']['oracle']['callables']['evaluate']
    if (Path(model['path']).resolve() != Path(actual['source_file'])
            or model['sha256'] != actual['source_sha256']
            or canonical_hash(model['source']) != canonical_hash(source)):
        raise ValueError('unbound_model_artifact')
    refs = [bundle_ref, *case_refs, *bundle['models'].values()]
    refs += [{'path': path, 'sha256': task_review['evidence_sha256'][key]}
             for key, path in task_review['evidence_refs'].items()]
    cases = []
    for ref in case_refs:
        case = _read(ref)
        inputs = saved_cases[ref['sha256']]
        if not isinstance(inputs, dict) or set(inputs) != {'runtime', 'capsule'}:
            raise ValueError('saved_case_inputs_invalid')
        runtime = _read(inputs['runtime'])
        capsule = ReplayCapsule.from_dict(_read(inputs['capsule']))
        row = case['row']
        if row.get('replay_path') != inputs['capsule']['path']:
            raise ValueError('saved_case_capsule_reference_mismatch')
        if (canonical_hash(capsule.input['target']) != descriptor['target_manifest_sha256']
                or capsule.input['target_revision'] != descriptor['target_revision']
                or capsule.input['target_id'] != descriptor['target_id']
                or capsule.input['target']['rule_contract'].get('primary_rule') != original_ref['rule_id']):
            raise ValueError('unsupported_contract_migration')
        refs.extend(inputs.values())
        refs.append({'path': row['raw_path'], 'sha256': row['raw_sha256']})
        cases.append((ref, case, runtime, capsule, inputs))

    def verify_all():
        measured = verify_source()
        binding.verify_unchanged()
        for ref in refs:
            verify_raw(ref['path'], ref['sha256'])
        return measured

    artifact = dict(schema_version='r2f.capability-reconnect-evaluation/v1',
                    status='rejected', interpretation='offline_reinterpretation',
                    implemented=False, new_execution=False, natural_candidates=0,
                    qualification='fixture' if bundle['provenance'] == 'fixture' else 'mechanism_only',
                    task_ref=cp2['task_ref'], validation_ref=bundle_ref,
                    capture_source=original_ref['source'], evaluation_source=source,
                    original_ref=original_ref, cp2_validation=cp2,
                    binding=descriptor, binding_sha256=binding.descriptor_sha256,
                    models=bundle['models'], call_receipts=[], diagnostics=[])
    verify_all()
    for ref, case, runtime, capsule, inputs in cases:
        receipt = dict(case_ref=ref, capture_identity=candidate_execution_identity(case['row']),
                       input_sha256=case['input_sha256'], kind=case['kind'],
                       capture_source=original_ref['source'], evaluation_source=source,
                       binding_sha256=binding.descriptor_sha256, input_refs=inputs,
                       before=case['before'], before_execution='recorded_only', expected=case['expected'],
                       before_sha256=canonical_hash(case['before']),
                       expected_sha256=canonical_hash(case['expected']), oracle_called=False)
        artifact['call_receipts'].append(receipt)
        try:
            verify_all()
            scenario, facts = rebuild_saved_case_facts(case, runtime=runtime, capsule=capsule,
                                                       services=binding.services)
            receipt.update(facts=snapshot(facts), facts_sha256=canonical_hash(facts),
                           evaluation_source_before=verify_all())
            scenario_before = _scenario_digest(scenario)
            receipt['oracle_called'] = True
            result, failure = _evaluate_oracle(binding.services, scenario, facts)
            receipt.update(after=result, result_sha256=canonical_hash(result),
                           oracle_tool_failure=failure)
            receipt['evaluation_source_after'] = source_manifest(repository)
            verify_all()
            current_scenario = _scenario_digest(scenario)
            if canonical_hash(facts) != receipt['facts_sha256'] or current_scenario != scenario_before:
                raise ValueError('oracle_mutated_facts_or_scenario')
            if failure is not None or result['verdict'] not in ('pass', 'fail'):
                raise ValueError('actual_oracle_unavailable')
            if canonical_hash(result) != receipt['expected_sha256']:
                raise ValueError('actual_oracle_did_not_match_expected')
            receipt['status'] = 'matched'
        except Exception as exc:
            receipt.update(status='rejected', diagnostic={'exception_type': type(exc).__name__, 'reason': str(exc)})
            artifact['diagnostics'].append(snapshot(receipt['diagnostic']))
            break
    try:
        verify_all()
    except Exception as exc:
        artifact['diagnostics'].append({'exception_type': type(exc).__name__, 'reason': str(exc)})
    if not artifact['diagnostics'] and len(artifact['call_receipts']) == len(cases):
        artifact['status'] = 'evaluated'
    artifact['evaluation_sha256'] = canonical_hash(artifact)
    return snapshot(artifact)


def _frozen_source(source):
    # Captures must carry the existing behavior-source identity, not just a label.
    return (isinstance(source, dict) and source.get('source_frozen') is True
            and source.get('dirty_behavior_paths') == []
            and isinstance(source.get('source_repo'), str) and bool(source['source_repo'])
            and isinstance(source.get('head'), str)
            and re.fullmatch('[0-9a-f]{40}', source['head']) is not None
            and isinstance(source.get('behavior_tree_sha256'), str)
            and re.fullmatch('[0-9a-f]{64}', source['behavior_tree_sha256']) is not None)


def _implementation_qualification(artifact, ledger, review):
    """Derive implementation from existing review/human authority, never JSON flags.

    CP2's current independent validation review is the improvement review. There
    is no second approval lifecycle. Blackbox obligations do not gate this result.
    """
    pending = []
    fixture = artifact['qualification'] == 'fixture'
    evaluated = artifact['status'] == 'evaluated'
    task = next(t for t in artifact['task_assessment']['proposed_tasks']
                if t['task_id'] == artifact['task_ref']['task_id'])
    # This reconnect binds/evaluates the oracle, not a seed, carrier or sidechannel.
    if task['kind'] != 'rule':
        pending.append('unsupported_model_task_kind')
    if fixture:
        pending.append('actual_feedback_evidence')
    if not evaluated:
        pending.append('actual_oracle_validation')
    if not _frozen_source(artifact['evaluation_source']):
        pending.append('evaluation_source_not_frozen')
    if not _frozen_source(artifact['capture_source']):
        pending.append('capture_source_not_qualified')
    candidate = next((c for c in ledger['candidates']
                      if c['candidate_id'] == review['candidate_id']), {})
    analysis = candidate.get('analysis') or {}
    current = analysis.get('current')
    opinions = analysis.get('current_opinions', [current] if current else [])
    current_review = (not ledger['review_errors'] and current is not None
        and any(r['record_id'] == review['record_id']
                and r['record_sha256'] == review['record_sha256'] for r in opinions)
        and all(r['conclusion'] == 'supported' and r['attribution'] == 'sut_candidate'
                for r in opinions) and bool(review['independent_of']))
    if not current_review:
        pending.append('current_independent_validation_review')
    human = (candidate.get('human_adjudication') or {}).get('current') or {}
    authenticated = (not ledger['human_errors'] and not candidate.get('human_qualification_error')
                     and candidate.get('candidate_status') == 'confirmed'
                     and not candidate.get('admission_error')
                     and human.get('action') == 'confirm'
                     and human.get('human_id') != review['reviewer_id'])
    human_confirmed = bool(not fixture and _frozen_source(artifact['capture_source'])
                           and current_review and authenticated)
    if not human_confirmed:
        pending.append('human_confirmation')
    implemented = not pending
    return dict(status=('implemented' if implemented else
                        'fixture_validated' if fixture and evaluated else 'pending'),
                implemented=implemented, human_confirmed=human_confirmed,
                human_record_id=human.get('record_id') if human_confirmed else None,
                pending_obligations=pending)


def evaluate_review_reconnect(*, review_log, validation_record_id, original_row, saved_cases,
                              human_log=None, human_authority=None):
    """Resolve canonical review authority and select current registered code.

    Implementation qualification is derived; this never creates a new execution.
    No caller-provided model loader, binding, version or verdict is accepted.
    """
    from r2f.core import Registry
    from r2f.core.evidence import raw_sha256
    from r2f.modules.s1.module import register
    from r2f.reports.candidate_report import (
        read_review_records, read_human_records, exploratory_candidate_ledger)

    original_row, saved_cases = snapshot([original_row, saved_cases])
    log_ref = {'path': str(Path(review_log).resolve()), 'sha256': raw_sha256(review_log)}
    records = read_review_records(review_log, original_rows=[original_row])
    review = next((r for r in records if r['record_id'] == validation_record_id), None)
    if review is None or review.get('capability_validation') is None:
        raise ValueError('reconnect_validation_review_missing')
    if any(r.get('supersedes_record_id') == validation_record_id for r in records):
        raise ValueError('reconnect_validation_review_superseded')
    parent = next((r for r in records if r['record_id'] == review.get('supersedes_record_id')), None)
    human_ref, human_records = None, []
    if human_log is not None:
        human_ref = {'path': str(Path(human_log).resolve()), 'sha256': raw_sha256(human_log)}
        human_records = read_human_records(human_log, original_rows=[original_row],
            reviews=records, human_authority=human_authority)
    registry = Registry()
    register(registry)
    source = source_manifest(Path(__file__).resolve().parents[3])
    artifact = evaluate_reconnect(review['capability_validation'],
        evidence_refs=review['evidence_refs'], original_ref=candidate_execution_identity(original_row),
        assessment=review.get('capability_assessment'), task_review=parent,
        registry=registry, source=source, saved_cases=saved_cases)
    verify_raw(log_ref['path'], log_ref['sha256'])
    artifact.pop('evaluation_sha256')
    artifact.update(request={'review_log': log_ref, 'validation_record_id': validation_record_id,
                             'original_row': original_row, 'saved_cases': saved_cases},
                    validation_review={'record_id': review['record_id'],
                                       'record_sha256': review['record_sha256']},
                    task_assessment=parent['capability_assessment'],
                    task_review={'record_id': parent['record_id'],
                                 'record_sha256': parent['record_sha256']},
                    selected_model_version=artifact['binding_sha256'])
    # Reopen host policy/signatures and lifecycle history after the oracle calls.
    # Log hashes bind bytes; they do not authenticate the caller's current trust.
    if human_ref is not None:
        human_records = read_human_records(human_ref['path'], original_rows=[original_row],
            reviews=records, human_authority=human_authority)
    ledger = exploratory_candidate_ledger([original_row], reviews=records,
        human_records=human_records, human_authority=human_authority)
    verify_raw(log_ref['path'], log_ref['sha256'])
    artifact['activation'] = _implementation_qualification(artifact, ledger, review)
    artifact['implemented'] = artifact['activation']['implemented']
    artifact['human_record_refs'] = [
        {'record_id': r['record_id'], 'record_sha256': r['record_sha256']} for r in human_records]
    if human_ref is not None:
        verify_raw(human_ref['path'], human_ref['sha256'])
        artifact['request']['human_log'] = human_ref
    artifact['evaluation_sha256'] = canonical_hash(artifact)
    return snapshot(artifact)


def verify_review_reconnect(evaluation, *, human_authority=None):
    """Recompute current oracle evidence; content hashes are not authority."""
    request = evaluation['request']
    required = {'review_log', 'validation_record_id', 'original_row', 'saved_cases'}
    if set(request) not in (required, required | {'human_log'}):
        raise ValueError('reconnect_request_invalid')
    log_ref = request['review_log']
    verify_raw(log_ref['path'], log_ref['sha256'])
    human_ref = request.get('human_log')
    if human_ref is not None:
        if not isinstance(human_ref, dict) or set(human_ref) != {'path', 'sha256'}:
            raise ValueError('reconnect_human_log_reference_invalid')
        verify_raw(human_ref['path'], human_ref['sha256'])
    actual = evaluate_review_reconnect(review_log=log_ref['path'],
        validation_record_id=request['validation_record_id'], original_row=request['original_row'],
        saved_cases=request['saved_cases'],
        human_log=human_ref['path'] if human_ref is not None else None,
        human_authority=human_authority)
    if canonical_hash(actual) != canonical_hash(evaluation):
        raise ValueError('reconnect_evaluation_mismatch')
    return actual
