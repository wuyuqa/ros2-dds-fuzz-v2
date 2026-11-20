"""F1 adapter for the existing exploratory schema/ledger, not an execution proof.

The standalone consumer repeats qualify_f1_artifacts for adapter receipt rows.
Saved labels are never used as a substitute for the original case artifacts.
"""
from copy import deepcopy
import json
from pathlib import Path

from r2f.core.evidence import canonical_hash
from r2f.core.replay import ReplayCapsule
from r2f.core.scenario import Scenario
from r2f.campaign.runtime_adapter import ingest_execution


def _same(a, b, reason):
    if canonical_hash(a) != canonical_hash(b):
        raise ValueError(reason)


def _read(path):
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict):
        raise ValueError('f1_artifact_not_an_object:' + str(path))
    return value


def _qualify_g01_rejection(root, row, cycle):
    """Regenerate a pure proposal rejection before bypassing execution gates."""
    from r2f.campaign.checkpoint import _identity, _validate_corpus_binding
    from r2f.campaign.corpus import Corpus
    from r2f.campaign.scheduler import validated_feedback
    from r2f.campaign.scheduler import ExploratoryScheduler
    from r2f.campaign.search_frontier import InputSearchFrontier, derived_record
    from r2f.campaign.runner import _build_case_plan, _derive_rng_seed, _NoEffectiveInputRejection
    from r2f.reports.run_report import _path
    case = root / 'cases' / row['cell_id']
    if {p.name for p in case.iterdir()} != {'row.json', 'mutation_plan.json'}:
        raise ValueError('f1_rejected_proposal_has_runtime_artifacts')
    original = _read(_path(root, str(case / 'row.json')))
    producer = deepcopy(row)
    if 'generation_binding' in producer:
        producer['binding'] = producer.pop('generation_binding')
    _same(original, producer, 'f1_persisted_rejected_row_mismatch')
    if (row.get('actual_run') is not False or row.get('execution_stage') != 'planned'
            or row.get('oracle_verdict') != 'not_applicable'
            or row.get('reason_code') != 'no_effective_input'
            or row.get('generation_status') != 'rejected'
            or row.get('input_layer_receipts') != []
            or row.get('candidate_eligible') is not False or row.get('feedback_eligible') is not False
            or any(row.get(k) is not None for k in (
                'raw_path', 'raw_sha256', 'runtime_result_path', 'replay_path', 'runtime_config_path',
                'mutation_receipt', 'runtime_execution_metadata', 'effective_input', 'effective_input_sha256'))
            or any(row.get(k) is True for k in ('workspace_executed', 'hit', 'effect_observed', 'seal_eligible'))):
        raise ValueError('f1_rejected_proposal_claims_execution')
    plan = _read(_path(root, str(case / 'mutation_plan.json')))
    _same(plan, row.get('rejected_mutation_plan'), 'f1_rejected_plan_mismatch')
    from r2f.core.mutation_plan import MutationPlan
    if MutationPlan.from_dict(plan).plan_hash() != row.get('rejected_mutation_plan_sha256'):
        raise ValueError('f1_rejected_plan_hash_mismatch')
    profile = cycle['profile']
    identities = {c.get('cycle_id') for c in profile.get('cycles', [])
                  if c.get('target_id') == row['target_id']}
    if len(identities) != 1:
        raise ValueError('f1_rejection_cycle_ambiguous')
    identity, seeds, registry = _identity(profile, identities.pop(), cycle['source_manifest'])
    scheduler_path = _path(root, 'selection_log.json')
    saved = _read(scheduler_path)
    parents = None
    frontier = None
    if saved.get('policy') == 'bfs':
        frontier = InputSearchFrontier.from_dict(saved.get('frontier'))
        frontier.validate_model(profile=profile, seeds=seeds, registry=registry)
        parents = {k: deepcopy(v['entry']) for k, v in frontier.nodes.items()}
    corpus = Corpus.from_dict(_read(_path(root, 'corpus.json')), model_parents=parents)
    _validate_corpus_binding(corpus, identity, seeds, registry)
    feedback = validated_feedback(_read(_path(root, 'feedback.json'))['events'])
    scheduler = ExploratoryScheduler.from_dict(saved, corpus, feedback, model_parents=parents)
    try:
        sequence = int(row['cell_id'].removeprefix(cycle['run_id'] + '.').split('.')[0])
        selection = scheduler.selection_log[sequence]
    except (ValueError, IndexError) as exc:
        raise ValueError('f1_rejection_selection_missing') from exc
    if row['cell_id'] != f"{cycle['run_id']}.{sequence:04d}.rejected":
        raise ValueError('f1_rejection_cell_sequence_mismatch')
    if frontier is not None:
        task = frontier.task(selection['task_id'])
        if task['status'] != 'rejected' or task.get('actual') is not False or task.get('execution') is not None:
            raise ValueError('f1_rejection_task_not_terminal')
        entry = frontier.entry(selection)
        search = row.get('input_search') or {}
        for key in ('task_id', 'baseline_content_sha256', 'parent_content_sha256',
                    'mutation_route', 'sampling_kind', 'derivation_depth', 'sample_slot', 'rng_seed'):
            _same(search.get(key), selection.get(key), 'f1_rejection_search_mismatch:' + key)
    else:
        context = scheduler.selection_contexts[sequence]
        entry = Corpus.from_dict(context['corpus']).entries[selection['content_sha256']]
    target = registry.get_target(entry['target_id'])
    contract = target.exploration_contract
    route = selection.get('mutation_route') or entry.get('mutation_routes', [None])[0]
    surface = contract.surface_by_id[entry['surface_id']]
    mutator = route['mutator_id'] if route else surface.mutator_ids[0]
    rng_seed = _derive_rng_seed(scheduler, selection, entry)
    try:
        _build_case_plan(entry, target, contract, surface, entry['final_config'],
                         mutator, profile, rng_seed, selection)
    except _NoEffectiveInputRejection as exc:
        _same(exc.plan.to_dict(), plan, 'f1_rejected_proposal_not_model_generatable')
        if str(exc) != row.get('no_effective_input_reason'):
            raise ValueError('f1_rejection_reason_mismatch')
        _same(derived_record(entry, exc.plan)['content_sha256'], row.get('content_sha256'),
              'f1_rejection_content_mismatch')
    else:
        raise ValueError('f1_proposal_not_a_generation_rejection')
    for key, expected in (('target_id', target.target_id), ('target_revision', target.target_revision),
                          ('profile_id', profile['profile_id']), ('rmw', entry['rmw']),
                          ('parent_content_sha256', entry['content_sha256'])):
        _same(row.get(key), expected, 'f1_rejection_identity_mismatch:' + key)
    refs = (case / 'row.json', case / 'mutation_plan.json', scheduler_path,
            _path(root, 'corpus.json'), _path(root, 'feedback.json'))
    return {str(p.relative_to(root)): canonical_hash(_read(p)) for p in refs}


def qualify_f1_artifacts(root, row, cycle):
    """Reopen sealed input, project persisted config, reingest raw and qualify.

    root is a caller-owned containment anchor, not a path taken from a receipt.
    No current-source attestation is made when reading historical evidence.
    """
    from r2f.campaign.replay import _registered_target, _qualification, DEFAULT_RUNTIME, ISOLATED_RUNTIME
    from r2f.reports.run_report import _path
    from r2f.campaign.runner import _F1_CYCLE_TARGETS
    root = Path(root).resolve()
    cell = row.get('cell_id')
    if not isinstance(cell, str) or cell in {'', '.', '..'} or Path(cell).name != cell or '\\' in cell:
        raise ValueError('f1_invalid_cell_id')
    if row.get('target_id') not in _F1_CYCLE_TARGETS:
        raise ValueError('f1_unregistered_report_route')
    _same(row.get('run_id'), cycle.get('run_id'), 'f1_report_run_mismatch')
    no_execution = {'real_ros_execution': False, 'real_runtime_attempt': False,
                    'qualification_errors': ['f1_not_executed']}
    if row.get('mutation_plan') is None:
        if (row.get('generation_status') != 'rejected' or row.get('actual_run') is not False
                or row.get('execution_stage') != 'planned'):
            raise ValueError('f1_missing_plan_not_generation_rejection')
        from r2f.campaign.runner import G01_TARGET
        references = (_qualify_g01_rejection(root, row, cycle)
                      if row.get('target_id') == G01_TARGET else {})
        return {'binding': deepcopy(row.get('binding')), 'qualification': no_execution,
                'artifact_references': references}
    if 'source_manifest' in row:
        _same(row['source_manifest'], cycle.get('source_manifest'), 'f1_report_source_mismatch')
    case = root / 'cases' / cell
    paths = {'replay_path': case / 'capsule.json', 'runtime_config_path': case / 'runtime_config.json'}
    for key, path in paths.items():
        if _path(root, row.get(key)) != path.resolve():
            raise ValueError('f1_artifact_layout_mismatch:' + key)
    original = _read(_path(root, str(case / 'row.json')))
    producer_row = deepcopy(row)
    if 'generation_binding' in producer_row:
        producer_row['binding'] = producer_row.pop('generation_binding')
    _same(original, producer_row, 'f1_persisted_row_mismatch')
    capsule = ReplayCapsule.from_dict(_read(paths['replay_path']))
    target, services, _ = _registered_target(capsule)
    if 'target' in row:
        _same(target.to_dict(), row['target'], 'f1_registered_target_mismatch')
    if 'model' in cycle:
        model = cycle['model']
        if (not isinstance(model, dict) or not isinstance(model.get('targets'), dict)
                or not isinstance(model.get('rules'), dict)):
            raise ValueError('f1_report_model_shape_invalid')
        _same((model.get('targets') or {}).get(target.target_id), target.to_dict(), 'f1_report_target_model_mismatch')
        _same((model.get('rules') or {}).get(target.target_id), target.rule_contract, 'f1_report_rule_model_mismatch')
        from r2f.core.mutators import ALL_MUTATOR_SPECS
        catalog = model.get('mutator_catalog')
        if not isinstance(catalog, dict) or not catalog:
            raise ValueError('f1_report_mutator_catalog_missing')
        for key, spec in catalog.items():
            if key not in ALL_MUTATOR_SPECS:
                raise ValueError('f1_report_mutator_unregistered:' + key)
            _same(spec, ALL_MUTATOR_SPECS[key].to_dict(), 'f1_report_mutator_spec_mismatch:' + key)
        if set(model.get('targets', {})) != set(cycle['profile']['target_ids']):
            raise ValueError('f1_report_target_catalog_mismatch')
        for key, value in (('model_sha256', model), ('target_sha256', model.get('targets')),
                           ('rule_sha256', model.get('rules')), ('profile_sha256', cycle.get('profile'))):
            if (cycle.get('identity') or {}).get(key) != canonical_hash(value):
                raise ValueError('f1_report_identity_mismatch:' + key)
    config = _read(paths['runtime_config_path'])
    _same(config.get('source_manifest'), cycle.get('source_manifest'), 'f1_config_report_source_mismatch')
    plan = _read(_path(root, str(case / 'mutation_plan.json')))
    for value in (row.get('mutation_plan'), config.get('mutation_plan'), capsule.input.get('mutation_plan')):
        _same(plan, value, 'f1_plan_artifact_mismatch')
    if row.get('mutation_plan_sha256') != canonical_hash(plan):
        raise ValueError('f1_plan_hash_mismatch')
    if plan.get('contract_hash') != target.exploration_contract.contract_hash():
        raise ValueError('f1_registered_contract_mismatch')
    if 'model' in cycle:
        for operation in plan['operations']:
            if operation['mutator_id'] not in cycle['model']['mutator_catalog']:
                raise ValueError('f1_report_used_mutator_missing')
    final = plan['final_config']
    for value in (row.get('final_input'), config.get('final_input'), capsule.input['effective_input']['final_input']):
        _same(final, value, 'f1_final_input_mismatch')
    # Independent operands: sealed final input and persisted runtime projection.
    # The runner restores int->float widening before persisting runtime_config.
    observed = {key: config[key] for key in final if key in config}
    _same(final, observed, 'f1_runtime_final_projection_mismatch')
    for key in ('scenario', 'effective_input', 'effective_input_sha256', 'source_manifest'):
        _same(config.get(key), capsule.input.get(key), 'f1_config_capsule_mismatch:' + key)
    for key in ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
                'effective_input_sha256', 'input_layer_receipts', 'mutation_plan_sha256'):
        _same(config.get(key), row.get(key), 'f1_config_row_mismatch:' + key)
    if config.get('cell_id') != cell or config.get('profile_id') != cycle['profile']['profile_id']:
        raise ValueError('f1_config_report_identity_mismatch')
    generation = row.get('generation_binding', row.get('binding')) or {}
    if (generation.get('status') != 'preserved' or generation.get('silent_normalization') is not False
            or generation.get('submitted_config_sha256') != canonical_hash(final)):
        raise ValueError('f1_generation_binding_invalid')
    references = {str(path.relative_to(root)): canonical_hash(_read(path))
                  for path in (*paths.values(), case / 'mutation_plan.json', case / 'row.json')}
    binding = {'schema': 'r2f.f1-adapter-binding/v1', 'status': 'preserved', 'silent_normalization': False,
               'expected_config_sha256': canonical_hash(final), 'observed_config_sha256': canonical_hash(observed),
               'projection_fields': sorted(final), 'effective_input_sha256': capsule.input['effective_input_sha256'],
               'generation_binding_sha256': canonical_hash(generation)}
    result_path = case / 'runtime' / 'runtime_result.json'
    qualified = no_execution
    if result_path.exists():
        if _path(root, row.get('runtime_result_path')) != result_path.resolve():
            raise ValueError('f1_artifact_layout_mismatch:runtime_result_path')
        result = _read(result_path)
        recalculated = ingest_execution(Scenario.from_dict(capsule.input['scenario']), config, result, capsule,
                                       services=services)
        for key in ('execution_stage', 'actual_run', 'abort_reason', 'typed_facts', 'oracle', 'oracle_verdict',
                    'rule_id', 'mutation_receipt', 'precondition_receipts', 'observation_receipts',
                    'raw_path', 'raw_sha256', 'runtime_status', 'runtime_reason', 'runtime_execution_metadata'):
            _same(row.get(key), recalculated.get(key), 'f1_recomputed_ingest_differs:' + key)
        origin = ((ISOLATED_RUNTIME if cycle.get('runtime_boundary') == 'isolated_netns' else DEFAULT_RUNTIME)
                  if cycle.get('runtime_formal') is True else 'injected_runtime')
        qualified = _qualification(origin, config, result, recalculated, output_root=case / 'runtime')
        references[str(result_path.relative_to(root))] = canonical_hash(result)
        if row.get('raw_path'):
            raw = _path(root, row['raw_path'])
            from r2f.core.evidence import verify_raw
            verify_raw(raw, row.get('raw_sha256'))
            references[str(raw.relative_to(root))] = row['raw_sha256']
    elif row.get('actual_run') is True or row.get('execution_stage') not in {'planned', 'aborted'}:
        raise ValueError('f1_execution_result_missing')
    if 'generation_binding' in row:
        _same(row.get('binding'), binding, 'f1_adapter_binding_receipt_mismatch')
    return {'binding': binding, 'qualification': qualified, 'artifact_references': references}


def persist_f1_exploratory_report(output_dir, cycle, registry, *, artifact_root=None):
    """Generate existing report/ledger without changing any original artifacts.

    An explicit historical read root does not relax the existing auditor's
    containment gate. Save any resulting audit errors verbatim.
    """
    from r2f.campaign.runner import _atomic_json
    from r2f.campaign.scheduler import required_catalog_hash
    from r2f.core.mutators import ALL_MUTATOR_SPECS
    from r2f.modules.s1.fuzz_catalog import catalog_digest
    from r2f.reports.candidate_report import exploratory_candidate_ledger
    from r2f.reports.run_report import EXPLORATORY_SCHEMA, exploratory_report_digest, audit_exploratory_report
    output_dir = Path(output_dir).resolve()
    root = Path(artifact_root).resolve() if artifact_root is not None else output_dir
    targets = {key: registry.get_target(key) for key in cycle['profile']['target_ids']}
    target_model = {key: target.to_dict() for key, target in targets.items()}
    rules = {key: target.rule_contract for key, target in targets.items()}
    used = {op['mutator_id'] for row in cycle['rows'] for op in (row.get('mutation_plan') or {}).get('operations', [])}
    if not used:
        used = {mid for target in targets.values() for surface in target.exploration_contract.surfaces
                for mid in surface.mutator_ids}
    mutators = {key: ALL_MUTATOR_SPECS[key].to_dict() for key in sorted(used)}
    model = {'catalog_digest': catalog_digest(), 'mutator_catalog': mutators,
             'mutator_catalog_sha256': canonical_hash(mutators), 'targets': target_model, 'rules': rules}
    rows, eligible, proofs, build_errors, exclusions = [], [], {}, [], []
    for original in cycle['rows']:
        row = deepcopy(original)
        try:
            proof = qualify_f1_artifacts(root, original, cycle)
            row['generation_binding'] = deepcopy(original.get('binding'))
            row['binding'] = proof['binding']
            proofs[row['cell_id']] = proof
            qualified = proof['qualification']['real_ros_execution'] is True
        except (OSError, ValueError, TypeError, KeyError) as exc:
            qualified = False
            build_errors.append(str(row.get('cell_id')) + ':' + str(exc))
            proofs[row.get('cell_id')] = {'qualification': {'real_ros_execution': False,
                                                         'qualification_errors': [str(exc)]}}
        rows.append(row)
        # Preserve failure/unknown/control accounting; only qualified judged
        # failures can enter natural admission. Original facts remain untouched.
        if qualified or row.get('execution_stage') != 'judged' or row.get('oracle_verdict') != 'fail':
            eligible.append(row)
        else:
            exclusions.append({'cell_id': row.get('cell_id'), 'reason': 'f1_execution_not_qualified'})
        if row.get('actual_run') is True and not qualified:
            build_errors.append(str(row['cell_id']) + ':f1_execution_not_qualified')
    report = {'schema_version': EXPLORATORY_SCHEMA, 'campaign_kind': 'exploratory',
              'run_id': cycle['run_id'], 'profile': deepcopy(cycle['profile']),
              'source_manifest': deepcopy(cycle['source_manifest']), 'model': model,
              'identity': {'profile_sha256': canonical_hash(cycle['profile']),
                           'source_sha256': canonical_hash(cycle['source_manifest']),
                           'model_sha256': canonical_hash(model), 'rule_sha256': canonical_hash(rules),
                           'target_sha256': canonical_hash(target_model)},
              'required_catalog_sha256': required_catalog_hash(), 'rows': rows,
              'budget': cycle['budget'], 'last_state': cycle['last_state'], 'abort': cycle['abort'],
              **({key: deepcopy(cycle[key]) for key in ('cycle_budget', 'resume_origin', 'row_sequences')
                  if key in cycle}),
              'runtime_boundary': cycle['runtime_boundary'], 'runtime_formal': cycle['runtime_formal'],
              'control_references': [], 'review_records': [],
              'proof_provenance': {'artifact_root': str(root), 'cycle_sha256': canonical_hash(cycle),
                                   'kind': 'offline_reopened_case_consistency',
                                   'source_head': cycle['source_manifest'].get('head'),
                                   'new_execution': False, 'controls_run_by_report_builder': False},
              'artifact_qualifications': proofs, 'producer_validation_errors': build_errors,
              'qualification_exclusions': exclusions,
              'producer_valid': not build_errors,
              'limitations': ['No new execution or current-source live proof.',
                              'No controls run or required 72-cell seal borrowed.',
                              'Historical external-root references are rejected by the standalone auditor.']}
    ledger = exploratory_candidate_ledger(eligible)
    _atomic_json(output_dir / 'candidate_ledger.json', ledger)
    report.update(candidate_ledger_path='candidate_ledger.json', candidate_ledger_sha256=canonical_hash(ledger))
    report['report_sha256'] = exploratory_report_digest(report)
    path = output_dir / 'report.json'
    _atomic_json(path, report)
    report['audit'] = audit_exploratory_report(path)
    _atomic_json(path, report)
    return path
