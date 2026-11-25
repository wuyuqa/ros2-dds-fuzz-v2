"""Persist one normal conformance rerun without changing its source evidence.

Execution and comparison use core.rerun_conformance and the existing runtime
config/ingest boundary. audit_replay only reads artifacts; it never starts ROS.
Injected runtimes establish offline contract evidence, regardless of their flags.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import uuid

from r2f.core import Registry, ReplayCapsule, Scenario, canonical_hash, raw_sha256, rerun_conformance
from r2f.core.replay import conformance_signature, validate_replay_attempt
from r2f.core.state_machine import utc_now
from r2f.modules.s1.module import S1Module, register_exploratory
from .identity import reject_execution_identity, runtime_config, source_manifest
from .runtime_adapter import ingest_execution

SCHEMA = 'r2f.campaign-replay-audit/v1'
REPO = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME = 'r2f.runtime.ros2_pair.run_pair'
ISOLATED_RUNTIME = 'r2f.runtime.execution_boundary.run_isolated_runtime'
FORMAL_RUNTIMES = frozenset({DEFAULT_RUNTIME, ISOLATED_RUNTIME})
_OFFLINE = {'fixture', 'synthetic', 'offline'}


class _Failure(ValueError):
    def __init__(self, status, code, reason, phase):
        super().__init__(reason)
        self.status, self.code, self.phase = status, code, phase


def _json(path):
    return json.loads(Path(path).read_text())


def _write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        json.dump(data, stream, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write('\n')


def _digest(report):
    return canonical_hash({k: v for k, v in report.items() if k != 'report_sha256'})


def _reference(path):
    path = Path(path).resolve()
    return {'path': str(path), 'available': path.is_file(),
            'sha256': raw_sha256(path) if path.is_file() else None}


def _reserve(capsule_path, output):
    root = capsule_path.parent
    if root.parent.name in {'cells', 'cases'}:
        root = root.parent.parent
    if capsule_path.exists() and (output.is_relative_to(root) or root.is_relative_to(output)):
        raise ValueError('replay output must be independent of the original evidence directory')
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise FileExistsError('replay output directory must be empty: ' + str(output))


def _check_domain(capsule, domain):
    if type(domain) is not int or not 0 <= domain <= 231:
        raise _Failure('fail_environment', 'invalid_domain', 'domain_id must be an integer in 0..231', 'preflight')
    old_domains = [capsule.input.get('origin_domain')]
    for execution in [capsule.execution, *capsule.execution_history]:
        if execution:
            old_domains.extend(execution.get(key) for key in ('domain', 'domain_id'))
            old_domains.append((execution.get('runtime_execution_metadata') or {}).get('domain_id'))
    if domain in old_domains:
        raise _Failure('fail_environment', 'fresh_domain_required', 'replay requires a fresh domain', 'preflight')


def _check_source(capsule, current):
    expected = capsule.input['source_manifest']
    for source in (expected, current):
        if source.get('source_frozen') is not True or not source.get('head') or source.get('dirty_behavior_paths') != []:
            raise _Failure('fail_environment', 'source_not_frozen', 'source behavior is not tracked and clean at HEAD', 'preflight')
    if Path(expected.get('source_repo', '')).resolve() != REPO or canonical_hash(expected) != canonical_hash(current):
        raise _Failure('fail_environment', 'source_identity_mismatch', 'capsule source differs from the loaded frozen source', 'preflight')
    return _registered_target(capsule)


def _registered_target(capsule):
    """Resolve an exact manifest, never infer scope from a target-id suffix."""
    registry = Registry()
    identifier = capsule.input['target_id']
    required = S1Module()
    exploratory = identifier not in required.targets
    if exploratory:
        # The saved full target manifest explicitly requests a registry
        # capability. Neither a target prefix nor a runtime_config/profile can
        # grant it. Exact manifest comparison below still binds every field.
        saved = capsule.input['target']
        if saved != capsule.input['scenario'].get('target'):
            raise _Failure('fail_artifact', 'target_contract_mismatch',
                           'capsule/scenario target manifests differ; exact registered target required', 'preflight')
        capability = (saved.get('observation_contract') or {}).get('runtime_capability')
        try:
            register_exploratory(registry, include_required=False,
                                 runtime_capability=capability)
        except ValueError as exc:
            raise _Failure('fail_artifact', 'target_contract_mismatch',
                           'saved target requests an unsupported registered capability: '
                           + str(exc), 'preflight') from exc
    else:
        registry.register(required)
    if (identifier not in registry.targets
            or canonical_hash(registry.get_target(identifier).to_dict())
            != canonical_hash(capsule.input['target'])):
        raise _Failure('fail_artifact', 'target_contract_mismatch',
                       'capsule target/revision differs from registered target', 'preflight')
    return registry.get_target(identifier), registry.get_services(identifier), exploratory


_OBSERVATION_KEYS = ('observation_mode', 'fastdds_peer_instrumentation', 'observe_native_peers')
_TOPOLOGY_TARGET = 'discovery.endpoint_topology.fuzz_v1'


def _replay_observation_config(capsule):
    """Restore execution selection only after checking its original raw binding.

    Never infer from runtime_config/profile/history. No native library load.
    SHA256 supplies artifact integrity, not authenticity of an unsigned capsule.
    """
    try:
        final = capsule.input['effective_input']['final_input']
        if set(final).intersection(_OBSERVATION_KEYS):
            raise ValueError('observation_selection_is_execution_metadata_not_input')
        execution = capsule.execution or {}
        metadata = execution.get('runtime_execution_metadata') or {}
        if not isinstance(metadata, dict):
            raise ValueError('observation_runtime_metadata_invalid')
        declared = any(key in execution for key in _OBSERVATION_KEYS)
        refresh_declared = any(a.get('op') == 'update_participant_userdata'
                               for a in final.get('topology_program', {}).get('actions', []))
        hints = (declared or refresh_declared or metadata.get('observation_mode') not in (None, 'blackbox')
                 or metadata.get('fastdds_peer_instrumentation') is not None
                 or 'observe_native_peers' in metadata)
        if capsule.input['target_id'] == 'discovery.interruption_recovery' and final.get('observe_participant_leases') is True:
            from .participant_lease import replay_lease_selection
            return replay_lease_selection(capsule)
        if capsule.input['target_id'] != _TOPOLOGY_TARGET:
            if hints:
                raise ValueError('observation_selection_unsupported_target')
            return {}
        if not execution:
            return {}
        raw_metadata = None
        if execution.get('raw_path') or execution.get('raw_sha256'):
            raw = Path(execution['raw_path']).read_bytes()
            if hashlib.sha256(raw).hexdigest() != execution.get('raw_sha256'):
                raise ValueError('observation_original_raw_hash_mismatch')
            events = [json.loads(line) for line in raw.splitlines() if line.strip()]
            records = [event for event in events if event.get('event') == 'topology_execution_metadata']
            native = any(event.get('event') == 'native_peer_provider_selected'
                         or str(event.get('event', '')).startswith(('native_pdp_', 'participant_refresh_', 'topology_participant_refresh_'))
                         for event in events)
            if len(records) > 1 or ((hints or native) and len(records) != 1):
                raise ValueError('observation_raw_metadata_not_unique')
            if records:
                raw_metadata = records[0].get('metadata')
                if not isinstance(raw_metadata, dict):
                    raise ValueError('observation_raw_metadata_invalid')
                hints = (hints or native
                         or raw_metadata.get('observation_mode') not in (None, 'blackbox')
                         or raw_metadata.get('fastdds_peer_instrumentation') is not None
                         or 'observe_native_peers' in raw_metadata)
        mode = execution.get('observation_mode', 'blackbox')
        reference = execution.get('fastdds_peer_instrumentation')
        if mode not in ('blackbox', 'instrumentation'):
            raise ValueError('observation_original_mode_unsupported')
        if mode == 'blackbox' and reference is not None:
            raise ValueError('observation_blackbox_has_native_reference')
        if not hints:
            return {}
        if raw_metadata is None:
            raise ValueError('observation_original_raw_binding_required')
        # Isolated wrapper PID annotations are deliberately irrelevant here;
        # selection, sealed input and source identity are NEVER normalized away.
        for record in (metadata, raw_metadata):
            if (record.get('observation_mode', 'blackbox') != mode
                    or record.get('fastdds_peer_instrumentation') != reference
                    or canonical_hash(record.get('observe_native_peers', True))
                       != canonical_hash(execution.get('observe_native_peers', True))
                    or ('observe_native_peers' in record) != ('observe_native_peers' in execution)):
                raise ValueError('observation_execution_selection_mismatch')
        if mode == 'instrumentation' and not all(key in execution for key in ('observation_mode', 'fastdds_peer_instrumentation')):
            raise ValueError('observation_original_execution_selection_missing')
        from r2f.core.replay import validate_replay_input, _v2_execution_input
        validate_replay_input(capsule.input)
        _v2_execution_input(execution, capsule.input)
        scenario = Scenario.from_dict(capsule.input['scenario'])
        if scenario.mutation_plan is None:
            raise ValueError('observation_saved_plan_missing')
        context = {key: execution.get(key) for key in
                   ('run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat')}
        context.update(input_identity_schema=capsule.input['input_identity_schema'],
                       effective_input_sha256=capsule.input['effective_input_sha256'],
                       plan_hash=scenario.mutation_plan.plan_hash())
        if not context['run_id'] or type(context['domain_id']) is not int:
            raise ValueError('observation_original_execution_identity_missing')
        for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed', 'mutation_kind', 'carrier'):
            if canonical_hash(execution.get(key)) != canonical_hash(capsule.input[key]):
                raise ValueError('observation_original_input_identity_mismatch:' + key)
        from r2f.runtime.participant_groups import enabled
        metadata_schema = ('r2f.topology-execution-metadata/v2' if enabled(final['topology_program'])
                           else 'r2f.topology-execution-metadata/v1')
        for record in (metadata, raw_metadata):
            if (record.get('schema') != metadata_schema
                    or record.get('context_basis') != 'sealed_input_declaration_not_sut_observation'
                    or canonical_hash(record.get('sealed_execution_context')) != canonical_hash(context)
                    or record.get('raw_refs_hash_binding') != 'enclosing_result.raw_sha256'
                    or record.get('raw_path') != execution.get('raw_path')
                    or record.get('input_program_sha256') != canonical_hash(final['topology_program'])
                    or canonical_hash(record.get('source_manifest')) != canonical_hash(capsule.input['source_manifest'])):
                raise ValueError('observation_original_sealed_context_mismatch')
            for key in ('run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat'):
                if canonical_hash(record.get(key)) != canonical_hash(context[key]):
                    raise ValueError('observation_original_runtime_identity_mismatch:' + key)
        selection = {key: _copy_json(execution[key]) for key in _OBSERVATION_KEYS if key in execution}
        from r2f.instrumentation.fastdds_peer.activation import reference_from_config
        reference_from_config(dict(selection, rmw=scenario.rmw))  # file-only validation
        from r2f.instrumentation.fastdds_peer.receipt import validate_sampling_control
        validate_sampling_control(events, selection)
        from r2f.instrumentation.fastdds_peer.activation import manifest_for_reference
        from r2f.instrumentation.fastdds_pdp_trace.receipt import validate_raw
        selected_reference = reference_from_config(dict(selection, rmw=scenario.rmw))
        raw_reference = dict(selected_reference) if selected_reference else None
        if raw_reference is not None:
            raw_reference['manifest_path'] = str(Path(execution['raw_path']).resolve().parent / 'native-peer-build-manifest.json')
        validate_raw(events, manifest_for_reference(selected_reference) if selected_reference else None,
                     peer_reference=raw_reference)
        from r2f.instrumentation.fastdds_pdp_trace.refresh import validate_replay
        validate_replay(events, final['topology_program'], selected_reference, raw_path=execution['raw_path'])
        return selection
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
        raise _Failure('fail_artifact', 'observation_execution_metadata_invalid',
                       str(exc), 'preflight') from exc


def _observation_replay_declaration(capsule, config):
    selection = {key: _copy_json(config[key]) for key in _OBSERVATION_KEYS if key in config}
    return {'schema': 'r2f.replay-observation-selection/v1', 'selection': selection,
            'effective_observation_mode': selection.get('observation_mode', 'blackbox'),
            'basis': ('original_execution_and_sha256_bound_raw_metadata' if selection
                      else 'legacy_or_planned_default_blackbox'),
            'original_execution_sha256': canonical_hash(capsule.execution),
            'original_raw_sha256': (capsule.execution or {}).get('raw_sha256'),
            'input_identity_unchanged': True}


def _replay_config(capsule, replay_id, domain, source):
    """Project sealed input/plan plus verified original observation execution mode.

    The default run_pair is already the exact-id dispatcher for endpoint,
    namespace and projection backends. No original runtime_config/profile is
    loaded, no provider/mutator is rerun, and no target identity is rewritten.
    """
    target, _, exploratory = _registered_target(capsule)
    scenario = Scenario.from_dict(capsule.input['scenario'])
    effective = capsule.input['effective_input']
    final = effective['final_input']
    observation = _replay_observation_config(capsule)
    execution = dict(
        run_id=replay_id, campaign_id=replay_id,
        cell_id=f'{scenario.rmw}.{scenario.arm}.{scenario.repeat}', campaign_kind='replay',
        target_id=scenario.target_id, target_revision=scenario.target_revision,
        seed=scenario.seed, rmw=scenario.rmw, arm=scenario.arm, repeat=scenario.repeat,
        mutation_kind=scenario.mutation_kind, carrier=scenario.carrier,
        scenario=scenario.to_dict(), domain_id=domain, environment_mode='ros_overlay',
        source_repo=str(REPO), expected_head=source['head'],
        expected_target_revision=scenario.target_revision, source_manifest=source,
        final_input=_copy_json(final), effective_input_sha256=capsule.input['effective_input_sha256'],
        input_layer_receipts=_copy_json(effective['input_layer_receipts']),
        evidence_purpose='normal_conformance_replay',
        control_kind='fault' if scenario.arm == 'negative' else None)
    execution.update(observation)  # execution-only; never part of effective input
    if set(final) & set(execution):
        raise ValueError('submitted input shadows replay execution metadata: '
                         + ','.join(sorted(set(final) & set(execution))))
    if not exploratory:
        reject_execution_identity(final)
        return runtime_config(final, **execution)

    from r2f.core.evidence import INPUT_SCHEMA_V2
    from r2f.runtime.ros2_pair import (
        EXPLORATORY_TARGETS, exploratory_bindings, normalize_config, require_binding_preserved,
    )
    from .exploratory_input import runtime_input
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
    topology = scenario.target_id == TOPOLOGY_TARGET
    spec = ({"backend": "topology_pair", "carrier": "ros2_endpoint", "mutation_kind": "endpoint_program"}
            if topology and target.observation_contract.get("runtime_capability") == TOPOLOGY_CAPABILITY
            else EXPLORATORY_TARGETS.get(scenario.target_id))
    backends = {'endpoint_pair', 'discovery_namespace', 'projection_pair', 'topology_pair'}
    if scenario.target_id == 'discovery.match_compatibility.fuzz_v1' and scenario.target_revision == 1:
        backends.add('relation_pair')
    if spec is None or spec['backend'] not in backends:
        raise _Failure('fail_environment', 'unsupported_runtime_backend',
                       'registered target has no admitted replay runtime backend', 'preflight')
    if (scenario.carrier, scenario.mutation_kind) != (spec['carrier'], spec['mutation_kind']):
        raise ValueError('replay target/runtime carrier contract mismatch')
    plan = scenario.mutation_plan
    if capsule.input.get('input_identity_schema') != INPUT_SCHEMA_V2 or plan is None:
        raise ValueError('exploratory replay requires the sealed v2 mutation plan')
    # Reuse pure admission for this exact registered route. Loading a capsule
    # and normalizing its config do not establish executable plan capability.
    # This does not call prepare/mutate or regenerate input/receipts.
    from r2f.modules.s1.providers import validate_exploratory_plan
    try:
        if topology:
            from r2f.modules.s1.topology_input import validate_topology_plan, topology_input_layer_receipts
            validate_topology_plan(plan)
            if topology_input_layer_receipts(plan) != effective['input_layer_receipts']:
                raise ValueError('topology_saved_input_layers_mismatch')
            if any(op.applicability_status in {'rejected', 'blocked'} for op in plan.operations):
                raise ValueError('topology_guard_plan_non_executable')
        else:
            validate_exploratory_plan(plan, target.exploration_contract,
                                      target_id=scenario.target_id, carrier=scenario.carrier,
                                      mutation_kind=scenario.mutation_kind)
    except ValueError as exc:
        raise _Failure('fail_artifact', 'exploratory_plan_not_routable', str(exc), 'preflight') from exc
    # Use the same pure saved-plan projection as initial execution. F2's
    # logical anchor class is not a path into concrete windows[0]; every
    # affected window must bind to the saved operation's requested offset.
    from .runner import _plan_surface_bindings
    surfaces, submitted = _plan_surface_bindings(plan, target.exploration_contract)
    submitted_input = runtime_input(final, backend=spec['backend'],
                                    runtime_capability=TOPOLOGY_CAPABILITY if topology else None)
    metadata_check = _copy_json(submitted_input)
    if topology:
        # runtime_input validated the complete program and exact hash-bound
        # reference shape. This locator identifies a pre-execution build INPUT,
        # not an output execution manifest. Exempt only this validated location
        # from the transient-key check; never change the sealed input or hash.
        for action in metadata_check['topology_program']['actions']:
            if action['op'] == 'lookup_topic':
                reference = action['params'].get('native_type_support')
                if reference is not None:
                    reference.pop('manifest_path')
    reject_execution_identity(metadata_check)
    config = dict(execution, **submitted_input)
    config.update(input_identity_schema=INPUT_SCHEMA_V2, effective_input=_copy_json(effective),
                  mutation_parameters=_copy_json(scenario.mutation_parameters),
                  mutation_plan=plan.to_dict(), mutation_plan_sha256=canonical_hash(plan.to_dict()),
                  fuzz_surfaces=surfaces, submitted_parameters=submitted)
    if topology:
        from r2f.runtime.topology_route import validate_topology_route
        config.update(runtime_capability=TOPOLOGY_CAPABILITY, plan_hash=plan.plan_hash())
        validate_topology_route(config)
        return config  # no pair normalizer, defaults, provider or mutation
    if plan.runtime_windows and 'environment_windows' not in final:
        config['environment_windows'] = [
            {key: _copy_json(value) for key, value in window.items() if key != 'schema'}
            for window in plan.runtime_windows]
    normalized = normalize_config(config)
    if spec['backend'] == 'projection_pair':
        from r2f.runtime.projection_pair import normalize_projection_config
        normalized = normalize_projection_config(normalized)
    elif spec['backend'] == 'discovery_namespace':
        from r2f.runtime.namespace_pair import normalize_discovery_config
        normalized = normalize_discovery_config(normalized)
    require_binding_preserved(exploratory_bindings(normalized), normalized)
    for key, value in final.items():
        if canonical_hash(normalized.get(key)) == canonical_hash(value):
            continue
        # Match runner's scalar numeric-widening compatibility only. The
        # submitted representation (and its canonical hash) remains intact.
        observed = normalized.get(key)
        if type(value) in (int, float) and type(observed) is float and float(value) == observed:
            normalized[key] = value
        else:
            raise ValueError('runtime normalized away submitted ' + key)
    return normalized


def _bind_execution_input(row, config, capsule):
    """Record the submission boundary for new v2 replay rows, not ROS proof."""
    from r2f.core.evidence import INPUT_SCHEMA_V2
    if capsule.input.get('input_identity_schema') == INPUT_SCHEMA_V2:
        final = {key: _copy_json(config[key]) for key in capsule.input['effective_input']['final_input']}
        if canonical_hash(final) != canonical_hash(capsule.input['effective_input']['final_input']):
            raise ValueError('actual submitted replay config differs from sealed input')
        row.update(input_identity_schema=INPUT_SCHEMA_V2, final_input=final,
                   actual_submitted_payload_sha256=canonical_hash(final),
                   input_layer_receipts=_copy_json(config['input_layer_receipts']))
    for key in _OBSERVATION_KEYS:
        if key in config:
            row[key] = _copy_json(config[key])
    return row


def _topology_comparison_view(capsule, execution, *, config, result, origin, output_root):
    """Requalify a hash-bound comparison copy; never consume attachment labels.

    Both original and fresh executions use this path. The caller fixes the
    artifact directory, not a path supplied in raw or in a saved assessment.
    Qualification, ingest, provider, identity and signature gates are rerun.
    """
    from .topology_candidate_assessment import TARGET, finalize_assessment
    from r2f.core.replay import validate_replay_input, _v2_execution_input
    from r2f.reports.run_report import execution_boundary_qualification
    if capsule.input.get('target_id') != TARGET:
        return _copy_json(execution)
    validate_replay_input(capsule.input)
    _v2_execution_input(execution, capsule.input)
    if (execution.get('execution_stage') != 'judged' or execution.get('actual_run') is not True
            or 'input_terminal' in execution or execution.get('oracle_verdict') not in {'pass', 'fail'}
            or origin not in FORMAL_RUNTIMES):
        raise ValueError('topology_comparison_requires_formal_judged_outcome')
    for key in ('scenario', 'mutation_plan', 'effective_input', 'effective_input_sha256', 'source_manifest'):
        if canonical_hash(config.get(key)) != canonical_hash(capsule.input.get(key)):
            raise ValueError('topology_comparison_config_input_mismatch:' + key)
    for key in ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed'):
        if config.get(key) != execution.get(key):
            raise ValueError('topology_comparison_execution_config_mismatch:' + key)
    source = config.get('source_manifest') or {}
    if (not source.get('source_repo')
            or canonical_hash(source_manifest(Path(source['source_repo']))) != canonical_hash(source)):
        raise ValueError('topology_comparison_current_source_mismatch')
    scenario = Scenario.from_dict(capsule.input['scenario'])
    services = _registered_target(capsule)[1]
    recalculated = ingest_execution(scenario, config, result, capsule, services=services)
    for key in ('execution_stage', 'actual_run', 'abort_reason', 'execution_source', 'typed_facts',
                'oracle', 'oracle_verdict', 'rule_id', 'mutation_receipt', 'observed_effect',
                'precondition_receipts', 'observation_receipts', 'fault_control_receipt', 'neutral_receipt',
                'raw_path', 'raw_sha256', 'runtime_status', 'runtime_reason', 'runtime_execution_metadata',
                'input_terminal', 'diagnostic_oracle', 'ingestion_error', 'tool_error'):
        if canonical_hash(execution.get(key)) != canonical_hash(recalculated.get(key)):
            raise ValueError('topology_comparison_recomputed_ingest_differs:' + key)
    qualified = _qualification(origin, config, result, recalculated, output_root=output_root)
    if qualified.get('real_ros_execution') is not True or qualified.get('qualification_errors') != []:
        raise ValueError('topology_comparison_execution_not_qualified')
    boundary = 'isolated_netns' if origin == ISOLATED_RUNTIME else 'host'
    boundary_result = execution_boundary_qualification(boundary, config=config, result=result,
        row=recalculated, formal_runtime=True, output_root=output_root)
    if boundary_result.get('status') not in {'qualified', 'not_applicable'}:
        raise ValueError('topology_comparison_boundary_not_qualified')
    view = _copy_json(execution)
    # Clear stale derived denials/errors rather than granting them authority.
    # Real ingestion/tool errors were compared above and still block finalization.
    finalize_assessment(scenario, config, capsule, view, boundary=boundary_result, qualification=qualified)
    view['comparison_binding'] = {
        'schema': 'r2f.topology-replay-comparison/v1',
        'input_sha256': canonical_hash(capsule.input),
        'execution_sha256': canonical_hash(execution),
        'original_execution_sha256': canonical_hash(capsule.execution),
        'execution_history_sha256': canonical_hash(capsule.execution_history),
        'config_sha256': canonical_hash(config), 'result_sha256': canonical_hash(result),
        'qualification_sha256': canonical_hash(qualified),
        'boundary_qualification_sha256': canonical_hash(boundary_result),
        'raw_sha256': execution.get('raw_sha256'),
    }
    return view


def _checkpoint_request_for_capsule(capsule_path, action_id):
    # Pin origin layout to the capsule's case, never a saved row's root/path.
    from .topology_episode_rebuild import checkpoint_request
    case = Path(capsule_path).resolve().parent
    return checkpoint_request(case / 'runtime_config.json',
                              case / 'runtime' / 'runtime_result.json', action_id)


def _original_topology_context(capsule_path):
    """Pinned original runner layout; saved row/raw paths cannot select roots."""
    case = Path(capsule_path).resolve().parent
    report_path = case.parents[1] / 'report.json' if case.parent.name == 'cases' else case / 'report.json'
    report = _json(report_path)
    boundary = report.get('runtime_boundary', 'host')
    if boundary not in {'host', 'isolated_netns'}:
        raise ValueError('topology_comparison_original_boundary_invalid')
    formal = report.get('formal_runtime', report.get('runtime_formal')) is True
    origin = (ISOLATED_RUNTIME if boundary == 'isolated_netns' else DEFAULT_RUNTIME) if formal else 'injected_runtime'
    return dict(config=_json(case / 'runtime_config.json'),
                result=_json(case / 'runtime' / 'runtime_result.json'),
                origin=origin, output_root=case / 'runtime'), [
                    case / 'runtime_config.json', case / 'runtime' / 'runtime_result.json', report_path]


def _replay_comparison_view(capsule, execution, *, original, capsule_path, origin, context):
    from .topology_candidate_assessment import TARGET
    if capsule.input.get('target_id') != TARGET:
        return _copy_json(execution)
    if original:
        arguments, _ = _original_topology_context(capsule_path)
    else:
        arguments = dict(config=context['config'], result=context['runtime_result'],
                         origin=origin, output_root=context['output_root'])
    return _topology_comparison_view(capsule, execution, **arguments)


def _worker_counts(config):
    """Expected process instances from the exact runtime backend and input."""
    from r2f.runtime.ros2_pair import EXPLORATORY_TARGETS
    backend = EXPLORATORY_TARGETS.get(config.get('target_id'), {}).get('backend')
    if config.get('target_id') == 'builtin.discovery_projection_reconciliation':
        backend = 'projection_pair'
    elif config.get('target_id') == 'discovery.interruption_recovery':
        backend = 'discovery_namespace'
    if backend == 'projection_pair':
        from r2f.runtime.projection_pair import projection_roles
        return Counter(projection_roles(config))
    counts = Counter({'writer': 1, 'reader': 1})
    if backend == 'discovery_namespace' and config.get('arm') == 'mutated':
        from r2f.runtime.namespace_pair import window_churn_schedule
        windows = config.get('runtime_windows') or [{'drop_duration_sec': config['drop_duration_sec']}]
        for window in windows:
            for action in window_churn_schedule(config, window['drop_duration_sec']):
                if action['kind'] in {'reader_create', 'writer_create'}:
                    counts[action['role']] += 1
            if config.get('discovery_probe') is True:
                counts['probe'] += 1
    return counts


def _worker_qualification(config, metadata, events):
    """Bind every generation to exactly one raw start/endpoint/exit triple."""
    from r2f.runtime.ros2_pair import role_rmw
    errors = []
    workers = metadata.get('workers') or []
    counts = _worker_counts(config)
    if (Counter(w.get('role') for w in workers) != counts
            or len({w.get('pid') for w in workers}) != len(workers)):
        errors.append('independent_worker_metadata_missing')
    linked = set()
    for role, count in counts.items():
        generations = [worker.get('worker_generation') for worker in workers if worker.get('role') == role]
        if (any(generation is not None for generation in generations)
                and (any(type(generation) is not int for generation in generations)
                     or sorted(generations) != list(range(1, count + 1)))):
            errors.append('worker_generation_sequence_invalid:' + str(role))
    identities, endpoints = set(), set()
    for worker in workers:
        role, pid = worker.get('role'), worker.get('pid')
        generation = worker.get('worker_generation')
        identity = (role, pid, generation)
        if identity in identities or (generation is not None and (type(generation) is not int or generation < 1)):
            errors.append('worker_generation_invalid:' + str(role))
        identities.add(identity)
        # Historical two-worker receipts may predate worker_generation; a
        # same-role restart cannot use that compatibility branch.
        if generation is None and counts.get(role, 0) > 1:
            errors.append('worker_generation_missing:' + str(role))
        if (type(pid) is not int or pid <= 0 or type(worker.get('returncode')) is not int
                or worker['returncode'] != 0 or worker.get('fresh_process') is not True
                or worker.get('separate_context') is not True):
            errors.append('worker_health_receipt_missing:' + str(role))
        links = []
        for kind in ('process_started', 'endpoint_created', 'process_exit'):
            found = [(index, event) for index, event in enumerate(events)
                     if event.get('event') == kind and event.get('role') == role
                     and event.get('pid') == pid
                     and event.get('worker_generation') == generation]
            if len(found) != 1:
                errors.append('worker_raw_link_missing:' + str(role) + ':' + kind)
            else:
                index, event = found[0]
                links.append(index)
                linked.add(index)
                if kind == 'endpoint_created':
                    endpoint = (event.get('endpoint_gid'), event.get('endpoint_generation'))
                    if (not all(isinstance(value, str) and value for value in endpoint)
                            or endpoint in endpoints):
                        errors.append('worker_endpoint_identity_invalid:' + str(role))
                    endpoints.add(endpoint)
                    if event.get('rmw_identifier') != role_rmw(config, role):
                        errors.append('worker_rmw_identity_mismatch:' + str(role))
                elif kind == 'process_exit' and event.get('returncode') != worker.get('returncode'):
                    errors.append('worker_exit_receipt_mismatch:' + str(role))
        if len(links) == 3 and links != sorted(links):
            errors.append('worker_raw_chronology_mismatch:' + str(role))
    expected_links = {index for index, event in enumerate(events)
                      if event.get('event') in {'process_started', 'endpoint_created', 'process_exit'}}
    if linked != expected_links:
        errors.append('worker_raw_identity_unaccounted')
    return errors


def _topology_worker_role_matches(config, worker, token):
    """Bind the endpoint kind and exact worker alias independently.

    Use the producer's declaration-order, four-per-kind alias assignment, not
    a prefix/digit heuristic (worker_role_kind alone also parses higher digits).
    Explicit raw alias fields must agree; none may mask another via fallback.
    """
    from r2f.runtime.topology_worker import worker_role_for, worker_role_kind

    program = config.get('topology_program')
    if not isinstance(program, dict) or not isinstance(program.get('endpoints'), list):
        return False
    bindings, ordinals = {}, {'writer': 0, 'reader': 0}
    for endpoint in program['endpoints']:
        if not isinstance(endpoint, dict):
            return False
        logical_id, kind = endpoint.get('logical_id'), endpoint.get('role')
        if (not isinstance(logical_id, str) or not logical_id or logical_id in bindings
                or kind not in ('writer', 'reader')):
            return False
        try:
            alias = worker_role_for(kind, ordinals[kind])
        except ValueError:
            return False
        bindings[logical_id] = (kind, alias)
        ordinals[kind] += 1
    logical_id = token.get('logical_id')
    if not isinstance(logical_id, str) or logical_id not in bindings:
        return False
    kind, alias = bindings[logical_id]
    return (worker.get('endpoint_role') == kind
            and worker_role_kind(alias) == kind
            and worker.get('role') == alias
            and token.get('role') == alias
            and any(key in token for key in ('worker_role', 'topology_worker_role'))
            and all(token[key] == alias for key in ('worker_role', 'topology_worker_role', 'role')
                    if key in token))


def _topology_worker_errors(config, result):
    """Verify N-generation worker references against raw, never a pair count.

    PID/PGID values are compared only inside their recorded runtime scope. The
    isolated helper binds that scope; no inner identifier is treated as a host PID.
    """
    from r2f.runtime.participant_groups import enabled, project_lineage
    if enabled(config.get('topology_program')):
        try:
            expected = project_lineage(config['topology_program'], result.get('events') or [],
                                       result.get('raw_path'), config=config)
            metadata = result.get('execution_metadata') or {}
            if (metadata.get('schema') != 'r2f.topology-execution-metadata/v2'
                    or metadata.get('participant_groups') != expected
                    or metadata.get('workers') != expected['workers']):
                raise ValueError('group_metadata_raw_projection_mismatch')
            if any(w['unknowns'] or not w['real_ros'] for w in expected['workers']):
                raise ValueError('group_fixture_not_actual_worker')
            return []
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            return ['topology_group_lineage_invalid:' + str(exc)]
    if ((result.get('execution_metadata') or {}).get('schema') == 'r2f.topology-execution-metadata/v2'
            or 'participant_groups' in (result.get('execution_metadata') or {})):
        return ['topology_group_metadata_without_sealed_opt_in']
    from r2f.runtime.endpoint_incarnation import endpoint_identity_at

    errors, seen, identities, gids = [], set(), set(), set()
    events = result.get('events') or []
    metadata = result.get('execution_metadata') or {}
    workers = metadata.get('workers')
    tokens = [e for e in events if e.get('event') == 'endpoint_created']
    if not isinstance(workers, list) or len(workers) != len(tokens):
        return ['topology_worker_metadata_count_mismatch']
    def refs(value, kinds):
        if not isinstance(value, list):
            raise ValueError('worker_refs_not_list')
        rows = []
        for ref in value:
            if (not isinstance(ref, dict) or set(ref) != {'raw_path', 'event_index'}
                    or ref['raw_path'] != result.get('raw_path')
                    or type(ref['event_index']) is not int or not 0 <= ref['event_index'] < len(events)):
                raise ValueError('worker_ref_identity_invalid')
            event = events[ref['event_index']]
            if event.get('event') not in kinds:
                raise ValueError('worker_ref_event_kind_invalid')
            rows.append(event)
        if len({e['event_index'] for e in rows}) != len(rows):
            raise ValueError('worker_ref_duplicate')
        return rows
    for worker in workers:
        try:
            token_rows = refs([worker['token_raw_ref']], {'endpoint_created'})
            token = token_rows[0]
            if token['event_index'] in seen:
                raise ValueError('worker_generation_duplicate')
            seen.add(token['event_index'])
            identity = (token.get('pid'), token.get('process_start_ticks'))
            gid = token.get('endpoint_gid')
            if (any(type(token.get(k)) is not int or token[k] < 1 for k in ('pid', 'pgid', 'process_start_ticks', 'generation'))
                    or identity in identities or not isinstance(gid, str) or not gid or gid in gids):
                raise ValueError('worker_process_or_gid_reused_or_invalid')
            identities.add(identity)
            gids.add(gid)
            for key in ('logical_id', 'generation', 'endpoint_gid', 'endpoint_generation',
                        'pid', 'pgid', 'process_start_ticks', 'worker_generation'):
                if worker.get(key) is None or worker.get(key) != token.get(key):
                    raise ValueError('worker_token_identity_mismatch:' + key)
            if not _topology_worker_role_matches(config, worker, token):
                raise ValueError('worker_role_mismatch')
            if (worker.get('fixture') is not False or worker.get('real_ros') is not True
                    or token.get('real_ros') is not True or token.get('backend') != 'ros2_public_api'):
                raise ValueError('worker_fixture_not_real')
            if (worker.get('rmw_identifier') != token.get('rmw_identifier')
                    or worker.get('rmw') != token.get('rmw_identifier')
                    or token.get('rmw_identifier') != config.get('rmw')
                    or worker.get('domain_id') != token.get('observed_domain_id')
                    or token.get('observed_domain_id') != config.get('domain_id')
                    or worker.get('execution_domain_id') != config.get('domain_id')
                    or not worker.get('domain_observation_source')):
                raise ValueError('worker_actual_domain_or_rmw_mismatch')
            lineage = [e for e in events if e.get('pid') == token['pid']
                       and e.get('logical_id', e.get('topology_logical_id')) == token['logical_id']
                       and e.get('generation', e.get('topology_generation')) == token['generation']]
            starts = refs(worker.get('start_raw_refs'), {'process_started'})
            exits = refs(worker.get('exit_raw_refs'), {'process_exit', 'worker_exit', 'topology_worker_exit'})
            cleanup = refs(worker.get('cleanup_raw_refs'), {'endpoint_destroyed', 'topology_endpoint_destroyed'})
            for actual, kinds in ((starts, {'process_started'}), (exits, {'process_exit', 'worker_exit', 'topology_worker_exit'}),
                                  (cleanup, {'endpoint_destroyed', 'topology_endpoint_destroyed'})):
                if actual != [e for e in lineage if e.get('event') in kinds]:
                    raise ValueError('worker_refs_do_not_cover_actual_lineage')
            if (len(starts) != 1 or sum(e.get('event') == 'process_exit' for e in exits) != 1
                    or starts[0].get('fresh_process') is not True or starts[0].get('separate_context') is not True
                    or worker.get('fresh_process') is not True or worker.get('separate_context') is not True):
                raise ValueError('worker_fresh_context_or_lifecycle_missing')
            if (not starts[0]['event_index'] < token['event_index']
                    or any(e['event_index'] <= token['event_index'] or e.get('returncode') != 0 for e in exits)
                    or worker.get('closed') is not True or worker.get('returncode') != 0
                    or worker.get('cleanup_status') != 'closed' or worker.get('unknowns') != []):
                raise ValueError('worker_cleanup_or_exit_invalid')
            # Creation/metadata endpoint_gid remains the immutable origin. Only
            # independently authenticated raw transitions authorize a new GID.
            current = endpoint_identity_at(events, token)
            transitions = [e for e in events if e.get('event') == 'endpoint_identity_transition'
                           and e.get('logical_id') == token['logical_id']
                           and e.get('generation') == token['generation']]
            incarnation_fields = ('current_endpoint_gid', 'incarnation_ordinal', 'identity_transition_raw_refs')
            if transitions or any(key in worker for key in incarnation_fields):
                if not all(key in worker for key in incarnation_fields):
                    raise ValueError('worker_incarnation_metadata_incomplete')
                if worker['current_endpoint_gid'] != current['endpoint_gid']:
                    raise ValueError('worker_current_endpoint_gid_mismatch')
                if (type(worker['incarnation_ordinal']) is not int
                        or worker['incarnation_ordinal'] != current['incarnation_ordinal']):
                    raise ValueError('worker_incarnation_ordinal_mismatch')
                transition_rows = refs(worker['identity_transition_raw_refs'], {'endpoint_identity_transition'})
                if transition_rows != transitions:
                    raise ValueError('worker_incarnation_refs_do_not_cover_actual_lineage')
            for event in exits:
                # The prefix is exclusive; +1 includes this exit, never a later
                # transition/ACK. Origin index/time and token refs stay intact.
                at_exit = endpoint_identity_at(events, token, before_index=event['event_index'] + 1)
                if at_exit['incarnation_ordinal']:
                    if (type(event.get('monotonic_ns')) is not int
                            or event['monotonic_ns'] < at_exit['identity_valid_after_ns']
                            or event['event_index'] <= at_exit['identity_valid_after_ref']):
                        raise ValueError('worker_exit_before_identity_ack')
                if (current['incarnation_ordinal'] != at_exit['incarnation_ordinal']
                        or current['endpoint_gid'] != at_exit['endpoint_gid']):
                    raise ValueError('worker_identity_transition_after_exit')
                # Public worker_exit omits PGID; process_exit and the
                # topology_worker_exit writer receipt bind their actual PGID.
                keys = ('process_start_ticks', 'endpoint_generation', 'endpoint_gid') + (
                    ('pgid',) if event.get('event') == 'process_exit' or 'pgid' in event else ())
                for key in keys:
                    if event.get(key) != at_exit.get(key):
                        raise ValueError('worker_exit_token_mismatch:' + key)
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            errors.append('topology_worker_lineage_invalid:' + str(exc))
    if seen != {e['event_index'] for e in tokens}:
        errors.append('topology_worker_generation_coverage_mismatch')
    return errors


def _topology_context_errors(config, result, *, output_root, audited_pid_scope=None):
    """Common execution context gates, without legacy two-role assumptions."""
    errors = []
    metadata = result.get('execution_metadata') or {}
    if config.get('observation_mode') == 'instrumentation':
        for key in _OBSERVATION_KEYS:
            if canonical_hash(metadata.get(key)) != canonical_hash(config.get(key)):
                errors.append('execution_metadata_observation_selection_mismatch:' + key)
    for key in ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'domain_id'):
        if canonical_hash(metadata.get(key)) != canonical_hash(config.get(key)):
            errors.append('execution_metadata_identity_mismatch:' + key)
    context_keys = ('run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat',
                    'input_identity_schema', 'effective_input_sha256', 'plan_hash')
    if (metadata.get('sealed_execution_context') != {k: config.get(k) for k in context_keys}
            or metadata.get('context_basis') != 'sealed_input_declaration_not_sut_observation'):
        errors.append('execution_metadata_sealed_context_mismatch')
    # The raw/disk metadata is inner-scope. Only annotations added by the
    # independently audited isolated wrapper are omitted for this comparison.
    raw_metadata = _copy_json(metadata)
    if audited_pid_scope is not None:
        for item in [raw_metadata, *(raw_metadata.get('workers') or [])]:
            if (item.get('pid_namespace') == audited_pid_scope
                    and item.get('pid_interpretation') == 'namespace_PID_not_host_PID'):
                item.pop('pid_namespace', None)
                item.pop('pid_interpretation', None)
    recorded = [e for e in result.get('events', []) if e.get('event') == 'topology_execution_metadata']
    if len(recorded) != 1 or canonical_hash(recorded[0].get('metadata')) != canonical_hash(raw_metadata):
        errors.append('raw_execution_metadata_mismatch')
    try:
        if output_root is None or canonical_hash(_json(Path(output_root) / 'execution_metadata.json')) != canonical_hash(raw_metadata):
            errors.append('persisted_execution_metadata_mismatch')
    except (OSError, ValueError, KeyError, TypeError):
        errors.append('persisted_execution_metadata_missing_or_invalid')
    source = config.get('source_manifest') or {}
    if canonical_hash(metadata.get('source_after')) != canonical_hash(source):
        errors.append('execution_metadata_source_after_mismatch')
    if canonical_hash(metadata.get('source_manifest')) != canonical_hash(source):
        errors.append('execution_metadata_source_manifest_mismatch')
    program = (config.get('final_input') or {}).get('topology_program')
    if metadata.get('input_program_sha256') != canonical_hash(program):
        errors.append('execution_metadata_program_identity_mismatch')
    if (metadata.get('declared_domain_id') != config.get('domain_id')
            or metadata.get('declared_rmw') != config.get('rmw')):
        errors.append('execution_metadata_declared_context_mismatch')
    env = result.get('environment')
    if not isinstance(env, dict):
        env = {}
        errors.append('environment_manifest_not_object')
    try:
        environment_path = Path(output_root) / 'environment.json'
        reference = metadata.get('environment_manifest') or {}
        if not isinstance(reference, dict):
            raise ValueError('environment_manifest_reference_not_object')
        if (reference.get('path') != str(environment_path)
                or reference.get('sha256') != raw_sha256(environment_path)):
            errors.append('execution_metadata_environment_reference_mismatch')
    except (OSError, ValueError, KeyError, TypeError):
        errors.append('execution_metadata_environment_reference_invalid')
    try:
        if output_root is None or canonical_hash(_json(Path(output_root) / 'environment.json')) != canonical_hash(env):
            errors.append('persisted_environment_manifest_differs')
    except (OSError, ValueError, KeyError, TypeError):
        errors.append('persisted_environment_manifest_missing_or_invalid')
    preflights = [e for e in result.get('events', []) if e.get('event') == 'environment_preflight']
    if (len(preflights) != 1 or preflights[0].get('ok') is not True
            or canonical_hash(preflights[0].get('environment')) != canonical_hash(env)):
        errors.append('raw_environment_preflight_manifest_mismatch')
    if (env.get('ok') is not True or env.get('environment_mode') != 'ros_overlay'
            or env.get('ros_distro') != 'jazzy'):
        errors.append('environment_manifest_invalid')
    if (env.get('domain_id') != config.get('domain_id')
            or env.get('target_revision') != config.get('target_revision')):
        errors.append('environment_execution_context_mismatch')
    if any((env.get('source') or {}).get(k) != source.get(k) for k in ('head', 'source_repo')):
        errors.append('environment_source_identity_mismatch')
    if canonical_hash(metadata.get('source_identity')) != canonical_hash(env.get('source')):
        errors.append('execution_metadata_environment_source_mismatch')
    if (env.get('rmw') or {}).get(config.get('rmw'), {}).get('available') is not True:
        errors.append('rmw_not_available_in_manifest')
    if (not env.get('dependency_lock_sha256') or (env.get('process_scan') or {}).get('ok') is not True
            or (env.get('old_lock_scan') or {}).get('ok') is not True):
        errors.append('preflight_receipts_missing')
    return errors


def _qualification(origin, config, result, row, *, output_root=None):
    """Use the report auditor's runtime/environment gates plus worker raw links.

    This is evidence validation, not cryptographic attestation of process origin.
    A failed preflight can be an actual default-runtime attempt without being an
    actual ROS execution: these two qualifications deliberately remain separate.
    """
    errors = []
    # The explicit caller-owned output directory is the containment anchor;
    # never infer it from manifest/raw paths supplied by the result.
    isolated = (origin == ISOLATED_RUNTIME or isinstance(config.get('execution_boundary'), dict)
                and config['execution_boundary'].get('enabled') is True
                or 'execution_boundary' in result
                or any(isinstance(item, dict) and item.get('pid_interpretation') == 'namespace_PID_not_host_PID'
                       for item in (result.get('execution_metadata'), result.get('execution'),
                                    row.get('runtime_execution_metadata'))))
    boundary_audit = None
    if isolated:
        from .boundary_audit import audit_execution_boundary
        if origin != ISOLATED_RUNTIME:
            errors.append('boundary_route_context_mismatch')
        if output_root is None:
            errors.append('boundary_output_root_missing')
        else:
            boundary_audit = audit_execution_boundary(config, result, output_root=output_root)
            if boundary_audit.get('valid') is not True:
                errors.append('execution_boundary_artifact_audit_failed')
        if result.get('runtime_status') != 'completed' or result.get('status') == 'failed':
            errors.append('boundary_runtime_not_completed')
        source = config.get('source_manifest') or {}
        if (source.get('source_frozen') is not True or source.get('dirty_behavior_paths') != []
                or not source.get('head') or not source.get('behavior_tree_sha256')):
            errors.append('frozen_source_binding_missing')
        context = result.get('execution_boundary') or {}
        scope = context.get('raw_pid_scope')
        metadata = result.get('execution_metadata') or {}
        # These are namespace PID annotations, not a claim that inner PID is
        # a host PID. Raw worker linking below stays within this actual scope.
        for item in [metadata, *(metadata.get('workers') or [])]:
            if (not scope or item.get('pid_namespace') != scope
                    or item.get('pid_interpretation') != 'namespace_PID_not_host_PID'):
                errors.append('worker_pid_namespace_scope_missing_or_mismatched')
                break
    if origin not in FORMAL_RUNTIMES:
        errors.append('injected_runtime_is_offline_contract_only')
    if result.get('evidence_origin') in _OFFLINE:
        errors.append('offline_fixture_cannot_establish_real_ros')
    raw_ok = False
    try:
        if result.get('raw_path') and result.get('raw_sha256'):
            events = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
            raw_ok = (raw_sha256(result['raw_path']) == result['raw_sha256']
                      and canonical_hash(events) == canonical_hash(result.get('events'))
                      and bool(events) and all(e.get('event_index') == i and type(e.get('event_index')) is int
                          and e.get('run_id') == config.get('run_id') and e.get('domain_id') == config.get('domain_id')
                          for i, e in enumerate(events)))
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    if not raw_ok:
        errors.append('runtime_raw_missing_or_invalid')
    if result.get('real_execution') is False and result.get('runtime_status') == 'blocked':
        raw_ok = raw_ok and row.get('environment_raw_evidence', {}).get('verified') is True
    actual_attempt = (origin in FORMAL_RUNTIMES
                      and result.get('evidence_origin') not in _OFFLINE and raw_ok)
    if isolated and 'frozen_source_binding_missing' in errors:
        actual_attempt = False
    if result.get('real_execution') is not True or row.get('actual_run') is not True or row.get('execution_stage') != 'judged':
        errors.append('real_judged_execution_missing')
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
    if config.get('target_id') == TOPOLOGY_TARGET:
        from .runner import _runtime_execution_config, runtime_for_boundary
        boundary = 'isolated_netns' if isolated else 'host'
        expected_origin = ISOLATED_RUNTIME if isolated else DEFAULT_RUNTIME
        # Origin is supplied by the existing canonical callable/report auditor,
        # not inferred from this execution-only flag. This is a consistency
        # check, never an origin attestation or a replacement for runtime gates.
        canonical = runtime_for_boundary(boundary) if origin == expected_origin else None
        expected_config = _runtime_execution_config(config, execute=True, runtime=canonical,
                                                     runtime_boundary=boundary)
        if canonical_hash(expected_config) != canonical_hash(config):
            errors.append('topology_execution_intent_origin_mismatch')
        topology_root = output_root
        # Only our successful strict audit can supply this view. Never accept
        # a caller-provided inner root/result or infer a root from a raw path.
        view = (boundary_audit or {}).get('inner_artifact_view') if isolated else None
        if view is not None and boundary_audit.get('valid') is True:
            try:
                fixed_root = Path(output_root).resolve() / 'inner-runtime'
                reference = view['result_ref']
                fixed_result = fixed_root / 'topology-namespace/child-runtime-result.json'
                if (view.get('schema') != 'r2f.audited-topology-inner-view/v1'
                        or view.get('output_root') != str(fixed_root)
                        or reference.get('path') != str(fixed_result)
                        or reference.get('kind') != 'json'
                        or not fixed_result.resolve(strict=True).is_relative_to(fixed_root)):
                    raise ValueError('invalid audited inner view')
                data = fixed_result.read_bytes()
                if hashlib.sha256(data).hexdigest() != reference.get('sha256'):
                    raise ValueError('audited child result changed')
                child = json.loads(data)
                if not isinstance(child, dict) or 'execution_boundary' in child or 'topology_namespace' in child:
                    raise ValueError('child is not unannotated runtime result')
                # Outer persisted wrapper and annotations were compared by the
                # audit. All following semantic gates consume original child
                # bytes, with the ORIGINAL input/plan/hash config unchanged.
                result, topology_root = child, fixed_root
            except (OSError, ValueError, KeyError, TypeError):
                errors.append('topology_audited_inner_view_invalid')
        audited_scope = None
        if (isolated and boundary_audit and boundary_audit.get('valid') is True
                and 'execution_boundary' in result):
            audited_scope = result['execution_boundary'].get('raw_pid_scope')
        errors.extend(_topology_context_errors(config, result, output_root=topology_root,
                                              audited_pid_scope=audited_scope))
        errors.extend(_topology_worker_errors(config, result))
        source = config.get('source_manifest') or {}
        if (source.get('source_frozen') is not True or source.get('dirty_behavior_paths') != []
                or not source.get('head') or not source.get('behavior_tree_sha256')):
            errors.append('topology_frozen_source_binding_missing')
        if not topology_root:
            errors.append('topology_output_root_missing')
        else:
            try:
                root = Path(topology_root).resolve()
                if (Path(result['raw_path']).resolve().parent != root
                        or canonical_hash(_json(root / 'runtime_result.json')) != canonical_hash(result)):
                    errors.append('topology_runtime_artifact_root_or_result_mismatch')
            except (OSError, ValueError, KeyError, TypeError):
                errors.append('topology_runtime_artifact_missing')
        # Recompute N-endpoint evidence; pair-count/GID gates are not a topology
        # proof. The independent producer audits every raw worker lineage,
        # canonical step, source/environment/locks, cleanup and state boundary.
        from .topology_adapter import facts_from_events
        facts = facts_from_events(result.get('events', []), result, config)
        for field in ('diagnostic_valid', 'execution_evidence', 'source_provenance_qualified',
                      'environment_healthy', 'process_healthy', 'preconditions_reached',
                      'endpoint_identity_complete'):
            if facts.get(field) is not True:
                errors.append('topology_' + field + '_missing_or_invalid')
        if facts.get('fixture') is not False:
            errors.append('topology_fixture_cannot_establish_real_execution')
        if result.get('runtime_status') != 'completed' or result.get('status') != 'executed':
            errors.append('topology_runtime_not_completed')
        cleanups = [e for e in result.get('events', []) if e.get('event') == 'topology_cleanup']
        cleanup = result.get('cleanup_receipt') or {}
        if (cleanup.get('closed') is not True or cleanup.get('survivors') not in (None, [])
                or cleanup.get('errors') not in (None, []) or len(cleanups) != 1
                or canonical_hash(cleanups[0].get('receipt')) != canonical_hash(cleanup)):
            errors.append('topology_cleanup_not_verified')
        qualified = {'execution_origin': origin, 'real_runtime_attempt': actual_attempt,
                     'real_ros_execution': not errors, 'qualification_errors': errors}
        if isolated:
            qualified['boundary_audit'] = boundary_audit
        return qualified
    metadata = result.get('execution_metadata') or {}
    for key in ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'domain_id'):
        if canonical_hash(metadata.get(key)) != canonical_hash(config.get(key)):
            errors.append('execution_metadata_identity_mismatch:' + key)
    env = result.get('environment', {})
    try:
        environment_path = Path(result['raw_path']).parent / 'environment.json'
        if canonical_hash(_json(environment_path)) != canonical_hash(env):
            errors.append('persisted_environment_manifest_differs')
    except (OSError, ValueError, KeyError, TypeError):
        errors.append('persisted_environment_manifest_missing_or_invalid')
    if env.get('ok') is not True or env.get('environment_mode') != 'ros_overlay' or env.get('ros_distro') != 'jazzy':
        errors.append('environment_manifest_invalid')
    expected_source = config.get('source_manifest', {})
    if any(env.get('source', {}).get(k) != expected_source.get(k) for k in ('head', 'source_repo')):
        errors.append('environment_source_identity_mismatch')
    from r2f.runtime.ros2_pair import role_rmw
    vendors = {config.get('rmw')} | {role_rmw(config, role) for role in _worker_counts(config)}
    if any(env.get('rmw', {}).get(vendor, {}).get('available') is not True for vendor in vendors):
        errors.append('rmw_not_available_in_manifest')
    if not env.get('dependency_lock_sha256') or env.get('process_scan', {}).get('ok') is not True or env.get('old_lock_scan', {}).get('ok') is not True:
        errors.append('preflight_receipts_missing')
    errors.extend(_worker_qualification(config, metadata, result.get('events', [])))
    qualification = {'execution_origin': origin, 'real_runtime_attempt': actual_attempt,
                     'real_ros_execution': not errors, 'qualification_errors': errors}
    if isolated:
        qualification['boundary_audit'] = boundary_audit
    return qualification


def replay_cell(capsule_path, *, output_dir, domain_id, runtime=None,
                runtime_boundary='host', episode_checkpoint=None) -> dict:
    """Replay an unchanged capsule into a fresh evidence directory and domain.

    output_dir must be empty and outside the original run. Source must match the
    capsule and be frozen even for an injected runtime; offline tests mock source
    observation rather than weakening that gate. Runtime owns leases/preflight.
    """
    capsule_path, output = Path(capsule_path).resolve(), Path(output_dir).resolve()
    if runtime_boundary not in ('host', 'isolated_netns'):
        raise ValueError('unsupported runtime_boundary: ' + repr(runtime_boundary))
    from .runner import _resolve_execution_runtime, _runtime_execution_config
    runtime, formal = _resolve_execution_runtime(runtime, runtime_boundary)
    origin = (ISOLATED_RUNTIME if runtime_boundary == 'isolated_netns' else DEFAULT_RUNTIME) \
        if formal else 'injected_runtime'
    _reserve(capsule_path, output)
    replay_id = 'replay-' + uuid.uuid4().hex
    request = {'schema': SCHEMA, 'replay_id': replay_id, 'capsule_path': str(capsule_path),
               'domain_id': domain_id, 'execution_origin': origin,
               'runtime_boundary': runtime_boundary, 'timestamp': utc_now()}
    if episode_checkpoint is not None:
        if not isinstance(episode_checkpoint, str) or not episode_checkpoint.strip():
            raise ValueError('episode_checkpoint must be a nonempty action id')
        request['episode_checkpoint'] = episode_checkpoint
    _write(output / 'request.json', request)
    report = dict(request, report_path=str(output / 'replay_report.json'),
                  formal_runtime=formal,
                  original_artifacts=[], original={},
                  runtime_invoked=False, capsule_loaded=False, maturity_denominator_changed=False)
    capsule, attempt = None, None
    context = {}
    phase = 'original_artifacts'
    try:
        reference = _reference(capsule_path)
        report['original_artifacts'].append(reference)
        original_bytes = capsule_path.read_bytes()
        (output / 'original_capsule.json').write_bytes(original_bytes)
        capsule = ReplayCapsule.from_dict(json.loads(original_bytes))
        report['capsule_loaded'] = True
        report['original'] = {
            'capsule_sha256': hashlib.sha256(original_bytes).hexdigest(),
            'input_sha256': canonical_hash(capsule.input),
            'effective_input_sha256': capsule.input['effective_input_sha256'],
            'target_sha256': canonical_hash(capsule.input['target']),
            'source_sha256': canonical_hash(capsule.input['source_manifest']),
            'execution_sha256': canonical_hash(capsule.execution),
            'execution_identity': {key: (capsule.execution or {}).get(key) for key in
                                   ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'raw_path', 'raw_sha256')},
        }
        _write(output / 'input_metadata.json', capsule.input)
        paths = [capsule_path.parent / 'runtime_config.json', capsule_path.parent / 'report.json']
        if capsule_path.parent.parent.name == 'cells':
            paths.append(capsule_path.parents[2] / 'report.json')
        for path in paths:
            if path.is_file():
                report['original_artifacts'].append(_reference(path))
        for execution in [capsule.execution, *capsule.execution_history]:
            if execution and (execution.get('raw_path') or execution.get('raw_sha256')):
                ref = _reference(execution['raw_path'])
                report['original_artifacts'].append(ref)
                if not ref['available'] or ref['sha256'] != execution.get('raw_sha256'):
                    raise _Failure('fail_artifact', 'original_raw_invalid', 'original raw artifact missing or SHA256 differs', phase)
        if (capsule.input.get('target_id') == 'discovery.endpoint_topology.fuzz_v1'
                and capsule.execution is not None):
            _, comparison_paths = _original_topology_context(capsule_path)
            for path in comparison_paths:
                if not any(ref['path'] == str(path) for ref in report['original_artifacts']):
                    report['original_artifacts'].append(_reference(path))
        phase = 'preflight'
        _check_domain(capsule, domain_id)
        before = source_manifest(REPO)
        context['source_before'] = before
        _write(output / 'source_before.json', before)
        _check_source(capsule, before)
        def runner(scenario, domain):
            try:
                config = _runtime_execution_config(
                    _replay_config(capsule, replay_id, domain, before), execute=True,
                    runtime=runtime, runtime_boundary=runtime_boundary)
                if episode_checkpoint is not None:
                    if capsule.input['target_id'] != _TOPOLOGY_TARGET:
                        raise ValueError('episode_checkpoint requires topology episode target')
                    config['episode_rebuild'] = _checkpoint_request_for_capsule(capsule_path, episode_checkpoint)
                    from .topology_episode_rebuild import load_checkpoint
                    loaded = load_checkpoint(config['episode_rebuild'], config['topology_program'], config)
                    dependencies = [config['episode_rebuild'][key]['path']
                                    for key in ('origin_config', 'origin_runtime', 'progress')]
                    dependencies += [r['observation']['prefix']['path'] for r in loaded['progress']['receipts']]
                    dependencies += [loaded['origin_raw']['path']]
                    dependencies += [e['initial_prefix']['path'] for e in loaded['origin_events']
                                     if e.get('event') == 'topology_episode_begin']
                    for path in dependencies:
                        if not any(ref['path'] == str(path) for ref in report['original_artifacts']):
                            report['original_artifacts'].append(_reference(path))
                context['config'] = config
                if capsule.input['target_id'] == _TOPOLOGY_TARGET:
                    report['observation_replay'] = _observation_replay_declaration(capsule, config)
                _write(output / 'runtime_config.json', config)
                runtime_dir = output / 'runtime'
                runtime_dir.mkdir()
                context['output_root'] = runtime_dir
                _, services, _ = _registered_target(capsule)
                if not services.carrier.accepts(scenario):
                    raise ValueError('registered carrier does not accept replay scenario')

                def invoke(config, directory):
                    # This flag records the actual runtime boundary, including
                    # a runtime exception. Carrier refusal never sets it.
                    report['runtime_invoked'] = True
                    context['submitted_config'] = _copy_json(config)
                    return runtime(config, directory)

                try:
                    result = services.carrier.apply(scenario, _copy_json(capsule.input['effective_input']),
                                                    execution={'config': config, 'output_dir': runtime_dir,
                                                               'runtime': invoke})
                except Exception as exc:
                    context['runtime_exception'] = type(exc).__name__
                    result = {'real_execution': False, 'runtime_status': 'tool_error',
                              'reason': f'{type(exc).__name__}: {exc}', 'events': []}
                if not isinstance(result, dict):
                    try:
                        response = _copy_json(result)
                    except (ValueError, TypeError):
                        response = {'unserializable_type': type(result).__name__}
                    _write(output / 'runtime_response.json', response)
                    context['invalid_runtime_result'] = True
                    result = {'real_execution': False, 'runtime_status': 'tool_error',
                              'reason': 'runtime result must be a dict', 'events': []}
                context['runtime_result'] = result
                _write(output / 'runtime_returned.json', result)
                result_path = runtime_dir / 'runtime_result.json'
                if result_path.exists():
                    if canonical_hash(_json(result_path)) != canonical_hash(result):
                        raise _Failure('fail_artifact', 'runtime_result_mismatch', 'returned and persisted runtime result differ', 'runtime')
                else:
                    _write(result_path, result)
                if context.get('invalid_runtime_result'):
                    raise _Failure('fail_artifact', 'invalid_runtime_result', result['reason'], 'runtime')
                if result.get('raw_path') and not Path(result['raw_path']).resolve().is_relative_to(runtime_dir):
                    raise _Failure('fail_artifact', 'runtime_raw_not_independent', 'runtime raw is outside replay output', 'runtime')
                row = ingest_execution(scenario, config, result, capsule, services=services)
                _bind_execution_input(row, context.get('submitted_config', config), capsule)
                row.update(domain=domain, domain_id=domain)
                context['row'] = row
                _write(output / 'execution_metadata.json', row)
                after = source_manifest(REPO)
                context['source_after'] = after
                _write(output / 'source_after.json', after)
                if canonical_hash(after) != canonical_hash(before):
                    raise _Failure('fail_environment', 'source_identity_changed', 'source HEAD or behavior changed during replay', 'runtime')
                if result.get('runtime_status') in {'blocked', 'failed', 'tool_error'}:
                    status = 'fail_timing' if context.get('runtime_exception') == 'TimeoutError' else 'fail_environment'
                    raise _Failure(status, 'runtime_preflight_blocked' if result['runtime_status'] == 'blocked' else 'runtime_tool_failure',
                                   result.get('reason') or 'runtime did not complete', 'runtime')
                if result.get('real_execution') is not True or result.get('runtime_status') != 'completed' or row['execution_stage'] == 'aborted':
                    raise _Failure('fail_artifact', 'ingestion_rejected', row.get('abort_reason') or 'runtime completion lacks execution evidence', 'ingest')
                return row
            except _Failure as exc:
                context['failure'] = exc
                raise
            except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
                context['failure'] = _Failure('fail_artifact', 'runtime_artifact_invalid', f'{type(exc).__name__}: {exc}', 'ingest')
                raise context['failure'] from exc

        phase = 'runtime'
        def comparison_view(execution, *, original):
            return _replay_comparison_view(capsule, execution, original=original,
                capsule_path=capsule_path, origin=origin, context=context)
        attempt = rerun_conformance(capsule, runner, domain=domain_id, plan_aware=True,
                                   comparison_view=comparison_view)
        if context.get('failure'):
            failure = context['failure']
            attempt.update(status=failure.status, failure_reason=str(failure), reason_code=failure.code, phase=failure.phase)
    except _Failure as exc:
        context['failure'] = exc
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
        context['failure'] = _Failure('fail_artifact', 'original_artifact_invalid' if phase == 'original_artifacts' else 'preflight_artifact_invalid',
                                      f'{type(exc).__name__}: {exc}', phase)
    if attempt is None:
        failure = context['failure']
        metadata = dict(domain=domain_id, reason_code=failure.code, phase=failure.phase)
        if capsule is not None:
            attempt = capsule.record_attempt(failure.status, failure_reason=str(failure), **metadata)
        else:
            attempt = dict(metadata, status=failure.status, failure_reason=str(failure), attempt_id='attempt-1', timestamp=utc_now())
    result, row = context.get('runtime_result', {}), context.get('row', {})
    attempt.update(runtime_status=result.get('runtime_status'), runtime_reason=result.get('reason'),
                   replay_id=replay_id, effective_input_sha256=(capsule.input['effective_input_sha256'] if capsule else None),
                   original_capsule_sha256=report['original'].get('capsule_sha256'),
                   original_execution_sha256=report['original'].get('execution_sha256'),
                   execution_origin=origin, **_qualification_fields(origin, context))
    for reference in report['original_artifacts']:
        if reference['available'] and _reference(reference['path']) != reference:
            attempt.update(status='fail_artifact', failure_reason='original evidence changed during replay',
                           reason_code='original_artifact_changed', phase='finalize')
    validate_replay_attempt(attempt)
    if capsule is not None:
        # record_attempt returns a snapshot: persist the outer boundary's additional
        # classification in the same one attempt, without adding an invented run.
        capsule.replay_attempts[-1] = _copy_json(attempt)
        _write(output / 'derived_capsule.json', capsule.to_dict())
    _write(output / 'attempt.json', attempt)
    report.update({key: attempt.get(key) for key in ('status', 'failure_reason', 'reason_code', 'phase',
                  'comparison_basis', 'comparison_matches_original', 'runtime_status', 'runtime_reason',
                  'execution_origin', 'real_runtime_attempt', 'real_ros_execution', 'qualification_errors')})
    if 'boundary_audit' in attempt:
        report['boundary_audit'] = attempt['boundary_audit']
    report.update(oracle_verdict=row.get('oracle_verdict'), finished_at=utc_now(),
                  artifacts={str(p.relative_to(output)): raw_sha256(p) for p in sorted(output.rglob('*')) if p.is_file()})
    report['report_sha256'] = _digest(report)
    _write(report['report_path'], report)
    return report


def _copy_json(data):
    return json.loads(json.dumps(data, allow_nan=False))


def _qualification_fields(origin, context):
    try:
        qualified = _qualification(origin, context.get('config', {}), context.get('runtime_result', {}),
                                   context.get('row', {}), output_root=context.get('output_root'))
    except (ValueError, TypeError, KeyError, AttributeError, OSError) as exc:
        qualified = {'real_runtime_attempt': False, 'real_ros_execution': False,
                     'qualification_errors': ['invalid_runtime_evidence:' + str(exc)]}
    qualified.pop('execution_origin', None)
    before, after = context.get('source_before'), context.get('source_after')
    expected = context.get('config', {}).get('source_manifest')
    if not before or before.get('source_frozen') is not True or before.get('dirty_behavior_paths') != [] or canonical_hash(before) != canonical_hash(expected):
        qualified['qualification_errors'].append('frozen_source_binding_missing')
        qualified.update(real_runtime_attempt=False, real_ros_execution=False)
    if canonical_hash(before) != canonical_hash(after):
        qualified['qualification_errors'].append('source_changed_or_after_snapshot_missing')
        qualified['real_ros_execution'] = False
    return qualified


def audit_replay(report_path) -> dict:
    """Verify persisted replay artifacts and outcome semantics, without ROS.

    valid means an intact audit record, including an honestly retained failure.
    real_ros_execution and real_runtime_attempt are separate evidence gates.
    """
    path = Path(report_path).resolve()
    errors = []
    report = {}
    try:
        report = _json(path)
        if report.get('schema') != SCHEMA or report.get('report_sha256') != _digest(report):
            raise ValueError('replay report schema or SHA256 mismatch')
        root = path.parent
        files = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p != path}
        if files != set(report['artifacts']):
            errors.append('artifact manifest does not cover exactly the persisted files')
        request = _json(root / 'request.json')
        if report.get('episode_checkpoint') != request.get('episode_checkpoint'):
            errors.append('request/report identity mismatch:episode_checkpoint')
        for key in ('replay_id', 'domain_id', 'execution_origin'):
            if report.get(key) != request.get(key):
                errors.append('request/report identity mismatch:' + key)
        # Missing boundary fields in historical host reports mean host only.
        boundary = report.get('runtime_boundary', 'host')
        if boundary not in ('host', 'isolated_netns'):
            errors.append('unsupported replay runtime boundary')
        if boundary != request.get('runtime_boundary', 'host'):
            errors.append('request/report identity mismatch:runtime_boundary')
        expected_origin = ISOLATED_RUNTIME if boundary == 'isolated_netns' else DEFAULT_RUNTIME
        if report.get('execution_origin') not in (expected_origin, 'injected_runtime'):
            errors.append('runtime origin/boundary mismatch')
        if boundary == 'isolated_netns' and report.get('execution_origin') != ISOLATED_RUNTIME:
            errors.append('isolated boundary requires canonical runtime origin')
        if report.get('formal_runtime') is not (report.get('execution_origin') in FORMAL_RUNTIMES):
            errors.append('runtime origin/formal qualification mismatch')
        if report.get('runtime_invoked') is not True and (report.get('real_runtime_attempt') or report.get('real_ros_execution')):
            errors.append('runtime qualifications require an invoked runtime')
        for relative, digest in report['artifacts'].items():
            artifact = (root / relative).resolve()
            if not artifact.is_relative_to(root) or raw_sha256(artifact) != digest:
                errors.append('artifact SHA256 mismatch: ' + relative)
        for reference in report['original_artifacts']:
            if reference['available'] and _reference(reference['path']) != reference:
                errors.append('original artifact changed: ' + reference['path'])
        attempt = _json(root / 'attempt.json')
        validate_replay_attempt(attempt)
        for key in ('status', 'failure_reason', 'comparison_basis', 'comparison_matches_original',
                    'runtime_status', 'runtime_reason', 'execution_origin', 'real_runtime_attempt', 'real_ros_execution'):
            if report.get(key) != attempt.get(key):
                errors.append('report/attempt differs: ' + key)
        if report['capsule_loaded']:
            original = ReplayCapsule.from_dict(_json(root / 'original_capsule.json'))
            derived = ReplayCapsule.from_dict(_json(root / 'derived_capsule.json'))
            if canonical_hash(original.input) != canonical_hash(derived.input) or _json(root / 'input_metadata.json') != original.input:
                errors.append('original input identity changed')
            if report['original']['input_sha256'] != canonical_hash(original.input) or report['original']['execution_sha256'] != canonical_hash(original.execution):
                errors.append('original capsule identity mismatch')
            for key, value in {
                'capsule_sha256': raw_sha256(root / 'original_capsule.json'),
                'effective_input_sha256': original.input['effective_input_sha256'],
                'target_sha256': canonical_hash(original.input['target']),
                'source_sha256': canonical_hash(original.input['source_manifest']),
            }.items():
                if report['original'].get(key) != value:
                    errors.append('original identity differs:' + key)
            if attempt.get('replay_id') != report['replay_id'] or attempt.get('effective_input_sha256') != original.input['effective_input_sha256'] or attempt.get('original_capsule_sha256') != report['original']['capsule_sha256'] or attempt.get('original_execution_sha256') != report['original']['execution_sha256']:
                errors.append('attempt original/input identity differs')
            if canonical_hash(derived.replay_attempts) != canonical_hash(original.replay_attempts + [attempt]):
                errors.append('derived capsule attempt history differs')
            new = attempt.get('execution_metadata')
            expected = ReplayCapsule.from_dict(original.to_dict())
            if new is not None:
                try:
                    expected.record_execution(new)
                except (ValueError, KeyError, TypeError, OSError):
                    if attempt['status'] != 'fail_artifact':
                        raise
                if (root / 'execution_metadata.json').exists() and canonical_hash(new) != canonical_hash(_json(root / 'execution_metadata.json')):
                    errors.append('attempt execution metadata differs from persisted ingest')
            if derived.execution != expected.execution or derived.execution_history != expected.execution_history or derived.non_replayable != original.non_replayable:
                errors.append('original execution/failure disposition was not retained')
            if attempt['status'] in {'pass', 'inconclusive'}:
                if new is None or new.get('domain') != report['domain_id']:
                    errors.append('successful comparison lacks fresh-domain execution')
                elif original.execution is None:
                    if attempt.get('comparison_basis') != 'execution_success_only' or attempt.get('comparison_matches_original') is not False or new.get('execution_stage') != 'judged':
                        errors.append('execution-only comparison misrepresented')
                else:
                    comparison_context = dict(config=_json(root / 'runtime_config.json'),
                        runtime_result=_json(root / 'runtime_returned.json'), output_root=root / 'runtime')
                    before_view = _replay_comparison_view(original, original.execution, original=True,
                        capsule_path=report['capsule_path'], origin=report['execution_origin'], context=comparison_context)
                    after_view = _replay_comparison_view(original, new, original=False,
                        capsule_path=report['capsule_path'], origin=report['execution_origin'], context=comparison_context)
                    before, after = conformance_signature(before_view), conformance_signature(after_view)
                    matches = before is not None and after is not None and canonical_hash(before) == canonical_hash(after)
                    if attempt['status'] != ('pass' if matches else 'inconclusive') or attempt.get('comparison_matches_original') is not matches or canonical_hash(attempt.get('original_signature')) != canonical_hash(before) or canonical_hash(attempt.get('replay_signature')) != canonical_hash(after):
                        errors.append('oracle/typed signature comparison mismatch')
                source = _json(root / 'source_before.json')
                if canonical_hash(source) != canonical_hash(original.input['source_manifest']) or source.get('source_frozen') is not True or canonical_hash(_json(root / 'source_after.json')) != canonical_hash(source):
                    errors.append('successful replay source was not frozen and unchanged')
            _check_audit_config(root, original, report, errors)
        if report.get('runtime_invoked'):
            result = _json(root / 'runtime_returned.json')
            if (root / 'runtime/runtime_result.json').exists() and report.get('reason_code') != 'runtime_result_mismatch' and _json(root / 'runtime/runtime_result.json') != result:
                errors.append('persisted runtime result differs from returned result')
            if report.get('runtime_reason') != result.get('reason') or report.get('runtime_status') != result.get('runtime_status'):
                errors.append('runtime status/reason lost')
            config = _json(root / 'runtime_config.json')
            row = _json(root / 'execution_metadata.json') if (root / 'execution_metadata.json').exists() else {}
            source_before = _json(root / 'source_before.json') if (root / 'source_before.json').exists() else None
            source_after = _json(root / 'source_after.json') if (root / 'source_after.json').exists() else None
            qualified = _qualification_fields(report['execution_origin'], {
                'config': config, 'runtime_result': result, 'row': row,
                'output_root': root / 'runtime',
                'source_before': source_before, 'source_after': source_after})
            if row and report.get('capsule_loaded'):
                recalculated = ingest_execution(Scenario.from_dict(original.input['scenario']), config, result, original,
                                               services=_registered_target(original)[1])
                _bind_execution_input(recalculated, config, original)
                keys = ('execution_stage', 'abort_reason', 'actual_run', 'execution_source', 'typed_facts',
                        'oracle', 'oracle_verdict', 'rule_id', 'mutation_receipt', 'observed_effect',
                        'precondition_receipts', 'fault_control_receipt', 'neutral_receipt', 'raw_path',
                        'raw_sha256', 'runtime_status', 'runtime_reason', 'runtime_execution_metadata',
                        'environment_raw_evidence', 'environment', 'observation_receipts',
                        'input_identity_schema', 'final_input', 'input_layer_receipts',
                        'actual_submitted_payload_sha256')
                for key in keys:
                    if canonical_hash(row.get(key)) != canonical_hash(recalculated.get(key)):
                        errors.append('recomputed ingest differs:' + key)
                if report.get('oracle_verdict') != row.get('oracle_verdict'):
                    errors.append('reported oracle differs from persisted ingest')
            for key, value in qualified.items():
                if report.get(key) != value:
                    errors.append('execution qualification differs: ' + key)
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
        errors.append(f'{type(exc).__name__}: {exc}')
    return {'schema': SCHEMA, 'valid': not errors, 'errors': errors, 'status': report.get('status'),
            'execution_origin': report.get('execution_origin'),
            'real_runtime_attempt': report.get('real_runtime_attempt') is True and not errors,
            'real_ros_execution': report.get('real_ros_execution') is True and not errors,
            'report_path': str(path)}


def _check_audit_config(root, original, report, errors):
    config_path = root / 'runtime_config.json'
    if not config_path.exists():
        return
    config = _json(config_path)
    if config.get('domain_id') != report['domain_id'] or config.get('run_id') != report['replay_id']:
        errors.append('replay config execution identity mismatch')
    for key in ('target_id', 'target_revision', 'seed', 'rmw', 'arm', 'repeat', 'mutation_kind', 'carrier', 'source_manifest', 'effective_input_sha256'):
        if canonical_hash(config.get(key)) != canonical_hash(original.input[key]):
            errors.append('replay config/input mismatch: ' + key)
    for key, value in original.input['effective_input']['final_input'].items():
        if canonical_hash(config.get(key)) != canonical_hash(value):
            errors.append('submitted input was changed: ' + key)
    from .runner import _runtime_execution_config, runtime_for_boundary
    boundary = report.get('runtime_boundary', 'host')
    expected_origin = ISOLATED_RUNTIME if boundary == 'isolated_netns' else DEFAULT_RUNTIME
    origin = report.get('execution_origin')
    formal = report.get('formal_runtime') is True and origin == expected_origin
    if (origin not in (expected_origin, 'injected_runtime')
            or report.get('formal_runtime') is not (origin in FORMAL_RUNTIMES)):
        errors.append('replay execution intent origin/formal mismatch')
    # A replay is an execute request even if carrier admission refused it before
    # invocation. Intent is reconstructed from report/request canonical origin,
    # not a user-supplied config bool; audit_replay separately binds request,
    # attempt, source snapshots and runtime artifacts/qualification.
    expected = _runtime_execution_config(
        _replay_config(original, report['replay_id'], report['domain_id'], original.input['source_manifest']),
        execute=True, runtime=runtime_for_boundary(boundary) if formal else None,
        runtime_boundary=boundary)
    if report.get('episode_checkpoint') is not None:
        expected['episode_rebuild'] = _checkpoint_request_for_capsule(
            report['capsule_path'], report['episode_checkpoint'])
        from .topology_episode_rebuild import load_checkpoint
        load_checkpoint(expected['episode_rebuild'], expected['topology_program'], expected)
    if canonical_hash(config) != canonical_hash(expected):
        errors.append('replay config differs from deterministic capsule reconstruction')
    if (report.get('observation_replay') is not None
            or expected.get('observation_mode') == 'instrumentation'):
        if report.get('observation_replay') != _observation_replay_declaration(original, expected):
            errors.append('replay observation declaration differs from original execution selection')
    _check_domain(original, report['domain_id'])
