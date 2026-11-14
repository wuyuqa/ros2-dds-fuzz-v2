"""Persist one normal conformance rerun without changing its source evidence.

Execution and comparison use core.rerun_conformance and the existing runtime
config/ingest boundary. audit_replay only reads artifacts; it never starts ROS.
Injected runtimes establish offline contract evidence, regardless of their flags.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import uuid

from r2f.core import ReplayCapsule, Scenario, canonical_hash, raw_sha256, rerun_conformance
from r2f.core.replay import conformance_signature, validate_replay_attempt
from r2f.core.state_machine import utc_now
from r2f.modules.s1.module import S1Module
from .identity import runtime_config, source_manifest
from .runtime_adapter import ingest_execution

SCHEMA = 'r2f.campaign-replay-audit/v1'
REPO = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME = 'r2f.runtime.ros2_pair.run_pair'
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
    if root.parent.name == 'cells':
        root = root.parent.parent
    if capsule_path.exists() and (output.is_relative_to(root) or root.is_relative_to(output)):
        raise ValueError('replay output must be independent of the original evidence directory')
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise FileExistsError('replay output directory must be empty: ' + str(output))


def _check_domain(capsule, domain):
    if type(domain) is not int or not 0 <= domain <= 231:
        raise _Failure('fail_environment', 'invalid_domain', 'domain_id must be an integer in 0..231', 'preflight')
    original = capsule.execution or {}
    old_domains = [capsule.input.get('origin_domain'), original.get('domain'), original.get('domain_id'),
                   (original.get('runtime_execution_metadata') or {}).get('domain_id')]
    if domain in old_domains:
        raise _Failure('fail_environment', 'fresh_domain_required', 'replay requires a fresh domain', 'preflight')


def _check_source(capsule, current):
    expected = capsule.input['source_manifest']
    for source in (expected, current):
        if source.get('source_frozen') is not True or not source.get('head') or source.get('dirty_behavior_paths') != []:
            raise _Failure('fail_environment', 'source_not_frozen', 'source behavior is not tracked and clean at HEAD', 'preflight')
    if Path(expected.get('source_repo', '')).resolve() != REPO or canonical_hash(expected) != canonical_hash(current):
        raise _Failure('fail_environment', 'source_identity_mismatch', 'capsule source differs from the loaded frozen source', 'preflight')
    target = S1Module().targets.get(capsule.input['target_id'])
    if target is None or canonical_hash(target.to_dict()) != canonical_hash(capsule.input['target']):
        raise _Failure('fail_artifact', 'target_contract_mismatch', 'capsule target/revision differs from registered target', 'preflight')


def _qualification(origin, config, result, row):
    """Use the report auditor's runtime/environment gates plus worker raw links.

    This is evidence validation, not cryptographic attestation of process origin.
    A failed preflight can be an actual default-runtime attempt without being an
    actual ROS execution: these two qualifications deliberately remain separate.
    """
    errors = []
    if origin != DEFAULT_RUNTIME:
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
    actual_attempt = origin == DEFAULT_RUNTIME and result.get('evidence_origin') not in _OFFLINE and raw_ok
    if result.get('real_execution') is not True or row.get('actual_run') is not True or row.get('execution_stage') != 'judged':
        errors.append('real_judged_execution_missing')
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
    if env.get('rmw', {}).get(config.get('rmw'), {}).get('available') is not True:
        errors.append('rmw_not_available_in_manifest')
    if not env.get('dependency_lock_sha256') or env.get('process_scan', {}).get('ok') is not True or env.get('old_lock_scan', {}).get('ok') is not True:
        errors.append('preflight_receipts_missing')
    events = result.get('events', [])
    workers = metadata.get('workers', [])
    if len(workers) != 2 or {w.get('role') for w in workers} != {'writer', 'reader'} or len({w.get('pid') for w in workers}) != 2:
        errors.append('independent_worker_metadata_missing')
    for worker in workers:
        role, pid = worker.get('role'), worker.get('pid')
        if type(pid) is not int or type(worker.get('returncode')) is not int or worker['returncode'] != 0 or worker.get('fresh_process') is not True or worker.get('separate_context') is not True:
            errors.append('worker_health_receipt_missing:' + str(role))
        for event in ('process_started', 'endpoint_created', 'process_exit'):
            if not any(e.get('event') == event and e.get('role') == role and e.get('pid') == pid for e in events):
                errors.append('worker_raw_link_missing:' + str(role) + ':' + event)
    return {'execution_origin': origin, 'real_runtime_attempt': actual_attempt,
            'real_ros_execution': not errors, 'qualification_errors': errors}


def replay_cell(capsule_path, *, output_dir, domain_id, runtime=None) -> dict:
    """Replay an unchanged capsule into a fresh evidence directory and domain.

    output_dir must be empty and outside the original run. Source must match the
    capsule and be frozen even for an injected runtime; offline tests mock source
    observation rather than weakening that gate. Runtime owns leases/preflight.
    """
    capsule_path, output = Path(capsule_path).resolve(), Path(output_dir).resolve()
    _reserve(capsule_path, output)
    replay_id = 'replay-' + uuid.uuid4().hex
    origin = DEFAULT_RUNTIME if runtime is None else 'injected_runtime'
    request = {'schema': SCHEMA, 'replay_id': replay_id, 'capsule_path': str(capsule_path),
               'domain_id': domain_id, 'execution_origin': origin, 'timestamp': utc_now()}
    _write(output / 'request.json', request)
    report = dict(request, report_path=str(output / 'replay_report.json'),
                  formal_runtime=runtime is None, original_artifacts=[], original={},
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
        phase = 'preflight'
        _check_domain(capsule, domain_id)
        before = source_manifest(REPO)
        context['source_before'] = before
        _write(output / 'source_before.json', before)
        _check_source(capsule, before)
        if runtime is None:
            from r2f.runtime.ros2_pair import run_pair
            runtime = run_pair

        def runner(scenario, domain):
            try:
                config = runtime_config(
                    scenario.final_input, run_id=replay_id, campaign_id=replay_id,
                    cell_id=f'{scenario.rmw}.{scenario.arm}.{scenario.repeat}', campaign_kind='replay',
                    target_id=scenario.target_id, target_revision=scenario.target_revision,
                    seed=scenario.seed, rmw=scenario.rmw, arm=scenario.arm, repeat=scenario.repeat,
                    mutation_kind=scenario.mutation_kind, carrier=scenario.carrier,
                    scenario=scenario.to_dict(), domain_id=domain, environment_mode='ros_overlay',
                    source_repo=str(REPO), expected_head=before['head'],
                    expected_target_revision=scenario.target_revision, source_manifest=scenario.source_manifest,
                    final_input=scenario.final_input, effective_input_sha256=scenario.effective_input_sha256,
                    evidence_purpose='normal_conformance_replay',
                    control_kind='fault' if scenario.arm == 'negative' else None)
                context['config'] = config
                _write(output / 'runtime_config.json', config)
                runtime_dir = output / 'runtime'
                runtime_dir.mkdir()
                report['runtime_invoked'] = True
                try:
                    result = runtime(_copy_json(config), runtime_dir)
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
                row = ingest_execution(scenario, config, result, capsule)
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
        attempt = rerun_conformance(capsule, runner, domain=domain_id)
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
    report.update(oracle_verdict=row.get('oracle_verdict'), finished_at=utc_now(),
                  artifacts={str(p.relative_to(output)): raw_sha256(p) for p in sorted(output.rglob('*')) if p.is_file()})
    report['report_sha256'] = _digest(report)
    _write(report['report_path'], report)
    return report


def _copy_json(data):
    return json.loads(json.dumps(data, allow_nan=False))


def _qualification_fields(origin, context):
    try:
        qualified = _qualification(origin, context.get('config', {}), context.get('runtime_result', {}), context.get('row', {}))
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
        for key in ('replay_id', 'domain_id', 'execution_origin'):
            if report.get(key) != request.get(key):
                errors.append('request/report identity mismatch:' + key)
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
                    before, after = conformance_signature(original.execution), conformance_signature(new)
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
                'source_before': source_before, 'source_after': source_after})
            if row and report.get('capsule_loaded'):
                recalculated = ingest_execution(Scenario.from_dict(original.input['scenario']), config, result, original)
                keys = ('execution_stage', 'abort_reason', 'actual_run', 'execution_source', 'typed_facts',
                        'oracle', 'oracle_verdict', 'rule_id', 'mutation_receipt', 'observed_effect',
                        'precondition_receipts', 'fault_control_receipt', 'neutral_receipt', 'raw_path',
                        'raw_sha256', 'runtime_status', 'runtime_reason', 'runtime_execution_metadata',
                        'environment_raw_evidence', 'environment', 'observation_receipts')
                for key in keys:
                    if canonical_hash(row.get(key)) != canonical_hash(recalculated.get(key)):
                        errors.append('recomputed ingest differs:' + key)
                if report.get('oracle_verdict') != row.get('oracle_verdict'):
                    errors.append('reported oracle differs from persisted ingest')
            for key, value in qualified.items():
                if report.get(key) != value:
                    errors.append('execution qualification differs: ' + key)
            if report.get('formal_runtime') is not (report.get('execution_origin') == DEFAULT_RUNTIME):
                errors.append('runtime origin/formal qualification mismatch')
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
    _check_domain(original, report['domain_id'])
