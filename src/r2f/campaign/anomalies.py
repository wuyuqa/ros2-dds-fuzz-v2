"""Derived runner evidence attachments; no execution or candidate admission gates."""
from copy import deepcopy
import json
import subprocess
import traceback
from pathlib import Path

from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports.candidate_report import (
    _write_immutable_json, review_packet, write_review_packet, validate_runner_replay_capsule,
)

IDENTITY_KEYS = ('run_id', 'cell_id', 'target_id', 'target_revision', 'rmw', 'arm',
                 'repeat', 'effective_input_sha256', 'rule_id')


def reference(root, path):
    path = Path(path).resolve()
    relative = path.relative_to(Path(root).resolve())
    return {'path': str(relative), 'sha256': raw_sha256(path)}


def identity_path(root, cell):
    if not isinstance(cell, str) or cell in {'', '.', '..'} or Path(cell).name != cell or '\\' in cell:
        raise ValueError('invalid evidence cell identity')
    return Path(root) / 'evidence' / cell / 'identity.json'


def prepare_identity(root, row, source, *, identity=None, rule=None):
    """Persist the available identity before runtime or report packaging."""
    saved = {key: deepcopy(row.get(key)) for key in IDENTITY_KEYS}
    saved['rule_id'] = row.get('rule_id') or rule
    saved.update(source_manifest=deepcopy(source), identity=deepcopy(identity or {}),
                 planned_row=deepcopy(row))
    saved['identity'].setdefault('source_sha256', canonical_hash(source))
    saved['incomplete_fields'] = [key for key in IDENTITY_KEYS if saved[key] is None]
    if not isinstance(source, dict) or not source.get('head'):
        saved['incomplete_fields'].append('source_manifest.head')
    path = identity_path(root, row['cell_id'])
    _write_immutable_json(path, saved)
    return reference(root, path)



def runtime_exception(exc):
    """Describe an unconfirmed attempt; exception text is never execution proof."""
    def text(value):
        return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value
    kind = ('timeout' if isinstance(exc, (TimeoutError, subprocess.TimeoutExpired)) else
            'worker_exit' if isinstance(exc, subprocess.CalledProcessError) else 'tool_failure')
    return {'real_execution': None, 'runtime_status': 'tool_error',
            'reason': f'{type(exc).__name__}:{exc}',
            'runner_exception': {'kind': kind, 'error_type': type(exc).__name__,
                                 'detail': str(exc), 'traceback': traceback.format_exc(),
                                 'returncode': getattr(exc, 'returncode', None),
                                 'stdout': text(getattr(exc, 'stdout', None)),
                                 'stderr': text(getattr(exc, 'stderr', None))}}


def persist_runtime_result(path, result):
    """A runtime may already have published its result, including a partial one."""
    path = Path(path)
    if not isinstance(result, dict):
        raise ValueError("runtime_result_not_an_object")
    if path.exists() or path.is_symlink():
        if result.get('runner_exception'):
            return
        if json.loads(path.read_text()) != result:
            raise ValueError('runtime_result_differs_from_returned_evidence')
        return
    _write_immutable_json(path, result)



def wrapper_failure(row, exc):
    """Mark unconfirmed tool failure on the original row, without new receipts."""
    result = runtime_exception(exc)
    rule = row.get('rule_id')
    return dict(row, execution_stage='aborted', actual_run=False, tool_error=True,
                abort_reason='runner_wrapper_failure', reason_code='runner_wrapper_failure',
                runtime_status='tool_error', runner_exception=result['runner_exception'],
                typed_facts={}, oracle_verdict='unknown',
                oracle={'verdict': 'unknown', 'rule_id': rule,
                        'reason_codes': ['runner_wrapper_failure']})


def _kind(row, runtime):
    if row.get('runner_exception'):
        return row['runner_exception']['kind']
    if isinstance(runtime, dict) and runtime.get('runtime_status') not in (None, 'completed'):
        reason = str(runtime.get('reason') or '').lower()
        if any(word in reason for word in ('timeout', 'timed out', 'deadline')):
            return 'timeout'
        execution = runtime.get('execution_metadata') or runtime.get('execution') or {}
        workers = execution.get('workers', []) if isinstance(execution, dict) else []
        if any(isinstance(worker, dict) and worker.get('returncode') not in (None, 0)
               for worker in workers):
            return 'worker_exit'
        return 'tool_failure'
    if row.get('tool_error') or row.get('runtime_status') == 'tool_error':
        return 'tool_failure'
    if row.get('oracle_verdict') == 'fail':
        return 'rule_failure'
    if row.get('execution_stage') == 'planned':
        return 'planned'
    if row.get('oracle_verdict') in {'unknown', 'not_applicable'}:
        return 'observation_insufficient'
    projection = (row.get('typed_facts') or {}).get('projection') or {}
    schema = projection.get('schema_check') or {}
    tools = schema.get('discovery_tools') or {}
    layers = schema.get('discovery_layers') or {}
    native_topic = (layers.get('topic') or {}).get('field_comparisons') or []
    if (tools.get('review_required') is True
            or any(item.get('status') == 'mismatch_requires_review' for item in native_topic)
            or schema.get('observation_obligations', {}).get('review_required') is True):
        return 'observation_insufficient'
    return 'normal'


def record_packaging_failure(record, exc, path):
    """Retain an export error alongside, rather than instead of, runtime facts."""
    problem = {'kind': 'packaging_failure', 'error_type': type(exc).__name__,
               'detail': str(exc), 'path': str(path)}
    if record.get('cell_id') is not None:
        problem['cell_id'] = record['cell_id']
    record.setdefault('packaging_failures', []).append(problem)


def collect_packets(root, record, *, source):
    """Attach diagnostic packets to existing rows without changing those rows."""
    root = Path(root)
    packets, identities = [], []
    problems = deepcopy(record.get('packaging_failures', []))
    for row in record['rows']:
        problems.extend(deepcopy(row.get('packaging_failures', [])))
        try:
            path = identity_path(root, row['cell_id'])
            if not path.exists():
                prepare_identity(root, row, source, identity=record.get('identity'))
            saved = json.loads(path.read_text())
            identities.append(dict(reference(root, path), cell_id=row['cell_id']))
            enriched = dict(row, rule_id=row.get('rule_id') or saved['rule_id'])
            packet = review_packet(enriched, identity=saved['identity'])
            packet['candidate_id'] = None
            packet['incomplete_fields'].append('candidate_id')
            packet['intake_identity'] = reference(root, path)
            artifacts = []
            result_path = row.get('runtime_result_path')
            if result_path:
                directory = Path(result_path).parent
                for artifact in sorted(directory.rglob('*')):
                    if artifact.is_file():
                        artifacts.append(reference(root, artifact))
            runtime = None
            if result_path and Path(result_path).is_file():
                try:
                    returned = json.loads(Path(result_path).read_text())
                    if isinstance(returned, dict):
                        runtime = {key: returned.get(key) for key in
                                   ('runtime_status', 'reason', 'real_execution', 'evidence_origin',
                                    'execution_metadata', 'execution', 'environment',
                                    'stdout', 'stderr', 'returncode', 'exit_code')}
                except (OSError, ValueError):
                    pass
            exception = row.get('runner_exception')
            packet['runner_diagnostic'] = {
                'kind': _kind(row, runtime), 'candidate_eligible': False,
                'replay_status': 'not_run', 'replay_reason': 'not_run_at_original_seal',
                'execution_stage': row.get('execution_stage'),
                'actual_run': None if exception else row.get('actual_run'),
                'error': exception, 'artifacts': artifacts, 'runtime': runtime,
                'cleanup': {'status': 'unknown', 'reason': 'no_cleanup_receipt'},
                'reason': row.get('abort_reason') or row.get('reason_code'),
            }
            missing = {'cleanup': 'no_cleanup_receipt'}
            if not row.get('raw_path') or not row.get('raw_sha256'):
                missing['raw'] = 'no_qualified_raw_reference_returned'
            if exception:
                missing['actual_run'] = 'wrapper_exception_does_not_confirm_execution'
                packet['incomplete_fields'].extend(['execution.actual_run', 'cleanup'])
            if exception or (runtime and runtime.get('runtime_status') not in (None, 'completed')):
                process = exception or runtime
                execution = (runtime or {}).get('execution_metadata') or (runtime or {}).get('execution') or {}
                workers = execution.get('workers', []) if isinstance(execution, dict) else []
                for field, member in (('exit_code', 'returncode'), ('stdout', 'stdout'), ('stderr', 'stderr')):
                    supplied = process.get(member) is not None
                    if field == 'exit_code':
                        supplied = supplied or process.get('exit_code') is not None or (
                            bool(workers) and all(isinstance(w, dict) and w.get('returncode') is not None
                                                  for w in workers))
                    else:
                        supplied = supplied or any(Path(a['path']).name == field + '.log' for a in artifacts)
                    if not supplied:
                        missing[field] = ('exception_did_not_supply_' if exception else
                                          'runtime_did_not_supply_') + field
            packet['runner_diagnostic']['missing_evidence'] = missing
            packet['incomplete_fields'].extend('runner_diagnostic.' + field for field in missing)
            packet_path = path.with_name('review-packet.json')
            write_review_packet(packet_path, packet)
            packets.append(dict(reference(root, packet_path), cell_id=row['cell_id']))
        except Exception as exc:
            problems.append({'kind': 'packet_export_failure', 'cell_id': row.get('cell_id'),
                             'error_type': type(exc).__name__, 'detail': str(exc)})
    return {'packets': packets, 'identities': identities, 'tool_problems': problems}


def finish_evidence(root, record, persist, *, source, terminal=None, automatic_replay=None):
    """Keep a last, immutable attachment index even if report packaging fails."""
    root = Path(root)
    intake = collect_packets(root, record, source=source)
    record['anomaly_intake'] = deepcopy(intake)
    reports = []
    try:
        path = persist()
        if path is not None:
            reports.append(reference(root, path))
    except Exception as exc:
        intake['tool_problems'].append({'kind': 'packaging_failure',
                                       'error_type': type(exc).__name__, 'detail': str(exc)})
        record['anomaly_intake'] = deepcopy(intake)
    if terminal is not None:
        try:
            terminal()
        except Exception as exc:
            intake['tool_problems'].append({'kind': 'terminal_index_failure',
                                           'error_type': type(exc).__name__, 'detail': str(exc)})
            record['anomaly_intake'] = deepcopy(intake)
    for name in ('report.json', 'cycle.json'):
        path = root / name
        if path.is_file() and not any(r['path'] == name for r in reports):
            reports.append(reference(root, path))
    index = {'schema_version': 'r2f.runner-evidence-index/v1', 'run_id': record['run_id'],
             'source_manifest': deepcopy(source), **intake, 'reports': reports,
             'manifest': reference(root, root / 'manifest.json')}
    try:
        _write_immutable_json(root / 'evidence-index.json', index)
    except Exception as exc:
        # The already published report/packets stay readable; do not rewrite
        # them to pretend the terminal attachment export succeeded.
        intake['tool_problems'].append({'kind': 'evidence_index_export_failure',
                                       'error_type': type(exc).__name__, 'detail': str(exc)})
        record['anomaly_intake'] = deepcopy(intake)
    if automatic_replay is not None:
        try:
            automatic_replays(root, record, **automatic_replay)
        except Exception as exc:
            # Original packets/report/index are already published. Preserve the
            # attachment failure separately, including any durable dispatches.
            problem = {'kind': 'automatic_replay_attachment_failure',
                       'run_id': record['run_id'], 'error_type': type(exc).__name__,
                       'detail': str(exc)}
            record['anomaly_intake']['tool_problems'].append(problem)
            try:
                _write_immutable_json(root / 'replay-failure.json', problem)
            except OSError:
                pass  # Existing dispatch requests remain queryable on export failure.
    return record


def automatic_replays(root, record, *, policy, runtime, execute, runtime_boundary, fixture_original):
    """Append associations to existing replay attempts; never revise original evidence.

    A policy declares output_dir, max_attempts and domain_ids. Domains are requests,
    not clearance: replay_cell and its canonical runtime retain all admission gates.
    Injected runtime callables are exclusively offline fixture boundaries.
    """
    import uuid
    from .replay import replay_cell
    from r2f.reports.candidate_report import query_runner_evidence

    root = Path(root).resolve()
    originals = [reference(root, p) for p in sorted(root.rglob('*')) if p.is_file()]
    original_query = query_runner_evidence(root)
    entries, used = [], 0
    valid_policy = isinstance(policy, dict)
    budget = policy.get('max_attempts') if valid_policy else None
    domains = policy.get('domain_ids') if valid_policy else None
    output = policy.get('output_dir') if valid_policy else None

    def unchanged():
        return all((root / ref['path']).is_file()
                   and raw_sha256(root / ref['path']) == ref['sha256'] for ref in originals)

    for row in record['rows']:
        entry = {'cell_id': row['cell_id'], 'status': 'not_run', 'candidate_eligible': False,
                 'original_confirmed': False, 'internal_state_recovery': False}
        reason = None
        if not execute:
            reason = 'original_execution_not_requested'
        elif policy is None:
            reason = 'automatic_replay_not_requested'
        elif not valid_policy:
            reason = 'replay_policy_invalid'
        elif not original_query['evidence_valid']:
            reason = 'original_evidence_invalid'
        elif not (root / 'report.json').is_file():
            reason = 'original_report_unavailable'
        elif row.get('execution_stage') == 'planned':
            reason = 'original_execution_unavailable'
        elif row.get('arm') == 'negative' or row.get('campaign_kind') == 'negative':
            reason = 'control_not_eligible_for_automatic_replay'
        elif not row.get('replay_path') or not Path(row['replay_path']).is_file():
            reason = 'original_capsule_unavailable'
        elif type(budget) is not int or budget < 0:
            reason = 'replay_budget_not_declared'
        elif used >= budget:
            reason = 'replay_budget_exhausted'
        elif not isinstance(domains, list) or used >= len(domains):
            reason = 'fresh_domain_not_configured'
        elif domains[used] in domains[:used]:
            reason = 'fresh_domain_reused'
        elif not isinstance(output, str) or not output.strip():
            reason = 'replay_output_not_configured'
        elif fixture_original and runtime is None:
            reason = 'fixture_replay_runtime_not_supplied'
        elif not unchanged():
            reason = 'original_artifact_changed'
        if reason is not None:
            entry['reason_code'] = reason
            entries.append(entry)
            continue

        # Read sealed identity/report bytes, never infer identity from the path
        # or the mutable in-memory row. Query independently repeats this check.
        try:
            saved, = [item for item in original_query['identities'] if item['cell_id'] == row['cell_id']]
            original = json.loads((root / 'report.json').read_text())
            original_row, = [item for item in original['rows'] if item['cell_id'] == row['cell_id']]
            entry['capsule_binding'] = validate_runner_replay_capsule(row['replay_path'], saved, original_row)
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            entry.update(reason_code=str(exc).split(':', 1)[0], detail=str(exc))
            entries.append(entry)
            continue

        dispatch_id = 'replay-' + uuid.uuid4().hex
        directory = root / 'replay-scheduling' / dispatch_id
        destination = Path(output).resolve() / dispatch_id
        request = {'dispatch_id': dispatch_id, 'run_id': record['run_id'],
                   'cell_id': row['cell_id'], 'intake_identity': reference(root, identity_path(root, row['cell_id'])),
                   'original_index': reference(root, root / 'evidence-index.json'),
                   'capsule': reference(root, row['replay_path']),
                   'output_dir': str(destination), 'domain_id': domains[used],
                   'runtime_boundary': runtime_boundary, 'injected_runtime': runtime is not None,
                   'max_attempts': budget, 'attempt_ordinal': used + 1}
        # Durable dispatch request precedes reserve/preflight/runtime/packaging.
        used += 1
        try:
            _write_immutable_json(directory / 'request.json', request)
        except Exception as exc:
            entry.update(reason_code='replay_request_export_failed', dispatch_id=dispatch_id,
                         error_type=type(exc).__name__, detail=str(exc),
                         requested_output_dir=str(destination))
            entries.append(entry)
            continue
        entry.update(dispatch_id=dispatch_id, request=reference(root, directory / 'request.json'))
        try:
            if runtime is not None:
                from .runner import _resolve_execution_runtime
                _, formal = _resolve_execution_runtime(runtime, runtime_boundary)
                if formal:
                    raise ValueError('automatic replay callable injection is offline-only; canonical runtime uses runtime=None')
            replay = replay_cell(row['replay_path'], output_dir=destination,
                                 domain_id=request['domain_id'], runtime=runtime,
                                 runtime_boundary=runtime_boundary)
            path = Path(replay['report_path'])
            entry.update(status=replay['status'], reason_code=replay.get('reason_code') or 'replay_completed',
                         replay_report={'path': str(path), 'sha256': raw_sha256(path)},
                         replay_attempt={'path': str(destination / 'attempt.json'),
                                         'sha256': raw_sha256(destination / 'attempt.json')},
                         real_ros_execution=replay.get('real_ros_execution', False),
                         runtime_invoked=replay['runtime_invoked'],
                         real_runtime_attempt=replay.get('real_runtime_attempt', False),
                         qualification_errors=replay.get('qualification_errors', []))
        except Exception as exc:
            entry.update(status='scheduling_failed', reason_code='replay_dispatch_failed',
                         error_type=type(exc).__name__, detail=str(exc), real_ros_execution=False)
        entry['original_artifacts_unchanged'] = unchanged()
        if not entry['original_artifacts_unchanged']:
            entry.update(status='fail_artifact', reason_code='original_artifact_changed',
                         real_ros_execution=False)
        _write_immutable_json(directory / 'outcome.json', entry)
        entry['outcome'] = reference(root, directory / 'outcome.json')
        entries.append(entry)
    summary = {'schema_version': 'r2f.runner-auto-replay/v1', 'run_id': record['run_id'],
               'original_index': (reference(root, root / 'evidence-index.json')
                                  if (root / 'evidence-index.json').is_file() else None),
               'policy': deepcopy(policy),
               'original_artifacts': originals, 'original_artifacts_unchanged': unchanged(),
               'entries': entries}
    summary['summary_sha256'] = canonical_hash(summary)
    _write_immutable_json(root / 'replay-summary.json', summary)
