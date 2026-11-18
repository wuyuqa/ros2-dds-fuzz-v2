"""Third target's supervised namespace body, reached only through run_pair.

The host owns preflight, leases, raw serialization and final execution metadata.
The private child entry requires inherited locked descriptors and an owner channel.
No ROS imports, fixtures, or alternate campaign runner live in this module.
"""
from __future__ import annotations
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
import time

from .discovery_network import (DiscoveryDropController, DiscoveryNetemController,
                              DiscoveryNetworkError, NETEM_IMPLEMENTATION, NETEM_LOSS_PCT,
                              NETEM_RESPONSIBILITY_LAYER, NETEM_SCOPE_FIELD,
                              NetemScopeError, WindowGateError, netem_hit_semantics,
                              WindowLedger, assert_isolated_namespace, namespace_command,
                              normalize_netem_spec, validate_window_plan)
from .discovery_profiles import write_profiles
from .domains import IsolationError, LeaseSet, probe_rtps_ports
from .environment import REPO, activated_environment, check_environment, source_identity
from .process import WorkerPool
from .ros2_pair import (RoleRmwWorkerPool, effective_rmws_receipt, mixed_base_environment,
                        mixed_worker_start_failure, observation_uses_single_vendor_contract,
                        observed_rmws_from_events, require_role_rmw, role_rmw,
                        role_rmw_differs_from_config, role_rmw_environments)

IMPLEMENTATION = 'isolated_netns_nft_discovery_udp_drop'
ENVIRONMENT_MUTATION_KINDS = ('discovery_drop', 'discovery_netem')
CLOCK_GUARD_NS = 50_000_000
RECOVERY_ACTIVITY_LIMIT = 32768

from .ros2_pair import F2_WINDOWS_TARGET, f2_windows_operations


def recovery_capture_spec(*, output_dir, run_id, domain_id, netns_identity, limits=None):
    """Build the third target's bounded observer spec without changing global defaults."""
    from r2f.instrumentation.sedp import build_capture_spec
    supplied = {'timeout_sec': 240, 'max_bytes': 32 * 1024 * 1024, 'snaplen': 65535,
                'activity_limit': RECOVERY_ACTIVITY_LIMIT, **(limits or {})}
    return build_capture_spec(run_id=run_id, domain_id=domain_id, netns_identity=netns_identity,
                              output_dir=output_dir, limits=supplied)


def execution_chain_identity():
    result = source_identity()
    digest = hashlib.sha256()
    for path in sorted((REPO / 'src/r2f/instrumentation').rglob('*.py')):
        digest.update(str(path.relative_to(REPO)).encode() + b'\0' + path.read_bytes())
    result['observer_source_sha256'] = digest.hexdigest()
    return result


def same_source(before, after):
    return all(before.get(key) == after.get(key) for key in ('head', 'runtime_source_sha256', 'observer_source_sha256'))


def normalize_environment_mutation(value):
    """Select the discovery carrier. None/absent preserves the legacy nft drop.

    Only ``discovery_drop`` and ``discovery_netem`` are defined. The netem spec
    is fully validated here so a bad field fails before any namespace work.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError('environment_mutation must be a dict or null')
    unknown = set(value) - {'kind', 'spec'}
    if unknown:
        raise ValueError('unknown environment_mutation field: ' + ','.join(sorted(unknown)))
    kind = value.get('kind')
    if kind not in ENVIRONMENT_MUTATION_KINDS:
        raise ValueError('unsupported environment_mutation kind: ' + str(kind))
    if kind == 'discovery_drop':
        if set(value) != {'kind'}:
            raise ValueError('discovery_drop environment_mutation accepts only kind')
        return {'kind': 'discovery_drop'}
    if 'spec' not in value:
        raise ValueError('discovery_netem requires spec')
    try:
        spec = normalize_netem_spec(value['spec'])
    except NetemScopeError as exc:
        # Preserve the offending field name instead of collapsing it into a
        # generic spec error; the dedicated carrier error carries it too.
        raise ValueError(f'invalid_netem_spec: {exc.field}: {exc}') from exc
    except DiscoveryNetworkError as exc:
        raise ValueError('invalid_netem_spec: ' + str(exc)) from exc
    return {'kind': 'discovery_netem', 'spec': spec}


def discovery_carrier_kind(config):
    environment_mutation = config.get('environment_mutation')
    return 'discovery_drop' if environment_mutation is None else environment_mutation['kind']


def normalize_discovery_config(config):
    c = dict(config)
    if c.get('target_id') == F2_WINDOWS_TARGET:
        f2_windows_operations(c)
    params = c['mutation_parameters']
    for key, default in (('drop_duration_sec', 3.0), ('recovery_timeout_sec', 8.0), ('counter_interval_sec', 0.25)):
        value = c.get(key, params.get(key, default))
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= 30:
            raise ValueError('invalid_discovery_parameter:' + key)
        c[key] = float(value)
    windows = c.get('environment_windows')
    if windows is not None:
        # Exploratory multi-window plan. Validation is input-only; execution
        # evidence is produced by the namespace body, never here.
        c['runtime_windows'] = validate_window_plan(windows)
        c['window_chain_requested'] = True
    else:
        c['runtime_windows'] = validate_window_plan([{
            'window_id': 'legacy',
            'window_index': 0,
            'trigger_anchor': 'fresh_match',
            'trigger_offset_sec': 0.0,
            'drop_duration_sec': c['drop_duration_sec'],
            'restore_interval_sec': 0.0,
        }])
        c['window_chain_requested'] = False
    validate_reader_churn_config(c)
    c['environment_mutation'] = normalize_environment_mutation(c.get('environment_mutation'))
    covers = c.get('netem_covers_delivery', False)
    if not isinstance(covers, bool):
        raise ValueError('invalid_netem_covers_delivery')
    c['netem_covers_delivery'] = covers
    if covers and (c['environment_mutation'] is None
                   or c['environment_mutation']['kind'] != 'discovery_netem'):
        raise ValueError('netem_covers_delivery_requires_discovery_netem')
    if (c['environment_mutation'] is not None
            and c['environment_mutation']['kind'] == 'discovery_netem'
            and windows is not None and len(c['runtime_windows']) > 1):
        # A single netem window replaces the single drop window and is what the
        # recovery runner always emits, so that shape stays admitted.  A 1..3
        # window *chain* is a drop/recovery gate with per-window counters; a
        # netem window has no drop semantics to chain, so refuse >1 instead of
        # silently reinterpreting one carrier as the other.
        raise ValueError('environment_windows_not_supported_for_discovery_netem')
    c['carrier_implementation'] = (NETEM_IMPLEMENTATION
                                   if c['environment_mutation'] is not None
                                   and c['environment_mutation']['kind'] == 'discovery_netem'
                                   else IMPLEMENTATION)
    c['window_budget_sec'] = (
        sum(w['trigger_offset_sec'] + w['drop_duration_sec'] + w['restore_interval_sec']
            for w in c['runtime_windows'])
        + c['recovery_timeout_sec'] * len(c['runtime_windows']))
    c['observe_extended_qos'] = True
    return c


def process_start_ticks(pid):
    return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19])


def validate_host_leases(request):
    """Validate inherited lock identity; no config flag can bypass host admission."""
    proof = assert_isolated_namespace()
    if request['host_pid'] != proof['launcher_pid'] or os.getppid() != request['host_pid']:
        raise IsolationError('namespace_owner_identity_mismatch')
    if request.get('host_preflight_ok') is not True or len(request.get('leases', [])) != 3:
        raise IsolationError('namespace_host_preflight_missing')
    for lease in request['leases']:
        fd = lease['fd']
        inherited, current = os.fstat(fd), os.stat(lease['path'])
        if (inherited.st_dev, inherited.st_ino) != (current.st_dev, current.st_ino):
            raise IsolationError('namespace_lease_inode_mismatch')
        owner = json.loads(os.pread(fd, 65536, 0))
        if owner != lease['owner'] or owner['pid'] != request['host_pid']:
            raise IsolationError('namespace_lease_owner_mismatch')
        # A separately opened description must conflict with our inherited flock.
        with open(lease['path'], 'r') as contender:
            try:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                fcntl.flock(contender, fcntl.LOCK_UN)
                raise IsolationError('namespace_lease_not_held')
    return proof


def _supervise(c, output_dir, environment, lease, source, emit):
    parent, child = socket.socketpair()
    process = None
    selector = selectors.DefaultSelector()
    workers, inner_result, buffer = {}, None, b''
    stderr_path = output_dir / 'namespace.stderr.log'
    request = {'config': c, 'output_dir': str(output_dir), 'host_pid': os.getpid(), 'host_preflight_ok': True,
               'source': source, 'leases': [dict(receipt, fd=handle.fileno())
                                          for receipt, handle in zip(lease.receipts, lease.handles)]}
    def consume(line):
        nonlocal inner_result
        try:
            event = json.loads(line)
            if not isinstance(event, dict) or 'event' not in event:
                raise ValueError('invalid namespace event')
        except (ValueError, TypeError):
            emit('namespace_stdout_diagnostic', text=line.decode(errors='replace'))
            return
        if event['event'] == '_namespace_result':
            if inner_result is not None:
                raise RuntimeError('duplicate_namespace_result')
            inner_result = event['result']
            return
        if event['event'] == 'process_started':
            pid = event.get('pid')
            if type(pid) is not int or pid <= 1:
                raise RuntimeError('invalid_worker_pid')
            # PID creation records arrive before endpoint tokens; retain start identity
            # for cleanup on unexpected controller exit, never signal a reused PID.
            workers[pid] = {'start_ticks': process_start_ticks(pid), 'role': event['role']}
        emit(event.pop('event'), **event)
    try:
        with stderr_path.open('xb') as stderr:
            argv = namespace_command(['/usr/bin/python3', '-B', '-m', __name__, '_child', str(child.fileno())],
                                     python_executable='/usr/bin/python3')
            process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=stderr,
                                       env=environment, cwd=REPO, start_new_session=True,
                                       pass_fds=(child.fileno(), *(h.fileno() for h in lease.handles)))
            child.close()
            parent.sendall(json.dumps(request, sort_keys=True).encode() + b'\n')
            emit('namespace_controller_started', controller_pid=process.pid, controller_pgid=process.pid,
                 implementation=('isolated_netns_passive_sedp_public_qos'
                                 if c['target_id'].startswith('builtin.discovery_projection_reconciliation')
                                 else c.get('carrier_implementation', IMPLEMENTATION)),
                 inherited_lease_count=len(lease.handles), stderr_path=str(stderr_path))
            os.set_blocking(process.stdout.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 30 + 4 * c['startup_timeout_sec'] + 4 * c['match_timeout_sec'] + c.get('window_budget_sec', c['drop_duration_sec'] + c['recovery_timeout_sec']) + c['delivery_timeout_sec'] + c['post_match_count'] * (c['publish_interval_sec'] + 1)
            source_check_at = 0
            while selector.get_map():
                now = time.monotonic()
                if now > deadline:
                    raise TimeoutError('namespace_controller_deadline')
                if now >= source_check_at:
                    if not same_source(source, execution_chain_identity()):
                        raise RuntimeError('source_identity_changed_during_execution')
                    source_check_at = now + 1
                for key, _ in selector.select(0.1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffer += chunk
                    while b'\n' in buffer:
                        line, buffer = buffer.split(b'\n', 1)
                        if line:
                            consume(line)
            if buffer:
                raise RuntimeError('namespace_stdout_truncated')
            code = process.wait(timeout=5)
            if code or inner_result is None:
                raise RuntimeError(f'namespace_controller_failed:exit={code}:result={inner_result is not None}')
            emit('namespace_controller_exited', controller_pid=process.pid, returncode=code)
            return inner_result
    finally:
        # EOF asks a live body to restore and close its workers while host leases stay held.
        parent.close()
        child.close()
        selector.close()
        if process is not None:
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
            for pid, identity in workers.items():
                try:
                    if process_start_ticks(pid) != identity['start_ticks']:
                        continue
                    # Child stdin EOF normally suffices. Escalate only our recorded
                    # worker identity after controller cleanup has already finished.
                    for sig in (signal.SIGTERM, signal.SIGKILL):
                        if process_start_ticks(pid) != identity['start_ticks']:
                            break
                        os.kill(pid, sig)
                        emit('owned_worker_cleanup_signal', worker_pid=pid, signal=int(sig))
                        end = time.monotonic() + 2
                        while time.monotonic() < end:
                            try:
                                if process_start_ticks(pid) != identity['start_ticks']:
                                    break
                                stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                                if stat[0] == 'Z':
                                    break
                            except FileNotFoundError:
                                break
                            time.sleep(0.02)
                        else:
                            continue
                        break
                except (FileNotFoundError, ProcessLookupError):
                    pass
            if process.stdout is not None:
                process.stdout.close()


def _write_role_profiles(output_dir, config):
    """Write each role's transport profile for its own vendor.

    The single-vendor helper is still used unchanged when ``rmws`` is absent.
    For mixed runs, Cyclone roles receive the Cyclone loopback XML and FastDDS
    roles receive the FastDDS profile; no role inherits another vendor's
    configuration path.
    """
    from .discovery_profiles import CYCLONE_XML, FAST_XML
    root = Path(output_dir) / 'profiles'
    root.mkdir(exist_ok=True)
    records, environments = {}, {}
    # Every churn role receives its own resolved vendor profile. Append
    # writer2 so its FastDDS participant ID is unique without changing any
    # existing role's index or profile hash.
    for index, role in enumerate(('writer', 'reader', 'reader2', 'reader3', 'reader4',
                                  'reader5', 'reader6', 'reader7', 'reader8', 'reader9',
                                  'writer2')):
        rmw = role_rmw(config, role)
        path = root / (role + '.xml')
        if path.exists():
            raise FileExistsError(str(path))
        text = CYCLONE_XML if rmw == 'rmw_cyclonedds_cpp' else FAST_XML.format(role=role, index=index)
        with path.open('x') as handle:
            handle.write(text)
        records[role] = {'path': str(path), 'sha256': hashlib.sha256(text.encode()).hexdigest(),
                         'kind': 'explicit_loopback_udp_profile', 'rmw': rmw,
                         'participant_index': 'auto' if rmw == 'rmw_cyclonedds_cpp' else index}
        key = 'CYCLONEDDS_URI' if rmw == 'rmw_cyclonedds_cpp' else 'FASTRTPS_DEFAULT_PROFILES_FILE'
        environments[role] = {key: str(path)}
    return records, environments


def run_discovery_pair(config, output_dir, result):
    """Host branch of the public run_pair entry; never called by the netns child."""
    from .ros2_pair import RECEIPT_KEYS
    try:
        if config['target_id'].startswith('builtin.discovery_projection_reconciliation'):
            from .projection_pair import normalize_projection_config
            c = normalize_projection_config(config)
        else:
            c = normalize_discovery_config(config)
            if c.get('fuzz_surfaces'):
                from .ros2_pair import exploratory_bindings, require_binding_preserved
                expected = exploratory_bindings(c)
                require_binding_preserved(expected, c)
                result['exploratory_bindings'] = expected
                result['runtime_capability'] = 'discovery_drop_window_chain'
    except (ValueError, RuntimeError) as exc:
        # New in-window lifecycle fields fail closed with RuntimeError so their
        # field name is explicit; admission must surface it as blocked, never
        # let an invalid stimulus escape as an unhandled host exception.
        result['reason'] = 'blocked:' + str(exc)
        return result
    result['effective_rmws'] = effective_rmws_receipt(c)
    projection = c['target_id'].startswith('builtin.discovery_projection_reconciliation')
    if projection and observation_uses_single_vendor_contract(c):
        # The projection body hard-codes one vendor in its readiness policy and
        # endpoint identity. Refuse before creating raw/state or any observer.
        result['reason'] = 'blocked:mixed_vendor_projection_not_supported'
        return result
    raw_path = output_dir / 'raw.jsonl'
    try:
        raw = raw_path.open('x', buffering=1)
    except FileExistsError:
        result['reason'] = 'blocked:state_directory_already_executed'
        return result
    events = result['events']
    result['raw_path'] = str(raw_path)
    implementation = ('isolated_netns_passive_sedp_public_qos' if projection
                      else c.get('carrier_implementation', IMPLEMENTATION))
    # Host-branch carrier selector: the finally-block summary needs the same
    # kind the netns body executed, so it cannot default to the drop facts.
    carrier_kind = discovery_carrier_kind(c)
    result['carrier_implementation'] = implementation
    result['carrier_kind'] = carrier_kind
    lease, source, started = None, execution_chain_identity(), time.monotonic_ns()
    def emit(event, **fields):
        token = {'event': event, 'role': 'host_controller', 'pid': os.getpid(), 'pgid': os.getpgrp(),
                 'run_id': c['run_id'], 'domain_id': c['domain_id'], 'time_ns': time.time_ns(),
                 'monotonic_ns': time.monotonic_ns(), **fields}
        token['event_index'] = len(events)
        if projection and event in ('mutation_begin', 'control_begin'):
            token['precondition_receipt_refs'] = [{'step_id': name,
                'raw_before_index': next(e['event_index'] for e in events if e['event'] == 'state_step_before' and e['step_id'] == name),
                'raw_after_index': next(e['event_index'] for e in events if e['event'] == 'state_step_after' and e['step_id'] == name)}
                for name in ('namespace_ready', 'observer_ready')]
        if projection and event == 'projection_graph_exported':
            for proof in token['graph_receipts']:
                for label in ('request', 'snapshot', 'match', 'ack'):
                    namespace_index = proof.pop(label + '_namespace_event_index')
                    proof[label + '_index'] = next(e['event_index'] for e in events
                        if e.get('namespace_event_index') == namespace_index)
        token.setdefault('raw_event_ref', f'{raw_path}#event_index={len(events)}')
        events.append(token)
        raw.write(json.dumps(token, sort_keys=True) + '\n')
        if event == 'endpoint_created':
            result['real_execution'] = True
    try:
        lease = LeaseSet(Path(c.get('lock_root', f'/tmp/r2f-runtime-locks-{os.getuid()}')), output_dir, c['campaign_id'], c['domain_id'])
        lease.__enter__()
        emit('isolation_locks_acquired', locks=lease.receipts)
        emit('effective_rmws', receipt=result['effective_rmws'],
             mixed=result['effective_rmws']['mixed'])
        emit('runtime_source_snapshot', source=source)
        environment = check_environment(c)
        result['environment'] = environment
        emit('environment_preflight', ok=environment['ok'], reason=environment.get('reason'))
        if not environment['ok']:
            raise IsolationError(environment.get('reason', 'host_preflight_failed'))
        from .ros2_pair import require_rmws_available
        require_rmws_available(environment, c)
        _, child_environment = activated_environment(c)
        inner = _supervise(c, output_dir, child_environment, lease, source, emit)
        for key in ('effective_rmws', 'effective_rmws_observed'):
            if key in inner:
                result[key] = inner[key]
        for key in ('runtime_status', 'reason', 'facts', 'precondition_receipts', 'observation_receipts', 'mutation_receipt'):
            result[key] = inner[key]
        original_reason = result['reason']
        result['reason'] = mixed_worker_start_failure(c, original_reason)
        if result['reason'] != original_reason:
            emit('mixed_vendor_worker_failure_blocked', original_reason=original_reason,
                 reason=result['reason'])
        result['status'] = result['runtime_status']
        result['effective_rmws'] = effective_rmws_receipt(c, observed_rmws_from_events(events))
        result['environment']['discovery_namespace'] = inner['environment']
        result['_workers'] = inner['workers']
        admitted = (len(inner['precondition_receipts']) == (2 if projection else 3)
                    and all(record.get('satisfied') is True for record in inner['precondition_receipts']))
        capability = result['capabilities']['projection_observation' if projection else 'discovery_network']
        capability['admitted'] = admitted
        if admitted:
            capability.update(available=True, availability_status='live_preflight_passed',
                admission_evidence_refs=[record['after'].get('raw_event_ref')
                                         for record in inner['precondition_receipts']])
    except (OSError, RuntimeError, TimeoutError, ValueError, subprocess.SubprocessError) as exc:
        reason = str(exc) if result['real_execution'] else mixed_worker_start_failure(c, str(exc))
        result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                      status='failed' if result['real_execution'] else 'blocked', reason=reason)
        emit('runtime_abort', reason=reason, error_receipt=getattr(exc, 'receipt', None))
    finally:
        after = execution_chain_identity()
        result['environment']['execution_chain_source_before'] = source
        result['environment']['execution_chain_source_after'] = after
        if not same_source(source, after):
            result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                          status='failed' if result['real_execution'] else 'blocked', reason='source_identity_changed_during_execution')
            emit('runtime_abort', reason=result['reason'])
        workers = result.pop('_workers', [])
        if result['facts'] and not projection:
            previous = result['facts']
            result['facts'] = summarize_discovery(events,
                active=(previous['discovery_carrier_mutation_applied']
                        if carrier_kind == 'discovery_netem' else previous['discovery_drop_applied']),
                counter_hit=(previous['discovery_netem_counter_hit']
                             if carrier_kind == 'discovery_netem' else previous['discovery_drop_counter_hit']),
                restored=previous['discovery_restore_verified'], activity_ok=previous['discovery_recovery_activity_observed'],
                fresh_recovery=previous['recovery_fresh_count_graph'], drop_activity_ok=previous['discovery_drop_activity_observed'],
                carrier_implementation=implementation, carrier_kind=carrier_kind,
                neutral_spec=bool(previous.get('discovery_carrier_neutral_spec', False)),
                # Re-deriving the summary must not erase carrier-level delivery
                # facts the body already established (netem coverage and the
                # data-plane observation limit).
                delivery_overlap=(None if previous.get('delivery_inside_mutation_window') is None
                                  else {'inside': bool(previous['delivery_inside_mutation_window']),
                                        'window_refs': previous.get('delivery_window_refs'),
                                        'install_ref': previous.get('netem_install_ref'),
                                        'restore_ref': previous.get('netem_restore_ref'),
                                        'boundary_ref': previous.get('netem_boundary_ref'),
                                        'chronology': previous.get('delivery_overlap_chronology')}),
                data_plane_observed=previous.get('data_plane_observed'),
                data_plane_reason=previous.get('data_plane_observation_reason'))
        result['facts']['candidate_eligible'] = c['arm'] != 'negative'
        if result['real_execution']:
            result['execution'] = {'real_execution': True, 'run_id': c['run_id'], 'target_id': c['target_id'],
                'target_revision': c['target_revision'], 'rmw': c['rmw'], 'domain_id': c['domain_id'], 'arm': c['arm'],
                'effective_rmws': result.get('effective_rmws') or effective_rmws_receipt(c),
                'repeat': c['repeat'], 'runtime_status': result['runtime_status'], 'started_monotonic_ns': started,
                'finished_monotonic_ns': time.monotonic_ns(), 'source': source, 'source_after': after,
                'workers': workers, 'carrier_implementation': implementation,
                'precondition_receipts': result['precondition_receipts'], 'observation_receipts': result['observation_receipts'],
                'mutation_receipt': result['mutation_receipt'], 'fault_control': bool(c['fault_id']),
                'candidate_eligible': not bool(c['fault_id']), 'evidence_purpose': c.get('evidence_purpose', 'runtime_execution')}
            result['execution_metadata'] = result['execution']
        try:
            emit('runtime_end', runtime_status=result['runtime_status'], real_execution=result['real_execution'],
                 mutation_receipt=result['mutation_receipt'], reason=result['reason'])
            raw.flush()
            os.fsync(raw.fileno())
            raw.close()
            result['raw_sha256'] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            if c.get('target_id') == F2_WINDOWS_TARGET:
                result['operation_receipts'] = []
                for event in events:
                    if event.get('event') != 'f2_operation_receipt':
                        continue
                    payload = dict(event['receipt'], raw_event_ref=event['raw_event_ref'])
                    selected = [_f2_event(events, ref) for ref in payload['evidence_refs']] + [event]
                    payload['raw_refs'] = [{'path': str(raw_path), 'sha256': result['raw_sha256'],
                                            'event_index': e['event_index']} for e in selected]
                    result['operation_receipts'].append(payload)
            if projection:
                from .projection_pair import finalize_projection
                finalize_projection(result)
            if result['execution'] is not None:
                result['execution'].update(raw_path=str(raw_path), raw_sha256=result['raw_sha256'])
                (output_dir / 'execution.json').write_text(json.dumps(result['execution'], sort_keys=True, indent=2) + '\n')
            (output_dir / 'environment.json').write_text(json.dumps(result['environment'], sort_keys=True, indent=2) + '\n')
            (output_dir / 'runtime_result.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
        finally:
            if not raw.closed:
                raw.close()
            if lease is not None:
                lease.__exit__(None, None, None)
    return result


def acknowledged_graphs(events, matches, endpoints):
    """Use the graph emitted by the same completed snapshot command, not a cache token."""
    result = {}
    for role in ('writer', 'reader'):
        cid = matches[role]['command_id']
        graphs = [(i, e) for i, e in enumerate(events) if e['event'] == 'graph_snapshot'
                  and e.get('role') == role and e.get('command_id') == cid]
        acks = [(i, e) for i, e in enumerate(events) if e['event'] == 'command_done'
                and e.get('role') == role and e.get('command_id') == cid]
        if len(graphs) != 1 or len(acks) != 1:
            raise RuntimeError('graph_command_evidence_incomplete:' + role)
        (gi, graph), (ai, ack) = graphs[0], acks[0]
        if not gi < ai or graph['monotonic_ns'] > ack['monotonic_ns']:
            raise RuntimeError('graph_ack_chronology_invalid:' + role)
        if ({p['endpoint_gid'] for p in graph['publishers']} != {endpoints['writer']['endpoint_gid']}
                or {p['endpoint_gid'] for p in graph['subscriptions']} != {endpoints['reader']['endpoint_gid']}):
            raise RuntimeError('graph_pair_gid_mismatch:' + role)
        result[role] = graph
    return result


def summarize_discovery(events, *, active, counter_hit, restored, activity_ok, fresh_recovery, drop_activity_ok,
                        carrier_implementation=IMPLEMENTATION, carrier_kind='discovery_drop',
                        neutral_spec=False, netem_scope=None, delivery_overlap=None,
                        data_plane_observed=None, data_plane_reason=None):
    from .ros2_pair import summarize_events
    # Reuse sequence/identity algebra, retaining original raw tokens and phase labels.
    normalized = []
    for event in events:
        token = dict(event)
        if token.get('phase') == 'post_restore':
            token['phase'] = 'post_match'
        if token.get('window') == 'post_restore_delivery':
            token['window'] = 'post_match_delivery'
        normalized.append(token)
    facts = summarize_events(normalized)
    netem = carrier_kind == 'discovery_netem'
    if not netem:
        # Legacy drop path: exact prior facts, byte-for-byte unchanged.
        facts.update(discovery_drop_counter_hit=bool(counter_hit), discovery_drop_activity_observed=bool(drop_activity_ok),
                     discovery_drop_applied=bool(active), discovery_restore_verified=restored if active else None,
                     discovery_recovery_activity_observed=bool(activity_ok), recovery_fresh_count_graph=bool(fresh_recovery),
                     post_restore_expected_sequences=facts['expected_post_match_sequences'],
                     post_restore_sent_sequences=facts['post_match_sent_sequences'],
                     post_restore_received_sequences=facts['post_match_received_sequences'],
                     discovery_recovery_status='observed' if activity_ok and fresh_recovery and (restored or not active) else 'unknown',
                     match_continuity_required=False, carrier_implementation=IMPLEMENTATION,
                     recovery_basis='pair_discovery_traffic_and_fresh_public_graph_count_and_delivery',
                     packet_observation_limit='AF_PACKET witnesses traffic, not delivery into DDS; combined with rule deletion and public endpoint evidence',
                     neutral_control=not active)
        return facts
    facts.update(discovery_drop_counter_hit=False,
                 discovery_drop_activity_observed=bool(drop_activity_ok),
                 discovery_drop_applied=None,
                 discovery_carrier='discovery_netem',
                 discovery_carrier_mutation_applied=bool(active),
                 discovery_carrier_neutral_spec=bool(neutral_spec),
                 discovery_netem_counter_hit=bool(counter_hit),
                 discovery_restore_verified=restored if active else None,
                 discovery_recovery_activity_observed=bool(activity_ok), recovery_fresh_count_graph=bool(fresh_recovery),
                 post_restore_expected_sequences=facts['expected_post_match_sequences'],
                 post_restore_sent_sequences=facts['post_match_sent_sequences'],
                 post_restore_received_sequences=facts['post_match_received_sequences'],
                 discovery_recovery_status='observed' if activity_ok and fresh_recovery and (restored or not active) else 'unknown',
                 match_continuity_required=False, carrier_implementation=carrier_implementation,
                 recovery_basis='pair_discovery_traffic_and_fresh_public_graph_count_and_delivery',
                 packet_observation_limit='AF_PACKET witnesses traffic, not delivery into DDS; combined with tc netem qdisc removal and public endpoint evidence',
                 neutral_control=bool(not active or neutral_spec),
                 netem_hit_is_not_drop_hit=True)
    if netem_scope is not None:
        # Additive audit field: which port classes the netem class matched.
        facts.update(discovery_netem_scope=netem_scope,
                     discovery_netem_responsibility_layer=NETEM_RESPONSIBILITY_LAYER,
                     discovery_netem_loss_pct=NETEM_LOSS_PCT)
    if delivery_overlap is not None:
        # Facts only: whether the judged delivery window sat inside the
        # installed class.  The oracle still judges send/receive sequences and
        # identity; this never promotes or demotes a candidate.
        facts.update(delivery_inside_mutation_window=bool(delivery_overlap['inside']),
                     delivery_window_refs=delivery_overlap['window_refs'],
                     netem_install_ref=delivery_overlap['install_ref'],
                     netem_restore_ref=delivery_overlap['restore_ref'],
                     netem_boundary_ref=delivery_overlap['boundary_ref'],
                     delivery_overlap_chronology=delivery_overlap['chronology'])
    if data_plane_observed is not None:
        # Distinguish "window did not overlap" (tool defect) from "the data
        # plane used another transport / was not countable" (observation
        # limit).  Both stay separate from a mutation_ineffective verdict.
        facts.update(data_plane_observed=bool(data_plane_observed),
                     data_plane_observation_reason=data_plane_reason)
    return facts


class OwnerCheckedPool(RoleRmwWorkerPool):
    def __init__(self, *args, owner_channel, **kwargs):
        super().__init__(*args, **kwargs)
        self.owner_channel = owner_channel
        self.check_owner = True

    def pump(self, timeout=0.05):
        if self.check_owner:
            try:
                chunk = self.owner_channel.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                if not chunk:
                    raise RuntimeError('host_owner_channel_closed')
            except BlockingIOError:
                pass
        super().pump(timeout)


class _RecoveryMatchTrajectory:
    """Raw-only public API diagnostics; never a match gate or an expiry claim.

    Snapshot commands run through the existing pump. Each scheduled pair has an
    absolute response deadline, so a missing ACK never adds a wait before restore.
    Worker events remain untouched, including zero counts and late responses.
    """
    INTERVAL_NS = 1_000_000_000
    RESPONSE_NS = 500_000_000
    IDENTITY_KEYS = ('pid', 'process_start_ticks', 'endpoint_generation',
                     'rmw_identifier', 'run_id', 'domain_id')

    def __init__(self, events, endpoints, emit, start_ns, deadline_ns, before):
        self.events, self.endpoints, self.emit = events, endpoints, emit
        self.start_ns, self.deadline_ns, self.before = start_ns, deadline_ns, before
        self.after = None
        self.restore_started_ns = self.restore_boundary_ns = None
        final_ns = max(start_ns, deadline_ns - min(self.RESPONSE_NS, (deadline_ns - start_ns) // 2))
        self.schedule = [(stamp, 'periodic') for stamp in
                         range(start_ns + self.INTERVAL_NS, final_ns, self.INTERVAL_NS)]
        self.schedule.append((final_ns, 'pre_restore'))
        self.samples = []

    @property
    def next_due_ns(self):
        return self.schedule[len(self.samples)][0] if len(self.samples) < len(self.schedule) else self.deadline_ns

    def poll(self, pool):
        while len(self.samples) < len(self.schedule) and time.monotonic_ns() >= self.next_due_ns:
            due_ns, purpose = self.schedule[len(self.samples)]
            deadline_ns = min(due_ns + self.RESPONSE_NS, self.deadline_ns)
            sample = {'scheduled_monotonic_ns': due_ns, 'deadline_monotonic_ns': deadline_ns,
                      'purpose': purpose, 'requests': {}, 'errors': []}
            self.samples.append(sample)
            for role in ('writer', 'reader'):
                requested_ns = time.monotonic_ns()
                command_id, error = None, None
                if requested_ns >= deadline_ns:
                    error = 'request_deadline_elapsed'
                else:
                    try:
                        command_id = pool.command(role, 'snapshot', requested_monotonic_ns=requested_ns,
                                                  diagnostic_purpose='discovery_match_trajectory')
                    except (OSError, ValueError) as exc:
                        error = type(exc).__name__ + ':' + str(exc)
                record = self.emit('discovery_match_trajectory_requested', worker_role=role,
                    command_id=command_id, requested_monotonic_ns=requested_ns,
                    deadline_monotonic_ns=deadline_ns, scheduled_monotonic_ns=due_ns,
                    purpose=purpose, command_error=error,
                    worker_identity={key: self.endpoints[role][key] for key in self.IDENTITY_KEYS})
                sample['requests'][role] = record
                if error:
                    sample['errors'].append(role + ':' + error)

    def _gate_requests(self, evidence):
        result = {}
        for role in ('writer', 'reader'):
            reference = (evidence or {}).get('matches', {}).get(role)
            snapshots = [e for e in self.events if e['event'] == 'match_snapshot'
                         and e.get('raw_event_ref') == reference and e.get('role') == role]
            if len(snapshots) == 1:
                requests = [e for e in self.events if e['event'] == 'match_snapshot_requested'
                            and e.get('role') == role and e.get('command_id') == snapshots[0].get('command_id')]
                if len(requests) == 1:
                    result[role] = requests[0]
        return result

    def _pair(self, requests, lower_ns, upper_ns):
        pair = {'complete': True, 'roles': {}}
        for role, remote in (('writer', 'reader'), ('reader', 'writer')):
            request = requests.get(role, {})
            cid, requested_ns = request.get('command_id'), request.get('requested_monotonic_ns')
            errors, found = [], {}
            observation = {'complete': False, 'command_id': cid, 'request_ref': request.get('raw_event_ref'),
                           'requested_monotonic_ns': requested_ns, 'response_deadline_monotonic_ns': upper_ns,
                           'errors': errors}
            pair['roles'][role] = observation
            if (type(cid) is not int or type(requested_ns) is not int
                    or type(request.get('monotonic_ns')) is not int
                    or type(lower_ns) is not int or type(upper_ns) is not int
                    or not lower_ns <= requested_ns <= request.get('monotonic_ns', -1) < upper_ns):
                errors.append('request_boundary_or_identity_incomplete')
            if (type(cid) is int and cid == requests.get(remote, {}).get('command_id')):
                errors.append('command_pair_not_independent')
            if request.get('event') == 'discovery_match_trajectory_requested' and (
                    request.get('worker_role') != role or request.get('worker_identity') !=
                    {key: self.endpoints[role][key] for key in self.IDENTITY_KEYS}):
                errors.append('request_worker_identity_invalid')
            for kind in ('match_snapshot', 'graph_snapshot', 'command_done'):
                matches = [e for e in self.events if type(cid) is int and e['event'] == kind
                           and e.get('role') == role and type(e.get('command_id')) is int and e['command_id'] == cid]
                observation[kind + '_refs'] = [e.get('raw_event_ref') for e in matches]
                if len(matches) != 1:
                    errors.append(kind + '_missing_or_duplicate')
                else:
                    found[kind] = matches[0]
            if 'match_snapshot' in found:
                snapshot = found['match_snapshot']
                observation.update(matched_count=snapshot.get('matched_count'),
                    sampled_monotonic_ns=snapshot.get('sampled_monotonic_ns'))
            if 'graph_snapshot' in found:
                graph = found['graph_snapshot']
                publishers, subscriptions = graph.get('publishers'), graph.get('subscriptions')
                if (not isinstance(publishers, list) or not isinstance(subscriptions, list)
                        or any(not isinstance(x, dict) or not isinstance(x.get('endpoint_gid'), str)
                               for x in (publishers or []) + (subscriptions or []))):
                    errors.append('graph_guid_evidence_incomplete')
                else:
                    pgids = [x['endpoint_gid'] for x in publishers]
                    sgids = [x['endpoint_gid'] for x in subscriptions]
                    observation.update(publisher_gids=pgids, subscription_gids=sgids,
                        expected_remote_gid=self.endpoints[remote]['endpoint_gid'],
                        remote_guid_present=self.endpoints[remote]['endpoint_gid'] in (sgids if role == 'writer' else pgids))
            observation['timed_out'] = (any(not observation[k + '_refs'] for k in
                ('match_snapshot', 'graph_snapshot', 'command_done')) or
                type(upper_ns) is int and any(type(e.get('monotonic_ns')) is int
                    and e['monotonic_ns'] >= upper_ns for e in found.values()))
            if observation['timed_out']:
                errors.append('response_timeout_or_late_evidence')
            if len(found) == 3:
                snapshot, graph, ack = (found[k] for k in ('match_snapshot', 'graph_snapshot', 'command_done'))
                stamps = [requested_ns, snapshot.get('sampled_monotonic_ns'), snapshot.get('monotonic_ns'),
                          graph.get('monotonic_ns'), ack.get('monotonic_ns'), upper_ns]
                # The parent's request record may follow the worker sample in time.
                # Only the worker chain, and separately the request record, use the deadline.
                indices = [e.get('namespace_event_index') for e in (request, snapshot, graph, ack)]
                if (not all(type(t) is int for t in stamps) or stamps != sorted(stamps)
                        or type(upper_ns) is not int or ack.get('monotonic_ns', upper_ns) >= upper_ns
                        or not all(type(i) is int for i in indices) or any(a >= b for a, b in zip(indices, indices[1:]))):
                    errors.append('worker_clock_or_event_order_invalid')
                if (any(any(e.get(k) != self.endpoints[role].get(k) for k in self.IDENTITY_KEYS)
                        for e in (snapshot, graph, ack))
                        or snapshot.get('endpoint_gid') != self.endpoints[role]['endpoint_gid']
                        or snapshot.get('count_api') != ('get_subscription_count' if role == 'writer' else 'get_publisher_count')
                        or type(snapshot.get('matched_count')) is not int or snapshot['matched_count'] < 0):
                    errors.append('worker_count_or_identity_invalid')
            observation['complete'] = not errors
            observation['loss_observed'] = (not errors and observation['matched_count'] == 0
                                            and observation.get('remote_guid_present') is False)
            observation['matched_observed'] = (not errors and observation['matched_count'] == 1
                and observation.get('publisher_gids') == [self.endpoints['writer']['endpoint_gid']]
                and observation.get('subscription_gids') == [self.endpoints['reader']['endpoint_gid']])
            pair['complete'] = pair['complete'] and observation['complete']
        pair['both_matched'] = all(r['matched_observed'] for r in pair['roles'].values())
        pair['loss_roles'] = [role for role, r in pair['roles'].items() if r['loss_observed']]
        return pair

    def summary(self):
        before = self._pair(self._gate_requests(self.before),
                            self.endpoints['reader']['created_monotonic_ns'], self.start_ns)
        openings = [e for e in self.events if e['event'] == 'window_open' and e.get('window') == 'post_restore_delivery']
        after = self._pair(self._gate_requests(self.after), self.restore_boundary_ns,
                           openings[0]['monotonic_ns'] if len(openings) == 1 else None)
        samples = []
        for sample in self.samples:
            upper = min(sample['deadline_monotonic_ns'], self.restore_started_ns or self.deadline_ns)
            samples.append({**sample, **self._pair(sample['requests'], sample['scheduled_monotonic_ns'], upper)})
        complete = (len(samples) == len(self.schedule) and all(s['complete'] and not s['errors'] for s in samples)
                    and before['both_matched'] and after['both_matched']
                    and type(self.restore_started_ns) is int and type(self.restore_boundary_ns) is int
                    and self.deadline_ns <= self.restore_started_ns <= self.restore_boundary_ns)
        bilateral = [i for i, sample in enumerate(samples) if sample['loss_roles'] == ['writer', 'reader']]
        status = 'incomplete'
        if complete and bilateral:
            status = 'observed_loss_then_recovery'
        elif complete and all(s['both_matched'] for s in samples):
            status = 'no_loss_at_sampled_points'
        return {'status': status, 'scope': 'sampled_public_count_and_remote_guid_graph_only',
                'limitation': 'No claim about gaps between samples, internal remote state, or participant lease expiry.',
                'start_monotonic_ns': self.start_ns, 'drop_deadline_monotonic_ns': self.deadline_ns,
                'restore_started_monotonic_ns': self.restore_started_ns,
                'restore_boundary_monotonic_ns': self.restore_boundary_ns,
                'expected_samples': len(self.schedule),
                'unsampled_scheduled_monotonic_ns': [stamp for stamp, _ in self.schedule[len(samples):]],
                'before': before, 'samples': samples, 'after': after,
                'bilateral_loss_sample_indices': bilateral,
                'primary_verdict_input': False}


MAX_READER_RECREATE_COUNT = 8
MAX_PUBLISH_BURST_COUNT = 4096


def _strict_fraction(value, field):
    """Return a finite float in [0,1), rejecting bools and numeric strings."""
    if isinstance(value, bool) or type(value) not in (int, float):
        raise RuntimeError(field + ' must be a number in [0,1)')
    value = float(value)
    if not math.isfinite(value) or not 0 <= value < 1:
        raise RuntimeError(field + ' must be in [0,1)')
    return value


def _strict_seconds(value, field):
    if isinstance(value, bool) or type(value) not in (int, float):
        raise RuntimeError(field + ' must be a nonnegative number')
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise RuntimeError(field + ' must be nonnegative')
    return value


def reader_churn_plan(c, drop_duration_sec):
    """Validate and fully resolve the in-window reader generation schedule.

    The legacy reader_recreate_at_fraction/interval formula remains the
    compatibility path.  reader_recreate_fractions, when present, is the
    authoritative absolute relative schedule and must contain exactly one
    strictly increasing fraction per generation.
    """
    if c.get('reader_recreate_in_window') is not True:
        return []
    count = c.get('reader_recreate_count', 1)
    if isinstance(count, bool) or type(count) is not int or not 1 <= count <= MAX_READER_RECREATE_COUNT:
        raise RuntimeError('reader_recreate_count must be an integer in 1..' + str(MAX_READER_RECREATE_COUNT))
    fractions = c.get('reader_recreate_fractions')
    if fractions is not None:
        if not isinstance(fractions, list):
            raise RuntimeError('reader_recreate_fractions must be a list')
        if len(fractions) != count:
            raise RuntimeError('reader_recreate_fractions length must equal reader_recreate_count')
        parsed = [_strict_fraction(value, 'reader_recreate_fractions[%d]' % index)
                  for index, value in enumerate(fractions)]
        if any(left >= right for left, right in zip(parsed, parsed[1:])):
            raise RuntimeError('reader_recreate_fractions must be strictly increasing')
        delays = [drop_duration_sec * value for value in parsed]
    else:
        fraction = c.get('reader_recreate_at_fraction')
        if fraction is not None:
            base_delay = drop_duration_sec * _strict_fraction(fraction, 'reader_recreate_at_fraction')
        else:
            base_delay = _strict_seconds(c.get('reader_recreate_delay_sec', 0.0),
                                         'reader_recreate_delay_sec')
        interval = c.get('reader_recreate_interval_sec')
        if interval is None:
            interval = drop_duration_sec * 0.2
        else:
            interval = _strict_seconds(interval, 'reader_recreate_interval_sec')
        # Preserve the legacy clamp exactly: generation 1 starts at base_delay
        # and later generations are capped at 90% of the window.
        delays = [base_delay if generation == 0 else min(
            drop_duration_sec * 0.9, base_delay + generation * interval) for generation in range(count)]
    return [{'role': 'reader%d' % (index + 2), 'generation': index + 1,
             'delay_sec': delay, 'fraction': delay / drop_duration_sec}
            for index, delay in enumerate(delays)]


def reader_destroy_schedule(c, count):
    """Validate which already-created readerN roles are destroyed in-window."""
    destroy = c.get('reader_destroy_fractions')
    if destroy is None:
        return []
    if not isinstance(destroy, list):
        raise RuntimeError('reader_destroy_fractions must be a list')
    if len(destroy) > count:
        raise RuntimeError('reader_destroy_fractions length must not exceed reader_recreate_count')
    parsed = [_strict_fraction(value, 'reader_destroy_fractions[%d]' % index)
              for index, value in enumerate(destroy)]
    if any(left >= right for left, right in zip(parsed, parsed[1:])):
        raise RuntimeError('reader_destroy_fractions must be strictly increasing')
    return [{'role': 'reader%d' % (index + 2), 'index': index, 'fraction': value}
            for index, value in enumerate(parsed)]


def validate_reader_churn_config(c):
    """Fail closed on all new in-window fields; no silent no-op configs."""
    for field in ('reader_recreate_in_window', 'publish_burst_in_window'):
        if type(c.get(field, False)) is not bool:
            raise RuntimeError(field + ' must be a boolean')
    plan = c.get('runtime_windows', [])
    durations = [item['drop_duration_sec'] for item in plan] if plan else [float(c.get('drop_duration_sec', 3.0))]
    # Validate each requested stimulus even when the arm is neutral: an invalid
    # exploratory config must not appear as a healthy baseline/control run.
    for duration in durations:
        schedule = reader_churn_plan(c, duration)
        destroys = reader_destroy_schedule(c, len(schedule))
        created_at = {item['role']: item['fraction'] for item in schedule}
        for item in destroys:
            if item['fraction'] <= created_at[item['role']]:
                raise RuntimeError('reader_destroy_fractions[%d] must be later than %s creation'
                                   % (item['index'], item['role']))
    writer_churn = c.get('writer_churn_in_window', False)
    if type(writer_churn) is not bool:
        raise RuntimeError('writer_churn_in_window must be a boolean')
    burst_count = c.get('publish_burst_count', 32)
    if type(burst_count) is not int or not 1 <= burst_count <= MAX_PUBLISH_BURST_COUNT:
        raise RuntimeError('publish_burst_count must be an integer in 1..' + str(MAX_PUBLISH_BURST_COUNT))


def _schedule_event(schedule, kind, fraction, role=None):
    schedule.append({'kind': kind, 'fraction': fraction, 'role': role})


def window_churn_schedule(c, drop_duration_sec):
    """Build one ordered timeline for reader create/destroy and writer2 churn.

    Every action is tied to its raw creation timestamp when it is emitted; the
    schedule only decides due times here and never back-fills reached state.
    """
    if c.get('reader_recreate_in_window') is not True:
        readers = []
        destroys = []
    else:
        readers = reader_churn_plan(c, drop_duration_sec)
        destroys = reader_destroy_schedule(c, len(readers))
    schedule = []
    for item in readers:
        _schedule_event(schedule, 'reader_create', item['fraction'], item['role'])
    for item in destroys:
        _schedule_event(schedule, 'reader_destroy', item['fraction'], item['role'])
    if c.get('writer_churn_in_window') is True:
        _schedule_event(schedule, 'writer_create', 0.0, 'writer2')
        _schedule_event(schedule, 'writer_publish', 0.0, 'writer2')
        _schedule_event(schedule, 'writer_destroy', 0.9, 'writer2')
    if c.get('publish_burst_in_window') is True:
        _schedule_event(schedule, 'pair_publish', 0.0, 'writer')
    # Stable sort: for equal fractions creation (reader then writer2) occurs
    # before destruction, and destruction preserves role order.  This is
    # deterministic and therefore testable without a clock.
    order = {'reader_create': 0, 'writer_create': 1, 'writer_publish': 2,
             'pair_publish': 2, 'reader_destroy': 3, 'writer_destroy': 4}
    schedule.sort(key=lambda item: (item['fraction'], order[item['kind']],
                                    item.get('role') or ''))
    return schedule


def _churn_ref(events, token):
    # Namespace and host raw use different index keys. The append-only list
    # position is the local boundary authority, not a producer's claimed index.
    index = token.get('namespace_event_index', token.get('event_index'))
    if type(index) is not int or not 0 <= index < len(events) or events[index] is not token:
        index = next(i for i, event in enumerate(events) if event is token)
    return {'raw_event_ref': token.get('raw_event_ref'), 'local_event_index': index,
            'namespace_event_index': token.get('namespace_event_index')}


def _churn_identity(token):
    identity = {key: token.get(key) for key in ('pid', 'endpoint_generation')}
    if (type(identity['pid']) is not int or identity['pid'] <= 0
            or not isinstance(identity['endpoint_generation'], str)
            or not identity['endpoint_generation']):
        raise RuntimeError('churn_worker_identity_incomplete')
    if 'worker_generation' in token:
        identity['worker_generation'] = token['worker_generation']
    return identity


def _churn_process(pool, role):
    process = getattr(pool, 'workers', {}).get(role)
    if (type(getattr(process, 'pid', None)) is not int or process.pid <= 0
            or not callable(getattr(process, 'poll', None))):
        raise RuntimeError('churn_process_identity_missing:' + role)
    return process


def _churn_current(pool, events, role):
    process = _churn_process(pool, role)
    pid = process.pid
    tokens = [e for e in events if e['event'] == 'endpoint_created'
              and e.get('role') == role and e.get('pid') == pid]
    if not tokens:
        raise RuntimeError('churn_current_endpoint_missing:' + role)
    token = tokens[-1]
    _churn_identity(token)
    return token


def _churn_timeout(deadline_ns, startup_timeout_sec):
    remaining = (deadline_ns - time.monotonic_ns()) / 1e9
    if remaining <= 0:
        raise RuntimeError('window_churn_deadline_exceeded')
    return min(startup_timeout_sec, remaining)


def _fresh_churn_events(events, boundary):
    for index in range(boundary, len(events)):
        token = events[index]
        # If an index is supplied it must describe this new append, not an
        # old receipt copied into a later list position.
        if all(key not in token or (type(token[key]) is int and token[key] == index)
               for key in ('namespace_event_index', 'event_index')):
            yield token


def _churn_wait(pool, events, boundary, role, kind, identity, timeout, label,
                *, after_monotonic_ns=None, **fields):
    def candidates():
        return [e for e in _fresh_churn_events(events, boundary) if e['event'] == kind
                and e.get('role') == role
                and (after_monotonic_ns is None or (type(e.get('monotonic_ns')) is int
                     and e['monotonic_ns'] >= after_monotonic_ns))
                and all(e.get(key) == value for key, value in {**identity, **fields}.items())]
    pool.wait(lambda: bool(candidates()), timeout, label)
    tokens = candidates()
    if len(tokens) != 1:
        raise RuntimeError('churn_missing_or_duplicate_evidence:' + label)
    return tokens[0]


def _stop_churn_role(pool, events, role, *, emit, startup_timeout_sec,
                     deadline_ns=None, reason='window_cleanup', requested_fraction=None):
    token = _churn_current(pool, events, role)
    identity = _churn_identity(token)
    prefix = ('writer' if role.startswith('writer') else
              'reader' if role.startswith('reader') else 'probe')
    requested_ns = time.monotonic_ns()
    # One shared absolute budget includes command submission, shutdown and exit.
    stop_deadline = requested_ns + int(startup_timeout_sec * 1e9)
    if deadline_ns is not None:
        stop_deadline = min(stop_deadline, deadline_ns)
    request = emit('window_churn_action_requested', action=prefix + '_destroy',
                   command='stop', requested_monotonic_ns=requested_ns,
                   deadline_monotonic_ns=stop_deadline, worker_role=role,
                   worker_identity=identity, endpoint_ref=_churn_ref(events, token),
                   reason=reason, requested_fraction=requested_fraction)
    boundary = len(events)
    _churn_timeout(stop_deadline, startup_timeout_sec)
    command_id = pool.command(role, 'stop')
    shutdown = _churn_wait(pool, events, boundary, role, 'worker_shutdown', identity,
        _churn_timeout(stop_deadline, startup_timeout_sec), role + '_stop',
        stop_command_id=command_id, after_monotonic_ns=requested_ns)
    process = _churn_process(pool, role)
    pool.wait(lambda: process.poll() is not None,
              _churn_timeout(stop_deadline, startup_timeout_sec), role + '_exit')
    if process.returncode != 0:
        raise RuntimeError('churn_worker_exit_failed:' + role)
    return emit('discovery_' + prefix + '_destroyed', role=role,
                **{prefix + '_gid': token.get('endpoint_gid'), prefix + '_pid': identity['pid']},
                generation=identity['endpoint_generation'], worker_identity=identity,
                reason=reason, pair_member=False, requested_fraction=requested_fraction,
                command_id=command_id, returncode=process.returncode, process_exit_observed=True,
                action_request_ref=(request or {}).get('raw_event_ref'),
                endpoint_ref=_churn_ref(events, token), shutdown_ref=_churn_ref(events, shutdown))


def run_window_churn(pool, schedule, events, *, window_start_ns,
                     window_duration_sec, startup_timeout_sec, emit,
                     pre_match_count, publish_burst_count,
                     publish_burst_interval_sec, config=None,
                     prior_created=(), prior_destroyed=()):
    """Execute real actions, with fresh per-command, per-process evidence.

    No historical-token fallback: list boundaries apply even to producers that
    do not assign event_index (namespace raw uses namespace_event_index).
    Same-role reuse is legal only after the previous process really exited.
    """
    pending = list(schedule)
    created_roles, destroyed_roles = set(prior_created), set(prior_destroyed)
    deadline_ns = window_start_ns + int(window_duration_sec * 1e9)
    while pending and time.monotonic_ns() < deadline_ns:
        item = pending[0]
        due_ns = window_start_ns + int(item['fraction'] * window_duration_sec * 1e9)
        if time.monotonic_ns() < due_ns:
            pool.pump(min(0.05, max(0.0, (min(due_ns, deadline_ns) - time.monotonic_ns()) / 1e9)))
            continue
        pending.pop(0)
        kind, role = item['kind'], item['role']
        if ((kind.startswith('reader_') and role not in {'reader%d' % i for i in range(2, 10)})
                or (kind.startswith('writer_') and role != 'writer2')
                or (kind == 'pair_publish' and role != 'writer')):
            raise RuntimeError('invalid_window_churn_role:' + str(role))
        if kind in ('reader_create', 'writer_create'):
            if role in created_roles and role not in destroyed_roles:
                emit('window_churn_action_rejected', action=kind, worker_role=role,
                     reason='churn_role_still_live', requested_fraction=item['fraction'])
                raise RuntimeError('churn_role_still_live:' + role)
            previous = [e for e in events if e['event'] == 'endpoint_created' and e.get('role') == role]
            generations = {e.get('endpoint_generation') for e in previous}
            requested_ns = time.monotonic_ns()
            request = emit('window_churn_action_requested', action=kind, worker_role=role,
                           requested_monotonic_ns=requested_ns, requested_fraction=item['fraction'],
                           window_start_ns=window_start_ns,
                           deadline_monotonic_ns=deadline_ns)
            boundary = len(events)
            _churn_timeout(deadline_ns, startup_timeout_sec)
            pool.start(role)
            process = _churn_process(pool, role)
            pid = process.pid
            starts = [e for e in _fresh_churn_events(events, boundary)
                      if e['event'] == 'process_started' and e.get('role') == role]
            if (len(starts) != 1 or starts[0].get('pid') != pid
                    or type(starts[0].get('worker_generation')) is not int):
                raise RuntimeError('churn_process_started_missing_or_duplicate:' + role)
            started = starts[0]

            def created():
                return [e for e in _fresh_churn_events(events, boundary) if e['event'] == 'endpoint_created'
                        and e.get('role') == role and e.get('pid') == pid
                        and isinstance(e.get('endpoint_generation'), str)
                        and e['endpoint_generation'] and e['endpoint_generation'] not in generations
                        and type(e.get('monotonic_ns')) is int and e['monotonic_ns'] >= requested_ns
                        and e.get('worker_generation') == started['worker_generation']
                        and _churn_ref(events, e)['local_event_index'] > _churn_ref(events, started)['local_event_index']]

            pool.wait(lambda: bool(created()), _churn_timeout(deadline_ns, startup_timeout_sec), role + '_created')
            tokens = created()
            if len(tokens) != 1:
                raise RuntimeError('churn_missing_or_duplicate_evidence:' + role + '_created')
            token = tokens[0]
            identity = _churn_identity(token)
            if not token.get('endpoint_gid'):
                raise RuntimeError('churn_endpoint_gid_missing:' + role)
            if config is not None:
                require_role_rmw(config, role, token)
            created_roles.add(role)
            destroyed_roles.discard(role)
            prefix = 'reader' if kind == 'reader_create' else 'writer'
            emit('discovery_' + prefix + '_recreated', role=role,
                 **{prefix + '_gid': token['endpoint_gid'], prefix + '_pid': identity['pid']},
                 generation=identity['endpoint_generation'], worker_identity=identity, pair_member=False,
                 recreate_index=int(role[len(prefix):]) - 1,
                 requested_fraction=item['fraction'],
                 action_request_ref=(request or {}).get('raw_event_ref'),
                 endpoint_ref=_churn_ref(events, token),
                 process_ref=_churn_ref(events, started))
        elif kind in ('reader_destroy', 'writer_destroy'):
            if role not in created_roles or role in destroyed_roles:
                raise RuntimeError(kind + '_before_create:' + role)
            _stop_churn_role(pool, events, role, emit=emit,
                startup_timeout_sec=startup_timeout_sec, deadline_ns=deadline_ns,
                reason='planned_in_window_churn', requested_fraction=item['fraction'])
            destroyed_roles.add(role)
        elif kind in ('writer_publish', 'pair_publish'):
            if kind == 'writer_publish' and (role not in created_roles or role in destroyed_roles):
                raise RuntimeError('writer_publish_before_create:' + role)
            token = _churn_current(pool, events, role)
            identity = _churn_identity(token)
            sequences = list(range(pre_match_count, pre_match_count + publish_burst_count))
            phase = 'window_burst' if kind == 'pair_publish' else 'writer2_churn'
            requested_ns = time.monotonic_ns()
            request = emit('window_churn_action_requested', action=kind, worker_role=role,
                           requested_monotonic_ns=requested_ns, deadline_monotonic_ns=deadline_ns,
                           worker_identity=identity, sequences=sequences, requested_fraction=item['fraction'])
            boundary = len(events)
            _churn_timeout(deadline_ns, startup_timeout_sec)
            command_id = pool.command(role, 'publish_burst', sequences=sequences,
                                      phase=phase, interval_sec=publish_burst_interval_sec)
            emit('publish_burst_submitted', command_id=command_id, role=role,
                 sequences=sequences, phase=phase, worker_identity=identity,
                 controller_waits_for_ack=True, pair_member=kind == 'pair_publish',
                 action_request_ref=(request or {}).get('raw_event_ref'))
            ack = _churn_wait(pool, events, boundary, role, 'command_done', identity,
                _churn_timeout(deadline_ns, startup_timeout_sec), role + '_burst',
                command_id=command_id, burst_count=len(sequences), after_monotonic_ns=requested_ns)
            ack_index = _churn_ref(events, ack)['local_event_index']
            sends = [e for e in _fresh_churn_events(events, boundary) if e['event'] == 'send'
                     and _churn_ref(events, e)['local_event_index'] < ack_index
                     and type(e.get('monotonic_ns')) is int and e['monotonic_ns'] >= requested_ns
                     and e.get('role') == role and e.get('phase') == phase
                     and all(e.get(key) == value for key, value in identity.items())]
            if [e.get('send_sequence') for e in sends] != sequences:
                raise RuntimeError('churn_burst_send_evidence_incomplete:' + role)
            emit('publish_burst_completed', role=role, command_id=command_id,
                 worker_identity=identity, phase=phase, sequences=sequences,
                 ack_ref=_churn_ref(events, ack),
                 send_refs=[_churn_ref(events, event) for event in sends],
                 action_request_ref=(request or {}).get('raw_event_ref'))
        else:
            raise RuntimeError('unknown_window_churn_action:' + str(kind))
        if time.monotonic_ns() >= deadline_ns:
            raise RuntimeError('window_churn_deadline_exceeded')
    if pending:
        raise RuntimeError('window_churn_schedule_incomplete')
    return {'created_roles': sorted(created_roles), 'destroyed_roles': sorted(destroyed_roles)}


def _run_window_stimuli(c, active, pool, events, emit, start_ns, duration_sec,
                        progress, *, prior_created=(), prior_destroyed=()):
    """Both drop and netem use the same scheduler, without extending a window.

    Continue carrier counters and trajectory sampling during due-time and
    command waits; OwnerCheckedPool.wait still owns health/channel checking.
    """
    schedule = window_churn_schedule(c, duration_sec) if active else []
    if not schedule:
        return {'created_roles': list(prior_created), 'destroyed_roles': list(prior_destroyed)}
    deadline_ns = start_ns + int(duration_sec * 1e9)

    class ProgressPool:
        def __getattr__(self, name):
            return getattr(pool, name)

        def pump(self, timeout=0.05):
            progress()
            pool.pump(timeout)

        def wait(self, predicate, timeout, label):
            def observed():
                if time.monotonic_ns() < deadline_ns:
                    progress()
                return predicate()
            return pool.wait(observed, timeout, label)

    return run_window_churn(ProgressPool(), schedule, events, window_start_ns=start_ns,
        window_duration_sec=duration_sec, startup_timeout_sec=c['startup_timeout_sec'], emit=emit,
        pre_match_count=c['pre_match_count'], publish_burst_count=c.get('publish_burst_count', 32),
        publish_burst_interval_sec=c.get('publish_burst_interval_sec', 0.0), config=c,
        prior_created=prior_created, prior_destroyed=prior_destroyed)


def _cleanup_window_roles(pool, events, roles, destroyed, c, emit):
    for role in sorted(set(roles) - set(destroyed)):
        _stop_churn_role(pool, events, role, emit=emit,
                         startup_timeout_sec=c['startup_timeout_sec'])
        destroyed.add(role)


def _f2_event(events, reference):
    found = [e for e in events if e.get('raw_event_ref') == reference and reference is not None]
    if len(found) != 1:
        raise WindowGateError('f2_anchor_missing_or_ambiguous')
    return found[0]


def _f2_anchor_deadline(c, plan, index, events, endpoints, precondition, previous_ref):
    """Discharge a real runtime guard, before any new window is installed."""
    anchor = _f2_event(events, precondition['raw_event_ref'] if index == 0 else previous_ref)
    expected_kind = 'discovery_precondition_snapshot' if index == 0 else 'discovery_recovery_confirmed'
    expected_label = 'fresh_match' if index == 0 else 'previous_restore_confirmed'
    if (anchor.get('event') != expected_kind or plan['trigger_anchor'] != expected_label
            or anchor.get('role') != 'controller'
            or any(anchor.get(k) != c[k] for k in ('run_id', 'domain_id'))
            or type(anchor.get('monotonic_ns')) is not int
            or anchor['monotonic_ns'] > time.monotonic_ns()):
        raise WindowGateError('f2_anchor_identity_or_future_boundary')
    if index == 0:
        if anchor.get('satisfied') is not True:
            raise WindowGateError('f2_anchor_precondition_not_satisfied')
        bindings, matched = anchor['bindings'], anchor['matched']
        proof_after_ns = endpoints['reader']['created_monotonic_ns']
    else:
        if (anchor.get('window_index') != index - 1
                or anchor.get('window_id') != c['runtime_windows'][index - 1]['window_id']
                or anchor.get('receipt', {}).get('recovery_verified') is not True):
            raise WindowGateError('f2_anchor_previous_recovery_not_confirmed')
        boundaries = [e for e in events if e.get('event') == 'discovery_restore_boundary'
                      and e.get('window_index') == index - 1]
        if (len(boundaries) != 1 or boundaries[0].get('restored') is not True
                or not boundaries[0]['monotonic_ns'] <= anchor['monotonic_ns']):
            raise WindowGateError('f2_anchor_previous_restore_boundary_missing')
        proof_after_ns = boundaries[0]['post_restore_monotonic_ns']
        bindings = anchor['receipt']['pair_bindings']
        matched = anchor['receipt']['activity_receipt']['fresh_graph_count']
    initial = precondition['bindings']
    admissions = [e for e in events if e.get('event') == 'namespace_host_admission_verified']
    if (len(admissions) != 1 or any(initial.get('isolation', {}).get(k) != admissions[0].get('isolation', {}).get(k)
            for k in ('net_namespace', 'user_namespace'))):
        raise WindowGateError('f2_anchor_namespace_identity_mismatch')
    if (bindings.get('worker_pids') != {r: t['pid'] for r, t in endpoints.items()}
            or bindings.get('worker_start_ticks') != initial.get('worker_start_ticks')
            or bindings.get('only_pair_bound_udp_sockets') is not True
            or any(bindings.get('isolation', {}).get(k) != initial.get('isolation', {}).get(k)
                   for k in ('net_namespace', 'user_namespace'))):
        raise WindowGateError('f2_anchor_namespace_or_process_identity_mismatch')
    refs = []
    position = {e.get('raw_event_ref'): n for n, e in enumerate(events)}
    for role, token in endpoints.items():
        match = _f2_event(events, matched['matches'][role])
        graph = _f2_event(events, matched['graphs'][role])
        acks = [e for e in events if e.get('event') == 'command_done'
                and e.get('role') == role and e.get('command_id') == match.get('command_id')]
        requests = [e for e in events if e.get('event') == 'match_snapshot_requested'
                    and e.get('role') == role and e.get('command_id') == match.get('command_id')]
        if len(acks) != 1 or len(requests) != 1:
            raise WindowGateError('f2_anchor_fresh_match_command_evidence_missing')
        request, ack = requests[0], acks[0]
        proof = (request, match, graph, ack)
        if (match.get('event') != 'match_snapshot' or graph.get('event') != 'graph_snapshot'
                or type(match.get('matched_count')) is not int or match['matched_count'] != 1
                or graph.get('command_id') != match.get('command_id')
                or match.get('endpoint_gid') != token.get('endpoint_gid')
                or match.get('count_api') != ('get_subscription_count' if role == 'writer' else 'get_publisher_count')
                or not max(token['created_monotonic_ns'], proof_after_ns) <= request.get('requested_monotonic_ns', -1)
                    <= match.get('sampled_monotonic_ns', -1) <= match['monotonic_ns']
                or any(any(e.get(k) != token.get(k) for k in ('pid', 'endpoint_generation', 'rmw_identifier'))
                       for e in (match, graph, ack))
                or not position[request['raw_event_ref']] < position[match['raw_event_ref']]
                    < position[graph['raw_event_ref']] < position[ack['raw_event_ref']]
                    < position[anchor['raw_event_ref']]
                or not request['monotonic_ns'] <= match['monotonic_ns'] <= graph['monotonic_ns']
                    <= ack['monotonic_ns'] <= anchor['monotonic_ns']):
            raise WindowGateError('f2_anchor_match_identity_or_chronology_invalid')
        refs.extend(e['raw_event_ref'] for e in proof)
    anchor_ns = anchor['monotonic_ns']
    offset_due = anchor_ns + round(plan['trigger_offset_sec'] * 1e9)
    interval_due = (anchor_ns + round(c['runtime_windows'][index - 1]['restore_interval_sec'] * 1e9)
                    if index else anchor_ns)
    return {'anchor_ref': anchor['raw_event_ref'], 'anchor_monotonic_ns': anchor_ns,
            'matched_event_refs': refs, 'offset_due_monotonic_ns': offset_due,
            'prior_interval_due_monotonic_ns': interval_due,
            'earliest_due_monotonic_ns': max(offset_due, interval_due)}


def _emit_f2_operation_receipts(c, events, emit):
    """Executed operations cite their own windows, never an E01 aggregate."""
    from .ros2_pair import f2_windows_operations
    operations = f2_windows_operations(c)
    for op in operations:
        windows, refs = [], []
        for index, plan in enumerate(c['runtime_windows']):
            selected = {}
            for name in ('discovery_window_timing', 'discovery_drop_begin', 'discovery_drop_end',
                         'discovery_recovery_confirmed', 'discovery_window_restore_verified'):
                found = [e for e in events if e.get('event') == name and e.get('window_index') == index]
                if len(found) != 1:
                    raise WindowGateError('f2_operation_window_evidence_missing:' + name)
                selected[name] = found[0]
            dropped, restored = selected['discovery_drop_begin'], selected['discovery_drop_end']
            hi = events.index(restored)
            counters = [e for e in events[hi + 1:] if e.get('event') == 'discovery_window_counter']
            if not counters or any(e.get('event') == 'discovery_drop_begin' for e in events[hi + 1:events.index(counters[0])]):
                raise WindowGateError('f2_operation_counter_missing')
            counter = counters[0]
            from .discovery_network import window_counter_delta
            delta = window_counter_delta(dropped['receipt'], restored['receipt'])
            if any(counter.get(k) != delta[k] for k in ('packets_delta', 'bytes_delta', 'hit', 'table')):
                raise WindowGateError('f2_operation_counter_mismatch')
            timing, recovery, verified = (selected[k] for k in ('discovery_window_timing',
                'discovery_recovery_confirmed', 'discovery_window_restore_verified'))
            good = (timing.get('status') == 'observed' and restored['receipt'].get('restored') is True
                    and recovery['receipt'].get('recovery_verified') is True
                    and verified.get('recovery_confirmed') is True)
            item = {'window_id': plan['window_id'], 'window_index': index,
                    'complete': good, 'hit': good and delta['hit'],
                    'packets_delta': delta['packets_delta'],
                    'raw_refs': {name: event['raw_event_ref'] for name, event in selected.items()},
                    'counter_ref': counter['raw_event_ref']}
            windows.append(item)
            refs.extend(list(item['raw_refs'].values()) + [item['counter_ref']])
        closing = [e for e in events if e.get('event') == 'window_close' and e.get('window') == 'post_restore_delivery']
        final = [e for e in events if e.get('event') == 'state_step_after' and e.get('step_id') == 'observe_final_graph'
                 and e.get('satisfied') is True]
        complete = all(w['complete'] for w in windows) and len(closing) == len(final) == 1
        refs.extend(e['raw_event_ref'] for e in closing + final)
        affected = op['affected_window_indices']
        if op['surface_id'] == 'E03':
            observed_parameters = {'anchor': op['parameters']['anchor'],
                'requested_offset_sec': op['parameters']['offset_sec'],
                'actual_offsets': [{'window_index': i, 'anchor_ref': _f2_event(events, windows[i]['raw_refs']['discovery_window_timing'])['anchor_ref'],
                    'actual_offset_sec': (_f2_event(events, windows[i]['raw_refs']['discovery_window_timing'])['actual_install_monotonic_ns']
                                          - _f2_event(events, windows[i]['raw_refs']['discovery_window_timing'])['anchor_monotonic_ns']) / 1e9}
                                   for i in affected]}
        else:
            observed_parameters = {'actual_window_count': len(windows),
                'actual_restore_intervals_sec': [(_f2_event(events, w['raw_refs']['discovery_window_timing'])['actual_install_monotonic_ns']
                                                 - _f2_event(events, w['raw_refs']['discovery_window_timing'])['anchor_monotonic_ns']) / 1e9
                                                for w in windows[1:]],
                'trailing_interval_status': 'not_applicable_no_next_window'}
        payload = {'schema': 'r2f.f2-window-operation/v1', 'op_id': op['op_id'],
                   'surface_id': op['surface_id'], 'mutator_id': op['mutator_id'],
                   'affected_window_indices': affected, 'observed_parameters': observed_parameters,
                   'plan_hash': c['mutation_plan_sha256'], 'status': 'executed' if complete else 'incomplete',
                   'applied': complete, 'hit': complete and all(w['hit'] for w in windows),
                   'effect_observed': complete and all(w['hit'] for w in windows),
                   'window_count': len(windows), 'windows': windows, 'evidence_refs': refs}
        emit('f2_operation_receipt', receipt=payload)


def _run_window_chain(c, active, receipt, emit, step, activity, activity_is_pair_observed,
                      snapshot_pair, pause, carrier, pool, events, endpoints, matched,
                      precondition, match_receipt, trajectories, window_records,
                      window_hits, window_drop_activity, window_recovery_activity,
                      window_fresh_recovery, window_restored):
    """Execute a validated 1..3 window drop->restore chain.

    Every window has its own drop begin/end, counter delta, restore boundary and
    post-restore recovery confirmation. A window may only start after the
    previous window's restore and fresh activity have been verified. Exceptions
    always restore the current window before propagating; the caller's outer
    handler keeps the original error and still cleans up the carrier.
    """
    plans = c['runtime_windows']
    f2_timing = c.get('target_id') == F2_WINDOWS_TARGET
    multi = True
    ledger = WindowLedger(plans)
    previous_recovery_ref = precondition['raw_event_ref']
    counter_hit = False
    chain_created, chain_destroyed = set(), set()
    try:
        if active:
            receipt.update(submitted=True, carrier_admitted=True)
            emit('mutation_begin', mutation_kind='discovery_drop', carrier='wire_proxy',
                 implementation=IMPLEMENTATION,
                 precondition_snapshot_ref=precondition['raw_event_ref'],
                 mutation_receipt=dict(receipt))
        for index, plan in enumerate(plans):
            anchor_ref = precondition['raw_event_ref'] if index == 0 else previous_recovery_ref
            if index and active and not (window_recovery_activity[-1] and window_fresh_recovery[-1]):
                raise WindowGateError('previous_window_recovery_not_confirmed')
            timing = (_f2_anchor_deadline(c, plan, index, events, endpoints, precondition, previous_recovery_ref)
                      if f2_timing else None)
            if timing is not None:
                while time.monotonic_ns() < timing['earliest_due_monotonic_ns']:
                    pool.pump(min(0.05, (timing['earliest_due_monotonic_ns'] - time.monotonic_ns()) / 1e9))
            elif plan['trigger_offset_sec']:
                offset_deadline = time.monotonic() + plan['trigger_offset_sec']
                while time.monotonic() < offset_deadline:
                    pool.pump(min(0.05, max(0.0, offset_deadline - time.monotonic())))
            window_fields = {'window_index': index, 'window_id': plan['window_id'],
                             'trigger_anchor': plan['trigger_anchor'], 'anchor_ref': anchor_ref}
            emit('discovery_window_begin', drop_installed=active, **window_fields)
            begin = None
            window_counter_hit = False
            window_activity_ok = False
            trajectory = None
            drop_wall_ns = time.time_ns()
            if active:
                window_precondition = dict(match_receipt)
                if index:
                    # Later windows must cite the preceding verified restore,
                    # never reuse the pre-first-window match snapshot.
                    window_precondition['evidence_ref'] = previous_recovery_ref
                    window_precondition['previous_restore_confirmed'] = True
                actual_start_ns = time.monotonic_ns()
                begin = carrier.start(match_receipt=window_precondition)
                receipt.update(configured=begin['configured'], applied=begin['applied'])
                ledger.start(index, begin)
                emit('discovery_drop_begin', receipt=begin, mutation_receipt=dict(receipt), **window_fields)
                drop_wall_ns = time.time_ns()
                if timing is not None:
                    installed = begin.get('drop_applied_monotonic_ns')
                    due = timing['earliest_due_monotonic_ns']
                    valid = (type(installed) is int and due <= actual_start_ns <= installed <= time.monotonic_ns()
                             and installed - due <= round(c['anchor_lateness_tolerance_sec'] * 1e9))
                    timing.update(actual_start_monotonic_ns=actual_start_ns, actual_install_monotonic_ns=installed,
                                  overshoot_ns=(installed - due if type(installed) is int else None),
                                  status='observed' if valid else 'incomplete',
                                  reason=None if valid else 'harness_anchor_timing_miss')
                    emit('discovery_window_timing', **timing, **{k: v for k, v in window_fields.items() if k != 'anchor_ref'})
            emit('window_open', window='discovery_interruption', active_drop=active, **window_fields)
            # The original input window owns one deadline, including all
            # startup/stop/command waits. Churn must never buy another window.
            trajectory_start_ns = time.monotonic_ns()
            deadline_ns = trajectory_start_ns + int(plan['drop_duration_sec'] * 1e9)
            trajectory = _RecoveryMatchTrajectory(events, endpoints, emit, trajectory_start_ns, deadline_ns, matched)
            trajectories.append(trajectory)
            created_roles, destroyed_roles = set(chain_created), set(chain_destroyed)
            restored_this = False

            def progress():
                nonlocal window_counter_hit
                if active:
                    counter = carrier.counters()
                    window_counter_hit = window_counter_hit or counter['hit']
                    emit('discovery_drop_counter', receipt=counter, **window_fields)
                trajectory.poll(pool)

            try:
                if timing is not None and timing['status'] != 'observed':
                    raise WindowGateError('harness_anchor_timing_miss')
                # The optional probe is never the judged delivery pair. Its
                # process lifecycle still needs this window's fresh evidence.
                if active and c.get('discovery_probe', False):
                    previous = {e.get('endpoint_generation') for e in events
                                if e['event'] == 'endpoint_created' and e.get('role') == 'probe'}
                    request = emit('window_churn_action_requested', action='probe_create',
                                   worker_role='probe', **window_fields)
                    boundary_index = len(events)
                    pool.start('probe')
                    probe_pid = pool.workers['probe'].pid
                    def probe_created():
                        return [e for e in _fresh_churn_events(events, boundary_index) if e['event'] == 'endpoint_created'
                                and e.get('role') == 'probe' and e.get('pid') == probe_pid
                                and e.get('endpoint_generation') not in previous
                                and isinstance(e.get('endpoint_generation'), str)]
                    pool.wait(lambda: bool(probe_created()),
                              _churn_timeout(deadline_ns, c['startup_timeout_sec']), 'probe_created')
                    if len(probe_created()) != 1:
                        raise RuntimeError('churn_missing_or_duplicate_evidence:probe_created')
                    probe_token = probe_created()[0]
                    emit('discovery_probe_created', probe_gid=probe_token.get('endpoint_gid'),
                         probe_pid=probe_pid, worker_identity=_churn_identity(probe_token),
                         probe_topic=c.get('probe_topic'), pair_member=False,
                         action_request_ref=(request or {}).get('raw_event_ref'),
                         endpoint_ref=_churn_ref(events, probe_token))
                churn_result = _run_window_stimuli(c, active, pool, events, emit,
                    trajectory_start_ns, plan['drop_duration_sec'], progress,
                    prior_created=chain_created, prior_destroyed=chain_destroyed)
                created_roles = set(churn_result['created_roles'])
                destroyed_roles = set(churn_result['destroyed_roles'])
                while time.monotonic_ns() < deadline_ns:
                    progress()
                    remaining = max(0, (deadline_ns - time.monotonic_ns()) / 1e9)
                    pause(min(c['counter_interval_sec'], remaining,
                              max(0, (trajectory.next_due_ns - time.monotonic_ns()) / 1e9)))
                during = activity(drop_wall_ns, 'discovery_interruption')
                window_activity_ok = activity_is_pair_observed(during['activity'])
            finally:
                trajectory.restore_started_ns = time.monotonic_ns()
                if active:
                    restore = carrier.restore()
                    restored_this = restore['restored'] is True
                    emit('discovery_drop_end', receipt=restore, **window_fields)
                    delta = ledger.end(index, restore)
                    emit('discovery_window_counter', **{k: v for k, v in delta.items() if k != 'event'})
                    receipt['applied'] = bool(receipt['applied'] and restored_this)
                    window_counter_hit = delta['hit']
            if active and c.get('discovery_probe', False):
                _stop_churn_role(pool, events, 'probe', emit=emit,
                                 startup_timeout_sec=c['startup_timeout_sec'])
            if active:
                _cleanup_window_roles(pool, events, created_roles, destroyed_roles, c, emit)
            # Track the final state, including post-window cleanup. A newly
            # created generation already cleared its predecessor's destroyed
            # marker; the next window cannot mistake it for a historical role.
            chain_created, chain_destroyed = created_roles, destroyed_roles
            window_restored.append(restored_this if active else None)
            window_hits.append(window_counter_hit)
            window_drop_activity.append(window_activity_ok)
            emit('window_close', window='discovery_interruption', active_drop=active,
                 mutation_receipt=dict(receipt), **window_fields)
            restore_wall_ns, restore_mono_ns = time.time_ns(), time.monotonic_ns()
            boundary = emit('discovery_restore_boundary', restored=restored_this if active else None,
                            neutral=not active, drop_was_installed=active, drop_installed=False,
                            post_restore_unix_ns=restore_wall_ns, post_restore_monotonic_ns=restore_mono_ns,
                            clock_guard_ns=CLOCK_GUARD_NS,
                            activity_since_unix_ns=restore_wall_ns + CLOCK_GUARD_NS, **window_fields)
            trajectory.restore_boundary_ns = restore_mono_ns
            result_clock = {'unix_ns': restore_wall_ns, 'monotonic_ns': restore_mono_ns,
                            'source': 'host_kernel_realtime_and_shared_monotonic',
                            'clock_guard_ns': CLOCK_GUARD_NS,
                            'limitation': 'packet realtime is checked for clock offset discontinuity'}
            recovery_deadline = time.monotonic() + c['recovery_timeout_sec']
            activity_event = None
            window_recovery_ok = False
            while time.monotonic() < recovery_deadline:
                activity_event = activity(restore_wall_ns + CLOCK_GUARD_NS, 'post_restore_%d' % index,
                                          after_monotonic_ns=restore_mono_ns)
                window_recovery_ok = activity_event['activity_observed']
                clock_offset = time.time_ns() - time.monotonic_ns()
                if abs(clock_offset - (restore_wall_ns - restore_mono_ns)) > CLOCK_GUARD_NS:
                    window_recovery_ok = False
                    emit('discovery_observation_unknown',
                         reason='realtime_monotonic_offset_discontinuity', **window_fields)
                    break
                if window_recovery_ok:
                    break
                pause(0.2)
            window_recovery_activity.append(window_recovery_ok)
            window_fresh = False
            post_evidence = None
            try:
                post_evidence = step('observe_recovery_graph_%d' % index, ['verify_namespace_bindings'],
                                     lambda restore_mono_ns=restore_mono_ns: snapshot_pair(restore_mono_ns),
                                     observation=True)
                trajectory.after = post_evidence
                window_fresh = True
            except (TimeoutError, RuntimeError) as exc:
                emit('discovery_observation_unknown', reason=str(exc), **window_fields)
            window_fresh_recovery.append(window_fresh)
            previous_recovery_ref = boundary['raw_event_ref']
            if active and window_recovery_ok and window_fresh:
                recovery = carrier.confirm_recovery(activity_receipt={
                    **match_receipt, 'monotonic_ns': time.monotonic_ns(),
                    'activity_observed': True, 'source': 'public_ros2_pair',
                    'evidence_ref': activity_event['raw_event_ref'], 'fresh_graph_count': post_evidence})
                recovery_event = emit('discovery_recovery_confirmed', receipt=recovery,
                                      requires_runtime_semantic_adjudication=True, **window_fields)
                previous_recovery_ref = recovery_event['raw_event_ref']
                ledger.confirm_recovery(index, recovery)
            window_records.append({
                'window_index': index, 'window_id': plan['window_id'],
                'trigger_anchor': plan['trigger_anchor'],
                'trigger_offset_sec': plan['trigger_offset_sec'],
                'drop_duration_sec': plan['drop_duration_sec'],
                'restore_interval_sec': plan['restore_interval_sec'],
                'restore_boundary_monotonic_ns': restore_mono_ns,
                'counter_hit': bool(window_counter_hit),
                'drop_activity_observed': bool(window_activity_ok),
                'recovery_activity_observed': bool(window_recovery_ok),
                'fresh_count_graph': bool(window_fresh),
                'restored': restored_this if active else None,
                'restore_boundary_ref': boundary['raw_event_ref'],
                'calendar_clock': result_clock,
                'counter': ledger.records[index]['counter'] if active else None})
            emit('discovery_window_restore_verified', window_index=index, window_id=plan['window_id'],
                 restored=restored_this if active else None,
                 recovery_confirmed=bool(active and window_recovery_ok and window_fresh),
                 counter_hit=bool(window_counter_hit),
                 restore_interval_sec=plan['restore_interval_sec'])
            if plan['restore_interval_sec'] and not f2_timing:
                pause(plan['restore_interval_sec'])
            if index < len(plans) - 1 and active and not (window_recovery_ok and window_fresh):
                raise WindowGateError('next_window_requires_previous_restore_evidence')
    except BaseException as exc:
        ledger.abort(type(exc).__name__ + ':' + str(exc))
        raise
    finally:
        emit('discovery_window_ledger', **{k: v for k, v in ledger.summary().items() if k != 'event'})
    return ledger


def _netem_scope(spec):
    """Scope of an installed netem window; absent stays the legacy discovery one."""
    return spec.get(NETEM_SCOPE_FIELD, 'discovery')


def _data_group_counter(counter):
    """Real data-group counter from a carrier receipt, or None if unavailable.

    No number is synthesized: a missing group or unavailable grouping yields
    None so the caller keeps hit=False instead of inventing packets.
    """
    if not isinstance(counter, dict) or counter.get('group_counters_available') is not True:
        return None
    groups = counter.get('group_counters')
    if not isinstance(groups, dict):
        return None
    data = groups.get('data')
    if not isinstance(data, dict):
        return None
    packets, byte_count = data.get('packets'), data.get('bytes')
    if type(packets) is not int or type(byte_count) is not int or packets < 0 or byte_count < 0:
        return None
    return {'packets': packets, 'bytes': byte_count}


def _netem_counter_hit(counter, scope, key='mutation_hit'):
    """Real counted packets only; ``scope='data'`` requires the data group itself.

    ``key`` lets legacy/mocked receipts (``{'mutation_hit': True}``) still be
    read. A data-only window that cannot prove a data-group count never becomes
    a hit; a ``both`` window counts the union, and the per-group numbers stay
    available so no discovery packet is misreported as a data-plane match.
    """
    if not isinstance(counter, dict):
        return False
    if scope in ('data', 'both') and counter.get('group_counters_available') is False:
        # Grouping failed: the flat class counter can no longer be attributed to
        # the data plane, so a data-bearing scope must not claim a hit.
        return False
    if scope == 'data':
        data = _data_group_counter(counter)
        return bool(data is not None and data['packets'] > 0)
    return bool(counter.get(key))


def _netem_hit_groups(counter):
    """Named port groups whose real counters are positive in this snapshot.

    Unavailable grouping yields ``[]``: without a trustworthy per-group count
    nothing may be listed as a counted group.
    """
    if not isinstance(counter, dict):
        return []
    if counter.get('group_counters_available') is False:
        return []
    groups = counter.get('group_counters')
    if not isinstance(groups, dict):
        return []
    return sorted(group_id for group_id, value in groups.items()
                  if isinstance(value, dict) and type(value.get('packets')) is int
                  and value['packets'] > 0)


def _run_netem_window(c, active, receipt, emit, step, activity, activity_is_pair_observed,
                       snapshot_pair, pause, carrier, pool, events, endpoints, matched,
                       precondition, match_receipt):
    """Bounded delay/reorder/duplicate window over the spec's UDP port scope.

    ``spec['scope']`` is passed through untouched to the carrier. With scope
    ``discovery`` the emitted event set and receipt fields are exactly the
    legacy ones. When the scope includes ``data``, the same window additionally
    emits ``data_mutation_begin`` / ``data_mutation_counter`` /
    ``data_mutation_end`` carrying the spec, labelled port groups, tc dump hashes
    and the restore dump-after hash; those events are additive and never replace
    the discovery events.

    Under the mutated arm the transparent-class spec is still installed (audited
    ``neutral=True``) but never reports ``applied`` or a hit, matching the D002
    no-op branch. Non-mutated arms install nothing. Every installed window is
    removed and then must show fresh independent pair activity plus a fresh
    graph/count before it can be treated as recovered.

    ``netem_covers_delivery`` (installed arm only) adds a second explicitly
    bounded segment: after the stimulus window has been restored and its
    recovery evidence observed, the same spec is installed again, the real
    post-restore delivery runs inside it, and only then is it restored.  That
    keeps the sealed stimulus chronology (install < open < boundary < graph)
    intact while making the class actually act on the judged delivery traffic;
    ``netem_delivery_overlap`` records the delivery-segment refs.

    ``receipt['hit']`` stays one of the seven nested boolean fields and is set
    only from real counted packets (>0) + a mutated arm + a non-neutral spec.
    ``hit_semantics`` on the data events states that the hit is a data-plane
    matched packet count, never a SUT defect; the oracle alone decides
    candidates and nothing here interprets a wire reorder/duplicate as an
    expected reader-side effect.
    """
    spec = c['environment_mutation']['spec']
    scope = _netem_scope(spec)
    data_scope = scope in ('data', 'both')
    installed = bool(active)
    mutation_active = bool(active and not spec['neutral'])
    # ``netem_covers_delivery`` puts the real post-restore delivery inside the
    # installed class.  It is only meaningful when something is installed: a
    # non-mutated arm installs nothing, so there is no window to overlap with
    # and the flag must never report an overlap it did not have.
    covers_delivery = bool(c.get('netem_covers_delivery')) and installed
    window_sec = c['drop_duration_sec']
    if spec['restore_sec']:
        window_sec = min(window_sec, spec['restore_sec'])
    counters_hit = False
    data_counters_hit = False
    last_counter = None
    restored = False
    begin = None
    delivery = None
    install_ref = restore_ref = boundary_ref = None
    install_index = restore_index = boundary_index = None
    drop_wall_ns = time.time_ns()

    delivery_open = [False]

    def sample(counter):
        """One counter snapshot: discovery receipt plus additive data receipts.

        Counters taken after the delivery window opened belong to the delivery
        segment and report under ``data_netem_delivery``; earlier ones keep the
        sealed ``data_netem`` names byte for byte.  ``discovery_netem_counter``
        is a stimulus-window receipt only, so it is not emitted from the
        delivery segment.
        """
        nonlocal counters_hit, data_counters_hit
        window = 'data_netem_delivery' if delivery_open[0] else 'data_netem'
        counters_hit = counters_hit or _netem_counter_hit(counter, scope)
        if not delivery_open[0]:
            emit('discovery_netem_counter', receipt=counter)
        if data_scope:
            data_counters_hit = data_counters_hit or bool(_netem_counter_hit(counter, 'data'))
            emit('data_mutation_counter', window=window, scope=scope,
                 port_groups=counter.get('port_groups'),
                 port_group_ids=[group['group_id'] for group in counter.get('port_groups') or []],
                 group_counters=counter.get('group_counters'),
                 group_counters_available=counter.get('group_counters_available'),
                 group_counters_reason=counter.get('group_counters_reason'),
                 hit_groups=_netem_hit_groups(counter),
                 expected_ports=counter.get('expected_ports'),
                 affected_packets=counter.get('affected_packets'),
                 affected_bytes=counter.get('affected_bytes'),
                 loss_pct=NETEM_LOSS_PCT, loss_defaulted=True,
                 hit_semantics=netem_hit_semantics(scope),
                 responsibility_layer=NETEM_RESPONSIBILITY_LAYER,
                 counter=counter)
    if installed:
        receipt.update(submitted=True, carrier_admitted=True)
        emit('mutation_begin', mutation_kind='discovery_netem', carrier='wire_proxy',
             implementation=NETEM_IMPLEMENTATION, neutral_spec=spec['neutral'],
             scope=scope,
             precondition_snapshot_ref=precondition['raw_event_ref'],
             mutation_receipt=dict(receipt))
        begin = carrier.start(spec)
        last_counter = begin.get('counter_snapshot')
        counters_hit = counters_hit or _netem_counter_hit(last_counter, scope)
        data_counters_hit = data_counters_hit or bool(
            _netem_counter_hit(last_counter, 'data'))
        receipt.update(configured=begin['configured'], applied=begin['applied'])
        install_event = emit('discovery_netem_begin', receipt=begin, mutation_receipt=dict(receipt),
                             neutral=begin['neutral'], scope=scope)
        install_ref, install_index = (install_event['raw_event_ref'],
                                      install_event.get('namespace_event_index'))
        if data_scope:
            emit('data_mutation_begin', window='data_netem', scope=scope,
                 spec=dict(spec),
                 port_groups=begin.get('groups'),
                 port_group_ids=[group['group_id'] for group in begin.get('groups') or []],
                 tc_dump_sha256=begin.get('tc_dump_sha256'),
                 tc_dump=begin.get('tc_dump'),
                 loss_pct=NETEM_LOSS_PCT, loss_defaulted=True,
                 responsibility_layer=NETEM_RESPONSIBILITY_LAYER,
                 hit_semantics=netem_hit_semantics(scope),
                 counted_packet_scope=('user_data_destination_udp_ports' if scope == 'data'
                                       else 'discovery_and_user_data_destination_udp_ports'),
                 counter_snapshot=last_counter,
                 mutation_receipt=dict(receipt))
        drop_wall_ns = time.time_ns()
    else:
        emit('control_begin', control_kind=c['arm'], carrier='discovery_netem',
             netem_installed=False, neutral_spec=spec['neutral'], scope=scope,
             precondition_snapshot_ref=precondition['raw_event_ref'], mutation_receipt=dict(receipt))
    emit('window_open', window='discovery_netem', active_mutation=mutation_active,
         netem_installed=installed, neutral_spec=spec['neutral'], window_sec=window_sec,
         scope=scope)
    trajectory_start_ns = time.monotonic_ns()
    deadline_ns = trajectory_start_ns + int(window_sec * 10**9)
    trajectory = _RecoveryMatchTrajectory(events, endpoints, emit, trajectory_start_ns, deadline_ns, matched)
    activity_ok = False
    created_roles, destroyed_roles = set(), set()

    def progress():
        nonlocal last_counter
        if installed:
            last_counter = carrier.counters()
            sample(last_counter)
        trajectory.poll(pool)

    try:
        churn_result = _run_window_stimuli(c, installed, pool, events, emit,
            trajectory_start_ns, window_sec, progress)
        created_roles = set(churn_result['created_roles'])
        destroyed_roles = set(churn_result['destroyed_roles'])
        while time.monotonic_ns() < deadline_ns:
            progress()
            pause(min(c['counter_interval_sec'], max(0, (deadline_ns - time.monotonic_ns()) / 1e9),
                      max(0, (trajectory.next_due_ns - time.monotonic_ns()) / 1e9)))
        during = activity(drop_wall_ns, 'discovery_netem_window')
        activity_ok = activity_is_pair_observed(during['activity'])
    finally:
        trajectory.restore_started_ns = time.monotonic_ns()
        if installed:
            end = carrier.restore()
            restored = end['restored'] is True
            if mutation_active:
                # A neutral transparent class must not flip applied on restore.
                receipt['applied'] = bool(receipt['applied'] and restored)
            restore_event = emit('discovery_netem_end', receipt=end)
            restore_ref = restore_event['raw_event_ref']
            restore_index = restore_event.get('namespace_event_index')
            if data_scope:
                end_counter = end.get('counter_snapshot') or {}
                emit('data_mutation_end', window='data_netem', scope=scope,
                     restored=restored,
                     tc_dump_after_sha256=end.get('tc_dump_after_sha256'),
                     tc_dump_sha256=(begin or {}).get('tc_dump_sha256'),
                     residual_qdisc=end.get('residual_qdisc'),
                     residual_filter=end.get('residual_filter'),
                     only_noqueue_root=end.get('only_noqueue_root'),
                     counter_snapshot=end.get('counter_snapshot'),
                     counter_observed=end.get('counter_observed'),
                     group_counters=end_counter.get('group_counters'),
                     group_counters_available=end_counter.get('group_counters_available'),
                     hit_groups=_netem_hit_groups(end_counter),
                     port_groups=(begin or {}).get('groups'),
                     port_group_ids=[group['group_id'] for group in (begin or {}).get('groups') or []],
                     loss_pct=NETEM_LOSS_PCT, loss_defaulted=True,
                     responsibility_layer=NETEM_RESPONSIBILITY_LAYER,
                     hit_semantics=netem_hit_semantics(scope),
                     restore_receipt=end)
    if installed:
        _cleanup_window_roles(pool, events, created_roles, destroyed_roles, c, emit)
    restore_wall_ns, restore_mono_ns = time.time_ns(), time.monotonic_ns()
    boundary = emit('discovery_restore_boundary', restored=restored if installed else None,
                    neutral=not installed, netem_was_installed=installed,
                    netem_installed=False, netem_neutral_spec=spec['neutral'],
                    carrier='discovery_netem',
                    post_restore_unix_ns=restore_wall_ns, post_restore_monotonic_ns=restore_mono_ns,
                    clock_guard_ns=CLOCK_GUARD_NS, activity_since_unix_ns=restore_wall_ns + CLOCK_GUARD_NS)
    boundary_ref, boundary_index = boundary['raw_event_ref'], boundary.get('namespace_event_index')
    stimulus_manifest = {'install_ref': install_ref, 'restore_ref': restore_ref,
                         'boundary_ref': boundary_ref,
                         'install_index': install_index, 'restore_index': restore_index,
                         'boundary_index': boundary_index}
    stimulus_counter = last_counter
    trajectory.restore_boundary_ns = restore_mono_ns
    recovery_deadline = time.monotonic() + c['recovery_timeout_sec']
    activity_event, fresh_recovery, post_evidence = None, False, None
    while time.monotonic() < recovery_deadline:
        activity_event = activity(restore_wall_ns + CLOCK_GUARD_NS, 'post_restore',
                                  after_monotonic_ns=restore_mono_ns)
        clock_offset = time.time_ns() - time.monotonic_ns()
        if abs(clock_offset - (restore_wall_ns - restore_mono_ns)) > CLOCK_GUARD_NS:
            emit('discovery_observation_unknown', reason='realtime_monotonic_offset_discontinuity')
            break
        if activity_event['activity_observed']:
            break
        pause(0.2)
    try:
        post_evidence = step('observe_recovery_graph', ['verify_namespace_bindings'],
                             lambda: snapshot_pair(restore_mono_ns), observation=True)
        trajectory.after = post_evidence
        fresh_recovery = True
    except (TimeoutError, RuntimeError) as exc:
        emit('discovery_observation_unknown', reason=str(exc))
    activity_ok = bool(activity_event and activity_event['activity_observed'])
    if installed and activity_ok and fresh_recovery:
        recovery = carrier.confirm_recovery(activity_receipt={**match_receipt,
            'monotonic_ns': time.monotonic_ns(), 'activity_observed': True,
            'source': 'public_ros2_pair', 'evidence_ref': activity_event['raw_event_ref'],
            'fresh_graph_count': post_evidence})
        emit('discovery_netem_recovery_confirmed', receipt=recovery)
    if covers_delivery and installed:
        # Second, explicitly bounded netem segment: the real public delivery.
        # The sealed recovery contract requires boundary < delivery window, so
        # the stimulus segment above restores first; the delivery window then
        # opens inside its own verified install..restore segment.
        dbegin = carrier.start(spec)
        dbegin_counter = dbegin.get('counter_snapshot')
        # The install snapshot lives on the begin receipt itself; emitting an
        # extra counter here would place a stimulus-series counter after that
        # series' own ``data_mutation_end``.
        delivery_install_event = emit('delivery_netem_begin', receipt=dbegin,
                                      mutation_receipt=dict(receipt), neutral=dbegin['neutral'],
                                      scope=scope, segment='post_restore_delivery')
        install_ref = delivery_install_event['raw_event_ref']
        install_index = delivery_install_event.get('namespace_event_index')
        if data_scope:
            emit('data_mutation_begin', window='data_netem_delivery', scope=scope,
                 segment='post_restore_delivery',
                 spec=dict(spec), port_groups=dbegin.get('groups'),
                 port_group_ids=[group['group_id'] for group in dbegin.get('groups') or []],
                 tc_dump_sha256=dbegin.get('tc_dump_sha256'), tc_dump=dbegin.get('tc_dump'),
                 loss_pct=NETEM_LOSS_PCT, loss_defaulted=True,
                 responsibility_layer=NETEM_RESPONSIBILITY_LAYER,
                 hit_semantics=netem_hit_semantics(scope),
                 counter_snapshot=dbegin_counter,
                 mutation_receipt=dict(receipt))
        delivery_open[0] = True
        delivery = _run_post_restore_delivery(c, emit, pool, events, pause)
        last_counter = carrier.counters()
        sample(last_counter)
        dend = carrier.restore()
        delivery_restored = dend['restored'] is True
        if mutation_active:
            receipt['applied'] = bool(receipt['applied'] and delivery_restored)
        if data_scope:
            dend_counter = dend.get('counter_snapshot') or {}
            emit('data_mutation_end', window='data_netem_delivery', scope=scope,
                 segment='post_restore_delivery',
                 restored=delivery_restored,
                 tc_dump_after_sha256=dend.get('tc_dump_after_sha256'),
                 tc_dump_sha256=dbegin.get('tc_dump_sha256'),
                 residual_qdisc=dend.get('residual_qdisc'),
                 residual_filter=dend.get('residual_filter'),
                 only_noqueue_root=dend.get('only_noqueue_root'),
                 counter_snapshot=dend.get('counter_snapshot'),
                 counter_observed=dend.get('counter_observed'),
                 group_counters=dend_counter.get('group_counters'),
                 group_counters_available=dend_counter.get('group_counters_available'),
                 hit_groups=_netem_hit_groups(dend_counter),
                 loss_pct=NETEM_LOSS_PCT, loss_defaulted=True,
                 responsibility_layer=NETEM_RESPONSIBILITY_LAYER,
                 hit_semantics=netem_hit_semantics(scope),
                 restore_receipt=dend)
        restore_event = emit('delivery_netem_end', receipt=dend,
                             mutation_receipt=dict(receipt), segment='post_restore_delivery')
        restore_ref = restore_event['raw_event_ref']
        restore_index = restore_event.get('namespace_event_index')
        delivery_restore_wall_ns, delivery_restore_mono_ns = time.time_ns(), time.monotonic_ns()
        delivery_boundary = emit(
            'delivery_restore_boundary', restored=delivery_restored,
            netem_was_installed=True, netem_installed=False,
            netem_neutral_spec=spec['neutral'], carrier='discovery_netem',
            segment='post_restore_delivery',
            post_restore_unix_ns=delivery_restore_wall_ns,
            post_restore_monotonic_ns=delivery_restore_mono_ns,
            clock_guard_ns=CLOCK_GUARD_NS,
            activity_since_unix_ns=delivery_restore_wall_ns + CLOCK_GUARD_NS,
            stimulus_boundary_ref=boundary_ref,
            delivery_window_refs={'open_event_ref': delivery['open_ref'],
                                  'close_event_ref': delivery['close_ref']})
        delivery_boundary_ref = delivery_boundary['raw_event_ref']
        delivery_boundary_index = delivery_boundary.get('namespace_event_index')
        # The final graph snapshot must be anchored after the last restore.
        restore_wall_ns, restore_mono_ns = delivery_restore_wall_ns, delivery_restore_mono_ns
        restored = bool(restored and delivery_restored)
    receipt['hit'] = bool(mutation_active and counters_hit)
    if mutation_active:
        # netem matched-packet counters are carrier evidence only; they can
        # never be reported as a discovery_drop hit and never as a SUT defect.
        receipt['effect_observed'] = bool(receipt['hit'] and restored and activity_ok and fresh_recovery)
    else:
        receipt['effect_observed'] = bool(activity_ok and fresh_recovery)
    if covers_delivery:
        # The delivery segment installed its own qdisc, so its final snapshot is
        # the data-plane count for the judged delivery only, never the stimulus
        # segment's packets.
        delivery_counter = last_counter
        data_counters_hit = bool(_netem_counter_hit(delivery_counter, 'data'))
    data_plane_known = bool(data_scope and installed
                            and (last_counter or {}).get('group_counters_available') is True)
    if covers_delivery:
        emit('netem_delivery_overlap', delivery_inside_mutation_window=True,
             install_event_ref=install_ref, delivery_open_ref=delivery['open_ref'],
             delivery_close_ref=delivery['close_ref'], restore_event_ref=restore_ref,
             boundary_event_ref=delivery_boundary_ref,
             stimulus_install_ref=stimulus_manifest['install_ref'],
             stimulus_restore_ref=stimulus_manifest['restore_ref'],
             stimulus_boundary_ref=stimulus_manifest['boundary_ref'],
             install_event_index=install_index,
             delivery_open_event_index=delivery['open_event_index'],
             delivery_close_event_index=delivery['close_event_index'],
             restore_event_index=restore_index, boundary_event_index=delivery_boundary_index,
             chronology='install<delivery_open<delivery_close<restore<boundary',
             origin='runtime.netem_covers_delivery',
             is_candidate_adjudication=False)
    emit('window_close', window='discovery_netem', active_mutation=mutation_active,
         netem_installed=installed, neutral_spec=spec['neutral'], scope=scope,
         data_plane_counter_hit=(bool(data_counters_hit) if data_plane_known else None),
         hit_groups=_netem_hit_groups(stimulus_counter) if data_scope else None,
         mutation_receipt=dict(receipt))
    effect = emit('mutation_effect' if mutation_active else 'neutral_control_observation',
                  effect_id='discovery_netem_then_restore', mutation_receipt=dict(receipt),
                  discovery_activity_observed=activity_ok, fresh_count_graph=fresh_recovery,
                  netem_last_counter=stimulus_counter, restore_boundary_ref=boundary['raw_event_ref'],
                  scope=scope,
                  data_plane_counter_hit=(bool(data_counters_hit) if data_plane_known else None),
                  neutral_no_op=not mutation_active)
    if covers_delivery:
        emit('netem_delivery_effect', effect_ref=effect['raw_event_ref'],
             delivery_restore_boundary_ref=delivery_boundary_ref,
             delivery_inside_mutation_window=True,
             is_candidate_adjudication=False)
    result = {'mutation_active': mutation_active, 'installed': installed,
              'neutral_spec': spec['neutral'], 'restored': restored if installed else None,
              'counter_hit': bool(receipt['hit']), 'activity_ok': activity_ok,
              'fresh_recovery': fresh_recovery, 'last_counter': last_counter,
              'window_sec': window_sec, 'spec': spec, 'begin': begin,
              'restore_mono_ns': restore_mono_ns,
              'scope': scope,
              'data_plane_counter_hit': (bool(data_counters_hit) if data_plane_known else None)}
    if covers_delivery:
        # Additive receipts for the covered-delivery contract only.  The
        # legacy shape keeps its result payload unchanged.
        result.update(
            covers_delivery=True, delivery=delivery,
            delivery_inside_mutation_window=True,
            delivery_window_refs={'open_event_ref': delivery['open_ref'],
                                  'close_event_ref': delivery['close_ref']},
            netem_install_ref=install_ref, netem_restore_ref=restore_ref,
            netem_boundary_ref=delivery_boundary_ref,
            delivery_boundary_ref=delivery_boundary_ref,
            stimulus_netem_manifest=stimulus_manifest,
            data_group_counters_available=(last_counter or {}).get('group_counters_available')
            if (data_scope and installed) else None,
            data_group_packets=(_data_group_counter(last_counter)['packets']
                                if _data_group_counter(last_counter) is not None else None))
    return result


def _run_post_restore_delivery(c, emit, pool, events, pause):
    """The single bounded post-restore delivery sequence.

    Both the legacy (window strictly after restore) and the
    ``netem_covers_delivery`` (window opened while the tc class is still
    installed) callers run this one implementation, so send/receive tokens,
    the delivery contract and the window events cannot drift apart.
    """
    expected = list(range(c['pre_match_count'], c['pre_match_count'] + c['post_match_count']))
    omitted = expected[len(expected) // 2] if c['fault_id'] else None
    emit('delivery_contract', expected_sequences=expected, sentinel_sequence=expected[-1], phase='post_restore',
         fault_id=c['fault_id'], contract_includes_harness_omission=True, prejoin_in_delivery_contract=False, qos=c['qos'])
    opening = emit('window_open', window='post_restore_delivery', expected_sequences=expected)
    for sequence in expected:
        cid = pool.command('writer', 'publish', send_sequence=sequence, phase='post_restore', omit=sequence == omitted)
        pool.wait(lambda cid=cid: sum(e['event'] == 'command_done' and e.get('role') == 'writer' and e.get('command_id') == cid for e in events) == 1,
                  c['startup_timeout_sec'], 'postrestore_publish_ack')
        pause(c['publish_interval_sec'])
    def delivered():
        received = {e['send_sequence'] for e in events if e['event'] == 'receive' and e.get('phase') == 'post_restore'}
        return set(expected) - ({omitted} if omitted is not None else set()) <= received
    timed_out = False
    try:
        pool.wait(delivered, c['delivery_timeout_sec'], 'postrestore_delivery_deadline')
    except TimeoutError:
        timed_out = True
    closing = emit('window_close', window='post_restore_delivery', deadline_expired=timed_out)
    return {'expected_sequences': expected, 'omitted_sequence': omitted,
            'timed_out': timed_out,
            'open_ref': opening['raw_event_ref'], 'close_ref': closing['raw_event_ref'],
            'open_event_index': opening.get('namespace_event_index'),
            'close_event_index': closing.get('namespace_event_index')}


def _body(request, owner_channel):
    if request['config']['target_id'] in ('builtin.discovery_projection_reconciliation',
                                          'builtin.discovery_projection_reconciliation.fuzz_v1'):
        from .projection_pair import namespace_body
        return namespace_body(request, owner_channel)
    from .ros2_pair import RECEIPT_KEYS, wait_for_fresh_match
    c, output_dir = request['config'], Path(request['output_dir'])
    events, preconditions, observations = [], [], []
    condition_refs = {}
    receipt = dict.fromkeys(RECEIPT_KEYS, False)
    active = c['arm'] == 'mutated'
    receipt['planned'] = active
    result = {'runtime_status': 'failed', 'reason': None, 'facts': {}, 'environment': {}, 'workers': [],
              'precondition_receipts': preconditions, 'observation_receipts': observations, 'mutation_receipt': receipt,
              'effective_rmws': effective_rmws_receipt(c)}
    pool = observer = carrier = None
    trajectories = []
    netem_delivery = None
    netem_overlap_facts = None
    data_plane_facts = (None, None)
    counter_hit = restored = activity_ok = fresh_recovery = drop_activity_ok = False
    carrier_kind = discovery_carrier_kind(c)
    implementation = c.get('carrier_implementation', IMPLEMENTATION)
    restored_aggregate = None
    window_records = []
    window_hits = []
    window_drop_activity = []
    window_recovery_activity = []
    window_fresh_recovery = []
    window_restored = []
    endpoints = {}
    raw_path = output_dir / 'raw.jsonl'
    def emit(event, **fields):
        token = {'event': event, 'role': 'controller', 'pid': os.getpid(), 'pgid': os.getpgrp(),
                 'run_id': c['run_id'], 'domain_id': c['domain_id'], 'time_ns': time.time_ns(),
                 'monotonic_ns': time.monotonic_ns(), **fields}
        token['raw_event_ref'] = f'{raw_path}#namespace_event={len(events)}'
        token['namespace_event_index'] = len(events)
        token = json.loads(json.dumps(token, sort_keys=True))
        events.append(token)
        print(json.dumps(token, sort_keys=True), flush=True)
        return token
    def step(name, requires, action, *, observation=False):
        record = {'step_id': name, 'requires': requires, 'receipt_kind': 'observation' if observation else 'precondition',
                  'before': {'monotonic_ns': time.monotonic_ns(), 'satisfied': False, 'basis': 'not_yet_verified_for_this_step'},
                  'requires_results': {dep: {'satisfied': dep in condition_refs, 'evidence_ref': condition_refs.get(dep)} for dep in requires},
                  'after': None, 'satisfied': False}
        (observations if observation else preconditions).append(record)
        before = emit('state_step_before', **record)
        record['before']['raw_event_ref'] = before['raw_event_ref']
        try:
            if any(dep not in condition_refs for dep in requires):
                raise RuntimeError('state_precondition_dependency_missing:' + name)
            evidence = action()
            record['satisfied'] = True
            record['evidence'] = evidence
            return evidence
        finally:
            record['after'] = {'monotonic_ns': time.monotonic_ns(), 'satisfied': record['satisfied']}
            after = emit('state_step_after', **record)
            record['after']['raw_event_ref'] = after['raw_event_ref']
            if record['satisfied']:
                condition_refs[name] = after['raw_event_ref']
    def pause(duration):
        deadline = time.monotonic() + duration
        pool.wait(lambda: time.monotonic() >= deadline, duration + 1, 'discovery_window')
    def snapshot_pair(anchor):
        matches = wait_for_fresh_match(pool, events, endpoints['reader']['created_monotonic_ns'], c['match_timeout_sec'], emit)
        if any(token['sampled_monotonic_ns'] < anchor for token in matches.values()):
            raise RuntimeError('fresh_count_precedes_required_boundary')
        graphs = acknowledged_graphs(events, matches, endpoints)
        return {'matches': {role: event['raw_event_ref'] for role, event in matches.items()},
                'graphs': {role: event['raw_event_ref'] for role, event in graphs.items()}}
    def activity(since_ns, label, after_monotonic_ns=None):
        value = observer.snapshot_activity(since_ns=since_ns,
                    participant_prefixes=[endpoints[role]['endpoint_gid'][:24] for role in ('writer', 'reader')])
        return emit('discovery_activity_observation', window=label, activity=value,
                    activity_observed=activity_is_pair_observed(value, after_monotonic_ns=after_monotonic_ns),
                    after_monotonic_ns=after_monotonic_ns,
                    clock_guard_ns=CLOCK_GUARD_NS if str(label).startswith('post_restore') else 0)
    try:
        proof = validate_host_leases(request)
        if not same_source(request['source'], execution_chain_identity()):
            raise RuntimeError('source_identity_changed_before_namespace_execution')
        admission_event = emit('namespace_host_admission_verified', isolation=proof, lease_paths=[x['path'] for x in request['leases']])
        condition_refs.update(environment_ok=admission_event['raw_event_ref'], locks_held=admission_event['raw_event_ref'])
        port_probe = probe_rtps_ports(c['domain_id'])
        if not port_probe['ok']:
            raise IsolationError('domain_conflict:namespace_rtps_port_block')
        carrier = (DiscoveryNetemController(c['domain_id']) if carrier_kind == 'discovery_netem'
                   else DiscoveryDropController(c['domain_id']))
        setup = carrier.setup()
        setup_event = emit('discovery_network_setup', receipt=setup)
        # Profiles are written per role; the legacy single-rmw call is kept for
        # the exact same file layout and hashes when rmws is absent.
        if role_rmw_differs_from_config(c):
            profile_records, role_env = _write_role_profiles(output_dir, c)
        else:
            profile_records, role_env = write_profiles(output_dir, c['rmw'])
        role_env = role_rmw_environments(c, role_env)
        identity = dict(proof)
        from r2f.instrumentation.sedp import PassiveSedpObserver
        spec = recovery_capture_spec(output_dir=output_dir / 'discovery-observer', run_id=c['run_id'],
                                     domain_id=c['domain_id'], netns_identity=identity)
        observer = PassiveSedpObserver(spec)
        ready = observer.start()
        ready_event = emit('discovery_observer_ready', receipt=ready)
        if setup.get('health_ok') is not True or ready.get('status') != 'ready':
            raise RuntimeError('namespace_observer_not_ready')
        namespace_ready = emit('namespace_ready', ready=True,
             admission_ref=admission_event['raw_event_ref'], carrier_setup_ref=setup_event['raw_event_ref'],
             observer_ready_ref=ready_event['raw_event_ref'], isolation=proof)
        condition_refs['namespace_ready'] = namespace_ready['raw_event_ref']
        result['environment'].update(isolation=proof, namespace_rtps_probe=port_probe, setup=setup,
                                     profiles=profile_records, observer_ready=ready,
                                     implementation=implementation, carrier_kind=carrier_kind,
                                     environment_mutation=c.get('environment_mutation'))
        base_environment = dict(os.environ)
        if role_rmw_differs_from_config(c):
            # Per-role profile selectors are the only authority when any role
            # overrides the global vendor.
            base_environment = mixed_base_environment(base_environment)
        pool = OwnerCheckedPool(c, base_environment, output_dir, emit,
                                owner_channel=owner_channel, role_environments=role_env)
        def create():
            for role in ('writer', 'reader'):
                if role == 'reader':
                    prejoin_refs = []
                    for sequence in range(c['pre_match_count']):
                        cid = pool.command('writer', 'publish', send_sequence=sequence, phase='prejoin')
                        pool.wait(lambda cid=cid: sum(e['event'] == 'command_done' and e.get('role') == 'writer'
                            and e.get('command_id') == cid for e in events) == 1,
                            c['startup_timeout_sec'], 'prejoin_publish_ack')
                        sent = [e for e in events if e['event'] == 'send' and e.get('phase') == 'prejoin'
                                and e.get('send_sequence') == sequence and e.get('publish_returned') is True]
                        if len(sent) != 1:
                            raise RuntimeError('prejoin_publish_receipt_incomplete')
                        prejoin_refs.append(sent[0]['raw_event_ref'])
                    emit('prejoin_publish_complete', count=c['pre_match_count'], publish_refs=prejoin_refs,
                         post_restore_delivery_obligation=False)
                    delay_begin = emit('reader_creation_delay_begin', configured_delay_sec=c['reader_delay_sec'])
                    pause(c['reader_delay_sec'])
                    emit('reader_creation_delay_end', configured_delay_sec=c['reader_delay_sec'],
                         delay_begin_ref=delay_begin['raw_event_ref'])
                pool.start(role)
                pool.wait(lambda role=role: any(e['event'] == 'endpoint_created' and e.get('role') == role for e in events),
                          c['startup_timeout_sec'], role + '_created')
                endpoints[role] = next(e for e in events if e['event'] == 'endpoint_created' and e.get('role') == role)
                token = endpoints[role]
                require_role_rmw(c, role, token)
                if not token.get('endpoint_gid'):
                    raise RuntimeError('endpoint_identity_incomplete:' + role)
            if endpoints['writer']['pid'] == endpoints['reader']['pid']:
                raise RuntimeError('independent_worker_processes_required')
            result['effective_rmws'] = effective_rmws_receipt(c, endpoints)
            emit('effective_rmws_observed', receipt=result['effective_rmws'],
                 mixed=result['effective_rmws']['mixed'])
            return {role: token['raw_event_ref'] for role, token in endpoints.items()}
        step('create_endpoints', ['environment_ok', 'locks_held', 'namespace_ready'], create)
        matched = step('establish_match', ['create_endpoints'], lambda: snapshot_pair(endpoints['reader']['created_monotonic_ns']))
        pids = {role: token['pid'] for role, token in endpoints.items()}
        binding = step('verify_namespace_bindings', ['establish_match'], lambda: carrier.pair_bindings(pids))
        precondition = emit('discovery_precondition_snapshot', satisfied=True, precondition_receipts=preconditions,
                            writer_gid=endpoints['writer']['endpoint_gid'], reader_gid=endpoints['reader']['endpoint_gid'],
                            worker_pids=pids, bindings=binding, matched=matched)
        match_receipt = {'domain_id': c['domain_id'], 'match_established': True,
                         'evidence_ref': precondition['raw_event_ref'], 'worker_pids': pids,
                         **{role + '_gid': token['endpoint_gid'] for role, token in endpoints.items()}}
        if carrier_kind == 'discovery_netem':
            netem = _run_netem_window(c, active, receipt, emit, step, activity,
                                      activity_is_pair_observed, snapshot_pair, pause,
                                      carrier, pool, events, endpoints, matched,
                                      precondition, match_receipt)
            counter_hit = bool(netem['counter_hit'])
            netem_covers = bool(netem.get('covers_delivery'))
            netem_delivery = netem['delivery'] if netem_covers else None
            if netem_covers:
                netem_overlap_facts = {
                    'inside': netem['delivery_inside_mutation_window'],
                    'window_refs': netem['delivery_window_refs'],
                    'install_ref': netem['netem_install_ref'],
                    'restore_ref': netem['netem_restore_ref'],
                    'boundary_ref': netem['netem_boundary_ref'],
                    'chronology': 'install<delivery_open<delivery_close<restore<boundary'}
            if netem_covers and netem['scope'] in ('data', 'both'):
                # Only the covered-delivery contract reports a data-plane
                # observation: the legacy shape must stay byte-identical.
                if netem.get('data_group_counters_available') is False:
                    # No trustworthy per-group attribution.  Never collapse this
                    # into mutation_ineffective: it is an observation limit.
                    data_plane_facts = (False, 'tc_action_group_counters_unavailable')
                elif netem['data_group_packets']:
                    data_plane_facts = (True, None)
                else:
                    # The window really covered the delivery, so a zero
                    # data-group count means the samples went over another
                    # transport (e.g. SHM) or were not attributable to these
                    # UDP ports.  This is an observation limit, not a
                    # mutation_ineffective verdict.
                    data_plane_facts = (
                        False, 'data_plane_not_observed_inside_covered_window:'
                               'possibly_shm_or_non_udp_transport')
            restored = netem['restored']
            activity_ok = bool(netem['activity_ok'])
            fresh_recovery = bool(netem['fresh_recovery'])
            drop_activity_ok = activity_ok
            # The netem window owns its own restore boundary; the shared final
            # graph step must use it instead of the drop-chain local, which is
            # never assigned on this branch.
            final_restore_mono_ns = netem['restore_mono_ns']
            result['environment']['discovery_netem'] = {
                'spec': netem['spec'], 'window_sec': netem['window_sec'],
                'neutral_spec': netem['neutral_spec'],
                'mutation_active': netem['mutation_active'],
                'netem_installed': netem['installed'],
                'scope': netem['scope'],
                'hit_semantics': netem_hit_semantics(netem['scope']),
                'responsibility_layer': NETEM_RESPONSIBILITY_LAYER,
                'loss_pct': NETEM_LOSS_PCT, 'loss_defaulted': True,
                'begin_receipt': netem['begin'],
                'last_counter': netem['last_counter'],
                'covers_delivery': netem_covers,
                'data_plane_counter_hit': netem['data_plane_counter_hit']}
            if netem_covers:
                result['environment']['discovery_netem'].update(
                    delivery_inside_mutation_window=netem['delivery_inside_mutation_window'],
                    delivery_window_refs=netem['delivery_window_refs'],
                    netem_install_ref=netem['netem_install_ref'],
                    netem_restore_ref=netem['netem_restore_ref'],
                    netem_boundary_ref=netem['netem_boundary_ref'],
                    data_plane_observed=(None if netem['data_group_counters_available'] is None
                                         else bool(netem['data_plane_counter_hit'])),
                    data_plane_observation_reason=data_plane_facts[1],
                    data_group_packets=netem['data_group_packets'])
        elif (not c.get('window_chain_requested')
              and not window_churn_schedule(c, c['drop_duration_sec'])
              and not c.get('discovery_probe', False)):
            # Legacy without optional stimuli: behaviour is preserved, including the
            # restore-in-finally drop loop that its AST contract test inspects
            # directly inside this function body.
            if active:
                receipt.update(submitted=True, carrier_admitted=True)
                emit('mutation_begin', mutation_kind='discovery_drop', carrier='wire_proxy', implementation=IMPLEMENTATION,
                     precondition_snapshot_ref=precondition['raw_event_ref'], mutation_receipt=dict(receipt))
                drop_wall_ns = time.time_ns()
                begin = carrier.start(match_receipt=match_receipt)
                receipt.update(configured=begin['configured'], applied=begin['applied'])
                emit('discovery_drop_begin', receipt=begin, mutation_receipt=dict(receipt))
                drop_wall_ns = time.time_ns()
            else:
                drop_wall_ns = time.time_ns()
                emit('control_begin', control_kind=c['arm'], drop_installed=False,
                     precondition_snapshot_ref=precondition['raw_event_ref'], mutation_receipt=dict(receipt))
            emit('window_open', window='discovery_interruption', active_drop=active)
            trajectory_start_ns = time.monotonic_ns()
            deadline_ns = trajectory_start_ns + int(c['drop_duration_sec'] * 10**9)
            deadline = deadline_ns / 10**9
            trajectory = _RecoveryMatchTrajectory(events, endpoints, emit, trajectory_start_ns, deadline_ns, matched)
            try:
                while time.monotonic() < deadline:
                    if active:
                        counter = carrier.counters()
                        counter_hit = counter_hit or counter['hit']
                        emit('discovery_drop_counter', receipt=counter)
                    trajectory.poll(pool)
                    pause(min(c['counter_interval_sec'], max(0, deadline - time.monotonic()),
                              max(0, (trajectory.next_due_ns - time.monotonic_ns()) / 10**9)))
                during = activity(drop_wall_ns, 'discovery_interruption')
                drop_activity_ok = activity_is_pair_observed(during['activity'])
            finally:
                trajectory.restore_started_ns = time.monotonic_ns()
                if active:
                    restore = carrier.restore()
                    restored = restore['restored']
                    counter_hit = counter_hit or restore.get('hit', False)
                    emit('discovery_drop_end', receipt=restore)
            # During an intentional discovery drop, absence of pair discovery
            # activity is the expected effect, not a failed mutation.  Hit is
            # established by the window counter plus verified restore/recovery.
            receipt['hit'] = bool(active and counter_hit)
            emit('window_close', window='discovery_interruption', active_drop=active, mutation_receipt=dict(receipt))
            # Paired clocks are captured AFTER verified deletion, or the explicit neutral boundary.
            restore_wall_ns, restore_mono_ns = time.time_ns(), time.monotonic_ns()
            boundary = emit('discovery_restore_boundary', restored=restored if active else None, neutral=not active,
                            drop_was_installed=active, drop_installed=False, post_restore_unix_ns=restore_wall_ns,
                            post_restore_monotonic_ns=restore_mono_ns, clock_guard_ns=CLOCK_GUARD_NS,
                            activity_since_unix_ns=restore_wall_ns + CLOCK_GUARD_NS)
            trajectory.restore_boundary_ns = restore_mono_ns
            result['environment']['recovery_clock'] = {'unix_ns': restore_wall_ns, 'monotonic_ns': restore_mono_ns,
                                                       'source': 'host_kernel_realtime_and_shared_monotonic', 'clock_guard_ns': CLOCK_GUARD_NS,
                                                       'limitation': 'packet realtime is checked for clock offset discontinuity'}
            recovery_deadline = time.monotonic() + c['recovery_timeout_sec']
            activity_event = None
            while time.monotonic() < recovery_deadline:
                activity_event = activity(restore_wall_ns + CLOCK_GUARD_NS, 'post_restore', after_monotonic_ns=restore_mono_ns)
                activity_ok = activity_event['activity_observed']
                clock_offset = time.time_ns() - time.monotonic_ns()
                if abs(clock_offset - (restore_wall_ns - restore_mono_ns)) > CLOCK_GUARD_NS:
                    activity_ok = False
                    emit('discovery_observation_unknown', reason='realtime_monotonic_offset_discontinuity')
                    break
                if activity_ok:
                    break
                pause(0.2)
            try:
                post_evidence = step('observe_recovery_graph', ['verify_namespace_bindings'],
                                     lambda: snapshot_pair(restore_mono_ns), observation=True)
                trajectory.after = post_evidence
                fresh_recovery = True
            except (TimeoutError, RuntimeError) as exc:
                emit('discovery_observation_unknown', reason=str(exc))
            if active and activity_ok and fresh_recovery:
                recovery = carrier.confirm_recovery(activity_receipt={**match_receipt,
                    'monotonic_ns': time.monotonic_ns(), 'activity_observed': True, 'source': 'public_ros2_pair',
                    'evidence_ref': activity_event['raw_event_ref'], 'fresh_graph_count': post_evidence})
                emit('discovery_recovery_confirmed', receipt=recovery, requires_runtime_semantic_adjudication=True)
            receipt['effect_observed'] = bool(receipt['hit'] and restored and activity_ok and fresh_recovery)
            emit('mutation_effect' if active else 'neutral_control_observation', effect_id='discovery_interruption_then_restore',
                 mutation_receipt=dict(receipt), discovery_activity_observed=activity_ok, fresh_count_graph=fresh_recovery,
                 restore_boundary_ref=boundary['raw_event_ref'])
        else:
            _run_window_chain(c, active, receipt, emit, step, activity, activity_is_pair_observed,
                              snapshot_pair, pause, carrier, pool, events, endpoints, matched,
                              precondition, match_receipt, trajectories, window_records,
                              window_hits, window_drop_activity, window_recovery_activity,
                              window_fresh_recovery, window_restored)
            counter_hit = bool(window_hits) and all(window_hits)
            drop_activity_ok = bool(window_drop_activity) and all(window_drop_activity)
            restored = bool(window_restored) and all(window_restored)
            activity_ok = bool(window_recovery_activity) and all(window_recovery_activity)
            fresh_recovery = bool(window_fresh_recovery) and all(window_fresh_recovery)
            receipt['hit'] = bool(active and counter_hit)
            # activity during the drop window is expected to be absent
            receipt['effect_observed'] = bool(receipt['hit'] and restored and fresh_recovery)
            emit('mutation_effect' if active else 'neutral_control_observation',
                 effect_id='discovery_interruption_then_restore', mutation_receipt=dict(receipt),
                 discovery_activity_observed=activity_ok, fresh_count_graph=fresh_recovery,
                 window_hits=window_hits)
        if netem_delivery is not None:
            # netem_covers_delivery already ran the delivery inside the installed
            # class; the delivery/overlap receipts came from that same call.
            omitted = netem_delivery['omitted_sequence']
        else:
            omitted = _run_post_restore_delivery(c, emit, pool, events, pause)['omitted_sequence']
        try:
            if window_records:
                final_restore_mono_ns = window_records[-1]['restore_boundary_monotonic_ns']
            step('observe_final_graph', ['verify_namespace_bindings'],
                 lambda: snapshot_pair(final_restore_mono_ns), observation=True)
        except (TimeoutError, RuntimeError) as exc:
            fresh_recovery = False
            emit('discovery_observation_unknown', reason=str(exc))
        if c['fault_id']:
            omissions = [e for e in events if e['event'] == 'harness_omission' and e.get('send_sequence') == omitted]
            emit('fault_control_receipt', fault_id=c['fault_id'], omitted_sequence=omitted, applied=len(omissions) == 1,
                 omission_event_ref=omissions[0]['raw_event_ref'] if omissions else None,
                 responsibility_layer='harness', candidate_eligible=False)
        if c.get('target_id') == F2_WINDOWS_TARGET:
            _emit_f2_operation_receipts(c, events, emit)
            ops = [e['receipt'] for e in events if e.get('event') == 'f2_operation_receipt']
            receipt['hit'] = bool(ops and all(o['hit'] for o in ops))
            receipt['effect_observed'] = bool(ops and all(o['effect_observed'] for o in ops))
        result.update(runtime_status='completed', reason=None)
    except Exception as exc:
        import traceback as _tb
        result.update(runtime_status='failed', reason=type(exc).__name__ + ':' + str(exc))
        emit('runtime_abort', reason=result['reason'], error_receipt=getattr(exc, 'receipt', None),
             tool_error_traceback=_tb.format_exc())
    finally:
        if carrier is not None:
            try:
                cleanup = carrier.restore()
                emit('discovery_cleanup_restore', receipt=cleanup)
            except Exception as exc:
                # V17: preserve the original failure; cleanup failure is
                # additional evidence, never a replacement error message.
                result['cleanup_error'] = 'discovery_restore_cleanup_failed:' + str(exc)
                emit('discovery_cleanup_failed', reason=result['cleanup_error'],
                     original_reason=result.get('reason'),
                     error_receipt=getattr(exc, 'receipt', None))
        if observer is not None:
            try:
                stopped = observer.stop()
                result['environment']['observer_stop'] = stopped
                emit('discovery_observer_stopped', receipt=stopped)
                if (stopped.get('health', {}).get('status') != 'ok'
                        or stopped.get('thread_stopped') is not True or stopped.get('socket_closed') is not True):
                    activity_ok = False
                    receipt['effect_observed'] = False
                    result.update(runtime_status='failed', reason='observer_final_health_incomplete')
                    emit('discovery_observation_unknown', reason=result['reason'])
            except Exception as exc:
                result.update(runtime_status='failed', reason='observer_cleanup_failed:' + str(exc))
                emit('runtime_abort', reason=result['reason'])
        if pool is not None:
            pool.check_owner = False
            try:
                pool.close()
            except Exception as exc:
                result.update(runtime_status='failed', reason='worker_cleanup_failed:' + str(exc))
                emit('runtime_abort', reason=result['reason'])
            result['workers'] = pool.metadata
            if any(worker.get('returncode') != 0 for worker in pool.metadata):
                result.update(runtime_status='failed', reason='worker_shutdown_failed')
        for trajectory in trajectories:
            emit('discovery_match_trajectory', active_drop=active, **trajectory.summary())
        if window_records:
            restored_aggregate = (all(r['restored'] is True for r in window_records)
                                  if active else None)
        result['facts'] = summarize_discovery(events, active=receipt['applied'], counter_hit=counter_hit,
            restored=restored_aggregate if window_records else restored,
            activity_ok=activity_ok, fresh_recovery=fresh_recovery, drop_activity_ok=drop_activity_ok,
            carrier_implementation=implementation, carrier_kind=carrier_kind,
            neutral_spec=bool(carrier_kind == 'discovery_netem'
                              and c['environment_mutation']['spec']['neutral']),
            netem_scope=(_netem_scope(c['environment_mutation']['spec'])
                         if carrier_kind == 'discovery_netem' else None),
            delivery_overlap=netem_overlap_facts,
            data_plane_observed=data_plane_facts[0],
            data_plane_reason=data_plane_facts[1])
        if window_records:
            result['facts'].update(discovery_windows=window_records,
                discovery_window_count=len(window_records),
                discovery_window_chain_requested=bool(c.get('window_chain_requested')),
                discovery_window_chain_complete=bool(
                    all(r['restored'] is True and r['recovery_activity_observed'] and r['fresh_count_graph']
                        for r in window_records) if active else True),
                discovery_window_hits=[r['counter_hit'] for r in window_records],
                discovery_counter_semantics='per_window_delta_only')
        if not same_source(request['source'], execution_chain_identity()):
            result.update(runtime_status='failed', reason='source_identity_changed_during_execution')
        if not any(e['event'] == 'endpoint_created' for e in events):
            result['runtime_status'] = 'blocked'
    return result


def activity_is_pair_observed(activity, *, after_monotonic_ns=None):
    """Observer contract is deliberately strict: absence/unknown is not positive evidence."""
    if not isinstance(activity, dict) or activity.get('status') != 'observed':
        return False
    if activity.get('health', {}).get('status') != 'ok' or activity.get('window_buffer_retained') is not True:
        return False
    prefixes = activity.get('participant_prefixes', [])
    if len(prefixes) != 2 or len(set(prefixes)) != 2:
        return False
    since, until = activity.get('since_ns'), activity.get('until_ns')
    if type(since) is not int or type(until) is not int or until < since:
        return False
    observed = set()
    for event in activity.get('events', []):
        stamp = event.get('unix_ns')
        if (event.get('source_guid_prefix') in prefixes and type(stamp) is int and since < stamp <= until
                and event.get('clock') == 'kernel_timestampns'
                and (after_monotonic_ns is None or
                     (type(event.get('received_monotonic_ns')) is int and event['received_monotonic_ns'] > after_monotonic_ns))
                and event.get('builtin_writer_id') in ('000100c2', '000003c2', '000004c2')
                and event.get('kind', '').startswith(('SPDP_', 'SEDP_'))
                and isinstance(event.get('raw_ref'), dict) and event['raw_ref'].get('frame_sha256')):
            observed.add(event['source_guid_prefix'])
    return observed == set(prefixes)


def main():
    if len(sys.argv) != 3 or sys.argv[1] != '_child':
        raise SystemExit('private namespace child entry requires a host-owned channel')
    channel = socket.socket(fileno=int(sys.argv[2]))
    data = b''
    channel.settimeout(10)
    while b'\n' not in data:
        chunk = channel.recv(65536)
        if not chunk:
            raise RuntimeError('host_request_missing')
        data += chunk
        if len(data) > 1024 * 1024:
            raise RuntimeError('host_request_too_large')
    request = json.loads(data.split(b'\n', 1)[0])
    channel.setblocking(False)
    try:
        result = _body(request, channel)
        print(json.dumps({'event': '_namespace_result', 'result': result}, sort_keys=True), flush=True)
    finally:
        channel.close()


if __name__ == '__main__':
    main()
