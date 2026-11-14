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

from .discovery_network import DiscoveryDropController, assert_isolated_namespace, namespace_command
from .discovery_profiles import write_profiles
from .domains import IsolationError, LeaseSet, probe_rtps_ports
from .environment import REPO, activated_environment, check_environment, source_identity
from .process import WorkerPool

IMPLEMENTATION = 'isolated_netns_nft_discovery_udp_drop'
CLOCK_GUARD_NS = 50_000_000


def execution_chain_identity():
    result = source_identity()
    digest = hashlib.sha256()
    for path in sorted((REPO / 'src/r2f/instrumentation').rglob('*.py')):
        digest.update(str(path.relative_to(REPO)).encode() + b'\0' + path.read_bytes())
    result['observer_source_sha256'] = digest.hexdigest()
    return result


def same_source(before, after):
    return all(before.get(key) == after.get(key) for key in ('head', 'runtime_source_sha256', 'observer_source_sha256'))


def normalize_discovery_config(config):
    c = dict(config)
    params = c['mutation_parameters']
    for key, default in (('drop_duration_sec', 3.0), ('recovery_timeout_sec', 8.0), ('counter_interval_sec', 0.25)):
        value = c.get(key, params.get(key, default))
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= 30:
            raise ValueError('invalid_discovery_parameter:' + key)
        c[key] = float(value)
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
                 implementation=('isolated_netns_passive_sedp_public_qos' if c['target_id'] == 'builtin.discovery_projection_reconciliation' else IMPLEMENTATION),
                 inherited_lease_count=len(lease.handles), stderr_path=str(stderr_path))
            os.set_blocking(process.stdout.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 30 + 4 * c['startup_timeout_sec'] + 4 * c['match_timeout_sec'] + c['drop_duration_sec'] + c['recovery_timeout_sec'] + c['delivery_timeout_sec'] + c['post_match_count'] * (c['publish_interval_sec'] + 1)
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


def run_discovery_pair(config, output_dir, result):
    """Host branch of the public run_pair entry; never called by the netns child."""
    from .ros2_pair import RECEIPT_KEYS
    try:
        if config['target_id'] == 'builtin.discovery_projection_reconciliation':
            from .projection_pair import normalize_projection_config
            c = normalize_projection_config(config)
        else:
            c = normalize_discovery_config(config)
    except ValueError as exc:
        result['reason'] = 'blocked:' + str(exc)
        return result
    raw_path = output_dir / 'raw.jsonl'
    try:
        raw = raw_path.open('x', buffering=1)
    except FileExistsError:
        result['reason'] = 'blocked:state_directory_already_executed'
        return result
    events = result['events']
    result['raw_path'] = str(raw_path)
    projection = c['target_id'] == 'builtin.discovery_projection_reconciliation'
    implementation = 'isolated_netns_passive_sedp_public_qos' if projection else IMPLEMENTATION
    result['carrier_implementation'] = implementation
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
        emit('runtime_source_snapshot', source=source)
        environment = check_environment(c)
        result['environment'] = environment
        emit('environment_preflight', ok=environment['ok'], reason=environment.get('reason'))
        if not environment['ok']:
            raise IsolationError(environment.get('reason', 'host_preflight_failed'))
        _, child_environment = activated_environment(c)
        inner = _supervise(c, output_dir, child_environment, lease, source, emit)
        for key in ('runtime_status', 'reason', 'facts', 'precondition_receipts', 'observation_receipts', 'mutation_receipt'):
            result[key] = inner[key]
        result['status'] = result['runtime_status']
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
        result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                      status='failed' if result['real_execution'] else 'blocked', reason=str(exc))
        emit('runtime_abort', reason=str(exc), error_receipt=getattr(exc, 'receipt', None))
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
                active=previous['discovery_drop_applied'], counter_hit=previous['discovery_drop_counter_hit'],
                restored=previous['discovery_restore_verified'], activity_ok=previous['discovery_recovery_activity_observed'],
                fresh_recovery=previous['recovery_fresh_count_graph'], drop_activity_ok=previous['discovery_drop_activity_observed'])
        result['facts']['candidate_eligible'] = c['arm'] != 'negative'
        if result['real_execution']:
            result['execution'] = {'real_execution': True, 'run_id': c['run_id'], 'target_id': c['target_id'],
                'target_revision': c['target_revision'], 'rmw': c['rmw'], 'domain_id': c['domain_id'], 'arm': c['arm'],
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


def summarize_discovery(events, *, active, counter_hit, restored, activity_ok, fresh_recovery, drop_activity_ok):
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


class OwnerCheckedPool(WorkerPool):
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


def _body(request, owner_channel):
    if request['config']['target_id'] == 'builtin.discovery_projection_reconciliation':
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
              'precondition_receipts': preconditions, 'observation_receipts': observations, 'mutation_receipt': receipt}
    pool = observer = carrier = None
    counter_hit = restored = activity_ok = fresh_recovery = drop_activity_ok = False
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
                    clock_guard_ns=CLOCK_GUARD_NS if label == 'post_restore' else 0)
    try:
        proof = validate_host_leases(request)
        if not same_source(request['source'], execution_chain_identity()):
            raise RuntimeError('source_identity_changed_before_namespace_execution')
        admission_event = emit('namespace_host_admission_verified', isolation=proof, lease_paths=[x['path'] for x in request['leases']])
        condition_refs.update(environment_ok=admission_event['raw_event_ref'], locks_held=admission_event['raw_event_ref'])
        port_probe = probe_rtps_ports(c['domain_id'])
        if not port_probe['ok']:
            raise IsolationError('domain_conflict:namespace_rtps_port_block')
        carrier = DiscoveryDropController(c['domain_id'])
        setup = carrier.setup()
        setup_event = emit('discovery_network_setup', receipt=setup)
        profile_records, role_env = write_profiles(output_dir, c['rmw'])
        identity = dict(proof)
        from r2f.instrumentation.sedp import PassiveSedpObserver, build_capture_spec
        spec = build_capture_spec(run_id=c['run_id'], domain_id=c['domain_id'], netns_identity=identity,
                                  output_dir=output_dir / 'discovery-observer',
                                  limits={'timeout_sec': 240, 'max_bytes': 32 * 1024 * 1024, 'snaplen': 65535})
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
                                     profiles=profile_records, observer_ready=ready, implementation=IMPLEMENTATION)
        pool = OwnerCheckedPool(c, dict(os.environ), output_dir, emit, owner_channel=owner_channel, role_environments=role_env)
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
                if not token.get('endpoint_gid') or token['rmw_identifier'] != c['rmw']:
                    raise RuntimeError('endpoint_identity_incomplete:' + role)
            if endpoints['writer']['pid'] == endpoints['reader']['pid']:
                raise RuntimeError('independent_worker_processes_required')
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
        deadline = time.monotonic() + c['drop_duration_sec']
        try:
            while time.monotonic() < deadline:
                if active:
                    counter = carrier.counters()
                    counter_hit = counter_hit or counter['hit']
                    emit('discovery_drop_counter', receipt=counter)
                pause(min(c['counter_interval_sec'], max(0, deadline - time.monotonic())))
            during = activity(drop_wall_ns, 'discovery_interruption')
            drop_activity_ok = activity_is_pair_observed(during['activity'])
        finally:
            if active:
                restore = carrier.restore()
                restored = restore['restored']
                counter_hit = counter_hit or restore.get('hit', False)
                emit('discovery_drop_end', receipt=restore)
        receipt['hit'] = bool(active and counter_hit and drop_activity_ok)
        emit('window_close', window='discovery_interruption', active_drop=active, mutation_receipt=dict(receipt))
        # Paired clocks are captured AFTER verified deletion, or the explicit neutral boundary.
        restore_wall_ns, restore_mono_ns = time.time_ns(), time.monotonic_ns()
        boundary = emit('discovery_restore_boundary', restored=restored if active else None, neutral=not active,
                        drop_was_installed=active, drop_installed=False, post_restore_unix_ns=restore_wall_ns,
                        post_restore_monotonic_ns=restore_mono_ns, clock_guard_ns=CLOCK_GUARD_NS,
                        activity_since_unix_ns=restore_wall_ns + CLOCK_GUARD_NS)
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
        expected = list(range(c['pre_match_count'], c['pre_match_count'] + c['post_match_count']))
        omitted = expected[len(expected) // 2] if c['fault_id'] else None
        emit('delivery_contract', expected_sequences=expected, sentinel_sequence=expected[-1], phase='post_restore',
             fault_id=c['fault_id'], contract_includes_harness_omission=True, prejoin_in_delivery_contract=False, qos=c['qos'])
        emit('window_open', window='post_restore_delivery', expected_sequences=expected)
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
        emit('window_close', window='post_restore_delivery', deadline_expired=timed_out)
        try:
            step('observe_final_graph', ['verify_namespace_bindings'], lambda: snapshot_pair(restore_mono_ns), observation=True)
        except (TimeoutError, RuntimeError) as exc:
            fresh_recovery = False
            emit('discovery_observation_unknown', reason=str(exc))
        if c['fault_id']:
            omissions = [e for e in events if e['event'] == 'harness_omission' and e.get('send_sequence') == omitted]
            emit('fault_control_receipt', fault_id=c['fault_id'], omitted_sequence=omitted, applied=len(omissions) == 1,
                 omission_event_ref=omissions[0]['raw_event_ref'] if omissions else None,
                 responsibility_layer='harness', candidate_eligible=False)
        result.update(runtime_status='completed', reason=None)
    except Exception as exc:
        result.update(runtime_status='failed', reason=type(exc).__name__ + ':' + str(exc))
        emit('runtime_abort', reason=result['reason'], error_receipt=getattr(exc, 'receipt', None))
    finally:
        if carrier is not None:
            try:
                cleanup = carrier.restore()
                emit('discovery_cleanup_restore', receipt=cleanup)
            except Exception as exc:
                result.update(runtime_status='failed', reason='discovery_restore_cleanup_failed:' + str(exc))
                emit('runtime_abort', reason=result['reason'], error_receipt=getattr(exc, 'receipt', None))
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
        result['facts'] = summarize_discovery(events, active=receipt['applied'], counter_hit=counter_hit,
            restored=restored, activity_ok=activity_ok, fresh_recovery=fresh_recovery, drop_activity_ok=drop_activity_ok)
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
