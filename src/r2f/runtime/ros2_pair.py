"""Real public-API ROS2 pair, isolated into two independently initialized processes.

Contract: run_pair(config: dict, output_dir: pathlib.Path) -> dict.
No fixture path exists. Failed preflight has real_execution=False, execution=None.
The runtime records facts, not oracle verdicts or campaign lifecycle decisions.
"""
from __future__ import annotations
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import re
import time
import uuid

from .domains import IsolationError, LeaseSet
from .environment import REPO, RMW_NAMES, activated_environment, check_environment, source_identity
from .process import WorkerPool

RECEIPT_KEYS = ('planned', 'submitted', 'carrier_admitted', 'configured', 'applied', 'hit', 'effect_observed')


def normalize_config(config):
    if not isinstance(config, dict):
        raise ValueError('config must be a dictionary')
    if any(k in config for k in ('fixture', 'fake_events', 'offline_events', 'events')):
        raise ValueError('offline_fixture_not_supported')
    c = dict(config)
    c.setdefault('run_id', uuid.uuid4().hex)
    c.setdefault('campaign_id', c['run_id'])
    c.setdefault('domain_id', c.get('domain', 173))
    if isinstance(c['domain_id'], bool) or not isinstance(c['domain_id'], int) or not 0 <= c['domain_id'] <= 231:
        raise ValueError('domain_id must be an integer in 0..231')
    if not isinstance(c['run_id'], str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', c['run_id']):
        raise ValueError('run_id must contain 1..128 safe identifier characters')
    c.setdefault('rmw', 'rmw_cyclonedds_cpp')
    if c['rmw'] not in RMW_NAMES:
        raise ValueError('unsupported_rmw:' + str(c['rmw']))
    c.setdefault('target_id', 'discovery.late_endpoint')
    c.setdefault('target_revision', 0)
    c.setdefault('arm', 'mutated')
    if c['arm'] not in ('baseline', 'transparent', 'mutated', 'negative'):
        raise ValueError('invalid_arm')
    c.setdefault('mutation_kind', 'late_joiner')
    c.setdefault('carrier', 'ros2_endpoint')
    c.setdefault('mutation_parameters', {})
    if not isinstance(c['mutation_parameters'], dict):
        raise ValueError('mutation_parameters must be a dict')
    params = c['mutation_parameters']
    c.setdefault('reader_delay_sec', params.get('reader_delay_sec', params.get('delay_sec', 0.15)))
    c.setdefault('baseline_reader_delay_sec', 0.02)
    for key, default in (('match_timeout_sec', 8), ('delivery_timeout_sec', 5), ('startup_timeout_sec', 10),
                         ('publish_interval_sec', 0.03)):
        c.setdefault(key, default)
    for key in ('reader_delay_sec', 'baseline_reader_delay_sec', 'match_timeout_sec', 'delivery_timeout_sec',
                'startup_timeout_sec', 'publish_interval_sec'):
        if isinstance(c[key], bool):
            raise ValueError('invalid_timeout:' + key)
        value = float(c[key])
        if not math.isfinite(value) or not 0 <= value <= 120:
            raise ValueError('invalid_timeout:' + key)
        c[key] = value
    c.setdefault('pre_match_count', c.get('pre_samples', 2))
    c.setdefault('post_match_count', c.get('post_samples', c.get('message_count', 6)))
    for key in ('pre_match_count', 'post_match_count'):
        if isinstance(c[key], bool) or not isinstance(c[key], int) or not 0 <= c[key] <= 10000:
            raise ValueError('invalid_count:' + key)
    if c['post_match_count'] < 3 or c['pre_match_count'] < 1:
        raise ValueError('need at least one prejoin and three postmatch samples')
    default_qos = {'reliability': 'reliable', 'durability': 'volatile', 'depth': 32}
    c['qos'] = {**default_qos, **c.get('qos', {})}
    for role in ('writer', 'reader'):
        c[role + '_qos'] = {**c['qos'], **c.get(role + '_qos', {})}
    for q in (c['qos'], c['writer_qos'], c['reader_qos']):
        if q['reliability'] not in ('reliable', 'best_effort') or q['durability'] not in ('volatile', 'transient_local'):
            raise ValueError('unsupported_qos_policy')
        if isinstance(q['depth'], bool) or not isinstance(q['depth'], int) or not 1 <= q['depth'] <= 10000:
            raise ValueError('invalid_qos_depth')
    c.setdefault('fault_id', 'late_endpoint_delivery_omission' if c['arm'] == 'negative' else None)
    if c['fault_id'] not in (None, 'late_endpoint_delivery_omission'):
        raise ValueError('unsupported_fault_id')
    if c['fault_id'] and c['arm'] != 'negative':
        raise ValueError('fault_requires_negative_arm')
    c.setdefault('topic', '/r2f_' + c['run_id'].replace('-', '_').replace('.', '_'))
    if not re.fullmatch(r'/[A-Za-z_][A-Za-z0-9_/]*', c['topic']):
        raise ValueError('invalid_topic')
    c.setdefault('source_repo', str(REPO))
    c['source_repo'] = str(Path(c['source_repo']).resolve())
    c.setdefault('environment_mode', 'ros_overlay')
    c.setdefault('repeat', 0)
    for key in ('repeat', 'target_revision'):
        if isinstance(c[key], bool) or not isinstance(c[key], int) or c[key] < 0:
            raise ValueError('invalid_integer:' + key)
    return c


def runtime_capabilities():
    """A network adapter advertises separately from the available endpoint path.

    discovery_network.py is independently owned. Integration may provide
    capabilities() -> dict and run_pair(config, output_dir) -> the same contract;
    runtime never substitutes pause-publishing for discovery drop.
    """
    result = {'public_endpoint_pair': True, 'independent_worker_processes': True,
              'transient_local_api': True, 'builtin_independent_projection': False,
              'discovery_network': {'available': False, 'reason': 'adapter_not_integrated'}}
    try:
        module = importlib.import_module('r2f.runtime.discovery_network')
        capability = getattr(module, 'capabilities', None)
        result['discovery_network'] = capability() if callable(capability) else {
            'available': False, 'reason': 'adapter_contract_pending', 'module_present': True}
    except ImportError:
        pass
    return result


def summarize_events(events):
    sends = [e for e in events if e['event'] == 'send' and e.get('phase') == 'post_match']
    recvs = [e for e in events if e['event'] == 'receive' and e.get('phase') == 'post_match']
    omitted = [e['send_sequence'] for e in events if e['event'] == 'harness_omission']
    sent, received = [e['send_sequence'] for e in sends], [e['send_sequence'] for e in recvs]
    contract_events = [e for e in events if e['event'] == 'delivery_contract']
    expected = contract_events[-1]['expected_sequences'] if contract_events else sorted(set(sent + omitted))
    intended_missing = sorted(set(expected) - set(received))
    missing = sorted(set(sent) - set(received))
    sentinel = expected[-1] if expected else None
    sentinel_received = sentinel in received if sentinel is not None else False
    writer = next((e for e in events if e['event'] == 'endpoint_created' and e.get('role') == 'writer'), {})
    reader = next((e for e in events if e['event'] == 'endpoint_created' and e.get('role') == 'reader'), {})
    writer_gid = writer.get('endpoint_gid')
    delivery_tokens = [e for e in events if e['event'] == 'receive']
    identity_complete = bool(writer_gid) and bool(delivery_tokens) and all(e.get('publisher_gid') for e in delivery_tokens)
    identity_consistent = all(e['publisher_gid'] == writer_gid for e in delivery_tokens) if identity_complete else None
    windows_open = [e for e in events if e['event'] == 'window_open' and e.get('window') == 'post_match_delivery']
    windows_close = [e for e in events if e['event'] == 'window_close' and e.get('window') == 'post_match_delivery']
    graph_stable = bool(writer_gid and reader.get('endpoint_gid') and windows_open and windows_close)
    graph_boundaries = {}
    if graph_stable:
        opening, closing = windows_open[-1]['monotonic_ns'], windows_close[-1]['monotonic_ns']
        for role in ('writer', 'reader'):
            graphs = sorted([e for e in events if e['event'] == 'graph_snapshot' and e.get('role') == role], key=lambda e: e['monotonic_ns'])
            before = [e for e in graphs if e['monotonic_ns'] <= opening]
            after = [e for e in graphs if e['monotonic_ns'] >= closing]
            bounded = ([before[-1]] if before else []) + [e for e in graphs if opening < e['monotonic_ns'] < closing] + ([after[0]] if after else [])
            valid = bool(before and after) and all(
                {p['endpoint_gid'] for p in g['publishers']} == {writer_gid}
                and {p['endpoint_gid'] for p in g['subscriptions']} == {reader['endpoint_gid']} for g in bounded)
            graph_stable = graph_stable and valid
            graph_boundaries[role] = {'before_index': before[-1].get('event_index') if before else None,
                                      'after_index': after[0].get('event_index') if after else None, 'stable': valid}
    correlated = bool(graph_stable and delivery_tokens and writer.get('pid') != reader.get('pid')
                      and all(e.get('payload_run_id') == writer.get('run_id') for e in delivery_tokens))
    match_kind = 'match_snapshot' if any(e['event'] == 'match_snapshot' for e in events) else 'match'
    matches = {role: any(e['event'] == match_kind and e.get('role') == role and e.get('matched_count') == 1
                        and (match_kind == 'match' or e.get('sampled_monotonic_ns', -1) >= reader.get('created_monotonic_ns', 0))
                        for e in events) for role in ('writer', 'reader')}
    return {'expected_post_match_sequences': expected, 'post_match_sent_sequences': sent,
            'post_match_received_sequences': received,
            'prejoin_sent_sequences': [e['send_sequence'] for e in events if e['event'] == 'send' and e.get('phase') == 'prejoin'],
            'prejoin_received_sequences': [e['send_sequence'] for e in events if e['event'] == 'receive' and e.get('phase') == 'prejoin'],
            'missing_sequences': missing, 'missing_expected_sequences': intended_missing,
            'harness_omitted_sequences': omitted, 'fault_control_primary_expected': 'fail' if omitted else None,
            'candidate_eligible': not bool(omitted), 'responsibility_layer': 'harness' if omitted else None,
            'sentinel_sequence': sentinel, 'sentinel_received': sentinel_received,
            'sut_missing_with_later_sentinel': [n for n in missing if sentinel_received and n < sentinel],
            'receive_order': received, 'in_order': received == sorted(received),
            'duplicate_sequences': sorted({x for x in received if received.count(x) > 1}),
            'writer_gid': writer_gid, 'reader_gid': reader.get('endpoint_gid'),
            'publisher_gid_evidence_complete': identity_complete,
            'publisher_gid_consistent': identity_consistent,
            'publisher_gid_status': 'verified' if identity_consistent is True else ('mismatch' if identity_consistent is False else 'unavailable_api'),
            'identity_basis': 'message_info_gid' if identity_complete else 'endpoint_correlated',
            'endpoint_identity_correlated': correlated, 'graph_stable_across_window': graph_stable,
            'graph_boundary_receipts': graph_boundaries,
            'identity_limitations': [] if identity_complete else ['Jazzy rclpy callback metadata has no publisher_gid; graph correlation is not direct per-sample DDS writer identity'],
            'writer_pid': writer.get('pid'), 'reader_pid': reader.get('pid'),
            'independent_processes': bool(writer.get('pid') and reader.get('pid') and writer['pid'] != reader['pid']),
            'reader_created_after_writer': bool(writer and reader and reader['created_monotonic_ns'] > writer['created_monotonic_ns']),
            'match_established': all(matches.values()), 'matched_roles': matches,
            'window_complete': any(e['event'] == 'window_close' and e.get('window') == 'post_match_delivery' for e in events),
            'observation_provenance': 'public_rclpy_graph_and_message_info',
            'independent_builtin_projection': False}


def wait_for_fresh_match(pool, events, reader_created_ns, timeout, emit=None):
    """Require command-correlated counts queried after the reader create return.

    Keep count-transition events as historical evidence. This live gate has no
    fallback to those events, including when an old transition says count=1.
    """
    deadline = time.monotonic() + timeout
    while True:
        requests = {}
        for role in ('writer', 'reader'):
            requested_ns = time.monotonic_ns()
            command_id = pool.command(role, 'snapshot')
            requests[role] = (command_id, requested_ns)
            if emit is not None:
                emit('match_snapshot_requested', role=role, command_id=command_id,
                     requested_monotonic_ns=requested_ns, reader_created_monotonic_ns=reader_created_ns)

        def snapshots():
            # A snapshot and its ACK can arrive in separate stdout chunks.
            # Only completed, unambiguous commands can release the live gate.
            responses = {role: {'match_snapshot': [], 'command_done': []} for role in requests}
            for index, event in enumerate(events):
                role, kind = event.get('role'), event.get('event')
                if role not in requests or kind not in responses[role]:
                    continue
                if (type(event.get('command_id')) is int
                        and event['command_id'] == requests[role][0]):
                    responses[role][kind].append((index, event))
            found = {}
            for role, response in responses.items():
                if len(response['match_snapshot']) != 1 or len(response['command_done']) != 1:
                    continue
                snapshot_index, event = response['match_snapshot'][0]
                ack_index, ack = response['command_done'][0]
                sampled_ns = event.get('sampled_monotonic_ns')
                snapshot_ns, ack_ns = event.get('monotonic_ns'), ack.get('monotonic_ns')
                if (all(type(value) is int for value in (sampled_ns, snapshot_ns, ack_ns))
                        and max(reader_created_ns, requests[role][1]) <= sampled_ns <= snapshot_ns <= ack_ns
                        and snapshot_index < ack_index):
                    found[role] = event
            return found

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('fresh_match_snapshot_deadline')
        pool.wait(lambda: len(snapshots()) == 2, remaining, 'fresh_match_snapshot_response')
        proof = snapshots()
        if all(type(e.get('matched_count')) is int and e['matched_count'] == 1 for e in proof.values()):
            return proof
        if time.monotonic() >= deadline:
            raise TimeoutError('fresh_match_snapshot_deadline')


def run_pair(config: dict, output_dir: Path) -> dict:
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {'real_execution': False, 'runtime_status': 'blocked', 'status': 'blocked', 'reason': None,
              'events': [], 'facts': {}, 'environment': {}, 'precondition_receipts': [], 'observation_receipts': [],
              'mutation_receipt': dict.fromkeys(RECEIPT_KEYS, False), 'execution': None,
              'execution_metadata': None, 'raw_path': None, 'raw_sha256': None,
              'capabilities': runtime_capabilities(), 'evidence_purpose': config.get('evidence_purpose', 'runtime_execution')}
    try:
        c = normalize_config(config)
    except (ValueError, TypeError) as exc:
        result['reason'] = 'blocked:invalid_config:' + str(exc)
        return result
    if c['target_id'] == 'discovery.interruption_recovery':
        try:
            adapter = importlib.import_module('r2f.runtime.discovery_network')
            entry = getattr(adapter, 'run_pair', None)
            if callable(entry) and result['capabilities']['discovery_network'].get('available'):
                return entry(c, output_dir)
        except ImportError:
            pass
        result['reason'] = 'blocked:discovery_network_adapter_not_integrated'
        return result
    if c['target_id'] == 'builtin.discovery_projection_reconciliation':
        result['reason'] = 'blocked:independent_builtin_projection_not_available'
        return result
    if c['target_id'] not in ('discovery.late_endpoint', 'discovery.late_join_delivery'):
        result['reason'] = 'blocked:target_not_supported'
        return result
    if c['carrier'] != 'ros2_endpoint' or c['mutation_kind'] != 'late_joiner':
        result['reason'] = 'blocked:endpoint_carrier_not_expressible'
        return result
    # Refuse to overwrite any evidence from an earlier invocation.
    raw_path = output_dir / 'raw.jsonl'
    try:
        raw = raw_path.open('x', buffering=1)
    except FileExistsError:
        result['reason'] = 'blocked:state_directory_already_executed'
        return result
    result['raw_path'] = str(raw_path)
    receipts = result['precondition_receipts']
    observation_receipts = result['observation_receipts']
    receipt = result['mutation_receipt']
    active_mutation = c['arm'] == 'mutated'
    receipt['planned'] = active_mutation
    events = result['events']
    pool = None
    lease = None
    before_source = source_identity()
    started_ns = time.monotonic_ns()

    def emit(event, **fields):
        value = {'event': event, 'time_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
                 'role': 'controller', 'pid': os.getpid(), 'pgid': os.getpgrp(),
                 'run_id': c['run_id'], 'domain_id': c['domain_id'], 'rmw_identifier': None}
        value.update(fields)
        value['event_index'] = len(events)
        events.append(value)
        raw.write(json.dumps(value, sort_keys=True) + '\n')
        if event == 'endpoint_created':
            result['real_execution'] = True

    def has(event, role=None, **fields):
        return any(e['event'] == event and (role is None or e.get('role') == role) and all(e.get(k) == v for k, v in fields.items()) for e in events)

    def step(name, predicate, action, timeout, requirement, *, observation=False):
        record = {'step_id': name, 'requires': requirement, 'before': {'monotonic_ns': time.monotonic_ns(), 'satisfied': bool(predicate())}, 'after': None, 'satisfied': False}
        record['receipt_kind'] = 'observation' if observation else 'precondition'
        (observation_receipts if observation else receipts).append(record)
        emit('state_step_before', **record)
        try:
            action()
            pool.wait(predicate, timeout, name)
            record['satisfied'] = True
        finally:
            record['after'] = {'monotonic_ns': time.monotonic_ns(), 'satisfied': bool(predicate())}
            emit('state_step_after', **record)

    def publish(sequence, phase, omit=False):
        command_id = pool.command('writer', 'publish', send_sequence=sequence, phase=phase, omit=omit)
        pool.wait(lambda: has('command_done', 'writer', command_id=command_id), c['startup_timeout_sec'], 'publish_ack')

    try:
        lock_root = Path(c.get('lock_root', f'/tmp/r2f-runtime-locks-{os.getuid()}'))
        lease = LeaseSet(lock_root, output_dir, c['campaign_id'], c['domain_id'])
        lease.__enter__()
        emit('isolation_locks_acquired', locks=lease.receipts)
        environment = check_environment(c)
        result['environment'] = environment
        (output_dir / 'environment.json').write_text(json.dumps(environment, sort_keys=True, indent=2) + '\n')
        emit('environment_preflight', ok=environment['ok'], reason=environment.get('reason'))
        if not environment['ok']:
            raise IsolationError(environment.get('reason', 'environment_preflight_failed'))
        _, child_environment = activated_environment(c)
        pool = WorkerPool(c, child_environment, output_dir, emit)
        emit('window_open', window='before_reader_match')
        step('writer_created', lambda: has('endpoint_created', 'writer'), lambda: pool.start('writer'),
             c['startup_timeout_sec'], ['environment_ok', 'locks_held'])
        writer_token = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'writer')
        if writer_token['rmw_identifier'] != c['rmw'] or not writer_token.get('endpoint_gid'):
            raise RuntimeError('writer_identity_or_rmw_incomplete')
        step('writer_prejoin_published', lambda: sum(e['event'] == 'send' and e.get('phase') == 'prejoin' for e in events) == c['pre_match_count'],
             lambda: [publish(i, 'prejoin') for i in range(c['pre_match_count'])], c['startup_timeout_sec'], ['writer_created'])
        preconditions_ok = len(receipts) == 2 and all(r['satisfied'] for r in receipts)
        if not preconditions_ok:
            raise RuntimeError('mutation_preconditions_not_reached')
        references = [{'step_id': r['step_id'],
                       'raw_before_index': next(e['event_index'] for e in events if e['event'] == 'state_step_before' and e['step_id'] == r['step_id']),
                       'raw_after_index': next(e['event_index'] for e in events if e['event'] == 'state_step_after' and e['step_id'] == r['step_id'])} for r in receipts]
        snapshot = {'writer_created': True, 'writer_prejoin_published': True, 'reader_created': False,
                    'writer_gid': writer_token['endpoint_gid'],
                    'prejoin_publish_sequences': [e['send_sequence'] for e in events if e['event'] == 'send' and e.get('phase') == 'prejoin']}
        receipt['submitted'] = active_mutation
        receipt['carrier_admitted'] = active_mutation
        receipt['configured'] = active_mutation
        emit('mutation_begin' if active_mutation else 'control_begin',
             mutation_kind=c['mutation_kind'], preconditions_snapshot=snapshot,
             precondition_receipt_refs=references, mutation_receipt=dict(receipt))
        # transparent and baseline execute the same input, including the delay.
        delay = c['reader_delay_sec'] if active_mutation else c['baseline_reader_delay_sec']
        delay_deadline = time.monotonic() + delay
        pool.wait(lambda: time.monotonic() >= delay_deadline, delay + 1, 'reader_creation_delay')
        step('reader_created', lambda: has('endpoint_created', 'reader'), lambda: pool.start('reader'),
             c['startup_timeout_sec'], ['writer_created', 'writer_prejoin_published'], observation=True)
        reader_token = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'reader')
        if reader_token['rmw_identifier'] != c['rmw'] or not reader_token.get('endpoint_gid') or reader_token['pid'] == writer_token['pid']:
            raise RuntimeError('reader_identity_or_process_isolation_incomplete')
        actual_delay = reader_token['created_monotonic_ns'] - writer_token['created_monotonic_ns']
        if actual_delay <= 0:
            raise RuntimeError('reader_not_created_after_writer')
        emit('reader_created_after_writer', writer_gid=writer_token['endpoint_gid'], reader_gid=reader_token['endpoint_gid'],
             delay_ns=actual_delay, configured_delay_sec=delay)
        if active_mutation:
            receipt.update(applied=True, hit=True, effect_observed=True)
            emit('mutation_effect', effect_id='reader_created_after_writer', mutation_receipt=dict(receipt))
        elif c['arm'] == 'transparent':
            emit('transparency_control', declaration={'mutation_kind': c['mutation_kind'], 'parameters': c['mutation_parameters']},
                 applied_input_equal_to_baseline=True, effective_reader_delay_sec=delay, mutation_hit=False)
        fresh_matches = {}
        def observe_fresh_matches():
            fresh_matches.update(wait_for_fresh_match(
                pool, events, reader_token['created_monotonic_ns'], c['match_timeout_sec'], emit))
        step('endpoints_matched', lambda: set(fresh_matches) == {'writer', 'reader'},
             observe_fresh_matches, c['match_timeout_sec'], ['reader_created', 'writer_created'], observation=True)
        # Check both observed endpoint GIDs in both graph projections.
        def graph_expected():
            for role in ('writer', 'reader'):
                snapshots = [e for e in events if e['event'] == 'graph_snapshot' and e.get('role') == role]
                if not snapshots:
                    return False
                snapshot = snapshots[-1]
                if {p['endpoint_gid'] for p in snapshot['publishers']} != {writer_token['endpoint_gid']}:
                    return False
                if {p['endpoint_gid'] for p in snapshot['subscriptions']} != {reader_token['endpoint_gid']}:
                    return False
            return True
        for role in ('writer', 'reader'):
            pool.command(role, 'snapshot')
        step('graph_identity_verified', graph_expected, lambda: None, c['match_timeout_sec'], ['endpoints_matched'], observation=True)
        emit('window_close', window='before_reader_match')
        expected = list(range(c['pre_match_count'], c['pre_match_count'] + c['post_match_count']))
        omitted = expected[len(expected) // 2] if c['fault_id'] else None
        emit('delivery_contract', expected_sequences=expected, sentinel_sequence=expected[-1],
             fault_id=c['fault_id'], contract_includes_harness_omission=True,
             prejoin_in_delivery_contract=False, qos=c['qos'])
        emit('window_open', window='post_match_delivery', expected_sequences=expected)
        for sequence in expected:
            publish(sequence, 'post_match', omit=sequence == omitted)
            pause_deadline = time.monotonic() + c['publish_interval_sec']
            pool.wait(lambda: time.monotonic() >= pause_deadline, c['publish_interval_sec'] + 1, 'publish_interval')
        # A fault omission is intentional absence before publish, not SUT data loss.
        def delivered():
            received = {e['send_sequence'] for e in events if e['event'] == 'receive' and e.get('phase') == 'post_match'}
            return set(expected) - ({omitted} if omitted is not None else set()) <= received
        try:
            pool.wait(delivered, c['delivery_timeout_sec'], 'delivery_deadline')
            delivery_timed_out = False
        except TimeoutError:
            delivery_timed_out = True
        emit('window_close', window='post_match_delivery', deadline_expired=delivery_timed_out,
             intended_missing_includes_omission=True)
        final_graph_commands = [(role, pool.command(role, 'snapshot')) for role in ('writer', 'reader')]
        for role, command_id in final_graph_commands:
            pool.wait(lambda role=role, command_id=command_id: has('command_done', role, command_id=command_id),
                      c['startup_timeout_sec'], 'final_graph_snapshot')
        identity_facts = summarize_events(events)
        emit('identity_observation', identity_basis=identity_facts['identity_basis'], graph_boundary_check=True,
             publisher_gid_status=identity_facts['publisher_gid_status'],
             graph_stable_across_window=identity_facts['graph_stable_across_window'],
             endpoint_identity_correlated=identity_facts['endpoint_identity_correlated'],
             limitations=identity_facts['identity_limitations'])
        if c['fault_id']:
            emit('fault_control_receipt', fault_id=c['fault_id'], omitted_sequence=omitted,
                 applied=has('harness_omission', 'writer', send_sequence=omitted),
                 responsibility_layer='harness', candidate_eligible=False)
        after_source = source_identity()
        if any(before_source[k] != after_source[k] for k in ('head', 'runtime_source_sha256')):
            raise RuntimeError('source_identity_changed_during_execution')
        result.update(runtime_status='completed', status='completed', reason=None)
    except (IsolationError, OSError, RuntimeError, TimeoutError, ValueError) as exc:
        result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                      status='failed' if result['real_execution'] else 'blocked', reason=str(exc))
        emit('runtime_abort', reason=str(exc))
    finally:
        if pool is not None:
            try:
                pool.close()
            except (OSError, RuntimeError, TimeoutError) as exc:
                result.update(runtime_status='failed', status='failed', reason='worker_cleanup_error:' + str(exc))
                emit('runtime_abort', reason=result['reason'])
            # A failed shutdown invalidates process health even if messages arrived.
            if any(m.get('returncode') != 0 for m in pool.metadata) and result['runtime_status'] == 'completed':
                result.update(runtime_status='failed', status='failed', reason='worker_shutdown_failed')
        final_source = source_identity()
        if any(before_source[k] != final_source[k] for k in ('head', 'runtime_source_sha256')):
            result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                          status='failed' if result['real_execution'] else 'blocked',
                          reason='source_identity_changed_during_execution')
            emit('runtime_abort', reason=result['reason'])
        if lease is not None:
            lease.__exit__(None, None, None)
        result['facts'] = summarize_events(events)
        if result['real_execution']:
            result['execution'] = {'real_execution': True, 'run_id': c['run_id'], 'target_id': c['target_id'],
                                   'target_revision': c['target_revision'], 'rmw': c['rmw'], 'domain_id': c['domain_id'],
                                   'arm': c['arm'], 'repeat': c['repeat'], 'runtime_status': result['runtime_status'],
                                   'started_monotonic_ns': started_ns, 'finished_monotonic_ns': time.monotonic_ns(),
                                   'workers': pool.metadata if pool else [], 'source': before_source, 'source_after': final_source,
                                   'precondition_receipts': receipts, 'observation_receipts': observation_receipts, 'mutation_receipt': dict(receipt),
                                   'fault_control': bool(c['fault_id']), 'candidate_eligible': not bool(c['fault_id'])}
            result['execution_metadata'] = result['execution']
        emit('runtime_end', runtime_status=result['runtime_status'], real_execution=result['real_execution'],
             mutation_receipt=dict(receipt), reason=result['reason'])
        raw.flush()
        os.fsync(raw.fileno())
        raw.close()
        result['raw_sha256'] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        if result['execution'] is not None:
            result['execution'].update(raw_path=str(raw_path), raw_sha256=result['raw_sha256'])
            (output_dir / 'execution.json').write_text(json.dumps(result['execution'], sort_keys=True, indent=2) + '\n')
        (output_dir / 'runtime_result.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    return result
