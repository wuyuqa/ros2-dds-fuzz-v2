"""Fourth target: passive wire versus public graph in the supervised pair netns.

This module has no launch entry or campaign implementation. The public run_pair
host owns admission, three leases, serialization and process-tree cleanup.
"""
from __future__ import annotations
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time

TARGET = 'builtin.discovery_projection_reconciliation'
INFINITE_NS = 9223372036854775807
DEFAULT_QOS = {'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
               'deadline_ns': INFINITE_NS, 'liveliness': 'automatic',
               'liveliness_lease_duration_ns': INFINITE_NS}


def validate_qos(qos):
    q = {**DEFAULT_QOS, **qos}
    if set(q) - set(DEFAULT_QOS):
        raise ValueError('unsupported_projection_qos_field')
    for key in ('deadline_ns', 'liveliness_lease_duration_ns'):
        if type(q[key]) is not int or not 0 <= q[key] <= INFINITE_NS:
            raise ValueError('invalid_projection_qos:' + key)
    if q['liveliness'] not in ('automatic', 'manual_by_topic'):
        raise ValueError('unsupported_public_liveliness')
    if q['reliability'] not in ('reliable', 'best_effort') or q['durability'] not in ('volatile', 'transient_local'):
        raise ValueError('unsupported_projection_policy')
    if type(q['depth']) is not int or not 1 <= q['depth'] <= 10000:
        raise ValueError('invalid_projection_depth')
    return q


def normalize_projection_config(config):
    c = deepcopy(config)
    c.setdefault('message_type', 'std_msgs/msg/String')
    if c['message_type'] != 'std_msgs/msg/String':
        raise ValueError('unsupported_projection_message_type')
    c['qos'] = validate_qos(c['qos'])
    for role in ('writer', 'reader'):
        c[role + '_qos'] = validate_qos(c[role + '_qos'])
    c['baseline_qos'] = validate_qos(c.get('baseline_qos', c['mutation_parameters'].get('baseline_qos', DEFAULT_QOS)))
    submitted = c['mutation_parameters'].get('submitted_qos')
    if submitted is not None and validate_qos(submitted) != c['qos']:
        raise ValueError('projection_submitted_qos_mismatch')
    if c['arm'] == 'mutated' and c['writer_qos'] == c['baseline_qos'] and c['reader_qos'] == c['baseline_qos']:
        raise ValueError('projection_variant_does_not_change_qos')
    if c['arm'] != 'mutated' and any(c[role + '_qos'] != c['baseline_qos'] for role in ('writer', 'reader')):
        raise ValueError('projection_control_requires_neutral_qos')
    timeout = c.get('projection_timeout_sec', 8.)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 30:
        raise ValueError('invalid_projection_timeout_sec')
    c.update(projection_timeout_sec=float(timeout), observe_extended_qos=True,
             # Supervisor budget fields only; no discovery interruption in this target.
             drop_duration_sec=0., recovery_timeout_sec=float(timeout))
    return c


def write_artifact(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()
    with Path(path).open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return {'path': str(Path(path).resolve()), 'sha256': hashlib.sha256(data).hexdigest()}


def capture_libraries(pid, start_ticks, path):
    """Hash only mapped DDS/RMW libraries. Never read general process secrets."""
    def start():
        return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19])
    if start() != start_ticks:
        raise RuntimeError('projection_worker_pid_generation_changed')
    libraries, seen = [], set()
    for line in Path(f'/proc/{pid}/maps').read_text().splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or not re.search(r'/lib(?:ddsc|fastrtps|fastdds|rmw_[A-Za-z0-9_]+)\.so', fields[5]):
            continue
        name = fields[5]
        if name in seen:
            continue
        seen.add(name)
        library = Path(name).resolve(strict=True)
        before = library.stat()
        digest = hashlib.sha256(library.read_bytes()).hexdigest()
        after = library.stat()
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or before.st_ino != int(fields[4])
                or f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}' != fields[3]):
            raise RuntimeError('projection_loaded_library_identity_changed')
        libraries.append({'path': str(library), 'sha256': digest, 'mapped_inode': before.st_ino})
    if start() != start_ticks or not libraries:
        raise RuntimeError('projection_loaded_libraries_unverified')
    return write_artifact(path, {'pid': pid, 'process_start_ticks': start_ticks, 'loaded_libraries': libraries})


def export_graph(proof, endpoints, topic, path):
    """One actual public graph projection; both role command chains are retained."""
    expected = {event['endpoint_gid'] for event in endpoints.values()}
    for role in ('writer', 'reader'):
        event = proof[role]['graph']
        if ({e['endpoint_gid'] for e in event['publishers']} != {endpoints['writer']['endpoint_gid']}
                or {e['endpoint_gid'] for e in event['subscriptions']} != {endpoints['reader']['endpoint_gid']}):
            raise RuntimeError('projection_public_graph_pair_not_complete')
    graph = proof['writer']['graph']
    entries = []
    for key, role in (('publishers', 'writer'), ('subscriptions', 'reader')):
        for item in graph[key]:
            entries.append({'endpoint_gid': item['endpoint_gid'], 'role': role, 'topic': topic,
                            'topic_type': item['topic_type'], 'qos': deepcopy(item['qos'])})
    if len(entries) != 2 or {entry['endpoint_gid'] for entry in entries} != expected:
        raise RuntimeError('projection_graph_ambiguous_endpoints')
    result = {'source': 'ros_graph', 'captured_at_unix_ns': graph['time_ns'], 'endpoints': entries}
    result['raw'] = write_artifact(path, result)
    return result


def controlled_graph(graph, capture, path):
    """One explicit harness-only derived field; original graph/wire stay immutable."""
    derived = deepcopy({key: value for key, value in graph.items() if key != 'raw'})
    indexes = [i for i, endpoint in enumerate(derived['endpoints']) if endpoint['role'] == 'writer']
    if len(indexes) != 1:
        raise ValueError('fault_writer_not_unique')
    index = indexes[0]
    before = derived['endpoints'][index]['qos']['reliability']
    if before not in ('reliable', 'best_effort'):
        raise ValueError('fault_field_not_observed')
    after = 'best_effort' if before == 'reliable' else 'reliable'
    derived['endpoints'][index]['qos']['reliability'] = after
    ref = write_artifact(path, derived)
    return {'schema': 'r2f.fault-control-receipt/v1',
            'fault_id': 'controlled_projection_field_mismatch', 'receipt_kind': 'projection_field_mismatch',
            'applied': True, 'responsibility_layer': 'harness', 'candidate_eligible': False,
            'projection_source': 'graph', 'field_path': f'/endpoints/{index}/qos/reliability',
            'before': before, 'after': after, 'controlled_projection_ref': ref['path'],
            'controlled_projection_sha256': ref['sha256'],
            'original_sources': {'builtin': {'raw_ref': capture['raw']['path'], 'raw_sha256': capture['raw']['sha256']},
                                 'graph': {'raw_ref': graph['raw']['path'], 'raw_sha256': graph['raw']['sha256']}}}


def namespace_body(request, owner_channel):
    from .namespace_pair import (OwnerCheckedPool, validate_host_leases, execution_chain_identity,
                                 same_source, probe_rtps_ports, write_profiles, CLOCK_GUARD_NS)
    from .ros2_pair import RECEIPT_KEYS, query_acknowledged_graphs
    from r2f.instrumentation.sedp import PassiveSedpObserver, build_capture_spec
    c, out = request['config'], Path(request['output_dir'])
    events, conditions, endpoints = [], {}, {}
    active = c['arm'] == 'mutated'
    receipt = dict.fromkeys(RECEIPT_KEYS, False)
    receipt['planned'] = active
    result = {'runtime_status': 'failed', 'reason': None, 'facts': {}, 'environment': {}, 'workers': [],
              'precondition_receipts': [], 'observation_receipts': [], 'mutation_receipt': receipt}
    pool = observer = None
    graph = capture = None
    window = None

    def emit(event, **fields):
        token = {'event': event, 'role': 'controller', 'pid': os.getpid(), 'pgid': os.getpgrp(),
                 'run_id': c['run_id'], 'domain_id': c['domain_id'], 'time_ns': time.time_ns(),
                 'monotonic_ns': time.monotonic_ns(), **fields}
        token.update(namespace_event_index=len(events), raw_event_ref=f"{out / 'raw.jsonl'}#namespace_event={len(events)}")
        token = json.loads(json.dumps(token, sort_keys=True))
        events.append(token)
        print(json.dumps(token, sort_keys=True), flush=True)
        return token

    def step(name, requires, action):
        record = {'step_id': name, 'requires': requires, 'receipt_kind': 'precondition',
                  'before': {'monotonic_ns': time.monotonic_ns(), 'satisfied': False},
                  'requires_results': {dep: {'satisfied': dep in conditions, 'evidence_ref': conditions.get(dep)} for dep in requires},
                  'after': None, 'satisfied': False}
        result['precondition_receipts'].append(record)
        before = emit('state_step_before', **record)
        record['before']['raw_event_ref'] = before['raw_event_ref']
        try:
            if not all(dep in conditions for dep in requires):
                raise RuntimeError('projection_precondition_dependency_missing:' + name)
            record['evidence'] = action()
            record['satisfied'] = True
            return record['evidence']
        finally:
            record['after'] = {'monotonic_ns': time.monotonic_ns(), 'satisfied': record['satisfied']}
            after = emit('state_step_after', **record)
            record['after']['raw_event_ref'] = after['raw_event_ref']
            if record['satisfied']:
                conditions[name] = after['raw_event_ref']

    try:
        proof = validate_host_leases(request)
        if not same_source(request['source'], execution_chain_identity()):
            raise RuntimeError('source_identity_changed_before_namespace_execution')
        admission = emit('namespace_host_admission_verified', isolation=proof,
                         lease_paths=[record['path'] for record in request['leases']])
        conditions.update(environment_ok=admission['raw_event_ref'], locks_held=admission['raw_event_ref'])
        def namespace_ready():
            ports = probe_rtps_ports(c['domain_id'])
            if not ports['ok']:
                raise RuntimeError('domain_conflict:namespace_rtps_port_block')
            event = emit('namespace_ready', ready=True, isolation=proof, admission_ref=admission['raw_event_ref'],
                         namespace_rtps_probe=ports, drop_installed=False)
            return {'raw_event_ref': event['raw_event_ref'], 'isolation': proof, 'rtps_probe': ports}
        step('namespace_ready', ['environment_ok', 'locks_held'], namespace_ready)
        profiles, role_env = write_profiles(out, c['rmw'])
        spec = build_capture_spec(out / 'discovery-observer', run_id=c['run_id'], domain_id=c['domain_id'],
                netns_identity=proof, limits={'timeout_sec': 240, 'max_bytes': 32 * 1024 * 1024, 'snaplen': 65535})
        observer = PassiveSedpObserver(spec)
        def observer_ready():
            ready = observer.start()
            event = emit('discovery_observer_ready', receipt=ready, ready=ready)
            if ready.get('status') != 'ready':
                raise RuntimeError('projection_observer_not_ready')
            return {'raw_event_ref': event['raw_event_ref'], 'ready': ready}
        ready = step('observer_ready', ['namespace_ready'], observer_ready)['ready']
        result['environment'].update(isolation=proof, profiles=profiles, observer_ready=ready,
            implementation='isolated_netns_passive_sedp_public_qos', drop_installed=False)
        receipt.update(submitted=active, carrier_admitted=active, configured=active)
        begin = emit('mutation_begin' if active else 'control_begin', mutation_kind=c['mutation_kind'],
            preconditions_snapshot={'namespace_ready': True, 'observer_ready': True, 'endpoints_created': False},
            precondition_receipt_refs=[{'step_id': r['step_id'], 'raw_before_ref': r['before']['raw_event_ref'],
                                      'raw_after_ref': r['after']['raw_event_ref']} for r in result['precondition_receipts']],
            mutation_receipt=dict(receipt), baseline_qos=c['baseline_qos'],
            submitted_qos={role: c[role + '_qos'] for role in ('writer', 'reader')})
        emit('window_open', window='endpoint_creation', start_ns=begin['time_ns'])
        pool = OwnerCheckedPool(c, dict(os.environ), out, emit, owner_channel=owner_channel, role_environments=role_env)
        library_refs = {}
        for role in ('writer', 'reader'):
            pool.start(role)
            pool.wait(lambda role=role: any(e['event'] == 'endpoint_created' and e['role'] == role for e in events),
                      c['startup_timeout_sec'], role + '_created')
            endpoint = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == role)
            starts = [e for e in events if e['event'] == 'endpoint_create_begin' and e['role'] == role]
            if (len(starts) != 1 or endpoint['rmw_identifier'] != c['rmw'] or not endpoint.get('endpoint_gid')
                    or not begin['monotonic_ns'] < starts[0]['monotonic_ns'] <= endpoint['monotonic_ns']
                    or endpoint['qos'] != c[role + '_qos']):
                raise RuntimeError('projection_endpoint_identity_or_chronology_incomplete')
            endpoints[role] = endpoint
            library_refs[role] = capture_libraries(endpoint['pid'], endpoint['process_start_ticks'], out / (role + '-libraries.json'))
        if endpoints['writer']['pid'] == endpoints['reader']['pid']:
            raise RuntimeError('projection_requires_independent_worker_processes')
        receipt.update(applied=active, hit=active, effect_observed=active)
        emit('mutation_effect' if active else 'neutral_projection_control', effect_id='public_qos_configuration',
             mutation_receipt=dict(receipt), endpoint_refs={r: e['raw_event_ref'] for r, e in endpoints.items()})
        deadline = time.monotonic() + c['projection_timeout_sec']
        while True:
            graph_proof = query_acknowledged_graphs(pool, events, c['startup_timeout_sec'], emit)
            try:
                graph = export_graph(graph_proof, endpoints, c['topic'], out / 'public-graph.json')
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                retry_at = time.monotonic() + .1
                pool.wait(lambda: time.monotonic() >= retry_at, .3, 'projection_graph_retry')
        exported = emit('projection_graph_exported', graph_snapshot=graph,
            graph_receipts=[{'role': role, 'command_id': pair['graph']['command_id'],
                'request_namespace_event_index': next(e['namespace_event_index'] for e in events
                    if e['event'] == 'graph_snapshot_requested' and e['role'] == role and e['command_id'] == pair['graph']['command_id']),
                'snapshot_namespace_event_index': pair['graph']['namespace_event_index'],
                'match_namespace_event_index': pair['snapshot']['namespace_event_index'],
                'ack_namespace_event_index': pair['ack']['namespace_event_index']}
                for role, pair in graph_proof.items()])
        window = {'start_ns': begin['time_ns'], 'end_ns': exported['time_ns']}
        end_offset = exported['time_ns'] - exported['monotonic_ns']
        start_offset = begin['time_ns'] - begin['monotonic_ns']
        if abs(end_offset - start_offset) > CLOCK_GUARD_NS:
            raise RuntimeError('projection_clock_discontinuity')
        emit('window_close', window='endpoint_creation', **window,
             clock_guard_ns=CLOCK_GUARD_NS, clock_offset_delta_ns=end_offset - start_offset)
        # Ready-to-graph capture includes initial SEDP; no injection or native observer.
        capture = observer.stop()
        observer = None
        emit('discovery_observer_stopped', receipt=capture)
        if (capture.get('health', {}).get('status') != 'ok' or capture.get('thread_stopped') is not True
                or capture.get('socket_closed') is not True):
            raise RuntimeError('projection_capture_health_incomplete')
        vendor = '0110' if c['rmw'] == 'rmw_cyclonedds_cpp' else '010f'
        policy = {'protocol_defaults': True, 'vendor_versions': {'0110': '0.10.5', '010f': '2.14.7'},
                  'vendor_provenance': {vendor: library_refs['writer']}}
        observation = emit('projection_observation', capture_receipt=capture, graph_snapshot=graph,
            observation_window=window, field_policy=policy,
            lifecycle_namespace_event_indices={role: {
                'begin': next(e['namespace_event_index'] for e in events if e['event'] == 'endpoint_create_begin' and e['role'] == role),
                'return': endpoints[role]['namespace_event_index']} for role in ('writer', 'reader')},
            graph_export_ref=exported['raw_event_ref'])
        if c['fault_id']:
            fault = controlled_graph(graph, capture, out / 'controlled-public-graph.json')
            emit('fault_control_receipt', **fault, projection_observation_ref=observation['raw_event_ref'])
        result['facts'] = {'independent_builtin_projection': True, 'projection_observation_status': 'captured',
                           'candidate_eligible': c['arm'] != 'negative', 'primary_oracle_evaluated': False}
        result.update(runtime_status='completed', reason=None)
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        result.update(runtime_status='failed', reason=str(exc))
        emit('runtime_abort', reason=str(exc))
    finally:
        if observer is not None:
            try:
                capture = observer.stop()
                emit('discovery_observer_stopped', receipt=capture)
            except Exception as exc:
                result.update(runtime_status='failed', reason='projection_observer_cleanup_failed:' + str(exc))
                emit('runtime_abort', reason=result['reason'])
        if pool is not None:
            pool.check_owner = False
            try:
                pool.close()
            except Exception as exc:
                result.update(runtime_status='failed', reason='projection_worker_cleanup_failed:' + str(exc))
                emit('runtime_abort', reason=result['reason'])
            result['workers'] = pool.metadata
            if any(worker.get('returncode') != 0 for worker in pool.metadata):
                result.update(runtime_status='failed', reason='worker_shutdown_failed')
        if not same_source(request['source'], execution_chain_identity()):
            result.update(runtime_status='failed', reason='source_identity_changed_during_execution')
        if not endpoints:
            result['runtime_status'] = 'blocked'
    return result


def finalize_projection(result):
    """Bind lifecycle to the final host JSONL without self-referential raw hashes."""
    events = result['events']
    observations = [e for e in events if e['event'] == 'projection_observation']
    if len(observations) != 1:
        return
    observation = observations[0]
    bindings = []
    for role in ('writer', 'reader'):
        begin = [e for e in events if e['event'] == 'endpoint_create_begin' and e['role'] == role]
        returned = [e for e in events if e['event'] == 'endpoint_created' and e['role'] == role]
        if len(begin) != 1 or len(returned) != 1:
            return
        event = returned[0]
        ref = {'path': result['raw_path'], 'sha256': result['raw_sha256']}
        bindings.append({'endpoint_guid': event['endpoint_gid'][:32], 'role': role,
            'generation': event['endpoint_generation'], 'pid': event['pid'], 'process_start_ticks': event['process_start_ticks'],
            'begin_raw_ref': {**ref, 'event_index': begin[0]['event_index']},
            'return_raw_ref': {**ref, 'event_index': event['event_index']}})
    result['discovery_projection'] = {key: deepcopy(observation[key]) for key in
        ('capture_receipt', 'graph_snapshot', 'observation_window', 'field_policy')}
    result['discovery_projection']['endpoint_bindings'] = bindings
