"""Derive supporting participant facts from immutable wire/lifecycle artifacts.

These observations never supply the primary recovery gates, create a candidate,
change a required target or turn an observation gap into a SUT failure.
"""
from copy import deepcopy
import re

from r2f.instrumentation.spdp_wire import parse_spdp_capture


def _ownership(event, config):
    value = event.get('participant_ownership') or {}
    guid = value.get('participant_gid')
    identities = value.get('entity_identities', [])
    root = value.get('participant_root_observation') or {}
    handle = value.get('participant_handle')
    if (value.get('schema') != 'r2f.cyclone-participant-ownership/v2'
            or value.get('status') != 'observed' or value.get('complete') is not True
            or value.get('domain_id') != config['domain_id']
            or value.get('local_gid') != event.get('endpoint_gid')
            or value.get('role_kind') != event.get('role')
            or event.get('run_id') != config['run_id'] or event.get('domain_id') != config['domain_id']
            or not event.get('endpoint_generation') or not event.get('process_start_ticks')
            or not isinstance(guid, str) or re.fullmatch('[0-9a-f]{24}000001c1', guid) is None
            or not value.get('provenance') or value.get('stable_sample_count') != 2
            or not type(handle) is int or handle <= 0
            or root.get('participant_handle') != handle or root.get('participant_self_return') != handle
            or handle not in root.get('lookup_participants', [])
            or not event['create_begin_monotonic_ns'] <= value['started_monotonic_ns']
                   <= value['ended_monotonic_ns'] <= event['monotonic_ns']
            or len([row for row in identities if row.get('handle') == value.get('local_handle')
                    and row.get('gid') == event.get('endpoint_gid')]) != 1
            or len([row for row in identities if row.get('handle') == handle and row.get('gid') == guid]) != 1):
        raise ValueError('participant_ownership_unavailable:' + str(event.get('role')))
    return guid


def participant_lease_facts(events, runtime, config, recovery):
    result = {'schema': 'r2f.participant-lease-facts/v1', 'status': 'unknown',
              'semantic_type': 'participant_discovery_lease', 'candidate_eligible': False,
              'expiry_status': 'unknown', 'cleanup_status': 'unknown', 'refresh_status': 'unknown',
              'race_status': 'not_established', 'bindings': [], 'observations': [],
              'receive_incarnations': [], 'reasons': [],
              'recovery': deepcopy(recovery.get('recovery', {})),
              'limitations': ['passive_advertisement_not_remote_lease_expiry',
                             'process_local_ownership_samples_not_atomic',
                             'endpoint_generation_not_remote_receive_incarnation',
                             'vendor_source_binary_equivalence_unknown',
                             'historical_reproduction_not_performed']}
    for role in ('writer', 'reader'):
        endpoints = [e for e in events if e.get('event') == 'endpoint_created' and e.get('role') == role]
        try:
            if len(endpoints) != 1:
                raise ValueError('participant_ownership_unavailable:' + role)
            event = endpoints[0]
            guid = _ownership(event, config)
            result['bindings'].append({k: event[k] for k in
                ('role', 'pid', 'process_start_ticks', 'endpoint_gid', 'endpoint_generation', 'run_id', 'domain_id')})
            result['bindings'][-1].update(participant_guid=guid, endpoint_created_ref=event.get('raw_event_ref'),
                                         ownership=deepcopy(event['participant_ownership']))
        except (KeyError, TypeError, ValueError) as exc:
            result['reasons'].append(str(exc))
    stops = [e for e in events if e.get('event') == 'discovery_observer_stopped']
    if len(stops) == 1:
        stopped = stops[0].get('receipt') or {}
        parsed = parse_spdp_capture({'raw': stopped.get('raw'), 'health': stopped.get('health')})
        result['wire_status'] = parsed['status']
        result['reasons'].extend(parsed['reasons'])
        result['wire_artifact'] = deepcopy(parsed.get('raw'))
        guids = {b['participant_guid'] for b in result['bindings']}
        result['observations'] = [e for e in parsed['observations'] if e.get('participant_guid') in guids]
        for guid in sorted(guids):
            if not any(e.get('participant_guid') == guid and e.get('status') == 'observed'
                       for e in result['observations']):
                result['reasons'].append('participant_advertisement_missing:' + guid)
    else:
        result['reasons'].append('participant_capture_unavailable')
    _lifecycle(result, events, config)
    _external_correlation(result, events, recovery)
    return result


def attach_participant_lease(facts, events, runtime, config):
    if config.get('observe_participant_leases') is True:
        facts['participant_lease'] = participant_lease_facts(events, runtime, config, facts)
    return facts


def _trace(record, config, binding):
    """Reopen the actual captured inode and recheck the live process audit."""
    import hashlib
    from pathlib import Path
    from r2f.instrumentation.participant_lease import parse_lifecycle_trace
    trace_config = record['config']
    path = Path(record['raw_path'])
    before = path.stat()
    if before.st_size > 64 * 1024 * 1024:
        raise ValueError('participant_lifecycle_trace_size_limit')
    data = path.read_bytes()
    after = path.stat()
    processes = record['processes']
    if (not path.is_absolute() or len(processes) != 1
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or hashlib.sha256(data).hexdigest() != record['raw_sha256']
            or trace_config['output_file'] != str(path)
            or trace_config['domain_id'] != config['domain_id']
            or trace_config['sha256'] != hashlib.sha256(trace_config['xml'].encode()).hexdigest()
            or 'trace' not in trace_config['categories']):
        raise ValueError('participant_lifecycle_trace_identity_mismatch')
    process = processes[0]
    if (record['vendor_process_verified'] is not True or process.get('scope') != 'observed_process'
            or process.get('pid') != binding['pid']
            or str(process.get('process_start_ticks')) != str(binding['process_start_ticks'])
            or any(process.get(k) is not True for k in ('process_identity_stable', 'vendor_loaded',
                   'config_matches', 'domain_matches', 'rmw_matches', 'trace_fd_attached'))
            or (process.get('trace_inode'), process.get('trace_device')) != (before.st_ino, before.st_dev)
            or process.get('selected_environment') != {
                'CYCLONEDDS_URI': trace_config['xml'], 'ROS_DOMAIN_ID': str(config['domain_id']),
                'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp'}):
        raise ValueError('participant_lifecycle_process_identity_mismatch')
    libraries = process['loaded_libraries']
    for name in ('/libddsc.so', '/librmw_cyclonedds_cpp.so'):
        matches = [lib for lib in libraries if name in lib.get('mapped_path', '')
                   and lib.get('identity_verified') is True]
        if len(matches) != 1 or hashlib.sha256(Path(matches[0]['path']).read_bytes()).hexdigest() != matches[0]['sha256']:
            raise ValueError('participant_lifecycle_library_identity_mismatch')
    parsed = parse_lifecycle_trace(data, domain_id=config['domain_id'])
    if parsed['diagnostics']:
        raise ValueError('participant_lifecycle_trace_diagnostics')
    return parsed


def _lifecycle(result, events, config):
    """Correlate receive incarnations; preserve original endpoint generations."""
    metadata = [e for e in events if e.get('event') == 'participant_lease_execution_metadata']
    if (len(metadata) != 1 or metadata[0].get('run_id') != config['run_id']
            or metadata[0].get('domain_id') != config['domain_id']
            or metadata[0].get('effective_input_sha256') != config.get('effective_input_sha256')):
        result['reasons'].append('participant_lease_execution_metadata_unavailable')
        return
    mode = config.get('observation_mode', 'blackbox')
    if metadata[0].get('observation_mode') != mode:
        result['reasons'].append('participant_lease_execution_mode_mismatch')
        return
    result['observation_mode'] = mode
    if mode != 'instrumentation':
        result['reasons'].append('internal_participant_lifecycle_not_observed:blackbox')
        return
    traces = []
    for binding in result['bindings']:
        role = binding['role']
        records = [e for e in events if e.get('event') == 'participant_lease_trace_closed'
                   and e.get('worker_role') == role]
        try:
            if len(records) != 1 or records[0].get('run_id') != config['run_id'] or records[0].get('domain_id') != config['domain_id']:
                raise ValueError('participant_lifecycle_trace_unavailable:' + role)
            record = records[0]
            if metadata[0]['trace_configs'].get(role) != record['config']:
                raise ValueError('participant_lifecycle_prepared_trace_mismatch')
            parsed = _trace(record, config, binding)
            remote = [b for b in result['bindings'] if b['role'] != role]
            if len(remote) != 1:
                raise ValueError('participant_lifecycle_remote_ownership_unavailable')
            remote = remote[0]
            selected = [e for e in parsed['events'] if e['participant_guid'] == remote['participant_guid']]
            if not selected:
                raise ValueError('participant_lifecycle_events_missing:' + role)
            incarnation = None
            role_incarnations = []
            for event in selected:
                event['raw_ref'] = {'path': record['raw_path'], 'sha256': record['raw_sha256'],
                                    **{k: event[k] for k in ('offset', 'length', 'line_sha256')}}
                if event['kind'] == 'discovered':
                    if incarnation is not None and incarnation['gc_status'] != 'observed':
                        raise ValueError('participant_receive_incarnation_overlap')
                    incarnation = {'observer_role': role, 'observer_pid': binding['pid'],
                        'observer_process_start_ticks': binding['process_start_ticks'],
                        'participant_guid': remote['participant_guid'],
                        'remote_endpoint_generation': remote['endpoint_generation'],
                        'receive_incarnation': 1 + len(role_incarnations),
                        'events': [], 'refresh_status': 'unknown', 'expiry_status': 'unknown', 'gc_status': 'unknown'}
                    role_incarnations.append(incarnation)
                if incarnation is None:
                    raise ValueError('participant_receive_incarnation_start_missing')
                if incarnation['events'] and event['unix_ns'] < incarnation['events'][-1]['unix_ns']:
                    raise ValueError('participant_lifecycle_clock_regressed')
                if event['kind'] == 'refreshed':
                    if incarnation['expiry_status'] == 'observed' or incarnation['gc_status'] == 'observed':
                        raise ValueError('participant_refresh_after_retirement')
                    incarnation['refresh_status'] = 'observed'
                elif event['kind'] == 'expired':
                    if incarnation['gc_status'] == 'observed':
                        raise ValueError('participant_expiry_after_retirement')
                    incarnation['expiry_status'] = 'observed'
                elif event['kind'] == 'gc_deleted':
                    incarnation['gc_status'] = 'observed'
                incarnation['events'].append(event)
            result['receive_incarnations'].extend(role_incarnations)
            traces.append({'observer_role': role, 'remote_participant_guid': remote['participant_guid'],
                           'events': selected, 'raw_path': record['raw_path'], 'raw_sha256': record['raw_sha256'],
                           'source_references': parsed['source_references']})
        except (KeyError, TypeError, ValueError, OSError) as exc:
            result['reasons'].append(str(exc))
    result['internal_traces'] = traces
    if len(traces) != 2:
        result['reasons'].append('internal_participant_lifecycle_incomplete')
        return
    for name, status in (('refreshed', 'refresh_status'), ('expired', 'expiry_status'), ('gc_deleted', 'cleanup_status')):
        if all(any(e['kind'] == name for e in t['events']) for t in traces):
            result[status] = 'observed'
    result['status'] = 'observed' if not result['reasons'] else 'unknown'
    result['internal_lifecycle_status'] = 'observed'


def replay_lease_selection(capsule):
    """Restore only the observer mode, bound to original execution and raw bytes."""
    import json
    from pathlib import Path
    from r2f.core.evidence import verify_raw
    execution = capsule.execution or {}
    if not execution:
        return {}
    metadata = execution.get('runtime_execution_metadata') or {}
    verify_raw(execution['raw_path'], execution['raw_sha256'])
    events = [json.loads(line) for line in Path(execution['raw_path']).read_text().splitlines() if line.strip()]
    records = [e for e in events if e.get('event') == 'participant_lease_execution_metadata']
    mode = execution.get('observation_mode', metadata.get('observation_mode'))
    if not records and mode is None:
        return {}  # Legacy wire-only opt-in, before lifecycle observation existed.
    if len(records) != 1 or mode not in ('blackbox', 'instrumentation'):
        raise ValueError('participant_lease_replay_selection_unavailable')
    raw = records[0]
    if raw.get('observation_mode') != mode or metadata.get('observation_mode') != mode:
        raise ValueError('participant_lease_replay_mode_mismatch')
    for key in ('run_id', 'domain_id', 'target_id', 'target_revision', 'arm', 'repeat'):
        if raw.get(key) != metadata.get(key) or raw.get(key) != execution.get(key):
            raise ValueError('participant_lease_replay_identity_mismatch:' + key)
    if raw.get('effective_input_sha256') != capsule.input['effective_input_sha256']:
        raise ValueError('participant_lease_replay_input_mismatch')
    return {'observation_mode': mode}


def _external_correlation(result, events, recovery):
    """Link sampled public state to each validated receive incarnation.

    The state is discrete, not continuous. A post-expiry empty graph corroborates
    public removal; only the vendor GC event witnesses internal collection.
    """
    rows = []
    for incarnation in result['receive_incarnations']:
        local = next(b for b in result['bindings'] if b['role'] == incarnation['observer_role'])
        remote = next(b for b in result['bindings'] if b['role'] != incarnation['observer_role'])
        expiry = next((e for e in incarnation['events'] if e['kind'] == 'expired'), None)
        end = next((e for e in incarnation['events'] if e['kind'] == 'gc_deleted'), None)
        down = []
        if expiry and end:
            later = [i['events'][0]['unix_ns'] for i in result['receive_incarnations']
                     if i['observer_role'] == incarnation['observer_role']
                     and i['receive_incarnation'] > incarnation['receive_incarnation']]
            until = min(later) if later else float('inf')
            for graph in events:
                if (graph.get('event') != 'graph_snapshot' or graph.get('role') != local['role']
                        or graph.get('pid') != local['pid'] or graph.get('endpoint_generation') != local['endpoint_generation']
                        or not expiry['unix_ns'] <= graph.get('time_ns', 0) < until):
                    continue
                remote_key = 'subscriptions' if local['role'] == 'writer' else 'publishers'
                local_key = 'publishers' if local['role'] == 'writer' else 'subscriptions'
                proof = _public_removal_sample(events, graph, local, expiry['unix_ns'], until)
                if (proof is not None
                        and any(e.get('endpoint_gid') == local['endpoint_gid'] for e in graph.get(local_key, []))
                        and all(e.get('endpoint_gid') != remote['endpoint_gid'] for e in graph.get(remote_key, []))):
                    down.append(proof)
        rows.append({'observer_role': incarnation['observer_role'],
                     'remote_participant_guid': incarnation['participant_guid'],
                     'remote_endpoint_generation': incarnation['remote_endpoint_generation'],
                     'receive_incarnation': incarnation['receive_incarnation'],
                     'public_removal_status': 'observed' if down else 'unknown',
                     'post_expiry_public_samples': down})
    result['external_correlation'] = rows
    result['recovery']['primary_evidence_valid'] = (not recovery.get('evidence_error')
        and recovery.get('endpoint_identity_complete') is True
        and recovery.get('recovery', {}).get('fresh_match_and_graph') is True)
    result['recovery']['delivery_status'] = 'observed' if (result['recovery']['primary_evidence_valid']
        and recovery.get('sent') and recovery.get('received')) else 'unknown'
    boundaries = [e for e in events if e.get('event') == 'discovery_restore_boundary']
    if len(boundaries) == 1:
        boundary = boundaries[0]
        result['recovery']['restore_boundary_ref'] = boundary.get('raw_event_ref')
        after = boundary['time_ns']
        traces = result.get('internal_traces', [])
        result['recovery']['post_restore_internal_rediscovery_status'] = ('observed' if len(traces) == 2
            and all(any(e['kind'] == 'discovered' and e['unix_ns'] > after for e in t['events']) for t in traces)
            else 'unknown')
    result['limitations'].append('public_graph_count_samples_not_continuous')


def _public_removal_sample(events, graph, local, since, until):
    cid, role = graph.get('command_id'), local['role']
    if type(cid) is not int:
        return None
    found = {}
    for kind in ('match_snapshot', 'graph_snapshot', 'command_done'):
        matches = [e for e in events if e.get('event') == kind
                   and e.get('role') == role and e.get('command_id') == cid]
        if len(matches) != 1:
            return None
        event = matches[0]
        if (any(event.get(k) != local[k] for k in
                ('pid', 'process_start_ticks', 'endpoint_generation', 'run_id', 'domain_id'))
                or not since + 50_000_000 <= event.get('time_ns', 0) < until):
            return None
        found[kind] = event
    snapshot, ack = found['match_snapshot'], found['command_done']
    requests = [e for e in events if e.get('command_id') == cid and (
        e.get('event') == 'match_snapshot_requested' and e.get('role') == role or
        e.get('event') == 'discovery_match_trajectory_requested' and e.get('worker_role') == role)]
    if len(requests) != 1:
        return None
    request = requests[0]
    if request['event'] == 'discovery_match_trajectory_requested':
        identity = request.get('worker_identity') or {}
        if any(identity.get(k) != local[k] for k in
               ('pid', 'process_start_ticks', 'endpoint_generation', 'run_id', 'domain_id')):
            return None
    stamps = [request.get('requested_monotonic_ns'), snapshot.get('sampled_monotonic_ns'),
              snapshot.get('monotonic_ns'), graph.get('monotonic_ns'), ack.get('monotonic_ns')]
    positions = [events.index(e) for e in (request, snapshot, graph, ack)]
    if (any(type(t) is not int for t in stamps) or stamps != sorted(stamps)
            or any(a >= b for a, b in zip(positions, positions[1:]))
            or snapshot.get('endpoint_gid') != local['endpoint_gid']
            or snapshot.get('count_api') != ('get_subscription_count' if role == 'writer' else 'get_publisher_count')
            or type(snapshot.get('matched_count')) is not int or snapshot['matched_count'] != 0
            or ack['monotonic_ns'] >= request.get('deadline_monotonic_ns', float('inf'))):
        return None
    # Vendor expiry uses wall time; count acquisition uses monotonic time.
    # Translate using the worker's paired emission clocks, keeping a guard and
    # rejecting clock movement across the response rather than trusting delay.
    sampled_wall = snapshot['time_ns'] - (snapshot['monotonic_ns'] - snapshot['sampled_monotonic_ns'])
    if (not since + 50_000_000 <= sampled_wall < until
            or any(abs((e['time_ns'] - snapshot['time_ns'])
                       - (e['monotonic_ns'] - snapshot['monotonic_ns'])) > 50_000_000
                   for e in (graph, ack))):
        return None
    return {'request_ref': request.get('raw_event_ref'), 'graph_ref': graph.get('raw_event_ref'),
            'match_ref': snapshot.get('raw_event_ref'), 'ack_ref': ack.get('raw_event_ref')}
