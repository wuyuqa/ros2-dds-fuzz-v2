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
FUZZ_TARGET = 'builtin.discovery_projection_reconciliation.fuzz_v1'
INFINITE_NS = 9223372036854775807
DEFAULT_QOS = {'reliability': 'reliable', 'durability': 'volatile', 'depth': 64,
               'deadline_ns': INFINITE_NS, 'liveliness': 'automatic',
               'liveliness_lease_duration_ns': INFINITE_NS}
DEFAULT_PROJECTION_ROLES = ('writer', 'reader')
_ROLE_PATTERN = re.compile(r'(writer|reader)([2-4])?')


def projection_role_kind(role):
    """Return the publisher/subscription family for a unique role alias."""
    match = _ROLE_PATTERN.fullmatch(role) if isinstance(role, str) else None
    return match.group(1) if match else None


def validate_projection_roles(roles):
    """Validate the explicit multi-endpoint role list without inventing roles."""
    if not isinstance(roles, list):
        raise ValueError('invalid_projection_roles: expected a list')
    if not 2 <= len(roles) <= 8:
        raise ValueError('invalid_projection_roles: count must be in 2..8')
    seen = set()
    for role in roles:
        if not isinstance(role, str) or projection_role_kind(role) is None:
            raise ValueError('invalid_projection_roles: unsupported role ' + repr(role))
        if role in seen:
            raise ValueError('invalid_projection_roles: duplicate role ' + role)
        seen.add(role)
    if not any(projection_role_kind(role) == 'writer' for role in roles):
        raise ValueError('invalid_projection_roles: at least one writer is required')
    if not any(projection_role_kind(role) == 'reader' for role in roles):
        raise ValueError('invalid_projection_roles: at least one reader is required')
    return tuple(roles)


def projection_roles(config):
    """Use the legacy pair only when the key is absent.

    An explicit ``projection_roles`` list always selects the generalized path,
    including the two-role spelling, so an operator can audit the exact shape.
    """
    if config.get('projection_roles') is None:
        return DEFAULT_PROJECTION_ROLES
    return validate_projection_roles(config['projection_roles'])


def projection_has_explicit_roles(config):
    return config.get('projection_roles') is not None or config.get('projection_role_qos') is not None


def is_writer_role(role):
    return projection_role_kind(role) == 'writer'


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
    roles = projection_roles(c)
    explicit_roles = projection_has_explicit_roles(c)
    if 'projection_role_qos' in c and c['projection_role_qos'] is not None:
        overrides = c['projection_role_qos']
        if not isinstance(overrides, dict):
            raise ValueError('invalid_projection_role_qos: expected a mapping')
        unknown = set(overrides) - set(roles)
        if unknown:
            raise ValueError('invalid_projection_role_qos: unknown role '
                             + ','.join(sorted(str(role) for role in unknown)))
        for role, value in overrides.items():
            if not isinstance(value, dict):
                raise ValueError('invalid_projection_role_qos:' + role + ': expected a mapping')
    if explicit_roles:
        # Base role profiles remain the deterministic template.  A per-role
        # override is a real matching-diversity input, not a hidden default.
        for role in roles:
            kind = projection_role_kind(role)
            merged = {**c.get(kind + '_qos', c['qos']),
                      **((c.get('projection_role_qos') or {}).get(role, {}))}
            c[role + '_qos'] = validate_qos(merged)
    else:
        for role in DEFAULT_PROJECTION_ROLES:
            c[role + '_qos'] = validate_qos(c[role + '_qos'])
    c['baseline_qos'] = validate_qos(c.get('baseline_qos', c['mutation_parameters'].get('baseline_qos', DEFAULT_QOS)))
    submitted = c['mutation_parameters'].get('submitted_qos')
    if submitted is not None and validate_qos(submitted) != c['qos']:
        raise ValueError('projection_submitted_qos_mismatch')
    if c['target_id'] == FUZZ_TARGET:
        # Fuzz target: the submitted input is the single source of truth. Every
        # role must use a public profile derived from it; controls are checked
        # against their own neutral profile rather than the mutated one.
        submitted = c.get('submitted_parameters')
        if c['arm'] == 'mutated' and (not isinstance(submitted, dict) or not any(
                isinstance(key, str) and key.startswith('qos.') for key in submitted)):
            raise ValueError('projection_fuzz_requires_submitted_qos_parameters')
        if c['arm'] != 'mutated':
            if any(c[role + '_qos'] != c['qos'] for role in roles):
                raise ValueError('projection_fuzz_control_requires_submitted_qos')
    else:
        if c['arm'] == 'mutated' and all(c[role + '_qos'] == c['baseline_qos'] for role in roles):
            raise ValueError('projection_variant_does_not_change_qos')
        if c['arm'] != 'mutated' and any(c[role + '_qos'] != c['baseline_qos'] for role in roles):
            raise ValueError('projection_control_requires_neutral_qos')
    timeout = c.get('projection_timeout_sec', 8.)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 30:
        raise ValueError('invalid_projection_timeout_sec')
    c.update(projection_timeout_sec=float(timeout), observe_extended_qos=True,
             # Supervisor budget fields only; no discovery interruption in this target.
             drop_duration_sec=0., recovery_timeout_sec=float(timeout))
    windows = c.get('environment_windows')
    if windows:
        from .discovery_network import validate_window_plan
        validate_window_plan(windows)
        raise ValueError('projection_target_does_not_apply_discovery_windows')
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


def _write_extra_profiles(out, roles, rmw, profiles, role_env):
    """Add only the explicitly requested extra roles; never rewrite pair files.

    The fake-profile seam used by offline tests returns empty mappings and
    deliberately does not create a profiles directory. In that case there is
    nothing to extend; real execution always has the pair records.
    """
    from .discovery_profiles import CYCLONE_XML, FAST_XML
    if not profiles:
        return profiles, role_env
    root = Path(out) / 'profiles'
    key = 'CYCLONEDDS_URI' if rmw == 'rmw_cyclonedds_cpp' else 'FASTRTPS_DEFAULT_PROFILES_FILE'
    for index, role in enumerate(roles):
        if role in profiles:
            continue
        if rmw == 'rmw_cyclonedds_cpp':
            text = CYCLONE_XML
        else:
            text = FAST_XML.format(role=role, index=index)
        path = root / (role + '.xml')
        with path.open('x') as handle:
            handle.write(text)
        profiles[role] = {'path': str(path), 'sha256': hashlib.sha256(text.encode()).hexdigest(),
                          'kind': 'explicit_loopback_udp_profile',
                          'participant_index': 'auto' if rmw == 'rmw_cyclonedds_cpp' else index}
        role_env[role] = {key: str(path)}
    return profiles, role_env


def _query_acknowledged_graphs(pool, events, roles, timeout, emit=None):
    """Multi-role form of the pair query; one unique chain per role."""
    requests = {}
    for role in roles:
        requested_ns = time.monotonic_ns()
        command_id = pool.command(role, 'snapshot')
        requests[role] = (command_id, requested_ns)
        if emit is not None:
            emit('graph_snapshot_requested', role=role, command_id=command_id,
                 requested_monotonic_ns=requested_ns)

    def completed():
        proof = {}
        for role, (command_id, requested_ns) in requests.items():
            responses = {kind: [] for kind in ('match_snapshot', 'graph_snapshot', 'command_done')}
            for index, event in enumerate(events):
                kind = event.get('event')
                if (kind in responses and event.get('role') == role
                        and type(event.get('command_id')) is int and event['command_id'] == command_id):
                    responses[kind].append((index, event))
            if any(len(items) != 1 for items in responses.values()):
                continue
            si, snapshot = responses['match_snapshot'][0]
            gi, graph = responses['graph_snapshot'][0]
            ai, ack = responses['command_done'][0]
            stamps = [snapshot.get('sampled_monotonic_ns'), snapshot.get('monotonic_ns'),
                      graph.get('monotonic_ns'), ack.get('monotonic_ns')]
            if (all(type(stamp) is int for stamp in stamps)
                    and requested_ns <= stamps[0] <= stamps[1] <= stamps[2] <= stamps[3]
                    and si < gi < ai):
                proof[role] = {'snapshot': snapshot, 'graph': graph, 'ack': ack}
        return proof

    pool.wait(lambda: len(completed()) == len(roles), timeout, 'acknowledged_graph_query')
    return completed()


def _graph_violations(event, writer_gids, reader_gids):
    reasons = []
    for key, expected in (('publishers', set(writer_gids.values())),
                          ('subscriptions', set(reader_gids.values()))):
        observed = [item.get('endpoint_gid') for item in event.get(key, [])]
        for gid in sorted(expected - set(observed)):
            reasons.append('projection_graph_missing_endpoint:' + gid)
        for gid in sorted(set(observed) - expected, key=str):
            reasons.append('projection_graph_extra_endpoint:' + str(gid))
        if len(observed) != len(set(observed)):
            reasons.append('projection_graph_duplicate_endpoint:' + key)
        if len(observed) != len(expected) and not reasons:
            reasons.append('projection_graph_endpoint_count_mismatch:' + key)
    return reasons


def export_graph(proof, endpoints, topic, path, *, strict=True, include_schema_check=False):
    """Export one actual public graph projection for every configured role.

    ``strict`` retains the historical pair contract (raise on disagreement).
    The multi-endpoint path sets it false: a missing or extra endpoint is a S1
    projection fact to be judged, not a reason to discard the observation.
    """
    # CP1 opt-in artifacts are offline schema material until the adapter contract
    # explicitly supports them; keep the canonical raw reproduction guard intact.
    roles = tuple(endpoints)
    if not roles or set(proof) != set(roles):
        raise RuntimeError('projection_graph_role_set_differs')
    writer_gids = {role: endpoints[role]['endpoint_gid'] for role in roles if is_writer_role(role)}
    reader_gids = {role: endpoints[role]['endpoint_gid'] for role in roles if not is_writer_role(role)}
    graph = proof[roles[0]]['graph']
    violations, missing = [], []
    for role in roles:
        found = _graph_violations(proof[role]['graph'], writer_gids, reader_gids)
        violations.extend(found)
        missing.extend(item for item in found if item.startswith('projection_graph_missing_endpoint:'))
    violations = sorted(set(violations))
    if violations and strict:
        # Preserve the historical pair error token, then make the exact gap
        # available in the message for the generalized path.
        raise RuntimeError('projection_public_graph_pair_not_complete:' + violations[0])
    by_gid = {gid: role for role, gid in {**writer_gids, **reader_gids}.items()}
    entries = []
    for key in ('publishers', 'subscriptions'):
        for item in graph.get(key, []):
            gid = item.get('endpoint_gid')
            entries.append({'endpoint_gid': gid, 'role': by_gid.get(gid, 'unknown'), 'topic': topic,
                            'topic_type': item.get('topic_type'), 'qos': deepcopy(item.get('qos', {}))})
    if strict and (len(entries) != len(roles)
                   or {entry['endpoint_gid'] for entry in entries}
                   != set(writer_gids.values()) | set(reader_gids.values())):
        raise RuntimeError('projection_graph_ambiguous_endpoints')
    result = {'source': 'ros_graph', 'captured_at_unix_ns': graph['time_ns'], 'endpoints': entries}
    if include_schema_check:
        from r2f.instrumentation.sedp_fields import discovery_layer_schema
        snapshot_times = {role: proof[role]['graph']['time_ns'] for role in roles}
        valid_times = all(isinstance(t, int) and not isinstance(t, bool)
                          for t in snapshot_times.values())
        compatible_clocks = all(proof[role]['graph'].get('timestamp_clock') == 'unix_ns_realtime'
                                for role in roles)
        qualified = valid_times and compatible_clocks
        result.update(schema_layers=discovery_layer_schema(),
                      observer_timing_status='qualified' if qualified else 'unknown',
                      observer_timing_reasons=([] if qualified else ['observer_clock_or_timestamp_unverified']),
                      observer_snapshot_times_unix_ns=snapshot_times,
                      observer_snapshot_spread_ns=(max(snapshot_times.values()) - min(snapshot_times.values())
                                                   if qualified else None))
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


def projection_live_budget(pool, deadline_ns):
    """Check owner and workers even when a wait predicate succeeds immediately."""
    if time.monotonic_ns() >= deadline_ns:
        raise TimeoutError('projection_deadline_exceeded')
    pool.pump(0)
    failed = [(role, worker.returncode) for role, worker in pool.workers.items() if worker.poll() is not None]
    if failed:
        raise RuntimeError(f'projection_worker_exited:{failed}')
    remaining = (deadline_ns - time.monotonic_ns()) / 10**9
    if remaining <= 0:
        raise TimeoutError('projection_deadline_exceeded')
    return remaining


def projection_witnesses(snapshot, decoded, endpoints, starts, since_ns):
    """Select the latest actual endpoint state; QoS equality is never readiness."""
    from r2f.instrumentation.sedp_fields import BASELINE_FIELDS
    reasons, selected = [], {}
    prefixes = sorted({e['endpoint_gid'][:24].lower() for e in endpoints.values()})
    filtered = snapshot.get('filtered_rtps', {})
    if (snapshot.get('health', {}).get('status') != 'ok'
            or snapshot.get('window_buffer_retained') is not True
            or filtered.get('metadata_complete') is not True or filtered.get('metadata_overflow') != 0
            or filtered.get('target_related') != []
            or filtered.get('completeness') != 'complete_for_known_target_prefixes'
            or snapshot.get('observation_completeness') != 'complete_for_known_target_prefixes'):
        reasons.append('capture_health_metadata_or_coverage_incomplete')
    until = snapshot.get('until_ns')
    if (snapshot.get('clock') != 'unix_ns_realtime' or snapshot.get('since_ns') != since_ns
            or type(until) is not int or until < since_ns
            or snapshot.get('participant_prefixes') != prefixes
            or type(snapshot.get('monotonic_ns')) is not int
            or type(snapshot.get('ready', {}).get('unix_ns')) is not int
            or snapshot['ready']['unix_ns'] > since_ns):
        reasons.append('snapshot_window_or_participant_identity_incomplete')
    if snapshot.get('status') != 'observed' or snapshot.get('all_participants_observed') is not True:
        reasons.append('attributed_activity_incomplete')
    if (decoded.get('independent_source') != 'passive_rtps_wire_prefix'
            or decoded.get('graph_or_config_field_input') is not False or decoded.get('diagnostics') != []
            or decoded.get('raw') != {**snapshot['raw_prefix'], 'linktype': 1, 'timestamp_precision_ns':
                                      decoded.get('raw', {}).get('timestamp_precision_ns')}):
        reasons.append('wire_prefix_decode_incomplete')
    for role, endpoint in endpoints.items():
        guid = endpoint['endpoint_gid'][:32].lower()
        matches = [(i, e) for i, e in enumerate(decoded.get('events', [])) if e.get('endpoint_guid') == guid]
        # Do not discard a later inline-only dispose before choosing the latest state.
        if not matches:
            reasons.append(role + ':exact_endpoint_missing')
            continue
        if any(type(e.get('unix_ns')) is not int or type(e.get('raw_ref', {}).get('packet_index')) is not int
               for _, e in matches):
            reasons.append(role + ':event_metadata_incomplete')
            continue
        _, event = max(matches, key=lambda pair: (pair[1]['unix_ns'], pair[1]['raw_ref']['packet_index'], pair[0]))
        selected[role] = event
        if [e['unix_ns'] for _, e in matches] != sorted(e['unix_ns'] for _, e in matches):
            reasons.append(role + ':endpoint_timestamp_order_inconsistent')
        fields = event.get('fields', {})
        if (event.get('lifecycle') != 'alive' or event.get('role') != projection_role_kind(role)
                or event.get('source_guid_prefix') != guid[:24]
                or event.get('builtin_writer_id') != ('000003c2' if is_writer_role(role) else '000004c2')
                or event.get('parameter_list', {}).get('complete') is not True
                or any(fields.get(name, {}).get('status') != 'observed'
                       or fields.get(name, {}).get('value') is None for name in BASELINE_FIELDS)
                or fields.get('endpoint_guid', {}).get('value') != guid
                or fields.get('endpoint_guid', {}).get('provenance') != 'wire_parameter'
                or fields.get('role', {}).get('value') != projection_role_kind(role)):
            reasons.append(role + ':latest_endpoint_not_live_complete_or_identity_bound')
        if (type(until) is not int or not max(since_ns, starts[role]['time_ns']) <= event['unix_ns'] <= until):
            reasons.append(role + ':endpoint_event_outside_creation_window')
    return selected, reasons


def wait_projection_readiness(observer, pool, endpoints, starts, since_ns, policy, deadline_ns, emit):
    from r2f.instrumentation.sedp import parse_sedp_capture_prefix
    prefixes = sorted({e['endpoint_gid'][:24].lower() for e in endpoints.values()})
    while True:
        projection_live_budget(pool, deadline_ns)
        requested = emit('projection_readiness_requested', since_ns=since_ns,
                         participant_prefixes=prefixes, deadline_monotonic_ns=deadline_ns)
        snapshot = observer.snapshot_activity(since_ns=since_ns, participant_prefixes=prefixes)
        raw = emit('projection_readiness_snapshot', snapshot=snapshot, request_ref=requested['raw_event_ref'])
        projection_live_budget(pool, deadline_ns)
        try:
            decoded = parse_sedp_capture_prefix(snapshot.get('raw_prefix'), field_policy=policy)
        except (OSError, ValueError) as exc:
            emit('projection_readiness_attempt', snapshot_ref=raw['raw_event_ref'], selected_events={},
                 reasons=['wire_prefix_invalid:' + str(exc)], ready=False)
            raise RuntimeError('projection_wire_prefix_invalid:' + str(exc)) from exc
        selected, reasons = projection_witnesses(snapshot, decoded, endpoints, starts, since_ns)
        if not requested['monotonic_ns'] <= snapshot.get('monotonic_ns', -1) <= raw['monotonic_ns']:
            reasons.append('snapshot_request_chronology_incomplete')
        if time.monotonic_ns() >= deadline_ns:
            reasons.append('projection_deadline_exceeded')
        attempt = emit('projection_readiness_attempt', snapshot_ref=raw['raw_event_ref'],
                       wire_raw=decoded['raw'], selected_events=selected, diagnostics=decoded['diagnostics'],
                       reasons=reasons, ready=not reasons)
        projection_live_budget(pool, deadline_ns)
        if not reasons:
            return emit('projection_wire_ready', snapshot_ref=raw['raw_event_ref'],
                        attempt_ref=attempt['raw_event_ref'], raw_prefix=snapshot['raw_prefix'],
                        selected_events=selected, deadline_monotonic_ns=deadline_ns)
        pool.pump(min(.05, projection_live_budget(pool, deadline_ns)))


def validate_projection_graph_chronology(proof, events, readiness, deadline_ns):
    for role, pair in proof.items():
        requests = [e for e in events if e['event'] == 'graph_snapshot_requested' and e['role'] == role
                    and e['command_id'] == pair['graph']['command_id']]
        if (len(requests) != 1 or requests[0]['namespace_event_index'] <= readiness['namespace_event_index']
                or not readiness['monotonic_ns'] <= requests[0]['requested_monotonic_ns']
                <= requests[0]['monotonic_ns'] < deadline_ns
                or any(not requests[0]['requested_monotonic_ns'] <= pair[kind]['monotonic_ns'] < deadline_ns
                       for kind in ('snapshot', 'graph', 'ack'))):
            raise RuntimeError('projection_graph_not_fresh_after_wire_readiness')


def verify_projection_prefix_chain(capture, prefixes, policy):
    """Reopen the inode-bound prefixes and independently validate the final pcap."""
    from r2f.instrumentation.sedp import parse_sedp_capture, parse_sedp_capture_prefix
    final = parse_sedp_capture(capture, field_policy=policy)
    if final['diagnostics']:
        raise RuntimeError('projection_final_wire_diagnostics')
    verified = []
    for prefix in prefixes:
        if (Path(prefix['path']).resolve() != Path(final['raw']['path']).resolve()
                or prefix['size'] > final['raw']['size']):
            raise RuntimeError('projection_prefix_not_in_final_capture')
        parsed = parse_sedp_capture_prefix(prefix, field_policy=policy)
        if parsed['diagnostics']:
            raise RuntimeError('projection_final_prefix_diagnostics')
        verified.append({'raw_prefix': prefix, 'final_raw': final['raw'], 'verified': True})
    return verified


def namespace_body(request, owner_channel):
    from .namespace_pair import (OwnerCheckedPool, validate_host_leases, execution_chain_identity,
                                 same_source, probe_rtps_ports, write_profiles, CLOCK_GUARD_NS)
    from .ros2_pair import RECEIPT_KEYS, query_acknowledged_graphs
    from r2f.instrumentation.sedp import PassiveSedpObserver, build_capture_spec
    c, out = request['config'], Path(request['output_dir'])
    roles = projection_roles(c)
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
        if roles != DEFAULT_PROJECTION_ROLES:
            profiles, role_env = _write_extra_profiles(out, roles, c['rmw'], profiles, role_env)
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
            submitted_qos={role: c[role + '_qos'] for role in roles})
        emit('window_open', window='endpoint_creation', start_ns=begin['time_ns'])
        pool = OwnerCheckedPool(c, dict(os.environ), out, emit, owner_channel=owner_channel, role_environments=role_env)
        library_refs = {}
        for role in roles:
            pool.start(role)
            pool.wait(lambda role=role: any(e['event'] == 'endpoint_created' and e['role'] == role for e in events),
                      c['startup_timeout_sec'], role + '_created')
            endpoint = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == role)
            starts = [e for e in events if e['event'] == 'endpoint_create_begin' and e['role'] == role]
            if (len(starts) != 1 or endpoint['rmw_identifier'] != c['rmw'] or not endpoint.get('endpoint_gid')
                    or type(starts[0].get('time_ns')) is not int
                    or not re.fullmatch(r'[0-9a-fA-F]{32}(?:[0-9a-fA-F]{16})?', endpoint['endpoint_gid'])
                    or any(starts[0].get(key) != endpoint.get(key) for key in ('pid', 'process_start_ticks', 'endpoint_generation'))
                    or not begin['monotonic_ns'] < starts[0]['monotonic_ns'] <= endpoint['monotonic_ns']
                    or endpoint['qos'] != c[role + '_qos']):
                raise RuntimeError('projection_endpoint_identity_or_chronology_incomplete')
            endpoints[role] = endpoint
            library_refs[role] = capture_libraries(endpoint['pid'], endpoint['process_start_ticks'], out / (role + '-libraries.json'))
        if len({endpoint['pid'] for endpoint in endpoints.values()}) != len(roles):
            raise RuntimeError('projection_requires_independent_worker_processes')
        if c['target_id'] == FUZZ_TARGET:
            observed = {}
            from .ros2_pair import wait_for_qos_binding
            for role in roles:
                # Same pipe race as the endpoint pair: the binding row is emitted
                # by the worker right after endpoint_created and may not be read
                # yet.  Wait bounded; timeout is an observation gap, not a SUT
                # verdict.  GID binding and non-empty profile remain strict.
                row = wait_for_qos_binding(events, pool, role, endpoints[role]['endpoint_gid'],
                                           c['startup_timeout_sec'], emit=emit, exactly_one=True)
                if row.get('requested') != c[role + '_qos']:
                    raise RuntimeError('qos_binding_requested_mismatch:' + role)
                if not row.get('observed_self') or row.get('observed_self_gid') != endpoints[role]['endpoint_gid']:
                    raise RuntimeError('qos_binding_observation_not_bound_to_endpoint:' + role)
                observed[role] = {'requested': row['requested'], 'observed': row['observed_self'],
                                  'observation_api': row.get('observation_api'),
                                  'raw_event_ref': row.get('raw_event_ref') or f"{out / 'raw.jsonl'}#namespace_event={row['namespace_event_index']}"}
            binding_event = emit('qos_binding_verified', endpoints={r: observed[r]['observed'] for r in observed},
                                 requested={r: observed[r]['requested'] for r in observed},
                                 observation_api={r: observed[r]['observation_api'] for r in observed})
            # Compare only fields the submission actually declared.  rclpy
            # reports additional protocol defaults (for example history=keep_last)
            # that were not part of the submitted profile; treating those as a
            # mismatch is a comparison bug, not a SUT finding.  The full observed
            # profile is retained separately for projection reconciliation.
            for role, row in observed.items():
                requested = row['requested']
                observed_self = row['observed']
                # DDS/RMW semantics: duration 0 is the infinite class.  The
                # graph API reports it as INT64_MAX, so compare semantic
                # classes instead of raw integers for these two policies.
                def _same(key, a, b):
                    if key in ('deadline_ns', 'liveliness_lease_duration_ns'):
                        infinite = 9223372036854775807
                        if a in (0, infinite) and b in (0, infinite):
                            return True
                    # FastDDS reports depth=0/history=unknown for some profiles
                    # where the field is not exposed by the graph projection.
                    # That is an observation limitation, not a QoS mismatch.
                    if key in ('depth', 'history') and b in (0, 'unknown', None):
                        return True
                    return a == b
                diff = {key: (requested.get(key), observed_self.get(key))
                        for key in requested
                        if not _same(key, requested.get(key), observed_self.get(key))}
                if diff:
                    raise RuntimeError(
                        'qos_binding_observed_differs_from_submitted:' + role
                        + ':' + ','.join(sorted(diff)))
                row['extra_observed_fields'] = {
                    key: value for key, value in observed_self.items()
                    if key not in requested}
            result['binding_receipts'] = observed
            result['binding_verification_ref'] = binding_event.get('raw_event_ref')
        # A submitted mutation that equals the neutral baseline is a no-op,
        # not a hit.  Record submitted/applied but keep hit false.
        effective_change = bool(c.get('qos') != c.get('baseline_qos'))
        receipt.update(submitted=active, carrier_admitted=active,
                       configured=active, applied=active,
                       hit=bool(active and effective_change),
                       effect_observed=bool(active and effective_change))
        emit('mutation_effect' if (active and effective_change)
             else ('mutation_no_effect' if active else 'neutral_projection_control'),
             effect_id='public_qos_configuration',
             reason=None if effective_change or not active else 'qos_equals_baseline',
             mutation_receipt=dict(receipt), endpoint_refs={r: e['raw_event_ref'] for r, e in endpoints.items()})
        deadline_ns = time.monotonic_ns() + int(c['projection_timeout_sec'] * 10**9)
        vendor = '0110' if c['rmw'] == 'rmw_cyclonedds_cpp' else '010f'
        writer_role = next(role for role in roles if is_writer_role(role))
        policy = {'protocol_defaults': True, 'vendor_versions': {'0110': '0.10.5', '010f': '2.14.7'},
                  'vendor_provenance': {vendor: library_refs[writer_role]}}
        starts = {role: next(e for e in events if e['event'] == 'endpoint_create_begin' and e['role'] == role)
                  for role in roles}
        prefixes = []
        while True:
            readiness = wait_projection_readiness(observer, pool, endpoints, starts, begin['time_ns'],
                                                 policy, deadline_ns, emit)
            prefixes.append(readiness['raw_prefix'])
            remaining = projection_live_budget(pool, deadline_ns)
            if roles == DEFAULT_PROJECTION_ROLES:
                graph_proof = query_acknowledged_graphs(pool, events, min(c['startup_timeout_sec'], remaining), emit)
            else:
                graph_proof = _query_acknowledged_graphs(pool, events, roles,
                                                         min(c['startup_timeout_sec'], remaining), emit)
            projection_live_budget(pool, deadline_ns)
            validate_projection_graph_chronology(graph_proof, events, readiness, deadline_ns)
            try:
                graph = export_graph(graph_proof, endpoints, c['topic'], out / 'public-graph.json',
                                     strict=roles == DEFAULT_PROJECTION_ROLES)
            except RuntimeError as exc:
                emit('projection_graph_retry', reason=str(exc), readiness_ref=readiness['raw_event_ref'])
                pool.pump(min(.05, projection_live_budget(pool, deadline_ns)))
                continue
            projection_live_budget(pool, deadline_ns)
            break
        exported = emit('projection_graph_exported', graph_snapshot=graph, readiness_ref=readiness['raw_event_ref'],
            graph_receipts=[({'role': role, 'command_id': pair['graph']['command_id'],
                'request_namespace_event_index': next(e['namespace_event_index'] for e in events
                    if e['event'] == 'graph_snapshot_requested' and e['role'] == role and e['command_id'] == pair['graph']['command_id']),
                'snapshot_namespace_event_index': pair['graph']['namespace_event_index'],
                'match_namespace_event_index': pair['snapshot']['namespace_event_index'],
                'ack_namespace_event_index': pair['ack']['namespace_event_index']}
                if roles == DEFAULT_PROJECTION_ROLES else
                # Non-default role sets keep the same key names as the default
                # path so one adapter contract reads both; only the role list
                # differs, never the receipt schema.
                {'role': role, 'command_id': pair['graph']['command_id'],
                'request_namespace_event_index': next(e['namespace_event_index'] for e in events
                    if e['event'] == 'graph_snapshot_requested' and e['role'] == role and e['command_id'] == pair['graph']['command_id']),
                'match_namespace_event_index': pair['snapshot']['namespace_event_index'],
                'snapshot_namespace_event_index': pair['graph']['namespace_event_index'],
                'ack_namespace_event_index': pair['ack']['namespace_event_index']})
                for role, pair in graph_proof.items()])
        projection_live_budget(pool, deadline_ns)
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
        chain = verify_projection_prefix_chain(capture, prefixes, policy)
        verified = emit('projection_prefix_chain_verified', prefix_chain=chain)
        observation = emit('projection_observation', capture_receipt=capture, graph_snapshot=graph,
            observation_window=window, field_policy=policy,
            lifecycle_namespace_event_indices={role: {
                'begin': next(e['namespace_event_index'] for e in events if e['event'] == 'endpoint_create_begin' and e['role'] == role),
                'return': endpoints[role]['namespace_event_index']} for role in roles},
            graph_export_ref=exported['raw_event_ref'], readiness_ref=readiness['raw_event_ref'],
            prefix_chain_ref=verified['raw_event_ref'])
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
    observation_roles = [e['role'] for e in events if e.get('event') == 'endpoint_created']
    for role in observation_roles:
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
