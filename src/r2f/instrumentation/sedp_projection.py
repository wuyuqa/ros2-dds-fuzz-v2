"""Independent wire/graph equality and separate offered/requested policy facts.

This module does not emit S1 verdicts, candidates or match receipts. Runtime and
S1 oracle retain lifecycle, observation-window and actual-match responsibilities.
"""
from __future__ import annotations
import copy
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import re

from .sedp_fields import BASELINE_FIELDS, semantic_duration
from .sedp_wire import parse_sedp_capture


def _bound_json(ref):
    data = Path(ref['path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != ref['sha256']:
        raise ValueError('raw_hash_mismatch')
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError('raw_JSON_object_required')
    return value


def _fields(endpoint):
    return endpoint.get('fields', endpoint)


def _observed(fields, name):
    field = fields.get(name, {})
    if not isinstance(field, dict) or field.get('status') != 'observed':
        return None
    return field.get('value')


def _enum(value):
    return value.upper() if isinstance(value, str) else value


def check_offered_requested(writer, reader, *, scope=None):
    """QoS compatibility facts only; source values must be observation fields.

    compatible does not imply actual matching. Incompatible with independently
    established positive match is a contradiction for the existing S1 oracle;
    compatible and no match in a finite window remains unknown there.
    """
    writer, reader = _fields(writer), _fields(reader)
    predicates = {}
    ranks = {'reliability': ['BEST_EFFORT', 'RELIABLE'],
             'durability': ['VOLATILE', 'TRANSIENT_LOCAL', 'TRANSIENT', 'PERSISTENT'],
             'liveliness.kind': ['AUTOMATIC', 'MANUAL_BY_PARTICIPANT', 'MANUAL_BY_TOPIC']}
    for name, rank in ranks.items():
        offered, requested = _enum(_observed(writer, name)), _enum(_observed(reader, name))
        value = rank.index(offered) >= rank.index(requested) if offered in rank and requested in rank else None
        predicates[name] = {'compatible': value, 'offered': offered, 'requested': requested,
                            'predicate': 'offered_kind >= requested_kind',
                            'reason': None if value is not None else 'unknown_observed_enum'}
    for name in ('deadline', 'liveliness.lease_duration'):
        offered = semantic_duration(_observed(writer, name))
        requested = semantic_duration(_observed(reader, name))
        value = None
        if offered is not None and requested is not None:
            value = True if requested == 'infinite' else False if offered == 'infinite' else offered <= requested
        predicates[name] = {'compatible': value, 'predicate': 'offered_duration <= requested_duration',
                            'offered': str(offered) if offered is not None else None,
                            'requested': str(requested) if requested is not None else None,
                            'reason': None if value is not None else 'unknown_observed_duration'}
    for name, predicate in predicates.items():
        predicate['offered_observation'] = copy.deepcopy(writer.get(name))
        predicate['requested_observation'] = copy.deepcopy(reader.get(name))
    values = [p['compatible'] for p in predicates.values()]
    compatible = False if False in values else None if None in values else True
    return {'kind': 'offered_requested_compatibility', 'compatible': compatible,
            'predicates': predicates, 'unknown_policies': [n for n, p in predicates.items() if p['compatible'] is None],
            'scope': copy.deepcopy(scope or {'policies': list(predicates), 'other_DDS_matching_conditions': 'not_evaluated'}),
            'actual_match': 'not_evaluated', 'candidate_admission': 'not_performed'}


def _guid(value):
    if isinstance(value, (bytes, bytearray, list, tuple)):
        try:
            value = bytes(value).hex()
        except (ValueError, TypeError):
            return None
    if isinstance(value, str) and re.fullmatch('[0-9a-fA-F]{32}', value):
        return value.lower()
    return None


def _ros_type(value):
    match = re.fullmatch(r'([A-Za-z0-9_]+)::msg::dds_::([A-Za-z0-9_]+)_', value) if isinstance(value, str) else None
    return match.group(1) + '/msg/' + match.group(2) if match else None


def _graph_fields(endpoint):
    qos = endpoint.get('qos', endpoint.get('qos_profile', {}))
    qos = qos if isinstance(qos, dict) else {}
    live = qos.get('liveliness')
    live_kind = live.get('kind') if isinstance(live, dict) else live
    live_lease = live.get('lease_duration') if isinstance(live, dict) else qos.get('liveliness_lease_duration', qos.get('liveliness_lease_ns', qos.get('liveliness_lease_duration_ns')))
    return {'endpoint_guid': _guid(endpoint.get('endpoint_guid', endpoint.get('endpoint_gid'))),
            'role': endpoint.get('role'), 'topic': endpoint.get('topic', endpoint.get('topic_name')),
            'type': endpoint.get('type', endpoint.get('type_name', endpoint.get('topic_type'))),
            'reliability': _enum(qos.get('reliability')), 'durability': _enum(qos.get('durability')),
            'deadline': qos.get('deadline', qos.get('deadline_ns')),
            'liveliness.kind': _enum(live_kind), 'liveliness.lease_duration': live_lease}


def _duration_graph_ns(value):
    if isinstance(value, dict):
        if value.get('infinite') is True:
            return 'infinite'
        value = value.get('nanoseconds')
    if isinstance(value, int) and not isinstance(value, bool):
        if value == 2**63 - 1:  # Jazzy RMW_DURATION_INFINITE, not DDS wire sentinel
            return 'infinite'
        if value >= 0:
            return value
    return None


def _equal(name, wire_value, graph_value):
    if wire_value is None or graph_value is None:
        return None
    if name == 'topic':
        return wire_value == 'rt' + graph_value if isinstance(wire_value, str) and isinstance(graph_value, str) and graph_value.startswith('/') else None
    if name == 'type':
        converted = _ros_type(wire_value)
        return converted == graph_value if converted else None
    if name in ('deadline', 'liveliness.lease_duration'):
        graph_ns = _duration_graph_ns(graph_value)
        if graph_ns is None or not isinstance(wire_value, dict):
            return None
        if wire_value.get('infinite') is True:
            return graph_ns == 'infinite'
        if graph_ns == 'infinite':
            return False
        wire_ns = wire_value.get('nanoseconds')
        if isinstance(wire_ns, int) and not isinstance(wire_ns, bool):
            return wire_ns == graph_ns
        exact = semantic_duration(wire_value)
        return exact == Fraction(graph_ns, 10**9) if isinstance(exact, Fraction) else None
    if name in ('reliability', 'durability', 'liveliness.kind'):
        return _enum(wire_value) == _enum(graph_value)
    return wire_value == graph_value


def _positive_int(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _lifecycle_event(ref):
    """Hash the entire immutable JSON/JSONL source, then select an exact event."""
    data = Path(ref['path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != ref['sha256']:
        raise ValueError('lifecycle_raw_hash_mismatch')
    try:
        value = json.loads(data)
    except ValueError:
        value = [json.loads(line) for line in data.splitlines() if line.strip()]
    events = value if isinstance(value, list) else value.get('events', [value])
    index = ref.get('event_index')
    if index is None:
        if len(events) != 1:
            raise ValueError('explicit_lifecycle_event_index_required')
        index = 0
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise ValueError('invalid_lifecycle_event_index')
    event = events[index]
    if not isinstance(event, dict):
        raise ValueError('lifecycle_event_object_required')
    return event


def _binding_times(binding, guid, graph_raw_path):
    """Bind create-call begin to post-return GUID without excluding early SEDP.

    Both receipts independently identify the same process and generation. Begin
    intentionally has no GUID: DDS may announce while create_*() is executing.
    """
    try:
        refs = [binding['begin_raw_ref'], binding['return_raw_ref']]
        if any(Path(ref['path']).resolve() == Path(graph_raw_path).resolve() for ref in refs):
            return None
        begin, returned = [_lifecycle_event(ref) for ref in refs]
        if begin.get('event') != 'endpoint_create_begin' or returned.get('event') != 'endpoint_created':
            return None
        generation = binding.get('generation')
        if not isinstance(generation, str) or not generation or re.fullmatch(r'(?:writer|reader)(?:[2-4])?', str(binding.get('role'))) is None:
            return None
        if not all(_positive_int(binding.get(k)) for k in ('pid', 'process_start_ticks')):
            return None
        for event in (begin, returned):
            if (event.get('generation', event.get('endpoint_generation')) != generation
                    or any(event.get(k) != binding[k] for k in ('pid', 'process_start_ticks', 'role'))):
                return None
        if _guid(returned.get('endpoint_guid', returned.get('endpoint_gid'))) != guid:
            return None
        begin_ns, return_ns = [e.get('unix_ns', e.get('time_ns')) for e in (begin, returned)]
        begin_mono, return_mono = [e.get('monotonic_ns') for e in (begin, returned)]
        if not all(_positive_int(t) for t in (begin_ns, return_ns, begin_mono, return_mono)):
            return None
        if begin_ns > return_ns or begin_mono > return_mono:
            return None
        return {'begin_ns': begin_ns, 'return_ns': return_ns,
                'begin_monotonic_ns': begin_mono, 'return_monotonic_ns': return_mono}
    except (KeyError, IndexError, AttributeError, OSError, TypeError, ValueError):
        return None


def _drain_window_valid(receipt, end):
    """New receipts cover the stop request cutoff, not the later close time."""
    if 'drain' not in receipt and 'complete_through_unix_ns' not in receipt:
        return True  # Retain the original contract for legacy receipts.
    drain = receipt.get('drain')
    complete = receipt.get('complete_through_unix_ns')
    if (not isinstance(drain, dict) or drain.get('status') != 'drained'
            or not _positive_int(complete) or not end <= complete <= receipt['unix_ns']
            or type(drain.get('cutoff_unix_ns')) is not int or drain['cutoff_unix_ns'] != complete
            or drain.get('nonblocking') is not True or drain.get('reason') != 'socket_empty'
            or type(drain.get('unread_packets_at_empty_boundary')) is not int
            or drain['unread_packets_at_empty_boundary'] != 0):
        return False
    request, empty = drain.get('request'), drain.get('empty_confirmed')
    statistics, ended = drain.get('empty_statistics'), drain.get('ended')
    if not all(isinstance(value, dict) for value in (request, empty, statistics, ended)):
        return False
    sampled = statistics.get('sampled')
    if (not isinstance(sampled, dict) or request.get('unix_ns') != complete
            or any(type(statistics.get(key)) is not int or statistics[key] < 0 for key in ('packets', 'drops'))
            or statistics['drops'] != 0):
        return False
    points = (request, empty, sampled, ended, receipt)
    for clock in ('unix_ns', 'monotonic_ns'):
        stamps = [point.get(clock) for point in points]
        if not all(_positive_int(stamp) for stamp in stamps) or stamps != sorted(stamps):
            return False
    deadline = drain.get('deadline_monotonic_ns')
    return (_positive_int(deadline) and request['monotonic_ns'] < deadline
            and sampled['monotonic_ns'] < deadline)


def _capture_window_valid(discovery, start, end):
    artifact = discovery.get('capture_receipt')
    if not isinstance(artifact, dict):
        return False
    try:
        receipt = _bound_json(artifact['receipt_raw'])
        if {k: v for k, v in artifact.items() if k != 'receipt_raw'} != receipt:
            return False
        return (receipt['raw']['sha256'] == discovery['raw']['sha256']
                and receipt['ready']['unix_ns'] <= start <= end <= receipt['unix_ns']
                and _drain_window_valid(receipt, end)
                and receipt['thread_stopped'] and receipt['socket_closed']
                and receipt['health']['status'] == 'ok'
                and receipt['health']['kernel_drop_status'] == 'known_at_stop'
                and not receipt['counters'].get('kernel_drops', 0)
                and not receipt['counters'].get('truncated_frames', 0)
                and not receipt.get('limit_reason') and not receipt.get('error'))
    except (KeyError, OSError, TypeError, ValueError):
        return False


def reconcile_discovery_projection(discovery, graph_snapshot, *, endpoint_bindings,
                                   observation_window, field_policy=None):
    """Field facts only. Missing/tampered sources, bindings/windows remain unknown.

    graph_snapshot is a JSON raw object with source='ros_graph', captured_at_unix_ns,
    endpoints and a separate raw={path,sha256} reference (raw_path/raw_sha256 also
    accepted). Endpoint bindings carry begin_raw_ref/return_raw_ref, generation,
    role, pid, process_start_ticks and endpoint_guid. Each raw ref hashes a whole
    JSON/JSONL file and selects event_index when it contains multiple events.
    The existing S1 oracle, not this helper, determines the canonical verdict.
    """
    unknown, differences, comparisons = [], [], []
    if discovery.get('independent_source') != 'passive_rtps_wire':
        unknown.append('independent_wire_source_required')
    try:
        actual = parse_sedp_capture(discovery.get('capture_receipt') or discovery['raw'],
                                    field_policy=field_policy if field_policy is not None else discovery.get('field_policy'))
        if actual['events'] != discovery.get('events') or actual['activity'] != discovery.get('activity'):
            unknown.append('wire_projection_not_bound_to_raw')
    except (KeyError, OSError, TypeError, ValueError):
        actual = {'events': []}
        unknown.append('wire_raw_unavailable_or_changed')
    graph_ref = graph_snapshot.get('raw', {'path': graph_snapshot.get('raw_path'), 'sha256': graph_snapshot.get('raw_sha256')})
    try:
        graph = _bound_json(graph_ref)
        if graph.get('source') != 'ros_graph':
            unknown.append('independent_public_graph_required')
        if Path(graph_ref['path']).resolve() == Path(discovery['raw']['path']).resolve():
            unknown.append('wire_graph_same_raw_source')
        if any(graph_snapshot.get(k) != graph.get(k) for k in ('source', 'captured_at_unix_ns', 'endpoints')):
            unknown.append('graph_projection_not_bound_to_raw')
    except (KeyError, OSError, TypeError, ValueError):
        graph = {}
        unknown.append('graph_raw_unavailable_or_changed')
    start, end = observation_window.get('start_ns'), observation_window.get('end_ns')
    captured = graph.get('captured_at_unix_ns')
    valid_times = all(isinstance(t, int) and not isinstance(t, bool) for t in (start, end, captured))
    if not valid_times or not start <= captured <= end or not _capture_window_valid(discovery, start, end):
        unknown.append('capture_window_or_health_not_established')
    if actual.get('diagnostics'):
        unknown.append('wire_parse_incomplete_or_unsupported')
    bindings = {}
    for binding in endpoint_bindings:
        if not isinstance(binding, dict):
            unknown.append('invalid_endpoint_binding')
            continue
        guid = _guid(binding.get('endpoint_guid', binding.get('endpoint_gid')))
        if guid in bindings:
            unknown.append('ambiguous_endpoint_generation_binding')
        if guid:
            bindings[guid] = binding
        else:
            unknown.append('invalid_endpoint_binding_guid')
    if not bindings:
        unknown.append('no_independent_endpoint_bindings')
    graph_by_guid = {}
    endpoints = graph.get('endpoints', [])
    if not isinstance(endpoints, list):
        endpoints = []
        unknown.append('invalid_graph_endpoints')
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            unknown.append('invalid_graph_endpoint')
            continue
        fields = _graph_fields(endpoint)
        guid = fields['endpoint_guid']
        if guid in graph_by_guid:
            unknown.append('duplicate_graph_endpoint_identity')
        if guid:
            graph_by_guid[guid] = fields
        else:
            unknown.append('invalid_graph_endpoint_guid')
    for guid, binding in bindings.items():
        lifecycle = _binding_times(binding, guid, graph_ref.get('path'))
        if lifecycle is None:
            unknown.append('unverified_process_generation_binding:' + guid)
        selected = [e for e in actual['events'] if e.get('endpoint_guid') == guid
                    and isinstance(captured, int) and isinstance(start, int) and start <= e['unix_ns'] <= captured]
        selected.sort(key=lambda e: (e['unix_ns'], e['raw_ref']['packet_index']))
        latest = selected[-1] if selected else None
        if latest is None or latest['lifecycle'] != 'alive':
            unknown.append('no_live_wire_endpoint_at_graph_snapshot:' + guid)
            continue
        if lifecycle is not None:
            first_alive = next((e for e in selected if e['lifecycle'] == 'alive'), latest)
            if not (start <= lifecycle['begin_ns'] <= first_alive['unix_ns']
                    and lifecycle['return_ns'] <= captured):
                unknown.append('endpoint_creation_chronology_unbound:' + guid)
        if latest['source_guid_prefix'] != guid[:24] or binding.get('role') != latest['role']:
            unknown.append('endpoint_participant_or_role_binding_mismatch:' + guid)
        counterpart = graph_by_guid.get(guid)
        if counterpart is None:
            unknown.append('endpoint_missing_from_finite_graph_window:' + guid)
            continue
        for name in BASELINE_FIELDS:
            field = latest.get('fields', {}).get(name, {})
            equal = _equal(name, field.get('value'), counterpart.get(name)) if field.get('status') == 'observed' else None
            row = {'endpoint_guid': guid, 'generation': binding.get('generation'), 'field': name,
                   'equal': equal, 'wire': copy.deepcopy(field), 'graph_value': counterpart.get(name),
                   'graph_raw': copy.deepcopy(graph_ref)}
            comparisons.append(row)
            if equal is False:
                differences.append(row)
            elif equal is None:
                unknown.append('unobserved_field:' + guid + ':' + name)
    return {'kind': 'independent_discovery_projection',
            'comparison_status': 'unknown' if unknown else 'mismatch' if differences else 'consistent',
            'comparisons': comparisons, 'differences': differences,
            'unknown_reasons': sorted(set(unknown)), 'endpoint_count': len(bindings),
            'history_depth': 'not_applicable_by_endpoint_schema', 'baseline_fields': list(BASELINE_FIELDS),
            'independent_sources': {'wire': discovery.get('raw'), 'graph': graph_ref},
            'actual_match': 'not_evaluated', 'offered_requested': 'separate_check_offered_requested',
            'candidate_admission': 'not_performed', 'maturity_denominator_changed': False}
