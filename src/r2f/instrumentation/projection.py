"""Independent SEDP receive projection versus a separately captured ROS graph.

This is a field-limited feasibility comparator, not a builtin-target seal oracle.
Missing fields/window/coverage are unknown; graph never supplies vendor values.
"""
import json
from pathlib import Path
from .cyclonedds import file_sha256, normalize_guid, parse_trace


def reconcile_projection(traces, graph_snapshot, *, dds_topic, ros_topic,
                         required_qos=('reliability', 'durability'), complete_window=False,
                         window_start_ns=None, window_end_ns=None):
    unknown, mismatches = [], []
    if graph_snapshot.get('source') != 'ros_graph':
        unknown.append('graph_source_not_independent')
    try:
        raw_path = Path(graph_snapshot['raw_path'])
        if file_sha256(raw_path) != graph_snapshot['raw_sha256']:
            unknown.append('graph_raw_hash_mismatch')
            graph_snapshot = {}
        else:
            decoded = json.loads(raw_path.read_text())
            if any(decoded.get(key) != graph_snapshot.get(key)
                   for key in ('source', 'captured_at_unix_ns', 'endpoints')):
                unknown.append('graph_projection_does_not_match_raw')
            graph_snapshot = decoded
    except (OSError, KeyError, TypeError, ValueError):
        unknown.append('graph_raw_unavailable')
        graph_snapshot = {}
    captured = graph_snapshot.get('captured_at_unix_ns')
    if (not complete_window or not isinstance(captured, int)
            or not isinstance(window_start_ns, int) or not isinstance(window_end_ns, int)
            or not window_start_ns <= captured <= window_end_ns):
        unknown.append('independent_observation_window_incomplete')
    window_valid = (complete_window and isinstance(captured, int)
                    and isinstance(window_start_ns, int) and isinstance(window_end_ns, int)
                    and window_start_ns <= captured <= window_end_ns)
    vendor, graph = {}, {}
    for trace in traces:
        if not trace.get('vendor_process_verified'):
            unknown.append('vendor_process_provenance_unverified')
            continue
        try:
            if file_sha256(trace['raw_path']) != trace['raw_sha256']:
                unknown.append('vendor_raw_hash_mismatch')
                continue
        except (OSError, KeyError, TypeError):
            unknown.append('vendor_raw_unavailable')
            continue
        parsed_events = parse_trace(Path(trace['raw_path']).read_bytes())['events']
        manifest_events = trace.get('events', [])
        if len(parsed_events) != len(manifest_events) or any(
                any(stored.get(key) != value for key, value in actual.items() if key != 'provenance_status')
                for actual, stored in zip(parsed_events, manifest_events)):
            unknown.append('vendor_projection_does_not_match_raw')
            continue
        for event in manifest_events:
            if event.get('provenance_status') != 'verified_vendor_process':
                unknown.append('event_provenance_unverified')
                continue
            if isinstance(captured, int) and event['event_time_ns'] > captured:
                continue
            if isinstance(window_start_ns, int) and event['event_time_ns'] < window_start_ns:
                continue
            identity = event.get('endpoint_guid')
            if event.get('event') == 'sedp_endpoint_disposed':
                vendor.pop(identity, None)
            elif event.get('topic_name') == dds_topic:
                previous = vendor.get(identity)
                if previous is None or previous['event_time_ns'] < event['event_time_ns']:
                    vendor[identity] = event
    for endpoint in graph_snapshot.get('endpoints', []):
        if endpoint.get('topic_name') != ros_topic:
            continue
        try:
            guid = normalize_guid(endpoint.get('endpoint_guid', endpoint.get('endpoint_gid')))
        except (ValueError, TypeError):
            unknown.append('graph_endpoint_guid_unknown')
            continue
        if guid in graph and graph[guid] != endpoint:
            unknown.append('conflicting_graph_endpoint_records:' + guid)
        graph[guid] = endpoint
    if not vendor or not graph:
        unknown.append('empty_independent_projection')
    for guid in sorted(set(vendor) | set(graph)):
        left, right = vendor.get(guid), graph.get(guid)
        if left is None or right is None:
            detail = {'endpoint_guid': guid, 'field': 'membership',
                      'vendor': left is not None, 'graph': right is not None}
            if window_valid and not unknown:
                mismatches.append(detail)
            else:
                unknown.append('endpoint_coverage_unknown:' + guid)
            continue
        comparisons = [('endpoint_kind', left.get('endpoint_kind'), right.get('endpoint_kind'))]
        comparisons += [(f'qos.{key}', left.get('qos', {}).get(key), right.get('qos', {}).get(key))
                        for key in required_qos]
        for field, a, b in comparisons:
            if a is None or b is None:
                unknown.append(f'{guid}:{field}')
            elif a != b:
                mismatches.append({'endpoint_guid': guid, 'field': field, 'vendor': a, 'graph': b})
    return {'schema': 'r2f.discovery-projection-feasibility/v1',
            'verdict': 'fail' if mismatches else 'unknown' if unknown else 'pass',
            'scope': 'selected_endpoint_identity_role_and_requested_qos_only',
            'target_maturity': 'not_evaluated', 'candidate_admission': 'not_performed',
            'vendor_source': 'CycloneDDS received SEDP internal trace', 'graph_source': 'ROS public graph API',
            'dds_topic': dds_topic, 'ros_topic': ros_topic, 'required_qos': list(required_qos),
            'vendor_endpoint_guids': sorted(vendor), 'graph_endpoint_guids': sorted(graph),
            'unknown_fields': sorted(set(unknown)), 'mismatches': mismatches}
