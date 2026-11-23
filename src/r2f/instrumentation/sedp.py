"""Public passive discovery observation API; runtime owns ROS and namespace lifecycle."""
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path

from .sedp_capture import PassiveSedpObserver, build_capture_spec

from .sedp_wire import parse_sedp_capture, parse_sedp_capture_prefix

__all__ = ['PassiveSedpObserver', 'build_capture_spec', 'parse_sedp_capture', 'parse_sedp_capture_prefix']

from .sedp_projection import check_offered_requested
__all__ += ['reconcile_discovery_projection', 'check_offered_requested']

# Enrich the existing comparator; its verdict, rules and admission stay canonical.
from .sedp_projection import reconcile_discovery_projection as _reconcile_projection
from .sedp_fields import discovery_layer_schema


def reconcile_discovery_projection(discovery, graph_snapshot, *, endpoint_bindings,
                                   observation_window, field_policy=None, discovery_layers=None):
    """Expose CP1 schema limits and asynchronous source times, without a new oracle."""
    result = _reconcile_projection(
        discovery, graph_snapshot, endpoint_bindings=endpoint_bindings,
        observation_window=observation_window, field_policy=field_policy)
    # Positive raw reproduction checks, independent of comparator reason names.
    from .sedp_projection import _bound_json
    identity_reasons = []
    actual, graph = {}, {}
    try:
        artifact = discovery.get('capture_receipt') or discovery['raw']
        actual = parse_sedp_capture(artifact, field_policy=(
            field_policy if field_policy is not None else discovery.get('field_policy')))
        reported = discovery['raw']
        if (Path(reported['path']).resolve() != Path(actual['raw']['path']).resolve()
                or reported['sha256'] != actual['raw']['sha256']
                or any(reported[key] != actual['raw'].get(key)
                       for key in ('size', 'linktype', 'timestamp_precision_ns') if key in reported)
                or discovery.get('independent_source') != 'passive_rtps_wire'
                or any(discovery.get(key) != actual[key] for key in ('events', 'activity'))):
            identity_reasons.append('wire_identity_or_projection_not_reproduced')
        if discovery.get('capture_receipt'):
            receipt = _bound_json(artifact['receipt_raw'])
            if receipt != {k: v for k, v in artifact.items() if k != 'receipt_raw'}:
                identity_reasons.append('capture_receipt_not_reproduced')
        graph_ref = result['independent_sources']['graph']
        graph = _bound_json(graph_ref)
        if (graph.get('source') != 'ros_graph'
                or any(graph_snapshot.get(k) != graph.get(k)
                       for k in ('source', 'captured_at_unix_ns', 'endpoints'))
                or Path(graph_ref['path']).samefile(actual['raw']['path'])):
            identity_reasons.append('graph_identity_or_projection_not_reproduced')
    except (KeyError, OSError, TypeError, ValueError):
        identity_reasons.append('source_identity_unavailable')
    qualification = {'identity': {'status': 'unknown' if identity_reasons else 'established',
                                   'reasons': identity_reasons}}
    from .sedp_projection import _graph_fields
    structure_reasons = []
    if not actual or actual.get('diagnostics'):
        structure_reasons.append('wire_decode_incomplete_or_unsupported')
    endpoints = graph.get('endpoints')
    if not isinstance(endpoints, list):
        structure_reasons.append('graph_endpoint_list_required')
    else:
        guids = []
        for endpoint in endpoints:
            if not isinstance(endpoint, dict):
                structure_reasons.append('graph_endpoint_object_required')
                continue
            guid = _graph_fields(endpoint)['endpoint_guid']
            if not guid or guid in guids:
                structure_reasons.append('graph_endpoint_identity_invalid_or_ambiguous')
            guids.append(guid)
    qualification['structure'] = {
        'status': 'unknown' if structure_reasons else 'established',
        'reasons': sorted(set(structure_reasons))}
    established = not identity_reasons and not structure_reasons
    from .sedp_projection import _capture_window_valid
    observations = []
    captured = graph.get('captured_at_unix_ns')
    start, end = observation_window.get('start_ns'), observation_window.get('end_ns')
    timing_reasons = []
    receipt = actual.get('capture_receipt')
    clock = None
    if not isinstance(receipt, Mapping):
        timing_reasons.append('capture_receipt_mapping_required')
    else:
        ready = receipt.get('ready')
        if not isinstance(ready, Mapping):
            timing_reasons.append('capture_ready_mapping_required')
        else:
            clock = ready.get('timestamp_clock')
        if (type(receipt.get('unix_ns')) is not int
                or (isinstance(ready, Mapping) and type(ready.get('unix_ns')) is not int)):
            timing_reasons.append('capture_receipt_time_type_invalid')
    if clock != 'kernel_SO_TIMESTAMPNS_realtime':
        timing_reasons.append('capture_realtime_clock_not_established')
    # Byte hashing and a *_unix_ns label alone do not establish clock origin.
    if graph.get('timestamp_clock') != 'unix_ns_realtime':
        timing_reasons.append('graph_realtime_clock_not_established')
    valid_times = all(type(t) is int for t in (start, end, captured))
    if not (valid_times and start <= captured <= end
            and _capture_window_valid(discovery, start, end)):
        timing_reasons.append('observation_window_or_capture_coverage_not_established')
    qualified = established and not timing_reasons
    qualification['timing'] = {
        'status': 'qualified' if qualified else 'unknown',
        'reasons': timing_reasons if established else ['sources_not_established'] + timing_reasons,
        'wire_clock': clock,
        'graph_clock': graph.get('timestamp_clock'),
        'clock_basis': 'hash-bound capture receipt and graph clock declarations'}
    # Only select already raw-validated events; never promote caller alterations.
    if established and all(isinstance(t, int) and not isinstance(t, bool)
                           for t in (start, captured)):
        for guid in sorted({row['endpoint_guid'] for row in result['comparisons']}):
            events = [event for event in discovery['events']
                      if event.get('endpoint_guid') == guid
                      and start <= event['unix_ns'] <= captured]
            if events:
                latest = sorted(events, key=lambda e: (e['unix_ns'], e['raw_ref']['packet_index']))[-1]
                observations.append({
                    'endpoint_guid': guid, 'wire_unix_ns': latest['unix_ns'],
                    'graph_unix_ns': captured,
                    'graph_minus_wire_ns': captured - latest['unix_ns'] if qualified else None,
                    'timing_status': qualification['timing']['status'],
                    'timing_reasons': list(qualification['timing']['reasons']),
                    'raw_ref': deepcopy(latest['raw_ref']),
                    'graph_raw': deepcopy(result['independent_sources']['graph'])})
    result['schema_check'] = {
        'schema': 'r2f.discovery-schema-check/v1',
        'independent_sources_established': established,
        'source_qualification': qualification,
        'layers': discovery_layer_schema(), 'endpoint_observations': observations,
        'all_layer_obligations_closed': False,
        'assurance': 'schema/field facts only; no participant/topic builtin or remote receipt claim'}
    if discovery_layers is not None:
        from .discovery_layers import discovery_layers_schema
        layers = discovery_layers_schema(discovery_layers,
            discovery.get('capture_receipt') or {}, field_policy=field_policy)
        result['schema_check']['discovery_layers'] = layers
        for name in ('participant', 'topic'):
            result['schema_check']['layers'][name].update(
                status='independent_observations' if layers[name].get('observation_status') == 'observed' else 'unknown',
                observation_ref='discovery_layers.' + name)
            result['schema_check']['layers'][name]['reconciliation_status'] = 'unknown'
            if 'applicability' in layers[name]:
                result['schema_check']['layers'][name].update(
                    fields=deepcopy(layers[name]['applicability']),
                    basis='independent builtin samples and same-capture wire; instance association remains qualified separately')
        result['schema_check']['assurance'] = 'raw-backed auxiliary observations; obligations remain open'
    return result
