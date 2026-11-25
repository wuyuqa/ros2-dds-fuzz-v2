"""Bounded discovery observations derived after the existing sidecar audit.

Configuration, received announcements and pair readiness are separate facts.
None is a mutation hit, a measured announcement period or global completion.
"""
from copy import deepcopy
import hashlib
from pathlib import Path

from r2f.instrumentation.participant_lease import parse_lifecycle_trace
from r2f.modules.s1.coverage import catalog_lineage

_MAX_TRACE_BYTES = 8 * 1024 * 1024
_CASES = ('Fast-DDS-PR6153', 'xrce115', 'eProsima/Micro-XRCE-DDS#41')


def _historical_context():
    cases = catalog_lineage()['cases']
    return {'reproduction_status': 'not_run', 'adds_product_backend': False,
            'cases': [{key: deepcopy(case[key]) for key in
                ('case_id', 'source_url', 'source_ref', 'research_classification',
                 'disposition', 'runtime_applicability')}
                for case in cases if case['case_id'] in _CASES],
            'limitations': ['backport_and_upstream_same_fix_chain_not_independent_roots',
                'XRCE_specific_control_flow_not_a_ROS2_DDS_mechanism',
                'test_timeout_change_not_protocol_fix',
                'generic_profile_and_discovery_obligations_remain_applicable']}


def discovery_profile_facts(config, prepared, workers, traces, comparisons, adopted):
    """Consume independently revalidated inputs; never trust a saved projection.

    Callers must first validate the process captures, endpoint generations, trace
    identities and graph command chains. No configuration fills a missing event.
    Participant correlation uses a GUID prefix, not native ownership proof.
    """
    result = {
        'schema': 'r2f.discovery-profile-consistency/v1', 'status': 'unknown',
        'candidate_eligible': False,
        'execution_identity': {key: config[key] for key in
            ('run_id', 'domain_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat',
             'effective_input_sha256')},
        'configuration': {'status': adopted.get('status', 'unknown'),
            'semantic_effect': 'unknown', 'prepared_profiles': deepcopy(prepared['profiles']),
            'adopted': deepcopy(adopted),
            'loaded_binaries': {role: deepcopy(worker['process']['loaded_libraries'])
                                for role, worker in workers.items()},
            'binary_source_equivalence': 'unknown'},
        'announcements': {'status': 'unknown', 'roles': {},
            'scope': 'received_participant_and_endpoint_announcements',
            'periodic_schedule_status': 'unverified'},
        'completion': {'status': 'unknown', 'roles': {},
            'scope': 'submitted_pair_at_acknowledged_graph_snapshots',
            'global_discovery_complete': False},
        'asynchronous_discovery': {'status': 'unknown', 'roles': {},
            'fact_kind': 'vendor_builtin_dispatch_labels',
            'os_thread_identity_status': 'unverified',
            'user_datawriter_async_status': 'unverified',
            'scheduling_and_concurrency_status': 'unverified',
            'race_status': 'not_established'},
        'historical_context': _historical_context(),
        'reasons': [],
        'limitations': ['finite_observation_not_global_discovery_completion',
            'participant_prefix_correlation_not_native_ownership',
            'vendor_wall_time_not_atomic_or_causal_order',
            'configured_interval_not_measured_announcement_schedule',
            'normal_observations_not_historical_reproduction'],
    }
    if config.get('observation_mode', 'blackbox') != 'instrumentation':
        result['reasons'].append('internal_discovery_not_observed_in_blackbox')
        return result
    for observer, remote in (('writer', 'reader'), ('reader', 'writer')):
        local, peer = workers[observer], workers[remote]
        matches = [trace for trace in traces
                   if trace['config']['output_file'] == prepared['trace_configs'][observer]['output_file']]
        graphs = [entry for entry in comparisons if entry['role'] == observer]
        if len(matches) != 1 or len(graphs) != 1:
            result['reasons'].append('discovery_sources_unavailable:' + observer)
            continue
        trace, comparison = matches[0], graphs[0]
        graph = comparison['graph']
        start, end = prepared['started_unix_ns'], graph['captured_at_unix_ns']
        path = Path(trace['raw_path'])
        if path.stat().st_size > _MAX_TRACE_BYTES:
            result['reasons'].append('profile_trace_size_limit:' + observer)
            continue
        with path.open('rb') as handle:
            raw = handle.read(_MAX_TRACE_BYTES + 1)
        if len(raw) > _MAX_TRACE_BYTES or hashlib.sha256(raw).hexdigest() != trace['raw_sha256']:
            raise ValueError('profile_trace_changed:' + observer)
        lifecycle = parse_lifecycle_trace(raw, domain_id=config['domain_id'])
        if lifecycle['diagnostics']:
            result['reasons'].append('participant_trace_diagnostics:' + observer)
            continue
        participant_guid = peer['endpoint_guid'][:24] + '000001c1'
        participant = [event for event in lifecycle['events']
                       if event['kind'] == 'discovered' and event['participant_guid'] == participant_guid
                       and start <= event['unix_ns'] <= end]
        endpoints = [event for event in trace['events']
                     if event['event'] == 'sedp_endpoint_alive'
                     and event['endpoint_guid'] == peer['endpoint_guid']
                     and event['endpoint_kind'] == remote
                     and event['domain_id'] == config['domain_id']
                     and event['topic_name'] == 'rt' + config['topic']
                     and start <= event['event_time_ns'] <= end
                     and event['provenance_status'] == 'verified_vendor_process']
        witness = {'observer_pid': local['pid'],
            'observer_process_start_ticks': local['process']['process_start_ticks'],
            'remote_endpoint_guid': peer['endpoint_guid'],
            'remote_endpoint_generation': peer['endpoint_event']['endpoint_generation'],
            'remote_process_start_ticks': peer['process']['process_start_ticks'],
            'participant': None, 'endpoint': None}
        if participant:
            witness['participant'] = dict(deepcopy(participant[-1]),
                raw_path=trace['raw_path'], raw_sha256=trace['raw_sha256'])
        else:
            result['reasons'].append('participant_announcement_unobserved:' + observer)
        if endpoints:
            witness['endpoint'] = deepcopy(endpoints[-1])
        else:
            result['reasons'].append('endpoint_announcement_unobserved:' + observer)
        retired = [event for event in lifecycle['events']
                   if participant and event['participant_guid'] == participant_guid
                   and event['kind'] in ('expired', 'gc_deleted')
                   and event['offset'] > participant[-1]['offset']
                   and start <= event['unix_ns'] <= end]
        witness['retirements_before_snapshot'] = [dict(deepcopy(event),
            raw_path=trace['raw_path'], raw_sha256=trace['raw_sha256']) for event in retired]
        current_endpoint = bool(participant and endpoints
                                and endpoints[-1]['raw_byte_offset'] > participant[-1]['offset'])
        if retired:
            result['reasons'].append('participant_retired_before_snapshot:' + observer)
        if participant and endpoints and not current_endpoint:
            result['reasons'].append('endpoint_not_announced_in_current_participant_incarnation:' + observer)
        result['announcements']['roles'][observer] = witness
        if (current_endpoint and not retired
                and witness['participant']['thread'] == witness['endpoint']['thread'] == 'dq.builtin'):
            result['asynchronous_discovery']['roles'][observer] = {
                'observer_pid': local['pid'],
                'participant': deepcopy(witness['participant']),
                'endpoint': deepcopy(witness['endpoint']),
                'basis': 'source_pinned_SPDP_and_SEDP_receive_logs_on_dq.builtin'}
        if current_endpoint and not retired and comparison['comparison']['verdict'] == 'pass':
            result['completion']['roles'][observer] = {
                'graph': deepcopy(graph), 'remote_endpoint_guid': peer['endpoint_guid'],
                'remote_endpoint_generation': peer['endpoint_event']['endpoint_generation'],
                'basis': 'received_announcements_and_fresh_acknowledged_exact_pair_graph'}
    if (len(result['announcements']['roles']) == 2
            and all(row['participant'] and row['endpoint']
                    for row in result['announcements']['roles'].values())):
        result['announcements']['status'] = 'observed'
    if len(result['asynchronous_discovery']['roles']) == 2:
        result['asynchronous_discovery']['status'] = 'observed'
    if len(result['completion']['roles']) == 2:
        result['completion']['status'] = 'observed'
    if (result['configuration']['status'] == 'observed'
            and result['announcements']['status'] == result['completion']['status'] == 'observed'):
        result['status'] = 'observed'
    return result
