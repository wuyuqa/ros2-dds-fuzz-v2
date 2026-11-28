"""Validate native match evidence against its immutable worker raw chain.

No DDS calls or mutable graph-derived peer lists. This supplements the existing
request/snapshot/ACK, current-generation and graph-scope gates; it never replaces
them and cannot turn a count-only sample into a membership observation.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import re

from .activation import OBSERVATION_SCHEMA, manifest_for_reference

MEMBERSHIP_BASIS = 'reliable_stateful_native_predicate_guid_set_under_entity_mutex'
ORDINAL_BASIS = 'provider_instance_observation_serial_not_match_epoch'


def _hex(value, length):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(length) + '}', value) is not None


def validate_observation(observation, match, token, events, request):
    if not isinstance(observation, dict):
        return 'native_peer_observation_missing'
    peers = match.get('matched_peer_gids')
    if (observation.get('schema') != OBSERVATION_SCHEMA
            or observation.get('observation_mode') != 'instrumentation'
            or observation.get('complete') is not True
            or observation.get('domain_id') != token.get('domain_id')
            or observation.get('local_gid') != token.get('endpoint_gid')
            or observation.get('matched_peer_gids') != peers
            or observation.get('membership_basis') != MEMBERSHIP_BASIS
            or observation.get('snapshot_ordinal_basis') != ORDINAL_BASIS):
        return 'native_peer_observation_contract_invalid'
    role = token.get('role')
    if not isinstance(role, str) or re.fullmatch(r'(writer|reader)(?:[2-4])?', role) is None:
        return 'native_peer_observation_role_invalid'
    kind = 'writer' if role.startswith('writer') else 'reader'
    if observation.get('role_kind') != kind or not isinstance(peers, list):
        return 'native_peer_observation_role_invalid'
    result = observation.get('native_result')
    if not isinstance(result, dict):
        return 'native_peer_result_missing'
    expected = dict(schema_version=1, struct_size=64, status=0,
                    endpoint_kind=1 if kind == 'writer' else 2,
                    required_count=len(peers), copied=len(peers), complete=1, reserved=0)
    if (any(type(result.get(k)) is not int or result[k] != v for k, v in expected.items())
            or result.get('local_guid') != token.get('endpoint_gid')
            or type(result.get('snapshot_ordinal')) is not int
            or not 0 < result['snapshot_ordinal'] <= (1 << 64) - 1):
        return 'native_peer_result_invalid'
    for prior in events[:match['event_index']]:
        if (prior.get('event') != 'match_snapshot' or any(
                token.get(key) is None or prior.get(key) != token[key]
                for key in ('pid', 'process_start_ticks', 'endpoint_generation'))):
            continue
        previous = prior.get('native_peer_observation')
        if not isinstance(previous, dict) or previous.get('schema') != OBSERVATION_SCHEMA:
            continue
        previous_result = previous.get('native_result')
        if isinstance(previous_result, dict) and previous_result.get('status') == 0:
            serial = previous_result.get('snapshot_ordinal')
            if type(serial) is not int or serial >= result['snapshot_ordinal']:
                return 'native_peer_observation_serial_reused_or_regressed'
    ref = observation.get('manifest_reference')
    if (not isinstance(ref, dict) or set(ref) != {'observation_mode', 'manifest_path', 'manifest_sha256'}
            or ref.get('observation_mode') != 'instrumentation'
            or not isinstance(ref.get('manifest_path'), str) or not Path(ref['manifest_path']).is_absolute()
            or not _hex(ref.get('manifest_sha256'), 64)):
        return 'native_peer_manifest_reference_invalid'
    identity_fields = ('logical_id', 'generation', 'endpoint_generation', 'pid', 'pgid',
                       'process_start_ticks', 'run_id', 'domain_id')
    selected = [row for row in events[:request['event_index']]
                if row.get('event') == 'native_peer_provider_selected'
                and all(token.get(k) is not None and row.get(k) == token[k] for k in identity_fields)]
    if len(selected) != 1:
        return 'native_peer_selection_not_unique_raw_worker'
    selected = selected[0]
    if (match.get('observation_mode') != 'instrumentation'
            or selected.get('observation_mode') != 'instrumentation'
            or selected.get('rmw_identifier') != 'rmw_fastrtps_cpp'
            or selected.get('observed_domain_id') != token.get('domain_id')
            or selected.get('manifest_reference') != ref
            or selected.get('provider_source_sha256') != hashlib.sha256(
                Path(__file__).with_name('provider.cpp').read_bytes()).hexdigest()):
        return 'native_peer_selected_source_identity_invalid'
    abi = selected.get('provider_abi', {})
    if (not isinstance(abi, dict) or abi.get('schema_version') != 1
            or abi.get('result_size') != 64 or abi.get('guid_size') != 16
            or abi.get('layout_changed') is not False or abi.get('virtual_slots_added') is not False):
        return 'native_peer_selected_abi_invalid'
    times = [selected.get('monotonic_ns'), request.get('monotonic_ns'),
             observation.get('started_monotonic_ns'), observation.get('ended_monotonic_ns'),
             match.get('monotonic_ns')]
    if (any(type(t) is not int or t <= 0 for t in times) or times != sorted(times)
            or selected.get('monotonic_clock_identity') != match.get('monotonic_clock_identity')):
        return 'native_peer_capture_chronology_invalid'
    try:
        manifest = manifest_for_reference(ref)
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return 'native_peer_selected_manifest_unavailable_or_invalid'
    if (selected.get('vendor_binary') != manifest['binaries']['vendor']
            or selected.get('provider_binary') != manifest['binaries']['provider']
            or selected.get('provider_source_sha256') != manifest['provider_source_sha256']
            or selected.get('provider_abi') != manifest['patch']['abi']):
        return 'native_peer_selected_binary_differs_from_manifest'
    mapped = observation.get('mapped_libraries')
    if not isinstance(mapped, list) or len(mapped) != 2:
        return 'native_peer_mapped_libraries_incomplete'
    for role in ('vendor', 'provider'):
        rows = [row for row in mapped if isinstance(row, dict) and row.get('role') == role]
        binary = selected.get(role + '_binary', {})
        if (len(rows) != 1 or not isinstance(binary, dict)
                or not _hex(binary.get('sha256'), 64)
                or not isinstance(binary.get('path'), str) or not Path(binary['path']).is_absolute()):
            return 'native_peer_selected_binary_invalid'
        row = rows[0]
        if (row.get('path') != binary['path'] or row.get('sha256') != binary['sha256']
                or type(row.get('inode')) is not int or row['inode'] <= 0
                or not isinstance(row.get('device'), str)
                or re.fullmatch(r'[0-9a-f]+:[0-9a-f]+', row['device']) is None):
            return 'native_peer_mapped_binary_identity_invalid'
        if role == 'vendor':
            actual = selected.get('vendor_identity', {})
            if (not isinstance(actual, dict) or actual.get('identity_verified') is not True
                    or actual.get('observation_mode') != 'instrumentation'
                    or actual.get('manifest_reference') != ref
                    or any(actual.get(k) != row[k] for k in ('path', 'sha256', 'device', 'inode'))):
                return 'native_peer_selected_vendor_mapping_invalid'
    return None


def validate_sampling_control(events, config):
    """Reject worker/capture contradictions even when summary declarations agree."""
    sampling = config.get('observe_native_peers', True)
    for row in events:
        if row.get('event') in {'native_peer_provider_selected', 'endpoint_created'} or 'observe_native_peers' in row:
            actual = row.get('observe_native_peers', True)
            if (type(actual) is not bool or actual != sampling
                    or ('observe_native_peers' in config and 'observe_native_peers' not in row)):
                raise ValueError('native_peer_sampling_control_raw_selection_mismatch')
    if not sampling:
        def native_capture(value):
            if isinstance(value, dict):
                return (value.get('schema') == OBSERVATION_SCHEMA
                        or value.get('peer_proof_source') == 'fastdds_native_stateful_peer_snapshot'
                        or any(native_capture(child) for child in value.values()))
            return isinstance(value, list) and any(native_capture(child) for child in value)
        if any(native_capture(row) for row in events):
            raise ValueError('native_peer_sampling_control_has_native_capture')


def validate_execution_mode(events, config, *, raw_path=None):
    """Bind actual worker declarations to explicit runner selection, not labels."""
    from .activation import reference_from_config, manifest_for_reference
    reference = reference_from_config(config)
    selected = [row for row in events if row.get('event') == 'native_peer_provider_selected']
    if reference is None:
        from r2f.instrumentation.fastdds_pdp_trace.receipt import validate_raw
        validate_raw(events, None)
        if selected or any(row.get('event') == 'endpoint_created'
                           and row.get('observation_mode', 'blackbox') != 'blackbox' for row in events):
            raise ValueError('native_peer_actual_mode_differs_from_blackbox_config')
        return None
    manifest = manifest_for_reference(reference)
    validate_sampling_control(events, config)
    expected_reference = dict(reference)
    if raw_path is not None:
        expected_reference['manifest_path'] = str(Path(raw_path).resolve().parent / 'native-peer-build-manifest.json')
        copied = manifest_for_reference(expected_reference)
        if copied != manifest:
            raise ValueError('native_peer_copied_manifest_content_differs')
    created = [row for row in events if row.get('event') == 'endpoint_created']
    keys = ('logical_id', 'generation', 'endpoint_generation', 'pid', 'pgid',
            'process_start_ticks', 'run_id', 'domain_id')
    if not created or len(selected) != len(created):
        raise ValueError('native_peer_selection_count_differs_from_created_workers')
    for worker in created:
        declarations = [row for row in selected if all(
            worker.get(k) is not None and row.get(k) == worker[k] for k in keys)]
        if len(declarations) != 1:
            raise ValueError('native_peer_worker_selection_not_unique')
        row = declarations[0]
        ref = row.get('manifest_reference')
        if (worker.get('observation_mode') != 'instrumentation'
                or row.get('observation_mode') != 'instrumentation'
                or ref != expected_reference
                or row.get('vendor_binary') != manifest['binaries']['vendor']
                or row.get('provider_binary') != manifest['binaries']['provider']
                or row.get('provider_source_sha256') != manifest['provider_source_sha256']
                or row.get('provider_abi') != manifest['patch']['abi']
                or row['event_index'] >= worker['event_index']):
            raise ValueError('native_peer_actual_selection_differs_from_config')
    from r2f.instrumentation.fastdds_pdp_trace.receipt import validate_raw
    validate_raw(events, manifest, peer_reference=expected_reference)
    return reference
