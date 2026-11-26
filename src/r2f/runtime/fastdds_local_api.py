"""Public Fast DDS local roots and borrowed endpoint parent observations.

The selected peer provider supplies copied values only. Pointer numbers are
process-local diagnostic identities; callers never dereference or reuse them.
"""
from __future__ import annotations
import ctypes as C
import json
import re
import time

SCHEMA = 'r2f.fastdds-participant-ownership/v1'
ROOT_SCHEMA = 'r2f.fastdds-local-participant-roots/v1'


def _require(ok, reason):
    if not ok:
        raise ValueError('fastdds_local_' + reason)


def _guid(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{32}', value) is not None and value != '0' * 32


def _pointer(value):
    return type(value) is int and 0 < value < 2**64


def _call(native, domain, pointer=0, kind='writer', name=None):
    _require(type(domain) is int and 0 <= domain <= 231, 'domain_invalid')
    _require(kind in ('writer', 'reader'), 'role_invalid')
    function = native.libs['provider'].r2f_fastdds_local_v1
    function.argtypes = [C.c_void_p, C.c_int, C.c_uint32, C.c_char_p, C.c_void_p, C.c_uint64]
    function.restype = C.c_int32
    output = C.create_string_buffer(131072)
    code = function(pointer, int(kind == 'reader'), domain,
                    name.encode('utf-8') if name is not None else None, output, len(output))
    _require(type(code) is int and code == 0, 'provider_error:' + str(code))
    value = json.loads(output.value)
    _require(type(value.get('schema_version')) is int and value['schema_version'] == 1
             and type(value.get('domain_id')) is int and value['domain_id'] == domain, 'domain_or_schema_mismatch')
    roots = value.get('roots')
    _require(isinstance(roots, list) and 0 <= len(roots) <= 256, 'roots_invalid')
    _require(all(isinstance(r, dict) and _pointer(r.get('participant_handle'))
                 and _guid(r.get('participant_gid')) and r.get('domain_id') == domain for r in roots), 'root_identity_invalid')
    _require(len({r['participant_handle'] for r in roots}) == len(roots)
             and len({r['participant_gid'] for r in roots}) == len(roots), 'duplicate_roots')
    return value


def sample_roots(reference, domain):
    from .fastdds_peer_observer import _Bindings
    native = _Bindings(reference)
    provenance = native.revalidate()
    value = _call(native, domain)
    _require(native.revalidate() == provenance, 'provenance_changed')
    return {'schema': ROOT_SCHEMA, 'domain_id': domain, 'complete': True,
            'roots': value['roots'], 'count': len(value['roots']), 'provenance': provenance,
            'api': 'DomainParticipantFactory::lookup_participants',
            'candidate_eligible': False, 'sampled_monotonic_ns': time.monotonic_ns()}


def sample_ownership(observer, domain, gid, kind):
    result = {'schema': SCHEMA, 'domain_id': domain, 'local_gid': gid, 'role_kind': kind,
              'complete': False, 'status': 'unknown', 'candidate_eligible': False,
              'scope': 'public_native_parent_and_factory_local_inventory_not_guid_prefix_inference'}
    try:
        _require(_guid(gid), 'endpoint_gid_invalid')
        with observer._lock:
            provenance = observer._native.revalidate()
            handle = observer.endpoint.handle
            with handle:
                pointer = observer._resolve(handle, kind)[2]
                value = _call(observer._native, domain, pointer, kind)
                _require(value.get('local_handle') == pointer and value.get('local_gid') == gid
                         and value.get('role_kind') == kind, 'endpoint_identity_mismatch')
                _require(_pointer(value.get('parent_handle')), 'parent_missing')
                roots = [r for r in value['roots'] if r['participant_handle'] == value.get('participant_handle')
                         and r['participant_gid'] == value.get('participant_gid')]
                _require(len(roots) == 1, 'endpoint_root_mismatch')
                _require(observer._native.revalidate() == provenance, 'provenance_changed')
        result.update(value, provenance=provenance, complete=True, status='observed',
                      api='DataWriter/Reader::get_publisher/subscriber::get_participant',
                      sampled_monotonic_ns=time.monotonic_ns())
    except (ValueError, TypeError, KeyError, OSError, AttributeError, RuntimeError) as exc:
        result.update(complete=False, status='unknown', reason=str(exc))
    return result


def qualify_group(observation, tokens):
    """Derive sharing from public native parents and factory roots, never labels."""
    result = {'endpoint_ownership_observed': False, 'local_root_inventory_complete': False,
              'group_relation_observed': False, 'actual_local_root_count': None, 'unknown_reasons': []}
    try:
        before, after = observation['roots_before'], observation['roots_after']
        for capture in (before, after):
            _require(capture.get('schema') == ROOT_SCHEMA and capture.get('complete') is True
                     and capture.get('candidate_eligible', False) is False and bool(capture.get('provenance')), 'group_roots_incomplete')
            roots = capture['roots']
            _require(capture.get('count') == len(roots) and len(roots) == 1, 'group_root_count')
            _require(all(_pointer(r.get('participant_handle')) and _guid(r.get('participant_gid'))
                         and r.get('domain_id') == capture['domain_id'] for r in roots), 'group_root_identity')
        result['actual_local_root_count'] = len(before['roots'])
        _require(all(before[k] == after[k] for k in ('roots', 'domain_id', 'provenance')), 'group_roots_changed')
        result['local_root_inventory_complete'] = True
        rows = observation['endpoints']
        _require(isinstance(rows, list) and len(rows) == len(tokens), 'group_endpoint_coverage')
        seen, handles = set(), set()
        from .participant_groups import ENDPOINT_KEYS
        from .topology_worker import worker_role_kind
        stable = ('local_handle', 'local_gid', 'parent_handle', 'participant_handle',
                  'participant_gid', 'role_kind', 'domain_id', 'roots', 'provenance')
        for row in rows:
            matches = [t for t in tokens if all(row.get(k) == t.get(k) for k in ENDPOINT_KEYS)]
            _require(len(matches) == 1 and row['logical_id'] not in seen, 'group_endpoint_identity')
            seen.add(row['logical_id'])
            samples = row['samples']
            _require(isinstance(samples, list) and len(samples) == 2, 'group_double_sample')
            for sample in samples:
                _require(sample.get('schema') == SCHEMA and sample.get('schema_version') == 1
                         and sample.get('complete') is True and sample.get('status') == 'observed'
                         and sample.get('candidate_eligible') is False, 'group_sample_incomplete')
                _require(sample.get('local_gid') == row['endpoint_gid']
                         and sample.get('role_kind') == worker_role_kind(matches[0].get('role', matches[0].get('kind')))
                         and _pointer(sample.get('local_handle')) and _pointer(sample.get('parent_handle')),
                         'group_local_identity')
                _require(sample.get('provenance') == before['provenance'] and sample.get('domain_id') == before['domain_id']
                         and sample.get('roots') == before['roots']
                         and sample.get('participant_handle') == before['roots'][0]['participant_handle']
                         and sample.get('participant_gid') == before['roots'][0]['participant_gid'], 'group_parent_root')
            _require(all(samples[0].get(k) == samples[1].get(k) for k in stable), 'group_sample_changed')
            _require(samples[0]['local_handle'] not in handles, 'group_reused_endpoint')
            handles.add(samples[0]['local_handle'])
        result['endpoint_ownership_observed'] = bool(tokens)
        result['group_relation_observed'] = bool(tokens)
    except (ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
        result['unknown_reasons'].append(str(exc))
    return result


def lookup_topic(observer, domain, gid, kind, scope, name):
    """Fast DDS 2.14.7 finds local names, not Cyclone's typed/global scopes."""
    from .scoped_topic_lookup import lookup_receipt
    result = lookup_receipt(scope, name, execution_evidence=True)
    result.update(binding='FastDDS_C++', domain_id=domain, local_gid=gid, role_kind=kind,
                  api='DomainParticipant::find_topic(name,timeout)', timeout_ns=0,
                  lookup_semantics='participant_local_name_with_independent_type_readback',
                  cleanup_confirmed=True,
                  limitations=['not_Cyclone_typeinfo_or_cross_scope_lookup',
                               'not_historical_binding_or_remote_discovery_validation'])
    if scope not in ('participant', 'local_domain', 'global') or not isinstance(name, str) or not 0 < len(name) <= 256 or '\0' in name:
        result.update(status='unknown', reason='topic_lookup_request_invalid')
        return result
    if scope != 'participant':
        result['reason'] = 'fastdds_find_topic_has_participant_local_scope_only'
        return result
    try:
        before = sample_ownership(observer, domain, gid, kind)
        _require(before.get('complete') is True, 'lookup_ownership_unverified')
        result['ownership_before'] = before
        with observer._lock:
            provenance = observer._native.revalidate()
            handle = observer.endpoint.handle
            with handle:
                pointer = observer._resolve(handle, kind)[2]
                result['cleanup_confirmed'] = False
                value = _call(observer._native, domain, pointer, kind, name)
                status = value.get('lookup_status')
                result['cleanup_confirmed'] = (status == 'not_found' and value.get('delete_topic_return_code') is None
                    or status == 'found' and type(value.get('delete_topic_return_code')) is int and value['delete_topic_return_code'] == 0)
                _require(value.get('local_handle') == pointer and value.get('local_gid') == gid
                         and value.get('role_kind') == kind, 'lookup_endpoint_identity_mismatch')
                _require(observer._native.revalidate() == provenance, 'lookup_provenance_changed')
        result.update(native_readback=value, provenance=provenance,
                      participant_handle=before['participant_handle'], participant_gid=before['participant_gid'])
        after = sample_ownership(observer, domain, gid, kind)
        result['ownership_after'] = after
        keys = ('local_handle', 'parent_handle', 'participant_handle', 'participant_gid', 'local_gid', 'domain_id', 'provenance')
        _require(after.get('complete') is True and all(before.get(k) == after.get(k) for k in keys)
                 and all(value.get(k) == before.get(k) for k in keys if k != 'provenance'), 'lookup_ownership_changed')
        status = value.get('lookup_status')
        _require(status in ('found', 'not_found'), 'lookup_status_invalid')
        result.update(status=status, complete=True, reason=None,
                      attribution='binding_observation', bound_type_name=value.get('source_type_name'),
                      actual_name=value.get('actual_name'), actual_type_name=value.get('actual_type_name'),
                      delete_topic_return_code=value.get('delete_topic_return_code'))
        if status == 'found':
            _require(isinstance(value.get('actual_name'), str) and isinstance(value.get('actual_type_name'), str), 'lookup_strings_missing')
            if value.get('delete_topic_return_code') != 0:
                result.update(status='tool_error', complete=False, cleanup_confirmed=False,
                              reason='topic_lookup_cleanup_unconfirmed', attribution='binding_adaptation_or_observation_error')
            elif (value.get('result_participant_handle') != before['participant_handle']
                  or value['actual_name'] != name or (name == value.get('source_name')
                  and value['actual_type_name'] != value.get('source_type_name'))):
                result.update(status='contract_mismatch', reason='topic_lookup_result_contract_mismatch',
                              attribution='type_or_scope_contract_requires_review')
        elif name == value.get('source_name'):
            result.update(status='contract_mismatch', reason='topic_lookup_existing_local_topic_not_found',
                          attribution='type_or_scope_contract_requires_review')
    except (ValueError, TypeError, KeyError, OSError, AttributeError, RuntimeError) as exc:
        result.update(status='unknown', complete=False, reason=str(exc), attribution='binding_adaptation_or_observation_error')
    return result
