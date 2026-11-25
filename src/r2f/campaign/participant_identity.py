"""Rebuild opt-in identity evidence from the shared SHA-bound topology raw log."""
from __future__ import annotations

import re

SCHEMA = 'r2f.participant-identity-audit/v1'
PROCESS_KEYS = ('pid', 'pgid', 'process_start_ticks')


def _require(value, reason):
    if not value:
        raise ValueError(reason)


def _guid(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{32}', value) is not None and value != '0' * 32


def _positive(value):
    return type(value) is int and value > 0


def _ownership(value, *, domain, gid=None):
    """Validate copied public API values, not the supporting receipt's flags."""
    _require(isinstance(value, dict) and value.get('schema') == 'r2f.cyclone-participant-ownership/v2'
        and value.get('complete') is True and value.get('status') == 'observed'
        and value.get('stable_sample_count') == 2 and value.get('domain_id') == domain,
        'independent_participant_ownership_incomplete')
    local, root = value.get('local_gid'), value.get('participant_gid')
    _require(_guid(local) and _guid(root) and local != root and (gid is None or gid == local),
             'independent_endpoint_guid_binding_mismatch')
    rows = value.get('entity_identities', [])
    for handle, guid in ((value.get('local_handle'), local), (value.get('participant_handle'), root)):
        matching = [r for r in rows if r.get('handle') == handle]
        _require(len(matching) == 1 and matching[0].get('gid') == guid
            and matching[0].get('domain_id') == domain and matching[0].get('guid_return_code') == 0,
            'ownership_inventory_entity_binding_mismatch')
    chain = value.get('parent_chain')
    _require(isinstance(chain, list) and len(chain) >= 2
        and chain[0] == value.get('local_handle') and chain[-1] == value.get('participant_handle')
        and len(set(chain)) == len(chain), 'ownership_parent_chain_incomplete')
    edges = dict(value.get('entity_inventory', []))
    _require(all(a in edges.get(b, []) for a, b in zip(chain, chain[1:])),
             'ownership_parent_chain_inventory_mismatch')
    observation = value.get('instance_identity', {})
    samples = observation.get('samples', [])
    _require(observation.get('schema') == 'r2f.cyclone-entity-instance-identity/v1'
        and observation.get('status') == 'observed'
        and observation.get('api') == 'dds_get_instance_handle+dds_get_guid'
        and observation.get('scope') == 'cyclonedds_entity_instance_id'
        and len(samples) == 2 and samples[0] == samples[1] and len(samples[0]) == 2,
        'independent_instance_identity_incomplete')
    for row, handle, guid in zip(samples[0], [value['participant_handle'], value['local_handle']], [root, local]):
        _require(row.get('entity_handle') == handle and row.get('guid') == guid
            and row.get('return_code') == 0 and row.get('guid_return_code') == 0
            and _positive(row.get('instance_id')) and row['instance_id'] < 2**64,
            'instance_id_guid_binding_mismatch')
    return root, local, samples[0][0]['instance_id'], samples[0][1]['instance_id']


def _library_binding(observation, initialization, fixture):
    if fixture:
        return
    from r2f.runtime.topology_worker import CycloneMatchedPeerObserver
    provenance = observation.get('provenance', {})
    allowed = CycloneMatchedPeerObserver.VERIFIED_C_ABIS
    _require((provenance.get('header_sha256'), provenance.get('library_sha256'),
              tuple(provenance.get('c_probe_layout', []))) in allowed
        and provenance.get('c_probe_sha256') == CycloneMatchedPeerObserver.C_ABI_PROBE_SHA256
        and provenance.get('c_probe_source_sha256') == CycloneMatchedPeerObserver.C_ABI_PROBE_SOURCE_SHA256
        and provenance.get('library') == initialization.get('library')
        and provenance.get('library_sha256') == initialization.get('library_sha256')
        and provenance.get('header_fingerprints', {}).get(initialization.get('version_header'))
            == initialization.get('version_header_sha256')
        and isinstance(initialization.get('version_declaration'), str)
        and '#define DDS_VERSION "0.10.5"' in initialization['version_declaration'],
        'identity_library_version_or_public_abi_provenance_unqualified')


def _phase_lifecycle(phase, previous, started, ended):
    _require(started < phase['started_monotonic_ns'] < phase['observed_monotonic_ns']
        < phase['closed_monotonic_ns'] < ended
        and (previous is None or previous['closed_monotonic_ns'] < phase['started_monotonic_ns']),
        'initialization_cross_generation_or_probe_chronology_invalid')
    participants = phase.get('participants', [])
    cleanups = phase.get('cleanup', [])
    _require(len(participants) == 2 and len(cleanups) == 2,
        'initialization_cleanup_inventory_incomplete')
    handles = []
    for row, cleanup in zip(participants, reversed(cleanups)):
        created, reader, ownership = row['participant_created'], row['reader_created'], row['ownership']
        root, local = created.get('participant_handle'), reader.get('reader_handle')
        _require(_positive(root) and _positive(local) and root != local,
            'initialization_entity_handles_invalid')
        handles += [root, local]
        _require(created.get('receipt_kind') == 'native_participant_created'
            and reader.get('receipt_kind') == 'native_builtin_reader_created'
            and created.get('real_execution') is True and reader.get('real_execution') is True
            and created.get('guid_return_code') == 0, 'initialization_creation_receipts_incomplete')
        _require(cleanup.get('receipt_kind') == 'native_participant_closed'
            and cleanup.get('cleanup_complete') is True and cleanup.get('failures') == []
            and cleanup.get('deleted') == [{'kind': 'builtin_reader', 'handle': local},
                                          {'kind': 'participant', 'handle': root}],
            'initialization_deletion_receipt_failed_or_mismatched')
        iid = ownership['instance_identity']
        times = [phase['started_monotonic_ns'], created['created_monotonic_ns'], created['monotonic_ns'],
                 reader['monotonic_ns'], ownership['started_monotonic_ns'], iid['started_monotonic_ns'],
                 iid['ended_monotonic_ns'], ownership['ended_monotonic_ns'], phase['observed_monotonic_ns'],
                 cleanup['monotonic_ns'], phase['closed_monotonic_ns']]
        _require(all(type(t) is int and t > 0 for t in times)
            and all(a <= b for a, b in zip(times, times[1:])),
            'initialization_creation_observation_deletion_chronology_invalid')
    _require(len(set(handles)) == 4, 'initialization_live_entity_handles_aliased')



def _fork_rendezvous(value, child):
    rows = value.get('rendezvous', [])
    _require(isinstance(rows, list) and [r.get('generation') for r in rows] == [1, 2],
             'initialization_fork_rendezvous_missing_or_duplicate')
    parent_phases, child_phases = [p['phases'] for p in value['processes']]
    for receipt, parent, peer in zip(rows, parent_phases, child_phases):
        snapshot = {k: v for k, v in peer.items() if k != 'closed_monotonic_ns'}
        snapshot = {**snapshot, 'cleanup': [], 'cleanup_complete': False}
        _require(receipt.get('child_identity') == child and receipt.get('child_phase') == snapshot
            and receipt.get('parent_observed_monotonic_ns') == parent['observed_monotonic_ns'],
            'initialization_fork_rendezvous_branch_binding_mismatch')
        observed = max(parent['observed_monotonic_ns'], peer['observed_monotonic_ns'])
        first_delete = min(c['monotonic_ns'] for p in (parent, peer) for c in p['cleanup'])
        received, released = receipt.get('received_monotonic_ns'), receipt.get('release_monotonic_ns')
        _require(type(received) is int and type(released) is int
            and observed <= received <= released < first_delete,
            'initialization_fork_entity_lifetimes_not_concurrent')


def audit_participant_identity(audit):
    contract = audit.program.get('identity_checks')
    if contract is None:
        return None
    result = {'schema': SCHEMA, 'contract': dict(contract), 'status': 'unknown',
              'observations': [], 'initializations': [], 'conflicts': [], 'unknown_reasons': [],
              'fixture': audit.fixture, 'candidate_eligible': False,
              'limitations': ['bounded_observations_not_exhaustive_uniqueness',
                  'no_wire_iid_or_implicit_participant_generation',
                  'fork_after_participant_unsupported', 'historical_pre_post_not_reproduced']}
    gids, iids = {}, {}

    def register(guid, iid, entity, raw_ref, subject):
        for table, value, kind in ((gids, guid, 'guid'), (iids, iid, 'iid')):
            members = table.setdefault(value, [])
            if any(previous['entity'] == entity for previous in members):
                continue
            current = {'entity': entity, 'raw_ref': raw_ref, 'subject': subject}
            # All logical pairs, rather than a PID-selected representative/star.
            for previous in members:
                result['conflicts'].append({'kind': kind + '_collision', 'value': value,
                    'first': previous, 'second': current})
            members.append(current)

    tokens = audit.tokens
    owners = {(t['pid'], t['process_start_ticks']) for t in tokens.values()}
    initializations = [e for e in audit.events if e['event'] == 'participant_identity_initialization']
    if not initializations:
        result['unknown_reasons'].append('initialization_observation_missing')
    for owner in sorted(owners):
        rows = [e for e in initializations if (e.get('pid'), e.get('process_start_ticks')) == owner]
        try:
            _require(len(rows) == 1, 'initialization_observation_missing_or_duplicate')
            event = rows[0]
            value = event.get('observation', {})
            raw_ref = audit.ref(event)
            own_tokens = [t for t in tokens.values() if (t['pid'], t['process_start_ticks']) == owner]
            owner_endpoints = sorted([{'logical_id': t['logical_id'], 'generation': t['generation']}
                for t in own_tokens], key=lambda t: (t['logical_id'], t['generation']))
            _require(event['run_id'] == audit.config['run_id'] and event['domain_id'] == audit.config['domain_id']
                and all(event['event_index'] < t['event_index'] and event['monotonic_ns'] < t['monotonic_ns']
                        and event.get('pgid') == t.get('pgid') for t in own_tokens),
                'initialization_worker_chronology_or_identity_mismatch')
            _require(value.get('schema') == 'r2f.participant-identity-initialization/v1'
                and value.get('contract') == contract and value.get('status') == 'observed'
                and value.get('cleanup_complete') is True, 'initialization_observation_incomplete')
            _require(value['started_monotonic_ns'] < value['ended_monotonic_ns'] <= event['monotonic_ns'],
                'initialization_probe_envelope_chronology_invalid')
            before = value.get('before_initialization', {})
            _require(before.get('local_participant_count') == 0 and before.get('thread_count') == 1
                and before.get('library_preloaded') is True
                and value['started_monotonic_ns'] < before['monotonic_ns'] < value['ended_monotonic_ns']
                and all(before.get('process', {}).get(k) == event.get(k) for k in PROCESS_KEYS),
                'initialization_not_fresh_single_threaded_worker')
            processes = value.get('processes', [])
            count = 2 if contract['initialization'] == 'fork_before_participant' else 1
            _require(len(processes) == count and len({(p['identity']['pid'], p['identity']['process_start_ticks'])
                for p in processes}) == count, 'initialization_process_inventory_invalid')
            _require(all(processes[0]['identity'].get(k) == event.get(k) for k in PROCESS_KEYS),
                'initialization_parent_binding_mismatch')
            if count == 2:
                child = value.get('child_kernel_identity', {})
                _require(processes[1].get('identity') == child and child.get('ppid') == event['pid']
                    and child.get('pgid') == event['pgid'] and value.get('child_reaped') is True
                    and value.get('child_returncode') == 0 and not value.get('child_forced_cleanup'),
                    'initialization_child_kernel_or_cleanup_mismatch')
            pending_identities = []
            for process_index, process in enumerate(processes):
                identity = process['identity']
                _require(all(_positive(identity.get(k)) for k in PROCESS_KEYS)
                    and process.get('cleanup_complete') is True, 'initialization_process_identity_or_cleanup_unknown')
                _require([p.get('generation') for p in process.get('phases', [])] == [1, 2],
                    'initialization_lifecycle_generations_incomplete')
                previous = None
                for phase in process['phases']:
                    _phase_lifecycle(phase, previous, before['monotonic_ns'], value['ended_monotonic_ns'])
                    previous = phase
                    _require(phase.get('cleanup_complete') is True and 'error' not in phase
                        and phase['started_monotonic_ns'] < phase['observed_monotonic_ns'] < phase['closed_monotonic_ns'],
                        'initialization_lifecycle_chronology_or_cleanup_unknown')
                    participants = phase.get('participants', [])
                    _require(len(participants) == 2 and [p.get('slot') for p in participants] == [0, 1],
                        'initialization_participant_inventory_incomplete')
                    for row in participants:
                        _library_binding(row.get('ownership', {}), value.get('provenance', {}), audit.fixture)
                        root, endpoint, rid, eid = _ownership(row.get('ownership'), domain=event['domain_id'])
                        created, reader = row['participant_created'], row['reader_created']
                        _require(created.get('guid') == root and created.get('domain_id') == event['domain_id']
                            and created.get('participant_handle') == row['ownership']['participant_handle']
                            and reader.get('reader_handle') == row['ownership']['local_handle'],
                            'initialization_create_to_observation_mismatch')
                        entity = [identity['pid'], identity['process_start_ticks'], phase['generation'], row['slot']]
                        subject = {'owner_endpoints': owner_endpoints, 'scope': 'initialization',
                            'branch': 'parent' if process_index == 0 else 'child',
                            'generation': phase['generation'], 'slot': row['slot']}
                        pending_identities.append((root, rid, entity + ['participant'], raw_ref, {**subject, 'kind': 'participant'}))
                        pending_identities.append((endpoint, eid, entity + ['reader'], raw_ref, {**subject, 'kind': 'reader'}))
            if count == 2:
                _fork_rendezvous(value, child)
            for registered in pending_identities:
                register(*registered)
            result['initializations'].append({'process': owner, 'raw_ref': raw_ref,
                'process_count': count, 'generations_per_process': 2, 'participants_per_generation': 2,
                'provenance': value.get('provenance'), 'cleanup_complete': True})
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            result['unknown_reasons'].append(str(exc))
    from r2f.runtime.endpoint_incarnation import endpoint_identity_at
    observed, required = set(), set()
    for key, token in tokens.items():
        try:
            current = endpoint_identity_at(audit.events, token)
            required.update((*key, ordinal) for ordinal in range(current['incarnation_ordinal'] + 1))
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            result['unknown_reasons'].append(str(exc))
    by_owner = {(tuple(r['process'])): r for r in result['initializations']}
    for event in [e for e in audit.events if e['event'] == 'participant_ownership_snapshot']:
        try:
            key = (event.get('logical_id'), event.get('generation'))
            _require(key in tokens, 'ownership_generation_unknown')
            audit.bound(event, tokens[key])
            current = endpoint_identity_at(audit.events, tokens[key], before_index=event['event_index'] + 1)
            ordinal = current['incarnation_ordinal']
            value = event.get('participant_ownership_observation', {})
            initialization = by_owner.get((event.get('pid'), event.get('process_start_ticks')), {})
            _library_binding(value, initialization.get('provenance', {}), audit.fixture)
            root, endpoint, rid, eid = _ownership(value, domain=audit.config['domain_id'], gid=event.get('endpoint_gid'))
            identity = [event[k] for k in PROCESS_KEYS]
            raw_ref = audit.ref(event)
            # A root may own multiple endpoints and survive their recreation.
            root_entity = identity + ['participant', value['participant_handle']]
            owner_endpoints = sorted([{'logical_id': t['logical_id'], 'generation': t['generation']}
                for t in tokens.values() if all(t.get(k) == event.get(k) for k in PROCESS_KEYS)],
                key=lambda t: (t['logical_id'], t['generation']))
            register(root, rid, root_entity, raw_ref,
                {'owner_endpoints': owner_endpoints, 'scope': 'ros_context', 'kind': 'participant'})
            register(endpoint, eid, identity + [*key, event['endpoint_generation'], ordinal], raw_ref,
                {'logical_id': key[0], 'generation': key[1], 'incarnation_ordinal': ordinal,
                 'scope': 'ros_context', 'kind': 'endpoint'})
            observed.add((*key, ordinal))
            result['observations'].append({'logical_id': key[0], 'generation': key[1],
                'endpoint_generation': event['endpoint_generation'], 'incarnation_ordinal': ordinal, 'process': dict(zip(PROCESS_KEYS, identity)),
                'participant_guid': root, 'endpoint_guid': endpoint, 'participant_iid': rid, 'endpoint_iid': eid,
                'raw_ref': raw_ref, 'provenance': value.get('provenance')})
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            result['unknown_reasons'].append(str(exc))
    if observed != required:
        result['unknown_reasons'].append('endpoint_generation_independent_identity_missing')
    if len(initializations) != len(owners):
        result['unknown_reasons'].append('initialization_owner_inventory_mismatch')
    result['unknown_reasons'] = sorted(set(result['unknown_reasons']))
    if result['conflicts']:
        result['status'] = 'conflict'
    elif not result['unknown_reasons']:
        result['status'] = 'consistent'
    return result


class _RawIdentityTrace:
    """The same artifact boundary for candidate intake and replay comparison."""
    def __init__(self, events, program, config, raw_path, raw_sha256):
        from r2f.core.evidence import canonical_hash
        self.events, self.program, self.config = events, program, config
        self.path, self.sha = raw_path, raw_sha256
        _require(bool(events) and all(e.get('event_index') == i and e.get('run_id') == config['run_id']
            and e.get('domain_id') == config['domain_id'] for i, e in enumerate(events)),
            'identity_raw_run_domain_or_index_mismatch')
        bindings = [e for e in events if e['event'] == 'input_binding']
        _require(len(bindings) == 1 and bindings[0].get('receipt', {}).get('program_sha256') == canonical_hash(program),
            'identity_raw_program_binding_mismatch')
        self.fixture = any(e.get('fixture') is True or e.get('offline_fake') is True
            or e.get('backend') not in (None, 'ros2_public_api') for e in events)
        created = [e for e in events if e['event'] == 'endpoint_created']
        self.tokens = {(e['logical_id'], e['generation']): e for e in created}
        _require(len(self.tokens) == len(created), 'identity_raw_endpoint_generation_ambiguous')

    def ref(self, event):
        return {'path': self.path, 'sha256': self.sha, 'event_index': event['event_index']}

    def bound(self, event, token):
        from r2f.runtime.endpoint_incarnation import endpoint_identity_at
        token = endpoint_identity_at(self.events, token, before_index=event['event_index'] + 1)
        _require(event['event_index'] > token['identity_valid_after_ref']
            and event['monotonic_ns'] >= token['identity_valid_after_ns']
            and all(event.get(k) == token.get(k) for k in
                    ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation', 'endpoint_gid')),
            'identity_raw_endpoint_process_generation_mismatch')


def rebuild_participant_identity(events, program, config, raw_path, raw_sha256):
    """Reopen raw evidence; recorded typed-fact summaries are never the input."""
    return audit_participant_identity(_RawIdentityTrace(events, program, config, raw_path, raw_sha256))
