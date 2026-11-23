"""Opt-in bounded ROS groups: pure input and raw-lineage contracts, no runtime owner.

participant_id is a group label. Neither allocation nor this projection proves
DDS ownership. Only supporting root captures can qualify a measured relation.
"""
from copy import deepcopy
import re

CONTROL = {'schema': 'r2f.ros-participant-control/v1', 'mechanism': 'process_context_per_group'}
LINEAGE_SCHEMA = 'r2f.ros-participant-group-lineage/v1'
OBSERVATION_SCHEMA = 'r2f.ros-participant-group-observation/v1'
LINK_KEYS = ('group_id', 'worker_instance_id', 'context_instance_id')
PROCESS_KEYS = ('pid', 'pgid', 'process_start_ticks', 'worker_instance_id')
ENDPOINT_KEYS = ('logical_id', 'generation', 'endpoint_generation', 'endpoint_gid')

def _endpoint_event_roles_match(event, alias):
    """Keep recorder actor provenance separate from exact endpoint aliases.

    Group transport requests and group-owner token-only replies inherit the
    topology recorder's actor default. Endpoint worker replies override it.
    All present worker_role fields still authenticate the addressed endpoint.
    """
    if 'worker_role' in event and event['worker_role'] != alias:
        return False
    if 'role' not in event or event['role'] == alias:
        return True
    kind = event.get('event')
    if event['role'] != 'topology_controller':
        return False
    if kind == 'group_command_requested':
        return event.get('command') in {'create_endpoint', 'destroy_endpoint', 'snapshot', 'publish_batch', 'group_snapshot'}
    if kind == 'topology_snapshot_requested':
        return event.get('command') == 'snapshot'
    if kind == 'participant_group_snapshot':
        return event.get('worker_role') == alias
    return (kind == 'command_done' and event.get('command') in {'create_endpoint', 'destroy_endpoint', 'group_snapshot'}
            and event.get('worker_role') == alias)


def enabled(program):
    return isinstance(program, dict) and 'participant_control' in program


def validate_control(program, config=None):
    """Return declared groups or fail before any resources are acquired."""
    if not enabled(program):
        return None
    if program['participant_control'] != CONTROL:
        raise ValueError('participant_control_tag_invalid')
    eps, actions = program.get('endpoints'), program.get('actions')
    if (not isinstance(eps, list) or not 2 <= len(eps) <= 8
            or any(not isinstance(e, dict) or e.get('role') not in {'writer', 'reader'} for e in eps)
            or any(not 1 <= sum(e['role'] == role for e in eps) <= 4 for role in ('writer', 'reader'))):
        raise ValueError('participant_control_scope:requires_1_to_4_writers_and_readers')
    ids = [e.get('logical_id') for e in eps]
    if (any(not isinstance(eid, str) or not eid.strip() for eid in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError('participant_control_scope:unique_endpoint_ids_required')
    groups = {}
    for ep in eps:
        label = ep.get('participant_id')
        if not isinstance(label, str) or not label.strip() or len(label) > 256:
            raise ValueError('participant_control_scope:group_label_required')
        if ep.get('filter') is not None or ep.get('type_name') != 'std_msgs/msg/String':
            raise ValueError('participant_control_scope:filter_or_type')
        groups.setdefault(label, []).append(ep['logical_id'])
    if (any(e.get('topic') != eps[0].get('topic') for e in eps)
            or not isinstance(eps[0].get('topic'), str) or not eps[0]['topic']
            or 'episode' in program):
        raise ValueError('participant_control_scope:topic_or_episode')
    if not isinstance(actions, list):
        raise ValueError('participant_control_scope:actions')
    creates = []
    generations, live = {}, set()
    for a in actions:
        if (not isinstance(a, dict) or a.get('op') not in {'create', 'destroy', 'recreate', 'snapshot', 'barrier', 'publish_batch'}
                or 'concurrency_group' in (a.get('params') or {})):
            raise ValueError('participant_control_scope:unsupported_action')
        op, eid, gen = a['op'], a.get('endpoint_id'), a.get('generation')
        if op == 'publish_batch' and set(creates) != set(ids):
            raise ValueError('participant_control_scope:publish_requires_both_creates')
        if op in {'create', 'recreate', 'destroy'}:
            if eid not in ids or type(gen) is not int:
                raise ValueError('participant_control_scope:generation')
            if op == 'create':
                if gen != 1 or eid in generations:
                    raise ValueError('participant_control_scope:generation')
                creates.append(eid)
            elif op == 'recreate':
                if eid not in generations or gen != generations[eid] + 1:
                    raise ValueError('participant_control_scope:generation')
            elif eid not in live or gen != generations[eid]:
                raise ValueError('participant_control_scope:generation')
            if op == 'destroy':
                live.remove(eid)
            else:
                generations[eid] = gen
                live.add(eid)
    if sorted(creates) != sorted(ids):
        raise ValueError('participant_control_scope:exactly_one_create_per_endpoint')
    if config is not None:
        if (config.get('rmw', 'rmw_cyclonedds_cpp') != 'rmw_cyclonedds_cpp'
                or config.get('observation_mode', 'blackbox') != 'blackbox'
                or config.get('environment_mutation') is not None
                or config.get('fastdds_peer_instrumentation') is not None):
            raise ValueError('participant_control_scope:rmw_mode_or_environment')
    return groups


def qualify_observation(observation, tokens):
    """Derive supporting status from captures; never trust supplied status flags."""
    from .topology_worker import worker_role_kind
    reasons = []
    before, after = observation.get('roots_before'), observation.get('roots_after')
    def roots(capture):
        if not isinstance(capture, dict) or capture.get('complete') is not True:
            raise ValueError('local_root_inventory_incomplete')
        if not isinstance(capture.get('provenance'), dict) or not capture['provenance']:
            raise ValueError('local_root_provenance_missing')
        rows = capture['roots']
        gids = [r['participant_gid'] for r in rows]
        if (not rows or len(set(gids)) != len(rows)
                or capture.get('count') != len(rows)
                or any(not isinstance(g, str) or re.fullmatch(r'[0-9a-f]{32}', g) is None or g == '0' * 32 for g in gids)
                or any(r.get('domain_id') != capture.get('domain_id') or r.get('valid') is not True for r in rows)):
            raise ValueError('local_root_inventory_identity_invalid')
        handles = [r['participant_handle'] for r in rows]
        if len(set(handles)) != len(handles):
            raise ValueError('local_root_handle_reuse')
        for r in rows:
            h, parent = r['participant_handle'], r.get('parent')
            if (type(h) is not int or h <= 0 or r.get('participant_self_return') != h
                    or r.get('domain_return_code') != 0
                    or not (type(parent) is int and (parent == 0 or (parent > 0 and parent not in handles
                        and r.get('parent_domain_return_code') == 0
                        and r.get('parent_domain_id') == capture['domain_id']
                        and r.get('parent_participant_return') == 0 and h in r.get('parent_children', []))))):
                raise ValueError('local_root_parent_or_self_invalid')
        return rows
    root_complete = ownership = relation = False
    count = None
    try:
        rb, ra = roots(before), roots(after)
        count = len(rb)
        if (rb != ra or before.get('domain_id') != after.get('domain_id')
                or before['provenance'] != after['provenance']):
            raise ValueError('local_root_inventory_changed')
        root_complete = True
        rows = observation.get('endpoints')
        if not isinstance(rows, list) or len(rows) != len(tokens):
            raise ValueError('group_endpoint_capture_coverage')
        seen, participant_gids = set(), set()
        for row in rows:
            matching = [t for t in tokens if all(row.get(k) == t.get(k) for k in ENDPOINT_KEYS)]
            if len(matching) != 1 or row['logical_id'] in seen:
                raise ValueError('group_endpoint_capture_identity')
            expected_alias = matching[0].get('worker_role', matching[0].get('role'))
            if any(k in row and row[k] != expected_alias for k in ('role', 'worker_role')):
                raise ValueError('group_endpoint_capture_role_alias')
            seen.add(row['logical_id'])
            samples = row.get('samples')
            if not isinstance(samples, list) or len(samples) != 2:
                raise ValueError('group_ownership_double_sample_missing')
            stable_keys = ('local_handle','participant_handle','participant_gid','parent_chain','entity_inventory','entity_identities','participant_root_observation','provenance')
            for sample in samples:
                if (sample.get('schema') != 'r2f.cyclone-participant-ownership/v2'
                        or sample.get('complete') is not True or sample.get('status') != 'observed'
                        or sample.get('local_gid') != row['endpoint_gid']
                        or sample.get('role_kind') != worker_role_kind(matching[0].get('role', matching[0].get('kind')))
                        or sample.get('provenance') != before['provenance']
                        or sample.get('domain_id') != before.get('domain_id')
                        or sample.get('stable_sample_count') != 2
                        or sample.get('candidate_eligible') is not False
                        or any(sample.get(k) is None for k in stable_keys)):
                    raise ValueError('group_ownership_sample_invalid')
                root = sample['participant_root_observation']
                if (sample['parent_chain'][0] != sample['local_handle']
                        or sample['parent_chain'][-1] != sample['participant_handle']
                        or root.get('participant_handle') != sample['participant_handle']
                        or root.get('participant_self_return') != sample['participant_handle']
                        or sample['participant_handle'] not in root.get('lookup_participants', [])):
                    raise ValueError('group_ownership_root_invalid')
                local = [r for r in sample['entity_identities'] if r.get('handle') == sample['local_handle']]
                # Existing v2 uses entity_handle, retained verbatim.
                if not local:
                    local = [r for r in sample['entity_identities'] if r.get('entity_handle') == sample['local_handle']]
                roots_in_sample = [r for r in sample['entity_identities'] if r.get('handle') == sample['participant_handle']]
                if (len(roots_in_sample) != 1 or roots_in_sample[0].get('gid') != sample['participant_gid']):
                    raise ValueError('group_ownership_root_guid_invalid')
                if len(local) != 1 or local[0].get('gid') != row['endpoint_gid']:
                    raise ValueError('group_ownership_endpoint_guid_invalid')
            if any(samples[0][k] != samples[1][k] for k in stable_keys):
                raise ValueError('group_ownership_samples_changed')
            participant_gids.add(samples[0]['participant_gid'])
        ownership = bool(tokens)
        if count != 1:
            raise ValueError('unexpected_local_participant_roots')
        if participant_gids != {rb[0]['participant_gid']}:
            raise ValueError('group_relation_root_mismatch')
        relation = True
    except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
        reasons.append(str(exc))
    return {'endpoint_ownership_observed': ownership, 'local_root_inventory_complete': root_complete,
            'group_relation_observed': relation, 'actual_local_root_count': count,
            'unknown_reasons': reasons}



def _validate_shared_process(tokens, events, *, request_index, before_index):
    """Authenticate a live bounded PID share using only already observed raw lineage.

    This is the process/context identity part of the group lineage contract,
    bounded at a fresh endpoint snapshot. It grants no DDS participant ownership
    or sharing: those remain independently qualified by qualify_observation.
    No future closure, group label alone, or GUID prefix supplies identity.
    """
    def require(ok, reason):
        if not ok:
            raise ValueError('group_lineage:' + reason)
    def one(rows, reason):
        require(len(rows) == 1, reason)
        return rows[0]
    def equal(row, other, keys):
        return all(row.get(k) is not None and row.get(k) == other.get(k) for k in keys)
    def ordered(*rows):
        return all(a['event_index'] < b['event_index']
                   and type(a.get('monotonic_ns')) is int
                   and type(b.get('monotonic_ns')) is int
                   and a['monotonic_ns'] <= b['monotonic_ns']
                   for a, b in zip(rows, rows[1:]))

    from .topology_worker import worker_role_kind, worker_role_for
    raw = events[:before_index]
    require(all(e.get('event_index') == i for i, e in enumerate(raw)), 'event_offsets')
    legacy_pair = len(tokens) == 2 and {t.get('role') for t in tokens} == {'writer', 'reader'}
    allowed_aliases = {worker_role_for(kind, i) for kind in ('writer', 'reader') for i in range(4)}
    require(2 <= len(tokens) <= 8
            and all(t.get('role') in allowed_aliases
                    and t.get('worker_role', t.get('role') if legacy_pair else None) == t.get('role')
                    for t in tokens)
            and len({t['role'] for t in tokens}) == len(tokens)
            and all(sum(worker_role_kind(t['role']) == role for t in tokens) <= 4 for role in ('writer', 'reader'))
            and len({t.get('logical_id') for t in tokens}) == len(tokens),
            'shared_process_scope')
    token = tokens[0]
    keys = PROCESS_KEYS + LINK_KEYS
    require(all(equal(t, token, keys) for t in tokens), 'endpoint_process_context_link')
    require(all(isinstance(token[k], str) and bool(token[k].strip()) for k in LINK_KEYS),
            'process_context_labels')
    require(all(type(token[k]) is int and token[k] > 0
                for k in ('pid', 'pgid', 'process_start_ticks'))
            and token['pid'] == token['pgid']
            and token['worker_instance_id'] != token['context_instance_id'], 'process_identity')
    binding = one([e for e in raw if e.get('event') == 'input_binding'], 'input_binding_count')
    require(binding.get('receipt', {}).get('participant_control') == CONTROL,
            'group_lineage_requires_explicit_input')
    # Inspect the entire process/group scope, so a foreign worker/context cannot
    # disappear by filtering it out before checking its identity.
    worker_events = {'process_started', 'process_exit', 'group_context_created',
                     'group_worker_ready', 'group_context_shutdown', 'endpoint_created',
                     'endpoint_group_bound', 'endpoint_destroyed', 'worker_exit',
                     'group_command_requested', 'command_done', 'participant_group_snapshot',
                     'topology_snapshot_requested', 'match_snapshot', 'graph_snapshot'}
    scoped = [e for e in raw if e.get('group_id') == token['group_id']
              or (e.get('event') in worker_events and e.get('pid') == token['pid'])]
    history = [e for e in scoped if e.get('event') == 'endpoint_created']
    require(len({(t.get('logical_id'), t.get('generation')) for t in history}) == len(history)
            and all(len({t.get(k) for t in history}) == len(history) for k in ('endpoint_gid', 'endpoint_generation')),
            'endpoint_reuse')
    for t in history:
        require(t.get('role') in allowed_aliases and t.get('worker_role', t.get('role') if legacy_pair else None) == t['role'],
                'endpoint_event_role_alias')
        same = [h for h in history if h.get('logical_id') == t.get('logical_id')]
        require(all(h['role'] == t['role'] for h in same)
                and sorted(h.get('generation') for h in same) == list(range(1, len(same)+1)), 'historical_incarnation_sequence')
    for e in scoped:
        require(equal(e, token, keys + ('run_id', 'domain_id')), 'raw_process_context_link')
        if e.get('logical_id') is not None:
            addressed = one([t for t in history if equal(t, e, ENDPOINT_KEYS[:2])],
                            'endpoint_event_address')
            address_keys = ENDPOINT_KEYS[:2]
            if not (e.get('event') == 'group_command_requested'
                    and e.get('command') == 'create_endpoint'):
                address_keys += ('endpoint_generation',)
            require(equal(e, addressed, address_keys), 'endpoint_event_context_or_incarnation')
            require(_endpoint_event_roles_match(e, addressed['role']),
                    'endpoint_event_role_alias')
    start = one([e for e in scoped if e.get('event') == 'process_started'], 'group_start')
    ctx = one([e for e in scoped if e.get('event') == 'group_context_created'], 'context_created')
    ready = one([e for e in scoped if e.get('event') == 'group_worker_ready'], 'group_ready')
    require(start.get('fresh_process') is True and start.get('separate_context') is False
            and not any(k in start for k in ENDPOINT_KEYS), 'process_identity')
    require(ordered(binding, start, ctx, ready) and ctx.get('ros_context_created') is True
            and ctx.get('api') == 'rclpy.context.Context+rclpy.init'
            and bool(ctx.get('rmw_identifier'))
            and (token.get('rmw_identifier') is None
                 or ctx['rmw_identifier'] == token['rmw_identifier'])
            and ctx.get('observed_domain_id') == token['domain_id'], 'context_chronology')
    require(not any(e.get('event') in {'group_context_shutdown', 'process_exit', 'worker_exit'} for e in scoped),
            'shared_process_retired')
    requests = [e for e in scoped if e.get('event') == 'group_command_requested']
    require([r.get('command_id') for r in requests] == list(range(1, len(requests)+1)),
            'process_command_serial')
    bindings = []
    live_history = []
    for t in history:
        bound = one([e for e in scoped if e.get('event') == 'endpoint_group_bound'
                     and equal(e.get('endpoint', {}), t, ENDPOINT_KEYS)], 'endpoint_binding')
        req = one([r for r in requests if r.get('command') == 'create_endpoint'
                   and equal(r, t, ENDPOINT_KEYS[:2])], 'create_request')
        ack = one([e for e in scoped if e.get('event') == 'command_done'
                   and e.get('command_id') == req.get('command_id')], 'create_ack')
        require(ack.get('command') == 'create_endpoint'
                and equal(ack, t, ENDPOINT_KEYS) and ordered(ready, req, t, bound, ack),
                'create_request_binding_ack_order')
        cleanups = [e for e in scoped if e.get('event') == 'endpoint_destroyed' and equal(e, t, ENDPOINT_KEYS)]
        if cleanups:
            cleanup = one(cleanups, 'endpoint_cleanup')
            destroy = one([r for r in requests if r.get('command') == 'destroy_endpoint'
                           and equal(r, t, ENDPOINT_KEYS[:3])], 'destroy_request')
            done = one([e for e in scoped if e.get('event') == 'command_done'
                        and e.get('command_id') == destroy.get('command_id')], 'destroy_ack')
            require(cleanup.get('cleanup_status') == done.get('cleanup_status') == 'closed'
                    and done.get('command') == 'destroy_endpoint' and equal(done, t, ENDPOINT_KEYS)
                    and ordered(ack, destroy, cleanup, done), 'destroy_request_cleanup_ack_order')
            for e in scoped:
                if e.get('event') in {'send', 'receive', 'match_snapshot', 'graph_snapshot', 'match'} and equal(e, t, ENDPOINT_KEYS[:3]):
                    require(e['event_index'] < cleanup['event_index'], 'retired_endpoint_activity')
        else:
            live_history.append(t)
        if t['generation'] > 1:
            prior = one([h for h in history if h['logical_id'] == t['logical_id'] and h['generation'] == t['generation'] - 1], 'predecessor_token')
            retirement = one([e for e in scoped if e.get('event') == 'command_done'
                              and e.get('command') == 'destroy_endpoint' and equal(e, prior, ENDPOINT_KEYS)], 'predecessor_destroy_ack')
            require(ordered(retirement, req), 'predecessor_retirement_order')
        bindings.append(ack)
    require(len(live_history) == len(tokens) and all(any(equal(t, h, ENDPOINT_KEYS) for h in live_history) for t in tokens),
            'live_incarnation_inventory')
    request = events[request_index]
    require(request.get('event') in {'topology_snapshot_requested', 'worker_command_requested'}
            and request.get('command') == 'snapshot'
            and all(ordered(b, request) for b in bindings), 'snapshot_command_chronology')
    if request.get('pid') != token['pid']:
        # The caller audits every shared process in the current raw universe.
        # This group's complete create/binding ACKs must precede the other
        # process's request. Its own snapshot chain is checked when queried;
        # do not manufacture a command addressed to this group.
        require(request.get('group_id') != token['group_id'], 'raw_process_context_link')
        return
    req = one([r for r in requests if r.get('command_id') == request.get('command_id')],
              'snapshot_request')
    ack = one([e for e in scoped if e.get('event') == 'command_done'
               and e.get('command_id') == req.get('command_id')], 'snapshot_ack')
    require(req.get('command') == request.get('command') == 'snapshot'
            and equal(req, request, ENDPOINT_KEYS[:3])
            and equal(ack, request, ENDPOINT_KEYS[:3])
            and ordered(req, request, ack)
            and all(ordered(b, req) for b in bindings), 'snapshot_command_chronology')


def project_lineage(program, events, raw_path, *, config=None):
    """Validate complete opt-in process/context/endpoint raw and project metadata.

    Caller owns SHA/path authentication and exact sealed input selection. A
    malformed group trace never falls back to legacy or relaxes a legacy gate.
    """
    import hashlib
    import json
    from .topology_worker import canonical_worker_roles
    groups = validate_control(program, config)
    if groups is None:
        raise ValueError('group_lineage_requires_explicit_input')
    aliases = canonical_worker_roles(program['endpoints'])
    def require(ok, reason):
        if not ok:
            raise ValueError('group_lineage:' + reason)
    def one(rows, reason):
        require(len(rows) == 1, reason)
        return rows[0]
    def ref(e):
        return {'raw_path': str(raw_path), 'event_index': e['event_index']}
    def equal(e, token, keys):
        return all(e.get(k) == token.get(k) for k in keys)
    require(all(isinstance(e, dict) and e.get('event_index') == i for i,e in enumerate(events)), 'event_offsets')
    bindings = [e for e in events if e.get('event') == 'input_binding']
    binding = one(bindings, 'input_binding_count')
    digest = hashlib.sha256(json.dumps(program, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    require(binding.get('receipt', {}).get('program_sha256') == digest
            and binding.get('receipt', {}).get('participant_control') == CONTROL, 'exact_program_binding')
    tokens = [e for e in events if e.get('event') == 'endpoint_created']
    order = [(a['endpoint_id'], a['generation']) for a in program['actions'] if a['op'] in {'create', 'recreate'}]
    require(len(tokens) == len(order) and {t.get('logical_id') for t in tokens} == {e['logical_id'] for e in program['endpoints']}, 'endpoint_coverage')
    require([(t.get('logical_id'), t.get('generation')) for t in tokens] == order, 'lazy_create_order')
    cleanups = {}
    for token in tokens:
        cleanup = one([e for e in events if e.get('event') == 'endpoint_destroyed'
                       and equal(e, token, ENDPOINT_KEYS)], 'endpoint_cleanup')
        cleanups[(token['logical_id'], token['generation'])] = cleanup
    expected_destroys, live = [], {}
    for action in program['actions']:
        eid, op = action.get('endpoint_id'), action['op']
        if op == 'destroy' or (op == 'recreate' and eid in live):
            expected_destroys.append((eid, live.pop(eid)))
        if op in {'create', 'recreate'}:
            live[eid] = action['generation']
    require([(e.get('logical_id'), e.get('generation')) for e in events
             if e.get('event') == 'group_command_requested' and e.get('command') == 'destroy_endpoint']
            == expected_destroys, 'authored_destroy_coverage')
    require(len({t.get('endpoint_gid') for t in tokens}) == len(tokens)
            and len({t.get('endpoint_generation') for t in tokens}) == len(tokens), 'endpoint_reuse')
    starts = [e for e in events if e.get('event') == 'process_started']
    exits = [e for e in events if e.get('event') == 'process_exit']
    require(len(starts) == len(exits) == len(groups), 'actual_process_inventory_count')
    for key in ('pid', 'worker_instance_id', 'context_instance_id'):
        require(len({e.get(key) for e in starts}) == len(groups), 'process_or_context_reuse:' + key)
    processes, contexts, workers, observations = [], [], [], []
    all_gids = set()
    for label, ids in groups.items():
        st = one([e for e in starts if e.get('group_id') == label], 'group_start')
        require(all(st.get(k) is not None for k in PROCESS_KEYS + LINK_KEYS)
                and all(type(st[k]) is int and st[k] > 0 for k in ('pid','pgid','process_start_ticks'))
                and st['pid'] == st['pgid'] and st.get('fresh_process') is True
                and st.get('separate_context') is False, 'process_identity')
        require(not any(k in st for k in ('logical_id','endpoint_gid','endpoint_generation')), 'endpoint_shaped_process_start')
        owned = [e for e in events if equal(e, st, PROCESS_KEYS + LINK_KEYS)]
        ex = one([e for e in exits if equal(e, st, PROCESS_KEYS + LINK_KEYS)], 'group_exit')
        require(ex.get('returncode') == 0 and not any(k in ex for k in ENDPOINT_KEYS), 'group_exit_status')
        ctx = one([e for e in owned if e.get('event') == 'group_context_created'], 'context_created')
        ready = one([e for e in owned if e.get('event') == 'group_worker_ready'], 'group_ready')
        shut = one([e for e in owned if e.get('event') == 'group_context_shutdown'], 'context_shutdown')
        require(binding['event_index'] < st['event_index'] < ctx['event_index'] < ready['event_index']
                < shut['event_index'] < ex['event_index'] and shut.get('closed') is True
                and shut.get('errors') == [] and ctx.get('ros_context_created') is True
                and ctx.get('api') == 'rclpy.context.Context+rclpy.init', 'context_chronology_or_cleanup')
        require(ctx.get('rmw_identifier') == 'rmw_cyclonedds_cpp'
                and (config is None or ctx.get('observed_domain_id') == config.get('domain_id')), 'context_rmw_domain')
        require(st['worker_instance_id'] != st['context_instance_id'], 'process_context_nonce_alias')
        requests = [e for e in owned if e.get('event') == 'group_command_requested']
        acks = [e for e in owned if e.get('event') == 'command_done']
        require([r.get('command_id') for r in requests] == list(range(1, len(requests)+1)), 'process_command_serial')
        require(len(requests) == len(acks), 'command_ack_coverage')
        for req in requests:
            ack = one([e for e in acks if e.get('command_id') == req['command_id']], 'command_ack_unique')
            require(req['event_index'] < ack['event_index'], 'command_ack_order')
            if req.get('command') == 'stop':
                require(ack.get('command') == 'stop' and ack['event_index'] < shut['event_index'], 'stop_ack')
                continue
            if req.get('command') == 'group_snapshot' and req.get('logical_id') is None:
                require(not any(k in e for e in (req, ack) for k in ENDPOINT_KEYS + ('worker_role',))
                        and ack.get('command') == 'group_snapshot'
                        and ack.get('endpoint_absence_observed') is True
                        and type(ack.get('active_endpoint_count')) is int and ack['active_endpoint_count'] == 0,
                        'empty_group_ack_identity')
                prior_tokens = [t for t in tokens if t['logical_id'] in ids and t['event_index'] < req['event_index']]
                for retired in prior_tokens:
                    cleanup = cleanups[(retired['logical_id'], retired['generation'])]
                    retired_ack = one([a for a in acks if a.get('command') == 'destroy_endpoint'
                                       and equal(a, retired, ENDPOINT_KEYS)], 'empty_group_retirement_ack')
                    require(cleanup['event_index'] < retired_ack['event_index'] < req['event_index']
                            and cleanup['monotonic_ns'] <= retired_ack['monotonic_ns'] <= req['monotonic_ns'],
                            'empty_group_retirement_not_closed')
                continue
            require(req.get('logical_id') in ids and type(req.get('generation')) is int
                    and req['generation'] > 0, 'command_address')
            token = one([t for t in tokens if equal(t, req, ENDPOINT_KEYS[:2])], 'command_token')
            require(equal(ack, token, ENDPOINT_KEYS[:3]) and ack.get('command', req.get('command')) == req.get('command'), 'addressed_ack_identity')
            if req.get('command') != 'create_endpoint':
                require(req.get('endpoint_generation') == token['endpoint_generation']
                        and token['event_index'] < req['event_index'], 'command_incarnation_address')
                cleanup = cleanups[(token['logical_id'], token['generation'])]
                if req.get('command') == 'destroy_endpoint':
                    require(req['event_index'] < cleanup['event_index'] < ack['event_index']
                            and equal(ack, token, ENDPOINT_KEYS) and ack.get('cleanup_status') == 'closed'
                            and req['monotonic_ns'] <= cleanup['monotonic_ns'] <= ack['monotonic_ns'], 'destroy_request_cleanup_ack_order')
                else:
                    require(ack['event_index'] < cleanup['event_index'], 'command_after_endpoint_cleanup')
            else:
                require(req['event_index'] < token['event_index'] < ack['event_index'], 'create_request_ack_order')
                if token['generation'] > 1:
                    predecessor = one([t for t in tokens if t['logical_id'] == token['logical_id']
                                       and t['generation'] == token['generation'] - 1], 'predecessor_token')
                    cleanup = cleanups[(predecessor['logical_id'], predecessor['generation'])]
                    destroy = one([r for r in requests if r.get('command') == 'destroy_endpoint'
                                   and equal(r, predecessor, ENDPOINT_KEYS[:3])], 'predecessor_destroy')
                    destroy_ack = one([a for a in acks if a.get('command_id') == destroy['command_id']], 'predecessor_destroy_ack')
                    require(cleanup['event_index'] < destroy_ack['event_index'] < req['event_index']
                            and destroy_ack['monotonic_ns'] <= req['monotonic_ns'] <= token['monotonic_ns']
                            <= ack['monotonic_ns'], 'predecessor_retirement_order')
        group_tokens = [t for t in tokens if t['logical_id'] in ids]
        for token in group_tokens:
            require(equal(token, st, PROCESS_KEYS + LINK_KEYS), 'endpoint_process_context_link')
            spec = next(e for e in program['endpoints'] if e['logical_id'] == token['logical_id'])
            require(type(token.get('generation')) is int and token['generation'] > 0
                    and token.get('role') == aliases[spec['logical_id']]
                    and token.get('worker_role') == aliases[spec['logical_id']]
                    and token.get('rmw_identifier') == ctx.get('rmw_identifier')
                    and token.get('observed_domain_id') == ctx.get('observed_domain_id')
                    and token.get('domain_id') == ctx.get('domain_id')
                    and token.get('topic') == spec['topic'] and token.get('message_type') == spec['type_name']
                    and token.get('endpoint_generation') not in (st['worker_instance_id'], st['context_instance_id'])
                    and isinstance(token.get('endpoint_generation'), str) and bool(token['endpoint_generation']), 'endpoint_identity')
            require(ready['event_index'] < token['event_index'] < shut['event_index'], 'endpoint_context_chronology')
            bound = one([e for e in owned if e.get('event') == 'endpoint_group_bound'
                         and equal(e.get('endpoint', {}), token, ENDPOINT_KEYS)], 'endpoint_binding')
            require(all(k not in bound['endpoint'] or bound['endpoint'][k] == aliases[token['logical_id']]
                        for k in ('role', 'worker_role')), 'endpoint_binding_role_alias')
            require(token['event_index'] < bound['event_index'] < shut['event_index'], 'binding_order')
            cleanup = one([e for e in owned if e.get('event') == 'endpoint_destroyed'
                           and equal(e, token, ENDPOINT_KEYS)], 'endpoint_cleanup')
            require(bound['event_index'] < cleanup['event_index'] < shut['event_index']
                    and cleanup.get('cleanup_status') == 'closed', 'endpoint_cleanup_order')
            actual = token.get('real_ros') is True and token.get('backend') == 'ros2_public_api' and token.get('fixture') is not True
            workers.append({
                'role': token['worker_role'], 'endpoint_role': spec['role'],
                **{k: token[k] for k in ENDPOINT_KEYS + PROCESS_KEYS + LINK_KEYS},
                'worker_generation': token['generation'], 'current_endpoint_gid': token['endpoint_gid'], 'incarnation_ordinal': 0,
                'identity_transition_raw_refs': [], 'rmw_identifier': token.get('rmw_identifier') if actual else None,
                'rmw': token.get('rmw_identifier') if actual else None,
                'domain_id': token.get('observed_domain_id') if actual else None,
                'domain_observation_source': token.get('domain_observation_source') if actual else None,
                'execution_domain_id': token.get('domain_id'), 'fixture': not actual, 'real_ros': actual,
                'fresh_process': actual, 'separate_context': len(ids) == 1 and actual,
                'group_context_owned': actual, 'context_shared': len(ids) > 1,
                'closed': True, 'returncode': 0, 'cleanup_status': 'closed',
                'token_raw_ref': ref(token), 'start_raw_refs': [ref(st)], 'exit_raw_refs': [ref(ex)],
                'cleanup_raw_refs': [ref(cleanup)], 'context_raw_ref': ref(ctx), 'group_binding_raw_ref': ref(bound),
                'unknowns': [] if actual else ['offline_fixture_not_actual_worker']})
        captures = [e for e in owned if e.get('event') == 'participant_group_snapshot']
        for capture in captures:
            require(capture.get('schema') == OBSERVATION_SCHEMA and capture.get('candidate_eligible') is False
                    and capture.get('supporting_diagnostic') is True, 'group_observation_schema')
            req = one([r for r in requests if r.get('command') == 'group_snapshot'
                       and r.get('command_id') == capture.get('command_id')], 'group_capture_request')
            ack = one([e for e in acks if e.get('command_id') == req['command_id']], 'group_capture_ack')
            active_tokens = [t for t in group_tokens if t['event_index'] < req['event_index']
                             and cleanups[(t['logical_id'], t['generation'])]['event_index'] > ack['event_index']]
            require(req['event_index'] < capture['event_index'] < ack['event_index']
                    and req['monotonic_ns'] <= capture['monotonic_ns'] <= ack['monotonic_ns'], 'group_capture_order')
            if active_tokens:
                require(max(t['event_index'] for t in active_tokens) < req['event_index']
                        and req.get('logical_id') is not None
                        and capture.get('endpoint_absence_observed') is not True, 'group_capture_order')
            else:
                require(req.get('logical_id') is None and capture.get('endpoint_absence_observed') is True
                        and type(capture.get('active_endpoint_count')) is int and capture['active_endpoint_count'] == 0
                        and not any(k in capture for k in ENDPOINT_KEYS + ('worker_role',))
                        and capture.get('endpoints') == [], 'empty_group_capture_inventory')
            projected = qualify_observation(capture, active_tokens)
            require(all(capture.get(k) == v for k,v in projected.items()), 'group_observation_status_forged')
            observations.append({'raw_ref': ref(capture), 'group_id': label,
                                 'schema': OBSERVATION_SCHEMA, 'supporting_diagnostic': True,
                                 'candidate_eligible': False,
                                 **({'endpoint_absence_observed': True, 'active_endpoint_count': 0}
                                    if not active_tokens else {}), **projected})
        require(bool(captures), 'group_capture_missing')
        if observations[-1]['group_relation_observed']:
            all_gids.add(captures[-1]['roots_before']['roots'][0]['participant_gid'])
        processes.append({**{k: st[k] for k in PROCESS_KEYS + LINK_KEYS},
            'start_raw_ref': ref(st), 'exit_raw_ref': ref(ex), 'endpoint_ids': list(ids)})
        contexts.append({**{k: st[k] for k in LINK_KEYS}, 'created_raw_ref': ref(ctx),
                         'shutdown_raw_ref': ref(shut), 'endpoint_ids': list(ids)})
    # Every raw group event must belong to a recorded process, not a forged alias.
    for event in events:
        if event.get('event') in {'group_context_created','group_worker_ready','endpoint_group_bound',
                                  'group_context_shutdown','participant_group_snapshot','group_command_requested'}:
            require(sum(equal(event, p, PROCESS_KEYS + LINK_KEYS) for p in processes) == 1, 'extra_or_unbound_group_event')
    for event in events:
        if event.get('logical_id') is not None and (('group_id' in event) or event.get('event') in {
            'endpoint_created','endpoint_create_begin','endpoint_destroyed','worker_ready','send','receive',
            'match','match_snapshot','graph_snapshot','command_done','participant_ownership_snapshot',
            'topology_snapshot_requested','group_command_requested'}):
            token = one([t for t in tokens if equal(t, event, ENDPOINT_KEYS[:2])], 'endpoint_event_address')
            keys = PROCESS_KEYS + LINK_KEYS + ENDPOINT_KEYS[:2]
            if not (event.get('event') == 'group_command_requested' and event.get('command') == 'create_endpoint'):
                keys += ('endpoint_generation',)
            require(equal(event, token, keys), 'endpoint_event_context_or_incarnation')
            require(_endpoint_event_roles_match(event, aliases[token['logical_id']]),
                    'endpoint_event_role_alias')
            cleanup = cleanups[(token['logical_id'], token['generation'])]
            if event.get('event') in {'send', 'receive', 'match', 'match_snapshot', 'graph_snapshot', 'participant_ownership_snapshot', 'worker_ready'}:
                require(event['event_index'] < cleanup['event_index'], 'retired_endpoint_activity')
    relation = (all(o['group_relation_observed'] for o in observations)
                and (len(groups) == 1 or len(all_gids) == len(groups)))
    return {'schema': LINEAGE_SCHEMA, 'processes': processes, 'contexts': contexts,
            'workers': workers, 'observations': observations,
            'group_relation_observed': relation, 'supporting_diagnostic': True, 'candidate_eligible': False}


def lineage_metadata(program, events, raw_path, *, config=None):
    """Failure-preserving projection: invalid traces retain an explicit error."""
    try:
        return project_lineage(program, events, raw_path, config=config)
    except (ValueError, TypeError, KeyError, IndexError) as exc:
        return {'schema': LINEAGE_SCHEMA, 'processes': [], 'contexts': [], 'workers': [],
                'observations': [], 'group_relation_observed': False,
                'supporting_diagnostic': True, 'candidate_eligible': False, 'error': str(exc)}
