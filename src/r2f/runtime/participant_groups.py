"""Opt-in CP1 ROS groups: pure input and raw-lineage contracts, no runtime owner.

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

def enabled(program):
    return isinstance(program, dict) and 'participant_control' in program


def validate_control(program, config=None):
    """Return declared groups or fail before any resources are acquired."""
    if not enabled(program):
        return None
    if program['participant_control'] != CONTROL:
        raise ValueError('participant_control_tag_invalid')
    eps, actions = program.get('endpoints'), program.get('actions')
    if (not isinstance(eps, list) or len(eps) != 2
            or sorted(str(e.get('role')) for e in eps if isinstance(e, dict)) != ['reader', 'writer']):
        raise ValueError('participant_control_scope:requires_1W1R')
    groups = {}
    for ep in eps:
        label = ep.get('participant_id')
        if not isinstance(label, str) or not label.strip() or len(label) > 256:
            raise ValueError('participant_control_scope:group_label_required')
        if ep.get('filter') is not None or ep.get('type_name') != 'std_msgs/msg/String':
            raise ValueError('participant_control_scope:filter_or_type')
        groups.setdefault(label, []).append(ep['logical_id'])
    if eps[0].get('topic') != eps[1].get('topic') or 'episode' in program:
        raise ValueError('participant_control_scope:topic_or_episode')
    if not isinstance(actions, list):
        raise ValueError('participant_control_scope:actions')
    creates = []
    for a in actions:
        if (not isinstance(a, dict) or a.get('op') not in {'create', 'snapshot', 'barrier', 'publish_batch'}
                or 'concurrency_group' in (a.get('params') or {})):
            raise ValueError('participant_control_scope:unsupported_action')
        if a['op'] == 'publish_batch' and set(creates) != {e['logical_id'] for e in eps}:
            raise ValueError('participant_control_scope:publish_requires_both_creates')
        if a['op'] == 'create':
            if type(a.get('generation')) is not int or a['generation'] != 1:
                raise ValueError('participant_control_scope:generation')
            creates.append(a.get('endpoint_id'))
    if sorted(creates) != sorted(e['logical_id'] for e in eps):
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
            seen.add(row['logical_id'])
            samples = row.get('samples')
            if not isinstance(samples, list) or len(samples) != 2:
                raise ValueError('group_ownership_double_sample_missing')
            stable_keys = ('local_handle','participant_handle','participant_gid','parent_chain','entity_inventory','entity_identities','participant_root_observation','provenance')
            for sample in samples:
                if (sample.get('schema') != 'r2f.cyclone-participant-ownership/v2'
                        or sample.get('complete') is not True or sample.get('status') != 'observed'
                        or sample.get('local_gid') != row['endpoint_gid']
                        or sample.get('role_kind') != matching[0].get('role', matching[0].get('kind'))
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
        ownership = True
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
    """Authenticate a live CP1 PID share using only already observed raw lineage.

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

    raw = events[:before_index]
    require(all(e.get('event_index') == i for i, e in enumerate(raw)), 'event_offsets')
    require(len(tokens) == 2 and {t.get('role') for t in tokens} == {'writer', 'reader'},
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
    for e in scoped:
        require(equal(e, token, keys + ('run_id', 'domain_id')), 'raw_process_context_link')
        if e.get('logical_id') is not None:
            addressed = one([t for t in tokens if t['logical_id'] == e['logical_id']],
                            'endpoint_event_address')
            address_keys = ENDPOINT_KEYS[:2]
            if not (e.get('event') == 'group_command_requested'
                    and e.get('command') == 'create_endpoint'):
                address_keys += ('endpoint_generation',)
            require(equal(e, addressed, address_keys), 'endpoint_event_context_or_incarnation')
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
    require(not any(e.get('event') in {'group_context_shutdown', 'process_exit',
                                     'endpoint_destroyed', 'worker_exit'} for e in scoped),
            'shared_process_retired')
    requests = [e for e in scoped if e.get('event') == 'group_command_requested']
    require([r.get('command_id') for r in requests] == list(range(1, len(requests)+1)),
            'process_command_serial')
    bindings = []
    for t in tokens:
        bound = one([e for e in scoped if e.get('event') == 'endpoint_group_bound'
                     and equal(e.get('endpoint', {}), t, ENDPOINT_KEYS)], 'endpoint_binding')
        req = one([r for r in requests if r.get('command') == 'create_endpoint'
                   and equal(r, t, ENDPOINT_KEYS[:2])], 'create_request')
        ack = one([e for e in scoped if e.get('event') == 'command_done'
                   and e.get('command_id') == req.get('command_id')], 'create_ack')
        require(ack.get('command') == 'create_endpoint'
                and equal(ack, t, ENDPOINT_KEYS) and ordered(ready, req, t, bound, ack),
                'create_request_binding_ack_order')
        bindings.append(ack)
    request = events[request_index]
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
    groups = validate_control(program, config)
    if groups is None:
        raise ValueError('group_lineage_requires_explicit_input')
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
    require(len(tokens) == 2 and {t.get('logical_id') for t in tokens} == {e['logical_id'] for e in program['endpoints']}, 'endpoint_coverage')
    order = [a['endpoint_id'] for a in program['actions'] if a['op'] == 'create']
    require([t['logical_id'] for t in tokens] == order, 'lazy_create_order')
    require(len({t.get('endpoint_gid') for t in tokens}) == 2
            and len({t.get('endpoint_generation') for t in tokens}) == 2, 'endpoint_reuse')
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
            require(req.get('logical_id') in ids and type(req.get('generation')) is int
                    and req['generation'] == 1, 'command_address')
            token = one([t for t in tokens if t['logical_id'] == req['logical_id']], 'command_token')
            require(equal(ack, token, ENDPOINT_KEYS[:3]), 'addressed_ack_identity')
            if req.get('command') != 'create_endpoint':
                require(req.get('endpoint_generation') == token['endpoint_generation']
                        and token['event_index'] < req['event_index'], 'command_incarnation_address')
            else:
                require(req['event_index'] < token['event_index'] < ack['event_index'], 'create_request_ack_order')
        group_tokens = [t for t in tokens if t['logical_id'] in ids]
        for token in group_tokens:
            require(equal(token, st, PROCESS_KEYS + LINK_KEYS), 'endpoint_process_context_link')
            spec = next(e for e in program['endpoints'] if e['logical_id'] == token['logical_id'])
            require(token.get('generation') == 1 and type(token['generation']) is int
                    and token.get('role') == spec['role'] and token.get('worker_role') == spec['role']
                    and token.get('rmw_identifier') == ctx.get('rmw_identifier')
                    and token.get('observed_domain_id') == ctx.get('observed_domain_id')
                    and token.get('domain_id') == ctx.get('domain_id')
                    and token.get('topic') == spec['topic'] and token.get('message_type') == spec['type_name']
                    and token.get('endpoint_generation') not in (st['worker_instance_id'], st['context_instance_id'])
                    and isinstance(token.get('endpoint_generation'), str) and bool(token['endpoint_generation']), 'endpoint_identity')
            require(ready['event_index'] < token['event_index'] < shut['event_index'], 'endpoint_context_chronology')
            bound = one([e for e in owned if e.get('event') == 'endpoint_group_bound'
                         and equal(e.get('endpoint', {}), token, ENDPOINT_KEYS)], 'endpoint_binding')
            require(token['event_index'] < bound['event_index'] < shut['event_index'], 'binding_order')
            cleanup = one([e for e in owned if e.get('event') == 'endpoint_destroyed'
                           and equal(e, token, ENDPOINT_KEYS)], 'endpoint_cleanup')
            require(bound['event_index'] < cleanup['event_index'] < shut['event_index']
                    and cleanup.get('cleanup_status') == 'closed', 'endpoint_cleanup_order')
            actual = token.get('real_ros') is True and token.get('backend') == 'ros2_public_api' and token.get('fixture') is not True
            workers.append({
                'role': token['worker_role'], 'endpoint_role': spec['role'],
                **{k: token[k] for k in ENDPOINT_KEYS + PROCESS_KEYS + LINK_KEYS},
                'worker_generation': 1, 'current_endpoint_gid': token['endpoint_gid'], 'incarnation_ordinal': 0,
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
            require(max(t['event_index'] for t in group_tokens) < req['event_index']
                    < capture['event_index'] < ack['event_index'], 'group_capture_order')
            projected = qualify_observation(capture, group_tokens)
            require(all(capture.get(k) == v for k,v in projected.items()), 'group_observation_status_forged')
            observations.append({'raw_ref': ref(capture), 'group_id': label,
                                 'schema': OBSERVATION_SCHEMA, 'supporting_diagnostic': True,
                                 'candidate_eligible': False, **projected})
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
            token = one([t for t in tokens if t['logical_id'] == event['logical_id']], 'endpoint_event_address')
            keys = PROCESS_KEYS + LINK_KEYS + ENDPOINT_KEYS[:2]
            if not (event.get('event') == 'group_command_requested' and event.get('command') == 'create_endpoint'):
                keys += ('endpoint_generation',)
            require(equal(event, token, keys), 'endpoint_event_context_or_incarnation')
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
