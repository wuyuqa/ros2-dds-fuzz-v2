"""Raw-bound DDS reader incarnations; never rewrite logical/process creation.

The ordinal is harness association metadata, not a DDS field. A graph change
alone, an API return alone, or an unclosed command cannot authorize rebinding.
This module does not qualify filtering effects, candidates or real execution.
"""
from copy import deepcopy
import re

SCHEMA = 'r2f.endpoint-identity-transition/v1'
SOURCE = 'get_subscriptions_info_by_topic'
API = 'rclpy.subscription.Subscription.set_content_filter'
LINEAGE = ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')
_BINDING = LINEAGE + ('pgid', 'run_id', 'domain_id', 'node_name',
                       'rmw_identifier', 'monotonic_clock_identity')


def _require(ok, reason):
    if not ok:
        raise ValueError('endpoint_incarnation:' + reason)


def _gid(value):
    return (isinstance(value, str) and re.fullmatch('[0-9a-f]{32}', value) is not None
            and value != '0' * 32)


def _one(rows, kind, **fields):
    found = [(i, e) for i, e in rows if e.get('event') == kind
             and all(e.get(k) == v for k, v in fields.items())]
    _require(len(found) == 1, 'missing_or_duplicate:' + kind + ':' + str(fields))
    return found[0]


def _bound(row, token):
    _require(all(token.get(k) is not None and row.get(k) == token[k] for k in _BINDING)
             and row.get('role') == token.get('worker_role', token.get('role')),
             'worker_run_domain_clock_mismatch')


def _snapshot(row, token, gid):
    _require(row.get('command') == 'update_filter' and row.get('identity_source') == SOURCE
             and row.get('endpoint_gid') == gid and _gid(gid), 'snapshot_identity_invalid')
    # Older endpoint_created may not record namespace; node names are random
    # process-bound names. No configured GID/type is substituted for observations.
    _require(row.get('topic') == token.get('topic') and bool(token.get('topic')),
             'snapshot_topic_mismatch')
    _require(isinstance(row.get('subscriptions'), list) and isinstance(row.get('publishers'), list),
             'snapshot_graph_missing')
    own = [e for e in row['subscriptions'] if isinstance(e, dict)
           and e.get('node_name') == token.get('node_name')]
    _require(len(own) == 1 and own[0].get('endpoint_gid') == gid
             and bool(token.get('message_type'))
             and own[0].get('topic_type') == token['message_type'],
             'snapshot_own_endpoint_missing_ambiguous_or_type_mismatch')
    for members in (row['publishers'], row['subscriptions']):
        gids = [e.get('endpoint_gid') if isinstance(e, dict) else None for e in members]
        _require(all(_gid(g) for g in gids) and len(gids) == len(set(gids)),
                 'snapshot_graph_gid_invalid_or_duplicate')


def validate_filter_identity_update(events, token, command_id):
    """Validate one complete worker command against its *prior* current token.

    Accepts canonical raw or a worker's captured stdout list. Positional order
    is used even when the latter has no global event_index. Caller updates live
    handles only after this function returns; original creation stays immutable.
    """
    _require(type(command_id) is int and command_id > 0, 'command_id_invalid')
    from .topology_worker import worker_role_kind
    _require(worker_role_kind(token.get('role')) == 'reader', 'not_reader')
    rows = [(i, e) for i, e in enumerate(events)
            if e.get('logical_id') == token.get('logical_id')
            and e.get('generation') == token.get('generation')
            and e.get('command_id') == command_id]
    before = _one(rows, 'endpoint_identity_snapshot', phase='before')
    after = _one(rows, 'endpoint_identity_snapshot', phase='after')
    begin = _one(rows, 'content_filter_api_call', api_stage='set_begin')
    results = [(i, e) for i, e in rows if e.get('event') == 'content_filter_api_call'
               and e.get('api_stage') in ('set_success', 'set_failure')]
    _require(len(results) == 1, 'set_result_missing_or_duplicate')
    result = results[0]
    dynamic = _one(rows, 'content_filter_dynamic_update')
    ack = _one(rows, 'command_done')
    old, new = before[1].get('endpoint_gid'), after[1].get('endpoint_gid')
    _require(old == token.get('endpoint_gid'), 'before_not_current_gid')
    for _, e in rows:
        if e.get('event') in ('endpoint_identity_snapshot', 'content_filter_api_call',
                              'content_filter_dynamic_update', 'endpoint_identity_transition', 'command_done'):
            _bound(e, token)
    _snapshot(before[1], token, old)
    _snapshot(after[1], token, new)
    calls = [(i, e) for i, e in rows if e.get('event') == 'content_filter_api_call']
    for _, e in calls:
        _require(e.get('reader_gid') == old, 'api_reader_gid_mismatch')
        _require(type(e.get('api_monotonic_ns')) is int
                 and e['api_monotonic_ns'] <= e.get('monotonic_ns', -1), 'api_clock_invalid')
    _require([e.get('api_stage') for _, e in calls][:2] == ['set_begin', result[1]['api_stage']],
             'set_call_order_invalid')
    stages = [e.get('api_stage') for _, e in calls]
    _require(stages in (['set_begin', 'set_failure'], ['set_begin', 'set_success'],
                        ['set_begin', 'set_success', 'get_begin', 'get_success'],
                        ['set_begin', 'set_success', 'get_begin', 'get_failure']),
             'api_call_stages_invalid')
    value = dynamic[1]
    _require(value.get('before_gid') == old and value.get('after_gid') == new
             and value.get('reader_gid') == new, 'dynamic_gid_binding_invalid')
    receipt = value.get('filter_receipt')
    success = result[1]['api_stage'] == 'set_success'
    _require(isinstance(receipt, dict)
             and receipt.get('api_status') == ('succeeded' if success else 'failed'),
             'api_receipt_status_mismatch')
    transitions = [(i, e) for i, e in rows if e.get('event') == 'endpoint_identity_transition']
    ordinal = token.get('incarnation_ordinal', 0)
    _require(type(ordinal) is int and ordinal >= 0, 'ordinal_invalid')
    chain = [before, *calls, after, dynamic]
    transition_ref = token.get('identity_transition_ref')
    if new != old:
        _require(len(transitions) == 1, 'transition_missing_or_duplicate')
        index, t = transitions[0]
        _require(t.get('schema') == SCHEMA and t.get('command') == 'update_filter'
                 and t.get('api') == API and t.get('identity_source') == SOURCE
                 and t.get('before_gid') == old and t.get('after_gid') == new
                 and t.get('endpoint_gid') == new
                 and type(t.get('incarnation_before')) is int
                 and type(t.get('incarnation_after')) is int
                 and t['incarnation_before'] == ordinal and t['incarnation_after'] == ordinal + 1,
                 'transition_fields_invalid')
        # GID reuse, even from this reader's past incarnation, is not a fresh
        # incarnation proof. Other workers' creation/transition aliases fail.
        aliases = [e for e in events[:index] if
                   (e.get('event') == 'endpoint_created' and e.get('endpoint_gid') == new)
                   or (e.get('event') == 'endpoint_identity_transition' and e.get('after_gid') == new)]
        _require(not aliases, 'gid_reused_or_aliased')
        chain.append(transitions[0])
        transition_ref = t.get('event_index', index)
        ordinal += 1
    else:
        _require(not transitions, 'unchanged_gid_cannot_increment_incarnation')
    chain.append(ack)
    _require(ack[1].get('command') == 'update_filter'
             and ack[1].get('endpoint_gid') == new and ack[1].get('reader_gid') == new
             and type(ack[1].get('incarnation_ordinal')) is int
             and ack[1]['incarnation_ordinal'] == ordinal, 'ack_identity_or_command_mismatch')
    _require(all(a[0] < b[0] for a, b in zip(chain, chain[1:])), 'raw_command_order_invalid')
    stamps = [e.get('monotonic_ns') for _, e in chain]
    _require(all(type(t) is int and t > 0 for t in stamps)
             and stamps == sorted(stamps), 'command_chronology_invalid')
    _require(type(value.get('update_started_ns')) is int
             and type(value.get('update_finished_ns')) is int
             and value['update_started_ns'] <= stamps[0]
             and after[1]['monotonic_ns'] <= value['update_finished_ns'] <= value['monotonic_ns'],
             'update_interval_invalid')
    previous_ns = token.get('identity_valid_after_ns',
                            token.get('monotonic_ns', token.get('created_monotonic_ns')))
    _require(type(previous_ns) is int and previous_ns <= value['update_started_ns'] <= stamps[0],
             'update_before_creation_or_previous_ack')
    previous_ref = token.get('identity_valid_after_ref')
    if previous_ref is not None:
        _require(type(previous_ref) is int
                 and previous_ref < before[1].get('event_index', before[0]),
                 'update_before_previous_ack_position')
    previous_call_ns = before[1]['monotonic_ns']
    for _, call in calls:
        _require(previous_call_ns <= call['api_monotonic_ns'] <= call['monotonic_ns'],
                 'api_call_outside_update_chronology')
        previous_call_ns = call['monotonic_ns']
    return {**deepcopy(token), 'endpoint_gid': new, 'incarnation_ordinal': ordinal,
            'identity_transition_ref': transition_ref,
            'identity_valid_after_ref': ack[1].get('event_index', ack[0]),
            'identity_valid_after_ns': ack[1]['monotonic_ns']}


def endpoint_identity_at(events, token, *, before_index=None):
    """Resolve an immutable creation to its identity at an exclusive raw prefix.

    An in-flight/unacknowledged transition is deliberately not usable. The
    creation's index/time remain unchanged; incarnation refs are additional.
    """
    limit = len(events) if before_index is None else before_index
    _require(type(limit) is int and 0 <= limit <= len(events), 'prefix_invalid')
    prefix = events[:limit]
    origins = [e for e in prefix if e.get('event') == 'endpoint_created'
               and e.get('logical_id') == token.get('logical_id')
               and e.get('generation') == token.get('generation')]
    _require(len(origins) == 1, 'creation_missing_or_duplicate')
    origin = origins[0]
    _require(all(origin.get(k) is not None and token.get(k) == origin[k] for k in LINEAGE),
             'creation_lineage_mismatch')
    current = {**deepcopy(origin), 'incarnation_ordinal': 0, 'identity_transition_ref': None,
               'identity_valid_after_ref': origin.get('event_index', prefix.index(origin)),
               'identity_valid_after_ns': origin.get('monotonic_ns')}
    transitions = [e for e in prefix if e.get('event') == 'endpoint_identity_transition'
                   and e.get('logical_id') == origin['logical_id']
                   and e.get('generation') == origin['generation']]
    commands = set()
    for t in transitions:
        cid = t.get('command_id')
        _require(type(cid) is int and cid > 0 and cid not in commands, 'transition_command_reused')
        commands.add(cid)
        current = validate_filter_identity_update(prefix, current, cid)
    return current
