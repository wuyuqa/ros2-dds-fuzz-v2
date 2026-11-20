"""Raw-bound configuration/state action evidence, never a filtering verdict.

A failed empty setter can leave an independently observed disabled reader. That
is an observed state, not a successful setter, empty-expression readback or hit.
"""
from .endpoint_incarnation import endpoint_identity_at, validate_filter_identity_update

SCHEMA = 'r2f.cft-action-state-evidence/v1'
_BINDING = ('logical_id', 'generation', 'pid', 'pgid', 'process_start_ticks',
            'endpoint_generation', 'run_id', 'domain_id', 'node_name', 'role',
            'rmw_identifier', 'monotonic_clock_identity')


def _require(ok, reason):
    if not ok:
        raise ValueError('filter_action:' + reason)


def _one(rows, kind, **fields):
    found = [e for e in rows if e.get('event') == kind
             and all(e.get(k) == v for k, v in fields.items())]
    _require(len(found) == 1, 'missing_or_duplicate:' + kind)
    return found[0]


def filter_action_evidence(action, events, *, before_index=None):
    """Recompute one completed update's bounded action evidence from raw.

    The exclusive prefix must include the controller result after command ACK.
    No runtime summary, planned filter evaluation or inferred state is accepted.
    """
    limit = len(events) if before_index is None else before_index
    _require(type(limit) is int and 0 <= limit <= len(events), 'prefix_invalid')
    rows = events[:limit]
    _require(action.get('op') == 'update_filter', 'wrong_action')
    eid, gen, aid = action.get('endpoint_id'), action.get('generation'), action.get('action_id')
    result = _one(rows, 'topology_filter_update_result', action_id=aid, logical_id=eid, generation=gen)
    rid = result.get('event_index')
    _require(type(rid) is int and 0 <= rid < limit and rows[rid] == result, 'result_raw_envelope_invalid')
    prefix = rows[:rid + 1]
    created = _one(prefix, 'endpoint_created', logical_id=eid, generation=gen)
    from .topology_worker import worker_role_kind
    _require(worker_role_kind(created.get('role')) == 'reader', 'reader_required')
    _require(not any(e.get('event') in {'endpoint_destroyed', 'topology_endpoint_destroyed', 'worker_exit', 'topology_worker_exit'}
                     and e.get('logical_id') == eid and e.get('generation') == gen
                     for e in prefix[created['event_index'] + 1:]), 'reader_retired')
    _require(all(created.get(k) is not None and result.get(k) == created[k]
                 for k in ('run_id', 'domain_id', 'monotonic_clock_identity')),
             'result_run_domain_clock_mismatch')
    cid = result.get('command_id')
    _require(type(cid) is int and cid > 0, 'command_id_invalid')
    scoped = [e for e in prefix if e.get('logical_id') == eid and e.get('generation') == gen
              and e.get('command_id') == cid]
    before = _one(scoped, 'endpoint_identity_snapshot', phase='before')
    after = _one(scoped, 'endpoint_identity_snapshot', phase='after')
    token = endpoint_identity_at(prefix, created, before_index=before['event_index'])
    current = validate_filter_identity_update(prefix, token, cid)
    dynamic = _one(scoped, 'content_filter_dynamic_update')
    ack = _one(scoped, 'command_done')
    receipt = dynamic.get('filter_receipt')
    _require(isinstance(receipt, dict) and receipt == result.get('receipt'), 'result_not_raw_backed')
    state = receipt.get('state_receipt')
    _require(isinstance(state, dict), 'independent_state_missing')
    observed = _one(scoped, 'content_filter_state_observation')
    state_calls = [e for e in scoped if e.get('event') == 'content_filter_state_api_call']
    _require(len(state_calls) == 2
             and [e.get('api_stage') for e in state_calls] == ['state_begin', 'state_success'],
             'state_api_chain_missing_or_failed')
    start, end = state_calls
    _require(all(e.get('api') == 'rclpy.subscription.Subscription.is_cft_enabled'
                 for e in state_calls), 'state_api_identity_missing_or_invalid')
    from .filter_backend import validate_filter
    validation = validate_filter(action['params']['expression'], action['params']['parameters'],
                                 field_types={'data': 'string'})
    _require(validation.get('ok') is True, 'input_configuration_rejected')
    requested = {k: validation[k] for k in ('enabled', 'expression', 'parameters')}
    _require(receipt.get('requested') == requested, 'requested_config_not_action_bound')
    _require(state == observed.get('filter_receipt')
             and state.get('schema') == 'r2f.participant-filter/v1'
             and state.get('receipt_kind') == 'filter_state'
             and state.get('api') == 'rclpy.subscription.Subscription.is_cft_enabled'
             and state.get('observation_basis') == 'public_subscription_is_cft_enabled'
             and state.get('status') == 'observed' and type(state.get('raw_result')) is bool
             and state.get('is_cft_enabled') is state['raw_result']
             and state.get('state') == ('enabled' if state['raw_result'] else 'disabled')
             and state.get('filter_effect_observed') is None
             and state.get('candidate_eligible') is False and state.get('hit') is False,
             'state_receipt_invalid')
    _require(state['raw_result'] is requested['enabled'], 'requested_state_not_observed')
    common = dict(command='update_filter', command_id=cid, phase='after',
                  reader_gid=current['endpoint_gid'], endpoint_gid=current['endpoint_gid'],
                  topic=created.get('topic'))
    _require(bool(common['topic']) and all(state.get(k) == v for k, v in common.items()),
             'state_receipt_command_or_gid_mismatch')
    for e in (start, end, observed):
        _require(all(created.get(k) is not None and e.get(k) == created[k] for k in _BINDING)
                 and all(e.get(k) == v for k, v in common.items())
                 and e.get('api', state['api']) == state['api'], 'state_raw_identity_mismatch')
        i = e.get('event_index')
        _require(type(i) is int and 0 <= i < rid and prefix[i] == e, 'state_raw_envelope_invalid')
    chain = [after, start, end, observed, dynamic, ack, result]
    _require(all(a['event_index'] < b['event_index']
                 and type(a.get('monotonic_ns')) is int and type(b.get('monotonic_ns')) is int
                 and a['monotonic_ns'] <= b['monotonic_ns'] for a, b in zip(chain, chain[1:])),
             'state_raw_chronology_invalid')
    # The API's own interval must lie inside this reader incarnation/command,
    # not merely be copied consistently into independently stale summaries.
    times = [after['monotonic_ns'], state.get('started_monotonic_ns'),
             start.get('api_monotonic_ns'), start['monotonic_ns'],
             state.get('completed_monotonic_ns'), end.get('api_monotonic_ns'),
             end['monotonic_ns'], state.get('monotonic_ns'), observed['monotonic_ns']]
    _require(all(type(t) is int and t > 0 for t in times) and times == sorted(times)
             and start.get('started_monotonic_ns') == times[1]
             and end.get('completed_monotonic_ns') == times[4], 'state_api_clock_invalid')
    _require(end.get('status') == state['status'] == 'observed'
             and end.get('result_type') == state.get('result_type') == 'bool'
             and end.get('raw_result') is state['raw_result']
             and end.get('is_cft_enabled') is state['raw_result']
             and end.get('state') == state['state']
             and all(e.get(k) is None for e in (end, state)
                     for k in ('reason', 'error', 'error_type', 'raw_result_status')),
             'state_api_result_invalid')
    calls = [e for e in scoped if e.get('event') == 'content_filter_api_call']
    _require(bool(calls) and calls[0].get('requested') == requested, 'api_request_not_action_bound')
    setter_succeeded = receipt.get('api_status') == 'succeeded'
    outcome, disposition = 'disabled_state_observed', 'observed'
    if requested['enabled']:
        _require(setter_succeeded and receipt.get('applied') is True
                 and receipt.get('readback_status') == 'matched'
                 and [e.get('api_stage') for e in calls] ==
                 ['set_begin', 'set_success', 'get_begin', 'get_success'], 'enabled_api_readback_missing')
        from .filter_backend import normalize_captured_update_readback
        captured = normalize_captured_update_readback(calls[-1].get('raw_readback'),
            fixture=calls[-1].get('fixture') is True or calls[-1].get('backend') == 'offline_fake')
        expected = {k: requested[k] for k in ('expression', 'parameters')}
        _require(captured == expected and receipt.get('observed') == expected
                 and calls[-1].get('observed') == expected and calls[0].get('requested') == requested,
                 'enabled_readback_not_raw_backed')
        outcome, disposition = 'configuration_readback', 'configured'
    _require(result.get('reader_gid') == current['endpoint_gid']
             and ack['event_index'] == current['identity_valid_after_ref'], 'result_identity_not_committed')
    requires = [dict(id='live_reader_current_generation', status='satisfied', raw_ref=created['event_index']),
                dict(id='identity_bound_filter_command_completed', status='satisfied', raw_ref=ack['event_index']),
                dict(id='requested_filter_configuration_observed' if requested['enabled'] else
                     'requested_disabled_state_observed', status='satisfied', raw_ref=observed['event_index'])]
    return dict(schema=SCHEMA, action_id=aid, logical_id=eid, generation=gen, command_id=cid,
        reader_gid=current['endpoint_gid'], reader_incarnation=current['incarnation_ordinal'],
        outcome_kind=outcome, disposition=disposition, satisfied=True, requires=requires,
        setter_succeeded=setter_succeeded, setter_api_status=receipt.get('api_status'),
        readback_status=receipt.get('readback_status'), observed_enabled=state['raw_result'],
        update_ref=rid, worker_update_ref=dynamic['event_index'], state_ref=observed['event_index'],
        state_api_refs=[start['event_index'], end['event_index']],
        api_call_refs=[e['event_index'] for e in calls], ack_ref=ack['event_index'],
        filter_effect_observed=None, mutation_applied=False, candidate_eligible=False,
        scope='configuration_or_public_state_only_not_filter_effect')
