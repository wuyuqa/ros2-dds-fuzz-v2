"""Independent, fail-closed consumer of raw liveliness evidence.

Wired into the final topology facts path as a fail-closed consumer.
Peer-coverage rows still require a trusted producer: graph snapshots alone cannot
establish an uninterrupted peer set. The caller is responsible for authenticating
raw bytes and the coverage source.
"""

LINEAGE = ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')
API = 'rclpy.publisher.Publisher.assert_liveliness'


def evaluate_liveliness(events, *, writer, reader, run_id, fixture=True,
                        assertion_command_id=None):
    """Return an observation, never a candidate; refs are raw event indices.

    Requires one trusted topology_liveliness_peer_coverage raw row describing
    a continuous observation interval, including all external peers. The current
    worker does not produce this row, so missing proof remains unknown. A command
    selector binds repeated cycles without merging their callbacks or receipts.
    Normal cleanup after a proven interval does not revoke that observation.
    """
    def unknown(reason):
        return {'status': 'unknown', 'reason': reason, 'loss_observed': None,
                'recovery_observed': None, 'effect_observed': None,
                'execution_evidence': False, 'candidate_eligible': False}

    def integer(x):
        return type(x) is int and x > 0

    def text(x):
        return isinstance(x, str) and bool(x)

    def same(row, token):
        return all(row.get(k) == token.get(k) for k in LINEAGE)

    if type(fixture) is not bool or not text(run_id) or not isinstance(events, list):
        return unknown('invalid_input')
    for token, role in ((writer, 'writer'), (reader, 'reader')):
        if (not isinstance(token, dict) or token.get('role') != role
                or not text(token.get('logical_id')) or not text(token.get('endpoint_gid'))
                or not text(token.get('endpoint_generation'))
                or not all(integer(token.get(k)) for k in ('generation', 'pid', 'process_start_ticks'))):
            return unknown('invalid_endpoint_token')
    if writer['endpoint_gid'] == reader['endpoint_gid'] or writer['logical_id'] == reader['logical_id']:
        return unknown('endpoint_identity_collision')
    if not events or any(not isinstance(e, dict) or e.get('event_index') != i
                         or e.get('run_id') != run_id for i, e in enumerate(events)):
        return unknown('invalid_raw_sequence_or_run')
    clock = events[0].get('monotonic_clock_identity')
    if not isinstance(clock, dict) or not text(clock.get('boot_id')) or not integer(clock.get('inode')) or not integer(clock.get('device')):
        return unknown('clock_identity_missing')
    if any(e.get('monotonic_clock_identity') != clock or not integer(e.get('monotonic_ns')) for e in events):
        return unknown('clock_identity_or_time_mismatch')
    if any(e.get('fixture') is True or e.get('offline_fake') is True for e in events):
        fixture = True
    if not fixture:
        provenance = [e for e in events if e.get('event') == 'topology_provenance']
        if (len(provenance) != 1 or provenance[0].get('runtime_kind') != 'ros2_public_api'
                or provenance[0].get('fixture') is not False):
            return unknown('real_execution_provenance_missing')
    def rows(kind, token=None):
        return [e for e in events if e.get('event') == kind and (token is None or same(e, token))]
    created = []
    for token in (writer, reader):
        r = rows('endpoint_created', token)
        if len(r) != 1 or r[0].get('endpoint_gid') != token['endpoint_gid']:
            return unknown('creation_identity_missing_or_ambiguous')
        created.append(r[0])
    asserts = rows('liveliness_asserted', writer)
    if assertion_command_id is not None:
        if not integer(assertion_command_id):
            return unknown('assert_command_selector_invalid')
        asserts = [e for e in asserts if e.get('command_id') == assertion_command_id]
    if len(asserts) != 1:
        return unknown('loss_assert_or_peer_coverage_missing_or_ambiguous')
    asserted = asserts[0]
    command = asserted.get('command_id')
    acks = [e for e in rows('command_done', writer) if e.get('command_id') == command]
    if not integer(command) or asserted.get('api') != API or asserted.get('endpoint_gid') != writer['endpoint_gid'] or len(acks) != 1:
        return unknown('assert_or_ack_invalid')
    ack = acks[0]

    # Select the trusted coverage interval by assertion time, not by list order.
    # A second overlapping coverage record is ambiguous, even if its bytes agree.
    coverage = [e for e in rows('topology_liveliness_peer_coverage', reader)
                if integer(e.get('coverage_start_ns')) and integer(e.get('coverage_end_ns'))
                and e['coverage_start_ns'] < asserted['monotonic_ns'] < e['coverage_end_ns']]
    if len(coverage) != 1:
        return unknown('loss_assert_or_peer_coverage_missing_or_ambiguous')
    proof = coverage[0]
    start, end = proof['coverage_start_ns'], proof['coverage_end_ns']
    if (proof.get('scope') != 'continuous_complete_peer_inventory'
            or proof.get('external_peers_known') is not True
            or proof.get('peer_gids') != [writer['endpoint_gid']]
            or proof.get('reader_gid') != reader['endpoint_gid']
            or proof.get('coverage_clock_identity') != clock
            or proof['monotonic_ns'] < end
            or max(e['monotonic_ns'] for e in created) >= start):
        return unknown('unique_peer_coverage_unproven')
    if len([e for e in rows('liveliness_asserted', writer)
            if start <= e['monotonic_ns'] <= end]) != 1:
        return unknown('overlapping_assertion_cycles')

    # Lifetime checks cover creation through interval close, not the full run.
    # Do not ignore malformed clocks to classify a retirement as "later"; all
    # rows already passed the common clock/time gate above.
    retired = {'endpoint_destroyed', 'topology_endpoint_destroyed', 'worker_exit',
               'topology_worker_exit', 'topology_generation_retired', 'worker_stopping'}
    for token, creation in zip((writer, reader), created):
        for e in events:
            if e.get('logical_id') != token['logical_id']:
                continue
            if (e.get('event') == 'endpoint_created' and not same(e, token)
                    and creation['monotonic_ns'] <= e['monotonic_ns'] <= end):
                return unknown('endpoint_recreated')
            if e.get('event') in retired and e['monotonic_ns'] <= end:
                if e.get('generation') == token['generation'] or e.get('generation') is None:
                    return unknown('endpoint_retired')
                if e['monotonic_ns'] >= creation['monotonic_ns']:
                    return unknown('retirement_generation_mismatch')

    # Callback time is authoritative for the interval. A callback flushed during
    # cleanup may have occurred *inside* it and must not be dropped by raw order.
    def window_callbacks(token, kind):
        selected = []
        for e in events:
            if (e.get('event') != 'topology_liveliness_event'
                    or e.get('logical_id') != token['logical_id']
                    or e.get('event_kind') != kind):
                continue
            callback_time = e.get('callback_monotonic_ns')
            if (not integer(callback_time) or e.get('callback_clock_identity') != clock
                    or callback_time > e['monotonic_ns']):
                return None
            if start <= callback_time <= end or start <= e['monotonic_ns'] <= end:
                if not same(e, token):
                    return None
                selected.append(e)
        return selected

    losses = window_callbacks(writer, 'liveliness_lost')
    changes = window_callbacks(reader, 'liveliness_changed')
    if losses is None or changes is None:
        return unknown('callback_window_identity_or_clock_invalid')
    if len(losses) != 1:
        return unknown('loss_assert_or_peer_coverage_missing_or_ambiguous')
    loss = losses[0]
    def callback_ok(e, token, scope):
        return (e.get('observation_scope') == scope and e.get('endpoint_gid') == token['endpoint_gid']
                and e.get('endpoint_gid_at_callback') == token['endpoint_gid']
                and e.get('endpoint_identity_status') == 'bound' and e.get('buffered') is False
                and e.get('during_cleanup') is False and e.get('callback_clock_identity') == clock
                and integer(e.get('callback_monotonic_ns'))
                and e['callback_monotonic_ns'] <= e['monotonic_ns'])
    if not callback_ok(loss, writer, 'local_writer'):
        return unknown('loss_callback_identity_or_clock_invalid')
    ls = loss.get('status_fields')
    if not isinstance(ls, dict) or not integer(ls.get('total_count')) or not integer(ls.get('total_count_change')):
        return unknown('loss_status_invalid')
    if any(not callback_ok(e, reader, 'reader_aggregate') or not isinstance(e.get('status_fields'), dict)
           or any(type(e['status_fields'].get(k)) is not int for k in
                  ('alive_count', 'not_alive_count', 'alive_count_change', 'not_alive_count_change'))
           for e in changes):
        return unknown('reader_callback_identity_or_clock_invalid')
    # Require a reader loss and recovery, not just a local writer loss or API call.
    down = [e for e in changes if isinstance(e.get('status_fields'), dict)
            and e['status_fields'].get('alive_count') == 0 and e['status_fields'].get('not_alive_count') == 1
            and e['status_fields'].get('alive_count_change') == -1 and e['status_fields'].get('not_alive_count_change') == 1]
    up = [e for e in changes if isinstance(e.get('status_fields'), dict)
          and e['status_fields'].get('alive_count') == 1 and e['status_fields'].get('not_alive_count') == 0
          and e['status_fields'].get('alive_count_change') == 1 and e['status_fields'].get('not_alive_count_change') == -1]
    if len(down) != 1 or len(up) != 1 or len(changes) != 2:
        return unknown('reader_transition_missing_or_ambiguous')
    down, up = down[0], up[0]
    times = [loss['callback_monotonic_ns'], down['callback_monotonic_ns'], asserted['monotonic_ns'], ack['monotonic_ns'], up['callback_monotonic_ns']]
    if not (max(created[0]['monotonic_ns'], created[1]['monotonic_ns']) < min(times[:2])
            and max(times[:2]) < times[2] < times[3] < times[4]):
        return unknown('lifecycle_chronology_invalid')
    if any(e.get('event') == 'send' and same(e, writer)
           and min(times[:2]) <= e['monotonic_ns'] <= times[4] for e in events):
        return unknown('intervening_writer_publish')
    if not (asserted['event_index'] < ack['event_index'] and loss['event_index'] < asserted['event_index']
            and down['event_index'] < asserted['event_index'] and ack['event_index'] < up['event_index']):
        return unknown('raw_order_invalid')
    if (not start < min(times[:2]) <= times[4] < end
            or proof['event_index'] <= max(loss['event_index'], down['event_index'],
                                           asserted['event_index'], ack['event_index'],
                                           up['event_index'])):
        return unknown('unique_peer_coverage_unproven')
    return {'status': 'fixture_observed' if fixture else 'observed', 'reason': None,
            'loss_observed': True, 'recovery_observed': True,
            'effect_observed': None if fixture else True, 'execution_evidence': not fixture,
            'candidate_eligible': False,
            'observation_window': {'start_ns': start, 'end_ns': end,
                                   'peer_coverage_ref': proof['event_index']},
            'refs': {'loss': loss['event_index'], 'reader_loss': down['event_index'],
            'assert': asserted['event_index'], 'ack': ack['event_index'], 'recovery': up['event_index'],
            'peer_coverage': proof['event_index']}}


def evaluate_peer_samples(events, *, run_id, fixture=True):
    """Audit callback-linked bounded samples; never establish continuous coverage.

    The raw parser/hash authentication remains the caller's responsibility. The
    producer's status and complete flags alone are not accepted as evidence.
    This diagnostic is intentionally separate from evaluate_liveliness's much
    stronger continuous-inventory gate.
    """
    if not isinstance(events, list):
        return []
    samples = [e for e in events if isinstance(e, dict)
               and e.get('event') == 'topology_liveliness_peer_snapshot']
    valid_raw = (type(fixture) is bool and isinstance(run_id, str) and bool(run_id)
                 and all(isinstance(e, dict) and type(e.get('event_index')) is int
                         and e['event_index'] == i and e.get('run_id') == run_id
                         for i, e in enumerate(events)))
    fixture = fixture or any(isinstance(e, dict) and (
        e.get('fixture') is True or e.get('offline_fake') is True
        or e.get('backend') == 'offline_fake' or e.get('real_ros') is False)
        for e in events)
    observations = []
    for sample in samples:
        value = {'status': 'unknown', 'reason': None,
                 'snapshot_ref': sample.get('event_index'),
                 'callback_id': sample.get('callback_id'), 'callback_ref': None,
                 'scope': 'bounded_peer_sample_not_continuous',
                 'continuous_coverage': False, 'external_peers_known': None,
                 'effect_observed': None, 'execution_evidence': False,
                 'candidate_eligible': False}
        observations.append(value)
        try:
            def require(condition, reason):
                if not condition:
                    raise ValueError(reason)
            def positive(number):
                return type(number) is int and number > 0
            require(valid_raw, 'raw_sequence_or_run_invalid')
            require(positive(sample.get('callback_id')), 'callback_id_invalid')
            require(all(sample.get(k) is not None for k in LINEAGE), 'sample_lineage_missing')
            related = [e for e in events if all(e.get(k) == sample[k] for k in LINEAGE)]
            callbacks = [e for e in related if e.get('event') == 'topology_liveliness_event'
                         and e.get('callback_id') == sample['callback_id']]
            creations = [e for e in related if e.get('event') == 'endpoint_created']
            linked_samples = [e for e in related
                              if e.get('event') == 'topology_liveliness_peer_snapshot'
                              and e.get('callback_id') == sample['callback_id']]
            require(len(callbacks) == len(creations) == len(linked_samples) == 1,
                    'callback_creation_or_sample_not_unique')
            callback, creation = callbacks[0], creations[0]
            require(type(sample.get('domain_id')) is int
                    and 0 <= sample['domain_id'] <= 231
                    and callback.get('domain_id') == creation.get('domain_id') == sample['domain_id'],
                    'sample_domain_mismatch')
            gid = sample.get('reader_gid')
            require(isinstance(gid, str) and bool(gid)
                    and creation.get('endpoint_gid') == callback.get('endpoint_gid')
                    == callback.get('endpoint_gid_at_callback') == gid
                    and callback.get('endpoint_identity_status') == 'bound'
                    and callback.get('observation_scope') == 'reader_aggregate'
                    and callback.get('buffered') is False
                    and callback.get('during_cleanup') is False, 'callback_identity_unbound')
            clock = sample.get('monotonic_clock_identity')
            require(isinstance(clock, dict) and isinstance(clock.get('boot_id'), str)
                    and bool(clock['boot_id'])
                    and positive(clock.get('device')) and positive(clock.get('inode'))
                    and all(e.get('monotonic_clock_identity') == clock for e in (creation, callback))
                    and callback.get('callback_clock_identity') == clock
                    and sample.get('callback_clock_identity') == clock
                    and sample.get('sample_clock_identity') == clock, 'sample_clock_invalid')
            times = [creation.get('monotonic_ns'), callback.get('callback_monotonic_ns'),
                     callback.get('monotonic_ns'), sample.get('sample_started_ns'),
                     sample.get('sample_finished_ns'), sample.get('monotonic_ns')]
            require(all(positive(t) for t in times)
                    and all(a <= b for a, b in zip(times, times[1:]))
                    and sample.get('callback_monotonic_ns') == callback['callback_monotonic_ns']
                    and creation['event_index'] < callback['event_index'] < sample['event_index'],
                    'sample_chronology_invalid')
            native = sample.get('native_peer_observation')
            require(isinstance(native, dict) and native.get('complete') is True
                    and native.get('schema') == 'r2f.cyclone-matched-peer-observation/v1'
                    and native.get('local_gid') == gid and native.get('role_kind') == 'reader'
                    and native.get('domain_id') == sample.get('domain_id'), 'native_sample_incomplete')
            require(positive(native.get('started_monotonic_ns'))
                    and positive(native.get('ended_monotonic_ns'))
                    and sample['sample_started_ns'] <= native['started_monotonic_ns']
                    <= native['ended_monotonic_ns'] <= sample['sample_finished_ns'],
                    'native_sample_chronology_invalid')
            peers = native.get('matched_peer_gids')
            require(isinstance(peers, list) and all(isinstance(g, str) and bool(g) for g in peers)
                    and len(set(peers)) == len(peers) and gid not in peers
                    and sample.get('matched_peer_gids') == peers
                    and type(sample.get('public_count_before')) is int
                    and type(sample.get('public_count_after')) is int
                    and sample['public_count_before'] == sample['public_count_after'] == len(peers),
                    'sample_peer_inventory_or_counts_invalid')
            value.update(status='fixture_sample' if fixture else 'bounded_sample',
                         callback_ref=callback['event_index'], matched_peer_gids=list(peers))
        except (ValueError, KeyError, TypeError) as exc:
            value['reason'] = str(exc) if isinstance(exc, ValueError) else 'sample_invalid:' + type(exc).__name__
    return observations
