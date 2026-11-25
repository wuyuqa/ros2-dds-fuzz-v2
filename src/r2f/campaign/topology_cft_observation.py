"""Supporting CFT observations in the existing topology facts/report chain.

Configuration and public enabled state are not sample decisions or peer-match
predictions. Missing evidence stays explicit; fixtures never acquire effects.
"""
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.filter_action_receipt import filter_action_evidence
from r2f.runtime.publish_occurrence import producer_occurrence_witness

SCHEMA = 'r2f.cft-sample-observations/v1'


def _configuration(action, spec, events, ref):
    initial = action['op'] != 'update_filter'
    requested = spec['filter'] if initial else action['params']
    item = dict(action_id=action['action_id'], logical_id=action['endpoint_id'],
                generation=action['generation'], phase='initial' if initial else 'update',
                requested={k: requested[k] for k in ('expression', 'parameters')},
                configuration_status='unknown', filter_effect_observed=None,
                candidate_eligible=False, reason=None)
    try:
        proof = filter_action_evidence(action, events)
        if initial and proof.get('observed') != item['requested']:
            raise ValueError('initial_filter_configuration_differs_from_program')
        if proof.get('observed_enabled') is not bool(requested['expression'].strip()):
            raise ValueError('public_enabled_state_differs_from_requested_state')
        item.update(configuration_status='observed', evidence=proof,
                    raw_ref=ref(events[proof['update_ref']]))
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        item['reason'] = str(exc)
    return item


def _sample(send, config, events, decisions, deliveries, ref, fixture):
    value = dict(action_id=config['action_id'], reader=config['logical_id'],
                 reader_generation=config['generation'], writer=send.get('logical_id'),
                 writer_generation=send.get('generation'), sequence=send.get('send_sequence'),
                 send_ref=ref(send), payload_sha256=send.get('payload_sha256'),
                 status='unknown', reason='configuration_evidence_unavailable',
                 sample_effect_observed=None, filter_effect_observed=None,
                 candidate_eligible=False)
    if fixture:
        return dict(value, reason='fixture_not_DDS_evidence')
    if config['configuration_status'] != 'observed':
        return value
    producer, reason = producer_occurrence_witness(send, events)
    if producer is None:
        return dict(value, reason='publish_occurrence_unknown:' + str(reason))
    proof = config['evidence']
    # A command begun but not committed at publish time is not the old state.
    begin = events[producer['scope_refs']['begin_ref']]['monotonic_ns']
    if events[proof['update_ref']]['monotonic_ns'] > begin:
        return dict(value, reason='filter_action_not_committed_before_publish')
    origins = [e for e in events if e.get('event') == 'endpoint_created'
               and e.get('logical_id') == value['reader']
               and e.get('generation') == value['reader_generation']]
    try:
        if len(origins) != 1:
            raise ValueError('reader_creation_not_unique')
        if origins[0].get('fixture') is True or origins[0].get('real_ros') is not True:
            raise ValueError('reader_not_real_execution')
        identity = endpoint_identity_at(events, origins[0], before_index=send['event_index'])
        if identity['endpoint_gid'] != proof['reader_gid'] or identity['incarnation_ordinal'] != proof['reader_incarnation']:
            raise ValueError('reader_incarnation_differs_from_filter_action')
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        return dict(value, reason='reader_identity_unknown:' + str(exc))
    if send.get('fixture') is True or send.get('backend') == 'offline_fake':
        return dict(value, reason='fixture_publish_not_DDS_evidence')
    value.update(producer_occurrence=producer, reader_gid=proof['reader_gid'],
                 reader_incarnation=proof['reader_incarnation'],
                 message_type=origins[0].get('message_type'), creation_ref=ref(origins[0]),
                 configuration_ref=config['raw_ref'])
    received = [d for d in deliveries if d.get('reader') == value['reader']
                and d.get('reader_generation') == value['reader_generation']
                and d.get('reader_gid') == proof['reader_gid']
                and d.get('send_ref') == value['send_ref']
                and d.get('basis') == 'direct_gid_scoped_publish_occurrence']
    if received:
        receive = events[received[0]['receive_ref']['event_index']]
        # Delivery after another attempted configuration is not observation of
        # the earlier disabled/enabled state, even with identical body bytes.
        if any(e.get('event') == 'topology_action_begin'
               and (e.get('action') or {}).get('op') == 'update_filter'
               and (e.get('action') or {}).get('endpoint_id') == value['reader']
               and (e.get('action') or {}).get('generation') == value['reader_generation']
               and events[proof['update_ref']]['monotonic_ns'] < e['monotonic_ns'] <= receive['monotonic_ns']
               for e in events):
            return dict(value, reason='configuration_changed_before_delivery')
    actual = [d for d in (decisions or {}).get('observations', [])
              if d.get('action_id') == config['action_id'] and d.get('reader') == value['reader']
              and d.get('reader_generation') == value['reader_generation']
              and d.get('reader_gid') == proof['reader_gid'] and d.get('send_ref') == value['send_ref']]
    if (decisions or {}).get('unknowns'):
        return dict(value, reason='native_decision_evidence_incomplete')
    if len(received) > 1 or len(actual) > 1:
        return dict(value, reason='sample_evidence_ambiguous')
    if actual:
        decision = actual[0]
        value['native_decision'] = decision
        if decision['decision'] is False:
            if received:
                return dict(value, reason='native_reject_conflicts_with_delivery')
            return dict(value, status='native_rejected', reason=None,
                        sample_effect_observed=True, filter_effect_observed=True)
        if received:
            return dict(value, status='native_accepted_and_delivered', reason=None,
                        delivery_witness=received[0], sample_effect_observed=True,
                        filter_effect_observed=True)
        return dict(value, reason='native_accept_delivery_unobserved')
    if received:
        return dict(value, status='delivered_while_enabled' if proof['observed_enabled'] else 'delivered_while_disabled',
                    reason=None, delivery_witness=received[0], sample_effect_observed=True)
    return dict(value, reason='missing_delivery_is_not_filter_evidence')


def content_filter_observations(program, events, *, decisions, deliveries, ref, fixture):
    specs = {e['logical_id']: e for e in program['endpoints']}
    configurations = [_configuration(action, specs[action['endpoint_id']], events, ref)
        for action in program['actions'] if action['op'] == 'update_filter' or (
            action['op'] in {'create', 'recreate'}
            and specs.get(action.get('endpoint_id'), {}).get('filter') is not None)]
    samples = []
    for send in (e for e in events if e.get('event') == 'send'):
        attempted = []
        for config in configurations:
            begins = [e for e in events if e.get('event') == 'topology_action_begin'
                      and e.get('action_id') == config['action_id']
                      and e['monotonic_ns'] < send['monotonic_ns']]
            if len(begins) == 1:
                attempted.append((begins[0]['monotonic_ns'], config))
        readers = {item['logical_id'] for _, item in attempted}
        for reader in sorted(readers):
            _, config = max((pair for pair in attempted if pair[1]['logical_id'] == reader), key=lambda pair: pair[0])
            samples.append(_sample(send, config, events, decisions, deliveries, ref, fixture))
    return dict(schema=SCHEMA, declared=bool(configurations), fixture=fixture,
                execution_evidence=not fixture, configuration_observations=configurations,
                sample_observations=samples, filter_false_implies_unmatch=False,
                native_decision_unknowns=(decisions or {}).get('unknowns', []),
                scope='supporting_CFT_observation_not_endpoint_matching_or_candidate')
