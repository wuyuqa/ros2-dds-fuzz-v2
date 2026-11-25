"""Supporting CFT observations in the existing topology facts/report chain.

Configuration and public enabled state are not sample decisions or peer-match
predictions. Missing evidence stays explicit; fixtures never acquire effects.
"""
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.filter_action_receipt import filter_action_evidence
from r2f.runtime.publish_occurrence import producer_occurrence_witness

from r2f.modules.s1.topology_cft_contract import predicate_contract

SCHEMA = 'r2f.cft-sample-observations/v1'


def string_filter_expectation(expression, parameters, text):
    """Independent, deliberately narrow SQL LIKE expectation over full String.data.

    Not a DDS observation. Unsupported predicates, escape syntax and '_' remain
    unknown rather than borrowing the configured model's evaluation flag.
    """
    import re
    if not isinstance(expression, str) or not isinstance(parameters, list) or not isinstance(text, str):
        return None
    if not expression.strip():
        return True if not parameters else None
    if (re.fullmatch(r"\s*data\s+LIKE\s+%0\s*", expression, re.I) is None
            or len(parameters) != 1 or not isinstance(parameters[0], str)):
        return None
    literal = parameters[0]
    if (len(literal) < 2 or literal[0] != "'" or literal[-1] != "'"
            or not literal.isascii() or any(c in literal[1:-1] for c in "_'\\")):
        return None
    parts = literal[1:-1].split('%')
    if len(parts) == 1:
        return text == parts[0]
    if not text.startswith(parts[0]) or not text.endswith(parts[-1]):
        return False
    position, end = len(parts[0]), len(text) - len(parts[-1])
    # Literal chunks consume disjoint input left to right. Unlike a sequence
    # of '.*' regexes, even adversarial wildcard runs cannot backtrack.
    for part in parts[1:-1]:
        if position > end:
            return False
        found = text.find(part, position, end)
        if found < 0:
            return False
        position = found + len(part)
    return position <= end


def _outcome(value, *, status, actual=None, delivered=None, conflict=False):
    """Evaluate an authenticated observation without turning absence into reject."""
    expected = value.get('expected_decision')
    value = dict(value, status=status, actual_decision=actual,
                 delivery_observed=delivered is not None, reason=None,
                 sample_effect_observed=True, violation=None)
    if delivered is not None:
        value['delivery_witness'] = delivered
    if conflict:
        return dict(value, status='evidence_conflict', reason='native_reject_conflicts_with_delivery',
                    sample_effect_observed=None, filter_effect_observed=None)
    if type(expected) is bool and ((type(actual) is bool and actual is not expected)
                                   or (delivered is not None and expected is False)):
        value['violation'] = 'topology.cft_predicate_mismatch'
    return value


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
    expected = string_filter_expectation(config['requested']['expression'],
                                         config['requested']['parameters'], send.get('string_data'))
    value.update(producer_occurrence=producer, reader_gid=proof['reader_gid'],
                 reader_incarnation=proof['reader_incarnation'],
                 message_type=origins[0].get('message_type'), creation_ref=ref(origins[0]),
                 configuration_ref=config['raw_ref'], expected_decision=expected)
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
    if len(received) > 1 or len(actual) > 1:
        return dict(value, reason='sample_evidence_ambiguous')
    if actual:
        decision = actual[0]
        value['native_decision'] = decision
        # An actual false and an authenticated receive of the same occurrence
        # conflict even when a different capture is incomplete. Preserve both.
        if decision['decision'] is False and received:
            return _outcome(value, status='evidence_conflict', actual=False,
                            delivered=received[0], conflict=True)
        if not (decisions or {}).get('unknowns'):
            if decision['decision'] is False:
                return _outcome(dict(value, filter_effect_observed=True),
                                status='native_rejected', actual=False)
            if received:
                return _outcome(dict(value, filter_effect_observed=True),
                                status='native_accepted_and_delivered', actual=True, delivered=received[0])
            return dict(value, reason='native_accept_delivery_unobserved', actual_decision=True)
    if received:
        value = _outcome(value, status='delivered_while_enabled' if proof['observed_enabled']
                         else 'delivered_while_disabled', delivered=received[0])
        value['native_decision_status'] = 'incomplete' if (decisions or {}).get('unknowns') else 'not_observed'
        return value
    if (decisions or {}).get('unknowns'):
        return dict(value, reason='native_decision_evidence_incomplete')
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
                execution_evidence=not fixture, predicate_contract=predicate_contract(),
                configuration_observations=configurations,
                sample_observations=samples, filter_false_implies_unmatch=False,
                review_required=any(c['configuration_status'] != 'observed' for c in configurations)
                    or any(item['status'] in {'unknown', 'evidence_conflict'} or item.get('violation')
                           or item.get('expected_decision') is None for item in samples),
                evidence_conflicts=[item['send_ref'] for item in samples if item['status'] == 'evidence_conflict'],
                native_decision_unknowns=(decisions or {}).get('unknowns', []),
                scope='supporting_CFT_observation_not_endpoint_matching_or_candidate')


def replay_cft_observations(program, events, claimed, *, peer_reference, ref):
    """Recompute CFT outcomes from original raw before normalizing replay facts."""
    from r2f.core.evidence import canonical_hash
    from .topology_filter_decision import native_filter_decision_evidence
    from .topology_sample_attribution import native_delivery_identity
    decisions = native_filter_decision_evidence(program, events, peer_reference=peer_reference, ref=ref)
    sends = [e for e in events if e.get('event') == 'send']
    deliveries = []
    for event in (e for e in events if e.get('event') == 'receive'):
        value = native_delivery_identity(event, sends, events, ref=ref)
        if value is not None:
            deliveries.append(value)
    rebuilt = content_filter_observations(program, events, decisions=decisions,
                                          deliveries=deliveries, ref=ref, fixture=False)
    if canonical_hash(rebuilt) != canonical_hash(claimed):
        raise ValueError('CFT replay observations differ from independent raw recomputation')
    if rebuilt['evidence_conflicts']:
        raise ValueError('CFT reject/delivery conflict cannot qualify a conformance signature')
    keys = ('action_id', 'writer', 'writer_generation', 'reader', 'reader_generation',
            'sequence', 'status', 'expected_decision', 'actual_decision', 'violation')
    return [{k: item.get(k) for k in keys} for item in rebuilt['sample_observations']]
