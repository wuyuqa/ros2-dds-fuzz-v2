"""Raw-backed boundary observations for the canonical writer/reader pair, v1.

This is not a runner, oracle, or N-endpoint topology model. Adapters first audit
execution-specific evidence; this layer additionally binds each state component
to immutable raw events. Missing evidence never becomes configuration truth.
"""
from copy import deepcopy
import json
from pathlib import Path

from r2f.core.evidence import verify_raw
from r2f.core.observation_status import assess_facts
from r2f.core.state_signature import STATE_COMPONENTS
from r2f.instrumentation.sedp import check_offered_requested

SCHEMA = 'r2f.s1-state-projection/v1'
PAIR = ('writer', 'reader')


def _one(events, kind, **fields):
    found = [e for e in events if e.get('event') == kind
             and all(e.get(k) == v for k, v in fields.items())]
    if len(found) != 1:
        raise ValueError('state_projection_expected_one:' + kind)
    return found[0]


def _gid(value):
    # Jazzy can return a padded 24-byte GID for a 16-byte RTPS endpoint GUID.
    if isinstance(value, str) and len(value) == 48 and value[32:] == '0' * 16:
        return value[:32]
    return value


def _generation(event):
    return event.get('endpoint_generation', event.get('generation'))


class _Projection:
    def __init__(self, facts, events, runtime, config, receipt):
        self.facts, self.events = facts, events
        self.runtime, self.config, self.receipt = runtime, config, receipt

    def ref(self, event):
        return {'path': self.runtime['raw_path'], 'sha256': self.runtime['raw_sha256'],
                'event_index': event['event_index']}

    def resolve(self, ref):
        if type(ref) is int:
            found = [e for e in self.events if e['event_index'] == ref]
        else:
            found = [e for e in self.events if e.get('raw_event_ref') == ref and ref is not None]
        if len(found) != 1:
            raise ValueError('state_projection_invalid_boundary_ref')
        return found[0]

    def component(self, side, name, value, events, *, status='observed', reason=None, **detail):
        boundary = self.receipt[side]['boundary_ref']['event_index']
        boundary_ns = self.events[boundary]['monotonic_ns']
        if not events or any(e['event_index'] > boundary or e['monotonic_ns'] > boundary_ns for e in events):
            raise ValueError('state_projection_component_boundary_chronology_invalid:' + side + ':' + name)
        self.facts['state_' + side][name] = value
        refs = {e['event_index']: self.ref(e) for e in events}
        self.receipt[side]['component_refs'][name] = {
            'status': status, 'refs': list(refs.values()), 'reason': reason, **detail}

    def boundary(self, side, event, tokens):
        self.receipt[side]['boundary_ref'] = self.ref(event)
        self.receipt[side]['identity'] = {
            role: {'endpoint_gid': _gid(token['endpoint_gid']), 'pid': token['pid'],
                   'endpoint_generation': _generation(token), 'actual_role': token['role'],
                   'endpoint_ref': self.ref(token)}
            for role, token in tokens.items()}

    def bound(self, event, role, token):
        if (event.get('role') != token['role'] or event.get('pid') != token['pid']
                or _generation(event) != _generation(token)):
            raise ValueError('state_projection_worker_identity_mismatch:' + role)
        if 'worker_generation' in token and event.get('worker_generation') != token['worker_generation']:
            raise ValueError('state_projection_transport_generation_mismatch:' + role)
        if 'endpoint_gid' in event and _gid(event['endpoint_gid']) != _gid(token['endpoint_gid']):
            raise ValueError('state_projection_endpoint_gid_mismatch:' + role)

    def chains(self, tokens, limit, floor_ns):
        """Latest request per role, never a fallback to an older valid command."""
        proof = {}
        for role, token in tokens.items():
            requests = [e for e in self.events if e.get('event') in
                        ('match_snapshot_requested', 'graph_snapshot_requested')
                        and e.get('role') == token['role'] and e['event_index'] < limit['event_index']]
            if not requests:
                raise ValueError('state_projection_fresh_snapshot_request_missing:' + role)
            request = requests[-1]
            cid = request.get('command_id')
            if type(cid) is not int:
                raise ValueError('state_projection_snapshot_command_missing:' + role)
            match, graph, ack = [_one(self.events, name, role=token['role'], command_id=cid)
                                 for name in ('match_snapshot', 'graph_snapshot', 'command_done')]
            requested, sampled = request.get('requested_monotonic_ns'), match.get('sampled_monotonic_ns')
            stamps = [requested, request['monotonic_ns'], sampled, match['monotonic_ns'],
                      graph['monotonic_ns'], ack['monotonic_ns'], limit['monotonic_ns']]
            indexes = [e['event_index'] for e in (request, match, graph, ack, limit)]
            if (any(type(t) is not int for t in stamps) or stamps != sorted(stamps)
                    or requested < floor_ns or len(set(indexes[:4])) != 4 or indexes != sorted(indexes)):
                raise ValueError('state_projection_stale_snapshot_or_chronology:' + role)
            for event in (match, graph, ack):
                self.bound(event, role, token)
            count = match.get('matched_count')
            if (type(count) is not int or count < 0 or match.get('count_api') !=
                    ('get_subscription_count' if role == 'writer' else 'get_publisher_count')):
                raise ValueError('state_projection_invalid_public_count:' + role)
            proof[role] = (request, match, graph, ack)
        if len({p[0]['command_id'] for p in proof.values()}) != len(proof):
            raise ValueError('state_projection_snapshot_commands_not_independent')
        return proof

    def live_at(self, tokens, boundary):
        for token in tokens.values():
            for event in self.events:
                if (event.get('event') in ('worker_shutdown', 'process_exit')
                        and event.get('role') == token['role'] and event.get('pid') == token['pid']
                        and token['event_index'] < event['event_index'] <= boundary['event_index']):
                    raise ValueError('state_projection_pair_not_live_at_boundary')

    def pair_components(self, side, tokens, chains):
        refs = list(tokens.values())
        self.component(side, 'live_roles', 'both', refs + [p[2] for p in chains.values()])
        created = [tokens[r].get('created_monotonic_ns', tokens[r]['monotonic_ns']) for r in PAIR]
        order = 'writer_first' if created[0] < created[1] else 'reader_first' if created[1] < created[0] else 'unresolved'
        self.component(side, 'creation_order_class', order, refs)
        qos = {}
        graph_refs = []
        counts = []
        for role, chain in chains.items():
            graph_refs.extend(chain)
            graph, count = chain[2], chain[1]['matched_count']
            counts.append(count)
            for member, key in (('writer', 'publishers'), ('reader', 'subscriptions')):
                rows = [r for r in graph.get(key, [])
                        if _gid(r.get('endpoint_gid')) == _gid(tokens[member]['endpoint_gid'])]
                if len(rows) != 1 or not isinstance(rows[0].get('qos'), dict):
                    raise ValueError('state_projection_graph_pair_identity_missing:' + member)
                actual = rows[0]['qos']
                if member in qos and qos[member] != actual:
                    raise ValueError('state_projection_graph_qos_sources_disagree:' + member)
                qos[member] = actual
        # Counts are participant-wide. Extra endpoints make them ambiguous for
        # this canonical pair, even though inventory itself remains observable.
        extras = any(len(p[2].get('publishers', [])) != 1 or len(p[2].get('subscriptions', [])) != 1
                     for p in chains.values())
        match = ('unknown' if extras else 'bidirectional' if all(c > 0 for c in counts)
                 else 'single_side' if any(c > 0 for c in counts) else 'none')
        self.component(side, 'observed_match_class', match, graph_refs,
                       reason='participant_counts_not_pair_specific' if extras else None,
                       scope='canonical_pair_public_counts')
        observed = {}
        for role in PAIR:
            fields = {}
            for policy, key in (('reliability', 'reliability'), ('durability', 'durability'),
                                ('liveliness.kind', 'liveliness'), ('deadline', 'deadline_ns'),
                                ('liveliness.lease_duration', 'liveliness_lease_duration_ns')):
                value = qos[role].get(key)
                if policy in ('deadline', 'liveliness.lease_duration'):
                    value = ({'infinite': True} if value == 9223372036854775807 else
                             {'nanoseconds': value} if type(value) is int and value >= 0 else None)
                if value is not None:
                    fields[policy] = {'status': 'observed', 'value': value}
            observed[role] = fields
        compatibility = check_offered_requested(observed['writer'], observed['reader'])
        source_disagreement = []
        observed_keys = ('reliability', 'durability', 'deadline_ns', 'liveliness', 'liveliness_lease_duration_ns')
        create_refs = []
        for role, token in tokens.items():
            actual = token.get('qos')
            if not isinstance(actual, dict):
                raise ValueError('state_projection_actual_create_qos_missing:' + role)
            calls = [e for e in self.events if e.get('event') == 'endpoint_create_begin'
                     and e.get('role') == token['role'] and e.get('pid') == token['pid']
                     and _generation(e) == _generation(token) and e['event_index'] < token['event_index']]
            if calls:
                call = calls[-1]
                if call.get('qos') != actual:
                    raise ValueError('state_projection_actual_create_call_qos_mismatch:' + role)
                create_refs.append(call)
            for key in observed_keys:
                if key in actual and key in qos[role] and actual[key] != qos[role][key]:
                    source_disagreement.append(role + ':' + key)
        value = compatibility['compatible'] if not source_disagreement else None
        self.component(side, 'compatibility_class', 'compatible' if value is True else
                       'incompatible' if value is False else 'unknown', refs + graph_refs + create_refs,
                       reason=('create_graph_policy_disagreement' if source_disagreement else
                               'unobserved_matching_policies' if value is None else None),
                       policy_scope=compatibility, create_graph_disagreements=source_disagreement,
                       actual_create_qos={role: token['qos'] for role, token in tokens.items()},
                       excluded_policies=['history', 'depth', 'resource_limits'],
                       limitation='Only listed observed offered/requested policies; not all DDS matching conditions.')

    def absent_components(self, side, tokens, boundary, reason):
        self.component(side, 'live_roles', 'writer' if tokens else 'none',
                       list(tokens.values()) + [boundary])
        for name, value in (('creation_order_class', 'unresolved'),
                            ('compatibility_class', 'unknown'), ('observed_match_class', 'none')):
            self.component(side, name, value, [boundary], status='not_applicable', reason=reason)

    def delivery(self, side, opening, closing, tokens):
        contract = _one(self.events, 'delivery_contract')
        expected = contract.get('expected_sequences')
        if (not isinstance(expected, list) or not expected or len(set(expected)) != len(expected)
                or contract.get('sentinel_sequence') != expected[-1]
                or not contract['event_index'] < opening['event_index'] < closing['event_index']):
            raise ValueError('state_projection_delivery_contract_or_boundary_missing')
        phase = 'post_restore' if opening['window'] == 'post_restore_delivery' else 'post_match'
        sent, received, omissions = [], [], []
        for event in self.events:
            if not opening['event_index'] < event['event_index'] < closing['event_index']:
                continue
            if event.get('event') not in ('send', 'receive', 'harness_omission'):
                continue
            if event.get('phase') not in (None, phase):
                continue
            if not opening['monotonic_ns'] <= event['monotonic_ns'] <= closing['monotonic_ns']:
                raise ValueError('state_projection_delivery_clock_outside_window')
            kind = event['event']
            role = 'reader' if kind == 'receive' else 'writer'
            self.bound(event, role, tokens[role])
            if kind == 'send':
                if (event.get('publish_returned') is not True
                        or _gid(event.get('writer_gid')) != _gid(tokens['writer']['endpoint_gid'])):
                    raise ValueError('state_projection_unacknowledged_send')
                sent.append(event)
            elif kind == 'receive':
                if (_gid(event.get('reader_gid')) != _gid(tokens['reader']['endpoint_gid'])
                        or event.get('payload_run_id') != self.config['run_id']
                        or event.get('publisher_gid') is not None and
                        _gid(event['publisher_gid']) != _gid(tokens['writer']['endpoint_gid'])):
                    raise ValueError('state_projection_delivery_identity_mismatch')
                received.append(event)
            else:
                omissions.append(event)
        sequences = [e.get('send_sequence') for e in sent + omissions]
        if sorted(sequences) != sorted(expected):
            raise ValueError('state_projection_delivery_obligations_missing')
        observed = {e.get('send_sequence') for e in received}
        if not observed.issubset(set(expected)):
            raise ValueError('state_projection_delivery_sequence_outside_contract')
        missing = [s for s in expected if s not in observed]
        self.component(side, 'delivery_closure_class', 'open' if missing else 'closed_within_contract',
                       [contract, opening, closing] + sent + received + omissions,
                       reason='finite_contract_has_missing_sequences' if missing else None,
                       expected_sequences=expected, missing_sequences=missing,
                       sentinel_received=expected[-1] in observed,
                       scope='explicit_finite_observed_delivery_contract_not_history_based_guarantee')

    def run(self):
        target = str(self.config.get('target_id', ''))
        family = ('late' if target.startswith(('discovery.late_endpoint', 'discovery.late_join_delivery')) else
                  'recovery' if target.startswith('discovery.interruption_recovery') else
                  'projection' if target.startswith('builtin.discovery_projection_reconciliation') else None)
        if (self.runtime.get('real_execution') is not True
                or self.runtime.get('runtime_status') != 'completed'):
            raise ValueError('state_projection_not_completed_execution')
        verify_raw(self.runtime['raw_path'], self.runtime['raw_sha256'])
        raw = [json.loads(line) for line in Path(self.runtime['raw_path']).read_text().splitlines() if line.strip()]
        if raw != self.events or self.runtime.get('events') != self.events:
            raise ValueError('state_projection_events_differ_from_raw')
        for index, event in enumerate(self.events):
            if (type(event.get('event_index')) is not int or event['event_index'] != index
                    or type(event.get('monotonic_ns')) is not int
                    or event.get('run_id') != self.config['run_id']
                    or event.get('domain_id') != self.config['domain_id']):
                raise ValueError('state_projection_raw_index_or_identity_invalid')
        if any(e.get('event') in ('worker_error', 'worker_unexpected_exit', 'runtime_abort') for e in self.events):
            raise ValueError('state_projection_raw_execution_error')
        end = _one(self.events, 'runtime_end')
        if end.get('real_execution') is not True or end.get('runtime_status') != 'completed':
            raise ValueError('state_projection_runtime_end_not_completed')
        if self.facts.get('evidence_error'):
            if any(sum(e.get('event') == 'endpoint_created' and e.get('role') == role
                       for e in self.events) > 1 for role in PAIR):
                self.receipt['status'] = 'unsupported'
                raise ValueError('state_projection_adapter_canonical_generation_history_not_supported')
            raise ValueError('state_projection_adapter_evidence_error:' + str(self.facts['evidence_error']))
        for flag in ('environment_healthy', 'process_healthy', 'preconditions_reached', 'endpoint_identity_complete'):
            if self.facts.get(flag) is not True:
                raise ValueError('state_projection_adapter_fact_incomplete:' + flag)
        if (self.facts.get('window') or {}).get('complete') is not True:
            raise ValueError('state_projection_adapter_window_incomplete')
        if family is None:
            self.receipt['status'] = 'unsupported'
            raise ValueError('state_projection_unsupported_target')
        begin = _one(self.events, 'mutation_begin' if self.config.get('arm') == 'mutated' else 'control_begin')
        tokens = {}
        for role in PAIR:
            identity = self.facts.get(role) or {}
            found = [e for e in self.events if e.get('event') == 'endpoint_created'
                     and _gid(e.get('endpoint_gid')) == _gid(identity.get('id'))
                     and _generation(e) == identity.get('generation')]
            if len(found) != 1:
                raise ValueError('state_projection_adapter_endpoint_identity_mismatch:' + role)
            tokens[role] = found[0]
        metadata = self.runtime.get('execution_metadata') or self.runtime.get('execution') or {}
        workers = metadata.get('workers', [])
        if isinstance(workers, dict):
            workers = list(workers.values())
        for role, token in tokens.items():
            identity = self.facts.get(role) or {}
            if (not isinstance(token.get('endpoint_gid'), str) or not token['endpoint_gid']
                    or type(token.get('pid')) is not int or token['pid'] <= 0
                    or not isinstance(_generation(token), str) or not _generation(token)
                    or _gid(identity.get('id')) != _gid(token['endpoint_gid'])
                    or identity.get('generation') != _generation(token)
                    or any(key in identity and identity[key] != token.get(key)
                           for key in ('pid', 'process_start_ticks'))
                    or identity.get('created_ns') != (token['monotonic_ns'] if family == 'projection' else
                                                    token.get('created_monotonic_ns', token['monotonic_ns']))
                    or not any(w.get('role') == token['role'] and w.get('pid') == token['pid']
                               and w.get('returncode') == 0 for w in workers)):
                raise ValueError('state_projection_adapter_endpoint_identity_mismatch:' + role)
        if len({t['pid'] for t in tokens.values()}) != 2 or len({_gid(t['endpoint_gid']) for t in tokens.values()}) != 2:
            raise ValueError('state_projection_pair_not_independent')
        # Inventory is evidence plumbing, deliberately outside STATE_COMPONENTS.
        self.receipt['endpoint_inventory'] = [
            {'role': e.get('role'), 'endpoint_gid': _gid(e.get('endpoint_gid')), 'pid': e.get('pid'),
             'endpoint_generation': _generation(e), 'endpoint_ref': self.ref(e)}
            for e in self.events if e.get('event') == 'endpoint_created']
        if family == 'recovery':
            before = _one(self.events, 'discovery_precondition_snapshot')
            if (self.resolve(begin.get('precondition_snapshot_ref')) is not before
                    or before.get('satisfied') is not True or not before['event_index'] < begin['event_index']
                    or before['monotonic_ns'] > begin['monotonic_ns']):
                raise ValueError('state_projection_precondition_snapshot_boundary_invalid')
            before_tokens = {}
            for role in PAIR:
                found = [e for e in self.events if e.get('event') == 'endpoint_created'
                         and e['event_index'] < before['event_index']
                         and e.get('endpoint_gid') == before.get(role + '_gid')
                         and e.get('pid') == before.get('worker_pids', {}).get(role)]
                if len(found) != 1:
                    raise ValueError('state_projection_precondition_endpoint_identity_mismatch:' + role)
                before_tokens[role] = found[0]
            self.live_at(before_tokens, before)
            self.boundary('before', before, before_tokens)
            proof = self.chains(before_tokens, before, max(t['monotonic_ns'] for t in before_tokens.values()))
            for role in PAIR:
                matched = before.get('matched', {})
                if (self.resolve(matched.get('graphs', {}).get(role)) is not proof[role][2]
                        or self.resolve(matched.get('matches', {}).get(role)) is not proof[role][1]
                        or before.get(role + '_gid') != before_tokens[role]['endpoint_gid']
                        or before.get('worker_pids', {}).get(role) != before_tokens[role]['pid']):
                    raise ValueError('state_projection_stale_precondition_snapshot:' + role)
            self.pair_components('before', before_tokens, proof)
            setup = _one(self.events, 'discovery_network_setup')
            sr = setup.get('receipt', {})
            if (setup['event_index'] >= before['event_index'] or sr.get('health_ok') is not True
                    or not (sr.get('drop_installed') is False or sr.get('mutation_installed') is False)):
                raise ValueError('state_projection_initial_network_not_observed_neutral')
            network_before = [setup, before]
        else:
            before = begin
            snap = begin.get('preconditions_snapshot', {})
            wanted = ({'writer_created': True, 'writer_prejoin_published': True, 'reader_created': False}
                      if family == 'late' else {'namespace_ready': True, 'observer_ready': True, 'endpoints_created': False})
            if any(snap.get(k) is not v for k, v in wanted.items()):
                raise ValueError('state_projection_precreate_snapshot_missing')
            existing = {}
            for role, token in tokens.items():
                prior = [e for e in self.events if e.get('event') == 'endpoint_created'
                         and e.get('role') == token['role'] and e['event_index'] < begin['event_index']]
                if prior:
                    existing[role] = prior[-1]
            if set(existing) != ({'writer'} if family == 'late' else set()):
                raise ValueError('state_projection_precreate_chronology_mismatch')
            self.live_at(existing, before)
            self.boundary('before', before, existing)
            self.absent_components('before', existing, before,
                                   'reader_not_yet_created' if family == 'late' else 'endpoints_not_yet_created')
            network_before = [begin]
        self.component('before', 'network_phase', 'normal', network_before,
                       status='observed' if family == 'recovery' else 'not_applicable',
                       reason=None if family == 'recovery' else 'no_network_mutation_in_this_target')
        self.component('before', 'recovery_cycle_bucket', '0', [before],
                       reason='no_recovery_cycle_before_mutation')
        self.component('before', 'delivery_closure_class', 'no_obligation', [before],
                       status='not_applicable', reason='post_mutation_delivery_contract_not_open')

        window = 'endpoint_creation' if family == 'projection' else 'post_restore_delivery' if family == 'recovery' else 'post_match_delivery'
        opening, closing = [_one(self.events, k, window=window) for k in ('window_open', 'window_close')]
        if (not begin['event_index'] < opening['event_index'] < closing['event_index'] < end['event_index']
                or not begin['monotonic_ns'] <= opening['monotonic_ns'] < closing['monotonic_ns'] <= end['monotonic_ns']):
            raise ValueError('state_projection_observation_window_chronology_invalid')
        if family == 'projection':
            after = closing
            floor = max(t['monotonic_ns'] for t in tokens.values())
        else:
            requests = [e for e in self.events if e.get('event') in
                        ('match_snapshot_requested', 'graph_snapshot_requested')
                        and e.get('role') in {t['role'] for t in tokens.values()} and closing['event_index'] < e['event_index'] < end['event_index']]
            if len(requests) < 2:
                raise ValueError('state_projection_postclose_snapshot_missing')
            acks = [_one(self.events, 'command_done', role=e['role'], command_id=e.get('command_id'))
                    for e in requests]
            last_ack = max(acks, key=lambda e: e['event_index'])
            # Poll merges preserve each worker's order, not cross-worker time
            # order. Prefer the actual controller observation after both ACKs;
            # never fabricate a timestamp or use cleanup/runtime_end as a cut.
            boundaries = [e for e in self.events
                          if e.get('role') == 'controller' and e.get('pid') == begin.get('pid')
                          and last_ack['event_index'] < e['event_index'] < end['event_index']
                          and ((family == 'late' and e.get('event') == 'identity_observation')
                               or (family == 'recovery' and e.get('event') == 'state_step_after'
                                   and e.get('step_id') == 'observe_final_graph'
                                   and e.get('receipt_kind') == 'observation'))]
            after = boundaries[0] if boundaries else last_ack
            # Older ACK-only evidence remains usable only if that actual event
            # covers every ACK in *both* index and monotonic time dimensions.
            if (any(ack['event_index'] > after['event_index'] or ack['monotonic_ns'] > after['monotonic_ns']
                    for ack in acks) or after['monotonic_ns'] > end['monotonic_ns']
                    or (boundaries and family == 'recovery' and after.get('satisfied') is not True)):
                raise ValueError('state_projection_postclose_boundary_chronology_invalid')
            floor = closing['monotonic_ns']
        for event in self.events:
            if event.get('event') in ('worker_stopping', 'worker_shutdown', 'process_exit',
                                     'discovery_reader_destroyed', 'discovery_writer_destroyed'):
                for token in tokens.values():
                    if (event.get('role') == token['role'] and event.get('pid') == token['pid']
                            and token['event_index'] < event['event_index']
                            and (event['event_index'] <= after['event_index']
                                 or event['monotonic_ns'] <= after['monotonic_ns'])):
                        raise ValueError('state_projection_pair_not_live_at_after_boundary')
        if family == 'recovery':
            for role in PAIR:
                old, new = before_tokens[role], tokens[role]
                if old is new:
                    continue
                stops = [e for e in self.events if e.get('event') == 'worker_shutdown'
                         and e.get('role') == old['role'] and e.get('pid') == old['pid']
                         and _generation(e) == _generation(old)
                         and before['event_index'] < e['event_index'] < new['event_index']]
                starts = [e for e in self.events if e.get('event') == 'process_started'
                          and e.get('role') == new['role'] and e.get('pid') == new['pid']
                          and before['event_index'] < e['event_index'] < new['event_index']]
                if len(stops) != 1 or len(starts) != 1 or stops[0].get('stop_command_id') is None:
                    raise ValueError('state_projection_generation_causal_chain_missing:' + role)
                shutdown, started = stops[0], starts[0]
                requests = [e for e in self.events if e.get('event') == 'worker_command_requested'
                            and e.get('command') == 'stop' and e.get('role') == old['role']
                            and e.get('pid') == old['pid']
                            and e.get('command_id') == shutdown['stop_command_id']
                            and before['event_index'] < e['event_index'] < shutdown['event_index']]
                exits = [e for e in self.events if e.get('returncode') == 0
                         and shutdown['event_index'] < e['event_index'] < started['event_index']
                         and ((e.get('event') == 'process_exit' and e.get('pid') == old['pid'])
                              or (e.get('event') in ('discovery_reader_destroyed', 'discovery_writer_destroyed')
                                  and e.get('process_exit_observed') is True
                                  and e.get('worker_identity', {}).get('pid') == old['pid']))]
                if len(requests) != 1 or len(exits) != 1:
                    raise ValueError('state_projection_generation_stop_exit_chain_missing:' + role)
                self.bound(requests[0], role, old)
                self.bound(shutdown, role, old)
                exit_event = exits[0]
                if exit_event['event'] == 'process_exit':
                    self.bound(exit_event, role, old)
                else:
                    identity = exit_event.get('worker_identity', {})
                    if (identity.get('endpoint_generation') != _generation(old)
                            or exit_event.get('command_id') != shutdown['stop_command_id']):
                        raise ValueError('state_projection_generation_exit_identity_mismatch:' + role)
                if (('endpoint_generation' in started and _generation(started) != _generation(new))
                        or ('worker_generation' in new and
                            started.get('worker_generation') != new['worker_generation'])):
                    raise ValueError('state_projection_generation_start_identity_mismatch:' + role)
                chain = (before, requests[0], shutdown, exit_event, started, new)
                times = [e['monotonic_ns'] for e in chain]
                indexes = [e['event_index'] for e in chain]
                if times != sorted(times) or indexes != sorted(indexes):
                    raise ValueError('state_projection_generation_chain_chronology_invalid:' + role)
                self.receipt.setdefault('generation_transitions', []).append({
                    'canonical_role': role, 'old_endpoint_ref': self.ref(old),
                    'stop_request_ref': self.ref(requests[0]), 'exit_ref': self.ref(exits[0]),
                    'shutdown_ref': self.ref(shutdown), 'process_ref': self.ref(started),
                    'new_endpoint_ref': self.ref(new)})
        self.boundary('after', after, tokens)
        proof = self.chains(tokens, after, floor)
        self.pair_components('after', tokens, proof)
        if family == 'recovery':
            verified = [e for e in self.events if e.get('event') == 'discovery_window_restore_verified']
            boundaries = [e for e in self.events if e.get('event') == 'discovery_restore_boundary']
            plans = self.config.get('environment_windows')
            expected_count = len(plans) if isinstance(plans, list) else 1
            if len(boundaries) != expected_count:
                raise ValueError('state_projection_recovery_window_count_mismatch')
            network_refs = []
            previous = begin
            anchor = before
            for index, boundary in enumerate(boundaries):
                if isinstance(plans, list):
                    if (boundary.get('window_index') != index
                            or boundary.get('window_id') != plans[index].get('window_id')):
                        raise ValueError('state_projection_recovery_window_plan_mismatch')
                if boundary.get('anchor_ref') is not None and self.resolve(boundary['anchor_ref']) is not anchor:
                    raise ValueError('state_projection_recovery_window_anchor_mismatch')
                confirmations = [e for e in self.events if e.get('event') == 'discovery_recovery_confirmed'
                                 and e.get('window_index') == boundary.get('window_index')]
                if not begin['event_index'] < boundary['event_index'] < opening['event_index'] or boundary.get('restored') is not True:
                    raise ValueError('state_projection_restore_boundary_invalid')
                if self.config.get('arm') == 'mutated':
                    if len(confirmations) != 1 or confirmations[0].get('receipt', {}).get('recovery_verified') is not True:
                        raise ValueError('state_projection_recovery_confirmation_missing')
                    confirm = confirmations[0]
                    if not previous['event_index'] < boundary['event_index'] < confirm['event_index'] < opening['event_index']:
                        raise ValueError('state_projection_recovery_chain_chronology_invalid')
                    if verified:
                        checks = [e for e in verified if e.get('window_index') == boundary.get('window_index')]
                        if len(checks) != 1 or checks[0].get('recovery_confirmed') is not True or checks[0].get('restored') is not True:
                            raise ValueError('state_projection_recovery_window_identity_mismatch')
                        previous = checks[0]
                        network_refs.append(previous)
                    else:
                        previous = confirm
                    network_refs.append(confirm)
                anchor = confirm if self.config.get('arm') == 'mutated' else boundary
                network_refs.append(boundary)
            self.component('after', 'network_phase', 'restored_verified', network_refs)
            self.component('after', 'recovery_cycle_bucket', '1' if expected_count == 1 else '2plus', network_refs)
        else:
            for name, value in (('network_phase', 'normal'), ('recovery_cycle_bucket', '0')):
                self.component('after', name, value, [begin, closing], status='not_applicable',
                               reason='no_network_mutation_in_this_target')
        if family == 'projection':
            self.component('after', 'delivery_closure_class', 'no_obligation', [opening, closing],
                           status='not_applicable', reason='projection_window_has_no_delivery_contract')
        else:
            self.delivery('after', opening, closing, tokens)
        for side in ('before', 'after'):
            health_refs = [_one(self.events, 'environment_preflight')]
            self.component(side, 'observation_completeness', 'complete',
                           health_refs + [before if side == 'before' else closing],
                           scope='boundary_evidence_not_oracle_verdict')
            self.facts['state_' + side].update(observation_complete=True, window_complete=True,
                                              endpoint_identity_consistent=True)
        # Base flags belong to the adapter; a projection cannot upgrade them.
        self.receipt['status'] = 'observed'


def attach_state_observations(facts, events, runtime, config):
    """Base observation completeness and state-edge completeness are independent.

    Projection absence never changes an already audited oracle/endpoint identity.
    A contradictory raw identity/index/chronology *does* invalidate base evidence.
    """
    if config.get('target_id') == 'discovery.interruption_recovery.fuzz_windows_v1':
        facts = deepcopy(facts)
        operations = facts.get('operation_receipts')
        if not operations or any(o.get('verified') is not True or o.get('status') != 'executed' for o in operations):
            facts['observation_complete'] = False
            facts.setdefault('evidence_error', 'f2_operation_receipts_not_verified')
    base = assess_facts(facts)
    result = deepcopy(facts)
    receipt = {'schema': SCHEMA, 'status': 'incomplete', 'scope': 'canonical_pair',
               'raw_path': runtime.get('raw_path'), 'raw_sha256': runtime.get('raw_sha256'),
               'before': {'boundary_ref': None, 'component_refs': {}, 'identity': {}},
               'after': {'boundary_ref': None, 'component_refs': {}, 'identity': {}}, 'reasons': []}
    result['state_projection_receipt'] = receipt
    identity_ok = result.get('endpoint_identity_consistent', result.get('endpoint_identity_complete') is True)
    for side in ('before', 'after'):
        result['state_' + side] = {k: 'unknown' for k in STATE_COMPONENTS}
        result['state_' + side].update(creation_order_class='unresolved', observation_completeness='incomplete',
                                      observation_complete=False, window_complete=False,
                                      endpoint_identity_consistent=identity_ok)
    result['observation_complete'] = base['complete']
    try:
        _Projection(result, events, runtime, config, receipt).run()
        assessment = assess_facts(result)
        if base['complete'] is not True:
            raise ValueError('state_projection_canonical_incomplete:' + ','.join(base['reasons']))
        if assessment.get('complete') is not True:
            raise ValueError('state_projection_canonical_incomplete:' + ','.join(assessment.get('reasons', [])))
    except (ValueError, KeyError, TypeError, OSError, IndexError) as exc:
        reason = str(exc)
        if receipt['status'] != 'unsupported':
            receipt['status'] = 'incomplete'
        receipt['reasons'].append(reason)
        # Missing state-specific samples are an observation limit, not a forged
        # endpoint. Conversely these contradictions corrupt primary evidence too.
        contradiction = any(word in reason for word in (
            'mismatch', 'differ', 'invalid', 'chronology', 'not_live', 'stale_',
            'not_completed_execution', 'runtime_end_not_completed', 'raw_execution_error'))
        primary_missing = (isinstance(exc, (OSError, TypeError)) or not runtime.get('raw_path')
                           or not runtime.get('raw_sha256')
                           or reason in ('state_projection_expected_one:runtime_end',
                                         'state_projection_expected_one:environment_preflight'))
        if contradiction or primary_missing:
            result['observation_complete'] = False
            if any(word in reason for word in ('identity', 'generation', 'gid', 'not_live')):
                result['endpoint_identity_consistent'] = False
        else:
            result['observation_complete'] = base['complete']
        for side in ('before', 'after'):
            state = result['state_' + side]
            state.update(observation_completeness='incomplete', observation_complete=False,
                         window_complete=False,
                         endpoint_identity_consistent=result.get('endpoint_identity_consistent', identity_ok))
    return result
