"""Audit third-target evidence and map it into the shared core execution gates.

Only the runtime applies the carrier. This adapter reads immutable raw tokens,
namespace receipts and independently captured normal discovery traffic.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import struct

from r2f.core.evidence import verify_raw
from r2f.core.state_machine import StepReceipt
from r2f.modules.s1.facts import integer
from .runtime_adapter import _one, _iso, fresh_match_proof

IMPLEMENTATION = 'isolated_netns_nft_discovery_udp_drop'


def _ref(event):
    return event.get('raw_event_ref', event['event_index'])


def _resolve(events, reference):
    found = [e for e in events if (type(reference) is int and e['event_index'] == reference)
             or (isinstance(reference, str) and e.get('raw_event_ref') == reference)]
    if len(found) != 1:
        raise ValueError('recovery raw reference is absent or ambiguous')
    return found[0]


def _isolation(proof):
    if (proof.get('isolated') is not True or proof.get('interfaces') != ['lo']
            or proof.get('observed_net_namespace') != proof.get('net_namespace')
            or proof.get('observed_user_namespace') != proof.get('user_namespace')
            or proof.get('net_namespace') == proof.get('host_net_namespace')
            or proof.get('user_namespace') == proof.get('host_user_namespace')
            or type(proof.get('host_uid')) is not int or proof['host_uid'] <= 0
            or proof.get('uid_map') != [[0, proof['host_uid'], 1]]
            or proof.get('euid') != 0 or type(proof.get('cap_eff')) is not int
            or not proof['cap_eff'] & (1 << 12) or proof.get('host_cap_eff', 1 << 12) & (1 << 12)):
        raise ValueError('isolated loopback user/net namespace identity incomplete')
    return proof


def _bindings(receipt, tokens, isolation, domain):
    if receipt.get('only_pair_bound_udp_sockets') is not True:
        raise ValueError('discovery bindings not restricted to the endpoint pair')
    if receipt.get('worker_pids') != {r: t['pid'] for r, t in tokens.items()}:
        raise ValueError('discovery binding worker identity differs')
    current = _isolation(receipt['isolation'])
    if any(current[k] != isolation[k] for k in ('net_namespace', 'user_namespace')):
        raise ValueError('discovery binding namespace differs')
    starts = receipt['worker_start_ticks']
    if any(not str(starts.get(r, '')).isdigit() for r in tokens):
        raise ValueError('discovery binding process generation missing')
    base = 7400 + 250 * domain
    selected = {base, *(base + 10 + 2 * p for p in range(32))}
    bindings = receipt['bindings']
    if not bindings or any(not b.get('roles') or set(b['roles']) - set(tokens)
                           or b.get('selected_discovery_port') != (b['port'] in selected) for b in bindings):
        raise ValueError('discovery UDP ownership/port selection invalid')
    if any(not any(r in b['roles'] and b['port'] in selected - {base} for b in bindings) for r in tokens):
        raise ValueError('pair unicast discovery bindings missing')
    return selected


def _step_evidence(events, actual):
    before = _one(events, 'state_step_before', step_id=actual['step_id'])
    after = _one(events, 'state_step_after', step_id=actual['step_id'])
    if actual.get('satisfied') is not True or after.get('satisfied') is not True:
        raise ValueError('recovery step not actually satisfied')
    for key, event in (('before', before), ('after', after)):
        value = dict(actual[key])
        reference = value.pop('raw_event_ref', None)
        raw_value = dict(event[key])
        raw_value.pop('raw_event_ref', None)
        if value != raw_value or _resolve(events, reference) != event:
            raise ValueError('recovery step snapshot/raw reference mismatch')
    if (not before['event_index'] < after['event_index']
            or not before['monotonic_ns'] <= actual['after']['monotonic_ns'] <= after['monotonic_ns']
            or after.get('evidence') != actual.get('evidence')):
        raise ValueError('recovery step chronology/evidence mismatch')
    return before, after


def _namespace_ready(events, create_before, create_started_ns):
    """Prove namespace readiness before starting endpoint creation, from raw only."""
    admission = _one(events, 'namespace_host_admission_verified')
    setup_event = _one(events, 'discovery_network_setup')
    ready_event = _one(events, 'discovery_observer_ready')
    isolation = _isolation(admission['isolation'])
    evidence = (admission, setup_event, ready_event)
    if (not admission['event_index'] < setup_event['event_index'] < ready_event['event_index'] < create_before['event_index']
            or not admission['monotonic_ns'] <= setup_event['monotonic_ns'] <= ready_event['monotonic_ns'] <= create_started_ns <= create_before['monotonic_ns']):
        raise ValueError('namespace readiness evidence was not established before create_endpoints')
    # All three receipts must belong to this namespace controller and execution.
    for event in (*evidence, create_before):
        if any(event.get(k) != admission.get(k) or admission.get(k) is None
               for k in ('run_id', 'domain_id', 'pid')):
            raise ValueError('namespace readiness controller/execution identity differs')
    setup = setup_event['receipt']
    setup_isolation = _isolation(setup['isolation'])
    if (any(setup_isolation[k] != isolation[k] for k in ('net_namespace', 'user_namespace', 'host_net_namespace', 'host_user_namespace', 'uid_map'))
            or setup.get('health_ok') is not True or setup.get('drop_installed') is not False
            or setup.get('hit') is not False or setup.get('counter_observed') is not False
            or not admission['monotonic_ns'] <= setup['monotonic_ns'] <= setup_event['monotonic_ns']):
        raise ValueError('namespace readiness setup is not healthy/neutral in admitted namespace')
    _command(setup['command'], ['nft', '-j', 'list', 'ruleset'], isolation)
    if (not admission['monotonic_ns'] <= setup['command']['started_monotonic_ns']
            <= setup['command']['ended_monotonic_ns'] <= setup['monotonic_ns']
            or json.loads(setup['command']['stdout']) != setup['initial_nft']
            or any('table' in entry for entry in setup['initial_nft']['nftables'])):
        raise ValueError('namespace readiness initial ruleset evidence invalid')
    ready = ready_event['receipt']
    namespace = ready['namespace']
    if (ready.get('status') != 'ready' or ready.get('interface') != 'lo' or ready.get('linktype') != 1
            or ready.get('timestamp_clock') != 'kernel_SO_TIMESTAMPNS_realtime'
            or ready.get('run_id') != admission['run_id'] or ready.get('domain_id') != admission['domain_id']
            or ready.get('process', {}).get('pid') != admission['pid']
            or not isinstance(ready.get('spec_sha256'), str) or len(ready['spec_sha256']) != 64
            or namespace.get('isolated') is not True or namespace.get('interfaces') != ['lo']
            or any(namespace.get(k) != isolation[k] for k in ('net_namespace', 'user_namespace', 'uid_map'))
            or type(namespace.get('cap_eff')) is not int or not namespace['cap_eff'] & (1 << 13)
            or type(namespace.get('loopback_flags')) is not int or namespace['loopback_flags'] & 9 != 9
            or not setup_event['monotonic_ns'] <= ready['monotonic_ns'] <= ready_event['monotonic_ns']):
        raise ValueError('namespace readiness observer evidence incomplete or mismatched')
    return {event['event']: {'raw_event_ref': _ref(event), 'event_index': event['event_index']}
            for event in evidence}


def recovery_receipts(scenario, runtime, events):
    env = _one(events, 'environment_preflight')
    locks = _one(events, 'isolation_locks_acquired')
    create_before = _one(events, 'state_step_before', step_id='create_endpoints')
    readiness = _namespace_ready(events, create_before, create_before['before']['monotonic_ns'])
    state = {'environment_ok': env.get('ok') is True, 'locks_held': len(locks.get('locks', [])) == 3,
             'namespace_ready': True}
    actuals = runtime.get('precondition_receipts', [])
    if len(actuals) != len(scenario.state_steps):
        raise ValueError('recovery precondition receipt count differs from target')
    receipts = []
    for step, actual in zip(scenario.state_steps, actuals):
        if step.step_id != actual.get('step_id') or actual.get('requires') != [r.condition_id for r in step.requires]:
            raise ValueError('recovery precondition identity/requires mismatch')
        before, after = _step_evidence(events, actual)
        checks = {r.condition_id: r.evaluate(state) for r in step.requires}
        if not all(checks.values()):
            raise ValueError('recovery precondition dependencies not reached')
        old = dict(state)
        state[step.step_id] = True
        receipts.append(StepReceipt(step.step_id, step.action, old, dict(state), checks, 'executed',
                                    _iso(before['time_ns']), _iso(after['time_ns']),
                                    {'runtime_step_id': step.step_id, 'raw_before_index': before['event_index'],
                                     'raw_after_index': after['event_index'], 'runtime_receipt': actual,
                                     **({'namespace_ready_evidence': readiness} if step.step_id == 'create_endpoints' else {})}).to_dict())
    snapshot = _one(events, 'discovery_precondition_snapshot')
    begin = _one(events, 'mutation_begin' if scenario.arm == 'mutated' else 'control_begin')
    if (snapshot.get('satisfied') is not True or snapshot.get('precondition_receipts') != actuals
            or _resolve(events, begin.get('precondition_snapshot_ref')) != snapshot
            or not after['event_index'] < snapshot['event_index'] < begin['event_index']
            or not after['monotonic_ns'] <= snapshot['monotonic_ns'] <= begin['monotonic_ns']):
        raise ValueError('discovery mutation began without completed pre-drop receipts')
    return receipts


def _artifact(runtime, path):
    root = Path(runtime['raw_path']).resolve().parent
    path = Path(path).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError('discovery artifact missing or escapes runtime directory')
    return path


def _capture(events, runtime, config, isolation):
    ready = _one(events, 'discovery_observer_ready')['receipt']
    stopped = _one(events, 'discovery_observer_stopped')['receipt']
    spec = stopped['spec']
    if (ready != stopped.get('ready') or spec.get('run_id') != config['run_id']
            or spec.get('domain_id') != config['domain_id'] or spec.get('interface') != 'lo'
            or spec.get('participant_created') is not False or spec.get('packet_transmission') is not False
            or ready.get('spec_sha256') != spec.get('spec_sha256')
            or hashlib.sha256(json.dumps({k: v for k, v in spec.items() if k != 'spec_sha256'}, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest() != spec.get('spec_sha256')
            or stopped.get('thread_stopped') is not True or stopped.get('socket_closed') is not True
            or stopped.get('error') or stopped.get('health', {}).get('status') != 'ok' or ready.get('timestamp_clock') != 'kernel_SO_TIMESTAMPNS_realtime'):
        raise ValueError('independent discovery capture lifecycle/spec invalid')
    namespace = spec['netns_identity']
    if any(namespace.get(k) != isolation.get(k) for k in ('net_namespace', 'user_namespace', 'host_net_namespace', 'host_user_namespace')):
        raise ValueError('discovery capture namespace mismatch')
    raw, index = stopped['raw'], stopped['index']
    path = _artifact(runtime, raw['path'])
    verify_raw(path, raw['sha256'])
    data = path.read_bytes()
    if len(data) != raw.get('size') or data[:4] != b'\x4d\x3c\xb2\xa1':
        raise ValueError('discovery pcap identity/header invalid')
    index_path = _artifact(runtime, index['path'])
    verify_raw(index_path, index['sha256'])
    entries = [json.loads(line) for line in index_path.read_text().splitlines() if line.strip()]
    by_packet = {}
    offset = 24
    for number, entry in enumerate(entries):
        reference = entry['raw_ref']
        sec, ns, length, original = struct.unpack_from('<IIII', data, offset)
        if (entry.get('packet_index') != number or reference.get('packet_index') != number
                or reference.get('record_offset') != offset or reference.get('frame_offset') != offset + 16
                or reference.get('captured_length') != length or original != length
                or entry.get('unix_ns') != sec * 10**9 + ns or entry.get('truncated') is not False
                or entry.get('clock') != 'kernel_timestampns' or _artifact(runtime, reference['path']) != path):
            raise ValueError('discovery packet/index chronology invalid')
        frame = data[offset + 16:offset + 16 + length]
        if len(frame) != length or hashlib.sha256(frame).hexdigest() != reference.get('frame_sha256'):
            raise ValueError('discovery frame hash/length mismatch')
        from r2f.instrumentation.rtps import discovery_activity
        decoded, diagnostics = discovery_activity(frame)
        if decoded != entry.get('activity') or diagnostics != entry.get('diagnostics'):
            raise ValueError('independent discovery packet decode mismatch')
        by_packet[number] = (entry, decoded)
        offset += 16 + length
    if offset != len(data):
        raise ValueError('discovery pcap packet index incomplete')
    return {'ready': ready, 'stopped': stopped, 'data': data, 'packets': by_packet}


def _activity(event, capture, tokens, since, until, ports, bindings, *, after_ns=None):
    summary = event['activity']
    prefixes = {t['endpoint_gid'][:24].lower() for t in tokens.values()}
    if (summary.get('status') != 'observed' or summary.get('since_ns') != since
            or summary.get('clock') != 'unix_ns_realtime' or summary.get('participant_prefixes') != sorted(prefixes)
            or summary.get('ready') != capture['ready'] or summary.get('window_buffer_retained') is not True
            or summary.get('health', {}).get('status') != 'ok'
            or not since <= summary.get('until_ns', -1) <= until):
        raise ValueError('post-boundary attributed discovery activity unavailable')
    prefix = summary['raw_prefix']
    size = integer(prefix['size'], 'capture prefix size')
    if (size > len(capture['data']) or prefix.get('path') != capture['stopped']['raw']['path']
            or hashlib.sha256(capture['data'][:size]).hexdigest() != prefix.get('sha256')):
        raise ValueError('discovery activity raw prefix mismatch')
    observed = summary.get('events')
    if not isinstance(observed, list) or not observed:
        raise ValueError('discovery activity summary has no independent packet events')
    selected, participants, received_stamps = [], set(), []
    source_roles = {t['endpoint_gid'][:24].lower(): role for role, t in tokens.items()}
    for witness in observed:
        entry, decoded = capture['packets'][witness['raw_ref']['packet_index']]
        body = {k: v for k, v in witness.items() if k not in ('unix_ns', 'clock', 'raw_ref', 'received_monotonic_ns')}
        if (witness['raw_ref'] != entry['raw_ref'] or body not in decoded
                or witness.get('unix_ns') != entry['unix_ns'] or witness.get('clock') != entry['clock']
                or not since <= entry['unix_ns'] <= summary['until_ns']
                or body.get('source_guid_prefix') not in prefixes
                or body.get('destination_port') not in ports
                or not any(b['port'] == body.get('source_port') and source_roles.get(body.get('source_guid_prefix')) in b['roles'] for b in bindings)
                or witness.get('clock') != 'kernel_timestampns'
                or witness.get('received_monotonic_ns') != entry.get('received_monotonic_ns')
                or type(entry.get('received_monotonic_ns')) is not int
                or after_ns is not None and entry['received_monotonic_ns'] <= after_ns
                or witness['raw_ref']['frame_offset'] + witness['raw_ref']['captured_length'] > size):
            raise ValueError('discovery activity not bound to pair packet and time window')
        selected.append(entry['unix_ns'])
        received_stamps.append(entry['received_monotonic_ns'])
        participants.add(body['source_guid_prefix'])
    if participants != prefixes:
        raise ValueError('discovery activity does not cover both matched participants')
    return {'unix_ns': min(selected), 'received_monotonic_ns': min(received_stamps),
            'participant_prefixes': sorted(participants)}


def _graph(events, proof, tokens, *, after_ns):
    result = {}
    for role, evidence in proof.items():
        snapshot = events[evidence['snapshot_index']]
        graph = _one(events, 'graph_snapshot', role=role, command_id=snapshot['command_id'])
        ack = events[evidence['ack_index']]
        if (not snapshot['event_index'] < graph['event_index'] < ack['event_index']
                or not after_ns <= graph['monotonic_ns'] <= ack['monotonic_ns']
                or {x['endpoint_gid'] for x in graph['publishers']} != {tokens['writer']['endpoint_gid']}
                or {x['endpoint_gid'] for x in graph['subscriptions']} != {tokens['reader']['endpoint_gid']}
                or any(graph.get(k) != tokens[role].get(k) for k in ('pid', 'endpoint_generation', 'rmw_identifier'))):
            raise ValueError('fresh recovery graph/count identity mismatch')
        result[role] = graph['event_index']
    return result


def recovery_fault_receipt(events, config):
    opening = _one(events, 'window_open', window='post_restore_delivery')
    closing = _one(events, 'window_close', window='post_restore_delivery')
    raw = _one(events, 'fault_control_receipt')
    contract = _one(events, 'delivery_contract')
    expected = contract['expected_sequences']
    if (config.get('arm') != 'negative' or config.get('control_kind') != 'fault'
            or not isinstance(expected, list) or len(expected) < 3
            or any(type(seq) is not int for seq in expected)
            or expected != sorted(set(expected)) or contract.get('sentinel_sequence') != expected[-1]
            or contract.get('phase') != 'post_restore' or contract.get('fault_id') != config.get('fault_id')):
        raise ValueError('postrestore fault control declaration/sequence contract invalid')
    omitted = expected[len(expected) // 2]
    omission = _one(events, 'harness_omission', send_sequence=omitted, phase='post_restore')
    if (config.get('fault_id') != 'post_restore_delivery_omission' or raw.get('fault_id') != config['fault_id']
            or raw.get('omitted_sequence') != omitted or raw.get('applied') is not True
            or raw.get('responsibility_layer') != 'harness' or raw.get('candidate_eligible') is not False
            or omission.get('fault_id') != config['fault_id'] or omission.get('responsibility_layer') != 'harness'
            or omission.get('candidate_eligible') is not False
            or _resolve(events, raw.get('omission_event_ref')) != omission
            or not opening['event_index'] < omission['event_index'] < closing['event_index'] < raw['event_index']
            or not opening['monotonic_ns'] <= omission['monotonic_ns'] <= closing['monotonic_ns']
            or any(e.get('event') in ('send', 'receive') and e.get('phase') == 'post_restore'
                   and e.get('send_sequence') == omitted for e in events)
            or not any(e.get('event') == 'receive' and e.get('phase') == 'post_restore'
                       and e.get('send_sequence') == expected[-1] and e.get('role') == 'reader'
                       and omission['event_index'] < e['event_index'] < closing['event_index']
                       and omission['monotonic_ns'] <= e['monotonic_ns'] <= closing['monotonic_ns'] for e in events)):
        raise ValueError('postrestore harness omission is not independently closed/bound')
    receipt = {k: raw[k] for k in ('fault_id', 'applied', 'omitted_sequence', 'responsibility_layer', 'candidate_eligible')}
    receipt.update(schema='r2f.fault-control-receipt/v1', receipt_kind='delivery_omission',
                   window='post_restore_delivery', raw_event_ref=raw['event_index'],
                   window_refs={'open_event_ref': opening['event_index'], 'close_event_ref': closing['event_index']})
    for key in ('schema', 'receipt_kind', 'window', 'window_refs'):
        if key in raw and raw[key] != receipt[key]:
            raise ValueError('raw postrestore fault schema differs from mapped core receipt')
    return receipt


def _command(record, argv, isolation):
    if (record.get('argv') != argv or record.get('returncode') != 0
            or record.get('net_namespace') != isolation['net_namespace']
            or not record['started_monotonic_ns'] <= record['ended_monotonic_ns']):
        raise ValueError('discovery carrier command identity/result invalid')


def _counter(receipt, table, ports, isolation):
    from r2f.runtime.discovery_network import parse_counter
    _command(receipt['command'], ['nft', '-j', 'list', 'table', 'inet', table], isolation)
    payload = receipt['nft_json']
    if json.loads(receipt['command']['stdout']) != payload:
        raise ValueError('discovery counter JSON differs from command output')
    counts = parse_counter(payload, table)
    if (receipt.get('table') != table or receipt.get('counter_observed') is not True
            or receipt.get('packets') != counts['packets'] or receipt.get('bytes') != counts['bytes']
            or receipt.get('hit') is not (counts['packets'] > 0)):
        raise ValueError('discovery counter summary differs from nft evidence')
    rules = [e['rule'] for e in payload['nftables'] if 'rule' in e and e['rule'].get('table') == table]
    if len(rules) != 1:
        raise ValueError('discovery drop rule missing or additional rules present')
    expressions = rules[0]['expr']
    matches = [e['match'] for e in expressions if 'match' in e]
    if (not any(m.get('left') == {'meta': {'key': 'iifname'}} and m.get('right') == 'lo' for m in matches)
            or not any(m.get('left') == {'payload': {'protocol': 'udp', 'field': 'dport'}}
                       and m.get('op') == '==' and isinstance(m.get('right'), dict)
                       and set(m['right'].get('set', [])) == ports for m in matches)
            or not any('drop' in e for e in expressions)):
        raise ValueError('discovery drop rule is not restricted to loopback discovery UDP ports')
    return counts['packets']


def recovery_facts(events, runtime, config):
    facts = {'schema_version': 1, 'environment_healthy': runtime.get('environment', {}).get('ok') is True,
             'process_healthy': False, 'preconditions_reached': False, 'endpoint_identity_complete': False,
             'observed_effects': [], 'sent': [], 'received': []}
    try:
        metadata = runtime.get('execution_metadata') or {}
        workers = metadata.get('workers', [])
        pair_workers = [w for w in workers if w.get('role') in ('writer', 'reader')]
        probe_workers = [w for w in workers if w.get('role') == 'probe']
        if (len(pair_workers) != 2 or len({w['pid'] for w in pair_workers}) != 2
                or len(probe_workers) > 1
                or any(w.get('returncode') != 0 for w in workers)
                or _one(events, 'namespace_controller_exited').get('returncode') != 0
                or runtime.get('carrier_implementation') != IMPLEMENTATION
                or any(e['event'] in ('worker_error', 'worker_unexpected_exit', 'runtime_abort', 'owned_worker_cleanup_signal') for e in events)):
            raise ValueError('discovery pair/controller lifecycle incomplete')
        if probe_workers and any(w.get('pid') in {p['pid'] for p in pair_workers} for w in probe_workers):
            raise ValueError('discovery probe must be an independent process')
        facts['process_healthy'] = True
        admission = _one(events, 'namespace_host_admission_verified')
        isolation = _isolation(admission['isolation'])
        setup_event = _one(events, 'discovery_network_setup')
        setup = setup_event['receipt']
        if setup.get('health_ok') is not True or setup.get('drop_installed') is not False:
            raise ValueError('discovery namespace initial state not healthy/neutral')
        if any('table' in e for e in setup['initial_nft']['nftables']):
            raise ValueError('discovery namespace had preexisting nft rules')
        tokens = {r: _one(events, 'endpoint_created', role=r) for r in ('writer', 'reader')}
        if len({t['pid'] for t in tokens.values()}) != 2 or len({t['endpoint_gid'] for t in tokens.values()}) != 2:
            raise ValueError('independent endpoint process/GID identity missing')
        for role, token in tokens.items():
            if (token.get('rmw_identifier') != config['rmw'] or token.get('topic') != config['topic']
                    or token.get('qos') != config['qos'] or token.get('message_type') != config['message_type']
                    or not token.get('endpoint_generation')
                    or not any(w.get('role') == role and w.get('pid') == token['pid'] for w in workers)):
                raise ValueError('discovery endpoint submitted identity/config mismatch')
            facts[role] = {'id': token['endpoint_gid'], 'generation': token['endpoint_generation'], 'created_ns': token['created_monotonic_ns']}
        # Verify submitted prejoin publication and reader delay as execution,
        # although neither prejoin sample is a postrestore delivery obligation.
        prejoin = [e for e in events if e['event'] == 'send' and e.get('phase') == 'prejoin']
        prejoin_done = _one(events, 'prejoin_publish_complete')
        delay_begin = _one(events, 'reader_creation_delay_begin')
        delay_end = _one(events, 'reader_creation_delay_end')
        if ([e.get('send_sequence') for e in prejoin] != list(range(config['pre_match_count']))
                or prejoin_done.get('count') != config['pre_match_count']
                or prejoin_done.get('publish_refs') != [_ref(e) for e in prejoin]
                or prejoin_done.get('post_restore_delivery_obligation') is not False
                or any(e.get('writer_gid') != tokens['writer']['endpoint_gid'] or e.get('publish_returned') is not True
                       or e.get('pid') != tokens['writer']['pid'] for e in prejoin)
                or delay_begin.get('configured_delay_sec') != config['reader_delay_sec']
                or delay_end.get('configured_delay_sec') != config['reader_delay_sec']
                or _resolve(events, delay_end.get('delay_begin_ref')) != delay_begin
                or not prejoin_done['event_index'] < delay_begin['event_index'] < delay_end['event_index'] < tokens['reader']['event_index']
                or delay_end['monotonic_ns'] - delay_begin['monotonic_ns'] < config['reader_delay_sec'] * 10**9):
            raise ValueError('submitted prejoin publications/reader delay lack actual raw proof')
        snapshot = _one(events, 'discovery_precondition_snapshot')
        ports = _bindings(snapshot['bindings'], tokens, isolation, config['domain_id'])
        if snapshot.get('worker_pids') != {r: t['pid'] for r, t in tokens.items()}:
            raise ValueError('pre-drop endpoint worker correlation mismatch')
        begin = _one(events, 'mutation_begin' if config['arm'] == 'mutated' else 'control_begin')
        matched_ns, initial_match = fresh_match_proof(events, tokens, begin, anchor_ns=tokens['reader']['created_monotonic_ns'])
        initial_graph = _graph(events, initial_match, tokens, after_ns=tokens['reader']['created_monotonic_ns'])
        if not matched_ns < snapshot['monotonic_ns'] < begin['monotonic_ns']:
            raise ValueError('match/binding evidence was not established before mutation')
        for role in tokens:
            if (_resolve(events, snapshot['matched']['matches'][role])['event_index'] != initial_match[role]['snapshot_index']
                    or _resolve(events, snapshot['matched']['graphs'][role])['event_index'] != initial_graph[role]
                    or snapshot.get(role + '_gid') != tokens[role]['endpoint_gid']):
                raise ValueError('pre-drop match/graph references differ from raw')
        # The profile files are execution artifacts. Their bytes must match the
        # frozen, arm-independent loopback profile implementation.
        from r2f.runtime.discovery_profiles import CYCLONE_XML, FAST_XML
        profiles = runtime['environment']['discovery_namespace']['profiles']
        profile_hashes = {}
        for number, role in enumerate(('writer', 'reader')):
            record = profiles[role]
            path = _artifact(runtime, record['path'])
            verify_raw(path, record['sha256'])
            expected_profile = CYCLONE_XML if config['rmw'] == 'rmw_cyclonedds_cpp' else FAST_XML.format(role=role, index=number)
            if path.read_bytes() != expected_profile.encode():
                raise ValueError('actual discovery transport profile differs from reviewed semantic carrier')
            profile_hashes[role] = record['sha256']
        facts['transport_profile_sha256'] = profile_hashes
        boundary = _one(events, 'discovery_restore_boundary')
        opening = _one(events, 'window_open', window='post_restore_delivery')
        closing = _one(events, 'window_close', window='post_restore_delivery')
        inter_open = _one(events, 'window_open', window='discovery_interruption')
        inter_close = _one(events, 'window_close', window='discovery_interruption')
        boundary_mono = integer(boundary['post_restore_monotonic_ns'], 'restore boundary')
        boundary_wall = integer(boundary['post_restore_unix_ns'], 'restore realtime boundary')
        if (not begin['event_index'] < inter_open['event_index'] < inter_close['event_index'] < boundary['event_index'] < opening['event_index'] < closing['event_index']
                or not inter_close['monotonic_ns'] <= boundary_mono <= boundary['monotonic_ns'] <= opening['monotonic_ns']
                or inter_close['monotonic_ns'] - inter_open['monotonic_ns'] < config['drop_duration_sec'] * 10**9):
            raise ValueError('discovery drop/restore/delivery window chronology invalid')
        active = config['arm'] == 'mutated'
        packets = 0
        capture = _capture(events, runtime, config, isolation)
        activity_events = [e for e in events if e['event'] == 'discovery_activity_observation'
                           and str(e.get('window', '')).startswith('post_restore')
                           and e['activity'].get('status') == 'observed'
                           and e['event_index'] < opening['event_index']]
        if not activity_events:
            raise ValueError('independent post-restore discovery observation unknown')
        observed = activity_events[-1]
        guard = integer(boundary['clock_guard_ns'], 'restore clock guard')
        since = integer(boundary['activity_since_unix_ns'], 'recovery activity since')
        if guard != 50_000_000 or since != boundary_wall + guard:
            raise ValueError('recovery activity clock guard differs from runtime contract')
        if any(e['event'] == 'discovery_observation_unknown' and e.get('reason') == 'realtime_monotonic_offset_discontinuity' for e in events):
            raise ValueError('recovery clocks changed offset')
        activity_proof = _activity(observed, capture, tokens, since, opening['time_ns'], ports,
                                   snapshot['bindings']['bindings'], after_ns=boundary_mono)
        activity_wall = activity_proof['unix_ns']
        if activity_wall < boundary_wall or observed.get('after_monotonic_ns') != boundary_mono or observed.get('clock_guard_ns') != guard:
            raise ValueError('recovery activity is not strictly after restoration boundary:'
                             f'activity_wall={activity_wall},boundary_wall={boundary_wall},'
                             f'after={observed.get("after_monotonic_ns")},boundary_mono={boundary_mono},'
                             f'guard={observed.get("clock_guard_ns")}/{guard}')
        # Preserve the realtime->monotonic boundary and its explicit uncertainty.
        if abs((observed['time_ns'] - observed['monotonic_ns']) - (boundary_wall - boundary_mono)) > 50_000_000:
            raise ValueError('recovery clocks changed offset')
        if active:
            dropped = _one(events, 'discovery_drop_begin')
            restored = _one(events, 'discovery_drop_end')
            dr, rr = dropped['receipt'], restored['receipt']
            table = dr['table']
            if (dr.get('scope') != 'ros2_pair' or dr.get('ros2_execution') is not True
                    or dr.get('configured') is not True or dr.get('applied') is not True
                    or rr.get('restored') is not True or rr.get('table_absent') is not True
                    or rr.get('table') != table or boundary.get('restored') is not True
                    or boundary.get('neutral') is not False
                    or not begin['event_index'] < dropped['event_index'] < inter_open['event_index']
                    or not inter_open['event_index'] < restored['event_index'] < boundary['event_index']
                    or rr['drop_end_monotonic_ns'] > boundary_mono):
                raise ValueError('real pair discovery drop/verified restore evidence incomplete')
            _bindings(dr['pair_bindings'], tokens, isolation, config['domain_id'])
            if dr['precondition'].get('evidence_ref') != _ref(snapshot) or dr['precondition'].get('match_established') is not True:
                raise ValueError('carrier start not bound to matched pre-drop pair')
            _command(dr['install_command'], ['nft', '-f', '-'], isolation)
            _command(rr['delete_command'], ['nft', 'delete', 'table', 'inet', table], isolation)
            _command(rr['verify_command'], ['nft', '-j', 'list', 'ruleset'], isolation)
            if (json.loads(rr['verify_command']['stdout']) != rr['final_nft']
                    or any(e.get('table', {}).get('name') == table for e in rr['final_nft']['nftables'])):
                raise ValueError('discovery restored table still present in raw nft output')
            counters = [e['receipt'] for e in events if e['event'] == 'discovery_drop_counter'
                        and inter_open['event_index'] < e['event_index'] < restored['event_index']]
            counters.append(rr['counter_snapshot'])
            counts = [_counter(c, table, ports, isolation) for c in counters]
            if counts != sorted(counts) or not counts or max(counts) <= 0:
                raise ValueError('discovery drop has no monotonically counted real hit')
            packets = max(counts)
            during = _one(events, 'discovery_activity_observation', window='discovery_interruption')
            _activity(during, capture, tokens, during['activity']['since_ns'], restored['time_ns'], ports, snapshot['bindings']['bindings'])
            confirmation = _one(events, 'discovery_recovery_confirmed')['receipt']
            if (confirmation.get('recovery_verified') is not True
                    or confirmation.get('restore_end_monotonic_ns') != rr['drop_end_monotonic_ns']
                    or _resolve(events, confirmation['activity_receipt'].get('evidence_ref')) != observed):
                raise ValueError('discovery recovery confirmation lacks independent activity binding')
            after_bindings = confirmation['pair_bindings']
            _bindings(after_bindings, tokens, isolation, config['domain_id'])
            if after_bindings['worker_start_ticks'] != snapshot['bindings']['worker_start_ticks']:
                raise ValueError('endpoint process generation changed during discovery drop')
        else:
            if (begin.get('drop_installed') is not False or boundary.get('neutral') is not True
                    or boundary.get('drop_installed') is not False
                    or any(e['event'] in ('discovery_drop_begin', 'discovery_drop_counter', 'discovery_drop_end') for e in events)):
                raise ValueError('neutral discovery arm installed or claimed drop')
        match_ns, proof = fresh_match_proof(events, tokens, opening, anchor_ns=boundary_mono)
        graphs = _graph(events, proof, tokens, after_ns=boundary_mono)
        facts['match_snapshot_proof'] = proof
        facts['match'] = {'writer_id': tokens['writer']['endpoint_gid'], 'reader_id': tokens['reader']['endpoint_gid'], 'at_ns': match_ns}
        facts['window'] = {'start_ns': opening['monotonic_ns'], 'end_ns': closing['monotonic_ns'], 'complete': True}
        expected = list(range(config['pre_match_count'], config['pre_match_count'] + config['post_match_count']))
        contract = _one(events, 'delivery_contract')
        if (contract.get('expected_sequences') != expected or opening.get('expected_sequences') != expected
                or contract.get('fault_id') != config.get('fault_id')
                or contract.get('sentinel_sequence') != expected[-1]
                or contract.get('phase') != 'post_restore' or contract.get('qos') != config['qos']
                or contract.get('prejoin_in_delivery_contract') is not False):
            raise ValueError('postrestore delivery contract differs from submitted config')
        sends = [e for e in events if e['event'] in ('send', 'harness_omission') and e.get('phase') == 'post_restore']
        if ([e.get('send_sequence') for e in sends] != expected or any(
                not opening['event_index'] < e['event_index'] < closing['event_index']
                or not opening['monotonic_ns'] <= e['monotonic_ns'] <= closing['monotonic_ns']
                or e.get('role') != 'writer' or e.get('pid') != tokens['writer']['pid']
                or e['event'] == 'send' and e.get('writer_gid') != tokens['writer']['endpoint_gid']
                or e.get('endpoint_generation') != tokens['writer']['endpoint_generation']
                for e in sends)):
            raise ValueError('postrestore publish/omission obligations lack complete raw identity/window proof')
        received = [e for e in events if e['event'] == 'receive' and e.get('phase') == 'post_restore']
        if any(not opening['event_index'] < e['event_index'] < closing['event_index']
               or not opening['monotonic_ns'] <= e['monotonic_ns'] <= closing['monotonic_ns']
               or e.get('role') != 'reader' or e.get('pid') != tokens['reader']['pid'] for e in received):
            raise ValueError('postrestore receive process/window proof incomplete')
        omissions = [e for e in events if e['event'] == 'harness_omission']
        if config['arm'] == 'negative':
            fault = recovery_fault_receipt(events, config)
            if len(omissions) != 1:
                raise ValueError('negative requires exactly one harness omission')
            facts['controlled_omission'] = {'sequence': fault['omitted_sequence'], 'responsibility_layer': 'harness', 'candidate_eligible': False}
        elif omissions or any(e['event'] == 'fault_control_receipt' for e in events):
            raise ValueError('unplanned postrestore harness fault')
        if not received:
            raise ValueError('no bounded delivery identity witness')
        direct = all(e.get('publisher_gid') is not None for e in received)
        for role in tokens:
            after = [e for e in events if e['event'] == 'graph_snapshot' and e.get('role') == role and e['monotonic_ns'] >= closing['monotonic_ns']]
            if not after or {x['endpoint_gid'] for x in after[0]['publishers']} != {tokens['writer']['endpoint_gid']} or {x['endpoint_gid'] for x in after[0]['subscriptions']} != {tokens['reader']['endpoint_gid']}:
                raise ValueError('postrestore graph identity has no closing boundary')
            graphs[role] = {'before_index': graphs[role], 'after_index': after[0]['event_index']}
        for event in events:
            if event['event'] in ('send', 'harness_omission') and event.get('phase') == 'post_restore':
                if event['event'] == 'send' and event.get('publish_returned') is not True:
                    raise ValueError('postrestore public publish receipt missing')
                facts['sent'].append({'sequence': event['send_sequence'], 'at_ns': event['monotonic_ns'],
                                      'writer_id': event.get('writer_gid') or tokens['writer']['endpoint_gid'],
                                      'generation': tokens['writer']['endpoint_generation']})
        for event in received:
            if (event.get('payload_run_id') != config['run_id'] or event.get('reader_gid') != tokens['reader']['endpoint_gid']
                    or event.get('endpoint_generation') != tokens['reader']['endpoint_generation']
                    or event.get('publisher_gid') is None and (event.get('publisher_gid_status') != 'unavailable_api' or event.get('identity_basis') != 'endpoint_correlated')):
                raise ValueError('postrestore delivery endpoint identity proof incomplete')
            facts['received'].append({'sequence': event['send_sequence'], 'at_ns': event['monotonic_ns'],
                                      'writer_id': event.get('publisher_gid') or tokens['writer']['endpoint_gid'],
                                      'writer_generation': tokens['writer']['endpoint_generation'],
                                      'reader_id': event['reader_gid'], 'reader_generation': event['endpoint_generation']})
        observations = runtime.get('observation_receipts', [])
        if [o.get('step_id') for o in observations] != ['observe_recovery_graph', 'observe_final_graph']:
            raise ValueError('postrestore observation receipt set incomplete')
        for observed_step in observations:
            _step_evidence(events, observed_step)
        cleanup = _one(events, 'discovery_cleanup_restore')['receipt']
        if cleanup.get('restored') is not True:
            raise ValueError('discovery cleanup restore not verified')
        facts.update(endpoint_identity_complete=True, preconditions_reached=len(runtime.get('precondition_receipts', [])) == 3,
                     identity_proof={'basis': 'message_info_gid' if direct else 'endpoint_correlated',
                                     'direct_gid_match': all(e.get('publisher_gid') == tokens['writer']['endpoint_gid'] for e in received) if direct else None,
                                     'publisher_gid_status': 'available' if direct else 'unavailable_api', 'graph_boundary_receipts': graphs},
                     drop={'active': active, 'configured': active, 'applied': active, 'hit': active and packets > 0,
                           'pair_bound': True, 'counter_observed': active, 'packets': packets},
                     recovery={'boundary_ns': boundary_mono, 'activity_ns': activity_proof['received_monotonic_ns'],
                               'activity_identity_correlated': True, 'fresh_match_and_graph': True, 'rules_restored': True,
                               'identity_stable': True,
                               'match_continuity': 'reestablished' if any(e['event'] == 'match_snapshot' and e.get('matched_count') == 0
                                   and matched_ns < e['monotonic_ns'] < match_ns for e in events) else 'retained',
                               'match_continuity_scope': 'observed_public_count_samples',
                               'clock_translation': 'kernel_realtime_plus_independent_receive_monotonic',
                               'activity_unix_ns': activity_wall, 'clock_guard_ns': guard,
                               'activity_event_ref': _ref(observed)},
                     observed_effects=['discovery_temporarily_unavailable', 'discovery_restored'] if active else ['discovery_restored'])
    except (KeyError, ValueError, TypeError, IndexError, OSError, struct.error) as exc:
        facts['evidence_error'] = str(exc)
        facts['endpoint_identity_complete'] = False
        facts['window'] = {'complete': False}
    return facts
