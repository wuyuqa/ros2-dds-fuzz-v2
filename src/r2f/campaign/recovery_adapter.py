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
from r2f.runtime.discovery_network import DiscoveryNetworkError
from r2f.core.state_machine import StepReceipt
from r2f.modules.s1.facts import integer
from .runtime_adapter import _one, _iso, fresh_match_proof
from .participant_lease import attach_participant_lease

from r2f.runtime.ros2_pair import role_rmw, F2_WINDOWS_TARGET, f2_windows_operations

IMPLEMENTATION = 'isolated_netns_nft_discovery_udp_drop'
NETEM_IMPLEMENTATION = 'isolated_netns_tc_netem_discovery_udp'
#: Each carrier has its own reviewed setup/restore receipt schema.  Selecting
#: by implementation keeps the drop contract byte-identical and never lets one
#: carrier's receipt satisfy the other's checks.
CARRIER_IMPLEMENTATIONS = (IMPLEMENTATION, NETEM_IMPLEMENTATION)

#: Reader generations reader2..reader9 are additional discovery stimuli.
MAX_RECREATED_READERS = 8


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


def _namespace_ready(events, create_before, create_started_ns, carrier_implementation=IMPLEMENTATION):
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
    # Each carrier reports its own neutral precondition: the drop carrier uses
    # nft fields, the netem carrier uses qdisc/filter fields.  A neutral carrier
    # must never report a hit under either schema.
    if carrier_implementation == NETEM_IMPLEMENTATION:
        neutral = (setup.get('mutation_installed') is False
                   and setup.get('initial_filter') in ([], None))
    else:
        neutral = setup.get('drop_installed') is False
    if (any(setup_isolation[k] != isolation[k] for k in ('net_namespace', 'user_namespace', 'host_net_namespace', 'host_user_namespace', 'uid_map'))
            or setup.get('health_ok') is not True or not neutral
            or setup.get('hit') is not False
            or (setup.get('counter_observed') is not False
                and carrier_implementation != NETEM_IMPLEMENTATION)
            or not admission['monotonic_ns'] <= setup['monotonic_ns'] <= setup_event['monotonic_ns']):
        raise ValueError('namespace readiness setup is not healthy/neutral in admitted namespace')
    if carrier_implementation == NETEM_IMPLEMENTATION:
        # netem health is proven by the qdisc/filter dump, not by an nft listing.
        if (setup.get('initial_qdisc') is None or setup.get('initial_filter') is None
                or not admission['monotonic_ns'] <= setup['monotonic_ns']
                or setup.get('implementation') != NETEM_IMPLEMENTATION):
            raise ValueError('namespace readiness netem carrier evidence invalid')
    else:
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
    readiness = _namespace_ready(events, create_before, create_before['before']['monotonic_ns'],
                                 runtime.get('carrier_implementation', IMPLEMENTATION))
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


def _multi_window_chain(events, config):
    """Whether raw/config describe the explicit window-chain path.

    The legacy single-window runtime never emits ``discovery_window_begin``.
    Its facts must stay byte-identical, so only the explicit chain (including
    a one-window explicit plan) enters the per-window collector.
    """
    if config.get('target_id') == F2_WINDOWS_TARGET:
        return True
    if any(e.get('event') == 'discovery_window_begin' for e in events):
        return True
    windows = config.get('environment_windows')
    return isinstance(windows, list) and len(windows) > 1


def _window_one(events, name, index, **fields):
    found = [e for e in events if e.get('event') == name
             and e.get('window_index') == index
             and all(e.get(k) == v for k, v in fields.items())]
    if len(found) != 1:
        raise ValueError(f'window[{index}]: {name} evidence absent or ambiguous')
    return found[0]


def _f2_window_timing(events, index, plan, tokens, isolation, config, expected_anchor):
    """Independently recompute eligibility from raw, not runtime/config echoes."""
    anchor = _resolve(events, expected_anchor)
    begin = _window_one(events, 'discovery_window_begin', index)
    drop = _window_one(events, 'discovery_drop_begin', index)
    timing = _window_one(events, 'discovery_window_timing', index)
    if (anchor.get('event') != ('discovery_precondition_snapshot' if index == 0 else 'discovery_recovery_confirmed')
            or anchor.get('role') != 'controller'
            or any(anchor.get(k) != config[k] for k in ('run_id', 'domain_id'))
            or not anchor['event_index'] < begin['event_index'] < drop['event_index'] < timing['event_index']
            or not anchor['monotonic_ns'] <= begin['monotonic_ns'] <= drop['monotonic_ns'] <= timing['monotonic_ns']
            or timing.get('window_id') != plan['window_id']
            or timing.get('anchor_ref') != expected_anchor or timing.get('trigger_anchor') != plan['trigger_anchor']):
        raise ValueError('f2_anchor_identity_or_chronology_invalid')
    snapshot = _one(events, 'discovery_precondition_snapshot')
    if index == 0:
        if anchor.get('satisfied') is not True:
            raise ValueError('f2_anchor_precondition_not_satisfied')
        matched = anchor['matched']
        proof_anchor = tokens['reader']['created_monotonic_ns']
    else:
        prior_plan = config['environment_windows'][index - 1]
        if (anchor.get('window_index') != index - 1 or anchor.get('window_id') != prior_plan['window_id']
                or anchor.get('receipt', {}).get('recovery_verified') is not True):
            raise ValueError('f2_anchor_previous_recovery_not_confirmed')
        matched = anchor['receipt']['activity_receipt']['fresh_graph_count']
        proof_anchor = _window_one(events, 'discovery_restore_boundary', index - 1)['post_restore_monotonic_ns']
    _, match_proof = fresh_match_proof(events, tokens, anchor, anchor_ns=proof_anchor)
    graphs = _graph(events, match_proof, tokens, after_ns=proof_anchor)
    proof_refs = []
    for role, proof in match_proof.items():
        if (matched['matches'][role] != _ref(events[proof['snapshot_index']])
                or matched['graphs'][role] != _ref(events[graphs[role]])):
            raise ValueError('f2_anchor_fresh_match_ref_mismatch')
        proof_refs.extend(_ref(events[j]) for j in (proof['request_index'], proof['snapshot_index'],
                                                   graphs[role], proof['ack_index']))
    if timing.get('matched_event_refs') != proof_refs:
        raise ValueError('f2_anchor_matched_event_refs_mismatch')
    dr = drop['receipt']
    _bindings(dr['pair_bindings'], tokens, isolation, config['domain_id'])
    if dr['pair_bindings'].get('worker_start_ticks') != snapshot['bindings'].get('worker_start_ticks'):
        raise ValueError('f2_anchor_process_generation_mismatch')
    anchor_ns = integer(anchor.get('monotonic_ns'), 'f2 anchor time')
    due_offset = anchor_ns + round(plan['trigger_offset_sec'] * 1e9)
    due_interval = (anchor_ns + round(config['environment_windows'][index - 1]['restore_interval_sec'] * 1e9)
                    if index else anchor_ns)
    due = max(due_offset, due_interval)
    start = integer(timing.get('actual_start_monotonic_ns'), 'f2 actual start')
    install = integer(dr.get('drop_applied_monotonic_ns'), 'f2 actual install')
    command = dr['install_command']
    if not (begin['monotonic_ns'] <= start <= dr['drop_begin_monotonic_ns']
            <= command['started_monotonic_ns'] <= command['ended_monotonic_ns']
            <= install <= drop['monotonic_ns'] <= timing['monotonic_ns']):
        raise ValueError('f2_install_command_chronology_invalid')
    calculated = {'anchor_monotonic_ns': anchor_ns, 'offset_due_monotonic_ns': due_offset,
                  'prior_interval_due_monotonic_ns': due_interval, 'earliest_due_monotonic_ns': due,
                  'actual_install_monotonic_ns': install, 'overshoot_ns': install - due}
    if any(timing.get(k) != v for k, v in calculated.items()):
        raise ValueError('f2_timing_receipt_mismatch')
    if (start < due or install < due or install - due > round(config['anchor_lateness_tolerance_sec'] * 1e9)
            or timing.get('status') != 'observed' or timing.get('reason') is not None):
        raise ValueError('harness_anchor_timing_miss')
    return {**calculated, 'actual_start_monotonic_ns': start,
            'actual_anchor_offset_sec': (install - anchor_ns) / 1e9,
            'actual_prior_restore_interval_sec': ((install - anchor_ns) / 1e9 if index else None),
            'anchor_ref': expected_anchor, 'matched_event_refs': proof_refs,
            'raw_event_ref': _ref(timing), 'complete': True}


def _f2_operation_facts(events, runtime, config, facts):
    from r2f.runtime.ros2_pair import f2_windows_operations
    operations = f2_windows_operations(config)
    if runtime.get('real_execution') is not True or runtime.get('runtime_status') != 'completed':
        raise ValueError('f2_operation_not_completed_execution')
    verify_raw(runtime['raw_path'], runtime['raw_sha256'])
    if ([json.loads(line) for line in Path(runtime['raw_path']).read_text().splitlines()] != events
            or any(e.get('event_index') != i for i, e in enumerate(events))):
        raise ValueError('f2_raw_index_or_content_mismatch')
    raw_ops = [e for e in events if e.get('event') == 'f2_operation_receipt']
    claimed = runtime.get('operation_receipts')
    if len(raw_ops) != len(operations) or not isinstance(claimed, list) or len(claimed) != len(operations):
        raise ValueError('f2_operation_receipts_missing_or_duplicate')
    result = []
    for operation, event, returned in zip(operations, raw_ops, claimed):
        if (event.get('role') != 'controller'
                or any(event.get(k) != config[k] for k in ('run_id', 'domain_id'))
                or any(e.get('event') in ('worker_stopping', 'worker_shutdown')
                       and e.get('role') in ('writer', 'reader')
                       for e in events[:event['event_index']])):
            raise ValueError('f2_operation_controller_identity_or_cleanup_chronology_invalid')
        receipt = event['receipt']
        for key in ('op_id', 'surface_id', 'mutator_id'):
            if receipt.get(key) != operation[key]:
                raise ValueError('f2_operation_identity_mismatch:' + key)
        if receipt.get('schema') != 'r2f.f2-window-operation/v1' or receipt.get('plan_hash') != config['mutation_plan_sha256']:
            raise ValueError('f2_operation_plan_hash_mismatch')
        observed = facts['windows']
        if receipt.get('window_count') != len(observed) or len(receipt.get('windows', [])) != len(observed):
            raise ValueError('f2_operation_window_count_mismatch')
        refs, all_hit = [], True
        transitions = []
        for index, (actual, declared) in enumerate(zip(observed, receipt['windows'])):
            plan = config['environment_windows'][index]
            names = ('discovery_window_timing', 'discovery_drop_begin', 'discovery_drop_end',
                     'discovery_recovery_confirmed', 'discovery_window_restore_verified')
            expected = {name: _ref(_window_one(events, name, index)) for name in names}
            counter = _resolve(events, actual['counter_event_ref'])
            hit = actual['complete'] and actual['drop']['hit']
            if (declared.get('window_id') != plan['window_id'] or declared.get('window_index') != index
                    or declared.get('raw_refs') != expected or declared.get('counter_ref') != _ref(counter)
                    or declared.get('complete') is not True or declared.get('hit') is not hit
                    or declared.get('packets_delta') != actual['drop']['packets_delta']):
                raise ValueError('f2_operation_window_receipt_mismatch')
            refs.extend(list(expected.values()) + [_ref(counter)])
            all_hit = all_hit and hit
            if index:
                transitions.append({'from_window_index': index - 1, 'to_window_index': index,
                                    'actual_interval_sec': actual['timing']['actual_prior_restore_interval_sec'],
                                    'required_interval_sec': config['environment_windows'][index - 1]['restore_interval_sec'],
                                    'anchor_ref': actual['timing']['anchor_ref'],
                                    'timing_ref': actual['timing']['raw_event_ref']})
        affected = operation['affected_window_indices']
        if operation['surface_id'] == 'E03':
            observed_parameters = {'anchor': operation['parameters']['anchor'],
                'requested_offset_sec': operation['parameters']['offset_sec'],
                'actual_offsets': [{'window_index': i, 'anchor_ref': observed[i]['timing']['anchor_ref'],
                                    'actual_offset_sec': observed[i]['timing']['actual_anchor_offset_sec']}
                                   for i in affected]}
        else:
            observed_parameters = {'actual_window_count': len(observed),
                'actual_restore_intervals_sec': [w['timing']['actual_prior_restore_interval_sec'] for w in observed[1:]],
                'trailing_interval_status': 'not_applicable_no_next_window'}
        if (receipt.get('affected_window_indices') != affected
                or receipt.get('observed_parameters') != observed_parameters):
            raise ValueError('f2_operation_affected_windows_or_observed_parameters_mismatch')
        closing = _one(events, 'window_close', window='post_restore_delivery')
        final = _one(events, 'state_step_after', step_id='observe_final_graph')
        if (final.get('satisfied') is not True or not closing['event_index'] < final['event_index'] < event['event_index']
                or not closing['monotonic_ns'] <= final['monotonic_ns'] <= event['monotonic_ns']):
            raise ValueError('f2_operation_closure_chronology_invalid')
        refs.extend((_ref(closing), _ref(final)))
        if (receipt.get('evidence_refs') != refs or receipt.get('status') != 'executed'
                or receipt.get('applied') is not True or receipt.get('hit') is not all_hit
                or receipt.get('effect_observed') is not all_hit
                or any(returned.get(k) != v for k, v in receipt.items())
                or returned.get('raw_event_ref') != _ref(event)):
            raise ValueError('f2_operation_result_or_hit_mismatch')
        audited_refs = [{'path': runtime['raw_path'], 'sha256': runtime['raw_sha256'],
                         'event_index': _resolve(events, ref)['event_index']}
                        for ref in refs + [_ref(event)]]
        if returned.get('raw_refs') != audited_refs:
            raise ValueError('f2_operation_raw_ref_mismatch')
        result.append({**deepcopy(receipt), 'verified': True, 'raw_refs': audited_refs,
                       'actual_window_count': len(observed), 'actual_restore_intervals': transitions,
                       'trailing_interval': {'status': 'not_applicable', 'reason': 'no_next_window'},
                       'raw_event_ref': _ref(event)})
    return result


def _window_fact(events, index, plan, tokens, isolation, config, previous_ref,
                 capture, ports, pair_bindings, guard):
    begin_event = _window_one(events, 'discovery_window_begin', index)
    if (begin_event.get('window_id') != plan['window_id']
            or begin_event.get('trigger_anchor') != plan['trigger_anchor']):
        raise ValueError('window declaration differs from the submitted plan')
    if index == 0:
        if plan['trigger_anchor'] != 'fresh_match':
            raise ValueError('first window is not anchored on fresh_match')
    elif plan['trigger_anchor'] != 'previous_restore_confirmed':
        raise ValueError('later window is not anchored on previous_restore_confirmed')
    snapshot = _one(events, 'discovery_precondition_snapshot')
    expected_anchor = snapshot['raw_event_ref'] if index == 0 else previous_ref
    if begin_event.get('anchor_ref') != expected_anchor:
        raise ValueError('window anchor does not cite the required prior evidence')

    dropped = _window_one(events, 'discovery_drop_begin', index)
    dr = dropped['receipt']
    if (dr.get('scope') != 'ros2_pair' or dr.get('ros2_execution') is not True
            or dr.get('configured') is not True or dr.get('applied') is not True):
        raise ValueError('window drop was not actually applied to this pair')
    _bindings(dr['pair_bindings'], tokens, isolation, config['domain_id'])
    if (config.get('target_id') == F2_WINDOWS_TARGET
            and dr['pair_bindings'].get('worker_start_ticks') != snapshot['bindings'].get('worker_start_ticks')):
        raise ValueError('f2_window_process_generation_mismatch')
    if dr.get('precondition', {}).get('evidence_ref') != expected_anchor:
        raise ValueError('window drop precondition is not bound to the required anchor')
    _command(dr['install_command'], ['nft', '-f', '-'], isolation)
    if 'counter_snapshot' not in dr:
        raise ValueError('window drop begin counter snapshot missing')

    inter_open = _window_one(events, 'window_open', index, window='discovery_interruption')
    inter_close = _window_one(events, 'window_close', index, window='discovery_interruption')
    restored = _window_one(events, 'discovery_drop_end', index)
    rr = restored['receipt']
    if (rr.get('restored') is not True or rr.get('table_absent') is not True
            or rr.get('table') != dr.get('table')):
        raise ValueError('window restore is not verified')
    if 'counter_snapshot' not in rr:
        raise ValueError('window drop end counter snapshot missing')
    _command(rr['delete_command'], ['nft', 'delete', 'table', 'inet', rr['table']], isolation)
    _command(rr['verify_command'], ['nft', '-j', 'list', 'ruleset'], isolation)
    if (json.loads(rr['verify_command']['stdout']) != rr['final_nft']
            or any(e.get('table', {}).get('name') == rr['table']
                   for e in rr['final_nft'].get('nftables', []))):
        raise ValueError('window restore left the drop table installed')

    from r2f.runtime.discovery_network import window_counter_delta
    delta = window_counter_delta(dr, rr)
    timing = None
    if config.get('target_id') == F2_WINDOWS_TARGET:
        _counter(dr['counter_snapshot'], dr['table'], ports, isolation)
        _counter(rr['counter_snapshot'], rr['table'], ports, isolation)
        timing = _f2_window_timing(events, index, plan, tokens, isolation, config, expected_anchor)
    # The runtime emits one delta immediately after this window's verified
    # restore.  The receipt itself carries no window_index, so bind it by the
    # window's own raw event interval instead of by global position.
    boundary_event = _window_one(events, 'discovery_restore_boundary', index)
    counter_events = [e for e in events if e.get('event') == 'discovery_window_counter'
                      and dropped['event_index'] < e['event_index'] < boundary_event['event_index']]
    if len(counter_events) != 1:
        raise ValueError('window counter delta receipt absent or duplicated')
    emitted = counter_events[0]
    for key in ('table', 'packets_before', 'packets_after', 'bytes_before',
                'bytes_after', 'packets_delta', 'bytes_delta',
                'counter_observed', 'hit', 'counter_semantics'):
        if emitted.get(key) != delta.get(key):
            raise ValueError('window emitted counter delta differs from raw begin/end')
    if emitted.get('window_index') not in (None, index):
        raise ValueError('window emitted counter index differs from its plan')

    boundary = _window_one(events, 'discovery_restore_boundary', index)
    boundary_mono = integer(boundary.get('post_restore_monotonic_ns'), 'window restore boundary')
    boundary_wall = integer(boundary.get('post_restore_unix_ns'), 'window restore realtime boundary')
    if (boundary.get('restored') is not True or boundary.get('drop_installed') is not False
            or boundary.get('clock_guard_ns') != guard
            or boundary.get('activity_since_unix_ns') != boundary_wall + guard
            or rr.get('drop_end_monotonic_ns') is None
            or rr['drop_end_monotonic_ns'] > boundary_mono):
        raise ValueError('window restore boundary is not after the verified delete')
    if not (begin_event['event_index'] < inter_open['event_index']
            < inter_close['event_index'] < boundary['event_index']
            and inter_close['monotonic_ns'] <= boundary_mono <= boundary['monotonic_ns']
            and inter_close['monotonic_ns'] - inter_open['monotonic_ns']
                >= plan['drop_duration_sec'] * 10**9):
        raise ValueError('window drop/restore chronology is invalid')

    verified = _window_one(events, 'discovery_window_restore_verified', index)
    activities = [e for e in events if e.get('event') == 'discovery_activity_observation'
                  and e.get('window') == 'post_restore_%d' % index
                  and e['event_index'] < verified['event_index']]
    if not activities:
        raise ValueError('window post-restore activity evidence absent')
    confirmed = _window_one(events, 'discovery_recovery_confirmed', index)
    cr = confirmed['receipt']
    activity_ref = cr.get('activity_receipt', {}).get('evidence_ref')
    bound = [e for e in activities if e['raw_event_ref'] == activity_ref]
    if len(bound) != 1:
        raise ValueError('window recovery confirmation lacks one bound activity observation')
    activity_event = bound[0]
    if activity_event is not activities[-1]:
        raise ValueError('window recovery confirmation cites a superseded activity sample')
    if (activity_event.get('after_monotonic_ns') != boundary_mono
            or activity_event.get('clock_guard_ns') != guard):
        raise ValueError('window activity is not strictly after this restore boundary')
    # The observer's summary window may extend beyond the packet observation.
    # Packet evidence itself is still required to be strictly after boundary.
    activity_proof = _activity(activity_event, capture, tokens, boundary_wall + guard,
                               activity_event['activity']['until_ns'], ports, pair_bindings,
                               after_ns=boundary_mono)
    if activity_proof['unix_ns'] < boundary_wall:
        raise ValueError('window activity realtime stamp precedes restoration')
    if abs((activity_event['time_ns'] - activity_event['monotonic_ns'])
           - (boundary_wall - boundary_mono)) > guard:
        raise ValueError('window clocks changed offset')
    activity_ns = activity_proof['received_monotonic_ns']
    if (cr.get('recovery_verified') is not True
            or cr.get('restore_end_monotonic_ns') != rr.get('drop_end_monotonic_ns')):
        raise ValueError('window recovery confirmation lacks independent activity binding')
    _bindings(cr['pair_bindings'], tokens, isolation, config['domain_id'])
    if cr['pair_bindings'].get('worker_start_ticks') != snapshot['bindings'].get('worker_start_ticks'):
        raise ValueError('window endpoint process generation changed')
    if verified.get('restored') is not True or verified.get('recovery_confirmed') is not True:
        raise ValueError('window restore verification receipt is incomplete')

    return {
        'window_id': plan['window_id'], 'window_index': index,
        'trigger_anchor': plan['trigger_anchor'],
        'trigger_offset_sec': plan['trigger_offset_sec'],
        'drop_duration_sec': plan['drop_duration_sec'],
        'restore_interval_sec': plan['restore_interval_sec'],
        'complete': True,
        'drop': {'active': True, 'configured': True, 'applied': True,
                 'pair_bound': True, 'counter_observed': True,
                 'packets_before': delta['packets_before'],
                 'packets_after': delta['packets_after'],
                 'packets_delta': delta['packets_delta'],
                 'bytes_before': delta['bytes_before'],
                 'bytes_after': delta['bytes_after'],
                 'bytes_delta': delta['bytes_delta'],
                 'hit': delta['hit'],
                 'counter_semantics': 'per_window_delta_only', 'table': delta['table']},
        'boundary_ns': boundary_mono, 'boundary_event_ref': _ref(boundary),
        'restored': True, 'restore_verified': True,
        'activity_observed': True, 'activity_ns': activity_ns,
        'activity_event_ref': _ref(activity_event),
        'recovery_confirmed': True,
        'recovery_event_ref': _ref(confirmed),
        'drop_event_ref': _ref(dropped), 'restore_event_ref': _ref(restored),
        **({'timing': timing, 'counter_event_ref': _ref(emitted)} if timing is not None else {}),
    }


def _multi_window_recovery_facts(events, runtime, config):
    facts = {'schema_version': 1,
             'environment_healthy': runtime.get('environment', {}).get('ok') is True,
             'process_healthy': False, 'preconditions_reached': False,
             'endpoint_identity_complete': False, 'observed_effects': [],
             'sent': [], 'received': [], 'windows': []}
    try:
        if config.get('target_id') == F2_WINDOWS_TARGET:
            f2_windows_operations(config)
        plans = config.get('environment_windows')
        if not isinstance(plans, list) or not 1 <= len(plans) <= 3:
            raise ValueError('window plan must contain 1..3 windows')
        metadata = runtime.get('execution_metadata') or {}
        workers = metadata.get('workers', [])
        writer_workers = [w for w in workers if w.get('role') == 'writer']
        reader_workers = [w for w in workers if w.get('role') in ('reader', 'reader2')]
        probe_workers = [w for w in workers if w.get('role') == 'probe']
        pair_workers = writer_workers[-1:] + [w for w in reader_workers if w.get('role') == 'reader'][-1:]
        if (len(pair_workers) != 2 or len({w['pid'] for w in pair_workers}) != 2
                or len(probe_workers) > 1
                or any(w.get('returncode') != 0 for w in workers)
                or _one(events, 'namespace_controller_exited').get('returncode') != 0
                or runtime.get('carrier_implementation') not in CARRIER_IMPLEMENTATIONS
                or any(e['event'] in ('worker_error', 'worker_unexpected_exit',
                                      'runtime_abort', 'owned_worker_cleanup_signal')
                       for e in events)):
            raise ValueError('discovery pair/controller lifecycle incomplete')
        facts['process_healthy'] = True
        admission = _one(events, 'namespace_host_admission_verified')
        isolation = _isolation(admission['isolation'])
        setup = _one(events, 'discovery_network_setup')['receipt']
        if setup.get('health_ok') is not True:
            raise ValueError('discovery namespace initial state not healthy')
        if runtime.get('carrier_implementation') == IMPLEMENTATION:
            # Drop carrier: the initial state must be an untouched nft ruleset.
            if setup.get('drop_installed') is not False:
                raise ValueError('discovery namespace initial state not neutral')
        else:
            # netem carrier: the initial state must have no installed qdisc or
            # filter, which it reports instead of the nft fields.
            if (setup.get('mutation_installed') is not False
                    or setup.get('initial_filter') not in ([], None)):
                raise ValueError('discovery netem carrier initial state not neutral')
        tokens = {r: _one(events, 'endpoint_created', role=r) for r in ('writer', 'reader')}
        if len({t['pid'] for t in tokens.values()}) != 2 or len({t['endpoint_gid'] for t in tokens.values()}) != 2:
            raise ValueError('independent endpoint process/GID identity missing')
        for role, token in tokens.items():
            if (token.get('rmw_identifier') != role_rmw(config, role) or token.get('topic') != config['topic']
                    or token.get('qos') != config['qos'] or token.get('message_type') != config['message_type']
                    or not token.get('endpoint_generation')):
                raise ValueError('discovery endpoint submitted identity/config mismatch')
            facts[role] = {'id': token['endpoint_gid'], 'generation': token['endpoint_generation'],
                           'created_ns': token['created_monotonic_ns']}
        snapshot = _one(events, 'discovery_precondition_snapshot')
        ports = _bindings(snapshot['bindings'], tokens, isolation, config['domain_id'])
        from r2f.runtime.discovery_profiles import CYCLONE_XML, FAST_XML
        profiles = runtime['environment']['discovery_namespace']['profiles']
        hashes = {}
        for number, role in enumerate(('writer', 'reader')):
            record = profiles[role]
            path = _artifact(runtime, record['path'])
            verify_raw(path, record['sha256'])
            expected_profile = CYCLONE_XML if config['rmw'] == 'rmw_cyclonedds_cpp'                 else FAST_XML.format(role=role, index=number)
            if path.read_bytes() != expected_profile.encode():
                raise ValueError('actual discovery transport profile differs from reviewed semantic carrier')
            hashes[role] = record['sha256']
        facts['transport_profile_sha256'] = hashes

        capture = _capture(events, runtime, config, isolation)
        guard = 50_000_000
        previous_ref = snapshot['raw_event_ref']
        for index, plan in enumerate(plans):
            fact = _window_fact(events, index, plan, tokens, isolation, config, previous_ref,
                                capture, ports, snapshot['bindings']['bindings'], guard)
            if index:
                prior = facts['windows'][index - 1]
                if fact['drop_event_ref'] and prior['recovery_event_ref']:
                    prior_event = _resolve(events, prior['recovery_event_ref'])
                    drop_event = _resolve(events, fact['drop_event_ref'])
                    if not prior_event['event_index'] < drop_event['event_index']:
                        raise ValueError('window[%d]: drop does not follow the previous recovery' % index)
                    if drop_event['monotonic_ns'] - prior_event['monotonic_ns'] \
                            < plans[index - 1]['restore_interval_sec'] * 10**9:
                        raise ValueError('window[%d]: restore_interval_sec not honoured before next drop' % index)
            facts['windows'].append(fact)
            previous_ref = fact['recovery_event_ref']

        opening = _one(events, 'window_open', window='post_restore_delivery')
        closing = _one(events, 'window_close', window='post_restore_delivery')
        # The recovery oracle needs a match proof established *after* the final
        # restore boundary.  The pre-window snapshot only proves the session was
        # healthy before the interruption and cannot stand in for re-establishment.
        final_boundary = _window_one(events, 'discovery_restore_boundary', len(plans) - 1)
        final_boundary_mono = integer(final_boundary.get('post_restore_monotonic_ns'),
                                      'final window restore boundary')
        match_ns, match_proof = fresh_match_proof(
            events, tokens, opening, anchor_ns=final_boundary_mono)
        facts['match_snapshot_proof'] = match_proof
        facts['match'] = {'writer_id': tokens['writer']['endpoint_gid'],
                          'reader_id': tokens['reader']['endpoint_gid'],
                          'at_ns': match_ns}
        # The netem carrier opens a single named window; a drop chain opens one
        # interruption window per planned index. Both must still be present.
        if runtime.get('carrier_implementation') == NETEM_IMPLEMENTATION:
            _one(events, 'window_open', window='discovery_netem')
        else:
            _window_one(events, 'window_open', 0, window='discovery_interruption')
        if not (_window_one(events, 'discovery_window_restore_verified',
                            len(plans) - 1)['event_index'] < opening['event_index']
                < closing['event_index']):
            raise ValueError('postrestore delivery window is not closed after the final window')
        expected = list(range(config['pre_match_count'],
                              config['pre_match_count'] + config['post_match_count']))
        contract = _one(events, 'delivery_contract')
        if (contract.get('expected_sequences') != expected
                or contract.get('sentinel_sequence') != expected[-1]
                or contract.get('phase') != 'post_restore'
                or contract.get('qos') != config['qos']
                or contract.get('prejoin_in_delivery_contract') is not False):
            raise ValueError('postrestore delivery contract differs from submitted config')
        sent = [e for e in events if e['event'] == 'send' and e.get('phase') == 'post_restore']
        received = [e for e in events if e['event'] == 'receive' and e.get('phase') == 'post_restore']
        if [e.get('send_sequence') for e in sent] != expected:
            raise ValueError('postrestore send obligations are incomplete')
        for event in sent:
            if (not opening['event_index'] < event['event_index'] < closing['event_index']
                    or event.get('writer_gid') != tokens['writer']['endpoint_gid']
                    or event.get('endpoint_generation') != tokens['writer']['endpoint_generation']
                    or event.get('publish_returned') is not True):
                raise ValueError('postrestore send identity/window proof incomplete')
        for event in received:
            if (not opening['event_index'] < event['event_index'] < closing['event_index']
                    or event.get('reader_gid') != tokens['reader']['endpoint_gid']
                    or event.get('endpoint_generation') != tokens['reader']['endpoint_generation']
                    or event.get('payload_run_id') != config['run_id']):
                raise ValueError('postrestore receive identity/window proof incomplete')
        if not received:
            raise ValueError('no bounded delivery identity witness')
        direct = all(e.get('publisher_gid') is not None for e in received)
        graphs = {}
        for role in tokens:
            after = [e for e in events if e['event'] == 'graph_snapshot'
                     and e.get('role') == role and e['monotonic_ns'] >= closing['monotonic_ns']]
            if (not after
                    or {x['endpoint_gid'] for x in after[0]['publishers']} != {tokens['writer']['endpoint_gid']}
                    or {x['endpoint_gid'] for x in after[0]['subscriptions']} != {tokens['reader']['endpoint_gid']}):
                raise ValueError('postrestore graph identity has no closing boundary')
            graphs[role] = {'after_index': after[0]['event_index']}
        for event in sent:
            facts['sent'].append({'sequence': event['send_sequence'], 'at_ns': event['monotonic_ns'],
                                  'writer_id': event.get('writer_gid') or tokens['writer']['endpoint_gid'],
                                  'generation': tokens['writer']['endpoint_generation']})
        for event in received:
            facts['received'].append({'sequence': event['send_sequence'], 'at_ns': event['monotonic_ns'],
                                      'writer_id': event.get('publisher_gid') or tokens['writer']['endpoint_gid'],
                                      'writer_generation': tokens['writer']['endpoint_generation'],
                                      'reader_id': event['reader_gid'],
                                      'reader_generation': event['endpoint_generation']})
        cleanup = _one(events, 'discovery_cleanup_restore')['receipt']
        if cleanup.get('restored') is not True:
            raise ValueError('discovery cleanup restore not verified')
        for observed_step in runtime.get('observation_receipts', []):
            _step_evidence(events, observed_step)
        total_hit = all(w['drop']['hit'] for w in facts['windows'])
        facts.update(
            endpoint_identity_complete=True,
            preconditions_reached=len(runtime.get('precondition_receipts', [])) == 3,
            identity_proof={'basis': 'message_info_gid' if direct else 'endpoint_correlated',
                            'direct_gid_match': all(e.get('publisher_gid') == tokens['writer']['endpoint_gid']
                                                    for e in received) if direct else None,
                            'publisher_gid_status': 'available' if direct else 'unavailable_api',
                            'graph_boundary_receipts': graphs},
            drop={'active': True, 'configured': True, 'applied': True, 'hit': total_hit,
                  'pair_bound': True, 'counter_observed': True,
                  'packets': max(w['drop']['packets_delta'] for w in facts['windows'])},
            recovery={'boundary_ns': facts['windows'][-1]['boundary_ns'],
                      'activity_ns': facts['windows'][-1]['activity_ns'],
                      'activity_identity_correlated': True, 'fresh_match_and_graph': True,
                      'rules_restored': True, 'identity_stable': True,
                      'match_continuity': 'retained',
                      'match_continuity_scope': 'observed_public_count_samples',
                      'clock_translation': 'kernel_realtime_plus_independent_receive_monotonic',
                      'window_count': len(facts['windows']),
                      'per_window_independent': True},
            observed_effects=['discovery_temporarily_unavailable', 'discovery_restored'],
            # PrimaryFacts' delivery window starts when the post-restore
            # delivery window opens, not when the interruption began.  The
            # interruption span is recorded separately under ``windows``.
            window={'start_ns': opening['monotonic_ns'],
                    'end_ns': closing['monotonic_ns'], 'complete': True},
            window_complete=True,
            window_completion={'complete': True, 'window_count': len(facts['windows'])},
        )
        if config.get('target_id') == F2_WINDOWS_TARGET:
            facts['operation_receipts'] = _f2_operation_facts(events, runtime, config, facts)
    except (KeyError, ValueError, TypeError, IndexError, OSError, struct.error, DiscoveryNetworkError) as exc:
        message = str(exc)
        if not message.startswith('window['):
            message = 'window[%d]: %s' % (len(facts.get('windows', [])), message)
        facts['evidence_error'] = message
        if config.get('target_id') == F2_WINDOWS_TARGET:
            facts['operation_receipts'] = []
            facts['observation_complete'] = False
            facts['responsibility_layer'] = 'harness'
            facts['candidate_eligible'] = False
        facts['endpoint_identity_complete'] = False
        facts['window'] = {'complete': False}
        facts['window_complete'] = False
        facts['window_completion'] = {'complete': False, 'reason': message,
                                      'completed_windows': len(facts.get('windows', []))}
    from .state_projection import attach_state_observations
    return attach_state_observations(facts, events, runtime, config)


def recovery_facts(events, runtime, config):
    if _multi_window_chain(events, config):
        return attach_participant_lease(_multi_window_recovery_facts(events, runtime, config),
                                        events, runtime, config)
    facts = {'schema_version': 1, 'environment_healthy': runtime.get('environment', {}).get('ok') is True,
             'process_healthy': False, 'preconditions_reached': False, 'endpoint_identity_complete': False,
             'observed_effects': [], 'sent': [], 'received': []}
    # Carrier-level facts the runtime established independently of the delivery
    # contract must survive into the typed facts; dropping them here would turn
    # an explicit observation limit back into a generic ineffective mutation.
    for key in ('delivery_inside_mutation_window', 'delivery_window_refs',
                'netem_install_ref', 'netem_restore_ref', 'netem_boundary_ref',
                'delivery_overlap_chronology', 'data_plane_observed',
                'data_plane_observation_reason'):
        value = (runtime.get('facts') or {}).get(key)
        if value is not None:
            facts[key] = value
    try:
        metadata = runtime.get('execution_metadata') or {}
        workers = metadata.get('workers', [])
        all_workers = [w for w in workers if w.get('role') in ('writer', 'reader', 'reader2')]
        probe_workers = [w for w in workers if w.get('role') == 'probe']
        writer_workers = [w for w in all_workers if w.get('role') == 'writer']
        reader_workers = [w for w in all_workers if w.get('role') in ('reader', 'reader2')]
        # Active pair: one writer and the latest reader generation.  Older
        # reader generations remain in metadata as history.
        pair_workers = [w for w in writer_workers if w.get('role') == 'writer'][-1:] + [w for w in reader_workers if w.get('role') == 'reader'][-1:]
        if (len(pair_workers) != 2 or len({w['pid'] for w in pair_workers}) != 2
                or len(probe_workers) > 1
                or any(w.get('returncode') != 0 for w in workers)
                or _one(events, 'namespace_controller_exited').get('returncode') != 0
                or runtime.get('carrier_implementation') not in CARRIER_IMPLEMENTATIONS
                or any(e['event'] in ('worker_error', 'worker_unexpected_exit', 'runtime_abort', 'owned_worker_cleanup_signal') for e in events)):
            raise ValueError('discovery pair/controller lifecycle incomplete')
        if probe_workers and any(w.get('pid') in {p['pid'] for p in pair_workers} for w in probe_workers):
            raise ValueError('discovery probe must be an independent process')
        facts['process_healthy'] = True
        admission = _one(events, 'namespace_host_admission_verified')
        isolation = _isolation(admission['isolation'])
        setup_event = _one(events, 'discovery_network_setup')
        setup = setup_event['receipt']
        if setup.get('health_ok') is not True:
            raise ValueError('discovery namespace initial state not healthy')
        if runtime.get('carrier_implementation') == IMPLEMENTATION:
            if setup.get('drop_installed') is not False:
                raise ValueError('discovery namespace initial state not neutral')
            if any('table' in e for e in setup['initial_nft']['nftables']):
                raise ValueError('discovery namespace had preexisting nft rules')
        else:
            if (setup.get('mutation_installed') is not False
                    or setup.get('initial_filter') not in ([], None)):
                raise ValueError('discovery netem carrier initial state not neutral')
        tokens = {r: _one(events, 'endpoint_created', role=r) for r in ('writer', 'reader')}
        # F3: recreated readers are additional discovery stimuli only.  The
        # original writer/reader pair stays the delivery and binding contract;
        # extras are recorded separately and never re-labelled as the reader.
        recreates = [e for e in events if e.get('event') == 'discovery_reader_recreated']
        if len(recreates) > MAX_RECREATED_READERS:
            raise ValueError('at most %d reader recreations are supported in one window'
                             % MAX_RECREATED_READERS)
        if recreates:
            # Recreated readers are explicit additional generations used to
            # stimulate discovery.  The original writer/reader pair remains the
            # delivery and binding contract; extras are recorded separately and
            # never re-labelled as the original reader.
            extras = []
            for recreate in recreates:
                role_name = recreate.get('role')
                token = next(e for e in events if e.get('event') == 'endpoint_created'
                             and e.get('role') == role_name)
                if token.get('endpoint_gid') in {t.get('endpoint_gid') for t in tokens.values()}:
                    raise ValueError('recreated reader reused a pair endpoint GID')
                extras.append({'role': role_name, 'endpoint_gid': token.get('endpoint_gid'),
                               'generation': token.get('endpoint_generation'),
                               'pid': token.get('pid'), 'pair_member': False,
                               'recreate_index': recreate.get('recreate_index'),
                               'destroyed': False})
            destroyed = {e.get('role') for e in events
                         if e.get('event') == 'discovery_reader_destroyed'}
            for extra in extras:
                extra['destroyed'] = extra['role'] in destroyed
            facts['recreated_readers'] = extras
        destroyed_readers = [e.get('role') for e in events
                             if e.get('event') == 'discovery_reader_destroyed']
        if destroyed_readers:
            facts['destroyed_readers'] = destroyed_readers
        writer_churn = [{'role': e.get('role'), 'generation': e.get('generation'),
                         'pid': e.get('pid'), 'pair_member': False}
                        for e in events if e.get('event') == 'discovery_writer_recreated']
        if writer_churn:
            facts['churned_writers'] = writer_churn
        if len({t['pid'] for t in tokens.values()}) != 2 or len({t['endpoint_gid'] for t in tokens.values()}) != 2:
            raise ValueError('independent endpoint process/GID identity missing')
        for role, token in tokens.items():
            if (token.get('rmw_identifier') != role_rmw(config, role) or token.get('topic') != config['topic']
                    or token.get('qos') != config['qos'] or token.get('message_type') != config['message_type']
                    or not token.get('endpoint_generation')
                    or not any(w.get('role') == token.get('role') and w.get('pid') == token['pid'] for w in workers)):
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
        netem_carrier = runtime.get('carrier_implementation') == NETEM_IMPLEMENTATION
        window_name = 'discovery_netem' if netem_carrier else 'discovery_interruption'
        inter_open = _one(events, 'window_open', window=window_name)
        inter_close = _one(events, 'window_close', window=window_name)
        boundary_mono = integer(boundary['post_restore_monotonic_ns'], 'restore boundary')
        boundary_wall = integer(boundary['post_restore_unix_ns'], 'restore realtime boundary')
        # ``netem_covers_delivery`` splits the netem carrier into two shapes.
        # The additive ``netem_delivery_overlap`` raw event is the only proof
        # that the judged delivery window ran while the tc class was installed;
        # its absence keeps the already-sealed post-restore chronology intact.
        overlap = next((e for e in events if e.get('event') == 'netem_delivery_overlap'), None)
        covers_delivery = bool(netem_carrier and overlap is not None)
        if covers_delivery:
            # Two explicitly bounded netem segments: the discovery stimulus
            # window (install/restore/boundary, sealed chronology) and then the
            # real public delivery window, opened inside its own verified
            # install..restore segment.  ``netem_delivery_overlap`` is the only
            # proof that the judged delivery ran while tc was installed.
            install = _one(events, 'delivery_netem_begin')
            end_receipt = _one(events, 'delivery_netem_end')
            delivery_boundary = _one(events, 'delivery_restore_boundary')
            delivery_boundary_mono = integer(delivery_boundary['post_restore_monotonic_ns'],
                                             'delivery restore boundary')
            stimulus_begin = _one(events, 'discovery_netem_begin')
            stimulus_end = _one(events, 'discovery_netem_end')
            bound = {'install_event_ref': install, 'delivery_open_ref': opening,
                     'delivery_close_ref': closing, 'restore_event_ref': end_receipt,
                     'boundary_event_ref': delivery_boundary,
                     'stimulus_install_ref': stimulus_begin,
                     'stimulus_restore_ref': stimulus_end,
                     'stimulus_boundary_ref': boundary}
            if (overlap.get('delivery_inside_mutation_window') is not True
                    or any(_resolve(events, overlap.get(key)) is not value
                           for key, value in bound.items())
                    or not (install['event_index'] < opening['event_index']
                            < closing['event_index'] < end_receipt['event_index']
                            < delivery_boundary['event_index'])):
                raise ValueError('netem delivery overlap event is not bound to the real windows')
            # Sealed stimulus chronology is unchanged: begin < netem open <
            # restore boundary < recovery observation < delivery window.  The
            # carrier's own window close follows the delivery segment, so it is
            # the outer bound rather than an inner one.
            chronological = (begin['event_index'] < inter_open['event_index']
                             < boundary['event_index'] < opening['event_index']
                             < closing['event_index'] < end_receipt['event_index']
                             < delivery_boundary['event_index'] < inter_close['event_index']
                             and inter_open['monotonic_ns'] <= boundary_mono
                             <= boundary['monotonic_ns'] <= opening['monotonic_ns']
                             <= closing['monotonic_ns'] <= delivery_boundary_mono
                             <= inter_close['monotonic_ns'])
            duration = inter_close['monotonic_ns'] - inter_open['monotonic_ns']
            required = config.get('drop_duration_sec') * 10**9
        elif netem_carrier:
            # netem installs, observes, restores, then closes its own window:
            # begin < open < restore_boundary < close < post_restore_open.
            chronological = (begin['event_index'] < inter_open['event_index']
                             < boundary['event_index'] < inter_close['event_index']
                             < opening['event_index'] < closing['event_index']
                             and inter_open['monotonic_ns'] <= boundary_mono
                             <= boundary['monotonic_ns'] <= inter_close['monotonic_ns']
                             <= opening['monotonic_ns'])
            duration = inter_close['monotonic_ns'] - inter_open['monotonic_ns']
            required = config.get('drop_duration_sec') * 10**9
        else:
            chronological = (begin['event_index'] < inter_open['event_index']
                             < inter_close['event_index'] < boundary['event_index']
                             < opening['event_index'] < closing['event_index']
                             and inter_close['monotonic_ns'] <= boundary_mono
                             <= boundary['monotonic_ns'] <= opening['monotonic_ns'])
            duration = inter_close['monotonic_ns'] - inter_open['monotonic_ns']
            required = config['drop_duration_sec'] * 10**9
        if not chronological or duration < required:
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
        if active and netem_carrier:
            # netem installs a qdisc/filter, not an nft drop table. Its own
            # receipt proves configure/apply/restore and grouped real counters;
            # the primary delivery oracle still judges wire behaviour.
            begin_receipt = _one(events, 'discovery_netem_begin')
            end_receipt = _one(events, 'discovery_netem_end')
            be, ee = begin_receipt['receipt'], end_receipt['receipt']
            neutral_spec = be.get('neutral') is True
            hit = bool(not neutral_spec and be.get('hit'))
            if (be.get('implementation') != NETEM_IMPLEMENTATION
                    or ee.get('restored') is not True
                    or ee.get('only_noqueue_root') is not True
                    or boundary.get('restored') is not True
                    or boundary.get('neutral') is not False
                    or not begin['event_index'] < begin_receipt['event_index'] < inter_open['event_index']
                    or not inter_open['event_index'] < end_receipt['event_index'] < boundary['event_index']):
                raise ValueError('real netem window install/verified restore evidence incomplete')
            counters = [e['receipt'] for e in events if e['event'] == 'discovery_netem_counter'
                        and inter_open['event_index'] < e['event_index'] < end_receipt['event_index']]
            counters.append(ee.get('counter_snapshot') or {})
            if not counters or not any(bool(c.get('hit') or c.get('mutation_hit')) for c in counters):
                raise ValueError('netem window has no counted real hit')
            packets = max(int(int((c.get('filter') or {}).get('packets') or 0)
                              + int((c.get('group_counters', {}).get('data') or {}).get('packets') or 0))
                          for c in counters)
            active = True
        elif active:
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
            # During an intentional discovery drop, absence of pair discovery
            # activity is the expected effect.  The drop is validated by the
            # monotonic counter above, not by requiring both participants to be
            # active in the dropped window.
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
        if covers_delivery:
            # Install/restore refs name the segment that actually covered the
            # judged delivery, not the earlier discovery-stimulus segment.
            facts['delivery_inside_mutation_window'] = True
            facts['delivery_window_refs'] = {'open_event_ref': _ref(opening),
                                             'close_event_ref': _ref(closing)}
            facts['netem_install_ref'] = _ref(_one(events, 'delivery_netem_begin'))
            facts['netem_restore_ref'] = _ref(_one(events, 'delivery_netem_end'))
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
        observed_ids = [o.get('step_id') for o in observations]
        if (len(observed_ids) < 2
                or not str(observed_ids[0]).startswith('observe_recovery_graph')
                or observed_ids[-1] != 'observe_final_graph'):
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
    except (KeyError, ValueError, TypeError, IndexError, OSError, struct.error, DiscoveryNetworkError) as exc:
        facts['evidence_error'] = str(exc)
        facts['endpoint_identity_complete'] = False
        facts['window'] = {'complete': False}
    from .state_projection import attach_state_observations
    facts = attach_participant_lease(facts, events, runtime, config)
    return attach_state_observations(facts, events, runtime, config)
