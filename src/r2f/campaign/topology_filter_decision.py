"""Typed actual-evaluate evidence from immutable raw; never a window verdict.

No native library is loaded. Explicit broken trace evidence stays declared and
unknown, rather than selecting a legacy/model interpretation.
"""
from __future__ import annotations

import ctypes as C
import hashlib
import os
import re
from copy import deepcopy
from pathlib import Path

from r2f.instrumentation.fastdds_filter_trace.activation import ABI, selected_manifest
from r2f.instrumentation.fastdds_filter_trace.observer import Record, Stats, decode_record, decode_stats
from r2f.instrumentation.fastdds_peer.activation import manifest_for_reference
from r2f.runtime.endpoint_incarnation import endpoint_identity_at
from r2f.runtime.filter_action_receipt import filter_action_evidence
from r2f.runtime.topology_worker import worker_role_kind
from .topology_sample_attribution import native_delivery_identity

SCHEMA = 'r2f.topology-native-filter-decision-evidence/v1'
_SELECTION = 'native_filter_trace_provider_selected'
_DECISION = 'native_content_filter_decision'
_HEALTH = 'native_filter_trace_health'
_LINEAGE = ('logical_id', 'generation', 'endpoint_generation', 'pid', 'pgid',
            'process_start_ticks', 'run_id', 'domain_id', 'monotonic_clock_identity',
            'rmw_identifier', 'node_name', 'topic', 'message_type', 'role')
_RETIRED = {'endpoint_destroyed', 'topology_endpoint_destroyed', 'worker_exit',
            'topology_worker_exit', 'topology_generation_retired'}


def _need(ok, reason):
    if not ok:
        raise ValueError('native_filter_decision:' + reason)


def _exact(left, right):
    """JSON equality without Python's bool == 1 or 1 == 1.0 aliases."""
    return (type(left) is type(right) and
            (left.keys() == right.keys() and all(_exact(left[k], right[k]) for k in left)
             if isinstance(left, dict) else
             len(left) == len(right) and all(_exact(a, b) for a, b in zip(left, right))
             if isinstance(left, list) else left == right))


def _same(row, token):
    return all(token.get(k) is not None and _exact(row.get(k), token[k]) for k in _LINEAGE)


def _one(rows, reason):
    _need(len(rows) == 1, reason)
    return rows[0]


def _integer(value, ctype):
    bits = C.sizeof(ctype) * 8
    signed = ctype(-1).value == -1
    low, high = (-(1 << (bits - 1)), (1 << (bits - 1)) - 1) if signed else (0, (1 << bits) - 1)
    _need(type(value) is int and low <= value <= high, 'native_integer_type_or_range')
    return value


def _decode(row):
    receipt = row.get('receipt')
    _need(isinstance(receipt, dict), 'receipt_missing')
    raw = receipt.get('native_result')
    _need(isinstance(raw, dict), 'native_result_missing')
    record = Record()
    for name, ctype in Record._fields_[:13]:
        setattr(record, name, _integer(raw.get(name), ctype))
    for name in ('reader_guid', 'writer_guid'):
        text = raw.get(name)
        _need(isinstance(text, str) and re.fullmatch('[0-9a-f]{32}', text) is not None
              and text != '0' * 32, 'native_guid_invalid')
        getattr(record, name)[:] = bytes.fromhex(text)
    text = raw.get('payload_hex')
    _need(isinstance(text, str) and re.fullmatch('(?:[0-9a-f]{2})*', text) is not None,
          'native_payload_hex_invalid')
    payload = bytes.fromhex(text)
    _need(len(payload) == record.payload_copied <= ABI['payload_capacity'], 'native_payload_size_invalid')
    record.payload[:len(payload)] = payload
    _need(type(row.get('monotonic_ns')) is int and type(row.get('pid')) is int, 'capture_envelope_invalid')
    decoded = decode_record(record, now_ns=row['monotonic_ns'], pid=row['pid'])
    expected = dict(decoded, manifest_reference=receipt.get('manifest_reference'),
                    provider_mapping=receipt.get('provider_mapping'))
    _need(_exact(receipt, expected), 'receipt_differs_from_native_decoder')
    _need(decoded['status'] == 'returned' and type(decoded['decision']) is bool
          and decoded['payload_complete'], 'evaluate_did_not_return_complete_bool')
    return decoded


def _cdr(decoded):
    payload = bytes.fromhex(decoded['payload_cdr_hex'])
    _need(len(payload) >= 9 and payload[2:4] == b'\0\0'
          and payload[:2] in (b'\0\0', b'\0\1'), 'cdr_encapsulation_invalid')
    kind = int.from_bytes(payload[:2], 'big')
    _need(decoded['payload_encapsulation'] == kind, 'cdr_encapsulation_mismatch')
    size = int.from_bytes(payload[4:8], 'little' if kind == 1 else 'big')
    _need(size > 0 and len(payload) == size + 8 and payload[-1:] == b'\0'
          and b'\0' not in payload[8:-1], 'cdr_string_length_or_nul_invalid')
    return payload[8:-1].decode('utf-8')


def _at_capture(events, origin, started, finished):
    """Keep original raw indices; cut at the first future event of this worker.

    Cross-worker stdout drain/envelope order is not a capture clock. In
    particular a delayed writer receipt must not resolve a later reader GID.
    """
    _need(type(origin.get('monotonic_ns')) is int and origin['monotonic_ns'] <= started,
          'capture_before_endpoint_creation')
    same = lambda e: (e.get('logical_id') == origin['logical_id']
                      and e.get('generation') == origin['generation'])
    _need(not any(same(e) and e.get('event') in _RETIRED
                  and e.get('monotonic_ns', finished + 1) <= finished for e in events),
          'capture_endpoint_retired')
    cut = next((i for i, e in enumerate(events) if same(e)
                and type(e.get('monotonic_ns')) is int and e['monotonic_ns'] > started), len(events))
    identity = endpoint_identity_at(events, origin, before_index=cut)
    _need(identity['identity_valid_after_ns'] <= started, 'capture_before_identity_ack')
    return identity


def _mapping(selection, manifest):
    binary = manifest['binary']
    path = Path(binary['path']).resolve(strict=True)
    stat = path.stat()
    expected = dict(path=str(path), sha256=binary['sha256'],
                    device=f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}', inode=stat.st_ino)
    _need(hashlib.sha256(path.read_bytes()).hexdigest() == expected['sha256']
          and _exact(selection.get('provider_mapping'), expected), 'provider_mapping_pin_invalid')
    return expected


def _worker_proof(selection, events, manifest, peer, peer_reference, raw_ref):
    origin = _one([e for e in events if e.get('event') == 'endpoint_created'
                   and _same(e, selection)], 'selected_worker_creation_not_unique')
    _need(selection['event_index'] < origin['event_index']
          and selection['monotonic_ns'] <= origin['monotonic_ns']
          and selection.get('observation_mode') == 'instrumentation'
          and selection.get('real_ros') is True and selection.get('fixture') is not True
          and selection.get('rmw_identifier') == 'rmw_fastrtps_cpp'
          and selection.get('observed_domain_id') == origin['domain_id'], 'selection_worker_scope_invalid')
    reference = selection.get('peer_manifest_reference')
    _need(isinstance(reference, dict) and reference.get('manifest_sha256') == peer_reference['manifest_sha256'],
          'selection_peer_reference_mismatch')
    expected_path = str(Path(raw_ref(origin)['path']).resolve().parent / 'native-peer-build-manifest.json')
    _need(reference.get('manifest_path') == expected_path
          and _exact(manifest_for_reference(reference), peer), 'selection_peer_manifest_path_or_content')
    _need(_exact(selection.get('manifest_reference'), peer['filter_trace'])
          and _exact(selection.get('binary'), manifest['binary'])
          and _exact(selection.get('vendor_binary'), manifest['vendor_binary'])
          and _exact(selection.get('abi'), ABI)
          and selection.get('hook_source_sha256') == manifest['hook_source_sha256']
          and selection.get('capture_scope') == 'actual_evaluate_calls_only', 'selection_trace_manifest_mismatch')
    mapping = _mapping(selection, manifest)
    native_selection = _one([e for e in events if e.get('event') == 'native_peer_provider_selected'
                            and _same(e, origin)], 'peer_selection_missing_or_duplicate')
    _need(native_selection['event_index'] < selection['event_index']
          and _exact(native_selection.get('manifest_reference'), reference)
          and _exact(native_selection.get('vendor_binary'), peer['binaries']['vendor'])
          and _exact(native_selection.get('provider_binary'), peer['binaries']['provider'])
          and native_selection.get('provider_source_sha256') == peer['provider_source_sha256']
          and _exact(native_selection.get('provider_abi'), peer['patch']['abi']),
          'native_peer_selection_differs_from_manifest')
    vendor = native_selection.get('vendor_identity', {})
    path = Path(peer['binaries']['vendor']['path']).resolve(strict=True)
    stat = path.stat()
    _need(vendor.get('identity_verified') is True and vendor.get('path') == str(path)
          and vendor.get('sha256') == peer['binaries']['vendor']['sha256']
          and type(vendor.get('inode')) is int and vendor['inode'] == stat.st_ino
          and vendor.get('device') == f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}',
          'selected_vendor_mapping_pin_invalid')
    records = [e for e in events if e.get('event') == _DECISION and _same(e, selection)]
    finals = [e for e in events if e.get('event') == _HEALTH and _same(e, selection) and e.get('final') is True]
    final = _one(finals, 'final_health_missing_or_duplicate')
    stats = final.get('stats')
    _need(isinstance(stats, dict), 'final_health_stats_missing')
    native = Stats()
    for key, ctype in Stats._fields_:
        setattr(native, key, _integer(stats.get(key), ctype))
    _need(_exact(decode_stats(native), stats) and stats['dropped'] == stats['pending'] == 0
          and stats['produced'] == stats['consumed'] == len(records)
          and stats['resolve_status'] in ((1,) if records else (0, 1))
          and final.get('capture_complete') is True and final.get('real_ros') is True
          and final.get('fixture') is not True and final.get('observation_mode') == 'instrumentation'
          and _exact(final.get('manifest_reference'), peer['filter_trace']), 'final_health_incomplete')
    _need(origin['event_index'] < final['event_index']
          and origin['monotonic_ns'] <= final['monotonic_ns'], 'final_health_before_creation')
    _need([e.get('receipt', {}).get('ordinal') for e in records] == list(range(1, len(records) + 1))
          and all(e['event_index'] < final['event_index'] and e['monotonic_ns'] <= final['monotonic_ns']
                  for e in records), 'ordinal_or_final_health_chronology')
    return origin, mapping, final


def _observation(row, events, program, selected, manifest, raw_ref):
    decoded = _decode(row)
    actor, mapping, health, selection = selected
    _need(_same(row, actor) and row.get('observation_mode') == 'instrumentation'
          and row.get('real_ros') is True and row.get('fixture') is not True
          and row['event_index'] > selection['event_index']
          and row['receipt']['manifest_reference'] == selection['manifest_reference']
          and _exact(row['receipt']['provider_mapping'], mapping), 'decision_selection_or_actor_mismatch')
    started, finished = decoded['capture_started_monotonic_ns'], decoded['capture_completed_monotonic_ns']
    writer = _one([e for e in events if e.get('event') == 'endpoint_created'
                   and worker_role_kind(e.get('role')) == 'writer'
                   and e.get('endpoint_gid') == decoded['writer_gid']], 'writer_creation_not_unique')
    readers = []
    for origin in events:
        if origin.get('event') != 'endpoint_created' or worker_role_kind(origin.get('role')) != 'reader':
            continue
        gids = [origin.get('endpoint_gid')] + [e.get('after_gid') for e in events
                if e.get('event') == 'endpoint_identity_transition'
                and e.get('logical_id') == origin.get('logical_id') and e.get('generation') == origin.get('generation')]
        if decoded['reader_gid'] in gids:
            readers.append(origin)
    reader = _one(readers, 'reader_creation_not_unique')
    current = _at_capture(events, reader, started, finished)
    _need(current['endpoint_gid'] == decoded['reader_gid'], 'reader_not_current_at_capture')
    _need(_at_capture(events, writer, started, finished)['endpoint_gid'] == decoded['writer_gid'],
          'writer_not_current_at_capture')
    _need(_same(actor, writer) or _same(actor, reader), 'capture_actor_not_pair_worker')
    _need(all(writer.get(k) is not None and _exact(reader.get(k), writer[k]) for k in
              ('run_id', 'domain_id', 'rmw_identifier', 'monotonic_clock_identity', 'topic', 'message_type'))
          and writer['message_type'] == 'std_msgs/msg/String', 'pair_context_mismatch')
    results = [e for e in events if e.get('event') == 'topology_filter_update_result'
               and e.get('logical_id') == reader['logical_id'] and e.get('generation') == reader['generation']
               and type(e.get('monotonic_ns')) is int and e['monotonic_ns'] <= started]
    _need(bool(results), 'enabled_action_before_capture_missing')
    latest = max(results, key=lambda e: e['monotonic_ns'])
    action = _one([a for a in program['actions'] if a.get('action_id') == latest.get('action_id')],
                  'filter_action_not_in_program')
    evidence = filter_action_evidence(action, events, before_index=latest['event_index'] + 1)
    _need(evidence['outcome_kind'] == 'configuration_readback' and evidence['observed_enabled'] is True
          and evidence['reader_gid'] == current['endpoint_gid']
          and evidence['reader_incarnation'] == current['incarnation_ordinal']
          and all(events[evidence[k]]['monotonic_ns'] <= started for k in ('update_ref', 'ack_ref', 'state_ref')),
          'current_enabled_action_not_committed')
    _need(not any(e.get('logical_id') == reader['logical_id'] and e.get('generation') == reader['generation']
                  and ((e.get('event') == 'endpoint_identity_snapshot' and e.get('phase') == 'before'
                        and latest['monotonic_ns'] < e.get('monotonic_ns', 0) <= finished)
                       or (e.get('event') == 'content_filter_dynamic_update'
                           and latest['monotonic_ns'] < e.get('update_started_ns', 0) <= finished)) for e in events),
          'next_update_started_before_capture_completed')
    text = _cdr(decoded)
    specs = {e['logical_id']: e for e in program['endpoints']}
    control_origins = [e for e in events if e.get('event') == 'endpoint_created'
                       and worker_role_kind(e.get('role')) == 'reader'
                       and 'initial_content_filter' in e and e['initial_content_filter'] is None
                       and specs.get(e.get('logical_id'), {}).get('filter') is None
                       and not any(a.get('op') == 'update_filter' and a.get('endpoint_id') == e['logical_id']
                                   for a in program['actions'])]
    controls = []
    sends = [e for e in events if e.get('event') == 'send']
    for received in events:
        if (received.get('event') != 'receive' or received.get('publisher_gid') != writer['endpoint_gid']
                or received.get('string_data') != text
                or not any(_same(received, c) for c in control_origins)):
            continue
        pubseq = received.get('message_info_receipt', {}).get('fields', {}).get('publication_sequence_number', {})
        if pubseq.get('value') != decoded['rtps_sequence']['value']:
            continue
        identity = native_delivery_identity(received, sends, events, ref=raw_ref)
        _need(identity is not None and identity['writer'] == writer['logical_id']
              and identity['writer_generation'] == writer['generation'], 'control_native_identity_unavailable')
        controls.append(identity)
    identity = _one(controls, 'unfiltered_control_missing_or_ambiguous')
    send = events[identity['send_ref']['event_index']]
    _need(send['string_data'] == text and identity['publication_sequence_number'] == decoded['rtps_sequence']['value']
          and identity['sequence'] == send['send_sequence'], 'cdr_control_send_mismatch')
    producer = send['publish_occurrence_receipt']
    _need(producer['call_begin_monotonic_ns'] <= started, 'capture_before_publish')
    if _same(actor, writer):
        _need(finished <= producer['call_return_monotonic_ns'], 'writer_capture_outside_publish_scope')
    return dict(writer=writer['logical_id'], writer_generation=writer['generation'], writer_gid=writer['endpoint_gid'],
        reader=reader['logical_id'], reader_generation=reader['generation'], reader_gid=current['endpoint_gid'],
        reader_incarnation=current['incarnation_ordinal'], sequence=identity['sequence'], decision=decoded['decision'],
        send_ref=identity['send_ref'], control_ref=identity['receive_ref'], control_identity=identity,
        decision_ref=raw_ref(row), action_id=action['action_id'], action_result_ref=raw_ref(latest),
        state_ref=raw_ref(events[evidence['state_ref']]), capture_started_ns=started, capture_finished_ns=finished,
        evidence=dict(schema=decoded['schema'], capture_basis=decoded['capture_basis'],
            payload_cdr_sha256=decoded['payload_cdr_sha256'], payload_sha256=hashlib.sha256(text.encode()).hexdigest(),
            native_ordinal=decoded['ordinal'], rtps_sequence=decoded['rtps_sequence'], filter_action=deepcopy(evidence)),
        provenance=dict(provider_selection_ref=raw_ref(selection), final_health_ref=raw_ref(health),
            manifest_reference=deepcopy(selection['manifest_reference']), peer_manifest_reference=deepcopy(selection['peer_manifest_reference']),
            provider_mapping=deepcopy(mapping), writer_creation_ref=raw_ref(writer), reader_creation_ref=raw_ref(reader),
            identity_ack_ref=raw_ref(events[evidence['ack_ref']])),
        filter_effect_observed=None, candidate_eligible=False)


def native_filter_decision_evidence(program, events, *, peer_reference, ref, before_index=None):
    """Return typed actual returns or explicit raw-backed unknowns, never SQL truth."""
    result = dict(schema=SCHEMA, declared=False, observations=[], unknowns=[])
    limit = len(events) if before_index is None else before_index
    _need(type(limit) is int and 0 <= limit <= len(events), 'prefix_invalid')
    rows = events[:limit]
    declarations = [e for e in rows if e.get('event') in {_SELECTION, _DECISION, _HEALTH}]

    def raw_ref(row):
        i = row.get('event_index')
        _need(type(i) is int and 0 <= i < len(rows) and rows[i] is row, 'raw_envelope_invalid')
        value = ref(row)
        _need(isinstance(value, dict) and value.get('event_index') == i
              and isinstance(value.get('path'), str) and bool(value['path'])
              and isinstance(value.get('sha256'), str) and re.fullmatch('[0-9a-f]{64}', value['sha256']) is not None,
              'raw_reference_invalid')
        return deepcopy(value)

    def unknown(row, exc):
        try:
            reference = raw_ref(row) if row is not None else None
        except (ValueError, KeyError, TypeError):
            reference = dict(event_index=row.get('event_index'), invalid_envelope=True)
        result['unknowns'].append(dict(raw_ref=reference, reason=str(exc),
                                      filter_effect_observed=None, candidate_eligible=False))

    anchor = declarations[0] if declarations else None
    try:
        peer = manifest_for_reference(peer_reference) if peer_reference is not None else None
        explicit = peer is not None and peer.get('filter_trace') is not None
        result['declared'] = bool(declarations) or explicit
        if not result['declared']:
            return result
        _need(explicit, 'raw_trace_without_explicit_manifest')
        manifest = selected_manifest(peer)
        _need(bool(declarations), 'explicit_trace_without_raw_selection')
        for row in declarations:
            raw_ref(row)
    except (ValueError, KeyError, TypeError, OSError, IndexError) as exc:
        result['declared'] = True
        for row in [e for e in declarations if e['event'] == _DECISION] or [anchor]:
            unknown(row, exc)
        return result
    selections = [e for e in declarations if e['event'] == _SELECTION]
    proofs = []
    for selection in selections:
        try:
            _one([e for e in selections if _same(e, selection)], 'worker_selection_duplicate')
            origin, mapping, health = _worker_proof(selection, rows, manifest, peer, peer_reference, raw_ref)
            proofs.append((origin, mapping, health, selection))
        except (ValueError, KeyError, TypeError, OSError, IndexError, AttributeError) as exc:
            unknown(selection, exc)
    for origin in [e for e in rows if e.get('event') == 'endpoint_created']:
        if not any(_same(origin, s) for s in selections):
            unknown(origin, 'native_filter_decision:worker_trace_selection_missing')
    for health in [e for e in declarations if e['event'] == _HEALTH]:
        if not any(_same(health, s) for s in selections):
            unknown(health, 'native_filter_decision:health_worker_selection_missing')
    for row in declarations:
        if row['event'] != _DECISION:
            continue
        try:
            proof = _one([p for p in proofs if _same(row, p[0])], 'decision_worker_proof_unavailable')
            result['observations'].append(_observation(row, rows, program, proof, manifest, raw_ref))
        except (ValueError, KeyError, TypeError, OSError, IndexError, AttributeError, UnicodeError) as exc:
            unknown(row, exc)
    return result
