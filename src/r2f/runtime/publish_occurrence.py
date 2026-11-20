"""Pure raw consumer for explicitly scoped RMW publish occurrences.

No ROS calls, inferred sequence offsets, timestamp tolerances, body-uniqueness
fallback, or effect/candidate decisions. A receipt asserts nothing without its
independent begin/trace/end/send rows. Indices refer to the supplied full raw
capture, not a filtered or renumbered subset.

Raw provider contract follows rmw_publish_observer: begin/end authenticate both
publisher handles and observed_writer_gid. Trace has scope, RMW handle and thread
IDs but no rcl handle/identity_api; its trace_ref is a provider reference, not a
raw_event_ref. Send nests the v1 receipt; no top-level handle copies are needed.

Receiver provenance follows rcl_take_bridge: rcl_take_receipt and the full
message_info_receipt.publisher_gid copied by the same successful public take.
pair adds explicit control/control_generation/control_gid to the usual keys.
"""
import hashlib
import re

from .endpoint_incarnation import endpoint_identity_at
from .message_info_receipt import SCHEMA as MESSAGE_INFO_SCHEMA, UINT64_MAX

SCHEMA = 'r2f.rmw-publish-occurrence/v1'
RMW = 'rmw_fastrtps_cpp'
IDENTITY_API = 'rcl_publisher_get_rmw_handle+rmw_get_gid_for_publisher'
TRACE_API = 'ros2:rmw_publish'
_LINEAGE = ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')
_BINDING = _LINEAGE + ('pgid', 'run_id', 'domain_id', 'monotonic_clock_identity',
                       'rmw_identifier', 'node_name', 'role', 'topic', 'message_type')
_SCOPE = ('call_id', 'command_id', 'send_sequence')
_COMMON = _SCOPE + ('payload_sha256', 'writer_gid', 'endpoint_gid', 'rmw_identifier')
_CALL = _COMMON + ('publisher_rmw_handle',
                 'rcl_publisher_handle', 'thread_id', 'native_thread_id')
_RETIRED = {'endpoint_destroyed', 'topology_endpoint_destroyed', 'worker_exit',
            'topology_worker_exit', 'topology_generation_retired'}


class _Unknown(ValueError):
    pass


def _need(ok, reason):
    if not ok:
        raise _Unknown(reason)


def _positive(value):
    return type(value) is int and value > 0


def _gid(value):
    return (isinstance(value, str) and re.fullmatch('[0-9a-f]{32}', value) is not None
            and value[:24] != '0' * 24 and value[24:] != '0' * 8)


def _stamp(value):
    return _positive(value) and value < UINT64_MAX


def _body(row):
    data = row.get('string_data')
    digest = row.get('payload_sha256')
    _need(isinstance(data, str) and isinstance(digest, str)
          and hashlib.sha256(data.encode()).hexdigest() == digest,
          'complete_string_data_hash_invalid')
    return data, digest


def _identity(row, eid, generation, gid, events):
    _need(_gid(gid) and type(generation) is int and generation > 0,
          'endpoint_identity_invalid')
    created = [e for e in events if e.get('event') == 'endpoint_created'
               and e.get('logical_id') == eid and e.get('generation') == generation]
    _need(len(created) == 1, 'endpoint_creation_missing_or_ambiguous')
    origin = created[0]
    _need(not any(e.get('event') == 'endpoint_created' and e is not origin
                  and e.get('endpoint_gid') == gid
                  and e.get('run_id') == origin.get('run_id')
                  and e.get('domain_id') == origin.get('domain_id') for e in events),
          'endpoint_gid_creation_alias')
    _need(all(origin.get(k) is not None and row.get(k) == origin[k] for k in _BINDING),
          'worker_run_domain_clock_identity_mismatch')
    _need(type(row.get('monotonic_ns')) is int and bool(row.get('monotonic_clock_identity')),
          'worker_clock_missing')
    current = endpoint_identity_at(events, origin, before_index=row['event_index'] + 1)
    _need(current.get('endpoint_gid') == gid
          and row['event_index'] > current['identity_valid_after_ref']
          and row['monotonic_ns'] >= current['identity_valid_after_ns'],
          'endpoint_incarnation_not_current')
    _need(not any(e.get('event') in _RETIRED and e.get('logical_id') == eid
                  and e.get('generation') == generation
                  and (e['event_index'] < row['event_index'] or
                       (type(e.get('monotonic_ns')) is int
                        and e['monotonic_ns'] < row['monotonic_ns'])) for e in events),
          'endpoint_generation_retired')
    return current


def _metadata(row):
    info = row.get('message_info_receipt') or {}
    _need(info.get('schema') == MESSAGE_INFO_SCHEMA, 'message_info_receipt_missing')
    fields = info.get('fields') or {}
    values = []
    for name in ('source_timestamp', 'publication_sequence_number'):
        field = fields.get(name) or {}
        _need(field.get('status') == 'observed' and _stamp(field.get('value')),
              'message_info_' + name + '_unavailable')
        values.append(field['value'])
    return tuple(values)


def _direct(row, gid):
    _need(_gid(row.get('publisher_gid')) and row['publisher_gid'][-2:] in ('02', '03')
          and row['publisher_gid'] == gid
          and row.get('publisher_gid_status') == 'available'
          and row.get('identity_basis') == 'message_info_gid',
          'direct_publisher_gid_missing_or_mismatch')
    info = row.get('message_info_receipt') or {}
    provenance = info.get('publisher_gid') or {}
    take = row.get('rcl_take_receipt') or {}
    _need(provenance.get('status') == 'available' and provenance.get('value') == gid
          and provenance.get('raw_value') == gid
          and type(provenance.get('storage_size')) is int and provenance['storage_size'] == 16
          and provenance.get('implementation_identifier') == RMW
          and info.get('api') == 'public rcl_take / rmw_message_info_t (ctypes ABI-pinned)'
          and info.get('from_intra_process') is False,
          'direct_publisher_gid_provenance_unavailable')
    _need(take.get('schema') == 'r2f.rcl-take-receipt/v1'
          and take.get('api') == 'public rcl_take'
          and take.get('observation_basis') == 'single_take_owner_public_rcl_api'
          and take.get('pointer_scope') == 'borrowed_fresh_inside_current_subscription_handle'
          and take.get('rmw_identifier') == RMW
          and type(take.get('return_code')) is int and take['return_code'] == 0
          and take.get('message_type') == 'std_msgs/msg/String' and take.get('raw') is False
          and isinstance(take.get('bridge_id'), str) and bool(take['bridge_id'])
          and _positive(take.get('take_index'))
          and isinstance(take.get('rcl_subscription_pointer'), str)
          and re.fullmatch('0x[0-9a-f]+', take['rcl_subscription_pointer']) is not None
          and int(take['rcl_subscription_pointer'], 16) > 0
          and isinstance(take.get('abi_pin_sha256'), str)
          and re.fullmatch('[0-9a-f]{64}', take['abi_pin_sha256']) is not None
          and take.get('message_info_receipt') == info,
          'rcl_take_provenance_unavailable')
    begin, end = take.get('take_begin_monotonic_ns'), take.get('take_return_monotonic_ns')
    _need(type(begin) is int and type(end) is int and type(row.get('monotonic_ns')) is int
          and 0 <= begin <= end <= row['monotonic_ns'], 'rcl_take_chronology_invalid')


def _identity_refs(identity):
    return dict(creation_ref=identity['event_index'],
        transition_ref=identity['identity_transition_ref'],
        ack_ref=(identity['identity_valid_after_ref']
                 if identity['identity_transition_ref'] is not None else None),
        valid_after_ref=identity['identity_valid_after_ref'],
        valid_after_monotonic_ns=identity['identity_valid_after_ns'],
        incarnation_ordinal=identity['incarnation_ordinal'],
        logical_id=identity['logical_id'], generation=identity['generation'],
        endpoint_generation=identity['endpoint_generation'], endpoint_gid=identity['endpoint_gid'],
        pid=identity['pid'], process_start_ticks=identity['process_start_ticks'])


def _receiver(row, eid, generation, gid, events):
    identity = _identity(row, eid, generation, gid, events)
    from .topology_worker import worker_role_kind
    _need(worker_role_kind(identity.get('role')) == 'reader', 'receive_endpoint_not_reader')
    _need(row.get('reader_gid') == gid, 'receive_reader_gid_mismatch')
    take = row['rcl_take_receipt']
    _need(take['take_begin_monotonic_ns'] >= identity['identity_valid_after_ns'],
          'take_precedes_current_reader_incarnation')
    aliases = [e for e in events if e.get('event') == 'receive'
               and all(e.get(k) == row.get(k) for k in _LINEAGE)
               and (e.get('rcl_take_receipt') or {}).get('bridge_id') == take['bridge_id']
               and (e.get('rcl_take_receipt') or {}).get('take_index') == take['take_index']]
    _need(len(aliases) == 1 and aliases[0] is row, 'single_take_receipt_reused')
    return identity


def _source(send, writer, events):
    receipt = send.get('publish_occurrence_receipt') or {}
    _need(receipt.get('schema') == SCHEMA and receipt.get('status') == 'observed'
          and receipt.get('reason') is None, 'publish_occurrence_receipt_unavailable')
    _need(receipt.get('rmw_identifier') == RMW and writer.get('rmw_identifier') == RMW
          and receipt.get('identity_api') == IDENTITY_API and receipt.get('trace_api') == TRACE_API,
          'publish_occurrence_source_unsupported')
    _need(send.get('publish_returned') is True and receipt.get('publish_returned') is True,
          'publish_did_not_return_successfully')
    _need(all(receipt.get(k) is not None and receipt[k] == send.get(k) for k in
              ('command_id', 'send_sequence', 'payload_sha256', 'writer_gid'))
          and all(k not in send or send[k] == receipt.get(k) for k in _CALL),
          'publish_occurrence_scope_mismatch')
    _need(isinstance(receipt.get('call_id'), str) and bool(receipt['call_id'])
          and (isinstance(receipt.get('command_id'), str) and bool(receipt['command_id'])
               or _positive(receipt.get('command_id'))) and type(receipt.get('send_sequence')) is int
          and receipt['send_sequence'] >= 0
          and all(_positive(receipt.get(k)) for k in ('publisher_rmw_handle',
              'rcl_publisher_handle', 'thread_id', 'native_thread_id', 'trace_message_handle'))
          and _stamp(receipt.get('source_timestamp'))
          and type(receipt.get('trace_count')) is int and receipt['trace_count'] == 1
          and receipt.get('trace_scope_matched') is True
          and receipt.get('observed_writer_gid') == writer['endpoint_gid']
          and receipt.get('endpoint_gid') == writer['endpoint_gid'],
          'publish_occurrence_scope_or_trace_invalid')
    trace_ref = receipt.get('trace_ref')
    _need(isinstance(trace_ref, str) and bool(trace_ref), 'publish_trace_ref_missing')
    reused = [e for e in events if e.get('event') == 'send'
              and (e.get('publish_occurrence_receipt') or {}).get('trace_ref') == trace_ref]
    _need(len(reused) == 1 and reused[0] is send, 'publish_trace_reused')
    call_rows = [e for e in events if (e.get('publish_occurrence_receipt') or {}).get('call_id',
                                     e.get('call_id')) == receipt['call_id']
                 and all(e.get(k) == writer.get(k) for k in _LINEAGE)
                 and e.get('event') in {'publish_call_begin', 'rmw_publish_trace',
                                       'publish_call_end', 'send'}]
    chain = []
    for kind in ('publish_call_begin', 'rmw_publish_trace', 'publish_call_end', 'send'):
        found = [e for e in call_rows if e.get('event') == kind]
        _need(len(found) == 1, 'publish_scope_raw_' + kind + '_missing_or_ambiguous')
        row = found[0]
        fields = (_COMMON + ('publisher_rmw_handle', 'thread_id', 'native_thread_id')
                  if kind == 'rmw_publish_trace' else _CALL)
        if kind != 'send':
            _need(all(row.get(k) == receipt[k] for k in fields), 'publish_scope_raw_binding_mismatch')
        if kind in ('publish_call_begin', 'publish_call_end'):
            _need(row.get('observed_writer_gid') == writer['endpoint_gid']
                  and row.get('identity_api') == IDENTITY_API, 'publish_raw_writer_identity_unavailable')
        _identity(row, writer['logical_id'], writer['generation'], writer['endpoint_gid'], events)
        chain.append(row)
    begin, trace, end, final = chain
    _need(final is send and all(a['event_index'] < b['event_index']
          and a['monotonic_ns'] <= b['monotonic_ns'] for a, b in zip(chain, chain[1:])),
          'publish_scope_raw_chronology_invalid')
    _need(end.get('publish_returned') is True and end.get('error_type') is None
          and type(end.get('trace_count')) is int and end['trace_count'] == 1,
          'publish_call_end_failed')
    _need(trace.get('trace_ref') == trace_ref
          and len([e for e in events if e.get('event') == 'rmw_publish_trace'
                   and e.get('trace_ref') == trace_ref]) == 1
          and trace.get('source_timestamp') == receipt['source_timestamp']
          and trace.get('trace_message_handle') == receipt['trace_message_handle']
          and trace.get('trace_scope_matched') is True and trace.get('trace_api') == TRACE_API,
          'publish_trace_raw_mismatch')
    for row, field in ((begin, 'call_begin_monotonic_ns'),
                       (trace, 'trace_monotonic_ns'), (end, 'call_return_monotonic_ns')):
        _need(type(receipt.get(field)) is int and row['monotonic_ns'] == receipt[field],
              'publish_scope_receipt_clock_mismatch')
    # If captured on creation, handles must also match that independent token.
    for field in ('publisher_rmw_handle', 'rcl_publisher_handle'):
        _need(field not in writer or writer[field] == receipt[field], 'publisher_creation_handle_mismatch')
    return receipt, begin, trace, end


def producer_occurrence_witness(send, events):
    """Authenticate one source scope and timestamp uniqueness, without any take."""
    try:
        _canonical(events)
        _need(send.get('event') == 'send' and type(send.get('event_index')) is int
              and 0 <= send['event_index'] < len(events)
              and events[send['event_index']] is send, 'send_not_in_raw_capture')
        gid = send.get('writer_gid')
        writer = _identity(send, send.get('logical_id'), send.get('generation'), gid, events)
        from .topology_worker import worker_role_kind
        _need(worker_role_kind(writer.get('role')) == 'writer', 'source_not_writer')
        receipt_stamp = (send.get('publish_occurrence_receipt') or {}).get('source_timestamp')
        return _producer(send, events, writer, gid, receipt_stamp), None
    except (ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
        return None, str(exc) if isinstance(exc, ValueError) else 'malformed_occurrence_raw_contract'


def _canonical(events):
    _need(isinstance(events, list) and all(type(e.get('event_index')) is int
          and e['event_index'] == i for i, e in enumerate(events)), 'raw_event_indices_not_canonical')


def _producer(send, events, writer, gid, receipt_stamp):
    sends = [e for e in events if e.get('event') == 'send'
             and e.get('logical_id') == writer['logical_id']
             and e.get('generation') == writer['generation']
             and e.get('publish_returned') is True]
    timestamps = {}
    for sent in sends:
        _identity(sent, writer['logical_id'], writer['generation'], gid, events)
        _need(sent.get('writer_gid') == gid, 'send_writer_gid_mismatch')
        value = (sent.get('publish_occurrence_receipt') or {}).get('source_timestamp')
        _need(_stamp(value), 'writer_success_send_timestamp_unavailable')
        _need(value not in timestamps, 'writer_source_timestamp_not_unique')
        timestamps[value] = sent
    _need(timestamps.get(receipt_stamp) is send, 'source_timestamp_has_no_successful_send')
    receipt, begin, trace, end = _source(send, writer, events)
    _body(send)
    return dict(send_ref=send['event_index'], payload_sha256=send['payload_sha256'],
        writer=writer['logical_id'], writer_generation=writer['generation'], writer_gid=gid,
        writer_identity=_identity_refs(writer),
        source_timestamp=receipt['source_timestamp'], send_sequence=send['send_sequence'],
        command_id=receipt['command_id'], call_id=receipt['call_id'], trace_ref=receipt['trace_ref'],
        scope_refs=dict(begin_ref=begin['event_index'], trace_ref=trace['event_index'],
                        end_ref=end['event_index']))


def publish_occurrence_witness(pair, receive, events):
    """Return (raw occurrence witness, None), or (None, specific unknown reason).

    r0 is joined to an explicitly named control reader on authenticated writer
    GID + exact source timestamp + complete String.data hash; pubseq must agree.
    This is independently scoped occurrence evidence, never filtering proof.
    No receipt failure falls back to the ambiguous payload-only consumer.
    """
    try:
        return _witness(pair, receive, events), None
    except (ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
        return None, str(exc) if isinstance(exc, ValueError) else 'malformed_occurrence_raw_contract'


def _witness(pair, receive, events):
    _canonical(events)
    _need(receive.get('event') == 'receive' and type(receive.get('event_index')) is int
          and 0 <= receive['event_index'] < len(events)
          and events[receive['event_index']] is receive, 'receive_not_in_raw_capture')
    gid = pair.get('writer_gid')
    _need(_gid(gid), 'writer_gid_invalid')
    _direct(receive, gid)
    stamp, pubseq = _metadata(receive)
    reader_identity = _receiver(receive, pair.get('reader'), pair.get('reader_generation'),
                                pair.get('reader_gid'), events)
    _need(receive.get('reader_gid') == pair.get('reader_gid'), 'receive_reader_gid_mismatch')
    data, digest = _body(receive)
    writers = [e for e in events if e.get('event') == 'endpoint_created'
               and e.get('logical_id') == pair.get('writer')
               and e.get('generation') == pair.get('writer_generation')]
    _need(len(writers) == 1 and writers[0].get('endpoint_gid') == gid,
          'writer_creation_missing_or_mismatch')
    writer = writers[0]
    from .topology_worker import worker_role_kind
    _need(worker_role_kind(writer.get('role')) == 'writer' and all(receive.get(k) == writer.get(k)
          and writer.get(k) is not None for k in ('run_id', 'domain_id',
              'monotonic_clock_identity', 'rmw_identifier', 'topic', 'message_type')),
          'receive_writer_context_mismatch')
    matches = [e for e in events if e.get('event') == 'send'
               and e.get('logical_id') == pair.get('writer')
               and e.get('generation') == pair.get('writer_generation')
               and (e.get('publish_occurrence_receipt') or {}).get('source_timestamp') == stamp]
    _need(len(matches) == 1, 'source_timestamp_send_missing_or_ambiguous')
    send = matches[0]
    producer, reason = producer_occurrence_witness(send, events)
    _need(producer is not None, reason)
    _need(_body(send) == (data, digest), 'source_timestamp_string_data_mismatch')
    controls = []
    for row in events:
        if row.get('event') != 'receive' or row.get('publisher_gid') != gid:
            continue
        # Read all same-writer observations, not just same-body rows. Conflicting
        # sample identities must not disappear through prefiltering by payload.
        other_stamp, other_seq = _metadata(row)
        if other_stamp != stamp and other_seq != pubseq:
            continue
        _direct(row, gid)
        _need(other_stamp == stamp and other_seq == pubseq and _body(row) == (data, digest),
              'writer_timestamp_publication_sequence_or_body_conflict')
        if (row.get('logical_id') == pair.get('control')
                and row.get('generation') == pair.get('control_generation')):
            control_identity = _receiver(row, pair.get('control'), pair.get('control_generation'),
                                         pair.get('control_gid'), events)
            _need(row.get('reader_gid') == pair.get('control_gid'), 'control_reader_gid_mismatch')
            _need(all(row.get(k) == receive.get(k) for k in ('run_id', 'domain_id',
                  'monotonic_clock_identity', 'rmw_identifier', 'topic', 'message_type')),
                  'control_receive_context_mismatch')
            controls.append(row)
    _need(len(controls) == 1, 'control_receive_missing_or_ambiguous')
    return dict(identity_basis='direct_gid_scoped_publish_occurrence',
        send_ref=send['event_index'], receive_ref=receive['event_index'],
        control_ref=controls[0]['event_index'], payload_sha256=digest,
        writer=pair['writer'], writer_generation=pair['writer_generation'], writer_gid=gid,
        reader=pair['reader'], reader_generation=pair['reader_generation'],
        reader_gid=pair['reader_gid'], direct_gid_match=True,
        reader_identity=_identity_refs(reader_identity),
        control_identity=_identity_refs(control_identity),
        writer_identity=producer['writer_identity'],
        source_timestamp=stamp, publication_sequence_number=pubseq,
        send_sequence=producer['send_sequence'], command_id=producer['command_id'],
        call_id=producer['call_id'], trace_ref=producer['trace_ref'],
        scope_refs=producer['scope_refs'])
