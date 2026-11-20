"""Self-produced offline raw contracts: no DDS, tracing provider or filter proof."""
from copy import deepcopy
import hashlib

import pytest

from r2f.runtime.publish_occurrence import publish_occurrence_witness, producer_occurrence_witness


WRITER, READER, CONTROL = '04' * 15 + '03', '01' * 16, '05' * 16


def fixture():
    rows = []
    base = dict(run_id='offline-occurrence', domain_id=191,
                monotonic_clock_identity='fixture-boot-clock',
                rmw_identifier='rmw_fastrtps_cpp', topic='/offline',
                message_type='std_msgs/msg/String')
    def add(event, token, **fields):
        row = dict(token, event=event, **fields)
        i = len(rows)
        row.update(event_index=i, monotonic_ns=100 + i,
                   raw_event_ref=f'offline://occurrence#event={i}')
        rows.append(row)
        return row
    tokens = []
    for i, (logical, role, gid) in enumerate([
            ('w0', 'writer', WRITER), ('r0', 'reader', READER),
            ('r1', 'reader2', CONTROL)]):
        tokens.append(add('endpoint_created', dict(base, logical_id=logical, role=role,
            generation=1, endpoint_gid=gid, pid=200 + i, pgid=200 + i,
            process_start_ticks=10 + i, endpoint_generation=f'offline-worker-{i}',
            node_name=logical)))
    writer, reader, control = tokens
    # Repeated identical complete String.data; body sequence is deliberately NOT
    # this publish's sequence. Independent timestamps/pubseq have no seq offset.
    text = '{"send_sequence":0,"payload":"identical"}'
    digest = hashlib.sha256(text.encode()).hexdigest()
    for sequence, stamp, pubseq in [(0, 7001, 41), (2, 9003, 88), (4, 12009, 102)]:
        common = dict(command_id=10 + sequence, send_sequence=sequence,
            call_id=f'publish-{sequence}', payload_sha256=digest, writer_gid=WRITER,
            publisher_rmw_handle=1001, rcl_publisher_handle=1002,
            thread_id=300, native_thread_id=301)
        common.update(endpoint_gid=WRITER, rmw_identifier='rmw_fastrtps_cpp')
        raw_identity = dict(observed_writer_gid=WRITER,
            identity_api='rcl_publisher_get_rmw_handle+rmw_get_gid_for_publisher')
        begin = add('publish_call_begin', writer, **common, **raw_identity)
        trace = add('rmw_publish_trace', writer, **common, source_timestamp=stamp,
            trace_message_handle=1003, trace_scope_matched=True,
            trace_api='ros2:rmw_publish')
        trace['trace_ref'] = f'publish-{sequence}.trace.0'
        # Real producer trace does NOT duplicate the rcl handle or identity API.
        trace.pop('rcl_publisher_handle')
        end = add('publish_call_end', writer, **common, **raw_identity,
                  publish_returned=True, error_type=None, trace_count=1)
        receipt = dict(common, **raw_identity, schema='r2f.rmw-publish-occurrence/v1', status='observed',
            reason=None, rmw_identifier='rmw_fastrtps_cpp', source_timestamp=stamp,
            publish_returned=True, call_begin_monotonic_ns=begin['monotonic_ns'],
            call_return_monotonic_ns=end['monotonic_ns'], trace_ref=trace['trace_ref'],
            trace_count=1, trace_monotonic_ns=trace['monotonic_ns'],
            trace_message_handle=1003, trace_scope_matched=True,
            trace_api='ros2:rmw_publish')
        send_common = {k: common[k] for k in ('command_id', 'send_sequence',
                                             'writer_gid', 'payload_sha256')}
        add('send', writer, **send_common, string_data=text, publish_returned=True,
            publish_occurrence_receipt=receipt)
        for token in (control, reader):
            receive = add('receive', token, reader_gid=token['endpoint_gid'],
                publisher_gid=WRITER, publisher_gid_status='available',
                identity_basis='message_info_gid', string_data=text, payload_sha256=digest,
                send_sequence=0,
                message_info_receipt=dict(schema='r2f.message-info-fields/v1', fields={
                    k: dict(status='observed', value=v) for k, v in
                    [('source_timestamp', stamp), ('received_timestamp', stamp + 1),
                     ('publication_sequence_number', pubseq), ('reception_sequence_number', 7)]}))
            info = receive['message_info_receipt']
            info.update(api='public rcl_take / rmw_message_info_t (ctypes ABI-pinned)',
                from_intra_process=False, publisher_gid=dict(status='available', value=WRITER, raw_value=WRITER,
                    storage_size=16, implementation_identifier='rmw_fastrtps_cpp'))
            receive['rcl_take_receipt'] = dict(schema='r2f.rcl-take-receipt/v1',
                api='public rcl_take', observation_basis='single_take_owner_public_rcl_api',
                bridge_id=f"offline-bridge-{token['logical_id']}", take_index=sequence + 1,
                rcl_subscription_pointer='0xff',
                pointer_scope='borrowed_fresh_inside_current_subscription_handle',
                rmw_identifier='rmw_fastrtps_cpp', return_code=0,
                message_type='std_msgs/msg/String', raw=False,
                take_begin_monotonic_ns=receive['monotonic_ns'] - 2,
                take_return_monotonic_ns=receive['monotonic_ns'] - 1,
                abi_pin_sha256='a' * 64, message_info_receipt=deepcopy(info))
    pair = dict(writer='w0', writer_generation=1, writer_gid=WRITER,
                reader='r0', reader_generation=1, reader_gid=READER,
                control='r1', control_generation=1, control_gid=CONTROL)
    return rows, pair


def test_repeat_body_joins_exact_timestamp_and_control_pubseq_without_seq_offset():
    rows, pair = fixture()
    original = deepcopy(rows)
    receives = [r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r0']
    for receive, send_ref, control_ref, sequence, stamp, pubseq in zip(
            receives, [6, 12, 18], [7, 13, 19], [0, 2, 4], [7001, 9003, 12009], [41, 88, 102]):
        witness, reason = publish_occurrence_witness(pair, receive, rows)
        assert reason is None, reason
        assert witness['send_ref'] == send_ref
        assert witness['control_ref'] == control_ref
        assert witness['source_timestamp'] == stamp
        assert witness['publication_sequence_number'] == pubseq
        assert witness['send_sequence'] == sequence
        assert witness['direct_gid_match'] is True
        assert witness['call_id'] == f'publish-{sequence}'
        assert witness['trace_ref'] == rows[send_ref - 2]['trace_ref']
        assert 'filter_effect_observed' not in witness and 'candidate_eligible' not in witness
    assert rows == original


def target(rows):
    return next(r for r in rows if r['event'] == 'receive' and r['logical_id'] == 'r0')


def sync_info(receive):
    receive['rcl_take_receipt']['message_info_receipt'] = deepcopy(receive['message_info_receipt'])


@pytest.mark.parametrize('field,value', [
    ('schema', 'other'), ('status', 'unknown'), ('reason', 'trace_lost'),
    ('rmw_identifier', 'rmw_cyclonedds_cpp'), ('identity_api', 'inferred_from_payload'),
    ('trace_api', 'other'), ('call_id', 'wrong'), ('command_id', 'wrong-command'),
    ('send_sequence', 123), ('payload_sha256', 'f' * 64), ('writer_gid', '00' * 16),
    ('endpoint_gid', '06' * 16), ('observed_writer_gid', '06' * 16),
    ('publisher_rmw_handle', 0), ('rcl_publisher_handle', False),
    ('thread_id', 0), ('native_thread_id', 0), ('source_timestamp', 0),
    ('trace_count', 0), ('trace_count', 2), ('trace_count', True),
    ('trace_scope_matched', False), ('trace_message_handle', 0),
    ('trace_ref', 'absent.trace'), ('trace_monotonic_ns', 999),
    ('call_begin_monotonic_ns', 999), ('call_return_monotonic_ns', 999),
    ('publish_returned', False),
])
def test_producer_receipt_is_not_its_own_proof(field, value):
    rows, _ = fixture()
    send = rows[6]
    send['publish_occurrence_receipt'][field] = value
    witness, reason = producer_occurrence_witness(send, rows)
    assert witness is None and reason


@pytest.mark.parametrize('index,field,value', [
    (3, 'event', 'offline_removed_begin'), (4, 'event', 'offline_removed_trace'),
    (5, 'event', 'offline_removed_end'),
    (3, 'command_id', 999), (4, 'send_sequence', 999), (5, 'call_id', 'foreign'),
    (3, 'observed_writer_gid', '07' * 16), (5, 'identity_api', 'graph'),
    (3, 'rcl_publisher_handle', 111), (4, 'publisher_rmw_handle', 111),
    (5, 'thread_id', 111), (4, 'native_thread_id', 111),
    (4, 'trace_ref', 'foreign.trace'), (4, 'trace_scope_matched', False),
    (4, 'source_timestamp', 7002), (4, 'trace_message_handle', 1010),
    (4, 'trace_api', 'other'), (5, 'trace_count', 2),
    (5, 'publish_returned', False), (5, 'error_type', 'WriteError'),
    (4, 'pid', 999), (3, 'run_id', 'foreign'), (4, 'domain_id', 192),
    (5, 'monotonic_clock_identity', 'foreign'), (4, 'endpoint_generation', 'foreign'),
    (4, 'monotonic_ns', 999), (5, 'monotonic_ns', 101),
    (6, 'publish_returned', False), (6, 'payload_sha256', 'a' * 64),
    (6, 'string_data', 'not the original data'), (6, 'writer_gid', '00' * 16),
])
def test_missing_or_foreign_scope_raw_fails_closed(index, field, value):
    rows, pair = fixture()
    rows[index][field] = value
    assert producer_occurrence_witness(rows[6], rows)[0] is None
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


def test_producer_timestamp_collision_checks_all_success_sends_not_same_body():
    rows, pair = fixture()
    other = rows[12]
    other['string_data'] = 'different complete String.data'
    other['payload_sha256'] = hashlib.sha256(other['string_data'].encode()).hexdigest()
    other['publish_occurrence_receipt']['source_timestamp'] = 7001
    witness, reason = producer_occurrence_witness(rows[6], rows)
    assert witness is None and reason == 'writer_source_timestamp_not_unique'
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


def test_success_send_without_stamp_prevents_claim_of_unique_timestamp():
    rows, _ = fixture()
    del rows[12]['publish_occurrence_receipt']
    assert producer_occurrence_witness(rows[6], rows) == (
        None, 'writer_success_send_timestamp_unavailable')


@pytest.mark.parametrize('field,value', [('source_timestamp', 0),
    ('source_timestamp', None), ('source_timestamp', 7002),
    ('publication_sequence_number', 0), ('publication_sequence_number', (1 << 64) - 1),
    ('publication_sequence_number', True), ('publication_sequence_number', 42)])
def test_exact_receiver_metadata_not_nearest_time_or_sequence(field, value):
    rows, pair = fixture()
    receive = target(rows)
    receive['message_info_receipt']['fields'][field]['value'] = value
    sync_info(receive)
    assert publish_occurrence_witness(pair, receive, rows)[0] is None


@pytest.mark.parametrize('status', ['unavailable_api', 'unsupported_rmw', 'read_error'])
def test_receiver_observed_status_required(status):
    rows, pair = fixture()
    receive = target(rows)
    receive['message_info_receipt']['fields']['publication_sequence_number']['status'] = status
    sync_info(receive)
    assert publish_occurrence_witness(pair, receive, rows)[0] is None


@pytest.mark.parametrize('field,value', [('publisher_gid', None), ('publisher_gid', ''),
    ('publisher_gid', '00' * 16), ('publisher_gid', 'invalid'),
    ('publisher_gid', '06' * 16), ('publisher_gid_status', 'unavailable_api'),
    ('identity_basis', 'endpoint_correlated'), ('reader_gid', '06' * 16),
    ('pid', 999), ('endpoint_generation', 'foreign'), ('run_id', 'foreign'),
    ('domain_id', 192), ('monotonic_clock_identity', 'foreign'),
    ('payload_sha256', 'a' * 64), ('string_data', 'tampered')])
def test_receiver_requires_current_identity_and_direct_not_payload_gid(field, value):
    rows, pair = fixture()
    receive = target(rows)
    receive[field] = value
    assert publish_occurrence_witness(pair, receive, rows)[0] is None


@pytest.mark.parametrize('path,value', [
    (('rcl_take_receipt',), None),
    (('rcl_take_receipt', 'schema'), 'foreign'),
    (('rcl_take_receipt', 'return_code'), 401),
    (('rcl_take_receipt', 'observation_basis'), 'sidecar_take'),
    (('rcl_take_receipt', 'pointer_scope'), 'cached_pointer'),
    (('rcl_take_receipt', 'take_index'), 0),
    (('rcl_take_receipt', 'rcl_subscription_pointer'), '0x0'),
    (('rcl_take_receipt', 'abi_pin_sha256'), ''),
    (('rcl_take_receipt', 'take_return_monotonic_ns'), 9999),
    (('message_info_receipt', 'publisher_gid', 'value'), '06' * 16),
    (('message_info_receipt', 'publisher_gid', 'status'), 'unavailable_api'),
    (('message_info_receipt', 'publisher_gid', 'storage_size'), 24),
    (('message_info_receipt', 'publisher_gid', 'implementation_identifier'), 'foreign'),
    (('message_info_receipt', 'from_intra_process'), True),
])
def test_bridge_full_gid_provenance_required(path, value):
    rows, pair = fixture()
    receive = target(rows)
    obj = receive
    for key in path[:-1]:
        obj = obj[key]
    obj[path[-1]] = value
    if path[0] == 'message_info_receipt':
        sync_info(receive)
    assert publish_occurrence_witness(pair, receive, rows)[0] is None


def test_control_pubseq_disagreement_is_not_a_rejection_effect():
    rows, pair = fixture()
    control = rows[7]
    control['message_info_receipt']['fields']['publication_sequence_number']['value'] = 42
    sync_info(control)
    witness, reason = publish_occurrence_witness(pair, target(rows), rows)
    assert witness is None and reason == 'writer_timestamp_publication_sequence_or_body_conflict'


def test_same_writer_pubseq_cannot_identify_two_timestamps():
    rows, pair = fixture()
    rows[13]['message_info_receipt']['fields']['publication_sequence_number']['value'] = 41
    sync_info(rows[13])
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


@pytest.mark.parametrize('field,value', [('pid', 999), ('reader_gid', READER),
    ('generation', 2), ('publisher_gid', '06' * 16)])
def test_control_cannot_borrow_other_endpoint_or_writer(field, value):
    rows, pair = fixture()
    rows[7][field] = value
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


def test_cannot_guess_control_from_reader_order():
    rows, pair = fixture()
    for key in ('control', 'control_generation', 'control_gid'):
        pair.pop(key)
    assert publish_occurrence_witness(pair, target(rows), rows) == (
        None, 'control_receive_missing_or_ambiguous')


@pytest.mark.parametrize('index', [3, 4, 5, 6, 7])
def test_duplicate_scope_trace_send_or_control_is_ambiguous(index):
    rows, pair = fixture()
    duplicate = deepcopy(rows[index])
    duplicate.update(event_index=len(rows), monotonic_ns=999)
    rows.append(duplicate)
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


def test_one_trace_cannot_be_reused_by_another_send_even_unknown_receipt():
    rows, _ = fixture()
    rows[12]['publish_occurrence_receipt'].update(status='unknown',
        trace_ref=rows[6]['publish_occurrence_receipt']['trace_ref'])
    assert producer_occurrence_witness(rows[6], rows) == (None, 'publish_trace_reused')


def test_no_legacy_fallback_after_present_but_failed_new_receipt():
    rows, pair = fixture()
    rows[6]['publish_occurrence_receipt']['status'] = 'unknown'
    witness, reason = publish_occurrence_witness(pair, target(rows), rows)
    assert witness is None and reason == 'publish_occurrence_receipt_unavailable'


def test_foreign_publisher_same_pubseq_cannot_poison_authenticated_writer():
    rows, pair = fixture()
    foreign = deepcopy(rows[7])
    foreign.update(event_index=len(rows), publisher_gid='09' * 16)
    foreign['rcl_take_receipt']['take_index'] = 999
    rows.append(foreign)
    assert publish_occurrence_witness(pair, target(rows), rows)[0]['send_ref'] == 6


def test_input_objects_must_be_from_canonical_raw_not_reconstructed_copy():
    rows, pair = fixture()
    assert producer_occurrence_witness(deepcopy(rows[6]), rows)[0] is None
    assert publish_occurrence_witness(pair, deepcopy(target(rows)), rows)[0] is None
    rows[4]['event_index'] = 99
    assert producer_occurrence_witness(rows[6], rows) == (None, 'raw_event_indices_not_canonical')


def reindex(rows):
    """Only self-produced in-memory rows; no persisted evidence is edited."""
    for i, row in enumerate(rows):
        row.update(event_index=i, monotonic_ns=100 + i)
        if row['event'] == 'content_filter_api_call':
            row['api_monotonic_ns'] = row['monotonic_ns']
        if row['event'] == 'receive':
            row['rcl_take_receipt'].update(take_begin_monotonic_ns=100 + i - 1,
                                          take_return_monotonic_ns=100 + i)
    for row in rows:
        if row['event'] == 'send':
            receipt = row['publish_occurrence_receipt']
            scope = [e for e in rows if e.get('call_id') == receipt['call_id']]
            receipt.update(call_begin_monotonic_ns=scope[0]['monotonic_ns'],
                trace_monotonic_ns=scope[1]['monotonic_ns'],
                call_return_monotonic_ns=scope[2]['monotonic_ns'])
    for row in rows:
        if row['event'] == 'content_filter_dynamic_update':
            row.update(update_started_ns=next(e['monotonic_ns'] for e in rows
                if e['event'] == 'endpoint_identity_snapshot' and e['phase'] == 'before'),
                update_finished_ns=row['monotonic_ns'])


def test_reader_transition_has_fresh_refs_and_cannot_rebind_old_callback():
    from test_endpoint_incarnation import trace, NEW
    rows, pair = fixture()
    origin = rows[1]
    protocol = trace()[1:]
    for i, row in enumerate(protocol):
        for key, value in origin.items():
            if key not in ('event', 'event_index', 'monotonic_ns', 'raw_event_ref', 'endpoint_gid'):
                row[key] = deepcopy(value)
        row.update(raw_event_ref=f'offline://transition/{i}', topic=origin['topic'])
        for endpoint in row.get('subscriptions', []):
            endpoint.update(node_name=origin['node_name'], topic_type=origin['message_type'])
    # First receive remains OLD. Only subsequent r0 callbacks see NEW.
    rows[14:14] = protocol
    for row in rows[14 + len(protocol):]:
        if row['event'] == 'receive' and row['logical_id'] == 'r0':
            row.update(reader_gid=NEW, endpoint_gid=NEW)
    reindex(rows)
    receive = next(e for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0'
                   and e['reader_gid'] == NEW)
    pair['reader_gid'] = NEW
    witness, reason = publish_occurrence_witness(pair, receive, rows)
    assert witness is not None, reason
    identity = witness['reader_identity']
    assert identity['creation_ref'] == 1
    assert identity['incarnation_ordinal'] == 1
    assert rows[identity['transition_ref']]['event'] == 'endpoint_identity_transition'
    assert rows[identity['ack_ref']]['command'] == 'update_filter'
    assert witness['publication_sequence_number'] == 88 and witness['source_timestamp'] == 9003
    pair['reader_gid'] = READER
    receive['reader_gid'] = READER
    assert publish_occurrence_witness(pair, receive, rows)[0] is None
    # A NEW callback cannot inherit an unacknowledged transition either.
    pair['reader_gid'] = NEW
    receive['reader_gid'] = NEW
    rows[identity['ack_ref']]['event'] = 'offline_removed_ack'
    assert publish_occurrence_witness(pair, receive, rows)[0] is None


def test_control_may_itself_measure_source_without_inventing_second_receive():
    rows, pair = fixture()
    pair.update(reader='r1', reader_generation=1, reader_gid=CONTROL)
    witness, reason = publish_occurrence_witness(pair, rows[7], rows)
    assert witness is not None, reason
    assert witness['control_ref'] == witness['receive_ref'] == 7
    assert witness['reader_identity'] == witness['control_identity']


def test_same_body_different_native_scopes_and_control_identities_remain_separate():
    rows, pair = fixture()
    a, _ = publish_occurrence_witness(pair, rows[8], rows)
    b, _ = publish_occurrence_witness(pair, rows[14], rows)
    assert (a['publication_sequence_number'], b['publication_sequence_number']) == (41, 88)
    assert (a['source_timestamp'], b['source_timestamp']) == (7001, 9003)
    assert a['reader_identity'] == b['reader_identity']
    assert a['reader_identity']['creation_ref'] != a['control_identity']['creation_ref']
    assert a['scope_refs'] != b['scope_refs']


def test_single_take_receipt_cannot_be_reused_even_when_control_is_same_body():
    rows, pair = fixture()
    rows[13]['rcl_take_receipt']['take_index'] = rows[7]['rcl_take_receipt']['take_index']
    assert publish_occurrence_witness(pair, target(rows), rows)[1] == 'single_take_receipt_reused'


def test_real_observer_and_take_bridge_contracts_join_with_only_native_boundaries_doubled():
    """Run actual Python providers, NOT NativePublishTrace or ROS/DDS calls."""
    from types import SimpleNamespace
    import time
    from test_rcl_take_bridge import Handle, Native, String
    from r2f.runtime.rcl_take_bridge import RclTakeBridge
    from r2f.runtime.rmw_publish_observer import PublishOccurrenceObserver
    rows, pair = fixture()
    rows = rows[:3]
    # Providers use actual monotonic time; endpoint creations precede them.
    for row in rows:
        row['monotonic_ns'] = time.monotonic_ns()
    def emit(event, **fields):
        row = {**rows[0], 'event': event, **fields}
        row.update(event_index=len(rows), raw_event_ref=f'offline://actual-contract/{len(rows)}')
        rows.append(row)
        return row
    class ObserverNative:
        provenance = {'fixture': True, 'execution_evidence': False}
        def __init__(self, callback):
            self.callback = callback
        def publisher_identity(self, pointer):
            return 222, WRITER
        def close(self):
            return 0
    observer = PublishOccurrenceObserver(emit, 'rmw_fastrtps_cpp', native_factory=ObserverNative)
    message = String()
    message.data = 'identical0\x00exact String.data'
    endpoint = SimpleNamespace(handle=Handle(),
        publish=lambda msg: observer.native.callback(222, 333, 123456789))
    receipt = observer.publish(endpoint, message, command_id='actual-command',
                               send_sequence=4, writer_gid=WRITER)
    send = emit('send', command_id='actual-command', send_sequence=4,
        payload_sha256=hashlib.sha256(message.data.encode()).hexdigest(), string_data=message.data,
        writer_gid=WRITER, publish_returned=True, publish_occurrence_receipt=receipt,
        monotonic_ns=time.monotonic_ns())
    assert 'rcl_publisher_handle' not in send
    native_trace = next(e for e in rows if e['event'] == 'rmw_publish_trace')
    assert 'rcl_publisher_handle' not in native_trace
    assert 'observed_writer_gid' not in native_trace
    assert native_trace['trace_ref'] != native_trace['raw_event_ref']
    for origin in (rows[2], rows[1]):
        handle = Handle()
        native = Native(handle)
        native.abi = {'pin_sha256': 'a' * 64}  # Fixture pin, NOT an installed ABI claim.
        native.payload, native.gid, native.pubseq = message.data.encode(), bytes.fromhex(WRITER), 997
        bridge = RclTakeBridge(_bindings=native, _string_type=String)
        sub = SimpleNamespace(handle=handle, msg_type=String, raw=False)
        taken, info = bridge.take(sub)
        row = dict(origin, event='receive', event_index=len(rows),
            monotonic_ns=time.monotonic_ns(), raw_event_ref=f'offline://actual-take/{len(rows)}',
            reader_gid=origin['endpoint_gid'], publisher_gid=info['publisher_gid'].hex(),
            publisher_gid_status=info['publisher_gid_status'], identity_basis=info['identity_basis'],
            rcl_take_receipt=info['rcl_take_receipt'], message_info_receipt=info['message_info_receipt'],
            payload_sha256=hashlib.sha256(taken.data.encode()).hexdigest(), string_data=taken.data,
            send_sequence=0)
        rows.append(row)
        bridge.close()
    assert producer_occurrence_witness(send, rows)[0]['send_sequence'] == 4
    witness, reason = publish_occurrence_witness(pair, rows[-1], rows)
    assert witness is not None, reason
    assert witness['source_timestamp'] == 123456789
    assert witness['publication_sequence_number'] == 997
    assert witness['send_sequence'] == 4 and rows[-1]['send_sequence'] == 0
    observer.close()


@pytest.mark.parametrize('field,value', [('raw_value', '00' * 16),
    ('raw_value', None), ('storage_size', True)])
def test_direct_gid_full_storage_cannot_be_reconstructed_from_payload(field, value):
    rows, pair = fixture()
    receive = target(rows)
    receive['message_info_receipt']['publisher_gid'][field] = value
    sync_info(receive)
    assert publish_occurrence_witness(pair, receive, rows)[0] is None


def test_writer_entity_kind_gate_is_not_relaxed_for_fixture():
    rows, pair = fixture()
    invalid = WRITER[:-2] + '11'
    receive = target(rows)
    receive['publisher_gid'] = pair['writer_gid'] = invalid
    receive['message_info_receipt']['publisher_gid'].update(value=invalid, raw_value=invalid)
    sync_info(receive)
    assert publish_occurrence_witness(pair, receive, rows)[1] == 'direct_publisher_gid_missing_or_mismatch'


@pytest.mark.parametrize('index,field', [(3, 'topic'), (4, 'message_type'), (6, 'topic')])
def test_new_source_context_fields_cannot_retro_qualify_old_raw(index, field):
    rows, _ = fixture()
    del rows[index][field]
    assert producer_occurrence_witness(rows[6], rows)[0] is None


@pytest.mark.parametrize('role,index', [('writer', 6), ('reader', 8), ('control', 7)])
def test_retirement_is_not_rebound_to_a_fresh_worker(role, index):
    rows, pair = fixture()
    retired = deepcopy(rows[index])
    retired.update(event='topology_worker_exit', event_index=len(rows), monotonic_ns=101)
    rows.append(retired)
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


def test_unique_body_does_not_downgrade_failed_new_receipt():
    rows, pair = fixture()
    # Keep only one offline publish and its observations, making payload unique.
    rows = rows[:9]
    rows[6]['publish_occurrence_receipt']['status'] = 'unknown'
    witness, reason = publish_occurrence_witness(pair, target(rows), rows)
    assert witness is None and reason == 'publish_occurrence_receipt_unavailable'


def test_duplicate_guid_cannot_borrow_a_different_logical_writer_creation():
    rows, pair = fixture()
    alias = deepcopy(rows[0])
    alias.update(event_index=len(rows), logical_id='w1', pid=999,
                 endpoint_generation='different-worker')
    rows.append(alias)
    assert publish_occurrence_witness(pair, target(rows), rows)[0] is None


@pytest.mark.parametrize('writer_role', ['writer', 'writer2', 'writer9'])
def test_writer_role_aliases_use_same_worker_protocol_as_reader_aliases(writer_role):
    rows, pair = fixture()
    for row in rows:
        if row.get('logical_id') == 'w0':
            row['role'] = writer_role
    producer, reason = producer_occurrence_witness(rows[6], rows)
    assert producer is not None, reason
    witness, reason = publish_occurrence_witness(pair, target(rows), rows)
    assert witness is not None, reason
    assert witness['send_ref'] == 6
    assert witness['publication_sequence_number'] == 41
    assert witness['control_identity']['logical_id'] == 'r1'
