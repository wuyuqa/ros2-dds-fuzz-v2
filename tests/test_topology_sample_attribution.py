"""Self-authored offline native raw contracts; never ROS/DDS execution evidence."""
from copy import deepcopy

import pytest

from r2f.campaign.topology_sample_attribution import native_delivery_identity
from test_topology_publish_occurrence import prepared


def raw_ref(event):
    return {'path': 'offline://native-attribution/raw.jsonl', 'sha256': 'f' * 64,
            'event_index': event['event_index']}


def sends(rows):
    return [row for row in rows if row['event'] == 'send']


def sample(rows, *, reader='r0', stamp=9003):
    return next(row for row in rows if row['event'] == 'receive'
                and row['logical_id'] == reader
                and row['message_info_receipt']['fields']['source_timestamp']['value'] == stamp)


def test_identical_body_maps_actual_0_2_4_not_body_sequence_and_keeps_raw_immutable():
    rows, _ = prepared()
    before = deepcopy(rows)
    for stamp, sequence in [(7001, 0), (9003, 2), (12009, 4)]:
        received = sample(rows, stamp=stamp)
        identity = native_delivery_identity(received, sends(rows), rows, ref=raw_ref)
        assert identity['sequence'] == sequence and received['send_sequence'] == 0
        assert identity['basis'] == 'direct_gid_scoped_publish_occurrence'
        assert identity['publisher_gid_source'] == 'public_rcl_take.rmw_message_info_t.publisher_gid'
        assert identity['direct_gid_match'] is True
        assert identity['source_timestamp'] == stamp
        assert identity['send_ref'] == raw_ref(next(r for r in sends(rows) if r['send_sequence'] == sequence))
        assert identity['receive_ref'] == raw_ref(received)
        assert identity['native_witness']['control_ref'] == identity['control_ref']
        assert identity['reader_identity']['creation_ref'] == raw_ref(rows[1])
        assert identity['writer_identity']['creation_ref'] == raw_ref(rows[0])
    assert rows == before


@pytest.mark.parametrize('fault', ['bad_hash', 'bad_schema', 'unknown_status', 'null_receipt',
                                   'delete_receipt', 'missing_trace', 'wrong_scope',
                                   'failed_publish', 'duplicate_timestamp'])
def test_declared_native_failure_never_returns_none_for_legacy_fallback(fault):
    rows, _ = prepared()
    send = sends(rows)[1]
    receipt = send['publish_occurrence_receipt']
    if fault == 'bad_hash':
        send['payload_sha256'] = '0' * 64
    elif fault == 'bad_schema':
        receipt['schema'] = 'wrong'
    elif fault == 'unknown_status':
        receipt['status'] = 'unknown'
    elif fault == 'null_receipt':
        send['publish_occurrence_receipt'] = None
    elif fault == 'delete_receipt':
        del send['publish_occurrence_receipt']
    elif fault == 'missing_trace':
        rows[10]['event'] = 'offline_removed_trace'
    elif fault == 'wrong_scope':
        rows[10]['command_id'] = 999
    elif fault == 'failed_publish':
        send['publish_returned'] = False
    else:
        sends(rows)[2]['publish_occurrence_receipt']['source_timestamp'] = receipt['source_timestamp']
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)


@pytest.mark.parametrize('fault', ['missing_gid', 'zero_gid', 'bad_gid', 'wrong_reader',
                                   'wrong_generation', 'wrong_pid', 'wrong_clock',
                                   'wrong_pubseq', 'no_take', 'forged_take', 'missing_control'])
def test_native_receive_and_independent_control_must_be_authenticated(fault):
    rows, _ = prepared()
    received = sample(rows)
    if fault == 'missing_gid':
        received.pop('publisher_gid')
    elif fault == 'zero_gid':
        received['publisher_gid'] = '0' * 32
    elif fault == 'bad_gid':
        received['publisher_gid'] = 'not-a-gid'
    elif fault == 'wrong_reader':
        received['reader_gid'] = '07' * 16
    elif fault == 'wrong_generation':
        received['generation'] = 2
    elif fault == 'wrong_pid':
        received['pid'] = 999
    elif fault == 'wrong_clock':
        received['monotonic_clock_identity'] = 'foreign'
    elif fault == 'wrong_pubseq':
        received['message_info_receipt']['fields']['publication_sequence_number']['value'] = 89
        received['rcl_take_receipt']['message_info_receipt'] = deepcopy(received['message_info_receipt'])
    elif fault == 'no_take':
        received.pop('rcl_take_receipt')
    elif fault == 'forged_take':
        received['rcl_take_receipt']['message_info_receipt']['publisher_gid']['value'] = '07' * 16
    else:
        sample(rows, reader='r1')['event'] = 'offline_removed_control'
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(received, sends(rows), rows, ref=raw_ref)


def test_no_producer_declaration_returns_none_without_rebuilding_legacy_logic():
    rows, _ = prepared()
    for row in rows:
        if row['event'] == 'send':
            row.pop('publish_occurrence_receipt')
        elif row['event'] in {'publish_call_begin', 'rmw_publish_trace', 'publish_call_end'}:
            row['event'] = 'offline_legacy_no_scope'
    assert native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref) is None


def test_empty_bad_receipt_key_alone_still_declares_native():
    rows, _ = prepared()
    for row in rows:
        if row['event'] == 'send':
            row['publish_occurrence_receipt'] = {}
        elif row['event'] in {'publish_call_begin', 'rmw_publish_trace', 'publish_call_end'}:
            row['event'] = 'offline_removed_scope'
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)


def add_legacy_writer(rows):
    writer = deepcopy(rows[0])
    writer.update(logical_id='legacy-writer', generation=1, endpoint_gid='07' * 15 + '03',
                  pid=600, pgid=600, process_start_ticks=600,
                  endpoint_generation='offline-legacy-writer', node_name='legacy-writer',
                  event_index=len(rows), monotonic_ns=1000 + len(rows))
    rows.append(writer)
    return writer


def test_bad_direct_gid_cannot_switch_native_sample_to_existing_legacy_writer():
    rows, _ = prepared()
    legacy = add_legacy_writer(rows)
    received = sample(rows)
    received['publisher_gid'] = legacy['endpoint_gid']
    # The actual single-take receipt still declares the native writer. Selection
    # cannot use a corrupted top-level GID to enable legacy body attribution.
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(received, sends(rows), rows, ref=raw_ref)


def test_separately_anchored_legacy_writer_does_not_inherit_other_writer_native_contract():
    rows, _ = prepared()
    legacy = add_legacy_writer(rows)
    received = deepcopy(sample(rows))
    received.update(event_index=len(rows), monotonic_ns=1100, publisher_gid=legacy['endpoint_gid'])
    info = received['message_info_receipt']['publisher_gid']
    info['value'] = info['raw_value'] = legacy['endpoint_gid']
    received['rcl_take_receipt']['message_info_receipt'] = deepcopy(received['message_info_receipt'])
    rows.append(received)
    assert native_delivery_identity(received, sends(rows), rows, ref=raw_ref) is None


def test_unfiltered_control_sample_can_self_measure_without_claiming_two_receives():
    rows, _ = prepared()
    received = sample(rows, reader='r1')
    value = native_delivery_identity(received, sends(rows), rows, ref=raw_ref)
    assert value['sequence'] == 2
    assert value['control_ref'] == value['receive_ref'] == raw_ref(received)
    assert value['control_identity']['logical_id'] == value['reader'] == 'r1'


def test_current_reader_incarnation_is_bound_to_actual_transition_and_ack():
    from test_topology_cft_occurrence_decoder import contrast_rows
    rows = contrast_rows()
    old = native_delivery_identity(sample(rows, stamp=7001), sends(rows), rows, ref=raw_ref)
    current = native_delivery_identity(sample(rows, stamp=12009), sends(rows), rows, ref=raw_ref)
    assert old['reader_generation'] == current['reader_generation'] == 1
    assert old['reader_gid'] != current['reader_gid']
    assert old['reader_identity']['incarnation_ordinal'] == 0
    assert old['reader_identity']['transition_ref'] is None
    assert current['reader_identity']['incarnation_ordinal'] == 1
    transition = next(r for r in rows if r['event'] == 'endpoint_identity_transition')
    ack = next(r for r in rows if r['event'] == 'command_done'
               and r.get('command') == 'update_filter')
    assert current['reader_identity']['transition_ref'] == raw_ref(transition)
    assert current['reader_identity']['ack_ref'] == raw_ref(ack)
    assert current['sequence'] == 4
    ack['event'] = 'offline_missing_ack'
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(sample(rows, stamp=12009), sends(rows), rows, ref=raw_ref)


@pytest.mark.parametrize('field,value', [('logical_id', 'foreign-writer'), ('generation', 2),
                                        ('pid', 999), ('writer_gid', '07' * 15 + '03')])
def test_foreign_source_scope_never_binds_by_body(field, value):
    rows, _ = prepared()
    sends(rows)[1][field] = value
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)


@pytest.mark.parametrize('fault', ['omitted_send', 'copied_send', 'duplicate_send',
                                   'copied_receive', 'bad_index', 'no_control_configuration',
                                   'take_reuse', 'foreign_rmw'])
def test_raw_object_set_and_native_provenance_cannot_be_substituted(fault):
    rows, _ = prepared()
    received = sample(rows)
    source_set = sends(rows)
    if fault == 'omitted_send':
        source_set.pop(1)
    elif fault == 'copied_send':
        source_set[1] = deepcopy(source_set[1])
    elif fault == 'duplicate_send':
        source_set.append(source_set[1])
    elif fault == 'copied_receive':
        received = deepcopy(received)
    elif fault == 'bad_index':
        received['event_index'] = True
    elif fault == 'no_control_configuration':
        del rows[2]['initial_content_filter']
    elif fault == 'take_reuse':
        received['rcl_take_receipt']['take_index'] = sample(rows, stamp=7001)['rcl_take_receipt']['take_index']
    else:
        for row in rows:
            row['rmw_identifier'] = 'rmw_cyclonedds_cpp'
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(received, source_set, rows, ref=raw_ref)


def test_writer_alias_and_opaque_raw_refs_are_preserved_without_mutating_capture():
    rows, _ = prepared()
    for row in rows:
        if row.get('logical_id') == 'w0':
            row['role'] = 'writer2'
    before = deepcopy(rows)
    value = native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert value['sequence'] == 2 and value['publication_sequence_number'] == 88
    proof = value['native_witness']
    assert proof['trace_ref'] == 'publish-2.trace.0'
    assert proof['scope_refs'] == {key: raw_ref(rows[index]) for key, index in
                                  [('begin_ref', 9), ('trace_ref', 10), ('end_ref', 11)]}
    value['reader_identity']['endpoint_gid'] = 'changed-output-only'
    assert rows == before


def test_payload_native_claim_cannot_downgrade_after_all_direct_gids_are_changed():
    rows, _ = prepared()
    legacy = add_legacy_writer(rows)
    received = sample(rows)
    received.update(publisher_gid=legacy['endpoint_gid'], payload_writer_id='w0', payload_writer_generation=1)
    for info in (received['message_info_receipt'], received['rcl_take_receipt']['message_info_receipt']):
        info['publisher_gid']['value'] = info['publisher_gid']['raw_value'] = legacy['endpoint_gid']
    with pytest.raises(ValueError, match='native_delivery_identity:'):
        native_delivery_identity(received, sends(rows), rows, ref=raw_ref)


def test_later_control_retirement_and_replacement_preserve_historical_identity():
    rows, _ = prepared()
    control = rows[2]
    rows.append(dict(control, event="topology_endpoint_destroyed", event_index=len(rows),
                     monotonic_ns=100 + len(rows)))
    rows.append(dict(control, event="endpoint_created", generation=2,
                     endpoint_gid="09" * 16, pid=302, process_start_ticks=20,
                     endpoint_generation="offline-control-generation-2",
                     event_index=len(rows), monotonic_ns=100 + len(rows)))
    original = deepcopy(rows)
    identity = native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert identity["control_identity"]["logical_id"] == "r1"
    assert identity["control_identity"]["generation"] == 1
    assert identity["control_identity"]["endpoint_gid"] == control["endpoint_gid"]
    assert identity["control_ref"] == raw_ref(sample(rows, reader="r1"))
    assert rows == original


@pytest.mark.parametrize("event", ["endpoint_destroyed", "topology_endpoint_destroyed",
                                   "endpoint_identity_transition", "content_filter_dynamic_update"])
def test_control_changes_proven_later_do_not_erase_prior_sample_identity(event):
    rows, _ = prepared()
    rows.append(dict(rows[2], event=event, event_index=len(rows), monotonic_ns=100 + len(rows)))
    original = deepcopy(rows)
    identity = native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert identity["control_identity"]["generation"] == 1
    assert identity["control_ref"] == raw_ref(sample(rows, reader="r1"))
    assert rows == original


@pytest.mark.parametrize("clock", [None, True, 0, 110])
def test_control_change_without_proven_later_clock_remains_unknown(clock):
    rows, _ = prepared()
    rows.append(dict(rows[2], event="topology_endpoint_destroyed", event_index=len(rows),
                     monotonic_ns=clock))
    with pytest.raises(ValueError, match="native_delivery_identity:"):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)


def test_future_reader_cannot_replace_missing_historical_control_configuration():
    rows, _ = prepared()
    future = dict(rows[2], generation=2, endpoint_gid="09" * 16,
                  endpoint_generation="offline-future-control", pid=302, process_start_ticks=20,
                  event_index=len(rows), monotonic_ns=100 + len(rows))
    rows[2].pop("initial_content_filter")
    rows.append(future)
    with pytest.raises(ValueError, match="native_delivery_identity:"):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)



def delayed_control_receive(rows):
    control = sample(rows, reader="r1")
    delayed = deepcopy(control)
    control["event"] = "offline_removed_control_receive"
    change = dict(rows[2], event="content_filter_dynamic_update",
                  event_index=len(rows), monotonic_ns=100 + len(rows),
                  command="update_filter", command_id=999)
    requested = dict(expression="data = %0", parameters=[delayed["string_data"]])
    change.update(reader_gid=rows[2]["endpoint_gid"], before_gid=rows[2]["endpoint_gid"],
                  after_gid=rows[2]["endpoint_gid"],
                  filter_receipt=dict(receipt_kind="filter_update", requested=requested,
                                      observed=deepcopy(requested), api_status="succeeded",
                                      readback_status="matched", applied=True,
                                      filter_effect_observed=None, hit=False))
    rows.append(change)
    delayed.update(event_index=len(rows), monotonic_ns=change["monotonic_ns"] + 3)
    delayed["rcl_take_receipt"].update(take_begin_monotonic_ns=change["monotonic_ns"] + 1,
                                       take_return_monotonic_ns=change["monotonic_ns"] + 2)
    rows.append(delayed)
    return change, delayed


def test_control_filtered_between_target_and_actual_control_sample_is_rejected():
    rows, _ = prepared()
    change, control = delayed_control_receive(rows)
    assert (sample(rows)["event_index"], sample(rows)["monotonic_ns"]) == (14, 114)
    assert (change["event_index"], change["monotonic_ns"]) == (21, 121)
    assert (control["event_index"], control["monotonic_ns"]) == (22, 124)
    original = deepcopy(rows)
    with pytest.raises(ValueError, match="native_delivery_identity:"):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert rows == original


@pytest.mark.parametrize("event", ["endpoint_destroyed", "topology_endpoint_destroyed",
                                   "endpoint_identity_transition", "content_filter_dynamic_update"])
def test_changes_after_both_delayed_samples_preserve_actual_control_identity(event):
    rows, _ = prepared()
    change, control = delayed_control_receive(rows)
    rows.remove(change)
    change.update(event=event, monotonic_ns=control["monotonic_ns"] + 1)
    rows.append(change)
    for index, row in enumerate(rows):
        row["event_index"] = index
    original = deepcopy(rows)
    identity = native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert identity["control_ref"] == raw_ref(control)
    assert identity["sequence"] == 2
    assert identity["control_identity"]["generation"] == 1
    assert rows == original


@pytest.mark.parametrize("event", ["endpoint_destroyed", "topology_endpoint_destroyed",
                                   "endpoint_identity_transition", "content_filter_dynamic_update"])
@pytest.mark.parametrize("raw_after,clock", [(False, 125), (True, 123), (True, 124),
                                            (True, None), (True, True)])
def test_control_change_requires_raw_and_clock_proof_after_actual_control_sample(event, raw_after, clock):
    rows, _ = prepared()
    change, control = delayed_control_receive(rows)
    change.update(event=event, monotonic_ns=clock)
    if raw_after:
        rows.remove(change)
        rows.append(change)
        for index, row in enumerate(rows):
            row["event_index"] = index
    original = deepcopy(rows)
    with pytest.raises(ValueError, match="native_delivery_identity:"):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert rows == original


@pytest.mark.parametrize("fault", ["body", "pubseq", "generation", "take", "send_collision", "alias"])
def test_later_control_change_cannot_weaken_actual_native_witness_authentication(fault):
    rows, _ = prepared()
    rows.append(dict(rows[2], event="content_filter_dynamic_update",
                     event_index=len(rows), monotonic_ns=100 + len(rows)))
    control = sample(rows, reader="r1")
    if fault == "body":
        control["string_data"] = "foreign body"
    elif fault == "pubseq":
        control["message_info_receipt"]["fields"]["publication_sequence_number"]["value"] = 999
        control["rcl_take_receipt"]["message_info_receipt"] = deepcopy(control["message_info_receipt"])
    elif fault == "generation":
        control["generation"] = 2
    elif fault == "take":
        control["rcl_take_receipt"]["take_index"] = sample(rows, reader="r1", stamp=7001)["rcl_take_receipt"]["take_index"]
    elif fault == "send_collision":
        rows.append(dict(deepcopy(sends(rows)[1]), event_index=len(rows), monotonic_ns=100 + len(rows)))
    else:
        control["role"] = "writer2"
    original = deepcopy(rows)
    with pytest.raises(ValueError, match="native_delivery_identity:"):
        native_delivery_identity(sample(rows), sends(rows), rows, ref=raw_ref)
    assert rows == original
