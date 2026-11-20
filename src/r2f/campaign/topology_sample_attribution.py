"""Canonical adapter's strict native-only per-sample attribution.

This module consumes existing raw contracts; it does not run ROS, rewrite body
sequences, implement the legacy attribution path, or qualify filtering effects.
A producer declaration selects the strong contract before inspecting its receipt
status/schema/hash. Invalid native evidence raises ValueError, never a signal to
retry the legacy sequence/unique-hash branch.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Callable, TypedDict

from r2f.runtime.publish_occurrence import publish_occurrence_witness
from r2f.runtime.topology_worker import worker_role_kind

_IDENTITY_REFS = ('creation_ref', 'transition_ref', 'ack_ref', 'valid_after_ref')


class NativeDeliveryIdentity(TypedDict):
    writer: str
    writer_generation: int
    reader: str
    reader_generation: int
    sequence: int
    basis: str
    direct_gid_match: bool
    publisher_gid: str
    publisher_gid_status: str
    publisher_gid_source: str
    send_ref: dict
    receive_ref: dict
    control_ref: dict
    source_timestamp: int
    publication_sequence_number: int
    reader_gid: str
    reader_identity: dict
    writer_identity: dict
    control_identity: dict
    native_witness: dict


def _require(condition, reason):
    if not condition:
        raise ValueError('native_delivery_identity:' + reason)


def declares_native_publish(row):
    """Select the FastDDS occurrence contract, not generic public API scopes.

    Trace/receipt presence selects strict validation even if malformed or on an
    unsupported RMW. Begin/end are emitted for ordinary Cyclone publishes too;
    only their explicit FastDDS/instrumentation declaration selects UST proof.
    This function dispatches contracts; it does not authenticate any sample.
    """
    kind = row.get('event')
    return (kind == 'rmw_publish_trace'
            or kind == 'send' and 'publish_occurrence_receipt' in row
            or kind in {'publish_call_begin', 'publish_call_end'}
            and (row.get('rmw_identifier') == 'rmw_fastrtps_cpp'
                 or row.get('observation_mode') == 'instrumentation'))


def _writer_for_native(event, events):
    declarations = [row for row in events if declares_native_publish(row)]
    if not declarations:
        return None
    publisher = event.get('publisher_gid')
    writers = [row for row in events if row.get('event') == 'endpoint_created'
               and worker_role_kind(row.get('role')) == 'writer'
               and publisher is not None and row.get('endpoint_gid') == publisher]
    # Missing/bad/ambiguous direct identity in a native capture cannot authorize
    # falling back to a body assertion. The strict witness checks full GID/take
    # provenance; this lookup is selection only, never authentication.
    _require(len(writers) == 1, 'native_writer_creation_missing_or_ambiguous')
    writer = writers[0]
    def declared(eid=None, generation=None, gid=None):
        return any(
            (eid is not None and generation is not None and row.get('logical_id') == eid
             and row.get('generation') == generation)
            or (gid is not None and (row.get('writer_gid') == gid or row.get('endpoint_gid') == gid))
            for row in declarations)

    if declared(writer.get('logical_id'), writer.get('generation'), publisher):
        return writer
    # Secondary raw claims can only select the stronger verifier/rejection,
    # never authenticate a sample. Otherwise changing the top-level GID to an
    # existing legacy writer could downgrade a still-native take receipt.
    hints = [event.get('message_info_receipt')]
    take = event.get('rcl_take_receipt')
    if isinstance(take, dict):
        hints.append(take.get('message_info_receipt'))
    for info in hints:
        gid_info = info.get('publisher_gid') if isinstance(info, dict) else None
        if isinstance(gid_info, dict):
            _require(not any(declared(gid=gid_info.get(key)) for key in ('value', 'raw_value')),
                     'native_receive_publisher_scope_mismatch')
    _require(not declared(event.get('payload_writer_id'), event.get('payload_writer_generation')),
             'native_payload_writer_scope_mismatch')
    return None


def _native_delivery_identity(event, sends, events, ref):
    writer = _writer_for_native(event, events)
    if writer is None:
        return None
    _require(callable(ref), 'raw_reference_callback_missing')
    # Reuse the runtime's unique, explicitly unfiltered control selection, then
    # call ONLY the strict occurrence verifier (not its payload legacy wrapper).
    from r2f.runtime.topology_pair import _occurrence_pair_with_control
    pair = dict(writer=writer.get('logical_id'), writer_generation=writer.get('generation'),
                writer_gid=writer.get('endpoint_gid'), reader=event.get('logical_id'),
                reader_generation=event.get('generation'), reader_gid=event.get('reader_gid'))
    pair = _occurrence_pair_with_control(pair, event, events)
    witness, reason = publish_occurrence_witness(pair, event, events)
    _require(witness is not None, reason or 'native_witness_unavailable')

    def row_at(index):
        _require(type(index) is int and 0 <= index < len(events), 'native_raw_reference_out_of_range')
        return events[index]

    chosen = row_at(witness['send_ref'])
    # Use the full capture for verification even if an adapter passes a subset
    # of sends. Neither subset filtering nor copied rows can hide a collision or
    # cause a different source to be substituted for witness.send_ref.
    indices = []
    for sent in sends:
        index = sent.get('event_index')
        _require(sent.get('event') == 'send' and row_at(index) is sent,
                 'adapter_send_not_in_raw_capture')
        indices.append(index)
    _require(len(indices) == len(set(indices)), 'adapter_send_set_duplicate')
    _require(sum(sent is chosen for sent in sends) == 1, 'native_send_not_in_adapter_send_set')
    _require(type(chosen.get('send_sequence')) is int and chosen['send_sequence'] >= 0
             and witness['send_sequence'] == chosen['send_sequence'], 'actual_send_sequence_mismatch')

    def raw_ref(index):
        row = row_at(index)
        result = ref(row)
        _require(isinstance(result, dict) and result.get('event_index') == index,
                 'raw_reference_callback_mismatch')
        return deepcopy(result)

    def identity_refs(identity):
        result = deepcopy(identity)
        for key in _IDENTITY_REFS:
            result[key] = None if identity[key] is None else raw_ref(identity[key])
        return result

    native = deepcopy(witness)
    for key in ('send_ref', 'receive_ref', 'control_ref'):
        native[key] = raw_ref(witness[key])
    native['scope_refs'] = {key: raw_ref(index) for key, index in witness['scope_refs'].items()}
    for key in ('reader_identity', 'writer_identity', 'control_identity'):
        native[key] = identity_refs(witness[key])
    return NativeDeliveryIdentity(
        writer=witness['writer'], writer_generation=witness['writer_generation'],
        reader=witness['reader'], reader_generation=witness['reader_generation'],
        sequence=chosen['send_sequence'], basis=witness['identity_basis'], direct_gid_match=True,
        publisher_gid=witness['writer_gid'], publisher_gid_status=event['publisher_gid_status'],
        publisher_gid_source='public_rcl_take.rmw_message_info_t.publisher_gid',
        send_ref=native['send_ref'], receive_ref=native['receive_ref'], control_ref=native['control_ref'],
        source_timestamp=witness['source_timestamp'],
        publication_sequence_number=witness['publication_sequence_number'],
        reader_gid=witness['reader_gid'], reader_identity=native['reader_identity'],
        writer_identity=native['writer_identity'], control_identity=native['control_identity'],
        native_witness=native)


def native_delivery_identity(event: dict, sends: list[dict], events: list[dict], *,
                             ref: Callable[[dict], dict]) -> NativeDeliveryIdentity | None:
    """Attribute one native sample, or return None ONLY for an undeclared scope.

    Inputs are the original raw objects, with canonical unfiltered event indices.
    ``ref`` is the adapter's raw-reference callback. Successful output maps all
    raw proof indices through it; the provider's trace_ref remains its original
    opaque string. ``sequence`` is the witnessed actual send sequence, never
    ``event.send_sequence`` or a publication-sequence offset. ``publisher_gid_source``
    explicitly names public rcl_take, NOT rclpy.MessageInfo publisher GID.

    A native capture with no unique direct writer anchor is unknown (ValueError).
    A separately anchored writer with no native declaration still returns None,
    even if another writer in the same capture declares the native contract.
    """
    try:
        return _native_delivery_identity(event, sends, events, ref)
    except ValueError as exc:
        if str(exc).startswith('native_delivery_identity:'):
            raise
        raise ValueError('native_delivery_identity:' + str(exc)) from exc
    except (KeyError, TypeError, IndexError, AttributeError) as exc:
        raise ValueError('native_delivery_identity:malformed_native_raw_contract') from exc
