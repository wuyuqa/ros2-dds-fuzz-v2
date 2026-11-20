"""Capture optional public MessageInfo fields without inferring DDS identity.

A callback may expose a dict or an attribute object. Missing fields and the
RMW uint64 unsupported sentinel stay explicit; payload sequence numbers are
never substituted. Publication sequence numbers identify an occurrence only
with a separately context-bound publisher identity, not across reader workers.
"""
from collections.abc import Mapping

SCHEMA = 'r2f.message-info-fields/v1'
UINT64_MAX = (1 << 64) - 1
_FIELDS = ('source_timestamp', 'received_timestamp',
           'publication_sequence_number', 'reception_sequence_number')
_MISSING = object()


def capture_message_info(info):
    """Return JSON-only observations, not source/support/identity qualification.

    Timestamps retain their public API clock domain: they are not comparable
    to worker monotonic time. Sequence wrap, reset, GID context and occurrence
    mapping need independent lifecycle evidence from a later consumer.
    """
    fields = {}
    for name in _FIELDS:
        try:
            value = info.get(name, _MISSING) if isinstance(info, Mapping) else getattr(info, name, _MISSING)
        except Exception as exc:
            fields[name] = {'status': 'read_error', 'value': None,
                            'error_type': type(exc).__name__}
            continue
        if value is _MISSING or value is None:
            fields[name] = {'status': 'unavailable_api', 'value': None}
        elif type(value) is not int or not 0 <= value <= UINT64_MAX:
            fields[name] = {'status': 'invalid_value', 'value': None,
                            'value_type': type(value).__name__}
        elif name.endswith('sequence_number') and value == UINT64_MAX:
            fields[name] = {'status': 'unsupported_rmw', 'value': None,
                            'raw_value': UINT64_MAX}
        elif name.endswith('timestamp') and value == 0:
            fields[name] = {'status': 'zero_unavailable_or_unspecified', 'value': None,
                            'raw_value': 0}
        else:
            fields[name] = {'status': 'observed', 'value': value}
    return {'schema': SCHEMA, 'api': 'rclpy subscription callback MessageInfo',
            'fields': fields, 'identity_qualified': False,
            'identity_limitation': 'requires_independent_context_bound_publisher_identity_and_occurrence_mapping',
            'timestamp_clock': 'rmw_api_unspecified_relation_to_worker_monotonic',
            'candidate_eligible': False, 'filter_effect_observed': None}
