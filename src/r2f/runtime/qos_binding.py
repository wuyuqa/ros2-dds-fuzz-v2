"""Pure request/effective-QoS checks. No endpoint, execution or hit is created.

A zero RMW duration is UNSPECIFIED, not a literal zero-time deadline/lease.
The effective value is always retained; this module never normalizes it back
into the request. History/depth are local resource policies, not graph match
policies, and require an actual local endpoint readback.
"""
from copy import deepcopy
from pathlib import PurePath
import re

SCHEMA = 'r2f.public-qos-comparison/v1'
READBACK_SCHEMA = 'r2f.public-endpoint-qos/v1'
INFINITE_NS = 9223372036854775807
FIELDS = frozenset({'history', 'depth', 'reliability', 'durability', 'deadline_ns',
                    'liveliness', 'liveliness_lease_duration_ns'})
DURATIONS = frozenset({'deadline_ns', 'liveliness_lease_duration_ns'})
ENUMS = {'history': {'keep_last', 'keep_all'}, 'reliability': {'reliable', 'best_effort'},
         'durability': {'volatile', 'transient_local'},
         'liveliness': {'automatic', 'manual_by_topic'}}


def _valid(field, value):
    if field in ENUMS:
        return isinstance(value, str) and value in ENUMS[field]
    if field == 'depth':
        return type(value) is int and 0 <= value <= (1 << 64) - 1
    return type(value) is int and 0 <= value <= INFINITE_NS


def compare_public_qos(requested, observed):
    """Compare explicit fields, documenting RMW default resolution separately.

    Missing/unknown fields, bools, strings in numeric fields and a changed
    explicit finite/infinite duration fail. A valid observed value for a
    submitted UNSPECIFIED duration is not an inferred semantic effect.
    """
    mismatches, resolutions = [], []
    if not isinstance(requested, dict) or not isinstance(observed, dict):
        mismatches.append({'reason': 'qos_mapping_required'})
    else:
        for field, wanted in sorted(requested.items()):
            actual = observed.get(field)
            if field not in FIELDS or not _valid(field, wanted):
                mismatches.append({'field': field, 'reason': 'requested_field_invalid'})
            elif field not in observed or not _valid(field, actual):
                mismatches.append({'field': field, 'reason': 'observed_field_missing_or_invalid'})
            elif type(actual) is type(wanted) and actual == wanted:
                continue
            elif field == 'depth' and requested.get('history') == observed.get('history') == 'keep_all':
                resolutions.append({'field': field, 'requested': wanted, 'observed': actual,
                                    'basis': 'HISTORY_KEEP_ALL_DEPTH_INACTIVE'})
            elif field in DURATIONS and wanted == 0:
                resolutions.append({'field': field, 'requested': wanted, 'observed': actual,
                                    'basis': 'RMW_DURATION_UNSPECIFIED'})
            else:
                mismatches.append({'field': field, 'reason': 'explicit_value_mismatch',
                                   'requested': wanted, 'observed': actual})
    return {'schema': SCHEMA, 'status': 'mismatch' if mismatches else 'matched',
            'default_resolutions': resolutions, 'mismatches': mismatches}


def validate_actual_qos_receipt(receipt, token, observed_monotonic_ns):
    """Bind a public C-API receipt to its existing worker creation token.

    This checks reopened raw structure/identity, not kernel execution or source
    qualification. Those remain the unified runner's independent obligations.
    """
    if not isinstance(receipt, dict) or receipt.get('schema') != READBACK_SCHEMA or receipt.get('status') != 'observed':
        raise ValueError('actual_qos_receipt_schema_or_status_invalid')
    role = token.get('role', '')
    kind = 'writer' if isinstance(role, str) and (role.startswith('writer') or role == 'probe') else 'reader' if isinstance(role, str) and role.startswith('reader') else None
    api = {'writer': 'rcl_publisher_get_actual_qos', 'reader': 'rcl_subscription_get_actual_qos'}.get(kind)
    if api is None or receipt.get('api') != api:
        raise ValueError('actual_qos_receipt_api_role_mismatch')
    if type(receipt.get('pid')) is not int or receipt['pid'] <= 0 or receipt['pid'] != token.get('pid'):
        raise ValueError('actual_qos_receipt_worker_mismatch')
    times = [token.get('created_monotonic_ns'), receipt.get('sampled_monotonic_ns'), observed_monotonic_ns]
    if any(type(t) is not int or t <= 0 for t in times) or not times[0] <= times[1] <= times[2]:
        raise ValueError('actual_qos_receipt_chronology_invalid')
    library, abi = receipt.get('library'), receipt.get('abi')
    if (not isinstance(library, dict) or not isinstance(library.get('path'), str)
            or not PurePath(library['path']).is_absolute()
            or not isinstance(library.get('sha256'), str)
            or re.fullmatch('[0-9a-f]{64}', library['sha256']) is None
            or not isinstance(abi, dict) or abi.get('verified') is not True):
        raise ValueError('actual_qos_receipt_public_library_or_abi_missing')
    qos = receipt.get('qos')
    if not isinstance(qos, dict) or set(qos) != FIELDS or any(not _valid(k, v) for k, v in qos.items()):
        raise ValueError('actual_qos_receipt_fields_invalid')
    return deepcopy(qos)


def validate_created_qos(created, requested):
    """Validate the new endpoint-created schema, without legacy substitution."""
    qos = validate_actual_qos_receipt(created.get('actual_endpoint_qos'), created,
                                      created.get('monotonic_ns'))
    comparison = compare_public_qos(requested, qos)
    if (created.get('configuration_source') != 'public_endpoint_actual_qos'
            or created.get('qos') != qos or created.get('requested_qos') != requested
            or comparison['status'] != 'matched'
            or created.get('qos_request_comparison') != comparison):
        raise ValueError('endpoint_actual_qos_binding_mismatch')
    return comparison
