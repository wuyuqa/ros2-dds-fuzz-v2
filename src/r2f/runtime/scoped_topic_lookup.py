"""Native typed lookup receipts; graph visibility is not a find-topic result.

Only the already fingerprint-qualified Cyclone 0.10.5 C adapter is supported.
C++/Python binding historical cases require their own pre/post validation.
"""
from __future__ import annotations


def lookup_receipt(scope, name, *, reason=None, execution_evidence=False):
    return {
        'schema': 'r2f.scoped-topic-lookup/v1', 'scope': scope,
        'requested_name': name, 'name_namespace': 'dds_native_literal',
        'status': 'unsupported', 'reason': reason, 'complete': False,
        'candidate_eligible': False, 'execution_evidence': execution_evidence,
        'api': None, 'binding': 'CycloneDDS_C',
        'attribution': 'capability_unsupported_not_sut_defect',
        's1_coverage_rows': ['C02', 'C09', 'C16'],
        'historical_validation': 'not_run',
        'historical_cases': ['core215', 'cxx294'],
        'limitations': ['not_rclpy_graph_or_cpp_python_binding_validation',
                        'no_historical_affected_or_first_fixed_release_claim'],
    }


def _text_readback(c, function, entity):
    buffer = c.create_string_buffer(1024)
    rc = function(entity, buffer, len(buffer))
    if type(rc) is not int or not 0 < rc < len(buffer):
        raise ValueError('topic_lookup_string_readback_incomplete')
    if buffer.raw[rc] != 0 or b'\x00' in buffer.raw[:rc]:
        raise ValueError('topic_lookup_string_readback_invalid')
    return buffer.raw[:rc].decode('utf-8', errors='strict')


def observe_topic_lookup(observer, domain, gid, kind, scope, name, *,
                         native_type_support=None, ros_type_name=None):
    """One nonblocking typed lookup in an already qualified worker library.

    The supplied observer is the existing fingerprint/ABI-qualified Cyclone
    observer. Its native handles never leave this process as usable handles.
    A found topic belongs to the requesting participant even in GLOBAL scope;
    it does not identify the remote participant that first advertised the type.
    """
    import time
    result = lookup_receipt(scope, name, execution_evidence=True)
    result.update(started_monotonic_ns=time.monotonic_ns(), domain_id=domain,
                  local_gid=gid, role_kind=kind,
                  provenance={'observer': observer.provenance, 'lookup_api_version': '0.10.5',
                              'creates_topic_reference': True,
                              'release_contract': 'dds_delete_found_topic_and_dds_free_typeinfo'},
                  timeout_ns=0, cleanup_confirmed=True,
                  result_ownership='requesting_participant_not_remote_origin',
                  attribution='binding_observation')
    if native_type_support is not None:
        result['native_type_support'] = {'reference': native_type_support}
    topic = None
    native_control = None
    type_info = observer.c.c_void_p()
    owns_type_info = False
    c, lib = observer.c, observer.lib
    try:
        if (scope not in ('participant', 'local_domain', 'global')
                or not isinstance(name, str) or not 0 < len(name) <= 256 or '\x00' in name):
            raise ValueError('topic_lookup_request_invalid')
        if observer.provenance.get('abi') != 'CycloneDDS-0.10.5-lp64-endpoint64':
            result['reason'] = 'topic_lookup_native_version_unqualified'
            return result
        signatures = {
            'dds_get_topic': ([c.c_int32], c.c_int32),
            'dds_get_typeinfo': ([c.c_int32, c.POINTER(c.c_void_p)], c.c_int32),
            'dds_free_typeinfo': ([c.c_void_p], c.c_int32),
            'dds_find_topic': ([c.c_int, c.c_int32, c.c_char_p, c.c_void_p, c.c_int64], c.c_int32),
            'dds_delete': ([c.c_int32], c.c_int32),
            'dds_get_name': ([c.c_int32, c.c_void_p, c.c_size_t], c.c_int32),
            'dds_get_type_name': ([c.c_int32, c.c_void_p, c.c_size_t], c.c_int32),
            'dds_get_participant': ([c.c_int32], c.c_int32),
        }
        for api, (args, returns) in signatures.items():
            function = getattr(lib, api, None)
            if not callable(function):
                result['reason'] = 'topic_lookup_api_unavailable:' + api
                return result
            function.argtypes, function.restype = args, returns
        result['api'] = 'dds_find_topic(scope,participant,name,type_info,timeout)'
        before = observer.sample_ownership(domain, gid, kind)
        result['ownership_before'] = before
        if before.get('complete') is not True:
            result.update(status='unknown', reason='topic_lookup_ownership_unverified')
            return result
        participant = before['participant_handle']
        local = before['local_handle']
        result.update(participant_handle=participant, participant_gid=before['participant_gid'])
        source_topic = lib.dds_get_topic(local)
        result['source_topic_return'] = source_topic
        if type(source_topic) is not int or source_topic <= 0:
            raise ValueError('topic_lookup_source_topic_unavailable')
        result['bound_type_name'] = _text_readback(c, lib.dds_get_type_name, source_topic)
        if native_type_support is not None:
            from .native_lookup_support import NativeLookupControl
            result['bound_rmw_type_name'] = result['bound_type_name']
            native_control = NativeLookupControl(observer, native_type_support, ros_type_name)
            result['native_type_support'] = native_control.receipt
            source_topic = native_control.prepare(participant, domain, name)
            result['native_source_topic'] = source_topic
            result['bound_type_name'] = _text_readback(c, lib.dds_get_type_name, source_topic)
            result['limitations'].append('native_control_does_not_supply_rmw_topic_typeinformation')
        rc = lib.dds_get_typeinfo(source_topic, c.byref(type_info))
        result['type_info_return_code'] = rc
        if rc != 0 or not type_info.value:
            # Qualified 0.10.5 ddsrt/retcode.h: NOT_FOUND = DDS_XRETCODE(10)
            # = -60. A real RMW topic can lack XTypes metadata even though it
            # serializes and matches normally. Do not retry name-only lookup:
            # without type_info the API may return an arbitrary same-name type.
            unsupported = rc in (-2, -60)
            result.update(status='unsupported' if unsupported else 'unknown',
                          reason='topic_lookup_bound_type_info_unavailable' if rc == -60
                          else 'topic_lookup_type_info_unavailable',
                          attribution='capability_unsupported_not_sut_defect' if unsupported
                          else 'binding_adaptation_or_observation_error')
            return result
        owns_type_info = True
        # 0.10.5 dds_public_impl.h enum, NOT the deprecated four-argument API.
        scope_value = {'global': 0, 'local_domain': 1, 'participant': 2}[scope]
        result.update(scope_value=scope_value, type_selection=(
            'native_type_info_from_explicit_generated_control' if native_control is not None
            else 'native_type_info_from_bound_endpoint'))
        topic = lib.dds_find_topic(scope_value, participant, name.encode('utf-8'), type_info, 0)
        result['find_return_code'] = topic
        if type(topic) is not int:
            raise ValueError('topic_lookup_return_invalid')
        if topic < 0:
            result.update(status='unsupported' if topic == -2 else 'unknown',
                          reason='topic_lookup_api_error')
        elif topic == 0:
            result.update(status='not_found', complete=True, reason=None,
                          attribution='normal_lookup_absence_not_sut_defect')
        else:
            result['actual_name'] = _text_readback(c, lib.dds_get_name, topic)
            result['actual_type_name'] = _text_readback(c, lib.dds_get_type_name, topic)
            result['result_participant_handle'] = lib.dds_get_participant(topic)
            if (result['actual_name'] != name
                    or result['actual_type_name'] != result['bound_type_name']
                    or result['result_participant_handle'] != participant):
                result.update(status='contract_mismatch', complete=True,
                              reason='topic_lookup_result_contract_mismatch',
                              attribution='type_or_scope_contract_requires_review')
            else:
                result.update(status='found', complete=True, reason=None)
        if native_control is not None and result.get('complete') is True:
            expected = native_control.expected_status(scope)
            observed = result['status']
            native_control.receipt.update(expected_status=expected, observed_status=observed,
                                          expectation_satisfied=observed == expected)
            if observed != expected:
                result.update(status='contract_mismatch',
                    reason='native_control_type_or_scope_contract_mismatch',
                    attribution='type_or_scope_contract_requires_review')
    except (ValueError, TypeError, OSError, AttributeError, RuntimeError) as exc:
        result.update(status='unknown', complete=False, reason=str(exc),
                      attribution='binding_adaptation_or_observation_error')
    finally:
        # Delete only the reference returned by find, never the borrowed source
        # topic, endpoint or participant. Preserve each cleanup return/failure.
        for api, value in (('dds_delete', topic if type(topic) is int and topic > 0 else None),
                           ('dds_free_typeinfo', type_info if owns_type_info else None)):
            if value is None:
                continue
            try:
                rc = getattr(lib, api)(value)
                result[api + '_return_code'] = rc
                if rc != 0:
                    result['cleanup_confirmed'] = False
            except (ValueError, TypeError, OSError, AttributeError, RuntimeError) as exc:
                result[api + '_error'] = str(exc)
                result['cleanup_confirmed'] = False
        if native_control is not None and not native_control.close():
            result['cleanup_confirmed'] = False
        if result.get('ownership_before', {}).get('complete') is True:
            try:
                after = observer.sample_ownership(domain, gid, kind)
                result['ownership_after'] = after
                keys = ('local_handle', 'participant_handle', 'participant_gid', 'local_gid', 'domain_id')
                before = result['ownership_before']
                if (after.get('complete') is not True
                        or any(before.get(key) != after.get(key) for key in keys)):
                    result.update(status='unknown', complete=False,
                                  reason='topic_lookup_ownership_changed')
            except (ValueError, TypeError, OSError, AttributeError, RuntimeError) as exc:
                result.update(status='unknown', complete=False, reason='topic_lookup_ownership_recheck:' + str(exc))
        if not result['cleanup_confirmed']:
            result.update(status='tool_error', complete=False, reason='topic_lookup_cleanup_unconfirmed',
                          attribution='harness_resource_cleanup')
        result['ended_monotonic_ns'] = time.monotonic_ns()
    return result
