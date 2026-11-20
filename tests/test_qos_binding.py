"""Pure QoS binding and synthetic raw contracts, never live DDS evidence."""
from copy import deepcopy
import pytest
from r2f.runtime.qos_binding import (compare_public_qos, validate_actual_qos_receipt,
                                      validate_created_qos, INFINITE_NS)

QOS = dict(history='keep_last', depth=64, reliability='reliable', durability='volatile',
           deadline_ns=INFINITE_NS, liveliness='automatic', liveliness_lease_duration_ns=INFINITE_NS)


def creation(requested=None, actual=None):
    requested = deepcopy(requested or QOS)
    actual = deepcopy(actual or requested)
    receipt = dict(schema='r2f.public-endpoint-qos/v1', status='observed',
                   api='rcl_publisher_get_actual_qos', qos=actual, pid=123,
                   sampled_monotonic_ns=110, library=dict(path='/fixture/librcl.so', sha256='a'*64),
                   abi={'verified': True, 'fixture': True})
    return dict(role='writer', pid=123, created_monotonic_ns=100, monotonic_ns=120,
                qos=actual, requested_qos=requested, configuration_source='public_endpoint_actual_qos',
                actual_endpoint_qos=receipt, qos_request_comparison=compare_public_qos(requested, actual))


def test_explicit_qos_values_are_not_defaults():
    assert compare_public_qos(QOS, QOS)['status'] == 'matched'
    for field, value in [('depth', 0), ('history', 'keep_all'), ('deadline_ns', 1)]:
        assert compare_public_qos(QOS, dict(QOS, **{field: value}))['status'] == 'mismatch'


@pytest.mark.parametrize('field', ['deadline_ns', 'liveliness_lease_duration_ns'])
@pytest.mark.parametrize('resolved', [1, 42, INFINITE_NS])
def test_only_unspecified_duration_is_documented_not_rewritten(field, resolved):
    requested = dict(QOS, **{field: 0}); actual = dict(QOS, **{field: resolved})
    token = creation(requested, actual)
    report = validate_created_qos(token, requested)
    assert report['status'] == 'matched'
    assert report['default_resolutions'] == [dict(field=field, requested=0, observed=resolved,
                                               basis='RMW_DURATION_UNSPECIFIED')]
    assert token['qos'][field] == resolved and token['requested_qos'][field] == 0
    assert 'hit' not in report and 'effect_observed' not in report


@pytest.mark.parametrize('field,value', [('depth', True), ('depth', '64'), ('deadline_ns', True),
                                         ('deadline_ns', -1), ('deadline_ns', INFINITE_NS+1),
                                         ('liveliness','system_default'), ('reliability', 'unknown')])
def test_unavailable_or_wrong_type_values_cannot_match(field, value):
    assert compare_public_qos(QOS, dict(QOS, **{field:value}))['status'] == 'mismatch'
    token = creation(actual=dict(QOS, **{field:value}))
    with pytest.raises(ValueError): validate_created_qos(token, QOS)


@pytest.mark.parametrize('damage', ['pid','pid_bool','api','stale','future','abi','library',
                                     'hash','missing','echo','source','comparison'])
def test_actual_receipt_identity_and_chronology_are_not_labels(damage):
    token = creation(); receipt=token['actual_endpoint_qos']
    if damage=='pid': receipt['pid']+=1
    elif damage=='pid_bool': receipt['pid']=True
    elif damage=='api': receipt['api']='get_publishers_info_by_topic'
    elif damage=='stale': receipt['sampled_monotonic_ns']=99
    elif damage=='future': receipt['sampled_monotonic_ns']=121
    elif damage=='abi': receipt['abi']['verified']=False
    elif damage=='library': receipt['library']['path']='relative.so'
    elif damage=='hash': receipt['library']['sha256']='not-a-hash'
    elif damage=='missing': receipt['qos'].pop('depth')
    elif damage=='echo': token['qos']=dict(QOS, depth=32)
    elif damage=='source': token['configuration_source']='requested_config'
    elif damage=='comparison': token['qos_request_comparison']['status']='observed'
    with pytest.raises(ValueError): validate_created_qos(token, QOS)


def test_reader_calls_subscription_api_and_does_not_use_graph_depth():
    token=creation(); token['role']='reader'; token['graph_qos']=dict(QOS,depth=0)
    token['actual_endpoint_qos']['api']='rcl_subscription_get_actual_qos'
    observed=validate_actual_qos_receipt(token['actual_endpoint_qos'],token,120)
    assert observed['depth']==64 and token['graph_qos']['depth']==0
