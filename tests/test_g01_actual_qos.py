"""Offline G01 seam checks. Mock receipts are not ROS/DDS execution proof."""
from copy import deepcopy
import sys
from types import ModuleType
from unittest.mock import Mock, patch

import pytest

from r2f.runtime.worker import g01_actual_endpoint_qos
from r2f.runtime.relation_observer import g01_binding_readback
from r2f.modules.s1.relation_surfaces import RelationSurfaceError


TARGET = 'discovery.match_compatibility.fuzz_v1'


def fixture_binding():
    qos = dict(history='keep_last', depth=64, reliability='reliable', durability='volatile',
               deadline_ns=9223372036854775807, liveliness='automatic',
               liveliness_lease_duration_ns=9223372036854775807)
    token = dict(role='writer', pid=4001, created_monotonic_ns=100)
    receipt = dict(schema='r2f.public-endpoint-qos/v1', status='observed',
                   api='rcl_publisher_get_actual_qos', pid=4001, sampled_monotonic_ns=110,
                   library=dict(path='/offline/librcl.so', sha256='a' * 64),
                   abi=dict(verified=True), qos=qos, offline_fixture=True)
    binding = dict(monotonic_ns=120, actual_endpoint_qos=receipt,
                   observed_self=dict(qos, history='unknown', depth=0))
    return token, binding, dict(qos, deadline_ns=0, liveliness_lease_duration_ns=0)


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_worker_uses_exact_public_helper_only_for_g01(kind):
    helper = ModuleType('r2f.runtime.actual_qos')
    receipt = {'offline_fixture': True}
    helper.read_public_endpoint_qos = Mock(return_value=receipt)
    endpoint = object()
    with patch.dict(sys.modules, {'r2f.runtime.actual_qos': helper}):
        assert g01_actual_endpoint_qos({'target_id': TARGET}, endpoint, kind) == {
            'actual_endpoint_qos': receipt}
        helper.read_public_endpoint_qos.assert_called_once_with(endpoint, kind=kind)
        for target in ('writer_restart', 'discovery.interruption_recovery.fuzz_v1', '', None):
            assert g01_actual_endpoint_qos({'target_id': target}, endpoint, kind) == {}
        helper.read_public_endpoint_qos.assert_called_once()


def test_worker_does_not_swallow_actual_api_failure_or_fallback_to_config():
    helper = ModuleType('r2f.runtime.actual_qos')
    helper.read_public_endpoint_qos = Mock(side_effect=RuntimeError('fixture_actual_api_unavailable'))
    with patch.dict(sys.modules, {'r2f.runtime.actual_qos': helper}):
        with pytest.raises(RuntimeError, match='fixture_actual_api_unavailable'):
            g01_actual_endpoint_qos({'target_id': TARGET}, object(), 'writer')


def test_actual_depth64_and_unspecified_duration_not_graph_depth0_are_compared():
    token, binding, requested = fixture_binding()
    original = deepcopy(binding)
    actual = g01_binding_readback(binding, token, requested)
    assert actual['depth'] == 64
    assert actual['deadline_ns'] == 9223372036854775807
    assert binding == original  # no graph/config normalization or backfill
    requested['depth'] = 63
    with pytest.raises(RelationSurfaceError, match='actual_qos_readback_mismatch'):
        g01_binding_readback(binding, token, requested)


def test_explicit_duration_mismatch_is_not_an_unspecified_default_resolution():
    token, binding, requested = fixture_binding()
    requested['deadline_ns'] = 12345
    with pytest.raises(RelationSurfaceError, match='actual_qos_readback_mismatch'):
        g01_binding_readback(binding, token, requested)


@pytest.mark.parametrize('field,value', [('reliability', 'best_effort'),
                                        ('durability', 'transient_local'),
                                        ('deadline_ns', 12345),
                                        ('liveliness', 'manual_by_topic'),
                                        ('liveliness_lease_duration_ns', 12345)])
def test_available_graph_rxo_fields_are_independently_reconciled(field, value):
    token, binding, requested = fixture_binding()
    binding['observed_self'][field] = value
    with pytest.raises(RelationSurfaceError, match='graph_qos_mismatch'):
        g01_binding_readback(binding, token, requested)
