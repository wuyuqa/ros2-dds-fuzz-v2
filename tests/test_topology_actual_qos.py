"""Synthetic public readback parser checks; no ROS, namespace or live credit."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch
import pytest
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend
from r2f.runtime.topology_route import audit_topology_application
from r2f.runtime.topology_operator_application import audit_operator_application
from r2f.runtime.qos_binding import compare_public_qos, INFINITE_NS
from test_topology_operator_application import _config, _rewrite_raw
from test_topology_deadline_application import _synthetic_api_chain


def raw_default_case(tmp_path):
    request={'operator_id':'param.boundary','slot_id':'S1.topology.qos.lease',
             'context':{'boundary_class':'zero'}}
    plan,config=_config([request],seed=11,writers=2,readers=1)
    with patch('r2f.runtime.topology_worker.RosTopologyBackend.prepare',side_effect=AssertionError('ROS forbidden')):
        runtime=run_pair(config,tmp_path,topology_backend=OfflineTopologyBackend())
    _synthetic_api_chain(plan,runtime)
    def actual(events):
        for token in events:
            if token['event']!='endpoint_created': continue
            requested=deepcopy(token['qos']); effective=deepcopy(requested)
            for field in ('deadline_ns','liveliness_lease_duration_ns'):
                if effective[field]==0: effective[field]=INFINITE_NS
            token.update(configuration_source='public_endpoint_actual_qos',requested_qos=requested,
                         qos=effective,graph_qos=dict(effective,depth=0),
                         actual_endpoint_qos=dict(schema='r2f.public-endpoint-qos/v1',status='observed',
                            api='rcl_publisher_get_actual_qos' if token['role'].startswith('writer') else 'rcl_subscription_get_actual_qos',
                            qos=deepcopy(effective),pid=token['pid'],sampled_monotonic_ns=token['created_monotonic_ns']+1,
                            library=dict(path='/fixture/librcl.so',sha256='a'*64),abi=dict(verified=True,fixture=True)),
                         qos_request_comparison=compare_public_qos(requested,effective))
        return events
    _rewrite_raw(runtime,actual)
    return plan,config,runtime


def audit(plan,config,runtime):
    events=[json.loads(line) for line in Path(runtime['raw_path']).read_text().splitlines()]
    return audit_operator_application(config['input_layer_receipts'],events,plan.final_config['topology_program'],fixture=False)[0]


def test_default_lease_is_resolved_not_literal_zero_or_graph_depth(tmp_path):
    plan,config,runtime=raw_default_case(tmp_path)
    record=audit(plan,config,runtime)
    assert record['reasons']==[],record
    assert record['configuration_application_observed']
    assert record['qos_default_resolutions']
    assert not record['hit'] and not record['effect_observed'] and not record['candidate_eligible']
    route=audit_topology_application(plan,config['input_layer_receipts'],runtime)
    assert route['status']!='invalid',route
    assert route['layers'][0].get('qos_default_resolutions')


@pytest.mark.parametrize('damage',['pid','qos','requested','sample','api','receipt','resolution'])
def test_new_effective_schema_never_waives_missing_or_mismatched_receipts(tmp_path,damage):
    plan,config,runtime=raw_default_case(tmp_path)
    def corrupt(events):
        token=next(e for e in events if e['event']=='endpoint_created' and e['requested_qos']['liveliness_lease_duration_ns']==0)
        if damage=='pid': token['actual_endpoint_qos']['pid']+=1
        elif damage=='qos': token['qos']['depth']+=1
        elif damage=='requested': token['requested_qos']['liveliness_lease_duration_ns']=1
        elif damage=='sample': token['actual_endpoint_qos']['sampled_monotonic_ns']=1
        elif damage=='api': token['actual_endpoint_qos']['api']='configuration_echo'
        elif damage=='receipt': token.pop('actual_endpoint_qos')
        elif damage=='resolution': token['qos_request_comparison']['default_resolutions']=[]
        return events
    _rewrite_raw(runtime,corrupt)
    record=audit(plan,config,runtime)
    assert record['reasons'],record
    assert not record['configuration_application_observed'] and not record['hit']
