"""Offline worker tokens at the public ROS/native boundary; no DDS execution."""
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest


@pytest.mark.parametrize('layers,leases,custom,rmw', [
    (False, False, False, 'rmw_cyclonedds_cpp'),
    (True, False, False, 'rmw_cyclonedds_cpp'),
    (False, True, False, 'rmw_cyclonedds_cpp'),
    (True, True, True, 'rmw_cyclonedds_cpp'),
    (True, True, False, 'rmw_fastrtps_cpp'),
])
def test_worker_preserves_both_opt_in_ownership_observations(monkeypatch, capsys, tmp_path,
                                                            layers, leases, custom, rmw):
    from r2f.runtime import worker, topology_worker

    class Finished(Exception):
        pass

    class Context:
        def shutdown(self):
            pass

    class Executor:
        def __init__(self, **kwargs):
            pass
        def add_node(self, node):
            pass
        def spin_once(self, **kwargs):
            raise Finished()
        def shutdown(self):
            pass

    class Node:
        def __init__(self, name, **kwargs):
            self.name = name
        def create_publisher(self, *args):
            return object()
        def get_publishers_info_by_topic(self, topic):
            return [SimpleNamespace(node_name=self.name, node_namespace='/', topic_type='std_msgs/msg/String',
                endpoint_gid=bytes.fromhex('01'*16), qos_profile=SimpleNamespace(
                    reliability=SimpleNamespace(name='RELIABLE'),
                    durability=SimpleNamespace(name='VOLATILE'), depth=7))]
        def get_subscriptions_info_by_topic(self, topic):
            return []
        def destroy_node(self):
            pass

    modules = {}
    for name in ('rclpy', 'rclpy.context', 'rclpy.executors', 'rclpy.node', 'rclpy.qos',
                 'rclpy.duration', 'std_msgs', 'std_msgs.msg',
                 'ament_index_python', 'ament_index_python.packages'):
        modules[name] = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, modules[name])
    modules['rclpy'].init = lambda **kwargs: None
    modules['rclpy'].get_rmw_implementation_identifier = lambda: rmw
    modules['rclpy.context'].Context = Context
    modules['rclpy.executors'].SingleThreadedExecutor = Executor
    modules['rclpy.node'].Node = Node
    modules['rclpy.qos'].QoSProfile = lambda **kwargs: SimpleNamespace(**kwargs)
    for name, values in (('ReliabilityPolicy', {'RELIABLE': 1}),
                         ('DurabilityPolicy', {'VOLATILE': 0}),
                         ('HistoryPolicy', {'KEEP_LAST': 0}), ('LivelinessPolicy', {})):
        setattr(modules['rclpy.qos'], name, SimpleNamespace(**values))
    modules['rclpy.duration'].Duration = lambda **kwargs: SimpleNamespace(**kwargs)
    modules['std_msgs.msg'].String = object
    modules['ament_index_python.packages'].get_package_prefix = lambda name: str(tmp_path)
    constructors = []
    class Native:
        def __init__(self, **kwargs):
            constructors.append(kwargs)
        def sample_ownership(self, domain, gid, kind):
            assert (domain, gid, kind) == (42, '01'*16, 'writer')
            return {'status': 'observed', 'complete': True, 'offline_fixture': True}
    monkeypatch.setattr(topology_worker, 'CycloneMatchedPeerObserver', Native)
    config = {'run_id': 'offline-ownership', 'domain_id': 42, 'rmw': rmw,
        'qos': {'depth': 7, 'reliability': 'reliable', 'durability': 'volatile'},
        'topic': '/test', 'startup_timeout_sec': 1,
        'projection_discovery_layers': layers, 'observe_participant_leases': leases}
    reference = {'path': '/offline/sut.json', 'sha256': 'a'*64}
    if custom:
        config['projection_sut_environment'] = reference
    monkeypatch.setenv('RMW_IMPLEMENTATION', rmw)
    monkeypatch.setattr(sys, 'argv', ['worker', json.dumps(config), 'writer2'])
    with pytest.raises(Finished):
        worker.main()
    tokens = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    endpoint, = [row for row in tokens if row['event'] == 'endpoint_created']
    selected = (layers or leases) and rmw == 'rmw_cyclonedds_cpp'
    assert ('participant_ownership' in endpoint) is selected
    assert constructors == ([{'sut_reference': reference} if custom else {}] if selected else [])
    if selected:
        assert endpoint['participant_ownership']['status'] == 'observed'
    assert tokens[-1]['event'] == 'worker_shutdown'
