"""Offline producer contract: retain child-observed type, never fill from input.

The process double never starts a subprocess/ROS/DDS or qualifies execution.
"""
from types import SimpleNamespace

import pytest

from r2f.runtime import topology_worker as tw
from test_topology_worker import spec


@pytest.mark.parametrize('observed', ['std_msgs/msg/String', None, 'observed/Other'])
def test_real_backend_handle_retains_raw_message_type(tmp_path, monkeypatch, observed):
    class ProcessDouble:
        def __init__(self, config, *_):
            self.process = SimpleNamespace(pid=424242)
            row = dict(event='endpoint_created', pid=424242, pgid=424242,
                       logical_id=config['logical_id'], generation=config['generation'],
                       endpoint_gid='aa11', endpoint_generation='fixture-generation',
                       process_start_ticks=17, fixture=True, real_ros=False)
            if observed is not None:
                row['message_type'] = observed
            self.events = [row]

        def start(self):
            pass

        def wait(self, predicate, *_):
            assert predicate()

        def has(self, event, **fields):
            return any(row['event'] == event and all(row.get(k) == v for k, v in fields.items())
                       for row in self.events)

        def close(self):
            return {'closed': True}

    backend = tw.RosTopologyBackend(
        {'run_id': 'offline-type-binding', 'domain_id': 191, 'rmw': 'fixture_no_rmw'}, tmp_path)
    backend.environment = {}  # No prepare or real runtime.
    monkeypatch.setattr(tw, '_EndpointProcess', ProcessDouble)
    monkeypatch.setattr(backend, '_start_ticks', lambda _: 17)
    handle = backend.create_endpoint(spec(), 1, 'writer')
    assert 'message_type' in handle
    assert handle['message_type'] == observed
    assert handle['type_name'] == 'std_msgs/msg/String'  # Requested type stays distinct.
