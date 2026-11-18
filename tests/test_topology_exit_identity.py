"""Producer/consumer identity regression using NON-ROS process and emit doubles.

No subprocess, ROS, DDS or namespace is started. Passing lineage checks here is
not real-execution qualification and does not repair any historical artifact.
"""
import ast
from copy import deepcopy
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.campaign import replay
from r2f.runtime import topology_worker as tw
from test_topology_replay import worker_artifacts


IDENTITY = ('logical_id', 'generation', 'endpoint_gid', 'pid', 'pgid',
            'process_start_ticks', 'endpoint_generation')


class ProcessDouble:
    def __init__(self, token):
        self.token = deepcopy(token)
        self.events = [dict(self.token, event='endpoint_created')]
        self.process = SimpleNamespace(pid=token['pid'], poll=lambda: 0)

    def start(self):
        return dict(self.token)

    def has(self, event, **fields):
        return any(row['event'] == event and all(row.get(k) == v for k, v in fields.items())
                   for row in self.events)

    def wait(self, predicate, *_):
        assert predicate()

    def close(self):
        return {'closed': True, 'returncode': 0, 'pid': self.token['pid'],
                'pgid': self.token['pgid']}


def backend_with_tokens(tmp_path, monkeypatch, tokens):
    rows = []
    backend = tw.RosTopologyBackend(
        {'run_id': 'exit-fixture', 'domain_id': 81, 'rmw': 'rmw_cyclonedds_cpp'},
        tmp_path, lambda event, **fields: rows.append(dict(event=event, **fields)))
    backend.environment = {}  # Explicit fixture only; never calls prepare/live entry.
    tokens = iter(tokens)
    monkeypatch.setattr(tw, '_EndpointProcess', lambda *_: ProcessDouble(next(tokens)))
    monkeypatch.setattr(tw, 'endpoint_expressibility', lambda *_: {
        'public_qos': {}, 'initial_content_filter': None, 'binding': {}})
    monkeypatch.setattr(backend, '_start_ticks', lambda pid: next(
        p.token['process_start_ticks'] for p in backend.processes if p.process.pid == pid))
    return backend, rows


def create(backend, token):
    return backend.create_endpoint(
        {'logical_id': token['logical_id'], 'role': token['role'], 'topic': '/fixture',
         'type_name': 'std_msgs/msg/String', 'qos': {}},
        token['generation'], token['worker_role'])


@pytest.mark.parametrize('path', ['destroy', 'close'])
def test_exit_equals_accepted_creation_token_and_controller_stays_controller(tmp_path, monkeypatch, path):
    config, result = worker_artifacts(tmp_path / 'authored-artifacts')
    tokens = [result['events'][w['token_raw_ref']['event_index']]
              for w in result['execution_metadata']['workers']]
    for index, token in enumerate(tokens):
        old_gid = token['endpoint_gid']
        new_gid = format(index + 1, '032x')
        for row in result['events']:
            if row.get('endpoint_gid') == old_gid:
                row['endpoint_gid'] = new_gid
        result['execution_metadata']['workers'][index]['endpoint_gid'] = new_gid
    backend, rows = backend_with_tokens(tmp_path / 'backend', monkeypatch, tokens)
    handles = [create(backend, token) for token in tokens]
    if path == 'destroy':
        for handle in handles:
            backend.destroy_endpoint(handle)
    assert backend.close()['closed']
    assert backend.close()['already_closed']
    for token in tokens:
        exits = [r for r in rows if r['event'] in ('worker_exit', 'topology_worker_exit')
                 and r['logical_id'] == token['logical_id']]
        assert len(exits) == 2
        for row in exits:
            assert {k: row[k] for k in IDENTITY} == {k: token[k] for k in IDENTITY}
        # Feed the actual producer rows to the unchanged consumer, not a copied
        # emitter implementation. The surrounding artifacts are authored fixtures.
        worker = next(w for w in result['execution_metadata']['workers']
                      if w['logical_id'] == token['logical_id'])
        for row in exits:
            row = dict(row, event_index=len(result['events']))
            result['events'].append(row)
            worker['exit_raw_refs'].append({'raw_path': result['raw_path'],
                                           'event_index': row['event_index']})
    assert replay._topology_worker_errors(config, result) == []
    controller = next(r for r in rows if r['event'] == 'topology_backend_closed')
    assert controller['pid'] == tw.os.getpid()
    assert controller['pgid'] == tw.os.getpgrp()
    assert controller['role'] == 'topology_backend'


def test_destroy_recreate_same_logical_id_and_new_generation_do_not_cross(tmp_path, monkeypatch):
    _, result = worker_artifacts(tmp_path / 'authored-artifacts')
    token = deepcopy(result['events'][result['execution_metadata']['workers'][0]['token_raw_ref']['event_index']])
    newer = dict(token, generation=token['generation'] + 1,
                 worker_generation=token['generation'] + 1, pid=token['pid'] + 10,
                 pgid=token['pgid'] + 10, process_start_ticks=token['process_start_ticks'] + 10,
                 endpoint_gid='cd' * 16, endpoint_generation='fixture-next-epoch')
    # create_endpoint validates GUID syntax; use authored hexadecimal GUIDs.
    token['endpoint_gid'] = 'ab' * 16
    backend, rows = backend_with_tokens(tmp_path / 'backend', monkeypatch, [token, newer])
    old_handle = create(backend, token)
    backend.destroy_endpoint(old_handle)
    new_handle = create(backend, newer)
    assert not old_handle['alive'] and new_handle['alive']
    assert backend.close()['closed']
    exits = [r for r in rows if r['event'] == 'worker_exit']
    assert [{k: r[k] for k in IDENTITY} for r in exits] == [
        {k: t[k] for k in IDENTITY} for t in (token, newer)]


@pytest.mark.parametrize('kind', ['worker_exit', 'topology_worker_exit'])
@pytest.mark.parametrize('key', IDENTITY)
@pytest.mark.parametrize('damage', ['missing', 'forged'])
def test_exit_identity_damage_still_fails_closed(tmp_path, key, damage, kind):
    config, result = worker_artifacts(tmp_path / 'authored-artifacts')
    worker = result['execution_metadata']['workers'][0]
    token = result['events'][worker['token_raw_ref']['event_index']]
    handle = {k: deepcopy(token[k]) for k in IDENTITY}
    handle.update(role='writer', process=ProcessDouble(token), alive=True)
    rows = []
    backend = tw.RosTopologyBackend({}, tmp_path / 'backend',
                                   lambda event, **fields: rows.append(dict(event=event, **fields)))
    if damage == 'missing':
        del handle[key]
    else:
        handle[key] = handle[key] + 1 if isinstance(handle[key], int) else handle[key] + '-forged'
    backend.destroy_endpoint(handle)
    row = next(r for r in rows if r['event'] == kind)
    row['event_index'] = len(result['events'])
    result['events'].append(row)
    worker['exit_raw_refs'].append({'raw_path': result['raw_path'], 'event_index': row['event_index']})
    assert replay._topology_worker_errors(config, result)


def test_actual_pair_emitter_keeps_controller_defaults_separate_from_worker(tmp_path):
    # Execute only the existing nested emit function, extracted from its AST;
    # never run the surrounding topology runner, preflight or leases.
    from r2f.runtime import topology_pair
    tree = ast.parse(Path(topology_pair.__file__).read_text())
    runner = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'run_topology')
    emitter = next(n for n in runner.body if isinstance(n, ast.FunctionDef) and n.name == 'emit')
    events = []
    namespace = dict(time=tw.time, os=tw.os, deepcopy=deepcopy, json=json, events=events,
                     raw=io.StringIO(), cfg={'run_id': 'emit-only-fixture', 'domain_id': 81},
                     backend=SimpleNamespace(backend_name=tw.ROS_BACKEND, real_ros=True), result={})
    exec(compile(ast.Module(body=[emitter], type_ignores=[]), str(topology_pair.__file__), 'exec'), namespace)
    emit = namespace['emit']
    _, fixture = worker_artifacts(tmp_path / 'authored-artifacts')
    token = fixture['events'][fixture['execution_metadata']['workers'][0]['token_raw_ref']['event_index']]
    backend = tw.RosTopologyBackend({}, tmp_path / 'backend', emit)
    handle = dict(token, process=ProcessDouble(token), alive=True)
    backend.destroy_endpoint(handle)
    worker_exit = next(r for r in events if r['event'] == 'worker_exit')
    assert {k: worker_exit[k] for k in IDENTITY} == {k: token[k] for k in IDENTITY}
    controller = emit('controller_fixture_only')
    assert controller['role'] == 'topology_controller'
    assert controller['pid'] == tw.os.getpid()
    assert controller['pgid'] == tw.os.getpgrp()
