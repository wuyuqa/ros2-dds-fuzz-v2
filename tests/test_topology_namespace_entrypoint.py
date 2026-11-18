"""Real Python entrypoints with explicit offline doubles; no ROS or netns launch.

The child handshake/kernel measurements are doubles, NOT execution evidence.
Only the two production pair callback blocks are executed (extracted by AST),
not a real pair campaign. This isolates Python module identity from DDS gates.
"""
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import textwrap

import pytest


MODULE = 'r2f.runtime.topology_namespace'
REPO = Path(__file__).resolve().parents[1]

# Loaded by a fresh interpreter before -m resolves __main__. Deliberately do not
# import topology_namespace here: the first canonical import happens in the
# entrypoint (fixed) or in the pair callback (the original regression).
OFFLINE_BOOTSTRAP = r'''
import ast
import atexit
import json
import importlib.abc
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

class NoRosImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'rclpy', 'rmw', 'rosidl_runtime_py'}:
            raise AssertionError('offline test must not import ROS: ' + fullname)

sys.meta_path.insert(0, NoRosImports())
def no_process(*args, **kwargs):
    raise AssertionError('offline child must not launch ROS/netns/workers')
subprocess.Popen = no_process
from r2f.runtime import execution_boundary as boundary
from r2f.runtime import ros2_pair, topology_route, topology_pair

proof = {'identity': {'start_ticks': 42, 'namespaces': {'pid': 'fixture-pidns'}}}
boundary._nested_proc_scope = lambda outer: proof
boundary._same_source = lambda source: source
boundary._dumpable = lambda: None
boundary._identity = lambda pid: proof['identity']
boundary._ticks = lambda pid: 42
boundary._send = lambda *args: None
acks = iter(({'kind': 'execute', 'config_sha256': boundary._hash({'lock_root': '/fixture-locks'})},
             {'kind': 'result_ack'}))
boundary._recv = lambda *args: (next(acks), (0, 0, 0), [])
topology_route.validate_topology_route = lambda config: None
# route_requested imports ROS_BACKEND but does not initialize ROS.
topology_route.topology_route_requested = lambda config: True
ros2_pair.verify_pair_lease = lambda *args: [{'fixture_lease': i} for i in range(3)]

# Keep the actual relative imports and dispatch in BOTH production callsites.
# Discover them structurally, rather than duplicating their implementation.
tree = ast.parse(Path(topology_pair.__file__).read_text())
blocks = {}
for node in ast.walk(tree):
    for field, value in ast.iter_fields(node):
        if not isinstance(value, list):
            continue
        for index, statement in enumerate(value):
            if (isinstance(statement, ast.ImportFrom)
                    and statement.module == 'topology_namespace'
                    and [a.name for a in statement.names] == ['current_child_context']):
                block = value[index:index + 3]
                calls = [n for st in block for n in ast.walk(st)
                         if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                         and n.func.attr in ('leases_ready', 'worker_created')]
                assert len(calls) == 1, 'pair callback shape changed'
                name = calls[0].func.attr
                assert name not in blocks, 'duplicate callback site'
                blocks[name] = compile(ast.Module(body=block, type_ignores=[]),
                                       topology_pair.__file__, 'exec')
assert set(blocks) == {'leases_ready', 'worker_created'}

def offline_pair(config, output):
    from r2f.runtime import topology_namespace as canonical
    ctx = canonical.current_child_context()
    assert type(ctx) is canonical._ChildContext, 'collector not installed in canonical module'
    assert ctx.lease_scope is None and ctx.worker_bindings == []
    events = []
    def emit(event, **fields):
        row = {'event': event, 'event_index': len(events), **fields}
        events.append(row)
        return row
    scope = {'__package__': topology_pair.__package__, 'lease': object(),
             'cfg': config, 'output_dir': output, 'emit': emit}
    exec(blocks['leases_ready'], scope)
    assert scope['namespace_context'] is ctx
    assert ctx.lease_scope['raw_event_ref'] == 0
    for number, role in enumerate(('writer', 'reader')):
        process = SimpleNamespace(pid=7100 + number, poll=lambda: None)
        scope['handle'] = {'process': SimpleNamespace(process=process), 'pid': process.pid,
                           'process_start_ticks': 42, 'logical_id': role, 'generation': 1,
                           'endpoint_gid': role + '-fixture-gid', 'endpoint_generation': 1}
        exec(blocks['worker_created'], scope)
        assert scope['namespace_context'] is ctx
    assert [row['event'] for row in events] == [
        'topology_namespace_ready', 'topology_namespace_worker_bound', 'topology_namespace_worker_bound']
    assert [row['raw_event_ref'] for row in ctx.worker_bindings] == [1, 2]
    assert canonical.current_child_context() is ctx
    return {'fixture': True, 'real_execution': False, 'raw_path': None,
            'callback_events': events}

ros2_pair.run_pair = offline_pair

def verify_cleared():
    from r2f.runtime import topology_namespace as canonical
    assert canonical.current_child_context() is None, 'collector leaked after child exit'
    print('OFFLINE_CANONICAL_CONTEXT_CLEARED')
atexit.register(verify_cleared)
'''


def child_env(tmp_path, *, bootstrap=False):
    env = os.environ.copy()
    paths = [str(REPO / 'src')]
    if bootstrap:
        (tmp_path / 'sitecustomize.py').write_text(textwrap.dedent(OFFLINE_BOOTSTRAP))
        paths.insert(0, str(tmp_path))
    env['PYTHONPATH'] = os.pathsep.join(paths)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env


@pytest.mark.parametrize('entrypoint', ['-m', 'run_module'])
def test_child_entrypoint_shares_canonical_pair_collector(tmp_path, entrypoint):
    side = tmp_path / 'topology-namespace'
    side.mkdir()
    # Route and kernel checks have explicit doubles; module identity does not.
    config = {'lock_root': '/fixture-locks',
              'execution_boundary': {'enabled': True}, 'backend': 'ros2_public_api'}
    from r2f.runtime.execution_boundary import _hash
    request = {'output_root': str(tmp_path), 'config': config,
               'config_sha256': _hash(config),
               'source_manifest': {'offline_fixture': True}, 'outer': {},
               'options': {'startup_timeout_sec': 1, 'kill_wait_sec': 1}, 'bridge_id': 'b' * 32}
    path = side / 'request.json'
    path.write_text(json.dumps(request))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    env = child_env(tmp_path, bootstrap=True)
    boot = tmp_path / 'sitecustomize.py'
    boot.write_text(boot.read_text().replace(
        "boundary._hash({'lock_root': '/fixture-locks'})", repr(request['config_sha256'])))
    parent, child = socket.socketpair()
    try:
        args = ['_child', str(path), digest, str(child.fileno())]
        if entrypoint == '-m':
            command = [sys.executable, '-B', '-m', MODULE, *args]
        else:
            command = [sys.executable, '-B', '-c',
                       "import runpy, sys; sys.argv = [sys.argv[1], *sys.argv[2:]]; "
                       "runpy.run_module(sys.argv[0], run_name='__main__')", MODULE, *args]
        completed = subprocess.run(command, env=env, cwd=REPO,
                                   pass_fds=(child.fileno(),), capture_output=True,
                                   text=True, timeout=20)
    finally:
        parent.close()
        child.close()
    failure = side / 'child-failure.json'
    assert completed.returncode == 0, (completed.stdout, completed.stderr,
                                      failure.read_text() if failure.exists() else None)
    assert not failure.exists()
    assert 'OFFLINE_CANONICAL_CONTEXT_CLEARED' in completed.stdout
    assert not completed.stderr
    envelope = json.loads((side / 'child-envelope.json').read_text())
    assert envelope['inner_lease_scope']['pid_scope'] == 'fixture-pidns'
    assert envelope['inner_lease_scope']['raw_event_ref'] == 0
    assert len(envelope['inner_lease_scope']['receipts']) == 3
    assert [row['logical_id'] for row in envelope['worker_bindings']] == ['writer', 'reader']
    assert [row['raw_event_ref'] for row in envelope['worker_bindings']] == [1, 2]
    runtime = json.loads((side / 'child-runtime-result.json').read_text())
    assert runtime['fixture'] and not runtime['real_execution']
    assert envelope['raw_ref'] is None  # offline callbacks are not live raw proof


@pytest.mark.parametrize('args', [[], ['other', 'request', 'sha', '4'],
                                  ['_child', 'request', 'sha'],
                                  ['_child', 'request', 'sha', '4', 'extra']])
def test_module_entrypoint_preserves_fixed_argv_guard(tmp_path, args):
    completed = subprocess.run([sys.executable, '-B', '-m', MODULE, *args],
                               env=child_env(tmp_path), cwd=REPO, capture_output=True,
                               text=True, timeout=20)
    assert completed.returncode != 0
    assert completed.stderr.strip() == 'only fixed canonical child dispatch is supported'


def test_module_entrypoint_rejects_noninteger_descriptor(tmp_path):
    completed = subprocess.run([sys.executable, '-B', '-m', MODULE,
                               '_child', 'request', 'sha', 'not-an-fd'],
                               env=child_env(tmp_path), cwd=REPO, capture_output=True,
                               text=True, timeout=20)
    assert completed.returncode != 0
    assert 'ValueError' in completed.stderr
    assert 'invalid literal for int()' in completed.stderr
