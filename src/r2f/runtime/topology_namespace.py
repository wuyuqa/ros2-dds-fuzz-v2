"""Authenticated nested controller for the canonical isolated topology route.

Not a runner or qualification authority. The real execution-boundary PID1 calls
this module after its original guardian handshake/preflight. Host/injected
backends never enter here. Child raw/result bytes are immutable; the parent adds
only an independently authenticated, hashed sidecar descriptor. No environment
flag or fixed nonDDS fixture can provide production admission.
"""
from __future__ import annotations

from copy import deepcopy
import contextlib
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import time
import uuid

from . import execution_boundary as boundary
from .discovery_network import namespace_command

MODULE = 'r2f.runtime.topology_namespace'
SCHEMA = 'r2f.topology-namespace/v1'
MANIFEST_SCHEMA = 'r2f.topology-namespace-manifest/v1'
AUTHENTICATION = 'SCM_CREDENTIALS+SCM_RIGHTS+pidfd+proc_start_ticks+NS_GET_NSTYPE'
CLEANUP_METHOD = 'pidfd-bound_nested_PID1_exit+unshare_wait+kernel_PIDNS_teardown'
_ACTIVE_CHILD = None
_WORKER_KEYS = ('logical_id', 'generation', 'endpoint_gid', 'endpoint_generation', 'pid', 'process_start_ticks')


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _ref(path, kind='json'):
    return {'path': str(Path(path).resolve()), 'sha256': _sha(path), 'kind': kind}


def _load_ref(ref, root, *, kind='json'):
    path = Path(ref['path'])
    resolved = path.resolve(strict=True)
    if (not path.is_absolute() or not resolved.is_relative_to(Path(root).resolve())
            or resolved == Path(root).resolve() or ref.get('kind') != kind
            or _sha(path) != ref.get('sha256')):
        raise boundary.BoundaryError('topology_namespace_artifact_path_or_hash_mismatch')
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != ref['sha256']:
        raise boundary.BoundaryError('topology_namespace_artifact_changed_during_read')
    return [json.loads(line) for line in data.splitlines()] if kind == 'jsonl' else json.loads(data)


def route_requested(config):
    """Only a routing request. Caller still needs all independent kernel gates."""
    from .topology_route import topology_route_requested
    from .topology_worker import ROS_BACKEND
    return (topology_route_requested(config)
            and isinstance(config.get('execution_boundary'), dict)
            and config['execution_boundary'].get('enabled') is True
            and config.get('backend') in (None, ROS_BACKEND))


def _outer_gate(config, request):
    from .topology_route import validate_topology_route
    actual = boundary._identity()
    mapping = [[int(v) for v in row.split()] for row in actual['uid_map'].splitlines()]
    if (actual['uid'] != 1000 or actual['cap_eff'] != 0 or os.getpid() != 1
            or actual['nspid'][-1] != 1 or mapping != [[1000, 1000, 1]]):
        raise boundary.BoundaryError('topology_namespace_outer_not_uid1000_cap0_pid1')
    if not route_requested(config) or config != request['config']:
        raise boundary.BoundaryError('topology_namespace_not_canonical_boundary_request')
    for name in boundary.NAMESPACES:
        if actual['namespaces'][name] == request['host']['namespaces'][name]:
            raise boundary.BoundaryError('topology_namespace_outer_not_independent:' + name)
    if boundary._hash(config) != request['inner_config_sha256']:
        raise boundary.BoundaryError('topology_namespace_outer_config_hash_mismatch')
    boundary._same_source(request['identity']['source_manifest'])
    validate_topology_route(config)
    return actual


def _bind_controller(packet, credentials, fds, outer, wrapper_pid):
    """Parent reads kernel identities in OUTER PIDNS, never calls them host PIDs."""
    if packet.get('kind') != 'ready' or len(fds) != 5:
        raise boundary.BoundaryError('topology_namespace_proof_fd_count_or_kind')
    pid, uid, gid = credentials
    if (pid != boundary._pidfd_pid(fds[0]) or uid != 1000 or gid != os.getgid()
            or boundary._parent_of(pid) != wrapper_pid or boundary._exited(fds[0])):
        raise boundary.BoundaryError('topology_namespace_credentials_pidfd_parent_mismatch')
    before = boundary._ticks(pid)
    actual = boundary._identity(pid)
    reported = packet['proof']['identity']
    maps = lambda text: [[int(v) for v in row.split()] for row in text.splitlines()]
    if (actual['uid'] != 1000 or actual['nspid'] != [pid, 1]
            or reported['pid'] != 1 or reported['nspid'] != [1] or reported['uid'] != 0
            or before != actual['start_ticks'] or before != boundary._ticks(pid)
            or before != reported['start_ticks'] or actual['namespaces'] != reported['namespaces']
            or actual['cap_eff'] != reported['cap_eff'] or not actual['cap_eff'] & (1 << 12)
            or maps(actual['uid_map']) != [[0, 1000, 1]] or maps(actual['gid_map']) != [[0, os.getgid(), 1]]
            or maps(reported['uid_map']) != maps(actual['uid_map'])
            or maps(reported['gid_map']) != maps(actual['gid_map'])):
        raise boundary.BoundaryError('topology_namespace_kernel_identity_mismatch')
    for name, fd in zip(boundary.NAMESPACES, fds[1:]):
        local, remote = os.stat(f'/proc/{pid}/ns/{name}'), os.fstat(fd)
        if (boundary.fcntl.ioctl(fd, 0xb703) != boundary.CLONE_FLAGS[name]
                or (local.st_dev, local.st_ino) != (remote.st_dev, remote.st_ino)
                or actual['namespaces'][name] == outer['namespaces'][name]):
            raise boundary.BoundaryError('topology_namespace_nsfd_mismatch:' + name)
    return {'authentication': AUTHENTICATION, 'parent_pid_scope': outer['namespaces']['pid'],
            'outer': actual, 'reported_inner': reported, 'inner_pid': 1,
            'pid_scope': actual['namespaces']['pid'],
            'pid_namespace_chain': [{'pid_scope': outer['namespaces']['pid'], 'pid': pid},
                                    {'pid_scope': actual['namespaces']['pid'], 'pid': 1}]}


class _ChildContext:
    """Process-local receipt collector, installed only after real child handshake."""
    def __init__(self, request, proof):
        self.request, self.proof = request, proof
        self.lease_scope = None
        self.worker_bindings = []

    def leases_ready(self, lease, config, output_dir, emit):
        from .ros2_pair import verify_pair_lease
        from .topology_worker import TopologyBackendBlocked
        try:
            scope = boundary._nested_proc_scope(self.request['outer'])
            if scope['identity'] != self.proof['identity']:
                raise boundary.BoundaryError('topology_namespace_controller_identity_changed')
            receipts = verify_pair_lease(lease, config, output_dir)
            if Path(config['lock_root']).resolve() != Path(self.request['config']['lock_root']).resolve():
                raise boundary.BoundaryError('topology_namespace_inner_lock_root_changed')
            boundary._same_source(self.request['source_manifest'])
            self.lease_scope = {'pid_scope': scope['identity']['namespaces']['pid'],
                'controller_start_ticks': scope['identity']['start_ticks'], 'owner_pid': 1,
                'lock_root': str(Path(config['lock_root']).resolve()), 'state_dir': str(Path(output_dir).resolve()),
                'receipts': deepcopy(receipts)}
            row = emit('topology_namespace_ready', namespace=scope, lease_scope=self.lease_scope,
                       config_sha256=self.request['config_sha256'], execution_evidence=True)
            self.lease_scope['raw_event_ref'] = row['event_index']
        except (OSError, ValueError, KeyError, RuntimeError) as exc:
            raise TopologyBackendBlocked('topology_namespace_lease_admission:' + str(exc)) from exc

    def worker_created(self, handle, emit):
        from .topology_worker import TopologyBackendBlocked
        try:
            if self.lease_scope is None:
                raise boundary.BoundaryError('topology_namespace_worker_before_lease_admission')
            process = handle['process'].process
            if process.poll() is not None or process.pid != handle['pid']:
                raise boundary.BoundaryError('topology_namespace_worker_not_live_or_owned')
            actual = boundary._identity(process.pid)
            if (actual['start_ticks'] != handle['process_start_ticks']
                    or actual['start_ticks'] != boundary._ticks(process.pid)
                    or actual['namespaces'] != self.proof['identity']['namespaces']
                    or any(handle.get(k) is None for k in _WORKER_KEYS)):
                raise boundary.BoundaryError('topology_namespace_worker_kernel_binding_mismatch')
            binding = {**{k: handle[k] for k in _WORKER_KEYS}, 'namespaces': actual['namespaces'],
                       'outer_pid': None, 'host_pid': None,
                       'mapping_status': 'nested_kernel_bound; ancestor mapping unobserved unless independently sampled'}
            row = emit('topology_namespace_worker_bound', binding=binding, execution_evidence=True)
            binding['raw_event_ref'] = row['event_index']
            self.worker_bindings.append(binding)
        except (OSError, ValueError, KeyError, RuntimeError) as exc:
            raise TopologyBackendBlocked('topology_namespace_worker_admission:' + str(exc)) from exc


def current_child_context():
    # A caller-provided bool/dict, cfg/env proof or injected backend never installs this.
    return _ACTIVE_CHILD if type(_ACTIVE_CHILD) is _ChildContext else None


def _child(request_path, request_sha, fd):
    global _ACTIVE_CHILD
    control = socket.socket(fileno=fd)
    request = None
    try:
        path = Path(request_path).resolve(strict=True)
        request = _load_ref({'path': str(path), 'sha256': request_sha, 'kind': 'json'}, path.parent)
        output = Path(request['output_root']).resolve()
        if path != output / 'topology-namespace/request.json':
            raise boundary.BoundaryError('topology_namespace_child_request_path_mismatch')
        # The argv request SHA binds bridge identity to these exact bytes;
        # it is transport provenance, never a config/input field or kernel proof.
        bridge_id = request.get('bridge_id')
        if (type(bridge_id) is not str or len(bridge_id) != 32
                or any(c not in '0123456789abcdef' for c in bridge_id)):
            raise boundary.BoundaryError('topology_namespace_child_bridge_id_invalid')
        config = request['config']
        if boundary._hash(config) != request['config_sha256'] or not route_requested(config):
            raise boundary.BoundaryError('topology_namespace_child_config_mismatch')
        from .topology_route import validate_topology_route
        validate_topology_route(config)
        source = boundary._same_source(request['source_manifest'])
        boundary._write(path.parent / 'source-before.json', source)
        boundary._dumpable()
        proof = boundary._nested_proc_scope(request['outer'])
        handles = [os.pidfd_open(os.getpid())] + [os.open('/proc/self/ns/' + name, os.O_RDONLY | os.O_CLOEXEC)
                                                for name in boundary.NAMESPACES]
        try:
            boundary._send(control, {'kind': 'ready', 'proof': proof, 'config_sha256': request['config_sha256']}, handles)
        finally:
            for handle in handles:
                os.close(handle)
        ack, credentials, handles = boundary._recv(control, request['options']['startup_timeout_sec'])
        for handle in handles:
            os.close(handle)
        if (ack != {'kind': 'execute', 'config_sha256': request['config_sha256']}
                or credentials != (0, 0, 0)):
            raise boundary.BoundaryError('topology_namespace_child_execute_not_authenticated')
        # All real ROS endpoints below are descendants of this one PID1. The
        # canonical runner/route performs source/preflight/three leases normally.
        _ACTIVE_CHILD = _ChildContext(request, proof)
        from .ros2_pair import run_pair
        runtime = run_pair(config, output)
        boundary._write(path.parent / 'child-runtime-result.json', runtime)
        source = boundary._same_source(request['source_manifest'])
        boundary._write(path.parent / 'source-after.json', source)
        envelope = {'inner_result_ref': _ref(path.parent / 'child-runtime-result.json'),
                    'source_before_ref': _ref(path.parent / 'source-before.json'),
                    'source_after_ref': _ref(path.parent / 'source-after.json'),
                    'inner_lease_scope': _ACTIVE_CHILD.lease_scope,
                    'worker_bindings': _ACTIVE_CHILD.worker_bindings,
                    'raw_ref': _ref(runtime['raw_path'], 'jsonl') if runtime.get('raw_path') else None}
        boundary._write(path.parent / 'child-envelope.json', envelope)
        boundary._send(control, {'kind': 'result', 'envelope_sha256': _sha(path.parent / 'child-envelope.json')})
        ack, credentials, handles = boundary._recv(control, request['options']['kill_wait_sec'])
        for handle in handles:
            os.close(handle)
        if ack.get('kind') != 'result_ack' or credentials != (0, 0, 0):
            raise boundary.BoundaryError('topology_namespace_child_result_not_acknowledged')
        return 0
    except BaseException as exc:
        if request is not None:
            with contextlib.suppress(Exception):
                boundary._write(Path(request['output_root']) / 'topology-namespace/child-failure.json',
                                {'error': type(exc).__name__ + ':' + str(exc)})
        return 127
    finally:
        _ACTIVE_CHILD = None
        control.close()


def _receipt_refs(envelope, runtime, root, pid_scope):
    """Bind receipt offsets to immutable raw bytes; never annotate raw itself."""
    raw_ref = envelope.get('raw_ref')
    events = _load_ref(raw_ref, root, kind='jsonl') if raw_ref is not None else []
    if (events != runtime.get('events', []) or runtime.get('raw_sha256') != (raw_ref or {}).get('sha256')
            or runtime.get('raw_path') != (raw_ref or {}).get('path')):
        raise boundary.BoundaryError('topology_namespace_raw_result_mismatch')
    scope, workers = deepcopy(envelope.get('inner_lease_scope')), deepcopy(envelope.get('worker_bindings', []))
    def bind(ref, event):
        if type(ref) is not int or not 0 <= ref < len(events) or events[ref].get('event') != event:
            raise boundary.BoundaryError('topology_namespace_receipt_raw_ref_invalid')
        return {'path': raw_ref['path'], 'sha256': raw_ref['sha256'], 'event_index': ref}
    if scope is not None:
        if scope['pid_scope'] != pid_scope or scope['owner_pid'] != 1 or len(scope['receipts']) != 3:
            raise boundary.BoundaryError('topology_namespace_lease_scope_mismatch')
        offset = scope['raw_event_ref']
        scope['raw_event_ref'] = bind(offset, 'topology_namespace_ready')
        if events[offset].get('lease_scope') != {k: v for k, v in scope.items() if k != 'raw_event_ref'}:
            raise boundary.BoundaryError('topology_namespace_lease_raw_bytes_mismatch')
    for row in workers:
        offset = row['raw_event_ref']
        row['raw_event_ref'] = bind(offset, 'topology_namespace_worker_bound')
        if (row['namespaces']['pid'] != pid_scope
                or events[offset].get('binding') != {k: v for k, v in row.items() if k != 'raw_event_ref'}):
            raise boundary.BoundaryError('topology_namespace_worker_raw_bytes_mismatch')
    if runtime.get('real_execution') is True:
        actual = [w for w in (runtime.get('execution_metadata') or {}).get('workers', []) if w.get('real_ros')]
        if scope is None or not actual or len(actual) != len(workers):
            raise boundary.BoundaryError('topology_namespace_missing_actual_worker_bindings')
        for worker in actual:
            matches = [w for w in workers if all(worker.get(k) == w.get(k) for k in _WORKER_KEYS)]
            if len(matches) != 1:
                raise boundary.BoundaryError('topology_namespace_worker_generation_unbound')
    return scope, workers


def _cleanup(proc, init_fd, options):
    """Own subtree only. Keep parent/guardian host leases until teardown proof."""
    if proc is None:
        return {'confirmed': False, 'controller_exited': False, 'wrapper_returncode': None, 'method': CLEANUP_METHOD}
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=options['term_grace_sec'])
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            # Bound PID1 even if unshare failed to forward a group signal.
            if init_fd is not None:
                boundary._signal_pidfd(init_fd, signal.SIGKILL)
            proc.wait(timeout=options['kill_wait_sec'])
    if init_fd is not None and not boundary._exited(init_fd):
        boundary._signal_pidfd(init_fd, signal.SIGKILL)
        select.select([init_fd], [], [], options['kill_wait_sec'])
    exited = init_fd is not None and boundary._exited(init_fd)
    # As outer PID1, reap adopted nested descendants, never unrelated host PIDs.
    while True:
        try:
            waited, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if not waited:
            break
    return {'confirmed': exited and proc.returncode is not None, 'controller_exited': exited,
            'wrapper_returncode': proc.returncode, 'method': CLEANUP_METHOD}


def run_isolated_topology(config, output_dir, *, boundary_request):
    """Called by authenticated execution_boundary._inner, not host run_pair."""
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    side = output / 'topology-namespace'
    # Never overwrite a prior failure or successful result.
    side.mkdir(mode=0o700)
    record = {'schema': MANIFEST_SCHEMA, 'bridge_id': uuid.uuid4().hex, 'output_root': str(output),
              'host_clearance_claimed': False, 'maturity_credit': False, 'error': None,
              'nested_controller_binding': None, 'raw_pid_scope': None, 'inner_lease_scope': None,
              'worker_bindings': [], 'parent_process_observations': []}
    result = boundary._blocked('blocked:topology_namespace:not_started')
    proc, parent, child, fds, complete = None, None, None, [], False
    options = boundary_request['options']
    observations = {}
    try:
        outer = _outer_gate(config, boundary_request)
        record['outer_controller'] = outer
        record['source_manifest'] = boundary_request['identity']['source_manifest']
        record['config_sha256'] = boundary._hash(config)
        request = {'bridge_id': record['bridge_id'], 'config': deepcopy(config),
                   'config_sha256': record['config_sha256'], 'outer': outer,
                   'source_manifest': record['source_manifest'], 'output_root': str(output), 'options': options}
        boundary._write(side / 'request.json', request)
        record['request_ref'] = _ref(side / 'request.json')
        parent, child = boundary._pair()
        command = namespace_command([sys.executable, '-B', '-m', MODULE, '_child', str(side / 'request.json'),
                                     record['request_ref']['sha256'], str(child.fileno())], pid_mount=True)
        record['command'] = command
        with (side / 'stdout.log').open('xb') as stdout, (side / 'stderr.log').open('xb') as stderr:
            proc = subprocess.Popen(command, pass_fds=(child.fileno(),), start_new_session=True,
                                    stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                    env=boundary._python_env(), cwd=boundary.REPO)
        child.close()
        packet, peer, fds = boundary._recv(parent, options['startup_timeout_sec'])
        binding = _bind_controller(packet, peer, fds, outer, proc.pid)
        if packet.get('config_sha256') != record['config_sha256']:
            raise boundary.BoundaryError('topology_namespace_ready_config_hash_mismatch')
        record['nested_controller_binding'] = binding
        record['raw_pid_scope'] = binding['pid_scope']
        boundary._same_source(record['source_manifest'])
        boundary._send(parent, {'kind': 'execute', 'config_sha256': record['config_sha256']})
        deadline = time.monotonic() + options['timeout_sec']
        while True:
            boundary._observe_owned_tree(binding['outer']['pid'], binding['outer']['start_ticks'], observations)
            try:
                packet, sender, received = boundary._recv(parent, min(.1, max(0, deadline - time.monotonic())))
                for handle in received:
                    os.close(handle)
                if sender != peer or packet.get('kind') != 'result' or received:
                    raise boundary.BoundaryError('topology_namespace_unbound_result')
                break
            except TimeoutError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('topology_namespace_runtime_timeout')
        envelope_ref = {'path': str(side / 'child-envelope.json'), 'sha256': packet['envelope_sha256'], 'kind': 'json'}
        envelope = _load_ref(envelope_ref, output)
        result = _load_ref(envelope['inner_result_ref'], output)
        for name in ('source_before_ref', 'source_after_ref'):
            if _load_ref(envelope[name], output) != record['source_manifest']:
                raise boundary.BoundaryError('topology_namespace_source_artifact_mismatch')
        scope, workers = _receipt_refs(envelope, result, output, binding['pid_scope'])
        record.update(inner_lease_scope=scope, worker_bindings=workers, envelope_ref=envelope_ref,
                      **{k: envelope[k] for k in ('inner_result_ref', 'source_before_ref', 'source_after_ref', 'raw_ref')})
        boundary._same_source(record['source_manifest'])
        boundary._send(parent, {'kind': 'result_ack'})
        proc.wait(timeout=options['kill_wait_sec'])
        if proc.returncode != 0:
            raise boundary.BoundaryError('topology_namespace_child_failed:' + str(proc.returncode))
        complete = True
    except BaseException as exc:
        record['error'] = type(exc).__name__ + ':' + str(exc)
        result = boundary._blocked('blocked:topology_namespace:' + record['error'])
    finally:
        try:
            record['cleanup'] = _cleanup(proc, fds[0] if fds else None, options)
        except BaseException as exc:
            record['cleanup'] = {'confirmed': False, 'controller_exited': False,
                                 'wrapper_returncode': proc.poll() if proc else None, 'method': CLEANUP_METHOD,
                                 'error': type(exc).__name__ + ':' + str(exc)}
        record['parent_process_observations'] = list(observations.values())
        for fd in fds:
            os.close(fd)
        for sock in (parent, child):
            if sock is not None:
                sock.close()
    if not complete or not record['cleanup']['confirmed']:
        if record['error'] is None:
            record['error'] = 'topology_namespace_cleanup_unconfirmed'
        result = boundary._blocked('blocked:topology_namespace:' + record['error'])
    boundary._write(side / 'manifest.json', record)
    # The descriptor lives only in the parent relay result. Child runtime and
    # envelope never point back to this manifest, so hashes cannot be cyclic.
    result['topology_namespace'] = {'schema': SCHEMA, 'bridge_id': record['bridge_id'],
        'manifest_path': str(side / 'manifest.json'), 'manifest_sha256': _sha(side / 'manifest.json'),
        **{k: record.get(k) for k in ('nested_controller_binding', 'outer_controller', 'raw_pid_scope',
                                    'inner_lease_scope', 'worker_bindings', 'cleanup')}}
    return result


if __name__ == '__main__':
    if len(sys.argv) != 5 or sys.argv[1] != '_child':
        raise SystemExit('only fixed canonical child dispatch is supported')
    raise SystemExit(_child(sys.argv[2], sys.argv[3], int(sys.argv[4])))
