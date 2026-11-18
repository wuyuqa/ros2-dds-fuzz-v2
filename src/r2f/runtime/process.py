"""Own only fresh worker process groups; never signal a preexisting PID."""
from __future__ import annotations
import contextlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time


class WorkerPool:
    def __init__(self, config, environment, output_dir, emit, *, role_environments=None):
        self.config, self.environment, self.output_dir, self.emit = config, environment, Path(output_dir), emit
        self.selector = selectors.DefaultSelector()
        self.workers, self.buffers, self.stderr_handles = {}, {}, []
        self.role_environments = role_environments or {}
        self.command_serial = 0
        self.metadata = []
        self.instances = []
        self.role_generations = {}
        # Roles that were deliberately stopped by the controller.  Their clean
        # rc0 exit is expected health state, never a worker crash.  This is
        # Current-role view only; instances below retain the stop state of
        # every generation so a restart cannot erase its predecessor's health.
        self.stopped_roles = set()

    def start(self, role):
        if role in self.workers and self.workers[role].poll() is None:
            raise RuntimeError('worker_role_still_live:' + role)
        for instance in self.instances:
            if instance['metadata']['role'] == role and instance['process'].poll() is not None:
                instance['metadata']['returncode'] = instance['process'].returncode
        env = dict(self.environment)
        env.update(self.role_environments.get(role, {}))
        env.update(ROS_DOMAIN_ID=str(self.config['domain_id']), RMW_IMPLEMENTATION=self.config['rmw'],
                   ROS_LOCALHOST_ONLY='1', ROS_LOG_DIR=str(self.output_dir / 'ros_logs'),
                   PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1')
        stderr_path = self.output_dir / f'{role}.stderr.log'
        # Same-role restarts are explicit churn: never overwrite an earlier
        # generation's stderr, suffix the new attempt instead.
        attempt = 1
        while True:
            try:
                stderr = stderr_path.open('xb')
                break
            except FileExistsError:
                attempt += 1
                stderr_path = self.output_dir / f'{role}.{attempt}.stderr.log'
        self.stderr_handles.append(stderr)
        process = subprocess.Popen(['/usr/bin/python3', '-B', '-m', 'r2f.runtime.worker',
                                    json.dumps(self.config, sort_keys=True), role],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                                   env=env, start_new_session=True, cwd=self.config['source_repo'])
        self.workers[role] = process
        self.buffers[role] = b''
        self.stopped_roles.discard(role)
        os.set_blocking(process.stdout.fileno(), False)
        generation = self.role_generations.get(role, 0) + 1
        self.role_generations[role] = generation
        metadata = {'role': role, 'pid': process.pid, 'pgid': process.pid,
                    'worker_generation': generation, 'fresh_process': True,
                    'separate_context': True, 'stderr_path': str(stderr_path)}
        instance = {'process': process, 'metadata': metadata, 'buffer': b'',
                    'stop_command_id': None, 'stop_requested_monotonic_ns': None,
                    'endpoint_generation': None}
        self.instances.append(instance)
        self.selector.register(process.stdout, selectors.EVENT_READ, instance)
        self.metadata.append(metadata)
        self.emit('process_started', **metadata)

    def command(self, role, command, **fields):
        self.command_serial += 1
        command_id = self.command_serial
        process = self.workers[role]
        instance = next(item for item in reversed(self.instances) if item['process'] is process)
        if command == 'stop' and instance['stop_command_id'] is not None:
            raise RuntimeError('worker_stop_already_requested:' + role)
        requested_ns = time.monotonic_ns()
        self.emit('worker_command_requested', requested_monotonic_ns=requested_ns,
                  role=role, pid=process.pid, pgid=process.pid, origin='controller_pipe',
                  worker_generation=instance['metadata']['worker_generation'],
                  endpoint_generation=instance['endpoint_generation'],
                  command=command, command_id=command_id)
        process.stdin.write((json.dumps({'command': command, 'command_id': command_id, **fields}) + '\n').encode())
        process.stdin.flush()
        if command == 'stop':
            self.stopped_roles.add(role)
            instance['stop_command_id'] = command_id
            instance['stop_requested_monotonic_ns'] = requested_ns
        return command_id

    def is_stopped(self, role):
        return role in self.stopped_roles

    def pump(self, timeout=0.05):
        for key, _ in self.selector.select(timeout):
            instance = key.data
            process, metadata = instance['process'], instance['metadata']
            role = metadata['role']
            chunk = os.read(key.fileobj.fileno(), 65536)
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            instance['buffer'] += chunk
            while b'\n' in instance['buffer']:
                line, instance['buffer'] = instance['buffer'].split(b'\n', 1)
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict) or 'event' not in event:
                        raise ValueError('not an event')
                    if event.get('role', role) != role or event.get('pid', process.pid) != process.pid:
                        raise ValueError('worker transport identity mismatch')
                    kind = event.pop('event')
                    generation = event.get('endpoint_generation')
                    if kind == 'endpoint_created':
                        if not isinstance(generation, str) or not generation:
                            raise ValueError('missing endpoint generation')
                        if instance['endpoint_generation'] not in (None, generation):
                            raise ValueError('worker endpoint generation changed')
                        instance['endpoint_generation'] = generation
                    elif (instance['endpoint_generation'] is not None
                          and generation != instance['endpoint_generation']):
                        raise ValueError('worker endpoint generation mismatch')
                    event.update(role=role, pid=process.pid,
                                 worker_generation=metadata['worker_generation'])
                    if kind == 'worker_shutdown':
                        # The worker has no stop ACK field. Bind its genuine
                        # shutdown to the stop submitted on this exact pipe;
                        # never attach a later same-role process's command.
                        requested_ns = instance['stop_requested_monotonic_ns']
                        sampled_ns = event.get('monotonic_ns')
                        event['stop_command_id'] = (instance['stop_command_id']
                            if type(requested_ns) is int and type(sampled_ns) is int
                            and sampled_ns >= requested_ns else None)
                        event['stop_command_binding'] = 'controller_pipe_request_not_worker_ack'
                    self.emit(kind, **event)
                except (ValueError, TypeError):
                    # Vendor stdout or invalid identity is never evidence.
                    self.emit('worker_stdout_diagnostic', role=role, pid=process.pid,
                              worker_generation=metadata['worker_generation'],
                              text=line.decode(errors='replace'))

    def wait(self, predicate, timeout, label, *, check_health=True):
        deadline = time.monotonic() + timeout
        while True:
            if predicate():
                return
            if check_health:
                # A worker deliberately stopped by the controller may exit
                # cleanly; any other exit, including a nonzero exit from a
                # stopped worker, remains a health failure.
                failed = [(role, p.returncode) for role, p in self.workers.items()
                          if p.poll() is not None and not (role in self.stopped_roles and p.returncode == 0)]
                failed += [(i['metadata']['role'], i['process'].returncode)
                           for i in getattr(self, 'instances', ())
                           if self.workers.get(i['metadata']['role']) is not i['process']
                           and i['process'].poll() is not None
                           and not (i['stop_command_id'] is not None and i['process'].returncode == 0)]
                if failed:
                    self.pump(0)
                    raise RuntimeError(f'worker_exited:{failed}')
            if time.monotonic() >= deadline:
                raise TimeoutError(label)
            self.pump(min(0.05, max(0, deadline - time.monotonic())))

    def close(self):
        for role, process in self.workers.items():
            if process.poll() is None and not self.is_stopped(role):
                with contextlib.suppress(OSError):
                    self.command(role, 'stop')
        deadline = time.monotonic() + 3
        while any(p.poll() is None for p in self.workers.values()) and time.monotonic() < deadline:
            self.pump(0.05)
        for instance in self.instances:
            process, metadata = instance['process'], instance['metadata']
            role = metadata['role']
            if process.poll() is None:
                # Only the process group allocated by this Popen is eligible.
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=2)
            self.pump(0)
            metadata['returncode'] = process.returncode
            self.emit('process_exit', role=role, pid=process.pid, pgid=process.pid,
                      worker_generation=metadata['worker_generation'], returncode=process.returncode)
            with contextlib.suppress(KeyError, ValueError):
                self.selector.unregister(process.stdout)
            process.stdout.close()
            process.stdin.close()
        self.selector.close()
        for handle in self.stderr_handles:
            handle.close()
