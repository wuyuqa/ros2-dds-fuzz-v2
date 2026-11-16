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

    def start(self, role):
        env = dict(self.environment)
        env.update(self.role_environments.get(role, {}))
        env.update(ROS_DOMAIN_ID=str(self.config['domain_id']), RMW_IMPLEMENTATION=self.config['rmw'],
                   ROS_LOCALHOST_ONLY='1', ROS_LOG_DIR=str(self.output_dir / 'ros_logs'),
                   PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1')
        stderr_path = self.output_dir / f'{role}.stderr.log'
        stderr = stderr_path.open('xb')
        self.stderr_handles.append(stderr)
        process = subprocess.Popen(['/usr/bin/python3', '-B', '-m', 'r2f.runtime.worker',
                                    json.dumps(self.config, sort_keys=True), role],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                                   env=env, start_new_session=True, cwd=self.config['source_repo'])
        self.workers[role] = process
        self.buffers[role] = b''
        os.set_blocking(process.stdout.fileno(), False)
        self.selector.register(process.stdout, selectors.EVENT_READ, role)
        self.metadata.append({'role': role, 'pid': process.pid, 'pgid': process.pid,
                              'fresh_process': True, 'separate_context': True, 'stderr_path': str(stderr_path)})
        self.emit('process_started', role=role, pid=process.pid, pgid=process.pid)

    def command(self, role, command, **fields):
        self.command_serial += 1
        command_id = self.command_serial
        process = self.workers[role]
        process.stdin.write((json.dumps({'command': command, 'command_id': command_id, **fields}) + '\n').encode())
        process.stdin.flush()
        return command_id

    def pump(self, timeout=0.05):
        for key, _ in self.selector.select(timeout):
            role = key.data
            chunk = os.read(key.fileobj.fileno(), 65536)
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            self.buffers[role] += chunk
            while b'\n' in self.buffers[role]:
                line, self.buffers[role] = self.buffers[role].split(b'\n', 1)
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict) or 'event' not in event:
                        raise ValueError('not an event')
                    event.setdefault('role', role)
                    self.emit(event.pop('event'), **event)
                except (ValueError, TypeError):
                    # DDS vendor stdout is retained verbatim; it is never evidence.
                    self.emit('worker_stdout_diagnostic', role=role, text=line.decode(errors='replace'))

    def wait(self, predicate, timeout, label, *, check_health=True):
        deadline = time.monotonic() + timeout
        while True:
            if predicate():
                return
            if check_health:
                # A probe is deliberately stopped during the window; its clean
                # exit (rc0) is expected.  Writer/reader must stay alive.
                failed = [(role, p.returncode) for role, p in self.workers.items()
                          if p.poll() is not None and not (role == 'probe' and p.returncode == 0)]
                if failed:
                    self.pump(0)
                    raise RuntimeError(f'worker_exited:{failed}')
            if time.monotonic() >= deadline:
                raise TimeoutError(label)
            self.pump(min(0.05, max(0, deadline - time.monotonic())))

    def close(self):
        for role, process in self.workers.items():
            if process.poll() is None:
                with contextlib.suppress(OSError):
                    self.command(role, 'stop')
        deadline = time.monotonic() + 3
        while any(p.poll() is None for p in self.workers.values()) and time.monotonic() < deadline:
            self.pump(0.05)
        for role, process in self.workers.items():
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
            self.metadata[[m['role'] for m in self.metadata].index(role)]['returncode'] = process.returncode
            self.emit('process_exit', role=role, pid=process.pid, pgid=process.pid, returncode=process.returncode)
            process.stdout.close()
            process.stdin.close()
        self.selector.close()
        for handle in self.stderr_handles:
            handle.close()
