"""Host-wide cooperative leases plus conservative, secret-free isolation probes."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import time


class IsolationError(RuntimeError):
    pass


def _ancestors():
    result = {os.getpid()}
    pid = os.getppid()
    while pid > 1 and pid not in result:
        result.add(pid)
        try:
            pid = int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    return result


def scan_processes(old_repo: Path, domain_id: int | None = None, proc_root=Path('/proc'), skip_pids=None):
    """Inspect only same-user processes; emit reasons and PIDs, never argv/env values."""
    skip_pids = _ancestors() if skip_pids is None else skip_pids
    conflicts, unreadable, excluded = [], [], []
    for entry in Path(proc_root).glob('[0-9]*'):
        pid = int(entry.name)
        if pid in skip_pids:
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
            argv = (entry / 'cmdline').read_bytes().split(b'\0')
            status = (entry / 'status').read_text() if (entry / 'status').exists() else ''
            if any(line.startswith('State:') and 'Z' in line for line in status.splitlines()):
                excluded.append({'pid': pid, 'reason': 'zombie_no_active_resources'})
                continue
            try:
                env = (entry / 'environ').read_bytes().split(b'\0')
            except PermissionError:
                executable_path = argv[0].decode(errors='replace') if argv else ''
                comm = (entry / 'comm').read_text().strip() if (entry / 'comm').exists() else ''
                if (executable_path, comm) in {('(sd-pam)', '(sd-pam)'), ('/usr/bin/vmtoolsd', 'vmtoolsd')}:
                    excluded.append({'pid': pid, 'reason': 'identified_os_session_helper', 'executable': executable_path, 'environment_readable': False})
                    continue
                raise
            args = [x.decode(errors='replace') for x in argv if x]
            old = any(str(old_repo) in a for a in args)
            try:
                cwd = (entry / 'cwd').resolve(strict=True)
                old = old or cwd == old_repo or old_repo in cwd.parents
            except OSError:
                pass
            reasons = []
            # A terminal, editor, or agent holding a cwd is not an executing campaign.
            executable = Path(args[0]).name if args else ''
            campaign = any(any(k in a.lower() for k in ('campaign', 'ros2_pair', 'run_pair', 'fuzz_driver')) for a in args)
            if old and (campaign or executable.startswith(('python', 'ros2'))):
                reasons.append('old_repo_active_process')
            if domain_id is not None:
                marker = f'ROS_DOMAIN_ID={domain_id}'.encode()
                if marker in env or marker.decode() in args:
                    reasons.append('domain_owner_process')
                for flag in ('--domain', '--domain-id', '--domain_id'):
                    if flag in args and args.index(flag) + 1 < len(args) and args[args.index(flag) + 1] == str(domain_id):
                        reasons.append('domain_owner_argument')
            if reasons:
                conflicts.append({'pid': pid, 'reasons': sorted(set(reasons))})
        except FileNotFoundError:
            continue
        except PermissionError:
            unreadable.append(pid)
    return {'ok': not conflicts and not unreadable, 'conflicts': conflicts, 'unreadable_same_uid_pids': unreadable, 'excluded_processes': excluded}


def scan_old_locks(old_repo: Path):
    """Read/probe existing files without creating or rewriting old-repository state."""
    records, errors = [], []
    if not old_repo.exists():
        return {'ok': True, 'locks': [], 'old_repo_exists': False}
    def onerror(error):
        errors.append(type(error).__name__)
    for root, dirs, files in os.walk(old_repo, onerror=onerror):
        dirs[:] = [d for d in dirs if d not in ('.git', 'node_modules', '__pycache__', '.venv')]
        for name in files:
            if not (name.endswith('.lock') or name in ('owner.json', 'run.lock', 'campaign.lock')):
                continue
            path = Path(root) / name
            record = {'path': str(path.relative_to(old_repo)), 'held': False, 'live_owner': False}
            try:
                with path.open('r') as handle:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(handle, fcntl.LOCK_UN)
                    except BlockingIOError:
                        record['held'] = True
                    data = handle.read(65536)
                    try:
                        payload = json.loads(data)
                        pid = payload.get('pid', payload.get('owner_pid'))
                        if pid:
                            try:
                                os.kill(int(pid), 0)
                                record['live_owner'] = True
                                record['owner_pid'] = int(pid)
                            except ProcessLookupError:
                                pass
                    except (ValueError, TypeError, AttributeError):
                        record['owner_unparseable'] = bool(data.strip())
            except OSError as exc:
                record['error'] = type(exc).__name__
            records.append(record)
    ok = not errors and not any(r.get('held') or r.get('live_owner') or r.get('error') or r.get('owner_unparseable') for r in records)
    return {'ok': ok, 'locks': records, 'scan_errors': errors, 'old_repo_exists': True}


def probe_rtps_ports(domain_id: int):
    """Reserve/probe the complete conventional DDSI domain UDP block then release.

    This is a point-in-time conflict probe, not a packet filter. Cooperative domain
    leases remain held throughout endpoint execution; an external noncooperating
    process can still race the probe. Both IPv4 and IPv6 wildcard binds are tested.
    """
    if isinstance(domain_id, bool) or not isinstance(domain_id, int) or not 0 <= domain_id <= 231:
        raise ValueError('domain_id must be an integer in 0..231')
    start = 7400 + 250 * domain_id
    sockets, busy, errors = [], set(), []
    try:
        for family, address in ((socket.AF_INET, '0.0.0.0'), (socket.AF_INET6, '::')):
            for port in range(start, start + 250):
                sock = socket.socket(family, socket.SOCK_DGRAM)
                try:
                    if family == socket.AF_INET6:
                        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                    sock.bind((address, port))
                    sockets.append(sock)
                except OSError as exc:
                    sock.close()
                    if exc.errno == 98:
                        busy.add(port)
                    elif family == socket.AF_INET6 and exc.errno in (97, 99):
                        break
                    else:
                        errors.append({'port': port, 'errno': exc.errno, 'family': int(family)})
    finally:
        for sock in sockets:
            sock.close()
    return {'ok': not busy and not errors, 'domain_id': domain_id, 'port_start': start,
            'port_end': start + 249, 'busy_ports': sorted(busy), 'errors': errors,
            'probe_monotonic_ns': time.monotonic_ns(), 'reservation': 'released_before_endpoint_start'}


class LeaseSet:
    """Never unlink lock inodes: unlinking a released flock creates split ownership."""
    def __init__(self, root: Path, state_dir: Path, campaign_id: str, domain_id: int):
        self.root, self.state_dir = Path(root), Path(state_dir).resolve()
        self.campaign_id, self.domain_id = campaign_id, domain_id
        self.handles = []
        self.receipts = []

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        state_key = hashlib.sha256(str(self.state_dir).encode()).hexdigest()
        campaign_key = hashlib.sha256(self.campaign_id.encode()).hexdigest()
        keys = [f'campaign-{campaign_key}.lock', f'domain-{self.domain_id}.lock', f'state-{state_key}.lock']
        try:
            for key in keys:
                path = self.root / key
                handle = path.open('a+')
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    handle.close()
                    raise IsolationError(f'domain_conflict:lock:{key}') from None
                self.handles.append(handle)
                owner = {'pid': os.getpid(), 'campaign_id': self.campaign_id, 'domain_id': self.domain_id,
                         'state_dir': str(self.state_dir), 'acquired_time_ns': time.time_ns()}
                handle.seek(0)
                handle.truncate()
                json.dump(owner, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
                self.receipts.append({'path': str(path), 'owner': owner, 'acquired': True})
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        for handle in reversed(self.handles):
            with contextlib.suppress(OSError):
                fcntl.flock(handle, fcntl.LOCK_UN)
                handle.close()
        self.handles.clear()
