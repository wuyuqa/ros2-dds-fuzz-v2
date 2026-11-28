"""Optional observations of the existing run_pair workers; never starts a process.

Runtime hooks, in order: prepare before WorkerPool; capture_worker after each
validated endpoint_created; capture_graph for a fresh snapshot and its unique
command_done; collect after pool.close and raw close/hash, before result JSON.
The returned intake is for core CandidateLifecycle, not admission or a ledger.
Only received SEDP identity/role/reliability/durability are compared here.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import unquote, urlparse
import xml.etree.ElementTree as ET

from r2f.instrumentation.cyclonedds import (
    RMW, build_trace_config, capture_process_provenance, collect_trace,
    environment_for_mode, normalize_guid,
)
from r2f.instrumentation.projection import reconcile_projection

_ROLES = ('writer', 'reader')
_PROC = Path('/proc')
_CONTRACT = 'cyclonedds.received_sedp.identity_role_reliability_durability/v1'
_PATH_KEYS = ('PATH', 'LD_LIBRARY_PATH', 'PYTHONPATH', 'AMENT_PREFIX_PATH',
              'CMAKE_PREFIX_PATH', 'COLCON_PREFIX_PATH')


FASTDDS_RMW = 'rmw_fastrtps_cpp'
_FASTDDS_LIBRARIES = (('librmw_fastrtps_cpp.so',), ('libfastrtps.so', 'libfastdds.so'), ('libfastcdr.so',))
FASTDDS_DEFAULT_XML = ('DEFAULT_FASTRTPS_PROFILES.xml', 'DEFAULT_FASTDDS_PROFILES.xml')


def observation_policy(rmw, mode):
    if mode not in ('blackbox', 'instrumentation'):
        raise ValueError('unsupported observation mode')
    if rmw not in (RMW, FASTDDS_RMW):
        raise ValueError('unsupported observation RMW')
    if mode == 'instrumentation' and rmw != RMW:
        raise ValueError('Cyclone vendor tracing requires rmw_cyclonedds_cpp')
    return {'schema': 'r2f.observation-profile-policy/v1', 'rmw': rmw, 'observation_mode': mode,
            'cyclonedds_profiles': 'required' if rmw == RMW else 'not_applicable',
            'contract': 'cyclonedds.non_tracing_profiles/v1' if rmw == RMW else 'fastdds.default_blackbox_absence/v1'}


def fastdds_environment_key(key):
    # Non-default transport/profile/trace settings have no supported observer
    # contract yet. Preserve their names in the audit and reject, even if empty.
    return key == 'SKIP_DEFAULT_XML' or key.startswith((
        'FASTDDS', 'FASTRTPS', 'RMW_FASTRTPS', 'CYCLONEDDS', 'ROS_TRACE', 'TRACETOOLS', 'R2F_INSTRUMENT'))


def fastdds_libraries_verified(libraries):
    return all(any(lib.get('identity_verified') is True and
                   any(Path(lib['mapped_path']).name == name or Path(lib['mapped_path']).name.startswith(name + '.')
                       for name in group) for lib in libraries) for group in _FASTDDS_LIBRARIES)


def mapped_library_identity_matches(library, maps):
    """Check every exact-path ELF segment against the verified on-disk identity."""
    try:
        mapped = Path(library['mapped_path'])
        path = Path(library['path']).resolve(strict=True)
        if not mapped.is_absolute() or mapped.resolve(strict=True) != path:
            return False
        stat = path.stat()
        device = f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}'
        if (str(stat.st_ino) != str(library['mapped_inode'])
                or library.get('mapped_device', device) != device):
            return False
        segments = [fields for line in maps.splitlines()
                    if len(fields := line.split(maxsplit=5)) == 6 and fields[5] == str(mapped)]
        return bool(segments) and all(fields[4] == str(stat.st_ino) and fields[3] == device
                                      for fields in segments)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _capture_fastdds_libraries(maps):
    libraries, seen = [], set()
    for line in maps.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or fields[5] in seen:
            continue
        name = Path(fields[5]).name
        if not any(name == prefix or name.startswith(prefix + '.') for group in _FASTDDS_LIBRARIES for prefix in group):
            continue
        seen.add(fields[5])
        record = {'mapped_path': fields[5], 'mapped_inode': int(fields[4]), 'mapped_device': fields[3]}
        try:
            path = Path(fields[5]).resolve(strict=True)
            before = path.stat()
            record.update(path=str(path), sha256=_sha(_read(path)), size=before.st_size)
            record['identity_verified'] = (before.st_ino == record['mapped_inode'] and _stat(before) == _stat(path.stat())
                and f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}' == fields[3])
        except (OSError, ValueError) as exc:
            record.update(identity_verified=False, error=type(exc).__name__)
        libraries.append(record)
    return libraries


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _stat(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read(path):
    path = Path(path)
    before = path.stat()
    data = path.read_bytes()
    if _stat(before) != _stat(path.stat()):
        raise ValueError('observation file changed while reading: ' + str(path))
    return data


def _write(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()
    with Path(path).open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    return {'raw_path': str(path), 'raw_sha256': _sha(data)}


def _tag(element):
    return element.tag.rsplit('}', 1)[-1]


def _tree(element):
    return [element.tag, sorted(element.attrib.items()), (element.text or '').strip(),
            [_tree(child) for child in element]]


def _xml(value, domain):
    """Freeze one inline/local XML source and remove only Tracing sections."""
    source = {'kind': 'default', 'raw_path': None, 'raw_sha256': None}
    if value:
        if value.lstrip().startswith('<'):
            text = value
            source['kind'] = 'inline'
        else:
            parsed = urlparse(value)
            if parsed.scheme and (parsed.scheme != 'file' or parsed.netloc not in ('', 'localhost')):
                raise ValueError('Cyclone config must be one inline or local XML source')
            if parsed.query or parsed.fragment or '$' in value:
                raise ValueError('Cyclone config expansion/list is not supported')
            path = Path(unquote(parsed.path) if parsed.scheme else value).resolve(strict=True)
            text = _read(path).decode('utf-8')
            source.update(kind='file', raw_path=str(path))
        source['raw_sha256'] = _sha(text.encode())
        root = ET.fromstring(text)
    else:
        root = ET.Element('CycloneDDS')
    if _tag(root) != 'CycloneDDS':
        raise ValueError('Cyclone config root must be CycloneDDS')
    ns = root.tag[:-len('CycloneDDS')]
    domains = [e for e in root if _tag(e) == 'Domain']
    if len(domains) > 1:
        raise ValueError('multiple Domain configurations require explicit normalization')
    section = domains[0] if domains else ET.SubElement(root, ns + 'Domain', {'Id': str(domain)})
    if section.get('Id', section.get('id', 'any')) not in ('any', str(domain)):
        raise ValueError('Cyclone Domain does not apply to requested domain')
    for parent in (root, section):
        for child in list(parent):
            if _tag(child) == 'Tracing':
                parent.remove(child)
    clean = ET.tostring(root, encoding='unicode')
    semantic_hash = _sha(json.dumps(_tree(root), sort_keys=True).encode())
    return clean, semantic_hash, source


def _guid(value):
    if isinstance(value, str) and len(value) == 48:
        value = bytes.fromhex(value)
    return normalize_guid(value)


def _forbidden(key):
    return key in ('LD_PRELOAD', 'LD_AUDIT') or key.startswith(('R2F_INSTRUMENT', 'CYCLONEDDS_TRACE'))


def _under(path, roots):
    return any(Path(path).resolve().is_relative_to(root) for root in roots)


def _observer_sources():
    from r2f.instrumentation import cyclonedds, projection
    paths = (Path(__file__), Path(cyclonedds.__file__), Path(projection.__file__))
    return {str(p.resolve()): _sha(_read(p)) for p in paths}


def prepare(config, out, base_env, role_env=None):
    """Return a context with cleaned base_env and complete per-role env overlays.

    observation_mode is execution metadata. Neither config, semantic input nor
    caller environments are modified. Existing role transport XML survives in
    both modes, with only Tracing replaced/removed. Explicit instrumentation is
    supported only for Cyclone; other RMWs can use the blackbox absence observer.
    """
    return ObservationContext(config, out, base_env, role_env)


class ObservationContext:
    def __init__(self, config, out, base_env, role_env):
        self.config = deepcopy(config)
        self.mode = config.get('observation_mode', 'blackbox')
        if self.mode not in ('blackbox', 'instrumentation'):
            raise ValueError('observation_mode must be blackbox or instrumentation')
        self.profile_policy = observation_policy(config.get('rmw'), self.mode)
        self.observe_adopted_config = config.get('observe_adopted_config', False)
        if type(self.observe_adopted_config) is not bool:
            raise ValueError('observe_adopted_config must be boolean')
        if self.observe_adopted_config and config.get('rmw') == RMW and self.mode != 'instrumentation':
            raise ValueError('adopted config tracing requires instrumentation mode')
        domain = config.get('domain_id')
        if type(domain) is not int or not 0 <= domain <= 231:
            raise ValueError('domain_id must be an integer in 0..231')
        for key in ('run_id', 'topic', 'rmw', 'target_id'):
            if not isinstance(config.get(key), str) or not config[key]:
                raise ValueError('observation requires ' + key)
        if role_env and set(role_env) - set(_ROLES):
            raise ValueError('unexpected role environment')
        self.out = Path(out).resolve()
        self.directory = self.out / 'observation'
        self.roots = tuple(Path(p).resolve() for p in config.get('instrumentation_roots', ()))
        self.base_env = environment_for_mode(base_env, mode='blackbox', instrumentation_roots=self.roots)
        self.role_env, self.trace_configs, self.profiles = {}, {}, {}
        for role in _ROLES:
            original = dict(base_env)
            original.update((role_env or {}).get(role, {}))
            if config['rmw'] == FASTDDS_RMW and any(fastdds_environment_key(k) for k in original):
                raise ValueError('unsupported FastDDS profile or trace environment')
            env = environment_for_mode(original, mode='blackbox', instrumentation_roots=self.roots)
            if config['rmw'] == RMW:
                clean, digest, source = _xml(original.get('CYCLONEDDS_URI'), domain)
                self.profiles[role] = {'non_tracing_sha256': digest, 'source': source, 'xml': clean}
                if self.mode == 'instrumentation':
                    trace = build_trace_config(self.directory / (role + '.trace.log'), domain_id=domain, base_xml=clean,
                                               observe_adopted_config=self.observe_adopted_config)
                    if _xml(trace.xml, domain)[1] != digest:
                        raise ValueError('Tracing changed the non-Tracing profile')
                    self.trace_configs[role] = trace
                    env.update(trace.environment)
                elif original.get('CYCLONEDDS_URI'):
                    env['CYCLONEDDS_URI'] = clean
            elif original.get('CYCLONEDDS_URI'):
                # An unused Cyclone variable must not enable tracing in blackbox.
                env.pop('CYCLONEDDS_URI', None)
            self.role_env[role] = env
        self.directory.mkdir(parents=True, exist_ok=False)
        self.execution_input = {'schema': 'r2f.observation-execution-input/v1',
            **{key: deepcopy(config[key]) for key in (
                'run_id', 'domain_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat',
                'seed', 'topic', 'message_type', 'effective_input_sha256', 'final_input',
                'input_layer_receipts') if key in config}}
        self.started_ns = time.time_ns()
        self.observer_sources = _observer_sources()
        self.workers, self.graphs, self.errors = {}, [], []
        self._collected = False
        self.prepared = _write(self.directory / 'prepared.json', {
            'schema': 'r2f.runtime-observation-prepared/v1', 'observation_mode': self.mode,
            'run_id': config['run_id'], 'domain_id': domain, 'rmw': config['rmw'],
            'execution_input': self.execution_input, 'profile_policy': self.profile_policy,
            'observe_adopted_config': self.observe_adopted_config,
            'profiles': self.profiles, 'trace_configs': {k: v.to_dict() for k, v in self.trace_configs.items()},
            'execution_metadata_only': True, 'started_unix_ns': self.started_ns,
            'observer_source_sha256': self.observer_sources,
        })

    def _identity(self, event, role, pid):
        c = self.config
        return (event.get('role') == role and event.get('pid') == pid
                and event.get('run_id') == c['run_id'] and event.get('domain_id') == c['domain_id']
                and event.get('rmw_identifier') == c['rmw'])

    def _reject(self, reason):
        self.errors.append(reason)
        return {'accepted': False, 'reason': reason}

    def capture_worker(self, role, pid, endpoint_event):
        """Take live maps/config/library/FD evidence only after endpoint creation."""
        if self._collected:
            raise RuntimeError('observation already collected')
        event = deepcopy(endpoint_event)
        if (role not in _ROLES or role in self.workers or type(pid) is not int
                or event.get('event') != 'endpoint_created' or not self._identity(event, role, pid)
                or not event.get('endpoint_generation') or not event.get('node_name')
                or type(event.get('time_ns')) is not int or event['time_ns'] < self.started_ns):
            return self._reject('invalid_endpoint_capture:' + str(role))
        try:
            guid = _guid(event.get('endpoint_gid'))
        except (TypeError, ValueError):
            return self._reject('endpoint_gid_unknown:' + role)
        trace = self.trace_configs.get(role)
        proof = capture_process_provenance(pid, trace_config=trace, proc_root=_PROC)
        audit = {'errors': [], 'maps': None, 'environment': {}, 'fd_targets': []}
        root = _PROC / str(pid)
        try:
            start = (root / 'stat').read_text().rsplit(')', 1)[1].split()[19]
            maps = (root / 'maps').read_text()
            env = dict(entry.split('=', 1) for entry in (root / 'environ').read_bytes().decode().split('\0') if '=' in entry)
            selected = {k: v for k, v in env.items() if k == 'CYCLONEDDS_URI' or _forbidden(k) or k in _PATH_KEYS
                        or k in ('RMW_IMPLEMENTATION', 'ROS_DOMAIN_ID', 'R2F_DISCOVERY_NETNS_PROOF')
                        or (self.config['rmw'] == FASTDDS_RMW and fastdds_environment_key(k))}
            audit.update(maps=maps, environment=selected)
            if self.config['rmw'] == FASTDDS_RMW:
                proof['loaded_libraries'].extend(_capture_fastdds_libraries(maps))
                proof['vendor_loaded'] = fastdds_libraries_verified(proof['loaded_libraries'])
                if any(fastdds_environment_key(k) for k in env):
                    audit['errors'].append('unsupported_fastdds_profile_or_trace_environment')
                cwd = (root / 'cwd').resolve(strict=True)
                audit['default_profile_files'] = {}
                audit['cwd'] = str(cwd)
                for filename in FASTDDS_DEFAULT_XML:
                    path = cwd / filename
                    try:
                        path.stat()
                        exists = True
                    except FileNotFoundError:
                        exists = False
                    audit['default_profile_files'][filename] = {'path': str(path), 'exists': exists}
                    if exists:
                        audit['errors'].append('unsupported_fastdds_default_profile:' + filename)
            if any(_forbidden(k) for k in env):
                audit['errors'].append('trace_or_loader_environment_present')
            for key in _PATH_KEYS:
                if any(_under(p, self.roots) for p in env.get(key, '').split(os.pathsep) if p):
                    audit['errors'].append('instrumentation_search_path_present:' + key)
            if env.get('ROS_DOMAIN_ID') != str(self.config['domain_id']) or env.get('RMW_IMPLEMENTATION') != self.config['rmw']:
                audit['errors'].append('worker_environment_identity_mismatch')
            expected = self.role_env[role].get('CYCLONEDDS_URI')
            if env.get('CYCLONEDDS_URI') != expected:
                audit['errors'].append('worker_config_mismatch')
            if self.config['rmw'] == RMW and self.mode == 'blackbox' and env.get('CYCLONEDDS_URI'):
                actual_xml = ET.fromstring(env['CYCLONEDDS_URI'])
                if any(_tag(e) == 'Tracing' for e in actual_xml.iter()):
                    audit['errors'].append('blackbox_tracing_enabled')
                if _xml(env['CYCLONEDDS_URI'], self.config['domain_id'])[1] != self.profiles[role]['non_tracing_sha256']:
                    audit['errors'].append('blackbox_semantic_profile_changed')
            if env.get('R2F_DISCOVERY_NETNS_PROOF') != self.role_env[role].get('R2F_DISCOVERY_NETNS_PROOF'):
                audit['errors'].append('namespace_proof_not_inherited')
            for library in proof.get('loaded_libraries', []):
                if not mapped_library_identity_matches(library, maps):
                    audit['errors'].append('loaded_library_mapping_identity_mismatch')
            for line in maps.splitlines():
                fields = line.split(maxsplit=5)
                if len(fields) == 6 and fields[5].startswith('/') and _under(fields[5], self.roots):
                    audit['errors'].append('instrumentation_library_loaded')
            for fd in (root / 'fd').iterdir():
                try:
                    target = os.readlink(fd)
                except FileNotFoundError:
                    continue
                audit['fd_targets'].append(target)
                if trace and target == trace.output_file:
                    s = fd.stat()
                    audit['trace_file_identity'] = [s.st_dev, s.st_ino]
                if (self.mode == 'blackbox' and target.startswith('/')
                        and (target.startswith(str(self.directory) + '/') or _under(target, self.roots))):
                    audit['errors'].append('instrumentation_fd_attached')
            end = (root / 'stat').read_text().rsplit(')', 1)[1].split()[19]
            if start != end or start != proof.get('process_start_ticks'):
                audit['errors'].append('process_identity_changed_during_audit')
        except (OSError, ValueError, UnicodeError) as exc:
            audit['errors'].append('live_audit_unavailable:' + type(exc).__name__)
        if (proof.get('process_group') != event.get('pgid')
                or proof.get('captured_at_unix_ns', 0) < event['time_ns']):
            audit['errors'].append('endpoint_process_binding_invalid')
        if not proof.get('process_identity_stable') or proof.get('unknown'):
            audit['errors'].append('process_provenance_incomplete')
        if not proof.get('vendor_loaded'):
            audit['errors'].append('vendor_library_identity_unverified')
        if trace and not all(proof.get(k) for k in ('config_matches', 'domain_matches', 'rmw_matches', 'trace_fd_attached')):
            audit['errors'].append('trace_not_attached_to_worker')
        record = {'role': role, 'pid': pid, 'endpoint_guid': guid, 'endpoint_event': event,
                  'process': proof, 'audit': audit, 'accepted': not audit['errors']}
        record['artifact'] = _write(self.directory / (role + '.process.json'), record)
        self.workers[role] = record
        return deepcopy(record)

    def capture_graph(self, snapshot_event, acknowledgement):
        """Accept only a same-worker fresh command graph followed by its ACK."""
        if self._collected:
            raise RuntimeError('observation already collected')
        graph, ack = deepcopy(snapshot_event), deepcopy(acknowledgement)
        role, command = graph.get('role'), graph.get('command_id')
        worker = self.workers.get(role)
        if (not worker or type(command) is not int or command <= 0 or graph.get('event') != 'graph_snapshot'
                or ack.get('event') != 'command_done' or ack.get('command_id') != command
                or not all(self._identity(e, role, worker['pid']) for e in (graph, ack))
                or any(e.get('endpoint_generation') != worker['endpoint_event']['endpoint_generation'] for e in (graph, ack))
                or any(type(e.get(k)) is not int for e in (graph, ack) for k in ('time_ns', 'monotonic_ns'))
                or not worker['endpoint_event']['time_ns'] <= graph['time_ns'] <= ack['time_ns']
                or graph['monotonic_ns'] >= ack['monotonic_ns']
                or any(g['snapshot']['role'] == role and g['snapshot']['command_id'] == command for g in self.graphs)):
            return self._reject('graph_ack_binding_invalid:' + str(role) + ':' + str(command))
        self.graphs.append({'snapshot': graph, 'acknowledgement': ack})
        return {'accepted': True, 'role': role, 'command_id': command}

    def _closed_runtime(self, result):
        errors, records = [], []
        path = Path(result.get('raw_path') or '')
        try:
            if path.resolve() != self.out / 'raw.jsonl':
                raise ValueError('raw must be the normal pair raw.jsonl')
            data = _read(path)
            if _sha(data) != result.get('raw_sha256'):
                raise ValueError('runtime raw hash mismatch')
            records = [json.loads(line) for line in data.splitlines()]
            if not records or records[-1].get('event') != 'runtime_end':
                raise ValueError('runtime raw is not finalized')
            for fd in Path('/proc/self/fd').iterdir():
                try:
                    if os.readlink(fd) == str(path):
                        flags = (Path('/proc/self/fdinfo') / fd.name).read_text().split('flags:', 1)[1].split()[0]
                        if int(flags, 8) & os.O_ACCMODE != os.O_RDONLY:
                            raise ValueError('runtime raw still open for writing')
                except FileNotFoundError:
                    continue
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors.append('closed_raw_unverified:' + str(exc))
        execution = result.get('execution') or {}
        if result.get('real_execution') is not True or result.get('runtime_status') != 'completed':
            errors.append('normal_pair_execution_incomplete')
        if (execution.get('real_execution') is not True or execution.get('runtime_status') != 'completed'
                or any(execution.get(k) != self.config.get(k) for k in
                       ('run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat'))):
            errors.append('execution_identity_mismatch')
        source, after = execution.get('source') or {}, execution.get('source_after') or {}
        if (any(not source.get(k) or source.get(k) != after.get(k) for k in ('head', 'runtime_source_sha256'))
                or source.get('observer_source_sha256') != after.get('observer_source_sha256')):
            errors.append('source_identity_missing_or_changed')
        if self.config.get('arm') == 'mutated' and not all(
                (execution.get('mutation_receipt') or {}).get(k) is True
                for k in ('carrier_admitted', 'configured', 'applied', 'hit')):
            errors.append('mutation_gate_incomplete')
        if records and (records[-1].get('run_id') != self.config['run_id']
                        or records[-1].get('domain_id') != self.config['domain_id']
                        or records[-1].get('runtime_status') != 'completed'):
            errors.append('runtime_end_identity_mismatch')
        if len(self.workers) == 2 and any(len({w[key] for w in self.workers.values()}) != 2
                                          for key in ('pid', 'endpoint_guid')):
            errors.append('worker_pair_identity_not_distinct')
        if execution.get('raw_path') != str(path) or execution.get('raw_sha256') != result.get('raw_sha256'):
            errors.append('execution_raw_binding_mismatch')
        for role in _ROLES:
            worker = self.workers.get(role)
            if not worker:
                errors.append('worker_capture_missing:' + role)
                continue
            metadata = [w for w in execution.get('workers', []) if w.get('role') == role]
            if (len(metadata) != 1 or metadata[0].get('pid') != worker['pid'] or metadata[0].get('returncode') != 0
                    or metadata[0].get('pgid') != worker['process'].get('process_group')):
                errors.append('worker_not_cleanly_reaped:' + role)
            try:
                stat = (_PROC / str(worker['pid']) / 'stat').read_text().rsplit(')', 1)[1].split()[19]
                if stat == worker['process'].get('process_start_ticks'):
                    errors.append('original_worker_still_present:' + role)
            except FileNotFoundError:
                pass
            except (OSError, IndexError) as exc:
                errors.append('worker_exit_unverified:' + role + ':' + type(exc).__name__)
            if not worker['accepted']:
                errors.extend(role + ':' + e for e in worker['audit']['errors'])
            if records.count(worker['endpoint_event']) != 1:
                errors.append('endpoint_not_uniquely_bound_to_raw:' + role)
            try:
                artifact = worker['artifact']
                if _sha(_read(artifact['raw_path'])) != artifact['raw_sha256']:
                    errors.append('process_capture_hash_mismatch:' + role)
            except (OSError, ValueError):
                errors.append('process_capture_unavailable:' + role)
        for pair in self.graphs:
            graph, ack = pair['snapshot'], pair['acknowledgement']
            related = [e for e in records if e.get('role') == graph['role']
                       and e.get('command_id') == graph['command_id'] and e.get('event') in ('graph_snapshot', 'command_done')]
            if related != [graph, ack]:
                errors.append('graph_not_uniquely_bound_to_closed_raw:' + graph['role'])
                continue
            chain = [e for e in records if e.get('role') == graph['role']
                     and e.get('command_id') == graph['command_id']
                     and e.get('event') in ('graph_snapshot_requested', 'match_snapshot', 'graph_snapshot', 'command_done')]
            valid = len(chain) == 4 and [e['event'] for e in chain] == [
                'graph_snapshot_requested', 'match_snapshot', 'graph_snapshot', 'command_done']
            if valid:
                request, match, _, _ = chain
                worker = self.workers[graph['role']]
                stamps = [request.get('requested_monotonic_ns'), match.get('sampled_monotonic_ns'),
                          match.get('monotonic_ns'), graph['monotonic_ns'], ack['monotonic_ns']]
                valid = (all(type(t) is int for t in stamps) and stamps == sorted(stamps)
                         and self._identity(match, graph['role'], worker['pid'])
                         and match.get('endpoint_generation') == worker['endpoint_event']['endpoint_generation']
                         and match.get('endpoint_gid') == worker['endpoint_event']['endpoint_gid']
                         and match.get('matched_count') == 1
                         and request.get('run_id') == self.config['run_id']
                         and request.get('domain_id') == self.config['domain_id'])
            if not valid:
                errors.append('graph_request_match_chain_unverified:' + graph['role'])
        return records, errors

    def _task(self, result):
        c = self.config
        return {'schema': 'r2f.blackbox-observation-task/v1', 'status': 'not_run',
                'origin_run_id': c['run_id'], 'observation_mode': 'blackbox',
                'same_input': {k: deepcopy(c[k]) for k in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat',
                              'seed', 'topic', 'message_type', 'final_input', 'effective_input_sha256', 'input_layer_receipts') if k in c},
                'precondition_receipts': deepcopy(result.get('precondition_receipts', [])),
                'non_tracing_profiles': deepcopy(self.profiles),
                'domain_rebinding': {'original_domain': c['domain_id'],
                                     'instruction': 'rebind an explicit Domain Id equal to original_domain when allocating the new domain'},
                'new_run_id_required': True, 'new_domain_required': True, 'new_output_directory_required': True,
                'execution_authority': 'existing campaign/runtime owner', 'executed_by_bridge': False}

    def _adopted_settings(self, traces, result, binding_errors):
        """Execution metadata only; configuration is never SEDP/candidate authority."""
        summary = {'schema': 'r2f.adopted-domain-settings/v1', 'roles': {},
                   'status': 'not_run', 'semantic_effect': 'unknown',
                   'parser_acceptance': 'pending_real_trace',
                   'fastdds': {'status': 'unsupported', 'execution': 'not_run'}}
        if self.config['rmw'] != RMW:
            summary['status'] = 'unsupported'
            return summary
        if not self.observe_adopted_config:
            return summary
        errors = list(binding_errors)
        try:
            prepared = json.loads(_read(self.prepared['raw_path']))
            if (prepared['trace_configs'] != json.loads(json.dumps({k: v.to_dict() for k, v in self.trace_configs.items()}))
                    or prepared['profiles'] != self.profiles):
                errors.append('prepared_projection_changed')
            if _sha(_read(result['raw_path'])) != result['raw_sha256']:
                errors.append('runtime_raw_changed_after_collection')
            if _observer_sources() != self.observer_sources:
                errors.append('observer_source_changed')
        except (OSError, ValueError, KeyError, TypeError):
            errors.append('configuration_binding_unavailable')
        for role, config in self.trace_configs.items():
            worker = self.workers.get(role)
            matches = [t for t in traces if t['config']['output_file'] == config.output_file]
            adopted = deepcopy(matches[0]['adopted_domain_settings']) if len(matches) == 1 else {
                'fields': {name: {'status': 'unknown', 'value': None, 'reason': 'trace_unavailable',
                                  'references': []} for name in ('transport', 'spdp_interval')}}
            role_errors = list(errors)
            try:
                captured = json.loads(_read(worker['artifact']['raw_path'])) if worker else {}
                if not isinstance(captured, dict):
                    role_errors.append('process_capture_not_object')
                elif (not worker or any(captured.get(key) != worker.get(key) for key in
                        ('role', 'pid', 'endpoint_guid', 'endpoint_event', 'process', 'audit', 'accepted'))):
                    role_errors.append('process_capture_projection_changed')
                trace_stat = Path(config.output_file).stat()
                if (not worker or not worker['accepted'] or len(matches) != 1
                        or adopted.get('provenance_status') != 'verified_vendor_process'
                        or worker['audit'].get('trace_file_identity') != [trace_stat.st_dev, trace_stat.st_ino]
                        or list(_stat(trace_stat)) != adopted.get('collection_identity')
                        or _sha(_read(config.output_file)) != adopted.get('raw_sha256')):
                    role_errors.append('trace_or_worker_binding_unverified')
            except (OSError, ValueError, TypeError, KeyError):
                role_errors.append('trace_unavailable')
            adopted['endpoint_binding'] = None
            if role_errors:
                adopted['provenance_status'] = 'unknown'
                for field in adopted['fields'].values():
                    field.update(status='unknown', value=None, reason='execution_binding_unverified')
            else:
                adopted['endpoint_binding'] = {
                    'role': role, 'pid': worker['pid'], 'endpoint_guid': worker['endpoint_guid'],
                    'endpoint_generation': worker['endpoint_event']['endpoint_generation'],
                    'endpoint_event': deepcopy(worker['endpoint_event']),
                    'process_capture': deepcopy(worker['artifact']),
                    'runtime_raw_path': result['raw_path'], 'runtime_raw_sha256': result['raw_sha256']}
            adopted['binding_unknowns'] = sorted(set(role_errors))
            adopted['unknown_fields'] = [name + ':' + f['reason'] for name, f in adopted['fields'].items()
                                        if f['status'] == 'unknown']
            adopted['status'] = 'unknown' if adopted['unknown_fields'] else 'observed'
            summary['roles'][role] = adopted
        summary['status'] = ('observed' if len(summary['roles']) == 2
                             and all(r['status'] == 'observed' for r in summary['roles'].values()) else 'unknown')
        return summary

    def collect(self, runtime_result):
        """Freeze sidecar observations; do not mutate runtime_result or its raw.

        Unknowns always suppress intake, including a comparator fail accompanied
        by unknown fields. Missing data is an observation gap, never a SUT fail.
        The caller attaches this returned object as execution metadata.
        """
        if self._collected:
            raise RuntimeError('observation already collected')
        self._collected = True
        result = deepcopy(runtime_result)
        records, errors = self._closed_runtime(result)
        errors.extend(self.errors)
        try:
            if _observer_sources() != self.observer_sources:
                errors.append('observer_source_changed')
        except (OSError, ValueError):
            errors.append('observer_source_unavailable')
        try:
            if _sha(_read(self.prepared['raw_path'])) != self.prepared['raw_sha256']:
                errors.append('prepared_config_hash_mismatch')
        except (OSError, ValueError):
            errors.append('prepared_config_unavailable')
        config_binding_errors = list(errors)
        task = self._task(result) if self.mode == 'instrumentation' else None
        traces, comparisons = [], []
        if self.mode == 'instrumentation':
            for role, config in self.trace_configs.items():
                worker = self.workers.get(role)
                try:
                    trace = collect_trace(config, processes=[worker['process']] if worker else [], control_task=task)
                    s = Path(config.output_file).stat()
                    if not worker or worker['audit'].get('trace_file_identity') != [s.st_dev, s.st_ino]:
                        errors.append('trace_file_identity_changed:' + role)
                    if not trace['vendor_process_verified']:
                        errors.append('vendor_process_unverified:' + role)
                    traces.append(trace)
                except (OSError, ValueError, TypeError) as exc:
                    errors.append('trace_collection_failed:' + role + ':' + str(exc))
            for role in _ROLES:
                graphs = [g for g in self.graphs if g['snapshot']['role'] == role]
                if not graphs:
                    errors.append('command_graph_missing:' + role)
                    continue
                graph = max(graphs, key=lambda g: g['snapshot']['monotonic_ns'])['snapshot']
                endpoints = []
                try:
                    for key, kind in (('publishers', 'writer'), ('subscriptions', 'reader')):
                        for e in graph[key]:
                            endpoints.append({'topic_name': self.config['topic'], 'endpoint_kind': kind,
                                              'endpoint_guid': _guid(e['endpoint_gid']), 'qos': e['qos'],
                                              'ros_type': e['topic_type']})
                    expected = {w['endpoint_guid'] for w in self.workers.values()}
                    if len(endpoints) != 2 or {e['endpoint_guid'] for e in endpoints} != expected:
                        errors.append('graph_not_exact_pair:' + role)
                    for endpoint in endpoints:
                        owner = next((w for w in self.workers.values() if w['endpoint_guid'] == endpoint['endpoint_guid']), None)
                        if owner and endpoint['endpoint_kind'] != owner['role']:
                            errors.append('graph_role_identity_mismatch:' + role)
                    projection = {'source': 'ros_graph', 'captured_at_unix_ns': graph['time_ns'],
                                  'endpoints': endpoints, 'application_event': graph}
                    projection.update(_write(self.directory / (role + '.graph.json'), projection))
                    comparison = reconcile_projection(traces, projection, dds_topic='rt' + self.config['topic'],
                                                      ros_topic=self.config['topic'], required_qos=('reliability', 'durability'),
                                                      complete_window=not errors, window_start_ns=self.started_ns,
                                                      window_end_ns=graph['time_ns'])
                    comparisons.append({'role': role, 'graph': projection, 'comparison': comparison})
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    errors.append('graph_projection_unavailable:' + role + ':' + str(exc))
            expected = {w['endpoint_guid'] for w in self.workers.values()}
            for trace in traces:
                for event in trace['events']:
                    if event.get('topic_name') == 'rt' + self.config['topic']:
                        if event.get('domain_id') != self.config['domain_id'] or event.get('endpoint_guid') not in expected:
                            errors.append('vendor_event_not_bound_to_pair')
            for entry in comparisons:
                errors.extend(entry['comparison']['unknown_fields'])
            if len(comparisons) != 2:
                errors.append('independent_reconciliation_incomplete')
        witnesses = []
        for entry in comparisons:
            captured = entry['graph']['captured_at_unix_ns']
            for mismatch in entry['comparison']['mismatches']:
                matching = [e for t in traces for e in t['events']
                            if e.get('endpoint_guid') == mismatch['endpoint_guid']
                            and e.get('topic_name') == 'rt' + self.config['topic']
                            and self.started_ns <= e['event_time_ns'] <= captured]
                if not matching:
                    errors.append('internal_failure_witness_missing')
                else:
                    witness = max(matching, key=lambda e: e['event_time_ns'])
                    witnesses.append({'comparison_role': entry['role'], 'field': mismatch['field'],
                                      'graph_raw_path': entry['graph']['raw_path'],
                                      'graph_raw_sha256': entry['graph']['raw_sha256'], 'vendor_event': witness})
        verdict = ('unknown' if errors else 'fail' if any(e['comparison']['verdict'] == 'fail' for e in comparisons) else 'pass')
        excluded = bool(self.config.get('fault_id') or self.config.get('control_kind') or self.config.get('arm') == 'negative'
                        or (result.get('execution') or {}).get('candidate_eligible') is False)
        adopted = self._adopted_settings(traces, result, config_binding_errors)
        presented = {role: {'scope': 'parent_prepared',
                           'worker_byte_read': 'not_observed',
                           'input_xml_source': deepcopy(profile['source']),
                           'non_tracing_sha256': profile['non_tracing_sha256'],
                           'trace_xml_sha256': self.trace_configs[role].sha256 if role in self.trace_configs else None,
                           'prepared': deepcopy(self.prepared)} for role, profile in self.profiles.items()}
        summary = {'schema': 'r2f.runtime-instrumentation/v1', 'observation_mode': self.mode,
                   'execution_metadata_only': True, 'run_id': self.config['run_id'],
                   'execution_input': deepcopy(self.execution_input),
                   'presented_configuration': presented, 'adopted_domain_settings': adopted,
                   'semantic_effect': 'unknown',
                   'profile_policy': deepcopy(self.profile_policy),
                   'actual_run': result.get('real_execution') is True,
                   'source_manifest': (result.get('execution') or {}).get('source'),
                   'status': 'incomplete' if errors else 'completed', 'verdict': verdict,
                   'unknown_fields': sorted(set(errors)), 'candidate_admission': 'not_performed',
                   'target_maturity': 'not_evaluated', 'raw_path': result.get('raw_path'), 'raw_sha256': result.get('raw_sha256'),
                   'prepared': self.prepared, 'observer_source_sha256': self.observer_sources, 'process_captures': {r: w['artifact'] for r, w in self.workers.items()},
                   'traces': traces, 'reconciliations': comparisons, 'blackbox_task': task,
                   'blackbox_validation': 'not_run' if self.mode == 'instrumentation' else 'not_applicable',
                   'blackbox_observer': {'status': 'verified' if self.mode == 'blackbox' and not errors else 'not_verified',
                                         'scope': 'configured_trace_disabled; loader_overrides_and_known_instrumentation_roots_absent'},
                   'intake': [], 'excluded_control': excluded}
        if verdict == 'fail' and not excluded:
            timestamp = max(w['vendor_event']['event_time_ns'] for w in witnesses)
            points = sorted({w['vendor_event']['instrumentation_point'] for w in witnesses})
            summary['intake'].append({
                **{k: deepcopy(self.config[k]) for k in ('run_id', 'target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'effective_input_sha256') if k in self.config},
                'timestamp': datetime.fromtimestamp(timestamp / 1e9, timezone.utc).isoformat(),
                'candidate_source': 'instrumentation', 'evidence_tier': 'instrumented',
                'raw_path': result['raw_path'], 'raw_sha256': result['raw_sha256'],
                'instrumentation_identity': {'id': _CONTRACT, 'raw_hashes': [t['raw_sha256'] for t in traces]},
                'internal_contract': _CONTRACT, 'observed_effect': [e['comparison'] for e in comparisons if e['comparison']['verdict'] == 'fail'],
                'execution_metadata': deepcopy(result.get('execution')),
                'provenance': {'instrumentation_point': ','.join(points), 'event_time': str(timestamp),
                               'control': 'same-input blackbox task: not_run',
                               'binary_identity': [w['process']['loaded_libraries'] for w in self.workers.values()],
                               'process_captures': summary['process_captures'], 'witnesses': witnesses, 'trace_raw': [
                                   {'raw_path': t['raw_path'], 'raw_sha256': t['raw_sha256']} for t in traces]},
                'candidate_admission': 'not_performed', 'lifecycle_owner': 'existing core CandidateLifecycle',
                'requires_core_enrichment': ['execution_stage', 'canonical execution row and mutation gates', 'triage'],
            })
        summary['candidate_count'] = len(summary['intake'])
        summary['zero_candidate'] = self.mode == 'instrumentation' and verdict == 'pass'
        summary['artifact'] = _write(self.directory / 'summary.json', summary)
        return summary
