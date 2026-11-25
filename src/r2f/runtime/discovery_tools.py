"""Opt-in dependency identity and owned observation for real Python DDS tools.

No import of the Python binding, participant, CLI or subprocess occurs on import.
The existing namespace controller/WorkerPool owns execution and cleanup.
"""
from copy import deepcopy
import hashlib
import json
import os
import time
from pathlib import Path
import re


KEY = 'projection_discovery_tools'


def validate_tools_reference(config):
    if KEY not in config:
        return
    if config.get('projection_discovery_layers') != 'participant_topic_v1':
        raise ValueError('discovery_tools_requires_projection_discovery_layers')
    ref = config[KEY]
    if (not isinstance(ref, dict) or set(ref) != {'path', 'sha256'}
            or not isinstance(ref['path'], str) or not Path(ref['path']).is_absolute()
            or not isinstance(ref['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', ref['sha256'])):
        raise ValueError('discovery_tools_reference_invalid')


def load_tools_environment(ref):
    """Read-only file verification. Package identity never proves DDS execution."""
    data = Path(ref['path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != ref['sha256']:
        raise ValueError('discovery_tools_manifest_hash_mismatch')
    manifest = json.loads(data)
    root = Path(manifest['python_root'])
    files = manifest['files']
    if (manifest.get('schema') != 'r2f.discovery-python-environment/v1'
            or manifest.get('distribution') != 'cyclonedds'
            or not isinstance(manifest.get('version'), str) or not manifest['version']
            or not root.is_absolute() or not isinstance(files, dict) or not files
            or str(root / 'cyclonedds' / '__init__.py') not in files
            or str(root / 'cyclonedds' / 'builtin.py') not in files
            or manifest['library']['path'] not in files
            or files[manifest['library']['path']] != manifest['library']['sha256']
            or not any(Path(path).name.startswith('_clayer.') and path.endswith('.so') for path in files)):
        raise ValueError('discovery_tools_manifest_incomplete')
    for path, expected in files.items():
        if (not Path(path).is_absolute() or not isinstance(expected, str)
                or not re.fullmatch('[0-9a-f]{64}', expected)
                or hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected):
            raise ValueError('discovery_tools_dependency_changed:' + path)
    # Imported source/extension additions must not silently alter this tree.
    actual = {str(path) for path in root.rglob('*') if path.is_file()
              and '__pycache__' not in path.parts and path.suffix in ('.py', '.so')}
    if actual != {path for path in files if Path(path).is_relative_to(root)
                  and Path(path).suffix in ('.py', '.so')}:
        raise ValueError('discovery_tools_dependency_inventory_changed')
    return manifest


class DiscoveryToolsObserver:
    """A helper pool inside the canonical controller, with the same ownership.

    A private fixed module is chosen by the adapter, never by user input. All
    process/group/pidfd cleanup remains in the existing WorkerPool implementation.
    """
    def __init__(self, config, output_dir, emit, owner_channel, environment):
        from .namespace_pair import OwnerCheckedPool

        class ToolsPool(OwnerCheckedPool):
            worker_module = 'r2f.runtime.discovery_tools_worker'

        self.config, self.out, self.emit = config, Path(output_dir), emit
        self.out.mkdir()
        self.records, self.reference = [], None
        self.raw = {'schema': 'r2f.discovery-tools/v1', 'status': 'unknown',
                    'candidate_eligible': False, 'responsibility_layer': 'observation_tools',
                    'manifest': deepcopy(config[KEY]), 'events': self.records,
                    'run_id': config['run_id'], 'domain_id': config['domain_id'],
                    'started_unix_ns': time.time_ns(), 'reasons': []}

        def capture(event, **fields):
            token = emit(event, **fields)
            self.records.append(deepcopy(token))
            return token

        self.pool = ToolsPool(config, dict(os.environ), self.out, capture,
            owner_channel=owner_channel, role_environments={'discovery_tools': environment})

    def start(self):
        load_tools_environment(self.config[KEY])
        self.pool.start('discovery_tools')
        self.pool.wait(lambda: any(e['event'] == 'discovery_tools_ready' for e in self.records),
                       self.config['startup_timeout_sec'], 'discovery_tools_ready')

    def collect(self):
        command = self.pool.command('discovery_tools', 'observe')
        self.pool.wait(lambda: any(e['event'] == 'command_done' and e.get('command_id') == command
                                  for e in self.records),
                       self.config['startup_timeout_sec'], 'discovery_python_observation')
        command = self.pool.command('discovery_tools', 'cli')
        self.pool.wait(lambda: any(e['event'] == 'command_done' and e.get('command_id') == command
                                  for e in self.records),
                       self.config['startup_timeout_sec'], 'discovery_cli_observation')
        load_tools_environment(self.config[KEY])

    def finish(self):
        if self.reference is not None:
            return deepcopy(self.reference)
        self.pool.check_owner = False
        try:
            self.pool.close()
            load_tools_environment(self.config[KEY])
        except (OSError, RuntimeError, TimeoutError, TypeError, ValueError) as exc:
            self.raw['reasons'].append(type(exc).__name__ + ':' + str(exc))
        self.raw.update(workers=deepcopy(self.pool.metadata),
                        closed_unix_ns=time.time_ns())
        self.raw['cleanup_complete'] = bool(self.pool.metadata) and all(
            worker.get('cleanup_confirmed') is True and worker.get('returncode') == 0
            for worker in self.pool.metadata)
        ready = [e for e in self.records if e['event'] == 'discovery_tools_ready']
        observations = [e for e in self.records if e['event'] == 'discovery_python_observation']
        if not self.raw['cleanup_complete']:
            self.raw['reasons'].append('discovery_tools_cleanup_unconfirmed')
        if len(ready) != 1 or len(observations) != 1:
            self.raw['reasons'].append('discovery_tools_observation_incomplete')
        if any(e.get('errors') for e in observations):
            self.raw['reasons'].append('binding_observation_failed')
        if not self.raw['reasons']:
            self.raw['status'] = 'observed'
        data = json.dumps(self.raw, sort_keys=True).encode()+b'\n'
        path = self.out / 'discovery-tools.json'
        with path.open('xb') as handle:
            handle.write(data)
        self.reference = {'schema': self.raw['schema'], 'status': self.raw['status'],
                          'raw': {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}}
        return deepcopy(self.reference)
